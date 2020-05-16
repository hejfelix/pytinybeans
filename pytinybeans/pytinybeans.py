import datetime
import time
import typing
from typing import List, Optional
from contextlib import contextmanager
import os
import boto3
from boto3_type_annotations.s3 import Client, ServiceResource
import uuid
import logging
from urllib.parse import urljoin
import json
import requests
from pathlib import Path
from dataclasses import dataclass

IOS_CLIENT_ID = "13bcd503-2137-9085-a437-d9f2ac9281a1"


class TinybeansUser(object):
    def __init__(self, data: dict) -> None:
        self.id = data["id"]
        self.email_address = data["emailAddress"]
        self.first_name = data["firstName"]
        self.last_name = data["lastName"]
        self.username = data["username"]


class TinybeanFollowing(object):
    def __init__(self, data: dict) -> None:
        self.id = data["id"]
        self.url = data["URL"]
        self.relationship = data["relationship"]["label"]
        self.journal = TinybeanJournal(data["journal"])


class TinybeanJournal(object):
    def __init__(self, data: dict) -> None:
        self.id = data["id"]
        self.title = data["title"]
        self.children: typing.List[TinybeanChild] = []

        for child in data["children"]:
            self.children.append(TinybeanChild(journal=self, data=child))


class TinybeanChild(object):
    def __init__(self, journal: TinybeanJournal, data: dict) -> None:
        self.id = data["id"]
        self.first_name = data["firstName"]
        self.last_name = data["lastName"]
        self.gender = data["gender"]
        self.date_of_birth = datetime.datetime.strptime(
            data["dob"], "%Y-%m-%d")
        self.journal = journal

    def __repr__(self) -> str:
        return "<{name} {dob}>".format(name=self.name, dob=self.date_of_birth,)

    @property
    def name(self):
        return "%s %s" % (self.first_name, self.last_name)


@dataclass
class MediaItem:
    day: int
    month: int
    year: int
    file: Path
    children: List[TinybeanChild]


class TinybeanEntry(object):
    def __init__(self, data: dict) -> None:
        self._data = data
        self.id = data["id"]
        self.uuid = data["uuid"]
        self.deleted = data["deleted"]

        if data.get("attachmentType") == "VIDEO":
            self.type = "VIDEO"
            self.video_url = data["attachmentUrl_mp4"]
        else:
            self.type = data["type"]

        try:
            self.latitude = data["latitude"]
            self.longitude = data["longitude"]
        except KeyError:
            self.latitude = None
            self.longitude = None

        self.caption = data["caption"]
        self.blobs = data["blobs"]
        self.emotions: typing.List[TinybeanEmotion] = []

        try:
            for emotion in data["emotions"]:
                self.emotions.append(TinybeanEmotion(emotion))
        except KeyError:
            pass

        self.comments: typing.List[TinybeanComment] = []

        try:
            for comment in data["comments"]:
                self.comments.append(TinybeanComment(comment))
        except KeyError:
            pass


class TinybeanComment(object):
    def __init__(self, data: dict) -> None:
        self.id = data["id"]
        self.text = data["details"]
        self.user = TinybeansUser(data["user"])


class TinybeanEmotion(object):
    def __init__(self, data: dict) -> None:
        self.id = data["id"]
        self.entry_id = data["entryId"]
        self.user_id = data["userId"]
        self.type = data["type"]["label"]


@contextmanager
def s3_client():
    boto3.set_stream_logger(level=logging.DEBUG)
    client = boto3.client('cognito-identity', region_name="us-east-1")
    identity_id = os.environ['IDENTITY_ID']
    credentials = client.get_credentials_for_identity(IdentityId=identity_id)
    access_key_id = credentials['Credentials']['AccessKeyId']
    secret_access_key = credentials['Credentials']['SecretKey']
    session_token = credentials['Credentials']['SessionToken']
    s3client: Client = boto3.client('s3', aws_access_key_id=access_key_id,
                                    aws_secret_access_key=secret_access_key, aws_session_token=session_token, region_name="us-west-2")
    logging.debug(f"access: {access_key_id}, secret: {secret_access_key}")
    logging.info(f"got S3 client: {s3client}")
    yield s3client


class PyTinybeans(object):
    API_BASE_URL = "https://tinybeans.com/api/1/"
    CLIENT_ID = IOS_CLIENT_ID

    def __init__(self) -> None:
        self.session = requests.Session()
        self._access_token = None

    def _api(
        self, path: str, params: dict = None, json: dict = None, method: str = "GET"
    ) -> requests.Response:
        url = urljoin(self.API_BASE_URL, path)
        logging.debug(f"Sending request to {method} {url}")
        if self._access_token:
            response = self.session.request(
                method,
                url,
                params=params,
                json=json,
                headers={"authorization": self._access_token},
            )
        else:
            response = self.session.request(
                method, url, params=params, json=json,)

        return response

    @property
    def logged_in(self):
        if self._access_token:
            return True

        return False

    def login(self, username: str, password: str) -> None:
        if self.logged_in:
            # check via api/me or something that this token works
            return

        response = self._api(
            path="authenticate",
            json={
                "username": username,
                "password": password,
                "clientId": IOS_CLIENT_ID,
            },
            method="POST",
        )
        self._access_token = response.json()["accessToken"]
        self.user = TinybeansUser(data=response.json()["user"])

    def get_followings(self):
        response = self._api(path="followings", params={
                             "clientId": self.CLIENT_ID},)

        for following in response.json()["followings"]:
            yield TinybeanFollowing(following)

    @property
    def children(self):
        children = []
        for following in self.get_followings():
            children.extend(following.journal.children)

        return children

    def delete(self, entry: TinybeanEntry):
        logging.info(f"Deleting entry: {entry.id}, {entry.type}, {entry.blobs['p']}")
        response = self._api(
            method="DELETE", path=f"journals/1572712/entries/{entry.id}")

    def upload_media(self, mediaItem: MediaItem, s3Client: Optional[Client] = None, index: int = 1, total_items: int = 1):
        id = str(uuid.uuid4()).upper()
        suffix = mediaItem.file.as_posix().split(".")[-1]
        pathToFile = mediaItem.file.as_posix()
        destinationFileName = f'{id}.{suffix}'
        file_size = mediaItem.file.stat().st_size
        self.bytes_uploaded = 0
        logging.info(f"Uploading item: {mediaItem}")

        def callback(numBytesProgress):
            self.bytes_uploaded += numBytesProgress
            print(f"\r{round(self.bytes_uploaded / file_size * 100,2)}%",end="",flush=True)

        if s3Client is not None:
            try:
                response = s3Client.upload_file(
                    Filename=pathToFile, Bucket="tinybeans-remote-upload-prod", Key=destinationFileName, Callback=callback)
                logging.debug(response)
            except Exception as err:
                logging.error(err)
        else:
            with s3_client() as s3:
                try:
                    response = s3.upload_file(
                        Filename=pathToFile, Bucket="tinybeans-remote-upload-prod", Key=destinationFileName, Callback=callback)
                    logging.debug(response)
                except Exception as err:
                    logging.error(err)
        total_progress = round(index / total_items * 100, 2)

        logging.info(
            f"Done, total progress: {index} / {total_items} ~ {total_progress}%")

        body = {
            "day": mediaItem.day,
            "month": mediaItem.month,
            "year": mediaItem.year,
            "children": [child.id for child in mediaItem.children],
            "caption": "",
            "remoteFileName": destinationFileName
        }

        response = self._api(
            method="POST", path="journals/1572712/entries", json=body)
        logging.debug(response)
        logging.debug(response.text)
        logging.debug(response.text)

    def upload_medias(self, mediaItems: List[MediaItem]):
        with s3_client() as s3:
            for index, item in enumerate(mediaItems):
                self.upload_media(item, s3, index, len(mediaItems))

    def get_entries(self, child: TinybeanChild, last: int = None, get_deleted_entries: bool = False):
        entries = []

        if last is None:
            last = int(
                time.mktime(
                    (
                        datetime.datetime.utcnow() - datetime.timedelta(days=0)
                    ).timetuple()
                )
                * 1000
            )
        response = self._api(
            path="journals/%s/entries" % child.journal.id,
            params={"clientId": self.CLIENT_ID,
                    "fetchSize": 200, "last": last, },
        )
        for entry in response.json()["entries"]:
            entries.append(TinybeanEntry(entry))

        print(response.json()["numEntriesRemaining"] )
        while response.json()["numEntriesRemaining"] > 0:
            last = response.json()["entries"][0]["timestamp"]

            response = self._api(
                path="journals/%s/entries" % child.journal.id,
                params={"clientId": self.CLIENT_ID,
                        "fetchSize": 200, "last": last, },
            )
            
            if "entries" in response.json():
                for entry in response.json()["entries"]:
                    entries.append(TinybeanEntry(entry))

        return [e for e in entries if not e.deleted and not get_deleted_entries]
