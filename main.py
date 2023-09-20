import os


from radiojavanapi import Client
import requests
import uuid
import boto3
import logging
from botocore.exceptions import ClientError

client = Client()
CRAWL_COUNT = 120000
LAST_SONG_ID = 500000
SONG_PATH_SAVE_MEMORY = "/tmp/radiojavan/songs/"
SONG_THUMBNAIL_PATH_SAVE_MEMORY = "/tmp/radiojavan/thumbnails/"
CSV_PATH_SAVE_MEMORY = "/tmp/radiojavan/metadata/music.csv"

# Configure logging
logging.basicConfig(level=logging.INFO)

s3_resource = boto3.resource(
    's3',
    endpoint_url='https://s3.ir-thr-at1.arvanstorage.ir',
    aws_access_key_id='665a3e30-0f4a-400e-9d93-fa48f945f9a5',
    aws_secret_access_key='f14ad7e3b8e94a3e1225c40517217633d5db5883af8278ea3dc0171b25ac28fe'
)

BUCKET = s3_resource.Bucket('yetar')


def get_uploaded_ids():
    ids = []
    with open(CSV_PATH_SAVE_MEMORY, "r") as file:
        for line in file.readlines():
            ids.append(int(line.split(',')[0]))
    return ids


def save_to_object_storage(file_path_os):
    file_name = file_path_os.split('/')[-1]
    try:
        with open(file_path_os, "rb") as file_os:
            path_os = BUCKET.put_object(
                ACL='public-read',
                Body=file_os,
                Key=file_name
            )
    except ClientError as e:
        logging.error(e)
        return False
    return path_os


def download_from_link(link, path, ext):
    response = requests.get(link)
    file_path = path + f'{uuid.uuid4()}' + '.' + ext
    with open(file_path, "wb") as file:
        file.write(response.content)
    return file_path


def crawl(rj_client: Client, last_song_id):
    i = 0
    while i <= CRAWL_COUNT:
        try:
            song = rj_client.get_song_by_id(last_song_id - i)
            if song.id in get_uploaded_ids():
                continue
            # DOWNLOAD
            ## download song
            logging.info(f"song {song.id} downloading song")
            if song.hq_link:
                link = song.hq_link
            else:
                link = song.link
            song_path = download_from_link(link, SONG_PATH_SAVE_MEMORY, link.split('.')[-1])

            ## download thumbnail
            logging.info(f"song {song.id} downloading thumbnail")
            if song.thumbnail:
                link_thumbnail = song.thumbnail
            song_thumbnail_path = download_from_link(link_thumbnail, SONG_THUMBNAIL_PATH_SAVE_MEMORY, link.split('.')[-1])

            # SAVE
            ## save song to object storage
            logging.info(f"song {song.id} save song")
            song_path_os = save_to_object_storage(song_path)
            ## save thumbnail to object storage
            logging.info(f"song {song.id} save thumbnail")
            song_thumbnail_path_os = save_to_object_storage(song_thumbnail_path)

            # SAVE METADATA
            ## save song metadata to csv
            logging.info(f"song {song.id} save metadata")
            with open(CSV_PATH_SAVE_MEMORY, "a") as csv_file:
                csv_file.write(f'{song.id},{song_path_os},{song_thumbnail_path_os}\n')

            # DELETE
            ## delete song
            logging.info(f"song {song.id} delete song")
            if os.path.exists(song_path):
                os.remove(song_path)
            ## delete thumbnail
            logging.info(f"song {song.id} delete thumbnail")
            if os.path.exists(song_thumbnail_path):
                os.remove(song_thumbnail_path)
            i += 1
            logging.info(f"song {song.id} finished")
        except:
            logging.error(f"song {song.id} failed")
            continue


def start():
    crawl(client, LAST_SONG_ID)
