import logging
import os
import sys
import tarfile
import tempfile
from pathlib import Path

import requests

DATA_URL = (
    "https://drive.google.com/uc?export=download&id=1jLcE2Jw1znaBy7FD7XCme_My_1PTZk17"
)
DATA_DIR = "uncommitted"
CHUNK_SIZE_8_MIB = 8 * 1024 * 1024

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s::: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def download_and_extract(url: str):
    with tempfile.TemporaryFile() as tmp:
        logger.info("Downloading %s", url)
        with requests.get(url, stream=True) as download_stream:
            download_stream.raise_for_status()
            for chunk in download_stream.iter_content(chunk_size=CHUNK_SIZE_8_MIB):
                tmp.write(chunk)
            logger.info("Uncompressing...")
            tmp.seek(0)
            with tarfile.open(fileobj=tmp) as uncompressed:
                uncompressed.extractall(path=DATA_DIR)


def ensure_data_directory():
    os.makedirs(DATA_DIR, exist_ok=True)


def list_data_directory():
    logger.info("The following data files were fetched:")
    for f in os.listdir(DATA_DIR):
        logger.info(" - %s", Path(DATA_DIR) / str(f))


def download_data():
    ensure_data_directory()
    download_and_extract(DATA_URL)
    list_data_directory()
    logger.info("All done!")


if __name__ == "__main__":
    download_data()
