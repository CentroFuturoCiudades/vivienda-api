import os
import requests
import time
import json
from urllib.parse import urlparse
import shutil

TTL = 3600 * 15 * 24  # seconds
BASE_LOCATION = os.getenv("BASE_FILE_LOCATION", "./temp")
TIMESTAMP_FILE = f"{BASE_LOCATION}/file_timestamps.json"

def load_timestamps():
    if os.path.exists(TIMESTAMP_FILE):
        with open(TIMESTAMP_FILE, 'r') as file:
            return json.load(file)
    return {}

def save_timestamps(timestamps):
    with open(TIMESTAMP_FILE, 'w') as file:
        json.dump(timestamps, file)

def get_file(url):
    # Parse the URL to get the file name
    parsed_url = urlparse(url)
    file_name = os.path.basename(parsed_url.path)
    file_path = f"{BASE_LOCATION}/{file_name}"

    timestamps = load_timestamps()

    if os.path.isfile(file_path):
        file_age = time.time() - timestamps.get(file_path, 0)
        if file_age < TTL:
            print(f"The file {file_path} exists and is within TTL.")
            return file_path
        else:
            print(f"The file {file_path} exists but is older than TTL, dowloading again.")
    else:
        print(f"The file {file_path} does not exist.")

    download_file(url)
    print("File downloaded")
    return file_path

BLOB_URL = "https://reimaginaurbanostorage.blob.core.windows.net"
def get_blob_url(file_name: str) -> str:
    if os.getenv("ENVIRONMENT") == "local":
        return f"data/_primavera/final/{file_name}"
    access_token = os.getenv("BLOB_TOKEN")
    return f"{BLOB_URL}/primavera/{file_name}?{access_token}"

def download_file(url):
    if os.getenv("ENVIRONMENT") == "local":
        filename = os.path.basename(urlparse(url).path)
        if not os.path.exists(f"{BASE_LOCATION}/{filename}"):
            shutil.copy(url, BASE_LOCATION)
        return url
    os.makedirs(BASE_LOCATION, exist_ok=True)

    # Parse the URL to get the file name
    parsed_url = urlparse(url)
    file_name = os.path.basename(parsed_url.path)
    file_path = f"{BASE_LOCATION}/{file_name}"

    # Download the file
    response = requests.get(url)
    response.raise_for_status()  # Check if the request was successful

    # Save the file locally
    with open(file_path, 'wb') as file:
        file.write(response.content)

    # Update the timestamps
    timestamps = load_timestamps()
    timestamps[file_path] = time.time()
    save_timestamps(timestamps)

    return os.path.abspath(file_path)