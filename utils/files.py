import os
import requests
import time
import json
from urllib.parse import urlparse

TTL = 3600  # seconds
BASE_LOCATION = "./temp"
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

def download_file(url):
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