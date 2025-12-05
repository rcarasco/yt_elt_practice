import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_HANDLE = "MrBeast"


def get_playlist_id():
    try:

        url = "https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={}&key={}".format(CHANNEL_HANDLE,API_KEY)

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()

        channel_items = data["items"][0]

        channel_playlist_id = channel_items["contentDetails"]["relatedPlaylists"]["uploads"]

        return channel_playlist_id
    
    except requests.exceptions.RequestException as e:
        raise e
    

if __name__ == "__main__":
    playlist_id = get_playlist_id()
    print("Playlist ID for channel '{}': {}".format(CHANNEL_HANDLE, playlist_id))