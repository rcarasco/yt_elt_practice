import requests
import json
from datetime import date
from airflow.sdk import task
from airflow.models import Variable
# import os
# from dotenv import load_dotenv
# load_dotenv(dotenv_path=".env")

API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
max_results = 50

@task
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
    

@task
def get_video_ids(playlist_id):
    video_ids = []
    next_page_token = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={max_results}&playlistId={playlist_id}&key={API_KEY}"

    try:
        while True:
            url = base_url
            if next_page_token:
                url += "&pageToken={}".format(next_page_token)

            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            for item in data["items"]:
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)
                
            next_page_token = data.get("nextPageToken")

            if not next_page_token:
                break
    except requests.exceptions.RequestException as e:
        raise e 
    
    return video_ids


@task
def batch_list(video_ids, batch_size):
    for video_id in range(0, len(video_ids), batch_size):
        yield video_ids[video_id:video_id + batch_size]

@task
def extract_video_data(video_ids):
    video_details = []
    

    try:
        def batch_list(video_ids, batch_size):
            for video_id in range(0, len(video_ids), batch_size):
                yield video_ids[video_id:video_id + batch_size]



        for video_id_batch in batch_list(video_ids, 50):
            ids = ",".join(video_id_batch)
            base_url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={ids}&key={API_KEY}"

            response = requests.get(base_url)
            response.raise_for_status()

            data = response.json()

            for item in data["items"]:
                video_info = {
                    "video_id": item["id"],
                    "title": item["snippet"]["title"],
                    "published_at": item["snippet"]["publishedAt"],
                    "duration": item["contentDetails"]["duration"],
                    "view_count": item["statistics"].get("viewCount", 0),
                    "like_count": item["statistics"].get("likeCount", 0),
                    "comment_count": item["statistics"].get("commentCount", 0)
                }
                video_details.append(video_info)
    except requests.exceptions.RequestException as e:
        raise e 
    
    return video_details


@task
def save_to_json(data):
    saved_file_path = f"./data/YT_ETL_data_{date.today()}.json"
    with open(saved_file_path, "w") as f:
        json.dump(data, f, indent=4)


if __name__ == "__main__":
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_details = extract_video_details(video_ids)
    save_to_json(video_details)
