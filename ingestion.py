import requests
import os
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

def fetch_channel_data():
    try:
        response = requests.get(f"https://youtube.googleapis.com/youtube/v3/channels?part=snippet,statistics,contentDetails&id=UCrkzfc2yf-7q6pd7EtzgNaQ&key={YOUTUBE_API_KEY}")
        response.raise_for_status()
        print("Channel Response aquired")
        data = response.json()
        data = data["items"][0]
        extraction_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        channel_data = {
            "extraction_time": extraction_time,
            "channel_id": data["id"],
            "channel_name": data["snippet"]["title"],
            "description": data["snippet"].get("description", ""),
            "published_at": data["snippet"]["publishedAt"],
            "view_count": int(data["statistics"].get("viewCount", 0)),
            "subscriber_count": int(data["statistics"].get("subscriberCount", 0)),
            "video_count": int(data["statistics"].get("videoCount", 0))
        }

        df = pd.DataFrame([channel_data])
        print(df)
        # Append to CSV file. and check if file exists to write header only once
        df.to_csv("channel_data.csv", index=False,mode='a', header=not os.path.exists("channel_data.csv"))

    except requests.RequestException as e:
        print(f"An error occurred while fetching data: {e}")



        
def fetch_video_data():
    try:
        response = requests.get(f"https://youtube.googleapis.com/youtube/v3/videos?part=snippet,statistics&id=k64goYZL69E&key={YOUTUBE_API_KEY}")
        response.raise_for_status()
        print("Video Response aquired")
        data = response.json()
        data = data["items"][0]
        extraction_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        video_data = {
            "extraction_time": extraction_time,
            "video_id": data["id"],
            "title": data["snippet"]["title"],
            "publish_date": data["snippet"]["publishedAt"],
            "view_count": int(data["statistics"]["viewCount"]),
            "like_count": int(data["statistics"]["likeCount"]),
            "comment_count": int(data["statistics"]["commentCount"])
        }
        df = pd.DataFrame([video_data])
        print(df)
        df.to_csv("video_data.csv", index=False, mode='a', header=not os.path.exists("video_data.csv"))

    except requests.RequestException as e:
        print(f"An error occurred while fetching data: {e}")