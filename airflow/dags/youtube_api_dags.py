from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG

import requests
import os
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

def fetch_data():
    try:
        # Fetch video IDs from the channel
        response = requests.get(f"https://youtube.googleapis.com/youtube/v3/search?part=id&channelId=UCrkzfc2yf-7q6pd7EtzgNaQ&maxResults=15&type=video&key={YOUTUBE_API_KEY}")
        response.raise_for_status()
        print("Channel Response aquired")
        data = response.json()
        data = data["items"]
        df = pd.DataFrame()
        for item in data:
            video_id = item["id"]["videoId"]
            print(f"Fetching data for video ID: {video_id}")
            video_data = fetch_video_data(video_id)
            if video_data:
                df = pd.concat([df, pd.DataFrame([video_data])], ignore_index=True)

        df.to_csv("/opt/airflow/new_video_data.csv", index=False)  # Overwrite the file each time


    except requests.RequestException as e:
        print(f"An error occurred while fetching data: {e}")



        
def fetch_video_data(video_id):
    try:
        response = requests.get(f"https://youtube.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={video_id}&key={YOUTUBE_API_KEY}")
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
        # the csv file should not be appended to but overwritten to prevent duplicates well keep the current one for backup
        df.to_csv("/opt/airflow/video_data.csv", index=False, mode='a', header=not os.path.exists("/opt/airflow/video_data.csv"))
        return video_data

    except requests.RequestException as e:
        print(f"An error occurred while fetching data: {e}")


with DAG(
    dag_id = "youtube_data_pipeline",
    start_date = datetime(2025, 10, 5),
    schedule_interval= '@hourly',
    catchup = False,
) as dag:
    fetch_data_task = PythonOperator(
        task_id = 'fetch_data',
        python_callable = fetch_data
    )
    run_pyspark_task = BashOperator(
        task_id='run_spark',
        bash_command='''
        docker exec -e HOME=/opt/bitnami/spark spark-master \
            spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            /opt/spark/data/spark_transformation.py
        '''
    )


    fetch_data_task >> run_pyspark_task



