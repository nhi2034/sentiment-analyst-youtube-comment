import json
import logging
from kafka import KafkaProducer
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
import time

# Kafka setup
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'youtube_comments'

# YouTube API setup
api_key = "AIzaSyCPtSwMcDbIwjSKOuVEvsquW1Sj3WXdR5c"
youtube = build('youtube', 'v3', developerKey=api_key)

# Search term for YouTube
search_term = 'Akame ga kill'


class YouTubeStreamer:
    def __init__(self, search_term):
        self.search_term = search_term

    def search_youtube_videos(self):
        try:
            # Perform a search request to the YouTube API
            request = youtube.search().list(
                q=self.search_term,
                type='video',
                part='snippet',
                maxResults=10  # You can adjust the number of results as needed
            )
            response = request.execute()

            # Process the search results and send to Kafka
            for item in response['items']:
                video_id = item['id']['videoId']
                video_data = {
                    'video_id': video_id,
                    'title': item['snippet']['title'],
                    'description': item['snippet']['description']
                }
                print(video_data)
                producer.send(topic_name, value=json.dumps(video_data).encode('utf-8'))

                # Fetch comments for the video
                self.fetch_video_comments(video_id)

        except HttpError as e:
            logging.error(f'Error during YouTube API request: {e}')

    def fetch_video_comments(self, video_id):
        try:
            # Perform a request to get video comments
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                textFormat='plainText',
                maxResults=10  # You can adjust the number of comments as needed
            )
            response = request.execute()

            # Process the comments and send to Kafka
            for item in response['items']:
                comment_data = {
                    'video_id': video_id,
                    'comment_id': item['id'],
                    'text': item['snippet']['topLevelComment']['snippet']['textDisplay']
                }
                print(comment_data)
                producer.send(topic_name, value=json.dumps(comment_data).encode('utf-8'))

        except HttpError as e:
            logging.error(f'Error during YouTube API request for comments: {e}')

    def start_streaming_youtube(self):
        while True:
            self.search_youtube_videos()
            time.sleep(10000)  # Sleep for 60 seconds before the next iteration (adjust as needed)


if __name__ == '__main__':
    youtube_streamer = YouTubeStreamer(search_term)
    youtube_streamer.start_streaming_youtube()