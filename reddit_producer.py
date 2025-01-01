import os
from dotenv import load_dotenv
load_dotenv() 
import requests
import time
import traceback
from datetime import datetime
import pytz
from confluent_kafka import Producer
from pymongo import MongoClient
import json

# Reddit API credentials
CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
SECRET_KEY = os.getenv("REDDIT_SECRET_KEY")
USERNAME = os.getenv("REDDIT_USERNAME")
PASSWORD = os.getenv("REDDIT_PASSWORD")

# Kafka and MongoDB using environment variables
producer = Producer({'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP")})
TOPIC_NAME = 'reddit_posts'

# MongoDB connection
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["reddit_data"]
posts_collection = db["posts"]

def delivery_report(err, msg):
    """Callback for Kafka delivery reports."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def save_to_mongo(post):
    """Save a Reddit post to MongoDB."""
    try:
        posts_collection.insert_one(post)
        print(f"Saved post ID {post['id']} to MongoDB.")
    except Exception as e:
        print(f"Error saving to MongoDB: {e}")

def get_access_token():
    """Fetch an access token from Reddit API."""
    auth = requests.auth.HTTPBasicAuth(CLIENT_ID, SECRET_KEY)
    data = {'grant_type': 'password', 'username': USERNAME, 'password': PASSWORD}
    headers = {'User-Agent': 'MyAPI/0.0.1'}
    response = requests.post('https://www.reddit.com/api/v1/access_token',
                             auth=auth, data=data, headers=headers)
    response.raise_for_status()
    token = response.json()['access_token']
    expires_in = response.json()['expires_in']
    return token, expires_in

def get_headers(token):
    """Generate headers for Reddit API requests."""
    return {'Authorization': f'bearer {token}', 'User-Agent': 'MyAPI/0.0.1'}

def is_bot(author):
    """Check if the author is a bot."""
    return 'bot' in author.lower()

def fetch_new_posts(subreddit, headers, last_timestamp, collected_ids, max_posts=10):
    """Fetch new posts from a subreddit."""
    params = {'limit': 100}
    response = requests.get(f'https://oauth.reddit.com/r/{subreddit}/new',
                            headers=headers, params=params)
    response.raise_for_status()
    data = response.json()

    posts = data['data']['children']
    new_posts = []
    for post in posts:
        if len(new_posts) >= max_posts:
            break
        post_data = post['data']
        created_utc = post_data.get('created_utc')
        post_id = post_data.get('id')
        author = post_data.get('author')
        if created_utc > last_timestamp and post_id not in collected_ids:
            if is_bot(author):
                print(f"Skipping bot post from author: {author}")
                continue
            collected_ids.add(post_id)
            utc_dt = datetime.fromtimestamp(created_utc, tz=pytz.utc)
            central_tz = pytz.timezone('US/Central')
            central_dt = utc_dt.astimezone(central_tz)
            readable_datetime = central_dt.strftime('%Y-%m-%d %H:%M:%S %Z')
            new_posts.append({
                'id': post_id,
                'title': post_data.get('title'),
                'selftext': post_data.get('selftext'),
                'datetime': readable_datetime,
                'created_utc': created_utc,
                'author': author
            })
    return new_posts

def collect_reddit_data(subreddit, max_posts=10):
    """Collect data from Reddit and send to Kafka and MongoDB."""
    token, expires_in = get_access_token()
    token_acquired_time = time.time()
    headers = get_headers(token)
    collected_ids = set()
    last_timestamp = time.time()
    total_collected = 0

    while total_collected < max_posts:
        if time.time() - token_acquired_time >= (expires_in - 60):
            token, expires_in = get_access_token()
            token_acquired_time = time.time()
            headers = get_headers(token)
            print("Access token refreshed.")

        try:
            new_posts = fetch_new_posts(subreddit, headers, last_timestamp, collected_ids, max_posts - total_collected)
            if not new_posts:
                print("No new posts found. Retrying in 30 seconds...")
                time.sleep(30)
                continue

            for post in new_posts:
                producer.produce(TOPIC_NAME, json.dumps(post), callback=delivery_report)
                producer.flush()
                save_to_mongo(post)  # Save post to MongoDB
                print(f"Sent post ID {post['id']} to Kafka topic '{TOPIC_NAME}'.")

                print(f"Author: {post['author']}")
                print(f"Title: {post['title']}")
                print(f"Body: {post['selftext']}")
                print(f"Time: {post['datetime']}")
                print('-' * 80)

                total_collected += 1
                if total_collected >= max_posts:
                    break

            last_timestamp = max(post['created_utc'] for post in new_posts)
            time.sleep(10)

        except Exception as e:
            print(f"An error occurred: {e}")
            traceback.print_exc()
            print("Retrying in 30 seconds...")
            time.sleep(30)

    print(f"Collected total of {total_collected} posts.")

if __name__ == "__main__":
    collect_reddit_data('testinglimits', max_posts=10)
