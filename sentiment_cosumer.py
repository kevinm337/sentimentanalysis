from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from transformers import pipeline
from simple_salesforce import Salesforce
import pandas as pd
import json
import os
import praw 
from praw.models import Redditor
from sklearn.cluster import KMeans
from slack_notifications import send_slack_alert
import time

# -----------------------------
# Configuration and Credentials
# -----------------------------

# Reddit API credentials (replace with your actual credentials)
CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
SECRET_KEY = os.getenv("REDDIT_SECRET_KEY")
USERNAME = os.getenv("REDDIT_USERNAME")
PASSWORD = os.getenv("REDDIT_PASSWORD")

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'sentiment_analysis_group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['reddit_posts'])

# MongoDB connection
MONGO_URI = "mongodb://localhost:27017/"
client = MongoClient(MONGO_URI)
db = client["reddit_data"]
sentiment_collection = db["sentiments"]

# Initialize Sentiment Analysis Pipeline (DistilBERT)
sentiment_pipeline = pipeline(
    'sentiment-analysis',
    model='distilbert-base-uncased-finetuned-sst-2-english',
    framework='pt',  # Force use of PyTorch
    device=-1  # CPU
)

# Salesforce credentials
SF_USERNAME = os.getenv('SF_USERNAME', 'test@gmail.com')
SF_PASSWORD = os.getenv('SF_PASSWORD', 'test234')
SF_SECURITY_TOKEN = os.getenv('SF_SECURITY_TOKEN', 'dfgfgdfsgfdgfshnfhsgdfgdf')

# Connect to Salesforce
try:
    sf = Salesforce(
        username=SF_USERNAME,
        password=SF_PASSWORD,
        security_token=SF_SECURITY_TOKEN
    )
    print("Salesforce connection successful!")
except Exception as e:
    print("Salesforce connection failed:", e)
    sf = None  # Prevent further Salesforce operations if connection fails

# Initialize PRAW Reddit Instance
reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=SECRET_KEY,
    user_agent='MyAPI/0.0.1',
    username=USERNAME,
    password=PASSWORD
)

# -----------------------------
# Utility Functions
# -----------------------------

def analyze_sentiment(interaction_text):
    """Perform sentiment analysis on the given text."""
    try:
        return sentiment_pipeline(interaction_text, truncation=True, max_length=512)[0]
    except Exception as e:
        print(f"Error during sentiment analysis: {e}")
        return None

def classify_sentiment(sentiment_result):
    """Classify sentiment into Low, Medium, High and return category and score."""
    label = sentiment_result['label']
    score = sentiment_result['score']
    numerical_score = score if label == 'POSITIVE' else -score
    if numerical_score <= -0.5:
        return 'High Risk', numerical_score
    elif -0.5 < numerical_score < 0.5:
        return 'Medium Risk', numerical_score
    else:
        return 'Low Risk', numerical_score

def fetch_additional_post_data(post_id):
    """Fetch additional data like upvotes and comments count."""
    try:
        submission = reddit.submission(id=post_id)
        submission_details = {
            'upvotes': submission.score,
            'comments': submission.num_comments
        }
        return submission_details
    except Exception as e:
        print(f"Error fetching additional post data: {e}")
        return {'upvotes': None, 'comments': None}

def fetch_user_karma(author_name):
    """Fetch Reddit user's karma."""
    try:
        redditor = Redditor(reddit, name=author_name)
        karma = redditor.link_karma + redditor.comment_karma
        return karma
    except Exception as e:
        print(f"Error fetching user karma: {e}")
        return None

def save_sentiment_to_mongo(post, sentiment_category, numerical_score):
    """Save sentiment analysis result to MongoDB."""
    try:
        additional_data = fetch_additional_post_data(post.get('id'))
        user_karma = fetch_user_karma(post.get('author'))

        sentiment_collection.insert_one({
            "post_id": post.get('id'),
            "author": post.get('author'),
            "title": post.get('title'),
            "body": post.get('selftext'),
            "sentiment_category": sentiment_category,
            "numerical_score": numerical_score,
            "created_utc": post.get('created_utc'),
            "upvotes": additional_data['upvotes'],
            "comments": additional_data['comments'],
            "reddit_karma": user_karma
        })
        print(f"Saved sentiment for post ID {post.get('id')} to MongoDB.")
    except Exception as e:
        print(f"Error saving sentiment to MongoDB: {e}")

def update_crm_with_sentiment(post, sentiment_category, numerical_score):
    """Update Salesforce Contact with sentiment data."""
    if sf is None:
        print("Salesforce connection is not initialized.")
        return

    reddit_username = post.get('author')
    message_body = post.get('selftext', '') or post.get('title', '')
    if not reddit_username:
        print("Post is missing a Reddit username.")
        return

    try:
        # Check for existing contact
        print(f"Querying Contact object for Reddit username: {reddit_username}")
        query = f"SELECT Id FROM Contact WHERE Reddit_Username__c = '{reddit_username}'"
        result = sf.query(query)

        if result['totalSize'] == 0:
            # Create a new contact if no match is found
            print(f"No matching Contact found. Creating new Contact for {reddit_username}.")
            new_contact = {
                'LastName': reddit_username,
                'Reddit_Username__c': reddit_username,
                'Latest_Sentiment_Label__c': sentiment_category,
                'Latest_Sentiment_Score__c': numerical_score,
                'Segment__c': sentiment_category,
                'Body__c': message_body
            }
            created_contact = sf.Contact.create(new_contact)
            print(f"New Contact created successfully with ID: {created_contact['id']}")
        else:
            # Update existing contact
            contact_id = result['records'][0]['Id']
            print(f"Found existing Contact with ID: {contact_id}. Updating...")
            sf.Contact.update(contact_id, {
                'Latest_Sentiment_Label__c': sentiment_category,
                'Latest_Sentiment_Score__c': numerical_score,
                'Segment__c': sentiment_category,
                'Body__c': message_body
            })
            print(f"Contact updated successfully: {contact_id}")
    except Exception as e:
        print(f"Error during Salesforce operation: {e}")

def post_comment(post_id, message):
    """Post a comment on a Reddit post using PRAW."""
    try:
        submission = reddit.submission(id=post_id)
        submission.reply(message)
        print(f"Posted comment on post ID {post_id}")
    except Exception as e:
        print(f"Error posting comment: {e}")

def trigger_real_time_intervention(post, sentiment_category, numerical_score):
    """
    Trigger real-time interventions:
    - If High Risk (negative sentiment), post an apology comment and send Slack alert.
    - If sentiment score > 0.8 (clearly positive), post a thank-you comment and send Slack alert.
    """
    post_id = post.get('id')
    author = post.get('author')
    comment = post.get('selftext', '') or post.get('title', '')

    if sentiment_category == "High Risk":
        # Apology for every negative post
        message = (
            "We noticed your concerns in this post. "
            "We sincerely apologize for any inconvenience caused. "
            "Please feel free to share more details so we can assist you better!"
        )
        post_comment(post_id, message)

        # Send Slack alert
        subject = "High Risk Alert: Negative Customer Sentiment Detected"
        content = (
            f"**Author:** {author}\n"
            f"**Sentiment Score:** {numerical_score}\n"
            f"**Comment:** {comment}\n\n"
            "Immediate action is required to address this customer's concerns."
        )
        send_slack_alert(subject, content)

    elif numerical_score > 0.8:
        # Thank-you for every highly positive post
        message = (
            "Thank you for your positive feedback! "
            "We’re thrilled to see your engagement and support! "
            "Feel free to share more of your thoughts!"
        )
        post_comment(post_id, message)

        # Send Slack alert
        subject = "Positive Sentiment Detected: Thank You!"
        content = (
            f"**Author:** {author}\n"
            f"**Sentiment Score:** {numerical_score}\n"
            f"**Comment:** {comment}\n\n"
            "Consider acknowledging this positive feedback in team meetings."
        )
        send_slack_alert(subject, content)

def weekly_aggregated_interventions():
    """
    Perform weekly aggregated interventions.
    """
    try:
        last_7_days = int(time.time() - 7 * 24 * 60 * 60)  # Timestamp for 7 days ago
        user_posts = sentiment_collection.aggregate([
            {"$match": {"created_utc": {"$gte": last_7_days}}},
            {"$group": {
                "_id": "$author",
                "negative_count": {"$sum": {"$cond": [{"$lt": ["$numerical_score", -0.5]}, 1, 0]}},
                "positive_count": {"$sum": {"$cond": [{"$gt": ["$numerical_score", 0.8]}, 1, 0]}},
                "recent_negative_post": {"$last": "$post_id"},
                "recent_positive_post": {"$first": "$post_id"}
            }}
        ])

        for user in user_posts:
            username = user["_id"]
            negative_count = user.get("negative_count", 0)
            positive_count = user.get("positive_count", 0)

            # Process Negative Posts
            if negative_count > 0:
                negative_post_id = user.get("recent_negative_post")
                if negative_post_id:
                    negative_message = (
                        f"Hi {username}, we noticed some concerns raised in your recent posts. "
                        "We’d love to understand how we can address them. Please feel free to share more details!"
                    )
                    post_comment(negative_post_id, negative_message)

                    # Send Slack alert
                    subject = "Weekly Alert: User with Negative Sentiment"
                    content = (
                        f"**Author:** {username}\n"
                        f"**Number of Negative Posts:** {negative_count}\n"
                        "An aggregated apology comment has been posted on their most recent negative post."
                    )
                    send_slack_alert(subject, content)

            # Process Positive Posts
            if positive_count > 0:
                positive_post_id = user.get("recent_positive_post")
                if positive_post_id:
                    positive_message = (
                        f"Hi {username}, thank you for your positive contributions! "
                        "We truly value your support and engagement!"
                    )
                    post_comment(positive_post_id, positive_message)

                    # Send Slack alert
                    subject = "Weekly Alert: User with Positive Sentiment"
                    content = (
                        f"**Author:** {username}\n"
                        f"**Number of Positive Posts:** {positive_count}\n"
                        "A thank-you comment has been posted on one of their most positive posts."
                    )
                    send_slack_alert(subject, content)

    except Exception as e:
        print(f"Error in weekly aggregated interventions: {e}")

def export_to_csv():
    """Export MongoDB collections to CSV for visualization in Tableau."""
    try:
        # Export the `sentiments` collection
        sentiments_data = list(sentiment_collection.find({}, {"_id": 0}))
        if sentiments_data:
            df_sentiments = pd.DataFrame(sentiments_data)
            df_sentiments.to_csv("sentiments.csv", index=False)
            print("Exported 'sentiments' collection to sentiments.csv.")
        else:
            print("No data found in 'sentiments' collection.")

        # Export the `user_segments` collection
        segments_data = list(db["user_segments"].find({}, {"_id": 0}))
        if segments_data:
            df_segments = pd.DataFrame(segments_data)
            df_segments.to_csv("user_segments.csv", index=False)
            print("Exported 'user_segments' collection to user_segments.csv.")
        else:
            print("No data found in 'user_segments' collection.")
    except Exception as e:
        print(f"Error exporting data to CSV: {e}")

# -----------------------------
# Main Kafka Consumer Loop
# -----------------------------
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Kafka Error: {msg.error()}")
            continue

        try:
            post = json.loads(msg.value().decode('utf-8'))
            print(f"Processing post: {post}")
            interaction_text = post.get('selftext', '') or post.get('title', '')
            if not interaction_text.strip():
                print(f"Skipping empty post ID {post.get('id', 'Unknown')}.")
                continue

            # Analyze sentiment
            sentiment_result = analyze_sentiment(interaction_text)
            if not sentiment_result:
                continue

            sentiment_category, numerical_score = classify_sentiment(sentiment_result)

            # Save sentiment to MongoDB
            save_sentiment_to_mongo(post, sentiment_category, numerical_score)

            # Update Salesforce CRM
            update_crm_with_sentiment(post, sentiment_category, numerical_score)

            # Real-time Interventions
            trigger_real_time_intervention(post, sentiment_category, numerical_score)

        except Exception as e:
            print(f"Error processing message: {e}")

except KeyboardInterrupt:
    print("Shutting down consumer...")
finally:
    consumer.close()

    # ----------------------------
    # Clustering Logic After Stop
    # ----------------------------
    print("Starting clustering process...")

    # Fetch documents from MongoDB for clustering
    docs = list(sentiment_collection.find({}, {
        "_id": 0,
        "author": 1,
        "numerical_score": 1
    }))

    if not docs:
        print("No sentiment data found in MongoDB for clustering.")
    else:
        df = pd.DataFrame(docs)

        # Feature engineering
        df['is_negative'] = df['numerical_score'] < 0

        user_stats = df.groupby('author').agg(
            avg_sentiment=('numerical_score', 'mean'),
            negative_count=('is_negative', 'sum'),
            total_posts=('author', 'count')
        ).reset_index()

        # Frequency of negative interactions
        user_stats['negative_frequency'] = user_stats['negative_count'] / user_stats['total_posts']

        # Select features for clustering
        X = user_stats[['avg_sentiment', 'negative_frequency']].values

        kmeans = KMeans(n_clusters=1, random_state=42)
        labels = kmeans.fit_predict(X)

        user_stats['cluster'] = labels

        # Assign segments (with 1 cluster, all high risk)
        user_stats['segment'] = "High Risk"

        print("User Segmentation Results:")
        print(user_stats[['author', 'avg_sentiment', 'negative_frequency', 'total_posts', 'segment']])

        # Store segments in MongoDB
        segments_collection = db["user_segments"]
        segments_collection.delete_many({})
        segments_collection.insert_many(user_stats.to_dict('records'))
        print("User segments stored in 'user_segments' collection.")

    # Export to CSV
    print("Exporting collections to CSV...")
    export_to_csv()
    print("CSV export completed.")

    # Trigger weekly aggregated interventions after processing
    print("Triggering weekly aggregated interventions...")
    weekly_aggregated_interventions()
    print("Weekly aggregated interventions completed.")
