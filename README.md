# Reddit Sentiment Pipeline

This repository contains Python scripts that:

1. Collect Reddit posts from specified subreddits (`reddit_producer.py`).
2. Produce those posts to a Kafka topic.
3. Consume posts from Kafka (`sentiment_consumer.py`), run them through a sentiment analysis pipeline, and store results in MongoDB.
4. Update Salesforce contacts with sentiment data and trigger Slack notifications for real-time interventions.
5. Provide weekly aggregated interventions and clustering logic.

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Environment Variables](#environment-variables)
3. [Running the Producer and Consumer](#running-the-producer-and-consumer)
4. [Additional Documentation](#additional-documentation)
5. [License](#license)

---

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Python 3.8+ installed
- Kafka and Zookeeper installed and running locally (or accessible remotely)
- MongoDB instance running locally (or accessible remotely)
- Salesforce account with valid credentials
- Slack workspace with a bot token

```bash
# (Optional) Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # on Linux/Mac
venv\Scripts\activate     # on Windows

pip install -r requirements.txt
```

Launch-
zookeeper-server-start.sh config/zookeeper.properties
kafka-server-start.sh config/server.properties
python reddit_producer.py
python sentiment_consumer.py
