import praw
from kafka import KafkaProducer
import json
import time

#API Credentials
REDDIT_CLIENT_ID = "4A-eVGV9hn19AD9rQFerng"
REDDIT_SECRET = "B7aj-dLWpI6y-3mb50O4pZL2GXXnNQ"
REDDIT_USER_AGENT = "reddit-bd-hw3-g21/1.0"

# Reddit connection
reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_SECRET,
    user_agent=REDDIT_USER_AGENT
)

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Choosing sub reddits to stream
subreddit = reddit.subreddit("news+AskReddit+politics")

print("Live: Sending Reddit comments to Kafka: ")

for comment in subreddit.stream.comments(skip_existing=True):
    data = {
        "id": comment.id,
        "subreddit": str(comment.subreddit),
        "author": str(comment.author),
        "body": comment.body,
        "created_utc": comment.created_utc,
        "score": comment.score
    }

    producer.send("reddit-comments", data)
    print("Sent:", data)
    time.sleep(1)
