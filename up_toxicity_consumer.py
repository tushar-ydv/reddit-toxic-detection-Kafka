from kafka import KafkaConsumer
from transformers import pipeline
import json
import csv
import os

# Setting up CSV for MODELS to use
csv_file = "stream_data.csv"
if not os.path.exists(csv_file):
    with open(csv_file, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["id", "subreddit", "author", "body", "score", "toxicity_label", "score_confidence"])

# Hugging face model: RoBERTa hate speech model is used to classify toxic comments
classifier = pipeline("text-classification", model="facebook/roberta-hate-speech-dynabench-r1-target")

# Kafka Consumer Setup
consumer = KafkaConsumer(
    'reddit-comments',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='huggingface-toxicity',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Reading and classifying the comments: ")

for msg in consumer:
    comment = msg.value
    text = comment["body"]

    try:
        result = classifier(text[:512])[0]  # limit to 512 tokens
        label_id = result['label']  # e.g., 'LABEL_2'
        score = round(result['score'], 3)

        # Label map for this model
        label_map = {
            'LABEL_0': 'non-toxic',
            'LABEL_1': 'neutral',
            'LABEL_2': 'toxic'
        }
        label = label_map.get(label_id, 'unknown')

        icon = "TOXIC" if label == "toxic" else "OK"
        print(f"{icon} | Score: {score} | {text}")

        # Save to CSV
        with open(csv_file, mode='a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                comment["id"],
                comment["subreddit"],
                comment["author"],
                comment["body"],
                comment["score"],
                label,
                score
            ])

    except Exception as e:
        print("Error during classification:", e)
