# Importing libraries
import requests as re
import pandas as pd
from datetime import datetime as dt, timedelta as delta
import json
import numpy as np
import time

# Handling access errors


def post_response(url, results):
    data_accessed = False
    max_attempt = 10
    attempt = 0
    while not data_accessed:
        try:
            response = re.post(url, results)
            if response.status_code == 200:
                data_accessed = True
                return response.json()
            elif attempt > max_attempt:
                return None
            else:
                print(f"Post failed - Status Code: {response.status_code}")
                attempt += 1
        except Exception as error:
            error = str(error)
            print(f"Output: {error}")


# Read parquet file with news table
news = pd.read_parquet(f'~/airflow/data/news-{dt.now().date()}')
dicio = {'P+': "Strong positive", "P": "Positive", "NEU": "Neutral",
         "N": "Negative", "N+": "Strong negative", "NONE": "Without polarity"}
news["Sentiment"] = np.empty(shape=len(news))

# Getting data from MeaningCloud API
url = "https://api.meaningcloud.com/sentiment-2.1"
for i, j in enumerate(news["Title"]):
    payload = {
        'key': 'dbc9eb662c759b9bbe9c1abd7746870d',
        'txt': j,
        'lang': 'en'
    }

    response = post_response(url, results=payload)
    news["Sentiment"][i] = response["score_tag"]
    time.sleep(1)

# Change name using a dict
for key, value in dicio.items():
    news['Sentiment'] = news['Sentiment'].replace(key, value)

# Save to parquet file with response sentiment from API
news.to_parquet(f'~/airflow/data/news-sentiment-{dt.now().date()}')
