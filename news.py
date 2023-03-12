# Importing libraries
import requests as re
import pandas as pd
from datetime import datetime as dt, timedelta as delta

# Handling access errors


def get_response(url):
    data_accessed = False
    max_attempt = 10
    attempt = 0
    while not data_accessed:
        try:
            response = re.get(url)
            if response.status_code == 200:
                data_accessed = True
                return response.json()
            elif attempt > max_attempt:
                return None
            else:
                print(f"Get failed - Status Code: {response.status_code}")
                attempt += 1
        except Exception as error:
            error = str(error)
            print(f"Output: {error}")


# Setting start date, end date and countries to get news
end = dt.strftime(dt.now(), '%Y-%m-%dT%H:%M:%SZ')
start = dt.strftime((dt.now() - delta(days=1)), '%Y-%m-%dT%H:%M:%SZ')
countries = ['us', 'au', 'ca']
news = pd.DataFrame()

# Getting data by gnews API
for country in countries:
    download = get_response(
        f'https://gnews.io/api/v4/search?q=business&lang=en&country={country}&max=4&from={start}&to={end}&apikey=e3382d184f2e7fab478f90d3a0eddf5e')

    for i in download['articles']:
        titles = pd.DataFrame([i['title'], dt.strptime(
            i['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').date(), i['source']['name'], i['url'], country.upper()]).T
        news = pd.concat([news, titles])

# Change columns name and save table with news to parquet file
news.columns = ['Title', 'Publish date', 'Source', 'URL', 'Countries']
news = news.reset_index(drop=True)
news_parquet = news.to_parquet(f'~/airflow/data/news-{dt.now().date()}')
