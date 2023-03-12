# Importing libraries
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Create a DAG and setting default parameters
with DAG(
        "news_and_sentiment",
        default_args={
            "depends_on_past": False,
            "email": ["exemple@hotmail.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=1)
        },
        description="Extração de noticias e identificação dos sentimentos por elas exprimidas",
        schedule="0 0 * * *",
        start_date=datetime(2023, 3, 8),
        end_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['News', 'sentiment'],
) as dag:
    # Creating tasks
    task1 = BashOperator(
        task_id="news",
        bash_command="python3 ~/airflow/dags/script/news.py",
    )
    task2 = BashOperator(
        task_id="sentiments",
        bash_command="python3 ~/airflow/dags/script/sentiment.py",
        retries=1,
    )
    task1 >> task2
