from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import boto3

# S3 configuration
BUCKET_NAME = "github-etl-data"
S3_KEY = "github/commits_transformed.csv"

# Functions
def extract():
    url = "https://api.github.com/repos/tensorflow/tensorflow/commits?per_page=50"
    response = requests.get(url)
    data = response.json()
    commits = []
    for commit in data:
        commits.append({
            "sha": commit.get("sha"),
            "author": commit["commit"]["author"].get("name"),
            "date": commit["commit"]["author"].get("date"),
            "message": commit["commit"].get("message")
        })
    df = pd.DataFrame(commits)
    df.to_csv("/tmp/github_commits_raw.csv", index=False)
    print("Extract completed")

def transform():
    df = pd.read_csv("/tmp/github_commits_raw.csv")
    df['date'] = pd.to_datetime(df['date'])
    df['message_length'] = df['message'].apply(len)
    df = df.drop_duplicates(subset=['sha'])
    df.to_csv("/tmp/github_commits_transformed.csv", index=False)
    print("Transform completed")

def load_to_s3():
    s3 = boto3.client('s3')
    s3.upload_file("/tmp/github_commits_transformed.csv", BUCKET_NAME, S3_KEY)
    print(f"Loaded to s3://{BUCKET_NAME}/{S3_KEY}")

# DAG definition
default_args = {
    'owner': 'rutuja',
    'start_date': datetime(2026, 3, 16),
    'retries': 1
}

with DAG(
    dag_id='github_etl_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='extract',
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    t3 = PythonOperator(
        task_id='load_to_s3',
        python_callable=load_to_s3
    )

    t1 >> t2 >> t3  # define task order
