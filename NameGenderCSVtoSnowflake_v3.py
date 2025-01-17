# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime, timedelta
import logging
import snowflake.connector
import boto3

# Snowflake 연결 함수
def get_snowflake_connection():
    """
    Snowflake 연결 설정
    """
    logging.info("Connecting to Snowflake")
    conn = snowflake.connector.connect(
        user=Variable.get("SNOWFLAKE_USER"),
        password=Variable.get("SNOWFLAKE_PASSWORD"),
        account=Variable.get("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",  # Snowflake 웨어하우스
        database=Variable.get("SNOWFLAKE_DBNAME"),
        schema=Variable.get("SNOWFLAKE_SCHEMA")
    )
    logging.info("Connected to Snowflake")
    return conn.cursor()

# S3 데이터 추출 함수
def extract(**context):
    """
    S3에서 데이터를 추출합니다.
    """
    bucket_name = Variable.get("S3_BUCKET_NAME")
    file_key = Variable.get("S3_FILE_KEY")
    logging.info(f"Extracting file from S3: Bucket={bucket_name}, Key={file_key}")
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),
        region_name='us-west-2'
    )
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    text = response['Body'].read().decode('utf-8')
    logging.info("Extracted data successfully from S3")
    return text

# Transform 함수
def transform(**context):
    """
    데이터를 변환합니다.
    """
    logging.info("Transforming data")
    text = context["task_instance"].xcom_pull(task_ids="extract")
    lines = text.strip().split("\n")[1:]  # 첫 번째 헤더 라인 제외
    records = []
    for line in lines:
        name, gender = line.split(",")
        records.append((name, gender))
    logging.info("Transformation complete")
    return records

# Load 함수
def load(**context):
    """
    변환된 데이터를 Snowflake로 로드합니다.
    """
    logging.info("Loading data to Snowflake")
    schema = Variable.get("SNOWFLAKE_SCHEMA")
    table = Variable.get("SNOWFLAKE_TABLE_NAME")
    
    records = context["task_instance"].xcom_pull(task_ids="transform")
    cur = get_snowflake_connection()

    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};")  # 기존 데이터 삭제
        for name, gender in records:
            sql = f"INSERT INTO {schema}.{table} (name, gender) VALUES (%s, %s)"
            cur.execute(sql, (name, gender))
        cur.execute("COMMIT;")
        logging.info("Data successfully loaded to Snowflake")
    except Exception as error:
        logging.error(f"Error during load: {error}")
        cur.execute("ROLLBACK;")
    finally:
        cur.close()

# Airflow DAG 정의
dag = DAG(
    dag_id='name_gender_snowflake_v3',
    start_date=datetime(2023, 4, 6),
    schedule='0 2 * * *',  # 매일 새벽 2시에 실행
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

# PythonOperator Task 정의
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag
)

# Task 의존성 설정
extract_task >> transform_task >> load_task