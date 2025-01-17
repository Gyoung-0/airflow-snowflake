# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import snowflake.connector
import boto3


# S3 데이터 추출 함수
def extract_from_s3(bucket_name, file_key):
    """
    S3에서 데이터를 추출합니다.
    """
    logging.info("Extract started")
    s3 = boto3.client(
        's3',
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),  # AWS Access Key
        aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),  # AWS Secret Key
        region_name='us-west-2'  # 리전
    )

    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    text = response['Body'].read().decode('utf-8')
    logging.info("Extract done")
    return text


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
        warehouse="COMPUTE_WH",  # Snowflake 웨어하우스 (고정값 또는 Variable로 관리 가능)
        database=Variable.get("SNOWFLAKE_DBNAME"),
        schema=Variable.get("SNOWFLAKE_SCHEMA")
    )
    logging.info("Connected to Snowflake")
    return conn.cursor()


# Transform 함수
def transform(text):
    """
    데이터를 변환합니다.
    """
    logging.info("Transform started")
    lines = text.strip().split("\n")[1:]  # 첫 번째 헤더 라인 제외
    records = []
    for line in lines:
        name, gender = line.split(",")  # CSV 데이터 파싱
        records.append((name, gender))
    logging.info("Transform ended")
    return records


# Load 함수
def load(records):
    """
    변환된 데이터를 Snowflake로 로드합니다.
    """
    logging.info("Load started")
    schema = Variable.get("SNOWFLAKE_SCHEMA")
    table = "name_gender"
    cur = get_snowflake_connection()

    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};")  # 기존 데이터 삭제
        for name, gender in records:
            logging.info(f"Inserting record: {name}, {gender}")
            sql = f"INSERT INTO {schema}.{table} (name, gender) VALUES (%s, %s)"
            cur.execute(sql, (name, gender))
        cur.execute("COMMIT;")
    except Exception as error:
        logging.error(f"Error during load: {error}")
        cur.execute("ROLLBACK;")
    finally:
        cur.close()
    logging.info("Load done")


# ETL 프로세스
def etl(**context):
    """
    S3에서 데이터를 추출하고 변환한 후 Snowflake로 로드하는 ETL 프로세스.
    """
    bucket_name = "nane-gender"
    file_key = "name_gender.csv"

    # S3에서 데이터 추출
    data = extract_from_s3(bucket_name, file_key)
    # 데이터 변환
    records = transform(data)
    # 데이터 로드
    load(records)


# Airflow DAG 정의
dag = DAG(
    dag_id='name_gender_snowflake_v2',
    start_date=datetime(2023, 4, 6),
    schedule='0 2 * * *',  # 매일 새벽 2시 실행
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)


# PythonOperator Task 정의
task = PythonOperator(
    task_id='perform_etl',
    python_callable=etl,
    dag=dag
)