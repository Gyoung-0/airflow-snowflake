# -*- coding: utf-8 -*-
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import logging
import snowflake.connector
import boto3

# S3 데이터 추출 함수
def extract_from_s3(bucket_name, file_key):
    """
    S3에서 데이터를 추출하는 함수.
    """
    logging.info(f"Attempting to fetch file from S3: Bucket={bucket_name}, Key={file_key}")
    s3 = boto3.client(
        's3',
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),  # AWS Access Key
        aws_secret_access_key=Variable.get("AWS_SECRET_KEY"),  # AWS Secret Key
        region_name='us-west-2'  # S3 버킷 리전
    )
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        logging.info("File successfully fetched from S3.")
        text = response['Body'].read().decode('utf-8')  # 파일 내용을 텍스트로 변환
        return text
    except Exception as e:
        logging.error(f"Error fetching file from S3: {e}")
        raise

# Snowflake 연결 함수
def get_snowflake_connection():
    """
    Snowflake 데이터베이스 연결 설정.
    """
    conn = snowflake.connector.connect(
        user=Variable.get("SNOWFLAKE_USER"),  # Snowflake 사용자 이름
        password=Variable.get("SNOWFLAKE_PASSWORD"),  # Snowflake 비밀번호
        account=Variable.get("SNOWFLAKE_ACCOUNT"),  # Snowflake 계정
        warehouse="COMPUTE_WH",  # Snowflake 웨어하우스
        database=Variable.get("SNOWFLAKE_DBNAME"),  # Snowflake 데이터베이스
        schema=Variable.get("SNOWFLAKE_SCHEMA")  # Snowflake 스키마
    )
    return conn

# Transform 함수
def transform(text):
    """
    데이터를 변환하는 함수.
    """
    logging.info("Transform started")
    lines = text.strip().split("\n")[1:]  # 첫 번째 헤더 라인을 제외
    records = []
    for l in lines:
        name, gender = l.split(",")  # CSV 데이터를 파싱
        records.append([name, gender])
    logging.info("Transform ended")
    return records

# Load 함수
def load(records):
    """
    데이터를 Snowflake에 로드하는 함수.
    """
    logging.info("Load started")
    schema = Variable.get("SNOWFLAKE_SCHEMA")
    table = "name_gender"
    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};")  # 기존 데이터 삭제
        for r in records:
            name, gender = r
            sql = f"INSERT INTO {schema}.{table} (name, gender) VALUES ('{name}', '{gender}')"
            cur.execute(sql)
        cur.execute("COMMIT;")
    except Exception as error:
        logging.error(f"Error during load: {error}")
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()
        conn.close()  # 커넥션 닫기
    logging.info("Load done")

# ETL 함수
def etl():
    """
    S3에서 데이터를 추출하여 변환 후 Snowflake에 적재.
    """
    bucket_name = Variable.get("S3_BUCKET_NAME")  # Airflow Variables에서 S3 버킷 이름 가져오기
    file_key = Variable.get("S3_FILE_KEY")  # Airflow Variables에서 S3 파일 경로 가져오기

    # 데이터 추출
    data = extract_from_s3(bucket_name, file_key)
    # 데이터 변환
    records = transform(data)
    # 데이터 로드
    load(records)

# Airflow DAG 정의
dag_snowflake_etl_s3 = DAG(
    dag_id='etl_s3_to_snowflake_name_gender',
    catchup=False,
    start_date=datetime(2023, 4, 6),  # 과거 날짜로 설정
    schedule='0 2 * * *'  # 매일 새벽 2시 실행
)

# Airflow Task 정의
task = PythonOperator(
    task_id='perform_etl',
    python_callable=etl,
    dag=dag_snowflake_etl_s3
)