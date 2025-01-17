from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import datetime, timedelta
import snowflake.connector
import logging
import requests


def get_snowflake_connection():
    """
    Snowflake 연결 설정
    """
    conn = snowflake.connector.connect(
        user=Variable.get("SNOWFLAKE_USER"),
        password=Variable.get("SNOWFLAKE_PASSWORD"),
        account=Variable.get("SNOWFLAKE_ACCOUNT"),
        warehouse=Variable.get("SNOWFLAKE_WAREHOUSE"),
        database=Variable.get("SNOWFLAKE_DBNAME"),
        schema=Variable.get("SNOWFLAKE_SCHEMA")
    )
    return conn.cursor()


@task
def extract(url):
    logging.info(f"Extracting data from URL: {url}")
    f = requests.get(url)
    logging.info("Data extracted successfully")
    return f.text


@task
def transform(text):
    lines = text.strip().split("\n")[1:]  # 첫 번째 헤더 라인을 제외
    records = []
    for l in lines:
        name, gender = l.split(",")  # CSV 데이터 파싱
        records.append((name, gender))
    logging.info("Transformation complete")
    return records


@task
def load(schema, table, records):
    """
    Snowflake로 데이터 적재
    """
    logging.info("Loading data into Snowflake")
    cur = get_snowflake_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};")  # 테이블 초기화
        for name, gender in records:
            sql = f"INSERT INTO {schema}.{table} (name, gender) VALUES (%s, %s)"
            cur.execute(sql, (name, gender))
        cur.execute("COMMIT;")
        logging.info("Data successfully loaded into Snowflake")
    except Exception as e:
        logging.error(f"Error while loading data: {e}")
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()


with DAG(
    dag_id='namegender_snowflake_v5',
    start_date=datetime(2022, 10, 6),  # 날짜가 미래인 경우 실행이 안됨
    schedule='0 2 * * *',  # 매일 새벽 2시 실행
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    url = Variable.get("csv_url")
    schema = Variable.get("SNOWFLAKE_SCHEMA")
    table = Variable.get("SNOWFLAKE_TABLE_NAME")

    # Task Pipeline
    lines = transform(extract(url))
    load(schema, table, lines)