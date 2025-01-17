from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import requests
import snowflake.connector


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
        warehouse="COMPUTE_WH",
        database=Variable.get("SNOWFLAKE_DBNAME"),
        schema=Variable.get("SNOWFLAKE_SCHEMA")
    )
    logging.info("Connected to Snowflake")
    return conn.cursor()


def extract(**context):
    """
    URL에서 데이터를 추출합니다.
    """
    link = context["params"]["url"]
    task_instance = context['task_instance']
    execution_date = context['execution_date']

    logging.info(f"Executing extract task at {execution_date}")
    f = requests.get(link)
    return f.text


def transform(**context):
    """
    데이터를 변환합니다.
    """
    logging.info("Transform started")
    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    lines = text.strip().split("\n")[1:]  # 첫 번째 헤더 라인 제외
    records = []
    for line in lines:
        name, gender = line.split(",")
        records.append((name, gender))
    logging.info("Transform ended")
    return records


def load(**context):
    """
    Snowflake에 데이터를 로드합니다.
    """
    logging.info("Load started")
    schema = Variable.get("SNOWFLAKE_SCHEMA")
    table = context["params"]["table"]

    records = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")

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
        raise
    finally:
        cur.close()
    logging.info("Load done")


# Airflow DAG 정의
dag = DAG(
    dag_id='name_gender_snowflake_v4',
    start_date=datetime(2023, 4, 6),
    schedule='0 2 * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)


# PythonOperator Task 정의
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    params={
        'url': Variable.get("csv_url")
    },
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
    params={
        'table': 'name_gender'  # Snowflake 테이블 이름
    },
    dag=dag
)

# Task 의존성 설정
extract_task >> transform_task >> load_task