from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from datetime import datetime
import yfinance as yf
import logging
import snowflake.connector


def get_snowflake_connection():
    """
    Snowflake 연결 설정
    """
    conn = snowflake.connector.connect(
        user=Variable.get("SNOWFLAKE_USER"),
        password=Variable.get("SNOWFLAKE_PASSWORD"),
        account=Variable.get("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database=Variable.get("SNOWFLAKE_DBNAME"),
        schema=Variable.get("SNOWFLAKE_SCHEMA")
    )
    return conn.cursor()


@task
def get_historical_prices(symbol):
    """
    주식 데이터를 Yahoo Finance에서 가져옵니다.
    """
    ticket = yf.Ticker(symbol)
    data = ticket.history()
    records = []

    for index, row in data.iterrows():
        date = index.strftime('%Y-%m-%d %H:%M:%S')
        records.append([date, row["Open"], row["High"], row["Low"], row["Close"], row["Volume"]])

    return records


@task
def load(schema, table, records):
    """
    Snowflake에 데이터를 적재합니다.
    """
    logging.info("Load started")
    cur = get_snowflake_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
CREATE TABLE {schema}.{table} (
    date TIMESTAMP,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT
);
        """)
        logging.info(f"Table {schema}.{table} created successfully")

        # 데이터 삽입
        for r in records:
            sql = f"""
            INSERT INTO {schema}.{table} (date, open, high, low, close, volume)
            VALUES ('{r[0]}', {r[1]}, {r[2]}, {r[3]}, {r[4]}, {r[5]});
            """
            logging.info(f"Executing query: {sql}")
            cur.execute(sql)

        cur.execute("COMMIT;")
        logging.info(f"Data successfully loaded into {schema}.{table}")
    except Exception as error:
        logging.error(f"Error during load: {error}")
        cur.execute("ROLLBACK;")
        raise
    finally:
        cur.close()


with DAG(
    dag_id='UpdateSymbol_Snowflake',
    start_date=datetime(2023, 5, 30),
    catchup=False,
    tags=['API'],
    schedule='0 10 * * *'
) as dag:

    results = get_historical_prices("AAPL")
    load(Variable.get("SNOWFLAKE_SCHEMA"), "stock_info", results)