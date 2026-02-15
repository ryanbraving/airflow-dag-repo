from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import requests
import logging
from sqlalchemy import create_engine, Column, String, Float, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert

Base = declarative_base()

class CryptoPrice(Base):
    __tablename__ = 'crypto_prices'
    id = Column(String, primary_key=True)
    symbol = Column(String)
    price = Column(Float)
    market_cap = Column(BigInteger)

API_URL = "https://api.coingecko.com/api/v3/coins/markets"

def extract():
    logging.info("Extracting data from API")
    response = requests.get(API_URL, params={"vs_currency": "usd"})
    response.raise_for_status()
    return response.json()

def transform(**context):
    logging.info("Transforming data")
    data = context['ti'].xcom_pull(task_ids='extract')
    cleaned = []
    for coin in data:
        cleaned.append({
            "id": coin["id"],
            "symbol": coin["symbol"],
            "price": coin["current_price"],
            "market_cap": coin["market_cap"]
        })
    return cleaned

def load(**context):
    logging.info("Loading data into Postgres")
    data = context['ti'].xcom_pull(task_ids='transform')
    
    conn = BaseHook.get_connection('postgres_uri')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    Session = sessionmaker(bind=engine)
    session = Session()
    
    for row in data:
        stmt = insert(CryptoPrice).values(**row)
        stmt = stmt.on_conflict_do_update(
            index_elements=['id'],
            set_={'price': stmt.excluded.price, 'market_cap': stmt.excluded.market_cap}
        )
        session.execute(stmt)
    
    session.commit()
    session.close()

with DAG(
    dag_id='coin_gecko',
    schedule='0 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    tags=['crypto', 'etl'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )
    
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    
    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    
    extract_task >> transform_task >> load_task
