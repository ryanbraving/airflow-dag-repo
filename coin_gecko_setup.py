from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import logging
from sqlalchemy import create_engine, text

def create_database():
    logging.info("Checking and creating database if needed")
    conn = BaseHook.get_connection('postgres_coin')
    admin_engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/postgres')
    with admin_engine.connect() as admin_conn:
        admin_conn.execute(text("COMMIT"))
        result = admin_conn.execute(text("SELECT 1 FROM pg_database WHERE datname = 'coin'"))
        if not result.fetchone():
            admin_conn.execute(text("CREATE DATABASE coin"))
            logging.info("Database 'coin' created")
        else:
            logging.info("Database 'coin' already exists")
    admin_engine.dispose()

def create_table():
    logging.info("Checking and creating table if needed")
    conn = BaseHook.get_connection('postgres_coin')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    with engine.connect() as conn:
        result = conn.execute(text("SELECT to_regclass('crypto_prices')"))
        if result.fetchone()[0] is None:
            conn.execute(text("""
                CREATE TABLE crypto_prices (
                    id VARCHAR PRIMARY KEY,
                    symbol VARCHAR,
                    price FLOAT,
                    market_cap BIGINT
                )
            """))
            conn.commit()
            logging.info("Table 'crypto_prices' created")
        else:
            logging.info("Table 'crypto_prices' already exists")
    engine.dispose()

with DAG(
    dag_id='coin_gecko_setup',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['crypto', 'setup'],
) as dag:
    
    create_db_task = PythonOperator(
        task_id='create_database',
        python_callable=create_database,
    )
    
    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )
    
    create_db_task >> create_table_task