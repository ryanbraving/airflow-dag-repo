"""GitHub ETL Pipeline - Running in Isolated K8s Pods

Each task runs in its own pod with isolated resources and dependencies.
Works with LocalExecutor - pods are scheduled by Airflow but run independently.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.hooks.base import BaseHook
from kubernetes.client import models as k8s

with DAG(
    dag_id='github_repos_etl_k8s',
    schedule='0 */6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['github', 'etl', 'kubernetes'],
) as dag:
    
    # Get credentials from Airflow connection at runtime
    try:
        conn = BaseHook.get_connection('postgres_connection')
        db_config = {
            'DB_HOST': conn.host,
            'DB_PORT': str(conn.port),
            'DB_NAME': conn.schema,
            'DB_USER': conn.login,
            'DB_PASSWORD': conn.password
        }
    except:
        # Fallback if connection not found during parse
        db_config = {}
    
    extract_task = KubernetesPodOperator(
        task_id='extract_github_repos',
        name='github-extract-pod',
        namespace='airflow',
        image='python:3.12-slim',
        cmds=['bash', '-c'],
        arguments=['''
            pip install requests && python3 << 'EOF'
import requests
import json
import time

repos = []
for page in range(1, 6):
    response = requests.get(
        "https://api.github.com/repositories",
        params={"since": page * 100, "per_page": 100},
        headers={"Accept": "application/vnd.github.v3+json"}
    )
    if response.status_code == 200:
        repos.extend(response.json())
        time.sleep(1)
    else:
        break

print(f"Extracted {len(repos)} repositories")
EOF
        '''],
        env_vars=db_config,
        get_logs=True,
        is_delete_operator_pod=True,
        do_xcom_push=False,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )
    
    transform_task = KubernetesPodOperator(
        task_id='transform_github_repos',
        name='github-transform-pod',
        namespace='airflow',
        image='python:3.12-slim',
        cmds=['bash', '-c'],
        arguments=['''
            python3 << 'EOF'
import requests
import json
from datetime import datetime
import time

# Fetch data
repos = []
for page in range(1, 6):
    response = requests.get(
        "https://api.github.com/repositories",
        params={"since": page * 100, "per_page": 100},
        headers={"Accept": "application/vnd.github.v3+json"}
    )
    if response.status_code == 200:
        repos.extend(response.json())
        time.sleep(1)
    else:
        break

# Transform
transformed = []
for repo in repos:
    transformed.append({
        "id": repo["id"],
        "name": repo["name"],
        "full_name": repo["full_name"],
        "owner_login": repo["owner"]["login"],
        "owner_type": repo["owner"]["type"],
        "description": repo.get("description", ""),
        "html_url": repo["html_url"],
        "created_at": repo.get("created_at"),
        "updated_at": repo.get("updated_at"),
        "language": repo.get("language"),
        "stargazers_count": repo.get("stargazers_count", 0),
        "forks_count": repo.get("forks_count", 0),
        "open_issues_count": repo.get("open_issues_count", 0),
        "is_fork": repo.get("fork", False),
        "is_private": repo.get("private", False),
        "processed_at": datetime.now().isoformat()
    })

print(f"Transformed {len(transformed)} repositories")
EOF
        '''],
        get_logs=True,
        is_delete_operator_pod=True,
        do_xcom_push=False,
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    
    load_task = KubernetesPodOperator(
        task_id='load_to_postgres',
        name='github-load-pod',
        namespace='airflow',
        image='python:3.12-slim',
        cmds=['bash', '-c'],
        arguments=['''
            pip install requests sqlalchemy psycopg2-binary && python3 << 'EOF'
import requests
import json
import os
import time
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert

Base = declarative_base()

class GitHubRepo(Base):
    __tablename__ = "github_repos"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    full_name = Column(String(255))
    owner_login = Column(String(255))
    owner_type = Column(String(50))
    description = Column(Text)
    html_url = Column(String(500))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    language = Column(String(100))
    stargazers_count = Column(Integer)
    forks_count = Column(Integer)
    open_issues_count = Column(Integer)
    is_fork = Column(Boolean)
    is_private = Column(Boolean)
    processed_at = Column(DateTime)

# Fetch data
repos = []
for page in range(1, 6):
    response = requests.get(
        "https://api.github.com/repositories",
        params={"since": page * 100, "per_page": 100},
        headers={"Accept": "application/vnd.github.v3+json"}
    )
    if response.status_code == 200:
        repos.extend(response.json())
        time.sleep(1)
    else:
        break

# Transform
transformed = []
for repo in repos:
    transformed.append({
        "id": repo["id"],
        "name": repo["name"],
        "full_name": repo["full_name"],
        "owner_login": repo["owner"]["login"],
        "owner_type": repo["owner"]["type"],
        "description": repo.get("description", ""),
        "html_url": repo["html_url"],
        "created_at": repo.get("created_at"),
        "updated_at": repo.get("updated_at"),
        "language": repo.get("language"),
        "stargazers_count": repo.get("stargazers_count", 0),
        "forks_count": repo.get("forks_count", 0),
        "open_issues_count": repo.get("open_issues_count", 0),
        "is_fork": repo.get("fork", False),
        "is_private": repo.get("private", False),
        "processed_at": datetime.now().isoformat()
    })

# Load to database
db_url = f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
engine = create_engine(db_url)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

try:
    for repo in transformed:
        stmt = insert(GitHubRepo).values(**repo)
        stmt = stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={
                "name": stmt.excluded.name,
                "description": stmt.excluded.description,
                "updated_at": stmt.excluded.updated_at,
                "stargazers_count": stmt.excluded.stargazers_count,
                "forks_count": stmt.excluded.forks_count,
                "open_issues_count": stmt.excluded.open_issues_count,
                "processed_at": stmt.excluded.processed_at
            }
        )
        session.execute(stmt)
    
    session.commit()
    print(f"Successfully loaded {len(transformed)} repositories")
except Exception as e:
    session.rollback()
    print(f"Error: {e}")
    raise
finally:
    session.close()
    engine.dispose()
EOF
        '''],
        env_vars=db_config,
        get_logs=True,
        is_delete_operator_pod=True,
        retries=3,
        retry_delay=timedelta(minutes=3),
    )
    
    extract_task >> transform_task >> load_task
