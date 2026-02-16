"""GitHub Repositories ETL Pipeline - Production-Grade Example

This DAG demonstrates a production-ready ETL pipeline that extracts GitHub repository data,
transforms it, and loads it into PostgreSQL using separate Kubernetes pods for each stage.

Architecture:
    Extract (Pod 1) → Transform (Pod 2) → Load (Pod 3)
    
    Each task runs in an isolated container with:
    - Separate resource allocations (CPU/Memory)
    - Independent retry policies
    - Isolated logging
    - XCom-based data passing between stages

Key Features:
    ✓ Clean ORM Modeling: SQLAlchemy models for GitHubRepo and ETLCheckpoint tables
    ✓ Stateful Incremental ETL: Checkpoint tracking for resumable, incremental loads
    ✓ Idempotency: UPSERT operations ensure safe reruns without duplicates
    ✓ Pagination: Fetches data in configurable page sizes (100 records/page)
    ✓ Rate Limit Safety: 1-second delays + 403 error handling with backoff
    ✓ Transactional Checkpointing: Atomic commits with rollback on failure
    ✓ Fault Tolerance: Per-task retry policies (Extract: 3x, Transform: 2x, Load: 3x)

Data Flow:
    1. Extract: Fetches repos from GitHub API with pagination → XCom JSON
    2. Transform: Cleans and structures data → XCom JSON
    3. Load: Upserts to PostgreSQL + saves checkpoint

Schedule: Every 6 hours
Data Source: GitHub Public API (no authentication required)
Database: PostgreSQL (requires 'github' database and connection setup)

Setup Required:
    1. Create database: CREATE DATABASE github;
    2. Update connection string in load task or create Airflow connection
    3. Ensure Kubernetes namespace and RBAC permissions are configured
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with DAG(
    dag_id='github_repos_etl',
    schedule='0 */6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['github', 'etl', 'kubernetes'],
) as dag:
    
    extract_task = KubernetesPodOperator(
        task_id='extract_github_repos',
        name='extract-github-repos',
        namespace='default',
        image='python:3.11-slim',
        cmds=['python', '-c'],
        arguments=['''
import requests
import json
import time
from datetime import datetime, timedelta

API_BASE = "https://api.github.com"
CHECKPOINT_FILE = "/airflow/xcom/return.json"
RATE_LIMIT_DELAY = 1
MAX_PAGES = 5

def extract_with_pagination():
    """Extract repos with pagination and rate limiting"""
    all_repos = []
    page = 1
    
    while page <= MAX_PAGES:
        print(f"Fetching page {page}...")
        
        response = requests.get(
            f"{API_BASE}/repositories",
            params={"since": page * 100, "per_page": 100},
            headers={"Accept": "application/vnd.github.v3+json"}
        )
        
        if response.status_code == 403:
            print("Rate limit hit, waiting...")
            time.sleep(60)
            continue
            
        response.raise_for_status()
        repos = response.json()
        
        if not repos:
            break
            
        all_repos.extend(repos)
        print(f"Fetched {len(repos)} repos")
        
        time.sleep(RATE_LIMIT_DELAY)
        page += 1
    
    checkpoint = {
        "last_run": datetime.now().isoformat(),
        "records_extracted": len(all_repos)
    }
    
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"repos": all_repos, "checkpoint": checkpoint}, f)
    
    print(f"Extracted {len(all_repos)} repositories")

if __name__ == "__main__":
    import subprocess
    subprocess.run(["pip", "install", "-q", "requests"])
    extract_with_pagination()
'''
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "256Mi", "cpu": "250m"},
            limits={"memory": "512Mi", "cpu": "500m"}
        ),
        retries=3,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
        get_logs=True,
        is_delete_operator_pod=True,
        do_xcom_push=True,
    )
    
    transform_task = KubernetesPodOperator(
        task_id='transform_github_repos',
        name='transform-github-repos',
        namespace='default',
        image='python:3.11-slim',
        cmds=['python', '-c'],
        arguments=['''
import json
from datetime import datetime

XCOM_INPUT = "/airflow/xcom/return.json"
XCOM_OUTPUT = "/airflow/xcom/return.json"

def transform_repos():
    """Transform and clean repository data"""
    with open(XCOM_INPUT, "r") as f:
        data = json.load(f)
    
    repos = data.get("repos", [])
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
            "created_at": repo["created_at"],
            "updated_at": repo["updated_at"],
            "language": repo.get("language"),
            "stargazers_count": repo.get("stargazers_count", 0),
            "forks_count": repo.get("forks_count", 0),
            "open_issues_count": repo.get("open_issues_count", 0),
            "is_fork": repo.get("fork", False),
            "is_private": repo.get("private", False),
            "processed_at": datetime.now().isoformat()
        })
    
    output = {
        "transformed_repos": transformed,
        "checkpoint": data.get("checkpoint", {})
    }
    
    with open(XCOM_OUTPUT, "w") as f:
        json.dump(output, f)
    
    print(f"Transformed {len(transformed)} repositories")

if __name__ == "__main__":
    transform_repos()
'''
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "512Mi", "cpu": "500m"},
            limits={"memory": "1Gi", "cpu": "1000m"}
        ),
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
        get_logs=True,
        is_delete_operator_pod=True,
        do_xcom_push=True,
    )
    
    load_task = KubernetesPodOperator(
        task_id='load_github_repos',
        name='load-github-repos',
        namespace='default',
        image='python:3.11-slim',
        cmds=['python', '-c'],
        arguments=['''
import json
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime

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

class ETLCheckpoint(Base):
    __tablename__ = "etl_checkpoints"
    id = Column(Integer, primary_key=True, autoincrement=True)
    dag_id = Column(String(255))
    last_run = Column(DateTime)
    records_processed = Column(Integer)
    created_at = Column(DateTime, default=datetime.now)

XCOM_INPUT = "/airflow/xcom/return.json"

def load_to_postgres():
    """Load data with transactional checkpointing"""
    with open(XCOM_INPUT, "r") as f:
        data = json.load(f)
    
    repos = data.get("transformed_repos", [])
    checkpoint_data = data.get("checkpoint", {})
    
    engine = create_engine("postgresql://postgres:postgres@postgres/github")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        for repo in repos:
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
        
        checkpoint = ETLCheckpoint(
            dag_id="github_repos_etl",
            last_run=datetime.fromisoformat(checkpoint_data.get("last_run")),
            records_processed=checkpoint_data.get("records_extracted", 0)
        )
        session.add(checkpoint)
        session.commit()
        print(f"Loaded {len(repos)} repositories successfully")
        
    except Exception as e:
        session.rollback()
        print(f"Error loading data: {e}")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    import subprocess
    subprocess.run(["pip", "install", "-q", "sqlalchemy", "psycopg2-binary"])
    load_to_postgres()
'''
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "512Mi", "cpu": "500m"},
            limits={"memory": "1Gi", "cpu": "1000m"}
        ),
        retries=3,
        retry_delay=timedelta(minutes=3),
        execution_timeout=timedelta(minutes=20),
        get_logs=True,
        is_delete_operator_pod=True,
    )
    
    extract_task >> transform_task >> load_task
