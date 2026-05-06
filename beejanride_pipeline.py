from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pendulum import datetime 
import subprocess
import time
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

# ======================
# CONFIG
# ======================

DBT_PROJECT_DIR = "/opt/airflow/dags/beejanride/beejan_ride"  
DBT_PROFILES_DIR = "/opt/airflow/.dbt"

DEFAULT_ARGS = {
    "owner": "beejanride-eng",
    "retries": 2,
    "retry_delay": timedelta(seconds=90),
}



def run_dbt(cmd):
    command = [
        "dbt",
        *cmd.split(),
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True
    )

    # Print logs to Airflow UI
    print(result.stdout)
    print(result.stderr)

    if result.returncode != 0:
        raise Exception(f"dbt failed: {cmd}")

def dbt_staging_task():
       run_dbt("run --select beejan_staging")

def dbt_intermediate_task():
        run_dbt("run --select intermediate_layer")

def dbt_marts_task():
        run_dbt("run --select beejan_marts")

def dbt_test_task():
     run_dbt("test")                   



# DAG
# ======================
with DAG(
    dag_id="beejanride_pipeline",
    start_date=datetime(2026, 4, 3),
    schedule="daily",
    catchup=True,   # enables backfills
    max_active_runs=2,
    default_args=DEFAULT_ARGS,
    tags=["Fraud", "operations", "finance"]
) as dag:

# 1. INGESTION

    airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="sync_airbyte",
        connection_id="bdd5e0db-4706-4458-b6cd-975339d9d4cb",
        airbyte_conn_id="airbyte_default",
        asynchronous=False,
        timeout=3600,
        wait_seconds=10,
    )


# 2. DBT WORKFLOW

    dbt_staging = BashOperator(
    task_id="dbt_staging",
    bash_command=f"dbt run --select beejan_staging --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_intermediate = BashOperator(
    task_id="dbt_intermediate",
    bash_command=f"dbt run --select intermediate_layer --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_marts = BashOperator(
    task_id="dbt_marts",
    bash_command=f"dbt run --select beejan_marts --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_tests = BashOperator(
    task_id="dbt_tests",
    bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}",
    )



    # DEPENDENCIES
     # ======================
    airbyte_sync >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_tests
