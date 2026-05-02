from airflow import DAG
#from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import time
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator


# CONFIG
# ======================
#AIRBYTE_URL = "http://host.docker.internal:8000" 

DBT_PROJECT_DIR = "/opt/airflow/dags/beejanride"  
DBT_PROFILES_DIR = "/opt/airflow/.dbt"

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}

# DAG
# ======================
with DAG(
    dag_id="beejanride_pipeline",
    start_date=datetime(2026, 3, 3),
    schedule="@daily",
    catchup=True,   # enables backfills
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["Fraud", "operations", "finance"]
) as dag:


    airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="sync_airbyte",
        connection_id="0ba680c2-9f69-482b-9ada-1c3e6b5a2433",
        airbyte_conn_id="airbyte_default",
        asynchronous=False,
        timeout=3600,
        wait_seconds=10,
    )

# DBT
# ======================
def run_dbt(cmd):
    full_cmd = f"cd {DBT_PROJECT_DIR} && dbt {cmd} --profiles-dir {DBT_PROFILES_DIR}"
    result = subprocess.run(full_cmd, shell=True)

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

    # 1. INGESTION (Airbyte)
    # ======================
    with TaskGroup("ingestion") as ingestion:

        for conn_id in AIRBYTE_CONNECTION_IDS:

            PythonOperator(
                task_id=f"sync_{conn_id[:6]}",
                python_callable=sync_airbyte,
                op_kwargs={"connection_id": conn_id},
            )

    
    # 2. STAGING
    # ======================
    with TaskGroup("staging") as staging:

        dbt_staging = PythonOperator(
            task_id="dbt_staging",
            python_callable=dbt_staging_task,
        )
    # 3. INTERMEDIATE
    # ======================
    with TaskGroup("intermediate") as intermediate:

        dbt_intermediate = PythonOperator(
            task_id="dbt_intermediate",
            python_callable=dbt_intermediate_task,
        )

   
    # 4. MARTS
    # ======================
    with TaskGroup("marts") as marts:

        dbt_marts = PythonOperator(
            task_id="dbt_marts",
            python_callable=dbt_marts_task,
        )

    
    # 5. TESTING
    # ======================
    with TaskGroup("testing") as testing:

        dbt_tests = PythonOperator(
            task_id="dbt_tests",
            python_callable=dbt_test_task,
        )

    # DEPENDENCIES
    # ======================
     ingestion >> staging >> intermediate >> marts >> testing
