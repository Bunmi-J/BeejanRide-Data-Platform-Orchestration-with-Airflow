# BeejanRide Automation


BeejanRide wants the hail and ride platform to run automatically in production. Your next task is to extend the existing project by implementing orchestration with Apache Airflow.

This project implements orchestration with Apache Airflow to coordinate, manage and complete Beejanride platform ELT workflow without any manual intervention in production.

## Project Objective
The main objective is to design and implement a production-grade orchestration layer for the BeejanRide platform using Airflow. The solution demonstrates:

 * Orchestration of Airbyte syncs
 * Trigger dbt runs and tests
 * Scheduling
 * Task dependencies
 * Failure handling
 * Retries
 * Monitoring
 * Backfills
 * Idempotency
 * Clear separation between ingestion, transformation, testing, and alerting

BeejanRide Orcherstration Workflow:
 * Airbyte sync → sync airbyte data with Airflow this enables automation of the data synchronization. interaction with airbyte directly Create order
 * Beejan_staging → Automation task to standardardize and transform raw data into clean models to ensure consistent building block for the downstream analytics.
 * Intermediate → Automation stage for managing and scheduling business logic are implemented by building on the staging models to create reuseable tranformation logics
 * Marts → Automation task for managing and scheduling dimension and fact tables deisgned for business intelligence tools (Power BI, Tableau), Analysts and dashboards
 * Test  → Automation task to ascertain that the tranformed data adheres to business rules, logics while enhancing data integrity and high quality data that meets desired business objectives.


## With Orchestration:

The Apache Airflow, calls each task in order.
If the data ingestion and sychrononization fails, the sync_airbyte, it triggers an error message and the upstreams which include the staging, intermediate, marts and test will all fail.
Updated architecture diagram

## SUCCESSFUL DAG 
<img width="607" height="98" alt="image" src="https://github.com/user-attachments/assets/4f1a6dc5-634c-49ec-82ed-93e4ef8db283" />

## DBT TEST FAILED DAG
<img width="614" height="191" alt="image" src="https://github.com/user-attachments/assets/07b9185d-94af-4057-b423-33bb1361e9e4" />

 * Failure in test completed_trip_successful_payment Got 1 result, configured to fail if != 0
   This is not a system failure the pipeline is actually working correctly. 
   This is just a dbt test doing its job and catching bad data. Every completed_trip must have a successful_payment but if there is no payment received, then an error is      raised.

## DBT TEST FAILED DAG RUN LOG 
[2026-05-05 22:44:47] INFO - 21:44:47  Finished running 93 data tests in 0 hours 0 minutes and 31.35 seconds (31.35s).
[2026-05-05 22:44:47] INFO - 21:44:47
[2026-05-05 22:44:47] INFO - 21:44:47  Completed with 1 error, 0 partial successes, and 0 warnings:
[2026-05-05 22:44:47] INFO - 21:44:47
[2026-05-05 22:44:47] INFO - 21:44:47  Failure in test completed_trip_successful_payment (tests/completed_trip_successful_payment.sql)
[2026-05-05 22:44:47] INFO - 21:44:47    Got 1 result, configured to fail if != 0
[2026-05-05 22:44:47] INFO - 21:44:47
[2026-05-05 22:44:47] INFO - 21:44:47    compiled code at target/compiled/beejan_ride/tests/completed_trip_successful_payment.sql
[2026-05-05 22:44:47] INFO - 21:44:47
[2026-05-05 22:44:47] INFO - 21:44:47  Done. PASS=92 WARN=0 ERROR=1 SKIP=0 NO-OP=0 TOTAL=93
[2026-05-05 22:44:49] INFO - Command exited with return code 1
[2026-05-05 22:44:49] ERROR - Task failed with exception
AirflowException: Bash command failed. The command returned a non-zero exit code 1.
File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 1112 in run

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/execution_time/task_runner.py", line 1528 in _execute_task

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/sdk/bases/operator.py", line 417 in wrapper

File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/standard/operators/bash.py", line 226 in execute

[2026-05-05 22:44:49] INFO - Task instance in failure state
[2026-05-05 22:44:49] INFO - Task start
[2026-05-05 22:44:49] INFO - Task:<Task(BashOperator): dbt_tests>
[2026-05-05 22:44:49] INFO - Failure caused by Bash command failed. The command returned a non-zero exit code 1.

## Explanation of How idempotency is Maintained
BeejanRide workflow are designed to be idempotent, meaning that running the task multiple times with the same input yields the same result. This is crucial for re-running DAGs without altering the final output.
For this project idempotency is maitained by splitting a task into separate tasks , ensuring that the failure of one task does not impact the other.
Tasks should is designed to be atomic to avoid partial completion and subsequent downstream errors. This is achieved by splitting a task into separate tasks, ensuring that the failure of one does not impact the other.
The orcherstration design supports retries and backfills, allowing for the resumption of tasks after failures. This is done by setting the retries parameter to the task's Operator or including it in the DAG's default_args object. 

