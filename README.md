BeejanRide wants the hail and ride platform to run automatically in production. Your next task is to extend the existing project by implementing orchestration with Apache Airflow.

This project implements orchestration with Apache Airflow to coordinate, manage and complete Beejanride platform ELT workflow without any manual intervention in production.

Project Objective
The main objective is to design and implement a production-grade orchestration layer for the BeejanRide platform using Airflow. The solution demonstrates:

Orchestration of Airbyte syncs
Trigger dbt runs and tests
Scheduling
Task dependencies
Failure handling
Retries
Monitoring
Backfills
Idempotency
Clear separation between ingestion, transformation, testing, and alerting
