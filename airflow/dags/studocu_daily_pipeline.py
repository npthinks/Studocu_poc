"""
Studocu Daily Event Pipeline
============================

Orchestrates the daily ETL flow for OpenSearch events from S3 bronze
through validation, transformation, and dbt modeling.

Flow:
    Glue ETL job (bronze -> silver + rejected)
    -> Glue Crawler (refresh catalog)
    -> dbt run (build staging + gold mart models)
    -> dbt test (run 11 data quality tests)
    -> notify on failure (Slack + email)

Schedule: daily at 02:00 UTC (after OpenSearch nightly export lands)
Owner: data-engineering
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.operators.bash import BashOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule


# ----------------------------------------------------------------------
# Default args
# ----------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-eng-alerts@studocu.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="studocu_daily_event_pipeline",
    description="Daily ingestion and modeling of OpenSearch events from S3",
    default_args=default_args,
    start_date=datetime(2026, 6, 1),
    schedule_interval="0 2 * * *",   # daily at 02:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["studocu", "events", "daily", "production"],
) as dag:

    # ------------------------------------------------------------------
    # 1. Run the Glue ETL job: bronze -> silver + rejected
    # ------------------------------------------------------------------
    glue_etl = GlueJobOperator(
        task_id="glue_bronze_to_silver",
        job_name="studocu-bronze-to-silver",
        region_name="eu-west-1",
        iam_role_name="studocu-poc-glue-role",
        script_args={
            "--source_path": "s3://studocu-poc-events-nishanth/bronze/",
            "--silver_path": "s3://studocu-poc-events-nishanth/silver/",
            "--rejected_path": "s3://studocu-poc-events-nishanth/rejected/",
        },
        wait_for_completion=False,   # let the sensor handle waiting
    )

    # ------------------------------------------------------------------
    # 2. Wait for Glue to finish, fail the DAG if Glue fails
    # ------------------------------------------------------------------
    wait_for_glue = GlueJobSensor(
        task_id="wait_for_glue_completion",
        job_name="studocu-bronze-to-silver",
        run_id="{{ task_instance.xcom_pull(task_ids='glue_bronze_to_silver', key='run_id') }}",
        aws_conn_id="aws_default",
    )

    # ------------------------------------------------------------------
    # 3. Refresh the Glue Catalog so new partitions become queryable
    # ------------------------------------------------------------------
    refresh_catalog = GlueCrawlerOperator(
        task_id="refresh_silver_catalog",
        config={"Name": "studocu-silver-crawler"},
        region_name="eu-west-1",
    )

    # ------------------------------------------------------------------
    # 4. Run dbt models (staging + mart)
    #    Production: replace BashOperator with Cosmos DbtTaskGroup
    #    so each dbt model becomes its own Airflow task
    # ------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/dbt/studocu_dbt && "
            "dbt run --profiles-dir /opt/dbt --target prod"
        ),
    )

    # ------------------------------------------------------------------
    # 5. Run dbt tests (not_null, unique, accepted_values, etc.)
    # ------------------------------------------------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/dbt/studocu_dbt && "
            "dbt test --profiles-dir /opt/dbt --target prod"
        ),
    )

    # ------------------------------------------------------------------
    # 6. Alert on failure (only runs if anything upstream failed)
    # ------------------------------------------------------------------
    notify_failure = SlackWebhookOperator(
        task_id="notify_failure_slack",
        http_conn_id="slack_webhook",
        message=(
            ":rotating_light: *Studocu daily event pipeline FAILED* "
            "for run {{ ds }}. "
            "Check Airflow logs: {{ ti.log_url }}"
        ),
        channel="#data-eng-alerts",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ------------------------------------------------------------------
    # Task dependencies
    # ------------------------------------------------------------------
    glue_etl >> wait_for_glue >> refresh_catalog >> dbt_run >> dbt_test
    [glue_etl, wait_for_glue, refresh_catalog, dbt_run, dbt_test] >> notify_failure