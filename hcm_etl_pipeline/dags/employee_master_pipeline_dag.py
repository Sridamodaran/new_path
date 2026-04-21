"""
dags/employee_master_pipeline_dag.py
--------------------------------------
Airflow DAG for the employee_master Landing → Raw → Staging pipeline.

Trigger: FileSensor watches /data/landing/employee_master/ every 5 minutes.
         Once both the CSV and manifest JSON arrive, the pipeline kicks off.

DAG tasks:
  1. sense_file          — FileSensor: waits for CSV + manifest
  2. validate_manifest   — PythonOperator: manifest validation
  3. spark_raw_ingestion — SparkSubmitOperator: Landing → Raw
  4. spark_staging       — SparkSubmitOperator: Raw → Staging (SCD2)
  5. reconciliation      — PythonOperator: dept-wise count + email report
  6. alert_on_failure    — triggered by on_failure_callback
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable


# ─── Default args ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["de-alerts@myorg.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

# ─── Config from Airflow Variables (set per env in Airflow UI) ────────────────
ENV = Variable.get("PIPELINE_ENV", default_var="dev")
RUN_DATE = "{{ ds }}"                          # Airflow logical date as YYYY-MM-DD
RUN_DATE_COMPACT = "{{ ds_nodash }}"           # YYYYMMDD for filenames
LANDING_PATH = Variable.get("LANDING_BASE_PATH", default_var="/data/landing")
SPARK_CONN_ID = Variable.get("SPARK_CONN_ID", default_var="spark_default")
DATASET = "employee_master"


# ─── DAG ──────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="employee_master_pipeline",
    default_args=default_args,
    description="Employee master: Landing → Raw → Staging with SCD2",
    schedule_interval="0 2 * * *",   # 2 AM daily — upstream delivers by 1 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,               # Prevent concurrent runs on same dataset
    tags=["hcm", "employee", "raw", "staging"],
) as dag:

    # ── Task 1: File Sensor ────────────────────────────────────────────────────
    # Waits for the CSV file to land. Polls every 5 minutes, times out after 6h.
    sense_csv = FileSensor(
        task_id="sense_csv_file",
        filepath=f"{LANDING_PATH}/{DATASET}/{DATASET}_{RUN_DATE_COMPACT}.csv",
        poke_interval=300,        # 5 minutes
        timeout=60 * 60 * 6,     # 6 hours max wait
        mode="reschedule",        # release worker slot between polls
        fs_conn_id="fs_default",
    )

    sense_manifest = FileSensor(
        task_id="sense_manifest_file",
        filepath=f"{LANDING_PATH}/{DATASET}/{DATASET}_{RUN_DATE_COMPACT}_manifest.json",
        poke_interval=300,
        timeout=60 * 60 * 6,
        mode="reschedule",
        fs_conn_id="fs_default",
    )

    # ── Task 2: Manifest validation (Python, no Spark needed) ─────────────────
    def validate_manifest_task(**context):
        """
        Run manifest validation before spinning up Spark.
        Raises AirflowException on failure to stop the DAG cleanly.
        """
        from airflow.exceptions import AirflowException
        from src.ingestion.manifest_validator import ManifestValidator

        run_date_compact = context["ds_nodash"]
        file_path = f"{LANDING_PATH}/{DATASET}/{DATASET}_{run_date_compact}.csv"
        manifest_path = f"{LANDING_PATH}/{DATASET}/{DATASET}_{run_date_compact}_manifest.json"

        result = ManifestValidator(
            file_path=file_path,
            manifest_path=manifest_path,
            filename_pattern=rf"^{DATASET}_\d{{8}}\.csv$",
        ).validate()

        if not result.passed:
            raise AirflowException(
                f"Manifest validation failed for {DATASET}: {result.errors}"
            )

        context["ti"].xcom_push(key="manifest_status", value="PASSED")

    validate_manifest = PythonOperator(
        task_id="validate_manifest",
        python_callable=validate_manifest_task,
    )

    # ── Task 3: Spark — Landing → Raw ─────────────────────────────────────────
    spark_raw_ingestion = SparkSubmitOperator(
        task_id="spark_raw_ingestion",
        application="src/ingestion/pipeline_runner.py",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--dataset", DATASET,
            "--run_date", RUN_DATE,
            "--env", ENV,
        ],
        conf={
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.sql.shuffle.partitions": "50",
            "spark.executor.memory": "4g",
            "spark.driver.memory": "2g",
            "spark.dynamicAllocation.enabled": "true",
        },
        jars="",   # Add Hive/HDFS jars if needed
        name=f"{DATASET}_raw_ingestion_{RUN_DATE_COMPACT}",
        verbose=True,
    )

    # ── Task 4: Spark — Raw → Staging (SCD2) ──────────────────────────────────
    spark_staging = SparkSubmitOperator(
        task_id="spark_staging",
        application="src/staging/staging_runner.py",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--dataset", DATASET,
            "--run_date", RUN_DATE,
            "--env", ENV,
        ],
        conf={
            "spark.sql.parquet.compression.codec": "snappy",
            "spark.sql.orc.compression.codec": "snappy",
            "spark.sql.shuffle.partitions": "50",
            "spark.executor.memory": "8g",
            "spark.driver.memory": "4g",
        },
        name=f"{DATASET}_staging_{RUN_DATE_COMPACT}",
    )

    # ── Task 5: Reconciliation ─────────────────────────────────────────────────
    def reconciliation_task(**context):
        """
        Dept-wise count comparison between raw and staging.
        Sends email report.
        """
        from src.reconciliation.audit_checker import AuditChecker
        from src.common.config_loader import load_config

        config = load_config(ENV)
        checker = AuditChecker(config, DATASET, context["ds"])
        report = checker.run()
        context["ti"].xcom_push(key="recon_report", value=report)

    reconciliation = PythonOperator(
        task_id="reconciliation",
        python_callable=reconciliation_task,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Task 6: Alert on failure ───────────────────────────────────────────────
    def failure_alert(context):
        """Called by on_failure_callback on any task failure."""
        from src.common.alert_notifier import AlertNotifier
        from src.common.config_loader import load_config

        config = load_config(ENV)
        alerter = AlertNotifier(config.get("alerts", {}))
        task_id = context.get("task_instance").task_id
        alerter.send(
            subject=f"[AIRFLOW FAIL] {DATASET} | task={task_id} | dt={context['ds']}",
            body=str(context.get("exception", "Unknown error")),
        )

    # Attach failure callback to all tasks
    for task in [validate_manifest, spark_raw_ingestion, spark_staging, reconciliation]:
        task.on_failure_callback = failure_alert

    # ── Task dependencies ──────────────────────────────────────────────────────
    [sense_csv, sense_manifest] >> validate_manifest >> spark_raw_ingestion >> spark_staging >> reconciliation
