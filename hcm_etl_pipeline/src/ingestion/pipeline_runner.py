"""
src/ingestion/pipeline_runner.py
----------------------------------
Orchestrates the full Landing → Raw pipeline for one dataset.

Steps (in order):
  1. Manifest validation (file arrival, checksum, count, size)
  2. Schema enforcement (permissive load, type cast, NOT NULL)
  3. DQ checks (custom per-column business rules)
  4. Quarantine write (bad rows → /quarantine/)
  5. Raw write (valid rows → /valid/ with MD5 hash + audit cols)
  6. Alert on any quarantine events

Usage (called by Airflow SparkSubmitOperator):
  spark-submit src/ingestion/pipeline_runner.py \
    --dataset employee_master \
    --run_date 20240115 \
    --env prod

Or directly in Python (e.g. from tests):
  runner = PipelineRunner(config, "employee_master", "2024-01-15")
  result = runner.run()
"""

import argparse
import logging
import sys
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict

import yaml

from pyspark.sql import SparkSession

from src.ingestion.manifest_validator import ManifestValidator
from src.ingestion.schema_enforcer import SchemaEnforcer
from src.ingestion.dq_checker import DQChecker, build_checks_from_contract, Severity
from src.ingestion.quarantine_writer import QuarantineWriter
from src.ingestion.raw_writer import RawWriter
from src.common.logger import get_logger
from src.common.spark_session import get_spark_session
from src.common.config_loader import load_config, load_contract
from src.common.alert_notifier import AlertNotifier

logger = get_logger(__name__)


# ─── Run result ───────────────────────────────────────────────────────────────
@dataclass
class PipelineRunResult:
    dataset_name: str
    pipeline_run_id: str
    status: str = "UNKNOWN"   # SUCCESS | FAILED | PARTIAL
    total_records: int = 0
    valid_records: int = 0
    quarantined_records: int = 0
    errors: list = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"[{self.status}] {self.dataset_name} | run={self.pipeline_run_id} "
            f"| total={self.total_records} valid={self.valid_records} "
            f"quarantine={self.quarantined_records}"
        )


# ─── Pipeline runner ──────────────────────────────────────────────────────────
class PipelineRunner:
    """
    Full Landing → Raw pipeline for one dataset.

    Parameters
    ----------
    config       : environment config dict (from conf/env/*.yaml)
    contract     : data contract dict (from conf/data_contracts/*.yaml)
    dataset_name : e.g. employee_master
    run_date     : YYYY-MM-DD
    """

    def __init__(
        self,
        config: Dict[str, Any],
        contract: Dict[str, Any],
        dataset_name: str,
        run_date: str,
    ):
        self.config = config
        self.contract = contract
        self.dataset_name = dataset_name
        self.run_date = run_date
        self.run_date_compact = run_date.replace("-", "")  # YYYYMMDD for filenames
        self.pipeline_run_id = str(uuid.uuid4())[:8]

        self.spark = get_spark_session(dataset_name, config.get("spark", {}))
        self.alerter = AlertNotifier(config.get("alerts", {}))

        # Paths from config
        base = config["storage"]
        self.landing_path = f"{base['landing']}/{dataset_name}"
        self.raw_base_path = f"{base['raw']}/{dataset_name}"
        self.quarantine_base_path = f"{base['raw']}/{dataset_name}"

        # File names follow pattern: <dataset>_YYYYMMDD.csv
        self.csv_file = f"{self.landing_path}/{dataset_name}_{self.run_date_compact}.csv"
        self.manifest_file = f"{self.landing_path}/{dataset_name}_{self.run_date_compact}_manifest.json"
        self.filename_pattern = rf"^{dataset_name}_\d{{8}}\.csv$"

    def run(self) -> PipelineRunResult:
        result = PipelineRunResult(
            dataset_name=self.dataset_name,
            pipeline_run_id=self.pipeline_run_id,
        )

        logger.info("=" * 70)
        logger.info("PIPELINE START | %s | run_id=%s | dt=%s",
                    self.dataset_name, self.pipeline_run_id, self.run_date)
        logger.info("=" * 70)

        try:
            # ── Step 1: Manifest validation ──────────────────────────────────
            logger.info("[STEP 1] Manifest validation")
            manifest_result = ManifestValidator(
                file_path=self.csv_file,
                manifest_path=self.manifest_file,
                filename_pattern=self.filename_pattern,
            ).validate()

            if not manifest_result.passed:
                msg = f"Manifest validation failed: {manifest_result.errors}"
                logger.error(msg)
                self.alerter.send(
                    subject=f"[PIPELINE FAIL] {self.dataset_name} — Manifest validation",
                    body=msg,
                )
                result.status = "FAILED"
                result.errors.append(msg)
                return result

            # ── Step 2: Schema enforcement ───────────────────────────────────
            logger.info("[STEP 2] Schema enforcement")
            schema_result_valid_df, schema_rejected_df, schema_result = SchemaEnforcer(
                spark=self.spark,
                contract=self.contract,
                dataset_name=self.dataset_name,
            ).enforce(self.csv_file)

            if schema_result.has_blocking_issues():
                msg = f"Schema blocking issues: {schema_result.missing_mandatory}"
                logger.error(msg)
                self.alerter.send(
                    subject=f"[PIPELINE FAIL] {self.dataset_name} — Schema enforcement",
                    body=msg,
                )
                result.status = "FAILED"
                result.errors.append(msg)
                return result

            # Write schema-rejected rows to quarantine
            if schema_rejected_df.count() > 0:
                qw = QuarantineWriter(
                    quarantine_base_path=self.quarantine_base_path,
                    dataset_name=self.dataset_name,
                    run_date=self.run_date,
                    pipeline_run_id=self.pipeline_run_id,
                )
                schema_quarantine_count = qw.write(
                    schema_rejected_df,
                    source_file=self.csv_file,
                    rejection_layer="SCHEMA_ENFORCEMENT",
                )
                result.quarantined_records += schema_quarantine_count

            # ── Step 3: DQ checks ────────────────────────────────────────────
            logger.info("[STEP 3] DQ checks")
            dq_checks = build_checks_from_contract(self.contract.get("dq_rules", []))

            dq_checker = DQChecker(
                checks=dq_checks,
                dataset_name=self.dataset_name,
                soft_fail_threshold=self.config.get("dq", {}).get("soft_fail_threshold"),
            )
            annotated_df = dq_checker.run(schema_result_valid_df)

            # Split valid vs DQ-failed
            dq_valid_df = annotated_df.filter("validation_flag = 1")
            dq_failed_df = (
                annotated_df
                .filter("validation_flag = 0")
                .withColumnRenamed("dq_failed_checks", "rejection_reason")
                .withColumn("rejection_layer", __import__("pyspark.sql.functions", fromlist=["lit"]).lit("DQ_CHECK"))
            )

            result.total_records = annotated_df.count()
            result.valid_records = dq_valid_df.count()

            # ── Step 4: Quarantine DQ-failed rows ────────────────────────────
            if dq_failed_df.count() > 0:
                logger.info("[STEP 4] Writing DQ-failed rows to quarantine")
                qw = QuarantineWriter(
                    quarantine_base_path=self.quarantine_base_path,
                    dataset_name=self.dataset_name,
                    run_date=self.run_date,
                    pipeline_run_id=self.pipeline_run_id,
                )
                dq_quarantine_count = qw.write(
                    dq_failed_df,
                    source_file=self.csv_file,
                    rejection_layer="DQ_CHECK",
                )
                result.quarantined_records += dq_quarantine_count

                self.alerter.send(
                    subject=f"[DQ WARN] {self.dataset_name} — {dq_quarantine_count} rows quarantined",
                    body=f"run_id={self.pipeline_run_id} | dt={self.run_date} | quarantine={self.quarantine_base_path}",
                )

            # ── Step 5: Write valid rows to raw zone ─────────────────────────
            logger.info("[STEP 5] Writing valid rows to raw zone")
            raw_writer = RawWriter(
                raw_base_path=self.raw_base_path,
                dataset_name=self.dataset_name,
                run_date=self.run_date,
            )
            written = raw_writer.write(dq_valid_df, pipeline_run_id=self.pipeline_run_id)

            result.valid_records = written
            result.status = "SUCCESS" if result.quarantined_records == 0 else "PARTIAL"

            logger.info(result.summary())
            return result

        except Exception as e:
            logger.exception("[PIPELINE] Unhandled exception for %s", self.dataset_name)
            self.alerter.send(
                subject=f"[PIPELINE ERROR] {self.dataset_name}",
                body=str(e),
            )
            result.status = "FAILED"
            result.errors.append(str(e))
            return result

        finally:
            self.spark.stop()


# ─── CLI entry point ──────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Run Landing → Raw pipeline")
    parser.add_argument("--dataset", required=True, help="Dataset name e.g. employee_master")
    parser.add_argument("--run_date", required=True, help="Run date YYYY-MM-DD")
    parser.add_argument("--env", default="dev", help="Environment: dev|uat|prod")
    args = parser.parse_args()

    config = load_config(args.env)
    contract = load_contract(args.dataset, config)

    runner = PipelineRunner(
        config=config,
        contract=contract,
        dataset_name=args.dataset,
        run_date=args.run_date,
    )
    result = runner.run()

    if result.status == "FAILED":
        sys.exit(1)


if __name__ == "__main__":
    main()
