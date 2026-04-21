"""
src/ingestion/quarantine_writer.py
------------------------------------
Writes rejected rows to the quarantine zone with a metadata sidecar.

Quarantine path structure:
  /data/raw/<dataset>/<dt>/quarantine/
    ├── part-00000.parquet       ← bad rows
    └── quarantine_meta.json     ← why they failed, counts, pipeline context
"""

import json
import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class QuarantineWriter:
    """
    Writes rejected rows to the quarantine partition and emits a metadata JSON.

    Parameters
    ----------
    quarantine_base_path : e.g. /data/raw/employee_master
    dataset_name         : e.g. employee_master
    run_date             : YYYY-MM-DD partition date
    pipeline_run_id      : Airflow run_id or UUID for traceability
    """

    def __init__(
        self,
        quarantine_base_path: str,
        dataset_name: str,
        run_date: str,
        pipeline_run_id: str,
    ):
        self.quarantine_path = f"{quarantine_base_path}/dt={run_date}/quarantine"
        self.meta_path = f"{self.quarantine_path}/quarantine_meta.json"
        self.dataset_name = dataset_name
        self.run_date = run_date
        self.pipeline_run_id = pipeline_run_id

    def write(
        self,
        rejected_df: DataFrame,
        source_file: str,
        rejection_layer: str = "DQ_CHECK",
    ) -> int:
        """
        Write rejected rows to quarantine. Returns count of quarantined rows.
        If rejected_df is empty, no-op.
        """
        count = rejected_df.count()
        if count == 0:
            logger.info("[QUARANTINE] No rows to quarantine for %s", self.dataset_name)
            return 0

        # Add quarantine metadata columns
        enriched = (
            rejected_df
            .withColumn("quarantine_ts", F.current_timestamp())
            .withColumn("pipeline_run_id", F.lit(self.pipeline_run_id))
            .withColumn("source_file", F.lit(source_file))
            .withColumn("rejection_layer", F.lit(rejection_layer))
        )

        logger.info(
            "[QUARANTINE] Writing %d rows to %s",
            count, self.quarantine_path
        )

        enriched.write.mode("overwrite").parquet(self.quarantine_path)

        # Write metadata JSON sidecar
        # Count failures by reason
        reason_counts = (
            rejected_df
            .groupBy("rejection_reason")
            .count()
            .collect()
        )
        reason_map = {row["rejection_reason"]: row["count"] for row in reason_counts}

        metadata = {
            "dataset_name": self.dataset_name,
            "run_date": self.run_date,
            "pipeline_run_id": self.pipeline_run_id,
            "source_file": source_file,
            "rejection_layer": rejection_layer,
            "total_quarantined": count,
            "quarantined_at": datetime.now().isoformat(),
            "quarantine_path": self.quarantine_path,
            "rejection_reasons": reason_map,
            "action_required": "Review quarantine path and notify upstream for redelivery.",
        }

        # Write via Python (meta file doesn't need Spark)
        try:
            with open(self.meta_path.replace("hdfs://", ""), "w") as f:
                json.dump(metadata, f, indent=2)
        except Exception as e:
            logger.warning("[QUARANTINE] Could not write local meta JSON: %s", e)
            # In real HDFS env, use spark to write meta as single-row parquet
            meta_df = enriched.sparkSession.createDataFrame([metadata])
            meta_df.write.mode("overwrite").json(f"{self.quarantine_path}/_meta")

        logger.warning(
            "[QUARANTINE] %d rows quarantined for %s | reasons: %s",
            count, self.dataset_name, reason_map
        )
        return count
