"""
src/ingestion/raw_writer.py
-----------------------------
Writes validated, schema-enforced rows to the raw zone as Parquet.
Also adds a per-row MD5 hash for downstream deduplication.

Output path structure:
  /data/raw/<dataset>/dt=<YYYY-MM-DD>/valid/part-*.parquet
"""

import logging
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)


class RawWriter:
    """
    Writes the validated DataFrame to the raw zone.

    Parameters
    ----------
    raw_base_path    : e.g. /data/raw/employee_master
    dataset_name     : e.g. employee_master
    run_date         : YYYY-MM-DD — used as partition value
    partition_cols   : additional partition columns (default: ['dt'])
    md5_source_cols  : columns to include in MD5 row hash computation
                       (defaults to all non-metadata columns)
    """

    def __init__(
        self,
        raw_base_path: str,
        dataset_name: str,
        run_date: str,
        partition_cols: Optional[List[str]] = None,
        md5_source_cols: Optional[List[str]] = None,
    ):
        self.valid_path = f"{raw_base_path}/dt={run_date}/valid"
        self.dataset_name = dataset_name
        self.run_date = run_date
        self.partition_cols = partition_cols or []
        self.md5_source_cols = md5_source_cols

    def write(self, df: DataFrame, pipeline_run_id: str) -> int:
        """
        Enriches with audit columns, adds MD5 row hash, writes to raw zone.
        Returns count of written rows.
        """
        count = df.count()
        if count == 0:
            logger.warning("[RAW_WRITER] Empty DataFrame — nothing to write for %s", self.dataset_name)
            return 0

        # Add audit columns
        df = (
            df
            .withColumn("_raw_load_ts", F.current_timestamp())
            .withColumn("_pipeline_run_id", F.lit(pipeline_run_id))
            .withColumn("_source_dataset", F.lit(self.dataset_name))
            .withColumn("_dt", F.lit(self.run_date))
        )

        # Add MD5 row hash
        df = self._add_md5_hash(df)

        logger.info(
            "[RAW_WRITER] Writing %d rows to %s",
            count, self.valid_path
        )

        writer = df.write.mode("overwrite").parquet(self.valid_path)

        logger.info("[RAW_WRITER] Write complete for %s | dt=%s", self.dataset_name, self.run_date)
        return count

    def _add_md5_hash(self, df: DataFrame) -> DataFrame:
        """
        Computes MD5 hash over specified columns (or all non-audit columns).
        Uses SHA2 (256-bit) as PySpark doesn't have a native MD5 UDF,
        but md5() IS available as a PySpark function.
        """
        exclude_prefixes = ("_raw_", "_pipeline_", "_source_", "_dt",
                            "dq_status", "dq_failed_checks", "validation_flag")

        if self.md5_source_cols:
            hash_cols = self.md5_source_cols
        else:
            hash_cols = [
                c for c in df.columns
                if not any(c.startswith(p) for p in exclude_prefixes)
            ]

        # Concatenate all hash columns as a single string, then MD5
        concat_expr = F.concat_ws("|", *[F.coalesce(F.col(c).cast(StringType()), F.lit("NULL")) for c in hash_cols])
        df = df.withColumn("_row_md5", F.md5(concat_expr))

        return df
