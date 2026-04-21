"""
src/ingestion/schema_enforcer.py
---------------------------------
Enforces the data contract schema on a PySpark DataFrame.

Flow:
  1. Load CSV in permissive mode (bad rows → _corrupt_record)
  2. Compare incoming columns vs data contract (additive check)
  3. Apply explicit type casting based on contract
  4. Identify rows failing type cast (nulls after cast where not expected)
  5. Enforce NOT NULL mandatory columns
  6. Return: (valid_df, rejected_df, schema_diff_report)

Additive columns (new columns not in contract) are ALLOWED and kept.
Incompatible changes (missing mandatory columns) → hard fail.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Tuple, Dict, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
    DateType, TimestampType, BooleanType,
)

logger = logging.getLogger(__name__)

# Map YAML type names → PySpark types
TYPE_MAP = {
    "StringType":    StringType(),
    "IntegerType":   IntegerType(),
    "LongType":      LongType(),
    "DoubleType":    DoubleType(),
    "DateType":      DateType(),
    "TimestampType": TimestampType(),
    "BooleanType":   BooleanType(),
}


# ─── Result dataclass ─────────────────────────────────────────────────────────
@dataclass
class SchemaEnforcementResult:
    dataset_name: str
    incoming_columns: List[str]
    contract_columns: List[str]
    additive_columns: List[str] = field(default_factory=list)    # in file, not in contract — OK
    missing_mandatory: List[str] = field(default_factory=list)   # in contract mandatory, not in file — FAIL
    missing_optional: List[str] = field(default_factory=list)    # in contract optional, not in file — OK, fill NULL
    type_cast_failures: List[str] = field(default_factory=list)
    passed: bool = True

    def has_blocking_issues(self) -> bool:
        return len(self.missing_mandatory) > 0


# ─── Schema Enforcer ──────────────────────────────────────────────────────────
class SchemaEnforcer:
    """
    Applies schema enforcement using a data contract config dict.

    Parameters
    ----------
    spark           : active SparkSession
    contract        : parsed data contract dict (from YAML)
    dataset_name    : used for logging / rejection metadata
    date_format     : default date format for casting (overridden per column)
    timestamp_format: default timestamp format
    """

    def __init__(
        self,
        spark: SparkSession,
        contract: Dict[str, Any],
        dataset_name: str,
        date_format: str = "yyyy-MM-dd",
        timestamp_format: str = "yyyy-MM-dd HH:mm:ss",
    ):
        self.spark = spark
        self.contract = contract
        self.dataset_name = dataset_name
        self.date_format = date_format
        self.timestamp_format = timestamp_format

        # Build column maps from contract
        self.mandatory_cols = {
            c["name"]: c for c in contract.get("mandatory_columns", [])
        }
        self.optional_cols = {
            c["name"]: c for c in contract.get("optional_columns", [])
        }
        self.all_contract_cols = {**self.mandatory_cols, **self.optional_cols}

    def enforce(self, file_path: str) -> Tuple[DataFrame, DataFrame, SchemaEnforcementResult]:
        """
        Main entry point.

        Returns
        -------
        valid_df    : rows passing all schema checks
        rejected_df : rows failing schema checks, with rejection_reason column
        result      : SchemaEnforcementResult metadata
        """
        logger.info("[SCHEMA] Loading %s in PERMISSIVE mode", file_path)

        # Step 1: Load in permissive mode
        raw_df = (
            self.spark.read
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("inferSchema", "false")   # everything comes in as string
            .csv(file_path)
        )

        incoming_cols = [c for c in raw_df.columns if c != "_corrupt_record"]
        result = SchemaEnforcementResult(
            dataset_name=self.dataset_name,
            incoming_columns=incoming_cols,
            contract_columns=list(self.all_contract_cols.keys()),
        )

        # Step 2: Schema diff
        incoming_set = set(incoming_cols)
        contract_set = set(self.all_contract_cols.keys())
        mandatory_set = set(self.mandatory_cols.keys())

        result.additive_columns = list(incoming_set - contract_set)
        result.missing_mandatory = list(mandatory_set - incoming_set)
        result.missing_optional = list((contract_set - mandatory_set) - incoming_set)

        if result.additive_columns:
            logger.warning("[SCHEMA] Additive columns found (allowed): %s", result.additive_columns)

        if result.missing_mandatory:
            result.passed = False
            logger.error(
                "[SCHEMA] HARD FAIL — mandatory columns missing from file: %s",
                result.missing_mandatory
            )
            # Cannot proceed — return empty valid_df, full file as rejected
            rejected_df = raw_df.withColumn(
                "rejection_reason",
                F.lit(f"MISSING_MANDATORY_COLUMNS:{','.join(result.missing_mandatory)}")
            ).withColumn("rejection_layer", F.lit("SCHEMA_ENFORCEMENT"))
            empty_df = self.spark.createDataFrame([], raw_df.schema)
            return empty_df, rejected_df, result

        # Step 3: Add missing optional columns as NULL
        df = raw_df
        for col_name in result.missing_optional:
            logger.info("[SCHEMA] Adding missing optional column as NULL: %s", col_name)
            df = df.withColumn(col_name, F.lit(None).cast(StringType()))

        # Step 4: Normalize "NA", "N/A", "none", blank → NULL
        df = self._normalize_nulls(df)

        # Step 5: Apply explicit type casting
        df, cast_fail_flags = self._apply_type_casting(df)

        # Step 6: Build rejection flag per row
        # A row is rejected if:
        #   a) it has a _corrupt_record value (CSV parse failed)
        #   b) a mandatory column is NULL after casting
        #   c) a type cast failed on a HARD_FAIL column

        rejection_conditions = []

        # Corrupt CSV rows
        if "_corrupt_record" in df.columns:
            rejection_conditions.append(
                F.when(F.col("_corrupt_record").isNotNull(), F.lit("CORRUPT_CSV_ROW"))
            )
            df = df.drop("_corrupt_record")

        # Mandatory NULL checks
        for col_name in self.mandatory_cols:
            if col_name in df.columns:
                rejection_conditions.append(
                    F.when(
                        F.col(col_name).isNull(),
                        F.lit(f"NULL_MANDATORY:{col_name}")
                    )
                )

        # Type cast failures
        for flag_col, col_name in cast_fail_flags:
            rejection_conditions.append(
                F.when(
                    F.col(flag_col) == True,
                    F.lit(f"TYPE_CAST_FAIL:{col_name}")
                )
            )
            df = df.drop(flag_col)   # remove the temp flag column

        # Combine all rejection reasons (take first failure per row)
        if rejection_conditions:
            reason_expr = rejection_conditions[0]
            for cond in rejection_conditions[1:]:
                reason_expr = F.coalesce(reason_expr, cond)
            df = df.withColumn("_rejection_reason", reason_expr)
        else:
            df = df.withColumn("_rejection_reason", F.lit(None).cast(StringType()))

        valid_df = df.filter(F.col("_rejection_reason").isNull()).drop("_rejection_reason")
        rejected_df = (
            df.filter(F.col("_rejection_reason").isNotNull())
            .withColumnRenamed("_rejection_reason", "rejection_reason")
            .withColumn("rejection_layer", F.lit("SCHEMA_ENFORCEMENT"))
        )

        logger.info(
            "[SCHEMA] %s — valid=%d, rejected=%d",
            self.dataset_name,
            valid_df.count(),
            rejected_df.count(),
        )

        return valid_df, rejected_df, result

    def _normalize_nulls(self, df: DataFrame) -> DataFrame:
        """Replace NA/N/A/none/blank strings with NULL."""
        null_values = {"NA", "N/A", "N/a", "n/a", "null", "NULL", "None", "none", ""}
        for col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.when(F.trim(F.col(col_name)).isin(null_values), None)
                 .otherwise(F.col(col_name))
            )
        return df

    def _apply_type_casting(self, df: DataFrame):
        """
        Cast columns to their contract types.
        For each cast, create a temp boolean flag column to detect silent null cast.
        Returns (df_with_casts, list_of_(flag_col, original_col) tuples).
        """
        cast_fail_flags = []

        for col_name, col_def in self.all_contract_cols.items():
            if col_name not in df.columns:
                continue

            type_name = col_def.get("type", "StringType")
            spark_type = TYPE_MAP.get(type_name, StringType())
            fmt = col_def.get("format", None)

            original_col = col_name
            temp_flag = f"_cast_fail_{col_name}"

            # Before cast, track which rows had a non-null string value
            df = df.withColumn(f"_pre_{col_name}", F.col(col_name))

            # Apply cast
            if type_name == "DateType" and fmt:
                df = df.withColumn(col_name, F.to_date(F.col(col_name), fmt))
            elif type_name == "TimestampType" and fmt:
                df = df.withColumn(col_name, F.to_timestamp(F.col(col_name), fmt))
            else:
                df = df.withColumn(col_name, F.col(col_name).cast(spark_type))

            # Flag cast failure: was non-null before, is null after → cast failed
            df = df.withColumn(
                temp_flag,
                F.when(
                    F.col(f"_pre_{col_name}").isNotNull() & F.col(col_name).isNull(),
                    F.lit(True)
                ).otherwise(F.lit(False))
            ).drop(f"_pre_{col_name}")

            cast_fail_flags.append((temp_flag, original_col))

        return df, cast_fail_flags
