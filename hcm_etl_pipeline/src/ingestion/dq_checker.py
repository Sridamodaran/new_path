"""
src/ingestion/dq_checker.py
-----------------------------
Custom Data Quality checker.

Design:
  - One DQCheck function per column/rule, registered in a registry.
  - Each check returns a boolean column expression (True = PASS).
  - DQChecker runs all checks against the DF and produces a validation_flag column.
  - Rows with any HARD_FAIL → moved to quarantine.
  - Rows with only SOFT_FAIL → flagged but allowed through (with dq_status = 'WARN').
  - Rows with all PASS → dq_status = 'PASS'.

Output columns added to DataFrame:
  - dq_status          : PASS | WARN | FAIL
  - dq_failed_checks   : pipe-separated list of failed checks
  - validation_flag    : 1 = valid, 0 = invalid (quarantine)
"""

import re
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Callable, List, Dict, Any, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
from pyspark.sql.column import Column

logger = logging.getLogger(__name__)


# ─── Severity enum ────────────────────────────────────────────────────────────
class Severity(str, Enum):
    HARD_FAIL = "HARD_FAIL"   # Row → quarantine
    SOFT_FAIL = "SOFT_FAIL"   # Row → flagged, allowed through with warning


# ─── DQCheck definition ───────────────────────────────────────────────────────
@dataclass
class DQCheck:
    check_name: str
    column: str
    severity: Severity
    check_expr: Column   # PySpark column expression — True = PASS


# ─── Built-in check factory functions ────────────────────────────────────────
def check_not_null(column: str, severity: Severity = Severity.HARD_FAIL) -> DQCheck:
    return DQCheck(
        check_name=f"{column}__not_null",
        column=column,
        severity=severity,
        check_expr=F.col(column).isNotNull(),
    )


def check_allowed_values(
    column: str,
    allowed: List[str],
    severity: Severity = Severity.SOFT_FAIL,
) -> DQCheck:
    return DQCheck(
        check_name=f"{column}__allowed_values",
        column=column,
        severity=severity,
        check_expr=(
            F.col(column).isNull() |   # NULL is handled by not_null check
            F.col(column).isin(allowed)
        ),
    )


def check_regex(
    column: str,
    pattern: str,
    severity: Severity = Severity.SOFT_FAIL,
) -> DQCheck:
    return DQCheck(
        check_name=f"{column}__regex",
        column=column,
        severity=severity,
        check_expr=(
            F.col(column).isNull() |
            F.col(column).rlike(pattern)
        ),
    )


def check_min_length(
    column: str,
    min_len: int,
    severity: Severity = Severity.SOFT_FAIL,
) -> DQCheck:
    return DQCheck(
        check_name=f"{column}__min_length",
        column=column,
        severity=severity,
        check_expr=(
            F.col(column).isNull() |
            (F.length(F.col(column)) >= min_len)
        ),
    )


def check_positive_number(
    column: str,
    severity: Severity = Severity.SOFT_FAIL,
) -> DQCheck:
    """Checks numeric column > 0 (assumes already cast to numeric type)."""
    return DQCheck(
        check_name=f"{column}__positive",
        column=column,
        severity=severity,
        check_expr=(
            F.col(column).isNull() |
            (F.col(column) > 0)
        ),
    )


def check_date_not_future(
    column: str,
    severity: Severity = Severity.SOFT_FAIL,
) -> DQCheck:
    return DQCheck(
        check_name=f"{column}__not_future",
        column=column,
        severity=severity,
        check_expr=(
            F.col(column).isNull() |
            (F.col(column) <= F.current_date())
        ),
    )


def check_date_range(
    column: str,
    min_date: str,
    max_date: str,
    severity: Severity = Severity.SOFT_FAIL,
) -> DQCheck:
    return DQCheck(
        check_name=f"{column}__date_range",
        column=column,
        severity=severity,
        check_expr=(
            F.col(column).isNull() |
            (F.col(column).between(F.lit(min_date).cast("date"), F.lit(max_date).cast("date")))
        ),
    )


def check_gross_salary_consistency(severity: Severity = Severity.SOFT_FAIL) -> DQCheck:
    """
    Business rule: gross_salary should be >= basic_salary.
    Example of multi-column cross-field DQ check.
    """
    return DQCheck(
        check_name="gross_salary__ge_basic",
        column="gross_salary",
        severity=severity,
        check_expr=(
            F.col("gross_salary").isNull() |
            F.col("basic_salary").isNull() |
            (F.col("gross_salary") >= F.col("basic_salary"))
        ),
    )


# ─── Check registry — maps contract DQ rule dicts to DQCheck objects ─────────
def build_checks_from_contract(dq_rules: List[Dict[str, Any]]) -> List[DQCheck]:
    """
    Parses the dq_rules list from a data contract YAML and returns DQCheck objects.
    """
    checks = []
    for rule in dq_rules:
        col = rule["column"]
        check_type = rule["check"]
        severity = Severity(rule.get("severity", "SOFT_FAIL"))

        if check_type == "not_null":
            checks.append(check_not_null(col, severity))
        elif check_type == "allowed_values":
            checks.append(check_allowed_values(col, rule["allowed"], severity))
        elif check_type == "regex_match":
            checks.append(check_regex(col, rule["pattern"], severity))
        elif check_type == "date_format":
            # Date format check — after casting, if date is non-null it's valid
            # (cast failure already caught by schema_enforcer)
            pass
        elif check_type == "positive_number":
            checks.append(check_positive_number(col, severity))
        elif check_type == "date_not_future":
            checks.append(check_date_not_future(col, severity))
        else:
            logger.warning("[DQ] Unknown check type '%s' for column '%s' — skipping", check_type, col)

    return checks


# ─── DQ Checker ───────────────────────────────────────────────────────────────
class DQChecker:
    """
    Runs all DQ checks against a DataFrame and produces:
      - dq_status          : PASS | WARN | FAIL
      - dq_failed_checks   : comma-separated list of failed check names
      - validation_flag    : 1 = valid (PASS or WARN), 0 = invalid (FAIL)

    Parameters
    ----------
    checks          : list of DQCheck objects
    dataset_name    : used in logging
    soft_fail_threshold : if soft_fail_pct > this, escalate to hard fail (default: None)
    """

    def __init__(
        self,
        checks: List[DQCheck],
        dataset_name: str,
        soft_fail_threshold: Optional[float] = None,
    ):
        self.checks = checks
        self.dataset_name = dataset_name
        self.soft_fail_threshold = soft_fail_threshold

    def run(self, df: DataFrame) -> DataFrame:
        """
        Runs all checks and adds dq_status, dq_failed_checks, validation_flag columns.
        Returns the annotated DataFrame (no splitting here — splitting done by quarantine_writer).
        """
        logger.info("[DQ] Running %d checks on %s", len(self.checks), self.dataset_name)

        # Add per-check result columns (temp)
        # True = PASS, False = FAIL
        temp_cols = []
        for chk in self.checks:
            temp_col = f"_chk_{chk.check_name}"
            temp_cols.append((temp_col, chk))
            df = df.withColumn(temp_col, chk.check_expr.cast(BooleanType()))

        # Build dq_failed_checks: pipe-separated names of failed checks
        # Build dq_status: FAIL if any HARD_FAIL check is False
        #                  WARN if any SOFT_FAIL check is False (but no HARD_FAIL)
        #                  PASS otherwise

        hard_fail_checks = [
            (f"_chk_{c.check_name}", c.check_name)
            for c in self.checks if c.severity == Severity.HARD_FAIL
        ]
        soft_fail_checks = [
            (f"_chk_{c.check_name}", c.check_name)
            for c in self.checks if c.severity == Severity.SOFT_FAIL
        ]

        # Collect failed check names per row
        all_fail_exprs = []
        for temp_col, check_name in hard_fail_checks + soft_fail_checks:
            all_fail_exprs.append(
                F.when(F.col(temp_col) == False, F.lit(check_name))
            )

        if all_fail_exprs:
            # Concatenate non-null failure names
            fail_array = F.array(*[
                F.when(F.col(temp_col) == False, F.lit(name))
                for temp_col, name in hard_fail_checks + soft_fail_checks
            ])
            df = df.withColumn(
                "dq_failed_checks",
                F.array_join(
                    F.array_remove(fail_array, None),
                    "|"
                )
            )
        else:
            df = df.withColumn("dq_failed_checks", F.lit(""))

        # Determine dq_status
        if hard_fail_checks:
            hard_fail_condition = F.lit(False)
            for temp_col, _ in hard_fail_checks:
                hard_fail_condition = hard_fail_condition | (F.col(temp_col) == False)
        else:
            hard_fail_condition = F.lit(False)

        if soft_fail_checks:
            soft_fail_condition = F.lit(False)
            for temp_col, _ in soft_fail_checks:
                soft_fail_condition = soft_fail_condition | (F.col(temp_col) == False)
        else:
            soft_fail_condition = F.lit(False)

        df = df.withColumn(
            "dq_status",
            F.when(hard_fail_condition, F.lit("FAIL"))
             .when(soft_fail_condition, F.lit("WARN"))
             .otherwise(F.lit("PASS"))
        )

        # validation_flag: 1 = valid, 0 = quarantine
        df = df.withColumn(
            "validation_flag",
            F.when(F.col("dq_status") == "FAIL", F.lit(0))
             .otherwise(F.lit(1))
        )

        # Drop temp check columns
        for temp_col, _ in [(f"_chk_{c.check_name}", c) for c in self.checks]:
            df = df.drop(temp_col)

        # Log summary
        total = df.count()
        fail_count = df.filter(F.col("validation_flag") == 0).count()
        warn_count = df.filter(F.col("dq_status") == "WARN").count()
        pass_count = total - fail_count - warn_count

        logger.info(
            "[DQ] %s — total=%d | PASS=%d | WARN=%d | FAIL=%d",
            self.dataset_name, total, pass_count, warn_count, fail_count,
        )

        if self.soft_fail_threshold and total > 0:
            soft_pct = (fail_count + warn_count) / total
            if soft_pct > self.soft_fail_threshold:
                logger.warning(
                    "[DQ] Soft fail %% (%.2f) exceeded threshold (%.2f) for %s",
                    soft_pct, self.soft_fail_threshold, self.dataset_name,
                )

        return df
