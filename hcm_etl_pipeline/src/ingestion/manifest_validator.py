"""
src/ingestion/manifest_validator.py
------------------------------------
Validates the manifest file for a landing zone dataset.
Checks: file arrival, filename pattern, manifest presence,
        checksum (MD5), record count, file size.

All results are returned as a structured ValidationResult dataclass
so the Airflow DAG or caller can decide how to proceed.
"""

import hashlib
import json
import os
import re
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ─── Result dataclass ─────────────────────────────────────────────────────────
@dataclass
class ManifestValidationResult:
    dataset_name: str
    file_path: str
    manifest_path: str
    passed: bool = True
    checks: dict = field(default_factory=dict)
    errors: list = field(default_factory=list)

    def fail(self, check: str, reason: str):
        self.passed = False
        self.checks[check] = "FAIL"
        self.errors.append(f"{check}: {reason}")
        logger.error("[MANIFEST] FAIL — %s | %s", check, reason)

    def ok(self, check: str, detail: str = ""):
        self.checks[check] = "PASS"
        logger.info("[MANIFEST] PASS — %s %s", check, detail)


# ─── Validator class ──────────────────────────────────────────────────────────
class ManifestValidator:
    """
    Validates a landing zone file against its manifest.

    Parameters
    ----------
    file_path      : full path to the CSV/data file
    manifest_path  : full path to the _manifest.json sidecar
    filename_pattern: regex pattern the filename must match
                      e.g. r'^employee_master_\d{8}\.csv$'
    """

    def __init__(
        self,
        file_path: str,
        manifest_path: str,
        filename_pattern: Optional[str] = None,
    ):
        self.file_path = file_path
        self.manifest_path = manifest_path
        self.filename_pattern = filename_pattern
        self.dataset_name = os.path.basename(file_path).split("_")[0]

    def validate(self) -> ManifestValidationResult:
        result = ManifestValidationResult(
            dataset_name=self.dataset_name,
            file_path=self.file_path,
            manifest_path=self.manifest_path,
        )

        # Check 1: File arrived
        if not os.path.exists(self.file_path):
            result.fail("file_arrived", f"File not found: {self.file_path}")
            return result   # Cannot proceed without the file
        result.ok("file_arrived")

        # Check 2: Filename pattern
        if self.filename_pattern:
            filename = os.path.basename(self.file_path)
            if not re.match(self.filename_pattern, filename):
                result.fail("filename_pattern", f"'{filename}' does not match '{self.filename_pattern}'")
            else:
                result.ok("filename_pattern", f"'{filename}'")

        # Check 3: Manifest file present
        if not os.path.exists(self.manifest_path):
            result.fail("manifest_present", f"Manifest not found: {self.manifest_path}")
            return result   # Cannot do checksum/count without manifest
        result.ok("manifest_present")

        # Parse manifest JSON
        try:
            with open(self.manifest_path, "r") as f:
                manifest = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            result.fail("manifest_parseable", str(e))
            return result
        result.ok("manifest_parseable")

        # Check 4: Checksum (MD5)
        expected_checksum = manifest.get("checksum", "")
        actual_checksum = self._compute_md5(self.file_path)
        if actual_checksum != expected_checksum:
            result.fail(
                "checksum_match",
                f"expected={expected_checksum}, actual={actual_checksum}"
            )
        else:
            result.ok("checksum_match", f"MD5={actual_checksum}")

        # Check 5: Record count
        expected_rows = manifest.get("expected_rows", -1)
        actual_rows = self._count_rows(self.file_path)
        if expected_rows != actual_rows:
            result.fail(
                "row_count_match",
                f"expected={expected_rows}, actual={actual_rows}"
            )
        else:
            result.ok("row_count_match", f"rows={actual_rows}")

        # Check 6: File size
        expected_size = manifest.get("file_size_bytes", -1)
        actual_size = os.path.getsize(self.file_path)
        if expected_size != actual_size:
            result.fail(
                "file_size_match",
                f"expected={expected_size}, actual={actual_size}"
            )
        else:
            result.ok("file_size_match", f"bytes={actual_size}")

        logger.info(
            "[MANIFEST] Validation complete for %s — %s | errors: %s",
            self.dataset_name,
            "PASSED" if result.passed else "FAILED",
            result.errors,
        )
        return result

    # ── Private helpers ───────────────────────────────────────────────────────
    @staticmethod
    def _compute_md5(filepath: str) -> str:
        h = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def _count_rows(filepath: str) -> int:
        """Count data rows (excludes header)."""
        with open(filepath, "r") as f:
            return sum(1 for _ in f) - 1
