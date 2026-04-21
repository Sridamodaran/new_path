"""
src/common/spark_session.py  — SparkSession factory
src/common/logger.py         — Structured logging
src/common/config_loader.py  — YAML config + contract loader
src/common/alert_notifier.py — Email/Slack alerting

All in one file for convenience; in production split into separate files.
"""

# ════════════════════════════════════════════════════════════
# spark_session.py
# ════════════════════════════════════════════════════════════
import logging
import os
import smtplib
import yaml
from email.mime.text import MIMEText
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession


def get_spark_session(app_name: str, spark_conf: Dict[str, str] = None) -> SparkSession:
    """
    Creates or retrieves a SparkSession with Hive support.

    Parameters
    ----------
    app_name   : Spark application name (used in Spark UI)
    spark_conf : dict of spark config overrides
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.orc.compression.codec", "snappy")
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
    )

    if spark_conf:
        for key, val in spark_conf.items():
            builder = builder.config(key, str(val))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ════════════════════════════════════════════════════════════
# logger.py
# ════════════════════════════════════════════════════════════
def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Returns a logger with a consistent format.
    In production, replace with your org's log aggregator (e.g. structlog).
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger


# ════════════════════════════════════════════════════════════
# config_loader.py
# ════════════════════════════════════════════════════════════
_CONFIG_BASE = os.path.join(os.path.dirname(__file__), "..", "..", "conf")


def load_config(env: str = "dev") -> Dict[str, Any]:
    """Load environment config from conf/env/<env>.yaml"""
    config_path = os.path.join(_CONFIG_BASE, "env", f"{env}.yaml")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def load_contract(dataset_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Load data contract YAML for a dataset."""
    version = config.get("contract_versions", {}).get(dataset_name, "v1")
    contract_path = os.path.join(
        _CONFIG_BASE, "data_contracts", f"{dataset_name}_{version}.yaml"
    )
    with open(contract_path, "r") as f:
        return yaml.safe_load(f)


# ════════════════════════════════════════════════════════════
# alert_notifier.py
# ════════════════════════════════════════════════════════════
class AlertNotifier:
    """
    Sends pipeline alerts via email (and optionally Slack).

    alert_config keys:
      enabled       : true/false
      smtp_host     : mail.myorg.com
      smtp_port     : 587
      from_email    : pipeline-alerts@myorg.com
      to_emails     : [de-team@myorg.com, manager@myorg.com]
      slack_webhook : https://hooks.slack.com/... (optional)
    """

    def __init__(self, alert_config: Dict[str, Any]):
        self.config = alert_config
        self.logger = get_logger(__name__)

    def send(self, subject: str, body: str):
        if not self.config.get("enabled", False):
            self.logger.info("[ALERT] Alerts disabled — would send: %s", subject)
            return

        self._send_email(subject, body)
        if self.config.get("slack_webhook"):
            self._send_slack(subject, body)

    def _send_email(self, subject: str, body: str):
        try:
            msg = MIMEText(body)
            msg["Subject"] = subject
            msg["From"] = self.config["from_email"]
            msg["To"] = ", ".join(self.config["to_emails"])

            with smtplib.SMTP(self.config["smtp_host"], self.config.get("smtp_port", 587)) as s:
                s.sendmail(self.config["from_email"], self.config["to_emails"], msg.as_string())
            self.logger.info("[ALERT] Email sent: %s", subject)
        except Exception as e:
            self.logger.error("[ALERT] Email failed: %s", e)

    def _send_slack(self, subject: str, body: str):
        import urllib.request
        import json
        payload = json.dumps({"text": f"*{subject}*\n{body}"}).encode()
        try:
            req = urllib.request.Request(
                self.config["slack_webhook"],
                data=payload,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception as e:
            self.logger.error("[ALERT] Slack failed: %s", e)
