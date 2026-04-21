import smtplib
import urllib.request
import json
from typing import Any, Dict
from src.common.logger import get_logger

class AlertNotifier:
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
            from email.mime.text import MIMEText
            msg = MIMEText(body)
            msg["Subject"] = subject
            msg["From"] = self.config["from_email"]
            msg["To"] = ", ".join(self.config["to_emails"])
            with smtplib.SMTP(self.config["smtp_host"], self.config.get("smtp_port", 587)) as s:
                s.sendmail(self.config["from_email"], self.config["to_emails"], msg.as_string())
        except Exception as e:
            self.logger.error("[ALERT] Email failed: %s", e)

    def _send_slack(self, subject: str, body: str):
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
