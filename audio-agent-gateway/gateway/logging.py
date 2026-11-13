import json
import logging
import os
import re
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path


def redact(text):
    text = re.sub(r"(?i)\b(?:sk[-_][\w-]{8,}|(?:bearer|basic)\s+[\w.~\-/+=]+)", "[REDACTED]", str(text))
    return re.sub(
        r"(?i)([?&](?:api[_-]?key|token|access_token|secret|password)=)[^&\s]+", r"\1[REDACTED]", text
    )


class JsonFormatter(logging.Formatter):
    def format(self, record):
        data = {
            "schema": "qwaudio.log/v1",
            "pid": os.getpid(),
            "time": datetime.now(UTC).isoformat(),
            "level": record.levelname.lower(),
            "component": record.name,
            "message": redact(record.getMessage()),
        }
        if record.exc_info:
            data["exception"] = redact(self.formatException(record.exc_info))
        return json.dumps(data, ensure_ascii=False)


def configure(directory):
    log = logging.getLogger("gateway")
    log.setLevel(
        {
            "trace": logging.DEBUG,
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warn": logging.WARNING,
            "error": logging.ERROR,
            "fatal": logging.CRITICAL,
            "silent": logging.CRITICAL + 1,
        }.get(os.getenv("AUDIO_LOG_LEVEL", "info"), logging.INFO)
    )
    if log.handlers:
        return

    def bounded(name, default, low, high):
        try:
            return min(high, max(low, int(os.getenv(name, str(default)))))
        except ValueError:
            return default

    if os.getenv("AUDIO_LOG_FILE") != "0":
        directory = Path(os.getenv("AUDIO_LOG_DIR") or directory)
        directory.mkdir(parents=True, exist_ok=True)
        handler = RotatingFileHandler(
            directory / "gateway.jsonl",
            maxBytes=bounded("AUDIO_LOG_MAX_BYTES", 10 * 1024 * 1024, 1024, 1024**3),
            backupCount=bounded("AUDIO_LOG_MAX_FILES", 5, 1, 100),
            encoding="utf-8",
        )
        handler.setFormatter(JsonFormatter())
        log.addHandler(handler)
    if os.getenv("AUDIO_LOG_CONSOLE") != "0":
        console = logging.StreamHandler()
        console.setFormatter(JsonFormatter())
        log.addHandler(console)
    if not log.handlers:
        log.addHandler(logging.NullHandler())
