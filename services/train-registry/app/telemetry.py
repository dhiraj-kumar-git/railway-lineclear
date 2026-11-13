"""JSON logs for Train Registry and uvicorn, and a log line for each route lookup made in a diversion."""

import json
import logging
import sys
from datetime import datetime, timezone

# Attributes every log record has; anything else was passed with extra= and goes into the JSON.
# color_message is uvicorn's coloured copy of the message.
_STANDARD_ATTRIBUTES = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {
    "message",
    "taskName",
    "color_message",
}

logger = logging.getLogger("train_registry")


class JsonFormatter(logging.Formatter):
    """One JSON object per line, so Loki can filter on fields such as txn_id."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "time": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        entry.update({key: value for key, value in vars(record).items() if key not in _STANDARD_ATTRIBUTES})
        if record.exc_info:
            entry["error"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure_logging() -> None:
    """JSON for the service's own logs and for uvicorn's, which sets up its own handlers at start."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers = [handler]
        uvicorn_logger.propagate = False
