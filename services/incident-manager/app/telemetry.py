"""JSON logs for the service and uvicorn, and gRPC request metrics."""

import json
import logging
import sys
import time
from datetime import datetime, timezone

import grpc
from prometheus_client import Counter, Histogram

# Attributes every log record has; anything else was passed with extra= and goes into the JSON.
# color_message is uvicorn's coloured copy of the message.
_STANDARD_ATTRIBUTES = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {
    "message",
    "taskName",
    "color_message",
}


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
    logging.getLogger("httpx").setLevel(logging.WARNING)  # the service logs its own calls, with txn_id


REQUESTS = Counter(
    "grpc_server_handled_total",
    "gRPC calls completed, by method and status code",
    ["grpc_service", "grpc_method", "grpc_code"],
)
DURATION = Histogram(
    "grpc_server_handling_seconds",
    "Time taken to answer a gRPC call",
    ["grpc_service", "grpc_method"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)


class MetricsInterceptor(grpc.ServerInterceptor):
    """Counts every call and its duration: rate, errors and duration for the dashboard."""

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        service, _, method = handler_call_details.method.lstrip("/").rpartition("/")
        # Health checks and reflection are tooling, not traffic
        if handler is None or handler.unary_unary is None or service.startswith("grpc."):
            return handler
        answer = handler.unary_unary

        def counted(request, context):
            started = time.perf_counter()
            code = grpc.StatusCode.OK
            try:
                return answer(request, context)
            except Exception:
                code = context.code() or grpc.StatusCode.UNKNOWN
                raise
            finally:
                code = context.code() or code
                REQUESTS.labels(service, method, code.name).inc()
                DURATION.labels(service, method).observe(time.perf_counter() - started)

        return grpc.unary_unary_rpc_method_handler(
            counted,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )