"""JSONL logging helpers for Wikimedia API request metrics."""

import json
import logging
import os
import socket
import time
import typing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from urllib.parse import parse_qs, urlparse

import requests
from flask import has_request_context, request

from . import user_agent


@dataclass(frozen=True)
class WikimediaApiLogConfig:
    """Configuration for Wikimedia API request logging."""

    tool: str
    log_path: Path
    user_agent: str


@dataclass(frozen=True)
class WikimediaApiRequestMetric:
    """Details of one Wikimedia API request."""

    tool: str
    url: str
    method: str
    status_code: int | None
    elapsed_ms: int
    user_agent: str
    error: str | None = None


_logger_cache: dict[Path, logging.Logger] = {}

wikimedia_log_config = WikimediaApiLogConfig(
    tool="owl-places",
    log_path=Path(
        os.environ.get(
            "OWL_PLACES_WIKIMEDIA_API_LOG",
            "/var/log/owl-places/wikimedia-api.jsonl",
        )
    ),
    user_agent=user_agent,
)


def setup_wikimedia_api_logger(log_path: Path) -> logging.Logger:
    """Create a JSONL logger for Wikimedia API request metrics."""
    if log_path in _logger_cache:
        return _logger_cache[log_path]

    logger_name = f"wikimedia_api_metrics.{log_path}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            handler: logging.Handler = logging.FileHandler(log_path)
        except OSError:
            handler = logging.NullHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)

    _logger_cache[log_path] = logger
    return logger


def get_mediawiki_action(url: str) -> str | None:
    """Extract the MediaWiki API action from a URL, if present."""
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    values = query.get("action")

    if not values:
        return None

    return values[0]


def flask_request_context() -> dict[str, str]:
    """Return details about the current Flask request, if any."""
    if not has_request_context():
        return {}

    context = {"flask_url": request.url}
    if request.endpoint is not None:
        context["flask_endpoint"] = request.endpoint
    return context


def build_log_record(metric: WikimediaApiRequestMetric) -> dict[str, object]:
    """Build a JSON-serialisable log record for one API request."""
    parsed = urlparse(metric.url)

    record: dict[str, object] = {
        "ts": datetime.now(UTC).isoformat(),
        "tool": metric.tool,
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "method": metric.method,
        "api_host": parsed.netloc,
        "path": parsed.path,
        "action": get_mediawiki_action(metric.url),
        "status_code": metric.status_code,
        "elapsed_ms": metric.elapsed_ms,
        "user_agent": metric.user_agent,
    }
    record.update(flask_request_context())

    if metric.error is not None:
        record["error"] = metric.error

    return record


def log_wikimedia_api_request(
    logger: logging.Logger,
    metric: WikimediaApiRequestMetric,
) -> None:
    """Write one Wikimedia API request metric as a JSONL log line."""
    record = build_log_record(metric)
    logger.info(json.dumps(record, separators=(",", ":"), sort_keys=True))


class WikimediaRequestTimer:
    """Context manager for timing and logging a Wikimedia API request."""

    def __init__(
        self,
        config: WikimediaApiLogConfig,
        method: str,
        url: str,
    ) -> None:
        self.config = config
        self.method = method
        self.url = url
        self.started = 0.0
        self.logger = setup_wikimedia_api_logger(config.log_path)

    def __enter__(self) -> "WikimediaRequestTimer":
        """Start timing a request."""
        self.started = time.monotonic()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Log failed requests when an exception escapes."""
        if exc is None:
            return False

        elapsed_ms = int((time.monotonic() - self.started) * 1000)

        log_wikimedia_api_request(
            self.logger,
            WikimediaApiRequestMetric(
                tool=self.config.tool,
                url=self.url,
                method=self.method,
                status_code=None,
                elapsed_ms=elapsed_ms,
                user_agent=self.config.user_agent,
                error=type(exc).__name__,
            ),
        )

        return False

    def log_response(self, status_code: int, final_url: str | None = None) -> None:
        """Log a completed request."""
        elapsed_ms = int((time.monotonic() - self.started) * 1000)

        log_wikimedia_api_request(
            self.logger,
            WikimediaApiRequestMetric(
                tool=self.config.tool,
                url=final_url or self.url,
                method=self.method,
                status_code=status_code,
                elapsed_ms=elapsed_ms,
                user_agent=self.config.user_agent,
            ),
        )


def logged_get(url: str, **kwargs: typing.Any) -> requests.Response:
    """Make a Wikimedia API GET request and log one JSONL metric line."""
    with WikimediaRequestTimer(wikimedia_log_config, "GET", url) as timer:
        r = requests.get(url, **kwargs)
        timer.log_response(r.status_code, r.url)
        return r


def logged_post(url: str, **kwargs: typing.Any) -> requests.Response:
    """Make a Wikimedia API POST request and log one JSONL metric line."""
    with WikimediaRequestTimer(wikimedia_log_config, "POST", url) as timer:
        r = requests.post(url, **kwargs)
        timer.log_response(r.status_code, r.url)
        return r
