"""Logging wrappers for Wikimedia API requests."""

import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)


def log_response(method: str, response: requests.Response) -> None:
    """Log failed Wikimedia API responses without changing request behaviour."""
    if response.ok:
        return

    logger.warning(
        "%s %s returned HTTP %s: %s",
        method,
        response.url,
        response.status_code,
        response.text[:500],
    )


def logged_get(url: str, **kwargs: Any) -> requests.Response:
    """Call requests.get and log unsuccessful Wikimedia API responses."""
    response = requests.get(url, **kwargs)
    log_response("GET", response)
    return response


def logged_post(url: str, **kwargs: Any) -> requests.Response:
    """Call requests.post and log unsuccessful Wikimedia API responses."""
    response = requests.post(url, **kwargs)
    log_response("POST", response)
    return response
