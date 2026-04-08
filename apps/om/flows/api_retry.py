"""Retry wrapper for Open-Meteo API requests.

Provides exponential backoff with jitter for transient HTTP errors
(429 rate-limit, 5xx server errors, timeouts, connection resets).
Used by pipeline_wind.py and pipeline_heat.py to avoid whole-task
retry amplification in Prefect.
"""

import logging
import random
import time
from typing import Any, Dict, Optional, Set

import requests

logger = logging.getLogger(__name__)

RETRIABLE_STATUS_CODES: Set[int] = {429, 500, 502, 503, 504}

_DEFAULT_MAX_RETRIES = 5
_DEFAULT_BASE_DELAY = 30.0  # seconds
_DEFAULT_MAX_DELAY = 300.0  # seconds


def post_with_retry(
    url: str,
    data: Dict[str, Any],
    timeout: int = 120,
    max_retries: int = _DEFAULT_MAX_RETRIES,
    base_delay: float = _DEFAULT_BASE_DELAY,
    max_delay: float = _DEFAULT_MAX_DELAY,
) -> requests.Response:
    """POST request with exponential backoff for transient API errors.

    Retries on 429, 5xx status codes, read timeouts, and connection
    errors.  Respects the ``Retry-After`` header when present.

    Args:
        url: Target URL.
        data: Form-encoded POST body.
        timeout: Per-request read timeout in seconds.
        max_retries: Maximum number of retry attempts.
        base_delay: Initial backoff delay in seconds.
        max_delay: Cap on backoff delay in seconds.

    Returns:
        Successful ``requests.Response``.

    Raises:
        requests.HTTPError: After all retries are exhausted for HTTP errors.
        requests.exceptions.ReadTimeout: After all retries for timeouts.
        requests.exceptions.ConnectionError: After all retries for connection
            failures.
    """
    last_exception: Optional[Exception] = None

    for attempt in range(max_retries + 1):
        try:
            response = requests.post(url, data=data, timeout=timeout)

            if response.status_code not in RETRIABLE_STATUS_CODES:
                response.raise_for_status()
                return response

            # Retriable status — exhaust retries before raising
            if attempt >= max_retries:
                response.raise_for_status()

            delay = _compute_delay(
                attempt, base_delay, max_delay, response.headers,
            )
            logger.warning(
                "Open-Meteo returned %d, retrying in %.0fs "
                "(attempt %d/%d) url=%s",
                response.status_code,
                delay,
                attempt + 1,
                max_retries,
                url,
            )
            time.sleep(delay)

        except (
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectionError,
        ) as exc:
            last_exception = exc
            if attempt >= max_retries:
                raise

            delay = _compute_delay(attempt, base_delay, max_delay)
            logger.warning(
                "Open-Meteo request failed (%s), retrying in %.0fs "
                "(attempt %d/%d) url=%s",
                type(exc).__name__,
                delay,
                attempt + 1,
                max_retries,
                url,
            )
            time.sleep(delay)

    # Unreachable in practice; satisfies type checker
    if last_exception:
        raise last_exception
    raise RuntimeError("Unexpected retry loop exit")  # pragma: no cover


def _compute_delay(
    attempt: int,
    base_delay: float,
    max_delay: float,
    headers: Optional[Any] = None,
) -> float:
    """Compute backoff delay with optional Retry-After header.

    Args:
        attempt: Zero-based attempt index.
        base_delay: Initial backoff in seconds.
        max_delay: Maximum backoff cap in seconds.
        headers: Response headers (checked for ``Retry-After``).

    Returns:
        Delay in seconds to sleep before the next attempt.
    """
    delay = base_delay * (2 ** attempt)

    if headers:
        retry_after = headers.get("Retry-After")
        if retry_after:
            try:
                delay = max(delay, float(retry_after))
            except (ValueError, TypeError):
                pass

    delay = min(delay, max_delay)
    # Add 10 % jitter to avoid thundering herd
    delay += random.uniform(0, delay * 0.1)
    return delay
