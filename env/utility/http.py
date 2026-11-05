import asyncio
import json as jsonlib
import logging
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

import aiohttp
import requests

DEFAULT_MAX_RETRIES = 3
DEFAULT_MAX_CONCURRENCY = 3
BACKOFF_BASE_SECONDS = 2.0
MAX_BACKOFF_SECONDS = 60.0
# Power BI admin throttling can ask for a long pause, so Retry-After is honored up to an hour.
MAX_RETRY_AFTER_SECONDS = 3600.0

_logger = logging.getLogger(__name__)
RETRYABLE_STATUSES = frozenset({0, 429, 502, 503, 504})


def is_retryable(status: int) -> bool:
    """Retry throttling, gateway/service failures and transport failures."""
    return status in RETRYABLE_STATUSES


class RequestCoordinator:
    """Share concurrency and server cooldowns across async and threaded callers."""

    def __init__(self, max_concurrency: int = DEFAULT_MAX_CONCURRENCY):
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self.max_concurrency = max_concurrency
        self._semaphore = threading.BoundedSemaphore(max_concurrency)
        self._cooldown_lock = threading.Lock()
        self._cooldown_until = 0.0

    def defer(self, seconds: float) -> None:
        with self._cooldown_lock:
            self._cooldown_until = max(self._cooldown_until, time.monotonic() + seconds)

    def _delay(self) -> float:
        with self._cooldown_lock:
            return max(0.0, self._cooldown_until - time.monotonic())

    def acquire(self, sleep=None) -> None:
        sleep = sleep or time.sleep
        self._semaphore.acquire()
        try:
            delay = self._delay()
            if delay > 0:
                sleep(delay)
        except BaseException:
            self.release()
            raise

    async def acquire_async(self, sleep=None) -> None:
        sleep = sleep or asyncio.sleep
        acquire_task = asyncio.create_task(asyncio.to_thread(self._semaphore.acquire))
        try:
            await asyncio.shield(acquire_task)
        except asyncio.CancelledError:
            if await acquire_task:
                self.release()
            raise
        try:
            delay = self._delay()
            if delay > 0:
                await sleep(delay)
        except BaseException:
            self.release()
            raise

    def release(self) -> None:
        self._semaphore.release()


def url_path(url: str) -> str:
    """Path only, so query strings (continuation tokens) never reach the logs."""
    return urlsplit(url).path or "/"


def parse_retry_after(value) -> float | None:
    if value is None:
        return None
    value = str(value).strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def retry_wait(attempt: int, retry_after=None) -> float:
    """Seconds to wait after failed attempt number `attempt` (1-based)."""
    hinted = parse_retry_after(retry_after)
    if hinted is not None:
        return min(max(hinted, 1.0), MAX_RETRY_AFTER_SECONDS)
    backoff = min(MAX_BACKOFF_SECONDS, BACKOFF_BASE_SECONDS * 2 ** (attempt - 1))
    return backoff + random.uniform(0, 1)


def _log_retry(logger, status: int, attempt: int, max_retries: int, wait: float, url: str) -> None:
    logger.warning(
        "HTTP retry: status=%s attempt=%d/%d wait=%.1fs path=%s",
        status if status else "network-error",
        attempt,
        max_retries + 1,
        wait,
        url_path(url),
    )


@dataclass(frozen=True)
class HttpResponse:
    status: int
    text: str = ""
    retry_after: str | None = None

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300

    def json(self):
        return jsonlib.loads(self.text) if self.text.strip() else {}

    def error(self) -> dict:
        """Error dict keeping the legacy "ERROR" key, with the real status code added."""
        return {
            "ERROR": f"HTTP {self.status}" if self.status else "Network error",
            "status_code": self.status,
            "message": self.text[:500],
        }


async def aio_request(
    method: str,
    url: str,
    *,
    headers=None,
    json_body=None,
    max_retries: int = DEFAULT_MAX_RETRIES,
    logger=None,
    timeout=None,
    connector_kwargs=None,
    sleep=None,
    coordinator: RequestCoordinator | None = None,
) -> HttpResponse:
    """aiohttp request with bounded retries for throttling and transient failures."""
    logger = logger or _logger
    sleep = sleep or asyncio.sleep
    attempt = 0
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(**(connector_kwargs or {})), timeout=timeout
    ) as session:
        while True:
            attempt += 1
            if coordinator is not None:
                await coordinator.acquire_async(sleep)
            try:
                try:
                    async with session.request(method, url, headers=headers, json=json_body) as response:
                        result = HttpResponse(
                            status=response.status,
                            text=await response.text(errors="replace"),
                            retry_after=response.headers.get("Retry-After"),
                        )
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    result = HttpResponse(status=0, text=f"{type(exc).__name__}: {exc}")

                retry = not result.ok and is_retryable(result.status) and attempt <= max_retries
                if retry:
                    wait = retry_wait(attempt, result.retry_after)
                    _log_retry(logger, result.status, attempt, max_retries, wait, url)
                    if coordinator is not None:
                        coordinator.defer(wait)
            finally:
                if coordinator is not None:
                    coordinator.release()

            if not retry:
                return result

            if coordinator is None:
                await sleep(wait)


async def async_request_with_retry(
    method: str,
    url: str,
    *,
    max_retries: int = DEFAULT_MAX_RETRIES,
    logger=None,
    **request_kwargs,
):
    """Run the synchronous retry helper in a worker thread from async code."""
    return await asyncio.to_thread(
        request_with_retry,
        method,
        url,
        max_retries=max_retries,
        logger=logger,
        **request_kwargs,
    )


def request_with_retry(
    method: str,
    url: str,
    *,
    max_retries: int = DEFAULT_MAX_RETRIES,
    logger=None,
    sleep=None,
    coordinator: RequestCoordinator | None = None,
    **request_kwargs,
):
    """
    requests call with the same retry policy. Blocking: call it from asyncio.to_thread, never
    directly from async code. Returns the last response; re-raises the last network error.
    """
    logger = logger or _logger
    sleep = sleep or time.sleep
    attempt = 0
    while True:
        attempt += 1
        if coordinator is not None:
            coordinator.acquire(sleep)
        try:
            try:
                response = requests.request(method, url, **request_kwargs)
            except requests.RequestException:
                if attempt > max_retries:
                    raise
                status, retry_after = 0, None
                retry = True
            else:
                status, retry_after = response.status_code, response.headers.get("Retry-After")
                retry = is_retryable(status) and attempt <= max_retries

            if retry:
                wait = retry_wait(attempt, retry_after)
                _log_retry(logger, status, attempt, max_retries, wait, url)
                if coordinator is not None:
                    coordinator.defer(wait)
        finally:
            if coordinator is not None:
                coordinator.release()

        if not retry:
            return response

        if coordinator is None:
            sleep(wait)
