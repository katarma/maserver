import asyncio
import time
import logging
import random
from typing import Callable, Any, Optional, Awaitable, Dict

SENTINEL = (float("inf"), 0.0, None, None, 0, 0.0)

class _RequestContext:
    def __init__(self, queue, fut):
        self.queue = queue
        self.fut = fut
    async def __aenter__(self):
        try:
            return await self.fut
        except asyncio.CancelledError:
            self.fut.cancel()
            raise
    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is asyncio.CancelledError:
            self.fut.cancel()

class RequestQueue:
    """
    비동기 API 요청 큐/레이트리밋/재시도/로깅/동적 weight/복구/취소/정확성/상태 저장/실제 동시성
    - concurrency_limit만큼 워커 동시 실행
    - 429/418 등 재시도(지수 백오프+jitter)
    - 큐 상태 로깅, 동적 weight, fail-safe 복구대기, consumer 취소/정상 shutdown
    - API weight_map 동적 갱신, tokens 상태 저장/복구(풀토큰 재시작)
    """

    def __init__(
        self,
        config: Dict[str, Any],
        state: Dict[str, Any],
        concurrency_limit: int = 8,
        rate_limit: int = 1200,
        interval: float = 60.0,
        request_timeout: float = 30.0,
        max_queue: int = 10000,
    ):
        if rate_limit <= 0 or interval <= 0 or concurrency_limit <= 0 or max_queue <= 0:
            raise ValueError("invalid rate/concurrency/interval/max_queue")
        self.config = config
        self.state = state
        self.concurrency_limit = concurrency_limit
        self.rate_limit = rate_limit
        self.interval = interval
        self.request_timeout = request_timeout
        self.max_queue = max_queue
        self._queue = asyncio.PriorityQueue(maxsize=max_queue)
        self._tokens = float(state.get("request_queue_tokens", rate_limit))
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        self._sem = asyncio.Semaphore(concurrency_limit)
        self._shutdown = False
        self._cond = asyncio.Condition()
        self._weight_map = config.get("api", {}).get("weight_map", {})
        self._last_weight_update = 0.0
        self._shutdown_event = asyncio.Event()
        self._workers: list[asyncio.Task] = []

    async def update_weight_map(self, client: Any):
        # 1시간마다 갱신, 구조 변화 방어
        if time.monotonic() - self._last_weight_update < 3600:
            return
        try:
            info = await client.get_exchange_info()
            new_map = {}
            for ep in info.get("rateLimits", []):
                if "endpoint" in ep and "weight" in ep:
                    new_map[ep["endpoint"]] = ep["weight"]
            self._weight_map = new_map
            self._last_weight_update = time.monotonic()
            logging.info(f"Updated weight_map: {self._weight_map}")
        except Exception as e:
            logging.error(f"Failed to update weight_map: {e}")

    async def _refill_tokens(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed > 0:
            tokens_to_add = (elapsed / self.interval) * self.rate_limit
            self._tokens = min(self.rate_limit, self._tokens + tokens_to_add)
            self._last_refill = now
            self.state["request_queue_tokens"] = self._tokens
            logging.debug(
                f"Refilled tokens: {self._tokens:.2f}, queue_size: {self._queue.qsize()}"
            )

    async def acquire(self, weight: int = 1):
        if weight > self.rate_limit:
            raise ValueError(f"weight({weight}) > rate_limit({self.rate_limit}) would never acquire")
        while True:
            if self._shutdown:
                raise asyncio.CancelledError("RequestQueue shutting down")
            async with self._lock:
                await self._refill_tokens()
                if self._tokens >= weight:
                    self._tokens -= weight
                    return
            await asyncio.sleep(min(0.1, max(0.0, self.interval / self.rate_limit)))

    async def put(
        self,
        func: Callable[[], Awaitable[Any]],
        weight: int = 1,
        priority: int = 0,
        endpoint: Optional[str] = None,
        timeout: Optional[float] = None,
        queue_timeout: Optional[float] = None
    ):
        if not self._workers:
            raise RuntimeError("RequestQueue workers not started. Call start() before put().")
        if self._shutdown:
            raise RuntimeError("RequestQueue is shutting down")
        max_wait = queue_timeout or 10.0
        start = time.monotonic()
        while self._queue.qsize() >= self.max_queue:
            if time.monotonic() - start > max_wait:
                raise RuntimeError(f"RequestQueue is full after {max_wait}s")
            logging.warning(f"Queue full (size={self._queue.qsize()}), waiting for space")
            await asyncio.sleep(0.1)
        effective_weight = self._weight_map.get(endpoint, weight) if endpoint else weight
        if effective_weight > self.rate_limit:
            raise ValueError(
                f"effective weight({effective_weight}) > rate_limit({self.rate_limit})"
            )
        fut = asyncio.get_running_loop().create_future()
        await self._queue.put(
            (-priority, time.monotonic(), fut, func, effective_weight, timeout or self.request_timeout)
        )
        logging.debug(
            f"Request queued: endpoint={endpoint}, priority={priority}, weight={effective_weight}, queue_size={self._queue.qsize()}"
        )
        return _RequestContext(self, fut)

    def start(self, num_workers: Optional[int] = None):
        n = num_workers or self.concurrency_limit
        if self._workers:
            return
        for _ in range(n):
            self._workers.append(asyncio.create_task(self.runner()))

    async def runner(self):
        try:
            while True:
                priority, ts, fut, func, weight, timeout = await self._queue.get()
                try:
                    if func is None:  # SENTINEL for shutdown
                        self._shutdown_event.set()
                        break
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            await self.acquire(weight)
                            async with self._sem:
                                coro = func()
                                try:
                                    resp = await asyncio.wait_for(coro, timeout=timeout)
                                    if not fut.cancelled() and not fut.done():
                                        fut.set_result(resp)
                                    break
                                except asyncio.TimeoutError as e:
                                    if not fut.cancelled() and not fut.done():
                                        fut.set_exception(asyncio.TimeoutError(f"Request timed out after {timeout} sec"))
                                    break
                                except Exception as e:
                                    if attempt < max_retries - 1:
                                        backoff = min(60.0, 2 ** attempt) + random.uniform(0, 0.5)
                                        logging.error(f"Request failed (attempt {attempt+1}/{max_retries}): {e}, retrying in {backoff:.2f}s")
                                        await asyncio.sleep(backoff)
                                    else:
                                        if not fut.cancelled() and not fut.done():
                                            fut.set_exception(e)
                        except Exception as e:
                            if not fut.cancelled() and not fut.done():
                                fut.set_exception(e)
                            break
                finally:
                    self._queue.task_done()
        except asyncio.CancelledError:
            pass
        finally:
            # 종료 시 잔여 요청 모두 취소
            while not self._queue.empty():
                try:
                    pr, ts, fut, func, w, t = self._queue.get_nowait()
                    if fut and not fut.done():
                        fut.set_exception(asyncio.CancelledError("RequestQueue shutting down"))
                    self._queue.task_done()
                except Exception:
                    break

    async def shutdown(self, cancel_pending: bool = True):
        self._shutdown = True

        # 1) 대기 작업 취소/드레인 (센티널 넣기 전에)
        if cancel_pending:
            while not self._queue.empty():
                pr, ts, fut, func, w, t = self._queue.get_nowait()
                if fut and not fut.done():
                    fut.set_exception(asyncio.CancelledError("RequestQueue shutting down"))
                self._queue.task_done()

        # 2) 워커 수만큼 센티널 삽입
        for _ in range(len(self._workers) or 1):
            await self._queue.put(SENTINEL)

        # 3) 워커 종료 대기
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
            self._workers.clear()

        # 4) 상태 저장(토큰만)
        self.state["request_queue_tokens"] = self._tokens

    @property
    def queue_size(self):
        return self._queue.qsize()

    @property
    def tokens_left(self):
        return int(self._tokens)