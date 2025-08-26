import asyncio
import random
import logging
import inspect
import time as _t
from math import fmod
from typing import Callable, Awaitable, Any, List, Dict, Optional

class ScheduledJob:
    def __init__(
        self,
        coro: Callable[[], Awaitable[Any]],
        interval_sec: int,
        jitter_sec: int = 0,
        name: str = "",
        max_retries: int = 3,
        priority: int = 0,
        depends_on: Optional[List[str]] = None,
        align_to_wall: bool = False,
    ):
        if interval_sec <= 0:
            raise ValueError("interval_sec must be > 0")
        if jitter_sec < 0:
            raise ValueError("jitter_sec must be >= 0")
        if not callable(coro):
            raise TypeError("coro must be a callable returning an awaitable")
        self.coro = coro
        self.interval_sec = interval_sec
        self.jitter_sec = jitter_sec
        self.max_retries = max_retries
        self.priority = priority
        self.depends_on = depends_on or []
        self.align_to_wall = align_to_wall
        self.task: Optional[asyncio.Task] = None
        self.running = False
        self.name = name or getattr(coro, "__name__", "scheduled_job")

    async def run_loop(self, last_run: Dict[str, float], state: Dict[str, float]):
        self.running = True
        try:
            base = _t.monotonic()
            if self.align_to_wall:
                now_s = _t.time()
                remainder = now_s % self.interval_sec
                initial_delay = (self.interval_sec - remainder) % self.interval_sec
                if initial_delay > 0:
                    try:
                        await asyncio.sleep(initial_delay)
                    except asyncio.CancelledError:
                        return
                base = _t.monotonic()
            next_run = base
            while self.running:
                # 의존성 체크(모든 dep가 최신이어야 OK)
                deps_ok = True
                for dep in self.depends_on:
                    if dep not in last_run or (_t.time() - last_run[dep]) > self.interval_sec:
                        logging.warning(f"[{self.name}] Dependency {dep} not ready, skipping this cycle")
                        deps_ok = False
                        break
                if not deps_ok:
                    try:
                        await asyncio.sleep(min(1.0, self.interval_sec / 10))
                    except asyncio.CancelledError:
                        break
                    continue

                for attempt in range(self.max_retries):
                    try:
                        res = self.coro()
                        if inspect.isawaitable(res):
                            await res
                        else:
                            raise TypeError(f"Scheduled job {self.name} returned non-awaitable")
                        now_ts = _t.time()
                        last_run[self.name] = now_ts
                        state[f"last_{self.name}_ts_utc"] = now_ts
                        break
                    except asyncio.CancelledError:
                        logging.info(f"[{self.name}] CancelledError received, terminating run_loop.")
                        raise
                    except Exception as e:
                        backoff = min(60, 2 ** attempt) + random.uniform(0, 0.5)
                        logging.exception(
                            f"[{self.name}] Scheduled job error (attempt {attempt+1}/{self.max_retries}), retrying in {backoff:.2f}s"
                        )
                        try:
                            await asyncio.sleep(backoff)
                        except asyncio.CancelledError:
                            logging.info(f"[{self.name}] Cancelled during backoff, terminating run_loop.")
                            break
                else:
                    logging.error(f"[{self.name}] Scheduled job failed after {self.max_retries} retries. last_run={last_run.get(self.name)}, state={state.get(f'last_{self.name}_ts_utc')}")
                # monotonic 기반 드리프트 제거, 음수 지연 캐치업
                jitter = 0.0 if self.align_to_wall else (random.uniform(0, self.jitter_sec) if self.jitter_sec > 0 else 0.0)
                next_run += self.interval_sec + jitter
                now_mono = _t.monotonic()
                delay = next_run - now_mono
                if delay < 0:
                    missed = int((-delay) // max(1e-6, self.interval_sec)) + 1
                    next_run += self.interval_sec * missed
                    delay = max(0.0, next_run - _t.monotonic())
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    logging.info(f"[{self.name}] Cancelled during main sleep, terminating run_loop.")
                    break
        finally:
            self.running = False

class Scheduler:
    def __init__(
        self,
        state: Dict[str, float],
        registry: Optional[Dict[str, Callable[[], Awaitable[Any]]]] = None,
    ):
        self.jobs: List[ScheduledJob] = []
        self._tasks: List[asyncio.Task] = []
        self._running = False
        self._last_run: Dict[str, float] = {}
        self.state = state
        self.registry = registry or {}

    def add_job(
        self,
        coro: Callable[[], Awaitable[Any]],
        interval_sec: int,
        jitter_sec: int = 0,
        name: str = "",
        max_retries: int = 3,
        priority: int = 0,
        depends_on: Optional[List[str]] = None,
        align_to_wall: bool = False,
    ) -> None:
        job = ScheduledJob(
            coro, interval_sec, jitter_sec, name, max_retries, priority, depends_on, align_to_wall
        )
        if job.name in (j.name for j in self.jobs):
            raise ValueError(f"Duplicate job name: {job.name}")
        self.jobs.append(job)
        # 실행 중 추가 시 즉시 스케줄
        if self._running:
            loop = asyncio.get_running_loop()
            job.task = loop.create_task(job.run_loop(self._last_run, self.state))
            self._tasks.append(job.task)

    def start(self) -> None:
        if self._running:
            logging.warning("Scheduler already running; start() ignored.")
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError("Scheduler.start() must be called within a running event loop")
        self._running = True
        self.jobs.sort(key=lambda j: j.priority, reverse=True)
        for job in self.jobs:
            job.task = loop.create_task(job.run_loop(self._last_run, self.state))
            self._tasks.append(job.task)

    async def stop(self, timeout: float = 10.0) -> None:
        if not self._running:
            return
        self._running = False
        # 모든 태스크를 취소
        for job in self.jobs:
            if job.task:
                job.task.cancel()
        # self._tasks에 대해 취소 보장
        for t in self._tasks:
            t.cancel()
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._tasks, return_exceptions=True),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            logging.warning("Scheduler stop timeout; some jobs did not shut down in time")
        finally:
            self._tasks.clear()

    async def reload(self, new_configs: Dict[str, Any]) -> None:
        if not self._running:
            return
        await self.stop()
        self.jobs.clear()
        for job_config in new_configs.get("settings", {}).get("schedule", []):
            name = job_config.get("name", "")
            coro = self.registry.get(name) or getattr(self, name, None)
            if not coro:
                logging.error(f"Unknown job in schedule: {name}")
                continue
            self.add_job(
                coro=coro,
                interval_sec=job_config.get("interval_sec", 300),
                jitter_sec=job_config.get("jitter_sec", 0),
                name=name,
                max_retries=job_config.get("max_retries", 3),
                priority=job_config.get("priority", 0),
                depends_on=job_config.get("depends_on", []),
                align_to_wall=job_config.get("align_to_wall", False),
            )
        # start() 호출 전 _running 상태 일관성 보장됨
        self.start()

    def is_running(self) -> bool:
        return self._running