import json
import asyncio
import time
import os
import shutil
import copy
import math
import random
import contextlib
from typing import Any, Dict, Tuple, Callable

try:
    from filelock import FileLock, Timeout
except ImportError:
    FileLock = None
    class Timeout(Exception):
        pass

import aiofiles
from contextlib import asynccontextmanager

class StateError(Exception):
    pass

class State:
    """
    운영 중간 상태 관리를 위한 안전한 key-value 스냅샷 모듈.
    - atomic load/save, 복원력, 파일락(비동기), deepcopy 스냅샷
    - 비동기 I/O, 스키마 검증, 음수/NaN/Inf/크기 가드, .bak 복구
    - 단조 timestamp 갱신(set_monotonic_ts, atomic_set_monotonic_ts)
    - 프로세스 원자적 갱신(atomic_update), 락 순서 표준화로 교착 방지
    """

    def __init__(self, path: str = "state/state.json", use_process_lock: bool = False, lock_timeout: float = 5.0, max_bytes: int = 16 * 1024 * 1024):
        self._path = path
        self._lock = asyncio.Lock()
        self._data: Dict[str, Any] = {}
        self._last_loaded = 0.0
        self._max_bytes = max_bytes
        if use_process_lock and FileLock is None:
            raise StateError("process lock requested but 'filelock' is not installed")
        self._plock = FileLock(self._path + ".lock", timeout=lock_timeout) if use_process_lock else None

    async def _validate(self, data: Dict[str, Any]) -> None:
        if not isinstance(data, dict):
            raise StateError("State root must be a dict")
        for k, v in data.items():
            if k.endswith("_ts_utc"):
                if (
                    not isinstance(v, (int, float))
                    or v < 0
                    or not math.isfinite(v)
                ):
                    raise StateError(f"Invalid timestamp for {k}: {v}")
            if k == "request_queue_tokens":
                if (
                    not isinstance(v, (int, float))
                    or v < 0
                    or not math.isfinite(v)
                ):
                    raise StateError(f"Invalid request_queue_tokens: {v}")
            if k == "last_indicator_ts_utc":
                if (
                    not isinstance(v, (int, float))
                    or v < 0
                    or not math.isfinite(v)
                ):
                    raise StateError(f"Invalid last_indicator_ts_utc: {v}")

    @asynccontextmanager
    async def _proc_lock(self):
        if self._plock:
            await asyncio.to_thread(self._plock.acquire)
            try:
                yield
            finally:
                await asyncio.to_thread(self._plock.release)
        else:
            yield

    async def _write_locked(self, data: Dict[str, Any]) -> None:
        await self._validate(data)
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        tmp_path = self._path + ".tmp"
        if os.path.exists(self._path):
            try:
                shutil.copyfile(self._path, self._path + ".bak")
            except Exception:
                pass
        blob = json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False)
        if len(blob.encode("utf-8")) > self._max_bytes:
            raise StateError("State payload too large")
        async with aiofiles.open(tmp_path, "w", encoding="utf-8") as f:
            await f.write(blob)
            await f.flush()
        fd = await asyncio.to_thread(os.open, tmp_path, os.O_RDONLY)
        try:
            try:
                await asyncio.to_thread(os.fsync, fd)
            except OSError:
                pass
        finally:
            os.close(fd)
        delay = 0.05
        for attempt in range(3):
            try:
                os.replace(tmp_path, self._path)
                break
            except PermissionError:
                if attempt == 2:
                    raise
                await asyncio.sleep(delay + random.uniform(0, delay))
                delay = min(0.5, delay * 2)
        try:
            dir_fd = await asyncio.to_thread(os.open, os.path.dirname(self._path) or ".", os.O_RDONLY)
            try:
                await asyncio.to_thread(os.fsync, dir_fd)
            finally:
                os.close(dir_fd)
        except Exception:
            pass

    async def load(self) -> None:
        # 1) 프로세스 락에서 디스크 읽기 (메모리 반영은 나중에)
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        tmp_path = self._path + ".tmp"
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

        data = {}
        try:
            async with self._proc_lock():
                if os.path.exists(self._path) and os.path.getsize(self._path) > self._max_bytes:
                    raise StateError("State file too large")
                try:
                    async with aiofiles.open(self._path, "r", encoding="utf-8") as f:
                        txt = await f.read(self._max_bytes + 1)
                    if len(txt.encode("utf-8")) > self._max_bytes:
                        raise StateError("State file too large")
                    data = json.loads(txt)
                    await self._validate(data)
                except FileNotFoundError:
                    data = {}
                except (json.JSONDecodeError, StateError):
                    bak = self._path + ".bak"
                    if os.path.exists(bak) and os.path.getsize(bak) <= self._max_bytes:
                        async with aiofiles.open(bak, "r", encoding="utf-8") as f:
                            txt = await f.read(self._max_bytes + 1)
                        if len(txt.encode("utf-8")) > self._max_bytes:
                            raise StateError("State file too large")
                        data = json.loads(txt)
                        await self._validate(data)
                    else:
                        raise
        except Timeout:
            raise StateError(f"State lock timeout: {self._path}")

        # 2) in-proc 락에서 메모리 반영
        async with self._lock:
            self._data = data
            self._last_loaded = time.time()

    async def save(self) -> None:
        async with self._lock:
            data = copy.deepcopy(self._data)
        try:
            async with self._proc_lock():
                await self._write_locked(data)
        except Timeout:
            raise StateError(f"State lock timeout: {self._path}")

    async def atomic_update(self, updater: Callable[[Dict[str, Any]], Dict[str, Any]]) -> None:
        if self._plock is None:
            raise StateError("atomic_update requires process lock (use_process_lock=True)")
        try:
            async with self._proc_lock():
                tmp_path = self._path + ".tmp"
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass
                base = {}
                if os.path.exists(self._path):
                    if os.path.getsize(self._path) > self._max_bytes:
                        raise StateError("State file too large")
                    try:
                        async with aiofiles.open(self._path, "r", encoding="utf-8") as f:
                            txt = await f.read(self._max_bytes + 1)
                        if len(txt.encode("utf-8")) > self._max_bytes:
                            raise StateError("State file too large")
                        base = json.loads(txt)
                        await self._validate(base)
                    except (json.JSONDecodeError, StateError):
                        bak = self._path + ".bak"
                        if os.path.exists(bak) and os.path.getsize(bak) <= self._max_bytes:
                            async with aiofiles.open(bak, "r", encoding="utf-8") as f:
                                txt = await f.read(self._max_bytes + 1)
                            if len(txt.encode("utf-8")) > self._max_bytes:
                                raise StateError("State file too large")
                            base = json.loads(txt)
                            await self._validate(base)
                        else:
                            base = await self.snapshot()
                next_data = updater(copy.deepcopy(base))
                await self._write_locked(next_data)
                async with self._lock:
                    self._data = copy.deepcopy(next_data)
        except Timeout:
            raise StateError(f"State lock timeout: {self._path}")

    async def get(self, key: str, default: Any = None) -> Any:
        async with self._lock:
            return self._data.get(key, default)

    async def set(self, key: str, value: Any) -> None:
        async with self._lock:
            self._data[key] = value

    async def update(self, data: Dict[str, Any]) -> None:
        async with self._lock:
            self._data.update(data)

    async def set_monotonic_ts(self, key: str, new_ts: float | int) -> float:
        if not key.endswith("_ts_utc"):
            raise StateError("monotonic_ts expects a '*_ts_utc' key")
        if not isinstance(new_ts, (int, float)) or new_ts < 0 or not math.isfinite(new_ts):
            raise StateError(f"Invalid timestamp: {new_ts}")

        async with self._lock:
            curr = self._data.get(key)
            if curr is None or new_ts >= curr:
                self._data[key] = new_ts
                return new_ts
            return curr

    async def atomic_set_monotonic_ts(self, key: str, new_ts: float | int) -> float:
        if self._plock is None:
            raise StateError("atomic_set_monotonic_ts requires process lock (use_process_lock=True)")
        if not key.endswith("_ts_utc"):
            raise StateError("monotonic_ts expects a '*_ts_utc' key")
        if not isinstance(new_ts, (int, float)) or new_ts < 0 or not math.isfinite(new_ts):
            raise StateError(f"Invalid timestamp: {new_ts}")

        updated = {"value": None}
        async def _upd(d):
            curr = d.get(key, 0)
            if new_ts >= curr:
                d[key] = new_ts
                updated["value"] = new_ts
            else:
                updated["value"] = curr
            return d
        await self.atomic_update(_upd)
        return updated["value"]

    async def snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return copy.deepcopy(self._data)

    async def keys(self) -> Tuple[str, ...]:
        async with self._lock:
            return tuple(self._data.keys())

    async def clear(self) -> None:
        async with self._lock:
            self._data = {}

    @property
    def path(self) -> str:
        return self._path

    @property
    def last_loaded(self) -> float:
        return self._last_loaded

# 예시 사용 패턴:
# state = State("state/state.json", use_process_lock=True)
# await state.load()
# await state.atomic_set_monotonic_ts("last_indicator_ts_utc", ts)
# await state.atomic_update(lambda d: {**d, "last_indicator_ts_utc": max(d.get("last_indicator_ts_utc", 0), ts)})
# await state.save()
# snapshot = await state.snapshot()