import logging
import asyncio
import time
import random
from typing import Dict, Any, Optional, List
from core.rate_limit import RequestQueue
from core.quality import QualityChecker
from binance.client import AsyncClient
from binance.exceptions import BinanceAPIException, BinanceRequestException
import aiohttp

class Collector:
    """
    - 완전한 비동기: AsyncClient 및 RequestQueue async 방식
    - Binance 전용: 인증/엔드포인트/타임스탬프/recvWindow 자동 관리/상한
    - 상태(state)/품질(QualityChecker) 연동: 아이덤포턴트(중복 방지), 고급 품질 검사
    - startTime 단위 통일, 엔드포인트 인자 안전, 시간 청크 수집 지원
    - 트랜지언트/HTTP/바이낸스 예외 재시도, Retry-After 우선 대기, -1021 예외형/딕셔너리형 모두 보정
    - CancelledError graceful 전파, 민감 파라미터 마스킹, next_start_ts 중복/누락 방지
    - 청크 경로 -1021 백오프/재시도 상한, 청크 수집 대용량 상한(OOM 방지)
    - 잡별 state 키, 빈구간 가드, 잡별 시간범위 자동 주입 제어
    """
    def __init__(
        self,
        config: Dict[str, Any],
        request_queue: RequestQueue,
        quality_checker: QualityChecker,
        state: Dict[str, Any],
        api_key: str,
        api_secret: str,
    ):
        self.config = config
        self.settings = config.get("settings", {})
        self.api_config = config.get("api", {})
        self.quality = self.settings.get("quality", {})
        self.schedule = self.settings.get("schedule", [])
        self.request_queue = request_queue
        self.quality_checker = quality_checker
        self.state = state
        self.api_key = api_key
        self.api_secret = api_secret
        self.client: Optional[AsyncClient] = None

    async def __aenter__(self):
        self.client = await AsyncClient.create(self.api_key, self.api_secret)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.client:
            await self.client.close_connection()
            self.client = None

    def _safe_params_for_log(self, params: Dict[str, Any]) -> Dict[str, Any]:
        secret_keys = {"signature", "api_key", "apikey", "token"}
        safe = {}
        for k, v in (params or {}).items():
            safe[k] = "***" if k.lower() in secret_keys else v
        return safe

    def _extract_last_ts(self, payload) -> Optional[int]:
        if isinstance(payload, list) and payload and isinstance(payload[-1], (list, tuple)) and len(payload[-1]) > 0:
            return int(payload[-1][0])
        if isinstance(payload, list) and payload and isinstance(payload[-1], dict):
            for k in ("T","E","time","eventTime","openTime","closeTime"):
                if k in payload[-1]:
                    return int(payload[-1][k])
        if isinstance(payload, dict):
            for k in ("closeTime","endTime","E","T"):
                if k in payload:
                    return int(payload[k])
        return None

    async def _invoke_with_retry(self, call, job_name, max_retries, base_backoff):
        attempt = 0
        while True:
            try:
                return await call()
            except asyncio.CancelledError:
                raise
            except (asyncio.TimeoutError, aiohttp.ClientError,
                    BinanceRequestException) as e:
                if attempt < max_retries:
                    await asyncio.sleep(min(60.0, (2 ** attempt) * base_backoff) + random.uniform(0, 0.5))
                    attempt += 1
                    continue
                logging.error(f"Transient error [{job_name}]: {e}")
                raise
            except BinanceAPIException as e:
                # (A) -1021: 타임스탬프 재동기화
                if getattr(e, "code", None) == -1021 and attempt < max_retries:
                    srv = await self.client.get_server_time()
                    offset = int(srv["serverTime"]) - int(time.time() * 1000)
                    setattr(self.client, "timestamp_offset", offset)
                    await asyncio.sleep(min(60.0, (2 ** attempt) * base_backoff) + random.uniform(0, 0.5))
                    attempt += 1
                    continue
                # (B) 429/5xx: Retry-After 우선
                status_code = getattr(e, "status_code", None)
                if status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
                    retry_after = None
                    try:
                        retry_after = e.response.headers.get("Retry-After") if e.response else None
                    except Exception:
                        retry_after = None
                    if retry_after:
                        try:
                            await asyncio.sleep(max(float(retry_after), 0.0))
                        except Exception:
                            await asyncio.sleep(min(60.0, (2 ** attempt) * base_backoff) + random.uniform(0, 0.5))
                    else:
                        await asyncio.sleep(min(60.0, (2 ** attempt) * base_backoff) + random.uniform(0, 0.5))
                    attempt += 1
                    continue
                raise

    async def _call_with_rl(self, func, weight=1, priority=0):
        if hasattr(self.request_queue, "put"):
            async with self.request_queue.put(func, weight=weight, priority=priority) as resp:
                return resp
        if hasattr(self.request_queue, "acquire"):
            await self.request_queue.acquire(weight)
            try:
                return await func()
            finally:
                pass
        logging.warning("RequestQueue has no compatible interface; calling without rate-limit")
        return await func()

    async def _collect_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        endpoint = job.get("endpoint", self.api_config.get("endpoint"))
        # 매개변수 정화
        params = {k: v for k, v in (job.get("params", {}) or {}).items() if v is not None}
        rw_cfg = job.get("recv_window", self.api_config.get("recv_window", 5000))
        recv_window = max(1, min(int(rw_cfg), 60000))
        weight = job.get("weight", self.api_config.get("weight", 1))
        priority = job.get("priority", 0)
        max_retries = int(job.get("max_retries", self.api_config.get("max_retries", 3)))
        base_backoff = float(job.get("backoff_base", self.api_config.get("backoff_base", 0.5)))
        chunk_ms = job.get("chunk_ms")
        attempt = 0
        if not endpoint:
            raise ValueError(f"Missing endpoint for job {job['name']}")

        try:
            method = getattr(self.client, endpoint)
        except AttributeError:
            raise ValueError(f"Unknown Binance endpoint: {endpoint}")

        # 시간 청크 수집(청크_ms 지정시)
        if chunk_ms:
            st = params.get("startTime")
            et = params.get("endTime")
            if st is None or et is None or st >= et:
                raise ValueError(f"{job['name']}: invalid start/end time for chunking")
            max_items = int(job.get("chunk_max_items", self.api_config.get("chunk_max_items", 100_000)))
            max_bytes = int(job.get("chunk_max_bytes", self.api_config.get("chunk_max_bytes", 20_000_000)))  # ~20MB
            acc: List[Any] = []
            total_bytes = 0
            cur = st
            attempt_1021 = 0
            while cur < et:
                cur_end = min(cur + chunk_ms, et)
                p = {k: v for k, v in {**params, "startTime": cur, "endTime": cur_end}.items() if v is not None}
                async def _invoke():
                    try:
                        return await method(**p, recvWindow=recv_window)
                    except TypeError:
                        return await method(**p)
                try:
                    resp = await self._invoke_with_retry(
                        lambda: self._call_with_rl(_invoke, weight=weight, priority=priority),
                        job['name'], max_retries, base_backoff)
                except asyncio.CancelledError:
                    raise
                if isinstance(resp, dict) and resp.get("code") == -1021:
                    srv = await self.client.get_server_time()
                    offset = int(srv["serverTime"]) - int(time.time() * 1000)
                    setattr(self.client, "timestamp_offset", offset)
                    logging.error(f"TIMESTAMP_OUT_OF_SYNC [{job['name']}], offset={offset}")
                    if attempt_1021 < max_retries:
                        await asyncio.sleep(min(60.0, (2 ** attempt_1021) * base_backoff) + random.uniform(0, 0.5))
                        attempt_1021 += 1
                        continue
                    raise ValueError(f"TIMESTAMP_OUT_OF_SYNC after retries for job {job['name']}")
                if not isinstance(resp, (dict, list)):
                    raise ValueError(f"Invalid response type for job {job['name']}: {type(resp).__name__}")
                if isinstance(resp, list):
                    acc.extend(resp)
                else:
                    acc.append(resp)
                total_bytes += len(str(resp).encode("utf-8"))
                if len(acc) > max_items:
                    acc = acc[:max_items]; break
                if total_bytes > max_bytes:
                    raise MemoryError(f"chunk payload exceeds max_bytes={max_bytes}")
                cur = cur_end
            return {"fetched_at_utc": time.time(), "payload": acc}

        # 기본(비청크) 경로
        while True:
            async def _invoke():
                try:
                    return await method(**params, recvWindow=recv_window)
                except TypeError:
                    return await method(**params)
            try:
                resp = await self._invoke_with_retry(
                    lambda: self._call_with_rl(_invoke, weight=weight, priority=priority),
                    job['name'], max_retries, base_backoff)
            except asyncio.CancelledError:
                raise
            data = resp
            if not isinstance(data, (dict, list)):
                raise ValueError(f"Invalid response type for job {job['name']}: {type(data).__name__}")
            if isinstance(data, dict) and data.get("code") == -1021:
                srv = await self.client.get_server_time()
                offset = int(srv["serverTime"]) - int(time.time() * 1000)
                setattr(self.client, "timestamp_offset", offset)
                logging.error(f"TIMESTAMP_OUT_OF_SYNC for job {job['name']}, offset={offset}")
                if attempt < max_retries:
                    await asyncio.sleep(min(60.0, (2 ** attempt) * base_backoff) + random.uniform(0, 0.5))
                    attempt += 1
                    continue
                raise ValueError(f"TIMESTAMP_OUT_OF_SYNC after retries for job {job['name']}")
            return {"fetched_at_utc": time.time(), "payload": data}

    def _validate_quality(self, result: Dict[str, Any], job: Dict[str, Any]) -> None:
        try:
            quality_flag = self.quality_checker.validate(
                data=result["payload"],
                job_name=job["name"],
                missing_pct_threshold=self.quality.get("partial_ok_threshold_pct", 5),
                zscore_threshold=self.quality.get("zscore_threshold", 4.0)
            )
            result["quality_flag"] = quality_flag
            if quality_flag in {"DEGRADED", "OUTLIER", "STALE"}:
                logging.warning(f"Quality issue [{job['name']}]: flag={quality_flag}")
        except Exception as e:
            logging.error(f"Quality check failed [{job['name']}]: {e}")
            raise

    async def collect(self, job_name: Optional[str] = None) -> Dict[str, Any]:
        jobs = [j for j in self.schedule if (not job_name or j["name"] == job_name)]
        results = {}
        for job in jobs:
            try:
                job_key = f"last_{job['name']}_ts_utc"
                start_ts = self.state.get(job_key, self.state.get("last_indicator_ts_utc", job.get("default_start_ts")))
                if start_ts is None:
                    raise ValueError(f"{job['name']}: start_ts not resolved")
                if start_ts < 10**12:
                    start_ts *= 1000
                end_ts = int(time.time() * 1000)
                # 빈 구간 가드
                if start_ts >= end_ts:
                    results[job["name"]] = {
                        "fetched_at_utc": time.time(),
                        "payload": [],
                        "next_start_ts": end_ts + 1,
                        "next_start_ts_key": job_key
                    }
                    continue
                # 시간 범위 자동 주입 잡별 제어
                use_range = job.get("use_time_range", True)
                job_params = dict(job.get("params", {}) or {})
                if use_range:
                    job_params.update({"startTime": start_ts, "endTime": end_ts})
                job = {**job, "params": job_params}
                result = await self._collect_job(job)
                self._validate_quality(result, job)
                last_ts = self._extract_last_ts(result["payload"])
                result["next_start_ts"] = (last_ts + 1) if last_ts is not None else (end_ts + 1)
                result["next_start_ts_key"] = job_key
                results[job["name"]] = result
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.error(
                    f"Collector error [{job['name']}]: {e} "
                    f"endpoint={job.get('endpoint')} params={self._safe_params_for_log(job.get('params'))}"
                )
                raise

# 사용 예시:
# async with Collector(config, queue, quality_checker, state, api_key, api_secret) as collector:
#     results = await collector.collect()