import os
import sys
import time
import json
import argparse
import logging
import signal
import asyncio
import yaml
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()
from filelock import FileLock
import structlog
from binance.client import Client

# --- Timezone UTC 고정 (선택적 권장)
if os.getenv("FORCE_UTC", "0") == "1":
    os.environ.setdefault("TZ", "UTC")
    try:
        time.tzset()
    except Exception:
        pass

# --- Logging (structlog with level/timestamp/JSON, 파일 인코딩 보장) ---
def setup_logging(level="INFO", logfile=None):
    handlers = [logging.StreamHandler(sys.stdout)]
    if logfile:
        handlers.append(logging.FileHandler(logfile, encoding="utf-8"))
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        handlers=handlers,
    )
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
    )
log = structlog.get_logger()

def load_env():
    load_dotenv()
    return {
        "BINANCE_API_KEY": os.getenv("BINANCE_API_KEY"),
        "BINANCE_API_SECRET": os.getenv("BINANCE_API_SECRET"),
        "RUN_MODE": os.getenv("RUN_MODE", "production"),
    }

def _safe_load_yaml(path: Path) -> dict:
    if not path.exists():
        log.warning("Missing config file", path=str(path))
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return data if isinstance(data, dict) else {}
    except Exception as e:
        log.error("Failed to read YAML", path=str(path), error=str(e))
        return {}

def load_yaml_config(config_dir: str) -> dict:
    base = Path(config_dir)
    names = ["api", "settings", "adaptive_thresholds", "indicators", "alerts"]
    cfg = {}
    for n in names:
        cfg[n] = _safe_load_yaml(base / f"{n}.yaml")
    return cfg

def init_state(days_back=7):
    now = int(time.time())
    return {
        "version": 1,
        "created_at_utc": now,
        "last_indicator_ts_utc": now - days_back*24*3600,
    }

def save_state(path, state):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(p) + ".lock")
    with lock:
        tmp = str(p) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(tmp, str(p))

def load_state(path):
    p = Path(path)
    lock = FileLock(str(p) + ".lock")
    with lock:
        if not p.exists():
            log.warning("State file not found", path=str(p))
            return init_state()
        try:
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
                if "last_indicator_ts_utc" not in data:
                    log.warning("State missing last_indicator_ts_utc; re-init")
                    return init_state()
                return data
        except Exception as e:
            log.error("State read failed, re-init", path=str(p), error=str(e))
            return init_state()

def validate_api_key(env: dict):
    client = Client(env["BINANCE_API_KEY"], env["BINANCE_API_SECRET"])
    try:
        client.get_system_status()
        # 실제 권한 검증 로직(엔드포인트/메서드/API 별도 구현 필요)
        # 예시: client.get_api_key_permission() 등
    except Exception as e:
        log.error("API key validation failed", error=str(e))
        sys.exit(1)

def check_memory(threshold_mb: int = 2000):
    try:
        import psutil
        mem = psutil.virtual_memory()
        if mem.available / (1024 * 1024) < threshold_mb:
            log.warning("Low available memory; triggering GC", available_mb=int(mem.available/1024/1024))
            import gc; gc.collect()
    except Exception as e:
        log.info("Memory check skipped", reason=str(e))

async def main_async():
    setup_logging(os.getenv("LOG_LEVEL", "INFO"), logfile="maserver.log")
    env = load_env()
    if not env.get("BINANCE_API_KEY") or not env.get("BINANCE_API_SECRET"):
        log.error("API 키 또는 시크릿 미설정 (.env 확인)")
        sys.exit(1)
    configs = load_yaml_config("config")
    try:
        validate_api_key(env)
    except SystemExit:
        raise
    except Exception as e:
        log.error("API 키 검증 오류", error=str(e))
        sys.exit(1)
    from core.versions import inject_version_hashes
    configs["indicators"] = inject_version_hashes(configs.get("indicators", {}) or {})
    rl = (configs.get("api", {}).get("ratelimit", {}) or {})
    futures_minute_limit = (rl.get("minute_limit", {}) or {}).get("futures", 2400)
    from core.rate_limit import RequestQueue
    queue = RequestQueue(
        minute_limit=futures_minute_limit,
        window_sec=60,
        burst_tokens=100,
    )
    state = load_state("state/state.json")
    from core.quality import QualityChecker
    quality_cfg = configs.get("settings", {}).get("quality", {})
    quality_checker = QualityChecker(
        missing_pct_threshold=quality_cfg.get("partial_ok_threshold_pct", 5),
        zscore_threshold=quality_cfg.get("zscore_threshold", 4.0)
    )
    from core.app import MAServerApp
    app = MAServerApp(env=env, configs=configs, state=state, request_queue=queue, quality_checker=quality_checker)
    loop = asyncio.get_running_loop()
    try:
        if hasattr(signal, "SIGHUP"):
            def _on_hup():
                log.info("SIGHUP received, reloading configs")
                try:
                    new_cfg = load_yaml_config("config")
                    from core.versions import inject_version_hashes
                    new_cfg["indicators"] = inject_version_hashes(new_cfg.get("indicators", {}) or {})
                    app.reload_configs(new_cfg)
                    new_rl = (new_cfg.get("api", {}).get("ratelimit", {}) or {})
                    new_limit = (new_rl.get("minute_limit", {}) or {}).get("futures", futures_minute_limit)
                    try:
                        queue.update_limits(minute_limit=new_limit)
                    except Exception:
                        pass
                except Exception as e:
                    log.error("Config reload failed", error=str(e))
            loop.add_signal_handler(signal.SIGHUP, _on_hup)
    except (NotImplementedError, RuntimeError):
        log.info("Signal handling not available on this platform")
    stop_event = asyncio.Event()
    async def _on_term():
        log.warning("Termination signal received, stopping app gracefully")
        try:
            await app.stop()
        except Exception as e:
            log.error("App stop failed", error=str(e))
        stop_event.set()
    for sig_name in ("SIGTERM", "SIGINT"):
        if hasattr(signal, sig_name):
            try:
                loop.add_signal_handler(getattr(signal, sig_name), lambda: asyncio.create_task(_on_term()))
            except Exception:
                signal.signal(getattr(signal, sig_name), lambda s,f: asyncio.create_task(_on_term()))
    app_task = asyncio.create_task(app.run())
    try:
        done, pending = await asyncio.wait(
            {app_task, asyncio.create_task(stop_event.wait())},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_event.is_set() and not app_task.done():
            try:
                await app.stop()
            except Exception as e:
                log.error("App stop failed", error=str(e))
    finally:
        try:
            save_state("state/state.json", getattr(app, "state", state))
        except Exception as e:
            log.error("State save failed", error=str(e))
    check_memory()

if __name__ == "__main__":
    asyncio.run(main_async())