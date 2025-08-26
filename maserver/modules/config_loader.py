import os, json, logging, time, hashlib
from typing import Dict, Any, Optional, List, Set
from filelock import FileLock, Timeout
import yaml

class ConfigLoader:
    def __init__(self, config_dir: str, encoding: str = "utf-8"):
        self.config_dir = config_dir
        self.encoding = encoding
        self._config: Optional[Dict[str, Any]] = None
        self._last_mtime: Dict[str, Optional[float]] = {}
        self._last_fingerprint: Dict[str, Optional[tuple]] = {}

    def _parse(self, raw: str, ext: str) -> Dict[str, Any]:
        if ext in (".yaml", ".yml"):
            data = yaml.safe_load(raw)
        elif ext == ".json":
            data = json.loads(raw)
        else:
            raise ValueError(f"Unsupported config file extension: {ext}")
        if not isinstance(data, dict):
            raise ValueError("Top-level config must be an object/dict")
        return data

    def _read_locked(self, path: str, retries: int = 2, backoff: float = 0.05, lock_timeout: float = 2.0) -> tuple[str, float]:
        try:
            with FileLock(path + ".lock", timeout=lock_timeout):
                last_raw = ""
                stable_mtime = os.path.getmtime(path)
                for _ in range(retries + 1):
                    before = os.path.getmtime(path)
                    with open(path, "r", encoding=self.encoding) as f:
                        raw = f.read()
                    after = os.path.getmtime(path)
                    if before == after:
                        stable_mtime = after
                        return raw, stable_mtime
                    last_raw = raw
                    time.sleep(backoff)
                # 마지막 확인
                before = os.path.getmtime(path)
                with open(path, "r", encoding=self.encoding) as f:
                    final_raw = f.read()
                after = os.path.getmtime(path)
                return (final_raw, after) if before == after else (last_raw, after)
        except Timeout:
            logging.error(f"Config lock timeout for {path}")
            raise
        except IOError as e:
            logging.error(f"IO error while reading config {path}: {e}")
            raise

    def load(self) -> Dict[str, Any]:
        config = {}
        names = ["api", "settings", "adaptive_thresholds", "indicators", "alerts"]
        for name in names:
            path = os.path.join(self.config_dir, f"{name}.yaml")
            if not os.path.exists(path):
                logging.warning(f"Config file not found: {path}")
                config[name] = {}
                continue
            try:
                raw, mtime = self._read_locked(path)
                ext = os.path.splitext(path)[1].lower()
                cfg = self._parse(raw, ext)
                config[name] = cfg
                self._last_mtime[name] = mtime
                self._last_fingerprint[name] = (mtime, len(raw.encode(self.encoding)),
                                                hashlib.sha256(raw.encode(self.encoding)).hexdigest())
            except Exception as e:
                logging.error(f"Config load failed for {path}: {e}")
                raise
        self._validate(config.get("settings", {}), full_config=config)
        self._config = config
        return config

    def reload(self) -> Dict[str, Any]:
        logging.info(f"Checking reload for {self.config_dir}")
        prev = self._config
        config = {}
        names = ["api", "settings", "adaptive_thresholds", "indicators", "alerts"]
        changed_keys = []
        for name in names:
            path = os.path.join(self.config_dir, f"{name}.yaml")
            if not os.path.exists(path):
                logging.warning(f"Config file not found: {path}")
                config[name] = prev.get(name, {}) if prev else {}
                continue
            try:
                raw, mtime = self._read_locked(path)
                ext = os.path.splitext(path)[1].lower()
                new_fp = (mtime, len(raw.encode(self.encoding)),
                          hashlib.sha256(raw.encode(self.encoding)).hexdigest())
                old_fp = self._last_fingerprint.get(name)
                if old_fp and new_fp == old_fp:
                    logging.debug(f"No content change in {path}, keeping previous config")
                    config[name] = prev.get(name, {}) if prev else {}
                    continue
                cfg = self._parse(raw, ext)
                config[name] = cfg
                self._last_mtime[name] = mtime
                self._last_fingerprint[name] = new_fp
                changed_keys.append(name)
            except Exception as e:
                logging.error(f"Hot reload failed for {path}: {e}")
                config[name] = prev.get(name, {}) if prev else {}
        try:
            self._validate(config.get("settings", {}), full_config=config)
            self._config = config
            if changed_keys:
                logging.info(f"Config changed for keys: {changed_keys}")
            return config
        except Exception as e:
            logging.error(f"Config validation failed, keeping previous config: {e}")
            if prev is None:
                raise
            return prev

    @property
    def config(self) -> Optional[Dict[str, Any]]:
        return self._config

    def _validate(self, settings: Dict[str, Any], *, full_config: Optional[Dict[str, Any]] = None) -> None:
        if "schedule" not in settings:
            raise ValueError("Missing required key: settings.schedule")
        schedule = settings["schedule"]
        if not isinstance(schedule, list):
            raise ValueError("settings.schedule must be a list")
        # quality 필드 검증
        quality = settings.get("quality", {})
        if not isinstance(quality, dict):
            raise ValueError("settings.quality must be a dict")
        if "partial_ok_threshold_pct" not in quality or not isinstance(quality["partial_ok_threshold_pct"], (int, float)) or quality["partial_ok_threshold_pct"] < 0:
            raise ValueError("settings.quality.partial_ok_threshold_pct must be a non-negative number")
        if "zscore_threshold" not in quality or not isinstance(quality["zscore_threshold"], (int, float)) or quality["zscore_threshold"] <= 0:
            raise ValueError("settings.quality.zscore_threshold must be a positive number")
        names: Set[str] = set()
        name_to_interval: Dict[str, int] = {}
        for idx, job in enumerate(schedule):
            if not isinstance(job, dict):
                raise ValueError(f"Job at index {idx} must be an object: {job}")
            name = job.get("name")
            interval = job.get("interval_sec")
            jitter = job.get("jitter_sec", 0)
            max_retries = job.get("max_retries", 3)
            align = job.get("align_to_wall", False)
            deps = job.get("depends_on", [])
            if not name or not isinstance(name, str):
                raise ValueError(f"Job missing valid 'name' at index {idx}: {job}")
            if name in names:
                raise ValueError(f"Duplicate job name: {name}")
            names.add(name)
            if not isinstance(interval, int) or interval <= 0:
                raise ValueError(f"{name}: interval_sec must be positive int")
            if not (isinstance(jitter, (int, float)) and jitter >= 0):
                raise ValueError(f"{name}: jitter_sec must be >= 0")
            if not (isinstance(max_retries, int) and max_retries >= 0):
                raise ValueError(f"{name}: max_retries must be >= 0")
            if not isinstance(align, bool):
                raise ValueError(f"{name}: align_to_wall must be bool")
            if not isinstance(deps, list) or any(not isinstance(d, str) for d in deps):
                raise ValueError(f"{name}: depends_on must be list[str]")
            # align_to_wall 규칙
            if align and jitter > 0:
                raise ValueError(f"{name}: align_to_wall requires jitter_sec == 0")
            if align and (interval % 60 != 0):
                raise ValueError(f"{name}: align_to_wall requires interval_sec to be a multiple of 60")
            name_to_interval[name] = interval
        self._validate_dependencies(schedule)
        # 의존성 주기 역전 방지
        for j in schedule:
            name = j["name"]
            for d in j.get("depends_on", []):
                if name_to_interval[name] < name_to_interval[d]:
                    raise ValueError(
                        f"{name}: interval_sec ({name_to_interval[name]}) must be >= dependency '{d}' interval_sec ({name_to_interval[d]})"
                    )
        # api.ratelimit 값 검증(있을 때만)
        if full_config:
            api_cfg = full_config.get("api", {}) or {}
            rl = api_cfg.get("ratelimit", {}) or {}
            minute = rl.get("minute_limit", {}) or {}
            fut = minute.get("futures")
            if fut is not None and (not isinstance(fut, int) or fut <= 0):
                raise ValueError("api.ratelimit.minute_limit.futures must be a positive integer")

    def _validate_dependencies(self, schedule: list) -> None:
        name_to_deps = {j["name"]: j.get("depends_on", []) for j in schedule}
        for n, ds in name_to_deps.items():
            for d in ds:
                if d not in name_to_deps:
                    raise ValueError(f"{n}: depends_on references unknown job '{d}'")
                if d == n:
                    raise ValueError(f"{n}: self-dependency is not allowed")
        visiting, visited = set(), set()
        def dfs(n: str):
            if n in visited: return
            if n in visiting:
                raise ValueError(f"Dependency cycle detected at '{n}'")
            visiting.add(n)
            for d in name_to_deps[n]:
                dfs(d)
            visiting.remove(n)
            visited.add(n)
        for n in name_to_deps:
            dfs(n)

def load_config(config_dir: str) -> Dict[str, Any]:
    loader = ConfigLoader(config_dir)
    return loader.load()

def hot_reload_config(config_loader: ConfigLoader) -> Dict[str, Any]:
    return config_loader.reload()