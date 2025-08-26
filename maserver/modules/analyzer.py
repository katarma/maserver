import logging
import time
from typing import Dict, Any, Optional, List, Tuple, Union
import numpy as np

class Analyzer:
    """
    품질/상태 분석 - 설계지침 완전 준수
    - flag: missing_pct 우선, coverage 보조, 빈 데이터 방어
    - flag: STALE 최상위 우선, OUTLIER 다음, 그 외 OK/DEGRADED/PARTIAL_OK
    - 타임스탬프: 전체 후보 중 최대값 기반, ms/s 정규화 + 미래값 방어(초/밀리초 모두)
    - 결측: None, "", {}, NaN, Inf 모두 포함
    - 이상치/트렌드: 시간순 정렬 후 계산
    - robust MAD 이상치: z-score 스케일 보정(1.4826)
    - Binance 숫자 문자열 float 변환 후 np.isfinite
    - 대용량 입력: 최근 max_points만 사용(메모리/시간 상한)
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.quality_thresholds = config.get("quality", {})
        self.outlier_threshold = self.quality_thresholds.get("zscore_threshold", 4.0)
        self.partial_ok_threshold_pct = self.quality_thresholds.get("partial_ok_threshold_pct", 5)
        self.stale_threshold_sec = self.quality_thresholds.get("stale_threshold_sec", 3600)
        self.coverage_field = config.get("coverage_field", "coverage_pct")
        self.robust_outlier = config.get("robust_outlier", True)
        self.trend_method = config.get("trend_method", "edge")
        self.mad_scale = config.get("mad_scale", 1.4826)
        self.max_points = int(config.get("max_points", 50000))  # 대용량 상한

    def analyze(self, payload: Union[List[Any], Dict[str, Any]], job_meta: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(payload, list) and len(payload) == 0:
            return {"missing_pct": 100.0, "coverage": None, "flag": "DEGRADED",
                    "last_ts": None, "is_stale": False, "outlier": False, "trend": None}
        if isinstance(payload, dict) and not payload:
            return {"missing_pct": 100.0, "coverage": None, "flag": "DEGRADED",
                    "last_ts": None, "is_stale": False, "outlier": False, "trend": None}

        coverage = self._extract_coverage(payload, job_meta)
        missing = self._missing_pct(payload)
        flag = None
        if missing is not None:
            flag = "DEGRADED" if missing >= self.partial_ok_threshold_pct else "PARTIAL_OK"
        elif coverage is not None:
            flag = "DEGRADED" if coverage < self.partial_ok_threshold_pct else ("PARTIAL_OK" if coverage < 100 else "OK")
        result = {"coverage": coverage, "missing_pct": missing}

        last_ts = self._extract_last_ts(payload)
        now_ts = int(time.time())
        is_stale = False
        if last_ts is not None:
            if (now_ts - last_ts) > self.stale_threshold_sec:
                is_stale = True

        result["last_ts"] = last_ts
        result["is_stale"] = is_stale

        outlier = self._detect_outlier(payload, job_meta)
        if self.robust_outlier:
            outlier = outlier or self._detect_outlier_robust(payload, job_meta)
        result["outlier"] = outlier

        result["trend"] = self._trend_analysis(payload, job_meta)

        if is_stale:
            result["flag"] = "STALE"
        elif outlier:
            result["flag"] = "OUTLIER"
        elif flag is None:
            result["flag"] = "OK"
        else:
            result["flag"] = flag

        return result

    def _extract_coverage(self, payload, job_meta) -> Optional[float]:
        if isinstance(payload, dict):
            if self.coverage_field in payload:
                try:
                    return float(payload[self.coverage_field])
                except Exception:
                    pass
        if isinstance(payload, list):
            expected = job_meta.get("expected_count")
            if isinstance(expected, int) and expected > 0:
                return (len(payload) / expected) * 100.0
        return None

    def _is_missing(self, x) -> bool:
        if x is None:
            return True
        if isinstance(x, str) and x.strip() == "":
            return True
        if isinstance(x, dict) and not x:
            return True
        if isinstance(x, (int, float)) and not np.isfinite(x):
            return True
        try:
            if np.isnan(x):
                return True
        except Exception:
            pass
        return False

    def _missing_pct(self, payload) -> Optional[float]:
        if isinstance(payload, list):
            total = len(payload)
            if total == 0:
                return 100.0
            missing = 0
            for x in payload:
                if self._is_missing(x):
                    missing += 1
            return (missing / total) * 100.0
        return None

    def _extract_last_ts(self, payload) -> Optional[int]:
        """
        전체 후보 타임스탬프 중 최대값을 반환하며,
        반환값은 항상 UTC 초(秒) 단위임 (ms→s 변환 포함).
        미래값(5분 초과)은 현재시각으로 클램프.
        ms/s 혼용 일관 처리.
        """
        cands = []
        if isinstance(payload, list):
            for x in payload:
                if isinstance(x, dict):
                    for k in ("T","E","time","eventTime","openTime","closeTime","endTime"):
                        v = x.get(k)
                        if isinstance(v, (int, float)) and v > 0:
                            cands.append(int(v))
                elif isinstance(x, (list, tuple)):
                    for idx in [0, 6]:
                        if len(x) > idx and isinstance(x[idx], (int, float)) and x[idx] > 0:
                            cands.append(int(x[idx]))
        if isinstance(payload, dict):
            for k in ("closeTime","endTime","E","T"):
                v = payload.get(k)
                if isinstance(v, (int, float)) and v > 0:
                    cands.append(int(v))
        if not cands:
            return None
        ts = max(cands)
        ts_ms = ts if ts >= 10**12 else ts * 1000
        now_ms = int(time.time() * 1000)
        if ts_ms > now_ms + 5 * 60 * 1000:
            ts_ms = now_ms
        return ts_ms // 1000

    def _values_series(self, payload, job_meta):
        value_field = job_meta.get("value_field")
        value_index = job_meta.get("value_index")
        ts_keys = ("T","E","time","eventTime","openTime","closeTime","endTime")
        series = []
        if not (value_field or (value_index is not None)) or not isinstance(payload, list):
            return None
        for x in payload:
            ts = None; val = None
            if isinstance(x, dict):
                for k in ts_keys:
                    v = x.get(k)
                    if isinstance(v, (int, float)) and v > 0:
                        ts = int(v); break
                if value_field and (vf := x.get(value_field)) is not None:
                    try:
                        fv = float(vf)
                        if np.isfinite(fv): val = fv
                    except: pass
            elif isinstance(x, (list, tuple)):
                if len(x) > 0 and isinstance(x[0], (int, float)) and x[0] > 0:
                    ts = int(x[0])
                if value_index is not None and len(x) > value_index:
                    try:
                        fv = float(x[value_index])
                        if np.isfinite(fv): val = fv
                    except: pass
            if ts is not None and val is not None:
                ts_ms = ts if ts >= 10**12 else ts*1000
                series.append((ts_ms, val))
        if not series: 
            return None
        series.sort(key=lambda t: t[0])
        vals = [v for _, v in series]
        if len(vals) > self.max_points:
            vals = vals[-self.max_points:]  # 최근 N개만
        return vals

    def _detect_outlier(self, payload, job_meta) -> bool:
        values = self._values_series(payload, job_meta)
        if not values or len(values) < 5:
            return False
        arr = np.asarray(values, dtype=float)
        std = np.std(arr)
        if std == 0:
            return False
        mean = np.mean(arr)
        zscores = np.abs((arr - mean) / std)
        return np.max(zscores) > self.outlier_threshold

    def _detect_outlier_robust(self, payload, job_meta) -> bool:
        values = self._values_series(payload, job_meta)
        if not values or len(values) < 5:
            return False
        arr = np.asarray(values, dtype=float)
        q1, q3 = np.percentile(arr, [25, 75])
        iqr = q3 - q1
        out_iqr = np.sum((arr < q1 - 1.5 * iqr) | (arr > q3 + 1.5 * iqr))
        if out_iqr > 0:
            return True
        median = np.median(arr)
        mad = np.median(np.abs(arr - median))
        if mad == 0:
            return False
        mad_z = np.abs((arr - median) / mad) * self.mad_scale
        return np.max(mad_z) > self.outlier_threshold

    def _trend_analysis(self, payload, job_meta) -> Optional[str]:
        values = self._values_series(payload, job_meta)
        if not values or len(values) < 2:
            return None
        if self.trend_method == "edge":
            if values[-1] > values[0]:
                return "UP"
            elif values[-1] < values[0]:
                return "DOWN"
            else:
                return "FLAT"
        if self.trend_method == "linear":
            x = np.arange(len(values))
            try:
                coeffs = np.polyfit(x, values, 1)
                slope = coeffs[0]
                if slope > 0:
                    return "UP"
                elif slope < 0:
                    return "DOWN"
                else:
                    return "FLAT"
            except Exception:
                return None
        if self.trend_method == "pct":
            pct_change = (values[-1] - values[0]) / (abs(values[0]) + 1e-8)
            if pct_change > 0.01:
                return "UP"
            elif pct_change < -0.01:
                return "DOWN"
            else:
                return "FLAT"
        return None