class QualityChecker:
    def __init__(self, missing_pct_threshold=5, zscore_threshold=4.0):
        self.missing_pct_threshold = missing_pct_threshold
        self.zscore_threshold = zscore_threshold

    def check(self, data):
        # 예시: 이상치/결측률 체크
        missing_pct = (data.count(None) / len(data)) * 100 if data else 0
        if missing_pct > self.missing_pct_threshold:
            return False
        # zscore 판정 등 추가
        return True