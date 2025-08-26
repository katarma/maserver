from core.quality import QualityChecker

def test_quality_check_missing_pct():
    qc = QualityChecker(missing_pct_threshold=10)
    # 결측률 40% > threshold 10 → False
    assert qc.check([None, None, 1, 2, 3]) is False
    # 결측률 25% < threshold 30 → True
    qc = QualityChecker(missing_pct_threshold=30)
    assert qc.check([None, None, 1, 2, 3, 4, 5]) is True

def test_quality_check_empty():
    qc = QualityChecker()
    assert qc.check([]) is True