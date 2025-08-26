def inject_version_hashes(indicators_dict):
    """
    임시 버전 해시. 실제는 git/hash/etc 활용.
    """
    indicators = indicators_dict.copy()
    indicators["_version_hash"] = "dummy_version_hash"
    return indicators