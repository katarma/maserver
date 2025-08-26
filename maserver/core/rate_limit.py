import asyncio
import structlog

log = structlog.get_logger()

class RequestQueue:
    def __init__(self, minute_limit=2400, window_sec=60, burst_tokens=100):
        self.minute_limit = minute_limit
        self.window_sec = window_sec
        self.burst_tokens = burst_tokens

    def update_limits(self, minute_limit=None):
        if minute_limit is not None:
            log.info("RequestQueue limits updated", minute_limit=minute_limit)
            self.minute_limit = minute_limit