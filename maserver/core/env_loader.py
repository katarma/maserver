import os
from dotenv import load_dotenv

load_dotenv()  # .env 파일 자동 로드

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

# 사용 예시
print("API Key:", BINANCE_API_KEY)