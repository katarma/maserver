# Maserver

**Binance BTCUSDT Perpetual 관찰/분석 자동화 프로그램**  
노트북(≤8GB) 환경 최적화 · 에러 내성 · 아이덤포턴트 · 개발자 배포판

---

## 개요

Maserver는 바이낸스 API 기반으로 BTCUSDT Perpetual 시장을 5분/15분 단위로 관찰·분석하여, 품질 관리된 데이터와 Markdown 보고서를 자동 생성합니다.  
예측이 아닌 관찰에 집중하며, 감정·심리·동기화 신호를 수치화하여 제공합니다.

---

## 주요 특징

- **에러 내성**: atomic+lock 저장, RequestQueue(예상 weight), 429/418/WS 드랍 자동 복구
- **데이터 품질**: quality_flag 세분화(OK/PARTIAL_OK/DEGRADED/OUTLIER/STALE), 기간누락/이상치/스테일 검증
- **아이덤포턴트**: 재실행 안전(state.json 기준), 중복 수집·저장 방지
- **노트북 친화**: 경량 연산(Polars scan/streaming), 메모리 자동 관리, 병렬 I/O 제한
- **확장성**: Docker/Compose 환경, config/*.yaml 설정 기반, 모듈형 구조

---

## 디렉토리 구조

```
maserver/
  main.py
  config/
    api.yaml
    settings.yaml
    adaptive_thresholds.yaml
    indicators.yaml
    alerts.yaml
  state/
    state.json
  data/
    raw/
      klines/<symbol>/<interval>/<YYYY-MM-DD>.parquet
      ...
    derived/
      indicators/<category>/<metric>/<YYYY-MM-DD>.parquet
  reports/
    YYYY-MM-DD/HHMM_report.md(.gz)
  modules/
    scheduler.py
    collector.py
    ws_collector.py
    indicator/
      price_volume.py
      ...
    analyzer.py
    reporter.py
    storage.py
    config_loader.py
  core/
    io_store.py
    time_sync.py
    dedupe.py
    rate_limit.py
    ws_backfill.py
    quality.py
    versions.py
    logging.py
    concurrency.py
    consistency.py
  tests/
    unit/
    sims/
  docker/
    Dockerfile
    compose.yaml
  README.md
  .gitignore
  requirements.txt or pyproject.toml
```

---

## 설치 및 실행

1. **의존성 설치**
   ```bash
   pip install -r requirements.txt
   ```
   또는
   ```bash
   poetry install
   ```

2. **환경 변수 설정**
   - `.env` 파일에 바이낸스 API 키/시크릿 입력
   - 예시:
     ```
     BINANCE_API_KEY=xxxxxx
     BINANCE_API_SECRET=yyyyyy
     ```

3. **설정 파일 확인 및 편집**
   - `config/api.yaml`, `settings.yaml` 등에서 레이트리밋, 품질 플래그, 엔드포인트 on/off 등 옵션 조정

4. **Docker 실행 (선택)**
   ```bash
   docker compose up
   ```

5. **메인 프로그램 실행**
   ```bash
   python main.py
   ```

---

## 사용 방법

- **자동 수집·분석**: 5분 단위 REST/WS 데이터 수집, 15분 단위 분석·보고서 생성
- **보고서 출력**: `reports/YYYY-MM-DD/HHMM_report.md(.gz)`
- **품질 요약**: 보고서 하단에 품질/이상치/누락 등 텍스트 메트릭 제공

---

## 필수 설정 파일

- `config/api.yaml` : 바이낸스 API 권한, 레이트리밋, RequestQueue 옵션
- `config/settings.yaml` : 수집 주기/윈도우, 품질 임계치, 보고서 옵션
- `config/indicators.yaml` : 지표 정의, 버전 해시
- `config/alerts.yaml` : 알람 레벨, 쿨다운, 집계 정책
- `config/adaptive_thresholds.yaml` : 백분위 기반 임계값

---

## 에러·품질 관리

- 모든 저장은 atomic+lock 보장
- RequestQueue로 예상 weight 기반 안전한 API 호출
- 429/418/WS 드랍 발생 시 자동 백오프+재시도
- 품질 플래그(OK/PARTIAL_OK/DEGRADED/OUTLIER/STALE)로 데이터 상태 표시
- state.json을 통한 재실행 안전성

---

## 테스트 및 배포

- **단위테스트**: `tests/unit/`
- **시뮬레이션**: `tests/sims/` (WS/레이트리밋/기간누락/극단치)
- **백테스트**: 과거 Parquet 데이터로 검증
- **Docker 이미지**: 경량/자원 친화적, 노트북 환경에서 안정 동작

---

## 참고 문서

- [blueprint0826.md] : 전체 설계서
- [order0826.md] : 개발 작업 순서
- [guideline0826.md] : 상세 설계 지침
- [indicator0826.md] : 모든 지표/알람 정의

---

## 문의 및 기여

- 에러/버그/제안은 GitHub Issue로 등록
- PR(풀리퀘) 기여 환영, 모듈별 테스트 필수
- 커뮤니티/협업은 개발자 배포판 기준

---

**Maserver는 예측이 아닌 관찰의 도구입니다. 데이터 품질과 에러 내성, 아이덤포턴트 실행을 최우선으로 설계되었습니다.**
