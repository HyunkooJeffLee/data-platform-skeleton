# Architecture

> source → Airflow → StarRocks / Druid → serving

## 한눈에

```
                     ┌──────────────────────────── Airflow (orchestration) ───────────────────────────┐
                     │                                                                                 │
PostgreSQL ──────────┼─▶ ingest_incremental  ─┐                                                        │
 (운영 DB, 디멘전)    │      (high-watermark)   │                                                        │
                     │                         ├─▶ Stream Load ─▶ StarRocks ─┐                          │
                     │   backfill_partitioned ─┘                             │                          │
                     │      (파티션 단위)                                     ├─▶ BI / 조인·집계 질의    │
                     │                                                       │   (배치 OLAP 서빙)       │
                     │   data_quality_checks ──▶ (fail ⇒ 다운스트림 차단)     │                          │
                     └───────────────────────────────────────────────────── ┘                          │
                                                                                                        │
Kafka ──────────────────────────────────────────▶ Druid (Kafka supervisor) ─▶ 실시간 시계열 대시보드   │
 (센서 텔레메트리 스트림)                                                                                │
                                                                                                        ┘
```

## 각 구성요소가 맡는 것

### Source
- **PostgreSQL** — 운영 DB이자 메타데이터/디멘전 소스. `sensor_readings`(팩트)와 `dim_device`(디멘전), 그리고 파이프라인의 watermark 메타 테이블(`etl_watermark`)이 여기 있다.
- **Kafka** — 센서에서 올라오는 append-only 텔레메트리 스트림. 순수 실시간 경로의 소스.

### Orchestration — Airflow
- **ingest_incremental** — 15분 주기 증분 적재. high-watermark(마지막 성공 적재 `event_time`)로 경계를 잡고, Stream Load label로 재시도 멱등성을 보장한다.
- **backfill_partitioned** — 과거 구간을 파티션(일 단위)으로만 다시 적재한다. `catchup=True`로 스케줄러가 구간별 run을 생성하고, 각 run은 자기 파티션만 덮어쓴다. 전체 재적재는 하지 않는다.
- **data_quality_checks** — row-count·null-ratio·freshness 게이트. 위반 시 태스크를 **실패**시켜 다운스트림(예: 서빙 스왑/집계)을 막는다.

### Storage / Serving
- **StarRocks** — 배치 OLAP 서빙. 세 가지 테이블 모델을 목적별로 쓴다.
  - Duplicate Key: append-only 원천(raw) 보존.
  - Aggregate Key: 사전 집계(rollup)로 대시보드 질의 비용을 낮춘다.
  - Primary Key: 디멘전 업서트(CDC로 갱신되는 기기 메타).
  - 조인·집계·ad-hoc SQL이 많은 BI 워크로드가 여기로 온다.
- **Druid** — 실시간 시계열. Kafka supervisor로 스트림을 인제스트하고 ingest-time rollup으로 초 단위 신선도의 시계열 대시보드를 서빙한다.

## 왜 StarRocks와 Druid를 함께 두는가

- **Druid**는 append-only 스트림의 실시간 인제스트와 시간축 필터/topN/timeseries에 강하다. 실시간 운영 대시보드는 계속 Druid가 맡는다.
- **StarRocks**는 MPP 조인·CBO·materialized view·업서트가 필요한 배치 분석 서빙에 강하다. 조인 많은 BI 질의를 여기로 옮겨 p95를 개선했다.
- 경계 원칙: **"실시간 append-only 시계열은 Druid, 조인/업서트/집계가 필요한 분석 서빙은 StarRocks."** 자세한 근거와 마이그레이션 이야기는 `druid-to-starrocks.md`.
