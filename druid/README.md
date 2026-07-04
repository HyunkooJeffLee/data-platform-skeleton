# druid

> 실시간 시계열 경로: Kafka 텔레메트리 스트림을 Druid supervisor로 인제스트하고 ingest-time rollup으로 초 단위 신선도의 대시보드를 서빙한다.

## 설계 의도

- **Druid는 강점 워크로드에만 남긴다.** 실시간 append-only 시계열과 시간축 슬라이스가 그것이다. 조인·업서트·집계 중심 BI 서빙은 StarRocks로 옮겼다 (`../docs/druid-to-starrocks.md`).
- **Ingest-time rollup으로 저장·질의 비용을 낮춘다.** 원 이벤트를 (분 단위 × 디멘전) 격자로 사전 집계해 시계열 대시보드를 싸게 만든다.

## `ingestion_spec.json` — Kafka supervisor spec

- `type: "kafka"` — Kafka indexing service supervisor. StarRocks Routine Load와 대칭되는 스트림 인제스트 경로다.
- `dataSchema`
  - `timestampSpec` — `event_time`을 ISO로 파싱.
  - `dimensionsSpec` — `device_id`, `metric`, `site_code`를 디멘전으로.
  - `metricsSpec` — `count` + `value`의 sum/max/min을 사전 집계.
  - `granularitySpec` — `segmentGranularity: HOUR`(세그먼트 파일 단위), `queryGranularity: MINUTE`(rollup 격자), `rollup: true`.
- `ioConfig` — 토픽 `sensor_readings`, JSON 입력, `bootstrap.servers`, `taskDuration PT1H`, 최초 실행은 earliest offset부터.
- `tuningConfig` — 메모리/세그먼트 행 상한.

## 제출

```bash
curl -XPOST -H 'Content-Type: application/json' \
  -d @ingestion_spec.json \
  http://localhost:8081/druid/indexer/v1/supervisor
```

- `8081` = Druid Overlord/Coordinator(라우터 구성에 따라 다름) 포트. supervisor는 등록 후 Druid가 오프셋을 추적하며 계속 소비한다.
- 상태 확인: `GET /druid/indexer/v1/supervisor/sensor_readings/status`.
