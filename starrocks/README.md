# starrocks

> OLAP 서빙 계층: 목적별 세 가지 테이블 모델, 두 가지 로더, 그리고 p95를 결정하는 최적화 노트.

## 설계 의도

- **한 엔진 안에서 워크로드별로 테이블 모델을 나눈다.** 원천 보존, 사전 집계, 업서트는 요구가 다르므로 StarRocks의 세 모델을 목적에 맞게 쓴다.
- **로딩은 push(Stream Load)와 pull(Routine Load) 두 경로를 둔다.** 배치는 Airflow가 Stream Load로 밀고, 스트림은 StarRocks가 Kafka에서 당긴다.
- **성능은 스키마 설계에서 결정된다.** 파티션·버킷·colocation·MV를 DDL 단계에서 심어 질의 시점 비용을 낮춘다.

## 테이블 모델 (`ddl/`)

| 파일 | 모델 | 용도 | 핵심 이유 |
| --- | --- | --- | --- |
| `01_raw_duplicate_key.sql` | Duplicate Key | append-only 원천 | 모든 행 보존, 키는 정렬(=pruning)용 |
| `02_rollup_aggregate_key.sql` | Aggregate Key | 시간별 사전 집계 | 로드 시 집계 → 질의 시 스캔 행 축소 |
| `03_dim_primary_key.sql` | Primary Key | 디멘전 업서트 | CDC 갱신을 재색인 없이 반영 |

## 로더 (`load/`)

| 파일 | 방식 | 트리거 |
| --- | --- | --- |
| `stream_load.sh` | HTTP push, 동기·트랜잭셔널 | Airflow가 배치를 밀 때 |
| `routine_load.sql` | Kafka pull, 서버 관리 | 상시 스트림 소비 |

## 최적화

- `query_optimization.md` — 파티셔닝, 버킷팅, colocation, MV, 정렬 키가 각각 p95를 어떻게 낮추는지.

## 방언·버전 가정

- **StarRocks 3.1+** 기준. 표현식 파티셔닝(`PARTITION BY date_trunc('day', ...)`)과 일 파티션 자동 명명(`pYYYYMMDD`)을 전제한다. 구버전은 명시적 `RANGE(event_time)` + dynamic partition으로 대체.
- SQL 방언은 StarRocks(MySQL 호환 프로토콜, 자체 DDL 확장). 표준 ANSI SQL이 아니다.
- 적용 순서: `01` → `03`(colocation group `cg_sensor_device`를 공유하므로 둘 다 필요) → `02` → 로더.
