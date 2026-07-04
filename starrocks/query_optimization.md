# StarRocks query optimization

> 대시보드 질의의 p95를 좌우하는 네 가지 knob: 파티셔닝, 버킷팅, colocation, materialized view. 각각이 "스캔 행 수"와 "데이터 이동량"을 어떻게 줄이는지에 초점을 둔다.

## 1. Partitioning — 시간 pruning

- `PARTITION BY date_trunc('day', event_time)`로 하루 단위 파티션을 만든다.
- 시간 범위 필터(`event_time >= ... AND < ...`)가 걸린 질의는 **관련 파티션만** 읽는다. 90일 중 하루를 보는 질의는 89일치를 건드리지 않는다.
- 부수 효과: 백필/삭제가 파티션 단위로 싸고 안전하다 (`TRUNCATE ... PARTITION`).

## 2. DISTRIBUTED BY bucketing — 병렬성 + 조인 준비

- `DISTRIBUTED BY HASH(device_id) BUCKETS 16`은 각 파티션을 16개 tablet으로 쪼갠다. tablet이 병렬 스캔 단위다.
- 버킷 키를 **조인 키(device_id)** 로 잡은 것이 핵심이다. 이래야 팩트와 디멘전이 같은 방식으로 쪼개져 colocation이 가능해진다.
- 버킷 수는 tablet 크기(대략 수백 MB~1GB 목표)와 클러스터 코어 수를 보고 정한다. 너무 많으면 메타/스케줄 오버헤드, 너무 적으면 병렬성 부족.

## 3. Colocation — 조인 shuffle 제거

- `raw_sensor_readings`와 `dim_device`를 **같은 키·같은 버킷 수·같은 replication**으로 만들고 `colocate_with = "cg_sensor_device"`로 묶었다.
- 같은 `device_id`의 팩트 tablet과 디멘전 tablet이 같은 노드에 놓이므로, 팩트↔디멘전 조인이 **네트워크 shuffle 없이 노드 로컬**로 끝난다.
- 조인이 무거운 질의의 p95(꼬리)는 대개 shuffle/broadcast에서 온다. 그걸 제거하는 것이 2.2x 개선의 큰 축이다.

## 4. Materialized View — 스캔 행 수 축소 + 투명 rewrite

- 반복되는 rollup/topN을 async MV로 사전 계산한다. 예: 시간별 집계, 사이트별 상위 N 기기.
- 옵티마이저가 원 질의를 **자동으로 MV로 rewrite**한다. 사용자는 raw 테이블을 질의해도 실제로는 MV를 읽는다 — 질의 문을 바꿀 필요가 없다.
- 스캔 행 수가 수 자릿수 줄면 꼬리 질의 비용이 급감한다.

```sql
-- 예시: 시간별 집계 MV. CBO가 동등한 GROUP BY 질의를 이 MV로 rewrite한다.
CREATE MATERIALIZED VIEW mv_sensor_hourly
DISTRIBUTED BY HASH(device_id) BUCKETS 16
REFRESH ASYNC EVERY (INTERVAL 10 MINUTE)
AS
SELECT date_trunc('hour', event_time) AS event_hour,
       device_id, metric,
       count(*) AS reading_cnt,
       sum(value) AS value_sum,
       max(value) AS value_max
FROM   example_db.raw_sensor_readings
GROUP  BY 1, 2, 3;
```

## 5. Sort key / prefix index + vectorized execution

- DUPLICATE/AGGREGATE 키 순서(`event_time, device_id, metric`)가 정렬 순서이자 prefix index다. **prefix에 걸린 필터**는 tablet 내부에서 대부분의 데이터 블록을 건너뛴다.
- 벡터화 실행 엔진이 컬럼 스캔·필터를 배치 단위로 처리해 질의당 CPU 비용을 낮춘다. 고동시성에서 이 절약이 꼬리를 압축한다.

## p95가 좋아지는 이유 요약

| 메커니즘 | 줄이는 것 | 꼬리에 미치는 효과 |
| --- | --- | --- |
| Partition pruning | 스캔 파티션 | 시간 필터 질의가 무관 파티션을 건너뜀 |
| Bucket + prefix index | 스캔 tablet/블록 | 키 필터가 tablet·블록을 잘라냄 |
| Colocation join | 네트워크 shuffle | 조인이 노드 로컬 → broadcast 폭발 제거 |
| Materialized view rewrite | 스캔 행 수 | 사전 집계로 수 자릿수 감소 |
| Vectorized execution | 질의당 CPU | 고동시성 구간의 꼬리 압축 |

> 핵심: p95는 "스캔 폭발 + 데이터 이동 + fan-out/merge"에서 터진다. 위 knob들은 각각 그 원인 하나씩을 제거한다. "그냥 빠른 엔진"이 아니라 **원인별 제거의 합**이다.
