# Druid → StarRocks migration

> 조인 중심 BI 서빙을 Druid에서 StarRocks로 옮기며 대시보드 질의 p95를 약 2.2x 개선했다. 개선이 어디서 왔는지를 메커니즘 단위로 정리한다.

## 먼저: Druid가 잘하는 것

- **실시간 append-only 스트림 인제스트.** Kafka supervisor로 exactly-once에 가깝게 스트림을 받고, ingest-time rollup으로 신선도를 초 단위로 유지한다.
- **시간축 슬라이스.** timeseries / topN / groupBy(저차원) 질의를 시간 파티션 세그먼트 위에서 낮은 지연으로 처리한다.
- **고동시성 시계열 대시보드.** 미리 정의된 슬라이스-앤-다이스에 대해 broker fan-out으로 빠르게 응답한다.

이 강점은 마이그레이션 후에도 유효하다. **실시간 시계열 경로는 Druid에 그대로 뒀다.** 옮긴 것은 조인·업서트·ad-hoc 집계가 얽힌 배치 BI 서빙뿐이다.

## Druid가 우리 BI 워크로드에서 불편했던 지점

- **조인.** 팩트↔디멘전 조인이 broker broadcast 또는 인제스트 시 denormalization에 의존한다. 디멘전이 커지거나 여러 개가 되면 tail latency가 튄다.
- **디멘전 업데이트.** 기기 메타가 CDC로 갱신되는데, Druid는 세그먼트 재색인(reindex) 없이는 업서트가 어색하다.
- **고차원 GROUP BY / 유연한 SQL.** 다중 디멘전 조인과 임의 조합 질의는 Druid의 강점이 아니다.
- **운영 복잡도.** coordinator/overlord/broker/historical/middlemanager/router 다프로세스 구조는 튜닝 지점이 많다.

## StarRocks를 택한 이유 (워크로드 관점)

- **진짜 MPP 조인 + CBO.** shuffle join과 colocation join을 옵티마이저가 비용 기반으로 선택한다. 스타 스키마를 denormalize하지 않고도 팩트↔디멘전 조인이 가능하다.
- **Primary Key 모델.** CDC로 갱신되는 디멘전을 업서트로 유지한다 (`dim_device`).
- **Materialized View.** 반복되는 rollup/topN을 async MV로 미리 계산하고, 옵티마이저가 원 질의를 MV로 **투명하게 rewrite**한다.
- **벡터화 실행 엔진 + 정렬 키/prefix index.** 컬럼 스캔이 CPU 효율적이고, 정렬 키 prefix 필터로 파티션·버킷·tablet을 잘라낸다.

## p95 개선의 출처 (메커니즘 귀속)

p50이 아니라 **p95(꼬리)** 가 좋아진 것이 핵심이다. 꼬리는 보통 "조인이 shuffle/broadcast로 터지거나, 스캔 행 수가 폭발하거나, fan-out/merge가 많아질 때" 생긴다. 개선은 아래 메커니즘의 합이다.

1. **Colocation join으로 조인 shuffle 제거.** 팩트(`raw_sensor_readings`)와 디멘전(`dim_device`)을 같은 조인 키(`device_id`)로 동일하게 버킷팅하고 colocation group에 묶었다. 조인이 노드 로컬에서 끝나 broadcast/shuffle이 사라졌고, 조인이 무거운 질의의 꼬리가 눌렸다.
2. **Materialized view rewrite로 스캔 행 수 축소.** 시간별/일별 집계와 사이트별 topN을 async MV로 사전 계산했다. CBO가 raw 질의를 MV로 rewrite해 꼬리 질의가 스캔하는 행 수를 수 자릿수 줄였다.
3. **정렬 키 + 파티션/버킷 pruning.** 정렬 키 prefix(`event_time, device_id`)로 필터가 걸리는 질의는 파티션·버킷·prefix index 단계에서 대부분의 tablet을 건드리지 않는다. 스캔이 줄면 꼬리가 짧아진다.
4. **벡터화 실행 + late materialization.** 컬럼 스캔·필터가 벡터 단위로 처리되어 질의당 CPU 비용이 낮아지고, 이는 고동시성 구간에서 꼬리를 압축한다.
5. **세그먼트 레이아웃 차이.** Druid의 다수 소형 세그먼트 대비, StarRocks의 정렬된 tablet은 고동시성에서 fan-out/merge 오버헤드를 줄인다. 이 오버헤드가 사라지는 곳이 정확히 p95다.

## 트레이드오프와 한계

- **이 2.2x는 워크로드 한정이다.** 조인이 많고 MV로 rewrite 가능하며 정렬 키 필터가 걸리는 대시보드 질의에서의 수치다. 단일 시계열 스캔이나 순수 실시간 인제스트에는 해당하지 않는다.
- **실시간 순수 스트리밍 인제스트는 Druid가 여전히 유리한 지점이 있다.** StarRocks의 Routine/Stream Load도 견고하지만, 초고속 append-only 스트림 + ingest-time rollup 성숙도는 Druid supervisor 모델의 강점이었다. 그래서 실시간 경로는 옮기지 않았다.
- **MV·colocation은 공짜가 아니다.** MV는 저장·리프레시 비용을 지고, colocation은 버킷 수/키를 맞춰야 하는 설계 제약을 준다. 이득이 확인된 질의 패턴에 한해 적용했다.
- **측정 전제.** 위 귀속은 동일 하드웨어·동일 질의 집합·동일 동시성에서 비교했을 때다. 벤치마크 조건이 달라지면 숫자는 달라진다.
