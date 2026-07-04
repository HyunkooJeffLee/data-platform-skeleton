# airflow

> Orchestration layer: three DAGs on Airflow 2.10 (TaskFlow API) that ingest, backfill, and gate telemetry into StarRocks.

## 설계 의도

- **Airflow는 "언제·무엇을·어떤 순서로"만 책임진다.** 적재/집계/업서트 모델링은 StarRocks가 맡는다. DAG은 얇게 유지한다.
- **멱등성이 기본값이다.** 증분은 watermark + Stream Load label, 백필은 파티션 단위 truncate+reload. 어떤 태스크든 재시도해도 결과가 같다.
- **DQ 게이트는 실패시킨다.** skip이 아니라 fail이라 다운스트림을 막고 알림이 뜬다.

## DAGs

| DAG | 목적 | 핵심 |
| --- | --- | --- |
| `ingest_incremental` | 15분 주기 증분 적재 | high-watermark 경계, content-scoped label로 재시도 멱등 |
| `backfill_partitioned` | 과거 구간 재적재 | `catchup=True`, 파티션 단위 truncate+reload, no full-refresh |
| `data_quality_checks` | 적재 후 품질 게이트 | row-count / null-ratio / freshness, 위반 시 fail |

## 버전 선택 — Airflow 2.10 (TaskFlow)

- 2.10 라인을 택했다. TaskFlow(`from airflow.decorators import dag, task`)가 널리 배포돼 있고 `schedule=`/`catchup=` 스케줄링이 안정적이다.
- **3.x 호환 메모:** `@dag`/`@task`, `schedule=`, `catchup=`은 3.x에서도 동일하다. 바뀐 것은 `airflow.datasets.Dataset` → `airflow.sdk.Asset`(이 스켈레톤은 Dataset을 쓰지 않는다)과 실행 아키텍처(Task SDK/Executor API)다. DAG 저작 API는 그대로 옮겨간다.

## 레이아웃 규약 (`include/` import)

- DAG은 `from include.watermark import ...`처럼 `include` 패키지를 import한다. 이는 Astronomer식 레이아웃 규약으로, 프로젝트 루트가 `PYTHONPATH`에 있어야 한다.
- 로컬에서: `export PYTHONPATH="$PWD/airflow:$PYTHONPATH"` 후 `airflow dags list`.
- `py_compile`은 import 해석 없이 문법만 보므로 경로 설정 없이도 통과한다.

## 필요한 Airflow Connections (값은 배포 시 주입, 코드에 시크릿 없음)

- `postgres_source` — 운영 PostgreSQL (팩트/디멘전 소스)
- `postgres_meta` — watermark 메타 테이블용 (Postgres 백엔드를 쓸 때만)
- `starrocks_fe` — StarRocks FE HTTP (host, port=8030, login, password) — Stream Load용
- `starrocks_mysql` — StarRocks FE MySQL 프로토콜 (host, port=9030) — DDL/DML/DQ 질의용

## 검증

```bash
python -m py_compile dags/*.py include/*.py   # 문법 검증 (Airflow 미설치도 통과)
# Airflow 설치 시:
export PYTHONPATH="$PWD:$PYTHONPATH"
airflow dags list                             # import 성공 여부까지 확인
```
