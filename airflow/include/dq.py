"""Data-quality assertions.

Design choice: a failed gate RAISES, it does not skip. Raising fails the Airflow
task, which (with the default all_success trigger rule) blocks every downstream
task and surfaces on the alerting path. A silent short-circuit would hide bad data.

The assertions are decoupled from Airflow: each takes a `scalar` callable
(sql -> single value) so they are unit-testable without a running scheduler.
The DAG wires `scalar` to a StarRocks connection (MySQL wire protocol).
"""
from __future__ import annotations

from typing import Any, Callable

# SQL scalar result is dynamically typed (int/float/None depending on the query);
# Any keeps callers honest at runtime while satisfying the type checker on int()/float().
Scalar = Callable[[str], Any]


class DataQualityError(AssertionError):
    """Raised when a data-quality gate fails; propagating it fails the task."""


def assert_row_count_at_least(scalar: Scalar, table: str, min_rows: int, where: str | None = None) -> int:
    """Guard against empty/under-loaded partitions."""
    sql = f"SELECT count(*) FROM {table}" + (f" WHERE {where}" if where else "")
    n = int(scalar(sql) or 0)
    if n < min_rows:
        raise DataQualityError(f"{table}: row_count={n} < min_rows={min_rows} (where={where})")
    return n


def assert_null_ratio_below(scalar: Scalar, table: str, column: str, max_ratio: float, where: str | None = None) -> float:
    """Guard against a column silently degrading to mostly-NULL."""
    base = f" WHERE {where}" if where else ""
    total = int(scalar(f"SELECT count(*) FROM {table}{base}") or 0)
    if total == 0:
        raise DataQualityError(f"{table}: empty relation, cannot evaluate null ratio for {column}")
    null_clause = f"{' AND' if where else ' WHERE'} {column} IS NULL"
    nulls = int(scalar(f"SELECT count(*) FROM {table}{base}{null_clause}") or 0)
    ratio = nulls / total
    if ratio > max_ratio:
        raise DataQualityError(f"{table}.{column}: null_ratio={ratio:.4f} > max={max_ratio}")
    return ratio


def assert_fresh(scalar: Scalar, table: str, ts_column: str, max_lag_minutes: int) -> int:
    """Guard against a stalled pipeline: newest row must be recent enough."""
    # timestampdiff(MINUTE, older, newer) and now() are valid StarRocks functions.
    lag = scalar(f"SELECT timestampdiff(MINUTE, max({ts_column}), now()) FROM {table}")
    if lag is None:
        raise DataQualityError(f"{table}: no rows, freshness undefined")
    lag = int(lag)
    if lag > max_lag_minutes:
        raise DataQualityError(f"{table}: freshness lag={lag}min > max={max_lag_minutes}min")
    return lag
