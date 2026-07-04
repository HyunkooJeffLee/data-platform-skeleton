#!/usr/bin/env bash
# Stream Load: push a local NDJSON/JSON batch into StarRocks over HTTP.
# Synchronous and transactional — the whole batch commits or none of it does.
#
# Usage: SR_USER=user SR_PASSWORD=password ./stream_load.sh ./batch.json
set -euo pipefail

FE_HOST="${SR_FE_HOST:-localhost}"
FE_HTTP_PORT="${SR_FE_HTTP_PORT:-8030}"   # FE HTTP port (NOT the 9030 MySQL port)
DB="${SR_DB:-example_db}"
TABLE="${SR_TABLE:-raw_sensor_readings}"
SR_USER="${SR_USER:-user}"                # placeholder — inject real creds via env
SR_PASSWORD="${SR_PASSWORD:-password}"
DATA_FILE="${1:-./batch.json}"

# Label = idempotency token. Re-sending the same label is rejected by StarRocks,
# so a retried load cannot double-insert. Derive it from content in real pipelines.
LABEL="raw_sensor_$(date +%Y%m%d%H%M%S)"

# --location-trusted is REQUIRED: the FE returns a 307 to a BE and we must
# re-send credentials across that redirect (the classic Stream Load gotcha).
curl --location-trusted -u "${SR_USER}:${SR_PASSWORD}" \
  -H "label:${LABEL}" \
  -H "format:json" \
  -H "strip_outer_array:true" \
  -H "jsonpaths:[\"\$.event_time\",\"\$.device_id\",\"\$.metric\",\"\$.value\"]" \
  -H "columns:event_time,device_id,metric,value" \
  -H "Expect:100-continue" \
  -T "${DATA_FILE}" \
  -XPUT "http://${FE_HOST}:${FE_HTTP_PORT}/api/${DB}/${TABLE}/_stream_load"

# Response is JSON: check "Status":"Success". "Label Already Exists" == a prior
# attempt already committed this batch and is safe to treat as success.
