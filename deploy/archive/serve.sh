#!/usr/bin/env bash
# Serve an archive fetch.sh made: the live dashboard over the whole window,
# from the archived metrics, at http://localhost:3300 (ADR 36).
#
#   deploy/archive/serve.sh DIR          start it
#   deploy/archive/serve.sh DIR --stop   stop it
#
# DIR is the directory fetch.sh printed. The dashboard opens on the window
# from its recorded start (state.tar) to the last decision Prometheus holds;
# Prometheus also kept the dry runs before it, which stay reachable by zooming.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
repo="$here/../.."
archive="$(cd "${1:?usage: deploy/archive/serve.sh DIR [--stop]}" && pwd)"
export ARCHIVE="$archive"
compose=(docker compose -f "$here/compose.yml")

if [ "${2:-}" = "--stop" ]; then
  "${compose[@]}" down
  exit 0
fi
[ -d "$archive/data/prometheus" ] || { echo "no data/prometheus under $archive" >&2; exit 1; }

python="$repo/.venv/Scripts/python.exe"
[ -x "$python" ] || python="$repo/.venv/bin/python"
"${compose[@]}" up -d prometheus
url="http://localhost:9090"
last=""
for _ in $(seq 1 60); do
  last=$("${compose[@]}" exec -T prometheus wget -qO- \
    "$url/api/v1/query?query=max_over_time(timestamp(verdict_last_decision_timestamp_seconds)%5B100d:1h%5D)" \
    2>/dev/null | "$python" -c \
    'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "")' \
    2>/dev/null || true)
  [ -n "$last" ] && break
  sleep 2
done
[ -n "$last" ] || { echo "Prometheus holds no decisions" >&2; exit 1; }
iso() { "$python" -c 'import datetime,sys; print(datetime.datetime.fromtimestamp(float(sys.argv[1]), datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))' "$1"; }
first=$(tar -xOf "$archive/state.tar" window-start | tr -d '[:space:]')
"$python" -m verdict.cli observe grafana --out "$archive/grafana" \
  --window-start "$first" --window-end "$(iso "$last")" >/dev/null
"${compose[@]}" up -d grafana
echo "the window $first to $(iso "$last") at http://localhost:${ARCHIVE_PORT:-3300}"
