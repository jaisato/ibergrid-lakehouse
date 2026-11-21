#!/usr/bin/env bash
# Runs ON THE SERVER, sent over SSH by the deploy job in release.yml:
#   ssh <user>@<host> bash -s -- <deploy-dir> <app sha256:digest> <orchestration sha256:digest> \
#     <api sha256:digest> < deploy/remote.sh
# Pins the released app, orchestration and api image digests in <deploy-dir>/release.env, pulls the
# images, creates the lake's tables (idempotent), starts the stack (waiting for health checks and
# one-shots) and runs `ibergrid doctor`.
# It never writes <deploy-dir>/.env, which holds the secrets. Re-running it with the previous
# digests is the rollback.
set -euo pipefail

usage="usage: remote.sh <deploy-dir> <app sha256:digest> <orchestration sha256:digest> <api sha256:digest>"
if [[ $# -ne 4 ]]; then
  echo "$usage" >&2
  exit 2
fi
dir="$1"
app_digest="$2"
orchestration_digest="$3"
api_digest="$4"
for digest in "$app_digest" "$orchestration_digest" "$api_digest"; do
  if [[ ! "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "not an image digest: '$digest'" >&2
    exit 2
  fi
done

cd "$dir"
if [[ ! -f .env ]]; then
  echo "$dir/.env is missing: create it from .env.example with the production values" >&2
  exit 1
fi
if [[ ! -r .env ]]; then
  echo "$dir/.env is not readable by $(id -un)" >&2
  exit 1
fi

# Write the new pins next to the old ones, then swap them in atomically.
tmp="$(mktemp release.env.XXXXXX)"
trap 'rm -f "$tmp"' EXIT
chmod 600 "$tmp"
printf 'IBERGRID_APP_DIGEST=%s\nIBERGRID_ORCHESTRATION_DIGEST=%s\nIBERGRID_API_DIGEST=%s\n' \
  "$app_digest" "$orchestration_digest" "$api_digest" > "$tmp"
mv "$tmp" release.env

compose=(docker compose --env-file .env --env-file release.env -f compose.yaml -f compose.prod.yaml)
"${compose[@]}" pull --quiet
# Every `run` reads stdin, which is this script when sent as `bash -s < remote.sh`: without
# </dev/null it would swallow the lines after it, and the deploy would stop there with exit 0.
# The tables exist before Dagster's schedules start; `run` first starts the catalog and catalog-init.
"${compose[@]}" run --rm doctor lake init </dev/null
"${compose[@]}" up -d --wait --remove-orphans
"${compose[@]}" run --rm --no-deps doctor </dev/null
