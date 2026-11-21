#!/usr/bin/env bash
# Runs ON THE SERVER, sent over SSH by the deploy job in release.yml:
#   ssh <user>@<host> bash -s -- <deploy-dir> <app sha256:digest> <orchestration sha256:digest> \
#     < deploy/remote.sh
# Pins the released app and orchestration image digests in <deploy-dir>/release.env, pulls the
# images, starts the stack (waiting for health checks and one-shots) and runs `ibergrid doctor`.
# It never writes <deploy-dir>/.env, which holds the secrets. Re-running it with the previous
# digests is the rollback.
set -euo pipefail

usage="usage: remote.sh <deploy-dir> <app sha256:digest> <orchestration sha256:digest>"
if [[ $# -ne 3 ]]; then
  echo "$usage" >&2
  exit 2
fi
dir="$1"
app_digest="$2"
orchestration_digest="$3"
for digest in "$app_digest" "$orchestration_digest"; do
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
printf 'IBERGRID_APP_DIGEST=%s\nIBERGRID_ORCHESTRATION_DIGEST=%s\n' "$app_digest" "$orchestration_digest" > "$tmp"
mv "$tmp" release.env

compose=(docker compose --env-file .env --env-file release.env -f compose.yaml -f compose.prod.yaml)
"${compose[@]}" pull --quiet
"${compose[@]}" up -d --wait --remove-orphans
"${compose[@]}" run --rm --no-deps doctor
