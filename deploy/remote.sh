#!/usr/bin/env bash
# Runs ON THE SERVER, sent over SSH by the deploy job in release.yml:
#   ssh <user>@<host> bash -s -- <deploy-dir> <sha256:digest> < deploy/remote.sh
# Pins the released app image digest in <deploy-dir>/.env, pulls the images, starts the stack
# (waiting for health checks and one-shots) and runs `ibergrid doctor`. Re-running it with the
# previous digest is the rollback.
set -euo pipefail

usage="usage: remote.sh <deploy-dir> <sha256:digest>"
dir="${1:?$usage}"
digest="${2:?$usage}"

if [[ ! "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "not an image digest: $digest" >&2
  exit 2
fi

cd "$dir"
if [[ ! -f .env ]]; then
  echo "$dir/.env is missing: create it from .env.example with the production values" >&2
  exit 1
fi

# Replace (or add) the digest line without touching the rest of .env; mv keeps the swap atomic.
tmp="$(mktemp .env.XXXXXX)"
grep -v '^IBERGRID_APP_DIGEST=' .env > "$tmp" || true
printf 'IBERGRID_APP_DIGEST=%s\n' "$digest" >> "$tmp"
chmod 600 "$tmp"
mv "$tmp" .env

compose=(docker compose -f compose.yaml -f compose.prod.yaml)
"${compose[@]}" pull --quiet
"${compose[@]}" up -d --wait --remove-orphans
"${compose[@]}" run --rm --no-deps doctor
