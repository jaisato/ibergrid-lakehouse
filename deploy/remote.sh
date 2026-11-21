#!/usr/bin/env bash
# Runs ON THE SERVER, sent over SSH by the deploy job in release.yml:
#   ssh <user>@<host> bash -s -- <deploy-dir> <sha256:digest> < deploy/remote.sh
# Pins the released app image digest in <deploy-dir>/release.env, pulls the images, starts the
# stack (waiting for health checks and one-shots) and runs `ibergrid doctor`. It never writes
# <deploy-dir>/.env, which holds the secrets. Re-running it with the previous digest is the rollback.
set -euo pipefail

usage="usage: remote.sh <deploy-dir> <sha256:digest>"
if [[ $# -ne 2 ]]; then
  echo "$usage" >&2
  exit 2
fi
dir="$1"
digest="$2"
if [[ ! "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "not an image digest: '$digest'" >&2
  exit 2
fi

cd "$dir"
if [[ ! -f .env ]]; then
  echo "$dir/.env is missing: create it from .env.example with the production values" >&2
  exit 1
fi
if [[ ! -r .env ]]; then
  echo "$dir/.env is not readable by $(id -un)" >&2
  exit 1
fi

# Write the new pin next to the old one, then swap it in atomically.
tmp="$(mktemp release.env.XXXXXX)"
trap 'rm -f "$tmp"' EXIT
chmod 600 "$tmp"
printf 'IBERGRID_APP_DIGEST=%s\n' "$digest" > "$tmp"
mv "$tmp" release.env

compose=(docker compose --env-file .env --env-file release.env -f compose.yaml -f compose.prod.yaml)
"${compose[@]}" pull --quiet
"${compose[@]}" up -d --wait --remove-orphans
"${compose[@]}" run --rm --no-deps doctor
