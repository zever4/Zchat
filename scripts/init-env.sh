#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ -e .env || -L .env ]]; then
  echo 'Existing .env preserved.'
  exit 0
fi

command -v openssl >/dev/null || { echo 'OpenSSL is required to generate secrets.' >&2; exit 1; }
umask 077
temporary=$(mktemp .env.init.XXXXXX)
trap 'rm -f "$temporary"' EXIT
db_secret=$(openssl rand -hex 32)
jwt_secret=$(openssl rand -hex 32)
db_found=false
jwt_found=false

while IFS= read -r line || [[ -n "$line" ]]; do
  case "$line" in
    DB_PASS=*) printf 'DB_PASS=%s\n' "$db_secret"; db_found=true ;;
    JWT_SECRET=*) printf 'JWT_SECRET=%s\n' "$jwt_secret"; jwt_found=true ;;
    *) printf '%s\n' "$line" ;;
  esac
done < .env.example > "$temporary"

if [[ "$db_found" != true || "$jwt_found" != true ]]; then
  echo '.env.example must contain DB_PASS and JWT_SECRET fields.' >&2
  exit 1
fi
# Atomically create the destination without replacing any file or symlink.
if ! ln "$temporary" .env 2>/dev/null; then
  if [[ -e .env || -L .env ]]; then
    echo 'Another invocation created .env; existing file preserved.'
    exit 0
  fi
  echo 'Could not create .env.' >&2
  exit 1
fi
echo 'Created .env with generated DB_PASS and JWT_SECRET.'
