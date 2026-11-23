#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
image=${1:?Usage: bash scripts/smoke-compose.sh IMAGE}
docker image inspect "$image" >/dev/null

# Every invocation owns a fresh project, random credentials and disposable data.
# Never read the developer's .env or reuse their Compose project/volumes.
project="zchat-check-$(openssl rand -hex 8)"
work=$(mktemp -d)
created=false
cat > "$work/image.yaml" <<'YAML'
services:
  server:
    image: ${ZCHAT_IMAGE:?Set the already built test image}
YAML
compose=(docker compose --env-file /dev/null -p "$project"
  -f "$root/compose.yaml" -f "$work/image.yaml")
cleanup() {
  result=$?
  trap - EXIT
  if [[ "$created" == true ]]; then
    docker rm -f "$project-client" >/dev/null 2>&1 || true
    if ! "${compose[@]}" --profile redis down --volumes --remove-orphans --timeout 15 >/dev/null; then
      echo "Could not remove test project $project" >&2
      result=1
    fi
  fi
  rm -rf "$work"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if [[ -n $(docker ps -aq --filter "label=com.docker.compose.project=$project") ||
      -n $(docker volume ls -q --filter "label=com.docker.compose.project=$project") ||
      -n $(docker network ls -q --filter "label=com.docker.compose.project=$project") ]]; then
  echo 'Refusing to reuse an existing test project' >&2
  exit 1
fi
export ZCHAT_IMAGE="$image" DB_USER=zchat_smoke DB_NAME=zchat_smoke
DB_PASS=$(openssl rand -hex 32)
JWT_SECRET=$(openssl rand -hex 32)
export DB_PASS JWT_SECRET
export NGINX_PORT=0 SERVER_HTTP_PORT=0 SERVER_GRPC_PORT=0 DB_HOST_PORT=0 REDIS_HOST_PORT=0
export LIVEKIT_API_KEY= LIVEKIT_API_SECRET= LIVEKIT_URL= ENABLE_EXPERIMENTAL_E2EE=false
export E2EE_REDIS_ADDR=
export ENABLE_EXPERIMENTAL_E2EE_BLOBS=false E2EE_BLOB_ROOT=/var/lib/zchat/e2ee-blobs
unset E2EE_BLOB_TOTAL_GB E2EE_BLOB_ACCOUNT_GB E2EE_BLOB_TOTAL_CLAIMS E2EE_BLOB_ACCOUNT_CLAIMS
unset E2EE_BLOB_TOTAL_ATTEMPTS E2EE_BLOB_ACCOUNT_ATTEMPTS E2EE_BLOB_MAX_ARTIFACTS
unset E2EE_BLOB_STARTUP_VERIFY_GB E2EE_BLOB_STARTUP_SECONDS SERVER_HEALTH_START_PERIOD
case ${SMOKE_EXPERIMENTAL_E2EE:-false} in
  true) export ENABLE_EXPERIMENTAL_E2EE=true E2EE_REDIS_ADDR=redis:6379 ;;
  false) ;;
  *) echo 'SMOKE_EXPERIMENTAL_E2EE must be true or false' >&2; exit 2 ;;
esac

docker run --rm --entrypoint sh "$image" -ec '
  test "$(id -u)" -eq 10001
  test -x /bin/server
  test -w /web/uploads
  test "$(stat -c %u:%a /var/lib/zchat/e2ee-blobs)" = 10001:700
  test ! -e /var/lib/zchat/e2ee-blobs/.store-id
  test -f /migrations/016_history_cursor_index.sql
  test -f /migrations/017_message_receipts.sql
  test -f /web/chat.js && test -f /web/login.js && test -f /web/login.css
  test -f /web/vendor/manifest.json
  test ! -e /.env && test ! -e /docs && test ! -e /src
'
"${compose[@]}" config --quiet
created=true
echo "Starting disposable test project $project"
"${compose[@]}" up -d --no-build --wait --wait-timeout 120
node_image=$(sed -n 's/^ARG NODE_IMAGE=//p' Dockerfile)
test -n "$node_image"
# Use the pinned supported Node even when the developer's host has Node18.
smoke() {
  docker run --rm --name "$project-client" --network "${project}_default" \
    --mount "type=bind,source=$root/scripts/smoke-http.mjs,target=/checks/smoke-http.mjs,readonly" \
    -e SMOKE_BASE_URL=http://nginx "$node_image" node /checks/smoke-http.mjs
}
test -z "$("${compose[@]}" --profile redis ps -aq redis)"
smoke
echo 'PASS: packaged runtime without Redis'
test "$("${compose[@]}" exec -T server sh -ec 'test ! -e /var/lib/zchat/e2ee-blobs/.store-id && echo off')" = off
test "$("${compose[@]}" exec -T db sh -ec 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atqc "SELECT (SELECT count(*) FROM e2ee_blob_global_quota)::text || chr(58) || (SELECT count(*) FROM e2ee_blob_storage)::text"')" = 0:0
echo 'PASS: default-off private storage has no namespace or quota initialization'
admission_metrics=$("${compose[@]}" exec -T server wget -q -O - http://127.0.0.1:8080/metrics)
test "$admission_metrics" != "${admission_metrics/zchat_admission_operations_active/}"
test "$admission_metrics" != "${admission_metrics/zchat_admission_requests_total\{class=\"data\",result=\"accepted\"\}/}"
test "$admission_metrics" != "${admission_metrics/zchat_admission_requests_total\{class=\"login\",result=\"accepted\"\}/}"
echo 'PASS: production HTTP/WS and login share the configured admission metrics'
server_id=$("${compose[@]}" ps -q server)
server_started=$(docker inspect --format '{{.State.StartedAt}}' "$server_id")
# Cleanup must remain active even with the experimental API disabled. Both
# successful lane counters appear only after actual repository passes, not init.
cleanup_ready=false
for attempt in {1..20}; do
  cleanup_metrics=$("${compose[@]}" exec -T server wget -q -O - http://127.0.0.1:8080/metrics)
  if [[ "$cleanup_metrics" == *'e2ee_cleanup_passes_total{lane="expired",result="ok"}'* &&
        "$cleanup_metrics" == *'e2ee_cleanup_passes_total{lane="replaced",result="ok"}'* ]]; then
    cleanup_ready=true
    break
  fi
  sleep 1
done
test "$cleanup_ready" = true
echo 'PASS: bounded background cleanup runs in both lanes independently of API flag'
"${compose[@]}" --profile redis up -d --no-build --wait --wait-timeout 60 redis
smoke
echo 'PASS: packaged runtime with optional Redis running'
"${compose[@]}" --profile redis stop redis
smoke
test "$("${compose[@]}" ps -q server)" = "$server_id"
test "$(docker inspect --format '{{.State.StartedAt}}' "$server_id")" = "$server_started"
test "$(docker inspect --format '{{.State.Health.Status}}' "$server_id")" = healthy
echo 'PASS: Redis outage did not restart or block server; migrations, sessions, HTTP/WS and cleanup'

# Both explicit opt-ins admit the owned file transport; UI capabilities stay off.
export ENABLE_EXPERIMENTAL_E2EE=true ENABLE_EXPERIMENTAL_E2EE_BLOBS=true E2EE_REDIS_ADDR=
"${compose[@]}" up -d --no-build --wait --wait-timeout 120 server
test "$("${compose[@]}" exec -T server stat -c %u:%a /var/lib/zchat/e2ee-blobs)" = 10001:700
marker_before=$("${compose[@]}" exec -T server sha256sum /var/lib/zchat/e2ee-blobs/.store-id)
test "$("${compose[@]}" exec -T db sh -ec 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atqc "SELECT reserved_bytes FROM e2ee_blob_global_quota"')" = 0
docker run --rm --name "$project-client" --network "${project}_default" "$node_image" \
  node --input-type=module -e '
    const r = await fetch("http://nginx/api/e2ee/blobs/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", {
      method: "PUT", headers: {"content-type": "application/octet-stream"}, body: new Uint8Array()
    });
    const b = await r.json();
    if (r.status !== 400 || b.error !== "invalid_metadata") throw new Error("raw upload route was not mounted");
  '
blob_cleanup_ready=false
for attempt in {1..20}; do
  blob_metrics=$("${compose[@]}" exec -T server wget -q -O - http://127.0.0.1:8080/metrics)
  if [[ "$blob_metrics" == *'e2ee_blob_cleanup_passes_total{lane="unfinished",result="ok"}'* &&
        "$blob_metrics" == *'e2ee_blob_cleanup_passes_total{lane="expired",result="ok"}'* ]]; then
    blob_cleanup_ready=true
    break
  fi
  sleep 1
done
test "$blob_cleanup_ready" = true
"${compose[@]}" restart --no-deps --timeout 15 server
"${compose[@]}" up -d --no-build --wait --wait-timeout 120 server
test "$marker_before" = "$("${compose[@]}" exec -T server sha256sum /var/lib/zchat/e2ee-blobs/.store-id)"
smoke
echo 'PASS: opt-in private volume ownership, route, collector metrics, restart identity and ordinary HTTP/WS'

# Turning off the API must preserve storage accounting and maintenance.
export ENABLE_EXPERIMENTAL_E2EE=false
"${compose[@]}" up -d --no-build --wait --wait-timeout 120 server
test "$marker_before" = "$("${compose[@]}" exec -T server sha256sum /var/lib/zchat/e2ee-blobs/.store-id)"
docker run --rm --name "$project-client" --network "${project}_default" "$node_image" \
  node --input-type=module -e '
    const r = await fetch("http://nginx/api/e2ee/blobs/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", {
      method: "PUT", headers: {"content-type": "application/json"}, body: "{}"
    });
    if (r.status !== 404) throw new Error("disabled blob API remains mounted");
  '
smoke
echo 'PASS: private namespace survives API disable and ordinary HTTP/WS still work'
