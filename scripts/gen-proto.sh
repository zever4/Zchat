#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

mode=${1:-generate}
if [[ "$mode" != generate && "$mode" != --check ]]; then
  echo "Usage: bash scripts/gen-proto.sh [--check]" >&2
  exit 2
fi
# Match the checked-in protocol outputs; update tools and outputs together.
[[ $(protoc --version) == 'libprotoc 3.21.12' ]] || { echo 'Requires protoc 3.21.12' >&2; exit 1; }
[[ $(protoc-gen-go --version) == 'protoc-gen-go v1.36.11' ]] || { echo 'Requires protoc-gen-go 1.36.11' >&2; exit 1; }
[[ $(protoc-gen-go-grpc --version) == 'protoc-gen-go-grpc 1.6.1' ]] || { echo 'Requires protoc-gen-go-grpc 1.6.1' >&2; exit 1; }
[[ $(protoc-gen-grpc-gateway --version) == 'Version v2.29.0,'* ]] || { echo 'Requires protoc-gen-grpc-gateway 2.29.0' >&2; exit 1; }

output=$(mktemp -d)
trap 'rm -rf "$output"' EXIT
for pair in chat:chat friend:friends user:user messenger:messenger e2ee:e2ee e2ee_epoch:e2eev2; do
  proto=${pair%:*}
  package=${pair#*:}
  mkdir -p "$output/$package"
  options=(-I proto "--go_out=$output/$package" --go_opt=paths=source_relative
    "--go-grpc_out=$output/$package" --go-grpc_opt=paths=source_relative)
  if [[ "$proto" != messenger && "$proto" != e2ee && "$proto" != e2ee_epoch ]]; then
    options+=("--grpc-gateway_out=$output/$package" --grpc-gateway_opt=paths=source_relative)
  fi
  protoc "${options[@]}" "proto/$proto.proto"
done
if [[ "$mode" == --check ]]; then
  diff -ru gen "$output"
  echo 'PASS: generated Go protocols match the committed sources'
else
  mkdir -p gen/{chat,friends,user,messenger,e2ee,e2eev2}
  for file in "$output"/*/*.go; do
    relative=${file#"$output/"}
    cp "$file" "gen/$relative"
  done
  echo 'Generated Go protocols; review and commit gen changes with proto changes'
fi
