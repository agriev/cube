#!/usr/bin/env bash
# pin-base-images.sh
#
# Resolves current upstream digests for the Cube Store Dockerfile's base
# images and rewrites the FROM lines in-place. Use this when intentionally
# rolling forward (e.g. picking up a debian:bookworm-slim CVE patch).
#
# Refuses to run if the working tree is dirty under rust/cubestore/Dockerfile.
# Prints a diff and exits 0 even if there is nothing to update.
#
# Requires: bash, git, docker (with `docker buildx imagetools`).

set -euo pipefail

cd "$(dirname "$0")/.."   # rust/cubestore

DOCKERFILE="Dockerfile"

if ! command -v docker >/dev/null; then
  echo "error: docker not found in PATH" >&2
  exit 1
fi

if ! git diff --quiet -- "$DOCKERFILE"; then
  echo "error: $DOCKERFILE has uncommitted changes; commit or stash first" >&2
  exit 1
fi

resolve_digest() {
  local image="$1"
  docker buildx imagetools inspect "$image" 2>/dev/null \
    | awk '/^Digest: / {print $2; exit}'
}

# Pin pairs: <image-without-digest>::<sed-anchor>
pairs=(
  "cubejs/rust-builder:bookworm-llvm-18::FROM cubejs/rust-builder:bookworm-llvm-18"
  "debian:bookworm-slim::FROM debian:bookworm-slim"
)

for entry in "${pairs[@]}"; do
  image="${entry%%::*}"
  anchor="${entry##*::}"
  digest=$(resolve_digest "$image")
  if [ -z "$digest" ]; then
    echo "error: could not resolve digest for $image" >&2
    exit 1
  fi
  printf 'pinning %s -> %s\n' "$image" "$digest"
  # Replace the entire @sha256:... suffix (or absent suffix) with the new
  # digest. Single-line, no extended regex needed.
  sed -i.bak -E \
    "s|(${anchor//\//\\/})(@sha256:[a-f0-9]+)?|\1@${digest}|" \
    "$DOCKERFILE"
done

rm -f "${DOCKERFILE}.bak"
git diff -- "$DOCKERFILE" || true
echo
echo "Done. Review the diff, then commit:"
echo "  git add $DOCKERFILE && git commit -m 'chore(docker): refresh base image digests'"
