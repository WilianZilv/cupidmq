#!/usr/bin/env bash
# Verify git tag vX.Y.Z matches master/Cargo.toml and python-client/pyproject.toml.
set -euo pipefail

TAG="${1:-${GITHUB_REF_NAME:-}}"
if [[ -z "$TAG" ]]; then
  echo "usage: check-release-version.sh v0.1.0" >&2
  exit 1
fi

if [[ ! "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "error: tag must look like v0.1.0 (got '$TAG')" >&2
  exit 1
fi

VERSION="${TAG#v}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cargo_ver="$(grep -E '^version\s*=' "$ROOT/master/Cargo.toml" | head -1 | sed -E 's/.*"([^"]+)".*/\1/')"
py_ver="$(grep -E '^version\s*=' "$ROOT/python-client/pyproject.toml" | head -1 | sed -E 's/.*"([^"]+)".*/\1/')"

fail=0
if [[ "$cargo_ver" != "$VERSION" ]]; then
  echo "error: master/Cargo.toml version=$cargo_ver, expected $VERSION (tag $TAG)" >&2
  fail=1
fi
if [[ "$py_ver" != "$VERSION" ]]; then
  echo "error: python-client/pyproject.toml version=$py_ver, expected $VERSION (tag $TAG)" >&2
  fail=1
fi

if [[ "$fail" -ne 0 ]]; then
  exit 1
fi

echo "ok: tag $TAG matches crate=$cargo_ver python=$py_ver"
