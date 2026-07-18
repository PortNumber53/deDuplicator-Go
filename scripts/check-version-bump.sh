#!/usr/bin/env bash
set -euo pipefail

base_ref="${VERSION_BASE_REF:-${GIT_PREVIOUS_SUCCESSFUL_COMMIT:-${GIT_PREVIOUS_COMMIT:-HEAD~1}}}"

extract_version() {
  sed -nE 's/^const VERSION = "([^"]+)"/\1/p' "$1"
}

extract_version_from_ref() {
  git show "${1}:main.go" | sed -nE 's/^const VERSION = "([^"]+)"/\1/p'
}

version_gt() {
  local current="$1"
  local previous="$2"
  local current_major current_minor current_patch
  local previous_major previous_minor previous_patch

  IFS=. read -r current_major current_minor current_patch <<< "${current}"
  IFS=. read -r previous_major previous_minor previous_patch <<< "${previous}"

  if (( current_major != previous_major )); then
    (( current_major > previous_major ))
    return
  fi
  if (( current_minor != previous_minor )); then
    (( current_minor > previous_minor ))
    return
  fi
  (( current_patch > previous_patch ))
}

semver_re='^[0-9]+[.][0-9]+[.][0-9]+$'
current_version="$(extract_version main.go)"

if [[ ! "${current_version}" =~ ${semver_re} ]]; then
  echo "VERSION must use MAJOR.MINOR.PATCH, got: ${current_version}" >&2
  exit 1
fi

if ! git rev-parse --verify "${base_ref}^{commit}" >/dev/null 2>&1; then
  echo "Skipping version bump check because base ref is unavailable: ${base_ref}"
  exit 0
fi

previous_version="$(extract_version_from_ref "${base_ref}")"
if [[ ! "${previous_version}" =~ ${semver_re} ]]; then
  echo "Skipping version bump check because ${base_ref}:main.go has no semantic VERSION"
  exit 0
fi

if ! version_gt "${current_version}" "${previous_version}"; then
  echo "VERSION must increase before push/deploy: current ${current_version}, previous ${previous_version} (${base_ref})" >&2
  exit 1
fi

echo "VERSION increased: ${previous_version} -> ${current_version}"
