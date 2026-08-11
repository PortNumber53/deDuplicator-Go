#!/usr/bin/env bash
set -euo pipefail

suite="${1:-}"
case "${suite}" in
  unit|bdd)
    ;;
  *)
    echo "usage: $0 unit|bdd" >&2
    exit 2
    ;;
esac

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

test_files=()
package_dirs=()

while IFS= read -r -d '' file; do
  case "${suite}:${file}" in
    unit:*_scenarios_test.go)
      continue
      ;;
    bdd:*_scenarios_test.go)
      ;;
    bdd:*)
      continue
      ;;
  esac

  test_files+=("${file}")
  directory="$(dirname "${file}")"
  found=false
  for existing in "${package_dirs[@]:-}"; do
    if [[ "${existing}" == "${directory}" ]]; then
      found=true
      break
    fi
  done
  if [[ "${found}" == false ]]; then
    package_dirs+=("${directory}")
  fi
done < <(find . -type f -name '*_test.go' -not -path './.git/*' -print0)

if [[ ${#test_files[@]} -eq 0 ]]; then
  echo "No ${suite} test files found."
  exit 0
fi

for directory in "${package_dirs[@]}"; do
  package_files=()
  for file in "${test_files[@]}"; do
    if [[ "$(dirname "${file}")" == "${directory}" ]]; then
      package_files+=("${file}")
    fi
  done

  test_names=()
  while IFS= read -r name; do
    if [[ -n "${name}" ]]; then
      test_names+=("${name}")
    fi
  done < <(sed -nE 's/^func (Test[A-Za-z0-9_]+)\(.*$/\1/p' "${package_files[@]}")

  if [[ ${#test_names[@]} -eq 0 ]]; then
    continue
  fi

  test_pattern="$(IFS='|'; echo "${test_names[*]}")"
  package="${directory}"
  if [[ "${directory}" != "." ]]; then
    package="./${directory#./}"
  fi

  echo "Running ${suite} tests in ${package}"
  go test -count=1 "${package}" -run "^(${test_pattern})$"
done
