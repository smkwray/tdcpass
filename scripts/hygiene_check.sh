#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

mode="repo"
if [[ "${1:-}" == "--staged" ]]; then
  mode="staged"
elif [[ -n "${1:-}" ]]; then
  echo "hygiene_check: unknown argument: $1" >&2
  exit 2
fi

files=()
if [[ "$mode" == "staged" ]]; then
  while IFS= read -r path; do
    [[ -n "$path" && -f "$path" ]] && files+=("$path")
  done < <(git diff --cached --name-only --diff-filter=ACMR)
else
  while IFS= read -r path; do
    case "$path" in
      do/*|data/*|output/*|dist/*|*.egg-info/*) continue ;;
    esac
    [[ -f "$path" ]] && files+=("$path")
  done < <(git ls-files)
fi

violations=0
for path in "${files[@]}"; do
  [[ "$path" == "scripts/hygiene_check.sh" ]] && continue
  case "$path" in
    *.py|*.md|*.yml|*.yaml|*.toml|*.sh|*.js|*.html|*.json|*.css)
      if grep -nE '(/Users/[A-Za-z0-9._-]+|/home/[A-Za-z0-9._-]+|[A-Za-z]:[\\/]Users)' "$path" >/dev/null 2>&1; then
        echo "hygiene_check: absolute system path in $path" >&2
        violations=$((violations + 1))
      fi
      ;;
  esac
done

if [[ "$mode" == "staged" ]]; then
  git diff --cached --check || violations=$((violations + 1))
else
  git diff --check || violations=$((violations + 1))
fi

if [[ "$violations" -gt 0 ]]; then
  echo "hygiene_check: FAIL ($violations violation(s))" >&2
  exit 1
fi

echo "hygiene_check: pass"
