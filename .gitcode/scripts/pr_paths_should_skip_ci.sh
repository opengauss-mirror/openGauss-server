#!/usr/bin/env bash
# Exit 0: every changed file is under .gitcode/doc/docs/docs-lite; skip LLT and
#         let prepare write ci-pipeline-success (PR trigger must still start).
# Exit 1: at least one file is outside those paths; CI tests should run.
# Exit 2: cannot decide (missing iid/token/API); caller should run CI (fail open).
set -euo pipefail

repo="${GITCODE_REPO:-openGauss/openGauss-server}"
pr_iid="${GITCODE_PR_IID:-}"
token="${PR_LABEL_TOKEN:-}"

# Same prefixes as the docs-only skip in openGauss_server_PR.yml prepare.
path_is_ignored() {
  local path="$1"
  path="${path#./}"
  case "${path}" in
    .gitcode|.gitcode/*|doc|doc/*|docs|docs/*|docs-lite|docs-lite/*) return 0 ;;
  esac
  return 1
}

# Comment events leave the workflow iid empty; resolve from the event payload.
if [[ -z "${pr_iid}" && -n "${RESOLVE_SCRIPT:-}" && -f "${RESOLVE_SCRIPT}" ]]; then
  resolved="$(mktemp)"
  if bash -c 'source "$1" >&2; printf "GITCODE_PR_IID=%q\n" "${GITCODE_PR_IID:-}"' _ "${RESOLVE_SCRIPT}" > "${resolved}"; then
    count="$(grep -c '^GITCODE_PR_IID=' "${resolved}" || true)"
    if [[ "${count}" -eq 1 ]]; then
      eval "$(cat "${resolved}")"
      pr_iid="${GITCODE_PR_IID:-}"
    fi
  fi
  rm -f "${resolved}"
fi

if [[ -z "${pr_iid}" || ! "${pr_iid}" =~ ^[0-9]+$ ]]; then
  echo "No pull request iid; cannot apply docs-only path skip." >&2
  exit 2
fi

if [[ ! "${repo}" =~ ^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$ ]]; then
  echo "Invalid repository: ${repo}" >&2
  exit 2
fi

if [[ -z "${token}" ]]; then
  echo "PR_LABEL_TOKEN is empty; cannot list pull request files." >&2
  exit 2
fi

api_root="https://api.gitcode.com/api/v5"
if [[ -n "${ATOMGIT_API_URL:-}" ]]; then
  api_root="${ATOMGIT_API_URL%/}"
  case "${api_root}" in
    */api/v5) ;;
    *) api_root="${api_root}/api/v5" ;;
  esac
elif [[ -n "${ATOMGIT_SERVER_URL:-${GITCODE_SERVER_URL:-}}" ]]; then
  api_host="${ATOMGIT_SERVER_URL:-${GITCODE_SERVER_URL}}"
  api_host="${api_host#*://}"
  api_host="${api_host%%/*}"
  api_host="${api_host%%:*}"
  case "${api_host}" in
    api.*) ;;
    '') api_host="api.gitcode.com" ;;
    *) api_host="api.${api_host}" ;;
  esac
  api_root="https://${api_host}/api/v5"
fi

urlencode() {
  local LC_ALL=C
  local s="$1"
  local i c
  for ((i = 0; i < ${#s}; i++)); do
    c="${s:i:1}"
    case "${c}" in
      [A-Za-z0-9._~-]) printf '%s' "${c}" ;;
      *) printf '%%%02X' "'${c}" ;;
    esac
  done
}

encoded="$(urlencode "${token}")"
body="$(mktemp)"
trap 'rm -f "${body}"' EXIT
code="$(curl -sS --max-time 60 -o "${body}" -w '%{http_code}' \
  -H "Authorization: Bearer ${token}" \
  -H "Accept: application/json" \
  "${api_root}/repos/${repo}/pulls/${pr_iid}/files?access_token=${encoded}&per_page=300" || true)"

if [[ "${code}" != "200" ]]; then
  echo "Cannot list pull request files: HTTP ${code}" >&2
  exit 2
fi

# Prefer python for JSON; fall back to grep on filename fields.
paths=""
if command -v python3 >/dev/null 2>&1 || command -v python >/dev/null 2>&1; then
  py=python3
  command -v python3 >/dev/null 2>&1 || py=python
  paths="$("${py}" - "${body}" << 'PY' || true
import json, sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
if not isinstance(data, list):
    sys.exit(2)
for item in data:
    if not isinstance(item, dict):
        continue
    name = item.get("filename") or item.get("new_path") or item.get("path") or ""
    if name:
        print(name)
PY
)" || paths=""
fi

if [[ -z "${paths}" ]]; then
  paths="$(grep -oE '"filename"[[:space:]]*:[[:space:]]*"[^"]+"' "${body}" | sed 's/.*"filename"[[:space:]]*:[[:space:]]*"//;s/"$//' || true)"
fi

if [[ -z "${paths}" ]]; then
  echo "Pull request file list is empty or unreadable; run CI." >&2
  exit 2
fi

outside=0
total=0
while IFS= read -r path || [[ -n "${path}" ]]; do
  [[ -n "${path}" ]] || continue
  total=$((total + 1))
  if ! path_is_ignored "${path}"; then
    echo "Non-ignored change: ${path}" >&2
    outside=1
  else
    echo "Ignored change: ${path}" >&2
  fi
done << EOF
${paths}
EOF

if [[ "${total}" -eq 0 ]]; then
  echo "No paths parsed; run CI." >&2
  exit 2
fi

if [[ "${outside}" -eq 0 ]]; then
  echo "All ${total} changed path(s) are under .gitcode/doc/docs/docs-lite; skip CI tests." >&2
  exit 0
fi

echo "Found changes outside ignored paths; run CI tests." >&2
exit 1
