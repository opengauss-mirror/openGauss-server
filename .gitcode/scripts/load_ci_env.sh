#!/usr/bin/env bash
# Source this file. It exports the checkout result without executing the file as shell.

if [[ ! -f .gitcode_ci_env ]]; then
  echo "Missing .gitcode_ci_env from checkout." >&2
  exit 1
fi

seen_iid=0
seen_target=0
seen_sha=0
seen_merge=0
while IFS= read -r line || [[ -n "${line}" ]]; do
  key="${line%%=*}"
  val="${line#*=}"
  case "${key}" in
    GITCODE_PR_IID)
      case "${val}" in
        *[!0-9]*) echo "Invalid PR iid" >&2; exit 1 ;;
      esac
      seen_iid=1
      ;;
    GITCODE_TARGET_BRANCH)
      case "${val}" in
        ''|*[!A-Za-z0-9._/-]*) echo "Invalid target branch" >&2; exit 1 ;;
      esac
      seen_target=1
      ;;
    GITCODE_AFTER_COMMIT_SHA)
      case "${val}" in
        *[!0-9a-fA-F]*) echo "Invalid commit sha" >&2; exit 1 ;;
      esac
      seen_sha=1
      ;;
    GITCODE_MERGE_REF)
      case "${val}" in
        '') ;;
        refs/merge-requests/*|refs/pull/*)
          case "${val}" in
            refs/merge-requests/*) num="${val#refs/merge-requests/}" ;;
            refs/pull/*) num="${val#refs/pull/}" ;;
          esac
          case "${num}" in
            */merge) num="${num%/merge}" ;;
            */head) num="${num%/head}" ;;
            *) echo "Invalid merge ref" >&2; exit 1 ;;
          esac
          case "${val}" in
            "refs/merge-requests/${num}/merge"|"refs/pull/${num}/merge"|"refs/merge-requests/${num}/head"|"refs/pull/${num}/head")
              case "${num}" in
                ''|*[!0-9]*) echo "Invalid merge ref" >&2; exit 1 ;;
              esac
              ;;
            *) echo "Invalid merge ref" >&2; exit 1 ;;
          esac
          ;;
        *) echo "Invalid merge ref" >&2; exit 1 ;;
      esac
      seen_merge=1
      ;;
    *)
      echo "Unexpected checkout env: ${key}" >&2
      exit 1
      ;;
  esac
  export "${key}=${val}"
done < .gitcode_ci_env

if [[ "${seen_iid}" -ne 1 || "${seen_target}" -ne 1 || "${seen_sha}" -ne 1 || "${seen_merge}" -ne 1 ]]; then
  echo "Incomplete checkout env" >&2
  exit 1
fi
