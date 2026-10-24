#!/usr/bin/env bash
set -euo pipefail

# Print PR iid, target branch, and head sha from a GitCode/AtomGit event file.
# Only fields that belong to the pull request are used. A comment id or a
# head branch ref is ignored. Output is four labeled lines.
parse_event_fields() {
  awk -v event_name="${GITCODE_EVENT_NAME:-${ATOMGIT_EVENT_NAME:-}}" '
    function skip_ws(s, i,    n, c) {
      n = length(s)
      while (i <= n) {
        c = substr(s, i, 1)
        if (c != " " && c != "\t" && c != "\n" && c != "\r") {
          return i
        }
        i++
      }
      return i
    }

    function key_at(s, i, key,    token) {
      token = "\"" key "\""
      if (substr(s, i, length(token)) != token) {
        return 0
      }
      return skip_ws(s, i + length(token))
    }

    function value_pos(s, start, key,    i, n, c, depth, colon) {
      n = length(s)
      if (substr(s, start, 1) != "{") {
        return 0
      }
      i = start + 1
      depth = 1
      while (i <= n && depth > 0) {
        c = substr(s, i, 1)
        if (c == "\"") {
          if (depth == 1) {
            colon = key_at(s, i, key)
            if (colon > 0 && substr(s, colon, 1) == ":") {
              return skip_ws(s, colon + 1)
            }
          }
          i++
          while (i <= n) {
            c = substr(s, i, 1)
            if (c == "\\") {
              i += 2
              continue
            }
            if (c == "\"") {
              i++
              break
            }
            i++
          }
          continue
        }
        if (c == "{") {
          depth++
        } else if (c == "}") {
          depth--
        }
        i++
      }
      return 0
    }

    function read_string(s, i,    n, c, out) {
      n = length(s)
      i++
      out = ""
      while (i <= n) {
        c = substr(s, i, 1)
        if (c == "\\") {
          out = out substr(s, i + 1, 1)
          i += 2
          continue
        }
        if (c == "\"") {
          return out
        }
        out = out c
        i++
      }
      return ""
    }

    function read_number(s, i,    n, c, out) {
      n = length(s)
      out = ""
      while (i <= n) {
        c = substr(s, i, 1)
        if (c !~ /[0-9]/) {
          break
        }
        out = out c
        i++
      }
      return out
    }

    function object_pos(s, start, key,    pos) {
      pos = value_pos(s, start, key)
      if (pos == 0 || substr(s, pos, 1) != "{") {
        return 0
      }
      return pos
    }

    function field_number(s, start, key,    pos) {
      pos = value_pos(s, start, key)
      if (pos == 0 || substr(s, pos, 1) !~ /[0-9]/) {
        return ""
      }
      return read_number(s, pos)
    }

    function field_string(s, start, key,    pos) {
      pos = value_pos(s, start, key)
      if (pos == 0 || substr(s, pos, 1) != "\"") {
        return ""
      }
      return read_string(s, pos)
    }

    # issue.number is not the pull request. Some comment payloads put only a
    # URL inside issue.pull_request; the number is the last path segment.
    function number_in_pr_url(url,    rest, n) {
      rest = url
      sub(/\?.*/, "", rest)
      sub(/\/$/, "", rest)
      if (match(rest, /\/(pulls|pull|merge_requests|merge-requests)\/[0-9]+$/)) {
        n = substr(rest, RSTART, RLENGTH)
        sub(/.*\//, "", n)
        return n
      }
      return ""
    }

    {
      gsub(/\r/, "")
      json = json $0 "\n"
    }

    END {
      start = index(json, "{")
      if (start == 0) {
        exit 0
      }

      pr = object_pos(json, start, "pull_request")
      mr = object_pos(json, start, "merge_request")
      attrs = object_pos(json, start, "object_attributes")
      issue = object_pos(json, start, "issue")
      iid = ""
      target = ""
      sha = ""
      context = 0

      if (pr > 0) {
        context = 1
        iid = field_number(json, pr, "number")
        if (iid == "") {
          iid = field_number(json, pr, "iid")
        }
        base = object_pos(json, pr, "base")
        if (base > 0) {
          target = field_string(json, base, "ref")
        }
        if (target == "") {
          target = field_string(json, pr, "target_branch")
        }
        head = object_pos(json, pr, "head")
        if (head > 0) {
          sha = field_string(json, head, "sha")
        }
      }
      if (mr > 0) {
        context = 1
        if (iid == "") {
          iid = field_number(json, mr, "iid")
          if (iid == "") {
            iid = field_number(json, mr, "number")
          }
        }
        if (target == "") {
          target = field_string(json, mr, "target_branch")
          if (target == "") {
            base = object_pos(json, mr, "base")
            if (base > 0) {
              target = field_string(json, base, "ref")
            }
          }
        }
        if (sha == "") {
          head = object_pos(json, mr, "head")
          if (head > 0) {
            sha = field_string(json, head, "sha")
          }
        }
        if (sha == "") {
          last_commit = object_pos(json, mr, "last_commit")
          if (last_commit > 0) {
            sha = field_string(json, last_commit, "id")
          }
        }
      }
      if (attrs > 0) {
        attr_iid = field_number(json, attrs, "iid")
        attr_target = field_string(json, attrs, "target_branch")
        # A note number is the comment id. Only iid is the pull request.
        if (attr_iid != "" || attr_target != "") {
          context = 1
          if (iid == "" && attr_iid != "") {
            iid = attr_iid
          }
          if (target == "") {
            target = attr_target
          }
          if (sha == "") {
            last_commit = object_pos(json, attrs, "last_commit")
            if (last_commit > 0) {
              sha = field_string(json, last_commit, "id")
            }
          }
        }
      }
      if (issue > 0) {
        issue_pr = object_pos(json, issue, "pull_request")
        if (issue_pr > 0) {
          context = 1
          if (iid == "") {
            # issue.number can be the comment id. The pull request number is
            # inside pull_request, or the last segment of its URL.
            iid = field_number(json, issue_pr, "number")
            if (iid == "") {
              iid = field_number(json, issue_pr, "iid")
            }
            if (iid == "") {
              iid = number_in_pr_url(field_string(json, issue_pr, "url"))
            }
            if (iid == "") {
              iid = number_in_pr_url(field_string(json, issue_pr, "html_url"))
            }
          }
          if (target == "") {
            base = object_pos(json, issue_pr, "base")
            if (base > 0) {
              target = field_string(json, base, "ref")
            }
            if (target == "") {
              target = field_string(json, issue_pr, "target_branch")
            }
          }
          if (sha == "") {
            head = object_pos(json, issue_pr, "head")
            if (head > 0) {
              sha = field_string(json, head, "sha")
            }
          }
        }
      }
      # A push payload can carry an unrelated top-level number. refs/heads/*
      # identifies the branch, and that number must not become the PR iid.
      push_ref = 0
      if (context == 0) {
        ref = field_string(json, start, "ref")
        if (ref ~ /^refs\/heads\//) {
          push_ref = 1
          iid = ""
          if (target == "") {
            target = substr(ref, 12)
          }
          if (sha == "") {
            sha = field_string(json, start, "after")
          }
        }
      }
      # No pull request object was found. On a comment, the root number is the
      # comment id, so it must not be used as the pull request number.
      # Root iid is still the pull request. A non-comment flat payload may
      # carry the pull request in number.
      if (push_ref == 0 && context == 0) {
        comment_event = 0
        if (tolower(event_name) ~ /comment/) {
          comment_event = 1
        }
        if (object_pos(json, start, "comment") > 0 || object_pos(json, start, "note") > 0) {
          comment_event = 1
        }
        if (iid == "") {
          iid = field_number(json, start, "iid")
          if (iid == "" && comment_event == 0) {
            iid = field_number(json, start, "number")
          }
        }
        if (target == "") {
          target = field_string(json, start, "target_branch")
        }
      }

      # One labeled line per field. A tab-separated record drops empty fields
      # because tab is IFS whitespace.
      printf "iid=%s\ntarget=%s\ncontext=%s\nsha=%s\n", iid, target, context, sha
    }
  ' "$1"
}

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

# Quoted pieces are literal, so a token containing * or & is not treated as a glob.
replace_all() {
  local s="$1"
  local old="$2"
  local new="$3"
  local out=""
  if [[ -z "${old}" ]]; then
    printf '%s' "${s}"
    return 0
  fi
  while [[ "${s}" == *"${old}"* ]]; do
    out="${out}${s%%"${old}"*}${new}"
    s="${s#*"${old}"}"
  done
  printf '%s' "${out}${s}"
}

# Same host rule as pr-label-action: api.<server host>/api/v5, otherwise api.gitcode.com.
gitcode_api_root() {
  local api server host
  api="${ATOMGIT_API_URL:-}"
  if [[ -n "${api}" ]]; then
    api="${api%/}"
    case "${api}" in
      */api/v5) printf '%s\n' "${api}" ;;
      *) printf '%s\n' "${api}/api/v5" ;;
    esac
    return 0
  fi
  server="${ATOMGIT_SERVER_URL:-${GITCODE_SERVER_URL:-}}"
  if [[ -n "${server}" ]]; then
    host="${server#*://}"
    host="${host%%/*}"
    host="${host%%:*}"
    case "${host}" in
      api.*) ;;
      '') host="api.gitcode.com" ;;
      *) host="api.${host}" ;;
    esac
    printf '%s\n' "https://${host}/api/v5"
    return 0
  fi
  printf '%s\n' "https://api.gitcode.com/api/v5"
}

# Comment payloads often have a PR number and no base ref. Read the target
# branch from the pull request itself. The response head sha is ignored.
lookup_pr_target() {
  local iid="$1"
  local repo_name url body_file wrapped parsed target line token api_root code err
  # The pull request number belongs to this repository, not the fork.
  repo_name="${GITCODE_REPO:-openGauss/openGauss-server}"
  if [[ ! "${repo_name}" =~ ^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$ ]]; then
    echo "Invalid repository: ${repo_name}" >&2
    return 1
  fi
  if [[ ! "${iid}" =~ ^[0-9]+$ ]]; then
    echo "Invalid PR iid: ${iid}" >&2
    return 1
  fi
  if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required to read the pull request target branch." >&2
    return 1
  fi
  api_root="$(gitcode_api_root)"
  url="${api_root}/repos/${repo_name}/pulls/${iid}"
  body_file="$(mktemp)"
  wrapped="$(mktemp)"
  token=""
  if [[ -n "${GITCODE_TOKEN_FILE:-}" && -f "${GITCODE_TOKEN_FILE}" ]]; then
    IFS= read -r token < "${GITCODE_TOKEN_FILE}" || true
  fi
  if [[ -z "${token}" ]]; then
    token="${PR_LABEL_TOKEN:-}"
  fi
  err="$(mktemp)"
  # Do not use curl -f. Its error text repeats the URL, including access_token.
  if [[ -n "${token}" ]]; then
    code="$(curl -sS --max-time 30 -G -o "${body_file}" -w '%{http_code}' \
      --data-urlencode "access_token=${token}" \
      -H "Authorization: Bearer ${token}" \
      "${url}" 2>"${err}")" || code="000"
  else
    code="$(curl -sS --max-time 30 -o "${body_file}" -w '%{http_code}' \
      "${url}" 2>"${err}")" || code="000"
  fi
  rm -f "${err}"
  case "${code}" in
    2*) ;;
    *)
      echo "Cannot read pull request ${iid} from GitCode: HTTP ${code}" >&2
      if [[ -s "${body_file}" ]]; then
        local encoded encoded_lower
        encoded=""
        encoded_lower=""
        if [[ -n "${token}" ]]; then
          encoded="$(urlencode "${token}")"
          encoded_lower="$(printf '%s' "${encoded}" | tr 'A-F' 'a-f')"
        fi
        while IFS= read -r line || [[ -n "${line}" ]]; do
          if [[ -n "${token}" ]]; then
            line="$(replace_all "${line}" "${token}" "[redacted]")"
            if [[ -n "${encoded}" && "${encoded}" != "${token}" ]]; then
              line="$(replace_all "${line}" "${encoded}" "[redacted]")"
            fi
            if [[ "${encoded_lower}" != "${encoded}" ]]; then
              line="$(replace_all "${line}" "${encoded_lower}" "[redacted]")"
            fi
          fi
          printf '%s\n' "${line}" >&2
        done < "${body_file}"
      fi
      rm -f "${body_file}" "${wrapped}"
      return 1
      ;;
  esac
  {
    printf '%s' '{"pull_request":'
    cat "${body_file}"
    printf '%s\n' '}'
  } > "${wrapped}"
  parsed="$(parse_event_fields "${wrapped}")"
  rm -f "${body_file}" "${wrapped}"
  target=""
  while IFS= read -r line || [[ -n "${line}" ]]; do
    case "${line}" in
      target=*) target="${line#target=}" ;;
    esac
  done <<< "${parsed}"
  if [[ -z "${target}" ]]; then
    echo "Pull request ${iid} response has no target branch." >&2
    return 1
  fi
  printf '%s\n' "${target}"
}

event_file="${ATOMGIT_EVENT_PATH:-${GITCODE_EVENT_PATH:-}}"
# Workflow env wins. CamelCase platform variables are not read: they can carry
# a comment id, the PR source branch, or the workflow commit instead of the PR head.
pr_iid="${GITCODE_PR_IID:-}"
target_branch="${GITCODE_TARGET_BRANCH:-}"
after_sha="${GITCODE_AFTER_COMMIT_SHA:-}"
merge_ref="${GITCODE_MERGE_REF:-}"
parsed_context="0"

if [[ -z "${pr_iid}" || -z "${target_branch}" || -z "${after_sha}" ]]; then
  if [[ -n "${event_file}" && -f "${event_file}" ]]; then
    parsed="$(parse_event_fields "${event_file}")"
    parsed_iid=""
    parsed_target=""
    parsed_sha=""
    if [[ -n "${parsed}" ]]; then
      while IFS= read -r line || [[ -n "${line}" ]]; do
        case "${line}" in
          iid=*) parsed_iid="${line#iid=}" ;;
          target=*) parsed_target="${line#target=}" ;;
          context=*) parsed_context="${line#context=}" ;;
          sha=*) parsed_sha="${line#sha=}" ;;
        esac
      done <<< "${parsed}"
    fi
    if [[ -z "${pr_iid}" ]]; then
      pr_iid="${parsed_iid}"
    fi
    if [[ -z "${target_branch}" ]]; then
      target_branch="${parsed_target}"
    fi
    if [[ -z "${after_sha}" ]]; then
      after_sha="${parsed_sha}"
    fi
  fi
fi

# On pull_request_target, ATOMGIT_SHA is the workflow revision, not the PR head.
if [[ -z "${after_sha}" && -z "${pr_iid}" ]]; then
  after_sha="${GITCODE_SHA:-${ATOMGIT_SHA:-}}"
fi

if [[ -n "${pr_iid}" && -z "${target_branch}" ]]; then
  echo "Pull request ${pr_iid} event has no target branch; querying the pull request." >&2
  target_branch="$(lookup_pr_target "${pr_iid}")" || exit 1
fi

event_name="${GITCODE_EVENT_NAME:-${ATOMGIT_EVENT_NAME:-}}"
case "${event_name}" in
  *comment*|*pull_request*)
    if [[ -z "${pr_iid}" || -z "${target_branch}" ]]; then
      echo "Event ${event_name} has no pull request number or target branch; refuse to test master." >&2
      exit 1
    fi
    ;;
esac

if [[ "${parsed_context}" == "1" && -z "${target_branch}" ]]; then
  echo "Pull request event has no target branch; refuse to default to master." >&2
  exit 1
fi

if [[ -n "${pr_iid}" && -z "${target_branch}" ]]; then
  echo "Pull request ${pr_iid} has no target branch; refuse to default to master." >&2
  exit 1
fi

target_branch="${target_branch:-master}"

if [[ -z "${merge_ref}" && -n "${pr_iid}" ]]; then
  merge_ref="refs/merge-requests/${pr_iid}/merge"
fi

# head.sha is the PR head, not the merge with the target branch. Clear it here.
# manual_checkout fills the merge commit when that ref can be fetched.
if [[ -n "${pr_iid}" && -n "${merge_ref}" ]]; then
  if [[ -n "${after_sha}" ]]; then
    echo "Ignoring pull request head ${after_sha}; it is not the merge result." >&2
  fi
  after_sha=""
fi

if [[ -n "${pr_iid}" && ! "${pr_iid}" =~ ^[0-9]+$ ]]; then
  echo "Invalid PR iid: ${pr_iid}" >&2
  exit 1
fi
case "${target_branch}" in
  ''|*[!A-Za-z0-9._/-]*)
    echo "Invalid target branch: ${target_branch}" >&2
    exit 1
    ;;
esac
if [[ -n "${after_sha}" && ! "${after_sha}" =~ ^[0-9a-fA-F]+$ ]]; then
  echo "Invalid commit sha: ${after_sha}" >&2
  exit 1
fi
if [[ -n "${merge_ref}" && ! "${merge_ref}" =~ ^refs/(merge-requests|pull)/[0-9]+/merge$ ]]; then
  echo "Invalid merge ref: ${merge_ref}" >&2
  exit 1
fi

export GITCODE_PR_IID="${pr_iid}"
export GITCODE_TARGET_BRANCH="${target_branch}"
export GITCODE_AFTER_COMMIT_SHA="${after_sha}"
export GITCODE_MERGE_REF="${merge_ref}"

# The merge commit is not known yet. manual_checkout writes ATOMGIT_ENV after
# the ref that actually fetched has replaced this guess.

if [[ -n "${GITCODE_TOKEN_FILE:-}" && -f "${GITCODE_TOKEN_FILE}" ]]; then
  rm -f "${GITCODE_TOKEN_FILE}"
fi

echo "Resolved GitCode env:"
echo "  GITCODE_PR_IID=${GITCODE_PR_IID}"
echo "  GITCODE_TARGET_BRANCH=${GITCODE_TARGET_BRANCH}"
echo "  GITCODE_AFTER_COMMIT_SHA=${GITCODE_AFTER_COMMIT_SHA}"
echo "  GITCODE_MERGE_REF=${GITCODE_MERGE_REF}"
