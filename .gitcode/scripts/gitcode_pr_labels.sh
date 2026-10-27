#!/usr/bin/env bash
set -euo pipefail

repo="${GITCODE_REPO:-openGauss/openGauss-server}"
# Drop platform workflow tokens. Label and comment writes must use
# PR_LABEL_TOKEN (opengauss-ci-bot) only. Merge rules reject action-bot.
unset ATOMGIT_TOKEN || true
unset GITHUB_TOKEN || true
token="${PR_LABEL_TOKEN:-}"
pr_iid="${GITCODE_PR_IID:-}"
action="${1:-}"

if [[ -z "${action}" ]]; then
  echo "Usage: $0 <running|success|failed|result>"
  exit 2
fi

# A comment event leaves the workflow iid empty. Resolve it from the event
# instead of trusting issue.number, which can be the comment id.
if [[ -z "${pr_iid}" && -n "${RESOLVE_SCRIPT:-}" ]]; then
  if [[ ! -f "${RESOLVE_SCRIPT}" ]]; then
    echo "Cannot find ${RESOLVE_SCRIPT}" >&2
    exit 3
  fi
  resolved="$(mktemp)"
  if bash -c 'source "$1" >&2; printf "GITCODE_PR_IID=%q\n" "${GITCODE_PR_IID:-}"' _ "${RESOLVE_SCRIPT}" > "${resolved}"; then
    count="$(grep -c '^GITCODE_PR_IID=' "${resolved}" || true)"
    if [[ "${count}" -eq 1 ]]; then
      eval "$(cat "${resolved}")"
    else
      rm -f "${resolved}"
      exit 3
    fi
  else
    rm -f "${resolved}"
    exit 3
  fi
  rm -f "${resolved}"
  pr_iid="${GITCODE_PR_IID:-}"
  if [[ -n "${pr_iid}" && ! "${pr_iid}" =~ ^[0-9]+$ ]]; then
    echo "Resolved PR iid is invalid: ${pr_iid}" >&2
    exit 3
  fi
fi

if [[ -z "${pr_iid}" ]]; then
  echo "No pull request IID found; skip GitCode PR labels."
  exit 0
fi

if [[ ! "${pr_iid}" =~ ^[0-9]+$ ]]; then
  echo "Invalid PR iid: ${pr_iid}" >&2
  exit 1
fi

if [[ ! "${repo}" =~ ^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$ ]]; then
  echo "Invalid repository: ${repo}" >&2
  exit 1
fi

if [[ -z "${token}" ]]; then
  echo "PR_LABEL_TOKEN is empty; cannot update GitCode PR labels." >&2
  exit 1
fi

run_id="${GITCODE_RUN_ID:-}"
if [[ -n "${run_id}" && ! "${run_id}" =~ ^[0-9]+$ ]]; then
  echo "Ignoring non-numeric GITCODE_RUN_ID." >&2
  run_id=""
fi
# One pipeline mints this once at the start and reuses it. Numbered runs record
# it beside ci-run-<id>, so a later pipeline with no run id can tell which write
# is newer. Two empty-id pipelines are ordered without a shared ci-run-0.
epoch="${GITCODE_RUN_EPOCH:-}"
if [[ -n "${epoch}" && ! "${epoch}" =~ ^[0-9]+$ ]]; then
  echo "Ignoring non-numeric GITCODE_RUN_EPOCH." >&2
  epoch=""
fi
# Step outputs do not reach the next job on this platform, so the shared epoch
# is often empty. Mint one here; a later job in the same pipeline is newer.
if [[ -z "${epoch}" ]]; then
  epoch="$(date +%s%3N 2>/dev/null || true)"
  case "${epoch}" in
    ''|*[!0-9]*) epoch="$(date +%s 2>/dev/null || true)" ;;
  esac
  case "${epoch}" in
    ''|*[!0-9]*) epoch="" ;;
    *) echo "GITCODE_RUN_EPOCH was empty; using ${epoch}." ;;
  esac
fi

api_root="https://api.gitcode.com/api/v5"
if [[ -n "${ATOMGIT_API_URL:-}" ]]; then
  # A root that already ends in /api/v5 must not become /api/v5/api/v5.
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
api_base="${api_root}/repos/${repo}/pulls/${pr_iid}/labels"

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

redact_line() {
  local line="$1"
  local encoded encoded_lower
  line="$(replace_all "${line}" "${token}" "[redacted]")"
  encoded="$(urlencode "${token}")"
  encoded_lower="$(printf '%s' "${encoded}" | tr 'A-F' 'a-f')"
  if [[ "${encoded}" != "${token}" ]]; then
    line="$(replace_all "${line}" "${encoded}" "[redacted]")"
  fi
  if [[ "${encoded_lower}" != "${encoded}" ]]; then
    line="$(replace_all "${line}" "${encoded_lower}" "[redacted]")"
  fi
  printf '%s\n' "${line}"
}

# Print the API body without the request URL. curl's own error text repeats that URL.
show_body() {
  local body="$1"
  local line
  if [[ ! -s "${body}" ]]; then
    return 0
  fi
  while IFS= read -r line || [[ -n "${line}" ]]; do
    redact_line "${line}" >&2
  done < "${body}"
}

delete_label() {
  local label="$1"
  local code err body
  err="$(mktemp)"
  body="$(mktemp)"
  code="$(curl -sS --max-time 30 -o "${body}" -w '%{http_code}' -G -X DELETE \
    --data-urlencode "access_token=${token}" \
    -H "Authorization: Bearer ${token}" \
    "${api_base}/${label}" 2>"${err}")" || code="000"
  rm -f "${err}"
  case "${code}" in
    200|204|404)
      rm -f "${body}"
      return 0
      ;;
    401|403)
      rm -f "${body}"
      echo "Failed to delete label ${label}: HTTP ${code}" >&2
      return 1
      ;;
    *)
      echo "Failed to delete label ${label}: HTTP ${code}" >&2
      show_body "${body}"
      rm -f "${body}"
      return 1
      ;;
  esac
}

label_color() {
  case "$1" in
    ci-pipeline-running) printf '%s\n' '#FBCA04' ;;
    ci-pipeline-success|ci-pipeline-passed) printf '%s\n' '#0E8A16' ;;
    ci-pipeline-failed) printf '%s\n' '#B60205' ;;
    *) printf '%s\n' '#CCCCCC' ;;
  esac
}

# A pull request can only wear a label that already exists on the repository.
# ci-pipeline-running, ci-pipeline-success and ci-pipeline-failed are created here.
ensure_repo_label() {
  local label="$1"
  local color code err body encoded
  color="$(label_color "${label}")"
  err="$(mktemp)"
  body="$(mktemp)"
  encoded="$(urlencode "${token}")"
  code="$(curl -sS --max-time 30 -o "${body}" -w '%{http_code}' -X POST \
    -H "Authorization: Bearer ${token}" \
    --form "name=${label}" \
    --form "color=${color}" \
    "${api_root}/repos/${repo}/labels?access_token=${encoded}" 2>"${err}")" || code="000"
  rm -f "${err}"
  case "${code}" in
    200|201)
      rm -f "${body}"
      echo "Created repository label ${label}." >&2
      return 0
      ;;
    400|409|422)
      rm -f "${body}"
      return 0
      ;;
    401|403)
      show_body "${body}"
      rm -f "${body}"
      echo "Cannot create repository label ${label}: HTTP ${code}" >&2
      return 1
      ;;
    *)
      echo "Repository label ${label} was not created: HTTP ${code}" >&2
      show_body "${body}"
      rm -f "${body}"
      return 0
      ;;
  esac
}

add_label() {
  local label="$1"
  local code err body encoded
  ensure_repo_label "${label}" || true
  err="$(mktemp)"
  body="$(mktemp)"
  encoded="$(urlencode "${token}")"
  # POST must not use -G. That flag moves the JSON body onto the query string.
  code="$(curl -sS --max-time 30 -o "${body}" -w '%{http_code}' -X POST \
    -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json;charset=UTF-8" \
    --data-binary "[\"${label}\"]" \
    "${api_base}?access_token=${encoded}" 2>"${err}")" || code="000"
  rm -f "${err}"
  case "${code}" in
    200|201|204)
      rm -f "${body}"
      return 0
      ;;
    409)
      # 409 does not by itself mean the label exists. Confirm with the list.
      rm -f "${body}"
      local present=0
      label_present "${label}" || present=$?
      if [[ "${present}" -eq 0 ]]; then
        echo "Label ${label} is already present." >&2
        return 0
      fi
      echo "Failed to add label ${label}: HTTP 409" >&2
      return 1
      ;;
    401|403)
      show_body "${body}"
      rm -f "${body}"
      echo "Failed to add label ${label}: HTTP ${code}" >&2
      return 1
      ;;
    *)
      echo "Failed to add label ${label}: HTTP ${code}" >&2
      show_body "${body}"
      rm -f "${body}"
      return 1
      ;;
  esac
}

delete_label_retry() {
  local label="$1"
  local attempt
  for attempt in 1 2 3; do
    if delete_label "${label}"; then
      return 0
    fi
    if [[ "${attempt}" -lt 3 ]]; then
      sleep 2
    fi
  done
  return 1
}

# Names of the CI labels currently on the pull request. Failure returns 1.
list_label_names() {
  local code err body
  err="$(mktemp)"
  body="$(mktemp)"
  code="$(curl -sS --max-time 30 -o "${body}" -w '%{http_code}' -G \
    --data-urlencode "access_token=${token}" \
    -H "Authorization: Bearer ${token}" \
    "${api_base}" 2>"${err}")" || code="000"
  rm -f "${err}"
  if [[ "${code}" != "200" ]]; then
    echo "Failed to list labels: HTTP ${code}" >&2
    show_body "${body}"
    rm -f "${body}"
    return 1
  fi
  grep -oE 'ci-pipeline-[A-Za-z0-9._-]+|ci-run-[0-9]+|ci-epoch-[0-9]+' "${body}" | sort -u || true
  rm -f "${body}"
}

# 0 present, 1 absent, 2 the list could not be read.
label_present() {
  local names
  if ! names="$(list_label_names)"; then
    return 2
  fi
  if printf '%s\n' "${names}" | grep -F -qx -- "$1"; then
    return 0
  fi
  return 1
}

num_gt() {
  local a="$1" b="$2"
  if [[ "${#a}" -gt "${#b}" ]]; then
    return 0
  fi
  if [[ "${#a}" -lt "${#b}" ]]; then
    return 1
  fi
  [[ "${a}" > "${b}" ]]
}

# 0: a newer run is present
# 1: the list was read and no newer run is present
# 2: the label list could not be read
newer_run_exists() {
  local names name num
  if [[ -z "${run_id}" ]]; then
    return 1
  fi
  if ! names="$(list_label_names)"; then
    return 2
  fi
  while IFS= read -r name || [[ -n "${name}" ]]; do
    if [[ "${name}" == ci-run-* ]]; then
      num="${name#ci-run-}"
      if [[ "${num}" =~ ^[0-9]+$ ]] && num_gt "${num}" "${run_id}"; then
        return 0
      fi
    elif [[ -n "${epoch}" && "${name}" == ci-epoch-* ]]; then
      num="${name#ci-epoch-}"
      if [[ "${num}" =~ ^[0-9]+$ && "${num}" != "${epoch}" ]] && num_gt "${num}" "${epoch}"; then
        return 0
      fi
    fi
  done <<< "${names}"
  return 1
}

delete_older_run_labels() {
  local names name num
  [[ -z "${run_id}" ]] && return 0
  names="$(list_label_names)" || return 0
  while IFS= read -r name || [[ -n "${name}" ]]; do
    [[ "${name}" == ci-run-* ]] || continue
    num="${name#ci-run-}"
    [[ "${num}" == "${run_id}" ]] && continue
    if [[ "${num}" =~ ^[0-9]+$ ]] && num_gt "${run_id}" "${num}"; then
      delete_label_retry "${name}" || true
    fi
  done <<< "${names}"
}

# Delete the old result names. On any failure, remove only the label this call
# just added. Never delete a result that may belong to a newer run.
delete_replaced_labels() {
  local add_name="$1"
  shift
  local label failed=0
  for label in "$@"; do
    # Arm retries running while the result job may already have finished.
    # Leave that success or failure in place.
    if [[ "${GITCODE_KEEP_TERMINAL_RESULT:-}" == "yes" ]]; then
      if [[ "${label}" == "ci-pipeline-success" || "${label}" == "ci-pipeline-failed" ]]; then
        continue
      fi
    fi
    if ! delete_label_retry "${label}"; then
      failed=1
    fi
  done
  if [[ "${failed}" -eq 0 ]]; then
    return 0
  fi
  echo "Removing ${add_name} because an old label could not be deleted." >&2
  delete_label_retry "${add_name}" || true
  return 1
}

# Add the new result, then delete the old ones. Callers that must confirm they
# are still the newest run use add_label and drop_observed_result_labels.
apply_labels() {
  local add_name="$1"
  add_label "${add_name}" || return 1
  delete_replaced_labels "$@"
}

# 0: names includes a run that must not have its result deleted
# 1: this snapshot does not show a newer run
# 0: this epoch must not replace the labels in the snapshot
# 1: this epoch may write
names_block_epoch() {
  local names="$1" name num saw_run=0 saw_running=0 saw_terminal=0 saw_our_epoch=0 saw_older_epoch=0
  [[ -z "${epoch}" ]] && return 1
  while IFS= read -r name || [[ -n "${name}" ]]; do
    if [[ "${name}" == ci-epoch-* ]]; then
      num="${name#ci-epoch-}"
      if [[ "${num}" == "${epoch}" ]]; then
        saw_our_epoch=1
      elif [[ "${num}" =~ ^[0-9]+$ ]] && num_gt "${num}" "${epoch}"; then
        return 0
      elif [[ "${num}" =~ ^[0-9]+$ ]] && num_gt "${epoch}" "${num}"; then
        saw_older_epoch=1
      fi
    elif [[ "${name}" == ci-run-* ]]; then
      num="${name#ci-run-}"
      if [[ "${num}" =~ ^[0-9]+$ ]]; then
        saw_run=1
      fi
    elif [[ "${name}" == "ci-pipeline-running" ]]; then
      saw_running=1
    elif [[ "${name}" == "ci-pipeline-success" || "${name}" == "ci-pipeline-failed" ]]; then
      saw_terminal=1
    fi
  done <<< "${names}"
  # A finished numbered run can be replaced once its time stamp is older, or
  # once it never recorded a time stamp and its result is already visible.
  # An in-flight numbered run that has no time stamp still owns the label.
  if [[ "${saw_run}" -eq 1 && "${saw_our_epoch}" -eq 0 && "${saw_older_epoch}" -eq 0 ]]; then
    if [[ "${saw_running}" -eq 1 || "${saw_terminal}" -eq 0 ]]; then
      return 0
    fi
  fi
  return 1
}

list_shows_newer_run() {
  local names="$1" name num blocked=0
  if [[ -n "${run_id}" ]]; then
    while IFS= read -r name || [[ -n "${name}" ]]; do
      if [[ "${name}" == ci-run-* ]]; then
        num="${name#ci-run-}"
        if [[ "${num}" =~ ^[0-9]+$ ]] && num_gt "${num}" "${run_id}"; then
          return 0
        fi
      elif [[ -n "${epoch}" && "${name}" == ci-epoch-* ]]; then
        num="${name#ci-epoch-}"
        if [[ "${num}" =~ ^[0-9]+$ && "${num}" != "${epoch}" ]] && num_gt "${num}" "${epoch}"; then
          return 0
        fi
      fi
    done <<< "${names}"
    return 1
  fi
  if [[ -n "${epoch}" ]]; then
    names_block_epoch "${names}" || blocked=$?
    if [[ "${blocked}" -eq 0 ]]; then
      return 0
    fi
  fi
  return 1
}

# Each old result is deleted only when a freshly read list still shows this run
# as newest and already contains that exact label. A result added after that
# read is not in the list, so it is left alone.
# 0: observed old names were removed
# 2: a newer run won; only this result and its marker were removed
# 1: the list could not be read, or a delete failed; this result was removed
drop_observed_result_labels() {
  local add_name="$1"
  local marker="$2"
  shift 2
  local names label stale
  for label in "$@"; do
    if [[ "${GITCODE_KEEP_TERMINAL_RESULT:-}" == "yes" ]]; then
      if [[ "${label}" == "ci-pipeline-success" || "${label}" == "ci-pipeline-failed" ]]; then
        continue
      fi
    fi
    if ! names="$(list_label_names)"; then
      echo "Cannot read pull request labels before deleting ${label}; removing ${add_name}." >&2
      delete_label_retry "${add_name}" || true
      return 1
    fi
    stale=0
    list_shows_newer_run "${names}" || stale=$?
    if [[ "${stale}" -eq 0 ]]; then
      echo "A newer pipeline run won before ${label} was removed; removing ${add_name}." >&2
      delete_label_retry "${add_name}" || true
      delete_label_retry "${marker}" || true
      return 2
    fi
    if printf '%s\n' "${names}" | grep -F -qx -- "${label}"; then
      if ! delete_label_retry "${label}"; then
        echo "Removing ${add_name} because ${label} could not be deleted." >&2
        delete_label_retry "${add_name}" || true
        return 1
      fi
    fi
  done
  return 0
}

# If this run is still newest but its result disappeared, write it again.
# 0: the result is present and this run is still newest
# 2: a newer run won; this result and its marker were removed
# 1: the list or the restore could not be confirmed
restore_result_if_newest() {
  local add_name="$1"
  local marker="$2"
  local names stale=0
  if ! names="$(list_label_names)"; then
    echo "Cannot read pull request labels after removing old results; leaving ${marker} in place." >&2
    return 1
  fi
  list_shows_newer_run "${names}" || stale=$?
  if [[ "${stale}" -eq 0 ]]; then
    echo "A newer pipeline run won after old labels were removed; removing ${add_name}." >&2
    delete_label_retry "${add_name}" || true
    delete_label_retry "${marker}" || true
    return 2
  fi
  if printf '%s\n' "${names}" | grep -F -qx -- "${add_name}"; then
    return 0
  fi
  echo "Restoring ${add_name}; it is still the newest result." >&2
  if ! add_label "${add_name}"; then
    return 1
  fi
  if ! names="$(list_label_names)"; then
    echo "Cannot read pull request labels after restoring ${add_name}; leaving ${marker} in place." >&2
    return 1
  fi
  stale=0
  list_shows_newer_run "${names}" || stale=$?
  if [[ "${stale}" -eq 0 ]]; then
    echo "A newer pipeline run won after ${add_name} was restored; removing it." >&2
    delete_label_retry "${add_name}" || true
    delete_label_retry "${marker}" || true
    return 2
  fi
  return 0
}

# 0: names includes ci-pipeline-success or ci-pipeline-failed
# 1: this snapshot has neither
terminal_result_visible() {
  local names="$1" name
  while IFS= read -r name || [[ -n "${name}" ]]; do
    if [[ "${name}" == "ci-pipeline-success" || "${name}" == "ci-pipeline-failed" ]]; then
      return 0
    fi
  done <<< "${names}"
  return 1
}

# Arm can retry running after the result label has been written. That retry
# must not remove the finished label, and must not leave running beside it.
# 0: no finished label, or this call is allowed to replace one
# 2: a finished label is present and ci-pipeline-running was removed
# 1: the label list could not be read, or running could not be removed
keep_finished_result() {
  local names stale=0
  [[ "${GITCODE_KEEP_TERMINAL_RESULT:-}" == "yes" ]] || return 0
  if ! names="$(list_label_names)"; then
    echo "Cannot read pull request labels while keeping a finished result." >&2
    return 1
  fi
  terminal_result_visible "${names}" || stale=$?
  if [[ "${stale}" -ne 0 ]]; then
    return 0
  fi
  echo "A success or failure label is already present; not replacing it with running." >&2
  if ! delete_label_retry "ci-pipeline-running"; then
    return 1
  fi
  return 2
}

finish_result_labels() {
  local add_name="$1"
  local marker="$2"
  shift 2
  local stale=0
  local kept=0
  keep_finished_result || kept=$?
  if [[ "${kept}" -eq 2 ]]; then
    return 0
  fi
  if [[ "${kept}" -ne 0 ]]; then
    return 1
  fi
  drop_observed_result_labels "${add_name}" "${marker}" "$@" || stale=$?
  if [[ "${stale}" -eq 2 ]]; then
    if [[ -n "${epoch}" && "${marker}" != "ci-epoch-${epoch}" ]]; then
      delete_label_retry "ci-epoch-${epoch}" || true
    fi
    return 0
  fi
  if [[ "${stale}" -ne 0 ]]; then
    return 1
  fi
  stale=0
  restore_result_if_newest "${add_name}" "${marker}" || stale=$?
  if [[ "${stale}" -eq 2 ]]; then
    if [[ -n "${epoch}" && "${marker}" != "ci-epoch-${epoch}" ]]; then
      delete_label_retry "ci-epoch-${epoch}" || true
    fi
    return 0
  fi
  if [[ "${stale}" -ne 0 ]]; then
    return 1
  fi
  kept=0
  keep_finished_result || kept=$?
  if [[ "${kept}" -eq 2 ]]; then
    return 0
  fi
  if [[ "${kept}" -ne 0 ]]; then
    return 1
  fi
  if [[ -n "${run_id}" ]]; then
    delete_older_run_labels || true
  fi
  if [[ -n "${epoch}" ]]; then
    delete_older_epochs || true
  fi
}

# No run id and no epoch cannot be ordered. Write only when nothing is recorded.
# Returning success when a marker is already present would record the label as
# written, and the arm job would not retry with the epoch from prepare.
finish_without_run_id() {
  local names
  if ! names="$(list_label_names)"; then
    echo "Cannot read pull request labels and GITCODE_RUN_ID is empty; leaving labels unchanged." >&2
    return 1
  fi
  if printf '%s\n' "${names}" | grep -Eq '^(ci-run-|ci-epoch-)'; then
    echo "A pipeline marker is already recorded and this job has no run id or epoch; leaving labels unchanged." >&2
    return 1
  fi
  local kept=0
  keep_finished_result || kept=$?
  if [[ "${kept}" -eq 2 ]]; then
    return 0
  fi
  if [[ "${kept}" -ne 0 ]]; then
    return 1
  fi
  apply_labels "$@" || return 1
  kept=0
  keep_finished_result || kept=$?
  if [[ "${kept}" -eq 2 ]]; then
    return 0
  fi
  if [[ "${kept}" -ne 0 ]]; then
    return 1
  fi
}

# 0: a newer time stamp is recorded, or an in-flight numbered run has no stamp
# 1: the list was read and this epoch may write
# 2: the label list could not be read
epoch_is_stale() {
  local names blocked=0
  if [[ -z "${epoch}" ]]; then
    return 1
  fi
  if ! names="$(list_label_names)"; then
    return 2
  fi
  names_block_epoch "${names}" || blocked=$?
  if [[ "${blocked}" -eq 0 ]]; then
    return 0
  fi
  return 1
}

delete_older_epochs() {
  local names name num
  [[ -z "${epoch}" ]] && return 0
  names="$(list_label_names)" || return 0
  while IFS= read -r name || [[ -n "${name}" ]]; do
    [[ "${name}" == ci-epoch-* ]] || continue
    num="${name#ci-epoch-}"
    [[ "${num}" == "${epoch}" ]] && continue
    if [[ "${num}" =~ ^[0-9]+$ ]] && num_gt "${epoch}" "${num}"; then
      delete_label_retry "${name}" || true
    fi
  done <<< "${names}"
}

# Same ordering as ci-run-<id>, but the marker is ci-epoch-<n> from this pipeline.
# A later time stamp can replace a finished numbered run. An in-flight numbered
# run that never recorded a time stamp still keeps its label.
finish_with_epoch() {
  local stale=0
  local add_name="$1"
  epoch_is_stale || stale=$?
  if [[ "${stale}" -eq 0 ]]; then
    echo "A newer or numbered pipeline already labeled this pull request; leaving labels unchanged." >&2
    return 0
  fi
  if [[ "${stale}" -eq 2 ]]; then
    echo "Cannot read pull request labels; leaving labels unchanged." >&2
    return 1
  fi
  if ! add_label "ci-epoch-${epoch}"; then
    echo "Could not record ci-epoch-${epoch}; still writing ${add_name}." >&2
  fi
  stale=0
  epoch_is_stale || stale=$?
  if [[ "${stale}" -eq 0 ]]; then
    echo "A newer pipeline run won the label update; leaving labels unchanged." >&2
    delete_label_retry "ci-epoch-${epoch}" || true
    return 0
  fi
  if [[ "${stale}" -eq 2 ]]; then
    echo "Cannot read pull request labels after recording ci-epoch-${epoch}; leaving result labels unchanged." >&2
    return 1
  fi
  local kept=0
  keep_finished_result || kept=$?
  if [[ "${kept}" -eq 2 ]]; then
    return 0
  fi
  if [[ "${kept}" -ne 0 ]]; then
    return 1
  fi
  if ! add_label "${add_name}"; then
    return 1
  fi
  stale=0
  epoch_is_stale || stale=$?
  if [[ "${stale}" -eq 0 ]]; then
    echo "A newer pipeline run won after ${add_name} was added; removing it." >&2
    delete_label_retry "${add_name}" || true
    delete_label_retry "ci-epoch-${epoch}" || true
    return 0
  fi
  if [[ "${stale}" -eq 2 ]]; then
    echo "Cannot read pull request labels after adding ${add_name}; removing it." >&2
    delete_label_retry "${add_name}" || true
    return 1
  fi
  finish_result_labels "${add_name}" "ci-epoch-${epoch}" "${@:2}" || return
  return 0
}

# One pipeline records ci-run-<id> and leaves it in place. A later call from an
# older id sees that label and does not replace the newer result. Other result
# labels are removed only after this id is still the newest one.
finish_labels() {
  local stale=0
  local add_name="$1"
  if [[ -z "${run_id}" ]]; then
    if [[ -n "${epoch}" ]]; then
      finish_with_epoch "$@" || return
      return 0
    fi
    finish_without_run_id "$@" || return
    return 0
  fi
  newer_run_exists || stale=$?
  if [[ "${stale}" -eq 0 ]]; then
    echo "A newer pipeline run already labeled this pull request; leaving labels unchanged." >&2
    return 0
  fi
  if [[ "${stale}" -eq 2 ]]; then
    echo "Cannot read pull request labels; leaving labels unchanged." >&2
    return 1
  fi
  if ! add_label "ci-run-${run_id}"; then
    echo "Could not record ci-run-${run_id}; still writing ${add_name}." >&2
  fi
  stale=0
  newer_run_exists || stale=$?
  if [[ "${stale}" -eq 0 ]]; then
    echo "A newer pipeline run won the label update; leaving result labels unchanged." >&2
    delete_label_retry "ci-run-${run_id}" || true
    return 0
  fi
  if [[ "${stale}" -eq 2 ]]; then
    echo "Cannot read pull request labels; leaving result labels unchanged." >&2
    return 1
  fi
  if [[ -n "${epoch}" ]]; then
    if ! add_label "ci-epoch-${epoch}"; then
      echo "Could not record ci-epoch-${epoch}; still writing ${add_name}." >&2
    fi
    stale=0
    newer_run_exists || stale=$?
    if [[ "${stale}" -eq 0 ]]; then
      echo "A newer pipeline run won after ci-epoch-${epoch} was added; removing it." >&2
      delete_label_retry "ci-epoch-${epoch}" || true
      delete_label_retry "ci-run-${run_id}" || true
      return 0
    fi
    if [[ "${stale}" -eq 2 ]]; then
      echo "Cannot read pull request labels after recording ci-epoch-${epoch}; removing it." >&2
      delete_label_retry "ci-epoch-${epoch}" || true
      return 1
    fi
  fi
  local kept=0
  keep_finished_result || kept=$?
  if [[ "${kept}" -eq 2 ]]; then
    return 0
  fi
  if [[ "${kept}" -ne 0 ]]; then
    return 1
  fi
  if ! add_label "${add_name}"; then
    return 1
  fi
  stale=0
  newer_run_exists || stale=$?
  if [[ "${stale}" -eq 0 ]]; then
    echo "A newer pipeline run won after ${add_name} was added; removing it." >&2
    delete_label_retry "${add_name}" || true
    delete_label_retry "ci-run-${run_id}" || true
    if [[ -n "${epoch}" ]]; then
      delete_label_retry "ci-epoch-${epoch}" || true
    fi
    return 0
  fi
  if [[ "${stale}" -eq 2 ]]; then
    echo "Cannot read pull request labels after adding ${add_name}; removing it." >&2
    delete_label_retry "${add_name}" || true
    if [[ -n "${epoch}" ]]; then
      delete_label_retry "ci-epoch-${epoch}" || true
    fi
    return 1
  fi
  finish_result_labels "${add_name}" "ci-run-${run_id}" "${@:2}" || return
  return 0
}

# pipeline.upstream_status is empty on AtomGit. The test jobs are visible on the
# actions run, so success or failed is taken from those jobs.
run_python() {
  if command -v python3 >/dev/null 2>&1; then
    python3 - "$@"
  elif command -v python >/dev/null 2>&1; then
    python - "$@"
  else
    return 1
  fi
}

test_jobs_status() {
  local json_path="$1"
  PIPELINE_RUN_JSON="${json_path}" run_python << 'PY'
import json, os, sys
run = json.load(open(os.environ["PIPELINE_RUN_JSON"], encoding="utf-8"))
prefixes = ("openGauss_server_x86", "openGauss_server_arm")
seen = False
failed = False
for stage in run.get("stages") or []:
    for job in stage.get("jobs") or []:
        ident = job.get("identifier") or ""
        if not ident.startswith(prefixes):
            continue
        seen = True
        status = (job.get("status") or "").upper()
        if status not in ("COMPLETED", "SUCCESS"):
            failed = True
if not seen:
    sys.exit(1)
print("failed" if failed else "success")
PY
}

derive_pipeline_status() {
  local api_root body code candidate verdict list_body run_id
  api_root="https://api.gitcode.com/api/v8"
  body="$(mktemp)"
  verdict=""
  try_run() {
    local id="$1"
    local try_code=""
    [[ -n "${id}" ]] || return 1
    try_code="$(curl -sS -o "${body}" -w '%{http_code}' --max-time 30 \
      "${api_root}/repos/${repo}/actions/runs/${id}" || true)"
    [[ "${try_code}" == "200" ]] || return 1
    grep -q '"stages"' "${body}" || return 1
    verdict="$(test_jobs_status "${body}")" || verdict=""
    [[ "${verdict}" == "success" || "${verdict}" == "failed" ]]
  }

  for candidate in \
    "${GITCODE_ACTIONS_RUN_ID:-}" \
    "${GITCODE_RUN_ID:-}" \
    "${ATOMGIT_RUN_ID:-}"
  do
    if try_run "${candidate}"; then
      printf '%s\n' "${verdict}"
      rm -f "${body}"
      return 0
    fi
  done

  list_body="$(mktemp)"
  code="$(curl -sS -o "${list_body}" -w '%{http_code}' --max-time 30 \
    "${api_root}/repos/${repo}/actions/runs?per_page=20" || true)"
  run_id=""
  if [[ "${code}" == "200" ]]; then
    run_id="$(
      PIPELINE_RUN_JSON="${list_body}" \
      PIPELINE_RUN_NUMBER="${ATOMGIT_RUN_NUMBER:-}" \
      PIPELINE_WORKFLOW="${ATOMGIT_WORKFLOW:-openGauss_server_PR}" \
      run_python << 'PY' || true
import json, os, sys
data = json.load(open(os.environ["PIPELINE_RUN_JSON"], encoding="utf-8"))
want_num = os.environ.get("PIPELINE_RUN_NUMBER", "")
want_name = os.environ.get("PIPELINE_WORKFLOW", "")
for item in data.get("workflow_runs") or []:
    if want_num and str(item.get("run_number")) != str(want_num):
        continue
    name = item.get("workflow_name") or ""
    if want_name and name and name != want_name:
        continue
    found = item.get("workflow_run_id") or ""
    if found:
        print(found)
        sys.exit(0)
sys.exit(1)
PY
    )" || run_id=""
  fi
  rm -f "${list_body}"
  if [[ -n "${run_id}" ]] && try_run "${run_id}"; then
    printf '%s\n' "${verdict}"
    rm -f "${body}"
    return 0
  fi
  rm -f "${body}"
  return 1
}

# A matrix that did not succeed must not be reported as success or completed.
matrix_result_incomplete() {
  local value
  value="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  case "${value}" in
    failure|failed|cancelled|canceled|skipped) return 0 ;;
  esac
  return 1
}

if [[ "${action}" == "result" ]]; then
  status="${PIPELINE_UPSTREAM_STATUS:-}"
  case "${status}" in
    ''|null|NULL)
      echo "pipeline.upstream_status is empty; reading test jobs from the actions run." >&2
      if ! status="$(derive_pipeline_status)"; then
        echo "Could not read test job status; reporting failure." >&2
        status="failed"
      fi
      echo "Derived pipeline status=${status}." >&2
      ;;
  esac
  lowered="$(printf '%s' "${status}" | tr '[:upper:]' '[:lower:]')"
  case "${lowered}" in
    success|completed)
      # Skipped, cancelled, or failed matrices can still leave the platform
      # status as success or completed. A cancelled test must not be labeled success.
      # proceed=skip: docs/ci-config only; prepare already wrote success.
      # Do not treat fail_closed job failure alone as pipeline failure: that job
      # runs in parallel with tests and can flake while LLT is still running.
      if [[ "${GITCODE_PREPARE_PROCEED:-}" == "skip" ]]; then
        echo "Only ignored paths changed; keeping success." >&2
        action="success"
      elif [[ "${GITCODE_TESTS_RAN:-}" == "no" || "${GITCODE_PREPARE_PROCEED:-}" == "no" || "${GITCODE_PREPARE_RESULT:-}" == "cancelled" || "${GITCODE_PREPARE_RESULT:-}" == "canceled" || "${GITCODE_PREPARE_RESULT:-}" == "skipped" ]] || matrix_result_incomplete "${GITCODE_X86_RESULT:-}" || matrix_result_incomplete "${GITCODE_ARM_RESULT:-}"; then
        echo "Tests did not run; reporting failure instead of success." >&2
        action="failed"
      else
        action="success"
      fi
      ;;
    *) action="failed" ;;
  esac
fi

find_actions_run_id() {
  local list_body code found candidate
  list_body="$(mktemp)"
  code="$(curl -sS -o "${list_body}" -w '%{http_code}' --max-time 30 \
    "https://api.gitcode.com/api/v8/repos/${repo}/actions/runs?per_page=20" || true)"
  found=""
  if [[ "${code}" == "200" ]]; then
    found="$(
      PIPELINE_RUN_JSON="${list_body}" \
      PIPELINE_RUN_NUMBER="${ATOMGIT_RUN_NUMBER:-}" \
      PIPELINE_WORKFLOW="${ATOMGIT_WORKFLOW:-openGauss_server_PR}" \
      run_python << 'PY' || true
import json, os, sys
data = json.load(open(os.environ["PIPELINE_RUN_JSON"], encoding="utf-8"))
want_num = os.environ.get("PIPELINE_RUN_NUMBER", "")
want_name = os.environ.get("PIPELINE_WORKFLOW", "")
for item in data.get("workflow_runs") or []:
    if want_num and str(item.get("run_number")) != str(want_num):
        continue
    name = item.get("workflow_name") or ""
    if want_name and name and name != want_name:
        continue
    run_id = item.get("workflow_run_id") or ""
    if run_id:
        print(run_id)
        sys.exit(0)
sys.exit(1)
PY
    )" || found=""
  fi
  rm -f "${list_body}"
  if [[ -n "${found}" ]]; then
    printf '%s\n' "${found}"
    return 0
  fi
  for candidate in "${GITCODE_ACTIONS_RUN_ID:-}" "${GITCODE_RUN_ID:-}" "${ATOMGIT_RUN_ID:-}"; do
    if [[ "${candidate}" =~ ^[0-9A-Za-z._-]+$ ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  return 1
}

post_pipeline_started_comment() {
  local run_id server page_url comment_file code err body encoded method
  run_id="$(find_actions_run_id)" || {
    echo "Cannot find this actions run; the start comment was not posted." >&2
    return 1
  }
  server="${ATOMGIT_SERVER_URL:-${GITCODE_SERVER_URL:-https://gitcode.com}}"
  server="${server%/}"
  case "${server}" in
    https://api.*) server="https://${server#https://api.}" ;;
    http://api.*) server="http://${server#http://api.}" ;;
  esac
  page_url="${server}/${repo}/actions/runs/${run_id}"
  comment_file="$(mktemp)"
  if ! PIPELINE_RUN_URL="${page_url}" PIPELINE_COMMENT_FILE="${comment_file}" run_python << 'PY'
import json, os
url = os.environ["PIPELINE_RUN_URL"]
text = (
    "| CI | 流水线已启动 |\n"
    "| --- | --- |\n"
    "| 执行详情 | [点击查看流水线](%s) |\n" % url
)
with open(os.environ["PIPELINE_COMMENT_FILE"], "w", encoding="utf-8") as handle:
    json.dump({"body": text}, handle, ensure_ascii=False)
PY
  then
    rm -f "${comment_file}"
    echo "Cannot build the start comment." >&2
    return 1
  fi
  err="$(mktemp)"
  body="$(mktemp)"
  encoded="$(urlencode "${token}")"
  code="000"
  for method in POST PUT; do
    code="$(curl -sS --max-time 30 -o "${body}" -w '%{http_code}' -X "${method}" \
      -H "Authorization: Bearer ${token}" \
      -H "Content-Type: application/json;charset=UTF-8" \
      --data-binary "@${comment_file}" \
      "${api_root}/repos/${repo}/pulls/${pr_iid}/comments?access_token=${encoded}" 2>"${err}")" || code="000"
    case "${code}" in
      200|201) break ;;
      405) continue ;;
      *) break ;;
    esac
  done
  rm -f "${err}" "${comment_file}"
  case "${code}" in
    200|201)
      rm -f "${body}"
      echo "Posted the pipeline start comment: ${page_url}"
      return 0
      ;;
    *)
      echo "Failed to post the pipeline start comment: HTTP ${code}" >&2
      show_body "${body}"
      rm -f "${body}"
      return 1
      ;;
  esac
}

case "${action}" in
  running)
    if [[ "${GITCODE_KEEP_TERMINAL_RESULT:-}" == "yes" ]]; then
      finish_labels ci-pipeline-running ci-pipeline-passed
    else
      finish_labels ci-pipeline-running ci-pipeline-success ci-pipeline-failed ci-pipeline-passed
      post_pipeline_started_comment
    fi
    ;;
  success)
    finish_labels ci-pipeline-success ci-pipeline-running ci-pipeline-failed ci-pipeline-passed
    ;;
  failed)
    finish_labels ci-pipeline-failed ci-pipeline-running ci-pipeline-success ci-pipeline-passed
    ;;
  *)
    echo "Unknown label action: ${action}"
    exit 2
    ;;
esac
