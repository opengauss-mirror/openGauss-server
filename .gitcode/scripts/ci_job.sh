#!/usr/bin/env bash
# One matrix test on a self-hosted runner. The workflow downloads this file
# from refs/heads/master and then executes that copy.
set -euo pipefail

# Release-branch launchers keep their own image and arguments. When a pull
# request is being tested and that launcher does not already pass the merge
# ref, insert it beside the commit variable. The merge commit is already
# known at this point; omitting the ref leaves the container with only a raw
# sha, which the server may refuse to fetch.
attach_merge_ref() {
  local tree_script="$1"
  local sha_arg ref_arg line anchor
  sha_arg='-e gitcodeAfterCommitSha=${GITCODE_AFTER_COMMIT_SHA}'
  sha_arg_quoted='-e "gitcodeAfterCommitSha=${GITCODE_AFTER_COMMIT_SHA}"'
  sha_arg_quoted_default='-e "gitcodeAfterCommitSha=${GITCODE_AFTER_COMMIT_SHA:-}"'
  ref_arg='-e gitcodeRef=${GITCODE_MERGE_REF}'
  ref_arg_quoted='-e "gitcodeRef=${GITCODE_MERGE_REF}"'
  # ${GITCODE_MERGE_REF:-} does not contain ${GITCODE_MERGE_REF} as text.
  # Either form already passes the ref and must not be patched again.
  ref_with_default='-e gitcodeRef=${GITCODE_MERGE_REF:-}'
  ref_with_default_quoted='-e "gitcodeRef=${GITCODE_MERGE_REF:-}"'
  if [[ -z "${GITCODE_PR_IID:-}" ]] \
    || grep -F -q -- "${ref_arg}" "${tree_script}" \
    || grep -F -q -- "${ref_arg_quoted}" "${tree_script}" \
    || grep -F -q -- "${ref_with_default}" "${tree_script}" \
    || grep -F -q -- "${ref_with_default_quoted}" "${tree_script}"; then
    printf '%s\n' "${tree_script}"
    return 0
  fi
  anchor=""
  if grep -F -q -- "${sha_arg}" "${tree_script}"; then
    anchor="${sha_arg}"
  elif grep -F -q -- "${sha_arg_quoted_default}" "${tree_script}"; then
    anchor="${sha_arg_quoted_default}"
  elif grep -F -q -- "${sha_arg_quoted}" "${tree_script}"; then
    anchor="${sha_arg_quoted}"
  else
    echo "Checked out ${tree_script} cannot receive the merge ref." >&2
    return 1
  fi
  patched_script="$(mktemp)"
  while IFS= read -r line || [[ -n "${line}" ]]; do
    printf '%s\n' "${line//"${anchor}"/"${anchor} ${ref_arg_quoted}"}"
  done < "${tree_script}" > "${patched_script}"
  echo "Checked out $(basename "${tree_script}") does not pass gitcodeRef; added it for this run." >&2
  printf '%s\n' "${patched_script}"
}

lock=/tmp/opengauss-server-ci.lock

# An environment variable must not skip the lock. Only the flock re-exec passes
# this argument.
if [[ "${1:-}" != "--lock-held" ]]; then
  if ! command -v flock >/dev/null 2>&1; then
    echo "flock is required so a killed job releases the machine lock." >&2
    exit 1
  fi
  # A live holder of the old directory lock removes it on exit. A directory that
  # stays fresh for 160 minutes is an orphan refresher, not a running test.
  if [[ -d "${lock}" ]]; then
    echo "Waiting for a legacy directory lock at ${lock}."
    waited=0
    while [[ -d "${lock}" && "${waited}" -lt 9600 ]]; do
      mtime="$(stat -c %Y "${lock}" 2>/dev/null || true)"
      if [[ -z "${mtime}" || "${mtime}" == "0" ]]; then
        echo "Cannot read the legacy lock time; refusing to delete a lock that may still be live." >&2
        exit 1
      fi
      now="$(date +%s)"
      if [[ $((now - mtime)) -gt 180 ]]; then
        rm -rf "${lock}"
        break
      fi
      sleep 15
      waited=$((waited + 15))
    done
    if [[ -d "${lock}" ]]; then
      echo "Legacy lock is still being refreshed after ${waited}s; removing the orphan."
      rm -rf "${lock}"
    fi
  fi
  exec flock "${lock}" bash "$0" --lock-held
fi

remove_ci_containers() {
  local name
  for name in opengauss-llt-1.0.0 opengauss-llt-A-1.0.0 opengauss-llt-B-1.0.0 opengauss-llt-C-1.0.0 opengauss-om-install-1.0.0 opengauss-om-upgrade-1.0.0 opengauss-ss-1.0.0 opengauss-ss_hatest-1.0.0; do
    docker rm -f "${name}" >/dev/null 2>&1 || true
  done
}

# /tmp lock is local to this machine. Include a machine id so a shared
# WORKSPACE_ROOT does not let another architecture rewrite this clone.
runner_id() {
  local host machine
  host="$(hostname -s 2>/dev/null || hostname 2>/dev/null || true)"
  machine=""
  if [[ -r /etc/machine-id ]]; then
    machine="$(tr -cd 'A-Za-z0-9' < /etc/machine-id)"
    machine="${machine:0:12}"
  fi
  host="$(printf '%s' "${host}" | tr -cd 'A-Za-z0-9._-')"
  host="${host:-runner}"
  if [[ -n "${machine}" ]]; then
    host="${host}-${machine}"
  fi
  printf '%s\n' "${host}"
}

script="${CI_MATRIX_SCRIPT:-}"
case "${script}" in
  main_llt_A.sh|main_llt_B.sh|main_llt_C.sh|main_om_install.sh|main_om_upgrade.sh|main_ss.sh|main_ss_hatest.sh)
    ;;
  *)
    echo "Unexpected test script: ${script}" >&2
    exit 1
    ;;
esac

# Keep the clone inside this job's directory. Never delete .git in the runner's
# original working directory, which may be a shared full clone.
workspace_base="${WORKSPACE_ROOT:-/usr1/gauss_jenkins/jenkins/workspace1}"
workspace_base="${workspace_base%/}"
workspace="${workspace_base}/opengauss-server-ci/$(runner_id)/${script%.sh}"
mkdir -p "${workspace}"
cd "${workspace}"
export CI_REINIT_GIT=1
remove_ci_containers

# A fork event must not choose the repository that supplies host scripts.
repo="${GITCODE_REPO:-openGauss/openGauss-server}"
if [[ ! "${repo}" =~ ^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$ ]]; then
  echo "Invalid repository: ${repo}" >&2
  exit 1
fi
server="${ATOMGIT_SERVER_URL:-}"
if [[ -z "${server}" ]]; then
  server="${GITCODE_SERVER_URL:-}"
fi
if [[ -z "${server}" ]]; then
  server="https://gitcode.com"
fi
repo_url="${server%/}/${repo}.git"

if [[ -d .git && ! -f .git/shallow ]]; then
  echo "Replacing a full clone so a depth-limited fetch can proceed."
  rm -rf .git
fi
if [[ ! -d .git ]]; then
  git init
fi
if git remote | grep -qx origin; then
  git remote set-url origin "${repo_url}"
else
  git remote add origin "${repo_url}"
fi
timeout 300 git fetch --depth=1 --force origin refs/heads/master

resolve_copy="$(mktemp)"
checkout_copy="$(mktemp)"
loader_copy="$(mktemp)"
launcher_copy="$(mktemp)"
labels_copy="$(mktemp)"
patched_script=""
token_file="${GITCODE_TOKEN_FILE:-}"
label_token=""
test_running=0
# The test process must not see the label token. Keep it in this shell only,
# and use it when the test is killed before the result job can write a label.
report_test_timeout() {
  [[ -n "${label_token}" && -s "${labels_copy}" && -s "${resolve_copy}" ]] || return 0
  echo "Test timed out or was cancelled; recording a failed pull request label." >&2
  PR_LABEL_TOKEN="${label_token}" \
    RESOLVE_SCRIPT="${resolve_copy}" \
    PIPELINE_UPSTREAM_STATUS=FAILED \
    GITCODE_TESTS_RAN=no \
    bash "${labels_copy}" result || true
}
cleanup_tmp() {
  if [[ "${test_running}" -eq 1 ]]; then
    test_running=0
    report_test_timeout || true
  fi
  rm -f "${resolve_copy}" "${checkout_copy}" "${loader_copy}" "${launcher_copy}" "${labels_copy}"
  if [[ -n "${patched_script}" ]]; then
    rm -f "${patched_script}"
  fi
  if [[ -n "${token_file}" ]]; then
    rm -f "${token_file}"
  fi
  remove_ci_containers
}
trap cleanup_tmp EXIT

git show "FETCH_HEAD:.gitcode/scripts/resolve_gitcode_env.sh" > "${resolve_copy}"
git show "FETCH_HEAD:.gitcode/scripts/manual_checkout.sh" > "${checkout_copy}"
git show "FETCH_HEAD:.gitcode/scripts/load_ci_env.sh" > "${loader_copy}"
git show "FETCH_HEAD:.gitcode/scripts/gitcode_pr_labels.sh" > "${labels_copy}"
git show "FETCH_HEAD:.gitcode/scripts/${script}" > "${launcher_copy}" || true

RESOLVE_SCRIPT="${resolve_copy}" bash "${checkout_copy}"
if [[ -n "${token_file}" && -f "${token_file}" ]]; then
  IFS= read -r label_token < "${token_file}" || true
fi
if [[ -n "${token_file}" ]]; then
  rm -f "${token_file}"
  token_file=""
fi
unset PR_LABEL_TOKEN || true
unset ATOMGIT_TOKEN || true
unset GITCODE_TOKEN_FILE || true
mkdir -p .gitcode/scripts
cp "${loader_copy}" .gitcode/scripts/load_ci_env.sh
if [[ ! -f .gitcode_ci_env ]]; then
  echo "Checkout did not write .gitcode_ci_env" >&2
  exit 1
fi
# shellcheck disable=SC1091
source .gitcode/scripts/load_ci_env.sh

tree_script=".gitcode/scripts/${script}"
run_script=""
if [[ -s "${tree_script}" ]]; then
  run_script="$(attach_merge_ref "${tree_script}")"
elif [[ -s "${launcher_copy}" ]]; then
  echo "Checked out tree has no ${script}; using master's copy."
  run_script="${launcher_copy}"
fi
if [[ -z "${run_script}" ]]; then
  echo "Cannot find ${script} on the checked out branch or on master." >&2
  exit 1
fi
# The label token was only needed to read the pull request target.
unset PR_LABEL_TOKEN
test_running=1
set +e
timeout 120m bash "${run_script}"
rc=$?
set -e
test_running=0
if [[ "${rc}" -eq 124 ]]; then
  report_test_timeout || true
fi
exit "${rc}"
