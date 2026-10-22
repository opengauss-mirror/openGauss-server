#!/usr/bin/env bash
set -euo pipefail

workspace="$PWD"
# A fork event must not choose the repository that supplies host scripts.
repo="${GITCODE_REPO:-opengauss/openGauss-server}"
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

resolve_script="${RESOLVE_SCRIPT:-.gitcode/scripts/resolve_gitcode_env.sh}"
if [[ ! -f "${resolve_script}" ]]; then
  echo "Cannot find ${resolve_script}" >&2
  exit 1
fi
# shellcheck disable=SC1090
source "${resolve_script}"

# Only branches listed here may supply the scripts that run on the runner.
# The list is the copy from master. A new branch name is not trusted until
# it is added here and merged to master.
target="${GITCODE_TARGET_BRANCH:-}"
target="${target#refs/heads/}"
pr_iid="${GITCODE_PR_IID:-}"
if [[ -n "${target}" ]]; then
  case "${target}" in
    *[!A-Za-z0-9._/-]*)
      echo "Invalid target branch: ${target}" >&2
      exit 1
      ;;
  esac
elif [[ -z "${pr_iid}" ]]; then
  target="master"
else
  echo "Pull request ${pr_iid} has no target branch; refuse to check out the source branch." >&2
  exit 1
fi

# hotfix is matched case-insensitively. Any branch that matches is allowed to
# supply the privileged host scripts, so those branches must be protected.
if [[ "${target}" == "master" || "${target}" =~ ^(2|3|5|6|7)\.[0-9]+\.[0-9]+$ || "${target}" =~ ^(2|3|5|6|7)\.[0-9]+\.[0-9]+-[Rr][Cc][0-9]+$ || "${target}" =~ ^(2|3|5|6|7)\.[0-9]+\.[0-9]+-[Hh][Oo][Tt][Ff][Ii][Xx][0-9]*$ ]]; then
  ref="refs/heads/${target}"
else
  echo "Target ${target} is not an allowed branch; runner uses master launchers."
  ref="refs/heads/master"
fi
echo "Manual checkout ${repo_url} ${ref} (container target ${target})"

mkdir -p "${workspace}"
cd "${workspace}"
# Only the CI job sets CI_REINIT_GIT, and only after entering its own directory.
# A shallow clone of another branch cannot accept a second depth-limited fetch
# on older git, so start this fetch from an empty repository.
if [[ "${CI_REINIT_GIT:-}" == "1" && -d .git ]]; then
  echo "Reinitializing the CI clone before fetching ${ref}."
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
timeout 300 git fetch --depth=1 --force origin "${ref}"
fetched="$(git rev-parse FETCH_HEAD)"
echo "Fetched ${ref} -> ${fetched}"
if [[ -z "${GITCODE_PR_IID}" ]]; then
  if [[ "${fetched}" != "${GITCODE_AFTER_COMMIT_SHA}" ]]; then
    echo "Commit ${GITCODE_AFTER_COMMIT_SHA:-empty} is not the tip of ${ref}; host and container use ${fetched}."
  fi
  GITCODE_AFTER_COMMIT_SHA="${fetched}"
  export GITCODE_AFTER_COMMIT_SHA
elif [[ -n "${GITCODE_AFTER_COMMIT_SHA}" && "${fetched}" != "${GITCODE_AFTER_COMMIT_SHA}" ]]; then
  echo "Commit ${GITCODE_AFTER_COMMIT_SHA} is not the tip of ${ref}; host scripts use ${fetched}."
fi
git checkout -f --detach "${fetched}"
git clean -ffdx

# Host scripts stay on the target branch. The container receives the merge
# commit through GITCODE_AFTER_COMMIT_SHA, which older launchers already pass.
# GitCode may advertise either the GitLab-style or the pull-style ref.
if [[ -n "${GITCODE_PR_IID}" ]]; then
  merge_sha=""
  for merge_ref in \
    "refs/merge-requests/${GITCODE_PR_IID}/merge" \
    "refs/pull/${GITCODE_PR_IID}/merge"
  do
    merge_dir="$(mktemp -d)"
    if git -C "${merge_dir}" init -q >/dev/null \
      && git -C "${merge_dir}" remote add origin "${repo_url}" >/dev/null \
      && timeout 300 git -C "${merge_dir}" fetch --depth=1 --force origin "${merge_ref}" >/dev/null \
      && merge_sha="$(git -C "${merge_dir}" rev-parse FETCH_HEAD)"; then
      rm -rf "${merge_dir}"
      GITCODE_MERGE_REF="${merge_ref}"
      GITCODE_AFTER_COMMIT_SHA="${merge_sha}"
      export GITCODE_MERGE_REF GITCODE_AFTER_COMMIT_SHA
      echo "Merge ref ${GITCODE_MERGE_REF} -> ${GITCODE_AFTER_COMMIT_SHA}"
      break
    fi
    rm -rf "${merge_dir}"
    merge_sha=""
  done
  if [[ -z "${merge_sha}" ]]; then
    echo "Cannot fetch refs/merge-requests/${GITCODE_PR_IID}/merge or refs/pull/${GITCODE_PR_IID}/merge; refuse to start the container without the merge commit." >&2
    exit 1
  fi
fi

case "${GITCODE_PR_IID}" in
  *[!0-9]*)
    echo "Invalid PR iid: ${GITCODE_PR_IID}" >&2
    exit 1
    ;;
esac
case "${GITCODE_AFTER_COMMIT_SHA}" in
  *[!0-9a-fA-F]*)
    echo "Invalid commit sha: ${GITCODE_AFTER_COMMIT_SHA}" >&2
    exit 1
    ;;
esac
export GITCODE_TARGET_BRANCH="${target}"

{
  printf 'GITCODE_PR_IID=%s\n' "${GITCODE_PR_IID}"
  printf 'GITCODE_TARGET_BRANCH=%s\n' "${GITCODE_TARGET_BRANCH}"
  printf 'GITCODE_AFTER_COMMIT_SHA=%s\n' "${GITCODE_AFTER_COMMIT_SHA}"
  printf 'GITCODE_MERGE_REF=%s\n' "${GITCODE_MERGE_REF}"
} > "${workspace}/.gitcode_ci_env"
env_file="${ATOMGIT_ENV:-${GITCODE_ENV:-}}"
if [[ -n "${env_file}" ]]; then
  cat "${workspace}/.gitcode_ci_env" >> "${env_file}"
fi
