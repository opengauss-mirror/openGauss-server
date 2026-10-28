#!/usr/bin/env bash

###############################################################
##  sh gitcode_pr_lables.sh add ci-pipeline-success         ###
###############################################################
set -euo pipefail

server_repo=openGauss-server

unset ATOMGIT_TOKEN || true
unset GITHUB_TOKEN || true
token="${PR_LABEL_TOKEN:-}"
pr_iid="${GITCODE_PR_IID:-}"
action="${1:-}"
pr_label="${2}"

if [[ -z "${action}" ]]; then
  echo "Usage: $0 <add|delete>"
  exit 2
fi
if [[ -z "${pr_label}" ]]; then
  echo "Usage: $0 <ci-pipeline-success|ci-pipeline-failed|ci-pipeline-running>"
  exit 2
fi

if [ "${action}" == "add" ]; then
  curl -X POST "https://gitcode.com/api/v5/repos/opengauss/${server_repo}/pulls/${pr_iid}/labels" -H "Authorization: Bearer ${token}" -H "Content-Type: application/json;charset=UTF-8" -d '["'"${pr_label}"'"]'
fi

if [ "${action}" == "delete" ]; then
  curl -X DELETE "https://gitcode.com/api/v5/repos/opengauss/${server_repo}/pulls/${pr_iid}/labels/${pr_label}" -H "Authorization: Bearer ${token}" -H 'Content-Type: application/json;charset=UTF-8'
fi
