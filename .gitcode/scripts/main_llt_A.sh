#!/bin/bash
set -euo pipefail

docker_name=opengauss-llt-A-1.0.1
docker_image_name=opengauss-llt:1.0.1
dockerfile=dockerfile_llt
part_type=A
## build docker images
# git clone
# docker build -t ${docker_image_name} -f ./${dockerfile} .

echo "gitcodePullRequestIid: ${GITCODE_PR_IID:-}"
echo "gitcodeAfterCommitSha: ${GITCODE_AFTER_COMMIT_SHA:-}"
echo "gitcodeRef: ${GITCODE_MERGE_REF:-}"

if [ "$(docker ps | grep ${docker_name})" != "" ]; then
	docker kill $docker_name
fi

if [ "$(docker ps -a | grep ${docker_name})" != "" ]; then
    docker rm $docker_name
fi

sync && echo 3>/proc/sys/vm/drop_caches
ipcrm -a

gitcodeTargetBranch="${GITCODE_TARGET_BRANCH:-master}"
## A 测试套不添加 --network=host， 用例forbid_ddl获取ip在容器内外会有差异。

containerid=`docker run --privileged=true -d -it -P  -v /etc/localtime:/etc/localtime \
        -e PKG_TYPE=DEBUG -e gitcodePullRequestIid=${GITCODE_PR_IID} \
        -e gitcodeAfterCommitSha=${GITCODE_AFTER_COMMIT_SHA} -e gitcodeRef=${GITCODE_MERGE_REF:-} -e PART_TYPE=${part_type} \
        -e gitcodeTargetBranch=${gitcodeTargetBranch} --name ${docker_name} ${docker_image_name}`

[ -n "${containerid}" ] || { echo "docker run 失败"; exit 1; }

docker logs -f "${containerid}" || true

# A running container reports exit code 0. Require the exited state.
status="$(docker inspect --format '{{.State.Status}}' "${containerid}" 2>/dev/null || true)"
exit_code="$(docker inspect --format '{{.State.ExitCode}}' "${containerid}" 2>/dev/null || true)"
oom="$(docker inspect --format '{{.State.OOMKilled}}' "${containerid}" 2>/dev/null || true)"
echo "状态=${status:-unknown} 退出码=${exit_code:-unknown} OOMKilled=${oom:-unknown}"

if [ "${status}" = "exited" ] && [ "${exit_code}" = "0" ] && [ "${oom}" = "false" ]; then
    echo "容器正常退出"
    docker rm -f "${containerid}" >/dev/null 2>&1 || true
else
    echo "容器异常退出 status=${status:-unknown} exit=${exit_code:-unknown}"
    docker rm -f "${containerid}" >/dev/null 2>&1 || true
    exit 1
fi
