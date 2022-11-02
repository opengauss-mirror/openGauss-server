#!/bin/bash
GO_DRIVRR_DIR=$work_dir/src/gitee.com/opengauss
GO_DRIVRR_PATH=$GO_DRIVRR_DIR/openGauss-connector-go-pq
DEVELOP_BRANCH=huawei/csi/gaussdb_kernel/v1.0.2
REPOSITORY=https://codehub-dg-y.huawei.com/OpenSourceCenter/openGauss-connector-go-pq.git

#prepare code
if [ ! -d ${GO_DRIVRR_PATH} ]; then
    git config --global http.sslVerify false
    mkdir -p $GO_DRIVRR_DIR
    cd $GO_DRIVRR_DIR
    git clone $REPOSITORY
    cd $GO_DRIVRR_PATH
    git reset --hard remotes/origin/${DEVELOP_BRANCH}
else
    cd $GO_DRIVRR_PATH
    git remote set-url origin $REPOSITORY
    git remote update origin
    git reset --hard remotes/origin/${DEVELOP_BRANCH}
fi