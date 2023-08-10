/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * tool_common.cpp
 *
 * IDENTIFICATION
 *        src/common/port/tool_common.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "securec.h"
#include "securec_check.h"
#include "tool_common.h"

SSInstanceConfig ss_instance_config = {
    .dss = {
        .enable_dss = false,
        .instance_id = -1,
        .primaryInstId = -1,
        .vgname = NULL,
        .vglog = NULL,
        .vgdata = NULL,
        .socketpath = NULL,
    },
};

datadir_t g_datadir;  /* need init when used in first time */

static void initFileDataPathStruct(datadir_t *dataDir);
static void initDSSDataPathStruct(datadir_t *dataDir);

void initDataPathStruct(bool enable_dss)
{
    if (enable_dss) {
        initDSSDataPathStruct(&g_datadir);
    } else {
        initFileDataPathStruct(&g_datadir);
    }
}

static void initFileDataPathStruct(datadir_t *dataDir)
{
    errno_t rc = EOK;

    // dir path
    rc = snprintf_s(dataDir->baseDir, MAXPGPATH, MAXPGPATH - 1, "%s/base", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->globalDir, MAXPGPATH, MAXPGPATH - 1, "%s/global", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->locationDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_location", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->tblspcDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_tblspc", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->clogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_clog", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->csnlogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_csnlog", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->notifyDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_notify", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->serialDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_serial", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->snapshotsDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_snapshots", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->twophaseDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_twophase", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->multixactDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_multixact", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->xlogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    // sub-file path
    rc = snprintf_s(dataDir->controlPath, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_control", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->controlBakPath, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_control.backup", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwOldPath, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_dw", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwPathPrefix, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_dw_", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwSinglePath, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_dw_single", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwBuildPath, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_dw.build", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwMetaPath, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_dw_meta", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwUpgradePath, MAXPGPATH, MAXPGPATH - 1, "%s/global/dw_upgrade", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwBatchUpgradeMetaPath, MAXPGPATH, MAXPGPATH - 1, "%s/global/dw_batch_upgrade_meta",
        dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwBatchUpgradeFilePath, MAXPGPATH, MAXPGPATH - 1, "%s/global/dw_batch_upgrade_files",
        dataDir->pg_data);
    securec_check_ss_c(rc, "", "");
}

static void initDSSDataPathStruct(datadir_t *dataDir)
{
    errno_t rc = EOK;

    // DSS file directory (cluster owner)
    rc = snprintf_s(dataDir->baseDir, MAXPGPATH, MAXPGPATH - 1, "%s/base", dataDir->dss_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->globalDir, MAXPGPATH, MAXPGPATH - 1, "%s/global", dataDir->dss_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->locationDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_location", dataDir->dss_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->tblspcDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_tblspc", dataDir->dss_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->controlPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_control", dataDir->dss_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->controlBakPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_control.backup", dataDir->dss_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->controlInfoPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_replication/pg_ss_ctl_info", dataDir->dss_data);
    securec_check_ss_c(rc, "", "");

    // DSS file directory (instance owner)
    rc = snprintf_s(dataDir->clogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_clog%d", dataDir->dss_data,
        dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->csnlogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_csnlog%d", dataDir->dss_data,
        dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->serialDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_serial%d", dataDir->dss_data,
        dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->snapshotsDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_snapshots%d", dataDir->dss_data,
        dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->twophaseDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_twophase%d", dataDir->dss_data,
        dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->multixactDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_multixact%d", dataDir->dss_data,
        dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->xlogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog%d", dataDir->dss_data,
        dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    // Unix file directory (instance owner)
    rc = snprintf_s(dataDir->dwDir.dwOldPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_doublewrite%d/pg_dw",
        dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwPathPrefix, MAXPGPATH, MAXPGPATH - 1, "%s/pg_doublewrite%d/pg_dw_",
        dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwSinglePath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_doublewrite%d/pg_dw_single",
        dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwBuildPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_doublewrite%d/pg_dw.build",
        dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwUpgradePath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_doublewrite%d/dw_upgrade",
        dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwMetaPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_doublewrite%d/pg_dw_meta",
        dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwBatchUpgradeMetaPath, MAXPGPATH, MAXPGPATH - 1,
        "%s/pg_doublewrite%d/dw_batch_upgrade_meta", dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwBatchUpgradeFilePath, MAXPGPATH, MAXPGPATH - 1,
        "%s/pg_doublewrite%d/dw_batch_upgrade_files", dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");
}
