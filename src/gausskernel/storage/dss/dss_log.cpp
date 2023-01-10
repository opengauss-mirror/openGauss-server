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
 * dss_log.cpp
 *  write log for dss
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/dss/dss_log.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "utils/palloc.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "knl/knl_thread.h"
#include "storage/dss/dss_api_def.h"
#include "storage/dss/dss_log.h"
#include "storage/dss/dss_adaptor.h"
#include "ddes/dms/ss_init.h"
#include "knl/knl_instance.h"

void DSSInitLogger()
{
    knl_instance_attr_dms *dms_attr = &g_instance.attr.attr_storage.dms_attr;
    logger_param_t log_param;
    log_param.log_level = (unsigned int)(dms_attr->sslog_level);
    log_param.log_backup_file_count = (unsigned int)(dms_attr->sslog_backup_file_count);
    log_param.log_max_file_size = ((uint64)(dms_attr->sslog_max_file_size)) * 1024;

    GetSSLogPath(log_param.log_home);
    int ret = dss_call_init_logger(log_param.log_home, log_param.log_level, log_param.log_backup_file_count, log_param.log_max_file_size);
    if (ret != DSS_SUCCESS) {
        ereport(FATAL,(errmsg("failed to init dss looger")));
    }
}

void DSSRefreshLogger(char *log_field, unsigned long long *value)
{
    dss_call_refresh_logger(log_field, value);
}