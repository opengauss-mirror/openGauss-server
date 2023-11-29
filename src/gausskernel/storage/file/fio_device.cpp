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
 * fio_device.cpp
 *  Storage Adapter Interface.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/file/fio_device.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <errno.h>
#include <stdio.h>
#include "c.h"
#include "storage/file/fio_device.h"

g_dss_io_stat g_dss_io_stat_var;
device_type_t fio_device_type(const char *name)
{
    if (is_dss_file(name)) {
        return DEV_TYPE_DSS;
    }

    return DEV_TYPE_FILE;
}

bool is_dss_type(device_type_t type)
{
    return type == DEV_TYPE_DSS;
}

bool is_file_exist(int err)
{
    return (err == EEXIST || err == ERR_DSS_DIR_CREATE_DUPLICATED);
}

bool is_file_delete(int err)
{
    return (err == ENOENT || err == ERR_DSS_FILE_NOT_EXIST);
}