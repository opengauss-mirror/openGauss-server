/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * -------------------------------------------------------------------------
 *
 * mot_fdw_error.h
 *    MOT Foreign Data Wrapper error reporting interfaces.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/mot_fdw_error.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_FDW_ERROR_H
#define MOT_FDW_ERROR_H

#include "global.h"

typedef struct tagMotErrToPGErrSt {
    int m_pgErr;
    const char* m_msg;
    const char* m_detail;
} MotErrToPGErrSt;

void report_pg_error(MOT::RC rc, void* arg1 = nullptr, void* arg2 = nullptr, void* arg3 = nullptr, void* arg4 = nullptr,
    void* arg5 = nullptr);

#endif /* MOT_FDW_ERROR_H */
