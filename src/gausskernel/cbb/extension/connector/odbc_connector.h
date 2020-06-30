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
 * odbc_connector.h
 *
 * IDENTIFICATION
 *     src/gausskernel/cbb/extension/connector/odbc_connector.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef ODBC_CONNECTOR_H
#define ODBC_CONNECTOR_H

#include "fmgr.h"
#include "funcapi.h"

extern "C" void connect_odbc(
    const char* dsn, const char* user, const char* pass, FuncCallContext* funcctx, FunctionCallInfo fcinfo);
extern "C" void exec_odbc(const char* dsn, const char* sql, const char* encoding);
extern "C" Datum fetch_odbc(const char* dsn, FuncCallContext* funcctx, bool& isEnd, const char* encoding);
extern "C" void end_odbc(const char* dsn);

#endif /* ODBC_CONNECTOR_H */
