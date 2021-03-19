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
 * ---------------------------------------------------------------------------------------
 * 
 * dblink_query.h
 *     Functions returning results from a remote database
 * 
 * 
 * IDENTIFICATION
 *        src/include/instruments/dblink_query.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DBLINK_QUERY_H
#define DBLINK_QUERY_H

#include "fmgr.h"

extern Datum wdr_xdb_query(PG_FUNCTION_ARGS);

extern void dblinkCloseConn(void);

extern void dblinkRequestCancel(void);

#endif /* DBLINK_QUERY_H */
