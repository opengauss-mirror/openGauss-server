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
 * connector.h
 *        Definitions for c-function interface of the module
 * 
 * 
 * IDENTIFICATION
 *        src/include/connector.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CONNECTOR_H
#define CONNECTOR_H

#include "postgres.h"
#include "knl/knl_variable.h"

/*
 * External declarations
 */
extern Datum exec_hadoop_sql(PG_FUNCTION_ARGS);
extern Datum exec_on_extension(PG_FUNCTION_ARGS);

enum EC_Status { EC_STATUS_INIT = 0, EC_STATUS_CONNECTED, EC_STATUS_EXECUTED, EC_STATUS_FETCHING, EC_STATUS_END };

#define NOT_EC_OPERATOR 0
#define IS_EC_OPERATOR 1
#define EC_LIBODBC_TYPE_ONE 1 /* libodbc.so.1 */
#define EC_LIBODBC_TYPE_TWO 2 /* libodbc.so.2 */

#endif /* CONNECTOR_H */
