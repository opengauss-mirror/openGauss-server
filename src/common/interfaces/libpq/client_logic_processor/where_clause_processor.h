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
 * where_clause_processor.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\where_clause_processor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef WHERE_CLAUSE_PROCESSOR_H
#define WHERE_CLAUSE_PROCESSOR_H

#include <string>
#include "client_logic_cache/icached_columns.h"

enum class CE_UNSUPPORTED;
struct List;
typedef struct pg_conn PGconn;
typedef unsigned int Oid;

typedef struct StatementData StatementData;
typedef struct PGClientLogicParams PGClientLogicParams;
struct ExprParts;
class ExprPartsList;
class ICachedColumn;
class CStringsMap;
class WhereClauseProcessor {
public:
    static bool process(const ICachedColumns *cached_columns, const ExprPartsList *expr_parts_list,
        StatementData *statement_data, const CStringsMap* target_list = NULL);
};

#endif
