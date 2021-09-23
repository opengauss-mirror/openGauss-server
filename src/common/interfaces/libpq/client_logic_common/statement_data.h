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
 * statement_data.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_common\statement_data.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef STATEMENT_DATA_H
#define STATEMENT_DATA_H

#include "libpq-int.h"
#include "client_logic_processor/raw_value.h"
#include "client_logic_processor/raw_values_list.h"
#include "client_logic_common/pg_client_logic_params.h"
#include "client_logic_cache/icached_column_manager.h"

typedef struct StatementData {
public:
    StatementData(PGconn *aconn, const char *aquery)
        : conn(aconn),
          query(aquery),
          stmtName(NULL),
          nParams(0),
          paramTypes(0),
          paramValues(0),
          paramLengths(0),
          paramFormats(0),
          offset(0) {};

    StatementData(PGconn *aconn, const char *astmt_name, const char *aquery)
        : conn(aconn),
          query(aquery),
          stmtName(astmt_name),
          nParams(0),
          paramTypes(0),
          paramValues(0),
          paramLengths(0),
          paramFormats(0),
          offset(0) {};

    StatementData(PGconn *aconn, const char *astmt_name, const size_t an_params, const Oid *aparam_types,
        const char * const * aparam_values, const int *aparam_lengths, const int *aparam_formats)
        : conn(aconn),
          query(NULL),
          stmtName(astmt_name),
          nParams(an_params),
          paramTypes(aparam_types),
          paramValues(aparam_values),
          paramLengths(aparam_lengths),
          paramFormats(aparam_formats),
          offset(0) {};

    StatementData(PGconn *aconn, const char *astmt_name, const char *aquery, const size_t an_params,
        const Oid *aparam_types, const char * const * aparam_values, const int *aparam_lengths,
        const int *aparam_formats)
        : conn(aconn),
          query(aquery),
          stmtName(astmt_name),
          nParams(an_params),
          paramTypes(aparam_types),
          paramValues(aparam_values),
          paramLengths(aparam_lengths),
          paramFormats(aparam_formats),
          offset(0) {};

    void replace_raw_values();
    /*
     *  @brief copy all params from statment data to params structre and allocating memory for adjusted
     */
    void copy_params();
    ICachedColumnManager* GetCacheManager() const;
    ~StatementData();

private:
    const size_t get_total_change() const;

public:
    PGconn *conn;
    const char *query;
    const char *stmtName;
    size_t nParams;
    const Oid *paramTypes;
    const char * const * paramValues;
    const int *paramLengths;
    const int *paramFormats;
    PGClientLogicParams params;
    size_t offset;
} StatementData;

#endif