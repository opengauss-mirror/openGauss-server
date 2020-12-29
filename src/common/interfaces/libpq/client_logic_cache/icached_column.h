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
 * icached_column.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\icached_column.h
 *
 * -------------------------------------------------------------------------
 */

/*
 * Data structure being used for saving column information in cache
 */
#ifndef ICACHED_COLUMN_H
#define ICACHED_COLUMN_H

#include "client_logic/client_logic_enums.h"
typedef unsigned int Oid;

class ColumnHookExecutorsList;
class ICachedColumn {
public:
    virtual ~ICachedColumn() = 0;

    /*
     * @return ordinal position of column in table
     */
    virtual unsigned int get_col_idx() const = 0;
    virtual const char *get_catalog_name() const = 0;
    virtual const char *get_schema_name() const = 0;
    virtual const char *get_table_name() const = 0;
    /*
     * @param  column name only, without table prefix
     */
    virtual void set_col_name(const char *col_name) = 0;
    virtual void set_catalog_name(const char* catalogname)= 0;
    virtual void set_schema_name(const char* schemaname) = 0;
    virtual void set_table_name(const char* tablename) = 0;

    virtual const char *get_col_name() const = 0;
    virtual const Oid get_table_oid() const = 0;

    virtual ColumnHookExecutorsList *get_column_hook_executors() const = 0;
    virtual void set_origdatatype_oid(Oid) = 0;
    virtual Oid get_origdatatype_oid() const = 0;
    virtual int get_origdatatype_mod() const = 0;
    virtual void set_data_type(Oid) = 0;
    virtual Oid get_data_type() const = 0;
    virtual void set_use_in_prepare(bool) = 0;
    virtual const bool is_in_prepare() const = 0;
    virtual const bool is_in_columns_list() const = 0;
};

inline ICachedColumn::~ICachedColumn() {};

typedef const ICachedColumn *ICachedColumnSptr;

#endif