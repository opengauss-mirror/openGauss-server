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
 * columns_list.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\columns_list.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef COLUMNS_LIST_H
#define COLUMNS_LIST_H

#include "c.h"

typedef unsigned int Oid;

class CachedColumn;

class ColumnsList {
public:
    ColumnsList();
    ~ColumnsList();
    void clear();
    bool add(CachedColumn *cached_column);
    const CachedColumn *get_by_oid(Oid table_oid, unsigned int column_position) const;
    const CachedColumn* get_by_oid(Oid oid) const;
    const CachedColumn *get_by_details(const char *database_name, const char *schema_name, const char *table_name,
        const char *column_name) const;
    const CachedColumn **get_by_details(const char *database_name, const char *schema_name, const char *table_name,
        size_t &columns_list_size) const;
    const CachedColumn **get_by_schema(const char *schema_name, size_t &columns_list_size) const;
    bool empty() const;

private:
    CachedColumn **m_columns_list;
    size_t m_columns_list_size;
};

#endif