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
 * cached_column.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cached_column.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <iostream>
#include "cached_column.h"
#include "libpq-int.h"
#include "client_logic_common/col_full_name.h"
#include "column_hook_executors_list.h"
#include "client_logic_hooks/column_hook_executor.h"

/*
 * build a new cachecolumn from const cacheColumn,but the new can change its name. it is necessary for alias name and
 * cte.
 */
CachedColumn::CachedColumn(const ICachedColumn *source)
{
    init(source);
}

void CachedColumn::init(const ICachedColumn *source)
{
    if (source == NULL) {
        printf("Invalid cached column source.\n");
        exit(EXIT_FAILURE);
    }
    m_own_oid = 0;
    m_in_columns_list = false;
    m_table_oid = source->get_table_oid();
    m_column_index = source->get_col_idx();
    m_data_type = source->get_data_type();
    m_data_type_original_oid = source->get_origdatatype_oid();
    m_data_type_original_mod = source->get_origdatatype_mod();
    m_col_full_name = new(std::nothrow) colFullName();
    if (m_col_full_name == NULL) {
        printf("out of memory\n");
        exit(EXIT_FAILURE);
    }
    m_col_full_name->set_catalog_name(source->get_catalog_name());
    m_col_full_name->set_schema_name(source->get_schema_name());
    m_col_full_name->set_table_name(source->get_table_name());
    m_col_full_name->set_col_name(source->get_col_name());
    m_column_hook_executors_list = new(std::nothrow) ColumnHookExecutorsList();
    if (m_column_hook_executors_list == NULL) {
        printf("out of memory\n");
        exit(EXIT_FAILURE);
    }
    for (size_t i = 0; i < source->get_column_hook_executors()->size(); ++i) {
        m_column_hook_executors_list->add(source->get_column_hook_executors()->at(i));
    }
    in_use_in_prepare = 0;
}

/*
 * according to the atrributes building a CacheColumn
 * it's useful to get CacheColumn in CacheLoader
 */
CachedColumn::CachedColumn(Oid oid, Oid table_oid, const char* database_name, const char* schema_name,
    const char* table_name, const char* column_name, int column_position, Oid data_type_oid, int data_type_mod)
    : m_column_hook_executors_list(new (std::nothrow) ColumnHookExecutorsList),
      m_own_oid(oid),
      m_table_oid(table_oid),
      m_column_index(column_position),
      m_data_type(0),
      m_data_type_original_oid(data_type_oid),
      m_data_type_original_mod(data_type_mod),
      m_col_full_name(new(std::nothrow) colFullName),
      m_in_columns_list(false)
{
    init();
    if (m_col_full_name != NULL) {
        m_col_full_name->set_catalog_name(database_name);
        m_col_full_name->set_schema_name(schema_name);
        m_col_full_name->set_table_name(table_name);
        m_col_full_name->set_col_name(column_name);
    }
}

/*
 * Initialize a CacheColumn
 * with no specific value
 */
CachedColumn::CachedColumn()
    : m_column_hook_executors_list(NULL),
      m_own_oid(0),
      m_table_oid(0),
      m_column_index(0),
      m_data_type(0),
      m_data_type_original_oid(0),
      m_data_type_original_mod(-1),
      m_col_full_name(NULL),
      m_in_columns_list(false)
{
    init();
}
void CachedColumn::init()
{
    if (m_col_full_name == NULL) {
        m_col_full_name = new(std::nothrow) colFullName();
        if (m_col_full_name == NULL) {
            printf("out of memory\n");
            exit(EXIT_FAILURE);
        }
    }
    if (m_column_hook_executors_list == NULL) {
        m_column_hook_executors_list = new(std::nothrow) ColumnHookExecutorsList();
        if (m_column_hook_executors_list == NULL) {
            printf("out of memory\n");
            exit(EXIT_FAILURE);
        }
    }
    in_use_in_prepare = 0;
}

/* delete a CacheColumn */
CachedColumn::~CachedColumn()
{
    delete m_col_full_name;
    delete m_column_hook_executors_list;
}

unsigned int CachedColumn::get_col_idx() const
{
    return m_column_index;
}

const char *CachedColumn::get_catalog_name() const
{
    return m_col_full_name->get_catalog_name();
}

void CachedColumn::set_catalog_name(const char *catalogname)
{
    m_col_full_name->set_catalog_name(catalogname);
}

const char *CachedColumn::get_col_name() const
{
    return m_col_full_name->get_col_name();
}

void CachedColumn::set_col_name(const char *colname)
{
    m_col_full_name->set_col_name(colname);
}

const char *CachedColumn::get_schema_name() const
{
    return m_col_full_name->get_schema_name();
}

void CachedColumn::set_schema_name(const char *schemaname)
{
    m_col_full_name->set_schema_name(schemaname);
}

const char *CachedColumn::get_table_name() const
{
    return m_col_full_name->get_table_name();
}

void CachedColumn::set_table_name(const char *tablename)
{
    m_col_full_name->set_table_name(tablename);
}

void CachedColumn::add_executor(ColumnHookExecutor *executor)
{
    m_column_hook_executors_list->add(executor);
}

ColumnHookExecutorsList *CachedColumn::get_column_hook_executors() const
{
    return m_column_hook_executors_list;
}

void CachedColumn::set_table_oid(Oid table_oid)
{
    m_table_oid = table_oid;
}

const Oid CachedColumn::get_table_oid() const
{
    return m_table_oid;
}

void CachedColumn::set_origdatatype_oid(Oid data_type_original_oid)
{
    m_data_type_original_oid = data_type_original_oid;
}

Oid CachedColumn::get_origdatatype_oid() const
{
    return m_data_type_original_oid;
}

int CachedColumn::get_origdatatype_mod() const
{
    return m_data_type_original_mod;
}

void CachedColumn::set_data_type(Oid data_type)
{
    m_data_type = data_type;
}

Oid CachedColumn::get_data_type() const
{
    return m_data_type;
}

void CachedColumn::set_use_in_prepare(bool in_prepare)
{
    Assert(in_use_in_prepare || in_prepare);
    in_use_in_prepare += in_prepare ? 1 : -1;
    for (size_t i = 0; i < m_column_hook_executors_list->size(); ++i) {
        if (in_prepare) {
            m_column_hook_executors_list->at(i)->inc_ref_count();
        } else {
            m_column_hook_executors_list->at(i)->dec_ref_count();
        }
    }
}

const bool CachedColumn::is_in_prepare() const
{
    return in_use_in_prepare;
}
const bool CachedColumn::is_in_columns_list() const
{
    return m_in_columns_list;
}
void CachedColumn::remove_from_columnslist()
{
    m_in_columns_list = false;
}
void CachedColumn::set_flag_columnslist()
{
    m_in_columns_list = true;
}
Oid CachedColumn::get_oid() const
{
    return m_own_oid;
}
void CachedColumn::set_oid(Oid oid)
{
    m_own_oid = oid;
}
