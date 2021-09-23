/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * column_ref_data.cpp
 *
 * IDENTIFICATION
 * 	  src\common\interfaces\libpq\client_logic_expressions\column_ref_data.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "column_ref_data.h"
#include <cstdio>
#include "client_logic_cache/icached_column.h"
/*
get the column's full FQDN, as full as possible, depending on the amount of information in the ColumnRef
column FQDN: "database"."schema"."table"."column"
*/

ColumnRefData::ColumnRefData()
{
    m_catalog_name.data[0] = '\0';
    m_schema_name.data[0] = '\0';
    m_table_name.data[0] = '\0';
    m_column_name.data[0] = '\0';
    m_alias_fqdn[0] = '\0';
}

/*
 * judge Cachecolumn by columnrefdata should include colname, tablename, schemaname and ...
 */
bool ColumnRefData::compare_with_cachecolumn(const ICachedColumn *cached_column, const char *colname_inquery) const
{
    if (cached_column == NULL) {
        return false;
    } else {
        if (cached_column->get_col_name() == NULL || cached_column->get_table_name() == NULL ||
            cached_column->get_schema_name() == NULL || cached_column->get_catalog_name() == NULL) {
            return false;
        }
        if (colname_inquery) {
            if (m_column_name.data[0] != '\0' && pg_strcasecmp(cached_column->get_col_name(), colname_inquery) != 0) {
                return false;
            }
        } else if (m_column_name.data[0] != '\0' &&
            pg_strcasecmp(cached_column->get_col_name(), m_column_name.data) != 0) {
            return false;
        }
        if (m_table_name.data[0] != '\0' && pg_strcasecmp(cached_column->get_table_name(), m_table_name.data) != 0) {
            return false;
        }
        if (m_schema_name.data[0] != '\0' && pg_strcasecmp(cached_column->get_schema_name(), m_schema_name.data) != 0) {
            return false;
        }
        if (m_catalog_name.data[0] != '\0' &&
            pg_strcasecmp(cached_column->get_catalog_name(), m_catalog_name.data) != 0) {
            return false;
        }
    }
    return true;
}
