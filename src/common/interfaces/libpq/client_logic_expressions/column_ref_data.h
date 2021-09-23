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
 * column_ref_data.h
 *
 * IDENTIFICATION
 * 	  src\common\interfaces\libpq\client_logic_expressions\column_ref_data.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef COLUMN_REF_DATA_H
#define COLUMN_REF_DATA_H
#include "libpq-int.h"
#include "c.h"
#include "client_logic_cache/icached_column.h"
class ICachedColumn;
class ColumnRefData {
public:
    ColumnRefData();
    ~ColumnRefData() {}
    NameData m_catalog_name;
    NameData m_schema_name;
    NameData m_table_name;
    NameData m_column_name;
    char m_alias_fqdn[NAMEDATALEN * 4]; /* this is usually col fqdn */
    bool compare_with_cachecolumn(const ICachedColumn *cache_column, const char *coname_inquery = NULL) const;
};
#endif
