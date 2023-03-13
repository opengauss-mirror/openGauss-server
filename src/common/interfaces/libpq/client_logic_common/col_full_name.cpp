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
 * col_full_name.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_common\col_full_name.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "col_full_name.h"
#include "libpq-int.h"
#include "securec.h"

colFullName::colFullName()
{
    m_col_name = {{0}};
}

colFullName::colFullName(const char *catalog_name, const char *schema_name, const char *rel_name, const char *colname)
    : tableFullName(catalog_name, schema_name, rel_name)
{
    const char *column_name = (colname != NULL) ? colname : "";
    check_strncpy_s(strncpy_s(m_col_name.data, sizeof(m_col_name.data), column_name, strlen(column_name)));
}

colFullName::~colFullName() {}

const char *colFullName::get_col_name() const
{
    return m_col_name.data;
}

void colFullName::set_col_name(const char *colname)
{
    check_strncpy_s(strncpy_s(m_col_name.data, sizeof(m_col_name.data), colname, strlen(colname)));
}
