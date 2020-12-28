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
 * table_full_name.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_common\table_full_name.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#include "table_full_name.h"
#include "libpq-int.h"
#include "securec.h"

tableFullName::tableFullName() {}

tableFullName::~tableFullName() {}

tableFullName::tableFullName(const char *catalog_name, const char *schema_name, const char *table_name)
{
    if (catalog_name) {
        check_strncpy_s(
            strncpy_s(m_catalog_name.data, sizeof(m_catalog_name.data), catalog_name, strlen(catalog_name)));
    }

    if (schema_name) {
        check_strncpy_s(strncpy_s(m_schema_name.data, sizeof(m_schema_name.data), schema_name, strlen(schema_name)));
    }

    if (table_name) {
        check_strncpy_s(strncpy_s(m_table_name.data, sizeof(m_table_name.data), table_name, strlen(table_name)));
    }
}

const char *tableFullName::get_catalog_name() const
{
    return m_catalog_name.data;
}

void tableFullName::set_catalog_name(const char *catalog_name)
{
    if (catalog_name) {
        check_strncpy_s(
            strncpy_s(m_catalog_name.data, sizeof(m_catalog_name.data), catalog_name, strlen(catalog_name)));
    }
}

const char *tableFullName::get_schema_name() const
{
    return m_schema_name.data;
}

void tableFullName::set_schema_name(const char *schema_name)
{
    if (schema_name) {
        check_strncpy_s(strncpy_s(m_schema_name.data, sizeof(m_schema_name.data), schema_name, strlen(schema_name)));
    }
}

const char *tableFullName::get_table_name() const
{
    return m_table_name.data;
}

void tableFullName::set_table_name(const char *table_name)
{
    if (table_name) {
        check_strncpy_s(strncpy_s(m_table_name.data, sizeof(m_table_name.data), table_name, strlen(table_name)));
    }
}
