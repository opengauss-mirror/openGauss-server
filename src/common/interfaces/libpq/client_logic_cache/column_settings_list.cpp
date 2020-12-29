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
 * column_settings_list.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\column_settings_list.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "column_settings_list.h"
#include "cached_column_setting.h"
#include "libpq-int.h"

ColumnSettingsList::ColumnSettingsList() : m_column_settings_list(NULL), m_column_settings_list_size(0) {}

ColumnSettingsList::~ColumnSettingsList()
{
    clear();
}
void ColumnSettingsList::clear()
{
    for (size_t i = 0; i < m_column_settings_list_size; ++i) {
        delete m_column_settings_list[i];
        m_column_settings_list[i] = NULL;
    }
    free(m_column_settings_list);
    m_column_settings_list = NULL;
    m_column_settings_list_size = 0;
}


bool ColumnSettingsList::add(CachedColumnSetting *cached_column_setting)
{
    m_column_settings_list = (CachedColumnSetting **)libpq_realloc(m_column_settings_list,
        sizeof(*m_column_settings_list) * m_column_settings_list_size,
        sizeof(*m_column_settings_list) * (m_column_settings_list_size + 1));
    if (m_column_settings_list == NULL) {
        return false;
    }
    m_column_settings_list[m_column_settings_list_size] = cached_column_setting;
    ++m_column_settings_list_size;
    return true;
}

const CachedColumnSetting *ColumnSettingsList::get_by_oid(Oid oid) const
{
    for (size_t i = 0; i < m_column_settings_list_size; ++i) {
        if (m_column_settings_list[i]->get_oid() == oid) {
            return m_column_settings_list[i];
        }
    }
    return NULL;
}

const CachedColumnSetting *ColumnSettingsList::get_by_fqdn(const char *fqdn) const
{
    for (size_t i = 0; i < m_column_settings_list_size; ++i) {
        if (pg_strcasecmp(m_column_settings_list[i]->get_fqdn(), fqdn) == 0) {
            return m_column_settings_list[i];
        }
    }
    return NULL;
}

const CachedColumnSetting **ColumnSettingsList::get_by_schema(const char *schema_name,
    size_t &column_settings_list_size) const
{
    const CachedColumnSetting **column_settings_list(NULL);
    column_settings_list_size = 0;
    for (size_t i = 0; i < m_column_settings_list_size; ++i) {
        if (pg_strcasecmp(m_column_settings_list[i]->get_schema_name(), schema_name) == 0) {
            column_settings_list = (const CachedColumnSetting **)libpq_realloc(column_settings_list,
                sizeof(*column_settings_list) * column_settings_list_size,
                sizeof(*column_settings_list) * (column_settings_list_size + 1));
            if (column_settings_list == NULL) {
                return NULL;
            }
            column_settings_list[column_settings_list_size] = m_column_settings_list[i];
            ++column_settings_list_size;
        }
    }
    return column_settings_list;
}


const CachedColumnSetting **ColumnSettingsList::get_by_global_setting(const char *global_setting,
    size_t &column_settings_list_size) const
{
    if (!global_setting) {
        return NULL;
    }
    const CachedColumnSetting **column_settings_list(NULL);
    column_settings_list_size = 0;
    for (size_t i = 0; i < m_column_settings_list_size; ++i) {
        if (pg_strcasecmp(m_column_settings_list[i]->m_cached_global_setting, global_setting) == 0) {
            column_settings_list = (const CachedColumnSetting **)libpq_realloc(column_settings_list,
                sizeof(*column_settings_list) * column_settings_list_size,
                sizeof(*column_settings_list) * (column_settings_list_size + 1));
            if (!column_settings_list) {
                return NULL;
            }
            column_settings_list[column_settings_list_size] = m_column_settings_list[i];
            ++column_settings_list_size;
        }
    }
    return column_settings_list;
}


bool ColumnSettingsList::empty() const
{
    return (m_column_settings_list_size == 0);
}
