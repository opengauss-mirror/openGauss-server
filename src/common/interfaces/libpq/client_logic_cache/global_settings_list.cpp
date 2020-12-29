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
 * global_settings_list.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\global_settings_list.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "global_settings_list.h"
#include "cached_global_setting.h"
#include "libpq-int.h"

GlobalSettingsList::GlobalSettingsList() : m_global_settings_list(NULL), m_global_settings_list_size(0) {}

GlobalSettingsList::~GlobalSettingsList() {}

void GlobalSettingsList::clear()
{
    for (size_t i = 0; i < m_global_settings_list_size; ++i) {
        delete m_global_settings_list[i];
        m_global_settings_list[i] = NULL;
    }
    free(m_global_settings_list);
    m_global_settings_list = NULL;
    m_global_settings_list_size = 0;
}


bool GlobalSettingsList::add(CachedGlobalSetting *cached_global_setting)
{
    m_global_settings_list = (CachedGlobalSetting **)libpq_realloc(m_global_settings_list,
        sizeof(*m_global_settings_list) * m_global_settings_list_size,
        sizeof(*m_global_settings_list) * (m_global_settings_list_size + 1));
    if (m_global_settings_list == NULL) {
        return false;
    }
    m_global_settings_list[m_global_settings_list_size] = cached_global_setting;
    ++m_global_settings_list_size;
    return true;
}

const CachedGlobalSetting *GlobalSettingsList::get_by_oid(Oid oid) const
{
    for (size_t i = 0; i < m_global_settings_list_size; ++i) {
        if (m_global_settings_list[i]->get_oid() == oid) {
            return m_global_settings_list[i];
        }
    }
    return NULL;
}

const CachedGlobalSetting *GlobalSettingsList::get_by_fqdn(const char *fqdn) const
{
    for (size_t i = 0; i < m_global_settings_list_size; ++i) {
        if (pg_strcasecmp(m_global_settings_list[i]->get_fqdn(), fqdn) == 0) {
            return m_global_settings_list[i];
        }
    }
    return NULL;
}

const CachedGlobalSetting *GlobalSettingsList::get_by_details(const char *database_name, const char *schema_name,
    const char *object_name) const
{
    for (size_t i = 0; i < m_global_settings_list_size; ++i) {
        if ((pg_strcasecmp(m_global_settings_list[i]->get_database_name(), database_name) == 0) &&
            (pg_strcasecmp(m_global_settings_list[i]->get_schema_name(), schema_name) == 0) &&
            (pg_strcasecmp(m_global_settings_list[i]->get_object_name(), object_name) == 0)) {
            return m_global_settings_list[i];
        }
    }
    return NULL;
}

bool GlobalSettingsList::empty() const
{
    return (m_global_settings_list_size == 0);
}

const CachedGlobalSetting **GlobalSettingsList::get_by_schema(const char *schema_name,
    size_t &global_settings_list_size) const
{
    const CachedGlobalSetting **global_settings_list(NULL);
    global_settings_list_size = 0;
    for (size_t i = 0; i < m_global_settings_list_size; ++i) {
        if (pg_strcasecmp(m_global_settings_list[i]->get_schema_name(), schema_name) == 0) {
            global_settings_list = (const CachedGlobalSetting **)libpq_realloc(global_settings_list,
                sizeof(*global_settings_list) * global_settings_list_size,
                sizeof(*global_settings_list) * (global_settings_list_size + 1));
            if (global_settings_list == NULL) {
                return NULL;
            }
            global_settings_list[global_settings_list_size] = m_global_settings_list[i];
            ++global_settings_list_size;
        }
    }
    return global_settings_list;
}

const GlobalHookExecutor **GlobalSettingsList::get_executors(size_t &global_hook_executors_size) const
{
    if (m_global_settings_list_size == 0) {
        return NULL;
    }

    const GlobalHookExecutor **global_hook_executors =
        (const GlobalHookExecutor **)malloc(m_global_settings_list_size * sizeof(GlobalHookExecutor *));
    if (global_hook_executors == NULL) {
        return NULL;
    }
    global_hook_executors_size = m_global_settings_list_size;
    for (size_t i = 0; i < m_global_settings_list_size; ++i) {
        global_hook_executors[i] = m_global_settings_list[i]->get_executor();
    }
    return global_hook_executors;
}
