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
 * global_settings_list.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\global_settings_list.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GLOBAL_SETTINGS_LIST_H
#define GLOBAL_SETTINGS_LIST_H

#include "c.h"

typedef unsigned int Oid;

class CachedGlobalSetting;
class GlobalHookExecutor;

class GlobalSettingsList {
public:
    GlobalSettingsList();
    ~GlobalSettingsList();
    void clear();
    bool add(CachedGlobalSetting *cached_global_setting);
    const CachedGlobalSetting *get_by_oid(Oid oid) const;
    const CachedGlobalSetting *get_by_fqdn(const char *fqdn) const;
    const CachedGlobalSetting *get_by_details(const char *database_name, const char *schema_name,
        const char *object_name) const;
    const CachedGlobalSetting **get_by_schema(const char *schema_name, size_t &global_settings_list_size) const;
    const GlobalHookExecutor **get_executors(size_t &global_hook_executors_size) const;
    bool empty() const;

private:
    CachedGlobalSetting **m_global_settings_list;
    size_t m_global_settings_list_size;
};

#endif