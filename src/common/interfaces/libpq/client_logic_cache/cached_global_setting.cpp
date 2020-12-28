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
 * cached_global_setting.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cached_global_setting.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cached_global_setting.h"
#include "client_logic_hooks/hooks_manager.h"
#include "client_logic_hooks/global_hook_executor.h"

CachedGlobalSetting::CachedGlobalSetting(const Oid oid, const char *database_name, const char *schema_name,
    const char *object_name)
    : CachedSetting(oid, database_name, schema_name, object_name), m_global_hook_executor(NULL)
{}

CachedGlobalSetting::~CachedGlobalSetting()
{
    if (m_global_hook_executor) {
        m_global_hook_executor->dec_ref_count();
        if (m_global_hook_executor->safe_to_remove()) {
            HooksManager::GlobalSettings::delete_global_hook_executor(m_global_hook_executor);
            m_global_hook_executor = NULL;
        }
    }
}

void CachedGlobalSetting::set_global_hook_executor(GlobalHookExecutor *global_hook_executor)
{
    m_global_hook_executor = global_hook_executor;
    m_global_hook_executor->inc_ref_count();
}
GlobalHookExecutor *CachedGlobalSetting::get_executor() const
{
    return m_global_hook_executor;
}
