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
 * cached_global_setting.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cached_global_setting.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CACHED_GLOBAL_SETTING_H
#define CACHED_GLOBAL_SETTING_H

#include "cached_setting.h"

class GlobalHookExecutor;

class CachedGlobalSetting : public CachedSetting {
public:
    CachedGlobalSetting(const Oid oid, const char *database_name, const char *schema_name, const char *object_name);
    ~CachedGlobalSetting();
    void set_global_hook_executor(GlobalHookExecutor *global_hook_executor);
    GlobalHookExecutor *get_executor() const;
    GlobalHookExecutor *m_global_hook_executor;
};

#endif