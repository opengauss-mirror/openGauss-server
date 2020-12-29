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
 * cached_column_setting.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cached_column_setting.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CACHED_COLUMN_SETTING_H
#define CACHED_COLUMN_SETTING_H
#include "cached_setting.h"
typedef unsigned int Oid;

class CachedGlobalSetting;
class ColumnHookExecutor;

class CachedColumnSetting : public CachedSetting {
public:
    CachedColumnSetting(const Oid oid, const char *database_name, const char *schema_name, const char *object_name);
    ~CachedColumnSetting();
    ColumnHookExecutor *get_executor() const;
    void set_executor(ColumnHookExecutor *executor);
    char m_cached_global_setting[4 * NAMEDATALEN + 1];
    ColumnHookExecutor *m_column_hook_executor;
};

#endif