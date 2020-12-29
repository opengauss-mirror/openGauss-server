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
 * column_settings_list.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\column_settings_list.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef COLUMN_SETTINGS_LIST_H
#define COLUMN_SETTINGS_LIST_H
#include "c.h"

typedef unsigned int Oid;

class CachedColumnSetting;

class ColumnSettingsList {
public:
    ColumnSettingsList();
    ~ColumnSettingsList();
    void clear();
    bool add(CachedColumnSetting *cached_column_setting);
    const CachedColumnSetting *get_by_oid(Oid oid) const;
    const CachedColumnSetting *get_by_fqdn(const char *fqdn) const;
    const CachedColumnSetting **get_by_schema(const char *schema_name, size_t &column_settings_list_size) const;
    const CachedColumnSetting **get_by_global_setting(const char *global_setting,
        size_t &column_settings_list_size) const;
    bool empty() const;

private:
    CachedColumnSetting **m_column_settings_list;
    size_t m_column_settings_list_size;
};

#endif