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
 * cached_setting.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cached_setting.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cached_setting.h"
#include "libpq-int.h"

/* initial a CacheColumns with given attributes */
CachedSetting::CachedSetting(const Oid oid, const char *database_name, const char *schema_name, const char *object_name)
    : m_oid(oid)
{
    init(database_name, schema_name, object_name);
    build_fqdn();
}

void CachedSetting::init(const char *database_name, const char *schema_name, const char *object_name)
{
    const char *database = (database_name) != NULL ? database_name : "";
    const char *schema = (schema_name) != NULL ? schema_name : "";
    const char *object = (object_name) != NULL ? object_name : "";
    check_strncpy_s(
        strncpy_s(m_database_name.data, sizeof(m_database_name.data), database, strlen(database)));
    check_strncpy_s(strncpy_s(m_schema_name.data, sizeof(m_schema_name.data), schema, strlen(schema)));
    check_strncpy_s(strncpy_s(m_object_name.data, sizeof(m_object_name.data), object, strlen(object)));
}

/* push m_database_name.m_schema_name.m_object_name into m_fgdn */
void CachedSetting::build_fqdn()
{
    check_strncpy_s(strncpy_s(m_fqdn, sizeof(m_fqdn), m_database_name.data, strlen(m_database_name.data)));
    check_strncpy_s(strncpy_s(m_fqdn + strlen(m_fqdn), sizeof(m_fqdn) - strlen(m_fqdn), ".", 1));
    check_strncpy_s(strncpy_s(m_fqdn + strlen(m_fqdn), sizeof(m_fqdn) - strlen(m_fqdn), m_schema_name.data,
        strlen(m_schema_name.data)));
    check_strncpy_s(strncpy_s(m_fqdn + strlen(m_fqdn), sizeof(m_fqdn) - strlen(m_fqdn), ".", 1));
    check_strncpy_s(strncpy_s(m_fqdn + strlen(m_fqdn), sizeof(m_fqdn) - strlen(m_fqdn), m_object_name.data,
        strlen(m_object_name.data)));
}

Oid CachedSetting::get_oid() const
{
    return m_oid;
}

const char *CachedSetting::get_fqdn() const
{
    return m_fqdn;
}

const char *CachedSetting::get_database_name() const
{
    return m_database_name.data;
}

const char *CachedSetting::get_schema_name() const
{
    return m_schema_name.data;
}

const char *CachedSetting::get_object_name() const
{
    return m_object_name.data;
}
