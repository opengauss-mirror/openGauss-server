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
 * cached_setting.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\cached_setting.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CACHED_SETTING_H
#define CACHED_SETTING_H
#include "c.h"

typedef unsigned int Oid;

/**
 * @class CachedSetting  should be abstract and contain common info
 * to global and column executors
 */
class CachedSetting {
public:
    CachedSetting(const Oid oid, const char *database_name, const char *schema_name, const char *object_name);
    void init(const char *database_name, const char *schema_name, const char *object_name);

    virtual ~CachedSetting() {
    }

    /**
     * @return object OID
     */
    Oid get_oid() const;

    /**
     * @return FQDN of object
     */
    const char *get_fqdn() const;

    /**
     * @return Database name
     */
    const char *get_database_name() const;

    /**
     * @return Schema name
     */
    const char *get_schema_name() const;

    /**
     * @return name only, without schema prefix
     */
    const char *get_object_name() const;

protected:
    Oid m_oid;
    NameData m_database_name;
    NameData m_schema_name;
    NameData m_object_name;
    char m_fqdn[NAMEDATALEN * 4]; /* database + schema + object + extra padding */

private:
    void build_fqdn();
};

#endif