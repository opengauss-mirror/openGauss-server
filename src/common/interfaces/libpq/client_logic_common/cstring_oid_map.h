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
 * cstring_oid_map.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_common\cstring_oid_map.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CSTRING_OID_MAP_H
#define CSTRING_OID_MAP_H

#include <cstdio>
#include <string>

#define NAMEDATALEN 64
typedef unsigned int Oid;

struct constKeyValueStrOid {
    char key[NAMEDATALEN];
    Oid value;
};

class CStringOidMap {
public:
    CStringOidMap();
    ~CStringOidMap();
    void set(const char *key, const Oid oid);
    void clear();
    const Oid find(const char *key) const;
    const char *find_by_oid(const Oid oid) const;
    const size_t Size() const
    {
        return size;
    }
    void fill_types_map();

private:
    size_t size;
    struct constKeyValueStrOid *k_v_map;
    const size_t index(const char *key) const;
};

#endif