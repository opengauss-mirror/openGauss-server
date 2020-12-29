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
 * cstring_oid_map.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_common\cstring_oid_map.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cstring_oid_map.h"
#include "libpq-int.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#define RESIZE_FACTOR 4

CStringOidMap::CStringOidMap() : size(0), k_v_map(NULL) {}
CStringOidMap::~CStringOidMap()
{
    clear();
}

void CStringOidMap::set(const char *key, const Oid value)
{
    Assert(key && value);
    size_t key_index = index(key);
    if (key_index == size) {
        if (size % RESIZE_FACTOR == 0) {
            k_v_map = (struct constKeyValueStrOid *)libpq_realloc(k_v_map, sizeof(*k_v_map) * size,
                sizeof(*k_v_map) * (size + RESIZE_FACTOR));
            if (k_v_map == NULL) {
                return;
            }
        }
        check_strncpy_s(strncpy_s(k_v_map[size].key, sizeof(k_v_map[size].key), key, strlen(key)));
        ++size;
    }
    k_v_map[key_index].value = value;
}

void CStringOidMap::clear()
{
    libpq_free(k_v_map);
    size = 0;
}

const Oid CStringOidMap::find(const char *key) const
{
    size_t i = index(key);
    if (i < size) {
        return k_v_map[i].value;
    }

    return InvalidOid;
}
const char *CStringOidMap::find_by_oid(const Oid oid) const
{
    for (size_t i = 0; i < size; ++i) {
        if (k_v_map[i].value == oid) {
            return k_v_map[i].key;
        }
    }

    return NULL;
}
const size_t CStringOidMap::index(const char *key) const
{
    size_t i = 0;
    for (; i < size; ++i) {
        if (pg_strncasecmp(k_v_map[i].key, key, NAMEDATALEN) == 0) {
            break;
        }
    }
    return i;
}
