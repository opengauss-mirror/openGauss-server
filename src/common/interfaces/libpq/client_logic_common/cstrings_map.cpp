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
 * cstrings_map.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_common\cstrings_map.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "client_logic/cstrings_map.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include <string.h>
#define RESIZE_FACTOR 5

CStringsMap::CStringsMap() : k_v_map(NULL), size(0) {}

CStringsMap::CStringsMap(const CStringsMap &other)
{
    init(other);
}

void CStringsMap::init(const CStringsMap &other)
{
    size = other.size;
    k_v_map = (ConstKeyValue *)malloc(size * sizeof(ConstKeyValue));
    if (k_v_map == NULL) {
        printf("out of memory\n");
        exit(EXIT_FAILURE);
    }
    check_memcpy_s(memcpy_s(k_v_map, size * sizeof(ConstKeyValue), other.k_v_map, size * sizeof(ConstKeyValue)));
}

CStringsMap &CStringsMap::operator = (const CStringsMap &other)
{
    libpq_free(k_v_map);
    size = other.size;
    if (size != 0) {
        k_v_map = (ConstKeyValue *)malloc(size * sizeof(ConstKeyValue));
        if (k_v_map == NULL) {
            printf("out of memory\n");
            exit(EXIT_FAILURE);
        }
        check_memcpy_s(memcpy_s(k_v_map, size * sizeof(ConstKeyValue), other.k_v_map, size * sizeof(ConstKeyValue)));
    }
    return *this;
}

CStringsMap::~CStringsMap()
{
    clear();
}
void CStringsMap::set(const char *key, const char *value, size_t valsize)
{
    Assert(key && value);
    size_t key_index = index(key);
    if (valsize == SIZE_MAX) {
        valsize = strlen(value);
    }
    if (valsize > MAX_VAL_LEN) {
        return;
    }
    /* if key doesn't already exist then create one */
    if (key_index == size) {
        if (size % RESIZE_FACTOR == 0) {
            k_v_map = (struct ConstKeyValue *)libpq_realloc(k_v_map, sizeof(*k_v_map) * size,
                sizeof(*k_v_map) * (size + RESIZE_FACTOR));
            if (k_v_map == NULL) {
                return;
            }
        }
        check_strncpy_s(strncpy_s(k_v_map[key_index].key, sizeof(k_v_map[key_index].key), key, strlen(key)));
        k_v_map[key_index].key[strlen(key)] = 0;
        ++size;
    }
    /* set value to key-pair */
    check_memcpy_s(memcpy_s(k_v_map[key_index].value, sizeof(k_v_map[key_index].value), value, valsize));
    k_v_map[key_index].valsize = valsize;
    k_v_map[key_index].value[valsize] = '\0';
}

void CStringsMap::clear()
{
    libpq_free(k_v_map);
    size = 0;
}

const char *CStringsMap::find(const char *key) const
{
    size_t i = index(key);
    if (i < size) {
        return k_v_map[i].value;
    }
    return NULL;
}
const char *CStringsMap::find(const char *key, size_t *valsize) const
{
    size_t i = index(key);
    if (i < size) {
        *valsize = k_v_map[i].valsize;
        return k_v_map[i].value;
    }
    return NULL;
}
const size_t CStringsMap::index(const char *key) const
{
    if (!key || strlen(key) > NAMEDATALEN) {
        return UINT_MAX;
    }
    size_t i = 0;
    for (; i < size; ++i) {
        if (pg_strncasecmp(k_v_map[i].key, key, NAMEDATALEN) == 0) {
            break;
        }
    }
    return i;
}
