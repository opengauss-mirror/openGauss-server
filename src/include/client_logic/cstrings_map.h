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
 * cstrings_map.h
 *
 * IDENTIFICATION
 *	  src\include\client_logic\cstrings_map.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef C_STRINGS_MAP_H
#define C_STRINGS_MAP_H
#include <cstdio>
#include <string>

#define MAX_VAL_LEN 1024
#define NAMEDATALEN 64
#define NAME_CNT 4

struct ConstKeyValue {
    char key[NAMEDATALEN * 4 + 1]; /* 4 is database + schema + object + extra padding */
    char value[MAX_VAL_LEN + 1];
    size_t valsize;
};

class CStringsMap {
public:
    CStringsMap();
    CStringsMap(const CStringsMap &other);
    CStringsMap &operator = (const CStringsMap &other);
    void init(const CStringsMap &other);
    ~CStringsMap();
    void set(const char *key, const char *value, size_t valsize = SIZE_MAX);
    void clear();
    const char *find(const char *key) const;
    const char *find(const char *key, size_t *size) const;
    const size_t Size() const
    {
        return size;
    }
    ConstKeyValue *at(size_t i) const
    {
        if (i < size) {
            return &k_v_map[i];
        } else {
            return NULL;
        }
    }
    struct ConstKeyValue *k_v_map;

private:
    size_t size;
    const size_t index(const char *key) const;
};

typedef CStringsMap StringArgsVec;
#endif /* C_STRINGS_MAP_H */
