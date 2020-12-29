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
 * search_path_list.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\search_path_list.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SEARCH_PATH_LIST_H
#define SEARCH_PATH_LIST_H

#include "c.h"

class SearchPathList {
public:
    SearchPathList();
    ~SearchPathList() {}

    bool add(const char *schema_name);
    bool add_user_schema(const char *schema_name);
    void set_user_schema(const char *user_name);
    size_t size() const;
    const char *at(size_t position) const;
    void clear();

private:
    bool exists(const char *schema_name) const;

private:
    NameData *m_schemas;
    size_t m_schemas_size;
    int m_user_schema_index; /* index of $user in search path, -1 if not applicable */
};

#endif