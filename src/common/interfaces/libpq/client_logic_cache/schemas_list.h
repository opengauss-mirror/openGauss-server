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
 * schemas_list.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_cache\schemas_list.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SCHEMAS_LIST_H
#define SCHEMAS_LIST_H

#include "c.h"

class SchemasList {
public:
    SchemasList();
    ~SchemasList();

    bool add(const char* schema_name);
    bool exists(const char* schema_name) const;
    bool remove(const char* schema_name);
    void clear();
private:
    NameData* m_schemas;
    size_t m_schemas_size;
};

#endif
