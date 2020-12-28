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
 * table_full_name.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_common\table_full_name.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef TABLE_FULL_NAME_H
#define TABLE_FULL_NAME_H

#include "c.h"

class tableFullName {
public:
    tableFullName();
    tableFullName(const char *catalog_name, const char *schema_name, const char *table_name);
    virtual ~tableFullName();
    bool operator == (const tableFullName &other) const = delete;

    /* catalog name */
    const char *get_catalog_name() const;
    void set_catalog_name(const char *catalog_name);

    /* schema name */
    const char *get_schema_name() const;
    void set_schema_name(const char *schema_name);

    /* table name */
    const char *get_table_name() const;
    void set_table_name(const char *table_name);

private:
    NameData m_catalog_name;
    NameData m_schema_name;
    NameData m_table_name;
};

#endif