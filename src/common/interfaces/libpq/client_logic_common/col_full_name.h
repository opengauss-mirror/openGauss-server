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
 * col_full_name.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_common\col_full_name.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef COL_FULL_NAME_H
#define COL_FULL_NAME_H

#include "table_full_name.h"
#include "c.h"

class colFullName : public tableFullName {
public:
    /* 
     * @brief constructor, creates objects from strings
     * @param catalog_name  catalog name where the table contains this column is located
     * @param schema_name  schmea name where the table contains this column is located
     * @param rel_name  table contains this column is located
     * @param colname  column name
     */
    colFullName(const char *catalog_name, const char *schema_name, const char *rel_name, const char *colname);
    colFullName();
    ~colFullName();

    /* 
     * @return column name only, without table prefix
     */
    const char *get_col_name() const;

    bool operator == (const colFullName &other) = delete;

    /* 
     * @param colName column name only, without table prefix
     */
    void set_col_name(const char *colname);

private:
    NameData m_col_name; /* column name only, without its table prefix */
};

#endif