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
 * prepared_statements_list.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\prepared_statements_list.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PREPARED_STATEMENTS_LIST_H
#define PREPARED_STATEMENTS_LIST_H

#include "libpq-fe.h"
#include "libpq-int.h"
#include "client_logic_processor/prepared_statement.h"

class PreparedStatementItem {
public:
    PreparedStatementItem()
    {
        m_statement_name[0] = '\0';
    }
    ~PreparedStatementItem() {}
    char m_statement_name[NAMEDATALEN];
    PreparedStatement m_prepared_statement;
};

class PreparedStatementsList {
public:
    PreparedStatementsList();
    ~PreparedStatementsList();
    PreparedStatement *create(const char *statement_name);
    PreparedStatement *get_or_create(const char *statement_name);
    bool merge(PreparedStatementsList *prepared_statements_list);
    void clear();

protected:
    PreparedStatementItem **m_prepared_statements;
    size_t m_prepared_statements_size;

private:
    PreparedStatement *find(const char *statement_name) const;
    bool find_idx(const char *statement_name, size_t *idx) const;
};

#endif
