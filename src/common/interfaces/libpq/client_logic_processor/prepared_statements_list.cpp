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
 * prepared_statements_list.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_processor\prepared_statements_list.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "prepared_statements_list.h"

PreparedStatementsList::PreparedStatementsList() : m_prepared_statements(NULL), m_prepared_statements_size(0) {}

PreparedStatementsList::~PreparedStatementsList()
{
    clear();
}

PreparedStatement *PreparedStatementsList::create(const char *statement_name)
{
    /* expand the array */
    m_prepared_statements = (PreparedStatementItem **)libpq_realloc(m_prepared_statements,
        sizeof(*m_prepared_statements) * m_prepared_statements_size,
        sizeof(*m_prepared_statements) * (m_prepared_statements_size + 1));
    if (m_prepared_statements == NULL) {
        return NULL;
    }

    /* initialize array item */
    m_prepared_statements[m_prepared_statements_size] = new (std::nothrow) PreparedStatementItem;
    if (m_prepared_statements[m_prepared_statements_size] == NULL) {
        fprintf(stderr, "failed to new PreparedStatementItem object\n");
        if (m_prepared_statements) {
            free(m_prepared_statements);
            m_prepared_statements = NULL;
        }
        exit(EXIT_FAILURE);
    }

    if (statement_name && statement_name[0] != '\0') {
        check_strncpy_s(strncpy_s(m_prepared_statements[m_prepared_statements_size]->m_statement_name,
            sizeof(m_prepared_statements[m_prepared_statements_size]->m_statement_name), statement_name,
            strlen(statement_name)));
    }
    PreparedStatement *prepared_statement = &(m_prepared_statements[m_prepared_statements_size]->m_prepared_statement);
    ++m_prepared_statements_size;
    return prepared_statement;
}

PreparedStatement *PreparedStatementsList::get_or_create(const char *statement_name)
{
    PreparedStatement *prepared_statement = find(statement_name);
    if (!prepared_statement) {
        return create(statement_name);
    }
    return prepared_statement;
}

bool PreparedStatementsList::merge(PreparedStatementsList *prepared_statements_list)
{
    if (prepared_statements_list == NULL) {
        return false;
    }
    for (size_t i = 0; i < prepared_statements_list->m_prepared_statements_size; ++i) {
        /* check if prepared statement by that name already exists, if it does, then overwrite. */
        size_t idx = 0;
        if (find_idx(prepared_statements_list->m_prepared_statements[i]->m_statement_name, &idx)) {
            delete m_prepared_statements[idx];
            m_prepared_statements[idx] = prepared_statements_list->m_prepared_statements[i];
        } else {
            /* if it's a new prepared statement, then enlarge the array and push it to the end */
            m_prepared_statements = (PreparedStatementItem **)libpq_realloc(m_prepared_statements,
                sizeof(*m_prepared_statements) * m_prepared_statements_size,
                sizeof(*m_prepared_statements) * (m_prepared_statements_size + 1));
            if (m_prepared_statements == NULL) {
                return false;
            }
            m_prepared_statements[m_prepared_statements_size] = prepared_statements_list->m_prepared_statements[i];
            ++m_prepared_statements_size;
        }
        prepared_statements_list->m_prepared_statements[i] = NULL;
    }
    if (prepared_statements_list->m_prepared_statements) {
        free(prepared_statements_list->m_prepared_statements);
        prepared_statements_list->m_prepared_statements = NULL;
    }
    prepared_statements_list->m_prepared_statements_size = 0;
    return true;
}


PreparedStatement *PreparedStatementsList::find(const char *statement_name) const
{
    for (size_t i = 0; i < m_prepared_statements_size; ++i) {
        if (pg_strcasecmp(m_prepared_statements[i]->m_statement_name, statement_name) == 0) {
            return &m_prepared_statements[i]->m_prepared_statement;
        }
    }
    return NULL;
}

bool PreparedStatementsList::find_idx(const char *statement_name, size_t *idx) const
{
    if (!idx) {
        return false;
    }
    for (size_t i = 0; i < m_prepared_statements_size; ++i) {
        if (pg_strcasecmp(m_prepared_statements[i]->m_statement_name, statement_name) == 0) {
            *idx = i;
            return true;
        }
    }
    return false;
}


void PreparedStatementsList::clear()
{
    for (size_t i = 0; i < m_prepared_statements_size; ++i) {
        delete m_prepared_statements[i];
        m_prepared_statements[i] = NULL;
    }
    if (m_prepared_statements) {
        free(m_prepared_statements);
        m_prepared_statements = NULL;
    }
    m_prepared_statements_size = 0;
}
