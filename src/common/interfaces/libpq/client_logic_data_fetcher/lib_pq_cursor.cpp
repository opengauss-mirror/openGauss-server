/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * lib_pq_cursor.cpp
 *
 * IDENTIFICATION
 * 	  src\common\interfaces\libpq\client_logic_data_fetcher\lib_pq_cursor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "lib_pq_cursor.h"
#include "stdio.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"


/* *
 * Constructor
 */
LibPQCursor::LibPQCursor(PGconn *conn)
    : m_conn(conn),
      m_data_handler(NULL),
      m_current_row(-1),
      m_number_of_rows(-1),
      m_nunber_of_fields(-1),
      m_error_code(0)
{}

/* *
 * Destrcutor
 */
LibPQCursor::~LibPQCursor()
{
    clear_result();
}

/* *
 * Loads the cursor data from libpq PGresult
 * @param[in] query query to use
 * @return on
 */
bool LibPQCursor::load(const char *query)
{
    m_conn->client_logic->disable_once = true;
    m_data_handler = PQexec(m_conn, query);
    if (!m_data_handler) {
        printfPQExpBuffer(&m_conn->errorMessage, libpq_gettext("Client encryption cache query: '%s' failed\n"), query);
        m_data_handler = NULL;
        return false;
    }
    /* check status */
    if (PQresultStatus(m_data_handler) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Client encryption cache query: '%s' failed with error : %d, '%s'\n", query,
            PQresultStatus(m_data_handler), PQresultErrorMessage(m_data_handler));
        PQclear(m_data_handler);
        m_data_handler = NULL;
        return false;
    }
    m_current_row = -1;
    m_number_of_rows = PQntuples(m_data_handler);
    m_nunber_of_fields = PQnfields(m_data_handler);
    return true;
}

/* *
 * Move next record
 * @return true on success and false for end of cursor
 */
bool LibPQCursor::next()
{
    if (m_current_row < m_number_of_rows - 1) {
        ++m_current_row;
        return true;
    }
    return false;
}

/* *
 * Retrieve column value from current record by index
 * @param[in] col column index
 * @return column value or null for invalid index
 */
const char *LibPQCursor::operator[](int col) const
{
    if (m_current_row < 0 || m_current_row >= m_number_of_rows || col >= m_nunber_of_fields) {
        fprintf(stderr, "Client encryption operator[] failed with a bad index: %d", col);
        return NULL;
    }
    if (m_data_handler != NULL) {
        return PQgetvalue(m_data_handler, m_current_row, col);
    }
    return NULL;
}

/* *
 * @param column_name column name
 * @return the column index of a given column name
 */
int LibPQCursor::get_column_index(const char *column_name) const
{
    return PQfnumber(m_data_handler, column_name);
}

/* *
 * Clears the cursor results and release memory
 */
void LibPQCursor::clear_result()
{
    if (m_data_handler != NULL) {
        PQclear(m_data_handler);
        m_data_handler = NULL;
    }
}
