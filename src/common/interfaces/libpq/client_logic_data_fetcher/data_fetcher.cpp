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
 * data_fetcher.cpp
 *
 * IDENTIFICATION
 * 	  src\common\interfaces\libpq\client_logic_data_fetcher\data_fetcher.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "data_fetcher.h"
#include "lib_pq_cursor.h"
#include "cursor_interface.h"
#include "jni_conn_cursor.h"

/* *
 *
 * @param cursor curosr to be used with this fetcher
 */
DataFetcher::DataFetcher(CursorInterface *cursor) : m_cursor(cursor) {}

/* *
 * Clears the cursor results when out of scope
 */
DataFetcher::~DataFetcher()
{
    if (m_cursor != NULL) {
        m_cursor->clear_result();
    }
}
/* *
 * move the cursor to the next record
 * @return true if there is a next record or false when no
 */
bool DataFetcher::next()
{
    if (m_cursor != NULL) {
        return m_cursor->next();
    }
    return false;
}
/* *
 * gets the value of a column by its index
 * @param col column index
 * @return the column value in the current record
 */
const char *DataFetcher::operator[](int col) const
{
    if (m_cursor != NULL) {
        return (*m_cursor)[col];
    }
    return NULL;
}

/* *
 * @param column_name column name
 * @return the column index of a given column name
 */
int DataFetcher::get_column_index(const char *column_name) const
{
    if (m_cursor != NULL) {
        return m_cursor->get_column_index(column_name);
    }
    return 0;
}

/* *
 * queries can be sent to server by either libpq itself (using PQexec),
 * or, if we are running by JNI and have already a connection - we can send the query back to java's connection.
 * in this function we determine the path and handle typical errors, returning a cursor-like object to the caller
 * @param query SQL query
 * @return true if a successful query made or false for failure
 */
bool DataFetcher::load(const char *query)
{
    if (m_cursor != NULL) {
        m_cursor->clear_result();
        return m_cursor->load(query);
    }
    return false;
}
