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
 * lib_pq_cursor.h
 *
 * IDENTIFICATION
 * 	  src\common\interfaces\libpq\client_logic_data_fetcher\lib_pq_cursor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SRC_LIB_PQ_CURSOR_H_
#define SRC_LIB_PQ_CURSOR_H_

#include "libpq-int.h"
#include "libpq/cl_state.h"
#include "cursor_interface.h"

/* ! \class LibPQCursor
    \brief cursor implementation using using libpq
*/
class LibPQCursor : public CursorInterface {
public:
    explicit LibPQCursor(PGconn *conn);
    ~LibPQCursor();
    const char *operator[](int col) const override;
    bool load(const char *query) override;

    bool next() override;
    int get_column_index(const char *column_name) const override;
    void clear_result() override;

private:
    PGconn *m_conn = NULL;
    PGresult *m_data_handler = NULL;
    int m_current_row;
    int m_number_of_rows;
    int m_nunber_of_fields;
    int m_error_code;
};

#endif /* SRC_LIB_PQ_CURSOR_H_ */
