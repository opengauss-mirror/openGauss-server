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
 * jni_conn_cursor.h
 *
 * IDENTIFICATION
 * 	  src\common\interfaces\libpq\client_logic_data_fetcher\jni_conn_cursor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CLIENT_LOGIC_DATA_FETCHER_JNI_CONN_CURSOR_H_
#define CLIENT_LOGIC_DATA_FETCHER_JNI_CONN_CURSOR_H_

#include "data_fetcher_manager.h"
#include "cursor_interface.h"

typedef struct JNIEnv_ JNIEnv; /* forward declaration */
class _jobjectArray;
typedef _jobjectArray *jobjectArray; /* forward declaration */
class _jobject;
typedef _jobject *jobject; /* forward declaration */

/* ! \class JNIConnCursor
    \brief CursorInterface implementation using the JDBC connection over JNI
*/
class JNIConnCursor : public CursorInterface {
public:
    explicit JNIConnCursor(PGconn *conn) : m_conn(conn) {}
    virtual ~JNIConnCursor();
    virtual bool next() override;
    virtual const char *operator[](int col) const override;
    virtual int get_column_index(const char *column_name) const override;
    bool load(const char *query) override;
    virtual void clear_result() override;

private:
    bool parse_result(const char *query);
    JNIEnv *get_jni_env() const;
    jobject get_jdbc_cl_impl() const;
    PGconn *m_conn = NULL;
    jobjectArray m_result_object = NULL;
    int m_current_row = -1;
    int m_number_of_rows = -1;
    int m_number_of_fields = -1;
    const char ***m_utf_strings = NULL;
};

#endif /* CLIENT_LOGIC_DATA_FETCHER_JNI_CONN_CURSOR_H_ */