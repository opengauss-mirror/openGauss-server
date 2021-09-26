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
 * data_fetcher_manager.cpp
 *
 * IDENTIFICATION
 * 	  src\common\interfaces\libpq\client_logic_data_fetcher\data_fetcher_manager.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "data_fetcher_manager.h"
#include "lib_pq_cursor.h"
#include "data_fetcher.h"
#include "jni_conn_cursor.h"

/* *
 * Data fetcher constructor
 * @param conn PGCOnn handle to be used to fetch data with (when not using JDBC) and to report back any erros
 * @param env - Java run time handle in case of JDBC, otherwise null
 * @param jni_cl_impl jdbc jni connecting class to be used when fetching data
 */
DataFetcherManager::DataFetcherManager(PGconn *conn, JNIEnv *env, jobject jni_cl_impl)
    : m_env(env), m_jdbc_cl_impl(jni_cl_impl)
{
    if (m_env == NULL || m_jdbc_cl_impl == NULL) {
        m_cursor = new (std::nothrow) LibPQCursor(conn);
    } else {
        m_cursor = new (std::nothrow) JNIConnCursor(conn);
    }

    if (m_cursor == NULL) {
        fprintf(stderr, "out of memory when init DataFetcherManager\n");
        exit(EXIT_FAILURE);
    }
}
/* *
 * Delete the cursor when exiting
 */
DataFetcherManager::~DataFetcherManager()
{
    if (m_cursor != NULL) {
        delete m_cursor;
        m_cursor = NULL;
    }
}
/* *
 * * @return the data fetcher instance (by value)
 */
DataFetcher DataFetcherManager::get_data_fetcher() const
{
    return DataFetcher(m_cursor);
}

/* *
 * sets the java run time & the JNI java class.
 * To be used for every time data may be required in the beginning of the JNI interface
 * It is important to use it every time a JNI call is made as the values that supplied on the constructor may be invalid
 * @param env - Java run time handle in case of JDBC, otherwise null
 * @param jni_cl_impl jdbc jni connecting class to be used when fetching data
 */
void DataFetcherManager::set_jni_env_and_cl_impl(JNIEnv *env, jobject jni_cl_impl)
{
    m_env = env;
    m_jdbc_cl_impl = jni_cl_impl;
}

/* *
 * @return pointer to the Java run time
 */
JNIEnv *DataFetcherManager::get_java_env() const
{
    return m_env;
}

/* *
 *
 * @return
 */ 
jobject DataFetcherManager::get_jdbc_cl_impl() const
{
    return m_jdbc_cl_impl;
}
