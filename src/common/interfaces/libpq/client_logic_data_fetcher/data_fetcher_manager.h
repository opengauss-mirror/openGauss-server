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
 * data_fetcher_manager.h
 *
 * IDENTIFICATION
 * 	  src\common\interfaces\libpq\client_logic_data_fetcher\data_fetcher_manager.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DATA_FETCHER_MANAGER_H_
#define DATA_FETCHER_MANAGER_H_

#include "cursor_interface.h"
#include "data_fetcher.h"
typedef struct JNIEnv_ JNIEnv; /* forward declaration */
class _jobject;
typedef _jobject *jobject; /* forward declaration */

typedef struct pg_conn PGconn;

/* ! \class DataFetcherManager
    \brief create the cursors and maintain its implementation eitherusing JNI or libpq connections
*/
class DataFetcherManager {
public:
    DataFetcherManager(PGconn *conn, JNIEnv *env, jobject jni_cl_impl);
    virtual ~DataFetcherManager();
    DataFetcher get_data_fetcher() const;
    void set_jni_env_and_cl_impl(JNIEnv *env, jobject jni_cl_impl);
    JNIEnv *get_java_env() const;
    jobject get_jdbc_cl_impl() const;

private:
    CursorInterface *m_cursor = NULL;

/* 
 * jni function prototype can not be modified
 * we make these vars public invoked by internal routine
 */
public:
    JNIEnv *m_env = NULL;
    jobject m_jdbc_cl_impl = NULL;
};

#endif /* DATA_FETCHER_MANAGER_H_ */
