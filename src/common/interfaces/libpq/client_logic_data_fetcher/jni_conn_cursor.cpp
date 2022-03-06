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
 * jni_conn_cursor.cpp
 *
 * IDENTIFICATION
 * 	  src\common\interfaces\libpq\client_logic_data_fetcher\jni_conn_cursor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "jni_conn_cursor.h"
#include "libpq-int.h"
#ifndef ENABLE_LITE_MODE
#include <jni.h>
#endif

static const int DATA_INDEX = 2;

/* *
 * Destrcutor
 */
JNIConnCursor::~JNIConnCursor()
{
    clear_result();
}

/* *
 * clears the memory maintain the cursor
 */
void JNIConnCursor::clear_result()
{
    if (m_utf_strings != NULL) {
        for (int i = 0; i < m_number_of_rows; ++i) {
            if (!m_utf_strings[i]) {
                continue;
            }
            jobjectArray data_row = (jobjectArray)get_jni_env()->GetObjectArrayElement(m_result_object, i + 1);
            // convert all jstrings to native. release upon destruction
            for (int j = 0; j < m_number_of_fields; ++j) {
                if (!m_utf_strings[i][j]) {
                    continue;
                }
                jstring java_string = (jstring)get_jni_env()->GetObjectArrayElement(data_row, j);
                get_jni_env()->ReleaseStringUTFChars(java_string, m_utf_strings[i][j]); // release resources
            }
            delete[] m_utf_strings[i];
        }
        delete[] m_utf_strings;
        m_utf_strings = NULL;
    }
    if (m_result_object != NULL) {
        get_jni_env()->DeleteLocalRef(m_result_object);
        m_result_object = NULL;
    }
    m_number_of_rows = -1;
    m_number_of_fields = -1;
    m_current_row = -1;
}

/* *
 * moves the current record forward by one
 * @return true if there is a record and false if not
 */
bool JNIConnCursor::next()
{
    if (m_current_row < m_number_of_rows - 1) {
        ++m_current_row;
        return true;
    }
    return false;
}

/* *
 * gets the value of a column by its index
 * @param col column index
 * @return the column value in the current record
 */
const char *JNIConnCursor::operator[](int col) const
{
    // get from m_result_object
    if (m_current_row < 0 || m_current_row >= m_number_of_rows) {
        return NULL;
    }
    if (m_utf_strings == NULL || m_utf_strings[m_current_row] == NULL) {
        return NULL;
    }
    if (col < 0 || col >= m_number_of_fields) {
        return NULL;
    }
    return m_utf_strings[m_current_row][col];
}

/* *
 * @param column_name column name
 * @return the column index of a given column name
 */
int JNIConnCursor::get_column_index(const char *column_name) const
{
    // m_result_object's first row is headers (data.add(headers);)
    if (m_result_object == NULL) {
        fprintf(stderr, "get_column_index result object is null\n");
    }

    jobjectArray header_row = (jobjectArray)get_jni_env()->GetObjectArrayElement(m_result_object, 1);
    for (int i = 0; i < m_number_of_fields; ++i) {
        jstring java_field_name = (jstring)get_jni_env()->GetObjectArrayElement(header_row, i);
        const char *field_name = get_jni_env()->GetStringUTFChars(java_field_name, NULL);
        int strcmp_res = strcmp(field_name, column_name);
        get_jni_env()->ReleaseStringUTFChars(java_field_name, field_name); // release resources
        if (strcmp_res == 0) {
            return i;
        }
    }
    fprintf(stderr, "get_column_index %s not found in any of %d fields\n", column_name, m_number_of_fields);
    return -1;
}
/* *
 * invoke a query
 * @param query SQL query
 * @return true if a successful query made or false for failure
 */
bool JNIConnCursor::load(const char *query)
{
    /* connection should be built before load data through jdbc so that jni_cl_impl is valid */
    jobject jni_cl_impl = get_jdbc_cl_impl();
    if (JNI_TRUE == get_jni_env()->IsSameObject(jni_cl_impl, NULL)) {
        fprintf(stderr, "Client encryption cache query: '%s' failed with error: jobject %p was invalid!\n", 
                query, jni_cl_impl);
        return false;
    }

    /* 
     * routine of invoke query is as below
     * 1. get obejct class
     * 2. get methodid for specific method by method name from class
     * 3. invoke the method by methodid 
     */
    jclass jdbc_cl_impl_cls = get_jni_env()->GetObjectClass(jni_cl_impl);
    if (jdbc_cl_impl_cls == NULL) {
        fprintf(stderr, "Client encryption cache query; '%s' failed with error: GetObjectClass failed\n", query);
        return false;
    }
    char *method_name = "fetchDataFromQuery";
    jmethodID fetch_data_mid =
        get_jni_env()->GetMethodID(jdbc_cl_impl_cls, method_name, "(Ljava/lang/String;)[Ljava/lang/Object;");
    if (fetch_data_mid == NULL) {
        get_jni_env()->DeleteLocalRef(jdbc_cl_impl_cls);
        fprintf(stderr, "Client encryption cache query: '%s' failed with error: GetMethodID failed\n", query);
        return false;
    }
    jstring java_query = get_jni_env()->NewStringUTF(query);
    m_result_object = (jobjectArray)get_jni_env()->CallObjectMethod(jni_cl_impl, fetch_data_mid, java_query);
    if (!parse_result(query)) {
        get_jni_env()->DeleteLocalRef(jdbc_cl_impl_cls);
        get_jni_env()->DeleteLocalRef(java_query);
        return false;
    }

    /* release java obejct reference */
    get_jni_env()->DeleteLocalRef(jdbc_cl_impl_cls);
    get_jni_env()->DeleteLocalRef(java_query);
    return true;
}

/* *
 * parses the result of a query that was sent via the JNI to the Java PgConnection object
 * @param query SQL query ran
 * @return true on success and  false on failure
 */
bool JNIConnCursor::parse_result(const char *query)
{
    /* result should have a first row for column names, rest of rows are data */
    if (m_result_object == NULL) {
        fprintf(stderr, "Client encryption cache query: '%s' failed, got empty response\n", query);
        clear_result();
        return false;
    }

    int array_length = get_jni_env()->GetArrayLength(m_result_object);
    if (array_length < 1) {
        fprintf(stderr, "Client encryption cache query: '%s' failed, got empty array response\n", query);
        clear_result();
        return false;
    }

    jstring java_error_message = (jstring)get_jni_env()->GetObjectArrayElement(m_result_object, 0);
    const char *error_message = get_jni_env()->GetStringUTFChars(java_error_message, NULL);
    if (strlen(error_message) > 0) {
        fprintf(stderr, "Client encryption cache query: '%s' failed with error: %s\n", query, error_message);
        get_jni_env()->ReleaseStringUTFChars(java_error_message, error_message); // release resources
        clear_result();
        return false;
    }
    get_jni_env()->ReleaseStringUTFChars(java_error_message, error_message); // release resources

    if (array_length < DATA_INDEX) {
        fprintf(stderr, "Client encryption cache query: '%s' failed, got empty array response with no data\n", query);
        clear_result();
        return false;
    }

    jobjectArray header_row = (jobjectArray)get_jni_env()->GetObjectArrayElement(m_result_object, 1);
    m_number_of_rows = array_length - DATA_INDEX;
    m_number_of_fields = get_jni_env()->GetArrayLength(header_row);
    if (m_number_of_fields < 1) {
        fprintf(stderr, "Client encryption cache query: '%s' failed, got empty fields array\n", query);
        clear_result();
        return false;
    }
    // test data validity
    m_utf_strings = new const char **[m_number_of_rows];
    if (m_utf_strings == NULL) {
        fprintf(stderr, "Client encryption cache query: '%s' failed, \
                failed to allocate memory for m_number_of_rows\n", query);
        clear_result();
        return false;
    }
    int nRet =
        memset_s(m_utf_strings, sizeof(const char **) * m_number_of_rows, 0, sizeof(const char **) * m_number_of_rows);
    securec_check_c(nRet, "\0", "\0");
    for (int i = DATA_INDEX; i < array_length; ++i) {
        jobjectArray data_row = (jobjectArray)get_jni_env()->GetObjectArrayElement(m_result_object, i);
        int number_of_fields_in_row = get_jni_env()->GetArrayLength(data_row);
        if (number_of_fields_in_row != m_number_of_fields) {
            fprintf(stderr, "Client encryption cache query: '%s' failed, m_number_of_fields mismatch\n", query);
            clear_result();
            return false;
        }
        // convert all jstrings to native. release upon destruction
        m_utf_strings[i - DATA_INDEX] = new const char *[number_of_fields_in_row];
        if (m_utf_strings == NULL) {
            fprintf(stderr, "Client encryption cache query: '%s' failed, \
                    failed to allocate memory for number_of_fields_in_row\n", query);
            clear_result();
            return false;
        }
        int nRet = memset_s(m_utf_strings[i - DATA_INDEX], sizeof(const char *) * number_of_fields_in_row, 0,
            sizeof(const char *) * number_of_fields_in_row);
        securec_check_c(nRet, "\0", "\0");
        for (int j = 0; j < number_of_fields_in_row; ++j) {
            jstring java_string = (jstring)get_jni_env()->GetObjectArrayElement(data_row, j);
            m_utf_strings[i - DATA_INDEX][j] = get_jni_env()->GetStringUTFChars(java_string, NULL);
        }
    }
    return true;
}

/* *
 * @return pointer to the Java run time
 */
JNIEnv *JNIConnCursor::get_jni_env() const
{
    return m_conn->client_logic->m_data_fetcher_manager->get_java_env();
}

/* *
 * @return pointer to the JDBC JNI class instance
 */
jobject JNIConnCursor::get_jdbc_cl_impl() const
{
    return m_conn->client_logic->m_data_fetcher_manager->get_jdbc_cl_impl();
}
