/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * org_postgresql_jdbc_ClientLogicImpl.cpp
 *
 * IDENTIFICATION
 * src/common/interfaces/libpq/jdbc/client_logic_jni/org_postgresql_jdbc_ClientLogicImpl.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "client_logic_jni.h"
#include "org_postgresql_jdbc_ClientLogicImpl.h"
#ifndef ENABLE_LITE_MODE
#include <jni.h>
#endif
#include <stdio.h>
#include "libpq-fe.h"
#include "jni_logger.h"
#include "jni_string_convertor.h"
#include "jni_util.h"
#include "jni_logger.h"
#include "client_logic_data_fetcher/data_fetcher_manager.h"

#define DELETE_ARRAY(ptr) \
    if (ptr != NULL) {    \
        delete[] ptr;     \
        ptr = NULL;       \
    }

static bool adjust_param_valid(const char *adjusted_param_value, const char* param_value)
{
    return adjusted_param_value && param_value &&
            strcmp(adjusted_param_value, param_value) != 0;
}

static bool check_pre_param_valid(JNIEnv *env, jstring statement_name_java, jobjectArray parameters_java)
{
    if (env == NULL || statement_name_java == NULL || parameters_java == NULL) {
        return false;
    }
    return true;
}

/*
 * Placeholder for few usefull methods in JNI pluming
 */
struct JniResult {
    JniResult(JNIEnv *env, int array_length) : m_env(env), m_array_length(array_length) {}
    /* *
     * Initializes the array to return
     * @return false on any failure or true on success
     */
    bool init()
    {
        if (m_env == NULL) {
            return false; /* Should never happen */
        }
        object_class = m_env->FindClass("java/lang/Object");
        if (object_class == NULL) {
            return false; /* Should never happen */
        }
        array = m_env->NewObjectArray(m_array_length, object_class, NULL);
        if (array == NULL) {
            return false; /* Should never happen */
        }
        return true;
    }
    /* *
     * Set the array to return error
     * @param status
     */
    void set_error_return(DriverError *status) const
    {
        if (status == NULL) {
            return;
        }
        set_error(m_env, object_class, array, status->get_error_code(),
                  status->get_error_message() ? status->get_error_message() : "");
    }
    /* *
     * sets to return OK success
     */
    void set_no_error_retrun() const
    {
        set_no_error(m_env, object_class, array);
    }
    /*
     * Convert java string to utf8 char * using JNIStringConvertor and handles errors if any
     * @param string_convertor string converter pointer
     * @param java_str the java string to convert
     * @param status
     * @param failure_message message to put in log for failures
     * @return true for success and false for failures
     */
    bool convert_string(JNIStringConvertor *string_convertor, jstring java_str, DriverError *status,
            const char *failure_message) const
    {
        if (string_convertor == NULL || java_str == NULL || status == NULL) {
            return false;
        }
        string_convertor->convert(m_env, java_str);
        if (string_convertor->c_str == NULL) {
            status->set_error(JNI_SYSTEM_ERROR_CODES::STRING_CREATION_FAILED);
            set_error_return(status);
            JNI_LOG_ERROR("string conversion failed :%s", failure_message ? failure_message : "");
            return false;
        }
        return true;
    }

    bool from_handle(long handle, ClientLogicJNI **client_logic, DriverError *status, const char *failure_message) const
    {
        if (!ClientLogicJNI::from_handle(handle, client_logic, status) || *client_logic == NULL) {
            JNI_LOG_ERROR("From handle failed: %ld, on: %s", (long)handle, failure_message ? failure_message : "");
            set_error_return(status);
            return false;
        }
        return true;
    }

    JNIEnv *m_env;
    int m_array_length;
    jobjectArray array = NULL;
    jclass object_class = NULL;
};

/* *
 * Links the client logic object with its libpq conn to the Java PgConnection Instance
 * @param env pointer to JVM
 * @param jdbc_cl_impl pointer back to the Java client logic impl instance
 * @return java array
 * [0][0] - int status code - zero for success
 * [0][1] - string status description
 * [1] - long - instance handle to be re-used in future calls by the same connection
 */
JNIEXPORT jobjectArray JNICALL Java_org_postgresql_jdbc_ClientLogicImpl_linkClientLogicImpl(JNIEnv *env,
    jobject jdbc_cl_impl, jstring database_name_java)
{
    JniResult result(env, 2);
    if (!result.init()) {
        return result.array; /* Should never happen */
    }
    if (env == NULL || jdbc_cl_impl == NULL) {
        return result.array; /* Should never happen */
    }
    DriverError status(0, "");

    // Link the client logic object
    ClientLogicJNI *client_logic_jni = NULL;
    client_logic_jni = new (std::nothrow) ClientLogicJNI();
    if (client_logic_jni == NULL) {
        status.set_error(UNEXPECTED_ERROR);
        JNI_LOG_ERROR("linkClientLogicImpl failed");
    } else {
        DriverError status(0, "");

        JNIStringConvertor database_name;
        database_name.convert(env, database_name_java);
        if (!client_logic_jni->link_client_logic(env, jdbc_cl_impl, database_name.c_str, &status)) {
            delete client_logic_jni;
            client_logic_jni = NULL;
            result.set_error_return(&status);
        } else {
            // Successful Connection
            result.set_no_error_retrun();
            jlong handle = (jlong)(intptr_t)client_logic_jni;
            place_jlong_in_target_array(env, handle, 1, result.array);
        }
    }
    return result.array;
}

JNIEXPORT jobjectArray JNICALL Java_org_postgresql_jdbc_ClientLogicImpl_setKmsInfoImpl(JNIEnv *env, jobject, 
    jlong handle, jstring key_java, jstring value_java)
{
    JNIStringConvertor key;
    JNIStringConvertor value;

    JniResult result(env, 1);
    if (!result.init()) {
        return result.array; /* Should never happen */
    }
    DriverError status(0, "");
    ClientLogicJNI *client_logic_jni = NULL;
    if (!result.from_handle((long)handle, &client_logic_jni, &status, "setKmsInfoImpl")) {
        return result.array;
    }

    if (!result.convert_string(&key, key_java, &status, "setKmsInfo dump kms info")) {
        return result.array;
    }
    if (!result.convert_string(&value, value_java, &status, "setKmsInfo dump kms info")) {
        return result.array;
    }

    if (client_logic_jni->set_kms_info(key.c_str, value.c_str)) {
        result.set_no_error_retrun();
    } else {
        status.set_error(INVALID_INPUT_PARAMETER, get_cmkem_errmsg(CMKEM_CHECK_INPUT_AUTH_ERR));
        result.set_error_return(&status);
    }

    return result.array;
}

/*
 * Runs the pre query, to replace client logic field values with binary format before sending the query to the database
 * server
 * @param env pointer to jvm
 * @param
 * @param handle pointer to ClientLogicJNI instance
 * @param original_query_java the query with potentially client logic values in user format
 * @return java array
 * [0][0] - int status code - zero for success
 * [0][1] - string status description
 * [1] - String - The modified query
 */
JNIEXPORT jobjectArray JNICALL Java_org_postgresql_jdbc_ClientLogicImpl_runQueryPreProcessImpl(JNIEnv *env,
    jobject jdbc_cl_impl, jlong handle, jstring original_query_java)
{
    JniResult result(env, 2);
    if (!result.init()) {
        return result.array; /* Should never happen */
    }
    if (env == NULL || original_query_java == NULL) {
        return result.array; /* Should never happen */
    }
    DriverError status(0, "");
    ClientLogicJNI *client_logic = NULL;
    if (!result.from_handle((long)handle, &client_logic, &status, "runQueryPreProcess")) {
        JNI_LOG_ERROR("no handle? %s", env->GetStringUTFChars(original_query_java, NULL));
        return result.array;
    }
    JNIStringConvertor original_query;

    original_query.convert(env, original_query_java);
    if (original_query.c_str == NULL) {
        status.set_error(JNI_SYSTEM_ERROR_CODES::STRING_CREATION_FAILED);
        result.set_error_return(&status);
        JNI_LOG_ERROR("Java_org_postgresql_jdbc_ClientLogicImpl_runQueryPreProcessImpl error code:%d text:'%s'",
                      status.get_error_code(), status.get_error_message() ? status.get_error_message() : "");
        return result.array;
    }
    const char *original_query_dup = client_logic->get_new_query(original_query.c_str);
    if (original_query_dup == NULL) {
        status.set_error(JNI_SYSTEM_ERROR_CODES::STRING_CREATION_FAILED);
        result.set_error_return(&status);
        JNI_LOG_ERROR("Java_org_postgresql_jdbc_ClientLogicImpl_runQueryPreProcessImpl error code:%d text:'%s'",
                      status.get_error_code(), status.get_error_message() ? status.get_error_message() : "");
        result.set_error_return(&status);
        return result.array;
    }
    client_logic->set_jni_env_and_cl_impl(env, jdbc_cl_impl);
    if (!client_logic->run_pre_query(original_query_dup, &status)) {
        JNI_LOG_ERROR(
            "Java_org_postgresql_jdbc_ClientLogicImpl_runQueryPreProcessImpl failed: %ld, error code: %d error: '%s'",
            (long)handle, status.get_error_code(), status.get_error_message() ? status.get_error_message() : "");
        result.set_error_return(&status);
        return result.array;
    }
    result.set_no_error_retrun();
    place_string_in_array(env, client_logic->get_pre_query_result(), 1, result.array);
    client_logic->clean_stmnt();
    return result.array;
}

/*
 * Runs post process on the backend, to free the client logic state machine when a query is done
 * @param env pointer to jvm
 * @param
 * @param handle pointer to ClientLogicJNI instance
 * @param statament_name_java when issued for prepared statement contains the statement name, otherwise an empty string
 * @return java array
 * [0][0] - int status code - zero for success
 * [0][1] - string status description
 */
JNIEXPORT jobjectArray JNICALL Java_org_postgresql_jdbc_ClientLogicImpl_runQueryPostProcessImpl(JNIEnv *env,
    jobject jdbc_cl_impl, jlong handle)
{
    JniResult result(env, 1);
    if (!result.init()) {
        return result.array; /* Should never happen */
    }
    DriverError status(0, "");
    ClientLogicJNI *client_logic = NULL;
    if (!result.from_handle((long)handle, &client_logic, &status, "runQueryPostProcess")) {
        return result.array;
    }
    client_logic->set_jni_env_and_cl_impl(env, jdbc_cl_impl);
    if (JNI_TRUE == env->IsSameObject(jdbc_cl_impl, NULL)) {
        fprintf(stderr, "Client encryption run_post_query failed jobject %p was invalid\n", jdbc_cl_impl);
    }
    if (!client_logic->run_post_query("", &status)) {
        JNI_LOG_ERROR("run_post_query failed: %ld, error code: %d error: '%s'", (long)handle, status.get_error_code(),
            status.get_error_message() ? status.get_error_message() : "");
        result.set_error_return(&status);
        return result.array;
    }
    result.set_no_error_retrun();
    return result.array;
}

/*
 * Replace client logic field value with user input - used when receiving data in a resultset
 * @param env pointer to jvm
 * @param
 * @param handle pointer to ClientLogicJNI instance
 * @param data_to_process_java the data in binary format (hexa)
 * @param data_type the oid (modid) of the original field type
 * @return java array with the format below:
 * [0][0] - int status code - zero for success
 * [0][1] - string status description
 * [1] - String - The data in user format
 */
JNIEXPORT jobjectArray JNICALL Java_org_postgresql_jdbc_ClientLogicImpl_runClientLogicImpl(JNIEnv *env,
    jobject jdbc_cl_impl, jlong handle, jstring data_to_process_java, jint data_type)
{
    JniResult result(env, 2);
    if (!result.init()) {
        return result.array; /* Should never happen */
    }

    if (env == NULL || data_to_process_java == NULL) {
        return result.array;
    }
    DriverError status(0, "");
    JNIStringConvertor data_to_process;
    if (!result.convert_string(&data_to_process, data_to_process_java, &status, "runClientLogicImpl")) {
        return result.array;
    }

    ClientLogicJNI *client_logic = NULL;
    if (!result.from_handle((long)handle, &client_logic, &status, "runClientLogicImpl")) {
        return result.array;
    }
    client_logic->set_jni_env_and_cl_impl(env, jdbc_cl_impl);
    unsigned char *proccessed_data = NULL;
    size_t length_output;
    if (!client_logic->deprocess_value(data_to_process.c_str, data_type, &proccessed_data, length_output, &status)) {
        libpq_free(proccessed_data);
        JNI_LOG_ERROR("Java_org_postgresql_jdbc_ClientLogicImpl_runClientLogicImpl failed:error code: %d error: '%s'",
                      status.get_error_code(), status.get_error_message() ? status.get_error_message() : "");
        result.set_error_return(&status);
        return result.array;
    }
    result.set_no_error_retrun();
    place_ustring_in_array(env, proccessed_data, 1, result.array);
    libpq_free(proccessed_data);
    return result.array;
}

/**
 * check if a type is holding a record type and then need to pass its values to process_record_data
 * In Java this is done once for every field in the resultset and
 * then we call process_record_data only for the fields that we need to
 * @param env pointer to jvm
 * @param
 * @param handle pointer to ClientLogicJNI instance
 * @param column_name_java the name of the column in the resultset
 * @param oid the oid of the field type
 * @return java array with the format below:
 * [0][0] - int status code - zero for success
 * [0][1] - string status description
 * [0...n] - array of ints with actual oids that the field contain
*/
JNIEXPORT jobjectArray JNICALL Java_org_postgresql_jdbc_ClientLogicImpl_getRecordIDsImpl(JNIEnv *env, jobject,
    jlong handle, jstring column_name_java, jint oid)
{
    DriverError status(0, "");
    JniResult result(env, 2);
    if (!result.init()) {
        return result.array; /* Should never happen */
    }

    if (env == NULL || column_name_java == NULL) {
        return result.array;
    }
    JNIStringConvertor column_type_name;
    if (!result.convert_string(&column_type_name, column_name_java, &status, "getRecordIDsImpl")) {
        return result.array;
    }

    ClientLogicJNI *client_logic = NULL;
    if (!result.from_handle((long)handle, &client_logic, &status, "getRecordIDsImpl")) {
        return result.array;
    }
    result.set_no_error_retrun();
    size_t number_of_oids = client_logic->get_record_data_oid_length(oid, column_type_name.c_str);
    if (number_of_oids > 0) {
        const int *oids = client_logic->get_record_data_oids(oid, column_type_name.c_str);
        place_ints_in_target_array(env, oids, number_of_oids, 1, result.array);
    }
    return result.array;
}

/**
 * gets a record in client logic format and returns it in user format
 * @param env pointer to jvm
 * @param
 * @param handle pointer to ClientLogicJNI instance
 * @param data_to_process_java the data in client logic format
 * @param original_oids_java the result returned from getRecordIDsImpl method for this field
 * @return java array with the format below:
 * [0][0] - int status code - zero for success
 * [0][1] - string status description
 * [1] - int 0 not client logic 1 - is client logic
 * [2] - String - The data in user format
*/
JNIEXPORT jobjectArray JNICALL Java_org_postgresql_jdbc_ClientLogicImpl_runClientLogic4RecordImpl(JNIEnv *env, jobject,
    jlong handle, jstring data_to_process_java, jintArray original_oids_java)
{
    JniResult result(env, ARRAY_SIZE + 1);
    if (!result.init()) {
        return result.array; /* Should never happen */
    }
    if (env == NULL || data_to_process_java == NULL || original_oids_java == NULL) {
        return result.array;
    }
    DriverError status(0, "");
    JNIStringConvertor data_to_process;
    if (!result.convert_string(&data_to_process, data_to_process_java, &status, "runClientLogic4RecordImpl")) {
        return result.array;
    }
    ClientLogicJNI *client_logic = NULL;
    if (!result.from_handle((long)handle, &client_logic, &status, "runClientLogic4RecordImpl")) {
        return result.array;
    }
    bool is_client_logic = false;
    unsigned char *proccessed_data = NULL;
    size_t length_output;
    size_t number_of_oids = env->GetArrayLength(original_oids_java);
    if (number_of_oids > 0) {
        /* Gauss wil not allow more than 250 fields with client logic */
        static const size_t MAXMIMUM_NUMBER_OF_CL_FIELDS_IN_TABLE = 250;
        Assert(number_of_oids < MAXMIMUM_NUMBER_OF_CL_FIELDS_IN_TABLE);
        int original_oids[MAXMIMUM_NUMBER_OF_CL_FIELDS_IN_TABLE] = {0};
        jint *oids_java = env->GetIntArrayElements(original_oids_java, 0);
        for (size_t index = 0; index < number_of_oids; ++index) {
            original_oids[index] = oids_java[index];
        }
        if (!client_logic->process_record_data(data_to_process.c_str, original_oids, number_of_oids,
            &proccessed_data, &is_client_logic, length_output, &status)) {
            libpq_free(proccessed_data);
            JNI_LOG_ERROR(
                "Java_org_postgresql_jdbc_ClientLogicImpl_runClientLogic4RecordImpl failed:error code: %d error: '%s'",
                status.get_error_code(), status.get_error_message() ? status.get_error_message() : "");
            result.set_error_return(&status);
            return result.array;
        }
        result.set_no_error_retrun();
    }
    if (is_client_logic) {
        place_int_in_target_array(env, 1, 1, result.array);
    } else {
        place_int_in_target_array(env, 0, 1, result.array);
    }
    if (proccessed_data != NULL) {
        place_ustring_in_array(env, proccessed_data, 2, result.array);
        libpq_free(proccessed_data);
    }
    return result.array;
}

/*
 * Run prepare statement
 * @param env pointer to jvm
 * @param
 * @param handle pointer to ClientLogicJNI instance
 * @param query_java
 * @param statement_name
 * @param parameter_count
 * @return Object in the following format:
 * [0][0][0] - error code - 0 for none
 * [0][1] - error text - if none, empty string
 * [1] - modified query
 */
JNIEXPORT jobjectArray JNICALL Java_org_postgresql_jdbc_ClientLogicImpl_prepareQueryImpl(JNIEnv *env,
    jobject jdbc_cl_impl, jlong handle, jstring query_java, jstring statement_name_java, jint parameter_count)
{
    JniResult result(env, 2);
    if (!result.init()) {
        return result.array;
    }
    if (env == NULL || statement_name_java == NULL || query_java == NULL) {
        return result.array;
    }
    DriverError status(0, "");
    ClientLogicJNI *client_logic = NULL;
    if (!result.from_handle((long)handle, &client_logic, &status, "prepareQuery")) {
        return result.array;
    }
    JNIStringConvertor original_query;

    original_query.convert(env, query_java);
    if (original_query.c_str == NULL) {
        status.set_error(JNI_SYSTEM_ERROR_CODES::STRING_CREATION_FAILED);
        result.set_error_return(&status);
        JNI_LOG_ERROR("prepareQuery failed getting the query string error code:%d text:'%s'", status.get_error_code(),
                      status.get_error_message() ? status.get_error_message() : "");
        return result.array;
    }
    JNIStringConvertor statement_name;
    if (!result.convert_string(&statement_name, statement_name_java, &status, "prepareQuery")) {
        return result.array;
    }
    client_logic->set_jni_env_and_cl_impl(env, jdbc_cl_impl);
    if (!client_logic->preare_statement(original_query.c_str, statement_name.c_str, parameter_count, &status)) {
        JNI_LOG_ERROR("preare_statement call failed: %ld, error code: %d error: '%s'", (long)handle,
                      status.get_error_code(), status.get_error_message() ? status.get_error_message() : "");
        result.set_error_return(&status);
        return result.array;
    }
    if (client_logic->get_statement_data() == NULL) {
        status.set_error(STATEMENT_DATA_EMPTY);
        JNI_LOG_ERROR("preare_statement get_statement_data call failed: %ld, error code: %d error: '%s'", (long)handle,
                      status.get_error_code(), status.get_error_message() ? status.get_error_message() : "");
        result.set_error_return(&status);
        return result.array;
    }
    result.set_no_error_retrun();
    place_string_in_array(env, client_logic->get_statement_data()->params.adjusted_query, 1, result.array);
    return result.array;
}

/*
 * Replaces parameters in prepare statement values before sending to execution
 * @param[in] env JVM environment
 * @param[in]
 * @param[in] handle pointer to ClientLogicJNI instance
 * @param[in] statement_name_java statement name
 * @param[in] parameters_java list of parameters in form of Java array of strings
 * @param[in] parameter_count number of parameters
 * @return Object in the following format:
 * [0][0] - error code - 0 for none
 * [0][1][0] - error text - if none, empty string
 * [1][0 ... parameter_count - 1] - array with the parameters value, if the parameter is not being replace a NULL apears
 * otherwise the replaced value
 * [2][0 ... parameter_count - 1] - array with the parameters' type-oids,
 * if the parameter is being replaced, otherwise 0
 */
JNIEXPORT jobjectArray JNICALL Java_org_postgresql_jdbc_ClientLogicImpl_replaceStatementParamsImpl(JNIEnv *env,
    jobject jdbc_cl_impl, jlong handle, jstring statement_name_java, jobjectArray parameters_java)
{
    JniResult result(env, 3);
    if (!result.init()) {
        return result.array;
    }
    if (!check_pre_param_valid(env, statement_name_java, parameters_java)) {
        return result.array;
    }

    DriverError status(0, "");
    JNIStringConvertor statement_name;
    if (!result.convert_string(&statement_name, statement_name_java, &status,
        "replaceStatementParams statement_name_java")) {
        return result.array;
    }
    ClientLogicJNI *client_logic = NULL;
    if (!result.from_handle((long)handle, &client_logic, &status, "replaceStatementParams")) {
        return result.array;
    }
    int parameter_count = env->GetArrayLength(parameters_java);

    bool convert_failure = false;
    const char **param_values = NULL;
    param_values = new (std::nothrow) const char *[(size_t)parameter_count];
    if (param_values == NULL) {
        status.set_error(JNI_SYSTEM_ERROR_CODES::UNEXPECTED_ERROR);
        result.set_error_return(&status);
        JNI_LOG_ERROR("out of memory");
        return result.array;
    }
    JNIStringConvertor *string_convertors = NULL;
    string_convertors = new (std::nothrow) JNIStringConvertor[(size_t)parameter_count];
    if (string_convertors == NULL) {
        delete[] param_values;
        status.set_error(JNI_SYSTEM_ERROR_CODES::UNEXPECTED_ERROR);
        result.set_error_return(&status);
        JNI_LOG_ERROR("out of memory");
        return result.array;
    }
    for (int i = 0; i < parameter_count && !convert_failure; ++i) {
        jstring value = (jstring)env->GetObjectArrayElement(parameters_java, i);
        string_convertors[i].convert(env, value);
        if (string_convertors[i].c_str == NULL) {
            status.set_error(JNI_SYSTEM_ERROR_CODES::STRING_CREATION_FAILED);
            result.set_error_return(&status);
            JNI_LOG_ERROR("replaceStatementParams failed getting the parameter at index %d", i);
            convert_failure = true;
        } else {
            param_values[i] = string_convertors[i].c_str;
        }
    }
    if (convert_failure) {
        delete[] param_values;
        delete[] string_convertors;
        return result.array;
    }
    client_logic->set_jni_env_and_cl_impl(env, jdbc_cl_impl);
    if (!client_logic->replace_statement_params(statement_name.c_str, param_values, parameter_count, &status)) {
        JNI_LOG_ERROR("replace_statement_params failed: %ld, error code: %d error: '%s'", (long)handle,
                      status.get_error_code(), status.get_error_message() ? status.get_error_message() : "");
        result.set_error_return(&status);
        delete[] param_values;
        delete[] string_convertors;
        return result.array;
    }

    // After parameters values replaces, place the new values on the target array
    jobjectArray parameters_array = env->NewObjectArray(parameter_count, result.object_class, NULL);
    const StatementData *stmnt_data = client_logic->get_statement_data();

    int *parameters_types_array = NULL;
    if (stmnt_data->params.adjusted_paramTypes != NULL) {
        // set adjusted types array to return to JNI
        parameters_types_array = new (std::nothrow) int[(size_t)parameter_count];
        if (parameters_types_array == NULL) {
            delete[] param_values;
            delete[] string_convertors;
            status.set_error(JNI_SYSTEM_ERROR_CODES::UNEXPECTED_ERROR);
            result.set_error_return(&status);
            JNI_LOG_ERROR("new failed");
            return result.array;
        }
    }

    for (int i = 0; i < parameter_count && !convert_failure; ++i) {
        /*
         * rawValue in INSERT could be NULL or empty string,
         * we have recgonize it in preare_statement routine and make adjusted_param_values[idx]
         * to the NULL value directly, therefore ignore it here just like the normal empty value.
         */
        if (adjust_param_valid(stmnt_data->params.adjusted_param_values[i], param_values[i])) {
            place_string_in_array(env, stmnt_data->params.adjusted_param_values[i], i, parameters_array);
        }
        /* return type oid for jdbc side distinguish the enc param */
        if (parameters_types_array != NULL) {
            parameters_types_array[i] = (int)stmnt_data->params.adjusted_paramTypes[i];
        }
    }

    /* [1][...] arryay object */
    env->SetObjectArrayElement(result.array, 1, parameters_array);
    /* [2][...] arryay object */
    place_ints_in_target_array(env, parameters_types_array, parameter_count, 2, result.array);
    delete[] param_values;
    delete[] string_convertors;
    client_logic->clean_stmnt();
    DELETE_ARRAY(parameters_types_array);
    result.set_no_error_retrun();
    return result.array;
}

/*
 * replace client logic values in error message coming from the server
 * For example, when inserting a duplicate unique value,
 * it will change the client logic value of \x... back to the user input
 * @param env java environment
 * @param
 * @param handle client logic instance handle
 * @param original_message_java the message received from the server and need to be converted
 * @return Object in the following format:
 * [0][0] - error code - 0 for none
 * [0][1][0] - error text - if none, empty string
 * [1] - converted message - if empty then the message has not changed
 */
JNIEXPORT jobjectArray JNICALL Java_org_postgresql_jdbc_ClientLogicImpl_replaceErrorMessageImpl(JNIEnv *env,
    jobject jdbc_cl_impl, jlong handle, jstring original_message_java)
{
    JniResult result(env, 2);
    if (!result.init()) {
        return result.array;
    }
    if (env == NULL || original_message_java == NULL) {
        return result.array;
    }
    DriverError status(0, "");
    // Find the client logic instance:
    ClientLogicJNI *client_logic = NULL;
    if (!result.from_handle((long)handle, &client_logic, &status, "replaceErrorMessage")) {
        return result.array;
    }
    JNIStringConvertor original_message;
    if (!result.convert_string(&original_message, original_message_java, &status, "replaceErrorMessage")) {
        return result.array;
    }
    client_logic->set_jni_env_and_cl_impl(env, jdbc_cl_impl);
    char *converted_message = NULL;
    bool retval = client_logic->replace_message(original_message.c_str, &converted_message, &status);
    if (!retval) {
        // returning false due to an error
        if (converted_message != NULL) {
            free(converted_message);
            converted_message = NULL;
        }
        if (status.get_error_code() != 0) {
            JNI_LOG_ERROR("replaceErrorMessage failed: %ld, error code: %d error: '%s'", (long)handle,
                          status.get_error_code(), status.get_error_message() ? status.get_error_message() : "");
            result.set_error_return(&status);
            return result.array;
        }
        // returning false may be due to not finding anything to replace and then the string is empty
        result.set_no_error_retrun();
        place_string_in_array(env, "", 1, result.array);
        return result.array;
    }

    result.set_no_error_retrun();
    if (converted_message == NULL) {
        /* Should not happen, but just in case */
        place_string_in_array(env, "", 1, result.array);
    } else {
        place_string_in_array(env, converted_message, 1, result.array);
        free(converted_message);
        converted_message = NULL;
    }
    return result.array;
}
/**
 * reloads the client logic cache
 * @param env java environment
 * @param jdbc_cl_impl pointer back to the Java client logic impl instance
 * @param handle client logic instance handle
 */
JNIEXPORT void JNICALL Java_org_postgresql_jdbc_ClientLogicImpl_reloadCacheImpl (JNIEnv *env,
    jobject jdbc_cl_impl, jlong handle)
{
    ClientLogicJNI *client_logic = NULL;
    DriverError status(0, "");
    if (!ClientLogicJNI::from_handle(handle, &client_logic, &status) || client_logic == NULL) {
        JNI_LOG_DEBUG("reloadCacheImpl failed: %ld, error code: %d error: '%s'", (long)handle, status.get_error_code(),
                status.get_error_message() ? status.get_error_message() : "");
        return;
    }
    client_logic->set_jni_env_and_cl_impl(env, jdbc_cl_impl);
    client_logic->reload_cache();
}

/**
 * reloads the client logic cache ONLY if the server timestamp is later than the client timestamp
 * @param env java environment
 * @param jdbc_cl_impl pointer back to the Java client logic impl instance
 * @param handle client logic instance handle
 */
JNIEXPORT void JNICALL Java_org_postgresql_jdbc_ClientLogicImpl_reloadCacheIfNeededImpl (JNIEnv *env,
		jobject jdbc_cl_impl, jlong handle){
    ClientLogicJNI *client_logic = NULL;
    DriverError status(0, "");
    if (!ClientLogicJNI::from_handle(handle, &client_logic, &status) || client_logic == NULL){
        JNI_LOG_DEBUG("reloadCacheIfNeededImpl failed: %ld, error code: %d error: '%s'",
        (long)handle, status.get_error_code(),
		status.get_error_message() ? status.get_error_message() : "");
        return;
    }
    client_logic->set_jni_env_and_cl_impl(env, jdbc_cl_impl);
    client_logic->reload_cache_if_needed();
}

/*
 * Class:     org_postgresql_jdbc_ClientLogicImpl
 * Method:    destroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_postgresql_jdbc_ClientLogicImpl_destroy(JNIEnv *env, jobject, jlong handle)
{
    JNI_LOG_DEBUG("About to destroy handle: %ld", (long)handle);
    ClientLogicJNI *client_logic = NULL;
    DriverError status(0, "");
    if (!ClientLogicJNI::from_handle(handle, &client_logic, &status) || client_logic == NULL) {
        JNI_LOG_DEBUG("Destroy failed: %ld, error code: %d error: '%s'", (long)handle, status.get_error_code(),
                status.get_error_message() ? status.get_error_message() : "");
        return;
    } else {
        delete client_logic;
        client_logic = NULL;
        JNI_LOG_DEBUG("Handle destroyed: %ld", (long)handle);
    }
    return;
}
