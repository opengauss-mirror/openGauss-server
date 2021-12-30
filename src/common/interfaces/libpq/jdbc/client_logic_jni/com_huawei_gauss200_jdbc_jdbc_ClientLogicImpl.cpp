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
 * com_huawei_gauss200_jdbc_jdbc_ClientLogicImpl.cpp
 *
 * IDENTIFICATION
 * src/common/interfaces/libpq/jdbc/client_logic_jni/com_huawei_gauss200_jdbc_jdbc_ClientLogicImpl.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "com_huawei_gauss200_jdbc_jdbc_ClientLogicImpl.h"
#include "org_postgresql_jdbc_ClientLogicImpl.h"

JNIEXPORT jobjectArray JNICALL Java_com_huawei_gauss200_jdbc_jdbc_ClientLogicImpl_linkClientLogicImpl(JNIEnv *env,
    jobject jdbc_cl_impl, jstring database_name_java)
{
    return Java_org_postgresql_jdbc_ClientLogicImpl_linkClientLogicImpl(env, jdbc_cl_impl, database_name_java);
}

JNIEXPORT jobjectArray JNICALL Java_com_huawei_gauss200_jdbc_jdbc_ClientLogicImpl_setKmsInfo(JNIEnv *env,
    jobject java_object, jlong handle, jstring key, jstring value)
{
    return Java_org_postgresql_jdbc_ClientLogicImpl_setKmsInfoImpl(env, java_object, handle, key, value);
}

JNIEXPORT jobjectArray JNICALL Java_com_huawei_gauss200_jdbc_jdbc_ClientLogicImpl_runQueryPreProcessImpl(JNIEnv *env,
    jobject java_object, jlong handle, jstring original_query_java)
{
    return Java_org_postgresql_jdbc_ClientLogicImpl_runQueryPreProcessImpl(env, java_object, handle,
        original_query_java);
}

JNIEXPORT jobjectArray JNICALL Java_com_huawei_gauss200_jdbc_jdbc_ClientLogicImpl_runQueryPostProcessImpl(JNIEnv *env,
    jobject java_object, jlong handle)
{
    return Java_org_postgresql_jdbc_ClientLogicImpl_runQueryPostProcessImpl(env, java_object, handle);
}

JNIEXPORT jobjectArray JNICALL Java_com_huawei_gauss200_jdbc_jdbc_ClientLogicImpl_runClientLogicImpl(JNIEnv *env,
    jobject java_object, jlong handle, jstring data_to_process_java, jint data_type)
{
    return Java_org_postgresql_jdbc_ClientLogicImpl_runClientLogicImpl(env, java_object, handle, data_to_process_java,
        data_type);
}

JNIEXPORT jobjectArray JNICALL JJava_com_huawei_gauss200_jdbc_jdbc_ClientLogicImpl_getRecordIDsImpl (JNIEnv *env,
    jobject java_object, jlong handle, jstring column_name_java, jint oid)
{
    return Java_org_postgresql_jdbc_ClientLogicImpl_getRecordIDsImpl(env, java_object, handle, column_name_java, oid);
}

JNIEXPORT jobjectArray JNICALL Java_com_huawei_gauss200_jdbc_jdbc_ClientLogicImpl_runClientLogic4RecordImpl(JNIEnv *env,
    jobject java_object, jlong handle, jstring data_to_process_java, jintArray original_oids_java)
{
    return Java_org_postgresql_jdbc_ClientLogicImpl_runClientLogic4RecordImpl(env, java_object, handle,
        data_to_process_java, original_oids_java);
}

JNIEXPORT jobjectArray JNICALL Java_com_huawei_gauss200_jdbc_jdbc_ClientLogicImpl_prepareQueryImpl(JNIEnv *env,
    jobject java_object, jlong handle, jstring query_java, jstring statement_name_java, jint parameter_count)
{
    return Java_org_postgresql_jdbc_ClientLogicImpl_prepareQueryImpl(env, java_object, handle, query_java,
        statement_name_java, parameter_count);
}

JNIEXPORT jobjectArray JNICALL Java_com_huawei_gauss200_jdbc_jdbc_ClientLogicImpl_replaceStatementParamsImpl(
    JNIEnv *env, jobject java_object, jlong handle, jstring statement_name_java, jobjectArray parameters_java)
{
    return Java_org_postgresql_jdbc_ClientLogicImpl_replaceStatementParamsImpl(env, java_object, handle,
        statement_name_java, parameters_java);
}

/*
 * Class:     com_huawei_gauss200_jdbc_jdbc_ClientLogicImpl
 * Method:    replaceErrorMessageImpl
 * Signature: (JLjava/lang/String;)[Ljava/lang/Object;
 */
JNIEXPORT jobjectArray JNICALL Java_com_huawei_gauss200_jdbc_jdbc_ClientLogicImpl_replaceErrorMessageImpl(JNIEnv *env,
    jobject java_object, jlong handle, jstring message)
{
    return Java_org_postgresql_jdbc_ClientLogicImpl_replaceErrorMessageImpl(env, java_object, handle, message);
}


JNIEXPORT void JNICALL Java_com_huawei_gauss200_jdbc_jdbc_ClientLogicImpl_destroy(JNIEnv *env, jobject java_object,
    jlong handle)
{
    return Java_org_postgresql_jdbc_ClientLogicImpl_destroy(env, java_object, handle);
}
