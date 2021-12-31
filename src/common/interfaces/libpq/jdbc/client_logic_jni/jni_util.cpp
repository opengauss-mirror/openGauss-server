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
 * jni_util.cpp
 *
 * IDENTIFICATION
 * src/common/interfaces/libpq/jdbc/client_logic_jni/jni_util.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "jni_util.h"

static const int ERROR_CODE_INDEX = 0;
static const int ERROR_TEXT_INDEX = 1;

/*
 * Handle result array for JNI, sets the no error flag in field
 * [0][0] error code to zero [0][0]
 * [0][1] error description to an empty string
 * @param env java environment
 * @param objectClass Java object class
 * @param arrayObject Java array class
 */
void set_no_error(JNIEnv *env, jclass objectClass, jobjectArray arrayObject)
{
    if (env == NULL || objectClass == NULL || arrayObject == NULL) {
        return;
    }
    jobjectArray status_object = env->NewObjectArray(ARRAY_SIZE, objectClass, NULL);
    place_int_in_target_array(env, 0, ERROR_CODE_INDEX, status_object);
    place_string_in_array(env, "", ERROR_TEXT_INDEX, status_object);
    env->SetObjectArrayElement(arrayObject, 0, status_object);
}
/*
 * Handle result array for JNI, sets the error details
 * [0][0] error code
 * [0][1] error description
 * @param env java environment
 * @param objectClass Java object class
 * @param arrayObject Java array class
 * @param error_code error number
 * @param error_text error description
 */
void set_error(JNIEnv *env, jclass objectClass, jobjectArray arrayObject, int error_code, const char *error_text)
{
    if (env == NULL || objectClass == NULL || arrayObject == NULL || error_text == NULL) {
        return;
    }
    jobjectArray status_object = env->NewObjectArray(ARRAY_SIZE, objectClass, NULL);
    place_int_in_target_array(env, error_code, ERROR_CODE_INDEX, status_object);
    place_string_in_array(env, error_text, ERROR_TEXT_INDEX, status_object);
    env->SetObjectArrayElement(arrayObject, 0, status_object);
}
/*
 * sets long value in JNI result array
 * @param env java environment
 * @param value long value
 * @param index2Store index in teh array to set
 * @param arrayObject Java array class
 */
void place_jlong_in_target_array(JNIEnv *env, const jlong value, const int index2Store, jobjectArray arrayObject)
{
    if (env == NULL || arrayObject == NULL) {
        return;
    }
    const jlong valuesArr[] = {value};
    jlongArray jlongArrayValues = env->NewLongArray(1);
    env->SetLongArrayRegion(jlongArrayValues, 0, 1, valuesArr);
    env->SetObjectArrayElement(arrayObject, index2Store, jlongArrayValues);
}
/*
 * sets integer value in JNI result array
 * @param env java environment
 * @param value long value
 * @param index2Store index in the array to set
 * @param arrayObject Java array class
 */
void place_int_in_target_array(JNIEnv *env, const int value, const int index2Store, jobjectArray arrayObject)
{
    if (env == NULL || arrayObject == NULL) {
        return;
    }
    const int valuesArr[] = {value};
    jintArray jintArrayValues = env->NewIntArray(1);
    env->SetIntArrayRegion(jintArrayValues, 0, 1, valuesArr);
    env->SetObjectArrayElement(arrayObject, index2Store, jintArrayValues);
}

/**
 * Places array of ints in a field of a target array
 * @param env java environment
 * @param values the int array
 * @param values_length the length of the int array
 * @param index2Store the index in the target array to store the int array
 * @param arrayObject the target array object
 */
void place_ints_in_target_array(JNIEnv *env, const int *values, size_t values_length, const int index2Store,
    jobjectArray arrayObject)
{
    if (env == NULL || arrayObject == NULL || values == NULL) {
        return;
    }
    jintArray jintArrayValues = env->NewIntArray(values_length);
    env->SetIntArrayRegion(jintArrayValues, 0, values_length, values);
    env->SetObjectArrayElement(arrayObject, index2Store, jintArrayValues);
}

/*
 * sets string value in JNI result array
 * @param env java environment
 * @param value long value
 * @param index2Store index in teh array to set
 * @param arrayObject Java array class
 */
void place_string_in_array(JNIEnv *env, const char *value, const int index2Store, jobjectArray arrayObject)
{
    if (env == NULL || arrayObject == NULL) {
        return;
    }
    jstring java_string_value = env->NewStringUTF(value);
    env->SetObjectArrayElement(arrayObject, index2Store, java_string_value);
}

/*
 * sets unsigned string value in JNI result array
 * @param env java environment
 * @param value long value
 * @param index2Store index in teh array to set
 * @param arrayObject Java array class
 */
void place_ustring_in_array(JNIEnv *env, unsigned char *value, const int index2Store, jobjectArray arrayObject)
{
    if (env == NULL || value == NULL || arrayObject == NULL) {
        return;
    }
    jstring java_string_value = env->NewStringUTF((char *)value);
    env->SetObjectArrayElement(arrayObject, index2Store, java_string_value);
}
