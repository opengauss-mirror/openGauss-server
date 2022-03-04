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
 * jni_util.h
 *
 * IDENTIFICATION
 * src/common/interfaces/libpq/jdbc/client_logic_jni/jni_util.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JNI_UTIL_H_
#define JNI_UTIL_H_
#ifndef ENABLE_LITE_MODE
#include <jni.h>
#endif

const int ARRAY_SIZE = 2; /* array size */
void set_no_error(JNIEnv *env, jclass objectClass, jobjectArray arrayObject);
void set_error(JNIEnv *env, jclass objectClass, jobjectArray arrayObject, int error_code, const char *error_text);
void place_jlong_in_target_array(JNIEnv *env, jlong value, const int index2Store, jobjectArray arrayObject);
void place_int_in_target_array(JNIEnv *env, const int value, const int index2Store, jobjectArray arrayObject);
void place_ints_in_target_array(JNIEnv *env, const int *values, size_t values_length, const int index2Store,
    jobjectArray arrayObject);
void place_string_in_array(JNIEnv *env, const char *value, const int index2Store, jobjectArray arrayObject);
void place_ustring_in_array(JNIEnv *env, unsigned char *value, const int index2Store, jobjectArray arrayObject);

#endif /* JNI_UTIL_H_ */
