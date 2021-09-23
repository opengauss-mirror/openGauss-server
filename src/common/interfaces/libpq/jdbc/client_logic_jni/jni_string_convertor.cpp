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
 * jni_string_convertor.cpp
 *
 * IDENTIFICATION
 * src/common/interfaces/libpq/jdbc/client_logic_jni/jni_string_convertor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "jni_string_convertor.h"

JNIStringConvertor::JNIStringConvertor() {}

JNIStringConvertor::~JNIStringConvertor()
{
    cleanup();
}

void JNIStringConvertor::cleanup()
{
    if (env != NULL && c_str != NULL) {
        env->ReleaseStringUTFChars(java_string, c_str); /* release resources */
    }
}

void JNIStringConvertor::convert(JNIEnv *env_input, jstring java_string_input)
{
    cleanup();
    env = env_input;
    java_string = java_string_input;
    c_str = env->GetStringUTFChars(java_string, NULL);
}
