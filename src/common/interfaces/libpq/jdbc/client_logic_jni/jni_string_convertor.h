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
 * jni_string_convertor.h
 *
 * IDENTIFICATION
 * src/common/interfaces/libpq/jdbc/client_logic_jni/jni_string_convertor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef JNI_STRING_CONVERTOR_H_
#define JNI_STRING_CONVERTOR_H_
#ifndef ENABLE_LITE_MODE
#include <jni.h>
#endif

class JNIStringConvertor {
public:
    JNIStringConvertor();
    virtual ~JNIStringConvertor();
    void convert(JNIEnv *env_input, jstring java_string_input);
    const char *c_str = NULL;

private:
    JNIEnv *env = NULL;
    jstring java_string = NULL;
    void cleanup();
};
#endif /* JNI_STRING_CONVERTOR_H_ */
