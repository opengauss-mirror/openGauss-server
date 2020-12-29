/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 * 
 * hotpatch_inner.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/hotpatch/hotpatch_inner.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef HOTPATCH_INNER_H
#define HOTPATCH_INNER_H

#define hp_check_intval(errno, express, file, line)  \
    do {                                             \
        if (errno < 0) {                             \
            fprintf(stderr,                          \
                "%s:%d failed on calling "           \
                "security function.errno is 0x%x\n", \
                file,                                \
                line,                                \
                errno);                              \
            express;                                 \
        }                                            \
    } while (0)

#define securec_check_hp(errno, express) hp_check_intval(errno, express, __FILE__, __LINE__)

typedef enum TIME_USAGE {
    TIME_STRING_FOR_FILE_NAME,
    TIME_STRING_FOR_DISPLAY,
} TIME_USAGE_T;
#endif
