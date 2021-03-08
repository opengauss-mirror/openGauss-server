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
 * securec_check.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/securec_check.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _SECUREC_CHECK_H_
#define _SECUREC_CHECK_H_

#include "securectype.h"
#include <stdarg.h>
/* free space for error function operator with exit-function mechanism */
#define freeSecurityFuncSpace_c(str1, str2)   \
    {                                         \
        if (str1 != NULL && strlen(str1) > 0) \
            free((char*)str1);                \
        if (str2 != NULL && strlen(str2) > 0) \
            free((char*)str2);                \
    }

#ifndef GDS_SERVER
/* gds using gds_securec_check and gds_securec_check_ss */
#define securec_check_c(errno, str1, str2)                                                                            \
    {                                                                                                                 \
        errno_t e_for_securec_check_c = (errno);                                                                      \
        if (EOK != e_for_securec_check_c) {                                                                           \
            freeSecurityFuncSpace_c(str1, str2);                                                                      \
            switch (e_for_securec_check_c) {                                                                          \
                case EINVAL:                                                                                          \
                    printf("ERROR at %s : %d : The destination buffer is NULL or not terminated. The second case "    \
                           "only occures in function strcat_s/strncat_s.\n",                                          \
                        __FILE__,                                                                                     \
                        __LINE__);                                                                                    \
                    break;                                                                                            \
                case EINVAL_AND_RESET:                                                                                \
                    printf("ERROR at %s : %d : The source buffer is NULL.\n", __FILE__, __LINE__);                    \
                    break;                                                                                            \
                case ERANGE:                                                                                          \
                    printf("ERROR at %s : %d : The parameter destMax is equal to zero or larger than the macro : "    \
                           "SECUREC_STRING_MAX_LEN.\n",                                                               \
                        __FILE__,                                                                                     \
                        __LINE__);                                                                                    \
                    break;                                                                                            \
                case ERANGE_AND_RESET:                                                                                \
                    printf("ERROR at %s : %d : The parameter destMax is too small or parameter count is larger than " \
                           "macro parameter SECUREC_STRING_MAX_LEN. The second case only occures in functions "       \
                           "strncat_s/strncpy_s.\n",                                                                  \
                        __FILE__,                                                                                     \
                        __LINE__);                                                                                    \
                    break;                                                                                            \
                case EOVERLAP_AND_RESET:                                                                              \
                    printf("ERROR at %s : %d : The destination buffer and source buffer are overlapped.\n",           \
                        __FILE__,                                                                                     \
                        __LINE__);                                                                                    \
                    break;                                                                                            \
                default:                                                                                              \
                    printf("ERROR at %s : %d : Unrecognized return type.\n", __FILE__, __LINE__);                     \
                    break;                                                                                            \
            }                                                                                                         \
            exit(1);                                                                                                  \
        }                                                                                                             \
    }

#define securec_check_ss_c(errno, str1, str2)                                                                        \
    do {                                                                                                             \
        if (errno == -1) {                                                                                           \
            freeSecurityFuncSpace_c(str1, str2);                                                                     \
            printf("ERROR at %s : %d : The destination buffer or format is a NULL pointer or the invalid parameter " \
                   "handle is invoked..\n",                                                                          \
                __FILE__,                                                                                            \
                __LINE__);                                                                                           \
            exit(1);                                                                                                 \
        }                                                                                                            \
    } while (0)
#endif

/* define a macro about the return value of security function */
#define securec_check_intval_core(val, express) \
    if (val == -1) {                            \
        fprintf(stderr,                         \
            "WARNING: %s:%d failed on calling " \
            "security function.\n",             \
            __FILE__,                           \
            __LINE__);                          \
        express;                                \
    }

#define securec_check_for_sscanf_s(ret, min_num, str1, str2)                                                         \
    do {                                                                                                             \
        if ((ret) < (min_num)) {                                                                                     \
            freeSecurityFuncSpace_c(str1, str2);                                                                     \
            printf("ERROR at %s : %d : The destination buffer or format is a NULL pointer or the invalid parameter " \
                   "handle is invoked..\n",                                                                          \
                __FILE__,                                                                                            \
                __LINE__);                                                                                           \
            exit(1);                                                                                                 \
        }                                                                                                            \
    } while (0)
#endif
