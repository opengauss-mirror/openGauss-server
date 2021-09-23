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
 *---------------------------------------------------------------------------------------
 *
 *  crypt.h
 *        Add crypt common function and variable
 *
 * IDENTIFICATION
 *        src/bin/gs_guc/crypt.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CRYPT_H
#define CRYPT_H

#define MAX_CRYPT_LEN 32
#define KEY_SPLIT_LEN 15
#define MAX_CHILD_NUM 3
#define MAX_CHILD_PATH 1024
#define MAX_COMMAND_LEN 4096

#define CRYPT_FREE(ptr)    \
    do {                   \
        if (NULL != ptr) { \
            free(ptr);     \
            ptr = NULL;    \
        }                  \
    } while (0)

extern void* crypt_malloc_zero(size_t size);

#endif
