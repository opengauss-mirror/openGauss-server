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
 * gs_malloc.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/gs_register/gs_malloc.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GS_MALLOC_H
#define GS_MALLOC_H

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include "gssignal/gs_signal.h"
#include "gs_threadlocal.h"

#ifdef WIN32
#define register_hash_len   1111
#define register_local_info_len   64
#define REGISTER_TRUE      1
#define REGISTER_FALSE     0
#endif

/* these memory apply macro is used only for memory context */
#define gs_free(local_register_ptr, local_register_size)             \
    do {                                                             \
        if ((local_register_size) < mmap_threshold) {                \
            free((void*)(local_register_ptr));                       \
        } else {                                                     \
            (void)munmap(local_register_ptr, (local_register_size)); \
        }                                                            \
    } while (0)

#define gs_calloc(local_register_nmemb, local_register_size, local_register_ptr, local_register_type)               \
    do {                                                                                                            \
        Size gSize = (local_register_nmemb) * (local_register_size);                                                \
        if (gSize < mmap_threshold) {                                                                               \
            (local_register_ptr) =                                                                                  \
                (local_register_type)calloc((size_t)(local_register_nmemb), (size_t)(local_register_size));         \
        } else {                                                                                                    \
            (local_register_ptr) =                                                                                  \
                (local_register_type)mmap(NULL, gSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0); \
        }                                                                                                           \
    } while (0)

#define gs_malloc(local_register_size, local_register_ptr, local_register_type)                           \
    do {                                                                                                  \
        if ((local_register_size) < mmap_threshold) {                                                     \
            (local_register_ptr) = (local_register_type)malloc((size_t)(local_register_size));            \
        } else {                                                                                          \
            (local_register_ptr) = (local_register_type)mmap(                                             \
                NULL, (local_register_size), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0); \
        }                                                                                                 \
    } while (0)

#define gs_realloc(local_register_old_ptr,                                                                    \
    local_register_old_size,                                                                                  \
    local_register_new_ptr,                                                                                   \
    local_register_new_size,                                                                                  \
    local_register_type)                                                                                      \
    do {                                                                                                      \
        if ((local_register_new_size) < mmap_threshold) {                                                     \
            (local_register_new_ptr) =                                                                        \
                (local_register_type)realloc((local_register_old_ptr), (local_register_new_size));            \
        } else {                                                                                              \
            (local_register_new_ptr) = (local_register_type)mmap(                                             \
                NULL, (local_register_new_size), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0); \
            if (NULL == (local_register_new_ptr)) {                                                           \
                break;                                                                                        \
            } else {                                                                                          \
                errno_t rc = EOK;                                                                             \
                rc = memmove_s(local_register_new_ptr,                                                        \
                    local_register_new_size,                                                                  \
                    local_register_old_ptr,                                                                   \
                    (local_register_old_size));                                                               \
                securec_check(rc, "\0", "\0");                                                                \
                gs_free(local_register_old_ptr, local_register_old_size);                                     \
            }                                                                                                 \
        }                                                                                                     \
    } while (0)

#define gs_strdup(local_register_str, local_register_new_str)                                         \
    do {                                                                                              \
        Size gSize = strlen(local_register_str) + 1;                                                  \
        if (gSize < mmap_threshold) {                                                                 \
            (local_register_new_str) = strdup((local_register_str));                                  \
        } else {                                                                                      \
            (local_register_new_str) =                                                                \
                (char*)mmap(NULL, gSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0); \
            if (NULL == (local_register_new_str)) {                                                   \
                break;                                                                                \
            } else {                                                                                  \
                errno_t rc = EOK;                                                                     \
                rc = strncpy_s(local_register_new_str, gSize, local_register_str, gSize - 1);         \
                securec_check(rc, "\0", "\0");                                                        \
                local_register_new_str[gSize - 1] = '\0';                                             \
            }                                                                                         \
        }                                                                                             \
    } while (0)

#ifdef WIN32
typedef struct tag_register_node {
    unsigned long size_mem;
    void *addr_mem;
    struct tag_register_node *next;
    struct tag_register_node *prior;
    unsigned long no_ctl; /* pool no.*/
    char register_local[register_local_info_len];
} register_node;
#endif

#endif

