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
 * -------------------------------------------------------------------------
 *
 * feparser_memutils.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\frontend_parser\feparser_memutils.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "libpq-fe.h"
#include "nodes/feparser_memutils.h"
#include "libpq-int.h"
#include <algorithm>

thread_local void **g_vec = NULL;
thread_local size_t g_vec_size = 0;
thread_local const size_t g_resize_factor = 256;
void insert_into_vector(void *tmp)
{
    if (!g_vec) {
        g_vec = (void **)malloc(g_resize_factor * sizeof(void *));
    } else if (g_vec_size % g_resize_factor == 0) {
        g_vec = (void **)libpq_realloc(g_vec, g_vec_size * sizeof(*g_vec),
            (g_vec_size + g_resize_factor) * sizeof(*g_vec));
    }
    if (g_vec == NULL) {
        printf("out of memory\n");
        exit(EXIT_FAILURE);
    }
    g_vec[g_vec_size] = tmp;
    g_vec_size++;
}

/*
 * "Safe" wrapper around strdup()
 */
char *feparser_strdup(const char *string)
{
    char *tmp = NULL;

    if (NULL == string) {
        printf("feparser_strdup: cannot duplicate null pointer (internal error)\n");
        exit(EXIT_FAILURE);
    }
    tmp = strdup(string);
    if (NULL == tmp) {
        printf("out of memory\n");
        exit(EXIT_FAILURE);
    }
    insert_into_vector(tmp);
    return tmp;
}

/*
 * "Safe" wrapper around strndup()
 */
char *feparser_strndup(const char *string, const size_t n)
{
    char *tmp = NULL;
    if (NULL == string) {
        printf("feparser_strndup: cannot duplicate null pointer (internal error)\n");
        exit(EXIT_FAILURE);
    }
    tmp = strndup(string, n);
    if (NULL == tmp) {
        printf("out of memory\n");
        exit(EXIT_FAILURE);
    }
    insert_into_vector(tmp);
    return tmp;
}

void *feparser_malloc(size_t size)
{
    void *tmp = NULL;

    /* Avoid unportable behavior of malloc(0) */
    if (size == 0) {
        size = 1;
    }
    tmp = malloc(size);
    if (NULL == tmp) {
        printf("out of memory\n");
        exit(EXIT_FAILURE);
    }
    insert_into_vector(tmp);
    return tmp;
}

void *feparser_malloc0(size_t size)
{
    void *tmp = NULL;
    errno_t rc = 0;

    tmp = feparser_malloc(size);
    rc = memset_s(tmp, size, 0, size);
    check_memset_s(rc);
    return tmp;
}

void *feparser_realloc(void *ptr, size_t size)
{
    void *tmp = NULL;

    /* When realloc failed gsql will exit, with no memory leak for ptr. */
    tmp = (void *)realloc(ptr, size);
    if (NULL == tmp) {
        printf("out of memory\n");
        exit(EXIT_FAILURE);
    }
    for (size_t i = 0; i < g_vec_size; i++) {
        if (g_vec[i] == ptr) {
            g_vec[i] = tmp;
        }
    }
    return tmp;
}
void feparser_free(void *pointer)
{
    for (size_t i = 0; i < g_vec_size; i++) {
        if (g_vec[i] == pointer) {
            g_vec[i] = g_vec[g_vec_size - 1];
            libpq_free(pointer);
            g_vec_size--;
            if (g_vec_size % g_resize_factor == 0) {
                g_vec = (void **)libpq_realloc(g_vec, (g_vec_size + g_resize_factor) * sizeof(*g_vec),
                    (g_vec_size) * sizeof(*g_vec));
                if (g_vec == NULL) {
                    return;
                }
            }
        }
    }
}
void free_memory()
{
    for (size_t i = 0; i < g_vec_size; i++) {
        libpq_free(g_vec[i]);
    }
    g_vec_size = 0;
    libpq_free(g_vec);
}
