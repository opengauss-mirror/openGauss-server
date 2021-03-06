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
 *  crypt.cpp
 *        Add crypt common function
 *
 * IDENTIFICATION
 *        src/bin/gs_guc/crypt.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <stdio.h>
#include "securec.h"
#include "securec_check.h"
#include "cipher.h"
#include "crypt.h"

void* crypt_malloc_zero(size_t size)
{
    void* ret = NULL;
    errno_t rc = 0;

    if (size == 0) {
        (void)fprintf(stderr, _("Invalid malloc size 0\n"));
        exit(EXIT_FAILURE);
    }

    ret = (void*)malloc(size);
    if (ret == NULL) {
        (void)fprintf(stderr, _("crypt: malloc failed\n"));
        exit(EXIT_FAILURE);
    }

    rc = memset_s(ret, size, '\0', size);
    securec_check_c(rc, "\0", "\0");
    return ret;
}
