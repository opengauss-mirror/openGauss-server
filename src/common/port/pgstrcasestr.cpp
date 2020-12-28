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
 * pgstrcasestr.cpp
 *
 * IDENTIFICATION
 *    src/common/port/pgstrcasestr.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "c.h"
#include "securec.h"
#include "securec_check.h"
#include <ctype.h>
#include <string.h>

int mask_single_passwd(char* spasswd)
{
    int len = 0;
    char* tmp = NULL;
    errno_t rc;

    if (spasswd != NULL) {
        len = strlen(spasswd);
    } else {
        return 0;
    }

    if (len >= 8) {
        len = 8;
    }
    tmp = (char*)malloc(len + 1);
    if (tmp != NULL) {
        rc = memset_s(tmp, len + 1, '*', len);
        securec_check_c(rc, "\0", "\0");
        tmp[len] = '\0';
        rc = memset_s(spasswd, strlen(spasswd), 0, strlen(spasswd));
        securec_check_c(rc, tmp, "\0");
        rc = strncpy_s(spasswd, len + 1, tmp, len);
        securec_check_c(rc, tmp, "\0");
        free(tmp);
        tmp = NULL;
    } else {
        return 0;
    }
    return 1;
}
