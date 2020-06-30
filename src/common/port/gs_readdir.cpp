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
 * gs_readdir.cpp
 *    Prototypes and macros around system calls, used to help make
 *    threaded libraries reentrant and safe to use system.
 *
 * IDENTIFICATION
 *    src/common/port/gs_readdir.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "gs_threadlocal.h"

#include <errno.h>
#include <dirent.h>
#include <limits.h>
#include <stddef.h>

#ifndef LEN_D_NAME
#define LEN_D_NAME offsetof(struct dirent, d_name)
#endif
#ifndef PATH_MAX
#define PATH_MAX 260
#endif

THR_LOCAL char gs_dir_buf[PATH_MAX + LEN_D_NAME + 1];

struct dirent* gs_readdir(DIR* dir)
{
#ifndef WIN32
    struct dirent* ent = NULL;
    struct dirent* tmp_dir = NULL;
    int error = 0;

    tmp_dir = (struct dirent*)gs_dir_buf;
/**
 * In arm environment, readdir_r warning about deprecated-declarations,
 * but for thread safe keep using readdir_r.
 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
    error = readdir_r(dir, tmp_dir, &ent);
#pragma GCC diagnostic pop

    if (error != 0) {
        errno = error;
        return NULL;
    }

    return ent;
#else
    return readdir(dir);
#endif
}
