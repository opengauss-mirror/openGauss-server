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
 * bbox_create.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_create.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __BBOX_CREATE_H__
#define __BBOX_CREATE_H__

#include "bbox_syscall_support.h"
#include "bbox_elf_dump.h"
#include "bbox_threads.h"

#define BBOX_TINE_LEN 32
#define BBOX_NAME_PATH_LEN 512

/* print bbox backtrace layers */
#define BBOX_BACKTRACE_LAYERS 100

/* process name which is depended by bbox, embed application should provide it. */
extern char* progname;

#define BBOX_CORE_FILE_ADD_NAME "bbox"
#define BBOX_SNAP_FILE_ADD_NAME "snap"
#define BBOX_TMP_FILE_ADD_NAME "bbox.tmp"
#define BBOX_TMP_DEL_TIME_INTERVAL (60 * 60 * 24) /* remove bbox file of one day ago. */

#define BBOX_DATE_TIME_CMD "date +'%Y_%m_%d_%H_%M_%S'"

extern long int g_iCoreDumpBeginTime;

#endif
