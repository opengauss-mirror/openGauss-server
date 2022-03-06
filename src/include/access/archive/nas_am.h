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
 * nas_am.h
 *    nass access method definitions.
 *
 *
 * IDENTIFICATION
 *    src/include/access/archive/nas_am.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef NAS_AM_H
#define NAS_AM_H

#include "postgres.h"
#include "nodes/pg_list.h"
#include "replication/slot.h"

size_t NasRead(const char* fileName, int offset, char *buffer, int length, ArchiveConfig *nas_config = NULL);
int NasWrite(const char* fileName, const char *buffer, const int bufferLength, ArchiveConfig *nas_config = NULL);
int NasDelete(const char* fileName, ArchiveConfig *nas_config = NULL);
List* NasList(const char* prefix, ArchiveConfig *nas_config = NULL);
bool checkNASFileExist(const char* file_path, ArchiveConfig *nas_config);

#endif /* NAS_AM_H */
