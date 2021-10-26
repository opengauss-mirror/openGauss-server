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
 * archive_am.h
 *    nass access method definitions.
 *
 *
 * IDENTIFICATION
 *    src/include/access/archive/archive_am.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef ARCHIVE_AM_H
#define ARCHIVE_AM_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "nodes/pg_list.h"
#include "storage/buf/buffile.h"
#include "replication/slot.h"

/* in archive/archive_am.cpp */
size_t ArchiveRead(const char* fileName, int offset, char *buffer, int length, ObsArchiveConfig *archive_config = NULL);
int ArchiveWrite(const char* fileName, const char *buffer, const int bufferLength,
    ObsArchiveConfig *archive_config = NULL);
int ArchiveDelete(const char* fileName, ObsArchiveConfig *archive_config = NULL);
List* ArchiveList(const char* prefix, ObsArchiveConfig *archive_config = NULL,
            bool reportError = true, bool shortenConnTime = false);

#endif /* ARCHIVE_AM_H */

