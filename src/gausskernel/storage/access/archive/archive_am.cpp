/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright 2008 Bryan Ischo <bryan@ischo.com>
 *
 *
 * obs_am.cpp
 *    obs access method definitions.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/obs/obs_am.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <assert.h>
#include <vector>
#include <string>
#include <iostream>
#include <stdio.h>

#include "access/archive/archive_am.h"
#include "access/archive/nas_am.h"
#include "access/obs/obs_am.h"

#include "replication/slot.h"


size_t ArchiveRead(const char* fileName, const int offset, char *buffer, const int length,
    ArchiveConfig *archive_config)
{
    if (archive_config == NULL) {
        return 0;
    }
    if (archive_config->media_type == ARCHIVE_OBS) {
        return obsRead(fileName, offset, buffer, length, archive_config);
    } else if (archive_config->media_type == ARCHIVE_NAS) {
        return NasRead(fileName, offset, buffer, length, archive_config);
    }

    return 0;
}

int ArchiveWrite(const char* fileName, const char *buffer, const int bufferLength, ArchiveConfig *archive_config)
{
    int ret = -1;
    if (archive_config == NULL) {
        return ret;
    }

    if (archive_config->media_type == ARCHIVE_OBS) {
        ret = obsWrite(fileName, buffer, bufferLength, archive_config);
    } else if (archive_config->media_type == ARCHIVE_NAS) {
        ret = NasWrite(fileName, buffer, bufferLength, archive_config);
    }

    return ret;
}

int ArchiveDelete(const char* fileName, ArchiveConfig *archive_config)
{
    int ret = -1;
    if (archive_config == NULL) {
        return ret;
    }

    if (archive_config->media_type == ARCHIVE_OBS) {
        ret = obsDelete(fileName, archive_config);
    } else if (archive_config->media_type == ARCHIVE_NAS) {
        ret = NasDelete(fileName, archive_config);
    }

    return ret;
}

List* ArchiveList(const char* prefix, ArchiveConfig *archive_config, bool reportError, bool shortenConnTime)
{
    List* fileNameList = NIL;
    if (archive_config == NULL) {
        return fileNameList;
    }

    if (archive_config->media_type == ARCHIVE_OBS) {
        fileNameList = obsList(prefix, archive_config, reportError, shortenConnTime);
    } else if (archive_config->media_type == ARCHIVE_NAS) {
        fileNameList = NasList(prefix, archive_config);
    }

    return fileNameList;
}

bool ArchiveFileExist(const char* file_path, ArchiveConfig *archive_config)
{
    bool ret = false;
    if (archive_config == NULL) {
        ereport(WARNING, (errmsg("when check file exist, the archive config is null")));
        return ret;
    }

    if (archive_config->media_type == ARCHIVE_OBS) {
        ret = checkOBSFileExist(file_path, archive_config);
    } else if (archive_config->media_type == ARCHIVE_NAS) {
        ret = checkNASFileExist(file_path, archive_config);
    }

    return ret;
}
