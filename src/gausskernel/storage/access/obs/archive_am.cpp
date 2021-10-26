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

#include "access/obs/archive_am.h"
#include "access/obs/nas_am.h"
#include "access/obs/obs_am.h"

#include "replication/slot.h"


size_t ArchiveRead(const char* fileName, const int offset, char *buffer, const int length,
    ObsArchiveConfig *archive_config)
{
    ObsArchiveConfig *archive_obs = NULL;

    if (archive_config != NULL) {
        archive_obs = archive_config;
    } else {
        archive_obs = getObsArchiveConfig();
    }

    if (archive_obs->media_type == ARCHIVE_OBS) {
        return obsRead(fileName, offset, buffer, length, archive_obs);
    } else if (archive_obs->media_type == ARCHIVE_NAS) {
        return NasRead(fileName, offset, buffer, length, archive_obs);
    }

    return 0;
}

int ArchiveWrite(const char* fileName, const char *buffer, const int bufferLength, ObsArchiveConfig *archive_slot)
{
    int ret = -1;
    if (archive_slot == NULL) {
        return ret;
    }

    if (archive_slot->media_type == ARCHIVE_OBS) {
        ret = obsWrite(fileName, buffer, bufferLength, archive_slot);
    } else if (archive_slot->media_type == ARCHIVE_NAS) {
        ret = NasWrite(fileName, buffer, bufferLength, archive_slot);
    }

    return ret;
}

int ArchiveDelete(const char* fileName, ObsArchiveConfig *archive_config)
{
    int ret = -1;

    ObsArchiveConfig *archive_obs = NULL;

    if (archive_config != NULL) {
        archive_obs = archive_config;
    } else {
        archive_obs = getObsArchiveConfig();
    }

    if (archive_obs->media_type == ARCHIVE_OBS) {
        ret = obsDelete(fileName, archive_obs);
    } else if (archive_obs->media_type == ARCHIVE_NAS) {
        ret = NasDelete(fileName, archive_obs);
    }

    return ret;
}

List* ArchiveList(const char* prefix, ObsArchiveConfig *archive_config, bool reportError, bool shortenConnTime)
{
    List* fileNameList = NIL;

    ObsArchiveConfig *archive_obs = NULL;

    if (archive_config != NULL) {
        archive_obs = archive_config;
    } else {
        archive_obs = getObsArchiveConfig();
    }

    if (archive_obs->media_type == ARCHIVE_OBS) {
        fileNameList = obsList(prefix, archive_obs);
    } else if (archive_obs->media_type == ARCHIVE_NAS) {
        fileNameList = NasList(prefix, archive_obs);
    }

    return fileNameList;
}
