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
 *  dfs_connector.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/dfs/dfs_connector.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "obs/obs_connector.h"
#include "foreign/foreign.h"
#include "c.h"
namespace dfs {
/*
 * @Description: create connctor for foreign table
 * @IN ctx: memory context
 * @IN foreignTableId: foreign table id
 * @Return: dfs connecort
 * @See also:
 */
DFSConnector *createConnector(MemoryContext ctx, Oid foreignTableId)
{
    ServerTypeOption srvType = getServerType(foreignTableId);
    switch (srvType) {
        case T_OBS_SERVER:
        case T_TXT_CSV_OBS_SERVER: {
#ifndef ENABLE_LITE_MODE
            return New(ctx) OBSConnector(ctx, foreignTableId);
#else
            FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
            return NULL;
#endif
        }
        case T_HDFS_SERVER: {
            FEATURE_NOT_PUBLIC_ERROR("HDFS is not yet supported.");
            return NULL;
        }
        default: {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmodule(MOD_DFS),
                            errmsg("dfs foreign server type error. type = %d", srvType)));
            return NULL;
        }
    }
}

/*
 * @Description: create connector for foreign table
 * @IN ctx: memory context
 * @IN foreignTableId: foreign table id
 * @Return: dfs connector
 * @See also:
 */
DFSConnector *createConnector(MemoryContext ctx, ServerTypeOption srvType, void *options)
{
    switch (srvType) {
        case T_OBS_SERVER: {
#ifndef ENABLE_LITE_MODE
            return New(ctx) OBSConnector(ctx, (ObsOptions *)options);
#else
            FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
            return NULL;
#endif
            break;
        }
        case T_HDFS_SERVER: {
            FEATURE_NOT_PUBLIC_ERROR("HDFS is not yet supported.");
            return NULL;
            break;
        }
        default: {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmodule(MOD_DFS),
                            errmsg("dfs forign server type error. type = %d", srvType)));
            return NULL;
        }
    }
}

/*
 * @Description: create connctor for table space, only support hdfs
 * @IN ctx: memory context
 * @IN srvOptions: server options
 * @IN tablespaceOid: tablespace oid
 * @Return:dfs connector
 * @See also:
 */
DFSConnector *createConnector(MemoryContext ctx, DfsSrvOptions *srvOptions, Oid tablespaceOid)
{
    FEATURE_NOT_PUBLIC_ERROR("HDFS is not yet supported.");
    return NULL;
}
}  // namespace dfs

/*
 * @Description: check file path should skip
 * @IN fileName: file name
 * @IN end_pos: end char position
 * @Return: true for skip, false for not skip
 * @See also:
 */
bool checkFileShouldSkip(char *fileName, int end_pos)
{
    if (fileName == NULL || *fileName == '\0') {
        return false;
    }

    Assert(end_pos <= (int)strlen(fileName));

    /* start with .  _  or # will be skip */
    if (*fileName == '.' || *fileName == '_' || *fileName == '#') {
        return true;
    }

    /* end with ~  will be skip */
    char *endChar = fileName + end_pos - 1;
    if (*endChar == '~') {
        return true;
    }

    return false;
}

/*
 * @Description: check file path should skip
 * @IN fileName: file name
 * @Return: true for skip, false for not skip
 * @See also:
 */
bool checkFileShouldSkip(char *fileName)
{
    if (fileName == NULL || *fileName == '\0') {
        return false;
    }

    bool isSkip = checkFileShouldSkip(fileName, strlen(fileName));

    return isSkip;
}

/*
 * @Description: check the each part of path should skip
 * @IN pathName: path name
 * @Return: true for skip, false for not skip
 * @See also:
 */
bool checkPathShouldSkip(char *pathName)
{
    if (pathName == NULL || *pathName == '\0') {
        return false;
    }

    int end_pos = 0;
    bool isSkip = false;
    char *pos = pathName;

    /* first char is slash */
    if (*pos == '/') {
        pos += 1;
    }

    /* find next slash */
    char *next_slash = strchr(pos, '/');
    /* get next slash position or the last char position */
    end_pos = (int)((next_slash != NULL) ? (next_slash - pos) : strlen(pos));

    /* check path name */
    isSkip = checkFileShouldSkip(pos, end_pos);

    while (!isSkip) {
        pos = next_slash;
        if (pos == NULL || *pos == '\0') {
            break;
        }

        pos += 1;
        /* find next slash */
        next_slash = strchr(pos, '/');
        /* get next slash position or the last char position */
        end_pos = (int)((next_slash != NULL) ? (next_slash - pos) : strlen(pos));

        /* check path name */
        isSkip = checkFileShouldSkip(pos, end_pos);
    }

    return isSkip;
}

