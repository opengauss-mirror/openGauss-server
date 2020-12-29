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
 *  obs_connector.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/dfs/obs/obs_connector.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "access/dfs/dfs_query.h"

#include "obs_connector.h"
#include "foreign/foreign.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/plog.h"

#if defined(__LP64__) || defined(__64BIT__)
    typedef unsigned int GS_UINT32;
#else
    typedef unsigned long GS_UINT32;
#endif

#define OBS_NOT_IMPLEMENT \
    ereport(ERROR,        \
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_OBS), errmsg("%s not implemented", __FUNCTION__)));
HTAB *OBSConnectorCache = NULL;

namespace dfs {
OBSConnector::OBSConnector(MemoryContext ctx, Oid foreignTableId)
    : m_memcontext(ctx), m_handler(NULL), srvType(T_INVALID)
{
    // build obs handler
    m_handler = searchConnectorCache(foreignTableId);
}

OBSConnector::OBSConnector(MemoryContext ctx, ObsOptions *obsOptions)
    : m_memcontext(ctx), m_handler(NULL), srvType(T_INVALID)
{
    // build obs handler
    m_handler = createRWHandler(obsOptions);
}

OBSConnector::~OBSConnector()
{
    Destroy();
}

void OBSConnector::Destroy()
{
    // IMPORT:  m_handler->m_prefix  not alloc on this class 's memeory context
    m_handler->m_object_info.key = NULL;
    DestroyObsReadWriteHandler(m_handler, true);
    m_handler = NULL;
}

/*
 * @Description: is file
 * @IN filePath: file path
 * @Return: true for file, false for path
 * @See also:
 */
bool OBSConnector::isDfsFile(const char *filePath)
{
    return this->isDfsFile(filePath, true);
}

/*
 * @Description: is file
 * @IN filePath:file path
 * @Return: true for file, false for path
 * @See also:
 */
bool OBSConnector::isDfsFile(const char *filePath, bool throw_error)
{
    Assert(filePath);

    bool isExist = false;
    char *bucket = NULL;
    char *prefix = NULL;

    AutoContextSwitch memGuard(m_memcontext);
    FetchUrlPropertiesForQuery(filePath, &bucket, &prefix);
    Assert(bucket != NULL);
    Assert(prefix != NULL);
    List *object_list = list_bucket_objects_for_query(m_handler, bucket, prefix);

    if (object_list == NIL) {
        // file not exists
        isExist = false;
    } else {
        // get the first one check if match with filePath
        SplitInfo *splitInfo = (SplitInfo *)lfirst(list_head(object_list));
        Assert(splitInfo);

        if (0 == strcmp(filePath, splitInfo->filePath)) {
            // file exist
            isExist = true;
        } else {
            // file not exist, may be is a directory or just prefix is same
            isExist = false;
        }

        // release object_list
        release_object_list(object_list);
        object_list = NIL;
    }

    return isExist;
}

/*
 * @Description: is empty file
 * @IN filePath:file path
 * @Return:true for empty, false for not empty
 * @See also:
 */
bool OBSConnector::isDfsEmptyFile(const char *filePath)
{
    Assert(filePath);
    bool isEmpty = false;
    char *bucket = NULL;
    char *prefix = NULL;

    AutoContextSwitch memGuard(m_memcontext);
    FetchUrlPropertiesForQuery(filePath, &bucket, &prefix);
    Assert(bucket != NULL);
    Assert(prefix != NULL);
    List *object_list = list_bucket_objects_for_query(m_handler, bucket, prefix);

    if (object_list == NIL) {
        // file not exists
        isEmpty = false;
    } else {
        // get the first one check if match with filePath
        SplitInfo *splitInfo = (SplitInfo *)lfirst(list_head(object_list));
        Assert(splitInfo);

        int64 fileSize = splitInfo->ObjectSize;

        if (strcmp(filePath, splitInfo->filePath) == 0) {
            // file exist, check if empty
            isEmpty = (fileSize == 0);
        } else {
            // file not exist, may be is a directory or just prefix is same
            isEmpty = false;
        }

        // release object_list
        release_object_list(object_list);
        object_list = NIL;
    }

    return isEmpty;
}

/*
 * @Description: get file size
 * @IN filePath: file path
 * @Return: file size
 * @See also:
 */
int64_t OBSConnector::getFileSize(const char *filePath)
{
    Assert(filePath);

    int64_t fileSize = 0;
    char *bucket = NULL;
    char *prefix = NULL;

    AutoContextSwitch memGuard(m_memcontext);

    if (srvType == T_TXT_CSV_OBS_SERVER) {
        char *hostName = NULL;
        FetchUrlProperties(filePath, &hostName, &bucket, &prefix);
    } else {
        FetchUrlPropertiesForQuery(filePath, &bucket, &prefix);
    }

    Assert(bucket != NULL);
    Assert(prefix != NULL);
    List *object_list = list_bucket_objects_for_query(m_handler, bucket, prefix);
    if (object_list == NIL) {
        fileSize = 0;
        ereport(LOG, (errmodule(MOD_OBS), errmsg("object not exists, bucket = %s, prefix = %s", bucket, prefix)));
    } else {
        // maybe list_length(object_list) != 1 just  get the first one
        SplitInfo *splitinfo = (SplitInfo *)lfirst(list_head(object_list));
        Assert(splitinfo);

        fileSize = splitinfo->ObjectSize;

        // release object_list
        release_object_list(object_list);
        object_list = NIL;
    }

    return fileSize;
}

/*
 * @Description: return ob handler
 * @See also:
 */
void *OBSConnector::getHandler() const
{
    return m_handler;
}

/*
 * @Description: list directory
 * @IN folderPath:directory path
 * @Return:directory list
 * @See also:
 */
List *OBSConnector::listDirectory(char *folderPath)
{
    return this->listDirectory(folderPath, true);
}

/*
 * @Description: list directory
 * @IN folderPath: directory path
 * @Return: directory list
 * @See also:
 */
List *OBSConnector::listDirectory(char *folderPath, bool throw_error)
{
    // obs does not support list directory
    OBS_NOT_IMPLEMENT;
    return NIL;
}

/*
 * @Description: list objects
 * @IN searchPath: object path, bucket + prefix
 * @IN primitivePrefix: primitive perfix to mark the part of object name get from obs
 * @Return:object list
 * @See also:
 */
List *OBSConnector::listObjectsStat(char *searchPath, const char *primitivePrefix)
{
    Assert(searchPath != NULL);
    Assert(primitivePrefix != NULL);

    ListCell *prev = NULL;
    ListCell *next = NULL;
    ListCell *cell = NULL;
    int slashNum = getSpecialCharCnt(primitivePrefix, '/');
    List *objectList = NIL;
    char *bucket = NULL;
    char *prefix = NULL;

    AutoContextSwitch memGuard(m_memcontext);

    FetchUrlPropertiesForQuery(searchPath, &bucket, &prefix);
    Assert(bucket != NULL);
    Assert(prefix != NULL);

    PROFILING_OBS_START();
    pgstat_report_waitevent(WAIT_EVENT_OBS_LIST);
    objectList = list_bucket_objects_for_query(m_handler, bucket, prefix);
    pgstat_report_waitevent(WAIT_EVENT_END);
    PROFILING_OBS_END_LIST(list_length(objectList));

    if (0 == list_length(objectList)) {
        ereport(LOG, (errmodule(MOD_OBS), errmsg("Do not find any object for \"/%s/%s\".", bucket, prefix)));
    }

    for (cell = list_head(objectList); cell != NULL; cell = next) {
        SplitInfo *splitinfo = (SplitInfo *)lfirst(cell);

        next = lnext(cell);

        // set slash num
        splitinfo->prefixSlashNum = slashNum;

        // find start position which exclude primitive prefix
        int pos = find_Nth(splitinfo->filePath, slashNum, "/");
        Assert(pos != -1);

        // get the part of the filename which exclude primitive prefix
        char *listedFileName = splitinfo->filePath + pos + 1;

        // skip the file path include special char and object size is 0
        if (splitinfo->ObjectSize == 0 || checkPathShouldSkip(listedFileName)) {
            objectList = list_delete_cell(objectList, cell, prev);

            ereport(DEBUG1, (errmodule(MOD_OBS), errmsg("list object, skipped, path is %s, size is %ld",
                                                        splitinfo->filePath, splitinfo->ObjectSize)));
        } else {
            prev = cell;

            ereport(DEBUG1, (errmodule(MOD_OBS), errmsg("list object, path is %s, size is %ld", splitinfo->filePath,
                                                        splitinfo->ObjectSize)));
        }
    }

    ereport(LOG, (errmodule(MOD_OBS),
                  errmsg("listObjectsStat \"%s\", objectListSize = %d", searchPath, list_length(objectList))));

    return objectList;
}

DFSBlockInfo *OBSConnector::getBlockLocations(char *filePath)
{
    OBS_NOT_IMPLEMENT;
    return NULL;
}

int OBSConnector::dropDirectory(const char *path, int recursive)
{
    OBS_NOT_IMPLEMENT;
    return 0;
}

int OBSConnector::createDirectory(const char *path)
{
    OBS_NOT_IMPLEMENT;
    return 0;
}

int OBSConnector::openFile(const char *path, int flag)
{
    char *bucket = NULL;
    char *prefix = NULL;

    FetchUrlPropertiesForQuery(path, &bucket, &prefix);

    m_handler->m_object_info.key = prefix;
    if (m_handler->m_option.bucket_options.bucket_name == NULL)
        m_handler->m_option.bucket_options.bucket_name = bucket;

    if (flag == O_RDONLY)
        ObsReadWriteHandlerSetType(m_handler, OBS_READ);
    else
        ObsReadWriteHandlerSetType(m_handler, OBS_WRITE);
    return 0;
}

int OBSConnector::deleteFile(const char *path, int recursive)
{
    int ret = 0;
    ret = deleteOBSObject(m_handler);
    return ret;
}

/*
 * @Description: directory exists
 * @IN filePath: directory path, must start and end with '/'
 * @Return: true for exists, false for not exists
 * @See also:
 */
bool OBSConnector::pathExists(const char *filePath)
{
    Assert(filePath);

    bool isContain = false;
    char *bucket = NULL;
    char *prefix = NULL;

    AutoContextSwitch memGuard(m_memcontext);

    FetchUrlPropertiesForQuery(filePath, &bucket, &prefix);
    Assert(bucket != NULL);
    Assert(prefix != NULL);
    List *object_list = list_bucket_objects_for_query(m_handler, bucket, prefix);
    if (object_list == NIL) {
        isContain = false;
    } else {
        // filePath last char is '/'  check return list if empty
        isContain = (list_length(object_list) != 0);

        // release object_list
        release_object_list(object_list);
        object_list = NIL;
    }

    return isContain;
}

/*
 * @Description: file exists
 * @IN path: file path
 * @Return: true for exists, false for not exists
 * @See also:
 */
bool OBSConnector::existsFile(const char *path)
{
    Assert(path);

    bool isExists = false;
    char *bucket = NULL;
    char *prefix = NULL;

    AutoContextSwitch memGuard(m_memcontext);

    FetchUrlPropertiesForQuery(path, &bucket, &prefix);
    Assert(bucket != NULL);
    Assert(prefix != NULL);
    List *object_list = list_bucket_objects_for_query(m_handler, bucket, prefix);
    if (object_list == NIL) {
        isExists = false;
    } else {
        // get the first one check if match with filePath
        SplitInfo *splitinfo = (SplitInfo *)lfirst(list_head(object_list));
        Assert(splitinfo);

        if (strcmp(path, splitinfo->filePath) == 0) {
            // file exist
            isExists = true;
        } else {
            // file not exist, may be is a directory or just prefix is same
            isExists = false;
        }

        // release object_list
        release_object_list(object_list);
        object_list = NIL;
    }

    return isExists;
}

bool OBSConnector::hasValidFile() const
{
    /* OBS_NOT_IMPLEMENT; */
    return false;
}

int OBSConnector::writeCurrentFile(const char *buffer, int length)
{
    int ret = 0;
    ret = writeObsTempFile(m_handler, buffer, length);
    return ret;
}

int OBSConnector::readCurrentFileFully(char *buffer, int length, int64 offset)
{
    int ret = 0;
    int readSize = (int)read_bucket_object(m_handler, buffer, length);
    if (readSize != length) {
        ret = -1;
    }
    return ret;
}

int OBSConnector::flushCurrentFile()
{
    /* OBS_NOT_IMPLEMENT; */
    return 0;
}

void OBSConnector::closeCurrentFile()
{
    /* OBS_NOT_IMPLEMENT; */
}

int OBSConnector::chmod(const char *filePath, short mode)
{
    /* obs dos not support chmod */
    OBS_NOT_IMPLEMENT;
    return false;
}

int OBSConnector::setLabelExpression(const char *filePath, const char *expression)
{
    /* obs dos not support setLabelExpression */
    OBS_NOT_IMPLEMENT;
    return false;
}

/*
 * @Description: get file modify time
 * @IN filePath:file path
 * @Return: file modify time
 * @See also:
 */
int64 OBSConnector::getLastModifyTime(const char *filePath)
{
    OBS_NOT_IMPLEMENT;
    return 0;
}

const char *OBSConnector::getValue(const char *key, const char *defValue) const
{
    if (pg_strcasecmp(key, "bucket") == 0) {
        return m_handler != NULL ? m_handler->m_option.bucket_options.bucket_name : NULL;
    }
    return NULL;
}

/*
 * @Description: get obs handler
 * @IN foreignTableId: foreign table id
 * @Return: obs handler
 * @See also:
 */
OBSReadWriteHandler *OBSConnector::searchConnectorCache(Oid foreignTableId)
{
    AutoContextSwitch memGuard(m_memcontext);

    OBSReadWriteHandler *handler = NULL;
    ObsOptions *obsOptions = NULL;
    if (IS_OBS_CSV_TXT_FOREIGN_TABLE(foreignTableId)) {
        /* Build it if target is not found for dfs table */
        obsOptions = getObsOptions(foreignTableId);
        srvType = T_TXT_CSV_OBS_SERVER;
    } else {
        obsOptions = getOptionFromCache(foreignTableId);
        srvType = T_OBS_SERVER;
    }

    handler = CreateObsReadWriteHandlerForQuery(obsOptions);
    if (SECUREC_UNLIKELY(handler == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_DFS), errmsg("memory alloc failed")));
    }
    handler->in_computing = false;

    return handler;
}

OBSReadWriteHandler *OBSConnector::createRWHandler(ObsOptions *obsOptions)
{
    AutoContextSwitch memGuard(m_memcontext);

    OBSReadWriteHandler *handler = NULL;

    handler = CreateObsReadWriteHandlerForQuery(obsOptions);
    handler->in_computing = true;

    return handler;
}

ObsOptions *OBSConnector::getOptionFromCache(Oid foreignTableId)
{
    bool found = false;
    ObsOptions *entry = NULL;

    Oid key = 0;

    /* Get key of the hash table. */
    key = GetForeignTable(foreignTableId)->serverid;

    /* First seach from the hash table cache. */
    (void)LWLockAcquire(DfsConnectorCacheLock, LW_SHARED);
    entry = (ObsOptions *)hash_search(OBSConnectorCache, (void *)&key, HASH_FIND, &found);
    LWLockRelease(DfsConnectorCacheLock);

    /* Return if target is found. */
    if (found && (entry && entry->access_key != NULL && entry->secret_access_key != NULL && entry->address != NULL)) {
        GS_UINT32 outPutLen;
        ObsOptions *retOption = copyObsOptions(entry);
        pfree(retOption->secret_access_key);
        retOption->secret_access_key = SEC_decodeBase64(entry->secret_access_key, &outPutLen);
        return retOption;
    }

    /* Build it if target is not found for dfs table */
    ObsOptions *obsOptions = getObsOptions(foreignTableId);

    /* Put the new connector info into the hash table. */
    (void)LWLockAcquire(DfsConnectorCacheLock, LW_EXCLUSIVE);
    entry = (ObsOptions *)hash_search(OBSConnectorCache, (void *)&key, HASH_ENTER, &found);
    if (entry == NULL)
        ereport(PANIC, (errcode(ERRCODE_UNDEFINED_OBJECT), errmodule(MOD_OBS),
                        errmsg("build global OBS connect cache hash table failed")));
    if (!found) {
        Assert(obsOptions != NULL);
        entry->address = obsOptions->address;
        entry->access_key = obsOptions->access_key;
        entry->secret_access_key = obsOptions->secret_access_key;
        entry->encrypt = obsOptions->encrypt;
        entry->bucket = NULL;
        entry->prefix = NULL;
        entry->chunksize = 0;
    }
    LWLockRelease(DfsConnectorCacheLock);

    ObsOptions *retOption = copyObsOptions(entry);
    GS_UINT32 outPutLen;
    pfree(retOption->secret_access_key);
    char *decode = SEC_decodeBase64(entry->secret_access_key, &outPutLen);
    retOption->secret_access_key = (char *)decode;
    return retOption;
}

/*
 * @Description: get connector type
 * @Return:connector type
 * @See also:
 */
int OBSConnector::getType()
{
    return (int)OBS_CONNECTOR;
}

static int matchOid(const void *key1, const void *key2, Size keySize)
{
    return (int)(*(Oid *)key1 - *(Oid *)key2);
}

void InitOBSConnectorCacheLock()
{
    HASHCTL ctl;
    errno_t rc = 0;

    if (OBSConnectorCache == NULL) {
        rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check(rc, "\0", "\0");
        ctl.hcxt = g_instance.instance_context;
        ctl.keysize = sizeof(Oid);
        ctl.entrysize = sizeof(ObsOptions);
        ctl.match = (HashCompareFunc)matchOid;
        ctl.hash = (HashValueFunc)oid_hash;
        OBSConnectorCache = hash_create("OBS connector cache", 50, &ctl,
                                        HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_SHRCTX);
        if (OBSConnectorCache == NULL)
            ereport(PANIC, (errmodule(MOD_HDFS), errmsg("could not initialize OBS connector hash table")));
    }
}

bool InvalidOBSConnectorCache(Oid serverOid)
{
    bool realClean = false;
    (void)LWLockAcquire(DfsConnectorCacheLock, LW_EXCLUSIVE);
    ObsOptions *entry = (ObsOptions *)hash_search(OBSConnectorCache, (void *)&serverOid, HASH_REMOVE, NULL);

    if (entry != NULL) {
        if (entry->access_key != NULL) {
            pfree_ext(entry->access_key);
        }
        if (entry->secret_access_key != NULL) {
            OPENSSL_free(entry->secret_access_key);
            entry->secret_access_key = NULL;
        }
        if (entry->address != NULL) {
            pfree_ext(entry->address);
        }
        realClean = true;
    }
    LWLockRelease(DfsConnectorCacheLock);
    return realClean;
}
}  // namespace dfs
