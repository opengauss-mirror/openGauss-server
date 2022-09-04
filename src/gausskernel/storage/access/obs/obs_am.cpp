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
#include <libgen.h>

#define strpos(p, s) (strstr((p), (s)) != NULL ? strstr((p), (s)) - (p) : -1)

#include "access/obs/obs_am.h"
#ifndef ENABLE_LITE_MODE
#include "eSDKOBS.h"
#endif

#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/value.h"
#include "pgstat.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "storage/lock/lwlock.h"
#include "securec.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/plog.h"
#include "postmaster/alarmchecker.h"
#include "replication/walreceiver.h"

/* ----------------------------------------------------------------------------
 * Utility functions
 * ----------------------------------------------------------------------------
 */
/*
 * - Brief: Find nth sub-string from the given string, return -1 when not found
 * - Parameter:
 *      @str: where to work
 *      @N: N'th ocurrence
 *      @find: what to 'find'
 * - Return:
 *      value -1: when not found
 *      value > 0: the actual position in given string
 * Notes: position index stats from 0.
 */
int find_Nth(const char *str, unsigned N, const char *find)
{
    int cursor, pos;
    unsigned i = 0;
    const char *curptr = str;

    Assert(str != NULL);

    if (N == 0) {
        return -1;
    }

    cursor = 0;
    curptr = str + cursor;

    while (i < N) {
        pos = strpos(curptr, find);
        if (pos == -1) {
            /* Not found, return directly */
            return -1;
        }

        cursor += pos + 1;
        curptr = str + cursor;

        i++;
    }
    return (cursor - 1);
}

/*
 * - Brief: Fetch hostname, bucket, prefix in given string
 * - Parameter:
 *      @url: input URL that will be parsed into hostbame, bucket, prefix
 *      @hostname: output hostname in palloc()'ed string
 *      @bucket: output bucket in palloc()'ed string
 *      @prefix: output prefix in palloc()'ed string
 * - Return:
 *      no return value
 */
void FetchUrlProperties(const char *url, char **hostname, char **bucket, char **prefix)
{
#define LOCAL_STRING_BUFFER_SIZE 512

    int ibegin = 0;
    int iend = 0;
    char buffer[LOCAL_STRING_BUFFER_SIZE];
    char *invalid_element = NULL;
    error_t rc = EOK;
    int copylen = 0;

    /* At least we should pass-in a valid url and one of to-be fetched properties */
    Assert(url != NULL && (hostname || bucket || prefix));

    /* hostname is requred to fetch from Object's URL */
    if (hostname != NULL) {
        rc = memset_s(buffer, LOCAL_STRING_BUFFER_SIZE, 0, LOCAL_STRING_BUFFER_SIZE);
        securec_check(rc, "\0", "\0");
        ibegin = find_Nth(url, 2, "/");
        iend = find_Nth(url, 3, "/");

        copylen = iend - ibegin - 1;

        /* if hostname is invalid, goto error message */
        if (ibegin < 0 || iend < 0 || copylen <= 0) {
            invalid_element = "hostname";
            goto FETCH_URL_ERROR;
        }

        rc = strncpy_s(buffer, LOCAL_STRING_BUFFER_SIZE, url + (ibegin + 1), copylen);
        securec_check(rc, "", "");

        *hostname = pstrdup(buffer);
    }

    /* bucket is required to fetch from Object's URL */
    if (bucket != NULL) {
        rc = memset_s(buffer, LOCAL_STRING_BUFFER_SIZE, 0, LOCAL_STRING_BUFFER_SIZE);
        securec_check(rc, "\0", "\0");
        ibegin = find_Nth(url, 3, "/");
        iend = find_Nth(url, 4, "/");

        copylen = iend - ibegin - 1;

        /* if bucket name is invalid, goto error message */
        if (ibegin < 0 || iend < 0 || copylen <= 0) {
            invalid_element = "bucket";
            goto FETCH_URL_ERROR;
        }

        rc = strncpy_s(buffer, LOCAL_STRING_BUFFER_SIZE, url + (ibegin + 1), copylen);
        securec_check(rc, "", "");

        *bucket = pstrdup(buffer);
    }

    /* prefix is required to fetch from Object's URL */
    if (prefix != NULL) {
        rc = memset_s(buffer, LOCAL_STRING_BUFFER_SIZE, 0, LOCAL_STRING_BUFFER_SIZE);
        securec_check(rc, "\0", "\0");
        ibegin = find_Nth(url, 4, "/");
        /* if prefix is invalid, goto error message */
        if (ibegin < 0) {
            invalid_element = "prefix";
            goto FETCH_URL_ERROR;
        }
        copylen = strlen(url) - ibegin;

        rc = strncpy_s(buffer, LOCAL_STRING_BUFFER_SIZE, url + (iend + 1), copylen);
        securec_check(rc, "", "");

        *prefix = pstrdup(buffer);
    }
    return;

FETCH_URL_ERROR:
    ereport(ERROR,
            (errcode(ERRCODE_FDW_INVALID_OPTION_DATA), errmsg("OBS URL's %s is not valid '%s'", invalid_element, url)));
}

/*
 * @Description:  get bucket and prefix from folder name
 * @IN folderName: folder name
 * @OUT bucket: bucket name
 * @OUT prefix: perifx
 * @See also:
 * @Important: use current memory context for bucket and prefix
 */
void FetchUrlPropertiesForQuery(const char *folderName, char **bucket, char **prefix)
{
    Assert(folderName && bucket && prefix);
    Assert(!(*bucket) && !(*prefix));

    error_t rc = EOK;
    char *invalid_element = NULL;

    int ibegin = 0;
    int iend = 0;
    int bucketLen = 0;
    int prefixLen = 0;

    if (folderName[0] == '/') {
        /*  /bucket/prefix  */
        ibegin = 1;
        iend = find_Nth(folderName, 2, "/");
    } else {
        /*  bucket/prefix  */
        ibegin = 0;
        iend = find_Nth(folderName, 1, "/");
    }

    /* get bucket */
    bucketLen = iend - ibegin;
    if (bucketLen <= 0) {
        invalid_element = "bucket";
        goto FETCH_URL_ERROR2;
    }

    *bucket = (char *)palloc0(bucketLen + 1);
    rc = strncpy_s((*bucket), (bucketLen + 1), (folderName + ibegin), bucketLen);
    securec_check(rc, "", "");

    /* get prefix */
    prefixLen = strlen(folderName) - (iend + 1);
    if (prefixLen < 0) {
        invalid_element = "prefix";
        goto FETCH_URL_ERROR2;
    }

    *prefix = (char *)palloc0(prefixLen + 1);
    rc = strncpy_s((*prefix), (prefixLen + 1), (folderName + iend + 1), prefixLen);
    securec_check(rc, "", "");

    return;

FETCH_URL_ERROR2:
    ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_DATA),
                    errmsg("OBS URL's %s is not valid '%s'", invalid_element, folderName)));
}

#ifndef ENABLE_LITE_MODE
// Some Windows stuff
#ifndef FOPEN_EXTRA_FLAGS
#define FOPEN_EXTRA_FLAGS ""
#endif

// Some Unix stuff (to work around Windows issues)
#ifndef SLEEP_UNITS_PER_SECOND
#define SLEEP_UNITS_PER_SECOND 1
#endif

#define MAX_RETRIES 3
#define ERROR_MESSAGE_LEN 1024
#define ERROR_DETAIL_LEN 4096
#define MAX_PATH_LEN 1024

#define OBS_MAX_PART 10000
static int64 ifModifiedSince = -1;
static int64 ifNotModifiedSince = -1;
static char* ifMatch = 0;
static char* ifNotMatch = 0;
static int headerLen = 22;

#define OBS_CIPHER_LIST "ECDHE-ECDSA-AES128-GCM-SHA256:" \
                        "ECDHE-ECDSA-AES256-GCM-SHA384:" \
                        "ECDHE-RSA-AES128-GCM-SHA256:" \
                        "ECDHE-RSA-AES256-GCM-SHA384:" \
                        "ECDHE-ECDSA-AES128-CCM:" \
                        "ECDHE-ECDSA-AES256-CCM"

using namespace std;

extern void decryptKeyString(const char *keyStr, char destplainStr[], uint32 destplainLength, const char *obskey);

void SetObsMemoryContext(MemoryContext mctx)
{
    Assert(mctx != NULL);
    t_thrd.obs_cxt.ObsMemoryContext = mctx;
}

void UnSetObsMemoryContext(void)
{
    Assert(t_thrd.obs_cxt.ObsMemoryContext != NULL);
    t_thrd.obs_cxt.ObsMemoryContext = NULL;
}

MemoryContext GetObsMemoryContext(void)
{
    return t_thrd.obs_cxt.ObsMemoryContext;
}

/* Request results, saved as globals ----------------------------------------- */
static THR_LOCAL obs_status statusG = OBS_STATUS_OK;
static THR_LOCAL char errorMessageG[ERROR_MESSAGE_LEN] = {0};
static THR_LOCAL char errorDetailsG[ERROR_DETAIL_LEN] = {0};

/* Environment variables, saved as globals
 *
 * Bucket operation variables for those scratch-up routines like create/delete/list
 * bucket, for those  more sophisticated operations like read/write we have to to
 * CreateObsHanlder() where OBS option are pass-in in constructor of OBS handler
 *
 * static THR_LOCAL obs_protocol protocolG = OBS_PROTOCOL_HTTP;
 *
 *
 * g_CAInfo is a process shared variable which is only initialized in postmastermain.
 * Don't add static or THR_LOCAL to its definition.
 */
char *g_CAInfo = NULL;

/* static common function declearation */
int S3_init();
static char *getCAInfo();
static void getOBSCredential(char **client_crt_filepath);
static int should_retry(int &retriesG);

static obs_status responsePropertiesCallback(const obs_response_properties *properties, void *callbackData);
static void responseCompleteCallback(obs_status status, const obs_error_details *error, void *callbackData);

typedef struct ListBucketCallBackData {
    int isTruncated;
    char *nextMarker;
    char *hostName;
    const char *bucket;
    List *objectList;
} ListBucketCallBackData;

static obs_status listBucketObjectCallback(int isTruncated, const char *nextMarker, int contentsCount,
                                           const obs_list_objects_content *contents, int commonPrefixesCount,
                                           const char **commonPrefixes, void *callbackData);

/*
 * Operational routines declearations
 *  #2. read bucket content
 */
typedef struct ReadDesc {
    /* Output buffer, should be allocated by caller */
    char *buffer;
    int target_length;
    int actual_length;
} ReadDesc;

static obs_status getObjectDataCallback(int bufferSize, const char *buffer, void *callbackData);

/*
 * Operational routines declearations
 * #3. write bucket content
 */
typedef struct WriteDesc {
    BufFile *buffile;
    int target_length;
    int actual_length;
} WriteDesc;

typedef struct WriteMemDesc {
    const char *buffer_data;
    int target_length;
    int actual_length;
} WriteMemDesc;

static int putObjectDataCallback(int bufferSize, char *buffer, void *callbackData);

static inline bool shouldListBucketObjectCallbackAbort(const int isTruncated, const char *nextMarker,
                                                       ListBucketCallBackData *data)
{
    if (isTruncated) {
        Assert(nextMarker);
        /* reduce string copy when callback func be called many times in one ListObjects call */
        if (data->nextMarker == NULL) {
            data->nextMarker = pstrdup(nextMarker);
        } else {
            /* something wrong in libobs with obs server , use WARNING to safe end callback */
            if (strcmp(data->nextMarker, nextMarker) != 0) {
                ereport(WARNING,
                        (errmodule(MOD_OBS), errmsg("marker changes in listobject callback, before: %s, current: %s",
                                                    nextMarker, data->nextMarker)));

                return true;
            }
        }
    }
    return false;
}

void check_danger_character(const char *inputEnvValue)
{
    if (inputEnvValue == NULL) {
        return;
    }

    const char *dangerCharacterList[] = { ";", "`", "\\", "'", "\"", ">", "<", "&", "|", "!", NULL };
    int i = 0;

    for (i = 0; dangerCharacterList[i] != NULL; i++) {
        if (strstr(inputEnvValue, dangerCharacterList[i]) != NULL) {
            ereport(ERROR, (errmsg("Failed to check input value: invalid token \"%s\".\n", dangerCharacterList[i])));
        }
    }
}

/*
 * Get the location of the OBS credential file (client.crt)
 * Currently, the location is defined by an enviroment variable 'S3_CLIENT_CRT_FILE'
 * @IN client_crt_filepath: the pointer to store the location of client.crt
 */
static void getOBSCredential(char **client_crt_filepath)
{
    char *temp_client = NULL;
    /* description: will be replaced by a secure oriented function (will be provided by Pengfei) */
    /* description:, getenv is not thread safe */
    if (client_crt_filepath != NULL) {
        *client_crt_filepath = gs_getenv_r("S3_CLIENT_CRT_FILE");
        if (*client_crt_filepath == NULL) {
            return;
        }
        canonicalize_path(*client_crt_filepath);
        if (strlen(*client_crt_filepath) > MAX_PATH_LEN - 1)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("invalid client_crt_filepath length %lu", strlen(*client_crt_filepath))));

        check_danger_character(*client_crt_filepath);
        temp_client = (char *)MemoryContextAllocZero(
            INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), strlen(*client_crt_filepath) + 1);
        errno_t rc = strcpy_s(temp_client, strlen(*client_crt_filepath) + 1, *client_crt_filepath);
        securec_check(rc, "", "");
        *client_crt_filepath = temp_client;
    }
}

/*
 * This function is only called by Postmatermain.
 * Important: don't call erreport or elog in this function.
 */
void initOBSCacheObject()
{
    FILE *fp = NULL;
    int nFileLength = 0;
    int nReadSize = 0;
    error_t rc = EOK;
    AutoContextSwitch(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    char *client_crt_filepath = NULL;

    getOBSCredential(&client_crt_filepath);

    if (client_crt_filepath == NULL) {
        return;
    }

    if ((fp = fopen(client_crt_filepath, "rb")) == NULL) {
        pfree_ext(client_crt_filepath);
        return;
    }

    fseek(fp, 0, SEEK_END);
    nFileLength = ftell(fp);
    if (nFileLength <= 0) {
#ifndef ENABLE_LLT
        goto ERRORProcess;
#endif
    }

    rewind(fp);

    /* g_CAInfo is always in g_instance.instance_context */
    g_CAInfo = (char *)palloc0(nFileLength * sizeof(char));
    /* if g_CAInfo failed to initialize, goto error message */
    if (g_CAInfo == NULL) {
#ifndef ENABLE_LLT
        fprintf(stderr, "\nError: Memory error\n");
        goto ERRORProcess;
#endif
    }

    rc = memset_s(g_CAInfo, nFileLength, 0, nFileLength);
    securec_check(rc, "\0", "\0");
    nReadSize = fread(g_CAInfo, 1, nFileLength, fp);
    /* if failed to read the OBS CA file (client.crt), goto error message */
    if (nReadSize != nFileLength) {
#ifndef ENABLE_LLT
        fprintf(stderr, "\nError: Read certificate file error\n");
        goto ERRORProcess;
#endif
    }

    fclose(fp);
    pfree_ext(client_crt_filepath);
    return;

#ifndef ENABLE_LLT
ERRORProcess:
    fclose(fp);
    if (g_CAInfo != NULL) {
        pfree(g_CAInfo);
        g_CAInfo = NULL;
    }
#endif

    pfree_ext(client_crt_filepath);
}

/*
 * Not like function initOBSCacheObject, getCAInfo is called during SQL processing.
 * In this situation, we can throw error.
 * On-Premise or on-cloud condition if we want to use obs foreign table we must set
 * related system environment. gCAInfo will be initialized in postmastermain and when
 * call getCAInfo to get gCAInfo, it should be not null.
 */
static char *getCAInfo()
{
    if (g_CAInfo == NULL) {
#ifndef ENABLE_LLT
        /* keep compiler silent, return is useless */
        return NULL;
#endif
    }
    return g_CAInfo;
}

static int should_retry(int &retriesG)
{
#ifndef ENABLE_LLT
    if (retriesG--) {
        sleep(t_thrd.obs_cxt.retrySleepInterval);
        /* Next sleep 1 second longer */
        t_thrd.obs_cxt.retrySleepInterval++;
        return 1;
    }

    /* Reset retry sleep interval */
    t_thrd.obs_cxt.retrySleepInterval = 1 * SLEEP_UNITS_PER_SECOND;
#endif

    return 0;
}

/* response properties callback
 *
 * This callback does the same thing for every request type: prints out the
 * properties if the user has requested them to be so
 */
static obs_status responsePropertiesCallback(const obs_response_properties *properties, void *callbackData)
{
    return OBS_STATUS_OK;
}

/*  response complete callback
 *
 * This callback does the same thing for every request type: saves the status
 * and error stuff in global variables
 */
static const char s3ErrorRessource[] = " Resource: ";
static const char s3ErrorDetail[] = " Further Details: ";
static const char s3ErrorExtraDetail[] = " Extra Details:";

static void responseCompleteCallback(obs_status status, const obs_error_details *error, void *callbackData)
{
    statusG = status;
    errorMessageG[0] = 0;
    errorDetailsG[0] = 0;

    /* copy  error message to errorMessageG */
    int ret = 0;
    if (error != NULL && error->message) {
        ret = snprintf_s(errorMessageG, sizeof(errorMessageG), sizeof(errorMessageG) - 1, "message: %s",
                         error->message);
        securec_check_ss(ret, "\0", "\0");
    }

    /*
     * Compose the error details message now, although we might not use it.
     * Can't just save a pointer to [error] since it's not guaranteed to last
     * beyond this callback
     */
    int len = 0;
    if (error != NULL && error->resource) {
#ifndef ENABLE_LLT
        /* errorDetailsG has enough space to hold error message */
        if (len + strlen(error->resource) + strlen(s3ErrorRessource) >= ERROR_DETAIL_LEN)
            return;

        ret = snprintf_s(&(errorDetailsG[len]), sizeof(errorDetailsG) - len, sizeof(errorDetailsG) - len - 1,
                         " Resource: %s\n", error->resource);
        securec_check_ss(ret, "\0", "\0");
        len += ret;
#endif
    }

    if (error != NULL && error->further_details) {
#ifndef ENABLE_LLT
        /* errorDetailsG has enough space to hold error message */
        if (len + strlen(error->further_details) + strlen(s3ErrorDetail) >= ERROR_DETAIL_LEN)
            return;

        ret = snprintf_s(&(errorDetailsG[len]), sizeof(errorDetailsG) - len, sizeof(errorDetailsG) - len - 1,
                         " Further Details: %s\n", error->further_details);
        securec_check_ss(ret, "\0", "\0");
        len += ret;
#endif
    }

    if (error != NULL && error->extra_details_count) {
        /* errorDetailsG has enough space to hold error message */
        if (len + strlen(s3ErrorExtraDetail) >= ERROR_DETAIL_LEN)
            return;

        ret = snprintf_s(&(errorDetailsG[len]), sizeof(errorDetailsG) - len, sizeof(errorDetailsG) - len - 1, "%s",
                         " Extra Details:\n");
        securec_check_ss(ret, "\0", "\0");
        len += ret;
        int i;
        for (i = 0; i < error->extra_details_count; i++) {
            /* try errorDetailsG has enough space to hold error message */
            if (len + strlen(error->extra_details[i].name) + strlen(error->extra_details[i].value) + 3 /* length of " :
                                                                                                          " */
                >= ERROR_DETAIL_LEN)
                return;

            ret = snprintf_s(&(errorDetailsG[len]), sizeof(errorDetailsG) - len, sizeof(errorDetailsG) - len - 1,
                             " %s: %s\n", error->extra_details[i].name, error->extra_details[i].value);
            securec_check_ss(ret, "\0", "\0");
            len += ret;
        }
    }
}

static obs_status listServiceCallback(const char *ownerId, const char *bucketName, int64_t creationDateSeconds,
                                      const char *ownerDisplayName, void *callbackData)
{
    /* Do nothing. */
    list_service_data *data = (list_service_data *)callbackData;
    statusG = data->ret_status;

    return OBS_STATUS_OK;
}

/* clean list object callback function */
typedef void (*cleanListCallback)(List *list);

/*
 * #1:OBS related callback functions to list buckets for dist obs foreign table.
 */
static obs_status listBucketObjectCallbackForAnalyze(int isTruncated, const char *nextMarker, int contentsCount,
                                                     const obs_list_objects_content *contents, int commonPrefixesCount,
                                                     const char **commonPrefixes, void *callbackData)
{
    Assert(callbackData);

    ListBucketCallBackData *data = (ListBucketCallBackData *)callbackData;
    data->isTruncated = isTruncated;

    if (shouldListBucketObjectCallbackAbort(isTruncated, nextMarker, data)) {
        return OBS_STATUS_AbortedByCallback;
    }

    int rc = 0;

    for (int i = 0; i < contentsCount; i++) {
        const obs_list_objects_content *content = &(contents[i]);

        if (content->size <= 0) {
            continue;
        }

        SplitInfo *splitinfo = makeNode(SplitInfo);

        /* format perfix string, for example,
         * gsobs://10.175.38.120/gaussdbcheck/obscheck/test_rescan
         */
        size_t filePathLen = strlen("gsobs://") + strlen(data->hostName) + 1 + strlen(data->bucket) + 1 +
                             strlen(content->key) + 1;
        char *filePath = (char *)palloc0(filePathLen);
        rc = snprintf_s(filePath, filePathLen, (filePathLen - 1), "gsobs://%s/%s/%s", data->hostName, data->bucket,
                        content->key);
        securec_check_ss(rc, "", "");
        filePath[filePathLen - 1] = '\0';

        splitinfo->filePath = filePath;
        splitinfo->ObjectSize = content->size;

        data->objectList = lappend(data->objectList, splitinfo);
    }

    return OBS_STATUS_OK;
}

/*
 * #1:OBS related callback functions to list buckets
 */
static obs_status listBucketObjectCallback(int isTruncated, const char *nextMarker, int contentsCount,
                                           const obs_list_objects_content *contents, int commonPrefixesCount,
                                           const char **commonPrefixes, void *callbackData)
{
    Assert(callbackData);

    ListBucketCallBackData *data = (ListBucketCallBackData *)callbackData;
    data->isTruncated = isTruncated;

    if (shouldListBucketObjectCallbackAbort(isTruncated, nextMarker, data)) {
        return OBS_STATUS_AbortedByCallback;
    }

    for (int i = 0; i < contentsCount; i++) {
        const obs_list_objects_content *content = &(contents[i]);
        char *key = pstrdup(content->key);
        data->objectList = lappend(data->objectList, key);
    }
    return OBS_STATUS_OK;
}

/*
 * @Description: list object callback function
 * @IN isTruncated: is results truncated
 * @IN nextMarker: next marker for next list object if truncated
 * @IN contentsCount: size of contents
 * @IN contents: object content array
 * @IN commonPrefixesCount: common prefixes conunt
 * @IN commonPrefixes:common prefixes
 * @IN/OUT callbackData: callbackdata: parameter pass to callback function
 * @Return: s3 retrun status
 * @See also:
 */
static obs_status listBucketObjectCallbackForQuery(int isTruncated, const char *nextMarker, int contentsCount,
                                                   const obs_list_objects_content *contents, int commonPrefixesCount,
                                                   const char **commonPrefixes, void *callbackData)
{
    Assert(callbackData);
    ListBucketCallBackData *data = (ListBucketCallBackData *)callbackData;
    data->isTruncated = isTruncated;

    if (shouldListBucketObjectCallbackAbort(isTruncated, nextMarker, data)) {
        return OBS_STATUS_AbortedByCallback;
    }

    /* get length of "/bucket/"  */
    size_t bucket_prefix_len = strlen(data->bucket) + strlen("//");
    int rc = 0;

    for (int i = 0; i < contentsCount; i++) {
        const obs_list_objects_content *content = &(contents[i]);

        SplitInfo *splitinfo = makeNode(SplitInfo);

        /* format perfix string */
        size_t filePathLen = bucket_prefix_len + strlen(content->key) + 1;
        char *filePath = (char *)palloc(filePathLen);
        rc = snprintf_s(filePath, filePathLen, (filePathLen - 1), "/%s/%s", data->bucket, content->key);
        securec_check_ss(rc, "", "");
        filePath[filePathLen - 1] = '\0';

        splitinfo->filePath = filePath;
        splitinfo->ObjectSize = content->size;
        /* fill later */
        splitinfo->prefixSlashNum = 0;
        splitinfo->eTag = pstrdup(content->etag);

        data->objectList = lappend(data->objectList, splitinfo);
    }

    return OBS_STATUS_OK;
}

/*
 * @Description: list object
 * @IN pobsOption: obs option, control context and other options
 * @IN prefix: prefx for object match
 * @IN cleanList: clean  list object callback function
 * @IN plistBucketHandler: s3 callback handler
 * @IN/OUT callbackdata: parameter pass to callback function , if truncated it have the informatin for next call
 * @Return: not zero for object list is truncated, continue call list_bucket_objects_loop for remain objects
 * @See also:
 */
int list_bucket_objects_loop(obs_options *pobsOption, char *prefix, cleanListCallback cleanList,
                             obs_list_objects_handler *plistObjectsHandler, ListBucketCallBackData *callbackdata,
                             bool reportError = true)
{
    Assert(pobsOption && prefix && cleanList && plistObjectsHandler && callbackdata);
    Assert(pobsOption->bucket_options.bucket_name);

    /* the marker in last call ListObjects, will be NULL at first time */
    char *marker = callbackdata->nextMarker;
    callbackdata->nextMarker = NULL;

    /* the last object list already pass to caller */
    callbackdata->objectList = NIL;

    int retriesG = MAX_RETRIES;

    do {
        /* clear callback data with free memory when retry */
        callbackdata->isTruncated = 0;
        cleanList(callbackdata->objectList);
        callbackdata->objectList = NIL;
        if (callbackdata->nextMarker != NULL) {
            pfree(callbackdata->nextMarker);
            callbackdata->nextMarker = NULL;
        }

        /* call list_bucket_objects */
        list_bucket_objects(pobsOption, prefix, marker /* marker */, NULL /* delimiter */, 0 /* maxkeys */,
                            plistObjectsHandler, (void *)callbackdata);
    } while (obs_status_is_retryable(statusG) && should_retry(retriesG));

    if (statusG != OBS_STATUS_OK) {
        /* description: is callbackdata->objectList undefined ? */
        pgstat_report_waitevent(WAIT_EVENT_END);
        PROFILING_OBS_ERROR(list_length(callbackdata->objectList), DSRQ_LIST);
        if (reportError == true) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_STATUS),
                 errmsg("Fail to list bucket object in node:%s with error code: %s, %s the bucket name: %s, prefix "
                        "name: %s",
                        g_instance.attr.attr_common.PGXCNodeName, obs_get_status_name(statusG), errorMessageG,
                        pobsOption->bucket_options.bucket_name, prefix)));
       } else {
            ereport(WARNING,
                (errcode(ERRCODE_INVALID_STATUS),
                errmsg("Fail to list bucket object in node:%s with error code: %s, %s the bucket name: %s, prefix "
                        "name: %s",
                        g_instance.attr.attr_common.PGXCNodeName, obs_get_status_name(statusG), errorMessageG,
                        pobsOption->bucket_options.bucket_name, prefix)));
        }

        if (strlen(errorDetailsG) > 0) {
            ereport(DEBUG1, (errmsg("Fail to list bucket object in node:%s, detail: %s",
                                    g_instance.attr.attr_common.PGXCNodeName, errorDetailsG)));
        }
    }

    /* clean the last marker */
    if (marker != NULL) {
        pfree(marker);
        marker = NULL;
    }

    return callbackdata->isTruncated;
}

/*
 * - Brief: List the objects of bucket in order to analyze dist obs foreign table.
 * - Parameter:
 *      @uri: input URL that will be parsed into hostbame, bucket, prefix
 *      @encrypt: input encrypt flag to use Http or Https
 *      @access_key: input authorized access key to OBS
 *      @secret_access_key: input authorized secret access key to OBS
 * - Return:
 *      the list of the objects on OBS bucket.
 */
List *list_bucket_objects_analyze(const char *uri, bool encrypt, const char *access_key, const char *secret_access_key)
{
    char *hostname = NULL;
    char *bucket = NULL;
    char *prefix = NULL;

    /* Parse uri into hostname, bucket, prefix */
    FetchUrlProperties(uri, &hostname, &bucket, &prefix);
    Assert(hostname && bucket && prefix);

    obs_options option;
    /* init the option data by obs api */
    init_obs_options(&option);

    /* fill the bucket context content */
    option.bucket_options.host_name = hostname;
    option.bucket_options.bucket_name = bucket;
    option.bucket_options.protocol = encrypt ? OBS_PROTOCOL_HTTPS : OBS_PROTOCOL_HTTP;
    option.bucket_options.uri_style = is_ip_address_format(hostname) ? OBS_URI_STYLE_PATH : OBS_URI_STYLE_VIRTUALHOST;
    option.bucket_options.access_key = (char *)access_key;
    option.bucket_options.secret_access_key = (char *)secret_access_key;
    option.bucket_options.certificate_info = encrypt ? t_thrd.obs_cxt.pCAInfo : NULL;
    option.request_options.ssl_cipher_list = OBS_CIPHER_LIST;

    obs_list_objects_handler listBucketHandler = {{ &responsePropertiesCallback, &responseCompleteCallback },
                                                  &listBucketObjectCallbackForAnalyze };

    /* init call back data */
    ListBucketCallBackData callbackdata;
    callbackdata.isTruncated = 0;
    callbackdata.nextMarker = NULL;
    callbackdata.bucket = bucket;
    callbackdata.hostName = hostname;
    callbackdata.objectList = NIL;

    int isTruncated = 0;
    List *loop_result = NIL;

    do {
        /* check interrupts */
        CHECK_FOR_INTERRUPTS();

        /* get objects, if not complete , the isTruncated will not zero and callbackdata.nextMarker will be set  for
         * next loop */
        isTruncated = list_bucket_objects_loop(&option, prefix, list_free_deep, &listBucketHandler, &callbackdata);

        /*  move object list elements to return list , and callbackdata clean in next loop when call
         * list_bucket_objects_loop */
        loop_result = list_concat(loop_result, callbackdata.objectList);
    } while (isTruncated != 0);

    /* hostname bucket prefix alloc memory in FetchUrlProperties from CurrentMemoryContext */
    pfree_ext(hostname);
    pfree_ext(bucket);
    pfree_ext(prefix);

    return loop_result;
}

/*
 * - Brief: List the objects of obs bucket
 * - Parameter:
 *      @uri: input URL that will be parsed into hostbame, bucket, prefix
 *      @encrypt: input encrypt flag to use Http or Https
 *      @access_key: input authorized access key to OBS
 *      @secret_access_key: input authorized secret access key to OBS
 * - Return:
 *      the list of the objects on OBS bucket.
 */
List *list_obs_bucket_objects(const char *uri, bool encrypt, const char *access_key, const char *secret_access_key)
{
    char *hostname = NULL;
    char *bucket = NULL;
    char *prefix = NULL;

    List *result_list = NIL;
    obs_options option;

    ereport(DEBUG1, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_OBS),
                     errmsg("The location string: %s in current list bucket objects.", uri)));

    /* Parse uri into hostname, bucket, prefix */
    FetchUrlProperties(uri, &hostname, &bucket, &prefix);
    Assert(hostname && bucket && prefix);

    /* init the option data by obs api */
    init_obs_options(&option);

    /* fill the bucket context content */
    option.bucket_options.host_name = hostname;
    option.bucket_options.bucket_name = bucket;
    option.bucket_options.protocol = encrypt ? OBS_PROTOCOL_HTTPS : OBS_PROTOCOL_HTTP;
    option.bucket_options.uri_style = is_ip_address_format(hostname) ? OBS_URI_STYLE_PATH : OBS_URI_STYLE_VIRTUALHOST;
    option.bucket_options.access_key = (char *)access_key;
    option.bucket_options.secret_access_key = (char *)secret_access_key;
    option.bucket_options.certificate_info = encrypt ? t_thrd.obs_cxt.pCAInfo : NULL;
    option.request_options.ssl_cipher_list = OBS_CIPHER_LIST;

    obs_list_objects_handler listBucketHandler = {{ &responsePropertiesCallback, &responseCompleteCallback },
                                                  &listBucketObjectCallback };

    /* init call back data */
    ListBucketCallBackData callbackdata;
    callbackdata.isTruncated = 0;
    callbackdata.nextMarker = NULL;
    callbackdata.bucket = bucket;
    callbackdata.hostName = NULL;
    callbackdata.objectList = NIL;

    int isTruncated = 0;
    List *loop_result = NIL;

    do {
        /* check interrupts */
        CHECK_FOR_INTERRUPTS();

        /* get objects, if not complete , the isTruncated will not zero and callbackdata.nextMarker will be set  for
         * next loop */
        isTruncated = list_bucket_objects_loop(&option, prefix, list_free_deep, &listBucketHandler, &callbackdata);

        /*  move object list elements to return list , and callbackdata clean in next loop when call
         * list_bucket_objects_loop */
        loop_result = list_concat(loop_result, callbackdata.objectList);
    } while (isTruncated != 0);

    ListCell *cell = NULL;
    foreach (cell, loop_result) {
        char *key = (char *)lfirst(cell);

        /* Construct object uri (full format which starting from gsobs:// to \0 ) */
        StringInfo si = makeStringInfo();
        appendStringInfo(si, "gsobs://%s/%s/%s", hostname, bucket, key);
        char *os = pstrdup(si->data);
        result_list = lappend(result_list, makeString(os));
    }

    /* release loop result list */
    list_free_deep(loop_result);
    loop_result = NIL;

    /* hostname bucket prefix alloc memory in FetchUrlProperties from CurrentMemoryContext */
    pfree_ext(hostname);
    pfree_ext(bucket);
    pfree_ext(prefix);

    return result_list;
}

/*
 * - Brief: List the objects of obs bucket
 * - Parameter:
 *      @uri: input URL that will be parsed into hostbame, bucket, prefix
 *      @encrypt: input encrypt flag to use Http or Https
 *      @access_key: input authorized access key to OBS
 *      @secret_access_key: input authorized secret access key to OBS
 * - Return:
 *      the list of the object's key on OBS bucket.
 */
List *listObsObjects(OBSReadWriteHandler *handler, bool reportError)
{
    List *result_list = NIL;

    ereport(DEBUG1, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_OBS),
                    errmsg("The location string: %s in current list bucket objects.", handler->m_object_info.key)));

    obs_list_objects_handler listBucketHandler = {{ &responsePropertiesCallback, &responseCompleteCallback },
                                                  &listBucketObjectCallback };

    /* init call back data */
    ListBucketCallBackData callbackdata;
    callbackdata.isTruncated = 0;
    callbackdata.nextMarker = NULL;
    callbackdata.bucket = NULL;
    callbackdata.hostName = NULL;
    callbackdata.objectList = NIL;

    int isTruncated = 0;

    do {
        /* check interrupts */
        CHECK_FOR_INTERRUPTS();

        /* get objects, if not complete , the isTruncated will not zero and callbackdata.nextMarker will be set  for
         * next loop */
        isTruncated = list_bucket_objects_loop(&(handler->m_option), handler->m_object_info.key,
                                               list_free_deep, &listBucketHandler, &callbackdata, reportError);

        /*  move object list elements to return list , and callbackdata clean in next loop when call
         * list_bucket_objects_loop */
        result_list = list_concat(result_list, callbackdata.objectList);
    } while (isTruncated != 0);

    return result_list;
}
/*
 * @Description: list object for given bucket name and prefix, call list_bucket_objects_loop for get all objects
 * @IN handler:obs handler
 * @IN bucket:bucket name
 * @IN prefix:prefix for object match
 * @Return: object list
 * @See also: list_bucket_objects_loop
 * @Important: use current memory context fro list and list elements
 */
List *list_bucket_objects_for_query(OBSReadWriteHandler *handler, const char *bucket, char *prefix)
{
    Assert(handler && bucket && prefix);

    obs_options *option = &(handler->m_option);
    option->bucket_options.bucket_name = (char *)bucket;

    obs_list_objects_handler listBucketHandler = {{ &responsePropertiesCallback, &responseCompleteCallback },
                                                  &listBucketObjectCallbackForQuery };

    /* init call back data */
    ListBucketCallBackData callbackdata;
    callbackdata.isTruncated = 0;
    callbackdata.nextMarker = NULL;
    callbackdata.bucket = bucket;
    callbackdata.hostName = NULL;
    callbackdata.objectList = NIL;

    int isTruncated = 0;
    List *return_list = NIL;

    do {
        /* check interrupts */
        CHECK_FOR_INTERRUPTS();

        /* get objects, if not complete , the isTruncated will not zero and callbackdata.nextMarker will be set  for
         * next loop */
        isTruncated = list_bucket_objects_loop(option, prefix, release_object_list, &listBucketHandler, &callbackdata);

        /* move object list elements to return list , and callbackdata clean in next loop when call
         * list_bucket_objects_loop */
        return_list = list_concat(return_list, callbackdata.objectList);
    } while (isTruncated != 0);

    return return_list;
}

/*
 * @Description:  deep release list which return by list_bucket_objects_for_query
 * @IN/OUT object_list: return list of list_bucket_objects_for_query
 */
void release_object_list(List *object_list)
{
    if (object_list == NIL) {
        return;
    }

    ListCell *cell = NULL;
    foreach (cell, object_list) {
        SplitInfo *splitinfo = (SplitInfo *)lfirst(cell);

        if (splitinfo->filePath) {
            pfree(splitinfo->filePath);
            splitinfo->filePath = NULL;
        }

        if (splitinfo->eTag) {
            pfree(splitinfo->eTag);
            splitinfo->eTag = NULL;
        }

        pfree(splitinfo);
        splitinfo = NULL;
    }

    list_free(object_list);
    object_list = NIL;
}

/*
 * #2:OBS related callback functions to read bucket content
 */
static obs_status getObjectDataCallback(int bufferSize, const char *buffer, void *callbackData)
{
    ReadDesc *rb = (ReadDesc *)callbackData;
    int need_read = 0;
    error_t rc = EOK;

    Assert(rb != NULL && rb->target_length >= rb->actual_length);

    if (rb->target_length > rb->actual_length) {
        need_read = rb->target_length - rb->actual_length;
    }

    if (need_read == 0) {
        return OBS_STATUS_OK;
    }

    int nread = (bufferSize < need_read) ? bufferSize : need_read;
    rc = memcpy_s(rb->buffer + rb->actual_length, nread, buffer, nread);
    securec_check(rc, "", "");

    rb->actual_length += nread;

    return OBS_STATUS_OK;
}

/*
 * - Brief: read the objects of bucket from OBS
 * - Parameter:
 *      @handler: OBSReadWriteHandler constains OBS operation properties
 *      @output_buffer:  output buffer to store the read data
 *      @len: target length of for the handler to read
 * - Return:
 *      The length of the read objects by the handler.
 */
size_t read_bucket_object(OBSReadWriteHandler *handler, char *output_buffer, uint32_t len)
{
    ReadDesc rb;
    rb.buffer = output_buffer;
    rb.target_length = len;
    int retriesG = MAX_RETRIES;

    handler->properties.get_cond.byte_count = len;

    obs_get_object_handler getObjectHandler = {{ &responsePropertiesCallback, &responseCompleteCallback },
                                               &getObjectDataCallback };

    do {
        rb.actual_length = 0;
        get_object(&handler->m_option,                    /* obs option, including object's bucket context */
                   &handler->m_object_info,               /* object's prefix (key) and version ID */
                   &handler->properties.get_cond,         /* get condition, the start cursor and read len */
                   (server_side_encryption_params *)NULL, /* server_side_encryption_params */
                   &getObjectHandler, (void *)&rb);
    } while (obs_status_is_retryable(statusG) && should_retry(retriesG));

    if (statusG == OBS_STATUS_InvalidRange) {
        /* Reach the object end */
        return (size_t)0;
    } else if (statusG != OBS_STATUS_OK) {
        pgstat_report_waitevent(WAIT_EVENT_END);
        PROFILING_OBS_ERROR(rb.actual_length, DSRQ_READ);

        if (t_thrd.role == BARRIER_CREATOR && t_thrd.postmaster_cxt.HaShmData->is_cross_region) {
            ereport(LOG, (errcode(ERRCODE_INVALID_STATUS),
                errmsg("Datanode '%s' fail to read OBS object bucket:'%s' key:'%s' with OBS error code:%s %s",
                g_instance.attr.attr_common.PGXCNodeName, handler->m_option.bucket_options.bucket_name,
                handler->m_object_info.key, obs_get_status_name(statusG), errorMessageG),
                errdetail_log("%s", errorDetailsG)));
            return -1;
        }
        /* Otherwise to error-out unexpected OBS read errors */
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Datanode '%s' fail to read OBS object bucket:'%s' key:'%s' with OBS error code:%s %s",
                               g_instance.attr.attr_common.PGXCNodeName, handler->m_option.bucket_options.bucket_name,
                               handler->m_object_info.key, obs_get_status_name(statusG), errorMessageG),
                        errdetail_log("%s", errorDetailsG)));
    }

    /* Update OBS reader cursor */
    handler->properties.get_cond.start_byte += rb.actual_length;

    return (size_t)((uint)rb.actual_length);
}

/*
 * #3 OBS related callback functions to write object content
 */
static int putObjectDataCallback(int bufferSize, char *buffer, void *callbackData)
{
    WriteDesc *wb = (WriteDesc *)callbackData;
    int need_write = 0;

    Assert(wb->target_length >= wb->actual_length);

    if (wb->target_length > wb->actual_length) {
        need_write = wb->target_length - wb->actual_length;
    }

    if (need_write == 0) {
        /* finish */
        return need_write;
    }

    int to_write = (need_write < bufferSize) ? need_write : bufferSize;

    /* Read data from Buffile into write buffer */
    if (to_write != (int)BufFileRead(wb->buffile, buffer, (uint)to_write)) {
#ifndef ENABLE_LLT
        ereport(ERROR, (errcode(ERRCODE_FLUSH_DATA_SIZE_MISMATCH),
                        errmsg("Fail to flush data content to OBS in buffile offset ['%d'] to_write ['%d']",
                               wb->actual_length, to_write)));
#endif
    }

    wb->actual_length += to_write;

    return to_write;
}

static int putTmpObjectDataCallback(int bufferSize, char *buffer, void *callbackData)
{
    WriteMemDesc *wb = (WriteMemDesc *)callbackData;
    int need_write = 0;
    errno_t rc = EOK;

    Assert(wb->target_length >= wb->actual_length);

    if (wb->target_length > wb->actual_length) {
        need_write = wb->target_length - wb->actual_length;
    }

    if (need_write == 0) {
        /* finish */
        return need_write;
    }

    int to_write = (need_write < bufferSize) ? need_write : bufferSize;

    /* Read data from buffer into write buffer */
    rc = memcpy_s(buffer, (uint)bufferSize, wb->buffer_data + wb->actual_length, (uint)to_write);
    securec_check(rc, "", "");

    wb->actual_length += to_write;

    return to_write;
}

/*
 *  - Brief: write the data to OBS bucket
 * - Parameter:
 *      @handler: OBSReadWriteHandler constains OBS operation properties
 *      @buffile: data buffer to write to OBS
 *      @total_len: target length of for the handler to write
 * - Return:
 *      The length of the write data to OBS by the handler.
 * - Notice:
 *      the caller should check return value of
 *      write_bucket_object, libobs service maybe not write
 *      total_len byte to OBS.
 */
size_t write_bucket_object(OBSReadWriteHandler *handler, BufFile *buffile, uint32_t total_len)
{
    Assert(handler != NULL && buffile != NULL);

    handler->properties.put_cond.byte_count = total_len;
    int retriesG = MAX_RETRIES;

    obs_put_object_handler putObjectHandler = {{ &responsePropertiesCallback, &responseCompleteCallback },
                                               &putObjectDataCallback };

    WriteDesc wb;
    wb.buffile = buffile;
    wb.target_length = total_len;

    PROFILING_OBS_START();
    pgstat_report_waitevent(WAIT_EVENT_OBS_WRITE);
    do {
        /*
         * try to put this object again,
         * so reset actual length , and file offset of buffer file.
         */
        int seek_res = BufFileSeek(wb.buffile, 0, 0L, SEEK_SET);
        wb.actual_length = 0;
        if (seek_res != 0) {
            ereport(ERROR, (errcode_for_file_access(),
                            errmsg("Datanode '%s' fail to seek buffer file be (0, 0) after reset actual length",
                                   g_instance.attr.attr_common.PGXCNodeName)));
        } else if (MAX_RETRIES - 1 == retriesG) {
            ereport(LOG, (errmsg("Datanode '%s' try to write OBS object %s with OBS error code:%s %s",
                                 g_instance.attr.attr_common.PGXCNodeName, handler->m_url, obs_get_status_name(statusG),
                                 errorMessageG),
                          errdetail_log("%s", errorDetailsG)));
        }

        put_object(&handler->m_option, handler->m_object_info.key, total_len, &(handler->properties.put_cond), NULL,
                   &putObjectHandler, &wb);
    } while (obs_status_is_retryable(statusG) && should_retry(retriesG));

    if (statusG != OBS_STATUS_OK) {
        pgstat_report_waitevent(WAIT_EVENT_END);
        PROFILING_OBS_ERROR(wb.actual_length, DSRQ_WRITE);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Datanode '%s' fail to write OBS object %s with OBS error code:%s %s",
                               g_instance.attr.attr_common.PGXCNodeName, handler->m_url, obs_get_status_name(statusG),
                               errorMessageG),
                        errdetail_log("%s", errorDetailsG)));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);
    PROFILING_OBS_END_WRITE(wb.actual_length);

    return (size_t)((uint)wb.actual_length);
}

/**
 * @Description: write the temp file
 * @return if success, return 0, otherwise return -1;
 */
int writeObsTempFile(OBSReadWriteHandler *handler, const char *bufferData, int dataSize)
{
    Assert(handler != NULL && bufferData != NULL);

    int ret = 0;
    int retriesG = 0;
    const int barrierCreatorRetries = 1;
    if (t_thrd.role == BARRIER_CREATOR && t_thrd.postmaster_cxt.HaShmData->is_cross_region) {
        retriesG = barrierCreatorRetries;
    } else {
        retriesG = MAX_RETRIES;
    }

    handler->properties.put_cond.byte_count = dataSize;

    obs_put_object_handler putObjectHandler = {{ &responsePropertiesCallback, &responseCompleteCallback },
                                               &putTmpObjectDataCallback };

    WriteMemDesc wb;
    wb.buffer_data = bufferData;
    wb.target_length = dataSize;
    wb.actual_length = 0;

    PROFILING_OBS_START();
    pgstat_report_waitevent(WAIT_EVENT_OBS_WRITE);
    do {
        wb.actual_length = 0;
        put_object(&handler->m_option, handler->m_object_info.key, dataSize, &(handler->properties.put_cond), NULL,
                   &putObjectHandler, &wb);
    } while (obs_status_is_retryable(statusG) && should_retry(retriesG));

    if (statusG != OBS_STATUS_OK) {
        pgstat_report_waitevent(WAIT_EVENT_END);
        PROFILING_OBS_ERROR(wb.actual_length, DSRQ_WRITE);
        ereport(LOG, (errmsg("NameNode '%s' fail to write OBS temp object %s with OBS error code:%s %s",
                             g_instance.attr.attr_common.PGXCNodeName, handler->m_url, obs_get_status_name(statusG),
                             errorMessageG),
                      errdetail_log("%s", errorDetailsG)));
        ret = -1;
    }
    pgstat_report_waitevent(WAIT_EVENT_END);
    PROFILING_OBS_END_WRITE(wb.actual_length);

    return ret;
}

int deleteOBSObject(OBSReadWriteHandler *handler)
{
    int retriesG = MAX_RETRIES;

    obs_response_handler responseHandler = { responsePropertiesCallback, &responseCompleteCallback };

    do {
        delete_object(&handler->m_option, &handler->m_object_info, &responseHandler, 0);
    } while (obs_status_is_retryable(statusG) && should_retry(retriesG));

    if (statusG == OBS_STATUS_OK) {
        return 0;
    } else {
        return -1;
    }
}

/* ************External Interface*************** */
int S3_init()
{
    obs_status status = OBS_STATUS_BUTT;

    t_thrd.obs_cxt.pCAInfo = getCAInfo();

    /* description: "china" */
    if ((status = obs_initialize(OBS_INIT_ALL)) != OBS_STATUS_OK) {
#ifndef ENABLE_LLT
        fprintf(stderr, "Failed to initialize libobs: %s\n", obs_get_status_name(status));
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Node '%s' fail to initialize libobs with OBS error code:%s",
                               g_instance.attr.attr_common.PGXCNodeName, obs_get_status_name(statusG))));

        /* keep compiler silent */
        return -1;
#endif
    }

    return 0;
}

/* ----------------------------------------------------------------------------
 * Common export fucntions
 * ----------------------------------------------------------------------------
 */
/*
 * - Brief: Create OBS handler by given type
 * - Parameter:
 *      @object_url: input url to we will create a handler to operate it
 *      @type: handler type, can be read/write ...
 *
 * - Return:
 *      Return the created OBS handler
 */
OBSReadWriteHandler *CreateObsReadWriteHandler(const char *object_url, OBSHandlerType type, ObsCopyOptions *options)
{
    /*
     * We do OBS handler and its owning variables by palloc() in specified memory
     * context which was assigned in constructor of OBSStream
     */
    MemoryContext oldcontext = MemoryContextSwitchTo(t_thrd.obs_cxt.ObsMemoryContext);

    errno_t rc = EOK;

    OBSReadWriteHandler *handler = (OBSReadWriteHandler *)palloc(sizeof(OBSReadWriteHandler));
    rc = memset_s(handler, sizeof(OBSReadWriteHandler), 0, sizeof(OBSReadWriteHandler));
    securec_check(rc, "", "");

    ObsOptions *obsOptions = (ObsOptions *)palloc0(sizeof(ObsOptions));
    obsOptions->encrypt = options->encrypt;
    obsOptions->access_key = options->access_key;
    obsOptions->secret_access_key = options->secret_access_key;
    obsOptions->chunksize = options->chunksize;
    obsOptions->address = NULL;
    obsOptions->bucket = NULL;
    obsOptions->prefix = NULL;
    handler->m_obs_options = obsOptions;
    handler->m_type = type;
    handler->m_url = pstrdup(object_url);

    /* Initialize obs option */
    init_obs_options(&handler->m_option);

    /* Get hostname, bucket, prefix part */
    FetchUrlProperties(handler->m_url, &handler->m_hostname, &handler->m_bucket, &handler->m_object_info.key);

    t_thrd.obs_cxt.pCAInfo = getCAInfo();

    Assert(options->access_key != NULL && options->secret_access_key != NULL);

    /* Initialize bucket context object */
    handler->m_option.bucket_options.host_name = handler->m_hostname;
    handler->m_option.bucket_options.bucket_name = handler->m_bucket;
    handler->m_option.bucket_options.protocol = options->encrypt ? OBS_PROTOCOL_HTTPS : OBS_PROTOCOL_HTTP;
    handler->m_option.bucket_options.uri_style = is_ip_address_format(handler->m_hostname) ? OBS_URI_STYLE_PATH
                                                                                            : OBS_URI_STYLE_VIRTUALHOST;
    handler->m_option.bucket_options.access_key = options->access_key;
    handler->m_option.bucket_options.secret_access_key = options->secret_access_key;
    handler->m_option.bucket_options.certificate_info = options->encrypt ? t_thrd.obs_cxt.pCAInfo : NULL;
    handler->m_option.request_options.ssl_cipher_list = OBS_CIPHER_LIST;

    handler->m_object_info.version_id = NULL;

    /* set handler type */
    ObsReadWriteHandlerSetType(handler, type);

    MemoryContextSwitchTo(oldcontext);

    return handler;
}

/*
 * @Description: create obs handler
 * @IN options: obs options
 * @Return: obs handler
 * @See also:
 * @Important: use current memory context for obs handler
 */
OBSReadWriteHandler *CreateObsReadWriteHandlerForQuery(ObsOptions *options)
{
    // memory alloc on current memory which set by caller
    OBSReadWriteHandler *handler = (OBSReadWriteHandler *)palloc0(sizeof(OBSReadWriteHandler));

    handler->m_url = NULL;
    handler->m_hostname = options->address ? pstrdup(options->address) : NULL;
    handler->m_bucket = NULL;
    handler->m_type = OBS_UNKNOWN;
    handler->m_obs_options = options;

    t_thrd.obs_cxt.pCAInfo = getCAInfo();

    Assert(options->access_key != NULL && options->secret_access_key != NULL);

    /* Initialize obs option */
    init_obs_options(&handler->m_option);

    /* Initialize bucket context object */
    handler->m_option.bucket_options.host_name = handler->m_hostname;
    handler->m_option.bucket_options.bucket_name = handler->m_bucket;
    handler->m_option.bucket_options.protocol = options->encrypt ? OBS_PROTOCOL_HTTPS : OBS_PROTOCOL_HTTP;
    handler->m_option.bucket_options.uri_style = is_ip_address_format(handler->m_hostname) ? OBS_URI_STYLE_PATH
                                                                                            : OBS_URI_STYLE_VIRTUALHOST;
    handler->m_option.bucket_options.access_key = options->access_key;
    handler->m_option.bucket_options.secret_access_key = options->secret_access_key;
    handler->m_option.bucket_options.certificate_info = options->encrypt ? t_thrd.obs_cxt.pCAInfo : NULL;
    handler->m_option.request_options.ssl_cipher_list = OBS_CIPHER_LIST;

    handler->m_object_info.key = NULL;
    handler->m_object_info.version_id = NULL;

    return handler;
}

/*
 * @Description: set handler for read or write
 * @IN/OUT handler: obs handler
 * @IN type: OBS_READ for read, OBS_WRITE for write
 * @See also:
 */
void ObsReadWriteHandlerSetType(OBSReadWriteHandler *handler, OBSHandlerType type)
{
    Assert(handler);

    handler->m_type = type;

    switch (type) {
        case OBS_READ:
            init_get_properties(&handler->properties.get_cond);
            break;

        case OBS_WRITE:
            init_put_properties(&handler->properties.put_cond);
            break;

        default:
            ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_HANDLE), errmsg("unsupported operation in OBS handler layer")));
    }
}

/*
 * - Brief: De-constructor of ObsReadWriteHandler
 * - Parameter:
 *      @handler: the handler to be freed
 *      @obsQueryType, whether obs query foreign table.
 */
void DestroyObsReadWriteHandler(OBSReadWriteHandler *handler, bool obsQueryType)
{
    Assert(handler != NULL);

    if (handler->m_url) {
        pfree(handler->m_url);
        handler->m_url = NULL;
    }

    if (handler->m_hostname) {
        pfree(handler->m_hostname);
        handler->m_hostname = NULL;
    }

    if (handler->m_bucket) {
        pfree(handler->m_bucket);
        handler->m_bucket = NULL;
    }

    if (obsQueryType) {
        /*
         * if in computing pool, can not free the sak using free.
         * the m_obs_options generated on CN of DWS, and transfer it to DN
         * conputing pool, and decrypt it. free it using pfree.
         * if in DWS, this option use SEC_encodeBase64,  get it from obs cache by,
         * copy it and use SEC_decodeBase64 function, si we free obsOptions->secret_access_key
         * by using free.
         */
        freeObsOptions(handler->m_obs_options, !handler->in_computing);
    }

    pfree_ext(handler);
}

/*
 * @Description: check obs server adress is valid
 * @IN hostName: obs server adress
 * @IN ak: access ey
 * @IN sk: secret access ey
 * @IN encrypt: true for https, false for http
 * @See also:
 */
void checkOBSServerValidity(char *hostName, char *ak, char *sk, bool encrypt)
{
#define ENCRYPT_STR_PREFIX "encryptstr"
#define DEST_CIPHER_LENGTH 1024

    char decryptSecretAccessKeyStr[DEST_CIPHER_LENGTH] = {0};

    int retriesG = MAX_RETRIES;

    obs_options option;
    init_obs_options(&option);

    list_service_data data;
    (void)memset_s(&data, sizeof(list_service_data), 0, sizeof(list_service_data));
    data.allDetails = 1;

    if (0 == strncmp(sk, ENCRYPT_STR_PREFIX, strlen(ENCRYPT_STR_PREFIX))) {
        decryptKeyString(sk, decryptSecretAccessKeyStr, DEST_CIPHER_LENGTH, NULL);
        sk = decryptSecretAccessKeyStr;
    }

    option.bucket_options.host_name = hostName;
    option.bucket_options.bucket_name = 0;
    option.bucket_options.protocol = encrypt ? OBS_PROTOCOL_HTTPS : OBS_PROTOCOL_HTTP;
    option.bucket_options.uri_style = is_ip_address_format(hostName) ? OBS_URI_STYLE_PATH : OBS_URI_STYLE_VIRTUALHOST;
    option.bucket_options.access_key = ak;
    option.bucket_options.secret_access_key = sk;
    option.bucket_options.certificate_info = encrypt ? t_thrd.obs_cxt.pCAInfo : NULL;
    option.request_options.ssl_cipher_list = OBS_CIPHER_LIST;

    obs_list_service_obs_handler listServiceHandle = {{ &responsePropertiesCallback, &responseCompleteCallback },
                                                      &listServiceCallback };

    do {
        /* check interrupts */
        CHECK_FOR_INTERRUPTS();

        list_bucket_obs(&option, &listServiceHandle, &data);
    } while (obs_status_is_retryable(statusG) && should_retry(retriesG));

    if (statusG != OBS_STATUS_OK) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Fail to connect OBS host %s in node:%s with error code: %s %s", hostName,
                               g_instance.attr.attr_common.PGXCNodeName, obs_get_status_name(statusG), errorMessageG),
                        errdetail_log("%s", errorDetailsG)));
    }
}

ObsOptions *copyObsOptions(ObsOptions *from)
{
    ObsOptions *obsOptions = (ObsOptions *)palloc0(sizeof(ObsOptions));

    if (from->access_key) {
        obsOptions->access_key = pstrdup(from->access_key);
    }
    if (from->address) {
        obsOptions->address = pstrdup(from->address);
    }
    if (from->bucket) {
        obsOptions->bucket = pstrdup(from->bucket);
    }
    if (from->prefix) {
        obsOptions->prefix = pstrdup(from->prefix);
    }

    if (from->secret_access_key) {
        obsOptions->secret_access_key = pstrdup(from->secret_access_key);
    }

    obsOptions->chunksize = from->chunksize;
    obsOptions->serverOid = from->serverOid;
    obsOptions->encrypt = from->encrypt;

    return obsOptions;
}

void freeObsOptions(ObsOptions *obsOptions, bool useSimpleFree)
{
    if (obsOptions != NULL) {
        if (obsOptions->access_key) {
            pfree_ext(obsOptions->access_key);
        }
        if (obsOptions->address) {
            pfree_ext(obsOptions->address);
        }
        if (obsOptions->bucket) {
            pfree_ext(obsOptions->bucket);
        }
        if (obsOptions->prefix) {
            pfree_ext(obsOptions->prefix);
        }

        if (obsOptions->secret_access_key) {
            errno_t rc = EOK;
            rc = memset_s(obsOptions->secret_access_key, strlen(obsOptions->secret_access_key),
                0, strlen(obsOptions->secret_access_key));
            securec_check(rc, "\0", "\0");

            if (useSimpleFree) {
                free(obsOptions->secret_access_key);
            } else {
                pfree(obsOptions->secret_access_key);
            }
            obsOptions->secret_access_key = NULL;
        }

        pfree_ext(obsOptions);
    }
}

/* ----------------------------------------------------------------------------
 * Utility functions
 * ----------------------------------------------------------------------------
 */
/*
 * - Brief: Check address is IPAddress, if IPAddress return true ,else return false.
 * - Parameter:
 *      @addr: address, like 'xxx.xxx.xxx.xxx' or 'xxx.xxx.xxx.xxx:xxxxx'
 * - Return:
 *      true for IPAddress, false for not IPAddress
 */
bool is_ip_address_format(const char *addr)
{
    Assert(NULL != addr);
    if (addr == NULL) {
        return false;
    }

    if (strlen(addr) == 0) {
        return false;
    }

    char temp[4];
    int count = 0;

    while (*addr != '\0') {
        int ipAddressIndex = 0;

        if (*addr == '0')
            return false;

        /* split a section */
        while (*addr != '\0' && *addr != ':' && *addr != '.' && ipAddressIndex < 4) {
            /* not a number */
            if (*addr < '0' || *addr > '9') {
                return false;
            }
            temp[ipAddressIndex++] = *addr;
            addr++;
        }
        if (ipAddressIndex == 4)
            return false;

        temp[ipAddressIndex] = '\0';
        int num = atoi(temp);
        if (num < 0 || num > 255)
            return false;

        count++;
        if (*addr == '\0' || *addr == ':') {
            if (count == 4)
                return true;
            else
                return false;
        }
        addr++;
    }
    return false;
}

ArchiveConfig *getArchiveConfig()
{
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    if (walrcv->archive_slot == NULL) {
        walrcv->archive_slot = GetArchiveRecoverySlot();
        if (walrcv->archive_slot == NULL) {
            ereport(LOG, (errmsg("Cannot get replication slots!")));
            return NULL;
        }
    }
    return &walrcv->archive_slot->archive_config;
}

static void fillBucketContext(OBSReadWriteHandler *handler, const char* key, ArchiveConfig *obs_config = NULL,
                                bool shortenConnTime = false)
{
    ArchiveConfig *archive_obs = NULL;
    errno_t rc = EOK;
    char xlogfpath[MAXPGPATH] = {0};

    if (obs_config != NULL) {
        archive_obs = obs_config;
    } else {
        archive_obs = getArchiveConfig();
    }

    if (archive_obs == NULL || archive_obs->conn_config == NULL || archive_obs->conn_config->obs_address == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Cannot get obs bucket config from replication slots")));
    }

    /* Initialize obs option */
    init_obs_options(&handler->m_option);

    handler->m_option.bucket_options.host_name = archive_obs->conn_config->obs_address;
    handler->m_option.bucket_options.bucket_name = archive_obs->conn_config->obs_bucket;
    handler->m_option.bucket_options.protocol = OBS_PROTOCOL_HTTPS;
    handler->m_option.bucket_options.uri_style = is_ip_address_format(handler->m_option.bucket_options.host_name) ?
                                                 OBS_URI_STYLE_PATH : OBS_URI_STYLE_VIRTUALHOST;
    handler->m_option.bucket_options.access_key = archive_obs->conn_config->obs_ak;
    handler->m_option.bucket_options.secret_access_key = archive_obs->conn_config->obs_sk;

    t_thrd.obs_cxt.pCAInfo = getCAInfo();
    handler->m_option.bucket_options.certificate_info = t_thrd.obs_cxt.pCAInfo;
    handler->m_option.request_options.ssl_cipher_list = OBS_CIPHER_LIST;
    if (shortenConnTime == true) {
        handler->m_option.request_options.connect_time = 1000;
    }
    if (t_thrd.role == BARRIER_CREATOR && t_thrd.postmaster_cxt.HaShmData->is_cross_region) {
        handler->m_option.request_options.connect_time = 100;
        handler->m_option.request_options.max_connected_time = 1;
    }

    /* Fill in obs full file path */
    if (strncmp(key, "global_barrier_records", headerLen) != 0) {
        rc = snprintf_s(xlogfpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", archive_obs->archive_prefix, key);
        securec_check_ss(rc, "\0", "\0");
    } else {
        char pathPrefix[MAXPGPATH] = {0};
        rc = strcpy_s(pathPrefix, MAXPGPATH, archive_obs->archive_prefix);
        securec_check_ss(rc, "\0", "\0");
        if (!IS_PGXC_COORDINATOR) {
            char *p = strrchr(pathPrefix, '/');
            if (p == NULL) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Obs path prefix is invalid")));
            }
            *p = '\0';
        }
        rc = snprintf_s(xlogfpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", pathPrefix, key);
        securec_check_ss(rc, "\0", "\0");
    }

    handler->m_object_info.key = pstrdup(xlogfpath);
    handler->m_object_info.version_id = NULL;
}


size_t obsRead(const char* fileName, const int offset, char *buffer, const int length, ArchiveConfig *obs_config)
{
    OBSReadWriteHandler *handler;
    errno_t rc = EOK;
    size_t readLength = 0;

    if ((fileName == NULL) || (buffer == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("The parameter cannot be NULL")));
    }

    handler = (OBSReadWriteHandler *)palloc(sizeof(OBSReadWriteHandler));
    rc = memset_s(handler, sizeof(OBSReadWriteHandler), 0, sizeof(OBSReadWriteHandler));
    securec_check(rc, "", "");

    /* Initialize bucket context object */
    fillBucketContext(handler, fileName, obs_config);

    /* set handler type */
    ObsReadWriteHandlerSetType(handler, OBS_READ);


    handler->properties.get_cond.start_byte = offset;
    readLength = read_bucket_object(handler, buffer, length);

    handler->properties.get_cond.start_byte = 0;

    pfree(handler->m_object_info.key);
    handler->m_object_info.key = NULL;
    pfree_ext(handler);

    return readLength;
}

int obsWrite(const char* fileName, const char *buffer, const int bufferLength, ArchiveConfig *obs_config)
{
    OBSReadWriteHandler *handler;
    int ret = 0;
    errno_t rc = EOK;

    if ((fileName == NULL) || (buffer == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("The parameter cannot be NULL")));
    }

    handler = (OBSReadWriteHandler *)palloc(sizeof(OBSReadWriteHandler));
    rc = memset_s(handler, sizeof(OBSReadWriteHandler), 0, sizeof(OBSReadWriteHandler));
    securec_check(rc, "", "");

    /* Initialize bucket context object */
    fillBucketContext(handler, fileName, obs_config);

    /* set handler type */
    ObsReadWriteHandlerSetType(handler, OBS_WRITE);

    ret = writeObsTempFile(handler, buffer, bufferLength);

    pfree(handler->m_object_info.key);
    handler->m_object_info.key = NULL;
    pfree_ext(handler);

    return ret;
}

int obsDelete(const char* fileName, ArchiveConfig *obs_config)
{
    OBSReadWriteHandler *handler;
    int ret = 0;
    errno_t rc = EOK;

    handler = (OBSReadWriteHandler *)palloc(sizeof(OBSReadWriteHandler));
    rc = memset_s(handler, sizeof(OBSReadWriteHandler), 0, sizeof(OBSReadWriteHandler));
    securec_check(rc, "", "");

    /* Initialize bucket context object */
    fillBucketContext(handler, fileName, obs_config);

    ret = deleteOBSObject(handler);

    pfree(handler->m_object_info.key);
    handler->m_object_info.key = NULL;
    pfree_ext(handler);

    return ret;
}

List* obsList(const char* prefix, ArchiveConfig *obs_config, bool reportError, bool shortenConnTime)
{
    OBSReadWriteHandler *handler;
    errno_t rc = EOK;
    List* fileNameList;

    handler = (OBSReadWriteHandler *)palloc(sizeof(OBSReadWriteHandler));
    rc = memset_s(handler, sizeof(OBSReadWriteHandler), 0, sizeof(OBSReadWriteHandler));
    securec_check(rc, "", "");

    /* Initialize bucket context object */
    fillBucketContext(handler, prefix, obs_config, shortenConnTime);

    fileNameList = listObsObjects(handler, reportError);

    pfree(handler->m_object_info.key);
    handler->m_object_info.key = NULL;
    pfree_ext(handler);

    return fileNameList;
}


void fillObsOption(obs_options *option, ArchiveConfig *obs_config = NULL)
{
    ArchiveConfig *archive_obs = NULL;

    if (obs_config != NULL) {
        archive_obs = obs_config;
    } else {
        archive_obs = getArchiveConfig();
    }

    if (archive_obs == NULL || archive_obs->conn_config == NULL || archive_obs->conn_config->obs_address == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Cannot get obs bucket config from replication slots")));
    }


    option->bucket_options.host_name = archive_obs->conn_config->obs_address;
    option->bucket_options.bucket_name = archive_obs->conn_config->obs_bucket;
    option->bucket_options.protocol = OBS_PROTOCOL_HTTPS;
    option->bucket_options.uri_style = OBS_URI_STYLE_PATH;
    option->bucket_options.access_key = archive_obs->conn_config->obs_ak;
    option->bucket_options.secret_access_key = archive_obs->conn_config->obs_sk;
    t_thrd.obs_cxt.pCAInfo = getCAInfo();
    option->bucket_options.certificate_info = t_thrd.obs_cxt.pCAInfo;
    option->request_options.ssl_cipher_list = OBS_CIPHER_LIST;
    if (t_thrd.role == BARRIER_CREATOR && t_thrd.postmaster_cxt.HaShmData->is_cross_region) {
        option->request_options.connect_time = 100;
        option->request_options.max_connected_time = 1;
    }

}


/*
 * regist function responsePropertiesCallbackForWrite to show Response Properties
 */
static obs_status responsePropertiesCallbackForWrite(const obs_response_properties* properties, void* callbackData)
{
    OBSWriteData* data = (OBSWriteData*)callbackData;
    int len = 0;
    int nRet = 0;

    if ((properties == NULL) || (data == NULL)) {
        return OBS_STATUS_OK;
    }
    if (properties->etag) {
        len = strlen(properties->etag);
        nRet = memcpy_s(data->eTag, sizeof(data->eTag), properties->etag, (size_t)len);
        securec_check_c(nRet, "\0", "\0");
        data->eTag[len] = '\0';
    }

    return OBS_STATUS_OK;
}

/*
 * regist function responsePropertiesCallbackForOpen to show Response Properties
 */
static obs_status responsePropertiesCallbackForOpen(const obs_response_properties* properties, void* callbackData)
{
    (void)callbackData;
    return OBS_STATUS_OK;
}

/*
 * print upload information
 */
obs_status CompleteMultipartUploadCallback(
    const char* location, const char* bucket, const char* key, const char* eTag, void* callbackData)
{
    (void)callbackData;
    ereport(DEBUG1, (errmsg("location = %s \n bucket = %s \n eTag = %s \n", location, bucket, eTag)));
    return OBS_STATUS_OK;
}

/*
 * regist function responsePropertiesCallbackForRead to show Response Properties
 */
static obs_status responsePropertiesCallbackForRead(const obs_response_properties* properties, void* callbackData)

{
    return OBS_STATUS_OK;
}


static obs_status openReadOBSFileCallback(int isTruncated, const char* nextMarker, int contentsCount,
    const obs_list_objects_content* contents, int commonPrefixesCount, const char** commonPrefixes, void* callbackData)
{
    OBSReadFile* data = (OBSReadFile*)callbackData;
    if (contentsCount != 1) {
        ereport(LOG, (errmsg("openReadOBSFileCallback: The function is incorrect. value number is %d. ", 
            contentsCount)));
    }
    if (data != NULL) {
        const obs_list_objects_content* content = &(contents[0]);
        data->fileSize = content->size;
    }
    return OBS_STATUS_OK;
}


/*
 * return the length it need to read
 */
static obs_status readOBSFileCallback(int bufferSize, const char* buffer, void* callbackData)
{
    int nRet = 0;
    OBSReadFile* obsFile = (OBSReadFile*)callbackData;

    if (obsFile == NULL) {
        return OBS_STATUS_OK;
    }

    if ((buffer != NULL) && (bufferSize > 0)) {
        nRet = memcpy_s((char*)obsFile->bufData + obsFile->actualLen,
            (size_t)(READ_BLOCK_SIZE - obsFile->actualLen),
            buffer,
            (size_t)bufferSize);
        securec_check_c(nRet, "\0", "\0");
    }

    /* obsFile->actualLen : the length of that we have read in buffer-block-size.
           After function GetObject() it be resetted to 0
     * obsFile->offset    : the length that we have read from obs file.
     */
    obsFile->actualLen += bufferSize;
    obsFile->offset += bufferSize;

    if (obsFile->fileSize == obsFile->offset) {
        obsFile->obs_eof = true;
    }
    return ((bufferSize < obsFile->byteCount && (obsFile->obs_eof == false)) ? OBS_STATUS_AbortedByCallback
                                                                             : OBS_STATUS_OK);

}

obs_status head_properties_callback(const obs_response_properties *properties, void *callback_data)
{
    return OBS_STATUS_OK;
}

static void head_complete_callback(obs_status status,
                                     const obs_error_details *error,
                                     void *callback_data)
{
    HEAD_OBJECT_DATA *data = (HEAD_OBJECT_DATA *)callback_data;
    data->ret_status = status;
    statusG = status;
}


/*
 * Create multipart upload file
 */
void* createOBSFile(const char* file_path, const char* mode, ArchiveConfig *obs_config)
{
    OBSWriteFile* obsFile = NULL;
    char uploadId[OBS_MAX_UPLOAD_ID_LEN] = {0};
    int nRet = 0;
    int retriesG = MAX_RETRIES;

    /* init the option data by obs api */
    obs_options option;
    init_obs_options(&option);

    /* fill the bucket context content */
    fillObsOption(&option, obs_config);

    /* struct putProperties */
    obs_put_properties putProperties = {0};
    init_put_properties(&putProperties);

    obs_response_handler responseInitiateHandler = {responsePropertiesCallbackForWrite, responseCompleteCallback};

    int ulIndex = 0;
    int len = 0;

    // initiate obsFile
    obsFile = (OBSWriteFile*)palloc(sizeof(OBSWriteFile));
    nRet = memset_s(obsFile, sizeof(OBSWriteFile), 0, sizeof(OBSWriteFile));
    securec_check(nRet, "", "");

    // initiate eTag list
    obsFile->eTagList = (char**)palloc0(sizeof(char*) * OBS_MAX_PART);

    for (ulIndex = 0; ulIndex < OBS_MAX_PART; ++ulIndex) {
        obsFile->eTagList[ulIndex] = (char*)palloc0(sizeof(char) * OBS_MAX_ETAG_LEN);
    }

    // initiate part number
    obsFile->partNum = 0;

    ereport(LOG, (errmsg("[OBS] before InitiateMultipartUpload Create OBS file %s.", file_path)));

    do {
        initiate_multi_part_upload(&option,  (char*)file_path,  sizeof(uploadId),  uploadId,  &putProperties,
            (server_side_encryption_params*)NULL, &responseInitiateHandler, NULL);
    } while (obs_status_is_retryable(statusG) && should_retry(retriesG));

    ereport(LOG, (errmsg("[OBS] after InitiateMultipartUpload Create OBS file %s.", file_path)));

    if (statusG != OBS_STATUS_OK) {
        // release the memory resource
        for (ulIndex = 0; ulIndex < OBS_MAX_PART; ++ulIndex) {
            pfree_ext(obsFile->eTagList[ulIndex]);
        }
        pfree_ext(obsFile->eTagList);
        pfree_ext(obsFile);
        ereport(ERROR, (errmsg("[OBS] Create OBS file %s failed. [OBS] Error: %s. [OBS] Error: %s", 
            file_path, obs_get_status_name(statusG), errorDetailsG)));
        return NULL;
    }

    /* write uploadId and file_path into obsFile */
    len = strlen(uploadId);
    nRet = memcpy_s(obsFile->uploadId, sizeof(obsFile->uploadId), uploadId, (size_t)len);
    securec_check_c(nRet, "\0", "\0");
    obsFile->uploadId[len] = '\0';

    if (file_path != NULL) {
        len = strlen(file_path);
        nRet = memcpy_s(obsFile->filePath, sizeof(obsFile->filePath), file_path, (size_t)len);
        securec_check_c(nRet, "\0", "\0");
        obsFile->filePath[len] = '\0';
        ereport(LOG, (errmsg("[OBS] Create OBS file %s successfully.", file_path)));
    }

    return (OBSFile*)obsFile;
}

/*
 * write data to OBS
 */
int writeOBSData(const void* write_data, size_t size, size_t len, OBSFile* fp, size_t* writeSize,
    ArchiveConfig *obs_config)
{
    int eTagLen = 0;
    int nRet = 0;
    OBSWriteData OBSDataBuf;
    int retriesG = MAX_RETRIES;

    if (writeSize == NULL) {
        return -1;
    }
    *writeSize = 0;

    /* init the option data by obs api */
    obs_options option;
    init_obs_options(&option);

    /* fill the bucket context content */
    fillObsOption(&option, obs_config);

    /* struct putProperties */
    obs_put_properties putProperties = {0};
    init_put_properties(&putProperties);

    obs_upload_handler responseHandler = {
        {responsePropertiesCallbackForWrite, responseCompleteCallback}, putTmpObjectDataCallback};

    OBSWriteFile* obsFile = (OBSWriteFile*)fp;

    if (obsFile == NULL) {
        return -1;
    }

    /* init OBSDataBuf */
    (void)memset_s(&OBSDataBuf, sizeof(OBSDataBuf), 0, sizeof(OBSDataBuf));
    OBSDataBuf.bufData = (void*)write_data;
    OBSDataBuf.byteCount = (size * len);

    /* init obs_upload part info */
    obs_upload_part_info uploadPartInfo;
    (void)memset_s(&uploadPartInfo, sizeof(obs_upload_part_info), 0, sizeof(obs_upload_part_info));
    uploadPartInfo.part_number = (obsFile->partNum) + 1;
    uploadPartInfo.upload_id = obsFile->uploadId;

    ereport(DEBUG1, (errmsg("[OBS] before call UploadPart, file: %s, partNum: %d",
        obsFile->filePath, obsFile->partNum + 1)));

    /* multipart upload. The segment size range is [5MB, 5GB], but the size of the last segment is [0,5GB] when
     * performing a merge segment operation. There is also a range limit for the number of uploaded segments, which
     * range is [1,10000].
     */
    do {
        OBSDataBuf.actualLen = 0;    // reset offset at the begin of upload
        upload_part(&option,  obsFile->filePath,  &uploadPartInfo,  OBSDataBuf.byteCount,  &putProperties,
            0,  &responseHandler,  &OBSDataBuf);
    } while (obs_status_is_retryable(statusG) && should_retry(retriesG));
    if (statusG != OBS_STATUS_OK) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Write data to OBS file %s failed [PartNum: %d] with error code: %s %s",
                            obsFile->filePath, obsFile->partNum, obs_get_status_name(statusG), errorMessageG),
                        errdetail_log("%s", errorDetailsG)));

        return -1;
    }

    eTagLen = strlen(OBSDataBuf.eTag);
    (void)memset_s(
        obsFile->eTagList[obsFile->partNum], sizeof(char) * OBS_MAX_ETAG_LEN, 0, sizeof(char) * OBS_MAX_ETAG_LEN);
    ereport(DEBUG1, (errmsg("[OBS] obsFile->partNum: %d ETAG: %s", obsFile->partNum, OBSDataBuf.eTag)));
    ereport(DEBUG1, (errmsg("[OBS] Get etag of data for file %s successfully.", obsFile->filePath)));

    /* remeber the eTag */
    nRet = memcpy_s(
        obsFile->eTagList[obsFile->partNum], sizeof(char) * OBS_MAX_ETAG_LEN, OBSDataBuf.eTag, (size_t)eTagLen);
    securec_check_c(nRet, "\0", "\0");
    obsFile->eTagList[obsFile->partNum][eTagLen] = '\0';
    ++(obsFile->partNum);

    ereport(LOG, (errmsg("[OBS] Write data to OBS file %s successfully. PartNum: %d",
        obsFile->filePath, obsFile->partNum)));

    *writeSize = (size_t)(OBSDataBuf.actualLen);
    return 0;
}

/*
 * close OBS file
 */
int closeOBSFile(void* filePtr, ArchiveConfig *obs_config)
{
    int ulIndex = 0;
    int retriesG = MAX_RETRIES;
    OBSWriteFile* obsFile = (OBSWriteFile*)filePtr;

    /* init the option data by obs api */
    obs_options option;
    init_obs_options(&option);

    /* fill the bucket context content */
    fillObsOption(&option, obs_config);

    /* struct putProperties */
    obs_put_properties putProperties = {0};
    init_put_properties(&putProperties);

    obs_complete_multi_part_upload_handler responseCompleteHandler = {
        {responsePropertiesCallbackForWrite, responseCompleteCallback}, CompleteMultipartUploadCallback};
    obs_response_handler responseAbortHandler = {&responsePropertiesCallbackForWrite, &responseCompleteCallback};

    /* upload info */
    obs_complete_upload_Info* uploadInfo = NULL;

    if (obsFile == NULL) {
        return 0;
    }

    /* init obs_complete_upload_Info */
    uploadInfo = (obs_complete_upload_Info*)palloc(sizeof(obs_complete_upload_Info) * obsFile->partNum);

    (void)memset_s(uploadInfo,
        sizeof(obs_complete_upload_Info) * obsFile->partNum,
        0,
        sizeof(obs_complete_upload_Info) * obsFile->partNum);

    for (ulIndex = 0; ulIndex < obsFile->partNum; ++ulIndex) {
        /* write partNumber and eTag into uploadInfo */
        uploadInfo[ulIndex].part_number = (unsigned int)(ulIndex + 1);
        uploadInfo[ulIndex].etag = obsFile->eTagList[ulIndex];

        ereport(DEBUG1, (errmsg("partNumber: %u", uploadInfo[ulIndex].part_number)));
        ereport(DEBUG1, (errmsg("eTag: %s", uploadInfo[ulIndex].etag)));
    }

    do {
        complete_multi_part_upload(&option, obsFile->filePath, obsFile->uploadId, obsFile->partNum,
            uploadInfo, &putProperties, &responseCompleteHandler, 0);
    } while (obs_status_is_retryable(statusG) && should_retry(retriesG));

    retriesG = MAX_RETRIES;
    // check whether complete operation is succeed or failed
    if (statusG != OBS_STATUS_OK) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("[OBS] Complete OBS file failed.")));
        if (statusG < OBS_STATUS_AccessDenied) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("[OBS] Error: %s", obs_get_status_name(statusG))));
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                errmsg("[OBS] Error: %s %s", obs_get_status_name(statusG), errorDetailsG)));
        }

        do {
            abort_multi_part_upload(&option, obsFile->filePath, obsFile->uploadId, &responseAbortHandler, 0);
        } while (obs_status_is_retryable(statusG) && should_retry(retriesG));

        // check whether abort operation is succeed or failed
        ereport(LOG, (errmsg("[OBS] Abort OBS file result: %s.", obs_get_status_name(statusG))));

        // release the memory resource
        for (ulIndex = 0; ulIndex < OBS_MAX_PART; ++ulIndex) {
            pfree_ext(obsFile->eTagList[ulIndex]);
        }
        pfree_ext(obsFile->eTagList);
        pfree_ext(obsFile);
        pfree_ext(uploadInfo);

        return -1;
    }

    ereport(LOG, (errmsg("[OBS] Complete OBS file %s successful.", obsFile->filePath)));
    ereport(DEBUG1, (errmsg("[OBS] Close OBS file!")));

    for (ulIndex = 0; ulIndex < OBS_MAX_PART; ++ulIndex) {
        pfree_ext(obsFile->eTagList[ulIndex]);
    }
    pfree_ext(obsFile->eTagList);
    pfree_ext(obsFile);
    pfree_ext(uploadInfo);

    return 0;
}

/*
 * open obs file
 */
void* openReadOBSFile(const char* file_path, const char* mode, ArchiveConfig *obs_config)
{
    OBSReadFile* obsFile = NULL;
    int retriesG = MAX_RETRIES;
    /* create obs_option for list bucket objects */
    obs_options option;
    init_obs_options(&option);

    /* struct bucket context for obs_option */
    fillObsOption(&option, obs_config);

    obs_list_objects_handler listBucketHandler = {
        {responsePropertiesCallbackForOpen, responseCompleteCallback}, openReadOBSFileCallback};
    int len = 0;
    int nRet = 0;

    if (file_path == NULL) {
        return NULL;
    }

    obsFile = (OBSReadFile*)palloc(sizeof(OBSReadFile));

    (void)memset_s(obsFile, sizeof(OBSReadFile), 0, sizeof(OBSReadFile));
    len = strlen(file_path);
    nRet = memcpy_s(obsFile->filePath, sizeof(obsFile->filePath), file_path, (size_t)len);
    securec_check_c(nRet, "\0", "\0");
    obsFile->filePath[len] = '\0';
    obsFile->obs_eof = false;
    obsFile->obs_error = false;
    obsFile->offset = 0;
    obsFile->bufData = NULL;
    obsFile->byteCount = 0;
    obsFile->actualLen = 0;
    obsFile->fileSize = 0;

    ereport(DEBUG1, (errmsg("[OBS] before ListObjects open OBS file %s", file_path)));

    do {
        list_bucket_objects(&option, (char *)file_path, 0, 0, 1, &listBucketHandler, (void*)obsFile);
    } while (obs_status_is_retryable(statusG) && should_retry(retriesG));

    ereport(DEBUG1, (errmsg("[OBS] before ListObjects open OBS file %s", file_path)));

    if (statusG != OBS_STATUS_OK) {
        pfree_ext(obsFile);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("[OBS] OBS file %s does not exist when list_bucket_objects, with error code: %s %s",
                            file_path, obs_get_status_name(statusG), errorMessageG),
                        errdetail_log("%s", errorDetailsG)));
        return NULL;
    }
    return obsFile;
}

/*
 * do restore for read OBS file
 * -1: failed
 *  0: sucess
 *  x: continue
 */
int readOBSFile(char* data, size_t size, size_t len, void* fp, size_t* readSize, ArchiveConfig *obs_config)
{
    OBSReadFile* obsFile = (OBSReadFile*)fp;

    if ((obsFile == NULL) || (data == NULL) || (readSize == NULL)) {
        return -1;
    }

    /* struct obs_option */
    obs_options option;
    init_obs_options(&option);
    fillObsOption(&option, obs_config);

    /* set obs_object_info, including key and version ID */
    obs_object_info object_info;
    (void)memset_s(&object_info, sizeof(obs_object_info), 0, sizeof(obs_object_info));
    object_info.key = obsFile->filePath;
    object_info.version_id = (char*)NULL;

    /* init get_condition info */
    obs_get_conditions getcondition;
    (void)memset_s(&getcondition, sizeof(obs_get_conditions), 0, sizeof(obs_get_conditions));
    init_get_properties(&getcondition);

    /* fill get_condition , indicates start_byte and byte_count */
    getcondition.if_modified_since = ifModifiedSince;
    getcondition.if_not_modified_since = ifNotModifiedSince;
    getcondition.if_match_etag = ifMatch;
    getcondition.if_not_match_etag = ifNotMatch;
    getcondition.start_byte = obsFile->offset;
    getcondition.byte_count = size * len;

    obs_get_object_handler getObjectHandler = {
        {responsePropertiesCallbackForRead, responseCompleteCallback}, readOBSFileCallback};
    size_t totalReadSize = 0;
    *readSize = 0;

    if (obsFile->obs_error) {
        return -1;
    }

    if (obsFile->obs_eof) {
        return 0;
    }

    ereport(DEBUG1, (errmsg("[OBS] before GetObject read OBS file %s", obsFile->filePath)));

    do {
        obsFile->bufData = (char*)data + totalReadSize;
        obsFile->actualLen = 0;

        get_object(&option,                       /* obs option, including object's bucket context */
            &object_info,                         /* object's prefix (key) and version ID */
            &getcondition,                        /* get condition, the start cursor and read len */
            (server_side_encryption_params*)NULL, /* server_side_encryption_params */
            &getObjectHandler,
            (void*)obsFile);

        totalReadSize = totalReadSize + obsFile->actualLen;

        if (statusG == OBS_STATUS_InvalidRange || obsFile->obs_eof) {
            ereport(LOG, (errmsg("[OBS] OBS file %s reach the end", obsFile->filePath)));
            obsFile->obs_eof = true;
            *readSize = totalReadSize;
            return 0;
        } else if (statusG != OBS_STATUS_OK) {
            obsFile->obs_eof = false;
            obsFile->obs_error = true;
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("[OBS] Read from OBS file %s failed, with error code: %s %s",
                            obsFile->filePath, obs_get_status_name(statusG), errorMessageG),
                        errdetail_log("%s", errorDetailsG)));
            return -1;
        }
    } while (totalReadSize < (size * len) && !obsFile->obs_eof);

    *readSize = totalReadSize;
    ereport(DEBUG1, (errmsg("[OBS] after GetObject read OBS file %s", obsFile->filePath)));
    return 0;
}

/*
 * OBS no need to do something, only release the memory
 */
int closeReadOBSFile(void* fp)
{
    OBSReadFile* obsFile = (OBSReadFile*)fp;
    pfree_ext(obsFile);
    return 0;
}

/*
 * check whether the file exists in the OBS
 */
bool checkOBSFileExist(const char* file_path, ArchiveConfig *obs_config)
{
    HEAD_OBJECT_DATA data;
    data.object_length = 0;
    data.ret_status = OBS_STATUS_OK;
    int retriesG = 0;
    const int barrierCreatorRetries = 1;
    if (t_thrd.role == BARRIER_CREATOR && t_thrd.postmaster_cxt.HaShmData->is_cross_region) {
        retriesG = barrierCreatorRetries;
    } else {
        retriesG = MAX_RETRIES;
    }
    int rc = 0;
    char obsFilePath[MAXPGPATH] = {0};
    /* create obs_option for list bucket objects */
    obs_options option;
    init_obs_options(&option);
    /* struct bucket context for obs_option */
    fillObsOption(&option, obs_config);
    obs_response_handler response_handler = {
        &head_properties_callback,
        &head_complete_callback
    };
    if (strncmp(file_path, "global_barrier_records", headerLen) != 0) {
        rc = snprintf_s(obsFilePath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", obs_config->archive_prefix, file_path);
        securec_check_ss(rc, "\0", "\0");
    } else {
        char pathPrefix[MAXPGPATH] = {0};
        rc = strcpy_s(pathPrefix, MAXPGPATH, obs_config->archive_prefix);
        securec_check_ss(rc, "\0", "\0");
        if (!IS_PGXC_COORDINATOR) {
            char *p = strrchr(pathPrefix, '/');
            if (p == NULL) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Obs path prefix is invalid")));
            }
            *p = '\0';
        }
        rc = snprintf_s(obsFilePath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", pathPrefix, file_path);
        securec_check_ss(rc, "\0", "\0");
    }

    ereport(DEBUG1, (errmsg("[OBS] before ListObjects check OBS file %s exist", obsFilePath)));

    do {
        obs_head_object(&option, obsFilePath, &response_handler, &data);
    } while (obs_status_is_retryable((obs_status)data.ret_status) && should_retry(retriesG));

    if ((statusG != OBS_STATUS_OK)) {
        ereport(LOG, (errmsg("[OBS] OBS file %s does not exist.", obsFilePath)));
        return false;
    }

    ereport(LOG, (errmsg("[OBS] OBS file %s exists.", obsFilePath)));
    return true;
}

bool feofReadOBSFile(const void* fp)
{
    if (fp == NULL) {
        return true;
    }
    return ((OBSReadFile*)fp)->obs_eof;
}

/*
 *  init libs3 by S3_initialize
 */
void initializeOBS()
{
    obs_status status = OBS_STATUS_OK;


    /* The last param should be NULL, now take "china" as default region */
    status = obs_initialize(OBS_INIT_ALL);
    return;
}

bool UploadOneFileToOBS(char* localFilePath, char* netBackupPath, ArchiveConfig *obs_config)
{
    FILE *fpReadFromDisk = NULL;
    OBSFile* fpWriteToNB = NULL;
    char* buffer = NULL;
    size_t readSize = 0;
    size_t writeSize = 0;
    int rc = 0;

    canonicalize_path(localFilePath);
    fpReadFromDisk = fopen(localFilePath, "r");
    if (fpReadFromDisk == NULL) {
        ereport(ERROR, (errmsg("Open local file %s failed: errirmsg %s", localFilePath, gs_strerror(errno))));
        return  false;
    }

    if (fseek(fpReadFromDisk, 0, SEEK_SET) != 0) {
        fclose(fpReadFromDisk);
        ereport(ERROR, (errmsg("File [%s] seek failed: %s!", localFilePath, gs_strerror(errno))));
        return false;
    }

    fpWriteToNB = (OBSFile *)createOBSFile(netBackupPath, "wb", obs_config);
    if (fpWriteToNB == NULL) {
        fclose(fpReadFromDisk);
        ereport(ERROR, (errmsg("Open OBS file %s failed", netBackupPath)));
        return false;
    }

    buffer = (char*)palloc(READ_BLOCK_SIZE + 1);

    while (1) {
        (void)memset_s(buffer, READ_BLOCK_SIZE, 0, READ_BLOCK_SIZE);
        readSize = fread(buffer, 1, READ_BLOCK_SIZE, fpReadFromDisk);
        if (readSize != READ_BLOCK_SIZE && !feof(fpReadFromDisk)) {
            fclose(fpReadFromDisk);
            pfree_ext(buffer);
            ereport(ERROR, (errmsg("file read failed file path is : %s", netBackupPath)));
            return false;
        }
        rc = writeOBSData(buffer, 1, readSize, fpWriteToNB, &writeSize, obs_config);
        if (rc == -1) {
            fclose(fpReadFromDisk);
            pfree_ext(buffer);
            ereport(ERROR, (errmsg("file write to net backup failed netbackup path is : %s", netBackupPath)));
            return false;
        }
        if (readSize < READ_BLOCK_SIZE) {
            ereport(LOG, (errmsg("write disk file to netbackup successfully")));
            break;
        }
    }
    closeOBSFile(fpWriteToNB, obs_config);
    fclose(fpReadFromDisk);
    pfree_ext(buffer);
    return true;
}

bool DownloadOneItemFromOBS(char* netBackupPath, char* localPath, ArchiveConfig *obs_config)
{
    ereport(LOG, (errmsg("start download obs file %s", netBackupPath)));
    void* fp = NULL;
    FILE* fpWriteToDisk = NULL;
    char* metadataBuffer = NULL;
    int nRet = 0;
    size_t writeBytes = 0;
    size_t readSize = 0;

    canonicalize_path(localPath);
    if ((fpWriteToDisk = fopen(localPath, "wb+")) == NULL) {
        ereport(ERROR, (errmsg("Open file %s failed: errirmsg %s", localPath, gs_strerror(errno))));
        return false;
    }

    if ((fp = openReadOBSFile(netBackupPath, "r", obs_config)) == NULL) {
        ereport(LOG, (errmsg("Open file [%s] failed", netBackupPath)));
        fclose(fpWriteToDisk);
        return false;
    }

    metadataBuffer = (char*)palloc(READ_BLOCK_SIZE);
    while (1) {
        (void)memset_s(metadataBuffer, READ_BLOCK_SIZE, 0, READ_BLOCK_SIZE);
        nRet = readOBSFile(metadataBuffer, 1, READ_BLOCK_SIZE, fp, &readSize, obs_config);
        if (nRet == -1) {
            fclose(fpWriteToDisk);
            ereport(ERROR, (errmsg("readOBSFile failed")));
            return false;
        } else if (readSize != 0) {
            writeBytes = fwrite(metadataBuffer, 1, readSize, fpWriteToDisk);
            if (writeBytes != readSize) {
                (void)closeReadOBSFile(fp);
                fclose(fpWriteToDisk);
                ereport(ERROR, (errmsg("readOBSFile failed, size not match")));
                return false;
            }
        }

        if (feofReadOBSFile(fp)) {
            break;
        }
    }

    ereport(LOG, (errmsg("Successful download obs file %s", netBackupPath)));

    (void)closeReadOBSFile(fp);
    fclose(fpWriteToDisk);
    pfree_ext(metadataBuffer);
    return true;
}
#endif
