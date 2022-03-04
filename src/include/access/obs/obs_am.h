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
 * obs_am.h
 *        obs access method definitions.
 *
 *
 * IDENTIFICATION
 *        src/include/access/obs/obs_am.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef OBS_AM_H
#define OBS_AM_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "nodes/pg_list.h"
#include "storage/buf/buffile.h"
#include "replication/slot.h"
#ifndef ENABLE_LITE_MODE
#include "eSDKOBS.h"
#endif

#define OBS_MAX_UPLOAD_ID_LEN 256
#define OBS_MAX_FILE_PATH 1024
#define OBS_MAX_ETAG_LEN 256
#define READ_BLOCK_SIZE 67108864 //64MB

typedef struct OBSWriteFile {
    char uploadId[OBS_MAX_UPLOAD_ID_LEN];
    char filePath[OBS_MAX_FILE_PATH];
    char** eTagList;
    int partNum;
} OBSWriteFile;

typedef struct OBSFile {
    char filePath[OBS_MAX_FILE_PATH];

    /* for write */
    char uploadId[OBS_MAX_UPLOAD_ID_LEN];
    char** eTagList;
    int partNum;

    /* for read */
    uint64_t fileSize;
    bool obs_eof;
    bool obs_error;
    void* bufData;
    int byteCount;
    int actualLen;
    size_t offset;
    char eTag[OBS_MAX_ETAG_LEN];
} OBSFile;

typedef struct OBSWriteData {
    void* bufData;
    int byteCount;
    int actualLen;
    char eTag[OBS_MAX_ETAG_LEN];
} OBSWriteData;

typedef struct OBSReadFile {
    char filePath[OBS_MAX_FILE_PATH];
    size_t fileSize;
    bool obs_eof;
    bool obs_error;
    void* bufData;
    int byteCount;
    int actualLen;
    size_t offset;
} OBSReadFile;

typedef struct HEAD_OBJECT_DATA {
    int ret_status;
    int object_length;
} HEAD_OBJECT_DATA;


/* OBS operation types */
typedef enum {
    OBS_UNKNOWN,
    OBS_READ,
    OBS_WRITE,
    /*
     * ...
     * More supported option types added here
     */
} OBSHandlerType;

/* OBS operation options */
#define OBS_TRANSPORT_ENCRYPT 0x0001

typedef struct ObsOptions {
    Oid serverOid;
    char* address;
    bool encrypt;
    char* access_key;
    char* secret_access_key;
    char* bucket;
    char* prefix;
    uint32_t chunksize;
} ObsOptions;

typedef struct ObsCopyOptions {
    bool encrypt;
    uint32_t chunksize;
    char* access_key;
    char* secret_access_key;
} ObsCopyOptions;

typedef struct OBSReadWriteHandler {
    char* m_url;
    char* m_hostname;
    char* m_bucket;
    /* char		*m_prefix;	=>m_object_info.key */

    ObsOptions* m_obs_options;
    OBSHandlerType m_type;
    bool in_computing;
    /* size_t	m_offset;	=>get_cond.start_byte */
    /* obs_bucket_context m_bucketCtx; =>m_option.bucket_options */
#ifndef ENABLE_LITE_MODE
    obs_options m_option;
    obs_object_info m_object_info;

    union {
        obs_get_conditions get_cond;
        obs_put_properties put_cond;
        /*
         * S3ListParts
         * S3ListVersions
         * ...
         * More OBS operation properties added here
         */
    } properties;
#endif
} OBSReadWriteHandler;

typedef struct list_service_data {
    int headerPrinted;
    int allDetails;
#ifndef ENABLE_LITE_MODE
    obs_status ret_status;
#endif
} list_service_data;

extern void SetObsMemoryContext(MemoryContext mctx);
extern MemoryContext GetObsMemoryContext(void);
extern void UnSetObsMemoryContext(void);

extern OBSReadWriteHandler *CreateObsReadWriteHandler(const char *object_url, OBSHandlerType type,
                                                      ObsCopyOptions *options);
extern OBSReadWriteHandler *CreateObsReadWriteHandlerForQuery(ObsOptions *options);
extern void ObsReadWriteHandlerSetType(OBSReadWriteHandler *handler, OBSHandlerType type);
extern void DestroyObsReadWriteHandler(OBSReadWriteHandler *handler, bool obsQueryType);

/* ----------------
 *      function prototypes for obs access method
 * ----------------
 */
/* in obs/obs_am.cpp */
extern List *list_obs_bucket_objects(const char *uri, bool encrypt, const char *access_key,
                                     const char *secret_access_key);
extern List *list_bucket_objects_analyze(const char *uri, bool encrypt, const char *access_key,
                                         const char *secret_access_key);
extern List *list_bucket_objects_for_query(OBSReadWriteHandler *handler, const char *bucket, char *prefix);
extern int writeObsTempFile(OBSReadWriteHandler *handler, const char *bufferData, int dataSize);
extern size_t read_bucket_object(OBSReadWriteHandler *handler, char *output_buffer, uint32_t len);
extern size_t write_bucket_object(OBSReadWriteHandler *handler, BufFile *buffile, uint32_t total_len);
extern void release_object_list(List *object_list);
extern void FetchUrlProperties(const char *url, char **hostname, char **bucket, char **prefix);
extern void FetchUrlPropertiesForQuery(const char *folderName, char **bucket, char **prefix);
extern int deleteOBSObject(OBSReadWriteHandler *handler);

/*
 * Function: initOBSCacheObjet
 * Description: This function is called only by postmastermain to initialize OBS CA Info
 * and secret key used to encrypt OBS access key and secret access key.
 * input: none
 * output: none
 */
extern void initOBSCacheObject();

extern int find_Nth(const char* str, unsigned N, const char* find);

extern void checkOBSServerValidity(char* addres, char* ak, char* sk, bool encrypt);

ObsOptions* copyObsOptions(ObsOptions* from);

void freeObsOptions(ObsOptions* obsOptions, bool freeUseIpsiFree);

extern bool is_ip_address_format(const char * addr);
extern void encryptKeyString(char* keyStr, char destplainStr[], uint32 destplainLength);
extern void decryptKeyString(const char *keyStr, char destplainStr[], uint32 destplainLength, const char *obskey);

ArchiveConfig *getArchiveConfig();
size_t obsRead(const char* fileName, int offset, char *buffer, int length, ArchiveConfig *obs_config = NULL);
int obsWrite(const char* fileName, const char *buffer, const int bufferLength, ArchiveConfig *obs_config = NULL);
int obsDelete(const char* fileName, ArchiveConfig *obs_config = NULL);
List* obsList(const char* prefix, ArchiveConfig *obs_config = NULL,
            bool reportError = true, bool shortenConnTime = false);
void* createOBSFile(const char* file_path, const char* mode, ArchiveConfig *obs_config = NULL);
int writeOBSData(const void* write_data, size_t size, size_t len, OBSFile* fp, size_t* writeSize,
    ArchiveConfig *obs_config = NULL);
int closeOBSFile(void* filePtr, ArchiveConfig *obs_config = NULL);
void* openReadOBSFile(const char* file_path, const char* mode, ArchiveConfig *obs_config = NULL);
int readOBSFile(char* data, size_t size, size_t len, void* fp, size_t* readSize, ArchiveConfig *obs_config = NULL);
int closeReadOBSFile(void* fp);
bool checkOBSFileExist(const char* file_path, ArchiveConfig *obs_config = NULL);
bool feofReadOBSFile(const void* fp);
void initializeOBS();
bool UploadOneFileToOBS(char* localFilePath, char* netBackupPath, ArchiveConfig *obs_config = NULL);
bool DownloadOneItemFromOBS(char* netBackupPath, char* localPath, ArchiveConfig *obs_config = NULL);


#endif /* OBS_AM_H */
