/*-------------------------------------------------------------------------
 *
 * oss_operator.h: OSS Operator used by Backup/Restore manager.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#ifndef OSS_OPERATOR_H
#define OSS_OPERATOR_H

#include "stddef.h"
#include <iostream>
#include <memory>
#include <vector>
#include "../../parray.h"

/* Constants Definition */

#define OSS_MAX_UPLOAD_ID_LEN 256
#define OSS_MAX_FILE_PATH 1024
#define OSS_MAX_ETAG_LEN 256

/* API Function */

namespace Oss {
using namespace std;
using SDKOptions = void *;
using S3Client = void *;

class Oss {
public:
    Oss(const char* endpoint, const char* access_key, const char* secret_key, const char* region = NULL, bool secure = false);
    ~Oss();
    void GetObject(const char* bucket_name, const char* object_name, const char* file_name, bool errorOk = false);
    void GetObject(const char* from_bucket, const char* object_key, void* file);
    void PutObject(const char* bucket_name, const char* file_path, const char* file_name);
    void RemoveObject(const char* bucket_name, const char* objcet_key);
    void ListObjects(char* bucket_name, parray* objects);
    void ListObjectsWithPrefix(char* bucket_name, char* prefix, parray* objects);
    void MakeBucket(char* bucket_name);
    void ListBuckets(parray* buckets);
    bool ObjectExists(char* bucket_name, char* object_name);
    bool BucketExists(char* bucket_name);
    void RemoveBucket(char* bucket_name);
    void StartMultipartUpload(char* bucket_name, char* object_name);
    void MultipartUpload(char* bucket_name, char* object_name, char* data, size_t data_size);
    void CompleteMultipartUploadRequest(char* bucket_name, char* object_name);

private:
    const string kEndpoint;
    const string kAccessKey;
    const string kSecretKey;
    const bool kSecure;
    string kRegion;
    void* completePartVector;
    string UploadId;
    int partNumber;
    SDKOptions options_;
    S3Client s3_client_;
};
}  // namespace Oss

extern void parseBackupControlFilePath(const char* path, char** bucket_name, char** object_name);
extern char* getBucketName();
extern char* getPrefixName(void* backup);
extern Oss::Oss* getOssClient();

#endif /* OSS_OPERATOR_H */

