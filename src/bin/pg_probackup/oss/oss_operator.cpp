/*-------------------------------------------------------------------------
 *
 * oss_operator.cpp: OSS operator used by Backup/Recovery manager.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#include <aws/core/Aws.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include <fstream>
#include <iostream>
#include <sys/stat.h>

#include "include/oss_operator.h"
#include "utils/elog.h"
#include "include/buffer.h"
#include "include/restore.h"

namespace Oss {
Oss::Oss(const char* endpoint, const char* access_key, const char* secret_key, const char* region, bool secure)
    : kEndpoint(endpoint), kAccessKey(access_key), kSecretKey(secret_key), kSecure(secure) {
    options_ = new Aws::SDKOptions;
    auto options = reinterpret_cast<Aws::SDKOptions *>(options_);
    Aws::InitAPI(*options);
    Aws::Client::ClientConfiguration config;
    if (region != NULL) {
        kRegion.assign(region);
        config.region = kRegion;
    }
    config.endpointOverride = kEndpoint;
    if (kSecure) {
        config.scheme = Aws::Http::Scheme::HTTPS;
        config.verifySSL = true;
    } else {
        config.scheme = Aws::Http::Scheme::HTTP;
        config.verifySSL = false;
    }
    completePartVector = new Aws::Vector<Aws::S3::Model::CompletedPart>();
    s3_client_ = new Aws::S3::S3Client(Aws::Auth::AWSCredentials(kAccessKey, kSecretKey), config,
                                       Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false);
}

Oss::~Oss() {
    auto options = reinterpret_cast<Aws::SDKOptions *>(options_);
    // Before the application the applications terminates, the SDK must be shut down.
    Aws::ShutdownAPI(*options);
    // Clean up
    delete reinterpret_cast<Aws::SDKOptions *>(options_);
    delete reinterpret_cast<Aws::S3::S3Client *>(s3_client_);
    delete reinterpret_cast<Aws::Vector<Aws::S3::Model::CompletedPart> *>(completePartVector);
}

void Oss::GetObject(const char* from_bucket, const char* object_key, void* filePtr) {
    BufferCxt* file = (BufferCxt*)(filePtr);
    auto s3_client = reinterpret_cast<Aws::S3::S3Client *>(s3_client_);
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(from_bucket);
    request.SetKey(object_key);

    auto outcome = s3_client->GetObject(request);
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        elog(ERROR, "GetObject: %s, %s", err.GetExceptionName().c_str(), err.GetMessage().c_str());
    }
    char buffer[BUFSIZE];
    std::stringstream ss;
    ss << outcome.GetResultWithOwnership().GetBody().rdbuf();
    while (!ss.eof()) {
        size_t readlen = ss.read(buffer, BUFSIZE).gcount();
        if (readlen < BUFSIZE) {
            file->fileEnd = true;
        }
        if (writeToBuffer(buffer, readlen, file) != readlen) {
            elog(ERROR, "GetObject: write to buffer failed.");
        }
        if (file->earlyExit) {
            break;
        }
    }
}

void Oss::GetObject(const char* bucket_name, const char* object_name, const char* file_name, bool errorOk) {
    auto s3_client = reinterpret_cast<Aws::S3::S3Client *>(s3_client_);
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(object_name);
    auto outcome = s3_client->GetObject(request);
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        int elevel = errorOk ? WARNING : ERROR;
        elog(elevel, "GetObject: %s, %s", err.GetExceptionName().c_str(), err.GetMessage().c_str());
        return;
    }
    char* separator_pos = last_dir_separator(file_name);
    char* dir_path = strndup(file_name, separator_pos - file_name);
    fio_mkdir(dir_path, DIR_PERMISSION, FIO_BACKUP_HOST);
    free(dir_path);
    Aws::OFStream local_file;
    local_file.open(file_name, ios::out | ios::binary);
    if (!local_file) {
        elog(ERROR, "Error GetObject : dst path's directory: %s is not existed.", file_name);
    }
    local_file << outcome.GetResultWithOwnership().GetBody().rdbuf();
}

void Oss::PutObject(const char* bucket_name, const char* file_path, const char* file_name) {
    struct stat buffer;
    if (stat(file_path, &buffer) == -1) {
        elog(ERROR, "PutObject: File %s  does not exist", file_name);
    }
    auto s3_client = reinterpret_cast<Aws::S3::S3Client *>(s3_client_);
    Aws::S3::Model::PutObjectRequest request;
    request.SetBucket(bucket_name);
    request.SetKey(file_name);
    shared_ptr<Aws::IOStream> input_data =
        Aws::MakeShared<Aws::FStream>("sSampleAllocationtTag", file_path, ios_base::in | ios_base::binary);
    request.SetBody(input_data);
    auto outcome = s3_client->PutObject(request);
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        elog(ERROR, "PutObject: %s, %s", err.GetExceptionName().c_str(), err.GetMessage().c_str());
    }
}

void Oss::StartMultipartUpload(char* bucket_name, char* object_name)
{
    auto s3_client = reinterpret_cast<Aws::S3::S3Client *>(s3_client_);
    auto completeParts = (Aws::Vector<Aws::S3::Model::CompletedPart>*)completePartVector;
    // set the target bucket and file path for uploading
    const Aws::String bucket = bucket_name;
    const Aws::String key = object_name;
    // initialize parts upload task
    Aws::S3::Model::CreateMultipartUploadRequest create_request;
    create_request.SetBucket(bucket_name);
    create_request.SetKey(object_name);
    Aws::S3::Model::CreateMultipartUploadOutcome outcome = s3_client->CreateMultipartUpload(create_request);
    // obtain upload ID and part number
    UploadId = std::string(outcome.GetResult().GetUploadId().c_str());
    partNumber = 1;
    completeParts->clear();
    // create part upload output
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        elog(ERROR, "StartMultipartUpload: %s, %s", err.GetExceptionName().c_str(), err.GetMessage().c_str());
    }
}

void Oss::MultipartUpload(char* bucket_name, char* object_name, char* data, size_t data_size)
{
    if (data_size == 0 || data == NULL) {
        return;
    }
    auto s3_client = reinterpret_cast<Aws::S3::S3Client *>(s3_client_);
    auto completeParts = (Aws::Vector<Aws::S3::Model::CompletedPart>*)completePartVector;
    const Aws::String bucket = bucket_name;
    const Aws::String key = object_name;
    Aws::S3::Model::UploadPartRequest request1;
    request1.SetBucket(bucket_name);
    request1.SetKey(object_name);
    // start uploading
    Aws::S3::Model::UploadPartRequest uploadPartRequest;
    uploadPartRequest.WithBucket(bucket).WithKey(key).WithUploadId(Aws::String(UploadId.c_str())).WithPartNumber(partNumber).WithContentLength(data_size);
    Aws::String str(data, data_size);
    auto input_data = Aws::MakeShared<Aws::StringStream>("UploadPartStream", str);
    uploadPartRequest.SetBody(input_data);
    auto uploadPartResult = s3_client->UploadPart(uploadPartRequest);
    completeParts->push_back(Aws::S3::Model::CompletedPart().WithETag(uploadPartResult.GetResult().GetETag()).WithPartNumber(partNumber));
    ++partNumber;
}

void Oss::CompleteMultipartUploadRequest(char* bucket_name, char* object_name)
{
    auto s3_client = reinterpret_cast<Aws::S3::S3Client *>(s3_client_);
    auto completeParts = (Aws::Vector<Aws::S3::Model::CompletedPart>*)completePartVector;
    const Aws::String bucket = bucket_name;
    const Aws::String key = object_name;
    // Complete parts upload
    Aws::S3::Model::CompleteMultipartUploadRequest request;
    request.SetBucket(bucket);
    request.SetKey(key);
    request.SetUploadId(Aws::String(UploadId.c_str()));
    Aws::S3::Model::CompletedMultipartUpload completed_multipart_upload;
    completed_multipart_upload.SetParts(*completeParts);
    request.SetMultipartUpload(completed_multipart_upload);
    Aws::S3::Model::CompleteMultipartUploadOutcome outcome = s3_client->CompleteMultipartUpload(request);
    if (!outcome.IsSuccess()) {
        elog(ERROR, "CompleteMultipartUploadRequest: %s", outcome.GetError().GetMessage().c_str());
    }
}


void Oss::RemoveObject(const char* bucket_name, const char* objcet_key) {
    auto s3_client = reinterpret_cast<Aws::S3::S3Client *>(s3_client_);
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithKey(objcet_key).WithBucket(bucket_name);
    auto outcome = s3_client->DeleteObject(request);
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        elog(WARNING, "RemoveObject: %s, %s", err.GetExceptionName().c_str(), err.GetMessage().c_str());
    }
}

void Oss::ListObjectsWithPrefix(char* bucket_name, char* prefix, parray* objects)
{
    auto s3_client = reinterpret_cast<Aws::S3::S3Client *>(s3_client_);
    Aws::S3::Model::ListObjectsRequest request;
    request.WithBucket(bucket_name);
    request.SetPrefix(prefix);
    auto outcome = s3_client->ListObjects(request);
    if (!outcome.IsSuccess()) {
        auto err = outcome.GetError();
        elog(ERROR, "ListObjectsWithPrefix: %s, %s", err.GetExceptionName().c_str(), err.GetMessage().c_str());
    }
    Aws::Vector<Aws::S3::Model::Object> resp = outcome.GetResult().GetContents();
    for (auto &bucket : resp) {
        char* key = pg_strdup(bucket.GetKey().c_str());
        parray_append(objects, key);
    }
}

bool Oss::BucketExists(char* bucket_name) {
    auto s3_client = reinterpret_cast<Aws::S3::S3Client *>(s3_client_);
    Aws::S3::Model::HeadBucketRequest request;
    request.SetBucket(bucket_name);
    auto outcome = s3_client->HeadBucket(request);
    if (!outcome.IsSuccess()) {
        return false;
    }
    return true;
}

bool Oss::ObjectExists(char* bucket_name, char* object_name) {
    auto s3_client = reinterpret_cast<Aws::S3::S3Client *>(s3_client_);
    Aws::S3::Model::HeadObjectRequest headObjectRequest;
    headObjectRequest.WithBucket(bucket_name).WithKey(object_name);
    auto result = s3_client->HeadObject(headObjectRequest);
    return result.IsSuccess();
}
}  // namespace Oss

char* getBucketName()
{
    char* bucket_name = instance_config.oss.access_bucket;
    if (bucket_name == NULL) {
        elog(ERROR, "Required parameter not specified: S3(--bucket_name)");
    }
    return bucket_name;
}

char* getPrefixName(void* backup)
{
    return ((pgBackup*)backup)->root_dir + 1;
}

Oss::Oss* getOssClient()
{
    if (oss_client == NULL) {
        const char* endpoint   = instance_config.oss.endpoint;
        const char* access_key = instance_config.oss.access_id;
        const char* secret_key = instance_config.oss.access_key;
        const char* region     = instance_config.oss.region;
        const char* access_bucket = instance_config.oss.access_bucket;
        if (!endpoint || !access_key || !secret_key || !access_bucket) {
            elog(ERROR,
                 "Required parameter not specified: S3(--endpoint, --access_bucket, --access_id or --access_key)");
        }
        oss_client = new Oss::Oss(endpoint, access_key, secret_key, region);
    }
    return (Oss::Oss*)oss_client;
}
