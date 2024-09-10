/*-------------------------------------------------------------------------
 *
 * thread.cpp: Thread api used by Backup/Recovery manager.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#include "include/thread.h"
#include "include/oss_operator.h"
#include "include/backup.h"
#include "include/appender.h"

#include "utils/palloc.h"
#include "common/fe_memutils.h"
#include "storage/file/fio_device.h"

void initBackupSenderContext(SenderCxt** cxt)
{
    SenderCxt* senderCxt = NULL;
    senderCxt = (SenderCxt*)palloc(sizeof(SenderCxt));
    if (senderCxt == NULL) {
        elog(ERROR, "sender context allocate failed: out of memory");
    }
    pthread_spin_init(&senderCxt->lock, PTHREAD_PROCESS_PRIVATE);
    senderCxt->state = SENDER_THREAD_STATE_INIT;
    senderCxt->bufferCxt = (BufferCxt*)palloc(sizeof(BufferCxt));
    if (senderCxt->bufferCxt == NULL) {
        pfree_ext(senderCxt);
        elog(ERROR, "buffer context allocate failed: out of memory");
    }
    /* Initialize the buffer context */
    initBufferCxt(senderCxt->bufferCxt, SENDER_BUFFER_SIZE);
    *cxt = senderCxt;
}

void startBackupSender()
{
    pthread_create(&current.sender_cxt->senderThreadId, nullptr, backupSenderThreadMain, (void*)current.sender_cxt);
}

bool isSenderThreadStopped(SenderCxt* senderCxt)
{
    pthread_spin_lock(&senderCxt->lock);
    bool isStopped = senderCxt->state == SENDER_THREAD_STATE_STOP;
    pthread_spin_unlock(&senderCxt->lock);
    return isStopped;
}

bool isReaderThreadStopped(ReaderCxt* readerCxt)
{
    pthread_spin_lock(&readerCxt->lock);
    bool isStopped = (readerCxt->state == READER_THREAD_STATE_STOP);
    pthread_spin_unlock(&readerCxt->lock);
    return isStopped;
}

void destoryBackupReaderContexts()
{
    ReaderCxt* readerCxt = NULL;
    for (uint i = 0; i < current.readerThreadCount; i++) {
        readerCxt = &current.readerCxt[i];
        pthread_spin_lock(&readerCxt->lock);
        readerCxt->state = READER_THREAD_STATE_STOP;
        pthread_spin_unlock(&readerCxt->lock);
        pthread_join(readerCxt->readerThreadId, NULL);
        pthread_spin_destroy(&readerCxt->lock);
        pfree_ext(readerCxt->file);
        pfree_ext(readerCxt->prefile);
        pfree_ext(readerCxt->fromPath);
        pfree_ext(readerCxt->fileBuffer);
        pfree_ext(readerCxt->segType);
        pfree_ext(readerCxt->fileRemoved);
    }
    pfree_ext(current.readerCxt);
}

void destoryBackupSenderContext()
{
    SenderCxt* senderCxt = current.sender_cxt;
    pthread_spin_lock(&senderCxt->lock);
    senderCxt->state = SENDER_THREAD_STATE_STOP;
    pthread_spin_unlock(&senderCxt->lock);
    pthread_join(senderCxt->senderThreadId, NULL);
    pthread_spin_destroy(&senderCxt->lock);
    pfree_ext(senderCxt->bufferCxt);
    pfree_ext(senderCxt);
}

void* backupSenderThreadMain(void* arg)
{
    SenderCxt* senderCxt = (SenderCxt*)arg;
    BufferCxt* bufferCxt = senderCxt->bufferCxt;
    BufferDesc* buff = NULL;
    Oss::Oss* oss = NULL;
    const uint32 partSize = 6 * 1024 * 1024; // 6MB
    uint32 partLeftSize = partSize;
    char* buffer = (char*)palloc(partSize * sizeof(char));
    char* bufferEndPtr = buffer + partSize;
    // Open OSS connection
    oss = getOssClient();
    // find OSS bucket
    char* bucket_name = getBucketName();
    char* object_name = NULL;
    SendFileInfo* fileinfo = NULL;
    errno_t rc;
    while (true) {
        // wait for the buffer to be consumed
        if (isSenderThreadStopped(senderCxt)) {
            break;
        } else if (getSenderState(senderCxt) != SENDER_THREAD_STATE_FINISH && !hasBufferForRead(bufferCxt)) {
            continue;
        } else if (getSenderState(senderCxt) == SENDER_THREAD_STATE_FINISH && !hasBufferForRead(bufferCxt)) {
            setSenderState(senderCxt, SENDER_THREAD_STATE_FINISHED);
            continue;
        }
        buff = tryGetNextFreeReadBuffer(bufferCxt);
        // write the buffer to OSS
        if (buff->usedLen != 0) {
            if (buff->fileId != -1) {
                pthread_spin_lock(&senderCxt->lock);
                fileinfo = (SendFileInfo*)parray_get(current.filesinfo, buff->fileId);
                pthread_spin_unlock(&senderCxt->lock);
            }
            if (fileinfo != NULL && object_name == NULL) {
                object_name = fileinfo->filename;
                oss->StartMultipartUpload(bucket_name, object_name);
                partLeftSize = partSize;
            } else if (fileinfo != NULL && strcmp(object_name, fileinfo->filename) != 0) {
                oss->MultipartUpload(bucket_name, object_name, buffer, (partSize - partLeftSize));
                oss->CompleteMultipartUploadRequest(bucket_name, object_name);
                object_name = fileinfo->filename;
                oss->StartMultipartUpload(bucket_name, object_name);
                partLeftSize = partSize;
            } else if (fileinfo == NULL) {
                elog(ERROR, "get file info failed.");
            }
            if (buff->usedLen < partLeftSize) {
                rc = memcpy_s(bufferEndPtr - partLeftSize, buff->usedLen, buffLoc(buff, bufferCxt), buff->usedLen);
                securec_check(rc, "\0", "\0");
                partLeftSize = partLeftSize - buff->usedLen;
            } else {
                uint32 buff_off = buff->usedLen - partLeftSize;
                rc = memcpy_s(bufferEndPtr - partLeftSize, partLeftSize, buffLoc(buff, bufferCxt), partLeftSize);
                securec_check(rc, "\0", "\0");
                oss->MultipartUpload(bucket_name, object_name, buffer, partSize);
                memset_s(buffer, partSize, 0, partSize);
                securec_check(rc, "\0", "\0");
                if (buff_off > 0) {
                    rc = memcpy_s(buffer, buff_off, buffLoc(buff, bufferCxt) + partLeftSize, buff_off);
                    securec_check(rc, "\0", "\0");
                }
                partLeftSize = partSize - buff_off;
            }
        }
        // reuse the buffer
        clearBuff(buff);
    }
    if (bucket_name != NULL && object_name != NULL) {
        oss->MultipartUpload(bucket_name, object_name, buffer, (partSize - partLeftSize));
        oss->CompleteMultipartUploadRequest(bucket_name, object_name);
    }
    if (bufferCxt != NULL) {
        destroyBufferCxt((BufferCxt *)bufferCxt);
    }
    pfree_ext(buffer);
    return NULL;
}

void initBackupReaderContexts(ReaderCxt** cxt)
{
    ReaderCxt* readerCxt= NULL;
    current.readerThreadCount = num_threads - 1;
    /* alloc memmory */
    ReaderCxt* current_readerCxt = (ReaderCxt*)palloc(sizeof(ReaderCxt) * current.readerThreadCount);
    if (current_readerCxt == NULL) {
        elog(ERROR, "reader thread allocate failed: out of memory");
    }
    /* Initialize the reader context */
    for (uint i = 0; i < current.readerThreadCount; i++) {
        readerCxt = &current_readerCxt[i];
        pthread_spin_init(&readerCxt->lock, PTHREAD_PROCESS_PRIVATE);
        readerCxt->state = READER_THREAD_STATE_INIT;
        readerCxt->file = (pgFile**)palloc(sizeof(pgFile*) * READER_THREAD_FILE_COUNT);
        if (readerCxt->file == NULL) {
            elog(ERROR, "file list allocate failed: out of memory");
        }
        readerCxt->prefile = (pgFile**)palloc(sizeof(pgFile*) * READER_THREAD_FILE_COUNT);
        if (readerCxt->prefile == NULL) {
            elog(ERROR, "prefile list allocate failed: out of memory");
        }
        readerCxt->fromPath = (char**)palloc(sizeof(char*) * READER_THREAD_FILE_COUNT);
        if (readerCxt->fromPath == NULL) {
            elog(ERROR, "fromPath list allocate failed: out of memory");
        }
        readerCxt->toPath = (char**)palloc(sizeof(char*) * READER_THREAD_FILE_COUNT);
        if (readerCxt->toPath == NULL) {
            elog(ERROR, "toPath list allocate failed: out of memory");
        }
        readerCxt->fileBuffer = (char *)palloc(FILE_BUFFER_SIZE * READER_THREAD_FILE_COUNT);
        if (readerCxt->fileBuffer == NULL) {
            elog(ERROR, "file buffer allocate failed: out of memory");
        }
        readerCxt->segType = (FILE_APPEND_SEG_TYPE *)palloc(sizeof(FILE_APPEND_SEG_TYPE) * READER_THREAD_FILE_COUNT);
        if (readerCxt->segType == NULL) {
            elog(ERROR, "segment type allocate failed: out of memory");
        }
        readerCxt->fileRemoved = (bool *)palloc(sizeof(bool) * READER_THREAD_FILE_COUNT);
        if (readerCxt->fileRemoved == NULL) {
            elog(ERROR, "file removed flag allocate failed: out of memory");
        }
    }
    for (uint i = 0; i < current.readerThreadCount; i++) {
        readerCxt = &current_readerCxt[i];
        readerCxt->fileCount = 0;
        readerCxt->readerThreadId = 0;
        readerCxt->appender = NULL;
        for (uint j = 0; j < READER_THREAD_FILE_COUNT; j++) {
            readerCxt->file[j] = NULL;
            readerCxt->prefile[j] = NULL;
            readerCxt->fromPath[j] = NULL;
            readerCxt->toPath[j] = NULL;
            readerCxt->segType[j] = FILE_APPEND_TYPE_UNKNOWN;
            readerCxt->fileRemoved[j] = false;
        }
    }
    *cxt = current_readerCxt;
}

void startBackupReaders(backup_files_arg* arg, backupReaderThreadArgs* thread_args)
{
    for (uint i = 0; i < current.readerThreadCount; i++) {
        backupReaderThreadArgs* args = &thread_args[i];
        args->arg = arg;
        args->readerCxt = &current.readerCxt[i];
        pthread_create(&(args->readerCxt->readerThreadId), nullptr, backupReaderThreadMain, (void*)args);
    }
}

void* backupReaderThreadMain(void* thread_args)
{
    backupReaderThreadArgs* args = (backupReaderThreadArgs*)thread_args;
    backup_files_arg* arg = (backup_files_arg*)(args->arg);
    ReaderCxt* readerCxt = (ReaderCxt*)(args->readerCxt);
    while (!isReaderThreadStopped(readerCxt)) {
        if (getReaderState(readerCxt) != READER_THREAD_STATE_START) {
            pg_usleep(WAIT_FOR_STATE_CHANGE_TIME);
            continue;
        }
        Assert(readerCxt->fileCount <= READER_THREAD_FILE_COUNT);
        for (uint i = 0; i < readerCxt->fileCount; i++) {
            copyFileToFileBuffer(readerCxt, i, arg);
        }
        setReaderState(readerCxt, READER_THREAD_STATE_FLUSHING);
    }
    return NULL; 
}

void copyFileToFileBuffer(ReaderCxt* readerCxt, int fileIndex, backup_files_arg* arg)
{
    pgFile* file = readerCxt->file[fileIndex];
    pgFile* prev_file = readerCxt->prefile[fileIndex];
    char* fileBuffer = readerCxt->fileBuffer + fileIndex * FILE_BUFFER_SIZE;
    char* from_fullpath = readerCxt->fromPath[fileIndex];
    char* to_fullpath = readerCxt->toPath[fileIndex];
    if (file->is_datafile && !file->is_cfs) {
        backup_data_file(&(arg->conn_arg), file, from_fullpath, to_fullpath,
                           arg->prev_start_lsn,
                           current.backup_mode,
                           instance_config.compress_alg,
                           instance_config.compress_level,
                           arg->nodeInfo->checksum_version,
                           arg->hdr_map, false, NULL, fileBuffer);
    } else {
        backup_non_data_file(file, prev_file, from_fullpath, to_fullpath,
                             current.backup_mode, current.parent_backup, true, NULL, fileBuffer);
    }
}

ReaderThreadState getReaderState(ReaderCxt* readerCxt)
{
    pthread_spin_lock(&readerCxt->lock);
    ReaderThreadState state = readerCxt->state;
    pthread_spin_unlock(&readerCxt->lock);
    return state;
}

void setReaderState(ReaderCxt* readerCxt, ReaderThreadState state)
{
    pthread_spin_lock(&readerCxt->lock);
    readerCxt->state = state;
    pthread_spin_unlock(&readerCxt->lock);
}

void setSenderState(SenderCxt* senderCxt, SenderThreadState state)
{
    pthread_spin_lock(&senderCxt->lock);
    senderCxt->state = state;
    pthread_spin_unlock(&senderCxt->lock);
}

SenderThreadState getSenderState(SenderCxt* senderCxt)
{
    pthread_spin_lock(&senderCxt->lock);
    SenderThreadState state = senderCxt->state;
    pthread_spin_unlock(&senderCxt->lock);
    return state;
}

void flushReaderContexts(void* arg)
{
    ReaderCxt* readerCxt = NULL;
    char* fileBuffer = NULL;
    backup_files_arg* args = (backup_files_arg*)arg;
    waitForReadersCopyComplete();
    for (uint i = 0; i < current.readerThreadCount; i++) {
        readerCxt = &current.readerCxt[i];
        for (uint j = 0; j < readerCxt->fileCount; j++) {
            if (!readerCxt->fileRemoved[j]) {
                fileBuffer = readerCxt->fileBuffer + j * FILE_BUFFER_SIZE;
                if (readerCxt->file[j]->is_datafile && !readerCxt->file[j]->is_cfs) {
                    backup_data_file(&(args->conn_arg), readerCxt->file[j], readerCxt->fromPath[j], readerCxt->toPath[j],
                                       args->prev_start_lsn,
                                       current.backup_mode,
                                       instance_config.compress_alg,
                                       instance_config.compress_level,
                                       args->nodeInfo->checksum_version,
                                       args->hdr_map, false, readerCxt->appender, NULL);
                } else {
                    backup_non_data_file(readerCxt->file[j], readerCxt->prefile[j], readerCxt->fromPath[j], readerCxt->toPath[j],
                                         current.backup_mode, current.parent_backup, true, readerCxt->appender, NULL);
                }
                pg_free(readerCxt->fromPath[j]);
                pg_free(readerCxt->toPath[j]);
                readerCxt->fileRemoved[j] = true;
                readerCxt->file[j] = NULL;
                readerCxt->prefile[j] = NULL;
            }
        }
        readerCxt->fileCount = 0;
        setReaderState(readerCxt, READER_THREAD_STATE_FLUSHED);
    }
}

void waitForReadersCopyComplete()
{
    ReaderCxt* readerCxt = NULL;
    for (uint i = 0; i < current.readerThreadCount; i++) {
        readerCxt = &current.readerCxt[i];
        if (getReaderState(readerCxt) == READER_THREAD_STATE_INIT ||
            getReaderState(readerCxt) == READER_THREAD_STATE_FLUSHED) {
            setReaderState(readerCxt, READER_THREAD_STATE_START);
        }
    }
    for (uint i = 0; i < current.readerThreadCount; i++) {
        readerCxt = &current.readerCxt[i];
        if (getReaderState(readerCxt) == READER_THREAD_STATE_START) {
            pg_usleep(WAIT_FOR_STATE_CHANGE_TIME);
            i = i - 1;
            continue;
        }
    }
}

void waitForSenderThread()
{
    SenderCxt* senderCxt = current.sender_cxt;
    while (getSenderState(senderCxt) != SENDER_THREAD_STATE_FINISHED) {
        pg_usleep(WAIT_FOR_STATE_CHANGE_TIME);
    }
}

void stopBackupReaders()
{
    ReaderCxt* readerCxt = NULL;
    for (uint i = 0; i < current.readerThreadCount; i++) {
        readerCxt = &current.readerCxt[i];
        setReaderState(readerCxt, READER_THREAD_STATE_STOP);
    }
    for (uint i = 0; i < current.readerThreadCount; i++) {
        readerCxt = &current.readerCxt[i];
        pthread_join(readerCxt->readerThreadId, NULL);
    }
}

void stopBackupSender()
{
    SenderCxt* senderCxt = current.sender_cxt;
    setSenderState(senderCxt, SENDER_THREAD_STATE_STOP);
    pthread_join(senderCxt->senderThreadId, NULL);
}

int getFreeReaderThread()
{
    int slot = -1;
    ReaderCxt* readerCxt = NULL;
    for (size_t i = 0; i < current.readerThreadCount; i++) {
        readerCxt = &current.readerCxt[i];
        if (getReaderState(readerCxt) == READER_THREAD_STATE_INIT ||
            getReaderState(readerCxt) == READER_THREAD_STATE_FLUSHED) {
            slot = i;
            break;
        }
    }
    return slot;
}

static int PbkFilenameCompare(const void *f1, const void *f2)
{
    char* filename1 = *(char**)f1;
    char* filename2 = *(char**)f2;
    size_t len1 = strlen(filename1);
    size_t len2 = strlen(filename2);
    if (len1 == len2) {
        return strcmp(filename1, filename2);
    }
    return len1 - len2;
}

void* restoreReaderThreadMain(void* arg)
{
    restoreReaderThreadArgs* args = (restoreReaderThreadArgs*)arg;
    /* get pbk file from oss server */
    Oss::Oss* oss = getOssClient();
    const int object_suffix_len = 4;
    char* object_name = NULL;
    char* prefix_name = getPrefixName(args->dest_backup);
    char* bucket_name = getBucketName();
    if (bucket_name == NULL || !oss->BucketExists(bucket_name)) {
        elog(ERROR, "bucket %s not found, please create it first", bucket_name ? bucket_name : "null");
    }
    parray* objects = parray_new();
    parray* pbkObjects = parray_new();
    oss->ListObjectsWithPrefix(bucket_name, prefix_name, objects);
    size_t objects_num = parray_num(objects);
    for (size_t i = 0; i < objects_num; ++i) {
        object_name = (char*)parray_get(objects, i);
        if (strncmp(object_name + strlen(object_name) - object_suffix_len, ".pbk", object_suffix_len) == 0) {
            parray_append(pbkObjects, object_name);
        }
    }
    size_t pbkObjectsNum = parray_num(pbkObjects);
    /* Sort by filename for restoring order */
    parray_qsort(pbkObjects, PbkFilenameCompare);
    args->bufferCxt->fileNum = pbkObjectsNum;
    elog(INFO, "the total number of backup %s's file objects is %d, and pbk file objects is %d",
         base36enc(args->dest_backup->start_time), objects_num, pbkObjectsNum);
    for (size_t i = 0; i < objects_num; ++i) {
        object_name = (char*)parray_get(objects, i);
        if (strncmp(object_name + strlen(object_name) - object_suffix_len, ".pbk", object_suffix_len) == 0) {
            continue;
        } else {
            elog(INFO, "download object: %s from s3", object_name);
            char file_name[MAXPGPATH];
            int rc = snprintf_s(file_name, MAXPGPATH, MAXPGPATH - 1, "/%s", object_name);
            securec_check_ss_c(rc, "\0", "\0");
            oss->GetObject(bucket_name, object_name, (char*)file_name);
        }
    }
    for (size_t i = 0; i < pbkObjectsNum; ++i) {
        if (args->bufferCxt->earlyExit) {
            break;
        }
        object_name = (char*)parray_get(pbkObjects, i);
        elog(INFO, "download object: %s from s3", object_name);
        args->bufferCxt->fileEnd = false;
        oss->GetObject(bucket_name, object_name, (void*)args->bufferCxt);
    }
    parray_free(objects);
    parray_free(pbkObjects);
    return NULL;
}