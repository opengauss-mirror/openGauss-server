/*-------------------------------------------------------------------------
 *
 * buffer.cpp: Buffer used by Backup/Recovery manager.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "include/buffer.h"
#include "include/thread.h"

void initBufferCxt(BufferCxt* cxt, size_t bufferSize)
{
    size_t bufnum = (bufferSize + BUFSIZE -1) / BUFSIZE;
    cxt->bufNum = bufnum;
    cxt->bufHeader = (BufferDesc*)palloc(sizeof(BufferDesc) * bufnum);
    cxt->bufData = (char*)palloc(BUFSIZE * bufnum);
    cxt->fileEnd = false;
    cxt->fileNum = -1;
    cxt->producerIdx = 0;
    cxt->producerIdxCache = 0;
    cxt->consumerIdx = 0;
    cxt->consumerIdxCache = 0;
    cxt->earlyExit = false;
    if (cxt->bufHeader == NULL || cxt->bufData == NULL) {
        pfree_ext(cxt->bufHeader);
        pfree_ext(cxt->bufData);
        elog(ERROR, "buffer context allocate failed: out of memory");
    }
    for(size_t i = 0; i < bufnum; i++) {
        cxt->bufHeader[i].bufId = i;
        cxt->bufHeader[i].fileId = -1;
        cxt->bufHeader[i].usedLen = 0;
        cxt->bufHeader[i].flags = 0;
        pthread_spin_init(&cxt->bufHeader[i].lock, PTHREAD_PROCESS_PRIVATE);
    }
}

void destroyBufferCxt(BufferCxt* cxt)
{
    for(size_t i = 0; i < cxt->bufNum; i++) {
        pthread_spin_destroy(&cxt->bufHeader[i].lock);
    }
    pfree_ext(cxt->bufHeader);
    pfree_ext(cxt->bufData);
}

BufferDesc* getNextFreeWriteBuffer(BufferCxt* cxt)
{
    BufferDesc* buff = NULL;
    const size_t producerIdx = cxt->producerIdx.load(std::memory_order_relaxed);
    const size_t nextIdx = (producerIdx + 1) % buffNum(cxt);
    /* check whether the buffer queue is full */
    if (nextIdx == cxt->consumerIdxCache) {
        cxt->consumerIdxCache = cxt->consumerIdx.load(std::memory_order_acquire);
        if (nextIdx == cxt->consumerIdxCache) {
            return NULL;
        }
    }
    buff = &(cxt->bufHeader[producerIdx]);
    if (testBufferFlag(buff, BUFF_FLAG_FILE_FINISHED | BUFF_FLAG_FILE_CLOSED)) {
        cxt->producerIdx.store(nextIdx, std::memory_order_release);
        return NULL;
    }
    return buff;
}

BufferDesc* tryGetNextFreeWriteBuffer(BufferCxt* cxt)
{
    BufferDesc* buff = NULL;
    while (!(buff = getNextFreeWriteBuffer(cxt))) {
        pg_usleep(WAIT_FOR_BUFF_SLEEP_TIME);
        continue;
    }
    if (buffFreeLen(buff) != 0) {
        return buff;
    }
    return tryGetNextFreeWriteBuffer(cxt);
}

BufferDesc* getNextFreeReadBuffer(BufferCxt* cxt)
{
    BufferDesc* buff = NULL;
    const size_t consumerIdx = cxt->consumerIdx.load(std::memory_order_relaxed);
    /* check whether the buffer queue is empty */
    if (consumerIdx == cxt->producerIdxCache) {
        cxt->producerIdxCache = cxt->producerIdx.load(std::memory_order_acquire);
        buff = &(cxt->bufHeader[consumerIdx]);
        if (!testBufferFlag(buff, BUFF_FLAG_FILE_FINISHED | BUFF_FLAG_FILE_CLOSED) && 
            consumerIdx  == cxt->producerIdxCache) {
            return NULL;
        }
    }
    buff = &(cxt->bufHeader[consumerIdx]);
    // buffer read finished
    if (!testBufferFlag(buff, BUFF_FLAG_FILE_FINISHED | BUFF_FLAG_FILE_CLOSED)) {
        return NULL;
    }
    const size_t next = (consumerIdx + 1) % buffNum(cxt);
    cxt->consumerIdx.store(next, std::memory_order_release);
    return buff;
}

BufferDesc* tryGetNextFreeReadBuffer(BufferCxt* cxt)
{
    BufferDesc* buff = NULL;
    while (!(buff = getNextFreeReadBuffer(cxt))) {
        pg_usleep(WAIT_FOR_BUFF_SLEEP_TIME);
    }
    /* the buffer is ready */
    if (buff->usedLen != 0) {
        return buff;
    }
    return tryGetNextFreeReadBuffer(cxt);
}

size_t writeToBuffer(const char* data, size_t len, void* fp)
{
    BufferCxt* cxt = (BufferCxt*)fp;
    BufferDesc* buff = NULL;
    int64 writeLen = 0;
    int64 freeLen = 0;
    int64 remainingLen = (int64)len;
    errno_t rc;
    while (remainingLen > 0) {
        buff = tryGetNextFreeWriteBuffer(cxt);
        if (buff == NULL) {
            return 0;
        }
        freeLen = buffFreeLen(buff);
        writeLen = (remainingLen > freeLen) ? freeLen : remainingLen;
        rc = memcpy_s(buffFreeLoc(buff, cxt), writeLen, data, writeLen);
        securec_check_c(rc, "\0", "\0");
        addBuffLen(buff, writeLen);
        data = data + writeLen;
        remainingLen = remainingLen - writeLen;
        if (buffFreeLen(buff) == 0) {
            markBufferFlag(buff, BUFF_FLAG_FILE_FINISHED);
        }
        if ((remainingLen == 0 && cxt->fileEnd)) {
            markBufferFlag(buff, BUFF_FLAG_FILE_CLOSED);
        }
    }
    return len;
}

bool hasBufferForRead(BufferCxt* cxt)
{
    const size_t consumerIdx = cxt->consumerIdx.load(std::memory_order_acquire);
    BufferDesc* buff = &(cxt->bufHeader[consumerIdx]);
    return testBufferFlag(buff, BUFF_FLAG_FILE_FINISHED | BUFF_FLAG_FILE_CLOSED);
}

void* openWriteBufferFile(const char* filename, const char* mode)
{
    BufferCxt* buffCxt = current.sender_cxt->bufferCxt;
    BufferDesc* buff = NULL;
    SendFileInfo* fileInfo = NULL;
    fileInfo = (SendFileInfo*)palloc(sizeof(SendFileInfo));
    if (fileInfo == NULL) {
        elog(ERROR, "file info allocate failed: out of memory");
    }
    fileInfo->filename = pgut_strdup(filename);
    parray_append(current.filesinfo, fileInfo);
    buffCxt->fileEnd = false;
    buff = tryGetNextFreeWriteBuffer(buffCxt);
    if (buff == NULL) {
        elog(ERROR, "Failed to open buff file: %s", fileInfo->filename);
    }
    markBufferFlag(buff, BUFF_FLAG_FILE_OPENED);
    buff->fileId = parray_num(current.filesinfo) - 1;
    return buffCxt;
}
