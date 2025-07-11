/*-------------------------------------------------------------------------
 *
 * buffer.h: Buffer used by Backup/Recovery manager.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUFFER_H
#define BUFFER_H

#include <atomic>

#include "../../pg_probackup.h"


/* Constants Definition */

#define BUFSIZE 2097152 /* 2 * 1024 * 1024, 2MB */
#define WAIT_FOR_BUFF_SLEEP_TIME 100000 /* 100 ms*/
constexpr int BUFF_FLAG_FILE_USED = 0x1;
#define BUFF_FLAG_FILE_CLOSED 0x2
#define BUFF_FLAG_FILE_FINISHED 0x4
const int GET_BUFF_RETRY_TIME = 50;

#ifdef __cpp_lib_hardware_interference_size
    static constexpr size_t CacheLineSize =
        std::hardware_constructive_interference_size;
#else
    static constexpr size_t CacheLineSize = 64;
#endif

/* Data Structure Definition*/

typedef struct SendFileInfo {
    char* filename;
} SendFileInfo;

/* each buffer's description */
typedef struct BufferDesc
{
    uint32 bufId;
    int32 fileId; /* the buffer belong to which file */
    uint32 usedLen;
    uint32 flags;
    pthread_spinlock_t lock; /* for lock schema */
} BufferDesc;

/* the context of buffers */
typedef struct BufferCxt
{
    BufferDesc* bufHeader;
    char* bufData;
    uint32 bufNum;
    int fileNum; /* for restore */
    std::atomic<bool> fileEnd = {false};
    std::atomic<bool> earlyExit = {false};
    std::atomic<int> fileId = {-1};
    std::atomic<int> producerCount = {0};
    std::atomic<int> consumerCount = {0};
    alignas(CacheLineSize) std::atomic<size_t> producerIdx = {0};
    alignas(CacheLineSize) size_t producerIdxCache = 0;
    alignas(CacheLineSize) std::atomic<size_t> consumerIdx = {0};
    alignas(CacheLineSize) size_t consumerIdxCache = 0;
} BufferCxt;

/* API Function */

extern void initBufferCxt(BufferCxt* cxt, size_t bufferSize);

extern void destroyBufferCxt(BufferCxt* cxt);

extern BufferDesc* tryGetNextFreeWriteBuffer(BufferCxt* cxt);

extern BufferDesc* getNextFreeWriteBuffer(BufferCxt* cxt);

extern BufferDesc* tryGetNextFreeReadBuffer(BufferCxt* cxt);

extern BufferDesc* getNextFreeReadBuffer(BufferCxt* cxt);

extern void* openWriteBufferFile(const char* filename, const char* mode);

extern void closeWriteBufferFile(void* fp);

extern size_t writeToBuffer(const char* data, size_t len, void* fp);

extern bool hasBufferForRead(BufferCxt* cxt);

extern bool hasNextBufferForRead(BufferCxt* cxt, const size_t buffIdx);

/* inline function */
inline uint32 buffFreeLen(BufferDesc* buff)
{
    uint32 freelen = 0;
    pthread_spin_lock(&buff->lock);
    freelen = BUFSIZE - buff->usedLen;
    pthread_spin_unlock(&buff->lock);
    return freelen;
}

inline uint32 buffUsedLen(BufferDesc* buff)
{
    uint32 usedLen = 0;
    pthread_spin_lock(&buff->lock);
    usedLen = buff->usedLen;
    pthread_spin_unlock(&buff->lock);
    return usedLen;
}

inline uint32 buffFileId(BufferDesc* buff)
{
    uint32 fileId = -1;
    pthread_spin_lock(&buff->lock);
    fileId = buff->fileId;
    pthread_spin_unlock(&buff->lock);
    return fileId;
}

inline uint32 buffNum(BufferCxt* cxt)
{
    return cxt->bufNum;
}

inline char* buffLoc(BufferDesc* buff, BufferCxt* cxt)
{
    char* loc = nullptr;
    pthread_spin_lock(&buff->lock);
    loc = cxt->bufData + buff->bufId * BUFSIZE;
    pthread_spin_unlock(&buff->lock);
    return loc;
}

inline char* buffFreeLoc(BufferDesc* buff, BufferCxt* cxt)
{
    char* freeloc = nullptr;
    pthread_spin_lock(&buff->lock);
    freeloc = cxt->bufData + buff->bufId * BUFSIZE + buff->usedLen;
    pthread_spin_unlock(&buff->lock);
    return freeloc;
}

inline void addBuffLen(BufferDesc* buff, uint32 len)
{
    pthread_spin_lock(&buff->lock);
    buff->usedLen += len;
    pthread_spin_unlock(&buff->lock);
    Assert(buff->usedLen <= BUFSIZE);
}

inline void markBufferFlag(BufferDesc* buff, uint32 flag)
{     
    pthread_spin_lock(&buff->lock);
    (*((volatile uint32*)&(buff->flags))) |= flag;
    pthread_spin_unlock(&buff->lock);
}

inline void setBufferFileId(BufferDesc* buff, int32 fileId)
{
    pthread_spin_lock(&buff->lock);
    buff->fileId = fileId;
    pthread_spin_unlock(&buff->lock);
}

inline bool testBufferFlag(BufferDesc* buff, uint32 flag)
{     
    bool testres = false;
    pthread_spin_lock(&buff->lock);
    testres = ((*((volatile uint32*)&(buff->flags))) & flag);
    pthread_spin_unlock(&buff->lock);
    return testres;
}

inline void clearBuff(BufferDesc* buff)
{
    pthread_spin_lock(&buff->lock);
    buff->usedLen = 0;
    buff->flags = 0;
    buff->fileId = -1;
    pthread_spin_unlock(&buff->lock);
}

#endif /* BUFFER_H */