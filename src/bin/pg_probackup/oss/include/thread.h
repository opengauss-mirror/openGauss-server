/*-------------------------------------------------------------------------
 *
 * thread.h: Thread utils used by Backup/Restore manager.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#ifndef OSS_THREAD_H
#define OSS_THREAD_H

#include "../../pg_probackup.h"
#include "appender.h"
#include "buffer.h"
#include "../../parray.h"

/* Constants Definition */

constexpr int SENDER_BUFFER_SIZE = 536870912; /* 512 * 1024 * 1024 Bytes, 512MB */
#define READER_THREAD_FILE_COUNT 8
#define FILE_BUFFER_SIZE 8388608 /* 8 * 1024 * 1024, 8MB */
#define WAIT_FOR_STATE_CHANGE_TIME 100000 /* 100 ms */

/* Data Structure Definition*/
typedef enum readerThreadState {
    READER_THREAD_STATE_INIT = 0,
    READER_THREAD_STATE_START,
    READER_THREAD_STATE_FLUSHING,
    READER_THREAD_STATE_FLUSHED,
    READER_THREAD_STATE_ERROR,
    READER_THREAD_STATE_STOP
} ReaderThreadState;

typedef enum SenderThreadState {
    SENDER_THREAD_STATE_INIT = 0,
    SENDER_THREAD_STATE_START,
    SENDER_THREAD_STATE_FINISH,
    SENDER_THREAD_STATE_FINISHED,
    SENDER_THREAD_STATE_ERROR,
    SENDER_THREAD_STATE_STOP
} SenderThreadState;

typedef struct ReaderCxt {
    pgFile** file;
    pgFile** prefile;
    char** fromPath;
    char** toPath;
    char* fileBuffer;
    uint32 fileCount;
    FileAppender* appender;
    NO_VERSION_SEG_TYPE* segType; /* no used */
    bool* fileRemoved;
    pthread_t readerThreadPid; /* phtread id */
    ReaderThreadState state;
    pthread_spinlock_t lock;
    short readerIndexId; /* probackup thread id: 0, reader thread index id: 1~10 */
} ReaderCxt;

typedef struct SenderCxt {
    BufferCxt* bufferCxt;
    pthread_t senderThreadId;
    SenderThreadState state;
    pthread_spinlock_t lock;
} SenderCxt;

typedef struct restoreReaderThreadArgs {
    BufferCxt* bufferCxt;
    pgBackup* dest_backup;
} restoreReaderThreadArgs;

typedef struct backupReaderThreadArgs
{
    backup_files_arg* arg;
    ReaderCxt* readerCxt;
} backupReaderThreadArgs;

typedef struct BackupReaderInfo {
    short readerIndexId; /* probackup thread id: 0, reader thread index id: 1~10 */
} BackupReaderInfo;

/* API Function */

int getFreeReaderThread();

extern ReaderThreadState getReaderState(ReaderCxt* readerCxt);

extern void setReaderState(ReaderCxt* readerCxt, ReaderThreadState state);

extern SenderThreadState getSenderState(SenderCxt* senderCxt);

extern void setSenderState(SenderCxt* senderCxt, SenderThreadState state);

extern void startBackupReaders(backup_files_arg* arg, backupReaderThreadArgs* thread_args);

extern void startBackupSender();

extern void* backupReaderThreadMain(void* arg);

extern void* backupSenderThreadMain(void* arg);

extern void initBackupSenderContext(SenderCxt** cxt);

extern void initBackupReaderContexts(ReaderCxt** cxt);

extern bool isSenderThreadStopped(SenderCxt* senderCxt);

extern void destoryBackupSenderContext();

extern bool isReaderThreadStopped(ReaderCxt* readerCxt);

extern void destoryBackupReaderContexts();

extern void copyFileToFileBuffer(ReaderCxt* readerCxt, int fileIndex, backup_files_arg* arg);

extern void flushReaderContexts(void* arg);

extern void waitForSenderThread();

extern void stopBackupReaders();

extern void stopBackupSender();

extern void waitForReadersCopyComplete();

extern void* restoreReaderThreadMain(void* arg);

#endif /* OSS_THREAD_H */