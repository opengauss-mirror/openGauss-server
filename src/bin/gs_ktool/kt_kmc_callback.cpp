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
 * -------------------------------------------------------------------------
 *
 * kt_kmc_callback.cpp
 *  Register callback functions of KMC :
 *      There are 8 types of callback functions, each type has several functions
 *      Wsec[Type]Callbacks : Callback[Func]
 *         type  |    func                                                          |   set
 *      ----------+------------------------------------------------------------------+--------
 *      mem       |   memAlloc, memFree, memCmp                                      |  default
 *      file      |   fileOpen, fileClose, fileRead, fileWrite, fileFlush,           |
 *                |        fileRemove, fileTell, fileSeek, fileEof, fileErrno        |    11
 *      lock      |   createLock, destroyLock, lock, unlock                          |    4
 *      procLock  |   createProcLock, destroyProcLock, procLock, procUnlock          |    4
 *      basicRely |   writeLog, notify, doEvents                                     |    3
 *      rng       |   getRandomNum, getEntropty, cleanupEntopy                       |    3
 *      time      |   gmTimeSate                                                     |    1
 *      hardWare  |   hwGetEncExtraData...(all 12)                                   |  
 *
 * IDENTIFICATION
 *    src/bin/gs_ktool/kt_kmc_callback.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "kt_kmc_callback.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <getopt.h>
#include <malloc.h>
#include <fcntl.h>
#include <semaphore.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "kt_log_manage.h"

const char *RND_DEVICE = "/dev/random";
const int ENTROPY_BUFF_SIZE = 128;
const int MAX_PROCESS_LOCK_NAME_LEN = 256;

static WsecBool KeCallbackCreateProcLock(WsecHandle *CProcLock);
static WsecVoid KeCallbackDestroyProcLock(WsecHandle DProcLock);
static WsecVoid KeCallbackProcLock(WsecHandle ProcLock);
static WsecVoid KeCallbackProcUnlock(WsecHandle ProcUnlock);
static WsecBool KeCallbackCreateThreadLock(WsecHandle *phMutex);
static WsecVoid KeCallbackDestroyThreadLock(WsecHandle phMutex);
static WsecVoid KeCallbackThreadLock(WsecHandle phMutex);
static WsecVoid KeCallbackThreadUnlock(WsecHandle phMutex);
static WsecHandle KeCallbackFopen(const char *filePathName, const KmcFileOpenMode mode);
static int KeCallbackFclose(WsecHandle stream);
/* only need to do single read process. the KMC caller has done the multiple read process */
static WsecBool KeCallbackFread(WsecVoid *buffer, size_t count, WsecHandle stream);
/* remove fsync, just kmc will flush data or close file after WSEC_FWRITE */
static WsecBool KeCallbackFwrite(const WsecVoid *buffer, size_t count, WsecHandle stream);
static int KeCallbackFflush(WsecHandle stream);
static int KeCallbackFremove(const char *path);
static long KeCallbackFtell(WsecVoid *stream);
/* return offset position if success(relative to file beginning), return -1 if failed */
static long KeCallbackFseek(WsecVoid *stream, long offset, KmcFileSeekPos origin);
static int KeCallbackFeof(WsecVoid *stream, WsecBool *endOfFile);
static int KeCallbackFerrno(WsecVoid *stream);
static int KeCallbackFexist(const char *filePathName);
static WsecBool KeCallbackUtcTime(const time_t *curTime, struct tm *curTm);
static unsigned long GetRandomNumbers(unsigned char *buff, size_t len);
static WsecBool KeCallbackGetEntropy(unsigned char **ppEnt, size_t buffLen);
static WsecVoid KeCallbackCleanupEntropy(unsigned char *pEnt, size_t buffLen);
static WsecVoid KeCallbackDoEvents(void);
static WsecVoid KeCallbackNotify(WsecUint32 eNtfCode, const WsecVoid *pData, size_t nDataSize);
static WsecVoid KeCallbackWriteLog(int nLevel, const char *module, const char *function, int line, const char *logCtx);

WsecBool KeCallbackCreateProcLock(WsecHandle* CProcLock)
{
    sem_t* h = NULL;
    uid_t user_id = 0;
    char user_pro_lock_name[MAX_PROCESS_LOCK_NAME_LEN] = {0};
    errno_t rc = 0;
    int sem_val = 0;
    int ret = 0;

    if (CProcLock == NULL) {
        return WSEC_FALSE;
    }

    user_id = getuid();
    rc = sprintf_s(user_pro_lock_name, MAX_PROCESS_LOCK_NAME_LEN, "GS_KTOOL_PROCESS_LOCK_FOR_%u", user_id);
    securec_check_ss_c(rc, "", "");
    h = sem_open(user_pro_lock_name, O_CREAT, S_IRUSR | S_IWUSR, 1);
    if (h == NULL) {
        insert_format_log(L_OPEN_SEM, KT_ERROR, "semaphore name : '%s'", user_pro_lock_name);
        return WSEC_FALSE;
    }

    ret = sem_getvalue(h, &sem_val);
    if (ret == -1) {
        insert_format_log(L_OPEN_SEM, KT_ERROR, "semaphore name : '%s'", user_pro_lock_name);
        return WSEC_FALSE;
    }

    if (sem_val <= 0) {
        if (sem_init(h, 0, 1) != 0) {
            insert_format_log(L_INIT_SEM, KT_ERROR, "semaphore name : '%s', semaphore value : %d",
                user_pro_lock_name, sem_val);
        }
        insert_format_log(L_INIT_SEM, KT_NOTICE,
            "semaphore name : '%s', old semaphore value : %d, new semaphore value : 1", user_pro_lock_name, sem_val);
    }

    *CProcLock = h;
    return WSEC_TRUE;
}

WsecVoid KeCallbackDestroyProcLock(WsecHandle DProcLock)
{
    sem_t* h = (sem_t*)DProcLock;
    if (h != NULL) {
        (void)sem_close(h);
    }
    return;
}

WsecVoid KeCallbackProcLock(WsecHandle ProcLock)
{
    sem_t* h = (sem_t*)ProcLock;
    int ret = 0;
    ret = (sem_wait(h) == 0);
    if (ret == WSEC_FALSE) {
        insert_format_log(L_WAIT_SEM, KT_ERROR, "");
    }
    return;
}

WsecVoid KeCallbackProcUnlock(WsecHandle ProcUnlock)
{
    sem_t* h = (sem_t*)ProcUnlock;
    int ret = 0;
    ret = (sem_post(h) == 0);
    if (ret == WSEC_FALSE) {
        insert_format_log(L_POST_SEM, KT_ERROR, "");
    }
    return;
}

static WsecBool KeCallbackCreateThreadLock(WsecHandle *phMutex)
{
    if (phMutex == NULL) {
        return WSEC_FALSE;
    }
    if (*phMutex != NULL) {
        return WSEC_FALSE;
    }

    pthread_mutex_t *mutex = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    if (mutex == NULL) {
        return WSEC_FALSE;
    }

    int result = pthread_mutex_init(mutex, NULL);
    if (result != 0) {
        kt_free(mutex);
        return WSEC_FALSE;
    }
    *phMutex = mutex;
    return WSEC_TRUE;
}

static WsecVoid KeCallbackDestroyThreadLock(WsecHandle phMutex)
{
    pthread_mutex_t *mutex = (pthread_mutex_t *)phMutex;
    if (mutex == NULL) {
        return;
    }

    pthread_mutex_destroy(mutex);
    kt_free(mutex);
}

static WsecVoid KeCallbackThreadLock(WsecHandle phMutex)
{
    pthread_mutex_t *mutex = (pthread_mutex_t *)phMutex;
    if (mutex == NULL) {
        return;
    }
    pthread_mutex_lock(mutex);
}

static WsecVoid KeCallbackThreadUnlock(WsecHandle phMutex)
{
    pthread_mutex_t *mutex = (pthread_mutex_t *)phMutex;
    if (mutex == NULL) {
        return;
    }
    pthread_mutex_unlock(mutex);
}

static WsecHandle KeCallbackFopen(const char *filePathName, const KmcFileOpenMode mode)
{
    int flag = 0;
    int *ret = NULL;
    if (mode == KMC_FILE_READ_BINARY) {
        flag = O_RDONLY;
    } else if (mode == KMC_FILE_WRITE_BINARY) {
        flag = O_CREAT | O_WRONLY;
    } else if (mode == KMC_FILE_READWRITE_BINARY) {
        flag = O_CREAT | O_RDWR;
    } else {
        return NULL;
    }
    int retFd = open(filePathName, flag, S_IRUSR | S_IWUSR);
    if (-1 != retFd) {
        ret = (int *)malloc(sizeof(int));
        if (ret == NULL) {
            insert_format_log(L_MALLOC_MEMORY, KT_ERROR, "");
            close(retFd);
            return NULL;
        }
        *ret = retFd;
    }
    return ret;
}

int KeCallbackFclose(WsecHandle stream)
{
    int fd = *(int *)stream;
    int ret = close(fd);
    kt_free(stream);
    return ret;
}

/* only need to do sigle read process.  the KMC callee has done the multiple read process */
static WsecBool KeCallbackFread(WsecVoid *buffer, size_t count, WsecHandle stream)
{
    int fd = *(int *)stream;
    size_t ret = -1;
    while (true) {
        ret = read(fd, buffer, count);
        if ((int)ret == -1 && errno == EINTR) {
            continue;
        }
        break;
    }
    if ((int)ret == -1) {
        return WSEC_FALSE;
    }
    return (count == ret) ? WSEC_TRUE : WSEC_FALSE;
}

/*
 * remove fsync, just kmc will flush data or close file after WSEC_FWRITE
 */
static WsecBool KeCallbackFwrite(const WsecVoid *buffer, size_t count, WsecHandle stream)
{
    int fd = *(int *)stream;
    size_t ret = -1;
    while (true) {
        ret = write(fd, buffer, count);
        if ((int)ret == -1 && errno == EINTR) {
            continue;
        }
        break;
    }
    if ((int)ret == -1) {
        return WSEC_FALSE;
    }
    return (count == ret) ? WSEC_TRUE : WSEC_FALSE;
}

/* Return 0 if success, return -1 if failed */
static int KeCallbackFflush(WsecHandle stream)
{
    return fsync(*(int *)stream);
}

/* Return 0 if success, return -1 if failed */
static int KeCallbackFremove(const char *path)
{
    return remove(path);
}

static long KeCallbackFtell(WsecVoid *stream)
{
    return lseek(*(int *)stream, 0, SEEK_CUR);
}

/* return offset position if success(relative to file beginning), return -1 if failed */
static long KeCallbackFseek(WsecVoid *stream, long offset, KmcFileSeekPos origin)
{
    int realOri = 0;
    int fd = *(int *)stream;
    long ret;
    if (origin == KMC_FILE_SEEK_CUR) {
        realOri = SEEK_CUR;
    } else if (origin == KMC_FILE_SEEK_SET) {
        realOri = SEEK_SET;
    } else if (origin == KMC_FILE_SEEK_END) {
        realOri = SEEK_END;
    } else {
        return -1;
    }
    ret = lseek(fd, offset, realOri);
    return ret;
}

static int KeCallbackFeof(WsecVoid *stream, WsecBool *endOfFile)
{
    int fd = *(int *)stream;
    long len;
    long curPos;
    if (endOfFile == NULL) {
        return -1;
    }
    curPos = lseek(fd, 0, SEEK_CUR);
    if (curPos == -1) {
        return -1;
    }
    len = lseek(fd, 0, SEEK_END);
    if (len == -1) {
        return -1;
    }
    if (lseek(fd, curPos, SEEK_SET) != curPos) {
        return -1;
    }
    if (len != curPos) {
        *endOfFile = WSEC_FALSE;
    } else {
        *endOfFile = WSEC_TRUE;
    }
    return 0;
}
static int KeCallbackFerrno(WsecVoid *stream)
{
    return errno;
}

static int KeCallbackFexist(const char *filePathName)
{
    return (access(filePathName, F_OK) == 0) ? WSEC_TRUE : WSEC_FALSE;
}

static WsecBool KeCallbackUtcTime(const time_t *curTime, struct tm *curTm)
{
    return (gmtime_r(curTime, curTm) == NULL) ? WSEC_FALSE : WSEC_TRUE;
}


static unsigned long GetRandomNumbers(unsigned char *buff, size_t len)
{
    unsigned long ret = 0xF;
    FILE *fet = NULL;
    fet = fopen(RND_DEVICE, "rb");
    if (fet != NULL) {
        if (len == fread(buff, 1, len, fet)) {
            ret = 0;
        }
        fclose(fet);
        fet = NULL;
    } else {
        insert_format_log(L_OPEN_FILE, KT_ERROR, "please make sure the device '%s' is avalilable", RND_DEVICE);
    }
    return ret;
}

static WsecBool KeCallbackGetEntropy(unsigned char **ppEnt, size_t buffLen)
{
    errno_t rc = 0;

    if (buffLen <= 0 || buffLen > ENTROPY_BUFF_SIZE) {
        return WSEC_FALSE;
    }
    *ppEnt = (unsigned char *)malloc(buffLen);
    if (*ppEnt == NULL) {
        return WSEC_FALSE;
    }
    if (GetRandomNumbers(*ppEnt, buffLen) != 0) {
        rc = memset_s(*ppEnt, buffLen, 0, buffLen);
        securec_check_c(rc, "", "");
        kt_free(*ppEnt);
        return WSEC_FALSE;
    }
    return WSEC_TRUE;
}

static WsecVoid KeCallbackCleanupEntropy(unsigned char *pEnt, size_t buffLen)
{
    errno_t rc = 0;
    rc = memset_s(pEnt, buffLen, 0, buffLen);
    securec_check_c(rc, "", "");
    kt_free(pEnt);
}

static WsecVoid KeCallbackDoEvents(void) {}

static WsecVoid KeCallbackNotify(WsecUint32 eNtfCode, const WsecVoid *pData, size_t nDataSize)
{
    (void)eNtfCode;
    (void)pData;
    (void)nDataSize;
}

static WsecVoid KeCallbackWriteLog(int nLevel, const char *module, const char *function, int line, const char *logCtx)
{
    (void)nLevel;
    (void)module;
    (void)function;
    (void)line;
    (void)logCtx;
}

unsigned long RegFunCallback()
{
    WsecCallbacks stMandatoryFun = { { 0 }, { 0 }, { 0 }, { 0 }, { 0 }, { 0 }, { 0 }, { 0 } };

    /* register application dependended callback functions */
    stMandatoryFun.basicRelyCallbacks.writeLog = KeCallbackWriteLog;
    stMandatoryFun.basicRelyCallbacks.notify = KeCallbackNotify;
    stMandatoryFun.basicRelyCallbacks.doEvents = KeCallbackDoEvents;

    /* Register Process Locker operation + Critical Section callback functions */
    stMandatoryFun.procLockCallbacks.createProcLock = KeCallbackCreateProcLock;
    stMandatoryFun.procLockCallbacks.destroyProcLock = KeCallbackDestroyProcLock;
    stMandatoryFun.procLockCallbacks.procLock = KeCallbackProcLock;
    stMandatoryFun.procLockCallbacks.procUnlock = KeCallbackProcUnlock;

    /* Register Thread Locker + Critical Section callback functions */
    stMandatoryFun.lockCallbacks.createLock = KeCallbackCreateThreadLock;
    stMandatoryFun.lockCallbacks.destroyLock = KeCallbackDestroyThreadLock;
    stMandatoryFun.lockCallbacks.lock = KeCallbackThreadLock;
    stMandatoryFun.lockCallbacks.unlock = KeCallbackThreadUnlock;

    /* Register File Operated related callback functions */
    stMandatoryFun.fileCallbacks.fileOpen = KeCallbackFopen;
    stMandatoryFun.fileCallbacks.fileClose = KeCallbackFclose;
    stMandatoryFun.fileCallbacks.fileRead = KeCallbackFread;
    stMandatoryFun.fileCallbacks.fileWrite = KeCallbackFwrite;
    stMandatoryFun.fileCallbacks.fileRemove = KeCallbackFremove;
    stMandatoryFun.fileCallbacks.fileSeek = KeCallbackFseek;
    stMandatoryFun.fileCallbacks.fileTell = KeCallbackFtell;
    stMandatoryFun.fileCallbacks.fileFlush = KeCallbackFflush;
    stMandatoryFun.fileCallbacks.fileEof = KeCallbackFeof;
    stMandatoryFun.fileCallbacks.fileErrno = KeCallbackFerrno;
    stMandatoryFun.fileCallbacks.fileExist = KeCallbackFexist;

    /* Register Random Number callback functions */
    stMandatoryFun.rngCallbacks.getRandomNum = NULL;
    stMandatoryFun.rngCallbacks.getEntropy = KeCallbackGetEntropy;
    stMandatoryFun.rngCallbacks.cleanupEntropy = KeCallbackCleanupEntropy;

    /* Register Time Related callback functions */
    stMandatoryFun.timeCallbacks.gmTimeSafe = KeCallbackUtcTime;

    unsigned long wsecRet = WsecRegFuncEx(&stMandatoryFun);
    return wsecRet;
}
