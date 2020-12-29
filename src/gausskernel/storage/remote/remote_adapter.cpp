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
 * remote_adapter.cpp
 *         using simple C API interface for rpc call PG founction
 *         Don't include any of RPC header file.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/remote/remote_adapter.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xlog.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "storage/custorage.h"
#include "storage/ipc.h"
#include "storage/remote_adapter.h"
#include "utils/guc.h"
#include "utils/memutils.h"

extern GaussdbThreadEntry GetThreadEntry(knl_thread_role role);
#define MAX_WAIT_TIMES 5

typedef struct RemoteReadContext {
    CUStorage* custorage;
    CU* cu;
    char* pagedata;
} RemoteReadContext;

/*
 * @Description: wait lsn to replay
 * @IN primary_insert_lsn: remote request lsn
 * @Return: remote read error code
 * @See also:
 */
int XLogWaitForReplay(uint64 primary_insert_lsn)
{
    int wait_times = 0;

    /* local replayed lsn */
    XLogRecPtr standby_replay_lsn = GetXLogReplayRecPtr(NULL, NULL);

    /* if primary_insert_lsn  >  standby_replay_lsn then need wait */
    while (!XLByteLE(primary_insert_lsn, standby_replay_lsn)) {
        /* if sleep to much times */
        if (wait_times >= MAX_WAIT_TIMES) {
            ereport(LOG,
                    (errmodule(MOD_REMOTE),
                     errmsg("replay slow. requre lsn %X/%X, replayed lsn %X/%X",
                            (uint32)(primary_insert_lsn >> 32),
                            (uint32)primary_insert_lsn,
                            (uint32)(standby_replay_lsn >> 32),
                            (uint32)standby_replay_lsn)));
            return REMOTE_READ_NEED_WAIT;
        }

        /* sleep 1s */
        pg_usleep(1000000L);
        ++wait_times;

        /* get current replay lsn again */
        (void)GetXLogReplayRecPtr(NULL, &standby_replay_lsn);
    }

    return REMOTE_READ_OK;
}

/*
 * @Description: read cu for primary
 * @IN spcnode: tablespace id
 * @IN dbnode: database id
 * @IN relnode: relfilenode
 * @IN colid: column id
 * @IN offset: cu offset
 * @IN size: cu size
 * @IN lsn: lsn wait for replay
 * @IN/OUT context: read context
 * @OUT cudata: output cu data
 * @Return: remote read error code
 * @See also:
 */
int StandbyReadCUforPrimary(uint32 spcnode, uint32 dbnode, uint32 relnode, int32 colid, uint64 offset, int32 size,
                            uint64 lsn, RemoteReadContext* context, char** cudata)
{
    Assert(cudata);

    (void)MemoryContextSwitchTo(t_thrd.storage_cxt.remote_function_context);

    int ret_code = REMOTE_READ_OK;

    /* wait request lsn for replay */
    ret_code = XLogWaitForReplay(lsn);
    if (ret_code != REMOTE_READ_OK) {
        return ret_code;
    }

    RelFileNode relfilenode {spcnode, dbnode, relnode, InvalidBktId};

    PG_TRY();
    {
        /* read from disk */
        CFileNode cfilenode(relfilenode, colid, MAIN_FORKNUM);

        context->custorage = New(CurrentMemoryContext) CUStorage(cfilenode);
        context->cu = New(CurrentMemoryContext) CU();
        context->cu->m_inCUCache = false;

        context->custorage->LoadCU(context->cu, offset, size, false, false);

        /* check crc */
        if (ret_code == REMOTE_READ_OK) {
            if (!context->cu->CheckCrc())
                ret_code = REMOTE_READ_CRC_ERROR;
            else
                *cudata = context->cu->m_compressedLoadBuf;
        }
    }
    PG_CATCH();
    {
        ret_code = REMOTE_READ_IO_ERROR;

        /* Using LOG level output the error message */
        (void)MemoryContextSwitchTo(t_thrd.storage_cxt.remote_function_context);
        ErrorData* edata = CopyErrorData();
        ereport(LOG, (errmodule(MOD_REMOTE), errmsg("Fail to load CU for remote, cause: %s", edata->message)));
        FlushErrorState();
        FreeErrorData(edata);
    }
    PG_END_TRY();

    return ret_code;
}

/*
 * @Description: read page for primary
 * @IN spcnode: tablespace id
 * @IN dbnode: database id
 * @IN relnode: relfilenode
 * @IN forknum: forknum
 * @IN blocknum: block number
 * @IN blocksize: block size
 * @IN lsn: lsn wait for replay
 * @IN/OUT context: read context
 * @OUT pagedata: output page data
 * @Return: remote read error code
 * @See also:
 */
int StandbyReadPageforPrimary(uint32 spcnode, uint32 dbnode, uint32 relnode, int16 bucketnode, int32 forknum,
                              uint32 blocknum, uint32 blocksize, uint64 lsn, RemoteReadContext* context, char** pagedata)
{
    Assert(pagedata);

    if (unlikely(blocksize != BLCKSZ))
        return REMOTE_READ_BLCKSZ_NOT_SAME;

    (void)MemoryContextSwitchTo(t_thrd.storage_cxt.remote_function_context);

    int ret_code = REMOTE_READ_OK;

    /* wait request lsn for replay */
    ret_code = XLogWaitForReplay(lsn);
    if (ret_code != REMOTE_READ_OK) {
        return ret_code;
    }

    RelFileNode relfilenode {spcnode, dbnode, relnode, bucketnode};

    PG_TRY();
    {
        context->pagedata = (char*)palloc0(BLCKSZ);

        bool hit = false;

        /* read page, if PageIsVerified failed will long jump to PG_CATCH() */
        Buffer buf = ReadBufferForRemote(relfilenode, forknum, blocknum, RBM_FOR_REMOTE, NULL, &hit);

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        Block block = BufferGetBlock(buf);

        errno_t rc = memcpy_s(context->pagedata, BLCKSZ, block, BLCKSZ);
        if (rc != EOK) {
            ereport(LOG, (errmodule(MOD_REMOTE), errmsg("memcpy_s error, retcode=%d", rc)));
            ret_code = REMOTE_READ_MEMCPY_ERROR;
        }

        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        ReleaseBuffer(buf);

        if (ret_code == REMOTE_READ_OK) {
            *pagedata = context->pagedata;
            PageSetChecksumInplace((Page) * pagedata, blocknum);
        }

        if (t_thrd.utils_cxt.CurrentResourceOwner != NULL)
            ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);

        smgrcloseall();
    }
    PG_CATCH();
    {
        ret_code = REMOTE_READ_IO_ERROR;

        /* Prevent interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /* clear buffer and lock */
        LWLockReleaseAll();
        pgstat_report_waitevent(WAIT_EVENT_END);
        AbortBufferIO();
        UnlockBuffers();
        if (t_thrd.utils_cxt.CurrentResourceOwner != NULL)
            ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);

        /* Using LOG level output the error message */
        (void)MemoryContextSwitchTo(t_thrd.storage_cxt.remote_function_context);
        ErrorData* edata = CopyErrorData();
        ereport(LOG, (errmodule(MOD_REMOTE), errmsg("Fail to load Page for remote, cause: %s", edata->message)));

        /* checksum error */
        if (edata->sqlerrcode == ERRCODE_DATA_CORRUPTED)
            ret_code = REMOTE_READ_CRC_ERROR;

        FlushErrorState();
        FreeErrorData(edata);

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        smgrcloseall();
    }
    PG_END_TRY();

    return ret_code;
}

/*
 * @Description: init thread state for rpc thread
 * @See also:
 */
void InitWorkEnv()
{
    if (!t_thrd.storage_cxt.work_env_init) {
        knl_thread_arg arg;
        arg.role = RPC_WORKER;
        arg.t_thrd = &t_thrd;
        /* binding static TLS variables for current thread */
        EarlyBindingTLSVariables();
        if ((GetThreadEntry(RPC_WORKER))(&arg) == 0) {
            t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "rpc worker thread",
                MEMORY_CONTEXT_STORAGE);
            t_thrd.storage_cxt.work_env_init = true;
        }
    }
}

/*
 * @Description: clear thread state for rpc thread
 * @See also:
 */
void CleanWorkEnv()
{
    if (t_thrd.storage_cxt.work_env_init) {
        proc_exit(0);
    }
}

/*
 * @Description: init remote read context
 * @Return: remote read context
 * @See also:
 */
RemoteReadContext* InitRemoteReadContext()
{
    InitWorkEnv();

    if (t_thrd.storage_cxt.remote_function_context == NULL) {
        t_thrd.storage_cxt.remote_function_context = AllocSetContextCreate((MemoryContext)NULL,
                                                                           "remote function",
                                                                           ALLOCSET_DEFAULT_MINSIZE,
                                                                           ALLOCSET_DEFAULT_INITSIZE,
                                                                           ALLOCSET_DEFAULT_MAXSIZE);
    }
    (void)MemoryContextSwitchTo(t_thrd.storage_cxt.remote_function_context);

    RemoteReadContext* context = (RemoteReadContext*)palloc0(sizeof(RemoteReadContext));
    return context;
}

/*
 * @Description: clear remote read context
 * @IN context:remote read context
 * @See also:
 */
void ReleaseRemoteReadContext(RemoteReadContext* context)
{
    if (context != NULL) {
        if (context->custorage != NULL)
            DELETE_EX(context->custorage);

        if (context->cu != NULL)
            DELETE_EX(context->cu);

        if (context->pagedata != NULL)
            pfree_ext(context->pagedata);

        pfree_ext(context);
    }

    MemoryContextReset(t_thrd.storage_cxt.remote_function_context);
}

/*
 * @Description:get the grpc certificate path environment
 * @IN envVar:get which env
 * @OUT outputEnvStr:env value
 * @See alse:
 */
bool GetCertEnv(const char* envName, char* outputEnvStr, size_t envValueLen)
{
    const char* tmpenv = NULL;
    char* envValue = NULL;
    const char* dangerCharacterList[] = {"|",
                                         ";",
                                         "&",
                                         "$",
                                         "<",
                                         ">",
                                         "`",
                                         "\\",
                                         "'",
                                         "\"",
                                         "{",
                                         "}",
                                         "(",
                                         ")",
                                         "[",
                                         "]",
                                         "~",
                                         "*",
                                         "?",
                                         "!",
                                         "\n",
                                         NULL
                                        };
    int index = 0;
    errno_t rc = EOK;

    if (envName == NULL) {
        ::OutputMsgforRPC(WARNING, "Invalid env Name: \"%s\".", envName);
        return false;
    }

    tmpenv = getenv(envName);
    if (tmpenv == NULL) {
        ::OutputMsgforRPC(WARNING,
                          "Failed to get environment variable: \"%s\". Please check and make sure it is configured!",
                          envName);
        return false;
    }

    size_t len = strlen(tmpenv);
    if (len >= envValueLen) {
        ::OutputMsgforRPC(WARNING, "len(%lu) > envValueLen(%lu), "
                          "Failed to get environment variable: \"%s\". Please check and make sure it is configured!",
                          len, envValueLen, envName);
        return false;
    }

    envValue = (char *)malloc(len + 1);
    if (envValue == NULL) {
        ::OutputMsgforRPC(WARNING,
                          "Failed to malloc when get environment variable: \"%s\"!", envName);
        return false;
    }
    rc = strcpy_s(envValue, len + 1, tmpenv);
    securec_check_c(rc, "", "");

    if (envValue[0] == '\0') {
        ::OutputMsgforRPC(WARNING,
                          "Failed to get environment variable: \"%s\". Please check and make sure it is configured!",
                          envName);
        free(envValue);
        return false;
    }

    for (; dangerCharacterList[index] != NULL; index++) {
        if (strstr(envValue, dangerCharacterList[index]) != NULL) {
            ::OutputMsgforRPC(
                WARNING, "Failed to check environment value: invalid token \"%s\".", dangerCharacterList[index]);
            free(envValue);
            return false;
        }
    }

    rc = strcpy_s(outputEnvStr, len + 1, envValue);
    securec_check_c(rc, "", "");

    free(envValue);
    return true;
}

/*
 * @Description:get the grpc certificate context
 * @IN path:Certification path
 * @OUT len: Certification len
 * @return buf: Certification context
 * @See alse:
 */
char* GetCertStr(char* path, int* len)
{
    char* retVal = NULL;
    int fd = -1;
    char* buf = NULL;
    char lRealPath[PATH_MAX] = {0};
    struct stat statBuf;

    if (path == NULL) {
        ::OutputMsgforRPC(LOG, "Invalid path: \"%s\". Please check it", path);
        return NULL;
    }
    retVal = realpath(path, lRealPath);
    if (retVal == NULL) {
        ::OutputMsgforRPC(LOG, "Could not get realpath: \"%s\".", path);
        return NULL;
    }
    if ((fd = open(lRealPath, O_RDONLY | PG_BINARY, 0)) < 0) {
        ::OutputMsgforRPC(LOG, "Could not open relpath: \"%s\".", path);
        return NULL;
    }
    if (fstat(fd, &statBuf) < 0) {
        ::OutputMsgforRPC(LOG, "Could not open file: \"%s\".", path);
        (void)close(fd);
        return NULL;
    }

    buf = (char*)malloc(statBuf.st_size + 1);
    if (buf == NULL) {
        ::OutputMsgforRPC(ERROR, "Out of memory when getting grpc certificates.");
        (void)close(fd);
        return NULL;
    }
    *len = (int)read(fd, buf, statBuf.st_size);
    if (*len != statBuf.st_size) {
        free(buf);
        *len = 0;
        (void)close(fd);
        return NULL;
    }
    buf[statBuf.st_size] = '\0';

    (void)close(fd);
    return buf;
}

/*
 * @Description: get remote mode for rpc thread
 * @Return: remote read mode
 * @See also:
 */
int GetRemoteReadMode()
{
    return g_instance.attr.attr_storage.remote_read_mode;
}

/*
 * @Description: output log fro rpc thread
 * @IN fmt: log format
 * @See also:
 */
void OutputMsgforRPC(int elevel, const char* fmt, ...)
{
    /* must call after InitWorkEnv() in RPC worker thread */
    if (fmt != NULL) {
        StringInfoData emsg;
        initStringInfo(&emsg);

        for (;;) {
            va_list args;
            bool success = false;

            va_start(args, fmt);
            success = appendStringInfoVA(&emsg, fmt, args);
            va_end(args);

            if (success) {
                break;
            }

            enlargeStringInfo(&emsg, emsg.maxlen);
        }

        ereport(elevel, (errmodule(MOD_REMOTE), errmsg("%s", emsg.data)));

        pfree(emsg.data);
        emsg.data = NULL;
    }
}
