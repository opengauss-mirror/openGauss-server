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
 * fencedudf.cpp
 * Implement to run UDF in fenced mode
 *
 * The core design is to implement a RPC server and client, run udf in RPC server.
 * User defined C function is not safe, because if the UDF has some bugs which can
 * cause coredump or memory leak in gaussdb, So we need provide an fenced mode for udf.
 * When we run fenecd udf, it will run in RPC server. If UDF cause coredump, there is not
 * any impact for gaussdb process.
 * -----------------------------------------------------------------------------
 * |	Database Process Model					|	Fenced Master Process
 * |											|		\	|	/
 * |											|			|	Fork
 * |____________________________________________|________________________________
 * |	Gaussdb process	 ----- RPC Request -----> 	Fenced Worker Process
 * |					 <------ Result---------
 * |____________________________________________________________________________
 * |	Gaussdb process	 ----- RPC Request ----->	Fenced Worker Process
 * |					 <------ Result---------
 * |----------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/fencedudf.cpp
 *
 * ------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>
#include <limits.h>
#include "lib/stringinfo.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "postmaster/syslogger.h"
#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pg_shmem.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "gssignal/gs_signal.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "utils/fmgrtab.h"
#include "utils/postinit.h"
#include "utils/relmapper.h"
#include "catalog/pg_language.h"
#include "catalog/pg_proc.h"
#include "dynloader.h"
#include "catalog/pg_type.h"
#include "utils/memutils.h"
#include <sys/resource.h>
#include <execinfo.h>
#include "pgxc/pgxc.h"
#include "port.h"
#include "fencedudf.h"

typedef enum { UDF_INFO = 5, UDF_ARGS, UDF_RESULT, UDF_ERROR } UDFMsgType;

typedef enum { UDF_SEND_ARGS = 0, UDF_RECV_ARGS = 1, UDF_SEND_RESULT = 2, UDF_RECV_RESULT = 3 } UDFArgHandlerType;

typedef enum { C_UDF, JAVA_UDF, PYTHON_UDF} UDF_LANG;

typedef enum { MASTER_PROCESS = 1, WORK_PROCESS, KILL_WORK } UDF_Process;

typedef enum {
    EXTENDLIB_CHECK_OPTION = 1,
    EXTENDLIB_PARSE_OPERATION,
    EXTENDLIB_CALL_OBS_DOWNLOAD,
    EXTENDLIB_CALL_OM
} ExtendLibraryStatus;

typedef enum {
    OBS_SUCCESS = 0,
    OBS_FAILED_INVALID_ARGUMENT = 99,
    OBS_FAILED_INCORRECT_REGION = 98,
    OBS_FAILED_IO_ERROR = 97,
    OBS_FAILED_BASE_EXCEPTION = 96,
    OBS_FAILED_CONNECT_TO_UDS_SERVER = 1,
    OBS_FAILED_DOWNLOAD_FROM_UDS_SERVER = 4,
    OBS_FAILED_TRY_AGAIN = 50
} OBSStatus;

typedef enum { OPTION_ADDJAR = 0, OPTION_LS, OPTION_RMJAR } LibraryOptType;

typedef struct OBSInfo {
    char* accesskey;
    char* secretkey;
    char* region;
    char* bucket;
    char* path;
} OBSInfo;

typedef struct LibraryInfo {
    char* option;
    char* operation;
    LibraryOptType optionType;
    char* destpath;
    bool isSourceLocal;
    char* localpath;
    OBSInfo* obsInfo;
} LibraryInfo;

#define KB 1024
#define UDF_MSG_BUFLEN 8192

#define FENCED_UDF_UNIXSOCKET ".gaussUDF.socket"

THR_LOCAL int UDFRPCSocket = -1;
THR_LOCAL Oid lastUDFOid = InvalidOid;

static MemoryContext UDFWorkMemContext = NULL;
static int listenUDFSocket = -1;
static bool UDFMasterQuitFlag = false;
static HTAB* UDFFuncHash = NULL;

/* PRC Server Declaration */
static void UDFMasterServerLoop();
static void UDFWorkerMain(int socket);
static pid_t StartUDFWorker(int socket);
static void SIGQUITUDFMaster(SIGNAL_ARGS);
static void RecvUDFInformation(int socket, FunctionCallInfoData* fcinfo);
static void FindOrInsertUDFHashTab(FunctionCallInfoData* fcinfo);
static void UDFCreateHashTab();
void SetUDFUnixSocketPath(struct sockaddr_un* unAddrPtr);

/* RPC Client Declaration */
template <bool batchMode>
static void SendUDFInformation(FunctionCallInfo fcinfo);

template <bool batchMode>
extern Datum RPCFencedUDF(FunctionCallInfo fcinfo);

bool RPCInitFencedUDFIfNeed(Oid functionId, FmgrInfo* finfo, HeapTuple procedureTuple);
void InitFuncCallUDFInfo(FunctionCallInfoData* fcinfo, int argN, bool setFuncPtr);
void InitUDFInfo(UDFInfoType* udfInfo, int argN, int batchRows);
void FencedUDFMasterMain(int argc, char* argv[]);

/* Extern Enterface Declaration */
extern void* internal_load_library(const char* libname);


void StartUDFMaster()
{
    pid_t fencedWorkPid = 0;
    switch ((fencedWorkPid = fork_process())) {
        case -1: {
            int errsv = errno;
            ereport(WARNING,
                (errmodule(MOD_UDF),
                    errcode(ERRCODE_INVALID_STATUS),
                    errmsg("could not fork gaussDB process, errno:%d, error reason:%s",
                        errsv,
                        strerror(errsv))));
            break;
        }
        case 0: {

            FencedUDFMasterMain(0, NULL);
            exit(0);
            break;
        }
        default: {
            break;
        }
    }
}
/*
 * @Description: The main function of UDF RPC server.
 * It will initialize envrionment, create listen socket and startup
 * erverLoop for accept connection and start worker process
 * @IN argc: the number of argment value
 * @IN argv: the argment values
 */
void FencedUDFMasterMain(int argc, char* argv[])
{
    SetProcessingMode(FencedProcessing);

    /* Just make set_ps_dispaly ok */
    IsUnderPostmaster = true;
    set_ps_display("gaussdb fenced UDF master process", true);

    /* Step 1: Initialize envrionment: signal handle, display info */
    gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
    gs_signal_block_sigusr2();

    (void)gspqsignal(SIGHUP, SIG_IGN);           /* reread config file and have
                                                  * children do same */
    (void)gspqsignal(SIGINT, SIGQUITUDFMaster);  /* send SIGTERM and shut down */
    (void)gspqsignal(SIGQUIT, SIGQUITUDFMaster); /* send SIGQUIT and die */
    (void)gspqsignal(SIGTERM, SIGQUITUDFMaster); /* wait for children and shut down */

    pqsignal(SIGPIPE, SIG_IGN); /* ignored */
    pqsignal(SIGFPE, SIG_IGN);
    pqsignal(SIGCLD, SIG_IGN);

    (void)gspqsignal(SIGUSR1, SIG_IGN); /* message from child process */
    (void)gspqsignal(SIGUSR2, SIG_IGN); /* unused, reserve for children */
    (void)gspqsignal(SIGCHLD, SIG_IGN); /* handle child termination */
    (void)gspqsignal(SIGTTIN, SIG_IGN); /* ignored */
    (void)gspqsignal(SIGTTOU, SIG_IGN); /* ignored */

    /* ignore SIGXFSZ, so that ulimit violations work like disk full */
#ifdef SIGXFSZ
    (void)gspqsignal(SIGXFSZ, SIG_IGN); /* ignored */
#endif

#if ((defined ENABLE_PYTHON2) || (defined ENABLE_PYTHON3))
    BaseInit();
    InitProcess();
    t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "fencedMaster",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_AI));
#endif

    /*
     * Step 2: Create socket for listening coming connection
     * Create listen socket using unix domain way which don't occupy port
     * In future, if we want to run RPC in remote machine, we need use tcp
     * or other communication protocol
     */
    int socketProtocol = AF_UNIX;
    struct sockaddr_un unAddr;
    unAddr.sun_family = AF_UNIX;
    listenUDFSocket = socket(socketProtocol, SOCK_STREAM, 0);
    if (listenUDFSocket < 0)
        ereport(FATAL, (errmodule(MOD_UDF), errmsg("could not create Unix-domain socket: %m")));

    SetUDFUnixSocketPath(&unAddr);

    unlink(unAddr.sun_path);
    if (bind(listenUDFSocket, (struct sockaddr*)&unAddr, sizeof(unAddr)) < 0) {
        ereport(FATAL, (errmodule(MOD_UDF), errmsg("bind socket path %s failed:%m", unAddr.sun_path)));
    }
    if (listen(listenUDFSocket, 65536) < 0) {
        ereport(FATAL, (errmodule(MOD_UDF), errmsg("listen socket failed: %m")));
    }

    ereport(LOG, (errmodule(MOD_UDF), errmsg("Fenced master process start OK.")));

    /* Step 3: ServerLoop for accept connection and start worker process */
    UDFMasterServerLoop();
    return;
}

/*
 * @Description: Server loop of UDF master process
 * It will fork new RPC worker process to run udf once RPC request arrive
 */
void UDFMasterServerLoop()
{
    const int nSockets = 1;
    struct pollfd ufds[1];

    ufds[0].fd = listenUDFSocket;
    ufds[0].events = POLLIN | POLLPRI;

    for (;;) {
        int selres;

        if (UDFMasterQuitFlag)
            exit(0);

        /*
         * Wait for a connection request to arrive.
         *
         * We wait at most one minute, to ensure that the other background
         * tasks handled below get done even when no requests are arriving.
         */
        gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
        (void)gs_signal_unblock_sigusr2();

        /* must set timeout each time; some OSes change it! */
        struct timeval timeout;

        timeout.tv_sec = 60;
        timeout.tv_usec = 0;

        selres = poll(ufds, nSockets, timeout.tv_sec * 1000);

        /*
         * Block all signals until we wait again.  (This makes it safe for our
         * signal handlers to do nontrivial work.)
         */
        gs_signal_setmask(&t_thrd.libpq_cxt.BlockSig, NULL);
        gs_signal_block_sigusr2();

        /* Now check the poll() result */
        if (selres < 0) {
            if (errno != EINTR && errno != EWOULDBLOCK) {
                int errsv = errno;
                ereport(ERROR,
                    (errmodule(MOD_UDF),
                        errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("poll() failed in UDFMasterServerLoop, errno:%d, error reason:%s",
                            errsv,
                            strerror(errsv))));
                return;
            }
        }

        /*
         * New connection pending on any of our sockets? If so, fork a child
         * process to deal with it.
         */
        if (selres > 0) {
            if (ufds[0].fd == PGINVALID_SOCKET) {
                ereport(WARNING,
                    (errmodule(MOD_UDF),
                        errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("poll() returned %d, but the first file descriptor is invalid.", selres)));
                break;
            }
            if (ufds[0].fd != listenUDFSocket) {
                ereport(WARNING,
                    (errmodule(MOD_UDF),
                        errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("poll() returned %d, but the first file descriptor is not listenUDFSocket.", selres)));
                break;
            }
            if (((unsigned int)ufds[0].revents) & POLLERR) {
                ereport(WARNING,
                    (errmodule(MOD_UDF),
                        errcode(ERRCODE_CONNECTION_EXCEPTION),
                        errmsg("poll() returned %d, but ufds[0].revents returned with POLLERR", selres)));
            }

            if (((unsigned int)ufds[0].revents) & POLLIN) {
                ufds[0].revents = 0;

                int socket = accept(ufds[0].fd, NULL, NULL);

                if (socket < 0) {
                    int errsv = errno;
                    ereport(WARNING,
                        (errmodule(MOD_UDF),
                            errcode(ERRCODE_CONNECTION_EXCEPTION),
                            errmsg("accept failed, errno:%d, error reason:%s", errsv, strerror(errsv))));
                } else {
                    int res;
                    if (StartUDFWorker(socket) < 0) {
                        ereport(WARNING,
                            (errmodule(MOD_UDF),
                                errcode(ERRCODE_INVALID_STATUS),
                                errmsg("failed to fork fencedWorker porcess")));
                    }
                    /* we must close socket because worker process have inherit it */
                    res = close(socket);
                    if (res != 0) {
                        int errsv = errno;
                        ereport(WARNING,
                            (errmodule(MOD_UDF),
                                errcode(ERRCODE_CONNECTION_EXCEPTION),
                                errmsg("UDF master close client socket failed, errno:%d, error reason:%s",
                                    errsv,
                                    strerror(errsv))));
                    }
                }
            }
        }
    }
}

/*
 * @Description: Fork worker process for running UDF
 * @IN socket: Communication with RPC client
 */
pid_t StartUDFWorker(int socket)
{
    pid_t fencedWorkPid = 0;

    switch ((fencedWorkPid = fork_process())) {
        case -1: {
            int errsv = errno;
            ereport(WARNING,
                (errmodule(MOD_UDF),
                    errcode(ERRCODE_INVALID_STATUS),
                    errmsg("could not fork fenced UDF master process, errno:%d, error reason:%s",
                        errsv,
                        strerror(errsv))));
            break;
        }
        case 0: {
            /* Child process */			
            /* Just make set_ps_dispaly ok */
            IsUnderPostmaster = true;

            /* Close listenUDFSocket inherit from parent process */
            if (listenUDFSocket > -1) {
                close(listenUDFSocket);
                listenUDFSocket = -1;
            } else {
                ereport(WARNING,
                    (errmodule(MOD_UDF),
                        errcode(ERRCODE_INVALID_STATUS),
                        errmsg("Unexpected error occurs, listenUDFSocket <= -1")));
            }
            set_ps_display("gaussdb fenced UDF worker process", true);
            UDFWorkerMain(socket);

            /* worker process should exit if done */
            exit(0);
            break;
        }
        default: {
            break;
        }
    }
    return fencedWorkPid;
}

static inline void SendMsg(int socket, char* val, int len)
{
    AssertEreport(len > 0, MOD_UDF, "Msg's length should be greater than 0 when it's being sent.");
    char* startPtr = val;
    char* endPtr = val + len;
    while (startPtr < endPtr) {
        int nbytes = write(socket, startPtr, endPtr - startPtr);
        if (nbytes < 0) {
            if (errno == EINTR)
                continue; /* Ok if interrupted */
            ereport(ERROR,
                (errmodule(MOD_UDF), errcode(ERRCODE_CONNECTION_FAILURE), errmsg("Socket send %d bytes: %m", nbytes)));
        }
        startPtr += nbytes;
    }
}

static inline void RecvMsg(int socket, char* buffer, int len, bool isIdle = false)
{
    int recvBytes = 0;
    StringInfoData hintMsgStr;
    while (recvBytes < len) {
        int bytes = read(socket, buffer + recvBytes, len - recvBytes);
        if (bytes <= 0) {
            if (errno == EINTR)
                continue; /* Ok if interrupted */
            char* err_reason = strerror(errno);

            /* The udf worker process is finished. */
            if (!IS_PGXC_COORDINATOR && !IS_PGXC_DATANODE && isIdle) {
                exit(0);
            }
            /* Both GaussDB kernel and UDF master/worker can call this function.
             * So, in GaussDB kernel, myName should be PGXCNodeName. If this
             * function is called from UDF master/worker, myName should be
             * "UDF Master/Worker".
             */
            char* myName = "UDF Master/Worker";
            char* peerName = "GaussDB Kernel(CN/DN)";
            if (IS_PGXC_COORDINATOR || IS_PGXC_DATANODE) {
#ifdef PGXC
                myName = g_instance.attr.attr_common.PGXCNodeName;
#else
                myName = "GaussDB Kernel(CN/DN)";
#endif
                peerName = "UDF Master/Worker";
            }

            StringInfoData errMsgStr;
            initStringInfo(&errMsgStr);
            appendStringInfo(&errMsgStr,
                "The connection between '%s' and '%s' maybe closed. '%s' recv %d bytes. Error: %s",
                myName,
                peerName,
                myName,
                bytes,
                err_reason);

            if (IS_PGXC_COORDINATOR || IS_PGXC_DATANODE) {
                initStringInfo(&hintMsgStr);
                if (u_sess->attr.attr_sql.FencedUDFMemoryLimit) {
                    appendStringInfo(&hintMsgStr,
                        "FencedUDFMemoryLimit is set to %dKB. Please check if it is too small, or you can just set "
                        "FencedUDFMemoryLimit to 0KB, which will not limit visual memory usage.",
                        u_sess->attr.attr_sql.FencedUDFMemoryLimit);
                }
                if (strlen(u_sess->attr.attr_sql.pljava_vmoptions) > 0) {
                    if (strlen(hintMsgStr.data) > 0)
                        appendStringInfo(&hintMsgStr, "\n");
                    appendStringInfo(&hintMsgStr,
                        "pljava_vmoptions is set to '%s', please check every parameter in it, e.g., if -Xmx or "
                        "-XX:MaxMetaspaceSize are set, please check if they are too large compared to "
                        "FencedUDFMemoryLimit",
                        u_sess->attr.attr_sql.pljava_vmoptions);
                }

                if (strlen(hintMsgStr.data) > 0) {
                    appendStringInfo(&errMsgStr, "\nHINT:%s", hintMsgStr.data);
                }
                pfree_ext(hintMsgStr.data);
            }

            ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_DATA_EXCEPTION), errmsg("%s", errMsgStr.data)));

            pfree_ext(errMsgStr.data);

            exit(0);
        }
        recvBytes += bytes;
    }
    AssertEreport(recvBytes == len, MOD_UDF, "Bytes received is not the same as expected.");
}

static char* GetBasicUDFInformation(FunctionCallInfoData* fcinfo)
{
    uint len = 0;
    Oid fnOid = 0;
    Oid lagOid = 0;
    char fnVolatile;
    FmgrInfo* flinfo = fcinfo->flinfo;
    char* readPtr = fcinfo->udfInfo.udfMsgBuf->data;
    uint remainLen = fcinfo->udfInfo.udfMsgBuf->len;

    /* Get Message type */
    short msgType;
    GetFixedMsgValSafe(readPtr, (char*)&msgType, sizeof(msgType), sizeof(msgType), remainLen);

    if (msgType == UDF_ARGS) {
        /* Get Function Oid */
        GetFixedMsgValSafe(readPtr, (char*)&fnOid, sizeof(fnOid), sizeof(fnOid), remainLen);
        flinfo->fn_oid = fnOid;
    } else {
        /* Get memory limit size */
        GetFixedMsgValSafe(readPtr, (char*)&u_sess->attr.attr_sql.FencedUDFMemoryLimit,
            sizeof(u_sess->attr.attr_sql.FencedUDFMemoryLimit),
            sizeof(u_sess->attr.attr_sql.FencedUDFMemoryLimit), remainLen);

        /* Get pljava_vmoptions */
        GetFixedMsgValSafe(readPtr, (char*)&len, sizeof(len), sizeof(len), remainLen);
        u_sess->attr.attr_sql.pljava_vmoptions = (char*)palloc(len + 1);
        GetFixedMsgValSafe(readPtr, u_sess->attr.attr_sql.pljava_vmoptions, len, len + 1, remainLen);
        u_sess->attr.attr_sql.pljava_vmoptions[len] = 0;

        /* Get Function Oid */
        GetFixedMsgValSafe(readPtr, (char*)&fnOid, sizeof(fnOid), sizeof(fnOid), remainLen);
        flinfo->fn_oid = fnOid;

        /* Get Language Oid */
        GetFixedMsgValSafe(readPtr, (char*)&lagOid, sizeof(lagOid), sizeof(lagOid), remainLen);
        flinfo->fn_languageId = lagOid;

        /* Get func volatile propertie */
        GetFixedMsgValSafe(readPtr, (char*)&fnVolatile, sizeof(fnVolatile), sizeof(fnVolatile), remainLen);
        flinfo->fn_volatile = fnVolatile;

        /* Get UDF name */
        GetVarMsgValSafe(readPtr, flinfo->fnName, len, NAMEDATALEN, remainLen);
        flinfo->fnName[len] = 0;

        /* Get Library path */
        GetFixedMsgValSafe(readPtr, (char*)&len, sizeof(len), sizeof(len), remainLen);
        flinfo->fnLibPath = (char*)palloc(len + 1);
        GetFixedMsgValSafe(readPtr, flinfo->fnLibPath, len, len + 1, remainLen);
        flinfo->fnLibPath[len] = 0;

        /* Get Argument number */
        GetFixedMsgValSafe(readPtr, (char*)&flinfo->fn_nargs, sizeof(flinfo->fn_nargs), sizeof(flinfo->fn_nargs), 
            remainLen);

        /* Macro is OK */
        InitFunctionCallInfoArgs(*fcinfo, flinfo->fn_nargs, BatchMaxSize);

        if (flinfo->fn_nargs > 0) {
            /* Get Argument type */
            GetFixedMsgValSafe(readPtr, (char*)fcinfo->argTypes, sizeof(Oid) * flinfo->fn_nargs, 
                sizeof(Oid) * flinfo->fn_nargs, remainLen);
        }

        /* Get Result Type */
        GetFixedMsgValSafe(readPtr, (char*)&flinfo->fn_rettype, sizeof(flinfo->fn_rettype), sizeof(flinfo->fn_rettype), 
            remainLen);
    }
    return readPtr;
}

static void GetUDFArguments(FunctionCallInfoData* fcinfo)
{
    GetFixedMsgVal(fcinfo->udfInfo.msgReadPtr,
        (char*)&fcinfo->udfInfo.argBatchRows,
        sizeof(fcinfo->udfInfo.argBatchRows),
        sizeof(fcinfo->udfInfo.argBatchRows));

    /* Argument Values Handler */
    for (int row = 0; row < fcinfo->udfInfo.argBatchRows; row++) {
        for (int idx = 0; idx < fcinfo->nargs; idx++) {
            (*(fcinfo->udfInfo.UDFArgsHandlerPtr + idx))(fcinfo, idx, 0);
            fcinfo->udfInfo.arg[row][idx] = fcinfo->arg[idx];
            fcinfo->udfInfo.null[row][idx] = fcinfo->argnull[idx];
        }
    }
}
static void RecvUDFInformation(int socket, FunctionCallInfoData* fcinfo)
{
    uint  len = 0;
    char* allocPtr = NULL;
    char buffer[UDF_MSG_BUFLEN];
    char* readPtr = buffer;

    /*
     * Step 1: Initialize function call structure and recv message header and body
     */
    FmgrInfo* flinfo = (FmgrInfo*)palloc0(sizeof(FmgrInfo));
    fcinfo->flinfo = flinfo;

    flinfo->fn_fenced = true;
    InitFunctionCallInfoData(*fcinfo, flinfo, 0, 0, NULL, NULL);

    /* Get message length */
    RecvMsg(socket, buffer, 4, true);
    len = *(uint*)buffer;
    if (unlikely(len >= MaxAllocSize)) {
        ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Invalid udf message len:%u", len)));
    }
    readPtr = buffer;
    if (unlikely(len >= UDF_MSG_BUFLEN)) {
        allocPtr = readPtr = (char*)palloc(len);
    }
    /* Get message body */
    RecvMsg(socket, readPtr, len);
    /* Copy readPtr data into fcinfo->udfInfo.udfMsgBuf */
    AppendBufFixedMsgVal(fcinfo->udfInfo.udfMsgBuf, readPtr, len);

    /*
     * Step 2: Get basic UDF information (OID, NAME, LIBPATH and so on)
     */
    readPtr = GetBasicUDFInformation(fcinfo);

    /* Find or Insert UDF hash table if not found */
    FindOrInsertUDFHashTab(fcinfo);

    /* Attention, We must set the current read cursor */
    fcinfo->udfInfo.msgReadPtr = readPtr;

    /*
     * Step 3: Get arguments of UDF
     */
    GetUDFArguments(fcinfo);

    if (unlikely(allocPtr != NULL))
        pfree_ext(allocPtr);

    return;
}

template <UDF_LANG udfLang>
static inline Datum RunUDF(FunctionCallInfoData* fcinfo)
{
    fcinfo->isnull = false;
    Datum result = 0;
    result = FunctionCallInvoke(fcinfo);
    return result;
}

void SetUDFMemoryLimit()
{
    struct rlimit memory_limit;

    if (u_sess->attr.attr_sql.FencedUDFMemoryLimit > 0) {
        /* baseVirtualMemSize is 200MB */
        const int64 baseVirtualMemSize = 200 * KB * KB;
        memory_limit.rlim_cur = memory_limit.rlim_max =
            (int64)u_sess->attr.attr_sql.FencedUDFMemoryLimit * KB + baseVirtualMemSize;
        if (setrlimit(RLIMIT_AS, &memory_limit))
            ereport(WARNING, (errmodule(MOD_UDF), errmsg("setrlimit error: %m")));
    }
}

void changeDatabase(unsigned int currentDatabaseId)
{
    if (currentDatabaseId!= 0 && currentDatabaseId != u_sess->proc_cxt.MyDatabaseId){
        u_sess->proc_cxt.MyDatabaseId = currentDatabaseId;
        t_thrd.proc_cxt.PostInit->InitFencedSysCache();
    }
}

/*
 * @Description: The main function which run UDF
 * @IN socket: argument values is socket communication with RPC client
 */
static void UDFWorkerMain(int socket)
{
    bool hasSetMemLimit = false;
    UDFWorkMemContext = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "UDF_Work_MemContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    if (!UDFWorkMemContext)
        ereport(FATAL, (errmodule(MOD_UDF), errmsg("create UDFWorkMemContext failed")));

    /* Step 1: create UDF hash table */
    UDFCreateHashTab();

    MemoryContextSwitchTo(UDFWorkMemContext);
    FunctionCallInfoData fcinfo;
    while (1) {
        /* Step 2: Receive UDF information */
        RecvUDFInformation(socket, &fcinfo);

        /* Step 3: Set memory limit */
        if (!hasSetMemLimit) {
            SetUDFMemoryLimit();
            hasSetMemLimit = true;
        }

        MemoryContext current_context = CurrentMemoryContext;
        PG_TRY();
        {
            /* Step 4: Run and Return result */
            resetStringInfo(fcinfo.udfInfo.udfMsgBuf);
            Reserve4BytesMsgHeader(fcinfo.udfInfo.udfMsgBuf);
            short msgType = (short)UDF_RESULT;
            AppendBufFixedMsgVal(fcinfo.udfInfo.udfMsgBuf, (char*)&msgType, sizeof(msgType));

            for (int row = 0; row < fcinfo.udfInfo.argBatchRows; ++row) {
                for (int idx = 0; idx < fcinfo.nargs; idx++) {
                    fcinfo.arg[idx] = fcinfo.udfInfo.arg[row][idx];
                    fcinfo.argnull[idx] = fcinfo.udfInfo.null[row][idx];
                }
                Datum result = 0;
                switch (fcinfo.flinfo->fn_languageId) {
                    case ClanguageId:
                        result = RunUDF<C_UDF>(&fcinfo);
                        break;
                    case JavalanguageId:
                        result = RunUDF<JAVA_UDF>(&fcinfo);
                        break;
                    default:
                        if(fcinfo.flinfo->fnLibPath != NULL) {
                            changeDatabase(atoi(fcinfo.flinfo->fnLibPath));
                        }
                        result = RunUDF<PYTHON_UDF>(&fcinfo);
                        break;
                }
                (fcinfo.udfInfo.UDFResultHandlerPtr)(&fcinfo, 0, result);
            }

            Fill4BytesMsgHeader(fcinfo.udfInfo.udfMsgBuf);
            SendMsg(socket, fcinfo.udfInfo.udfMsgBuf->data, fcinfo.udfInfo.udfMsgBuf->len);
        }
        PG_CATCH();
        {
            /* Meet error, we parse the error message and send back to client */
            resetStringInfo(fcinfo.udfInfo.udfMsgBuf);
            Reserve4BytesMsgHeader(fcinfo.udfInfo.udfMsgBuf);
            short msgType = (short)UDF_ERROR;
            AppendBufFixedMsgVal(fcinfo.udfInfo.udfMsgBuf, (char*)&msgType, sizeof(msgType));

            MemoryContext ecxt = MemoryContextSwitchTo(current_context);

            ErrorData* edata = CopyErrorData();

            int len = strlen(edata->message);
            AppendBufVarMsgVal(fcinfo.udfInfo.udfMsgBuf, (char*)edata->message, len);

            Fill4BytesMsgHeader(fcinfo.udfInfo.udfMsgBuf);
            SendMsg(socket, fcinfo.udfInfo.udfMsgBuf->data, fcinfo.udfInfo.udfMsgBuf->len);

            MemoryContextSwitchTo(ecxt);
            PG_RE_THROW();
        }
        PG_END_TRY();

        /* Step 4: Reset memory context */
        MemoryContextReset(UDFWorkMemContext);
    }
}

/*
 * -------------------------------------------------------------------
 *			Argument format and result msg format
 * ------------------|------------------------------------------------
 *		null flag	|	value
 * -------------------like varchar type-------------------------------
 *		1 byte      |   length			    |	string value
 * -------------like fixed length (int32, int64) type-----------------
 * 		1 byte		|	8 bytes value		|
 * -------------like fixed length ( > 8 bytes) type-------------------
 * 		1 byte      |   fixed bytes values  |
 * -------------------------------------------------------------------
 */
template <Oid type, UDFArgHandlerType handlerType>
Datum UDFArgumentHandler(FunctionCallInfoData* fcinfo, int idx, Datum val)
{
    StringInfo udfMsgBuf = fcinfo->udfInfo.udfMsgBuf;
    Datum result = val;
    int valLen = 0;
    char isNull = 0;
    char* readPtr = fcinfo->udfInfo.msgReadPtr;

    switch (type) {
        case BOOLOID:
        case INT8OID:
        case INT2OID:
        case INT4OID:
        case OIDOID:
        case FLOAT4OID:
        case FLOAT8OID:
        case INT1OID:
        case DATEOID:
        case TIMEOID:
        case TIMESTAMPOID:
        case SMALLDATETIMEOID:
        /* typlen: 8 */
        case CASHOID:
        /* typlen: 8 */
        case TIMESTAMPTZOID:
        /* typlen: 1 */
        case CHAROID:
        /* typlen: 4 */
        case REGPROCOID:
        /* typlen: 6 */
        case TIDOID:
        /* typlen: 4 */
        case ABSTIMEOID:
        /* typlen: 4 */
        case RELTIMEOID: {
            if (handlerType == UDF_SEND_ARGS) {
                Datum v = fcinfo->arg[idx];
                isNull = fcinfo->argnull[idx] ? 1 : 0;
                AppendBufFixedMsgVal(udfMsgBuf, (char*)&isNull, sizeof(isNull));
                if (!isNull) {
                    AppendBufFixedMsgVal(udfMsgBuf, (char*)&v, sizeof(v));
                }
            } else if (handlerType == UDF_RECV_ARGS) {
                /* is NULL ? */
                GetFixedMsgVal(readPtr, (char*)&isNull, sizeof(isNull), sizeof(isNull));
                if (!isNull) {
                    /* Keep Always Send Datum, So Recv Datum */
                    Datum value = 0;
                    GetFixedMsgVal(readPtr, (char*)&value, sizeof(value), sizeof(value));
                    fcinfo->arg[idx] = value;
                    fcinfo->argnull[idx] = false;
                } else {
                    fcinfo->argnull[idx] = true;
                }
            } else if (handlerType == UDF_SEND_RESULT) {
                valLen = 0;
                isNull = fcinfo->isnull ? 1 : 0;
                AppendBufFixedMsgVal(udfMsgBuf, (char*)&isNull, sizeof(isNull));
                if (isNull == 0) {
                    AppendBufFixedMsgVal(udfMsgBuf, (char*)&result, sizeof(result));
                }
            } else {
                Assert(handlerType == UDF_RECV_RESULT);
                if (handlerType != UDF_RECV_RESULT) {
                    ereport(ERROR,
                        (errmodule(MOD_UDF),
                            errcode(ERRORCODE_ASSERT_FAILED),
                            errmsg("Otherwise, handlerType can only be UDF_RECV_RESULT")));
                }

                /* Get null flag */
                GetFixedMsgVal(readPtr, (char*)&isNull, sizeof(isNull), sizeof(isNull));

                /* Get result if not null */
                if (isNull == 0) {
                    GetFixedMsgVal(readPtr, (char*)&result, sizeof(result), sizeof(result));
                    fcinfo->isnull = false;
                } else {
                    fcinfo->isnull = true;
                }
            }
            break;
        }
        case BOOLARRAYOID:
        case INT2ARRAYOID:
        case INT4ARRAYOID:
        case INT8ARRAYOID:
        case FLOAT4ARRAYOID:
        case FLOAT8ARRAYOID:
        case CHARARRAYOID:
        case VARCHARARRAYOID:
        case TEXTARRAYOID:
        case NAMEARRAYOID:
        case BYTEARRAYOID:
        case DATEARRAYOID:
        case TIMEARRAYOID:
        case ARRAYTIMETZOID:
        case TIMESTAMPARRAYOID:
        case TIMESTAMPTZARRAYOID: {
            /* C UDF dose not support array and void type arguments */
            if (fcinfo->flinfo->fn_languageId == ClanguageId)
                goto HANDLE_UNSOPPORTED_VAL;
        }
        /* fall through */
        case BPCHAROID:
        case VARCHAROID:
        case NUMERICOID:
        case BYTEAOID:
        case INT2VECTOROID:
        case TEXTOID:
        case OIDVECTOROID:
        case PATHOID:
        case POLYGONOID:
        case UNKNOWNOID:
        case INETOID:
        case CIDROID:
        case VARBITOID: {
            if (handlerType == UDF_SEND_ARGS) {
                Datum v = fcinfo->arg[idx];
                isNull = fcinfo->argnull[idx] ? 1 : 0;
                AppendBufFixedMsgVal(udfMsgBuf, (char*)&isNull, sizeof(isNull));

                if (!isNull) {
                    struct varlena* value = PG_DETOAST_DATUM(v);
                    valLen = VARSIZE_ANY(value);
                    AppendBufVarMsgVal(udfMsgBuf, (char*)value, valLen);
                }
            } else if (handlerType == UDF_RECV_ARGS) {
                valLen = 0;
                GetFixedMsgVal(readPtr, (char*)&isNull, sizeof(isNull), sizeof(isNull));
                if (!isNull) {
                    GetFixedMsgVal(readPtr, (char*)&valLen, sizeof(valLen), sizeof(valLen));
                    if (valLen < 0) {
                        ereport(ERROR,
                            (errmodule(MOD_UDF), 
                                errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
                                errmsg("Variable length %d cannot be negative", valLen)));
                    }
                    char* buffer = (char*)palloc0(valLen + 1);
                    GetFixedMsgVal(readPtr, buffer, valLen, valLen + 1);

                    /* memory should free */
                    fcinfo->arg[idx] = PointerGetDatum(buffer);
                    fcinfo->argnull[idx] = false;
                } else {
                    fcinfo->argnull[idx] = true;
                }
            } else if (handlerType == UDF_SEND_RESULT) {
                valLen = 0;
                varlena* resultVar = NULL;
                isNull = fcinfo->isnull ? 1 : 0;
                AppendBufFixedMsgVal(udfMsgBuf, (char*)&isNull, sizeof(isNull));
                if (!isNull) {
                    resultVar = PG_DETOAST_DATUM(result);
                    valLen = VARSIZE_ANY(resultVar);
                    AppendBufVarMsgVal(udfMsgBuf, (char*)resultVar, valLen);
                }
            } else {
                Assert(handlerType == UDF_RECV_RESULT);
                if (handlerType != UDF_RECV_RESULT) {
                    ereport(ERROR,
                        (errmodule(MOD_UDF),
                            errcode(ERRORCODE_ASSERT_FAILED),
                            errmsg("Otherwise, handlerType can only be UDF_RECV_RESULT")));
                }

                /* Get null falg */
                GetFixedMsgVal(readPtr, (char*)&isNull, sizeof(isNull), sizeof(isNull));

                /* Get result if not null */
                if (isNull == 0) {
                    valLen = 0;
                    GetFixedMsgVal(readPtr, (char*)&valLen, sizeof(valLen), sizeof(valLen));
                    if (valLen < 0) {
                        ereport(ERROR,
                            (errmodule(MOD_UDF), 
                                errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
                                errmsg("Variable length %d cannot be negative", valLen)));
                    }
                    char* ptr = (char*)palloc0(valLen + 1);
                    GetFixedMsgVal(readPtr, (char*)ptr, valLen, valLen + 1);
                    result = PointerGetDatum(ptr);
                    fcinfo->isnull = false;
                } else {
                    fcinfo->isnull = true;
                }
            }
            break;
        }
        case VOIDOID: {
            valLen = 0;
            fcinfo->isnull = true;
            break;
        }
        /* typlen: 16 */
        case INTERVALOID: {
            valLen = 16;
            goto HANDLE_BIG_FIXED_VAL;
        }
        /* typlen: 12 */
        case TIMETZOID: {
            valLen = 12;
            goto HANDLE_BIG_FIXED_VAL;
        }
        /* typlen: 64 */
        case NAMEOID: {
            valLen = 64;
            goto HANDLE_BIG_FIXED_VAL;
        }
        /* typlen: 28 */
        case XIDOID: {
            valLen = 28;
            goto HANDLE_BIG_FIXED_VAL;
        }
        /* typlen: 29 */
        case CIDOID: {
            valLen = 29;
            goto HANDLE_BIG_FIXED_VAL;
        }
        /* typlen: 16 */
        case POINTOID: {
            valLen = 16;
            goto HANDLE_BIG_FIXED_VAL;
        }
        /* typlen: 32 */
        case LSEGOID: {
            valLen = 32;
            goto HANDLE_BIG_FIXED_VAL;
        }
        /* typlen: 32 */
        case BOXOID: {
            valLen = 32;
            goto HANDLE_BIG_FIXED_VAL;
        }
        /* typlen: 32 */
        case LINEOID: {
            valLen = 32;
            goto HANDLE_BIG_FIXED_VAL;
        }
        /* typlen: 12 */
        case TINTERVALOID: {
            valLen = 12;
            goto HANDLE_BIG_FIXED_VAL;
        }
        /* typlen: 24 */
        case CIRCLEOID: {
            valLen = 24;

        HANDLE_BIG_FIXED_VAL:
            if (handlerType == UDF_SEND_ARGS) {
                Datum v = fcinfo->arg[idx];
                isNull = fcinfo->argnull[idx] ? 1 : 0;
                AppendBufFixedMsgVal(udfMsgBuf, (char*)&isNull, sizeof(isNull));
                if (!isNull) {
                    char* value = DatumGetPointer(v);
                    AppendBufVarMsgVal(udfMsgBuf, (char*)value, valLen);
                }
            } else if (handlerType == UDF_RECV_ARGS) {
                GetFixedMsgVal(readPtr, (char*)&isNull, sizeof(isNull), sizeof(isNull));
                if (!isNull) {
                    GetFixedMsgVal(readPtr, (char*)&valLen, sizeof(valLen), sizeof(valLen));
                    if (valLen < 0) {
                        ereport(ERROR,
                            (errmodule(MOD_UDF), 
                                errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
                                errmsg("Variable length %d cannot be negative", valLen)));
                    }
                    char* buffer = (char*)palloc0(valLen + 1);
                    GetFixedMsgVal(readPtr, buffer, valLen, valLen + 1);
                    fcinfo->argnull[idx] = false;
                    /* memory should free */
                    fcinfo->arg[idx] = PointerGetDatum(buffer);
                } else {
                    fcinfo->argnull[idx] = true;
                }
            } else if (handlerType == UDF_SEND_RESULT) {
                char* resultVar = NULL;
                char isNull = fcinfo->isnull ? 1 : 0;

                /* Fill NULL flag 1 byte */
                AppendBufFixedMsgVal(udfMsgBuf, (char*)&isNull, sizeof(isNull));

                /* Fill fixed-length value if not null */
                if (!isNull) {
                    resultVar = DatumGetPointer(result);
                    AppendBufFixedMsgVal(udfMsgBuf, (char*)resultVar, valLen);
                }
            } else {
                Assert(handlerType == UDF_RECV_RESULT);
                if (handlerType != UDF_RECV_RESULT) {
                    ereport(ERROR,
                        (errmodule(MOD_UDF),
                            errcode(ERRORCODE_ASSERT_FAILED),
                            errmsg("Otherwise, handlerType can only be UDF_RECV_RESULT")));
                }
                /* Get null flag */
                GetFixedMsgVal(readPtr, (char*)&isNull, sizeof(isNull), sizeof(isNull));

                /* Get result if not null */
                if (isNull == 0) {
                    char* ptr = (char*)palloc0(valLen + 1);
                    GetFixedMsgVal(readPtr, (char*)ptr, valLen, valLen + 1);
                    result = PointerGetDatum(ptr);
                    fcinfo->isnull = false;
                } else {
                    fcinfo->isnull = true;
                }
            }
            break;
        }
        default: {
        HANDLE_UNSOPPORTED_VAL:
            ereport(ERROR,
                (errmodule(MOD_UDF),
                    errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Recv Unsupported argument type: %u", type)));
            break;
        }
    }
    /* Attention: We must save the current read cursor */
    if (UDF_RECV_ARGS == handlerType || UDF_RECV_RESULT == handlerType)
        fcinfo->udfInfo.msgReadPtr = readPtr;
    return result;
}

template <UDFArgHandlerType handlerType>
void InitUDFArgsHandler(FunctionCallInfo fcinfo)
{
    int n = 1;
    if (handlerType == UDF_SEND_ARGS || handlerType == UDF_RECV_ARGS)
        n = fcinfo->nargs;

    for (int i = 0; i < n; ++i) {
        Oid type = 0;
        UDFArgsFuncType* funcPPtr = NULL;
        if (handlerType == UDF_SEND_RESULT || handlerType == UDF_RECV_RESULT) {
            type = fcinfo->flinfo->fn_rettype;
            funcPPtr = &fcinfo->udfInfo.UDFResultHandlerPtr;
        } else {
            AssertEreport(handlerType == UDF_SEND_ARGS || handlerType == UDF_RECV_ARGS,
                MOD_UDF,
                "Otherwise, handlerType can only be UDF_SEND_ARGS or UDF_RECV_ARGS");

            type = fcinfo->argTypes[i];
            funcPPtr = fcinfo->udfInfo.UDFArgsHandlerPtr + i;
        }
        switch (type) {
            case BOOLOID: {
                *funcPPtr = UDFArgumentHandler<BOOLOID, handlerType>;
                break;
            }
            case INT8OID: {
                *funcPPtr = UDFArgumentHandler<INT8OID, handlerType>;
                break;
            }
            case INT2OID: {
                *funcPPtr = UDFArgumentHandler<INT2OID, handlerType>;
                break;
            }
            case INT4OID: {
                *funcPPtr = UDFArgumentHandler<INT4OID, handlerType>;
                break;
            }
            case OIDOID: {
                *funcPPtr = UDFArgumentHandler<OIDOID, handlerType>;
                break;
            }
            case FLOAT4OID: {
                *funcPPtr = UDFArgumentHandler<FLOAT4OID, handlerType>;
                break;
            }
            case FLOAT8OID: {
                *funcPPtr = UDFArgumentHandler<FLOAT8OID, handlerType>;
                break;
            }
            case INT1OID: {
                *funcPPtr = UDFArgumentHandler<INT1OID, handlerType>;
                break;
            }
            case DATEOID: {
                *funcPPtr = UDFArgumentHandler<DATEOID, handlerType>;
                break;
            }
            case TIMEOID: {
                *funcPPtr = UDFArgumentHandler<TIMEOID, handlerType>;
                break;
            }
            case TIMESTAMPOID: {
                *funcPPtr = UDFArgumentHandler<TIMESTAMPOID, handlerType>;
                break;
            }
            case SMALLDATETIMEOID: {
                *funcPPtr = UDFArgumentHandler<SMALLDATETIMEOID, handlerType>;
                break;
            }
            case BPCHAROID: {
                *funcPPtr = UDFArgumentHandler<BPCHAROID, handlerType>;
                break;
            }
            case VARCHAROID: {
                *funcPPtr = UDFArgumentHandler<VARCHAROID, handlerType>;
                break;
            }
            case TIMESTAMPTZOID: {
                *funcPPtr = UDFArgumentHandler<TIMESTAMPTZOID, handlerType>;
                break;
            }
            case INTERVALOID: {
                *funcPPtr = UDFArgumentHandler<INTERVALOID, handlerType>;
                break;
            }
            case TIMETZOID: {
                *funcPPtr = UDFArgumentHandler<TIMETZOID, handlerType>;
                break;
            }
            case NUMERICOID: {
                *funcPPtr = UDFArgumentHandler<NUMERICOID, handlerType>;
                break;
            }
            case BYTEAOID: {
                *funcPPtr = UDFArgumentHandler<BYTEAOID, handlerType>;
                break;
            }
            case CHAROID: {
                *funcPPtr = UDFArgumentHandler<CHAROID, handlerType>;
                break;
            }
            case NAMEOID: {
                *funcPPtr = UDFArgumentHandler<NAMEOID, handlerType>;
                break;
            }
            case INT2VECTOROID: {
                *funcPPtr = UDFArgumentHandler<INT2VECTOROID, handlerType>;
                break;
            }
            case REGPROCOID: {
                *funcPPtr = UDFArgumentHandler<REGPROCOID, handlerType>;
                break;
            }
            case TEXTOID: {
                *funcPPtr = UDFArgumentHandler<TEXTOID, handlerType>;
                break;
            }
            case TIDOID: {
                *funcPPtr = UDFArgumentHandler<TIDOID, handlerType>;
                break;
            }
            case XIDOID: {
                *funcPPtr = UDFArgumentHandler<XIDOID, handlerType>;
                break;
            }
            case CIDOID: {
                *funcPPtr = UDFArgumentHandler<CIDOID, handlerType>;
                break;
            }
            case OIDVECTOROID: {
                *funcPPtr = UDFArgumentHandler<OIDVECTOROID, handlerType>;
                break;
            }
            case POINTOID: {
                *funcPPtr = UDFArgumentHandler<POINTOID, handlerType>;
                break;
            }
            case LSEGOID: {
                *funcPPtr = UDFArgumentHandler<LSEGOID, handlerType>;
                break;
            }
            case PATHOID: {
                *funcPPtr = UDFArgumentHandler<PATHOID, handlerType>;
                break;
            }
            case BOXOID: {
                *funcPPtr = UDFArgumentHandler<BOXOID, handlerType>;
                break;
            }
            case POLYGONOID: {
                *funcPPtr = UDFArgumentHandler<POLYGONOID, handlerType>;
                break;
            }
            case LINEOID: {
                *funcPPtr = UDFArgumentHandler<LINEOID, handlerType>;
                break;
            }
            case ABSTIMEOID: {
                *funcPPtr = UDFArgumentHandler<ABSTIMEOID, handlerType>;
                break;
            }
            case RELTIMEOID: {
                *funcPPtr = UDFArgumentHandler<RELTIMEOID, handlerType>;
                break;
            }
            case TINTERVALOID: {
                *funcPPtr = UDFArgumentHandler<TINTERVALOID, handlerType>;
                break;
            }
            case UNKNOWNOID: {
                *funcPPtr = UDFArgumentHandler<UNKNOWNOID, handlerType>;
                break;
            }
            case CIRCLEOID: {
                *funcPPtr = UDFArgumentHandler<CIRCLEOID, handlerType>;
                break;
            }
            case CASHOID: {
                *funcPPtr = UDFArgumentHandler<CASHOID, handlerType>;
                break;
            }
            case INETOID: {
                *funcPPtr = UDFArgumentHandler<INETOID, handlerType>;
                break;
            }
            case CIDROID: {
                *funcPPtr = UDFArgumentHandler<CIDROID, handlerType>;
                break;
            }
            case VARBITOID: {
                *funcPPtr = UDFArgumentHandler<VARBITOID, handlerType>;
                break;
            }
            case VOIDOID: {
                *funcPPtr = UDFArgumentHandler<VOIDOID, handlerType>;
                break;
            }
            case BOOLARRAYOID: {
                *funcPPtr = UDFArgumentHandler<BOOLARRAYOID, handlerType>;
                break;
            }
            case INT2ARRAYOID: {
                *funcPPtr = UDFArgumentHandler<INT2ARRAYOID, handlerType>;
                break;
            }
            case INT4ARRAYOID: {
                *funcPPtr = UDFArgumentHandler<INT4ARRAYOID, handlerType>;
                break;
            }
            case INT8ARRAYOID: {
                *funcPPtr = UDFArgumentHandler<INT8ARRAYOID, handlerType>;
                break;
            }
            case FLOAT4ARRAYOID: {
                *funcPPtr = UDFArgumentHandler<FLOAT4ARRAYOID, handlerType>;
                break;
            }
            case FLOAT8ARRAYOID: {
                *funcPPtr = UDFArgumentHandler<FLOAT8ARRAYOID, handlerType>;
                break;
            }
            case CHARARRAYOID: {
                *funcPPtr = UDFArgumentHandler<CHARARRAYOID, handlerType>;
                break;
            }
            case VARCHARARRAYOID: {
                *funcPPtr = UDFArgumentHandler<VARCHARARRAYOID, handlerType>;
                break;
            }
            case TEXTARRAYOID: {
                *funcPPtr = UDFArgumentHandler<TEXTARRAYOID, handlerType>;
                break;
            }
            case NAMEARRAYOID: {
                *funcPPtr = UDFArgumentHandler<NAMEARRAYOID, handlerType>;
                break;
            }
            case BYTEARRAYOID: {
                *funcPPtr = UDFArgumentHandler<BYTEARRAYOID, handlerType>;
                break;
            }
            case DATEARRAYOID: {
                *funcPPtr = UDFArgumentHandler<DATEARRAYOID, handlerType>;
                break;
            }
            case TIMEARRAYOID: {
                *funcPPtr = UDFArgumentHandler<TIMEARRAYOID, handlerType>;
                break;
            }
            case ARRAYTIMETZOID: {
                *funcPPtr = UDFArgumentHandler<ARRAYTIMETZOID, handlerType>;
                break;
            }
            case TIMESTAMPARRAYOID: {
                *funcPPtr = UDFArgumentHandler<TIMESTAMPARRAYOID, handlerType>;
                break;
            }
            case TIMESTAMPTZARRAYOID: {
                *funcPPtr = UDFArgumentHandler<TIMESTAMPTZARRAYOID, handlerType>;
                break;
            }
            default: {
                ereport(ERROR,
                    (errmodule(MOD_UDF), errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("Unsupport type: %u", type)));
                break;
            }
        }
    }
}

static void FillBasicUDFInformation(FunctionCallInfo fcinfo)
{
    StringInfo udfMsgBuf = fcinfo->udfInfo.udfMsgBuf;
    int len = 0;
    short msgType = (short)UDF_INFO;
    Oid resultType;

    if (fcinfo->flinfo->fn_oid == lastUDFOid) {
        msgType = (short)UDF_ARGS;
        Oid oid = fcinfo->flinfo->fn_oid;

        AppendBufFixedMsgVal(udfMsgBuf, (char*)&msgType, sizeof(msgType));
        AppendBufFixedMsgVal(udfMsgBuf, (char*)&oid, sizeof(oid));
    } else {
        /* Message Type */
        msgType = (short)UDF_INFO;
        AppendBufFixedMsgVal(udfMsgBuf, (char*)&msgType, sizeof(msgType));

        /* memory limit of udf worker process */
        AppendBufFixedMsgVal(udfMsgBuf,
            (char*)&u_sess->attr.attr_sql.FencedUDFMemoryLimit,
            sizeof(u_sess->attr.attr_sql.FencedUDFMemoryLimit));

        /* pljava_vmoptions */
        len = strlen(u_sess->attr.attr_sql.pljava_vmoptions);
        AppendBufVarMsgVal(udfMsgBuf, (char*)u_sess->attr.attr_sql.pljava_vmoptions, len);

        /* UDF OID */
        AppendBufFixedMsgVal(udfMsgBuf, (char*)&fcinfo->flinfo->fn_oid, sizeof(Oid));

        /* UDF language OID */
        AppendBufFixedMsgVal(udfMsgBuf, (char*)&fcinfo->flinfo->fn_languageId, sizeof(Oid));

        /* UDF volatile property */
        AppendBufFixedMsgVal(udfMsgBuf, (char*)&fcinfo->flinfo->fn_volatile, sizeof(char));

        /* UDF Name */
        len = strlen(fcinfo->flinfo->fnName);
        AppendBufVarMsgVal(udfMsgBuf, (char*)fcinfo->flinfo->fnName, len);

        /* UDF Library */
        len = strlen(fcinfo->flinfo->fnLibPath);
        AppendBufVarMsgVal(udfMsgBuf, (char*)fcinfo->flinfo->fnLibPath, len);

        /* UDF Number of Argument */
        AppendBufFixedMsgVal(udfMsgBuf, (char*)&fcinfo->nargs, sizeof(fcinfo->nargs));

        if (fcinfo->nargs > 0) {
            /* the type of arguments */
            Oid* argTypes = fcinfo->argTypes;
            AppendBufFixedMsgVal(udfMsgBuf, (char*)argTypes, sizeof(Oid) * fcinfo->nargs);
        }

        /* The type of result */
        resultType = fcinfo->flinfo->fn_rettype;
        AppendBufFixedMsgVal(udfMsgBuf, (char*)&resultType, sizeof(resultType));
    }
}

template <bool batchMode>
static void FillUDFArguments(FunctionCallInfo fcinfo)
{
    StringInfo udfMsgBuf = fcinfo->udfInfo.udfMsgBuf;
    int batchRows = batchMode ? fcinfo->udfInfo.argBatchRows : 1;
    AppendBufFixedMsgVal(udfMsgBuf, (char*)&batchRows, sizeof(batchRows));

    if (unlikely(!fcinfo->udfInfo.valid_UDFArgsHandlerPtr)) {
        InitUDFArgsHandler<UDF_SEND_ARGS>(fcinfo);
        InitUDFArgsHandler<UDF_RECV_RESULT>(fcinfo);
        fcinfo->udfInfo.valid_UDFArgsHandlerPtr = true;
    }
    /* Fill UDF Argument Values */
    for (int row = 0; row < batchRows; row++) {
        for (int idx = 0; idx < fcinfo->nargs; idx++) {
            if (batchMode) {
                fcinfo->arg[idx] = fcinfo->udfInfo.arg[row][idx];
                fcinfo->argnull[idx] = fcinfo->udfInfo.null[row][idx];
            }
            (*(fcinfo->udfInfo.UDFArgsHandlerPtr + idx))(fcinfo, idx, 0);
        }
    }
}

template <bool batchMode>
static void SendUDFInformation(FunctionCallInfo fcinfo)
{
    StringInfo udfMsgBuf = fcinfo->udfInfo.udfMsgBuf;

    /* Step 1: Initialize and Reset the udfMsgBuf */
    resetStringInfo(fcinfo->udfInfo.udfMsgBuf);
    Reserve4BytesMsgHeader(udfMsgBuf);

    /*
     * Step 2: Fill the basic UDF information (OID, NAME,
     * LIBPATH and so on)
     */
    FillBasicUDFInformation(fcinfo);

    /*
     * Step 3: Fill UDF arguments
     * the number of batch argument values
     */
    FillUDFArguments<batchMode>(fcinfo);

    /*
     * Step 4: Fill the length of body msg and send msg
     */
    Fill4BytesMsgHeader(udfMsgBuf);
    SendMsg(UDFRPCSocket, udfMsgBuf->data, udfMsgBuf->len);

    lastUDFOid = fcinfo->flinfo->fn_oid;
}

bool RPCInitFencedUDFIfNeed(Oid functionId, FmgrInfo* finfo, HeapTuple procedureTuple)
{
    Datum prosrcattr;
    Datum probinattr;
    char* udfName = NULL;
    char* udfLibPath = NULL;
    bool isnull = false;
    Oid languageId = finfo->fn_languageId;
    bool fencedMode = false;

    Datum procFenced = SysCacheGetAttr(PROCOID, procedureTuple, Anum_pg_proc_fenced, &isnull);
    fencedMode = !isnull && DatumGetBool(procFenced);

    probinattr = SysCacheGetAttr(PROCOID, procedureTuple, Anum_pg_proc_probin, &isnull);

    if (languageId == ClanguageId && isnull)
        ereport(ERROR,
            (errmodule(MOD_UDF),
                errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("null probin for C function %u", functionId)));

    if (fencedMode) {
        if (languageId == ClanguageId) {
            /*
             * Get prosrc and probin strings (link symbol and library filename).
             * While in general these columns might be null, that's not allowed
             * for C-language functions.
             */
            prosrcattr = SysCacheGetAttr(PROCOID, procedureTuple, Anum_pg_proc_prosrc, &isnull);

            if (isnull)
                ereport(ERROR,
                    (errmodule(MOD_UDF),
                        errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("null prosrc for C function %u", functionId)));
            udfName = TextDatumGetCString(prosrcattr);

            udfLibPath = TextDatumGetCString(probinattr);
            char* udfLibFullPath = expand_dynamic_library_name(udfLibPath);
            errno_t errorno = memcpy_s(finfo->fnName, sizeof(finfo->fnName), udfName, strlen(udfName) + 1);
            securec_check_c(errorno, "\0", "\0");

            finfo->fnLibPath = (char*)MemoryContextAllocZero(finfo->fn_mcxt, strlen(udfLibFullPath) + 1);
            errorno = memcpy_s(finfo->fnLibPath, strlen(udfLibFullPath) + 1, udfLibFullPath, strlen(udfLibFullPath));
            securec_check_c(errorno, "\0", "\0");
            pfree_ext(udfLibFullPath);
        } else {
            /*
             * Get prosrc strings (package_name.class_name.method_name(type_list) )
             * While in general these colums might be null, that's not allowed
             * for Java-language functions.
             */
            prosrcattr = SysCacheGetAttr(PROCOID, procedureTuple, Anum_pg_proc_prosrc, &isnull);
            if (isnull)
                ereport(ERROR,
                    (errmodule(MOD_UDF),
                        errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("null prosrc for Java function %u", functionId)));
            if (languageId == JavalanguageId) {
                udfLibPath = TextDatumGetCString(prosrcattr);
            } else {
                const int dbinLen = 10;
                udfLibPath = (char *)palloc0(dbinLen * sizeof(char));
                pg_itoa(u_sess->proc_cxt.MyDatabaseId, udfLibPath);
            }
            finfo->fnLibPath = (char*)MemoryContextAllocZero(finfo->fn_mcxt, strlen(udfLibPath) + 1);
            errno_t errorno = memcpy_s(finfo->fnLibPath, strlen(udfLibPath) + 1, udfLibPath, strlen(udfLibPath));
            securec_check_c(errorno, "\0", "\0");
        }

        finfo->fn_addr = RPCFencedUDF<false>;
        finfo->fn_fenced = true;
    }
    finfo->fn_languageId = languageId;
    return fencedMode;
}

/*
 * @Description: The client for calling udf RPC
 * The implementation is to build connection and then send UDF information.
 * It will send argument and recveive result.
 * @IN fcinfo: UDF function information
 */
template <bool batchMode>
Datum RPCFencedUDF(FunctionCallInfo fcinfo)
{
    Datum result = 0;
    /* Step 1: Build UDF RPC connection if need */
    if (unlikely((-1 == UDFRPCSocket))) {
        struct sockaddr_un addrUnix;
        errno_t rc = memset_s((void*)&addrUnix, sizeof(addrUnix), 0, sizeof(addrUnix));
        securec_check(rc, "\0", "\0");

        addrUnix.sun_family = AF_UNIX;
        SetUDFUnixSocketPath(&addrUnix);

        UDFRPCSocket = socket(AF_UNIX, SOCK_STREAM, 0);
        if (UDFRPCSocket < 0) {
            ereport(
                ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_CONNECTION_FAILURE), errmsg("create socket failed: %m")));
        }

        if (connect(UDFRPCSocket, (struct sockaddr*)&addrUnix, sizeof(addrUnix)) < 0) {
            ereport(ERROR,
                (errmodule(MOD_UDF), errcode(ERRCODE_CONNECTION_FAILURE), errmsg("Run udf RPC connect failed: %m")));
        }
    }

    /* Step 2: Send UDF information: name, library path, arguments */
    SendUDFInformation<batchMode>(fcinfo);

    /* Step 3: Get UDF result */
    int len = 0;
    resetStringInfo(fcinfo->udfInfo.udfMsgBuf);
    RecvMsg(UDFRPCSocket, (char*)&len, sizeof(len));    
    if (len < 0) {
        ereport(ERROR,
            (errmodule(MOD_UDF), 
                errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
                errmsg("Variable length %d cannot be negative", len)));
    }
    char* ptr = (char*)palloc(len + 1);
    RecvMsg(UDFRPCSocket, ptr, len);
    AppendBufFixedMsgVal(fcinfo->udfInfo.udfMsgBuf, ptr, len);
    fcinfo->udfInfo.msgReadPtr = fcinfo->udfInfo.udfMsgBuf->data;
    pfree_ext(ptr);

    /* Get Message type */
    short msgType;
    GetFixedMsgVal(fcinfo->udfInfo.msgReadPtr, (char*)&msgType, sizeof(msgType), sizeof(msgType));

    if (msgType == UDF_RESULT) {
        int batchRows = batchMode ? fcinfo->udfInfo.argBatchRows : 1;
        for (int row = 0; row < batchRows; ++row) {
            result = (fcinfo->udfInfo.UDFResultHandlerPtr)(fcinfo, 0, 0);
            if (batchMode) {
                fcinfo->udfInfo.result[row] = result;
                fcinfo->udfInfo.resultIsNull[row] = fcinfo->isnull;
            }
        }
    } else if (msgType == UDF_ERROR) {
        int valLen = 0;
        GetFixedMsgVal(fcinfo->udfInfo.msgReadPtr, (char*)&valLen, sizeof(valLen), sizeof(valLen));        
        if (valLen < 0) {
            ereport(ERROR,
                (errmodule(MOD_UDF), 
                    errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
                    errmsg("Variable length %d cannot be negative", valLen)));
        }
        char* errMsg = (char*)palloc0(valLen + 1);
        GetFixedMsgVal(fcinfo->udfInfo.msgReadPtr, (char*)errMsg, valLen, valLen + 1);
        ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_DATA_EXCEPTION), errmsg("UDF Error:%s", errMsg)));
    }
    return result;
}

static void UDFCreateHashTab()
{
    if (NULL == UDFFuncHash) {
        HASHCTL hash_ctl;

        errno_t errorno = EOK;
        errorno = memset_s(&hash_ctl, sizeof(HASHCTL), 0, sizeof(hash_ctl));
        securec_check(errorno, "\0", "\0");
        hash_ctl.keysize = sizeof(Oid);
        hash_ctl.entrysize = sizeof(UDFFuncHashTabEntry);
        hash_ctl.hash = oid_hash;
        UDFFuncHash = hash_create("UDFFuncHash", 100, &hash_ctl, HASH_ELEM | HASH_FUNCTION);
    }
}

static void FindOrInsertUDFHashTab(FunctionCallInfoData* fcinfo)
{
    AssertEreport(UDFFuncHash, MOD_UDF, "Hash table is not initialized in UDF.");

    Oid oid = fcinfo->flinfo->fn_oid;
    UDFFuncHashTabEntry* entry = NULL;
    entry = (UDFFuncHashTabEntry*)hash_search(UDFFuncHash, &oid, HASH_FIND, NULL);

    if (NULL != entry) {

        fcinfo->flinfo->fn_languageId = entry->fn_languageId;
        fcinfo->flinfo->fn_addr = entry->user_fn;
        fcinfo->flinfo->fn_nargs = fcinfo->nargs = entry->argNum;

        /* Just make don't initilize udfInfo memory in order to get from entry */
        fcinfo->flinfo->fn_fenced = false;
        InitFunctionCallInfoData(*fcinfo, fcinfo->flinfo, fcinfo->flinfo->fn_nargs, 0, NULL, NULL);
        fcinfo->flinfo->fn_fenced = true;

        fcinfo->udfInfo = entry->udfInfo;
    } else {
        AutoContextSwitch contextSwitcher(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
        entry = (UDFFuncHashTabEntry*)hash_search(UDFFuncHash, &oid, HASH_ENTER, NULL);
        if (NULL != entry) {
            FmgrInfo* flinfo = fcinfo->flinfo;

            /* Load the shared library, unless we already did */
            if (flinfo->fn_languageId == ClanguageId) {
                void* libHandle = internal_load_library(flinfo->fnLibPath);
                if (NULL == libHandle)
                    ereport(ERROR,
                        (errmodule(MOD_UDF),
                            errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                            errmsg("internal_load_library %s failed: %m", flinfo->fnLibPath)));

                /* Look up the function within the library */
                flinfo->fn_addr = (PGFunction)pg_dlsym(libHandle, flinfo->fnName);
            } else if (flinfo->fn_languageId == JavalanguageId){
#ifndef ENABLE_LITE_MODE
                /* Load libpljava.so to support Java UDF */
                char pathbuf[MAXPGPATH];
                get_lib_path(my_exec_path, pathbuf);
                join_path_components(pathbuf, pathbuf, "libpljava.so");
                char* libpljava_location = strdup(pathbuf);
                void* libpljava_handler = internal_load_library(libpljava_location);
                if (NULL == libpljava_handler)
                    ereport(ERROR,
                        (errmodule(MOD_UDF),
                            errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                            errmsg("internal_load_library %s failed: %m", libpljava_location)));
                free(libpljava_location);
                /* Look up the java_call_handler function within libpljava.so */
                Datum (*pljava_call_handler)(FunctionCallInfoData*);
                pljava_call_handler = (Datum(*)(FunctionCallInfoData*))pg_dlsym(libpljava_handler, "java_call_handler");
                if (NULL == pljava_call_handler)
                    ereport(ERROR,
                        (errmodule(MOD_UDF),
                            errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                            errmsg("load java_call_handler failed.")));
                flinfo->fn_addr = pljava_call_handler;
#else
                FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
            } else {
                char pathbuf[MAXPGPATH];
                get_lib_path(my_exec_path, pathbuf);
#ifdef ENABLE_PYTHON2
                join_path_components(pathbuf, pathbuf, "postgresql/plpython2.so");
#else
                join_path_components(pathbuf, pathbuf, "postgresql/plpython3.so");
#endif
                char *libpl_location = strdup(pathbuf);
                void *libpl_handler = internal_load_library(libpl_location);
                if (NULL == libpl_handler)
                    ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                                    errmsg("internal_load_library %s failed: %m", libpl_location)));
                free(libpl_location);
                Datum (*plpython_call_handler)(FunctionCallInfoData *);
#ifdef ENABLE_PYTHON2
                plpython_call_handler = (Datum(*)(FunctionCallInfoData *))pg_dlsym(libpl_handler, "plpython_call_handler");
#else
                plpython_call_handler = 
                            (Datum(*)(FunctionCallInfoData *))pg_dlsym(libpl_handler, "plpython3_call_handler");
#endif
                if (NULL == plpython_call_handler) {
                    ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                                    errmsg("load plpython_call_handler failed.")));
                }
                flinfo->fn_addr = plpython_call_handler;
            }
            entry->user_fn = fcinfo->flinfo->fn_addr;

            InitUDFArgsHandler<UDF_RECV_ARGS>(fcinfo);
            InitUDFArgsHandler<UDF_SEND_RESULT>(fcinfo);

            InitUDFInfo(&entry->udfInfo, fcinfo->nargs, BatchMaxSize);
            for (int i = 0; i < fcinfo->nargs; ++i) {
                entry->udfInfo.UDFArgsHandlerPtr[i] = fcinfo->udfInfo.UDFArgsHandlerPtr[i];
            }
            entry->udfInfo.UDFResultHandlerPtr = fcinfo->udfInfo.UDFResultHandlerPtr;
            entry->udfInfo.valid_UDFArgsHandlerPtr = true;
            entry->argNum = fcinfo->nargs;
            entry->fn_oid = oid;
            entry->fn_languageId = flinfo->fn_languageId;
        }
    }
}

void SIGQUITUDFMaster(SIGNAL_ARGS)
{
    UDFMasterQuitFlag = true;
}

void inline SetUDFUnixSocketPath(struct sockaddr_un* unAddrPtr)
{
    /* Check and set unix domain socket path */
    uint32 socketPathLen = strlen(g_instance.attr.attr_network.UnixSocketDir) + 1 + strlen(FENCED_UDF_UNIXSOCKET) + 1;
    if (socketPathLen > sizeof(unAddrPtr->sun_path))
        ereport(FATAL,
            (errmodule(MOD_UDF),
                errmsg("UnixSocketDir is not valid, length is between 1 and %lu: %s/%s",
                    sizeof(unAddrPtr->sun_path) - sizeof(FENCED_UDF_UNIXSOCKET) - 2,
                    g_instance.attr.attr_network.UnixSocketDir,
                    FENCED_UDF_UNIXSOCKET)));

    errno_t rc = snprintf_s(unAddrPtr->sun_path,
        sizeof(unAddrPtr->sun_path),
        sizeof(unAddrPtr->sun_path) - 1,
        "%s/%s",
        g_instance.attr.attr_network.UnixSocketDir,
        FENCED_UDF_UNIXSOCKET);
    securec_check_ss(rc, "\0", "\0");
}

void InitUDFInfo(UDFInfoType* udfInfo, int argN, int batchRows)
{
    udfInfo->udfMsgBuf = makeStringInfo();
    udfInfo->arg = (Datum**)palloc(sizeof(Datum*) * batchRows);
    udfInfo->null = (bool**)palloc(sizeof(bool*) * batchRows);
    udfInfo->result = (Datum*)palloc(sizeof(Datum) * batchRows);
    udfInfo->resultIsNull = (bool*)palloc(sizeof(bool) * batchRows);
    udfInfo->argBatchRows = 0;
    udfInfo->msgReadPtr = NULL;
    udfInfo->valid_UDFArgsHandlerPtr = false;
    udfInfo->UDFResultHandlerPtr = NULL;
    udfInfo->allocRows = batchRows;

    for (int i = 0; i < batchRows; ++i) {
        udfInfo->arg[i] = (Datum*)palloc(sizeof(Datum) * argN);
        udfInfo->null[i] = (bool*)palloc(sizeof(bool) * argN);
    }
    udfInfo->UDFArgsHandlerPtr = (UDFArgsFuncType*)palloc0((argN) * sizeof(UDFArgsFuncType));
}

void InitFunctionCallUDFArgs(FunctionCallInfoData* fcinfo, int argN, int batchRows)
{
    /* For UDF, FmgrInfo must be valid, if not invalid, don't need init the following */
    if (argN > 0) {
        InitUDFInfo(&fcinfo->udfInfo, argN, batchRows);
    }
}

void InitFuncCallUDFInfo(FunctionCallInfoData* fcinfo, int argN, bool setFuncPtr = true)
{
    if (unlikely(fcinfo->flinfo && fcinfo->flinfo->fn_fenced)) {
        if (setFuncPtr)
            fcinfo->flinfo->fn_addr = RPCFencedUDF<true>;
        if (argN > 0) {
            int batchRows = setFuncPtr ? BatchMaxSize : 1;
            InitFunctionCallUDFArgs(fcinfo, argN, batchRows);
        } else {
            fcinfo->udfInfo.udfMsgBuf = makeStringInfo();
            fcinfo->udfInfo.msgReadPtr = NULL;
        }
    }
}

Datum fenced_udf_process(PG_FUNCTION_ARGS)
{
    int32 arg = PG_GETARG_INT32(0);
    char result[1024] = {0};
    StringInfoData strinfo;
    initStringInfo(&strinfo);

    /* UDF master process */
    if (MASTER_PROCESS == arg) {
        appendStringInfo(&strinfo, "ps ux | grep 'gaussdb fenced UDF master process' | grep -v grep |  wc -l");
    } else if (WORK_PROCESS == arg) {
        /* UDF work process */
        appendStringInfo(&strinfo, "ps ux | grep 'gaussdb fenced UDF worker process' | grep -v grep |  wc -l");
    } else if (KILL_WORK == arg) {
        /* kill UDF work process */
        appendStringInfo(&strinfo,
            "ps ux | grep 'gaussdb fenced UDF worker process' | grep -v grep | awk '{print $2}' | xargs kill -9");
    } else {
        ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid parameter.")));
    }

    FILE* fp = popen(strinfo.data, "r");

    if (fp == NULL) {
        ereport(ERROR,
            (errmodule(MOD_UDF), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("execute cmd %s fail.", strinfo.data)));
    }

    /* Get process num. */
    if (MASTER_PROCESS == arg || WORK_PROCESS == arg) {
        if (fgets(result, sizeof(result), fp) == NULL) {
            pfree_ext(strinfo.data);
            pclose(fp);
            ereport(ERROR,
                (errmodule(MOD_UDF), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("Can not read process num.")));
        }

        int len = strlen(result);
        /* delete '\n' */
        result[len - 1] = '\0';
    }

    pfree_ext(strinfo.data);
    pclose(fp);

    if (MASTER_PROCESS == arg || WORK_PROCESS == arg) {
        PG_RETURN_TEXT_P(cstring_to_text(result));
    } else {
        PG_RETURN_NULL();
    }
}

static char* parse_value(char* p_start)
{
    char* value = NULL;
    char* p_end = p_start;
    // seek for the first whitespace or '\0'
    while (*p_end != '\0' && *p_end != ' ') {
        p_end++;
    }
    if (p_end != p_start) {
        value = (char*)palloc((p_end - p_start + 1) * sizeof(char));
        errno_t errorno = strncpy_s(value, p_end - p_start + 1, p_start, p_end - p_start);
        securec_check_c(errorno, "\0", "\0");
    }
    return value;
}

static void check_input_for_security_s(const char* input)
{
    char* danger_token[] = {"|",
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
        NULL};

    for (int i = 0; danger_token[i] != NULL; ++i) {
        if (strstr(input, danger_token[i])) {
            ereport(ERROR,
                (errmodule(MOD_UDF),
                    errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Contains invaid character: \"%s\"", danger_token[i])));
        }
    }
}

static void check_extend_library_option(LibraryInfo* libInfo)
{
    const char* option = libInfo->option;

    int len = 0;
    /* check option which should not be null */
    if (option == NULL || (len = strlen(option)) == 0) {
        ereport(ERROR,
            (errmodule(MOD_UDF),
                errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("Invalid argument: should appoint an option of ls, addjar or rmjar.")));
    }

    /* check option which should be ls, addjar or rmjar */
    if ((len == 2) && (strncmp(option, "ls", len) == 0)) {
        libInfo->optionType = OPTION_LS;
    } else if ((len == 6) && (strncmp(option, "addjar", len) == 0)) {
        libInfo->optionType = OPTION_ADDJAR;
    } else if ((len == 5) && (strncmp(option, "rmjar", len) == 0)) {
        libInfo->optionType = OPTION_RMJAR;
    } else {
        ereport(ERROR,
            (errmodule(MOD_UDF),
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid argument: only support ls, addjar, rmjar options.")));
    }
}

/*
 * @Description: parse user-defined obs file path information.
 * @in obsinfo: user-defined obs file path information
 * @in filepath: another palloc string of the file path behind "obs://" in obsinfo
 * @out: OBSInfo structure
 */
static OBSInfo* parse_obsinfo(char* obsinfo, char* filepath)
{
    OBSInfo* obsInfo = (OBSInfo*)palloc0(sizeof(OBSInfo));
    if (filepath == NULL) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("filepath should not be NULL")));
    }
    char* p = filepath;
    while (*p != '\0') {
        if (*p == '/' && p != filepath) {
            obsInfo->bucket = (char*)palloc((p - filepath + 1) * sizeof(char));
            errno_t errorno = strncpy_s(obsInfo->bucket, p - filepath + 1, filepath, p - filepath);
            securec_check_c(errorno, "\0", "\0");

            p++;
            obsInfo->path = (char*)palloc((strlen(p) + 1) * sizeof(char));
            errorno = strncpy_s(obsInfo->path, strlen(p) + 1, p, strlen(p));
            securec_check_c(errorno, "\0", "\0");
            break;
        }

        p++;
    }

    if (obsInfo->bucket == NULL) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid argument: must set correct bucket.")));
    }

    if (obsInfo->path == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid argument: must set correct obs file path.")));
    }

    if ((p = strstr(obsinfo, "accesskey=")) != NULL) {
        obsInfo->accesskey = parse_value(p + strlen("accesskey="));
    }

    if (obsInfo->accesskey == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid argument: must set accesskey.")));
    }

    if ((p = strstr(obsinfo, "secretkey=")) != NULL) {
        obsInfo->secretkey = parse_value(p + strlen("secretkey="));
    }

    if (obsInfo->secretkey == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid argument: must set secretkey.")));
    }

    if ((p = strstr(obsinfo, "region=")) != NULL) {
        obsInfo->region = parse_value(p + strlen("region="));
    }

    if (obsInfo->region == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid argument: must set region.")));
    }

    return obsInfo;
}

static void parse_extend_library_operation(LibraryInfo* libInfo)
{
    /* parse libInfo->operation
     * the operation format should be like either:
     * "file://srcpath libraryname=libname"
     * for the case of copying a file in local system to every node in mppdb, where
     * 'srcpath' should be an absolute file path starting with / and
     * 'libname' should be a relative file path in mppdb.
     * OR
     * "obs://bucket/srcpath accesskey=ak secretkey=sk region=rg libraryname=libname"
     * for the case of downing a file in obs server to every node in mppdb, where
     * 'srcpath' should be the file path in obs server and
     * 'libname' should be a relative file path in mppdb.
     * OR
     * "libraryname=libname"
     * Note: unrecognized arguments will be omitted,
     * and in all cases except OPTION_LS, the 'libraryname' key can not be omitted.
     */

    char* operation = libInfo->operation;
    char* tmp = NULL;

    /* Check if there is "libraryname=", and it cannot be omitted except OPTION_LS */
    if ((tmp = strstr(operation, "libraryname=")) != NULL) {
        libInfo->destpath = parse_value(tmp + strlen("libraryname="));
    }
    if (libInfo->destpath == NULL) {
        if (libInfo->optionType != OPTION_LS)
            ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Invalid argument: must set a \"libraryname\".")));
    } else {
        if ((strstr(libInfo->destpath, "/") != NULL) || libInfo->destpath[0] == '.')
            ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Invalid argument: \"libraryname\" should not contain \"/\" or starting with \".\".")));
    }

    if (libInfo->optionType != OPTION_ADDJAR) {
        return;
    }

    if (strncmp(operation, "file://", strlen("file://")) == 0) {
        /* Check if there is "file://".
         * If it exists, it means the source path is local.
         */
        libInfo->isSourceLocal = true;
        tmp = operation + strlen("file://");
        if ((libInfo->localpath = parse_value(tmp)) == NULL || libInfo->localpath[0] != '/') {
            ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Invalid argument: must set an absolute path followed by \"file:///\".")));
        }
        if (strlen(libInfo->localpath) < 4 ||
            (strncmp(libInfo->localpath + strlen(libInfo->localpath) - 4, ".jar", 4) != 0 )) {
            ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Invalid argument: the source file must be .jar file.")));
        }
    } else if (strncmp(operation, "obs://", strlen("obs://")) == 0) {
        /* Check if there is "obs://".
         * If it exists, it means the source path is from obs,
         * then we shoule parse bucket, obspath, accesskey, secretkey and region
         */
        if (!isSecurityMode) {
            ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid argument: must set an absolute path followed by \"file:///\".")));
        }
        libInfo->isSourceLocal = false;
        tmp = operation + strlen("obs://");
        if ((tmp = parse_value(tmp)) == NULL) {
            ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Invalid argument: must set bucket and filepath followed by \"obs://\".")));
        }

        if (strlen(tmp) < 4 || (strncmp(tmp + strlen(tmp) - 4, ".jar", 4) != 0)) {
            ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Invalid argument: the source file must be .jar file.")));
        }

        libInfo->obsInfo = parse_obsinfo(operation, tmp);
        pfree_ext(tmp);
    } else {
        /* must set "file://" or "obs://" when OPTION_ADDJAR */
        ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("must set correct source file path.")));
    }

    char pathbuf[MAXPGPATH];
    get_pkglib_path(my_exec_path, pathbuf);
    join_path_components(pathbuf, pathbuf, "java");
    join_path_components(pathbuf, pathbuf, libInfo->destpath);
    if (access(pathbuf, F_OK) == 0) {
        ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid argument: the library already existed, please remove it first using rmjar.")));
    }
}

/*
 * @Description: Download obs file using udstools.py into
 *               $GAUSSHOME/share/postgresql/tmp/.
 * @in obsInfo: OBSInfo parsed from user-defined obs path
 * @in tmp_path: $GAUSSHOME/share/postgresql/tmp
 * @out: void
 */
static void get_obsfile(OBSInfo* obsInfo, const char* tmp_path)
{
#ifndef ENABLE_MULTIPLE_NODES
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else

    StringInfoData cmd;
    initStringInfo(&cmd);

    check_backend_env(tmp_path);

    appendStringInfo(&cmd,
        "python %s/udstools.py %s %s %s %s %s %s",
        tmp_path, obsInfo->accesskey, obsInfo->secretkey, obsInfo->path,
        obsInfo->region, obsInfo->bucket, tmp_path);

    /* reset key buffer to avoid private info leak */
    errno_t ret = memset_s(obsInfo->accesskey, strlen(obsInfo->accesskey) + 1, 0, strlen(obsInfo->accesskey) + 1);
    securec_check(ret, "\0", "\0");
    ret = memset_s(obsInfo->secretkey, strlen(obsInfo->secretkey) + 1, 0, strlen(obsInfo->secretkey) + 1);
    securec_check(ret, "\0", "\0");
    ereport(DEBUG1,
        (errmodule(MOD_UDF),
            errmsg("[udstools]:%s/udstools.py\n"
                   "[path]:%s\n"
                   "[region]:%s\n"
                   "[bucket]:%s\n",
                tmp_path,
                obsInfo->path,
                obsInfo->region,
                obsInfo->bucket)));

    /* execute downloading from obs */
    check_input_for_security_s(cmd.data);
    pid_t rc = system(cmd.data);

    /* reset key buffer to avoid private info leak */
    ret = memset_s(cmd.data, cmd.len, 0, cmd.len);
    securec_check(ret, "\0", "\0");
    resetStringInfo(&cmd);
    appendStringInfo(&cmd,
        "python %s/udstools.py * * %s %s %s %s",
        tmp_path,
        obsInfo->path,
        obsInfo->region,
        obsInfo->bucket,
        tmp_path);

    if (rc == -1 || WIFEXITED(rc) == 0) {
        ereport(LOG,
            (errcode(ERRCODE_SYSTEM_ERROR), errmsg("[status]:%d %d System error. \"%s\"", rc, WIFEXITED(rc), cmd.data)));
        ereport(ERROR,
            (errcode(ERRCODE_SYSTEM_ERROR), errmsg("%d %d: System error.", rc, WIFEXITED(rc))));
    }

    ereport(LOG, (errmsg("[status]:%d %d %d [command]:\"%s\"", rc, WIFEXITED(rc), WEXITSTATUS(rc), cmd.data)));

    /* check download execution results, the rc should equal the 'os._exit(rc)' in udstools.py */
    rc = WEXITSTATUS(rc);
    switch (rc) {
        case OBS_SUCCESS:
            elog(LOG, "%d: Download from obs success. \"%s\"", rc, cmd.data);
            break;

        case OBS_FAILED_IO_ERROR:
            ereport(ERROR,
                ((errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
                    errmsg("%d: Failed to access system files.", rc))));
            break;

        case OBS_FAILED_INCORRECT_REGION:
            ereport(ERROR,
                ((errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
                    errmsg("%d: Invalid argument, must set correct region.", rc))));
            break;

        case OBS_FAILED_INVALID_ARGUMENT:
            ereport(ERROR,
                ((errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("%d: Parameters error.", rc))));
            break;

        case OBS_FAILED_BASE_EXCEPTION:
            ereport(
                ERROR, ((errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("%d: System error.", rc))));
            break;

        case OBS_FAILED_CONNECT_TO_UDS_SERVER:
            ereport(ERROR,
                ((errcode(ERRCODE_CONNECTION_FAILURE),
                    errmsg("%d: Failed to connect to obs server.", rc))));
            break;

        case OBS_FAILED_DOWNLOAD_FROM_UDS_SERVER:
            ereport(ERROR,
                ((errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
                    errmsg("%d: Failed to download from obs server.", rc))));
            break;

        case OBS_FAILED_TRY_AGAIN:
            ereport(ERROR,
                ((errcode(ERRCODE_CONNECTION_FAILURE),
                    errmsg("%d: Obs server is busy. Try again later.", rc))));
            break;

        default:
            ereport(ERROR, ((errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("%d: Download failed.", rc))));
            break;
    }

    pfree_ext(cmd.data);
#endif
}

static void execute_udstools_commnd(LibraryInfo* libInfo)
{
    /* As gs_om addjar command can only distrubte local file,
     * so we have to download obs file using udstools.py into $GAUSSHOME/share/postgresql/tmp/ first,
     * then in EXTENDLIB_CALL_OM stage, we use gs_om addjar command to distrubte the tmp file,
     * after that, we delete the tmp file.
     */

    char pathbuf[MAXPGPATH];
    get_share_path(my_exec_path, pathbuf);
    join_path_components(pathbuf, pathbuf, "tmp");
    char* tmp_path = pstrdup(pathbuf);

    get_obsfile(libInfo->obsInfo, tmp_path);

    join_path_components(pathbuf,
        pathbuf,
        (last_dir_separator(libInfo->obsInfo->path) == NULL) ? libInfo->obsInfo->path
                                                             : (last_dir_separator(libInfo->obsInfo->path) + 1));
    libInfo->localpath = pstrdup(pathbuf);

    pfree_ext(tmp_path);
}

static char* execute_gsom_javaudf_command(LibraryInfo* libInfo)
{
    StringInfoData cmd;
    initStringInfo(&cmd);
    appendStringInfo(&cmd, "gs_om -t javaUDF -m %s", libInfo->option);
    if (libInfo->localpath != NULL) {
        appendStringInfo(&cmd, " -s %s", libInfo->localpath);
    }
    if (libInfo->destpath != NULL) {
        appendStringInfo(&cmd, " -d %s", libInfo->destpath);
    }
    ereport(DEBUG1, (errmodule(MOD_UDF), errmsg("[gs_om]: %s", cmd.data)));

    FILE* fp = NULL;
    char results[MAXPGPATH]; /* handle the gs_om command return message per line. */
    StringInfoData ret;
    initStringInfo(&ret);
    bool isFailed = false; /* check if the gs_om command execution is failed. */
    bool isError = false;  /* check if the gs_om command reports failures in stderr. */

    check_input_for_security_s(cmd.data);
    /* we should catch both stderr and stdout so we append redirect tags to gs_om command */
    appendStringInfo(&cmd, " 2>&1");

    if ((fp = popen(cmd.data, "r")) != NULL) {
        error_t errorno;
        /* parse the gs_om command returns by line,
         * the returns contains either exepected results or error massages,
         * so we should parse the error massage and then elog() the massage,
         * or parse the expected results and stored in 'ret'.
         */
        while (fgets(results, sizeof(results), fp) != NULL) {
            char* tmp = NULL;
            if ((tmp = strstr(results, "command not found")) != NULL) {
                /* get rid of the last \n */
                tmp[strlen(tmp) - 1] = '\0';
                errorno =
                    snprintf_s(results, MAXPGPATH, MAXPGPATH - 1, "gs_om %s. Please check the environment.\n", tmp);
                securec_check_ss(errorno, "\0", "\0");
                isError = true;
            } else if ((tmp = strstr(results, "[GAUSS-50201]")) != NULL) {
                errorno =
                    snprintf_s(results, MAXPGPATH, MAXPGPATH - 1, "Invalid argument: the library does not exist.\n");
                securec_check_ss(errorno, "\0", "\0");
                isError = true;
            } else if ((tmp = strstr(results, "[GAUSS-")) != NULL) {
                /* error reports in gs_om command stderr
                 * the message starts with '[GAUSS-'
                 */
                errorno = snprintf_s(results, MAXPGPATH, MAXPGPATH - 1, "[%s", tmp + strlen("[GAUSS-"));
                securec_check_ss(errorno, "\0", "\0");
                char tails[MAXPGPATH];
                while (fgets(tails, sizeof(tails), fp) != NULL) {
                    errorno = strncat_s(results, MAXPGPATH, tails, strlen(tails));
                    securec_check_c(errorno, "\0", "\0");
                }
                isError = true;
            } else if ((tmp = strstr(results, "Success: File consistent")) != NULL) {
                /* expected results when find a consistent file when ls,
                 * the message starts with 'Success: File consistent'
                 */
                tmp = strstr(results, "[");
                if (tmp != NULL) {
                    errorno = snprintf_s(results, MAXPGPATH, MAXPGPATH - 1, "t\t%s", tmp);
                } else {
                    errorno = snprintf_s(results, MAXPGPATH, MAXPGPATH - 1, "System Error.\n");
                }
                securec_check_ss(errorno, "\0", "\0");
            } else if ((tmp = strstr(results, "Failed:  File inconsistent")) != NULL) {
                /* expected results when find a inconsistent file when ls
                 * the message starts with 'Failed:  File inconsistent'
                 */
                tmp = strstr(results, "[");
                if (tmp != NULL) {
                    errorno = snprintf_s(results, MAXPGPATH, MAXPGPATH - 1, "f\t%s", tmp);
                } else {
                    errorno = snprintf_s(results, MAXPGPATH, MAXPGPATH - 1, "System Error.\n");
                }
                securec_check_ss(errorno, "\0", "\0");
            } else {
                results[0] = '\0';
            }

            if (strlen(results) > 0)
                appendStringInfoString(&ret, results);
            ereport(DEBUG1, (errmodule(MOD_UDF), errmsg("%d: %s", isError, ret.data)));
            if (isError)
                break;
        }
        pclose(fp);
    } else {
        isFailed = true;
    }

    if (ret.len > 0) {
        /* get rid of the last \n */
        ret.data[ret.len - 1] = '\0';
    }

    /* remove tmp file if there is any no matter the execution failed or not */
    if (libInfo->optionType == OPTION_ADDJAR && !libInfo->isSourceLocal && libInfo->localpath != NULL) {
        resetStringInfo(&cmd);
        appendStringInfo(&cmd, "rm -rf %s", libInfo->localpath);
        ereport(DEBUG1, (errmodule(MOD_UDF), errmsg("[rm_tmp]: %s", cmd.data)));
        int rc = system(cmd.data);
        if (rc != 0) {
            ereport(LOG,
                (errcode(ERRCODE_SYSTEM_ERROR),
                errmsg("[WARNING] remove the tmp file downloaded from the OBS Server failed, [command] %s", cmd.data)));
        }
    }

    if (isFailed) {
        ereport(LOG, (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("execute command failed, [command] %s", cmd.data)));
        ereport(ERROR, (errmodule(MOD_UDF),
                (errcode(ERRCODE_SQL_ROUTINE_EXCEPTION), errmsg("execute command failed"))));
    }

    if (isError) {
        ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
                errmsg("%s", (ret.len > 0) ? ret.data : "Cannot get detailed execution results.")));
    }

    /* the execution succeed, we return \"t\" as a successful operation */
    if (libInfo->optionType != OPTION_LS) {
        appendStringInfoString(&ret, "t");
    }

    return ret.data;
}

PG_FUNCTION_INFO_V1(gs_extend_library);
/*
 * gs_extend_library:
 *	function to distribute a jar file to every node
 *
 * @IN cstring: option of either ls, addjar or rmjar
 * @IN cstring: operation string
 * @RETURN: success or error messages.
 */
Datum gs_extend_library(PG_FUNCTION_ARGS)
{
#ifndef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), (errmsg("Unsupport feature"),
        errdetail("gs_extend_library is not supported for centralize deployment"),
        errcause("The function is not implemented."), erraction("Do not use this function."))));
#endif

    if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("must be system admin to use the gs_extend_library function"))));
    }

    LibraryInfo* libInfo = (LibraryInfo*)palloc0(sizeof(LibraryInfo));
    libInfo->option = PG_GETARG_CSTRING(0);
    libInfo->operation = PG_GETARG_CSTRING(1);
    libInfo->destpath = NULL;
    libInfo->localpath = NULL;
    libInfo->isSourceLocal = true;
    libInfo->obsInfo = NULL;

    char* result = NULL;
    ExtendLibraryStatus status = EXTENDLIB_CHECK_OPTION;
    bool done = false;
    while (!done) {
        switch (status) {
            case EXTENDLIB_CHECK_OPTION: {
                check_extend_library_option(libInfo);
                status = EXTENDLIB_PARSE_OPERATION;
                break;
            }
            case EXTENDLIB_PARSE_OPERATION: {
                if (libInfo->operation == NULL) {
                    if (libInfo->optionType == OPTION_LS) {
                        status = EXTENDLIB_CALL_OM;
                        break;
                    }

                    ereport(ERROR, (errmodule(MOD_UDF),
                            errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("Invaid argument: must at least set libraryname")));
                }

                parse_extend_library_operation(libInfo);
                if (!libInfo->isSourceLocal) {
                    status = EXTENDLIB_CALL_OBS_DOWNLOAD;
                } else {
                    status = EXTENDLIB_CALL_OM;
                }
                break;
            }
            case EXTENDLIB_CALL_OBS_DOWNLOAD: {
                execute_udstools_commnd(libInfo);
                status = EXTENDLIB_CALL_OM;
                break;
            }
            case EXTENDLIB_CALL_OM: {
                result = execute_gsom_javaudf_command(libInfo);
                done = true;
                break;
            }
            default: {
                ereport(ERROR, (errmodule(MOD_UDF), errcode(ERRCODE_SYSTEM_ERROR), errmsg("System Error.")));
                break;
            }
        }
    }

    if (libInfo->obsInfo != NULL) {
        pfree_ext(libInfo->obsInfo->accesskey);
        pfree_ext(libInfo->obsInfo->secretkey);
        pfree_ext(libInfo->obsInfo->region);
        pfree_ext(libInfo->obsInfo->bucket);
        pfree_ext(libInfo->obsInfo->path);
        pfree_ext(libInfo->obsInfo);
    }

    pfree_ext(libInfo->option);
    pfree_ext(libInfo->operation);
    pfree_ext(libInfo->destpath);
    pfree_ext(libInfo->localpath);
    pfree_ext(libInfo);

    if (result == NULL || strlen(result) == 0) {
        PG_RETURN_NULL();
    }
    PG_RETURN_CSTRING(result);
}

/*
 * @Description: Download obs file to $GAUSSHOME/share/postgresql/tmp/
 *               by udstools.py.
 * @in pathname: user-defined obs path information
 * @in basename: user-defined obs file name
 * @in extension: user-defined obs file name extension
 * @out: $GAUSSHOME/share/postgresql/tmp/
 */
char* get_obsfile_local(char* pathname, const char* basename, const char* extension)
{
    char* fullname = NULL;
    char* tmp_path = NULL;
    OBSInfo* obsinfo = NULL;
    StringInfoData tmp;
    initStringInfo(&tmp);

    /* get fullname */
    appendStringInfo(&tmp, "%s.%s", basename, extension);
    fullname = pstrdup(tmp.data);
    resetStringInfo(&tmp);

    /* add fullname to the end */
    tmp_path = parse_value(pathname);
    if (tmp_path == NULL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid argument: must set correct bucket and obs file path.")));

    appendStringInfo(&tmp, "%s", tmp_path);
    if (tmp_path[strlen(tmp_path) - 1] != '/')
        appendStringInfo(&tmp, "/");
    appendStringInfo(&tmp, "%s", fullname);
    pfree_ext(tmp_path);
    /* get OBSInfo */
    obsinfo = parse_obsinfo(pathname, tmp.data);
    resetStringInfo(&tmp);

    /* get the tmp path */
    char pathbuf[MAXPGPATH];
    get_share_path(my_exec_path, pathbuf);
    appendStringInfo(&tmp, "%s/tmp", pathbuf);
    tmp_path = pstrdup(tmp.data);
    /* delete the downloaded file when commit or abort */
    appendStringInfo(&tmp, "/%s", fullname);
    InsertIntoPendingLibraryDelete(tmp.data, true);
    InsertIntoPendingLibraryDelete(tmp.data, false);
    /* check if there is a file of the same name */
    if (file_exists(tmp.data)) {
        ereport(LOG, (errmodule(MOD_TS), errmsg("Dictionary file exists and will be replaced: \"%s\"", tmp.data)));
    }
    /* ok, download the obs file to the tmp path */
    get_obsfile(obsinfo, tmp_path);

    /* clean up */
    pfree_ext(obsinfo->accesskey);
    pfree_ext(obsinfo->secretkey);
    pfree_ext(obsinfo->region);
    pfree_ext(obsinfo->bucket);
    pfree_ext(obsinfo->path);
    pfree_ext(obsinfo);
    pfree_ext(tmp.data);
    return tmp_path;
}
