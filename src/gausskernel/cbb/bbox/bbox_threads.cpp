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
 * bbox_threads.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_threads.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "bbox_syscall_support.h"
#include "bbox_threads.h"
#include "bbox_print.h"
#include "bbox_atomic.h"
#include "../../src/include/securec.h"
#include "../../src/include/securec_check.h"

static const u32 iSyncSignals[] = {
    SIGABRT,
    SIGILL,
    SIGFPE,
    SIGSEGV,
    SIGBUS,
    SIGXCPU,
    SIGXFSZ,
};

struct TASK_ATTACH_INFO {
    pid_t pid;      /* pid */
    u8 cIsAttached; /* Whether the thread is ptraced */
};

struct TASK_CHECK_RESUME_ARGS {
    struct TASK_ATTACH_INFO* pstTaskInfo; /* recover process information */
    s32 iThreadCount;                     /* thread count */
    GET_THREAD_TYPE enType;               /* get thread type */
};

struct BBOX_ListParams {
    s32 iResult;                          /* result */
    s32 iError;                           /* error code */
    u8* pAltStackMem;                     /* Execution stack information */
    BBOX_GetAllThreadsCallBack pCallBack; /* Executes the callback function exported by the thread */
    BBOX_GetAllThreadDone pDoneCallback;  /* call back function after exporting the thread information */
    void* pDoneArgs;                      /* parameter of call back function pDoneCallback */
    GET_THREAD_TYPE enGetType;            /* get data type */
    va_list ap;                           /* parameter list of function pCallBack */
};

u8 g_szAltStackMem[BBOX_ALT_STACKSIZE]; /* independent thread stack memory */

BBOX_ATOMIC_STRU g_isBusy = BBOX_ATOMIC_INIT(0); /* whether deal with core file. */

/*
 * reserved count bytes on current stack, and set 0
 */
void BBOX_ReserveZeroStack(s32 count)
{
    char buff[count];

    errno_t rc = memset_s(buff, count, 0, count);
    securec_check_c(rc, "\0", "\0");

    (void)sys_read(-1, buff, count);
}

/*
 * clone the current process and runs the specified function
 * return 0 if seccess else err code
 */
s32 BBOX_CloneRun(u32 uFlags, s32 (*pFn)(void*), void* pArg, ...)
{
    /* reserve 4K when calling and running a function to protect waitpid can exit correct. */
    /* CLONE_UNTRACED flag is must or gdb will debug new process which is cloned. */
    pid_t pid;
    if (pArg == NULL || pFn == NULL) {
        return -1;
    }

    pid = sys__clone(pFn, (((char*)(pArg)) - 4096), uFlags | CLONE_UNTRACED, pArg, 0, 0, 0);

    return pid;
}

/*
 * get count of thread
 */
s32 BBOX_GetTaskNumber(char* szTaskPath)
{
    struct kernel_stat stProcSB = {0};
    s32 iProc = -1;

    if (szTaskPath == NULL) {
        bbox_print(PRINT_ERR, "parameter szTaskPath is null.\n");

        return -1;
    }

    /* open /proc */
    iProc = sys_open(szTaskPath, O_RDONLY | O_DIRECTORY, 0);
    if (iProc < 0) {
        bbox_print(PRINT_ERR, "open file %s failed, errno = %d\n", szTaskPath, errno);

        return -1;
    }

    /* get the number of first file nodes in the directory */
    if (sys_fstat(iProc, &stProcSB)) {
        bbox_print(PRINT_ERR, "Get file %s fstat, errno = %d, iProc = %d\n", szTaskPath, errno, iProc);

        sys_close(iProc);
        return -1;
    }

    sys_close(iProc);

    return stProcSB.st_nlink;
}

/*
 * get thread pid
 */
s32 BBOX_GetTaskId(struct TASK_ATTACH_INFO* pstTaskInfo, s32 iSize, char* szTaskPath)
{
    s32 iProc = -1;
    pid_t pid = 0;
    struct linux_dirent* pstEntry = NULL;
    char szBuff[PAGE_SIZE];
    ssize_t nBytes;
    const char* pszPtr = NULL;
    s32 iThreadCount = 0;

    if (pstTaskInfo == NULL || szTaskPath == NULL) {
        bbox_print(PRINT_ERR,
            "parameter is invalid, pstTaskInfo or szTaskPath is NULL, iSize = %d \n",
            iSize);

        return -1;
    }

    iProc = sys_open(szTaskPath, O_RDONLY | O_DIRECTORY, 0);
    if (iProc < 0) {
        bbox_print(PRINT_ERR, "open '%s' failed, errno = %d\n", szTaskPath, errno);

        return -1;
    }

    /* traverse /proc/[pid]/task, get count of thread and pid. */
    for (; iThreadCount < iSize;) {
        /* get pointer to the directory structure */
        nBytes = sys_getdents(iProc, (struct linux_dirent*)szBuff, sizeof(szBuff));
        if (nBytes < 0) {
            bbox_print(PRINT_ERR, "Get dents '%s' failed, errno = %d\n", szTaskPath, errno);

            goto errout;
        } else if (0 == nBytes) {
            sys_lseek(iProc, 0, SEEK_SET);
            break;
        }

        /* recursively traversing the directory */
        for (pstEntry = (struct linux_dirent*)szBuff;
             (pstEntry < (struct linux_dirent*)&szBuff[nBytes]) && (iThreadCount < iSize);
             pstEntry = (struct linux_dirent*)((char*)pstEntry + pstEntry->d_reclen)) {
            if (pstEntry->d_ino == 0) {
                continue;
            }

            pszPtr = pstEntry->d_name;
            bbox_print(PRINT_DBG, "dir: %s/%s\n", szTaskPath, pszPtr);

            if (*pszPtr == '.') {
                pszPtr++;
            }

            /* ignore directory that is not a proc. */
            if (*pszPtr < '0' || *pszPtr > '9') {
                continue;
            }

            /* get pid information */
            pid = bbox_atoi(pszPtr);

            if (!pid || pid == sys_gettid()) {
                bbox_print(PRINT_ERR, "pid is invalid , pid = %d\n", pid);

                continue;
            }

            /* add thread information into a array. */
            pstTaskInfo[iThreadCount].pid = pid;
            pstTaskInfo[iThreadCount].cIsAttached = 0;
            iThreadCount++;
        }
    }

    sys_close(iProc);
    return iThreadCount;
errout:
    sys_close(iProc);
    return -1;
}

/*
 * a ptrace debug thread
 * in      : TASK_ATTACH_INFO - thread information
 *           iPidCount - count of thread information
 *           iDoPtraceCheck - check if ptrace success
 * return  : 0 - success
 *           err code - failed
 */
s32 BBOX_PtraceAttachPid(struct TASK_ATTACH_INFO* pstTaskInfo, s32 iPidCount, s32 iDoPtraceCheck)
{
    u32 i;
    pid_t pid;
    unsigned long m = 0;
    unsigned long n = 0;

    if (pstTaskInfo == NULL || iPidCount <= 0) {
        return RET_ERR;
    }

    for (i = 0; i < (u32)iPidCount; i++) {
        /* walk through all the threads and debug the trace. */
        pid = pstTaskInfo[i].pid;
        if (sys_ptrace(PTRACE_ATTACH, pid, (void*)0, (void*)0) < 0) {
            bbox_print(PRINT_ERR, "ptrace failed, pid = %d, errno = %d\n", pid, errno);
            continue;
        }

        /* check that if the thread state is normal. */
        while (sys_waitpid(pid, (int*)0, __WALL) < 0) {
            if (errno != EINTR) {
                bbox_print(PRINT_ERR, "wait for %d failed, errno = %d\n", pid, errno);

                sys_ptrace(PTRACE_DETACH, pid, 0, 0);
                break;
            }
        }

        if (iDoPtraceCheck) {
            int ret = 0;
            ret = sys_ptrace(PTRACE_PEEKDATA, pid, &m, &n);
            /* check that if the trace is valid */
            if (ret || (m != n)) {
                bbox_print(PRINT_ERR,
                    "ptrace peek data failed, pid = %d, ret = %d,m = %lu, n = %lu, errno = %d\n",
                    pid, ret, m, n, errno);

                sys_ptrace(PTRACE_DETACH, pid, 0, 0);
                continue;
            }
        }

        bbox_print(PRINT_LOG, "ptrace attach %d\n", pid);
        pstTaskInfo[i].cIsAttached = 1;
    }

    return RET_OK;
}

/*
 * cancel ptrace debug thread
 * in      : TASK_ATTACH_INFO - thread information
 *           iPidCount - count of thread information
 *           iDoPtraceCheck - check if ptrace success
 * return  : 0 - success
 *           err code - failed
 */
void BBOX_DetachAllThread(struct TASK_ATTACH_INFO* pstTaskInfo, s32 iPidCount)
{
    u32 i;
    pid_t pid;

    if (pstTaskInfo == NULL || iPidCount <= 0) {
        return;
    }

    for (i = 0; i < (u32)iPidCount; i++) {
        pid = pstTaskInfo[i].pid;
        if (!pstTaskInfo[i].cIsAttached) {
            continue;
        }
        bbox_print(PRINT_LOG, "ptrace detach %d\n", pid);

        /* cancel track. */
        sys_sched_yield();
        sys_ptrace(PTRACE_DETACH, pid, 0, 0);
        sys_kill(pid, SIGCONT);
    }

    return;
}

/*
 * notification function after dump module have got all data, to recover a thread.
 */
void BBOX_CheckResumeThread(void* pArgs)
{
    struct TASK_CHECK_RESUME_ARGS* pstTaskCheck = NULL;

    if (pArgs == NULL) {
        return;
    }

    pstTaskCheck = (struct TASK_CHECK_RESUME_ARGS*)pArgs;

    if (GET_TYPE_SNAP == pstTaskCheck->enType) {
        BBOX_DetachAllThread(pstTaskCheck->pstTaskInfo, pstTaskCheck->iThreadCount);
    }
}

/*
 * ptrace thread and run function.
 * in      : pstArgs - information of callback function
 *           iMaxThreadCount - max count of thread
 *           pszProcSelfTask - /proc/[pid]/task of current tracked thread.
 * return 0 if success else err code.
 */
s32 BBOX_PtraceAndRun(struct BBOX_ListParams* pstArgs, s32 iMaxThreadCount, char* pszProcSelfTask)
{
    struct TASK_ATTACH_INFO stTaskInfo[iMaxThreadCount];
    pid_t thread_pids[iMaxThreadCount];

    struct TASK_CHECK_RESUME_ARGS stTaskCheck;
    s32 iThreadCount = 0;
    s32 iAttachCount = 0;
    s32 iRet = 0;
    s32 iDoPtraceCheck = 1;
    s32 i;

    if (pstArgs == NULL || pszProcSelfTask == NULL || iMaxThreadCount <= 0) {

        bbox_print(PRINT_ERR,
            "Parameter is invald, pstArgs or pszProcSelfTask is NULL, iMaxThreadCount = %d \n",
            iMaxThreadCount);

        return RET_ERR;
    }

    errno_t rc = memset_s(stTaskInfo, sizeof(stTaskInfo), 0, sizeof(stTaskInfo));
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(thread_pids, sizeof(thread_pids), 0, sizeof(thread_pids));
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(&stTaskCheck, sizeof(stTaskCheck), 0, sizeof(stTaskCheck));
    securec_check_c(rc, "\0", "\0");

    iThreadCount = BBOX_GetTaskId(stTaskInfo, iMaxThreadCount, pszProcSelfTask);
    if (iThreadCount <= 0) {
        bbox_print(PRINT_ERR, "Get task id failed.\n");

        goto errout;
    }

    if (GET_TYPE_DUMP != pstArgs->enGetType) {
        iDoPtraceCheck = 0;
    } else {
        iDoPtraceCheck = 1;
    }

    iRet = BBOX_PtraceAttachPid(stTaskInfo, iThreadCount, iDoPtraceCheck);
    if (iRet != RET_OK) {
        bbox_print(PRINT_ERR, "Ptrace attache failed.\n");
        goto errout;
    }

    /* copy information of thread that have been attaching. */
    for (i = 0; i < iThreadCount; i++) {
        if (!stTaskInfo[i].cIsAttached) {
            continue;
        }
        thread_pids[iAttachCount] = stTaskInfo[i].pid;
        iAttachCount++;
    }

    stTaskCheck.pstTaskInfo = stTaskInfo;
    stTaskCheck.enType = pstArgs->enGetType;
    stTaskCheck.iThreadCount = iThreadCount;

    /* run call back function. */
    bbox_print(PRINT_TIP, "Thread count :%d\n", iThreadCount);
    bbox_print(PRINT_TIP, "Ptraced thread count :%d\n", iAttachCount);
    bbox_print(PRINT_LOG, "Run callback: thread count = %d\n", iThreadCount);
    /* Callback to the thread information handler. */
    pstArgs->iResult = pstArgs->pCallBack(BBOX_CheckResumeThread, &stTaskCheck, iAttachCount, thread_pids, pstArgs->ap);
    pstArgs->iError = errno;

    BBOX_DetachAllThread(stTaskInfo, iThreadCount);

    return RET_OK;

errout:
    BBOX_DetachAllThread(stTaskInfo, iThreadCount);
    return RET_ERR;
}

/*
 * print log information if export failed.
 */
void BBOX_PrintFailedLog(const char* pFileName)
{
    ssize_t iRet = 0;
    s32 iWriteSize = 0;
    s32 iBboxLogFd = -1;

    iBboxLogFd = sys_open(pFileName, O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (iBboxLogFd < 0) {

        bbox_print(PRINT_ERR, "open failed, errno = %d\n", errno);

        return;
    }

    iWriteSize = bbox_strnlen(g_acBBoxLog, BBOX_LOG_SIZE) + 1;
    iWriteSize = (iWriteSize > BBOX_LOG_SIZE) ? BBOX_LOG_SIZE : iWriteSize;
    iRet = sys_write(iBboxLogFd, g_acBBoxLog, iWriteSize);
    if (iRet < 0) {
        bbox_print(PRINT_ERR, "write failed, errno = %d\n", errno);

        sys_close(iBboxLogFd);
        return;
    }

    sys_close(iBboxLogFd);
}

/*
 * export thread information.
 */
void BBOX_ListThread(struct BBOX_ListParams* pstArgs)
{
    pid_t ppid = 0;
    s32 iMaker = -1;
    s32 iMaxThreadCount = 0;
    s32 iRet = 0;

    struct kernel_stat stMarkerSB;
    char szProcSelfTask[BBOX_PROC_PATH_LEN];
    char pszMarkPath[BBOX_PROC_PATH_LEN];
    stack_t altstack;
    errno_t rc = EOK;

    if (pstArgs == NULL) {
        bbox_print(PRINT_ERR, "pstArgs is NULL.\n");

        return;
    }

    ppid = sys_getppid();

    iMaker = sys_socket(PF_LOCAL, SOCK_DGRAM, 0);
    if (iMaker < 0) {
        bbox_print(PRINT_ERR, "sys_socket error, errno = %d\n", errno);
        goto errout;
    }

    if (sys_fcntl(iMaker, F_SETFD, FD_CLOEXEC) < 0) {
        bbox_print(PRINT_ERR, "sys_fcntl error, errno = %d\n", errno);
        goto errout;
    }

    if (bbox_snprintf(szProcSelfTask, BBOX_PROC_PATH_LEN, "/proc/%d/task", ppid) <= 0) {
        bbox_print(PRINT_ERR, "bbox_snprintf is failed, errno = %d.\n", errno);
        goto errout;
    }

    if (bbox_snprintf(pszMarkPath, BBOX_PROC_PATH_LEN, "/proc/%d/fd/%d", ppid, iMaker) <= 0) {
        bbox_print(PRINT_ERR, "bbox_snprintf is failed, errno = %d.\n", errno);
        goto errout;
    }

    bbox_print(PRINT_TIP, "Get information for pid %d:\n", ppid);

    rc = memset_s(&stMarkerSB, sizeof(stMarkerSB), 0, sizeof(stMarkerSB));
    securec_check_c(rc, "\0", "\0");

    if (sys_stat(pszMarkPath, &stMarkerSB) < 0) {
        bbox_print(PRINT_ERR, "sys_stat error, errno = %d, path = %s\n", errno, pszMarkPath);
        goto errout;
    }

    /* switch stack pointer */
    rc = memset_s(&altstack, sizeof(altstack), 0, sizeof(altstack));
    securec_check_c(rc, "\0", "\0");
    altstack.ss_sp = pstArgs->pAltStackMem;
    altstack.ss_flags = 0;
    altstack.ss_size = BBOX_ALT_STACKSIZE;
    sys_sigaltstack(&altstack, (const stack_t*)NULL);

    /* get max count of task. */
    iMaxThreadCount = BBOX_GetTaskNumber(szProcSelfTask);
    if (iMaxThreadCount <= 0) {
        bbox_print(PRINT_ERR, "Get task number failed.\n");
        goto errout;
    }

    /* ptrace and run thread. */
    iRet = BBOX_PtraceAndRun(pstArgs, iMaxThreadCount, szProcSelfTask);
    if (iRet != RET_OK) {
        bbox_print(PRINT_ERR, "ptrace task and run failed.\n");
        goto errout;
    }

    bbox_print(PRINT_TIP, "Get information success.\n");

    sys_close(iMaker);

    if (RET_OK != pstArgs->iResult) {
        BBOX_PrintFailedLog((char*)(((struct BBOX_ListDirParam*)(pstArgs->pDoneArgs))->pArg2));
    }

    if (pstArgs->pDoneCallback != NULL) {
        pstArgs->pDoneCallback(pstArgs->pDoneArgs);
    }

    sys_exit(0);
errout:
    if (iMaker > 0) {
        sys_close(iMaker);
    }

    bbox_print(PRINT_ERR, "Get information failed.\n");

    pstArgs->iResult = -1;
    pstArgs->iError = errno;

    BBOX_PrintFailedLog((char*)(((struct BBOX_ListDirParam*)(pstArgs->pDoneArgs))->pArg2));
    if (pstArgs->pDoneCallback != NULL) {
        pstArgs->pDoneCallback(pstArgs->pDoneArgs);
    }

    sys_exit(1);
}

/*
 * get return value of child process
 * in      : iClonePid - PID of child process
 *           pstArgs - parameter
 *           iCloneErrno - err code
 * return 0 if success else failed.
 */
s32 BBOX_GetClonePidResult(pid_t iClonePid, struct BBOX_ListParams* pstArgs, s32 iCloneErrno)
{
    s32 iStatus = 0;
    s32 iRet = 0;

    if (iClonePid < 0) {

        bbox_print(PRINT_ERR, "Clone failed, can't create child process, errno = %d.\n", iCloneErrno);

        BBOX_PrintFailedLog((char*)(((struct BBOX_ListDirParam*)(pstArgs->pDoneArgs))->pArg2));
        if (pstArgs->pDoneCallback != NULL) {
            pstArgs->pDoneCallback(pstArgs->pDoneArgs);
        }

        return RET_ERR;
    }

    /* wait child process exit. */
    while ((iRet = sys_waitpid(iClonePid, &iStatus, __WALL)) < 0 && errno == EINTR) {
        continue;
    }

    bbox_print(PRINT_LOG, "clone pid %d ret is %x, status = %x\n", iClonePid, iRet, WIFEXITED(iStatus));

    if (iRet < 0) {
        pstArgs->iError = errno;
        pstArgs->iResult = -1;
    } else if (WIFEXITED(iStatus)) {
        switch (WEXITSTATUS(iStatus)) {
            case 0:
                break;
            case 2:
                pstArgs->iError = EFAULT;
                pstArgs->iResult = -1;
                break;
            case 3:
                pstArgs->iError = EPERM;
                pstArgs->iResult = 1;
                break;
            default:
                pstArgs->iError = ECHILD;
                pstArgs->iResult = -1;
                break;
        }
    } else if (!WIFEXITED(iStatus)) {
        pstArgs->iError = EFAULT;
        pstArgs->iResult = -1;
        bbox_print(PRINT_ERR, "WIFEXITED status failed");
    } else {
        pstArgs->iError = iCloneErrno;
        pstArgs->iResult = -1;
        bbox_print(PRINT_ERR, "WIFEXITED error, errno = %d\n", iCloneErrno);
    }

    return RET_OK;
}

/*
 * get all threads and run specify function
 */
s32 BBOX_GetAllThreads(
    GET_THREAD_TYPE enType, BBOX_GetAllThreadDone pDone, void* pDoneArgs, BBOX_GetAllThreadsCallBack pCallback, ...)
{
    struct BBOX_ListParams stArgs;
    struct kernel_sigset_t stSigBlocked;
    struct kernel_sigset_t stSigOld;
    s32 iDumpable = 1;
    s32 iSigNo;
    s32 iCloneErrno;
    pid_t ClonePid;

    errno = 0;

    if (BBOX_AtomicIncReturn(&g_isBusy) > 1) {
        BBOX_AtomicDec(&g_isBusy);

        bbox_print(PRINT_ERR, "Dump task is running.\n");

        errno = EALREADY;
        return -1;
    }

    if (enType >= GET_TYPE_BUTT || pCallback == NULL) {
        bbox_print(PRINT_ERR, "Parameter is invalid, enType = %d, and maybe pCallback is NULL", enType);
        BBOX_AtomicDec(&g_isBusy);
        return -1;
    }

    errno_t rc = memset_s(&stArgs, sizeof(stArgs), 0, sizeof(stArgs));
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(&stSigBlocked, sizeof(stSigBlocked), 0, sizeof(stSigBlocked));
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(&stSigOld, sizeof(stSigOld), 0, sizeof(stSigOld));
    securec_check_c(rc, "\0", "\0");

    va_start(stArgs.ap, pCallback);

    /* clear new stack */
    rc = memset_s(g_szAltStackMem, BBOX_ALT_STACKSIZE, 0, BBOX_ALT_STACKSIZE);
    securec_check_c(rc, "\0", "\0");
    /* reserve 32K */
    BBOX_ReserveZeroStack(1024 * 32);

    /* check and set dump flag. */
    iDumpable = sys_prctl(PR_GET_DUMPABLE, 0, 0, 0, 0);
    if (!iDumpable) {
        sys_prctl(PR_SET_DUMPABLE, 1, 0, 0, 0);
    }

    /* set start parameter of dump thread. */
    stArgs.iResult = -1;
    stArgs.iError = 0;
    stArgs.pAltStackMem = g_szAltStackMem;
    stArgs.pCallBack = pCallback;
    stArgs.pDoneCallback = pDone;
    stArgs.pDoneArgs = pDoneArgs;
    stArgs.enGetType = enType;

    /* suspend all signals */
    sys_sigfillset(&stSigBlocked);
    for (iSigNo = 0; iSigNo < (s32)(sizeof(iSyncSignals) / sizeof(*iSyncSignals)); iSigNo++) {
        sys_sigdelset(&stSigBlocked, iSyncSignals[iSigNo]);
    }

    /* block all signals */
    if (sys_sigprocmask(SIG_BLOCK, &stSigBlocked, &stSigOld)) {
        stArgs.iError = errno;
        stArgs.iResult = -1;
        bbox_print(PRINT_ERR, "sys_sigprocmask error, errno = %d\n", errno);
        goto errout;
    }

    /* create child process and run function to export thread information. */
    if (GET_TYPE_DUMP == enType) {

        ClonePid = BBOX_CloneRun(CLONE_VM | CLONE_FS | CLONE_FILES, (s32(*)(void*))BBOX_ListThread, &stArgs);
    } else {
        /* copy VMA if type is snapshoot. */
        ClonePid = BBOX_CloneRun(CLONE_FS | CLONE_FILES, (s32(*)(void*))BBOX_ListThread, &stArgs);
    }

    iCloneErrno = errno;

    /* restoring signal */
    sys_sigprocmask(SIG_SETMASK, &stSigOld, &stSigOld);

    if (BBOX_GetClonePidResult(ClonePid, &stArgs, iCloneErrno) != RET_OK) {
        bbox_print(PRINT_ERR, "BBOX_GetClonePidResult error\n");
    }

errout:
    BBOX_AtomicDec(&g_isBusy);

    if (!iDumpable) {
        sys_prctl(PR_SET_DUMPABLE, iDumpable, 0, 0, 0);
    }

    va_end(stArgs.ap);

    errno = stArgs.iError;
    return stArgs.iResult;
}
