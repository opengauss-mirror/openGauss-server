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
 * bbox_lib.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_lib.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "bbox_syscall_support.h"
#include "bbox_lib.h"
#include "bbox_print.h"
#include "../../src/include/securec.h"
#include "../../src/include/securec_check.h"

#define PAGE_SIZE 4096
#define BBOX_MAX_PIDS 32
#define BBOX_TMP_LEN_32 32

#define container_of(ptr, type, member)                   \
    ({                                                    \
        const typeof(((type*)0)->member)* __mptr = (ptr); \
        (type*)((char*)__mptr - offsetof(type, member));  \
    })

#define typeof decltype

struct PIPE_ID {
    s32 iFd;
    pid_t pid;
};

struct PIPE_IDS {
    struct PIPE_ID stPid;
    s32 isUsed;
};

static struct PIPE_IDS astPipeIds[BBOX_MAX_PIDS];

/*
 * compare string pszSrc and pszTarget
 */
s32 bbox_strncmp(const char* pszSrc, const char* pszTarget, s32 count)
{
    signed char cRes = 0;

    while (count) {
        if ((cRes = *pszSrc - *pszTarget++) != 0 || !*pszSrc++) {
            break;
        }
        count--;
    }

    return cRes;
}

/*
 * compare string pszSrc and pszTarget
 */
s32 bbox_strcmp(const char* pszSrc, const char* pszTarget)
{
    unsigned char c1, c2;

    while (1) {
        c1 = *pszSrc++;
        c2 = *pszTarget++;

        if (c1 != c2) {
            return c1 < c2 ? -1 : 1;
        }

        if (c1 == 0) {
            break;
        }
    }
    return 0;
}

/*
 * get the length of string pszString
 */
s32 bbox_strlen(const char* pszString)
{
    const char* pszTemp = NULL;

    for (pszTemp = pszString; *pszTemp != '\0'; ++pszTemp) {
        /* nothing */;
        continue;
    }

    return pszTemp - pszString;
}

/*
 * get the length of string pszString
 */
s32 bbox_strnlen(const char* pszString, s32 count)
{
    const char* pszTemp = NULL;

    for (pszTemp = pszString; count-- && *pszTemp != '\0'; ++pszTemp) {
        /* nothing */;
    }

    return pszTemp - pszString;
}

/*
 * convert a string to interger
 */
s32 bbox_atoi(const char* pszString)
{
    s32 n = 0;
    s32 iNeg = 0;

    if (*pszString == '-') {
        iNeg = 1;
    }

    if (iNeg) {
        pszString++;
    }

    while (*pszString >= '0' && *pszString <= '9') {
        n = 10 * n + (*pszString++ - '0');
    }

    return iNeg ? -n : n;
}

/*
 * compare memory
 */
s32 bbox_memcmp(const void* cs, const void* ct, s32 count)
{
    const unsigned char *su1 = NULL;
    const unsigned char *su2 = NULL;

    for (su1 = (const unsigned char*)cs, su2 = (const unsigned char*)ct; count > 0; ++su1, ++su2, count--) {
        if (*su1 != *su2) {
            return *su1 < *su2 ? -1 : +1;
        }
    }

    return 0;
}

/*
 * search string l2 in l1
 */
char* bbox_strstr(const char* s1, const char* s2)
{
    int l1, l2;

    l2 = bbox_strlen(s2);
    if (!l2) {
        return (char*)s1;
    }

    l1 = bbox_strlen(s1);
    while (l1 >= l2) {
        l1--;
        if (!bbox_memcmp(s1, s2, l2)) {
            return (char*)s1;
        }
        s1++;
    }
    return NULL;
}

/*
 * make a directory
 */
s32 bbox_mkdir(const char* pszDir)
{
    char szDirName[BBOX_TMP_LEN_32 * 16];
    char* p = NULL;
    s32 len;

    if (bbox_snprintf(szDirName, sizeof(szDirName), "%s", pszDir) <= 0) {
        return RET_ERR;
    }

    len = bbox_strnlen(szDirName, sizeof(szDirName));
    if (szDirName[len - 1] == '/') {
        if (len == 1) {
            return RET_OK;
        }

        szDirName[len - 1] = 0;
    }

    for (p = szDirName + 1; *p; p++) {
        if (*p != '/') {
            continue;
        }

        *p = 0;
        if (sys_mkdir(szDirName, S_IRWXU) < 0) {
            if (errno != EEXIST) {
                return RET_ERR;
            }
        }

        *p = '/';
    }

    if (sys_mkdir(szDirName, S_IRWXU) < 0) {
        if (errno != EEXIST) {
            return RET_ERR;
        }
    }

    return RET_OK;
}

/*
 * search free pipe id
 */
struct PIPE_ID* bbox_GetFreePid(void)
{
    u32 i;

    for (i = 0; i < BBOX_MAX_PIDS; i++) {
        if (astPipeIds[i].isUsed == 0) {
            astPipeIds[i].isUsed = 1;
            return &(astPipeIds[i].stPid);
        }
    }

    return NULL;
}

/*
 * Release the occupied pipeid
 */
void bbox_PutPid(struct PIPE_ID* pstPid)
{
    struct PIPE_IDS* pstPids = NULL;
    if (pstPid == NULL) {
        return;
    }

    pstPids = container_of(pstPid, struct PIPE_IDS, stPid);

    errno_t rc = memset_s(pstPids, sizeof(struct PIPE_IDS), 0, sizeof(struct PIPE_IDS));
    securec_check_c(rc, "\0", "\0");
}

/*
 * find available pipe id by file handle
 */
struct PIPE_ID* bbox_FindPid(int iFd)
{
    u32 i;

    for (i = 0; i < BBOX_MAX_PIDS; i++) {
        if (astPipeIds[i].isUsed == 0) {
            continue;
        }

        if (astPipeIds[i].stPid.iFd == iFd) {
            return &(astPipeIds[i].stPid);
        }
    }

    return NULL;
}

/*
 * run popen
 */
s32 sys_popen(char* pszCmd, const char* pszMode)
{
    struct PIPE_ID* volatile stCurPid = NULL;
    s32 iFd;
    s32 saIpedes[2] = {0};
    pid_t pid;

    if (NULL == pszCmd || NULL == pszMode) {
        errno = EINVAL;
        return -1;
    }

    /* determine whether the read-write mode is correct */
    if ((*pszMode != 'r' && *pszMode != 'w') || pszMode[1] != '\0') {
        errno = EINVAL;
        return -1;
    }

    /* get free pipe id */
    stCurPid = bbox_GetFreePid();
    if (NULL == stCurPid) {
        return -1;
    }

    /* create pipe */
    if (sys_pipe(saIpedes) < 0) {
        return -1;
    }

    /* create child prosess */
    pid = sys_fork();
    if (pid < 0) {
        /* close fd if error */
        sys_close(saIpedes[0]);
        sys_close(saIpedes[1]);
        bbox_PutPid(stCurPid);
        return -1;
    } else if (pid == 0) { /* Child. */
        /* child prosess */
        s32 i = 0;
        char* pArgv[4];
        pArgv[0] = "sh";
        pArgv[1] = "-c";
        pArgv[2] = pszCmd;
        pArgv[3] = 0;

        /* Restore signal function */
        sys_signal(SIGQUIT, SIG_DFL);
        sys_signal(SIGTSTP, SIG_IGN);
        sys_signal(SIGTERM, SIG_DFL);
        sys_signal(SIGINT, SIG_DFL);

        for (i = STDERR_FILENO + 1; i < 1024; i++) {
            /* do not close pipe. */
            if (i == saIpedes[0] || i == saIpedes[1]) {
                continue;
            }

            sys_close((int)i);
        }

        if (*pszMode == 'r') {
            int tpipedes1 = saIpedes[1];

            sys_close(saIpedes[0]);
            /*
             * We must NOT modify saIpedes, due to the
             * semantics of vfork.
             */
            if (tpipedes1 != STDOUT_FILENO) {
                sys_dup2(tpipedes1, STDOUT_FILENO);
                sys_close(tpipedes1);
                tpipedes1 = STDOUT_FILENO;
            }
        } else {
            sys_close(saIpedes[1]);
            if (saIpedes[0] != STDIN_FILENO) {
                sys_dup2(saIpedes[0], STDIN_FILENO);
                sys_close(saIpedes[0]);
            }
        }

        sys_execve("/bin/sh", pArgv, environ);
        bbox_print(PRINT_ERR, "exec '%s' failed, errno = %d, pid = %d\n", pszCmd, errno, pid);
        sys_exit(127);
        /* NOTREACHED */
    }

    /* Parent; assume fdopen can't fail. */
    if (*pszMode == 'r') {
        iFd = saIpedes[0];
        sys_close(saIpedes[1]);
    } else {
        iFd = saIpedes[1];
        sys_close(saIpedes[0]);
    }

    /* Link into list of file descriptors. */
    stCurPid->iFd = iFd;
    stCurPid->pid = pid;
    return iFd;
}

/*
 * close file handle
 */
int sys_pclose(s32 iFd)
{
    struct PIPE_ID* pstCur = NULL;
    s32 iStat = -1;
    pid_t pid;

    pstCur = bbox_FindPid(iFd);
    if (pstCur == NULL) {
        return -1;
    }

    sys_close(iFd);

    do {
        pid = sys_waitpid(pstCur->pid, &iStat, 0);
    } while (pid == -1 && errno == EINTR);

    bbox_PutPid(pstCur);
    return (pid == -1 ? -1 : iStat);
}

/*
 * list file in directory
 */
s32 bbox_listdir(const char* pstPath, BBOX_LIST_DIR_CALLBACK callback, void* pArgs)
{
    struct linux_dirent* pstEntry = NULL;
    s32 iDir;
    char szBuff[PAGE_SIZE];
    ssize_t nBytes;
    s32 iRet = RET_OK;

    if (callback == NULL || pstPath == NULL) {
        return RET_ERR;
    }

    iDir = sys_open(pstPath, O_RDONLY | O_DIRECTORY, 0);
    if (iDir < 0) {
        bbox_print(PRINT_ERR, "open directory failed, errno = %d\n", errno);
        return RET_ERR;
    }

    /* get the file in directory */
    do {
        nBytes = sys_getdents(iDir, (struct linux_dirent*)szBuff, sizeof(szBuff));
        if (nBytes < 0) {
            bbox_print(PRINT_ERR, "get directory ents failed, errno = %d\n", errno);
            sys_close(iDir);
            return RET_ERR;
        } else if (nBytes == 0) {
            /* break when there is no file not read */
            break;
        }

        for (pstEntry = (struct linux_dirent*)szBuff;
             (pstEntry < (struct linux_dirent*)&szBuff[nBytes]) && iRet == RET_OK;
             pstEntry = (struct linux_dirent*)((char*)pstEntry + pstEntry->d_reclen)) {
            if (pstEntry->d_ino == 0) {
                continue;
            }

            iRet = callback(pstPath, pstEntry->d_name, pArgs);
        }
    } while (nBytes > 0 && iRet == RET_OK);

    sys_close(iDir);

    return iRet;
}
