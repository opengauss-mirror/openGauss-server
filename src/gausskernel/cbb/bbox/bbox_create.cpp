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
 * bbox_create.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_create.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "bbox_elf_dump_base.h"
#include "bbox_create.h"
#include "bbox.h"
#include "../../src/include/securec.h"
#include "../../src/include/securec_check.h"

/* Core path of the bbox process */
char g_szBboxCorePath[BBOX_NAME_PATH_LEN] = "./";

/* count of the BBOX process */
s32 g_iBBoxCoreFileCount = 30;

/* the max core file size of bbox process */
s32 g_iBBoxCoreFileSize = 50;

long int g_iCoreDumpBeginTime = 0;
long int g_iCoreDumpEndTime = 0;

char g_acDateTime[BBOX_TINE_LEN];

/*
 * get system date time.
 * return 0 if sucess else err code.
 */
int BBOX_GetSysDateTime(void)
{
    int iCommandFD = -1;
    int iReadSize = 0;

    BBOX_NOINTR(iCommandFD = sys_popen(BBOX_DATE_TIME_CMD, "r"));
    if (iCommandFD < 0) {
        bbox_print(PRINT_ERR, "sys_popen is failed, errno = %d.\n", errno);
        return RET_ERR;
    }

    /* read the result of sys_read command */
    BBOX_NOINTR(iReadSize = sys_read(iCommandFD, g_acDateTime, BBOX_TINE_LEN));
    if (iReadSize <= 0) {
        (void)sys_pclose(iCommandFD);
        bbox_print(PRINT_ERR, "sys_read is failed, iReadSize = %d.\n", iReadSize);
        return RET_ERR;
    }

    (void)sys_pclose(iCommandFD);

    if (iReadSize > 0) {
        g_acDateTime[iReadSize - 1] = '\0'; /* remove '\n' */
    }

    bbox_print(PRINT_LOG, "Get system time %s.\n", g_acDateTime);

    return RET_OK;
}

/*
 * get default core file name
 * return 0 if sucess else err code.
 */
s32 BBOX_GetDefaultCoreName(char* szFileName, s32 iSize, const char* szAddText)
{
    if (szFileName == NULL) {
        return RET_ERR;
    }

    if (szAddText == NULL) {
        szAddText = "";
    }

    struct kernel_timeval stProgramCoreDumpTime = {0};
    sys_gettimeofday(&stProgramCoreDumpTime, NULL);

    s32 ret = bbox_snprintf(szFileName,
        iSize,
        "%s/core-%s-%d-%s-%s.lz4",
        g_szBboxCorePath,
        progname,
        sys_getpid(),
        g_acDateTime,
        szAddText);

    return (ret > 0) ? RET_OK : RET_ERR;
}

/*
 * get temp core file name
 * return 0 if sucess else err code.
 */
s32 BBOX_GetTmpCoreName(char* szFileName, s32 iSize)
{
    if (szFileName == NULL) {
        return RET_ERR;
    }

    struct kernel_timeval stProgramCoreDumpTime = {0};
    sys_gettimeofday(&stProgramCoreDumpTime, NULL);

    g_iCoreDumpBeginTime = stProgramCoreDumpTime.tv_sec;
    bbox_print(PRINT_TIP, "coredump begin at %ld\n", stProgramCoreDumpTime.tv_sec);

    s32 ret = bbox_snprintf(szFileName,
        iSize,
        "%s/%s-%d-%s-%s",
        g_szBboxCorePath,
        progname,
        sys_getpid(),
        g_acDateTime,
        BBOX_TMP_FILE_ADD_NAME);

    return (ret > 0) ? RET_OK : RET_ERR;
}

/*
 * get core file count of bbox process.
 * return 0 if sucess else err code.
 */
s32 BBOX_GetBBoxFileCount(const char* pszPath, const char* pszName, void* pArgs)
{
    s32* pIFileCount = NULL;

    pIFileCount = (s32*)pArgs;
    if (pIFileCount == NULL) {
        bbox_print(PRINT_ERR, "Invalid argument pstArgs\n");
        return RET_ERR;
    }

    /* ignore path "." and ".." */
    if ((0 == bbox_strcmp(pszName, ".")) || (0 == bbox_strcmp(pszName, ".."))) {
        return RET_OK;
    }

    /* ignore the file which not belong to bbox */
    if ((0 == bbox_strstr(pszName, BBOX_SNAP_FILE_ADD_NAME ".lz4")) &&
        (0 == bbox_strstr(pszName, BBOX_CORE_FILE_ADD_NAME ".lz4"))) {
        return RET_OK;
    }

    /* ignore the file which not created by this process. */
    if (bbox_strstr(pszName, progname) == 0) {
        return RET_OK;
    }

    /* file count ++ */
    *pIFileCount = *pIFileCount + 1;

    return RET_OK;
}

/*
 * get bbox ildiest name.
 * return 0 if sucess else err code.
 */
s32 BBOX_GetBBoxOldiestName(const char* pszPath, const char* pszName, void* pArgs)
{
    struct BBOX_ListDirParam* pstArgs = NULL;
    struct kernel_stat* pstOldiestState = NULL;
    char* pszOldName = NULL;
    struct kernel_stat stCurrState = {0};
    char szFileName[BBOX_NAME_PATH_LEN];

    pstArgs = (struct BBOX_ListDirParam*)pArgs;
    if (pstArgs == NULL) {
        bbox_print(PRINT_ERR, "Invalid argument pstArgs\n");
        return RET_ERR;
    }

    pstOldiestState = (struct kernel_stat*)pstArgs->pArg1;
    pszOldName = (char*)pstArgs->pArg2;

    /* ignore path "." and ".." */
    if ((0 == bbox_strcmp(pszName, ".")) || (0 == bbox_strcmp(pszName, ".."))) {
        return RET_OK;
    }

    /* ignore the file which not belong to bbox */
    if ((0 == bbox_strstr(pszName, BBOX_SNAP_FILE_ADD_NAME ".lz4")) &&
        (0 == bbox_strstr(pszName, BBOX_CORE_FILE_ADD_NAME ".lz4"))) {
        return RET_OK;
    }

    /* ignore the file which not created by this process. */
    if (bbox_strstr(pszName, progname) == 0) {
        return RET_OK;
    }

    if (bbox_snprintf(szFileName, sizeof(szFileName), "%s/%s", pszPath, pszName) <= 0) {
        bbox_print(PRINT_ERR, "bbox_snprintf is failed, errno = %d.\n", errno);
        return RET_ERR;
    }

    if (sys_stat(szFileName, &stCurrState) < 0) {
        bbox_print(PRINT_ERR, "Get stat of '%s' failed, errno = %d\n", szFileName, errno);
        return RET_ERR;
    }

    /* compare and judge if it is the oldiest time */
    if (stCurrState.st_mtime_ < pstOldiestState->st_mtime_) {
        /* record the oldiest file information */
        *pstOldiestState = stCurrState;
        if (bbox_snprintf(pszOldName, BBOX_NAME_PATH_LEN, "%s/%s", pszPath, pszName) <= 0) {
            bbox_print(PRINT_ERR, "bbox_snprintf is failed, errno = %d.\n", errno);
            return RET_ERR;
        }
    }

    return RET_OK;
}

/*
 * remove the oldiest bbox file.
 */
void BBOX_RemoveOldiestBBoxFile(void)
{
    s32 iRet = 0;
    struct kernel_stat stOldiestState;
    struct BBOX_ListDirParam stArgs = {0};
    char szFileName[BBOX_NAME_PATH_LEN];
    errno_t rc = EOK;

    /* set the file modified time to the max value acquiescently. */
    rc = memset_s(&stOldiestState, sizeof(stOldiestState), 0xFF, sizeof(stOldiestState));
    securec_check_c(rc, "\0", "\0");

    stOldiestState.st_size = -1;

    stArgs.pArg1 = &stOldiestState;
    stArgs.pArg2 = szFileName;

    /* search the oldiest file. */
    iRet = bbox_listdir(g_szBboxCorePath, BBOX_GetBBoxOldiestName, &stArgs);
    if (iRet != RET_OK) {
        return;
    }

    /* remove it if found. */
    if (-1 != stOldiestState.st_size) {
        sys_unlink(szFileName);
    }
}

/*
 * remove the old bbox file
 */
void BBOX_RemoveOldBBoxFile(void)
{
    s32 iFileCount = 0;
    s32 iRet = 0;
    s32 i;

    iRet = bbox_listdir(g_szBboxCorePath, BBOX_GetBBoxFileCount, &iFileCount);
    if (iRet != RET_OK) {
        bbox_print(PRINT_ERR, "Get bbox file count failed.\n");
        return;
    }

    /* remove redundant file */
    for (i = 0; i < (iFileCount - g_iBBoxCoreFileCount + 1); i++) {
        /* remove the oldiest bbox file. */
        BBOX_RemoveOldiestBBoxFile();
    }
}

/*
 * remove bbox temp file.
 */
s32 BBOX_DoRemoveTempBBoxFile(const char* pszPath, const char* pszName, void* pArgs)
{
    struct kernel_stat stCurrState = {0};
    struct kernel_timeval stCurrTime = {0};
    char szFileName[BBOX_NAME_PATH_LEN];

    /* ignore path "." and ".." */
    if ((0 == bbox_strcmp(pszName, ".")) || (0 == bbox_strcmp(pszName, ".."))) {
        return RET_OK;
    }

    /* ignore the file which not belong to bbox */
    if (0 == bbox_strstr(pszName, BBOX_TMP_FILE_ADD_NAME)) {
        return RET_OK;
    }

    /* ignore the file which not created by this process. */
    if (0 != bbox_strncmp(pszName, progname, bbox_strlen(progname))) {
        return RET_OK;
    }

    if (bbox_snprintf(szFileName, sizeof(szFileName), "%s/%s", pszPath, pszName) <= 0) {
        bbox_print(PRINT_ERR, "bbox_snprintf is failed, errno = %d.\n", errno);
        return RET_ERR;
    }

    if (sys_stat(szFileName, &stCurrState) < 0) {
        bbox_print(PRINT_ERR, "Get stat of '%s' failed, errno = %d\n", szFileName, errno);
        return RET_ERR;
    }

    /* get current time */
    sys_gettimeofday(&stCurrTime, NULL);

    /* remove temp file before BBOX_TMP_DEL_TIME_INTERVAL */
    if (stCurrState.st_mtime_ < (unsigned long)(stCurrTime.tv_sec - BBOX_TMP_DEL_TIME_INTERVAL)) {
        /* rm file */
        bbox_print(PRINT_ERR, "Delete bad bbox core file %s \n", szFileName);
        sys_unlink(szFileName);
    }

    return RET_OK;
}

/*
 * remove temp bbox file
 */
void BBOX_RemoveTempBBoxFile(void)
{
    s32 iRet = 0;

    iRet = bbox_listdir(g_szBboxCorePath, BBOX_DoRemoveTempBBoxFile, NULL);
    if (iRet != RET_OK) {
        bbox_print(PRINT_ERR, "Get bbox file count failed.\n");
        return;
    }
}

/*
 finish core dump file.
 in : args  defined by user and it is file name here.

*/
void BBOX_FinishDumpFile(void* args)
{
    char* pszNewName = NULL;
    char* pszOldName = NULL;
    struct kernel_timeval stProgramCoreDumpTime = {0};
    struct BBOX_ListDirParam* pstArgs = (struct BBOX_ListDirParam*)args;

    if (args == NULL) {
        bbox_print(PRINT_ERR, "BBOX_FinishDumpFile args is null.\n");
        return;
    }

    sys_gettimeofday(&stProgramCoreDumpTime, NULL);

    g_iCoreDumpEndTime = stProgramCoreDumpTime.tv_sec;
    bbox_print(PRINT_TIP, "coredump End at %ld\n", stProgramCoreDumpTime.tv_sec);

    bbox_print(PRINT_TIP, "coredump used time: %ld sec\n", g_iCoreDumpEndTime - g_iCoreDumpBeginTime);

    /* remove oldiest file */
    BBOX_RemoveOldBBoxFile();

    pszNewName = (char*)pstArgs->pArg1;
    pszOldName = (char*)pstArgs->pArg2;

    /* change file mode to 0600 */
    if (sys_chmod(pszOldName, 0600)) {
        bbox_print(PRINT_ERR, "set %s mode to 0600 failed, errno = %d\n", pszOldName, errno);
    }

    /* rename file */
    if (pszNewName != NULL && pszOldName != NULL) {
        if (sys_rename(pszOldName, pszNewName) < 0) {
            bbox_print(PRINT_ERR, "rename file %s to %s failed, errno = %d.\n", pszOldName, pszNewName, errno);
        }
    }

    /* remove temp bbox file. */
    BBOX_RemoveTempBBoxFile();
}

/*
 * create core dump file.
 * return 0 if success else err code.
 */
s32 BBOX_CreateCoredump(char* file_name)
{
    s32 iRet = 0;
    char szFileName[BBOX_NAME_PATH_LEN];
    char szTmpName[BBOX_NAME_PATH_LEN];
    struct BBOX_ListDirParam stArgs = {0};
    char* file_tmp = file_name;
    FRAME(frame);

    bbox_initlog(0);

    bbox_print(PRINT_TIP, "\nBBOX LOG\n-------------------------------\n");

    iRet = BBOX_GetSysDateTime();
    if (iRet != RET_OK) {
        return RET_ERR;
    }

    if (bbox_mkdir(g_szBboxCorePath) < 0) {
        bbox_print(PRINT_ERR, "bbox_mkdir is failed, errno = %d.\n", errno);
        return RET_ERR;
    }

    /* if file_name is NULL, create it using default name. */
    if (file_name == NULL) {
        if (BBOX_GetDefaultCoreName(szFileName, BBOX_NAME_PATH_LEN, BBOX_CORE_FILE_ADD_NAME) == RET_OK) {
            file_name = szFileName;
        } else {
            bbox_print(PRINT_ERR, "BBOX_GetDefaultCoreName is failed, errno = %d.\n", errno);
            return RET_ERR;
        }
    }

    bbox_print(PRINT_TIP, "core file path is %s\n", file_name);

    if (BBOX_GetTmpCoreName(szTmpName, BBOX_NAME_PATH_LEN) == RET_OK) {
        file_tmp = szTmpName;
    } else {
        bbox_print(PRINT_ERR, "BBOX_GetTmpCoreName is failed, errno = %d.\n", errno);
        return RET_ERR;
    }

    stArgs.pArg1 = file_name;
    stArgs.pArg2 = file_tmp;

    iRet = BBOX_GetAllThreads(GET_TYPE_DUMP, BBOX_FinishDumpFile, &stArgs, BBOX_DoDumpElfCore, &frame, file_tmp);

    return iRet;
}

/*
 * set core dump path
 * return 0 if success else err code.
 */
s32 BBOX_SetCoredumpPath(const char* pszPath)
{
    if (NULL != pszPath) {
        if (bbox_snprintf(g_szBboxCorePath, sizeof(g_szBboxCorePath), "%s", pszPath) <= 0) {
            bbox_print(PRINT_ERR, "bbox_snprintf is failed, errno = %d.\n", errno);
            return RET_ERR;
        }
    } else {
        return RET_ERR;
    }

    if (0 == bbox_strlen(g_szBboxCorePath)) {
        return RET_ERR;
    }

    if (bbox_mkdir(g_szBboxCorePath) < 0) {
        bbox_print(PRINT_ERR, "bbox_mkdir is failed, errno = %d.\n", errno);
        return RET_ERR;
    }

    return RET_OK;
}

/*
 * set core file count
 * return 0 if success else err code.
 */
s32 BBOX_SetCoreFileCount(s32 iCount)
{
    if (iCount < 0) {
        return RET_ERR;
    }

    g_iBBoxCoreFileCount = iCount;

    return RET_OK;
}

/*
 * add an blacklist item to exclude it from core file.
 *      void *pAddress : the head address of excluded memory
 *      u64 uilen : memory size
 * return RET_OK if success else RET_ERR.
 */
s32 BBOX_AddBlackListAddress(void* pAddress, u64 uiLen)
{
    if (pAddress == NULL || uiLen == 0) {
        bbox_print(PRINT_ERR, "parameter pAddress(******) uiLen(%llu) is invaild.\n", uiLen);
        return RET_ERR;
    }

    return _BBOX_AddBlackListAddress(pAddress, uiLen);
}

/*
 * remove an blacklist item.
 *      void *pAddress : the head address of excluded memory
 * return RET_OK if success else RET_ERR.
 */
s32 BBOX_RmvBlackListAddress(void* pAddress)
{
    if (pAddress == NULL) {
        bbox_print(PRINT_ERR, "parameter pAddress(******) is invaild.\n");
        return RET_ERR;
    }

    return _BBOX_RmvBlackListAddress(pAddress);
}

