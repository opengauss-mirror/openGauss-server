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
 * bbox_elf_dump_base.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_elf_dump_base.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "bbox.h"
#include "bbox_elf_dump_base.h"
#include "bbox_syscall_support.h"
#include "../../src/include/securec.h"
#include "../../src/include/securec_check.h"

#ifdef __cplusplus
#if __cplusplus
extern "C" {
#endif
#endif /* __cplusplus */

BBOX_ATOMIC_STRU g_stLockBlackList = BBOX_ATOMIC_INIT(0); /* black list of atomic variable */

/*
 * Determines whether the byte order of the local machine is large or small
 * return :  ELFDATA2LSB - large
 *        :  ELFDATA2MSB - small
 */
int BBOX_DetermineMsb(void)
{
    union INT_PROBE {
        short sShortInt;
        char cSplit[sizeof(short)];
    } unProbe;

    unProbe.sShortInt = BBOX_MSB_LSB_INT;

    if ((BBOX_LITTER_BITS == unProbe.cSplit[0]) && (BBOX_HIGH_BITS == unProbe.cSplit[1])) {
        return ELFDATA2LSB;
    } else {
        return ELFDATA2MSB;
    }
}

/*
 * converts a string to time
 * in     :  char *pSwitch - the string to be converted
 * out    :  struct BBOX_ELF_TIMEVAL *pstElfTimeval - result
 * return :  RET_OK - success
 *           RET_ERR - failed
 */
int BBOX_StringToTime(const char* pSwitch, struct BBOX_ELF_TIMEVAL* pstElfTimeval)
{
    int iSwitchTimes = 0;

    if (NULL == pSwitch || NULL == pstElfTimeval) {
        bbox_print(PRINT_ERR,
            "BBOX_StringToTime parameters is invalid: pSwitch or pstElfTimeval is NULL.\n");
        return RET_ERR;
    }

    /* converts a string to num */
    while (*pSwitch && *pSwitch != ' ') {
        iSwitchTimes = DECIMALISM_SPAN * iSwitchTimes + (*pSwitch) - '0';
        pSwitch++;
    }

    pstElfTimeval->lTvSec = iSwitchTimes / SEC_CHANGE_MIRCO_SEC;
    pstElfTimeval->lTvMicroSec = (iSwitchTimes % SEC_CHANGE_MIRCO_SEC) * SEC_CHANGE_MIRCO_SEC;

    return RET_OK;
}

/*
 * read a character from file
 * in     :  struct BBOX_READ_FILE_IO *pstIO - bbox file pointer
 * return :  the character read in file - success
 *           RET_ERR - failed
 */
int BBOX_GetCharFromFile(struct BBOX_READ_FILE_IO* pstIO)
{
    ssize_t iReadSize = -1;

    if (NULL == pstIO) {
        bbox_print(PRINT_ERR, "BBOX_GetCharFromFile parameters is invalid: pstIO is NULL.\n");
        return RET_ERR;
    }

    unsigned char* pTempIO = pstIO->pData;
    if (pTempIO == pstIO->pEnd) {
        /* read character from file when the buffer is empty, and push it into buffer */
        BBOX_NOINTR(iReadSize = sys_read(pstIO->iFd, pstIO->szBuff, sizeof(pstIO->szBuff)));
        if (iReadSize <= 0) {
            if (0 == iReadSize) {
                errno = 0;
            }

            return RET_ERR;
        }

        pTempIO = &(pstIO->szBuff[0]);
        pstIO->pEnd = &(pstIO->szBuff[iReadSize]);
    }

    pstIO->pData = pTempIO + 1;

    return *pTempIO;
}

/*
 * converts a string to num
 * in      : struct BBOX_READ_FILE_IO *pstIO - file to be read
 * out     : size_t *pAddress - buffer to store result
 * return  : the result num - success
 *           RET_ERR - failed
 */
int BBOX_StringSwitchInt(struct BBOX_READ_FILE_IO* pstIO, size_t* pAddress)
{
    int iMappingTextChar = 0;

    if (NULL == pstIO || NULL == pAddress) {
        bbox_print(
            PRINT_ERR, "BBOX_StringSwitchInt parameters is invalid: pstIO or pAddress is NULL.\n");
        return RET_ERR;
    }

    *pAddress = 0;
    iMappingTextChar = BBOX_GetCharFromFile(pstIO);
    while (
        (iMappingTextChar >= '0' && iMappingTextChar <= '9') || (iMappingTextChar >= 'a' && iMappingTextChar <= 'f')) {

        /* left shift the variable, and add the num converted from character at the end. */
        *pAddress =
            (*pAddress << ONE_HEXA_DECIMAL_BITS) |
            (unsigned int)(iMappingTextChar < 'A' ? iMappingTextChar - '0'
                                                  : ((unsigned int)iMappingTextChar & 0xF) + ASC2_CHAR_GREATER_NUM);
        iMappingTextChar = BBOX_GetCharFromFile(pstIO); /* read next character */
    }

    return iMappingTextChar;
}

/*
 * when read file /proc/self/maps, ignore unusefull information and skip to the end of line 
 * after we have geting all necessary information.
 * return  : count of character store into buffer - success
 *           RET_ERR - failed
 */
int BBOX_SkipToLineEnd(struct BBOX_READ_FILE_IO* pstReadIO)
{
    int iGetChar = -1;

    if (NULL == pstReadIO) {
        bbox_print(PRINT_ERR, "BBOX_SkipToLineEnd parameters is invalid: pstReadIO is NULL.\n");
        return RET_ERR;
    }

    do {
        /* reads characters until the newline character */
        iGetChar = BBOX_GetCharFromFile(pstReadIO);
        if (RET_ERR == iGetChar) {
            bbox_print(PRINT_ERR, "BBOX_SkipToLineEnd is failed, iGetChar= %d.\n", iGetChar);
            return RET_ERR;
        }
    } while (iGetChar != '\n');

    return RET_OK;
}

/*
 * get status information of process
 * in      : char *pBuffer - buffer to store result
 *           unsigned int uiBufLen - buffer size
 * return  : count of character store into buffer - success
 *           RET_ERR - failed
 */
int BBOX_GetStatusInfo(char* pBuffer, unsigned int uiBufLen)
{
    int iResult = 0;
    int iStatFD = -1;
    int iAllSize = 0;
    int iReadSize = 0;

    if (NULL == pBuffer) {
        bbox_print(PRINT_ERR, "BBOX_GetStatusInfo parameters is invalid.\n");
        return RET_ERR;
    }

    /* information title */
    iResult = bbox_snprintf(pBuffer, uiBufLen, "\nSTATUS INFO\n--------------------------------------------\n");
    if (iResult < 0 || iResult > (int)uiBufLen) {
        bbox_print(PRINT_ERR, "bbox_snprintf is failed, errno = %d.\n", errno);
        return RET_ERR;
    }

    --iResult;
    pBuffer += iResult;
    uiBufLen -= iResult;
    iAllSize += iResult;

    /* open /proc/self/status */
    BBOX_NOINTR(iStatFD = sys_open(BBOX_SELF_STATUS_PATH, O_RDONLY, 0));
    if (iStatFD < 0) {
        bbox_print(PRINT_ERR, "sys_open is failed, errno = %d.\n", errno);
        return RET_ERR;
    }

    /* read /proc/self/status */
    BBOX_NOINTR(iReadSize = sys_read(iStatFD, pBuffer, uiBufLen));
    if (iReadSize < 0) {
        (void)sys_close(iStatFD);
        bbox_print(PRINT_ERR, "sys_read is failed, errno = %d.\n", errno);
        return RET_ERR;
    }

    iAllSize += iReadSize;
    (void)sys_close(iStatFD);

    return iAllSize;
}

/*
 * get status information of cpu
 * in      : char *pBuffer - buffer to store result
 *           unsigned int uiBufLen - buffer size
 * return  : count of character store into buffer - success
 *           RET_ERR - failed
 */
int BBOX_GetCpuInfo(char* pBuffer, unsigned int uiBufLen)
{
    int iResult = 0;
    int iStatFD = -1;
    int iAllSize = 0;
    int iReadSize = 0;

    if (NULL == pBuffer) {
        bbox_print(PRINT_ERR, "BBOX_GetCpuInfo parameters is invalid.\n");
        return RET_ERR;
    }

    /* information title */
    iResult = bbox_snprintf(pBuffer, uiBufLen, "\nCPU INFO\n--------------------------------------------\n");
    if (iResult < 0 || iResult > (int)uiBufLen) {
        bbox_print(PRINT_ERR, "bbox_snprintf is failed, errno = %d.\n", errno);
        return RET_ERR;
    }

    --iResult;
    pBuffer += iResult;
    uiBufLen -= iResult;
    iAllSize += iResult;

    /* open /proc/stat */
    BBOX_NOINTR(iStatFD = sys_open(BBOX_PROC_INTER_PATH, O_RDONLY, 0));
    if (iStatFD < 0) {
        bbox_print(PRINT_ERR, "sys_open is failed, errno = %d.\n", errno);
        return RET_ERR;
    }

    /* read /proc/stat */
    BBOX_NOINTR(iReadSize = sys_read(iStatFD, pBuffer, uiBufLen));
    if (iReadSize < 0) {
        (void)sys_close(iStatFD);
        bbox_print(PRINT_ERR, "sys_read is failed, errno = %d.\n", errno);
        return RET_ERR;
    }

    iAllSize += iReadSize;
    (void)sys_close(iStatFD);

    return iAllSize;
}

/*
 * get information of system internal storage
 * in      : char *pBuffer - buffer to store result
 *           unsigned int uiBufLen - buffer size
 * return  : count of character store into buffer - success
 *           RET_ERR - failed
 */
int BBOX_GetMemInfo(char* pBuffer, unsigned int uiBufLen)
{
    int iResult = 0;
    int iStatFD = -1;
    int iAllSize = 0;
    int iReadSize = 0;

    if (NULL == pBuffer) {
        bbox_print(PRINT_ERR, "BBOX_GetMemInfo parameters is invalid.\n");
        return RET_ERR;
    }

    /* information title */
    iResult = bbox_snprintf(pBuffer, uiBufLen, "\nMEM INFO\n--------------------------------------------\n");
    if (iResult <= 0 || iResult > (int)uiBufLen) {
        bbox_print(PRINT_ERR, "bbox_snprintf is failed, errno = %d.\n", errno);
        return RET_ERR;
    }

    --iResult;
    pBuffer += iResult;
    uiBufLen -= iResult;
    iAllSize += iResult;

    /* open /proc/meminfo */
    BBOX_NOINTR(iStatFD = sys_open(BBOX_PROC_MEMINFO_PATH, O_RDONLY, 0));
    if (iStatFD < 0) {
        bbox_print(PRINT_ERR, "sys_open is failed, iStatFD = %d.\n", iStatFD);
        return RET_ERR;
    }

    /* read /proc/meminfo */
    BBOX_NOINTR(iReadSize = sys_read(iStatFD, pBuffer, uiBufLen));
    if (iReadSize < 0) {
        (void)sys_close(iStatFD);
        bbox_print(PRINT_ERR, "sys_read is failed, iReadSize = %d.\n", iReadSize);
        return RET_ERR;
    }

    iAllSize += iReadSize;
    (void)sys_close(iStatFD);

    return iAllSize;
}

/*
 * get information of ps command
 * in     :  char *pBuffer - buffer to write result information
 *        :  unsigned int uiBufLen  - size of buffer
 * return :  success -  count of characters written to the buffer
 *           failed  - RET_ERR
 */
int BBOX_GetPsInfo(char* pBuffer, unsigned int uiBufLen)
{
    int iResult = 0;
    int iCommandFD = -1;
    int iAllSize = 0;
    int iReadSize = 0;

    if (NULL == pBuffer) {
        bbox_print(PRINT_ERR, "BBOX_GetPsInfo parameters is invalid.\n");

        return RET_ERR;
    }

    /* information title */
    iResult = bbox_snprintf(pBuffer, uiBufLen, "\nPROCESS INFO\n--------------------------------------------\n");
    if (iResult <= 0 || iResult > (int)uiBufLen) {
        bbox_print(PRINT_ERR, "bbox_snprintf is failed, errno = %d.\n", errno);
        return RET_ERR;
    }

    --iResult;
    pBuffer += iResult;  /* calculate the start index of next part */
    uiBufLen -= iResult; /* calculate free length */
    iAllSize += iResult; /* counts the number of characters written to buffer */

    /* run ps commend */
    BBOX_NOINTR(iCommandFD = sys_popen(BBOX_PS_CMD, "r"));
    if (iCommandFD < 0) {
        bbox_print(PRINT_ERR, "sys_popen is failed, iStatFD = %d.\n", iCommandFD);
        return RET_ERR;
    }

    /* read result of cpmmand ps */
    BBOX_NOINTR(iReadSize = sys_read(iCommandFD, pBuffer, uiBufLen));
    if (iReadSize < 0) {
        (void)sys_pclose(iCommandFD);

        bbox_print(PRINT_ERR, "sys_read is failed, iReadSize = %d.\n", iReadSize);

        return RET_ERR;
    }

    iAllSize += iReadSize;
    (void)sys_pclose(iCommandFD);

    if (0 != iReadSize) {
        pBuffer[iReadSize - 1] = '\0';
    }

    return iAllSize;
}

/*
 *
 * get running information of system
 * in    :   char *pBuffer - the buffer to store information
 *           unsigned int uiBufLen - size of buffer
 * return:   success - count of characters written to the buffer
 *           failed  - RET_ERR
 */
int _BBOX_GetAddonInfo(char* pBuffer, unsigned int uiBufLen)
{
    int iResult = 0;
    unsigned int uiAllStringSz = 0;
    unsigned int uiLastLen = uiBufLen;
    char* pBufPos = pBuffer;
    errno_t rc = EOK;

    rc = memset_s(pBuffer, uiBufLen, 0, uiBufLen);
    securec_check_c(rc, "\0", "\0");

    /* get status information of process */
    iResult = BBOX_GetStatusInfo(pBufPos, uiLastLen);
    if (iResult < 0) {

        bbox_print(PRINT_ERR, "BBOX_GetStatusInfo is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    /* get status information of cpu */
    pBufPos += iResult;       /* calculate the offset of next written */
    uiLastLen -= iResult;     /* calculate free length */
    uiAllStringSz += iResult; /* counts the number of characters written to buffer */
    iResult = BBOX_GetCpuInfo(pBufPos, uiLastLen);
    if (iResult < 0) {

        bbox_print(PRINT_ERR, "BBOX_GetCpuInfo is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    /* get information of system internal storage */
    pBufPos += iResult;       /* calculate the offset of next written */
    uiLastLen -= iResult;     /* calculate free length */
    uiAllStringSz += iResult; /* counts the number of characters written to buffer */
    iResult = BBOX_GetMemInfo(pBufPos, uiLastLen);
    if (iResult < 0) {

        bbox_print(PRINT_ERR, "BBOX_GetMemInfo is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    /* get status information of another process */
    pBufPos += iResult;       /* calculate the offset of next written */
    uiLastLen -= iResult;     /* calculate free length */
    uiAllStringSz += iResult; /* counts the number of characters written to buffer */
    iResult = BBOX_GetPsInfo(pBufPos, uiLastLen);
    if (iResult < 0) {

        bbox_print(PRINT_ERR, "BBOX_GetPSInfo is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    uiAllStringSz += iResult; /* counts the number of characters written to buffer */

    return uiAllStringSz;
}

#ifdef __cplusplus
#if __cplusplus
}
#endif
#endif /* __cplusplus */
