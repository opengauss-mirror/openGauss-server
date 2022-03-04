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
 * bbox_elf_dump.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_elf_dump.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "bbox_elf_dump.h"
#include "bbox_syscall_support.h"
#include "postgres.h"
#include "gs_bbox.h"
#include "../../src/include/securec.h"
#include "../../src/include/securec_check.h"

#if UINTPTR_MAX == 0xffffffff
#define Elf_Ehdr Elf32_Ehdr
#else
#define Elf_Ehdr Elf64_Ehdr
#endif

#ifdef __cplusplus
#if __cplusplus
extern "C" {
#endif
#endif /* __cplusplus */

extern long int g_iCoreDumpBeginTime; /* begin time of coredump */

struct BBOX_ELF_SECTION g_stElfSectionInfo;          /* information of all section. */
BBOX_SECTION_STRU g_stSectionInfo[BBOX_SECTION_NUM]; /* array to record section information. */
char g_acBboxAddonInfo[BBOX_ADDON_INFO_SIZE];        /* record system information. */
char g_acBboxStrTabInfo[BBOX_SH_STR_TAB_SIZE];       /* record string symbol table. */

/*
 * ignore the field that discribe equipment or node when analyze a line of /proc/self/maps.
 * in      : struct BBOX_READ_FILE_IO *pstReadIO - file pointer to be read
 * return  : iGetChar -  current character of the file being read
 *           RET_ERR - failed
 */
static int BBOX_SkipDeviceAndNodeField(struct BBOX_READ_FILE_IO* pstReadIO)
{
    int iCount = -1;
    int iGetChar = -1; 

    if (NULL == pstReadIO) {

        bbox_print(PRINT_ERR, "BBOX_SkipDeviceAndNodeField parameters is invalid: pstReadIO is NULL.\n");

        return RET_ERR;
    }

    iGetChar = BBOX_GetCharFromFile(pstReadIO);
    if (RET_ERR == iGetChar) {

        bbox_print(PRINT_ERR, "BBOX_GetCharFromFile is failed, iGetChar= %d.\n", iGetChar);

        return RET_ERR;
    }

    for (iCount = 0; iCount < DEVICE_AND_NODE_FIELD_NUM; iCount++) {
        while (iGetChar == ' ') {
            iGetChar = BBOX_GetCharFromFile(pstReadIO);
            if (RET_ERR == iGetChar) {

                bbox_print(PRINT_ERR, "BBOX_GetCharFromFile is failed, iGetChar= %d.\n", iGetChar);

                return RET_ERR;
            }
        }

        while (iGetChar != ' ' && iGetChar != '\n') {
            if (RET_ERR == iGetChar) {

                bbox_print(PRINT_ERR, "BBOX_GetCharFromFile is failed, iGetChar= %d.\n", iGetChar);

                return RET_ERR;
            }
            iGetChar = BBOX_GetCharFromFile(pstReadIO);
        }

        while (iGetChar == ' ') {
            iGetChar = BBOX_GetCharFromFile(pstReadIO);
            if (RET_ERR == iGetChar) {

                bbox_print(PRINT_ERR, "BBOX_GetCharFromFile is failed, iGetChar= %d.\n", iGetChar);

                return RET_ERR;
            }
        }
    }

    return iGetChar;
}

/*
 * set whether the mapping is labeled as PF_DEVICE, means that, whether or not it's a device mapping.
 * in      : int *piGetChar - pointer to the character read
 *           struct BBOX_READ_FILE_IO *pstReadIO - pointer to struct of file read
 *           struct BBOX_VM_MAPS *pstVmMappingSegment - pointer to discription of mapping segment structure.
 * return RET_OK or RET_ERR
 */
static int BBOX_SetMappingDeviceFlag(
    int* piGetChar, struct BBOX_READ_FILE_IO* pstReadIO, struct BBOX_VM_MAPS* pstSegmentMapping)
{
    int iIsMappingDevicesFlag = BBOX_FALSE;
    const char* pszDeviceZero = DEVICE_ZERO_NAME_STRING;
    const char* pszDevice = pszDeviceZero;

    if (NULL == piGetChar || NULL == pstReadIO || NULL == pstSegmentMapping) {

        bbox_print(PRINT_ERR,
            "BBOX_FillMappingFlagsAndOffset parameters is invalid: " \
            "piGetChar, pstReadIO or pstSegmentMapping is NULL.\n");

        return RET_ERR;
    }

    /* compare and determine if it is a device */
    while (*pszDevice && *piGetChar == *pszDevice) {
        *piGetChar = BBOX_GetCharFromFile(pstReadIO);
        if (RET_ERR == *piGetChar) {
            bbox_print(PRINT_ERR, "BBOX_GetCharFromFile is failed, *piGetChar= %d.\n", *piGetChar);
            return RET_ERR;
        }

        pszDevice++;
    }

    iIsMappingDevicesFlag = (pszDevice >= pszDeviceZero + DEVICE_PREFIX_LEN) &&
                            ((*piGetChar != '\n' && *piGetChar != ' ') || *pszDevice != '\000');
    if (BBOX_TRUE == iIsMappingDevicesFlag) {
        pstSegmentMapping->iFlags |= PF_DEVICE; /* set flag of equipment. */

        bbox_print(PRINT_DBG,
            " Get Device Segment: StartAddr = %zu, EndAddr = %zu.\n",
            pstSegmentMapping->uiStartAddress,
            pstSegmentMapping->uiEndAddress);
    }

    return RET_OK;
}

/*
 * set whether the mapping is labeled as PF_VDSO, means that, whether or not it's a VDSO mapping.
 * in      : int *piGetChar - pointer to the character read
 *           struct BBOX_READ_FILE_IO *pstReadIO - pointer to struct of file read
 *           struct BBOX_VM_MAPS *pstVmMappingSegment - pointer to discription of mapping segment structure.
 * return RET_OK or RET_ERR
 */
static int BBOX_SetMappingVDSOFlag(
    int* piGetChar, struct BBOX_READ_FILE_IO* pstReadIO, struct BBOX_VM_MAPS* pstSegmentMapping)
{
    int iIsMappingVdsoFlag = BBOX_FALSE;
    int iIsMappingVvarFlag = BBOX_TRUE;
    const char* pszVdso = VDSO_NAME_STRING;
    const char* pszVvar = VVAR_NAME_STRING;

    if (NULL == piGetChar || NULL == pstReadIO || NULL == pstSegmentMapping) {

        bbox_print(PRINT_ERR,
            "BBOX_FillMappingFlagsAndOffset parameters is invalid: piGetChar," \
            "pstReadIO or pstSegmentMapping is NULL.\n");

        return RET_ERR;
    }

    while (*pszVdso && *piGetChar == *pszVdso) {
        if (iIsMappingVvarFlag == BBOX_TRUE) {
            iIsMappingVvarFlag = (*piGetChar == *pszVvar) ? BBOX_TRUE : BBOX_FALSE;
            pszVvar++;
        }
        *piGetChar = BBOX_GetCharFromFile(pstReadIO);
        if (RET_ERR == *piGetChar) {
            bbox_print(PRINT_ERR, "BBOX_GetCharFromFile is failed, *piGetChar= %d.\n", *piGetChar);

            return RET_ERR;
        }

        pszVdso++;
    }

    iIsMappingVdsoFlag = (*pszVdso == '\0' && (*piGetChar == '\n' || *piGetChar == ' ' || *piGetChar == '\0'));
    if (BBOX_TRUE == iIsMappingVdsoFlag) {
        pstSegmentMapping->iFlags |= PF_VDSO; /* set VDSO flag. */

        bbox_print(PRINT_DBG,
            " Get VDSO StartAddr = %zu, EndAddr = %zu.\n",
            pstSegmentMapping->uiStartAddress,
            pstSegmentMapping->uiEndAddress);
    }

    if (iIsMappingVvarFlag == BBOX_TRUE) {
        while (*pszVdso && *piGetChar == *pszVvar) {
            *piGetChar = BBOX_GetCharFromFile(pstReadIO);
            if (RET_ERR == *piGetChar) {
                bbox_print(PRINT_ERR, "BBOX_GetCharFromFile is failed, *piGetChar= %d.\n", *piGetChar);

                return RET_ERR;
            }

            pszVvar++;
        }

        if (*pszVvar == '\0' && (*piGetChar == '\n' || *piGetChar == ' ' || *piGetChar == '\0')) {
            pstSegmentMapping->iFlags |= PF_VVAR; /* set VVAR flag */

            bbox_print(PRINT_DBG,
                " Get VVAR StartAddr = %zu, EndAddr = %zu.\n",
                pstSegmentMapping->uiStartAddress, pstSegmentMapping->uiEndAddress);
        }
    }

    return RET_OK;
}

/*
 * check if the file is a dynamic library file.
 * in      : char* pszFilePath - path
 *           struct BBOX_VM_MAPS *pstSegmentMapping - pointer to discription of mapping segment structuer.
 * return RET_OK or RET_ERR.
 */
static int BBOX_SettingMappedFileFlag(char* pszFilePath, struct BBOX_VM_MAPS* pstSegmentMapping)
{
    int fd = -1;
    int retval = -1;
    Elf_Ehdr ehdr;

    fd = sys_open(pszFilePath, O_RDONLY, 0);
    if (fd < 0) {
        bbox_print(PRINT_ERR, "open file %s failed, errno = %d\n", pszFilePath, errno);
        return RET_ERR;
    }

    /* read ELF header */
    retval = sys_read(fd, &ehdr, sizeof(ehdr));
    if (retval < 0) {
        bbox_print(PRINT_ERR, "read elf failed, errno = %d\n", errno);
        goto errout;
    }

    /* check if the file is ELF file. */
    if (bbox_strncmp(ELFMAG, (char*)ehdr.e_ident, SELFMAG)) {
        pstSegmentMapping->iFlags |= PF_MAPPEDFILE;
        bbox_print(PRINT_DBG, "not an elf file.\n");
        /* this is map file instead of dynamic library file. */
        bbox_print(PRINT_DBG, "mapped file : %s\n", pszFilePath);
    }

    sys_close(fd);
    return RET_OK;

errout:

    if (fd > 0) {
        sys_close(fd);
    }
    return RET_ERR;
}

/*
 * check if the file is equipment file.
 * in      : char* pszFilePath - path
 *           struct BBOX_VM_MAPS *pstSegmentMapping - pointer to discription of mapping segment structuer.
 * return RET_OK or RET_ERR.
 */
static int BBOX_SettingDeviceFileFlag(char* pszFilePath, struct BBOX_VM_MAPS* pstSegmentMapping)
{
    struct kernel_stat stMarkerSB = {0};
    if (sys_stat(pszFilePath, &stMarkerSB) < 0) {
        bbox_print(PRINT_ERR, "sys_stat error, errno = %d, path = %s\n", errno, pszFilePath);
        return RET_ERR;
    }

    if (S_ISCHR(stMarkerSB.st_mode) || S_ISBLK(stMarkerSB.st_mode) || S_ISFIFO(stMarkerSB.st_mode) ||
        S_ISSOCK(stMarkerSB.st_mode)) {
        bbox_print(PRINT_DBG, "Device file : %s\n", pszFilePath);
        pstSegmentMapping->iFlags |= PF_DEVICE;
        return RET_OK;
    }

    return RET_ERR;
}

/*
 * check if corresponding address is a general file mapping by mmap instead of a dynamic library file
 * when read /proc/self/maps.
 * in      : struct BBOX_READ_FILE_IO *pstReadIO - pointer to file to be read.
 *           struct BBOX_VM_MAPS *pstSegmentMapping - pointer to discription of mapping segment structuer.
 * return  : int iGetChar - current character of file reading
 *           RET_ERR - read failed
 */
static int BBOX_SettingFileFlags(
    int* iGetChar, struct BBOX_READ_FILE_IO* pstReadIO, struct BBOX_VM_MAPS* pstSegmentMapping)
{
    char MapFilePath[PATH_MAX];
    int i = 0;

    MapFilePath[0] = *iGetChar;
    i++;

    /* read mapping file path after reading all address. */
    while ((*iGetChar = BBOX_GetCharFromFile(pstReadIO)) != '\n' && i < PATH_MAX - 1) {
        if (RET_ERR == *iGetChar) {
            bbox_print(PRINT_ERR, "BBOX_GetCharFromFile is failed, iGetChar= %d.\n", *iGetChar);
            return RET_ERR;
        }

        MapFilePath[i] = *iGetChar;
        i++;
    }

    MapFilePath[i] = 0;

    if (BBOX_SettingDeviceFileFlag(MapFilePath, pstSegmentMapping) == RET_OK) {
        return RET_OK;
    }

    if (BBOX_SettingMappedFileFlag(MapFilePath, pstSegmentMapping) == RET_OK) {
        return RET_OK;
    }

    return RET_ERR;
}

/*
 * fill flag and offset of struct BBOX_VM_MAPS when reading /proc/self/maps.
 * in      : struct BBOX_READ_FILE_IO *pstReadIO - pointer to file to be read.
 *           struct BBOX_VM_MAPS *pstSegmentMapping - pointer to discription of mapping segment structuer.
 * return  : int iGetChar - current character of file reading
 *           RET_ERR - read failed
 */
static int BBOX_FillMappingFlagsAndOffset(struct BBOX_READ_FILE_IO* pstReadIO, struct BBOX_VM_MAPS* pstSegmentMapping)
{
    int iRessult = 0;
    int iGetChar = -1;
    int iIsMappingAnonymousFlag = BBOX_FALSE;
    int iIsMappedFile = BBOX_FALSE;

    if (NULL == pstReadIO || NULL == pstSegmentMapping) {
        bbox_print(PRINT_ERR,
            "BBOX_FillMappingFlagsAndOffset parameters is invalid: pstReadIO or " \
            "pstSegmentMapping is NULL.\n");
        return RET_ERR;
    }

    /* read flags and set '-' to 0 after reading all address. */
    while ((iGetChar = BBOX_GetCharFromFile(pstReadIO)) != ' ') {
        if (RET_ERR == iGetChar) {
            bbox_print(PRINT_ERR, "BBOX_GetCharFromFile is failed, iGetChar= %d.\n", iGetChar);
            return RET_ERR;
        }

        pstSegmentMapping->iFlags = (pstSegmentMapping->iFlags << 1) | (unsigned int)(iGetChar != '-');
    }

    pstSegmentMapping->iFlags = ((pstSegmentMapping->iFlags) >> 1) & PF_MASK;

    /* read offset */
    iGetChar = BBOX_StringSwitchInt(pstReadIO, &(pstSegmentMapping->uiOffset));
    if (RET_ERR == iGetChar) {
        bbox_print(PRINT_ERR, "BBOX_StringSwitchInt is failed, iGetChar= %d.\n", iGetChar);
        return RET_ERR;
    }

    /* ignore the feild that discribe equipment and node which are not needed. */
    iGetChar = BBOX_SkipDeviceAndNodeField(pstReadIO);
    if (RET_ERR == iGetChar) {
        bbox_print(PRINT_ERR, "BBOX_SkipDeviceAndNodeField is failed, iGetChar= %d.\n", iGetChar);
        return RET_ERR;
    }

    /* judge the next field is start with '[' or is end, if yes, mark it as a anonymity equipment. */
    iIsMappingAnonymousFlag = ((iGetChar == '\n') || (iGetChar == '['));
    if (BBOX_TRUE == iIsMappingAnonymousFlag) {
        pstSegmentMapping->iFlags |= PF_ANONYMOUS;
        /* judge where it is VDSO segment, if yes, mark it. */
        iRessult = BBOX_SetMappingVDSOFlag(&iGetChar, pstReadIO, pstSegmentMapping);
        if (RET_OK != iRessult) {
            bbox_print(PRINT_ERR, "BBOX_SetMappingVDSOFlag is failed, iRessult= %d.\n", iRessult);
            return RET_ERR;
        }

        return iGetChar;
    }

    /* judge if it is discribing someone equipment. */
    iRessult = BBOX_SetMappingDeviceFlag(&iGetChar, pstReadIO, pstSegmentMapping);
    if (RET_OK != iRessult) {
        bbox_print(PRINT_ERR, "BBOX_SetMappingDeviceFlag is failed, iRessult= %d.\n", iRessult);
        return RET_ERR;
    }

    /* judge if it has mapping file. */
    iIsMappedFile = (iGetChar == '/');

    if (BBOX_TRUE == iIsMappedFile) {
        iRessult = BBOX_SettingFileFlags(&iGetChar, pstReadIO, pstSegmentMapping);
        if (RET_OK != iRessult) {
            bbox_print(PRINT_ERR, "BBOX_SettingFileFlags is failed, iRessult= %d.\n", iRessult);
            return RET_ERR;
        }
    }

    return iGetChar;
}

/*
 * read /proc/self/maps and get count of line in which.
 * return  : iLineNum - line count of /proc/self/maps, it is not a negative.
 *           RET_ERR
 */
static int BBOX_GetVmMapsNum(void)
{
    int iFd = -1;
    int iLineNum = 0;
    int iCount = 0;
    ssize_t iReadSize = -1;
    char acBuff[BBOX_BUFF_LITTLE_SIZE];
    errno_t rc = EOK;
    BBOX_NOINTR(iFd = sys_open(THREAD_SELF_MAPS_FILE, O_RDONLY, 0));
    if (iFd < 0) {
        bbox_print(PRINT_ERR, "sys_open is failed, iFd= %d.\n", iFd);
        return RET_ERR;
    }

    bbox_print(PRINT_DBG, "Read file : /proc/self/Maps:\n");

    do {
        iReadSize = -1;
        rc = memset_s(acBuff, BBOX_BUFF_LITTLE_SIZE, 0, sizeof(acBuff));
        securec_check_c(rc, "\0", "\0");

        BBOX_NOINTR(iReadSize = sys_read(iFd, acBuff, sizeof(acBuff)));
        if (RET_ERR == iReadSize) {
            BBOX_NOINTR(sys_close(iFd));
            return RET_ERR;
        } else if (0 == iReadSize) {
            break;
        }

        bbox_print(PRINT_DBG, "%s", acBuff);

        for (iCount = 0; iCount < iReadSize; iCount++) {
            if ('\n' == acBuff[iCount]) {
                iLineNum++;
            }
        }
    } while (iReadSize);

    BBOX_NOINTR(sys_close(iFd));

    return iLineNum ? iLineNum : RET_ERR;
}

/*
 * fill address feild in struct BBOX_VM_MAPS when reading /proc/self/maps.
 * in      : struct BBOX_READ_FILE_IO *pstReadIO - pointer to file to be read.
 *           struct BBOX_VM_MAPS *pstSegmentMapping - pointer to discription of mapping segment structuer.
 * return  : int iGetChar - current character of file reading
 *           RET_ERR - read failed
 */
static char BBOX_FillMappingAddress(struct BBOX_READ_FILE_IO* pstReadIO, struct BBOX_VM_MAPS* pstSegmentMapping)
{
    int iGetChar = -1;

    if (NULL == pstReadIO || NULL == pstSegmentMapping) {
        bbox_print(PRINT_ERR,
            "BBOX_FillMappingAddress parameters is invalid: pstReadIO or pstSegmentMapping is NULL.\n");
        return RET_ERR;
    }

    /* read characters from file and convert it to interger until get '-'. 
	   This is start address and then ready to read end address. */
    iGetChar = BBOX_StringSwitchInt(pstReadIO, &(pstSegmentMapping->uiStartAddress));
    if ('-' == iGetChar) {
        /* read characters from file and convert it to interger until get '-'. This is end address. */
        iGetChar = BBOX_StringSwitchInt(pstReadIO, &(pstSegmentMapping->uiEndAddress));
        if (' ' != iGetChar) {
            bbox_print(PRINT_ERR, "BBOX_StringSwitchInt is failed, iGetChar= %d.\n", iGetChar);

            return RET_ERR;
        }
    } else {

        bbox_print(PRINT_ERR, "BBOX_StringSwitchInt is failed, iGetChar= %d.\n", iGetChar);

        return RET_ERR;
    }

    return (char)iGetChar;
}

/*
 * use blacklist items to split the segment, thus drop partal slice from core file.
 *      struct BBOX_WRITE_FDS *pstWriteFds : segment to be written into core file
 * return RET_OK if success else RET_ERR.
 */
static int BBOX_VmExecludeBlackList(struct BBOX_VM_MAPS *pstVmMappingSegment)
{
    void    *pStartAddress = (void *)(uintptr_t)pstVmMappingSegment->uiStartAddress;
    void    *pEndAddress = (void *)(uintptr_t)pstVmMappingSegment->uiEndAddress;
    void    *pBlackStartAddress = NULL;
    void    *pBlackEndAddress = NULL;
    size_t  uiPageSize = sys_sysconf(_SC_PAGESIZE);
    BBOX_BLACKLIST_STRU *pstBlackNode = NULL;
    struct BBOX_VM_MAPS *pstNextVmSegment = NULL;

    pstBlackNode = _BBOX_FindAddrInBlackList(pStartAddress, pEndAddress);
    if (pstBlackNode != NULL) {

        pBlackStartAddress = pstBlackNode->pBlackStartAddr;
        pBlackEndAddress = pstBlackNode->pBlackEndAddr;

        pstNextVmSegment = pstVmMappingSegment + 1;
        pstNextVmSegment->iFlags = pstVmMappingSegment->iFlags;

        if (pStartAddress < pBlackStartAddress) {
            pstVmMappingSegment->uiWriteSize= (size_t)((uintptr_t)pBlackStartAddress - (uintptr_t)pStartAddress);
            if ((pstVmMappingSegment->uiWriteSize) < uiPageSize) {
                pstVmMappingSegment->uiEndAddress =
                    (size_t)((uintptr_t)pBlackStartAddress + (pstVmMappingSegment->uiWriteSize % uiPageSize));
            } else {
                pstVmMappingSegment->uiEndAddress = (size_t)(uintptr_t)pBlackStartAddress;
            }

            pstVmMappingSegment->uiWriteSize= (size_t)((uintptr_t)pBlackStartAddress - (uintptr_t)pStartAddress);
            pstVmMappingSegment->uiStartAddress = (size_t)(uintptr_t)pStartAddress;
            pstVmMappingSegment->iIsRemoveFlags = BBOX_FALSE;
        } else {
            pstVmMappingSegment->iIsRemoveFlags = BBOX_TRUE;
        }

        if (pBlackEndAddress < pEndAddress) {
            pstNextVmSegment->uiWriteSize = (size_t)((uintptr_t)pEndAddress - (uintptr_t)pBlackEndAddress);
            if ((pstNextVmSegment->uiWriteSize) < uiPageSize) {
                pstNextVmSegment->uiStartAddress =
                    (size_t)((uintptr_t)pBlackEndAddress - (pstNextVmSegment->uiWriteSize % uiPageSize));
            } else {
                pstNextVmSegment->uiStartAddress = (size_t)(uintptr_t)pBlackEndAddress;
            }

            pstNextVmSegment->uiWriteSize = (size_t)((uintptr_t)pEndAddress - (uintptr_t)pBlackEndAddress);
            pstNextVmSegment->uiEndAddress = (size_t)(uintptr_t)pEndAddress;
            pstNextVmSegment->iIsRemoveFlags = BBOX_FALSE;
        } else {
            pstNextVmSegment->iIsRemoveFlags = BBOX_TRUE;
        }
    } else {
        /* do nothing */
    }

    bbox_print(PRINT_DBG, "execlude black list success.\n");
    return RET_OK;
}

/*
 * read /proc/self/maps and fill struct BBOX_VM_MAPS
 * in     : struct BBOX_VM_MAPS *pstVmMappingSegment - pointer to discription of mapping segment structure.
 *          int iSegmentNum - count of mapping segment in address space.
 * return RET_OK or RET_ERR.
 */
static int BBOX_GetMappingSegment(struct BBOX_VM_MAPS* pstSegmentMapping, int* piSegmentNum)
{
    int iGetChar = -1;
    int iResult = RET_ERR;
    int iNewVmTotal = 0;
    int iVmTotal = 0;
    struct BBOX_READ_FILE_IO stReadIO;
    struct BBOX_VM_MAPS* pstCurSegment = NULL;
    errno_t rc = EOK;
    if (NULL == pstSegmentMapping || NULL == piSegmentNum) {
        bbox_print(PRINT_ERR,
            "BBOX_GetMappingSegment parameters is invalid: " \
            "pstSegmentMapping or piSegmentNum is NULL.\n");
        return RET_ERR;
    }

    iVmTotal = *piSegmentNum;

    rc = memset_s(&stReadIO, sizeof(struct BBOX_READ_FILE_IO), 0, sizeof(struct BBOX_READ_FILE_IO));
    securec_check_c(rc, "\0", "\0");

    stReadIO.iFd = -1;
    stReadIO.pData = NULL;
    stReadIO.pEnd = NULL;

    BBOX_NOINTR(stReadIO.iFd = sys_open(THREAD_SELF_MAPS_FILE, O_RDONLY, 0));
    if (stReadIO.iFd < 0) {
        bbox_print(PRINT_ERR, "sys_open is failed, stReadIO.iFd = %d.\n", stReadIO.iFd);
        return RET_ERR;
    }

    pstCurSegment = pstSegmentMapping;
    while (iVmTotal) {
        bbox_print(PRINT_DBG, "iVmTotal = %d.\n", iVmTotal);

        /* get start address and end address of every segment in process address space. */
        iGetChar = BBOX_FillMappingAddress(&stReadIO, pstCurSegment);
        if (RET_ERR == iGetChar) {
            BBOX_NOINTR(sys_close(stReadIO.iFd));
            bbox_print(PRINT_ERR, "BBOX_FillMappingAddress is failed, iGetChar = %d.\n", iGetChar);
            return RET_ERR;
        }

        /* get jurisdiction flag and offset of every segment in process address space. */
        iGetChar = BBOX_FillMappingFlagsAndOffset(&stReadIO, pstCurSegment);
        if (RET_ERR == iGetChar) {
            BBOX_NOINTR(sys_close(stReadIO.iFd));
            bbox_print(PRINT_ERR, "BBOX_FillMappingFlagsAndOffset is failed, iGetChar = %d.\n", iGetChar);
            return RET_ERR;
        }

        if (iGetChar != '\n') {
            /* ignore other information and skip to the end of line for preparing to read next line. */
            iResult = BBOX_SkipToLineEnd(&stReadIO);
            if (RET_OK != iResult) {
                BBOX_NOINTR(sys_close(stReadIO.iFd));
                bbox_print(PRINT_ERR, "BBOX_SkipToLineEnd is failed, iResult = %d.\n", iResult);
                return RET_ERR;
            }
        }

        while (_BBOX_FindAddrInBlackList(
            (void *)(uintptr_t)pstCurSegment->uiStartAddress, (void *)(uintptr_t)pstCurSegment->uiEndAddress)) {

            iResult = BBOX_VmExecludeBlackList(pstCurSegment);
            if (RET_OK != iResult) {
                BBOX_NOINTR(sys_close(stReadIO.iFd));
                bbox_print(PRINT_ERR, "BBOX_VmExecludeBlackList is failed.\n");
                return RET_ERR;
            }

            iNewVmTotal++;
            pstCurSegment++;
        }

        pstCurSegment++;
        iVmTotal--;
    }

    (*piSegmentNum) = (*piSegmentNum) + iNewVmTotal;

    bbox_print(PRINT_TIP, "after BL, all segment num is %d.\n", *piSegmentNum);

    BBOX_NOINTR(sys_close(stReadIO.iFd));

    return RET_OK;
}

/*
 * judge if it should be written into core file for every segment in address space,
 * and calculate the size to be written in.
 * in      : struct BBOX_VM_MAPS *pstVmMappingSegment - pointer to mapping segment structure.
 *           int iSegmentNum - count of mapping segment in address space
 *           int *piValidSegmentNum - count of valid mapping segment in address space,
 *                                    which need to be written into core file.
 * return RET_OK or RET_ERR.
 */
static int BBOX_VmMappingSizeDump(struct BBOX_VM_MAPS* pstVmMappingSegment, int iSegmentNum, int* piValidSegmentNum)
{
    int iCount = 0;
    struct BBOX_VM_MAPS* pstVmMapping = NULL;

    if (NULL == pstVmMappingSegment || NULL == piValidSegmentNum || iSegmentNum <= 0) {
        bbox_print(PRINT_ERR,
            "BBOX_VmMappingSizeDump parameters is invalid: pstVmMappingSegment, " \
            "piValidSegmentNum or iSegmentNum is NULL.\n");

        return RET_ERR;
    }

    *piValidSegmentNum = iSegmentNum;

    bbox_print(PRINT_LOG, "write segment to core file:\n");

    for (iCount = 0; iCount < iSegmentNum; iCount++) {
        pstVmMapping = pstVmMappingSegment + iCount;

        if (BBOX_TRUE == pstVmMapping->iIsRemoveFlags) {
            pstVmMapping->uiWriteSize = 0;
            (*piValidSegmentNum)--;
            continue;
        }

        /* If the segment is a code segment or a non-anonymous segment that cannot be read or written,
           set the size written to the core file to 0, otherwise the size is calculated */
#if defined(__ARM_ARCH_5TE__) || (defined(__ARM_ARCH_7A__)) || (defined(__aarch64__))
        if ((pstVmMapping->iFlags & (PF_ANONYMOUS | PF_W | PF_R)) == 0 || (pstVmMapping->iFlags & PF_MAPPEDFILE)) {
#else
        if ((pstVmMapping->iFlags & (PF_ANONYMOUS | PF_W | PF_R)) == 0 || (pstVmMapping->iFlags & PF_X) ||
            (pstVmMapping->iFlags & PF_MAPPEDFILE)) {
#endif
            pstVmMapping->uiWriteSize =
                ((pstVmMapping->iFlags & PF_VDSO) ? (pstVmMapping->uiEndAddress - pstVmMapping->uiStartAddress) : 0);

        } else {
            pstVmMapping->uiWriteSize = pstVmMapping->uiEndAddress - pstVmMapping->uiStartAddress;

            bbox_print(PRINT_DBG, "Segment[%d]: set writesize = %zu.\n", iCount, pstVmMapping->uiWriteSize);
        }

        /* mark a segment not write into core file
           if it cannot be read, discribes equipment segment or segment size is 0 or vvar segement */
        if (((pstVmMapping->iFlags & PF_R) == 0) || pstVmMapping->uiStartAddress == pstVmMapping->uiEndAddress ||
            (pstVmMapping->iFlags & PF_VVAR) ||
            (pstVmMapping->iFlags & PF_DEVICE)) {
            pstVmMapping->uiWriteSize = 0;
            (*piValidSegmentNum)--;
            pstVmMapping->iIsRemoveFlags = BBOX_TRUE;
        }

        if (pstVmMapping->iIsRemoveFlags != BBOX_TRUE) {
            bbox_print(PRINT_DBG,
                "segment[%d]: %zu %zu %u\n",
                iCount,
                pstVmMapping->uiOffset,
                pstVmMapping->uiWriteSize,
                pstVmMapping->iFlags);
        }
    }

    return RET_OK;
}

/*
 * fill the structure, which should be written into core file and discribes mapping segment of process address space.
 * in      : struct BBOX_VM_MAPS *pstVmMappingSegment - pointer to mapping segment structure.
 *           int iSegmentNum - count of mapping segment in address space
 *           int *piValidSegmentNum - count of valid mapping segment in address space,
 *                                    which need to be written into core file.
 * return RET_OK or RET_ERR.
 */
static int BBOX_FillVmMappingInfo(struct BBOX_VM_MAPS* pstVmMappingSegment, int* piSegmentNum, int* piValidSegmentNum)
{
    int iResult = RET_ERR;

    if (NULL == pstVmMappingSegment || NULL == piValidSegmentNum || NULL == piSegmentNum) {
        bbox_print(PRINT_ERR,
            "BBOX_FillVmMappingInfo parameters is invalid: pstVmMappingSegment, " \
            "piValidSegmentNum or piSegmentNum is NULL.\n");
        return RET_ERR;
    }

    /* read /proc/self/maps and fill struct BBOX_VM_MAPS. */
    iResult = BBOX_GetMappingSegment(pstVmMappingSegment, piSegmentNum);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_GetMappingSegment is failed, iResult = %d.\n", iResult);
        return RET_ERR;
    }

    /* judge if it need to be written into core file and the size to be written in
       for every segment in process address space. */
    iResult = BBOX_VmMappingSizeDump(pstVmMappingSegment, *piSegmentNum, piValidSegmentNum);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_VmMappingSizeDump is failed, iResult = %d.\n", iResult);
        return RET_ERR;
    }

    bbox_print(PRINT_LOG, "Fill Vm mapping segment info success.\n");
    return RET_OK;
}

/*
 * read /proc/self/auxv and get count of auxv and VDSO address
 * in      : union BBOX_VM_VDSO *pstVmVDSO - pointer to structure that decribes VDSO segment.
 * return  : iAuxvNum - count of auxv, it is not a negative.
 *           RET_ERR
 */
static int BBOX_GetVDSOAndVmAuxvNum(union BBOX_VM_VDSO* pstVmVDSO)
{
    int iAuxvFd = -1;
    ssize_t iReadSize = RET_ERR;
    int iAuxvNum = 0;
    BBOX_AUXV_T stAuxv;
    errno_t rc = EOK;
    if (NULL == pstVmVDSO) {
        bbox_print(PRINT_ERR, "BBOX_GetVDSOAndVmAuxvNum parameters is invalid: pstVmVDSO is NULL.\n");
        return RET_ERR;
    }

    pstVmVDSO->pVDSOEhdr = NULL;

    /* read /proc/self/auxv, get count of Auxv written into core file and load address of VDSO. */
    BBOX_NOINTR(iAuxvFd = sys_open(THREAD_SELF_AUXV_FILE, O_RDONLY, 0));
    if (iAuxvFd < 0) {
        bbox_print(PRINT_ERR, "sys_open is failed, iAuxvFd = %d.\n", iAuxvFd);
        return RET_ERR;
    }

    do {
        iReadSize = RET_ERR;
        rc = memset_s(&stAuxv, sizeof(BBOX_AUXV_T), 0, sizeof(BBOX_AUXV_T));
        securec_check_c(rc, "\0", "\0");

        BBOX_NOINTR(iReadSize = sys_read(iAuxvFd, &stAuxv, sizeof(BBOX_AUXV_T)));
        if (RET_ERR == iReadSize) {
            bbox_print(PRINT_ERR, "sys_read is failed, iReadSize = %zd.\n", iReadSize);
            BBOX_NOINTR(sys_close(iAuxvFd));
            return RET_ERR;
        }
        if (iReadSize != sizeof(BBOX_AUXV_T)) {
            break;
        }

        iAuxvNum++;
        if (stAuxv.a_type == AT_SYSINFO_EHDR) {
            /* get VDSO load address from AT_SYSINFO_EHDR of Auxv. */
            pstVmVDSO->pVDSOEhdr = (BBOX_EHDR*)stAuxv.a_un.a_val;
        }
    } while (stAuxv.a_type != AT_NULL);

    BBOX_NOINTR(sys_close(iAuxvFd));

    return iAuxvNum;
}

/*
 * judge if the VDSO address we got is valid
 * in      : BBOX_EHDR *pstVDSOEhdr - pointer to elf header of VDSO.
 *           size_t uiStartAddress - start address of a segment in address space.
 *           size_t uiEndAddress - end address of a segment in address space.
 * return  : RET_OK - success
 *           RET_BBOX_VDSO_INVALID - failed
 */
static int BBOX_CheakVDSOEhdr(BBOX_EHDR* pstVDSOEhdr, size_t uiStartAddress, size_t uiEndAddress)
{
    int iCount = 0;
    BBOX_PHDR* pstVDSOPhdr = NULL;

    if (NULL == pstVDSOEhdr) {
        bbox_print(PRINT_ERR, "BBOX_CheakVDSOEhdr parameters is invalid: pstVDSOEhdr is NULL.\n");

        return RET_ERR;
    }

    const size_t uiEhdrAddress = (size_t)pstVDSOEhdr;

    if (uiEhdrAddress & (sizeof(size_t) - 1)) {
        /* not aligned properly */
        bbox_print(PRINT_ERR, "VDSO is invalid: not aligned properly.\n");
        return RET_BBOX_VDSO_INVALID;
    }

    if (uiEndAddress <= uiEhdrAddress + sizeof(BBOX_EHDR)) {
        /* pstVDSOEhdr has Incomplete head */
        bbox_print(PRINT_ERR, "VDSO head is invalid: pstVDSOEhdr has Incomplete head.\n");
        return RET_BBOX_VDSO_INVALID;
    }

    if (pstVDSOEhdr->e_phoff & (sizeof(size_t) - 1)) {
        /* not aligned properly */
        bbox_print(PRINT_ERR, "VDSO Ehdr is invalid: not aligned properly.\n");
        return RET_BBOX_VDSO_INVALID;
    }

    pstVDSOPhdr = (BBOX_PHDR*)(uiEhdrAddress + pstVDSOEhdr->e_phoff);
    if ((size_t)pstVDSOPhdr <= uiStartAddress || uiEndAddress <= (size_t)(pstVDSOPhdr + pstVDSOEhdr->e_phnum)) {
        /* VDSOPhdr is incompleted */
        bbox_print(PRINT_ERR, "VDSO Phdr is invalid: VDSOPhdr is incompleted.\n");
        return RET_BBOX_VDSO_INVALID;
    }

    if (pstVDSOPhdr[0].p_type != PT_LOAD || pstVDSOPhdr[0].p_vaddr != uiStartAddress ||
        pstVDSOPhdr[0].p_vaddr + pstVDSOPhdr[0].p_memsz >= uiEndAddress) {
        bbox_print(PRINT_ERR, "VDSO Phdr is invalid.\n");
        return RET_BBOX_VDSO_INVALID;
    }

    for (iCount = 1; iCount < pstVDSOEhdr->e_phnum; iCount++) {
        /* VDSO has multiple PT_LOAD segments. */
        if (pstVDSOPhdr[iCount].p_type == PT_LOAD) {
            bbox_print(PRINT_ERR, "VDSO Phdr is invalid: VDSO has multiple PT_LOAD segments.\n");
            return RET_BBOX_VDSO_INVALID;
        }

        /* VDSOPhdr is not aligned properly. */
        if (pstVDSOPhdr[0].p_vaddr & (sizeof(size_t) - 1)) {            
            bbox_print(PRINT_ERR, "VDSO Phdr is invalid: VDSOPhdr is not aligned properly.\n");
            return RET_BBOX_VDSO_INVALID;
        }

        /* Phdr data range is out of bounds */
        if (pstVDSOPhdr[iCount].p_vaddr != uiStartAddress ||
            (pstVDSOPhdr[iCount].p_vaddr + pstVDSOPhdr[iCount].p_memsz >= uiEndAddress)) {
            bbox_print(PRINT_ERR, "VDSO Phdr is invalid: Phdr data range is out of bounds.\n");
            return RET_BBOX_VDSO_INVALID;
        }
    }

    return RET_OK;
}

/*
 * judge if the VDSO address we got is valid and if it need to be written into core file.
 * in     : struct BBOX_VM_MAPS *pstVmMappingSegment - pointer to mapping segment structure.
 *          int iSegmentNum - count of mapping segment in address space.
 *          union BBOX_VM_VDSO *pstVmVDSO - pointer to VDSO structure.
 * return RET_OK or RET_ERR
 */
static int BBOX_CheckVDSO(struct BBOX_VM_MAPS* pstVmMappingSegment, int iSegmentNum, union BBOX_VM_VDSO* pstVmVDSO)
{
    int iCount = 0;
    int iResult = RET_ERR;
    struct BBOX_VM_MAPS* pstVmMappingTemp = NULL;

    if (NULL == pstVmMappingSegment || NULL == pstVmVDSO || iSegmentNum <= 0) {
        bbox_print(PRINT_ERR,
            "BBOX_CheckVDSO parameters is invalid: pstVmMappingSegment or "\
            "pstVmVDSO is NULL, iSegmentNum = %d.\n",
            iSegmentNum);

        return RET_ERR;
    }

    for (iCount = 0; iCount < iSegmentNum; iCount++) {
        pstVmMappingTemp = (pstVmMappingSegment + iCount);
        if (BBOX_FALSE == pstVmMappingTemp->iIsRemoveFlags) {
            /* traverse segment, and judge if VDSO address is in segment scope. */
            if ((pstVmMappingTemp->iFlags & PF_R) && (pstVmMappingTemp->uiStartAddress <= pstVmVDSO->uiVDSOAddress) &&
                (pstVmMappingTemp->uiEndAddress > pstVmVDSO->uiVDSOAddress)) {
                /* judge if VDSO address is valid. */
                iResult = BBOX_CheakVDSOEhdr(
                    pstVmVDSO->pVDSOEhdr, pstVmMappingTemp->uiStartAddress, pstVmMappingTemp->uiEndAddress);
                if (RET_BBOX_VDSO_INVALID == iResult) {
                    pstVmVDSO->pVDSOEhdr = NULL;
                } else if (RET_ERR == iResult) {
                    bbox_print(PRINT_ERR, "BBOX_CheakVDSOEhdr is failed, iResult = %d.\n", iResult);

                    return RET_ERR;
                }

                break;
            }
        }
    }

    if (iCount == iSegmentNum) {
        pstVmVDSO->uiVDSOAddress = 0;
    }

    return RET_OK;
}

/*
 * get count of VDSO writen into core file.
 * in      : union BBOX_VM_VDSO *pstVmVDSO pointer to VDSO union structure.
 * return  : iExtraPhdrNum - result
 *           RET_ERR - failed
 */
static int BBOX_GetExtraPhdrNum(union BBOX_VM_VDSO* pstVmVDSO)
{
    int iExtraPhdrNum = 0;
    int iCount = 0;
    BBOX_PHDR* pVDSOPhdr = NULL;

    if (NULL == pstVmVDSO) {
        bbox_print(PRINT_ERR, "BBOX_GetExtraPhdrNum parameters is invalid: pstVmVDSO is NULL.\n");

        return RET_ERR;
    } else {
        if (0 == pstVmVDSO->uiVDSOAddress) {
            return iExtraPhdrNum;
        }

        /* if segment in VDSO is not PT_LOAD, add it into core file. */
        pVDSOPhdr = (BBOX_PHDR*)(pstVmVDSO->uiVDSOAddress + pstVmVDSO->pVDSOEhdr->e_phoff);
        for (iCount = 0; iCount < pstVmVDSO->pVDSOEhdr->e_phnum; iCount++) {
            if (pVDSOPhdr[iCount].p_type != PT_LOAD) {
                iExtraPhdrNum++;
            }
        }
    }

    bbox_print(PRINT_DBG, "Extra Phdr Num: iExtraPhdrNum = %d.\n", iExtraPhdrNum);

    return iExtraPhdrNum;
}

/*
 * fill structure that discribe VDSO
 * in      : int *piAuxvNum - count of Auxv
 *           int *piExtraPhdrNum - count of VDSO written into core file
 *           int iSegmentNum - count of mapping segment in address space
 *           union BBOX_VM_VDSO *pstVmVDSO - pointer to structure that discribe VDSO segment.
 *           struct BBOX_VM_MAPS *pstVmMappingSegment - pointer to structure that discribes mapping segment.
 * return RET_OK or RET_ERR.
 */
static int BBOX_FillVDSOInfo(int* piAuxvNum, int* piExtraPhdrNum, int iSegmentNum, union BBOX_VM_VDSO* pstVmVDSO,
    struct BBOX_VM_MAPS* pstVmMappingSegment)
{
    int iResult = RET_ERR;

    if (NULL == piAuxvNum || NULL == piExtraPhdrNum || NULL == pstVmVDSO || NULL == pstVmMappingSegment ||
        iSegmentNum <= 0) {
        bbox_print(PRINT_ERR,
            "BBOX_FillVDSOInfo parameters is invalid: piAuxvNum, piExtraPhdrNum," \
            "pstVmVDSO or pstVmMappingSegment is NULL, iSegmentNum = %d.\n",
            iSegmentNum);

        return RET_ERR;
    }

    /* get VDSO, load address of VDSO segment and count of Auxv be written into core file. */
    iResult = BBOX_GetVDSOAndVmAuxvNum(pstVmVDSO);
    if (RET_ERR == iResult) {
        bbox_print(PRINT_ERR, "BBOX_GetVDSOAndVmAuxvNum is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }
    *piAuxvNum = iResult;

    /* check if VDSO is valid */
    iResult = BBOX_CheckVDSO(pstVmMappingSegment, iSegmentNum, pstVmVDSO);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_CheckVDSO is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    /* get the number of additional segments, which means that add VDSO into segment. */
    iResult = BBOX_GetExtraPhdrNum(pstVmVDSO);
    if (RET_ERR == iResult) {
        bbox_print(PRINT_ERR, "BBOX_GetExtraPhdrNum is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    (*piExtraPhdrNum) += iResult;

    bbox_print(PRINT_LOG, "Fill VDSO info success.\n");
    return RET_OK;
}

/*
 * get the process run time and write it into structure.
 * in      : char *pszReadFile - store the character array read from file.
 *           int iReadSize - count of character in array.
 *           struct BBOX_ELF_PRPSSTATUS *pstPrPsStatus - pointer to structure that store process status information.
 * return RET_OK or RET_ERR.
 */
static int BBOX_SetPsStatusTime(char* pszReadFile, int iReadSize, struct BBOX_ELF_PRPSSTATUS* pstPrPsStatus)
{
    int iFlag = 1;
    const int iUTimePos = 13;      /* the 13th is User time */
    const int iSTimePos = 14;      /* the 14th is System time */
    const int iCUTimePos = 15;     /* the 15th is Cumulative user time */
    const int iCSTimePos = 16;     /* the 16th is Cumulative system time */
    const int iPendingSigPos = 30; /* the 30th is Pending signals */
    const int iHeldSigPos = 31;    /* the 31th is Held signals */
    unsigned int uCount = 0;
    unsigned int uItemNum = 0;
    char* pcSignalstr = 0;
    char* pStatItem[BBOX_STAT_ITEM_NUM];

    if (NULL == pszReadFile || NULL == pstPrPsStatus || iReadSize <= 0) {
        bbox_print(PRINT_ERR,
            "BBOX_SetPsStatusTime parameters is invalid: pszReadFile or pstPrPsStatus is NULL, iReadSize = %d.\n",
            iReadSize);

        return RET_ERR;
    }

    for (uCount = 0; uCount < (unsigned int)iReadSize; uCount++) {
        /* use pointer to record the string be divided. */
        if (iFlag) {
            pStatItem[uItemNum++] = (pszReadFile + uCount);
            iFlag = 0;
        }

        /* convert ' ' to '\0' */
        if (pszReadFile[uCount] == ' ') {
            pszReadFile[uCount] = '\0';
            iFlag = 1;
        }
    }

    /* the 13th is User time */
    bbox_print(PRINT_DBG, "User Time : %s\n", pStatItem[iUTimePos]);
    (void)BBOX_StringToTime(pStatItem[iUTimePos], &(pstPrPsStatus->stUserTime));

    /* the 14th is System time */
    bbox_print(PRINT_DBG, "System Time : %s\n", pStatItem[iSTimePos]);
    (void)BBOX_StringToTime(pStatItem[iSTimePos], &(pstPrPsStatus->stSystemTime));

    /* the 15th is Cumulative user time */
    bbox_print(PRINT_DBG, "Cumulative user Time : %s\n", pStatItem[iCUTimePos]);
    (void)BBOX_StringToTime(pStatItem[iCUTimePos], &(pstPrPsStatus->stCumulativeUserTime));

    /* the 16th is Cumulative system time */
    bbox_print(PRINT_DBG, "Cumulative system : %s\n", pStatItem[iCSTimePos]);
    (void)BBOX_StringToTime(pStatItem[iCSTimePos], &(pstPrPsStatus->stCumulativeSystemTime));

    /* the 30th is Pending signals */
    pcSignalstr = pStatItem[iPendingSigPos];
    pstPrPsStatus->ulSigPend = 0;
    while (*pcSignalstr != '\0') {
        pstPrPsStatus->ulSigPend = 10 * pstPrPsStatus->ulSigPend + (*pcSignalstr - '0');
        pcSignalstr++;
    }

    /* the 31th is Held signals */
    pcSignalstr = pStatItem[iHeldSigPos];
    pstPrPsStatus->ulSigHold = 0;
    while (*pcSignalstr != '\0') {
        pstPrPsStatus->ulSigHold = 10 * pstPrPsStatus->ulSigHold + (*pcSignalstr - '0');
        pcSignalstr++;
    }

    return RET_OK;
}

/*
 * get the process run time
 * in      : struct BBOX_ELF_PRPSSTATUS *pstPrPsStatus - pointer to structure that store process status information.
 * return RET_OK or RET_ERR.
 */
static int BBOX_FillPsStatusTimeInfo(struct BBOX_ELF_PRPSSTATUS* pstPrPsStatus)
{
    ssize_t iReadSize = RET_ERR;
    int iStatFileFd = -1;
    int iResult = RET_ERR;
    char szReadFile[BBOX_BUFF_LITTLE_SIZE];
    errno_t rc = EOK;
    if (NULL == pstPrPsStatus) {
        bbox_print(PRINT_ERR, "BBOX_FillPsStatusTimeInfo parameters is invalid: pstPrPsStatus is NULL.\n");

        return RET_ERR;
    }

    rc = memset_s(szReadFile, sizeof(szReadFile), 0, sizeof(szReadFile));
    securec_check_c(rc, "\0", "\0");

    /* read /proc/self/stat */
    BBOX_NOINTR(iStatFileFd = sys_open(THREAD_SELF_STAT_FILE, O_RDONLY, 0));
    if (iStatFileFd < 0) {
        bbox_print(PRINT_ERR, "sys_open is failed iStatFileFd = %d.\n", iStatFileFd);

        return RET_ERR;
    }

    do {
        iReadSize = RET_ERR;
        rc = memset_s(szReadFile, sizeof(szReadFile), 0, sizeof(szReadFile));
        securec_check_c(rc, "\0", "\0");

        BBOX_NOINTR(iReadSize = sys_read(iStatFileFd, szReadFile, sizeof(szReadFile)));
        if (iReadSize > 0) {
            iResult = BBOX_SetPsStatusTime(szReadFile, iReadSize, pstPrPsStatus);
            if (RET_OK != iResult) {
                bbox_print(PRINT_ERR, "BBOX_SetPsStatusTime is failed, iResult = %d.\n", iResult);
                BBOX_NOINTR(sys_close(iStatFileFd));
                return RET_ERR;
            }
        } else if (iReadSize < 0) {
            bbox_print(PRINT_ERR, "failed to read status file, iResult = %zd.\n", iReadSize);
            BBOX_NOINTR(sys_close(iStatFileFd));
            return RET_ERR;
        }
    } while (iReadSize > 0);

    BBOX_NOINTR(sys_close(iStatFileFd));

    return RET_OK;
}

/*
 * get register information of parent process, get user data structure information.
 * in      : struct BBOX_ELF_PRPSINFO *pstPrPsInfo - pointer to structure that store process status information.
 *           pid_t tMainPid - main process pid.
 * return RET_OK or RET_ERR.
 */
static int BBOX_FillPrPsInfo(struct BBOX_ELF_PRPSINFO* pstPrPsInfo, pid_t tMainPid)
{
    char szBuff[BBOX_BUFF_SIZE];

    ssize_t iReadSize = 0;
    ssize_t iLen = 0;
    int iCommandLineFileFd = -1;
    char* pExePathName = szBuff;
    char* pTemp = NULL;
    errno_t rc = EOK;
    if (NULL == pstPrPsInfo) {
        bbox_print(PRINT_ERR, "BBOX_FillPrPsInfo parameters is invalid: pstPrPsInfo is NULL.\n");

        return RET_ERR;
    }

    rc = memset_s(pstPrPsInfo, sizeof(struct BBOX_ELF_PRPSINFO), 0, sizeof(struct BBOX_ELF_PRPSINFO));
    securec_check_c(rc, "\0", "\0");

    pstPrPsInfo->cSname = 'R';
    pstPrPsInfo->cNice = (signed char)sys_getpriority(PRIO_PROCESS, 0);
#if (defined(__x86_64__)) || (defined(__aarch64__))
    pstPrPsInfo->tUid = (uint32_t)sys_geteuid();
    pstPrPsInfo->tGid = (uint32_t)sys_getegid();
#elif (defined(__i386__)) || (defined(__ARM_ARCH_5TE__)) || (defined(__ARM_ARCH_7A__))
    pstPrPsInfo->tUid = (uint16_t)sys_geteuid();
    pstPrPsInfo->tGid = (uint16_t)sys_getegid();
#endif
    pstPrPsInfo->tpid = tMainPid;
    pstPrPsInfo->tPpid = sys_getppid();
    pstPrPsInfo->tPgrp = sys_getpgrp();
    pstPrPsInfo->tSid = sys_getsid(0);

    rc = memset_s(szBuff, sizeof(szBuff), 0, sizeof(szBuff));
    securec_check_c(rc, "\0", "\0");

    iReadSize = sys_readlink(THREAD_SELF_EXE_FILE, szBuff, sizeof(szBuff));
    iLen = 0;
    for (pTemp = szBuff; (*pTemp != '\000') && ((iReadSize--) > 0); pTemp++) {
        /* get the command name of the program to run (/bin/bash --> bash) */
        if (*pTemp == '/') {
            pExePathName = pTemp + 1;
            iLen = 0;
        } else {
            iLen++;
        }
    }
    rc = memcpy_s(pstPrPsInfo->cFname,
        sizeof(pstPrPsInfo->cFname),
        pExePathName,
        (iLen > (ssize_t)sizeof(pstPrPsInfo->cFname) ? sizeof(pstPrPsInfo->cFname) : iLen));
    securec_check_c(rc, "\0", "\0");

    /* read /proc/self/command, get parameter list of command. */
    BBOX_NO_INTR(iCommandLineFileFd = sys_open(THREAD_SELF_COMMAND_LINE_FILE, O_RDONLY, 0));
    if (iCommandLineFileFd < 0) {
        bbox_print(PRINT_ERR, "sys_open is failed: iCommandLineFileFd = %d.\n", iCommandLineFileFd);

        return RET_ERR;
    }

    BBOX_NO_INTR(iReadSize = sys_read(iCommandLineFileFd, pstPrPsInfo->cPsargs, sizeof(pstPrPsInfo->cPsargs)));
    if (iReadSize < 0) {
        BBOX_NO_INTR(sys_close(iCommandLineFileFd));
        return RET_ERR;
    }

    for (pTemp = pstPrPsInfo->cPsargs; (iReadSize--) > 0; pTemp++) {
        /* convert '\0' to ' ' so that all of it can be print out one time. */
        if (*pTemp == '\000') {
            *pTemp = ' ';
        }
    }

    BBOX_NO_INTR(sys_close(iCommandLineFileFd));

    bbox_print(PRINT_LOG, "Fill Prpsinfo Info success.\n");
    return RET_OK;
}

/*
 * get process register content.
 * in     : Frame *pFrame - pointer for writing core file.
 *          struct BBOX_ELF_NOTE_INFO *pstNoteInfo - pointer to structure of note segment distription.
 *          pid_t *ptPids - pointer to process id array.
 *          int iSegmentNum - count of mapping segment in address space.
 *          int *piPhdrSum - sum count of mapping segment and VDSO segment in core file.
 *          int iThreadNum - size of process array.
 * return RET_OK or RET_ERR.
 */
static int BBOX_FillPrPsStatusRegs(Frame* pFrame, struct BBOX_ELF_NOTE_INFO* pstNoteInfo, pid_t* ptPids, int iThreadNum)
{
    char acBuff[BBOX_BUFF_LITTLE_SIZE];
    unsigned int uCount = 0;
    errno_t rc = EOK;

    if (NULL == pFrame || NULL == pstNoteInfo || NULL == ptPids || iThreadNum <= 0) {
        bbox_print(PRINT_ERR,
            "BBOX_FillPrPsStatusRegs parameters is invalid: pFrame, pstNoteInfo or " \
            "ptPids is NULL, iThreadNum = %d.\n",
            iThreadNum);

        return RET_ERR;
    }

    struct BBOX_THREAD_NOTE_INFO* pstThreadNoteInfo = pstNoteInfo->pstThreadNoteInfo;

    for (uCount = 0; uCount < (unsigned int)iThreadNum; uCount++) {
        rc = memset_s(acBuff, BBOX_BUFF_LITTLE_SIZE, 0xFF, sizeof(acBuff));
        securec_check_c(rc, "\0", "\0");

#if defined(__aarch64__)
        /* get cpu register information of pid[i], run if err, try best to create core file. */
        void* pregset = (void*)NT_PRSTATUS;
        struct iovec io_vec;

        io_vec.iov_base = acBuff;
        io_vec.iov_len = sizeof(struct CPURegs);
        if (RET_OK == sys_ptrace(PTRACE_GETREGSET, ptPids[uCount], pregset, &io_vec)) {
            rc = memcpy_s(&((pstThreadNoteInfo + uCount)->stPrpsstatus.stRegisters),
                sizeof(struct CPURegs),
                acBuff,
                sizeof(struct CPURegs));
            securec_check_c(rc, "\0", "\0");
            rc = memset_s(acBuff, sizeof(acBuff), 0xFF, sizeof(acBuff));
            securec_check_c(rc, "\0", "\0");
        }
#else
        /* get cpu register information of pid[i], run if err, try best to create core file. */
        if (RET_OK == sys_ptrace(PTRACE_GETREGS, ptPids[uCount], acBuff, acBuff)) {
            rc = memcpy_s(&((pstThreadNoteInfo + uCount)->stPrpsstatus.stRegisters),
                sizeof(struct CPURegs),
                acBuff,
                sizeof(struct CPURegs));
            securec_check_c(rc, "\0", "\0");

            if (ptPids[uCount] == pstNoteInfo->tMainPid) {
                SET_FRAME(*(Frame*)pFrame, (pstThreadNoteInfo + uCount)->stPrpsstatus.stRegisters);
            }

            rc = memset_s(acBuff, BBOX_BUFF_LITTLE_SIZE, 0xFF, sizeof(acBuff));
            securec_check_c(rc, "\0", "\0");
        }

        /* get fpu register information of pid[i], run if err, try best to create core file. */
        if (RET_OK == sys_ptrace(PTRACE_GETFPREGS, ptPids[uCount], acBuff, acBuff)) {
            rc = memcpy_s(&((pstThreadNoteInfo + uCount)->stFpRegisters),
                sizeof(struct BBOX_FPREGSET),
                acBuff,
                sizeof(struct BBOX_FPREGSET));
            securec_check_c(rc, "\0", "\0");
            rc = memset_s(acBuff, BBOX_BUFF_LITTLE_SIZE, 0xFF, sizeof(acBuff));
            securec_check_c(rc, "\0", "\0");
        }
#endif

#if (defined(__i386__))

        /* get sse register information of pid[i], run if err, try best to create core file. */
        if (RET_OK == sys_ptrace(PTRACE_GETFPXREGS, ptPids[uCount], acBuff, acBuff)) {
            rc = memcpy_s(&((pstThreadNoteInfo + uCount)->stFpxRegisters),
                sizeof(struct BBOX_FPXREGSET),
                acBuff,
                sizeof(struct BBOX_FPXREGSET));
            securec_check_c(rc, "\0", "\0");
            pstNoteInfo->iFpxRegistersFlag = BBOX_TRUE;
        } else {
            pstNoteInfo->iFpxRegistersFlag = BBOX_FALSE;
        }
#else

        /* sse register information is stored in sse structure in x86-64. */
        pstNoteInfo->iFpxRegistersFlag = BBOX_FALSE;
#endif

        (pstThreadNoteInfo + uCount)->stPrpsstatus.tpid = ptPids[uCount];
    }

    return RET_OK;
}

/*
 * get user data structure information.
 * in       : struct BBOX_CORE_USER *pstCoreUser - pointer to strcuture that store user data. 
 *            struct CPURegs *pstThreadRegs - struct pointer that store parent process register information.
 *            pid_t *ptPids - pointer to process id array.
 * return RET_OK or RET_ERR.
 */
static int BBOX_GetParentRegs(struct BBOX_CORE_USER* pstCoreUser, struct CPURegs* pstThreadRegs, pid_t* ptPids)
{
    int iCount = 0;

    if (NULL == pstCoreUser || NULL == pstThreadRegs || NULL == ptPids) {
        bbox_print(PRINT_ERR,
            "BBOX_GetParentRegs parameters is invalid: pstCoreUser, " \
            "pstThreadRegs or ptPids is NULL.\n");

        return RET_ERR;
    }

    for (iCount = 0; iCount < (int)(sizeof(struct BBOX_CORE_USER) / sizeof(int)); iCount++) {
        /* get register information of parent process and copy it into user data structure,
           run if err, try best to create core file. */
        (void)sys_ptrace(
            PTRACE_PEEKUSER, ptPids[0], (void*)(iCount * sizeof(int)), ((char*)pstCoreUser) + iCount * sizeof(int));
    }

    errno_t rc = memcpy_s(&(pstCoreUser->stRegisters), sizeof(struct CPURegs), pstThreadRegs, sizeof(struct CPURegs));
    securec_check_c(rc, "\0", "\0");

    return RET_OK;
}

/*
 * fill note segment structure information of core file.
 * in     : Frame *pFrame - pointer of backstack information
 *          struct BBOX_ELF_NOTE_INFO *pstNoteInfo - pointer to execute note structure.
 *          pid_t *ptPids - pointer to process id.
 *          int iAuxvNum - count of Auxv that need to be written into core file.
 * return RET_OK or RET_ERR.
 */
static int BBOX_FillNoteInfo(Frame* pFrame, struct BBOX_ELF_NOTE_INFO* pstNoteInfo, pid_t* ptPids, int iAuxvNum)
{
    unsigned int uCount = 0;
    int iResult = -1;
    struct BBOX_ELF_PRPSSTATUS stTempPrPsStatus;
    errno_t rc = EOK;
    if (NULL == pFrame || NULL == pstNoteInfo || NULL == ptPids || iAuxvNum < 0) {
        bbox_print(PRINT_ERR,
            "BBOX_FillNoteInfo parameters is invalid: pFrame, "\
             "pstNoteInfo or ptPids is NULL, iAuxvNum = %d.\n",
            iAuxvNum);

        return RET_ERR;
    }

    pstNoteInfo->iAuxvNoteInfoNum = iAuxvNum;
    rc = memset_s(&(stTempPrPsStatus), sizeof(struct BBOX_ELF_PRPSSTATUS), 0, sizeof(struct BBOX_ELF_PRPSSTATUS));
    securec_check_c(rc, "\0", "\0");

    rc = memset_s(&(pstNoteInfo->stCoreUser), sizeof(struct BBOX_CORE_USER), 0, sizeof(struct BBOX_CORE_USER));
    securec_check_c(rc, "\0", "\0");

    rc = memset_s(&(pstNoteInfo->stPrpsinfo), sizeof(struct BBOX_ELF_PRPSINFO), 0, sizeof(struct BBOX_ELF_PRPSINFO));
    securec_check_c(rc, "\0", "\0");

    struct BBOX_ELF_PRPSINFO* pstPrPsInfo = &(pstNoteInfo->stPrpsinfo);
    struct BBOX_THREAD_NOTE_INFO* pstThreadPrpsstatus = pstNoteInfo->pstThreadNoteInfo;
    int iThreadNum = pstNoteInfo->iThreadNoteInfoNum;

    /* get run status, priority, group id, parent process id of a process
       and record them into struct BBOX_ELF_PRPSINFO. */
    iResult = BBOX_FillPrPsInfo(pstPrPsInfo, pstNoteInfo->tMainPid);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_FillPrPsInfo is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    /* get register, FPU and SSE information of process and record them into struct BBOX_THREAD_NOTE_INFO. */
    iResult = BBOX_FillPrPsStatusRegs(pFrame, pstNoteInfo, ptPids, iThreadNum);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_FillPrPsStatusRegs is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    /* get user data structure information and record it into struct BBOX_CORE_USER. */
    iResult =
        BBOX_GetParentRegs(&(pstNoteInfo->stCoreUser), &(pstThreadPrpsstatus[0].stPrpsstatus.stRegisters), ptPids);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_GetParentRegs is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    /* get process time information and record it into struct BBOX_ELF_PRPSSTATUS. */
    iResult = BBOX_FillPsStatusTimeInfo(&stTempPrPsStatus);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_FillPsStatusTimeInfo is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    for (uCount = 0; uCount < (unsigned int)iThreadNum; uCount++) {
        (pstThreadPrpsstatus + uCount)->stPrpsstatus.tPpid = pstNoteInfo->stPrpsinfo.tPpid;
        (pstThreadPrpsstatus + uCount)->stPrpsstatus.tPgrp = pstNoteInfo->stPrpsinfo.tPgrp;
        (pstThreadPrpsstatus + uCount)->stPrpsstatus.tSid = pstNoteInfo->stPrpsinfo.tSid;
        (pstThreadPrpsstatus + uCount)->stPrpsstatus.tFpvalid = BBOX_TRUE;

        (pstThreadPrpsstatus + uCount)->stPrpsstatus.stUserTime.lTvSec = stTempPrPsStatus.stUserTime.lTvSec;
        (pstThreadPrpsstatus + uCount)->stPrpsstatus.stUserTime.lTvMicroSec = stTempPrPsStatus.stUserTime.lTvMicroSec;

        (pstThreadPrpsstatus + uCount)->stPrpsstatus.stSystemTime.lTvSec = stTempPrPsStatus.stSystemTime.lTvSec;
        (pstThreadPrpsstatus + uCount)->stPrpsstatus.stSystemTime.lTvMicroSec =
            stTempPrPsStatus.stSystemTime.lTvMicroSec;

        (pstThreadPrpsstatus + uCount)->stPrpsstatus.stCumulativeUserTime.lTvSec =
            stTempPrPsStatus.stCumulativeUserTime.lTvSec;
        (pstThreadPrpsstatus + uCount)->stPrpsstatus.stCumulativeUserTime.lTvMicroSec =
            stTempPrPsStatus.stCumulativeUserTime.lTvMicroSec;

        (pstThreadPrpsstatus + uCount)->stPrpsstatus.stCumulativeSystemTime.lTvSec =
            stTempPrPsStatus.stCumulativeSystemTime.lTvSec;
        (pstThreadPrpsstatus + uCount)->stPrpsstatus.stCumulativeSystemTime.lTvMicroSec =
            stTempPrPsStatus.stCumulativeSystemTime.lTvMicroSec;
    }

    return RET_OK;
}

/*
 * create symbol table
 * in      : char *pBuffer - buffer
 *           unsigned int uiBufLen - buffer size
 * return RET_OK or RET_ERR.
 */
int BBOX_GenerateStrTab(char* pBuffer, unsigned int uiBufLen)
{
    int iResult = 0;
    unsigned int uCount = 0;
    unsigned int uiAllStringSz = 0;

    /* string symbol table */
    char* pacShName[BBOX_SECTION_NUM] = {BBOX_ADDON_INFO, BBOX_LOG, BBOX_STR_TAB};

    /* concatenate the above strings into the buffer, take care '\0'. */
    for (uCount = 0; uCount < BBOX_SECTION_NUM; uCount++) {
        iResult = bbox_snprintf(pBuffer, uiBufLen, pacShName[uCount]);
        if (iResult <= 0 || iResult > (int)uiBufLen) {
            bbox_print(PRINT_ERR, "bbox_snprintf is failed, errno = %d.\n", errno);

            return RET_ERR;
        }

        pBuffer += iResult;
        uiBufLen -= iResult;
        uiAllStringSz += iResult;
    }

    return uiAllStringSz;
}

/*
 * get content that be written into section.
 * return RET_OK or RET_ERR.
 */
void BBOX_FillSectionInfo(void)
{
    int iResult = RET_ERR;
    errno_t rc = EOK;

    /* create symbol table, if occur err, don't return ,run and try best to create core file. */
    iResult = BBOX_GenerateStrTab(g_acBboxStrTabInfo, sizeof(g_acBboxStrTabInfo));
    if (iResult < 0) {
        bbox_print(PRINT_ERR, "_BBOX_GenerateStrTab is failed.\n");
    }

    bbox_print(PRINT_LOG, "Generate section string table successful.\n");

    /* Get system information, continue if failed, try best to create core file. */
    iResult = _BBOX_GetAddonInfo(g_acBboxAddonInfo, sizeof(g_acBboxAddonInfo));
    if (iResult < 0) {
        bbox_print(PRINT_ERR, "_BBOX_GetAddonInfo is failed.\n");
    }

    bbox_print(PRINT_LOG, "Generate addition system info successful.\n");

    g_stElfSectionInfo.uiSectionNum = BBOX_SECTION_NUM;
    g_stElfSectionInfo.pstSection = g_stSectionInfo;

    /* BBOX_ADDONINFO */
    g_stSectionInfo[0].uiSectionType = SHT_NOTE;
    g_stSectionInfo[0].pacSectionDesc = g_acBboxAddonInfo;
    g_stSectionInfo[0].uiSectionDescSize = sizeof(g_acBboxAddonInfo);
    g_stSectionInfo[0].uiSectionNameSize = sizeof(BBOX_ADDON_INFO);
    rc = strncpy_s(
        g_stSectionInfo[0].acSectionName, BBOX_SECTION_NAME_LEN, BBOX_ADDON_INFO, g_stSectionInfo[0].uiSectionNameSize);
    securec_check_c(rc, "\0", "\0");

    /* BBOX_LOG */
    g_stSectionInfo[1].uiSectionType = SHT_NOTE;
    g_stSectionInfo[1].pacSectionDesc = g_acBBoxLog;
    g_stSectionInfo[1].uiSectionDescSize = sizeof(g_acBBoxLog);
    g_stSectionInfo[1].uiSectionNameSize = sizeof(BBOX_LOG);
    rc = strncpy_s(
        g_stSectionInfo[1].acSectionName, BBOX_SECTION_NAME_LEN, BBOX_LOG, g_stSectionInfo[1].uiSectionNameSize);
    securec_check_c(rc, "\0", "\0");

    /* .shstrtab */
    g_stSectionInfo[2].uiSectionType = SHT_STRTAB;
    g_stSectionInfo[2].pacSectionDesc = g_acBboxStrTabInfo;
    g_stSectionInfo[2].uiSectionDescSize = sizeof(g_acBboxStrTabInfo);
    g_stSectionInfo[2].uiSectionNameSize = sizeof(BBOX_STR_TAB);
    rc = strncpy_s(
        g_stSectionInfo[2].acSectionName, BBOX_SECTION_NAME_LEN, BBOX_STR_TAB, g_stSectionInfo[2].uiSectionNameSize);
    securec_check_c(rc, "\0", "\0");

    bbox_print(PRINT_LOG, "Fill all section successful.\n");

    return;
}

/*
 * fill every structure written into core file, include mapping, note, VDSO.
 * in      : Frame *pFrame - Write the structure pointer to the core file
 *           struct BBOX_VM_MAPS *pstVmMappingSegment - Pointer to the structure that describes the mapping segment
 *           int iSegmentNum - The number of mapping segments in the address space
 *           int *piPhdrSum - Sum of mapping and VDSO in core file
 *           pid_t *ptPids - A pointer to an array of process Numbers
 *           struct BBOX_ELF_NOTE_INFO *pstNoteInfo - Pointer to the structure that describes the note segment
 *           union BBOX_VM_VDSO *pstVmVDSO - Pointer to the structure that describes the VDSO segment
 */
static int BBOX_FillAllInfoOfCoreFile(Frame* pFrame, struct BBOX_VM_MAPS* pstVmMappingSegment, int* piSegmentNum,
    int* piPhdrSum, pid_t* ptPids, struct BBOX_ELF_NOTE_INFO* pstNoteInfo, union BBOX_VM_VDSO* pstVmVDSO)
{
    int iResult = 1;
    int iValidSegmentNum = 0;
    int iAuxvNum = 0;
    int iExtraPhdrNum = 0;

    if (NULL == pFrame || NULL == pstVmMappingSegment || NULL == piPhdrSum || NULL == ptPids || NULL == pstNoteInfo ||
        NULL == pstVmVDSO || NULL == piSegmentNum) {
        bbox_print(PRINT_ERR,
            "BBOX_FillAllInfoOfCoreFile parameters is invalid: pFrame, pstVmMappingSegment, " \
            "piPhdrSum, ptPids, pstNoteInfo, pstVmVDSO or piSegmentNum is NULL.\n");
        return RET_ERR;
    }

    /* read /proc/self/maps, fill structure that discribes process address space information. */
    iResult = BBOX_FillVmMappingInfo(pstVmMappingSegment, piSegmentNum, &iValidSegmentNum);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_FillVmMappingInfo is failed, iResult = %d.\n", iResult);
        return RET_ERR;
    }

    /* get number of auxv written into core file in /proc/self/auxv, record VDSO address.
       Determines whether VDSO is written to the core file and the number of segments written,
       record it into iSegmentNum */
    iResult = BBOX_FillVDSOInfo(&iAuxvNum, &iExtraPhdrNum, *piSegmentNum, pstVmVDSO, pstVmMappingSegment);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_FillVDSOInfo is failed, iResult = %d.\n", iResult);
        return RET_ERR;
    }

    /* calculate all segment that need to be written into core file except note segment */
    *piPhdrSum = iValidSegmentNum + iExtraPhdrNum;
    bbox_print(PRINT_DBG,
        "Get all Phdr numbers success, iValidSegmentNum = %d, iExtraPhdrNum = %d, *piPhdrSum=%d.\n",
        iValidSegmentNum,
        iExtraPhdrNum,
        *piPhdrSum);

    /* Fill struct BBOX_ELF_NOTE_INFO */
    iResult = BBOX_FillNoteInfo(pFrame, pstNoteInfo, ptPids, iAuxvNum);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_FillNoteInfo is failed, iResult = %d.\n", iResult);
        return RET_ERR;
    }

    /* fill section */
    BBOX_FillSectionInfo();

    bbox_print(PRINT_LOG, "Fill Note Info success.\n");

    return RET_OK;
}

/*
 * create and open core file, get fd.
 * in      : Frame *pFrame - Pointer to the structure of the core file
 *           char *pFileName - Pointer to an array of core file names
 *           struct BBOX_WRITE_FDS *pstFileWriteFd - Pointer to a structure that describes the properties of a core file
 */
static int BBOX_OpenCoreFile(Frame* pFrame, const char* pFileName, struct BBOX_WRITE_FDS* pstFileWriteFd)
{
    char szCmd[BBOX_CMD_LEN];
    errno_t rc = EOK;

    if (NULL == pFrame || NULL == pstFileWriteFd) {
        bbox_print(PRINT_ERR,
            "BBOX_OpenCoreFile parameters is invalid: pFrame or "\
            "pstFileWriteFd is NULL.\n");
        return RET_ERR;
    }

    rc = memset_s(szCmd, BBOX_CMD_LEN, 0, sizeof(szCmd));
    securec_check_c(rc, "\0", "\0");

    /* If the user does not define the file name, set it to core.tid.lz4 */
    if (NULL != pFileName) {
        bbox_snprintf(szCmd, BBOX_CMD_LEN, COMPRESSION_CMD, 1, pFileName);
    } else {
        bbox_snprintf(szCmd, BBOX_CMD_LEN, COMPRESSION_CMD_WITH_FILENAME, 1, ((Frame*)pFrame)->tid);
    }

    /* create core file using the way of compression while writing */
    pstFileWriteFd->iWriteFd = sys_popen(szCmd, "w");
    if (pstFileWriteFd->iWriteFd < 0) {
        bbox_print(PRINT_ERR, "sys_popen is failed, pstFileWriteFd->iWriteFd = %d.\n", pstFileWriteFd->iWriteFd);
        return RET_ERR;
    }

    pstFileWriteFd->uiMaxLength = ~(size_t)0;

    bbox_print(PRINT_LOG, "Open core file success.\n");

    return RET_OK;
}

/*
 * write to file
 * in      : struct BBOX_WRITE_FDS *pstWriteFds - A pointer to core file.
 *           void *pWriteData - Points to what will be written to the file
 *           size_t uiWriteSize - The size of what will be written to the file
 */
static ssize_t BBOX_DoWrite(struct BBOX_WRITE_FDS* pstWriteFds, void* pWriteData, size_t uiWriteSize)
{
    ssize_t iRet = 0;
    ssize_t iWriteCount = 0;
    const ssize_t iMaxSize = (1024 * 1024 * 1024);  // 1G
    size_t uiSize = 0;

    if (NULL == pstWriteFds || NULL == pWriteData) {
        bbox_print(PRINT_ERR,
            "BBOX_DoWrite parameters is invalid: pstWriteFds or "
            "pWriteData is NULL.\n");
        return RET_ERR;
    }

    while (uiWriteSize) {
        uiSize = uiWriteSize;
        if (uiSize > (size_t)iMaxSize) {
            uiSize = iMaxSize;
        }

        BBOX_NOINTR(iRet = sys_write(pstWriteFds->iWriteFd, pWriteData, uiSize));
        if (iRet <= 0) {
            bbox_print(PRINT_ERR,
                "sys_write failed, iRet = %zd, "
                "uiSize = %zu, errno = %d.\n",
                iRet,
                uiSize,
                errno);

            return iRet;
        }

        iWriteCount += iRet;
        uiWriteSize -= iRet;
        pWriteData = (char*)pWriteData + iRet;
    }

    return iWriteCount;
}

/*
 * The file header Ehdr structure that populates the elf file
 * in      : BBOX_EHDR *pEhdr - The header structure of the core file
 *           int iPhdrSum - The number of segments written to the core file in the address space
 */
static int BBOX_FillEhdr(BBOX_EHDR* pstEhdr, int iPhdrSum)
{
    if (NULL == pstEhdr || iPhdrSum <= 0) {
        bbox_print(PRINT_ERR, "BBOX_FillEhdr parameters is invalid: pstEhdr maybe NULL, iPhdrSum = %d.\n", iPhdrSum);
        return RET_ERR;
    }

    pstEhdr->e_ident[0] = ELFMAG0;
    pstEhdr->e_ident[1] = (unsigned char)ELFMAG1;
    pstEhdr->e_ident[2] = ELFMAG2;
    pstEhdr->e_ident[3] = ELFMAG3;
    pstEhdr->e_ident[4] = BBOX_ELF_CLASS;
    pstEhdr->e_ident[5] = (unsigned char)BBOX_DetermineMsb(); /* Determine whether the system is large or small */
    pstEhdr->e_ident[6] = EV_CURRENT;
    pstEhdr->e_type = ET_CORE;
    pstEhdr->e_machine = ELF_ARCH;
    pstEhdr->e_version = EV_CURRENT;
    pstEhdr->e_phoff = sizeof(BBOX_EHDR);
    pstEhdr->e_ehsize = sizeof(BBOX_EHDR);
    pstEhdr->e_phentsize = sizeof(BBOX_PHDR);

#if (defined(__i386__))
    pstEhdr->e_phnum = (Elf32_Half)(iPhdrSum + 1); /* Number of memory address space segments plus note segments */
    pstEhdr->e_shnum = (Elf32_Half)(BBOX_SECTION_NUM);
#elif (defined(__x86_64__)) || (defined(__ARM_ARCH_5TE__)) || (defined(__ARM_ARCH_7A__)) || (defined(__aarch64__))
    pstEhdr->e_phnum = (Elf64_Half)(iPhdrSum + 1); /* Number of memory address space segments plus note segments */
    pstEhdr->e_shnum = (Elf64_Half)(BBOX_SECTION_NUM);
#endif

    pstEhdr->e_shoff = sizeof(BBOX_EHDR) + (iPhdrSum + 1) * sizeof(BBOX_PHDR);
    pstEhdr->e_shentsize = sizeof(BBOX_SHDR);

    pstEhdr->e_shstrndx = BBOX_SHSTR_INDEX;

    bbox_print(PRINT_LOG, "Fill Elf Ehdr success.\n");

    return RET_OK;
}

/*
 * write file header of core to core file.
 * in      : int iPhdrSum - The number of segment in the core file
 *           struct BBOX_WRITE_FDS *pstFileWriteFd - A pointer to core file
 */
static int BBOX_WriteElfEhdr(int iPhdrSum, struct BBOX_WRITE_FDS* pstFileWriteFd)
{
    int iResult = RET_ERR;
    ssize_t iWriteSize = -1;
    BBOX_EHDR stElfCoreHead;
    errno_t rc = EOK;
    if (NULL == pstFileWriteFd || iPhdrSum <= 0) {
        bbox_print(PRINT_ERR,
            "BBOX_WriteElfEhdr parameters is invalid: pstFileWriteFd may be NULL, " \
            "iPhdrSum = %d.\n",
            iPhdrSum);

        return RET_ERR;
    }

    rc = memset_s(&stElfCoreHead, sizeof(BBOX_EHDR), 0, sizeof(BBOX_EHDR));
    securec_check_c(rc, "\0", "\0");

    iResult = BBOX_FillEhdr(&stElfCoreHead, iPhdrSum);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_FillEhdr is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    iWriteSize = BBOX_DoWrite(pstFileWriteFd, &stElfCoreHead, sizeof(BBOX_EHDR));
    if (iWriteSize == RET_ERR || sizeof(BBOX_EHDR) != iWriteSize) {
        bbox_print(PRINT_ERR, "BBOX_DoWrite is failed, iWriteSize = %zd\n", iWriteSize);

        return RET_ERR;
    }

    bbox_print(PRINT_LOG, "Write Ehdr info to the core file success.\n");

    return RET_OK;
}

/*
 * Fill the header of the note segment of the core file describes the structure
 * in      : struct BBOX_ELF_NOTE_INFO *pstNoteInfo - The content structure of the note segment
 *           int iPhdrSum - The number of segments written to the core file in the address space
 *           size_t *puiOffset - The starting offset of the next segment
 *           struct BBOX_WRITE_FDS *pstFileWriteFd - A pointer to core file
 */
static int BBOX_WriteNotePhdr(
    struct BBOX_ELF_NOTE_INFO* pstNoteInfo, int iPhdrSum, size_t* puiOffset, struct BBOX_WRITE_FDS* pstFileWriteFd)
{
    size_t uiOffSize = 0;
    size_t uiFileSize = 0;
    size_t uiCoreUserSize = 0;
    size_t uiNoteSize = 0;
    size_t uiAuxvSize = 0;
    size_t uiNoteAlign = 0;
    ssize_t iWriteSize = -1;
    int iPageSize = sys_sysconf(_SC_PAGESIZE); /* Gets the system page size */
    BBOX_PHDR stElfPhdr;

    if (NULL == pstNoteInfo || NULL == puiOffset || NULL == pstFileWriteFd || iPhdrSum <= 0) {
        bbox_print(PRINT_ERR,
            "BBOX_WriteNotePhdr parameters is invalid: pstNoteInfo, puiOffset or pstFileWriteFd is NULL, " \
            "iPhdrSum = %d.\n",
            iPhdrSum);

        return RET_ERR;
    }

    errno_t rc = memset_s(&stElfPhdr, sizeof(BBOX_PHDR), 0, sizeof(BBOX_PHDR));
    securec_check_c(rc, "\0", "\0");

    /* Calculates the starting position and size of the note segment in the core file */
    uiOffSize = sizeof(BBOX_EHDR) + (iPhdrSum + 1) * sizeof(BBOX_PHDR) + 3 * sizeof(BBOX_SHDR);

    /* Calculate the size of the user data */
    uiCoreUserSize = sizeof(BBOX_NHDR) + BBOX_CORE_STRING_LENGTH + sizeof(struct BBOX_CORE_USER);

    /* Calculate the size of the BBOX_ELF_PRPSSTATUS */
#if defined(__aarch64__)
    uiNoteSize = (pstNoteInfo->iThreadNoteInfoNum) *
        (sizeof(BBOX_NHDR) + BBOX_CORE_STRING_LENGTH + sizeof(struct BBOX_ELF_PRPSSTATUS));
#else
    uiNoteSize = (pstNoteInfo->iThreadNoteInfoNum) *
        (sizeof(BBOX_NHDR) + BBOX_CORE_STRING_LENGTH + sizeof(struct BBOX_ELF_PRPSSTATUS) + sizeof(BBOX_NHDR) +
        BBOX_CORE_STRING_LENGTH + sizeof(struct BBOX_FPREGSET));
#endif

    if (pstNoteInfo->iFpxRegistersFlag) {
        /* If the SSE register exists, add its size */
        uiNoteSize += (pstNoteInfo->iThreadNoteInfoNum) *
                      (sizeof(BBOX_NHDR) + BBOX_CORE_STRING_LENGTH + sizeof(struct BBOX_FPXREGSET));
    }

    /* Calculates the size of the Auxv written */
    uiAuxvSize = sizeof(BBOX_NHDR) + BBOX_CORE_STRING_LENGTH + (pstNoteInfo->iAuxvNoteInfoNum) * sizeof(BBOX_AUXV_T);
    uiFileSize = sizeof(BBOX_NHDR) + BBOX_CORE_STRING_LENGTH + sizeof(struct BBOX_ELF_PRPSINFO) + uiCoreUserSize +
                 uiNoteSize + uiAuxvSize;

    stElfPhdr.p_type = PT_NOTE;
    stElfPhdr.p_offset = uiOffSize;
    stElfPhdr.p_filesz = uiFileSize;
    *puiOffset = uiOffSize + uiFileSize;

    iWriteSize = BBOX_DoWrite(pstFileWriteFd, &stElfPhdr, sizeof(BBOX_PHDR));
    if (iWriteSize == RET_ERR || sizeof(BBOX_PHDR) != iWriteSize) {
        bbox_print(PRINT_ERR, "BBOX_DoWrite is failed, iWriteSize = %zd.\n", iWriteSize);
        return RET_ERR;
    }

    /* Calculate the size of the page alignment to fill, and calculate the starting position of the next segment */
    stElfPhdr.p_align = iPageSize;
    uiNoteAlign = stElfPhdr.p_align - ((*puiOffset) % stElfPhdr.p_align);
    if (uiNoteAlign == stElfPhdr.p_align) {
        uiNoteAlign = 0;
    }

    pstNoteInfo->uiNoteAlign = uiNoteAlign;
    (*puiOffset) += uiNoteAlign;

    return RET_OK;
}

/*
 * Fill the header of the note segment of the core file describes the structure
 * in      : struct BBOX_VM_MAPS *pstVmMappingSegment - The content structure of the note segment
 *           int   iSegmentNum - The number of segments written to the core file in the address space
 *           size_t *puiOffset - The starting offset of the next segment
 *           struct BBOX_WRITE_FDS *pstFileWriteFd - A pointer to core file
 */
static int BBOX_WriteVmSegmentPhdr(
    struct BBOX_VM_MAPS* pstVmMappingSegment, int iSegmentNum, size_t* puiOffset, struct BBOX_WRITE_FDS* pstFileWriteFd)
{
    size_t uiFileSize = 0;
    ssize_t iWriteSize = -1;
    unsigned int uCount = 0;
    int iPageSize = sys_sysconf(_SC_PAGESIZE);
    BBOX_PHDR stElfPhdr;

    if (NULL == pstVmMappingSegment || NULL == puiOffset || NULL == pstFileWriteFd || iSegmentNum <= 0) {
        bbox_print(PRINT_ERR,
            "BBOX_WriteVmSegmentPhdr parameters is invalid: pstVmMappingSegment, puiOffset or "
            "pstFileWriteFd is NULL, iSegmentNum = %d.\n",
            iSegmentNum);
        return RET_ERR;
    }

    errno_t rc = memset_s(&stElfPhdr, sizeof(BBOX_PHDR), 0, sizeof(BBOX_PHDR));
    securec_check_c(rc, "\0", "\0");

    stElfPhdr.p_type = PT_LOAD;
    stElfPhdr.p_align = iPageSize;
    stElfPhdr.p_paddr = 0;

    for (uCount = 0; uCount < (unsigned int)iSegmentNum; uCount++) {
        if (pstVmMappingSegment[uCount].iIsRemoveFlags == 0) {
            /* calculate size */
            uiFileSize = (pstVmMappingSegment[uCount].uiEndAddress) - (pstVmMappingSegment[uCount].uiStartAddress);
            stElfPhdr.p_offset = *puiOffset;                                  /* offset */
            stElfPhdr.p_vaddr = (pstVmMappingSegment[uCount].uiStartAddress); /* start address in address space of segment */
            stElfPhdr.p_memsz = uiFileSize;                                   /* size of segment in address space */

            uiFileSize = (pstVmMappingSegment[uCount].uiWriteSize); /* size of segment in file */
            stElfPhdr.p_filesz = uiFileSize;
            stElfPhdr.p_flags = (pstVmMappingSegment[uCount].iFlags) & PF_MASK;

            (*puiOffset) += uiFileSize; /* next offset of segment. */

            iWriteSize = BBOX_DoWrite(pstFileWriteFd, &stElfPhdr, sizeof(BBOX_PHDR));
            if (iWriteSize == RET_ERR || sizeof(BBOX_PHDR) != iWriteSize) {
                bbox_print(PRINT_ERR, "BBOX_DoWrite is failed, iWriteSize = %zd.\n", iWriteSize);

                return RET_ERR;
            }
        }
    }

    return RET_OK;
}

/*
 * Writes a part of the VDSO segment to the core file
 * in      : struct BBOX_WRITE_FDS *pstFileWriteFd - A pointer to the structure of the core file
 *           union BBOX_VM_VDSO *pstVmVDSO - The structure that points to the VDSO segment
 *           size_t *puiOffset - The starting offset pointer of the next segment
 * return RET_OK or RET_ERR.
 */
static int BBOX_WriteVDSOPhdr(struct BBOX_WRITE_FDS* pstFileWriteFd, union BBOX_VM_VDSO* pstVmVDSO, size_t* puiOffset)
{
    size_t uiFileSize = 0;
    ssize_t iWriteSize = -1;
    unsigned int uCount = 0;
    BBOX_PHDR stElfPhdr;

    if (NULL == pstFileWriteFd || NULL == pstVmVDSO || NULL == puiOffset) {
        bbox_print(PRINT_ERR,
            "BBOX_WriteVDSOPhdr parameters is invalid: pstFileWriteFd, pstVmVDSO or " \
            "puiOffset is NULL.\n");
        return RET_ERR;
    }

    errno_t rc = memset_s(&stElfPhdr, sizeof(BBOX_PHDR), 0, sizeof(BBOX_PHDR));
    securec_check_c(rc, "\0", "\0");

    if (pstVmVDSO->uiVDSOAddress) {
        BBOX_PHDR* pstVDSOPhdr = (BBOX_PHDR*)(pstVmVDSO->uiVDSOAddress + pstVmVDSO->pVDSOEhdr->e_phoff);

        for (uCount = 0; uCount < pstVmVDSO->pVDSOEhdr->e_phnum; uCount++) {
            if (pstVDSOPhdr[uCount].p_type != PT_LOAD) {
                /* Write the non-load segment of VDSO to the core file */
                rc = memcpy_s(&stElfPhdr, sizeof(BBOX_PHDR), pstVDSOPhdr + uCount, sizeof(BBOX_PHDR));
                securec_check_c(rc, "\0", "\0");

                uiFileSize = stElfPhdr.p_filesz;
                stElfPhdr.p_offset = *puiOffset;
                stElfPhdr.p_paddr = 0;

                iWriteSize = BBOX_DoWrite(pstFileWriteFd, &stElfPhdr, sizeof(BBOX_PHDR));
                if (iWriteSize == RET_ERR || sizeof(BBOX_PHDR) != iWriteSize) {
                    bbox_print(PRINT_ERR, "BBOX_DoWrite is failed, iWriteSize = %zd.\n", iWriteSize);

                    return RET_ERR;
                }

                (*puiOffset) += uiFileSize;
            }
        }
    }

    return RET_OK;
}

/*
 * Writes the header of section to the core file
 * in      : int iPhdrSum - The number of program headers in the core file
 *           struct BBOX_ELF_SECTION *pstSectionInfo - The starting offset pointer of the next segment
 *           struct BBOX_WRITE_FDS *pstFileWriteFd - A pointer to the structure of the core file
 * return RET_OK or RET_ERR.
 */
static int BBOX_WriteElfShdr(
    struct BBOX_ELF_SECTION* pstSectionInfo, size_t* puiOffset, struct BBOX_WRITE_FDS* pstFileWriteFd)
{
    size_t uiOffSize = 0;
    size_t uiFileSize = 0;
    unsigned int uCount = 0;
    int iSecNameOffset = 0;
    BBOX_SHDR stElfShdr;
    BBOX_SECTION_STRU* pstSection = NULL;

    for (uCount = 0; uCount < pstSectionInfo->uiSectionNum; uCount++) {
        pstSection = (pstSectionInfo->pstSection) + uCount;
        errno_t rc = memset_s(&stElfShdr, sizeof(BBOX_SHDR), 0, sizeof(BBOX_SHDR));
        securec_check_c(rc, "\0", "\0");

        uiOffSize = *puiOffset;
        uiFileSize = pstSection->uiSectionDescSize;

        /* name of section, it is not a string and it's value is the offset of string in string table. */
        stElfShdr.sh_name = iSecNameOffset;
        stElfShdr.sh_type = pstSection->uiSectionType; /* type of section */
        stElfShdr.sh_offset = uiOffSize;
        stElfShdr.sh_size = uiFileSize;

        BBOX_WRITE(pstFileWriteFd, &stElfShdr, sizeof(BBOX_SHDR));

        bbox_print(PRINT_LOG, "Write section[%s] head to fill successed.\n", pstSection->acSectionName);

        *puiOffset = uiOffSize + uiFileSize;
        iSecNameOffset += pstSection->uiSectionNameSize;
    }

    bbox_print(PRINT_LOG, "Write all section head to fill successed.\n");

    return RET_OK;
}

/*
 * Writes all the progrem header of core file to the core file
 * in      : int iPhdrSum - The number of program headers in the core file
 *           struct BBOX_ELF_NOTE_INFO *pstNoteInfo - A pointer to the structure of note segment
 *           int iSegmentNum - The number of mapping segments in the address space
 *           union BBOX_VM_VDSO *pstVmVDSO - A pointer to the structure of VDSO segment
 *           struct BBOX_VM_MAPS *pstVmMappingSegment - A pointer to the structure of mapping segment
 *           struct BBOX_WRITE_FDS *pstFileWriteFd - A pointer to the structure of the core file
 * return RET_OK or RET_ERR.
 */
static int BBOX_WriteElfHdr(int iPhdrSum, struct BBOX_ELF_NOTE_INFO* pstNoteInfo, int iSegmentNum,
    union BBOX_VM_VDSO* pstVmVDSO, struct BBOX_VM_MAPS* pstVmMappingSegment, struct BBOX_WRITE_FDS* pstFileWriteFd)
{
    int iResult = RET_ERR;
    size_t uiOffset = 0;

    if (NULL == pstNoteInfo || NULL == pstVmMappingSegment || NULL == pstVmVDSO || NULL == pstFileWriteFd ||
        iSegmentNum <= 0 || iPhdrSum <= 0) {
        bbox_print(PRINT_ERR,
            "BBOX_WriteElfPhdr parameters is invalid: pstNoteInfo, pstVmMappingSegment," \
            "pstVmVDSO or pstFileWriteFd in NULL, iSegmentNum = %d, iPhdrSum = %d.\n",
            iSegmentNum,
            iPhdrSum);
        return RET_ERR;
    }

    /* write Phdr of Note to core file. */
    iResult = BBOX_WriteNotePhdr(pstNoteInfo, iPhdrSum, &uiOffset, pstFileWriteFd);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_WriteNotePhdr is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    bbox_print(PRINT_LOG, "Write Note Phdr info to the core file success.\n");

    /* write Phdr of process address space to core file. */
    iResult = BBOX_WriteVmSegmentPhdr(pstVmMappingSegment, iSegmentNum, &uiOffset, pstFileWriteFd);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_WriteVmSegmentPhdr is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    bbox_print(PRINT_LOG, "Write Vm mapping segment Phdr info to the core file success.\n");

    /* write Phdr of VDSO to core file. */
    iResult = BBOX_WriteVDSOPhdr(pstFileWriteFd, pstVmVDSO, &uiOffset);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_WriteVDSOPhdr is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    bbox_print(PRINT_LOG, "Write VDSO Phdr info to the core file success.\n");

    /* write Shdr of section to core file. */
    iResult = BBOX_WriteElfShdr(&g_stElfSectionInfo, &uiOffset, pstFileWriteFd);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_WriteElfShdr is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    bbox_print(PRINT_LOG, "Write section head info to the core file success.\n");

    return RET_OK;
}

/*
 * Writes the prpsinfo to the core file
 * in      : struct BBOX_ELF_PRPSINFO *pstPrPsInfo - A pointer to prpsinfo
 *           struct BBOX_WRITE_FDS *pstWriteFds - A pointer to the structure of the core file
 * return RET_OK or RET_ERR.
 */
static int BBOX_WritePrPsinfoToFile(struct BBOX_ELF_PRPSINFO* pstPrPsInfo, struct BBOX_WRITE_FDS* pstFileFds)
{
    BBOX_NHDR stElfNhdr;

    if (NULL == pstPrPsInfo || NULL == pstFileFds) {
        bbox_print(PRINT_ERR,
            "BBOX_WritePrPsinfoToFile parameters is invalid: pstPrPsInfo or pstFileFds is NULL.\n");

        return RET_ERR;
    }

    errno_t rc = memset_s(&stElfNhdr, sizeof(BBOX_NHDR), 0, sizeof(BBOX_NHDR));
    securec_check_c(rc, "\0", "\0");

    stElfNhdr.n_namesz = BBOX_CORE_NAME_LENGTH;
    stElfNhdr.n_descsz = sizeof(struct BBOX_ELF_PRPSINFO);
    stElfNhdr.n_type = NT_PRPSINFO;

    if (BBOX_DoWrite(pstFileFds, &stElfNhdr, sizeof(BBOX_NHDR)) != sizeof(BBOX_NHDR) ||
        BBOX_DoWrite(pstFileFds, (void*)BBOX_CORE_STRING, BBOX_CORE_STRING_LENGTH) != BBOX_CORE_STRING_LENGTH ||
        BBOX_DoWrite(pstFileFds, pstPrPsInfo, sizeof(struct BBOX_ELF_PRPSINFO)) != sizeof(struct BBOX_ELF_PRPSINFO)) {
        bbox_print(PRINT_ERR, "BBOX_DoWrite  is failed.\n");

        return RET_ERR;
    }

    bbox_print(PRINT_LOG,
        "Write Prpsinfo size = %zd to the core file success.\n",
        sizeof(BBOX_NHDR) + BBOX_CORE_STRING_LENGTH + sizeof(struct BBOX_ELF_PRPSINFO));

    return RET_OK;
}

/*
 * Writes the user data information to the core file
 * in      : struct BBOX_ELF_PRPSINFO *pPrPsInfo - A pointer to User Core
 *           struct BBOX_WRITE_FDS *pstWriteFds - A pointer to the structure of the core file
 * return RET_OK or RET_ERR.
 */
static int BBOX_WriteUserRegistersToFile(struct BBOX_CORE_USER* pstCoreUserInfo, struct BBOX_WRITE_FDS* pstFileFds)
{
    BBOX_NHDR stElfNhdr;

    if (NULL == pstCoreUserInfo || NULL == pstFileFds) {
        bbox_print(PRINT_ERR,
            "BBOX_WriteUserRegistersToFile parameters is invalid: pstCoreUserInfo or pstFileFds is NULL.\n");

        return RET_ERR;
    }

    errno_t rc = memset_s(&stElfNhdr, sizeof(BBOX_NHDR), 0, sizeof(BBOX_NHDR));
    securec_check_c(rc, "\0", "\0");

    stElfNhdr.n_namesz = BBOX_CORE_NAME_LENGTH;
    stElfNhdr.n_descsz = sizeof(struct BBOX_CORE_USER);
    stElfNhdr.n_type = NT_PRXREG;

    if (BBOX_DoWrite(pstFileFds, &stElfNhdr, sizeof(BBOX_NHDR)) != sizeof(BBOX_NHDR) ||
        BBOX_DoWrite(pstFileFds, (void*)BBOX_CORE_STRING, BBOX_CORE_STRING_LENGTH) != BBOX_CORE_STRING_LENGTH ||
        BBOX_DoWrite(pstFileFds, pstCoreUserInfo, sizeof(struct BBOX_CORE_USER)) != sizeof(struct BBOX_CORE_USER)) {
        bbox_print(PRINT_ERR, "BBOX_DoWrite  is failed.\n");

        return RET_ERR;
    }

    bbox_print(PRINT_LOG,
        "Write UserRegisters size = %zd to the core file success.\n",
        sizeof(BBOX_NHDR) + BBOX_CORE_STRING_LENGTH + sizeof(struct BBOX_CORE_USER));

    return RET_OK;
}

/*
 * read /proc/self/auxv, write parts of it to a core file
 * in       : int iAuxvNum - Number of Auxv structures written to the core file
 *            struct BBOX_WRITE_FDS *pstWriteFds - pointer to core file.
 * return RET_OK or RET_ERR.
 */
static int BBOX_WriteAuxvInfoToFile(int iAuxvNum, struct BBOX_WRITE_FDS* pstFileFds)
{
    int iAuxvFd = -1;
    ssize_t iReadSize = -1;
    unsigned int uCount = 0;
    BBOX_NHDR stElfNhdr;
    BBOX_AUXV_T stAuxv;
    errno_t rc = EOK;
    if (NULL == pstFileFds || iAuxvNum < 0) {
        bbox_print(PRINT_ERR,
            "BBOX_WriteAuxvInfoToFile parameters is invalid: pstFileFds may be NULL," \
            "iAuxvNum = %d.\n",
            iAuxvNum);

        return RET_ERR;
    }

    rc = memset_s(&stElfNhdr, sizeof(BBOX_NHDR), 0, sizeof(BBOX_NHDR));
    securec_check_c(rc, "\0", "\0");

    stElfNhdr.n_namesz = BBOX_CORE_NAME_LENGTH;
    stElfNhdr.n_descsz = iAuxvNum * sizeof(BBOX_AUXV_T);
    stElfNhdr.n_type = NT_AUXV;

    if (BBOX_DoWrite(pstFileFds, &stElfNhdr, sizeof(BBOX_NHDR)) != sizeof(BBOX_NHDR) ||
        BBOX_DoWrite(pstFileFds, (void*)BBOX_CORE_STRING, BBOX_CORE_STRING_LENGTH) != BBOX_CORE_STRING_LENGTH) {
        bbox_print(PRINT_ERR, "BBOX_DoWrite  is failed.\n");

        return RET_ERR;
    }

    /* read /proc/self/Auxv, write to core file. */
    BBOX_NOINTR(iAuxvFd = sys_open(THREAD_SELF_AUXV_FILE, O_RDONLY, 0));
    if (iAuxvFd < 0) {
        bbox_print(PRINT_ERR, "sys_open is failed, iAuxvFd = %d.\n", iAuxvFd);

        return RET_ERR;
    }

    for (uCount = 0; uCount < (unsigned int)iAuxvNum; uCount++) {
        iReadSize = -1;
        rc = memset_s(&stAuxv, sizeof(BBOX_AUXV_T), 0, sizeof(BBOX_AUXV_T));
        securec_check_c(rc, "\0", "\0");

        BBOX_NOINTR(iReadSize = sys_read(iAuxvFd, &stAuxv, sizeof(BBOX_AUXV_T)));
        if (iReadSize != sizeof(BBOX_AUXV_T)) {
            bbox_print(PRINT_ERR, "sys_read  is failed, iReadSize = %zd.\n", iReadSize);
            BBOX_NOINTR(sys_close(iAuxvFd));
            return RET_ERR;
        }
        if (sizeof(BBOX_AUXV_T) != BBOX_DoWrite(pstFileFds, &stAuxv, sizeof(BBOX_AUXV_T))) {
            bbox_print(PRINT_ERR, "BBOX_DoWrite  is failed.\n");
            BBOX_NOINTR(sys_close(iAuxvFd));
            return RET_ERR;
        }
    }

    BBOX_NOINTR(sys_close(iAuxvFd));

    bbox_print(PRINT_LOG,
        "Write Auxv size = %zd to the core file success.\n",
        sizeof(BBOX_NHDR) + BBOX_CORE_STRING_LENGTH + stElfNhdr.n_descsz);

    return RET_OK;
}
/*
 * Writes information of the structure that describes the state of a process to the core file
 * in      : struct BBOX_ELF_PRPSSTATUS *pstPrPsStatusInfo - A pointer to the structure that describes the state
 *           struct BBOX_WRITE_FDS *pstWriteFds - A pointer to the structure of the core file
 * return RET_OK or RET_ERR.
 */
static int BBOX_WritePrPsStatusToFile(struct BBOX_ELF_PRPSSTATUS* pstPrPsStatusInfo, struct BBOX_WRITE_FDS* pstFileFds)
{
    BBOX_NHDR stElfNhdr;
    int iResult = -1;

    if (NULL == pstPrPsStatusInfo || NULL == pstFileFds) {
        bbox_print(PRINT_ERR,
            "BBOX_WritePrPsStatusToFile parameters is invalid: pstPrPsStatusInfo or pstFileFds is NULL.\n");

        return RET_ERR;
    }

    errno_t rc = memset_s(&stElfNhdr, sizeof(BBOX_NHDR), 0, sizeof(BBOX_NHDR));
    securec_check_c(rc, "\0", "\0");

    stElfNhdr.n_namesz = BBOX_CORE_NAME_LENGTH;
    stElfNhdr.n_descsz = sizeof(struct BBOX_ELF_PRPSSTATUS);
    stElfNhdr.n_type = NT_PRSTATUS;

    if (BBOX_DoWrite(pstFileFds, &stElfNhdr, sizeof(BBOX_NHDR)) != sizeof(BBOX_NHDR) ||
        BBOX_DoWrite(pstFileFds, (void*)BBOX_CORE_STRING, BBOX_CORE_STRING_LENGTH) != BBOX_CORE_STRING_LENGTH) {
        bbox_print(PRINT_ERR, "BBOX_DoWrite  is failed.\n");
        return RET_ERR;
    }

    iResult = BBOX_DoWrite(pstFileFds, pstPrPsStatusInfo, sizeof(struct BBOX_ELF_PRPSSTATUS));
    if (iResult == RET_ERR || sizeof(struct BBOX_ELF_PRPSSTATUS) != iResult) {
        bbox_print(PRINT_ERR, "BBOX_DoWrite  is failed, iResult = %d.\n", iResult);
        return RET_ERR;
    }

    return RET_OK;
}

/*
 * Writes the FPU register contents to the core file
 * in      : struct BBOX_FPREGSET *pstFpRegisters - A pointer to an FPU register
 *           struct BBOX_WRITE_FDS *pstWriteFds - A pointer to the structure of the core file
 * return RET_OK or RET_ERR.
 */
static int BBOX_WriteFpRegistersToFile(struct BBOX_FPREGSET* pstFpRegisters, struct BBOX_WRITE_FDS* pstFileFds)
{
/* since ptrace() doesn't support to obtain float registers' context in aarch64, don't dump it out. */
#if !defined(__aarch64__)
    BBOX_NHDR stElfNhdr;

    if (NULL == pstFpRegisters || NULL == pstFileFds) {
        bbox_print(PRINT_ERR,
            "BBOX_WriteFpRegistersToFile parameters is invalid: pstFpRegisters or pstFileFds is NULL.\n");
        return RET_ERR;
    }

    errno_t rc = memset_s(&stElfNhdr, sizeof(BBOX_NHDR), 0, sizeof(BBOX_NHDR));
    securec_check_c(rc, "\0", "\0");

    stElfNhdr.n_namesz = BBOX_CORE_NAME_LENGTH;
    stElfNhdr.n_descsz = sizeof(struct BBOX_FPREGSET);
    stElfNhdr.n_type = NT_FPREGSET;

    if (BBOX_DoWrite(pstFileFds, &stElfNhdr, sizeof(BBOX_NHDR)) != sizeof(BBOX_NHDR) ||
        BBOX_DoWrite(pstFileFds, (void*)BBOX_CORE_STRING, BBOX_CORE_STRING_LENGTH) != BBOX_CORE_STRING_LENGTH ||
        BBOX_DoWrite(pstFileFds, pstFpRegisters, sizeof(struct BBOX_FPREGSET)) != sizeof(struct BBOX_FPREGSET)) {
        bbox_print(PRINT_ERR, "BBOX_DoWrite  is failed.\n");
        return RET_ERR;
    }
#endif

    return RET_OK;
}

/*
 * Writes the SSE structure contents to the core file
 * in      : struct BBOX_FPXREGSET *pstFpxRegisters - A pointer to an SSE structure
 *           struct BBOX_WRITE_FDS *pstWriteFds - A pointer to the structure of the core file
 * return RET_OK or RET_ERR.
 */
static int BBOX_WriteFpxRegistersToFile(struct BBOX_FPXREGSET* pstFpxRegisters, struct BBOX_WRITE_FDS* pstFileFds)
{
    BBOX_NHDR stElfNhdr;

    if (NULL == pstFpxRegisters || NULL == pstFileFds) {
        bbox_print(PRINT_ERR,
            "BBOX_WriteFpxRegistersToFile parameters is invalid: pstFpxRegisters or pstFileFds is NULL.\n");
        return RET_ERR;
    }

    errno_t rc = memset_s(&stElfNhdr, sizeof(BBOX_NHDR), 0, sizeof(BBOX_NHDR));
    securec_check_c(rc, "\0", "\0");

    stElfNhdr.n_namesz = BBOX_LINUX_NAME_LENGTH;
    stElfNhdr.n_descsz = sizeof(struct BBOX_FPXREGSET);
    stElfNhdr.n_type = NT_PRXFPREG;

    if (BBOX_DoWrite(pstFileFds, &stElfNhdr, sizeof(BBOX_NHDR)) != sizeof(BBOX_NHDR) ||
        BBOX_DoWrite(pstFileFds, (void*)BBOX_LINUX_STRING, BBOX_CORE_STRING_LENGTH) != BBOX_CORE_STRING_LENGTH ||
        BBOX_DoWrite(pstFileFds, pstFpxRegisters, sizeof(struct BBOX_FPXREGSET)) != sizeof(struct BBOX_FPXREGSET)) {
        bbox_print(PRINT_ERR, "BBOX_DoWrite  is failed.\n");
        return RET_ERR;
    }

    return RET_OK;
}

/*
 * Writes the prpsstatus information and the register information for the process to the core file
 * in      : int iFpxRegistersFlag - Whether to write Fpx register information token
 *           struct BBOX_THREAD_NOTE_INFO *pstThreadPrPsStatusInfo - A pointer to the structure of thread note segment
 *           struct BBOX_WRITE_FDS *pstWriteFds - A pointer to the structure of the core file
 * return RET_OK or RET_ERR.
 */
static int BBOX_WriteThreadNoteToFile(
    int iFpxRegistersFlag, struct BBOX_THREAD_NOTE_INFO* pstThreadPrPsStatusInfo, struct BBOX_WRITE_FDS* pstFileFds)
{
    int iResult = RET_ERR;

    if (NULL == pstThreadPrPsStatusInfo || NULL == pstFileFds) {
        bbox_print(PRINT_ERR,
            "BBOX_WriteThreadNoteToFile parameters is invalid: pstThreadPrPsStatusInfo or pstFileFds is NULL.\n");
        return RET_ERR;
    }

    /* Write CPU register information to core file */
    iResult = BBOX_WritePrPsStatusToFile(&(pstThreadPrPsStatusInfo->stPrpsstatus), pstFileFds);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_WritePrPsStatusToFile is failed, iResult = %d.\n", iResult);
        return RET_ERR;
    }

    /* Write FPU register information to core file */
    iResult = BBOX_WriteFpRegistersToFile(&(pstThreadPrPsStatusInfo->stFpRegisters), pstFileFds);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_WriteFpRegistersToFile is failed, iResult = %d.\n", iResult);
        return RET_ERR;
    }

    if (BBOX_TRUE == iFpxRegistersFlag) {
        /* Write SSE information into core file if exist. */
        iResult = BBOX_WriteFpxRegistersToFile(&(pstThreadPrPsStatusInfo->stFpxRegisters), pstFileFds);
        if (RET_OK != iResult) {
            bbox_print(PRINT_ERR, "BBOX_WriteFpxRegistersToFile is failed, iResult = %d.\n", iResult);
            return RET_ERR;
        }
    }

    return RET_OK;
}

/*
 * To align, write 0 to the core file
 * in      : size_t uiNoteAlign  - The number of bytes that need to be written to the file for alignment
 *           struct BBOX_WRITE_FDS *pstWriteFds - A pointer to the structure of the core file
 */
static int BBOX_CoreFileAlignToPage(size_t uiNoteAlign, struct BBOX_WRITE_FDS* pstFileFds)
{
    ssize_t iWriteSize = -1;
    size_t iDateSize = 0;
    char acNoteAlign[BBOX_BUFF_LITTLE_SIZE];

    if (NULL == pstFileFds) {
        bbox_print(PRINT_ERR, "BBOX_CoreFileAlignToPage parameters is invalid: pstFileFds is NULL.\n");
        return RET_ERR;
    }

    while (uiNoteAlign > 0) {
        if (uiNoteAlign > sizeof(acNoteAlign)) {
            iDateSize = sizeof(acNoteAlign);
            uiNoteAlign -= sizeof(acNoteAlign);
        } else {
            iDateSize = uiNoteAlign;
            uiNoteAlign = 0;
        }

        errno_t rc = memset_s(acNoteAlign, BBOX_BUFF_LITTLE_SIZE, 0, sizeof(acNoteAlign));
        securec_check_c(rc, "\0", "\0");

        iWriteSize = BBOX_DoWrite(pstFileFds, acNoteAlign, iDateSize);
        if (iWriteSize == RET_ERR || iDateSize != (size_t)iWriteSize) {
            bbox_print(PRINT_ERR, "BBOX_DoWrite is failed, iWriteSize = %zd.\n", iWriteSize);

            return RET_ERR;
        }
    }

    bbox_print(PRINT_LOG, "Write Note align size = %zu to the core file success.\n", uiNoteAlign);

    return RET_OK;
}

/*
 * write Note segment into core file.
 * in      : struct BBOX_ELF_NOTE_INFO *pstNoteInfo - A pointer to the structure of the note segment
 *           struct BBOX_WRITE_FDS *pstWriteFds - A pointer to the structure of the core file
 */
static int BBOX_WriteNoteInfo(struct BBOX_ELF_NOTE_INFO* pstNoteInfo, struct BBOX_WRITE_FDS* pstFileFds)
{
    unsigned int uCount = 0;
    int iResult = RET_ERR;

    if (NULL == pstNoteInfo || NULL == pstFileFds) {
        bbox_print(PRINT_ERR,
            "BBOX_WriteNoteInfo parameters is invalid: pstNoteInfo or pstFileFds is NULL.\n");
        return RET_ERR;
    }

    int iThreadNum = pstNoteInfo->iThreadNoteInfoNum;
    struct BBOX_THREAD_NOTE_INFO* pstThreadPrPsStatusInfo = pstNoteInfo->pstThreadNoteInfo;
    struct BBOX_ELF_PRPSINFO* pstPrPsInfo = &(pstNoteInfo->stPrpsinfo);
    struct BBOX_CORE_USER* pstCoreUserInfo = &(pstNoteInfo->stCoreUser);

    /* write BBOX_ELF_PRPSINFO into core file */
    iResult = BBOX_WritePrPsinfoToFile(pstPrPsInfo, pstFileFds);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_WritePrPsinfoToFile is failed, iResult = %d.\n", iResult);
        return RET_ERR;
    }

    /* write user data into core file */
    iResult = BBOX_WriteUserRegistersToFile(pstCoreUserInfo, pstFileFds);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_WriteUserRegistersToFile is failed, iResult = %d.\n", iResult);
        return RET_ERR;
    }

    if (pstNoteInfo->iAuxvNoteInfoNum) {
        /* write Auxv into core */
        iResult = BBOX_WriteAuxvInfoToFile(pstNoteInfo->iAuxvNoteInfoNum, pstFileFds);
        if (RET_OK != iResult) {
            bbox_print(PRINT_ERR, "BBOX_WriteAuxvInfoToFile is failed, iResult = %d.\n", iResult);

            return RET_ERR;
        }
    }

    for (uCount = 0; uCount < (unsigned int)iThreadNum; uCount++) {
        if ((pstThreadPrPsStatusInfo + uCount)->stPrpsstatus.tpid == pstNoteInfo->tMainPid) {
            /* write primary process register information into core file. */
            iResult = BBOX_WriteThreadNoteToFile(
                pstNoteInfo->iFpxRegistersFlag, (pstThreadPrPsStatusInfo + uCount), pstFileFds);
            if (RET_OK != iResult) {
                bbox_print(PRINT_ERR, "BBOX_WriteThreadNoteToFile is failed, iResult = %d.\n", iResult);

                return RET_ERR;
            }
        }
    }

    for (uCount = 0; uCount < (unsigned int)iThreadNum; uCount++) {
        if ((pstThreadPrPsStatusInfo + uCount)->stPrpsstatus.tpid != pstNoteInfo->tMainPid) {
            /* write all non-primary process register information into core file. */
            iResult = BBOX_WriteThreadNoteToFile(
                pstNoteInfo->iFpxRegistersFlag, (pstThreadPrPsStatusInfo + uCount), pstFileFds);
            if (RET_OK != iResult) {
                bbox_print(PRINT_ERR, "BBOX_WriteThreadNoteToFile is failed, iResult = %d.\n", iResult);

                return RET_ERR;
            }
        }
    }

    bbox_print(PRINT_LOG, "Write Prpsstatus and Registers to the core file success.\n");

    /* align the Note segment */
    iResult = BBOX_CoreFileAlignToPage(pstNoteInfo->uiNoteAlign, pstFileFds);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_CoreFileAlignToPage is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    bbox_print(PRINT_LOG, "Write Note info to the core file success.\n");

    return RET_OK;
}

/*
 * read content of address space, and write it into core file.
 * in      : struct BBOX_VM_MAPS *pstVmMappingSegment - A pointer to the structure of the mapping segment
 *           int iVmMappingNum - The number of mapping segments in the address space
 *           union BBOX_VM_VDSO *pstVmVDSO - A pointer to the structure of the VDSO segment
 *           struct BBOX_WRITE_FDS *pstWriteFds - A pointer to core file
 * return RET_OK or RET_ERR
 */
static int BBOX_WriteElfVmToFile(struct BBOX_VM_MAPS* pstVmMappingSegment, int iVmMappingNum,
    union BBOX_VM_VDSO* pstVmVDSO, struct BBOX_WRITE_FDS* pstFileFds)
{
    unsigned int uCount = 0;
    size_t uiStartAddress = 0;
    size_t uiWriteSize = 0;
    ssize_t iResult = RET_ERR;

    if (NULL == pstVmMappingSegment || iVmMappingNum < 0 || NULL == pstFileFds || NULL == pstVmVDSO) {
        bbox_print(PRINT_ERR,
            "BBOX_WriteElfVmToFile parameters is invalid: iVmMappingNum = %d, " \
            "pstVmMappingSegment, pstFileFds or pstVmVDSO is NULL.\n",
            iVmMappingNum);

        return RET_ERR;
    }

    /* read content from start address to end address in address space, and write it into core file. */
    for (uCount = 0; uCount < (unsigned int)iVmMappingNum; uCount++) {
        uiStartAddress = pstVmMappingSegment[uCount].uiStartAddress;
        uiWriteSize = pstVmMappingSegment[uCount].uiWriteSize;

        if (pstVmMappingSegment[uCount].iIsRemoveFlags == BBOX_FALSE && uiWriteSize > 0) {
            iResult = BBOX_DoWrite(pstFileFds, (void*)uiStartAddress, uiWriteSize);
            if ((iResult == RET_ERR) || (iResult != (ssize_t)uiWriteSize)) {
                bbox_print(PRINT_ERR, "BBOX_DoWrite parameters is failed, iWriteSize = %zu.\n", uiWriteSize);

                return RET_ERR;
            }

            bbox_print(
                PRINT_DBG, "pstVmMappingSegment[%u] : write size = %zu to the core file.\n", uCount, uiWriteSize);
        }
    }

    if (pstVmVDSO->uiVDSOAddress) {
        BBOX_PHDR* pstVDSOPhdr = (BBOX_PHDR*)(pstVmVDSO->uiVDSOAddress + pstVmVDSO->pVDSOEhdr->e_phoff);
        for (uCount = 0; uCount < pstVmVDSO->pVDSOEhdr->e_phnum; uCount++) {
            /* wirte VDSO information into core file */
            BBOX_PHDR* pstVDSOTempPhdr = pstVDSOPhdr + uCount;
            if (PT_LOAD != pstVDSOTempPhdr->p_type) {
                iResult = BBOX_DoWrite(pstFileFds, (void*)pstVDSOTempPhdr->p_vaddr, pstVDSOTempPhdr->p_filesz);
                if ((iResult == RET_ERR) || (iResult != (ssize_t)(pstVDSOTempPhdr->p_filesz))) {
                    bbox_print(PRINT_ERR, "BBOX_DoWrite is failed, iResult = %zd.\n", iResult);
                    return RET_ERR;
                }

                bbox_print(PRINT_DBG, "VDSO[%u] : write size = %zd to the core file.\n", uCount, iResult);
            }
        }
    }

    bbox_print(PRINT_LOG, "Write Vm mapping segment to the core file success.\n");

    return RET_OK;
}

/*
 * write all segment into core file, include NOTE, mapping, VDSO
 * in      : struct BBOX_ELF_NOTE_INFO *pstNoteInfo - A pointer to the structure of the note segment
 *           struct BBOX_VM_MAPS *pstVmMappingSegment - A pointer to the structure of the mapping segment
 *           int iSegmentNum - The number of mapping segments in the address space
 *           union BBOX_VM_VDSO *pstVmVDSO - A pointer to the structure of the VDSO segment
 *           struct BBOX_WRITE_FDS *pstFileWriteFd - A pointer to core file
 */
static int BBOX_WriteElfSegment(struct BBOX_ELF_NOTE_INFO* pstNoteInfo, struct BBOX_VM_MAPS* pstVmMappingSegment,
    int iSegmentNum, union BBOX_VM_VDSO* pstVmVDSO, struct BBOX_WRITE_FDS* pstFileWriteFd)
{
    int iResult = RET_ERR;

    if (NULL == pstNoteInfo || NULL == pstVmMappingSegment || NULL == pstVmVDSO || NULL == pstFileWriteFd ||
        iSegmentNum <= 0) {
        bbox_print(PRINT_ERR,
            "BBOX_WriteElfSegment parameters is invalid: pstNoteInfo, pstVmMappingSegment, " \
            "pstVmVDSO or pstFileWriteFd is NULL, iSegmentNum = %d.\n",
            iSegmentNum);

        return RET_ERR;
    }

    /* write Note segment into core file */
    iResult = BBOX_WriteNoteInfo(pstNoteInfo, pstFileWriteFd);
    if (RET_OK != iResult) {

        bbox_print(PRINT_ERR, "BBOX_WriteNoteInfo is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    /* write process address space information into core file. */
    iResult = BBOX_WriteElfVmToFile(pstVmMappingSegment, iSegmentNum, pstVmVDSO, pstFileWriteFd);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_WriteElfVmToFile is failed, iResult = %d.\n", iResult);

        return RET_ERR;
    }

    return RET_OK;
}

/*
 * calculate end time of coredump and print the time coredump take.
 */
static void BBOX_CalculateUsedTime(void)
{
    long int iCoreDumpEndTime = 0;
    struct kernel_timeval stProgramCoreDumpTime = {0};

    /* calculate time. */
    sys_gettimeofday(&stProgramCoreDumpTime, NULL);
    iCoreDumpEndTime = stProgramCoreDumpTime.tv_sec;

    bbox_print(PRINT_TIP, "Coredump probably end at %ld\n", stProgramCoreDumpTime.tv_sec);
    bbox_print(PRINT_TIP, "Coredump used time: %ld sec\n", iCoreDumpEndTime - g_iCoreDumpBeginTime);
    bbox_print(PRINT_LOG, "Get information success.\n");
    bbox_print(PRINT_LOG, "Create core file success.\n");
}

/*
 * Write all section into core file.
 * in      : struct BBOX_ELF_SECTION *pstElfSectionInfo - pointer to the structure of a section
 *           struct BBOX_WRITE_FDS *pstFileWriteFd - pointer to the core file
 * return RET_OK or RET_ERR
 */
static int BBOX_WriteElfSection(struct BBOX_ELF_SECTION* pstElfSectionInfo, struct BBOX_WRITE_FDS* pstFileFds)
{
    unsigned int uiCount = 0;
    BBOX_SECTION_STRU* pstSection = NULL;

    /* print time and end information of core file. */
    BBOX_CalculateUsedTime();

    /* Write section into core file. */
    for (uiCount = 0; uiCount < pstElfSectionInfo->uiSectionNum; uiCount++) {
        pstSection = pstElfSectionInfo->pstSection + uiCount;
        BBOX_WRITE(pstFileFds, pstSection->pacSectionDesc, pstSection->uiSectionDescSize);
    }

    bbox_print(PRINT_LOG, "Write all section to fill successed.\n");

    return RET_OK;
}

/*
 * close core file
 * in      : struct BBOX_WRITE_FDS *pstFileWriteFd - Pointer to a structure that describes the properties of a core file.
 * return RET_OK or RET_ERR
 */
static int BBOX_CloseCoreFile(struct BBOX_WRITE_FDS* pstFileWriteFd)
{
    if (NULL == pstFileWriteFd) {
        bbox_print(PRINT_ERR, "BBOX_CloseCoreFile parameters is invalid: pstFileWriteFd is NULL.\n");

        return RET_ERR;
    }

    if (pstFileWriteFd->iWriteFd >= 0) {
        sys_pclose(pstFileWriteFd->iWriteFd);
        pstFileWriteFd->iWriteFd = -1;
    }

    bbox_print(PRINT_LOG, "Close core file success.\n");
    return RET_OK;
}

/*
 * create elf core file.
 * in     : BBOX_GetAllThreadDone pDone - The callback function for thawing
 *          void *pDoneHandle - The callback function parameter to be thawed
 *          int iNumThreads - The number of processes, that is, the length of the process number array
 *          pid_t *pPids - pointer to execute the process number array
 *          va_list ap - Multiparameter list
 * return RET_OK or RET_ERR
 */
int BBOX_DoDumpElfCore(BBOX_GetAllThreadDone pDone, void* pDoneHandle, int iNumThreads, pid_t* ptPids, va_list ap)
{
    int iSegmentNum = -1;
    int iResult = RET_ERR;
    int iCloseFile = RET_ERR;
    int iPhdrSum = 0;
    pid_t tMainPid = 0;
    Frame* pFrame = NULL;
    char* pFileName = NULL;
    union BBOX_VM_VDSO stVmVDSO;
    struct BBOX_ELF_NOTE_INFO stNoteInfo;
    struct BBOX_WRITE_FDS stFileWriteFd;
    errno_t rc = EOK;

    if (NULL == ptPids || NULL == pDone || NULL == pDoneHandle || iNumThreads <= 0) {
        bbox_print(PRINT_ERR,
            "BBOX_CloseCoreFile parameters is invalid: pDone, ptPids or pDoneHandle is NULL, " \
            "iNumThreads = %d.\n",
            iNumThreads);

        return RET_ERR;
    }

    /* Atomic variable lock to prevent reentry. */
    while (BBOX_AtomicIncReturn(&g_stLockBlackList) > 1) {
        BBOX_AtomicDec(&g_stLockBlackList);
        bbox_print(PRINT_DBG, "add blacklist addr is running, waiting.\n");
        sleep(1);
    }

    struct BBOX_THREAD_NOTE_INFO astThreadNoteInfo[iNumThreads];

    pFrame = (Frame*)va_arg(ap, Frame*);
    if (NULL == pFrame) {
        bbox_print(PRINT_ERR, "Get stack frame failed.\n");
        BBOX_AtomicDec(&g_stLockBlackList);
        return RET_ERR;
    }

    rc = memset_s(&stVmVDSO, sizeof(union BBOX_VM_VDSO), 0, sizeof(union BBOX_VM_VDSO));
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(&stNoteInfo, sizeof(struct BBOX_ELF_NOTE_INFO), 0, sizeof(struct BBOX_ELF_NOTE_INFO));
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(&stFileWriteFd, sizeof(struct BBOX_WRITE_FDS), 0, sizeof(struct BBOX_WRITE_FDS));
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(astThreadNoteInfo,
        iNumThreads * sizeof(struct BBOX_THREAD_NOTE_INFO),
        0,
        iNumThreads * sizeof(struct BBOX_THREAD_NOTE_INFO));
    securec_check_c(rc, "\0", "\0");

    stNoteInfo.pstThreadNoteInfo = astThreadNoteInfo;
    stNoteInfo.iThreadNoteInfoNum = iNumThreads; /* count of thread */
    stVmVDSO.uiVDSOAddress = 0;
    tMainPid = pFrame->tid;
    stNoteInfo.tMainPid = tMainPid;

    /* Get count of line in /proc/self/maps, it also is segment num. */
    iSegmentNum = BBOX_GetVmMapsNum();
    if (iSegmentNum <= 0) {
        bbox_print(PRINT_ERR, "BBOX_GetVmMapsNum is invald iSegmentNum = %d\n", iSegmentNum);
        BBOX_AtomicDec(&g_stLockBlackList);
        return RET_ERR;
    }

    bbox_print(PRINT_LOG, "Get Vm mapping number success, iSegmentNum = %d\n", iSegmentNum);

    /* define variable to record start address, end address, jurisdiction, offset and so on of segment in /proc/self/maps */
    struct BBOX_VM_MAPS astVmMappingSegment[iSegmentNum + BBOX_EXTERN_VM_MAX];

    rc = memset_s(astVmMappingSegment,
        (iSegmentNum + BBOX_EXTERN_VM_MAX) * sizeof(struct BBOX_VM_MAPS),
        0,
        (iSegmentNum + BBOX_EXTERN_VM_MAX) * sizeof(struct BBOX_VM_MAPS));
    securec_check_c(rc, "\0", "\0");

    /* Fill BBOX_VM_MAPS, BBOX_ELF_NOTE_INFO, BBOX_VM_VDSO, and write them into core file. */
    iResult = BBOX_FillAllInfoOfCoreFile(
        pFrame, astVmMappingSegment, &iSegmentNum, &iPhdrSum, ptPids, &stNoteInfo, &stVmVDSO);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_FillAllInfoOfCoreFile is failed, iResult = %d.\n", iResult);
        BBOX_AtomicDec(&g_stLockBlackList);
        return RET_ERR;
    }

    /* Notifie the thread module that the data retrieval is complete. */
    pDone(pDoneHandle);

    pFileName = (char*)va_arg(ap, char*);
    if (CheckFilenameValid(pFileName) == RET_ERR) {
        bbox_print(PRINT_ERR, "check core file name failed\n");
        return RET_ERR;
    }
    iResult = BBOX_OpenCoreFile(pFrame, pFileName, &stFileWriteFd); /* create core file. */
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_OpenCoreFile is failed, iResult = %d.\n", iResult);

        goto ERR;
    }

    /* fill core file header and write it into core file. */
    iResult = BBOX_WriteElfEhdr(iPhdrSum, &stFileWriteFd);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_WriteElfEhdr is failed, iResult = %d.\n", iResult);
        goto ERR;
    }

    /* Fill ElfPhdr and write it into core file. */
    iResult = BBOX_WriteElfHdr(iPhdrSum, &stNoteInfo, iSegmentNum, &stVmVDSO, astVmMappingSegment, &stFileWriteFd);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_WriteElfPhdr is failed, iResult = %d.\n", iResult);
        goto ERR;
    }

    /* Write Note segment and process address content into core file. */
    iResult = BBOX_WriteElfSegment(&stNoteInfo, astVmMappingSegment, iSegmentNum, &stVmVDSO, &stFileWriteFd);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_WriteElfSegment is failed, iResult = %d.\n", iResult);

        goto ERR;
    }

    /* Write BBOX_ELF_PRPSINFO into core file */
    iResult = BBOX_WriteElfSection(&g_stElfSectionInfo, &stFileWriteFd);
    if (RET_OK != iResult) {
        bbox_print(PRINT_ERR, "BBOX_WriteElfSection is failed, iResult = %d.\n", iResult);

        goto ERR;
    }

    bbox_print(PRINT_LOG, "Create core file success.\n");

ERR:
    BBOX_AtomicDec(&g_stLockBlackList);

    iCloseFile = BBOX_CloseCoreFile(&stFileWriteFd);
    if (RET_OK != iCloseFile) {
        bbox_print(PRINT_ERR, "BBOX_CloseCoreFile is failed, iCloseFile = %d.\n", iCloseFile);
    }

    return (iCloseFile == RET_OK) ? iResult : RET_ERR;
}

#ifdef __cplusplus
#if __cplusplus
}
#endif
#endif /* __cplusplus */
