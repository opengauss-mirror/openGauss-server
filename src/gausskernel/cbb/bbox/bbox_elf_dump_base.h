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
 * bbox_elf_dump_base.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_elf_dump_base.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __BBOX_ELF_DUMP_BASE_H__
#define __BBOX_ELF_DUMP_BASE_H__

#include "bbox_atomic.h"
#include "bbox_print.h"

#ifdef __cplusplus
#if __cplusplus
extern "C" {
#endif
#endif /* __cplusplus */

#define BBOX_MSB_LSB_INT (0x0102)
#define BBOX_HIGH_BITS (1)
#define BBOX_LITTER_BITS (2)

#define SEC_CHANGE_MIRCO_SEC (1000)
#define DECIMALISM_SPAN (10)

#define ONE_HEXA_DECIMAL_BITS 4
#define ASC2_CHAR_GREATER_NUM 9

#define BBOX_BUFF_LITTLE_SIZE (8 * 1024)

/* max count of blacklist */
#define BBOX_BLACK_LIST_COUNT_MAX 128

/* every time you add a blacklist, you might add a segment */
#define BBOX_EXTERN_VM_MAX (BBOX_BLACK_LIST_COUNT_MAX)
#define BBOX_BLACK_LIST_MIN_LEN (8 * 1024)

#define BBOX_SECTION_NAME_LEN (32)
#define BBOX_ADD_CPU_INFO "CPUINFO"
#define BBOX_ADD_MEM_INFO "MEMINFO"
#define BBOX_ADD_NET_INFO "NETINFO"
#define BBOX_ADD_PS_INFO "PSINFO"
#define BBOX_SELF_STATUS_PATH "/proc/self/status"
#define BBOX_PROC_INTER_PATH "/proc/interrupts"
#define BBOX_PROC_MEMINFO_PATH "/proc/meminfo"
#define BBOX_PS_CMD \
    "ps --ppid 2 -p 2 --deselect f --sort -rss --cols 256 -o pid,ppid,uid,tty,stat,wchan,psr,time,rss,%mem,nlwp,cmd"

/* run fn until errno is not EINTR */
#define BBOX_NOINTR(fn) \
    do {                \
    } while ((fn) < 0 && errno == EINTR)

struct BBOX_ELF_TIMEVAL { /* Time value with microsecond resolution    */
    long lTvSec;          /* Seconds                                   */
    long lTvMicroSec;     /* Microseconds                              */
};

struct BBOX_READ_FILE_IO {
    int iFd;                                     /* fd of file  */
    unsigned char* pData;                        /* pointer of the charactor to be read in buffer */
    unsigned char* pEnd;                         /* pointer of buffer end */
    unsigned char szBuff[BBOX_BUFF_LITTLE_SIZE]; /* buffer to store result */
};

typedef struct BBOX_BLACKLIST {
    void* pBlackStartAddr; /* start address of blacklist */
    void* pBlackEndAddr;   /* end address of blacklist */
    unsigned long long uiLength; /* length of blacklist */
} BBOX_BLACKLIST_STRU;

extern BBOX_ATOMIC_STRU g_stLockBlackList; /* it will be locked when core dump */

extern int BBOX_DetermineMsb(void);
extern int BBOX_StringToTime(const char* pSwitch, struct BBOX_ELF_TIMEVAL* pstElfTimeval);
extern int BBOX_GetCharFromFile(struct BBOX_READ_FILE_IO* pstIO);
extern int BBOX_StringSwitchInt(struct BBOX_READ_FILE_IO* pstIO, size_t* pAddress);
extern int BBOX_SkipToLineEnd(struct BBOX_READ_FILE_IO* pstReadIO);

/* find if a memory segment overlaps with the memory segment in the blacklist */
extern BBOX_BLACKLIST_STRU* _BBOX_FindAddrInBlackList(const void* pStartAddress, const void* pEndAddress);

/* add blacklist address */
extern int _BBOX_AddBlackListAddress(void* pAddress, unsigned long long uiLen);

/* remove blacklist address */
extern int _BBOX_RmvBlackListAddress(void* pAddress);

/* get additional system information */
extern int _BBOX_GetAddonInfo(char* pBuffer, unsigned int uiBufLen);

/* set compression ratio */
extern int _BBOX_SetCompressRatio(unsigned int uiRatio);

/* get compression ratio */
extern unsigned int _BBOX_GetCompressRatio(void);

#ifdef __cplusplus
#if __cplusplus
}
#endif
#endif /* __cplusplus */

#endif
