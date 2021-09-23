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
 * aiocompleter.h
 *        Exports from postmaster/aiocompleter.c.
 * 
 * 
 * IDENTIFICATION
 *        src/include/postmaster/aiocompleter.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _AIOCOMPLETER_H
#define _AIOCOMPLETER_H

#include "storage/buf/block.h"
#include "storage/buf/bufmgr.h"
#include "storage/buf/buf_internals.h"
#include "storage/smgr/relfilenode.h"
#include "storage/smgr/smgr.h"
#include <libaio.h>

/*
 * requests/completers types
 */
typedef enum {
    // PageRangePrefetchType=0,
    PageListPrefetchType = 0,
    // PageRangeBackWriteType,
    PageListBackWriteType,
    CUListPrefetchType,
    CUListWriteType,
    NUM_AIOCOMPLTR_TYPES /* Number of types, must be last */
} AioCompltrType;

/*
 * request priorities (io nice values)
 */
typedef enum { DefaultPri = 0, HighPri = 1, MediumPri = 2, LowPri = 3 } AioPriority;

typedef enum { AioRead = 0, AioWrite = 1, AioVacummFull = 2, AioUnkown } AioDescType;

/*
 * Context for Page AIO completion
 */
typedef struct BlockDesc {
    SMgrRelation smgrReln;
    ForkNumber forkNum;
    BlockNumber blockNum;
    Block buffer;
    int32 blockSize;
    BufferDesc* bufHdr;
    AioCompltrType reqType;
    AioDescType descType;
} BlockDesc_t;

typedef struct AioCUDesc {
    char* buf;
    uint64 offset;  // use uint64 instead of CUPointer  for not include
    int size;       // read: align cu_size;  write: cu_size
    int fd;
    bool* io_error;
    int slotId;              // read means slotId of cucache; write means cuid
    volatile int io_finish;  // used for which has no cucache, ep write;
    uint64 cu_pointer;       // only used for async write,
    AioCompltrType reqType;
} AioCUDesc_t;

/*
 * The system iocb and BlockDesc_t are allocated together in an
 * AioDispatchDesc.
 */
typedef struct AioDispatchDesc {
    struct iocb aiocb;
    BlockDesc_t blockDesc;
} AioDispatchDesc_t;

typedef struct AioDispatchCUDesc {
    struct iocb aiocb;
    AioCUDesc_t cuDesc;
} AioDispatchCUDesc_t;

/* GUC options */
extern int AioCompltrSets;
extern int AioCompltrEvents;

extern void AioCompltrMain(int ac, char** av);
extern void AioCompltrStop(int signal);
extern int AioCompltrStart(void);
extern bool AioCompltrIsReady(void);
extern io_context_t CompltrContext(AioCompltrType reqType, int h);
extern short CompltrPriority(AioCompltrType reqType);

/*
 * These Storage Manager AIO prototypes would normally
 * go in smgr.h, and that file would include this one.
 * because the prototypes require the AioDispatchDesc_t.
 * However, it is not possible because this file must
 * include smgr.h itself.
 */
extern void smgrasyncread(SMgrRelation reln, ForkNumber forknum, AioDispatchDesc_t** dList, int32 dn);
extern void smgrasyncwrite(SMgrRelation reln, ForkNumber forknum, AioDispatchDesc_t** dList, int32 dn);

extern void mdasyncread(SMgrRelation reln, ForkNumber forkNum, AioDispatchDesc_t** dList, int32 dn);
extern void mdasyncwrite(SMgrRelation reln, ForkNumber forkNumber, AioDispatchDesc_t** dList, int32 dn);

extern void AioResourceInitialize(void);

#endif /* _AIOCOMPLETER_H */
