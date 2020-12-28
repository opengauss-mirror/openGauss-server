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
 * xlogproc.h
 *
 *
 * IDENTIFICATION
 *        src/include/access/xlogproc.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef XLOG_PROC_H
#define XLOG_PROC_H
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xlogreader.h"
#include "storage/buf/bufmgr.h"
#include "access/xlog_basic.h"
#include "access/xlogutils.h"
#include "access/clog.h"

#ifndef byte
#define byte unsigned char
#endif

typedef void (*relasexlogreadstate)(void* record);
/* **************define for parse end******************************* */
#define MIN(_a, _b) ((_a) > (_b) ? (_b) : (_a))

/* for common blockhead  begin  */

#define XLogBlockHeadGetInfo(blockhead) ((blockhead)->xl_info)
#define XLogBlockHeadGetXid(blockhead) ((blockhead)->xl_xid)
#define XLogBlockHeadGetRmid(blockhead) ((blockhead)->xl_rmid)

#define XLogBlockHeadGetLSN(blockhead) ((blockhead)->end_ptr)
#define XLogBlockHeadGetRelNode(blockhead) ((blockhead)->relNode)
#define XLogBlockHeadGetSpcNode(blockhead) ((blockhead)->spcNode)
#define XLogBlockHeadGetDbNode(blockhead) ((blockhead)->dbNode)
#define XLogBlockHeadGetForkNum(blockhead) ((blockhead)->forknum)
#define XLogBlockHeadGetBlockNum(blockhead) ((blockhead)->blkno)
#define XLogBlockHeadGetBucketId(blockhead) ((blockhead)->bucketNode)
#define XLogBlockHeadGetValidInfo(blockhead) ((blockhead)->block_valid)

/* for common blockhead end  */

/* for block data beging  */
#define XLogBlockDataHasBlockImage(blockdata) ((blockdata)->blockhead.has_image)
#define XLogBlockDataHasBlockData(blockdata) ((blockdata)->blockhead.has_data)
#define XLogBlockDataGetLastBlockLSN(_blockdata) ((_blockdata)->blockdata.last_lsn)
#define XLogBlockDataGetBlockFlags(blockdata) ((blockdata)->blockhead.flags)

#define XLogBlockDataGetBlockId(blockdata) ((blockdata)->blockhead.cur_block_id)
#define XLogBlockDataGetAuxiBlock1(blockdata) ((blockdata)->blockhead.auxiblk1)
#define XLogBlockDataGetAuxiBlock2(blockdata) ((blockdata)->blockhead.auxiblk2)
/* for block data end  */

typedef struct {
    RelFileNode rnode;
    ForkNumber forknum;
    BlockNumber blkno;
} RedoBufferTag;

typedef struct {
    Page page;  // pagepointer
    Size pagesize;
} RedoPageInfo;

typedef struct {
    XLogRecPtr lsn; /* block cur lsn */
    Buffer buf;
    RedoBufferTag blockinfo;
    RedoPageInfo pageinfo;
    int dirtyflag; /* true if the buffer changed */
} RedoBufferInfo;

#define MakeRedoBufferDirty(bufferinfo) ((bufferinfo)->dirtyflag = true)
#define RedoBufferDirtyClear(bufferinfo) ((bufferinfo)->dirtyflag = false)
#define IsRedoBufferDirty(bufferinfo) ((bufferinfo)->dirtyflag == true)

#define RedoMemIsValid(memctl, bufferid) (((bufferid) > InvalidBuffer) && ((bufferid) <= (memctl->totalblknum)))

typedef struct {
    RedoBufferTag blockinfo;
    pg_atomic_uint32 state;
} RedoBufferDesc;

typedef struct {
    Buffer buff_id;
    pg_atomic_uint32 state;
} ParseBufferDesc;

#define RedoBufferSlotGetBuffer(bslot) ((bslot)->buf_id)

#define EnalbeWalLsnCheck true

#pragma pack(push, 1)

#define INVALID_BLOCK_ID (XLR_MAX_BLOCK_ID + 2)

#define LOW_BLOKNUMBER_BITS (32)
#define LOW_BLOKNUMBER_MASK (((uint64)1 << 32) - 1)


/* ********BLOCK COMMON HEADER  BEGIN ***************** */
typedef enum {
    BLOCK_DATA_MAIN_DATA_TYPE = 0,     /* BLOCK DATA */
    BLOCK_DATA_VM_TYPE,           /* VM */
    BLOCK_DATA_FSM_TYPE,          /* FSM */
    BLOCK_DATA_DDL_TYPE,          /* DDL */
    BLOCK_DATA_BCM_TYPE,          /* bcm */
    BLOCK_DATA_NEWCU_TYPE,        /* cu newlog */
    BLOCK_DATA_CLOG_TYPE,         /* CLog */
    BLOCK_DATA_MULITACT_OFF_TYPE, /* MultiXact */
    BLOCK_DATA_MULITACT_MEM_TYPE,
    BLOCK_DATA_CSNLOG_TYPE, /* CSNLog */
    /* *****xact don't need sent to dfv  */
    BLOCK_DATA_MULITACT_UPDATEOID_TYPE,
    BLOCK_DATA_XACTDATA_TYPE, /* XACT */
    BLOCK_DATA_RELMAP_TYPE,   /* RELMAP */
    BLOCK_DATA_SLOT_TYPE,
    BLOCK_DATA_BARRIER_TYPE,
    BLOCK_DATA_PREPARE_TYPE,    /* prepare */
    BLOCK_DATA_INVALIDMSG_TYPE, /* INVALIDMSG */
    BLOCK_DATA_INCOMPLETE_TYPE,
    BLOCK_DATA_VACUUM_PIN_TYPE,
    BLOCK_DATA_XLOG_COMMON_TYPE,
    BLOCK_DATA_CREATE_DATABASE_TYPE,
    BLOCK_DATA_DROP_DATABASE_TYPE,
    BLOCK_DATA_CREATE_TBLSPC_TYPE,
    BLOCK_DATA_DROP_TBLSPC_TYPE,
    BLOCK_DATA_DROP_SLICE_TYPE,
} XLogBlockParseEnum;

/* ********BLOCK COMMON HEADER  END ***************** */

/* **************define for parse begin ******************************* */

/* ********BLOCK DATE BEGIN ***************** */

typedef struct {
    uint8 cur_block_id; /* blockid */
    uint8 flags;
    uint8 has_image;
    uint8 has_data;
    BlockNumber auxiblk1;
    BlockNumber auxiblk2;
} XLogBlocDatakHead;

#define XLOG_BLOCK_DATAHEAD_LEN sizeof(XLogBlocDatakHead)

typedef struct {
    uint16 extra_flag;
    uint16 hole_offset;
    uint16 hole_length; /* image position */
    uint16 data_len;    /* data length */
    XLogRecPtr last_lsn;
    char* bkp_image;
    char* data;
} XLogBlockData;

#define XLOG_BLOCK_DATA_LEN sizeof(XLogBlockData)

typedef struct {
    XLogBlocDatakHead blockhead;
    XLogBlockData blockdata;
    uint32 main_data_len; /* main data portion's length */
    char* main_data;      /* point to XLogReaderState's main_data */
} XLogBlockDataParse;
/* ********BLOCK DATE END ***************** */
#define XLOG_BLOCK_DATA_PARSE_LEN sizeof(XLogBlockDataParse)

/* ********BLOCK DDL BEGIN ***************** */
typedef enum {
    BLOCK_DDL_TYPE_NONE  = 0,
    BLOCK_DDL_CREATE_RELNODE,
    BLOCK_DDL_DROP_RELNODE,
    BLOCK_DDL_EXTEND_RELNODE,
    BLOCK_DDL_TRUNCATE_RELNODE,
    BLOCK_DDL_CLOG_ZERO,
    BLOCK_DDL_CLOG_TRUNCATE,
    BLOCK_DDL_MULTIXACT_OFF_ZERO,
    BLOCK_DDL_MULTIXACT_MEM_ZERO
} XLogBlockDdlInfoEnum;

typedef struct {
    uint32 blockddltype;
    uint32 columnrel;
    Oid ownerid;
    char *mainData;
} XLogBlockDdlParse;

/* ********BLOCK DDL END ***************** */

/* ********BLOCK CLOG BEGIN ***************** */

#define MAX_BLOCK_XID_NUMS (28)
typedef struct {
    TransactionId topxid;
    uint16 status;
    uint16 xidnum;
    uint16 xidsarry[MAX_BLOCK_XID_NUMS];
} XLogBlockCLogParse;

/* ********BLOCK CLOG END ***************** */

/* ********BLOCK CSNLOG BEGIN ***************** */
typedef struct {
    TransactionId topxid;
    CommitSeqNo cslseq;
    uint32 xidnum;
    uint16 xidsarry[MAX_BLOCK_XID_NUMS];
} XLogBlockCSNLogParse;

/* ********BLOCK CSNLOG END ***************** */

/* ********BLOCK prepare BEGIN ***************** */
struct TwoPhaseFileHeader;

typedef struct {
    TransactionId maxxid;
    Size maindatalen;
    char* maindata;
} XLogBlockPrepareParse;

/* ********BLOCK prepare  END ***************** */

/* ********BLOCK Bcm BEGIN ***************** */
typedef struct {
    uint64 startblock;
    int count;
    int status;
} XLogBlockBcmParse;

/* ********BLOCK Bcm   END ***************** */

/* ********BLOCK Vm BEGIN ***************** */
typedef struct {
    BlockNumber heapBlk;
} XLogBlockVmParse;

#define XLOG_BLOCK_VM_PARSE_LEN sizeof(XLogBlockVmParse)
/* ********BLOCK Vm   END ***************** */

/* ********BLOCK NewCu BEGIN ***************** */
typedef struct {
    uint32 main_data_len; /* main data portion's length */
    char* main_data;      /* point to XLogReaderState's main_data */
} XLogBlockNewCuParse;


/* ********BLOCK NewCu   END ***************** */

/* ********BLOCK InvalidMsg BEGIN ***************** */
typedef struct {
    TransactionId cutoffxid;
} XLogBlockInvalidParse;

/* ********BLOCK   InvalidMsg END ***************** */

/* ********BLOCK Incomplete BEGIN ***************** */

typedef enum {
    INCOMPLETE_ACTION_LOG = 0,
    INCOMPLETE_ACTION_FORGET
} XLogBlockIncompleteEnum;

typedef struct {
    uint16 action; /* 	split or delete */
    bool issplit;
    bool isroot;
    BlockNumber downblk;
    BlockNumber leftblk;
    BlockNumber rightblk;
} XLogBlockIncompleteParse;

/* ********BLOCK   Incomplete END ***************** */

/* ********BLOCK VacuumPin BEGIN ***************** */
typedef struct {
    BlockNumber lastBlockVacuumed;
} XLogBlockVacuumPinParse;

/* ********BLOCK XLOG   Common BEGIN ***************** */
typedef struct {
    XLogRecPtr readrecptr;
    Size maindatalen;
    char* maindata;
} XLogBlockXLogComParse;

/* ********BLOCK XLOG   Common END ***************** */

/* ********BLOCK DataBase BEGIN ***************** */
typedef struct {
    Oid src_db_id;
    Oid src_tablespace_id;
} XLogBlockDataBaseParse;

/* ********BLOCK DataBase   Common END ***************** */

/* ********BLOCK table spc BEGIN ***************** */
typedef struct {
    char* tblPath;
    bool isRelativePath;
} XLogBlockTblSpcParse;

/* ********BLOCK table spc END ***************** */

/* ********BLOCK Multi Xact Offset BEGIN ***************** */
typedef struct {
    MultiXactId multi;
    MultiXactOffset moffset;
} XLogBlockMultiXactOffParse;

/* ********BLOCK Multi Xact Offset END ***************** */

/* ********BLOCK Multi Xact Mem BEGIN ***************** */
typedef struct {
    MultiXactId multi;
    MultiXactOffset startoffset;
    uint64 xidnum;
    TransactionId xidsarry[MAX_BLOCK_XID_NUMS];
} XLogBlockMultiXactMemParse;
/* ********BLOCK Multi Xact Mem END ***************** */

/* ********BLOCK Multi Xact update oid BEGIN ***************** */
typedef struct {
    MultiXactId nextmulti;
    MultiXactOffset nextoffset;
    TransactionId maxxid;
} XLogBlockMultiUpdateParse;
/* ********BLOCK Multi Xact update oid END ***************** */

/* ********BLOCK rel map BEGIN ***************** */
typedef struct {
    Size maindatalen;
    char* maindata;
} XLogBlockRelMapParse;
/* ********BLOCK rel map END ***************** */

typedef struct {
    uint32 xl_term;
} XLogBlockRedoHead;

#define XLogRecRedoHeadEncodeSize (offsetof(XLogBlockRedoHead, refrecord))
typedef struct {
    XLogRecPtr start_ptr;
    XLogRecPtr end_ptr; /* copy from XLogReaderState's EndRecPtr */    
    BlockNumber blkno;
    Oid relNode;        /* relation */
    uint16 block_valid; /* block data validinfo see XLogBlockInfoEnum */
    uint8 xl_info;      /* flag bits, see below */
    RmgrId xl_rmid;     /* resource manager for this record */
    ForkNumber forknum;
    TransactionId xl_xid; /* xact id */
    Oid spcNode;          /* tablespace */
    Oid dbNode;           /* database */
    int4 bucketNode;      /* bucket   */
} XLogBlockHead;

#define XLogBlockHeadEncodeSize (sizeof(XLogBlockHead))

#define BYTE_NUM_BITS (8)
#define BYTE_MASK (0xFF)
#define U64_BYTES_NUM (8)
#define U32_BYTES_NUM (4)
#define U16_BYTES_NUM (2)
#define U8_BYTES_NUM (1)

#define U32_BITS_NUM (BYTE_NUM_BITS * U32_BYTES_NUM)

extern uint64 XLog_Read_N_Bytes(char* buffer, Size buffersize, Size readbytes);

#define XLog_Read_1_Bytes(buffer, buffersize) XLog_Read_N_Bytes(buffer, buffersize, U8_BYTES_NUM)
#define XLog_Read_2_Bytes(buffer, buffersize) XLog_Read_N_Bytes(buffer, buffersize, U16_BYTES_NUM)
#define XLog_Read_4_Bytes(buffer, buffersize) XLog_Read_N_Bytes(buffer, buffersize, U32_BYTES_NUM)
#define XLog_Read_8_Bytes(buffer, buffersize) XLog_Read_N_Bytes(buffer, buffersize, U64_BYTES_NUM)

extern bool XLog_Write_N_bytes(uint64 values, Size writebytes, byte* buffer);

#define XLog_Write_1_Bytes(values, buffer) XLog_Write_N_bytes(values, U8_BYTES_NUM, buffer)
#define XLog_Write_2_Bytes(values, buffer) XLog_Write_N_bytes(values, U16_BYTES_NUM, buffer)
#define XLog_Write_4_Bytes(values, buffer) XLog_Write_N_bytes(values, U32_BYTES_NUM, buffer)
#define XLog_Write_8_Bytes(values, buffer) XLog_Write_N_bytes(values, U64_BYTES_NUM, buffer)

typedef struct XLogBlockEnCode {
    bool (*xlog_encodefun)(byte* buffer, Size buffersize, Size* encodesize, void* xlogbody);
    uint16 block_valid;
} XLogBlockEnCode;

typedef struct XLogBlockRedoCode {
    void (*xlog_redofun)(char* buffer, Size buffersize, XLogBlockHead* blockhead, XLogBlockRedoHead* redohead,
        void* page, Size pagesize);
    uint16 block_valid;
} XLogBlockRedoCode;

#pragma pack(pop)

/* ********BLOCK Xact BEGIN ***************** */
typedef struct {
    uint8 delayddlflag;
    uint8 updateminrecovery;
    uint16 committype;
    int invalidmsgnum;
    int nrels; /* delete rels */
    int nlibs; /* delete libs */
    uint64 xinfo;
    TimestampTz xact_time;
    TransactionId maxxid;
    CommitSeqNo maxcommitseq;
    void* invalidmsg;
    void* xnodes;
    void* libfilename;
} XLogBlockXactParse;

typedef struct {
    Size maindatalen;
    char* maindata;
} XLogBlockSlotParse;
/* ********BLOCK slot END ***************** */

/* ********BLOCK barrier BEGIN ***************** */
typedef struct {
    XLogRecPtr startptr;
    XLogRecPtr endptr;
} XLogBlockBarrierParse;

/* ********BLOCK Xact  END ***************** */

/* ********BLOCK   VacuumPin END ***************** */
typedef struct {
    XLogBlockHead blockhead;
    XLogBlockRedoHead redohead;
    union {
        XLogBlockDataParse blockdatarec;
        XLogBlockVmParse blockvmrec;
        XLogBlockDdlParse blockddlrec;
        XLogBlockBcmParse blockbcmrec;
        XLogBlockNewCuParse blocknewcu;
        XLogBlockCLogParse blockclogrec;
        XLogBlockCSNLogParse blockcsnlogrec;
        XLogBlockXactParse blockxact;
        XLogBlockPrepareParse blockprepare;
        XLogBlockInvalidParse blockinvalidmsg;
        // XLogBlockIncompleteParse blockincomplete;
        XLogBlockVacuumPinParse blockvacuumpin;
        XLogBlockXLogComParse blockxlogcommon;
        XLogBlockDataBaseParse blockdatabase;
        XLogBlockTblSpcParse blocktblspc;
        XLogBlockMultiXactOffParse blockmultixactoff;
        XLogBlockMultiXactMemParse blockmultixactmem;
        XLogBlockMultiUpdateParse blockmultiupdate;
        XLogBlockRelMapParse blockrelmap;
        XLogBlockSlotParse blockslot;
        XLogBlockBarrierParse blockbarrier;
    } extra_rec;
} XLogBlockParse;


typedef struct
{
    Buffer			buf_id;
	Buffer			freeNext;
} RedoMemSlot;

typedef void (*InterruptFunc)();

typedef struct
{
	int    totalblknum;    /* total slot */
	int    usedblknum;     /* used slot */
	Size   itemsize;
	Buffer firstfreeslot;  /* first free slot */
	Buffer firstreleaseslot;  /* first release slot */
	RedoMemSlot *memslot;  /* slot itme */
	bool  isInit;
	InterruptFunc doInterrupt;
}RedoMemManager;

typedef void (*RefOperateFunc)(void *record);
#ifdef USE_ASSERT_CHECKING
typedef void (*RecordCheckFunc)(void *record, XLogRecPtr curPageLsn, uint32 blockId, bool replayed);
#endif

typedef struct {
    RefOperateFunc refCount;
    RefOperateFunc DerefCount;
#ifdef USE_ASSERT_CHECKING
    RecordCheckFunc checkFunc;
#endif
}RefOperate;

typedef struct
{
    void *BufferBlockPointers;   /* RedoBufferDesc + block */
	RedoMemManager memctl;
	RefOperate *refOperate;
}RedoBufferManager;



typedef struct
{
    void   *parsebuffers; /* ParseBufferDesc + XLogRecParseState */
	RedoMemManager memctl;
	RefOperate *refOperate;
}RedoParseManager;



typedef struct {
    void* nextrecord;
    XLogBlockParse blockparse; /* block data  */	
    RedoParseManager* manager;
    void* refrecord; /* origin dataptr, for mem release */
	uint64 batchcount;
	bool isFullSyncCheckpoint;
} XLogRecParseState;

typedef struct XLogBlockRedoExtreRto {
    void (*xlog_redoextrto)(XLogBlockHead* blockhead, void* blockrecbody, RedoBufferInfo* bufferinfo);
    uint16 block_valid;
} XLogBlockRedoExtreRto;

typedef struct XLogParseBlock {
    XLogRecParseState* (*xlog_parseblock)(XLogReaderState* record, uint32* blocknum);
    RmgrId rmid;
} XLogParseBlock;

typedef enum {
    HEAP_INSERT_ORIG_BLOCK_NUM = 0
} XLogHeapInsertBlockEnum;

typedef enum {
    HEAP_DELETE_ORIG_BLOCK_NUM = 0
} XLogHeapDeleteBlockEnum;

typedef enum {
    HEAP_UPDATE_NEW_BLOCK_NUM = 0,
    HEAP_UPDATE_OLD_BLOCK_NUM
} XLogHeapUpdateBlockEnum;

typedef enum {
    HEAP_BASESHIFT_ORIG_BLOCK_NUM = 0
} XLogHeapBaeShiftBlockEnum;

typedef enum {
    HEAP_NEWPAGE_ORIG_BLOCK_NUM = 0
} XLogHeapNewPageBlockEnum;

typedef enum {
    HEAP_LOCK_ORIG_BLOCK_NUM = 0
} XLogHeapLockBlockEnum;

typedef enum {
    HEAP_INPLACE_ORIG_BLOCK_NUM = 0
} XLogHeapInplaceBlockEnum;

typedef enum {
    HEAP_FREEZE_ORIG_BLOCK_NUM = 0
} XLogHeapFreezeBlockEnum;

typedef enum {
    HEAP_CLEAN_ORIG_BLOCK_NUM = 0
} XLogHeapCleanBlockEnum;

typedef enum {
    HEAP_VISIBLE_VM_BLOCK_NUM = 0,
    HEAP_VISIBLE_DATA_BLOCK_NUM
} XLogHeapVisibleBlockEnum;

typedef enum {
    HEAP_MULTI_INSERT_ORIG_BLOCK_NUM = 0
} XLogHeapMultiInsertBlockEnum;

typedef enum {
    HEAP_PAGE_UPDATE_ORIG_BLOCK_NUM = 0
} XLogHeapPageUpdateBlockEnum;

extern THR_LOCAL RedoParseManager* g_parseManager;
extern THR_LOCAL RedoBufferManager* g_bufferManager;

extern void* XLogMemCtlInit(RedoMemManager* memctl, Size itemsize, int itemnum);
extern RedoMemSlot* XLogMemAlloc(RedoMemManager* memctl);
extern void XLogMemRelease(RedoMemManager* memctl, Buffer bufferid);

extern void XLogRedoBufferInit(RedoBufferManager* buffermanager, int buffernum, RefOperate *refOperate, 
    InterruptFunc interruptOperte);
extern void XLogRedoBufferDestory(RedoBufferManager* buffermanager);
extern RedoMemSlot* XLogRedoBufferAlloc(
    RedoBufferManager* buffermanager, RelFileNode relnode, ForkNumber forkNum, BlockNumber blockNum);
extern bool XLogRedoBufferIsValid(RedoBufferManager* buffermanager, Buffer bufferid);
extern void XLogRedoBufferRelease(RedoBufferManager* buffermanager, Buffer bufferid);
extern BlockNumber XLogRedoBufferGetBlkNumber(RedoBufferManager* buffermanager, Buffer bufferid);
extern Block XLogRedoBufferGetBlk(RedoBufferManager* buffermanager, RedoMemSlot* bufferslot);
extern Block XLogRedoBufferGetPage(RedoBufferManager* buffermanager, Buffer bufferid);
extern void XLogRedoBufferSetState(RedoBufferManager* buffermanager, RedoMemSlot* bufferslot, uint32 state);

#define XLogRedoBufferInitFunc(bufferManager, buffernum, defOperate, interruptOperte) do { \
    XLogRedoBufferInit(bufferManager, buffernum, defOperate, interruptOperte); \
} while (0)
#define XLogRedoBufferDestoryFunc(bufferManager) do { \
    XLogRedoBufferDestory(bufferManager); \
} while (0)
#define XLogRedoBufferAllocFunc(relnode, forkNum, blockNum, bufferslot) do { \
    *bufferslot = XLogRedoBufferAlloc(g_bufferManager, relnode, forkNum, blockNum); \
} while (0)
#define XLogRedoBufferIsValidFunc(bufferid, isvalid) do { \
    *isvalid = XLogRedoBufferIsValid(g_bufferManager, bufferid); \
} while (0)
#define XLogRedoBufferReleaseFunc(bufferid) do { \
    XLogRedoBufferRelease(g_bufferManager, bufferid); \
} while (0)

#define XLogRedoBufferGetBlkNumberFunc(bufferid, blknumber) do { \
    *blknumber = XLogRedoBufferGetBlkNumber(g_bufferManager, bufferid); \
} while (0)

#define XLogRedoBufferGetBlkFunc(bufferslot, blockdata) do { \
    *blockdata = XLogRedoBufferGetBlk(g_bufferManager, bufferslot); \
} while (0)

#define XLogRedoBufferGetPageFunc(bufferid, blockdata) do { \
    *blockdata = (Page)XLogRedoBufferGetPage(g_bufferManager, bufferid); \
} while (0)
#define XLogRedoBufferSetStateFunc(bufferslot, state) do { \
    XLogRedoBufferSetState(g_bufferManager, bufferslot, state); \
} while (0)

extern void XLogParseBufferInit(RedoParseManager* parsemanager, int buffernum, RefOperate *refOperate, 
    InterruptFunc interruptOperte);
extern void XLogParseBufferDestory(RedoParseManager* parsemanager);
extern void XLogParseBufferRelease(XLogRecParseState* recordstate);
extern XLogRecParseState* XLogParseBufferAllocList(RedoParseManager* parsemanager, XLogRecParseState* blkstatehead, void *record);
extern XLogRedoAction XLogReadBufferForRedo(XLogReaderState* record, uint8 buffer_id, RedoBufferInfo* bufferinfo);
extern void XLogInitBufferForRedo(XLogReaderState* record, uint8 block_id, RedoBufferInfo* bufferinfo);
extern XLogRedoAction XLogReadBufferForRedoExtended(XLogReaderState* record, uint8 buffer_id, ReadBufferMode mode,
    bool get_cleanup_lock, RedoBufferInfo* bufferinfo, ReadBufferMethod readmethod = WITH_NORMAL_CACHE);

#define XLogParseBufferInitFunc(parseManager, buffernum, defOperate, interruptOperte) do { \
    XLogParseBufferInit(parseManager, buffernum, defOperate, interruptOperte); \
} while (0)

#define XLogParseBufferDestoryFunc(parseManager) do { \
    XLogParseBufferDestory(parseManager); \
} while (0)

#define XLogParseBufferReleaseFunc(recordstate) do { \
    XLogParseBufferRelease(recordstate);    \
} while (0)

#define XLogParseBufferAllocListFunc(record, newblkstate, blkstatehead) do { \
    *newblkstate = XLogParseBufferAllocList(g_parseManager, blkstatehead, record); \
} while (0)

#define XLogParseBufferAllocListStateFunc(record, newblkstate, blkstatehead) do { \
    if (*blkstatehead == NULL) {                                                   \
        *newblkstate = XLogParseBufferAllocList(g_parseManager, NULL, record);          \
        *blkstatehead = *newblkstate;                                              \
    } else {                                                                       \
        *newblkstate = XLogParseBufferAllocList(g_parseManager, *blkstatehead, record); \
    }                                                                              \
} while (0)




#ifdef EXTREME_RTO_DEBUG_AB
typedef void (*AbnormalProcFunc)(void);
typedef enum {
    A_THREAD_EXIT,
    ALLOC_FAIL,
    OPEN_FILE_FAIL,
    WAIT_LONG,
    ABNORMAL_NUM,
}AbnormalType;
extern AbnormalProcFunc g_AbFunList[ABNORMAL_NUM];


#define ADD_ABNORMAL_POSITION(pos) do {                                                         \
    static int __count##pos = 0;                                                                \
    __count##pos++;                                                                                  \
    if (g_instance.attr.attr_storage.extreme_rto_ab_pos == pos) {                        \
        if (g_instance.attr.attr_storage.extreme_rto_ab_count == __count##pos) {                \
            ereport(LOG, (errmsg("extreme rto debug abnormal stop pos:%d, type:%d, count:%d", pos, \
                g_instance.attr.attr_storage.extreme_rto_ab_type, __count##pos)));                \
            g_AbFunList[g_instance.attr.attr_storage.extreme_rto_ab_type % ABNORMAL_NUM]();     \
        }                                                                                       \
    }                                                                                           \
} while(0)
#else
#define ADD_ABNORMAL_POSITION(pos)
#endif



void HeapXlogCleanOperatorPage(
    RedoBufferInfo* buffer, void* recorddata, void* blkdata, Size datalen, Size* freespace, bool repairFragmentation);
void HeapXlogFreezeOperatorPage(RedoBufferInfo* buffer, void* recorddata, void* blkdata, Size datalen);
void HeapXlogVisibleOperatorPage(RedoBufferInfo* buffer, void* recorddata);
void HeapXlogVisibleOperatorVmpage(RedoBufferInfo* vmbuffer, void* recorddata);
void HeapXlogDeleteOperatorPage(RedoBufferInfo* buffer, void* recorddata, TransactionId recordxid);
void HeapXlogInsertOperatorPage(RedoBufferInfo* buffer, void* recorddata, bool isinit, void* blkdata, Size datalen,
    TransactionId recxid, Size* freespace);
void HeapXlogMultiInsertOperatorPage(RedoBufferInfo* buffer, void* recoreddata, bool isinit, void* blkdata,
    Size len, TransactionId recordxid, Size* freespace);
void HeapXlogUpdateOperatorOldpage(RedoBufferInfo* buffer, void* recoreddata, bool hot_update, bool isnewinit,
    BlockNumber newblk, TransactionId recordxid);
void HeapXlogUpdateOperatorNewpage(RedoBufferInfo* buffer, void* recorddata, bool isinit, void* blkdata,
    Size datalen, TransactionId recordxid, Size* freespace);
void HeapXlogPageUpgradeOperatorPage(RedoBufferInfo* buffer);
void HeapXlogLockOperatorPage(RedoBufferInfo* buffer, void* recorddata);
void HeapXlogInplaceOperatorPage(RedoBufferInfo* buffer, void* recorddata, void* blkdata, Size newlen);
void HeapXlogBaseShiftOperatorPage(RedoBufferInfo* buffer, void* recorddata);

void BtreeRestoreMetaOperatorPage(RedoBufferInfo* metabuf, void* recorddata, Size datalen);
void BtreeXlogInsertOperatorPage(RedoBufferInfo* buffer, void* recorddata, void* data, Size datalen);
void BtreeXlogSplitOperatorRightpage(
    RedoBufferInfo* rbuf, void* recorddata, BlockNumber leftsib, BlockNumber rnext, void* blkdata, Size datalen);
void BtreeXlogSplitOperatorNextpage(RedoBufferInfo* buffer, BlockNumber rightsib);
void BtreeXlogSplitOperatorLeftpage(
    RedoBufferInfo* lbuf, void* recorddata, BlockNumber rightsib, bool onleft, void* blkdata, Size datalen);
void BtreeXlogVacuumOperatorPage(RedoBufferInfo* redobuffer, void* recorddata, void* blkdata, Size len);
void BtreeXlogDeleteOperatorPage(RedoBufferInfo* buffer, void* recorddata, Size recorddatalen);
void btreeXlogDeletePageOperatorRightpage(RedoBufferInfo* buffer, void* recorddata);

void BtreeXlogDeletePageOperatorLeftpage(RedoBufferInfo* buffer, void* recorddata);

void BtreeXlogDeletePageOperatorCurrentpage(RedoBufferInfo* buffer, void* recorddata);

void BtreeXlogNewrootOperatorPage(RedoBufferInfo* buffer, void* record, void* blkdata, Size len, BlockNumber* downlink);
void BtreeXlogHalfdeadPageOperatorParentpage(
    RedoBufferInfo* pbuf, void* recorddata);
void BtreeXlogHalfdeadPageOperatorLeafpage(
    RedoBufferInfo* lbuf, void* recorddata);
void BtreeXlogUnlinkPageOperatorRightpage(RedoBufferInfo* rbuf, void* recorddata);
void BtreeXlogUnlinkPageOperatorLeftpage(RedoBufferInfo* lbuf, void* recorddata);
void BtreeXlogUnlinkPageOperatorCurpage(RedoBufferInfo* buf, void* recorddata);
void BtreeXlogUnlinkPageOperatorChildpage(RedoBufferInfo* cbuf, void* recorddata);

void BtreeXlogClearIncompleteSplit(RedoBufferInfo* buffer);

void XLogRecSetBlockCommonState(XLogReaderState* record, XLogBlockParseEnum blockvalid, ForkNumber forknum,
    BlockNumber blockknum, RelFileNode* relnode, XLogRecParseState* recordblockstate);

void XLogRecSetBlockCLogState(
    XLogBlockCLogParse* blockclogstate, TransactionId topxid, uint16 status, uint16 xidnum, uint16* xidsarry);

void XLogRecSetBlockCSNLogState(
    XLogBlockCSNLogParse* blockcsnlogstate, TransactionId topxid, CommitSeqNo csnseq, uint16 xidnum, uint16* xidsarry);
void XLogRecSetXactRecoveryState(XLogBlockXactParse* blockxactstate, TransactionId maxxid, CommitSeqNo maxcsnseq,
    uint8 delayddlflag, uint8 updateminrecovery);
void XLogRecSetXactDdlState(XLogBlockXactParse* blockxactstate, int nrels, void* xnodes, int invalidmsgnum,
    void* invalidmsg, int nlibs, void* libfilename);
void XLogRecSetXactCommonState(
    XLogBlockXactParse* blockxactstate, uint16 committype, uint64 xinfo, TimestampTz xact_time);
void XLogRecSetBcmState(XLogBlockBcmParse* blockbcmrec, uint64 startblock, int count, int status);
void XLogRecSetNewCuState(XLogBlockNewCuParse* blockcudata, char* main_data, uint32 main_data_len);
void XLogRecSetInvalidMsgState(XLogBlockInvalidParse* blockinvalid, TransactionId cutoffxid);
void XLogRecSetIncompleteMsgState(XLogBlockIncompleteParse* blockincomplete, uint16 action, bool issplit, bool isroot,
    BlockNumber downblk, BlockNumber leftblk, BlockNumber rightblk);
void XLogRecSetPinVacuumState(XLogBlockVacuumPinParse* blockvacuum, BlockNumber lastblknum);

void XLogRecSetAuxiBlkNumState(XLogBlockDataParse* blockdatarec, BlockNumber auxilaryblkn1, BlockNumber auxilaryblkn2);
void XLogRecSetBlockDataState(XLogReaderState* record, uint32 blockid, XLogRecParseState* recordblockstate);
extern char* XLogBlockDataGetBlockData(XLogBlockDataParse* datadecode, Size* len);
void Heap2RedoDataBlock(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo);
extern void HeapRedoDataBlock(
    XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo);
extern void xlog_redo_data_block(
    XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo);
extern void XLogRecSetBlockDdlState(XLogBlockDdlParse* blockddlstate, uint32 blockddltype, uint32 columnrel, 
    char *mainData, Oid ownerid = InvalidOid);
XLogRedoAction XLogCheckBlockDataRedoAction(XLogBlockDataParse* datadecode, RedoBufferInfo* bufferinfo);
void BtreeRedoDataBlock(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo);
XLogRecParseState* XactXlogCsnlogParseToBlock(XLogReaderState* record, uint32* blocknum, TransactionId xid,
    int nsubxids, TransactionId* subxids, CommitSeqNo csn, XLogRecParseState* recordstatehead);
extern void XLogRecSetVmBlockState(XLogReaderState* record, uint32 blockid, XLogRecParseState* recordblockstate);
extern void DoLsnCheck(RedoBufferInfo* bufferinfo, bool willInit, XLogRecPtr lastLsn);
char* XLogBlockDataGetMainData(XLogBlockDataParse* datadecode, Size* len);
void HeapRedoVmBlock(XLogBlockHead* blockhead, XLogBlockVmParse* blockvmrec, RedoBufferInfo* bufferinfo);
void Heap2RedoVmBlock(XLogBlockHead* blockhead, XLogBlockVmParse* blockvmrec, RedoBufferInfo* bufferinfo);
XLogRecParseState* xlog_redo_parse_to_block(XLogReaderState* record, uint32* blocknum);
XLogRecParseState* smgr_redo_parse_to_block(XLogReaderState* record, uint32* blocknum);
XLogRecParseState* XactXlogClogParseToBlock(XLogReaderState* record, XLogRecParseState* recordstatehead,
    uint32* blocknum, TransactionId xid, int nsubxids, TransactionId* subxids, CLogXidStatus status);
XLogRecParseState* xact_xlog_commit_parse_to_block(XLogReaderState* record, XLogRecParseState* recordstatehead,
    uint32* blocknum, TransactionId maxxid, CommitSeqNo maxseqnum);
void visibilitymap_clear_buffer(RedoBufferInfo* bufferinfo, BlockNumber heapBlk);
XLogRecParseState* xact_xlog_abort_parse_to_block(XLogReaderState* record, XLogRecParseState* recordstatehead,
    uint32* blocknum, TransactionId maxxid, CommitSeqNo maxseqnum);
XLogRecParseState* xact_xlog_prepare_parse_to_block(
    XLogReaderState* record, XLogRecParseState* recordstatehead, uint32* blocknum, TransactionId maxxid);
XLogRecParseState* xact_xlog_parse_to_block(XLogReaderState* record, uint32* blocknum);
XLogRecParseState* ClogRedoParseToBlock(XLogReaderState* record, uint32* blocknum);

XLogRecParseState* DbaseRedoParseToBlock(XLogReaderState* record, uint32* blocknum);

XLogRecParseState* Heap2RedoParseIoBlock(XLogReaderState* record, uint32* blocknum);

extern XLogRecParseState* HeapRedoParseToBlock(XLogReaderState* record, uint32* blocknum);
extern XLogRecParseState* BtreeRedoParseToBlock(XLogReaderState* record, uint32* blocknum);
extern XLogRecParseState* Heap3RedoParseToBlock(XLogReaderState* record, uint32* blocknum);

extern Size SalEncodeXLogBlock(void* recordblockstate, byte* buffer, void* sliceinfo);

extern XLogRecParseState* XLogParseToBlockForDfv(XLogReaderState* record, uint32* blocknum);
extern Size getBlockSize(XLogRecParseState* recordblockstate);
extern XLogRecParseState* GistRedoParseToBlock(XLogReaderState* record, uint32* blocknum);
extern XLogRecParseState* GinRedoParseToBlock(XLogReaderState* record, uint32* blocknum);

extern void GistRedoClearFollowRightOperatorPage(RedoBufferInfo* buffer);
extern void GistRedoPageUpdateOperatorPage(RedoBufferInfo* buffer, void* recorddata, void* blkdata, Size datalen);
extern void GistRedoPageSplitOperatorPage(
    RedoBufferInfo* buffer, void* recorddata, void* data, Size datalen, bool Markflag, BlockNumber rightlink);
extern void GistRedoCreateIndexOperatorPage(RedoBufferInfo* buffer);

extern void GinRedoCreateIndexOperatorMetaPage(RedoBufferInfo* MetaBuffer);
extern void GinRedoCreateIndexOperatorRootPage(RedoBufferInfo* RootBuffer);
extern void GinRedoCreatePTreeOperatorPage(RedoBufferInfo* buffer, void* recordData);
extern void GinRedoClearIncompleteSplitOperatorPage(RedoBufferInfo* buffer);
extern void GinRedoVacuumDataOperatorLeafPage(RedoBufferInfo* buffer, void* recorddata);
extern void GinRedoDeletePageOperatorCurPage(RedoBufferInfo* dbuffer);
extern void GinRedoDeletePageOperatorParentPage(RedoBufferInfo* pbuffer, void* recorddata);
extern void GinRedoDeletePageOperatorLeftPage(RedoBufferInfo* lbuffer, void* recorddata);
extern void GinRedoUpdateOperatorMetapage(RedoBufferInfo* metabuffer, void* recorddata);
extern void GinRedoUpdateOperatorTailPage(RedoBufferInfo* buffer, void* payload, Size totaltupsize, int32 ntuples);
extern void GinRedoInsertListPageOperatorPage(
    RedoBufferInfo* buffer, void* recorddata, void* payload, Size totaltupsize);
extern void GinRedoUpdateAddNewTail(RedoBufferInfo* buffer, BlockNumber newRightlink);
extern void GinRedoInsertData(RedoBufferInfo* buffer, bool isLeaf, BlockNumber rightblkno, void* rdata);
extern void GinRedoInsertEntry(RedoBufferInfo* buffer, bool isLeaf, BlockNumber rightblkno, void* rdata);

extern void GinRedoDeleteListPagesOperatorPage(RedoBufferInfo* metabuffer, const void* recorddata);
extern void GinRedoDeleteListPagesMarkDelete(RedoBufferInfo* buffer);

extern void spgRedoCreateIndexOperatorMetaPage(RedoBufferInfo* buffer);
extern void spgRedoCreateIndexOperatorRootPage(RedoBufferInfo* buffer);
extern void spgRedoCreateIndexOperatorLeafPage(RedoBufferInfo* buffer);
extern void spgRedoAddLeafOperatorPage(RedoBufferInfo* bufferinfo, void* recorddata);
extern void spgRedoAddLeafOperatorParent(RedoBufferInfo* bufferinfo, void* recorddata, BlockNumber blknoLeaf);
extern void spgRedoMoveLeafsOpratorDstPage(RedoBufferInfo* buffer, void* recorddata, void* insertdata, void* tupledata);
extern void spgRedoMoveLeafsOpratorSrcPage(
    RedoBufferInfo* buffer, void* recorddata, void* insertdata, void* deletedata, BlockNumber blknoDst, int nInsert);
extern void spgRedoMoveLeafsOpratorParentPage(
    RedoBufferInfo* buffer, void* recorddata, void* insertdata, BlockNumber blknoDst, int nInsert);
extern void spgRedoAddNodeUpdateSrcPage(RedoBufferInfo* buffer, void* recorddata, void* tuple, void* tupleheader);
extern void spgRedoAddNodeOperatorSrcPage(RedoBufferInfo* buffer, void* recorddata, BlockNumber blknoNew);
extern void spgRedoAddNodeOperatorDestPage(
    RedoBufferInfo* buffer, void* recorddata, void* tuple, void* tupleheader, BlockNumber blknoNew);
extern void spgRedoAddNodeOperatorParentPage(RedoBufferInfo* buffer, void* recorddata, BlockNumber blknoNew);
extern void spgRedoSplitTupleOperatorDestPage(RedoBufferInfo* buffer, void* recorddata, void* tuple);
extern void spgRedoSplitTupleOperatorSrcPage(RedoBufferInfo* buffer, void* recorddata, void* pretuple, void* posttuple);
extern void spgRedoPickSplitRestoreLeafTuples(
    RedoBufferInfo* buffer, void* recorddata, bool destflag, void* pageselect, void* insertdata);
extern void spgRedoPickSplitOperatorSrcPage(RedoBufferInfo* srcBuffer, void* recorddata, void* deleteoffset,
    BlockNumber blknoInner, void* pageselect, void* insertdata);
extern void spgRedoPickSplitOperatorDestPage(
    RedoBufferInfo* destBuffer, void* recorddata, void* pageselect, void* insertdata);
extern void spgRedoPickSplitOperatorInnerPage(
    RedoBufferInfo* innerBuffer, void* recorddata, void* tuple, void* tupleheader, BlockNumber blknoInner);
extern void spgRedoPickSplitOperatorParentPage(RedoBufferInfo* parentBuffer, void* recorddata, BlockNumber blknoInner);
extern void spgRedoVacuumLeafOperatorPage(RedoBufferInfo* buffer, void* recorddata);
extern void spgRedoVacuumRootOperatorPage(RedoBufferInfo* buffer, void* recorddata);
extern void spgRedoVacuumRedirectOperatorPage(RedoBufferInfo* buffer, void* recorddata);

extern XLogRecParseState* SpgRedoParseToBlock(XLogReaderState* record, uint32* blocknum);

extern void seqRedoOperatorPage(RedoBufferInfo* buffer, void* itmedata, Size itemsz);
extern void seq_redo_data_block(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo);

extern void Heap3RedoDataBlock(
    XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo);

extern XLogRecParseState* xact_redo_parse_to_block(XLogReaderState* record, uint32* blocknum);

extern bool XLogBlockRedoForExtremeRTO(XLogRecParseState* redoblocktate, RedoBufferInfo *bufferinfo, 
                                                      bool notfound);
void XLogBlockParseStateRelease_debug(XLogRecParseState* recordstate, const char *func, uint32 line);
#define XLogBlockParseStateRelease(recordstate)  XLogBlockParseStateRelease_debug(recordstate, __FUNCTION__, __LINE__)
#ifdef USE_ASSERT_CHECKING
extern void DoRecordCheck(XLogRecParseState *recordstate, XLogRecPtr pageLsn, bool replayed);
#endif
extern XLogRecParseState* XLogParseBufferCopy(XLogRecParseState *srcState);
extern XLogRecParseState* XLogParseToBlockForExtermeRTO(XLogReaderState* record, uint32* blocknum);
extern XLogRedoAction XLogReadBufferForRedoBlockExtend(RedoBufferTag* redoblock, ReadBufferMode mode, bool get_cleanup_lock,
    RedoBufferInfo* redobufferinfo, XLogRecPtr xloglsn, ReadBufferMethod readmethod);
extern XLogRecParseState* tblspc_redo_parse_to_block(XLogReaderState* record, uint32* blocknum);
extern XLogRecParseState* relmap_redo_parse_to_block(XLogReaderState* record, uint32* blocknum);
extern XLogRecParseState* HashRedoParseToBlock(XLogReaderState* record, uint32* blocknum);
extern XLogRecParseState* seq_redo_parse_to_block(XLogReaderState* record, uint32* blocknum);
extern XLogRecParseState* slot_redo_parse_to_block(XLogReaderState* record, uint32* blocknum);
extern XLogRecParseState* barrier_redo_parse_to_block(XLogReaderState* record, uint32* blocknum);
extern XLogRecParseState* multixact_redo_parse_to_block(XLogReaderState* record, uint32* blocknum);
extern void ExtremeRtoFlushBuffer(RedoBufferInfo *bufferinfo, bool updateFsm);
extern void XLogForgetDDLRedo(XLogRecParseState* redoblockstate);
extern void SyncOneBufferForExtremRto(RedoBufferInfo *bufferinfo);
extern void XLogBlockInitRedoBlockInfo(XLogBlockHead* blockhead, RedoBufferTag* blockinfo);
extern void XLogBlockDdlDoSmgrAction(XLogBlockHead* blockhead, void* blockrecbody, RedoBufferInfo* bufferinfo);
extern void GinRedoDataBlock(XLogBlockHead* blockhead, XLogBlockDataParse* blockdatarec, RedoBufferInfo* bufferinfo);
extern void GistRedoDataBlock(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo);
extern bool IsCheckPoint(const XLogRecParseState *parseState);

#endif
