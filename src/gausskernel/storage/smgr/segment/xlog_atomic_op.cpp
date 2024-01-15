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
 * xlog_atomic_op.cpp
 *
 * IDENTIFICATION
 *     src/gausskernel/storage/smgr/segment/xlog_atomic_op.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "catalog/storage_xlog.h"
#include "miscadmin.h"

#include "storage/smgr/segment.h"
#include "utils/palloc.h"
#include "replication/ss_disaster_cluster.h"

/*
 * In segment-page storage, an atomic operation may cross multiple functions, touching multiple pages. For example,
 * seg_extend needs allocating an extent first, which modifies MapHead page and BitMap page, then initializing segment
 * head page, i.e., three pages in total. Current XLog inferface is not friendly to this cross-function scenario:
 *      1. Buffer should be locked until XLogInsert finish. Thus, only the upper function can release all buffers, and
 * inner functions must return buffers they modified to their callers.
 *      2. XLogRegisterData just pass the data pointer to xlog context, and these data are assembled until XLogInsert.
 * If inner functions pass variables on their stacks, and upper functions call the XLogInsert later, XLogInsert will
 * try to read data on the released stack.
 *      3. When register buffer and block using XLogRegisterBuffer, must assign a block id indicating which one this
 * block is in current XLog. Some basic functions may be invoked in different places, thus blocks they modifying may
 * have different block id in different cases.
 *
 * Thus, we add a xlog atomic interface based on current XLog APIs. The main idea is:
 *  1. Add a global stack. Each registered buffer is pushed into the stack, and unlocked/marked as dirty/set LSN when
 * the XLog is committed.
 *  2. Block id is determined at XLogInsert, according to the order they are pushed into the stack.
 *  3. Each registered data is copied to a global cache immediately.
 */

/*
 * BufferStack: record locked buffers in this xlog
 */
static const int BufStackInitSize = 20;
static const int BufMaxOperations = 16;

struct XLogOperation {
    char *data;
    uint32 data_len;
};

struct XLogBuffer {
    Buffer buffer;
    uint8 flags;
    uint8 opnum;
    XLogOperation operations[BufMaxOperations];
    uint32 clean_flag; // what to do when xlog commit
    uint32 rdata_len;
};

struct BufferStack {
    BufferStack() :mcnxt(NULL), buffers(NULL), depth(0), capacity(0) { }
    MemoryContext mcnxt;
    XLogBuffer *buffers;
    int depth;
    int capacity;

    void Init(MemoryContext mc);
    int Insert(Buffer buf, uint8 flags, uint32 clean_flag);
    void Reset();
    int contains(Buffer buf);
    void RegisterOpData(int block_id, char *opData, int len, bool is_opcode);
};

void BufferStack::Init(MemoryContext mc)
{
    mcnxt = mc;
    depth = 0;
    capacity = BufStackInitSize;

    MemoryContext oldcnxt = MemoryContextSwitchTo(mcnxt);
    buffers = (XLogBuffer *)palloc(sizeof(XLogBuffer) * capacity);
    MemoryContextSwitchTo(oldcnxt);
}

int BufferStack::Insert(Buffer buf, uint8 flags, uint32 clean_flag)
{
    if (depth >= capacity) {
        /*
         * We do not use repalloc to dynamically extend BufferStack, because
         * XLog operations are sometimes in critical section, and thus repalloc
         * is forbidden.
         */
        ereport(PANIC, (errmsg("XLogAtomicOp buffer stack execeeds its capacity")));
    }
    buffers[depth].buffer = buf;
    buffers[depth].rdata_len = 0;
    buffers[depth].flags = flags;
    buffers[depth].clean_flag = clean_flag;
    buffers[depth].opnum = 0;
    depth++;

    return depth - 1;
}

void BufferStack::Reset()
{
    depth = 0;
}

/*
 * return -1 if not contains, otherwise return index in the array
 */
int BufferStack::contains(Buffer buf)
{
    for (int i = 0; i < depth; i++) {
        if (buffers[i].buffer == buf) {
            return i;
        }
    }

    return -1;
}

void BufferStack::RegisterOpData(int block_id, char *opData, int len, bool is_opcode)
{
    XLogOperation *xlog_op = &buffers[block_id].operations[buffers[block_id].opnum - 1];
    if (is_opcode) {
        xlog_op->data = opData;
        xlog_op->data_len = len;
    } else {
        SegmentCheck(xlog_op->data + xlog_op->data_len == opData);
        xlog_op->data_len += len;
    }
}

/*
 * XLogDataCache: copy data into heap when invoking XLogRegisterDataXXX
 */
static const int DataBufferInitSize = 1024 * 1024; // 1MB

struct XLogDataCache {
    XLogDataCache(): mcnxt(NULL), cache(NULL), used(0), capacity(0) { }
    MemoryContext mcnxt;
    char *cache;
    int used;
    int capacity;

    void Init(MemoryContext mc);
    char *RegisterData(char *data, int len);
    void Reset();
};

void XLogDataCache::Init(MemoryContext mc)
{
    mcnxt = mc;
    capacity = DataBufferInitSize;
    used = 0;

    MemoryContext oldcnxt = MemoryContextSwitchTo(mcnxt);
    cache = (char *)palloc(capacity);
    MemoryContextSwitchTo(oldcnxt);
}

char *XLogDataCache::RegisterData(char *data, int len)
{
    int expectLen = len + used;
    /*
     * We do not use repalloc to dynamically extend XLogDataCache, because
     * XLog operations are sometimes in critical section, and thus repalloc
     * is forbidden.
     */
    if (expectLen > capacity) {
        ereport(PANIC, (errmsg("XLogDataCache is used up")));
    }

    char *res = cache + used;
    errno_t er = memcpy_s(res, capacity - used, data, len);
    securec_check(er, "\0", "\0");
    used += len;

    return res;
}

void XLogDataCache::Reset()
{
    /* buffer memory can be used again */
    used = 0;
}

/*
 * XLogAtomicOperation: atomic operation controller
 */
struct XLogAtomicOperation {
    BufferStack stack;
    XLogDataCache dataCache;

    int curr_blockid;
    bool critical_section;

    void Init();
    void Reset();
    void XLogStart();
    void RegisterBuffer(Buffer buffer, uint8 flags, uint8 op, uint32 clean_flag);
    void RegisterBufData(char *data, int len, bool is_opcode);
    void XLogCommit(RmgrId rmid, uint8 info, int bucket_id);
    int CurrentBlockId();
};

void XLogAtomicOperation::Init()
{
    MemoryContext mcnxt = THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);
    stack.Init(mcnxt);
    dataCache.Init(mcnxt);
    critical_section = false;
}

void XLogAtomicOperation::Reset()
{
    stack.Reset();
    dataCache.Reset();
}

int XLogAtomicOperation::CurrentBlockId()
{
    // Must register buffer first.
    SegmentCheck(curr_blockid >= 0);
    return curr_blockid;
}

void XLogAtomicOperation::XLogStart()
{
    SegmentCheck(stack.depth == 0);
    if (t_thrd.int_cxt.CritSectionCount == 0) {
        /*
         * We can not invoke XLogEnsureRecordSpace in critical section. Thus, if XLogAtomicOperation
         * is used in critical section, it should extend record space manually before entring the
         * critical section. Currently, the only case is FinishPreparedTransaction deleting files (segment)
         * in twophase.cpp.
         */
        XLogEnsureRecordSpace(30, 80);
    }
}

void XLogAtomicOperation::RegisterBuffer(Buffer buffer, uint8 flags, uint8 op, uint32 clean_flag)
{
    /*
     * From now, we enter the critical section, because we may modify buffer data after XLogAtomicOpRegisterBuffer.
     * If error happens, xlog is lost and we do not allow any dirty buffer.
     */
    if (!critical_section) {
        START_CRIT_SECTION();
        critical_section = true;
    }
    ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE),
                     errmsg("[XLogAtomicOperation] register buffer %d flags %d op %d", buffer, flags, op)));

    int buf_id = stack.contains(buffer);
    if (buf_id >= 0) {
        curr_blockid = buf_id;
    } else {
        curr_blockid = stack.Insert(buffer, flags, clean_flag);
    }
    stack.buffers[curr_blockid].opnum++;
    if (stack.buffers[curr_blockid].opnum > BufMaxOperations) {
        ereport(PANIC, (errmsg("[XLogAtomicOperation] each buffer supports at most %d operations", BufMaxOperations)));
    }

    /* register op code here */
    RegisterBufData((char *)&op, sizeof(op), true);
}

/* Register data for last registered buffer */
void XLogAtomicOperation::RegisterBufData(char *data, int len, bool is_opcode)
{
    ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE), errmsg("[XLogAtomicOperation] register data, len %d", len)));
    int block_id = CurrentBlockId();
    char *opData = dataCache.RegisterData(data, len);
    stack.RegisterOpData(block_id, opData, len, is_opcode);
}

void XLogAtomicOperation::XLogCommit(RmgrId rmid, uint8 info, int bucket_id)
{
    /*
     * Do actual xlog begin/register/insert.
     */

    // 1. Begin insert
    XLogBeginInsert();

    // 2. Register buffer number as xlog data.
    XLogRegisterData((char *)&stack.depth, sizeof(stack.depth));

    // 3. Register each buffer and data.
    for (int i = 0; i < stack.depth; i++) {
        XLogBuffer *buf = &stack.buffers[i];
        SegMarkBufferDirty(buf->buffer);
        XLogRegisterBuffer(i, buf->buffer, buf->flags);

        SegmentCheck(buf->opnum >= 1);

        ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE),
                         errmsg("[XLogAtomicOperation] XLogRegisterBuffer, operations %d", buf->opnum)));

        XLogRegisterBufData(i, (char *)&buf->opnum, sizeof(uint8));
        for (int j = 0; j < buf->opnum; j++) {
            XLogOperation *xlog_op = &buf->operations[j];
            XLogRegisterBufData(i, (char *)&xlog_op->data_len, sizeof(uint32));
            XLogRegisterBufData(i, xlog_op->data, xlog_op->data_len);

            ereport(DEBUG5, (errmodule(MOD_SEGMENT_PAGE),
                             errmsg("[XLogAtomicOperation] XLogRegisterOperation, operation %d, data len %u", i,
                                    xlog_op->data_len)));
        }
    }

    // 4. xlog insert
    XLogRecPtr recptr = XLogInsert(rmid, info, bucket_id);

    // 5. mark dirty, set lsn, and release lock
    for (int i = 0; i < stack.depth; i++) {
        Buffer buf = stack.buffers[i].buffer;

        // Current, only used for segment metadata buffer.
        SegmentCheck(IsSegmentBufferID(buf - 1));

        PageSetLSN(BufferGetPage(buf), recptr);

        uint32 clean_flag = stack.buffers[i].clean_flag;
        if (BUF_NEED_UNLOCK_RELEASE(clean_flag)) {
            SegUnlockReleaseBuffer(buf);
        } else {
            if (BUF_NEED_RELEASE(clean_flag)) {
                SegReleaseBuffer(buf);
            }
            if (BUF_NEED_UNLOCK(clean_flag)) {
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);
            }
        }
    }

    if (critical_section) {
        END_CRIT_SECTION();
        critical_section = false;
    }
    Reset();
}

#define XLogAtomicOpMgr ((XLogAtomicOperation *)t_thrd.xlog_cxt.xlog_atomic_op)

void XLogAtomicOpInit()
{
    if (SECUREC_UNLIKELY(t_thrd.xlog_cxt.xlog_atomic_op != NULL)) {
        return;
    }

    MemoryContext oldcnxt = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    PG_TRY();
    {
        t_thrd.xlog_cxt.xlog_atomic_op = (XLogAtomicOperation *)palloc0(sizeof(XLogAtomicOperation));
        XLogAtomicOpMgr->Init();
    }
    PG_CATCH();
    {
        if (XLogAtomicOpMgr != NULL) {
            pfree_ext(XLogAtomicOpMgr->stack.buffers);
            pfree_ext(XLogAtomicOpMgr->dataCache.cache);
        }
        pfree_ext(t_thrd.xlog_cxt.xlog_atomic_op);
        PG_RE_THROW();
    }
    PG_END_TRY();
    MemoryContextSwitchTo(oldcnxt);
}

void XLogAtomicOpStart()
{
    if (XLogAtomicOpMgr == NULL) {
        XLogAtomicOpInit();
    }
    if (!XLogInsertAllowed()) {
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("recovery is in progress"),
                        errhint("cannot make new WAL entries during recovery")));
    }

    if (SS_DORADO_STANDBY_CLUSTER) {
        ereport(FATAL, (errmsg("SS dorado standby cluster cannot make new WAL entries")));
    }

    XLogAtomicOpMgr->XLogStart();
}

void XLogAtomicOpRegisterBufData(char *data, int len)
{
    SegmentCheck(t_thrd.xlog_cxt.xlog_atomic_op != NULL);
    XLogAtomicOpMgr->RegisterBufData(data, len, false);
}

void XLogAtomicOpRegisterBuffer(Buffer buffer, uint8 flags, uint8 op, uint32 clean_flag)
{
    SegmentCheck(t_thrd.xlog_cxt.xlog_atomic_op != NULL);
    XLogAtomicOpMgr->RegisterBuffer(buffer, flags, op, clean_flag);
}

void XLogAtomicOpCommit()
{
    SegmentCheck(t_thrd.xlog_cxt.xlog_atomic_op != NULL);
    XLogAtomicOpMgr->XLogCommit(RM_SEGPAGE_ID, XLOG_SEG_ATOMIC_OPERATION, SegmentBktId);
}

void XLogAtomicOpReset()
{
    if (t_thrd.xlog_cxt.xlog_atomic_op != NULL) {
        XLogAtomicOpMgr->Reset();
    }
}
