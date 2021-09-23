/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * sequence.cpp
 *    parse sequence xlog
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/sequence.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/gtm.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "gtm/gtm_client.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/smgr/smgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "commands/dbcommands.h"

#ifdef PGXC
#include "pgxc/groupmgr.h"
#include "pgxc/pgxc.h"
/* PGXC_COORD */
#include "access/gtm.h"
#include "utils/memutils.h"
#endif
#include "gs_register/gs_malloc.h"

typedef enum {
    SEQ_XLOG_ORIG_BLOCK_NUM = 0
} XLogSeqBlockEnum;

void seqRedoOperatorPage(RedoBufferInfo *buffer, void *itmedata, Size itemsz)
{
    Page page = buffer->pageinfo.page;
    Page localpage;
    sequence_magic *sm = NULL;
    errno_t rc = EOK;
    HeapPageHeader phdr;
    char *item = (char *)itmedata;

    /*
     * We must always reinit the page and reinstall the magic number (see
     * comments in fill_seq_with_data).  However, since this WAL record type
     * is also used for updating sequences, it's possible that a hot-standby
     * backend is examining the page concurrently; so we mustn't transiently
     * trash the buffer.  The solution is to build the correct new page
     * contents in local workspace and then memcpy into the buffer.  Then only
     * bytes that are supposed to change will change, even transiently. We
     * must palloc the local page for alignment reasons.
     */
    localpage = (Page)palloc(buffer->pageinfo.pagesize);

    PageInit(localpage, buffer->pageinfo.pagesize, sizeof(sequence_magic), true);
    sm = (sequence_magic *)PageGetSpecialPointer(localpage);
    sm->magic = SEQ_MAGIC;

    phdr = (HeapPageHeader)localpage;
    phdr->pd_xid_base = u_sess->utils_cxt.RecentXmin - FirstNormalTransactionId;
    phdr->pd_multi_base = 0;

    if (PageAddItem(localpage, (Item)item, itemsz, FirstOffsetNumber, false, false) == InvalidOffsetNumber)
        elog(PANIC, "seq_redo: failed to add item to page");

    PageSetLSN(localpage, buffer->lsn);

    rc = memcpy_s(page, BufferGetPageSize(buffer->buf), localpage, buffer->pageinfo.pagesize);
    securec_check(rc, "\0", "\0");
    pfree_ext(localpage);
}

XLogRecParseState *seq_xlog_parse_block(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 1;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    XLogRecSetBlockDataState(record, SEQ_XLOG_ORIG_BLOCK_NUM, recordstatehead);
    return recordstatehead;
}

XLogRecParseState *seq_redo_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    XLogRecParseState *recordstatehead = NULL;
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    *blocknum = 0;
    if (info == XLOG_SEQ_LOG) {
        recordstatehead = seq_xlog_parse_block(record, blocknum);
    } else {
        elog(PANIC, "seq_redo_parse_to_block: unknown op code %u", info);
    }

    return recordstatehead;
}

void seq_redo_data_block(XLogBlockHead *blockhead, XLogBlockDataParse *blockdatarec, RedoBufferInfo *bufferinfo)
{
    Size mainDataLen;
    xl_seq_rec *xlrec = (xl_seq_rec *)XLogBlockDataGetMainData(blockdatarec, &mainDataLen);
    char *item = (char *)xlrec + sizeof(xl_seq_rec);
    Size itemsz = mainDataLen - sizeof(xl_seq_rec);

    seqRedoOperatorPage(bufferinfo, item, itemsz);
    MakeRedoBufferDirty(bufferinfo);
}
