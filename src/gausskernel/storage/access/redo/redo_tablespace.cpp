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
 * tablespace.cpp
 *    parse table space xlog
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/tablespace.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogproc.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_tablespace.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "commands/seclabel.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"
#include "postmaster/bgwriter.h"
#include "storage/smgr/fd.h"
#include "storage/standby.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#ifdef PGXC
#include "pgxc/execRemote.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#endif
#include "gs_register/gs_malloc.h"
#include "replication/replicainternal.h"

void XLogRecSetTblSpcState(XLogBlockTblSpcParse *blocktblspcstate, char *tblPath, bool isRelativePath)
{
    blocktblspcstate->tblPath = tblPath;
    blocktblspcstate->isRelativePath = isRelativePath;
}

XLogRecParseState *tblspc_xlog_common_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordstatehead = NULL;
    XLogBlockParseEnum blockparsetype;
    RelFileNode relnode;
    RelFileNodeForkNum filenode;
    bool isRelativePath = false;
    char *tblPath = NULL;

    if (info == XLOG_TBLSPC_DROP) {
        xl_tblspc_drop_rec *xlrec = (xl_tblspc_drop_rec *)XLogRecGetData(record);
        relnode.spcNode = xlrec->ts_id;
        blockparsetype = BLOCK_DATA_DROP_TBLSPC_TYPE;
    } else {
        xl_tblspc_create_rec *xlrec = (xl_tblspc_create_rec *)XLogRecGetData(record);
        relnode.spcNode = xlrec->ts_id;
        tblPath = xlrec->ts_path;
        blockparsetype = BLOCK_DATA_CREATE_TBLSPC_TYPE;

        if (info == XLOG_TBLSPC_RELATIVE_CREATE) {
            isRelativePath = true;
        }
    }

    relnode.dbNode = InvalidOid;
    relnode.relNode = InvalidOid;
    relnode.bucketNode = InvalidBktId;
    relnode.opt = 0;
    (*blocknum)++;
    XLogParseBufferAllocListFunc(record, &recordstatehead, NULL);
    if (recordstatehead == NULL) {
        return NULL;
    }

    filenode = RelFileNodeForkNumFill(&relnode, InvalidBackendId, InvalidForkNumber, InvalidBlockNumber);
    XLogRecSetBlockCommonState(record, blockparsetype, filenode, recordstatehead);
    XLogRecSetTblSpcState(&recordstatehead->blockparse.extra_rec.blocktblspc, tblPath, isRelativePath);
    recordstatehead->isFullSync = record->isFullSync;
    return recordstatehead;
}

XLogRecParseState *tblspc_redo_parse_to_block(XLogReaderState *record, uint32 *blocknum)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    XLogRecParseState *recordstatehead = NULL;

    *blocknum = 0;
    if ((info == XLOG_TBLSPC_CREATE) || (info == XLOG_TBLSPC_DROP) || (info == XLOG_TBLSPC_RELATIVE_CREATE)) {
        recordstatehead = tblspc_xlog_common_parse_to_block(record, blocknum);
    } else {
        ereport(PANIC, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("tblspc_redo: unknown op code %u", info)));
    }

    return recordstatehead;
}
