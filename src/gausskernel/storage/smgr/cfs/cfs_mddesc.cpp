/* -------------------------------------------------------------------------
 *
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * cfs_mddesc.cpp
 *	  shrink redo descriptor routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/smgr/cfs/cfs_mddesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/catalog.h"
#include "catalog/storage_xlog.h"
#include "storage/custorage.h"
#include "storage/smgr/relfilenode.h"

void CfsShrinkDesc(StringInfo buf, XLogReaderState *record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    if (info == XLOG_CFS_SHRINK_OPERATION) {
        CfsShrink_t *data = (CfsShrink_t *)(void *)XLogRecGetData(record);
        appendStringInfo(buf, "[CfsShrink_desc] cfs arrange the relation:[%u, %u, %u, %d], forknum:[%d]",
                         data->node.spcNode, data->node.dbNode, data->node.relNode, data->node.bucketNode,
                         data->forknum);
    } else {
        appendStringInfo(buf, "UNKNOWN");
    }
    
    return ;
}
