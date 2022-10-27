/* -------------------------------------------------------------------------
 *
 * smgrdesc.cpp
 *	  rmgr descriptor routines for catalog/storage.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/rmgrdesc/smgrdesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/catalog.h"
#include "catalog/storage_xlog.h"
#include "storage/custorage.h"
#include "storage/smgr/relfilenode.h"

const char* smgr_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    if (info == XLOG_SMGR_CREATE) {
        return "smgr_create";
    } else if (info == XLOG_SMGR_TRUNCATE) {
        return "truncate";
    } else {
        return "unkown_type";
    }
}

void smgr_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    bool compress = (bool)(XLogRecGetInfo(record) & XLR_REL_COMPRESS);
    if (info == XLOG_SMGR_CREATE) {
        xl_smgr_create *xlrec = (xl_smgr_create *)rec;
        RelFileNode rnode;
        RelFileNodeCopy(rnode, xlrec->rnode, XLogRecGetBucketId(record));
        rnode.opt = GetCreateXlogFileNodeOpt(record);
        char *path = relpathperm(rnode, xlrec->forkNum);

        appendStringInfo(buf, "file create: %s%s", path, compress ? COMPRESS_STR : "");
#ifdef FRONTEND
        free(path);
        path = NULL;
#else
        pfree_ext(path);
#endif
    } else if (info == XLOG_SMGR_TRUNCATE) {
        xl_smgr_truncate *xlrec = (xl_smgr_truncate *)rec;
        RelFileNode rnode;
        RelFileNodeCopy(rnode, xlrec->rnode, XLogRecGetBucketId(record));
        rnode.opt = GetTruncateXlogFileNodeOpt(record);
        char *path = relpathperm(rnode, MAIN_FORKNUM);

        appendStringInfo(buf, "file truncate: %s%s to %u blocks", path, compress ? COMPRESS_STR : "", xlrec->blkno);
#ifdef FRONTEND
        free(path);
        path = NULL;
#else
        pfree_ext(path);
#endif
    } else
        appendStringInfo(buf, "UNKNOWN");
}
