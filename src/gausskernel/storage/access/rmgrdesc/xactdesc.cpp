/* -------------------------------------------------------------------------
 *
 * xactdesc.cpp
 *	  rmgr descriptor routines for access/transam/xact.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/rmgrdesc/xactdesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "access/transam.h"
#include "access/twophase.h"
#include "catalog/catalog.h"
#include "storage/sinval.h"
#include "utils/timestamp.h"
#include "securec.h"

/*
 * @Description: Desc library file.
 * @out buf: String data.
 * @in filename: Library file start ptr.
 * @in library_path: Library path include head info.
 */
static void desc_library(StringInfo buf, char *filename, int nlibrary)
{
    int len = 0;
    char *library_path = NULL;
    int nlib = nlibrary;
    char *ptr = filename;
    errno_t rc = 0;

    while (nlib > 0) {
        rc = memcpy_s(&len, sizeof(int), ptr, sizeof(int));
        securec_check_c(rc, "\0", "\0");
        ptr += sizeof(int);

        if (len + 1 <= 0) {
            return;
        }
        library_path = (char *)malloc((size_t)len + 1);
        if (library_path == NULL) {
            return;
        }
        rc = memcpy_s(library_path, (size_t)len + 1, ptr, (size_t)len);
        securec_check_c(rc, "\0", "\0");

        library_path[len] = '\0';
        ptr += len;

        appendStringInfo(buf, "; library: %s", library_path);
        free(library_path);
        library_path = NULL;
        nlib--;
    }
}

static void xact_desc_commit(StringInfo buf, xl_xact_commit *xlrec, RepOriginId origin_id, bool compress)
{
    int i;
    int nsubxacts = xlrec->nsubxacts;
    TransactionId *subxacts = NULL;

    subxacts = GET_SUB_XACTS(xlrec->xnodes, xlrec->nrels, compress);
    appendStringInfoString(buf, timestamptz_to_str(xlrec->xact_time));
    appendStringInfo(buf, "; csn:%lu", xlrec->csn);

    if (xlrec->nrels > 0) {
        appendStringInfo(buf, "; rels:");
        for (i = 0; i < xlrec->nrels; i++) {
            ColFileNode colFileNode;
            if (compress) {
                ColFileNode *colFileNodeRel = ((ColFileNode *)(void *)xlrec->xnodes) + i;
                ColFileNodeFullCopy(&colFileNode, colFileNodeRel);
            } else {
                ColFileNodeRel *colFileNodeRel = xlrec->xnodes + i;
                ColFileNodeCopy(&colFileNode, colFileNodeRel);
            }
            char *path = relpathperm(colFileNode.filenode, MAIN_FORKNUM);

            /*
             * because *relpathperm()* cannot handle column table now,
             * so we have to append Cxxxxx.0 to filename buffer.
             */
            if (IsValidColForkNum(colFileNode.forknum))
                appendStringInfo(buf, " %s_C%d", path, ColForkNum2ColumnId(colFileNode.forknum));
            else
                appendStringInfo(buf, " %s", path);
#ifdef FRONTEND
            free(path);
#else
            pfree(path);
#endif
            path = NULL;
        }
    }
    if (xlrec->nsubxacts > 0) {
        appendStringInfo(buf, "; subxacts:");
        for (i = 0; i < xlrec->nsubxacts; i++)
            appendStringInfo(buf, " " XID_FMT, subxacts[i]);
    }
    if (xlrec->nmsgs > 0) {
        SharedInvalidationMessage *msgs = NULL;

        msgs = (SharedInvalidationMessage *)&subxacts[xlrec->nsubxacts];

        if (XactCompletionRelcacheInitFileInval(xlrec->xinfo))
            appendStringInfo(buf, "; relcache init file inval dbid %u tsid %u", xlrec->dbId, xlrec->tsId);

        appendStringInfo(buf, "; inval msgs:");
        for (i = 0; i < xlrec->nmsgs; i++) {
            SharedInvalidationMessage *msg = &msgs[i];

            if (msg->id >= 0)
                appendStringInfo(buf, " catcache %d", msg->id);
            else if (msg->id == SHAREDINVALCATALOG_ID)
                appendStringInfo(buf, " catalog %u", msg->cat.catId);
            else if (msg->id == SHAREDINVALRELCACHE_ID)
                appendStringInfo(buf, " relcache %u", msg->rc.relId);
            /* remaining cases not expected, but print something anyway */
            else if (msg->id == SHAREDINVALSMGR_ID)
                appendStringInfo(buf, " smgr");
            else if (msg->id == SHAREDINVALRELMAP_ID)
                appendStringInfo(buf, " relmap");
            else
                appendStringInfo(buf, " unknown id %d", msg->id);
        }
    }

#ifndef ENABLE_MULTIPLE_NODES
    SharedInvalidationMessage* msgs = (SharedInvalidationMessage*)&subxacts[xlrec->nsubxacts];
    TransactionId* recentXmin = (TransactionId *)&(msgs[xlrec->nmsgs]);
    appendStringInfo(buf, "; RecentXmin:%lu", *recentXmin);
    nsubxacts++;
#endif

    if (xlrec->nlibrary > 0) {
        char *filename = NULL;

        filename = (char *)xlrec->xnodes + (xlrec->nrels * SIZE_OF_COLFILENODE(compress)) +
                   (nsubxacts * sizeof(TransactionId)) + (xlrec->nmsgs * sizeof(SharedInvalidationMessage));

        desc_library(buf, filename, xlrec->nlibrary);
    }

    if (xlrec->xinfo & XACT_HAS_ORIGIN) {
        xl_xact_origin *origin = (xl_xact_origin *)GetRepOriginPtr((char *)xlrec->xnodes, xlrec->xinfo,
            xlrec->nsubxacts, xlrec->nmsgs, xlrec->nrels, xlrec->nlibrary, compress);
        appendStringInfo(buf, "; origin: node %u, lsn %X/%X, at %s", origin_id,
            (uint32)(origin->origin_lsn >> BITS_PER_INT),
            (uint32)origin->origin_lsn, timestamptz_to_str(origin->origin_timestamp));
    }
}

static void xact_desc_commit_compact(StringInfo buf, xl_xact_commit_compact *xlrec)
{
    int i;

    appendStringInfoString(buf, timestamptz_to_str(xlrec->xact_time));
    appendStringInfo(buf, "; csn:%lu", xlrec->csn);

    if (xlrec->nsubxacts > 0) {
        appendStringInfo(buf, "; subxacts:");
        for (i = 0; i < xlrec->nsubxacts; i++)
            appendStringInfo(buf, " " XID_FMT, xlrec->subxacts[i]);
    }

#ifndef ENABLE_MULTIPLE_NODES
    appendStringInfo(buf, "; RecentXmin:%lu", xlrec->subxacts[xlrec->nsubxacts]);
#endif
}

static void xact_desc_abort(StringInfo buf, xl_xact_abort *xlrec, bool abortXlogNewVersion, bool compress)
{
    int i;

    appendStringInfoString(buf, timestamptz_to_str(xlrec->xact_time));
    if (xlrec->nrels > 0) {
        appendStringInfo(buf, "; rels:");
        for (i = 0; i < xlrec->nrels; i++) {
            ColFileNode colFileNode;
            if (compress) {
                ColFileNode *colFileNodeRel = ((ColFileNode *)(void *)xlrec->xnodes) + i;
                ColFileNodeFullCopy(&colFileNode, colFileNodeRel);
            } else {
                ColFileNodeRel *colFileNodeRel = xlrec->xnodes + i;
                ColFileNodeCopy(&colFileNode, colFileNodeRel);
            }
            char *path = relpathperm(colFileNode.filenode, MAIN_FORKNUM);

            /*
             * because *relpathperm()* cannot handle column table now,
             * so we append Cxxxxx.0 to filename buffer.
             */
            if (IsValidColForkNum(colFileNode.forknum))
                appendStringInfo(buf, " %s_C%d", path, ColForkNum2ColumnId(colFileNode.forknum));
            else
                appendStringInfo(buf, " %s", path);
#ifdef FRONTEND
            free(path);
#else
            pfree(path);
#endif
            path = NULL;
        }
    }
    if (xlrec->nsubxacts > 0) {
        TransactionId *xacts = GET_SUB_XACTS(xlrec->xnodes, xlrec->nrels, compress);

        appendStringInfo(buf, "; subxacts:");
        for (i = 0; i < xlrec->nsubxacts; i++)
            appendStringInfo(buf, " " XID_FMT, xacts[i]);
    }
    if (xlrec->nlibrary > 0) {
        char *filename = NULL;
        filename = (char *)xlrec->xnodes + (xlrec->nrels * SIZE_OF_COLFILENODE(compress)) +
                   (xlrec->nsubxacts * sizeof(TransactionId));
        if (abortXlogNewVersion) {
            appendStringInfo(buf, "; current xact: %lu", *(TransactionId*)(filename));
            filename += sizeof(TransactionId);
        }
        desc_library(buf, filename, xlrec->nlibrary);
    } else if (abortXlogNewVersion) {
        appendStringInfo(buf, "; current xact: %lu",
                         *(TransactionId *)(void *)((char *)xlrec->xnodes +
                                                    (xlrec->nrels * SIZE_OF_COLFILENODE(compress)) +
                                                    ((uint32)xlrec->nsubxacts * sizeof(TransactionId))));
    }
}

const char *xact_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    if (info == XLOG_XACT_COMMIT_COMPACT) {
        return "commit_compact";
    } else if (info == XLOG_XACT_COMMIT) {
        return "commit";
    } else if (info == XLOG_XACT_ABORT) {
        return "abort";
    } else if (info == XLOG_XACT_ABORT_WITH_XID) {
        return "abort_with_xid";
    } else if (info == XLOG_XACT_PREPARE) {
        return "prepare";
    } else if (info == XLOG_XACT_COMMIT_PREPARED) {
        return "commit_prepared";
    } else if (info == XLOG_XACT_ABORT_PREPARED) {
        return "abort_prepared";
    } else if (info == XLOG_XACT_ASSIGNMENT) {
        return "assignment";
    } else {
        return "unkown_type";
    }
}


void xact_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    bool compress = (bool)(XLogRecGetInfo(record) & XLR_REL_COMPRESS);
    if (info == XLOG_XACT_COMMIT_COMPACT) {
        xl_xact_commit_compact *xlrec = (xl_xact_commit_compact *)rec;

        appendStringInfo(buf, "XLOG_XACT_COMMIT_COMPACT commit: ");
        xact_desc_commit_compact(buf, xlrec);
    } else if (info == XLOG_XACT_COMMIT) {
        xl_xact_commit *xlrec = (xl_xact_commit *)rec;

        appendStringInfo(buf, "XLOG_XACT_COMMIT commit: ");
        xact_desc_commit(buf, xlrec, XLogRecGetOrigin(record), compress);
    } else if (info == XLOG_XACT_ABORT) {
        xl_xact_abort *xlrec = (xl_xact_abort *)rec;

        appendStringInfo(buf, "abort: ");
        xact_desc_abort(buf, xlrec, false, compress);
    } else if (info == XLOG_XACT_ABORT_WITH_XID) {
        xl_xact_abort *xlrec = (xl_xact_abort *)rec;
        appendStringInfo(buf, "abort_with_xid: ");
        xact_desc_abort(buf, xlrec, true, compress);
    } else if (info == XLOG_XACT_PREPARE) {
        TwoPhaseFileHeader *hdr = (TwoPhaseFileHeader *)rec;
        appendStringInfo(buf, "prepare transaction, gid: %s", hdr->gid);
    } else if (info == XLOG_XACT_COMMIT_PREPARED) {
        xl_xact_commit_prepared *xlrec = (xl_xact_commit_prepared *)rec;

        appendStringInfo(buf, "commit prepared " XID_FMT ": ", xlrec->xid);
        xact_desc_commit(buf, &xlrec->crec, XLogRecGetOrigin(record), compress);
    } else if (info == XLOG_XACT_ABORT_PREPARED) {
        xl_xact_abort_prepared *xlrec = (xl_xact_abort_prepared *)rec;

        appendStringInfo(buf, "abort prepared " XID_FMT ": ", xlrec->xid);
        xact_desc_abort(buf, &xlrec->arec, false, compress);
    } else if (info == XLOG_XACT_ASSIGNMENT) {
        xl_xact_assignment *xlrec = (xl_xact_assignment *)rec;

        /*
         * Note that we ignore the WAL record's xid, since we're more
         * interested in the top-level xid that issued the record and which
         * xids are being reported here.
         */
        appendStringInfo(buf, "xid assignment xtop " XID_FMT ": ", xlrec->xtop);
    } else
        appendStringInfo(buf, "UNKNOWN");
}
