/* -------------------------------------------------------------------------
 *
 * spgdesc.cpp
 *	  rmgr descriptor routines for access/spgist/spgxlog.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/rmgrdesc/spgdesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/spgist_private.h"
#include "utils/rel_gs.h"

const char* spg_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    switch (info) {
        case XLOG_SPGIST_CREATE_INDEX:
            return "spgist_create_index";
            break;
        case XLOG_SPGIST_ADD_LEAF:
            return "spgist_add_leaf";
            break;
        case XLOG_SPGIST_MOVE_LEAFS:
            return "spgist_move_leafs";
            break;
        case XLOG_SPGIST_ADD_NODE:
            return "spgist_add_node";
            break;
        case XLOG_SPGIST_SPLIT_TUPLE:
            return "spgist_split";
            break;
        case XLOG_SPGIST_PICKSPLIT:
            return "spgist_picksplit";
            break;
        case XLOG_SPGIST_VACUUM_LEAF:
            return "spgist_vacuum_leaf";
            break;
        case XLOG_SPGIST_VACUUM_ROOT:
            return "spgist_vacuum_root";
            break;
        case XLOG_SPGIST_VACUUM_REDIRECT:
            return "spgist_vacuum_redirect";
            break;
        default:
            break;
    }
    return "unknow_type";
}


void spg_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_SPGIST_CREATE_INDEX:
            appendStringInfo(buf, "create index");
            /* no further information */
            break;
        case XLOG_SPGIST_ADD_LEAF: {
            spgxlogAddLeaf *xlrec = (spgxlogAddLeaf *)rec;

            appendStringInfo(buf, "add leaf to page");
            appendStringInfo(buf, "; off %u; headoff %u; parentoff %u", (uint32)xlrec->offnumLeaf,
                             (uint32)xlrec->offnumHeadLeaf, (uint32)xlrec->offnumParent);
            if (xlrec->newPage)
                appendStringInfo(buf, " (newpage)");
            if (xlrec->storesNulls)
                appendStringInfo(buf, " (nulls)");
        } break;
        case XLOG_SPGIST_MOVE_LEAFS:
            appendStringInfo(buf, "%u leafs", (uint32)((spgxlogMoveLeafs *)rec)->nMoves);
            break;
        case XLOG_SPGIST_ADD_NODE:
            appendStringInfo(buf, "off %u", (uint32)((spgxlogAddNode *)rec)->offnum);
            break;
        case XLOG_SPGIST_SPLIT_TUPLE:
            appendStringInfo(buf, "prefix off: %u, postfix off: %u (same %d, new %d)",
                             (uint32)((spgxlogSplitTuple *)rec)->offnumPrefix,
                             (uint32)((spgxlogSplitTuple *)rec)->offnumPostfix,
                             ((spgxlogSplitTuple *)rec)->postfixBlkSame, ((spgxlogSplitTuple *)rec)->newPage);
            break;
        case XLOG_SPGIST_PICKSPLIT: {
            spgxlogPickSplit *xlrec = (spgxlogPickSplit *)rec;

            appendStringInfo(buf, "ndel %u; nins %u", (uint32)xlrec->nDelete, (uint32)xlrec->nInsert);
            if (xlrec->innerIsParent)
                appendStringInfo(buf, " (innerIsParent)");
            if (xlrec->isRootSplit)
                appendStringInfo(buf, " (isRootSplit)");
        } break;
        case XLOG_SPGIST_VACUUM_LEAF:
            appendStringInfo(buf, "vacuum leaf");
            /* no further information */
            break;
        case XLOG_SPGIST_VACUUM_ROOT:
            appendStringInfo(buf, "vacuum root");
            /* no further information */
            break;
        case XLOG_SPGIST_VACUUM_REDIRECT:
            appendStringInfo(buf, "newest XID " XID_FMT, ((spgxlogVacuumRedirect *)rec)->newestRedirectXid);
            break;
        default:
            appendStringInfo(buf, "unknown spgist op code %u", (uint32)info);
            break;
    }
}
