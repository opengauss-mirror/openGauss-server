/* -------------------------------------------------------------------------
 *
 * gindesc.cpp
 *	  rmgr descriptor routines for access/transam/gin/ginxlog.cpp
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/rmgrdesc/gindesc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/gin_private.h"
#include "access/xlogutils.h"
#include "lib/stringinfo.h"
#ifdef FRONTEND
#include "common/fe_memutils.h"
#endif
#include "storage/smgr/relfilenode.h"

static void desc_recompress_leaf(StringInfo buf, ginxlogRecompressDataLeaf *insertData)
{
    int i;
    char *walbuf = ((char *)insertData) + sizeof(ginxlogRecompressDataLeaf);

    appendStringInfo(buf, " %d segments:", (int)insertData->nactions);

    for (i = 0; i < insertData->nactions; i++) {
        uint8 a_segno = *((uint8 *)(walbuf++));
        uint8 a_action = *((uint8 *)(walbuf++));
        uint16 nitems = 0;
        uint newsegsize = 0;

        if (a_action == GIN_SEGMENT_INSERT || a_action == GIN_SEGMENT_REPLACE) {
            newsegsize = SizeOfGinPostingList((GinPostingList *)walbuf);
            walbuf += SHORTALIGN(newsegsize);
        }

        if (a_action == GIN_SEGMENT_ADDITEMS) {
            errno_t rc = memcpy_s(&nitems, sizeof(uint16), walbuf, sizeof(uint16));
            securec_check(rc, "\0", "\0");
            walbuf += sizeof(uint16);
            walbuf += nitems * sizeof(ItemPointerData);
        }

        switch (a_action) {
            case GIN_SEGMENT_ADDITEMS:
                appendStringInfo(buf, " %d (add %d items)", a_segno, nitems);
                break;
            case GIN_SEGMENT_DELETE:
                appendStringInfo(buf, " %d (delete)", a_segno);
                break;
            case GIN_SEGMENT_INSERT:
                appendStringInfo(buf, " %d (insert)", a_segno);
                break;
            case GIN_SEGMENT_REPLACE:
                appendStringInfo(buf, " %d (replace)", a_segno);
                break;
            default:
                appendStringInfo(buf, " %d unknown action %d ?\?\?", a_segno, a_action);
                /* cannot decode unrecognized actions further */
                return;
        }
    }
}

const char* gin_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    switch (info) {
        case XLOG_GIN_CREATE_INDEX:
            return "gin_create_index";
            break;
        case XLOG_GIN_CREATE_PTREE:
            return "gin_create_ptree";
            break;
        case XLOG_GIN_INSERT:
            return "gin_insert";
            break;
        case XLOG_GIN_SPLIT:
            return "gin_split";
            break;
        case XLOG_GIN_VACUUM_PAGE:
            return "gin_vacuum";
            break;
        case XLOG_GIN_VACUUM_DATA_LEAF_PAGE:
            return "gin_vacuum_leaf";
            break;
        case XLOG_GIN_DELETE_PAGE:
            return "gin_delete_page";
            break;
        case XLOG_GIN_UPDATE_META_PAGE:
            return "gin_update_metapage";
            break;
        case XLOG_GIN_INSERT_LISTPAGE:
            return "gin_insert_listpage";
            break;
        case XLOG_GIN_DELETE_LISTPAGE:
            return "gin_delete_listpage";
            break;
        default:
            return "unknow_type";
            break;
    }
}

void gin_desc(StringInfo buf, XLogReaderState *record)
{
    char *rec = XLogRecGetData(record);
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    switch (info) {
        case XLOG_GIN_CREATE_INDEX:
            appendStringInfoString(buf, "Create index");
            /* no further information */
            break;
        case XLOG_GIN_CREATE_PTREE:
            appendStringInfoString(buf, "Create ptree");
            /* no further information */
            break;
        case XLOG_GIN_INSERT: {
            ginxlogInsert *xlrec = (ginxlogInsert *)rec;

            appendStringInfoString(buf, "Insert item, ");
            appendStringInfo(buf, "isdata: %c isleaf: %c", (xlrec->flags & GIN_INSERT_ISDATA) ? 'T' : 'F',
                             (xlrec->flags & GIN_INSERT_ISLEAF) ? 'T' : 'F');
            if (!(xlrec->flags & GIN_INSERT_ISLEAF)) {
                char *payload = rec + sizeof(ginxlogInsert);
                BlockNumber leftChildBlkno;
                BlockNumber rightChildBlkno;

                leftChildBlkno = BlockIdGetBlockNumber((BlockId)payload);
                payload += sizeof(BlockIdData);
                rightChildBlkno = BlockIdGetBlockNumber((BlockId)payload);
                payload += sizeof(BlockNumber);
                appendStringInfo(buf, " children: %u/%u", leftChildBlkno, rightChildBlkno);
            }
            if (XLogRecHasBlockImage(record, 0))
                appendStringInfoString(buf, " (full page image)");
            else {
                char *payload = XLogRecGetBlockData(record, 0, NULL);

                if (!(xlrec->flags & GIN_INSERT_ISDATA))
                    appendStringInfo(buf, " isdelete: %c", (((ginxlogInsertEntry *)payload)->isDelete) ? 'T' : 'F');
                else if (xlrec->flags & GIN_INSERT_ISLEAF)
                    desc_recompress_leaf(buf, (ginxlogRecompressDataLeaf *)payload);
                else {
                    ginxlogInsertDataInternal *insertData = (ginxlogInsertDataInternal *)payload;

                    appendStringInfo(buf, " pitem: %u-%u/%hu", PostingItemGetBlockNumber(&insertData->newitem),
                                     ItemPointerGetBlockNumber(&insertData->newitem.key),
                                     ItemPointerGetOffsetNumber(&insertData->newitem.key));
                }
            }
        } break;
        case XLOG_GIN_SPLIT: {
            ginxlogSplit *xlrec = (ginxlogSplit *)rec;

            appendStringInfoString(buf, "Page split, ");
            appendStringInfo(buf, "isrootsplit: %c", (((ginxlogSplit *)rec)->flags & GIN_SPLIT_ROOT) ? 'T' : 'F');
            appendStringInfo(buf, " isdata: %c isleaf: %c", (xlrec->flags & GIN_INSERT_ISDATA) ? 'T' : 'F',
                             (xlrec->flags & GIN_INSERT_ISLEAF) ? 'T' : 'F');
        } break;
        case XLOG_GIN_VACUUM_PAGE:
            appendStringInfoString(buf, "Vacuum page");
            /* no further information */
            break;
        case XLOG_GIN_VACUUM_DATA_LEAF_PAGE: {
            appendStringInfoString(buf, "Vacuum data leaf page, ");
            if (XLogRecHasBlockImage(record, 0))
                appendStringInfoString(buf, " (full page image)");
            else {
                ginxlogVacuumDataLeafPage *xlrec = (ginxlogVacuumDataLeafPage *)XLogRecGetBlockData(record, 0, NULL);

                desc_recompress_leaf(buf, &xlrec->data);
            }
        } break;
        case XLOG_GIN_DELETE_PAGE:
            appendStringInfoString(buf, "Delete page");
            break;
        case XLOG_GIN_UPDATE_META_PAGE:
            appendStringInfoString(buf, "Update metapage");
            break;
        case XLOG_GIN_INSERT_LISTPAGE:
            appendStringInfoString(buf, "Insert new list page");
            break;
        case XLOG_GIN_DELETE_LISTPAGE:
            appendStringInfo(buf, "Delete list pages, ndeleted: %d", ((ginxlogDeleteListPages *)rec)->ndeleted);
            break;
        default:
            appendStringInfo(buf, "unknown gin op code %hhu", info);
            break;
    }
}
