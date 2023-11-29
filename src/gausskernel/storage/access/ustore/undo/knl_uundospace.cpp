/* -------------------------------------------------------------------------
 *
 * knl_uundospace.cpp
 *    c++ code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/ustore/undo/knl_uundospace.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/ustore/undo/knl_uundospace.h"
#include "access/ustore/undo/knl_uundozone.h"
#include "access/ustore/knl_whitebox_test.h"
#include "storage/lock/lwlock.h"
#include "storage/smgr/smgr.h"
#include "access/multi_redo_api.h"

namespace undo {
static uint64 USEG_SIZE(uint32 dbId)
{
    if (dbId == UNDO_DB_OID) {
        return UNDO_LOG_SEGMENT_SIZE;
    }
    return UNDO_META_SEGMENT_SIZE;
}

static uint32 USEG_BLOCKS(uint32 dbId)
{
    if (dbId == UNDO_DB_OID) {
        return UNDOSEG_SIZE;
    }
    return UNDO_META_SEG_SIZE;
}

uint32 UndoSpace::Used(void)
{
    WHITEBOX_TEST_STUB(UNDO_USED_FAILED, WhiteboxDefaultErrorEmit);

    if (tail_ < head_) {
        ereport(PANIC, (errmodule(MOD_UNDO),
            errmsg(UNDOFORMAT("space tail %lu < head %lu."), tail_, head_)));
    }
    return (uint32)((tail_ - head_) / BLCKSZ);
}

UndoLogOffset UndoSpace::find_oldest_offset(int zid, uint32 db_id) const
{
    UndoLogOffset offset = head_;
    BlockNumber blockno;
    RelFileNode rnode;
    UNDO_PTR_ASSIGN_REL_FILE_NODE(rnode, MAKE_UNDO_PTR(zid, offset), db_id);
    SMgrRelation reln = smgropen(rnode, InvalidBackendId);
    uint64 seg_size = USEG_SIZE(db_id);
    while (offset >=seg_size) {
        offset -= seg_size;
        blockno = (BlockNumber)(offset / BLCKSZ);
        if (!smgrexists(reln, MAIN_FORKNUM, blockno)) {
            offset += seg_size;
            break;
        }
    }
    smgrclose(reln);
    return offset;
}

/* Create segments needed to increase end_ to newEnd. */
void UndoSpace::ExtendUndoLog(int zid, UndoLogOffset offset, uint32 dbId)
{
    RelFileNode rnode;
    UndoLogOffset tail = tail_;
    Assert(tail < offset && head_ <= tail_);
    BlockNumber blockno;
    UNDO_PTR_ASSIGN_REL_FILE_NODE(rnode, MAKE_UNDO_PTR(zid, offset), dbId);
    SMgrRelation reln = smgropen(rnode, InvalidBackendId);
    uint64 segSize = USEG_SIZE(dbId);
    uint32 segBlocks = USEG_BLOCKS(dbId);
    Assert(offset % segSize == 0);

    WHITEBOX_TEST_STUB(UNDO_EXTEND_LOG_FAILED, WhiteboxDefaultErrorEmit);
    while (tail < offset) {
        if ((!t_thrd.xlog_cxt.InRecovery) && (static_cast<int>(g_instance.undo_cxt.undoTotalSize) +
            static_cast<int>(g_instance.undo_cxt.undoMetaSize) >= u_sess->attr.attr_storage.undo_space_limit_size)) {
            uint64 undoSize = (g_instance.undo_cxt.undoTotalSize + g_instance.undo_cxt.undoMetaSize) * BLCKSZ /
                (1024 * 1024);
            uint64 limitSize = u_sess->attr.attr_storage.undo_space_limit_size * BLCKSZ / (1024 * 1024);
            ereport(ERROR, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT(
                "undo space size %luM > limit size %luM. Please increase the undo_space_limit_size."),
                undoSize, limitSize)));
        }
        blockno = (BlockNumber)(tail / BLCKSZ + 1);
        /* Create a new undo segment. */
        smgrextend(reln, MAIN_FORKNUM, blockno, NULL, false);
        pg_atomic_fetch_add_u32(&g_instance.undo_cxt.undoTotalSize, segBlocks);
        tail += segSize;
    }

    ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT(
        "entxend undo log, total blocks=%u, zid=%d, dbid=%u, head=%lu."),
        g_instance.undo_cxt.undoTotalSize, zid, dbId, offset)));
    SetTail(offset);
    return;
}

/* Unlink undo segment file from startOffset to endOffset. */
void UndoSpace::UnlinkUndoLog(int zid, UndoLogOffset offset, uint32 dbId)
{
    RelFileNode rnode;
    UndoLogOffset head;
    UndoLogOffset old_head;
    if (t_thrd.undorecycler_cxt.is_recovery_in_progress) {
        head = head_exrto;
        old_head = head_exrto;
        set_head_exrto(offset);
    } else {
        head = head_;
        old_head = head_;
        SetHead(offset);
    }
    Assert(head < offset && head_ <= tail_);
    UNDO_PTR_ASSIGN_REL_FILE_NODE(rnode, MAKE_UNDO_PTR(zid, offset), dbId);
    SMgrRelation reln = smgropen(rnode, InvalidBackendId);
    uint64 segSize = USEG_SIZE(dbId);
    uint32 segBlocks = USEG_BLOCKS(dbId);
    Assert(offset % segSize == 0);
    SetHead(offset);

    WHITEBOX_TEST_STUB(UNDO_UNLINK_LOG_FAILED, WhiteboxDefaultErrorEmit);
    while (head < offset) {
        /* Create a new undo segment. */
        smgrdounlink(reln, t_thrd.xlog_cxt.InRecovery, (head / BLCKSZ));
        ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT(
            "unlink undo log, zid=%d, dbid=%u, new_head=%lu, segId:%lu."),
            zid, dbId, offset, head/segSize)));
        if (g_instance.undo_cxt.undoTotalSize < segBlocks) {
            ereport(PANIC, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT(
                "unlink undo log, total blocks=%u < segment size."),
                g_instance.undo_cxt.undoTotalSize)));
        }
        pg_atomic_fetch_sub_u32(&g_instance.undo_cxt.undoTotalSize, segBlocks);
        head += segSize;
    }
    smgrclose(reln);
    ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT(
        "unlink undo log, total blocks=%u, zid=%d, dbid=%u, head=%lu, old_head:%lu."),
        g_instance.undo_cxt.undoTotalSize, zid, dbId, offset, old_head)));
    return;
}

/*
 * Unlink undo segment files which are residual in extreme RTO standby read,
 * unlink from start to end(not include).
 */
void UndoSpace::unlink_residual_log(int zid, UndoLogOffset start, UndoLogOffset end, uint32 db_id) const
{
    RelFileNode rnode;
    UNDO_PTR_ASSIGN_REL_FILE_NODE(rnode, MAKE_UNDO_PTR(zid, start), db_id);
    SMgrRelation reln = smgropen(rnode, InvalidBackendId);
    uint64 seg_size = USEG_SIZE(db_id);

    while (start/seg_size < end/seg_size) {
        /* delete a new undo segment. */
        BlockNumber block = (BlockNumber)(start / BLCKSZ);
        smgrdounlink(reln, t_thrd.xlog_cxt.InRecovery, block);
        ereport(DEBUG1, (errmodule(MOD_UNDO), errmsg(UNDOFORMAT(
            "unlink_residual_log, zid=%d, dbid=%u, start=%lu, end=%lu, segId:%lu, endSegId:%lu."),
            zid, db_id, start, end, start/seg_size, end/seg_size)));
        start += seg_size;
    }
    smgrclose(reln);
    return;
}

void UndoSpace::CreateNonExistsUndoFile(int zid, uint32 dbId)
{
    if (head_ == tail_) {
        return;
    }
    UndoLogOffset offset = head_;
    RelFileNode rnode;
    BlockNumber blockno;
    UNDO_PTR_ASSIGN_REL_FILE_NODE(rnode, MAKE_UNDO_PTR(zid, offset), dbId);
    SMgrRelation reln = smgropen(rnode, InvalidBackendId);
    uint64 segSize = USEG_SIZE(dbId);
    uint32 segBlocks = USEG_BLOCKS(dbId);
    while (offset < tail_) {
        blockno = (BlockNumber)(offset / BLCKSZ + 1);
        if (!smgrexists(reln, MAIN_FORKNUM, blockno)) {
            smgrextend(reln, MAIN_FORKNUM, blockno, NULL, false);
            ereport(DEBUG1, (errmodule(MOD_UNDO), 
                errmsg(UNDOFORMAT("undo file not exists, zid %d, blockno=%u."), zid, blockno)));
            pg_atomic_fetch_add_u32(&g_instance.undo_cxt.undoTotalSize, segBlocks);
        }
        offset += segSize;
    }
    smgrclose(reln);
    return;
}

/*
 * Persist undospace metadata to disk. The fomart as follows
 * ----------|--------|---------|---------|--------------|
 * undoMeta |undoMeta|undoMeta |undoMeta | pageCRC(32bit)
 * |->--------------------512---------------------------<-|
 */
void UndoSpace::CheckPointUndoSpace(int fd, UndoSpaceType type)
{
    Assert(fd > 0);
    bool retry = false;
    bool needFlushMetaPage = false;
    uint32 ret = 0;
    uint32 totalPageCnt = 0;
    uint32 uspOffset = 0;
    uint64 writeSize = 0;
    pg_crc32 metaPageCrc = 0;
    XLogRecPtr flushLsn = InvalidXLogRecPtr;
    char uspMetaPagebuffer[UNDO_WRITE_SIZE] = {'\0'};
    uint64 currWritePos = 0;
    uint32 cycle = 0;
    int rc = 0;
    if (type == UNDO_LOG_SPACE) {
        UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOZONE_COUNT_PER_PAGE, totalPageCnt);
        lseek(fd, totalPageCnt * UNDO_META_PAGE_SIZE, SEEK_SET);
    } else {
        UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOZONE_COUNT_PER_PAGE, totalPageCnt);
        uint32 seek = totalPageCnt * UNDO_META_PAGE_SIZE;
        UNDOZONE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOSPACE_COUNT_PER_PAGE, totalPageCnt);
        seek += totalPageCnt * UNDO_META_PAGE_SIZE;
        lseek(fd, seek, SEEK_SET);
    }
    int spacePageBegin = lseek(fd, 0, SEEK_CUR);
    if (spacePageBegin < 0) {
        ereport(ERROR,
            (errcode_for_file_access(), errmsg(UNDOFORMAT("could not seek start position in undo metadata: %m"))));
        return;
    }
    /* Get total page count of storing all undospaces. */
    UNDOSPACE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOSPACE_COUNT_PER_PAGE, totalPageCnt);
    DECLARE_NODE_COUNT();
    uint32 zoneCountPerLevel = ZONE_COUNT_PER_LEVELS(nodeCount);
    for (uint32 loop = 0; loop < PERSIST_ZONE_COUNT; loop++) {
        UndoSpace *usp = NULL;
        UndoSpaceMetaInfo *uspMetaPointer = NULL;
        if (loop % UNDOSPACE_COUNT_PER_WRITE == 0) {
            cycle = loop;
            rc = memset_s(uspMetaPagebuffer, UNDO_WRITE_SIZE, 0, UNDO_WRITE_SIZE);
            securec_check(rc, "\0", "\0");

            if ((uint32)(PERSIST_ZONE_COUNT - loop) < UNDOSPACE_COUNT_PER_WRITE) {
                writeSize = ((PERSIST_ZONE_COUNT - loop) / UNDOSPACE_COUNT_PER_WRITE + 1) * UNDO_META_PAGE_SIZE;
            } else {
                writeSize = UNDO_WRITE_SIZE;
            }
        }
        if (g_instance.undo_cxt.uZones[loop] != NULL) {
            if (type == UNDO_LOG_SPACE) {
                usp = ((UndoZone *)g_instance.undo_cxt.uZones[loop])->GetUndoSpace();
            } else {
                usp = ((UndoZone *)g_instance.undo_cxt.uZones[loop])->GetSlotSpace();
            }
            /* If one space on a meta page is dirty, write total spaces of this 4K page. */
            if (usp->IsDirty()) {
                needFlushMetaPage = true;
            }
        }

        if (needFlushMetaPage && 
            ((loop + 1) % UNDOSPACE_COUNT_PER_WRITE == 0 || (loop + 1) == PERSIST_ZONE_COUNT)) {
            while (cycle <= loop) {
                /* Locate the undospace on the undo page, then refresh. */
                uspOffset = cycle % UNDOSPACE_COUNT_PER_PAGE;
                char *pageOffset = uspMetaPagebuffer + 
                        ((cycle % UNDOSPACE_COUNT_PER_WRITE) / UNDOSPACE_COUNT_PER_PAGE) * UNDO_META_PAGE_SIZE;
                uspMetaPointer = (UndoSpaceMetaInfo *)(pageOffset + uspOffset * sizeof(UndoSpaceMetaInfo));

                if (g_instance.undo_cxt.uZones[cycle] != NULL) {
                    if (type == UNDO_LOG_SPACE) {
                        usp = ((UndoZone *)g_instance.undo_cxt.uZones[cycle])->GetUndoSpace();
                    } else {
                        usp = ((UndoZone *)g_instance.undo_cxt.uZones[cycle])->GetSlotSpace();
                    }
                    usp->LockSpace();
                    /* Set the initial value of flushLsn when the first undospace of each meta page is traversed. */
                    if (cycle % UNDOSPACE_COUNT_PER_WRITE == 0) {
                        flushLsn = usp->LSN();
                    } else {
                        /* Pick out max lsn of total undospaces on one meta page. */
                        if (usp->LSN() > flushLsn) {
                            flushLsn = usp->LSN();
                        }
                    }

                    
                    if (cycle < zoneCountPerLevel) {
                        uspMetaPointer->version = UNDO_SPACE_META_VERSION;
                        uspMetaPointer->lsn = usp->LSN();
                        uspMetaPointer->head = usp->Head();
                        uspMetaPointer->tail = usp->Tail();
                    } else {
                        uspMetaPointer->version = UNDO_SPACE_META_VERSION;
                        uspMetaPointer->lsn = 0;
                        uspMetaPointer->head = 0;
                        uspMetaPointer->tail = 0;
                    }
                    usp->MarkClean();
                    usp->UnlockSpace();
                } else {
                    uspMetaPointer->version = UNDO_SPACE_META_VERSION;
                    uspMetaPointer->lsn = 0;
                    uspMetaPointer->head = 0;
                    uspMetaPointer->tail = 0;
                }

                /* If needFlushMetaPage is set to true, all undospaces on the meta page are written. */
                if ((cycle + 1) % UNDOSPACE_COUNT_PER_PAGE == 0 || cycle == PERSIST_ZONE_COUNT - 1) {
                    uint64 crcSize;
                    if (cycle == PERSIST_ZONE_COUNT - 1) {
                        crcSize = (PERSIST_ZONE_COUNT % UNDOSPACE_COUNT_PER_PAGE) * sizeof(UndoSpaceMetaInfo);
                    } else {
                        crcSize = UNDOSPACE_COUNT_PER_PAGE * sizeof(UndoSpaceMetaInfo);
                    }
                    /* Flush wal buffer of undo xlog first. */
                    INIT_CRC32C(metaPageCrc);
                    COMP_CRC32C(metaPageCrc, (void *)pageOffset, crcSize);
                    FIN_CRC32C(metaPageCrc);

                    /* Store CRC behind the last undospace meta on the page. */
                    *(pg_crc32 *)(pageOffset + crcSize) = metaPageCrc;
                }
                if ((cycle + 1) % UNDOSPACE_COUNT_PER_WRITE == 0 || cycle == PERSIST_ZONE_COUNT - 1) {
                    XLogWaitFlush(flushLsn);
                    currWritePos = lseek(fd, spacePageBegin + (cycle / UNDOSPACE_COUNT_PER_WRITE) * UNDO_WRITE_SIZE, 
                        SEEK_SET);
                RE_WRITE:
                    lseek(fd, currWritePos, SEEK_SET);
                    ret = write(fd, uspMetaPagebuffer, writeSize);
                    if (ret != writeSize && !retry) {
                        retry = true;
                        goto RE_WRITE;
                    } else if (ret != writeSize && retry) {
                        ereport(ERROR, (errmsg(UNDOFORMAT("Write meta page failed expect size(%lu) real size(%u)."),
                            writeSize, ret)));
                        return;
                    }
                    ereport(DEBUG1, (errmsg(UNDOFORMAT("undo metadata write loop %u."), cycle)));
                    needFlushMetaPage = false;
                }
                cycle++;
            }
        }
    }
}

void UndoSpace::RecoveryUndoSpace(int fd, UndoSpaceType type)
{
    Assert(fd > 0);
    int rc = 0;
    uint32 zoneId = 0;
    uint32 spaceMetaSize = 0;
    uint32 totalPageCnt = 0;
    pg_crc32 pageCrcVal = 0; /* CRC store in undo meta page */
    pg_crc32 comCrcVal = 0;  /* calculating CRC current */
    char *uspMetaBuffer = NULL;
    MemoryContext oldContext = MemoryContextSwitchTo(g_instance.undo_cxt.undoContext);
    char *persistBlock = (char *)palloc0(UNDO_META_PAGE_SIZE * PAGES_READ_NUM);
    (void*)MemoryContextSwitchTo(oldContext);
    if (type == UNDO_LOG_SPACE) {
        UNDOSPACE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOZONE_COUNT_PER_PAGE, totalPageCnt);
        lseek(fd, totalPageCnt * UNDO_META_PAGE_SIZE, SEEK_SET);
    } else {
        UNDOSPACE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOZONE_COUNT_PER_PAGE, totalPageCnt);
        uint32 seek = totalPageCnt * UNDO_META_PAGE_SIZE;
        UNDOSPACE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOSPACE_COUNT_PER_PAGE, totalPageCnt);
        seek += totalPageCnt * UNDO_META_PAGE_SIZE;
        lseek(fd, seek, SEEK_SET);
    }
    
    UNDOSPACE_META_PAGE_COUNT(PERSIST_ZONE_COUNT, UNDOSPACE_COUNT_PER_PAGE, totalPageCnt);
    spaceMetaSize = totalPageCnt * UNDO_META_PAGE_SIZE / BLCKSZ;
    g_instance.undo_cxt.undoMetaSize += spaceMetaSize;

    for (zoneId = 0; zoneId < PERSIST_ZONE_COUNT; zoneId++) {
        UndoSpaceMetaInfo *uspMetaInfo = NULL;
        if (zoneId % (UNDOSPACE_COUNT_PER_PAGE * PAGES_READ_NUM) == 0) {
            Size readSize;
            if ((uint32)(PERSIST_ZONE_COUNT - zoneId) < UNDOSPACE_COUNT_PER_PAGE * PAGES_READ_NUM) {
                readSize = ((uint32)(PERSIST_ZONE_COUNT - zoneId) / UNDOSPACE_COUNT_PER_PAGE + 1) 
                    * UNDO_META_PAGE_SIZE;
            } else {
                readSize = UNDO_META_PAGE_SIZE * PAGES_READ_NUM;
            }
            rc = memset_s(persistBlock, UNDO_META_PAGE_SIZE * PAGES_READ_NUM, 0, 
                UNDO_META_PAGE_SIZE * PAGES_READ_NUM);
            securec_check(rc, "\0", "\0");
            uint32 ret = read(fd, persistBlock, readSize);
            if (ret != readSize) {
                ereport(ERROR, (errmsg(UNDOFORMAT("Read undo meta page, expect size(%lu), real size(%u)."),
                    readSize, ret)));
                return;
            }
        }
        if (zoneId % UNDOSPACE_COUNT_PER_PAGE == 0) {
            uspMetaBuffer = persistBlock + 
                ((zoneId % (UNDOSPACE_COUNT_PER_PAGE * PAGES_READ_NUM)) / UNDOSPACE_COUNT_PER_PAGE)
                * UNDO_META_PAGE_SIZE;
            uint32 count = UNDOSPACE_COUNT_PER_PAGE;
            if ((uint32)(PERSIST_ZONE_COUNT - zoneId) < UNDOSPACE_COUNT_PER_PAGE) {
                count = PERSIST_ZONE_COUNT - zoneId;
            }
            /* Get page CRC from uspMetaBuffer. */
            pageCrcVal = *(pg_crc32 *)(uspMetaBuffer + sizeof(UndoSpaceMetaInfo) * count);
            /* 
             * Calculate the CRC value based on all undospace meta information stored on the page. 
             * Then compare with pageCrcVal.
             */
            INIT_CRC32C(comCrcVal);
            COMP_CRC32C(comCrcVal, (void *)uspMetaBuffer, sizeof(UndoSpaceMetaInfo) * count);
            FIN_CRC32C(comCrcVal);
            if (!EQ_CRC32C(pageCrcVal, comCrcVal)) {
                ereport(ERROR,
                    (errmsg(UNDOFORMAT("Undo meta CRC calculated(%u) is different from CRC recorded(%u) in page."),
                        comCrcVal, pageCrcVal)));
                return;
            }
        }

        int offset = zoneId % UNDOSPACE_COUNT_PER_PAGE;
        uspMetaInfo = (UndoSpaceMetaInfo *)(uspMetaBuffer + offset * sizeof(UndoSpaceMetaInfo));
        if (uspMetaInfo->tail == 0 &&
            (g_instance.undo_cxt.uZones == NULL || g_instance.undo_cxt.uZones[zoneId] == NULL)) {
            continue;
        }
        UndoZone *uzone = UndoZoneGroup::GetUndoZone(zoneId, true);
        UndoSpace *usp = uzone->GetSpace(type);
        usp->MarkClean();
        usp->SetLSN(uspMetaInfo->lsn);
        usp->SetHead(uspMetaInfo->head);
        usp->set_head_exrto(uspMetaInfo->head);
        usp->SetTail(uspMetaInfo->tail);
        if (type == UNDO_LOG_SPACE) {
            usp->CreateNonExistsUndoFile(zoneId, UNDO_DB_OID);
        } else {
            usp->CreateNonExistsUndoFile(zoneId, UNDO_SLOT_DB_OID);
        }
        pg_atomic_fetch_add_u32(&g_instance.undo_cxt.undoTotalSize, usp->Used());
        ereport(DEBUG1, (errmsg(UNDOFORMAT("recovery_space_meta, zone_id:%u, type:%u, "
            "lsn:%lu, head:%lu, tail:%lu."),
            zoneId, type, uspMetaInfo->lsn, uspMetaInfo->head, uspMetaInfo->tail)));
    }
    pfree(persistBlock);
}
}
