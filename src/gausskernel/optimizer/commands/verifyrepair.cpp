/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
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
 * pagerepair.cpp
 *		verify and repair bad pages and files..
 *
 * IDENTIFICATION
 *      src/gausskernel/optimizer/commands/verifyrepair.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * @Description: add statistics of bad block where read a bad page/cu
 * @IN RelFileNodeBackend: RelFileNodeBackend for page/cu
 * @IN forknum: forknum for page/cu
 * @IN blocknum: blocknum for page/cu
 */

#include "commands/verify.h"
#include "commands/copy.h"
#include "access/tableam.h"
#include "commands/tablespace.h"
#include "storage/smgr/fd.h"
#include "storage/smgr/relfilenode_hash.h"
#include "storage/smgr/segment.h"
#include "storage/cfs/cfs_converter.h"
#include "storage/cfs/cfs_repair.h"

const int TIMEOUT_MIN = 60;
const int TIMEOUT_MAX = 3600;
static void checkUserPermission();
static void checkInstanceType();
static void checkSupUserOrOperaMode();
void gs_tryrepair_compress_extent(SMgrRelation reln, BlockNumber logicBlockNumber);

/*
 * Record a statistics of bad block:
 * read a bad page/cu  --> addGlobalRepairBadBlockStat() record in global_repair_bad_block_stat
 *
 * Query statistics of bad block:
 *  local_bad_block_info() read the global_repair_bad_block_stat and output
 *
 * @Description: init process statistics of bad block hash table
 */
void initRepairBadBlockStat()
{
    HASHCTL info;
    if (g_instance.repair_cxt.global_repair_bad_block_stat == NULL) {
        /* hash accessed by database file id */
        errno_t rc = memset_s(&info, sizeof(info), 0, sizeof(info));
        securec_check(rc, "", "");
        info.keysize = sizeof(BadBlockKey);
        info.entrysize = sizeof(BadBlockEntry);
        info.hash = BadBlockKeyHash;
        info.match = BadBlockKeyMatch;
        info.hcxt = INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE);
        g_instance.repair_cxt.global_repair_bad_block_stat =
            hash_create("Page Repair Hash Table", MAX_REPAIR_PAGE_NUM, &info,
                        HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);
        if (!g_instance.repair_cxt.global_repair_bad_block_stat) {
            ereport(FATAL, (errcode(ERRCODE_INITIALIZE_FAILED),
                    (errmsg("could not initialize page repair Hash table"))));
        }
    }
}

bool getCompressedRelOpt(RelFileNode *relnode)
{
    Relation relation = try_relation_open(relnode->relNode, AccessShareLock);
    if (!RelationIsValid(relation)) {
        return false;
    }
    *relnode = relation->rd_node;
    relation_close(relation, AccessShareLock);
    return true;
}

/* BatchClearBadBlock
 *          clear global_repair_bad_block_stat hashtable entry when the relation drop or truncate.
 */
void BatchClearBadBlock(const RelFileNode rnode, ForkNumber forknum, BlockNumber startblkno)
{
    HASH_SEQ_STATUS status;
    BadBlockEntry *entry = NULL;
    bool found = false;
    HTAB* bad_block_hash = g_instance.repair_cxt.global_repair_bad_block_stat;

    if (IsSegmentFileNode(rnode)) {
        return;
    }

    LWLockAcquire(RepairBadBlockStatHashLock, LW_EXCLUSIVE);
    if (g_instance.repair_cxt.global_repair_bad_block_stat == NULL) {
        LWLockRelease(RepairBadBlockStatHashLock);
        return;
    }

    hash_seq_init(&status, bad_block_hash);
    while ((entry = (BadBlockEntry *)hash_seq_search(&status)) != NULL) {
        if (RelFileNodeEquals(rnode, entry->key.relfilenode) && entry->key.forknum == forknum &&
            entry->key.blocknum >= startblkno) {
            (void)hash_search(bad_block_hash, &(entry->key), HASH_REMOVE, &found);
        }
    }

    LWLockRelease(RepairBadBlockStatHashLock);
}

bool BadBlockMatch(BadBlockEntry *entry, RelFileNode rnode, ForkNumber forknum, BlockNumber segno)
{
    if (IsSegmentFileNode(rnode)) {
        Oid relNode = 0;
        BlockNumber blknum = 0;

        if (entry->pblk.relNode != EXTENT_INVALID) {
            relNode = entry->pblk.relNode;
            blknum = entry->pblk.block;
        } else {
            SegPageLocation loc =
                    seg_get_physical_location(entry->key.relfilenode, entry->key.forknum, entry->key.blocknum);
            entry->pblk.relNode = relNode = (uint8) EXTENT_SIZE_TO_TYPE(loc.extent_size);
            entry->pblk.block = blknum = loc.blocknum;
        }

        if (relNode == rnode.relNode && entry->key.relfilenode.spcNode == rnode.spcNode &&
            entry->key.relfilenode.dbNode == rnode.dbNode && entry->key.forknum == forknum &&
            blknum >= segno * RELSEG_SIZE && blknum < (segno + 1) * RELSEG_SIZE) {
            return true;
        }
    } else {
        if (RelFileNodeEquals(rnode, entry->key.relfilenode) && entry->key.forknum == forknum &&
            entry->key.blocknum >= segno * RELSEG_SIZE && entry->key.blocknum < (segno + 1) * RELSEG_SIZE) {
            return true;
        }
    }
    return false;
}

/* BatchUpdateRepairTime
 *          update global_repair_bad_block_stat hashtable repair time, when the file repair finish.
 */
void BatchUpdateRepairTime(RelFileNode rnode, ForkNumber forknum, BlockNumber segno)
{
    HASH_SEQ_STATUS status;
    BadBlockEntry *entry = NULL;
    HTAB* bad_block_hash = g_instance.repair_cxt.global_repair_bad_block_stat;

    LWLockAcquire(RepairBadBlockStatHashLock, LW_EXCLUSIVE);
    if (g_instance.repair_cxt.global_repair_bad_block_stat == NULL) {
        LWLockRelease(RepairBadBlockStatHashLock);
        return;
    }

    hash_seq_init(&status, bad_block_hash);
    while ((entry = (BadBlockEntry *)hash_seq_search(&status)) != NULL) {
        if (BadBlockMatch(entry, rnode, forknum, segno)) {
            entry->repair_time = GetCurrentTimestamp();
        }
    }

    LWLockRelease(RepairBadBlockStatHashLock);
}

void UpdateRepairTime(const RelFileNode &rnode, ForkNumber forknum, BlockNumber blocknum)
{
    bool found = false;
    BadBlockKey key;

    key.relfilenode.spcNode = rnode.spcNode;
    key.relfilenode.dbNode = rnode.dbNode;
    key.relfilenode.relNode = rnode.relNode;
    key.relfilenode.bucketNode = rnode.bucketNode;
    key.relfilenode.opt = rnode.opt;
    key.forknum = forknum;
    key.blocknum = blocknum;

    Assert(g_instance.repair_cxt.global_repair_bad_block_stat != NULL);
    LWLockAcquire(RepairBadBlockStatHashLock, LW_EXCLUSIVE);
    if (g_instance.repair_cxt.global_repair_bad_block_stat == NULL) {
        LWLockRelease(RepairBadBlockStatHashLock);
        return;
    }
     /* insert if not find, if out of memory return NULL */
    BadBlockEntry* entry =
            (BadBlockEntry*)hash_search(g_instance.repair_cxt.global_repair_bad_block_stat, &key, HASH_ENTER, &found);

    if (entry != NULL) {
        if (!found) {
            // Update the check time when the first insertion is performed.
            char* path = relpathperm(key.relfilenode, key.forknum);
            errno_t rc = snprintf_s(entry->path, MAX_PATH, MAX_PATH - 1, "%s", path);
            securec_check_ss(rc, "\0", "\0");
            pfree(path);
            entry->check_time = GetCurrentTimestamp();
            entry->repair_time = GetCurrentTimestamp();
            entry->key = key;
        } else {
            entry->repair_time = GetCurrentTimestamp();
        }
    }
    LWLockRelease(RepairBadBlockStatHashLock);
}

void addGlobalRepairBadBlockStat(const RelFileNodeBackend &rnode, ForkNumber forknum, BlockNumber blocknum)
{
    TimestampTz check_time = GetCurrentTimestamp();
    BadBlockKey key;
    errno_t rc = 0;
    bool found = false;

    key.relfilenode.spcNode = rnode.node.spcNode;
    key.relfilenode.dbNode = rnode.node.dbNode;
    key.relfilenode.relNode = rnode.node.relNode;
    key.relfilenode.bucketNode = rnode.node.bucketNode;
    key.relfilenode.opt = rnode.node.opt;
    key.forknum = forknum;
    key.blocknum = blocknum;

    Assert(g_instance.repair_cxt.global_repair_bad_block_stat != NULL);
    LWLockAcquire(RepairBadBlockStatHashLock, LW_EXCLUSIVE);
    if (g_instance.repair_cxt.global_repair_bad_block_stat == NULL) {
        LWLockRelease(RepairBadBlockStatHashLock);
        return;
    }

    /* insert if not find, if out of memory return NULL */
    BadBlockEntry* entry =
            (BadBlockEntry*)hash_search(g_instance.repair_cxt.global_repair_bad_block_stat, &key, HASH_ENTER, &found);

    if (entry != NULL) {
        if (!found) {
            // Update the check time when the first insertion is performed.
            char* path = relpathperm(key.relfilenode, key.forknum);
            rc = snprintf_s(entry->path, MAX_PATH, MAX_PATH - 1, "%s", path);
            securec_check_ss(rc, "\0", "\0");
            pfree(path);
            entry->check_time = check_time;
            entry->repair_time = -1;
            entry->key = key;
            entry->pblk.relNode = EXTENT_INVALID;
            entry->pblk.block = InvalidBlockNumber;
        }
        if (entry->repair_time != -1) {
            entry->check_time = check_time;
            entry->repair_time = -1;
        }
    }
    LWLockRelease(RepairBadBlockStatHashLock);
}


bool CheckSum(const PageHeader page, BlockNumber blockNum)
{
    bool checksum_matched = false;
    if (CheckPageZeroCases(page)) {
        uint16 checksum = pg_checksum_page((char*)page, (BlockNumber)blockNum);
        checksum_matched = (checksum == page->pd_checksum);
    }
    return checksum_matched;
}

// only check page is in memory or not, not read it
Buffer PageIsInMemory(SMgrRelation smgr, ForkNumber forkNum, BlockNumber blockNum)
{
    Buffer buf = InvalidBuffer;
    int buf_id = 0;
    BufferDesc *bufDesc = NULL;
    BufferTag new_tag;

    INIT_BUFFERTAG(new_tag, smgr->smgr_rnode.node, forkNum, blockNum);
    uint32 new_hash = BufTableHashCode(&new_tag);
    LWLock *new_partition_lock = BufMappingPartitionLock(new_hash);
    /* see if the block is in the buffer pool already */
    (void)LWLockAcquire(new_partition_lock, LW_SHARED);
    buf_id = BufTableLookup(&new_tag, new_hash);
    if (buf_id != -1) {
        ResourceOwnerEnlargeBuffers(t_thrd.utils_cxt.CurrentResourceOwner);
        bufDesc = GetBufferDescriptor(buf_id);
        buf = BufferDescriptorGetBuffer(bufDesc);
        if (!PinBuffer(bufDesc, NULL)) {
            buf = InvalidBuffer;
        }
    }
    LWLockRelease(new_partition_lock);

    return buf;
}


// init RelFileNode and outputFilename
void PrepForRead(char* path, uint blocknum, bool is_segment, RelFileNode *relnode)
{
    char* pathFirstpart = (char*)palloc0(MAXFNAMELEN);

    RelFileNodeForkNum relfilenode;
    if (is_segment) {
        char* bucketNodestr = strstr(path, "_b");
        if (NULL != bucketNodestr) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    (errmsg("Un-support feature"),
                            errdetail("The repair do not support hashbucket."),
                            errcause("The function is not implemented."),
                            erraction("Do not repair hashbucket."))));
        }
        relfilenode = relpath_to_filenode(path);
        relfilenode.rnode.node.bucketNode = SegmentBktId;
        if (!IsSegmentPhysicalRelNode(relfilenode.rnode.node)) {
            Oid relation_oid = RelidByRelfilenodeCache(relfilenode.rnode.node.spcNode,
                relfilenode.rnode.node.relNode);
            if (!OidIsValid(relation_oid)) {
                relation_oid = RelidByRelfilenode(relfilenode.rnode.node.spcNode,
                    relfilenode.rnode.node.relNode, is_segment);
            }
            if (!OidIsValid(relation_oid)) {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Error parameter to get relation.")));
            }

            SMgrRelation reln = smgropen(relfilenode.rnode.node, InvalidBackendId);
            if (reln->seg_desc == NULL ||
                !(seg_exists(reln, MAIN_FORKNUM, blocknum) && reln->seg_desc[MAIN_FORKNUM] != NULL)) {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Error parameter to get smgr relation.")));
            }

            RelFileNode relFileNode = {
                    .spcNode = reln->seg_space->spcNode,
                    .dbNode = reln->seg_space->dbNode,
                    .relNode = EXTENT_SIZE_TO_TYPE(LEVEL0_PAGE_EXTENT_SIZE),
                    .bucketNode = SegmentBktId
            };

            Buffer buffer = ReadBufferFast((reln->seg_space), relFileNode, MAIN_FORKNUM,
                                           (reln->seg_desc[MAIN_FORKNUM]->head_blocknum), RBM_NORMAL);
            SegmentHead *head = (SegmentHead *)PageGetContents(BufferGetPage(buffer));
            if (head->magic == BUCKET_SEGMENT_MAGIC) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        (errmsg("Un-support feature"),
                                errdetail("The repair do not support hashbucket."),
                                errcause("The function is not implemented."),
                                erraction("Do not repair hashbucket."))));
            }
        } else {
            SegSpace *spc = spc_open(relfilenode.rnode.node.spcNode, relfilenode.rnode.node.dbNode, false, false);
            if (!spc) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        (errmsg("Spc open failed. spcNode is: %u, dbNode is %u",
                                relfilenode.rnode.node.spcNode, relfilenode.rnode.node.dbNode))));
            }
            int egid = EXTENT_TYPE_TO_GROUPID(relfilenode.rnode.node.relNode);
            SegExtentGroup *seg = &spc->extent_group[egid][MAIN_FORKNUM];
            if (seg->segfile->total_blocks <= blocknum) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        (errmsg("Total block is: %u, given block is %u",
                                seg->segfile->total_blocks, blocknum))));
            }
            struct stat statBuf;
            if (stat(path, &statBuf) < 0) {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Error parameter to open file.")));
            }
        }
    } else {
        relfilenode = relpath_to_filenode(path);
        relfilenode.rnode.node.bucketNode = InvalidBktId;
    }
    if (relfilenode.forknumber != MAIN_FORKNUM) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (errmsg("Error forknum is: %d", relfilenode.forknumber))));
    }
    RelFileNodeCopy(*relnode, relfilenode.rnode.node, relfilenode.rnode.node.bucketNode);
    pfree(pathFirstpart);
}

bool tryRepairPage(BlockNumber blocknum, bool is_segment, RelFileNode *relnode, int timeout)
{
    char *buf = (char*)palloc0(BLCKSZ);
    XLogPhyBlock pblk;
    ForkNumber forknum = MAIN_FORKNUM;
    RelFileNode logicalRelNode = {0};
    int logicalBlocknum = 0;
    BlockNumber checksumBlock = blocknum;

    /* if it is compressed relation, fill the opt in RelFileNode */
    if (!IsSegmentPhysicalRelNode(*relnode)) {
        if (!getCompressedRelOpt(relnode)) {
            relnode->opt = 0;
        }
    }

    SMgrRelation smgr = smgropen(*relnode, InvalidBackendId, GetColumnNum(forknum));
    
    /* we need check the pca header page first if the relation is compression relation,
       and we need to try our best to repair this pca header page and the whole extent */
    if (IS_COMPRESSED_RNODE(*relnode, MAIN_FORKNUM)) {
        bool need_repair_pca = false;
        if (!IsSegmentPhysicalRelNode(*relnode)) {
            CfsHeaderPageCheckAndRepair(smgr, blocknum, NULL, ERR_MSG_LEN, &need_repair_pca);
            if (need_repair_pca) {
                /* clear local cache and smgr */
                CacheInvalidateSmgr(smgr->smgr_rnode);

                /* try repair the whole extent */
                gs_tryrepair_compress_extent(smgr, blocknum);
            }
        } else {
            ;
        }
    }

    RelFileNodeBackend relnodeBack;
    relnodeBack.node = *relnode;
    relnodeBack.backend = InvalidBackendId;
    bool isSegmentPhysical = is_segment && IsSegmentPhysicalRelNode(*relnode);

    if (is_segment && !isSegmentPhysical) {
        SegPageLocation loc =
                seg_get_physical_location(*relnode, forknum, blocknum);
        pblk = {
                .relNode = (uint8) EXTENT_SIZE_TO_TYPE(loc.extent_size),
                .block = loc.blocknum,
                .lsn = InvalidXLogRecPtr
        };
        logicalRelNode.relNode = (uint8) EXTENT_SIZE_TO_TYPE(loc.extent_size);
        logicalRelNode.spcNode = relnode->spcNode;
        logicalRelNode.dbNode = relnode->dbNode;
        logicalRelNode.bucketNode = SegmentBktId;
        logicalRelNode.opt = 0;
        logicalBlocknum = loc.blocknum;
    }

    // to repair page
    if (is_segment && !isSegmentPhysical) {
        RemoteReadBlock(relnodeBack, forknum, blocknum, buf, &pblk, timeout);
        checksumBlock = logicalBlocknum;
    } else {
        RemoteReadBlock(relnodeBack, forknum, blocknum, buf, NULL, timeout);
    }

    if (PageIsVerified((Page)buf, checksumBlock)) {
        if (is_segment) {
            SegSpace* spc = spc_open(relnode->spcNode, relnode->dbNode, false, false);
            if (!spc) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        (errmsg("Spc open failed. spcNode is: %u, dbNode is %u",
                                relnode->spcNode, relnode->dbNode))));
            }
            if (isSegmentPhysical) {
                seg_physical_write(spc, *relnode, forknum, blocknum, buf, true);
            } else {
                seg_physical_write(spc, logicalRelNode, forknum, logicalBlocknum, buf, true);
            }
        } else {
            smgrwrite(smgr, forknum, blocknum, buf, true);
        }
        BadBlockKey key;

        key.relfilenode.spcNode = relnode->spcNode;
        key.relfilenode.dbNode = relnode->dbNode;
        key.relfilenode.relNode = relnode->relNode;
        key.relfilenode.bucketNode = relnode->bucketNode;
        key.forknum = forknum;
        key.blocknum = blocknum;

        Assert(g_instance.repair_cxt.global_repair_bad_block_stat != NULL);

        LWLockAcquire(RepairBadBlockStatHashLock, LW_EXCLUSIVE);
        bool found = false;
        BadBlockEntry* entry =
                (BadBlockEntry*)hash_search(g_instance.repair_cxt.global_repair_bad_block_stat, &key,
                                            HASH_ENTER, &found);

        TimestampTz currentTime = GetCurrentTimestamp();
        entry->repair_time = currentTime;
        if (!found) {
            char* path = relpathperm(key.relfilenode, key.forknum);
            errno_t rc = snprintf_s(entry->path, MAX_PATH, MAX_PATH - 1, "%s", path);
            securec_check_ss(rc, "\0", "\0");
            pfree(path);
            entry->check_time = currentTime;
            entry->pblk.relNode = EXTENT_INVALID;
            entry->pblk.block = InvalidBlockNumber;
        }
        LWLockRelease(RepairBadBlockStatHashLock);
        return true;
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("remote get page check error")));
        return false;
    }
}

bool repairPage(char* path, uint blocknum, bool is_segment, int timeout)
{
    // check parameters
    if (blocknum > MaxBlockNumber)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (errmsg("Blocknum should be between 0 and %u. \n", MaxBlockNumber))));

    RelFileNode relnode = {0};

    if (timeout < TIMEOUT_MIN || timeout > TIMEOUT_MAX) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (errmsg("The timeout(%d) is an incorrect input. Value range: [60, 3600]. \n", timeout))));
        return false;
    }
    t_thrd.storage_cxt.timeoutRemoteOpera = timeout;

    PrepForRead((char*)path, blocknum, is_segment, &relnode);

    if (IsSegmentPhysicalRelNode(relnode)) {
        repair_check_physical_type(relnode.spcNode, relnode.dbNode, MAIN_FORKNUM, &(relnode.relNode),
                                   &blocknum);
    }

    return tryRepairPage(blocknum, is_segment, &relnode, timeout);
}

Datum local_bad_block_info(PG_FUNCTION_ARGS)
{
    checkUserPermission();
#define BAD_BLOCK_STAT_NATTS 10
    FuncCallContext* funcctx = NULL;
    HASH_SEQ_STATUS* hash_seq = NULL;

    LWLockAcquire(RepairBadBlockStatHashLock, LW_SHARED);

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc = NULL;
        MemoryContext oldcontext = NULL;
        int i = 1;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();
        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(BAD_BLOCK_STAT_NATTS, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "spc_node", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "db_node", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "rel_node", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "bucket_node", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "fork_num", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "block_num", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "file_path", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "check_time", TIMESTAMPTZOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "repair_time", TIMESTAMPTZOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        if (g_instance.repair_cxt.global_repair_bad_block_stat) {
            hash_seq = (HASH_SEQ_STATUS*)palloc0(sizeof(HASH_SEQ_STATUS));
            hash_seq_init(hash_seq, g_instance.repair_cxt.global_repair_bad_block_stat);
        } else {
            (void)MemoryContextSwitchTo(oldcontext);
            LWLockRelease(RepairBadBlockStatHashLock);
            SRF_RETURN_DONE(funcctx);
        }

        funcctx->user_fctx = (void*)hash_seq;

        (void)MemoryContextSwitchTo(oldcontext);
    }
    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->user_fctx != NULL) {
        hash_seq = (HASH_SEQ_STATUS*)funcctx->user_fctx;
        BadBlockEntry* badblock_entry = (BadBlockEntry*)hash_seq_search(hash_seq);

        if (badblock_entry != NULL) {
            Datum values[BAD_BLOCK_STAT_NATTS];
            bool nulls[BAD_BLOCK_STAT_NATTS];
            HeapTuple tuple = NULL;

            errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
            securec_check(rc, "\0", "\0");

            int i = 0;

            values[i++] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
            values[i++] = UInt32GetDatum(badblock_entry->key.relfilenode.spcNode);
            values[i++] = UInt32GetDatum(badblock_entry->key.relfilenode.dbNode);
            values[i++] = UInt32GetDatum(badblock_entry->key.relfilenode.relNode);
            values[i++] = Int32GetDatum(badblock_entry->key.relfilenode.bucketNode);
            values[i++] = Int32GetDatum(badblock_entry->key.forknum);
            values[i++] = UInt32GetDatum(badblock_entry->key.blocknum);
            values[i++] = CStringGetTextDatum(badblock_entry->path);
            values[i++] = TimestampTzGetDatum(badblock_entry->check_time);
            if (badblock_entry->repair_time == -1) {
                nulls[i++] = true;
            } else {
                values[i++] = TimestampTzGetDatum(badblock_entry->repair_time);
            }

            tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

            LWLockRelease(RepairBadBlockStatHashLock);
            SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
        } else {
            LWLockRelease(RepairBadBlockStatHashLock);
            SRF_RETURN_DONE(funcctx);
        }
    } else {
        LWLockRelease(RepairBadBlockStatHashLock);
        SRF_RETURN_DONE(funcctx);
    }
}

Datum local_clear_bad_block_info(PG_FUNCTION_ARGS)
{
    checkUserPermission();
    HASH_SEQ_STATUS hash_seq;
    BadBlockEntry* tempEntry = NULL;
    bool found = false;
    uint32 no_repair_num = 0;

    LWLockAcquire(RepairBadBlockStatHashLock, LW_EXCLUSIVE);

    if (g_instance.repair_cxt.global_repair_bad_block_stat) {
        hash_seq_init(&hash_seq, g_instance.repair_cxt.global_repair_bad_block_stat);
    } else {
        LWLockRelease(RepairBadBlockStatHashLock);
        PG_RETURN_BOOL(false);
    }

    while ((tempEntry = (BadBlockEntry*)hash_seq_search(&hash_seq)) != NULL) {
        if (tempEntry->repair_time != -1) {
            hash_search(g_instance.repair_cxt.global_repair_bad_block_stat, tempEntry, HASH_REMOVE, &found);
        } else {
            no_repair_num++;
        }
    }

    LWLockRelease(RepairBadBlockStatHashLock);

    PG_RETURN_BOOL(true);
}

Datum gs_repair_page(PG_FUNCTION_ARGS)
{
    if (ENABLE_DMS) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Not support repair page while DMS and DSS enabled")));
    }

    checkInstanceType();
    checkSupUserOrOperaMode();
    // read in parameters
    char* path = text_to_cstring(PG_GETARG_TEXT_P(0));
    uint32 blockNum = PG_GETARG_UINT32(1);
    bool is_segment = PG_GETARG_BOOL(2);
    int32 timeout = PG_GETARG_INT32(3);

    bool result = repairPage(path, blockNum, is_segment, timeout);
    PG_RETURN_BOOL(result);
}

/* check whether the input path is a legal path */
bool CheckRelDataFilePath(const char* path)
{
    const char *danger_character_list[] = {"|", ";", "&", "$", "<", ">", "`", "\\", "'", "\"", "{",
                                           "}", "(", ")", "[", "]", "~", "*", "?",  "!", "\n", " ", NULL};
    for (int i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr(path, danger_character_list[i]) != NULL) {
            return false;
        }
    }
    
    return true;
}

Datum gs_repair_file(PG_FUNCTION_ARGS)
{
    if (ENABLE_DMS) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Not support repair file while DMS and DSS enabled")));
    }
    checkInstanceType();
    checkSupUserOrOperaMode();
    Oid tableOid = PG_GETARG_UINT32(0);
    char* path = text_to_cstring(PG_GETARG_TEXT_P(1));
    int32 timeout = PG_GETARG_INT32(2);

    if (!CheckRelDataFilePath(path)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            (errmsg("The input path(%s) is an incorrect relation file path input. \n", path))));
        return false;
    }

    if (timeout < TIMEOUT_MIN || timeout > TIMEOUT_MAX) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (errmsg("The timeout(%d) is an incorrect input. Value range: [60, 3600]. \n", timeout))));
        return false;
    }
    t_thrd.storage_cxt.timeoutRemoteOpera = timeout;

    bool result = gsRepairFile(tableOid, path, timeout);
    PG_RETURN_BOOL(result);
}

void gs_verify_page_by_disk(SMgrRelation smgr, ForkNumber forkNum, int blockNum, char* disk_page_res)
{
    char* buffer = (char*)palloc0(BLCKSZ);
    errno_t rc = 0;
    SMGR_READ_STATUS rdStatus = smgrread(smgr, forkNum, blockNum, buffer);
    if (rdStatus == SMGR_RD_CRC_ERROR) {
        uint16 checksum = pg_checksum_page((char*)buffer, blockNum);
        PageHeader pghr = (PageHeader)buffer;
        rc = snprintf_s(disk_page_res, ERR_MSG_LEN, ERR_MSG_LEN - 1,
                        "page verification failed, calculated checksum %hu but expected %hu.",
                        checksum, pghr->pd_checksum);
        securec_check_ss(rc, "\0", "\0");
        addGlobalRepairBadBlockStat(smgr->smgr_rnode, forkNum, blockNum);
    } else if (rdStatus == SMGR_RD_NO_BLOCK) {
        rc = snprintf_s(disk_page_res, ERR_MSG_LEN, ERR_MSG_LEN - 1,
                        "The page does not exist.");
        securec_check_ss(rc, "\0", "\0");
    } else if (rdStatus == SMGR_RD_OK) {
        rc = snprintf_s(disk_page_res, ERR_MSG_LEN, ERR_MSG_LEN - 1,
                        "page verification succeeded.");
        securec_check_ss(rc, "\0", "\0");
    } else {
        rc = snprintf_s(disk_page_res, ERR_MSG_LEN, ERR_MSG_LEN - 1,
                        "Unrecognized Error.");
        securec_check_ss(rc, "\0", "\0");
    }
    pfree(buffer);
}

void splicMemPageMsg(bool isPageValid, bool isDirty, char* mem_page_res)
{
    errno_t rc = 0;
    if (!isPageValid) {
        rc = snprintf_s(mem_page_res, ERR_MSG_LEN, ERR_MSG_LEN - 1,
                        "page in memory, page verification failed, calculated checksum is error.");
        securec_check_ss(rc, "\0", "\0");
    } else if (isDirty) {
        rc = snprintf_s(mem_page_res, ERR_MSG_LEN, ERR_MSG_LEN - 1,
                        "page is dirty, page verification succeeded.");
        securec_check_ss(rc, "\0", "\0");
    } else {
        rc = snprintf_s(mem_page_res, ERR_MSG_LEN, ERR_MSG_LEN - 1,
                        "page is not dirty, page verification succeeded.");
        securec_check_ss(rc, "\0", "\0");
    }
}

bool isNeedRepairPageByMem(char* disk_page_res, BlockNumber blockNum, char* mem_page_res,
    bool isSegment, RelFileNode relnode)
{
    bool found = true;
    bool need_repair = false;
    bool isDirty = false;
    bool isPageValid = false;
    BufferDesc* buf_desc = NULL;
    char* buffer = (char*)palloc0(BLCKSZ);
    bool is_repair = false;
    errno_t rc = 0;
    SMGR_READ_STATUS rdStatus = SMGR_RD_CRC_ERROR;
    SegSpace *spc = NULL;

    SMgrRelation smgr = smgropen(relnode, InvalidBackendId, GetColumnNum(MAIN_FORKNUM));
    /* check memory buffer */
    Buffer buf = PageIsInMemory(smgr, MAIN_FORKNUM, blockNum);
    if (BufferIsInvalid(buf)) {
        found = false;
        rc = snprintf_s(mem_page_res, ERR_MSG_LEN, ERR_MSG_LEN - 1, "page not in memory");
        securec_check_ss(rc, "\0", "\0");
    } else {
        buf_desc = GetBufferDescriptor(buf - 1);
        uint64 old_buf_state = LockBufHdr(buf_desc);
        isDirty = old_buf_state & BM_DIRTY;
        UnlockBufHdr(buf_desc, old_buf_state);
        Page page = BufferGetPage(buf);

        isPageValid = (PageGetPageLayoutVersion(page) == PG_UHEAP_PAGE_LAYOUT_VERSION) ?
                       UPageHeaderIsValid((UHeapPageHeaderData *) page) :
                       PageHeaderIsValid((PageHeader)page);
        splicMemPageMsg(isPageValid, isDirty, mem_page_res);
    }

    if (IsSegmentPhysicalRelNode(relnode)) {
        spc = spc_open(relnode.spcNode, relnode.dbNode, false, false);
        seg_physical_read(spc, relnode, MAIN_FORKNUM, blockNum, buffer);
        if (PageIsVerified(buffer, blockNum)) {
            rdStatus = SMGR_RD_OK;
        } else {
            rdStatus = SMGR_RD_CRC_ERROR;
        }
    } else {
        rdStatus = smgrread(smgr, MAIN_FORKNUM, blockNum, buffer);
    }

    if (rdStatus == SMGR_RD_OK) {
        rc = snprintf_s(disk_page_res, ERR_MSG_LEN, ERR_MSG_LEN - 1, "page verification succeeded.");
        securec_check_ss(rc, "\0", "\0");
    } else {
        rc = snprintf_s(disk_page_res, ERR_MSG_LEN, ERR_MSG_LEN - 1,
                        "page verification failed, calculated checksum is error.");
        securec_check_ss(rc, "\0", "\0");
        need_repair = true;
    }

    if (!found && need_repair) {
        if (isSegment) {
            const int TIMEOUT = 1200;
            is_repair = tryRepairPage(blockNum, true, &relnode, TIMEOUT);
        } else {
            buf = ReadBufferWithoutRelcache(relnode, MAIN_FORKNUM, blockNum, RBM_NORMAL, NULL, NULL);
            is_repair = true;
            UpdateRepairTime(relnode, MAIN_FORKNUM, blockNum);
        }
    } else if (found && need_repair && isPageValid) {
        buf_desc = GetBufferDescriptor(buf - 1);
        if (!isDirty) {
            LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
            MarkBufferDirty(buf);
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        }
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        FlushBuffer(buf_desc, NULL, WITH_NORMAL_CACHE, true);
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);
        is_repair = true;
        UpdateRepairTime(relnode, MAIN_FORKNUM, blockNum);
    }

    if (!BufferIsInvalid(buf)) {
        buf_desc = GetBufferDescriptor(buf - 1);
        UnpinBuffer(buf_desc, true);
    }
    pfree(buffer);
    return is_repair;
}
    
/* the extent is in compression relation,  */
void gs_tryrepair_compress_extent(SMgrRelation reln, BlockNumber logicBlockNumber)
{
    errno_t rc = 0;
    ExtentLocation location = cfsLocationConverts[COMMON_STORAGE](reln, MAIN_FORKNUM, logicBlockNumber, true,
                                                                  EXTENT_OPEN_FILE);

    char path[MAX_PATH];
    rc = sprintf_s(path, MAX_PATH, "[RelFileNode:%u/%u/%u], extentNumber:%d, extentStart:%d,"
                  "extentOffset:%d, headerNum:%d, chunk_size:%d",
        location.relFileNode.spcNode, location.relFileNode.dbNode, location.relFileNode.relNode,
        (int)location.extentNumber, (int)location.extentStart, (int)location.extentOffset,
        (int)location.headerNum, (int)location.chrunk_size);
    securec_check_ss(rc, "", "");

    RemoteReadFileKey repairFileKey;
    repairFileKey.relfilenode = reln->smgr_rnode.node;
    repairFileKey.forknum = MAIN_FORKNUM;

    /* read the remote size */
    int64 size = RemoteReadFileSize(&repairFileKey, TIMEOUT_MIN);
    if (size == -1) {
        ereport(WARNING,
            (errmsg("The file does not exist on the standby DN, don't need repair, path is %s", path)));
        return;
    }
    if (size == 0) {
        ereport(WARNING, (errmsg("standby size is zero, path is %s", path)));
        return;
    }

    BlockNumber total = (BlockNumber)(size / BLCKSZ);
    if (total <= logicBlockNumber) {
        ereport(WARNING, (errmsg("Invalid Block Number, logicBlockNumber is %u, max number is %u.",
                                 logicBlockNumber, total)));
        return;
    }
    uint32 diff = (total - location.extentNumber * CFS_LOGIC_BLOCKS_PER_EXTENT);
    uint32 read_size = (diff > CFS_LOGIC_BLOCKS_PER_EXTENT ? CFS_LOGIC_BLOCKS_PER_EXTENT : diff) * BLCKSZ;
    repairFileKey.blockstart = location.extentNumber * CFS_LOGIC_BLOCKS_PER_EXTENT;
    uint32 remote_size = 0;
    char *buf = (char*)palloc0((Size)read_size);

    /* read many pages from remote */
    RemoteReadFile(&repairFileKey, buf, read_size, TIMEOUT_MIN, &remote_size);

    /* write into extent */
    rc = WriteRepairFile_Compress_extent(reln, logicBlockNumber, path, buf, location.extentStart * BLCKSZ,
                                         (uint32)read_size / BLCKSZ);
    if (rc != 0) {
        pfree(buf);
        ereport(ERROR, (errmsg("repair the whole extent failed, the information is %s", path)));
    }

    pfree(buf);
}

Datum gs_verify_and_tryrepair_page(PG_FUNCTION_ARGS)
{
    if (ENABLE_DMS) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Not support verify and tryrepair page while DMS and DSS enabled")));
    }
#define REPAIR_BLOCK_STAT_NATTS 6
    checkInstanceType();
    checkSupUserOrOperaMode();
    /* read in parameters */
    char* path = text_to_cstring(PG_GETARG_TEXT_P(0));
    uint32 blockNum = PG_GETARG_UINT32(1);
    bool verify_mem = PG_GETARG_BOOL(2);
    bool is_segment = PG_GETARG_BOOL(3);
    errno_t rc = 0;
    TupleDesc tupdesc = NULL;
    Datum values[REPAIR_BLOCK_STAT_NATTS];
    bool nulls[REPAIR_BLOCK_STAT_NATTS] = {false};
    HeapTuple tuple = NULL;
    int i = 0;
    char* disk_page_res = (char*)palloc0(ERR_MSG_LEN);
    char* mem_page_res = (char*)palloc0(ERR_MSG_LEN);
    char* pca_page_res = (char*)palloc0(ERR_MSG_LEN);
    bool is_repair = false;
    int j = 1;
  
    /* build tupdesc for result tuples */
    tupdesc = CreateTemplateTupleDesc(REPAIR_BLOCK_STAT_NATTS, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "node_name", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "path", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "blocknum", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "disk_page_res", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "mem_page_res", TEXTOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)j++, "is_repair", BOOLOID, -1, 0);
    tupdesc = BlessTupleDesc(tupdesc);

    rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    securec_check(rc, "\0", "\0");

    /* check parameters */
    if (blockNum > MaxBlockNumber)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (errmsg("Blocknum should be between 0 and %u. \n", MaxBlockNumber))));

    RelFileNode relnode = {0, 0, 0, -1, 0};
    PrepForRead((char*)path, blockNum, is_segment, &relnode);

    /* if it is compressed relation, fill the opt in RelFileNode */
    if (!IsSegmentPhysicalRelNode(relnode)) {
        if (!getCompressedRelOpt(&relnode)) {
            relnode.opt = 0;
        }
    }

    SMgrRelation smgr = smgropen(relnode, InvalidBackendId, GetColumnNum(MAIN_FORKNUM));
    
    /* we need check the pca header page first if the relation is compression relation,
       and we need to try our best to repair this pca header page and the whole extent */
    if (IS_COMPRESSED_RNODE(relnode, MAIN_FORKNUM)) {
        bool need_repair_pca = false;
        if (!IsSegmentPhysicalRelNode(relnode)) {
            CfsHeaderPageCheckAndRepair(smgr, blockNum, pca_page_res, ERR_MSG_LEN, &need_repair_pca);
            if (need_repair_pca) {
                /* clear local cache and smgr */
                CacheInvalidateSmgr(smgr->smgr_rnode);

                /* try repair the whole extent */
                gs_tryrepair_compress_extent(smgr, blockNum);
            }
        } else {
            ;
        }

        if (pca_page_res != NULL) {
            ereport(NOTICE, (errmsg("The relative pca page check result is %s.", pca_page_res)));
        }
    }

    /* the single page check or repair */
    if (!verify_mem && !IsSegmentPhysicalRelNode(relnode)) { // only check disk
        gs_verify_page_by_disk(smgr, MAIN_FORKNUM, blockNum, disk_page_res);
    } else {
        BlockNumber logicBlockNum = blockNum;
        if (IsSegmentPhysicalRelNode(relnode)) {
            repair_check_physical_type(relnode.spcNode, relnode.dbNode, MAIN_FORKNUM, &(relnode.relNode),
                                       &logicBlockNum);
        }
        is_repair = isNeedRepairPageByMem(disk_page_res, logicBlockNum, mem_page_res, is_segment, relnode);
    }

    values[i++] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
    values[i++] = CStringGetTextDatum(path);
    values[i++] = UInt32GetDatum(blockNum);
    values[i++] = CStringGetTextDatum(disk_page_res);
    if (verify_mem) {
        values[i++] = CStringGetTextDatum(mem_page_res);
    } else {
        nulls[i++] = true;   /* memory res is null */
    }

    values[i++] = BoolGetDatum(is_repair);

    tuple = heap_form_tuple(tupdesc, values, nulls);
    pfree(disk_page_res);
    pfree(mem_page_res);
    pfree(pca_page_res);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * Read block from buffer from primary, returning it as bytea
 */
Datum gs_read_segment_block_from_remote(PG_FUNCTION_ARGS)
{
    if (GetUserId() != BOOTSTRAP_SUPERUSERID) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("must be initial account to read files"))));
    }
    bytea* result = NULL;

    /* handle optional arguments */
    uint32 spcNode = PG_GETARG_UINT32(0);
    uint32 dbNode = PG_GETARG_UINT32(1);
    uint32 relNode = PG_GETARG_UINT32(2);
    int16 bucketNode = PG_GETARG_INT16(3);
    int32 forkNum = PG_GETARG_INT32(4);
    uint64 blockNum = (uint64)PG_GETARG_TRANSACTIONID(5);
    uint32 blockSize = PG_GETARG_UINT32(6);
    uint64 lsn = (uint64)PG_GETARG_TRANSACTIONID(7);
    uint32 seg_relNode = PG_GETARG_UINT32(8);
    uint32 seg_block = PG_GETARG_UINT32(9);
    int32 timeout = PG_GETARG_INT32(10);

    XLogPhyBlock pblk = {
        .relNode = seg_relNode,
        .block = seg_block,
        .lsn = InvalidXLogRecPtr
    };

    RepairBlockKey key;
    key.relfilenode.spcNode = spcNode;
    key.relfilenode.dbNode = dbNode;
    key.relfilenode.relNode = relNode;
    key.relfilenode.bucketNode = bucketNode;
    key.relfilenode.opt = 0;
    key.forknum = forkNum;
    key.blocknum = blockNum;

    (void)StandbyReadPageforPrimary(key, blockSize, lsn, &result, timeout, &pblk);

    if (NULL != result) {
        PG_RETURN_BYTEA_P(result);
    } else {
        PG_RETURN_NULL();
    }
}

Datum gs_verify_data_file(PG_FUNCTION_ARGS)
{
    checkSupUserOrOperaMode();
#define VERIFY_DATA_FILE_NATTS 4
    // read in parameters
    FuncCallContext* funcctx = NULL;
    bool is_segment = PG_GETARG_BOOL(0);
    List *badFileItems = NIL;
    bool isNull = false;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc = NULL;
        HeapTuple classTup;
        MemoryContext oldcontext = NULL;
        int i = 1;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();
        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        /* build tupdesc for result tuples */
        tupdesc = CreateTemplateTupleDesc(VERIFY_DATA_FILE_NATTS, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "node_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "rel_oid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "rel_name", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)i++, "miss_file_path", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        Relation relation = heap_open(RelationRelationId, AccessShareLock);
        SysScanDesc scan  = systable_beginscan(relation, InvalidOid, false, NULL, 0, NULL);
        List *spcList = NIL;
        while ((classTup = systable_getnext(scan)) != NULL) {
            Form_pg_class classForm = (Form_pg_class)GETSTRUCT(classTup);
            Datum bucketdatum = tableam_tops_tuple_getattr(classTup, Anum_pg_class_relbucket,
                                                           RelationGetDescr(relation), &isNull);
            Oid bucketOid = ObjectIdGetDatum(bucketdatum);
            if ((classForm->relkind != RELKIND_RELATION &&
                 classForm->relkind != RELKIND_TOASTVALUE) ||
                (classForm->relpersistence != RELPERSISTENCE_PERMANENT &&
                 classForm->relpersistence != RELPERSISTENCE_UNLOGGED) ||
                ((bucketOid <= 0) && is_segment) ||
                ((bucketOid > 0) && !is_segment)) {
                continue;
            }

            Oid relOid = 0;
            Datum oiddatum = tableam_tops_tuple_getattr(classTup, ObjectIdAttributeNumber,
                                                        RelationGetDescr(relation), &isNull);
            relOid = ObjectIdGetDatum(oiddatum);
            Relation tableRel = heap_open(relOid, AccessShareLock);

            if (is_segment) {
                spcList = appendIfNot(spcList, ConvertToRelfilenodeTblspcOid(classForm->reltablespace));
            } else {
                badFileItems = getNonSegmentBadFiles(badFileItems, relOid, classForm, tableRel);
            }
            heap_close(tableRel, AccessShareLock);
        }
        systable_endscan(scan);
        heap_close(relation, AccessShareLock);
        if (spcList != NIL) {
            badFileItems = getSegmentBadFiles(spcList, badFileItems);
        }
        funcctx->user_fctx = (void*)badFileItems;
        (void)MemoryContextSwitchTo(oldcontext);
    }
    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();
    if (funcctx->user_fctx != NULL) {
        badFileItems = (List*)funcctx->user_fctx;
        ListCell* badFileItemCell = list_head(badFileItems);
        BadFileItem* badFileItem = (BadFileItem*)lfirst(badFileItemCell);
        Datum values[VERIFY_DATA_FILE_NATTS];
        bool nulls[VERIFY_DATA_FILE_NATTS] = {false};
        HeapTuple tuple = NULL;
        int i = 0;
        errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        values[i++] = CStringGetTextDatum(g_instance.attr.attr_common.PGXCNodeName);
        values[i++] = UInt32GetDatum(badFileItem->reloid);
        values[i++] = CStringGetTextDatum(badFileItem->relname.data);
        values[i++] = CStringGetTextDatum(badFileItem->relfilepath);

        badFileItems = list_delete_first(badFileItems);
        funcctx->user_fctx = (void*)badFileItems;
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }
    SRF_RETURN_DONE(funcctx);
}

List* getNonSegmentBadFiles(List* badFileItems, Oid relOid, Form_pg_class classForm, Relation tableRel)
{
    if (classForm->parttype == PARTTYPE_PARTITIONED_RELATION ||
            classForm->parttype == PARTTYPE_SUBPARTITIONED_RELATION) {
        badFileItems = getPartitionBadFiles(tableRel, badFileItems, relOid);
    } else {
        badFileItems = getTableBadFiles(badFileItems, relOid, classForm, tableRel);
    }
    return badFileItems;
}

List* getPartitionBadFiles(Relation tableRel, List* badFileItems, Oid relOid)
{
    HeapTuple partitionTup;
    Relation prelation = heap_open(PartitionRelationId, AccessShareLock);
    SysScanDesc pscan  = systable_beginscan(prelation, InvalidOid, false, NULL, 0, NULL);
    while ((partitionTup = systable_getnext(pscan)) != NULL) {
        Form_pg_partition partitionForm = (Form_pg_partition)GETSTRUCT(partitionTup);
        RelFileNode prnode = {0};
        Oid prelOid = 0;
        bool isNull = false;
        int maxSegno = 0;

        Datum poiddatum = tableam_tops_tuple_getattr(partitionTup, ObjectIdAttributeNumber,
                                                     RelationGetDescr(prelation), &isNull);
        prelOid = ObjectIdGetDatum(poiddatum);

        if (partitionForm->relfilenode == 0 && partitionForm->parentid == relOid &&
                partitionForm->parttype == PARTTYPE_PARTITIONED_RELATION) {
            badFileItems =  getPartitionBadFiles(tableRel, badFileItems, prelOid);
            continue;
        } else if (partitionForm->relfilenode == 0 || partitionForm->parentid != relOid) {
            continue;
        }

        Partition ptmpRel = partitionOpen(tableRel, prelOid, AccessShareLock, InvalidBktId);
        prnode = ptmpRel->pd_node;
        partitionClose(tableRel, ptmpRel, AccessShareLock);

        char* path = relpathperm(prnode, MAIN_FORKNUM);
        char* openFilePath = path;
        char dst[MAXPGPATH];
        if (IS_COMPRESSED_RNODE(prnode, MAIN_FORKNUM)) {
            CopyCompressedPath(dst, path);
            openFilePath = dst;
        }

        struct stat statBuf;
        if (stat(openFilePath, &statBuf) < 0) {
            badFileItems = appendBadFileItems(badFileItems, prelOid, partitionForm->relname.data, openFilePath);
        }

        maxSegno = getMaxSegno(&prnode);
        if (maxSegno != 0) {
            badFileItems = getSegnoBadFiles(path, maxSegno, prelOid, partitionForm->relname.data, badFileItems,
                                            IS_COMPRESSED_RNODE(prnode, MAIN_FORKNUM));
        }
        pfree(path);
    }
    systable_endscan(pscan);
    heap_close(prelation, AccessShareLock);
    return badFileItems;
}

int getMaxSegno(RelFileNode* prnode)
{
    const int POINT_LEN = 2;
    int maxSegno = 0;
    errno_t rc = 0;
    char* oidStr;
    char* dirPath = relSegmentDir(*prnode, MAIN_FORKNUM);
    DIR* pdir = NULL;
    struct dirent* ent = NULL;

    oidStr = (char*)palloc0(getIntLength(prnode->relNode) + POINT_LEN);
    rc = snprintf_s(oidStr, getIntLength(prnode->relNode) + POINT_LEN,
                    getIntLength(prnode->relNode) + POINT_LEN - 1, "%u.", prnode->relNode);
    securec_check_ss(rc, "\0", "\0");

    pdir = opendir(dirPath);
    if (NULL != pdir) {
        while (NULL != (ent = readdir(pdir))) {
            /* skip . and .. */
            if (0 == strcmp(ent->d_name, ".") || 0 == strcmp(ent->d_name, "..") ||
                0 != strncmp(ent->d_name, oidStr, getIntLength(prnode->relNode) + 1)) {
                continue;
            }
            int segno = 0;
            Oid relNode = 0;
            int nRet = 0;
            nRet = sscanf_s(ent->d_name, "%u.%d", &relNode, &segno);
            securec_check_ss_c(nRet, "", "");

            if (segno > maxSegno) {
                maxSegno = segno;
            }
        }
        (void)closedir(pdir);
        pdir = NULL;
    }
    pfree(oidStr);

    return maxSegno;
}

List* getTableBadFiles(List* badFileItems, Oid relOid, Form_pg_class classForm, Relation tableRel)
{
    int maxSegno = 0;
    RelFileNode rnode = tableRel->rd_node;
    char* path = relpathperm(rnode, MAIN_FORKNUM);
    char* openFilePath = path;
    char dst[MAXPGPATH];
    if (IS_COMPRESSED_RNODE(rnode, MAIN_FORKNUM)) {
        CopyCompressedPath(dst, path);
        openFilePath = dst;
    }
    
    struct stat statBuf;
    if (stat(openFilePath, &statBuf) < 0) {
        /* Skip to appendBadFileItems where pmState in archive recovery mode or hot standby mode */
        if((pmState != PM_RECOVERY && pmState != PM_HOT_STANDBY)
            || (classForm->relpersistence != RELPERSISTENCE_UNLOGGED) || g_instance.attr.attr_storage.xlog_file_path != NULL) {
            badFileItems = appendBadFileItems(badFileItems, relOid, classForm->relname.data, openFilePath);
        }
    }

    if (classForm->relpersistence == RELPERSISTENCE_UNLOGGED) {
        char* initPath = relpathperm(rnode, INIT_FORKNUM);
        if (stat(initPath, &statBuf) < 0) {
            badFileItems = appendBadFileItems(badFileItems, relOid, classForm->relname.data, initPath);
        }
        pfree(initPath);
    }

    maxSegno = getMaxSegno(&rnode);

    if (maxSegno != 0) {
        badFileItems = getSegnoBadFiles(path, maxSegno, relOid, classForm->relname.data, badFileItems,
                                        IS_COMPRESSED_RNODE(rnode, MAIN_FORKNUM));
    }
    pfree(path);
    return badFileItems;
}

List* getSegmentBadFiles(List* spcList, List* badFileItems)
{
    ListCell *currentCell = NULL;
    struct stat statBuf;
    foreach (currentCell, spcList) {
        RelFileNode relFileNode = {
            .spcNode = lfirst_oid(currentCell),
            .dbNode = u_sess->proc_cxt.MyDatabaseId,
            .relNode = 1,
            .bucketNode = SegmentBktId,
            .opt = 0
        };
        char* segmentDir = relSegmentDir(relFileNode, MAIN_FORKNUM);
        List* segmentFiles = getSegmentMainFilesPath(segmentDir, '/', 5, false);
        ListCell *currentCell = NULL;
        foreach(currentCell, segmentFiles) {
            if (stat((char*)lfirst(currentCell), &statBuf) < 0) {
                badFileItems = appendBadFileItems(badFileItems, 0, "none", (char*)lfirst(currentCell));
            } else {
                uint32 highWater =  getSegmentFileHighWater((char*)lfirst(currentCell));
                int fileNum = highWater / (REGR_MCR_SIZE_1GB / BLCKSZ) + 1;
                badFileItems = getSegnoBadFiles((char*)lfirst(currentCell), fileNum - 1, 0, "none",
                                                badFileItems, false);
            }
        }
    }
    return badFileItems;
}

List* getSegnoBadFiles(char* path, int maxSegno, Oid relOid, char* tabName, List* badFileItems, bool isCompressed)
{
    if (maxSegno < 1) {
        return badFileItems;
    }
    struct stat statBuf;
    List* segmentFiles = getSegmentMainFilesPath(path, '.', maxSegno, isCompressed);
    ListCell *currentCell = NULL;
    foreach(currentCell, segmentFiles) {
        if (stat((char*)lfirst(currentCell), &statBuf) < 0) {
            badFileItems = appendBadFileItems(badFileItems, relOid, tabName, (char*)lfirst(currentCell));
        }
    }
    return badFileItems;
}

List* appendBadFileItems(List* badFileItems, Oid relOid, char* tabName, char* path)
{
    errno_t rc = 0;
    BadFileItem *badFileItem = (BadFileItem*)palloc0(sizeof(BadFileItem));

    badFileItem->reloid = relOid;
    rc = snprintf_s(badFileItem->relname.data, NAMEDATALEN,
                    NAMEDATALEN - 1, "%s", tabName);
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(badFileItem->relfilepath, MAX_PATH,
                    MAX_PATH - 1, "%s", path);
    securec_check_ss(rc, "\0", "\0");
    badFileItems = lappend(badFileItems, badFileItem);

    return badFileItems;
}

static int containsNums(const char *str, const char chr)
{
    int count = 0;
    int i = 0;
    while (*(str + i)) {
        if (str[i] == chr) {
            ++count;
        }
        ++i;
    }
    return count;
}

char* relSegmentDir(RelFileNode rnode, ForkNumber forknum)
{
    if (forknum != MAIN_FORKNUM) {
        return NULL;
    }
    char* path = NULL;
    char* pathDir = (char*)palloc0(MAX_PATH);
    int times = 0;
    char *token = NULL;
    char *tmptoken = NULL;

    path = relpathperm(rnode, forknum);
    times = containsNums(path, '/');
    token = strtok_r(path, "/", &tmptoken);
    int index = 0;
    while (token != NULL) {
        errno_t rc = 0;
        if (index == 0) {
            rc = snprintf_s(pathDir, MAX_PATH, MAX_PATH - 1, "%s", token);
        } else {
            rc = snprintf_s(pathDir, MAX_PATH, MAX_PATH - 1, "%s/%s", pathDir, token);
        }
        securec_check_ss(rc, "\0", "\0");
        token = strtok_r(NULL, "/", &tmptoken);
        if (times == ++index) {
            break;
        }
    }
    pfree(path);
    return pathDir;
}

List* getSegmentMainFilesPath(char* segmentDir, char split, int num, bool isCompressed)
{
    if (segmentDir == NULL) {
        return NULL;
    }

    List* segmentMainFilesPath = NIL;

    for (int i = 1; i <= num; i++) {
        char* path = NULL;
        int pathlen = strlen(segmentDir) + getIntLength(num) + 2;
        path = (char*)palloc0(pathlen);
        int rc = snprintf_s(path, pathlen, pathlen - 1, "%s%c%d", segmentDir, split, i);
        securec_check_ss(rc, "\0", "\0");

        char* openFilePath = path;
        if (isCompressed) {
            char* dst = (char*)palloc0(MAXPGPATH);
            CopyCompressedPath(dst, path);
            pfree(path);
            openFilePath = dst;
        }

        segmentMainFilesPath = lappend(segmentMainFilesPath, openFilePath);
    }
    return segmentMainFilesPath;
}

List* appendIfNot(List* targetList, Oid datum)
{
    bool found = false;
    ListCell *currentcell = NULL;

    foreach (currentcell, targetList) {
        if (lfirst_oid(currentcell) == datum) {
            found = true;
            break;
        }
    }

    if (!found) {
        targetList = lappend_oid(targetList, datum);
    }

    return targetList;
}

uint32 getSegmentFileHighWater(char* path)
{
    uint32 flags = O_RDWR | PG_BINARY;
    int fd = -1;
    char *buffer = (char*)palloc0(BLCKSZ);
    uint32 result = 0;
    st_df_map_head* head;
    fd = BasicOpenFile(path, flags, S_IWUSR | S_IRUSR);
    if (fd < 0) {
        pfree(buffer);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", path)));
    }
    off_t offset = ((off_t)DF_MAP_HEAD_PAGE) * BLCKSZ;
    pgstat_report_waitevent(WAIT_EVENT_DATA_FILE_READ);
    int nbytes = pread(fd, buffer, BLCKSZ, offset);
    pgstat_report_waitevent(WAIT_EVENT_END);

    if (close(fd)) {
        pfree(buffer);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not close file \"%s\": %m", path)));
    }

    if (nbytes != BLCKSZ) {
        pfree(buffer);
        ereport(ERROR,
                (errcode(MOD_SEGMENT_PAGE),
                        errcode_for_file_access(),
                        errmsg("could not read segment block %d in file %s", DF_MAP_HEAD_PAGE, path),
                        errdetail("errno: %d", errno)));
        result = 0;
    } else {
        head = (st_df_map_head*)PageGetContents(buffer);
        result = head->high_water_mark;
    }
    pfree(buffer);
    return result;
}

int getIntLength(uint32 intValue)
{
    int length = 1;
    const int BITSMAX = 9;
    const int TEN = 10;

    while (intValue > BITSMAX) {
        length++;
        intValue /= TEN;
    }
    return length;
}

static bool PrimaryRepairSegFile_Segment(RemoteReadFileKey *repairFileKey, char* path, int32 seg_no, int32 maxSegno,
    int timeout, int64 size)
{
    struct stat statBuf;
    errno_t rc;
    char* buf = NULL;
    char *segpath = (char *)palloc0(strlen(path) + SEGLEN);
    uint32 seg_size  = ((seg_no < maxSegno || (size % (RELSEG_SIZE * BLCKSZ)) == 0) ?
                        (RELSEG_SIZE * BLCKSZ) : (size % (RELSEG_SIZE * BLCKSZ)));

    if (seg_no == 0) {
        rc = sprintf_s(segpath, strlen(path) + SEGLEN, "%s", path);
    } else {
        rc = sprintf_s(segpath, strlen(path) + SEGLEN, "%s.%d", path, seg_no);
    }
    securec_check_ss(rc, "", "");

    if (stat(segpath, &statBuf) < 0) {
        /* ENOENT is expected after the last segment... */
        if (errno != ENOENT) {
            ereport(WARNING, (errcode_for_file_access(),
                    errmsg("could not stat file \"%s\" before repair: %m", segpath)));
            pfree(segpath);
            return false;
        }

        RelFileNodeBackend rnode;
        rnode.node = repairFileKey->relfilenode;
        rnode.backend = InvalidBackendId;
        RelFileNodeForkNum fileNode;
        fileNode.rnode = rnode;
        fileNode.forknumber = repairFileKey->forknum;
        fileNode.segno = (BlockNumber)seg_no;
        fileNode.storage = ROW_STORE;
        if (!repair_deleted_file_check(fileNode, -1)) {
            ereport(WARNING, (errcode_for_file_access(),
                    errmsg("could not repair file \"%s\" before deleted not closed: ", segpath)));
            pfree(segpath);
            return false;
        }

        /* clear local cache and smgr */
        CacheInvalidateSmgr(rnode);

        /* create temp repair file */
        int fd = CreateRepairFile(segpath);
        if (fd < 0) {
            ereport(WARNING, (errcode_for_file_access(),
                    errmsg("could not create repair file \"%s\", segno is %d",
                           relpathperm(repairFileKey->relfilenode, repairFileKey->forknum), seg_no)));
            pfree(segpath);
            return false;
        }

        buf = (char*)palloc0(MAX_BATCH_READ_BLOCKNUM * BLCKSZ);
        int batch_size = MAX_BATCH_READ_BLOCKNUM * BLCKSZ;
        int max_times = (int)seg_size % batch_size == 0 ? (int)seg_size / batch_size : ((int)seg_size / batch_size + 1);

        for (int j = 0; j < max_times; j++) {
            uint32 read_size = 0;
            uint32 remote_size = 0;
            repairFileKey->blockstart = seg_no * RELSEG_SIZE + j * MAX_BATCH_READ_BLOCKNUM;
            if (seg_size % batch_size != 0) {
                read_size = (j == max_times - 1 ? seg_size % batch_size : batch_size);
            } else {
                read_size = (uint32)batch_size;
            }

            /* read many pages from remote */
            RemoteReadFile(repairFileKey, buf, read_size, timeout, &remote_size);

            /* write to local temp file */
            rc = WriteRepairFile(fd, segpath, buf, (uint32)(j * batch_size), read_size);
            if (rc != 0) {
                (void)close(fd);
                pfree(buf);
                pfree(segpath);
                ereport(WARNING, (errcode_for_file_access(),
                        errmsg("could not write repair file \"%s\", segno is %d",
                               relpathperm(repairFileKey->relfilenode, repairFileKey->forknum), seg_no)));
                return false;
            }
        }

        if (!repair_deleted_file_check(fileNode, fd)) {
            (void)close(fd);
        }
        pfree(buf);
        rc = CheckAndRenameFile(segpath);
        if (rc != 0) {
            pfree(segpath);
            ereport(WARNING, (errcode_for_file_access(),
                    errmsg("could not rename file \"%s\", segno is %d",
                           relpathperm(repairFileKey->relfilenode, repairFileKey->forknum), seg_no)));
            return false;
        }
        BatchUpdateRepairTime(repairFileKey->relfilenode, repairFileKey->forknum, (BlockNumber)seg_no);
    }
    pfree(segpath);
    return true;
}

static bool PrimaryRepairSegFile_NonSegment(const RelFileNode &rd_node, RemoteReadFileKey *repairFileKey, char* path,
                                            int32 seg_no, int32 maxSegno, int timeout, int64 size)
{
    struct stat statBuf;
    errno_t rc;
    char* buf = NULL;
    int64 segpathlen = strlen(path) + SEGLEN + strlen(COMPRESS_STR);
    char *segpath = (char *)palloc0((Size)segpathlen);
    BlockNumber relSegSize = IS_COMPRESSED_RNODE(rd_node, MAIN_FORKNUM) ? CFS_LOGIC_BLOCKS_PER_FILE: RELSEG_SIZE;
    uint32 seg_size  = (uint32)((seg_no < maxSegno || (size % (relSegSize * BLCKSZ)) == 0) ?
                        (relSegSize * BLCKSZ) : (size % (relSegSize * BLCKSZ)));

    if (seg_no == 0) {
        rc = sprintf_s(segpath, (uint64)segpathlen, "%s%s", path,
            IS_COMPRESSED_RNODE(rd_node, MAIN_FORKNUM) ? COMPRESS_STR : "");
    } else {
        rc = sprintf_s(segpath, (uint64)segpathlen, "%s.%d%s", path, seg_no,
            IS_COMPRESSED_RNODE(rd_node, MAIN_FORKNUM) ? COMPRESS_STR : "");
    }
    securec_check_ss(rc, "", "");

    if (stat(segpath, &statBuf) < 0) {
        /* ENOENT is expected after the last segment... */
        if (errno != ENOENT) {
            ereport(WARNING, (errcode_for_file_access(),
                    errmsg("could not stat file \"%s\" before repair: ", segpath)));
            pfree(segpath);
            return false;
        }

        RelFileNodeBackend rnode;
        rnode.node = repairFileKey->relfilenode;
        rnode.backend = InvalidBackendId;
        RelFileNodeForkNum fileNode;
        fileNode.rnode = rnode;
        fileNode.forknumber = repairFileKey->forknum;
        fileNode.segno = seg_no;
        fileNode.storage = ROW_STORE;
        if (!repair_deleted_file_check(fileNode, -1)) {
            ereport(WARNING, (errcode_for_file_access(),
                    errmsg("could not repair file \"%s\" before deleted not closed: %m", segpath)));
            pfree(segpath);
            return false;
        }

        /* clear local cache and smgr */
        CacheInvalidateSmgr(rnode);

        /* create temp repair file */
        int fd = CreateRepairFile(segpath);
        if (fd < 0) {
            ereport(WARNING, (errcode_for_file_access(),
                              errmsg("could not create repair file \"%s\", segno is %d",
                                     relpathperm(repairFileKey->relfilenode, repairFileKey->forknum),
                                     seg_no)));
            pfree(segpath);
            return false;
        }

        int batch_size = MAX_BATCH_READ_BLOCKNUM * BLCKSZ;
        buf = (char*)palloc0((uint32)batch_size);
        int max_times = (int)(seg_size % batch_size == 0 ? (int)seg_size / batch_size :
                                                         ((int)seg_size / batch_size + 1));

        for (int j = 0; j < max_times; j++) {
            int read_size = 0;
            uint32 remote_size = 0;
            repairFileKey->blockstart = (uint32)(seg_no * (int)relSegSize + j * MAX_BATCH_READ_BLOCKNUM);
            if ((int)seg_size % batch_size != 0) {
                read_size = (j == max_times - 1 ? (int)seg_size % batch_size : batch_size);
            } else {
                read_size = batch_size;
            }

            /* read many pages from remote */
            RemoteReadFile(repairFileKey, buf, read_size, timeout, &remote_size);

            /* write to local temp file */
            if (IS_COMPRESSED_RNODE(rd_node, MAIN_FORKNUM)) {
                rc = WriteRepairFile_Compress(rd_node, fd, segpath, buf,
                                              (BlockNumber)(j * MAX_BATCH_READ_BLOCKNUM),
                                              (uint32)read_size / BLCKSZ);
            } else {
                rc = WriteRepairFile(fd, segpath, buf, j * batch_size, read_size);
            }
            if (rc != 0) {
                (void)close(fd);
                pfree(buf);
                pfree(segpath);
                ereport(WARNING, (errcode_for_file_access(),
                        errmsg("could not write repair file \"%s\", segno is %d",
                               relpathperm(repairFileKey->relfilenode, repairFileKey->forknum), seg_no)));
                return false;
            }
        }

        if (!repair_deleted_file_check(fileNode, fd)) {
            (void)close(fd);
        }
        pfree(buf);
        rc = CheckAndRenameFile(segpath);
        if (rc != 0) {
            pfree(segpath);
            ereport(WARNING, (errcode_for_file_access(),
                    errmsg("could not rename file \"%s\", segno is %d",
                           relpathperm(repairFileKey->relfilenode, repairFileKey->forknum), seg_no)));
            return false;
        }
        BatchUpdateRepairTime(repairFileKey->relfilenode, repairFileKey->forknum, seg_no);
    }

    pfree(segpath);
    return true;
}

void CreateZeroFile(char* path)
{
    int fd = -1;
    int retry_times = 0;
    const int MAX_RETRY_TIME = 2;

    while (fd < 0) {
        retry_times++;
        fd = BasicOpenFile((char*)path, O_CREAT | O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);

        if (fd < 0) {
            if (retry_times < MAX_RETRY_TIME) {
                continue;
            }
            if (errno != ENOENT) {
                ereport(WARNING, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", path)));
                return;
            }
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not create file \"%s\": %m", path)));
        }
        (void)close(fd);
        return;
    }
}

static void checkFileNeedCreate(char* firstPath, RemoteReadFileKey key)
{
    struct stat statBuf;
    if (stat(firstPath, &statBuf) < 0 && errno == ENOENT) {
        CreateZeroFile(firstPath);
        ereport(WARNING, (errmsg("standby size is zero, only create file, file path is %s", firstPath)));
    }
    if (key.forknum == INIT_FORKNUM) {
        char* unlogPath = relpathperm(key.relfilenode, MAIN_FORKNUM);
        if (stat(unlogPath, &statBuf) < 0) {
            CreateZeroFile(unlogPath);
            ereport(WARNING, (errmsg("standby size is zero, only create file, file path is %s", unlogPath)));
        }
        pfree(unlogPath);
    }
    return;
}

bool gsRepairFile(Oid tableOid, char* path, int timeout)
{
    Relation relation = NULL;
    bool isSegment = false;
    RelFileNodeForkNum relFileNodeForkNum;
    RemoteReadFileKey repairFileKey;
    bool isCsnOrCLog = isCLogOrCsnLogPath(path);
    if (isCsnOrCLog) {
        return gsRepairCsnOrCLog(path, timeout);
    }

    relFileNodeForkNum = relpath_to_filenode(path);
    isSegment = relFileNodeForkNum.rnode.node.relNode <= 5 && relFileNodeForkNum.rnode.node.relNode > 0;
    if (isSegment) {
        relFileNodeForkNum.rnode.node.bucketNode = SegmentBktId;
        DirectFunctionCall2(pg_advisory_xact_lock_int4, t_thrd.postmaster_cxt.xc_lockForBackupKey1,
                            t_thrd.postmaster_cxt.xc_lockForBackupKey2);
    } else {
        relation = heap_open(tableOid, AccessExclusiveLock);
    }

    repairFileKey.relfilenode = relFileNodeForkNum.rnode.node;
    repairFileKey.forknum = relFileNodeForkNum.forknumber;
    if (!isSegment && IS_COMPRESSED_RNODE(relation->rd_node, MAIN_FORKNUM)) {
        repairFileKey.relfilenode.opt = relation->rd_node.opt;
    }
    char* firstPath = relpathperm(repairFileKey.relfilenode, repairFileKey.forknum);

    int64 size = RemoteReadFileSize(&repairFileKey, timeout);
    if (size == -1) {
        if (!isSegment) {
            heap_close(relation, AccessExclusiveLock);
        }
        ereport(WARNING,
            (errmsg("The file does not exist on the standby DN, don't need repair, file path is %s", firstPath)));
        pfree(firstPath);
        BatchClearBadBlock(repairFileKey.relfilenode, repairFileKey.forknum, 0);
        return false;
    }
    if (size == 0) {
        ereport(WARNING, (errmsg("standby size is zero, file path is %s", firstPath)));
        checkFileNeedCreate(firstPath, repairFileKey);
        pfree(firstPath);
        BatchClearBadBlock(repairFileKey.relfilenode, repairFileKey.forknum, 0);
        return true;
    }

    if (isSegment) {
        int maxSegno = (size % (RELSEG_SIZE * BLCKSZ)) != 0 ? size / (RELSEG_SIZE * BLCKSZ) :
                        (size / (RELSEG_SIZE * BLCKSZ)) - 1;

        for (int i = 0; i <= maxSegno; i++) {
            bool repair = PrimaryRepairSegFile_Segment(&repairFileKey, firstPath, i, maxSegno, timeout, size);
            if (!repair) {
                ereport(WARNING, (errmsg("repair file %s seg_no is %d, failed", path, i)));
                pfree(firstPath);
                return false;
            }
        }

        RepairFileKey key;
        key.relfilenode = repairFileKey.relfilenode;
        key.forknum = repairFileKey.forknum;
        key.segno = maxSegno;
        df_close_all_file(key, maxSegno);
        df_open_all_file(key, maxSegno);
    } else {
        BlockNumber relSegSize = IS_COMPRESSED_RNODE(relation->rd_node,
                                                     MAIN_FORKNUM) ? CFS_LOGIC_BLOCKS_PER_FILE: RELSEG_SIZE;
        int32 maxSegno = (int32)((size % ((int64)relSegSize * BLCKSZ)) != 0
                                     ? (size / ((int64)relSegSize * BLCKSZ))
                                     : (size / ((int64)relSegSize * BLCKSZ)) - 1);

        for (int32 i = 0; i <= maxSegno; i++) {
            bool repair = PrimaryRepairSegFile_NonSegment(relation->rd_node, &repairFileKey, firstPath, i, maxSegno,
                                                          timeout, size);
            if (!repair) {
                ereport(WARNING, (errmsg("repair file %s seg_no is %d, failed", path, i)));
                pfree(firstPath);
                return false;
            }
        }

        heap_close(relation, AccessExclusiveLock);
    }

    pfree(firstPath);
    return true;
}

int CheckAndRenameFile(char* path)
{
    char *tempPath = (char *)palloc0(strlen(path) + SEGLEN);
    errno_t rc;

    rc = sprintf_s(tempPath, strlen(path) + SEGLEN, "%s.repair", path);
    securec_check_ss(rc, "", "")
    rc = durable_rename(tempPath, path, ERROR);
    if (rc != 0) {
        ereport(WARNING, (errcode_for_file_access(),
                errmsg("could not stat file \"%s\":%m", path)));
        pfree(tempPath);
        return -1;
    } else {
        ereport(LOG, (errmodule(MOD_REDO),
                errmsg("file rename from %s to %s finish", tempPath, path)));
    }
    pfree(tempPath);
    return 0;
}

bool isCLogOrCsnLogPath(char* path)
{
    if ((strstr((char *)path, "pg_clog/")) != NULL || (strstr((char *)path, "pg_csnlog/")) != NULL) {
        return true;
    }
    return false;
}

bool gsRepairCsnOrCLog(char* path, int timeout)
{
    struct stat statBuf;
    if (stat(path, &statBuf) >= 0) {
        ereport(ERROR,
               (errmsg("file %s exists.", path)));
        return false;
    }
    int nmatch = 0;
    char* logType = (char*)palloc0(MAX_PATH_LEN);
    Oid logName = 0;
    int transType = 0;
    char* tmptoken = NULL;
    errno_t rc = strcpy_s(logType, strlen(path) + 1, path);
    securec_check(rc, "\0", "\0");
    strtok_s(logType, "/", &tmptoken);
    nmatch = sscanf_s(tmptoken, "%u", &logName);
    if (nmatch != 1) {
        pfree(logType);
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("path does not contain valid logName")));
        return false;
    }

    if (strcmp(logType, "pg_clog") == 0) {
        transType = 1;
    } else if (strcmp(logType, "pg_csnlog") == 0) {
        transType = 2;
    } else {
        pfree(logType);
        ereport(ERROR, (errcode_for_file_access(),
                errmsg("path not match clog or csnlog")));
        return false;
    }
    pfree(logType);

    RelFileNode relFileNode = {
        .spcNode = (uint32)transType,
        .dbNode = 0,
        .relNode = logName,
        .bucketNode = InvalidBktId,
        .opt = 0
    };

    RemoteReadFileKey repairFileKey = {
        .relfilenode = relFileNode,
        .forknum = MAIN_FORKNUM,
        .blockstart = 0
    };

    uint32 log_size = 16 * REGR_MCR_SIZE_1MB;

    int fd = CreateRepairFile(path);
    if (fd < 0) {
        ereport(WARNING, (errcode_for_file_access(),
                errmsg("could not create repair file \"%s\"", path)));
        return false;
    }

    char* buf = (char*)palloc0(log_size);
    uint32 remote_size = 0;
    RemoteReadFile(&repairFileKey, buf, log_size, timeout, &remote_size);

    rc = WriteRepairFile(fd, path, buf, 0, remote_size);
    if (rc != 0) {
        pfree(buf);
        (void)close(fd);
        return false;
    }
    pfree(buf);
    (void)close(fd);
    CheckAndRenameFile(path);
    return true;
}

static void checkSupUserOrOperaMode()
{
    if (!CheckVerionSupportRepair()) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Verify and repair page and file is not supported yet")));
    }
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin or operator admin in operation mode to call this function."))));
    }
}

static void checkUserPermission()
{
    if (!superuser() && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)
        && !isMonitoradmin(GetUserId())) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be system admin, operator admin in operation mode or monitor admin "
                "to call this function."))));
    }
}


static void checkInstanceType()
{
    load_server_mode();

    if (t_thrd.xlog_cxt.server_mode != PRIMARY_MODE) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("Must be in primary DN."))));
    }
}
