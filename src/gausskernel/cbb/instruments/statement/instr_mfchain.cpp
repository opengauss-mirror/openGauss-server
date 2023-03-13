/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * instr_mfchain.cpp
 *   functions for full/slow SQL in standby
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/instruments/statement/instr_mfchain.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "instruments/instr_mfchain.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "knl/knl_thread.h"
#include "storage/checksum_impl.h"
#include "storage/smgr/fd.h"
#include "access/heapam.h"
#include "miscadmin.h"

#define GetMemFileItemLen(item) (((MemFileItem*)(item))->len)
#define MFCHAIN_IS_ONLINE(mfchain) (mfchain != NULL && mfchain->state == MFCHAIN_STATE_ONLINE)

#define MFBLOCKBUFF_GET_HEADER(buff) ((MemFileBlockBuffHeader*)(buff))
#define MFBLOCKBUFF_GET_TAIL(buff)   (((char*)(buff)) + MFBLOCK_SIZE)
#define MFBLOCK_GET_FIRSTITEM_P(buff, ofs) (char*)(((char*)(buff)) + (ofs))
#define MFBLOCK_GET_FIRSTITEM_O(buff, ptr) (uint32)((ptr) - ((char*)(buff)))

#define BlockIsEmpty(block) ((block) == NULL || \
                             ((block)->state == MFBLOCK_IN_MEMORY && (block)->firstItem == (block)->barrier2) || \
                             (block)->state == MFBLOCK_DELETED)

typedef enum BlockActionState {
    BLOCK_ACTION_SUCCESS,

    /* using for fill block */
    BLOCK_FILL_READONLY_ERR,
    BLOCK_FILL_FULL_ERR,
    BLOCK_FILL_BIGITEM_ERR,

    /* using for advance block state */
    BLOCK_FLUSH_DISK_ERR,
    BLOCK_ADV_MEMORY_ERR,

    /* using for reload and verify block file */
    BLOCK_RELOAD_NO_FILE_ERR,
    BLOCK_RELOAD_FILE_DAMAGE_ERR,
    BLOCK_VERIFY_VERSION_ERR,
    BLOCK_VERIFY_CHECKSUM_ERR,
    BLOCK_VERIFY_SDESC_ERR,
    BLOCK_GET_PATH_ERR
} BlockActionState;

typedef enum ChainActionState {
    CHAIN_ACTION_SUCCESS,

    /* CREATING */
    CHAIN_CREATE_LONGNAME_ERR,
    CHAIN_CREATE_DIR_ERR,
    CHAIN_CREATE_SIZEPARAM_ERR,
    CHAIN_CREATE_TYPEMOD_ERR,
    CHAIN_CREATE_LOCK_ERR,
    CHAIN_CREATE_BLOCK_ERR,

    /* ADVANCE */
    CHAIN_ADV_DISK_ERR,

    /* INSERT */
    CHAIN_SDESC_RECREATE,

    CHAIN_TURN_ON,
    CHAIN_TURN_OFF,

    CHAIN_LOAD_FILE_ERR
} ChainActionState;

typedef struct BlockAdvanceResult {
    BlockActionState  state;
    MemFileBlockBuff* freeBuff;

    BlockAdvanceResult() {
        state = BLOCK_ACTION_SUCCESS;
        freeBuff = NULL;
    }
} BlockAdvanceResult;

static ChainActionState ResetChain(MemFileChain* mfchain);
void ReportChainException(MemFileChain* mfchain, ChainActionState state);

/* ------------------------------------------------
 * UTILS func
 * ------------------------------------------------
 */
static bool ParseMFBlockFileName(const char* filename, uint32 *id)
{
    char* ptr = NULL;
    int64 res = strtol(filename, &ptr, 10);
    if (*ptr != '\0' || res < MFCHAIN_FIRST_ID || res > UINT32_MAX) {
        return false;
    }
    *id = (uint32)res;
    return true;
}

static int CmpMemFileBlockId(const void *a, const void *b)
{
    const MemFileBlock* block1 = *((const MemFileBlock**)a);
    const MemFileBlock* block2 = *((const MemFileBlock**)b);

    if (block1->id < block2->id) 
        return 1;
    else if (block1->id == block2->id)
        return 0;
    else
        return -1;
}

/*
 * Because we only support append cloumn to system table in upgrade, so we can just compare natts.
 */
static inline bool CompareSimpleTupleDesc(SimpleTupleDesc* sDesc, TupleDesc desc)
{
    return sDesc->natts == desc->natts;
}

static inline bool CompareSimpleTupleDesc(SimpleTupleDesc* sDesc1, SimpleTupleDesc* sDesc2)
{
    return sDesc1->natts == sDesc2->natts;
}

static void ZeroMemFileChainStruct(MemFileChain* mfchain)
{
    mfchain->memCxt = NULL;
    mfchain->lock = NULL;
    mfchain->state = MFCHAIN_STATE_NOT_READY;
    mfchain->needClean = true;
    mfchain->path = NULL;
    mfchain->name = NULL;
    mfchain->chainHead = NULL;
    mfchain->chainBoundary = NULL;
    mfchain->chainTail = NULL;
    mfchain->blockNumM = 0;
    mfchain->blockNum = 0;
    mfchain->maxBlockNumM = 0;
    mfchain->maxBlockNum = 0;
    mfchain->retentionTime = 0;
    mfchain->relOid = InvalidOid;
    mfchain->sDesc = NULL;
}

/* ------------------------------------------------
 * BLOCK utils
 * ------------------------------------------------
 */
static void ReportBlockException(uint32 blockId, BlockActionState state, int level = WARNING)
{
    if (state == BLOCK_ACTION_SUCCESS) {
        return;
    }

    switch(state) {
        case BLOCK_FILL_READONLY_ERR:
            ereport(level, (errmsg("MemFileBlock %u fill exception: block is readonly.", blockId)));
            break;
        case BLOCK_FILL_FULL_ERR:
            ereport(level, (errmsg("MemFileBlock %u fill exception: block is full.", blockId)));
            break;
        case BLOCK_FILL_BIGITEM_ERR:
            ereport(level, (errmsg("MemFileBlock fill exception: "
                "item too big so that even cannot store it using a whole block.")));
            break;
        case BLOCK_FLUSH_DISK_ERR:
            ereport(level, (errmsg("MemFileBlock flush exception: "
                "Disk is inaccessible or insufficient space, errno(%d)", errno)));
            break;
        case BLOCK_ADV_MEMORY_ERR:
            ereport(level, (errmsg("MemFileBlock flush exception: The memory is inaccessible or insufficient.")));
            break;
        case BLOCK_RELOAD_NO_FILE_ERR:
            ereport(level, (errmsg("MemFileBlock %u reload exception: file not exists.", blockId)));
            break;
        case BLOCK_RELOAD_FILE_DAMAGE_ERR:
            ereport(level, (errmsg("MemFileBlock %u reload exception: file is damaged.", blockId)));
            break;
        case BLOCK_VERIFY_VERSION_ERR:
            ereport(level, (errmsg("MemFileBlock %u verify exception: version is different from now.", blockId)));
            break;
        case BLOCK_VERIFY_CHECKSUM_ERR:
            ereport(level, (errmsg("MemFileBlock %u verify exception: checksum not match.", blockId)));
            break;
        case BLOCK_VERIFY_SDESC_ERR:
            ereport(level, (errmsg("MemFileBlock %u verify exception: SimpleTupleDesc not match to now.", blockId)));
            break;
        case BLOCK_GET_PATH_ERR:
            ereport(level, (errmodule(MOD_INSTR),
                errmsg("MemFileBlock %u get path exception: The memory is inaccessible or insufficient.", blockId)));
            break;
        default:
            ereport(ERROR, (errmsg("Unknow Mem-File-Block action state.")));
    }
}

/*
 * Compute the checksum for a mem-file block.
 * detail reference pg_checksum_page
 */
static uint32 CheckSumBlockBuff(MemFileBlockBuff* buff)
{
    MemFileBlockBuffHeader* bheader = MFBLOCKBUFF_GET_HEADER(buff);
    uint32 oldChecksum = bheader->checksum;
    bheader->checksum = 0;
    uint32 checksum = pg_checksum_block((char*)buff, MFBLOCK_SIZE);
    bheader->checksum = oldChecksum;

    return (checksum % UINT32_MAX) + 1;
}

static char* GetBlockPath(MemFileBlock* block, char* buff = NULL)
{
    Assert(block->parent != NULL);

    char* path = buff != NULL ? buff : (char*)palloc0_noexcept(MAXPGPATH);
    if (path == NULL) {
        ReportBlockException(block->id, BLOCK_GET_PATH_ERR);
        return path;
    }
    errno_t rc = sprintf_s(path, MAXPGPATH, "%s/%u", block->parent->path, block->id);
    securec_check_ss(rc, "", "");
    return path;
}

/*
 * 1. Verify Version, to prevent block file of different versions from being used together.
 * 2. Verify checksum, to prevent read damaged block file.
 * 3. Verify SimpleTupleDesc, to prevent block file of different table versions from being used together.
 *    because we only support append cloumn to system table in upgrade, so we can just compare natts.
 */
static BlockActionState VerifyBlockBuff(MemFileBlockBuff* buff, SimpleTupleDesc* sDesc)
{
    MemFileBlockBuffHeader* bheader = MFBLOCKBUFF_GET_HEADER(buff);
    if (bheader->version != MFBLOCK_VERSION) {
        return BLOCK_VERIFY_VERSION_ERR;
    }

    uint32 checksum = CheckSumBlockBuff(buff);
    if (checksum != bheader->checksum) {
        return BLOCK_VERIFY_CHECKSUM_ERR;
    }

    if (!CompareSimpleTupleDesc(&bheader->sDesc, sDesc)) {
        return BLOCK_VERIFY_SDESC_ERR;
    }

    return BLOCK_ACTION_SUCCESS;
}

/*
 * calculate and write information from Block into BlockBuff Header space.
 */
static BlockActionState CompleteBlockBuffHeader(MemFileBlock* block)
{
    Assert(block->buff != NULL);
    Assert(block->parent != NULL);

    MemFileBlockBuffHeader* bheader = MFBLOCKBUFF_GET_HEADER(block->buff);
    errno_t rc = memset_s(bheader, MFBLOCK_HEADER_SIZE, 0, MFBLOCK_HEADER_SIZE);
    securec_check(rc, "", "");

    bheader->version = MFBLOCK_VERSION;
    bheader->createTime = block->createTime;
    bheader->flushTime = block->flushTime;
    bheader->firstItem = MFBLOCK_GET_FIRSTITEM_O(block->buff, block->firstItem);
    
    bheader->sDesc.natts = block->parent->sDesc->natts;
    for (int i = 0; i < bheader->sDesc.natts; i++) {
        bheader->sDesc.attrs[i] = block->parent->sDesc->attrs[i];
        Assert((char*)&bheader->sDesc.attrs[i] < block->barrier1);
    }

    /* finally calculate checksum */
    bheader->checksum = CheckSumBlockBuff(block->buff);
    return BLOCK_ACTION_SUCCESS;
}

static BlockActionState FlushBlockBuff(MemFileBlock* block)
{
    Assert(block->state == MFBLOCK_IN_MEMORY);
    Assert(block->buff != NULL);

    block->flushTime = GetCurrentTimestamp();
    BlockActionState state = CompleteBlockBuffHeader(block);
    if (state != BLOCK_ACTION_SUCCESS) {
        return state;
    }

    char* path = GetBlockPath(block);
    if (path == NULL) {
        return BLOCK_GET_PATH_ERR;
    }
    int fd = open(path, O_RDWR | O_CREAT, S_IREAD | S_IWRITE);
    if (fd == -1) {
        pfree(path);
        return BLOCK_FLUSH_DISK_ERR;
    }
    char* ptr = (char*)block->buff;
    bool success = true;
    int writeSize = -1;
    int leftSize = MFBLOCK_SIZE;
    while (leftSize > 0) {
        writeSize = write(fd, ptr, leftSize);
        if (writeSize <= 0) {
            success = false;
            break;
        }
        leftSize -= writeSize;
        ptr += writeSize;
    }
    if (success) {
        fsync(fd);
    } else {
        unlink(path);
    }

    close(fd);
    pfree(path);

    return success ? BLOCK_ACTION_SUCCESS : BLOCK_FLUSH_DISK_ERR;
}

static BlockActionState ReloadBlockFile(char* filename, char* buffer, bool justHead)
{
    Assert(buffer != NULL);
    int size = justHead ? MFBLOCK_HEADER_SIZE : MFBLOCK_SIZE;
    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        return BLOCK_RELOAD_NO_FILE_ERR;
    }
    int readSize = read(fd, buffer, size);
    close(fd);

    return readSize == size ? BLOCK_ACTION_SUCCESS : BLOCK_RELOAD_FILE_DAMAGE_ERR;
}

/* ------------------------------------------------
 * BLOCK interface
 * ------------------------------------------------
 * Only five actions are public:
 *     Create, Reset, Advance, Destory, Fill
 */
static void ResetBlock(MemFileBlock* block)
{
    Assert(block->state == MFBLOCK_IN_MEMORY);
    block->firstItem = block->barrier2;
    block->flushTime = DT_NOEND;
}

/* Create a in memory block */
static MemFileBlock* CreateBlock(MemFileChain* parent, uint32 id, MemFileBlockBuff* buff = NULL)
{
    Assert(parent != NULL && id > MFCHAIN_INVALID_ID);

    MemFileBlock* block = (MemFileBlock*)palloc0_noexcept(sizeof(MemFileBlock));
    if (block == NULL) {
        ReportChainException(parent, CHAIN_CREATE_BLOCK_ERR);
        return block;
    }
    if (buff == NULL) {
        buff = (MemFileBlockBuff*)palloc0_noexcept(sizeof(MemFileBlockBuff));
    }
    if (buff == NULL) {
        ReportChainException(parent, CHAIN_CREATE_BLOCK_ERR);
        pfree_ext(block);
        return NULL;
    }

    block->state = MFBLOCK_IN_MEMORY;
    block->parent = parent;
    block->id = id;
    block->buff = buff;
    block->createTime = GetCurrentTimestamp();
    block->barrier1 = ((char*)buff + MFBLOCK_HEADER_SIZE);
    block->barrier2 = ((char*)buff + MFBLOCK_SIZE);
    ResetBlock(block);

    block->next = NULL;
    block->prev = NULL;

    return block;
}

/* create a in file block */
static MemFileBlock* CreateBlock(MemFileChain* parent, uint32 id, char* filename, char* buff, TimestampTz oldestTime)
{
    Assert(buff != NULL);
    BlockActionState res = ReloadBlockFile(filename, buff, true);
    if (res != BLOCK_ACTION_SUCCESS) {
        return NULL;
    }

    MemFileBlockBuffHeader* bheader = (MemFileBlockBuffHeader*)buff;

    /* do some simple check, ignore when failed */
    if (bheader->version != MFBLOCK_VERSION || bheader->flushTime <= bheader->createTime ||
        bheader->flushTime < oldestTime) {
        return NULL;
    }
    
    MemFileBlock* block = (MemFileBlock*)palloc(sizeof(MemFileBlock));
    block->parent = parent;
    block->state = MFBLOCK_IN_FILE;
    block->id = id;
    block->createTime = bheader->createTime;
    block->flushTime = bheader->flushTime;
    block->buff = NULL;
    block->firstItem = NULL;
    block->barrier1 = NULL;
    block->barrier2 = NULL;
    block->next = NULL;
    block->prev = NULL;

    return block;
}

/*
 * Advance State Table:
 *     MFBLOCK_IN_MEMORY -> MFBLOCK_IN_BOTH -> MFBLOCK_IN_FILE -> MFBLOCK_DELETED
 */
static BlockAdvanceResult AdvanceBlock(MemFileBlock* block)
{
    BlockAdvanceResult res;

    switch (block->state) {
        case MFBLOCK_IN_MEMORY: {
            res.state = FlushBlockBuff(block);
            if (res.state == BLOCK_ACTION_SUCCESS) {
                block->state = MFBLOCK_IN_BOTH;
            }
            /* do nothing if err, chain will handle it */
        } break;

        case MFBLOCK_IN_BOTH: {
            res.freeBuff = block->buff;
            block->state = MFBLOCK_IN_FILE;
            block->buff = NULL;
            block->firstItem = NULL;
            block->barrier1 = NULL;
            block->barrier2 = NULL;
            res.state = BLOCK_ACTION_SUCCESS;
        } break;

        case MFBLOCK_IN_FILE: {
            char* path = GetBlockPath(block);
            if (access(path, F_OK) == 0) {
                unlink(path);
            }
            pfree(path);
            block->state = MFBLOCK_DELETED;
            res.state = BLOCK_ACTION_SUCCESS;
        } break;

        /* do nothing for MFBLOCK_DELETED, chain will destory it */
        default:
            res.state = BLOCK_ACTION_SUCCESS;
            break;
    }

    return res;
}

static void DestoryBlock(MemFileBlock* block, bool deep = true)
{
    if (block->state == MFBLOCK_IN_MEMORY) {
        pfree(block->buff);
        pfree(block);
        return;
    } else if (block->state == MFBLOCK_DELETED) {
        pfree(block);
        return;
    }

    if (deep) {
        char* path = GetBlockPath(block);
        if (path != nullptr) {
            unlink(path);
            pfree(path);
        }
    }

    if (block->state == MFBLOCK_IN_BOTH) {
        pfree(block->buff);
    }
    pfree(block);
}


/* read HeapTuple and push it as item into Block */
static BlockActionState FillBlock(MemFileBlock* block, HeapTuple tup)
{
#define GetBlockBuffLeftSize(block) ((block)->firstItem - (block)->barrier1)

    Assert(block->state == MFBLOCK_IN_MEMORY);
    if (tup == NULL) {
        return BLOCK_ACTION_SUCCESS;
    }

    /* we must cpoy data first, then set firstItem pointer, to prevent read err data. see also ScannerLoadBlock */
    uint32 itemLen = tup->t_len + sizeof(uint32);
    if (GetBlockBuffLeftSize(block) >= itemLen) {
        MemFileItem* item = (MemFileItem*)(block->firstItem - itemLen);
        item->len = itemLen;
        errno_t rc = memcpy_s(item->data, tup->t_len, tup->t_data, tup->t_len);
        securec_check(rc, "", "");

        block->firstItem = (char*)item;
        return BLOCK_ACTION_SUCCESS;

    } else if (unlikely(itemLen > MFBLOCK_SIZE - MFBLOCK_HEADER_SIZE)) {
        return BLOCK_FILL_BIGITEM_ERR;

    } else {
        return BLOCK_FILL_FULL_ERR;
    }
}

/* ------------------------------------------------
 * CHAIN utils
 * ------------------------------------------------
 */
typedef ChainActionState (*CreateMFChainStep)(MemFileChain* mfchain, MemFileChainCreateParam* param);

#define IsLegalChainSize(maxBlockNumM, maxBlockNum, retentionTime)            \
    ((maxBlockNumM) >= MIN_MBLOCK_NUM && (maxBlockNumM) <= MAX_MBLOCK_NUM &&  \
     (maxBlockNum)  >= MIN_FBLOCK_NUM && (maxBlockNum)  <= MAX_FBLOCK_NUM &&  \
     (maxBlockNumM) <= (maxBlockNum)  && (retentionTime) > 0)

void ReportChainException(MemFileChain* mfchain, ChainActionState state)
{
    if (state == CHAIN_ACTION_SUCCESS) {
        return;
    }
    int level = WARNING;
    switch (state) {
        case CHAIN_CREATE_LONGNAME_ERR:
            ereport(level, (errmsg("MemFileChain create exception: name '%s' too long.", mfchain->name)));
            break;
        case CHAIN_CREATE_DIR_ERR:
            ereport(level, (errmsg("MemFileChain create exception: cannot create dir '%s'.", mfchain->path)));
            break;
        case CHAIN_CREATE_SIZEPARAM_ERR:
            ereport(level, (errmsg("MemFileChain create exception: invalid size param.")));
            break;
        case CHAIN_CREATE_TYPEMOD_ERR:
            ereport(level, (errmsg("MemFileChain create exception: table column has typemod.")));
            break;
        case CHAIN_CREATE_LOCK_ERR:
            ereport(level, (errmsg("MemFileChain create exception: invalid lock.")));
            break;
        case CHAIN_CREATE_BLOCK_ERR:
            ereport(level,
                (errmsg("MemFileChain create block exception: The memory is inaccessible or insufficient.")));
            break;
        case CHAIN_ADV_DISK_ERR:
            ereport(level, (errmsg("MemFileChain advance exception: Disk is inaccessible or insufficient space.")));
            break;
        case CHAIN_SDESC_RECREATE:
            ereport(level, (errmsg("MemFileChain insert exception: sDesc is changed and recreate.")));
            break;
        case CHAIN_TURN_ON:
            ereport(level, (errmsg("MemFileChain state:  mem file chain turn on success.")));
            break;
        case CHAIN_TURN_OFF:
            ereport(level, (errmsg("MemFileChain exception: Something bad happened, mem file chain turn off.")));
            break;
        case CHAIN_LOAD_FILE_ERR:
            ereport(level, (errmsg("MemFileChain exception: load file error.")));
            break;
        default:
            ereport(ERROR, (errmsg("Unknow MemFileChain action state.")));
    }
}

static bool GetBlockFileAndPrecheck(const char* name, const char* path, char* outFullname, uint32* id)
{
    if (unlikely(strcmp(name, ".") == 0 || strcmp(name, "..") == 0))
        return false;

    errno_t rc = snprintf_s(outFullname, MAXPGPATH, MAXPGPATH - 1, "%s/%s", path, name);
    securec_check_ss(rc, "\0", "\0");
    
    struct stat fst;
    if (stat(outFullname, &fst) < 0 ||
        S_ISDIR(fst.st_mode) ||
        !ParseMFBlockFileName(name, id)) {
        return false;
    }
    return true;
}

static MemFileBlock** ReconstructOldBlockFile(MemFileChain* mfchain, List* blocksList, int* size)
{
    /* firstly, sort by id */
    int len = list_length(blocksList);
    MemFileBlock** blocks = (MemFileBlock**)palloc0_noexcept(len * sizeof(MemFileBlock*));
    if (blocks == NULL) {
        ereport(WARNING, (errmodule(MOD_INSTR),
                errmsg("MemFileChain load file, palloc file blocks memory is inaccessible or insufficient.")));
        return NULL;
    }

    int i = 0;
    ListCell* lc = NULL;
    foreach(lc, blocksList) {
        blocks[i] = (MemFileBlock*)lfirst(lc);
        i++;
    }
    qsort(blocks, len, sizeof(MemFileBlock*), CmpMemFileBlockId);

    /* then, do a pre trim, and reconstruct file name(id) */
    uint32 newId = MFCHAIN_FIRST_ID;
    char path[MAXPGPATH] = {0};
    char newpath[MAXPGPATH] = {0};
    for (i = len - 1; i >= 0; i--) {
        if (i < len - mfchain->maxBlockNum) {
            DestoryBlock(blocks[i]);
            blocks[i] = NULL;
        } else {
            GetBlockPath(blocks[i], path);
            blocks[i]->id = newId;
            GetBlockPath(blocks[i], newpath);
            if (path[0] == '\0' || newpath[0] == '\0') {
                return NULL;
            }

            rename(path, newpath);
            newId++;
        }
    }
    *size = (int)newId - 1;
    return blocks;
}

/* scan all file in chain location, remove or reload it. */
static List* ReloadOrCleanOldBlockFile(MemFileChain* mfchain, int* len)
{
    struct dirent* direntry = NULL;
    char filename[MAXPGPATH] = {'\0'};
    DIR* dirdesc = AllocateDir(mfchain->path);
    bool clean = mfchain->needClean;
    if (unlikely(NULL == dirdesc)) {
        *len = 0;
        ereport(WARNING, (errmodule(MOD_INSTR),
                errmsg("MemFileChain load file, allocate dir error.")));
        return NULL;
    }

    char* buffer = (char*)palloc(MFBLOCK_HEADER_SIZE);
    List* blocksList = NULL;
    uint32 id;
    TimestampTz oldestTime = GetCurrentTimestamp() - (TimestampTz)mfchain->retentionTime * USECS_PER_SEC;
    while ((direntry = ReadDir(dirdesc, mfchain->path)) != NULL) {
        if (!GetBlockFileAndPrecheck(direntry->d_name, mfchain->path, filename, &id)) {
            continue;
        }

        if (clean) {
            unlink(filename);
        } else {
            MemFileBlock* block = CreateBlock(mfchain, id, filename, buffer, oldestTime);
            if (block != NULL) {
                blocksList = lappend(blocksList, block);
            } else {
                /* file damaged or too old */
                unlink(filename);
            }
        }
    }
    pfree(buffer);
    FreeDir(dirdesc);
    return blocksList;
}

static inline bool ChainNeedTrimOldest(MemFileChain* mfchain)
{
    return (mfchain->chainTail->state != MFBLOCK_IN_MEMORY && 
        (GetCurrentTimestamp() - mfchain->chainTail->flushTime) / USECS_PER_SEC > mfchain->retentionTime);
}

static void ChainTrimBlock(MemFileChain* mfchain, int maxBlockNumM, int maxBlockNum, int retentionTime)
{
    Assert(IsLegalChainSize(maxBlockNumM, maxBlockNum,retentionTime));

    while (mfchain->blockNumM > maxBlockNumM) {
        BlockAdvanceResult res = AdvanceBlock(mfchain->chainBoundary);
        mfchain->blockNumM--;
        mfchain->chainBoundary = mfchain->chainBoundary->prev;
        pfree(res.freeBuff);
    }

    while (mfchain->blockNum > maxBlockNum) {
        MemFileBlock* tmp = mfchain->chainTail;
        mfchain->chainTail = mfchain->chainTail->prev;
        mfchain->chainTail->next = NULL;
        mfchain->blockNum--;
        DestoryBlock(tmp);
    }

    TimestampTz currTime = GetCurrentTimestamp();
    while (mfchain->chainTail->state != MFBLOCK_IN_MEMORY && 
           (currTime - mfchain->chainTail->flushTime) / USECS_PER_SEC > retentionTime) {
        MemFileBlock* tmp = mfchain->chainTail;
        mfchain->chainTail = mfchain->chainTail->prev;
        mfchain->chainTail->next = NULL;
        mfchain->blockNum--;

        if (tmp->state == MFBLOCK_IN_BOTH) {
            mfchain->blockNumM--;
            mfchain->chainBoundary = mfchain->chainTail;
            Assert(mfchain->blockNumM == mfchain->blockNum);
        }
        DestoryBlock(tmp, true);
    }
}

/* bind the location param, and make sure the location is valid */
static ChainActionState CreateMFChainLocation(MemFileChain* mfchain, MemFileChainCreateParam* param)
{
    Assert(param->name != NULL && param->dir != NULL);

    mfchain->name = pstrdup(param->name);  // we will use it when destory mfchain
    if (strlen(param->name) > MFBLOCK_NAME_LENGTH) {
        return CHAIN_CREATE_LONGNAME_ERR;
    }

    char* path = (char*)palloc0_noexcept(MAXPGPATH);
    if (path == NULL) {
        return CHAIN_CREATE_DIR_ERR;
    }
    errno_t rc = sprintf_s(path, MAXPGPATH, "%s/%s", t_thrd.proc_cxt.DataDir, param->dir);
    securec_check_ss(rc, "", "");
    if (access(path, F_OK) == -1) {
        if (mkdir(path, S_IRWXU) == -1) {
            pfree(path);
            return CHAIN_CREATE_DIR_ERR;
        }
    }

    rc = sprintf_s(path, MAXPGPATH, "%s/%s/%s", t_thrd.proc_cxt.DataDir, param->dir, param->name);
    securec_check_ss(rc, "", "");
    if (access(path, F_OK) == -1) {
        if (mkdir(path, S_IRWXU) == -1) {
            pfree(path);
            return CHAIN_CREATE_DIR_ERR;
        }
    }

    mfchain->path = path;
    return CHAIN_ACTION_SUCCESS;
}

static ChainActionState CreateMFChainSize(MemFileChain* mfchain, MemFileChainCreateParam* param)
{
    if (IsLegalChainSize(param->maxBlockNumM, param->maxBlockNum, param->retentionTime)) {
        mfchain->maxBlockNumM  = param->maxBlockNumM;
        mfchain->maxBlockNum   = param->maxBlockNum;
        mfchain->retentionTime = param->retentionTime;
        return CHAIN_ACTION_SUCCESS;
    }
    return CHAIN_CREATE_SIZEPARAM_ERR;   
}

static ChainActionState CreateMFChainBlocks(MemFileChain* mfchain, MemFileChainCreateParam* param)
{
    MemFileBlock** blocks = NULL;
    int newId = 0;
    mfchain->needClean = param->needClean;

    /* 1, reload or clean old block file, it will do some trimming here. */
    List* blocksList = ReloadOrCleanOldBlockFile(mfchain, &newId);
    int len = 0;
    if (blocksList != NULL) {
        len = list_length(blocksList);
        blocks = ReconstructOldBlockFile(mfchain, blocksList, &newId);
        list_free(blocksList);
 
        if (blocks == NULL) {
            return CHAIN_LOAD_FILE_ERR;
        }
    }

    /* 2, create a new chain head */
    MemFileBlock* headBlock = CreateBlock(mfchain, newId + MFCHAIN_FIRST_ID);
    if (headBlock == NULL) {
        return CHAIN_CREATE_BLOCK_ERR;
    }
    mfchain->chainHead = headBlock;
    mfchain->chainBoundary = headBlock;
    mfchain->chainTail = headBlock;
    mfchain->blockNum = 1;
    mfchain->blockNumM = 1;

    /* 3.append old blocks if have */
    for (int i = 0; i < len; i++) {
        if (blocks[i] == NULL) {
            continue;
        }
        mfchain->chainTail->next = blocks[i];
        blocks[i]->prev = mfchain->chainTail;
        mfchain->chainTail = blocks[i];
        mfchain->blockNum++;
    }

    /* 4.do a trim formally */
    ChainTrimBlock(mfchain, mfchain->maxBlockNumM, mfchain->maxBlockNum, mfchain->retentionTime);

    if (blocks != NULL) {
        pfree(blocks);
    }
    return CHAIN_ACTION_SUCCESS;
}

static ChainActionState ReCreateMFChainSDesc(MemFileChain* mfchain, Oid oid, Relation rel)
{
    if (OidIsValid(oid)) {
        Assert(rel == NULL);
        rel = relation_open(oid, AccessShareLock);
    }
    Assert(rel != NULL);

    ChainActionState res = CHAIN_ACTION_SUCCESS;
    TupleDesc desc = RelationGetDescr(rel);
    SimpleTupleDesc *sDesc = (SimpleTupleDesc*)MemoryContextAlloc(mfchain->memCxt, 
                                                                  sizeof(int) + sizeof(Oid) * desc->natts);
    sDesc->natts = desc->natts;
    for (int i = 0; i < desc->natts; i++) {
        sDesc->attrs[i] = desc->attrs[i].atttypid;
        if (desc->attrs[i].atttypmod != -1) {
            res = CHAIN_CREATE_TYPEMOD_ERR;
            break;
        }
    }
    if (res == CHAIN_ACTION_SUCCESS) {
        pfree_ext(mfchain->sDesc);
        mfchain->sDesc = sDesc;
        mfchain->relOid = RelationGetRelid(rel);
    }

    if (OidIsValid(oid)) {
        relation_close(rel, AccessShareLock);
    }
    return res;
}

static ChainActionState CreateMFChainSDesc(MemFileChain* mfchain, MemFileChainCreateParam* param)
{
    return ReCreateMFChainSDesc(mfchain, InvalidOid, param->rel);
}

static ChainActionState CreateMFChainLock(MemFileChain* mfchain, MemFileChainCreateParam* param)
{
    if (param->lock == NULL) {
        mfchain->lock = NULL;
        return CHAIN_CREATE_LOCK_ERR;
    }
    mfchain->lock  = param->lock;
    return CHAIN_ACTION_SUCCESS;
}

static void MemFileChainTurnOff(MemFileChain* mfchain)
{
    if (mfchain->state != MFCHAIN_STATE_ONLINE) {
        return;
    }
    mfchain->state = MFCHAIN_STATE_OFFLINE;
    ReportChainException(mfchain, CHAIN_TURN_OFF);
}

static void MemFileChainTurnOn(MemFileChain* mfchain)
{
    if (mfchain->state != MFCHAIN_STATE_OFFLINE) {
        return;
    }

    /* when we turn off, relation may be changed, recheck it. */
    Relation rel = relation_open(mfchain->relOid, AccessShareLock);
    if (!CompareSimpleTupleDesc(mfchain->sDesc, RelationGetDescr(rel))) {
        ResetChain(mfchain);
        ChainActionState res = ReCreateMFChainSDesc(mfchain, InvalidOid, rel);
        if (res != CHAIN_ACTION_SUCCESS) {
            relation_close(rel, AccessShareLock);
            ReportChainException(mfchain, res);
            ereport(ERROR, (errmsg("MemFileChain of %s cannot turn on.", mfchain->name)));
        }
    }
    relation_close(rel, AccessShareLock);

    mfchain->state = MFCHAIN_STATE_ONLINE;
    ReportChainException(mfchain, CHAIN_TURN_ON);
}

/*
 * advance the mfchain, include follows:
 *    1. flush header block and create a new block on chain header
 *    2. advance chain boundary
 *    3. remove oldest
 */
static ChainActionState AdvanceChain(MemFileChain* mfchain)
{
    if (BlockIsEmpty(mfchain->chainHead)) {
        return CHAIN_ACTION_SUCCESS;
    }
    MemoryContext oldContext = MemoryContextSwitchTo(mfchain->memCxt);
    /* 1. extend chain */
    uint32 newId = mfchain->chainHead->id + 1;
    MemFileBlockBuff* buff = (mfchain->blockNumM < mfchain->maxBlockNumM) ? NULL : mfchain->chainBoundary->buff;
    MemFileBlock* newBlock = CreateBlock(mfchain, newId, buff);
    if (newBlock == NULL) {
        return CHAIN_CREATE_BLOCK_ERR;
    }
 
    /* 2. AdvanceBlock */
    /* 2.1, flush header node into disk */
    BlockAdvanceResult res = AdvanceBlock(mfchain->chainHead);
    if (res.state != BLOCK_ACTION_SUCCESS) {
        if (res.state == BLOCK_GET_PATH_ERR) {
            DestoryBlock(newBlock, false);
            return CHAIN_CREATE_BLOCK_ERR;
        }
 
        Assert(res.state == BLOCK_FLUSH_DISK_ERR);
        // disk is full, so try to trim 1/3 of chain and retry, if still failed,
        // report and turn off the chain.
        ReportBlockException(mfchain->chainHead->id, res.state);
        int blockNum = Max(MIN_FBLOCK_NUM, mfchain->blockNum / 3 * 2);    // look above
        int blockNumM = Min(mfchain->blockNumM, blockNum);
        ChainTrimBlock(mfchain, blockNum, blockNumM, mfchain->retentionTime);
        res = AdvanceBlock(mfchain->chainHead);
        if (res.state != BLOCK_ACTION_SUCCESS) {
            ReportBlockException(mfchain->chainHead->id, res.state);
            MemFileChainTurnOff(mfchain);
            return CHAIN_TURN_OFF;
        }
    }

    /* 2.2, extend chain */
    newBlock->next = mfchain->chainHead;
    mfchain->chainHead->prev = newBlock;
    mfchain->chainHead = newBlock;
    mfchain->blockNum++;
    if (mfchain->blockNumM < mfchain->maxBlockNumM) {
        mfchain->blockNumM++;
    } else {
        (void)AdvanceBlock(mfchain->chainBoundary);
        mfchain->chainBoundary = mfchain->chainBoundary->prev;
    }

    /* 3, remove oldest if needed */
    ChainTrimBlock(mfchain, mfchain->maxBlockNumM, mfchain->maxBlockNum, mfchain->retentionTime);

    MemoryContextSwitchTo(oldContext);
    return CHAIN_ACTION_SUCCESS;
}

static ChainActionState ResetChain(MemFileChain* mfchain)
{
    while (mfchain->chainTail->state != MFBLOCK_IN_MEMORY) {
        MemFileBlock* tmp = mfchain->chainTail;
        mfchain->chainTail = mfchain->chainTail->prev;
        DestoryBlock(tmp, true);
    }

    Assert(mfchain->chainTail == mfchain->chainHead);
    ResetBlock(mfchain->chainHead);
    mfchain->chainBoundary = mfchain->chainHead;
    mfchain->blockNumM = 1;
    mfchain->blockNum = 1;
    return CHAIN_ACTION_SUCCESS;
}

/* ------------------------------------------------
 * CHAIN interface
 * ------------------------------------------------
 * Create, Destory, Insert, Regulate
 * 
 * notice: the mfchain struct is provide by caller.
 */
MemFileChain* MemFileChainCreate(MemFileChainCreateParam* param)
{
    MemFileChain* mfchain = NULL;
    /* if not init, just create and return a empty mfchain struct. */
    if (param->notInit) {
        mfchain = (MemFileChain*)MemoryContextAllocZero(param->parent, sizeof(MemFileChain));
        ZeroMemFileChainStruct(mfchain);
        return mfchain;
    }

    mfchain = param->initTarget;
    if (mfchain == NULL || mfchain->state != MFCHAIN_STATE_NOT_READY) {
        ereport(ERROR, (errmsg("Invalid mem-file chain init params.")));
    }

    /* firstly, create memory context and mfchain struct */
    MemoryContext memcxt = AllocSetContextCreate(param->parent,
        param->name,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);
    MemoryContext oldcxt = MemoryContextSwitchTo(memcxt);
    mfchain->memCxt = memcxt;
    mfchain->state = MFCHAIN_STATE_OFFLINE;

    CreateMFChainStep steps[] = {
        CreateMFChainLock,              // step 1. bind lock
        CreateMFChainLocation,          // step 2. check and complete path and  
        CreateMFChainSize,              // step 3. assign param size
        CreateMFChainSDesc,             // step 4. create simple Desc
        CreateMFChainBlocks,            // step 5. init chain of mem file block. keep it last
        NULL                            // nothing, just a end flag
    };
    for (int i = 0; steps[i] != NULL; i++) {
        ChainActionState res = steps[i](mfchain, param);
        if (res != CHAIN_ACTION_SUCCESS) {
            ReportChainException(mfchain, res);
            MemFileChainDestory(mfchain);
            MemoryContextSwitchTo(oldcxt);
            ereport(ERROR, (errmodule(MOD_INSTR), errmsg("MemFileChain of %s init done and error.", param->name)));
            return NULL;
        }
    }

    mfchain->state = MFCHAIN_STATE_ONLINE;
    MemoryContextSwitchTo(oldcxt);

    ereport(LOG, (errmsg("Mem-file chain of %s init done and online.", param->name)));
    return mfchain;
}

void MemFileChainDestory(MemFileChain* mfchain)
{
    if (mfchain == NULL || mfchain->state == MFCHAIN_STATE_NOT_READY) {
        return;
    }
    ereport(LOG, (errmodule(MOD_INSTR),
            errmsg("MemFileChain destory start, clean is %d.", mfchain->needClean)));

    LWLock* lock = mfchain->lock;
    if (lock != NULL) {
        LWLockAcquire(lock, LW_EXCLUSIVE);
    }

    /*
     * if neecClean, remove all files by destory block.
     * if not, just do an chain-advance to flush the header block, the memory of all blocks
     * is palloced from mfchain->memCxt, it will be free when mfchain->memCxt deleted.
     */
    if (mfchain->needClean) {
        MemFileBlock* block = mfchain->chainHead;
        MemFileBlock* next = NULL;
        while (block != NULL) {
            next = block->next;
            DestoryBlock(block, true);
            block = next;
        }
        if (rmdir(mfchain->path) < 0) {
            ereport(WARNING, (errmodule(MOD_INSTR),
                errmsg("MemFileChain destory block failed: %s.", mfchain->path)));
        }
    } else {
        ChainActionState cres = AdvanceChain(mfchain);
        if (cres != CHAIN_ACTION_SUCCESS) {
            ReportChainException(mfchain, cres);
        }
    }
    ereport(LOG, (errmsg("Mem-file chain of %s %s destory.", mfchain->name, mfchain->needClean ? "deep" : "light")));
    MemoryContextDelete(mfchain->memCxt);

    /* mfchain struct is provided by caller, reset it to zero. */
    ZeroMemFileChainStruct(mfchain);

    /* the lock is provided by caller when create, not in mfchain->memCxt. */
    if (lock != NULL) {
        LWLockRelease(lock);
    }
}

/*
 * turn chain on if it already offline.
 * trim chain oldest block if need, trim and assign it with new size
 */
void MemFileChainRegulate(MemFileChain* mfchain, MemFileChainRegulateType type,
    int maxBlockNumM, int maxBlockNum, int retentionTime)
{
    if (mfchain == NULL || mfchain->state == MFCHAIN_STATE_NOT_READY) {
        return;
    }

    /* turn it on or off automatically */
    if (mfchain->state == MFCHAIN_STATE_OFFLINE) {
        LWLockAcquire(mfchain->lock, LW_EXCLUSIVE);
        MemFileChainTurnOn(mfchain);
        LWLockRelease(mfchain->lock);
    }

    if (type == MFCHAIN_TRIM_OLDEST) {
        if (ChainNeedTrimOldest(mfchain)) {
            maxBlockNumM = mfchain->maxBlockNumM;
            maxBlockNum = mfchain->maxBlockNum;
            retentionTime = mfchain->retentionTime;
        } else {
            return;
        }
    } else if (type == MFCHAIN_ASSIGN_NEW_SIZE) {
        if ((maxBlockNumM != mfchain->maxBlockNumM || maxBlockNum != mfchain->maxBlockNum ||
            retentionTime != mfchain->retentionTime) && IsLegalChainSize(maxBlockNumM, maxBlockNum, retentionTime)) {
            /* only size param change and new size is legal size, do trim, and assign them after trim. */
        } else {
            return;
        }
    } else {
        return;
    }
    LWLockAcquire(mfchain->lock, LW_EXCLUSIVE);
    ChainTrimBlock(mfchain, maxBlockNumM, maxBlockNum, retentionTime);
    if (type == MFCHAIN_ASSIGN_NEW_SIZE) {
        mfchain->maxBlockNumM = maxBlockNumM;
        mfchain->maxBlockNum = maxBlockNum;
        mfchain->retentionTime = retentionTime;
    }
    LWLockRelease(mfchain->lock);
}

/*
 * currently on support insert a HeapTuple into mfchain
 * if we cannot insert it, just report a warning and skip it
 */
bool MemFileChainInsert(MemFileChain* mfchain, HeapTuple tup, Relation rel)
{
    if (!MFCHAIN_IS_ONLINE(mfchain) || tup == NULL || rel == NULL) {
        return false;
    }

    /*
     * if the rel column has changed, we cannot insert in to the block, it is better to
     * recreate whole chain, insert it.
     */
    if (!CompareSimpleTupleDesc(mfchain->sDesc, RelationGetDescr(rel))) {
        LWLockAcquire(mfchain->lock, LW_EXCLUSIVE);
        ChainActionState res = ReCreateMFChainSDesc(mfchain, InvalidOid, rel);
        if (res == CHAIN_ACTION_SUCCESS) {
            ResetChain(mfchain);
            ReportChainException(mfchain, CHAIN_SDESC_RECREATE);
            LWLockRelease(mfchain->lock);
        } else {
            MemFileChainTurnOff(mfchain);
            LWLockRelease(mfchain->lock);
            return false;
        }
    }

    BlockActionState res = FillBlock(mfchain->chainHead, tup);

    if (res == BLOCK_FILL_FULL_ERR) {
        /* first Block is full, advance chain and retry insert */
        LWLockAcquire(mfchain->lock, LW_EXCLUSIVE);
        ChainActionState cres = AdvanceChain(mfchain);
        LWLockRelease(mfchain->lock);
        if (cres == CHAIN_TURN_OFF || cres == CHAIN_CREATE_BLOCK_ERR) {
            return false;
        }
        res = FillBlock(mfchain->chainHead, tup);
        Assert(res == BLOCK_ACTION_SUCCESS);

    } else if (res == BLOCK_FILL_BIGITEM_ERR) {
        /* Item too big, just ignore it */
        ReportBlockException(mfchain->chainHead->id, res);
    }
    return true;
}

/* ------------------------------------------------
 * SCAN
 * ------------------------------------------------
 * scan the mfchain
 * init Chain with one Node in Mem
 */
static inline void ScannerSetScanDone(MemFileChainScanner* scanner)
{
    scanner->scanDone = true;
    scanner->nextBlock = NULL;
    scanner->nextBlockId = MFCHAIN_INVALID_ID;
    scanner->nextItem = NULL;
}

static void ScannerSetScanProgress(MemFileChainScanner* scanner, TimestampTz time1, TimestampTz time2)
{
    /* we must have two valid time, scan from time1 to time2 */
    if (time1 > time2) {
        ereport(ERROR, (errmsg("invalid time param for mfchain scanner, time1:%ld, time2:%ld", time1, time2)));
    }

    if (time1 > GetCurrentTimestamp()) {
        ScannerSetScanDone(scanner);
        return;
    }

    LWLockAcquire(scanner->mfchain->lock, LW_SHARED);
    if (time2 < scanner->mfchain->chainTail->createTime) {
        ScannerSetScanDone(scanner);
    } else {
        scanner->scanDone = false;
        scanner->range[0] = time1;
        scanner->range[1] = time2;
        scanner->nextBlock = scanner->mfchain->chainHead;
        scanner->nextBlockId = scanner->mfchain->chainHead->id;
    }
    LWLockRelease(scanner->mfchain->lock);
    return;
 }

/*
 * read a Block, get and copy data into scanner buffer
 */
static BlockActionState ScannerLoadBlock(MemFileChainScanner* scanner, MemFileBlock* block)
{
    Assert(!BlockIsEmpty(block));

    /*
     * when we read a MFBLOCK_IN_MEMORY block, must get Item point first, then copy the buff,
     * to prevent read err data, see also FillBlock().
     */
    if (block->state == MFBLOCK_IN_MEMORY || block->state == MFBLOCK_IN_BOTH) {
        int firstItemOffset = MFBLOCK_GET_FIRSTITEM_O(block->buff, block->firstItem);
        scanner->nextItem = MFBLOCK_GET_FIRSTITEM_P(scanner->buff, firstItemOffset);

        errno_t rc = memcpy_s(scanner->buff, MFBLOCK_SIZE, block->buff, MFBLOCK_SIZE);
        securec_check(rc, "", "");

    } else if (block->state == MFBLOCK_IN_FILE) {
        char* path = GetBlockPath(block);
        if (path == NULL) {
            ereport(ERROR, (errmodule(MOD_INSTR), errmsg("Load File: memory is temporarily unavailable")));
        }
        BlockActionState res = ReloadBlockFile(path, (char*)scanner->buff, false);
        pfree(path);
        if (res != BLOCK_ACTION_SUCCESS) {
            return res;
        }
        res = VerifyBlockBuff(scanner->buff, scanner->mfchain->sDesc);
        if (res != BLOCK_ACTION_SUCCESS) {
            return res;
        }

        MemFileBlockBuffHeader* bheader = MFBLOCKBUFF_GET_HEADER(scanner->buff);
        scanner->nextItem = MFBLOCK_GET_FIRSTITEM_P(scanner->buff, bheader->firstItem);
    }
    return BLOCK_ACTION_SUCCESS;
}

/*
 * 1 means this block is too new, try next
 * 0 means we want it
 * -1 means this block and behind are too old and not need.
 */
static int CheckBlockIsWanted(MemFileChainScanner* scanner, MemFileBlock* block)
{
    if (block == NULL) {
        return -1;
    } else if (block->createTime >  scanner->range[1]) {
        return 1;
    } else if (block->createTime >= scanner->range[0]) {
        /* mabey it */
    } else if (block->flushTime  >= scanner->range[0]) {
        /* mabey it */
    } else {
        return -1;
    }

    /* skip if block is empty, and try next */
    if (BlockIsEmpty(block)) {
        return 1;
    }
    return 0;
}

/* find and load next in scan range, living, not empty block into scanner */
static bool ScannerNextBlock(MemFileChainScanner* scanner)
{
    if (scanner->nextBlockId == MFCHAIN_INVALID_ID) {
        return false;
    }

    MemFileChain* mfchain = scanner->mfchain;
    bool find = false;

    /* pre check if the next block is exists. */
    LWLockAcquire(mfchain->lock, LW_SHARED);
    if (scanner->nextBlockId < mfchain->chainTail->id) {
        LWLockRelease(mfchain->lock);
        return false;
    }

    while (!find) {
        /* check if the block in range and not empty */
        int position = CheckBlockIsWanted(scanner, scanner->nextBlock);
        if (position == 1) {
            scanner->nextBlock = scanner->nextBlock->next;
            continue;
        } else if (position == -1) {
            break;
        }

        BlockActionState res = ScannerLoadBlock(scanner, scanner->nextBlock);
        if (res == BLOCK_ACTION_SUCCESS) {
            find = true;
        } else {
            ReportBlockException(scanner->nextBlock->id, res);
        }

        scanner->nextBlock = scanner->nextBlock->next;
    };

    scanner->nextBlockId = scanner->nextBlock == NULL ? MFCHAIN_INVALID_ID : scanner->nextBlock->id;
    LWLockRelease(mfchain->lock);
    return find;
}

MemFileChainScanner* MemFileChainScanStart(MemFileChain* mfchain, TimestampTz time1, TimestampTz time2)
{
    if (!MFCHAIN_IS_ONLINE(mfchain)) {
        return NULL;
    }

    MemFileChainScanner* scanner = (MemFileChainScanner*)palloc(sizeof(MemFileChainScanner));
    scanner->mfchain = mfchain;
    ScannerSetScanProgress(scanner, time1, time2);
    scanner->nextItem = NULL;

    if (scanner->scanDone) {
        scanner->buff = NULL;
        scanner->barrier = NULL;
    } else {
        scanner->buff = (MemFileBlockBuff*)palloc(sizeof(MemFileBlockBuff));
        scanner->barrier = MFBLOCKBUFF_GET_TAIL(scanner->buff);
    }
    return scanner;
}

MemFileItem* MemFileChainScanGetNext(MemFileChainScanner* scanner)
{
    if (scanner->scanDone || !MFCHAIN_IS_ONLINE(scanner->mfchain)) {
        return NULL;
    }

    if (scanner->nextItem == NULL ||                                 // just started
        scanner->nextItem >= scanner->barrier ||                     // at the end of scanner buffer
        scanner->nextItem + sizeof(uint32) > scanner->barrier  ||    // left space is not enough an len or item
        scanner->nextItem + GetMemFileItemLen(scanner->nextItem) > scanner->barrier) {
        /* the block in scanner buffer is scan done, get next */
        if (!ScannerNextBlock(scanner)) {
            ScannerSetScanDone(scanner);
            return NULL;
        }
    }

    MemFileItem* item = (MemFileItem*)scanner->nextItem;
    scanner->nextItem = scanner->nextItem + item->len;

    return item;
}

void MemFileChainScanEnd(MemFileChainScanner* scanner)
{
    if (scanner == NULL) {
        return;
    }
    if (scanner->buff != NULL) {
        pfree(scanner->buff);
    }
    pfree(scanner);
}
