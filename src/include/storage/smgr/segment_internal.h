/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * ---------------------------------------------------------------------------------------
 *
 * segment_internal.h
 *
 *
 * IDENTIFICATION
 *        src/include/storage/segment_internal.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef STORAGE_SEGMENT_INTERNAL_H
#define STORAGE_SEGMENT_INTERNAL_H

#include "c.h"
#include "pgxc/groupmgr.h"
#include "postmaster/aiocompleter.h"
#include "storage/buf/block.h"
#include "storage/buf/buf.h"
#include "storage/buf/bufpage.h"
#include "storage/lock/lwlock.h"
#include "storage/smgr/smgr.h"
#include "utils/segment_test.h"

const int DF_MAP_GROUP_RESERVED = 3;
const int DF_MAX_MAP_GROUP_CNT = 33;

static const int SEGMENT_MAX_FORKNUM = VISIBILITYMAP_FORKNUM;
/*
 * File directory organization:
 *
 *  tablespace/database/
 *      .
 *      ..
 *      relfilenode1 (heap table)
 *      relfilenode2 (heap table)
 *      1  (segment file)
 *      1.0  (segment file)
 *      2  (segment file)
 *      3  (segment file)
 *      4  (segment file)
 *      5  (segment file)
 */

/*
 * Logic segment file
 *
 * A logic segment file is divided into several sequential physical files, to avoid the size of
 * a single file exceeding the file system's limit, or concurrent visist to the same physical file.
 * This design is similar to file segment (_MdfdVec) in md.c. We use the term "file slice" here
 * to distinguish with the term "segment".
 */

typedef struct SegPhysicalFile {
    int fd;
    int sliceno;
} SegPhysicalFile;

const int DF_ARRAY_EXTEND_STEP = 4;
const ssize_t DF_FILE_EXTEND_STEP_BLOCKS = RELSEG_SIZE / 8;
const ssize_t DF_FILE_EXTEND_STEP_SIZE = DF_FILE_EXTEND_STEP_BLOCKS * BLCKSZ; // 128MB
const ssize_t DF_FILE_SLICE_BLOCKS = RELSEG_SIZE;
const ssize_t DF_FILE_SLICE_SIZE = DF_FILE_SLICE_BLOCKS * BLCKSZ;             // 1GB
const ssize_t DF_FILE_MIN_BLOCKS = DF_FILE_EXTEND_STEP_BLOCKS;

#define DF_OFFSET_TO_SLICENO(offset) (offset / DF_FILE_SLICE_SIZE)
#define DF_OFFSET_TO_SLICE_OFFSET(offset) (offset % DF_FILE_SLICE_SIZE)

typedef struct SegLogicFile {
    SegPhysicalFile *segfiles;
    RelFileNode relNode;
    ForkNumber forknum;
    int vector_capacity; // current segfile array capacity
    int file_num;
    BlockNumber total_blocks;
    char filename[MAXPGPATH];
    pthread_mutex_t filelock;
} SegLogicFile;

void df_ctrl_init(SegLogicFile *sf, RelFileNode relNode, ForkNumber forknum);
void df_open_files(SegLogicFile *sf);
void df_extend(SegLogicFile *sf, BlockNumber target_blocks);
void df_pread_block(SegLogicFile *sf, char *buffer, BlockNumber blocknum);
void df_pwrite_block(SegLogicFile *sf, const char *buffer, BlockNumber blocknum);
void df_fsync(SegLogicFile *sf);
void df_unlink(SegLogicFile *sf);
void df_create_file(SegLogicFile *sf, bool redo);
void df_shrink(SegLogicFile *sf, BlockNumber target);
void df_flush_data(SegLogicFile *sf, BlockNumber blocknum, BlockNumber nblocks);

/*
 * Data files status in the segment space;
 */
enum SpaceDataFileStatus {
    EMPTY,   // space has not been created; there is no data file
    CRASHED, // space data files are crashed, only because creation failed halfway
    NORMAL   // space data files are consistent and able to provide services
};

/*
 * Extent group.
 *
 * We have five types of extent size. Extents with the same size are managed
 * into one extent group.
 *
 * Segment metadata block, e.g., segment head, use a special type extent with size 1.
 * ------------   -----------------  -----------
 * extent size  | extent page count |   size id
 * ------------   -----------------  -----------
 *    8K                1                0
 *
 * We have four type extent with different size used for relation data.
 * Extent size is auto-increased based on extents count, following is the detail:
 * ------------   -----------------  -----------  ----------------------- --------------
 * extent size  | extent page count |   size id  |   extents count range |  total size
 * ------------   -----------------  -----------  ----------------------- --------------
 *    64K               8                1            [1, 16]                   1M
 * ------------   -----------------  ------------ ----------------------- --------------
 *    1M               128               2            [17, 143]                128M
 * ------------   -----------------  ------------ ----------------------- --------------
 *    8M               1024              3            [144, 255]                1G
 * ------------   -----------------  ------------ ----------------------- --------------
 *    64M              8192              4            [256, ...)                ...
 * ------------   -----------------  ------------ ----------------------- --------------
 *
 * Different types of extents are stored in different physical files.
 * Thus, Metadata blocks and data blocks are physically distinguished.
 */
typedef enum ExtentSize {
    EXT_SIZE_1 = 1,
    EXT_SIZE_8 = 8,
    EXT_SIZE_128 = 128,
    EXT_SIZE_1024 = 1024,
    EXT_SIZE_8192 = 8192,
    INVALID_EXT_SIZE = 0,
} ExtentSize;

typedef enum ExtentBoundary {
    EXT_SIZE_8_BOUNDARY = 16,
    EXT_SIZE_128_BOUNDARY = 143,
    EXT_SIZE_1024_BOUNDARY = 255,
} ExtentBoundary;

typedef enum ExtentTotalPages {
    EXT_SIZE_8_TOTAL_PAGES = 128,
    EXT_SIZE_128_TOTAL_PAGES = 16384,
    EXT_SIZE_1024_TOTAL_PAGES = 131072,
} ExtentTotalPages;

typedef enum EXTENT_TYPE {
    EXTENT_INVALID = 0, // 0 means invalid
    EXTENT_1 = 1,
    EXTENT_8 = 2,
    EXTENT_128 = 3,
    EXTENT_1024 = 4,
    EXTENT_8192 = 5
} EXTENT_TYPE;

#ifdef ENABLE_SEGMENT_TEST
#define SEGMENTTEST(TEST_CODE, msg) \
    if (SEGMETN_TEST_STUB(TEST_CODE)) { \
        ereport(g_instance.segment_test_param_instance->elevel, msg); \
    }
#else
#define SEGMENTTEST(TEST_CODE, msg) ((void)0)
#endif
#define ExtentTypeIsValid(type) ((type) > EXTENT_INVALID && (type) <= EXTENT_8192)
#define EXTENT_TYPES EXTENT_8192
#define EXTENT_GROUPS EXTENT_8192

#define SEGMENT_HEAD_EXTENT_SIZE EXT_SIZE_1
#define SEGMENT_HEAD_EXTENT_TYPE EXTENT_1
#define LEVEL0_PAGE_EXTENT_SIZE EXT_SIZE_1
#define PhyBlockIsValid(pblk) (ExtentTypeIsValid((pblk).relNode) && (pblk).block != InvalidBlockNumber)

#define CM_ALIGN_ANY(size, align) (((size) + (align)-1) / (align) * (align))

/* bitmap functions */
#define DF_MAP_FREE(bitmap, pos) (!((bitmap)[(pos) >> 3] & (1 << ((pos)&0x07))))
#define DF_MAP_NOT_FREE(bitmap, pos) ((bitmap)[(pos) >> 3] & (1 << ((pos)&0x07)))
#define DF_MAP_SET(bitmap, pos) ((bitmap)[(pos) >> 3] |= 1 << ((pos)&0x07))
#define DF_MAP_UNSET(bitmap, pos) ((bitmap)[(pos) >> 3] &= ~(1 << ((pos)&0x07)))

typedef struct st_df_map_group {
    BlockNumber first_map; // start page id of bitmap pages of this group
    uint32 free_page;       // first free page of this group
    uint8  page_count;      // count of bitmap pages of this group
    uint8  reserved[DF_MAP_GROUP_RESERVED];
} df_map_group_t;

typedef struct st_df_map_head {
    uint16 bit_unit;    // page count that managed by one bit
    uint16 group_count; // count of bitmap group that already exists
    uint32 free_group; // first free group
    uint32 reserved;
    uint32 allocated_extents;
    uint32 high_water_mark;
    df_map_group_t groups[DF_MAX_MAP_GROUP_CNT];
} df_map_head_t;

typedef struct st_df_map_page {
    BlockNumber first_page; // first page managed by this bitmap
    uint16 free_begin;      // first free bit
    uint16 dirty_last;      // last dirty bit
    uint16 free_bits;       // free bits
    uint16 reserved;
    uint8 bitmap[0]; // following is the bitmap
} df_map_page_t;

typedef struct SpaceMapLocation {
    df_map_group_t group;
    uint32 map_id;
    uint32 bit_id;
    uint16 map_group;
} SpaceMapLocation;

/*
 * page distribution in datafile with bitmap:
 * ----- ------ ----------------------- ------ --------------------------------------------------
 * |file_head |spc head| map head | map pages | reverse pointer pages | data pages |  map pages |
 * ----- ------ ----------------------- ------ --------------------------------------------------
 *    1            1        1           64             ...                  ...          64
 *
 */

#define DF_MAP_HEAD_PAGE 2
#define DF_MAP_GROUP_SIZE 64

static const uint16 DF_MAP_SIZE = (BLCKSZ - offsetof(df_map_page_t, bitmap) - MAXALIGN(SizeOfPageHeaderData));
static const uint16 DF_MAP_BIT_CNT = (DF_MAP_SIZE * 8);
static const uint32 DF_MAP_GROUP_EXTENTS = (1LU * DF_MAP_BIT_CNT * DF_MAP_GROUP_SIZE);

#define MapLocationIsInvalid(loc) ((loc).group.first_map == 0)
/*
 * An Extent Group is a logical segment file which allocates extents in the same size.
 */

/* Space object status in memory */
enum SpaceObjectStatus { INITIAL, OPENED, CLOSED };

typedef struct SegExtentGroup {
    SegLogicFile *segfile;
    RelFileNode rnode;  // rnode.relNode is equal to the EXTENT_TYPE of this extent group
    ForkNumber forknum;
    int extent_size;
    SegSpace *space;
    pthread_mutex_t lock;
    BlockNumber map_head_entry; // default is DF_MAP_HEAD_PAGE
    df_map_head_t *map_head;
    Buffer map_head_buffer;
} SegExtentGroup;

typedef struct SegmentSpaceStat {
    uint32 extent_size;
    ForkNumber forknum;
    uint32 total_blocks;
    uint32 meta_data_blocks; // Including MapHead, MapPage.
    uint32 used_data_blocks; // Including segment head, normal data extent.
    float utilization;
    uint32 high_water_mark;
} SegmentSpaceStat;

/*
 * Extent inverse pointer to segment, used for space shrinking.
 *
 * Note: please just append in the last when adding new extent usage type, not modifying current
 * types, otherwise existing systems would crash after upgrading. 'ININVALID_EXTNT_USAGE' is always
 * equal to the last valid type value + 1.
 */
enum ExtentUsageType {
    NOT_USED = 0,   // default
    SEGMENT_HEAD,   // non-bucket table main segment head
    FORK_HEAD,      // non-bucket table fork segment head
    BUCKET_SEGMENT, // segment head for the whole bucket table
    BUCKET_MAP,     // bucket table map block
    BUCKET_HEAD,    //  segment head for single bucket
    DATA_EXTENT,    // extent used to store table data

    INVALID_EXTNT_USAGE, // must always be the last value in this enumerate.
};

#define SEGMENT_HEAD_MAGIC    0x44414548544e454d
#define BUCKET_SEGMENT_MAGIC  0x544e454d47455354
#define BUCKETMAP_MAGIC       0x50414d54454b4355
#define BMTLEVEL0_MAGIC       0x306c6576654c544d

#define IsNormalSegmentHead(head) (((SegmentHead*)(head))->magic == SEGMENT_HEAD_MAGIC)
#define IsBucketMainHead(head) (((BktMainHead*)(head))->magic == BUCKET_SEGMENT_MAGIC)
#define IsBucketMapBlock(head) (((BktHeadMapBlock*)(head))->magic == BUCKETMAP_MAGIC)
#define IsBMTLevel0Block(head) (((BMTLevel0Page*)(head))->magic == BMTLEVEL0_MAGIC)

typedef struct ExtentInversePointer {
    uint32 flag;
    BlockNumber owner; // owner oid (or block number)
} ExtentInversePointer;

/*
 * Extent Inverse Pointer flag is a 32-bit variable, combined as follow (from high to low)
 *  - 4 bits usage type
 *  - 10 bits reserved, set as zero
 *  - 18 bits usage-special data, for example, DATA_EXTENT use it to store extent id. BUCKET_MAP use
 *          it to store map group id. Default is zero.
 */
#define SPC_INVRSPTR_USAGE_MASK 0xF0000000U
#define SPC_INVRSPTR_USAGE_SHIFT 28
#define SPC_INVRSPTR_GET_USAGE(eip) ((ExtentUsageType)((eip).flag >> SPC_INVRSPTR_USAGE_SHIFT))
#define SPC_INVRSPTR_SPECIAL_DATA_MASK ((1U << 18) - 1)
#define SPC_INVRSPTR_GET_SPECIAL_DATA(eip) ((eip).flag & SPC_INVRSPTR_SPECIAL_DATA_MASK)

#define InversePointerIsValid(eip)                                                                                     \
    (SPC_INVRSPTR_GET_USAGE(eip) > NOT_USED && SPC_INVRSPTR_GET_USAGE(eip) < INVALID_EXTNT_USAGE)

/* Assemble usage and special_data into one uint32 variable */
#define SPC_INVRSPTR_ASSEMBLE_FLAG(usage, special_data)                                                                \
    ((((uint32)(special_data)) & SPC_INVRSPTR_SPECIAL_DATA_MASK) + (((uint32)(usage)) << SPC_INVRSPTR_USAGE_SHIFT))

/* Inverse pointer array size in each block */
static const uint32 IPBLOCK_SIZE = BLCKSZ - TYPEALIGN(sizeof(ExtentInversePointer), SizeOfPageHeaderData);
static const uint32 EXTENTS_PER_IPBLOCK = IPBLOCK_SIZE / sizeof(ExtentInversePointer);
static const uint32 IPBLOCK_GROUP_SIZE =
    (uint32)(DF_MAP_BIT_CNT * DF_MAP_GROUP_SIZE + EXTENTS_PER_IPBLOCK - 1) / EXTENTS_PER_IPBLOCK;

typedef struct IpBlockLocation {
    BlockNumber ipblock;
    uint32 offset;
} IpBlockLocation;

void SetInversePointer(SegExtentGroup *eg, BlockNumber extent, ExtentInversePointer iptr);
ExtentInversePointer GetInversePointer(SegExtentGroup *eg, BlockNumber extent, Buffer *buf);
void GetAllInversePointer(SegExtentGroup *seg, uint32 *cnt, ExtentInversePointer **iptrs, BlockNumber **extents);
const char *GetExtentUsageName(ExtentInversePointer iptr);

BlockNumber eg_df_size(SegExtentGroup *eg);
bool eg_df_exists(SegExtentGroup *eg);
void eg_create_df(SegExtentGroup *eg);
void eg_extend_df(SegExtentGroup *eg, BlockNumber target);
void eg_shrink_df(SegExtentGroup *eg, BlockNumber target);

void eg_ctrl_init(SegSpace *spc, SegExtentGroup *seg, int extent_size, ForkNumber forknum);
SpaceDataFileStatus eg_status(SegExtentGroup *eg);

BlockNumber eg_alloc_extent(SegExtentGroup *seg, BlockNumber preassigned_block, ExtentInversePointer iptr);
void eg_free_extent(SegExtentGroup* seg, BlockNumber blocknum);

bool eg_empty(SegExtentGroup *seg);
void eg_clean_data_files(SegExtentGroup *eg);
void eg_init_data_files(SegExtentGroup *eg, bool redo, XLogRecPtr rec_ptr);
SegmentSpaceStat eg_storage_stat(SegExtentGroup *seg);

/*
 * Segment space data structure
 *
 * Each <tablespace, datababase> has one SegSpace; each SegSpace has five extent groups.
 */
typedef struct SegSpcTag {
    Oid spcNode;
    Oid dbNode;
} SegSpcTag;

typedef struct SegSpace {
    Oid spcNode;
    Oid dbNode;
    volatile SpaceObjectStatus status;
    pthread_mutex_t lock;
    SegExtentGroup extent_group[EXTENT_TYPES][SEGMENT_MAX_FORKNUM + 1];
} SegSpace;

#define SMgrOpenSpace(reln)                                                                                            \
    do {                                                                                                               \
        if ((reln)->seg_space == NULL) {                                                                               \
            (reln)->seg_space = spc_open((reln)->smgr_rnode.node.spcNode, (reln)->smgr_rnode.node.dbNode, false);      \
        }                                                                                                              \
    } while (0)

extern void InitSegSpcCache(void);
void InitSpaceNode(SegSpace *spc, Oid spcNode, Oid dbNode, bool is_redo);

SegSpace *spc_open(Oid tablespace_id, Oid database_id, bool create, bool isRedo = false);
SegSpace *spc_init_space_node(Oid spcNode, Oid dbNode);
SpaceDataFileStatus spc_status(SegSpace *spc);
SegSpace *spc_drop(Oid tablespace_id, Oid database_id, bool redo);
void spc_lock(SegSpace *spc);
void spc_unlock(SegSpace *spc);

BlockNumber spc_alloc_extent(SegSpace *spc, int extent_size, ForkNumber forknum, BlockNumber designate_block,
                             ExtentInversePointer iptr);
void spc_free_extent(SegSpace *spc, int extent_size, ForkNumber forknum, BlockNumber blocknum);

void spc_write_block(SegSpace *spc, RelFileNode relNode, ForkNumber forknum, const char *buffer, BlockNumber blocknum);
void spc_read_block(SegSpace *spc, RelFileNode relNode, ForkNumber forknum, char *buffer, BlockNumber blocknum);
void spc_writeback(SegSpace *spc, int extent_size, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks);
void spc_shrink(Oid spcNode, Oid dbNode, int extent_size);
BlockNumber spc_size(SegSpace *spc, BlockNumber egRelNode, ForkNumber forknum);
void spc_datafile_create(SegSpace *spc, BlockNumber egRelNode, ForkNumber forknum);
void spc_extend_file(SegSpace *spc, BlockNumber egRelNode, ForkNumber forknum, BlockNumber blkno);
bool spc_datafile_exist(SegSpace *spc, BlockNumber egRelNode, ForkNumber forknum);

extern void spc_shrink_files(SegExtentGroup *seg, BlockNumber target_size, bool redo);

/*
 * Block Map Tree (BMT) related constants
 */
static const int BMT_HEADER_LEVEL0_SLOTS = 1255; // level0 slots must bigger or equal than EXT_SIZE_1024_BOUNDARY
static const int BMT_HEADER_LEVEL1_SLOTS = 256;
static const int BMT_HEADER_LEVEL0_TOTAL_PAGES =
    EXT_SIZE_1024_TOTAL_PAGES + (BMT_HEADER_LEVEL0_SLOTS - EXT_SIZE_1024_BOUNDARY) * EXT_SIZE_8192;
static const int BMT_TOTAL_SLOTS = BMT_HEADER_LEVEL0_SLOTS + BMT_HEADER_LEVEL1_SLOTS;

static const uint32_t BMT_LEVEL0_SLOTS = 2000;

typedef struct SegmentHead {
    uint64 magic;
    XLogRecPtr lsn;
    uint32 nblocks;      // block number reported to upper layer
    uint32 nextents;     // extent number allocated to this segment
    uint32 nslots : 16;  // not used yet
    int bucketid : 16;   // not used yet
    uint32 total_blocks; // total blocks can be allocated to table (exclude metadata pages)
    uint64 reserved;
    BlockNumber level0_slots[BMT_HEADER_LEVEL0_SLOTS];
    BlockNumber level1_slots[BMT_HEADER_LEVEL1_SLOTS];

    BlockNumber fork_head[SEGMENT_MAX_FORKNUM + 1];
} SegmentHead;

typedef struct BMTLevel0Page {
    uint64 magic;
    BlockNumber slots[BMT_LEVEL0_SLOTS];
} BMTLevel0Page;

typedef struct SegPageLocation {
    ExtentSize extent_size;
    uint32 extent_id;
    BlockNumber blocknum;
} SegPageLocation;

SegPageLocation seg_get_physical_location(RelFileNode rnode, ForkNumber forknum, BlockNumber blocknum);
void seg_record_new_extent_on_level0_page(SegSpace *spc, Buffer seg_head_buffer, uint32 new_extent_id,
                                          BlockNumber new_extent_first_pageno);
void seg_head_update_xlog(Buffer head_buffer, SegmentHead *seg_head, int level0_slot,
                          int level1_slot);
BlockNumber seg_extent_location(SegSpace *spc, SegmentHead *seg_head, int extent_id);

inline static ExtentSize ExtentSizeByCount(uint32 count)
{
    if (count < EXT_SIZE_8_BOUNDARY) {
        return EXT_SIZE_8;
    } else if (count < EXT_SIZE_128_BOUNDARY) {
        return EXT_SIZE_128;
    } else if (count < EXT_SIZE_1024_BOUNDARY) {
        return EXT_SIZE_1024;
    } else {
        return EXT_SIZE_8192;
    }
}

inline static uint32 ExtentIdToLevel1Slot(uint32 extent_id)
{
    Assert(extent_id >= BMT_HEADER_LEVEL0_SLOTS);
    return (extent_id - BMT_HEADER_LEVEL0_SLOTS) / BMT_LEVEL0_SLOTS;
}

inline static uint32 ExtentIdToLevel0PageOffset(uint32 extent_id)
{
    Assert(extent_id >= BMT_HEADER_LEVEL0_SLOTS);
    return (extent_id - BMT_HEADER_LEVEL0_SLOTS) % BMT_LEVEL0_SLOTS;
}

inline static BlockNumber ExtentIdToLogicBlockNum(uint32 extent_id)
{
    if (extent_id < EXT_SIZE_8_BOUNDARY) {
        return extent_id * EXT_SIZE_8;
    } else if (extent_id < EXT_SIZE_128_BOUNDARY) {
        return (extent_id - EXT_SIZE_8_BOUNDARY) * EXT_SIZE_128 + EXT_SIZE_8_TOTAL_PAGES;
    } else if (extent_id < EXT_SIZE_1024_BOUNDARY) {
        return (extent_id - EXT_SIZE_128_BOUNDARY) * EXT_SIZE_1024 + EXT_SIZE_128_TOTAL_PAGES;
    } else {
        return (extent_id - EXT_SIZE_1024_BOUNDARY) * EXT_SIZE_8192 + EXT_SIZE_1024_TOTAL_PAGES;
    }
}

/* Segment information in SMgrRelationData */
typedef struct SegmentDesc {
    BlockNumber head_blocknum; // Segment Head block number
    uint32 timeline;
} SegmentDesc;

#define IsNormalForknum(forknum)                                                                                       \
    ((forknum) == MAIN_FORKNUM || (forknum) == FSM_FORKNUM || (forknum) == VISIBILITYMAP_FORKNUM)

#define ASSERT_NORMAL_FORK(forknum) Assert(IsNormalForknum(forknum))

/* internal help function */

inline static BlockNumber ExtentIdToLevel0PageNumber(SegmentHead *seg_head, uint32 extent_id)
{
    extent_id -= BMT_HEADER_LEVEL0_SLOTS;

    uint32 groups = extent_id / BMT_LEVEL0_SLOTS;
    return seg_head->level1_slots[groups];
}

/*
 * Whether a BMT level 0 page is required when allocating a new extent.
 * The extent must have 8192 blocks.
 */
inline static bool RequireNewLevel0Page(uint32 new_extent_id)
{
    if (new_extent_id < BMT_HEADER_LEVEL0_SLOTS) {
        return false;
    }
    new_extent_id -= BMT_HEADER_LEVEL0_SLOTS;
    return (new_extent_id % BMT_LEVEL0_SLOTS) == 0;
}

/*
 * seg_read/seg_write are not conflicted with seg_extend, because seg_extend is an append-only operation
 * that only add new extent at the end of the segment. It does not change existing Block Map Tree. And
 * these three operations are frequent, thus we do not want they block each other.
 *
 * But spc_shrink has conflict with seg_read/seg_write because it will change the value in the Block Map Tree.
 * We add a new kind of LWLock for this conflict:
 *      spc_read/seg_write/seg_extend gets the shared lock,
 *      spc_shrink gets the exclusive lock.
 * Luckily, spc_shrink is an extra-low frequent operation. It should not influence the performance.
 */
#define SegmentHeadPartition(hashcode) ((hashcode) % NUM_SEGMENT_HEAD_PARTITIONS)
#define SegmentHeadPartitionLock(hashcode)                                                                             \
    (&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstSegmentHeadLock + SegmentHeadPartition(hashcode)].lock)
#define SegmentHeadPartitionLockByIndex(i) (&t_thrd.shemem_ptr_cxt.mainLWLockArray[FirstSegmentHeadLock + (i)].lock)

void LockSegmentHeadPartition(Oid spcNode, Oid dbNode, BlockNumber head, LWLockMode mode);
void UnlockSegmentHeadPartition(Oid spcNode, Oid dbNode, BlockNumber head);

/*
 * In hashbucket tables, each bucket has an individual segment (as well as an individual segment head).
 *
 * We use a two-level hiearchy to store bucket segment head locations. A hashbucket table has one BktMainHead
 * block, containing an two-dimension array recording segment heads of each bucket. The first dimension is
 * fork number, and the second dimension is bucket id. Because the array is too long to be stored in one
 * block, we divide the array into continuous BktHeadMapBlock, BktMainHead stores these BktHeadMapBlock location.
 *
 * The layout of hashbucket table are as follow:
 *      BktMainHead -> BktHeadMapBlock-> SegmentHead
 */
static const uint32 BktMapEntryNumberPerBlock =
    ((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - sizeof(uint64) - sizeof(XLogRecPtr)) / (sizeof(BlockNumber)));
static const uint32 BktMapBlockNumber =
    ((MAX_BUCKETMAPLEN * (SEGMENT_MAX_FORKNUM + 1) + BktMapEntryNumberPerBlock - 1) / BktMapEntryNumberPerBlock);
static const uint32 BktBitMaxMapCnt = (MAX_BUCKETMAPLEN / (sizeof(uint32) * 8));

typedef struct BktHeadMapBlock {
    uint64 magic;
    XLogRecPtr lsn;
    BlockNumber head_block[BktMapEntryNumberPerBlock];
} BktHeadMapBlock;

typedef struct SegRedisInfo {
    TransactionId redis_xid;
    uint32  nwords;
    uint32  reserved;
    uint32  words[BktBitMaxMapCnt];
} SegRedisInfo;


#define WORDOFF(x)	((x) / (sizeof(uint32) * 8))
#define BITOFF(x)	((x) % (sizeof(uint32) * 8))
#define SET_BKT_MAP_BIT(map, x) (map[WORDOFF(x)] |= ((uint32) 1 << (uint32)BITOFF(x)))
#define GET_BKT_MAP_BIT(map, x) (map[WORDOFF(x)] & ((uint32) 1 << (uint32)BITOFF(x)))
typedef struct BktMainHead {
    uint64 magic;
    XLogRecPtr lsn;
    SegRedisInfo  redis_info;
    uint64  reserved;
    BlockNumber bkt_map[BktMapBlockNumber];
} BktMainHead;

void bucket_init_map_page(Buffer map_buffer, XLogRecPtr lsn);

/*
 * To create a relation with segment storage, we need invoke the function "seg_alloc_segment" to get a segment head
 * and use its start block number as "relfilenode" which is used to identify the segment later.
 */
BlockNumber seg_alloc_segment(Oid tablespace_id, Oid database_id, bool isbucket, BlockNumber preassigned_block);

/* Make sure the segment has enough space for blkno */
void seg_preextend(RelFileNode &rNode, ForkNumber forkNum, BlockNumber blkno);


inline int EXTENT_TYPE_TO_SIZE(int type)
{
    if (type == EXTENT_1) {
        return EXT_SIZE_1;
    } else if (type == EXTENT_8) {
        return EXT_SIZE_8;
    } else if (type == EXTENT_128) {
        return EXT_SIZE_128;
    } else if (type == EXTENT_1024) {
        return EXT_SIZE_1024;
    } else if (type == EXTENT_8192) {
        return EXT_SIZE_8192;
    } else {
        return 0;
    }
}

inline EXTENT_TYPE EXTENT_SIZE_TO_TYPE(int extent_size)
{
    if (extent_size == EXT_SIZE_1) {
        return EXTENT_1;
    } else if (extent_size == EXT_SIZE_8) {
        return EXTENT_8;
    } else if (extent_size == EXT_SIZE_128) {
        return EXTENT_128;
    } else if (extent_size == EXT_SIZE_1024) {
        return EXTENT_1024;
    } else if (extent_size == EXT_SIZE_8192) {
        return EXTENT_8192;
    } else {
        Assert(0);
        return EXTENT_INVALID;
    }
}

#define EXTENT_TYPE_TO_GROUPID(extent_type) (extent_type - 1)
#define EXTENT_SIZE_TO_GROUPID(ext_size) (EXTENT_SIZE_TO_TYPE(ext_size) - 1)
#define EXTENT_GROUPID_TO_SIZE(egid) (EXTENT_TYPE_TO_SIZE(egid + 1))
#define EXTENT_GROUPID_TO_TYPE(egid) (egid + 1)

/* Segment page store xlog-related function */
extern void eg_init_segment_head_buffer_content(Buffer seg_head_buffer, BlockNumber seg_head_blocknum, XLogRecPtr lsn);
extern void eg_init_bitmap_page_content(Page bitmap_page, BlockNumber first_page);

/* System view */
static const int SEGMENT_SPACE_INFO_VIEW_COL_NUM = 8;
static const int SEGMENT_SPACE_EXTENT_USAGE_COL_NUM = 5;

SegmentSpaceStat spc_storage_stat(SegSpace *spc, int group_id, ForkNumber forknum);

#endif
