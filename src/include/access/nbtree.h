/* -------------------------------------------------------------------------
 *
 * nbtree.h
 *	  header file for openGauss btree access method implementation.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/nbtree.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NBTREE_H
#define NBTREE_H

#include "access/cstore_am.h"
#include "access/genam.h"
#include "access/itup.h"
#include "access/sdir.h"
#include "access/xlogreader.h"
#include "catalog/pg_index.h"
#include "lib/stringinfo.h"
#include "storage/buf/bufmgr.h"
#include "utils/tuplesort.h"
#include "access/relscan.h"
#include "nodes/execnodes.h"

#ifdef USE_SPQ
#include "access/spq_btbuild.h"
#endif

/* There's room for a 16-bit vacuum cycle ID in BTPageOpaqueData */
typedef uint16 BTCycleId;

/*
 *	BTPageOpaqueData -- At the end of every page, we store a pointer
 *	to both siblings in the tree.  This is used to do forward/backward
 *	index scans.  The next-page link is also critical for recovery when
 *	a search has navigated to the wrong page due to concurrent page splits
 *	or deletions; see src/backend/access/nbtree/README for more info.
 *
 *	In addition, we store the page's btree level (counting upwards from
 *	zero at a leaf page) as well as some flag bits indicating the page type
 *	and status.  If the page is deleted, we replace the level with the
 *	next-transaction-ID value indicating when it is safe to reclaim the page.
 *
 *	We also store a "vacuum cycle ID".	When a page is split while VACUUM is
 *	processing the index, a nonzero value associated with the VACUUM run is
 *	stored into both halves of the split page.	(If VACUUM is not running,
 *	both pages receive zero cycleids.)	This allows VACUUM to detect whether
 *	a page was split since it started, with a small probability of false match
 *	if the page was last split some exact multiple of MAX_BT_CYCLE_ID VACUUMs
 *	ago.  Also, during a split, the BTP_SPLIT_END flag is cleared in the left
 *	(original) page, and set in the right page, but only if the next page
 *	to its right has a different cycleid.
 *
 *	NOTE: the BTP_LEAF flag bit is redundant since level==0 could be tested
 *	instead.
 */

typedef struct BTPageOpaqueDataInternal {
    BlockNumber btpo_prev; /* left sibling, or P_NONE if leftmost */
    BlockNumber btpo_next; /* right sibling, or P_NONE if rightmost */
    union {
        uint32 level;                /* tree level --- zero for leaf pages */
        ShortTransactionId xact_old; /* next transaction ID, if deleted */
    } btpo;
    uint16 btpo_flags;      /* flag bits, see below */
    BTCycleId btpo_cycleid; /* vacuum cycle ID of latest split */
} BTPageOpaqueDataInternal;

typedef BTPageOpaqueDataInternal* BTPageOpaqueInternal;

typedef struct BTPageOpaqueData {
    BTPageOpaqueDataInternal bt_internal;
    TransactionId xact; /* next transaction ID, if deleted */
} BTPageOpaqueData;

typedef BTPageOpaqueData* BTPageOpaque;

/*
 * UBTPageOpaqueInternal can be directly converted to BTPageOpaqueInternal for use, but
 * UBTPageOpaque can't convert to BTPageOpaque. Must used carefully.
 */
typedef struct UBTPageOpaqueDataInternal {
    BlockNumber btpo_prev; /* left sibling, or P_NONE if leftmost */
    BlockNumber btpo_next; /* right sibling, or P_NONE if rightmost */
    union {
        uint32 level;                /* tree level --- zero for leaf pages */
        ShortTransactionId xact_old; /* next transaction ID, if deleted */
    } btpo;
    uint16 btpo_flags;      /* flag bits, see below */
    BTCycleId btpo_cycleid; /* vacuum cycle ID of latest split */

    TransactionId last_delete_xid;
    TransactionId xid_base;
    int16 activeTupleCount;
} UBTPageOpaqueDataInternal;

typedef UBTPageOpaqueDataInternal* UBTPageOpaqueInternal;

typedef struct UBTPageOpaqueData {
    UBTPageOpaqueDataInternal bt_internal;
    TransactionId xact; /* next transaction ID, if deleted */
} UBTPageOpaqueData;

typedef UBTPageOpaqueData* UBTPageOpaque;

/*
 * UBTPCRPageOpaqueData
 */
typedef struct {
    BlockNumber btpo_prev; /* left sibling, or P_NONE if leftmost */
    BlockNumber btpo_next; /* right sibling, or P_NONE if rightmost */
    union {
        uint32 level;                /* tree level --- zero for leaf pages */
        ShortTransactionId xact_old; /* next transaction ID, if deleted */
    } btpo;
    uint16 btpo_flags;      /* flag bits, see below */
    BTCycleId btpo_cycleid; /* vacuum cycle ID of latest split */

    TransactionId xact; /* next transaction id, if deleted */
    TransactionId last_delete_xid;
    TransactionId last_commit_xid;
    TransactionId last_prune_xid;
    uint8 td_count;
    uint16 activeTupleCount;
    uint32 flags;
} UBTPCRPageOpaqueData;
typedef UBTPCRPageOpaqueData* UBTPCRPageOpaque;

#define BTPageGetOpaqueInternal(page) ((BTPageOpaqueInternal) PageGetSpecialPointer(page))

typedef struct BTDedupIntervalData {
    OffsetNumber base_off;
    uint16 num_items;
} BTDedupIntervalData;

typedef BTDedupIntervalData* BTDedupInterval;

typedef struct BTDedupStateData {
    bool deduplicate;
    int num_max_items;
    Size max_posting_size;

    IndexTuple base;
    OffsetNumber base_off;
    Size base_tuple_size;

    ItemPointer heap_tids;
    int num_heap_tids;
    int num_items;
    Size size_freed;

    int num_intervals;
    BTDedupIntervalData intervals[MaxIndexTuplesPerPage];
} BTDedupStateData;

typedef BTDedupStateData *BTDedupState;

typedef struct BTVacuumPostingData
{
    IndexTuple	itup;
    OffsetNumber updated_offset;

    uint16		num_deleted_tids;
    uint16		delete_tids[FLEXIBLE_ARRAY_MEMBER];
} BTVacuumPostingData;

typedef BTVacuumPostingData *BTVacuumPosting;

#define MAX_TIDS_PER_BTREE_PAGE \
    (int) ((BLCKSZ - SizeOfPageHeaderData - sizeof(BTPageOpaqueData)) / \
		   sizeof(ItemPointerData))

/* Bits defined in btpo_flags */
#define BTP_LEAF (1 << 0)        /* leaf page, i.e. not internal page */
#define BTP_ROOT (1 << 1)        /* root page (has no parent) */
#define BTP_DELETED (1 << 2)     /* page has been deleted from tree */
#define BTP_META (1 << 3)        /* meta-page */
#define BTP_HALF_DEAD (1 << 4)   /* empty, but still in tree */
#define BTP_SPLIT_END (1 << 5)   /* rightmost page of split group */
#define BTP_HAS_GARBAGE (1 << 6) /* page has LP_DEAD tuples */
#define BTP_INCOMPLETE_SPLIT (1 << 7)	/* right sibling's downlink is missing */
#define BTP_VACUUM_DELETING (1 << 8)    /* vacuum worker is deleting this page */

/*
 * The max allowed value of a cycle ID is a bit less than 64K.	This is
 * for convenience of pg_filedump and similar utilities: we want to use
 * the last 2 bytes of special space as an index type indicator, and
 * restricting cycle ID lets btree use that space for vacuum cycle IDs
 * while still allowing index type to be identified.
 */
#define MAX_BT_CYCLE_ID 0xFF7F

/*
 * The Meta page is always the first page in the btree index.
 * Its primary purpose is to point to the location of the btree root page.
 * We also point to the "fast" root, which is the current effective root;
 * see README for discussion.
 */

typedef struct BTMetaPageData {
    uint32 btm_magic;         /* should contain BTREE_MAGIC */
    uint32 btm_version;       /* should contain BTREE_VERSION */
    BlockNumber btm_root;     /* current root location */
    uint32 btm_level;         /* tree level of the root page */
    BlockNumber btm_fastroot; /* current "fast" root location */
    uint32 btm_fastlevel;     /* tree level of the "fast" root page */
    bool btm_allequalimage;
    uint32 btm_reltoastrelid; /* uheap toast oid used for pcr ubtree */
} BTMetaPageData;

#define BTPageGetMeta(p) ((BTMetaPageData*)PageGetContents(p))

#define BTREE_METAPAGE 0     /* first page is meta */
#define BTREE_MAGIC 0x053162 /* magic number of btree pages */
#define BTREE_VERSION 3      /* current version number */
#define BTREE_MIN_VERSION 2
#define BTREE_OLD_VERSION 2  /* old btree version with all meta fields set */
#define UBTREE_VERSION     2 /* current ubtree rcr version number */
#define UBTREE_PCR_VERSION 4 /* current ubtree pcr version number */

/* Upgrade support for btree split/delete optimization. */
#define BTREE_SPLIT_DELETE_UPGRADE_VERSION 92136
#define BTREE_SPLIT_UPGRADE_FLAG 0x01
#define BTREE_SPLIT_OPAQUE_FLAG 0x01
#define BTREE_DELETE_UPGRADE_FLAG XLR_BTREE_UPGRADE_FLAG
#define BTREE_DEDUPLICATION_FLAG 0x04

/*
 * Maximum size of a btree index entry, including its tuple header.
 *
 * We actually need to be able to fit three items on every page,
 * so restrict any one item to 1/3 the per-page available space.
 */
#define BTREE_MAX_ITEM_SIZE(page)                                                                          \
	(MAXALIGN_DOWN((PageGetPageSize(page) - \
					MAXALIGN(SizeOfPageHeaderData + 3*sizeof(ItemIdData) + 3*sizeof(ItemPointerData)) - \
					MAXALIGN(sizeof(BTPageOpaqueData))) / 3))

#define BTREE_MAX_ITEM_SIZE_NO_HEAP_TID(page)                                                                          \
	MAXALIGN_DOWN((PageGetPageSize(page) - \
				   MAXALIGN(SizeOfPageHeaderData + 3*sizeof(ItemIdData)) - \
				   MAXALIGN(sizeof(BTPageOpaqueData))) / 3)

/*
 * The leaf-page fillfactor defaults to 90% but is user-adjustable.
 * For pages above the leaf level, we use a fixed 70% fillfactor.
 * The fillfactor is applied during index build and when splitting
 * a rightmost page; when splitting non-rightmost pages we try to
 * divide the data equally.
 */
#define BTREE_MIN_FILLFACTOR 10
#define BTREE_DEFAULT_FILLFACTOR 90
#define BTREE_NONLEAF_FILLFACTOR 70
#define BTREE_SINGLEVAL_FILLFACTOR	96

/*
 *	Test whether two btree entries are "the same".
 *
 *	Old comments:
 *	In addition, we must guarantee that all tuples in the index are unique,
 *	in order to satisfy some assumptions in Lehman and Yao.  The way that we
 *	do this is by generating a new OID for every insertion that we do in the
 *	tree.  This adds eight bytes to the size of btree index tuples.  Note
 *	that we do not use the OID as part of a composite key; the OID only
 *	serves as a unique identifier for a given index tuple (logical position
 *	within a page).
 *
 *	New comments:
 *	actually, we must guarantee that all tuples in A LEVEL
 *	are unique, not in ALL INDEX. So, we can use the t_tid
 *	as unique identifier for a given index tuple (logical position
 *	within a level). - vadim 04/09/97
 */
#define BTTidSame(i1, i2)                                                                        \
    ((i1).ip_blkid.bi_hi == (i2).ip_blkid.bi_hi && (i1).ip_blkid.bi_lo == (i2).ip_blkid.bi_lo && \
        (i1).ip_posid == (i2).ip_posid)
#define BTEntrySame(i1, i2) BTTidSame((i1)->t_tid, (i2)->t_tid)

/*
 *	In general, the btree code tries to localize its knowledge about
 *	page layout to a couple of routines.  However, we need a special
 *	value to indicate "no page number" in those places where we expect
 *	page numbers.  We can use zero for this because we never need to
 *	make a pointer to the metadata page.
 */

#define P_NONE 0

/*
 * Macros to test whether a page is leftmost or rightmost on its tree level,
 * as well as other state info kept in the opaque data.
 */
#define P_LEFTMOST(opaque) ((opaque)->btpo_prev == P_NONE)
#define P_RIGHTMOST(opaque) ((opaque)->btpo_next == P_NONE)
#define P_ISLEAF(opaque) ((opaque)->btpo_flags & BTP_LEAF)
#define P_ISROOT(opaque) ((opaque)->btpo_flags & BTP_ROOT)
#define P_ISDELETED(opaque) ((opaque)->btpo_flags & BTP_DELETED)
#define P_ISMETA(opaque) (((opaque)->btpo_flags & BTP_META) != 0)
#define P_ISHALFDEAD(opaque) ((opaque)->btpo_flags & BTP_HALF_DEAD)
#define P_IGNORE(opaque) ((opaque)->btpo_flags & (BTP_DELETED | BTP_HALF_DEAD))
#define P_HAS_GARBAGE(opaque) ((opaque)->btpo_flags & BTP_HAS_GARBAGE)
#define P_INCOMPLETE_SPLIT(opaque) ((opaque)->btpo_flags & BTP_INCOMPLETE_SPLIT)
#define P_VACUUM_DELETING(opaque) ((opaque)->btpo_flags & BTP_VACUUM_DELETING)

/*
 *	Lehman and Yao's algorithm requires a ``high key'' on every non-rightmost
 *	page.  The high key is not a data key, but gives info about what range of
 *	keys is supposed to be on this page.  The high key on a page is required
 *	to be greater than or equal to any data key that appears on the page.
 *	If we find ourselves trying to insert a key > high key, we know we need
 *	to move right (this should only happen if the page was split since we
 *	examined the parent page).
 *
 *	Our insertion algorithm guarantees that we can use the initial least key
 *	on our right sibling as the high key.  Once a page is created, its high
 *	key changes only if the page is split.
 *
 *	On a non-rightmost page, the high key lives in item 1 and data items
 *	start in item 2.  Rightmost pages have no high key, so we store data
 *	items beginning in item 1.
 */

#define P_HIKEY ((OffsetNumber)1)
#define P_FIRSTKEY ((OffsetNumber)2)
#define P_FIRSTDATAKEY(opaque) (P_RIGHTMOST(opaque) ? P_HIKEY : P_FIRSTKEY)

/*
 * XLOG records for btree operations
 *
 * XLOG allows to store some information in high 4 bits of log
 * record xl_info field
 */
#define XLOG_BTREE_INSERT_LEAF 0x00      /* add index tuple without split */
#define XLOG_BTREE_INSERT_UPPER 0x10     /* same, on a non-leaf page */
#define XLOG_BTREE_INSERT_META 0x20      /* same, plus update metapage */
#define XLOG_BTREE_SPLIT_L 0x30          /* add index tuple with split */
#define XLOG_BTREE_SPLIT_R 0x40          /* as above, new item on right */
#define XLOG_BTREE_SPLIT_L_ROOT 0x50     /* add tuple with split of root */
#define XLOG_BTREE_SPLIT_R_ROOT 0x60     /* as above, new item on right */
#define XLOG_BTREE_DELETE 0x70           /* delete leaf index tuples for a page */
#define XLOG_BTREE_UNLINK_PAGE 0x80      /* delete an entire page */
#define XLOG_BTREE_UNLINK_PAGE_META 0x90 /* same, and update metapage */
#define XLOG_BTREE_NEWROOT 0xA0          /* new root page */
#define XLOG_BTREE_MARK_PAGE_HALFDEAD  \
    0xB0 /* page deletion that makes \
          * parent half-dead */
#define XLOG_BTREE_VACUUM                   \
    0xC0 /* delete entries on a page during \
          * vacuum */
#define XLOG_BTREE_REUSE_PAGE                   \
    0xD0 /* old page is about to be reused from \
          * FSM */
#define XLOG_BTREE_INSERT_POST 0xE0
#define XLOG_BTREE_DEDUP 0xF0


enum {
    BTREE_INSERT_ORIG_BLOCK_NUM = 0,
    BTREE_INSERT_CHILD_BLOCK_NUM,
    BTREE_INSERT_META_BLOCK_NUM,
};

enum {
    BTREE_SPLIT_LEFT_BLOCK_NUM = 0,
    BTREE_SPLIT_RIGHT_BLOCK_NUM,
    BTREE_SPLIT_RIGHTNEXT_BLOCK_NUM,
    BTREE_SPLIT_CHILD_BLOCK_NUM
};

enum {
    BTREE_VACUUM_ORIG_BLOCK_NUM = 0,
};

enum {
    BTREE_DELETE_ORIG_BLOCK_NUM = 0,
};

enum {
    BTREE_HALF_DEAD_LEAF_PAGE_NUM = 0,
    BTREE_HALF_DEAD_PARENT_PAGE_NUM,
};

enum {
    BTREE_UNLINK_PAGE_CUR_PAGE_NUM = 0,
    BTREE_UNLINK_PAGE_LEFT_NUM,
    BTREE_UNLINK_PAGE_RIGHT_NUM,
    BTREE_UNLINK_PAGE_CHILD_NUM,
    BTREE_UNLINK_PAGE_META_NUM,
};

enum {
    BTREE_NEWROOT_ORIG_BLOCK_NUM = 0,
    BTREE_NEWROOT_LEFT_BLOCK_NUM,
    BTREE_NEWROOT_META_BLOCK_NUM
};

enum {
    BTREE_REUSE_PAGE_BLOCK_NUM = 0,
};

enum ScanMode {
    PCR_SCAN_MODE,
    PBRCR_SCAN_MODE
};

typedef struct xl_btree_metadata_old {
    BlockNumber root;
    uint32 level;
    BlockNumber fastroot;
    uint32 fastlevel;
} xl_btree_metadata_old;

/*
 * All that we need to regenerate the meta-data page
 */
typedef struct xl_btree_metadata {
    BlockNumber root;
    uint32 level;
    BlockNumber fastroot;
    uint32 fastlevel;
    uint32 version;
    bool allequalimage;
} xl_btree_metadata;

#define SizeOfBtreeMetadataNoAllEqualImage (offsetof(xl_btree_metadata, version) + sizeof(uint32))

/*
 * This is what we need to know about simple (without split) insert.
 *
 * This data record is used for INSERT_LEAF, INSERT_UPPER, INSERT_META.
 * Note that INSERT_META implies it's not a leaf page.
 *
 * Backup Blk 0: original page (data contains the inserted tuple)
 * Backup Blk 1: xl_btree_metadata, if INSERT_META
 */
typedef struct xl_btree_insert {
    OffsetNumber offnum;
} xl_btree_insert;

#define SizeOfBtreeInsert (offsetof(xl_btree_insert, offnum) + sizeof(OffsetNumber))

/*
 * On insert with split, we save all the items going into the right sibling
 * so that we can restore it completely from the log record.  This way takes
 * less xlog space than the normal approach, because if we did it standardly,
 * XLogInsert would almost always think the right page is new and store its
 * whole page image.  The left page, however, is handled in the normal
 * incremental-update fashion.
 *
 * Note: the four XLOG_BTREE_SPLIT xl_info codes all use this data record.
 * The _L and _R variants indicate whether the inserted tuple went into the
 * left or right split page (and thus, whether newitemoff and the new item
 * are stored or not).  The _HIGHKEY variants indicate that we've logged
 * explicitly left page high key value, otherwise redo should use right page
 * leftmost key as a left page high key.  _HIGHKEY is specified for internal
 * pages where right page leftmost key is suppressed, and for leaf pages
 * of covering indexes where high key have non-key attributes truncated.
 *
 * Backup Blk 0: original page / new left page
 *
 * The left page's data portion contains the new item, if it's the _L variant.
 * (In the _R variants, the new item is one of the right page's tuples.)
 * If level > 0, an IndexTuple representing the HIKEY of the left page
 * follows.  We don't need this on leaf pages, because it's the same as the
 * leftmost key in the new right page.
 *
 * Backup Blk 1: new right page
 *
 * The right page's data portion contains the right page's tuples in the
 * form used by _bt_restore_page.
 *
 * Backup Blk 2: next block (orig page's rightlink), if any
 */
typedef struct xl_btree_split {
    uint32 level;            /* tree level of page being split */
    OffsetNumber firstright; /* first item moved to right page */
    OffsetNumber newitemoff; /* new item's offset (if placed on left page) */
} xl_btree_split;

#define SizeOfBtreeSplit sizeof(xl_btree_split)

typedef struct xl_btree_split_posting {
    uint32 level;
    OffsetNumber firstright;
    OffsetNumber newitemoff;
    uint16 posting_off;
} xl_btree_split_posting;

#define SizeOfBtreeSplitPosting sizeof(xl_btree_split_posting)

/*
 * This is what we need to know about delete of individual leaf index tuples.
 * The WAL record can represent deletion of any number of index tuples on a
 * single index page when *not* executed by VACUUM.
 *
 * Backup Blk 0: index page
 */
typedef struct xl_btree_delete {
    RelFileNodeOld hnode; /* RelFileNode of the heap the index currently
                        * points at */
    int nitems;

    /* TARGET OFFSET NUMBERS FOLLOW AT THE END */
} xl_btree_delete;

#define SizeOfBtreeDelete (offsetof(xl_btree_delete, nitems) + sizeof(int))

/*
 * This is what we need to know about page reuse within btree.
 */
typedef struct xl_btree_reuse_page {
    RelFileNodeOld node;
    BlockNumber block;
    TransactionId latestRemovedXid;
} xl_btree_reuse_page;

#define SizeOfBtreeReusePage (sizeof(xl_btree_reuse_page))

typedef struct xl_btree_update {
    uint16 num_deleted_tids;
} xl_btree_update;

#define SizeOfBtreeUpdate (sizeof(xl_btree_update))

typedef struct xl_btree_dedup {
    uint16 num_intervals;
} xl_btree_dedup;

#define SizeOfBtreeDedup (sizeof(xl_btree_dedup))

/*
 * This is what we need to know about vacuum of individual leaf index tuples.
 * The WAL record can represent deletion of any number of index tuples on a
 * single index page when executed by VACUUM.
 *
 * The correctness requirement for applying these changes during recovery is
 * that we must do one of these two things for every block in the index:
 *		* lock the block for cleanup and apply any required changes
 *		* EnsureBlockUnpinned()
 * The purpose of this is to ensure that no index scans started before we
 * finish scanning the index are still running by the time we begin to remove
 * heap tuples.
 *
 * Any changes to any one block are registered on just one WAL record. All
 * blocks that we need to run EnsureBlockUnpinned() are listed as a block range
 * starting from the last block vacuumed through until this one. Individual
 * block numbers aren't given.
 *
 * Note that the *last* WAL record in any vacuum of an index is allowed to
 * have a zero length array of offsets. Earlier records must have at least one.
 */
typedef struct xl_btree_vacuum {
    BlockNumber lastBlockVacuumed;

    /* TARGET OFFSET NUMBERS FOLLOW */
} xl_btree_vacuum;

#define SizeOfBtreeVacuum (offsetof(xl_btree_vacuum, lastBlockVacuumed) + sizeof(BlockNumber))

typedef struct xl_btree_vacuum_posting {
    BlockNumber lastBlockVacuumed;
    uint16 num_deleted;
    uint16 num_updated;
} xl_btree_vacuum_posting;

#define SizeOfBtreeVacuumPosting (offsetof(xl_btree_vacuum_posting, num_updated) + sizeof(uint16))

/*
 * This is what we need to know about deletion of a btree page.  The target
 * identifies the tuple removed from the parent page (note that we remove
 * this tuple's downlink and the *following* tuple's key).	Note we do not
 * store any content for the deleted page --- it is just rewritten as empty
 * during recovery, apart from resetting the btpo.xact.
 *
 * Backup Blk 0: target block being deleted
 * Backup Blk 1: target block's left sibling, if any
 * Backup Blk 2: target block's right sibling
 * Backup Blk 3: target block's parent
 * Backup Blk 4: metapage (if rightsib becomes new fast root)
 */
typedef struct xl_btree_delete_page {
    OffsetNumber poffset;    /* deleted tuple id in parent page */
    BlockNumber leftblk;     /* child block's left sibling, if any */
    BlockNumber rightblk;    /* child block's right sibling */
    TransactionId btpo_xact; /* value of btpo.xact for use in recovery */
                             /* xl_btree_metadata FOLLOWS IF XLOG_BTREE_DELETE_PAGE_META */
} xl_btree_delete_page;

#define SizeOfBtreeDeletePage (offsetof(xl_btree_delete_page, btpo_xact) + sizeof(TransactionId))


/*
 * This is what we need to know about marking an empty branch for deletion.
 * The target identifies the tuple removed from the parent page (note that we
 * remove this tuple's downlink and the *following* tuple's key).  Note that
 * the leaf page is empty, so we don't need to store its content --- it is
 * just reinitialized during recovery using the rest of the fields.
 *
 * Backup Blk 0: leaf block
 * Backup Blk 1: top parent
 */
typedef struct xl_btree_mark_page_halfdead {
    OffsetNumber poffset; /* deleted tuple id in parent page */

    /* information needed to recreate the leaf page: */
    BlockNumber leafblk;   /* leaf block ultimately being deleted */
    BlockNumber leftblk;   /* leaf block's left sibling, if any */
    BlockNumber rightblk;  /* leaf block's right sibling */
    BlockNumber topparent; /* topmost internal page in the branch */
} xl_btree_mark_page_halfdead;

#define SizeOfBtreeMarkPageHalfDead (offsetof(xl_btree_mark_page_halfdead, topparent) + sizeof(BlockNumber))

/*
 * This is what we need to know about deletion of a btree page.  Note we do
 * not store any content for the deleted page --- it is just rewritten as empty
 * during recovery, apart from resetting the btpo.xact.
 *
 * Backup Blk 0: target block being deleted
 * Backup Blk 1: target block's left sibling, if any
 * Backup Blk 2: target block's right sibling
 * Backup Blk 3: leaf block (if different from target)
 * Backup Blk 4: metapage (if rightsib becomes new fast root)
 */
typedef struct xl_btree_unlink_page {
    BlockNumber leftsib;  /* target block's left sibling, if any */
    BlockNumber rightsib; /* target block's right sibling */

    /*
     * Information needed to recreate the leaf page, when target is an
     * internal page.
     */
    BlockNumber leafleftsib;
    BlockNumber leafrightsib;
    BlockNumber topparent; /* next child down in the branch */
    TransactionId btpo_xact; /* value of btpo.xact for use in recovery */
                             /* xl_btree_metadata FOLLOWS IF XLOG_BTREE_UNLINK_PAGE_META */
} xl_btree_unlink_page;

#define SizeOfBtreeUnlinkPage	(offsetof(xl_btree_unlink_page, btpo_xact) + sizeof(TransactionId))

/*
 * New root log record.  There are zero tuples if this is to establish an
 * empty root, or two if it is the result of splitting an old root.
 *
 * Note that although this implies rewriting the metadata page, we don't need
 * an xl_btree_metadata record --- the rootblk and level are sufficient.
 *
 * Backup Blk 0: new root page (2 tuples as payload, if splitting old root)
 * Backup Blk 1: metapage
 */
typedef struct xl_btree_newroot {
    BlockNumber rootblk; /* location of new root (redundant with blk 0) */
    uint32 level;        /* its tree level */
                         /* 0 or 2 INDEX TUPLES FOLLOW AT END OF STRUCT */
} xl_btree_newroot;

#define SizeOfBtreeNewroot (offsetof(xl_btree_newroot, level) + sizeof(uint32))

/*
 * INCLUDE B-Tree indexes have non-key attributes.  These are extra
 * attributes that may be returned by index-only scans, but do not influence
 * the order of items in the index (formally, non-key attributes are not
 * considered to be part of the key space).  Non-key attributes are only
 * present in leaf index tuples whose item pointers actually point to heap
 * tuples.  All other types of index tuples (collectively, "pivot" tuples)
 * only have key attributes, since pivot tuples only ever need to represent
 * how the key space is separated.  In general, any B-Tree index that has
 * more than one level (i.e. any index that does not just consist of a
 * metapage and a single leaf root page) must have some number of pivot
 * tuples, since pivot tuples are used for traversing the tree.
 *
 * We store the number of attributes present inside pivot tuples by abusing
 * their item pointer offset field, since pivot tuples never need to store a
 * real offset (downlinks only need to store a block number).  The offset
 * field only stores the number of attributes when the INDEX_ALT_TID_MASK
 * bit is set (we never assume that pivot tuples must explicitly store the
 * number of attributes, and currently do not bother storing the number of
 * attributes unless indnkeyatts actually differs from indnatts).
 * INDEX_ALT_TID_MASK is only used for pivot tuples at present, though it's
 * possible that it will be used within non-pivot tuples in the future.  Do
 * not assume that a tuple with INDEX_ALT_TID_MASK set must be a pivot
 * tuple.
 *
 * The 12 least significant offset bits are used to represent the number of
 * attributes in INDEX_ALT_TID_MASK tuples, leaving 4 bits that are reserved
 * for future use (BT_RESERVED_OFFSET_MASK bits). BT_N_KEYS_OFFSET_MASK should
 * be large enough to store any number <= INDEX_MAX_KEYS.
 */
#define INDEX_ALT_TID_MASK INDEX_AM_RESERVED_BIT
#define BT_STATUS_OFFSET_MASK 0xF000
#define BT_N_KEYS_OFFSET_MASK 0x0FFF
#define BT_PIVOT_HEAP_TID_ATTR 0x1000
#define BT_IS_POSTING 0x2000

/* Get/set downlink block number */
#define BTreeInnerTupleGetDownLink(itup) ItemPointerGetBlockNumberNoCheck(&((itup)->t_tid))
#define BTreeInnerTupleSetDownLink(itup, blkno) ItemPointerSetBlockNumber(&((itup)->t_tid), (blkno))

static inline bool btree_tuple_is_pivot(IndexTuple itup)
{
    if ((itup->t_info & INDEX_ALT_TID_MASK) == 0) {
        return false;
    }

    if ((ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) & BT_IS_POSTING) != 0) {
        return false;
    }

    return true;
}

static inline bool btree_tuple_is_posting(IndexTuple itup)
{
    if ((itup->t_info & INDEX_ALT_TID_MASK) == 0) {
        return false;
    }

    if ((ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) & BT_IS_POSTING) == 0) {
        return false;
    }

    return true;
}

static inline void btree_tuple_set_posting(IndexTuple itup, uint16 num_heap_tids, int posting_off)
{
    Assert(num_heap_tids > 1);
    Assert((num_heap_tids & BT_STATUS_OFFSET_MASK) == 0);
    Assert((size_t)posting_off == MAXALIGN(posting_off));
    Assert(posting_off < INDEX_SIZE_MASK);
    Assert(!btree_tuple_is_pivot(itup));

    itup->t_info |= INDEX_ALT_TID_MASK;
    ItemPointerSetOffsetNumber(&itup->t_tid, (num_heap_tids | BT_IS_POSTING));
    ItemPointerSetBlockNumber(&itup->t_tid, posting_off);
}

static inline uint32 btree_tuple_get_posting_off(IndexTuple posting)
{
    Assert(btree_tuple_is_posting(posting));

    return ItemPointerGetBlockNumberNoCheck(&posting->t_tid);
}

static inline ItemPointer btree_tuple_get_posting(IndexTuple posting)
{
    return (ItemPointer)((char *)posting + btree_tuple_get_posting_off(posting));
}

static inline ItemPointer btree_tuple_get_posting_n(IndexTuple posting, int n)
{
    return btree_tuple_get_posting(posting) + n;
}

static inline uint16 btree_tuple_get_nposting(IndexTuple posting)
{
    Assert(btree_tuple_is_posting(posting));

    OffsetNumber existing = ItemPointerGetOffsetNumberNoCheck(&posting->t_tid);
    return (existing & BT_N_KEYS_OFFSET_MASK);
}

static inline bool btree_do_dedup(Relation heap_rel, Relation index_rel)
{
    Assert(index_rel->rd_rel->relkind == RELKIND_INDEX && index_rel->rd_rel->relam == BTREE_AM_OID);

    if (t_thrd.proc->workingVersionNum < NBTREE_DEDUPLICATION_VERSION_NUM) {
        return false;
    }

    if (RelationIsPartition(heap_rel)) {
        return false;
    }

    if (index_rel->rd_options == NULL) {
        return g_instance.attr.attr_common.enable_default_index_deduplication;
    }

    return ((StdRdOptions *)index_rel->rd_options)->deduplication;
}

static inline ItemPointer btree_tuple_get_heap_tid(IndexTuple itup)
{
    if (btree_tuple_is_pivot(itup)) {
        if ((ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) & BT_PIVOT_HEAP_TID_ATTR) != 0)
            return (ItemPointer)((char *)itup + IndexTupleSize(itup) - sizeof(ItemPointerData));
        return NULL;
    } else if (btree_tuple_is_posting(itup)) {
        return btree_tuple_get_posting(itup);
    }

    return &itup->t_tid;
}

static inline ItemPointer btree_tuple_get_max_heap_tid(IndexTuple itup)
{
    Assert(!btree_tuple_is_pivot(itup));

    if (btree_tuple_is_posting(itup)) {
        uint16 nposting = btree_tuple_get_nposting(itup);
        return btree_tuple_get_posting_n(itup, nposting - 1);
    }

    return &itup->t_tid;
}

#define BTREE_TUPLE_GET_NUM_OF_ATTS(itup, rel)                                                                  \
    ((btree_tuple_is_pivot(itup)) ? (ItemPointerGetOffsetNumberNoCheck(&(itup)->t_tid) & BT_N_KEYS_OFFSET_MASK) \
                                  : IndexRelationGetNumberOfAttributes(rel))

static inline void btree_tuple_set_num_of_atts(IndexTuple itup, uint16 nkeyatts, bool heaptid)
{
    Assert(nkeyatts <= INDEX_MAX_KEYS);
    Assert((nkeyatts & BT_STATUS_OFFSET_MASK) == 0);
    Assert(!heaptid || nkeyatts > 0);
    Assert(!btree_tuple_is_pivot(itup) || nkeyatts == 0);

    itup->t_info |= INDEX_ALT_TID_MASK;

    if (heaptid)
        nkeyatts |= BT_PIVOT_HEAP_TID_ATTR;

    /* BT_IS_POSTING bit is deliberately unset here */
    ItemPointerSetOffsetNumber(&itup->t_tid, nkeyatts);
    Assert(btree_tuple_is_pivot(itup));
}

static inline BlockNumber btree_tuple_get_top_parent(IndexTuple leafhikey)
{
    return ItemPointerGetBlockNumberNoCheck(&leafhikey->t_tid);
}

static inline void btree_tuple_set_top_parent(IndexTuple leafhikey, BlockNumber blkno)
{
    ItemPointerSetBlockNumber(&leafhikey->t_tid, blkno);
    btree_tuple_set_num_of_atts(leafhikey, 0, false);
}

/*
 *	Operator strategy numbers for B-tree have been moved to access/skey.h,
 *	because many places need to use them in ScanKeyInit() calls.
 *
 *	The strategy numbers are chosen so that we can commute them by
 *	subtraction, thus:
 */
#define BTCommuteStrategyNumber(strat) (BTMaxStrategyNumber + 1 - (strat))

/*
 *	When a new operator class is declared, we require that the user
 *	supply us with an amproc procedure (BTORDER_PROC) for determining
 *	whether, for two keys a and b, a < b, a = b, or a > b.	This routine
 *	must return < 0, 0, > 0, respectively, in these three cases.  (It must
 *	not return INT_MIN, since we may negate the result before using it.)
 *
 *	To facilitate accelerated sorting, an operator class may choose to
 *	offer a second procedure (BTSORTSUPPORT_PROC).	For full details, see
 *	src/include/utils/sortsupport.h.
 */

#define BTORDER_PROC 1
#define BTSORTSUPPORT_PROC 2
#define BTEQUALIMAGE_PROC 3

/*
 *	We need to be able to tell the difference between read and write
 *	requests for pages, in order to do locking correctly.
 */

#define BT_READ BUFFER_LOCK_SHARE
#define BT_WRITE BUFFER_LOCK_EXCLUSIVE
/* BT_NO_LOCK is actually BUFFER_LOCK_UNLOCK(0 value) and should be used carefully */
#define BT_NO_LOCK BUFFER_LOCK_UNLOCK

/*
 *	BTStackData -- As we descend a tree, we push the (location, downlink)
 *	pairs from internal pages onto a private stack.  If we split a
 *	leaf, we use this stack to walk back up the tree and insert data
 *	into parent pages (and possibly to split them, too).  Lehman and
 *	Yao's update algorithm guarantees that under no circumstances can
 *	our private stack give us an irredeemably bad picture up the tree.
 *	Again, see the paper for details.
 */

typedef struct BTStackData {
    BlockNumber bts_blkno;
    OffsetNumber bts_offset;
    BlockNumber bts_btentry;
    struct BTStackData* bts_parent;
} BTStackData;

typedef BTStackData* BTStack;

/*
 * BTScanOpaqueData is the btree-private state needed for an indexscan.
 * This consists of preprocessed scan keys (see _bt_preprocess_keys() for
 * details of the preprocessing), information about the current location
 * of the scan, and information about the marked location, if any.	(We use
 * BTScanPosData to represent the data needed for each of current and marked
 * locations.)	In addition we can remember some known-killed index entries
 * that must be marked before we can move off the current page.
 *
 * Index scans work a page at a time: we pin and read-lock the page, identify
 * all the matching items on the page and save them in BTScanPosData, then
 * release the read-lock while returning the items to the caller for
 * processing.	This approach minimizes lock/unlock traffic.  Note that we
 * keep the pin on the index page until the caller is done with all the items
 * (this is needed for VACUUM synchronization, see nbtree/README).	When we
 * are ready to step to the next page, if the caller has told us any of the
 * items were killed, we re-lock the page to mark them killed, then unlock.
 * Finally we drop the pin and step to the next page in the appropriate
 * direction.
 *
 * If we are doing an index-only scan, we save the entire IndexTuple for each
 * matched item, otherwise only its heap TID and offset.  The IndexTuples go
 * into a separate workspace array; each BTScanPosItem stores its tuple's
 * offset within that array.
 */

typedef struct BTScanPosItem { /* what we remember about each match */
    ItemPointerData heapTid;   /* TID of referenced heap item */
    OffsetNumber indexOffset;  /* index item's location within page */
    LocationIndex tupleOffset; /* IndexTuple's offset in workspace, if any */
    Oid partitionOid;          /* partition table oid in workspace, if any */
    int2 bucketid;             /* bucketid in workspace, if any */
    /* only used in ubtree */
    bool needRecheck;
} BTScanPosItem;

typedef struct BTScanPosData {
    Buffer buf; /* if valid, the buffer is pinned */

    BlockNumber nextPage; /* page's right link when we scanned it */
    BlockNumber currPage;       /* page referenced by items array */

    TransactionId xid_base;

    /*
     * moreLeft and moreRight track whether we think there may be matching
     * index entries to the left and right of the current page, respectively.
     * We can clear the appropriate one of these flags when _bt_checkkeys()
     * returns continuescan = false.
     */
    bool moreLeft;
    bool moreRight;

    /*
     * If we are doing an index-only scan, nextTupleOffset is the first free
     * location in the associated tuple storage workspace.
     */
    int nextTupleOffset;

    /*
     * The items array is always ordered in index order (ie, increasing
     * indexoffset).  When scanning backwards it is convenient to fill the
     * array back-to-front, so we start at the last slot and fill downwards.
     * Hence we need both a first-valid-entry and a last-valid-entry counter.
     * itemIndex is a cursor showing which entry was last returned to caller.
     */
    int firstItem; /* first valid index in items[] */
    int lastItem;  /* last valid index in items[] */
    int itemIndex; /* current index in items[] */

    BTScanPosItem items[MAX_TIDS_PER_BTREE_PAGE]; /* MUST BE LAST */
} BTScanPosData;

typedef BTScanPosData* BTScanPos;

#define BTScanPosIsValid(scanpos) BufferIsValid((scanpos).buf)

/* We need one of these for each equality-type SK_SEARCHARRAY scan key */
typedef struct BTArrayKeyInfo {
    int scan_key;       /* index of associated key in arrayKeyData */
    int cur_elem;       /* index of current element in elem_values */
    int mark_elem;      /* index of marked element in elem_values */
    int num_elems;      /* number of elems in current array value */
    Datum* elem_values; /* array of num_elems Datums */
} BTArrayKeyInfo;

typedef struct BTScanOpaqueData {
    /* these fields are set by _bt_preprocess_keys(): */
    bool qual_ok;     /* false if qual can never be satisfied */
    bool xs_want_xid; /* indicate that the index scan want xid. Put it here for alignment */
    int numberOfKeys; /* number of preprocessed scan keys */
    ScanKey keyData;  /* array of preprocessed scan keys */
    ScanMode scanMode;  /* pcr or pbrcr */

    /* workspace for SK_SEARCHARRAY support */
    ScanKey arrayKeyData;       /* modified copy of scan->keyData */
    int numArrayKeys;           /* number of equality-type array keys (-1 if
                                 * there are any unsatisfiable array keys) */
    int arrayKeyCount;  /* count indicating number of array scan keys
                                 * processed */
    BTArrayKeyInfo* arrayKeys;  /* info about each equality-type array key */
    MemoryContext arrayContext; /* scan-lifespan context for array data */

    /* info about killed items if any (killedItems is NULL if never used) */
    int* killedItems; /* currPos.items indexes of killed items */
    int numKilled;    /* number of currently stored items */

    /*
     * If we are doing an index-only scan, these are the tuple storage
     * workspaces for the currPos and markPos respectively.  Each is of size
     * BLCKSZ, so it can hold as much as a full page's worth of tuples.
     */
    char* currTuples; /* tuple storage for currPos */
    char* markTuples; /* tuple storage for markPos */

    /*
     * used in ubtree only, indicate the last returned index tuple which is modified
     * by current transaction. see UBTreeCheckCid() for more information.
     */
    char* lastSelfModifiedItup;
    uint16 lastSelfModifiedItupBufferSize;
    IndexInfo* indexInfo;
    EState* fakeEstate;

    bool isPageInfoLogged;

    /*
     * If the marked position is on the same page as current position, we
     * don't use markPos, but just keep the marked itemIndex in markItemIndex
     * (all the rest of currPos is valid for the mark position). Hence, to
     * determine if there is a mark, first look at markItemIndex, then at
     * markPos.
     */
    int markItemIndex; /* itemIndex, or -1 if not valid */

    /* keep these last in struct for efficiency */
    BTScanPosData currPos; /* current position data */
    BTScanPosData markPos; /* marked position, if any */
} BTScanOpaqueData;

typedef BTScanOpaqueData* BTScanOpaque;

/*
 * We use some private sk_flags bits in preprocessed scan keys.  We're allowed
 * to use bits 16-31 (see skey.h).	The uppermost bits are copied from the
 * index's indoption[] array entry for the index attribute.
 */
#define SK_BT_REQFWD 0x00010000  /* required to continue forward scan */
#define SK_BT_REQBKWD 0x00020000 /* required to continue backward scan */
#define SK_BT_INDOPTION_SHIFT 24 /* must clear the above bits */
#define SK_BT_DESC (INDOPTION_DESC << SK_BT_INDOPTION_SHIFT)
#define SK_BT_NULLS_FIRST (INDOPTION_NULLS_FIRST << SK_BT_INDOPTION_SHIFT)

/*
 * BTScanInsertData is the btree-private state needed to find an initial
 * position for an indexscan, or to insert new tuples -- an "insertion
 * scankey" (not to be confused with a search scankey).  It's used to descend
 * a B-Tree using _bt_search.
 *
 * heapkeyspace indicates if we expect all keys in the index to be physically
 * unique because heap TID is used as a tiebreaker attribute, and if index may
 * have truncated key attributes in pivot tuples.  This is actually a property
 * of the index relation itself (not an indexscan).  heapkeyspace indexes are
 * indexes whose version is >= version 4.  It's convenient to keep this close
 * by, rather than accessing the metapage repeatedly.
 *
 * anynullkeys indicates if any of the keys had NULL value when scankey was
 * built from index tuple (note that already-truncated tuple key attributes
 * set NULL as a placeholder key value, which also affects value of
 * anynullkeys).  This is a convenience for unique index non-pivot tuple
 * insertion, which usually temporarily unsets scantid, but shouldn't iff
 * anynullkeys is true.  Value generally matches non-pivot tuple's HasNulls
 * bit, but may not when inserting into an INCLUDE index (tuple header value
 * is affected by the NULL-ness of both key and non-key attributes).
 *
 * When nextkey is false (the usual case), _bt_search and _bt_binsrch will
 * locate the first item >= scankey.  When nextkey is true, they will locate
 * the first item > scan key.
 *
 * pivotsearch is set to true by callers that want to re-find a leaf page
 * using a scankey built from a leaf page's high key.  Most callers set this
 * to false.
 *
 * scantid is the heap TID that is used as a final tiebreaker attribute.  It
 * is set to NULL when index scan doesn't need to find a position for a
 * specific physical tuple.  Must be set when inserting new tuples into
 * heapkeyspace indexes, since every tuple in the tree unambiguously belongs
 * in one exact position (it's never set with !heapkeyspace indexes, though).
 * Despite the representational difference, nbtree search code considers
 * scantid to be just another insertion scankey attribute.
 *
 * scankeys is an array of scan key entries for attributes that are compared
 * before scantid (user-visible attributes).  keysz is the size of the array.
 * During insertion, there must be a scan key for every attribute, but when
 * starting a regular index scan some can be omitted.  The array is used as a
 * flexible array member, though it's sized in a way that makes it possible to
 * use stack allocations.  See nbtree/README for full details.
 */
typedef struct BTScanInsertData {
    bool heapkeyspace;
    bool allequalimage;
    bool anynullkeys;
    bool nextkey;
    bool pivotsearch;                     /* seems not used yet (always false) */
    ItemPointer scantid;                  /* tiebreaker for scankeys */
    int keysz;                            /* Size of scankeys array */
    ScanKeyData scankeys[INDEX_MAX_KEYS]; /* Must appear last */
} BTScanInsertData;

typedef BTScanInsertData* BTScanInsert;

// BTPageState and BTWriteState are moved here from nbtsort.cpp
/*
 * Status record for a btree page being built.	We have one of these
 * for each active tree level.
 *
 * The reason we need to store a copy of the minimum key is that we'll
 * need to propagate it to the parent node when this page is linked
 * into its parent.  However, if the page is not a leaf page, the first
 * entry on the page doesn't need to contain a key, so we will not have
 * stored the key itself on the page.  (You might think we could skip
 * copying the minimum key on leaf pages, but actually we must have a
 * writable copy anyway because we'll poke the page's address into it
 * before passing it up to the parent...)
 */
typedef struct BTPageState {
    Page btps_page;                /* workspace for page building */
    BlockNumber btps_blkno;        /* block # to write this page at */
    IndexTuple btps_minkey;        /* copy of minimum key (first item) on page */
    OffsetNumber btps_lastoff;     /* last item offset loaded */
    uint32 btps_level;             /* tree level (0 = leaf) */
    Size btps_full;                /* "full" if less than this much free space */
    Size btps_lastextra;
    struct BTPageState* btps_next; /* link to parent level, if any */
} BTPageState;

/*
 * Overall status record for index writing phase.
 */
typedef struct BTWriteState {
    Relation heap;
    Relation index;
    BTScanInsert inskey;		    /* generic insertion scankey */
    bool btws_use_wal;              /* dump pages to WAL? */
    BlockNumber btws_pages_alloced; /* # pages allocated */
    BlockNumber btws_pages_written; /* # pages written out */
    Page btws_zeropage;             /* workspace for filling zeroes */
#ifdef USE_SPQ
    SPQLeaderState *spqleader;      /* spq btbuild leader */
#endif
} BTWriteState;

typedef struct BTOrderedIndexListElement {
    IndexTuple itup;
    BlockNumber heapModifiedOffset;
    IndexScanDesc indexScanDesc;
} BTOrderedIndexListElement;

typedef struct BTCheckElement {
    Buffer buffer;
    BTStack btStack;
    BTScanInsert itup_key;
    OffsetNumber offset;
    int posting_off;
    int indnkeyatts;
    bool useFastPath;
    BlockNumber targetBlock;
} BTCheckElement;

/*
 * prototypes for functions in nbtsort.c
 */

/*
 * Status record for spooling/sorting phase.  (Note we may have two of
 * these due to the special requirements for uniqueness-checking with
 * dead tuples.)
 */
struct BTSpool {
    Tuplesortstate *sortstate; /* state data for tuplesort.c */
    Relation heap;
    Relation index;
    bool isunique;
};

/*
 * Status for index builds performed in parallel.  This is allocated in a
 * dynamic shared memory segment.  Note that there is a separate tuplesort TOC
 * entry, private to tuplesort.c but allocated by this module on its behalf.
 */
typedef struct BTShared {
    /*
     * These fields are not modified during the sort.  They primarily exist
     * for the benefit of worker processes that need to create BTSpool state
     * corresponding to that used by the leader.
     */
    Oid heaprelid;
    Oid indexrelid;
    Oid heappartid;
    Oid indexpartid;
    bool isunique;
    int scantuplesortstates;

    /*
     * mutex protects all fields before heapdesc.
     *
     * These fields contain status information of interest to B-Tree index
     * builds that must work just the same when an index is built in parallel.
     */
    slock_t mutex;

    /*
     * Mutable state that is maintained by workers, and reported back to
     * leader at end of parallel scan.
     *
     * reltuples is the total number of input heap tuples.
     *
     * havedead indicates if RECENTLY_DEAD tuples were encountered during
     * build.
     *
     * indtuples is the total number of tuples that made it into the index.
     *
     * brokenhotchain indicates if any worker detected a broken HOT chain
     * during build.
     */
    double reltuples;
    double *alltuples;
    uint32 nparts;
    bool isplain;
    bool havedead;
    double indtuples;
    bool brokenhotchain;
    int workmem[2];
    pg_atomic_uint64 curiter;
    Sharedsort *sharedsort;
    Sharedsort *sharedsort2;

    /*
     * This variable-sized field must come last.
     */
    ParallelHeapScanDescData heapdesc;
} BTShared;

/*
 * Status for leader in parallel index build.
 */
typedef struct BTLeader {
    /*
     * nparticipanttuplesorts is the exact number of worker processes
     * successfully launched, plus one leader process if it participates as a
     * worker (only DISABLE_LEADER_PARTICIPATION builds avoid leader
     * participating as a worker).
     */
    int nparticipanttuplesorts;

    /*
     * Leader process convenience pointers to shared state (leader avoids TOC
     * lookups).
     *
     * btshared is the shared state for entire build.  sharedsort is the
     * shared, tuplesort-managed state passed to each process tuplesort.
     * sharedsort2 is the corresponding btspool2 shared state, used only when
     * building unique indexes.
     */
    BTShared *btshared;
} BTLeader;

/* Working state for btbuild and its callback */
typedef struct {
    bool isUnique;
    bool haveDead;
    Relation heapRel;
    BTSpool* spool;

    /*
     * spool2 is needed only when the index is an unique index. Dead tuples
     * are put into spool2 instead of spool in order to avoid uniqueness
     * check.
     */
    BTSpool* spool2;
    double indtuples;

    /*
     * btleader is only present when a parallel index build is performed, and
     * only in the leader process. (Actually, only the leader has a
     * BTBuildState.  Workers have their own spool and spool2, though.)
     */
    BTLeader   *btleader;
#ifdef USE_SPQ
    /* spq btbuild leader */
    SPQLeaderState *spqleader;
#endif
} BTBuildState;

/* 
 * Unified Atomic Iterator, which is used for iterating hashbuckets
 * and partitions atomically in multiple threads. It contains two
 * internal iterators, the first one is for the inner-most layer,
 * the second is for the outer layer. Whether both iterators are
 * valid depends on the granularity of the target relation. If the
 * target relation is a hashbucket, only the first iterator is valid,
 * it is same to paritition-only relation. If the target relation is 
 * the parent table which contains both partitions and hashbuckets,
 * both iterators will be activated and iterate starting from the first
 * bucket of the first partition, then move forward like the second hand
 * and the minute hand of a clock.
 */
typedef union unified_atomic_iterator {
    uint64 value;
    uint32 iters[2];
} unified_atomic_iterator_t;

#define INVALID_ATOMIC_ITERATOR PG_UINT32_MAX

/*
 * prototypes for functions in nbtree.c (external entry points for btree)
 */
extern Datum btbuild(PG_FUNCTION_ARGS);
extern Datum btbuildempty(PG_FUNCTION_ARGS);
extern Datum btinsert(PG_FUNCTION_ARGS);
extern Datum btbeginscan(PG_FUNCTION_ARGS);
extern void* Btbuildparallelscan(void);
extern void Btinitparallelscan(void *target);
extern Datum btgettuple(PG_FUNCTION_ARGS);
extern Datum btgetbitmap(PG_FUNCTION_ARGS);
extern Datum cbtreegetbitmap(PG_FUNCTION_ARGS);
extern Datum btrescan(PG_FUNCTION_ARGS);
extern Datum btendscan(PG_FUNCTION_ARGS);
extern Datum btmarkpos(PG_FUNCTION_ARGS);
extern Datum btrestrpos(PG_FUNCTION_ARGS);
extern Datum btbulkdelete(PG_FUNCTION_ARGS);
extern Datum btvacuumcleanup(PG_FUNCTION_ARGS);
extern Datum btcanreturn(PG_FUNCTION_ARGS);
extern Datum btoptions(PG_FUNCTION_ARGS);

extern void btparallelrescan(IndexScanDesc scan);
extern void Btinitparallelscan(void *target);
/*
 * prototypes for internal functions in nbtree.c
 */
extern bool _bt_parallel_seize(IndexScanDesc scan, BlockNumber *pageno);
extern void _bt_parallel_release(IndexScanDesc scan, BlockNumber scan_page);
extern void _bt_parallel_done(IndexScanDesc scan);
extern void _bt_parallel_advance_array_keys(IndexScanDesc scan);

extern inline IndexBuildResult *btbuild_internal(Relation heap, Relation index, IndexInfo *index_info);

extern inline void btbuildempty_internal(Relation index);

extern inline bool btinsert_internal(Relation rel, Datum *values, const bool *isnull, ItemPointer ht_ctid, Relation heapRel, IndexUniqueCheck checkUnique);

extern inline IndexBulkDeleteResult *btbulkdelete_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback, const void *callback_state);

extern inline IndexBulkDeleteResult *btvacuumcleanup_internal(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);

inline bool btcanreturn_internal()
{
    return true;
}

extern inline IndexScanDesc btbeginscan_internal(Relation rel, int nkeys, int norderbys);

extern inline void btrescan_internal(IndexScanDesc scan, ScanKey scankey);

extern inline int64 btgetbitmap_internal(IndexScanDesc scan, TIDBitmap *tbm);

extern inline void btendscan_internal(IndexScanDesc scan);

extern inline void btmarkpos_internal(IndexScanDesc scan);

extern inline void btrestrpos_internal(IndexScanDesc scan);

extern inline IndexBuildResult *btmerge_internal(Relation dstIdxRel, List *srcIdxRelScans, List *srcPartMergeOffsets);

/*
 * this is the interface of merge 2 or more index for btree index
 * we also have similar interfaces for other kind of indexes, like hash/gist/gin
 * thought, we are not going to implement them right now.
 */
extern Datum btmerge(PG_FUNCTION_ARGS);
/*
 * prototypes for functions in nbtinsert.c
 */
extern bool _bt_doinsert(Relation rel, IndexTuple itup, IndexUniqueCheck checkUnique, Relation heapRel);
extern Buffer _bt_getstackbuf(Relation rel, BTStack stack);
extern void _bt_insert_parent(Relation rel, Buffer buf, Buffer rbuf, BTStack stack, bool is_root, bool is_only);
extern void _bt_finish_split(Relation rel, Buffer bbuf, BTStack stack);
extern TransactionId _bt_check_unique(Relation rel, IndexTuple itup, Relation heapRel, Buffer buf,
    OffsetNumber offset, BTScanInsert itup_key, IndexUniqueCheck checkUnique, bool *is_unique, GPIScanDesc gpiDesc,
    CBIScanDesc cbiScan, CUDescScan* cudesc);
extern bool SearchBufferAndCheckUnique(Relation rel, IndexTuple itup, IndexUniqueCheck checkUnique, Relation heapRel,
    GPIScanDesc gpiScan, CBIScanDesc cbiScan, CUDescScan* cudescScan, BTCheckElement* element);
extern bool CheckPartitionIsInvisible(GPIScanDesc gpiScan);

/*
 * prototypes for functions in nbtpage.c
 */
extern void _bt_initmetapage(Page page, BlockNumber rootbknum, uint32 level, bool allequalimage, bool is_systable);
extern Buffer _bt_getroot(Relation rel, int access);
extern Buffer _bt_gettrueroot(Relation rel);
extern int _bt_getrootheight(Relation rel);
extern void _bt_checkbuffer_valid(Relation rel, Buffer buf);
extern void _bt_checkpage(Relation rel, Buffer buf, BlockNumber par_blkno = InvalidBlockNumber);
extern Buffer _bt_getbuf(Relation rel, BlockNumber blkno, int access);
extern Buffer _bt_relandgetbuf(Relation rel, Buffer obuf, BlockNumber blkno, int access,
                               BlockNumber par_blkno = InvalidBlockNumber);
extern void _bt_relbuf(Relation rel, Buffer buf);
extern void _bt_pageinit(Page page, Size size);
extern bool _bt_page_recyclable(Page page);
extern void _bt_delitems_delete(Relation rel, Buffer buf, OffsetNumber* itemnos, int nitems, Relation heapRel);
extern void _bt_delitems_vacuum(const Relation rel, Buffer buf, OffsetNumber *deletable, int num_deletable,
                                BTVacuumPosting *updatable, int num_updatable, BlockNumber last_block_vacuumed);
extern int _bt_pagedel(Relation rel, Buffer buf, BTStack stack);
extern void _bt_page_localupgrade(Page page);
extern void btree_meta_version(Relation rel, bool *heapkeyspace, bool *allequalimage);

/*
 * prototypes for functions in nbtsearch.c
 */
extern BTStack _bt_search(
    Relation rel, BTScanInsert key, Buffer* bufP, int access, bool needStack = true);
extern Buffer _bt_moveright(Relation rel, BTScanInsert key, Buffer buf, bool forupdate, BTStack stack, int access);
extern OffsetNumber _bt_binsrch(Relation rel, BTScanInsert key, Buffer buf, int *posting_off);
extern int32 _bt_compare(Relation rel, BTScanInsert key, Page page, OffsetNumber offnum);
extern bool _bt_first(IndexScanDesc scan, ScanDirection dir);
extern bool _bt_next(IndexScanDesc scan, ScanDirection dir);
extern Buffer _bt_walk_left(Relation rel, Buffer buf);
extern Buffer _bt_get_endpoint(Relation rel, uint32 level, bool rightmost);
extern bool _bt_gettuple_internal(IndexScanDesc scan, ScanDirection dir);
extern bool _bt_check_natts(const Relation index, bool heapkeyspace, Page page, OffsetNumber offnum);
extern int _bt_getrootheight(Relation rel);

/*
 * prototypes for functions in nbtutils.c
 */
extern BTScanInsert _bt_mkscankey(Relation rel, IndexTuple itup);
extern ScanKey _bt_mkscankey_nodata(Relation rel);
extern void _bt_freeskey(ScanKey skey);
extern void _bt_freestack(BTStack stack);
extern int _bt_sort_array_elements(
    IndexScanDesc scan, ScanKey skey, bool reverse, Datum* elems, int nelems);
extern void _bt_preprocess_array_keys(IndexScanDesc scan);
extern void _bt_start_array_keys(IndexScanDesc scan, ScanDirection dir);
extern bool _bt_advance_array_keys(IndexScanDesc scan, ScanDirection dir);
extern void _bt_mark_array_keys(IndexScanDesc scan);
extern void _bt_restore_array_keys(IndexScanDesc scan);
extern void _bt_preprocess_keys(IndexScanDesc scan);
extern IndexTuple _bt_checkkeys(
    IndexScanDesc scan, Page page, OffsetNumber offnum, ScanDirection dir, bool* continuescan);
extern bool _bt_check_rowcompare(ScanKey skey, IndexTuple tuple, TupleDesc tupdesc,
    ScanDirection dir, bool *continuescan);
extern void _bt_killitems(IndexScanDesc scan, bool haveLock);
extern BTCycleId _bt_vacuum_cycleid(Relation rel);
extern BTCycleId _bt_start_vacuum(Relation rel);
extern void _bt_end_vacuum(Relation rel);
extern void _bt_end_vacuum_callback(int code, Datum arg);
extern Size BTreeShmemSize(void);
extern void BTreeShmemInit(void);
extern double _bt_spools_heapscan(Relation heap, Relation index, BTBuildState *buildstate, IndexInfo *indexInfo,
    double **allPartTuples);
extern void _bt_end_parallel();
extern BTSpool* _bt_spoolinit(Relation heap, Relation index, bool isunique, bool isdead, void* meminfo);
extern void _bt_spooldestroy(BTSpool* btspool);
extern void _bt_spool(BTSpool *btspool, ItemPointer self, Datum *values, const bool *isnull);
extern void _bt_leafbuild(BTSpool* btspool, BTSpool* spool2);
// these 4 functions are move here from nbtsearch.cpp(static functions)
extern void _bt_buildadd(BTWriteState* wstate, BTPageState* state, IndexTuple itup, Size truncextra);
extern void _bt_uppershutdown(BTWriteState* wstate, BTPageState* state);
BTPageState* _bt_pagestate(BTWriteState* wstate, uint32 level);
extern bool _bt_index_tuple_compare(TupleDesc tupdes, ScanKey indexScanKey, int keysz, IndexTuple itup, IndexTuple itup2);
extern List* insert_ordered_index(List* list, TupleDesc tupdes, ScanKey indexScanKey, int keysz, IndexTuple itup,
    BlockNumber heapModifiedOffset, IndexScanDesc srcIdxRelScan);
extern uint64 uniter_next(pg_atomic_uint64 *curiter, uint32 cycle0, uint32 cycle1);
extern IndexTuple btree_truncate(Relation rel, IndexTuple lastleft, IndexTuple firstright, BTScanInsert itup_key);
extern void btree_check_third_page(Relation rel, Relation heap, bool need_heaptid_space, Page page, IndexTuple tuple);
extern int btree_num_keep_atts_fast(Relation rel, IndexTuple lastleft, IndexTuple firstright);
extern bool btree_allequalimage(Relation rel, bool debugmessage);

#ifdef USE_SPQ
extern void spq_load(BTWriteState wstate);
#endif

/*
 * prototypes for functions in nbtxlog.c
 */
extern void btree_redo(XLogReaderState* record);
extern void btree_desc(StringInfo buf, XLogReaderState* record);
extern const char* btree_type_name(uint8 subtype);
extern void btree_xlog_startup(void);
extern void btree_xlog_cleanup(void);
extern bool btree_safe_restartpoint(void);
extern void* btree_get_incomplete_actions();
extern void btree_clear_imcompleteAction();
extern bool IsBtreeVacuum(const XLogReaderState* record);
extern void _bt_restore_page(Page page, char* from, int len);
extern void DumpBtreeDeleteInfo(XLogRecPtr lsn, OffsetNumber offsetList[], uint64 offsetNum);
extern void btbuildCallback(Relation index, HeapTuple htup, Datum *values, const bool *isnull, bool tupleIsAlive,
                            void *state);

/*
 * prototypes for functions in nbtdedup.cpp
 */
extern void btree_dedup_page(Relation rel, Relation heapRel, Buffer buf, IndexTuple new_tuple, Size new_tuple_size);
extern IndexTuple btree_dedup_form_posting(IndexTuple base, ItemPointer heap_tids, int num_heap_tids);
extern void btree_dedup_begin(BTDedupState state, IndexTuple base, OffsetNumber base_off);
extern bool btree_dedup_merge(BTDedupState state, IndexTuple itup);
extern Size btree_dedup_end(Page newpage, BTDedupState state);
extern void btree_dedup_update_posting(BTVacuumPosting vac_posting);
extern IndexTuple btree_dedup_swap_posting(IndexTuple newitem, IndexTuple orignal_posting, int posting_off);

#endif /* NBTREE_H */
