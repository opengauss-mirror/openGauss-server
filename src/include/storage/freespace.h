/* -------------------------------------------------------------------------
 *
 * freespace.h
 *	  openGauss free space map for quickly finding free space in relations
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/freespace.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef FREESPACE_H_
#define FREESPACE_H_

#include "storage/buf/block.h"
#include "storage/smgr/relfilenode.h"
#include "utils/relcache.h"
#include "utils/partcache.h"

typedef uint64 XLogRecPtr;


/*
 * We use just one byte to store the amount of free space on a page, so we
 * divide the amount of free space a page can have into 256 different
 * categories. The highest category, 255, represents a page with at least
 * MaxFSMRequestSize bytes of free space, and the second highest category
 * represents the range from 254 * FSM_CAT_STEP, inclusive, to
 * MaxFSMRequestSize, exclusive.
 *
 * MaxFSMRequestSize depends on the architecture and BLCKSZ, but assuming
 * default 8k BLCKSZ, and that MaxFSMRequestSize is 24 bytes, the categories
 * look like this
 *
 *
 * Range	 Category
 * 0	- 31   0
 * 32	- 63   1
 * ...	  ...  ...
 * 8096 - 8127 253
 * 8128 - 8163 254
 * 8164 - 8192 255
 *
 * The reason that MaxFSMRequestSize is special is that if MaxFSMRequestSize
 * isn't equal to a range boundary, a page with exactly MaxFSMRequestSize
 * bytes of free space wouldn't satisfy a request for MaxFSMRequestSize
 * bytes. If there isn't more than MaxFSMRequestSize bytes of free space on a
 * completely empty page, that would mean that we could never satisfy a
 * request of exactly MaxFSMRequestSize bytes.
 */
#define FSM_CATEGORIES 256
#define FSM_CAT_STEP (BLCKSZ / FSM_CATEGORIES)
#define MaxFSMRequestSize MaxHeapTupleSize

/*
 * Depth of the on-disk tree. We need to be able to address 2^32-1 blocks,
 * and 1626 is the smallest number that satisfies X^3 >= 2^32-1. Likewise,
 * 216 is the smallest number that satisfies X^4 >= 2^32-1. In practice,
 * this means that 4096 bytes is the smallest BLCKSZ that we can get away
 * with a 3-level tree, and 512 is the smallest we support.
 */
#define FSM_TREE_DEPTH ((SlotsPerFSMPage >= 1626) ? 3 : 4)

#define FSM_ROOT_LEVEL (FSM_TREE_DEPTH - 1)
#define FSM_BOTTOM_LEVEL 0

/*
 * The internal FSM routines work on a logical addressing scheme. Each
 * level of the tree can be thought of as a separately addressable file.
 */
typedef struct {
    int level;     /* level */
    int logpageno; /* page number within the level */
} FSMAddress;



/* prototypes for public functions in freespace.c */
extern Size GetRecordedFreeSpace(Relation rel, BlockNumber heapBlk);
extern BlockNumber GetPageWithFreeSpace(Relation rel, Size spaceNeeded);
extern BlockNumber RecordAndGetPageWithFreeSpace(
    Relation rel, BlockNumber oldPage, Size oldSpaceAvail, Size spaceNeeded);
extern void RecordPageWithFreeSpace(Relation rel, BlockNumber heapBlk, Size spaceAvail);
extern void XLogRecordPageWithFreeSpace(const RelFileNode& rnode, BlockNumber heapBlk, Size spaceAvail);

extern void FreeSpaceMapTruncateRel(Relation rel, BlockNumber nblocks);
extern void FreeSpaceMapVacuum(Relation rel);
extern void UpdateFreeSpaceMap(Relation rel, BlockNumber firtsBlkNum, BlockNumber lastBlkNum, Size freespace,
    bool search = true);
extern BlockNumber FreeSpaceMapCalTruncBlkNo(BlockNumber relBlkNo);
extern void XLogBlockTruncateRelFSM(Relation rel, BlockNumber nblocks);
extern FSMAddress fsm_get_location(BlockNumber heapblk, uint16* slot);
extern BlockNumber fsm_logical_to_physical(const FSMAddress& addr);
extern uint8 fsm_space_avail_to_cat(Size avail);
extern bool fsm_set_avail(Page page, int slot, uint8 value);

#endif /* FREESPACE_H_ */
