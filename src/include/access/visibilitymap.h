/* -------------------------------------------------------------------------
 *
 * visibilitymap.h
 *		visibility map interface
 *
 *
 * Portions Copyright (c) 2007-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/visibilitymap.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef VISIBILITYMAP_H
#define VISIBILITYMAP_H

#include "access/xlogdefs.h"
#include "storage/buf/block.h"
#include "storage/buf/buf.h"
#include "utils/relcache.h"

/*
 * Size of the bitmap on each visibility map page, in bytes. There's no
 * extra headers, so the whole page minus the standard page header is
 * used for the bitmap.
 */
#define MAPSIZE (BLCKSZ - MAXALIGN(SizeOfPageHeaderData))

/* Number of bits allocated for each heap block. */
#define BITS_PER_HEAPBLOCK 1

/* Number of heap blocks we can represent in one byte. */
#define HEAPBLOCKS_PER_BYTE 8

/* Number of heap blocks we can represent in one visibility map page. */
#define HEAPBLOCKS_PER_PAGE (MAPSIZE * HEAPBLOCKS_PER_BYTE)

/* Mapping from heap block number to the right bit in the visibility map */
#define HEAPBLK_TO_MAPBLOCK(x) ((x) / HEAPBLOCKS_PER_PAGE)
#define HEAPBLK_TO_MAPBYTE(x) (((x) % HEAPBLOCKS_PER_PAGE) / HEAPBLOCKS_PER_BYTE)
#define HEAPBLK_TO_MAPBIT(x) ((x) % HEAPBLOCKS_PER_BYTE)

extern void visibilitymap_clear(Relation rel, BlockNumber heapBlk, Buffer vmbuf);
extern bool visibilitymap_clear_page(Page mappage, BlockNumber heapBlk);

extern void visibilitymap_pin(Relation rel, BlockNumber heapBlk, Buffer* vmbuf);
extern bool visibilitymap_pin_ok(BlockNumber heapBlk, Buffer vmbuf);

extern void visibilitymap_set(Relation rel, BlockNumber heapBlk, Buffer heapBuf, XLogRecPtr recptr, Buffer vmbuf,
    TransactionId cutoff_xid, bool free_dict);
extern bool visibilitymap_test(Relation rel, BlockNumber heapBlk, Buffer* vmbuf);
extern BlockNumber visibilitymap_count(Relation rel, Partition part);

extern void visibilitymap_truncate(Relation rel, BlockNumber nheapblocks);
extern BlockNumber VisibilityMapCalTruncBlkNo(BlockNumber relBlkNo);
extern bool visibilitymap_set_page(Page page, BlockNumber heapBlk);
extern void XLogBlockTruncateRelVM(Relation rel, BlockNumber nheapblocks);

#endif /* VISIBILITYMAP_H */

