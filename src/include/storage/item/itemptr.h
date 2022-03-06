/* -------------------------------------------------------------------------
 *
 * itemptr.h
 *	  openGauss disk item pointer definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/item/itemptr.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef ITEMPTR_H
#define ITEMPTR_H

#include "storage/buf/block.h"
#include "storage/off.h"

/*
 * ItemPointer:
 *
 * This is a pointer to an item within a disk page of a known file
 * (for example, a cross-link from an index to its parent table).
 * blkid tells us which block, posid tells us which entry in the linp
 * (ItemIdData) array we want.
 *
 * Note: because there is an item pointer in each tuple header and index
 * tuple header on disk, it's very important not to waste space with
 * structure padding bytes.  The struct is designed to be six bytes long
 * (it contains three int16 fields) but a few compilers will pad it to
 * eight bytes unless coerced.	We apply appropriate persuasion where
 * possible, and to cope with unpersuadable compilers, we try to use
 * "SizeOfIptrData" rather than "sizeof(ItemPointerData)" when computing
 * on-disk sizes.
 */
typedef struct ItemPointerData {
    BlockIdData ip_blkid;
    OffsetNumber ip_posid;
}

#ifdef __arm__
__attribute__((packed)) /* Appropriate whack upside the head for ARM */
#endif
ItemPointerData;

#define SizeOfIptrData (offsetof(ItemPointerData, ip_posid) + sizeof(OffsetNumber))

typedef ItemPointerData* ItemPointer;

/* ------------------------------------------
 * special values used in heap tuples (t_ctid)
 * ------------------------------------------
 */

/*
 * If a heap tuple holds a speculative insertion token rather than a real
 * TID, ip_posid is set to SpecTokenOffsetNumber, and the token is stored in
 * ip_blkid. SpecTokenOffsetNumber must be higher than MaxOffsetNumber, so
 * that it can be distinguished from a valid offset number in a regular item
 * pointer.
 */
#define SpecTokenOffsetNumber           0xfffe

/*
 * When a tuple is moved to a different partition by UPDATE, the t_ctid of
 * the old tuple version is set to this magic value.
 */
#define MovedPartitionsOffsetNumber 0xfffd
#define MovedPartitionsBlockNumber      InvalidBlockNumber

/* ----------------
 *		support macros
 * ----------------
 */

/*
 * ItemPointerIsValid
 *		True iff the disk item pointer is not NULL.
 */
#define ItemPointerIsValid(pointer) ((bool)(PointerIsValid(pointer) && ((pointer)->ip_posid != 0)))

/*
 * ItemPointerGetBlockNumber
 *		Returns the block number of a disk item pointer.
 */
#define ItemPointerGetBlockNumber(pointer) \
    (AssertMacro(ItemPointerIsValid(pointer)), BlockIdGetBlockNumber(&(pointer)->ip_blkid))

/*
 * ItemPointerGetBlockNumberNoCheck
 *		Returns the block number of a disk item pointer.
 */
#define ItemPointerGetBlockNumberNoCheck(pointer) (BlockIdGetBlockNumber(&(pointer)->ip_blkid))

/*
 * ItemPointerGetOffsetNumberNoCheck
 *		Returns the offset number of a disk item pointer.
 */
#define ItemPointerGetOffsetNumberNoCheck(pointer) ((pointer)->ip_posid)

/*
 * ItemPointerGetOffsetNumber
 *		Returns the offset number of a disk item pointer.
 */
#define ItemPointerGetOffsetNumber(pointer) (AssertMacro(ItemPointerIsValid(pointer)), (pointer)->ip_posid)

/*
 * ItemPointerSet
 *		Sets a disk item pointer to the specified block and offset.
 */
#define ItemPointerSet(pointer, blockNumber, offNum)     \
    (AssertMacro(PointerIsValid(pointer)),               \
        BlockIdSet(&((pointer)->ip_blkid), blockNumber), \
        (pointer)->ip_posid = offNum)

/*
 * ItemPointerZero
 *      Sets a disk item pointer to zero.
 */
#define ItemPointerZero(pointer)            \
    (AssertMacro(PointerIsValid(pointer)),  \
        (pointer)->ip_blkid.bi_hi = 0,      \
        (pointer)->ip_blkid.bi_lo = 0,      \
        (pointer)->ip_posid = 0)

/*
 * ItemPointerSetBlockNumber
 *		Sets a disk item pointer to the specified block.
 */
#define ItemPointerSetBlockNumber(pointer, blockNumber) \
    (AssertMacro(PointerIsValid(pointer)), BlockIdSet(&((pointer)->ip_blkid), blockNumber))

/*
 * ItemPointerSetOffsetNumber
 *		Sets a disk item pointer to the specified offset.
 */
#define ItemPointerSetOffsetNumber(pointer, offsetNumber) \
    (AssertMacro(PointerIsValid(pointer)), (pointer)->ip_posid = (offsetNumber))

/*
 * ItemPointerCopy
 *		Copies the contents of one disk item pointer to another.
 *
 * Should there ever be padding in an ItemPointer this would need to be handled
 * differently as it's used as hash key.
 */
#define ItemPointerCopy(fromPointer, toPointer) \
    (AssertMacro(PointerIsValid(toPointer)), AssertMacro(PointerIsValid(fromPointer)), *(toPointer) = *(fromPointer))

/*
 * ItemPointerSetInvalid
 *		Sets a disk item pointer to be invalid.
 */
#define ItemPointerSetInvalid(pointer)                          \
    (AssertMacro(PointerIsValid(pointer)),                      \
        BlockIdSet(&((pointer)->ip_blkid), InvalidBlockNumber), \
        (pointer)->ip_posid = InvalidOffsetNumber)

/*
 * ItemPointerSetMovedPartitions
 *      Indicate that the item referenced by the itempointer has moved into a
 *      different partition.
 */
#define ItemPointerSetMovedPartitions(pointer) \
        ItemPointerSet((pointer), MovedPartitionsBlockNumber, MovedPartitionsOffsetNumber)

/* ----------------
 *		externs
 * ----------------
 */

extern bool ItemPointerEquals(ItemPointer pointer1, ItemPointer pointer2);
/* for upgrade from existed session, syscache has builtin tuple, AssertMacro(ItemPointerIsValid(pointer)) will fail */
extern bool ItemPointerEqualsNoCheck(ItemPointer pointer1, ItemPointer pointer2);
extern int32 ItemPointerCompare(ItemPointer arg1, ItemPointer arg2);

/* --------------------------------------------------------
 *      support macros for dfs ItemPointer
 *
 *      -------------------------------------
 *      |  0  |  1  |  2  |  3  |  4  |  5  |  memory address(low->high)
 *      -------------------------------------
 *      |       ip_blkid        | ip_posid  |  \
 *      -------------------------------------   ItemPointerData
 *      |   bi_hi   |   bi_lo   | ip_posid  |  /
 *      -------------------------------------
 *      |     file_id     |     offset      |  layout of ItemPointerData for dfs
 *      -------------------------------------
 *
 *  for compatibility, only 23 bits of "offset" is dedicated for position in file,
 *  the highest bit of "ip_posid" is always 1 for valid dfs tid, so the low 8 bit
 *  of bi_lo and the low 15 bit of ip_posid are used for offset.
 *
 *  Just get/set macros are provided here, the design of tid for dfs provides the
 *  most compatibility with others macro and function of tid of row and column
 *  storage. For example, you can use ItemPointerIsValid() to check dfs tid.
 * --------------------------------------------------------
 */

#define DfsInvalidBlockNumber ((BlockNumber)0x00FFFFFF)
#define DfsMaxBlockNumber ((BlockNumber)0x00FFFFFE)

#define DfsInvalidOffsetNumber ((uint32)0)
#define DfsFirstOffsetNumber ((uint32)1)
#define DfsMaxOffset (0x007fffff)

/*
 * DfsItemPointerGetFileId
 *		Returns the file id of a dfs item pointer.
 */
#define DfsItemPointerGetFileId(pointer)       \
    (AssertMacro(ItemPointerIsValid(pointer)), \
        (BlockNumber)(((pointer)->ip_blkid).bi_hi << 8) | (BlockNumber)(((pointer)->ip_blkid).bi_lo >> 8))

/*
 * DfsItemPointerGetOffset
 *		Returns the offset in file of a dfs item pointer.
 */
#define DfsItemPointerGetOffset(pointer)       \
    (AssertMacro(ItemPointerIsValid(pointer)), \
        (uint32)((((pointer)->ip_blkid).bi_lo & 0x00ff) << 15) | ((pointer)->ip_posid & 0x7fff))

/*
 * DfsItemPointerSetFileId
 *		Sets a dfs item pointer to the specified file id.
 */
#define DfsItemPointerSetFileId(pointer, fileId)             \
    (AssertMacro(PointerIsValid(pointer)),                   \
        ((pointer)->ip_blkid).bi_hi = ((fileId) << 8) >> 16, \
        ((pointer)->ip_blkid).bi_lo &= 0x00ff,               \
        ((pointer)->ip_blkid).bi_lo |= (((fileId) << 8) & 0xff00))

/*
 * DfsItemPointerSetOffset
 *		Sets a dfs item pointer to the specified offset.
 */
#define DfsItemPointerSetOffset(pointer, offset)  \
    (AssertMacro(PointerIsValid(pointer)),        \
        (pointer)->ip_posid = 0x8000,             \
        (pointer)->ip_posid |= ((offset)&0x7fff), \
        ((pointer)->ip_blkid).bi_lo &= 0xff00,    \
        ((pointer)->ip_blkid).bi_lo |= (((offset) >> 15) & 0x00ff))

/*
 * DfsItemPointerSet
 *		Sets a dfs item pointer to the specified file id and offset.
 */
#define DfsItemPointerSet(pointer, fileId, offset) \
    (AssertMacro(PointerIsValid(pointer)),         \
        DfsItemPointerSetFileId(pointer, fileId),  \
        DfsItemPointerSetOffset(pointer, offset))

#endif /* ITEMPTR_H */
