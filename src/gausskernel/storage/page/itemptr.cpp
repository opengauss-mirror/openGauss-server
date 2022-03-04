/* -------------------------------------------------------------------------
 *
 * itemptr.cpp
 *	  openGauss disk item pointer code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/page/itemptr.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "storage/item/itemptr.h"

/*
 * ItemPointerEquals
 *	Returns true if both item pointers point to the same item,
 *	 otherwise returns false.
 *
 * Note:
 *	Asserts that the disk item pointers are both valid!
 */
bool ItemPointerEquals(ItemPointer pointer1, ItemPointer pointer2)
{
    if (ItemPointerGetBlockNumber(pointer1) == ItemPointerGetBlockNumber(pointer2) &&
        ItemPointerGetOffsetNumber(pointer1) == ItemPointerGetOffsetNumber(pointer2))
        return true;
    else
        return false;
}

bool ItemPointerEqualsNoCheck(ItemPointer pointer1, ItemPointer pointer2)
{
    if ((ItemPointerGetBlockNumberNoCheck(pointer1) == ItemPointerGetBlockNumberNoCheck(pointer2)) &&
        (ItemPointerGetOffsetNumberNoCheck(pointer1) == ItemPointerGetOffsetNumberNoCheck(pointer2)))
        return true;
    else
        return false;
}

/*
 * ItemPointerCompare
 *		Generic btree-style comparison for item pointers.
 */
int32 ItemPointerCompare(ItemPointer arg1, ItemPointer arg2)
{
    /*
     * Don't use ItemPointerGetBlockNumber or ItemPointerGetOffsetNumber here,
     * because they assert ip_posid != 0 which might not be true for a
     * user-supplied TID.
     */
    BlockNumber b1 = BlockIdGetBlockNumber(&(arg1->ip_blkid));
    BlockNumber b2 = BlockIdGetBlockNumber(&(arg2->ip_blkid));
    if (b1 < b2)
        return -1;
    else if (b1 > b2)
        return 1;
    else if (arg1->ip_posid < arg2->ip_posid)
        return -1;
    else if (arg1->ip_posid > arg2->ip_posid)
        return 1;
    else
        return 0;
}

