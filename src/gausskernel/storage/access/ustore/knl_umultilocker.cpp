/* -------------------------------------------------------------------------
 *
 * knl_umultilocker.cpp
 * Multilocker implementation for inplace update engine.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/gausskernel/storage/access/ustore/knl_umultilocker.cpp
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/ustore/knl_umultilocker.h"

static bool IsUMultiLockListMember(const List *members, const UMultiLockMember *mlmember);

/*
 * IsUMultiLockListMember - Returns true iff mlmember is a member of list
 * 	members.  Equality is determined by comparing all the variables of
 * 	member.
 */
static bool IsUMultiLockListMember(const List *members, const UMultiLockMember *mlmember)
{
    ListCell *lc;

    foreach (lc, members) {
        UMultiLockMember *lc_member = (UMultiLockMember *)lfirst(lc);

        if (lc_member->xid == mlmember->xid && lc_member->td_slot_id == mlmember->td_slot_id &&
            lc_member->mode == mlmember->mode)
            return true;
    }

    return false;
}

/*
 * UMultiLockMembersSame -  Returns true, iff all the members in list2 list
 * 	are present in list1 list
 */
bool UMultiLockMembersSame(const List *list1, const List *list2)
{
    ListCell *lc;

    if (list_length(list2) > list_length(list1))
        return false;

    foreach (lc, list2) {
        UMultiLockMember *mlmember = (UMultiLockMember *)lfirst(lc);

        if (!IsUMultiLockListMember(list1, mlmember))
            return false;
    }

    return true;
}

LockTupleMode GetOldLockMode(uint16 infomask)
{
    LockTupleMode oldLockMode;

    /*
     * Normally, if the tuple is not marked as locked only, it should not
     * contain any locker information. But, during rollback of
     * (in-)update/delete, we retain the multilocker information. See
     * execute_undo_actions_page for details.
     */
    if (UHEAP_XID_IS_LOCKED_ONLY(infomask) || !IsUHeapTupleModified(infomask)) {
        if (UHEAP_XID_IS_KEYSHR_LOCKED(infomask))
            oldLockMode = LockTupleKeyShare;
        else if (UHEAP_XID_IS_SHR_LOCKED(infomask))
            oldLockMode = LockTupleShared;
        else if (UHEAP_XID_IS_NOKEY_EXCL_LOCKED(infomask))
            oldLockMode = LockTupleNoKeyExclusive;
        else if (UHEAP_XID_IS_EXCL_LOCKED(infomask))
            oldLockMode = LockTupleExclusive;
        else {
            /* LOCK_ONLY can't be present alone */
            pg_unreachable();
        }
    } else {
        /* it's an update, but which kind? */
        if (infomask & UHEAP_XID_EXCL_LOCK)
            oldLockMode = LockTupleExclusive;
        else
            oldLockMode = LockTupleNoKeyExclusive;
    }

    return oldLockMode;
}

/*
 * UGetMultiLockInfo - Helper function for ComputeNewXidInfomask to
 * 	get the multi lockers information.
 */
void UGetMultiLockInfo(uint16 oldInfomask, TransactionId tupXid, int tupTdSlot, TransactionId addToXid,
    uint16 *newInfomask, int *newTdSlot, LockTupleMode *mode, bool *oldTupleHasUpdate, LockOper lockoper)
{
    LockTupleMode oldMode;

    oldMode = GetOldLockMode(oldInfomask);

    if (tupXid == addToXid) {
        if (UHeapTupleHasMultiLockers(oldInfomask)) {
            elog(PANIC, "Set infomask UHEAP_MULTI_LOCKERS."); // not fall through here in ustore
            *newInfomask |= UHEAP_MULTI_LOCKERS;
        }
        /* acquire the strongest of both */
        if (*mode < oldMode)
            *mode = oldMode;
    } else {
        elog(PANIC, "Set infomask UHEAP_MULTI_LOCKERS."); // not fall through here in ustore
        *newInfomask |= UHEAP_MULTI_LOCKERS;

        /*
         * Acquire the strongest of both and keep the transaction slot of the
         * stronger lock.
         */
        if (*mode < oldMode) {
            *mode = oldMode;
        }

        /* For lockers, we want to store the updater's transaction slot. */
        if (lockoper != ForUpdate)
            *newTdSlot = tupTdSlot;
    }

    /*
     * We want to propagate the updaters information for lockers only provided
     * the tuple is already locked by others (aka it has its multi-locker bit
     * set).
     */
    if (lockoper != ForUpdate && UHeapTupleHasMultiLockers(*newInfomask) && IsUHeapTupleModified(oldInfomask) &&
        !UHEAP_XID_IS_LOCKED_ONLY(oldInfomask)) {
        *oldTupleHasUpdate = true;

        if (UHeapTupleIsInPlaceUpdated(oldInfomask)) {
            *newInfomask |= UHEAP_INPLACE_UPDATED;
        } else {
            Assert(UHeapTupleIsUpdated(oldInfomask));
            *newInfomask |= UHEAP_UPDATED;
        }
    }
}
