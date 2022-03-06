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
