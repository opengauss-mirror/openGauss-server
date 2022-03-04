/* -------------------------------------------------------------------------
 *
 * knl_umultilocker.h
 * UStore multi locker function definitions.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * opengauss_server/src/include/access/ustore/knl_umultilocker.h
 * -------------------------------------------------------------------------
 */

#ifndef KNL_UMULTILOCKER_H
#define KNL_UMULTILOCKER_H

#include "access/xact.h"
#include "access/relscan.h"
#include "pgstat.h"
#include "access/multixact.h"
#include "access/ustore/knl_utuple.h"

/*
 * Get the heavy-weight lock mode from lock tuple mode.
 */
inline LOCKMODE GetHWLockModeFromMode(LockTupleMode mode)
{
    return TupleLockExtraInfo[mode].hwlock;
}

/* Get the LOCKMODE for a given LockTupleMode */
#define HWLOCKMODE_from_locktupmode(lockmode) (GetHWLockModeFromMode(lockmode))

typedef struct UMultiLockMember {
    TransactionId xid;
    SubTransactionId subxid;
    int td_slot_id;
    LockTupleMode mode;
} UMultiLockMember;

bool UMultiLockMembersSame(const List *list1, const List *list2);

#endif
