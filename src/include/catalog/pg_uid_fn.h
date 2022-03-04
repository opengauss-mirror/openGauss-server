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
 * pg_uid_fn.h
 *
 *
 * IDENTIFICATION
 *        src/include/catalog/pg_uid_fn.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PG_UID_FN_H
#define PG_UID_FN_H

#define BACKUP_NO_START (0)
#define BACKUP_IN_PROGRESS (1)

#ifdef USE_ASSERT_CHECKING
#define UID_RESTORE_DURATION (5)
#else
#define UID_RESTORE_DURATION (2000000)
#endif

typedef struct UidHashKey {
    Oid dbOid;  /* database */
    Oid relOid; /* relation */
} UidHashKey;

typedef struct UidHashValue {
    /* key field */
    UidHashKey key;
    pg_atomic_uint64 currentUid;
    pg_atomic_uint64 backupUidRange;
    pg_atomic_uint32 backUpState;
} UidHashValue;

extern void DeleteDatabaseUidEntry(Oid dbOid);
extern void DeleteUidEntry(Oid relid);
extern void InsertUidEntry(Oid relid);
extern void UpdateUidEntry(Oid dbOid, Oid relOid, uint64 backupUid);
extern void InitUidCache(void);
extern uint64 GetNewUidForTuple(Relation relation);
extern void BuildUidHashCache(Oid dbOid, Oid relOid);

#endif

