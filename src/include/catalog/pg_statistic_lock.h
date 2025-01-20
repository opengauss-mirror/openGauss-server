/* Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * --------------------------------------------------------------------------------------
 *
 * pg_statistic_lock.h
 *      definition of the system "statistic_lock" relation (pg_statistic_lock)
 *      along with the relation's initial contents.
 *
 * src/include/catalog/pg_statistic_lock.h
 *
 * NOTES
 *      the genbki.pl script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_STATISTIC_LOCK_H
#define PG_STATISTIC_LOCK_H

#include "catalog/genbki.h"

/* ----------------
 *        pg_statistic_lock definition.  cpp turns this into
 *        typedef struct FormData_pg_statistic_lock
 * ----------------
 */
#define StatisticLockRelationId  4897
#define StatisticLockRelation_Rowtype_Id 4898

CATALOG(pg_statistic_lock,4897) BKI_WITHOUT_OIDS BKI_ROWTYPE_OID(4898) BKI_SCHEMA_MACRO
{
    /* These fields form the unique key for the entry: */
    Oid         namespaceid;
    char        stalocktype;
    Oid         relid;
    Oid         partid;
    bool        lock;
} FormData_pg_statistic_lock;

/* ----------------
 *        Form_pg_statistic corresponds to a pointer to a tuple with
 *        the format of pg_statistic_lock relation.
 * ----------------
 */
typedef FormData_pg_statistic_lock *Form_pg_statistic_lock;

/* ----------------
 *        compiler constants for pg_statistic_lock
 * ----------------
 */
#define Natts_pg_statistic_lock                 5
#define Anum_pg_statistic_lock_namespaceid      1
#define Anum_pg_statistic_lock_stalocktype      2
#define Anum_pg_statistic_lock_relid            3
#define Anum_pg_statistic_lock_partid           4
#define Anum_pg_statistic_lock_lock             5

extern bool CheckRelationLocked(Oid namespaceid, Oid relid, Oid partid = InvalidOid, bool checkSchema = true);
extern void RemoveStatisticLockTab(Oid namespaceid, Oid relid);
extern void LockStatistic(Oid namespaceid, Oid relid, Oid partid = InvalidOid);
extern void UnlockStatistic(Oid namespaceid, Oid relid, Oid partid = InvalidOid);

#endif   /* PG_STATISTIC_LOCK_H */