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
 * -------------------------------------------------------------------------
 * pg_snapshot.h
 *      definition of the system "pg_snapshot" relation (pg_snapshot)
 *      along with the relation's initial contents.
 *
 * IDENTIFICATION
 * src/include/catalog/pg_snapshot.h
 *
 * NOTES
 *      the genbki.pl script reads this file and generates .bki
 *      information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_SNAPSHOT_H
#define PG_SNAPSHOT_H

#include "catalog/genbki.h"

#define timestamptz Datum
#define int8 int64


/* ----------------
 *        pg_snapshot definition.  cpp turns this into
 *        typedef struct FormData_pg_snapshot.
 * ----------------
 */
#define SnapshotRelationId  8645
#define SnapshotRelation_Rowtype_Id 8646

CATALOG(gs_txn_snapshot,8645) BKI_WITHOUT_OIDS BKI_SCHEMA_MACRO
{
    /* timestamp of snapshot taken */
    timestamptz    snptime;

    /* xmin of the snapshot, ref to .snpsnapshot::xmin */
    int8           snpxmin;

    /* csn of the snapshot, ref to .snpsnapshot::snapshotcsn */
    int8           snpcsn;

#ifdef CATALOG_VARLEN
    /* serialized SnapshotData */
    text           snpsnapshot;
#endif
} FormData_pg_snapshot;


#undef timestamptz
#undef int8


/* ----------------
 *        Form_pg_snapshot corresponds to a pointer to a tuple with
 *        the format of pg_snapshot relation.
 * ----------------
 */
typedef FormData_pg_snapshot *Form_pg_snapshot;

/* ----------------
 *        compiler constants for pg_snapshot
 * ----------------
 */
#define Natts_pg_snapshot                    4
#define Anum_pg_snapshot_snptime             1
#define Anum_pg_snapshot_snpxmin             2
#define Anum_pg_snapshot_snpcsn              3
#define Anum_pg_snapshot_snpsnapshot         4

#endif   /* PG_SNAPSHOT_H */

