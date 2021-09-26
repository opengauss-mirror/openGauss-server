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
 * pgxc_slice.h
 *  system catalog table that storing slice information for list/range distributed table.
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pgxc_slice.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PGXC_SLICE_H
#define PGXC_SLICE_H

#include "access/tupdesc.h"
#include "catalog/genbki.h"
#include "nodes/primnodes.h"

#define PgxcSliceRelationId 9035
#define PgxcSliceRelation_Rowtype_Id 9032

CATALOG(pgxc_slice,9035) BKI_WITHOUT_OIDS BKI_SCHEMA_MACRO
{
    NameData    relname;    /* list/range distributed table name or slice name of the table */
    char        type;
    char        strategy;
    Oid         relid;      /* table oid */
    Oid         referenceoid; /* table oid that slice referenced to */
    int4        sindex;

#ifdef CATALOG_VARLEN
    text        interval[1];
    text        transitboundary[1];
#endif
    int4        transitno;
    Oid         nodeoid;
#ifdef CATALOG_VARLEN
    text        boundaries[1];
#endif
    bool        specified; /* whether nodeoid is specified by user in DDL */
    int4        sliceorder;
} FormData_pgxc_slice;

typedef FormData_pgxc_slice *Form_pgxc_slice;

#define Natts_pgxc_slice                13
#define Anum_pgxc_slice_relname         1
#define Anum_pgxc_slice_type            2
#define Anum_pgxc_slice_strategy        3
#define Anum_pgxc_slice_relid           4
#define Anum_pgxc_slice_referenceoid    5
#define Anum_pgxc_slice_sindex          6
#define Anum_pgxc_slice_interval        7
#define Anum_pgxc_slice_transitboundary 8
#define Anum_pgxc_slice_transitno       9
#define Anum_pgxc_slice_nodeoid         10
#define Anum_pgxc_slice_boundaries      11
#define Anum_pgxc_slice_specified       12
#define Anum_pgxc_slice_sliceorder      13

#define PGXC_SLICE_TYPE_TABLE 't'
#define PGXC_SLICE_TYPE_SLICE 's'

extern void PgxcSliceCreate(const char* relname, Oid relid, DistributeBy* distributeby,
    TupleDesc desc, const Oid* nodeoids, uint32 nodenum, uint32 startpos);
extern void RemovePgxcSlice(Oid relid);

#endif  /* PGXC_SLICE_H */
