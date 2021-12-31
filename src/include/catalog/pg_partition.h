/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * pg_partition.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/pg_partition.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PG_PARTITION_H
#define PG_PARTITION_H
#include "catalog/genbki.h"

#define PartitionRelationId 9016
#define PartitionRelation_Rowtype_Id 3790 
CATALOG(pg_partition,9016) BKI_ROWTYPE_OID(3790) BKI_SCHEMA_MACRO
{
    NameData        relname;
    char            parttype;
    Oid             parentid;
    int4            rangenum;
    int4            intervalnum;
    char            partstrategy;
    Oid             relfilenode;
    Oid             reltablespace;
    float8          relpages;
    float8          reltuples;
    int4            relallvisible;
    Oid             reltoastrelid;
    Oid             reltoastidxid;
    Oid             indextblid;     /* index partition's table partition's oid */    
    bool            indisusable;   /* is this index partition useable for insert and select? */
                                           /*if yes, insert and select should ignore this index partition */
    Oid             reldeltarelid;  /* if ColStore table, it is not 0 */
    Oid             reldeltaidx;
    Oid             relcudescrelid; /* if ColStore table, it is not 0 */
    Oid             relcudescidx;        
    ShortTransactionId  relfrozenxid;
    int4            intspnum;    
#ifdef CATALOG_VARLEN    
    int2vector      partkey;
    oidvector       intervaltablespace;
    text            interval[1];
    text            boundaries[1];
    text            transit[1];
    text            reloptions[1];    /* access-method-specific options */
#endif
    TransactionId  relfrozenxid64;
    TransactionId relminmxid;     /* all multixacts in this rel are >= this.
                                   * this is really a MultiXactId */
} FormData_pg_partition;
/* Size of fixed part of pg_partition tuples, not counting var-length fields */
#define PARTITION_TUPLE_SIZE \
	 (offsetof(FormData_pg_partition,intspnum) + sizeof(int4))

/* Get a more readable name of partition strategy by given abbreviation */
#define GetPartitionStrategyNameByType(s)\
(\
    (s == 'r') ? "RANGE-PARTITION" : \
        (s == 'i') ? "INTERVAL-PARTITION" : \
            (s == 'l') ? "LIST-PARTITION" : \
            (s == 'h') ? "HASH-PARTITION" : \
            (s == 'v') ? "VALUE-PARTITION" : "INVALID"\
)\

typedef FormData_pg_partition *Form_pg_partition;

#define PART_STRATEGY_RANGE              'r'
#define PART_STRATEGY_INTERVAL           'i'
#define PART_STRATEGY_VALUE              'v'
#define PART_STRATEGY_LIST               'l'
#define PART_STRATEGY_HASH               'h'
#define PART_STRATEGY_INVALID            'n'

#define PART_OBJ_TYPE_PARTED_TABLE       'r'
#define PART_OBJ_TYPE_TOAST_TABLE        't'
#define PART_OBJ_TYPE_TABLE_PARTITION    'p'
#define PART_OBJ_TYPE_TABLE_SUB_PARTITION 's'
#define PART_OBJ_TYPE_INDEX_PARTITION    'x'

#define Natts_pg_partition               29
#define Anum_pg_partition_relname        1
#define Anum_pg_partition_parttype       2
#define Anum_pg_partition_parentid       3
#define Anum_pg_partition_rangenum       4
#define Anum_pg_partition_intervalnum    5
#define Anum_pg_partition_partstrategy   6
#define Anum_pg_partition_relfilenode    7
#define Anum_pg_partition_reltablespace  8
#define Anum_pg_partition_relpages       9
#define Anum_pg_partition_reltuples      10
#define Anum_pg_partition_relallvisible  11
#define Anum_pg_partition_reltoastrelid  12
#define Anum_pg_partition_reltoastidxid  13
#define Anum_pg_partition_indextblid     14
#define Anum_pg_partition_indisusable    15
#define Anum_pg_partition_deltarelid     16
#define Anum_pg_partition_reldeltaidx    17
#define Anum_pg_partition_relcudescrelid 18
#define Anum_pg_partition_relcudescidx   19
#define Anum_pg_partition_relfrozenxid   20
#define Anum_pg_partition_intspnum       21
#define Anum_pg_partition_partkey        22
#define Anum_pg_partition_intablespace   23
#define Anum_pg_partition_interval       24
#define Anum_pg_partition_boundaries     25
#define Anum_pg_partition_transit        26
#define Anum_pg_partition_reloptions     27
#define Anum_pg_partition_relfrozenxid64 28
#define Anum_pg_partition_relminmxid     29
#endif/*PG_PARTITION_H*/

