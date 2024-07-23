/*-------------------------------------------------------------------------
 *
 * pg_proc_ext.h
 *   extension of pg_proc
 *
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
 * -------------------------------------------------------------------------
 *
 * src/include/catalog/pg_proc_ext.h
 *
 * NOTES
 *   the genbki.pl script reads this file and generates .bki
 *   information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROC_EXT_H
#define PG_PROC_EXT_H

#include "catalog/genbki.h"
#include "nodes/parsenodes_common.h"

/* ----------------
 *     pg_proc_ext definition.    cpp turns this into
 *     typedef struct FormData_pg_proc_ext
 * ----------------
 */
#define ProcedureExtensionRelationId 3483
#define ProcedureExtensionRelationId_Rowtype_Id 3484

CATALOG(pg_proc_ext,3483) BKI_WITHOUT_OIDS BKI_ROWTYPE_OID(3484) BKI_SCHEMA_MACRO
{
    Oid   proc_oid;                    /* procedure oid */
    int2  parallel_cursor_seq;         /* specify which cursor arg to be parallel for function */
    int2  parallel_cursor_strategy;    /* specify what strategy to partition for parallel cursor */
#ifdef CATALOG_VARLEN
    text  parallel_cursor_partkey[1];  /* specify what keys to partition for parallel cursor */
#endif
} FormData_pg_proc_ext;

/* ----------------
 *     Form_pg_proc_ext corresponds to a pointer to a tuple with
 *     the format of pg_proc_ext relation.
 * ----------------
 */
typedef FormData_pg_proc_ext *Form_pg_proc_ext;

/* ----------------
 *     compiler constants for pg_proc_ext
 * ----------------
 */
#define Natts_pg_proc_ext                          4
#define Anum_pg_proc_ext_proc_oid                  1
#define Anum_pg_proc_ext_parallel_cursor_seq       2
#define Anum_pg_proc_ext_parallel_cursor_strategy  3
#define Anum_pg_proc_ext_parallel_cursor_partkey   4

extern void InsertPgProcExt(Oid oid, FunctionPartitionInfo* partInfo);
extern int2 GetParallelCursorSeq(Oid oid);
extern FunctionPartitionStrategy GetParallelStrategyAndKey(Oid oid, List** partkey);
extern void DeletePgProcExt(Oid oid);

#endif   /* PG_PROC_EXT_H */

