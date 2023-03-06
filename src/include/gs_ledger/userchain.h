/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * userchain.h
 *    Declaration of functions for ledger history table management.
 *
 * IDENTIFICATION
 *    src/include/gs_ledger/userchain.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_USERCHAIN_H
#define GS_USERCHAIN_H
#include "c.h"
#include "nodes/parsenodes_common.h"
#include "storage/item/itemptr.h"
#include "utils/relcache.h"
#include "utils/uuid.h"

#define UINT64STRSIZE 20

/* DDL support for user_table and hist_table */
void create_hist_relation(Relation rel, Datum reloptions, CreateStmt *mainTblStmt);
void rename_hist_by_usertable(Oid relid, const char *newname);
void rename_hist_by_newnsp(Oid user_relid, const char *new_nsp_name);
void rename_histlist_by_newnsp(List *usertable_oid_list, const char *new_nsp_name);

/* DML support for user table */
HeapTuple set_user_tuple_hash(HeapTuple tup, Relation rel, TupleTableSlot *slot, bool hash_exists = false);
int user_hash_attrno(const TupleDesc rd_att);
uint64 get_user_tuple_hash(HeapTuple tuple, TupleDesc desc);
uint64 get_user_tupleid_hash(Relation relation, ItemPointer tupleid);
bool get_copyfrom_line_relhash(const char *row_data, int len, int hash_colno, char split, uint64 *hash);

/* ledger check function */
Oid get_hist_oid(Oid relid, const char *rel_name = NULL, Oid rel_nsp = InvalidOid);
void gen_hist_tuple_hash(Oid relid, char *info, bool pre_row_exist, hash32_t *pre_row_hash, hash32_t *hash);
bool hist_table_record_insert(Relation rel, HeapTuple tup, uint64 *res_hash);
bool hist_table_record_update(Relation rel, HeapTuple newtup, uint64 hash_del, uint64 *res_hash);
bool hist_table_record_delete(Relation rel, uint64 hash_del, uint64 *res_hash);
bool hist_table_record_internal(Oid hist_oid, const uint64 *hash_ins, const uint64 *hash_del);
#endif