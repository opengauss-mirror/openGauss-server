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
 * ledger_utils.h
 *    Declaration of functions for common tools and ledger hash cache management.
 *
 * IDENTIFICATION
 *    src/include/gs_ledger/ledger_utils.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_LEDGER_UTILS_H
#define GS_LEDGER_UTILS_H

#include "postgres.h"
#include "access/transam.h"
#include "utils/atomic.h"
#include "storage/lock/lwlock.h"
#include "nodes/parsenodes.h"
#include "commands/tablecmds.h"
#include "utils/builtins.h"
#include "catalog/pg_class.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/uuid.h"
#include "catalog/heap.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_opclass.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/gs_global_chain.h"
#include "parser/parse_type.h"

#define GCHAIN_NAME "gs_global_chain"
#define PREVIOUS_HASH_LEN 65

typedef struct RecNumItem {
    Oid relid;
    pg_atomic_uint64 rec_num;
} RecNumItem;

typedef struct HistBlock {
    uint64 rec_num;
    hash32_t prev_hash;
} HistBlock;

typedef struct HistBlockItem {
    Oid relid;
    HistBlock block;
} HistBlockItem;

typedef struct GlobalPrevBlock {
    uint64 blocknum;
    hash32_t globalhash;
} GlobalPrevBlock;

typedef struct GChainArchItem {
    uint64 rec_nums;
    Datum values[Natts_gs_global_chain];
    bool nulls[Natts_gs_global_chain];
} GChainArchItem;

typedef struct GChainArchEntry {
    Oid relid;
    GChainArchItem val;
} GChainArchEntry;

typedef struct {
    const char* name;
    int hash_offset;
} hash_offset_pair;

enum HistTableColumn {
    USERCHAIN_COLUMN_REC_NUM,
    USERCHAIN_COLUMN_HASH_INS,
    USERCHAIN_COLUMN_HASH_DEL,
    USERCHAIN_COLUMN_PREVHASH,
    USERCHAIN_COLUMN_NUM
};

uint64 get_next_g_blocknum();
void reset_g_blocknum();

uint64 reload_g_rec_num(Oid histoid);
uint64 get_next_recnum(Oid histoid);
bool remove_hist_recnum_cache(Oid histoid);

void lock_gchain_cache(LWLockMode mode);
void release_gchain_cache();

void lock_hist_hash_cache(LWLockMode mode);
void release_hist_hash_cache();

uint64 hash_combiner(List *relhash_list);
Oid get_target_query_relid(List* rte_list, int resultRelation);
bool is_ledger_usertable(Oid relid);
bool is_ledger_hist_table(Oid relid);
bool is_ledger_related_rel(Relation rel);
bool ledger_usertable_check(Oid relid, Oid nspoid, const char *table_name, const char *table_nsp);

List *namespace_get_depended_relid(Oid nspid);
void check_ledger_attrs_support(List *attrs);
bool get_ledger_msg_hash(char *message, uint64 *hash, int *msg_len = NULL);
bool querydesc_contains_ledger_usertable(QueryDesc *query_desc);
bool get_hist_name(Oid relid, const char *rel_name, char *hist_name,
                   Oid nsp_oid = InvalidOid, const char *nsp_name = NULL);
bool is_ledger_rowstore(List *defList);
bool is_ledger_hashbucketstore(List *defList);
void ledger_check_switch_schema(Oid old_nsp, Oid new_nsp);

#endif
