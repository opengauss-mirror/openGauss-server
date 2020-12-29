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
 * connection.h
 *        Head file for streaming engine connection.
 *
 *
 * IDENTIFICATION
 *        src/distribute/kernel/extension/streaming/include/connection.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CONTRIB_STREAMING_INCLUDE_DICTCACHE_H_
#define CONTRIB_STREAMING_INCLUDE_DICTCACHE_H_

#include "streaming/init.h"
#include "knl/knl_thread.h"
#include "knl/knl_variable.h"

#define OIDTYPE 0
#define TEXTTYPE 1

#define Natts_dict          2

#define Anum_dict_id        1
#define Anum_dict_value     2

#define DICT_SQL_MAX_LEN 1024
#define DICT_HASH_NELEM     32

void init_dict(void);
HeapTuple dict_cache_lookup(int id, Datum key, int type);
#define AEXPR_TO_SUBLINK_SQL "select distinct id from %s where value='%s'"
#define RESTARGET_TO_SUBLINK_SQL "select distinct value from %s where %s=id"

/*
 * Definition of dictionary mapping hash entry
 */
typedef struct DictEntry
{
    uint32 id;          /* hash key of DictEntry */
    HeapTuple tuple;    /* dict mapping tuple */
    bool valid;         /* valid flag*/
} DictEntry;

typedef struct DictAnalyzeContext
{
    List *a_exprs;
    List *rels;
    List *cqtabs;
    List *cols;
} DictAnalyzeContext;

DictAnalyzeContext *make_dict_analyze_context();

bool collect_aexprs(Node *node, DictAnalyzeContext *context);
void init_dict(void);
Oid dict_table_insert(int id, HeapTuple tup, Datum key);
HeapTuple dict_cache_lookup(int id, Datum key, int type);
Datum dict_get_attr(int id, HeapTuple tup, AttrNumber attr, bool *isnull);
int get_id_by_dictrelid(Oid dictrelid);
bool is_contquery_with_dict(Node* node);
bool collect_rels(Node *node, DictAnalyzeContext *context);
bool collect_cols(Node *node, DictAnalyzeContext *context);
void transform_contquery_selectstmt_with_dict(SelectStmt * selectstmt);
#endif /* CONTRIB_STREAMING_INCLUDE_DICTCACHE_H_ */
