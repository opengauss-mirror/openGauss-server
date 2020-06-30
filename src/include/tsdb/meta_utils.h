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
 * meta_utils.h
 *      the utils of meta data
 *
 * IDENTIFICATION
 *        src/include/tsdb/meta_utils.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef TSDB_META_UTILS_H
#define TSDB_META_UTILS_H

#include "access/attnum.h"
#include "nodes/parsenodes.h"
#include "utils/relcache.h"
#include "utils/hsearch.h"
#include "utils/snapshot.h"
#include "tsdb/constant_def.h"
#include "catalog/pg_type.h"

typedef enum {
    META_TABLE_CUDESC = 0,
    META_TABLE_TAGS = 1,
    META_TABLE_INVALID
} META_TABLE_TYPE;

class MetaUtils {
public:
    static void create_ts_store_tables(Oid relOid, Datum reloptions, CreateStmt *mainTblStmt);
private:
    static Oid create_tag_table(Relation rel, Datum reloptions);
    static Oid create_tag_index(Oid relid, const char* tbl_name, int index_num, List *index_col_names);
    static void update_catlog(Oid rel_oid, Oid tag_relid);   
    template<META_TABLE_TYPE type>
    static void get_meta_tbl_name(Oid rel_oid, char* tbl_name, int tbl_name_len);
    static void get_meta_tbl_idx_name(const char* tbl_name, char* idx_name, int idx_name_len); 
};

#endif /* TSDB_META_UTILS_H */

