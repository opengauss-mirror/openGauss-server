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
 * userchain.cpp
 *    functions for ledger history table achivement.
 *
 * IDENTIFICATION
 *    src/gausskernel/security/gs_ledger/userchain.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "gs_ledger/ledger_utils.h"
#include "gs_ledger/userchain.h"
#include "gs_ledger/blockchain.h"
#include "catalog/indexing.h"
#include "catalog/pg_proc.h"
#include "catalog/cstore_ctlg.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "access/hash.h"
#include "access/visibilitymap.h"
#include "access/hbucket_am.h"
#include "libpq/md5.h"
#include "utils/uuid.h"
#include "utils/snapmgr.h"
#include "catalog/gs_global_chain.h"

/*
 * create_hist_relation -- create a hist table based on the original user table.
 *
 * rel: The original user relation
 * reloptions: relation options used to define new relation
 * mainTblStmt: Some statement of the query when create the new relation.
 */
void create_hist_relation(Relation rel, Datum reloptions, CreateStmt *mainTblStmt)
{
    errno_t rc;
    char hist_name[NAMEDATALEN];
    Oid relid = RelationGetRelid(rel);
    Oid nsp_oid = PG_BLOCKCHAIN_NAMESPACE;
    Oid hist_oid;
    Oid collationObjectId[1];
    Oid classObjectId[1];
    int16 coloptions[1];
    bool shared_relation = rel->rd_rel->relisshared;

    get_hist_name(relid, get_rel_name(relid), hist_name);

    /*
     * history chain table contains all the columns from the origin user table, and then need to
     * record the command type,  blocknum, and hash value of last block record.
     */
    TupleDesc chain_desc = CreateTemplateTupleDesc(USERCHAIN_COLUMN_NUM, false);

    /* Now consider the additional columns and initilize the description */
    TupleDescInitEntry(chain_desc, USERCHAIN_COLUMN_REC_NUM + 1, "rec_num", INT8OID, -1, 0);
    TupleDescInitEntry(chain_desc, USERCHAIN_COLUMN_HASH_INS + 1, "hash_ins", HASH16OID, -1, 0);
    TupleDescInitEntry(chain_desc, USERCHAIN_COLUMN_HASH_DEL + 1, "hash_del", HASH16OID, -1, 0);
    TupleDescInitEntry(chain_desc, USERCHAIN_COLUMN_PREVHASH + 1, "pre_hash", HASH32OID, -1, 0);

    reloptions = AddInternalOption(reloptions, INTERNAL_MASK_DALTER | INTERNAL_MASK_DDELETE |
        INTERNAL_MASK_DINSERT | INTERNAL_MASK_DUPDATE);

    hist_oid = heap_create_with_catalog(hist_name, nsp_oid, rel->rd_rel->reltablespace, InvalidOid,
                                        InvalidOid, InvalidOid, rel->rd_rel->relowner, chain_desc, NIL, 'r',
                                        (rel->rd_rel->relpersistence == 't') ? 'u' : rel->rd_rel->relpersistence,
                                        shared_relation, false, true, 0, ONCOMMIT_NOOP, reloptions, false, true,
                                        NULL, REL_CMPRS_NOT_SUPPORT, NULL, false);

    /* make the history chain relation visible, else heap_open will fail */
    CommandCounterIncrement();

#ifdef ENABLE_MULTIPLE_NODES
    bool is_initdb_on_dn = false;
    /* Add to pgxc_class */
    /* When the sum of shmemNumDataNodes and shmemNumCoords equals to one,
     * the create table command is executed on datanode during initialization .
     * In this case, we do not write created table info in pgxc_class.
     */
    if ((*t_thrd.pgxc_cxt.shmemNumDataNodes + *t_thrd.pgxc_cxt.shmemNumCoords) == 1) {
        is_initdb_on_dn = true;
    }

    /* only support normal table, do not support foreign table (can be supported in the future) */
    if ((!u_sess->attr.attr_common.IsInplaceUpgrade || !IsSystemNamespace(nsp_oid)) &&
        (IS_PGXC_COORDINATOR || (isRestoreMode && mainTblStmt->distributeby != NULL && !is_initdb_on_dn))) {
        AddRelationDistribution(hist_name, hist_oid, NULL, mainTblStmt->subcluster,
                                InvalidOid, chain_desc, true);
        CommandCounterIncrement();
        /* Make sure locator info gets rebuilt */
        RelationCacheInvalidateEntry(hist_oid);
    }
#endif

    /* now create index for this new history table */
    char hist_index_name[NAMEDATALEN];
    rc = snprintf_s(hist_index_name, NAMEDATALEN, NAMEDATALEN - 1, "gs_hist_%u_index", relid);
    securec_check_ss(rc, "", "");

    /* open the previous created history chain table */
    Relation hist_rel = heap_open(hist_oid, ShareLock);
    IndexInfo *hist_index = makeNode(IndexInfo);
    hist_index->ii_NumIndexAttrs = 1;
    hist_index->ii_NumIndexKeyAttrs = 1;
    hist_index->ii_KeyAttrNumbers[0] = 1;
    hist_index->ii_Expressions = NIL;
    hist_index->ii_ExpressionsState = NIL;
    hist_index->ii_Predicate = NIL;
    hist_index->ii_PredicateState = NIL;
    hist_index->ii_ExclusionOps = NULL;
    hist_index->ii_ExclusionProcs = NULL;
    hist_index->ii_ExclusionStrats = NULL;
    hist_index->ii_Unique = true;
    hist_index->ii_ReadyForInserts = true;
    hist_index->ii_Concurrent = false;
    hist_index->ii_BrokenHotChain = false;
    hist_index->ii_PgClassAttrId = Anum_pg_class_relhasindex;

    collationObjectId[0] = InvalidOid;
    classObjectId[0] = INT4_BTREE_OPS_OID;
    coloptions[0] = 0;

    IndexCreateExtraArgs extra;
    extra.existingPSortOid = InvalidOid;
    extra.isPartitionedIndex = false;
    extra.isGlobalPartitionedIndex = false;

    index_create(hist_rel, hist_index_name, InvalidOid, InvalidOid,
                 hist_index, list_make1((void *)"rec_num"), BTREE_AM_OID,
                 rel->rd_rel->reltablespace, collationObjectId, classObjectId,
                 coloptions, (Datum) 0, true, false, false, false,
                 true, false, false, &extra, false);

    heap_close(hist_rel, NoLock);

    /* Specify dependent between history table and origin table with depend option audo. */
    ObjectAddress myself;
    ObjectAddress referenced;
    myself.classId = RelationRelationId;
    myself.objectId = hist_oid;
    myself.objectSubId = 0;
    referenced.classId = RelationRelationId;
    referenced.objectId = relid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    pfree_ext(chain_desc);
    /*
     * Make changes visible
     */
    CommandCounterIncrement();
}

/*
 * rename_hist_by_usertable -- rename hist table name by its user table and new usertable name
 *
 * relid: relation oid of user table
 * new_usertable_name: the new table name of user table
 *
 * Note: This function is used after origin user table renamed, and then caller
 * can use this function to rename the corresponding hist table name.
 */
void rename_hist_by_usertable(Oid relid, const char *new_usertable_name)
{
    Oid hist_oid = get_hist_oid(relid);
    char new_hist_name[NAMEDATALEN];
    get_hist_name(relid, new_usertable_name, new_hist_name);

    /* Do rename hist table. */
    RenameRelationInternal(hist_oid, new_hist_name);
}

/*
 * rename_hist_by_newnsp -- rename one hist table while altering schema name
 *
 * user_relid: relation oid of user table
 * new_nsp_name: the new schema name of user table
 */
void rename_hist_by_newnsp(Oid user_relid, const char *new_nsp_name)
{
    Oid hist_oid;
    char old_hist_name[NAMEDATALEN] = {0};
    char new_hist_name[NAMEDATALEN] = {0};
    get_hist_name(user_relid, get_rel_name(user_relid), old_hist_name);
    hist_oid = get_relname_relid(old_hist_name, PG_BLOCKCHAIN_NAMESPACE);
    /* Some especial tables such as foreign tables have no hist table. So make sure hist exists. */
    if (!OidIsValid(hist_oid)) {
        return;
    }
    get_hist_name(user_relid, get_rel_name(user_relid), new_hist_name, get_rel_namespace(user_relid), new_nsp_name);

    RenameRelationInternal(hist_oid, new_hist_name);
}

/*
 * rename_histlist_by_newnsp -- rename a list of hist table while altering schema name
 *
 * usertable_oid_list: relation oid list of user tables
 * new_nsp_name: the new schema name of user table
 */
void rename_histlist_by_newnsp(List *usertable_oid_list, const char *new_nsp_name)
{
    ListCell *lc = NULL;

    foreach (lc, usertable_oid_list) {
        Oid relid = (Oid)lfirst_oid(lc);
        rename_hist_by_newnsp(relid, new_nsp_name);
    }
}

/*
 * user_hash_attrno -- get the attribute number of user table's hash column.
 *
 * rd_att: tuple description of user table
 */
int user_hash_attrno(const TupleDesc rd_att)
{
    int hash_natt = -1;
    Form_pg_attribute rel_attr = NULL;
    for (int i = rd_att->natts - 1; i >= 0; i--) {
        rel_attr = rd_att->attrs[i];
        if (strcmp(rel_attr->attname.data, "hash") == 0) {
            hash_natt = i;
            break;
        }
    }
    return hash_natt;
}

/*
 * hash_combine_tuple_data -- generate hash of each attribute and return the combination string.
 *
 * data_string: combination of hash that calculate from each attribute
 * tabledesc: tuple description of user table
 * tuple: row data of user table
 */
static void hash_combine_tuple_data(char *buf, int buf_size, TupleDesc tabledesc, HeapTuple tuple)
{
    int natts = tabledesc->natts;
    int buflen = 0;
    errno_t rc = EOK;
    char hash_str[UINT64STRSIZE + 1] = {0};
    Datum *values = (Datum *) palloc0(natts * sizeof(Datum));
    bool *nulls = (bool *) palloc0(natts * sizeof(bool));
    heap_deform_tuple(tuple, tabledesc, values, nulls);
    for (int i = 0; i < natts - 1; ++i) { /* except 'hash' column. */
        if (nulls[i]) {
            continue;
        }

        uint64 col_hash = compute_hash(tabledesc->attrs[i]->atttypid, values[i], LOCATOR_TYPE_HASH);
        rc = snprintf_s(hash_str, UINT64STRSIZE + 1, UINT64STRSIZE, "%lu", col_hash);
        securec_check_ss(rc, "", "");
        rc = snprintf_s(buf + buflen, buf_size - buflen, buf_size - buflen - 1, "%s", hash_str);
        securec_check_ss(rc, "", "");
        buflen += strlen(hash_str);
    }
    pfree_ext(values);
    pfree_ext(nulls);
}

/*
 * get_user_tuple_hash -- get the hash value of usertable's tuple.
 *
 * tuple: row data of user table
 * desc: tuple description of user table
 */
uint64 get_user_tuple_hash(HeapTuple tuple, TupleDesc desc)
{
    Datum value;
    bool isnull = false;
    int hash_attno = user_hash_attrno(desc);
    value = heap_getattr(tuple, hash_attno + 1, desc, &isnull); /* get last column. */
    Assert(!isnull);
    return DatumGetUInt64(value);
}

/*
 * gen_user_tuple_hash -- generate hash of each user table's tuple.
 *
 * rel: user table
 * tuple: row data of user table
 */
static uint64 gen_user_tuple_hash(Relation rel, HeapTuple tuple)
{
    TupleDesc tabledesc = RelationGetDescr(rel);
    int data_size = UINT64STRSIZE * tabledesc->natts + 1;
    char *data_string = (char *)palloc0(data_size * sizeof(char));
    hash_combine_tuple_data(data_string, data_size, tabledesc, tuple);

    uint8 sum[16];
    if (pg_md5_binary(data_string, strlen(data_string), sum) == false) {
        pfree_ext(data_string);
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }
    uint64 result = 0;
    for (int i = 0; i < 7; i++) {
        result |= sum[4 + i];
        result = (result << 8);
    }
    result |= sum[11];

    pfree_ext(data_string);
    return result;
}

/*
 * set_user_tuple_hash -- calculate and fill the hash attribute of user table's tuple.
 *
 * tup: row data of user table
 * rel: user table
 * hash_exists: whether tuple comes with tuplehash.
 *
 * Note: if hash_exists is true, we should recompute
 * tuple hash and compare with tuplehash of itself.
 */
HeapTuple set_user_tuple_hash(HeapTuple tup, Relation rel, bool hash_exists)
{
    uint64 row_hash = gen_user_tuple_hash(rel, tup);
    int hash_attrno = user_hash_attrno(rel->rd_att);
    if (hash_exists) {
        bool is_null;
        Datum hash = heap_getattr(tup, hash_attrno + 1, rel->rd_att, &is_null);
        if (is_null || row_hash != DatumGetUInt64(hash)) {
            ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM), errmsg("Invalid tuple hash.")));
        }
        return tup;
    }
    Datum *values = NULL;
    bool *nulls = NULL;
    bool *replaces = NULL;
    /* Build modified tuple */
    int2 nattrs = RelationGetNumberOfAttributes(rel);
    values = (Datum*)palloc0(nattrs * sizeof(Datum));
    nulls = (bool*)palloc0(nattrs * sizeof(bool));
    replaces = (bool*)palloc0(nattrs * sizeof(bool));
    values[hash_attrno] = UInt64GetDatum(row_hash);
    replaces[hash_attrno] = true;
    HeapTuple newtup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);

    pfree_ext(values);
    pfree_ext(nulls);
    pfree_ext(replaces);
    return newtup;
}

/*
 * get_hist_oid -- get the oid of history table by oid, name and namespace name of user table
 *
 * relid: relation oid of user table
 * rel_name: relation name of user table
 * rel_nsp: namespace name of user table
 */
Oid get_hist_oid(Oid relid, const char *rel_name, Oid rel_nsp)
{
    if (rel_name == NULL) {
        rel_name = get_rel_name(relid);
    }
    char hist_name[NAMEDATALEN];
    get_hist_name(relid, rel_name, hist_name, rel_nsp);
    Oid hist_oid = get_relname_relid(hist_name, PG_BLOCKCHAIN_NAMESPACE);
    return hist_oid;
}

/*
 * get_user_tupleid_hash -- get the hash value of usertable's tupleid
 *
 * relation: relation of user table
 * tupleid: tupleid of user tuple
 */
uint64 get_user_tupleid_hash(Relation relation, ItemPointer tupleid)
{
    BlockNumber block;
    Buffer buffer;
    Buffer vmbuffer = InvalidBuffer;
    Page page;
    ItemId lp;
    HeapTupleData tp;
    TupleDesc tabledescr;
    uint64 result;

    tabledescr = RelationGetDescr(relation);
    /* get tuple use tupleid */
    block = ItemPointerGetBlockNumber(tupleid);
    buffer = ReadBuffer(relation, block);
    page = BufferGetPage(buffer);
    if (PageIsAllVisible(page)) {
        visibilitymap_pin(relation, block, &vmbuffer);
    }

    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

    lp = PageGetItemId(page, ItemPointerGetOffsetNumber(tupleid));
    tp.t_tableOid = RelationGetRelid(relation);
    tp.t_data = (HeapTupleHeader) PageGetItem(page, lp);
    tp.t_len = ItemIdGetLength(lp);
    tp.t_self = *tupleid;

    result = get_user_tuple_hash(&tp, tabledescr);

    UnlockReleaseBuffer(buffer);
    if (vmbuffer != InvalidBuffer) {
        ReleaseBuffer(vmbuffer);
    }

    return result;
}

/*
 * gen_hist_tuple_hash -- calculate pre_hash of hist table
 *
 * relid: relation oid of history table
 * current_block_data: data of current row, includes hash_ins and hash_del
 * pre_row_exist: when current row is the first row, it's true
 * pre_row_hash: the pre_hash value of previous row
 * hash: the result hash
 */
void gen_hist_tuple_hash(Oid relid, char *current_block_data, bool pre_row_exist,
                         hash32_t *pre_row_hash, hash32_t *hash)
{
    errno_t rc;
    int buf_size = strlen(current_block_data) + NAMEDATALEN + 1;
    char *data_string = (char *)palloc0(buf_size * sizeof(char));
    if (pre_row_exist) {
        char *pre_hash_str = DatumGetCString(DirectFunctionCall1(hash32out, HASH32GetDatum(pre_row_hash)));
        rc = snprintf_s(data_string, buf_size, buf_size - 1, "%s%s", current_block_data, pre_hash_str);
        pfree_ext(pre_hash_str);
    } else {
        char *rel_name = get_rel_name(relid);
        rc = snprintf_s(data_string, buf_size, buf_size - 1, "%s%s", current_block_data, rel_name);
    }
    securec_check_ss(rc, "", "");

    if (!pg_md5_binary(data_string, strlen(data_string), hash->data)) {
        pfree_ext(data_string);
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }
    pfree_ext(data_string);
}

/*
 * fill_hist_block -- get newest preblock in cache, and prepare new block for flushing.
 *
 * histoid: relation oid of history table
 * hash_ins: hash_ins value of current row
 * hash_del: hash_del value of current row
 * block: row id and pre_hash from current row
 */
static void fill_hist_block(Oid histoid, uint64 hash_ins, uint64 hash_del, HistBlock *block)
{
    char data[NAMEDATALEN] = {0};

    block->rec_num = get_next_recnum(histoid);
    /* Before generate previous hash, we should get current block information */
    error_t rc = sprintf_s(data, NAMEDATALEN, "%lu%lu%lu", block->rec_num, hash_ins, hash_del);
    securec_check_ss(rc, "", "");

    gen_hist_tuple_hash(histoid, data, false, NULL, &block->prev_hash);
}

/*
 * hist_table_record_internal -- append record to history table when user table is modified
 *
 * hist_oid: relation oid of history table
 * hash_ins: hash of row that added by current operation
 * hash_del: hash of row that deleted by current operation
 */
bool hist_table_record_internal(Oid hist_oid, const uint64 *hash_ins, const uint64 *hash_del)
{
    Datum values[USERCHAIN_COLUMN_NUM] = {0};
    bool nulls[USERCHAIN_COLUMN_NUM] = {false};
    bool ins_null = hash_ins == NULL;
    bool del_null = hash_del == NULL;
    uint64 t_ins = ins_null ? 0 : *hash_ins;
    uint64 t_del = del_null ? 0 : *hash_del;
    HistBlock block;

    if (!OidIsValid(hist_oid)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE), errmsg("could not find history table")));
        return false;
    }

    /* Before generate previous hash, we should get current block information */
    fill_hist_block(hist_oid, t_ins, t_del, &block);

    values[USERCHAIN_COLUMN_REC_NUM] = UInt64GetDatum(block.rec_num);
    values[USERCHAIN_COLUMN_HASH_INS] = UInt64GetDatum(t_ins);
    values[USERCHAIN_COLUMN_HASH_DEL] = UInt64GetDatum(t_del);
    values[USERCHAIN_COLUMN_PREVHASH] = HASH32GetDatum(&block.prev_hash);
    nulls[USERCHAIN_COLUMN_REC_NUM] = false;
    nulls[USERCHAIN_COLUMN_HASH_INS] = ins_null;
    nulls[USERCHAIN_COLUMN_HASH_DEL] = del_null;
    nulls[USERCHAIN_COLUMN_PREVHASH] = false;

    Relation hist_rel = heap_open(hist_oid, RowExclusiveLock);
    TupleDesc hist_desc = RelationGetDescr(hist_rel);

    HeapTuple tuple = heap_form_tuple(hist_desc, values, nulls);
    simple_heap_insert(hist_rel, tuple);
    heap_freetuple(tuple);
    heap_close(hist_rel, RowExclusiveLock);

    return true;
}

/*
 * hist_table_record_insert -- append a record while inserting into user table
 *
 * rel: relation of user table
 * tup: the tuple which is inserted into user table
 * res_hash: delta of user table hash
 */
bool hist_table_record_insert(Relation rel, HeapTuple tup, uint64 *res_hash)
{
    /* check all inputs are avaliable */
    if (tup == NULL || rel == NULL) {
        return false;  /* Do some thing */
    }

    uint64 hash_ins = get_user_tuple_hash(tup, rel->rd_att);
    Oid hist_oid = get_hist_oid(RelationGetRelid(rel), RelationGetRelationName(rel), RelationGetNamespace(rel));
    *res_hash = hash_ins;
    return hist_table_record_internal(hist_oid, &hash_ins, NULL);
}

/*
 * hist_table_record_delete -- append a record while deleting from user table
 *
 * rel: relation of user table
 * hash_del: the hash of deleted tuple
 * res_hash: delta of user table hash
 */
bool hist_table_record_delete(Relation rel, uint64 hash_del, uint64 *res_hash)
{
    Oid hist_oid = get_hist_oid(RelationGetRelid(rel), RelationGetRelationName(rel), RelationGetNamespace(rel));
    *res_hash = -hash_del;
    /* insert history record into userchain table */
    return hist_table_record_internal(hist_oid, NULL, &hash_del);
}

/*
 * hist_table_record_update -- append a record while updating user table
 *
 * rel: relation of user table
 * newtup: the tupleid which is updated from user table
 * hash_del: the hash of deleted tuple
 * res_hash: delta of user table hash
 */
bool hist_table_record_update(Relation rel, HeapTuple newtup, uint64 hash_del, uint64 *res_hash)
{
    uint64 hash_ins = get_user_tuple_hash(newtup, rel->rd_att);
    Oid hist_oid = get_hist_oid(RelationGetRelid(rel), RelationGetRelationName(rel), RelationGetNamespace(rel));
    *res_hash = hash_ins - hash_del;
    /* insert history record into userchain table */
    return hist_table_record_internal(hist_oid, &hash_ins, &hash_del);
}

/*
 * get_copyfrom_line_relhash -- extract hash from each copyfrom line
 *
 * row_data: string line from txt
 * len: length of row_data
 * hash_colno: the number of split char before hash text
 * split: split char
 * hash: the buffer of getting hash.
 */
bool get_copyfrom_line_relhash(const char *row_data, int len, int hash_colno, char split, uint64 *hash)
{
    int pos;
    /* Not found hash column. */
    if (hash_colno == -1) {
        return false;
    }
    for (pos = 0; pos < len && hash_colno > 0; ++pos) {
        if (row_data[pos] == split) {
            --hash_colno;
        }
    }
    if (hash_colno == 0) {
        int remain_len = len - pos;
        const char *hash_str = row_data + pos;
        if (remain_len > 0) {
            *hash = DatumGetUInt64(DirectFunctionCall1(hash16in, CStringGetDatum(hash_str)));
            return true;
        }
    }
    return false;
}
