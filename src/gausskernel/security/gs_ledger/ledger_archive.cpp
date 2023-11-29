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
 * ledger_archive.cpp
 *     functions for archiving history tables.
 *
 * IDENTIFICATION
 *    src/gausskernel/security/gs_ledger/ledger_archive.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "gs_ledger/ledger_utils.h"
#include "gs_ledger/userchain.h"
#include "gs_ledger/blockchain.h"
#include "gs_ledger/ledger_archive.h"
#include "catalog/indexing.h"
#include "catalog/pg_proc.h"
#include "catalog/cstore_ctlg.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "access/hash.h"
#include "access/visibilitymap.h"
#include "libpq/md5.h"
#include "utils/uuid.h"
#include "utils/snapmgr.h"
#include "catalog/gs_global_chain.h"
#include "nodes/makefuncs.h"
#include "commands/copy.h"
#include "access/sysattr.h"
#include "utils/sec_rls_utils.h"
#include "gs_policy/gs_vector.h"
#include "access/hbucket_am.h"
#include "mb/pg_wchar.h"

#ifdef ENABLE_UT
#define static
#endif

/*
 * prepare_histback_dir -- create hist_back dir under pg_audit.
 */
static void prepare_histback_dir(void)
{
    char ledger_histback_dir[MAXPGPATH] = {0};
    int rc = snprintf_s(ledger_histback_dir, MAXPGPATH, MAXPGPATH - 1,
                        "%s/hist_bak", g_instance.attr.attr_security.Audit_directory);
    securec_check_ss(rc, "\0", "\0");

    /*
     * Create histback directory if not present; ignore errors
     */
    (void)pg_mkdir_p(g_instance.attr.attr_security.Audit_directory, S_IRWXU);
    (void)pg_mkdir_p(ledger_histback_dir, S_IRWXU);
}

/*
 * ledger_copytable -- copy rows of hist table.
 *
 * cstate: copy information state
 *
 * Note: this function is only used to copy user hist table or gchain.
 * we only copy commited data, thus for each history row, we will
 * recalculate prehash to ensure the consistency of front-to-back order.
 */
static uint64 ledger_copytable(CopyState cstate)
{
    Relation cur_rel;
    TupleDesc tuple_desc;
    FormData_pg_attribute *attr = NULL;
    ListCell *cur = NULL;
    int num_phys_attrs;
    uint64 processed = 0;
    bool is_gchain;

    cur_rel = cstate->curPartionRel;
    is_gchain = RelationGetRelid(cur_rel) == GsGlobalChainRelationId;
    tuple_desc = RelationGetDescr(cur_rel);
    attr = tuple_desc->attrs;
    num_phys_attrs = tuple_desc->natts;
    cstate->null_print_client = cstate->null_print;

    /* We use fe_msgbuf as a per-row buffer regardless of copy_dest */
    if (cstate->fe_msgbuf == NULL) {
        cstate->fe_msgbuf = makeStringInfo();
        if (IS_PGXC_COORDINATOR || g_instance.role == VSINGLENODE)
            ProcessFileHeader(cstate);
    }

    /* For each column type, get its out function. */
    cstate->out_functions = (FmgrInfo*)palloc(num_phys_attrs * sizeof(FmgrInfo));
    cstate->out_convert_funcs = (FmgrInfo*)palloc(num_phys_attrs * sizeof(FmgrInfo));
    cstate->attr_encodings = (int*)palloc(num_phys_attrs * sizeof(int));
    foreach (cur, cstate->attnumlist) {
        int attnum = lfirst_int(cur);
        Oid out_func_oid;
        bool isvarlena = false;
        getTypeOutputInfo(attr[attnum - 1].atttypid, &out_func_oid, &isvarlena);
        fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
        /* set conversion functions */
        cstate->attr_encodings[attnum - 1] = get_valid_charset_by_collation(attr[attnum - 1].attcollation);
        construct_conversion_fmgr_info(cstate->attr_encodings[attnum - 1], cstate->file_encoding,
            (void*)&cstate->out_convert_funcs[attnum - 1]);
        if (cstate->attr_encodings[attnum - 1] != cstate->file_encoding) {
            cstate->need_transcoding = true;
        }
    }

    /*
     * Create a temporary memory context that we can reset once per row to
     * recover palloc'd memory.  This avoids any problems with leaks inside
     * datatype output routines, and should be faster than retail pfree's
     * anyway. (We don't need a whole econtext as CopyFrom does.)
     */
    cstate->rowcontext = AllocSetContextCreate(
        CurrentMemoryContext, "COPY TO", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * For non-binary copy, we need to convert null_print to file
     * encoding, because it will be sent directly with CopySendString.
     */
    if (cstate->need_transcoding) {
        cstate->null_print_client = pg_server_to_any(cstate->null_print, cstate->null_print_len, cstate->file_encoding);
    }

    Tuple tuple;
    TableScanDesc scan_desc;
    uint64 rec_num = 0;
    hash32_t pre_hash;
    Datum *values = NULL;
    bool *nulls = NULL;

    values = (Datum*)palloc0(num_phys_attrs * sizeof(Datum));
    nulls = (bool*)palloc0(num_phys_attrs * sizeof(bool));

    scan_desc = scan_handler_tbl_beginscan(cur_rel, GetActiveSnapshot(), 0, NULL);

    /* For each row, we will recalculate previous hash. */
    while ((tuple = scan_handler_tbl_getnext(scan_desc, ForwardScanDirection, cur_rel)) != NULL) {
        CHECK_FOR_INTERRUPTS();
        /* Deconstruct the tuple ... faster than repeated heap_getattr */
        tableam_tops_deform_tuple2(tuple, tuple_desc, values, nulls, GetTableScanDesc(scan_desc, cur_rel)->rs_cbuf);
        if (!is_gchain) {
            char comb_str[NAMEDATALEN] = {0};
            uint64 t_ins = nulls[USERCHAIN_COLUMN_HASH_INS] ? 0 : DatumGetUInt64(values[USERCHAIN_COLUMN_HASH_INS]);
            uint64 t_del = nulls[USERCHAIN_COLUMN_HASH_DEL] ? 0 : DatumGetUInt64(values[USERCHAIN_COLUMN_HASH_DEL]);
            errno_t rc = sprintf_s(comb_str, NAMEDATALEN, "%lu%lu", t_ins, t_del);
            securec_check_ss(rc, "", "");
            gen_hist_tuple_hash(RelationGetRelid(cur_rel), comb_str, rec_num > 0, &pre_hash, &pre_hash);
            values[USERCHAIN_COLUMN_REC_NUM] = UInt64GetDatum(rec_num);
            values[USERCHAIN_COLUMN_PREVHASH] = HASH32GetDatum(&pre_hash);
        } else {
            char *db_name = DatumGetCString(DirectFunctionCall1(nameout, values[Anum_gs_global_chain_dbname - 1]));
            char *user_name = DatumGetCString(DirectFunctionCall1(nameout, values[Anum_gs_global_chain_username - 1]));
            char *rel_nsp = DatumGetCString(DirectFunctionCall1(nameout, values[Anum_gs_global_chain_relnsp - 1]));
            char *rel_name = DatumGetCString(DirectFunctionCall1(nameout, values[Anum_gs_global_chain_relname - 1]));
            char *query_string = TextDatumGetCString(values[Anum_gs_global_chain_txcommand - 1]);
            uint64 rel_hash = DatumGetUInt64(values[Anum_gs_global_chain_relhash - 1]);
            char *comb_str = set_gchain_comb_string(db_name, user_name, rel_nsp, rel_name, query_string, rel_hash);
            gen_global_hash(&pre_hash, comb_str, rec_num > 0, &pre_hash);
            values[Anum_gs_global_chain_blocknum - 1] = UInt64GetDatum(rec_num);
            values[Anum_gs_global_chain_globalhash - 1] = HASH32GetDatum(&pre_hash);
            pfree(comb_str);
        }
        /* Format and send the data */
        CopyOneRowTo(cstate, HeapTupleGetOid((HeapTuple)tuple), values, nulls);
        rec_num++;
        processed++;
    }

    scan_handler_tbl_endscan(scan_desc);

    pfree_ext(values);
    pfree_ext(nulls);
    MemoryContextDelete(cstate->rowcontext);

    return processed;
}

/*
 * ledger_docopy -- the copy process of hist table
 *
 * stmt: the copy statement constructed by callers.
 * queryString: the copy query string.
 *
 * Note: notice that this function is only used for copying hist table.
 * DO NOT use in other copy scenarios.
 */
static uint64 ledger_docopy(CopyStmt *stmt, const char *queryString)
{
    Relation rel;
    TupleDesc tup_desc;
    CopyState cstate;
    uint64 processed;
    RangeTblEntry *rte = NULL;
    Node *query = NULL;
    int attnum;

    /* Open and lock the relation, using the appropriate lock type. */
    rel = heap_openrv(stmt->relation, AccessShareLock);
    rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_RELATION;
    rte->relid = RelationGetRelid(rel);
    rte->relkind = rel->rd_rel->relkind;
    rte->requiredPerms = ACL_SELECT;

    tup_desc = RelationGetDescr(rel);
    attnum = (rte->relid == GsGlobalChainRelationId) ? Natts_gs_global_chain : USERCHAIN_COLUMN_NUM;
    /* add columns that need select permission. */
    for (int i = 1; i <= attnum; ++i) {
        int attno = i - FirstLowInvalidHeapAttributeNumber;
        rte->selectedCols = bms_add_member(rte->selectedCols, attno);
    }
    (void)ExecCheckRTPerms(list_make1(rte), true);

    cstate = BeginCopyTo(rel, query, queryString, stmt->filename, stmt->attlist, stmt->options);
    cstate->range_table = list_make1(rte);
    cstate->curPartionRel = cstate->rel;
    processed = ledger_copytable(cstate);
    EndCopyTo(cstate);

    if (rel != NULL) {
        heap_close(rel, AccessShareLock);
    }

    return processed;
}

/*
 * get_current_timestamp_text -- generate time text for name appending
 *
 * time_str: time text buffer to fill.
 *
 * Note: get current server time and remove dot.
 */
static void get_current_timestamp_text(char *time_str)
{
    const char *now = timestamptz_to_str(GetCurrentTimestamp());
    size_t time_len = strlen(now);
    size_t pos = 0;
    for (size_t i = 0; i < time_len; ++i) {
        if (now[i] >= '0' && now[i] <= '9') {
            time_str[pos++] = now[i];
        } else if (now[i] == '.') {
            break;
        }
    }
    time_str[pos] = '\0';
}

/*
 * copy_local_hist_table -- copy history table to hist_back dir.
 *
 * relid: oid of usertable
 * histname: the name of history table
 * time: time text which will append to copyfile name.
 */
static void copy_local_hist_table(Oid relid, char *histname, const char *time)
{
    errno_t rc;
    char path[MAXPGPATH] = {0};
    StringInfoData buf;
    initStringInfo(&buf);
    CopyStmt *stmt = makeNode(CopyStmt);
    RangeVar *relation = makeRangeVar("blockchain", histname, -1);
    if (!is_absolute_path(g_instance.attr.attr_security.Audit_directory)) {
        rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s/hist_bak/%s_%u_%s.hist",
                        t_thrd.proc_cxt.DataDir, g_instance.attr.attr_security.Audit_directory, histname, relid, time);
    } else {
        rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/hist_bak/%s_%u_%s.hist",
                        g_instance.attr.attr_security.Audit_directory, histname, relid, time);
    }
    securec_check_ss(rc, "", "");
    appendStringInfo(&buf, "COPY blockchain.%s to \'%s\'", histname, path);
    stmt->relation = relation;
    stmt->is_from = false;
    stmt->filename = path;
    ledger_docopy((CopyStmt *)stmt, buf.data);
}

/*
 * open_histback_dir -- open hist_back dir.
 *
 * dir_path: hist_back dir path.
 *
 * Note: caller need to close hist_back dir later.
 */
static DIR *open_histback_dir(char *dir_path)
{
    DIR *dir = NULL;
    errno_t rc;
    if (!is_absolute_path(g_instance.attr.attr_security.Audit_directory)) {
        rc = snprintf_s(dir_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s/hist_bak",
            t_thrd.proc_cxt.DataDir, g_instance.attr.attr_security.Audit_directory);
    } else {
        rc = snprintf_s(dir_path, MAXPGPATH, MAXPGPATH - 1, "%s/hist_bak",
            g_instance.attr.attr_security.Audit_directory);
    }
    securec_check_ss(rc, "", "");
    dir = AllocateDir(dir_path);
    return dir;
}

/*
 * get_histback_dir_filesize -- count all file size of hist_back dir.
 */
static uint64 get_histback_dir_filesize()
{
    DIR *dir = NULL;
    struct dirent *file = NULL;
    char dir_path[MAXPGPATH] = {0};
    errno_t rc = EOK;
    uint64 size = 0;
    dir = open_histback_dir(dir_path);
    if (dir == NULL) {
        return 0;
    }

    while ((file = ReadDir(dir, dir_path)) != NULL) {
        struct stat statbuf;
        char filepath[MAXPGPATH];
        if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0) {
            continue;
        }
        rc = snprintf_s(filepath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", dir_path, file->d_name);
        securec_check_ss(rc, "", "");

        if (file->d_type == DT_REG && stat(filepath, &statbuf) == 0) {
            size += (uint64)statbuf.st_size;
        }
    }
    FreeDir(dir);
    return size;
}

/*
 * remove_oldest_histback_file -- remove oldest file in hist_back
 *
 * Note: the defination of 'oldest' here is the earliest file that
 * was created. this function will find and try to delete this file.
 */
static uint64 remove_oldest_histback_file()
{
    DIR *dir = NULL;
    errno_t rc = EOK;
    uint64 filesize = 0;
    long min_ctime = LONG_MAX;
    char dir_path[MAXPGPATH] = {0};
    char del_file[MAXPGPATH] = {0};
    struct dirent *file = NULL;
    struct stat stat_buf;

    dir = open_histback_dir(dir_path);
    if (dir == NULL) {
        return 0;
    }

    while ((file = ReadDir(dir, dir_path)) != NULL) {
        char file_path[MAXPGPATH];
        if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0) {
            continue;
        }
        rc = snprintf_s(file_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", dir_path, file->d_name);
        securec_check_ss(rc, "", "");

        if (file->d_type == DT_REG && stat(file_path, &stat_buf) == 0) {
            if (min_ctime >= stat_buf.st_ctime) {
                min_ctime = stat_buf.st_ctime;
                rc = snprintf_s(del_file, MAXPGPATH, MAXPGPATH - 1, "%s", file_path);
                securec_check_ss(rc, "", "");
                filesize = (uint64)stat_buf.st_size;
            }
        }
    }
    FreeDir(dir);
    if (unlink(del_file) < 0) {
        ereport(WARNING, (errmsg("could not remove histbak file: %s", del_file)));
    }
    return filesize;
}

/*
 * ledger_hist_archive -- interface for history table archive
 *
 * param1: the namespace of usertable.
 * param2: the rel_name of usertable.
 *
 * Note: this function will copy and unify hash_ins and hash_del
 * of history table belonging to given usertable.
 */
Datum ledger_hist_archive(PG_FUNCTION_ARGS)
{
    if (!isRelSuperuser() && !isAuditadmin(GetUserId())) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    }
    text *rel_nsp = PG_GETARG_TEXT_PP(0);
    text *rel_name = PG_GETARG_TEXT_PP(1);
    char *table_name;
    char *table_nsp;
    char hist_name[NAMEDATALEN];
    char current_time[NAMEDATALEN + 1];
    Oid relid;
    bool res = false;
    /* Init and prepare bak dictionary. */
    prepare_histback_dir();

    table_nsp = text_to_cstring(rel_nsp);
    table_name = text_to_cstring(rel_name);
    Oid nspoid = get_namespace_oid(table_nsp, false);
    relid = get_relname_relid(table_name, nspoid);
    ledger_usertable_check(relid, nspoid, table_name, table_nsp);

    get_hist_name(relid, table_name, hist_name, nspoid, table_nsp);
    get_current_timestamp_text(current_time);
    if (g_instance.role != VCOORDINATOR) {
        /*
         * Step 1. Copy user history table.
         */
        uint64 total_histback_size = get_histback_dir_filesize();
        while (total_histback_size >= (uint64)(u_sess->attr.attr_security.Audit_SpaceLimit * 1024L)) {
            total_histback_size -= remove_oldest_histback_file();
        }
        /* Copy history table. */
        copy_local_hist_table(relid, hist_name, current_time);
        /*
         * Step 2. Do unify and truncate.
         */
        Datum values[USERCHAIN_COLUMN_NUM] = {0};
        bool nulls[USERCHAIN_COLUMN_NUM] = {true};
        bool hist_empty = true;
        bool is_null = false;
        uint64 hash_ins = 0;
        uint64 hash_del = 0;
        uint64 max_rec_num = 0;
        uint64 cur_rec_num = 0;
        TableScanDesc scan;
        HeapTuple tuple;
        /* sum all hash_ins and hash_del for unification. */
        lock_hist_hash_cache(LW_EXCLUSIVE);
        Relation histRel = heap_open(get_relname_relid(hist_name, PG_BLOCKCHAIN_NAMESPACE), AccessExclusiveLock);
        scan = heap_beginscan(histRel, SnapshotNow, 0, NULL);
        while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
            hist_empty = false;
            Datum value = heap_getattr(tuple, USERCHAIN_COLUMN_HASH_INS + 1, histRel->rd_att, &is_null);
            if (!is_null) {
                hash_ins += DatumGetUInt64(value);
                nulls[USERCHAIN_COLUMN_HASH_INS] = false;
            }
            value = heap_getattr(tuple, USERCHAIN_COLUMN_HASH_DEL + 1, histRel->rd_att, &is_null);
            if (!is_null) {
                hash_del += DatumGetUInt64(value);
                nulls[USERCHAIN_COLUMN_HASH_DEL] = false;
            }
            cur_rec_num = DatumGetUInt64(heap_getattr(tuple, USERCHAIN_COLUMN_REC_NUM + 1, histRel->rd_att, &is_null));
            if (max_rec_num <= cur_rec_num) {
                max_rec_num = cur_rec_num;
                values[USERCHAIN_COLUMN_PREVHASH] =
                    heap_getattr(tuple, USERCHAIN_COLUMN_PREVHASH + 1, histRel->rd_att, &is_null);
            }
        }
        heap_endscan(scan);
        /* Empty table should not truncate and archive any more. */
        if (hist_empty) {
            heap_close(histRel, AccessExclusiveLock);
            release_hist_hash_cache();
            res = true;
            return BoolGetDatum(res);
        }
        values[USERCHAIN_COLUMN_REC_NUM] = UInt64GetDatum(max_rec_num);
        values[USERCHAIN_COLUMN_HASH_INS] = UInt64GetDatum(hash_ins);
        values[USERCHAIN_COLUMN_HASH_DEL] = UInt64GetDatum(hash_del);
        nulls[USERCHAIN_COLUMN_REC_NUM] = false;
        nulls[USERCHAIN_COLUMN_PREVHASH] = false;
        tuple = heap_form_tuple(RelationGetDescr(histRel), values, nulls);

        /* Do real truncate. */
        heap_truncate_one_rel(histRel);

        /* Do insertion for unified row. */
        simple_heap_insert(histRel, tuple);
        CatalogUpdateIndexes(histRel,tuple);
        heap_freetuple(tuple);

        /*
         * Step 3. Flush history hash table cache.
         */
        remove_hist_recnum_cache(RelationGetRelid(histRel));
        heap_close(histRel, AccessExclusiveLock);
        release_hist_hash_cache();
        res = true;
    }
    return BoolGetDatum(res);
}

/*
 * ledger_gchain_archive -- archive gs_global_chain and unify each user rel
 *
 * Note: this function will copy gchain to hist_back dir and accumulate
 * rel_hash for each relid. Additionally, recalculate globalhash to ensure the
 * consistency of front-to-back order.
 */
Datum ledger_gchain_archive(PG_FUNCTION_ARGS)
{
    if (!isRelSuperuser() && !isAuditadmin(GetUserId())) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Permission denied.")));
    }
    bool res = false;
    /* Init and prepare bak dictionary. */
    prepare_histback_dir();

    /* gs_global_chain table should only archived in CN */
    if (g_instance.role != VCOORDINATOR && g_instance.role != VSINGLENODE) {
        return BoolGetDatum(res);
    }

    uint64 total_histback_size = get_histback_dir_filesize();
    while (total_histback_size >= (uint64)(u_sess->attr.attr_security.Audit_SpaceLimit * 1024L)) {
        total_histback_size -= remove_oldest_histback_file();
    }

    /*
     * Step 1. Using CopyStmt to copy global chain.
     */
    CopyStmt *stmt = makeNode(CopyStmt);
    RangeVar *relation = makeRangeVar("pg_catalog", GCHAIN_NAME, -1);
    char path[MAXPGPATH] = {0};
    StringInfoData buf;
    initStringInfo(&buf);
    errno_t rc;

    char current_time[NAMEDATALEN + 1];
    get_current_timestamp_text(current_time);
    if (!is_absolute_path(g_instance.attr.attr_security.Audit_directory)) {
        rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s/hist_bak/%s_%s.bak",
                        t_thrd.proc_cxt.DataDir, g_instance.attr.attr_security.Audit_directory,
                        GCHAIN_NAME, current_time);
    } else {
        rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/hist_bak/%s_%s.bak",
                        g_instance.attr.attr_security.Audit_directory, GCHAIN_NAME, current_time);
    }
    securec_check_ss(rc, "", "");
    appendStringInfo(&buf, "COPY pg_catalog.%s to \'%s\'", GCHAIN_NAME, path);
    stmt->relation = relation;
    stmt->is_from = false;
    stmt->filename = path;
    ledger_docopy((CopyStmt *)stmt, buf.data);

    /*
     * Step 2. Do unify and truncate.
     */
    TableScanDesc scan;
    HeapTuple tuple;
    HASHCTL hash_ctl;
    bool found = false;
    bool is_null = false;
    gs_stl::gs_vector<Oid, true> user_rel_arr;

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");
    /* Using hash table to do unify, each hash_entry refers to one relid informations. */
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(GChainArchEntry);
    hash_ctl.hash = oid_hash;
    hash_ctl.hcxt = THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_SECURITY);
    HTAB *global_map = hash_create("Global Archive Hash", 256, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    if (global_map == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INITIALIZE_FAILED), errmsg("could not initialize Global Archive Hash table.")));
    }
    /* Split gs_global_chain by relid, and accumulate rel_hash to a new record for each rel. */
    LWLockAcquire(GlobalPrevHashLock, LW_EXCLUSIVE);
    Relation global_rel = heap_open(GsGlobalChainRelationId, AccessExclusiveLock);
    scan = heap_beginscan(global_rel, SnapshotNow, 0, NULL);
    while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL) {
        Oid relid = DatumGetObjectId(heap_getattr(tuple, Anum_gs_global_chain_relid, global_rel->rd_att, &is_null));
        GChainArchEntry *item = (GChainArchEntry *)hash_search(global_map, &relid, HASH_ENTER, &found);
        if (!found) {
            /* If not exist in hash table, create an entry and records Datums. */
            user_rel_arr.push_back(relid);
            heap_deform_tuple(tuple, RelationGetDescr(global_rel), item->val.values, item->val.nulls);
            item->val.values[Anum_gs_global_chain_dbname - 1] =
                DirectFunctionCall1(namein, item->val.values[Anum_gs_global_chain_dbname - 1]);
            item->val.values[Anum_gs_global_chain_username - 1] =
                DirectFunctionCall1(namein, item->val.values[Anum_gs_global_chain_username - 1]);
            item->val.values[Anum_gs_global_chain_relnsp - 1] =
                DirectFunctionCall1(namein, item->val.values[Anum_gs_global_chain_relnsp - 1]);
            item->val.values[Anum_gs_global_chain_relname - 1] =
                DirectFunctionCall1(namein, item->val.values[Anum_gs_global_chain_relname - 1]);
            item->val.values[Anum_gs_global_chain_txcommand - 1] = CStringGetTextDatum("Archived.");
            item->val.rec_nums = 1;
        } else {
            /* If exists in hash table, just add rel_hash. */
            uint64 rel_hash =
                DatumGetUInt64(heap_getattr(tuple, Anum_gs_global_chain_relhash, global_rel->rd_att, &is_null));
            rel_hash += DatumGetUInt64(item->val.values[Anum_gs_global_chain_relhash - 1]);
            item->val.values[Anum_gs_global_chain_relhash - 1] = UInt64GetDatum(rel_hash);
            ++item->val.rec_nums;
        }
    }
    heap_endscan(scan);

    if (user_rel_arr.empty()) {
        heap_close(global_rel, AccessExclusiveLock);
        LWLockRelease(GlobalPrevHashLock);
        hash_destroy(global_map);
        res = true;
        return BoolGetDatum(res);
    }

    /* Do rel truncate. */
    heap_truncate_one_rel(global_rel);

    /* Insert newest record to gchain order by relid. */
    hash32_t global_hash;
    uint64 blocknum = -1;
    for (int i = user_rel_arr.size() - 1; i >= 0; --i) {
        GChainArchEntry *item = (GChainArchEntry *)hash_search(global_map, &user_rel_arr[i], HASH_FIND, &found);
        /* Prepare common string */
        const char *query_string = "Archived.";
        char *db_name =
            DatumGetCString(DirectFunctionCall1(nameout, item->val.values[Anum_gs_global_chain_dbname - 1]));
        char *user_name =
            DatumGetCString(DirectFunctionCall1(nameout, item->val.values[Anum_gs_global_chain_username - 1]));
        char *rel_nsp =
            DatumGetCString(DirectFunctionCall1(nameout, item->val.values[Anum_gs_global_chain_relnsp - 1]));
        char *rel_name =
            DatumGetCString(DirectFunctionCall1(nameout, item->val.values[Anum_gs_global_chain_relname - 1]));
        uint64 rel_hash = DatumGetUInt64(item->val.values[Anum_gs_global_chain_relhash - 1]);
        char *com_str = set_gchain_comb_string(db_name, user_name, rel_nsp, rel_name, query_string, rel_hash);
        /* Generate global_hash. */
        gen_global_hash(&global_hash, com_str, i < (int)user_rel_arr.size() - 1, &global_hash);
        blocknum += item->val.rec_nums;
        item->val.values[Anum_gs_global_chain_blocknum - 1] = UInt64GetDatum(blocknum);
        item->val.values[Anum_gs_global_chain_globalhash - 1] = HASH32GetDatum(&global_hash);
        tuple = heap_form_tuple(RelationGetDescr(global_rel), item->val.values, item->val.nulls);
        simple_heap_insert(global_rel, tuple);
        CatalogUpdateIndexes(global_rel, tuple);
        heap_freetuple(tuple);
        pfree(com_str);
    }

    hash_destroy(global_map);
    /*
     * Step 3. Flush global_hash cache.
     */
    reset_g_blocknum();
    heap_close(global_rel, AccessExclusiveLock);
    LWLockRelease(GlobalPrevHashLock);
    res = true;

    return BoolGetDatum(res);
}
