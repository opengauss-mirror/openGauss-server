/* -------------------------------------------------------------------------
 *
 * reloptions.cpp
 *	  Core support for relation options (pg_class.reloptions)
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/common/reloptions.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "knl/knl_variable.h"

#include "access/gist_private.h"
#include "access/hash.h"
#include "access/nbtree.h"
#include "access/reloptions.h"
#include "access/spgist.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/tablespace.h"
#include "nodes/makefuncs.h"
#include "pgxc/redistrib.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/utils/delta_utils.h"
#endif
#include "tsearch/ts_public.h"
#include "utils/array.h"
#include "utils/attoptcache.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "tde_key_management/tde_key_manager.h"
#include "tde_key_management/tde_key_storage.h"

/*
 * Contents of pg_class.reloptions
 *
 * To add an option:
 *
 * (i) decide on a type (integer, real, bool, string), name, default value,
 * upper and lower bounds (if applicable); for strings, consider a validation
 * routine.
 * (ii) add a record below (or use add_<type>_reloption).
 * (iii) add it to the appropriate options struct (perhaps StdRdOptions)
 * (iv) add it to the appropriate handling routine (perhaps
 * default_reloptions)
 * (v) don't forget to document the option
 *
 * Note that we don't handle "oids" in relOpts because it is handled by interpretOidsOption().
 */
/* value check functions for reloptions */
static void ValidateStrOptOrientation(const char *val);
static void  ValidateStrOptIndexsplit(const char *val);
static void ValidateStrOptCompression(const char *val);
static void ValidateStrOptTableAccessMethod(const char* val);
static void ValidateStrOptTTL(const char *val);
static void ValidateStrOptPeriod(const char *val);
static void ValidateStrOptPartitionInterval(const char *val);
static void ValidateStrOptTimeColumn(const char *val);
static void ValidateStrOptTTLInterval(const char *val);
static void ValidateStrOptGatherInterval(const char *val);
static void ValidateStrOptSwInterval(const char *val);
static void ValidateStrOptVersion(const char *val);
static void ValidateStrOptSpcFileSystem(const char *val);
static void ValidateStrOptSpcAddress(const char *val);
static void ValidateStrOptSpcCfgPath(const char *val);
static void ValidateStrOptSpcStorePath(const char *val);
static void check_append_mode(const char *val);
static void ValidateStrOptStringOptimize(const char *val);
static void ValidateStrOptEncryptAlgo(const char *val);
static void ValidateStrOptDekCipher(const char *val);
static void ValidateStrOptCmkId(const char *val);

static relopt_bool boolRelOpts[] = {
    {{ "autovacuum_enabled", "Enables autovacuum in this relation", RELOPT_KIND_HEAP | RELOPT_KIND_TOAST }, true },
    {{ "user_catalog_table",
       "Declare a table as an additional catalog table, e.g. for the purpose of logical replication",
       RELOPT_KIND_HEAP }, false },
    {{ "fastupdate", "Enables \"fast update\" feature for this GIN index", RELOPT_KIND_GIN }, true },
    {{ "security_barrier", "View acts as a row security barrier", RELOPT_KIND_VIEW }, false },
    {{ "enable_rowsecurity", "Enable row level security or not", RELOPT_KIND_HEAP }, false },
    {{ "force_rowsecurity", "Row security forced for owners or not", RELOPT_KIND_HEAP }, false },
    {{"enable_tsdb_delta", "Enables delta table for this timeseries relation", RELOPT_KIND_HEAP}, false},
    {{ "punctuation_ignore", "Ignore punctuation in zhparser/N-gram text search praser",
       RELOPT_KIND_ZHPARSER | RELOPT_KIND_NPARSER }, true },
    {{ "grapsymbol_ignore", "ignore grapsymbol in N-gram text search praser", RELOPT_KIND_NPARSER }, false },
    {{ "seg_with_duality", "segmente interfacing idle words with duality in zhparser text search praser",
       RELOPT_KIND_ZHPARSER }, false },
    {{ "multi_short", "segmente long words to short words in zhparser text search praser", RELOPT_KIND_ZHPARSER },
     true },
    {{ "multi_duality", "segmente long words with duality in zhparser text search praser", RELOPT_KIND_ZHPARSER },
     false },
    {{ "multi_zmain", "segmente main word from long words in zhparser text search praser", RELOPT_KIND_ZHPARSER },
     false },
    {{ "multi_zall", "segmente all word from long words in zhparser text search praser", RELOPT_KIND_ZHPARSER },
     false },
    {{ "ignore_enable_hadoop_env", "ignore enable_hadoop_env option", RELOPT_KIND_HEAP }, false },
    {{ "hashbucket", "Enables hashbucket in this relation", RELOPT_KIND_HEAP }, false },
    {{ "segment", "Enables segment in this relation", RELOPT_KIND_HEAP}, false},
    {{ "primarynode", "Enables primarynode for replicatition relation", RELOPT_KIND_HEAP }, false },
    {{ "on_commit_delete_rows", "global temp table on commit options", RELOPT_KIND_HEAP}, true},
    {{ "crossbucket", "Enables cross bucket index creation in this index relation", RELOPT_KIND_BTREE}, false },
    {{ "enable_tde", "enable table's level transparent data encryption", RELOPT_KIND_HEAP }, false },
    {{ "hasuids", "Enables uids in this relation", RELOPT_KIND_HEAP }, false },
    {{ "compress_byte_convert", "Whether do byte convert in compression", RELOPT_KIND_HEAP | RELOPT_KIND_BTREE},
     false },
    {{ "compress_diff_convert", "Whether do diiffer convert in compression", RELOPT_KIND_HEAP | RELOPT_KIND_BTREE},
     false },
    /* list terminator */
    {{NULL}}
};

static relopt_int intRelOpts[] = {
    {{ "fillfactor", "Packs table pages only to this percentage", RELOPT_KIND_HEAP },
     HEAP_DEFAULT_FILLFACTOR,
     HEAP_MIN_FILLFACTOR,
     100 },
    {{ "fillfactor", "Packs btree index pages only to this percentage", RELOPT_KIND_BTREE },
     BTREE_DEFAULT_FILLFACTOR,
     BTREE_MIN_FILLFACTOR,
     100 },
    {{ "fillfactor", "Packs hash index pages only to this percentage", RELOPT_KIND_HASH },
     HASH_DEFAULT_FILLFACTOR,
     HASH_MIN_FILLFACTOR,
     100 },
    {{ "fillfactor", "Packs gist index pages only to this percentage", RELOPT_KIND_GIST },
     GIST_DEFAULT_FILLFACTOR,
     GIST_MIN_FILLFACTOR,
     100 },
    {{ "fillfactor", "Packs spgist index pages only to this percentage", RELOPT_KIND_SPGIST },
     SPGIST_DEFAULT_FILLFACTOR,
     SPGIST_MIN_FILLFACTOR,
     100 },
    {{ "autovacuum_vacuum_threshold", "Minimum number of tuple updates or deletes prior to vacuum",
       RELOPT_KIND_HEAP | RELOPT_KIND_TOAST },
     -1,
     0,
     INT_MAX },
    {{ "autovacuum_analyze_threshold", "Minimum number of tuple inserts, updates or deletes prior to analyze",
       RELOPT_KIND_HEAP },
     -1,
     0,
     INT_MAX },
    {{ "autovacuum_vacuum_cost_delay", "Vacuum cost delay in milliseconds, for autovacuum",
       RELOPT_KIND_HEAP | RELOPT_KIND_TOAST },
     -1,
     0,
     100 },
    {{ "autovacuum_vacuum_cost_limit", "Vacuum cost amount available before napping, for autovacuum",
       RELOPT_KIND_HEAP | RELOPT_KIND_TOAST },
     -1,
     1,
     10000 },
#ifdef ENABLE_MULTIPLE_NODES
    {
        { "tsdb_deltamerge_interval", "job interval for tsdb delta merge", RELOPT_KIND_HEAP},
        Tsdb::DELTAMERGE_INTERVAL_DEFAULT,
        Tsdb::DELTAMERGE_INTERVAL_MIN,
        Tsdb::DELTAMERGE_INTERVAL_MAX
    },
    {
        { "tsdb_deltamerge_threshold", "if the number of rows in a tsdb delta table is less than "\
        "tsdb_deltamerge_threshold, skip delta merge", RELOPT_KIND_HEAP},
        Tsdb::DELTAMERGE_THRESHOLD_DEFAULT,
        Tsdb::DELTAMERGE_THRESHOLD_MIN,
        Tsdb::DELTAMERGE_THRESHOLD_MAX
    },
    {
        { "tsdb_deltainsert_threshold", "insert data into delta table if the number of rows of one insert"\
        " operation is less than tsdb_deltainsert_threshold", RELOPT_KIND_HEAP},
        Tsdb::DELTAINSERT_THRESHOLD_DEFAULT,
        Tsdb::DELTAINSERT_THRESHOLD_MIN,
        Tsdb::DELTAINSERT_THRESHOLD_MAX
    },
#endif
    {{ "max_batchrow", "the upmost rows at each batch inserting", RELOPT_KIND_HEAP | RELOPT_KIND_PSORT },
     RelDefaultFullCuSize,
     10 * BatchMaxSize,
     RelMaxFullCuSize },
    {{ "deltarow_threshold", "if smaller than it, insert into delta table; otherwise insert into normal table",
       RELOPT_KIND_HEAP | RELOPT_KIND_PSORT },
     RelDefaultDletaRows,
     0,
     9999 },
    {{ "partial_cluster_rows", "row numbers of partial cluster feature", RELOPT_KIND_HEAP | RELOPT_KIND_PSORT },
     RelDefaultPartialClusterRows,
     -1,
     0x7fffffff },
    {{ "internal_mask", "internal mask", RELOPT_KIND_HEAP | RELOPT_KIND_PSORT }, 0, 0, 0x7fffffff },
    {{ "gin_pending_list_limit", "Maximum size of the pending list for this GIN index, in kilobytes.",
       RELOPT_KIND_GIN },
     -1,
     64,
     MAX_KILOBYTES },
    {{ "gram_size", "Gram size for N-gram text search praser.", RELOPT_KIND_NPARSER }, 2, 1, 4 },

    /* COMPRESSLEVEL option */
    {
        { "compresslevel", "column relation's compress level",
          /* in fact PSORT is also a heap relation */
          RELOPT_KIND_HEAP | RELOPT_KIND_PSORT },
        REL_MIN_COMPRESSLEVEL, /* default value of compress level */
        REL_MIN_COMPRESSLEVEL, /* min value of compress level */
        REL_MAX_COMPRESSLEVEL  /* max value of compress level */
    },

    /* append_mode_internal option */
    {
        { "append_mode_internal", "internal value for append_mode",
          /* in fact PSORT is also a heap relation */
          RELOPT_KIND_HEAP | RELOPT_KIND_PSORT },
        REDIS_REL_NORMAL,     /* not using append mode */
        REDIS_REL_INVALID,    /* min value of append mode */
        REDIS_REL_DESTINATION /* REDIS_REL_DESTINATION is the max value of append mode that can set by users. */
    },

    {{ "rel_cn_oid", "rel oid on coordinator", RELOPT_KIND_HEAP }, 0, 0, 2000000000 },
    {{ "exec_step", "redis exec step", RELOPT_KIND_HEAP }, 0, 1, 4 },
    {{ "init_td", "number of td slots", RELOPT_KIND_HEAP }, UHEAP_DEFAULT_TD, UHEAP_MIN_TD, UHEAP_MAX_TD },
    {{ "bucketcnt", "number of bucket map counts", RELOPT_KIND_HEAP }, 0, 32, 16384 },
    {
        {
            "parallel_workers",
            "Number of parallel processes that can be used per executor node for this relation.",
            RELOPT_KIND_HEAP,
        },
        0, 1, 32
    },
    {{ "compress_level", "Level of page compression.", RELOPT_KIND_HEAP | RELOPT_KIND_BTREE}, 0, -31, 31},
    {{ "compresstype", "compress type (none, pglz or zstd).", RELOPT_KIND_HEAP | RELOPT_KIND_BTREE}, 0, 0, 2},
    {{ "compress_chunk_size", "Size of chunk to store compressed page.", RELOPT_KIND_HEAP | RELOPT_KIND_BTREE},
     BLCKSZ / 2,
     BLCKSZ / 16,
     BLCKSZ / 2},
    {{ "compress_prealloc_chunks", "Number of prealloced chunks for each block.", RELOPT_KIND_HEAP | RELOPT_KIND_BTREE},
     0,
     0,
     7},
    /* list terminator */
    {{NULL}}
};

static relopt_int64 int64RelOpts[] = {
    {{ "autovacuum_freeze_min_age", "Minimum age at which VACUUM should freeze a table row, for autovacuum",
       RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock },
     INT64CONST(-1),
     INT64CONST(0),
     INT64CONST(1000000000) },
    {{ "autovacuum_freeze_max_age", "Age at which to autovacuum a table to prevent excessive clog",
       RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock },
     INT64CONST(-1),
     INT64CONST(100000),
     INT64CONST(2000000000) },
    {{ "autovacuum_freeze_table_age", "Age at which VACUUM should perform a full table sweep to freeze row versions",
       RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock },
     INT64CONST(-1),
     INT64CONST(0),
     INT64CONST(2000000000) },
    {{ "create_time", "redis tmp table create time",
       RELOPT_KIND_HEAP },
     INT64CONST(0),
     INT64CONST(1),
     INT64CONST(INT64_MAX) },
    /* list terminator */
    {{NULL}}
};

static relopt_real realRelOpts[] = {
    {{ "autovacuum_vacuum_scale_factor",
       "Number of tuple updates or deletes prior to vacuum as a fraction of reltuples",
       RELOPT_KIND_HEAP | RELOPT_KIND_TOAST },
     -1,
     0.0,
     100.0 },
    {{ "autovacuum_analyze_scale_factor",
       "Number of tuple inserts, updates or deletes prior to analyze as a fraction of reltuples", RELOPT_KIND_HEAP },
     -1,
     0.0,
     100.0 },
    {{ "seq_page_cost", "Sets the planner's estimate of the cost of a sequentially fetched disk page.",
       RELOPT_KIND_TABLESPACE },
     -1,
     0.0,
     DBL_MAX },
    {{ "random_page_cost", "Sets the planner's estimate of the cost of a nonsequentially fetched disk page.",
       RELOPT_KIND_TABLESPACE },
     -1,
     0.0,
     DBL_MAX },
    {{ "n_distinct",
       "Sets the planner's estimate of the number of distinct values appearing in a column (excluding child "
       "relations).",
       RELOPT_KIND_ATTRIBUTE },
     0,
     -1.0,
     DBL_MAX },
    {{ "n_distinct_inherited",
       "Sets the planner's estimate of the number of distinct values appearing in a column (including child "
       "relations).",
       RELOPT_KIND_ATTRIBUTE },
     0,
     -1.0,
     DBL_MAX },
    /* list terminator */
    {{NULL}}
};

static relopt_string stringRelOpts[] = {
    {{ "split_flag", "split flag for pound text search praser.", RELOPT_KIND_PPARSER }, 2, false, NULL, "#" },
    {{ "buffering", "Enables buffering build for this GiST index", RELOPT_KIND_GIST },
     4,
     false,
     gistValidateBufferingOption,
     "auto" },

    {
        { "orientation", "row-store, col-store, orc-store, inplace-store or timeseries", RELOPT_KIND_HEAP },
        10,
        false,
        ValidateStrOptOrientation,
        ORIENTATION_ROW,
    },
    {
        {"indexsplit", "default, insertpt", RELOPT_KIND_BTREE},
        7,
        false,
        ValidateStrOptIndexsplit,
        INDEXSPLIT_OPT_DEFAULT,
    },
    {
        { "ttl", "time to live for timeseries data management", RELOPT_KIND_HEAP },
        9,
        false,
        ValidateStrOptTTL,
        TIME_UNDEFINED,
    },
    {
        { "period", "partition range for timeseries data management", RELOPT_KIND_HEAP },
        9,
        false,
        ValidateStrOptPeriod,
        TIME_UNDEFINED,
    },
    {
        { "partition_interval", "partition interval for streaming contview table", RELOPT_KIND_HEAP },
        9,
        false,
        ValidateStrOptPartitionInterval,
        TIME_UNDEFINED,
    },
    {
        { "time_column", "time column for streaming contview table", RELOPT_KIND_HEAP },
        9,
        false,
        ValidateStrOptTimeColumn,
        COLUMN_UNDEFINED,
    },
    {
        { "ttl_interval", "ttl interval for streaming contview table", RELOPT_KIND_HEAP },
        9,
        false,
        ValidateStrOptTTLInterval,
        TIME_UNDEFINED,
    },
    {
        { "gather_interval", "gather interval for streaming contview table", RELOPT_KIND_HEAP },
        9,
        false,
        ValidateStrOptGatherInterval,
        TIME_UNDEFINED,
    },
    {
        { "sw_interval", "sliding window interval for streaming contquery table", RELOPT_KIND_HEAP },
        9,
        false,
        ValidateStrOptSwInterval,
        TIME_UNDEFINED,
    },
    {
        { "version", "store version", RELOPT_KIND_HEAP },
        4,
        false,
        ValidateStrOptVersion,
        ORC_VERSION_012,
    },
    {
        { "compression", "which compression level applied to, or not compressed", RELOPT_KIND_HEAP },
        6,
        false,
        ValidateStrOptCompression,
        COMPRESSION_LOW,
    },
    {
        { "filesystem", "which filesystem applied", RELOPT_KIND_TABLESPACE },
        7,
        false,
        ValidateStrOptSpcFileSystem,
        FILESYSTEM_GENERAL,
    },
    {
        { "address", "which address server applied", RELOPT_KIND_TABLESPACE },
        6,
        false,
        ValidateStrOptSpcAddress,
        "",
    },
    {
        { "cfgpath", "config information path", RELOPT_KIND_TABLESPACE },
        6,
        false,
        ValidateStrOptSpcCfgPath,
        "",
    },
    {
        { "storepath", "store information path", RELOPT_KIND_TABLESPACE },
        6,
        false,
        ValidateStrOptSpcStorePath,
        "",
    },
    {
        { "append_mode", "set relation insert under append mode", RELOPT_KIND_HEAP },
        6,
        false,
        check_append_mode,
        "",
    },

    {
        { "start_ctid_internal", "set relation start ctid during redistribution", RELOPT_KIND_HEAP },
        6,
        false,
        NULL,
        "",
    },

    {
        { "end_ctid_internal", "set relation end ctid during redistribution", RELOPT_KIND_HEAP },
        6,
        false,
        NULL,
        "",
    },

    {
        { "merge_list", "set merge_list as bucketid1:start1:end1;bucketid1:start1:end1...", RELOPT_KIND_HEAP },
        0,
        true,
        NULL,
        "",
    },
    {
        {"storage_type", "Specifies the Table accessor routines",
         RELOPT_KIND_HEAP | RELOPT_KIND_BTREE | RELOPT_KIND_TOAST},
        strlen(TABLE_ACCESS_METHOD_ASTORE),
        false,
        ValidateStrOptTableAccessMethod,
        TABLE_ACCESS_METHOD_ASTORE,
    },
    {
        { "dek_cipher", "The cipher of TDE dek", RELOPT_KIND_HEAP },
        0,
        false,
        ValidateStrOptDekCipher,
        "",
    },
    {
        { "cmk_id", "The id of TDE cmk", RELOPT_KIND_HEAP },
        0,
        false,
        ValidateStrOptCmkId,
        "",
    },
    {
        { "encrypt_algo", "The algo of TDE", RELOPT_KIND_HEAP },
        0,
        false,
        ValidateStrOptEncryptAlgo,
        "",
    },
    {
        {"wait_clean_gpi", "Whether to wait for gpi cleanup", RELOPT_KIND_HEAP },
        1,
        false,
        CheckWaitCleanGpi,
        "n",
    },
    {
        {"wait_clean_cbi", "Whether to wait for cbi cleanup", RELOPT_KIND_BTREE },
        1,
        false,
        CheckWaitCleanCbi,
        "n",
    },
    {
        { "string_optimize", "string optimize for streaming contview table", RELOPT_KIND_HEAP },
        9,
        false,
        ValidateStrOptStringOptimize,
        COLUMN_UNDEFINED,
    },
    /* list terminator */
    {{NULL}}
};

static void initialize_reloptions(void);
static void parse_one_reloption(relopt_value *option, const char *text_str, int text_len, bool validate);

/*
 * initialize_reloptions
 *		initialization routine, must be called before parsing
 *
 * Initialize the relOpts array and fill each variable's type and name length.
 */
static void initialize_reloptions(void)
{
    int i;
    int j;

    j = 0;
    for (i = 0; boolRelOpts[i].gen.name; i++)
        j++;
    for (i = 0; intRelOpts[i].gen.name; i++)
        j++;
    for (i = 0; int64RelOpts[i].gen.name; i++)
        j++;
    for (i = 0; realRelOpts[i].gen.name; i++)
        j++;
    for (i = 0; stringRelOpts[i].gen.name; i++)
        j++;
    j += t_thrd.relopt_cxt.num_custom_options;

    if (t_thrd.relopt_cxt.relOpts) {
        pfree(t_thrd.relopt_cxt.relOpts);
        t_thrd.relopt_cxt.relOpts = NULL;
    }
    t_thrd.relopt_cxt.relOpts = (relopt_gen **)MemoryContextAlloc(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), (j + 1) * sizeof(relopt_gen *));

    j = 0;
    for (i = 0; boolRelOpts[i].gen.name; i++) {
        t_thrd.relopt_cxt.relOpts[j] = &boolRelOpts[i].gen;
        t_thrd.relopt_cxt.relOpts[j]->type = RELOPT_TYPE_BOOL;
        t_thrd.relopt_cxt.relOpts[j]->namelen = strlen(t_thrd.relopt_cxt.relOpts[j]->name);
        j++;
    }

    for (i = 0; intRelOpts[i].gen.name; i++) {
        t_thrd.relopt_cxt.relOpts[j] = &intRelOpts[i].gen;
        t_thrd.relopt_cxt.relOpts[j]->type = RELOPT_TYPE_INT;
        t_thrd.relopt_cxt.relOpts[j]->namelen = strlen(t_thrd.relopt_cxt.relOpts[j]->name);
        j++;
    }

    for (i = 0; int64RelOpts[i].gen.name; i++) {
        t_thrd.relopt_cxt.relOpts[j] = &int64RelOpts[i].gen;
        t_thrd.relopt_cxt.relOpts[j]->type = RELOPT_TYPE_INT64;
        t_thrd.relopt_cxt.relOpts[j]->namelen = strlen(t_thrd.relopt_cxt.relOpts[j]->name);
        j++;
    }

    for (i = 0; realRelOpts[i].gen.name; i++) {
        t_thrd.relopt_cxt.relOpts[j] = &realRelOpts[i].gen;
        t_thrd.relopt_cxt.relOpts[j]->type = RELOPT_TYPE_REAL;
        t_thrd.relopt_cxt.relOpts[j]->namelen = strlen(t_thrd.relopt_cxt.relOpts[j]->name);
        j++;
    }

    for (i = 0; stringRelOpts[i].gen.name; i++) {
        t_thrd.relopt_cxt.relOpts[j] = &stringRelOpts[i].gen;
        t_thrd.relopt_cxt.relOpts[j]->type = RELOPT_TYPE_STRING;
        t_thrd.relopt_cxt.relOpts[j]->namelen = strlen(t_thrd.relopt_cxt.relOpts[j]->name);
        j++;
    }

    for (i = 0; i < t_thrd.relopt_cxt.num_custom_options; i++) {
        t_thrd.relopt_cxt.relOpts[j] = t_thrd.relopt_cxt.custom_options[i];
        j++;
    }

    /* add a list terminator */
    t_thrd.relopt_cxt.relOpts[j] = NULL;

    /* flag the work is complete */
    t_thrd.relopt_cxt.need_initialization = false;
}

/*
 * add_reloption_kind
 *		Create a new relopt_kind value, to be used in custom reloptions by
 *		user-defined AMs.
 */
relopt_kind add_reloption_kind(void)
{
    /* don't hand out the last bit so that the enum's behavior is portable */
    if (t_thrd.relopt_cxt.last_assigned_kind >= RELOPT_KIND_MAX)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("user-defined relation parameter types limit exceeded")));
    t_thrd.relopt_cxt.last_assigned_kind <<= 1;
    return (relopt_kind)t_thrd.relopt_cxt.last_assigned_kind;
}

/*
 * add_reloption
 *		Add an already-created custom reloption to the list, and recompute the
 *		main parser table.
 */
static void add_reloption(relopt_gen *newoption)
{
    if (t_thrd.relopt_cxt.num_custom_options >= t_thrd.relopt_cxt.max_custom_options) {
        MemoryContext oldcxt;

        oldcxt = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

        if (t_thrd.relopt_cxt.max_custom_options == 0) {
            t_thrd.relopt_cxt.max_custom_options = 8;
            t_thrd.relopt_cxt.custom_options =
                (relopt_gen **)palloc(t_thrd.relopt_cxt.max_custom_options * sizeof(relopt_gen *));
        } else {
            t_thrd.relopt_cxt.max_custom_options *= 2;
            t_thrd.relopt_cxt.custom_options =
                (relopt_gen **)repalloc(t_thrd.relopt_cxt.custom_options,
                                        t_thrd.relopt_cxt.max_custom_options * sizeof(relopt_gen *));
        }
        MemoryContextSwitchTo(oldcxt);
    }
    t_thrd.relopt_cxt.custom_options[t_thrd.relopt_cxt.num_custom_options++] = newoption;

    t_thrd.relopt_cxt.need_initialization = true;
}

/*
 * allocate_reloption
 *		Allocate a new reloption and initialize the type-agnostic fields
 *		(for types other than string)
 */
static relopt_gen *allocate_reloption(bits32 kinds, int type, const char *name, const char *desc)
{
    MemoryContext oldcxt;
    size_t size;
    relopt_gen *newoption = NULL;

    oldcxt = MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));

    switch (type) {
        case RELOPT_TYPE_BOOL:
            size = sizeof(relopt_bool);
            break;
        case RELOPT_TYPE_INT:
            size = sizeof(relopt_int);
            break;
        case RELOPT_TYPE_INT64:
            size = sizeof(relopt_int64);
            break;
        case RELOPT_TYPE_REAL:
            size = sizeof(relopt_real);
            break;
        case RELOPT_TYPE_STRING:
            size = sizeof(relopt_string);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported option type")));
            return NULL; /* keep compiler quiet */
    }

    newoption = (relopt_gen *)palloc(size);

    newoption->name = pstrdup(name);
    if (desc != NULL)
        newoption->desc = pstrdup(desc);
    else
        newoption->desc = NULL;
    newoption->kinds = kinds;
    newoption->namelen = strlen(name);
    newoption->type = (relopt_type)type;

    MemoryContextSwitchTo(oldcxt);

    return newoption;
}

/*
 * add_bool_reloption
 *		Add a new boolean reloption
 */
void add_bool_reloption(bits32 kinds, const char *name, const char *desc, bool default_val)
{
    relopt_bool *newoption = NULL;

    newoption = (relopt_bool *)allocate_reloption(kinds, RELOPT_TYPE_BOOL, name, desc);
    newoption->default_val = default_val;

    add_reloption((relopt_gen *)newoption);
}

/*
 * add_int_reloption
 *		Add a new integer reloption
 */
void add_int_reloption(bits32 kinds, const char *name, const char *desc, int default_val, int min_val, int max_val)
{
    relopt_int *newoption = NULL;

    newoption = (relopt_int *)allocate_reloption(kinds, RELOPT_TYPE_INT, name, desc);
    newoption->default_val = default_val;
    newoption->min = min_val;
    newoption->max = max_val;

    add_reloption((relopt_gen *)newoption);
}

/*
 * add_int64_reloption
 *		Add a new 64-bit integer reloption
 */
void add_int64_reloption(bits32 kinds, const char *name, const char *desc, int64 default_val, int64 min_val,
                         int64 max_val)
{
    relopt_int64 *newoption = NULL;
    newoption = (relopt_int64 *)allocate_reloption(kinds, RELOPT_TYPE_INT64, name, desc);
    newoption->default_val = default_val;
    newoption->min = min_val;
    newoption->max = max_val;
    add_reloption((relopt_gen *)newoption);
}

/*
 * add_real_reloption
 *		Add a new float reloption
 */
void add_real_reloption(bits32 kinds, const char *name, const char *desc, double default_val, double min_val,
                        double max_val)
{
    relopt_real *newoption = NULL;

    newoption = (relopt_real *)allocate_reloption(kinds, RELOPT_TYPE_REAL, name, desc);
    newoption->default_val = default_val;
    newoption->min = min_val;
    newoption->max = max_val;

    add_reloption((relopt_gen *)newoption);
}

/*
 * add_string_reloption
 *		Add a new string reloption
 *
 * "validator" is an optional function pointer that can be used to test the
 * validity of the values.	It must elog(ERROR) when the argument string is
 * not acceptable for the variable.  Note that the default value must pass
 * the validation.
 */
void add_string_reloption(bits32 kinds, const char *name, const char *desc, const char *default_val,
                          validate_string_relopt validator)
{
    relopt_string *newoption = NULL;

    /* make sure the validator/default combination is sane */
    if (validator)
        (validator)(default_val);

    newoption = (relopt_string *)allocate_reloption(kinds, RELOPT_TYPE_STRING, name, desc);
    newoption->validate_cb = validator;
    if (default_val != NULL) {
        newoption->default_val =
            MemoryContextStrdup(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), default_val);
        newoption->default_len = strlen(default_val);
        newoption->default_isnull = false;
    } else {
        newoption->default_val = "";
        newoption->default_len = 0;
        newoption->default_isnull = true;
    }

    add_reloption((relopt_gen *)newoption);
}

/*
 * Transform a relation options list (list of DefElem) into the text array
 * format that is kept in pg_class.reloptions, including only those options
 * that are in the passed namespace.  The output values do not include the
 * namespace.
 *
 * This is used for three cases: CREATE TABLE/INDEX, ALTER TABLE SET, and
 * ALTER TABLE RESET.  In the ALTER cases, oldOptions is the existing
 * reloptions value (possibly NULL), and we replace or remove entries
 * as needed.
 *
 * If ignoreOids is true, then we should ignore any occurrence of "oids"
 * in the list (it will be or has been handled by interpretOidsOption()).
 *
 * Note that this is not responsible for determining whether the options
 * are valid, but it does check that namespaces for all the options given are
 * listed in validnsps.  The NULL namespace is always valid and need not be
 * explicitly listed.  Passing a NULL pointer means that only the NULL
 * namespace is valid.
 *
 * Both oldOptions and the result are text arrays (or NULL for "default"),
 * but we declare them as Datums to avoid including array.h in reloptions.h.
 */
Datum transformRelOptions(Datum oldOptions, List *defList, const char *namspace, const char *const validnsps[],
                          bool ignoreOids, bool isReset)
{
    Datum result;
    ArrayBuildState *astate = NULL;
    ListCell *cell = NULL;

    /* no change if empty list */
    if (defList == NIL)
        return oldOptions;

    /* We build new array using accumArrayResult */
    astate = NULL;

    /* Copy any oldOptions that aren't to be replaced */
    if (PointerIsValid(DatumGetPointer(oldOptions))) {
        ArrayType *array = DatumGetArrayTypeP(oldOptions);
        Datum *oldoptions = NULL;
        int noldoptions;
        int i;

        Assert(ARR_ELEMTYPE(array) == TEXTOID);

        deconstruct_array(array, TEXTOID, -1, false, 'i', &oldoptions, NULL, &noldoptions);

        for (i = 0; i < noldoptions; i++) {
            text *oldoption = DatumGetTextP(oldoptions[i]);
            char *text_str = VARDATA(oldoption);
            int text_len = VARSIZE(oldoption) - VARHDRSZ;

            /* Search for a match in defList */
            foreach (cell, defList) {
                DefElem *def = (DefElem *)lfirst(cell);
                int kw_len;
                    /* ignore if not in the same namespace */
                    if (namspace == NULL) {
                        if (def->defnamespace != NULL)
                            continue;
                    } else if (def->defnamespace == NULL)
                        continue;
                    else if (pg_strcasecmp(def->defnamespace, namspace) != 0)
                        continue;
                kw_len = strlen(def->defname);
                if (text_len > kw_len && text_str[kw_len] == '=' && pg_strncasecmp(text_str, def->defname, kw_len) == 0)
                    break;
            }
            if (cell == NULL) {
                /* No match, so keep old option */
                astate = accumArrayResult(astate, oldoptions[i], false, TEXTOID, CurrentMemoryContext);
            }
        }

        /* Free the memory used by array. */
        if (DatumGetPointer(oldOptions) != DatumGetPointer(array)) {
            pfree(array);
        }
        pfree(oldoptions);
    }

    /*
     * If CREATE/SET, add new options to array; if RESET, just check that the
     * user didn't say RESET (option=val).  (Must do this because the grammar
     * doesn't enforce it.)
     */
    const char *storageType = NULL;
    bool toastStorageTypeSet = false;
    foreach (cell, defList) {
        DefElem *def = (DefElem *)lfirst(cell);

        if (isReset) {
            if (def->arg != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("RESET must not include values for parameters")));
        } else {
            text *t = NULL;
            const char *value = NULL;
            Size len;
            errno_t rc = EOK;

            /*
             * Error out if the namespace is not valid.  A NULL namespace is
             * always valid.
             */
            if (def->defnamespace != NULL) {
                bool valid = false;
                int i;

                if (validnsps) {
                    for (i = 0; validnsps[i]; i++) {
                        if (pg_strcasecmp(def->defnamespace, validnsps[i]) == 0) {
                            valid = true;
                            break;
                        }
                    }
                }

                if (!valid)
                    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                    errmsg("unrecognized parameter namespace \"%s\"", def->defnamespace)));
            }

            if (ignoreOids && pg_strcasecmp(def->defname, "oids") == 0)
                continue;

            /* ignore if not in the same namespace */
            if (namspace == NULL) {
                if (def->defnamespace != NULL)
                    continue;
            } else if (pg_strcasecmp(def->defname, "storage_type") == 0 && pg_strcasecmp(namspace, "toast") == 0) {
                /* save the storage type of parent table for toast table, may be used as its storage type */
                if (def->defnamespace == NULL) {
                    /* save parent storage type for toast */
                    storageType = ((def->arg != NULL) ? defGetString(def) : "true");
                    continue;
                } else if (pg_strcasecmp(def->defnamespace, namspace) != 0) {
                    continue;
                }
                toastStorageTypeSet = true; /* toast table set the storage type itself */
            } else if (def->defnamespace == NULL)
                continue;
            else if (pg_strcasecmp(def->defnamespace, namspace) != 0)
                continue;

            /*
             * Flatten the DefElem into a text string like "name=arg". If we
             * have just "name", assume "name=true" is meant.  Note: the
             * namespace is not output.
             */
            if (def->arg != NULL)
                value = defGetString(def);
            else
                value = "true";

            len = VARHDRSZ + strlen(def->defname) + 1 + strlen(value);
            /* +1 leaves room for sprintf's trailing null */
            t = (text *)palloc(len + 1);
            SET_VARSIZE(t, len);
            rc = sprintf_s(VARDATA(t), len + 1, "%s=%s", def->defname, value);
            securec_check_ss(rc, "\0", "\0");
            astate = accumArrayResult(astate, PointerGetDatum(t), false, TEXTOID, CurrentMemoryContext);
        }
    }

    /* we did not specify a storage type for toast, so use the same storage type as its parent */
    if (namspace != NULL && pg_strcasecmp(namspace, "toast") == 0 && !toastStorageTypeSet) {
        if (storageType != NULL) {
            Size len = VARHDRSZ + strlen("storage_type") + 1 + strlen(storageType);
            /* +1 leaves room for sprintf's trailing null */
            text *t = (text *)palloc(len + 1);
            SET_VARSIZE(t, len);
            errno_t rc = sprintf_s(VARDATA(t), len + 1, "%s=%s", "storage_type", storageType);
            securec_check_ss(rc, "\0", "\0");
            astate = accumArrayResult(astate, PointerGetDatum(t), false, TEXTOID, CurrentMemoryContext);
        }
    }

    if (astate != NULL)
        result = makeArrayResult(astate, CurrentMemoryContext);
    else
        result = (Datum)0;

    return result;
}

/*
 * Convert the text-array format of reloptions into a List of DefElem.
 * This is the inverse of transformRelOptions().
 */
List *untransformRelOptions(Datum options)
{
    List *result = NIL;
    ArrayType *array = NULL;
    Datum *optiondatums = NULL;
    int noptions;
    int i;

    /* Nothing to do if no options */
    if (!PointerIsValid(DatumGetPointer(options)))
        return result;

    array = DatumGetArrayTypeP(options);

    Assert(ARR_ELEMTYPE(array) == TEXTOID);

    deconstruct_array(array, TEXTOID, -1, false, 'i', &optiondatums, NULL, &noptions);

    for (i = 0; i < noptions; i++) {
        char *s = NULL;
        char *p = NULL;
        Node *val = NULL;

        s = TextDatumGetCString(optiondatums[i]);
        p = strchr(s, '=');
        if (p != NULL) {
            *p++ = '\0';
            val = (Node *)makeString(pstrdup(p));
        }
        result = lappend(result, makeDefElem(s, val));
    }

    /* Free the memory used by array. */
    if (DatumGetPointer(options) != DatumGetPointer(array)) {
        pfree(array);
    }
    pfree(optiondatums);

    return result;
}

/*
 * Extract and parse reloptions from a pg_class tuple.
 *
 * This is a low-level routine, expected to be used by relcache code and
 * callers that do not have a table's relcache entry (e.g. autovacuum).  For
 * other uses, consider grabbing the rd_options pointer from the relcache entry
 * instead.
 *
 * tupdesc is pg_class' tuple descriptor.  amoptions is the amoptions regproc
 * in the case of the tuple corresponding to an index, or InvalidOid otherwise.
 */
bytea *extractRelOptions(HeapTuple tuple, TupleDesc tupdesc, Oid amoptions)
{
    bytea *options = NULL;
    bool isnull = false;
    Datum datum;
    Form_pg_class classForm;

    datum = fastgetattr(tuple, Anum_pg_class_reloptions, tupdesc, &isnull);
    if (isnull)
        return NULL;

    classForm = (Form_pg_class)GETSTRUCT(tuple);

    /* Parse into appropriate format; don't error out here */
    switch (classForm->relkind) {
        case RELKIND_RELATION:
        case RELKIND_TOASTVALUE:
        case RELKIND_VIEW:
        case RELKIND_CONTQUERY:
        case RELKIND_MATVIEW:
            options = heap_reloptions(classForm->relkind, datum, false);
            break;
        case RELKIND_INDEX:
        case RELKIND_GLOBAL_INDEX:
            if (!RegProcedureIsValid(amoptions)) {
                tuple = SearchSysCache1(AMOID, classForm->relam);
                if (!HeapTupleIsValid(tuple))
                    ereport(ERROR,
                        (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                            errmsg("cache lookup failed for access method %u", classForm->relam)));
                amoptions = ((Form_pg_am)GETSTRUCT(tuple))->amoptions;
                ReleaseSysCache(tuple);
            }
            options = index_reloptions(amoptions, datum, false);
            break;
        case RELKIND_STREAM:
        case RELKIND_FOREIGN_TABLE:
            options = NULL;
            break;
        default:
            Assert(false);  /* can't get here */
            options = NULL; /* keep compiler quiet */
            break;
    }

    return options;
}

/*
 * Interpret reloptions that are given in text-array format.
 *
 * options is a reloption text array as constructed by transformRelOptions.
 * kind specifies the family of options to be processed.
 *
 * The return value is a relopt_value * array on which the options actually
 * set in the options array are marked with isset=true.  The length of this
 * array is returned in *numrelopts.  Options not set are also present in the
 * array; this is so that the caller can easily locate the default values.
 *
 * If there are no options of the given kind, numrelopts is set to 0 and NULL
 * is returned.
 *
 * Note: values of type int, bool and real are allocated as part of the
 * returned array.	Values of type string are allocated separately and must
 * be freed by the caller.
 */
relopt_value *parseRelOptions(Datum options, bool validate, relopt_kind kind, int *numrelopts)
{
    relopt_value *reloptions = NULL;
    int numoptions = 0;
    int i;
    int j;

    if (t_thrd.relopt_cxt.need_initialization)
        initialize_reloptions();

    /* Build a list of expected options, based on kind */
    for (i = 0; t_thrd.relopt_cxt.relOpts[i]; i++)
        if (t_thrd.relopt_cxt.relOpts[i]->kinds & kind)
            numoptions++;

    if (numoptions == 0) {
        *numrelopts = 0;
        return NULL;
    }

    reloptions = (relopt_value *)palloc(numoptions * sizeof(relopt_value));

    for (i = 0, j = 0; t_thrd.relopt_cxt.relOpts[i]; i++) {
        if (t_thrd.relopt_cxt.relOpts[i]->kinds & kind) {
            reloptions[j].gen = t_thrd.relopt_cxt.relOpts[i];
            reloptions[j].isset = false;
            j++;
        }
    }

    /* Done if no options */
    if (PointerIsValid(DatumGetPointer(options))) {
        ArrayType *array = NULL;
        Datum *optiondatums = NULL;
        int noptions;

        array = DatumGetArrayTypeP(options);
        AssertEreport(ARR_ELEMTYPE(array) == TEXTOID, MOD_MAX, "The option type should be text.");

        deconstruct_array(array, TEXTOID, -1, false, 'i', &optiondatums, NULL, &noptions);

        for (i = 0; i < noptions; i++) {
            text *optiontext = DatumGetTextP(optiondatums[i]);
            char *text_str = VARDATA(optiontext);
            int text_len = VARSIZE(optiontext) - VARHDRSZ;

            /* Search for a match in reloptions */
            for (j = 0; j < numoptions; j++) {
                int kw_len = reloptions[j].gen->namelen;

                if (text_len > kw_len && text_str[kw_len] == '=' &&
                    pg_strncasecmp(text_str, reloptions[j].gen->name, kw_len) == 0) {
                    parse_one_reloption(&reloptions[j], text_str, text_len, validate);
                    break;
                }
            }

            if (j >= numoptions && validate) {
                char *s = NULL;
                char *p = NULL;

                s = TextDatumGetCString(optiondatums[i]);
                p = strchr(s, '=');
                if (p != NULL)
                    *p = '\0';
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("unrecognized parameter \"%s\"", s)));
            }
        }

        if (DatumGetPointer(options) != DatumGetPointer(array)) {
            pfree(array);
        }
        pfree(optiondatums);
    }

    *numrelopts = numoptions;
    return reloptions;
}

/*
 * Subroutine for parseRelOptions, to parse and validate a single option's
 * value
 */
static void parse_one_reloption(relopt_value *option, const char *text_str, int text_len, bool validate)
{
    char *value = NULL;
    int value_len;
    bool parsed = false;
    bool nofree = false;
    errno_t rc = EOK;

    if (option->isset && validate)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("parameter \"%s\" specified more than once", option->gen->name)));

    value_len = text_len - option->gen->namelen - 1;
    value = (char *)palloc(value_len + 1);
    rc = memcpy_s(value, value_len + 1, text_str + option->gen->namelen + 1, value_len);
    securec_check(rc, "\0", "\0");
    value[value_len] = '\0';

    switch (option->gen->type) {
        case RELOPT_TYPE_BOOL: {
            parsed = parse_bool(value, &option->values.bool_val);
            if (validate && !parsed)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("invalid value for boolean option \"%s\": %s", option->gen->name, value)));
        } break;
        case RELOPT_TYPE_INT: {
            relopt_int *optint = (relopt_int *)option->gen;

            parsed = parse_int(value, &option->values.int_val, 0, NULL);
            if (validate && !parsed)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("invalid value for integer option \"%s\": %s", option->gen->name, value)));
            if (validate && (option->values.int_val < optint->min || option->values.int_val > optint->max))
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("value %s out of bounds for option \"%s\"", value, option->gen->name),
                                errdetail("Valid values are between \"%d\" and \"%d\".", optint->min, optint->max)));
        } break;
        case RELOPT_TYPE_INT64: {
            relopt_int64 *optint = (relopt_int64 *)option->gen;

            parsed = parse_int64(value, &option->values.int64_val, NULL);
            if (validate && !parsed)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("invalid value for 64-bit integer option \"%s\": %s", option->gen->name, value)));
            if (validate && (option->values.int64_val < optint->min || option->values.int64_val > optint->max))
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("value %s out of bounds for option \"%s\"", value, option->gen->name),
                                errdetail("Valid values are between \"" INT64_FORMAT "\" and \"" INT64_FORMAT "\".",
                                          optint->min, optint->max)));
        } break;
        case RELOPT_TYPE_REAL: {
            relopt_real *optreal = (relopt_real *)option->gen;

            parsed = parse_real(value, &option->values.real_val);
            if (validate && !parsed)
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("invalid value for floating point option \"%s\": %s", option->gen->name, value)));
            if (validate && (option->values.real_val < optreal->min || option->values.real_val > optreal->max))
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         (errmsg("value %s out of bounds for option \"%s\"", value, option->gen->name),
                          errdetail("Valid values are between \"%f\" and \"%f\".", optreal->min, optreal->max))));
        } break;
        case RELOPT_TYPE_STRING: {
            relopt_string *optstring = (relopt_string *)option->gen;

            option->values.string_val = value;
            nofree = true;
            if (validate && optstring->validate_cb)
                (optstring->validate_cb)(value);
            parsed = true;
        } break;
        default:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("unsupported reloption type %d", option->gen->type)));
            parsed = true; /* quiet compiler */
            break;
    }

    if (parsed)
        option->isset = true;
    if (!nofree)
        pfree(value);
}

bool CheckRelOptionValue(Datum options, const char *opt_name)
{
    int i;
    bool ret = false;

    if (options == (Datum)0)
        return false;

    /* Done if no options */
    if (PointerIsValid(DatumGetPointer(options))) {
        ArrayType *array = NULL;
        Datum *optiondatums = NULL;
        int noptions;

        array = DatumGetArrayTypeP(options);

        Assert(ARR_ELEMTYPE(array) == TEXTOID);

        deconstruct_array(array, TEXTOID, -1, false, 'i', &optiondatums, NULL, &noptions);

        for (i = 0; i < noptions; i++) {
            const char *s = TextDatumGetCString(optiondatums[i]);

            if (pg_strncasecmp(s, opt_name, strlen(opt_name)) == 0) {
                ret = true;
                break;
            }
        }
    }

    return ret;
}

/*
 * Given the result from parseRelOptions, allocate a struct that's of the
 * specified base size plus any extra space that's needed for string variables.
 *
 * "base" should be sizeof(struct) of the reloptions struct (StdRdOptions or
 * equivalent).
 */
void *allocateReloptStruct(Size base, relopt_value *options, int numoptions)
{
    Size size = base;
    int i;

    for (i = 0; i < numoptions; i++)
        if (options[i].gen->type == RELOPT_TYPE_STRING)
            size += GET_STRING_RELOPTION_LEN(options[i]) + 1;

    return palloc0(size);
}

/*
 * @Description: Given user options, find the first invalid option from
 *    invalidOptions[invalidOptionsNum]. firstInvalidOpt will remember
 *    position of the first one.
 * @Param[OUT] firstInvalidOpt: position of the first invalid option
 * @Param[IN] invalidOptions: invalid options
 * @Param[IN] invalidOptionsNum: number of invalid options
 * @Param[IN] userOptions: user options to check
 * @Return: whether invalid options exist
 * @See also:
 */
static bool FindInvalidOption(List *userOptions, const char *invalidOptions[], int invalidOptionsNum,
                              int *firstInvalidOpt)
{
    ListCell *opt = NULL;

    for (int i = 0; i < invalidOptionsNum; ++i) {
        foreach (opt, userOptions) {
            DefElem *def = (DefElem *)lfirst(opt);

            if (pg_strcasecmp(def->defname, invalidOptions[i]) == 0) {
                *firstInvalidOpt = i;
                return true;
            }
        }
    }

    *firstInvalidOpt = -1;
    return false;
}

/*
 * @Description: forbid out user to set un-supported options
 * @Param[IN] errorDetail: detail info for error report
 * @Param[IN] unsupported: unsupported options
 * @Param[IN] unsupportedNum: number of unsupported options
 * @Param[IN] userOptions: user options to be checked
 * @See also:
 */
void ForbidUserToSetUnsupportedOptions(List *userOptions, const char *unsupported[], int unsupportedNum,
                                       const char *errorDetail)
{
    if (userOptions != NIL) {
        int firstInvalidOpt = -1;

        if (FindInvalidOption(userOptions, unsupported, unsupportedNum, &firstInvalidOpt)) {
            Assert(firstInvalidOpt >= 0 && firstInvalidOpt < unsupportedNum);

            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Un-support feature"),
                     errdetail("Forbid to set option \"%s\" for %s", unsupported[firstInvalidOpt], errorDetail)));
        }
    }
}

/*
 * Given the result of parseRelOptions and a parsing table, fill in the
 * struct (previously allocated with allocateReloptStruct) with the parsed
 * values.
 *
 * rdopts is the pointer to the allocated struct to be filled.
 * basesize is the sizeof(struct) that was passed to allocateReloptStruct.
 * options, of length numoptions, is parseRelOptions' output.
 * elems, of length numelems, is the table describing the allowed options.
 * When validate is true, it is expected that all options appear in elems.
 */
void fillRelOptions(void *rdopts, Size basesize, relopt_value *options, int numoptions, bool validate,
                    const relopt_parse_elt *elems, int numelems)
{
    int i;
    int offset = basesize;
    errno_t rc = EOK;

    for (i = 0; i < numoptions; i++) {
        int j;
        bool found = false;

        for (j = 0; j < numelems; j++) {
            if (pg_strcasecmp(options[i].gen->name, elems[j].optname) == 0) {
                relopt_string *optstring = NULL;
                char *itempos = ((char *)rdopts) + elems[j].offset;
                char *string_val = NULL;

                switch (options[i].gen->type) {
                    case RELOPT_TYPE_BOOL:
                        *(bool *)itempos = options[i].isset ? options[i].values.bool_val
                                                            : ((relopt_bool *)options[i].gen)->default_val;
                        break;
                    case RELOPT_TYPE_INT:
                        *(int *)itempos = options[i].isset ? options[i].values.int_val
                                                            : ((relopt_int *)options[i].gen)->default_val;
                        break;
                    case RELOPT_TYPE_INT64:
                        *(int64 *)itempos = options[i].isset ? options[i].values.int64_val
                                                                : ((relopt_int64 *)options[i].gen)->default_val;
                        break;
                    case RELOPT_TYPE_REAL:
                        *(double *)itempos = options[i].isset ? options[i].values.real_val
                                                                : ((relopt_real *)options[i].gen)->default_val;
                        break;
                    case RELOPT_TYPE_STRING:
                        optstring = (relopt_string *)options[i].gen;
                        if (options[i].isset)
                            string_val = options[i].values.string_val;
                        else if (!optstring->default_isnull)
                            string_val = optstring->default_val;
                        else
                            string_val = NULL;

                        /*
                         * Important:
                         * for string type, data is appended at the tail of its parent struct.
                         * CHAR* member of this STRUCT stores the offet of its string data.
                         * offset=0 means that it's a NULL string.
                         */
                        if (string_val == NULL)
                            *(int *)itempos = 0;
                        else {
                            rc = strcpy_s((char *)rdopts + offset, strlen(string_val) + 1, string_val);
                            securec_check(rc, "\0", "\0");
                            *(int *)itempos = offset;
                            offset += strlen(string_val) + 1;
                        }
                        break;
                    default:
                        ereport(ERROR, (errcode(ERRCODE_MOST_SPECIFIC_TYPE_MISMATCH),
                                        errmsg("unrecognized reloption type %c", options[i].gen->type)));
                        break;
                }
                found = true;
                break;
            }
        }
        if (validate && !found)
            ereport(ERROR, (errcode(ERRCODE_CASE_NOT_FOUND),
                            errmsg("reloption \"%s\" not found in parse table", options[i].gen->name)));
    }
    SET_VARSIZE(rdopts, offset);
}

/*
 * @Description: fill options for TDE(Transparent Data Encryption) relations
 * @Param[IN] options: input user options.
 */
void fillTdeRelOptions(List *options, char relkind)
{
    ListCell *listptr1 = NULL;
    bool spec_encrypt = false;
    bool algo_flag = false;
    bool dek_flag = false;
    bool cmk_flag = false;
    DefElem *opt_dek = makeNode(DefElem);
    DefElem *opt_cmk = makeNode(DefElem);
    DefElem *opt_algo = makeNode(DefElem);

    foreach(listptr1, options) {
        DefElem *defs = reinterpret_cast<DefElem *>(lfirst(listptr1));
        if (pg_strcasecmp(defs->defname, "enable_tde") == 0) {
            if (t_thrd.proc->workingVersionNum < TDE_VERSION_NUM) {
                ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("create TDE table failed"),
                        errdetail("current version does not support TDE feature"),
                        errcause("TDE feature is not supported for current version"),
                        erraction("check database version about create TDE table")));
            }
            if (relkind == RELKIND_MATVIEW) {
                ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("create matview with TDE failed"),
                        errdetail("materialized views do not support TDE feature"),
                        errcause("TDE feature is not supported for Create materialized views"),
                        erraction("check CREATE syntax about create the materialized views")));
            }
            spec_encrypt = true;
            continue;
        }

        if (pg_strcasecmp(defs->defname, "encrypt_algo") == 0) {
            if (defs->arg == NULL) {
                ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("set relation option %s failed", defs->defname),
                    errdetail("%s requires a string parameter", defs->defname)));
            }
            algo_flag = true;
            continue;
        }

        if (pg_strcasecmp(defs->defname, "dek_cipher") == 0) {
            if (defs->arg == NULL) {
                ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("set relation option %s failed", defs->defname),
                    errdetail("%s requires a string parameter", defs->defname)));
            }
            dek_flag = true;
            continue;
        }

        if (pg_strcasecmp(defs->defname, "cmk_id") == 0) {
            if (defs->arg == NULL) {
                ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("set relation option %s failed", defs->defname),
                    errdetail("%s requires a string parameter", defs->defname)));
            }
            cmk_flag = true;
            continue;
        }
    }

    if (!spec_encrypt && (dek_flag || cmk_flag)) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("set relation option failed"),
            errdetail("enable_tde must be set when cmk_id or dek_cipher is set")));
        return;
    }

    if (dek_flag != cmk_flag) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("create TDE table failed"),
            errdetail("should not set cmk_id or dek_cipher only to enable TDE feature")));
        return;
    }

    if (!g_instance.attr.attr_security.enable_tde && spec_encrypt) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set relation option enable_tde failed"),
            errdetail("guc parameter enable_tde must be set to on to enable TDE feature")));
        return;
    }

    if (algo_flag && !spec_encrypt) {
        ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("set relation option encrypt_algo failed"), 
            errdetail("enable_tde option must be set when encrypt_algo is set")));
        return;
    }

    if (spec_encrypt) {
        if (!dek_flag && !cmk_flag) {
            const TDEData* tde_data = NULL;
            TDEKeyManager* tde_key_manager = New(CurrentMemoryContext) TDEKeyManager();
            tde_key_manager->init();
            tde_data = tde_key_manager->create_dek();
            char* dek_cipher = (char *)palloc0(strlen(tde_data->dek_cipher) + 1); /* should not free in this function */
            char* cmk_id = (char *)palloc0(strlen(tde_data->cmk_id) + 1); /* should not free in this function */
            errno_t rc = EOK;
            rc = strcpy_s(dek_cipher, strlen(tde_data->dek_cipher) + 1, tde_data->dek_cipher);
            securec_check(rc, "\0", "\0");
            rc = strcpy_s(cmk_id, strlen(tde_data->cmk_id) + 1, tde_data->cmk_id);
            securec_check(rc, "\0", "\0");

            opt_dek->type = T_DefElem;
            opt_dek->defnamespace = NULL;
            opt_dek->defname = "dek_cipher";
            opt_dek->defaction = DEFELEM_UNSPEC;
            opt_dek->arg = reinterpret_cast<Node *>(makeString(dek_cipher));
            options = lappend(options, opt_dek);

            opt_cmk->type = T_DefElem;
            opt_cmk->defnamespace = NULL;
            opt_cmk->defname = "cmk_id";
            opt_cmk->defaction = DEFELEM_UNSPEC;
            opt_cmk->arg = reinterpret_cast<Node *>(makeString(cmk_id));
            options = lappend(options, opt_cmk);
            if (IS_PGXC_DATANODE) {
                tde_key_manager->save_key(tde_data);
            }
            DELETE_EX2(tde_key_manager);
        }

        if (!algo_flag) {
            opt_algo->type = T_DefElem;
            opt_algo->defnamespace = NULL;
            opt_algo->defname = "encrypt_algo";
            opt_algo->defaction = DEFELEM_UNSPEC;
            opt_algo->arg = reinterpret_cast<Node *>(makeString("AES_128_CTR"));
            options = lappend(options, opt_algo);
        }
    } 
}

/*
 * @Description: check compression option for row relation
 * @Param[IN] options: input user options.
 * @See also:
 */
void RowTblCheckCompressionOption(List *options, int8 rowCompress)
{
    if (IsCompressedByCmprsInPgclass((RelCompressType) rowCompress)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                        errmsg("row-oriented table does not support compression")));
    }

    ListCell *opt = NULL;

    if (options == NULL) {
        return; /* nothing to do */
    }

    foreach (opt, options) {
        DefElem *def = (DefElem *)lfirst(opt);

        if (pg_strcasecmp(def->defname, "compression") == 0) {
            /* def->arg is NULL, that means it's a RESET action. ignore it.
             * def->arg is not NULL, that means it's a SET action, so check it.
             */
            if (def->arg) {
                const char *valstr = defGetString(def);

                if (pg_strcasecmp(valstr, "no") != 0) {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("row-oriented table does not support compression")));
                }
            }
            break;
        }
    }
}

void RowTblCheckHashBucketOption(List* options, StdRdOptions* std_opt)
{
    int  bucketcnt  = std_opt->bucketcnt;
    bool hashbucket = std_opt->hashbucket;
    bool segment    = std_opt->segment;
    ListCell *opt = NULL;

    if (options == NULL) {
        return; /* nothing to do */
    }

    bool check_hashbucket = (bucketcnt != 0 && hashbucket == false);
    bool check_segment = ((segment == false) && (bucketcnt != 0 || hashbucket == true));

    if (check_hashbucket || check_segment) {
        bool set_hashbucket = false;
        bool set_segment = false;
        foreach (opt, options) {
            DefElem *def = (DefElem *)lfirst(opt);

            if (pg_strcasecmp(def->defname, "hashbucket") == 0) {
                set_hashbucket = true;
            } else if (pg_strcasecmp(def->defname, "segment") == 0) {
                set_segment = true;
            }
        }

        if ((check_segment && set_segment) || (check_hashbucket && set_hashbucket)) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("Please Check the setting of [hashbucket] [segment] and [bucketcnt] options"),
                     errdetail("Their dependencies are as follow [bucketcnt =>] hashbucket[on] => segment[on]")));
        }
    }

    if (!hashbucket && bucketcnt != 0) {
        ereport(NOTICE,
            (errmsg("bucketcnt can only used for hashbucket table, set hashbucket to on by default")));
        hashbucket = true;
    }

    if (hashbucket && !segment) {
        ereport(NOTICE,
            (errmsg("hashbucket table need segment storage, set segment to on by default")));
        segment = true;
    }

    std_opt->hashbucket = hashbucket;
    std_opt->segment = segment;
}

/*
 * @Description: check compression option for row relation
 * @Param[IN] options: input user options.
 * @See also:
 */
static void tsstore_tbl_check_compression_option(List *options)
{
    ListCell *opt = NULL;

    if (options == NULL) {
        return; /* nothing to do */
    }

    foreach (opt, options) {
        DefElem *def = (DefElem *)lfirst(opt);

        if (pg_strcasecmp(def->defname, "compression") == 0) {
            /* def->arg is NULL, that means it's a RESET action. ignore it.
             * def->arg is not NULL, that means it's a SET action, so check it.
             */
            if (def->arg) {
                const char *valstr = defGetString(def);

                if (!(pg_strcasecmp(valstr, "yes") == 0 || pg_strcasecmp(valstr, "no") == 0)) {
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                             errmsg("Value \"%s\" of option \"compression\" is invalid for timeseries table", valstr),
                             errdetail("Valid values are \"yes\" and \"no\"")));
                }
            }
            break;
        }
    }
}

/*
 * @Description: check relation options for row table
 * @Param[IN] options: input user options
 * @See also:
 */
void ForbidToSetOptionsForRowTbl(List *options)
{
    /* row relation's unsupported options */
    static const char *unsupported[] = {
        "max_batchrow",
        "deltarow_threshold",
        "partial_cluster_rows",
        "compresslevel",
        "enable_tsdb_delta",
        "tsdb_deltamerge_interval",
        "tsdb_deltamerge_threshold",
        "tsdb_deltainsert_threshold"
    };

    /* check relation's options for row table */
    ForbidUserToSetUnsupportedOptions(options, unsupported, lengthof(unsupported), "row relation");

    /* row table has different COMPRESSION values */
    RowTblCheckCompressionOption(options);
}

/*
 * @Description: check relation options for column table
 * @Param[IN] options: input user options
 * @See also:
 */
void ForbidToSetOptionsForColTbl(List *options)
{
    static const char *unsupported[] = {
        "fillfactor",
        "autovacuum_vacuum_threshold",
        "autovacuum_vacuum_cost_delay",
        "autovacuum_vacuum_cost_limit",
        "autovacuum_freeze_min_age",
        "autovacuum_freeze_max_age",
        "autovacuum_freeze_table_age",
        "autovacuum_vacuum_scale_factor",
        "security_barrier",
        "enable_tsdb_delta",
        "tsdb_deltamerge_interval",
        "tsdb_deltamerge_threshold",
        "tsdb_deltainsert_threshold",
        "enable_tde",
        "encrypt_algo",
        "dek_cipher",
        "cmk_id",
        "hasuids"
    };

    ForbidUserToSetUnsupportedOptions(options, unsupported, lengthof(unsupported), "column relation");
}

/*
 * @Description: check relation options for non-tde table
 * @Param[IN] options: input user options
 */
void ForbidToSetTdeOptionsForNonTdeTbl(List *options)
{
    static const char *unsupported[] = {
        "enable_tde",
        "dek_cipher",
        "cmk_id",
        "encrypt_algo"
    };

    ForbidUserToSetUnsupportedOptions(options, unsupported, lengthof(unsupported), "Non-TDE relation");
}

/*
 * @Description: not allowed to alter "dek_cipher" and "cmk_id" by user directly.
 * @Param[IN] options: input user options
 */
void ForbidToAlterOptionsForTdeTbl(List *options)
{
    static const char *unsupported[] = {
        "dek_cipher",
        "cmk_id"
    };
    ForbidUserToSetUnsupportedOptions(options, unsupported, lengthof(unsupported), "tde relation");
}

/*
 * @Description: check relation options for ustore table
 * @Param[IN] options: input user options
 */
void ForbidToSetOptionsForUstoreTbl(List *options)
{
    static const char *unsupported[] = {
        "enable_tde",
        "dek_cipher",
        "cmk_id",
        "encrypt_algo",
        "hasuids"
    };

    ForbidUserToSetUnsupportedOptions(options, unsupported, lengthof(unsupported), "ustore relation");
}

/*
 * @Description: check relation options for timeseries table
 * @Param[IN] options: input user options
 * @See also:
 */
void forbid_to_set_options_for_timeseries_tbl(List *options)
{
    static const char *unsupported[] = {
        "fillfactor",
        "autovacuum_vacuum_threshold",
        "autovacuum_vacuum_cost_delay",
        "autovacuum_vacuum_cost_limit",
        "autovacuum_freeze_min_age",
        "autovacuum_freeze_max_age",
        "autovacuum_freeze_table_age",
        "autovacuum_vacuum_scale_factor",
        "security_barrier",
        "max_batchrow",
        "deltarow_threshold",
        "partial_cluster_rows",
        "compresslevel",
        "hasuids"
    };

    ForbidUserToSetUnsupportedOptions(options, unsupported, lengthof(unsupported), "timeseries relation");

    /* tsstore table has different COMPRESSION values */
    tsstore_tbl_check_compression_option(options);
}

/*
 * @Description: check relation options for PSort table
 * @Param[IN] options: input user options
 * @See also:
 */
void ForbidToSetOptionsForPSort(List *options)
{
    static const char *unsupported[] = {
        "fillfactor",
        "autovacuum_enabled",
        "autovacuum_vacuum_threshold",
        "autovacuum_analyze_threshold",
        "autovacuum_vacuum_cost_delay",
        "autovacuum_vacuum_cost_limit",
        "autovacuum_freeze_min_age",
        "autovacuum_freeze_max_age",
        "autovacuum_freeze_table_age",
        "autovacuum_vacuum_scale_factor",
        "autovacuum_analyze_scale_factor",
        "security_barrier",
        "compression",
        "enable_tsdb_delta",
        "tsdb_deltamerge_interval",
        "tsdb_deltamerge_threshold",
        "tsdb_deltainsert_threshold",
        "hasuids"
    };

    ForbidUserToSetUnsupportedOptions(options, unsupported, lengthof(unsupported), "psort index");
}

/*
 * Option parser for anything that uses StdRdOptions (i.e. fillfactor and
 * autovacuum)
 */
bytea *default_reloptions(Datum reloptions, bool validate, relopt_kind kind)
{
    relopt_value *options = NULL;
    StdRdOptions *rdopts = NULL;
    int numoptions;
    static const relopt_parse_elt tab[] = {
        { "fillfactor", RELOPT_TYPE_INT, offsetof(StdRdOptions, fillfactor) },
        { "autovacuum_enabled", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, enabled) },
        { "autovacuum_vacuum_threshold", RELOPT_TYPE_INT,
          offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_threshold) },
        { "autovacuum_analyze_threshold", RELOPT_TYPE_INT,
          offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, analyze_threshold) },
        { "autovacuum_vacuum_cost_delay", RELOPT_TYPE_INT,
          offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_cost_delay) },
        { "autovacuum_vacuum_cost_limit", RELOPT_TYPE_INT,
          offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_cost_limit) },
        { "autovacuum_freeze_min_age", RELOPT_TYPE_INT64,
          offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, freeze_min_age) },
        { "autovacuum_freeze_max_age", RELOPT_TYPE_INT64,
          offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, freeze_max_age) },
        { "autovacuum_freeze_table_age", RELOPT_TYPE_INT64,
          offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, freeze_table_age) },
        { "autovacuum_vacuum_scale_factor", RELOPT_TYPE_REAL,
          offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, vacuum_scale_factor) },
        { "autovacuum_analyze_scale_factor", RELOPT_TYPE_REAL,
          offsetof(StdRdOptions, autovacuum) + offsetof(AutoVacOpts, analyze_scale_factor) },
        { "security_barrier", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, security_barrier) },
        { "enable_rowsecurity", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, enable_rowsecurity) },
        { "force_rowsecurity", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, force_rowsecurity) },
        { "enable_tsdb_delta", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, enable_tsdb_delta) },
        { "tsdb_deltamerge_interval", RELOPT_TYPE_INT, offsetof(StdRdOptions, tsdb_deltamerge_interval) },
        { "tsdb_deltamerge_threshold", RELOPT_TYPE_INT, offsetof(StdRdOptions, tsdb_deltamerge_threshold) },
        { "tsdb_deltainsert_threshold", RELOPT_TYPE_INT, offsetof(StdRdOptions, tsdb_deltainsert_threshold) },
        { "max_batchrow", RELOPT_TYPE_INT, offsetof(StdRdOptions, max_batch_rows) },
        { "deltarow_threshold", RELOPT_TYPE_INT, offsetof(StdRdOptions, delta_rows_threshold) },
        { "partial_cluster_rows", RELOPT_TYPE_INT, offsetof(StdRdOptions, partial_cluster_rows) },
        { "internal_mask", RELOPT_TYPE_INT, offsetof(StdRdOptions, internalMask) },
        { "orientation", RELOPT_TYPE_STRING, offsetof(StdRdOptions, orientation) },
        { "indexsplit", RELOPT_TYPE_STRING, offsetof(StdRdOptions, indexsplit) },
        { "compression", RELOPT_TYPE_STRING, offsetof(StdRdOptions, compression) },
        { "storage_type", RELOPT_TYPE_STRING, offsetof(StdRdOptions, storage_type) },
        { "ttl", RELOPT_TYPE_STRING, offsetof(StdRdOptions, ttl) },
        { "period", RELOPT_TYPE_STRING, offsetof(StdRdOptions, period) },
        { "string_optimize", RELOPT_TYPE_STRING, offsetof(StdRdOptions, string_optimize) },
        { "partition_interval", RELOPT_TYPE_STRING, offsetof(StdRdOptions, partition_interval) },
        { "time_column", RELOPT_TYPE_STRING, offsetof(StdRdOptions, time_column) },
        { "ttl_interval", RELOPT_TYPE_STRING, offsetof(StdRdOptions, ttl_interval) },
        { "gather_interval", RELOPT_TYPE_STRING, offsetof(StdRdOptions, gather_interval) },
        { "sw_interval", RELOPT_TYPE_STRING, offsetof(StdRdOptions, sw_interval) },
        { "version", RELOPT_TYPE_STRING, offsetof(StdRdOptions, version) },
        { "compresslevel", RELOPT_TYPE_INT, offsetof(StdRdOptions, compresslevel) },
        { "ignore_enable_hadoop_env", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, ignore_enable_hadoop_env) },
        { "append_mode", RELOPT_TYPE_STRING, offsetof(StdRdOptions, append_mode) },
        { "merge_list", RELOPT_TYPE_STRING, offsetof(StdRdOptions, merge_list) },
        { "rel_cn_oid", RELOPT_TYPE_INT, offsetof(StdRdOptions, rel_cn_oid) },
        { "exec_step", RELOPT_TYPE_INT, offsetof(StdRdOptions, exec_step) },
        { "create_time", RELOPT_TYPE_INT64, offsetof(StdRdOptions, create_time) },
        { "init_td", RELOPT_TYPE_INT, offsetof(StdRdOptions, initTd) },
        { "append_mode_internal", RELOPT_TYPE_INT, offsetof(StdRdOptions, append_mode_internal) },
        { "start_ctid_internal", RELOPT_TYPE_STRING, offsetof(StdRdOptions, start_ctid_internal) },
        { "end_ctid_internal", RELOPT_TYPE_STRING, offsetof(StdRdOptions, end_ctid_internal) },
        { "user_catalog_table", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, user_catalog_table) },
        { "hashbucket", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, hashbucket) },
        { "segment", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, segment) }, 
        { "primarynode", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, primarynode) },
        { "on_commit_delete_rows", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, on_commit_delete_rows)},
        { "wait_clean_gpi", RELOPT_TYPE_STRING, offsetof(StdRdOptions, wait_clean_gpi)},
        { "bucketcnt", RELOPT_TYPE_INT, offsetof(StdRdOptions, bucketcnt)},
        { "parallel_workers", RELOPT_TYPE_INT, offsetof(StdRdOptions, parallel_workers)},
        { "crossbucket", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, crossbucket)},
        { "wait_clean_cbi", RELOPT_TYPE_STRING, offsetof(StdRdOptions, wait_clean_cbi)},
        { "dek_cipher", RELOPT_TYPE_STRING, offsetof(StdRdOptions, dek_cipher)},
        { "cmk_id", RELOPT_TYPE_STRING, offsetof(StdRdOptions, cmk_id)},
        { "encrypt_algo", RELOPT_TYPE_STRING, offsetof(StdRdOptions, encrypt_algo)},
        { "enable_tde", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, enable_tde)},
        { "hasuids", RELOPT_TYPE_BOOL, offsetof(StdRdOptions, hasuids) },
        { "compresstype", RELOPT_TYPE_INT, 
          offsetof(StdRdOptions, compress) + offsetof(PageCompressOpts, compressType)},
        { "compress_level", RELOPT_TYPE_INT,
          offsetof(StdRdOptions, compress) + offsetof(PageCompressOpts, compressLevel)},
        { "compress_chunk_size", RELOPT_TYPE_INT,
          offsetof(StdRdOptions, compress) + offsetof(PageCompressOpts, compressChunkSize)},
        {"compress_prealloc_chunks", RELOPT_TYPE_INT,
          offsetof(StdRdOptions, compress) + offsetof(PageCompressOpts, compressPreallocChunks)},
        { "compress_byte_convert", RELOPT_TYPE_BOOL,
          offsetof(StdRdOptions, compress) + offsetof(PageCompressOpts, compressByteConvert)},
        { "compress_diff_convert", RELOPT_TYPE_BOOL,
          offsetof(StdRdOptions, compress) + offsetof(PageCompressOpts, compressDiffConvert)},

    };

    options = parseRelOptions(reloptions, validate, kind, &numoptions);

    /* if none set, we're done */
    if (numoptions == 0)
        return NULL;

    rdopts = (StdRdOptions *)allocateReloptStruct(sizeof(StdRdOptions), options, numoptions);

    fillRelOptions((void *)rdopts, sizeof(StdRdOptions), options, numoptions, validate, tab, lengthof(tab));

    for (int i = 0; i < numoptions; i++) {
        if (options[i].gen->type == RELOPT_TYPE_STRING && options[i].isset)
            pfree(options[i].values.string_val);
    }
    pfree(options);

    return (bytea *)rdopts;
}

/*
 * Parse options for heaps, views and toast tables.
 */
bytea *heap_reloptions(char relkind, Datum reloptions, bool validate)
{
    StdRdOptions *rdopts = NULL;

    switch (relkind) {
        case RELKIND_TOASTVALUE:
            rdopts = (StdRdOptions *)default_reloptions(reloptions, validate, RELOPT_KIND_TOAST);
            if (rdopts != NULL) {
                /* adjust default-only parameters for TOAST relations */
                rdopts->fillfactor = 100;
                rdopts->autovacuum.analyze_threshold = -1;
                rdopts->autovacuum.analyze_scale_factor = -1;
            }
            return (bytea *)rdopts;
        case RELKIND_RELATION:
        case RELKIND_MATVIEW:
            return default_reloptions(reloptions, validate, RELOPT_KIND_HEAP);
        case RELKIND_VIEW:
        case RELKIND_CONTQUERY:
            return default_reloptions(reloptions, validate, RELOPT_KIND_VIEW);
        default:
            /* other relkinds are not supported */
            return NULL;
    }
}

/*
 * Parse options for indexes.
 *
 *	amoptions	Oid of option parser
 *	reloptions	options as text[] datum
 *	validate	error flag
 */
bytea *index_reloptions(RegProcedure amoptions, Datum reloptions, bool validate)
{
    FmgrInfo flinfo;
    FunctionCallInfoData fcinfo;
    Datum result;

    Assert(RegProcedureIsValid(amoptions));

    /* Assume function is strict */
    if (!PointerIsValid(DatumGetPointer(reloptions)))
        return NULL;

    /* Can't use OidFunctionCallN because we might get a NULL result */
    fmgr_info(amoptions, &flinfo);

    InitFunctionCallInfoData(fcinfo, &flinfo, 2, InvalidOid, NULL, NULL);

    fcinfo.arg[0] = reloptions;
    fcinfo.arg[1] = BoolGetDatum(validate);
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;
    result = FunctionCallInvoke(&fcinfo);
    if (fcinfo.isnull || DatumGetPointer(result) == NULL)
        return NULL;

    return DatumGetByteaP(result);
}

/*
 * @Description: Unsupported Option for attribute reloptions
 * @Param[IN] options: input user options
 * @See also:
 */
void ForbidToSetOptionsForAttribute(List *options)
{
    static const char *unsupportedOptions[] = {"n_distinct_inherited"};

    /* report warning if option 'n_distinct_inherited' exists */
    ForbidUserToSetUnsupportedOptions(options, unsupportedOptions, lengthof(unsupportedOptions),
                                      "both row and column relation");
}

/*
 * Option parser for attribute reloptions
 */
bytea *attribute_reloptions(Datum reloptions, bool validate)
{
    relopt_value *options = NULL;
    AttributeOpts *aopts = NULL;
    int numoptions;
    static const relopt_parse_elt tab[] = {
        { "n_distinct", RELOPT_TYPE_REAL, offsetof(AttributeOpts, n_distinct) },
        { "n_distinct_inherited", RELOPT_TYPE_REAL, offsetof(AttributeOpts, n_distinct_inherited) }
    };

    options = parseRelOptions(reloptions, validate, RELOPT_KIND_ATTRIBUTE, &numoptions);

    /* if none set, we're done */
    if (numoptions == 0)
        return NULL;

    aopts = (AttributeOpts *)allocateReloptStruct(sizeof(AttributeOpts), options, numoptions);

    fillRelOptions((void *)aopts, sizeof(AttributeOpts), options, numoptions, validate, tab, lengthof(tab));

    pfree(options);

    return (bytea *)aopts;
}

/*
 * Option parser for tablespace reloptions
 */
bytea *tablespace_reloptions(Datum reloptions, bool validate)
{
    relopt_value *options = NULL;
    TableSpaceOpts *tsopts = NULL;
    int numoptions;
    static const relopt_parse_elt tab[] = {
        { "random_page_cost", RELOPT_TYPE_REAL, offsetof(TableSpaceOpts, random_page_cost) },
        { "seq_page_cost", RELOPT_TYPE_REAL, offsetof(TableSpaceOpts, seq_page_cost) },
        { "filesystem", RELOPT_TYPE_STRING, offsetof(TableSpaceOpts, filesystem) },
        { "address", RELOPT_TYPE_STRING, offsetof(TableSpaceOpts, address) },
        { "cfgpath", RELOPT_TYPE_STRING, offsetof(TableSpaceOpts, cfgpath) },
        { "storepath", RELOPT_TYPE_STRING, offsetof(TableSpaceOpts, storepath) }
    };

    options = parseRelOptions(reloptions, validate, RELOPT_KIND_TABLESPACE, &numoptions);

    /* if none set, we're done */
    if (numoptions == 0)
        return NULL;

    tsopts = (TableSpaceOpts *)allocateReloptStruct(sizeof(TableSpaceOpts), options, numoptions);

    fillRelOptions((void *)tsopts, sizeof(TableSpaceOpts), options, numoptions, validate, tab, lengthof(tab));

    pfree(options);

    return (bytea *)tsopts;
}

/*
 * Brief        : Check the orientation option Validity.
 * Input        : val, the version option value.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptOrientation(const char *val)
{
    if (0 != pg_strncasecmp(val, ORIENTATION_COLUMN, strlen(val)) &&
        0 != pg_strncasecmp(val, ORIENTATION_ROW, strlen(val)) &&
        0 != pg_strncasecmp(val, ORIENTATION_TIMESERIES, strlen(val)) &&
        0 != pg_strncasecmp(val, ORIENTATION_ORC, strlen(val))) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid string for  \"ORIENTATION\" option"),
                        errdetail("Valid string are \"column\", \"row\", \"timeseries\", \"orc\".")));
    }
}

/*
 * Brief        : Check the orientation option Validity.
 * Input        : val, the version option value.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptIndexsplit(const char *val)
{
    if (pg_strncasecmp(val, INDEXSPLIT_OPT_DEFAULT, strlen(val)) != 0 &&
        pg_strncasecmp(val, INDEXSPLIT_OPT_INSERTPT, strlen(val)) != 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid string for  \"INDEXSPLIT\" option"),
            errdetail("Valid string are \"default\", \"insertpt\".")));
    }
}

/*
 * Brief        : Check the TTL option Validity.
 * Input        : val, the version option value.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptTTL(const char *val)
{
    int32 typmod = -1;
    Interval *result = char_to_interval((char *)val, typmod);
    int64 usec = 0;
    if (result == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid interval string for  \"ttl\" option"),
                        errdetail("Valid interval string are like \"1 day\", \"1 week\", \"2 week\".")));
    }
    usec = INTERVAL_TO_USEC(result);
    if (result->month < 0 || result->day < 0 || result->month > MONTHS_PER_YEAR * 100 ||
        usec > 100 * DAYS_PER_NYEAR * USECS_PER_DAY || usec < USECS_PER_HOUR ||
        (result->month >= MONTHS_PER_YEAR * 100 && (result->day > 0 || result->time > 0))) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid interval range for  \"ttl\" option"),
                        errdetail("Valid interval range from \"1 hour\" to \"100 year\".")));
    }
}

/*
 * Brief        : Check the Peroid option Validity.
 * Input        : val, the version option value.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptPeriod(const char *val)
{
    int32 typmod = -1;
    Interval *result = char_to_interval((char *)val, typmod);
    int64 usec = 0;
    if (result == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid interval string for  \"period\" option"),
                 errdetail("Valid period string are like \"1 day\", \"1 week\", \"2 week\".")));
    }
    usec = INTERVAL_TO_USEC(result);
    if (result->month < 0 || result->day < 0 || usec < USECS_PER_HOUR || usec > 1 * DAYS_PER_NYEAR * USECS_PER_DAY ||
        (result->month == MONTHS_PER_YEAR && (result->day > 0 || result->time > 0))) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid interval range for  \"period\" option"),
                 errdetail("Valid interval range from \"1 hour\" to \"1 year\".")));
    }
}

/*
 * Brief        : Check the partition_interval Validity.
 * Input        : val, the partition_interval option value.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptPartitionInterval(const char *val)
{
    int32 typmod = -1;
    Interval *result = char_to_interval((char *)val, typmod);
    int64 usec = 0;
    if (result == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
                 errmsg("Invalid interval string for  \"partition_interval\" option"),
                 errdetail("Valid period string are like \"30 minute\", \"1 hour\", \"2 day\".")));
    }
    usec = INTERVAL_TO_USEC(result);
    if (result->month < 0 || result->day < 0 || usec < 30 * USECS_PER_MINUTE 
        || usec > 1 * DAYS_PER_NYEAR * USECS_PER_DAY ||
        (result->month == MONTHS_PER_YEAR && (result->day > 0 || result->time > 0))) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
                 errmsg("Invalid interval range for  \"partition_interval\" option"),
                 errdetail("Valid interval range from \"30 minute\" to \"1 year\".")));
    }
}

/*
 * Brief        : Check the time_column Validity.
 * Input        : val, the time_column option value.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptTimeColumn(const char *val)
{
    if (val == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
                 errmsg("Invalid interval string for  \"time_column\" option"),
                 errdetail("Valid time_column string in contview.")));
    }
}

/*
 * Brief        : Check the ttl_interval Validity.
 * Input        : val, the ttl_interval option value.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptTTLInterval(const char *val)
{
    if (val == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
                 errmsg("Invalid interval string for  \"ttl_interval\" option"),
                 errdetail("Valid ttl_interval string in contview.")));
    }
}

/*
 * Brief        : Check the gather_interval Validity.
 * Input        : val, the gather_interval option value.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptGatherInterval(const char *val)
{
    if (val == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
                 errmsg("Invalid interval string for  \"gather_interval\" option"),
                 errdetail("Valid gather_interval string in contview.")));
    }
}

/*
 * Brief        : Check the sw_interval Validity.
 * Input        : val, the sw_interval option value.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptSwInterval(const char *val)
{
    if (val == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Invalid interval string for  \"sw_interval\" option"),
                 errdetail("Valid sw_interval string in contquery.")));
    }
}

/*
 * Brief        : Check the version option Validity.
 * Input        : val, the version option value.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptVersion(const char *val)
{
    if (0 != pg_strcasecmp(val, ORC_VERSION_011) && 0 != pg_strcasecmp(val, ORC_VERSION_012)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid string for  \"VERSION\" option"),
                        errdetail("Valid string are \"0.11\", \"0.12\".")));
    }
}
/*
 * check parameter of append_mode . Allows "on", "off"
 * and "auto" values.
 */
void check_append_mode(const char *value)
{
    if (value == NULL || (strcmp(value, "on") != 0 && strcmp(value, "off") != 0 && strcmp(value, "refresh") != 0 &&
                          strcmp(value, "read_only") != 0 && strcmp(value, "end_catchup") != 0)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid value for \"append_mode\" option"),
                 errdetail("Valid values are \"on\", \"off\", \"refresh\", \"read_only\" and \"end_catchup\".")));
    }
}

/*
 * check parameter of wait_clean_gpi . Allows "y", "n"
 * and "auto" values.
 */
void CheckWaitCleanGpi(const char* value)
{
    if (value == NULL || (strcmp(value, "y") != 0 && strcmp(value, "n") != 0)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid value for \"wait_clean_gpi\" option"),
                errdetail("Valid values are \"y\", \"n\".")));
    }
}

/*
 * check parameter of wait_clean_cbi . Allows "y", "n"
 * and "auto" values.
 */
void CheckWaitCleanCbi(const char* value)
{
    if (value == NULL || (strcmp(value, "y") != 0 && strcmp(value, "n") != 0)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid value for \"wait_clean_cbi\" option"),
                errdetail("Valid values are \"y\", \"n\".")));
    }
}


/*
 * Brief        : Check the compression mode for tablespace.
 * Input        : val, the compression algorithm value.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptCompression(const char *val)
{
    if (pg_strcasecmp(val, COMPRESSION_NO) != 0 && pg_strcasecmp(val, COMPRESSION_YES) != 0 &&
        pg_strcasecmp(val, COMPRESSION_LOW) != 0 && pg_strcasecmp(val, COMPRESSION_MIDDLE) != 0 &&
        pg_strcasecmp(val, COMPRESSION_HIGH) != 0 && pg_strcasecmp(val, COMPRESSION_ZLIB) != 0 &&
        pg_strcasecmp(val, COMPRESSION_SNAPPY) != 0 && pg_strcasecmp(val, COMPRESSION_LZ4) != 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid string for  \"COMPRESSION\" option."),
                 errdetail("Valid string are \"no\", \"yes\", \"low\", \"middle\", \"high\" for non-dfs table. "
                           "Valid string are \"no\", \"yes\", \"low\", \"middle\", \"high\", \"snappy\", \"zlib\", "
                           "\"lz4\" for dfs table.")));
}

/*
 * Brief        : Validates the Table Access Method reloption for a table type
 * Input        : val, Table Access Method value.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptTableAccessMethod(const char* val)
{
    if (pg_strcasecmp(val, TABLE_ACCESS_METHOD_ASTORE) != 0 && pg_strcasecmp(val, TABLE_ACCESS_METHOD_USTORE) != 0)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid string for \"TABLE_ACCESS_METHOD\" option."),
                errdetail("Valid strings are \"ASTORE\", \"USTORE\"")));
}

/*
 * Brief        : Check the filesystem option for tablespace.
 * Input        : val, the filesystem option value.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptSpcFileSystem(const char *val)
{
    if ((0 != pg_strcasecmp(val, FILESYSTEM_GENERAL)) && 0 != pg_strcasecmp(val, FILESYSTEM_HDFS)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid string for  \"filesystem\" option."),
                        errdetail("Valid string are \"general\", \"hdfs\".")));
    }
}

/*
 * Brief        : Check the address option for tablespace.
 * Input        : val, the address option value.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptSpcAddress(const char *val)
{
    CheckGetServerIpAndPort(val, NULL, true, -1);
}

/*
 * Brief        : Check the cfgpath option for tablespace.
 * Input        : val, the cfgpath option value.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptSpcCfgPath(const char *val)
{
    if (0 == strlen(val)) {
        ereport(ERROR,
                (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmsg("No cfgpath is specified for a DFS server.")));
    }

    CheckFoldernameOrFilenamesOrCfgPtah(val, "cfgpath");
}

/*
 * Brief        : Check the storepath option for tablespace.
 * Input        : val, the storepath option.
 * Output       : None.
 * Return Value : None.
 * Notes        : None.
 */
static void ValidateStrOptSpcStorePath(const char *val)
{
    if (0 == strlen(val)) {
        ereport(ERROR,
                (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmsg("No storepath is specified for a DFS server.")));
    }

    CheckFoldernameOrFilenamesOrCfgPtah(val, "storepath");
}

/*
 *  * Brief        : Check the string optimize option Validity.
 *  * Input        : val, the version option value.
 *  * Output       : None.
 *  * Return Value : None.
 *  * Notes        : None.
 *  */
static void ValidateStrOptStringOptimize(const char *val)
{
    if (val == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid interval string for  \"string_optimize\" option"),
            errdetail("Valid string_optimize string in contview.")));
    }
}

/*
 * Brief        : Check the encrypt_algo option Validity.
 * Input        : val, the encrypt_algo option value for tde.
 */
static void ValidateStrOptEncryptAlgo(const char *val)
{
    if (pg_strcasecmp(val, "AES_128_CTR") != 0 && pg_strcasecmp(val, "SM4_CTR") != 0) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid string for \"encrypt_algo\" option"),
                        errdetail("Valid string are \"AES_128_CTR\", \"SM4_CTR\".")));
    }
}

/*
 * Brief        : Check the dek_cipher option Validity.
 * Input        : val, the dek_cipher option value for tde.
 */
static void ValidateStrOptDekCipher(const char *val)
{
    /* The max length of dek_cipher is 312 for AES256, and 280 for AES128 and SE4 */
    const int dek_cipher_len_min = 280;
    const int dek_cipher_len_max = 312;
    if (strlen(val) < dek_cipher_len_min || strlen(val) > dek_cipher_len_max) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid string for \"dek_cipher\" option"),
                        errdetail("Valid string should be taken from kms.")));
    }
}

/*
 * Brief        : Check the cmk_id option Validity.
 * Input        : val, the cmk_id option value for tde.
 */
static void ValidateStrOptCmkId(const char *val)
{
    const int cmk_id_len = 36; /* The length of cmk_id is 36 */
    if (strlen(val) != cmk_id_len) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid string for \"cmk_id\" option"),
                        errdetail("Valid string should be taken from kms and set to the tde_cmk_id guc parameter.")));
    }
}

/*
 * @Description: get heap relation's compression option value
 * @IN compressOpt: compression option string
 * @Return: *OptCompress* compression option value
 * @See also:
 */
static OptCompress heap_get_compression(const char *compressOpt)
{
    /* COMPRESSION is 'no' */
    if (pg_strcasecmp(compressOpt, COMPRESSION_NO) == 0)
        return COMPRESS_NO;

    /* COMPRESSION is either 'yes' or 'middle' */
    if (pg_strcasecmp(compressOpt, COMPRESSION_YES) == 0 || pg_strcasecmp(compressOpt, COMPRESSION_MIDDLE) == 0)
        return COMPRESS_MIDDLE;

    /* COMPRESSION is 'low' */
    if (pg_strcasecmp(compressOpt, COMPRESSION_LOW) == 0)
        return COMPRESS_LOW;

    /* COMPRESSION is 'high' */
    return COMPRESS_HIGH;
}

/* we will combine COMPRESSION and COMPRESSLEVEL into an int16 value,
 * which is called compressing-modes. each one is int8 type, so that
 * 1. compressing-modes = int8 values array whose size is 2.
 * 2. COMPRESSION   = int8 values[0]
 * 3. COMPRESSLEVEL = int8 values[1]
 */
const int IDX_COMPRESSION_IN_MODES = 0;
const int IDX_COMPRESSLEVEL_IN_MODES = 1;

/*
 * @Description: combine COMPRESSION and COMPRESSLEVEL into compressing-modes
 * @IN rel: columnar heap relation
 * @OUT modes: compressing-modes value
 * @Return:
 * @See also:
 */
void heaprel_set_compressing_modes(Relation rel, int16 *modes_ptr)
{
    int8 *opt = (int8 *)modes_ptr;
    opt[IDX_COMPRESSION_IN_MODES] = (int8)heap_get_compression(RelationGetCompression(rel));
    opt[IDX_COMPRESSLEVEL_IN_MODES] = (int8)relation_get_compresslevel(rel);
}

/*
 * @Description: get COMPRESSION value
 * @IN modes: compressing-modes value
 * @Return: COMPRESSION value
 * @See also:
 */
int8 heaprel_get_compression_from_modes(int16 modes)
{
    int8 *opt = (int8 *)&modes;
    return opt[IDX_COMPRESSION_IN_MODES];
}

/*
 * @Description: get COMPRESSLEVEL value
 * @IN modes: compressing-modes value
 * @Return: COMPRESSLEVEL value
 * @See also:
 */
int8 heaprel_get_compresslevel_from_modes(int16 modes)
{
    int8 *opt = (int8 *)&modes;
    return opt[IDX_COMPRESSLEVEL_IN_MODES];
}

/*
 * @Description: some storage parameter cannot be changed by ALTER TABLE statement.
 *   this function do the checking work.
 * @Param[IN] options: input user options
 * @See also:
 */
void ForbidUserToSetDefinedOptions(List *options)
{
    /* the following option must be in tab[] of default_reloptions(). */
    static const char *unchangedOpt[] = {"orientation", "hashbucket", "bucketcnt", "segment", "encrypt_algo",
        "storage_type"};

    int firstInvalidOpt = -1;
    if (FindInvalidOption(options, unchangedOpt, lengthof(unchangedOpt), &firstInvalidOpt)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        (errmsg("Un-support feature"),
                         errdetail("Option \"%s\" doesn't allow ALTER", unchangedOpt[firstInvalidOpt]))));
    }
}

/*
 * @Description: compressed parameter cannot be changed by ALTER TABLE statement if table is uncompressed table.
 *   this function do the checking work.
 * @Param[IN] options: input user options
 * @See also:
 */
void ForbidUserToSetCompressedOptions(List *options)
{
    static const char *unSupportOptions[] = {"compresstype",   "compress_chunk_size",   "compress_prealloc_chunks",
                                             "compress_level", "compress_byte_convert", "compress_diff_convert"};
    int firstInvalidOpt = -1;
    if (FindInvalidOption(options, unSupportOptions, lengthof(unSupportOptions), &firstInvalidOpt)) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 (errmsg("Un-support feature"), errdetail("Option \"%s\" doesn't allow ALTER on uncompressed table",
                                                          unSupportOptions[firstInvalidOpt]))));
    }
}

/*
 * @Description: forbid to change inner option
 *   inner options only can be used by system itself.
 *   forbid all the users to set or change the inner options.
 * @Param[IN] userOptions: input user options
 * @See also:
 */
void ForbidOutUsersToSetInnerOptions(List *userOptions)
{
    static const char* innnerOpts[] = {
        "internal_mask", "start_ctid_internal", "end_ctid_internal", "append_mode_internal", "wait_clean_gpi"};

    if (userOptions != NULL) {
        int firstInvalidOpt = -1;

        if (FindInvalidOption(userOptions, innnerOpts, lengthof(innnerOpts), &firstInvalidOpt)) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Un-support feature"),
                            errdetail("Forbid to set or change inner option \"%s\"", innnerOpts[firstInvalidOpt])));
        }
    }
}

void ForbidUserToSetDefinedIndexOptions(List *options)
{
    /* the following option must be in tab[] of default_reloptions(). */
    static const char *unchangedOpt[] = {"crossbucket", "storage_type"};

    int firstInvalidOpt = -1;
    if (FindInvalidOption(options, unchangedOpt, lengthof(unchangedOpt), &firstInvalidOpt)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        (errmsg("Un-support feature"),
                         errdetail("Option \"%s\" doesn't allow ALTER", unchangedOpt[firstInvalidOpt]))));
    }
}

/**
 * @Description: parse text search options. there are 3 inline parsers(default,zhparser,
 *                      ngram), and only  configuration defined with N-gram and zhparser
 *                      support options, configuration's options have strong correlation with
 *                      its parser, and echo options corresponds to a guc paramater
 *
 * @in tsoptions - options defined in create /alter synax
 * @in validate -if it will check options
 * @in prsoid - text search parser oid, only N-gram/zhparser have options
 * @return -option values
 */
bytea *tsearch_config_reloptions(Datum tsoptions, bool validate, Oid prsoid, bool missing_ok)
{
    relopt_value *options = NULL;
    ParserCfOpts *cfopts = NULL;
    NgramCfOpts *ncf = NULL;
    PoundCfOpts *pcf = NULL;
    int numoptions;

    /*
     * N-gram parser's options
     * ngram_tab.punctuation_ignore -> ngram_punctuation_ignore
     * ngram_tab.gram_size -> ngram_gram_size
     * ngram_tab.grapsymbol_ignore -> ngram_grapsymbol_ignore
     */
    static const relopt_parse_elt ngram_tab[] = {
        { "gram_size", RELOPT_TYPE_INT, offsetof(NgramCfOpts, gram_size) },
        { "punctuation_ignore", RELOPT_TYPE_BOOL, offsetof(NgramCfOpts, punctuation_ignore) },
        { "grapsymbol_ignore", RELOPT_TYPE_BOOL, offsetof(NgramCfOpts, grapsymbol_ignore) }
    };

    /*
     * pound parser's options
     * pound_tab.pound_split_flag -> pound_split_flag
     */
    static const relopt_parse_elt pound_tab[] = {{ "split_flag", RELOPT_TYPE_STRING, offsetof(PoundCfOpts, split_flag) }};

    /*
     * we parse configuration options with following steps
     *     a) parse the options and validate the value
     *     b) fill in undefined filed with default value
     */
    if (prsoid == NGRAM_PARSER) {
        options = parseRelOptions(tsoptions, validate, RELOPT_KIND_NPARSER, &numoptions);
        Assert(options && numoptions);
        ncf = (NgramCfOpts *)allocateReloptStruct(sizeof(NgramCfOpts), options, numoptions);
        fillRelOptions((void *)ncf, sizeof(NgramCfOpts), options, numoptions, validate, ngram_tab, lengthof(ngram_tab));
        cfopts = (ParserCfOpts *)ncf;
    } else if (prsoid == ZHPARSER_PARSER) {
        ereport(ERROR,
                (errmodule(MOD_TS), errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Zhparser is not supported!")));
    } else if (prsoid == POUND_PARSER) {
        options = parseRelOptions(tsoptions, validate, RELOPT_KIND_PPARSER, &numoptions);
        Assert(options && numoptions);

        if (options == NULL) {
            ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("options is NULL when using pound parser")));
        }

        /* Add some restrictions on pound parser split flag configuration. */
        if (options->isset) {
            if (strlen(options->values.string_val) != 1) {
                ereport(ERROR, (errcode(ERRCODE_ZERO_LENGTH_CHARACTER_STRING),
                                errmsg("The split flag should exactly be one character and can not be NULL.")));
            }
            if (*options->values.string_val != '@' && *options->values.string_val != '/' &&
                *options->values.string_val != '#' && *options->values.string_val != '$' &&
                *options->values.string_val != '%') {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("%s is not supported by pound parser.", options->values.string_val)));
            }
        }

        pcf = (PoundCfOpts *)allocateReloptStruct(sizeof(PoundCfOpts), options, numoptions);
        fillRelOptions((void *)pcf, sizeof(PoundCfOpts), options, numoptions, validate, pound_tab, lengthof(pound_tab));
        cfopts = (ParserCfOpts *)pcf;
    } else if (!missing_ok) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("current text search configuration doesnot support options"),
                 errdetail("only text search configuration defined with ngram/zhparser parser support options")));
    }

    if (options != NULL)
        pfree(options);

    return (bytea *)cfopts;
}

/* remove an option from options list. If succeeded, set removed = true */ 
List* RemoveRelOption(List* options, const char* optName, bool* removed)
{
    ListCell* lcell = NULL;
    DefElem* opt = NULL;
    bool found = false;

    foreach (lcell, options) {
        opt = (DefElem*)lfirst(lcell);
        if (strncmp(opt->defname, optName, strlen(optName)) == 0) {
            found = true;
            break;
        }
    }

    if (found) {
        options = list_delete_ptr(options, opt);
        pfree_ext(opt);
    }
    if (removed != NULL) {
        *removed = found;
    }

    return options;
}

/*
 * determine the final crossbucket option value based on the combination of various factors:
 * 
 * default_index_kind (kind): { 0, 1, 2 }, represented by 2 bits
 *     0: denotes turning off multiple nodes GPI feature
 *     1: denotes creating local index by default
 *     2: denotes creating global index by default
 * stmtoptcbi (cbi): { -1, 0, 1 }, represented by 2 bits after + 1
 *    -1: denotes no crossbucket option is specified in CREATE INDEX statement
 *     0: denotes crossbucket=off is specified
 *     1: denotes crossbucket=on is specified
 * stmtoptgpi (gpi): { 0, 1 }, represented by 1 bit
 *     0: denotes stmt->isGlobal is false
 *     1: denotes stmt->isGlobal is true
 * 
 * combine above three items from high to low to one 5 bits value:
 * 
 *     kind cbi gpi  bits  res
 *     00   00  0  = 0x0   0
 *     00   00  1  = 0x1  -1
 *     00   01  0  = 0x2   0
 *     00   01  1  = 0x3  -1
 *     00   10  0  = 0x4   1
 *     00   10  1  = 0x5  -1
 *     01   00  0  = 0x8   0
 *     01   00  1  = 0x9   1
 *     01   01  0  = 0xA   0
 *     01   01  1  = 0xB  -1
 *     01   10  0  = 0xC   1
 *     01   10  1  = 0xD   1
 *     10   00  0  = 0x10  1
 *     10   00  1  = 0x11  1
 *     10   01  0  = 0x12  0
 *     10   01  1  = 0x13 -1
 *     10   10  0  = 0x14  1
 *     10   10  1  = 0x15  1
 * 
 * return values:
 *    -1: invalid combination
 *     0: the determined crossbucket option is false
 *     1: the determined crossbucket option is true
 */
static int map_crossbucket_option(const int default_index_kind, const int stmtoptcbi, const bool stmtoptgpi)
{
    const uint8 width = 2;
    uint8 bits = default_index_kind & 0xFF;
    uint8 cbi = (stmtoptcbi + 1) & 0xFF;
    uint8 gpi = stmtoptgpi ? 1 : 0;
    int res = -1;

    bits <<= width;
    bits |= cbi;
    bits <<= 1;
    bits |= gpi;

    switch (bits) {
        case 0x0:
        case 0x2:
        case 0x8:
        case 0xA:
        case 0x12:
            res = 0;
            break;
        case 0x4:
        case 0x9:
        case 0xC:
        case 0xD:
        case 0x10:
        case 0x11:
        case 0x14:
        case 0x15:
            res = 1;
            break;
        default:
            break;
    }

    return res;
}

/* caller must make sure it gets called for hashbucket-enabled relation */
bool get_crossbucket_option(List **options_ptr, bool stmtoptgpi, char *accessmethod, int *crossbucketopt)
{
    ListCell *cell = NULL;
    int stmtoptcbi = -1; /* -1 means the SQL statement doesn't contain crossbucket option */
    int res;

    Assert(options_ptr != NULL);

    foreach (cell, *options_ptr) {
        DefElem *elem = (DefElem *)lfirst(cell);
        if (strcmp(elem->defname, "crossbucket") == 0) {
            stmtoptcbi = defGetBoolean(elem) ? 1 : 0;
            break;
        }
    }

    if (crossbucketopt != NULL) {
        *crossbucketopt = stmtoptcbi;
    }

    res = map_crossbucket_option(u_sess->attr.attr_storage.default_index_kind, stmtoptcbi, stmtoptgpi);
    if (res < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("Invalid crossbucket setting for global partitioned index with hashbucket enabled.")));
    }
    if (res > 0 && stmtoptcbi == -1) {
        if (accessmethod != NULL && pg_strcasecmp(accessmethod, "btree") != 0) {
            /* skip non-btree index and return false */
            return false;
        } else {
            /* 
             * The above logic determined crossbucket option should be true, but the
             * statement didn't contain it, so append it here to make it persistent.
             */
            *options_ptr = lappend(*options_ptr, makeDefElem("crossbucket", (Node*)makeString("on")));
        }
    }

    return ((res <= 0) ? false : true);
}

bool is_contain_crossbucket(List *defList)
{
    ListCell *lc = NULL;
    /* Scan list to see if orientation was ROW store. */
    foreach (lc, defList) {
        DefElem* def = (DefElem*)lfirst(lc);
        if (pg_strcasecmp(def->defname, "crossbucket") == 0) {
            return true;
        }
    }
    return false;
}

bool is_cstore_option(char relkind, Datum reloptions)
{
    StdRdOptions* std_opt = (StdRdOptions*)heap_reloptions(relkind, reloptions, false);
    bool result = std_opt != NULL && pg_strcasecmp(ORIENTATION_COLUMN,
        StdRdOptionsGetStringData(std_opt, orientation, ORIENTATION_ROW)) == 0;
    pfree_ext(std_opt);
    return result;
}

void SetOneOfCompressOption(DefElem* defElem, TableCreateSupport* tableCreateSupport)
{
    auto defname = defElem->defname;
    if (pg_strcasecmp(defname, "compresstype") == 0) {
        tableCreateSupport->compressType = defGetInt64(defElem);
    } else if (pg_strcasecmp(defname, "compress_chunk_size") == 0) {
        tableCreateSupport->compressChunkSize = true;
    } else if (pg_strcasecmp(defname, "compress_prealloc_chunks") == 0) {
        tableCreateSupport->compressPreAllocChunks = true;
    } else if (pg_strcasecmp(defname, "compress_level") == 0) {
        tableCreateSupport->compressLevel = true;
    } else if (pg_strcasecmp(defname, "compress_byte_convert") == 0) {
        tableCreateSupport->compressByteConvert = defGetBoolean(defElem);
    } else if (pg_strcasecmp(defname, "compress_diff_convert") == 0) {
        tableCreateSupport->compressDiffConvert = defGetBoolean(defElem);
    }
}

void CheckCompressOption(TableCreateSupport *tableCreateSupport)
{
    if (!tableCreateSupport->compressType && HasCompressOption(tableCreateSupport)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPTION),
                        errmsg("compress_chunk_size/compress_prealloc_chunks/compress_level/compress_byte_convert/"
                               "compress_diff_convert should be used with compresstype.")));
    }
    if (!tableCreateSupport->compressByteConvert && tableCreateSupport->compressDiffConvert) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPTION),
                        errmsg("compress_diff_convert should be used with compress_byte_convert.")));
    }
    if (tableCreateSupport->compressType != COMPRESS_TYPE_ZSTD && tableCreateSupport->compressLevel) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_OPTION),
            errmsg("compress_level should be used with ZSTD algorithm.")));
    }
}