/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
 * knl_session.h
 *        Data stucture for session level global variables.
 *
 * several guidelines for put variables in knl_session.h
 *
 * variables which related to session only and can be decoupled with thread status 
 * should put int knl_session.h
 *
 * pay attention to memory allocation
 * allocate runtime session variable in session context, which is highly recommended
 * to do that.
 *
 * variables related to dedicated utility thread(wal, bgwrited, log...) usually put
 * in knl_thread.h
 *
 * all context define below should follow naming rules:
 * knl_u_sess->on_xxxx
 * 
 * IDENTIFICATION
 *        src/include/knl/knl_session.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_KNL_KNL_SESSION_H_
#define SRC_INCLUDE_KNL_KNL_SESSION_H_

#include <signal.h>

#include "access/ustore/undo/knl_uundotype.h"
#include "access/skey.h"
#include "c.h"
#include "datatype/timestamp.h"
#include "gs_thread.h"
#include "knl/knl_guc.h"
#include "lib/dllist.h"
#include "lib/ilist.h"
#include "lib/stringinfo.h"
#include "libpq/pqcomm.h"
#include "nodes/pg_list.h"
#include "nodes/bitmapset.h"
#include "cipher.h"
#include "openssl/ossl_typ.h"
#include "portability/instr_time.h"
#include "storage/backendid.h"
#include "storage/lock/s_lock.h"
#include "storage/shmem.h"
#include "utils/palloc.h"
#include "utils/memgroup.h"
#include "storage/lock/lock.h"


typedef void (*pg_on_exit_callback)(int code, Datum arg);

/* all session level attribute which expose to user. */
typedef struct knl_session_attr {

    knl_session_attr_sql attr_sql;
    knl_session_attr_storage attr_storage;
    knl_session_attr_security attr_security;
    knl_session_attr_network attr_network;
    knl_session_attr_memory attr_memory;
    knl_session_attr_resource attr_resource;
    knl_session_attr_common attr_common;
} knl_session_attr;

typedef struct knl_u_stream_context {
    uint32 producer_dop;

    uint32 smp_id;

    bool in_waiting_quit;

    bool dummy_thread;

    bool enter_sync_point;

    bool inStreamEnv;

    bool stop_mythread;

    ThreadId stop_pid;

    uint64 stop_query_id;

    class StreamNodeGroup* global_obj;

    class StreamProducer* producer_obj;

    MemoryContext stream_runtime_mem_cxt;

    /* Shared memory context for in-memory data exchange. */
    MemoryContext data_exchange_mem_cxt;
} knl_u_stream_context;

typedef struct knl_u_executor_context {
    List* remotequery_list;

    bool exec_result_checkqual_fail;

    bool under_stream_runtime;
    bool under_auto_explain;

    /* This variable indicates wheather the prescribed extension is supported */
    bool extension_is_valid;

    uint2* global_bucket_map;

    int    global_bucket_cnt;

    struct HTAB* vec_func_hash;

    struct PartitionIdentifier* route;

    struct TupleHashTableData* cur_tuple_hash_table;

    class lightProxy* cur_light_proxy_obj;

    /*
     * ActivePortal is the currently executing Portal (the most closely nested,
     * if there are several).
     */
    struct PortalData* ActivePortal;
    bool need_track_resource;
    int nesting_level;

    HTAB* PortalHashTable;
    unsigned int unnamed_portal_count;
    /* the single instance for statement retry controll per thread*/
    struct StatementRetryController* RetryController;

    bool hasTempObject;
    bool DfsDDLIsTopLevelXact;
    /* global variable used to determine if we could cancel redistribution transaction */
    bool could_cancel_redistribution;

    bool g_pgaudit_agent_attached;
    /* whether to track sql stats */
    bool pgaudit_track_sqlddl;

    struct HashScanListData* HashScans;

    /* the flag indicate the executor can stop, do not send anything to outer */
    bool executorStopFlag;

    struct OpFusion* CurrentOpFusionObj;

    int global_iteration;

    bool is_exec_trigger_func;

    /* the owner of cast */
    Oid cast_owner;

    bool single_shard_stmt;
    struct SendRouter* CurrentRouter;
    bool is_dn_enable_router;

    bool isExecTrunc;

    bool isLockRows;
} knl_u_executor_context;

typedef struct knl_u_sig_context {
    /*
     * Flag to mark SIGHUP. Whenever the main loop comes around it
     * will reread the configuration file. (Better than doing the
     * reading in the signal handler, ey?)
     */
    volatile sig_atomic_t got_SIGHUP;

    /*
     * like got_PoolReload, but just for the compute pool.
     * see CPmonitor_MainLoop for more details.
     */
    volatile sig_atomic_t got_pool_reload;
    volatile sig_atomic_t cp_PoolReload;
} knl_u_sig_context;

class AutonomousSession;

typedef struct TableOfIndexPass {
    Oid tableOfIndexType = InvalidOid;
    HTAB* tableOfIndex = NULL;
    int tableOfNestLayer = -1; /* number of layers of this tablevar */
    int tableOfGetNestLayer = -1; /* number of layers of this tablevar needs to be get */
} TableOfIndexPass;

typedef struct knl_u_SPI_context {
#define BUFLEN 64
#define XACTMSGLEN 128

    Oid lastoid;

    char buf[BUFLEN];

    int _stack_depth; /* allocated size of _SPI_stack */

    int _connected;

    int _curid;

    struct _SPI_connection* _stack;

    struct _SPI_connection* _current;

    HTAB* SPICacheTable;

    bool is_allow_commit_rollback;
    char forbidden_commit_rollback_err_msg[XACTMSGLEN];

    /* Is Procedure or Function. */
    bool is_stp;

    bool is_complex_sql;

    /* Whether procedure contains option setting guc. */
    bool is_proconfig_set;

    /* Recording the nested exception counts for commit/rollback statement after. */
    int portal_stp_exception_counter;

    /* 
     * Recording the current execute procedure or function is with a exception declare
     * or not, if it called by a procedure or function with exception,it's value still
     * be true
     */
    bool current_stp_with_exception;

    AutonomousSession *autonomous_session;
    /* for commit/rollback in spi execute plan, we add refcount to avoid cplan
     * be released by resource owner, but need to make sure refcount be sub before longjmp abort transaction */
    struct SPICachedPlanStack* spi_exec_cplan_stack;
    struct CachedPlan* cur_spi_cplan;
    /* save table's index into session, for pass into function */
    struct TableOfIndexPass* cur_tableof_index;

    bool has_stream_in_cursor_or_forloop_sql;
} knl_u_SPI_context;

typedef struct knl_u_index_context {
    typedef uint64 XLogRecPtr;
    XLogRecPtr counter;
} knl_u_index_context;

typedef struct knl_u_instrument_context {
    bool perf_monitor_enable;

    bool can_record_to_table;

    int operator_plan_number;

    bool OBS_instr_valid;

    bool* p_OBS_instr_valid;

    class StreamInstrumentation* global_instr;

    class ThreadInstrumentation* thread_instr;

    class OBSInstrumentation* obs_instr;

    struct Qpid* gs_query_id;

    struct BufferUsage* pg_buffer_usage;

    int    plan_size;
} knl_u_instrument_context;

typedef struct knl_u_analyze_context {
    bool is_under_analyze;

    bool need_autoanalyze;

    MemoryContext analyze_context;

    struct AutoAnaProcess* autoanalyze_process;

    struct StringInfoData* autoanalyze_timeinfo;

    struct BufferAccessStrategyData* vac_strategy;
} knl_u_analyze_context;

#define PATH_SEED_FACTOR_LEN 3

typedef struct knl_u_optimizer_context {
    bool disable_dop_change;

    bool enable_nodegroup_explain;

    bool has_obsrel;

    bool is_stream;

    bool is_stream_support;

    bool is_multiple_nodegroup_scenario;

    /* Record the different nodegroup count for multi nodegroup scenario. */
    int different_nodegroup_count;

    bool is_randomfunc_shippable;

    bool is_dngather_support;

    int srvtype;

    int qrw_inlist2join_optmode;

    uint32 plan_current_seed;

    uint32 plan_prev_seed;

    uint16 path_current_seed_factor[PATH_SEED_FACTOR_LEN];

    double cursor_tuple_fraction;

    /* Global variables used for parallel execution. */
    int query_dop_store; /* Store the dop. */

    int query_dop; /* Degree of parallel, 1 means not parallel. */

    double smp_thread_cost;

    /* u_sess->opt_cxt.max_query_dop:
     * When used in dynamic smp, this variable set the max limit of dop
     * <0 means turned off dynsmp.
     *    0 means optimized dop based on query and resources
     *    1 is the same as set u_sess->opt_cxt.query_dop=1
     *    2..n means max limit of dop
     */
    int max_query_dop;

    int parallel_debug_mode; /* Control the parallel debug mode. */

    int skew_strategy_opt;

    int op_work_mem;

    MemoryContext ft_context;

    struct Distribution* in_redistribution_group_distribution;

    struct Distribution* compute_permission_group_distribution;

    struct Distribution* query_union_set_group_distribution;

    struct Distribution* single_node_distribution;

    struct DynamicSmpInfo* dynamic_smp_info;

    struct PartitionIdentifier* bottom_seq;

    struct PartitionIdentifier* top_seq;

    struct HTAB* opr_proof_cache_hash;

    struct ShippingInfo* not_shipping_info;

    bool is_under_append_plan;
} knl_u_optimizer_context;

typedef struct knl_u_parser_context {
    bool eaten_begin;

    bool eaten_declare;

    bool has_dollar;

    bool has_placeholder;

    bool stmt_contains_operator_plus;

    bool is_in_package_body;

    bool isFunctionDeclare;

    bool is_procedure;

    bool is_load_copy;

    List* col_list;

    char* copy_fieldname;

    List* hint_list;

    List* hint_warning;

    struct HTAB* opr_cache_hash;

    void* param_info;

    StringInfo param_message;

    MemoryContext ddl_pbe_context;

    bool in_package_function_compile;

    /* this is used in parser.gram.y not in plpgsql */
    bool isCreateFuncOrProc;
    
    bool isTimeCapsule;
} knl_u_parser_context;

typedef struct knl_u_trigger_context {
    struct HTAB* ri_query_cache;

    struct HTAB* ri_compare_cache;

    bool exec_row_trigger_on_datanode;

    /* How many levels deep into trigger execution are we? */
    int MyTriggerDepth;
    List* info_list;
    struct AfterTriggersData* afterTriggers;
} knl_u_trigger_context;

enum guc_attr_strategy {
    GUC_ATTR_SQL = 0,
    GUC_ATTR_STORAGE,
    GUC_ATTR_SECURITY,
    GUC_ATTR_NETWORK,
    GUC_ATTR_MEMORY,
    GUC_ATTR_RESOURCE,
    GUC_ATTR_COMMON,
    MAX_GUC_ATTR
};

typedef struct knl_u_utils_context {
    char suffix_char;

    Oid suffix_collation;

    int test_err_type;

    /*
     * While compute_array_stats is running, we keep a pointer to the extra data
     * here for use by assorted subroutines.  compute_array_stats doesn't
     * currently need to be re-entrant, so avoiding this is not worth the extra
     * notational cruft that would be needed.
     */
    struct ArrayAnalyzeExtraData* array_extra_data;

    struct ItemPointerData* cur_last_tid;

    struct DistributeTestParam* distribute_test_param;

    struct SegmentTestParam* segment_test_param;

    struct PGLZ_HistEntry** hist_start;

    struct PGLZ_HistEntry* hist_entries;

    struct PGLZ_HistEntry** new_hist_start;

    struct PGLZ_HistEntry* new_hist_entries;


    char* analysis_options_configure;

    int* guc_new_value;

    int GUC_check_errcode_value;

    TimestampTz lastFailedLoginTime;

    struct StringInfoData* input_set_message; /*Add for set command in transaction*/

    char* GUC_check_errmsg_string;

    char* GUC_check_errdetail_string;

    char* GUC_check_errhint_string;

    HTAB* set_params_htab;

    struct config_generic** sync_guc_variables;

    struct config_bool* ConfigureNamesBool[MAX_GUC_ATTR];

    struct config_int* ConfigureNamesInt[MAX_GUC_ATTR];

    struct config_real* ConfigureNamesReal[MAX_GUC_ATTR];

    struct config_int64* ConfigureNamesInt64[MAX_GUC_ATTR];

    struct config_string* ConfigureNamesString[MAX_GUC_ATTR];

    struct config_enum* ConfigureNamesEnum[MAX_GUC_ATTR];

    int size_guc_variables;

    bool guc_dirty; /* TRUE if need to do commit/abort work */

    bool reporting_enabled; /* TRUE to enable GUC_REPORT */

    int GUCNestLevel; /* 1 when in main transaction */

    unsigned int behavior_compat_flags;

    int save_argc;

    char** save_argv;

    /* Hash table to lookup combo cids by cmin and cmax */
    HTAB* comboHash;

    /*
     * An array of cmin,cmax pairs, indexed by combo command id.
     * To convert a combo cid to cmin and cmax, you do a simple array lookup.
     */
    struct ComboCidKeyData* comboCids;

    /* use for stream producer thread */
    struct ComboCidKeyData* StreamParentComboCids;

    int usedComboCids; /* number of elements in comboCids */

    int sizeComboCids; /* allocated size of array */

    int StreamParentsizeComboCids; /* allocated size of array for  StreamParentComboCids */

    /*
     * CurrentSnapshot points to the only snapshot taken in transaction-snapshot
     * mode, and to the latest one taken in a read-committed transaction.
     * SecondarySnapshot is a snapshot that's always up-to-date as of the current
     * instant, even in transaction-snapshot mode.    It should only be used for
     * special-purpose code (say, RI checking.)
     *
     * These SnapshotData structs are static to simplify memory allocation
     * (see the hack in GetSnapshotData to avoid repeated malloc/free).
     */
    struct SnapshotData* CurrentSnapshotData;

    struct SnapshotData* SecondarySnapshotData;

    struct SnapshotData* CurrentSnapshot;

    struct SnapshotData* SecondarySnapshot;

    struct SnapshotData* CatalogSnapshot;

    struct SnapshotData* HistoricSnapshot;

    /* Staleness detection for CatalogSnapshot. */
    bool CatalogSnapshotStale;

    /*
     * These are updated by GetSnapshotData.  We initialize them this way
     * for the convenience of TransactionIdIsInProgress: even in bootstrap
     * mode, we don't want it to say that BootstrapTransactionId is in progress.
     *
     * RecentGlobalXmin, RecentGlobalDataXmin, RecentGlobalCatalogXmin are initialized to
     * InvalidTransactionId, to ensure that no one tries to use a stale
     * value. Readers should ensure that it has been set to something else
     * before using it.
     */
    TransactionId TransactionXmin;

    TransactionId RecentXmin;

    TransactionId RecentDataXmin;

    TransactionId RecentGlobalXmin;

    TransactionId RecentGlobalDataXmin;

    /* recent global catalog xmin, consider replication slot catalog xmin */
    TransactionId RecentGlobalCatalogXmin;

    /* Global snapshot data */
    bool cn_xc_maintain_mode;
    int snapshot_source;
    TransactionId gxmin;
    TransactionId gxmax;
    uint32 GtmTimeline;
    struct GTM_SnapshotData* g_GTM_Snapshot;
    uint64 g_snapshotcsn;
    bool snapshot_need_sync_wait_all;
    bool is_autovacuum_snapshot;

    /* (table, ctid) => (cmin, cmax) mapping during timetravel */
    HTAB* tuplecid_data;

    /* Top of the stack of active snapshots */
    struct ActiveSnapshotElt* ActiveSnapshot;

    /*
     * How many snapshots is resowner.c tracking for us?
     *
     * Note: for now, a simple counter is enough.  However, if we ever want to be
     * smarter about advancing our MyPgXact->xmin we will need to be more
     * sophisticated about this, perhaps keeping our own list of snapshots.
     */
    int RegisteredSnapshots;

    /* first GetTransactionSnapshot call in a transaction? */
    bool FirstSnapshotSet;

    /*
     * Remember the serializable transaction snapshot, if any.    We cannot trust
     * FirstSnapshotSet in combination with IsolationUsesXactSnapshot(), because
     * GUC may be reset before us, changing the value of IsolationUsesXactSnapshot.
     */
    struct SnapshotData* FirstXactSnapshot;

    /* Current xact's exported snapshots (a list of Snapshot structs) */
    List* exportedSnapshots;

    uint8_t g_output_version; /* Set the default output schema. */

    int XactIsoLevel;

    /* a string list parsed from GUC variable (uuncontrolled_memory_context),see more @guc.cpp */
    struct memory_context_list* memory_context_limited_white_list;

#ifdef ENABLE_QUNIT
    int qunit_case_number;
#endif

    bool enable_memory_context_control;

    syscalllock deleMemContextMutex;
} knl_u_utils_context;

typedef struct knl_u_security_context {
    /*
     * In common cases the same roleid (ie, the session or current ID) will
     * be queried repeatedly.  So we maintain a simple one-entry cache for
     * the status of the last requested roleid.  The cache can be flushed
     * at need by watching for cache update events on pg_authid.
     */
    Oid last_roleid; /* InvalidOid == cache not valid */

    bool last_roleid_is_super;

    bool last_roleid_is_sysdba;

    bool last_roleid_is_securityadmin; /* Indicates whether a security admin */

    bool last_roleid_is_auditadmin; /* Indicates whether an audit admin */

    bool last_roleid_is_monitoradmin; /* Indicates whether a monitor admin */

    bool last_roleid_is_operatoradmin; /* Indicates whether a operator admin */

    bool last_roleid_is_policyadmin;  /* Indicates whether a policy admin */

    bool roleid_callback_registered;
} knl_u_security_context;

typedef enum AdviseMode {
    /* use optimizer cost */
    AM_COST,
    /* only use expert advice */
	AM_HEURISITICS,
    AM_NONE
} AdviseMode;

typedef enum AdviseType {
    AT_DISTRIBUTIONKEY,
    AT_INDEXKEY,
    AT_NONE
} AdviseType;

typedef enum AdviseCostMode {
    /* use total sql to recommand distribution key */
    ACM_TOTALCOSTLOW,
    /*
     * filter sqls that cost are too large or too small,
     * use partial sql to recommand distribution key.
     */
    ACM_MEDCOSTLOW,        
    ACM_NONE
} AdviseCostMode;

/* Only use it for AM_COST model, different mdoels have different search spaces sizes. */
typedef enum AdviseCompressLevel {
    /*
     * Based on the join actual cost, the combination with the highest weight 
     * is selected by using greedy algorithm.
     */
    ACL_HIGH,
    /*
     * Also using greedy algorithm, but save all combinations regardless 
     * of whether they have the highest weight. Then for each combination,
     * recalculate queries cost using the columns under that combination.
     * Finally find lowest cost of all queries.
     */
    ACL_MED,
    /*
     * Exhaust all columns that meet the conditions, find lowest cost of all queries.
     * Obviously, this model is the most time consuming.
     */
    ACL_LOW,
    ACL_NONE
} AdviseCompressLevel;

typedef struct knl_u_advisor_context {
    enum AdviseMode adviseMode;
    enum AdviseType adviseType;
    enum AdviseCostMode adviseCostMode;
    enum AdviseCompressLevel adviseCompressLevel;
    bool isOnline;
    bool isConstraintPrimaryKey;
    /*
     * This param is uesd when run by online model, we jump recompute search path.
     * Notice: different from isCostModeRunning
     */
    bool isJumpRecompute;
    /* This param is uesd when we choose cost model, we assign virtual distribution key or not. */
    bool isCostModeRunning;
    int currGroupIndex;
    float joinWeight;
    float groupbyWeight;
    float qualWeight;
    TimestampTz maxTime;
    TimestampTz startTime;
    TimestampTz endTime;
    int maxMemory;
    int maxsqlCount;

    HTAB* candicateTables;
    List* candicateQueries;
    List* candicateAdviseGroups;
    List* result;

    MemoryContext SQLAdvisorContext;

    /* This param is used for index_advisor */
    void* getPlanInfoFunc;
} knl_u_advisor_context;

typedef struct knl_u_commands_context {
    /* Tablespace usage  information management struct */
    struct TableSpaceUsageStruct* TableSpaceUsageArray;
    /* cached limit information for segment-page storage, see notes in IsExceedMaxsize*/
    Oid l_tableSpaceOid;
    uint64 l_maxSize;
    bool l_isLimit;

    bool isUnderCreateForeignTable;

    Oid CurrentExtensionObject;

    List* PendingLibraryDeletes;
    TransactionId OldestXmin;
    TransactionId FreezeLimit;
    MultiXactId MultiXactFrzLimit;
    struct SeqTableData* seqtab; /* Head of list of SeqTable items */
    /*
     * last_used_seq is updated by nextval() to point to the last used
     * sequence.
     */
    struct SeqTableData* last_used_seq;

    /* Form of the type created during inplace upgrade */
    char TypeCreateType;

    List* label_provider_list;
    /* bulkload_compatible_illegal_chars and bulkload_copy_state are to be deleted */
    bool bulkload_compatible_illegal_chars;

    /*
     * This variable ought to be a temperary fix for copy to file encoding bug caused by
     * our modification to PGXC copy procejure.
     */
    struct CopyStateData* bulkload_copy_state;
    int dest_encoding_for_copytofile;
    bool need_transcoding_for_copytofile;
    MemoryContext OBSParserContext;
    List* on_commits;

    /*
     * Indicate that the top relation is temporary in my session.
     * If it is, also we treat its all subordinate relations as temporary and in my session.
     * For an example, if a column relation is temporary in my session,
     * also its cudesc, cudesc index, delta are temporary and in my session.
     */
    bool topRelatationIsInMyTempSession;
    Node bogus_marker; /* marks conflicting defaults */
} knl_u_commands_context;

const int ELF_MAGIC_CACHE_LEN = 10;
typedef struct FileSlot {
    FILE* file;
    /* store header of file to detect a elf file */
    char elf_magic[ELF_MAGIC_CACHE_LEN];
    size_t max_linesize;
    int encoding;
    int32 id;
} FileSlot;

typedef struct knl_u_contrib_context {
    /* for assigning cursor numbers and prepared statement numbers */
    unsigned int cursor_number;

    int current_context_id;
    int file_format;  // enum DFSFileType
    int file_number;

    struct FileSlot* slots; /* initilaized with zeros */
    int32 slotid;           /* next slot id */
    int max_linesize;
    char* cur_directory;
	
} knl_u_contrib_context;

#define LC_ENV_BUFSIZE (NAMEDATALEN + 20)

typedef struct knl_u_locale_context {
    /* lc_time localization cache */
    char* localized_abbrev_days[7];

    char* localized_full_days[7];

    char* localized_abbrev_months[12];

    char* localized_full_months[12];

    bool cur_lc_conv_valid;

    bool cur_lc_time_valid;

    struct lconv* cur_lc_conv;

    int lc_ctype_result;

    int lc_collate_result;

    struct HTAB* collation_cache;

    struct lconv current_lc_conv;
} knl_u_locale_context;

typedef struct knl_u_log_context {
    char* syslog_ident;

    char* module_logging_configure;
    /*
     * msgbuf is declared as static to save the data to put
     * which can be flushed in next put_line()
     */
    struct StringInfoData* msgbuf;
} knl_u_log_context;

typedef struct knl_u_mb_context {
    bool insertValuesBind_compatible_illegal_chars;

    List* ConvProcList; /* List of ConvProcInfo */

    /*
     * These variables point to the currently active conversion functions,
     * or are NULL when no conversion is needed.
     */
    struct FmgrInfo* ToServerConvProc;

    struct FmgrInfo* ToClientConvProc;

    NameData datcollate; /* LC_COLLATE setting */
    NameData datctype;   /* LC_CTYPE setting */

    /*
     * These variables track the currently selected FE and BE encodings.
     */
    struct pg_enc2name* ClientEncoding;

    struct pg_enc2name* DatabaseEncoding;

    struct pg_enc2name* PlatformEncoding;

    /*
     * During backend startup we can't set client encoding because we (a)
     * can't look up the conversion functions, and (b) may not know the database
     * encoding yet either.  So SetClientEncoding() just accepts anything and
     * remembers it for InitializeClientEncoding() to apply later.
     */
    bool backend_startup_complete;

    int pending_client_encoding;
} knl_u_mb_context;

typedef struct knl_u_plancache_context {
    /*
     * This is the head of the backend's list of "saved" CachedPlanSources (i.e.,
     * those that are in long-lived storage and are examined for sinval events).
     * We thread the structs manually instead of using List cells so that we can
     * guarantee to save a CachedPlanSource without error.
     */
    struct CachedPlanSource* first_saved_plan;
    /* the head of plancache list not in gpc on cn. */
    struct CachedPlanSource* ungpc_saved_plan;

    /*
     * If an unnamed prepared statement exists, it's stored here.
     * We keep it separate from the hashtable kept by commands/prepare.c
     * in order to reduce overhead for short-lived queries.
     */
    struct CachedPlanSource* unnamed_stmt_psrc;
    /* current plancache pointer for pbe in dn gpc */
    struct CachedPlanSource* cur_stmt_psrc;
    /* plancache pointer count in dn gpc for dfx, always be 0 or 1 */
    int private_refcount;

    /* recode if the query contains params, used in pgxc_shippability_walker */
    bool query_has_params;

    /*
     * The hash table in which prepared queries are stored. This is
     * per-backend: query plans are not shared between backends.
     * The keys for this hash table are the arguments to PREPARE and EXECUTE
     * (statement names); the entries are PreparedStatement structs.
     */
    HTAB* prepared_queries;

    HTAB* stmt_lightproxy_htab; /* mapping statement name and lightproxy obj, only for gpc */

    lightProxy* unnamed_gpc_lp; /* light proxy ptr for shard unnamed cachedplansource, only for gpc */

    HTAB* lightproxy_objs;

    /*
     * The hash table used to find OpFusions.
     * The keys are portal names;
     * the entries are OpFusion pointers.
     */
    HTAB* pn_fusion_htab;

#ifdef PGXC
    /*
     * The hash table where Datanode prepared statements are stored.
     * The keys are statement names referenced from cached RemoteQuery nodes; the
     * entries are DatanodeStatement structs
     */
    HTAB* datanode_queries;
#endif
    char* cur_stmt_name; /* cngpc non lp need set stmt_name in portal run */
    bool gpc_in_ddl;
    bool gpc_remote_msg;
    bool gpc_first_send;
    bool gpc_in_try_store;
    bool gpc_in_batch; /* true if is doing 2 ~ n batch execute, false if not in batch or doing first batch execute */
} knl_u_plancache_context;

typedef struct knl_u_typecache_context {
    /* The main type cache hashtable searched by lookup_type_cache */
    struct HTAB* TypeCacheHash;

    struct HTAB* RecordCacheHash;

    struct tupleDesc** RecordCacheArray;

    int32 RecordCacheArrayLen; /* allocated length of array */

    int32 NextRecordTypmod; /* number of entries used */
} knl_u_typecache_context;

typedef struct knl_u_tscache_context {
    struct HTAB* TSParserCacheHash;

    struct TSParserCacheEntry* lastUsedParser;

    struct HTAB* TSDictionaryCacheHash;

    struct TSDictionaryCacheEntry* lastUsedDictionary;

    struct HTAB* TSConfigCacheHash;

    struct TSConfigCacheEntry* lastUsedConfig;

    Oid TSCurrentConfigCache;
} knl_u_tscache_context;

/*
 * Description:
 *        There are three processing modes in openGauss.  They are
 * BootstrapProcessing or "bootstrap," InitProcessing or
 * "initialization," and NormalProcessing or "normal."
 *
 * The first two processing modes are used during special times. When the
 * system state indicates bootstrap processing, transactions are all given
 * transaction id "one" and are consequently guaranteed to commit. This mode
 * is used during the initial generation of template databases.
 *
 * Initialization mode: used while starting a backend, until all normal
 * initialization is complete.    Some code behaves differently when executed
 * in this mode to enable system bootstrapping.
 *
 * If a openGauss backend process is in normal mode, then all code may be
 * executed normally.
 */

typedef enum ProcessingMode {
    BootstrapProcessing,  /* bootstrap creation of template database */
    InitProcessing,       /* initializing system */
    NormalProcessing,     /* normal processing */
    PostUpgradeProcessing, /* Post upgrade to run script */
    FencedProcessing
} ProcessingMode;

/*
 *    node group mode
 *     now we support two mode: common node group and logic cluster group
 */
typedef enum NodeGroupMode {
    NG_UNKNOWN = 0, /* unknown mode */
    NG_COMMON,      /* common node group */
    NG_LOGIC,       /* logic cluster node group */
} NodeGroupMode;

typedef struct knl_u_misc_context {
    enum ProcessingMode Mode;

    /* Note: we rely on this to initialize as zeroes */
    char socketLockFile[MAXPGPATH];
    char hasocketLockFile[MAXPGPATH];

    /* ----------------------------------------------------------------
     *    User ID state
     *
     * We have to track several different values associated with the concept
     * of "user ID".
     *
     * AuthenticatedUserId is determined at connection start and never changes.
     *
     * SessionUserId is initially the same as AuthenticatedUserId, but can be
     * changed by SET SESSION AUTHORIZATION (if AuthenticatedUserIsSuperuser).
     * This is the ID reported by the SESSION_USER SQL function.
     *
     * OuterUserId is the current user ID in effect at the "outer level" (outside
     * any transaction or function).  This is initially the same as SessionUserId,
     * but can be changed by SET ROLE to any role that SessionUserId is a
     * member of.  (XXX rename to something like CurrentRoleId?)
     *
     * CurrentUserId is the current effective user ID; this is the one to use
     * for all normal permissions-checking purposes.  At outer level this will
     * be the same as OuterUserId, but it changes during calls to SECURITY
     * DEFINER functions, as well as locally in some specialized commands.
     *
     * SecurityRestrictionContext holds flags indicating reason(s) for changing
     * CurrentUserId.  In some cases we need to lock down operations that are
     * not directly controlled by privilege settings, and this provides a
     * convenient way to do it.
     * ----------------------------------------------------------------
     */
    Oid AuthenticatedUserId;

    Oid SessionUserId;

    Oid OuterUserId;

    Oid CurrentUserId;

    int SecurityRestrictionContext;

    const char* CurrentUserName;

    /*
     * logic cluster information every backend.
     */
    Oid current_logic_cluster;

    enum NodeGroupMode current_nodegroup_mode;

    bool nodegroup_callback_registered;

    /*
     * Pseudo_CurrentUserId always refers to CurrentUserId, except in executing stored procedure,
     * in that case, Pseudo_CurrentUserId refers to the user id that created the stored procedure.
     */
    Oid* Pseudo_CurrentUserId;

    /* We also have to remember the superuser state of some of these levels */
    bool AuthenticatedUserIsSuperuser;

    bool SessionUserIsSuperuser;

    /* We also remember if a SET ROLE is currently active */
    bool SetRoleIsActive;

    /* Flag telling that we are loading shared_preload_libraries */
    bool process_shared_preload_libraries_in_progress;

    /* Flag telling that authentication already finish */
    bool authentication_finished;
} knl_misc_context;


/*
 *Session-level status of base backups
 * 
 *This is used in parallel with the shared memory status to control parallel
 *execution of base backup functions for a given session, be it a backend
 *dedicated to replication or a normal backend connected to a database. The
 *update of the session-level status happens at the same time as the shared
 *memory counters to keep a consistent global and local state of the backups
 *running.
 */
typedef enum SessionBackupState {
    SESSION_BACKUP_NONE,
    SESSION_BACKUP_EXCLUSIVE,
    SESSION_BACKUP_NON_EXCLUSIVE
} SessionBackupState;

typedef struct knl_u_proc_context {
    struct Port* MyProcPort;

    char firstChar = ' ';

    Oid MyRoleId;

    Oid MyDatabaseId;

    Oid MyDatabaseTableSpace;

    /*
     * DatabasePath is the path (relative to t_thrd.proc_cxt.DataDir) of my database's
     * primary directory, ie, its directory in the default tablespace.
     */
    char* DatabasePath;

    bool Isredisworker;
    bool IsInnerMaintenanceTools; /* inner tool check flag */
    bool clientIsGsrewind;        /* gs_rewind tool check flag */
    bool clientIsGsredis;         /* gs_redis tool check flag */
    bool clientIsGsdump;          /* gs_dump tool check flag */
    bool clientIsGsCtl;           /* gs_ctl tool check flag */
    bool clientIsGsBasebackup;    /* gs_basebackup tool check flag */
    bool clientIsGsroach;         /* gs_roach tool check flag */
    bool clientIsGsRestore;       /* gs_restore tool check flag */
    bool clientIsSubscription;    /* subscription client check flag */
    bool IsBinaryUpgrade;
    bool IsWLMWhiteList;          /* this proc will not be controled by WLM */
    bool gsRewindAddCount;
    bool PassConnLimit;

    char applicationName[NAMEDATALEN];        /* receive application name in ProcessStartupPacket */

    /*
     * * Session status of running backup, used for sanity checks in SQL-callable
     * * functions to start and stop backups.
     * */
    enum SessionBackupState sessionBackupState;
    bool registerExclusiveHandlerdone;
    /*
     * Store label file and tablespace map during non-exclusive backups.
     */
    char* LabelFile;
    char* TblspcMapFile;
    bool  registerAbortBackupHandlerdone;    /* unterminated backups handler flag */
} knl_u_proc_context;

/* maximum possible number of fields in a date string */
#define MAXDATEFIELDS 25

typedef struct knl_u_time_context {
    /*
     * DateOrder defines the field order to be assumed when reading an
     * ambiguous date (anything not in YYYY-MM-DD format, with a four-digit
     * year field first, is taken to be ambiguous):
     *    DATEORDER_YMD specifies field order yy-mm-dd
     *    DATEORDER_DMY specifies field order dd-mm-yy ("European" convention)
     *    DATEORDER_MDY specifies field order mm-dd-yy ("US" convention)
     *
     * In the openGauss and SQL DateStyles, DateOrder also selects output field
     * order: day comes before month in DMY style, else month comes before day.
     *
     * The user-visible "DateStyle" run-time parameter subsumes both of these.
     */
    int DateStyle;

    int DateOrder;

    /*
     * u_sess->time_cxt.HasCTZSet is true if user has set timezone as a numeric offset from UTC.
     * If so, CTimeZone is the timezone offset in seconds (using the Unix-ish
     * sign convention, ie, positive offset is west of UTC, rather than the
     * SQL-ish convention that positive is east of UTC).
     */
    bool HasCTZSet;

    int CTimeZone;

    int sz_timezone_tktbl;

    /*
     * datetktbl holds date/time keywords.
     *
     * Note that this table must be strictly alphabetically ordered to allow an
     * O(ln(N)) search algorithm to be used.
     *
     * The text field is NOT guaranteed to be NULL-terminated.
     *
     * To keep this table reasonably small, we divide the lexval for TZ and DTZ
     * entries by 15 (so they are on 15 minute boundaries) and truncate the text
     * field at TOKMAXLEN characters.
     * Formerly, we divided by 10 rather than 15 but there are a few time zones
     * which are 30 or 45 minutes away from an even hour, most are on an hour
     * boundary, and none on other boundaries.
     *
     * The static table contains no TZ or DTZ entries, rather those are loaded
     * from configuration files and stored in t_thrd.time_cxt.timezone_tktbl, which has the same
     * format as the static datetktbl.
     */
    struct datetkn* timezone_tktbl;

    const struct datetkn* datecache[MAXDATEFIELDS];

    const struct datetkn* deltacache[MAXDATEFIELDS];

    struct HTAB* timezone_cache;
} knl_u_time_context;

typedef struct knl_u_upgrade_context {
    bool InplaceUpgradeSwitch;
#ifndef WIN32
    PGDLLIMPORT Oid binary_upgrade_next_etbl_pg_type_oid;
    PGDLLIMPORT Oid binary_upgrade_next_etbl_array_pg_type_oid;
    PGDLLIMPORT Oid binary_upgrade_next_etbl_toast_pg_type_oid;
    PGDLLIMPORT Oid binary_upgrade_next_etbl_heap_pg_class_oid;
    PGDLLIMPORT Oid binary_upgrade_next_etbl_index_pg_class_oid;
    PGDLLIMPORT Oid binary_upgrade_next_etbl_toast_pg_class_oid;
    PGDLLIMPORT Oid binary_upgrade_next_etbl_heap_pg_class_rfoid;
    PGDLLIMPORT Oid binary_upgrade_next_etbl_index_pg_class_rfoid;
    PGDLLIMPORT Oid binary_upgrade_next_etbl_toast_pg_class_rfoid;
#endif
    /* Potentially set by contrib/pg_upgrade_support functions */
    Oid binary_upgrade_next_array_pg_type_oid;
    Oid Inplace_upgrade_next_array_pg_type_oid;

    /* Potentially set by contrib/pg_upgrade_support functions */
    Oid binary_upgrade_next_pg_authid_oid;

    Oid binary_upgrade_next_toast_pg_type_oid;
    int32 binary_upgrade_max_part_toast_pg_type_oid;
    int32 binary_upgrade_cur_part_toast_pg_type_oid;
    Oid* binary_upgrade_next_part_toast_pg_type_oid;
    Oid binary_upgrade_next_heap_pg_class_oid;
    Oid binary_upgrade_next_toast_pg_class_oid;
    Oid binary_upgrade_next_heap_pg_class_rfoid;
    Oid binary_upgrade_next_toast_pg_class_rfoid;
    int32 binary_upgrade_max_part_pg_partition_oid;
    int32 binary_upgrade_cur_part_pg_partition_oid;
    Oid* binary_upgrade_next_part_pg_partition_oid;
    int32 binary_upgrade_max_part_pg_partition_rfoid;
    int32 binary_upgrade_cur_part_pg_partition_rfoid;
    Oid* binary_upgrade_next_part_pg_partition_rfoid;
    int32 binary_upgrade_max_part_toast_pg_class_oid;
    int32 binary_upgrade_cur_part_toast_pg_class_oid;
    Oid* binary_upgrade_next_part_toast_pg_class_oid;
    int32 binary_upgrade_max_part_toast_pg_class_rfoid;
    int32 binary_upgrade_cur_part_toast_pg_class_rfoid;
    Oid* binary_upgrade_next_part_toast_pg_class_rfoid;
    Oid binary_upgrade_next_partrel_pg_partition_oid;
    Oid binary_upgrade_next_partrel_pg_partition_rfoid;
    Oid Inplace_upgrade_next_heap_pg_class_oid;
    Oid Inplace_upgrade_next_toast_pg_class_oid;
    Oid binary_upgrade_next_index_pg_class_oid;
    Oid binary_upgrade_next_index_pg_class_rfoid;
    int32 binary_upgrade_max_part_index_pg_class_oid;
    int32 binary_upgrade_cur_part_index_pg_class_oid;
    Oid* binary_upgrade_next_part_index_pg_class_oid;
    int32 binary_upgrade_max_part_index_pg_class_rfoid;
    int32 binary_upgrade_cur_part_index_pg_class_rfoid;
    Oid* binary_upgrade_next_part_index_pg_class_rfoid;
    int32 bupgrade_max_psort_pg_class_oid;
    int32 bupgrade_cur_psort_pg_class_oid;
    Oid* bupgrade_next_psort_pg_class_oid;
    int32 bupgrade_max_psort_pg_type_oid;
    int32 bupgrade_cur_psort_pg_type_oid;
    Oid* bupgrade_next_psort_pg_type_oid;
    int32 bupgrade_max_psort_array_pg_type_oid;
    int32 bupgrade_cur_psort_array_pg_type_oid;
    Oid* bupgrade_next_psort_array_pg_type_oid;
    int32 bupgrade_max_psort_pg_class_rfoid;
    int32 bupgrade_cur_psort_pg_class_rfoid;
    Oid* bupgrade_next_psort_pg_class_rfoid;
    Oid Inplace_upgrade_next_index_pg_class_oid;
    Oid binary_upgrade_next_pg_enum_oid;
    Oid Inplace_upgrade_next_general_oid;
    int32 bupgrade_max_cudesc_pg_class_oid;
    int32 bupgrade_cur_cudesc_pg_class_oid;
    Oid* bupgrade_next_cudesc_pg_class_oid;
    int32 bupgrade_max_cudesc_pg_type_oid;
    int32 bupgrade_cur_cudesc_pg_type_oid;
    Oid* bupgrade_next_cudesc_pg_type_oid;
    int32 bupgrade_max_cudesc_array_pg_type_oid;
    int32 bupgrade_cur_cudesc_array_pg_type_oid;
    Oid* bupgrade_next_cudesc_array_pg_type_oid;
    int32 bupgrade_max_cudesc_pg_class_rfoid;
    int32 bupgrade_cur_cudesc_pg_class_rfoid;
    Oid* bupgrade_next_cudesc_pg_class_rfoid;
    int32 bupgrade_max_cudesc_index_oid;
    int32 bupgrade_cur_cudesc_index_oid;
    Oid* bupgrade_next_cudesc_index_oid;
    int32 bupgrade_max_cudesc_toast_pg_class_oid;
    int32 bupgrade_cur_cudesc_toast_pg_class_oid;
    Oid* bupgrade_next_cudesc_toast_pg_class_oid;
    int32 bupgrade_max_cudesc_toast_pg_type_oid;
    int32 bupgrade_cur_cudesc_toast_pg_type_oid;
    Oid* bupgrade_next_cudesc_toast_pg_type_oid;
    int32 bupgrade_max_cudesc_toast_index_oid;
    int32 bupgrade_cur_cudesc_toast_index_oid;
    Oid* bupgrade_next_cudesc_toast_index_oid;
    int32 bupgrade_max_cudesc_index_rfoid;
    int32 bupgrade_cur_cudesc_index_rfoid;
    Oid* bupgrade_next_cudesc_index_rfoid;
    int32 bupgrade_max_cudesc_toast_pg_class_rfoid;
    int32 bupgrade_cur_cudesc_toast_pg_class_rfoid;
    Oid* bupgrade_next_cudesc_toast_pg_class_rfoid;
    int32 bupgrade_max_cudesc_toast_index_rfoid;
    int32 bupgrade_cur_cudesc_toast_index_rfoid;
    Oid* bupgrade_next_cudesc_toast_index_rfoid;
    int32 bupgrade_max_delta_toast_pg_class_oid;
    int32 bupgrade_cur_delta_toast_pg_class_oid;
    Oid* bupgrade_next_delta_toast_pg_class_oid;
    int32 bupgrade_max_delta_toast_pg_type_oid;
    int32 bupgrade_cur_delta_toast_pg_type_oid;
    Oid* bupgrade_next_delta_toast_pg_type_oid;
    int32 bupgrade_max_delta_toast_index_oid;
    int32 bupgrade_cur_delta_toast_index_oid;
    Oid* bupgrade_next_delta_toast_index_oid;
    int32 bupgrade_max_delta_toast_pg_class_rfoid;
    int32 bupgrade_cur_delta_toast_pg_class_rfoid;
    Oid* bupgrade_next_delta_toast_pg_class_rfoid;
    int32 bupgrade_max_delta_toast_index_rfoid;
    int32 bupgrade_cur_delta_toast_index_rfoid;
    Oid* bupgrade_next_delta_toast_index_rfoid;
    int32 bupgrade_max_delta_pg_class_oid;
    int32 bupgrade_cur_delta_pg_class_oid;
    Oid* bupgrade_next_delta_pg_class_oid;
    int32 bupgrade_max_delta_pg_type_oid;
    int32 bupgrade_cur_delta_pg_type_oid;
    Oid* bupgrade_next_delta_pg_type_oid;
    int32 bupgrade_max_delta_array_pg_type_oid;
    int32 bupgrade_cur_delta_array_pg_type_oid;
    Oid* bupgrade_next_delta_array_pg_type_oid;
    int32 bupgrade_max_delta_pg_class_rfoid;
    int32 bupgrade_cur_delta_pg_class_rfoid;
    Oid* bupgrade_next_delta_pg_class_rfoid;
    Oid Inplace_upgrade_next_pg_proc_oid;
    Oid Inplace_upgrade_next_gs_package_oid;
    Oid Inplace_upgrade_next_pg_namespace_oid;
    Oid binary_upgrade_next_pg_type_oid;
    Oid Inplace_upgrade_next_pg_type_oid;
    bool new_catalog_isshared;
    bool new_catalog_need_storage;
    List* new_shared_catalog_list;
} knl_u_upgrade_context;

typedef struct knl_u_bootstrap_context {
#define MAXATTR 40
#define NAMEDATALEN 64
    int num_columns_read;
    int yyline; /* line number for error reporting */

    struct RelationData* boot_reldesc;                /* current relation descriptor */
    struct FormData_pg_attribute* attrtypes[MAXATTR]; /* points to attribute info */
    int numattr;                                      /* number of attributes for cur. rel */

    struct typmap** Typ;
    struct typmap* Ap;

    MemoryContext nogc; /* special no-gc mem context */
    struct _IndexList* ILHead;
    char newStr[NAMEDATALEN + 1]; /* array type names < NAMEDATALEN long */
} knl_u_bootstrap_context;

typedef enum {
    IDENTIFIER_LOOKUP_NORMAL,  /* normal processing of var names */
    IDENTIFIER_LOOKUP_DECLARE, /* In DECLARE --- don't look up names */
    IDENTIFIER_LOOKUP_EXPR     /* In SQL expression --- special case */
} IdentifierLookup;

typedef struct knl_u_xact_context {
    struct XactCallbackItem* Xact_callbacks;
    struct SubXactCallbackItem* SubXact_callbacks;
#ifdef PGXC
    struct abort_callback_type* dbcleanup_info;
    List *dbcleanupInfoList;
#endif

    /*
     * GID to be used for preparing the current transaction.  This is also
     * global to a whole transaction, so we don't keep it in the state stack.
     */
    char* prepareGID;
    char* savePrepareGID;

    bool pbe_execute_complete;
    List *sendSeqDbName;
    List *sendSeqSchmaName;
    List *sendSeqName;
    List *send_result;
    Oid ActiveLobRelid;
} knl_u_xact_context;

typedef struct PLpgSQL_compile_context {
    int datums_alloc;
    int datums_pkg_alloc;
    int plpgsql_nDatums;
    int plpgsql_pkg_nDatums;
    int datums_last;
    int datums_pkg_last;
    char* plpgsql_error_funcname;
    char* plpgsql_error_pkgname;
    
    struct PLpgSQL_stmt_block* plpgsql_parse_result;
    struct PLpgSQL_stmt_block* plpgsql_parse_error_result;
    struct PLpgSQL_datum** plpgsql_Datums;
    struct PLpgSQL_function* plpgsql_curr_compile;

    bool* datum_need_free; /* need free datum when free function/package memory? */
    bool plpgsql_DumpExecTree;
    bool plpgsql_pkg_DumpExecTree;

    /* pl_funcs.cpp */
    /* ----------
     * Local variables for namespace handling
     *
     * The namespace structure actually forms a tree, of which only one linear
     * list or "chain" (from the youngest item to the root) is accessible from
     * any one plpgsql statement.  During initial parsing of a function, ns_top
     * points to the youngest item accessible from the block currently being
     * parsed.    We store the entire tree, however, since at runtime we will need
     * to access the chain that's relevant to any one statement.
     *
     * Block boundaries in the namespace chain are marked by PLPGSQL_NSTYPE_LABEL
     * items.
     * ----------
     */
    struct PLpgSQL_nsitem* ns_top;

    /* pl_scanner.cpp */
    /* Klugy flag to tell scanner how to look up identifiers */
    IdentifierLookup plpgsql_IdentifierLookup;
    /*
     * Scanner working state.  At some point we might wish to fold all this
     * into a YY_EXTRA struct.    For the moment, there is no need for plpgsql's
     * lexer to be re-entrant, and the notational burden of passing a yyscanner
     * pointer around is great enough to not want to do it without need.
     */

    /* The stuff the core lexer needs(core_yyscan_t) */
    void* yyscanner;
    struct core_yy_extra_type* core_yy;

    /* The original input string */
    const char* scanorig;

    /* Current token's length (corresponds to plpgsql_yylval and plpgsql_yylloc) */
    int plpgsql_yyleng;

    /* Token pushback stack */
#define MAX_PUSHBACKS 100

    int num_pushbacks;
    int pushback_token[MAX_PUSHBACKS];

    /* State for plpgsql_location_to_lineno() */
    const char* cur_line_start;
    const char* cur_line_end;
    int cur_line_num;
    List* goto_labels;

    bool plpgsql_check_syntax;

    struct PLpgSQL_package* plpgsql_curr_compile_package;

    /* A context appropriate for short-term allocs during compilation */
    MemoryContext compile_tmp_cxt;
    MemoryContext compile_cxt;
} PLpgSQL_compile_context;

typedef struct knl_u_plpgsql_context {
    bool inited;

    /* ----------
     * Hash table for compiled functions
     * ----------
     */
    HTAB* plpgsql_HashTable;
    HTAB* plpgsql_pkg_HashTable;
    /* list of functions' cell. */
    DList* plpgsql_dlist_objects; /* for plpgsql_HashEnt */
    /* list of package's cell */
    DList* plpgsqlpkg_dlist_objects; /* for plpgsql_pkg_HashEnt */

    int plpgsql_IndexErrorVariable;
    
    int compile_status;
    struct PLpgSQL_compile_context* curr_compile_context;
    struct List* compile_context_list;

    /* pl_exec.cpp */
    struct EState* simple_eval_estate;
    struct ResourceOwnerData* shared_simple_eval_resowner;
    struct SimpleEcontextStackEntry* simple_econtext_stack;
    struct PLpgSQL_pkg_execstate* pkg_execstate;

    /*
     * "context_array" is allocated from top_transaction_mem_cxt, but the variables inside
     * of SQLContext is created under "SQLContext Entry" memory context.
     */
    List* context_array;
    List* pkg_execstate_list;

    /* pl_handler.cpp */
    /* Hook for plugins */
    struct PLpgSQL_plugin** plugin_ptr;

    /* pl_funcs.cpp */
    int dump_indent;

    struct HTAB* rendezvousHash;

    struct HTAB* debug_proc_htbl;

    struct DebugClientInfo* debug_client;

    struct DebugInfo* cur_debug_server;

    bool has_step_into;

    /* dbe.output buffer limit */
    uint32 dbe_output_buffer_limit;
    Oid running_pkg_oid;
    bool is_delete_function;
    bool is_package_instantiation;
    char* client_info;
    pthread_mutex_t client_info_lock;
    char sess_cxt_name[128];
    HTAB* sess_cxt_htab;
    bool have_error;
    bool create_func_error;

    /* Next ID for PL's executor EState of evaluating "simple" expressions. */
    int64 nextStackEntryId;

    /* how many savepoint have estabulished since current statemnt starts. */
    int stp_savepoint_cnt;

    /* xact context still attached by SPI while transaction is terminated. */
    void *spi_xact_context;

    /* store reseverd subxact resowner's scope temporay during finshing. */
    int64 minSubxactStackId;

    int package_as_line;
    int package_first_line;
    int procedure_start_line;
    int procedure_first_line;
    bool rawParsePackageFunction;
    bool insertError;
    List* errorList;
    int plpgsql_yylloc;
    bool isCreateFunction;
    bool need_pkg_dependencies;
    List* pkg_dependencies;
    List* func_tableof_index;
    TupleDesc pass_func_tupdesc; /* save tupdesc for inner function pass out param to outter function's value */

    int portal_depth; /* portal depth of current thread */

    /* ----------
     * variables for autonomous procedure out ref cursor param and reference package variables
     * ----------
     */
    struct SessionPackageRuntime* auto_parent_session_pkgs; /* package values from parent session */
    bool not_found_parent_session_pkgs; /* flag of found parent session package values? */
    List* storedPortals; /* returned portal date from auto session */
    List* portalContext; /* portal context from parent session */
    bool call_after_auto; /* call after a autonomous transaction procedure? */
    uint64 parent_session_id;
    ThreadId parent_thread_id;
    MemoryContext parent_context; /* parent_context from parent session */

    Oid ActiveLobToastOid;
    struct ExceptionContext* cur_exception_cxt;
    bool pragma_autonomous; /* save autonomous flag */
    char* debug_query_string;
    bool is_insert_gs_source; /* is doing insert gs_source? */
} knl_u_plpgsql_context;

//this is used to define functions in package
typedef knl_u_plpgsql_context knl_u_plpgsql_pkg_context;

typedef struct knl_u_postgres_context {
    /*
     * Flags to implement skip-till-Sync-after-error behavior for messages of
     * the extended query protocol.
     */
    bool doing_extended_query_message;
    bool ignore_till_sync;
} knl_u_postgres_context;

typedef struct knl_u_stat_context {
    char* pgstat_stat_filename;
    char* pgstat_stat_tmpname;

    struct HTAB* pgStatDBHash;
    struct TabStatusArray* pgStatTabList;

    /*
     * BgWriter global statistics counters (unused in other processes).
     * Stored directly in a stats message structure so it can be sent
     * without needing to copy things around.  We assume this inits to zeroes.
     */
    struct PgStat_MsgBgWriter* BgWriterStats;

    /*
     * Hash table for O(1) t_id -> tsa_entry lookup
     */
    HTAB* pgStatTabHash;
    /* MemoryContext for pgStatTabHash */
    MemoryContext pgStatTabHashContext;

    /*
     * Backends store per-function info that's waiting to be sent to the collector
     * in this hash table (indexed by function OID).
     */
    struct HTAB* pgStatFunctions;

    /*
     * Indicates if backend has some function stats that it hasn't yet
     * sent to the collector.
     */
    bool have_function_stats;

    bool pgStatRunningInCollector;

    struct PgStat_SubXactStatus* pgStatXactStack;

    int pgStatXactCommit;

    int pgStatXactRollback;

    int64 pgStatBlockReadTime;

    int64 pgStatBlockWriteTime;

    struct PgBackendStatus* localBackendStatusTable;

    int localNumBackends;

    /*
     * Cluster wide statistics, kept in the stats collector.
     * Contains statistics that are not collected per database
     * or per table.
     */
    struct PgStat_GlobalStats* globalStats;

    /*
     * Total time charged to functions so far in the current backend.
     * We use this to help separate "self" and "other" time charges.
     * (We assume this initializes to zero.)
     */
    instr_time total_func_time;

    struct HTAB* analyzeCheckHash;

    union NumericValue* osStatDataArray;
    struct OSRunInfoDesc* osStatDescArray;
    TimestampTz last_report;
    bool isTopLevelPlSql;
    int64* localTimeInfoArray;
    uint64* localNetInfo;

    MemoryContext pgStatLocalContext;
    MemoryContext pgStatCollectThdStatusContext;

    /* Track memory usage in chunks at individual session level */
    int32 trackedMemChunks;

    /* Track memory usage in bytes at individual session level */
    int64 trackedBytes;

    List* hotkeyCandidates;
    MemoryContext hotkeySessContext;
} knl_u_stat_context;

#define MAX_LOCKMETHOD 2

typedef uint16 CycleCtr;
typedef void* Block;
typedef struct knl_u_storage_context {
    /*
     * How many buffers PrefetchBuffer callers should try to stay ahead of their
     * ReadBuffer calls by.  This is maintained by the assign hook for
     * effective_io_concurrency.  Zero means "never prefetch".
     */
    int target_prefetch_pages;

    volatile bool session_timeout_active;
    /* session_fin_time is valid only if session_timeout_active is true */
    TimestampTz session_fin_time;

    /* Number of file descriptors known to be in use by VFD entries. */
    int nfile;

    /*
     * Flag to tell whether it's worth scanning VfdCache looking for temp files
     * to close
     */
    bool have_xact_temporary_files;
    /*
     * Tracks the total size of all temporary files.  Note: when temp_file_limit
     * is being enforced, this cannot overflow since the limit cannot be more
     * than INT_MAX kilobytes.    When not enforcing, it could theoretically
     * overflow, but we don't care.
     */
    uint64 temporary_files_size;
    int numAllocatedDescs;
    int maxAllocatedDescs;
    struct AllocateDesc* allocatedDescs;
    /*
     * Number of temporary files opened during the current session;
     * this is used in generation of tempfile names.
     */
    long tempFileCounter;
    /*
     * Array of OIDs of temp tablespaces.  When numTempTableSpaces is -1,
     * this has not been set in the current transaction.
     */
    Oid* tempTableSpaces;
    int numTempTableSpaces;
    int nextTempTableSpace;
    /* record how many IO request submit in async mode, because if error happen,
     *  we should distinguish which IOs has been submit, if not, the resource clean will
     *  meet error like ref_count, flags
     *  remember set AsyncSubmitIOCount to 0 after resource clean up sucessfully
     */
    int AsyncSubmitIOCount;

    /*
     * Virtual File Descriptor array pointer and size.    This grows as
     * needed.    'File' values are indexes into this array.
     * Note that VfdCache[0] is not a usable VFD, just a list header.
     */
    struct vfd* VfdCache;
    Size SizeVfdCache;

    /*
     * Each backend has a hashtable that stores all extant SMgrRelation objects.
     * In addition, "unowned" SMgrRelation objects are chained together in a list.
     */
    struct HTAB* SMgrRelationHash;
    dlist_head unowned_reln;

    /* undofile.cpp */
    MemoryContext UndoFileCxt;

    /* md.cpp */
    MemoryContext MdCxt; /* context for all md.c allocations */

    /* sync.cpp */
    MemoryContext pendingOpsCxt;
    struct HTAB *pendingOps;
    List *pendingUnlinks;
    CycleCtr sync_cycle_ctr;
    CycleCtr checkpoint_cycle_ctr;

    LocalTransactionId nextLocalTransactionId;

    /*
     * Whether current session is holding a session lock after transaction commit.
     * This may appear ugly, but since at present all lock information is stored in
     * per-thread pgproc, we should not decouple session from thread if session lock
     * is held.
     * We now set this flag at LockReleaseAll phase during transaction commit or abort.
     * As a result, all callbackups for releasing session locks should come before it.
     */
    bool holdSessionLock[MAX_LOCKMETHOD];

    /* true if datanode is within the process of two phase commit */
    bool twoPhaseCommitInProgress;
    int32 dumpHashbucketIdNum;
    int2 *dumpHashbucketIds;

    /* Pointers to shared state */
    // struct BufferStrategyControl* StrategyControl;
    int NLocBuffer; /* until buffers are initialized */
    struct BufferDesc* LocalBufferDescriptors;
    Block* LocalBufferBlockPointers;
    int32* LocalRefCount;
    int nextFreeLocalBuf;
    struct HTAB* LocalBufHash;
    char* cur_block;
    int next_buf_in_block;
    int num_bufs_in_block;
    int total_bufs_allocated;
    MemoryContext LocalBufferContext;
} knl_u_storage_context;


typedef struct knl_u_libpq_context {
    /*
     * LO "FD"s are indexes into the cookies array.
     *
     * A non-null entry is a pointer to a LargeObjectDesc allocated in the
     * LO private memory context "fscxt".  The cookies array itself is also
     * dynamically allocated in that context.  Its current allocated size is
     * cookies_len entries, of which any unused entries will be NULL.
     */
    struct LargeObjectDesc** cookies;
    int cookies_size;
    MemoryContext fscxt;
    GS_UCHAR* server_key;

    /*
     * These variables hold the pre-parsed contents of the ident usermap
     * configuration file.    ident_lines is a triple-nested list of lines, fields
     * and tokens, as returned by tokenize_file.  There will be one line in
     * ident_lines for each (non-empty, non-comment) line of the file.    Note there
     * will always be at least one field, since blank lines are not entered in the
     * data structure.    ident_line_nums is an integer list containing the actual
     * line number for each line represented in ident_lines.  ident_context is
     * the memory context holding all this.
     */
    List* ident_lines;
    List* ident_line_nums;
    MemoryContext ident_context;
    bool IsConnFromCmAgent;
    bool HasErrorAccurs; /* error accurs, need destory handle */
#ifdef USE_SSL
    bool ssl_loaded_verify_locations;
    bool ssl_initialized;
    SSL_CTX* SSL_server_context;
#endif
} knl_u_libpq_context;

typedef struct knl_u_relcache_context {
    struct HTAB* RelationIdCache;

    /*
     * This flag is false until we have prepared the critical relcache entries
     * that are needed to do indexscans on the tables read by relcache building.
     * Should be used only by relcache.c and catcache.c
     */
    bool criticalRelcachesBuilt;

    /*
     * This flag is false until we have prepared the critical relcache entries
     * for shared catalogs (which are the tables needed for login).
     * Should be used only by relcache.c and postinit.c
     */
    bool criticalSharedRelcachesBuilt;

    /*
     * This counter counts relcache inval events received since backend startup
     * (but only for rels that are actually in cache).    Presently, we use it only
     * to detect whether data about to be written by write_relcache_init_file()
     * might already be obsolete.
     */
    long relcacheInvalsReceived;

    /*
     * This list remembers the OIDs of the non-shared relations cached in the
     * database's local relcache init file.  Note that there is no corresponding
     * list for the shared relcache init file, for reasons explained in the
     * comments for RelationCacheInitFileRemove.
     */
    List* initFileRelationIds;

    /*
     * This flag lets us optimize away work in AtEO(Sub)Xact_RelationCache().
     */
    bool RelCacheNeedEOXActWork;

    HTAB* OpClassCache;

    struct tupleDesc* pgclassdesc;

    struct tupleDesc* pgindexdesc;

    /*
     * BucketMap Cache, consists of a list of BucketMapCache element.
     * Location information of every rel cache is actually pointed to these list
     * members.
     * Attention: we need to invalidate bucket map caches when accepting
     * SI messages of tuples in PGXC_GROUP or SI reset messages!
     */
    List* g_bucketmap_cache;
    uint32 max_bucket_map_size;

    /*
     * These three vaiables are used to preserve pointers to old tupleDescs when processing invalid messages.
     * Because old tupleDescs maybe referenced by other data structures, we can not free them when process invalid
     * messages. So we save them to an array first, and free them in a batch when the transaction ends;
     *
     * Please refer to RememberToFreeTupleDescAtEOX and AtEOXact_FreeTupleDesc for detailed information.
     */
    struct tupleDesc **EOXactTupleDescArray;
    int NextEOXactTupleDescNum;
    int EOXactTupleDescArrayLen;
} knl_u_relcache_context;

#if defined(HAVE_SETPROCTITLE)
#define PS_USE_SETPROCTITLE
#elif defined(HAVE_PSTAT) && defined(PSTAT_SETCMD)
#define PS_USE_PSTAT
#elif defined(HAVE_PS_STRINGS)
#define PS_USE_PS_STRINGS
#elif (defined(BSD) || defined(__hurd__)) && !defined(__darwin__)
#define PS_USE_CHANGE_ARGV
#elif defined(__linux__) || defined(_AIX) || defined(__sgi) || (defined(sun) && !defined(BSD)) || defined(ultrix) || \
    defined(__ksr__) || defined(__osf__) || defined(__svr5__) || defined(__darwin__)
#define PS_USE_CLOBBER_ARGV
#elif defined(WIN32)
#define PS_USE_WIN32
#else
#define PS_USE_NONE
#endif

typedef struct knl_u_ps_context {
#ifndef PS_USE_CLOBBER_ARGV
/* all but one option need a buffer to write their ps line in */
#define PS_BUFFER_SIZE 256
    char ps_buffer[PS_BUFFER_SIZE];
    size_t ps_buffer_size;
#else                            /* PS_USE_CLOBBER_ARGV */
    char* ps_buffer;        /* will point to argv area */
    size_t ps_buffer_size;  /* space determined at run time */
    size_t last_status_len; /* use to minimize length of clobber */
#endif                           /* PS_USE_CLOBBER_ARGV */
    size_t ps_buffer_cur_len;    /* nominal strlen(ps_buffer) */
    size_t ps_buffer_fixed_size; /* size of the constant prefix */

    /* save the original argv[] location here */
    int save_argc;
    char** save_argv;
} knl_u_ps_context;

typedef struct knl_u_ustore_context {
#define MAX_UNDORECORDS_PER_OPERATION 2 /* multi-insert may need special handling */
    class URecVector *urecvec;
    class UndoRecord *undo_records[MAX_UNDORECORDS_PER_OPERATION];

/*
 * Caching several undo buffers.
 * max undo buffers per record = 2
 * max undo records per operation = 2
 */
#define MAX_UNDO_BUFFERS 16 /* multi-insert may need special handling */
    struct UndoBuffer *undo_buffers;
    int undo_buffer_idx;

#define TD_RESERVATION_TIMEOUT_MS (60 * 1000) // 60 seconds
    TimestampTz tdSlotWaitFinishTime;
    bool tdSlotWaitActive;
} knl_u_ustore_context;

typedef struct knl_u_undo_context {
    Bitmapset *undo_zones;
} knl_u_undo_context;

typedef struct ParctlState {
    unsigned char global_reserve;    /* global reserve active statement flag */
    unsigned char rp_reserve;        /* resource pool reserve active statement flag */
    unsigned char reserve;           /* reserve active statement flag */
    unsigned char release;           /* release active statement flag */
    unsigned char global_release;    /* global release active statement */
    unsigned char rp_release;        /* resource pool release active statement */
    unsigned char except;            /* a flag to handle exception while the query executing */
    unsigned char special;           /* check the query whether is a special query */
    unsigned char transact;          /* check the query whether is in a transaction block*/
    unsigned char transact_begin;    /* check the query if "begin transaction" */
    unsigned char simple;            /* check the query whether is a simple query */
    unsigned char iocomplex;         /* check the query whether is a IO simple query */
    unsigned char enqueue;           /* check the query whether do global parallel control */
    unsigned char errjmp;            /* this is error jump point */
    unsigned char global_waiting;    /* waiting in the global list */
    unsigned char respool_waiting;   /* waiting in the respool list */
    unsigned char preglobal_waiting; /* waiting in simple global list */
    unsigned char central_waiting;   /* waiting in central_waiting */
    unsigned char attach;            /* attach cgroup */
    unsigned char subquery;          /* check the query whether is in a stored procedure */

} ParctlState;

typedef struct knl_u_relmap_context {
    /*
     * The currently known contents of the shared map file and our database's
     * local map file are stored here.    These can be reloaded from disk
     * immediately whenever we receive an update sinval message.
     */
    struct RelMapFile* shared_map;
    struct RelMapFile* local_map;

    /*
     * We use the same RelMapFile data structure to track uncommitted local
     * changes in the mappings (but note the magic and crc fields are not made
     * valid in these variables).  Currently, map updates are not allowed within
     * subtransactions, so one set of transaction-level changes is sufficient.
     *
     * The active_xxx variables contain updates that are valid in our transaction
     * and should be honored by RelationMapOidToFilenode.  The pending_xxx
     * variables contain updates we have been told about that aren't active yet;
     * they will become active at the next CommandCounterIncrement.  This setup
     * lets map updates act similarly to updates of pg_class rows, ie, they
     * become visible only at the next CommandCounterIncrement boundary.
     */
    struct RelMapFile* active_shared_updates;
    struct RelMapFile* active_local_updates;
    struct RelMapFile* pending_shared_updates;
    struct RelMapFile* pending_local_updates;

    struct ScanKeyData relfilenodeSkey[2];
    /* used for ustore table. */
    struct ScanKeyData uHeapRelfilenodeSkey[2];
    /* Hash table for informations about each relfilenode <-> oid pair */
    struct HTAB* RelfilenodeMapHash;
    struct HTAB* UHeapRelfilenodeMapHash;
} knl_u_relmap_context;

typedef struct knl_u_unique_sql_context {
    /* each sql should have one unique query id
     * limitation: Query* is needed by calculating unique query id,
     * and Query * is generated after parsing and
     * rewriting SQL, so unique query id only can be used
     * after SQL rewrite
     */
    uint64 unique_sql_id;
    Oid unique_sql_user_id;
    uint32 unique_sql_cn_id;

    /*
     * storing unique sql start time,
     * in PortalRun method, we will update unique sql elapse time,
     * use it to store the start time.
     *
     * Note: as in exec_simple_query, we can get multi parsetree,
     * and will generate multi the unique sql id, so we set
     * unique_sql_start_time at each LOOP(parsetree) started
     *
     * exec_simple_query
     *
     *  pg_parse_query(parsetree List)
     *
     *  LOOP start
     *  ****-> set unique_sql_start_time
     *  pg_analyze_and_rewrite
     *      analyze
     *      rewrite
     *  pg_plan_queries
     *  PortalStart
     *  PortalRun
     *      ****-> UpdateUniqueSQLStat/UpdateUniqueSQLElapseTime
     *  PortalDrop
     *
     *  LOOP end
     */
    int64 unique_sql_start_time;

    /* unique sql's returned rows counter, only updated on CN */
    uint64 unique_sql_returned_rows_counter;

    /* parse counter, both CN and DN can update the counter  */
    uint64 unique_sql_soft_parse;
    uint64 unique_sql_hard_parse;

    /*
     * last_stat_counter - store pgStatTabList's total counter values when
     *          exit from pgstat_report_stat last time
     * current_table_counter - store current unique sql id's row activity stat
     */
    struct PgStat_TableCounts* last_stat_counter;
    struct PgStat_TableCounts* current_table_counter;

    /*
     * handle multi SQLs case in exec_simple_query function,
     * for below case:
     *   - we send sqls "select * from xx; update xx;" using libpq
     *     PQexec method(refer to gs_ctl tool)
     *   - then exec_simple_query will get two SQLs
     *   - pg_parse_query will generate parsetree list with two nodes
     *   - then run twice:
     *      # call unique_sql_post_parse_analyze(generate unique sql id/
     *        normalized sql string)
     *        -- here we get the sql using debug_query_string or ParseState.p_sourcetext
     *        -- but the two way to get sql string will be two SQLs, not single one
     *        -- so we need add below two variables to handle this case
     *      # call PortalDefineQuery
     *      # call PortalRun -> update unique sql stat
     */
    bool is_multi_unique_sql;
    /* for each sql in multi queries, remember the offset location */
    int32 multi_sql_offset;
    char* curr_single_unique_sql;

    /*
     * for case unique sql track type is 'UNIQUE_SQL_TRACK_TOP'
     * main logic(only happened on CN):
     * - postgresmain
     *   >> is_top_unique_sql = false
     *
     * - sql parse -> generate unique sql id(top SQL)
     *   >> is_top_unique_sql = true
     *
     * - exec_simple_query
     *   or exec_bind_message
     *   >> PortalRun -> using TOP SQL's unique sql id(will sned to DN)
     *
     * - is_top_unique_sql to false
     */
    bool is_top_unique_sql;
    bool need_update_calls;

    /*
     * For example, the execute direct on statement enters the unique SQL hook
     * for multiple times in the parse_analyze function.
     * The sub-statements of the statement do not need to invoke the hook.
     * Instead, the complete statement needs to generate the unique SQL ID.
     */
    uint64 skipUniqueSQLCount;

    /* sort and hash instrment states */
    struct unique_sql_sorthash_instr* unique_sql_sort_instr;
    struct unique_sql_sorthash_instr* unique_sql_hash_instr;

    /* handle nested portal calling */
    uint32 portal_nesting_level;
#ifndef ENABLE_MULTIPLE_NODES
    char* unique_sql_text;
#endif
} knl_u_unique_sql_context;

typedef struct unique_sql_sorthash_instr {
    bool has_sorthash;       /* true if query contains sort/hash operation */
    uint64 counts;           /* # of operation during unique sql */
    int64 used_work_mem;         /* space of used work mem by kbs */
    TimestampTz enter_time;  /* time stamp at the start point */
    TimestampTz total_time;  /* execution time */
    uint64 spill_counts;     /* # of spill times during the operation */
    uint64 spill_size;       /* spill size for temp table by kbs */
} unique_sql_sorthash_instr;

typedef struct knl_u_percentile_context {
    struct SqlRTInfo* LocalsqlRT;
    int LocalCounter;
} knl_u_percentile_context;

#define STATEMENT_SQL_KIND 2
#define INSTR_STMT_NULL_PORT (-2)
typedef struct knl_u_statement_context {
    int statement_level[STATEMENT_SQL_KIND]; /* diff levels for full or slow SQL */

    /* basic information, should not be changed during one session  */
    char* db_name;          /* which database */
    char* user_name;        /* client's user name */
    char* client_addr;      /* client's IP address */
    int client_port;        /* client's port */
    uint64 session_id;     /* session's identifier */


    void *curStatementMetrics;      /* current Statement handler to record metrics */
    int allocatedCxtCnt;            /* how many of handers allocated */
    void *toFreeStatementList;       /* handers to be freed. */
    int free_count;                 /* length of toFreeStatementList */
    void *suspendStatementList;      /* handers to be flushed into system table */
    int suspend_count;              /* length of suspendStatementList */
    syscalllock list_protect;       /* concurrency control for above two lists */
    MemoryContext stmt_stat_cxt;    /* statement stat context */
    int executer_run_level;
} knl_u_statement_context;

struct Qid_key {
    Oid procId;        /* cn id for the statement */
    uint64 queryId;    /* query id for statement, it's a session id */
    TimestampTz stamp; /* query time stamp */
};

struct SlowQueryInfo {
    uint64 unique_sql_id;
    uint64 debug_query_sql_id;
    int64* localTimeInfoArray;
    struct PgStat_TableCounts* current_table_counter;
    uint64 n_returned_rows;
};


typedef struct knl_u_slow_query_context {
    SlowQueryInfo slow_query;
} knl_u_slow_query_context;

typedef struct knl_u_ledger_context {
    char *resp_tag;
} knl_u_ledger_context;

typedef struct knl_u_user_login_context {
    /*
     * when user is login, will update the variable in PerformAuthentication,
     * and update the user's login counter.
     *
     * when proc exit, we register the callback function, in the callback
     * function, will update the logout counter, and reset CurrentInstrLoginUserOid.
     */
    Oid CurrentInstrLoginUserOid;
} knl_u_user_login_context;

#define MAXINVALMSGS 32
typedef struct knl_u_inval_context {
    int32 DeepthInAcceptInvalidationMessage;

    struct TransInvalidationInfo* transInvalInfo;

    union SharedInvalidationMessage* SharedInvalidMessagesArray;

    int numSharedInvalidMessagesArray;

    int maxSharedInvalidMessagesArray;

    struct SYSCACHECALLBACK* syscache_callback_list;

    int syscache_callback_count;

    struct RELCACHECALLBACK* relcache_callback_list;

    int relcache_callback_count;

    struct PARTCACHECALLBACK* partcache_callback_list;

    int partcache_callback_count;

    uint64 SIMCounter; /* SharedInvalidMessageCounter, there are two counter, both on sess and thrd, 
        u_sess->inval_cxt.SIMCounter;
        if (EnableLocalSysCache()) {t_thrd.lsc_cxt.lsc->inval_cxt.SIMCounter;} */

    volatile sig_atomic_t catchupInterruptPending;

    union SharedInvalidationMessage* messages;

    volatile int nextmsg;

    volatile int nummsgs;
} knl_u_inval_context;

typedef struct knl_u_cache_context {
    /* num of cached re's(regular expression) */
    int num_res;

    /* cached re's (regular expression) */
    struct cached_re_str* re_array;

    /*
     * We frequently need to test whether a given role is a member of some other
     * role.  In most of these tests the "given role" is the same, namely the
     * active current user.  So we can optimize it by keeping a cached list of
     * all the roles the "given role" is a member of, directly or indirectly.
     * The cache is flushed whenever we detect a change in pg_auth_members.
     *
     * There are actually two caches, one computed under "has_privs" rules
     * (do not recurse where rolinherit isn't true) and one computed under
     * "is_member" rules (recurse regardless of rolinherit).
     *
     * Possibly this mechanism should be generalized to allow caching membership
     * info for multiple roles?
     *
     * The has_privs cache is:
     * cached_privs_role is the role OID the cache is for.
     * cached_privs_roles is an OID list of roles that cached_privs_role
     *        has the privileges of (always including itself).
     * The cache is valid if cached_privs_role is not InvalidOid.
     *
     * The is_member cache is similarly:
     * cached_member_role is the role OID the cache is for.
     * cached_membership_roles is an OID list of roles that cached_member_role
     *        is a member of (always including itself).
     * The cache is valid if cached_member_role is not InvalidOid.
     */
    Oid cached_privs_role;

    List* cached_privs_roles;

    Oid cached_member_role;

    List* cached_membership_roles;

    struct _SPI_plan* plan_getrulebyoid;

    struct _SPI_plan* plan_getviewrule;

    /* Hash table for informations about each attribute's options */
    struct HTAB* att_opt_cache_hash;

    /* Cache management header --- pointer is NULL until created */
    struct CatCacheHeader* cache_header;

    /* Hash table for information about each tablespace */
    struct HTAB* TableSpaceCacheHash;

    struct HTAB* PartitionIdCache;

    struct HTAB* BucketIdCache;

    struct HTAB* dn_hash_table;

    bool PartCacheNeedEOXActWork;

    bool bucket_cache_need_eoxact_work; 

} knl_u_cache_context;


typedef struct knl_u_syscache_context {
    MemoryContext SysCacheMemCxt;

    struct CatCache** SysCache;

    bool CacheInitialized;

    Oid* SysCacheRelationOid;

} knl_u_syscache_context;

namespace dfs {
class DFSConnector;
}
typedef struct knl_u_catalog_context {
    bool nulls[4];
    struct PartitionIdentifier* route;

    /*
     * If "Create function ... LANGUAGE SQL" include agg function, agg->aggtype
     * is the final aggtype. While for "Select agg()", agg->aggtype should be agg->aggtrantype.
     * Here we use Parse_sql_language to distinguish these two cases.
     */
    bool Parse_sql_language;
    struct PendingRelDelete* pendingDeletes; /* head of linked list */
    /* Handle deleting BCM files.
     *
     * For column relation, one bcm file for each column file.
     * For row relation, only one bcm file for the whole relation.
     * We have to handle BCM files of some column file during rollback.
     * Take an example, ADD COLUMN or SET TABLESPACE, may be canceled by user in some cases.
     * Shared buffer must be invalided before BCM files are deleted.
     * See also CStoreCopyColumnDataEnd(), RelationDropStorage(), ect.
     */
    struct RelFileNodeBackend* ColMainFileNodes;
    int ColMainFileNodesMaxNum;
    int ColMainFileNodesCurNum;
    List* pendingDfsDeletes;
    dfs::DFSConnector* delete_conn;
    struct StringInfoData* vf_store_root;
    Oid currentlyReindexedHeap;
    Oid currentlyReindexedIndex;
    List* pendingReindexedIndexes;
    /* These variables define the actually active state: */
    List* activeSearchPath;
    /* default place to create stuff; if InvalidOid, no default */
    Oid activeCreationNamespace;
    /* if TRUE, activeCreationNamespace is wrong, it should be temp namespace */
    bool activeTempCreationPending;
    /* These variables are the values last derived from namespace_search_path: */
    List* baseSearchPath;
    Oid baseCreationNamespace;
    bool baseTempCreationPending;
    Oid namespaceUser;
    /* The above four values are valid only if baseSearchPathValid */
    bool baseSearchPathValid;
    List* overrideStack;
    bool overrideStackValid;
    Oid myTempNamespaceOld;
    /* The above two values are used for create command */
    bool setCurCreateSchema;
    char* curCreateSchema;
    /*
     * myTempNamespace is InvalidOid until and unless a TEMP namespace is set up
     * in a particular backend session (this happens when a CREATE TEMP TABLE
     * command is first executed).    Thereafter it's the OID of the temp namespace.
     *
     * myTempToastNamespace is the OID of the namespace for my temp tables' toast
     * tables.    It is set when myTempNamespace is, and is InvalidOid before that.
     *
     * myTempNamespaceSubID shows whether we've created the TEMP namespace in the
     * current subtransaction.    The flag propagates up the subtransaction tree,
     * so the main transaction will correctly recognize the flag if all
     * intermediate subtransactions commit.  When it is InvalidSubTransactionId,
     * we either haven't made the TEMP namespace yet, or have successfully
     * committed its creation, depending on whether myTempNamespace is valid.
     */
    Oid myTempNamespace;
    Oid myTempToastNamespace;
    bool deleteTempOnQuiting;
    SubTransactionId myTempNamespaceSubID;
    /* stuff for online expansion redis-cancel */
    bool redistribution_cancelable;
} knl_u_catalog_context;

/* Maximum number of preferred Datanodes that can be defined in cluster */
#define MAX_PREFERRED_NODES 64

typedef struct knl_u_pgxc_context {
    /* Current size of dn_handles and co_handles */
    int NumDataNodes;
    int NumCoords;
    int NumTotalDataNodes; /* includes all DN in disaster cluster*/
    int NumStandbyDataNodes;

    /* Number of connections held */
    int datanode_count;
    int coord_count;

    /* dn oid matrics for multiple standby deployment */
    Oid** dn_matrics;
    int dn_num;
    int standby_num;
    Oid primary_data_node;
    int num_preferred_data_nodes;
    Oid preferred_data_node[MAX_PREFERRED_NODES];
    int* disasterReadArray; /* array to save dn node index for disaster read */
    bool DisasterReadArrayInit;

    /*
     * Datanode handles saved in session memory context
     * when PostgresMain is launched.
     * Those handles are used inside a transaction by Coordinator to Datanodes.
     */
    struct pgxc_node_handle* dn_handles;
    /*
     * Coordinator handles saved in session memory context
     * when PostgresMain is launched.
     * Those handles are used inside a transaction by Coordinator to Coordinators
     */
    struct pgxc_node_handle* co_handles;

    uint32 num_skipnodes;
    void* skipnodes;

    struct RemoteXactState* remoteXactState;

    int PGXCNodeId;
    /*
     * When a particular node starts up, store the node identifier in this variable
     * so that we dont have to calculate it OR do a search in cache any where else
     * This will have minimal impact on performance
     */
    uint32 PGXCNodeIdentifier;

    /*
     * List of PGXCNodeHandle to track readers and writers involved in the
     * current transaction
     */
    List* XactWriteNodes;
    List* XactReadNodes;
    char* preparedNodes;

    /* Pool */
    char sock_path[MAXPGPATH];
    int last_reported_send_errno;
    bool PoolerResendParams;
    struct PGXCNodeConnectionInfo* PoolerConnectionInfo;
    struct PoolAgent* poolHandle;
    bool ConsistencyPointUpdating;

    List* connection_cache;
    List* connection_cache_handle;

    bool is_gc_fdw;
    bool is_gc_fdw_analyze;
    int gc_fdw_current_idx;
    int gc_fdw_max_idx;
    int gc_fdw_run_version;
    struct SnapshotData* gc_fdw_snapshot;
} knl_u_pgxc_context;

typedef struct knl_u_fmgr_context {
    struct df_files_init* file_list_init;

    struct df_files_init* file_init_tail;

} knl_u_fmgr_context;

typedef struct knl_u_erand_context {
    unsigned short rand48_seed[3];
} knl_u_erand_context;

typedef struct knl_u_regex_context {
    int pg_regex_strategy; /* enum PG_Locale_Strategy */
    Oid pg_regex_collation;
    struct pg_ctype_cache* pg_ctype_cache_list;
} knl_u_regex_context;

#ifdef ENABLE_MOT
namespace MOT {
  class SessionContext;
  class TxnManager;
}

namespace JitExec {
    struct JitContext;
    struct JitContextPool;
}

namespace tvm {
    class JitIf;
    class JitWhile;
    class JitDoWhile;
}

namespace llvm {
    class JitIf;
    class JitWhile;
    class JitDoWhile;
}

typedef struct knl_u_mot_context {
    bool callbacks_set;

    // session attributes
    uint32_t session_id;
    uint32_t connection_id;
    MOT::SessionContext* session_context;
    MOT::TxnManager* txn_manager;

    // JIT
    JitExec::JitContextPool* jit_session_context_pool;
    uint32_t jit_context_count;
    llvm::JitIf* jit_llvm_if_stack;
    llvm::JitWhile* jit_llvm_while_stack;
    llvm::JitDoWhile* jit_llvm_do_while_stack;
    tvm::JitIf* jit_tvm_if_stack;
    tvm::JitWhile* jit_tvm_while_stack;
    tvm::JitDoWhile* jit_tvm_do_while_stack;
    JitExec::JitContext* jit_context;
    MOT::TxnManager* jit_txn;
} knl_u_mot_context;
#endif

typedef struct knl_u_gtt_context {
    bool gtt_cleaner_exit_registered;
    HTAB* gtt_storage_local_hash;
    MemoryContext gtt_relstats_context;

    /* relfrozenxid of all gtts in the current session */
    List* gtt_session_relfrozenxid_list;
    TransactionId gtt_session_frozenxid;
    pg_on_exit_callback gtt_sess_exit;
} knl_u_gtt_context;

typedef struct knl_u_streaming_context {
    bool gather_session;
    bool streaming_ddl_session;
} knl_u_streaming_context;

enum knl_pl_body_type {
    PL_BODY_FUNCTION,
    PL_BODY_PKG_SPEC,
    PL_BODY_PKG_BODY
};

enum knl_ext_fdw_type {
    MYSQL_TYPE_FDW,
    ORACLE_TYPE_FDW,
    POSTGRES_TYPE_FDW,
    /* Add new external FDW type before MAX_TYPE_FDW */
    PLDEBUG_TYPE,
    MAX_TYPE_FDW
};

typedef struct knl_u_ext_fdw_context {
    union {
        void* connList;                     /* Connection info to other DB */
        void* pldbg_ctx;                    /* Pldebugger info */
    };
    pg_on_exit_callback fdwExitFunc;    /* Exit callback, will be called when session exit */
} knl_u_ext_fdw_context;

enum knl_session_status {
    KNL_SESS_FAKE,
    KNL_SESS_UNINIT,
    KNL_SESS_ATTACH,
    KNL_SESS_DETACH,
    KNL_SESS_CLOSE,
    KNL_SESS_END_PHASE1,
    KNL_SESS_CLOSERAW,  // not initialize and
};

typedef struct sess_orient{
	uint64 cn_sessid;
	uint32 cn_timeline;
	uint32 cn_nodeid;
}sess_orient;

struct SessionInfo;

#define MAX_TRACE_ID_SIZE 33
typedef struct knl_u_trace_context {
    char trace_id[MAX_TRACE_ID_SIZE];
} knl_u_trace_context;

struct ReplicationState;
struct ReplicationStateShmStruct;
typedef struct knl_u_rep_origin_context {
    RepOriginId originId;
    XLogRecPtr originLsn;
    TimestampTz originTs;
    bool registeredCleanup;
    /*
     * Backend-local, cached element from ReplicationStates for use in a backend
     * replaying remote commits, so we don't have to search ReplicationStates for
     * the backends current RepOriginId.
     */
    ReplicationState *curRepState;

    ReplicationStateShmStruct *repStatesShm;
} knl_u_rep_origin_context;

/* Record start time and end time in session initialize for client connection */
typedef struct knl_u_clientConnTime_context {
    instr_time connStartTime;
    instr_time connEndTime;

    /* Flag to indicate that this session is in initial process(client connect) or not */
    bool checkOnlyInConnProcess;
} knl_u_clientConnTime_context;

typedef struct knl_u_hook_context {
    void *analyzerRoutineHook;
    void *transformStmtHook;
} knl_u_hook_context;

typedef struct knl_session_context {
    volatile knl_session_status status;
    /* used for threadworker, elem in m_readySessionList */
    Dlelem elem;
     /* used for threadworker && gsc, elems in m_session_bucket
      * this variable is used for syscache hit */
    Dlelem elem2;

    ThreadId attachPid;

    MemoryContext top_mem_cxt;
    MemoryContext cache_mem_cxt;
    MemoryContext top_transaction_mem_cxt;
    MemoryContext dbesql_mem_cxt;
    MemoryContext self_mem_cxt;
    MemoryContext top_portal_cxt;
    MemoryContext probackup_context;
    MemoryContextGroup* mcxt_group;
    /* temp_mem_cxt is a context which will be reset when the session attach to a thread */
    MemoryContext temp_mem_cxt;
    int session_ctr_index;
    uint64 session_id;
    GlobalSessionId globalSessionId;
    knl_u_trace_context trace_cxt;
    uint64 debug_query_id;
    List* ts_cached_queryid;

    //use this to identify which cn connection to.
    sess_orient sess_ident;
    uint32 cn_session_abort_count;
    long cancel_key;
    char* prog_name;

    bool ClientAuthInProgress;

    bool need_report_top_xid;

    bool is_autonomous_session;

    uint64 autonomous_parent_sessionid;

    struct config_generic** guc_variables;

    int num_guc_variables;

    int on_sess_exit_index;
    
    List* plsqlErrorList;
    knl_session_attr attr;
    struct knl_u_wlm_context* wlm_cxt;
    knl_u_analyze_context analyze_cxt;
    knl_u_cache_context cache_cxt;
    knl_u_catalog_context catalog_cxt;
    knl_u_commands_context cmd_cxt;
    knl_u_contrib_context contrib_cxt;
    knl_u_erand_context rand_cxt;
    knl_u_executor_context exec_cxt;
    knl_u_fmgr_context fmgr_cxt;
    knl_u_index_context index_cxt;
    knl_u_instrument_context instr_cxt;
    knl_u_inval_context inval_cxt;
    knl_u_locale_context lc_cxt;
    knl_u_log_context log_cxt;
    knl_u_libpq_context libpq_cxt;
    knl_u_mb_context mb_cxt;
    knl_u_misc_context misc_cxt;
    knl_u_optimizer_context opt_cxt;
    knl_u_parser_context parser_cxt;
    knl_u_pgxc_context pgxc_cxt;
    knl_u_plancache_context pcache_cxt;
    knl_u_plpgsql_context plsql_cxt;
    knl_u_postgres_context postgres_cxt;
    knl_u_proc_context proc_cxt;
    knl_u_ps_context ps_cxt;
    knl_u_regex_context regex_cxt;
    knl_u_xact_context xact_cxt;
    knl_u_sig_context sig_cxt;
    knl_u_SPI_context SPI_cxt;
    knl_u_relcache_context relcache_cxt;
    knl_u_relmap_context relmap_cxt;
    knl_u_stat_context stat_cxt;
    knl_u_storage_context storage_cxt;
    knl_u_stream_context stream_cxt;
    knl_u_syscache_context syscache_cxt;
    knl_u_time_context time_cxt;
    knl_u_trigger_context tri_cxt;
    knl_u_tscache_context tscache_cxt;
    knl_u_typecache_context tycache_cxt;
    knl_u_upgrade_context upg_cxt;
    knl_u_utils_context utils_cxt;
    knl_u_security_context sec_cxt;
    knl_u_advisor_context adv_cxt;

#ifdef ENABLE_MOT
    knl_u_mot_context mot_cxt;
#endif

    knl_u_ustore_context ustore_cxt;
    knl_u_undo_context undo_cxt;

    /* instrumentation */
    knl_u_unique_sql_context unique_sql_cxt;
    knl_u_user_login_context user_login_cxt;
    knl_u_percentile_context percentile_cxt;
    knl_u_statement_context statement_cxt;

    knl_u_slow_query_context slow_query_cxt;
    knl_u_ledger_context ledger_cxt;
    /* external FDW */
    knl_u_ext_fdw_context ext_fdw_ctx[MAX_TYPE_FDW];
    /* GTT */
    knl_u_gtt_context gtt_ctx;
    /* extension streaming */
    knl_u_streaming_context streaming_cxt;

    /* comm_cn_dn_logic_conn */
    SessionInfo *session_info_ptr;

    instr_time last_access_time;

    knl_u_rep_origin_context reporigin_cxt;

    /*
     * Initialize context which records time for client connection establish.
     * This time records start on incommining resuest arrives e.g. poll() invoked to accept() and
     * end on returning message by server side clientfd.
     */
    struct knl_u_clientConnTime_context clientConnTime_cxt;

    knl_u_hook_context hook_cxt;
} knl_session_context;

enum stp_xact_err_type {
    STP_XACT_OPEN_FOR,
    STP_XACT_USED_AS_EXPR,
    STP_XACT_GUC_IN_OPT_CLAUSE,
    STP_XACT_OF_SECURE_DEFINER,
    STP_XACT_AFTER_TRIGGER_BEGIN,
    STP_XACT_PACKAGE_INSTANTIATION,
    STP_XACT_IN_TRIGGER,
    STP_XACT_IMMUTABLE,
    STP_XACT_COMPL_SQL,
    STP_XACT_TOO_MANY_PORTAL
};

extern void knl_u_inval_init(knl_u_inval_context* inval_cxt);
extern void knl_u_relmap_init(knl_u_relmap_context* relmap_cxt);
extern void knl_session_init(knl_session_context* sess_cxt);
extern void knl_u_executor_init(knl_u_executor_context* exec_cxt);
extern knl_session_context* create_session_context(MemoryContext parent, uint64 id);
extern void free_session_context(knl_session_context* session);
extern void use_fake_session();
extern bool stp_set_commit_rollback_err_msg(stp_xact_err_type type);
extern bool enable_out_param_override();

extern THR_LOCAL knl_session_context* u_sess;

inline bool stp_disable_xact_and_set_err_msg(bool *save_commit_rollback_state, stp_xact_err_type type) 
{
    *save_commit_rollback_state = u_sess->SPI_cxt.is_allow_commit_rollback;
    u_sess->SPI_cxt.is_allow_commit_rollback = false;
    return stp_set_commit_rollback_err_msg(type);
}

inline bool stp_enable_and_get_old_xact_stmt_state() 
{
    bool save_commit_rollback_state = u_sess->SPI_cxt.is_allow_commit_rollback;
    u_sess->SPI_cxt.is_allow_commit_rollback = true;
    return save_commit_rollback_state;
}

inline void stp_retore_old_xact_stmt_state(bool OldState) 
{
    u_sess->SPI_cxt.is_allow_commit_rollback = OldState;
}

inline void stp_reset_commit_rolback_err_msg()
{
    u_sess->SPI_cxt.forbidden_commit_rollback_err_msg[0] = '\0';
}

inline void stp_reset_opt_values()
{
    u_sess->SPI_cxt.is_allow_commit_rollback = false;
    stp_reset_commit_rolback_err_msg();
    u_sess->SPI_cxt.is_stp = true;
    u_sess->SPI_cxt.is_proconfig_set = false;
    u_sess->SPI_cxt.portal_stp_exception_counter = 0;
    u_sess->plsql_cxt.stp_savepoint_cnt = 0;
}

inline void stp_reset_xact_state_and_err_msg(bool savedisAllowCommitRollback, bool needResetErrMsg)
{
    stp_retore_old_xact_stmt_state(savedisAllowCommitRollback);
    if (needResetErrMsg) {
        stp_reset_commit_rolback_err_msg();
    }
}

#endif /* SRC_INCLUDE_KNL_KNL_SESSION_H_ */

