/* --------------------------------------------------------------------
 * guc.h
 *
 * External declarations pertaining to backend/utils/misc/guc.c and
 * backend/utils/misc/guc-file.l
 *
 * Copyright (c) 2000-2012, PostgreSQL Global Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * src/include/utils/guc.h
 * --------------------------------------------------------------------
 */
#ifndef GUC_H
#define GUC_H

#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/array.h"

/* upper limit for GUC variables measured in kilobytes of memory */
/* note that various places assume the byte size fits in a "long" variable */
#if SIZEOF_SIZE_T > 4 && SIZEOF_LONG > 4
#define MAX_KILOBYTES INT_MAX
#else
#define MAX_KILOBYTES (INT_MAX / 1024)
#endif

/*
 * Certain options can only be set at certain times. The rules are
 * like this:
 *
 * INTERNAL options cannot be set by the user at all, but only through
 * internal processes ("server_version" is an example).  These are GUC
 * variables only so they can be shown by SHOW, etc.
 *
 * POSTMASTER options can only be set when the postmaster starts,
 * either from the configuration file or the command line.
 *
 * SIGHUP options can only be set at postmaster startup or by changing
 * the configuration file and sending the HUP signal to the postmaster
 * or a backend process. (Notice that the signal receipt will not be
 * evaluated immediately. The postmaster and the backend check it at a
 * certain point in their main loop. It's safer to wait than to read a
 * file asynchronously.)
 *
 * BACKEND options can only be set at postmaster startup, from the
 * configuration file, or by client request in the connection startup
 * packet (e.g., from libpq's PGOPTIONS variable).  Furthermore, an
 * already-started backend will ignore changes to such an option in the
 * configuration file.	The idea is that these options are fixed for a
 * given backend once it's started, but they can vary across backends.
 *
 * SUSET options can be set at postmaster startup, with the SIGHUP
 * mechanism, or from SQL if you're a superuser.
 *
 * USERSET options can be set by anyone any time.
 */
typedef enum { PGC_INTERNAL, PGC_POSTMASTER, PGC_SIGHUP, PGC_BACKEND, PGC_SUSET, PGC_USERSET } GucContext;

/*
 * The following type represents the application environment of the
 * parameter.
 * 
 * NODE_ALL: general parameters, suitable for centralized and distributed
 * scenarios.
 * NODE_SINGLENODE: centralized parameters, suitable for centralized scenarios.
 * NODE_DISTRIBUTE: distributed parameters, suitable for distributed scenarios.
 */
typedef enum { NODE_ALL, NODE_SINGLENODE, NODE_DISTRIBUTE } GucNodeType;

/*
 * The following type records the source of the current setting.  A
 * new setting can only take effect if the previous setting had the
 * same or lower level.  (E.g, changing the config file doesn't
 * override the postmaster command line.)  Tracking the source allows us
 * to process sources in any convenient order without affecting results.
 * Sources <= PGC_S_OVERRIDE will set the default used by RESET, as well
 * as the current value.  Note that source == PGC_S_OVERRIDE should be
 * used when setting a PGC_INTERNAL option.
 *
 * PGC_S_INTERACTIVE isn't actually a source value, but is the
 * dividing line between "interactive" and "non-interactive" sources for
 * error reporting purposes.
 *
 * PGC_S_TEST is used when testing values to be stored as per-database or
 * per-user defaults ("doit" will always be false, so this never gets stored
 * as the actual source of any value).	This is an interactive case, but
 * it needs its own source value because some assign hooks need to make
 * different validity checks in this case.
 *
 * NB: see GucSource_Names in guc.c if you change this.
 */
typedef enum {
    PGC_S_DEFAULT,         /* hard-wired default ("boot_val") */
    PGC_S_DYNAMIC_DEFAULT, /* default computed during initialization */
    PGC_S_ENV_VAR,         /* postmaster environment variable */
    PGC_S_FILE,            /* postgresql.conf */
    PGC_S_ARGV,            /* postmaster command line */
    PGC_S_DATABASE,        /* per-database setting */
    PGC_S_USER,            /* per-user setting */
    PGC_S_DATABASE_USER,   /* per-user-and-database setting */
    PGC_S_CLIENT,          /* from client connection request */
    PGC_S_OVERRIDE,        /* special case to forcibly set default */
    PGC_S_INTERACTIVE,     /* dividing line for error reporting */
    PGC_S_TEST,            /* test per-database or per-user setting */
    PGC_S_SESSION          /* SET command */
} GucSource;

typedef enum {
    AES_128_CBC,
    AES_192_CBC,
    AES_256_CBC,
    AES_128_CFB1,
    AES_192_CFB1,
    AES_256_CFB1,
    AES_128_CFB8,
    AES_192_CFB8,
    AES_256_CFB8,
    AES_128_CFB128,
    AES_192_CFB128,
    AES_256_CFB128,
    AES_128_OFB,
    AES_192_OFB,
    AES_256_OFB
} AesBlockMode;

/*
 * Parsing the configuration file will return a list of name-value pairs
 * with source location info.
 */
typedef struct ConfigVariable {
    char* name;
    char* value;
    char* filename;
    int sourceline;
    struct ConfigVariable* next;
} ConfigVariable;

extern bool ParseConfigFile(const char* config_file, const char* calling_file, bool strict, int depth, int elevel,
    ConfigVariable** head_p, ConfigVariable** tail_p);
extern bool ParseConfigFp(
    FILE* fp, const char* config_file, int depth, int elevel, ConfigVariable** head_p, ConfigVariable** tail_p);
extern void FreeConfigVariables(ConfigVariable* list);

/*
 * The possible values of an enum variable are specified by an array of
 * name-value pairs.  The "hidden" flag means the value is accepted but
 * won't be displayed when guc.c is asked for a list of acceptable values.
 */
struct config_enum_entry {
    const char* name;
    int val;
    bool hidden;
};

/*
 * Signatures for per-variable check/assign/show hook functions
 */
typedef bool (*GucBoolCheckHook)(bool* newval, void** extra, GucSource source);
typedef bool (*GucIntCheckHook)(int* newval, void** extra, GucSource source);
typedef bool (*GucInt64CheckHook)(int64* newval, void** extra, GucSource source);
typedef bool (*GucRealCheckHook)(double* newval, void** extra, GucSource source);
typedef bool (*GucStringCheckHook)(char** newval, void** extra, GucSource source);
typedef bool (*GucEnumCheckHook)(int* newval, void** extra, GucSource source);

typedef void (*GucBoolAssignHook)(bool newval, void* extra);
typedef void (*GucIntAssignHook)(int newval, void* extra);
typedef void (*GucInt64AssignHook)(int64 newval, void* extra);
typedef void (*GucRealAssignHook)(double newval, void* extra);
typedef void (*GucStringAssignHook)(const char* newval, void* extra);
typedef void (*GucEnumAssignHook)(int newval, void* extra);

typedef const char* (*GucShowHook)(void);

/*
 * Miscellaneous
 */
typedef enum {
    /* Types of set_config_option actions */
    GUC_ACTION_SET,   /* regular SET command */
    GUC_ACTION_LOCAL, /* SET LOCAL command */
    GUC_ACTION_SAVE   /* function SET option, or temp assignment */
} GucAction;

/* set params entry */
typedef struct {
    char name[NAMEDATALEN]; /* param name */
    char* query;            /* set query string */
} GucParamsEntry;

typedef struct {
    char name[NAMEDATALEN];   /* user-defined name */
    Const *value;
    bool isParse;
} GucUserParamsEntry;

#define GUC_QUALIFIER_SEPARATOR '.'

/*
 * bit values in "flags" of a GUC variable
 */
#define GUC_LIST_INPUT 0x0001         /* input can be list format */
#define GUC_LIST_QUOTE 0x0002         /* double-quote list elements */
#define GUC_NO_SHOW_ALL 0x0004        /* exclude from SHOW ALL */
#define GUC_NO_RESET_ALL 0x0008       /* exclude from RESET ALL */
#define GUC_REPORT 0x0010             /* auto-report changes to client */
#define GUC_NOT_IN_SAMPLE 0x0020      /* not in postgresql.conf.sample */
#define GUC_DISALLOW_IN_FILE 0x0040   /* can't set in postgresql.conf */
#define GUC_CUSTOM_PLACEHOLDER 0x0080 /* placeholder for custom variable */
#define GUC_SUPERUSER_ONLY 0x0100     /* show only to superusers */
#define GUC_IS_NAME 0x0200            /* limit string to NAMEDATALEN-1 */

#define GUC_UNIT_KB 0x0400      /* value is in kilobytes */
#define GUC_UNIT_BLOCKS 0x0800  /* value is in blocks */
#define GUC_UNIT_XBLOCKS 0x0C00 /* value is in xlog blocks */
#define GUC_UNIT_MEMORY 0x0C00  /* mask for KB, BLOCKS, XBLOCKS */

#define GUC_UNIT_MS 0x1000   /* value is in milliseconds */
#define GUC_UNIT_S 0x2000    /* value is in seconds */
#define GUC_UNIT_MIN 0x4000  /* value is in minutes */
#define GUC_UNIT_HOUR 0x5000 /* value is in hour */
#define GUC_UNIT_DAY 0x6000  /* value is in day */
#define GUC_UNIT_TIME 0x7000 /* mask for MS, S, MIN */

#define GUC_NOT_WHILE_SEC_REST 0x8000 /* can't set if security restricted */

#define MAX_BULK_IO_SIZE  64 /* bulk process IO size */

extern THR_LOCAL int log_min_messages;
extern THR_LOCAL bool force_backtrace_messages;
extern THR_LOCAL int client_min_messages;
extern THR_LOCAL int comm_ackchk_time;

#define SHOW_DEBUG_MESSAGE()  (SECUREC_UNLIKELY(log_min_messages <= DEBUG1))

/*
 * Functions exported by guc.c
 */
extern void SetConfigOption(const char* name, const char* value, GucContext context, GucSource source);

extern void DefineCustomBoolVariable(const char* name, const char* short_desc, const char* long_desc, bool* valueAddr,
    bool bootValue, GucContext context, int flags, GucBoolCheckHook check_hook, GucBoolAssignHook assign_hook,
    GucShowHook show_hook);

extern void DefineCustomIntVariable(const char* name, const char* short_desc, const char* long_desc, int* valueAddr,
    int bootValue, int minValue, int maxValue, GucContext context, int flags, GucIntCheckHook check_hook,
    GucIntAssignHook assign_hook, GucShowHook show_hook);

extern void DefineCustomInt64Variable(const char* name, const char* short_desc, const char* long_desc, int64* valueAddr,
    int64 bootValue, int64 minValue, int64 maxValue, GucContext context, int flags, GucInt64CheckHook check_hook,
    GucInt64AssignHook assign_hook, GucShowHook show_hook);

extern void DefineCustomRealVariable(const char* name, const char* short_desc, const char* long_desc, double* valueAddr,
    double bootValue, double minValue, double maxValue, GucContext context, int flags, GucRealCheckHook check_hook,
    GucRealAssignHook assign_hook, GucShowHook show_hook);

extern void DefineCustomStringVariable(const char* name, const char* short_desc, const char* long_desc,
    char** valueAddr, const char* bootValue, GucContext context, int flags, GucStringCheckHook check_hook,
    GucStringAssignHook assign_hook, GucShowHook show_hook);

extern void DefineCustomEnumVariable(const char* name, const char* short_desc, const char* long_desc, int* valueAddr,
    int bootValue, const struct config_enum_entry* options, GucContext context, int flags, GucEnumCheckHook check_hook,
    GucEnumAssignHook assign_hook, GucShowHook show_hook);

extern void EmitWarningsOnPlaceholders(const char* className);

extern const char* GetConfigOption(const char* name, bool missing_ok, bool restrict_superuser);
extern const char* GetConfigOptionResetString(const char* name);
extern void ProcessConfigFile(GucContext context);
extern void InitializeGUCOptions(void);
extern void InitializePostmasterGUC();
extern void init_sync_guc_variables(void);
extern void repair_guc_variables(void);
extern bool SelectConfigFiles(const char* userDoption, const char* progname);
extern void ResetAllOptions(void);
extern void AtStart_GUC(void);
extern int NewGUCNestLevel(void);
extern void AtEOXact_GUC(bool isCommit, int nestLevel);
extern char* GetGucName(const char *command, char *target_guc_name);
extern void BeginReportingGUCOptions(void);
extern void ParseLongOption(const char* string, char** name, char** value);
extern bool parse_int(const char* value, int* result, int flags, const char** hintmsg);
extern bool parse_int64(const char* value, int64* result, int flags, const char** hintmsg);
extern bool parse_real(const char* value, double* result, int flags = 0, const char** hintmsg = NULL);
double TimeUnitConvert(char** endptr, double value, int flags, const char** hintmsg);
double MemoryUnitConvert(char** endptr, double value, int flags, const char** hintmsg);
extern int set_config_option(const char* name, const char* value, GucContext context, GucSource source,
    GucAction action, bool changeVal, int elevel, bool isReload = false);
#ifndef ENABLE_MULTIPLE_NODES
extern void AlterSystemSetConfigFile(AlterSystemStmt* setstmt);
#endif
extern char* GetConfigOptionByName(const char* name, const char** varname);
extern void GetConfigOptionByNum(int varnum, const char** values, bool* noshow);
extern int GetNumConfigOptions(void);

extern void SetPGVariable(const char* name, List* args, bool is_local);
extern void GetPGVariable(const char* name, const char* likename, DestReceiver* dest);
extern void gramShowWarningsErrors(int offset, int count, DestReceiver *dest, bool isShowErrors);
extern void gramShowWarningsErrorsCount(DestReceiver *dest, bool isShowErrors);
extern void getDiagnosticsInfo(List* condInfo, bool hasCondNum, List* condNum);
extern TupleDesc GetPGVariableResultDesc(const char* name);
#ifdef PGXC
extern char* RewriteBeginQuery(char* query_string, const char* name, List* args);
#endif

extern void ExecSetVariableStmt(VariableSetStmt* stmt, ParamListInfo paramInfo);
extern char* ExtractSetVariableArgs(VariableSetStmt* stmt);

extern void ProcessGUCArray(ArrayType* array, GucContext context, GucSource source, GucAction action);
extern ArrayType* GUCArrayAdd(ArrayType* array, const char* name, const char* value);
extern ArrayType* GUCArrayDelete(ArrayType* array, const char* name);
extern ArrayType* GUCArrayReset(ArrayType* array);

#ifdef EXEC_BACKEND
extern void write_nondefault_variables(GucContext context);
extern void read_nondefault_variables(void);
#endif

extern void GUC_check_errcode(int sqlerrcode);

#define GUC_check_errmsg \
    pre_format_elog_string(errno, TEXTDOMAIN), u_sess->utils_cxt.GUC_check_errmsg_string = format_elog_string

#define GUC_check_errdetail \
    pre_format_elog_string(errno, TEXTDOMAIN), u_sess->utils_cxt.GUC_check_errdetail_string = format_elog_string

#define GUC_check_errhint \
    pre_format_elog_string(errno, TEXTDOMAIN), u_sess->utils_cxt.GUC_check_errhint_string = format_elog_string

#define guc_free(p)      \
    do {                 \
        if (p != NULL) { \
            free(p);     \
            p = NULL;    \
        }                \
    } while (0)

#define IsGlobalGuc(context)        \
    ((context) == PGC_INTERNAL ||   \
     (context) == PGC_POSTMASTER || \
     (context) == PGC_SIGHUP ||     \
     (context) == PGC_BACKEND) 


#define IsSessionGuc(context)        \
    ((context) == PGC_SUSET ||       \
     (context) == PGC_USERSET)

/*
 * The following functions are not in guc.c, but are declared here to avoid
 * having to include guc.h in some widely used headers that it really doesn't
 * belong in.
 */

/* in commands/tablespace.c */
extern bool check_default_tablespace(char** newval, void** extra, GucSource source);
extern bool check_temp_tablespaces(char** newval, void** extra, GucSource source);
extern void assign_temp_tablespaces(const char* newval, void* extra);

/* in catalog/namespace.c */
extern bool check_search_path(char** newval, void** extra, GucSource source);
extern void assign_search_path(const char* newval, void* extra);
extern bool check_percentile(char** newval, void** extra, GucSource source);
extern bool check_numa_distribute_mode(char** newval, void** extra, GucSource source);
extern bool check_asp_flush_mode(char** newval, void** extra, GucSource source);

/* in access/transam/xlog.c */
extern bool check_wal_buffers(int* newval, void** extra, GucSource source);
extern void assign_xlog_sync_method(int new_sync_method, void* extra);

/* in tcop/stmt_retry.cpp */
extern bool check_errcode_list(char** newval, void** extra, GucSource source);

/* in nvm.cpp */
extern bool check_nvm_path(char** newval, void** extra, GucSource source);

extern struct config_generic* find_option(const char* name, bool create_placeholders, int elevel);
/*
 * Error code for config file
 */
typedef enum {
    CODE_OK = 0,                /* success */
    CODE_UNKNOW_CONFFILE_PATH,  /* fail to get the specified config file */
    CODE_OPEN_CONFFILE_FAILED,  /* fail to open config file */
    CODE_CLOSE_CONFFILE_FAILED, /* fail to close config file */
    CODE_READE_CONFFILE_ERROR,  /* fail to read config file */
    CODE_WRITE_CONFFILE_ERROR,  /* fail to write config file */
    CODE_LOCK_CONFFILE_FAILED,
    CODE_UNLOCK_CONFFILE_FAILED,
    CODE_INTERNAL_ERROR
} ErrCode;

typedef enum {
    NO_REWRITE = 0, /* not allow lazy agg and magic set rewrite*/
    LAZY_AGG = (1 << 0),   /* allow lazy agg */
    MAGIC_SET = (1 << 1),   /* allow query qual push */
    PARTIAL_PUSH = (1 << 2), /* allow partial push */
    SUBLINK_PULLUP_WITH_UNIQUE_CHECK = (1 << 3), /* allow pull sublink with unqiue check */
    SUBLINK_PULLUP_DISABLE_REPLICATED = (1 << 4), /* disable pull up sublink with replicated table */
    SUBLINK_PULLUP_IN_TARGETLIST = (1 << 5), /* allow pull sublink in targetlist */
    PRED_PUSH = (1 << 6), /* push predicate into subquery block */
    PRED_PUSH_NORMAL = (1 << 7),
    PRED_PUSH_FORCE = (1 << 8),
    SUBLINK_PULLUP_DISABLE_EXPR = (1 << 9), /* disable pull sublink in expr clause */
    SUBLINK_PULLUP_ENHANCED = (1 << 10),
    REMOVE_REDUNDANT_DISTINCT_GROUP_BY = (1 << 11)
} rewrite_param;

typedef enum {
    NO_BETA_FEATURE = 0,
    SEL_SEMI_POISSON = (1 << 0), /* use poisson distribution model to calibrate semi join selectivity */
    SEL_EXPR_INSTR = (1 << 1), /* use pattern sel to calibrate instr() related base rel selectivity */
    PARAM_PATH_GEN = (1 << 2), /* Parametrized Path Generation */
    RAND_COST_OPT = (1 << 3),  /* Optimizing sc_random_page_cost */
    PARAM_PATH_OPT = (1 << 4), /* Parametrized Path Optimization. */
    PAGE_EST_OPT = (1 << 5),   /* More accurate (rowstored) index pages estimation */
    NO_UNIQUE_INDEX_FIRST = (1 << 6), /* use unique index first rule in path generation */
    JOIN_SEL_WITH_CAST_FUNC = (1 << 7), /* support cast function while calculating join selectivity */
    CANONICAL_PATHKEY = (1 << 8), /* Use canonicalize pathkeys directly */
    INDEX_COST_WITH_LEAF_PAGES_ONLY = (1 << 9), /* compute index cost with consideration of leaf-pages-only */
    PARTITION_OPFUSION = (1 << 10), /* Enable partition opfusion */
    A_STYLE_COERCE = (1 << 11),
    PLPGSQL_STREAM_FETCHALL = (1 << 12), /* fetch all tuple when has stream sql under plpgsql's for-loop */
    PREDPUSH_SAME_LEVEL = (1 << 13), /* predpush same level */
    PARTITION_FDW_ON = (1 << 14), /* support create foreign table on partitioned table */
    DISABLE_BITMAP_COST_WITH_LOSSY_PAGES = (1 << 15), /* stop computing bitmap path cost with lossy pages */
    EXTRACT_PUSHDOWN_OR_CLAUSE = (1 << 16)  /* Extract restriction OR clauses. */
} sql_beta_param;

typedef enum {
    OFF_VECTOR_ENGINE,
    FORCE_VECTOR_ENGINE,
    OPT_VECTOR_ENGINE
} TryVectorEngineStrategy;

typedef enum {
    SQL_IGNORE_NULL,
    SQL_OVERWRITE_NULL
}SqlIgnoreHandleStrategy;

typedef enum {
    BAYESNET_OPTION,
    MCV_OPTION,
    ALL_OPTION
} MultiStatsOptions;

#define ENABLE_PRED_PUSH(root) \
    ((PRED_PUSH & (uint)u_sess->attr.attr_sql.rewrite_rule) && permit_predpush(root))

#define ENABLE_PRED_PUSH_NORMAL(root) \
    ((PRED_PUSH_NORMAL & (uint)u_sess->attr.attr_sql.rewrite_rule) && permit_predpush(root))

#define ENABLE_PRED_PUSH_FORCE(root) \
    ((PRED_PUSH_FORCE & (uint)u_sess->attr.attr_sql.rewrite_rule) && permit_predpush(root))

#define DISABLE_SUBLINK_PULLUP_EXPR() \
    ((SUBLINK_PULLUP_DISABLE_EXPR) & (uint)u_sess->attr.attr_sql.rewrite_rule)

#define ENABLE_SUBLINK_PULLUP_ENHANCED() \
    ((SUBLINK_PULLUP_ENHANCED) & (uint)u_sess->attr.attr_sql.rewrite_rule)

#define ENABLE_PRED_PUSH_ALL(root) \
    ((ENABLE_PRED_PUSH(root) || ENABLE_PRED_PUSH_NORMAL(root) || ENABLE_PRED_PUSH_FORCE(root)) && permit_predpush(root))

#define ENABLE_REMOVE_REDUNDANT_DISTINCT_GROUP_BY() \
    ((REMOVE_REDUNDANT_DISTINCT_GROUP_BY) & (uint)u_sess->attr.attr_sql.rewrite_rule)

#define ENABLE_SQL_BETA_FEATURE(feature) \
    ((bool)((uint)u_sess->attr.attr_sql.sql_beta_feature & feature))

#define PARTITION_OPFUSION_MAX_NUMA_NODE 4
#define PARTITION_ENABLE_CACHE_OPFUSION             \
    (ENABLE_SQL_BETA_FEATURE(PARTITION_OPFUSION) && \
        g_instance.shmem_cxt.numaNodeNum <= PARTITION_OPFUSION_MAX_NUMA_NODE && \
        !g_instance.attr.attr_common.enable_global_syscache)

typedef enum {
    SUMMARY = 0, /* not collect multi column statistics info */
    DETAIL = 1,  /* collect multi column statistics info */
} resource_track_log_param;

typedef struct {
    FILE* fp;
    size_t size;
} ConfFileLock;

#define PG_LOCKFILE_SIZE 1024

#define CONFIG_BAK_FILENAME_WAL "postgresql.conf.wal"

extern void* pg_malloc(size_t size);
extern char* xstrdup(const char* s);

extern char** read_guc_file(const char* path);
extern ErrCode write_guc_file(const char* path, char** lines);
extern int find_guc_option(char** optlines, const char* opt_name,
    int* name_offset, int* name_len, int* value_offset, int* value_len, bool ignore_case);

extern void modify_guc_lines(char*** optlines, const char** opt_name, char** copy_from_line);
extern void modify_guc_one_line(char*** guc_optlines, const char* opt_name, const char* copy_from_line);
extern ErrCode copy_guc_lines(char** copy_to_line, char** optlines, const char** opt_name);
extern ErrCode copy_asyn_lines(char* path, char** copy_to_line, const char** opt_name);

extern ErrCode generate_temp_file(char* buf, char* temppath, size_t size);
extern ErrCode update_temp_file(char* tempfilepath, char** copy_to_line, const char** opt_name);
extern ErrCode get_file_lock(const char* path, ConfFileLock* filelock);
extern void release_file_lock(ConfFileLock* filelock);

extern void comment_guc_lines(char** optlines, const char** opt_name);
extern int add_guc_optlines_to_buffer(char** optlines, char** buffer);

/*Add for set command in transaction*/
extern char* get_set_string();
extern void reset_set_message(bool);
extern void append_set_message(const char* str);

extern void init_set_params_htab(void);
extern void init_set_user_params_htab(void);
extern void make_set_message(void);
extern int check_set_message_to_send(const VariableSetStmt* stmt, const char* queryString);
extern void check_set_user_message(const UserSetElem *elem);
extern void check_variable_value_info(const char* var_name, const Expr* var_expr);

#define TRANS_ENCRYPT_SAMPLE_RNDM "1234567890ABCDEF"
#define TRANS_ENCRYPT_SAMPLE_STRING "TRANS_ENCRY_SAMPLE_STRING"

/* For transparent encryption. For more information,
 * see the definition of this variable.
 */
extern THR_LOCAL char* transparent_encrypted_string;
extern THR_LOCAL char* transparent_encrypt_kms_url;
extern THR_LOCAL char* transparent_encrypt_kms_region;

extern void release_opt_lines(char** opt_lines);
extern char** alloc_opt_lines(int opt_lines_num);

#ifdef ENABLE_QUNIT
extern void set_qunit_case_number_hook(int newval, void* extra);
#endif

extern GucContext get_guc_context();

extern bool check_double_parameter(double* newval, void** extra, GucSource source);
extern bool CheckReplChannel(const char* ChannelInfo);
extern bool logging_module_check(char** newval, void** extra, GucSource source);
extern void logging_module_guc_assign(const char* newval, void* extra);
extern bool check_directory(char** newval, void** extra, GucSource source);
extern bool transparent_encrypt_kms_url_region_check(char** newval, void** extra, GucSource source);
extern bool check_canonical_path(char** newval, void** extra, GucSource source);
extern char* SetVariableExprGetConfigOption(SetVariableExpr* set);
#ifdef ENABLE_MULTIPLE_NODES
extern const char* show_nodegroup_mode(void);
#endif

extern THR_LOCAL GucContext currentGucContext;

#endif /* GUC_H */
