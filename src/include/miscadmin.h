/* -------------------------------------------------------------------------
 *
 * miscadmin.h
 *	  This file contains general openGauss administration and initialization
 *	  stuff that used to be spread out between the following files:
 *		globals.h						global variables
 *		pdir.h							directory path crud
 *		pinit.h							openGauss initialization
 *		pmod.h							processing modes
 *	  Over time, this has also become the preferred place for widely known
 *	  resource-limitation stuff, such as u_sess->attr.attr_memory.work_mem and check_stack_depth().
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/miscadmin.h
 *
 * NOTES
 *	  some of the information in this file should be moved to other files.
 *
 * -------------------------------------------------------------------------
 */
#ifndef MISCADMIN_H
#define MISCADMIN_H

#ifndef FRONTEND
#include "postgres.h"
#include "knl/knl_variable.h"
#endif
#include "gs_thread.h"
#include "pgtime.h" /* for pg_time_t */
#include "libpq/libpq-be.h"

#define PG_BACKEND_VERSIONSTR "gaussdb " DEF_GS_VERSION "\n"

/*****************************************************************************
 *	  Backend version and inplace upgrade staffs
 *****************************************************************************/
extern const uint32 SUPPORT_GS_DEPENDENCY_VERSION_NUM;
extern const uint32 TXNSTATUS_CACHE_DFX_VERSION_NUM;
extern const uint32 PARAM_MARK_VERSION_NUM;
extern const uint32 TIMESCALE_DB_VERSION_NUM;
extern const uint32 NBTREE_INSERT_OPTIMIZATION_VERSION_NUM;
extern const uint32 NBTREE_DEDUPLICATION_VERSION_NUM;
extern const uint32 MULTI_CHARSET_VERSION_NUM;
extern const uint32 SRF_FUSION_VERSION_NUM;
extern const uint32 INNER_UNIQUE_VERSION_NUM;
extern const uint32 PARTITION_ENHANCE_VERSION_NUM;
extern const uint32 SELECT_INTO_FILE_VERSION_NUM;
extern const uint32 CHARACTER_SET_VERSION_NUM;
extern const uint32 SELECT_INTO_VAR_VERSION_NUM;
extern const uint32 LARGE_SEQUENCE_VERSION_NUM;
extern const uint32 GRAND_VERSION_NUM;
extern const uint32 DOLPHIN_ENABLE_DROP_NUM;
extern const uint32 SQL_PATCH_VERSION_NUM;
extern const uint32 PREDPUSH_SAME_LEVEL_VERSION_NUM;
extern const uint32 UPSERT_WHERE_VERSION_NUM;
extern const uint32 FUNC_PARAM_COL_VERSION_NUM;
extern const uint32 SUBPARTITION_VERSION_NUM;
extern const uint32 PBESINGLEPARTITION_VERSION_NUM;
extern const uint32 COMMENT_PROC_VERSION_NUM;
extern const uint32 COMMENT_ROWTYPE_TABLEOF_VERSION_NUM;
extern const uint32 COMMENT_ROWTYPE_NEST_TABLEOF_VERSION_NUM;
extern const uint32 COMMENT_PCT_TYPE_VERSION_NUM;
extern const uint32 HINT_ENHANCEMENT_VERSION_NUM;
extern const uint32 MATERIALIZED_CTE_NUM;
extern const uint32 DEFAULT_MAT_CTE_NUM;
extern const uint32 MATVIEW_VERSION_NUM;
extern const uint32 SWCB_VERSION_NUM;
extern const uint32 PARTIALPUSH_VERSION_NUM;
extern const uint32 SUBLINKPULLUP_VERSION_NUM;
extern const uint32 PREDPUSH_VERSION_NUM;
extern const uint32 GTMLITE_VERSION_NUM;
extern const uint32 EXECUTE_DIRECT_ON_MULTI_VERSION_NUM;
extern const uint32 FIX_PBE_CUSTOME_PLAN_BUG_VERSION_NUM;
extern const uint32 FUNCNAME_PUSHDOWN_VERSION_NUM;
extern const uint32 STP_SUPPORT_COMMIT_ROLLBACK;
extern const uint32 SUPPORT_GPI_VERSION_NUM;
extern const uint32 PRIVS_VERSION_NUM;
extern const uint32 ML_OPT_MODEL_VERSION_NUM;
extern const uint32 RANGE_LIST_DISTRIBUTION_VERSION_NUM;
extern const uint32 FIX_SQL_ADD_RELATION_REF_COUNT;
extern const uint32 INPLACE_UPDATE_VERSION_NUM;
extern const uint32 GENERATED_COL_VERSION_NUM;
extern const uint32 SEGMENT_PAGE_VERSION_NUM;
extern const uint32 DECODE_ABORT_VERSION_NUM;
extern const uint32 COPY_TRANSFORM_VERSION_NUM;
extern const uint32 TDE_VERSION_NUM;
extern const uint32 PARALLEL_DECODE_VERSION_NUM;
extern const uint32 BACKEND_VERSION_INCLUDE_NUM;
extern const uint32 BACKEND_VERSION_PRE_END_NUM;
extern const uint32 BACKEND_VERSION_PRE_INCLUDE_NUM;
extern const uint32 TWOPHASE_FILE_VERSION;
extern const uint32 CLIENT_ENCRYPTION_PROC_VERSION_NUM;
extern const uint32 PRIVS_DIRECTORY_VERSION_NUM;
extern const uint32 COMMENT_RECORD_PARAM_VERSION_NUM;
extern const uint32 INVALID_INVISIBLE_TUPLE_VERSION;
extern const uint32 ENHANCED_TUPLE_LOCK_VERSION_NUM;
extern const uint32 HASUID_VERSION_NUM;
extern const uint32 CREATE_INDEX_CONCURRENTLY_DIST_VERSION_NUM;
extern const uint32 WAIT_N_TUPLE_LOCK_VERSION_NUM;
extern const uint32 DISASTER_READ_VERSION_NUM;
extern const uint32 SKIP_LOCKED_VERSION_NUM;
extern const uint32 SUPPORT_DATA_REPAIR;
extern const uint32 SCAN_BATCH_MODE_VERSION_NUM;
extern const uint32 RELMAP_4K_VERSION_NUM;
extern const uint32 PUBLICATION_VERSION_NUM;
extern const uint32 SUBSCRIPTION_BINARY_VERSION_NUM;
extern const uint32 ANALYZER_HOOK_VERSION_NUM;
extern const uint32 SUPPORT_HASH_XLOG_VERSION_NUM;
extern const uint32 PITR_INIT_VERSION_NUM;
extern const uint32 PUBLICATION_INITIAL_DATA_VERSION_NAME;
extern const uint32 REPLACE_INTO_VERSION_NUM;
extern const uint32 CREATE_FUNCTION_DEFINER_VERSION;
extern const uint32 KEYWORD_IGNORE_COMPART_VERSION_NUM;
extern const uint32 CSN_TIME_BARRIER_VERSION;
extern const uint32 ADVANCE_CATALOG_XMIN_VERSION_NUM;
extern const uint32 V5R2C00_ADVANCE_CATALOG_XMIN_VERSION_NUM;
extern const uint32 LOGICAL_DECODE_FLATTEN_TOAST_VERSION_NUM;
extern const uint32 SWITCH_ROLE_VERSION_NUM;
extern const uint32 MULTI_PARTITIONS_VERSION_NUM;
extern const uint32 MULTI_MODIFY_VERSION_NUM;
extern const uint32 COMMENT_SUPPORT_VERSION_NUM;
extern const uint32 PLAN_SELECT_VERSION_NUM;
extern const uint32 ON_UPDATE_TIMESTAMP_VERSION_NUM;
extern const uint32 STANDBY_STMTHIST_VERSION_NUM;
extern const uint32 PG_AUTHID_PASSWORDEXT_VERSION_NUM;
extern const uint32 MAT_VIEW_RECURSIVE_VERSION_NUM;
extern const uint32 SUPPORT_VIEW_AUTO_UPDATABLE;
extern const uint32 FDW_SUPPORT_JOIN_AGG_VERSION_NUM;
extern const uint32 UNION_NULL_VERSION_NUM;
extern const uint32 INSERT_RIGHT_REF_VERSION_NUM;
extern const uint32 CREATE_INDEX_IF_NOT_EXISTS_VERSION_NUM;
extern const uint32 SLOW_SQL_VERSION_NUM;
extern const uint32 INDEX_HINT_VERSION_NUM;
extern const uint32 CREATE_TABLE_AS_VERSION_NUM;
extern const uint32 GB18030_2022_VERSION_NUM;
extern const uint32 PARTITION_ACCESS_EXCLUSIVE_LOCK_UPGRADE_VERSION;
extern const uint32 SPQ_VERSION_NUM;
extern const uint32 UPSERT_ALIAS_VERSION_NUM;
extern const uint32 SELECT_STMT_HAS_USERVAR;
extern const uint32 PUBLICATION_DDL_VERSION_NUM;

extern void register_backend_version(uint32 backend_version);
extern bool contain_backend_version(uint32 version_number);

#define INPLACE_UPGRADE_PRECOMMIT_VERSION 1

// b_format_behavior_compat_options params
#define B_FORMAT_OPT_ENABLE_SET_SESSION_TRANSACTION 1
#define B_FORMAT_OPT_ENABLE_SET_VARIABLES 2
#define B_FORMAT_OPT_ENABLE_MODIFY_COLUMN 4
#define B_FORMAT_OPT_DEFAULT_COLLATION 8
#define B_FORMAT_OPT_FETCH 16
#define B_FORMAT_OPT_DIAGNOSTICS 32
#define B_FORMAT_OPT_ENABLE_MULTI_CHARSET 64
#define B_FORMAT_OPT_MAX 7

#define ENABLE_SET_SESSION_TRANSACTION                                                                   \
    ((u_sess->utils_cxt.b_format_behavior_compat_flags & B_FORMAT_OPT_ENABLE_SET_SESSION_TRANSACTION) && \
     u_sess->attr.attr_sql.sql_compatibility == B_FORMAT)
#define ENABLE_SET_VARIABLES (u_sess->utils_cxt.b_format_behavior_compat_flags & B_FORMAT_OPT_ENABLE_SET_VARIABLES)
#define USE_DEFAULT_COLLATION (u_sess->utils_cxt.b_format_behavior_compat_flags & B_FORMAT_OPT_DEFAULT_COLLATION && \
    t_thrd.proc->workingVersionNum >= CHARACTER_SET_VERSION_NUM && u_sess->attr.attr_common.upgrade_mode == 0)
#define ENABLE_MODIFY_COLUMN \
        ((u_sess->utils_cxt.b_format_behavior_compat_flags & B_FORMAT_OPT_ENABLE_MODIFY_COLUMN) && \
        u_sess->attr.attr_sql.sql_compatibility == B_FORMAT)
#define B_FETCH ((u_sess->utils_cxt.b_format_behavior_compat_flags & B_FORMAT_OPT_FETCH) && \
        u_sess->attr.attr_sql.sql_compatibility == B_FORMAT)
#define ENABLE_MULTI_CHARSET \
        ((u_sess->utils_cxt.b_format_behavior_compat_flags & B_FORMAT_OPT_ENABLE_MULTI_CHARSET) && \
        u_sess->attr.attr_sql.sql_compatibility == B_FORMAT)

#define B_DIAGNOSTICS ((u_sess->utils_cxt.b_format_behavior_compat_flags & B_FORMAT_OPT_DIAGNOSTICS) && \
        u_sess->attr.attr_sql.sql_compatibility == B_FORMAT)

#define OPT_DISPLAY_LEADING_ZERO 1
#define OPT_END_MONTH_CALCULATE 2
#define OPT_COMPAT_ANALYZE_SAMPLE 4
#define OPT_BIND_SCHEMA_TABLESPACE 8
#define OPT_RETURN_NS_OR_NULL 16
#define OPT_BIND_SEARCHPATH 32
#define OPT_UNBIND_DIVIDE_BOUND 64
#define OPT_CORRECT_TO_NUMBER 128
#define OPT_CONCAT_VARIADIC 256
#define OPT_MERGE_UPDATE_MULTI 512
#define OPT_CONVERT_TO_NUMERIC 1024
#define OPT_PLSTMT_IMPLICIT_SAVEPOINT 2048
#define OPT_HIDE_TAILING_ZERO 4096
#define OPT_SECURITY_DEFINER 8192
#define OPT_SKIP_GS_SOURCE 16384
#define OPT_PROC_OUTPARAM_OVERRIDE 32768
#define OPT_ALLOW_PROCEDURE_COMPILE_CHECK 65536
#define OPT_IMPLICIT_FOR_LOOP_VARIABLE 131072
#define OPT_AFORMAT_NULL_TEST 262144
#define OPT_AFORMAT_REGEX_MATCH 524288
#define OPT_ROWNUM_TYPE_COMPAT 1048576
#define OPT_COMPAT_CURSOR 2097152
#define OPT_CHAR_COERCE_COMPAT 4194304
#define OPT_PGFORMAT_SUBSTR 8388608
#define OPT_TRUNC_NUMERIC_TAIL_ZERO 16777216
#define OPT_ALLOW_ORDERBY_UNDISTINCT_COLUMN 33554432
#define OPT_SELECT_INTO_RETURN_NULL 67108864
#define OPT_ACCEPT_EMPTY_STR 134217728
#define OPT_PLPGSQL_DEPENDENCY 268435456
#define OPT_PROC_UNCHECK_DEFAULT_PARAM 536870912
#define OPT_MAX 30

#define PLPSQL_OPT_FOR_LOOP 1
#define PLPSQL_OPT_OUTPARAM 2
#define PLPSQL_OPT_MAX 2

#define DISPLAY_LEADING_ZERO (u_sess->utils_cxt.behavior_compat_flags & OPT_DISPLAY_LEADING_ZERO)
#define END_MONTH_CALCULATE (u_sess->utils_cxt.behavior_compat_flags & OPT_END_MONTH_CALCULATE)
#define SUPPORT_PRETTY_ANALYZE (!(u_sess->utils_cxt.behavior_compat_flags & OPT_COMPAT_ANALYZE_SAMPLE))
#define SUPPORT_BIND_TABLESPACE (u_sess->utils_cxt.behavior_compat_flags & OPT_BIND_SCHEMA_TABLESPACE)
#define SUPPORT_BIND_DIVIDE (!(u_sess->utils_cxt.behavior_compat_flags & OPT_UNBIND_DIVIDE_BOUND))
#define RETURN_NS (u_sess->utils_cxt.behavior_compat_flags & OPT_RETURN_NS_OR_NULL)
#define CORRECT_TO_NUMBER (u_sess->utils_cxt.behavior_compat_flags & OPT_CORRECT_TO_NUMBER)
#define SUPPORT_BIND_SEARCHPATH (u_sess->utils_cxt.behavior_compat_flags & OPT_BIND_SEARCHPATH)
#define ACCEPT_EMPTY_STR (u_sess->utils_cxt.behavior_compat_flags & OPT_ACCEPT_EMPTY_STR)
/*CONCAT_VARIADIC controls 1.the variadic type process, and 2. td mode null return process in concat. By default, the
 * option is blank and the behavior is new and compatible with current A and C mode, if the option is set, the
 * behavior is old and the same as previous GAUSSDB kernel. */
#define CONCAT_VARIADIC (!(u_sess->utils_cxt.behavior_compat_flags & OPT_CONCAT_VARIADIC))
#define MERGE_UPDATE_MULTI (u_sess->utils_cxt.behavior_compat_flags & OPT_MERGE_UPDATE_MULTI)
#define CONVERT_STRING_DIGIT_TO_NUMERIC (u_sess->utils_cxt.behavior_compat_flags & OPT_CONVERT_TO_NUMERIC)
#define PLSTMT_IMPLICIT_SAVEPOINT (u_sess->utils_cxt.behavior_compat_flags & OPT_PLSTMT_IMPLICIT_SAVEPOINT)
#define HIDE_TAILING_ZERO (u_sess->utils_cxt.behavior_compat_flags & OPT_HIDE_TAILING_ZERO)
#define PLSQL_SECURITY_DEFINER (u_sess->utils_cxt.behavior_compat_flags & OPT_SECURITY_DEFINER)
#define SKIP_GS_SOURCE (u_sess->utils_cxt.behavior_compat_flags & OPT_SKIP_GS_SOURCE)
#define PROC_OUTPARAM_OVERRIDE (u_sess->utils_cxt.behavior_compat_flags & OPT_PROC_OUTPARAM_OVERRIDE)
#define ALLOW_PROCEDURE_COMPILE_CHECK (u_sess->utils_cxt.behavior_compat_flags & OPT_ALLOW_PROCEDURE_COMPILE_CHECK)
#define IMPLICIT_FOR_LOOP_VARIABLE (u_sess->utils_cxt.behavior_compat_flags & OPT_IMPLICIT_FOR_LOOP_VARIABLE \
    || (u_sess->utils_cxt.plsql_compile_behavior_compat_flags & PLPSQL_OPT_FOR_LOOP))
#define AFORMAT_NULL_TEST (u_sess->utils_cxt.behavior_compat_flags & OPT_AFORMAT_NULL_TEST)
#define AFORMAT_REGEX_MATCH (u_sess->utils_cxt.behavior_compat_flags & OPT_AFORMAT_REGEX_MATCH)
#define ROWNUM_TYPE_COMPAT (u_sess->utils_cxt.behavior_compat_flags & OPT_ROWNUM_TYPE_COMPAT)
#define COMPAT_CURSOR (u_sess->utils_cxt.behavior_compat_flags & OPT_COMPAT_CURSOR)
#define CHAR_COERCE_COMPAT (u_sess->utils_cxt.behavior_compat_flags & OPT_CHAR_COERCE_COMPAT)
#define PGFORMAT_SUBSTR (u_sess->utils_cxt.behavior_compat_flags & OPT_PGFORMAT_SUBSTR)
#define TRUNC_NUMERIC_TAIL_ZERO (u_sess->utils_cxt.behavior_compat_flags & OPT_TRUNC_NUMERIC_TAIL_ZERO)
#define ALLOW_ORDERBY_UNDISTINCT_COLUMN (u_sess->utils_cxt.behavior_compat_flags & OPT_ALLOW_ORDERBY_UNDISTINCT_COLUMN)

#define PLSQL_COMPILE_FOR_LOOP (u_sess->utils_cxt.plsql_compile_behavior_compat_flags & PLPSQL_OPT_FOR_LOOP)
#define PLSQL_COMPILE_OUTPARAM (u_sess->utils_cxt.plsql_compile_behavior_compat_flags & PLPSQL_OPT_OUTPARAM)

#define SELECT_INTO_RETURN_NULL (u_sess->utils_cxt.behavior_compat_flags & OPT_SELECT_INTO_RETURN_NULL)
#define PLPGSQL_DEPENDENCY (u_sess->utils_cxt.behavior_compat_flags & OPT_PLPGSQL_DEPENDENCY)
#define PROC_UNCHECK_DEFAULT_PARAM (u_sess->utils_cxt.behavior_compat_flags & OPT_PROC_UNCHECK_DEFAULT_PARAM)

/* define database compatibility Attribute */
typedef struct {
    int flag;
    char name[256];
} DB_CompatibilityAttr;
#define DB_CMPT_A 0
#define DB_CMPT_B 1
#define DB_CMPT_C 2
#define DB_CMPT_PG 3

extern bool checkCompArgs(const char *cmptFmt);

typedef struct {
    char name[32];
} IntervalStylePack;

extern DB_CompatibilityAttr g_dbCompatArray[];
extern IntervalStylePack g_interStyleVal;

/* in tcop/postgres.c */
extern void ProcessInterrupts(void);

#ifndef WIN32

#define CHECK_FOR_INTERRUPTS()   \
    do {                         \
        if (InterruptPending)    \
            ProcessInterrupts(); \
    } while (0)
#else /* WIN32 */

#define CHECK_FOR_INTERRUPTS()                 \
    do {                                       \
        if (UNBLOCKED_SIGNAL_QUEUE())          \
            pgwin32_dispatch_queued_signals(); \
        if (InterruptPending)                  \
            ProcessInterrupts();               \
    } while (0)
#endif /* WIN32 */

#define HOLD_INTERRUPTS() (t_thrd.int_cxt.InterruptHoldoffCount++)

#define RESUME_INTERRUPTS()                                                                      \
    do {                                                                                         \
        if (t_thrd.int_cxt.InterruptCountResetFlag && t_thrd.int_cxt.InterruptHoldoffCount== 0){ \
            t_thrd.int_cxt.InterruptCountResetFlag = false;                                      \
        } else {                                                                                 \
            Assert(t_thrd.int_cxt.InterruptHoldoffCount > 0);                                    \
            t_thrd.int_cxt.InterruptHoldoffCount--;                                              \
        }                                                                                        \
    } while (0)

#define PREVENT_POOL_VALIDATE_SIGUSR2()                                        \
    do {                                                                       \
        if (t_thrd.int_cxt.PoolValidateCancelPending && IS_PGXC_COORDINATOR) { \
            g_pq_interrupt_happened = false;                                   \
            t_thrd.int_cxt.ProcDiePending = false;                             \
            t_thrd.int_cxt.QueryCancelPending = false;                         \
            t_thrd.int_cxt.PoolValidateCancelPending = false;                  \
        }                                                                      \
    } while (0)                                                                \

#define START_CRIT_SECTION() (t_thrd.int_cxt.CritSectionCount++)

#define END_CRIT_SECTION()                           \
    do {                                             \
        Assert(t_thrd.int_cxt.CritSectionCount > 0); \
        t_thrd.int_cxt.CritSectionCount--;           \
    } while (0)

#define HOLD_CANCEL_INTERRUPTS()  (t_thrd.int_cxt.QueryCancelHoldoffCount++)

#define RESUME_CANCEL_INTERRUPTS() \
do { \
    Assert(t_thrd.int_cxt.QueryCancelHoldoffCount > 0); \
    t_thrd.int_cxt.QueryCancelHoldoffCount--; \
} while (0)

/*****************************************************************************
 *	  globals.h --															 *
 *****************************************************************************/
extern bool open_join_children;
extern bool will_shutdown;
extern bool dummyStandbyMode;

/*
 * from utils/init/globals.c
 */
extern THR_LOCAL PGDLLIMPORT volatile bool InterruptPending;

extern volatile ThreadId PostmasterPid;
extern bool IsPostmasterEnvironment;
extern volatile uint32 WorkingGrandVersionNum;
extern bool InplaceUpgradePrecommit;

extern THR_LOCAL PGDLLIMPORT bool IsUnderPostmaster;
extern THR_LOCAL PGDLLIMPORT char my_exec_path[];

extern uint8 ce_cache_refresh_type;

#define MAX_QUERY_DOP (64)
#define MIN_QUERY_DOP -(MAX_QUERY_DOP)

extern const uint32 BACKUP_SLOT_VERSION_NUM;

/* Debug mode.
 * 0 - Do not change any thing.
 * 1 - For test: Parallel when inExplain. And change the scan limit to MIN_ROWS_L.
 * 2 - For llt: Do not parallel when inExplain. And change the scan limit to MIN_ROWS_L.
 */
#define DEFAULT_MODE 0
#define DEBUG_MODE 1
#define LLT_MODE 2

/*
 * Date/Time Configuration
 *
 * u_sess->time_cxt.DateStyle defines the output formatting choice for date/time types:
 *	USE_POSTGRES_DATES specifies traditional Postgres format
 *	USE_ISO_DATES specifies ISO-compliant format
 *	USE_SQL_DATES specifies A db/Ingres-compliant format
 *	USE_GERMAN_DATES specifies German-style dd.mm/yyyy
 *
 */

/* valid u_sess->time_cxt.DateStyle values */
#define USE_POSTGRES_DATES 0
#define USE_ISO_DATES 1
#define USE_SQL_DATES 2
#define USE_GERMAN_DATES 3
#define USE_XSD_DATES 4

/* valid u_sess->time_cxt.DateOrder values */
#define DATEORDER_YMD 0
#define DATEORDER_DMY 1
#define DATEORDER_MDY 2

/*
 * u_sess->attr.attr_common.IntervalStyles
 *	 INTSTYLE_POSTGRES			   Like Postgres < 8.4 when u_sess->time_cxt.DateStyle = 'iso'
 *	 INTSTYLE_POSTGRES_VERBOSE	   Like Postgres < 8.4 when u_sess->time_cxt.DateStyle != 'iso'
 *	 INTSTYLE_SQL_STANDARD		   SQL standard interval literals
 *	 INTSTYLE_ISO_8601			   ISO-8601-basic formatted intervals
 */
#define INTSTYLE_POSTGRES 0
#define INTSTYLE_POSTGRES_VERBOSE 1
#define INTSTYLE_SQL_STANDARD 2
#define INTSTYLE_ISO_8601 3
#define INTSTYLE_A 4

#define MAXTZLEN 10                        /* max TZ name len, not counting tr. null */
#define OPT_MAX_OP_MEM (4 * 1024L * 1024L) /* 4GB, used to restrict operator mem in optimizer */

#ifdef PGXC
extern bool useLocalXid;
#endif
extern bool EnableInitDBSegment;

#define DEFUALT_STACK_SIZE 16384

/* in tcop/postgres.c */

#if defined(__ia64__) || defined(__ia64)
typedef struct {
    char* stack_base_ptr;
    char* register_stack_base_ptr;
} pg_stack_base_t;
#else
typedef char* pg_stack_base_t;
#endif

extern pg_stack_base_t set_stack_base(void);
extern void restore_stack_base(pg_stack_base_t base);
extern void check_stack_depth(void);
extern bool stack_is_too_deep(void);

/* in tcop/utility.c */
extern void PreventCommandIfReadOnly(const char* cmdname);
extern void PreventCommandDuringRecovery(const char* cmdname);
extern void PreventCommandDuringSSOndemandRedo(Node* parseTree);

extern int trace_recovery(int trace_level);

/*****************************************************************************
 *	  pdir.h --																 *
 *			openGauss directory path definitions.							 *
 *****************************************************************************/

/* flags to be OR'd to form sec_context */
#define SECURITY_LOCAL_USERID_CHANGE 0x0001
#define SECURITY_RESTRICTED_OPERATION 0x0002
#define SENDER_LOCAL_USERID_CHANGE 0x0004
#define RECEIVER_LOCAL_USERID_CHANGE 0x0008

/* now in utils/init/miscinit.c */
extern char* GetUserNameFromId(Oid roleid);
extern char* GetUserNameById(Oid roleid);
extern Oid GetAuthenticatedUserId(void);
extern Oid GetUserId(void);
extern Oid getCurrentNamespace();
extern Oid GetCurrentUserId(void);
extern Oid GetOldUserId(bool isReceive);
extern void SetOldUserId(Oid userId, bool isReceive);
extern Oid GetOuterUserId(void);
extern Oid GetSessionUserId(void);
extern void GetUserIdAndSecContext(Oid* userid, int* sec_context);
extern void SetUserIdAndSecContext(Oid userid, int sec_context);
extern bool InLocalUserIdChange(void);
extern bool InSecurityRestrictedOperation(void);
extern bool InReceivingLocalUserIdChange();
extern bool InSendingLocalUserIdChange();
extern void GetUserIdAndContext(Oid* userid, bool* sec_def_context);
extern void SetUserIdAndContext(Oid userid, bool sec_def_context);
extern void InitializeSessionUserId(const char* rolename, bool flagresue = false, Oid useroid = InvalidOid);
extern void InitializeSessionUserIdStandalone(void);
extern void SetSessionAuthorization(Oid userid, bool is_superuser);
extern Oid GetCurrentRoleId(void);
extern void SetCurrentRoleId(Oid roleid, bool is_superuser);
void ss_initdwsubdir(char *dssdir, int instance_id);
extern void initDSSConf(void);

extern Oid get_current_lcgroup_oid();
extern const char* get_current_lcgroup_name();
extern bool is_lcgroup_admin();
extern bool is_logic_cluster(Oid group_id);
extern bool in_logic_cluster();
extern bool exist_logic_cluster();
#ifdef ENABLE_MULTIPLE_NODES
extern const char* show_nodegroup_mode(void);
#endif

#ifndef ENABLE_MULTIPLE_NODES
extern const int GetCustomParserId();
#endif
extern const char* show_lcgroup_name();
extern Oid get_pgxc_logic_groupoid(Oid roleid);
extern Oid get_pgxc_logic_groupoid(const char* groupname);
extern void Reset_Pseudo_CurrentUserId(void);

extern void SetDataDir(const char* dir);
extern void ChangeToDataDir(void);
extern char* make_absolute_path(const char* path);

/* in utils/misc/superuser.c */
extern bool superuser(
    void); /* when privileges separate is used,current user is or superuser or sysdba,if not,current user is superuser*/
extern bool isRelSuperuser(
    void);                     /*current user is real superuser.if you don't want sysdba to entitle to operate,use it*/
extern bool initialuser(void); /* current user is initial user. */
extern bool superuser_arg(Oid roleid);                   /* given user is superuser */
extern bool superuser_arg_no_seperation(Oid roleid);     /* given user is superuser (no seperation)*/
extern bool systemDBA_arg(Oid roleid);                   /* given user is systemdba */
extern bool isSecurityadmin(Oid roleid);                 /* given user is security admin */
extern bool isAuditadmin(Oid roleid);                    /* given user is audit admin */
extern bool isMonitoradmin(Oid roleid);                 /* given user is monitor admin */
extern bool isOperatoradmin(Oid roleid);                /* given user is operator admin */
extern bool isPolicyadmin(Oid roleid);                  /* given user is policy admin */
extern bool CheckExecDirectPrivilege(const char* query); /* check user have privilege to use execute direct */

/*****************************************************************************
 *	  pmod.h --																 *
 *			openGauss processing mode definitions.							 *
 *****************************************************************************/

#define IsBootstrapProcessingMode() (u_sess->misc_cxt.Mode == BootstrapProcessing)
#define IsInitProcessingMode() (u_sess->misc_cxt.Mode == InitProcessing)
#define IsNormalProcessingMode() (u_sess->misc_cxt.Mode == NormalProcessing)
#define IsPostUpgradeProcessingMode() (u_sess->misc_cxt.Mode == PostUpgradeProcessing)
#define IsFencedProcessingMode() (u_sess->misc_cxt.Mode == FencedProcessing)

#define GetProcessingMode() u_sess->misc_cxt.Mode

#define SetProcessingMode(mode)                                                                              \
    do {                                                                                                     \
        AssertArg((mode) == BootstrapProcessing || (mode) == InitProcessing || (mode) == NormalProcessing || \
                  (mode) == PostUpgradeProcessing ||  (mode) == FencedProcessing);                                                          \
        u_sess->misc_cxt.Mode = (mode);                                                                      \
    } while (0)

#ifdef ENABLE_LITE_MODE
#define ENABLE_DSS false
#define ENABLE_DSS_AIO false
#else
#define ENABLE_DSS (g_instance.attr.attr_storage.dss_attr.ss_enable_dss)
#define ENABLE_DSS_AIO (ENABLE_DSS && g_instance.attr.attr_storage.dms_attr.enable_dss_aio && !IsInitdb)
#endif

/*
 * Auxiliary-process type identifiers.
 */
typedef enum {
    NotAnAuxProcess = -1,
    CheckerProcess = 0,
    BootstrapProcess,
    StartupProcess,
    BgWriterProcess,
    SpBgWriterProcess,
    PageRepairProcess,
    CheckpointerProcess,
    WalWriterProcess,
    WalWriterAuxiliaryProcess,
    WalReceiverProcess,
    WalRcvWriterProcess,
    DataReceiverProcess,
    DataRcvWriterProcess,
    HeartbeatProcess,
#ifdef PGXC
    TwoPhaseCleanerProcess,
    WLMWorkerProcess,
    WLMMonitorWorkerProcess,
    WLMArbiterWorkerProcess,
    CPMonitorProcess,
    FaultMonitorProcess,
    CBMWriterProcess,
    RemoteServiceProcess,
#endif
    AsyncIOCompleterProcess,
    TpoolSchdulerProcess,
    UndoRecyclerProcess,
    TsCompactionProcess,
    TsCompactionAuxiliaryProcess,
    XlogCopyBackendProcess,
    BarrierPreParseBackendProcess,
    DmsAuxiliaryProcess,
    ExrtoRecyclerProcess,
    NUM_SINGLE_AUX_PROC, /* Sentry for auxiliary type with single thread. */

    /*
     * If anyone want add a new auxiliary thread type, and will create several
     * threads for this type, then you must add it below NUM_SINGLE_AUX_PROC.
     * Meanwhile, you must update NUM_MULTI_AUX_PROC and GetAuxProcEntryIndex().
     */
    PageWriterProcess,
    PageRedoProcess,
    TpoolListenerProcess,
    TsCompactionConsumerProcess,
    CsnMinSyncProcess,
    ParallelDecodeProcess,
    LogicalReadRecord,

    NUM_AUXPROCTYPES /* Must be last! */
} AuxProcType;

#define AmBootstrapProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == BootstrapProcess)
#define AmStartupProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == StartupProcess)
#define AmPageRedoProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == PageRedoProcess)
#define AmBackgroundWriterProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == BgWriterProcess)
#define AmCheckpointerProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == CheckpointerProcess)
#define AmWalWriterProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == WalWriterProcess)
#define AmWalWriterAuxiliaryProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == WalWriterAuxiliaryProcess)
#define AmWalReceiverProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == WalReceiverProcess)
#define AmWalReceiverWriterProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == WalRcvWriterProcess)
#define AmDataReceiverProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == DataReceiverProcess)
#define AmDataReceiverWriterProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == DataRcvWriterProcess)
#define AmWLMWorkerProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == WLMWorkerProcess)
#define AmWLMMonitorProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == WLMMonitorWorkerProcess)
#define AmWLMArbiterProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == WLMArbiterWorkerProcess)
#define AmCPMonitorProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == CPMonitorProcess)
#define AmCBMWriterProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == CBMWriterProcess)
#define AmRemoteServiceProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == RemoteServiceProcess)
#define AmPageWriterProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == PageWriterProcess)
#define AmPageWriterMainProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == PageWriterProcess && \
    t_thrd.pagewriter_cxt.pagewriter_id == 0)
#define AmHeartbeatProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == HeartbeatProcess)
#define AmTsCompactionProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == TsCompactionProcess)
#define AmTsCompactionConsumerProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == TsCompactionConsumerProcess)
#define AmTsCompactionAuxiliaryProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == TsCompactionAuxiliaryProcess)
#define AmPageRedoWorker() (t_thrd.bootstrap_cxt.MyAuxProcType == PageRedoProcess)
#define AmDmsReformProcProcess() (t_thrd.role == DMS_WORKER && t_thrd.dms_cxt.is_reform_proc)
#define AmDmsProcess() (t_thrd.role == DMS_WORKER)
#define AmErosRecyclerProcess() (t_thrd.bootstrap_cxt.MyAuxProcType == ExrtoRecyclerProcess)
#define AmDmsAuxiliaryProcess() (t_thrd.role == DMS_AUXILIARY_THREAD)



/*****************************************************************************
 *	  pinit.h --															 *
 *			openGauss initialization and cleanup definitions.				 *
 *****************************************************************************/

/* in utils/init/postinit.c */
extern void pg_split_opts(char** argv, int* argcp, char* optstr);
extern void PostgresResetUsernamePgoption(const char* username, bool ispoolerreuse = false);
extern void BaseInit(void);

/*
 * As of 9.1, the contents of the data-directory lock file are:
 *
 * line #
 *		1	postmaster PID (or negative of a standalone backend's PID)
 *		2	data directory path
 *		3	postmaster start timestamp (time_t representation)
 *		4	port number
 *		5	socket directory path (empty on Windows)
 *		6	first listen_address (IP address or "*"; empty if no TCP port)
 *		7	shared memory key (not present on Windows)
 *
 * Lines 6 and up are added via AddToDataDirLockFile() after initial file
 * creation; they have to be ordered according to time of addition.
 *
 * The socket lock file, if used, has the same contents as lines 1-5.
 */
#define LOCK_FILE_LINE_PID 1
#define LOCK_FILE_LINE_DATA_DIR 2
#define LOCK_FILE_LINE_START_TIME 3
#define LOCK_FILE_LINE_PORT 4
#define LOCK_FILE_LINE_SOCKET_DIR 5
#define LOCK_FILE_LINE_LISTEN_ADDR 6
#define LOCK_FILE_LINE_SHMEM_KEY 7

extern void CreateDataDirLockFile(bool amPostmaster);
extern void CreateSocketLockFile(const char* socketfile, bool amPostmaster, bool is_create_psql_sock = true);
extern void TouchSocketLockFile(void);
extern void AddToDataDirLockFile(int target_line, const char* str);
extern void ValidatePgVersion(const char* path);
extern void process_shared_preload_libraries(void);
extern void process_local_preload_libraries(void);
extern void pg_bindtextdomain(const char* domain);
extern bool has_rolreplication(Oid roleid);
extern bool has_rolvcadmin(Oid roleid);

/* in access/transam/xlog.c */
extern bool BackupInProgress(void);
extern void CancelBackup(void);

extern void EarlyBindingTLSVariables(void);

/*
 * converts the 64 bits unsigned integer between host byte order and network byte order.
 * Note that the network byte order is BIG ENDIAN.
 */
#if __BYTE_ORDER == __LITTLE_ENDIAN
#define htonl64(x) htobe64(x)
#define ntohl64(x) be64toh(x)
#else
#define htonl64(x) (x)
#define ntohl64(x) (x)
#endif

#define UPSERT_ROW_STORE_VERSION_NUM 92073 /* Version control for UPSERT */
#endif /* MISCADMIN_H */
