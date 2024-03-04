/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2004-2012, PostgreSQL Global Development Group
 *
 *
 * pgaudit.cpp
 * 		auditor process
 *
 * 		The audit collector (auditor) catches all audit output from
 * 		the postmaster, backends, and other subprocesses by redirecting to a
 * 		pipe, and writes it to a set of auditfiles.
 * 		It's possible to have size and age limits for the auditfile configured
 * 		in postgresql.conf. If these limits are reached or passed, the
 * 		current auditfile is closed and a new one is created (rotated).
 * 		The auditfiles are stored in a subdirectory (configurable in
 * 		postgresql.conf), using a user-selectable naming scheme.
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/pgaudit.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>
#include <limits.h> /* for PIPE_BUF */
#include <sys/time.h>

#include "lib/stringinfo.h"
#include "libpq/libpq-be.h"
#include "libpq/pqsignal.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "pgtime.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "pgaudit.h"
#include "gs_policy/curl_utils.h"
#include "pgxc/pgxc.h"
#include "storage/ipc.h"
#include "storage/smgr/fd.h"
#include "storage/latch.h"
#include "storage/pg_shmem.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "utils/acl.h"
#include "utils/elog.h"
#include "auditfuncs.h"

#include "gssignal/gs_signal.h"
#include "gs_policy/policy_common.h"
#include <string>
#include <fstream>

#ifdef ENABLE_UT
#define static
#endif
/*
 * Primitive protocol structure for writing to sysauditor pipe(s).  The idea
 * here is to divide long messages into chunks that are not more than
 * PIPE_BUF bytes long, which according to POSIX spec must be written into
 * the pipe atomically.  The pipe reader then uses the protocol headers to
 * reassemble the parts of a message into a single string.    The reader can
 * also cope with non-protocol data coming down the pipe, though we cannot
 * guarantee long strings won't get split apart.
 *
 * We use non-nul bytes in is_last to make the protocol a tiny bit
 * more robust against finding a false double nul byte prologue. But
 * we still might find it in the len and/or pid bytes unless we're careful.
 */
#ifdef PIPE_BUF
/* Are there any systems with PIPE_BUF > 64K?  Unlikely, but ... */
#if PIPE_BUF > 65536
#define PIPE_CHUNK_SIZE 65536
#else
#define PIPE_CHUNK_SIZE ((int)PIPE_BUF)
#endif
#else /* not defined */
/* POSIX says the value of PIPE_BUF must be at least 512, so use that */
#define PIPE_CHUNK_SIZE 512
#endif

#define MAX_QUEUE_SIZE 100000
#define MAX_CONNECTION_INFO_SIZE (MAXNUMLEN * 4 + NI_MAXHOST)
const static uint32 uint64_max_len = 20;
const uint32 AUDIT_INDEX_TABLE_VERSION_NUM = 92601;

typedef struct {
    char nuls[2]; /* always \0\0 */
    uint16 len;   /* size of this chunk (counts data only) */
    ThreadId pid; /* writer's pid */
    char is_last; /* last chunk of message? 't' or 'f' ('T' or 'F' for CSV case) */
    char data[1]; /* data payload starts here */
} PipeProtoHeader;

typedef union {
    PipeProtoHeader proto;
    char filler[PIPE_CHUNK_SIZE];
} PipeProtoChunk;

#define PIPE_HEADER_SIZE offsetof(PipeProtoHeader, data)
#define PIPE_MAX_PAYLOAD ((int)(PIPE_CHUNK_SIZE - PIPE_HEADER_SIZE))

#define PIPE_READ_INDEX(index) (index * 2)
#define PIPE_WRITE_INDEX(index) (index * 2 + 1)

#define AUDIT_THREADNUM_ENLARGE \
    ((uint32)g_instance.audit_cxt.thread_num > g_instance.audit_cxt.audit_indextbl->thread_num)

/*
 * We really want line-buffered mode for auditfile output, but Windows does
 * not have it, and interprets _IOLBF as _IOFBF (bozos).  So use _IONBF
 * instead on Windows.
 */
#ifdef WIN32
#define LBF_MODE _IONBF
#define DIR_SEP  "\\"
#else
#define LBF_MODE _IOLBF
#define DIR_SEP  "/"
#endif

/*
 * We read() into a temp buffer twice as big as a chunk, so that any fragment
 * left after processing can be moved down to the front and we'll still have
 * room to read a full chunk.
 */
#define READ_BUF_SIZE (2 * PIPE_CHUNK_SIZE)

THR_LOCAL int       PolicyAudit_RotationAge = 5; // 5 seconds
/*
 * Brief        : bitnum in integer Audit_Session
 * Description    :
 */
typedef enum {
    SESSION_LOGIN_SUCCESS = 0,
    SESSION_LOGIN_FAILED,
    SESSION_LOGOUT
} SessionType;

/*
 * Private state
 */
static char* pgaudit_filename = "%s/%d_adt";
static char* policy_audit_filename = "%s/%lld_event.bin";
static int pgaudit_filemode = S_IRUSR | S_IWUSR;
/* rotation time for auditing policy */
static THR_LOCAL pg_time_t policy_next_rotation_time;

CurlUtils m_curlUtils;

/*
 * Buffers for saving partial messages from different backends.
 *
 * Keep NBUFFER_LISTS lists of these, with the entry for a given source pid
 * being in the list numbered (pid % NBUFFER_LISTS), so as to cut down on
 * the number of entries we have to examine for any one incoming message.
 * There must never be more than one entry for the same source pid.
 *
 * An inactive buffer is not removed from its list, just held for re-use.
 * An inactive buffer has pid == 0 and undefined contents of data.
 */
typedef struct {
    ThreadId pid;        /* PID of source process */
    StringInfoData data; /* accumulated data, as a StringInfo */
} save_buffer;

#define NBUFFER_LISTS 256

/*
 * Flags set by interrupt handlers for later service in the main loop.
 */
#define SPACE_INTERVAL_SIZE (10 * 1024 * 1024)                          // 10MB
const static uint64 SPACE_MAXIMUM_SIZE = (1024 * 1024 * 1024 * 1024L);  // 1024GB
/* The static variable for print log when exceeding the space limit */

/*
 * Brief        : audit index item in index table
 * Description    :
 */
typedef struct AuditIndexItem {
    /*
     * file create time. used when scan all the audit data.
     * if system time changed when auditor write into this file,
     * then ctime would less than zero.
     */
    pg_time_t ctime;

    uint32 filenum;  /* file number */
    uint32 filesize; /* file size */
} AuditIndexItem;

/*
 * Brief        : audit index table
 * Description    :
 */
typedef struct AuditIndexTableNew {
    uint32 maxnum;                          /* max count of the audit index item */
    uint32 begidx;                          /* the position of the first audit index item */
    uint32 thread_num;                      /* the running audit thread num */
    volatile uint32 latest_idx;             /* the latest next position of all audit threads index items */
    uint32 curidx[MAX_AUDIT_NUM];           /* the position of the current audit thread index item */
    uint32 count;                           /* the count of the audit index item */
    pg_time_t last_audit_time;              /* the audit time of the latest audit record */
    AuditIndexItem data[1];
} AuditIndexTableNew;

/*
 * Brief        : old audit index table
 * Description    :
 */
typedef struct AuditIndexTable {
    uint32 maxnum;             /* max count of the audit index item */
    uint32 begidx;             /* the position of the first audit index item */
    uint32 curidx;             /* the position of the current audit index item */
    uint32 count;              /* the count of the audit index item */
    pg_time_t last_audit_time; /* the audit time of the latest audit record */
    AuditIndexItem data[1];
} AuditIndexTable;

static const char audit_indextbl_file[] = "index_table_new";
static const char audit_indextbl_old_file[] = "index_table";
static const int indextbl_header_size = offsetof(AuditIndexTableNew, data);
static const int old_indextbl_header_size = offsetof(AuditIndexTable, data);

static const char* AuditTypeDescs[] = {"unknown",
                                       "login_success",
                                       "login_failed",
                                       "user_logout",
                                       "system_start",
                                       "system_stop",
                                       "system_recover",
                                       "system_switch",
                                       "lock_user",
                                       "unlock_user",
                                       "grant_role",
                                       "revoke_role",
                                       "user_violation",
                                       "ddl_database",
                                       "ddl_directory",
                                       "ddl_tablespace",
                                       "ddl_schema",
                                       "ddl_user",
                                       "ddl_table",
                                       "ddl_index",
                                       "ddl_view",
                                       "ddl_trigger",
                                       "ddl_function",
                                       "ddl_resourcepool",
                                       "ddl_workload",
                                       "ddl_serverforhadoop",
                                       "ddl_datasource",
                                       "ddl_nodegroup",
                                       "ddl_rowlevelsecurity",
                                       "ddl_synonym",
                                       "ddl_type",
                                       "ddl_textsearch",
                                       "dml_action",
                                       "dml_action_select",
                                       "internal_event",
                                       "function_exec",
                                       "system_function_exec",
                                       "copy_to",
                                       "copy_from",
                                       "set_parameter",
                                       "audit_policy",
                                       "masking_policy",
                                       "security_policy",
                                       "ddl_sequence",
                                       "ddl_key",
                                       "ddl_package",
                                       "ddl_model",
                                       "ddl_globalconfig",
                                       "ddl_publication_subscription",
                                       "ddl_foreign_data_wrapper",
                                       "ddl_sql_patch",
                                       "ddl_event"
};

static const int AuditTypeNum = sizeof(AuditTypeDescs) / sizeof(char*);

#define AuditTypeDesc(type) (((type) > 0 && (type) < AuditTypeNum) ? AuditTypeDescs[(type)] : AuditTypeDescs[0])

static const char* AuditResultDescs[] = {"unknown", "ok", "failed"};

static const int AuditResultNum = sizeof(AuditResultDescs) / sizeof(char*);

#define AuditResultDesc(type) (((type) > 0 && (type) < AuditResultNum) ? AuditResultDescs[(type)] : AuditResultDescs[0])

/*
 * Brief        : The audit message header
 * Description    : exactly 160 bits.
 */
typedef struct AuditMsgHdr {
    char signature[2]; /* always 'A''U' */
    uint16 version;    /* current is 0 */
    uint16 fields;     /* the field counts */
    uint16 flags;      /* flags mark the tuple is deleted */
    pg_time_t time;    /* audit time */
    uint32 size;       /* record length */
} AuditMsgHdr;

#define AUDIT_TUPLE_NORMAL 1 /* normal tuple */
#define AUDIT_TUPLE_DEAD 2   /* dead tuple */

typedef struct AuditEncodedData {
    AuditMsgHdr header;
    char data[1]; /* data payload starts here */
} AuditEncodedData;

/*
 * AuditData holds the data accumulated during any one audit_report() cycle.
 * Any non-NULL pointers must point to palloc'd data.
 * (The const pointers are an exception; we assume they point at non-freeable
 * constant strings.)
 */
typedef struct AuditData {
    AuditMsgHdr header;
    AuditType type;
    AuditResult result;
    char varstr[1]; /* variable length array - must be last */
} AuditData;

#define AUDIT_HEADER_SIZE offsetof(AuditData, varstr)

#define FILED_NULLABLE(field) (field ? field : _("null"))

#define PGAUDIT_RESTART_INTERVAL 60

#define PGAUDIT_QUERY_COLS 13

#define MAXNUMLEN 16

#define WRITE_TO_AUDITPIPE           (g_instance.audit_cxt.audit_init_done && t_thrd.role != AUDITOR)
#define WRITE_TO_STDAUDITFILE(ctype) (t_thrd.role == AUDITOR && ctype == STD_AUDIT_TYPE)
#define WRITE_TO_UNIAUDITFILE(ctype) (t_thrd.role == AUDITOR && ctype == UNIFIED_AUDIT_TYPE)

struct AuditEventInfo {
    AuditEventInfo() : userid{0}, 
                       username(NULL),
                       dbname(NULL),
                       appname(NULL),
                       remotehost(NULL),
                       client_info{0},
                       threadid{0},
                       localport{0},
                       remoteport{0} {}

    char userid[MAXNUMLEN];
    const char* username;
    const char* dbname;
    const char* appname;
    const char* remotehost;
    char client_info[MAX_CONNECTION_INFO_SIZE];
    char threadid[MAXNUMLEN * 4];
    char localport[MAXNUMLEN];
    char remoteport[MAXNUMLEN];
};

typedef enum {
    SYSAUDITFILE_TYPE = 1,
    POLICYAUDITFILE_TYPE,
    UNKNOWNFILE_TYPE
} AuditFileType;

/* Local subroutines */
static void process_pipe_input(char* auditbuffer, int* bytes_in_auditbuffer);
static void flush_pipe_input(char* auditbuffer, int* bytes_in_auditbuffer);
static void pgaudit_write_file(char* buffer, int count);

static void auditfile_init(bool allow_errors = false);
static FILE *auditfile_open(pg_time_t timestamp, const char *mode, bool allow_errors,
    const char *filename = pgaudit_filename, bool ignore_num = false);
static void auditfile_close(AuditFileType flag);

/****** audit logs management ******/
static void auditfile_rotate(bool time_based_rotation, bool size_based_rotation);
static void set_next_rotation_time(void);
static void pgaudit_cleanup(void);

/****** signale handler *****/
static void pgaudit_exit(SIGNAL_ARGS);
static void sigHupHandler(SIGNAL_ARGS);
static void sigUsr1Handler(SIGNAL_ARGS);
static void sig_thread_quit_handler();
static void sig_thread_config_handler(int &currentAuditRotationAge, int &currentAuditRemainThreshold);
static void pgauditor_kill(int code, Datum arg);

static void audit_process_cxt_init();
static void audit_process_cxt_exit();

static void write_pipe_chunks(char* data, int len, AuditClassType type = STD_AUDIT_TYPE);
static void appendStringField(StringInfo str, const char* s);
static void pgaudit_close_file(FILE* fp, const char* file);
static void pgaudit_read_indexfile(const char* audit_directory);
static void pgaudit_update_indexfile(const char* mode, bool allow_errors);
static bool pgaudit_find_indexfile(void);
static void pgaudit_indexfile_upgrade(void);
static void pgaudit_indexfile_sync(const char* mode, bool allow_errors);
static void pgaudit_rewrite_indexfile(void);
static void pgaudit_indextbl_init_new(void);
static void pgaudit_reset_indexfile();
static const char* pgaudit_string_field(AuditData* adata, int num);
static void deserialization_to_tuple(Datum (&values)[PGAUDIT_QUERY_COLS], 
                                     AuditData *adata, 
                                     const AuditMsgHdr &header);
static void pgaudit_query_file(Tuplestorestate *state, TupleDesc tdesc, uint32 fnum, TimestampTz begtime,
                               TimestampTz endtime, const char *audit_directory);
static TimestampTz pgaudit_headertime(uint32 fnum, const char *audit_directory);
static void pgaudit_query_valid_check(const ReturnSetInfo *rsinfo, FunctionCallInfoData *fcinfo, TupleDesc &tupdesc);

static uint32 pgaudit_get_auditfile_num();
static void   pgaudit_update_auditfile_time(pg_time_t timestamp, bool exist);
static void   pgaudit_switch_next_auditfile();

/********** unified audit ******/
static void pgaudit_send_data_to_elastic();
static void pgaudit_query_file_for_elastic();
static void elasic_search_connection_test();
static void policy_auditfile_rotate();
static void set_next_policy_rotation_time(void);
static void pgaudit_write_policy_audit_file(const char* buffer, int count);

inline bool pgaudit_need_check_time_rotation()
{
    return (u_sess->attr.attr_security.Audit_RotationAge > 0 && !t_thrd.audit.rotation_disabled);
}
inline bool pgaudit_need_check_size_rotation()
{
    return (!t_thrd.audit.rotation_requested && u_sess->attr.attr_security.Audit_RotationSize > 0 &&
            !t_thrd.audit.rotation_disabled);
}

/********** toughness *********/
static void CheckAuditFile(void);
static bool pgaudit_invalid_header(const AuditMsgHdr* header);
static void pgaudit_mark_corrupt_info(uint32 fnum);
static void audit_append_xid_info(const char *detail_info, char *detail_info_xid, uint32 len);
static bool audit_status_check_ok();

static void init_audit_signal_handlers()
{
    (void)gspqsignal(SIGHUP, sigHupHandler); /* set flag to read config file */
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, SIG_IGN);
    (void)gspqsignal(SIGQUIT, pgaudit_exit);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, sigUsr1Handler); /* request audit rotation */
    (void)gspqsignal(SIGUSR2, SIG_IGN);

    /* Reset some signals that are accepted by postmaster but not here */
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);
    (void)gspqsignal(SIGURG, print_stack);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();
}

static void pgaudit_handle_exception()
{
    /* Since not using PG_TRY, must reset error stack by hand */
    t_thrd.log_cxt.error_context_stack = NULL;
    t_thrd.log_cxt.call_stack = NULL;

    /* Prevents interrupts while cleaning up */
    HOLD_INTERRUPTS();

    /* Report the error to the server log */
    EmitErrorReport();

    ereport(LOG, (errmsg("auditor thread with thread index : %d shutdown abnormaly", t_thrd.audit.cur_thread_idx)));

    (void)MemoryContextSwitchTo(t_thrd.mem_cxt.pgAuditLocalContext);
    FlushErrorState();

    /* Flush any leaked data in the top-level context */
    MemoryContextResetAndDeleteChildren(t_thrd.mem_cxt.pgAuditLocalContext);

    LWLockReleaseAll();
    if (t_thrd.utils_cxt.CurrentResourceOwner) {
        ResourceOwnerRelease(t_thrd.utils_cxt.CurrentResourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
    }

    /* Now we can allow interrupts again */
    RESUME_INTERRUPTS();

    /*
     * Sleep at least 1 second after any error.  A write error is likely
     * to be repeated, and we don't want to be filling the error logs as
     * fast as we can.
     */
    pg_usleep(1000000L);
    return;
}

static void sig_thread_quit_handler()
{
    /*
     * audit master thread should try its best to do the flush job when exit:
     * 1. wait other audit thread exit, so that index file will not change
     * 2. flush index audit file
     */
    if (t_thrd.audit.cur_thread_idx == 0) {
        while (t_thrd.audit.need_exit) {
            if (pg_atomic_read_u32(&g_instance.audit_cxt.current_audit_index) == 1) {
                break;
            }
        }
        pgaudit_cleanup();
        pgaudit_update_indexfile(PG_BINARY_W, true);
    }

    /* audit master thread should exit last as expected */
    ereport(LOG, (errmsg("auditor thread exit, id : %d", t_thrd.audit.cur_thread_idx)));

    /*
     * From here on, elog(ERROR) should end with exit(1), not send
     * control back to the sigsetjmp block above.
     */
    u_sess->attr.attr_common.ExitOnAnyError = true;
}

static void sig_thread_config_handler(int &currentAuditRotationAge, int &currentAuditRemainThreshold)
{
    ProcessConfigFile(PGC_SIGHUP);

    /*
     * If rotation time parameter changed, reset next rotation time,
     * but don't immediately force a rotation.
     */
    if (currentAuditRotationAge != u_sess->attr.attr_security.Audit_RotationAge) {
        currentAuditRotationAge = u_sess->attr.attr_security.Audit_RotationAge;
        set_next_rotation_time();
    }

    /* If file remain threshold parameter changed, reset audit index table */
    if (t_thrd.audit.cur_thread_idx == 0 &&
        currentAuditRemainThreshold != u_sess->attr.attr_security.Audit_RemainThreshold) {
        currentAuditRemainThreshold = u_sess->attr.attr_security.Audit_RemainThreshold;

        /* the audit index table may be dirty, so update index table first */
        pgaudit_update_indexfile(PG_BINARY_W, true);

        /* reset the audit index table */
        pgaudit_reset_indexfile();
    }

    /*
     * If we had a rotation-disabling failure, re-enable rotation
     * attempts after SIGHUP, and force one immediately.
     */
    if (t_thrd.audit.rotation_disabled) {
        t_thrd.audit.rotation_disabled = false;
        t_thrd.audit.rotation_requested = true;
    }
}
/*
 * Main entry point for auditor process
 * argc/argv parameters are valid only in EXEC_BACKEND case.
 */
NON_EXEC_STATIC void PgAuditorMain()
{
    char auditbuffer[READ_BUF_SIZE + 1] = {0};
    int bytes_in_auditbuffer = 0;
    int currentAuditRotationAge;
    int currentAuditRemainThreshold;
    pg_time_t now;

    IsUnderPostmaster = true; /* we are a postmaster subprocess now */

    t_thrd.proc_cxt.MyProcPid = gs_thread_self(); /* reset t_thrd.proc_cxt.MyProcPid */

    t_thrd.proc_cxt.MyStartTime = time(NULL); /* set our start time in case we call elog */
    now = t_thrd.proc_cxt.MyStartTime;

    t_thrd.role = AUDITOR;

    /* get thread index here, index 0 is audit master thread as default */
    (void)audit_load_thread_index();
    init_ps_display("auditor process", "", "", "");

    /* Initialize private latch for use by signal handlers */
    InitLatch(&t_thrd.audit.sysAuditorLatch);

    /*
     * Properly accept or ignore signals the postmaster might send us
     *
     * Note: we ignore all termination signals, and instead exit only when all
     * upstream processes are gone, to ensure we don't miss any dying gasps of
     * broken backends...
     */
    init_audit_signal_handlers();
    (void)gspqsignal(SIGURG, print_stack);

    if (t_thrd.mem_cxt.pgAuditLocalContext == NULL)
        t_thrd.mem_cxt.pgAuditLocalContext = AllocSetContextCreate(t_thrd.top_mem_cxt,
            "audit memory context",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE * 3,
            ALLOCSET_DEFAULT_MAXSIZE * 3);

    on_shmem_exit(pgauditor_kill, (Datum)0);
    (void)MemoryContextSwitchTo(t_thrd.mem_cxt.pgAuditLocalContext);

    /* If an exception is encountered, processing resumes here. */
    sigjmp_buf local_sigjmp_buf;
    int curTryCounter;
    int* oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);
        pgaudit_handle_exception();
    }
    oldTryCounter = gstrace_tryblock_entry(&curTryCounter);
    t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf; /* We can now handle ereport(ERROR) */


    /* remember active auditfile parameters */
    currentAuditRotationAge = u_sess->attr.attr_security.Audit_RotationAge;
    currentAuditRemainThreshold = u_sess->attr.attr_security.Audit_RemainThreshold;

    /* audit master thread */
    if (t_thrd.audit.cur_thread_idx == 0) {
        m_curlUtils.initialize(false, "", "", "");
        elasic_search_connection_test();
        audit_process_cxt_init();
    } else {
        /* for sub audit threads, they should wait for master thread after init finished */
        while (g_instance.audit_cxt.audit_init_done == 0) {
            if (pg_atomic_read_u32(&g_instance.audit_cxt.audit_init_done) == 1) {
                break;
            }
        }
    }

    /* set next planned rotation time */
    set_next_rotation_time();

    auditfile_init();

    /* main worker loop */
    for (;;) {
        bool time_based_rotation = false;
        bool size_based_rotation = false;
        long cur_timeout;
        unsigned int cur_flags;
        unsigned int rc = 0;

        /* Clear any already-pending wakeups */
        ResetLatch(&t_thrd.audit.sysAuditorLatch);

        /*
         * Quit if we get SIGQUIT from the postmaster.
         */
        if (t_thrd.audit.need_exit) {
            sig_thread_quit_handler();
            break;
        }

        if (t_thrd.audit.cur_thread_idx == 0) {
            policy_auditfile_rotate();
            pgaudit_send_data_to_elastic();
        }

        /*
         * Process any requests or signals received recently.
         */
        if (t_thrd.audit.got_SIGHUP) {
            t_thrd.audit.got_SIGHUP = false;
            sig_thread_config_handler(currentAuditRotationAge, currentAuditRemainThreshold);
        }

        if (pgaudit_need_check_time_rotation()) {
            /* Do a auditfile rotation if it's time */
            now = (pg_time_t)time(NULL);
            if (now >= t_thrd.audit.next_rotation_time)
                t_thrd.audit.rotation_requested = time_based_rotation = true;
        }

        if (pgaudit_need_check_size_rotation()) {
            int64 filesize = (t_thrd.audit.sysauditFile != NULL) ? ftell(t_thrd.audit.sysauditFile) : 0;
            /* Do a rotation if file is too big */
            if (filesize >= (int64)u_sess->attr.attr_security.Audit_RotationSize * 1024L ||
                filesize >= (int64)u_sess->attr.attr_security.Audit_SpaceLimit * 1024L) {
                t_thrd.audit.rotation_requested = size_based_rotation = true;
            }
        }

        if (t_thrd.audit.rotation_requested) {
            /*
             * Force rotation when both values are zero. It means the request
             * was sent by pg_rotate_auditfile.
             */
            if (!time_based_rotation && !size_based_rotation) {
                size_based_rotation = true;
            }
            auditfile_rotate(time_based_rotation, size_based_rotation);
        }

        if (t_thrd.audit.cur_thread_idx == 0) {
            pgaudit_cleanup();
        }

        /*
         * Calculate time till next time-based rotation, so that we don't
         * sleep longer than that.  We assume the value of "now" obtained
         * above is still close enough.  Note we can't make this calculation
         * until after calling auditfile_rotate(), since it will advance
         * t_thrd.audit.next_rotation_time.
         */
        if (u_sess->attr.attr_security.Audit_RotationAge > 0 && !t_thrd.audit.rotation_disabled) {
            if (now < t_thrd.audit.next_rotation_time)
                cur_timeout = (t_thrd.audit.next_rotation_time - now) * 1000L; /* msec */
            else
                cur_timeout = 0;

            cur_flags = WL_TIMEOUT;
        } else {
            cur_timeout = -1L;
            cur_flags = 0;
        }

        /*
         * Sleep until there's something to do
         */
        rc = WaitLatchOrSocket(&t_thrd.audit.sysAuditorLatch, WL_LATCH_SET | WL_SOCKET_READABLE | cur_flags,
                               g_instance.audit_cxt.sys_audit_pipes[PIPE_READ_INDEX(t_thrd.audit.cur_thread_idx)],
                               cur_timeout);
        if (rc & WL_SOCKET_READABLE) {
            int bytesRead = read(g_instance.audit_cxt.sys_audit_pipes[PIPE_READ_INDEX(t_thrd.audit.cur_thread_idx)],
                                 auditbuffer + bytes_in_auditbuffer, sizeof(auditbuffer) - bytes_in_auditbuffer - 1);
            if (bytesRead > 0) {
                /* Check the current audit file */
                CheckAuditFile();
            }
            if (bytesRead < 0) {
                if (errno != EINTR)
                    ereport(LOG, (errcode_for_socket_access(), errmsg("could not read from auditor pipe: %m")));
            } else if (bytesRead > 0) {
                bytes_in_auditbuffer += bytesRead;
                process_pipe_input(auditbuffer, &bytes_in_auditbuffer);
                continue;
            } else {
                /*
                 * Zero bytes read when select() is saying read-ready means
                 * EOF on the pipe: that is, there are no longer any processes
                 * with the pipe write end open.  Therefore, the postmaster
                 * and all backends are shut down, and we are done.
                 */
                t_thrd.audit.pipe_eof_seen = true;

                /* if there's any data left then force it out now */
                flush_pipe_input(auditbuffer, &bytes_in_auditbuffer);
            }
        }

        if (t_thrd.audit.pipe_eof_seen) {
            break;
        }

    }

    ereport(LOG, (errmsg("auditor shutting down")));

    if (t_thrd.audit.sysauditFile) {
        fclose(t_thrd.audit.sysauditFile);
        t_thrd.audit.sysauditFile = NULL;
    }

    /* policy auditing */
    if (t_thrd.audit.policyauditFile) {
        fclose(t_thrd.audit.policyauditFile);
        t_thrd.audit.policyauditFile = NULL;
    }

    /* Release memory, if any was allocated */
    if (t_thrd.mem_cxt.pgAuditLocalContext != NULL) {
        MemoryContextDelete(t_thrd.mem_cxt.pgAuditLocalContext);
        t_thrd.mem_cxt.pgAuditLocalContext = NULL;
    }

    if (t_thrd.audit.cur_thread_idx == 0) {
        m_curlUtils.~CurlUtils();
        audit_process_cxt_exit();
    }

    proc_exit(0);
}

void pgaudit_send_data_to_elastic()
{
    if (IS_PGXC_COORDINATOR && g_instance.attr.attr_security.use_elastic_search) {
        pgaudit_query_file_for_elastic();
    } 
}

/*
 * Start all audit subprocesses
 */
void pgaudit_start_all(void)
{
    if (g_instance.pid_cxt.PgAuditPID == NULL) {
        return;
    }

    for (int i = 0; i < g_instance.audit_cxt.thread_num; ++i) {
        if (g_instance.pid_cxt.PgAuditPID[i] == 0) {
            g_instance.pid_cxt.PgAuditPID[i] = pgaudit_start();
            ereport(LOG, (errmsg("auditor process %d started, pid=%lu", i, g_instance.pid_cxt.PgAuditPID[i])));
        }
    }
}

/*
 * Stop all audit subprocesses
 */
void pgaudit_stop_all(void)
{
    for (int i = 0; i < g_instance.audit_cxt.thread_num; ++i) {
        if (g_instance.pid_cxt.PgAuditPID[i] != 0) {
            Assert(!dummyStandbyMode);
            signal_child(g_instance.pid_cxt.PgAuditPID[i], SIGQUIT, -1);
        }
    }
}

/*
 * Postmaster subroutine to start a sysauditor subprocess.
 */
ThreadId pgaudit_start(void)
{
    ThreadId sysauditorPid;
    if (!u_sess->attr.attr_security.Audit_enabled)
        return 0;

    sysauditorPid = initialize_util_thread(AUDITOR);
    if (sysauditorPid != 0) {
        /* success, in postmaster */
        return (ThreadId)sysauditorPid;
    }

    /* we should never reach here */
    return 0;
}

void allow_immediate_pgaudit_restart(void)
{
    t_thrd.audit.last_pgaudit_start_time = 0;
}

/* --------------------------------
 *        pipe protocol handling
 * --------------------------------
 */
/*
 * Process data received through the sysauditor pipe.
 *
 * This routine interprets the audit pipe protocol which sends audit messages as
 * (hopefully atomic) chunks - such chunks are detected and reassembled here.
 *
 * The protocol has a header that starts with two nul bytes, then has a 16 bit
 * length, the pid of the sending process, and a flag to indicate if it is
 * the last chunk in a message. Incomplete chunks are saved until we read some
 * more, and non-final chunks are accumulated until we get the final chunk.
 *
 * All of this is to avoid 2 problems:
 * . partial messages being written to auditfiles (messes rotation), and
 * . messages from different backends being interleaved (messages garbled).
 *
 * Any non-protocol messages are written out directly. These should only come
 * from non-PostgreSQL sources, however (e.g. third party libraries writing to
 * stderr).
 *
 * auditbuffer is the data input buffer, and *bytes_in_auditbuffer is the number
 * of bytes present.  On exit, any not-yet-eaten data is left-justified in
 * auditbuffer, and *bytes_in_auditbuffer is updated.
 */
static void process_pipe_input(char* auditbuffer, int* bytes_in_auditbuffer)
{
    char* cursor = auditbuffer;
    int count = *bytes_in_auditbuffer;

    /* While we have enough for a header, process data... */
    while (count >= (int)sizeof(PipeProtoHeader)) {
        PipeProtoHeader p;
        int chunklen;
        errno_t errorno = EOK;

        /* Do we have a valid header? */
        errorno = memcpy_s(&p, sizeof(PipeProtoHeader), cursor, sizeof(PipeProtoHeader));
        securec_check(errorno, "\0", "\0");
        if (p.nuls[0] == '\0' && p.nuls[1] == '\0' && p.len > 0 && p.len <= PIPE_MAX_PAYLOAD && p.pid != 0 &&
            (p.is_last == 't' || p.is_last == 'f')) {
            List* buffer_list = NULL;
            ListCell* cell = NULL;
            save_buffer* existing_slot = NULL;
            save_buffer* free_slot = NULL;
            StringInfo str;
            chunklen = PIPE_HEADER_SIZE + p.len;
            /* Fall out of loop if we don't have the whole chunk yet */
            if (count < chunklen) {
                break;
            }
            /* Locate any existing buffer for this source pid */
            buffer_list = t_thrd.audit.buffer_lists[p.pid % NBUFFER_LISTS];
            foreach (cell, buffer_list) {
                save_buffer* buf = (save_buffer*)lfirst(cell);

                if (buf->pid == p.pid) {
                    existing_slot = buf;
                    break;
                }

                if (buf->pid == 0 && free_slot == NULL) {
                    free_slot = buf;
                }
            }

            if (p.is_last == 'f') {
                /*
                 * Save a complete non-final chunk in a per-pid buffer
                 */
                if (existing_slot != NULL) {
                    /* Add chunk to data from preceding chunks */
                    str = &(existing_slot->data);
                    appendBinaryStringInfo(str, cursor + PIPE_HEADER_SIZE, p.len);
                } else {
                    /* First chunk of message, save in a new buffer */
                    if (free_slot == NULL) {
                        /*
                         * Need a free slot, but there isn't one in the list,
                         * so create a new one and extend the list with it.
                         */
                        free_slot = (save_buffer*)palloc(sizeof(save_buffer));
                        buffer_list = lappend(buffer_list, free_slot);
                        t_thrd.audit.buffer_lists[p.pid % NBUFFER_LISTS] = buffer_list;
                    }
                    free_slot->pid = p.pid;
                    str = &(free_slot->data);
                    initStringInfo(str);
                    appendBinaryStringInfo(str, cursor + PIPE_HEADER_SIZE, p.len);
                }
            } else {
                /*
                 * Final chunk --- add it to anything saved for that pid, and
                 * either way write the whole thing out.
                 */
                if (existing_slot != NULL) {
                    str = &(existing_slot->data);
                    appendBinaryStringInfo(str, cursor + PIPE_HEADER_SIZE, p.len);
                    pgaudit_write_file(str->data, str->len);
                    pgaudit_write_policy_audit_file(str->data, str->len);

                    /* Mark the buffer unused, and reclaim string storage */
                    existing_slot->pid = 0;
                    pfree(str->data);
                } else {
                    /* The whole message was one chunk, evidently. */
                    pgaudit_write_file(cursor + PIPE_HEADER_SIZE, p.len);
                    pgaudit_write_policy_audit_file(cursor + PIPE_HEADER_SIZE, p.len);
                }
            }

            /* Finished processing this chunk */
            cursor += chunklen;
            count -= chunklen;
        } else {
            /* Process non-protocol data */

            /*
             * Look for the start of a protocol header.  If found, dump data
             * up to there and repeat the loop.  Otherwise, dump it all and
             * fall out of the loop.  (Note: we want to dump it all if at all
             * possible, so as to avoid dividing non-protocol messages across
             * auditfiles.  We expect that in many scenarios, a non-protocol
             * message will arrive all in one read(), and we want to respect
             * the read() boundary if possible.)
             */
            for (chunklen = 1; chunklen < count; chunklen++) {
                if (cursor[chunklen] == '\0') {
                    break;
                }
            }
            /* fall back on the stderr audit as the destination */
            pgaudit_write_file(cursor, chunklen);
            pgaudit_write_policy_audit_file(cursor, chunklen);
            cursor += chunklen;
            count -= chunklen;
        }
    }

    /* We don't have a full chunk, so left-align what remains in the buffer */
    if (count > 0 && cursor != auditbuffer) {
        errno_t errorno;
        errorno = memmove_s(auditbuffer, READ_BUF_SIZE, cursor, count);
        securec_check(errorno, "\0", "\0");
    }
    *bytes_in_auditbuffer = count;
}

/*
 * Force out any buffered data
 *
 * This is currently used only at sysauditor shutdown, but could perhaps be
 * useful at other times, so it is careful to leave things in a clean state.
 */
static void flush_pipe_input(char* auditbuffer, int* bytes_in_auditbuffer)
{
    int i;

    /* Dump any incomplete protocol messages */
    for (i = 0; i < NBUFFER_LISTS; i++) {
        List* list = t_thrd.audit.buffer_lists[i];
        ListCell* cell = NULL;

        foreach (cell, list) {
            save_buffer* buf = (save_buffer*)lfirst(cell);

            if (buf->pid != 0) {
                StringInfo str = &(buf->data);

                pgaudit_write_file(str->data, str->len);
                pgaudit_write_policy_audit_file(str->data, str->len);
                /* Mark the buffer unused, and reclaim string storage */
                buf->pid = 0;
                pfree(str->data);
            }
        }
    }

    /*
     * Force out any remaining pipe data as-is; we don't bother trying to
     * remove any protocol headers that may exist in it.
     */
    if (*bytes_in_auditbuffer > 0) {
        pgaudit_write_file(auditbuffer, *bytes_in_auditbuffer);
        pgaudit_write_policy_audit_file(auditbuffer, *bytes_in_auditbuffer);
    }
    *bytes_in_auditbuffer = 0;
}

/* --------------------------------
 *        auditfile routines
 * --------------------------------
 */
/*
 * Write data to the currently open auditfile
 *
 * This is exported so that elog.c can call it when t_thrd.audit.am_sysauditor is true.
 * This allows the sysauditor process to record elog messages of its own,
 * even though its stderr does not point at the sysaudit pipe.
 */
static void pgaudit_write_file(char* buffer, int count)
{
    int rc;
    pg_time_t curtime;
    errno_t errorno = EOK;

    if (buffer == NULL || t_thrd.audit.sysauditFile == NULL)
        return;

    curtime = time(NULL);
    errorno = memcpy_s(buffer + offsetof(AuditMsgHdr, time), READ_BUF_SIZE - offsetof(AuditMsgHdr, time), &curtime,
        sizeof(pg_time_t));
    securec_check(errorno, "\0", "\0");
    errorno = memcpy_s(
        buffer + offsetof(AuditMsgHdr, size), READ_BUF_SIZE - offsetof(AuditMsgHdr, size), &count, sizeof(uint32));
    securec_check(errorno, "\0", "\0");

    errno = 0;
     
    /* if record time is earlier than current file's create time,
     * create a new audit file to avoid the confusion caused by system clock change */
    FILE* fh = NULL;
    if (g_instance.audit_cxt.audit_indextbl) {
        AuditIndexItem *cur_item =
        g_instance.audit_cxt.audit_indextbl->data +
        g_instance.audit_cxt.audit_indextbl->curidx[t_thrd.audit.cur_thread_idx];
        if (curtime < cur_item->ctime) {
            auditfile_close(SYSAUDITFILE_TYPE);
            fh = auditfile_open((pg_time_t)time(NULL), "a", true);
            if (fh != NULL) {
                t_thrd.audit.sysauditFile = fh;
            }
        }
    }

retry1:
    rc = fwrite(buffer, 1, count, t_thrd.audit.sysauditFile);

    if (rc != count) {
        /*
         * If no disk space, we will retry, and we can not report a log as
         * there is not space to write.
         */
        if (errno == ENOSPC) {
            ereport(WARNING, (errmsg("No free space left on audit disk.")));
            pg_usleep(1000000);
            goto retry1;
        }
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to audit file: %m")));
    }
    /*
     * The contents of the audit logfile haven't newline which is difference from syslog, so
     * LBF_MODE set by setvbuf can't make sure the write buffer be fflushed into the logfile
     * immediately. We need call the fflush function here by ourself to make sure this.
     * NOTICE : in some version of glibc, ftell have the flush feature built-in but it's not
     * standard practice to rely ftell to flush, so fflush here is the most assured.
     */
    (void)fflush(t_thrd.audit.sysauditFile);
}

static void pgaudit_write_policy_audit_file(const char* buffer, int count)
{
    /* flush policy audit info only if elastic system configure */
    if (!IS_PGXC_COORDINATOR || !g_instance.attr.attr_security.use_elastic_search) {
        return;
    }

    if (t_thrd.audit.policyauditFile == NULL) {
        return;
    }
/* temporary duble writing to policy auditing file */
retry:
    int rc = fwrite(buffer, 1, count, t_thrd.audit.policyauditFile);
    if (rc != count) {
        /*
         * If no disk space, we will retry, and we can not report a log as
         * there is not space to write.
         */
        if (errno == ENOSPC) {
            ereport(WARNING, (errmsg("No free space left on audit disk.")));
            pg_usleep(1000000);
            goto retry;
        }
    }

    (void)fflush(t_thrd.audit.policyauditFile);
}

/*
 * Brief        : initialize the audit file.
 * Description  : set parameter allow_erros as error level, do not allow error as default
 */
static void auditfile_init(bool allow_errors)
{
    /*
     * The initial auditfile is created right in the postmaster, to verify that
     * the Audit_directory is writable.
     */
    if (!t_thrd.audit.sysauditFile) {
        t_thrd.audit.sysauditFile = auditfile_open(time(NULL), "a", allow_errors);
        if (t_thrd.audit.sysauditFile != NULL) {
            /* trigger one internal event in traditional audit log */
            audit_report(AUDIT_INTERNAL_EVENT, AUDIT_OK, "file", "create a new audit file");
        }
    }
    if (!t_thrd.audit.policyauditFile && IS_PGXC_COORDINATOR && (t_thrd.audit.cur_thread_idx == 0)) {
        t_thrd.audit.policyauditFile = auditfile_open(time(NULL), "a", allow_errors, policy_audit_filename, true);
    }
}

/*
 * Brief        : open a new audit file.
 * Description    :
 *         Open a new auditfile with proper permissions and buffering options.
 *
 *         If allow_errors is true, we just audit any open failure and return NULL
 *         (with errno still correct for the fopen failure).
 *         Otherwise, errors are treated as fatal.
 */
static FILE *auditfile_open(pg_time_t timestamp, const char *mode, bool allow_errors, const char *_filename,
    bool ignore_num)
{
    FILE       *fh = NULL;
    char* filename = NULL;
    uint32    fnum = 0;
    int thread_idx = t_thrd.audit.cur_thread_idx;

    struct stat st;
    bool exist = false;
    if (!ignore_num && g_instance.audit_cxt.audit_indextbl) {
        fnum = pgaudit_get_auditfile_num();
    }
    ereport(DEBUG1, (errmsg("audit thread idx: %d auditfile_open ok fnum : %d", thread_idx, fnum)));

    filename = (char*)palloc(MAXPGPATH);
    int rc = snprintf_s(
        filename, MAXPGPATH, MAXPGPATH - 1, _filename, g_instance.attr.attr_security.Audit_directory, fnum);
    securec_check_intval(rc,, NULL);

    /*
     * Note we do not let pgaudit_filemode disable IWUSR, since we certainly want
     * to be able to write the files ourselves.
     */
    if (stat(filename, &st) == 0)
        exist = true;
    fh = fopen(filename, mode);
    if (fh != NULL) {
        setvbuf(fh, NULL, LBF_MODE, 0);
        if (!ignore_num && g_instance.audit_cxt.audit_indextbl) {
            pgaudit_update_auditfile_time(timestamp, exist);
            pgaudit_update_indexfile(PG_BINARY_W, true);
        }
    } else {
        int save_errno = errno;

        ereport(allow_errors ? LOG : FATAL,
            (errcode_for_file_access(), errmsg("could not open audit file \"%s\": %m", filename)));
        errno = save_errno;
    }

    if (!exist) {
        if (chmod(filename, S_IWUSR | (mode_t)pgaudit_filemode) < 0) {
            int save_errno = errno;

            ereport(allow_errors ? LOG : FATAL,
                (errcode_for_file_access(), errmsg("could not chmod audit file \"%s\": %m", filename)));
            errno = save_errno;
        }
    }

    pfree(filename);
    return fh;
}

/*
 * Brief        : close the audit file.
 * Description    :
 */
static void auditfile_close(AuditFileType flag)
{
    if (t_thrd.audit.sysauditFile == NULL)
        return;

    if ((flag == SYSAUDITFILE_TYPE) && g_instance.audit_cxt.audit_indextbl != NULL) {
        /* switch to next audit file */
        pgaudit_switch_next_auditfile();
        pgaudit_update_indexfile(PG_BINARY_W, true);
    }
    if (flag == SYSAUDITFILE_TYPE) {
        fclose(t_thrd.audit.sysauditFile);
        t_thrd.audit.sysauditFile = NULL;
    }
    if (flag == POLICYAUDITFILE_TYPE) {
        fclose(t_thrd.audit.policyauditFile);
        t_thrd.audit.policyauditFile = NULL;
    }

    return;
}

static void policy_auditfile_rotate()
{
    if (t_thrd.audit.policyauditFile == NULL)
        return;
    pg_time_t   fntime = (pg_time_t) time(NULL);
    int rc;
    /*
     * When doing a time-based rotation, invent the new auditfile name based on
     * the planned rotation time, not current time, to avoid "slippage" in the
     * file name when we don't do the rotation immediately.
     */
    int64 filesize = ftell(t_thrd.audit.policyauditFile);
    if ((fntime + PolicyAudit_RotationAge)  > policy_next_rotation_time && filesize > 0) {
        auditfile_close(POLICYAUDITFILE_TYPE);
        char oldname[MAXPGPATH] = {0};
        // current file
        rc = snprintf_s(oldname, sizeof(oldname), sizeof(oldname) - 1, "%s" DIR_SEP "0_event.bin",
            g_instance.attr.attr_security.Audit_directory);
        securec_check_ss(rc, "\0", "\0");
        char newfile[MAXPGPATH] = {0};
        // new rotate file
        rc = snprintf_s(newfile, sizeof(oldname), sizeof(oldname) - 1, "%s" DIR_SEP "done" DIR_SEP "%lld_event.bin",
            g_instance.attr.attr_security.Audit_directory, (long long)fntime);
        securec_check_ss(rc, "\0", "\0");
        if (rename(oldname, newfile) != 0) {
            ereport(LOG, (errmsg("can't rename \"%s\" to \"%s\": %m", oldname, newfile)));
        }
        FILE* fh = auditfile_open(fntime, "a", true, policy_audit_filename, true);

        if (fh == NULL) {
            /*
             * ENFILE/EMFILE are not too surprising on a busy system; just
             * keep using the old file till we manage to get a new one.
             * Otherwise, assume something's wrong with Audit_directory and stop
             * trying to create files.
             */
            if (errno != ENFILE && errno != EMFILE) {
                ereport(LOG,
                        (errmsg("disabling automatic rotation (use SIGHUP to re-enable)")));
            }
            return;
        }
        t_thrd.audit.policyauditFile = fh;
        set_next_policy_rotation_time();
    }
}

/*
 * Brief        : perform audit file rotation
 * Description    :
 */
static void auditfile_rotate(bool time_based_rotation, bool size_based_rotation)
{
    pg_time_t fntime;
    FILE* fh = NULL;
    t_thrd.audit.rotation_requested = false;
    /*
     * When doing a time-based rotation, invent the new auditfile name based on
     * the planned rotation time, not current time, to avoid "slippage" in the
     * file name when we don't do the rotation immediately.
     */
    if (time_based_rotation)
        fntime = t_thrd.audit.next_rotation_time;
    else
        fntime = time(NULL);

    if (time_based_rotation || size_based_rotation) {
        auditfile_close(SYSAUDITFILE_TYPE);
        fh = auditfile_open(fntime, "a", true);
        if (fh == NULL) {
            /*
             * ENFILE/EMFILE are not too surprising on a busy system; just
             * keep using the old file till we manage to get a new one.
             * Otherwise, assume something's wrong with Audit_directory and stop
             * trying to create files.
             */
            if (errno != ENFILE && errno != EMFILE) {
                ereport(LOG, (errmsg("disabling automatic rotation (use SIGHUP to re-enable)")));
                t_thrd.audit.rotation_disabled = true;
            }
            return;
        }
        t_thrd.audit.sysauditFile = fh;
        audit_report(AUDIT_INTERNAL_EVENT, AUDIT_OK, "file", "create a new audit file");
    }
    set_next_rotation_time();
}

static void set_next_policy_rotation_time(void)
{
    if (PolicyAudit_RotationAge <= 0) {
        return;
    }
    policy_next_rotation_time = (pg_time_t) time(NULL);
    policy_next_rotation_time += PolicyAudit_RotationAge;
}

/*
 * Determine the next planned rotation time, and store in t_thrd.audit.next_rotation_time.
 */
static void set_next_rotation_time(void)
{
    pg_time_t now;
    struct pg_tm* tm = NULL;
    int rotinterval;
    /* nothing to do if time-based rotation is disabled */
    if (u_sess->attr.attr_security.Audit_RotationAge <= 0)
        return;
    /*
     * The requirements here are to choose the next time > now that is a
     * "multiple" of the audit rotation interval.  "Multiple" can be interpreted
     * fairly loosely.    In this version we align to audit_timezone rather than
     * GMT.
     */
    rotinterval = u_sess->attr.attr_security.Audit_RotationAge * SECS_PER_MINUTE; /* convert to seconds */
    now = (pg_time_t)time(NULL);
    tm = pg_localtime(&now, log_timezone);
    if (tm == NULL) {
        ereport(ERROR, (errmsg("pg_localtime must not be null!")));
    }
    now += tm->tm_gmtoff;
    now -= now % rotinterval;
    now += rotinterval;
    now -= tm->tm_gmtoff;
    t_thrd.audit.next_rotation_time = now;
}

void pgaudit_gen_auditfile_warning(pg_time_t remain_time, uint4 filesize)
{
    if ((u_sess->attr.attr_security.Audit_CleanupPolicy || remain_time == 0) &&
        (g_instance.audit_cxt.pgaudit_totalspace + filesize >=
         (uint64)u_sess->attr.attr_security.Audit_SpaceLimit * 1024L))
#ifdef HAVE_LONG_LONG_INT
        ereport(WARNING, (errmsg("audit file total space(%lld B) exceed guc parameter(audit_space_limit: %d KB)",
                                 (long long int)(g_instance.audit_cxt.pgaudit_totalspace + filesize),
                                 u_sess->attr.attr_security.Audit_SpaceLimit)));
#else
            ereport(WARNING,
                (errmsg("audit file total space(%ld B) exceed guc parameter(audit_space_limit: %d KB)",
                    g_instance.audit_cxt.pgaudit_totalspace + filesize),
                    u_sess->attr.attr_security.Audit_SpaceLimit)));
#endif
    else if (u_sess->attr.attr_security.Audit_CleanupPolicy == 0 && remain_time &&
             (g_instance.audit_cxt.pgaudit_totalspace + filesize >=
              (uint64)u_sess->attr.attr_security.Audit_SpaceLimit * 1024L))
#ifdef HAVE_LONG_LONG_INT
        ereport(WARNING, (errmsg("Based on time-priority policy, the oldest audit file is beyond %d days or "
                                 "audit file total space(%lld B) exceed guc parameter(audit_space_limit: %d KB)",
                                 u_sess->attr.attr_security.Audit_RemainAge,
                                 (long long int)(g_instance.audit_cxt.pgaudit_totalspace + filesize),
                                 u_sess->attr.attr_security.Audit_SpaceLimit)));
#else
        ereport(WARNING, (errmsg("Based on time-priority policy, the oldest audit file is beyond %d days or "
                                 "audit file total space(%ld B) exceed guc parameter(audit_space_limit: %d KB)",
                                 u_sess->attr.attr_security.Audit_RemainAge,
                                 (g_instance.audit_cxt.pgaudit_totalspace + filesize),
                                 u_sess->attr.attr_security.Audit_SpaceLimit)));
#endif
    if (g_instance.audit_cxt.audit_indextbl->count > (uint32)u_sess->attr.attr_security.Audit_RemainThreshold)
        ereport(WARNING,
                (errmsg("audit file total count(%u) exceed guc parameter(audit_file_remain_threshold: %d)",
                        g_instance.audit_cxt.audit_indextbl->count, u_sess->attr.attr_security.Audit_RemainThreshold)));
    ereport(WARNING, (errmsg("%s", t_thrd.audit.pgaudit_filepath)));
}

bool should_keep_basedon_timepolicy(pg_time_t remain_time, uint4 filesize, uint32 index, const AuditIndexItem *item)
{
    if (g_instance.audit_cxt.audit_indextbl->count <= (uint32)u_sess->attr.attr_security.Audit_RemainThreshold &&
        remain_time && (g_instance.audit_cxt.pgaudit_totalspace + filesize <= SPACE_MAXIMUM_SIZE)) {

        /* As current audit log rotation policy is based on time, just give the warning here for toatl space */
        if ((uint64)(g_instance.audit_cxt.pgaudit_totalspace + filesize -
                     (uint64)u_sess->attr.attr_security.Audit_SpaceLimit * 1024L) >= t_thrd.audit.space_beyond_size) {
            ereport(WARNING,
                    (errmsg("audit file total space(%lld B) exceed guc parameter(audit_space_limit: %d KB) about %d MB",
                            (long long int)(g_instance.audit_cxt.pgaudit_totalspace + filesize),
                            u_sess->attr.attr_security.Audit_SpaceLimit,
                            (int)(t_thrd.audit.space_beyond_size / (1024 * 1024)))));

            t_thrd.audit.space_beyond_size += SPACE_INTERVAL_SIZE;
        }

        /* get the current && next item to estimate time-based policy */
        AuditIndexItem *next =
            g_instance.audit_cxt.audit_indextbl->data + ((index + 1) % g_instance.audit_cxt.audit_indextbl->maxnum);
        if (remain_time >= (g_instance.audit_cxt.audit_indextbl->last_audit_time - item->ctime) ||
            (next && (remain_time > (g_instance.audit_cxt.audit_indextbl->last_audit_time - next->ctime)))) {
            ereport(WARNING, (errmsg("should_keep_basedon_timepolicy not removed as in remain time")));
            return true;
        }
    }

    return false;
}

/*
 * pgaudit_cleanup
 *
 * Check audit data cleanup condition and delete old audit file then returns
 */
static void pgaudit_cleanup(void)
{
    uint32 index = 0;
    AuditIndexItem* item = NULL;
    bool truncated = false;
    LWLockAcquire(AuditIndexFileLock, LW_EXCLUSIVE);
    if (g_instance.audit_cxt.audit_indextbl == NULL) {
        LWLockRelease(AuditIndexFileLock);
        return;
    }

    pg_time_t remain_time = (int64)u_sess->attr.attr_security.Audit_RemainAge * SECS_PER_DAY;  // how many seconds
    uint64 filesize = u_sess->attr.attr_security.Audit_RotationSize * g_instance.audit_cxt.thread_num *
                      1024L;  // filesize for current writting files

    index = g_instance.audit_cxt.audit_indextbl->begidx;
    while (g_instance.audit_cxt.pgaudit_totalspace + filesize >=
        ((uint64)u_sess->attr.attr_security.Audit_SpaceLimit * 1024L) ||
        g_instance.audit_cxt.audit_indextbl->count > (uint32)u_sess->attr.attr_security.Audit_RemainThreshold) {
        errno_t errorno = EOK;
        item = g_instance.audit_cxt.audit_indextbl->data + index;
        uint32 fnum = item->filenum;
        /* to check how long the audit file is remained:
         * a. it must be time-based policy and the specified value is valid;
         * b. the remained time of oldest audit file is beyond the specified value;
         * c. the total size is not beyond the maximum space size.
         */
        if (u_sess->attr.attr_security.Audit_CleanupPolicy == 0 &&
            should_keep_basedon_timepolicy(remain_time, filesize, index, item)) {
            break;
        }

        /* trunate audit file */
        int rc = snprintf_s(t_thrd.audit.pgaudit_filepath, MAXPGPATH, MAXPGPATH - 1, pgaudit_filename,
            g_instance.attr.attr_security.Audit_directory, item->filenum);
        securec_check_intval(rc, , );
        struct stat statbuf;
        if (stat(t_thrd.audit.pgaudit_filepath, &statbuf) == 0 && unlink(t_thrd.audit.pgaudit_filepath) < 0) {
            ereport(WARNING, (errmsg("could not remove audit file: %m")));
            break;
        }
        truncated = true;
        pgaudit_gen_auditfile_warning(remain_time, filesize);

        /* update index file object As curretn audit file is removed */
        if (g_instance.audit_cxt.pgaudit_totalspace >= item->filesize) {
            g_instance.audit_cxt.pgaudit_totalspace -= item->filesize;
        }
        if (g_instance.audit_cxt.audit_indextbl->count > 0) {
            --g_instance.audit_cxt.audit_indextbl->count;
        }
        errorno = memset_s(item, sizeof(AuditIndexItem), 0, sizeof(AuditIndexItem));
        securec_check(errorno, "\0", "\0");

        /* generate audit info for removing an audit file, we only do this thing under auditor thread */
        if (t_thrd.role != AUDITOR) {
            rc = snprintf_truncated_s(t_thrd.audit.pgaudit_filepath, MAXPGPATH, "remove an audit file(number: %u)",
                                      fnum);
            securec_check_ss(rc, "\0", "\0");
            audit_report(AUDIT_INTERNAL_EVENT, AUDIT_OK, "file", t_thrd.audit.pgaudit_filepath);
        }
        /* stop till the current writting index */
        uint32 earliest_idx = g_instance.audit_cxt.audit_indextbl->latest_idx - g_instance.audit_cxt.thread_num;
        if (index == earliest_idx) {
            break;
        }

        /* udpate audit index for next loop  */
        g_instance.audit_cxt.audit_indextbl->begidx = (index + 1) % g_instance.audit_cxt.audit_indextbl->maxnum;
        index = g_instance.audit_cxt.audit_indextbl->begidx;
    }
    LWLockRelease(AuditIndexFileLock);

    if (truncated) {
        pgaudit_update_indexfile(PG_BINARY_W, true);
    }
}

/* --------------------------------
 *        signal handler routines
 * --------------------------------
 */
/* SIGQUIT signal handler for auditor process */
static void pgaudit_exit(SIGNAL_ARGS)
{
    int save_errno = errno;
    t_thrd.audit.need_exit = true;
    SetLatch(&t_thrd.audit.sysAuditorLatch);
    errno = save_errno;
}

/* SIGHUP: set flag to reload config file */
static void sigHupHandler(SIGNAL_ARGS)
{
    int save_errno = errno;
    t_thrd.audit.got_SIGHUP = true;
    SetLatch(&t_thrd.audit.sysAuditorLatch);
    errno = save_errno;
}

/* SIGUSR1: set flag to rotate auditfile */
static void sigUsr1Handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.audit.rotation_requested = true;
    SetLatch(&t_thrd.audit.sysAuditorLatch);

    errno = save_errno;
}

/*
 * Send data to the syslogger using the chunked protocol
 *
 * Note: when there are multiple backends writing into the syslogger pipe,
 * it's critical that each write go into the pipe indivisibly, and not
 * get interleaved with data from other processes.  Fortunately, the POSIX
 * spec requires that writes to pipes be atomic so long as they are not
 * more than PIPE_BUF bytes long.  So we divide long messages into chunks
 * that are no more than that length, and send one chunk per write() call.
 * The collector process knows how to reassemble the chunks.
 *
 * Because of the atomic write requirement, there are only two possible
 * results from write() here: -1 for failure, or the requested number of
 * bytes.  There is not really anything we can do about a failure; retry would
 * probably be an infinite loop, and we can't even report the error usefully.
 * (There is noplace else we could send it!)  So we might as well just ignore
 * the result from write().  However, on some platforms you get a compiler
 * warning from ignoring write()'s result, so do a little dance with casting
 * rc to void to shut up the compiler.
 */
static void write_pipe_chunks(char* data, int len, AuditClassType type)
{
    static volatile uint32 pipe_count = 0;
    int thread_num = g_instance.audit_cxt.thread_num;

    /*
     * for audit process, postgres will send messages to pipes with round-robin
     * for unified audit process, just first pipe will used
     */
    int last_pipe_count = pg_atomic_fetch_add_u32(&pipe_count, 1);
    int cur_pipe_idx = (type == STD_AUDIT_TYPE) ? (((last_pipe_count + thread_num) % thread_num) * 2 + 1) : 1;

    ereport(DEBUG1, (errmsg("write_pipe_chunks for pipe : %d", cur_pipe_idx)));

    PipeProtoChunk p;
    errno_t errorno = EOK;
    int rc;

    Assert(len > 0);

    p.proto.nuls[0] = p.proto.nuls[1] = '\0';
    p.proto.pid = t_thrd.proc_cxt.MyProcPid;

    /* write all but the last chunk */
    while (len > PIPE_MAX_PAYLOAD) {
        p.proto.is_last = 'f';
        p.proto.len = PIPE_MAX_PAYLOAD;
        errorno = memcpy_s(p.proto.data, PIPE_MAX_PAYLOAD, data, PIPE_MAX_PAYLOAD);
        securec_check(errorno, "\0", "\0");
        rc = write(g_instance.audit_cxt.sys_audit_pipes[cur_pipe_idx], &p, PIPE_HEADER_SIZE + PIPE_MAX_PAYLOAD);
        (void)rc;

        data += PIPE_MAX_PAYLOAD;
        len -= PIPE_MAX_PAYLOAD;
    }

    /* write the last chunk */
    p.proto.is_last = 't';
    p.proto.len = len;
    errorno = memcpy_s(p.proto.data, PIPE_MAX_PAYLOAD, data, len);
    securec_check(errorno, "\0", "\0");

    rc = write(g_instance.audit_cxt.sys_audit_pipes[cur_pipe_idx], &p, PIPE_HEADER_SIZE + len);
    if (rc == -1) {
        ereport(ERROR, (errmsg("write into pipe error")));
    }
    (void)rc;
}

/*
 * Brief        : append a string field to a streamed data
 * Description    :
 */
static void appendStringField(StringInfo str, const char* s)
{
    int size = 0;

    if (str == NULL)
        return;

    if (s == NULL)
        appendBinaryStringInfo(str, (char*)&size, sizeof(int));
    else {
        size = strlen(s) + 1;
        appendBinaryStringInfo(str, (char*)&size, sizeof(int));
        appendBinaryStringInfo(str, s, size);
    }
}

static pg_time_t current_timestamp()
{
    struct timeval te;
    gettimeofday(&te, NULL); // get current time
    pg_time_t milliseconds = te.tv_sec * 1000LL + te.tv_usec / 1000; // calculate milliseconds
    return milliseconds;
}

static bool audit_status_check_ok()
{
#ifdef ENABLE_MULTIPLE_NODES
    /* check whether POSTMASTER is running in standby mode */
    if (!u_sess->attr.attr_security.Audit_enabled || (PGSharedMemoryAttached() && t_thrd.postmaster_cxt.HaShmData &&
        (STANDBY_MODE == t_thrd.postmaster_cxt.HaShmData->current_mode ||
        PENDING_MODE == t_thrd.postmaster_cxt.HaShmData->current_mode)))
        return false;
#else
    /* After the standby read function is added, the standby node needs to be audited. */
    if (!u_sess->attr.attr_security.Audit_enabled || (PGSharedMemoryAttached() && t_thrd.postmaster_cxt.HaShmData &&
        PENDING_MODE == t_thrd.postmaster_cxt.HaShmData->current_mode))
        return false;
#endif

    return true;
}

/*
 * check the valid for specific audit type
 */
static bool audit_type_validcheck(AuditType type)
{
    unsigned int type_status = 0;
    switch (type) {
        case AUDIT_LOGIN_SUCCESS:
            type_status = CHECK_AUDIT_LOGIN(SESSION_LOGIN_SUCCESS);
            break;
        case AUDIT_LOGIN_FAILED:
            type_status = CHECK_AUDIT_LOGIN(SESSION_LOGIN_FAILED);
            break;
        case AUDIT_USER_LOGOUT:
            type_status = CHECK_AUDIT_LOGIN(SESSION_LOGOUT);
            break;
        case AUDIT_SYSTEM_START:
        case AUDIT_SYSTEM_STOP:
        case AUDIT_SYSTEM_RECOVER:
        case AUDIT_SYSTEM_SWITCH:
            type_status = (unsigned int)u_sess->attr.attr_security.Audit_ServerAction;
            break;
        case AUDIT_LOCK_USER:
        case AUDIT_UNLOCK_USER:
            type_status  = (unsigned int)u_sess->attr.attr_security.Audit_LockUser;
            break;
        case AUDIT_GRANT_ROLE:
        case AUDIT_REVOKE_ROLE:
            type_status = (unsigned int)u_sess->attr.attr_security.Audit_PrivilegeAdmin;
            break;
        case AUDIT_USER_VIOLATION:
            type_status = (unsigned int)u_sess->attr.attr_security.Audit_UserViolation;
            break;
        case AUDIT_DDL_DATABASE:
            type_status = CHECK_AUDIT_DDL(DDL_DATABASE);
            break;
        case AUDIT_DDL_DIRECTORY:
            type_status = CHECK_AUDIT_DDL(DDL_DIRECTORY);
            break;
        case AUDIT_DDL_TABLESPACE:
            type_status = CHECK_AUDIT_DDL(DDL_TABLESPACE);
            break;
        case AUDIT_DDL_SCHEMA:
            type_status = CHECK_AUDIT_DDL(DDL_SCHEMA);
            break;
        case AUDIT_DDL_USER:
            type_status = CHECK_AUDIT_DDL(DDL_USER);
            break;
        case AUDIT_DDL_TABLE:
            type_status = CHECK_AUDIT_DDL(DDL_TABLE);
            break;
        case AUDIT_DDL_INDEX:
            type_status = CHECK_AUDIT_DDL(DDL_INDEX);
            break;
        case AUDIT_DDL_VIEW:
            type_status = CHECK_AUDIT_DDL(DDL_VIEW);
            break;
        case AUDIT_DDL_EVENT:
            type_status = CHECK_AUDIT_DDL(DDL_EVENT);
            break;
        case AUDIT_DDL_TRIGGER:
            type_status = CHECK_AUDIT_DDL(DDL_TRIGGER);
            break;
        case AUDIT_DDL_FUNCTION:
            type_status = CHECK_AUDIT_DDL(DDL_FUNCTION);
            break;
        case AUDIT_DDL_PACKAGE:
            type_status = CHECK_AUDIT_DDL(DDL_PACKAGE);
            break;
        case AUDIT_DDL_RESOURCEPOOL:
            type_status = CHECK_AUDIT_DDL(DDL_RESOURCEPOOL);
            break;
        case AUDIT_DDL_GLOBALCONFIG:
            type_status = CHECK_AUDIT_DDL(DDL_GLOBALCONFIG);
            break;
        case AUDIT_DDL_WORKLOAD:
            type_status = CHECK_AUDIT_DDL(DDL_WORKLOAD);
            break;
        case AUDIT_DDL_SERVERFORHADOOP:
            type_status = CHECK_AUDIT_DDL(DDL_SERVERFORHADOOP);
            break;
        case AUDIT_DDL_DATASOURCE:
            type_status = CHECK_AUDIT_DDL(DDL_DATASOURCE);
            break;
        case AUDIT_DDL_NODEGROUP:
            type_status = CHECK_AUDIT_DDL(DDL_NODEGROUP);
            break;
        case AUDIT_DDL_ROWLEVELSECURITY:
            type_status = CHECK_AUDIT_DDL(DDL_ROWLEVELSECURITY);
            break;
        case AUDIT_DDL_SYNONYM:
            type_status = CHECK_AUDIT_DDL(DDL_SYNONYM);
            break;
        case AUDIT_DDL_TYPE:
            type_status = CHECK_AUDIT_DDL(DDL_TYPE);
            break;
        case AUDIT_DDL_TEXTSEARCH:
            type_status = CHECK_AUDIT_DDL(DDL_TEXTSEARCH);
            break;
        case AUDIT_DDL_SEQUENCE:
            type_status = CHECK_AUDIT_DDL(DDL_SEQUENCE);
            break;
        case AUDIT_DDL_KEY:
            type_status = CHECK_AUDIT_DDL(DDL_KEY);
            break;
        case AUDIT_DDL_MODEL:
            type_status = CHECK_AUDIT_DDL(DDL_MODEL);
            break;
        case AUDIT_DDL_PUBLICATION_SUBSCRIPTION:
            type_status = CHECK_AUDIT_DDL(DDL_PUBLICATION_SUBSCRIPTION);
            break;
        case AUDIT_DDL_FOREIGN_DATA_WRAPPER:
            type_status = CHECK_AUDIT_DDL(DDL_FOREIGN_DATA_WRAPPER);
            break;
        case AUDIT_DML_ACTION:
            type_status = (unsigned int)u_sess->attr.attr_security.Audit_DML;
            break;
        case AUDIT_DML_ACTION_SELECT:
            type_status = (unsigned int)u_sess->attr.attr_security.Audit_DML_SELECT;
            break;
        case AUDIT_FUNCTION_EXEC:
            type_status = (unsigned int)u_sess->attr.attr_security.Audit_Exec;
            break;
        case AUDIT_SYSTEM_FUNCTION_EXEC:
            type_status = (unsigned int)u_sess->attr.attr_security.audit_system_function_exec;
            break;
        case AUDIT_POLICY_EVENT:
        case MASKING_POLICY_EVENT:
        case SECURITY_EVENT:
        case AUDIT_INTERNAL_EVENT:
            type_status = 1; /* audit these cases by default */
            break;
        case AUDIT_COPY_TO:
        case AUDIT_COPY_FROM:
            type_status = (unsigned int)u_sess->attr.attr_security.Audit_Copy;
            break;
        case AUDIT_SET_PARAMETER:
            type_status = (unsigned int)u_sess->attr.attr_security.Audit_Set;
            break;
        case AUDIT_DDL_SQL_PATCH:
            type_status = CHECK_AUDIT_DDL(DDL_SQL_PATCH);
            break;
        case AUDIT_UNKNOWN_TYPE:
        default:
            type_status = 0;
            ereport(WARNING, (errmsg("unknown audit type, discard it.")));
            break;
    }
    if (audit_check_full_audit_user() && type != AUDIT_UNKNOWN_TYPE) {
        type_status = 1;
    }

    return type_status > 0;
}

/* get all Audit Event Info from Proc Port */
static bool audit_get_clientinfo(AuditType type, const char* object_name, AuditEventInfo &event_info)
{
    /*
     * Note that the number of field should keep the same with PGAUDIT_QUERY_COLS
     * which will make sure the data size will match with fields number. even though the value of field is null
     * it still should be appended to the buf with the size 0.
     */
    if (u_sess->proc_cxt.MyProcPort == NULL) {
        return true;
    }

    char *userid = event_info.userid;
    const char **username = &(event_info.username);
    const char **dbname = &(event_info.dbname);
    const char **appname = &(event_info.appname);
    const char **remotehost = &(event_info.remotehost);
    char *threadid = event_info.threadid;
    char *localport = event_info.localport;
    char *remoteport = event_info.remoteport;

    errno_t errorno = EOK;
    /* append user name information */
    if (u_sess->misc_cxt.CurrentUserName != NULL) {
        *username = u_sess->misc_cxt.CurrentUserName;
    } else {
        *username = u_sess->proc_cxt.MyProcPort->user_name;
    }
    
    /*
     * append user id information, get user id from table as invalid in session
     * not safe when access table when run logout process as not in normal transaction
     */
    Oid useroid = GetCurrentUserId();
    /* not safe dealing with user logout when invalid oid */
    if (type == AUDIT_USER_LOGOUT && !OidIsValid(useroid)) {
        return false;
    }
    if (*username != NULL && useroid == 0) {
        ResourceOwner currentOwner = NULL;
        currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
        ResourceOwner tmpOwner =  ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner, "CheckUserOid",
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_SECURITY));
        t_thrd.utils_cxt.CurrentResourceOwner = tmpOwner;
        useroid = get_role_oid(*username, true);
        ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
        ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_LOCKS, true, true);
        ResourceOwnerRelease(tmpOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
        t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
        ResourceOwnerDelete(tmpOwner);
    }

    errorno = snprintf_s(userid, MAXNUMLEN, MAXNUMLEN - 1, "%u", useroid);
    securec_check_ss(errorno, "", "");

    if (*username == NULL || (**username) == '\0') {
        *username = _("[unknown]");
    }

    /* append dbname, appname and ip information */
    *dbname = u_sess->proc_cxt.MyProcPort->database_name;
    *appname = u_sess->attr.attr_common.application_name;
    *remotehost = u_sess->proc_cxt.MyProcPort->remote_host;
    switch (type) {
        case AUDIT_POLICY_EVENT:
        case MASKING_POLICY_EVENT:
        case SECURITY_EVENT:
            *remotehost = object_name;
            break;
        default:
            break;
    }
    t_thrd.audit.user_login_time = GetCurrentTimestamp();
    errorno = snprintf_s(threadid,
        MAXNUMLEN * 4,
        MAXNUMLEN * 4 - 1,
        "%lu@%ld",
        t_thrd.proc_cxt.MyProcPid,
        t_thrd.audit.user_login_time);
    securec_check_ss(errorno, "\0", "\0");

    int portNum;
    if (IsHAPort(u_sess->proc_cxt.MyProcPort)) {
        portNum = g_instance.attr.attr_network.PoolerPort;
    } else {
        portNum = g_instance.attr.attr_network.PostPortNumber;
    }

    errorno = snprintf_s(localport, MAXNUMLEN, MAXNUMLEN - 1, "%d", portNum);
    securec_check_ss(errorno, "\0", "\0");

    errorno = snprintf_s(remoteport, MAXNUMLEN, MAXNUMLEN - 1, "%s", u_sess->proc_cxt.MyProcPort->remote_port);
    securec_check_ss(errorno, "\0", "\0");

    /* append database name */
    if (*dbname == NULL || (**dbname) == '\0')
        *dbname = _("[unknown]");
    if (*appname == NULL || (**appname) == '\0')
        *appname = _("[unknown]");
    if (*remotehost == NULL || (**remotehost) == '\0')
        *remotehost = _("[unknown]");

    errorno = snprintf_s(event_info.client_info,
        MAX_CONNECTION_INFO_SIZE,
        MAX_CONNECTION_INFO_SIZE - 1,
        "%s@%s",
        *appname,
        *remotehost);
    securec_check_ss(errorno, "\0", "\0");

    return true;
}

/*
 * Brief          : report audit info to the system auditor
 * Description    : called by all backends, the main routines is as below
 *      1. verify type and process to decide whehter to report audit or not
 *      2. get all audit info from connection
 *      3. append audit info to string buffer
 *      4. last, write audit file or send to auditor process to deal with 
 * the fileds are arraged as below sequence, Note it's not liable to modify them as to keep compatibility of version
 * header|userid|username|dbname|client_info|object_name|detail_info|nodename|threadid|localport|remoteport
 */
void audit_report(AuditType type, AuditResult result, const char *object_name, const char *detail_info,
                  AuditClassType ctype)
{
    /* check the process status to decide whether to report it */
    if (!audit_status_check_ok() || (detail_info == NULL)) {
        return;
    }

    /* check the audit type to decide whether to report it */
    if (!audit_type_validcheck(type)) {
        return;
    }

    /* get event info from port */
    StringInfoData buf;
    AuditData adata;
    AuditEventInfo event_info;
    if (!audit_get_clientinfo(type, object_name, event_info)) {
        return; 
    }
    /* judge if the remote_host  info in the blacklist */
    if (audit_check_client_blacklist(event_info.client_info)) {
        return;
    }
    char *userid = event_info.userid;
    const char* username = event_info.username;
    const char* dbname = event_info.dbname;
    char* client_info = event_info.client_info;
    char* threadid = event_info.threadid;
    char* localport = event_info.localport;
    char* remoteport = event_info.remoteport;

    /* append xid info when audit_xid_info = 1 */
    char *detail_info_xid = NULL;
    bool audit_xid_info = (u_sess->attr.attr_security.audit_xid_info == 1);
    if (audit_xid_info) {
        uint32 len = uint64_max_len + strlen("xid=, ") + strlen(detail_info) + 1;
        detail_info_xid = (char *)palloc0(len);
        audit_append_xid_info(detail_info, detail_info_xid, len);
    }

    /* append data header */
    adata.header.signature[0] = 'A';
    adata.header.signature[1] = 'U';
    adata.header.version = 0;
    adata.header.fields = PGAUDIT_QUERY_COLS;
    adata.header.flags = AUDIT_TUPLE_NORMAL;
    adata.header.time = current_timestamp();
    adata.header.size = 0;
    adata.type = type;
    adata.result = result;
    initStringInfo(&buf);
    appendBinaryStringInfo(&buf, (char*)&adata, AUDIT_HEADER_SIZE);

    /* append audit data */
    appendStringField(&buf, userid);
    appendStringField(&buf, username);
    appendStringField(&buf, dbname);
    appendStringField(&buf, (client_info[0] != '\0') ? client_info : NULL);
    appendStringField(&buf, object_name);
    appendStringField(&buf, (!audit_xid_info) ? detail_info : detail_info_xid);
    appendStringField(&buf, g_instance.attr.attr_common.PGXCNodeName);
    appendStringField(&buf, (threadid[0] != '\0') ? threadid : NULL);
    appendStringField(&buf, (localport[0] != '\0') ? localport : NULL);
    appendStringField(&buf, (remoteport[0] != '\0') ? remoteport : NULL);

    /*
     * Use the chunking protocol if we know the syslogger should be
     * catching stderr output, and we are not ourselves the syslogger.
     * Otherwise, just do a vanilla write to stderr.
     */
    if (WRITE_TO_AUDITPIPE) {
        write_pipe_chunks(buf.data, buf.len, ctype);
    } else if (WRITE_TO_STDAUDITFILE(ctype)) {
        pgaudit_write_file(buf.data, buf.len);
    } else if (WRITE_TO_UNIAUDITFILE(ctype)) {
        pgaudit_write_policy_audit_file(buf.data, buf.len);
    } else if (detail_info != NULL) {
        ereport(LOG, (errmsg("discard audit data: %s", (!audit_xid_info) ? detail_info : detail_info_xid)));
    }

    if (detail_info_xid != NULL) {
        pfree(detail_info_xid);
    }
    pfree(buf.data);
}

/* Brief        : close a file. */
static void pgaudit_close_file(FILE* fp, const char* file)
{
    if (NULL == file || NULL == fp) {
        return;
    }

    if (ferror(fp)) {
        ereport(LOG, (errcode_for_file_access(), errmsg("could not write audit file \"%s\": %m", file)));
        if (FreeFile(fp) < 0) {
            ereport(LOG, (errcode_for_file_access(), errmsg("could not close audit file \"%s\": %m", file)));
        }
    } else if (FreeFile(fp) < 0) {
        ereport(LOG, (errcode_for_file_access(), errmsg("could not close audit file \"%s\": %m", file)));
    }
}

/* Brief        : read the index table into memory from file. */
static void pgaudit_read_indexfile(const char* audit_directory)
{
    FILE* fp = NULL;
    struct stat statbuf;
    char tblfile_path[MAXPGPATH] = {0};
    size_t nread = 0;
    AuditIndexTableNew indextbl;

    int rc = snprintf_s(tblfile_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", audit_directory, audit_indextbl_file);
    securec_check_intval(rc,,);
    /* upgrade processing and sync old index table for old version */
    if (pgaudit_find_indexfile()) {
        pgaudit_rewrite_indexfile();
    }
    /*
     * Check whether the map file is exist
     * there will be no index audit table file here when audit process first init
     * return directly and keep the index audit table values NULL, pgaudit_update_indexfile will flush new one
     */
    if (stat(tblfile_path, &statbuf) != 0) {
        return;
    }

    /* Open the audit index table file to write out the current values. */
    fp = AllocateFile(tblfile_path, PG_BINARY_R);
    if (NULL == fp) {
        ereport(LOG,
                (errcode_for_file_access(), errmsg("could not open audit index table file \"%s\": %m", tblfile_path)));
        return;
    }
    /* read the audit index table header first */
    nread = fread(&indextbl, indextbl_header_size, 1, fp);
    if (1 == nread) {
        errno_t errorno = EOK;
        /* maxnum should be restricted with guc parameter audit_file_remain_threshold */
        if (indextbl.maxnum == 0 || indextbl.maxnum > (1024 * 1024 + 1)) {
            ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("fail to read indextbl maxnum")));
        }

        LWLockAcquire(AuditIndexFileLock, LW_EXCLUSIVE);

        /* free the current audit index table */
        pfree_ext(g_instance.audit_cxt.audit_indextbl);

        /* read the whole audit index table */
        g_instance.audit_cxt.audit_indextbl = (AuditIndexTableNew *)MemoryContextAllocZero(
            g_instance.audit_cxt.global_audit_context,
            (indextbl.maxnum * sizeof(AuditIndexItem) + indextbl_header_size));
        errorno =
            memcpy_s(g_instance.audit_cxt.audit_indextbl,
                     indextbl.maxnum * sizeof(AuditIndexItem) + indextbl_header_size, &indextbl, indextbl_header_size);
        securec_check(errorno, "\0", "\0");

        nread = fread(g_instance.audit_cxt.audit_indextbl->data, sizeof(AuditIndexItem), indextbl.maxnum, fp);
        if (nread != indextbl.maxnum) {
            ereport(WARNING,
                    (errcode_for_file_access(), errmsg("could not read audit index file \"%s\": %m", tblfile_path)));
        }

        LWLockRelease(AuditIndexFileLock);
    }

    pgaudit_close_file(fp, tblfile_path);
}

/*
 * Brief        : write the index table into file from memory.
 * Description    :
 */
static void pgaudit_update_indexfile(const char* mode, bool allow_errors)
{
    FILE* fp = NULL;
    char tblfile_path[MAXPGPATH] = {0};
    size_t nwritten = 0;
    size_t count = 0;

    int rc = snprintf_s(tblfile_path,
        MAXPGPATH,
        MAXPGPATH - 1,
        "%s/%s",
        g_instance.attr.attr_security.Audit_directory,
        audit_indextbl_file);
    securec_check_intval(rc,,);

    /* Open the audit index table file to write out the current values. */
    fp = AllocateFile(tblfile_path, mode);
    if (NULL == fp) {
        if (allow_errors) {
            ereport(LOG,
                (errcode_for_file_access(), errmsg("could not open audit index table file \"%s\": %m", tblfile_path)));
            return;
        } else {
            LWLockAcquire(AuditIndexFileLock, LW_EXCLUSIVE);
            pfree_ext(g_instance.audit_cxt.audit_indextbl);
            LWLockRelease(AuditIndexFileLock);
            ereport(FATAL,
                (errcode_for_file_access(), errmsg("could not open audit index table file \"%s\": %m", tblfile_path)));
        }
    }
    /* check upgrade version to do audit upgrade processing */
    pgaudit_indexfile_upgrade();
    LWLockAcquire(AuditIndexFileLock, LW_EXCLUSIVE);
    if (g_instance.audit_cxt.audit_indextbl != NULL) {
        count = g_instance.audit_cxt.audit_indextbl->maxnum * sizeof(AuditIndexItem) + indextbl_header_size;
        nwritten = fwrite(g_instance.audit_cxt.audit_indextbl, 1, count, fp);
        if (nwritten != count)
            ereport(allow_errors ? LOG : FATAL,
                (errcode_for_file_access(), errmsg("could not write to audit index file: %m")));
    }
    LWLockRelease(AuditIndexFileLock);
    ereport(DEBUG1, (errmsg("pgaudit_update_indexfile index size: %ld", (long)ftell(fp))));

    pgaudit_close_file(fp, tblfile_path);
}

static uint32 pgaudit_get_max_fnum(uint32 old_thread_num)
{
    uint32 currrent_max_fnum = 0;
    for (uint32 i = 0; i < old_thread_num; ++i) {
        AuditIndexItem *cur_item =
            g_instance.audit_cxt.audit_indextbl->data + g_instance.audit_cxt.audit_indextbl->curidx[i];
        if (currrent_max_fnum < cur_item->filenum) {
            currrent_max_fnum = cur_item->filenum;
        }
    }
    return currrent_max_fnum;
}

/*
 * Brief : pgaudit_find_indexfile
 * Description : find pgaudit old index file function
 */
static bool pgaudit_find_indexfile(void)
{
    struct stat statbuf;
    char tblfile_path[MAXPGPATH] = {0};
    int rc = snprintf_s(tblfile_path,
        MAXPGPATH,
        MAXPGPATH - 1,
        "%s/%s",
        g_instance.attr.attr_security.Audit_directory,
        audit_indextbl_old_file);
    securec_check_ss(rc, "\0", "\0");
    if (stat(tblfile_path, &statbuf) == 0) {
        return true;
    }
    return false;
}

/*
 * Brief : pgaudit_indexfile_upgrade
 * Description : index table file upgrade function
 */
static void pgaudit_indexfile_upgrade(void)
{
    struct stat statbuf;
    char tblfile_path[MAXPGPATH] = {0};
    if (t_thrd.proc == NULL) {
        return;
    }
    int rc = snprintf_s(tblfile_path,
        MAXPGPATH,
        MAXPGPATH - 1,
        "%s/%s",
        g_instance.attr.attr_security.Audit_directory,
        audit_indextbl_old_file);
    securec_check_intval(rc,,);
    ereport(DEBUG1, (errmsg("audit upgrade processing index file upgrade enter")));

    if (t_thrd.proc->workingVersionNum >= AUDIT_INDEX_TABLE_VERSION_NUM) {
        /* version is equal to AUDIT_INDEX_TABLE_VERSION_NUM upgrade success */
        if (stat(tblfile_path, &statbuf) == 0) {
            /* find old index table and delete it */
            LWLockAcquire(AuditIndexFileLock, LW_EXCLUSIVE);
            if (unlink(tblfile_path) < 0) {
                ereport(WARNING, (errmsg("could not remove audit old index table file \"%s\": %m", tblfile_path)));
            }
            LWLockRelease(AuditIndexFileLock);
        }
    } else {
        /* upgrade processing and sync old index table for old version */
        if (stat(tblfile_path, &statbuf) != 0) {
            /* old index table file is not exited */
            ereport(WARNING, (errmsg("could not find audit old index table file \"%s\": %m", tblfile_path)));
        }
        /* sys index file from new to old */
        pgaudit_indexfile_sync(PG_BINARY_W, true);
    }
}

/*
 * Brief : pgaudit_indexfile_sync
 * Description : sync old and new index table file function
 */
static void pgaudit_indexfile_sync(const char* mode, bool allow_errors)
{
    FILE* fp = NULL;
    struct stat statbuf;
    char tblfile_path[MAXPGPATH] = {0};
    size_t nwritten = 0;
    size_t count = 0;

    int rc = snprintf_s(tblfile_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", g_instance.attr.attr_security.Audit_directory,
        audit_indextbl_old_file);
    securec_check_intval(rc, ,);
    ereport(DEBUG1, (errmsg("audit upgrade processing index file sync enter")));
    if (stat(tblfile_path, &statbuf) == 0) {
        /* old index table file is exist and sync file */
        fp = AllocateFile(tblfile_path, mode);
        if (NULL == fp) {
            ereport(allow_errors ? LOG : FATAL, (errcode_for_file_access(),
                errmsg("could not open audit index table file \"%s\": %m", tblfile_path)));
            return;
        }
        /* write down the current values from audit_indextbl to audit_indextbl_old */
        LWLockAcquire(AuditIndexFileLock, LW_EXCLUSIVE);
        /* copy audit indextbl from new to old in memory */
        if (g_instance.audit_cxt.audit_indextbl != NULL) {
            ereport(LOG, (errmsg("audit upgrade processing audit_indextbl != NULL")));
            g_instance.audit_cxt.audit_indextbl_old->maxnum = g_instance.audit_cxt.audit_indextbl->maxnum;
            g_instance.audit_cxt.audit_indextbl_old->begidx = g_instance.audit_cxt.audit_indextbl->begidx;
            g_instance.audit_cxt.audit_indextbl_old->curidx = g_instance.audit_cxt.audit_indextbl->curidx[0];
            g_instance.audit_cxt.audit_indextbl_old->count = g_instance.audit_cxt.audit_indextbl->count;
            g_instance.audit_cxt.audit_indextbl_old->last_audit_time =
                g_instance.audit_cxt.audit_indextbl->last_audit_time;
            errno_t errorno = EOK;
            errorno = memcpy_s(g_instance.audit_cxt.audit_indextbl_old->data,
                               g_instance.audit_cxt.audit_indextbl->maxnum * sizeof(AuditIndexItem),
                               g_instance.audit_cxt.audit_indextbl->data,
                               g_instance.audit_cxt.audit_indextbl->maxnum * sizeof(AuditIndexItem));
            securec_check(errorno, "\0", "\0");
        }
        if (g_instance.audit_cxt.audit_indextbl_old != NULL) {
            count = g_instance.audit_cxt.audit_indextbl_old->maxnum * sizeof(AuditIndexItem) + old_indextbl_header_size;
            nwritten = fwrite(g_instance.audit_cxt.audit_indextbl_old, 1, count, fp);
            if (nwritten != count)
                ereport(allow_errors ? LOG : FATAL,
                    (errcode_for_file_access(), errmsg("could not write to audit old index file: %m")));
        }
        LWLockRelease(AuditIndexFileLock);
        ereport(LOG, (errmsg("pgaudit_indexfile_sync index size: %ld", (long)ftell(fp))));
        pgaudit_close_file(fp, tblfile_path);
    }
}

/*
 * Brief : pgaudit_rewrite_indexfile
 * Description : read old index file and rewrite to new index file function
 */
static void pgaudit_rewrite_indexfile(void)
{
    FILE* fp = NULL;
    char tblfile_path[MAXPGPATH] = {0};
    size_t nread = 0;
    AuditIndexTable old_index_tbl;

    int rc = snprintf_s(tblfile_path,
        MAXPGPATH,
        MAXPGPATH - 1,
        "%s/%s",
        g_instance.attr.attr_security.Audit_directory,
        audit_indextbl_old_file);
    securec_check_intval(rc,,);
    /* open old index file and read audit index table */
    fp = AllocateFile(tblfile_path, PG_BINARY_R);
    if (NULL == fp) {
        ereport(LOG,
            (errcode_for_file_access(), errmsg("could not open audit old index table file \"%s\": %m", tblfile_path)));
        return;
    }
    ereport(LOG, (errmsg("audit upgrade processing rewrite enter")));
    /* read the audit old index table header first */
    nread = fread(&old_index_tbl, old_indextbl_header_size, 1, fp);
    if (1 == nread) {
        errno_t errorno = EOK;
        /* maxnum should be restricted with guc parameter audit_file_remain_threshold */
        if (old_index_tbl.maxnum == 0 || old_index_tbl.maxnum > (1024 * 1024 + 1)) {
            ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("fail to read indextbl maxnum")));
        }
        LWLockAcquire(AuditIndexFileLock, LW_EXCLUSIVE);
        /* free the current audit index table */
        pfree_ext(g_instance.audit_cxt.audit_indextbl);
        pfree_ext(g_instance.audit_cxt.audit_indextbl_old);
        /* read the whole audit index table */
        g_instance.audit_cxt.audit_indextbl_old = (AuditIndexTable *)MemoryContextAllocZero(
            g_instance.audit_cxt.global_audit_context,
            (old_index_tbl.maxnum * sizeof(AuditIndexItem) + old_indextbl_header_size));
        errorno = memcpy_s(g_instance.audit_cxt.audit_indextbl_old,
            old_index_tbl.maxnum * sizeof(AuditIndexItem) + old_indextbl_header_size,
            &old_index_tbl, old_indextbl_header_size);
        securec_check(errorno, "\0", "\0");
        /* rewrite old index table to new index table */
        g_instance.audit_cxt.audit_indextbl = (AuditIndexTableNew *)MemoryContextAllocZero(
            g_instance.audit_cxt.global_audit_context,
            (old_index_tbl.maxnum * sizeof(AuditIndexItem) + indextbl_header_size));
        g_instance.audit_cxt.audit_indextbl->maxnum = old_index_tbl.maxnum;
        g_instance.audit_cxt.audit_indextbl->count = old_index_tbl.count;
        g_instance.audit_cxt.audit_indextbl->begidx = old_index_tbl.begidx;
        g_instance.audit_cxt.audit_indextbl->last_audit_time = old_index_tbl.last_audit_time;
        g_instance.audit_cxt.audit_indextbl->thread_num = 1;
        g_instance.audit_cxt.audit_indextbl->curidx[0] = old_index_tbl.curidx;
        g_instance.audit_cxt.audit_indextbl->latest_idx = old_index_tbl.curidx + 1;
        /* read index item data */
        nread = fread(g_instance.audit_cxt.audit_indextbl->data, sizeof(AuditIndexItem), old_index_tbl.maxnum, fp);
        if (nread != old_index_tbl.maxnum) {
            ereport(WARNING,
                    (errcode_for_file_access(), errmsg("could not read audit index file \"%s\": %m", tblfile_path)));
        }

        LWLockRelease(AuditIndexFileLock);
    }
    pgaudit_close_file(fp, tblfile_path);
}

/*
 * Brief        :  init index table in memory
 * Description  :
 * 1. init the index file table based on thread num if index table file is not exit
 * 2. calculate pgaudit_totalspace based on cuurent audit files
 * it's safe no lock here As only PM or audit master thread can invoke init func
 */
static void pgaudit_indextbl_init_new(void)
{
    /* load from the index file or create new one */
    pgaudit_read_indexfile(g_instance.attr.attr_security.Audit_directory);

    /* init new one when but not from index file when database init first time */
    LWLockAcquire(AuditIndexFileLock, LW_EXCLUSIVE);
    if (g_instance.audit_cxt.audit_indextbl == NULL) {
        ereport(LOG, (errmsg("pgaudit_indextbl_init_new first init")));
        g_instance.audit_cxt.audit_indextbl =
            (AuditIndexTableNew *)MemoryContextAllocZero(g_instance.audit_cxt.global_audit_context,
            (u_sess->attr.attr_security.Audit_RemainThreshold + 1) * sizeof(AuditIndexItem) + indextbl_header_size);
        g_instance.audit_cxt.audit_indextbl->maxnum = u_sess->attr.attr_security.Audit_RemainThreshold + 1;
        g_instance.audit_cxt.audit_indextbl->count = 0; /* audit files count will be updated by auditfile_open */
        g_instance.audit_cxt.audit_indextbl->begidx = 0;
        g_instance.audit_cxt.audit_indextbl->thread_num = g_instance.audit_cxt.thread_num;

        for (int i = 0; i < g_instance.audit_cxt.thread_num; ++i) {
            g_instance.audit_cxt.audit_indextbl->curidx[i] = i;
            AuditIndexItem *item =
                g_instance.audit_cxt.audit_indextbl->data + g_instance.audit_cxt.audit_indextbl->curidx[i];
            item->filenum = i;
        }

        g_instance.audit_cxt.audit_indextbl->latest_idx = g_instance.audit_cxt.thread_num;
    }

    uint32 index = 0;
    AuditIndexItem *item = NULL;

    /*
     * thread num changed routine
     * for thread num enlarge: update latest_idx、curidxes、items and thread_num, if new audit file did not exist,
     * audit thread will reinit new one
     * for thread num shrink: dismiss the old ones and update curidxes、thread_num but not udpate latest_idx
     */
    if (g_instance.audit_cxt.audit_indextbl->thread_num != (uint32)g_instance.audit_cxt.thread_num) {
        uint32 old_thread_num = g_instance.audit_cxt.audit_indextbl->thread_num;
        uint32 new_thread_num = (uint32)g_instance.audit_cxt.thread_num;
        uint32 latest_idx = g_instance.audit_cxt.audit_indextbl->latest_idx;

        if (AUDIT_THREADNUM_ENLARGE) {
            /*
             * before arrage fnums for new audit threads, get the last max fnum
             * then increase the fnum based on the max fnum
             */
            uint32 old_max_fnum = pgaudit_get_max_fnum(old_thread_num);

            /* update curidxes and corresponding items */
            uint32 step = new_thread_num - old_thread_num;
            for (uint32 i = 0; i < step; ++i) {
                uint32 new_idx = old_thread_num + i;
                g_instance.audit_cxt.audit_indextbl->curidx[new_idx] = latest_idx + i;
                AuditIndexItem *new_item =
                    g_instance.audit_cxt.audit_indextbl->data + g_instance.audit_cxt.audit_indextbl->curidx[new_idx];
                new_item->filenum = (old_max_fnum + i + 1);
            }
            g_instance.audit_cxt.audit_indextbl->latest_idx += step;
        } else {
            uint32 step = old_thread_num - new_thread_num;
            uint32 *curidxes = g_instance.audit_cxt.audit_indextbl->curidx;
            errno_t errorno =
                memmove_s(curidxes, MAX_AUDIT_NUM * sizeof(uint32), curidxes + step, new_thread_num * sizeof(uint32));
            securec_check(errorno, "\0", "\0");
        }

        g_instance.audit_cxt.audit_indextbl->thread_num = (uint32)g_instance.audit_cxt.thread_num;
    }

    /* audit threads are writing files in range [earliest_idx, latest_idx) */
    uint32 earliest_idx = 0;
    if ((int32)g_instance.audit_cxt.audit_indextbl->latest_idx >= g_instance.audit_cxt.thread_num) {
        earliest_idx = g_instance.audit_cxt.audit_indextbl->latest_idx - g_instance.audit_cxt.thread_num;
        index = g_instance.audit_cxt.audit_indextbl->begidx;
    } else {
        earliest_idx = g_instance.audit_cxt.audit_indextbl->maxnum + g_instance.audit_cxt.audit_indextbl->latest_idx -
            g_instance.audit_cxt.thread_num;
        index = g_instance.audit_cxt.audit_indextbl->latest_idx;
    }
    /* calculate total space of all audit files */
    g_instance.audit_cxt.pgaudit_totalspace = 0;
    do {
        item = g_instance.audit_cxt.audit_indextbl->data + index;
        g_instance.audit_cxt.pgaudit_totalspace += item->filesize;
        /* stop till the current writting index */
        if (index == earliest_idx) {
            break;
        }
        index = (index + 1) % g_instance.audit_cxt.audit_indextbl->maxnum;
    } while (true);

    /* used for give space warning in logs when audit log rotation policy is besed on time */
    t_thrd.audit.space_beyond_size =
        (g_instance.audit_cxt.pgaudit_totalspace / SPACE_INTERVAL_SIZE) * SPACE_INTERVAL_SIZE + SPACE_INTERVAL_SIZE;

    LWLockRelease(AuditIndexFileLock);
    return;
}

static void pgaudit_update_maxnum()
{
    errno_t errorno = EOK;
    int thread_num = g_instance.attr.attr_security.audit_thread_num;

    int new_indextbl_data_lenth = (u_sess->attr.attr_security.Audit_RemainThreshold + 1) * sizeof(AuditIndexItem);
    AuditIndexTableNew *new_indextbl = (AuditIndexTableNew *)MemoryContextAllocZero(
        g_instance.audit_cxt.global_audit_context, new_indextbl_data_lenth + indextbl_header_size);

    /* curidx and latest_idx should be updated later from old index table file */
    new_indextbl->begidx = 0;
    new_indextbl->maxnum = u_sess->attr.attr_security.Audit_RemainThreshold + 1;
    new_indextbl->last_audit_time = g_instance.audit_cxt.audit_indextbl->last_audit_time;
    new_indextbl->thread_num = g_instance.audit_cxt.audit_indextbl->thread_num;

    if (g_instance.audit_cxt.audit_indextbl->count > 0) {
        AuditIndexItem *item = NULL;
        uint32 latest_idx = g_instance.audit_cxt.audit_indextbl->latest_idx;
        uint32 last_idx = (latest_idx == 0) ? (g_instance.audit_cxt.audit_indextbl->maxnum - 1): (latest_idx - 1);
        uint32 index = g_instance.audit_cxt.audit_indextbl->begidx;
        uint32 pos = new_indextbl->begidx;
        do {
            item = g_instance.audit_cxt.audit_indextbl->data + index;
            errorno = memcpy_s(new_indextbl->data + pos, (new_indextbl_data_lenth - (pos * sizeof(AuditIndexItem))),
                               item, sizeof(AuditIndexItem));
            securec_check(errorno, "\0", "\0");
            new_indextbl->count++;

            /*
             * finished copy old index table file from range [begin, latest_idx)
             * then update new index table file curidxes
             */
            if (index == last_idx) {
                for (int i = 0; i < thread_num; ++i) {
                    new_indextbl->curidx[i] = pos - thread_num + 1 + i;
                }
                new_indextbl->latest_idx = pos + 1;
                break;
            }

            pos++;
            index = (index + 1) % g_instance.audit_cxt.audit_indextbl->maxnum;
        } while (true);
    }
    pfree(g_instance.audit_cxt.audit_indextbl);
    g_instance.audit_cxt.audit_indextbl = new_indextbl;
}

/*
 * Brief        : reset the index file based on new parameter
 * Description  :
 * 1. clean up the current auditfiles
 * 2. alloc new index table using new parameter & swap old one
 * 3. flush the index table from memory
 */
static void pgaudit_reset_indexfile()
{
    /* If file remain threshold parameter changed more little, than need to cleanup the audit data first */
    uint32 old_maxnum = g_instance.audit_cxt.audit_indextbl->maxnum;
    if (old_maxnum > (uint32)u_sess->attr.attr_security.Audit_RemainThreshold + 1) {
        int rc = snprintf_s(t_thrd.audit.pgaudit_filepath, MAXPGPATH, MAXPGPATH - 1, "%s/%s",
            g_instance.attr.attr_security.Audit_directory, audit_indextbl_file);
        securec_check_intval(rc,,);

        if (unlink(t_thrd.audit.pgaudit_filepath) < 0)
            ereport(WARNING, (errmsg("could not remove audit index table file: %m")));

        pgaudit_cleanup();
        ereport(LOG, (errmsg("pgaudit pgaudit_reset_indexfile clean up audit files trigger by parameter changing")));
    }

    /* If file remain threshold parameter changed, than copy the old audit index table to the new table */
    if (old_maxnum != (uint32)u_sess->attr.attr_security.Audit_RemainThreshold + 1) {
        LWLockAcquire(AuditIndexFileLock, LW_EXCLUSIVE);
        pgaudit_update_maxnum();
        LWLockRelease(AuditIndexFileLock);
    }

    pgaudit_update_indexfile(PG_BINARY_W, true);
}

/*
 * Brief        : get the specified string field.
 * Description    :
 */
static const char* pgaudit_string_field(AuditData* adata, int num)
{
    int index = 0;
    uint32 size = 0;
    uint32 datalen = 0;
    const char* field = NULL;

    if (adata == NULL)
        return NULL;

    datalen = adata->header.size - AUDIT_HEADER_SIZE;
    field = adata->varstr;
    do {
        errno_t errorno = EOK;

        errorno = memcpy_s(&size, sizeof(uint32), field, sizeof(uint32));
        securec_check(errorno, "\0", "\0");

        datalen -= sizeof(uint32);
        if (size > datalen) { /* invalid data */
            return NULL;
        }

        field = field + sizeof(uint32);
        if (index == num) {
            break;
        }
        field = field + size;
        datalen -= size;
        index++;
    } while (index <= num);

    if (size == 0)
        return NULL;
    return field;
}

static char* serialize_event_to_json(AuditData *adata, long long eventTime)
{
    AuditElasticEvent event;
    WRITE_JSON_START(AuditElasticEvent, &event);

    event.aDataType = AuditTypeDesc(adata->type);
    WRITE_JSON_STRING(aDataType);
    event.aDataResult = AuditResultDesc(adata->result);
    WRITE_JSON_STRING(aDataResult);
    event.auditUserId = pgaudit_string_field(adata, AUDIT_USER_ID);
    WRITE_JSON_STRING(auditUserId);
    event.auditUserName = pgaudit_string_field(adata, AUDIT_USER_NAME);
    WRITE_JSON_STRING(auditUserName);
    event.auditDatabaseName = pgaudit_string_field(adata, AUDIT_DATABASE_NAME);
    WRITE_JSON_STRING(auditDatabaseName);
    event.clientConnInfo = pgaudit_string_field(adata, AUDIT_CLIENT_CONNINFO);
    WRITE_JSON_STRING(clientConnInfo);
    event.objectName = pgaudit_string_field(adata, AUDIT_OBJECT_NAME);
    WRITE_JSON_STRING(objectName);
    event.detailInfo = pgaudit_string_field(adata, AUDIT_DETAIL_INFO);
    WRITE_JSON_STRING(detailInfo);
    event.nodeNameInfo = pgaudit_string_field(adata, AUDIT_NODENAME_INFO);
    WRITE_JSON_STRING(nodeNameInfo);
    event.threadIdInfo = pgaudit_string_field(adata, AUDIT_THREADID_INFO);
    WRITE_JSON_STRING(threadIdInfo);
    event.localPortInfo = pgaudit_string_field(adata, AUDIT_LOCALPORT_INFO);
    WRITE_JSON_STRING(localPortInfo);
    event.remotePortInfo = pgaudit_string_field(adata, AUDIT_REMOTEPORT_INFO);
    WRITE_JSON_STRING(remotePortInfo);
    event.eventTime = eventTime;
    WRITE_JSON_INT(eventTime);
    WRITE_JSON_END();
}

struct AuditElasticIndex {
    const char* _index;
    const char* _type;
};

char* serialize_index_to_json(const char* audit_str)
{
    AuditElasticIndex index;
    WRITE_JSON_START(AuditElasticIndex, &index);
    index._index = audit_str;
    WRITE_JSON_STRING(_index);
    index._type = audit_str;
    WRITE_JSON_STRING(_type);
    WRITE_JSON_END();
}

void fix_json(char* json)
{
    for (int i = 0; i < (int)strlen(json); i++) {
        if (json[i] == '\n') {
            json[i] = ' ';
        }
        if (json[i] == 0) {
            break;
        }
    }
}

static void pgaudit_query_file_for_elastic()
{
    AuditMsgHdr header;
    AuditData *adata = NULL;
    int rc;
    char file_path[MAXPGPATH] = {0};
    rc = snprintf_s(file_path, sizeof(file_path), sizeof(file_path) - 1, "%s" DIR_SEP "done",
        g_instance.attr.attr_security.Audit_directory);
    securec_check_ss(rc, "\0", "\0");
    std::vector<std::string> done_files;
    get_files_list(file_path, done_files, ".bin", MAX_QUEUE_SIZE);
    while (!done_files.empty()) {
        for (const std::string& file_item : done_files) {
            rc = snprintf_s(file_path, sizeof(file_path), sizeof(file_path) - 1, "%s" DIR_SEP "done" DIR_SEP "%s",
                g_instance.attr.attr_security.Audit_directory, file_item.c_str());
            securec_check_ss(rc, "\0", "\0");
            /* Open the audit file to scan the audit record. */
            FILE* fp = AllocateFile(file_path, PG_BINARY_R);
            if (fp == NULL) {
                ereport(WARNING,
                    (errcode_for_file_access(), errmsg("could not open audit file \"%s\": %m", file_path)));
                continue;
            }

            FILE* bulkfile = fopen("audit_bulk.json", "w"); // , "a");
            int event_count = 0;
            do {
                errno_t errorno = EOK;
                /* read the audit message header first */
                int nread = fread((char*)&header, sizeof(AuditMsgHdr), 1, fp);
                if (nread == 0) {
                    break;
                }

                if (header.signature[0] != 'A' ||
                    header.signature[1] != 'U' ||
                    header.version != 0 ||
                    header.fields != PGAUDIT_QUERY_COLS ||
                    (header.size <= sizeof(AuditMsgHdr))) {
                    ereport(LOG,    (errmsg("invalid data in audit file \"%s\"", file_path)));
                    break;
                }
                /* read the whole audit record */
                adata = (AuditData *)palloc(header.size);
                errorno = memcpy_s(adata, header.size, &header, sizeof(AuditMsgHdr));
                securec_check(errorno, "\0", "\0");
                nread = fread((char*)adata + sizeof(AuditMsgHdr), header.size - sizeof(AuditMsgHdr), 1, fp);
                if (nread != 1) {
                    ereport(WARNING,
                            (errcode_for_file_access(), errmsg("could not read audit file \"%s\": %m", file_path)));
                    pfree(adata);
                    break;
                }

                char* json = serialize_event_to_json(adata, adata->header.time);
                ereport(DEBUG1, (errmsg("++++++++++++++++++++++++++ Write to JSON adata->type = %d(%s)", adata->type,
                    AuditTypeDesc(adata->type))));
                fix_json(json);
                const char* indexType = "audit";
                switch (adata->type) {
                    case AUDIT_POLICY_EVENT:
                        indexType = "audit_policy";
                        break;
                    case MASKING_POLICY_EVENT:
                        indexType = "masking_policy";
                        break;
                    case SECURITY_EVENT:
                        indexType = "security_management";
                        break;
                    default:
                        break;
                }
                char* jsonIndex = serialize_index_to_json(indexType);
                fix_json(jsonIndex);

                /* And append a separator */
                if (bulkfile) {
                    fprintf(bulkfile, "{\"index\":%s}\n", jsonIndex);
                    fprintf(bulkfile, "%s\n", json);
                    ++event_count;
                }
                pfree(adata);
            } while (true);
            // unlink file
            if (unlink(file_path)) {
                ereport(WARNING, (errmsg("could not unlink file \"%s\": %m", file_path)));
            }

            if (bulkfile != NULL) {
                fclose(bulkfile);
            }
            pgaudit_close_file(fp, file_path);
            if (event_count) {
                /*
                 * total curl command is as below, first for http as sencond for https protocal
                 * curl -XPOST http://ip:9200/audit/events/_bulk?pretty -H 'Content-type: application/json'
                 * --data-binary @audit_bulk.json curl -XPOST -k https://ip:9200/audit/events/_bulk?pretty -H
                 * 'Content-type: application/json' --data-binary @audit_bulk.json
                 */
                std::string url = ((std::string) g_instance.attr.attr_security.elastic_search_ip_addr) + 
                                  ((std::string) ":9200/audit/events/_bulk?pretty");
                m_curlUtils.http_post_file_request(url, "audit_bulk.json");
            }
        }
        rc = snprintf_s(file_path, sizeof(file_path), sizeof(file_path) - 1, "%s" DIR_SEP "done",
            g_instance.attr.attr_security.Audit_directory);
        securec_check_ss(rc, "\0", "\0");
        done_files.clear();
        get_files_list(file_path, done_files, ".bin", MAX_QUEUE_SIZE);
    }
}

/*
 * Brief          : scan the specified audit file into tuple
 * Description    : Note we use old/new version to differ whether there is user_id field in the file.
 *                  for expanding new field later, maybe we will depend on version id to implement 
 *                  backward compatibility but not bool variable
 */
static void deserialization_to_tuple(Datum (&values)[PGAUDIT_QUERY_COLS], 
                                     AuditData *adata, 
                                     const AuditMsgHdr &header)
{
    /* append timestamp info to data tuple */
    int i = 0;
    values[i++] = TimestampTzGetDatum(time_t_to_timestamptz(adata->header.time));
    values[i++] = CStringGetTextDatum(AuditTypeDesc(adata->type));
    values[i++] = CStringGetTextDatum(AuditResultDesc(adata->result));

    /*
     * new format of the audit file under correct record
     * the older audit file do not have userid info, so let it to be null
     */
    int index_field = 0;
    const char* field = NULL;
    bool new_version = (header.fields == PGAUDIT_QUERY_COLS);
    field = new_version ? pgaudit_string_field(adata, index_field++) : NULL;
    values[i++] = CStringGetTextDatum(FILED_NULLABLE(field)); /* user id */
    field = pgaudit_string_field(adata, index_field++);
    values[i++] = CStringGetTextDatum(FILED_NULLABLE(field)); /* user name */
    field = pgaudit_string_field(adata, index_field++);
    values[i++] = CStringGetTextDatum(FILED_NULLABLE(field)); /* dbname */
    field = pgaudit_string_field(adata, index_field++);
    values[i++] = CStringGetTextDatum(FILED_NULLABLE(field)); /* client info */
    field = pgaudit_string_field(adata, index_field++);
    values[i++] = CStringGetTextDatum(FILED_NULLABLE(field)); /* object name */
    field = pgaudit_string_field(adata, index_field++);
    values[i++] = CStringGetTextDatum(FILED_NULLABLE(field)); /* detail info */
    field = pgaudit_string_field(adata, index_field++);
    values[i++] = CStringGetTextDatum(FILED_NULLABLE(field)); /* node name */
    field = pgaudit_string_field(adata, index_field++);
    values[i++] = CStringGetTextDatum(FILED_NULLABLE(field)); /* thread id */
    field = pgaudit_string_field(adata, index_field++);
    values[i++] = CStringGetTextDatum(FILED_NULLABLE(field)); /* local port */
    field = pgaudit_string_field(adata, index_field++);
    values[i++] = CStringGetTextDatum(FILED_NULLABLE(field)); /* remote port */

    Assert(i == PGAUDIT_QUERY_COLS);
}

static void pgaudit_query_file(Tuplestorestate *state, TupleDesc tdesc, uint32 fnum, TimestampTz begtime,
                               TimestampTz endtime, const char *audit_directory)
{
    FILE* fp = NULL;
    size_t nread = 0;
    TimestampTz datetime;
    AuditMsgHdr header;
    AuditData* adata = NULL;

    if (state == NULL || tdesc == NULL)
        return;

    int rcs =
        snprintf_s(t_thrd.audit.pgaudit_filepath, MAXPGPATH, MAXPGPATH - 1, pgaudit_filename, audit_directory, fnum);
    securec_check_intval(rcs,,);
    /* Open the audit file to scan the audit record. */
    fp = AllocateFile(t_thrd.audit.pgaudit_filepath, PG_BINARY_R);
    if (fp == NULL) {
        ereport(LOG,
            (errcode_for_file_access(), errmsg("could not open audit file \"%s\": %m", t_thrd.audit.pgaudit_filepath)));
        return;
    }

    do {
        Datum values[PGAUDIT_QUERY_COLS] = {0};
        bool nulls[PGAUDIT_QUERY_COLS] = {0};
        errno_t errorno = EOK;
        /* 
         * two scenarios tell that the audit file corrupt
         * 1. fail to parse the header length
         * 2. header encoding is not valid
         */
        if (fgetc(fp) == EOF) {
            break;
        }
        (void)fseek(fp, -1, SEEK_CUR);
        size_t header_available = fread(&header, sizeof(AuditMsgHdr), 1, fp);
        if (header_available != 1 || pgaudit_invalid_header(&header)) {
            ereport(LOG, (errmsg("invalid data in audit file \"%s\"", t_thrd.audit.pgaudit_filepath)));
            /* label the currupt file num, then it may be reinit in audit thread but not here. */
            pgaudit_mark_corrupt_info(fnum);
            break;
        }

        /* read the whole audit record */
        adata = (AuditData*)palloc(header.size);
        errorno = memcpy_s(adata, header.size, &header, sizeof(AuditMsgHdr));
        securec_check(errorno, "\0", "\0");
        nread = fread((char*)adata + sizeof(AuditMsgHdr), header.size - sizeof(AuditMsgHdr), 1, fp);
        if (nread != 1) {
            ereport(LOG,
                (errcode_for_file_access(),
                    errmsg("could not read audit file \"%s\": %m", t_thrd.audit.pgaudit_filepath)));
            /* label the currupt file num, then it may be reinit in audit thread but not here. */
            pgaudit_mark_corrupt_info(fnum);
            pfree(adata);
            break;
        }

        /* filt and assemble audit info into tuplestore */
        datetime = time_t_to_timestamptz(adata->header.time);
        if (datetime >= begtime && datetime < endtime && header.flags == AUDIT_TUPLE_NORMAL) {
            deserialization_to_tuple(values, adata, header);
            tuplestore_putvalues(state, tdesc, values, nulls);
        }

        pfree(adata);
    } while (true);

    pgaudit_close_file(fp, t_thrd.audit.pgaudit_filepath);
}

/*
 * Brief        : scan the specified audit file to delete audit.
 * Description    :
 */
static void pgaudit_delete_file(uint32 fnum, TimestampTz begtime, TimestampTz endtime)
{
    int fd = -1;
    ssize_t nread = 0;
    TimestampTz datetime;
    AuditMsgHdr header;

    int rc = snprintf_s(t_thrd.audit.pgaudit_filepath,
        MAXPGPATH,
        MAXPGPATH - 1,
        pgaudit_filename,
        g_instance.attr.attr_security.Audit_directory,
        fnum);
    securec_check_intval(rc,,);

    /* Open the audit file to scan the audit record. */
    fd = open(t_thrd.audit.pgaudit_filepath, O_RDWR, pgaudit_filemode);
    if (fd < 0) {
        ereport(LOG,
            (errcode_for_file_access(), errmsg("could not open audit file \"%s\": %m", t_thrd.audit.pgaudit_filepath)));
        return;
    }

    do {
        /* read the audit message header first */
        nread = read(fd, &header, sizeof(AuditMsgHdr));
        if (nread <= 0)
            break;

        if (header.signature[0] != 'A' ||
            header.signature[1] != 'U' ||
            header.version != 0 ||
            !(header.fields == (PGAUDIT_QUERY_COLS - 1) ||
            header.fields == PGAUDIT_QUERY_COLS)) {
            /* make sure we are compatible with the older version audit file */
            ereport(LOG, (errmsg("invalid data in audit file \"%s\"", t_thrd.audit.pgaudit_filepath)));
            break;
        }

        datetime = time_t_to_timestamptz(header.time);
        if (datetime >= begtime && datetime < endtime && header.flags == AUDIT_TUPLE_NORMAL) {
            long offset = sizeof(AuditMsgHdr);
            header.flags = AUDIT_TUPLE_DEAD;
            if (lseek(fd, -offset, SEEK_CUR) < 0) {
                ereport(WARNING, (errcode_for_file_access(), errmsg("could not seek in audit file: %m")));
                break;
            }
            if (write(fd, &header, sizeof(AuditMsgHdr)) != sizeof(AuditMsgHdr)) {
                ereport(WARNING, (errcode_for_file_access(), errmsg("could not write to audit file: %m")));
                break;
            }
        }
        if (lseek(fd, header.size - sizeof(AuditMsgHdr), SEEK_CUR) < 0) {
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not seek in audit file: %m")));
            break;
        }
    } while (true);

    close(fd);
}

/* check whether system changed when auditor write audit data to current file */
static bool pgaudit_check_system(TimestampTz begtime, TimestampTz endtime, uint32 index, const char *audit_dir = NULL)
{
    bool satisfied = false;
    TimestampTz curr_filetime = 0;
    TimestampTz next_filetime = 0;
    AuditIndexItem* item = t_thrd.audit.audit_indextbl->data + index;
    uint32 earliest_idx = t_thrd.audit.audit_indextbl->latest_idx - g_instance.audit_cxt.thread_num;

    if (item->ctime > 0) {
        curr_filetime = time_t_to_timestamptz(item->ctime);
        /* check whether the item is the last item */
        if ((index >= earliest_idx && index < t_thrd.audit.audit_indextbl->latest_idx)) {
            if (curr_filetime <= endtime) {
                satisfied = true;
            }
        } else {
            item = t_thrd.audit.audit_indextbl->data + (index + 1) % t_thrd.audit.audit_indextbl->maxnum;
            if (item->ctime > 0) {
                next_filetime = time_t_to_timestamptz(item->ctime);
                /*
                 * check whether the time quantum between begtime and endtime
                 * intersect with the time quantum between curr_filetime and next_filetime
                 */
                curr_filetime = (curr_filetime > begtime) ? curr_filetime : begtime;
                next_filetime = (next_filetime < endtime) ? next_filetime : endtime;
                if (curr_filetime <= next_filetime) {
                    satisfied = true;
                } else {
                    /* compare header datetime if the create time of current file is larger
                     * under multi-thread situation
                     */
                    audit_dir = (audit_dir == NULL) ? g_instance.attr.attr_security.Audit_directory : audit_dir;
                    TimestampTz curr_headertime = pgaudit_headertime(index, audit_dir);
                    TimestampTz next_headertime = pgaudit_headertime(index + 1, audit_dir);
                    if (curr_headertime <= next_headertime) {
                        satisfied = true;
                    }
                }
            } else if (curr_filetime <= begtime || curr_filetime <= endtime) {
                satisfied = true;
            }
        }
    } else {
        satisfied = true;
    }

    return satisfied;
}

/* fetch the datetime of the file header of the audit record under pg_audit */
static TimestampTz pgaudit_headertime(uint32 fnum, const char *audit_directory)
{

    int fd = -1;
    ssize_t nread = 0;
    TimestampTz datetime;
    AuditMsgHdr header;
    char pgaudit_filepath[MAXPGPATH];

    int rc = snprintf_s(pgaudit_filepath, MAXPGPATH, MAXPGPATH - 1, pgaudit_filename,
        audit_directory, fnum);
    securec_check_intval(rc, , time_t_to_timestamptz(0));

    /* Open the audit file to scan the audit record. */
    fd = open(pgaudit_filepath, O_RDWR, pgaudit_filemode);
    if (fd < 0) {
        ereport(LOG,
            (errcode_for_file_access(), errmsg("could not open audit file \"%s\": %m", pgaudit_filepath)));
        return time_t_to_timestamptz(0);
    }
    /* read the audit message header first */
    nread = read(fd, &header, sizeof(AuditMsgHdr));
    if (nread <= 0) {
        close(fd);
        return time_t_to_timestamptz(0);
    }
    datetime = time_t_to_timestamptz(header.time);
    close(fd);
    return datetime;
}

/*
 * Brief        : whether the invoke is allowed for query audit.
 * Description  :
 */
static void pgaudit_query_valid_check(const ReturnSetInfo *rsinfo, FunctionCallInfoData *fcinfo, TupleDesc &tupdesc)
{
    Oid roleid = InvalidOid;
    /* Check some permissions first */
    roleid = GetUserId();
    if (!has_auditadmin_privilege(roleid)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("permission denied to query audit")));
    }

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
    }
    if (!((unsigned int)rsinfo->allowedModes & SFRM_Materialize)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("materialize mode required, but it is not allowed in this context")));
    }

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("return type must be a row type")));
    }

    if (tupdesc->natts != PGAUDIT_QUERY_COLS) {
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("attribute count of the return row type not matched")));
    }
}

/*
 * Brief        : query audit information between begin time and end time.
 * Description  :
 */
Datum pg_query_audit(PG_FUNCTION_ARGS)
{
    ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
    TupleDesc tupdesc = NULL;
    Tuplestorestate* tupstore = NULL;
    MemoryContext per_query_ctx = NULL;
    MemoryContext oldcontext = NULL;
    MemoryContext query_audit_ctx = NULL;
    TimestampTz begtime = PG_GETARG_TIMESTAMPTZ(0);
    TimestampTz endtime = PG_GETARG_TIMESTAMPTZ(1);
    char* audit_dir = NULL;
    char real_audit_dir[PATH_MAX] = {0};

    pgaudit_query_valid_check(rsinfo, fcinfo, tupdesc);

    /*
     * When g_instance.audit_cxt.audit_indextbl is not NULL,
     * but its origin memory context is NULL, free it will generate core
     */
    if (PG_NARGS() == PG_QUERY_AUDIT_ARGS_MAX) {
        audit_dir = text_to_cstring(PG_GETARG_TEXT_PP(PG_QUERY_AUDIT_ARGS_MAX - 1));
    }
    audit_dir = (audit_dir == NULL) ? g_instance.attr.attr_security.Audit_directory : audit_dir;
    if (realpath(audit_dir, real_audit_dir) == NULL) {
        ereport(ERROR, (errmsg("Failed to canonicalization path of audit_directory.")));
    }

    /*
     * load the index audit table from global index audit table instance
     * then use the local thread one when iterate all audit files
     */
    pgaudit_read_indexfile(real_audit_dir);
    LWLockAcquire(AuditIndexFileLock, LW_SHARED);
    if (g_instance.audit_cxt.audit_indextbl != NULL) {
        int indextbl_len =
            (u_sess->attr.attr_security.Audit_RemainThreshold + 1) * sizeof(AuditIndexItem) + indextbl_header_size;
        t_thrd.audit.audit_indextbl = (AuditIndexTableNew *)palloc0(indextbl_len);
        error_t errorno =
            memcpy_s(t_thrd.audit.audit_indextbl, indextbl_len, g_instance.audit_cxt.audit_indextbl, indextbl_len);
        securec_check(errorno, "\0", "\0");
    }
    LWLockRelease(AuditIndexFileLock);
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    MemoryContextSwitchTo(oldcontext);
    query_audit_ctx = AllocSetContextCreate(per_query_ctx, "query audit file", ALLOCSET_DEFAULT_SIZES);

    if (begtime < endtime && t_thrd.audit.audit_indextbl != NULL && t_thrd.audit.audit_indextbl->count > 0) {
        bool satisfied = false;
        uint32 index = 0;
        uint32 fnum = 0;
        AuditIndexItem* item = NULL;

        index = t_thrd.audit.audit_indextbl->begidx;
        do {
            item = t_thrd.audit.audit_indextbl->data + index;
            fnum = item->filenum;

            /* check whether system changed when auditor write audit data to current file */
            satisfied = pgaudit_check_system(begtime, endtime, index, real_audit_dir);
            if (satisfied) {
                oldcontext = MemoryContextSwitchTo(query_audit_ctx);
                pgaudit_query_file(tupstore, tupdesc, fnum, begtime, endtime, real_audit_dir);
                MemoryContextSwitchTo(oldcontext);
                MemoryContextReset(query_audit_ctx);
                satisfied = false;
            }
            ereport(DEBUG5, (errmsg("pg_query_audit current fnum: %u", fnum)));

            if (index == (t_thrd.audit.audit_indextbl->latest_idx - 1)) {
                break;
            }

            index = (index + 1) % t_thrd.audit.audit_indextbl->maxnum;
        } while (true);
    }

    /* clean up and return the tuplestore */
    pfree_ext(t_thrd.audit.audit_indextbl);
    MemoryContextDelete(query_audit_ctx);
    tuplestore_donestoring(tupstore);
    return (Datum)0;
}

/*
 * Brief        : delete audit information between begin time and end time.
 * Description  :
 */
Datum pg_delete_audit(PG_FUNCTION_ARGS)
{
    TimestampTz begtime = PG_GETARG_TIMESTAMPTZ(0);
    TimestampTz endtime = PG_GETARG_TIMESTAMPTZ(1);

    t_thrd.audit.Audit_delete = true;

    /* Check some permissions first */
    Oid roleid = GetUserId();
    if (!has_auditadmin_privilege(roleid)) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("permission denied to delete audit")));
    }

    /* 
     * load the index audit table from global index audit table instance
     * then use the local thread one when iterate all audit files
     */
    pgaudit_read_indexfile(g_instance.attr.attr_security.Audit_directory);
    LWLockAcquire(AuditIndexFileLock, LW_SHARED);
    if (g_instance.audit_cxt.audit_indextbl != NULL) {
        int indextbl_len =
            (u_sess->attr.attr_security.Audit_RemainThreshold + 1) * sizeof(AuditIndexItem) + indextbl_header_size;
        t_thrd.audit.audit_indextbl = (AuditIndexTableNew *)palloc0(indextbl_len);
        error_t errorno =
            memcpy_s(t_thrd.audit.audit_indextbl, indextbl_len, g_instance.audit_cxt.audit_indextbl, indextbl_len);
        securec_check(errorno, "\0", "\0");
    }
    LWLockRelease(AuditIndexFileLock);

    if (begtime < endtime && (t_thrd.audit.audit_indextbl != NULL) && t_thrd.audit.audit_indextbl->count > 0) {
        bool satisfied = false;
        uint32 index;
        uint32 fnum;
        AuditIndexItem* item = NULL;

        index = t_thrd.audit.audit_indextbl->begidx;
        do {
            item = t_thrd.audit.audit_indextbl->data + index;
            fnum = item->filenum;

            /* check whether system changed when auditor write audit data to current file */
            satisfied = pgaudit_check_system(begtime, endtime, index);
            if (satisfied) {
                pgaudit_delete_file(fnum, begtime, endtime);
                satisfied = false;
            }

            if (index == t_thrd.audit.audit_indextbl->latest_idx - 1) {
                break;
            }

            index = (index + 1) % t_thrd.audit.audit_indextbl->maxnum;
        } while (true);
    }

    pfree_ext(t_thrd.audit.audit_indextbl);
    PG_RETURN_VOID();
}

/* 
 * if use_elastic_search is set on, user should make sure connection is ok, 
 * or process will not start successfully
 */
static void elasic_search_connection_test()
{
    if (!g_instance.attr.attr_security.use_elastic_search) {
        return;
    }
    std::string url = ((std::string) g_instance.attr.attr_security.elastic_search_ip_addr) + 
                      ((std::string) ":9200/audit/events/_bulk?pretty");
    (void)m_curlUtils.http_post_file_request(url, "", true);
}

/* 
 * check and reinit the audit files
 * 1. whether the audit file is exist
 * 2. recognize the corrupt file
 * 3. try to reinit audit file if sysauditFile is NULL
 */
static void CheckAuditFile(void)
{
    uint32 fnum = 0;
    AuditIndexItem *item = NULL;
    struct stat statBuf;
    errno_t rc;
    int thread_idx = t_thrd.audit.cur_thread_idx;

    LWLockAcquire(AuditIndexFileLock, LW_SHARED);
    item = g_instance.audit_cxt.audit_indextbl->data + g_instance.audit_cxt.audit_indextbl->curidx[thread_idx];
    fnum = item->filenum;
    LWLockRelease(AuditIndexFileLock);

    rc = snprintf_s(t_thrd.audit.pgaudit_filepath, MAXPGPATH, MAXPGPATH - 1, pgaudit_filename,
                    g_instance.attr.attr_security.Audit_directory, fnum);
    securec_check_ss(rc, "\0", "\0");

    /*
     * If the current write file is not exist, we'll reinit the sysauditFile.
     * Also, when we querying the auditfile and finding invalid header file,
     * we'll truncate it. And in there we'll reinit it too.
     * allow error here as process have been rnning but not startup
     */
    if (lstat(t_thrd.audit.pgaudit_filepath, &statBuf) == -1 ||
        (lstat(t_thrd.audit.pgaudit_filepath, &statBuf) == 0 && statBuf.st_size == 0)) {
        if (t_thrd.audit.sysauditFile != NULL) {
            fclose(t_thrd.audit.sysauditFile);
            t_thrd.audit.sysauditFile = NULL;
        }
    }

    /* reinit audit file if corrupted */
    uint32 corrupt_audit_fnum = pg_atomic_read_u32(&g_instance.audit_cxt.audit_coru_fnum[thread_idx]);
    if (corrupt_audit_fnum != UINT32_MAX && corrupt_audit_fnum == fnum) {
        ereport(WARNING, (errmsg("invalid data in audit file fnum %d", fnum)));

        /* truncate the current file */
        int fd = open(t_thrd.audit.pgaudit_filepath, O_RDWR | O_TRUNC, pgaudit_filemode);
        if (fd < 0) {
            ereport(ERROR, (errcode_for_file_access(),
                errmsg("could not truncate audit file \"%s\": %m", t_thrd.audit.pgaudit_filepath)));
        } else {
            close(fd);
        }

        /* audit file will init after make sysauditFile NULL generating the new file audit log the same time */
        if (t_thrd.audit.sysauditFile != NULL) {
            fclose(t_thrd.audit.sysauditFile);
            t_thrd.audit.sysauditFile = NULL;
        }

        pg_atomic_write_u32(&g_instance.audit_cxt.audit_coru_fnum[thread_idx], UINT32_MAX);
    }

    /*
     * make sure init audit file if pgaudit_filepath accessable
     * directly return when sysauditFile exist
     */
    auditfile_init(true);
}

static bool pgaudit_invalid_header(const AuditMsgHdr* header)
{
    return ((header->signature[0]) != 'A' || header->signature[1] != 'U' || header->version != 0 ||
        !(header->fields == (PGAUDIT_QUERY_COLS - 1) || header->fields == PGAUDIT_QUERY_COLS) ||
        (header->size <= sizeof(AuditMsgHdr)) ||
        (header->size >= (uint32)u_sess->attr.attr_security.Audit_RotationSize *  1024L));
}

/*
 *  mark corrupt fnum by postgres thread
 *  used for reinit audit files in audit thread
 */
static void pgaudit_mark_corrupt_info(uint32 fnum)
{
    /*
     * only the writing audit files could be mark corrupt info
     * ignore the old audit files here
     */
    int thread_num = g_instance.audit_cxt.thread_num;

    LWLockAcquire(AuditIndexFileLock, LW_SHARED);

    /*
     * iterate the all writing index looking for the thread idx of fnum
     */
    int thread_idx = -1;
    for (int i = 0; i < thread_num; ++i) {
        AuditIndexItem *item =
            g_instance.audit_cxt.audit_indextbl->data + g_instance.audit_cxt.audit_indextbl->curidx[i];
        if (fnum == item->filenum) {
            thread_idx = i;
            break;
        }
    }

    LWLockRelease(AuditIndexFileLock);

    /* old audit files */
    if (thread_idx == -1) {
        return;
    }

    ereport(WARNING, (errmsg("audit file num %d is corrupted.", fnum)));
    
    /*
     * if any other thread have updated the audit_fnum or fnum is older one, do nothing but break here
     */
    uint32 audit_corrupt_fnum = pg_atomic_read_u32(&g_instance.audit_cxt.audit_coru_fnum[thread_idx]);
    if (audit_corrupt_fnum < fnum || audit_corrupt_fnum == UINT32_MAX) {
        while (!pg_atomic_compare_exchange_u32(&g_instance.audit_cxt.audit_coru_fnum[thread_idx], &audit_corrupt_fnum,
                                               fnum)) {
            audit_corrupt_fnum = pg_atomic_read_u32(&g_instance.audit_cxt.audit_coru_fnum[thread_idx]);
            if (audit_corrupt_fnum >= fnum && audit_corrupt_fnum != UINT32_MAX) {
                break;
            }
        }
    }
}

static void audit_append_xid_info(const char *detail_info, char *detail_info_xid, uint32 len)
{
    Assert(u_sess->attr.attr_security.audit_xid_info == 1);
    int rc = 0;
    TransactionId xid = InvalidTransactionId;
    if (IsTransactionState() && !RecoveryInProgress() &&
        !(t_thrd.xlog_cxt.LocalXLogInsertAllowed == 0 && g_instance.streaming_dr_cxt.isInSwitchover == true)) {
        xid = GetCurrentTransactionId();
        rc = snprintf_s(detail_info_xid, len, len - 1, "xid=%llu, %s", xid, detail_info);
        securec_check_ss(rc, "\0", "\0");
    } else {
        rc = snprintf_s(detail_info_xid, len, len - 1, "xid=NA, %s", detail_info);
        securec_check_ss(rc, "\0", "\0");
    }
}

/*
 * Brief        : audit process init for multi-thread manage
 * Description  : init audit global env for audit threads including
 * 1. index file lock
 * 2. pipes for audit
 * 3. audit logs & path
 * 4. audit index file
 */
static void audit_process_cxt_init()
{
    Assert(t_thrd.role == AUDITOR);
    Assert(t_thrd.audit.cur_thread_idx == 0);

    /* return directly when audit process init have done */
    if (g_instance.audit_cxt.audit_init_done == 1) {
        return;
    }

    ereport(LOG, (errmsg("audit_process_cxt_init enter")));
    errno_t errorno = 0;
    int thread_num = g_instance.attr.attr_security.audit_thread_num;

    MemoryContext oldcontext = MemoryContextSwitchTo(g_instance.audit_cxt.global_audit_context);
    g_instance.audit_cxt.thread_num = thread_num;

    /* init all pipes for all audit threads */
    if (g_instance.audit_cxt.sys_audit_pipes == NULL) {
        g_instance.audit_cxt.sys_audit_pipes =
            (int *)palloc0(sizeof(int) * 2 * thread_num); /* 2 descriptor for one pipe */
        int *&pipes = g_instance.audit_cxt.sys_audit_pipes;
        errorno = memset_s(pipes, sizeof(pipes), -1, sizeof(pipes));
        securec_check(errorno, "\0", "\0");
        for (int i = 0; i < thread_num; ++i) {
            if (pipe(&pipes[PIPE_READ_INDEX(i)]) < 0) {
                ereport(FATAL, (errcode_for_socket_access(), (errmsg("could not create pipe for sysaudit: %m"))));
            }
            ereport(LOG, (errmsg("audit_process_cxt_init pipe init successfully for pipe : %d file descriptor: %d", i,
                                 g_instance.audit_cxt.sys_audit_pipes[i * 2])));
        }
    }

    /* init audit path */
    char Audit_directory_Done[MAXPGPATH] = {0};
    int rc = snprintf_s(Audit_directory_Done, sizeof(Audit_directory_Done), sizeof(Audit_directory_Done) - 1, "%s/done",
        g_instance.attr.attr_security.Audit_directory);
    securec_check_ss(rc, "\0", "\0");
    (void)pg_mkdir_p(g_instance.attr.attr_security.Audit_directory, S_IRWXU);
    (void)pg_mkdir_p(Audit_directory_Done, S_IRWXU);

    /* init index file & hold the content into g_instance.audit_cxt.audit_indextbl */
    g_instance.audit_cxt.audit_indextbl = NULL;
    pgaudit_indextbl_init_new();
    pgaudit_update_indexfile(PG_BINARY_A, false);

    g_instance.audit_cxt.audit_init_done = 1;

    (void)MemoryContextSwitchTo(oldcontext);
    ereport(LOG, (errmsg("audit_process_cxt_init success")));
}

/*
 * Brief        : audit process exit
 * Description  : when exit the audit thread in PM thread, release related pipes
 * audit master thread will do the index audit file flush job, not do it here
 */
static void audit_process_cxt_exit()
{
    Assert(t_thrd.role == AUDITOR);
    if (g_instance.audit_cxt.audit_init_done == 0) {
        return;
    }
    g_instance.audit_cxt.audit_init_done = 0;

    /* close unused reading and writing end */
    int thread_num = g_instance.attr.attr_security.audit_thread_num;
    int *sys_audit_pipe = g_instance.audit_cxt.sys_audit_pipes;

    /* for close all pipes safely, wait sub audit thread exited here */
    while (true) {
        if (pg_atomic_read_u32(&g_instance.audit_cxt.current_audit_index) == 1) {
            break;
        }
    }

    for (int i = 0; i < thread_num; ++i) {
        if (sys_audit_pipe[PIPE_READ_INDEX(i)] > 0) {
            close(sys_audit_pipe[PIPE_READ_INDEX(i)]);
            sys_audit_pipe[PIPE_READ_INDEX(i)] = -1;
        }
    }
    pfree_ext(g_instance.audit_cxt.sys_audit_pipes);

    LWLockAcquire(AuditIndexFileLock, LW_EXCLUSIVE);
    pfree_ext(g_instance.audit_cxt.audit_indextbl);
    LWLockRelease(AuditIndexFileLock);
}

/*
 * Brief        : load audit thread index
 * Description  :
 */
int audit_load_thread_index()
{
    if (t_thrd.audit.cur_thread_idx != -1) {
        return t_thrd.audit.cur_thread_idx;
    }

    int idx = pg_atomic_fetch_add_u32(&g_instance.audit_cxt.current_audit_index, 1);
    t_thrd.audit.cur_thread_idx = idx;

    Assert(t_thrd.audit.cur_thread_idx >= 0);
    Assert(t_thrd.audit.cur_thread_idx < g_instance.audit_cxt.thread_num);

    return t_thrd.audit.cur_thread_idx;
}

/*
 * Brief        : get audit file num for current thread index
 * Description  :
 */
uint32 pgaudit_get_auditfile_num()
{
    Assert(t_thrd.role == AUDITOR);

    uint32 fnum = 0;
    int thread_idx = t_thrd.audit.cur_thread_idx;
    LWLockAcquire(AuditIndexFileLock, LW_SHARED);
    AuditIndexItem *item =
        g_instance.audit_cxt.audit_indextbl->data + g_instance.audit_cxt.audit_indextbl->curidx[thread_idx];
    fnum = item->filenum;
    LWLockRelease(AuditIndexFileLock);
    return fnum;
}

/*
 * Brief        : update index table file when new file openned
 * Description  : flush index table file will just invoke later
 */
void pgaudit_update_auditfile_time(pg_time_t timestamp, bool exist)
{
    int thread_idx = t_thrd.audit.cur_thread_idx;
    LWLockAcquire(AuditIndexFileLock, LW_EXCLUSIVE);
    if (!exist) {
        AuditIndexItem *item =
            g_instance.audit_cxt.audit_indextbl->data + g_instance.audit_cxt.audit_indextbl->curidx[thread_idx];
        item->ctime = timestamp;
        ++g_instance.audit_cxt.audit_indextbl->count;
    }
    LWLockRelease(AuditIndexFileLock);
}

/*
 * Brief        : iterate the next audit file & update the index table info
 * Description  : calc the total space here when rotate one audit file
 */
void pgaudit_switch_next_auditfile()
{
    AuditIndexItem *item = NULL;
    uint32 new_fnum = 0;
    int thread_idx = t_thrd.audit.cur_thread_idx;

    LWLockAcquire(AuditIndexFileLock, LW_EXCLUSIVE);

    /* update the current item filesize */
    item = g_instance.audit_cxt.audit_indextbl->data + g_instance.audit_cxt.audit_indextbl->curidx[thread_idx];
    item->filesize = ftell(t_thrd.audit.sysauditFile);

    /* update the last_audit_time for audit_resource_policy 0 policy */
    pg_time_t curtime = time(NULL);
    g_instance.audit_cxt.audit_indextbl->last_audit_time = curtime;

    /* update total space */
    g_instance.audit_cxt.pgaudit_totalspace += item->filesize;

    /* get the next writing audit file index */
    uint32 current_max_fnum = pgaudit_get_max_fnum(g_instance.audit_cxt.thread_num);
    uint32 new_idx = pg_atomic_fetch_add_u32(&g_instance.audit_cxt.audit_indextbl->latest_idx, 1);
    g_instance.audit_cxt.audit_indextbl->latest_idx =
        (g_instance.audit_cxt.audit_indextbl->latest_idx) % g_instance.audit_cxt.audit_indextbl->maxnum;
    g_instance.audit_cxt.audit_indextbl->curidx[thread_idx] = (new_idx) % g_instance.audit_cxt.audit_indextbl->maxnum;

    item = g_instance.audit_cxt.audit_indextbl->data + g_instance.audit_cxt.audit_indextbl->curidx[thread_idx];
    item->filenum = ++current_max_fnum;

    LWLockRelease(AuditIndexFileLock);
    ereport(DEBUG1, (errmsg("pgaudit_switch_next_auditfile new fnum :%d cur pgaudit_totalspace: %ld MB", new_fnum,
        g_instance.audit_cxt.pgaudit_totalspace)));
}

/*
 * Brief        : do the thread index decreasing when thread exit
 * Description  :
 */
static void pgauditor_kill(int code, Datum arg)
{
    pg_atomic_fetch_sub_u32(&g_instance.audit_cxt.current_audit_index, 1);
}

/*
 * Brief        : check auditor thread
 * Description  :
 */
bool pg_auditor_thread(ThreadId pid)
{
    if (g_instance.pid_cxt.PgAuditPID == NULL) {
        return false;
    }
    for (int i = 0; i < g_instance.audit_cxt.thread_num; ++i) {
        if (pid == g_instance.pid_cxt.PgAuditPID[i]) {
            return true;
        }
    }
    return false;
}
