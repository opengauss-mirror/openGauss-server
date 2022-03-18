/* -------------------------------------------------------------------------
 *
 * pgstat.cpp
 *
 *	All the statistics collector stuff hacked up in one big, ugly file.
 *
 *			- Separate collector, postmaster and backend stuff
 *			  into different files.
 *
 *			- Add some automatic call for pgstat vacuuming.
 *
 *			- Add a pgstat config column to pg_database, so this
 *			  entire thing can be enabled/disabled on a per db basis.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *	Copyright (c) 2001-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/postmaster/pgstat.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "libpq/pqformat.h"
#include <fcntl.h>
#include <sys/param.h>
#include <sys/time.h>
#include <arpa/inet.h>
#include "funcapi.h"
#include "utils/syscache.h"
#include "pgstat.h"
#include "access/gtm.h"
#include "access/heapam.h"
#include "utils/lsyscache.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "commands/vacuum.h"
#include "commands/user.h"
#include "gaussdb_version.h"
#include "foreign/fdwapi.h"
#include "gssignal/gs_signal.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pg_trace.h"
#include "postmaster/autovacuum.h"
#include "postmaster/fork_process.h"
#include "postmaster/postmaster.h"
#include "postmaster/pagewriter.h"
#include "replication/catchup.h"
#include "replication/walsender.h"
#include "storage/backendid.h"
#include "storage/smgr/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/pg_shmem.h"
#include "storage/procsignal.h"
#include "storage/procarray.h"
#include "threadpool/threadpool.h"
#include "utils/ascii.h"
#include "utils/atomic.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/hotkey.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/timestamp.h"
#include "utils/snapmgr.h"
#include "gssignal/gs_signal.h"
#include "utils/builtins.h"
#include "storage/proc.h"
#include "workload/workload.h"
#include "workload/gscgroup.h"
#include "utils/memprot.h"
#include "pgxc/poolmgr.h"
#include "access/multi_redo_api.h"
#include "instruments/instr_unique_sql.h"
#include "instruments/instr_event.h"
#include "instruments/instr_slow_query.h"
#include "instruments/instr_statement.h"
#include "instruments/instr_handle_mgr.h"

#ifdef ENABLE_UT
#define static
#endif

#define pg_stat_relation(flag) (InvalidOid == (flag))

/* ----------
 * Timer definitions.
 * ----------
 */
#define PGSTAT_STAT_INTERVAL                   \
    500 /* Minimum time between stats file \ \ \
         * updates; in milliseconds. */

#define PGSTAT_RETRY_DELAY                        \
    10 /* How long to wait between checks for \ \ \
        * a new file; in milliseconds. */

#define PGSTAT_MAX_WAIT_TIME                      \
    10000 /* Maximum time to wait for a stats \ \ \
           * file update; in milliseconds. */

#define PGSTAT_INQ_INTERVAL                        \
    640 /* How often to ping the collector for \ \ \
         * a new file; in milliseconds. */

#define PGSTAT_RESTART_INTERVAL                 \
    60 /* How often to attempt to restart a \ \ \
        * failed statistics collector; in   \ \ \
        * seconds. */

#define PGSTAT_POLL_LOOP_COUNT (PGSTAT_MAX_WAIT_TIME / PGSTAT_RETRY_DELAY)
#define PGSTAT_INQ_LOOP_COUNT (PGSTAT_INQ_INTERVAL / PGSTAT_RETRY_DELAY)

/* Minimum receive buffer size for the collector's socket. */
#define PGSTAT_MIN_RCVBUF (100 * 1024)

/* ----------
 * The initial size hints for the hash tables used in the collector.
 * ----------
 */
#define PGSTAT_DB_HASH_SIZE 16
#define PGSTAT_TAB_HASH_SIZE 512
#define PGSTAT_FUNCTION_HASH_SIZE 512

/* ----------
 * Macros of the os statistic file system path.
 * ----------
 */
#define JiffiesGetCentiSec(x) ((x) * (100 / HZ))
#define ProcPathMax 4096
#define VmStatFileReadBuffer 4096
#define SysFileSystemPath "/sys/devices/system"
#define SysCpuPath "/sys/devices/system/cpu/cpu%u"
#define ThreadSiblingFile "/sys/devices/system/cpu/cpu0/topology/thread_siblings"
#define CoreSiblingFile "/sys/devices/system/cpu/cpu0/topology/core_siblings"

extern void WLMGetCPUDataIndicator(PgBackendStatus*, WLMDataIndicator<int64>*);

/*
 * Structures in which backends store per-table info that's waiting to be
 * sent to the collector.
 *
 * NOTE: once allocated, TabStatusArray structures are never moved or deleted
 * for the life of the backend.  Also, we zero out the t_id fields of the
 * contained PgStat_TableStatus structs whenever they are not actively in use.
 * This allows relcache pgstat_info pointers to be treated as long-lived data,
 * avoiding repeated searches in pgstat_initstats() when a relation is
 * repeatedly opened during a transaction.
 */
#define TABSTAT_QUANTUM 100 /* we alloc this many at a time */

typedef struct TabStatusArray {
    struct TabStatusArray* tsa_next;                 /* link to next array, if any */
    int tsa_used;                                    /* # entries currently used */
    PgStat_TableStatus tsa_entries[TABSTAT_QUANTUM]; /* per-table data */
} TabStatusArray;

/* pgStatTabHash entry */
typedef struct TabStatHashEntry {
    PgStat_StatTabKey t_key;
    PgStat_TableStatus* tsa_entry;
} TabStatHashEntry;

/*
 * Tuple insertion/deletion counts for an open transaction can't be propagated
 * into PgStat_TableStatus counters until we know if it is going to commit
 * or abort.  Hence, we keep these counts in per-subxact structs that live
 * in u_sess->top_transaction_mem_cxt.  This data structure is designed on the assumption
 * that subxacts won't usually modify very many tables.
 */
typedef struct PgStat_SubXactStatus {
    int nest_level;                    /* subtransaction nest level */
    struct PgStat_SubXactStatus* prev; /* higher-level subxact if any */
    PgStat_TableXactStatus* first;     /* head of list for this subxact */
} PgStat_SubXactStatus;

/* Record that's written to 2PC state file when pgstat state is persisted */
typedef struct TwoPhasePgStatRecord {
    PgStat_Counter tuples_inserted;    /* tuples inserted in xact */
    PgStat_Counter tuples_updated;     /* tuples updated in xact */
    PgStat_Counter tuples_deleted;     /* tuples deleted in xact */
    PgStat_Counter tuples_inplace_updated;
    PgStat_Counter inserted_pre_trunc; /* tuples inserted prior to truncate */
    PgStat_Counter updated_pre_trunc;  /* tuples updated prior to truncate */
    PgStat_Counter deleted_pre_trunc;  /* tuples deleted prior to truncate */
    PgStat_Counter inplace_updated_pre_trunc;
    /* The following members with _accum suffix will not be erased by truncate */
    PgStat_Counter tuples_inserted_accum;
    PgStat_Counter tuples_updated_accum;
    PgStat_Counter tuples_deleted_accum;
    PgStat_Counter tuples_inplace_updated_accum;

    Oid t_id;                          /* table's OID */
    bool t_shared;                     /* is it a shared catalog? */
    bool t_truncated;                  /* was the relation truncated? */
    /*
     * if t_id is a parition oid , then t_statFlag is the corresponding
     * partitioned table oid;  if t_id is a non-parition oid, then t_statFlag is InvlaidOId
     */
    uint32 t_statFlag; /* corresponding partitioned table oid */
} TwoPhasePgStatRecord;

/*
 * Original value for u_sess->stat_cxt.osStatDescArray. For most members,
 * the values are fixed, and for got, we initialize it to false.
 */
const OSRunInfoDesc osStatDescArrayOrg[TOTAL_OS_RUN_INFO_TYPES] = {
    /* cpu numbers */
    {Int32GetNumberDatum, "NUM_CPUS", false, false, "Number of CPUs or processors available"},

    {Int32GetNumberDatum,
        "NUM_CPU_CORES",
        false,
        false,
        "Number of CPU cores available (includes subcores of multicore CPUs as well as single-core CPUs)"},

    {Int32GetNumberDatum,
        "NUM_CPU_SOCKETS",
        false,
        false,
        "Number of CPU sockets available (represents an absolute count of CPU chips on the system, regardless of "
        "multithreading or multi-core architectures)"},

    /* cpu times */
    {Int64GetNumberDatum,
        "IDLE_TIME",
        true,
        false,
        "Number of hundredths of a second that a processor has been idle, totalled over all processors"},

    {Int64GetNumberDatum,
        "BUSY_TIME",
        true,
        false,
        "Number of hundredths of a second that a processor has been busy executing user or kernel code, totalled over "
        "all processors"},

    {Int64GetNumberDatum,
        "USER_TIME",
        true,
        false,
        "Number of hundredths of a second that a processor has been busy executing user code, totalled over all "
        "processors"},

    {Int64GetNumberDatum,
        "SYS_TIME",
        true,
        false,
        "Number of hundredths of a second that a processor has been busy executing kernel code, totalled over all "
        "processors"},

    {Int64GetNumberDatum,
        "IOWAIT_TIME",
        true,
        false,
        "Number of hundredths of a second that a processor has been waiting for I/O to complete, totalled over all "
        "processors"},

    {Int64GetNumberDatum,
        "NICE_TIME",
        true,
        false,
        "Number of hundredths of a second that a processor has been busy executing low-priority user code, totalled "
        "over all processors"},

    /* avg cpu times */
    {Int64GetNumberDatum,
        "AVG_IDLE_TIME",
        true,
        false,
        "Number of hundredths of a second that a processor has been idle, averaged over all processors"},

    {Int64GetNumberDatum,
        "AVG_BUSY_TIME",
        true,
        false,
        "Number of hundredths of a second that a processor has been busy executing user or kernel code, averaged over "
        "all processors"},

    {Int64GetNumberDatum,
        "AVG_USER_TIME",
        true,
        false,
        "Number of hundredths of a second that a processor has been busy executing user code, averaged over all "
        "processors"},

    {Int64GetNumberDatum,
        "AVG_SYS_TIME",
        true,
        false,
        "Number of hundredths of a second that a processor has been busy executing kernel code, averaged over all "
        "processors"},

    {Int64GetNumberDatum,
        "AVG_IOWAIT_TIME",
        true,
        false,
        "Number of hundredths of a second that a processor has been waiting for I/O to complete, averaged over all "
        "processors"},

    {Int64GetNumberDatum,
        "AVG_NICE_TIME",
        true,
        false,
        "Number of hundredths of a second that a processor has been busy executing low-priority user code, averaged "
        "over all processors"},

    /* virtual memory page in/out data */
    {Int64GetNumberDatum,
        "VM_PAGE_IN_BYTES",
        true,
        false,
        "Total number of bytes of data that have been paged in due to virtual memory paging"},

    {Int64GetNumberDatum,
        "VM_PAGE_OUT_BYTES",
        true,
        false,
        "Total number of bytes of data that have been paged out due to virtual memory paging"},

    /* os run load */
    {Float8GetNumberDatum,
        "LOAD",
        false,
        false,
        "Current number of processes that are either running or in the ready state, waiting to be selected by the "
        "operating-system scheduler to run. On many platforms, this statistic reflects the average load over the past "
        "minute."},

    /* physical memory size */
    {Int64GetNumberDatum, "PHYSICAL_MEMORY_BYTES", false, false, "Total number of bytes of physical memory"}};

/* ----------
 * Local function forward declarations
 * ----------
 */
static void pgstat_exit(SIGNAL_ARGS);
static void pgstat_beshutdown_hook(int code, Datum arg);
static void pgstat_sighup_handler(SIGNAL_ARGS);

static PgStat_StatDBEntry* pgstat_get_db_entry(Oid databaseid, bool create);
static PgStat_StatTabEntry* pgstat_get_tab_entry(
    PgStat_StatDBEntry* dbentry, Oid tableoid, bool create, uint32 statFlag);
static void pgstat_write_statsfile(bool permanent);
static HTAB* pgstat_read_statsfile(Oid onlydb, bool permanent);
static void backend_read_statsfile(void);

static void pgstat_send_tabstat(PgStat_MsgTabstat* tsmsg);
static void pgstat_send_funcstats(void);
static HTAB* pgstat_collect_oids(Oid catalogid);
static HTAB* pgstat_collect_tabkeys(void);
static PgStat_TableStatus* get_tabstat_entry(Oid rel_id, bool isshared, uint32 statFlag);
static void pgstat_collect_thread_status_setup_memcxt(void);
static void pgstat_collect_thread_status_clear_resource(void);

const char* pgstat_get_wait_io(WaitEventIO w);

static void pgstat_setheader(PgStat_MsgHdr* hdr, StatMsgType mtype);
void pgstat_send(void* msg, int len);

static void pgstat_recv_inquiry(PgStat_MsgInquiry* msg, int len);
static void pgstat_recv_tabstat(PgStat_MsgTabstat* msg, int len);
static void pgstat_recv_tabpurge(PgStat_MsgTabpurge* msg, int len);
static void pgstat_recv_dropdb(PgStat_MsgDropdb* msg, int len);
static void pgstat_recv_resetcounter(PgStat_MsgResetcounter* msg, int len);
static void pgstat_recv_resetsharedcounter(PgStat_MsgResetsharedcounter* msg, int len);
static void pgstat_recv_resetsinglecounter(PgStat_MsgResetsinglecounter* msg, int len);
static void pgstat_recv_autovac(PgStat_MsgAutovacStart* msg, int len);
static void pgstat_recv_vacuum(PgStat_MsgVacuum* msg, int len);
static void pgstat_recv_data_changed(PgStat_MsgDataChanged* msg, int len);
static void pgstat_recv_truncate(PgStat_MsgTruncate* msg, int len);
static void pgstat_recv_analyze(PgStat_MsgAnalyze* msg, int len);
static void pgstat_recv_bgwriter(PgStat_MsgBgWriter* msg, int len);
static void pgstat_recv_funcstat(PgStat_MsgFuncstat* msg, int len);
static void pgstat_recv_funcpurge(PgStat_MsgFuncpurge* msg, int len);
static void pgstat_recv_recoveryconflict(const PgStat_MsgRecoveryConflict* msg);
static void pgstat_recv_deadlock(const PgStat_MsgDeadlock* msg);
static void pgstat_recv_tempfile(PgStat_MsgTempFile* msg, int len);
static void pgstat_recv_memReserved(const PgStat_MsgMemReserved* msg);
static void pgstat_recv_autovac_stat(PgStat_MsgAutovacStat* msg, int len);
static void PgstatRecvPrunestat(PgStat_MsgPrune* msg, int len);

static void pgstat_send_badblock_stat(void);
static void pgstat_recv_badblock_stat(PgStat_MsgBadBlock* msg, int len);

static bool checkSysFileSystem(void);
static bool checkLogicalCpu(uint32 cpuNum);
static uint32 parseSiblingFile(const char* path);
static void pgstat_recv_filestat(PgStat_MsgFile* msg, int len);
static void prepare_calculate(SqlRTInfoArray* sql_rt_info, int* counter);
static void pgstat_recv_sql_responstime(PgStat_SqlRT* msg, int len);

void initGlobalBadBlockStat();

static void initMySessionStatEntry(void);
static void initMySessionTimeEntry(void);
static void initMySessionMemoryEntry(void);

static void AttachMySessionStatEntry(void);
static void AttachMySessionTimeEntry(void);
static void AttachMySessionMemoryEntry(void);

static void endMySessionTimeEntry(int code, Datum arg);
static void DetachMySessionTimeEntry(volatile SessionTimeEntry* pEntry);

/* ------------------------------------------------------------
 * Public functions called from postmaster follow
 * ------------------------------------------------------------
 */
/* ----------
 * pgstat_init() -
 *
 *	Called from postmaster at startup. Create the resources required
 *	by the statistics collector process.  If unable to do so, do not
 *	fail --- better to let the postmaster start with stats collection
 *	disabled.
 * ----------
 */
void pgstat_init(void)
{
    ACCEPT_TYPE_ARG3 alen;
    struct addrinfo *addrs = NULL, *addr = NULL, hints;
    int ret;
    fd_set rset;
    struct timeval tv;
    char test_byte;
    int sel_res;
    int tries = 0;
    struct sockaddr_storage pgStatAddr;

#define TESTBYTEVAL ((char)199)

    /*
     * Create the UDP socket for sending and receiving statistic messages
     */
    hints.ai_flags = AI_PASSIVE;
    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = 0;
    hints.ai_addrlen = 0;
    hints.ai_addr = NULL;
    hints.ai_canonname = NULL;
    hints.ai_next = NULL;
    ret = pg_getaddrinfo_all("localhost", NULL, &hints, &addrs);
    if (ret || (addrs == NULL)) {
        ereport(LOG, (errmsg("could not resolve \"localhost\": %s", gai_strerror(ret))));
        goto startup_failed;
    }

    /*
     * On some platforms, pg_getaddrinfo_all() may return multiple addresses
     * only one of which will actually work (eg, both IPv6 and IPv4 addresses
     * when kernel will reject IPv6).  Worse, the failure may occur at the
     * bind() or perhaps even connect() stage.	So we must loop through the
     * results till we find a working combination. We will generate LOG
     * messages, but no error, for bogus combinations.
     */
    for (addr = addrs; addr; addr = addr->ai_next) {
#ifdef HAVE_UNIX_SOCKETS
        /* Ignore AF_UNIX sockets, if any are returned. */
        if (addr->ai_family == AF_UNIX)
            continue;
#endif

        if (++tries > 1)
            ereport(LOG, (errmsg("trying another address for the statistics collector")));

        /*
         * Create the socket.
         */
        if ((g_instance.stat_cxt.pgStatSock = socket(addr->ai_family, SOCK_DGRAM, 0)) == PGINVALID_SOCKET) {
            ereport(LOG, (errcode_for_socket_access(), errmsg("could not create socket for statistics collector: %m")));
            continue;
        }

#ifdef F_SETFD
        if (fcntl(g_instance.stat_cxt.pgStatSock, F_SETFD, FD_CLOEXEC) == -1) {
            ereport(LOG,
                (errcode_for_socket_access(), errmsg("setsockopt(FD_CLOEXEC) failed for statistics collector: %m")));
            closesocket(g_instance.stat_cxt.pgStatSock);
            g_instance.stat_cxt.pgStatSock = PGINVALID_SOCKET;
            continue;
        }
#endif /* F_SETFD */

        /*
         * Bind it to a kernel assigned port on localhost and get the assigned
         * port via getsockname().
         */
        if (bind(g_instance.stat_cxt.pgStatSock, addr->ai_addr, addr->ai_addrlen) < 0) {
            ereport(LOG, (errcode_for_socket_access(), errmsg("could not bind socket for statistics collector: %m")));
            closesocket(g_instance.stat_cxt.pgStatSock);
            g_instance.stat_cxt.pgStatSock = PGINVALID_SOCKET;
            continue;
        }

        alen = sizeof(pgStatAddr);
        if (getsockname(g_instance.stat_cxt.pgStatSock, (struct sockaddr*)&pgStatAddr, &alen) < 0) {
            ereport(LOG,
                (errcode_for_socket_access(), errmsg("could not get address of socket for statistics collector: %m")));
            closesocket(g_instance.stat_cxt.pgStatSock);
            g_instance.stat_cxt.pgStatSock = PGINVALID_SOCKET;
            continue;
        }

        /*
         * Connect the socket to its own address.  This saves a few cycles by
         * not having to respecify the target address on every send. This also
         * provides a kernel-level check that only packets from this same
         * address will be received.
         */
        if (connect(g_instance.stat_cxt.pgStatSock, (struct sockaddr*)&pgStatAddr, alen) < 0) {
            ereport(
                LOG, (errcode_for_socket_access(), errmsg("could not connect socket for statistics collector: %m")));
            closesocket(g_instance.stat_cxt.pgStatSock);
            g_instance.stat_cxt.pgStatSock = PGINVALID_SOCKET;
            continue;
        }

        /*
         * Try to send and receive a one-byte test message on the socket. This
         * is to catch situations where the socket can be created but will not
         * actually pass data (for instance, because kernel packet filtering
         * rules prevent it).
         */
        test_byte = TESTBYTEVAL;

    retry1:
        errno = 0;
        if (send(g_instance.stat_cxt.pgStatSock, &test_byte, 1, 0) <= 0) {
            if (errno == EINTR)
                goto retry1; /* if interrupted, just retry */
            ereport(LOG,
                (errcode_for_socket_access(),
                    errmsg("could not send test message on socket for statistics collector: %m")));
            closesocket(g_instance.stat_cxt.pgStatSock);
            g_instance.stat_cxt.pgStatSock = PGINVALID_SOCKET;
            continue;
        }

        /*
         * There could possibly be a little delay before the message can be
         * received.  We arbitrarily allow up to half a second before deciding
         * it's broken.
         * need a loop to handle EINTR
         */
        for (;;) {
            FD_ZERO(&rset);
            if (g_instance.stat_cxt.pgStatSock + 1 > FD_SETSIZE) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("fd + 1 cannot be greater than FD_SETSIZE")));
            }
            FD_SET(g_instance.stat_cxt.pgStatSock, &rset);

            tv.tv_sec = 0;
            tv.tv_usec = 500000;
            sel_res = select(g_instance.stat_cxt.pgStatSock + 1, &rset, NULL, NULL, &tv);
            if (sel_res >= 0 || errno != EINTR)
                break;
        }
        if (sel_res < 0) {
            ereport(LOG, (errcode_for_socket_access(), errmsg("select() failed in statistics collector: %m")));
            closesocket(g_instance.stat_cxt.pgStatSock);
            g_instance.stat_cxt.pgStatSock = PGINVALID_SOCKET;
            continue;
        }
        if (sel_res == 0 || !FD_ISSET(g_instance.stat_cxt.pgStatSock, &rset)) {
            /*
             * This is the case we actually think is likely, so take pains to
             * give a specific message for it.
             *
             * errno will not be set meaningfully here, so don't use it.
             */
            ereport(LOG,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                    errmsg("test message did not get through on socket for statistics collector")));
            closesocket(g_instance.stat_cxt.pgStatSock);
            g_instance.stat_cxt.pgStatSock = PGINVALID_SOCKET;
            continue;
        }

        test_byte++; /* just make sure variable is changed */

    retry2:
        errno = 0;
        if (recv(g_instance.stat_cxt.pgStatSock, &test_byte, 1, 0) <= 0) {
            if (errno == EINTR)
                goto retry2; /* if interrupted, just retry */
            ereport(LOG,
                (errcode_for_socket_access(),
                    errmsg("could not receive test message on socket for statistics collector: %m")));
            closesocket(g_instance.stat_cxt.pgStatSock);
            g_instance.stat_cxt.pgStatSock = PGINVALID_SOCKET;
            continue;
        }

        if (test_byte != TESTBYTEVAL) {
            /* strictly paranoia ... */
            ereport(LOG,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("incorrect test message transmission on socket for statistics collector")));
            closesocket(g_instance.stat_cxt.pgStatSock);
            g_instance.stat_cxt.pgStatSock = PGINVALID_SOCKET;
            continue;
        }

        /* If we get here, we have a working socket */
        break;
    }

    /* Did we find a working address? */
    if ((addr == NULL) || g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET)
        goto startup_failed;

    /*
     * Set the socket to non-blocking IO.  This ensures that if the collector
     * falls behind, statistics messages will be discarded; backends won't
     * block waiting to send messages to the collector.
     */
    if (!pg_set_noblock(g_instance.stat_cxt.pgStatSock)) {
        ereport(LOG,
            (errcode_for_socket_access(), errmsg("could not set statistics collector socket to nonblocking mode: %m")));
        goto startup_failed;
    }

    /*
     * Try to ensure that the socket's receive buffer is at least
     * PGSTAT_MIN_RCVBUF bytes, so that it won't easily overflow and lose
     * data.  Use of UDP protocol means that we are willing to lose data under
     * heavy load, but we don't want it to happen just because of ridiculously
     * small default buffer sizes (such as 8KB on older Windows versions).
     */
    {
        int old_rcvbuf;
        int new_rcvbuf;
        ACCEPT_TYPE_ARG3 rcvbufsize = sizeof(old_rcvbuf);

        if (getsockopt(g_instance.stat_cxt.pgStatSock, SOL_SOCKET, SO_RCVBUF, (char*)&old_rcvbuf, &rcvbufsize) < 0) {
            elog(LOG, "getsockopt(SO_RCVBUF) failed: %m");
            /* if we can't get existing size, always try to set it */
            old_rcvbuf = 0;
        }

        new_rcvbuf = PGSTAT_MIN_RCVBUF;
        if (old_rcvbuf < new_rcvbuf) {
            if (setsockopt(
                    g_instance.stat_cxt.pgStatSock, SOL_SOCKET, SO_RCVBUF, (char*)&new_rcvbuf, sizeof(new_rcvbuf)) <
                0) {
                elog(LOG, "setsockopt(SO_RCVBUF) failed: %m");
            }
        }
    }

    pg_freeaddrinfo_all(hints.ai_family, addrs);

    initGlobalBadBlockStat();

    return;

startup_failed:
    ereport(LOG, (errmsg("disabling statistics collector for lack of working socket")));

    if (NULL != addrs)
        pg_freeaddrinfo_all(hints.ai_family, addrs);

    if (g_instance.stat_cxt.pgStatSock != PGINVALID_SOCKET)
        closesocket(g_instance.stat_cxt.pgStatSock);
    g_instance.stat_cxt.pgStatSock = PGINVALID_SOCKET;

    /*
     * Adjust GUC variables to suppress useless activity, and for debugging
     * purposes (seeing track_counts off is a clue that we failed here). We
     * use PGC_S_OVERRIDE because there is no point in trying to turn it back
     * on from postgresql.conf without a restart.
     */
    SetConfigOption("track_counts", "off", PGC_INTERNAL, PGC_S_OVERRIDE);
}

/*
 * pgstat_reset_all() -
 *
 * Remove the stats file.  This is currently used only if WAL
 * recovery is needed after a crash.
 */
void pgstat_reset_all(void)
{
    elog(LOG,
        "[Pgstat] remove statfiles in %s, %s",
        u_sess->stat_cxt.pgstat_stat_filename,
        PGSTAT_STAT_PERMANENT_FILENAME);
    unlink(u_sess->stat_cxt.pgstat_stat_filename);
    unlink(PGSTAT_STAT_PERMANENT_FILENAME);
}

/*
 * pgstat_start() -
 *
 *	Called from postmaster at startup or after an existing collector
 *	died.  Attempt to fire up a fresh statistics collector.
 *
 *	Returns PID of child process, or 0 if fail.
 *
 *	Note: if fail, we will be called again from the postmaster main loop.
 */
ThreadId pgstat_start(void)
{
    time_t curtime;

    /*
     * Check that the socket is there, else pgstat_init failed and we can do
     * nothing useful.
     */
    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET)
        return 0;

    /*
     * Do nothing if too soon since last collector start.  This is a safety
     * valve to protect against continuous respawn attempts if the collector
     * is dying immediately at launch.	Note that since we will be re-called
     * from the postmaster main loop, we will get another chance later.
     */
    curtime = time(NULL);
    if ((unsigned int)(curtime - g_instance.stat_cxt.last_pgstat_start_time) < (unsigned int)PGSTAT_RESTART_INTERVAL)
        return 0;
    g_instance.stat_cxt.last_pgstat_start_time = curtime;

    return initialize_util_thread(PGSTAT);
}

void allow_immediate_pgstat_restart(void)
{
    g_instance.stat_cxt.last_pgstat_start_time = 0;
}

/* ------------------------------------------------------------
 * Public functions used by backends follow
 * ------------------------------------------------------------
 */
/* sizeof(TabStatusArray) = 14416 ~~ 15KB
 * we assume that the uplimit memory size of u_sess->stat_cxt.pgStatTabList should be
 * 2MB. that is, there are at most 128~136 TabStatusArray items. so
 * 128 is chosen. if it's bigger than this limit, u_sess->stat_cxt.pgStatTabList should
 * be destroyed.
 */
static inline bool pgstat_tablist_too_big(const int len)
{
    return (len >= 128);
}

/* destroy current u_sess->stat_cxt.pgStatTabList and make it be null */
static void pgstat_free_tablist(void)
{
    TabStatusArray* currItem = NULL;
    TabStatusArray* nextItem = NULL;

    currItem = u_sess->stat_cxt.pgStatTabList;

    /* make u_sess->stat_cxt.pgStatTabList be null and then free it */
    for (; currItem != NULL; currItem = nextItem) {
        /* record the next item to free */
        nextItem = currItem->tsa_next;
        pfree_ext(currItem);
    }
    u_sess->stat_cxt.pgStatTabList = NULL;
}

/* ----------
 * pgstat_report_stat() -
 *
 *	Called from tcop/postgres.c to send the so far collected per-table
 *	and function usage statistics to the collector.  Note that this is
 *	called only when not within a transaction, so it is fair to use
 *	transaction stop time as an approximation of current time.
 * ----------
 */
void pgstat_report_stat(bool force)
{
    /* we assume this inits to all zeroes: */
    static const PgStat_TableCounts all_zeroes = {0};

    TimestampTz now;
    PgStat_MsgTabstat regular_msg;
    PgStat_MsgTabstat shared_msg;
    TabStatusArray* tsa = NULL;
    int i;
    int tablist_len = 0; /* length of u_sess->stat_cxt.pgStatTabList */
    bool force_to_destory = false;
    errno_t rc = EOK;

    /* Don't expend a clock check if nothing to do */
    bool stat_no_change = ((u_sess->stat_cxt.pgStatTabList == NULL || u_sess->stat_cxt.pgStatTabList->tsa_used == 0) &&
                           !u_sess->stat_cxt.have_function_stats && !force);
    if (stat_no_change) {
        return;
    }

    /*
     * Don't send a message unless
     * 1. it's been at least PGSTAT_STAT_INTERVAL msec since we last sent one
     * 2. the caller wants to force stats out
     * 3. it is on coordinator since it will wait for command too long in pooler mode
     */
    now = GetCurrentTimestamp();
    if (!force && !TimestampDifferenceExceeds(u_sess->stat_cxt.last_report, now, PGSTAT_STAT_INTERVAL))
        return;

    u_sess->stat_cxt.last_report = now;

    /*
     * Scan through the TabStatusArray struct(s) to find tables that actually
     * have counts, and build messages to send.  We have to separate shared
     * relations from regular ones because the databaseid field in the message
     * header has to depend on that.
     */
    regular_msg.m_databaseid = u_sess->proc_cxt.MyDatabaseId;
    shared_msg.m_databaseid = InvalidOid;
    regular_msg.m_nentries = 0;
    shared_msg.m_nentries = 0;

    DEBUG_MOD_START_TIMER(MOD_PGSTAT);

    for (tsa = u_sess->stat_cxt.pgStatTabList; tsa != NULL; tsa = tsa->tsa_next) {
        /* count the length of u_sess->stat_cxt.pgStatTabList */
        tablist_len++;
        for (i = 0; i < tsa->tsa_used; i++) {
            PgStat_TableStatus* entry = &tsa->tsa_entries[i];
            PgStat_MsgTabstat* this_msg = NULL;
            PgStat_TableEntry* this_ent = NULL;

            /* Shouldn't have any pending transaction-dependent counts */
            Assert(entry->trans == NULL);

            /*
             * Ignore entries that didn't accumulate any actual counts, such
             * as indexes that were opened by the planner but not used.
             */
            if (memcmp(&entry->t_counts, &all_zeroes, sizeof(PgStat_TableCounts)) == 0)
                continue;

            /*
             * OK, insert data into the appropriate message, and send if full.
             */
            this_msg = entry->t_shared ? &shared_msg : &regular_msg;
            this_ent = &this_msg->m_entry[this_msg->m_nentries];
            this_ent->t_id = entry->t_id;
            this_ent->t_statFlag = entry->t_statFlag;
            rc =
                memcpy_s(&this_ent->t_counts, sizeof(PgStat_TableCounts), &entry->t_counts, sizeof(PgStat_TableCounts));
            securec_check(rc, "", "");
            if ((unsigned int)++this_msg->m_nentries >= PGSTAT_NUM_TABENTRIES) {
                pgstat_send_tabstat(this_msg);
                this_msg->m_nentries = 0;
            }
        }
        /* zero out TableStatus structs after use */
        rc = memset_s(tsa->tsa_entries,
            TABSTAT_QUANTUM * sizeof(PgStat_TableStatus),
            0,
            tsa->tsa_used * sizeof(PgStat_TableStatus));
        securec_check(rc, "\0", "\0");

        tsa->tsa_used = 0;
    }

    /* reset last_stat_counter as current local pgStatTabList is reset */
    bool need_reset_counter =
        (is_unique_sql_enabled() && IS_PGXC_DATANODE && u_sess->unique_sql_cxt.last_stat_counter != NULL);
    if (need_reset_counter) {
        rc = memset_s(
            u_sess->unique_sql_cxt.last_stat_counter, sizeof(PgStat_TableCounts), 0, sizeof(PgStat_TableCounts));
        securec_check(rc, "\0", "\0");
    }

    /* force to destory u_sess->stat_cxt.pgStatTabList if it is too large since it consumes too much memory */
    if (pgstat_tablist_too_big(tablist_len)) {
        force_to_destory = true;
        force = true;
    }

    /* pgStatTabHash is outdated on this point so we have to clean it. */
    hash_destroy(u_sess->stat_cxt.pgStatTabHash);
    u_sess->stat_cxt.pgStatTabHash = NULL;

    /*
     * Send partial messages.  If force is true, make sure that any pending
     * xact commit/abort gets counted, even if no table stats to send.
     */
    if (regular_msg.m_nentries > 0 ||
        (force && (u_sess->stat_cxt.pgStatXactCommit > 0 || u_sess->stat_cxt.pgStatXactRollback > 0)))
        pgstat_send_tabstat(&regular_msg);
    if (shared_msg.m_nentries > 0)
        pgstat_send_tabstat(&shared_msg);

    /* Now, send function statistics */
    pgstat_send_funcstats();

    /* send badblock statistics */
    pgstat_send_badblock_stat();

    DEBUG_MOD_STOP_TIMER(MOD_PGSTAT, "send collected usage statistics to collector");

    /* check the list' length and destroy it if needed */
    if (force_to_destory) {
        pgstat_free_tablist();
    }
}

/*
 * Subroutine for pgstat_report_stat: finish and send a tabstat message
 */
static void pgstat_send_tabstat(PgStat_MsgTabstat* tsmsg)
{
    int n;
    int len;

    /* It's unlikely we'd get here with no socket, but maybe not impossible */
    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET)
        return;

    /*
     * Report and reset accumulated xact commit/rollback and I/O timings
     * whenever we send a normal tabstat message
     */
    if (OidIsValid(tsmsg->m_databaseid)) {
        tsmsg->m_xact_commit = u_sess->stat_cxt.pgStatXactCommit;
        tsmsg->m_xact_rollback = u_sess->stat_cxt.pgStatXactRollback;
        tsmsg->m_block_read_time = u_sess->stat_cxt.pgStatBlockReadTime;
        tsmsg->m_block_write_time = u_sess->stat_cxt.pgStatBlockWriteTime;
        u_sess->stat_cxt.pgStatXactCommit = 0;
        u_sess->stat_cxt.pgStatXactRollback = 0;
        u_sess->stat_cxt.pgStatBlockReadTime = 0;
        u_sess->stat_cxt.pgStatBlockWriteTime = 0;
    } else {
        tsmsg->m_xact_commit = 0;
        tsmsg->m_xact_rollback = 0;
        tsmsg->m_block_read_time = 0;
        tsmsg->m_block_write_time = 0;
    }

    n = tsmsg->m_nentries;
    len = offsetof(PgStat_MsgTabstat, m_entry[0]) + n * sizeof(PgStat_TableEntry);

    pgstat_setheader(&tsmsg->m_hdr, PGSTAT_MTYPE_TABSTAT);
    pgstat_send(tsmsg, len);
}

/*
 * Subroutine for pgstat_report_stat: populate and send a function stat message
 */
static void pgstat_send_funcstats(void)
{
    /* we assume this inits to all zeroes: */
    static const PgStat_FunctionCounts all_zeroes = {0};

    PgStat_MsgFuncstat msg;
    PgStat_BackendFunctionEntry* entry = NULL;
    HASH_SEQ_STATUS fstat;

    if (u_sess->stat_cxt.pgStatFunctions == NULL)
        return;

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_FUNCSTAT);
    msg.m_databaseid = u_sess->proc_cxt.MyDatabaseId;
    msg.m_nentries = 0;

    hash_seq_init(&fstat, u_sess->stat_cxt.pgStatFunctions);
    while ((entry = (PgStat_BackendFunctionEntry*)hash_seq_search(&fstat)) != NULL) {
        PgStat_FunctionEntry* m_ent = NULL;
        errno_t rc;

        /* Skip it if no counts accumulated since last time */
        if (memcmp(&entry->f_counts, &all_zeroes, sizeof(PgStat_FunctionCounts)) == 0)
            continue;

        /* need to convert format of time accumulators */
        m_ent = &msg.m_entry[msg.m_nentries];
        m_ent->f_id = entry->f_id;
        m_ent->f_numcalls = entry->f_counts.f_numcalls;
        m_ent->f_total_time = INSTR_TIME_GET_MICROSEC(entry->f_counts.f_total_time);
        m_ent->f_self_time = INSTR_TIME_GET_MICROSEC(entry->f_counts.f_self_time);

        if ((unsigned int)++msg.m_nentries >= PGSTAT_NUM_FUNCENTRIES) {
            pgstat_send(&msg, offsetof(PgStat_MsgFuncstat, m_entry[0]) + msg.m_nentries * sizeof(PgStat_FunctionEntry));
            msg.m_nentries = 0;
        }

        /* reset the entry's counts */
        rc = memset_s(&entry->f_counts, sizeof(PgStat_FunctionCounts), 0, sizeof(PgStat_FunctionCounts));
        securec_check(rc, "\0", "\0");
    }

    if (msg.m_nentries > 0)
        pgstat_send(&msg, offsetof(PgStat_MsgFuncstat, m_entry[0]) + msg.m_nentries * sizeof(PgStat_FunctionEntry));

    u_sess->stat_cxt.have_function_stats = false;
}

/* ----------
 * pgstat_vacuum_stat() -
 *
 *	Will tell the collector about objects he can get rid of.
 * ----------
 */
void pgstat_vacuum_stat(void)
{
    HTAB* htab = NULL;
    PgStat_MsgTabpurge msg;
    PgStat_MsgFuncpurge f_msg;
    HASH_SEQ_STATUS hstat;
    PgStat_StatDBEntry* dbentry = NULL;
    PgStat_StatTabEntry* tabentry = NULL;
    PgStat_StatFuncEntry* funcentry = NULL;
    int len;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET)
        return;

    /*
     * If not done for this transaction, read the statistics collector stats
     * file into some hash tables.
     */
    backend_read_statsfile();

    /*
     * Read pg_database and make a list of OIDs of all existing databases
     */
    htab = pgstat_collect_oids(DatabaseRelationId);

    /*
     * Search the database hash table for dead databases and tell the
     * collector to drop them.
     */
    hash_seq_init(&hstat, u_sess->stat_cxt.pgStatDBHash);
    while ((dbentry = (PgStat_StatDBEntry*)hash_seq_search(&hstat)) != NULL) {
        Oid dbid = dbentry->databaseid;

        CHECK_FOR_INTERRUPTS();

        /* the DB entry for shared tables (with InvalidOid) is never dropped */
        if (OidIsValid(dbid) && hash_search(htab, (void*)&dbid, HASH_FIND, NULL) == NULL)
            pgstat_drop_database(dbid);
    }

    /* Clean up */
    hash_destroy(htab);

    /*
     * Lookup our own database entry; if not found, nothing more to do.
     */
    dbentry = (PgStat_StatDBEntry*)hash_search(
        u_sess->stat_cxt.pgStatDBHash, (void*)&u_sess->proc_cxt.MyDatabaseId, HASH_FIND, NULL);
    if (dbentry == NULL || dbentry->tables == NULL)
        return;

    /*
     * Similarly to above, make a list of all known relations in this DB.
     */
    htab = pgstat_collect_tabkeys();

    /*
     * Initialize our messages table counter to zero
     */
    msg.m_nentries = 0;

    /*
     * Check for all tables listed in stats hashtable if they still exist.
     */
    hash_seq_init(&hstat, dbentry->tables);
    while ((tabentry = (PgStat_StatTabEntry*)hash_seq_search(&hstat)) != NULL) {
        PgStat_StatTabKey tabkey = tabentry->tablekey;

        CHECK_FOR_INTERRUPTS();

        if (hash_search(htab, (void*)(&tabkey), HASH_FIND, NULL) != NULL)
            continue;

        /*
         * Not there, so add this table's Oid to the message
         */
        msg.m_entry[msg.m_nentries].m_tableid = tabkey.tableid;
        msg.m_entry[msg.m_nentries].m_statFlag = tabkey.statFlag;
        msg.m_nentries++;

        /*
         * If the message is full, send it out and reinitialize to empty
         */
        if ((unsigned int)msg.m_nentries >= PGSTAT_NUM_TABPURGE) {
            len = offsetof(PgStat_MsgTabpurge, m_entry[0]) + msg.m_nentries * sizeof(PgStat_MsgTabEntry);

            pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_TABPURGE);
            msg.m_databaseid = u_sess->proc_cxt.MyDatabaseId;
            pgstat_send(&msg, len);

            msg.m_nentries = 0;
        }
    }

    /*
     * Send the rest
     */
    if (msg.m_nentries > 0) {
        len = offsetof(PgStat_MsgTabpurge, m_entry[0]) + msg.m_nentries * sizeof(PgStat_MsgTabEntry);

        pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_TABPURGE);
        msg.m_databaseid = u_sess->proc_cxt.MyDatabaseId;
        pgstat_send(&msg, len);
    }

    /* Clean up */
    hash_destroy(htab);

    /*
     * Now repeat the above steps for functions.  However, we needn't bother
     * in the common case where no function stats are being collected.
     */
    if (dbentry->functions != NULL && hash_get_num_entries(dbentry->functions) > 0) {
        htab = pgstat_collect_oids(ProcedureRelationId);

        pgstat_setheader(&f_msg.m_hdr, PGSTAT_MTYPE_FUNCPURGE);
        f_msg.m_databaseid = u_sess->proc_cxt.MyDatabaseId;
        f_msg.m_nentries = 0;

        hash_seq_init(&hstat, dbentry->functions);
        while ((funcentry = (PgStat_StatFuncEntry*)hash_seq_search(&hstat)) != NULL) {
            Oid funcid = funcentry->functionid;

            CHECK_FOR_INTERRUPTS();

            if (hash_search(htab, (void*)&funcid, HASH_FIND, NULL) != NULL)
                continue;

            /*
             * Not there, so add this function's Oid to the message
             */
            f_msg.m_functionid[f_msg.m_nentries++] = funcid;

            /*
             * If the message is full, send it out and reinitialize to empty
             */
            if ((unsigned int)f_msg.m_nentries >= PGSTAT_NUM_FUNCPURGE) {
                len = offsetof(PgStat_MsgFuncpurge, m_functionid[0]) + f_msg.m_nentries * sizeof(Oid);

                pgstat_send(&f_msg, len);

                f_msg.m_nentries = 0;
            }
        }

        /*
         * Send the rest
         */
        if (f_msg.m_nentries > 0) {
            len = offsetof(PgStat_MsgFuncpurge, m_functionid[0]) + f_msg.m_nentries * sizeof(Oid);

            pgstat_send(&f_msg, len);
        }

        hash_destroy(htab);
    }
}

/* ----------
 * pgstat_collect_oids() -
 *
 *	Collect the OIDs of all objects listed in the specified system catalog
 *	into a temporary hash table.  Caller should hash_destroy the result
 *	when done with it.	(However, we make the table in CurrentMemoryContext
 *	so that it will be freed properly in event of an error.)
 * ----------
 */
static HTAB* pgstat_collect_oids(Oid catalogid)
{
    HTAB* htab = NULL;
    HASHCTL hash_ctl;
    Relation rel;
    TableScanDesc scan;
    HeapTuple tup;
    errno_t rc = EOK;

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(Oid);
    hash_ctl.hash = oid_hash;
    hash_ctl.hcxt = CurrentMemoryContext;
    htab = hash_create(
        "Temporary table of OIDs", PGSTAT_TAB_HASH_SIZE, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    rel = heap_open(catalogid, AccessShareLock);
    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        Oid thisoid = HeapTupleGetOid(tup);

        CHECK_FOR_INTERRUPTS();

        (void)hash_search(htab, (void*)&thisoid, HASH_ENTER, NULL);
    }
    tableam_scan_end(scan);
    heap_close(rel, AccessShareLock);

    return htab;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: Collect the OIDs of all objects listed in the specified system catalog into
 *			: a temporary hash table.  Caller should hash_destroy the result when
 *			: done with it. (However, we make the table in CurrentMemoryContext
 *			: so that it will be freed properly in event of an error.)
 */
static HTAB* pgstat_collect_tabkeys(void)
{
    HTAB* htab = NULL;
    HASHCTL hash_ctl;
    Relation pgRelation;
    Relation pgPartition;
    TableScanDesc scan;
    HeapTuple tuple;
    Form_pg_partition partForm;
    PgStat_StatTabKey tabkey;

    errno_t rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");
    hash_ctl.keysize = sizeof(PgStat_StatTabKey);
    hash_ctl.entrysize = sizeof(PgStat_StatTabKey);
    hash_ctl.hash = tag_hash;
    hash_ctl.hcxt = CurrentMemoryContext;
    htab = hash_create(
        "Temporary table of OIDs", PGSTAT_TAB_HASH_SIZE, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    /* deal with relation */
    tabkey.statFlag = InvalidOid;
    pgRelation = heap_open(RelationRelationId, AccessShareLock);
    scan = tableam_scan_begin(pgRelation, SnapshotNow, 0, NULL);
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        tabkey.tableid = HeapTupleGetOid(tuple);
        CHECK_FOR_INTERRUPTS();

        (void)hash_search(htab, (void*)(&tabkey), HASH_ENTER, NULL);
    }
    tableam_scan_end(scan);
    heap_close(pgRelation, AccessShareLock);

    /* deal with partition */
    pgPartition = heap_open(PartitionRelationId, AccessShareLock);
    scan = tableam_scan_begin(pgPartition, SnapshotNow, 0, NULL);
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        partForm = (Form_pg_partition)GETSTRUCT(tuple);

        /* skip partitioned object in pg_partition */
        if (partForm->parttype == PART_OBJ_TYPE_PARTED_TABLE)
            continue;

        tabkey.tableid = HeapTupleGetOid(tuple);
        tabkey.statFlag = partForm->parentid;

        CHECK_FOR_INTERRUPTS();

        (void)hash_search(htab, (void*)(&tabkey), HASH_ENTER, NULL);
    }
    tableam_scan_end(scan);
    heap_close(pgPartition, AccessShareLock);

    return htab;
}

/* ----------
 * pgstat_drop_database() -
 *
 *	Tell the collector that we just dropped a database.
 *	(If the message gets lost, we will still clean the dead DB eventually
 *	via future invocations of pgstat_vacuum_stat().)
 * ----------
 */
void pgstat_drop_database(Oid databaseid)
{
    PgStat_MsgDropdb msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET)
        return;

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_DROPDB);
    msg.m_databaseid = databaseid;
    pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_drop_relation() -
 *
 *	Tell the collector that we just dropped a relation.
 *	(If the message gets lost, we will still clean the dead entry eventually
 *	via future invocations of pgstat_vacuum_stat().)
 *
 *	Currently not used for lack of any good place to call it; we rely
 *	entirely on pgstat_vacuum_stat() to clean out stats for dead rels.
 * ----------
 */
#ifdef NOT_USED
void pgstat_drop_relation(Oid relid)
{
    PgStat_MsgTabpurge msg;
    int len;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET)
        return;
    msg.m_entry[0].m_tableid = relid;
    msg.m_entry[0].m_statFlag = STATFLG_RELATION;
    msg.m_nentries = 1;

    len = offsetof(PgStat_MsgTabpurge, m_entry[0]) + sizeof(PgStat_MsgTabEntry);

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_TABPURGE);
    msg.m_databaseid = u_sess->proc_cxt.MyDatabaseId;
    pgstat_send(&msg, len);
}
#endif /* NOT_USED */

/* ----------
 * pgstat_reset_counters() -
 *
 *	Tell the statistics collector to reset counters for our database.
 * ----------
 */
void pgstat_reset_counters(void)
{
    PgStat_MsgResetcounter msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET)
        return;

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to reset statistics counters")));

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RESETCOUNTER);
    msg.m_databaseid = u_sess->proc_cxt.MyDatabaseId;
    pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_reset_shared_counters() -
 *
 *	Tell the statistics collector to reset cluster-wide shared counters.
 * ----------
 */
void pgstat_reset_shared_counters(const char* target)
{
    PgStat_MsgResetsharedcounter msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET)
        return;

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to reset statistics counters")));

    if (strcmp(target, "bgwriter") == 0)
        msg.m_resettarget = RESET_BGWRITER;
    else
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("unrecognized reset target: \"%s\"", target),
                errhint("Target must be \"bgwriter\".")));

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RESETSHAREDCOUNTER);
    pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_reset_single_counter() -
 *
 *	Tell the statistics collector to reset a single counter.
 * ----------
 */
void pgstat_reset_single_counter(Oid p_objoid, Oid objoid, PgStat_Single_Reset_Type type)
{
    PgStat_MsgResetsinglecounter msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET)
        return;

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be system admin to reset statistics counters")));

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RESETSINGLECOUNTER);
    msg.m_databaseid = u_sess->proc_cxt.MyDatabaseId;
    msg.m_resettype = type;
    msg.m_objectid = objoid;
    msg.p_objectid = p_objoid;

    pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_report_autovac() -
 *
 *	Called from autovacuum.c to report startup of an autovacuum process.
 *	We are called before InitPostgres is done, so can't rely on u_sess->proc_cxt.MyDatabaseId;
 *	the db OID must be passed in, instead.
 * ----------
 */
void pgstat_report_autovac(Oid dboid)
{
    PgStat_MsgAutovacStart msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET)
        return;

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_AUTOVAC_START);
    msg.m_databaseid = dboid;
    msg.m_start_time = GetCurrentTimestamp();

    pgstat_send(&msg, sizeof(msg));
}

/* ---------
 * pgstat_report_vacuum() -
 *
 *	Tell the collector about the table we just vacuumed.
 * ---------
 */
void pgstat_report_autovac_timeout(Oid tableoid, uint32 statFlag, bool shared)
{
    PgStat_MsgAutovacStat msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET || !u_sess->attr.attr_common.pgstat_track_counts)
        return;

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_AUTOVAC_STAT);
    msg.m_databaseid = shared ? InvalidOid : u_sess->proc_cxt.MyDatabaseId;
    msg.m_tableoid = tableoid;
    msg.m_statFlag = statFlag;
    msg.m_autovacStat = AV_TIMEOUT;

    pgstat_send(&msg, sizeof(msg));
}

/* ---------
 * pgstat_report_vacuum() -
 *
 *	Tell the collector about the table we just vacuumed.
 * ---------
 */
void pgstat_report_vacuum(Oid tableoid, uint32 statFlag, bool shared, PgStat_Counter tuples)
{
    PgStat_MsgVacuum msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET || !u_sess->attr.attr_common.pgstat_track_counts)
        return;

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_VACUUM);
    msg.m_databaseid = shared ? InvalidOid : u_sess->proc_cxt.MyDatabaseId;
    msg.m_tableoid = tableoid;
    msg.m_statFlag = statFlag;
    msg.m_autovacuum = IsAutoVacuumWorkerProcess() || IsFromAutoVacWoker();
    msg.m_vacuumtime = GetCurrentTimestamp();
    msg.m_tuples = tuples;
    pgstat_send(&msg, sizeof(msg));
}

/* ---------
 * PgstatReportPrunestat() -
 *
 *	Tell the collector how many blocks we scanned and successfully pruned
 * ---------
 */
void PgstatReportPrunestat(Oid tableoid, uint32 statFlag,
                            bool shared, PgStat_Counter scanned,
                            PgStat_Counter pruned)
{
    PgStat_MsgPrune msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET)
        return;

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_PRUNESTAT);
    msg.m_databaseid = shared ? InvalidOid : u_sess->proc_cxt.MyDatabaseId;
    msg.m_tableoid = tableoid;
    msg.m_statFlag = statFlag;
    msg.m_scanned_blocks = scanned;
    msg.m_pruned_blocks = pruned;
    pgstat_send(&msg, sizeof(msg));
}

/* ---------
 * pgstat_report_data_changed() -
 *
 * Tell the collector about the table we just insert/delete/update/
 * copy/[exchange/truncate/drop] partition.
 * ---------
 */
void pgstat_report_data_changed(Oid tableoid, uint32 statFlag, bool shared)
{
    PgStat_MsgDataChanged msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET || !u_sess->attr.attr_common.pgstat_track_counts)
        return;

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_DATA_CHANGED);
    msg.m_databaseid = shared ? InvalidOid : u_sess->proc_cxt.MyDatabaseId;
    msg.m_tableoid = tableoid;
    msg.m_statFlag = statFlag;
    msg.m_changed_time = GetCurrentTimestamp();
    pgstat_send(&msg, sizeof(msg));
}

/* ---------
 * pgstat_report_sql_rt() -
 *
 *	Tell the collector about the sql responsetime.
 * ---------
 */
void pgstat_report_sql_rt(uint64 UniqueSQLId, int64 start_time, int64 rt)
{
    PgStat_SqlRT msg;

    if (IS_SINGLE_NODE) {
        return; /* disable in single node tmp for performance issue */
    }
    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET || !u_sess->attr.attr_common.enable_instr_rt_percentile)
        return;
    if (!PMstateIsRun()) {
        return;
    }
    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RESPONSETIME);
    msg.sqlRT.UniqueSQLId = UniqueSQLId;
    msg.sqlRT.start_time = start_time;
    msg.sqlRT.rt = rt;
    pgstat_send(&msg, sizeof(msg));
}

void pgstat_report_process_percentile()
{
    PgStat_PrsPtl msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET)
        return;
    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_PROCESSPERCENTILE);
    msg.now = GetCurrentTimestamp();
    pgstat_send(&msg, sizeof(msg));
}

/* ---------
 * pgstat_report_truncate() -
 *
 *	Tell the collector about the table we just truncated.
 * ---------
 */
void pgstat_report_truncate(Oid tableoid, uint32 statFlag, bool shared)
{
    PgStat_MsgTruncate msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET || !u_sess->attr.attr_common.pgstat_track_counts)
        return;

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_TRUNCATE);
    msg.m_databaseid = shared ? InvalidOid : u_sess->proc_cxt.MyDatabaseId;
    msg.m_tableoid = tableoid;
    msg.m_statFlag = statFlag;
    pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_analyze() -
 *
 *	Tell the collector about the table we just analyzed.
 * --------
 */
void pgstat_report_analyze(Relation rel, PgStat_Counter livetuples, PgStat_Counter deadtuples)
{
    PgStat_MsgAnalyze msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET || !u_sess->attr.attr_common.pgstat_track_counts)
        return;

    /*
     * Unlike VACUUM, ANALYZE might be running inside a transaction that has
     * already inserted and/or deleted rows in the target table. ANALYZE will
     * have counted such rows as live or dead respectively. Because we will
     * report our counts of such rows at transaction end, we should subtract
     * off these counts from what we send to the collector now, else they'll
     * be double-counted after commit.	(This approach also ensures that the
     * collector ends up with the right numbers if we abort instead of
     * committing.)
     */
    if (rel->pgstat_info != NULL) {
        PgStat_TableXactStatus* trans = NULL;

        for (trans = rel->pgstat_info->trans; trans; trans = trans->upper) {
            livetuples -= trans->tuples_inserted - trans->tuples_deleted;
            deadtuples -= (trans->tuples_updated - trans->tuples_inplace_updated) + trans->tuples_deleted;
        }
        /* count stuff inserted by already-aborted subxacts, too */
        deadtuples -= rel->pgstat_info->t_counts.t_delta_dead_tuples;
        /* Since ANALYZE's counts are estimates, we could have underflowed */
        livetuples = Max(livetuples, 0);
        deadtuples = Max(deadtuples, 0);
    }

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_ANALYZE);
    msg.m_databaseid = rel->rd_rel->relisshared ? InvalidOid : u_sess->proc_cxt.MyDatabaseId;
    msg.m_tableoid = RelationGetRelid(rel);
    msg.m_statFlag = rel->parentId;
    msg.m_autovacuum = IsAutoVacuumWorkerProcess() || IsFromAutoVacWoker();
    msg.m_analyzetime = GetCurrentTimestamp();
    msg.m_live_tuples = livetuples;
    msg.m_dead_tuples = deadtuples;
    pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_recovery_conflict() -
 *
 *	Tell the collector about a Hot Standby recovery conflict.
 * --------
 */
void pgstat_report_recovery_conflict(int reason)
{
    PgStat_MsgRecoveryConflict msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET || !u_sess->attr.attr_common.pgstat_track_counts)
        return;

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_RECOVERYCONFLICT);
    msg.m_databaseid = u_sess->proc_cxt.MyDatabaseId;
    msg.m_reason = reason;
    pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_deadlock() -
 *
 *	Tell the collector about a deadlock detected.
 * --------
 */
void pgstat_report_deadlock(void)
{
    PgStat_MsgDeadlock msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET || !u_sess->attr.attr_common.pgstat_track_counts)
        return;

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_DEADLOCK);
    msg.m_databaseid = u_sess->proc_cxt.MyDatabaseId;
    pgstat_send(&msg, sizeof(msg));
}

/* --------
 * pgstat_report_tempfile() -
 *
 *	Tell the collector about a temporary file.
 * --------
 */
void pgstat_report_tempfile(size_t filesize)
{
    PgStat_MsgTempFile msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET || !u_sess->attr.attr_common.pgstat_track_counts)
        return;

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_TEMPFILE);
    msg.m_databaseid = u_sess->proc_cxt.MyDatabaseId;
    msg.m_filesize = filesize;
    pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_report_memReserved() -
 *
 *      Tell the collector about memory reservation
 * ----------
 */
void pgstat_report_memReserved(int4 memReserved, int reserve_or_release)
{
    PgStat_MsgMemReserved msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET || !u_sess->attr.attr_common.pgstat_track_counts)
        return;

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_MEMRESERVED);
    msg.m_databaseid = u_sess->proc_cxt.MyDatabaseId;
    msg.m_memMbytes = memReserved;
    msg.m_reserve_or_release = reserve_or_release;
    pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_ping() -
 *
 *	Send some junk data to the collector to increase traffic.
 * ----------
 */
void pgstat_ping(void)
{
    PgStat_MsgDummy msg;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET)
        return;

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_DUMMY);
    pgstat_send(&msg, sizeof(msg));
}

/* ----------
 * pgstat_send_inquiry() -
 *
 *	Notify collector that we need fresh data.
 *	ts specifies the minimum acceptable timestamp for the stats file.
 * ----------
 */
static void pgstat_send_inquiry(TimestampTz ts)
{
    PgStat_MsgInquiry msg;

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_INQUIRY);
    msg.inquiry_time = ts;
    pgstat_send(&msg, sizeof(msg));
}

/*
 * Initialize function call usage data.
 * Called by the executor before invoking a function.
 */
void pgstat_init_function_usage(FunctionCallInfoData* fcinfo, PgStat_FunctionCallUsage* fcu)
{
    PgStat_BackendFunctionEntry* htabent = NULL;
    bool found = false;
    errno_t rc;

    if (u_sess->attr.attr_common.pgstat_track_functions <= fcinfo->flinfo->fn_stats) {
        /* stats not wanted */
        fcu->fs = NULL;
        return;
    }

    if (!u_sess->stat_cxt.pgStatFunctions) {
        /* First time through - initialize function stat table */
        HASHCTL hash_ctl;

        rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
        securec_check(rc, "\0", "\0");
        hash_ctl.keysize = sizeof(Oid);
        hash_ctl.entrysize = sizeof(PgStat_BackendFunctionEntry);
        hash_ctl.hash = oid_hash;
        hash_ctl.hcxt = u_sess->stat_cxt.pgStatLocalContext;
        u_sess->stat_cxt.pgStatFunctions = hash_create(
            "Function stat entries", PGSTAT_FUNCTION_HASH_SIZE, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    }

    /* Get the stats entry for this function, create if necessary */
    htabent = (PgStat_BackendFunctionEntry*)hash_search(
        u_sess->stat_cxt.pgStatFunctions, &fcinfo->flinfo->fn_oid, HASH_ENTER, &found);
    if (!found) {
        rc = memset_s(&htabent->f_counts, sizeof(PgStat_FunctionCounts), 0, sizeof(PgStat_FunctionCounts));
        securec_check(rc, "\0", "\0");
    }

    fcu->fs = &htabent->f_counts;

    /* save stats for this function, later used to compensate for recursion */
    fcu->save_f_total_time = htabent->f_counts.f_total_time;

    /* save current backend-wide total time */
    fcu->save_total = u_sess->stat_cxt.total_func_time;

    /* get clock time as of function start */
    INSTR_TIME_SET_CURRENT(fcu->f_start);
}

/*
 * find_funcstat_entry - find any existing PgStat_BackendFunctionEntry entry
 *		for specified function
 *
 * If no entry, return NULL, don't create a new one
 */
PgStat_BackendFunctionEntry* find_funcstat_entry(Oid func_id)
{
    if (u_sess->stat_cxt.pgStatFunctions == NULL)
        return NULL;

    return (PgStat_BackendFunctionEntry*)hash_search(
        u_sess->stat_cxt.pgStatFunctions, (void*)&func_id, HASH_FIND, NULL);
}

/*
 * Calculate function call usage and update stat counters.
 * Called by the executor after invoking a function.
 *
 * In the case of a set-returning function that runs in value-per-call mode,
 * we will see multiple pgstat_init_function_usage/pgstat_end_function_usage
 * calls for what the user considers a single call of the function.  The
 * finalize flag should be TRUE on the last call.
 */
void pgstat_end_function_usage(PgStat_FunctionCallUsage* fcu, bool finalize)
{
    PgStat_FunctionCounts* fs = fcu->fs;
    instr_time f_total;
    instr_time f_others;
    instr_time f_self;

    /* stats not wanted? */
    if (fs == NULL)
        return;

    /* total elapsed time in this function call */
    INSTR_TIME_SET_CURRENT(f_total);
    INSTR_TIME_SUBTRACT(f_total, fcu->f_start);

    /* self usage: elapsed minus anything already charged to other calls */
    f_others = u_sess->stat_cxt.total_func_time;
    ;
    INSTR_TIME_SUBTRACT(f_others, fcu->save_total);
    f_self = f_total;
    INSTR_TIME_SUBTRACT(f_self, f_others);

    /* update backend-wide total time */
    INSTR_TIME_ADD(u_sess->stat_cxt.total_func_time, f_self);

    /*
     * Compute the new f_total_time as the total elapsed time added to the
     * pre-call value of f_total_time.	This is necessary to avoid
     * double-counting any time taken by recursive calls of myself.  (We do
     * not need any similar kluge for self time, since that already excludes
     * any recursive calls.)
     */
    INSTR_TIME_ADD(f_total, fcu->save_f_total_time);

    /* update counters in function stats table */
    if (finalize)
        fs->f_numcalls++;
    fs->f_total_time = f_total;
    INSTR_TIME_ADD(fs->f_self_time, f_self);

    /* indicate that we have something to send */
    u_sess->stat_cxt.have_function_stats = true;
}

/* ----------
 * pgstat_initstats() -
 *
 *	Initialize a relcache entry to count access statistics.
 *	Called whenever a relation is opened.
 *
 *	We assume that a relcache entry's pgstat_info field is zeroed by
 *	relcache.c when the relcache entry is made; thereafter it is long-lived
 *	data.  We can avoid repeated searches of the TabStatus arrays when the
 *	same relation is touched repeatedly within a transaction.
 * ----------
 */
void pgstat_initstats(Relation rel)
{
    Oid rel_id = rel->rd_id;
    char relkind = rel->rd_rel->relkind;

    /* We only count stats for things that have storage */
    if (!(relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW || relkind == RELKIND_INDEX ||
            relkind == RELKIND_GLOBAL_INDEX || relkind == RELKIND_TOASTVALUE || RELKIND_IS_SEQUENCE(relkind))) {
        rel->pgstat_info = NULL;
        return;
    }

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET || !u_sess->attr.attr_common.pgstat_track_counts) {
        /* We're not counting at all */
        rel->pgstat_info = NULL;
        return;
    }

    /* Find or make the PgStat_TableStatus entry, and update link */
    rel->pgstat_info = get_tabstat_entry(rel_id, rel->rd_rel->relisshared, InvalidOid);
}

/*
 * Make sure pgStatTabList and pgStatTabHash are initialized.
 */
static void make_sure_stat_tab_initialized()
{
    HASHCTL ctl;
    errno_t rc;

    if (u_sess->stat_cxt.pgStatTabList == NULL) {
        /* This is first time procedure is called */
        u_sess->stat_cxt.pgStatTabList =
            (TabStatusArray*)MemoryContextAllocZero(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX), sizeof(TabStatusArray));
    }

    if (u_sess->stat_cxt.pgStatTabHash != NULL) {
        return;
    }

    if (u_sess->stat_cxt.pgStatTabHashContext == NULL) {
        u_sess->stat_cxt.pgStatTabHashContext =
            AllocSetContextCreate(u_sess->top_mem_cxt, "PGStatLookupHashTableContext", ALLOCSET_DEFAULT_SIZES);
    } else {
        MemoryContextReset(u_sess->stat_cxt.pgStatTabHashContext);
    }

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(PgStat_StatTabKey);
    ctl.entrysize = sizeof(TabStatHashEntry);
    ctl.hash = tag_hash;
    ctl.hcxt = u_sess->stat_cxt.pgStatTabHashContext;

    u_sess->stat_cxt.pgStatTabHash = hash_create("pgstat sessionid to tsa_entry lookup hash table",
        TABSTAT_QUANTUM,
        &ctl,
        HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

/*
 * get_tabstat_entry - find or create a PgStat_TableStatus entry for rel
 */
static PgStat_TableStatus* get_tabstat_entry(Oid rel_id, bool isshared, uint32 statFlag)
{
    TabStatHashEntry* hash_entry = NULL;
    PgStat_TableStatus* entry = NULL;
    TabStatusArray* tsa = NULL;
    bool found = false;

    make_sure_stat_tab_initialized();

    /* Find an entry or create a new one. */
    PgStat_StatTabKey rel_key;
    rel_key.tableid = rel_id;
    rel_key.statFlag = statFlag;
    hash_entry = (TabStatHashEntry*)hash_search(u_sess->stat_cxt.pgStatTabHash, &rel_key, HASH_ENTER, &found);
    if (found) {
        return hash_entry->tsa_entry;
    }

    /*
     * `hash_entry` was just created and now we have to fill it.
     * First make sure there is a free space in a last element of pgStatTabList.
     */
    tsa = u_sess->stat_cxt.pgStatTabList;
    while (tsa->tsa_used == TABSTAT_QUANTUM) {
        if (tsa->tsa_next == NULL) {
            tsa->tsa_next = (TabStatusArray*)MemoryContextAllocZero(
                SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX), sizeof(TabStatusArray));
        }
        tsa = tsa->tsa_next;
    }

    /*
     * Add an entry.
     */
    entry = &tsa->tsa_entries[tsa->tsa_used];
    entry->t_id = rel_id;
    entry->t_shared = isshared;
    entry->t_statFlag = statFlag;
    tsa->tsa_used++;

    /*
     * Add a corresponding entry to pgStatTabHash.
     */
    hash_entry->tsa_entry = entry;

    return entry;
}

/*
 * find_tabstat_entry - find any existing PgStat_TableStatus entry for rel
 *
 * If no entry, return NULL, don't create a new one
 */
PgStat_TableStatus* find_tabstat_entry(Oid rel_id, uint32 statFlag)
{
    TabStatHashEntry* hash_entry;

    /*
     * There are no entries at all.
     */
    if (!u_sess->stat_cxt.pgStatTabHash)
        return NULL;

    PgStat_StatTabKey rel_key;
    rel_key.tableid = rel_id;
    rel_key.statFlag = statFlag;
    hash_entry = (TabStatHashEntry*)hash_search(u_sess->stat_cxt.pgStatTabHash, &rel_key, HASH_FIND, NULL);
    if (!hash_entry)
        return NULL;

    return hash_entry->tsa_entry;
}

/*
 * get_tabstat_stack_level - add a new (sub)transaction stack entry if needed
 */
static PgStat_SubXactStatus* get_tabstat_stack_level(int nest_level)
{
    PgStat_SubXactStatus* xact_state = NULL;

    xact_state = u_sess->stat_cxt.pgStatXactStack;
    if (xact_state == NULL || xact_state->nest_level != nest_level) {
        xact_state =
            (PgStat_SubXactStatus*)MemoryContextAlloc(u_sess->top_transaction_mem_cxt, sizeof(PgStat_SubXactStatus));
        xact_state->nest_level = nest_level;
        xact_state->prev = u_sess->stat_cxt.pgStatXactStack;
        xact_state->first = NULL;
        u_sess->stat_cxt.pgStatXactStack = xact_state;
    }
    return xact_state;
}

/*
 * add_tabstat_xact_level - add a new (sub)transaction state record
 */
static void add_tabstat_xact_level(PgStat_TableStatus* pgstat_info, int nest_level)
{
    PgStat_SubXactStatus* xact_state = NULL;
    PgStat_TableXactStatus* trans = NULL;

    /*
     * If this is the first rel to be modified at the current nest level, we
     * first have to push a transaction stack entry.
     */
    xact_state = get_tabstat_stack_level(nest_level);

    /* Now make a per-table stack entry */
    trans = (PgStat_TableXactStatus*)MemoryContextAllocZero(
        u_sess->top_transaction_mem_cxt, sizeof(PgStat_TableXactStatus));
    trans->nest_level = nest_level;
    trans->upper = pgstat_info->trans;
    trans->parent = pgstat_info;
    trans->next = xact_state->first;
    xact_state->first = trans;
    pgstat_info->trans = trans;
}

/*
 * pgstat_count_heap_insert - count a tuple insertion of n tuples
 */
void pgstat_count_heap_insert(Relation rel, int n)
{
    PgStat_TableStatus* pgstat_info = rel->pgstat_info;

    if (pgstat_info != NULL) {
        /* We have to log the effect at the proper transactional level */
        int nest_level = GetCurrentTransactionNestLevel();

        if (pgstat_info->trans == NULL || pgstat_info->trans->nest_level != nest_level)
            add_tabstat_xact_level(pgstat_info, nest_level);

        pgstat_info->trans->tuples_inserted += n;
        pgstat_info->trans->tuples_inserted_accum += n;
    }
}

/*
 * pgstat_count_heap_update - count a tuple update
 */
void pgstat_count_heap_update(Relation rel, bool hot)
{
    PgStat_TableStatus* pgstat_info = rel->pgstat_info;

    if (pgstat_info != NULL) {
        /* We have to log the effect at the proper transactional level */
        int nest_level = GetCurrentTransactionNestLevel();

        if (pgstat_info->trans == NULL || pgstat_info->trans->nest_level != nest_level)
            add_tabstat_xact_level(pgstat_info, nest_level);

        pgstat_info->trans->tuples_updated++;
        pgstat_info->trans->tuples_updated_accum++;

        /* t_tuples_hot_updated is nontransactional, so just advance it */
        if (hot)
            pgstat_info->t_counts.t_tuples_hot_updated++;
    }
}

/*
 * pgstat_count_heap_delete - count a tuple deletion
 */
void pgstat_count_heap_delete(Relation rel)
{
    PgStat_TableStatus* pgstat_info = rel->pgstat_info;

    if (pgstat_info != NULL) {
        /* We have to log the effect at the proper transactional level */
        int nest_level = GetCurrentTransactionNestLevel();

        if (pgstat_info->trans == NULL || pgstat_info->trans->nest_level != nest_level)
            add_tabstat_xact_level(pgstat_info, nest_level);

        pgstat_info->trans->tuples_deleted++;
        pgstat_info->trans->tuples_deleted_accum++;
    }
}

/*
 * pgstat_truncate_save_counters
 *
 * Whenever a table is truncated, we save its i/u/d counters so that they can
 * be cleared, and if the (sub)xact that executed the truncate later aborts,
 * the counters can be restored to the saved (pre-truncate) values.  Note we do
 * this on the first truncate in any particular subxact level only.
 */
static void pgstat_truncate_save_counters(PgStat_TableXactStatus* trans)
{
    if (!trans->truncated) {
        trans->inserted_pre_trunc = trans->tuples_inserted;
        trans->updated_pre_trunc = trans->tuples_updated;
        trans->deleted_pre_trunc = trans->tuples_deleted;
        trans->inplace_updated_pre_trunc = trans->tuples_inplace_updated;
        trans->truncated = true;
    }
}

/*
 * pgstat_truncate_restore_counters - restore counters when a truncate aborts
 */
static void pgstat_truncate_restore_counters(PgStat_TableXactStatus* trans)
{
    if (trans->truncated) {
        trans->tuples_inserted = trans->inserted_pre_trunc;
        trans->tuples_updated = trans->updated_pre_trunc;
        trans->tuples_deleted = trans->deleted_pre_trunc;
        trans->tuples_inplace_updated = trans->inplace_updated_pre_trunc;
    }
}

/*
 * pgstat_count_truncate - update tuple counters due to truncate
 */
void pgstat_count_truncate(Relation rel)
{
    PgStat_TableStatus* pgstat_info = rel->pgstat_info;

    if (pgstat_info != NULL) {
        /* We have to log the effect at the proper transactional level */
        int nest_level = GetCurrentTransactionNestLevel();

        if (pgstat_info->trans == NULL || pgstat_info->trans->nest_level != nest_level)
            add_tabstat_xact_level(pgstat_info, nest_level);

        pgstat_truncate_save_counters(pgstat_info->trans);
        pgstat_info->trans->tuples_inserted = 0;
        pgstat_info->trans->tuples_updated = 0;
        pgstat_info->trans->tuples_deleted = 0;
        pgstat_info->trans->tuples_inplace_updated = 0;
    }
}

/*
 * pgstat_count_uheap_update - count a tuple update inplace
 */
void
PgstatCountHeapUpdateInplace(Relation rel)
{
    PgStat_TableStatus *pgstat_info = rel->pgstat_info;

    if (pgstat_info != NULL) {
        /* We have to log the effect at the proper transactional level */
        int nest_level = GetCurrentTransactionNestLevel();

        if (pgstat_info->trans == NULL || pgstat_info->trans->nest_level != nest_level) {
            add_tabstat_xact_level(pgstat_info, nest_level);
        }

        /* increase, similar to pgstat_count_heap_update */
        pgstat_info->trans->tuples_updated++;
        pgstat_info->trans->tuples_updated_accum++;

        pgstat_info->trans->tuples_inplace_updated++;
    }
}

/*
 * pgstat_update_heap_dead_tuples - update dead-tuples count
 *
 * The semantics of this are that we are reporting the nontransactional
 * recovery of "delta" dead tuples; so t_delta_dead_tuples decreases
 * rather than increasing, and the change goes straight into the per-table
 * counter, not into transactional state.
 */
void pgstat_update_heap_dead_tuples(Relation rel, int delta)
{
    PgStat_TableStatus* pgstat_info = rel->pgstat_info;

    if (pgstat_info != NULL)
        pgstat_info->t_counts.t_delta_dead_tuples -= delta;
}

/*
 * pgstat_count_cu/dfs_update - count a cstore/dfs update
 */
void pgstat_count_cu_update(Relation rel, int n)
{
    PgStat_TableStatus* pgstat_info = rel->pgstat_info;

    if (pgstat_info != NULL) {
        /* We have to log the effect at the proper transactional level */
        int nest_level = GetCurrentTransactionNestLevel();

        if (pgstat_info->trans == NULL || pgstat_info->trans->nest_level != nest_level)
            add_tabstat_xact_level(pgstat_info, nest_level);

        /* cstore and dfs don't have hot update chain */
        pgstat_info->trans->tuples_updated += n;
        pgstat_info->trans->tuples_updated_accum += n;
    }
}

/*
 * pgstat_count_cu/dfs_delete - count a cstore/dfs deletion
 */
void pgstat_count_cu_delete(Relation rel, int n)
{
    PgStat_TableStatus* pgstat_info = rel->pgstat_info;

    if (pgstat_info != NULL) {
        /* We have to log the effect at the proper transactional level */
        int nest_level = GetCurrentTransactionNestLevel();

        if (pgstat_info->trans == NULL || pgstat_info->trans->nest_level != nest_level)
            add_tabstat_xact_level(pgstat_info, nest_level);

        pgstat_info->trans->tuples_deleted += n;
        pgstat_info->trans->tuples_deleted_accum += n;
    }
}

static inline void init_tuplecount_truncate(PgStat_TableStatus* tabstat)
{
    /* forget live/dead stats seen by backend thus far */
    tabstat->t_counts.t_tuples_inserted_post_truncate = 0;
    tabstat->t_counts.t_tuples_updated_post_truncate = 0;
    tabstat->t_counts.t_tuples_deleted_post_truncate = 0;
    tabstat->t_counts.t_tuples_inplace_updated_post_truncate = 0;
    tabstat->t_counts.t_delta_live_tuples = 0;
    tabstat->t_counts.t_delta_dead_tuples = 0;
}

/* ----------
 * AtEOXact_PgStat
 *
 *	Called from access/transam/xact.c at top-level transaction commit/abort.
 * ----------
 */
void AtEOXact_PgStat(bool isCommit)
{
    PgStat_SubXactStatus* xact_state = NULL;

    /*
     * Count transaction commit or abort.  (We use counters, not just bools,
     * in case the reporting message isn't sent right away.)
     */
    if (isCommit) {
        u_sess->stat_cxt.pgStatXactCommit++;
    } else if (!AM_WAL_DB_SENDER) {
        /*
         * Walsender for logical decoding will not count rollback transaction
         * any more. Logical decoding frequently starts and rollback transactions
         * to access system tables and syscache, see ReorderBufferCommit()
         * for more details.
         */
        u_sess->stat_cxt.pgStatXactRollback++;
    }

    pgstatCountTransactionCommit4SessionLevel(isCommit);

    /*
     * Transfer transactional insert/update counts into the base tabstat
     * entries.  We don't bother to free any of the transactional state, since
     * it's all in u_sess->top_transaction_mem_cxt and will go away anyway.
     */
    xact_state = u_sess->stat_cxt.pgStatXactStack;
    if (xact_state != NULL) {
        PgStat_TableXactStatus* trans = NULL;

        Assert(xact_state->nest_level == 1);
        Assert(xact_state->prev == NULL);
        for (trans = xact_state->first; trans != NULL; trans = trans->next) {
            PgStat_TableStatus* tabstat = NULL;

            Assert(trans->nest_level == 1);
            Assert(trans->upper == NULL);
            tabstat = trans->parent;
            Assert(tabstat->trans == trans);
            /* restore pre-truncate stats (if any) in case of aborted xact */
            if (!isCommit)
                pgstat_truncate_restore_counters(trans);
            /* count attempted actions regardless of commit/abort */
            tabstat->t_counts.t_tuples_inserted += trans->tuples_inserted_accum;
            tabstat->t_counts.t_tuples_updated += trans->tuples_updated_accum;
            tabstat->t_counts.t_tuples_deleted += trans->tuples_deleted_accum;
            tabstat->t_counts.t_tuples_inplace_updated += trans->tuples_inplace_updated_accum;
            if (isCommit) {
                tabstat->t_counts.t_truncated = trans->truncated;
                if (trans->truncated) {
                    init_tuplecount_truncate(tabstat);
                }
                if (tabstat->t_counts.t_truncated) {
                    tabstat->t_counts.t_tuples_inserted_post_truncate += trans->tuples_inserted;
                    tabstat->t_counts.t_tuples_updated_post_truncate += trans->tuples_updated;
                    tabstat->t_counts.t_tuples_deleted_post_truncate += trans->tuples_deleted;
                    tabstat->t_counts.t_tuples_inplace_updated_post_truncate += trans->tuples_inplace_updated;
                }
                /* insert adds a live tuple, delete removes one */
                tabstat->t_counts.t_delta_live_tuples += trans->tuples_inserted - trans->tuples_deleted;
                /* update and delete each create a dead tuple */
                tabstat->t_counts.t_delta_dead_tuples +=
                    (trans->tuples_updated - trans->tuples_inplace_updated) + trans->tuples_deleted;
                /* insert, update, delete each count as one change event */
                tabstat->t_counts.t_changed_tuples +=
                    trans->tuples_inserted + trans->tuples_updated + trans->tuples_deleted;
            } else {
                /* inserted tuples are dead, deleted tuples are unaffected */
                tabstat->t_counts.t_delta_dead_tuples +=
                    trans->tuples_inserted + (trans->tuples_updated - trans->tuples_inplace_updated);
                /* an aborted xact generates no changed_tuple events */
            }
            tabstat->trans = NULL;
        }
    }
    u_sess->stat_cxt.pgStatXactStack = NULL;

    /* Make sure any stats snapshot is thrown away */
    pgstat_clear_snapshot();
}

/* ----------
 * AtEOSubXact_PgStat
 *
 *	Called from access/transam/xact.c at subtransaction commit/abort.
 * ----------
 */
void AtEOSubXact_PgStat(bool isCommit, int nestDepth)
{
    PgStat_SubXactStatus* xact_state = NULL;

    /*
     * Transfer transactional insert/update counts into the next higher
     * subtransaction state.
     */
    xact_state = u_sess->stat_cxt.pgStatXactStack;
    if (xact_state != NULL && xact_state->nest_level >= nestDepth) {
        PgStat_TableXactStatus* trans = NULL;
        PgStat_TableXactStatus* next_trans = NULL;

        /* delink xact_state from stack immediately to simplify reuse case */
        u_sess->stat_cxt.pgStatXactStack = xact_state->prev;

        for (trans = xact_state->first; trans != NULL; trans = next_trans) {
            PgStat_TableStatus* tabstat = NULL;

            next_trans = trans->next;
            Assert(trans->nest_level == nestDepth);
            tabstat = trans->parent;
            Assert(tabstat->trans == trans);
            if (isCommit) {
                if (trans->upper && trans->upper->nest_level == nestDepth - 1) {
                    if (trans->truncated) {
                        /* propagate the truncate status one level up */
                        pgstat_truncate_save_counters(trans->upper);
                        /* replace upper xact stats with ours */
                        trans->upper->tuples_inserted = trans->tuples_inserted;
                        trans->upper->tuples_updated = trans->tuples_updated;
                        trans->upper->tuples_deleted = trans->tuples_deleted;
                        trans->upper->tuples_inplace_updated = trans->tuples_inplace_updated;
                    } else {
                        trans->upper->tuples_inserted += trans->tuples_inserted;
                        trans->upper->tuples_updated += trans->tuples_updated;
                        trans->upper->tuples_deleted += trans->tuples_deleted;
                        trans->upper->tuples_inplace_updated += trans->tuples_inplace_updated;
                    }
                    /* accumulated stats shall not be replaced */
                    trans->upper->tuples_inserted_accum += trans->tuples_inserted_accum;
                    trans->upper->tuples_updated_accum += trans->tuples_updated_accum;
                    trans->upper->tuples_deleted_accum += trans->tuples_deleted_accum;
                    trans->upper->tuples_inplace_updated_accum += trans->tuples_inplace_updated_accum;
                    tabstat->trans = trans->upper;
                    pfree(trans);
                } else {
                    /*
                     * When there isn't an immediate parent state, we can just
                     * reuse the record instead of going through a
                     * palloc/pfree pushup (this works since it's all in
                     * u_sess->top_transaction_mem_cxt anyway).  We have to re-link it
                     * into the parent level, though, and that might mean
                     * pushing a new entry into the u_sess->stat_cxt.pgStatXactStack.
                     */
                    PgStat_SubXactStatus* upper_xact_state = NULL;

                    upper_xact_state = get_tabstat_stack_level(nestDepth - 1);
                    trans->next = upper_xact_state->first;
                    upper_xact_state->first = trans;
                    trans->nest_level = nestDepth - 1;
                }
            } else {
                /*
                 * On abort, update top-level tabstat counts, then forget the
                 * subtransaction
                 */
                /* first restore values obliterated by truncate */
                pgstat_truncate_restore_counters(trans);
                /* count attempted actions regardless of commit/abort */
                tabstat->t_counts.t_tuples_inserted += trans->tuples_inserted_accum;
                tabstat->t_counts.t_tuples_updated += trans->tuples_updated_accum;
                tabstat->t_counts.t_tuples_deleted += trans->tuples_deleted_accum;
                tabstat->t_counts.t_tuples_inplace_updated += trans->tuples_inplace_updated_accum;
                /* inserted tuples are dead, deleted tuples are unaffected */
                tabstat->t_counts.t_delta_dead_tuples += 
                    trans->tuples_inserted + (trans->tuples_updated - trans->tuples_inplace_updated);
                tabstat->trans = trans->upper;
                pfree(trans);
            }
        }
        pfree(xact_state);
    }
}

/*
 * AtPrepare_PgStat
 *		Save the transactional stats state at 2PC transaction prepare.
 *
 * In this phase we just generate 2PC records for all the pending
 * transaction-dependent stats work.
 */
void AtPrepare_PgStat(void)
{
    PgStat_SubXactStatus* xact_state = NULL;

    xact_state = u_sess->stat_cxt.pgStatXactStack;
    if (xact_state != NULL) {
        PgStat_TableXactStatus* trans = NULL;

        Assert(xact_state->nest_level == 1);
        Assert(xact_state->prev == NULL);
        for (trans = xact_state->first; trans != NULL; trans = trans->next) {
            PgStat_TableStatus* tabstat = NULL;
            TwoPhasePgStatRecord record;

            Assert(trans->nest_level == 1);
            Assert(trans->upper == NULL);
            tabstat = trans->parent;
            Assert(tabstat->trans == trans);

            record.tuples_inserted = trans->tuples_inserted;
            record.tuples_updated = trans->tuples_updated;
            record.tuples_deleted = trans->tuples_deleted;
            record.tuples_inplace_updated = trans->tuples_inplace_updated;
            record.tuples_inserted_accum = trans->tuples_inserted_accum;
            record.tuples_updated_accum = trans->tuples_updated_accum;
            record.tuples_deleted_accum = trans->tuples_deleted_accum;
            record.tuples_inplace_updated_accum = trans->tuples_inplace_updated_accum;
            record.inserted_pre_trunc = trans->inserted_pre_trunc;
            record.updated_pre_trunc = trans->updated_pre_trunc;
            record.deleted_pre_trunc = trans->deleted_pre_trunc;
            record.inplace_updated_pre_trunc = trans->inplace_updated_pre_trunc;
            record.t_id = tabstat->t_id;
            record.t_shared = tabstat->t_shared;
            record.t_statFlag = tabstat->t_statFlag;
            record.t_truncated = trans->truncated;

            RegisterTwoPhaseRecord(TWOPHASE_RM_PGSTAT_ID, 0, &record, sizeof(TwoPhasePgStatRecord));
        }
    }
}

/*
 * PostPrepare_PgStat
 *		Clean up after successful PREPARE.
 *
 * All we need do here is unlink the transaction stats state from the
 * nontransactional state.	The nontransactional action counts will be
 * reported to the stats collector immediately, while the effects on live
 * and dead tuple counts are preserved in the 2PC state file.
 *
 * Note: AtEOXact_PgStat is not called during PREPARE.
 */
void PostPrepare_PgStat(void)
{
    PgStat_SubXactStatus* xact_state = NULL;

    /*
     * We don't bother to free any of the transactional state, since it's all
     * in u_sess->top_transaction_mem_cxt and will go away anyway.
     */
    xact_state = u_sess->stat_cxt.pgStatXactStack;
    if (xact_state != NULL) {
        PgStat_TableXactStatus* trans = NULL;

        for (trans = xact_state->first; trans != NULL; trans = trans->next) {
            PgStat_TableStatus* tabstat = NULL;

            tabstat = trans->parent;
            tabstat->trans = NULL;
        }
    }
    u_sess->stat_cxt.pgStatXactStack = NULL;

    /* Make sure any stats snapshot is thrown away */
    pgstat_clear_snapshot();
}

/*
 * 2PC processing routine for COMMIT PREPARED case.
 *
 * Load the saved counts into our local pgstats state.
 */
void pgstat_twophase_postcommit(TransactionId xid, uint16 info, void* recdata, uint32 len)
{
    TwoPhasePgStatRecord* rec = (TwoPhasePgStatRecord*)recdata;
    PgStat_TableStatus* pgstat_info = NULL;

    /* Find or create a tabstat entry for the rel */
    pgstat_info = get_tabstat_entry(rec->t_id, rec->t_shared, rec->t_statFlag);

    /* Same math as in AtEOXact_PgStat, commit case */
    pgstat_info->t_counts.t_tuples_inserted += rec->tuples_inserted_accum;
    pgstat_info->t_counts.t_tuples_updated += rec->tuples_updated_accum;
    pgstat_info->t_counts.t_tuples_deleted += rec->tuples_deleted_accum;
    pgstat_info->t_counts.t_tuples_inplace_updated += rec->tuples_inplace_updated_accum;
    pgstat_info->t_counts.t_truncated = rec->t_truncated;
    if (rec->t_truncated) {
        init_tuplecount_truncate(pgstat_info);
    }
    if (pgstat_info->t_counts.t_truncated) {
        pgstat_info->t_counts.t_tuples_inserted_post_truncate += rec->tuples_inserted;
        pgstat_info->t_counts.t_tuples_updated_post_truncate += rec->tuples_updated;
        pgstat_info->t_counts.t_tuples_deleted_post_truncate += rec->tuples_deleted;
        pgstat_info->t_counts.t_tuples_inplace_updated_post_truncate += rec->tuples_inplace_updated;
    }
    pgstat_info->t_counts.t_delta_live_tuples += rec->tuples_inserted - rec->tuples_deleted;
    pgstat_info->t_counts.t_delta_dead_tuples += 
        (rec->tuples_updated - rec->tuples_inplace_updated) + rec->tuples_deleted;
    pgstat_info->t_counts.t_changed_tuples += rec->tuples_inserted + rec->tuples_updated + rec->tuples_deleted;
}

/*
 * 2PC processing routine for ROLLBACK PREPARED case.
 *
 * Load the saved counts into our local pgstats state, but treat them
 * as aborted.
 */
void pgstat_twophase_postabort(TransactionId xid, uint16 info, void* recdata, uint32 len)
{
    TwoPhasePgStatRecord* rec = (TwoPhasePgStatRecord*)recdata;
    PgStat_TableStatus* pgstat_info = NULL;

    /* Find or create a tabstat entry for the rel */
    pgstat_info = get_tabstat_entry(rec->t_id, rec->t_shared, rec->t_statFlag);

    /* Same math as in AtEOXact_PgStat, abort case */
    if (rec->t_truncated) {
        rec->tuples_inserted = rec->inserted_pre_trunc;
        rec->tuples_updated = rec->updated_pre_trunc;
        rec->tuples_deleted = rec->deleted_pre_trunc;
        rec->tuples_inplace_updated = rec->inplace_updated_pre_trunc;
    }
    pgstat_info->t_counts.t_tuples_inserted += rec->tuples_inserted_accum;
    pgstat_info->t_counts.t_tuples_updated += rec->tuples_updated_accum;
    pgstat_info->t_counts.t_tuples_deleted += rec->tuples_deleted_accum;
    pgstat_info->t_counts.t_tuples_inplace_updated += rec->tuples_inplace_updated_accum;
    pgstat_info->t_counts.t_delta_dead_tuples +=
        rec->tuples_inserted + (rec->tuples_updated - rec->tuples_inplace_updated);
}

/* ----------
 * pgstat_fetch_stat_dbentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the collected statistics for one database or NULL. NULL doesn't mean
 *	that the database doesn't exist, it is just not yet known by the
 *	collector, so the caller is better off to report ZERO instead.
 * ----------
 */
PgStat_StatDBEntry* pgstat_fetch_stat_dbentry(Oid dbid)
{
    /*
     * If not done for this transaction, read the statistics collector stats
     * file into some hash tables.
     */
    backend_read_statsfile();

    /*
     * Lookup the requested database; return NULL if not found
     */
    return (PgStat_StatDBEntry*)hash_search(u_sess->stat_cxt.pgStatDBHash, (void*)&dbid, HASH_FIND, NULL);
}

/* ----------
 * pgstat_fetch_stat_tabentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the collected statistics for one table or NULL. NULL doesn't mean
 *	that the table doesn't exist, it is just not yet known by the
 *	collector, so the caller is better off to report ZERO instead.
 * ----------
 */
PgStat_StatTabEntry* pgstat_fetch_stat_tabentry(PgStat_StatTabKey* tabkey)
{
    Oid dbid;
    PgStat_StatDBEntry* dbentry = NULL;
    PgStat_StatTabEntry* tabentry = NULL;

    /*
     * If not done for this transaction, read the statistics collector stats
     * file into some hash tables.
     */
    backend_read_statsfile();

    /*
     * Lookup our database, then look in its table hash table.
     */
    dbid = u_sess->proc_cxt.MyDatabaseId;
    dbentry = (PgStat_StatDBEntry*)hash_search(u_sess->stat_cxt.pgStatDBHash, (void*)&dbid, HASH_FIND, NULL);
    if (dbentry != NULL && dbentry->tables != NULL) {
        tabentry = (PgStat_StatTabEntry*)hash_search(dbentry->tables, (void*)tabkey, HASH_FIND, NULL);
        if (tabentry != NULL)
            return tabentry;
    }

    /*
     * If we didn't find it, maybe it's a shared table.
     */
    dbid = InvalidOid;
    dbentry = (PgStat_StatDBEntry*)hash_search(u_sess->stat_cxt.pgStatDBHash, (void*)&dbid, HASH_FIND, NULL);
    if (dbentry != NULL && dbentry->tables != NULL) {
        tabentry = (PgStat_StatTabEntry*)hash_search(dbentry->tables, (void*)tabkey, HASH_FIND, NULL);
        if (tabentry != NULL)
            return tabentry;
    }

    return NULL;
}

/* ----------
 * pgstat_fetch_stat_funcentry() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	the collected statistics for one function or NULL.
 * ----------
 */
PgStat_StatFuncEntry* pgstat_fetch_stat_funcentry(Oid func_id)
{
    PgStat_StatDBEntry* dbentry = NULL;
    PgStat_StatFuncEntry* funcentry = NULL;

    /* load the stats file if needed */
    backend_read_statsfile();

    /* Lookup our database, then find the requested function.  */
    dbentry = pgstat_fetch_stat_dbentry(u_sess->proc_cxt.MyDatabaseId);
    if (dbentry != NULL && dbentry->functions != NULL) {
        funcentry = (PgStat_StatFuncEntry*)hash_search(dbentry->functions, (void*)&func_id, HASH_FIND, NULL);
    }

    return funcentry;
}

/*
 * ---------
 * pgstat_fetch_global() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	a pointer to the global statistics struct.
 * ---------
 */
PgStat_GlobalStats* pgstat_fetch_global(void)
{
    backend_read_statsfile();

    return u_sess->stat_cxt.globalStats;
}

/* ------------------------------------------------------------
 * Functions for management of the shared-memory PgBackendStatus array
 * ------------------------------------------------------------
 */
static THR_LOCAL char* BackendNspRelnameBuffer = NULL;

PgBackendStatus* PgBackendStatusArray = NULL;


THR_LOCAL XLogStatCollect *g_xlog_stat_shared = NULL;

Size XLogStatShmemSize(void)
{
    return sizeof(XLogStatCollect);
}

void XLogStatShmemInit(void)
{
    bool found = false;
    errno_t rc;

    g_xlog_stat_shared = (XLogStatCollect *)ShmemInitStruct("XLogStat", XLogStatShmemSize(), &found);
    g_xlog_stat_shared->remoteFlushWaitCount = 0;

    if (!IsUnderPostmaster) {
        Assert(!found);
        rc = memset_s(g_xlog_stat_shared, XLogStatShmemSize(), 0, XLogStatShmemSize());
        securec_check(rc, "\0", "\0");
    } else {
        Assert(found);
    }
}

/*
 * ---------
 * pgstat_fetch_waitcount() -
 *
 *	Support function for the SQL-callable pgstat* functions. Returns
 *	a pointer to WaitCountBuffer.
 * ---------
 */
PgStat_WaitCountStatus* pgstat_fetch_waitcount(void)
{
    return t_thrd.shemem_ptr_cxt.WaitCountBuffer;
}

/*
 * Report shared-memory space needed by CreateSharedBackendStatus.
 */
Size BackendStatusShmemSize(void)
{
    Size size;

    /* PgBackendStatus array */
    size = mul_size(sizeof(PgBackendStatus), BackendStatusArray_size);
    /* application name array */
    size = add_size(size, mul_size(NAMEDATALEN, BackendStatusArray_size));
    /* conninfo array */
    size = add_size(size, mul_size(CONNECTIONINFO_LEN, BackendStatusArray_size));
    /* client hostname array */
    size = add_size(size, mul_size(NAMEDATALEN, BackendStatusArray_size));
    /* activity array */
    size =
        add_size(size, mul_size(g_instance.attr.attr_common.pgstat_track_activity_query_size, BackendStatusArray_size));
    /* WaitCountStatus */
    size = add_size(size, sizeof(PgStat_WaitCountStatus));
    /* relname array */
    size = add_size(size, mul_size(NAMEDATALEN * 2, BackendStatusArray_size));
    return size;
}
/**
 * init memory whose size may be large than INT_MAX by calling func memset_s many times
 */
static void MemsetHugeSize(char* dest, const Size size, const char c)
{
    Size block_size = 0x40000000;
    errno_t rc;
    Size remain_size = size;
    char* ptr = dest;
    while (remain_size > 0) {
        if (remain_size < block_size) {
            block_size = remain_size;
        }
        rc = memset_s(ptr, block_size, c, block_size);
        securec_check(rc, "\0", "\0");
        ptr += block_size;
        remain_size -= block_size;
    }
}

/*
 * Initialize the shared status array and several string buffers
 * during postmaster startup.
 */
void CreateSharedBackendStatus(void)
{
    Size size;
    bool found = false;
    int i;
    char* buffer = NULL;
    errno_t rc;

    /* Create or attach to the shared array */
    size = mul_size(sizeof(PgBackendStatus), BackendStatusArray_size);
    t_thrd.shemem_ptr_cxt.BackendStatusArray = (PgBackendStatus*)ShmemInitStruct("Backend Status Array", size, &found);

    if (!found) {
        /*
         * We're the first - initialize.
         * call MemsetHugeSize because the size may be larger than INT_MAX,
         * when memset_s can only init memory less than INT_MAX.
         */
        MemsetHugeSize((char*)t_thrd.shemem_ptr_cxt.BackendStatusArray, size, 0);

        /* init mutex for full/slow sql */
        TimestampTz current_time = GetCurrentTimestamp();
        for (i = 0; i < BackendStatusArray_size; i++) {
            (void)syscalllockInit(&t_thrd.shemem_ptr_cxt.BackendStatusArray[i].statement_cxt_lock);
            /* init last updated time for wait event */
            InstrWaitEventInitLastUpdated(&t_thrd.shemem_ptr_cxt.BackendStatusArray[i], current_time);
        }
    }

    /* Create or attach to the shared appname buffer */
    size = mul_size(NAMEDATALEN, BackendStatusArray_size);
    t_thrd.shemem_ptr_cxt.BackendAppnameBuffer =
        (char*)ShmemInitStruct("Backend Application Name Buffer", size, &found);

    if (!found) {
        rc = memset_s(t_thrd.shemem_ptr_cxt.BackendAppnameBuffer, size, 0, size);
        securec_check(rc, "\0", "\0");

        /* Initialize st_appname pointers. */
        buffer = t_thrd.shemem_ptr_cxt.BackendAppnameBuffer;
        for (i = 0; i < BackendStatusArray_size; i++) {
            t_thrd.shemem_ptr_cxt.BackendStatusArray[i].st_appname = buffer;
            buffer += NAMEDATALEN;
        }
    }

    /* Create or attach to the shared conninfo buffer */
    size = mul_size(CONNECTIONINFO_LEN, BackendStatusArray_size);
    t_thrd.shemem_ptr_cxt.BackendConninfoBuffer =
        (char*)ShmemInitStruct("Backend Connection Information Buffer", size, &found);

    if (!found) {
        /* call MemsetHugeSize because the size may be larger than INT_MAX,
         * when memset_s can only init memory less than INT_MAX.
         */
        MemsetHugeSize((char*)t_thrd.shemem_ptr_cxt.BackendConninfoBuffer, size, 0);

        /* Initialize st_appname pointers. */
        buffer = t_thrd.shemem_ptr_cxt.BackendConninfoBuffer;
        for (i = 0; i < BackendStatusArray_size; i++) {
            t_thrd.shemem_ptr_cxt.BackendStatusArray[i].st_conninfo = buffer;
            buffer += CONNECTIONINFO_LEN;
        }
    }

    /* Create or attach to the shared client hostname buffer */
    size = mul_size(NAMEDATALEN, BackendStatusArray_size);
    t_thrd.shemem_ptr_cxt.BackendClientHostnameBuffer =
        (char*)ShmemInitStruct("Backend Client Host Name Buffer", size, &found);

    if (!found) {
        rc = memset_s(t_thrd.shemem_ptr_cxt.BackendClientHostnameBuffer, size, 0, size);
        securec_check(rc, "\0", "\0");

        /* Initialize st_clienthostname pointers. */
        buffer = t_thrd.shemem_ptr_cxt.BackendClientHostnameBuffer;
        for (i = 0; i < BackendStatusArray_size; i++) {
            t_thrd.shemem_ptr_cxt.BackendStatusArray[i].st_clienthostname = buffer;
            buffer += NAMEDATALEN;
        }
    }

    /* Create or attach to the shared activity buffer */
    t_thrd.shemem_ptr_cxt.BackendActivityBufferSize =
        mul_size(g_instance.attr.attr_common.pgstat_track_activity_query_size, BackendStatusArray_size);
    t_thrd.shemem_ptr_cxt.BackendActivityBuffer =
        (char*)ShmemInitStruct("Backend Activity Buffer", t_thrd.shemem_ptr_cxt.BackendActivityBufferSize, &found);

    if (!found) {
        /* call MemsetHugeSize instead because the size may be larger than INT_MAX. */
        MemsetHugeSize(
            t_thrd.shemem_ptr_cxt.BackendActivityBuffer, t_thrd.shemem_ptr_cxt.BackendActivityBufferSize, 0);

        /* Initialize st_activity pointers. */
        buffer = t_thrd.shemem_ptr_cxt.BackendActivityBuffer;
        for (i = 0; i < BackendStatusArray_size; i++) {
            t_thrd.shemem_ptr_cxt.BackendStatusArray[i].st_activity = buffer;
            buffer += g_instance.attr.attr_common.pgstat_track_activity_query_size;
        }
    }

    /* Create or attach to the shared array */
    size = sizeof(PgStat_WaitCountStatus);
    t_thrd.shemem_ptr_cxt.WaitCountBuffer = (PgStat_WaitCountStatus*)ShmemInitStruct("Wait Count Buffer", size, &found);
    if (!found) {
        /* initialize */
        rc = memset_s(t_thrd.shemem_ptr_cxt.WaitCountBuffer, size, 0, size);
        securec_check(rc, "\0", "\0");
    }

    /* Create or attach to the shared relname buffer */
    size = mul_size(NAMEDATALEN * 2, BackendStatusArray_size);
    BackendNspRelnameBuffer = (char*)ShmemInitStruct("Backend Namespace Relname Buffer", size, &found);
    if (!found) {
        rc = memset_s(BackendNspRelnameBuffer, size, 0, size);
        securec_check(rc, "\0", "\0");

        /* Initialize st_relname pointers. */
        buffer = BackendNspRelnameBuffer;
        for (i = 0; i < BackendStatusArray_size; i++) {
            t_thrd.shemem_ptr_cxt.BackendStatusArray[i].st_relname = buffer;
            buffer += NAMEDATALEN * 2;
        }
    }

    if (PgBackendStatusArray == NULL)
        PgBackendStatusArray = t_thrd.shemem_ptr_cxt.BackendStatusArray;
}

/* get entry index of all various stat arrays for auxiliary threads */
static int GetAuxProcStatEntryIndex()
{
    int index = GetAuxProcEntryIndex(MAX_BACKEND_SLOT);
    if (t_thrd.bootstrap_cxt.MyAuxProcType == PageRedoProcess) {
        SetPageRedoWorkerIndex(index);
    }
    return index;
}

static const char* REMOTE_CONN_TYPET[REMOTE_CONN_GTM_TOOL + 1] = {
    "app", "coordinator", "datanode", "gtm", "gtm_proxy", "inetrnal_tool", "gtm_tool"};

const char* remote_conn_type_string(int remote_conn_type)
{
    Assert(remote_conn_type >= REMOTE_CONN_APP && remote_conn_type <= REMOTE_CONN_GTM_TOOL);
    return REMOTE_CONN_TYPET[remote_conn_type];
}

/* ----------
 * pgstat_initialize() -
 *
 *	Initialize pgstats state, and set up our on-proc-exit hook.
 *	Called from InitPostgres.  t_thrd.proc_cxt.MyBackendId must be set,
 *	but we must not have started any transaction yet (since the
 *	exit hook must run after the last transaction exit).
 *	NOTE: u_sess->proc_cxt.MyDatabaseId isn't set yet; so the shutdown hook has to be careful.
 * ----------
 */
void pgstat_initialize(void)
{
    /* Initialize MyBEEntry */
    Assert(t_thrd.shemem_ptr_cxt.BackendStatusArray);

    // backend thread
    if (t_thrd.proc_cxt.MyBackendId != InvalidBackendId) {
        Assert(t_thrd.proc_cxt.MyBackendId >= 1 &&
               t_thrd.proc_cxt.MyBackendId <= (g_instance.attr.attr_common.enable_thread_pool
                                                      ? GLOBAL_RESERVE_SESSION_NUM
                                                      : g_instance.shmem_cxt.MaxBackends));
        t_thrd.shemem_ptr_cxt.MyBEEntry = &t_thrd.shemem_ptr_cxt.BackendStatusArray[t_thrd.proc_cxt.MyBackendId - 1];
    } else {
        // Auxiliary thread
        int index = GetAuxProcStatEntryIndex();
        t_thrd.shemem_ptr_cxt.MyBEEntry = &t_thrd.shemem_ptr_cxt.BackendStatusArray[index];
    }

    /* init local thread bad block statistics */
    initLocalBadBlockStat();

    /* init other session level stats */
    initMySessionStatEntry();
    initMySessionTimeEntry();
    /* at present, stream worker share the same entry with their parent session */
    if (t_thrd.role != STREAM_WORKER)
        initMySessionMemoryEntry();

    /* Set up a process-exit hook to clean up */
    on_shmem_exit(pgstat_beshutdown_hook, 0);
}

/* ----------
 * pgstat_initialize_session() -
 *
 *	change MyBEEntry to session entry
 */
void pgstat_initialize_session(void)
{
    Assert(t_thrd.shemem_ptr_cxt.BackendStatusArray);
    Assert(u_sess->session_ctr_index >= GLOBAL_RESERVE_SESSION_NUM && u_sess->session_ctr_index < MAX_BACKEND_SLOT);
    Assert(t_thrd.proc_cxt.MyBackendId != InvalidBackendId);
    Assert(
        t_thrd.shemem_ptr_cxt.MyBEEntry == &t_thrd.shemem_ptr_cxt.BackendStatusArray[t_thrd.proc_cxt.MyBackendId - 1]);

    /*
     * pool worker reports being coupled to one session
     * app name of thread pool worker might be over-written during session
     * guc initilization, need to restore it.
     */
    pgstat_report_appname("ThreadPoolWorker");
    pgstat_report_activity(STATE_COUPLED, NULL);

    /* change stat object to session */
    t_thrd.shemem_ptr_cxt.MyBEEntry = &t_thrd.shemem_ptr_cxt.BackendStatusArray[u_sess->session_ctr_index];

    /* switch other stat obejct to session */
    AttachMySessionStatEntry();
    AttachMySessionTimeEntry();
    AttachMySessionMemoryEntry();
}

/* ----------
 * pgstat_deinitialize_session() -
 *
 *	change MyBEEntry back to thread pool worker entry
 */
void pgstat_deinitialize_session(void)
{
    Assert(t_thrd.shemem_ptr_cxt.BackendStatusArray);
    Assert((u_sess->session_ctr_index >= GLOBAL_RESERVE_SESSION_NUM && u_sess->session_ctr_index < MAX_BACKEND_SLOT) ||
           u_sess == t_thrd.fake_session);
    Assert(t_thrd.proc_cxt.MyBackendId != InvalidBackendId);

    /* change stat object back to worker thread */
    t_thrd.shemem_ptr_cxt.mySessionStatEntry = &t_thrd.shemem_ptr_cxt.sessionStatArray[t_thrd.proc_cxt.MyBackendId - 1];
    t_thrd.shemem_ptr_cxt.mySessionTimeEntry = &t_thrd.shemem_ptr_cxt.sessionTimeArray[t_thrd.proc_cxt.MyBackendId - 1];
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry =
        &t_thrd.shemem_ptr_cxt.sessionMemoryArray[t_thrd.proc_cxt.MyBackendId - 1];

    /* proc_exit already release the slot of MyBeEntry, cannot release again */
    if (!t_thrd.proc_cxt.proc_exit_inprogress) {
        t_thrd.shemem_ptr_cxt.MyBEEntry = &t_thrd.shemem_ptr_cxt.BackendStatusArray[t_thrd.proc_cxt.MyBackendId - 1];

        /*
         * reset worker's pgproc memory info
         * be careful that during process exit, t_thrd.proc may have been cleared
         */
        t_thrd.proc->sessMemorySessionid = t_thrd.fake_session->session_id;

        /* pool worker reports being decoupled to one session */
        pgstat_report_activity(STATE_DECOUPLED, NULL);
    }
}

/* ----------
 * pgstat_bestart() -
 *
 *	Initialize this backend's entry in the PgBackendStatus array.
 *	Called from InitPostgres.
 *	u_sess->proc_cxt.MyDatabaseId, session userid, and application_name must be set
 *	(hence, this cannot be combined with pgstat_initialize).
 * ----------
 */
void pgstat_bestart(void)
{
    TimestampTz proc_start_timestamp;
    Oid userid = InvalidOid;
    SockAddr clientaddr;
    volatile PgBackendStatus* beentry = NULL;
    errno_t rc = 0;

    /*
     * To minimize the time spent modifying the PgBackendStatus entry, fetch
     * all the needed data first.
     *
     * If we have a u_sess->proc_cxt.MyProcPort, use its session start time (for consistency,
     * and to save a kernel call).
     */
    if (u_sess->proc_cxt.MyProcPort) {
        proc_start_timestamp = u_sess->proc_cxt.MyProcPort->SessionStartTime;
    } else if (t_thrd.proc_cxt.MyStartTime) {
        proc_start_timestamp = t_thrd.proc_cxt.MyStartTime;
    } else {
        proc_start_timestamp = GetCurrentTimestamp();
    }
    if (t_thrd.proc_cxt.MyBackendId != InvalidBackendId && !IsCatchupProcess() &&
        !(IS_THREAD_POOL_WORKER && u_sess->session_id == 0)) {
        userid = GetSessionUserId();  // for backend threads
    }
    /*
     * We may not have a u_sess->proc_cxt.MyProcPort (eg, if this is the autovacuum process).
     * If so, use all-zeroes client address, which is dealt with specially in
     * pg_stat_get_backend_client_addr and pg_stat_get_backend_client_port.
     */
    if (u_sess->proc_cxt.MyProcPort)
        rc = memcpy_s(&clientaddr, sizeof(clientaddr), &u_sess->proc_cxt.MyProcPort->raddr, sizeof(clientaddr));
    else
        rc = memset_s(&clientaddr, sizeof(clientaddr), 0, sizeof(clientaddr));
    securec_check(rc, "\0", "\0");

    /*
     * Initialize my status entry, following the protocol of bumping
     * st_changecount before and after; and make sure it's even afterwards. We
     * use a volatile pointer here to ensure the compiler doesn't try to get
     * cute.
     */
    beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    do {
        pgstat_increment_changecount_before(beentry);
    } while ((beentry->st_changecount & 1) == 0);

    beentry->st_procpid = t_thrd.proc_cxt.MyProcPid;
    if (!ENABLE_THREAD_POOL) {
        beentry->st_sessionid = t_thrd.proc_cxt.MyProcPid;
    } else {
        if (IS_THREAD_POOL_WORKER && u_sess->session_id > 0)
            beentry->st_sessionid = u_sess->session_id;
        else
            beentry->st_sessionid = t_thrd.proc_cxt.MyProcPid;
    }
    beentry->globalSessionId.sessionId = 0;
    beentry->globalSessionId.nodeId = 0;
    beentry->globalSessionId.seq = 0;
    beentry->st_proc_start_timestamp = proc_start_timestamp;
    beentry->st_activity_start_timestamp = 0;
    beentry->st_state_start_timestamp = 0;
    beentry->st_xact_start_timestamp = 0;
    beentry->st_databaseid = u_sess->proc_cxt.MyDatabaseId;
    beentry->st_userid = userid;
    rc = memcpy_s((void*)&beentry->st_clientaddr, sizeof(SockAddr), &clientaddr, sizeof(clientaddr));
    securec_check(rc, "\0", "\0");
    beentry->st_clienthostname[0] = '\0';
    beentry->st_state = STATE_UNDEFINED;
    beentry->st_appname[0] = '\0';
    beentry->st_conninfo[0] = '\0';
    beentry->st_activity[0] = '\0';
    /* Also make sure the last byte in each string area is always 0 */
    beentry->st_clienthostname[NAMEDATALEN - 1] = '\0';
    beentry->st_appname[NAMEDATALEN - 1] = '\0';
    beentry->st_conninfo[CONNECTIONINFO_LEN - 1] = '\0';
    beentry->st_activity[g_instance.attr.attr_common.pgstat_track_activity_query_size - 1] = '\0';

    beentry->st_queryid = 0;
    beentry->st_unique_sql_key.unique_sql_id = 0;
    beentry->st_unique_sql_key.user_id = 0;
    beentry->st_unique_sql_key.cn_id = 0;
    beentry->st_tid = gettid();
    beentry->st_parent_sessionid = 0;
    beentry->st_thread_level = 0;
    beentry->st_smpid = 0;
    beentry->st_waitstatus = STATE_WAIT_UNDEFINED;
    beentry->st_nodeid = -1;
    beentry->st_waitnode_count = 0;
    beentry->st_plannodeid = -1;
    beentry->st_numnodes = -1;
    beentry->trace_cxt.trace_id[0] = '\0';
    /* Initialize wait event information. */
    beentry->st_waitevent = WAIT_EVENT_END;
    beentry->st_xid = 0;
    beentry->st_waitstatus_phase = PHASE_NONE;
    beentry->st_relname[0] = '\0';
    beentry->st_relname[NAMEDATALEN * 2 - 1] = '\0';
    beentry->st_libpq_wait_nodeid = InvalidOid;
    beentry->st_libpq_wait_nodecount = 0;
    beentry->st_tempid = 0;
    beentry->st_timelineid = 0;

    beentry->st_debug_info = &u_sess->wlm_cxt->wlm_debug_info;
    beentry->st_cgname = u_sess->wlm_cxt->control_group;
    beentry->st_stmtmem = 0;
    beentry->st_block_sessionid = 0;

    beentry->st_connect_info = u_sess->pgxc_cxt.PoolerConnectionInfo;
    /*
     * st_gtmhost and st_gtmtimeline fields are not initialized here,
     * because they have already been registered in InitPostgres
     * with StartTransactionCommand.
     */
    /* make this count be odd */
    do {
        beentry->lw_count++;
    } while (CHANGECOUNT_IS_EVEN(beentry->lw_count));
    beentry->lw_want_lock = NULL;
    beentry->lw_held_num = get_held_lwlocks_num();
    beentry->lw_held_locks = get_held_lwlocks();
    beentry->st_lw_access_flag = false;
    beentry->st_lw_is_cleanning_flag = false;

    pgstat_increment_changecount_after(beentry);
    beentry->statement_cxt = bind_statement_context();

    /* add remote node info */
    if (u_sess->proc_cxt.MyProcPort != NULL) {
        if (u_sess->proc_cxt.MyProcPort->is_logic_conn) {
            if (u_sess->proc_cxt.MyProcPort->libcomm_addrinfo != NULL) {
                rc = memcpy_s((void*)&beentry->remote_info.remote_name,
                    NAMEDATALEN,
                    u_sess->proc_cxt.MyProcPort->libcomm_addrinfo->nodename,
                    NAMEDATALEN);
                securec_check(rc, "\0", "\0");
                rc = memcpy_s((void*)&beentry->remote_info.remote_ip,
                    MAX_IP_STR_LEN,
                    u_sess->proc_cxt.MyProcPort->libcomm_addrinfo->host,
                    strlen(u_sess->proc_cxt.MyProcPort->libcomm_addrinfo->host) + 1);
                securec_check(rc, "\0", "\0");
                rc = memcpy_s((void*)&beentry->remote_info.remote_port,
                    MAX_PORT_LEN,
                    &(u_sess->proc_cxt.MyProcPort->libcomm_addrinfo->listen_port),
                    MAX_PORT_LEN);
                securec_check(rc, "\0", "\0");
                beentry->remote_info.socket = -1;
                beentry->remote_info.logic_id = u_sess->proc_cxt.MyProcPort->gs_sock.sid;
            }
        } else {
            if (u_sess->proc_cxt.MyProcPort->sock >= 0) {
                const char* remote_node = remote_conn_type_string(u_sess->attr.attr_common.remoteConnType);
                rc = memcpy_s(
                    (void*)&beentry->remote_info.remote_name, NAMEDATALEN, remote_node, strlen(remote_node) + 1);
                securec_check(rc, "\0", "\0");
                if (u_sess->proc_cxt.MyProcPort->remote_host != NULL) {
                    rc = memcpy_s((void*)&beentry->remote_info.remote_ip,
                        MAX_IP_STR_LEN,
                        u_sess->proc_cxt.MyProcPort->remote_host,
                        strlen(u_sess->proc_cxt.MyProcPort->remote_host) + 1);
                    securec_check(rc, "\0", "\0");
                }

                if (u_sess->proc_cxt.MyProcPort->remote_port != NULL &&
                    u_sess->proc_cxt.MyProcPort->remote_port[0] != '\0') {
                    rc = memcpy_s((void*)&beentry->remote_info.remote_port,
                        MAX_PORT_LEN,
                        u_sess->proc_cxt.MyProcPort->remote_port,
                        strlen(u_sess->proc_cxt.MyProcPort->remote_port) + 1);
                    securec_check(rc, "\0", "\0");
                }

                beentry->remote_info.socket = u_sess->proc_cxt.MyProcPort->sock;
                beentry->remote_info.logic_id = -1;
            }
        }
    }

    if (u_sess->proc_cxt.MyProcPort && u_sess->proc_cxt.MyProcPort->remote_hostname) {
        strlcpy(beentry->st_clienthostname, u_sess->proc_cxt.MyProcPort->remote_hostname, NAMEDATALEN);
    }

    /* Update app name to current GUC setting */
    if (u_sess->attr.attr_common.application_name) {
        pgstat_report_appname(u_sess->attr.attr_common.application_name);
    }

    /* Update connection info to current GUC setting */
    if (u_sess->attr.attr_sql.connection_info) {
        pgstat_report_conninfo(u_sess->attr.attr_sql.connection_info);
    }
}

/*
 * only if light-weight lock information is never accessed by other backends,
 * this backend can exit.
 */
static void WaitUntilLWLockInfoNeverAccess(volatile PgBackendStatus* backendEntry)
{
    START_CRIT_SECTION();

    /* wait until lwAccessFlag flag is false */
    backendEntry->st_lw_is_cleanning_flag = true;
    pg_memory_barrier();
    while (backendEntry->st_lw_access_flag) {
        pg_usleep(1);
    }

    pg_memory_barrier();

    /* clean up light-weight lock information */
    backendEntry->lw_held_num = NULL;
    backendEntry->lw_held_locks = NULL;

    END_CRIT_SECTION();
}

/* ----------
 * pgstat_couple_decouple_session() -
 *
 *	update/remove thread info to coupled/decoupled session backend stat entry
 * ----------
 */
void pgstat_couple_decouple_session(bool is_couple)
{
    volatile PgBackendStatus* beentry = NULL;
    Assert(u_sess->session_id != 0);

    /*
     * update my status entry with new thread info, following the protocol of bumping
     * st_changecount before and after; and make sure it's even afterwards. We
     * use a volatile pointer here to ensure the compiler doesn't try to get
     * cute.
     */
    beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    do {
        pgstat_increment_changecount_before(beentry);
    } while ((beentry->st_changecount & 1) == 0);

    Assert(beentry->st_sessionid == u_sess->session_id || beentry->st_sessionid == 0);
    beentry->st_procpid = is_couple ? t_thrd.proc_cxt.MyProcPid : 0;
    beentry->st_tid = is_couple ? gettid() : 0;
    beentry->lw_held_num = is_couple ? get_held_lwlocks_num() : NULL;
    beentry->lw_held_locks = is_couple ? get_held_lwlocks() : NULL;
    /* make this count be odd */
    do {
        beentry->lw_count++;
    } while (CHANGECOUNT_IS_EVEN(beentry->lw_count));

    pgstat_increment_changecount_after(beentry);
}

/*
 * Shut down a single backend's statistics reporting at process exit.
 *
 * Flush any remaining statistics counts out to the collector.
 * Without this, operations triggered during backend exit (such as
 * temp table deletions) won't be counted.
 *
 * Lastly, clear out our entry in the PgBackendStatus array.
 * For sessions in thread pool, just repoint stat object to pool worker.
 */
static void pgstat_beshutdown_hook(int code, Datum arg)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    /*
     * If we got as far as discovering our own database ID, we can report what
     * we did to the collector.  Otherwise, we'd be sending an invalid
     * database ID, so forget it.  (This means that accesses to pg_database
     * during failed backend starts might never get counted.)
     */
    /* u_sess->proc_cxt.MyDatabaseId will be zero for AuxiliaryProcessMain threads
     * u_sess->proc_cxt.MyDatabaseId maybe non-zero only for backend threads
     */
    if (OidIsValid(u_sess->proc_cxt.MyDatabaseId))
        pgstat_report_stat(true);

    /*
     * Clear my status entry, following the protocol of bumping st_changecount
     * before and after.  We use a volatile pointer here to ensure the
     * compiler doesn't try to get cute.
     */
    pgstat_increment_changecount_before(beentry);

    beentry->st_procpid = 0;   /* mark pid invalid */
    beentry->st_sessionid = 0; /* mark sessionid invalid */
    beentry->globalSessionId.sessionId = 0;
    beentry->globalSessionId.nodeId = 0;
    beentry->globalSessionId.seq = 0;

    /*
     * make sure st_changecount is an even before release it.
     *
     * In case some thread was interrupted by SIGTERM at any time with a mess st_changecount
     * in PgBackendStatus, PgstatCollectorMain may hang-up in waiting its change to even and
     * can not exit after receiving SIGTERM signal
     */
    do {
        pgstat_increment_changecount_after(beentry);
    } while ((beentry->st_changecount & 1) != 0);

    /*
     * handle below cases:
     * if thread pool worker is shuting down, this hook is called previously,
     * we need to release thread/session statement contexts.
     * normal backend, only need to release thread statement context.
     */
    if (IS_THREAD_POOL_WORKER && u_sess->session_ctr_index >= GLOBAL_RESERVE_SESSION_NUM &&
        u_sess->session_ctr_index < MAX_BACKEND_SLOT) {
        release_statement_context(&t_thrd.shemem_ptr_cxt.BackendStatusArray[u_sess->session_ctr_index],
            __FUNCTION__, __LINE__);
    }
    if (t_thrd.proc_cxt.MyBackendId != InvalidBackendId) {
        release_statement_context(&t_thrd.shemem_ptr_cxt.BackendStatusArray[t_thrd.proc_cxt.MyBackendId - 1],
            __FUNCTION__, __LINE__);
    } else {
        release_statement_context(t_thrd.shemem_ptr_cxt.MyBEEntry, __FUNCTION__, __LINE__);
    }

    WaitUntilLWLockInfoNeverAccess(beentry);

    /*
     * Clear the thread-local pointer MyBEEntry
     * so that it can not be revisited.
     */
    t_thrd.shemem_ptr_cxt.MyBEEntry = NULL;
}

/*
 * Shut down a single session's statistics reporting at session close.
 *
 * Flush any remaining statistics counts out to the collector.
 * Without this, operations triggered during backend exit (such as
 * temp table deletions) won't be counted.
 */
void pgstat_beshutdown_session(int ctrl_index)
{
    /* proc_exit already release the slot of MyBeEntry, cannot release again */
    if (!t_thrd.proc_cxt.proc_exit_inprogress) {
        volatile PgBackendStatus* beentry = &t_thrd.shemem_ptr_cxt.BackendStatusArray[ctrl_index];

        /*
         * If we got as far as discovering our own database ID, we can report what
         * we did to the collector.  Otherwise, we'd be sending an invalid
         * database ID, so forget it.  (This means that accesses to pg_database
         * during failed backend starts might never get counted.)
         */
        if (OidIsValid(beentry->st_databaseid))
            pgstat_report_stat(true);

        /*
         * Clear my status entry, following the protocol of bumping st_changecount
         * before and after.  We use a volatile pointer here to ensure the
         * compiler doesn't try to get cute.
         */
        pgstat_increment_changecount_before(beentry);
        beentry->st_procpid = 0;   /* mark pid invalid */
        beentry->st_sessionid = 0; /* mark sessionid invalid */
        beentry->globalSessionId.sessionId = 0;
        beentry->globalSessionId.nodeId = 0;
        beentry->globalSessionId.seq = 0;

        /*
         * make sure st_changecount is an even before release it.
         *
         * In case some thread was interrupted by SIGTERM at any time with a mess st_changecount
         * in PgBackendStatus, PgstatCollectorMain may hang-up in waiting its change to even and
         * can not exit after receiving SIGTERM signal
         */
        do {
            pgstat_increment_changecount_after(beentry);
        } while ((beentry->st_changecount & 1) != 0);

        /*
         * pgstat_beshutdown_session will be called in thread pool mode:
         * if t_thrd.proc_cxt.proc_exit_inprogress is true, thread backend entry and
         * session backend entry can be reused by other backend(CleanupInvalidationState
         * is called before), so backend entry cann't be accessed during this stage.
         * when false, just case that thread pool worker is detaching or closing session, 
         * so we need to release statemement context.
         */
        release_statement_context(&t_thrd.shemem_ptr_cxt.BackendStatusArray[ctrl_index], __FUNCTION__, __LINE__);

        /*
         * During process exit, t_thrd.proc may have been cleared, so that
         * we can no longer hold LWlocks.
         * Maybe we should move clean up of session before worker thread
         * clears its share memory stuffs.
         */
        DetachMySessionTimeEntry(&t_thrd.shemem_ptr_cxt.sessionTimeArray[ctrl_index]);

        /* We must guarantee atomic for set access flag */
        START_CRIT_SECTION();

        /* Wait until st_lw_access_flag is false */
        beentry->st_lw_is_cleanning_flag = true;
        pg_read_barrier();
        while (beentry->st_lw_access_flag) {
            (void)sched_yield();
            pg_usleep(1);
        }

        beentry->lw_held_num = NULL;
        beentry->lw_held_locks = NULL;
        beentry->st_lw_is_cleanning_flag = false;

        END_CRIT_SECTION();
    }

    /* shutdown other session level stats */
    t_thrd.shemem_ptr_cxt.sessionStatArray[ctrl_index].isValid = false;

    t_thrd.shemem_ptr_cxt.sessionTimeArray[ctrl_index].changeCount++;

    /* mark not active. */
    t_thrd.shemem_ptr_cxt.sessionTimeArray[ctrl_index].isActive = false;

    t_thrd.shemem_ptr_cxt.sessionTimeArray[ctrl_index].changeCount++;
    Assert((t_thrd.shemem_ptr_cxt.sessionTimeArray[ctrl_index].changeCount & 1) == 0);

    t_thrd.shemem_ptr_cxt.sessionMemoryArray[ctrl_index].isValid = false;
}

/* ----------
 * pgstat_report_activity() -
 *
 *	Called from tcop/postgres.c to report what the backend is actually doing
 *	(usually "<IDLE>" or the start of the query to be executed).
 *
 * All updates of the status entry follow the protocol of bumping
 * st_changecount before and after.  We use a volatile pointer here to
 * ensure the compiler doesn't try to get cute.
 * ----------
 */
void pgstat_report_activity(BackendState state, const char* cmd_str)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    TimestampTz start_timestamp;
    TimestampTz current_timestamp;
    int len = 0;
    errno_t rc = EOK;

    TRACE_POSTGRESQL_STATEMENT_STATUS(cmd_str);

    if (beentry == NULL)
        return;

    if (!u_sess->attr.attr_common.pgstat_track_activities && beentry->st_state == STATE_DISABLED)
        return;

    /*
     * To minimize the time spent modifying the entry, fetch all the needed
     * data first.
     */
    current_timestamp = GetCurrentTimestamp();

    if (!u_sess->attr.attr_common.pgstat_track_activities && beentry->st_state != STATE_DISABLED) {
        /*
         * Track activities is disabled, but we have a non-disabled state set.
         * That means the status changed - so as our last update, tell the
         * collector that we disabled it and will no longer update.
         */
        pgstat_increment_changecount_before(beentry);
        beentry->st_state = STATE_DISABLED;
        beentry->st_state_start_timestamp = current_timestamp;
        pgstat_increment_changecount_after(beentry);
        return;
    }

    /*
     * Fetch more data before we start modifying the entry
     */
    if (IS_SINGLE_NODE || IS_PGXC_DATANODE)
        start_timestamp = GetCurrentStatementLocalStartTimestamp();
    else
        start_timestamp = GetCurrentStatementStartTimestamp();

    if (cmd_str != NULL) {
        len = pg_mbcliplen(cmd_str, strlen(cmd_str), g_instance.attr.attr_common.pgstat_track_activity_query_size - 1);
    }

    /*
     * Now update the status entry
     */
    pgstat_increment_changecount_before(beentry);

    beentry->st_state = state;
    beentry->st_state_start_timestamp = current_timestamp;

    if (cmd_str != NULL) {
        char *mask_string = NULL;
        if (len == g_instance.attr.attr_common.pgstat_track_activity_query_size - 1 &&
            t_thrd.mem_cxt.mask_password_mem_cxt != NULL) {
            /* mask the cmd_str when the cmd_str is truncated. */
            mask_string = maskPassword(cmd_str);
        }

        /* If mask successfully, store the mask_string. Otherwise, the cmd_str is recorded. */
        if (mask_string == NULL) {
            rc = memcpy_s((char*)beentry->st_activity, g_instance.attr.attr_common.pgstat_track_activity_query_size,
                cmd_str, len);
            securec_check(rc, "\0", "\0");
        } else {
            int copy_len = strlen(mask_string);
            if (len < copy_len) {
                copy_len = len;
            }
            rc = memcpy_s((char*)beentry->st_activity, g_instance.attr.attr_common.pgstat_track_activity_query_size,
                mask_string, copy_len);
            securec_check(rc, "\0", "\0");
            pfree(mask_string);
        }

        beentry->st_activity[len] = '\0';
        beentry->st_activity_start_timestamp = start_timestamp;
    }

    pgstat_increment_changecount_after(beentry);
}

/* ----------
 * pgstat_report_appname() -
 *
 *	Called to update our application name.
 * ----------
 */
void pgstat_report_appname(const char* appname)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    int len;
    errno_t rc;

    if (beentry == NULL)
        return;

    /* This should be unnecessary if GUC did its job, but be safe */
    len = pg_mbcliplen(appname, strlen(appname), NAMEDATALEN - 1);

    /*
     * Update my status entry, following the protocol of bumping
     * st_changecount before and after.  We use a volatile pointer here to
     * ensure the compiler doesn't try to get cute.
     */
    pgstat_increment_changecount_before(beentry);

    if (len > 0) {
        rc = memcpy_s((char*)beentry->st_appname, len, appname, len);
        securec_check(rc, "\0", "\0");
    }

    beentry->st_appname[len] = '\0';

    pgstat_increment_changecount_after(beentry);
}

/* ----------
 * pgstat_report_conninfo() -
 *
 *	Called to update our connection info.
 * ----------
 */
void pgstat_report_conninfo(const char* conninfo)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    int len;
    errno_t rc;

    if (beentry == NULL)
        return;

    /* This should be unnecessary if GUC did its job, but be safe */
    len = pg_mbcliplen(conninfo, strlen(conninfo), CONNECTIONINFO_LEN - 1);

    /*
     * Update my status entry, following the protocol of bumping
     * st_changecount before and after.  We use a volatile pointer here to
     * ensure the compiler doesn't try to get cute.
     */
    pgstat_increment_changecount_before(beentry);

    if (len > 0) {
        rc = memcpy_s((char*)beentry->st_conninfo, len, conninfo, len);
        securec_check(rc, "\0", "\0");
    }

    beentry->st_conninfo[len] = '\0';

    pgstat_increment_changecount_after(beentry);
}

/*
 * Report current transaction start timestamp as the specified value.
 * Zero means there is no active transaction.
 */
void pgstat_report_xact_timestamp(TimestampTz tstamp)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (!u_sess->attr.attr_common.pgstat_track_activities || (beentry == NULL))
        return;

    /*
     * Update my status entry, following the protocol of bumping
     * st_changecount before and after.  We use a volatile pointer here to
     * ensure the compiler doesn't try to get cute.
     */
    pgstat_increment_changecount_before(beentry);
    beentry->st_xact_start_timestamp = tstamp;
    pgstat_increment_changecount_after(beentry);
}

void pgstat_report_global_session_id(GlobalSessionId globalSessionId)
{
#ifndef ENABLE_MULTIPLE_NODES
    return;
#endif
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (IS_PGSTATE_TRACK_UNDEFINE || globalSessionId.sessionId == 0)
        return;
    pgstat_increment_changecount_before(beentry);
    beentry->globalSessionId.sessionId = globalSessionId.sessionId;
    beentry->globalSessionId.nodeId = globalSessionId.nodeId;
    beentry->globalSessionId.seq = globalSessionId.seq;
    pgstat_increment_changecount_after(beentry);
}

void pgstat_report_unique_sql_id(bool resetUniqueSql)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return;
    if (resetUniqueSql) {
        beentry->st_unique_sql_key.unique_sql_id = 0;
        beentry->st_unique_sql_key.cn_id = 0;
        beentry->st_unique_sql_key.user_id = 0;
    } else {
        beentry->st_unique_sql_key.unique_sql_id = u_sess->unique_sql_cxt.unique_sql_id;
        beentry->st_unique_sql_key.cn_id = u_sess->unique_sql_cxt.unique_sql_cn_id;
        beentry->st_unique_sql_key.user_id = u_sess->unique_sql_cxt.unique_sql_user_id;
    }
}

void pgstat_report_queryid(uint64 queryid)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return;

    /*
     * Since this is a single-byte field in a struct that only this process
     * may modify, there seems no need to bother with the st_changecount
     * protocol.  The update must appear atomic in any case.
     */
    beentry->st_queryid = queryid;
}

void pgstat_report_trace_id(knl_u_trace_context *trace_cxt, bool is_report_trace_id)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    if (IS_PGSTATE_TRACK_UNDEFINE)
        return;
    if (is_report_trace_id) {
        errno_t rc =
            memcpy_s((void*)beentry->trace_cxt.trace_id, MAX_TRACE_ID_SIZE, trace_cxt->trace_id,
                     strlen(trace_cxt->trace_id) + 1);
        securec_check(rc, "\0", "\0");
    }
}

void pgstat_report_jobid(uint64 jobid)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return;

    /*
     * Update jobid when it starts
     */
    beentry->st_jobid = jobid;
}

void pgstat_report_parent_sessionid(uint64 sessionid, uint32 level)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return;

    /*
     * Since this is a single-byte field in a struct that only this process
     * may modify, there seems no need to bother with the st_changecount
     * protocol.  The update must appear atomic in any case.
     */
    beentry->st_sessionid = sessionid;
    beentry->st_parent_sessionid = sessionid;
    beentry->st_thread_level = level;
}

/* ----------
 * pgstat_report_connected_gtm_host() -
 *
 * Called from InitGTM to report the latest connected GTM host index.
 * Also Called from CloseGTM to clear the above information.
 *
 * NB: this *must* be able to survive being called before MyBEEntry has been
 * initialized.
 * ----------
 */
void pgstat_report_connected_gtm_host(GtmHostIndex gtm_host)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (beentry == NULL)
        return;

    /*
     * Since this is an enumeration-type field in a struct that
     * only this process may modify, there seems no need
     * to bother with the st_changecount protocol.
     *
     * The update must appear atomic in any case.
     */
    beentry->st_gtmhost = gtm_host;
}

/* ----------
 * pgstat_report_connected_gtm_timeline() -
 *
 * Called from InitGTM_Reporttimeline to report
 * the latest connected GTM host timeline.
 * Also Called from CloseGTM to clear the above information.
 *
 * NB: this *must* be able to survive being called before MyBEEntry has been
 * initialized.
 * ----------
 */
void pgstat_report_connected_gtm_timeline(GTM_Timeline gtm_timeline)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (beentry == NULL)
        return;

    /*
     * Since this is an uint32-type field in a struct that
     * only this process may modify, there seems no need
     * to bother with the st_changecount protocol.
     *
     * The update must appear atomic in any case.
     */
    beentry->st_gtmtimeline = gtm_timeline;
}

void pgstat_report_smpid(uint32 smpid)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return;

    /*
     * Since this is a single-byte field in a struct that only this process
     * may modify, there seems no need to bother with the st_changecount
     * protocol.  The update must appear atomic in any case.
     */
    beentry->st_smpid = smpid;
}

/*
 * report blocking session into waitLockThrd's PgBackendStatus
 */
void pgstat_report_blocksid(void* waitLockThrd, uint64 blockSessionId)
{
    if (IS_PGSTATE_TRACK_UNDEFINE)
        return;

    knl_thrd_context* thrd = (knl_thrd_context*)waitLockThrd;
    volatile PgBackendStatus* beentry = thrd->shemem_ptr_cxt.MyBEEntry;

    if (beentry->st_block_sessionid != blockSessionId) {
        /* dont print lock info while wait ends. Moreover waitLock is not access at this time. */
        if (blockSessionId == 0) {
            ereport(DEBUG1,
                (errmsg("thread %lu waiting for lock ends", thrd->proc_cxt.MyProcPid)));
        } else if (thrd->proc->waitLock != NULL) {
            ereport(DEBUG1,
                (errmsg("thread %lu waiting for %s on %s, blocking session %lu",
                    thrd->proc_cxt.MyProcPid,
                    GetLockmodeName(thrd->proc->waitLock->tag.locktag_lockmethodid, thrd->proc->waitLockMode),
                    LocktagToString(thrd->proc->waitLock->tag),
                    blockSessionId)));
        }

        /*
         * NOTE: use 'volatile' to control concurrency instead of 'changecount' here since this
         * mothod could be not only invoked by itself thread but also other thread. 'changecount'
         * can work well only when write is done by one thread.
         */
        beentry->st_block_sessionid = blockSessionId;
    }
}

/*
 * updateMaxValueForAtomicType - using atomic type to store max value,
 * we need update the max value by using atomic method
 */
static void updateMaxValueForAtomicType(uint64 new_val, uint64* max)
{
    uint64 prev;
    do
        prev = *max;
    while (prev < new_val && !pg_atomic_compare_exchange_u64(max, &prev, new_val));
}

/*
 * updateMinValueForAtomicType - update ming value for atomic type
 */
static void updateMinValueForAtomicType(uint64 new_val, uint64* mix)
{
    uint64 prev;
    do
        prev = *mix;
    while ((prev == 0 || prev > new_val) && !pg_atomic_compare_exchange_u64(mix, &prev, new_val));
}

/*
 * @Description: remove user from WaitCountHashTbl and WaitCountStatusList for the user does not exist.
 * @in -userid: oid of the user who was dropped
 * @out - void
 */
static void RemoveWaitCount(Oid userId)
{
    if (g_instance.stat_cxt.WaitCountHashTbl == NULL) {
        return;
    }

    /* remove user from WaitCountHashTbl */
    WaitCountHashValue* waitCountIdx =
        (WaitCountHashValue*)hash_search(g_instance.stat_cxt.WaitCountHashTbl, &userId, HASH_REMOVE, NULL);
    if (waitCountIdx == NULL) {
        return;
    }

    /* reset the location in WaitCountStatusList */
    int dataId = waitCountIdx->idx % WAIT_COUNT_ARRAY_SIZE;
    int listNodeId = waitCountIdx->idx / WAIT_COUNT_ARRAY_SIZE;
    ListCell* lc = list_nth_cell(g_instance.stat_cxt.WaitCountStatusList, listNodeId);
    PgStat_WaitCountStatusCell* waitCountStatusCell = (PgStat_WaitCountStatusCell*)lfirst(lc);
    pg_atomic_write_u32(&waitCountStatusCell->WaitCountArray[dataId].userid, 0);
}

bool pg_check_authid(Oid authid)
{
    HeapTuple roletup = NULL;

    /*
     * Get the pg_authid entry and print the result
     */
    roletup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(authid));
    if (HeapTupleIsValid(roletup)) {
        ReleaseSysCache(roletup);
        return true;
    } else {
        return false;
    }
}

/*
 * @Description: check whether the user exists, and remove sql count if not exist and required.
 * @in1 - userid: user oid.
 * @in2 - removeCount: if remove sql count when user does not exist.
 * @out - bool
 */
bool CheckUserExist(Oid userId, bool removeCount)
{
    if (!OidIsValid(userId)) {
        return false;
    }

    bool isExist = pg_check_authid(userId);
    if (!isExist && removeCount) {
        /* remove the user from sql count list */
        LWLockRelease(WaitCountHashLock);
        LWLockAcquire(WaitCountHashLock, LW_EXCLUSIVE);
        RemoveWaitCount(userId);
        LWLockRelease(WaitCountHashLock);
        LWLockAcquire(WaitCountHashLock, LW_SHARED);
    }
    return isExist;
}
#define UPDATE_SQL_COUNT(count, elapseTime)                                                       \
    do {                                                                                          \
        TimestampTz duration = GetCurrentTimestamp() - GetCurrentStatementLocalStartTimestamp();  \
        duration = (duration == 0) ? 1 : duration;                                                \
        pg_atomic_fetch_add_u64(&(count), 1);                                                     \
        pg_atomic_fetch_add_u64(&((elapseTime).total_time), duration);                            \
        updateMaxValueForAtomicType(duration, &((elapseTime).max_time));                          \
        updateMinValueForAtomicType(duration, &((elapseTime).min_time));                          \
    } while (0)
/*
 * @Description:  according to wait_event_info to add sql count for user,
 *    add action realize by pg_atomic_fetch_add_u64 function
 * @in wait_event_info - one kind of WaitEventSQL
 * @out - void
 */
void pgstat_report_wait_count(unsigned int wait_event_info)
{
    Oid userid;
    WaitCountHashValue* WaitCountIdx = NULL;
    int dataid;
    int listNodeid;
    uint32 classId = wait_event_info & 0xFF000000;

    LWLockAcquire(WaitCountHashLock, LW_SHARED);
    /* check if hash table exist */
    if (g_instance.stat_cxt.WaitCountHashTbl == NULL) {
        ereport(LOG, (errcode(ERRCODE_WARNING), (errmsg("sql count hashtable: WaitCountHashTbl is not initialized!"))));
        LWLockRelease(WaitCountHashLock);
        return;
    }

    userid = GetUserId();
    WaitCountIdx = (WaitCountHashValue*)hash_search(g_instance.stat_cxt.WaitCountHashTbl, &userid, HASH_FIND, NULL);
    /* check if find the user in hashtable */
    if (WaitCountIdx == NULL) {
        ereport(LOG,
            (errcode(ERRCODE_WARNING), (errmsg("can not find the user in sql count hashtable: userid %u", userid))));
        LWLockRelease(WaitCountHashLock);

        /* return if user does not exist */
        if (!CheckUserExist(userid, false)) {
            return;
        }

        LWLockAcquire(WaitCountHashLock, LW_EXCLUSIVE);
        WaitCountIdx = (WaitCountHashValue*)hash_search(g_instance.stat_cxt.WaitCountHashTbl, &userid, HASH_FIND, NULL);
        if (WaitCountIdx == NULL) {
            initWaitCount(userid);
            ereport(LOG,
                (errcode(ERRCODE_WARNING),
                    (errmsg("success to insert user into WaitCountHashTbl: userid %u!", userid))));
        }
        LWLockRelease(WaitCountHashLock);
        LWLockAcquire(WaitCountHashLock, LW_SHARED);
        WaitCountIdx = (WaitCountHashValue*)hash_search(g_instance.stat_cxt.WaitCountHashTbl, &userid, HASH_FIND, NULL);
        if (WaitCountIdx == NULL) {
            ereport(WARNING,
                (errcode(ERRCODE_WARNING),
                    (errmsg("failed to insert user into WaitCountHashTbl: userid %u!, sql count failed!", userid))));
            LWLockRelease(WaitCountHashLock);
            return;
        }
    }

    /* Get the location of the user in g_instance.stat_cxt.WaitCountStatusList */
    dataid = WaitCountIdx->idx % WAIT_COUNT_ARRAY_SIZE;
    listNodeid = WaitCountIdx->idx / WAIT_COUNT_ARRAY_SIZE;
    ListCell* lc = NULL;
    PgStat_WaitCountStatusCell* WaitCountStatusCell = NULL;

    lc = list_nth_cell(g_instance.stat_cxt.WaitCountStatusList, listNodeid);
    WaitCountStatusCell = (PgStat_WaitCountStatusCell*)lfirst(lc);

    /* Using pg atomic function to add count for corresponsible WaitEventSQL */
    if (classId == PG_WAIT_SQL) {
        WaitEventSQL w = (WaitEventSQL)wait_event_info;
        switch (w) {
            case WAIT_EVENT_SQL_SELECT: {
                UPDATE_SQL_COUNT(WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_select,
                    WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.selectElapse);
            } break;
            case WAIT_EVENT_SQL_UPDATE: {
                UPDATE_SQL_COUNT(WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_update,
                    WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.updateElapse);
            } break;
            case WAIT_EVENT_SQL_INSERT: {
                UPDATE_SQL_COUNT(WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_insert,
                    WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.insertElapse);
            } break;
            case WAIT_EVENT_SQL_DELETE: {
                UPDATE_SQL_COUNT(WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_delete,
                    WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.deleteElapse);
            } break;
            case WAIT_EVENT_SQL_MERGEINTO:
                pg_atomic_fetch_add_u64(&(WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_mergeinto), 1);
                break;
            case WAIT_EVENT_SQL_DDL:
                pg_atomic_fetch_add_u64(&(WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_ddl), 1);
                break;
            case WAIT_EVENT_SQL_DML:
                pg_atomic_fetch_add_u64(&(WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_dml), 1);
                break;
            case WAIT_EVENT_SQL_DCL:
                pg_atomic_fetch_add_u64(&(WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_dcl), 1);
                break;
            case WAIT_EVENT_SQL_TCL:
                pg_atomic_fetch_add_u64(&(WaitCountStatusCell->WaitCountArray[dataid].wc_cnt.wc_sql_tcl), 1);
                break;
            default:
                break;
        }
    }
    LWLockRelease(WaitCountHashLock);
}

/* ----------
 * pgstat_report_waiting_on_resource() -
 *
 *	Called from GTM to report beginning or end of a wait on reserving memory.
 *
 * ----------
 */
void pgstat_report_waiting_on_resource(WorkloadManagerEnqueueState waiting)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return;

    beentry->st_waiting_on_resource = waiting;
    switch (waiting) {
        case STATE_ACTIVE_STATEMENTS:
            (void)pgstat_report_waitstatus(STATE_WAIT_ACTIVE_STATEMENT);
            break;
        case STATE_MEMORY:
            (void)pgstat_report_waitstatus(STATE_WAIT_MEMORY);
            break;
        case STATE_NO_ENQUEUE:
            (void)pgstat_report_waitstatus(STATE_WAIT_UNDEFINED);
            break;
        default:
            (void)pgstat_report_waitstatus(STATE_WAIT_UNDEFINED);
            break;
    }
}

/* ----------
 * pgstat_report_statement_wlm_status() -
 *
 * set current statement wlm status, include block time, cpu time.
 * ----------
 */
void pgstat_report_statement_wlm_status()
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (IS_PGSTATE_TRACK_UNDEFINE)
        return;

    /* get current workload backend state */
    WLMGetStatistics((TimestampTz*)&beentry->st_block_start_time,
        (TimestampTz*)&beentry->st_elapsed_start_time,
        (WLMStatistics*)&beentry->st_backstat);
}

/* ----------
 * pgstat_refresh_statement_wlm_time() -
 *
 * refresh the block time and elapsed time for a statement state.
 * ----------
 */
void pgstat_refresh_statement_wlm_time(volatile PgBackendStatus* beentry)
{
    if (NULL != beentry) {
        WLMStatistics* backstat = (WLMStatistics*)&beentry->st_backstat;

        if (!StringIsValid(backstat->status))
            return;

        /* compute the block time and elapsed time */
        if (strcmp(backstat->status, "pending") == 0) {
            backstat->blocktime = (GetCurrentTimestamp() - beentry->st_block_start_time) / USECS_PER_SEC;
            backstat->elapsedtime = 0;
        } else if (strcmp(backstat->status, "running") == 0)
            backstat->elapsedtime = (GetCurrentTimestamp() - beentry->st_elapsed_start_time) / USECS_PER_SEC;

        WLMDataIndicator<int64> indicator;

        errno_t errval = memset_s(&indicator, sizeof(indicator), 0, sizeof(indicator));
        securec_check_errval(errval, , LOG);

        /* get cpu collect info */
        WLMGetCPUDataIndicator((PgBackendStatus*)beentry, &indicator);

        backstat->maxcputime = indicator.max_value / MSECS_PER_SEC;
        backstat->totalcputime = indicator.total_value / MSECS_PER_SEC;
        backstat->skewpercent = indicator.skew_percent;
    }
}

/* ----------
 * pgstat_increase_session_spill() -
 *
 *  increase current session spill count.
 * ----------
 */
void pgstat_increase_session_spill(void)
{
    gs_atomic_add_32(&t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->spillCount, 1);
}

/* ----------
 * pgstat_increase_session_spill_size() -
 *
 *  increase current session spill size.
 * ----------
 */
void pgstat_increase_session_spill_size(int64 size)
{
    gs_atomic_add_64(&t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->spillSize, size);
}

/* ----------
 * pgstat_add_warning_early_spill() -
 *
 *  add warning for early spill.
 * ----------
 */
void pgstat_add_warning_early_spill()
{
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->warning |= (1 << WLM_WARN_EARLY_SPILL);
}

/* ----------
 * pgstat_add_warning_spill_on_memory_spread() -
 *
 *  add warning for spill on memory spread.
 * ----------
 */
void pgstat_add_warning_spill_on_memory_spread()
{
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->warning |= (1 << WLM_WARN_SPILL_ON_MEMORY_SPREAD);
}

/* ----------
 * pgstat_add_warning_spill_on_memory_spread() -
 *
 *  add warning for spill on memory spread.
 * ----------
 */
void pgstat_add_warning_hash_conflict()
{
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->warning |= (1 << WLM_WARN_HASH_CONFLICT);
}

/* ----------
 * pgstat_set_io_state() -
 *
 *  set backend io state.
 * ----------
 */
void pgstat_set_io_state(WorkloadManagerIOState iostate)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (!u_sess->attr.attr_common.pgstat_track_activities || (beentry == NULL))
        return;

    /* set current workload backend state */
    beentry->st_io_state = iostate;
    u_sess->wlm_cxt->wlm_params.iostate = (unsigned char)iostate;
}

/* ----------
 * pgstat_set_stmt_tag() -
 *
 *  set backend statement tag.
 * ----------
 */
void pgstat_set_stmt_tag(WorkloadManagerStmtTag stmttag)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;

    if (!u_sess->attr.attr_common.pgstat_track_activities || (beentry == NULL))
        return;

    /* set current workload backend statement tag */
    beentry->st_stmttag = stmttag;
}

WaitInfo* read_current_instr_wait_info(void)
{
    volatile PgBackendStatus* beentry = NULL;
    int i;
    errno_t rc;
    WaitInfo* gsInstrWaitInfo = (WaitInfo*)palloc0(sizeof(WaitInfo) * 1);
    beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray;

    for (i = 1; i <= BackendStatusArray_size; i++) {
        /*
         * Follow the protocol of retrying if st_changecount changes while we
         * copy the entry, or if it's odd.  (The check for odd is needed to
         * cover the case where we are able to completely copy the entry while
         * the source backend is between increment steps.)	We use a volatile
         * pointer here to ensure the compiler doesn't try to get cute.
         */
        WaitInfo waitinfo;
        for (;;) {
            int before_changecount;
            int after_changecount;

            pgstat_save_changecount_before(beentry, before_changecount);
            rc = memcpy_s(&waitinfo, sizeof(WaitInfo), (WaitInfo*)&beentry->waitInfo, sizeof(WaitInfo));
            securec_check(rc, "", "");
            pgstat_save_changecount_after(beentry, after_changecount);
            if (before_changecount == after_changecount && (before_changecount & 1) == 0)
                break;

            /* Make sure we can break out of loop if stuck... */
            CHECK_FOR_INTERRUPTS();
        }
        CollectWaitInfo(gsInstrWaitInfo, waitinfo.status_info, waitinfo.event_info);
        beentry++;
    }
    return gsInstrWaitInfo;
}

/* ----------
 * pgstat_reset_current_status() -
 *
 *  reset current status
 * ----------
 */
void pgstat_reset_current_status(void)
{
    if (u_sess->stat_cxt.pgStatLocalContext) {
        MemoryContextDelete(u_sess->stat_cxt.pgStatLocalContext);
        u_sess->stat_cxt.pgStatLocalContext = NULL;
    }

    u_sess->stat_cxt.localBackendStatusTable = NULL;
}

/* ----------
 * pgstat_get_waitlock() -
 *
 * Return true if waiting for a lmgr lock.
 * ----------
 */
bool pgstat_get_waitlock(uint32 wait_event_info)
{
    uint32 classId;
    classId = wait_event_info & 0xFF000000;

    return classId == PG_WAIT_LOCK;
}

/* ----------
 * pgstat_get_wait_event() -
 *
 * Return a string representing the current wait event, backend is
 * waiting on.
 */
const char* pgstat_get_wait_event(uint32 wait_event_info)
{
    uint32 classId;
    uint16 eventId;
    const char* event_name = NULL;

    classId = wait_event_info & 0xFF000000;
    eventId = wait_event_info & 0x0000FFFF;

    switch (classId) {
        case PG_WAIT_LWLOCK:
            event_name = GetLWLockIdentifier(classId, eventId);
            break;
        case PG_WAIT_LOCK:
            event_name = GetLockNameFromTagType(eventId);
            break;
        case PG_WAIT_IO: {
            WaitEventIO w = (WaitEventIO)wait_event_info;
            event_name = pgstat_get_wait_io(w);
            break;
        }
        default:
            event_name = "unknown wait event";
            break;
    }
    return event_name;
}

void LWLockReportWaitFailed(LWLock* lock)
{
    pgstat_report_wait_lock_failed(PG_WAIT_LWLOCK | lock->tranche);
}

/* ----------
 * pgstat_get_wait_io() -
 *
 * Convert WaitEventIO to string.
 * ----------
 */
const char* pgstat_get_wait_io(WaitEventIO w)
{
    const char* event_name = "unknown wait event";

    switch (w) {
        case WAIT_EVENT_BUFFILE_READ:
            event_name = "BufFileRead";
            break;
        case WAIT_EVENT_BUFFILE_WRITE:
            event_name = "BufFileWrite";
            break;
        case WAIT_EVENT_BUF_HASH_SEARCH:
            event_name = "BufHashTableSearch";
            break;
        case WAIT_EVENT_BUF_STRATEGY_GET:
            event_name = "StrategyGetBuffer";
            break;
        case WAIT_EVENT_CONTROL_FILE_READ:
            event_name = "ControlFileRead";
            break;
        case WAIT_EVENT_CONTROL_FILE_SYNC:
            event_name = "ControlFileSync";
            break;
        case WAIT_EVENT_CONTROL_FILE_SYNC_UPDATE:
            event_name = "ControlFileSyncUpdate";
            break;
        case WAIT_EVENT_CONTROL_FILE_WRITE:
            event_name = "ControlFileWrite";
            break;
        case WAIT_EVENT_CONTROL_FILE_WRITE_UPDATE:
            event_name = "ControlFileWriteUpdate";
            break;
        case WAIT_EVENT_COPY_FILE_READ:
            event_name = "CopyFileRead";
            break;
        case WAIT_EVENT_COPY_FILE_WRITE:
            event_name = "CopyFileWrite";
            break;
        case WAIT_EVENT_DATA_FILE_EXTEND:
            event_name = "DataFileExtend";
            break;
        case WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC:
            event_name = "DataFileImmediateSync";
            break;
        case WAIT_EVENT_DATA_FILE_PREFETCH:
            event_name = "DataFilePrefetch";
            break;
        case WAIT_EVENT_DATA_FILE_READ:
            event_name = "DataFileRead";
            break;
        case WAIT_EVENT_DATA_FILE_SYNC:
            event_name = "DataFileSync";
            break;
        case WAIT_EVENT_DATA_FILE_TRUNCATE:
            event_name = "DataFileTruncate";
            break;
        case WAIT_EVENT_DATA_FILE_WRITE:
            event_name = "DataFileWrite";
            break;
        case WAIT_EVENT_LOCK_FILE_ADDTODATADIR_READ:
            event_name = "LockFileAddToDataDirRead";
            break;
        case WAIT_EVENT_LOCK_FILE_ADDTODATADIR_SYNC:
            event_name = "LockFileAddToDataDirSync";
            break;
        case WAIT_EVENT_LOCK_FILE_ADDTODATADIR_WRITE:
            event_name = "LockFileAddToDataDirWrite";
            break;
        case WAIT_EVENT_LOCK_FILE_CREATE_READ:
            event_name = "LockFileCreateRead";
            break;
        case WAIT_EVENT_LOCK_FILE_CREATE_SYNC:
            event_name = "LockFileCreateSync";
            break;
        case WAIT_EVENT_LOCK_FILE_CREATE_WRITE:
            event_name = "LockFileCreateWRITE";
            break;
        case WAIT_EVENT_RELATION_MAP_READ:
            event_name = "RelationMapRead";
            break;
        case WAIT_EVENT_RELATION_MAP_SYNC:
            event_name = "RelationMapSync";
            break;
        case WAIT_EVENT_RELATION_MAP_WRITE:
            event_name = "RelationMapWrite";
            break;
        case WAIT_EVENT_REPLICATION_SLOT_READ:
            event_name = "ReplicationSlotRead";
            break;
        case WAIT_EVENT_REPLICATION_SLOT_RESTORE_SYNC:
            event_name = "ReplicationSlotRestoreSync";
            break;
        case WAIT_EVENT_REPLICATION_SLOT_SYNC:
            event_name = "ReplicationSlotSync";
            break;
        case WAIT_EVENT_REPLICATION_SLOT_WRITE:
            event_name = "ReplicationSlotWrite";
            break;
        case WAIT_EVENT_SLRU_FLUSH_SYNC:
            event_name = "SLRUFlushSync";
            break;
        case WAIT_EVENT_SLRU_READ:
            event_name = "SLRURead";
            break;
        case WAIT_EVENT_SLRU_SYNC:
            event_name = "SLRUSync";
            break;
        case WAIT_EVENT_SLRU_WRITE:
            event_name = "SLRUWrite";
            break;
        case WAIT_EVENT_TWOPHASE_FILE_READ:
            event_name = "TwophaseFileRead";
            break;
        case WAIT_EVENT_TWOPHASE_FILE_SYNC:
            event_name = "TwophaseFileSync";
            break;
        case WAIT_EVENT_TWOPHASE_FILE_WRITE:
            event_name = "TwophaseFileWrite";
            break;
        case WAIT_EVENT_WAL_BOOTSTRAP_SYNC:
            event_name = "WALBootstrapSync";
            break;
        case WAIT_EVENT_WAL_BOOTSTRAP_WRITE:
            event_name = "WALBootstrapWrite";
            break;
        case WAIT_EVENT_WAL_COPY_READ:
            event_name = "WALCopyRead";
            break;
        case WAIT_EVENT_WAL_COPY_SYNC:
            event_name = "WALCopySync";
            break;
        case WAIT_EVENT_WAL_COPY_WRITE:
            event_name = "WALCopyWrite";
            break;
        case WAIT_EVENT_WAL_INIT_SYNC:
            event_name = "WALInitSync";
            break;
        case WAIT_EVENT_WAL_INIT_WRITE:
            event_name = "WALInitWrite";
            break;
        case WAIT_EVENT_WAL_READ:
            event_name = "WALRead";
            break;
        case WAIT_EVENT_WAL_SYNC_METHOD_ASSIGN:
            event_name = "WALSyncMethodAssign";
            break;
        case WAIT_EVENT_WAL_WRITE:
            event_name = "WALWrite";
            break;
        case WAIT_EVENT_WAL_BUFFER_FULL:
            event_name = "WALBufferFull";
            break;
        case WAIT_EVENT_WAL_BUFFER_ACCESS:
            event_name = "WALBufferAccess";
            break;
        case WAIT_EVENT_DW_READ:
            event_name = "DoubleWriteFileRead";
            break;
        case WAIT_EVENT_DW_WRITE:
            event_name = "DoubleWriteFileWrite";
            break;
        case WAIT_EVENT_DW_SINGLE_POS:
            event_name = "DWSingleFlushGetPos";
            break;
        case WAIT_EVENT_DW_SINGLE_WRITE:
            event_name = "DWSingleFlushWrite";
            break;
        case WAIT_EVENT_PREDO_PROCESS_PENDING:
            event_name = "PredoProcessPending";
            break;
        case WAIT_EVENT_PREDO_APPLY:
            event_name = "PredoApply";
            break;
        case WAIT_EVENT_DISABLE_CONNECT_FILE_READ:
            event_name = "DisableConnectFileRead";
            break;
        case WAIT_EVENT_DISABLE_CONNECT_FILE_SYNC:
            event_name = "DisableConnectFileSync";
            break;
        case WAIT_EVENT_DISABLE_CONNECT_FILE_WRITE:
            event_name = "DisableConnectFileWrite";
            break;
        case WAIT_EVENT_MPFL_INIT:
            event_name = "MPFL_INIT";
            break;
        case WAIT_EVENT_MPFL_READ:
            event_name = "MPFL_READ";
            break;
        case WAIT_EVENT_MPFL_WRITE:
            event_name = "MPFL_WRITE";
            break;
        case WAIT_EVENT_OBS_LIST:
            event_name = "OBSList";
            break;
        case WAIT_EVENT_OBS_READ:
            event_name = "OBSRead";
            break;
        case WAIT_EVENT_OBS_WRITE:
            event_name = "OBSWrite";
            break;
        case WAIT_EVENT_LOGCTRL_SLEEP:
            event_name = "LOGCTRL_SLEEP";
            break;
        case WAIT_EVENT_COMPRESS_ADDRESS_FILE_FLUSH:
            event_name = "PCA_FLUSH";
            break;
        case WAIT_EVENT_COMPRESS_ADDRESS_FILE_SYNC:
            event_name = "PCA_SYNC";
            break;
            /* no default case, so that compiler will warn */
        case IO_EVENT_NUM:
            break;
        case WAIT_EVENT_UNDO_FILE_PREFETCH:
            event_name = "UndoFilePrefetch";
            break;
        case WAIT_EVENT_UNDO_FILE_READ:
            event_name = "UndoFileRead";
            break;
        case WAIT_EVENT_UNDO_FILE_WRITE:
            event_name = "UndoFileWrite";
            break;
        case WAIT_EVENT_UNDO_FILE_SYNC:
            event_name = "UndoFileSync";
            break;
        case WAIT_EVENT_UNDO_FILE_EXTEND:
            event_name = "UndoFileExtend";
            break;
        case WAIT_EVENT_UNDO_FILE_UNLINK:
            event_name = "UndoFileUnlink";
            break;
        case WAIT_EVENT_UNDO_META_SYNC:
            event_name = "UndoMetaSync";
            break;
        default:
            event_name = "unknown wait event";
            break;
    }
    return event_name;
}

/* ----------
 * pgstat_get_current_active_numbackends() -
 *
 *  get current active count of backends
 * ----------
 */
int pgstat_get_current_active_numbackends(void)
{
    int result_counter = 0;

    // If BackendStatusArray is NULL, we will get it from other thread.
    if (t_thrd.shemem_ptr_cxt.BackendStatusArray == NULL) {
        if (NULL != PgBackendStatusArray)
            t_thrd.shemem_ptr_cxt.BackendStatusArray = PgBackendStatusArray;
        else
            return result_counter;
    }

    PgBackendStatusNode* node = gs_stat_read_current_status(NULL);

    while (node != NULL) {
        PgBackendStatus* beentry = node->data;

        /*
         * If the backend thread is valid and the state
         * is not idle or undefined, we treat it as a active thread.
         */
        if ((beentry != NULL) && beentry->st_sessionid > 0 &&
            (beentry->st_state != STATE_IDLE && beentry->st_state != STATE_UNDEFINED &&
                beentry->st_state != STATE_DECOUPLED))
            ++result_counter;
        node = node->next;
    }

    /* Make sure the active count is not beyond max connections */
    if (result_counter > g_instance.attr.attr_network.MaxConnections)
        result_counter = g_instance.attr.attr_network.MaxConnections;

    return result_counter;
}

/* get the pointer to the specified backend status */
PgBackendStatus* pgstat_get_backend_single_entry(uint64 sessionid)
{
    PgBackendStatus* beentry = NULL;
    int i = 0;

    beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + BackendStatusArray_size - 1;

    /*
     * We probably shouldn't get here before shared memory has been set up,
     * but be safe.
     */
    if (beentry == NULL)
        return NULL;

    /* We go through BackendStatusArray from back to front. */
    for (i = 1; i <= BackendStatusArray_size; i++, beentry--) {
        if (beentry->st_sessionid == sessionid)
            return beentry;
    }

    return NULL;
}

/*
 * @Description: get user backend entry
 * @IN userid: userid
 * @Return: user data list
 * @See also:
 */
List* pgstat_get_user_backend_entry(Oid userid)
{
    // If BackendStatusArray is NULL, we will get it from other thread.
    if (t_thrd.shemem_ptr_cxt.BackendStatusArray == NULL) {
        if (PgBackendStatusArray != NULL)
            t_thrd.shemem_ptr_cxt.BackendStatusArray = PgBackendStatusArray;
        else
            return NULL;
    }

    List* entry_list = NULL;
    PgBackendStatusNode* node = gs_stat_read_current_status(NULL);

    while (node != NULL) {
        PgBackendStatus* beentry = node->data;
        /*
         * If the backend thread is valid and the user id
         * matched, we treat it as a active thread.
         */
        if (beentry != NULL) {
            WLMStatistics* backstat = (WLMStatistics*)&beentry->st_backstat;

            if (beentry->st_procpid <= 0 && beentry->st_sessionid == 0) {
                node = node->next;
                continue;
            }

            if (!(backstat->qtype && strcmp(backstat->qtype, "Internal") != 0)) {
                node = node->next;
                continue;
            }

            if ((backstat->status && strcmp(backstat->status, "running") == 0) ||
                (backstat->enqueue && strcmp(backstat->enqueue, "Transaction") == 0) ||
                (backstat->enqueue && strcmp(backstat->enqueue, "StoredProc") == 0) ||
                (backstat->status && strcmp(backstat->status, "pending") == 0 && backstat->enqueue &&
                    (strcmp(backstat->enqueue, "None") == 0 || strcmp(backstat->enqueue, "Respool") == 0)) ||
                (backstat->status && strcmp(backstat->status, "finished") == 0 && backstat->enqueue &&
                    strcmp(backstat->enqueue, "Respool") == 0)) {
                entry_list = lappend(entry_list, beentry);
            }
        }

        node = node->next;
    }

    return entry_list;
}
/*
 * @Description: get user backend entry
 * @IN userid: user oid
 * @OUT num: entry count
 * @Return: all thread of the user
 * @See also:
 */
ThreadId* pgstat_get_user_io_entry(Oid userid, int* num)
{
    int idx = 0;

    // If BackendStatusArray is NULL, we will get it from other thread.
    if (t_thrd.shemem_ptr_cxt.BackendStatusArray == NULL) {
        if (NULL != PgBackendStatusArray)
            t_thrd.shemem_ptr_cxt.BackendStatusArray = PgBackendStatusArray;
        else
            return NULL;
    }

    uint32 num_backends = 0;
    PgBackendStatusNode* node = gs_stat_read_current_status(&num_backends);
    if (num_backends == 0)
        return NULL;

    Assert(!u_sess->stat_cxt.pgStatRunningInCollector);

    ThreadId* threads = (ThreadId*)palloc0_noexcept(num_backends * sizeof(ThreadId));
    if (threads == NULL) {
        pgstat_reset_current_status();
        ereport(LOG, (errmsg("alloc memory failed for get user io entry.")));
        return NULL;
    }

    while (node != NULL) {
        PgBackendStatus* beentry = node->data;

        /*
         * If the backend thread is valid and the io state
         * is writing, we treat it as a active thread.
         * For thread pool, sessions decoupled from workers are not generating data.
         * Therefore, no need to count them and send signal to them.
         */
        if ((beentry != NULL) && beentry->st_userid == userid && beentry->st_io_state == IOSTATE_WRITE)
            threads[idx++] = beentry->st_procpid;

        node = node->next;
    }

    *num = idx;

    pgstat_reset_current_status();

    return threads;
}

/*
 * @Description: get stmt tag write backend entry
 * @OUT num: entry count
 * @Return: all thread with st_stmttag as write
 * @See also:
 */
ThreadId* pgstat_get_stmttag_write_entry(int* num)
{
    int idx = 0;

    // If BackendStatusArray is NULL, we will get it from other thread.
    if (t_thrd.shemem_ptr_cxt.BackendStatusArray == NULL) {
        if (PgBackendStatusArray != NULL)
            t_thrd.shemem_ptr_cxt.BackendStatusArray = PgBackendStatusArray;
        else
            return NULL;
    }

    uint32 num_backends = 0;
    PgBackendStatusNode* node = gs_stat_read_current_status(&num_backends);
    if (num_backends == 0)
        return NULL;

    Assert(!u_sess->stat_cxt.pgStatRunningInCollector);

    ThreadId* threads = (ThreadId*)palloc0_noexcept(num_backends * sizeof(ThreadId));

    if (threads == NULL) {
        pgstat_reset_current_status();
        ereport(WARNING, (errmsg("Failed to allocate memory for get io write entry.")));
        return NULL;
    }

    while (node != NULL) {
        PgBackendStatus* beentry = node->data;
        node = node->next;
        /*
         * If the backend thread is valid and the stmt tag
         * is writing, we treat it as a active thread.
         * For thread pool, sessions decoupled from workers are not generating data.
         * Therefore, no need to count them and send signal to them.
         */
        if (beentry != NULL) {
            if (beentry->st_tid > 0 && (beentry->st_state != STATE_IDLE && beentry->st_state != STATE_UNDEFINED) &&
                beentry->st_stmttag == STMTTAG_WRITE)
                threads[idx++] = beentry->st_procpid;
            else {
                WLMStatistics* backstat = (WLMStatistics*)&beentry->st_backstat;
                if (backstat->enqueue && strcmp(backstat->enqueue, "Transaction") == 0)
                    threads[idx++] = beentry->st_procpid;
            }
        }
    }

    *num = idx;

    pgstat_reset_current_status();

    return threads;
}

/*
 * @Description: get pid from status entries by application name.
 * @IN num: application name
 * @OUT num: entries count
 * @Return: all node status which names are 'application name'
 */
PgBackendStatusNode* pgstat_get_backend_status_by_appname(const char* appName, int* resultEntryNum)
{
    int idx = 0;

    /* Initialize result number to 0, in case of return NULL directly */
    if (resultEntryNum != NULL) {
        *resultEntryNum = 0;
    }

    /* If BackendStatusArray is NULL, we will get it from other thread */
    if (t_thrd.shemem_ptr_cxt.BackendStatusArray == NULL) {
        if (PgBackendStatusArray != NULL) {
            t_thrd.shemem_ptr_cxt.BackendStatusArray = PgBackendStatusArray;
        } else {
            return NULL;
        }
    }

    /* Get all status entries, which procpid or sessionid is valid */
    uint32 numBackends = 0;
    PgBackendStatusNode* node = gs_stat_read_current_status(&numBackends);

    /* If all entries procpid or sessionid are invalid, get numBackends is 0 and should return directly */
    if (numBackends == 0) {
        FreeBackendStatusNodeMemory(node);
        return NULL;
    }

    /* This function is not under PgstatCollectorMain, this pointer is NULL */
    Assert(!u_sess->stat_cxt.pgStatRunningInCollector);

    /* Initialize head pointer, all nodes struct form to a list. This list does not match wich appname */
    PgBackendStatusNode* otherNodeList = (PgBackendStatusNode*)palloc(sizeof(PgBackendStatusNode));
    PgBackendStatusNode* otherNodeListHead = otherNodeList;
    otherNodeList->data = NULL;
    otherNodeList->next = NULL;
    /* Initialize head pointer, all nodes struct form to a list, This list matches wich appname */
    PgBackendStatusNode* resultNodeList = (PgBackendStatusNode*)palloc(sizeof(PgBackendStatusNode));
    PgBackendStatusNode* resultNodeListHead = resultNodeList;
    resultNodeList->data = NULL;
    resultNodeList->next = NULL;

    while (node != NULL) {
        PgBackendStatus* beentry = node->data;

        /* If the backend thread is valid and application name of beentry equals to appName, record this thread pid */
        if (beentry != NULL) {
            if (beentry->st_appname == NULL || beentry->st_tid < 0 || strcmp(beentry->st_appname, appName) != 0) {
                /* Node name does not match wich appname, link this pointer to otherNodeList */
                otherNodeList->next = node;
                otherNodeList = otherNodeList->next;
                node = node->next;
                continue;
            }

            /* Node name matches appname, link this pointer to resultNodeList and return */
            resultNodeList->next = node;
            resultNodeList = resultNodeList->next;
            idx++;
        } else {
            /* If beentry is NULL, should link this pointer to otherNodeList in order to free memory */
            otherNodeList->next = node;
            otherNodeList = otherNodeList->next;
        }
        node = node->next;
    }
    /* Must set tail pointer's next to NULL to separate node list to otherNodeList and resultNodeList */
    otherNodeList->next = NULL;
    resultNodeList->next = NULL;

    if (resultEntryNum != NULL) {
        *resultEntryNum = idx;
    }

    /* Because we have separate node list to other two lists, so just free the first list here. */
    FreeBackendStatusNodeMemory(otherNodeListHead);

    return resultNodeListHead;
}

/* ----------
 * pgstat_get_backend_current_activity() -
 *
 *	Return a string representing the current activity of the backend with
 *	the specified PID.	This looks directly at the BackendStatusArray,
 *	and so will provide current information regardless of the age of our
 *	transaction's snapshot of the status array.
 *
 *	It is the caller's responsibility to invoke this only for backends whose
 *	state is expected to remain stable while the result is in use.	The
 *	only current use is in deadlock reporting, where we can expect that
 *	the target backend is blocked on a lock.  (There are corner cases
 *	where the target's wait could get aborted while we are looking at it,
 *	but the very worst consequence is to return a pointer to a string
 *	that's been changed, so we won't worry too much.)
 *
 *	Note: return strings for special cases match pg_stat_get_backend_activity.
 * ----------
 */
const char* pgstat_get_backend_current_activity(ThreadId pid, bool checkUser)
{
    PgBackendStatus* beentry = NULL;
    int i;

    /* We go through BackendStatusArray from back to front. */
    beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + BackendStatusArray_size - 1;
    for (i = 1; i <= BackendStatusArray_size; i++) {
        /*
         * Although we expect the target backend's entry to be stable, that
         * doesn't imply that anyone else's is.  To avoid identifying the
         * wrong backend, while we check for a match to the desired PID we
         * must follow the protocol of retrying if st_changecount changes
         * while we examine the entry, or if it's odd.  (This might be
         * unnecessary, since fetching or storing an int is almost certainly
         * atomic, but let's play it safe.)  We use a volatile pointer here to
         * ensure the compiler doesn't try to get cute.
         */
        volatile PgBackendStatus* vbeentry = beentry;
        bool found = false;

        for (;;) {
            int before_changecount;
            int after_changecount;

            pgstat_save_changecount_before(vbeentry, before_changecount);

            found = (vbeentry->st_procpid == pid);

            pgstat_save_changecount_after(vbeentry, after_changecount);

            if (before_changecount == after_changecount && (before_changecount & 1) == 0)
                break;

            /* Make sure we can break out of loop if stuck... */
            CHECK_FOR_INTERRUPTS();
        }

        if (found) {
            /* Now it is safe to use the non-volatile pointer */
            if (checkUser && !superuser() && beentry->st_userid != GetUserId())
                return "<insufficient privilege>";
            else if (*(beentry->st_activity) == '\0')
                return "<command string not enabled>";
            else
                return beentry->st_activity;
        }

        beentry--;
    }

    /* If we get here, caller is in error ... */
    return "<backend information not available>";
}

/* ----------
 * pgstat_get_crashed_backend_activity() -
 *
 *	Return a string representing the current activity of the backend with
 *	the specified PID.	Like the function above, but reads shared memory with
 *	the expectation that it may be corrupt.  On success, copy the string
 *	into the "buffer" argument and return that pointer.  On failure,
 *	return NULL.
 *
 *	This function is only intended to be used by the postmaster to report the
 *	query that crashed a backend.  In particular, no attempt is made to
 *	follow the correct concurrency protocol when accessing the
 *	BackendStatusArray.  But that's OK, in the worst case we'll return a
 *	corrupted message.	We also must take care not to trip on ereport(ERROR).
 * ----------
 */
const char* pgstat_get_crashed_backend_activity(ThreadId pid, char* buffer, int buflen)
{
    volatile PgBackendStatus* beentry = NULL;
    int i;

    beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + BackendStatusArray_size - 1;

    /*
     * We probably shouldn't get here before shared memory has been set up,
     * but be safe.
     */
    if (beentry == NULL || t_thrd.shemem_ptr_cxt.BackendActivityBuffer == NULL)
        return NULL;

    /* We go through BackendStatusArray from back to front. */
    for (i = 1; i <= BackendStatusArray_size; i++) {
        if (beentry->st_procpid == pid) {
            /* Read pointer just once, so it can't change after validation */
            const char* activity = beentry->st_activity;
            const char* activity_last = NULL;

            /*
             * We mustn't access activity string before we verify that it
             * falls within the BackendActivityBuffer. To make sure that the
             * entire string including its ending is contained within the
             * buffer, subtract one activity length from the buffer size.
             */
            activity_last = t_thrd.shemem_ptr_cxt.BackendActivityBuffer +
                            t_thrd.shemem_ptr_cxt.BackendActivityBufferSize -
                            g_instance.attr.attr_common.pgstat_track_activity_query_size;

            if (activity < t_thrd.shemem_ptr_cxt.BackendActivityBuffer || activity > activity_last)
                return NULL;

            /* If no string available, no point in a report */
            if (activity[0] == '\0')
                return NULL;

            /*
             * Copy only ASCII-safe characters so we don't run into encoding
             * problems when reporting the message; and be sure not to run off
             * the end of memory.
             */
            ascii_safe_strlcpy(
                buffer, activity, Min(buflen, g_instance.attr.attr_common.pgstat_track_activity_query_size));

            return buffer;
        }

        beentry--;
    }

    /* PID not found */
    return NULL;
}

/*
 * Send SIGINT to those backends of which the running queries
 * were started with a GTM that has broken down or been demoted.
 *
 * At present, this function is called automatically by twophasecleaner
 * every gtm_conn_validation_interval. It can also be called manually with
 * the built-in pg_cancel_invalid_query function, only by super-user.
 */
void pgstat_cancel_invalid_gtm_conn(void)
{
    volatile PgBackendStatus* beentry = NULL;
    bool CalledByBackend = false;

    volatile GtmHostIndex hostindex = GTM_HOST_INVAILD;
    GTM_Timeline txnTimeline = InvalidTransactionTimeline;

    if (GTM_FREE_MODE)
        return;

    beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray;

    /*
     * Register the host index and the timeline of the currently
     * running GTM to MyBEEntry.
     * If no GTM is running, both fields are set to be 0.
     */
    InitGTM_Reporttimeline();

    /*
     * Compared with the newly obtained GTM host index or timeline,
     * those backends having different entries in the BackendStatusArray
     * must be connected to a demoted or broken down GTM. So
     * send SIGINT to them.
     */
    for (int i = 1; i <= BackendStatusArray_size; i++, beentry++) {
        SpinLockAcquire(&beentry->use_mutex);
        hostindex = beentry->st_gtmhost;
        txnTimeline = beentry->st_gtmtimeline;
        SpinLockRelease(&beentry->use_mutex);

        /*
         * Skip unstarted or unconnected gtm backend.
         * Under thread pool mode, no need to cancel decoupled sessions.
         */
        if (beentry->st_procpid == 0 || hostindex == GTM_HOST_INVAILD)
            continue;

        if (hostindex != t_thrd.shemem_ptr_cxt.MyBEEntry->st_gtmhost ||
            txnTimeline != t_thrd.shemem_ptr_cxt.MyBEEntry->st_gtmtimeline) {
            if (gs_signal_send(beentry->st_procpid, SIGUSR2))
                ereport(WARNING, (errmsg("could not send signal to thread %lu: %m", beentry->st_procpid)));
            else
                ereport(LOG,
                    (errmsg("Success to send SIGUSR2 to openGauss thread: %lu in "
                            "pgstat_cancel_invalid_gtm_conn",
                        beentry->st_procpid)));
        }

        if (beentry->st_procpid == t_thrd.shemem_ptr_cxt.MyBEEntry->st_procpid)
            CalledByBackend = true;
    }

    /*
     * If this function is called by a backend, leave
     * the GTM connection unclosed for future transaction.
     */
    if (!CalledByBackend)
        CloseGTM();
}

/* ------------------------------------------------------------
 * Local support functions follow
 * ------------------------------------------------------------
 */
/* ----------
 * pgstat_setheader() -
 *
 *		Set common header fields in a statistics message
 * ----------
 */
static void pgstat_setheader(PgStat_MsgHdr* hdr, StatMsgType mtype)
{
    hdr->m_type = mtype;
}

/* ----------
 * pgstat_send() -
 *
 *		Send out one statistics message to the collector
 * ----------
 */
void pgstat_send(void* msg, int len)
{
    int rc;

    if (g_instance.stat_cxt.pgStatSock == PGINVALID_SOCKET)
        return;

    ((PgStat_MsgHdr*)msg)->m_size = len;

    /* We'll retry after EINTR, but ignore all other failures */
    do {
        PGSTAT_INIT_TIME_RECORD();
        PGSTAT_START_TIME_RECORD();
        rc = send(g_instance.stat_cxt.pgStatSock, msg, len, 0);
        END_NET_SEND_INFO(rc);
    } while (rc < 0 && errno == EINTR);

#ifdef USE_ASSERT_CHECKING
    /* In debug builds, log send failures ... */
    if (rc < 0)
        elog(LOG, "could not send to statistics collector: %m");
#endif
}

/* ----------
 * pgstat_send_bgwriter() -
 *
 *		Send bgwriter statistics to the collector
 * ----------
 */
void pgstat_send_bgwriter(void)
{
    /* We assume this initializes to zeroes */
    static const PgStat_MsgBgWriter all_zeroes = {{PGSTAT_MTYPE_DUMMY}};

    /*
     * This function can be called even if nothing at all has happened. In
     * this case, avoid sending a completely empty message to the stats
     * collector.
     */
    if (memcmp(u_sess->stat_cxt.BgWriterStats, &all_zeroes, sizeof(PgStat_MsgBgWriter)) == 0)
        return;

    /*
     * Prepare and send the message
     */
    pgstat_setheader(&u_sess->stat_cxt.BgWriterStats->m_hdr, PGSTAT_MTYPE_BGWRITER);
    pgstat_send(u_sess->stat_cxt.BgWriterStats, sizeof(PgStat_MsgBgWriter));

    /*
     * Clear out the statistics buffer, so it can be re-used.
     */
    errno_t rc = memset_s(u_sess->stat_cxt.BgWriterStats, sizeof(PgStat_MsgBgWriter), 0, sizeof(PgStat_MsgBgWriter));
    securec_check(rc, "\0", "\0");
}

static void PgstatCollectThreadStatus(void)
{
    // setup a new memcontext
    pgstat_collect_thread_status_setup_memcxt();

    PgBackendStatusNode* node = gs_stat_read_current_status(NULL);
    while (node != NULL) {
        PgBackendStatus* beentry = node->data;
        node = node->next;
        char* wait_status = NULL;

        if (NULL == beentry)
            continue;

        /* No need to print 'none' or 'wait cmd' thread status to server log */
        if (STATE_WAIT_UNDEFINED == beentry->st_waitstatus || STATE_WAIT_COMM == beentry->st_waitstatus)
            continue;

        wait_status = getThreadWaitStatusDesc(beentry);

        // log out thread wait status from beentry.
        elog(LOG,
            "PgstatCollectThreadStatus, node_name<%s>, datid<%u>, app_name<%s>, "
            "query_id<%lu>, tid<%lu>, lwtid<%d>, parent_sessionid<%lu>, thread_level<%d>, wait_status<%s>",
            g_instance.attr.attr_common.PGXCNodeName,
            beentry->st_databaseid,
            beentry->st_appname ? beentry->st_appname : "unnamed thread",
            beentry->st_queryid,
            beentry->st_procpid,
            beentry->st_tid,
            beentry->st_parent_sessionid,
            beentry->st_thread_level,
            wait_status);

        pfree(wait_status);
    }

    pgstat_collect_thread_status_clear_resource();
}

/* ----------
 * PgstatCollectorMain() -
 *
 *	Start up the statistics collector process.	This is the body of the
 *	postmaster child process.
 *
 *	The argc/argv parameters are valid only in EXEC_BACKEND case.
 * ----------
 */
void PgstatCollectorMain()
{
    int len;
    PgStat_Msg msg;
    int wr;
    TimestampTz get_thread_status_start_time;

    IsUnderPostmaster = true; /* we are a postmaster subprocess now */

    t_thrd.proc_cxt.MyProcPid = gs_thread_self(); /* reset t_thrd.proc_cxt.MyProcPid */

    t_thrd.proc_cxt.MyStartTime = time(NULL); /* record Start Time for logging */

    t_thrd.proc_cxt.MyProgName = "PgstatCollector";

    t_thrd.myLogicTid = noProcLogicTid + PGSTAT_LID;

    /* Initialize private latch for use by signal handlers */
    InitLatch(&g_instance.stat_cxt.pgStatLatch);

    /*
     * Ignore all signals usually bound to some action in the postmaster,
     * except SIGHUP and SIGQUIT.  Note we don't need a SIGUSR1 handler to
     * support latch operations, because g_instance.stat_cxt.pgStatLatch is local not shared.
     */

    (void)gspqsignal(SIGHUP, pgstat_sighup_handler);
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, SIG_IGN);
    (void)gspqsignal(SIGQUIT, pgstat_exit);
    (void)gspqsignal(SIGALRM, SIG_IGN);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, SIG_IGN);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGTTIN, SIG_DFL);
    (void)gspqsignal(SIGTTOU, SIG_DFL);
    (void)gspqsignal(SIGCONT, SIG_DFL);
    (void)gspqsignal(SIGWINCH, SIG_DFL);

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
    (void)gs_signal_unblock_sigusr2();

    /*
     * Identify myself via ps
     */
    init_ps_display("stats collector process", "", "", "");

    /*
     * Arrange to write the initial status file right away
     */
    get_thread_status_start_time = g_instance.stat_cxt.last_statrequest = GetCurrentTimestamp();
    g_instance.stat_cxt.last_statwrite = g_instance.stat_cxt.last_statrequest - 1;

    /*
     * Read in an existing statistics stats file or initialize the stats to
     * zero.
     */
    u_sess->stat_cxt.pgStatRunningInCollector = true;
    u_sess->stat_cxt.pgStatDBHash = pgstat_read_statsfile(InvalidOid, true);

    t_thrd.mem_cxt.mask_password_mem_cxt = AllocSetContextCreate(t_thrd.top_mem_cxt,
        "MaskPasswordCtx",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);
    /*
     * Loop to process messages until we get SIGQUIT or detect ungraceful
     * death of our parent postmaster.
     *
     * For performance reasons, we don't want to do ResetLatch/WaitLatch after
     * every message; instead, do that only after a recv() fails to obtain a
     * message.  (This effectively means that if backends are sending us stuff
     * like mad, we won't notice postmaster death until things slack off a
     * bit; which seems fine.)	To do that, we have an inner loop that
     * iterates as long as recv() succeeds.  We do recognize got_SIGHUP inside
     * the inner loop, which means that such interrupts will get serviced but
     * the latch won't get cleared until next time there is a break in the
     * action.
     */
    for (;;) {
        TimestampTz get_thread_status_current_time = GetCurrentTimestamp();

        if (u_sess->attr.attr_common.pgstat_collect_thread_status_interval > 0 &&
            TimestampDifferenceExceeds(get_thread_status_start_time,
                get_thread_status_current_time,
                60 * u_sess->attr.attr_common.pgstat_collect_thread_status_interval * 1000)) {
            // transfer from minute into msec
            PgstatCollectThreadStatus();
            get_thread_status_start_time = GetCurrentTimestamp();
        }
        /* Clear any already-pending wakeups */
        ResetLatch(&g_instance.stat_cxt.pgStatLatch);

        /*
         * Quit if we get SIGQUIT from the postmaster.
         */
        if (t_thrd.stat_cxt.need_exit)
            break;

        /*
         * Inner loop iterates as long as we keep getting messages, or until
         * need_exit becomes set.
         */
        while (!t_thrd.stat_cxt.need_exit) {
            /*
             * Reload configuration if we got SIGHUP from the postmaster.
             */
            if (g_instance.stat_cxt.got_SIGHUP) {
                g_instance.stat_cxt.got_SIGHUP = false;
                ProcessConfigFile(PGC_SIGHUP);
            }

            /*
             * Write the stats file if a new request has arrived that is not
             * satisfied by existing file.
             */
            if (g_instance.stat_cxt.last_statwrite < g_instance.stat_cxt.last_statrequest)
                pgstat_write_statsfile(false);

            PgstatUpdateHotkeys();

            /*
             * Try to receive and process a message.  This will not block,
             * since the socket is set to non-blocking mode.
             *
             * XXX On Windows, we have to force pgwin32_recv to cooperate,
             * despite the previous use of pg_set_noblock() on the socket.
             * This is extremely broken and should be fixed someday.
             */
#ifdef WIN32
            pgwin32_noblock = 1;
#endif
            len = recv(g_instance.stat_cxt.pgStatSock, (char*)&msg, sizeof(PgStat_Msg), 0);

#ifdef WIN32
            pgwin32_noblock = 0;
#endif

            if (len < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                    break; /* out of inner loop */
                ereport(ERROR, (errcode_for_socket_access(), errmsg("could not read statistics message: %m")));
            }

            /*
             * We ignore messages that are smaller than our common header
             */
            if ((size_t)len < sizeof(PgStat_MsgHdr))
                continue;

            /*
             * The received length must match the length in the header
             */
            if (msg.msg_hdr.m_size != len)
                continue;

            /*
             * O.K. - we accept this message.  Process it.
             */
            switch (msg.msg_hdr.m_type) {
                case PGSTAT_MTYPE_DUMMY:
                    break;

                case PGSTAT_MTYPE_INQUIRY:
                    pgstat_recv_inquiry((PgStat_MsgInquiry*)&msg, len);
                    break;

                case PGSTAT_MTYPE_TABSTAT:
                    pgstat_recv_tabstat((PgStat_MsgTabstat*)&msg, len);
                    break;

                case PGSTAT_MTYPE_TABPURGE:
                    pgstat_recv_tabpurge((PgStat_MsgTabpurge*)&msg, len);
                    break;

                case PGSTAT_MTYPE_DROPDB:
                    pgstat_recv_dropdb((PgStat_MsgDropdb*)&msg, len);
                    break;

                case PGSTAT_MTYPE_RESETCOUNTER:
                    pgstat_recv_resetcounter((PgStat_MsgResetcounter*)&msg, len);
                    break;

                case PGSTAT_MTYPE_RESETSHAREDCOUNTER:
                    pgstat_recv_resetsharedcounter((PgStat_MsgResetsharedcounter*)&msg, len);
                    break;

                case PGSTAT_MTYPE_RESETSINGLECOUNTER:
                    pgstat_recv_resetsinglecounter((PgStat_MsgResetsinglecounter*)&msg, len);
                    break;

                case PGSTAT_MTYPE_AUTOVAC_START:
                    pgstat_recv_autovac((PgStat_MsgAutovacStart*)&msg, len);
                    break;

                case PGSTAT_MTYPE_VACUUM:
                    pgstat_recv_vacuum((PgStat_MsgVacuum*)&msg, len);
                    break;

                case PGSTAT_MTYPE_AUTOVAC_STAT:
                    pgstat_recv_autovac_stat((PgStat_MsgAutovacStat*)&msg, len);
                    break;

                case PGSTAT_MTYPE_DATA_CHANGED:
                    pgstat_recv_data_changed((PgStat_MsgDataChanged*)&msg, len);
                    break;

                case PGSTAT_MTYPE_TRUNCATE:
                    pgstat_recv_truncate((PgStat_MsgTruncate*)&msg, len);
                    break;

                case PGSTAT_MTYPE_ANALYZE:
                    pgstat_recv_analyze((PgStat_MsgAnalyze*)&msg, len);
                    break;

                case PGSTAT_MTYPE_BGWRITER:
                    pgstat_recv_bgwriter((PgStat_MsgBgWriter*)&msg, len);
                    break;

                case PGSTAT_MTYPE_FUNCSTAT:
                    pgstat_recv_funcstat((PgStat_MsgFuncstat*)&msg, len);
                    break;

                case PGSTAT_MTYPE_FUNCPURGE:
                    pgstat_recv_funcpurge((PgStat_MsgFuncpurge*)&msg, len);
                    break;

                case PGSTAT_MTYPE_RECOVERYCONFLICT:
                    pgstat_recv_recoveryconflict((PgStat_MsgRecoveryConflict*)&msg);
                    break;

                case PGSTAT_MTYPE_DEADLOCK:
                    pgstat_recv_deadlock((PgStat_MsgDeadlock*)&msg);
                    break;

                case PGSTAT_MTYPE_FILE:
                    pgstat_recv_filestat((PgStat_MsgFile*)&msg, len);
                    break;

                case PGSTAT_MTYPE_TEMPFILE:
                    pgstat_recv_tempfile((PgStat_MsgTempFile*)&msg, len);
                    break;

                case PGSTAT_MTYPE_MEMRESERVED:
                    pgstat_recv_memReserved((PgStat_MsgMemReserved*)&msg);
                    break;

                case PGSTAT_MTYPE_BADBLOCK:
                    pgstat_recv_badblock_stat((PgStat_MsgBadBlock*)&msg, len);
                    break;

                case PGSTAT_MTYPE_RESPONSETIME:
                    pgstat_recv_sql_responstime((PgStat_SqlRT*)&msg, len);
                    break;

                case PGSTAT_MTYPE_CLEANUPHOTKEYS:
                    pgstat_recv_cleanup_hotkeys((PgStat_MsgCleanupHotkeys*)&msg, len);
                    break;

                case PGSTAT_MTYPE_PRUNESTAT:
                    PgstatRecvPrunestat((PgStat_MsgPrune*)&msg, len);
                    break;

                default:
                    break;
            }
        } /* end of inner message-processing loop */

        /* Sleep until there's something to do */
#ifndef WIN32
        wr = WaitLatchOrSocket(&g_instance.stat_cxt.pgStatLatch,
            WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_READABLE | WL_TIMEOUT,
            g_instance.stat_cxt.pgStatSock,
            60 * 1000L /* msec */);
#else

        /*
         * Windows, at least in its Windows Server 2003 R2 incarnation,
         * sometimes loses FD_READ events.	Waking up and retrying the recv()
         * fixes that, so don't sleep indefinitely.  This is a crock of the
         * first water, but until somebody wants to debug exactly what's
         * happening there, this is the best we can do.  The two-second
         * timeout matches our pre-9.2 behavior, and needs to be short enough
         * to not provoke "using stale statistics" complaints from
         * backend_read_statsfile.
         */
        wr = WaitLatchOrSocket(&g_instance.stat_cxt.pgStatLatch,
            WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_SOCKET_READABLE | WL_TIMEOUT,
            g_instance.stat_cxt.pgStatSock,
            2 * 1000L /* msec */);
#endif

        /*
         * Emergency bailout if postmaster has died.  This is to avoid the
         * necessity for manual cleanup of all postmaster children.
         */
        if ((uint32)wr & WL_POSTMASTER_DEATH)
            break;
    } /* end of outer loop */

    /*
     * Save the final stats to reuse at next startup.
     */
    pgstat_write_statsfile(true);

    DEC_NUM_ALIVE_THREADS_WAITTED();
    gs_thread_exit(0);
}

/* SIGQUIT signal handler for collector process */
static void pgstat_exit(SIGNAL_ARGS)
{
    int save_errno = errno;

    t_thrd.stat_cxt.need_exit = true;
    SetLatch(&g_instance.stat_cxt.pgStatLatch);

    errno = save_errno;
}

/* SIGHUP handler for collector process */
static void pgstat_sighup_handler(SIGNAL_ARGS)
{
    int save_errno = errno;

    g_instance.stat_cxt.got_SIGHUP = true;
    SetLatch(&g_instance.stat_cxt.pgStatLatch);

    errno = save_errno;
}

/*
 * Lookup the hash table entry for the specified database. If no hash
 * table entry exists, initialize it, if the create parameter is true.
 * Else, return NULL.
 */
static PgStat_StatDBEntry* pgstat_get_db_entry(Oid databaseid, bool create)
{
    PgStat_StatDBEntry* result = NULL;
    bool found = false;
    HASHACTION action = (create ? HASH_ENTER : HASH_FIND);
    errno_t rc = EOK;

    /* Lookup or create the hash table entry for this database */
    result = (PgStat_StatDBEntry*)hash_search(u_sess->stat_cxt.pgStatDBHash, &databaseid, action, &found);

    if (!create && !found)
        return NULL;

    /* If not found, initialize the new one. */
    if (!found) {
        HASHCTL hash_ctl;

        result->tables = NULL;
        result->functions = NULL;
        result->n_xact_commit = 0;
        result->n_xact_rollback = 0;
        result->n_blocks_fetched = 0;
        result->n_blocks_hit = 0;
        result->n_cu_mem_hit = 0;
        result->n_cu_hdd_sync = 0;
        result->n_cu_hdd_asyn = 0;
        result->n_tuples_returned = 0;
        result->n_tuples_fetched = 0;
        result->n_tuples_inserted = 0;
        result->n_tuples_updated = 0;
        result->n_tuples_deleted = 0;
        result->last_autovac_time = 0;
        result->n_conflict_tablespace = 0;
        result->n_conflict_lock = 0;
        result->n_conflict_snapshot = 0;
        result->n_conflict_bufferpin = 0;
        result->n_conflict_startup_deadlock = 0;
        result->n_temp_files = 0;
        result->n_temp_bytes = 0;
        result->n_deadlocks = 0;
        result->n_block_read_time = 0;
        result->n_block_write_time = 0;
        result->n_mem_mbytes_reserved = 0;

        result->stat_reset_timestamp = GetCurrentTimestamp();

        rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
        securec_check(rc, "\0", "\0");
        hash_ctl.keysize = sizeof(PgStat_StatTabKey);
        hash_ctl.entrysize = sizeof(PgStat_StatTabEntry);
        hash_ctl.hash = tag_hash;
        result->tables = hash_create("Per-database table", PGSTAT_TAB_HASH_SIZE, &hash_ctl, HASH_ELEM | HASH_FUNCTION);

        hash_ctl.keysize = sizeof(Oid);
        hash_ctl.entrysize = sizeof(PgStat_StatFuncEntry);
        hash_ctl.hash = oid_hash;
        result->functions =
            hash_create("Per-database function", PGSTAT_FUNCTION_HASH_SIZE, &hash_ctl, HASH_ELEM | HASH_FUNCTION);
    }

    return result;
}

/*
 * Lookup the hash table entry for the specified table. If no hash
 * table entry exists, initialize it, if the create parameter is true.
 * Else, return NULL.
 */
static PgStat_StatTabEntry* pgstat_get_tab_entry(
    PgStat_StatDBEntry* dbentry, Oid tableoid, bool create, uint32 statFlag)
{
    PgStat_StatTabEntry* result = NULL;
    bool found = false;
    HASHACTION action = (create ? HASH_ENTER : HASH_FIND);
    PgStat_StatTabKey tabkey;

    tabkey.statFlag = statFlag;
    tabkey.tableid = tableoid;

    /* Lookup or create the hash table entry for this table */
    result = (PgStat_StatTabEntry*)hash_search(dbentry->tables, (void*)(&tabkey), action, &found);

    if (!create && !found)
        return NULL;

    /* If not found, initialize the new one. */
    if (!found) {
        result->numscans = 0;
        result->tuples_returned = 0;
        result->tuples_fetched = 0;
        result->tuples_inserted = 0;
        result->tuples_updated = 0;
        result->tuples_deleted = 0;
        result->tuples_hot_updated = 0;
        result->tuples_inplace_updated = 0;
        result->n_live_tuples = 0;
        result->n_dead_tuples = 0;
        result->changes_since_analyze = 0;
        result->blocks_fetched = 0;
        result->blocks_hit = 0;
        result->cu_mem_hit = 0;
        result->cu_hdd_sync = 0;
        result->cu_hdd_asyn = 0;
        result->vacuum_timestamp = 0;
        result->vacuum_count = 0;
        result->autovac_vacuum_timestamp = 0;
        result->autovac_vacuum_count = 0;
        result->analyze_timestamp = 0;
        result->analyze_count = 0;
        result->autovac_analyze_timestamp = 0;
        result->autovac_analyze_count = 0;
        result->autovac_status = 0;
        result->data_changed_timestamp = 0;
        result->success_prune_cnt = 0;
        result->total_prune_cnt = 0;
    }

    return result;
}

/* ----------
 * pgstat_write_statsfile() -
 *
 *	Tell the news.
 *	If writing to the permanent file (happens when the collector is
 *	shutting down only), remove the temporary file so that backends
 *	starting up under a new postmaster can't read the old data before
 *	the new collector is ready.
 * ----------
 */
static void pgstat_write_statsfile(bool permanent)
{
    HASH_SEQ_STATUS hstat;
    HASH_SEQ_STATUS tstat;
    HASH_SEQ_STATUS fstat;
    PgStat_StatDBEntry* dbentry = NULL;
    PgStat_StatTabEntry* tabentry = NULL;
    PgStat_StatFuncEntry* funcentry = NULL;
    FILE* fpout = NULL;
    int32 format_id;
    const char* tmpfile = permanent ? PGSTAT_STAT_PERMANENT_TMPFILE : u_sess->stat_cxt.pgstat_stat_tmpname;
    const char* statfile = permanent ? PGSTAT_STAT_PERMANENT_FILENAME : u_sess->stat_cxt.pgstat_stat_filename;
    int rc;

    /*
     * Open the statistics temp file to write out the current values.
     */
    fpout = AllocateFile(tmpfile, PG_BINARY_W);
    if (fpout == NULL) {
        ereport(
            LOG, (errcode_for_file_access(), errmsg("could not open temporary statistics file \"%s\": %m", tmpfile)));
        return;
    }

    /*
     * Set the timestamp of the stats file.
     */
    u_sess->stat_cxt.globalStats->stats_timestamp = GetCurrentTimestamp();

    /*
     * Write the file header --- currently just a format ID.
     */
    format_id = PGSTAT_FILE_FORMAT_ID;
    rc = fwrite(&format_id, sizeof(format_id), 1, fpout);
    (void)rc; /* we'll check for error with ferror */

    /*
     * Write global stats struct
     */
    rc = fwrite(u_sess->stat_cxt.globalStats, sizeof(PgStat_GlobalStats), 1, fpout);
    (void)rc; /* we'll check for error with ferror */

    /*
     * Walk through the database table.
     */
    hash_seq_init(&hstat, u_sess->stat_cxt.pgStatDBHash);
    while ((dbentry = (PgStat_StatDBEntry*)hash_seq_search(&hstat)) != NULL) {
        /*
         * Write out the DB entry including the number of live backends. We
         * don't write the tables or functions pointers, since they're of no
         * use to any other process.
         */
        fputc('D', fpout);
        rc = fwrite(dbentry, offsetof(PgStat_StatDBEntry, tables), 1, fpout);
        (void)rc; /* we'll check for error with ferror */

        /*
         * Walk through the database's access stats per table.
         */
        hash_seq_init(&tstat, dbentry->tables);
        while ((tabentry = (PgStat_StatTabEntry*)hash_seq_search(&tstat)) != NULL) {
            fputc('T', fpout);
            rc = fwrite(tabentry, sizeof(PgStat_StatTabEntry), 1, fpout);
            (void)rc; /* we'll check for error with ferror */
        }

        /*
         * Walk through the database's function stats table.
         */
        hash_seq_init(&fstat, dbentry->functions);
        while ((funcentry = (PgStat_StatFuncEntry*)hash_seq_search(&fstat)) != NULL) {
            fputc('F', fpout);
            rc = fwrite(funcentry, sizeof(PgStat_StatFuncEntry), 1, fpout);
            (void)rc; /* we'll check for error with ferror */
        }

        /*
         * Mark the end of this DB
         */
        fputc('d', fpout);
    }

    if (g_instance.repair_cxt.global_repair_bad_block_stat != NULL) {
        LWLockAcquire(RepairBadBlockStatHashLock, LW_SHARED);
        HASH_SEQ_STATUS repairStat;
        BadBlockEntry* repairEntry;
        // write the bad page information recorded in global_repair_bad_block_stat.
        hash_seq_init(&repairStat, g_instance.repair_cxt.global_repair_bad_block_stat);
        while ((repairEntry = (BadBlockEntry*)hash_seq_search(&repairStat)) != NULL) {
            fputc('R', fpout);
            rc = fwrite(repairEntry, sizeof(BadBlockEntry), 1, fpout);
            (void)rc; /* we'll check for error with ferror */
        }
        LWLockRelease(RepairBadBlockStatHashLock);
        // Use 'r' as the terminator
        fputc('r', fpout);
    }

    /*
     * No more output to be done. Close the temp file and replace the old
     * pgstat.stat with it.  The ferror() check replaces testing for error
     * after each individual fputc or fwrite above.
     */
    fputc('E', fpout);

    if (ferror(fpout)) {
        ereport(
            LOG, (errcode_for_file_access(), errmsg("could not write temporary statistics file \"%s\": %m", tmpfile)));
        FreeFile(fpout);
        unlink(tmpfile);
    } else if (FreeFile(fpout) < 0) {
        ereport(
            LOG, (errcode_for_file_access(), errmsg("could not close temporary statistics file \"%s\": %m", tmpfile)));
        unlink(tmpfile);
    } else if (rename(tmpfile, statfile) < 0) {
        ereport(LOG,
            (errcode_for_file_access(),
                errmsg("could not rename temporary statistics file \"%s\" to \"%s\": %m", tmpfile, statfile)));
        unlink(tmpfile);
    } else {
        /*
         * Successful write, so update g_instance.stat_cxt.last_statwrite.
         */
        g_instance.stat_cxt.last_statwrite = u_sess->stat_cxt.globalStats->stats_timestamp;

        /*
         * If there is clock skew between backends and the collector, we could
         * receive a stats request time that's in the future.  If so, complain
         * and reset g_instance.stat_cxt.last_statrequest.	Resetting ensures that no inquiry
         * message can cause more than one stats file write to occur.
         */
        if (g_instance.stat_cxt.last_statrequest > g_instance.stat_cxt.last_statwrite) {
            char* reqtime = NULL;
            char* mytime = NULL;

            /* Copy because timestamptz_to_str returns a static buffer */
            reqtime = pstrdup(timestamptz_to_str(g_instance.stat_cxt.last_statrequest));
            mytime = pstrdup(timestamptz_to_str(g_instance.stat_cxt.last_statwrite));
            elog(LOG, "g_instance.stat_cxt.last_statrequest %s is later than collector's time %s", reqtime, mytime);
            pfree(reqtime);
            pfree(mytime);

            g_instance.stat_cxt.last_statrequest = g_instance.stat_cxt.last_statwrite;
        }
    }

    if (permanent)
        unlink(u_sess->stat_cxt.pgstat_stat_filename);
}

/* ----------
 * pgstat_read_statsfile() -
 *
 *	Reads in an existing statistics collector file and initializes the
 *	databases' hash table (whose entries point to the tables' hash tables).
 * ----------
 */
static HTAB* pgstat_read_statsfile(Oid onlydb, bool permanent)
{
    PgStat_StatDBEntry* dbentry = NULL;
    PgStat_StatDBEntry dbbuf;
    PgStat_StatTabEntry* tabentry = NULL;
    PgStat_StatTabEntry tabbuf;
    PgStat_StatFuncEntry funcbuf;
    PgStat_StatFuncEntry* funcentry = NULL;
    BadBlockEntry* repairEntry = NULL;
    BadBlockEntry repairBuf;
    HASHCTL hash_ctl;
    HTAB* dbhash = NULL;
    HTAB* tabhash = NULL;
    HTAB* funchash = NULL;
    FILE* fpin = NULL;
    int32 format_id;
    bool found = false;
    const char* statfile = permanent ? PGSTAT_STAT_PERMANENT_FILENAME : u_sess->stat_cxt.pgstat_stat_filename;
    errno_t rc = EOK;

    /*
     * The tables will live in u_sess->stat_cxt.pgStatLocalContext.
     */
    pgstat_setup_memcxt();

    /*
     * Create the DB hashtable
     */
    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(PgStat_StatDBEntry);
    hash_ctl.hash = oid_hash;
    hash_ctl.hcxt = u_sess->stat_cxt.pgStatLocalContext;
    dbhash = hash_create("Databases hash", PGSTAT_DB_HASH_SIZE, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    /*
     * Clear out global statistics so they start from zero in case we can't
     * load an existing statsfile.
     */
    rc = memset_s(u_sess->stat_cxt.globalStats, sizeof(PgStat_GlobalStats), 0, sizeof(PgStat_GlobalStats));
    securec_check(rc, "\0", "\0");

    /*
     * Set the current timestamp (will be kept only in case we can't load an
     * existing statsfile.
     */
    u_sess->stat_cxt.globalStats->stat_reset_timestamp = GetCurrentTimestamp();

    /*
     * Try to open the status file. If it doesn't exist, the backends simply
     * return zero for anything and the collector simply starts from scratch
     * with empty counters.
     *
     * ENOENT is a possibility if the stats collector is not running or has
     * not yet written the stats file the first time.  Any other failure
     * condition is suspicious.
     */
    if ((fpin = AllocateFile(statfile, PG_BINARY_R)) == NULL) {
        if (errno != ENOENT)
            ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                (errcode_for_file_access(), errmsg("could not open statistics file \"%s\": %m", statfile)));
        elog(LOG, "[Pgstat] statfile %s is missing, using empty dbhash.", statfile);
        return dbhash;
    }

    /*
     * Verify it's of the expected format.
     */
    if (fread(&format_id, 1, sizeof(format_id), fpin) != sizeof(format_id) || format_id != PGSTAT_FILE_FORMAT_ID) {
        ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
            (errmsg("corrupted statistics file \"%s\"", statfile)));
        goto done;
    }

    /*
     * Read global stats struct
     */
    if (fread(u_sess->stat_cxt.globalStats, 1, sizeof(PgStat_GlobalStats), fpin) != sizeof(PgStat_GlobalStats)) {
        ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
            (errmsg("corrupted statistics file \"%s\"", statfile)));
        goto done;
    }

    /*
     * We found an existing collector stats file. Read it and put all the
     * hashtable entries into place.
     */
    for (;;) {
        switch (fgetc(fpin)) {
                /*
                 * 'D'	A PgStat_StatDBEntry struct describing a database
                 * follows. Subsequently, zero to many 'T' and 'F' entries
                 * will follow until a 'd' is encountered.
                 */
            case 'D':
                if (fread(&dbbuf, 1, offsetof(PgStat_StatDBEntry, tables), fpin) !=
                    offsetof(PgStat_StatDBEntry, tables)) {
                    ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                        (errmsg("corrupted statistics file \"%s\"", statfile)));
                    goto done;
                }

                /*
                 * Add to the DB hash
                 */
                dbentry = (PgStat_StatDBEntry*)hash_search(dbhash, (void*)&dbbuf.databaseid, HASH_ENTER, &found);
                if (found) {
                    ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                        (errmsg("corrupted statistics file \"%s\"", statfile)));
                    goto done;
                }

                rc = memcpy_s(dbentry, sizeof(PgStat_StatDBEntry), &dbbuf, sizeof(PgStat_StatDBEntry));
                securec_check(rc, "", "");
                dbentry->tables = NULL;
                dbentry->functions = NULL;

                /*
                 * Don't collect tables if not the requested DB (or the
                 * shared-table info)
                 */
                if (onlydb != InvalidOid) {
                    if (dbbuf.databaseid != onlydb && dbbuf.databaseid != InvalidOid)
                        break;
                }

                rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
                securec_check(rc, "\0", "\0");
                hash_ctl.keysize = sizeof(PgStat_StatTabKey);
                hash_ctl.entrysize = sizeof(PgStat_StatTabEntry);
                hash_ctl.hash = tag_hash;
                hash_ctl.hcxt = u_sess->stat_cxt.pgStatLocalContext;
                dbentry->tables = hash_create(
                    "Per-database table", PGSTAT_TAB_HASH_SIZE, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

                hash_ctl.keysize = sizeof(Oid);
                hash_ctl.entrysize = sizeof(PgStat_StatFuncEntry);
                hash_ctl.hash = oid_hash;
                hash_ctl.hcxt = u_sess->stat_cxt.pgStatLocalContext;
                dbentry->functions = hash_create("Per-database function",
                    PGSTAT_FUNCTION_HASH_SIZE,
                    &hash_ctl,
                    HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

                /*
                 * Arrange that following records add entries to this
                 * database's hash tables.
                 */
                tabhash = dbentry->tables;
                funchash = dbentry->functions;
                break;

                /*
                 * 'd'	End of this database.
                 */
            case 'd':
                tabhash = NULL;
                funchash = NULL;
                break;

                /*
                 * 'T'	A PgStat_StatTabEntry follows.
                 */
            case 'T':
                if (fread(&tabbuf, 1, sizeof(PgStat_StatTabEntry), fpin) != sizeof(PgStat_StatTabEntry)) {
                    ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                        (errmsg("corrupted statistics file \"%s\"", statfile)));
                    goto done;
                }

                /*
                 * Skip if table belongs to a not requested database.
                 */
                if (tabhash == NULL)
                    break;

                tabentry = (PgStat_StatTabEntry*)hash_search(tabhash, (void*)&(tabbuf.tablekey), HASH_ENTER, &found);

                if (found) {
                    ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                        (errmsg("corrupted statistics file \"%s\"", statfile)));
                    goto done;
                }

                rc = memcpy_s(tabentry, sizeof(PgStat_StatTabEntry), &tabbuf, sizeof(tabbuf));
                securec_check(rc, "", "");
                break;

                /*
                 * 'F'	A PgStat_StatFuncEntry follows.
                 */
            case 'F':
                if (fread(&funcbuf, 1, sizeof(PgStat_StatFuncEntry), fpin) != sizeof(PgStat_StatFuncEntry)) {
                    ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                        (errmsg("corrupted statistics file \"%s\"", statfile)));
                    goto done;
                }

                /*
                 * Skip if function belongs to a not requested database.
                 */
                if (funchash == NULL)
                    break;

                funcentry =
                    (PgStat_StatFuncEntry*)hash_search(funchash, (void*)&funcbuf.functionid, HASH_ENTER, &found);

                if (found) {
                    ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                        (errmsg("corrupted statistics file \"%s\"", statfile)));
                    goto done;
                }

                rc = memcpy_s(funcentry, sizeof(PgStat_StatFuncEntry), &funcbuf, sizeof(funcbuf));
                securec_check(rc, "", "");
                break;
            case 'R':

                if (g_instance.repair_cxt.global_repair_bad_block_stat == NULL) {
                    break;
                }

                if (fread(&repairBuf, 1, sizeof(BadBlockEntry), fpin) != sizeof(BadBlockEntry)) {
                    ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                            (errmsg("corrupted statistics file \"%s\"", statfile)));
                    goto done;
                }
                /*
                 * Add to the DB hash
                 */
                repairEntry = (BadBlockEntry*)hash_search(g_instance.repair_cxt.global_repair_bad_block_stat,
                    (void*)&(repairBuf.key), HASH_ENTER, &found);
                if (found) {
                    ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                            (errmsg("corrupted statistics file \"%s\"", statfile)));
                }

                rc = memcpy_s(repairEntry, sizeof(BadBlockEntry), &repairBuf, sizeof(BadBlockEntry));
                securec_check(rc, "", "");

                break;

            case 'r':
                break;

                /*
                 * 'E'	The EOF marker of a complete stats file.
                 */
            case 'E':
                goto done;

            default:
                ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                    (errmsg("corrupted statistics file \"%s\"", statfile)));
                goto done;
        }
    }

done:
    (void)FreeFile(fpin);

    if (permanent)
        unlink(PGSTAT_STAT_PERMANENT_FILENAME);

    return dbhash;
}

/* ----------
 * pgstat_read_statsfile_timestamp() -
 *
 *	Attempt to fetch the timestamp of an existing stats file.
 *	Returns TRUE if successful (timestamp is stored at *ts).
 * ----------
 */
static bool pgstat_read_statsfile_timestamp(bool permanent, TimestampTz* ts)
{
    PgStat_GlobalStats myGlobalStats;
    FILE* fpin = NULL;
    int32 format_id;
    const char* statfile = permanent ? PGSTAT_STAT_PERMANENT_FILENAME : u_sess->stat_cxt.pgstat_stat_filename;

    /*
     * Try to open the status file.  As above, anything but ENOENT is worthy
     * of complaining about.
     */
    if ((fpin = AllocateFile(statfile, PG_BINARY_R)) == NULL) {
        if (errno != ENOENT)
            ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                (errcode_for_file_access(), errmsg("could not open statistics file \"%s\": %m", statfile)));
        return false;
    }

    /*
     * Verify it's of the expected format.
     */
    if (fread(&format_id, 1, sizeof(format_id), fpin) != sizeof(format_id) || format_id != PGSTAT_FILE_FORMAT_ID) {
        ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
            (errmsg("corrupted statistics file \"%s\"", statfile)));
        (void)FreeFile(fpin);
        return false;
    }

    /*
     * Read global stats struct
     */
    if (fread(&myGlobalStats, 1, sizeof(myGlobalStats), fpin) != sizeof(myGlobalStats)) {
        ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
            (errmsg("corrupted statistics file \"%s\"", statfile)));
        (void)FreeFile(fpin);
        return false;
    }

    *ts = myGlobalStats.stats_timestamp;

    (void)FreeFile(fpin);
    return true;
}

/*
 * If not already done, read the statistics collector stats file into
 * some hash tables.  The results will be kept until pgstat_clear_snapshot()
 * is called (typically, at end of transaction).
 */
static void backend_read_statsfile(void)
{
    TimestampTz cur_ts;
    TimestampTz min_ts;
    int count;

    /* already read it? */
    if (u_sess->stat_cxt.pgStatDBHash)
        return;
    Assert(!u_sess->stat_cxt.pgStatRunningInCollector);

    /*
     * We set the minimum acceptable timestamp to PGSTAT_STAT_INTERVAL msec
     * before now.	This indirectly ensures that the collector needn't write
     * the file more often than PGSTAT_STAT_INTERVAL.  In an autovacuum
     * worker, however, we want a lower delay to avoid using stale data, so we
     * use PGSTAT_RETRY_DELAY (since the number of worker is low, this
     * shouldn't be a problem).
     *
     * Note that we don't recompute min_ts after sleeping; so we might end up
     * accepting a file a bit older than PGSTAT_STAT_INTERVAL.	In practice
     * that shouldn't happen, though, as long as the sleep time is less than
     * PGSTAT_STAT_INTERVAL; and we don't want to lie to the collector about
     * what our cutoff time really is.
     */
    cur_ts = GetCurrentTimestamp();
    if (IsAutoVacuumWorkerProcess())
        min_ts = TimestampTzPlusMilliseconds(cur_ts, -PGSTAT_RETRY_DELAY);
    else
        min_ts = TimestampTzPlusMilliseconds(cur_ts, -PGSTAT_STAT_INTERVAL);

    /*
     * Loop until fresh enough stats file is available or we ran out of time.
     * The stats inquiry message is sent repeatedly in case collector drops
     * it; but not every single time, as that just swamps the collector.
     */
    for (count = 0; count < PGSTAT_POLL_LOOP_COUNT; count++) {
        TimestampTz file_ts = 0;

        CHECK_FOR_INTERRUPTS();

        if (pgstat_read_statsfile_timestamp(false, &file_ts) && file_ts >= min_ts)
            break;
        if (g_instance.pid_cxt.PgStatPID == 0) {
            ereport(LOG, (errmsg("statistics collector process is not exists using stale statistics")));
            break;
        }
        /* Not there or too old, so kick the collector and wait a bit */
        if ((count % PGSTAT_INQ_LOOP_COUNT) == 0)
            pgstat_send_inquiry(min_ts);

        pg_usleep(PGSTAT_RETRY_DELAY * 1000L);
    }

    if (count >= PGSTAT_POLL_LOOP_COUNT)
        ereport(LOG,
            (errmsg("using stale statistics instead of current ones "
                    "because stats collector is not responding")));

    /* Autovacuum launcher and the global stats tracker want stats about all databases */
    if (IsAutoVacuumLauncherProcess() || IsGlobalStatsTrackerProcess())
        u_sess->stat_cxt.pgStatDBHash = pgstat_read_statsfile(InvalidOid, false);
    else
        u_sess->stat_cxt.pgStatDBHash = pgstat_read_statsfile(u_sess->proc_cxt.MyDatabaseId, false);
}

void pgstat_read_analyzed()
{
    /* Similar to pgstat_read_statsfile. */
    FILE* fpin = NULL;
    int32 format_id;
    PgStat_StatDBEntry dbbuf;
    PgStat_StatTabEntry tabbuf;
    PgStat_StatFuncEntry funcbuf;
    HASHCTL hash_ctl;
    errno_t errorno = EOK;
    PgStat_AnaCheckEntry* tabentry = NULL;
    const char* statfile = u_sess->stat_cxt.pgstat_stat_filename;
    bool need_collected = false;
    bool found = false;

    /* Similar to backend_read_statsfile */
    TimestampTz cur_ts;
    TimestampTz min_ts;
    int count;

    Assert(!u_sess->stat_cxt.pgStatRunningInCollector);

    /* Loop until fresh enough stats file is available or we ran out of time. */
    cur_ts = GetCurrentTimestamp();
    min_ts = TimestampTzPlusMilliseconds(cur_ts, -PGSTAT_STAT_INTERVAL);
    for (count = 0; count < PGSTAT_POLL_LOOP_COUNT; count++) {
        TimestampTz file_ts = 0;

        CHECK_FOR_INTERRUPTS();

        if (pgstat_read_statsfile_timestamp(false, &file_ts) && file_ts >= min_ts)
            break;

        /* Not there or too old, so kick the collector and wait a bit */
        if ((count % PGSTAT_INQ_LOOP_COUNT) == 0)
            pgstat_send_inquiry(min_ts);

        pg_usleep(PGSTAT_RETRY_DELAY * 1000L);
    }

    if (count >= PGSTAT_POLL_LOOP_COUNT)
        ereport(LOG,
            (errmsg("using stale statistics instead of current ones "
                    "because stats collector is not responding")));

    /*
     * Similar to pgstat_read_statsfile.
     * The hash table will live in u_sess->stat_cxt.pgStatLocalContext
     */
    pgstat_setup_memcxt();

    errorno = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(errorno, "\0", "\0");
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(PgStat_AnaCheckEntry);
    hash_ctl.hash = oid_hash;
    hash_ctl.hcxt = u_sess->stat_cxt.pgStatLocalContext;
    u_sess->stat_cxt.analyzeCheckHash =
        hash_create("AnalyzeCheck hash", PGSTAT_TAB_HASH_SIZE, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    errorno = memset_s(u_sess->stat_cxt.globalStats, sizeof(PgStat_GlobalStats), 0, sizeof(PgStat_GlobalStats));
    securec_check(errorno, "\0", "\0");
    u_sess->stat_cxt.globalStats->stat_reset_timestamp = GetCurrentTimestamp();

    /* 1.Try to open the status file */
    if ((fpin = AllocateFile(statfile, PG_BINARY_R)) == NULL) {
        if (errno != ENOENT)
            ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                (errcode_for_file_access(), errmsg("could not open statistics file \"%s\": %m", statfile)));
        elog(LOG, "[Pgstat] statfile %s is missing, using empty dbhash.", statfile);
        return;
    }

    /* 2.Verify it's of the expected format */
    if (fread(&format_id, 1, sizeof(format_id), fpin) != sizeof(format_id) || format_id != PGSTAT_FILE_FORMAT_ID) {
        ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
            (errmsg("corrupted statistics file \"%s\"", statfile)));
        goto done;
    }

    /* 3.Read global stats struct */
    if (fread(u_sess->stat_cxt.globalStats, 1, sizeof(PgStat_GlobalStats), fpin) != sizeof(PgStat_GlobalStats)) {
        ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
            (errmsg("corrupted statistics file \"%s\"", statfile)));
        goto done;
    }

    /* 4.Fill analyzeCheckHash */
    for (;;) {
        switch (fgetc(fpin)) {
            /*
             * Database begins. Subsequently, zero to many 'T' and 'F' entries
             * will follow until a 'd' is encountered.
             */
            case 'D':
                if (fread(&dbbuf, 1, offsetof(PgStat_StatDBEntry, tables), fpin) !=
                    offsetof(PgStat_StatDBEntry, tables)) {
                    ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                        (errmsg("corrupted statistics file \"%s\"", statfile)));
                    goto done;
                }

                /* Don't collect tables if not the requested DB */
                if (dbbuf.databaseid != u_sess->proc_cxt.MyDatabaseId)
                    need_collected = false;
                else
                    need_collected = true;

                break;

            /* Database ends */
            case 'd':
                need_collected = false;
                break;

            /* Table begins */
            case 'T':
                if (fread(&tabbuf, 1, sizeof(PgStat_StatTabEntry), fpin) != sizeof(PgStat_StatTabEntry)) {
                    ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                        (errmsg("corrupted statistics file \"%s\"", statfile)));
                    goto done;
                }

                /* Skip if table is not required */
                if (!need_collected || tabbuf.tablekey.statFlag != STATFLG_RELATION)
                    break;

                tabentry = (PgStat_AnaCheckEntry*)hash_search(
                    u_sess->stat_cxt.analyzeCheckHash, (void*)&(tabbuf.tablekey.tableid), HASH_ENTER, &found);
                if (found) {
                    ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                        (errmsg("corrupted statistics file \"%s\"", statfile)));
                    goto done;
                }

                if (tabbuf.analyze_timestamp != 0)
                    tabentry->is_analyzed = true;
                else
                    tabentry->is_analyzed = false;

                break;

            /* Function begins */
            case 'F':
                if (fread(&funcbuf, 1, sizeof(PgStat_StatFuncEntry), fpin) != sizeof(PgStat_StatFuncEntry)) {
                    ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                        (errmsg("corrupted statistics file \"%s\"", statfile)));
                    goto done;
                }

                break;

            /* The EOF marker of a complete stats file */
            case 'E':
                goto done;

            default:
                ereport(u_sess->stat_cxt.pgStatRunningInCollector ? LOG : WARNING,
                    (errmsg("corrupted statistics file \"%s\"", statfile)));
                goto done;
        }
    }

done:
    (void)FreeFile(fpin);
}

/* ----------
 * pgstat_setup_memcxt() -
 *
 *	Create u_sess->stat_cxt.pgStatLocalContext, if not already done.
 * ----------
 */
void pgstat_setup_memcxt(void)
{
    if (u_sess->stat_cxt.pgStatLocalContext == NULL)
        u_sess->stat_cxt.pgStatLocalContext = AllocSetContextCreate(u_sess->top_mem_cxt,
            "Statistics snapshot",
            ALLOCSET_SMALL_MINSIZE,
            ALLOCSET_SMALL_INITSIZE,
            ALLOCSET_SMALL_MAXSIZE);
}

void pgstat_clean_memcxt(void)
{
    if (u_sess->stat_cxt.pgStatLocalContext != NULL) {
        MemoryContextDelete(u_sess->stat_cxt.pgStatLocalContext);
        u_sess->stat_cxt.pgStatLocalContext = NULL;
    }
}

static void pgstat_collect_thread_status_setup_memcxt(void)
{
    Assert(u_sess->stat_cxt.pgStatLocalContext);
    Assert(!u_sess->stat_cxt.pgStatCollectThdStatusContext);

    u_sess->stat_cxt.pgStatCollectThdStatusContext = AllocSetContextCreate(u_sess->stat_cxt.pgStatLocalContext,
        "PgStatCollectThdStatus",
        ALLOCSET_SMALL_MINSIZE,
        ALLOCSET_SMALL_INITSIZE,
        ALLOCSET_SMALL_MAXSIZE);
}

static void pgstat_collect_thread_status_clear_resource(void)
{
    MemoryContextDelete(u_sess->stat_cxt.pgStatCollectThdStatusContext);

    /* Reset variables */
    u_sess->stat_cxt.pgStatCollectThdStatusContext = NULL;
    u_sess->stat_cxt.localBackendStatusTable = NULL;
    u_sess->stat_cxt.localNumBackends = 0;
}

/* ----------
 * pgstat_clear_snapshot() -
 *
 *	Discard any data collected in the current transaction.	Any subsequent
 *	request will cause new snapshots to be read.
 *
 *	This is also invoked during transaction commit or abort to discard
 *	the no-longer-wanted snapshot.
 * ----------
 */
void pgstat_clear_snapshot(void)
{
    /* Release memory, if any was allocated */
    if (u_sess->stat_cxt.pgStatLocalContext)
        MemoryContextDelete(u_sess->stat_cxt.pgStatLocalContext);

    /* Reset variables */
    u_sess->stat_cxt.pgStatLocalContext = NULL;
    u_sess->stat_cxt.pgStatDBHash = NULL;
    u_sess->stat_cxt.localBackendStatusTable = NULL;
    u_sess->stat_cxt.localNumBackends = 0;
    u_sess->stat_cxt.analyzeCheckHash = NULL;
}

/* ----------
 * pgstat_recv_inquiry() -
 *
 *	Process stat inquiry requests.
 * ----------
 */
static void pgstat_recv_inquiry(PgStat_MsgInquiry* msg, int len)
{
    if (msg->inquiry_time > g_instance.stat_cxt.last_statrequest)
        g_instance.stat_cxt.last_statrequest = msg->inquiry_time;
}

static void inline init_tabentry_truncate(PgStat_StatTabEntry* tabentry, PgStat_TableEntry* tabmsg)
{
    tabentry->n_live_tuples = 0;
    tabentry->n_dead_tuples = 0;
}

static void overflow_check(int64 stat_value, int64 add_value)
{
    if (stat_value > LONG_LONG_MAX - add_value) {
        ereport(ERROR, (errmsg("Integer overflow when update database-wid stats")));
    }
}

/* ----------
 * pgstat_recv_tabstat() -
 *
 *	Count what the backend has done.
 * ----------
 */
static void pgstat_recv_tabstat(PgStat_MsgTabstat* msg, int len)
{
    PgStat_StatDBEntry* dbentry = NULL;
    PgStat_StatTabEntry* tabentry = NULL;
    int i;
    bool found = false;

    dbentry = pgstat_get_db_entry(msg->m_databaseid, true);
    if (dbentry == NULL) {
        ereport(ERROR, (errmsg("Failed to get database-wide stats.")));
    }

    /*
     * Check and Update database-wide stats.
     */
    overflow_check(dbentry->n_xact_commit, (PgStat_Counter)(msg->m_xact_commit));
    overflow_check(dbentry->n_xact_rollback, (PgStat_Counter)(msg->m_xact_rollback));
    overflow_check(dbentry->n_block_read_time, msg->m_block_read_time);
    overflow_check(dbentry->n_block_write_time, msg->m_block_write_time);

    dbentry->n_xact_commit += (PgStat_Counter)(msg->m_xact_commit);
    dbentry->n_xact_rollback += (PgStat_Counter)(msg->m_xact_rollback);
    dbentry->n_block_read_time += msg->m_block_read_time;
    dbentry->n_block_write_time += msg->m_block_write_time;

    /*
     * Process all table entries in the message.
     */
    for (i = 0; i < msg->m_nentries; i++) {
        PgStat_TableEntry* tabmsg = &(msg->m_entry[i]);
        PgStat_StatTabKey tabkey;

        tabkey.statFlag = tabmsg->t_statFlag;
        tabkey.tableid = tabmsg->t_id;

        tabentry = (PgStat_StatTabEntry*)hash_search(dbentry->tables, (void*)(&tabkey), HASH_ENTER, &found);

        if (!found) {
            /*
             * If it's a new table entry, initialize counters to the values we
             * just got.
             */
            tabentry->numscans = tabmsg->t_counts.t_numscans;
            tabentry->tuples_returned = tabmsg->t_counts.t_tuples_returned;
            tabentry->tuples_fetched = tabmsg->t_counts.t_tuples_fetched;
            tabentry->tuples_inserted = tabmsg->t_counts.t_tuples_inserted;
            tabentry->tuples_updated = tabmsg->t_counts.t_tuples_updated;
            tabentry->tuples_deleted = tabmsg->t_counts.t_tuples_deleted;
            tabentry->tuples_inplace_updated = tabmsg->t_counts.t_tuples_inplace_updated;
            tabentry->tuples_hot_updated = tabmsg->t_counts.t_tuples_hot_updated;
            if (tabmsg->t_counts.t_truncated) {
                tabentry->n_live_tuples = 0;
                tabentry->n_dead_tuples = 0;
            }
            tabentry->n_live_tuples = tabmsg->t_counts.t_delta_live_tuples;
            tabentry->n_dead_tuples = tabmsg->t_counts.t_delta_dead_tuples;
            tabentry->changes_since_analyze = tabmsg->t_counts.t_changed_tuples;
            tabentry->blocks_fetched = tabmsg->t_counts.t_blocks_fetched;
            tabentry->blocks_hit = tabmsg->t_counts.t_blocks_hit;
            tabentry->cu_mem_hit = tabmsg->t_counts.t_cu_mem_hit;
            tabentry->cu_hdd_sync = tabmsg->t_counts.t_cu_hdd_sync;
            tabentry->cu_hdd_asyn = tabmsg->t_counts.t_cu_hdd_asyn;

            tabentry->vacuum_timestamp = 0;
            tabentry->vacuum_count = 0;
            tabentry->autovac_vacuum_timestamp = 0;
            tabentry->autovac_vacuum_count = 0;
            tabentry->analyze_timestamp = 0;
            tabentry->analyze_count = 0;
            tabentry->autovac_analyze_timestamp = 0;
            tabentry->autovac_analyze_count = 0;
            tabentry->autovac_status = 0;
            tabentry->data_changed_timestamp = 0;
            tabentry->success_prune_cnt = 0;
            tabentry->total_prune_cnt = 0;
        } else {
            /*
             * Otherwise add the values to the existing entry.
             */
            tabentry->numscans += tabmsg->t_counts.t_numscans;
            tabentry->tuples_returned += tabmsg->t_counts.t_tuples_returned;
            tabentry->tuples_fetched += tabmsg->t_counts.t_tuples_fetched;
            tabentry->tuples_inserted += tabmsg->t_counts.t_tuples_inserted;
            tabentry->tuples_updated += tabmsg->t_counts.t_tuples_updated;
            tabentry->tuples_deleted += tabmsg->t_counts.t_tuples_deleted;
            tabentry->tuples_inplace_updated += tabmsg->t_counts.t_tuples_inplace_updated;
            tabentry->tuples_hot_updated += tabmsg->t_counts.t_tuples_hot_updated;
            /* If table was truncated, first reset the live/dead counters */
            if (tabmsg->t_counts.t_truncated) {
                tabentry->n_live_tuples = 0;
                tabentry->n_dead_tuples = 0;
            }
            tabentry->n_live_tuples += tabmsg->t_counts.t_delta_live_tuples;
            tabentry->n_dead_tuples += tabmsg->t_counts.t_delta_dead_tuples;
            tabentry->changes_since_analyze += tabmsg->t_counts.t_changed_tuples;
            tabentry->blocks_fetched += tabmsg->t_counts.t_blocks_fetched;
            tabentry->blocks_hit += tabmsg->t_counts.t_blocks_hit;
            tabentry->cu_mem_hit += tabmsg->t_counts.t_cu_mem_hit;
            tabentry->cu_hdd_sync += tabmsg->t_counts.t_cu_hdd_sync;
            tabentry->cu_hdd_asyn += tabmsg->t_counts.t_cu_hdd_asyn;
        }

        /* Clamp n_live_tuples in case of negative delta_live_tuples */
        tabentry->n_live_tuples = Max(tabentry->n_live_tuples, 0);
        /* Likewise for n_dead_tuples */
        tabentry->n_dead_tuples = Max(tabentry->n_dead_tuples, 0);

        /*
         * Add per-table stats to the per-database entry, too.
         */
        dbentry->n_tuples_returned += tabmsg->t_counts.t_tuples_returned;
        dbentry->n_tuples_fetched += tabmsg->t_counts.t_tuples_fetched;
        dbentry->n_tuples_inserted += tabmsg->t_counts.t_tuples_inserted;
        dbentry->n_tuples_updated += tabmsg->t_counts.t_tuples_updated;
        dbentry->n_tuples_deleted += tabmsg->t_counts.t_tuples_deleted;
        dbentry->n_blocks_fetched += tabmsg->t_counts.t_blocks_fetched;
        dbentry->n_blocks_hit += tabmsg->t_counts.t_blocks_hit;
        dbentry->n_cu_mem_hit += tabmsg->t_counts.t_cu_mem_hit;
        dbentry->n_cu_hdd_sync += tabmsg->t_counts.t_cu_hdd_sync;
        dbentry->n_cu_hdd_asyn += tabmsg->t_counts.t_cu_hdd_asyn;

        /* partitioned table alse should record UDI info */
        if (pg_stat_relation(tabkey.statFlag))
            continue;

        tabkey.tableid = tabmsg->t_statFlag;
        tabkey.statFlag = InvalidOid;

        tabentry = (PgStat_StatTabEntry*)hash_search(dbentry->tables, (void*)(&tabkey), HASH_ENTER, &found);

        if (!found) {
            /*
             * If it's a new table entry, initialize counters to the values we
             * just got.
             */
            tabentry->numscans = tabmsg->t_counts.t_numscans;
            tabentry->tuples_returned = tabmsg->t_counts.t_tuples_returned;
            tabentry->tuples_fetched = tabmsg->t_counts.t_tuples_fetched;
            tabentry->tuples_inserted = tabmsg->t_counts.t_tuples_inserted;
            tabentry->tuples_updated = tabmsg->t_counts.t_tuples_updated;
            tabentry->tuples_deleted = tabmsg->t_counts.t_tuples_deleted;
            tabentry->tuples_hot_updated = tabmsg->t_counts.t_tuples_hot_updated;
            tabentry->tuples_inplace_updated = tabmsg->t_counts.t_tuples_inplace_updated;
            tabentry->n_live_tuples = tabmsg->t_counts.t_delta_live_tuples;
            tabentry->n_dead_tuples = tabmsg->t_counts.t_delta_dead_tuples;
            tabentry->changes_since_analyze = tabmsg->t_counts.t_changed_tuples;
            tabentry->blocks_fetched = tabmsg->t_counts.t_blocks_fetched;
            tabentry->blocks_hit = tabmsg->t_counts.t_blocks_hit;
            tabentry->cu_mem_hit = tabmsg->t_counts.t_cu_mem_hit;
            tabentry->cu_hdd_sync = tabmsg->t_counts.t_cu_hdd_sync;
            tabentry->cu_hdd_asyn = tabmsg->t_counts.t_cu_hdd_asyn;

            tabentry->vacuum_timestamp = 0;
            tabentry->vacuum_count = 0;
            tabentry->autovac_vacuum_timestamp = 0;
            tabentry->autovac_vacuum_count = 0;
            tabentry->analyze_timestamp = 0;
            tabentry->analyze_count = 0;
            tabentry->autovac_analyze_timestamp = 0;
            tabentry->autovac_analyze_count = 0;
            tabentry->autovac_status = 0;
            tabentry->data_changed_timestamp = 0;
            tabentry->success_prune_cnt = 0;
            tabentry->total_prune_cnt = 0;
        } else {
            /*
             * Otherwise add the values to the existing entry.
             */
            tabentry->numscans += tabmsg->t_counts.t_numscans;
            tabentry->tuples_returned += tabmsg->t_counts.t_tuples_returned;
            tabentry->tuples_fetched += tabmsg->t_counts.t_tuples_fetched;
            tabentry->tuples_inserted += tabmsg->t_counts.t_tuples_inserted;
            tabentry->tuples_updated += tabmsg->t_counts.t_tuples_updated;
            tabentry->tuples_deleted += tabmsg->t_counts.t_tuples_deleted;
            tabentry->tuples_hot_updated += tabmsg->t_counts.t_tuples_hot_updated;
            tabentry->tuples_inplace_updated += tabmsg->t_counts.t_tuples_inplace_updated;
            tabentry->n_live_tuples += tabmsg->t_counts.t_delta_live_tuples;
            tabentry->n_dead_tuples += tabmsg->t_counts.t_delta_dead_tuples;
            tabentry->changes_since_analyze += tabmsg->t_counts.t_changed_tuples;
            tabentry->blocks_fetched += tabmsg->t_counts.t_blocks_fetched;
            tabentry->blocks_hit += tabmsg->t_counts.t_blocks_hit;
            tabentry->cu_mem_hit += tabmsg->t_counts.t_cu_mem_hit;
            tabentry->cu_hdd_sync += tabmsg->t_counts.t_cu_hdd_sync;
            tabentry->cu_hdd_asyn += tabmsg->t_counts.t_cu_hdd_asyn;
        }
    }
}

/* ----------
 * pgstat_recv_tabpurge() -
 *
 *	Arrange for dead table removal.
 * ----------
 */
static void pgstat_recv_tabpurge(PgStat_MsgTabpurge* msg, int len)
{
    PgStat_StatDBEntry* dbentry = NULL;
    PgStat_StatTabKey tabkey;
    PgStat_StatTabEntry* tabentry = NULL;
    PgStat_StatTabKey parentkey;
    PgStat_StatTabEntry* parententry = NULL;
    int i;

    dbentry = pgstat_get_db_entry(msg->m_databaseid, false);
    /*
     * No need to purge if we don't even know the database.
     */
    if ((dbentry == NULL) || (dbentry->tables == NULL))
        return;

    /*
     * Process all table entries in the message.
     */
    for (i = 0; i < msg->m_nentries; i++) {
        tabkey.statFlag = msg->m_entry[i].m_statFlag;
        tabkey.tableid = msg->m_entry[i].m_tableid;

        tabentry = (PgStat_StatTabEntry*)hash_search(dbentry->tables, (void*)(&tabkey), HASH_FIND, NULL);

        /* modify parent table's stat info if it is a patition */
        if ((tabentry != NULL) && tabkey.statFlag) {
            parentkey.tableid = tabkey.statFlag;
            parentkey.statFlag = InvalidOid;
            parententry = (PgStat_StatTabEntry*)hash_search(dbentry->tables, (void*)(&parentkey), HASH_FIND, NULL);
            if (parententry != NULL) {
                parententry->n_dead_tuples = Max(0, parententry->n_dead_tuples - tabentry->n_dead_tuples);
                parententry->n_live_tuples = Max(0, parententry->n_live_tuples - tabentry->n_live_tuples);
                parententry->changes_since_analyze += tabentry->changes_since_analyze;
            }
        }

        /* Remove from hashtable if present; we don't care if it's not. */
        (void*)hash_search(dbentry->tables, (void*)(&tabkey), HASH_REMOVE, NULL);
    }
}

/* ----------
 * pgstat_recv_dropdb() -
 *
 *	Arrange for dead database removal
 * ----------
 */
static void pgstat_recv_dropdb(PgStat_MsgDropdb* msg, int len)
{
    PgStat_StatDBEntry* dbentry = NULL;
    Oid db_oid = msg->m_databaseid;
    /*
     * Lookup the database in the hashtable.
     */
    dbentry = pgstat_get_db_entry(db_oid, false);

    /*
     * If found, remove it.
     */
    if (NULL != dbentry) {
        if (dbentry->tables != NULL)
            hash_destroy(dbentry->tables);
        if (dbentry->functions != NULL)
            hash_destroy(dbentry->functions);

        if (hash_search(u_sess->stat_cxt.pgStatDBHash, (void*)&(db_oid), HASH_REMOVE, NULL) == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("database hash table corrupted "
                           "during cleanup --- abort")));
        }
    }

    /*
     * delete database related hotkeys statistics
     */
    g_instance.stat_cxt.lru->DeleteHotkeysInDB(db_oid);

}

/* ----------
 * pgstat_recv_resetcounter() -
 *
 *	Reset the statistics for the specified database.
 * ----------
 */
static void pgstat_recv_resetcounter(PgStat_MsgResetcounter* msg, int len)
{
    HASHCTL hash_ctl;
    PgStat_StatDBEntry* dbentry = NULL;
    errno_t rc = EOK;

    /*
     * Lookup the database in the hashtable.  Nothing to do if not there.
     */
    dbentry = pgstat_get_db_entry(msg->m_databaseid, false);
    if (dbentry == NULL)
        return;

    /*
     * We simply throw away all the database's table entries by recreating a
     * new hash table for them.
     */
    if (dbentry->tables != NULL)
        hash_destroy(dbentry->tables);
    if (dbentry->functions != NULL)
        hash_destroy(dbentry->functions);

    dbentry->tables = NULL;
    dbentry->functions = NULL;

    (void)gs_lock_test_and_set_64(&g_instance.stat_cxt.NodeStatResetTime, GetCurrentTimestamp());

    /*
     * Reset database-level stats too.	This should match the initialization
     * code in pgstat_get_db_entry().
     */
    dbentry->n_xact_commit = 0;
    dbentry->n_xact_rollback = 0;
    dbentry->n_blocks_fetched = 0;
    dbentry->n_blocks_hit = 0;
    dbentry->n_cu_mem_hit = 0;
    dbentry->n_cu_hdd_sync = 0;
    dbentry->n_cu_hdd_asyn = 0;
    dbentry->n_tuples_returned = 0;
    dbentry->n_tuples_fetched = 0;
    dbentry->n_tuples_inserted = 0;
    dbentry->n_tuples_updated = 0;
    dbentry->n_tuples_deleted = 0;
    dbentry->last_autovac_time = 0;
    dbentry->n_temp_bytes = 0;
    dbentry->n_temp_files = 0;
    dbentry->n_deadlocks = 0;
    dbentry->n_block_read_time = 0;
    dbentry->n_block_write_time = 0;
    dbentry->n_mem_mbytes_reserved = 0;

    dbentry->stat_reset_timestamp = GetCurrentTimestamp();

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");
    hash_ctl.keysize = sizeof(PgStat_StatTabKey);
    hash_ctl.entrysize = sizeof(PgStat_StatTabEntry);
    hash_ctl.hash = tag_hash;
    dbentry->tables = hash_create("Per-database table", PGSTAT_TAB_HASH_SIZE, &hash_ctl, HASH_ELEM | HASH_FUNCTION);

    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(PgStat_StatFuncEntry);
    hash_ctl.hash = oid_hash;
    dbentry->functions = hash_create("Per-database function", 512, &hash_ctl, HASH_ELEM | HASH_FUNCTION);
}

/* ----------
 * pgstat_recv_resetshared() -
 *
 *	Reset some shared statistics of the cluster.
 * ----------
 */
static void pgstat_recv_resetsharedcounter(PgStat_MsgResetsharedcounter* msg, int len)
{
    errno_t rc = EOK;

    if (msg->m_resettarget == RESET_BGWRITER) {
        /* Reset the global background writer statistics for the cluster. */
        rc = memset_s(u_sess->stat_cxt.globalStats, sizeof(PgStat_GlobalStats), 0, sizeof(PgStat_GlobalStats));
        securec_check(rc, "\0", "\0");
        u_sess->stat_cxt.globalStats->stat_reset_timestamp = GetCurrentTimestamp();
        gs_lock_test_and_set_64(&g_instance.stat_cxt.NodeStatResetTime, GetCurrentTimestamp());
    }

    /*
     * Presumably the sender of this message validated the target, don't
     * complain here if it's not valid
     */
}

/* ----------
 * pgstat_recv_resetsinglecounter() -
 *
 *	Reset a statistics for a single object
 * ----------
 */
static void pgstat_recv_resetsinglecounter(PgStat_MsgResetsinglecounter* msg, int len)
{
    PgStat_StatDBEntry* dbentry = NULL;

    dbentry = pgstat_get_db_entry(msg->m_databaseid, false);
    if (dbentry == NULL)
        return;

    /* Set the reset timestamp for the whole database */
    dbentry->stat_reset_timestamp = GetCurrentTimestamp();
    gs_lock_test_and_set_64(&g_instance.stat_cxt.NodeStatResetTime, GetCurrentTimestamp());

    /* Remove object if it exists, ignore it if not */
    if (msg->m_resettype == RESET_TABLE) {
        PgStat_StatTabKey tabkey;

        tabkey.statFlag = msg->p_objectid;
        tabkey.tableid = msg->m_objectid;

        (void)hash_search(dbentry->tables, (void*)&(tabkey), HASH_REMOVE, NULL);
    } else if (msg->m_resettype == RESET_FUNCTION) {
        (void)hash_search(dbentry->functions, (void*)&(msg->m_objectid), HASH_REMOVE, NULL);
    }
}

/* ----------
 * pgstat_recv_autovac() -
 *
 *	Process an autovacuum signalling message.
 * ----------
 */
static void pgstat_recv_autovac(PgStat_MsgAutovacStart* msg, int len)
{
    PgStat_StatDBEntry* dbentry = NULL;

    /*
     * Store the last autovacuum time in the database's hashtable entry.
     */
    dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

    dbentry->last_autovac_time = msg->m_start_time;
}

/* ----------
 * pgstat_recv_vacuum() -
 *
 *	Process a VACUUM message.
 * ----------
 */
static void pgstat_recv_vacuum(PgStat_MsgVacuum* msg, int len)
{
    PgStat_StatDBEntry* dbentry = NULL;
    PgStat_StatTabEntry* tabentry = NULL;

    /*
     * Store the data in the table's hashtable entry.
     */
    dbentry = pgstat_get_db_entry(msg->m_databaseid, true);
    tabentry = pgstat_get_tab_entry(dbentry, msg->m_tableoid, true, msg->m_statFlag);

    /* Resetting dead_tuples ... use negtive number to verify Cstore */
    if (msg->m_tuples < 0)
        tabentry->n_dead_tuples = 0;
    else
        tabentry->n_dead_tuples = Max(0, tabentry->n_dead_tuples - msg->m_tuples);

    if (msg->m_autovacuum) {
        tabentry->autovac_vacuum_timestamp = msg->m_vacuumtime;
        tabentry->autovac_vacuum_count++;
        tabentry->vacuum_timestamp = msg->m_vacuumtime;
        tabentry->vacuum_count++;
    } else {
        tabentry->vacuum_timestamp = msg->m_vacuumtime;
        tabentry->vacuum_count++;
    }
}

/* ----------
 * PgstatRecvPrunestat() -
 *
 *	Process a pruning message
 * ----------
 */
static void PgstatRecvPrunestat(PgStat_MsgPrune* msg, int len)
{
    PgStat_StatDBEntry* dbentry = NULL;
    PgStat_StatTabEntry* tabentry = NULL;

    /*
     * Store the data in the table's hashtable entry.
     */
    dbentry = pgstat_get_db_entry(msg->m_databaseid, true);
    tabentry = pgstat_get_tab_entry(dbentry, msg->m_tableoid, true, msg->m_statFlag);

    tabentry->success_prune_cnt += msg->m_pruned_blocks;
    tabentry->total_prune_cnt += msg->m_scanned_blocks;
}


/* ----------
 * pgstat_recv_data_changed() -
 *
 *   Process a insert/delete/update/copy/[exchange/truncate/drop] partition message.
 * ----------
 */
static void pgstat_recv_data_changed(PgStat_MsgDataChanged* msg, int len)
{
    PgStat_StatDBEntry* dbentry = NULL;
    PgStat_StatTabEntry* tabentry = NULL;

    /*
     * Store the data in the table's hashtable entry.
     */
    dbentry = pgstat_get_db_entry(msg->m_databaseid, true);
    tabentry = pgstat_get_tab_entry(dbentry, msg->m_tableoid, true, msg->m_statFlag);

    /* store start time of insert/delete/update operation */
    tabentry->data_changed_timestamp = msg->m_changed_time;
}

/* ----------
 * pgstat_recv_autovac_stat() -
 *
 *	Process a autovac stat message.
 * ----------
 */
static void pgstat_recv_autovac_stat(PgStat_MsgAutovacStat* msg, int len)
{
    PgStat_StatDBEntry* dbentry = NULL;
    PgStat_StatTabEntry* tabentry = NULL;

    /*
     * Store the data in the table's hashtable entry.
     */
    dbentry = pgstat_get_db_entry(msg->m_databaseid, true);
    tabentry = pgstat_get_tab_entry(dbentry, msg->m_tableoid, true, msg->m_statFlag);
    if (tabentry && AV_TIMEOUT == msg->m_autovacStat) {
        increase_continued_timeout(tabentry->autovac_status);
        increase_toatl_timeout(tabentry->autovac_status);
    }
}

/* ----------
 * pgstat_recv_truncate() -
 *
 *	Process a TRUNCATE message.
 * ----------
 */
static void pgstat_recv_truncate(PgStat_MsgTruncate* msg, int len)
{
    PgStat_StatDBEntry* dbentry = NULL;
    PgStat_StatTabEntry* tabentry = NULL;
    PgStat_StatTabEntry* parent_entry = NULL;

    /*
     * Store the data in the table's hashtable entry.
     */
    dbentry = pgstat_get_db_entry(msg->m_databaseid, true);
    tabentry = pgstat_get_tab_entry(dbentry, msg->m_tableoid, true, msg->m_statFlag);

    /* modify parent table's stat info(dead_tuple, live_tuple, changes_since_analyze) if it is a patition */
    if (msg->m_statFlag) {
        parent_entry = pgstat_get_tab_entry(dbentry, msg->m_statFlag, true, InvalidOid);

        if (NULL != parent_entry) {
            parent_entry->n_dead_tuples = Max(0, parent_entry->n_dead_tuples - tabentry->n_dead_tuples);
            parent_entry->n_live_tuples = Max(0, parent_entry->n_live_tuples - tabentry->n_live_tuples);
            parent_entry->changes_since_analyze += tabentry->changes_since_analyze;
        }
    }

    /* After truncate reset dead_tuple and live_tuple */
    tabentry->n_dead_tuples = 0;
    tabentry->n_live_tuples = 0;
}

/* ----------
 * pgstat_recv_analyze() -
 *
 *	Process an ANALYZE message.
 * ----------
 */
static void pgstat_recv_analyze(PgStat_MsgAnalyze* msg, int len)
{
    PgStat_StatDBEntry* dbentry = NULL;
    PgStat_StatTabEntry* tabentry = NULL;

    /*
     * Store the data in the table's hashtable entry.
     */
    dbentry = pgstat_get_db_entry(msg->m_databaseid, true);
    tabentry = pgstat_get_tab_entry(dbentry, msg->m_tableoid, true, msg->m_statFlag);

    tabentry->n_live_tuples = msg->m_live_tuples;
    tabentry->n_dead_tuples = msg->m_dead_tuples;

    /*
     * We reset changes_since_analyze to zero, forgetting any changes that
     * occurred while the ANALYZE was in progress.
     */
    tabentry->changes_since_analyze = 0;

    if (msg->m_autovacuum) {
        tabentry->autovac_analyze_timestamp = msg->m_analyzetime;
        tabentry->autovac_analyze_count++;
        tabentry->analyze_timestamp = msg->m_analyzetime;
        tabentry->analyze_count++;
        reset_continued_timeout(tabentry->autovac_status);
    } else {
        tabentry->analyze_timestamp = msg->m_analyzetime;
        tabentry->analyze_count++;
    }
}

/* ----------
 * pgstat_recv_bgwriter() -
 *
 *	Process a BGWRITER message.
 * ----------
 */
static void pgstat_recv_bgwriter(PgStat_MsgBgWriter* msg, int len)
{
    if (u_sess->stat_cxt.globalStats->timed_checkpoints > (INT64_MAX - msg->m_timed_checkpoints)) {
        ereport(ERROR, (errmsg("timed_checkpoints overflow")));
    }
    u_sess->stat_cxt.globalStats->timed_checkpoints += msg->m_timed_checkpoints;

    if (u_sess->stat_cxt.globalStats->requested_checkpoints > (INT64_MAX - msg->m_requested_checkpoints)) {
        ereport(ERROR, (errmsg("requested_checkpoints overflow")));
    }
    u_sess->stat_cxt.globalStats->requested_checkpoints += msg->m_requested_checkpoints;

    if (u_sess->stat_cxt.globalStats->checkpoint_write_time > (INT64_MAX - msg->m_checkpoint_write_time)) {
        ereport(ERROR, (errmsg("checkpoint_write_time overflow")));
    }
    u_sess->stat_cxt.globalStats->checkpoint_write_time += msg->m_checkpoint_write_time;

    if (u_sess->stat_cxt.globalStats->checkpoint_sync_time > (INT64_MAX - msg->m_checkpoint_sync_time)) {
        ereport(ERROR, (errmsg("checkpoint_sync_time overflow")));
    }
    u_sess->stat_cxt.globalStats->checkpoint_sync_time += msg->m_checkpoint_sync_time;

    if (u_sess->stat_cxt.globalStats->buf_written_checkpoints > (INT64_MAX - msg->m_buf_written_checkpoints)) {
        ereport(ERROR, (errmsg("buf_written_checkpoints overflow")));
    }
    u_sess->stat_cxt.globalStats->buf_written_checkpoints += msg->m_buf_written_checkpoints;

    if (u_sess->stat_cxt.globalStats->buf_written_clean > (INT64_MAX - msg->m_buf_written_clean)) {
        ereport(ERROR, (errmsg("buf_written_clean overflow")));
    }
    u_sess->stat_cxt.globalStats->buf_written_clean += msg->m_buf_written_clean;

    if (u_sess->stat_cxt.globalStats->maxwritten_clean > (INT64_MAX - msg->m_maxwritten_clean)) {
        ereport(ERROR, (errmsg("maxwritten_clean overflow")));
    }
    u_sess->stat_cxt.globalStats->maxwritten_clean += msg->m_maxwritten_clean;

    if (u_sess->stat_cxt.globalStats->buf_written_backend > (INT64_MAX - msg->m_buf_written_backend)) {
        ereport(ERROR, (errmsg("buf_written_backend overflow")));
    }
    u_sess->stat_cxt.globalStats->buf_written_backend += msg->m_buf_written_backend;

    if (u_sess->stat_cxt.globalStats->buf_fsync_backend > (INT64_MAX - msg->m_buf_fsync_backend)) {
        ereport(ERROR, (errmsg("buf_fsync_backend overflow")));
    }
    u_sess->stat_cxt.globalStats->buf_fsync_backend += msg->m_buf_fsync_backend;

    if (u_sess->stat_cxt.globalStats->buf_alloc > (INT64_MAX - msg->m_buf_alloc)) {
        ereport(ERROR, (errmsg("buf_alloc overflow")));
    }
    u_sess->stat_cxt.globalStats->buf_alloc += msg->m_buf_alloc;
}

/* ----------
 * pgstat_recv_recoveryconflict() -
 *
 *	Process a RECOVERYCONFLICT message.
 * ----------
 */
static void pgstat_recv_recoveryconflict(const PgStat_MsgRecoveryConflict* msg)
{
    PgStat_StatDBEntry* dbentry = NULL;

    dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

    switch (msg->m_reason) {
        case PROCSIG_RECOVERY_CONFLICT_DATABASE:

            /*
             * Since we drop the information about the database as soon as it
             * replicates, there is no point in counting these conflicts.
             */
            break;
        case PROCSIG_RECOVERY_CONFLICT_TABLESPACE:
            dbentry->n_conflict_tablespace++;
            break;
        case PROCSIG_RECOVERY_CONFLICT_LOCK:
            dbentry->n_conflict_lock++;
            break;
        case PROCSIG_RECOVERY_CONFLICT_SNAPSHOT:
            dbentry->n_conflict_snapshot++;
            break;
        case PROCSIG_RECOVERY_CONFLICT_BUFFERPIN:
            dbentry->n_conflict_bufferpin++;
            break;
        case PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK:
            dbentry->n_conflict_startup_deadlock++;
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_CASE_NOT_FOUND),
                    errmsg("unrecognized bypass recovery conflict reason: %d", msg->m_reason)));
    }
}

/* ----------
 * pgstat_recv_deadlock() -
 *
 *	Process a DEADLOCK message.
 * ----------
 */
static void pgstat_recv_deadlock(const PgStat_MsgDeadlock* msg)
{
    PgStat_StatDBEntry* dbentry = NULL;

    dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

    dbentry->n_deadlocks++;
}

/* ----------
 * pgstat_recv_tempfile() -
 *
 *	Process a TEMPFILE message.
 * ----------
 */
static void pgstat_recv_tempfile(PgStat_MsgTempFile* msg, int len)
{
    PgStat_StatDBEntry* dbentry = NULL;

    dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

    dbentry->n_temp_bytes += msg->m_filesize;
    dbentry->n_temp_files += 1;
}

void pgstate_update_percentile_responsetime(void)
{
    if (IS_PGXC_COORDINATOR || IS_SINGLE_NODE) {
        int64 elapse_start = GetCurrentStatementLocalStartTimestamp();
        if (elapse_start != 0) {
            int64 duration = GetCurrentTimestamp() - elapse_start;
            if (IS_SINGLE_NODE) {
                pgstat_update_responstime_singlenode(u_sess->unique_sql_cxt.unique_sql_id, elapse_start, duration);
            } else {
                pgstat_report_sql_rt(u_sess->unique_sql_cxt.unique_sql_id, elapse_start, duration);
            }
        }
    }
}

void pgstat_update_responstime_singlenode(uint64 UniqueSQLId, int64 start_time, int64 rt)
{
    int32 sqlRTIndex;

    if (!u_sess->attr.attr_common.enable_instr_rt_percentile)
        return;

    /* if the percentile thread is memory copying or memory is null, not record */
    if (g_instance.stat_cxt.force_process != false || g_instance.stat_cxt.sql_rt_info_array == NULL) {
        return;
    }

    LWLockAcquire(PercentileLock, LW_SHARED);
    /* if the current is full , not record this time, set force process to true to active percentile thread */
    sqlRTIndex = gs_atomic_add_32(&g_instance.stat_cxt.sql_rt_info_array->sqlRTIndex, 1);
    if ((sqlRTIndex >= MAX_SQL_RT_INFO_COUNT) || (sqlRTIndex < 0)) {
        g_instance.stat_cxt.force_process = true;
        LWLockRelease(PercentileLock);
        return;
    }
    g_instance.stat_cxt.sql_rt_info_array->sqlRT[sqlRTIndex].UniqueSQLId = UniqueSQLId;
    g_instance.stat_cxt.sql_rt_info_array->sqlRT[sqlRTIndex].start_time = start_time;
    g_instance.stat_cxt.sql_rt_info_array->sqlRT[sqlRTIndex].rt = rt;
    LWLockRelease(PercentileLock);
}

static void pgstat_recv_sql_responstime(PgStat_SqlRT* msg, int len)
{

    if (g_instance.stat_cxt.sql_rt_info_array == NULL) {
        return;
    }

    LWLockAcquire(PercentileLock, LW_EXCLUSIVE);

    int32 sqlRTIndex = g_instance.stat_cxt.sql_rt_info_array->sqlRTIndex;

    if (sqlRTIndex == MAX_SQL_RT_INFO_COUNT) {
        g_instance.stat_cxt.sql_rt_info_array->isFull = true;  // other cn will cover the value from first one
        if (PgxcIsCentralCoordinator(g_instance.attr.attr_common.PGXCNodeName)) {
            g_instance.stat_cxt.force_process = true;
            LWLockRelease(PercentileLock);
            while (g_instance.stat_cxt.force_process) {
                pg_usleep(PGSTAT_RETRY_DELAY);  // CCN wait force process percentile done
            }
            LWLockAcquire(PercentileLock, LW_EXCLUSIVE);
        }
        sqlRTIndex = 0;
        g_instance.stat_cxt.sql_rt_info_array->sqlRTIndex = sqlRTIndex;
    }

    g_instance.stat_cxt.sql_rt_info_array->sqlRT[sqlRTIndex].UniqueSQLId = msg->sqlRT.UniqueSQLId;
    g_instance.stat_cxt.sql_rt_info_array->sqlRT[sqlRTIndex].start_time = msg->sqlRT.start_time;
    g_instance.stat_cxt.sql_rt_info_array->sqlRT[sqlRTIndex].rt = msg->sqlRT.rt;
    g_instance.stat_cxt.sql_rt_info_array->sqlRTIndex = g_instance.stat_cxt.sql_rt_info_array->sqlRTIndex + 1;
    LWLockRelease(PercentileLock);
}

static int sqlRTComparator(const void* s1, const void* s2)
{
    const SqlRTInfo* sqlRT1 = (SqlRTInfo*)s1;
    const SqlRTInfo* sqlRT2 = (SqlRTInfo*)s2;
    if (sqlRT1->rt > sqlRT2->rt)
        return 1;
    else if (sqlRT1->rt < sqlRT2->rt)
        return -1;
    else
        return 0;
}

static void prepare_calculate(SqlRTInfoArray* sql_rt_info, int* counter)
{
    int sql_rt_info_count = 0;
    LWLockAcquire(PercentileLock, LW_SHARED);

    if (sql_rt_info->isFull)
        sql_rt_info_count = MAX_SQL_RT_INFO_COUNT;
    else
        sql_rt_info_count = sql_rt_info->sqlRTIndex;

    *counter = 0;
    if (sql_rt_info_count == 0) {
        sql_rt_info->isFull = false;
        LWLockRelease(PercentileLock);
        return;
    }

    if (u_sess->percentile_cxt.LocalsqlRT != NULL)
        pfree_ext(u_sess->percentile_cxt.LocalsqlRT);

    u_sess->percentile_cxt.LocalsqlRT = (SqlRTInfo*)MemoryContextAlloc(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX), sql_rt_info_count * sizeof(SqlRTInfo));

    int ss_rc = memset_s(u_sess->percentile_cxt.LocalsqlRT,
        sql_rt_info_count * sizeof(SqlRTInfo),
        0,
        sql_rt_info_count * sizeof(SqlRTInfo));
    securec_check(ss_rc, "\0", "\0");
    ss_rc = memcpy_s(u_sess->percentile_cxt.LocalsqlRT,
        sql_rt_info_count * sizeof(SqlRTInfo),
        sql_rt_info->sqlRT,
        sql_rt_info_count * sizeof(SqlRTInfo));
    securec_check(ss_rc, "\0", "\0");

    sql_rt_info->isFull = false;
    sql_rt_info->sqlRTIndex = 0;
    LWLockRelease(PercentileLock);
    *counter = sql_rt_info_count;
    qsort(u_sess->percentile_cxt.LocalsqlRT, sql_rt_info_count, sizeof(SqlRTInfo), sqlRTComparator);
}

static void prepare_calculate_single(const SqlRTInfoArray* sql_rt_info, int* counter)
{
    int32 sql_rt_info_count = g_instance.stat_cxt.sql_rt_info_array->sqlRTIndex;

    if (sql_rt_info_count > MAX_SQL_RT_INFO_COUNT) {
        sql_rt_info_count = MAX_SQL_RT_INFO_COUNT;
    }

    *counter = 0;
    if (sql_rt_info_count == 0) {
        return;
    }

    if (u_sess->percentile_cxt.LocalsqlRT != NULL)
        pfree_ext(u_sess->percentile_cxt.LocalsqlRT);

    u_sess->percentile_cxt.LocalsqlRT = (SqlRTInfo*)MemoryContextAllocZero(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DFX), sql_rt_info_count * sizeof(SqlRTInfo));

    LWLockAcquire(PercentileLock, LW_EXCLUSIVE);
    errno_t ss_rc = memcpy_s(u_sess->percentile_cxt.LocalsqlRT,
        sql_rt_info_count * sizeof(SqlRTInfo),
        sql_rt_info->sqlRT,
        sql_rt_info_count * sizeof(SqlRTInfo));
    LWLockRelease(PercentileLock);
    securec_check(ss_rc, "\0", "\0");
    g_instance.stat_cxt.sql_rt_info_array->sqlRTIndex = 0;
    g_instance.stat_cxt.force_process = false;
    *counter = sql_rt_info_count;
    qsort(u_sess->percentile_cxt.LocalsqlRT, sql_rt_info_count, sizeof(SqlRTInfo), sqlRTComparator);
}

/* ----------
 * pgstat_recv_memReserved() -
 *
 *      Process a reserving-memory message.
 * ----------
 */
static void pgstat_recv_memReserved(const PgStat_MsgMemReserved* msg)
{
    PgStat_StatDBEntry* dbentry = NULL;

    dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

    if (msg->m_reserve_or_release == 1)
        dbentry->n_mem_mbytes_reserved += msg->m_memMbytes;
    else if (msg->m_reserve_or_release == -1)
        dbentry->n_mem_mbytes_reserved -= msg->m_memMbytes;

    if (dbentry->n_mem_mbytes_reserved < 0)
        dbentry->n_mem_mbytes_reserved = 0;
}

/* ----------
 * pgstat_recv_funcstat() -
 *
 *	Count what the backend has done.
 * ----------
 */
static void pgstat_recv_funcstat(PgStat_MsgFuncstat* msg, int len)
{
    PgStat_FunctionEntry* funcmsg = &(msg->m_entry[0]);
    PgStat_StatDBEntry* dbentry = NULL;
    PgStat_StatFuncEntry* funcentry = NULL;
    int i;
    bool found = false;

    dbentry = pgstat_get_db_entry(msg->m_databaseid, true);

    /*
     * Process all function entries in the message.
     */
    for (i = 0; i < msg->m_nentries; i++, funcmsg++) {
        funcentry = (PgStat_StatFuncEntry*)hash_search(dbentry->functions, (void*)&(funcmsg->f_id), HASH_ENTER, &found);

        if (!found) {
            /*
             * If it's a new function entry, initialize counters to the values
             * we just got.
             */
            funcentry->f_numcalls = funcmsg->f_numcalls;
            funcentry->f_total_time = funcmsg->f_total_time;
            funcentry->f_self_time = funcmsg->f_self_time;
        } else {
            /*
             * Otherwise add the values to the existing entry.
             */
            funcentry->f_numcalls += funcmsg->f_numcalls;
            funcentry->f_total_time += funcmsg->f_total_time;
            funcentry->f_self_time += funcmsg->f_self_time;
        }
    }
}

/* ----------
 * pgstat_recv_funcpurge() -
 *
 *	Arrange for dead function removal.
 * ----------
 */
static void pgstat_recv_funcpurge(PgStat_MsgFuncpurge* msg, int len)
{
    PgStat_StatDBEntry* dbentry = NULL;
    int i;

    dbentry = pgstat_get_db_entry(msg->m_databaseid, false);

    /*
     * No need to purge if we don't even know the database.
     */
    if ((dbentry == NULL) || (dbentry->functions == NULL))
        return;

    /*
     * Process all function entries in the message.
     */
    for (i = 0; i < msg->m_nentries; i++) {
        /* Remove from hashtable if present; we don't care if it's not. */
        (void)hash_search(dbentry->functions, (void*)&(msg->m_functionid[i]), HASH_REMOVE, NULL);
    }
}

void pgstat_initstats_partition(Partition part)
{
    Oid part_id = part->pd_id;

    if (PGINVALID_SOCKET == g_instance.stat_cxt.pgStatSock || !u_sess->attr.attr_common.pgstat_track_counts) {
        /* We're not counting at all */
        part->pd_pgstat_info = NULL;

        return;
    }

    /* find or make the PgStat_TableStatus entry, and update link */
    Oid relid;
    if (part->pd_part->parttype == PART_OBJ_TYPE_TABLE_SUB_PARTITION) {
        relid = partid_get_parentid(part->pd_part->parentid);
    } else {
        relid = PartitionGetRelid(part);
    }
    part->pd_pgstat_info = get_tabstat_entry(part_id, false, relid);
}

/*
 *we calculate cpu numbers from sysfs, so we should make sure we can access this file system.
 */
static bool checkSysFileSystem(void)
{
    /* Read through sysfs. */
    if (access(SysFileSystemPath, F_OK))
        return false;

    if (access(ThreadSiblingFile, F_OK))
        return false;

    if (access(CoreSiblingFile, F_OK))
        return false;

    return true;
}

/*
 *check whether the SysCpuPath is accessable.one accessable path represented one logical cpu.
 */
static bool checkLogicalCpu(uint32 cpuNum)
{
    char pathbuf[ProcPathMax] = "";

    errno_t ret = sprintf_s(pathbuf, sizeof(pathbuf), SysCpuPath, cpuNum);
    securec_check_ss(ret, "\0", "\0");

    return access(pathbuf, F_OK) == 0;
}

/* count the set bit in a mapping file */
#define pg_isxdigit(c)                                                               \
    (((c) >= (int)'0' && (c) <= (int)'9') || ((c) >= (int)'a' && (c) <= (int)'f') || \
        ((c) >= (int)'A' && (c) <= (int)'F'))
static uint32 parseSiblingFile(const char* path)
{
    int c;
    uint32 result = 0;
    char s[2];
    FILE* fp = NULL;
    union {
        uint32 a : 4;
        struct {
            uint32 a1 : 1;
            uint32 a2 : 1;
            uint32 a3 : 1;
            uint32 a4 : 1;
        } b;
    } d;
    errno_t errorno = EOK;
    errorno = memset_s(&d, sizeof(d), 0, sizeof(d));
    securec_check_c(errorno, "\0", "\0");

    if ((fp = fopen(path, "r")) != NULL) {
        while ((c = fgetc(fp)) != EOF) {
            if (pg_isxdigit(c)) {
                s[0] = c;
                s[1] = '\0';
                d.a = strtoul(s, NULL, 16);
                result += d.b.a1;
                result += d.b.a2;
                result += d.b.a3;
                result += d.b.a4;
            }
        }
        (void)fclose(fp);
    }

    return result;
}

/*
 *This function is to get the number of logical cpus, cores and physical cpus of the system.
 *We get these infomation by analysing sysfs file system. If we failed to get the three fields,
 *we just ignore them when we report. And if we got this field, we will not analyse the files
 *when we call this function next time.
 *
 *Note: This function must be called before getCpuTimes because we need logical cpu number
 *to calculate the avg cpu consumption.
 */
void getCpuNums(void)
{
    uint32 cpuNum = 0;
    uint32 threadPerCore = 0;
    uint32 threadPerSocket = 0;

    /* if we have already got the cpu numbers. it's not necessary to read the files again. */
    if (u_sess->stat_cxt.osStatDescArray[NUM_CPUS].got && u_sess->stat_cxt.osStatDescArray[NUM_CPU_CORES].got &&
        u_sess->stat_cxt.osStatDescArray[NUM_CPU_SOCKETS].got)
        return;

    /* if the sysfs file system is not accessable. we can't get the cpu numbers. */
    if (checkSysFileSystem()) {
        /* check the SysCpuPath, one accessable path represented one logical cpu. */
        while (checkLogicalCpu(cpuNum))
            cpuNum++;

        if (cpuNum > 0) {
            /* cpu numbers */
            u_sess->stat_cxt.osStatDataArray[NUM_CPUS].int32Value = cpuNum;
            u_sess->stat_cxt.osStatDescArray[NUM_CPUS].got = true;

            /*
             *parse the mapping files ThreadSiblingFile and CoreSiblingFile. if we failed open the file or read wrong
             *data, we just ignore this field.
             */
            threadPerCore = parseSiblingFile(ThreadSiblingFile);
            if (threadPerCore > 0) {
                /* core numbers */
                u_sess->stat_cxt.osStatDataArray[NUM_CPU_CORES].int32Value = cpuNum / threadPerCore;
                u_sess->stat_cxt.osStatDescArray[NUM_CPU_CORES].got = true;
            }

            threadPerSocket = parseSiblingFile(CoreSiblingFile);
            if (threadPerSocket > 0) {
                /* socket numbers */
                u_sess->stat_cxt.osStatDataArray[NUM_CPU_SOCKETS].int32Value = cpuNum / threadPerSocket;
                u_sess->stat_cxt.osStatDescArray[NUM_CPU_SOCKETS].got = true;
            }
        }
    }
}

/*
 *This function is to get the system cpu time consumption details. We read /proc/stat
 *file for this infomation. If we failed to get the ten fields, we just ignore them when we
 *report.
 *
 *Note: Remember to call getCpuNums before this function.
 */
void getCpuTimes(void)
{
    const char* statPath = "/proc/stat";
    FILE* fd = NULL;
    char* line = NULL;
    size_t len = 0;
    uint64 readTime[NumOfCpuTimeReads];
    char* temp = NULL;
    int i;

    /* reset the member "got" of u_sess->stat_cxt.osStatDescArray to false */
    errno_t rc = memset_s(readTime, sizeof(readTime), 0, sizeof(readTime));
    securec_check(rc, "\0", "\0");

    for (i = IDLE_TIME; i <= AVG_NICE_TIME; i++) {
        u_sess->stat_cxt.osStatDescArray[i].got = false;
    }

    /* open /proc/stat file. */
    if ((fd = fopen(statPath, "r")) != NULL) {
        /* get the first line of the file. */
        if (gs_getline(&line, &len, fd) > 0) {
            /* get the second to sixth word of the line. */
            temp = line + sizeof("cpu");
            for (i = 0; i < NumOfCpuTimeReads; i++) {
                readTime[i] = strtoul(temp, &temp, 10);
            }

            /* convert the jiffies time to centi-sec. for busy time, it equals user time plus sys time */
            u_sess->stat_cxt.osStatDataArray[USER_TIME].int64Value = JiffiesGetCentiSec(readTime[0]);
            u_sess->stat_cxt.osStatDataArray[NICE_TIME].int64Value = JiffiesGetCentiSec(readTime[1]);
            u_sess->stat_cxt.osStatDataArray[SYS_TIME].int64Value = JiffiesGetCentiSec(readTime[2]);
            u_sess->stat_cxt.osStatDataArray[IDLE_TIME].int64Value = JiffiesGetCentiSec(readTime[3]);
            u_sess->stat_cxt.osStatDataArray[IOWAIT_TIME].int64Value = JiffiesGetCentiSec(readTime[4]);
            u_sess->stat_cxt.osStatDataArray[BUSY_TIME].int64Value = JiffiesGetCentiSec(readTime[0] + readTime[2]);

            /* as we have already got the cpu times, we set the "got" to true. */
            for (i = IDLE_TIME; i <= NICE_TIME; i++) {
                u_sess->stat_cxt.osStatDescArray[i].got = true;
            }

            /* if the cpu numbers have been got, we can calculate the avg cpu times and set the "got" to true. */
            if (u_sess->stat_cxt.osStatDescArray[NUM_CPUS].got) {
                u_sess->stat_cxt.osStatDataArray[AVG_USER_TIME].int64Value =
                    u_sess->stat_cxt.osStatDataArray[USER_TIME].int64Value /
                    u_sess->stat_cxt.osStatDataArray[NUM_CPUS].int32Value;
                u_sess->stat_cxt.osStatDataArray[AVG_NICE_TIME].int64Value =
                    u_sess->stat_cxt.osStatDataArray[NICE_TIME].int64Value /
                    u_sess->stat_cxt.osStatDataArray[NUM_CPUS].int32Value;
                u_sess->stat_cxt.osStatDataArray[AVG_SYS_TIME].int64Value =
                    u_sess->stat_cxt.osStatDataArray[SYS_TIME].int64Value /
                    u_sess->stat_cxt.osStatDataArray[NUM_CPUS].int32Value;
                u_sess->stat_cxt.osStatDataArray[AVG_IDLE_TIME].int64Value =
                    u_sess->stat_cxt.osStatDataArray[IDLE_TIME].int64Value /
                    u_sess->stat_cxt.osStatDataArray[NUM_CPUS].int32Value;
                u_sess->stat_cxt.osStatDataArray[AVG_IOWAIT_TIME].int64Value =
                    u_sess->stat_cxt.osStatDataArray[IOWAIT_TIME].int64Value /
                    u_sess->stat_cxt.osStatDataArray[NUM_CPUS].int32Value;
                u_sess->stat_cxt.osStatDataArray[AVG_BUSY_TIME].int64Value =
                    u_sess->stat_cxt.osStatDataArray[BUSY_TIME].int64Value /
                    u_sess->stat_cxt.osStatDataArray[NUM_CPUS].int32Value;

                for (i = AVG_IDLE_TIME; i <= AVG_NICE_TIME; i++) {
                    u_sess->stat_cxt.osStatDescArray[i].got = true;
                }
            }
        }

        if (NULL != line)
            pfree(line);
        fclose(fd);
    }
}

/*
 *This function is to get the system virtual memory paging infomation (actually it will
 *get how many bytes paged in/out due to virtual memory paging). We read /proc/vmstat
 *file for this infomation. If we failed to get the two fields, we just ignore them when
 *we report.
 */
void getVmStat(void)
{
    const char* vmStatPath = "/proc/vmstat";
    int fd = -1;
    char buffer[VmStatFileReadBuffer + 1];
    char* temp = NULL;
    uint64 inPages = 0;
    uint64 outPages = 0;
    uint64 pageSize = sysconf(_SC_PAGE_SIZE);

    if (pageSize == 0) {
        ereport(ERROR, (errmsg("Invalid page size")));
    }

    /* reset the member "got" of u_sess->stat_cxt.osStatDescArray to false */
    u_sess->stat_cxt.osStatDescArray[VM_PAGE_IN_BYTES].got = false;
    u_sess->stat_cxt.osStatDescArray[VM_PAGE_OUT_BYTES].got = false;

    /* open /proc/vmstat file. */
    if ((fd = open(vmStatPath, O_RDONLY, 0)) >= 0) {
        /* read the file to local buffer. */
        if (read(fd, buffer, VmStatFileReadBuffer) > 0) {
            buffer[VmStatFileReadBuffer] = '\0';
            /* find the pgpgin and pgpgout field. if failed, we just ignore this field */
            temp = strstr(buffer, "pswpin");
            if (NULL != temp) {
                temp += sizeof("pswpin");
                inPages = strtoul(temp, NULL, 10);

                if (inPages < ULONG_MAX / pageSize) {
                    u_sess->stat_cxt.osStatDataArray[VM_PAGE_IN_BYTES].int64Value = inPages * pageSize;
                    u_sess->stat_cxt.osStatDescArray[VM_PAGE_IN_BYTES].got = true;
                }
            }

            temp = strstr(buffer, "pswpout");
            if (NULL != temp) {
                temp += sizeof("pswpout");
                outPages = strtoul(temp, NULL, 10);

                if (outPages < ULONG_MAX / pageSize) {
                    u_sess->stat_cxt.osStatDataArray[VM_PAGE_OUT_BYTES].int64Value = outPages * pageSize;
                    u_sess->stat_cxt.osStatDescArray[VM_PAGE_OUT_BYTES].got = true;
                }
            }
        }

        close(fd);
    }
}

/*
 *This function is to get the total physical memory size of the system. We read /proc/meminfo
 *file for this infomation. If we failed to get this field, we just ignore it when we report. And if
 *if we got this field, we will not read the file when we call this function next time.
 */
void getTotalMem(void)
{
    const char* memInfoPath = "/proc/meminfo";
    FILE* fd = NULL;
    char* line = NULL;
    char* temp = NULL;
    uint64 ret = 0;
    size_t len = 0;

    /* if we have already got the physical memory size. it's not necessary to read the files again. */
    if (u_sess->stat_cxt.osStatDescArray[PHYSICAL_MEMORY_BYTES].got)
        return;

    /* open /proc/meminfo file. */
    if ((fd = fopen(memInfoPath, "r")) != NULL) {
        /* get the first line of the file. and read the number. */
        if (gs_getline(&line, &len, fd) > 0) {
            temp = line + sizeof("MemTotal:");
            ret = strtoul(temp, NULL, 10);

            if (ret < ULONG_MAX / 1024) {
                u_sess->stat_cxt.osStatDataArray[PHYSICAL_MEMORY_BYTES].int64Value = ret * 1024;
                u_sess->stat_cxt.osStatDescArray[PHYSICAL_MEMORY_BYTES].got = true;
            }
        }

        if (NULL != line)
            pfree(line);
        (void)fclose(fd);
    }
}

/*
 *This function is to get the load infomation of the system (actually it's the avg length of cpu
 *wait queue in the past one minute). We read /proc/loadavg file for this infomation. If we
 *failed to get this field, we just ignore it when we report.
 */
void getOSRunLoad(void)
{
    const char* loadAvgPath = "/proc/loadavg";
    FILE* fd = NULL;
    char* line = NULL;
    size_t len = 0;

    /* reset the member "got" of u_sess->stat_cxt.osStatDescArray to false */
    u_sess->stat_cxt.osStatDescArray[RUNLOAD].got = false;

    /* open the /proc/loadavg file. */
    if ((fd = fopen(loadAvgPath, "r")) != NULL) {
        /* get the first line of the file and read the first number of the line. */
        if (gs_getline(&line, &len, fd) > 0) {
            u_sess->stat_cxt.osStatDataArray[RUNLOAD].float8Value = strtod(line, NULL);
            u_sess->stat_cxt.osStatDescArray[RUNLOAD].got = true;
        }

        if (NULL != line)
            pfree(line);
        fclose(fd);
    }
}

Datum Int64GetNumberDatum(NumericValue value)
{
    return DirectFunctionCall1(int8_numeric, Int64GetDatum(value.int64Value));
}

Datum Float8GetNumberDatum(NumericValue value)
{
    return DirectFunctionCall1(float8_numeric, Float8GetDatum(value.float8Value));
}

Datum Int32GetNumberDatum(NumericValue value)
{
    return DirectFunctionCall1(int4_numeric, Int32GetDatum(value.int32Value));
}

void getSessionID(char* sessid, pg_time_t startTime, ThreadId Threadid)
{
    int rc = 0;
    Assert(NULL != sessid);
    rc = snprintf_s(sessid, SESSION_ID_LEN, SESSION_ID_LEN - 1, "%ld.%lu", startTime, Threadid);
    securec_check_ss(rc, "\0", "\0");
}

void getThrdID(char* thrdid, pg_time_t startTime, ThreadId Threadid)
{
    int rc = 0;
    Assert(NULL != thrdid);
    rc = snprintf_s(thrdid, SESSION_ID_LEN, SESSION_ID_LEN - 1, "%ld.%lu", startTime, Threadid);
    securec_check_ss(rc, "\0", "\0");
}

#ifdef ENABLE_MOT
MotSessionMemoryDetail* GetMotSessionMemoryDetail(uint32* num)
{
    MotSessionMemoryDetail* returnDetailArray = NULL;
    ForeignDataWrapper* fdw = NULL;
    FdwRoutine* fdwroutine = NULL;

    *num = 0;

    fdw = GetForeignDataWrapperByName(MOT_FDW, false);
    if (fdw != NULL) {
        fdwroutine = GetFdwRoutine(fdw->fdwhandler);
        if (fdwroutine) {
            returnDetailArray = fdwroutine->GetForeignSessionMemSize(num);
        }
    }

    return returnDetailArray;
}

MotMemoryDetail* GetMotMemoryDetail(uint32* num, bool isGlobal)
{
    MotMemoryDetail* returnDetailArray = NULL;
    ForeignDataWrapper* fdw = NULL;
    FdwRoutine* fdwroutine = NULL;

    *num = 0;

    fdw = GetForeignDataWrapperByName(MOT_FDW, false);
    if (fdw != NULL) {
        fdwroutine = GetFdwRoutine(fdw->fdwhandler);
        if (fdwroutine != NULL) {
            returnDetailArray = fdwroutine->GetForeignMemSize(num, isGlobal);
        }
    }

    return returnDetailArray;
}
#endif

int64 getCpuTime(void)
{
#ifndef WIN32
    struct timespec tv;
    int64 res;

    (void)clock_gettime(CLOCK_THREAD_CPUTIME_ID, &tv);
    res = tv.tv_sec * 1000000 + tv.tv_nsec / 1000;

    return res;
#else
    return 0;
#endif
}

int64 JiffiesToSec(uint64 jiffies)
{
    return JiffiesGetCentiSec(jiffies) / 100;
}

Size sessionTimeShmemSize(void)
{
    return mul_size(SessionTimeArraySize, sizeof(SessionTimeEntry));
}

const char* TimeInfoTypeName[TOTAL_TIME_INFO_TYPES] = {"DB_TIME",
    "CPU_TIME",
    "EXECUTION_TIME",
    "PARSE_TIME",
    "PLAN_TIME",
    "REWRITE_TIME",
    "PL_EXECUTION_TIME",
    "PL_COMPILATION_TIME",
    "NET_SEND_TIME",
    "DATA_IO_TIME"};

void sessionTimeShmemInit(void)
{
    bool found = false;
    errno_t rc;

    t_thrd.shemem_ptr_cxt.sessionTimeArray = (SessionTimeEntry*)ShmemInitStruct(
        "SessionTime Array", mul_size(SessionTimeArraySize, sizeof(SessionTimeEntry)), &found);

    if (!found) {
        rc = memset_s(t_thrd.shemem_ptr_cxt.sessionTimeArray,
            mul_size(SessionTimeArraySize, sizeof(SessionTimeEntry)),
            0,
            mul_size(SessionTimeArraySize, sizeof(SessionTimeEntry)));
        securec_check(rc, "\0", "\0");
    }
}

void initMySessionTimeEntry(void)
{
    /* Initialize mySessionTimeEntry */
    Assert(t_thrd.shemem_ptr_cxt.sessionTimeArray);

    // backend thread
    if (t_thrd.proc_cxt.MyBackendId != InvalidBackendId) {
        Assert(t_thrd.proc_cxt.MyBackendId >= 1 &&
               t_thrd.proc_cxt.MyBackendId <= (g_instance.attr.attr_common.enable_thread_pool
                                                      ? GLOBAL_RESERVE_SESSION_NUM
                                                      : g_instance.shmem_cxt.MaxBackends));
        t_thrd.shemem_ptr_cxt.mySessionTimeEntry =
            &t_thrd.shemem_ptr_cxt.sessionTimeArray[t_thrd.proc_cxt.MyBackendId - 1];
    } else {
        // Auxiliary thread
        int index = GetAuxProcStatEntryIndex();
        t_thrd.shemem_ptr_cxt.mySessionTimeEntry = &t_thrd.shemem_ptr_cxt.sessionTimeArray[index];
    }

    /* ensure the changeCount will not go wrong, when we start to record this entry. */
    do {
        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->changeCount++;
    } while ((t_thrd.shemem_ptr_cxt.mySessionTimeEntry->changeCount & 1) == 0);

    /* reset my entry. */
    t_thrd.shemem_ptr_cxt.mySessionTimeEntry->isActive = true;

    if (!ENABLE_THREAD_POOL)
        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->sessionid = t_thrd.proc_cxt.MyProcPid;
    else {
        if (IS_THREAD_POOL_WORKER && u_sess->session_id > 0)
            t_thrd.shemem_ptr_cxt.mySessionTimeEntry->sessionid = u_sess->session_id;
        else
            t_thrd.shemem_ptr_cxt.mySessionTimeEntry->sessionid = t_thrd.proc_cxt.MyProcPid;
    }

    t_thrd.shemem_ptr_cxt.mySessionTimeEntry->myStartTime = t_thrd.proc_cxt.MyStartTime;

    errno_t rc = memset_s((int64*)t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array,
        sizeof(t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array),
        0,
        sizeof(t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array));
    securec_check(rc, "\0", "\0");

    t_thrd.shemem_ptr_cxt.mySessionTimeEntry->changeCount++;
    Assert((t_thrd.shemem_ptr_cxt.mySessionTimeEntry->changeCount & 1) == 0);

    on_shmem_exit(endMySessionTimeEntry, 0);
}

static void DetachMySessionTimeEntry(volatile SessionTimeEntry* pEntry)
{
    LWLockAcquire(InstanceTimeLock, LW_EXCLUSIVE);

    pEntry->changeCount++;

    /* save logout session's timeinfo into gInstanceTimeInfo */
    for (uint32 idx = 0; idx < TOTAL_TIME_INFO_TYPES; idx++) {
        g_instance.stat_cxt.gInstanceTimeInfo[idx] += pEntry->array[idx];
    }

    /* mark my entry not active. */
    pEntry->isActive = false;

    pEntry->changeCount++;
    Assert((pEntry->changeCount & 1) == 0);

    LWLockRelease(InstanceTimeLock);
}

/*
 * mark t_thrd.shemem_ptr_cxt.mySessionTimeEntry closed when we shut down.
 */
static void endMySessionTimeEntry(int code, Datum arg)
{
    DetachMySessionTimeEntry(t_thrd.shemem_ptr_cxt.mySessionTimeEntry);

    /* don't bother this entry any more. */
    t_thrd.shemem_ptr_cxt.mySessionTimeEntry = NULL;
}

/*
 * attach mySessionTimeEntry to session entry
 */
void AttachMySessionTimeEntry(void)
{
    Assert(t_thrd.shemem_ptr_cxt.sessionTimeArray);
    Assert(u_sess->session_ctr_index >= GLOBAL_RESERVE_SESSION_NUM && u_sess->session_ctr_index < MAX_BACKEND_SLOT);
    Assert(t_thrd.proc_cxt.MyBackendId != InvalidBackendId);
    Assert(t_thrd.shemem_ptr_cxt.mySessionTimeEntry ==
           &t_thrd.shemem_ptr_cxt.sessionTimeArray[t_thrd.proc_cxt.MyBackendId - 1]);

    /* change stat object to session */
    t_thrd.shemem_ptr_cxt.mySessionTimeEntry = &t_thrd.shemem_ptr_cxt.sessionTimeArray[u_sess->session_ctr_index];

    /* ensure the changeCount will not go wrong, when we start to record this entry. */
    do {
        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->changeCount++;
    } while ((t_thrd.shemem_ptr_cxt.mySessionTimeEntry->changeCount & 1) == 0);

    /* initialize entry if necessary */
    if (t_thrd.shemem_ptr_cxt.mySessionTimeEntry->isActive == false) {
        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->myStartTime =
            timestamptz_to_time_t(u_sess->proc_cxt.MyProcPort->SessionStartTime);
        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->sessionid = u_sess->session_id;
        errno_t rc = memset_s((int64*)t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array,
            sizeof(t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array),
            0,
            sizeof(t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array));
        securec_check(rc, "\0", "\0");
        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->isActive = true;
    }

    t_thrd.shemem_ptr_cxt.mySessionTimeEntry->changeCount++;
    Assert((t_thrd.shemem_ptr_cxt.mySessionTimeEntry->changeCount & 1) == 0);
}

void timeInfoRecordStart(void)
{
    if (u_sess->stat_cxt.localTimeInfoArray[DB_TIME] == 0) {
        u_sess->stat_cxt.localTimeInfoArray[DB_TIME] = GetCurrentTimestamp();
        if (u_sess->attr.attr_common.enable_instr_cpu_timer)
            u_sess->stat_cxt.localTimeInfoArray[CPU_TIME] = getCpuTime();
    }
}

void timeInfoRecordEnd(void)
{
    errno_t rc;
    if (u_sess->stat_cxt.localTimeInfoArray[DB_TIME] != 0) {
        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->changeCount++;

        if (u_sess->attr.attr_common.enable_instr_cpu_timer) {
            int64 cur = getCpuTime();

            u_sess->stat_cxt.localTimeInfoArray[CPU_TIME] = cur - u_sess->stat_cxt.localTimeInfoArray[CPU_TIME];
        }
        u_sess->stat_cxt.localTimeInfoArray[DB_TIME] =
            GetCurrentTimestamp() - u_sess->stat_cxt.localTimeInfoArray[DB_TIME];

        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array[CPU_TIME] += u_sess->stat_cxt.localTimeInfoArray[CPU_TIME];
        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array[DB_TIME] += u_sess->stat_cxt.localTimeInfoArray[DB_TIME];

        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array[EXECUTION_TIME] +=
            u_sess->stat_cxt.localTimeInfoArray[EXECUTION_TIME];
        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array[PARSE_TIME] += u_sess->stat_cxt.localTimeInfoArray[PARSE_TIME];
        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array[PLAN_TIME] += u_sess->stat_cxt.localTimeInfoArray[PLAN_TIME];
        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array[REWRITE_TIME] +=
            u_sess->stat_cxt.localTimeInfoArray[REWRITE_TIME];

        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array[PL_EXECUTION_TIME] +=
            u_sess->stat_cxt.localTimeInfoArray[PL_EXECUTION_TIME];
        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array[PL_COMPILATION_TIME] +=
            u_sess->stat_cxt.localTimeInfoArray[PL_COMPILATION_TIME];

        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array[NET_SEND_TIME] +=
            u_sess->stat_cxt.localTimeInfoArray[NET_SEND_TIME];
        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->array[DATA_IO_TIME] +=
            u_sess->stat_cxt.localTimeInfoArray[DATA_IO_TIME];

        t_thrd.shemem_ptr_cxt.mySessionTimeEntry->changeCount++;
        Assert((t_thrd.shemem_ptr_cxt.mySessionTimeEntry->changeCount & 1) == 0);

        UniqueSQLStat sqlStat;
        sqlStat.timeInfo = u_sess->stat_cxt.localTimeInfoArray;
        sqlStat.netInfo = u_sess->stat_cxt.localNetInfo;
        if (u_sess->unique_sql_cxt.unique_sql_id != 0 && is_unique_sql_enabled())
            UpdateUniqueSQLStat(NULL, NULL, 0, NULL, &sqlStat);
        rc = memset_s(u_sess->stat_cxt.localTimeInfoArray,
            sizeof(int64) * TOTAL_TIME_INFO_TYPES,
            0,
            sizeof(int64) * TOTAL_TIME_INFO_TYPES);
        securec_check(rc, "\0", "\0");
        rc = memset_s(u_sess->stat_cxt.localNetInfo,
            sizeof(uint64) * TOTAL_NET_INFO_TYPES,
            0,
            sizeof(uint64) * TOTAL_NET_INFO_TYPES);
        securec_check(rc, "\0", "\0");
    }
}

/* generate result of pv_session_time view */
void getSessionTimeStatus(Tuplestorestate *tupStore, TupleDesc tupDesc,
    void (*insert)(Tuplestorestate *tupStore, TupleDesc tupDesc, const SessionTimeEntry *entry))
{
    SessionTimeEntry *localEntry = NULL;
    SessionTimeEntry *entry = NULL;
    int entryIndex;

    localEntry = (SessionTimeEntry *)palloc0(sizeof(SessionTimeEntry));
    for (entryIndex = 0; entryIndex < SessionTimeArraySize; entryIndex++) {
        entry = &(t_thrd.shemem_ptr_cxt.sessionTimeArray[entryIndex]);

        READ_AN_ENTRY(localEntry, entry, entry->changeCount, SessionTimeEntry);

        if (!localEntry->isActive)
            continue;

        insert(tupStore, tupDesc, localEntry);
    }
    pfree(localEntry);
}

/* generate result of pv_instance_time view */
SessionTimeEntry* getInstanceTimeStatus()
{
    SessionTimeEntry* retEntry = NULL;
    SessionTimeEntry localEntry;
    SessionTimeEntry* entry = NULL;
    int entryIndex, idx;

    retEntry = (SessionTimeEntry*)palloc0(sizeof(SessionTimeEntry));
    LWLockAcquire(InstanceTimeLock, LW_SHARED);
    for (entryIndex = 0; entryIndex < SessionTimeArraySize; entryIndex++) {
        entry = &(t_thrd.shemem_ptr_cxt.sessionTimeArray[entryIndex]);
        if (entry->isActive) {
            READ_AN_ENTRY(&localEntry, entry, entry->changeCount, SessionTimeEntry);
            for (idx = 0; idx < TOTAL_TIME_INFO_TYPES; idx++)
                retEntry->array[idx] += localEntry.array[idx];
        }
    }

    for (idx = 0; idx < TOTAL_TIME_INFO_TYPES; idx++)
        retEntry->array[idx] += g_instance.stat_cxt.gInstanceTimeInfo[idx];

    LWLockRelease(InstanceTimeLock);

    return retEntry;
}

PgStat_RedoEntry redoStatistics;

void reportRedoWrite(PgStat_Counter blks, PgStat_Counter tim)
{
    PgStat_RedoEntry* entry = &redoStatistics;

    if (0 >= blks) {
        return;
    }

    // trace point was protected by this lwlock already.
    entry->writes++;
    entry->writeBlks += blks;
    entry->writeTime += tim;
    entry->avgIOTime = entry->writeTime / entry->writes;
    entry->lstIOTime = tim;
    if (entry->minIOTime > tim || 0 == entry->minIOTime) {
        entry->minIOTime = tim;
    }
    if (entry->maxIOTime < tim) {
        entry->maxIOTime = tim;
    }
}

static void endMySessionStatEntry(int code, Datum arg);

Size sessionStatShmemSize(void)
{
    return mul_size(sizeof(SessionLevelStatistic), SessionStatArraySize);
}

void sessionStatShmemInit(void)
{
    bool found = false;
    errno_t rc;

    t_thrd.shemem_ptr_cxt.sessionStatArray =
        (SessionLevelStatistic*)ShmemInitStruct("Session Statistic Array", sessionStatShmemSize(), &found);

    if (!found) {
        rc = memset_s(t_thrd.shemem_ptr_cxt.sessionStatArray, sessionStatShmemSize(), 0, sessionStatShmemSize());
        securec_check(rc, "\0", "\0");
    }
}

void initMySessionStatEntry(void)
{
    /* Initialize mySessionStatEntry */
    Assert(t_thrd.shemem_ptr_cxt.sessionStatArray);

    // backend thread
    if (t_thrd.proc_cxt.MyBackendId != InvalidBackendId) {
        Assert(t_thrd.proc_cxt.MyBackendId >= 1 &&
               t_thrd.proc_cxt.MyBackendId <= (g_instance.attr.attr_common.enable_thread_pool
                                                      ? GLOBAL_RESERVE_SESSION_NUM
                                                      : g_instance.shmem_cxt.MaxBackends));
        t_thrd.shemem_ptr_cxt.mySessionStatEntry =
            &t_thrd.shemem_ptr_cxt.sessionStatArray[t_thrd.proc_cxt.MyBackendId - 1];
    } else {
        // Auxiliary thread
        int index = GetAuxProcStatEntryIndex();
        t_thrd.shemem_ptr_cxt.mySessionStatEntry = &t_thrd.shemem_ptr_cxt.sessionStatArray[index];
    }

    errno_t rc = memset_s((SessionLevelStatistic*)t_thrd.shemem_ptr_cxt.mySessionStatEntry,
        sizeof(SessionLevelStatistic),
        0,
        sizeof(SessionLevelStatistic));
    securec_check(rc, "\0", "\0");

    t_thrd.shemem_ptr_cxt.mySessionStatEntry->sessionStartTime = t_thrd.proc_cxt.MyStartTime;
    if (!ENABLE_THREAD_POOL)
        t_thrd.shemem_ptr_cxt.mySessionStatEntry->sessionid = t_thrd.proc_cxt.MyProcPid;
    else {
        if (IS_THREAD_POOL_WORKER && u_sess->session_id > 0)
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->sessionid = u_sess->session_id;
        else
            t_thrd.shemem_ptr_cxt.mySessionStatEntry->sessionid = t_thrd.proc_cxt.MyProcPid;
    }
    t_thrd.shemem_ptr_cxt.mySessionStatEntry->isValid = true;

    on_shmem_exit(endMySessionStatEntry, 0);
}

static void endMySessionStatEntry(int code, Datum arg)
{
    /* mark my entry not active. */
    t_thrd.shemem_ptr_cxt.mySessionStatEntry->isValid = false;

    /* don't bother this entry any more. */
    t_thrd.shemem_ptr_cxt.mySessionStatEntry = NULL;
}

/*
 * attach mySessionStatEntry to session entry
 */
void AttachMySessionStatEntry(void)
{
    Assert(t_thrd.shemem_ptr_cxt.sessionStatArray);
    Assert(u_sess->session_ctr_index >= GLOBAL_RESERVE_SESSION_NUM && u_sess->session_ctr_index < MAX_BACKEND_SLOT);
    Assert(t_thrd.proc_cxt.MyBackendId != InvalidBackendId);
    Assert(t_thrd.shemem_ptr_cxt.mySessionStatEntry ==
           &t_thrd.shemem_ptr_cxt.sessionStatArray[t_thrd.proc_cxt.MyBackendId - 1]);

    /* change stat object to session */
    t_thrd.shemem_ptr_cxt.mySessionStatEntry = &t_thrd.shemem_ptr_cxt.sessionStatArray[u_sess->session_ctr_index];

    /* initialize entry if necessary */
    if (t_thrd.shemem_ptr_cxt.mySessionStatEntry->isValid == false) {
        t_thrd.shemem_ptr_cxt.mySessionStatEntry->sessionStartTime =
            timestamptz_to_time_t(u_sess->proc_cxt.MyProcPort->SessionStartTime);
        t_thrd.shemem_ptr_cxt.mySessionStatEntry->sessionid = u_sess->session_id;
        t_thrd.shemem_ptr_cxt.mySessionStatEntry->isValid = true;
    }
}

/* generate result of pv_session_stat view */
void getSessionStatistics(Tuplestorestate* tupStore, TupleDesc tupDesc,
    void (* insert)(Tuplestorestate* tupStore, TupleDesc tupDesc, const SessionLevelStatistic* entry))
{
    SessionLevelStatistic* entry = NULL;
    SessionLevelStatistic* localEntry = NULL;

    int entryIndex;
    errno_t rc;

    localEntry = (SessionLevelStatistic*)palloc0(sizeof(SessionLevelStatistic));

    for (entryIndex = 0; entryIndex < SessionStatArraySize; entryIndex++) {
        entry = &(t_thrd.shemem_ptr_cxt.sessionStatArray[entryIndex]);
        rc = memcpy_s(localEntry, sizeof(SessionLevelStatistic), entry, sizeof(SessionLevelStatistic));
        securec_check(rc, "\0", "\0");

        if (!localEntry->isValid)
            continue;
        insert(tupStore, tupDesc, localEntry);
    }
    pfree(localEntry);
}

PgStat_FileEntry pgStatFileArray[NUM_FILES];
uint32 fileStatCount = 0;

void reportFileStat(PgStat_MsgFile* msg)
{
    pgstat_setheader(&(msg->m_hdr), PGSTAT_MTYPE_FILE);
    pgstat_send(msg, sizeof(PgStat_MsgFile));
}

static void pgstat_recv_filestat(PgStat_MsgFile* msg, int len)
{
    PgStat_FileEntry* entry = NULL;
    int i;
    errno_t rc;

    if (0 >= msg->blks) {
        return;
    }

    LWLockAcquire(FileStatLock, LW_SHARED);
    for (i = 0; i < NUM_FILES; i++) {
        entry = (PgStat_FileEntry*)&pgStatFileArray[i];
        if (entry->fn == msg->fn || entry->fn == InvalidOid) {
            break;
        }
    }
    LWLockRelease(FileStatLock);

    if (i == NUM_FILES || entry->fn == InvalidOid) {
        LWLockAcquire(FileStatLock, LW_EXCLUSIVE);
        entry = (PgStat_FileEntry*)&pgStatFileArray[fileStatCount];

        /* reset this entry */
        rc = memset_s(entry, sizeof(PgStat_FileEntry), 0, sizeof(PgStat_FileEntry));
        securec_check(rc, "\0", "\0");

        entry->dbid = msg->dbid;
        entry->spcid = msg->spcid;
        entry->fn = msg->fn;

        fileStatCount = (fileStatCount + 1) % NUM_FILES;
        LWLockRelease(FileStatLock);
    }

    entry->changeCount++;
    g_instance.stat_cxt.fileIOStat->changeCount++;

    if ('r' == msg->rw) {
        entry->reads += msg->cnt;
        entry->readBlks += msg->blks;
        entry->readTime += msg->tim;
        g_instance.stat_cxt.fileIOStat->reads += msg->cnt;
        g_instance.stat_cxt.fileIOStat->readBlks += msg->blks;
    } else {
        entry->writes += msg->cnt;
        entry->writeBlks += msg->blks;
        entry->writeTime += msg->tim;
        g_instance.stat_cxt.fileIOStat->writes += msg->cnt;
        g_instance.stat_cxt.fileIOStat->writeBlks += msg->blks;
    }
    g_instance.stat_cxt.fileIOStat->changeCount++;

    if ((entry->reads + entry->writes) <= 0) {
        entry->avgIOTime = 0;
    } else {
        entry->avgIOTime = (entry->readTime + entry->writeTime) / (entry->reads + entry->writes);
    }

    entry->lstIOTime = msg->lsttim;
    if (entry->minIOTime > msg->mintim || 0 == entry->minIOTime) {
        entry->minIOTime = msg->mintim;
    }
    if (entry->maxIOTime < msg->maxtim) {
        entry->maxIOTime = msg->maxtim;
    }

    entry->changeCount++;

    return;
}

/* clear key message that may appear in core file for security */
PgBackendStatus* GetMyBEEntry(void)
{
    return t_thrd.shemem_ptr_cxt.MyBEEntry;
}

static void endMySessionMemoryEntry(int code, Datum arg);

Size sessionMemoryShmemSize(void)
{
    return mul_size(sizeof(SessionLevelMemory), SessionMemoryArraySize);
}

void sessionMemoryShmemInit(void)
{
    bool found = false;
    errno_t rc;

    t_thrd.shemem_ptr_cxt.sessionMemoryArray =
        (SessionLevelMemory*)ShmemInitStruct("Session Memory Array", sessionMemoryShmemSize(), &found);

    if (!found) {
        rc = memset_s(t_thrd.shemem_ptr_cxt.sessionMemoryArray, sessionMemoryShmemSize(), 0, sessionMemoryShmemSize());
        securec_check(rc, "\0", "\0");
    }
}

void initMySessionMemoryEntry(void)
{
    /* Initialize mySessionStatEntry */
    Assert(t_thrd.shemem_ptr_cxt.sessionMemoryArray);

    // backend thread
    if (t_thrd.proc_cxt.MyBackendId != InvalidBackendId) {
        Assert(t_thrd.proc_cxt.MyBackendId >= 1 &&
               t_thrd.proc_cxt.MyBackendId <= (g_instance.attr.attr_common.enable_thread_pool
                                                      ? GLOBAL_RESERVE_SESSION_NUM
                                                      : g_instance.shmem_cxt.MaxBackends));
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry =
            &t_thrd.shemem_ptr_cxt.sessionMemoryArray[t_thrd.proc_cxt.MyBackendId - 1];
    } else {
        // Auxiliary thread
        int index = GetAuxProcStatEntryIndex();
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry = &t_thrd.shemem_ptr_cxt.sessionMemoryArray[index];
    }

    errno_t rc = EOK;
    rc =
        memset_s(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry, sizeof(SessionLevelMemory), 0, sizeof(SessionLevelMemory));
    securec_check(rc, "\0", "\0");
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->threadStartTime = t_thrd.proc_cxt.MyStartTime;
    if (!ENABLE_THREAD_POOL)
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->sessionid = t_thrd.proc_cxt.MyProcPid;
    else {
        if (IS_THREAD_POOL_WORKER && u_sess->session_id > 0)
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->sessionid = u_sess->session_id;
        else
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->sessionid = t_thrd.proc_cxt.MyProcPid;
    }
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->queryMemInChunks = t_thrd.utils_cxt.trackedMemChunks;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->initMemInChunks = t_thrd.utils_cxt.trackedMemChunks;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->peakChunksQuery = t_thrd.utils_cxt.trackedMemChunks;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->spillCount = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->spillSize = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->broadcastSize = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->estimate_time = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->estimate_memory = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->warning = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan = NULL;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->plan_size = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan_issue = NULL;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->dnStartTime = GetCurrentTimestamp();
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->dnEndTime = 0;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->isValid = true;

    on_shmem_exit(endMySessionMemoryEntry, 0);
}

static void endMySessionMemoryEntry(int code, Datum arg)
{

    /* release the memory on mySessionMemoryEntry */
    pgstat_release_session_memory_entry();

    /* mark my entry not active. */
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->isValid = false;

    /* don't bother this entry any more. */
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry = NULL;
}

/*
 * attach mySessionMemoryEntry to session entry
 */
void AttachMySessionMemoryEntry(void)
{
    Assert(t_thrd.shemem_ptr_cxt.sessionMemoryArray);
    Assert(u_sess->session_ctr_index >= GLOBAL_RESERVE_SESSION_NUM && u_sess->session_ctr_index < MAX_BACKEND_SLOT);
    Assert(t_thrd.proc_cxt.MyBackendId != InvalidBackendId);
    Assert(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry ==
           &t_thrd.shemem_ptr_cxt.sessionMemoryArray[t_thrd.proc_cxt.MyBackendId - 1]);

    /* change stat object to session */
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry = &t_thrd.shemem_ptr_cxt.sessionMemoryArray[u_sess->session_ctr_index];

    /* initialize entry if necessary */
    if (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->isValid == false) {
        errno_t rc = EOK;
        rc = memset_s(
            t_thrd.shemem_ptr_cxt.mySessionMemoryEntry, sizeof(SessionLevelMemory), 0, sizeof(SessionLevelMemory));
        securec_check(rc, "\0", "\0");
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->threadStartTime =
            timestamptz_to_time_t(u_sess->proc_cxt.MyProcPort->SessionStartTime);
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->sessionid = u_sess->session_id;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->queryMemInChunks = t_thrd.utils_cxt.trackedMemChunks;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->initMemInChunks = t_thrd.utils_cxt.trackedMemChunks;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->peakChunksQuery = t_thrd.utils_cxt.trackedMemChunks;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->spillCount = 0;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->spillSize = 0;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->broadcastSize = 0;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->estimate_time = 0;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->estimate_memory = 0;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->warning = 0;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan = NULL;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->plan_size = 0;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan_issue = NULL;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->dnStartTime = GetCurrentTimestamp();
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->dnEndTime = 0;
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->isValid = true;
    }

    /* worker's pgproc memory info is also related to the session */
    t_thrd.proc->sessMemorySessionid = u_sess->session_id;
}

/* generate result of pv_session_stat view */
void getSessionMemory(Tuplestorestate* tupStore, TupleDesc tupDesc,
                      void (* insert)(Tuplestorestate* tupStore, TupleDesc tupDesc, const SessionLevelMemory* entry))
{
    SessionLevelMemory* localEntry = NULL;
    SessionLevelMemory* entry = NULL;
    int entryIndex;
    errno_t rc;

    localEntry = (SessionLevelMemory*)palloc0(sizeof(SessionLevelMemory));
    for (entryIndex = 0; entryIndex < SessionMemoryArraySize; entryIndex++) {
        entry = &(t_thrd.shemem_ptr_cxt.sessionMemoryArray[entryIndex]);
        rc = memcpy_s(localEntry, sizeof(SessionLevelMemory), entry, sizeof(SessionLevelMemory));
        securec_check(rc, "\0", "\0");

        if (false == localEntry->isValid || 0 == (localEntry->peakChunksQuery - localEntry->initMemInChunks))
            continue;

        insert(tupStore, tupDesc, localEntry);
    }

    pfree(localEntry);
}

typedef AllocSetContext* AllocSet;

static void calculateThreadMemoryContextStats(const volatile PGPROC* proc, const MemoryContext context, bool isShared,
    Tuplestorestate* tupStore, TupleDesc tupDesc)
{
    AllocSet set = (AllocSet)context;

    if (!isShared) {
        char sessId[SESSION_ID_LEN] = {0};
        ThreadId threadId;
        char threadType[PROC_NAME_LEN] = {0};
        errno_t rc;

        if (proc != NULL) {
            threadId = proc->pid;
            rc = strncpy_s(threadType,
                PROC_NAME_LEN,
                (proc->myProgName != NULL) ? (const char*)proc->myProgName : "",
                PROC_NAME_LEN - 1);
            securec_check(rc, "\0", "\0");
            getSessionID(sessId, proc->myStartTime, threadId);
        } else {
            threadId = PostmasterPid;
            rc = strncpy_s(threadType, PROC_NAME_LEN, "postmaster", PROC_NAME_LEN - 1);
            securec_check(rc, "\0", "\0");
            getSessionID(sessId, (pg_time_t)0, threadId);
        }

        Datum values[NUM_THREAD_MEMORY_DETAIL_ELEM] = {0};
        bool nulls[NUM_THREAD_MEMORY_DETAIL_ELEM] = {false};

        values[0] = CStringGetTextDatum(sessId);
        values[1] = Int64GetDatum(threadId);
        values[2] = CStringGetTextDatum(threadType);
        values[3] = CStringGetTextDatum(context->name);
        values[4] = Int16GetDatum(context->level);
        if (context->level > 0 && context->parent != NULL)
            values[5] = CStringGetTextDatum(context->parent->name);
        else
            nulls[5] = true;
        values[6] = Int64GetDatum(set->totalSpace);
        values[7] = Int64GetDatum(set->freeSpace);
        values[8] = Int64GetDatum(set->totalSpace - set->freeSpace);

        tuplestore_putvalues(tupStore, tupDesc, values, nulls);
    } else {
        Datum values[NUM_SHARED_MEMORY_DETAIL_ELEM] = {0};
        bool nulls[NUM_SHARED_MEMORY_DETAIL_ELEM] = {false};

        values[0] = CStringGetTextDatum(context->name);
        values[1] = Int16GetDatum(context->level);
        if (context->level > 0 && context->parent != NULL)
            values[2] = CStringGetTextDatum(context->parent->name);
        else
            nulls[2] = true;
        values[3] = Int64GetDatum(set->totalSpace);
        values[4] = Int64GetDatum(set->freeSpace);
        values[5] = Int64GetDatum(set->totalSpace - set->freeSpace);

        tuplestore_putvalues(tupStore, tupDesc, values, nulls);
    }
}

static void recursiveThreadMemoryContext(const volatile PGPROC* proc, const MemoryContext context, bool isShared,
    Tuplestorestate* tupStore, TupleDesc tupDesc)
{
    MemoryContext child;

    bool checkLock = false;
    PG_TRY();
    {
        /*check for pending interrupts before waiting.*/
        CHECK_FOR_INTERRUPTS();

        if (isShared) {
            MemoryContextLock(context);
            checkLock = true;
        }

        /* calculate MemoryContext Stats */
        calculateThreadMemoryContextStats(proc, context, isShared, tupStore, tupDesc);

        /* recursive MemoryContext's child */
        for (child = context->firstchild; child != NULL; child = child->nextchild) {
            if (child->is_shared == isShared) {
                recursiveThreadMemoryContext(proc, child, isShared, tupStore, tupDesc);
            }
        }
    }
    PG_CATCH();
    {
        if (isShared && checkLock) {
            MemoryContextUnlock(context);
        }

        PG_RE_THROW();
    }
    PG_END_TRY();

    if (isShared) {
        MemoryContextUnlock(context);
    }
}

/*
 * @@GaussDB@@
 * Target		: pv_shared_memory_detail view
 * Brief		:
 * Description	:
 */
void getSharedMemoryDetail(Tuplestorestate* tupStore, TupleDesc tupDesc)
{
    recursiveThreadMemoryContext(NULL, g_instance.instance_context, true, tupStore, tupDesc);
}

/*
 * @@GaussDB@@
 * Target       : pv_thread_memory_detail view
 * Brief        :
 * Description  :
 * Notes        :
 * Author       :
 */
void getThreadMemoryDetail(Tuplestorestate* tupStore, TupleDesc tupDesc, uint32* procIdx)
{
    uint32 max_thread_count = g_instance.proc_base->allProcCount -
        g_instance.attr.attr_storage.max_prepared_xacts * NUM_TWOPHASE_PARTITIONS;
    volatile PGPROC* proc = NULL;
    uint32 idx = 0;

    PG_TRY();
    {
        HOLD_INTERRUPTS();

        for (idx = 0; idx < max_thread_count; idx++) {
            proc = g_instance.proc_base_all_procs[idx];
            *procIdx = idx;

            /* lock this proc's delete MemoryContext action */
            (void)syscalllockAcquire(&((PGPROC*)proc)->deleMemContextMutex);
            if (proc->topmcxt != NULL)
                recursiveThreadMemoryContext(proc, proc->topmcxt, false, tupStore, tupDesc);
            (void)syscalllockRelease(&((PGPROC*)proc)->deleMemContextMutex);
        }

        /* get memory context detail from postmaster thread */
        if (IsNormalProcessingMode())
            recursiveThreadMemoryContext(NULL, PmTopMemoryContext, false, tupStore, tupDesc);

        RESUME_INTERRUPTS();
    }
    PG_CATCH();
    {
        if (*procIdx < max_thread_count) {
            proc = g_instance.proc_base_all_procs[*procIdx];
            (void)syscalllockRelease(&((PGPROC*)proc)->deleMemContextMutex);
        }
        PG_RE_THROW();
    }
    PG_END_TRY();
}

////////////////////////////////////////////////////////////////////////////////
#ifdef MEMORY_CONTEXT_CHECKING

extern void dumpAllocBlock(AllocSet set, StringInfoData* buf);
extern void dumpAsanBlock(AsanSet set, StringInfoData* buf);

static void recursiveMemoryContextForDump(const MemoryContext context, char* ctx_name, StringInfoData* buf)
{
#ifndef ENABLE_MEMORY_CHECK
    if (!strcmp(ctx_name, context->name) &&
        (context->type == T_AllocSetContext || context->type == T_SharedAllocSetContext))
        dumpAllocBlock((AllocSet)context, buf);
#else
    if ((context->type == T_AsanSetContext || context->type == T_SharedAllocSetContext)) {
        appendStringInfo(buf, "context : %s\n", context->name);
        dumpAsanBlock((AsanSet)context, buf);
    }
#endif
    /* recursive MemoryContext's child */
    for (MemoryContext child = context->firstchild; child != NULL; child = child->nextchild) {
        recursiveMemoryContextForDump(child, ctx_name, buf);
    }

    return;
}

void DumpMemoryContext(DUMP_TYPE type)
{
#define DUMPFILE_BUFF_SIZE 128
    char ctx_name[MEMORY_CONTEXT_NAME_LEN] = {0};
    char dump_dir[MAX_PATH_LEN] = {0};
    errno_t ss_rc;
    bool is_absolute = false;

    // get the path of dump file
    is_absolute = is_absolute_path(u_sess->attr.attr_common.Log_directory);
    if (is_absolute) {
        ss_rc = snprintf_s(
            dump_dir, sizeof(dump_dir), sizeof(dump_dir) - 1, "%s/memdump", u_sess->attr.attr_common.Log_directory);
        securec_check_ss(ss_rc, "\0", "\0");
    } else {
        ss_rc = snprintf_s(dump_dir,
            sizeof(dump_dir),
            sizeof(dump_dir) - 1,
            "%s/pg_log/memdump",
            g_instance.attr.attr_common.data_directory);
        securec_check_ss(ss_rc, "\0", "\0");
    }

    // 1. copy the name of memory context to thread local variable
    ss_rc = strcpy_s(ctx_name, MEMORY_CONTEXT_NAME_LEN, dump_memory_context_name);
    securec_check(ss_rc, "\0", "\0");

    // 2. create directory "/tmp/dumpmem" to save memory context log file
    int ret;
    struct stat info;
    if (stat(dump_dir, &info) == 0) {
        if (!S_ISDIR(info.st_mode)) {
            // S_ISDIR() doesn't exist on my windows
            elog(LOG, "%s maybe not a directory.", dump_dir);
            return;
        }
    } else {
        ret = mkdir(dump_dir, S_IRWXU);
        if (ret < 0) {
            elog(LOG, "Fail to create %s", dump_dir);
            return;
        }
    }

    // 3. create file to be dump
    // file name
    char dump_file[MAX_PATH_LEN] = {0};
#ifdef ENABLE_MEMORY_CHECK
    pid_t pid = getpid();
    int rc = sprintf_s(dump_file,
        MAX_PATH_LEN,
        "%s/%s_%s_%d.log",
        dump_dir,
        g_instance.attr.attr_common.PGXCNodeName,
        g_instance.instance_context->name,
        pid);
#else
    ThreadId tid = gs_thread_self();
    int rc = sprintf_s(
        dump_file, MAX_PATH_LEN, "%s/%s_%lu_%lu.log", dump_dir, ctx_name, (unsigned long)tid, (uint64)time(NULL));
#endif
    securec_check_ss(rc, "\0", "\0");
    FILE* dump_fp = fopen(dump_file, "w");
    if (NULL == dump_fp) {
        elog(LOG, "dump_memory: Failed to create file: %s, cause: %s", dump_file, strerror(errno));
        return;
    }

    MemoryContext ctx;

    switch (type) {
        case STANDARD_DUMP:
            ctx = t_thrd.top_mem_cxt;
            break;
        case SHARED_DUMP:
            ctx = g_instance.instance_context;
            break;
        default:
            elog(LOG, "dump_memory: invalid dump type: %d", type);
            fclose(dump_fp);
            return;
    }

    // 4. walk all memory context
    PG_TRY();
    {
        StringInfoData memBuf;

        initStringInfo(&memBuf);

        recursiveMemoryContextForDump(ctx, ctx_name, &memBuf);

        uint64 bytes = fwrite(memBuf.data, 1, memBuf.len, dump_fp);

        if (bytes != (uint64)memBuf.len) {
            elog(LOG, "Could not write memory usage information. Attempted to write %d", memBuf.len);
        }

        pfree(memBuf.data);
    }
    PG_CATCH();
    {
        fclose(dump_fp);
        return;
    }
    PG_END_TRY();

    fclose(dump_fp);
    return;
}
#endif  // MEMORY_CONTEXT_CHECKING

static void FetchLWLockInfoOfAllBackends(lwm_lwlocks** lwlockInfo, int& nBackends)
{
    nBackends = BackendStatusArray_size;
    *lwlockInfo = (lwm_lwlocks*)palloc0(sizeof(lwm_lwlocks) * nBackends);

    lwm_lwlocks* lwlock = *lwlockInfo;

    for (int n = 0; n < nBackends; ++n) {
        volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + n;

        /* we must guarantee atomic for set access flag */
        START_CRIT_SECTION();

        /* skip beentry when it is cleaning */
        beentry->st_lw_access_flag = true;
        pg_memory_barrier();

        if (beentry->st_lw_is_cleanning_flag || beentry->st_procpid == 0) {
            beentry->st_lw_access_flag = false;
            END_CRIT_SECTION();

            /* next backend entry */
            ++beentry;
            continue;
        }

        for (;;) {
            int before_changecount;
            int after_changecount;

            pgstat_save_changecount_before(beentry, before_changecount);
            if (beentry->lw_held_num == NULL) {
                break;
            }

            lwlock->lwlocks_num = *(beentry->lw_held_num);
            if (lwlock->lwlocks_num > 0) {
                lwlock->be_tid.thread_id = beentry->st_procpid;
                lwlock->be_tid.st_sessionid = beentry->st_sessionid;
                lwlock->want_lwlock.lock = beentry->lw_want_lock;
                const int needSize = (sizeof(lwlock_id_mode) * lwlock->lwlocks_num);
                if (lwlock->held_lwlocks != NULL) {
                    lwlock->held_lwlocks = (lwlock_id_mode*)repalloc(lwlock->held_lwlocks, needSize);
                } else {
                    lwlock->held_lwlocks = (lwlock_id_mode*)palloc0(needSize);
                }
                copy_held_lwlocks(beentry->lw_held_locks, lwlock->held_lwlocks, lwlock->lwlocks_num);

                pgstat_save_changecount_after(beentry, after_changecount);
                if (before_changecount == after_changecount && (before_changecount & 1) == 0) {
                    /* valid lwlock information is consecutive */
                    ++lwlock;
                    break;
                }
            } else {
                break;
            }
        }

        pg_memory_barrier();
        beentry->st_lw_access_flag = false;

        END_CRIT_SECTION();

        /* next backend entry */
        ++beentry;
    }
    nBackends = (lwlock - *lwlockInfo);
}

static void OutLogLWLockInfoOfAllBackends(lwm_lwlocks* lwlockInfo, int nBackends)
{
    StringInfoData fmtOutLog;
    initStringInfo(&fmtOutLog);

    lwm_lwlocks* info = lwlockInfo;
    for (int beIdx = 0; beIdx < nBackends; ++beIdx) {
        /* format lwlock info of each backend */
        appendStringInfo(&fmtOutLog,
            "[LWLOCK INFO] thread %lu, required (%s), held num %d, locks(name, mode):",
            info->be_tid.thread_id,
            T_NAME(info->want_lwlock.lock),
            info->lwlocks_num);
        for (int lockIdx = 0; lockIdx < info->lwlocks_num; ++lockIdx) {
            appendStringInfo(&fmtOutLog,
                "(%s, %d)",
                T_NAME(info->held_lwlocks[lockIdx].lock_addr.lock),
                info->held_lwlocks[lockIdx].lock_sx);
        }
        /* print lwlock info of each backend */
        ereport(LOG, (errmsg("%s", fmtOutLog.data)));
        resetStringInfo(&fmtOutLog);

        ++info;
    }

    pfree(fmtOutLog.data);
}

static void FreeLWLockInfoOfAllBackends(lwm_lwlocks* lwlockInfo, int nBackends)
{
    lwm_lwlocks* info = lwlockInfo;
    for (int i = 0; i < nBackends; ++i) {
        pfree(info->held_lwlocks);
        ++info;
    }
    pfree(lwlockInfo);
}

void DumpLWLockInfoToServerLog(void)
{
    lwm_lwlocks* lwlockInfo = NULL;
    int nBackends = 0;
    FetchLWLockInfoOfAllBackends(&lwlockInfo, nBackends);
    OutLogLWLockInfoOfAllBackends(lwlockInfo, nBackends);
    FreeLWLockInfoOfAllBackends(lwlockInfo, nBackends);
}

/*
 * read light-weight detect data from backends data.
 */
lwm_light_detect* pgstat_read_light_detect(void)
{
    const int n = BackendStatusArray_size;
    /* reset all this memory be 0 */
    lwm_light_detect* lw_detect = (lwm_light_detect*)palloc0(sizeof(lwm_light_detect) * n);
    lwm_light_detect* lw_det = lw_detect;
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray;

    for (int i = 0; i < n; i++) {
        lwm_light_detect ld = {0, 0};

        for (;;) {
            int before_changecount;
            int after_changecount;

            pgstat_save_changecount_before(beentry, before_changecount);
            /* the only Prerequisites is that thread is valid. */
            if (beentry->st_procpid > 0 || beentry->st_sessionid > 0) {
                /* read thread id and fast count from this backend status. */
                ld.entry_id.thread_id = beentry->st_procpid;
                ld.entry_id.st_sessionid = beentry->st_sessionid;
                ld.lw_count = beentry->lw_count;
            }

            /* the only Prerequisites is that thread is valid. */
            pgstat_save_changecount_after(beentry, after_changecount);
            if (before_changecount == after_changecount && (before_changecount & 1) == 0) {
                break;
            }
        }

        if (ld.entry_id.thread_id != 0 && CHANGECOUNT_IS_EVEN(ld.lw_count)) {
            /*
             * remember the two info only if both the following hold
             * 1) thread id is valid means that backend is running.
             * 2) fast count is even means that backend is waiting lwlock.
             */
            lw_det->entry_id.thread_id = ld.entry_id.thread_id;
            lw_det->entry_id.st_sessionid = ld.entry_id.st_sessionid;
            lw_det->lw_count = ld.lw_count;
        }

        /* advance and handle next backend thread */
        ++beentry;
        ++lw_det;
    }
    return lw_detect;
}

/*
 * read diagnosis data according to the positions and number of candidates.
 * these diagnosis information includes: 1) lwlock to require; 2) thread id;
 * 3) the number and id of lwlocks held by this thread;
 */
lwm_lwlocks* pgstat_read_diagnosis_data(lwm_light_detect* light_det, const int* candidates_pos, int num_candidates)
{
    Size need_size = sizeof(lwm_lwlocks) * (uint32)num_candidates;
    /* Must all these memory be reset be 0 */
    lwm_lwlocks* lwlocks = (lwm_lwlocks*)palloc0(need_size);
    lwm_lwlocks* cur_lwlock = NULL;
    lwm_lwlocks tmplock;
    errno_t errval = memset_s(&tmplock, sizeof(tmplock), 0, sizeof(tmplock));
    securec_check_errval(errval, , LOG);

    cur_lwlock = lwlocks;
    for (int i = 0; i < num_candidates; i++) {
        /* remember its position */
        tmplock.be_idx = candidates_pos[i];
        tmplock.held_lwlocks = (lwlock_id_mode*)palloc0(get_held_lwlocks_maxnum() * sizeof(lwlock_id_mode));

        volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + candidates_pos[i];
        lwm_light_detect* cur_det = light_det + candidates_pos[i];
        bool success = true;

        /* We must guarantee atomic for set access flag */
        START_CRIT_SECTION();

        /* Skip beentry when it is cleanning */
        beentry->st_lw_access_flag = true;
        pg_read_barrier();

        if (beentry->st_lw_is_cleanning_flag) {
            beentry->st_lw_access_flag = false;
            END_CRIT_SECTION();
            continue;
        }

        for (;;) {
            int before_changecount;
            int after_changecount;

            pgstat_save_changecount_before(beentry, before_changecount);
            if (cur_det->entry_id.thread_id == beentry->st_procpid) {
                tmplock.be_tid.thread_id = beentry->st_procpid;
                tmplock.be_tid.st_sessionid = beentry->st_sessionid;
                tmplock.want_lwlock.lock = beentry->lw_want_lock;

                /* the second time to check changecount */
                if (cur_det->lw_count == beentry->lw_count) {
                    tmplock.lwlocks_num = *beentry->lw_held_num;
                    copy_held_lwlocks(beentry->lw_held_locks, tmplock.held_lwlocks, tmplock.lwlocks_num);
                } else {
                    success = false; /* changecount changed */
                    break;
                }
            } else {
                success = false; /* thread id changed */
                break;
            }

            /* the third time to check changecount */
            pgstat_save_changecount_after(beentry, after_changecount);
            if ((cur_det->lw_count == beentry->lw_count) && (before_changecount == after_changecount) &&
                (before_changecount & 1) == 0) {
                break;
            }
        }

        pg_memory_barrier();
        beentry->st_lw_access_flag = false;

        END_CRIT_SECTION();

        if (success) {
            /* remember this candidate */
            cur_lwlock->be_idx = tmplock.be_idx;
            cur_lwlock->be_tid.thread_id = tmplock.be_tid.thread_id;
            cur_lwlock->be_tid.st_sessionid = tmplock.be_tid.st_sessionid;
            cur_lwlock->want_lwlock = tmplock.want_lwlock;
            cur_lwlock->lwlocks_num = tmplock.lwlocks_num;
            cur_lwlock->held_lwlocks = tmplock.held_lwlocks;
        } else {
            pfree_ext(tmplock.held_lwlocks);
        }
        ++cur_lwlock;
    }
    return lwlocks;
}

/* read xact starting time of this thread */
TimestampTz pgstat_read_xact_start_tm(int be_index)
{
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + be_index;
    TimestampTz tmp_xact_tm = 0;

    /* read xact starting time from this backend */
    for (;;) {
        /* just make sure this thread is valid */
        int before_changecount;
        int after_changecount;

        pgstat_save_changecount_before(beentry, before_changecount);
        if (beentry->st_procpid > 0) {
            tmp_xact_tm = beentry->st_xact_start_timestamp;
        }
        pgstat_save_changecount_after(beentry, after_changecount);
        if (before_changecount == after_changecount && (before_changecount & 1) == 0) {
            break;
        }
    }
    return tmp_xact_tm;
}

/*
 * @Description: parellel get table's distribution through ExecRemoteFunctionInParallel in RemoteFunctionResultHandler.
 * @return : distribution info contain function state and tuple slot.
 * NOTICE : the memory palloced in this function should be free outside where it was called.
 */
TableDistributionInfo* getTableDataDistribution(TupleDesc tuple_desc, char* schema_name, char* table_name)
{
    StringInfoData buf;
    TableDistributionInfo* distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called. */
    distribuion_info = (TableDistributionInfo*)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    if (schema_name != NULL && table_name != NULL) {
        /* get only one table's distribution info */
        appendStringInfo(&buf,
            "select                                                         "
            "n.nspname,                                                     "
            "c.relname,                                                     "
            "pg_catalog.pgxc_node_str(),                                    "
            "pg_catalog.pg_table_size(c.oid)                                "
            "from pg_catalog.pg_class c                                     "
            "inner join pg_catalog.pg_namespace n on c.relnamespace = n.oid "
            "where n.nspname = \'%s\' and c.relname=\'%s\'                  ",
            schema_name,
            table_name);
    } else {
        /* get all table's distribution in current database and we only get info about user's table without catalog
         * table. */
        appendStringInfo(&buf,
            "select                                                                                "
            "n.nspname,                                                                            "
            "c.relname,                                                                            "
            "pg_catalog.pgxc_node_str(),                                                           "
            "pg_catalog.pg_table_size(c.oid)                                                       "
            "from pg_class c                                                                       "
            "inner join pg_catalog.pg_namespace n on c.relnamespace = n.oid                        "
            "where c.relkind = 'r' and c.oid > %d and n.nspname <> 'cstore' and n.nspname <> 'pmk';",
            FirstNormalObjectId);
    }

    /* send sql and parallel fetch distribution info from all data nodes */
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_DATANODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    return distribuion_info;
}

TableDistributionInfo* getTableStat(TupleDesc tuple_desc, int dirty_pecent, int n_tuples, char* schema_name)
{
    StringInfoData buf;
    TableDistributionInfo* distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called. */
    distribuion_info = (TableDistributionInfo*)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    /* get all table's stat in current database and we only get info about user's table without catalog table. */
    if (schema_name == NULL)
        appendStringInfo(&buf,
            "select                                                                                "
            "c.relname,                                                                            "
            "n.nspname,                                                                            "
            "pg_stat_get_tuples_inserted(c.oid),                                                   "
            "pg_stat_get_tuples_updated(c.oid),                                                    "
            "pg_stat_get_tuples_deleted(c.oid),                                                    "
            "pg_stat_get_live_tuples(c.oid) n_live_tuples,                                         "
            "pg_stat_get_dead_tuples(c.oid) n_dead_tuples                                          "
            "from pg_class c                                                                       "
            "left join pg_namespace n on (c.relnamespace = n.oid)                                  "
            "where c.relkind = 'r' and relpersistence <> 't'                                       "
            "and cast((n_dead_tuples)/(n_live_tuples+n_dead_tuples+0.00001) * 100                  "
            "as numeric(5,2)) >= %d                                                                "
            "and (n_live_tuples+n_dead_tuples) >= %d                                               "
            "and n.nspname not in ('pg_toast','information_schema','cstore','pmk');                ",
            dirty_pecent,
            n_tuples);
    else
        appendStringInfo(&buf,
            "select                                                                                "
            "c.relname,                                                                            "
            "n.nspname,                                                                            "
            "pg_stat_get_tuples_inserted(c.oid),                                                   "
            "pg_stat_get_tuples_updated(c.oid),                                                    "
            "pg_stat_get_tuples_deleted(c.oid),                                                    "
            "pg_stat_get_live_tuples(c.oid) n_live_tuples,                                         "
            "pg_stat_get_dead_tuples(c.oid) n_dead_tuples                                          "
            "from pg_class c                                                                       "
            "left join pg_namespace n on (c.relnamespace = n.oid)                                  "
            "where c.relkind = 'r' and relpersistence <> 't'                                       "
            "and cast((n_dead_tuples)/(n_live_tuples+n_dead_tuples+0.00001) * 100                  "
            "as numeric(5,2)) >= %d                                                                "
            "and (n_live_tuples+n_dead_tuples) >= %d                                               "
            "and n.nspname = \'%s\';                                                               ",
            dirty_pecent,
            n_tuples,
            schema_name);

    /* send sql and parallel fetch distribution info from all data nodes */
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_DATANODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    return distribuion_info;
}

TableDistributionInfo* get_remote_stat_pagewriter(TupleDesc tuple_desc)
{
    StringInfoData buf;
    TableDistributionInfo* distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called. */
    distribuion_info = (TableDistributionInfo*)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    appendStringInfo(&buf,
        "select                                                                "
        "node_name, pgwr_actual_flush_total_num, pgwr_last_flush_num, remain_dirty_page_num,   "
        "queue_head_page_rec_lsn, queue_rec_lsn, current_xlog_insert_lsn, ckpt_redo_point      "
        "from local_pagewriter_stat();                                                         ");

    /* send sql and parallel fetch distribution info from all data nodes */
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    return distribuion_info;
}

TableDistributionInfo* get_remote_stat_ckpt(TupleDesc tuple_desc)
{
    StringInfoData buf;
    TableDistributionInfo* distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called. */
    distribuion_info = (TableDistributionInfo*)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    appendStringInfo(&buf,
        "select                                                     "
        "node_name,ckpt_redo_point,ckpt_clog_flush_num,ckpt_csnlog_flush_num,       "
        "ckpt_multixact_flush_num,ckpt_predicate_flush_num,ckpt_twophase_flush_num  "
        "from local_ckpt_stat();                                                    ");

    /* send sql and parallel fetch distribution info from all data nodes */
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    return distribuion_info;
}

TableDistributionInfo* get_remote_stat_bgwriter(TupleDesc tuple_desc)
{
    StringInfoData buf;
    TableDistributionInfo* distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called. */
    distribuion_info = (TableDistributionInfo*)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    appendStringInfo(&buf,
        "select                                                                     "
        "node_name,bgwr_actual_flush_total_num,bgwr_last_flush_num,candidate_slots, "
        "get_buffer_from_list,get_buf_clock_sweep                                   "
        "from local_bgwriter_stat();                                                ");

    /* send sql and parallel fetch distribution info from all data nodes */
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    return distribuion_info;
}

TableDistributionInfo* get_remote_stat_candidate(TupleDesc tuple_desc)
{
    StringInfoData buf;
    TableDistributionInfo* distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called. */
    distribuion_info = (TableDistributionInfo*)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    appendStringInfo(&buf,
        "select                                                                     "
        "node_name,candidate_slots,get_buf_from_list,get_buf_clock_sweep,           "
        "seg_candidate_slots,seg_get_buf_from_list,seg_get_buf_clock_sweep          "
        "from local_candidate_stat();                                               ");

    /* send sql and parallel fetch distribution info from all data nodes */
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    return distribuion_info;
}


TableDistributionInfo* get_remote_single_flush_dw_stat(TupleDesc tuple_desc)
{
    StringInfoData buf;
    TableDistributionInfo* distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called. */
    distribuion_info = (TableDistributionInfo*)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    appendStringInfo(&buf,
        "SELECT node_name, curr_dwn, curr_start_page, total_writes, file_trunc_num, file_reset_num "
        "FROM local_single_flush_dw_stat();");

    /* send sql and parallel fetch distribution info from all data nodes */
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    return distribuion_info;
}

TableDistributionInfo* get_remote_stat_double_write(TupleDesc tuple_desc)
{
    StringInfoData buf;
    TableDistributionInfo* distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called. */
    distribuion_info = (TableDistributionInfo*)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    appendStringInfo(&buf,
        "SELECT node_name, curr_dwn, curr_start_page, file_trunc_num, file_reset_num, "
        "total_writes, low_threshold_writes, high_threshold_writes, "
        "total_pages, low_threshold_pages, high_threshold_pages, file_id "
        "FROM local_double_write_stat();");

    /* send sql and parallel fetch distribution info from all data nodes */
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    return distribuion_info;
}

TableDistributionInfo* get_remote_stat_redo(TupleDesc tuple_desc)
{
    StringInfoData buf;
    TableDistributionInfo* distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called. */
    distribuion_info = (TableDistributionInfo*)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    appendStringInfo(&buf,
        "SELECT node_name, redo_start_ptr, redo_start_time, redo_done_time, curr_time, "
        "min_recovery_point, read_ptr, last_replayed_read_ptr, recovery_done_ptr, "
        "read_xlog_io_counter, read_xlog_io_total_dur, "
        "read_data_io_counter, read_data_io_total_dur, "
        "write_data_io_counter, write_data_io_total_dur, "
        "process_pending_counter, process_pending_total_dur, "
        "apply_counter, apply_total_dur, "
        "speed, local_max_ptr, primary_flush_ptr, worker_info "
        "FROM local_redo_stat();");

    /* send sql and parallel fetch distribution info from all data nodes */
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    return distribuion_info;
}

TableDistributionInfo* get_rto_stat(TupleDesc tuple_desc)
{
    StringInfoData buf;
    TableDistributionInfo* distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called. */
    distribuion_info = (TableDistributionInfo*)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    appendStringInfo(&buf, "SELECT node_name, rto_info FROM local_rto_stat();");

    /* send sql and parallel fetch distribution info from all data nodes */
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    return distribuion_info;
}

TableDistributionInfo* get_recovery_stat(TupleDesc tuple_desc)
{
    StringInfoData buf;
    TableDistributionInfo* distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called. */
    distribuion_info = (TableDistributionInfo*)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    appendStringInfo(&buf,
        "SELECT node_name, standby_node_name, source_ip, source_port, dest_ip, dest_port, current_rto, target_rto, "
        "current_sleep_time FROM "
        "local_recovery_status();");

    /* send sql and parallel fetch distribution info from all data nodes */
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    return distribuion_info;
}

TableDistributionInfo* streaming_hadr_get_recovery_stat(TupleDesc tuple_desc)
{
    StringInfoData buf;
    TableDistributionInfo* distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called. */
    distribuion_info = (TableDistributionInfo*)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    appendStringInfo(&buf,
        "SELECT hadr_sender_node_name, hadr_receiver_node_name, "
        "source_ip, source_port, dest_ip, dest_port, current_rto, target_rto, current_rpo, target_rpo, "
        "rto_sleep_time, rpo_sleep_time FROM "
        "gs_hadr_local_rto_and_rpo_stat();");

    /* send sql and parallel fetch distribution info from all data nodes */
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    return distribuion_info;
}

TableDistributionInfo* get_remote_node_xid_csn(TupleDesc tuple_desc)
{
    StringInfoData buf;
    TableDistributionInfo* distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called.*/
    distribuion_info = (TableDistributionInfo*)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    appendStringInfo(&buf, "select node_name, next_xid, next_csn FROM gs_get_next_xid_csn();");

    /* send sql and parallel fetch distribution info from all data nodes */
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    return distribuion_info;
}

#ifdef ENABLE_MULTIPLE_NODES
TableDistributionInfo* get_remote_index_status(TupleDesc tuple_desc, const char *schname, const char *idxname)
{
    StringInfoData buf;
    TableDistributionInfo* distribuion_info = NULL;

    /* the memory palloced here should be free outside where it was called.*/
    distribuion_info = (TableDistributionInfo*)palloc0(sizeof(TableDistributionInfo));

    initStringInfo(&buf);

    appendStringInfo(&buf, "select a.node_name::text, b.indisready, b.indisvalid from pg_index b "
        "left join pgxc_node a on b.xc_node_id = a.node_id "
        "where indexrelid = (select oid from pg_class where relnamespace = "
        "(select oid from pg_namespace where nspname = %s) "
        "and relname = %s);", quote_literal_cstr(schname), quote_literal_cstr(idxname));

    /* send sql and parallel fetch distribution info from all nodes */
    distribuion_info->state = RemoteFunctionResultHandler(buf.data, NULL, NULL, true, EXEC_ON_ALL_NODES, true);
    distribuion_info->slot = MakeSingleTupleTableSlot(tuple_desc);

    pfree_ext(buf.data);

    return distribuion_info;
}

#endif

/*
 * the whole process statistics of bad block
 * used for query statistics of bad block
 * update by pgstat_recv_badblock_stat()
 */
MemoryContext global_bad_block_mcxt = NULL;
HTAB* global_bad_block_stat = NULL;

/*
 * Record a statistics of bad block:
 * Backend thread: read a bad page/cu  --> addBadBlockStat() record in local_bad_block_stat -->
 * pgstat_send_badblock_stat() send msg to PgstatCollector thread PgstatCollector thread: pgstat_recv_badblock_stat
 * receive msg from Backen thread, and record in global_bad_block_stat
 *
 * Query statistics of bad block:
 * Backend thread: pg_stat_bad_block() read the global_bad_block_stat and output
 */
/*
 * @Description: init process statistics of bad block hash table
 * @See also:
 */
void initGlobalBadBlockStat()
{
    /* global bad block statistics */
    /* in g_instance.instance_context */
    if (global_bad_block_mcxt == NULL) {
        global_bad_block_mcxt = AllocSetContextCreate((MemoryContext)g_instance.instance_context,
            "bad block stat global memory context",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            SHARED_CONTEXT);
    }

    LWLockAcquire(BadBlockStatHashLock, LW_EXCLUSIVE);

    if (global_bad_block_stat == NULL) {

        HASHCTL hash_ctl;
        errno_t errval = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
        securec_check_errval(errval, , LOG);

        hash_ctl.hcxt = global_bad_block_mcxt;
        hash_ctl.keysize = sizeof(BadBlockHashKey);
        hash_ctl.entrysize = sizeof(BadBlockHashEnt);
        hash_ctl.hash = tag_hash;

        global_bad_block_stat =
            hash_create("bad block stat global hash table", 64, &hash_ctl, HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION);
    }

    LWLockRelease(BadBlockStatHashLock);
}

/*
 * @Description: init thread statistics of bad block hash table
 * @See also:
 */
void initLocalBadBlockStat()
{
    /* local thread bad block statistics */
    /* in t_thrd.top_mem_cxt */
    if (t_thrd.stat_cxt.local_bad_block_mcxt == NULL) {
        t_thrd.stat_cxt.local_bad_block_mcxt = AllocSetContextCreate((MemoryContext)t_thrd.top_mem_cxt,
            "bad block session memory context",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    }

    if (t_thrd.stat_cxt.local_bad_block_stat == NULL) {
        HASHCTL hash_ctl;
        errno_t errval = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
        securec_check_errval(errval, , LOG);

        hash_ctl.hcxt = t_thrd.stat_cxt.local_bad_block_mcxt;
        hash_ctl.keysize = sizeof(BadBlockHashKey);
        hash_ctl.entrysize = sizeof(BadBlockHashEnt);
        hash_ctl.hash = tag_hash;

        t_thrd.stat_cxt.local_bad_block_stat =
            hash_create("bad block stat session hash table", 16, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
    }
}

/*
 * @Description: add statistics of bad block where read a bad page/cu
 * @IN relfilenode: relfilenode for page/cu
 * @IN forknum: forknum for page/cu
 * @See also:
 */
void addBadBlockStat(const RelFileNode* relfilenode, ForkNumber forknum)
{
    if (t_thrd.stat_cxt.local_bad_block_stat == NULL || relfilenode == NULL) {
        return;
    }

    TimestampTz last_time = GetCurrentTimestamp();

    BadBlockHashKey hash_key;
    hash_key.relfilenode.spcNode = relfilenode->spcNode;
    hash_key.relfilenode.dbNode = relfilenode->dbNode;
    hash_key.relfilenode.relNode = relfilenode->relNode;
    hash_key.relfilenode.bucketNode = relfilenode->bucketNode;
    hash_key.relfilenode.opt = relfilenode->opt;
    hash_key.forknum = forknum;

    bool found = false;

    /* insert if not find, if out of memory return NULL */
    BadBlockHashEnt* entry =
        (BadBlockHashEnt*)hash_search(t_thrd.stat_cxt.local_bad_block_stat, &hash_key, HASH_ENTER, &found);

    if (entry != NULL) {
        if (!found) {
            /* first time */
            entry->first_time = last_time;
            entry->error_count = 0;
        }

        ++entry->error_count;
        entry->last_time = last_time;
    }
}

/*
 * @Description: clear the whole process statistics of bad block
 * @See also:
 */
void resetBadBlockStat()
{
    if (global_bad_block_stat == NULL)
        return;

    LWLockAcquire(BadBlockStatHashLock, LW_EXCLUSIVE);

    if (global_bad_block_stat == NULL) {
        LWLockRelease(BadBlockStatHashLock);
        return;
    }

    /* destory hash table */
    hash_remove(global_bad_block_stat);
    global_bad_block_stat = NULL;

    /* create hash table */
    HASHCTL hash_ctl;
    errno_t errval = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check_errval(errval, , LOG);

    hash_ctl.hcxt = global_bad_block_mcxt;
    hash_ctl.keysize = sizeof(BadBlockHashKey);
    hash_ctl.entrysize = sizeof(BadBlockHashEnt);
    hash_ctl.hash = tag_hash;

    global_bad_block_stat =
        hash_create("bad block stat hash table", 64, &hash_ctl, HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION);

    LWLockRelease(BadBlockStatHashLock);
}

/*
 * @Description: Subroutine for pgstat_report_stat: populate and send a bad block stat message
 * @See also:
 */
static void pgstat_send_badblock_stat(void)
{
    if (t_thrd.stat_cxt.local_bad_block_stat == NULL)
        return;

    PgStat_MsgBadBlock msg;
    HASH_SEQ_STATUS hash_seq;
    BadBlockHashEnt* badblock_entry = NULL;

    pgstat_setheader(&msg.m_hdr, PGSTAT_MTYPE_BADBLOCK);
    msg.m_nentries = 0;

    hash_seq_init(&hash_seq, t_thrd.stat_cxt.local_bad_block_stat);

    while ((badblock_entry = (BadBlockHashEnt*)hash_seq_search(&hash_seq)) != NULL) {
        /* jump empty */
        if (badblock_entry->error_count == 0)
            continue;

        BadBlockHashEnt* m_ent = &msg.m_entry[msg.m_nentries];
        errno_t rc = memcpy_s(m_ent, sizeof(BadBlockHashEnt), badblock_entry, sizeof(BadBlockHashEnt));
        securec_check(rc, "", "");

        if ((unsigned int)++msg.m_nentries >= PGSTAT_NUM_BADBLOCK_ENTRIES) {
            pgstat_send(&msg, offsetof(PgStat_MsgBadBlock, m_entry[0]) + msg.m_nentries * sizeof(BadBlockHashEnt));
            msg.m_nentries = 0;
        }

        /* reset the entry's counts */
        badblock_entry->error_count = 0;
        badblock_entry->first_time = 0;
        badblock_entry->last_time = 0;
    }

    if (msg.m_nentries > 0)
        pgstat_send(&msg, offsetof(PgStat_MsgBadBlock, m_entry[0]) + msg.m_nentries * sizeof(BadBlockHashEnt));
}

/*
 * @Description: Process bad block stat message
 * @IN msg: bad block stat message
 * @IN len: without use
 * @See also:
 */
static void pgstat_recv_badblock_stat(PgStat_MsgBadBlock* msg, int /* len */)
{
    if (global_bad_block_stat == NULL || msg == NULL) {
        return;
    }

    BadBlockHashEnt* badblock_entry = &(msg->m_entry[0]);

    LWLockAcquire(BadBlockStatHashLock, LW_EXCLUSIVE);

    if (global_bad_block_stat == NULL) {
        LWLockRelease(BadBlockStatHashLock);
        return;
    }

    for (int i = 0; i < msg->m_nentries; i++, badblock_entry++) {
        bool found = false;
        BadBlockHashKey* badblock_key = &(badblock_entry->key);

        /* insert if not find, if out of memory return NULL */
        BadBlockHashEnt* entry = (BadBlockHashEnt*)hash_search(global_bad_block_stat, badblock_key, HASH_ENTER, &found);

        if (entry != NULL) {
            if (!found) {
                /* first time */
                entry->first_time = badblock_entry->first_time;
                entry->error_count = 0;
            }

            entry->error_count += badblock_entry->error_count;
            entry->last_time = badblock_entry->last_time;
        }
    }

    LWLockRelease(BadBlockStatHashLock);
}

/*
 * get current total counter
 */
void GetCurrentTotalTableCounter(PgStat_TableCounts* total_table_counter)
{
    TabStatusArray* tsa = NULL;

    for (tsa = u_sess->stat_cxt.pgStatTabList; tsa != NULL; tsa = tsa->tsa_next) {
        for (int i = 0; i < tsa->tsa_used; i++) {
            PgStat_TableStatus* entry = &tsa->tsa_entries[i];
            PgStat_TableXactStatus* trans = NULL;

            if (entry != NULL) {
                // get total table counts stat
                UniqueSQLSumTableStatCounter((*total_table_counter), entry->t_counts);

                // get sub xact counter(IUD counters are in PgStat_TableXactStatus)
                for (trans = entry->trans; trans != NULL; trans = trans->upper) {
                    total_table_counter->t_tuples_inserted += trans->tuples_inserted;
                    total_table_counter->t_tuples_updated += trans->tuples_updated;
                    total_table_counter->t_tuples_deleted += trans->tuples_deleted;
                }
            }
        }
    }
}

/*
 * CalcSQLRowStatCounter - get current unique sql's row
 *   activity's counter
 *
 * Used for Instrumentation/UniqueSQL.
 *
 * last_total_counter	- when exit pgstat_report_stat last time,
 *			store the total counter into last_total_counter.
 * current_table_counter- current unique sql's stat counter
 *
 * return true if current_table_counter has values
 */
bool CalcSQLRowStatCounter(PgStat_TableCounts* last_total_counter, PgStat_TableCounts* current_sql_table_counter)
{
    Assert(current_sql_table_counter && last_total_counter);

    if (u_sess->stat_cxt.pgStatTabList == NULL || u_sess->stat_cxt.pgStatTabList->tsa_used == 0)
        return false;

    /*
     * dont calculate Counter for towphase's Prepare since tuple activity has been removed from
     * PgStat_TableCounts in PostPrepare_PgStat. if do it, smaller number will be found in T
     * than L's, and Prepare will has minus number for row stats.
     */
    if (u_sess->storage_cxt.twoPhaseCommitInProgress)
        return false;

    ereport(DEBUG1,
        (errmodule(MOD_INSTR),
            errmsg("[UniqueSQL] unique id: %lu, calc row stat counter", u_sess->unique_sql_cxt.unique_sql_id)));

    PrintPgStatTableCounter('L', last_total_counter);

    PgStat_TableCounts total_table_counter = {0};

    // get current total counter
    GetCurrentTotalTableCounter(&total_table_counter);
    PrintPgStatTableCounter('T', &total_table_counter);

    errno_t rc;
    // current sql counter = total - last_total
    if (total_table_counter.t_tuples_returned != 0 || total_table_counter.t_tuples_fetched != 0 ||
        total_table_counter.t_tuples_inserted != 0 || total_table_counter.t_tuples_updated != 0 ||
        total_table_counter.t_tuples_deleted != 0 || total_table_counter.t_blocks_fetched != 0 ||
        total_table_counter.t_blocks_hit != 0) {
        UniqueSQLDiffTableStatCounter((*current_sql_table_counter), total_table_counter, (*last_total_counter));
        PrintPgStatTableCounter('C', current_sql_table_counter);

        /*
         * if local counter is cleaned in pgstat_report_stat method,
         * reset last total counter in pgstat_report_stat method.
         *
         * if not cleaned, update last total counter to total counter.
         */
        rc = memcpy_s(last_total_counter, sizeof(PgStat_TableCounts), &total_table_counter, sizeof(PgStat_TableCounts));
        securec_check(rc, "\0", "\0");

        return true;
    } else {
        rc = memset_s(last_total_counter, sizeof(PgStat_TableCounts), 0, sizeof(PgStat_TableCounts));
        securec_check(rc, "\0", "\0");
        return false;
    }
}

int pgstat_fetch_sql_rt_info_counter(void)
{
    if (g_instance.stat_cxt.sql_rt_info_array != NULL) {
        if (IS_SINGLE_NODE) {
            prepare_calculate_single(g_instance.stat_cxt.sql_rt_info_array, &u_sess->percentile_cxt.LocalCounter);
        } else {
            prepare_calculate(g_instance.stat_cxt.sql_rt_info_array, &u_sess->percentile_cxt.LocalCounter);
        }
    }
    return u_sess->percentile_cxt.LocalCounter;
}

void pgstat_fetch_sql_rt_info_internal(SqlRTInfo* sqlrt)
{
    if (sqlrt == NULL || u_sess->percentile_cxt.LocalCounter == 0 || u_sess->percentile_cxt.LocalsqlRT == NULL)
        return;
    errno_t rc = memcpy_s(sqlrt,
        u_sess->percentile_cxt.LocalCounter * sizeof(SqlRTInfo),
        u_sess->percentile_cxt.LocalsqlRT,
        u_sess->percentile_cxt.LocalCounter * sizeof(SqlRTInfo));
    securec_check(rc, "\0", "\0");
    pfree_ext(u_sess->percentile_cxt.LocalsqlRT);
}

void pgstat_reply_percentile_record_count()
{
    StringInfoData buf;
    g_instance.stat_cxt.calculate_on_other_cn = true;

    (void)pgstat_fetch_sql_rt_info_counter();

    pq_beginmessage(&buf, 'c');
    pq_sendint(&buf, u_sess->percentile_cxt.LocalCounter, sizeof(int));
    pq_endmessage(&buf);
    pq_beginmessage(&buf, 'f');
    pq_endmessage(&buf);
    pq_flush();
}

void pgstat_reply_percentile_record()
{
    StringInfoData buf;
    if (u_sess->percentile_cxt.LocalsqlRT != NULL) {
        for (int i = 0; i < u_sess->percentile_cxt.LocalCounter; i++) {
            pq_beginmessage(&buf, 'r');
            pq_sendint64(&buf, u_sess->percentile_cxt.LocalsqlRT[i].UniqueSQLId);
            pq_sendint64(&buf, u_sess->percentile_cxt.LocalsqlRT[i].rt);
            pq_endmessage(&buf);
        }
        pfree_ext(u_sess->percentile_cxt.LocalsqlRT);
    }
    pq_beginmessage(&buf, 'f');
    pq_endmessage(&buf);
    g_instance.stat_cxt.calculate_on_other_cn = false;
    pq_flush();
}

void pgstat_release_session_memory_entry()
{
    if (t_thrd.shemem_ptr_cxt.mySessionMemoryEntry != NULL) {
        pfree_ext(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan);
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->plan_size = 0;
        pfree_ext(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->query_plan_issue);
    }
}

