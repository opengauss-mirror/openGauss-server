/* -------------------------------------------------------------------------
 *
 * bootstrap.c
 *	  routines to support running openGauss in 'bootstrap' mode
 *	bootstrap mode is used to create the initial template database
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  src/backend/bootstrap/bootstrap.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "pgstat.h"
#include <time.h>
#include <unistd.h>
#include <signal.h>
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

#include "access/tableam.h"
#include "bootstrap/bootstrap.h"
#include "catalog/index.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_class.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "nodes/makefuncs.h"
#include "postmaster/aiocompleter.h"
#include "postmaster/bgwriter.h"
#include "postmaster/pagewriter.h"
#include "postmaster/cbmwriter.h"
#include "postmaster/startup.h"
#include "postmaster/twophasecleaner.h"
#include "postmaster/licensechecker.h"
#include "postmaster/walwriter.h"
#include "postmaster/lwlockmonitor.h"
#include "replication/walreceiver.h"
#include "replication/datareceiver.h"
#include "storage/buf/bufmgr.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "threadpool/threadpool.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc_storage.h"
#include "utils/memutils.h"
#include "utils/plog.h"
#include "utils/postinit.h"
#include "utils/ps_status.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/relmapper.h"
#include "utils/snapmgr.h"
#include "access/parallel_recovery/page_redo.h"

#ifdef PGXC
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#endif

#include "gssignal/gs_signal.h"
#include "storage/file/fio_device.h"
#include "storage/dss/dss_adaptor.h"
#include "storage/dss/dss_log.h"

#define ALLOC(t, c) ((t*)selfpalloc0((unsigned)(c) * sizeof(t)))

static void CheckerModeMain(void);
static void BootstrapModeMain(void);
static void bootstrap_signals(void);
static Form_pg_attribute AllocateAttribute(void);
static Oid gettype(char* type);
static void cleanup(void);

/*
 * Basic information associated with each type.  This is used before
 * pg_type is filled, so it has to cover the datatypes used as column types
 * in the core "bootstrapped" catalogs.
 *
 *		XXX several of these input/output functions do catalog scans
 *			(e.g., F_REGPROCIN scans pg_proc).	this obviously creates some
 *			order dependencies in the catalog creation process.
 */
struct typinfo {
    char name[NAMEDATALEN];
    Oid oid;
    Oid elem;
    int16 len;
    bool byval;
    char align;
    char storage;
    Oid collation;
    Oid inproc;
    Oid outproc;
};

static const struct typinfo TypInfo[] = {{"bool", BOOLOID, 0, 1, true, 'c', 'p', InvalidOid, F_BOOLIN, F_BOOLOUT},
    {"bytea", BYTEAOID, 0, -1, false, 'i', 'x', InvalidOid, F_BYTEAIN, F_BYTEAOUT},
    {"char", CHAROID, 0, 1, true, 'c', 'p', InvalidOid, F_CHARIN, F_CHAROUT},
    {"int1", INT1OID, 0, 1, true, 'c', 'p', InvalidOid, F_INT1IN, F_INT1OUT},
    {"int2", INT2OID, 0, 2, true, 's', 'p', InvalidOid, F_INT2IN, F_INT2OUT},
    {"int4", INT4OID, 0, 4, true, 'i', 'p', InvalidOid, F_INT4IN, F_INT4OUT},
    {"float4", FLOAT4OID, 0, 4, FLOAT4PASSBYVAL, 'i', 'p', InvalidOid, F_FLOAT4IN, F_FLOAT4OUT},
    {"name", NAMEOID, CHAROID, NAMEDATALEN, false, 'c', 'p', InvalidOid, F_NAMEIN, F_NAMEOUT},
    {"regclass", REGCLASSOID, 0, 4, true, 'i', 'p', InvalidOid, F_REGCLASSIN, F_REGCLASSOUT},
    {"regproc", REGPROCOID, 0, 4, true, 'i', 'p', InvalidOid, F_REGPROCIN, F_REGPROCOUT},
    {"regtype", REGTYPEOID, 0, 4, true, 'i', 'p', InvalidOid, F_REGTYPEIN, F_REGTYPEOUT},
    {"text", TEXTOID, 0, -1, false, 'i', 'x', DEFAULT_COLLATION_OID, F_TEXTIN, F_TEXTOUT},
    {"oid", OIDOID, 0, 4, true, 'i', 'p', InvalidOid, F_OIDIN, F_OIDOUT},
    {"tid", TIDOID, 0, 6, false, 's', 'p', InvalidOid, F_TIDIN, F_TIDOUT},
    {"xid", XIDOID, 0, 8, FLOAT8PASSBYVAL, 'd', 'p', InvalidOid, F_XIDIN, F_XIDOUT},
    {"xid32", SHORTXIDOID, 0, 4, FLOAT4PASSBYVAL, 'i', 'p', InvalidOid, F_XIDIN4, F_XIDOUT4},
    {"cid", CIDOID, 0, 4, true, 'i', 'p', InvalidOid, F_CIDIN, F_CIDOUT},
    {"pg_node_tree",
        PGNODETREEOID,
        0,
        -1,
        false,
        'i',
        'x',
        DEFAULT_COLLATION_OID,
        F_PG_NODE_TREE_IN,
        F_PG_NODE_TREE_OUT},
    {"int2vector", INT2VECTOROID, INT2OID, -1, false, 'i', 'p', InvalidOid, F_INT2VECTORIN, F_INT2VECTOROUT},
    {"oidvector", OIDVECTOROID, OIDOID, -1, false, 'i', 'p', InvalidOid, F_OIDVECTORIN, F_OIDVECTOROUT},
    {"_int2", INT2ARRAYOID, INT2OID, -1, false, 'i', 'x', InvalidOid, F_ARRAY_IN, F_ARRAY_OUT},
    {"_int4", INT4ARRAYOID, INT4OID, -1, false, 'i', 'x', InvalidOid, F_ARRAY_IN, F_ARRAY_OUT},
    {"_text", 1009, TEXTOID, -1, false, 'i', 'x', DEFAULT_COLLATION_OID, F_ARRAY_IN, F_ARRAY_OUT},
    {"_oid", 1028, OIDOID, -1, false, 'i', 'x', InvalidOid, F_ARRAY_IN, F_ARRAY_OUT},
    {"_char", 1002, CHAROID, -1, false, 'i', 'x', InvalidOid, F_ARRAY_IN, F_ARRAY_OUT},
    {"_aclitem", 1034, ACLITEMOID, -1, false, 'i', 'x', InvalidOid, F_ARRAY_IN, F_ARRAY_OUT},
    {"raw", RAWOID, 0, -1, false, 'i', 'x', InvalidOid, F_BYTEAIN, F_BYTEAOUT},
    {"oidvector_extend",
        OIDVECTOREXTENDOID,
        OIDOID,
        -1,
        false,
        'i',
        'x',
        InvalidOid,
        F_OIDVECTORIN_EXTEND,
        F_OIDVECTOROUT_EXTEND},
    {"int2vector_extend",
        INT2VECTOREXTENDOID,
        INT2OID,
        -1,
        false,
        'i',
        'x',
        InvalidOid,
        F_INT2VECTORIN,
        F_INT2VECTOROUT}};

static const int n_types = sizeof(TypInfo) / sizeof(struct typinfo);

struct typmap { /* a hack */
    Oid am_oid;
    FormData_pg_type am_typ;
};

static THR_LOCAL Datum values[MAXATTR]; /* current row's attribute values */
static THR_LOCAL bool Nulls[MAXATTR];

/*
 *	At bootstrap time, we first declare all the indices to be built, and
 *	then build them.  The IndexList structure stores enough information
 *	to allow us to build the indices after they've been declared.
 */
typedef struct _IndexList {
    Oid il_heap;
    Oid il_ind;
    IndexInfo* il_info;
    struct _IndexList* il_next;
} IndexList;

/*
 *	 BootStrapProcessMain
 *
 *	 The main entry point for auxiliary processes, such as the bgwriter,
 *	 walwriter, walreceiver, bootstrapper and the shared memory checker code.
 *
 *	 This code is here just because of historical reasons.
 */
void BootStrapProcessMain(int argc, char* argv[])
{
    char* progName = argv[0];
    int flag;
    char* userDoption = NULL;
    OptParseContext optCtxt;
    errno_t errorno = EOK;

    /*
     * initialize globals
     */
    PostmasterPid = gs_thread_self();

    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    t_thrd.proc_cxt.MyStartTime = time(NULL);

    /*
     * Initialize random() for the first time, like PostmasterMain() would.
     * In a regular IsUnderPostmaster backend, BackendRun() computes a
     * high-entropy seed before any user query.  Fewer distinct initial seeds
     * can occur here.
     */
    srandom((unsigned int)(t_thrd.proc_cxt.MyProcPid ^ (unsigned int)t_thrd.proc_cxt.MyStartTime));

    t_thrd.proc_cxt.MyProgName = "BootStrap";
    /*
     * Fire up essential subsystems: error and memory management
     *
     * If we are running under the postmaster, this is done already.
     */
    if (!IsUnderPostmaster) {
        MemoryContextInit();
        init_plog_global_mem();
    }

    /* Compute paths, if we didn't inherit them from postmaster */
    if (my_exec_path[0] == '\0') {
        if (find_my_exec(progName, my_exec_path) < 0)
            ereport(FATAL, (errmsg("%s: could not locate my own executable path", progName)));
    }

    /*
     * process command arguments
     */
    /* Set defaults, to be overriden by explicit options below */
    if (!IsUnderPostmaster) {
        InitializeGUCOptions();
    }

    /* Ignore the initial --boot argument, if present */
    if (argc > 1 && strcmp(argv[1], "--boot") == 0) {
        argv++;
        argc--;
    }

    /* If no -x argument, we are a CheckerProcess */
    t_thrd.bootstrap_cxt.MyAuxProcType = CheckerProcess;

    initOptParseContext(&optCtxt);
    while ((flag = getopt_r(argc, argv, "B:c:d:D:FGr:x:g:-:", &optCtxt)) != -1) {
        switch (flag) {
            case 'B':
                SetConfigOption("shared_buffers", optCtxt.optarg, PGC_POSTMASTER, PGC_S_ARGV);
                break;
            case 'D':
                userDoption = optCtxt.optarg;
                break;
            case 'd': {
                int debugStrLen = strlen("debug") + strlen(optCtxt.optarg) + 1;
                /* Turn on debugging for the bootstrap process. */
                char* debugstr = (char*)palloc(debugStrLen);

                errorno = snprintf_s(debugstr, debugStrLen, debugStrLen - 1, "debug%s", optCtxt.optarg);
                securec_check_ss(errorno, "\0", "\0");
                SetConfigOption("log_min_messages", debugstr, PGC_POSTMASTER, PGC_S_ARGV);
                SetConfigOption("client_min_messages", debugstr, PGC_POSTMASTER, PGC_S_ARGV);
                pfree(debugstr);
            } break;
            case 'F':
                SetConfigOption("fsync", "false", PGC_POSTMASTER, PGC_S_ARGV);
                break;
            case 'G':
                EnableInitDBSegment = true;
                break;
            case 'g':
                SetConfigOption("xlog_file_path", optCtxt.optarg, PGC_POSTMASTER, PGC_S_ARGV);
                break;
            case 'r':
                errorno = strcpy_s(t_thrd.proc_cxt.OutputFileName, MAXPGPATH, optCtxt.optarg);
                securec_check(errorno, "\0", "\0");
                break;
            case 'x':
                t_thrd.bootstrap_cxt.MyAuxProcType = (AuxProcType)atoi(optCtxt.optarg);
                break;
            case 'c':
            case '-': {
                char* name = NULL;
                char* value = NULL;

                ParseLongOption(optCtxt.optarg, &name, &value);
                if (value == NULL) {
                    if (flag == '-')
                        ereport(
                            ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("--%s requires a value", optCtxt.optarg)));
                    else
                        ereport(
                            ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("-c %s requires a value", optCtxt.optarg)));
                }

                SetConfigOption(name, value, PGC_POSTMASTER, PGC_S_ARGV);
                pfree(name);
                if (value != NULL)
                    pfree(value);
                break;
            }

            default:
                write_stderr("Try \"%s --help\" for more information.\n", progName);
                proc_exit(1);
                break;
        }
    }

    if (argc != optCtxt.optind) {
        write_stderr("%s: invalid command-line arguments\n", progName);
        proc_exit(1);
    }

    /* Acquire configuration parameters, unless inherited from postmaster */
    if (!IsUnderPostmaster) {
        if (!SelectConfigFiles(userDoption, progName)) {
            proc_exit(1);
        }
        InitializeNumLwLockPartitions();
    }
    g_instance.global_sysdbcache.Init(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
    CreateLocalSysDBCache();

    /* Validate we have been given a reasonable-looking t_thrd.proc_cxt.DataDir */
    Assert(t_thrd.proc_cxt.DataDir);
    ValidatePgVersion(t_thrd.proc_cxt.DataDir);

    /* Change into t_thrd.proc_cxt.DataDir (if under postmaster, should be done already) */
    if (!IsUnderPostmaster)
        ChangeToDataDir();

    /* If standalone, create lockfile for data directory */
    if (!IsUnderPostmaster)
        CreateDataDirLockFile(false);
    
    /* Callback function for dss operator */
    if (dss_device_init(g_instance.attr.attr_storage.dss_attr.ss_dss_conn_path,
        g_instance.attr.attr_storage.dss_attr.ss_enable_dss) != DSS_SUCCESS) {
        ereport(PANIC, (errmsg("failed to init dss device")));
        proc_exit(1);
    }
    if (ENABLE_DSS) {
       DSSInitLogger();
    }
    initDSSConf();

    SetProcessingMode(BootstrapProcessing);
    u_sess->attr.attr_common.IgnoreSystemIndexes = true;

    BaseInit();

    pgstat_initialize();
    pgstat_bestart();
    if (!IsUnderPostmaster) {
        ShareStorageInit();
    }
    /*
     * XLOG operations
     */
    SetProcessingMode(NormalProcessing);

    switch (t_thrd.bootstrap_cxt.MyAuxProcType) {
        case CheckerProcess:
            /* don't set signals, they're useless here */
            CheckerModeMain();
            proc_exit(1); /* should never return */

        case BootstrapProcess:
            bootstrap_signals();
            BootStrapXLOG();
            MemoryContextUnSeal(t_thrd.top_mem_cxt);
            BootstrapModeMain();
            MemoryContextSeal(t_thrd.top_mem_cxt);
            proc_exit(1); /* should never return */

        default:
            ereport(PANIC, (errmsg("unrecognized process type: %d", (int)t_thrd.bootstrap_cxt.MyAuxProcType)));
            proc_exit(1);
    }
}

/*
 * In shared memory checker mode, all we really want to do is create shared
 * memory and semaphores (just to prove we can do it with the current GUC
 * settings).  Since, in fact, that was already done by BaseInit(),
 * we have nothing more to do here.
 */
static void CheckerModeMain(void)
{
    proc_exit(0);
}

/*
 *	 The main entry point for running the backend in bootstrap mode
 *
 *	 The bootstrap mode is used to initialize the template database.
 *	 The bootstrap backend doesn't speak SQL, but instead expects
 *	 commands in a special bootstrap language.
 */
static void BootstrapModeMain(void)
{
    int i;

    Assert(!IsUnderPostmaster);

    SetProcessingMode(BootstrapProcessing);

    /*
     * Do backend-like initialization for bootstrap mode
     */
    InitProcess();

    t_thrd.proc_cxt.PostInit->SetDatabaseAndUser(NULL, InvalidOid, NULL);
    t_thrd.proc_cxt.PostInit->InitBootstrap();

    /* Initialize stuff for bootstrap-file processing */
    for (i = 0; i < MAXATTR; i++) {
        t_thrd.bootstrap_cxt.attrtypes[i] = NULL;
        Nulls[i] = false;
    }

    /*
     * Process bootstrap input.
     */
    boot_yyparse();

    /*
     * We should now know about all mapped relations, so it's okay to write
     * out the initial relation mapping files.
     */
    RelationMapFinishBootstrap();

    /* Clean up and exit */
    cleanup();
    proc_exit(0);
}

/* ----------------------------------------------------------------
 *						misc functions
 * ----------------------------------------------------------------
 */
/*
 * Set up signal handling for a bootstrap process
 */
static void bootstrap_signals(void)
{
    if (IsUnderPostmaster) {
        /*
         * Properly accept or ignore signals the postmaster might send us
         */
        (void)gspqsignal(SIGHUP, SIG_IGN);
        (void)gspqsignal(SIGINT, SIG_IGN); /* ignore query-cancel */
        (void)gspqsignal(SIGTERM, die);
        (void)gspqsignal(SIGQUIT, quickdie);
        (void)gspqsignal(SIGALRM, SIG_IGN);
        (void)gspqsignal(SIGPIPE, SIG_IGN);
        (void)gspqsignal(SIGUSR1, SIG_IGN);
        (void)gspqsignal(SIGUSR2, SIG_IGN);

        /*
         * Reset some signals that are accepted by postmaster but not here
         */
        (void)gspqsignal(SIGCHLD, SIG_DFL);
        (void)gspqsignal(SIGTTIN, SIG_DFL);
        (void)gspqsignal(SIGTTOU, SIG_DFL);
        (void)gspqsignal(SIGCONT, SIG_DFL);
        (void)gspqsignal(SIGWINCH, SIG_DFL);

        /*
         * Unblock signals (they were blocked when the postmaster forked us)
         */
        gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);
        (void)gs_signal_unblock_sigusr2();

    } else {
        /* Set up appropriately for interactive use */
        (void)gspqsignal(SIGHUP, die);
        (void)gspqsignal(SIGINT, die);
        (void)gspqsignal(SIGTERM, die);
        (void)gspqsignal(SIGQUIT, die);
        (void)gs_signal_unblock_sigusr2();
    }
}

/* ----------------------------------------------------------------
 *				MANUAL BACKEND INTERACTIVE INTERFACE COMMANDS
 * ----------------------------------------------------------------
 */
/* ----------------
 *		boot_openrel
 * ----------------
 */
void boot_openrel(char* relname)
{
    int i;
    struct typmap** app;
    Relation rel;
    TableScanDesc scan;
    HeapTuple tup;
    errno_t rc;

    if (strlen(relname) >= NAMEDATALEN)
        relname[NAMEDATALEN - 1] = '\0';

    if (t_thrd.bootstrap_cxt.Typ == NULL) {
        /* We can now load the pg_type data */
        rel = heap_open(TypeRelationId, NoLock);
        scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
        i = 0;

        while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL)
            ++i;
        tableam_scan_end(scan);
        app = t_thrd.bootstrap_cxt.Typ = ALLOC(struct typmap*, i + 1);
        while (i-- > 0)
            *app++ = ALLOC(struct typmap, 1);
        *app = NULL;
        scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
        app = t_thrd.bootstrap_cxt.Typ;
        while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
            (*app)->am_oid = HeapTupleGetOid(tup);
            rc =
                memcpy_s((char*)&(*app)->am_typ, sizeof((*app)->am_typ), (char*)GETSTRUCT(tup), sizeof((*app)->am_typ));
            securec_check(rc, "\0", "\0");
            app++;
        }
        tableam_scan_end(scan);
        heap_close(rel, NoLock);
    }

    if (t_thrd.bootstrap_cxt.boot_reldesc != NULL)
        closerel(NULL);

    ereport(DEBUG4, (errmsg("open relation %s, attrsize %d", relname, (int)ATTRIBUTE_FIXED_PART_SIZE)));

    t_thrd.bootstrap_cxt.boot_reldesc = heap_openrv(makeRangeVar(NULL, relname, -1), NoLock);
    t_thrd.bootstrap_cxt.numattr = RelationGetNumberOfAttributes(t_thrd.bootstrap_cxt.boot_reldesc);
    for (i = 0; i < t_thrd.bootstrap_cxt.numattr; i++) {
        if (t_thrd.bootstrap_cxt.attrtypes[i] == NULL)
            t_thrd.bootstrap_cxt.attrtypes[i] = AllocateAttribute();
        rc = memmove_s((char*)t_thrd.bootstrap_cxt.attrtypes[i],
            ATTRIBUTE_FIXED_PART_SIZE,
            (char*)&t_thrd.bootstrap_cxt.boot_reldesc->rd_att->attrs[i],
            ATTRIBUTE_FIXED_PART_SIZE);
        securec_check(rc, "\0", "\0");

        {
            Form_pg_attribute at = t_thrd.bootstrap_cxt.attrtypes[i];

            ereport(DEBUG4,
                (errmsg("create attribute %d name %s len %d num %d type %u",
                    i,
                    NameStr(at->attname),
                    at->attlen,
                    at->attnum,
                    at->atttypid)));
        }
    }
}

/* ----------------
 *		closerel
 * ----------------
 */
void closerel(char* name)
{
    if (name != NULL) {
        if (t_thrd.bootstrap_cxt.boot_reldesc) {
            if (strcmp(RelationGetRelationName(t_thrd.bootstrap_cxt.boot_reldesc), name) != 0)
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("close of %s when %s was expected",
                            name,
                            RelationGetRelationName(t_thrd.bootstrap_cxt.boot_reldesc))));
        } else
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("close of %s before any relation was opened", name)));
    }

    if (t_thrd.bootstrap_cxt.boot_reldesc == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("no open relation to close")));
    else {
        ereport(DEBUG4, (errmsg("close relation %s", RelationGetRelationName(t_thrd.bootstrap_cxt.boot_reldesc))));
        heap_close(t_thrd.bootstrap_cxt.boot_reldesc, NoLock);
        t_thrd.bootstrap_cxt.boot_reldesc = NULL;
    }
}

/*
* fix roluseft ,rolmonitoradmin, roloperatoradmin and rolpolicyadmin column of pg_authid to notnull
*/
static void fix_attr_notnull(const char* name, int attnum)
{
    if (strncmp(name, "roluseft", strlen("roluseft")) == 0 && strlen(name) == strlen("roluseft")) {
        t_thrd.bootstrap_cxt.attrtypes[attnum]->attnotnull = true;
    }
    if (strncmp(name, "rolmonitoradmin", strlen("rolmonitoradmin")) == 0 && strlen(name) == strlen("rolmonitoradmin")) {
        t_thrd.bootstrap_cxt.attrtypes[attnum]->attnotnull = true;
    }
    if (strncmp(name, "roloperatoradmin", strlen("roloperatoradmin")) == 0 &&
        strlen(name) == strlen("roloperatoradmin")) {
        t_thrd.bootstrap_cxt.attrtypes[attnum]->attnotnull = true;
    }
    if (strncmp(name, "rolpolicyadmin", strlen("rolpolicyadmin")) == 0 && strlen(name) == strlen("rolpolicyadmin")) {
        t_thrd.bootstrap_cxt.attrtypes[attnum]->attnotnull = true;
    }
}

/* ----------------
 * DEFINEATTR()
 *
 * define a <field,type> pair
 * if there are n fields in a relation to be created, this routine
 * will be called n times
 * ----------------
 */
void DefineAttr(const char* name, char* type, int attnum)
{
    Oid typeoid;
    if (t_thrd.bootstrap_cxt.boot_reldesc != NULL) {
        ereport(WARNING, (errmsg("no open relations allowed with CREATE command")));
        closerel(NULL);
    }

    if (t_thrd.bootstrap_cxt.attrtypes[attnum] == NULL)
        t_thrd.bootstrap_cxt.attrtypes[attnum] = AllocateAttribute();
    MemSet(t_thrd.bootstrap_cxt.attrtypes[attnum], 0, ATTRIBUTE_FIXED_PART_SIZE);

    (void)namestrcpy(&t_thrd.bootstrap_cxt.attrtypes[attnum]->attname, name);
    ereport(DEBUG4, (errmsg("column %s %s", NameStr(t_thrd.bootstrap_cxt.attrtypes[attnum]->attname), type)));
    t_thrd.bootstrap_cxt.attrtypes[attnum]->attnum = attnum + 1; /* fillatt */

    typeoid = gettype(type);

    if (t_thrd.bootstrap_cxt.Typ != NULL) {
        t_thrd.bootstrap_cxt.attrtypes[attnum]->atttypid = t_thrd.bootstrap_cxt.Ap->am_oid;
        t_thrd.bootstrap_cxt.attrtypes[attnum]->attlen = t_thrd.bootstrap_cxt.Ap->am_typ.typlen;
        t_thrd.bootstrap_cxt.attrtypes[attnum]->attbyval = t_thrd.bootstrap_cxt.Ap->am_typ.typbyval;
        t_thrd.bootstrap_cxt.attrtypes[attnum]->attstorage = t_thrd.bootstrap_cxt.Ap->am_typ.typstorage;
        t_thrd.bootstrap_cxt.attrtypes[attnum]->attalign = t_thrd.bootstrap_cxt.Ap->am_typ.typalign;
        t_thrd.bootstrap_cxt.attrtypes[attnum]->attcollation = t_thrd.bootstrap_cxt.Ap->am_typ.typcollation;
        /* if an array type, assume 1-dimensional attribute */
        if (t_thrd.bootstrap_cxt.Ap->am_typ.typelem != InvalidOid && t_thrd.bootstrap_cxt.Ap->am_typ.typlen < 0)
            t_thrd.bootstrap_cxt.attrtypes[attnum]->attndims = 1;
        else
            t_thrd.bootstrap_cxt.attrtypes[attnum]->attndims = 0;
    } else {
        t_thrd.bootstrap_cxt.attrtypes[attnum]->atttypid = TypInfo[typeoid].oid;
        t_thrd.bootstrap_cxt.attrtypes[attnum]->attlen = TypInfo[typeoid].len;
        t_thrd.bootstrap_cxt.attrtypes[attnum]->attbyval = TypInfo[typeoid].byval;
        t_thrd.bootstrap_cxt.attrtypes[attnum]->attstorage = TypInfo[typeoid].storage;
        t_thrd.bootstrap_cxt.attrtypes[attnum]->attalign = TypInfo[typeoid].align;
        t_thrd.bootstrap_cxt.attrtypes[attnum]->attcollation = TypInfo[typeoid].collation;
        /* if an array type, assume 1-dimensional attribute */
        if (TypInfo[typeoid].elem != InvalidOid && t_thrd.bootstrap_cxt.attrtypes[attnum]->attlen < 0)
            t_thrd.bootstrap_cxt.attrtypes[attnum]->attndims = 1;
        else
            t_thrd.bootstrap_cxt.attrtypes[attnum]->attndims = 0;
    }

    t_thrd.bootstrap_cxt.attrtypes[attnum]->attstattarget = -1;
    t_thrd.bootstrap_cxt.attrtypes[attnum]->attcacheoff = -1;
    t_thrd.bootstrap_cxt.attrtypes[attnum]->atttypmod = -1;
    t_thrd.bootstrap_cxt.attrtypes[attnum]->attislocal = true;

    /*
     * Mark as "not null" if type is fixed-width and prior columns are too.
     * This corresponds to case where column can be accessed directly via C
     * struct declaration.
     *
     * oidvector and int2vector are also treated as not-nullable, even though
     * they are no longer fixed-width.
     */
#define MARKNOTNULL(att) ((att)->attlen > 0 || (att)->atttypid == OIDVECTOROID || (att)->atttypid == INT2VECTOROID)

    if (MARKNOTNULL(t_thrd.bootstrap_cxt.attrtypes[attnum])) {
        int i;

        for (i = 0; i < attnum; i++) {
            if (!MARKNOTNULL(t_thrd.bootstrap_cxt.attrtypes[i]))
                break;
        }
        if (i == attnum)
            t_thrd.bootstrap_cxt.attrtypes[attnum]->attnotnull = true;
    }

    // fix partkey/intervaltablespace/intspnum columns of pg_partition to nullable
    if (strcmp(name, "partkey") == 0 || strcmp(name, "intervaltablespace") == 0 || strcmp(name, "intspnum") == 0) {
        t_thrd.bootstrap_cxt.attrtypes[attnum]->attnotnull = false;
    }

    // fix roluseft ,rolmonitoradmin, roloperatoradmin and rolpolicyadmin column of pg_authid to notnull
    fix_attr_notnull(name, attnum);

}

/* ----------------
 *      ChangePgClassBucketValueForSegment
 * ----------------
 */
static inline void ChangePgClassBucketValueForSegment()
{
    values[Anum_pg_class_relbucket - 1] = ObjectIdGetDatum(VirtualSegmentOid);
    Nulls[Anum_pg_class_relbucket - 1] = false;
}

/* ----------------
 *		InsertOneTuple
 *
 * If objectid is not zero, it is a specific OID to assign to the tuple.
 * Otherwise, an OID will be assigned (if necessary) by heap_insert.
 * ----------------
 */
void InsertOneTuple(Oid objectid)
{
    HeapTuple tuple;
    TupleDesc tupDesc;
    int i;

    ereport(DEBUG4, (errmsg("inserting row oid %u, %d columns", objectid, t_thrd.bootstrap_cxt.numattr)));

    if (IsBootingPgProc(t_thrd.bootstrap_cxt.boot_reldesc)) {
        ereport(FATAL, (errmsg("Built-in functions should not be added into pg_proc")));
    }

    if (IsBootingPgClass(t_thrd.bootstrap_cxt.boot_reldesc) && EnableInitDBSegment) {
        ChangePgClassBucketValueForSegment();
    }
    tupDesc = CreateTupleDesc(t_thrd.bootstrap_cxt.numattr,
        RelationGetForm(t_thrd.bootstrap_cxt.boot_reldesc)->relhasoids,
        t_thrd.bootstrap_cxt.attrtypes,
        t_thrd.bootstrap_cxt.boot_reldesc->rd_tam_ops);
    tuple = (HeapTuple) tableam_tops_form_tuple(tupDesc, values, Nulls);
    if (objectid != (Oid)0)
        HeapTupleSetOid(tuple, objectid);
    pfree(tupDesc); /* just free's tupDesc, not the attrtypes */

    (void)simple_heap_insert(t_thrd.bootstrap_cxt.boot_reldesc, tuple);
    tableam_tops_free_tuple(tuple);
    ereport(DEBUG4, (errmsg("row inserted")));

    /*
     * Reset null markers for next tuple
     */
    for (i = 0; i < t_thrd.bootstrap_cxt.numattr; i++)
        Nulls[i] = false;
}

/* ----------------
 *		InsertOneValue
 * ----------------
 */
void InsertOneValue(char* value, int i)
{
    Oid typoid;
    int16 typlen;
    bool typbyval = false;
    char typalign;
    char typdelim;
    Oid typioparam;
    Oid typinput;
    Oid typoutput;

    AssertArg(i >= 0 && i < MAXATTR);

    ereport(DEBUG4, (errmsg("inserting column %d value \"%s\"", i, value)));

    typoid = t_thrd.bootstrap_cxt.boot_reldesc->rd_att->attrs[i].atttypid;

    boot_get_type_io_data(typoid, &typlen, &typbyval, &typalign, &typdelim, &typioparam, &typinput, &typoutput);

    values[i] = OidInputFunctionCall(typinput, value, typioparam, -1);
    ereport(DEBUG4, (errmsg("inserted -> %s", OidOutputFunctionCall(typoutput, values[i]))));
}

/* ----------------
 *		InsertOneNull
 * ----------------
 */
void InsertOneNull(int i)
{
    ereport(DEBUG4, (errmsg("inserting column %d NULL", i)));
    Assert(i >= 0 && i < MAXATTR);
    values[i] = PointerGetDatum(NULL);
    Nulls[i] = true;
}

/* ----------------
 *		cleanup
 * ----------------
 */
static void cleanup(void)
{
    if (t_thrd.bootstrap_cxt.boot_reldesc != NULL)
        closerel(NULL);
}

/* ----------------
 *		gettype
 *
 * NB: this is really ugly; it will return an integer index into TypInfo[],
 * and not an OID at all, until the first reference to a type not known in
 * TypInfo[].  At that point it will read and cache pg_type in the Typ array,
 * and subsequently return a real OID (and set the global pointer Ap to
 * point at the found row in Typ).	So caller must check whether Typ is
 * still NULL to determine what the return value is!
 * ----------------
 */
static Oid gettype(char* type)
{
    int i;
    Relation rel;
    TableScanDesc scan;
    HeapTuple tup;
    struct typmap** app;
    errno_t rc;

    if (t_thrd.bootstrap_cxt.Typ != NULL) {
        for (app = t_thrd.bootstrap_cxt.Typ; *app != NULL; app++) {
            if (strncmp(NameStr((*app)->am_typ.typname), type, NAMEDATALEN) == 0) {
                t_thrd.bootstrap_cxt.Ap = *app;
                return (*app)->am_oid;
            }
        }
    } else {
        for (i = 0; i < n_types; i++) {
            if (strncmp(type, TypInfo[i].name, NAMEDATALEN) == 0)
                return i;
        }
        ereport(DEBUG4, (errmsg("external type: %s", type)));
        rel = heap_open(TypeRelationId, NoLock);
        scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
        i = 0;
        while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL)
            ++i;
        tableam_scan_end(scan);
        app = t_thrd.bootstrap_cxt.Typ = ALLOC(struct typmap*, i + 1);
        while (i-- > 0)
            *app++ = ALLOC(struct typmap, 1);
        *app = NULL;
        scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
        app = t_thrd.bootstrap_cxt.Typ;
        while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
            (*app)->am_oid = HeapTupleGetOid(tup);
            rc = memmove_s(
                (char*)&(*app++)->am_typ, sizeof((*app)->am_typ), (char*)GETSTRUCT(tup), sizeof((*app)->am_typ));
            securec_check(rc, "\0", "\0");
        }
        tableam_scan_end(scan);
        heap_close(rel, NoLock);
        return gettype(type);
    }
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("unrecognized type \"%s\"", type)));
    /* not reached, here to make compiler happy */
    return 0;
}

/* ----------------
 *		boot_get_type_io_data
 *
 * Obtain type I/O information at bootstrap time.  This intentionally has
 * almost the same API as lsyscache.c's get_type_io_data, except that
 * we only support obtaining the typinput and typoutput routines, not
 * the binary I/O routines.  It is exported so that array_in and array_out
 * can be made to work during early bootstrap.
 * ----------------
 */
void boot_get_type_io_data(Oid typid, int16* typlen, bool* typbyval, char* typalign, char* typdelim, Oid* typioparam,
    Oid* typinput, Oid* typoutput)
{
    if (t_thrd.bootstrap_cxt.Typ != NULL) {
        /* We have the boot-time contents of pg_type, so use it */
        struct typmap** app;
        struct typmap* ap = NULL;

        app = t_thrd.bootstrap_cxt.Typ;
        while (*app && (*app)->am_oid != typid)
            ++app;
        ap = *app;
        if (ap == NULL)
            ereport(
                ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("type OID %u not found in Typ list", typid)));

        *typlen = ap->am_typ.typlen;
        *typbyval = ap->am_typ.typbyval;
        *typalign = ap->am_typ.typalign;
        *typdelim = ap->am_typ.typdelim;

        /* XXX this logic must match getTypeIOParam() */
        if (OidIsValid(ap->am_typ.typelem))
            *typioparam = ap->am_typ.typelem;
        else
            *typioparam = typid;

        *typinput = ap->am_typ.typinput;
        *typoutput = ap->am_typ.typoutput;
    } else {
        /* We don't have pg_type yet, so use the hard-wired TypInfo array */
        int typeindex;

        for (typeindex = 0; typeindex < n_types; typeindex++) {
            if (TypInfo[typeindex].oid == typid)
                break;
        }
        if (typeindex >= n_types)
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("type OID %u not found in TypInfo", typid)));

        *typlen = TypInfo[typeindex].len;
        *typbyval = TypInfo[typeindex].byval;
        *typalign = TypInfo[typeindex].align;
        /* We assume typdelim is ',' for all boot-time types */
        *typdelim = ',';

        /* XXX this logic must match getTypeIOParam() */
        if (OidIsValid(TypInfo[typeindex].elem))
            *typioparam = TypInfo[typeindex].elem;
        else
            *typioparam = typid;

        *typinput = TypInfo[typeindex].inproc;
        *typoutput = TypInfo[typeindex].outproc;
    }
}

/* ----------------
 *		AllocateAttribute
 *
 * Note: bootstrap never sets any per-column ACLs, so we only need
 * ATTRIBUTE_FIXED_PART_SIZE space per attribute.
 * ----------------
 */
static Form_pg_attribute AllocateAttribute(void)
{
    Form_pg_attribute attribute = (Form_pg_attribute)MemoryContextAlloc(
        SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), ATTRIBUTE_FIXED_PART_SIZE);

    if (!PointerIsValid(attribute))
        ereport(FATAL, (errmsg("out of memory")));
    MemSet(attribute, 0, ATTRIBUTE_FIXED_PART_SIZE);

    return attribute;
}

/* ----------------
 *		MapArrayTypeName
 * XXX arrays of "basetype" are always "_basetype".
 *	   this is an evil hack inherited from rel. 3.1.
 * XXX array dimension is thrown away because we
 *	   don't support fixed-dimension arrays.  again,
 *	   sickness from 3.1.
 *
 * the string passed in must have a '[' character in it
 *
 * the string returned is a pointer to static storage and should NOT
 * be freed by the CALLER.
 * ----------------
 */
const char* MapArrayTypeName(const char* s)
{
    int i;
    int j;

    if (s == NULL || s[0] == '\0')
        return s;

    j = 1;
    t_thrd.bootstrap_cxt.newStr[0] = '_';
    for (i = 0; i < NAMEDATALEN - 1 && s[i] != '['; i++, j++)
        t_thrd.bootstrap_cxt.newStr[j] = s[i];

    t_thrd.bootstrap_cxt.newStr[j] = '\0';

    return t_thrd.bootstrap_cxt.newStr;
}

/*
 *	index_register() -- record an index that has been set up for building
 *						later.
 *
 *		At bootstrap time, we define a bunch of indexes on system catalogs.
 *		We postpone actually building the indexes until just before we're
 *		finished with initialization, however.	This is because the indexes
 *		themselves have catalog entries, and those have to be included in the
 *		indexes on those catalogs.	Doing it in two phases is the simplest
 *		way of making sure the indexes have the right contents at the end.
 */
void index_register(Oid heap, Oid ind, IndexInfo* indexInfo)
{
    IndexList* newind = NULL;
    MemoryContext oldcxt;
    errno_t rc;

    /*
     * XXX mao 10/31/92 -- don't gc index reldescs, associated info at
     * bootstrap time.	we'll declare the indexes now, but want to create them
     * later.
     */
    if (t_thrd.bootstrap_cxt.nogc == NULL)
        t_thrd.bootstrap_cxt.nogc = AllocSetContextCreate(
            NULL, "BootstrapNoGC", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    oldcxt = MemoryContextSwitchTo(t_thrd.bootstrap_cxt.nogc);

    newind = (IndexList*)palloc(sizeof(IndexList));
    newind->il_heap = heap;
    newind->il_ind = ind;
    newind->il_info = (IndexInfo*)palloc(sizeof(IndexInfo));

    rc = memcpy_s(newind->il_info, sizeof(IndexInfo), indexInfo, sizeof(IndexInfo));
    securec_check(rc, "\0", "\0");
    /* expressions will likely be null, but may as well copy it */
    newind->il_info->ii_Expressions = (List*)copyObject(indexInfo->ii_Expressions);
    newind->il_info->ii_ExpressionsState = NIL;
    /* predicate will likely be null, but may as well copy it */
    newind->il_info->ii_Predicate = (List*)copyObject(indexInfo->ii_Predicate);
    newind->il_info->ii_PredicateState = NIL;
    /* no exclusion constraints at bootstrap time, so no need to copy */
    Assert(indexInfo->ii_ExclusionOps == NULL);
    Assert(indexInfo->ii_ExclusionProcs == NULL);
    Assert(indexInfo->ii_ExclusionStrats == NULL);

    newind->il_next = t_thrd.bootstrap_cxt.ILHead;
    t_thrd.bootstrap_cxt.ILHead = newind;

    (void)MemoryContextSwitchTo(oldcxt);
}

/*
 * build_indices -- fill in all the indexes registered earlier
 */
void build_indices(void)
{
    for (; t_thrd.bootstrap_cxt.ILHead != NULL; t_thrd.bootstrap_cxt.ILHead = t_thrd.bootstrap_cxt.ILHead->il_next) {
        Relation heap;
        Relation ind;

        /* need not bother with locks during bootstrap */
        heap = heap_open(t_thrd.bootstrap_cxt.ILHead->il_heap, NoLock);
        ind = index_open(t_thrd.bootstrap_cxt.ILHead->il_ind, NoLock);
        index_build(
            heap, NULL, ind, NULL, t_thrd.bootstrap_cxt.ILHead->il_info, false, false, INDEX_CREATE_NONE_PARTITION);

        index_close(ind, NoLock);
        heap_close(heap, NoLock);
    }
}
