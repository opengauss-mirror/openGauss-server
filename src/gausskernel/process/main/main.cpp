/*
 *
 * main.cpp
 *	  Stub main() routine for the openGauss executable.
 *
 * This does some essential startup tasks for any incarnation of openGauss
 * (postmaster, standalone backend, standalone bootstrap process, or a
 * separately exec'd child of a postmaster) and then dispatches to the
 * proper FooMain() routine for the incarnation.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/main/main.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <pwd.h>
#include <unistd.h>

#if defined(__alpha) && defined(__osf__) /* no __alpha__ ? */
#include <sys/sysinfo.h>
#include "machine/hal_sysinfo.h"
#define ASSEMBLER
#include <sys/proc.h>
#undef ASSEMBLER
#endif

#if defined(__NetBSD__)
#include <sys/param.h>
#endif

#include "bootstrap/bootstrap.h"
#include "postmaster/postmaster.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"
#include "utils/fmgrtab.h"
#include "utils/help_config.h"
#include "utils/pg_locale.h"
#include "utils/ps_status.h"
#ifdef WIN32
#include "libpq/pqsignal.h"
#endif

#include "utils/syscall_lock.h"
#include "gssignal/gs_signal.h"
#include "utils/memutils.h"
#include "utils/mmpool.h"
#include "utils/plog.h"
#include "gstrace/gstrace_infra.h"

#include <sys/prctl.h>

THR_LOCAL bool IsInitdb = false;

size_t mmap_threshold = (size_t)0xffffffff;

const char* progname = NULL;

static void startup_hacks(const char* progname);
static void help(const char* progname);
static void check_root(const char* progname);
static char* get_current_username(const char* progname);
static void syscall_lock_init(void);

extern int encrypte_main(int argc, char* const argv[]);

/*
 * Any openGauss server process begins execution here.
 */
int main(int argc, char* argv[])
{
    char* mmap_env = NULL;

    /* disable THP (transparent huge page) early before mallocs happen */
#if defined(ENABLE_LITE_MODE) && defined(__linux__) && defined(PR_SET_THP_DISABLE)
    prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0);
#endif

    syscall_lock_init();

    mmap_env = gs_getenv_r("GAUSS_MMAP_THRESHOLD");
    if (mmap_env != NULL) {
        check_backend_env(mmap_env);
        mmap_threshold = (size_t)atol(mmap_env);
    }

    knl_instance_init();

    g_instance.increCheckPoint_context = AllocSetContextCreate(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE),
        "IncreCheckPointContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    g_instance.account_context = AllocSetContextCreate(g_instance.instance_context,
        "StandbyAccontContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);
		
    g_instance.comm_cxt.comm_global_mem_cxt = AllocSetContextCreate(g_instance.instance_context,
        "CommunnicatorGlobalMemoryContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    g_instance.builtin_proc_context = AllocSetContextCreate(g_instance.instance_context,
        "builtin_procGlobalMemoryContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);
    /*
     * Fire up essential subsystems: error and memory management
     *
     * Code after this point is allowed to use elog/ereport, though
     * localization of messages may not work right away, and messages won't go
     * anywhere but stderr until GUC settings get loaded.
     */
    MemoryContextInit();

    PmTopMemoryContext = t_thrd.top_mem_cxt;

    knl_thread_init(MASTER_THREAD);

    t_thrd.fake_session = create_session_context(t_thrd.top_mem_cxt, 0);
    t_thrd.fake_session->status = KNL_SESS_FAKE;

    u_sess = t_thrd.fake_session;

    SelfMemoryContext = THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT);

    MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));

    progname = get_progname(argv[0]);

#ifdef PROFILE_PID_DIR
{
    char* gmon_env = NULL;
    gmon_env = gs_getenv_r("GMON_OUT_PREFIX");
    if(gmon_env == NULL){
        if (gs_putenv_r("GMON_OUT_PREFIX=gmon.out") == -1) {
            ereport(WARNING,
                (errmsg("Failed to set ENV, cannot output gmon.out under multi-progress: EnvName=%s,"
                        " Errno=%d, Errmessage=%s.",
                    "GMON_OUT_PREFIX",
                    errno,
                    gs_strerror(errno))));
        }
    }
}
#endif

    /*
     * Platform-specific startup hacks
     */
    startup_hacks(progname);

    /* if gaussdb's name is gs_encrypt, so run in encrypte_main() */
    if (!strcmp(progname, "gs_encrypt")) {
        return encrypte_main(argc, argv);
    }

    init_plog_global_mem();

    /*
     * Remember the physical location of the initially given argv[] array for
     * possible use by ps display.	On some platforms, the argv[] storage must
     * be overwritten in order to set the process title for ps. In such cases
     * save_ps_display_args makes and returns a new copy of the argv[] array.
     *
     * save_ps_display_args may also move the environment strings to make
     * extra room. Therefore this should be done as early as possible during
     * startup, to avoid entanglements with code that might save a getenv()
     * result pointer.
     */
    argv = save_ps_display_args(argc, argv);

    /*
     * If supported on the current platform, set up a handler to be called if
     * the backend/postmaster crashes with a fatal signal or exception.
     */
#if defined(WIN32) && defined(HAVE_MINIDUMP_TYPE)
    pgwin32_install_crashdump_handler();
#endif

    /*
     * Set up locale information from environment.	Note that LC_CTYPE and
     * LC_COLLATE will be overridden later from pg_control if we are in an
     * already-initialized database.  We set them here so that they will be
     * available to fill pg_control during initdb.	LC_MESSAGES will get set
     * later during GUC option processing, but we set it here to allow startup
     * error messages to be localized.
     */
    set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("gaussdb"));

#ifdef WIN32

    /*
     * Windows uses codepages rather than the environment, so we work around
     * that by querying the environment explicitly first for LC_COLLATE and
     * LC_CTYPE. We have to do this because initdb passes those values in the
     * environment. If there is nothing there we fall back on the codepage.
     */
    {
        char* env_locale = NULL;

        if ((env_locale = gs_getenv_r("LC_COLLATE")) != NULL) {
            check_backend_env(env_locale);
            pg_perm_setlocale(LC_COLLATE, env_locale);
        } else
            pg_perm_setlocale(LC_COLLATE, "");

        if ((env_locale = gs_getenv_r("LC_CTYPE")) != NULL) {
            check_backend_env(env_locale);
            pg_perm_setlocale(LC_CTYPE, env_locale);
        } else
            pg_perm_setlocale(LC_CTYPE, "");
    }
#else
    pg_perm_setlocale(LC_COLLATE, "");
    pg_perm_setlocale(LC_CTYPE, "");
#endif

    /*
     * LC_MESSAGES will get set later during GUC option processing, but we set
     * it here to allow startup error messages to be localized.
     */
#if defined(ENABLE_NLS) && defined(LC_MESSAGES)
    pg_perm_setlocale(LC_MESSAGES, "");
#endif

    /*
     * We keep these set to "C" always, except transiently in pg_locale.c; see
     * that file for explanations.
     */
    pg_perm_setlocale(LC_MONETARY, "C");
    pg_perm_setlocale(LC_NUMERIC, "C");
    pg_perm_setlocale(LC_TIME, "C");

    /*
     * Now that we have absorbed as much as we wish to from the locale
     * environment, remove any LC_ALL setting, so that the environment
     * variables installed by pg_perm_setlocale have force.
     */
    (void)unsetenv("LC_ALL");

    /*
     * Catch standard options before doing much else
     */
    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            help(progname);
            exit(0);
        }
        if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0) {
            puts("gaussdb " DEF_GS_VERSION);
            exit(0);
        }
    }

    /*
     * Make sure we are not running as root.
     */
    check_root(progname);

    /*
     * Dispatch to one of various subprograms depending on first argument.
     */
#ifdef WIN32

    /*
     * Start our win32 signal implementation
     *
     * SubPostmasterMain() will do this for itself, but the remaining modes
     * need it here
     */
    pgwin32_signal_initialize();
#endif

    t_thrd.mem_cxt.gs_signal_mem_cxt = AllocSetContextCreate(
        t_thrd.top_mem_cxt, "gs_signal", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    if (NULL == t_thrd.mem_cxt.gs_signal_mem_cxt) {
        ereport(LOG, (errmsg("could not start a new thread, because of no  enough system resource. ")));
        proc_exit(1);
    }

    /*
     * @BuiltinFunc
     * Create a global BuiltinFunc object shared among threads
     */
    if (g_sorted_funcs[0] == NULL) {
        initBuiltinFuncs();
    }

    bool isBoot = (argc > 1 && strcmp(argv[1], "--boot") == 0);
    if (isBoot) {
        IsInitdb = true;
        gs_signal_monitor_startup();
        gs_signal_slots_init(1);
        (void)gs_signal_unblock_sigusr2();
        gs_signal_startup_siginfo("AuxiliaryProcessMain");
        BootStrapProcessMain(argc, argv); /* does not return */
    }

    if (argc > 1 && strcmp(argv[1], "--describe-config") == 0)
        exit(GucInfoMain());

    if (argc > 1 && strcmp(argv[1], "--single") == 0) {
        IsInitdb = true;
        gs_signal_monitor_startup();
        gs_signal_slots_init(1);
        (void)gs_signal_unblock_sigusr2();
        gs_signal_startup_siginfo("PostgresMain");

        exit(PostgresMain(argc, argv, NULL, get_current_username(progname)));
    }

    exit(PostmasterMain(argc, argv));
}

/*
 * Place platform-specific startup hacks here.	This is the right
 * place to put code that must be executed early in the launch of any new
 * server process.	Note that this code will NOT be executed when a backend
 * or sub-bootstrap process is forked, unless we are in a fork/exec
 * environment (ie EXEC_BACKEND is defined).
 *
 * XXX The need for code here is proof that the platform in question
 * is too brain-dead to provide a standard C execution environment
 * without help.  Avoid adding more here, if you can.
 */
static void startup_hacks(const char* progname)
{
    /*
     * On some platforms, unaligned memory accesses result in a kernel trap;
     * the default kernel behavior is to emulate the memory access, but this
     * results in a significant performance penalty.  We want PG never to make
     * such unaligned memory accesses, so this code disables the kernel
     * emulation: unaligned accesses will result in SIGBUS instead.
     */
#ifdef NOFIXADE

#if defined(__alpha) /* no __alpha__ ? */
    {
        int buffer[] = {SSIN_UACPROC, UAC_SIGBUS | UAC_NOPRINT};

        if (setsysinfo(SSI_NVPAIRS, buffer, 1, (caddr_t)NULL, (unsigned long)NULL) < 0)
            write_stderr("%s: setsysinfo failed: %s\n", progname, gs_strerror(errno));
    }
#endif /* __alpha */
#endif /* NOFIXADE */

    /*
     * Windows-specific execution environment hacking.
     */
#ifdef WIN32
    {
        WSADATA wsaData;
        int err;

        /* Make output streams unbuffered by default */
        setvbuf(stdout, NULL, _IONBF, 0);
        setvbuf(stderr, NULL, _IONBF, 0);

        /* Prepare Winsock */
        err = WSAStartup(MAKEWORD(2, 2), &wsaData);
        if (err != 0) {
            write_stderr("%s: WSAStartup failed: %d\n", progname, err);
            exit(1);
        }

        /* In case of general protection fault, don't show GUI popup box */
        SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
    }
#endif /* WIN32 */

    /* Binding static TLS variables for current thread */
    EarlyBindingTLSVariables();
}

/*
 * Help display should match the options accepted by PostmasterMain()
 * and PostgresMain().
 */
static void help(const char* progname)
{
    printf(_("%s is the gaussdb server.\n\n"), progname);
    printf(_("Usage:\n  %s [OPTION]...\n\n"), progname);
    printf(_("Options:\n"));
#ifdef USE_ASSERT_CHECKING
    printf(_("  -A 1|0             enable/disable run-time assert checking\n"));
#endif
    printf(_("  -B NBUFFERS        number of shared buffers\n"));
    printf(_("  -b BINARY UPGRADES flag used for binary upgrades\n"));
    printf(_("  -c NAME=VALUE      set run-time parameter\n"));
    printf(_("  -C NAME            print value of run-time parameter, then exit\n"));
    printf(_("  -d 1-5             debugging level\n"));
    printf(_("  -D DATADIR         database directory\n"));
    printf(_("  -e                 use European date input format (DMY)\n"));
    printf(_("  -F                 turn fsync off\n"));
    printf(_("  -h HOSTNAME        host name or IP address to listen on\n"));
    printf(_("  -i                 enable TCP/IP connections\n"));
    printf(_("  -k DIRECTORY       Unix-domain socket location\n"));
#ifdef USE_SSL
    printf(_("  -l                 enable SSL connections\n"));
#endif
    printf(_("  -N MAX-CONNECT     maximum number of allowed connections\n"));
    printf(_("  -M SERVERMODE      the database start as the appointed server mode\n"));

    printf(_("  -o OPTIONS         pass \"OPTIONS\" to each server process (obsolete)\n"));
    printf(_("  -p PORT            port number to listen on\n"));
#ifdef ENABLE_MULTIPLE_NODES
    printf(_("  -R                 indicate run as xlogreiver.Only used with -M standby\n"));
#endif
    printf(_("  -s                 show statistics after each query\n"));
    printf(_("  -S WORK-MEM        set amount of memory for sorts (in kB)\n"));
    printf(_("  -u NUM             set the num of kernel version before upgrade\n"));
    printf(_("  -V, --version      output version information, then exit\n"));
    printf(_("  --NAME=VALUE       set run-time parameter\n"));
    printf(_("  --describe-config  describe configuration parameters, then exit\n"));
    printf(_("  --securitymode     allow database system run in security mode\n"));
    printf(_("  --single_node      A SingleDN mode is being activated\n"));
    printf(_("  -?, --help         show this help, then exit\n"));

    printf(_("\nServer mode:\n"));
    printf(_("      primary        database system starts as a primary server, send xlog to standby server\n"));
    printf(_("      standby        database system starts as a standby server, receive xlog from primary server\n"));
    printf(_("      pending        database system starts as a pending server, wait for promoting to primary or "
             "demoting to standby\n"));
#ifdef ENABLE_MULTIPLE_NODES
    printf(_("      fenced         database system starts a fenced master process, serve UDF execution in secure mode "
             "(run separately from Gaussdb process)\n"));
#endif

    printf(_("\nDeveloper options:\n"));
    printf(_("  -f s|i|n|m|h       forbid use of some plan types\n"));
    printf(_("  -n                 do not reinitialize shared memory after abnormal exit\n"));
    printf(_("  -O                 allow system table structure changes\n"));
    printf(_("  -P                 disable system indexes\n"));
    printf(_("  -t pa|pl|ex        show timings after each query\n"));
    printf(_("  -T                 send SIGSTOP to all backend processes if one dies\n"));
    printf(_("  -W NUM             wait NUM seconds to allow attach from a debugger\n"));
    printf(_("  --localxid         use local transaction id (used only by gs_initdb)\n"));

    printf(_("\nOptions for single-user mode:\n"));
    printf(_("  --single           selects single-user mode (must be first argument)\n"));
    printf(_("  DBNAME             database name (defaults to user name)\n"));
    printf(_("  -d 0-5             override debugging level\n"));
    printf(_("  -E                 echo statement before execution\n"));
    printf(_("  -j                 do not use newline as interactive query delimiter\n"));
    printf(_("  -r FILENAME        send stdout and stderr to given file\n"));

    printf(_("\nOptions for bootstrapping mode:\n"));
    printf(_("  --boot             selects bootstrapping mode (must be first argument)\n"));
#ifdef ENABLE_MULTIPLE_NODES
    printf(_("  DBNAME             database name (mandatory argument in bootstrapping mode)\n"));
#endif
    printf(_("  -r FILENAME        send stdout and stderr to given file\n"));
    printf(_("  -x NUM             internal use\n"));
    printf(_("  -G                 store tables in segment page while initdb\n"));

#ifdef ENABLE_MULTIPLE_NODES
    printf(_("\nNode options:\n"));
#ifdef ENABLE_MULTIPLE_NODES
    printf(_("  --coordinator      start as a Coordinator\n"));
    printf(_("  --datanode         start as a Datanode\n"));
#endif
    printf(_("  --restoremode      start to restore existing schema on the new node to be added\n"));
    printf(_("  --single_node      start as single node\n"));
#else
    printf(_("\nNode options:\n"));
    printf(_("  --single_node      start a single node database. This is default setting.\n"));
#endif

    printf(_("\nPlease read the documentation for the complete list of run-time\n"
             "configuration settings and how to set them on the command line or in\n"
             "the configuration file.\n"));
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    printf(_("\nReport bugs to GaussDB support.\n"));
#else
    printf(_("\nReport bugs to openGauss community by raising an issue.\n"));
#endif
}

static void check_root(const char* progname)
{
#ifndef WIN32
    if (geteuid() == 0) {
        write_stderr("\"root\" execution of the gaussdb server is not permitted.\n"
                     "The server must be started under an unprivileged user ID to prevent\n"
                     "possible system security compromise.  See the documentation for\n"
                     "more information on how to properly start the server.\n");
        exit(1);
    }

    /*
     * Also make sure that real and effective uids are the same. Executing as
     * a setuid program from a root shell is a security hole, since on many
     * platforms a nefarious subroutine could setuid back to root if real uid
     * is root.  (Since nobody actually uses postgres as a setuid program,
     * trying to actively fix this situation seems more trouble than it's
     * worth; we'll just expend the effort to check for it.)
     */
    if (getuid() != geteuid()) {
        write_stderr("%s: real and effective user IDs must match\n", progname);
        exit(1);
    }
#else  /* WIN32 */
    if (pgwin32_is_admin()) {
        write_stderr("Execution of gaussdb by a user with administrative permissions is not\n"
                     "permitted.\n"
                     "The server must be started under an unprivileged user ID to prevent\n"
                     "possible system security compromises.  See the documentation for\n"
                     "more information on how to properly start the server.\n");
        exit(1);
    }
#endif /* WIN32 */
}

static char* get_current_username(const char* progname)
{
#ifndef WIN32
    struct passwd* pw = NULL;
    char* pRet = NULL;

    (void)syscalllockAcquire(&getpwuid_lock);
    pw = getpwuid(geteuid());
    if (pw == NULL) {
        (void)syscalllockRelease(&getpwuid_lock);
        write_stderr("%s: invalid effective UID: %d\n", progname, (int)geteuid());
        exit(1);
    }
    /* Allocate new memory because later getpwuid() calls can overwrite it. */
    pRet = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), pw->pw_name);
    (void)syscalllockRelease(&getpwuid_lock);
    return pRet;
#else
    unsigned long namesize = 256 /* UNLEN */ + 1;
    char* name = NULL;

    name = MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB), namesize);
    if (!GetUserName(name, &namesize)) {
        write_stderr("%s: could not determine user name (GetUserName failed)\n", progname);
        exit(1);
    }

    return name;
#endif
}

static void syscall_lock_init(void)
{
    syscalllockInit(&getpwuid_lock);
    syscalllockInit(&env_lock);
    syscalllockInit(&dlerror_lock);
    syscalllockInit(&kerberos_conn_lock);
    syscalllockInit(&read_cipher_lock);
    syscalllockInit(&file_list_lock);
}
