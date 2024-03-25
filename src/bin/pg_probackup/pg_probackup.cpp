/*-------------------------------------------------------------------------
 *
 * pg_probackup.c: Backup/Recovery manager for PostgreSQL.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2019, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"

#include "getopt_long.h"
#include "streamutil.h"
#include "file.h"

#include <sys/stat.h>

#include "tool_common.h"
#include "configuration.h"
#include "thread.h"
#include <time.h>
#include "common/fe_memutils.h"
#include "storage/file/fio_device.h"
#include "storage/dss/dss_adaptor.h"
#include <sys/resource.h>

#define MIN_ULIMIT_STACK_SIZE 8388608     // 1024 * 1024 * 8

const char  *PROGRAM_NAME = NULL;        /* PROGRAM_NAME_FULL without .exe suffix
                                         * if any */
const char  *PROGRAM_NAME_FULL = NULL;
const char  *PROGRAM_FULL_PATH = NULL;

typedef enum ProbackupSubcmd
{
    NO_CMD = 0,
    INIT_CMD,
    ADD_INSTANCE_CMD,
    DELETE_INSTANCE_CMD,
    BACKUP_CMD,
    RESTORE_CMD,
    VALIDATE_CMD,
    DELETE_CMD,
    MERGE_CMD,
    SHOW_CMD,
    SET_CONFIG_CMD,
    SET_BACKUP_CMD,
    SHOW_CONFIG_CMD,
} ProbackupSubcmd;


/* directory options */
char       *backup_path = NULL;
/*
 * path or to the data files in the backup catalog
 * $BACKUP_PATH/backups/instance_name
 */
char        backup_instance_path[MAXPGPATH];
/*
 * path or to the wal files in the backup catalog
 * $BACKUP_PATH/wal/instance_name
 */
char        arclog_path[MAXPGPATH] = "";

/* colon separated external directories list ("/path1:/path2") */
char       *externaldir = NULL;
/* common options */
static char *backup_id_string = NULL;
int            num_threads = 1;
bool        stream_wal = true;
pid_t       my_pid = 0;
__thread int  my_thread_num = 1;
bool        progress = false;
bool        no_sync = false;
#if PG_VERSION_NUM >= 100000
char       *replication_slot = NULL;
#endif
bool        temp_slot = false;
char       *password = NULL;
int        rw_timeout = 0;

/* backup options */
bool         backup_logs = false;
bool         backup_replslots = false;
bool         smooth_checkpoint;
char        *remote_agent;
static char *backup_note = NULL;
/* restore options */
static char           *target_time = NULL;
static char           *target_xid = NULL;
static char           *target_lsn = NULL;
static char           *target_inclusive = NULL;
static TimeLineID    target_tli;
static char           *target_stop;
static bool            target_immediate;
static char           *target_name = NULL;
static char           *target_action = NULL;

static pgRecoveryTarget *recovery_target_options = NULL;
static pgRestoreParams *restore_params = NULL;

time_t current_time = 0;
bool no_validate = false;
IncrRestoreMode incremental_mode = INCR_NONE;

bool skip_block_validation = false;
bool skip_external_dirs = false;

bool specify_extdir = false;
bool specify_tbsdir = false;

/* delete options */
bool        delete_wal = false;
bool        delete_expired = false;
bool        merge_expired = false;
bool        force = false;
bool        dry_run = false;
static char *delete_status = NULL;
/* compression options */
bool         compress_shortcut = false;

/* other options */
char       *instance_name;

/* archive push options */
int        batch_size = 1;
static char *wal_file_path;
static char *wal_file_name;
static bool file_overwrite = false;
static bool no_ready_rename = false;

/* archive get options */
static char *prefetch_dir;
bool no_validate_wal = false;

/* show options */
ShowFormat show_format = SHOW_PLAIN;
bool show_archive = false;

/* set-backup options */
int64 ttl = -1;
static char *expire_time_string = NULL;
static pgSetBackupParams *set_backup_params = NULL;

/* current settings */
pgBackup    current;
static ProbackupSubcmd backup_subcmd = NO_CMD;

static bool help_opt = false;

static void opt_incr_restore_mode(ConfigOption *opt, const char *arg);
static void opt_backup_mode(ConfigOption *opt, const char *arg);
static void opt_show_format(ConfigOption *opt, const char *arg);

static void compress_init(void);
static void dss_init(void);
static int ss_get_primary_id(void);
static void check_unlimit_stack_size(void);

/*
 * Short name should be non-printable ASCII character.
 * Use values between 128 and 255.
 */
static ConfigOption cmd_options[] =
{
    { 'b', 130, "help",            &help_opt,            SOURCE_CMD_STRICT },
    { 's', 'B', "backup-path",        &backup_path,        SOURCE_CMD_STRICT },
    { 'u', 'j', "threads",            &num_threads,        SOURCE_CMD_STRICT },
    { 'b', 131, "stream",            &stream_wal,        SOURCE_CMD_STRICT },
    { 'b', 132, "progress",            &progress,            SOURCE_CMD_STRICT },
    { 's', 'i', "backup-id",        &backup_id_string,    SOURCE_CMD_STRICT },
    { 'b', 133, "no-sync",            &no_sync,            SOURCE_CMD_STRICT },
    { 'b', 180, "backup-pg-log",    &backup_logs,        SOURCE_CMD_STRICT },
    { 'f', 'b', "backup-mode",        (void *)opt_backup_mode,    SOURCE_CMD_STRICT },
    { 'b', 'C', "smooth-checkpoint", &smooth_checkpoint,    SOURCE_CMD_STRICT },
    { 's', 'S', "slot",                &replication_slot,    SOURCE_CMD_STRICT },
    { 'b', 181, "temp-slot",        &temp_slot,            SOURCE_CMD_STRICT },
    { 'b', 182, "delete-wal",        &delete_wal,        SOURCE_CMD_STRICT },
    { 'b', 183, "delete-expired",    &delete_expired,    SOURCE_CMD_STRICT },
    { 'b', 184, "merge-expired",    &merge_expired,        SOURCE_CMD_STRICT },
    { 'b', 185, "dry-run",            &dry_run,            SOURCE_CMD_STRICT },
    { 's', 238, "note",                &backup_note,        SOURCE_CMD_STRICT },
    { 's', 136, "recovery-target-time",    &target_time,    SOURCE_CMD_STRICT },
    { 's', 137, "recovery-target-xid",    &target_xid,    SOURCE_CMD_STRICT },
    { 's', 144, "recovery-target-lsn",    &target_lsn,    SOURCE_CMD_STRICT },
    { 's', 138, "recovery-target-inclusive",    &target_inclusive,    SOURCE_CMD_STRICT },
    { 'f', 'T', "tablespace-mapping", (void *)opt_tablespace_map,    SOURCE_CMD_STRICT },
    { 'f', 155, "external-mapping",    (void *)opt_externaldir_map,    SOURCE_CMD_STRICT },
    { 's', 141, "recovery-target-name",    &target_name,        SOURCE_CMD_STRICT },
    { 'b', 143, "no-validate",        &no_validate,        SOURCE_CMD_STRICT },
    { 'b', 154, "skip-block-validation", &skip_block_validation,    SOURCE_CMD_STRICT },
    { 'b', 156, "skip-external-dirs", &skip_external_dirs,    SOURCE_CMD_STRICT },
    { 'f', 'I', "incremental-mode", (void *)opt_incr_restore_mode,    SOURCE_CMD_STRICT },
    { 'b', 145, "wal",                &delete_wal,        SOURCE_CMD_STRICT },
    { 'b', 146, "expired",            &delete_expired,    SOURCE_CMD_STRICT },
    { 's', 172, "status",            &delete_status,        SOURCE_CMD_STRICT },
    { 'b', 186, "backup-pg-replslot",   &backup_replslots,    SOURCE_CMD_STRICT},

    { 'b', 147, "force",            &force,                SOURCE_CMD_STRICT },
    { 'b', 148, "compress",            &compress_shortcut,    SOURCE_CMD_STRICT },
    { 'B', 'w', "no-password",        &prompt_password,    SOURCE_CMD_STRICT },
    { 's', 'W', "password",            &password,            SOURCE_CMD_STRICT },
    { 'u', 't', "rw-timeout",          &rw_timeout,           SOURCE_CMD_STRICT },
    { 's', 149, "instance",            &instance_name,        SOURCE_CMD_STRICT },
    { 's', 150, "wal-file-path",    &wal_file_path,        SOURCE_CMD_STRICT },
    { 's', 151, "wal-file-name",    &wal_file_name,        SOURCE_CMD_STRICT },
    { 'b', 152, "overwrite",        &file_overwrite,    SOURCE_CMD_STRICT },
    { 'b', 153, "no-ready-rename",    &no_ready_rename,    SOURCE_CMD_STRICT },
    { 'i', 162, "batch-size",        &batch_size,        SOURCE_CMD_STRICT },
    { 's', 163, "prefetch-dir",        &prefetch_dir,        SOURCE_CMD_STRICT },
    { 'b', 164, "no-validate-wal",    &no_validate_wal,    SOURCE_CMD_STRICT },
    { 'f', 165, "format",            (void *)opt_show_format,    SOURCE_CMD_STRICT },
    { 'b', 166, "archive",            &show_archive,        SOURCE_CMD_STRICT },
    { 'I', 170, "ttl", &ttl, SOURCE_CMD_STRICT, SOURCE_DEFAULT, 0, OPTION_UNIT_S, option_get_value},
    { 's', 171, "expire-time",        &expire_time_string,    SOURCE_CMD_STRICT },

    { 's', 136, "time",                &target_time,        SOURCE_CMD_STRICT },
    { 's', 137, "xid",                &target_xid,        SOURCE_CMD_STRICT },
    { 's', 138, "inclusive",        &target_inclusive,    SOURCE_CMD_STRICT },
    { 'u', 139, "timeline",            &target_tli,        SOURCE_CMD_STRICT },
    { 's', 144, "lsn",                &target_lsn,        SOURCE_CMD_STRICT },
    { 'b', 140, "immediate",        &target_immediate,    SOURCE_CMD_STRICT },

    { 0 }
};


static void
setMyLocation(void)
{

#ifdef WIN32
    if (IsSshProtocol())
        elog(ERROR, "Currently remote operations on Windows are not supported");
#endif

    MyLocation = IsSshProtocol()
        ? (backup_subcmd == BACKUP_CMD || backup_subcmd == RESTORE_CMD || backup_subcmd == ADD_INSTANCE_CMD)
            ? FIO_BACKUP_HOST
            : FIO_LOCAL_HOST
        : FIO_LOCAL_HOST;
}

static void parse_non_subcommand_option(char *option, int argc, char *argv[])
{
#ifdef WIN32
        if (strcmp(argv[1], "ssh") == 0)
            launch_ssh(argv);
#endif
        if (strcmp(argv[1], "agent") == 0)
        {
            if (argc > 2)
            {
                elog(ERROR, "Version mismatch, gs_probackup binary with version '%s' "
                        "is launched as an agent for gs_probackup binary with version '%s'",
                        PROGRAM_VERSION, argv[2]);
            }
            fio_communicate(STDIN_FILENO, STDOUT_FILENO);
            exit(0);
        }
        else if (strcmp(argv[1], "--help") == 0 ||
                 strcmp(argv[1], "-?") == 0 ||
                 strcmp(argv[1], "help") == 0)
        {
            if (argc > 2)
                help_command(argv[2]);
            else
                help_pg_probackup();
        }
        else if (strcmp(argv[1], "--version") == 0
                 || strcmp(argv[1], "version") == 0
                 || strcmp(argv[1], "-V") == 0)
        {
            puts("gs_probackup " DEF_GS_VERSION);
            exit(0);
        }
        else
            elog(ERROR, "Unknown subcommand \"%s\"", argv[1]);
}

static void parse_options(char *option, int argc, char *argv[])
{
    if (strcmp(option, "add-instance") == 0)
        backup_subcmd = ADD_INSTANCE_CMD;
    else if (strcmp(option, "del-instance") == 0)
        backup_subcmd = DELETE_INSTANCE_CMD;
    else if (strcmp(option, "init") == 0)
        backup_subcmd = INIT_CMD;
    else if (strcmp(option, "backup") == 0)
        backup_subcmd = BACKUP_CMD;
    else if (strcmp(option, "restore") == 0)
        backup_subcmd = RESTORE_CMD;
    else if (strcmp(option, "validate") == 0)
        backup_subcmd = VALIDATE_CMD;
    else if (strcmp(option, "delete") == 0)
        backup_subcmd = DELETE_CMD;
    else if (strcmp(option, "merge") == 0)
        backup_subcmd = MERGE_CMD;
    else if (strcmp(option, "show") == 0)
        backup_subcmd = SHOW_CMD;
    else if (strcmp(option, "set-config") == 0)
        backup_subcmd = SET_CONFIG_CMD;
    else if (strcmp(option, "set-backup") == 0)
        backup_subcmd = SET_BACKUP_CMD;
    else if (strcmp(option, "show-config") == 0)
        backup_subcmd = SHOW_CONFIG_CMD;
    else
        parse_non_subcommand_option(option, argc, argv);
}

static char *make_command_string(int argc, char *argv[])
{
    char *command = NULL;
    errno_t rc = 0;
    
    if (backup_subcmd == BACKUP_CMD ||
        backup_subcmd == RESTORE_CMD ||
        backup_subcmd == VALIDATE_CMD ||
        backup_subcmd == DELETE_CMD ||
        backup_subcmd == MERGE_CMD ||
        backup_subcmd == SET_CONFIG_CMD ||
        backup_subcmd == SET_BACKUP_CMD)
    {
        int            i,
                    len = 0,
                    allocated = 0;

        allocated = sizeof(char) * MAXPGPATH;
        command = (char *) gs_palloc0(allocated);

        for (i = 0; i < argc; i++)
        {
            int            arglen = strlen(argv[i]);

            if (arglen + len > allocated)
            {
                allocated *= 2;
                command = (char *)gs_repalloc(command, allocated);
            }

            rc = strncpy_s(command + len, allocated - len, argv[i], arglen);
            securec_check_c(rc, "", "");
            len += arglen;
            command[len++] = ' ';
        }

        command[len] = '\0';
    }
    
    return command;
}

static void parse_instance_name()
{
    errno_t rc = 0;

        /*
     * Option --instance is required for all commands except
     * init, show and validate
     */
    if (instance_name == NULL)
    {
        if (backup_subcmd != INIT_CMD && backup_subcmd != SHOW_CMD &&
            backup_subcmd != VALIDATE_CMD)
            elog(ERROR, "required parameter not specified: --instance");
    }
    else
        /* Set instance name */
        instance_config.name = pgut_strdup(instance_name);

    /*
     * If --instance option was passed, construct paths for backup data and
     * xlog files of this backup instance.
     */
    if ((backup_path != NULL) && instance_name)
    {
        /*
         * Fill global variables used to generate pathes inside the instance's
         * backup catalog.
         * TODO replace global variables with InstanceConfig structure fields
         */
        rc = sprintf_s(backup_instance_path, MAXPGPATH, "%s/%s/%s",
                backup_path, BACKUPS_DIR, instance_name);
        securec_check_ss_c(rc, "", "");
        rc = sprintf_s(arclog_path, MAXPGPATH, "%s/%s/%s", backup_path, "wal", instance_name);
        securec_check_ss_c(rc, "", "");

        /*
         * Fill InstanceConfig structure fields used to generate pathes inside
         * the instance's backup catalog.
         * TODO continue refactoring to use these fields instead of global vars
         */
        rc = sprintf_s(instance_config.backup_instance_path, MAXPGPATH, "%s/%s/%s",
                backup_path, BACKUPS_DIR, instance_name);
        securec_check_ss_c(rc, "", "");
        canonicalize_path(instance_config.backup_instance_path);

        rc = sprintf_s(instance_config.arclog_path, MAXPGPATH, "%s/%s/%s",
                backup_path, "wal", instance_name);
        securec_check_ss_c(rc, "", "");
        canonicalize_path(instance_config.arclog_path);

        /*
         * Ensure that requested backup instance exists.
         * for all commands except init, which doesn't take this parameter,
         * add-instance, which creates new instance
         */
        if (backup_subcmd != INIT_CMD && backup_subcmd != ADD_INSTANCE_CMD)
        {
            struct stat st;

            if (fio_stat(backup_instance_path, &st, true, FIO_BACKUP_HOST) != 0)
            {
                elog(WARNING, "Failed to access directory \"%s\": %s",
                    backup_instance_path, strerror(errno));

                // TODO: redundant message, should we get rid of it?
                elog(ERROR, "Instance '%s' does not exist in this backup catalog",
                            instance_name);
            }
            else
            {
                /* Ensure that backup_path is a path to a directory */
                if (!S_ISDIR(st.st_mode))
                    elog(ERROR, "-B, --backup-path must be a path to directory");
            }
        }
    }

    /*
     * We read options from command line, now we need to read them from
     * configuration file since we got backup path and instance name.
     * For some commands an instance option isn't required, see above.
     */
    if (instance_name)
    {
        char        path[MAXPGPATH];
        /* Read environment variables */
        config_get_opt_env(instance_options);

        /* Read options from configuration file */
        if (backup_subcmd != ADD_INSTANCE_CMD)
        {
            join_path_components(path, backup_instance_path,
                                 BACKUP_CATALOG_CONF_FILE);
            config_read_opt(path, instance_options, ERROR, true, false);
        }
        setMyLocation();
    }

}

static void parse_cmdline_args(int argc, char *argv[], const char *command_name)
{
    config_get_opt(argc, argv, cmd_options, instance_options);
    
    if (password) {
        if (!prompt_password) {
            elog(ERROR, "You cannot specify --password and --no-password options together");
        }
        replace_password(argc, argv, "-W");
        replace_password(argc, argv, "--password");
    }

    if (specify_tbsdir && !specify_extdir) {
        elog(ERROR, "If specify --tablespace-mapping option, you must specify --external-mapping option together");
    }

    pgut_init();

    if (help_opt)
        help_command(command_name);

    /* backup_path is required for all pg_probackup commands except help */
    if (backup_path == NULL)
    {
        /*
         * If command line argument is not set, try to read BACKUP_PATH
         * from environment variable
         */
        backup_path = getenv("BACKUP_PATH");
        if (backup_path == NULL)
            elog(ERROR, "required parameter not specified: BACKUP_PATH (-B, --backup-path)");
    }

    setMyLocation();

    if (backup_path != NULL)
    {
        canonicalize_path(backup_path);

        /* Ensure that backup_path is an absolute path */
        if (!is_absolute_path(backup_path))
            elog(ERROR, "-B, --backup-path must be an absolute path");
    }

    /* Ensure that backup_path is an absolute path */
    if (backup_path && !is_absolute_path(backup_path))
        elog(ERROR, "-B, --backup-path must be an absolute path");

   parse_instance_name();

}

static void do_delete_operate()
{
    if (delete_expired && backup_id_string)
                elog(ERROR, "You cannot specify --delete-expired and (-i, --backup-id) options together");
            if (merge_expired && backup_id_string)
                elog(ERROR, "You cannot specify --merge-expired and (-i, --backup-id) options together");
            if (delete_status && backup_id_string)
                elog(ERROR, "You cannot specify --status and (-i, --backup-id) options together");
            if (!delete_expired && !merge_expired && !delete_wal && delete_status == NULL && !backup_id_string)
                elog(ERROR, "You must specify at least one of the delete options: "
                                "--delete-expired |--delete-wal |--merge-expired |--status |(-i, --backup-id)");
            if (!backup_id_string)
            {
                if (delete_status)
                    do_delete_status(&instance_config, delete_status);
                else
                    do_retention();
            }
            else
                    do_delete(current.backup_id);
}

static int do_validate_operate()
{
    if (current.backup_id == 0 && target_time == 0 && target_xid == 0 && !target_lsn &&
        !recovery_target_options->target_name) {
        return do_validate_all();
    } else {
        /* PITR validation and, optionally, partial validation */
        return do_restore_or_validate(current.backup_id,
                  recovery_target_options,
                  restore_params,
                  no_sync);
    }
}

static int do_actual_operate()
{
    int res = 0;
    pgut_atexit_push(unlink_lock_atexit, NULL);
    switch (backup_subcmd)
    {
        case ADD_INSTANCE_CMD:
            return do_add_instance(&instance_config);
        case DELETE_INSTANCE_CMD:
            res = do_delete_instance();
            break;
        case INIT_CMD:
            return do_init();
        case BACKUP_CMD:
            {
                time_t    start_time = time(NULL);

                current.stream = stream_wal;

                /* sanity */
                if (current.backup_mode == BACKUP_MODE_INVALID)
                    elog(ERROR, "required parameter not specified: BACKUP_MODE "
                         "(-b, --backup-mode)");

                res = do_backup(start_time, set_backup_params, no_validate, no_sync, backup_logs, backup_replslots);
                break;
            }
        case RESTORE_CMD:
            res = do_restore_or_validate(current.backup_id,
                            recovery_target_options,
                            restore_params, no_sync);
            break;
        case VALIDATE_CMD:
            res = do_validate_operate();
            break;
        case SHOW_CMD:
            return do_show(instance_name, current.backup_id, show_archive);
        case DELETE_CMD:
            do_delete_operate();
            break;
        case MERGE_CMD:
            do_merge(current.backup_id);
            break;
        case SHOW_CONFIG_CMD:
            do_show_config();
            break;
        case SET_CONFIG_CMD:
            do_set_config(false);
            break;
        case SET_BACKUP_CMD:
            if (!backup_id_string)
                elog(ERROR, "You must specify parameter (-i, --backup-id) for 'set-backup' command");
            do_set_backup(instance_name, current.backup_id, set_backup_params);
            break;
        case NO_CMD:
            /* Should not happen */
            elog(ERROR, "Unknown subcommand");
    }

    on_cleanup();
    release_logfile();

    return res;
}

static void parse_backup_option_to_params(char *command, char *command_name)
{
    if (backup_subcmd == SET_BACKUP_CMD || backup_subcmd == BACKUP_CMD)
    {
        time_t expire_time = 0;

        if (expire_time_string && ttl >= 0)
            elog(ERROR, "You cannot specify '--expire-time' and '--ttl' options together");

        /* Parse string to seconds */
        if (expire_time_string)
        {
            if (!parse_time(expire_time_string, &expire_time, false))
                elog(ERROR, "Invalid value for '--expire-time' option: '%s'",
                     expire_time_string);
        }

        if (expire_time > 0 || ttl >= 0 || backup_note)
        {
            set_backup_params = pgut_new(pgSetBackupParams);
            set_backup_params->ttl = ttl;
            set_backup_params->expire_time = expire_time;
            set_backup_params->note = backup_note;

            if (backup_note && strlen(backup_note) > MAX_NOTE_SIZE)
                elog(ERROR, "Backup note cannot exceed %u bytes", MAX_NOTE_SIZE);
        }
    }

    /* sanity */
    if (backup_subcmd == VALIDATE_CMD && restore_params->no_validate)
        elog(ERROR, "You cannot specify \"--no-validate\" option with the \"%s\" command",
            command_name);

    if (num_threads < 1)
        num_threads = 1;

    if (batch_size < 1)
        batch_size = 1;
}

static void check_restore_option(char *command_name)
{
    if (backup_subcmd == VALIDATE_CMD || backup_subcmd == RESTORE_CMD)
    {
        /*
         * Parse all recovery target options into recovery_target_options
         * structure.
         */
        recovery_target_options =
            parseRecoveryTargetOptions(target_time, target_xid,
                target_inclusive, target_tli, target_lsn,
                (target_stop != NULL) ? target_stop :
                    (target_immediate) ? "immediate" : NULL,
                target_name, target_action);

        if (force && backup_subcmd != RESTORE_CMD)
            elog(ERROR, "You cannot specify \"--force\" flag with the \"%s\" command",
                command_name);

        if (force)
            no_validate = true;

        /* keep all params in one structure */
        restore_params = pgut_new(pgRestoreParams);
        restore_params->is_restore = (backup_subcmd == RESTORE_CMD);
        restore_params->force = force;
        restore_params->no_validate = no_validate;
        restore_params->skip_block_validation = skip_block_validation;
        restore_params->skip_external_dirs = skip_external_dirs;
        restore_params->incremental_mode = incremental_mode;
    }
}

static void check_unlimit_stack_size(void)
{
    bool DssFlags = IsDssMode();
    if (DssFlags == false || (DssFlags == true && backup_subcmd != RESTORE_CMD))
        return;

    struct rlimit lim;
    if (getrlimit(RLIMIT_STACK, &lim) != 0) {
        elog(ERROR, "getrlimit RLIMIT_STACK failed.");
    }
    if ((int)lim.rlim_cur < MIN_ULIMIT_STACK_SIZE) {
        elog(ERROR, "current ulimit stack size is %d,"
            " please run the ulimit -s size(size >= 8192) command to change it.", lim.rlim_cur);
    }
}

static void check_backid_option(char *command_name)
{
    if (backup_id_string != NULL)
    {
        if (backup_subcmd != RESTORE_CMD &&
            backup_subcmd != VALIDATE_CMD &&
            backup_subcmd != DELETE_CMD &&
            backup_subcmd != MERGE_CMD &&
            backup_subcmd != SET_BACKUP_CMD &&
            backup_subcmd != SHOW_CMD)
            elog(ERROR, "Cannot use -i (--backup-id) option together with the \"%s\" command",
                 command_name);

        current.backup_id = base36dec(backup_id_string);
        if (current.backup_id == 0)
            elog(ERROR, "Invalid backup-id \"%s\"", backup_id_string);
    }

    if (!instance_config.conn_opt.pghost &&
        instance_config.remote.host &&
        IsSshProtocol()) {
            instance_config.conn_opt.pghost = instance_config.remote.host;
    }

        /* Setup stream options. They are used in streamutil.c. */
    if (instance_config.conn_opt.pghost != NULL)
        dbhost = gs_pstrdup(instance_config.conn_opt.pghost);
    if (instance_config.conn_opt.pgport != NULL)
        dbport = gs_pstrdup(instance_config.conn_opt.pgport);
    if (instance_config.conn_opt.pguser != NULL)
        dbuser = gs_pstrdup(instance_config.conn_opt.pguser);
}

static void check_dss_input()
{
    if (!instance_config.dss.enable_dss && instance_config.dss.vgname != NULL) {
        instance_config.dss.enable_dss = true;
    }

    if (instance_config.dss.enable_dss && instance_config.dss.socketpath == NULL) {
        instance_config.dss.socketpath = getSocketpathFromEnv();
    }
}

int main(int argc, char *argv[])
{
    char       *command = NULL,
               *command_name;

    PROGRAM_NAME_FULL = argv[0];

    /* Initialize current backup */
    pgBackupInit(&current);

    /* Initialize current instance configuration */
    init_config(&instance_config, instance_name);

    PROGRAM_NAME = get_progname(argv[0]);
    PROGRAM_FULL_PATH = (char *)gs_palloc0(MAXPGPATH);

    /* Get current time */
    current_time = time(NULL);

    my_pid = getpid();

#if PG_VERSION_NUM >= 110000
    /*
     * Reset WAL segment size, we will retreive it using RetrieveWalSegSize()
     * later.
     */
    WalSegSz = 0;
#endif

    /*
     * Save main thread's tid. It is used call exit() in case of errors.
     */
    main_tid = pthread_self();

    /* Parse subcommands and non-subcommand options */
    if (argc > 1)
    {
        parse_options(argv[1], argc, argv);
    }

    if (backup_subcmd == NO_CMD)
        elog(ERROR, "No subcommand specified");

    /*
     * Make command string before getopt_long() will call. It permutes the
     * content of argv.
     */
    /* TODO why do we do that only for some commands? */
    command_name = gs_pstrdup(argv[1]);
    command = make_command_string(argc, argv);

    optind += 1;
    /* Parse command line only arguments */
    parse_cmdline_args(argc, argv, command_name);

    /* Initialize logger */
    init_logger(backup_path, &instance_config.logger);
    
    /* command was initialized for a few commands */
    if (command)
    {
        elog_file(INFO, "command: %s", command);

        pfree(command);
        command = NULL;
    }

    if (find_my_exec(argv[0],(char *) const_cast<char*>(PROGRAM_FULL_PATH)) < 0)
    {
        pfree((char *) const_cast<char*>(PROGRAM_FULL_PATH));
        PROGRAM_FULL_PATH = NULL;
        elog(WARNING, "%s: could not find a full path to executable", PROGRAM_NAME);
    }

    /*
     * We have read pgdata path from command line or from configuration file.
     * Ensure that pgdata is an absolute path.
     */
    if (instance_config.pgdata != NULL)
        canonicalize_path(instance_config.pgdata);
    if (instance_config.pgdata != NULL &&
        !is_absolute_path(instance_config.pgdata))
        elog(ERROR, "-D, --pgdata must be an absolute path");

    /* prepare pgdata of g_datadir struct */
    if (instance_config.pgdata != NULL)
    {
        errno_t rc = strcpy_s(g_datadir.pg_data, strlen(instance_config.pgdata) + 1, instance_config.pgdata);
        securec_check_c(rc, "\0", "\0");
    }

#if PG_VERSION_NUM >= 110000
    /* Check xlog-seg-size option */
    if (instance_name &&
        backup_subcmd != INIT_CMD &&
        backup_subcmd != ADD_INSTANCE_CMD && backup_subcmd != SET_CONFIG_CMD &&
        !IsValidWalSegSize(instance_config.xlog_seg_size))
    {
        /* If we are working with instance of PG<11 using PG11 binary,
         * then xlog_seg_size is equal to zero. Manually set it to 16MB.
         */
        if (instance_config.xlog_seg_size == 0)
            instance_config.xlog_seg_size = DEFAULT_XLOG_SEG_SIZE;
        else
            elog(ERROR, "Invalid WAL segment size %u", instance_config.xlog_seg_size);
    }
#endif

    /* Sanity check of --backup-id option */
    check_backid_option(command_name);
    
    check_restore_option(command_name);

    /*
     * Parse set-backup options into set_backup_params structure.
     */
    parse_backup_option_to_params(command, command_name);

    pfree(command_name);

    /* compress_init */
    compress_init();

    check_dss_input();

    dss_init();

    check_unlimit_stack_size();

    initDataPathStruct(IsDssMode());

    /* do actual operation */
    return do_actual_operate();
}

static void
opt_incr_restore_mode(ConfigOption *opt, const char *arg)
{
    if (pg_strcasecmp(arg, "none") == 0)
    {
        incremental_mode = INCR_NONE;
        return;
    }
    else if (pg_strcasecmp(arg, "checksum") == 0)
    {
        incremental_mode = INCR_CHECKSUM;
        return;
    }
    else if (pg_strcasecmp(arg, "lsn") == 0)
    {
        incremental_mode = INCR_LSN;
        return;
    }

    elog(ERROR, "Invalid value for '--incremental-mode' option: '%s'", arg);
}

static void
opt_backup_mode(ConfigOption *opt, const char *arg)
{
    current.backup_mode = parse_backup_mode(arg);
}

static void
opt_show_format(ConfigOption *opt, const char *arg)
{
    const char *v = arg;
    size_t        len;

    while (IsSpace(*v))
        v++;
    len = strlen(v);

    if (len > 0)
    {
        if (pg_strncasecmp("plain", v, len) == 0)
            show_format = SHOW_PLAIN;
        else if (pg_strncasecmp("json", v, len) == 0)
            show_format = SHOW_JSON;
        else
            elog(ERROR, "Invalid show format \"%s\"", arg);
    }
    else
        elog(ERROR, "Invalid show format \"%s\"", arg);
}

/*
 * Initialize compress and sanity checks for compress.
 */
static void
compress_init(void)
{
    /* Default algorithm is zlib */
    if (compress_shortcut)
        instance_config.compress_alg = ZLIB_COMPRESS;

    if (backup_subcmd != SET_CONFIG_CMD)
    {
        if (instance_config.compress_level != COMPRESS_LEVEL_DEFAULT
            && instance_config.compress_alg == NOT_DEFINED_COMPRESS)
            elog(ERROR, "Cannot specify compress-level option alone without "
                                                "compress-algorithm option");
    }

    if (instance_config.compress_level < 0 || instance_config.compress_level > 9)
        elog(ERROR, "--compress-level value must be in the range from 0 to 9");

    if (instance_config.compress_alg == ZLIB_COMPRESS && instance_config.compress_level == 0)
        elog(WARNING, "Compression level 0 will lead to data bloat!");

    if (instance_config.compress_alg == LZ4_COMPRESS && instance_config.compress_level > 1)
    {
        elog(WARNING, "Compression level will be set to 1 due to lz4 only supports level 0 and 1!");
    }

    if (backup_subcmd == BACKUP_CMD)
    {
#ifndef HAVE_LIBZ
        if (instance_config.compress_alg == ZLIB_COMPRESS)
            elog(ERROR, "This build does not support zlib compression");
#endif
    }
}

static int ss_get_primary_id(void)
{
    int fd = -1;
    int len = 0;
    int primary_id = -1;
    errno_t rc = 0;
    struct stat statbuf;
    char control_file_path[MAXPGPATH];

    rc = memset_s(control_file_path, MAXPGPATH, 0, MAXPGPATH);
    securec_check_c(rc, "\0", "\0");
    rc = snprintf_s(control_file_path, MAXPGPATH, MAXPGPATH - 1, "%s/pg_control", instance_config.dss.vgdata);
    securec_check_ss_c(rc, "\0", "\0");

    fd = open(control_file_path, O_RDONLY | PG_BINARY, 0);
    if (fd < 0) {
        pg_log(PG_WARNING, _("failed to open pg_contol\n"));
        close(fd);
        exit(1);
    }

    if (stat(control_file_path, &statbuf) < 0) {
        pg_log(PG_WARNING, _("failed to stat pg_contol\n"));
        close(fd);
        exit(1);
    }

    len = statbuf.st_size;
    char *tmpBuffer = (char*)malloc(len + 1);

    if ((read(fd, tmpBuffer, len)) != len) {
        close(fd);
        free(tmpBuffer);
        pg_log(PG_WARNING, _("failed to read pg_contol\n"));
        exit(1);
    }

    ss_reformer_ctrl_t* reformerCtrl;

    /* Calculate the offset to obtain the primary_id of the last page */
    reformerCtrl = (ss_reformer_ctrl_t*)(tmpBuffer + REFORMER_CTL_INSTANCEID * PG_CONTROL_SIZE);
    primary_id = reformerCtrl->primaryInstId;
    free(tmpBuffer);
    return primary_id;
}

static void dss_init(void)
{
    if (IsDssMode()) {
        /* skip in some special backup modes */
        if (backup_subcmd == DELETE_CMD || backup_subcmd == DELETE_INSTANCE_CMD || 
            backup_subcmd == SHOW_CMD || backup_subcmd == MERGE_CMD) {
            return;
        }

        /* register for dssapi */
        if (dss_device_init(instance_config.dss.socketpath, IsDssMode()) != DSS_SUCCESS) {
            elog(ERROR, "fail to init dss device");
            return;
        }

        if (IsSshProtocol()) {
            elog(ERROR, "Remote operations on dss mode are not supported");
        }

        if (instance_config.dss.vgname == NULL) {
            elog(ERROR, "Vgname must be specified in dss mode.");
        }

        parse_vgname_args(instance_config.dss.vgname);

        /* Check dss connect */
        struct stat st;
        if (stat(instance_config.dss.vgdata, &st) != 0 || !S_ISDIR(st.st_mode)) {
            elog(ERROR, "Could not connect dssserver, vgdata: \"%s\", socketpath: \"%s\", check and retry later.",
                 instance_config.dss.vgdata, instance_config.dss.socketpath);
        }

        if (strlen(instance_config.dss.vglog) && (stat(instance_config.dss.vglog, &st) != 0 || !S_ISDIR(st.st_mode))) {
            elog(ERROR, "Could not connect dssserver, vglog: \"%s\", socketpath: \"%s\", check and retry later.",
                 instance_config.dss.vglog, instance_config.dss.socketpath);
        }

        /* Check backup instance id in shared storage mode */
        int id = instance_config.dss.instance_id;
        if (id < MIN_INSTANCEID || id > MAX_INSTANCEID) {
            elog(ERROR, "Instance id must be specified in dss mode, valid range is %d - %d.",
                MIN_INSTANCEID, MAX_INSTANCEID);
        }

        if (backup_subcmd == BACKUP_CMD || backup_subcmd == ADD_INSTANCE_CMD) {
            int primary_instance_id = ss_get_primary_id();
            if (id != primary_instance_id) {
                elog(ERROR, "backup only support on primary in dss mode, primary instance id: %d, backup instance id: %d",
                    primary_instance_id, id);
            }
        }

        if (backup_subcmd != RESTORE_CMD) {
            off_t size = 0;
            char xlog_control_path[MAXPGPATH];

            join_path_components(xlog_control_path, instance_config.dss.vgdata, PG_XLOG_CONTROL_FILE);
            if ((size = dss_get_file_size(xlog_control_path)) == INVALID_DEVICE_SIZE) {
                elog(ERROR, "Could not get \"%s\" size: %s", xlog_control_path, strerror(errno));
            }

            if (size < (off_t)BLCKSZ * id) {
                elog(ERROR, "Cound not read beyond end of file \"%s\", file_size: %ld, instance_id: %d\n",
                    xlog_control_path, size, id);
            }
        }

        /* Prepare some g_datadir parameters */
        g_datadir.instance_id = id;

        errno_t rc = strcpy_s(g_datadir.dss_data, strlen(instance_config.dss.vgdata) + 1, instance_config.dss.vgdata);
        securec_check_c(rc, "\0", "\0");

        rc = strcpy_s(g_datadir.dss_log, strlen(instance_config.dss.vglog) + 1, instance_config.dss.vglog);
        securec_check_c(rc, "\0", "\0");

        XLogSegmentSize = DSS_XLOG_SEG_SIZE;
        instance_config.xlog_seg_size = DSS_XLOG_SEG_SIZE;
    }
}