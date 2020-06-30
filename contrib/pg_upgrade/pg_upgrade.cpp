/*
 *	pg_upgrade.c
 *
 *	main source file
 *
 *	Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *	contrib/pg_upgrade/pg_upgrade.c
 */

/*
 *	To simplify the upgrade process, we force certain system values to be
 *	identical between old and new clusters:
 *
 *	We control all assignments of pg_class.oid (and relfilenode) so toast
 *	oids are the same between old and new clusters.  This is important
 *	because toast oids are stored as toast pointers in user tables.
 *
 *	FYI, while pg_class.oid and pg_class.relfilenode are initially the same
 *	in a cluster, but they can diverge due to CLUSTER, REINDEX, or VACUUM
 *	FULL.  The new cluster will have matching pg_class.oid and
 *	pg_class.relfilenode values and be based on the old oid value.	This can
 *	cause the old and new pg_class.relfilenode values to differ.  In summary,
 *	old and new pg_class.oid and new pg_class.relfilenode will have the
 *	same value, and old pg_class.relfilenode might differ.
 *
 *	We control all assignments of pg_type.oid because these oids are stored
 *	in user composite type values.
 *
 *	We control all assignments of pg_enum.oid because these oids are stored
 *	in user tables as enum values.
 *
 *	We control all assignments of pg_authid.oid because these oids are stored
 *	in pg_largeobject_metadata.
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "pg_upgrade.h"
#include "utils/pg_crc.h"
#include "utils/pg_crc_tables.h"
#include "replication/slot.h"
#include <sys/types.h>
#include <string.h>

#ifdef HAVE_LANGINFO_H
#include <langinfo.h>
#endif

#define SLRU_PAGES_PER_SEGMENT_OLD 32
#define CLOG_PER_SEGMENT_OLD BLCKSZ* SLRU_PAGES_PER_SEGMENT_OLD

#define ReplicationSlotOnDiskDynamicSizeOldLogical \
    sizeof(ReplicationSlotOnDiskOldLogical) - ReplicationSlotOnDiskConstantSize
#define ReplicationSlotOnDiskDynamicSizeOld sizeof(ReplicationSlotOnDiskOld) - ReplicationSlotOnDiskConstantSize

/* transform old version LSN into new version. */
#define XLogRecPtrSwap(x) (((uint64)((x).xlogid)) << 32 | (x).xrecoff)

#define InvalidTransactionId ((TransactionId)0)

typedef struct ReplicationSlotPersistentDataOld {
    /* The slot's identifier */
    NameData name;

    /* database the slot is active on */
    Oid database;

    bool isDummyStandby;

    /*
     * xmin horizon for data
     *
     * NB: This may represent a value that hasn't been written to disk yet;
     * see notes for effective_xmin, below.
     */
    ShortTransactionId xmin;

    /* oldest LSN that might be required by this replication slot */
    XLogRecPtrOld restart_lsn;

} ReplicationSlotPersistentDataOld;
/*
 * Replication slot on-disk data structure.
 */
typedef struct ReplicationSlotOnDiskOld {
    /* first part of this struct needs to be version independent */

    /* data not covered by checksum */
    uint32 magic;
    pg_crc32 checksum;

    /* data covered by checksum */
    uint32 version;
    uint32 length;

    ReplicationSlotPersistentDataOld slotdata;
} ReplicationSlotOnDiskOld;

typedef struct ReplicationSlotPersistentDataOldLogical {
    /* The slot's identifier */
    NameData name;
    /* database the slot is active on */
    Oid database;
    /*
     * The slot's behaviour when being dropped (or restored after a crash).
     */
    ReplicationSlotPersistency persistency;
    bool isDummyStandby;
    /*
     * xmin horizon for data
     *
     * NB: This may represent a value that hasn't been written to disk yet;
     * see notes for effective_xmin, below.
     */
    ShortTransactionId xmin;
    /*
     * xmin horizon for catalog tuples
     *
     * NB: This may represent a value that hasn't been written to disk yet;
     * see notes for effective_xmin, below.
     */
    ShortTransactionId catalog_xmin;
    /* oldest LSN that might be required by this replication slot */
    XLogRecPtrOld restart_lsn;
    /* oldest LSN that the client has acked receipt for */
    XLogRecPtrOld confirmed_flush;
    /* plugin name */
    NameData plugin;
} ReplicationSlotPersistentDataOldLogical;

typedef struct ReplicationSlotOnDiskOldLogical {
    /* first part of this struct needs to be version independent */
    /* data not covered by checksum */
    uint32 magic;
    pg_crc32c checksum;
    /* data covered by checksum */
    uint32 version;
    uint32 length;
    ReplicationSlotPersistentDataOldLogical slotdata;
} ReplicationSlotOnDiskOldLogical;

static void prepare_new_cluster(void);
static void prepare_new_databases(void);
static void create_new_objects(void);
static void copy_clog_xlog_xid(void);
static void set_frozenxids(void);
static void setup(char* argv0, bool live_check);
void create_tablspace_directories(void);
char* form_standby_transfer_command(char* newdatadir);
int execute_command_in_remote_node(char* nodename, char* command, bool bparallel);
void read_popen_output_parallel();

void process_cluster_upgrade_options(void);

extern char* argv0;
ClusterInfo old_cluster, new_cluster;
OSInfo os_info;
char instance_name[NAMEDATALEN] = {0};
char output_files[PG_LOG_TYPE_BUT][MAXPGPATH] = {{0}};

char* cluster_type = NULL;

int main(int argc, char** argv)
{
    parseCommandLine(argc, argv);

    process_cluster_upgrade_options();

    return 0;
}

int upgrade_local_instance(void)
{
    char path[MAXPGPATH] = {0};
    int nRet = 0;

    instance_name[0] = '\0';
    (void)get_local_instancename_by_dbpath(old_cluster.pgdata, instance_name);

    if ((user_opts.command == COMMAND_CHECK) || (user_opts.command == COMMAND_INIT_UPGRADE) ||
        (user_opts.command == COMMAND_FULLUPGRADE)) {
        bool live_check = false;
        struct passwd* pwd = NULL;

        output_check_banner(&live_check, instance_name);
        setup(argv0, live_check);
        check_cluster_versions();
        get_sock_dir(&old_cluster, live_check);

        if (false == user_opts.inplace_upgrade)
            get_sock_dir(&new_cluster, false);

        check_cluster_compatibility(live_check);

        nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", old_cluster.pgdata, "upgrade_info");
        securec_check_ss_c(nRet, "\0", "\0");
        mkdir(path, 0700);

        if (os_info.is_root_user) {
            pwd = getpwnam(old_cluster.user);
            if (NULL != pwd) {
                chown(path, pwd->pw_uid, pwd->pw_gid);
            }
        }

        check_old_cluster(live_check);
        if (os_info.is_root_user) {
            nRet = snprintf_s(
                path, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s", old_cluster.pgdata, "upgrade_info", ALL_DUMP_FILE);
            securec_check_ss_c(nRet, "\0", "\0");
            if (NULL != pwd) {
                chown(path, pwd->pw_uid, pwd->pw_gid);
            }

            nRet = snprintf_s(
                path, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s", old_cluster.pgdata, "upgrade_info", GLOBALS_DUMP_FILE);
            securec_check_ss_c(nRet, "\0", "\0");
            if (NULL != pwd) {
                chown(path, pwd->pw_uid, pwd->pw_gid);
            }

            nRet = snprintf_s(
                path, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s", old_cluster.pgdata, "upgrade_info", DB_DUMP_FILE);
            securec_check_ss_c(nRet, "\0", "\0");
            if (NULL != pwd) {
                chown(path, pwd->pw_uid, pwd->pw_gid);
            }

            nRet = snprintf_s(
                path, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s", old_cluster.pgdata, "upgrade_info", USERLOG_DUMP_FILE);
            securec_check_ss_c(nRet, "\0", "\0");
            if (NULL != pwd) {
                chown(path, pwd->pw_uid, pwd->pw_gid);
            }

            nRet = snprintf_s(
                path, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s", old_cluster.pgdata, "upgrade_info", STATISTIC_DUMP_FILE);
            securec_check_ss_c(nRet, "\0", "\0");
            if (NULL != pwd) {
                chown(path, pwd->pw_uid, pwd->pw_gid);
            }
        }

        if (true == user_opts.inplace_upgrade) {
            if (user_opts.command == COMMAND_INIT_UPGRADE) {
                saveClusterInfo(&old_cluster, &os_info, old_cluster.pgdata, old_cluster.user, "oldclusterstatus");
            }
            exit(0);
        }
    } else {
        pg_log(PG_REPORT,
            "Performing Upgrade operation on %s\n"
            "Old database path: %s\n"
            "New database path: %s\n",
            instance_name,
            old_cluster.pgdata,
            new_cluster.pgdata);
        pg_log(PG_REPORT, "--------------------------------------------------\n");

        check_cluster_versions();
        get_sock_dir(&new_cluster, false);
        get_control_data(&new_cluster, false, false);
    }

    if (user_opts.command == COMMAND_UPGRADE) {
        char* datadirectory = old_cluster.pgdata;
        readClusterInfo(&old_cluster, &os_info, old_cluster.pgdata, "oldclusterstatus");
        if (old_cluster.is_standby) {
            pg_log(PG_REPORT, "Current instance is standby, so upgrade will be performed along with master instance\n");
            exit(0);
        }

        old_cluster.pgdata = datadirectory;
        if (true == user_opts.inplace_upgrade) {
            new_cluster.port = old_cluster.port;
        }

        get_control_data_xlog_info(&old_cluster);
    }

    /*
     * Destructive Changes to New Cluster
     */

    copy_clog_xlog_xid();

    /* -- NEW -- */
    start_postmaster(&new_cluster);

    check_new_cluster();
    report_clusters_compatible();

    create_tablspace_directories();

    pg_log(PG_REPORT, "\nPerforming Upgrade\n");
    pg_log(PG_REPORT, "------------------\n");

    prepare_new_cluster();

    stop_postmaster(false);

    /* New now using xids of the old system */

    /* -- NEW -- */
    start_postmaster(&new_cluster);

    prepare_new_databases();

    create_new_objects();

    stop_postmaster(false);

    /*
     * Most failures happen in create_new_objects(), which has completed at
     * this point.	We do this here because it is just before linking, which
     * will link the old and new cluster data files, preventing the old
     * cluster from being safely started once the new cluster is started.
     */
    if (user_opts.transfer_mode == TRANSFER_MODE_LINK)
        disable_old_cluster();

    /*
     * Assuming OIDs are only used in system tables, there is no need to
     * restore the OID counter because we have not transferred any OIDs from
     * the old system, but we do it anyway just in case.  We do it late here
     * because there is no need to have the schema load use new oids.
     */
    prep_status("Setting next OID for new cluster");

    if (os_info.is_root_user) {
        exec_prog(UTILITY_LOG_FILE,
            NULL,
            true,
            "sudo -u %s sh -c \""
            "export LD_LIBRARY_PATH=%s/../lib:\\$LD_LIBRARY_PATH;"
            "\\\"%s/pg_resetxlog\\\" -o %u \\\"%s\\\""
            "\"",
            new_cluster.user,
            new_cluster.bindir,
            new_cluster.bindir,
            old_cluster.controldata.chkpnt_nxtoid,
            new_cluster.pgdata);
    } else {
        exec_prog(UTILITY_LOG_FILE,
            NULL,
            true,
            "\"%s/pg_resetxlog\" -o %u \"%s\"",
            new_cluster.bindir,
            old_cluster.controldata.chkpnt_nxtoid,
            new_cluster.pgdata);
    }
    check_ok();

    if (user_opts.pgHAdata != NULL) {
        struct passwd* pwd;

        pwd = getpwnam(new_cluster.user);

        /* save old cluster & new cluster information */
        nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", new_cluster.pgdata, "upgrade_info");
        securec_check_ss_c(nRet, "\0", "\0");
        mkdir(path, 0700);
        if (NULL != pwd) {
            chown(path, pwd->pw_uid, pwd->pw_gid);
        }
        saveClusterInfo(&old_cluster, &os_info, new_cluster.pgdata, new_cluster.user, "oldclusterstatus");
        saveClusterInfo(&new_cluster, &os_info, new_cluster.pgdata, new_cluster.user, "newclusterstatus");
    }

    if (os_info.is_root_user) {
        exec_prog(UTILITY_LOG_FILE,
            NULL,
            true,
            "chown -R --reference=\"%s/global/pg_control\" \"%s\"",
            new_cluster.pgdata,
            new_cluster.pgdata);
        update_permissions_for_tablspace_dir();
    }

    if (user_opts.pgHAdata != NULL) {
        create_standby_instance_setup();
    }

    transfer_all_new_dbs(&old_cluster.dbarr, &new_cluster.dbarr, old_cluster.pgdata, new_cluster.pgdata);

    if (os_info.is_root_user) {
        exec_prog(UTILITY_LOG_FILE,
            NULL,
            true,
            "chown -R --reference=\"%s/global/pg_control\" \"%s\"",
            new_cluster.pgdata,
            new_cluster.pgdata);
        update_permissions_for_tablspace_dir();
    }

    if (user_opts.pgHAdata != NULL) {
        char* cmd = form_standby_transfer_command(user_opts.pgHAdata);
        execute_command_in_remote_node(user_opts.pgHAipaddr, cmd, false);
        read_popen_output_parallel();
        pg_free(cmd);
    }

    pg_log(PG_REPORT, "\nUpgrade Complete\n");
    pg_log(PG_REPORT, "----------------\n");

    return 0;
}

static void setup(char* argv0, bool live_check)
{
    char exec_path[MAXPGPATH]; /* full path to my executable */

    /*
     * make sure the user has a clean environment, otherwise, we may confuse
     * libpq when we connect to one (or both) of the servers.
     */
    check_pghost_envvar();

    verify_directories();

    /* no postmasters should be running */
    if (!live_check && is_server_running(old_cluster.pgdata))
        pg_log(PG_FATAL,
            "There seems to be a postmaster servicing the old cluster.\n"
            "Please shutdown that postmaster and try again.\n");

    if (false == user_opts.inplace_upgrade) {
        /* same goes for the new postmaster */
        if (is_server_running(new_cluster.pgdata))
            pg_log(PG_FATAL,
                "There seems to be a postmaster servicing the new cluster.\n"
                "Please shutdown that postmaster and try again.\n");
    }

    /* get path to pg_upgrade executable */
    if (find_my_exec(argv0, exec_path) < 0)
        pg_log(PG_FATAL, "Could not get path name to gs_upgrade: %s\n", getErrorText(errno));

    /* Trim off program name and keep just path */
    *last_dir_separator(exec_path) = '\0';
    canonicalize_path(exec_path);
    os_info.exec_path = pg_strdup(exec_path);
}

static void prepare_new_cluster(void)
{
    /*
     * It would make more sense to freeze after loading the schema, but that
     * would cause us to lose the frozenids restored by the load. We use
     * --analyze so autovacuum doesn't update statistics later
     */
    prep_status("Analyzing all rows in the new cluster");
    exec_prog(UTILITY_LOG_FILE,
        NULL,
        true,
        "\"%s/vacuumdb\" %s --all --analyze %s",
        new_cluster.bindir,
        cluster_conn_opts(&new_cluster),
        log_opts.verbose ? "--verbose" : "");
    check_ok();

    /*
     * We do freeze after analyze so pg_statistic is also frozen. template0 is
     * not frozen here, but data rows were frozen by initdb, and we set its
     * datfrozenxid and relfrozenxids later to match the new xid counter
     * later.
     */
    prep_status("Freezing all rows on the new cluster");
    exec_prog(UTILITY_LOG_FILE,
        NULL,
        true,
        "\"%s/vacuumdb\" %s --all --freeze %s",
        new_cluster.bindir,
        cluster_conn_opts(&new_cluster),
        log_opts.verbose ? "--verbose" : "");
    check_ok();

    get_pg_database_relfilenode(&new_cluster);
}

static void prepare_new_databases(void)
{
    /*
     * We set autovacuum_freeze_max_age to its maximum value so autovacuum
     * does not launch here and delete clog files, before the frozen xids are
     * set.
     */

    set_frozenxids();

    prep_status("Creating databases in the new cluster");

    /*
     * Install support functions in the global-object restore database to
     * preserve pg_authid.oid.	pg_dumpall uses 'template0' as its template
     * database so objects we add into 'template1' are not propogated.	They
     * are removed on pg_upgrade exit.
     */
    install_support_functions_in_new_db("template1");

    /*
     * We have to create the databases first so we can install support
     * functions in all the other databases.  Ideally we could create the
     * support functions in template1 but pg_dumpall creates database using
     * the template0 template.
     */
    exec_prog(RESTORE_LOG_FILE,
        NULL,
        true,
        "\"%s/gsql\" " EXEC_PSQL_ARGS " %s -f \"%s/upgrade_info/%s\"",
        new_cluster.bindir,
        cluster_conn_opts(&new_cluster),
        old_cluster.pgdata,
        GLOBALS_DUMP_FILE);
    check_ok();

    /* we load this to get a current list of databases */
    get_db_and_rel_infos(&new_cluster);
}

static void restore_db_schema()
{
    int i, rc;
    char filename[MAXPGPATH] = {0};

    /* filename : instance_path/upgrade_info/gs_upgrade_dump_all.sql */
    rc = snprintf_s(
        filename, sizeof(filename), sizeof(filename) - 1, "%s/upgrade_info/%s", old_cluster.pgdata, ALL_DUMP_FILE);
    securec_check_ss_c(rc, "\0", "\0");

    /* filename : instance_path/upgrade_info/gs_upgrade_dump */
    filename[strlen(filename) - 4] = '\0';

    for (i = 0; i < old_cluster.dbarr.ndbs; i++) {

        char newdbfilename[MAXPGPATH] = {0};
        char* tmpDbName = NULL;

        tmpDbName = change_database_name(old_cluster.dbarr.dbs[i].db_name);
        rc = snprintf_s(
            newdbfilename, sizeof(newdbfilename), sizeof(newdbfilename) - 1, "%s_new_%s.sql", filename, tmpDbName);
        securec_check_ss_c(rc, "\0", "\0");
        free(tmpDbName);
        exec_prog(RESTORE_LOG_FILE,
            NULL,
            true,
            "\"%s/gsql\" " EXEC_PSQL_ARGS " %s -f \"%s\"",
            new_cluster.bindir,
            cluster_conn_opts(&new_cluster),
            newdbfilename);
    }
}

static void create_new_objects(void)
{
    int dbnum;

    prep_status("Adding support functions to new cluster");

    for (dbnum = 0; dbnum < new_cluster.dbarr.ndbs; dbnum++) {
        DbInfo* new_db = &new_cluster.dbarr.dbs[dbnum];

        /* skip db we already installed */
        if (strcmp(new_db->db_name, "template1") != 0)
            install_support_functions_in_new_db(new_db->db_name);
    }
    check_ok();

    prep_status("Restoring database schema to new cluster");
    restore_db_schema();
    check_ok();

    prep_status("Restoring user information to new cluster");

    if (0 != strncmp(new_cluster.usersha256, old_cluster.usersha256, strlen(new_cluster.usersha256))) {
        exec_prog(RESTORE_LOG_FILE,
            NULL,
            true,
            "\"%s/gsql\" " EXEC_PSQL_ARGS
            " %s -c \"UPDATE pg_catalog.pg_authid SET rolpassword = '%s' WHERE oid = %u;\"",
            new_cluster.bindir,
            cluster_conn_opts(&new_cluster),
            old_cluster.usersha256,
            new_cluster.install_role_oid);
    }

    exec_prog(RESTORE_LOG_FILE,
        NULL,
        true,
        "\"%s/gsql\" " EXEC_PSQL_ARGS " %s -c \"DELETE FROM pg_catalog.pg_auth_history; "
        "DELETE FROM pg_catalog.pg_user_status;\"",
        new_cluster.bindir,
        cluster_conn_opts(&new_cluster));

    exec_prog(RESTORE_LOG_FILE,
        NULL,
        true,
        "\"%s/gsql\" " EXEC_PSQL_ARGS " %s -f \"%s/upgrade_info/%s\"",
        new_cluster.bindir,
        cluster_conn_opts(&new_cluster),
        old_cluster.pgdata,
        USERLOG_DUMP_FILE);
    check_ok();

    prep_status("Restoring statistics information to new cluster");

    exec_prog(RESTORE_LOG_FILE,
        NULL,
        true,
        "\"%s/gsql\" " EXEC_PSQL_ARGS " %s -f \"%s/upgrade_info/%s\"",
        new_cluster.bindir,
        cluster_conn_opts(&new_cluster),
        old_cluster.pgdata,
        STATISTIC_DUMP_FILE);
    check_ok();

    uninstall_support_functions_from_new_cluster();

    /* regenerate now that we have objects in the databases */
    get_db_and_rel_infos(&new_cluster);
}

/*
 * Delete the given subdirectory contents from the new cluster, and copy the
 * files from the old cluster into it.
 */
static void copy_subdir_files(char* subdir)
{
    char old_path[MAXPGPATH];
    char new_path[MAXPGPATH];
    int nRet = 0;

    prep_status("Deleting files from new %s", subdir);

    nRet = snprintf_s(old_path, sizeof(old_path), sizeof(old_path) - 1, "%s/%s", old_cluster.pgdata, subdir);
    securec_check_ss_c(nRet, "\0", "\0");

    nRet = snprintf_s(new_path, sizeof(new_path), sizeof(new_path) - 1, "%s/%s", new_cluster.pgdata, subdir);
    securec_check_ss_c(nRet, "\0", "\0");

    if (!rmtree(new_path, true))
        pg_log(PG_FATAL, "could not delete directory \"%s\"\n", new_path);
    check_ok();

    prep_status("Copying old %s to new server", subdir);

    exec_prog(UTILITY_LOG_FILE,
        NULL,
        true,
#ifndef WIN32
        "cp -Rf \"%s\" \"%s\"",
#else
        /* flags: everything, no confirm, quiet, overwrite read-only */
        "xcopy /e /y /q /r \"%s\" \"%s\\\"",
#endif
        old_path,
        new_path);

#ifndef WIN32
    if (os_info.is_root_user) {
        exec_prog(UTILITY_LOG_FILE, NULL, true, "chown -R %s \"%s\"", new_cluster.user, new_path);
    }
#endif

    check_ok();
}

static void copy_clog_xlog_xid(void)
{
    /* copy old commit logs to new data dir */
    copy_subdir_files("pg_clog");

    /* set the next transaction id of the new cluster */
    prep_status("Setting next transaction ID for new cluster");

    if (os_info.is_root_user) {
        exec_prog(UTILITY_LOG_FILE,
            NULL,
            true,
            "sudo -u %s sh -c \""
            "export LD_LIBRARY_PATH=%s/../lib:\\$LD_LIBRARY_PATH;"
            "\\\"%s/pg_resetxlog\\\" -f -x %lu \\\"%s\\\""
            "\"",
            new_cluster.user,
            new_cluster.bindir,
            new_cluster.bindir,
            old_cluster.controldata.chkpnt_nxtxid,
            new_cluster.pgdata);
    } else {
        exec_prog(UTILITY_LOG_FILE,
            NULL,
            true,
            "\"%s/pg_resetxlog\" -f -x %lu \"%s\"",
            new_cluster.bindir,
            old_cluster.controldata.chkpnt_nxtxid,
            new_cluster.pgdata);
    }
    check_ok();

    /* now reset the wal archives in the new cluster */
    prep_status("Resetting WAL archives");

    if (os_info.is_root_user) {
        exec_prog(UTILITY_LOG_FILE,
            NULL,
            true,
            "sudo -u %s sh -c \""
            "export LD_LIBRARY_PATH=%s/../lib:\\$LD_LIBRARY_PATH;"
            "\\\"%s/pg_resetxlog\\\" -l %s \\\"%s\\\""
            "\"",
            new_cluster.user,
            new_cluster.bindir,
            new_cluster.bindir,
            old_cluster.controldata.nextxlogfile,
            new_cluster.pgdata);
    } else {
        exec_prog(UTILITY_LOG_FILE,
            NULL,
            true,
            "\"%s/pg_resetxlog\" -l %s \"%s\"",
            new_cluster.bindir,
            old_cluster.controldata.nextxlogfile,
            new_cluster.pgdata);
    }
    check_ok();
}

/*
 *	set_frozenxids()
 *
 *	We have frozen all xids, so set relfrozenxid and datfrozenxid
 *	to be the old cluster's xid counter, which we just set in the new
 *	cluster.  User-table frozenxid values will be set by pg_dumpall
 *	--binary-upgrade, but objects not set by the pg_dump must have
 *	proper frozen counters.
 */
static void set_frozenxids(void)
{
    int dbnum;
    PGconn *conn, *conn_template1;
    PGresult* dbres = NULL;
    int ntups;
    int i_datname;
    int i_datallowconn;

    prep_status("Setting frozenxid, frozenxid64 counters in new cluster");

    conn_template1 = connectToServer(&new_cluster, "template1");

    /* set pg_database.datfrozenxid64 */
    PQclear(executeQueryOrDie(conn_template1,
        "UPDATE pg_catalog.pg_database "
        "SET	datfrozenxid64 = '%lu'",
        old_cluster.controldata.chkpnt_nxtxid));

    /* get database names */
    dbres = executeQueryOrDie(conn_template1,
        "SELECT	datname, datallowconn "
        "FROM	pg_catalog.pg_database");

    i_datname = PQfnumber(dbres, "datname");
    i_datallowconn = PQfnumber(dbres, "datallowconn");

    ntups = PQntuples(dbres);
    for (dbnum = 0; dbnum < ntups; dbnum++) {
        char* datname = PQgetvalue(dbres, dbnum, i_datname);
        char* datallowconn = PQgetvalue(dbres, dbnum, i_datallowconn);

        /*
         * We must update databases where datallowconn = false, e.g.
         * template0, because autovacuum increments their datfrozenxids and
         * relfrozenxids even if autovacuum is turned off, and even though all
         * the data rows are already frozen  To enable this, we temporarily
         * change datallowconn.
         */
        if (strcmp(datallowconn, "f") == 0)
            PQclear(executeQueryOrDie(conn_template1,
                "UPDATE pg_catalog.pg_database "
                "SET	datallowconn = true "
                "WHERE datname = '%s'",
                datname));

        conn = connectToServer(&new_cluster, datname);

        /* set pg_class.relfrozenxid64 */
        PQclear(executeQueryOrDie(conn,
            "UPDATE	pg_catalog.pg_class "
            "SET	relfrozenxid64 = '%lu' "
            /* only heap and TOAST are vacuumed */
            "WHERE	relkind IN ('r', 't')",
            old_cluster.controldata.chkpnt_nxtxid));
        PQfinish(conn);

        /* Reset datallowconn flag */
        if (strcmp(datallowconn, "f") == 0)
            PQclear(executeQueryOrDie(conn_template1,
                "UPDATE pg_catalog.pg_database "
                "SET	datallowconn = false "
                "WHERE datname = '%s'",
                datname));
    }

    PQclear(dbres);

    PQfinish(conn_template1);

    check_ok();
}

/**
 * @Description:  need to check that the rewritten clog is consistent with the previous clog
 * @in: clog path
 */
void check_clog(char* path)
{
    DIR* cldir = NULL;
    struct dirent* clde;
    int64 segno;
    int fd;
    int fd_new;
    errno_t rc;

    if ((cldir = opendir(path)) == NULL) {
        fprintf(stderr, _("could not open directory \"%s\"\n"), path);
        exit(2);
    }
    while ((clde = readdir(cldir)) != NULL) {
        if (strlen(clde->d_name) == 4 && strspn(clde->d_name, "0123456789ABCDEF") == 4) {
            char old[MAXPGPATH];
            char new_path[MAXPGPATH];
            int offset_newfile = 0;

            rc = snprintf_s(old, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s", new_cluster.pgdata, "pg_clog", clde->d_name);

            if ((fd = open(old, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR)) == -1) {
                fprintf(stderr, _("could not open file \"%s\" for checking: %s\n"), old, strerror(errno));
                exit(2);
            }

            segno = (int)strtol(clde->d_name, NULL, 16);

            offset_newfile = segno % 64 * BLCKSZ * SLRU_PAGES_PER_SEGMENT_OLD;
            rc = snprintf_s(new_path,
                MAXPGPATH,
                MAXPGPATH - 1,
                "%s/%s/%04X%08X",
                new_cluster.pgdata,
                "pg_clog",
                (uint32)((segno) >> 32),
                (uint32)((segno / 64) & (int64)0xFFFFFFFF));
            securec_check_ss_c(rc, "\0", "\0");

            if ((fd_new = open(new_path, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR)) == -1) {
                fprintf(stderr, _("could not open file \"%s\" for checking: %s\n"), new_path, strerror(errno));
                exit(2);
            }
            if (lseek(fd_new, (off_t)offset_newfile, SEEK_SET) < 0) {
                fprintf(stderr, "lseek error !\n");
                close(fd_new);
                exit(2);
            }

            int read_length_old = 0;
            int read_length_new = 0;
            char read_per_old[CLOG_PER_SEGMENT_OLD];
            char read_per_new[CLOG_PER_SEGMENT_OLD];
            read_length_old = read(fd, read_per_old, CLOG_PER_SEGMENT_OLD);
            read_length_new = read(fd_new, read_per_new, CLOG_PER_SEGMENT_OLD);
            if (read_length_old == -1 || read_length_new == -1) {
                fprintf(stderr, _("could not read file \"%s\" or file \"%s\" : %s\n"), old, new_path, strerror(errno));
                close(fd);
                exit(2);
            }
            if (memcmp(read_per_old, read_per_new, read_length_old) != 0) {
                fprintf(stderr,
                    _("clog check error! old file \"%s\" new file \"%s\" : %s\n"),
                    old,
                    new_path,
                    strerror(errno));
                close(fd);
                close(fd_new);
                exit(2);
            } else {
                if (unlink(old) != 0) {
                    fprintf(stderr, _("could not remove old log file \"%s\" "), old);
                    exit(2);
                }
            }

            if (close(fd_new)) {
                fprintf(stderr, _("could not close file \"%s\" "), new_path);
                exit(2);
            }
            if (close(fd)) {
                fprintf(stderr, _("could not close file \"%s\" "), old);
                exit(2);
            }
        }
    }
    closedir(cldir);
    return;
}

/**
 * @Description:  64-bit xid feature, File name length expanded to 12, SLRU_PAGES_PER_SEGMENT
 * changed to 2048, need to combine the first 64 into one.
 */
void upgrade_clog_dir(void)
{
    DIR* cldir = NULL;
    struct dirent* clde;
    int64 segno = 0;
    char path[MAXPGPATH];
    int fd = 0;
    int fd_new = 0;
    errno_t rc = EOK;

    rc = memset_s(path, MAXPGPATH, 0, MAXPGPATH);
    securec_check_ss_c(rc, "\0", "\0");

    rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", new_cluster.pgdata, "pg_clog");
    securec_check_ss_c(rc, "\0", "\0");

    if ((cldir = opendir(path)) == NULL) {
        fprintf(stderr, _("could not open directory \"%s\"\n"), path);
        exit(2);
    }

    while ((clde = readdir(cldir)) != NULL) {
        if (strlen(clde->d_name) == 4 && strspn(clde->d_name, "0123456789ABCDEF") == 4) {
            char old[MAXPGPATH];
            char new_path[MAXPGPATH];
            int offset_newfile = 0;

            rc = snprintf_s(old, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s", new_cluster.pgdata, "pg_clog", clde->d_name);
            securec_check_ss_c(rc, "\0", "\0");

            if ((fd = open(old, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR)) == -1) {
                fprintf(stderr, _("could not open file \"%s\" for reading: %s\n"), old, strerror(errno));
                exit(2);
            }

            segno = (int)strtol(clde->d_name, NULL, 16);

            /*
             * The new clog file is 64 times the size of the original file, so
             * need to calculate the offset in the new file based on segno
             */
            offset_newfile = segno % 64 * BLCKSZ * SLRU_PAGES_PER_SEGMENT_OLD;
            rc = snprintf_s(new_path,
                MAXPGPATH,
                MAXPGPATH - 1,
                "%s/%s/%04X%08X",
                new_cluster.pgdata,
                "pg_clog",
                (uint32)(0),
                (uint32)((segno / 64) & (int64)0xFFFFFFFF));
            securec_check_ss_c(rc, "\0", "\0");

            if ((fd_new = open(new_path, O_CREAT | O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR)) == -1) {
                fprintf(stderr, _("could not open file \"%s\" for writing: %s\n"), new_path, strerror(errno));
                exit(2);
            }
            if (lseek(fd_new, (off_t)offset_newfile, SEEK_SET) < 0) {
                fprintf(stderr, "lseek error !\n");
                close(fd_new);
                exit(2);
            }

            int read_length = 0;
            char read_per_length[CLOG_PER_SEGMENT_OLD];
            read_length = read(fd, read_per_length, CLOG_PER_SEGMENT_OLD);
            if (read_length == -1 || read_length % BLCKSZ != 0) {
                fprintf(stderr, _("could not read file \"%s\": %s\n"), old, strerror(errno));
                close(fd);
                exit(2);
            }
            if (write(fd_new, read_per_length, read_length) != read_length) {
                fprintf(stderr, _("could not write file \"%s\" "), new_path);
                close(fd_new);
                exit(2);
            }
            if (fsync(fd_new) != 0) {
                fprintf(stderr, _("could not fsync file \"%s\" "), new_path);
                close(fd_new);
                exit(2);
            }
            if (close(fd)) {
                fprintf(stderr, _("could not close file \"%s\" "), new_path);
                exit(2);
            }
            if (close(fd_new)) {
                fprintf(stderr, _("could not close file \"%s\" "), new_path);
                exit(2);
            }
        }
    }
    /* check clog*/
    check_clog(path);
    closedir(cldir);
    return;
}

/**
 * @Description: 64-bit xid feature and Logical replication feature,
 *  ReplicationSlotOnDisk structure changes and needs to be updated.
 */
void upgrade_replslot()
{
    char path[MAXPGPATH];
    char childpath[MAXPGPATH];
    ReplicationSlotOnDisk cp;
    ReplicationSlotOnDiskOld cp_old;
    DIR* cldir = NULL;
    struct dirent* clde;
    int fd;
    int readlength;
    errno_t rc;

    rc = memset_s(path, MAXPGPATH, 0, MAXPGPATH);
    securec_check_ss_c(rc, "\0", "\0");

    rc = memset_s(childpath, MAXPGPATH, 0, MAXPGPATH);
    securec_check_ss_c(rc, "\0", "\0");

    rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/pg_replslot/", new_cluster.pgdata);
    securec_check_ss_c(rc, "\0", "\0");

    if ((cldir = opendir(path)) == NULL) {
        fprintf(stderr, _("could not open directory \"%s\"\n"), path);
        exit(2);
    }
    while ((clde = readdir(cldir)) != NULL) {

        if (strcmp(clde->d_name, ".") == 0 || strcmp(clde->d_name, "..") == 0)
            continue;

        rc = snprintf_s(childpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s/state", path, clde->d_name);
        securec_check_ss_c(rc, "\0", "\0");

        if ((fd = open(childpath, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR)) == -1) {
            fprintf(stderr, _("could not open file \"%s\" for rewriting: %s\n"), childpath, strerror(errno));
            exit(2);
        }

        readlength = read(fd, &cp_old, sizeof(cp_old));
        if (readlength != sizeof(cp_old)) {
            fprintf(stderr, _("could not read file \"%s\": %s\n"), childpath, strerror(errno));
            close(fd);
            exit(2);
        }
        if (cp_old.length != ReplicationSlotOnDiskDynamicSize) {
            if (cp_old.length == ReplicationSlotOnDiskDynamicSizeOld) {
                cp.magic = SLOT_MAGIC;
                cp.version = SLOT_VERSION;
                cp.length = ReplicationSlotOnDiskDynamicSize;
                cp.slotdata.name = cp_old.slotdata.name;
                cp.slotdata.database = cp_old.slotdata.database;
                cp.slotdata.persistency = RS_PERSISTENT;
                cp.slotdata.isDummyStandby = cp_old.slotdata.isDummyStandby;
                cp.slotdata.xmin = cp_old.slotdata.xmin;
                cp.slotdata.catalog_xmin = InvalidTransactionId;
                cp.slotdata.restart_lsn = XLogRecPtrSwap(cp_old.slotdata.restart_lsn);
                cp.slotdata.confirmed_flush = InvalidXLogRecPtr;
                strncpy(NameStr(cp.slotdata.plugin), "", NAMEDATALEN);
            } else if (cp_old.length == ReplicationSlotOnDiskDynamicSizeOldLogical) {
                ReplicationSlotOnDiskOldLogical cp_old_logic;
                if (lseek(fd, (off_t)0, SEEK_SET) < 0) {
                    fprintf(stderr, "lseek error !\n");
                    close(fd);
                    exit(2);
                }
                readlength = read(fd, &cp_old_logic, sizeof(cp_old_logic));
                if (readlength != sizeof(cp_old_logic)) {
                    fprintf(stderr, _("could not read file \"%s\": %s\n"), childpath, strerror(errno));
                    close(fd);
                    exit(2);
                }
                cp.magic = SLOT_MAGIC;
                cp.version = SLOT_VERSION;
                cp.length = ReplicationSlotOnDiskDynamicSize;
                cp.slotdata.name = cp_old_logic.slotdata.name;
                cp.slotdata.database = cp_old_logic.slotdata.database;
                cp.slotdata.persistency = cp_old_logic.slotdata.persistency;
                cp.slotdata.isDummyStandby = cp_old_logic.slotdata.isDummyStandby;
                cp.slotdata.xmin = cp_old_logic.slotdata.xmin;
                cp.slotdata.catalog_xmin = InvalidTransactionId;
                cp.slotdata.restart_lsn = XLogRecPtrSwap(cp_old_logic.slotdata.restart_lsn);
                cp.slotdata.confirmed_flush = XLogRecPtrSwap(cp_old_logic.slotdata.confirmed_flush);
                cp.slotdata.plugin = cp_old_logic.slotdata.plugin;
            }

            INIT_CRC32C(cp.checksum);
            COMP_CRC32C(cp.checksum, (char*)&cp + ReplicationSlotOnDiskConstantSize, ReplicationSlotOnDiskDynamicSize);
            FIN_CRC32C(cp.checksum);

            if (lseek(fd, (off_t)0, SEEK_SET) < 0) {
                fprintf(stderr, "lseek error !\n");
                close(fd);
                exit(2);
            }
            if ((write(fd, &cp, sizeof(cp))) != sizeof(cp)) {
                fprintf(stderr, _("could not write file \"%s\" "), childpath);
                close(fd);
                exit(2);
            }
            if (fsync(fd) != 0) {
                fprintf(stderr, _("could not fsync file \"%s\" "), childpath);
                close(fd);
                exit(2);
            }
            if (close(fd)) {
                fprintf(stderr, _("could not close file \"%s\" "), childpath);
                exit(2);
            }
        }
    }
    closedir(cldir);
    return;
}

/**
 * @Description:  64-bit xid feature,The TransactionId extends from 32-bit to 64-bit,
 * where the data previously stored in the file was 32-bit transactionId or offset, needs
 * to be updated to 64 bits.
 * @in: The folder name of the file that needs to be updated.
 */
void upgrade_log(char* foldname)
{
    DIR* dir = NULL;
    struct dirent* de;
    char path[MAXPGPATH];
    int fd;
    int fd_new;
    int64 segno = 0;
    errno_t rc;

    if (foldname == NULL)
        return;
    memset_s(path, MAXPGPATH, 0, MAXPGPATH);

    rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", new_cluster.pgdata, foldname);
    securec_check_ss_c(rc, "\0", "\0");

    if ((dir = opendir(path)) == NULL) {
        fprintf(stderr, _("could not open directory \"%s\"\n"), path);
        exit(2);
    }

    while ((de = readdir(dir)) != NULL) {
        uint32 data_old;
        uint64 data_new;

        if (strlen(de->d_name) == 4 && strspn(de->d_name, "0123456789ABCDEF") == 4) {
            char old[MAXPGPATH];
            char new_path[MAXPGPATH];
            int data_newfile = 0;

            rc = snprintf_s(old, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s", new_cluster.pgdata, foldname, de->d_name);
            securec_check_ss_c(rc, "\0", "\0");

            if ((fd = open(old, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR)) == -1) {
                fprintf(stderr, _("could not open file \"%s\" for writing: %s\n"), old, strerror(errno));
                exit(2);
            }

            segno = (int)strtol(de->d_name, NULL, 16);
            data_newfile = segno % 32 * BLCKSZ * SLRU_PAGES_PER_SEGMENT_OLD;

            rc = snprintf_s(new_path,
                MAXPGPATH,
                MAXPGPATH - 1,
                "%s/%s/%04X%08X",
                new_cluster.pgdata,
                foldname,
                (uint32)(0),
                (uint32)((segno / 32) & (int64)0xFFFFFFFF));
            securec_check_ss_c(rc, "\0", "\0");

            if ((fd_new = open(new_path, O_CREAT | O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR)) == -1) {
                fprintf(stderr, _("could not open file \"%s\" for writing: %s\n"), new_path, strerror(errno));
                exit(2);
            }
            if (lseek(fd_new, (off_t)data_newfile, SEEK_SET) < 0) {
                fprintf(stderr, "lseek error !\n");
                close(fd_new);
                exit(2);
            }

            while (true) {
                int read_length = 0;
                read_length = read(fd, &data_old, sizeof(uint32));
                if (read_length == -1) {
                    fprintf(stderr, _("could not read file \"%s\": %s\n"), old, strerror(errno));
                    close(fd);
                    exit(2);
                }
                /* Read to the end of the file, break out of the loop and read the next file */
                else if (read_length != sizeof(uint32)) {
                    if (close(fd)) {
                        fprintf(stderr, _("could not close file \"%s\" "), old);
                        exit(2);
                    }
                    if (unlink(old) != 0) {
                        fprintf(stderr, _("could not remove old log file \"%s\" "), old);
                        exit(2);
                    }
                    break;
                }

                data_new = data_old;
                if (write(fd_new, &data_new, sizeof(uint64)) != sizeof(uint64)) {
                    fprintf(stderr, _("could not write file \"%s\" "), new_path);
                    close(fd_new);
                    exit(2);
                }
            }
            if (fsync(fd_new) != 0) {
                fprintf(stderr, _("could not fsync file \"%s\" "), new_path);
                close(fd_new);
                exit(2);
            }
            if (close(fd_new)) {
                fprintf(stderr, _("could not close file \"%s\" "), new_path);
                exit(2);
            }
        }
    }

    closedir(dir);
    return;
}

/**
 * @Description:  64-bit xid feature, file name length expanded to 12, SLRU_PAGES_PER_SEGMENT
 * changed to 2048, need unlink old pg_notify file.
 */
void unlink_pg_notify(void)
{
    DIR* notifydir = NULL;
    struct dirent* notifyde;
    char path[MAXPGPATH];
    errno_t rc;

    rc = memset_s(path, MAXPGPATH, 0, MAXPGPATH);
    securec_check_ss_c(rc, "\0", "\0");

    rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", new_cluster.pgdata, "pg_notify");
    securec_check_ss_c(rc, "\0", "\0");

    if ((notifydir = opendir(path)) == NULL) {
        fprintf(stderr, _("could not open directory \"%s\"\n"), path);
        exit(2);
    }
    while ((notifyde = readdir(notifydir)) != NULL) {
        if (strlen(notifyde->d_name) == 4 && strspn(notifyde->d_name, "0123456789ABCDEF") == 4) {
            char old[MAXPGPATH];

            rc = snprintf_s(
                old, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s", new_cluster.pgdata, "pg_notify", notifyde->d_name);
            securec_check_ss_c(rc, "\0", "\0");
            if (unlink(old) != 0) {
                fprintf(stderr, _("could not remove old notify file \"%s\" "), old);
                exit(2);
            }
        }
    }

    closedir(notifydir);
    return;
}
