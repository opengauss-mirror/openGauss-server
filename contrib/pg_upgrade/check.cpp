/*
 *	check.c
 *
 *	server checks and output routines
 *
 *	Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *	contrib/pg_upgrade/check.c
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "pg_upgrade.h"

static void set_locale_and_encoding(ClusterInfo* cluster);
static void check_locale_and_encoding(ControlData* oldctrl, ControlData* newctrl);
static void check_is_super_user(ClusterInfo* cluster);
static void check_for_isn_and_int8_passing_mismatch(ClusterInfo* cluster);
static void check_for_reg_data_type_usage(ClusterInfo* cluster);
static char* get_canonical_locale_name(int category, const char* locale);
bool check_is_stanby_instance(ClusterInfo* cluster);

void output_check_banner(bool* live_check, char* instance_name)
{
    if (user_opts.command == COMMAND_CHECK && is_server_running(old_cluster.pgdata)) {
        *live_check = true;
        pg_log(PG_REPORT,
            "Performing Consistency Checks for %s\n"
            "Old database path: %s\n"
            "New database path: %s\n",
            instance_name,
            old_cluster.pgdata,
            new_cluster.pgdata);
        pg_log(PG_REPORT, "--------------------------------------------------\n");
    } else if (user_opts.command == COMMAND_INIT_UPGRADE) {
        *live_check = true;
        if (!is_server_running(old_cluster.pgdata)) {
            pg_log(PG_FATAL,
                "There seems to be a gaussdb is not servicing the old instance database.\n"
                "Please start that gaussdb and try again.\n");
        }
        pg_log(PG_REPORT,
            "Performing Consistency Checks & and Collecting Info %s\n"
            "Old database path: %s\n"
            "New database path: %s\n",
            instance_name,
            old_cluster.pgdata,
            new_cluster.pgdata);
        pg_log(PG_REPORT, "------------------------------------------------------------\n");
    } else {
        pg_log(PG_REPORT,
            "Performing Consistency Checks for %s\n"
            "Old database path: %s\n"
            "New database path: %s\n",
            instance_name,
            old_cluster.pgdata,
            new_cluster.pgdata);
        pg_log(PG_REPORT, "--------------------------------------------------\n");
    }
}

void check_old_cluster(bool live_check)
{
    /* -- OLD -- */

    if (!live_check)
        start_postmaster(&old_cluster);

    old_cluster.is_standby = false;
    if (check_is_stanby_instance(&old_cluster)) {
        old_cluster.is_standby = true;
        if (user_opts.command == COMMAND_INIT_UPGRADE) {
            saveClusterInfo(&old_cluster, &os_info, old_cluster.pgdata, old_cluster.user, "oldclusterstatus");
        }
        exit(0);
    }

    set_locale_and_encoding(&old_cluster);

    get_pg_database_relfilenode(&old_cluster);

    /* Extract a list of databases and tables from the old cluster */
    get_db_and_rel_infos(&old_cluster);

    init_tablespaces();

    get_loadable_libraries();

    /*
     * Check for various failure cases
     */
    check_is_super_user(&old_cluster);
    check_for_reg_data_type_usage(&old_cluster);
    check_for_isn_and_int8_passing_mismatch(&old_cluster);

    /* Pre-PG 9.0 had no large object permissions */
    if (GET_MAJOR_VERSION(old_cluster.major_version) <= 804)
        new_9_0_populate_pg_largeobject_metadata(&old_cluster, true);

    /*
     * While not a check option, we do this now because this is the only time
     * the old server is running.
     */
    if (user_opts.check != COMMAND_CHECK) {
        generate_old_dump();
        split_old_dump();
        generate_statistics_dump(&old_cluster);
    }

    if (!live_check)
        stop_postmaster(false);
}

void check_new_cluster(void)
{
    set_locale_and_encoding(&new_cluster);

    check_locale_and_encoding(&old_cluster.controldata, &new_cluster.controldata);

    get_db_and_rel_infos(&new_cluster);

    if (user_opts.transfer_mode == TRANSFER_MODE_LINK)
        check_hard_link();

    check_is_super_user(&new_cluster);

    /*
     *	We don't restore our own user, so both clusters must match have
     *	matching install-user oids.
     */
    if (old_cluster.install_role_oid != new_cluster.install_role_oid)
        pg_log(PG_FATAL, "Old and new cluster install users have different values for pg_authid.oid.\n");

    /*
     *	We only allow the install user in the new cluster because other
     *	defined users might match users defined in the old cluster and
     *	generate an error during pg_dump restore.
     */
    if (new_cluster.role_count != 1)
        pg_log(PG_FATAL, "Only the install user can be defined in the new cluster.\n");
}

void report_clusters_compatible(void)
{
    if ((user_opts.command == COMMAND_CHECK) || (user_opts.command == COMMAND_INIT_UPGRADE)) {
        pg_log(PG_REPORT, "\n*Clusters are compatible*\n");
        /* stops new cluster */
        stop_postmaster(false);

        if (user_opts.command == COMMAND_INIT_UPGRADE) {
            saveClusterInfo(&old_cluster, &os_info, old_cluster.pgdata, old_cluster.user, "oldclusterstatus");
        }
        exit(0);
    }

    pg_log(PG_REPORT,
        "\n"
        "If gs_upgrade fails after this point, you must re-initdb the\n"
        "new cluster before continuing.\n");
}

void check_cluster_versions(void)
{
    prep_status("Checking cluster versions");

    /* get old and new cluster versions */
    old_cluster.major_version = get_major_server_version(&old_cluster);
    new_cluster.major_version = get_major_server_version(&new_cluster);

    /*
     * We allow upgrades from/to the same major version for alpha/beta
     * upgrades
     */

    if (GET_MAJOR_VERSION(old_cluster.major_version) < 803)
        pg_log(PG_FATAL, "This utility can only upgrade from PostgreSQL version 8.3 and later.\n");

    /* Only current PG version is supported as a target */
    if (GET_MAJOR_VERSION(new_cluster.major_version) != GET_MAJOR_VERSION(PG_VERSION_NUM))
        pg_log(PG_FATAL, "This utility can only upgrade to PostgreSQL version %s.\n", PG_MAJORVERSION);

    /*
     * We can't allow downgrading because we use the target pg_dumpall, and
     * pg_dumpall cannot operate on new database versions, only older
     * versions.
     */
    if (old_cluster.major_version > new_cluster.major_version)
        pg_log(PG_FATAL, "This utility cannot be used to downgrade to older major PostgreSQL versions.\n");

    check_ok();
}

void check_cluster_compatibility(bool live_check)
{
    /* get/check pg_control data of servers */
    get_control_data(&old_cluster, live_check, true);

    if (true == user_opts.inplace_upgrade) {
        return;
    }

    get_control_data(&new_cluster, false, false);
    check_control_data(&old_cluster.controldata, &new_cluster.controldata);

    /* Is it 9.0 but without tablespace directories? */
    if (GET_MAJOR_VERSION(new_cluster.major_version) == 900 &&
        new_cluster.controldata.cat_ver < TABLE_SPACE_SUBDIRS_CAT_VER)
        pg_log(PG_FATAL,
            "This utility can only upgrade to PostgreSQL version 9.0 after 2010-01-11\n"
            "because of backend API changes made during development.\n");

    /* We read the real port number for PG >= 9.1 */
    if (live_check && GET_MAJOR_VERSION(old_cluster.major_version) < 901 && old_cluster.port == DEF_PGUPORT)
        pg_log(PG_FATAL,
            "When checking a pre-PG 9.1 live old server, "
            "you must specify the old server's port number.\n");

    if (live_check && old_cluster.port == new_cluster.port)
        pg_log(PG_FATAL,
            "When checking a live server, "
            "the old and new port numbers must be different.\n");
}

/*
 * set_locale_and_encoding()
 *
 * query the database to get the template0 locale
 */
static void set_locale_and_encoding(ClusterInfo* cluster)
{
    ControlData* ctrl = &cluster->controldata;
    PGconn* conn = NULL;
    PGresult* res = NULL;
    int i_encoding;
    int cluster_version = cluster->major_version;

    conn = connectToServer(cluster, "template1");

    /* for pg < 80400, we got the values from pg_controldata */
    if (cluster_version >= 80400) {
        int i_datcollate;
        int i_datctype;

        res = executeQueryOrDie(conn,
            "SELECT datcollate, datctype "
            "FROM 	pg_catalog.pg_database "
            "WHERE	datname = 'template0' ");
        assert(PQntuples(res) == 1);

        i_datcollate = PQfnumber(res, "datcollate");
        i_datctype = PQfnumber(res, "datctype");

        if (GET_MAJOR_VERSION(cluster->major_version) < 902) {
            /*
             *	Pre-9.2 did not canonicalize the supplied locale names
             *	to match what the system returns, while 9.2+ does, so
             *	convert pre-9.2 to match.
             */
            ctrl->lc_collate = get_canonical_locale_name(LC_COLLATE, pg_strdup(PQgetvalue(res, 0, i_datcollate)));
            ctrl->lc_ctype = get_canonical_locale_name(LC_CTYPE, pg_strdup(PQgetvalue(res, 0, i_datctype)));
        } else {
            ctrl->lc_collate = pg_strdup(PQgetvalue(res, 0, i_datcollate));
            ctrl->lc_ctype = pg_strdup(PQgetvalue(res, 0, i_datctype));
        }

        PQclear(res);
    }

    res = executeQueryOrDie(conn,
        "SELECT pg_catalog.pg_encoding_to_char(encoding) "
        "FROM 	pg_catalog.pg_database "
        "WHERE	datname = 'template0' ");
    assert(PQntuples(res) == 1);

    i_encoding = PQfnumber(res, "pg_encoding_to_char");
    ctrl->encoding = pg_strdup(PQgetvalue(res, 0, i_encoding));

    PQclear(res);

    PQfinish(conn);
}

/*
 * check_locale_and_encoding()
 *
 *	locale is not in pg_controldata in 8.4 and later so
 *	we probably had to get via a database query.
 */
static void check_locale_and_encoding(ControlData* oldctrl, ControlData* newctrl)
{
    /*
     *	These are often defined with inconsistent case, so use pg_strcasecmp().
     *	They also often use inconsistent hyphenation, which we cannot fix, e.g.
     *	UTF-8 vs. UTF8, so at least we display the mismatching values.
     */
    if (pg_strcasecmp(oldctrl->lc_collate, newctrl->lc_collate) != 0)
        pg_log(PG_FATAL,
            "lc_collate cluster values do not match:  old \"%s\", new \"%s\"\n",
            oldctrl->lc_collate,
            newctrl->lc_collate);
    if (pg_strcasecmp(oldctrl->lc_ctype, newctrl->lc_ctype) != 0)
        pg_log(PG_FATAL,
            "lc_ctype cluster values do not match:  old \"%s\", new \"%s\"\n",
            oldctrl->lc_ctype,
            newctrl->lc_ctype);
    if (pg_strcasecmp(oldctrl->encoding, newctrl->encoding) != 0)
        pg_log(PG_FATAL,
            "encoding cluster values do not match:  old \"%s\", new \"%s\"\n",
            oldctrl->encoding,
            newctrl->encoding);
}

/*
 *	check_is_super_user()
 *
 *	Check we are superuser, and out user id and user count
 */
static void check_is_super_user(ClusterInfo* cluster)
{
    PGresult* res = NULL;
    PGconn* conn = connectToServer(cluster, "template1");
    int nRet = 0;

    /* Can't use pg_authid because only superusers can view it. */
    res = executeQueryOrDie(conn,
        "SELECT rolsuper, oid "
        "FROM pg_catalog.pg_roles "
        "WHERE rolname = current_user");

    if (PQntuples(res) != 1 || strcmp(PQgetvalue(res, 0, 0), "t") != 0)
        pg_log(PG_FATAL, "database user \"%s\" is not a system admin\n", cluster->user);

    cluster->install_role_oid = atooid(PQgetvalue(res, 0, 1));

    PQclear(res);

    res = executeQueryOrDie(conn,
        "SELECT rolpassword "
        "FROM pg_catalog.pg_authid "
        "WHERE rolname = current_user;");

    if (PQntuples(res) != 1)
        pg_log(PG_FATAL, "user \"%s\" information not found in system tables\n", cluster->user);

    nRet =
        snprintf_s(cluster->usersha256, SHA256_PASSWD_LEN * 2, SHA256_PASSWD_LEN * 2 - 1, "%s", PQgetvalue(res, 0, 0));
    securec_check_ss_c(nRet, "\0", "\0");

    PQclear(res);

    res = executeQueryOrDie(conn,
        "SELECT COUNT(*) "
        "FROM pg_catalog.pg_roles ");

    if (PQntuples(res) != 1)
        pg_log(PG_FATAL, "could not determine the number of users\n");

    cluster->role_count = atoi(PQgetvalue(res, 0, 0));

    PQclear(res);

    PQfinish(conn);
}

/*
 *	check_for_isn_and_int8_passing_mismatch()
 *
 *	contrib/isn relies on data type int8, and in 8.4 int8 can now be passed
 *	by value.  The schema dumps the CREATE TYPE PASSEDBYVALUE setting so
 *	it must match for the old and new servers.
 */
static void check_for_isn_and_int8_passing_mismatch(ClusterInfo* cluster)
{
    int dbnum;
    FILE* script = NULL;
    bool found = false;
    char output_path[MAXPGPATH];
    int nRet = 0;

    prep_status("Checking for contrib/isn with bigint-passing mismatch");

    if (old_cluster.controldata.float8_pass_by_value == new_cluster.controldata.float8_pass_by_value) {
        /* no mismatch */
        check_ok();
        return;
    }

    nRet =
        snprintf_s(output_path, sizeof(output_path), sizeof(output_path) - 1, "contrib_isn_and_int8_pass_by_value.txt");
    securec_check_ss_c(nRet, "\0", "\0");

    for (dbnum = 0; dbnum < cluster->dbarr.ndbs; dbnum++) {
        PGresult* res = NULL;
        bool db_used = false;
        int ntups;
        int rowno;
        int i_nspname, i_proname;
        DbInfo* active_db = &cluster->dbarr.dbs[dbnum];
        PGconn* conn = connectToServer(cluster, active_db->db_name);

        /* Find any functions coming from contrib/isn */
        res = executeQueryOrDie(conn,
            "SELECT n.nspname, p.proname "
            "FROM	pg_catalog.pg_proc p, "
            "		pg_catalog.pg_namespace n "
            "WHERE	p.pronamespace = n.oid AND "
            "		p.probin = '$libdir/isn'");

        ntups = PQntuples(res);
        i_nspname = PQfnumber(res, "nspname");
        i_proname = PQfnumber(res, "proname");
        for (rowno = 0; rowno < ntups; rowno++) {
            found = true;
            if (script == NULL && (script = fopen_priv(output_path, "w")) == NULL)
                pg_log(PG_FATAL, "Could not open file \"%s\": %s\n", output_path, getErrorText(errno));
            if (!db_used) {
                fprintf(script, "Database: %s\n", active_db->db_name);
                db_used = true;
            }
            fprintf(script, "  %s.%s\n", PQgetvalue(res, rowno, i_nspname), PQgetvalue(res, rowno, i_proname));
        }

        PQclear(res);

        PQfinish(conn);
    }

    if (script)
        fclose(script);

    if (found) {
        pg_log(PG_REPORT, "fatal\n");
        pg_log(PG_FATAL,
            "Your installation contains \"contrib/isn\" functions which rely on the\n"
            "bigint data type.  Your old and new clusters pass bigint values\n"
            "differently so this cluster cannot currently be upgraded.  You can\n"
            "manually upgrade databases that use \"contrib/isn\" facilities and remove\n"
            "\"contrib/isn\" from the old cluster and restart the upgrade.  A list of\n"
            "the problem functions is in the file:\n"
            "    %s\n\n",
            output_path);
    } else
        check_ok();
}

/*
 * check_for_reg_data_type_usage()
 *	pg_upgrade only preserves these system values:
 *		pg_class.oid
 *		pg_type.oid
 *		pg_enum.oid
 *
 *	Many of the reg* data types reference system catalog info that is
 *	not preserved, and hence these data types cannot be used in user
 *	tables upgraded by pg_upgrade.
 */
static void check_for_reg_data_type_usage(ClusterInfo* cluster)
{
    int dbnum;
    FILE* script = NULL;
    bool found = false;
    char output_path[MAXPGPATH];
    int nRet = 0;

    prep_status("Checking for reg* system OID user data types");

    nRet = snprintf_s(output_path, sizeof(output_path), sizeof(output_path) - 1, "tables_using_reg.txt");
    securec_check_ss_c(nRet, "\0", "\0");

    for (dbnum = 0; dbnum < cluster->dbarr.ndbs; dbnum++) {
        PGresult* res = NULL;
        bool db_used = false;
        int ntups;
        int rowno;
        int i_nspname, i_relname, i_attname;
        DbInfo* active_db = &cluster->dbarr.dbs[dbnum];
        PGconn* conn = connectToServer(cluster, active_db->db_name);

        /*
         * While several relkinds don't store any data, e.g. views, they can
         * be used to define data types of other columns, so we check all
         * relkinds.
         */
        res = executeQueryOrDie(conn,
            "SELECT n.nspname, c.relname, a.attname "
            "FROM	pg_catalog.pg_class c, "
            "		pg_catalog.pg_namespace n, "
            "		pg_catalog.pg_attribute a "
            "WHERE	c.oid = a.attrelid AND "
            "		NOT a.attisdropped AND "
            "		a.atttypid IN ( "
            "			'pg_catalog.regproc'::pg_catalog.regtype, "
            "			'pg_catalog.regprocedure'::pg_catalog.regtype, "
            "			'pg_catalog.regoper'::pg_catalog.regtype, "
            "			'pg_catalog.regoperator'::pg_catalog.regtype, "
            /* regclass.oid is preserved, so 'regclass' is OK */
            /* regtype.oid is preserved, so 'regtype' is OK */
            "			'pg_catalog.regconfig'::pg_catalog.regtype, "
            "			'pg_catalog.regdictionary'::pg_catalog.regtype) AND "
            "		c.relnamespace = n.oid AND "
            "		n.nspname != 'pg_catalog' AND "
            "		n.nspname != 'information_schema'");

        ntups = PQntuples(res);
        i_nspname = PQfnumber(res, "nspname");
        i_relname = PQfnumber(res, "relname");
        i_attname = PQfnumber(res, "attname");
        for (rowno = 0; rowno < ntups; rowno++) {
            found = true;
            if (script == NULL && (script = fopen_priv(output_path, "w")) == NULL)
                pg_log(PG_FATAL, "Could not open file \"%s\": %s\n", output_path, getErrorText(errno));
            if (!db_used) {
                fprintf(script, "Database: %s\n", active_db->db_name);
                db_used = true;
            }
            fprintf(script,
                "  %s.%s.%s\n",
                PQgetvalue(res, rowno, i_nspname),
                PQgetvalue(res, rowno, i_relname),
                PQgetvalue(res, rowno, i_attname));
        }

        PQclear(res);

        PQfinish(conn);
    }

    if (script)
        fclose(script);

    if (found) {
        pg_log(PG_REPORT, "fatal\n");
        pg_log(PG_FATAL,
            "Your installation contains one of the reg* data types in user tables.\n"
            "These data types reference system OIDs that are not preserved by\n"
            "gs_upgrade, so this cluster cannot currently be upgraded.  You can\n"
            "remove the problem tables and restart the upgrade.  A list of the problem\n"
            "columns is in the file:\n"
            "    %s\n\n",
            output_path);
    } else
        check_ok();
}

/*
 * get_canonical_locale_name
 *
 * Send the locale name to the system, and hope we get back a canonical
 * version.  This should match the backend's check_locale() function.
 */
static char* get_canonical_locale_name(int category, const char* locale)
{
    char* save = NULL;
    char* res = NULL;

    save = setlocale(category, NULL);
    if (!save)
        pg_log(PG_FATAL, "failed to get the current locale\n");

    /* 'save' may be pointing at a modifiable scratch variable, so copy it. */
    save = pg_strdup(save);

    /* set the locale with setlocale, to see if it accepts it. */
    res = setlocale(category, locale);

    if (!res)
        pg_log(PG_FATAL, "failed to get system locale name for \"%s\"\n", locale);

    res = pg_strdup(res);

    /* restore old value. */
    if (!setlocale(category, save))
        pg_log(PG_FATAL, "failed to restore old locale \"%s\"\n", save);

    free(save);

    return res;
}

static void write_string(int32 upd_info_fd, char* string)
{
    int32 len;

    if (NULL == string) {
        len = -1;
        write(upd_info_fd, &len, sizeof(int32));
        return;
    }

    len = strlen(string);
    write(upd_info_fd, &len, sizeof(int32));
    if (len > 0) {
        write(upd_info_fd, string, len);
    }
}

void saveClusterInfo(ClusterInfo* cluster, OSInfo* osinfo, char* datadir, char* owner, char* cluster_filename)
{
    ClusterInfo temp_clusterinfo;
    int32 upd_info_fd;
    int32 i;
    char path[MAXPGPATH];
    struct passwd* pwd;
    int nRet = 0;

    pwd = getpwnam(owner);
    nRet = snprintf_s(path, sizeof(path), sizeof(path) - 1, "%s/upgrade_info/%s", datadir, cluster_filename);
    securec_check_ss_c(nRet, "\0", "\0");
    if ((upd_info_fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR)) < 0) {
        return;
    }

    MemCpy(&temp_clusterinfo, cluster, sizeof(ClusterInfo));

    temp_clusterinfo.controldata.lc_collate = NULL;
    temp_clusterinfo.controldata.lc_ctype = NULL;
    temp_clusterinfo.controldata.encoding = NULL;
    temp_clusterinfo.dbarr.dbs = NULL;
    temp_clusterinfo.pgdata = NULL;
    temp_clusterinfo.pgconfig = NULL;
    temp_clusterinfo.bindir = NULL;
    temp_clusterinfo.pgopts = NULL;
    temp_clusterinfo.sockdir = NULL;
    temp_clusterinfo.tablespace_suffix = NULL;
    temp_clusterinfo.user = NULL;

    /*ControlData controldata*/
    write(upd_info_fd, &temp_clusterinfo, sizeof(ClusterInfo));

    /* n*DbInfo */
    write(upd_info_fd, cluster->dbarr.dbs, (sizeof(DbInfo) * cluster->dbarr.ndbs));

    for (i = 0; i < cluster->dbarr.ndbs; i++) {
        /*nrels * RelInfo */
        write(upd_info_fd, cluster->dbarr.dbs[i].rel_arr.rels, (cluster->dbarr.dbs[i].rel_arr.nrels * sizeof(RelInfo)));
    }

    write_string(upd_info_fd, cluster->controldata.lc_collate);
    write_string(upd_info_fd, cluster->controldata.lc_ctype);
    write_string(upd_info_fd, cluster->controldata.encoding);

    write_string(upd_info_fd, cluster->pgdata);
    write_string(upd_info_fd, cluster->pgconfig);
    write_string(upd_info_fd, cluster->bindir);
    write_string(upd_info_fd, cluster->pgopts);
    write_string(upd_info_fd, cluster->sockdir);
    write_string(upd_info_fd, cluster->tablespace_suffix);
    write_string(upd_info_fd, cluster->user);

    /* tablespace information */
    write(upd_info_fd, &osinfo->num_tablespaces, sizeof(int32));
    for (i = 0; i < osinfo->num_tablespaces; i++) {
        write_string(upd_info_fd, osinfo->tablespaces[i]);
        write_string(upd_info_fd, osinfo->slavetablespaces[i]);
    }

    close(upd_info_fd);

    if (NULL != pwd) {
        chown(path, pwd->pw_uid, pwd->pw_gid);
    }
}

static char* read_string(int32 upd_info_fd)
{
    int32 len;
    char* str = NULL;

    read(upd_info_fd, &len, sizeof(int32));
    if (0 > len) {
        return NULL;
    }

    str = (char*)pg_malloc(len + 1);
    if (len > 0) {
        read(upd_info_fd, str, len);
    }

    str[len] = '\0';

    return str;
}

void readClusterInfo(ClusterInfo* cluster, OSInfo* osinfo, char* datadir, char* cluster_filename)
{
    int32 upd_info_fd;
    int32 i;
    char path[MAXPGPATH];
    int nRet = 0;

    nRet = snprintf_s(path, sizeof(path), sizeof(path) - 1, "%s/upgrade_info/%s", datadir, cluster_filename);
    securec_check_ss_c(nRet, "\0", "\0");

    if ((upd_info_fd = open(path, O_RDONLY, 0)) < 0) {
        pg_log(PG_FATAL, "can't read old cluster infromation\n");
        return;
    }

    /* */
    read(upd_info_fd, cluster, sizeof(ClusterInfo));

    cluster->dbarr.dbs = (DbInfo*)pg_malloc((sizeof(DbInfo) * cluster->dbarr.ndbs));
    /* n * DbInfo */
    read(upd_info_fd, cluster->dbarr.dbs, (sizeof(DbInfo) * cluster->dbarr.ndbs));

    for (i = 0; i < cluster->dbarr.ndbs; i++) {
        cluster->dbarr.dbs[i].rel_arr.rels = (RelInfo*)pg_malloc(cluster->dbarr.dbs[i].rel_arr.nrels * sizeof(RelInfo));

        read(upd_info_fd, cluster->dbarr.dbs[i].rel_arr.rels, (cluster->dbarr.dbs[i].rel_arr.nrels * sizeof(RelInfo)));
    }

    cluster->controldata.lc_collate = read_string(upd_info_fd);
    cluster->controldata.lc_ctype = read_string(upd_info_fd);
    cluster->controldata.encoding = read_string(upd_info_fd);

    cluster->pgdata = read_string(upd_info_fd);
    cluster->pgconfig = read_string(upd_info_fd);
    cluster->bindir = read_string(upd_info_fd);
    cluster->pgopts = read_string(upd_info_fd);
    cluster->sockdir = read_string(upd_info_fd);
    cluster->tablespace_suffix = read_string(upd_info_fd);
    cluster->user = read_string(upd_info_fd);

    /* tablespace information */
    read(upd_info_fd, &osinfo->num_tablespaces, sizeof(int32));
    osinfo->tablespaces = (char**)pg_malloc(osinfo->num_tablespaces * sizeof(char*));
    osinfo->slavetablespaces = (char**)pg_malloc(osinfo->num_tablespaces * sizeof(char*));
    for (i = 0; i < osinfo->num_tablespaces; i++) {
        osinfo->tablespaces[i] = read_string(upd_info_fd);
        osinfo->slavetablespaces[i] = read_string(upd_info_fd);
    }

    close(upd_info_fd);

    return;
}
