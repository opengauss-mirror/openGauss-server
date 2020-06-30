/*
 *	dump.c
 *
 *	dump functions
 *
 *	Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *	contrib/pg_upgrade/dump.c
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "pg_upgrade.h"
#include "catalog/pg_statistic.h"
#include "common/config/cm_config.h"
#include <sys/types.h>

char* dump_option = "";
extern char instance_name[NAMEDATALEN];
static void doAnalyzeForTable(
    ClusterInfo* cluster, const char* dbname, const char* executeSqlCommand, const char* dumpFile);
/*
 * executeQuery: execute a SQL query.
 *
 * Params:
 * @conn:  the database connection fd.
 * @query: SQL query string.
 *
 * Returns:
 * res: execute result
 *
 */
static PGresult* executeQuery(PGconn* conn, const char* query)
{
    PGresult* res = NULL;

    res = PQexec(conn, query);
    if (!res || PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQfinish(conn);

        report_status(PG_REPORT, "*failure*");
        pg_log(PG_FATAL, "Failed to execute SQL query. \"%s\"\n", PQerrorMessage(conn));
    }
    return res;
}

/*
 * is_column_exists: check whether the column is in system table relid or not.
 *
 * Params:
 * @conn:  the database connection fd.
 * @relid: the system table relation id.
 * @column_name: the column name.
 *
 * Returns:
 * true:  it is in relid
 * false: it is not in relid
 */
bool is_column_exists(PGconn* conn, Oid relid, char* column_name)
{
    PQExpBuffer buf = createPQExpBuffer();
    PGresult* res = NULL;
    int ntups;
    printfPQExpBuffer(buf,
        "SELECT true as b_rolvalidbegin "
        "FROM pg_catalog.pg_attribute "
        "WHERE attrelid = %u AND attname = '%s';",
        relid,
        column_name);
    res = executeQuery(conn, buf->data);
    ntups = PQntuples(res);
    PQclear(res);
    destroyPQExpBuffer(buf);
    if (ntups > 0)
        return true;
    else
        return false;
}

bool is_table_exists(ClusterInfo* cluster)
{
    PGconn* conn = connectToServer(cluster, "template1");
    PQExpBuffer buf = createPQExpBuffer();
    PGresult* res = NULL;
    int ntups;

    printfPQExpBuffer(buf,
        "SELECT relname "
        "FROM pg_class c, pg_namespace n "
        "WHERE c.relname='pg_job' AND n.nspname='pg_catalog' AND c.relnamespace=n.oid;");

    res = executeQuery(conn, buf->data);
    ntups = PQntuples(res);
    PQclear(res);
    PQfinish(conn);
    destroyPQExpBuffer(buf);

    if (ntups > 0)
        return true;
    else
        return false;
}

const long default_cpu_set = 8;

void generate_old_dump(void)
{
    char usermap[USER_NAME_SIZE * 2] = {0};
    int nRet = 0;
    /* run new pg_dumpall binary */
    prep_status("Creating catalog dump");

    if (os_info.is_root_user) {
        nRet = snprintf_s(usermap,
            sizeof(usermap),
            sizeof(usermap) - 1,
            " --binary-upgrade-usermap \"%s=%s\" ",
            old_cluster.user,
            new_cluster.user);
        securec_check_ss_c(nRet, "\0", "\0");
    }

    /*
     * --binary-upgrade records the width of dropped columns in pg_class, and
     * restores the frozenid's for databases and relations.
     */
    exec_prog(UTILITY_LOG_FILE,
        NULL,
        true,
        "\"%s/gs_dumpall\" %s --dump-wrm --schema-only %s --include-nodes --include-buckets --quote-all-identifiers "
        "--include-templatedb --binary-upgrade %s %s %s -f %s/upgrade_info/%s --parallel-jobs %ld",
        new_cluster.bindir,
        cluster_conn_opts(&old_cluster),
        dump_option,
        log_opts.verbose ? "--verbose" : "",
        os_info.is_root_user ? "-T upgrd" : "",
        usermap,
        old_cluster.pgdata,
        ALL_DUMP_FILE,
        default_cpu_set);
    check_ok();

    prep_status("Creating user information dump");

    exec_prog(UTILITY_LOG_FILE,
        NULL,
        true,
        "\"%s/gs_dump\" postgres -a -F p "
        "--table pg_auth_history --table pg_user_status "
        "--schema=pg_catalog %s -f %s/upgrade_info/%s",
        new_cluster.bindir,
        cluster_conn_opts(&old_cluster),
        old_cluster.pgdata,
        USERLOG_DUMP_FILE);
    check_ok();
}

#define MAX_STATEMENT_PER_TRANSACTION 500
#define OBJECT_FORMAT "-- Name:"

/*
 *	split_old_dump
 *
 *	This function splits pg_dumpall output into global values and
 *	database creation, and per-db schemas.	This allows us to create
 *	the support functions between restoring these two parts of the
 *	dump.  We split on the first "\connect " after a CREATE ROLE
 *	username match;  this is where the per-db restore starts.
 *
 *	We suppress recreation of our own username so we don't generate
 *	an error during restore
 */
void split_old_dump(void)
{
    FILE *all_dump, *globals_dump, *db_dump;
    FILE* current_output = NULL;
    char line[LINE_ALLOC];
    bool start_of_line = true;
    char create_role_str[USER_NAME_SIZE];
    char create_role_str_quote[USER_NAME_SIZE];
    char alter_role_str[USER_NAME_SIZE];
    char alter_role_str_quote[USER_NAME_SIZE];
    char oldowner_role_str[USER_NAME_SIZE];
    char newowner_role_str[USER_NAME_SIZE];
    char filename[MAXPGPATH];
    bool suppressed_username = false;

    int32 line_len;
    int32 to_user_len;
    int32 from_user_len;
    char change_to_user_str[USER_NAME_SIZE];
    char change_from_user_str[USER_NAME_SIZE];

    char new_to_user_str[USER_NAME_SIZE];
    char new_from_user_str[USER_NAME_SIZE];

    char create_self_node_str[USER_NAME_SIZE];
    char create_self_node_Qstr[USER_NAME_SIZE];
    int i = 0;
    int rc;

    rc = snprintf_s(
        change_to_user_str, sizeof(change_to_user_str), sizeof(change_to_user_str) - 1, " TO %s;\n", old_cluster.user);
    securec_check_ss_c(rc, "\0", "\0");
    rc = snprintf_s(change_from_user_str,
        sizeof(change_from_user_str),
        sizeof(change_from_user_str) - 1,
        " FROM %s;\n",
        old_cluster.user);
    securec_check_ss_c(rc, "\0", "\0");
    rc = snprintf_s(
        new_to_user_str, sizeof(new_to_user_str), sizeof(new_to_user_str) - 1, " TO %s;\n", new_cluster.user);
    securec_check_ss_c(rc, "\0", "\0");
    rc = snprintf_s(
        new_from_user_str, sizeof(new_from_user_str), sizeof(new_from_user_str) - 1, " FROM %s;\n", new_cluster.user);
    securec_check_ss_c(rc, "\0", "\0");

    rc = snprintf_s(
        oldowner_role_str, sizeof(oldowner_role_str), sizeof(oldowner_role_str) - 1, " OWNER %s ", old_cluster.user);
    securec_check_ss_c(rc, "\0", "\0");
    rc = snprintf_s(
        newowner_role_str, sizeof(newowner_role_str), sizeof(newowner_role_str) - 1, " OWNER %s ", new_cluster.user);
    securec_check_ss_c(rc, "\0", "\0");

    to_user_len = strlen(change_to_user_str);
    from_user_len = strlen(change_from_user_str);

    /*
     * Open all files in binary mode to avoid line end translation on Windows,
     * boths for input and output.
     */

    rc = snprintf_s(
        filename, sizeof(filename), sizeof(filename) - 1, "%s/upgrade_info/%s", old_cluster.pgdata, ALL_DUMP_FILE);
    securec_check_ss_c(rc, "\0", "\0");
    if ((all_dump = fopen(filename, PG_BINARY_R)) == NULL)
        pg_log(PG_FATAL, "Could not open dump file \"%s\": %s\n", filename, getErrorText(errno));
    rc = snprintf_s(
        filename, sizeof(filename), sizeof(filename) - 1, "%s/upgrade_info/%s", old_cluster.pgdata, GLOBALS_DUMP_FILE);
    securec_check_ss_c(rc, "\0", "\0");
    if ((globals_dump = fopen_priv(filename, PG_BINARY_W)) == NULL)
        pg_log(PG_FATAL, "Could not write to dump file \"%s\": %s\n", filename, getErrorText(errno));
    rc = snprintf_s(
        filename, sizeof(filename), sizeof(filename) - 1, "%s/upgrade_info/%s", old_cluster.pgdata, DB_DUMP_FILE);
    securec_check_ss_c(rc, "\0", "\0");
    if ((db_dump = fopen_priv(filename, PG_BINARY_W)) == NULL)
        pg_log(PG_FATAL, "Could not write to dump file \"%s\": %s\n", filename, getErrorText(errno));

    current_output = globals_dump;

    /* patterns used to prevent our own username from being recreated */
    rc = snprintf_s(
        create_role_str, sizeof(create_role_str), sizeof(create_role_str) - 1, "CREATE ROLE %s", old_cluster.user);
    securec_check_ss_c(rc, "\0", "\0");
    rc = snprintf_s(create_role_str_quote,
        sizeof(create_role_str_quote),
        sizeof(create_role_str_quote) - 1,
        "CREATE ROLE %s",
        quote_identifier(old_cluster.user));
    securec_check_ss_c(rc, "\0", "\0");
    rc = snprintf_s(
        alter_role_str, sizeof(alter_role_str), sizeof(alter_role_str) - 1, "ALTER ROLE %s", old_cluster.user);
    securec_check_ss_c(rc, "\0", "\0");
    rc = snprintf_s(alter_role_str_quote,
        sizeof(alter_role_str_quote),
        sizeof(alter_role_str_quote) - 1,
        "ALTER ROLE %s",
        quote_identifier(old_cluster.user));
    securec_check_ss_c(rc, "\0", "\0");

    rc = snprintf_s(create_self_node_str,
        sizeof(create_self_node_str),
        sizeof(create_self_node_str) - 1,
        "CREATE NODE %s",
        instance_name);
    securec_check_ss_c(rc, "\0", "\0");
    rc = snprintf_s(create_self_node_Qstr,
        sizeof(create_self_node_Qstr),
        sizeof(create_self_node_Qstr) - 1,
        "CREATE NODE %s",
        quote_identifier(instance_name));
    securec_check_ss_c(rc, "\0", "\0");

    while (fgets(line, sizeof(line), all_dump) != NULL) {
        line_len = strlen(line);
        /* switch to db_dump file output? */
        if (current_output == globals_dump && start_of_line && suppressed_username &&
            strncmp(line, "\\connect ", strlen("\\connect ")) == 0)
            current_output = db_dump;

        if (start_of_line && strncmp(line, "CREATE TABLESPACE ", sizeof("CREATE TABLESPACE ") - 1) == 0) {
            char* temp = line + sizeof("CREATE TABLESPACE ") - 1;

            /*skip the tablespace name*/
            if (temp[0] == '"') {
                temp++;
                while (temp[0] != '"' && temp[0] != '\0')
                    temp++;
            } else {
                while (temp[0] != ' ' && temp[0] != '\0')
                    temp++;
            }

            if ((temp != NULL && temp[0] != '\0') &&
                (0 == strncmp(temp, oldowner_role_str, strlen(oldowner_role_str)))) {
                temp[0] = '\0';
                fputs(line, current_output);
                fputs(newowner_role_str, current_output);
                fputs(temp + strlen(oldowner_role_str), current_output);
                continue;
            }
        }

        /* output unless we are recreating our own username */
        if (current_output != globals_dump || !start_of_line ||
            ((strncmp(line, create_role_str, strlen(create_role_str)) != 0 &&
                 strncmp(line, create_role_str_quote, strlen(create_role_str_quote)) != 0) &&
                (strncmp(line, create_self_node_str, strlen(create_self_node_str)) != 0 &&
                    strncmp(line, create_self_node_Qstr, strlen(create_self_node_Qstr)) != 0) &&
                (strncmp(line, alter_role_str, strlen(alter_role_str)) != 0 &&
                    strncmp(line, alter_role_str_quote, strlen(alter_role_str_quote)) != 0))) {
            if (os_info.is_root_user) {
                if ((line_len > to_user_len) &&
                    (0 == strncmp(line + (line_len - to_user_len), change_to_user_str, to_user_len + 1))) {
                    *(line + (line_len - to_user_len)) = '\0';
                    fputs(line, current_output);
                    fputs(new_to_user_str, current_output);
                    *(line + (line_len - to_user_len)) = ' ';
                } else if ((line_len > from_user_len) &&
                           (0 == strncmp(line + (line_len - from_user_len), change_from_user_str, from_user_len + 1))) {
                    *(line + (line_len - from_user_len)) = '\0';
                    fputs(line, current_output);
                    fputs(new_from_user_str, current_output);
                    *(line + (line_len - to_user_len)) = ' ';
                } else {
                    fputs(line, current_output);
                }
            } else {
                fputs(line, current_output);
            }
        } else
            suppressed_username = true;

        if (line_len > 0 && line[line_len - 1] == '\n')
            start_of_line = true;
        else
            start_of_line = false;
    }

    fclose(all_dump);
    fclose(globals_dump);
    fclose(db_dump);

    for (i = 0; i < old_cluster.dbarr.ndbs; i++) {
        rebuild_database_dump(old_cluster.dbarr.dbs[i].db_name);
    }
}

void rebuild_database_dump(char* dbname)
{
    int rc;
    char filename[MAXPGPATH] = {0};
    char olddbfilename[MAXPGPATH] = {0};
    char newdbfilename[MAXPGPATH] = {0};
    char line[LINE_ALLOC] = {0};
    FILE* oldfp = NULL;
    FILE* newfp = NULL;
    int cur_statement = 0;

    /* filename : instance_path/upgrade_info/gs_upgrade_dump_all.sql */
    rc = snprintf_s(
        filename, sizeof(filename), sizeof(filename) - 1, "%s/upgrade_info/%s", old_cluster.pgdata, ALL_DUMP_FILE);
    securec_check_ss_c(rc, "\0", "\0");

    /* filename : instance_path/upgrade_info/gs_upgrade_dump_all */
    filename[strlen(filename) - 4] = '\0';

    rc = snprintf_s(olddbfilename, sizeof(olddbfilename), sizeof(olddbfilename) - 1, "%s_%s.sql", filename, dbname);
    securec_check_ss_c(rc, "\0", "\0");

    /* filename : instance_path/upgrade_info/gs_upgrade_dump_new_ */

    rc = snprintf_s(newdbfilename, sizeof(newdbfilename), sizeof(newdbfilename) - 1, "%s_new_%s.sql", filename, dbname);
    securec_check_ss_c(rc, "\0", "\0");

    if ((oldfp = fopen(olddbfilename, PG_BINARY_R)) == NULL) {
        pg_log(PG_FATAL, "Could not open dump file [%s]: %s\n", olddbfilename, getErrorText(errno));
    }

    if ((newfp = fopen(newdbfilename, PG_BINARY_W)) == NULL) {
        pg_log(PG_FATAL, "Could not open dump file [%s]: %s\n", newdbfilename, getErrorText(errno));
    }

    fprintf(newfp, "\n\\connect \"%s\"\n", dbname);
    fputs("\nSET xc_maintenance_mode = on;\n", newfp);
    fputs("\nSTART TRANSACTION;\n", newfp);

    while (fgets(line, sizeof(line), oldfp) != NULL) {
        if (strncmp(line, OBJECT_FORMAT, strlen(OBJECT_FORMAT)) == 0) {
            cur_statement++;
            if (cur_statement > MAX_STATEMENT_PER_TRANSACTION) {
                fputs("\nCOMMIT;\n\n", newfp);
                fputs("\nSTART TRANSACTION;\n\n", newfp);
                cur_statement = 0;
            }
        }
        fputs(line, newfp);
    }

    fputs("\nCOMMIT;\n\n", newfp);

    fclose(oldfp);
    fclose(newfp);
}

/*
 * change_database_name
 *     change the database name, if the name including "$"
 */
char* change_database_name(char* dbname)
{
    char* oldDbName = pg_strdup(dbname);
    char* newDbName = (char*)pg_malloc(MAXPGPATH * sizeof(char));
    errno_t errorno = 0;
    int i, j;
    int len = (int)strlen(oldDbName);

    errorno = memset_s(newDbName, MAXPGPATH, '\0', MAXPGPATH);
    securec_check_c(errorno, "\0", "\0");

    for (i = 0, j = 0; i < len && j < (MAXPGPATH - 1); i++, j++) {
        if ('$' == oldDbName[i]) {
            newDbName[j] = '\\';
            j++;
            newDbName[j] = oldDbName[i];
        } else {
            newDbName[j] = oldDbName[i];
        }
    }
    newDbName[j] = '\0';

    free(oldDbName);
    return newDbName;
}

void generate_statistics_dump(ClusterInfo* cluster)
{
    int dbnum = 0;
    char temppath[MAXPGPATH] = {0};
    char analyzeFilePath[MAXPGPATH] = {0};
    char jobDataFilePath[MAXPGPATH] = {0};
    char cmd[MAXPGPATH + MAXPGPATH];
    int nRet = 0;

    PGconn* conn = NULL;
    FILE* fp = NULL;
    char* encoding = NULL;
    int retval = 0;
    bool is_exists = false;
    char sqlCommand[MAXPGPATH];
    char* env_value = NULL;
    struct passwd* pwd = NULL;
    uint32 datanode_num = 0;
    char* tmpDbName = NULL;

    prep_status("Creating statistic dump information");
    nRet =
        snprintf_s(temppath, MAXPGPATH, MAXPGPATH - 1, "%s/upgrade_info/%s", old_cluster.pgdata, STATISTIC_DUMP_FILE);
    securec_check_ss_c(nRet, "\0", "\0");

    env_value = getenv("PGHOST");
    if ((NULL == env_value) || ('\0' == env_value[0])) {
        pg_log(PG_REPORT, "Failed to obtain environment variable \"PGHOST\".\n");
        exit(0);
    }

    if (MAXPGPATH <= strlen(env_value)) {
        pg_log(PG_REPORT, "The value of environment variable \"PGHOST\" is too long.\n");
        exit(0);
    }

    if (g_currentNode->coordinate && 0 == strcmp(g_currentNode->DataPath, old_cluster.pgdata)) {
        /* create analyze SQL file */
        nRet = snprintf_s(
            analyzeFilePath, MAXPGPATH, MAXPGPATH - 1, "%s/%s_%d", env_value, ANALYZE_DUMP_FILE, old_cluster.port);
        securec_check_ss_c(nRet, "\0", "\0");
        unlink(analyzeFilePath);
        fp = fopen(analyzeFilePath, "w");
        if (NULL == fp) {
            report_status(PG_REPORT, "*failure*");
            pg_log(PG_FATAL, "Could not open analyze file \"%s\"\n", analyzeFilePath);
        }
        fclose(fp);
        pwd = getpwnam(old_cluster.user);
        if (NULL != pwd) {
            chown(analyzeFilePath, pwd->pw_uid, pwd->pw_gid);
        }

        if (is_table_exists(cluster)) {
            /* create pg_job data file . if pg_catalog.pg_job is exists. */
            nRet = snprintf_s(
                jobDataFilePath, MAXPGPATH, MAXPGPATH - 1, "%s/%s_%d", env_value, JOB_DATA_DUMP_FILE, old_cluster.port);
            securec_check_ss_c(nRet, "\0", "\0");
            unlink(jobDataFilePath);
            fp = fopen(jobDataFilePath, "w");
            if (NULL == fp) {
                report_status(PG_REPORT, "*failure*");
                pg_log(PG_FATAL, "Could not open copy job data file \"%s\"\n", jobDataFilePath);
            }
            fclose(fp);
            pwd = getpwnam(old_cluster.user);
            if (NULL != pwd) {
                chown(jobDataFilePath, pwd->pw_uid, pwd->pw_gid);
            }

            exec_prog(UTILITY_LOG_FILE,
                NULL,
                true,
                "\"%s/gs_dump\" postgres -a -F p "
                "--table pg_job --table pg_job_proc "
                "--schema=pg_catalog %s -f %s",
                new_cluster.bindir,
                cluster_conn_opts(&old_cluster),
                jobDataFilePath);
            check_ok();
        }
    }

    fp = fopen(temppath, "w");
    if (NULL == fp) {
        report_status(PG_REPORT, "*failure*");
        pg_log(PG_FATAL, "Could not open dump statistics file \"%s\"\n", temppath);
    }

    fputs("\n\n--\n-- UPGRADE STATISTICS\n--\n", fp);

    for (dbnum = 0; dbnum < cluster->dbarr.ndbs; dbnum++) {
        if (0 == strncmp(cluster->dbarr.dbs[dbnum].db_name, "template", strlen("template"))) {
            continue;
        }

        /* Compatible with the V1R6C00 version which does not support the 'stadndistinct' for table pg_statistic */
        conn = connectToServer(cluster, cluster->dbarr.dbs[dbnum].db_name);
        encoding = pg_strdup(pg_encoding_to_char(PQclientEncoding(conn)));
        is_exists = is_column_exists(conn, StatisticRelationId, "stadndistinct");
        PQfinish(conn);

        fputs("\n\n--\n-- RELATION STATS\n--\n", fp);
        fprintf(fp, "\\connect %s\n\n", cluster->dbarr.dbs[dbnum].db_name);

        /* Disable enable_hadoop_env in archive for pg_restore/psql
         * if enable_hadoop_env = on, we can not restore the local tables.
         */
        fputs("SET enable_hadoop_env = off;\n\n\n", fp);
        fputs("SET xc_maintenance_mode = on;\n\n\n", fp);

        /* Create two temporary tables and two temporary procedures
         *    Table: pg_class_stat  ->  Stores the information of pg_class
         *           pg_statistic_stat  -> Stores the information of pg_statistic
         *    PROCEDURE: restore_pg_class_stat  ->  The data is updated with OID
         *               restore_pg_statistic_stat -> The data is updated with starelid, starelkind, staattnum and
         * stainherit
         */
        fputs("CREATE TABLE binary_upgrade.pg_class_stat AS \n"
              "  SELECT relname, nspname, relpages, reltuples, relallvisible \n"
              "  FROM pg_class c JOIN pg_namespace n ON(c.relnamespace = n.oid AND n.nspname NOT IN ('pg_toast', "
              "'pg_catalog', 'information_schema'))\n"
              "  WHERE false;\n"
              "\n"
              "CREATE TABLE binary_upgrade.pg_statistic_stat AS \n"
              "  SELECT relname, rn.nspname as relnspname, staattnum, starelkind, t.typname as typname, tn.nspname as "
              "typnspname, stainherit, stanullfrac,\n"
              "    stawidth, stadistinct, stakind1, stakind2, stakind3, stakind4, stakind5, staop1, staop2, staop3, "
              "staop4, staop5,\n"
              "    stanumbers1::text, stanumbers2::text, stanumbers3::text, stanumbers4::text, stanumbers5::text,\n"
              "    stavalues1::text, stavalues2::text, stavalues3::text, stavalues4::text, stavalues5::text,\n",
            fp);
        if (is_exists) {
            fputs("    t.typelem as typelem, s.stadndistinct as stadndistinct\n", fp);
        } else {
            fputs("    t.typelem as typelem\n", fp);
        }
        fputs("  FROM pg_statistic s JOIN pg_class c ON(c.oid = s.starelid AND c.relnamespace <>11) \n"
              "    JOIN pg_attribute a ON (a.attrelid = s.starelid AND a.attnum = s.staattnum) \n"
              "    JOIN pg_namespace rn ON(rn.oid = c.relnamespace AND rn.nspname NOT IN('pg_toast', 'pg_catalog', "
              "'information_schema'))\n"
              "    JOIN pg_type t ON (t.oid = a.atttypid)\n"
              "    JOIN pg_namespace tn ON(tn.oid = t.typnamespace)\n"
              "  WHERE false;\n\n",
            fp);

        fprintf(fp,
            "set client_encoding = '%s'; \nCOPY binary_upgrade.pg_class_stat FROM STDIN WITH ENCODING '%s';\n",
            encoding,
            encoding);
        fflush(fp);
        fclose(fp);

        tmpDbName = change_database_name(cluster->dbarr.dbs[dbnum].db_name);
        /* In order to avoid the client and the service code format inconsistency, so using 'set client_encoding' */
        nRet = snprintf_s(cmd,
            MAXPGPATH + MAXPGPATH,
            MAXPGPATH + MAXPGPATH - 1,
            "\"%s/gsql\" -d \"%s\" %s --quiet -c \"set client_encoding = '%s'; COPY (SELECT relname, nspname, "
            "relpages, reltuples, relallvisible "
            " FROM pg_class c JOIN pg_namespace n ON(c.relnamespace = n.oid "
            "AND n.nspname NOT IN ('pg_toast', 'pg_catalog', 'information_schema')))"
            "TO STDOUT WITH ENCODING '%s';\" >> %s",
            new_cluster.bindir,
            tmpDbName,
            cluster_conn_opts(&old_cluster),
            encoding,
            encoding,
            temppath);
        securec_check_ss_c(nRet, "\0", "\0");
        free(tmpDbName);

        retval = system(cmd);
        if (retval != 0) {
            report_status(PG_REPORT, "*failure*");
            pg_log(PG_FATAL, "There were problems executing \"%s\"\n", cmd);
        }

        fp = fopen(temppath, "a+");
        if (NULL == fp) {
            report_status(PG_REPORT, "*failure*");
            pg_log(PG_FATAL, "Could not open dump statistics file \"%s\"\n", temppath);
        }
        fputs("\\.\n;\n\n\n--\n-- STATS\n--\n", fp);
        fprintf(fp,
            "set client_encoding = '%s'; \nCOPY binary_upgrade.pg_statistic_stat FROM STDIN WITH ENCODING '%s';\n",
            encoding,
            encoding);
        fflush(fp);
        fclose(fp);

        /*
         * The current version does not support the backup of the statistics for schema 'cstore' and
         * if the table's column type is tsvetor or tsquery, it is also not supported.
         * So when we execute COPY TO, we skip them. And after the update operation is complete, analyze them
         * separately.
         */
        tmpDbName = change_database_name(cluster->dbarr.dbs[dbnum].db_name);
        if (is_exists) {
            retval = snprintf_s(cmd,
                MAXPGPATH + MAXPGPATH,
                MAXPGPATH + MAXPGPATH - 1,
                "\"%s/gsql\" -d \"%s\" %s --quiet -c \"set client_encoding = '%s'; COPY (SELECT relname, rn.nspname as "
                "relnspname, "
                "staattnum, starelkind,  typname,  tn.nspname as typnspname, stainherit, stanullfrac, "
                "stawidth, stadistinct, stakind1, stakind2, stakind3, stakind4, stakind5, "
                "staop1, staop2, staop3, staop4, staop5, stanumbers1::text, "
                "stanumbers2::text, stanumbers3::text, stanumbers4::text, stanumbers5::text, "
                "stavalues1::text, stavalues2::text, stavalues3::text, stavalues4::text, stavalues5::text, typelem, "
                "stadndistinct "
                "FROM pg_statistic s JOIN pg_class c ON(c.oid = s.starelid AND c.relnamespace <>11) "
                "JOIN pg_attribute a ON (a.attrelid = s.starelid AND a.attnum = s.staattnum) "
                "JOIN pg_namespace rn ON(rn.oid = c.relnamespace AND rn.nspname "
                "NOT IN('pg_toast', 'pg_catalog', 'information_schema', 'cstore')) "
                "JOIN pg_type t ON (t.oid = a.atttypid) JOIN pg_namespace tn ON(tn.oid = t.typnamespace) "
                "WHERE s.starelid NOT IN(SELECT c.oid FROM pg_class c, pg_attribute a, pg_type t "
                "WHERE c.relnamespace <>11 AND c.oid=a.attrelid AND c.relnamespace IN(SELECT rn.oid FROM pg_namespace "
                "rn "
                "WHERE rn.nspname NOT IN('pg_toast', 'pg_catalog', 'information_schema', 'cstore')) AND t.oid = "
                "a.atttypid "
                "AND t.typname IN('tsvector', 'tsquery', 'gtsvector') GROUP BY c.oid))"
                "TO STDOUT WITH ENCODING '%s';\" >> %s",
                new_cluster.bindir,
                tmpDbName,
                cluster_conn_opts(&old_cluster),
                encoding,
                encoding,
                temppath);
        } else {
            retval = snprintf_s(cmd,
                MAXPGPATH + MAXPGPATH,
                MAXPGPATH + MAXPGPATH - 1,
                "\"%s/gsql\" -d \"%s\" %s --quiet -c \"set client_encoding = '%s'; COPY (SELECT relname, rn.nspname as "
                "relnspname, "
                "staattnum, starelkind,  typname,  tn.nspname as typnspname, stainherit, stanullfrac, "
                "stawidth, stadistinct, stakind1, stakind2, stakind3, stakind4, stakind5, "
                "staop1, staop2, staop3, staop4, staop5, stanumbers1::text, "
                "stanumbers2::text, stanumbers3::text, stanumbers4::text, stanumbers5::text, "
                "stavalues1::text, stavalues2::text, stavalues3::text, stavalues4::text, stavalues5::text, typelem "
                "FROM pg_statistic s JOIN pg_class c ON(c.oid = s.starelid AND c.relnamespace <>11) "
                "JOIN pg_attribute a ON (a.attrelid = s.starelid AND a.attnum = s.staattnum) "
                "JOIN pg_namespace rn ON(rn.oid = c.relnamespace AND rn.nspname "
                "NOT IN('pg_toast', 'pg_catalog', 'information_schema', 'cstore')) "
                "JOIN pg_type t ON (t.oid = a.atttypid) JOIN pg_namespace tn ON(tn.oid = t.typnamespace) "
                "WHERE s.starelid NOT IN(SELECT c.oid FROM pg_class c, pg_attribute a, pg_type t "
                "WHERE c.relnamespace <>11 AND c.oid=a.attrelid AND c.relnamespace IN(SELECT rn.oid FROM pg_namespace "
                "rn "
                "WHERE rn.nspname NOT IN('pg_toast', 'pg_catalog', 'information_schema', 'cstore')) AND t.oid = "
                "a.atttypid "
                "AND t.typname IN('tsvector', 'tsquery', 'gtsvector') GROUP BY c.oid))"
                "TO STDOUT WITH ENCODING '%s';\" >> %s",
                new_cluster.bindir,
                tmpDbName,
                cluster_conn_opts(&old_cluster),
                encoding,
                encoding,
                temppath);
        }
        securec_check_ss_c(retval, "\0", "\0");
        free(tmpDbName);

        retval = system(cmd);
        if (retval != 0) {
            report_status(PG_REPORT, "*failure*");
            pg_log(PG_FATAL, "There were problems executing \"%s\"\n", cmd);
        }

        fp = fopen(temppath, "a+");
        if (NULL == fp) {
            report_status(PG_REPORT, "*failure*");
            pg_log(PG_FATAL, "Could not open dump statistics file \"%s\"\n", temppath);
        }
        fputs("\\.\n;\n\n\n--\n-- EXECUTE STATS\n--\n"
              "analyze pg_catalog.pg_statistic;\n"
              "analyze pg_catalog.pg_class;\n"
              "analyze pg_catalog.pg_type;\n"
              "analyze pg_catalog.pg_attribute; \n"
              "analyze pg_catalog.pg_namespace; \n\n",
            fp);

        fputs("\n\n\n--\n-- UPDATE pg_class\n--\n"
              "UPDATE pg_class SET (relpages, reltuples, relallvisible) = (t.relpages, t.reltuples, t.relallvisible) "
              "FROM binary_upgrade.pg_class_stat t \n"
              "    WHERE oid = (SELECT oid FROM pg_class WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE "
              "nspname = t.nspname) and relname = t.relname);\n\n",
            fp);

        fputs("\n\n\n--\n-- INSERT pg_statistic\n--\n", fp);
        /*    Upgrade Info:
         *    V1R6C00, V1R6C10, V1R7C00 stadndistinct exists
         *    V1R6C00  ->  V1R7C00
         *    V1R6C10  ->  V1R7C00
         *    V1R7C00  ->  V1R7C00
         */
        if (is_exists) {
            fputs("INSERT INTO pg_statistic (starelid, starelkind, staattnum, stainherit, stanullfrac, stawidth, "
                  "stadistinct, stadndistinct, \n"
                  "    stakind1, stakind2, stakind3, stakind4, stakind5, staop1, staop2, staop3, staop4, staop5, "
                  "stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5, \n"
                  "    stavalues1, stavalues2, stavalues3, stavalues4, stavalues5) \n"
                  "    SELECT (SELECT oid FROM pg_class WHERE relname=t.relname AND relnamespace=(SELECT oid FROM "
                  "pg_namespace WHERE nspname=t.relnspname)) relid, t.starelkind, \n"
                  "    t.staattnum, t.stainherit, t.stanullfrac, t.stawidth, t.stadistinct, t.stadndistinct, stakind1, "
                  "stakind2, stakind3, stakind4, stakind5, staop1, staop2, staop3, staop4, staop5, \n"
                  "    stanumbers1::real[], stanumbers2::real[], stanumbers3::real[], stanumbers4::real[], "
                  "stanumbers5::real[], \n"
                  "    array_in(stavalues1::cstring, CASE WHEN stakind1=4 AND typelem <> 0 THEN typelem ELSE (SELECT "
                  "oid FROM pg_type WHERE typname=t.typname AND typnamespace=(SELECT oid FROM pg_namespace WHERE "
                  "nspname=t.typnspname) LIMIT 1) END, -1), \n"
                  "    array_in(stavalues2::cstring, CASE WHEN stakind2=4 AND typelem <> 0 THEN typelem ELSE (SELECT "
                  "oid FROM pg_type WHERE typname=t.typname AND typnamespace=(SELECT oid FROM pg_namespace WHERE "
                  "nspname=t.typnspname) LIMIT 1) END, -1), \n"
                  "    array_in(stavalues3::cstring, CASE WHEN stakind3=4 and typelem <> 0 THEN typelem ELSE (SELECT "
                  "oid FROM pg_type WHERE typname=t.typname AND typnamespace=(SELECT oid FROM pg_namespace WHERE "
                  "nspname=t.typnspname) LIMIT 1) END, -1), \n"
                  "    array_in(stavalues4::cstring, CASE WHEN stakind4=4 and typelem <> 0 THEN typelem ELSE (SELECT "
                  "oid FROM pg_type WHERE typname=t.typname AND typnamespace=(SELECT oid FROM pg_namespace WHERE "
                  "nspname=t.typnspname) LIMIT 1) END, -1), \n"
                  "    array_in(stavalues5::cstring, case when stakind5=4 and typelem <> 0 THEN typelem ELSE (SELECT "
                  "oid FROM pg_type WHERE typname=t.typname AND typnamespace=(SELECT oid FROM pg_namespace WHERE "
                  "nspname=t.typnspname) LIMIT 1) END, -1) \n"
                  "    FROM binary_upgrade.pg_statistic_stat t WHERE relid IS NOT NULL;\n\n",
                fp);
        }
        /*
         *    V1R5C10 (stadndistinct not exists)
         *    V1R5C10  ->  V1R7C00   need update stadndistinct
         */
        else {
            fputs("INSERT INTO pg_statistic (starelid, starelkind, staattnum, stainherit, stanullfrac, stawidth, "
                  "stadistinct, \n"
                  "    stakind1, stakind2, stakind3, stakind4, stakind5, staop1, staop2, staop3, staop4, staop5, "
                  "stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5, \n"
                  "    stavalues1, stavalues2, stavalues3, stavalues4, stavalues5) \n"
                  "    SELECT (SELECT oid FROM pg_class WHERE relname=t.relname AND relnamespace=(SELECT oid FROM "
                  "pg_namespace WHERE nspname=t.relnspname)) relid, t.starelkind, \n"
                  "    t.staattnum, t.stainherit, t.stanullfrac, t.stawidth, t.stadistinct, stakind1, stakind2, "
                  "stakind3, stakind4, stakind5, staop1, staop2, staop3, staop4, staop5, \n"
                  "    stanumbers1::real[], stanumbers2::real[], stanumbers3::real[], stanumbers4::real[], "
                  "stanumbers5::real[], \n"
                  "    array_in(stavalues1::cstring, CASE WHEN stakind1=4 AND typelem <> 0 THEN typelem ELSE (SELECT "
                  "oid FROM pg_type WHERE typname=t.typname AND typnamespace=(SELECT oid FROM pg_namespace WHERE "
                  "nspname=t.typnspname) LIMIT 1) END, -1), \n"
                  "    array_in(stavalues2::cstring, CASE WHEN stakind2=4 AND typelem <> 0 THEN typelem ELSE (SELECT "
                  "oid FROM pg_type WHERE typname=t.typname AND typnamespace=(SELECT oid FROM pg_namespace WHERE "
                  "nspname=t.typnspname) LIMIT 1) END, -1), \n"
                  "    array_in(stavalues3::cstring, CASE WHEN stakind3=4 and typelem <> 0 THEN typelem ELSE (SELECT "
                  "oid FROM pg_type WHERE typname=t.typname AND typnamespace=(SELECT oid FROM pg_namespace WHERE "
                  "nspname=t.typnspname) LIMIT 1) END, -1), \n"
                  "    array_in(stavalues4::cstring, CASE WHEN stakind4=4 and typelem <> 0 THEN typelem ELSE (SELECT "
                  "oid FROM pg_type WHERE typname=t.typname AND typnamespace=(SELECT oid FROM pg_namespace WHERE "
                  "nspname=t.typnspname) LIMIT 1) END, -1), \n"
                  "    array_in(stavalues5::cstring, case when stakind5=4 and typelem <> 0 THEN typelem ELSE (SELECT "
                  "oid FROM pg_type WHERE typname=t.typname AND typnamespace=(SELECT oid FROM pg_namespace WHERE "
                  "nspname=t.typnspname) LIMIT 1) END, -1) \n"
                  "    FROM binary_upgrade.pg_statistic_stat t WHERE relid IS NOT NULL;\n\n",
                fp);

            // restore global statistic in new cluster when upgrade from V1R5C10 to V1R7C00. only update at coordinator
            // instance
            if (g_currentNode->coordinate && 0 == strcmp(g_currentNode->DataPath, old_cluster.pgdata)) {
                datanode_num = get_cluster_datanode_num();

                fputs("\n\n\n--\n-- UPDATE pg_statistic\n--\n", fp);
                fputs("UPDATE pg_statistic SET stadndistinct = (case when stadistinct=0.0 then 1.0 else stadistinct "
                      "end) WHERE starelid in\n"
                      "       (SELECT pcrelid FROM pgxc_class WHERE pclocatortype='R');\n",
                    fp);

                fprintf(fp,
                    "UPDATE pg_statistic SET stadndistinct = (case when stadistinct=0.0 then 1.0 else stadistinct "
                    "end),\n"
                    "    stadistinct = binary_upgrade.estimate_global_distinct(stadistinct, reltuples, %u, "
                    "int2vectorout(pcattnum)::text=staattnum::text),\n"
                    "    stanullfrac = stanullfrac / (case when int2vectorout(pcattnum)::text=staattnum::text then %u "
                    "else 1 end),\n"
                    "    stanumbers1 = case when stakind1=2 and int2vectorout(pcattnum)::text=staattnum::text then "
                    "binary_upgrade.adj_mcv(stanumbers1, %u) else stanumbers1 end,\n"
                    "    stanumbers2 = case when stakind2=2 and int2vectorout(pcattnum)::text=staattnum::text then "
                    "binary_upgrade.adj_mcv(stanumbers2, %u) else stanumbers2 end,\n"
                    "    stanumbers3 = case when stakind3=2 and int2vectorout(pcattnum)::text=staattnum::text then "
                    "binary_upgrade.adj_mcv(stanumbers3, %u) else stanumbers3 end,\n"
                    "    stanumbers4 = case when stakind4=2 and int2vectorout(pcattnum)::text=staattnum::text then "
                    "binary_upgrade.adj_mcv(stanumbers4, %u) else stanumbers4 end,\n"
                    "    stanumbers5 = case when stakind5=2 and int2vectorout(pcattnum)::text=staattnum::text then "
                    "binary_upgrade.adj_mcv(stanumbers5, %u) else stanumbers5 end\n"
                    "    FROM pg_class, pgxc_class WHERE starelid=pg_class.oid and starelid=pcrelid and pclocatortype "
                    "<> 'R';\n",
                    datanode_num,
                    datanode_num,
                    datanode_num,
                    datanode_num,
                    datanode_num,
                    datanode_num,
                    datanode_num);

                fprintf(fp,
                    "UPDATE pg_class SET (relpages, reltuples) = (relpages * %u, reltuples * %u)\n"
                    "    WHERE oid in (SELECT pcrelid FROM pgxc_class WHERE pclocatortype <> 'R');\n",
                    datanode_num,
                    datanode_num);
            }
        }
        free(encoding);

        fflush(fp);
        fclose(fp);

        /*
         * Only CN instance needs to execute it
         * Connect to the database and find the table name and schema name for the table
         * which containing the tsvector, gtsvector and tsquery type column.or the index depends on
         * the tsvector, gtsvector and tsquery type column
         * Parses the execution results, create the analyze statement, and write it into gs_upgrade_dump_statistic.sql
         */
        if (g_currentNode->coordinate && 0 == strcmp(g_currentNode->DataPath, old_cluster.pgdata)) {
            retval = memset_s(sqlCommand, MAXPGPATH, 0, MAXPGPATH);
            securec_check_c(retval, "\0", "\0");
            /* do analyze for the table */
            retval = snprintf_s(sqlCommand,
                MAXPGPATH,
                MAXPGPATH - 1,
                "%s",
                "SELECT c.relname, rn.nspname "
                "FROM pg_class c "
                "JOIN pg_namespace rn ON(c.relnamespace <> 11 AND c.relkind <> 'i' AND rn.oid = c.relnamespace AND "
                "rn.nspname "
                "NOT IN('pg_toast', 'pg_catalog', 'information_schema', 'cstore')) "
                "JOIN pg_attribute a ON(c.oid=a.attrelid) "
                "JOIN pg_type t ON(t.oid = a.atttypid and t.typname IN('tsvector', 'tsquery', 'gtsvector')) "
                "GROUP BY c.relname, rn.nspname order by c.relname");
            securec_check_ss_c(retval, "\0", "\0");
            doAnalyzeForTable(cluster, cluster->dbarr.dbs[dbnum].db_name, sqlCommand, analyzeFilePath);

            retval = memset_s(sqlCommand, MAXPGPATH, 0, MAXPGPATH);
            securec_check_c(retval, "\0", "\0");
            /* do analyze for the table which the index depends on*/
            retval = snprintf_s(sqlCommand,
                MAXPGPATH,
                MAXPGPATH,
                "%s",
                "SELECT cl.relname, np.nspname "
                "FROM pg_class cl, pg_namespace np "
                "WHERE np.oid = cl.relnamespace AND "
                "cl.oid IN (SELECT d.refobjid "
                "FROM pg_class c "
                "JOIN pg_namespace rn ON(c.relnamespace <> 11 AND c.relkind = 'i' AND rn.oid = c.relnamespace AND "
                "rn.nspname "
                "NOT IN('pg_toast', 'pg_catalog', 'information_schema', 'cstore')) "
                "JOIN pg_attribute a ON(c.oid=a.attrelid) "
                "JOIN pg_type t ON(t.oid = a.atttypid and t.typname IN('tsvector', 'tsquery', 'gtsvector')) "
                "JOIN pg_depend d ON(d.classid = 'pg_class'::regclass AND d.refobjsubid > 0 AND d.deptype = 'a' AND "
                "d.objid = c.oid)) "
                "GROUP BY cl.relname, np.nspname order by cl.relname");
            securec_check_ss_c(retval, "\0", "\0");
            doAnalyzeForTable(cluster, cluster->dbarr.dbs[dbnum].db_name, sqlCommand, analyzeFilePath);
        }

        fp = fopen(temppath, "a+");
        if (NULL == fp) {
            report_status(PG_REPORT, "*failure*");
            pg_log(PG_FATAL, "Could not open dump statistics file \"%s\"\n", temppath);
        }
        fflush(fp);
    }

    fprintf(fp, "\n\n\n--\n-- DO CHECKPOINT as this is last operation\n--\nCHECKPOINT;\n\n");

    fflush(fp);
    fclose(fp);

    check_ok();
}

static void doAnalyzeForTable(
    ClusterInfo* cluster, const char* dbname, const char* executeSqlCommand, const char* dumpFile)
{
    PGconn* conn = NULL;
    PGresult* res = NULL;
    FILE* fp = NULL;
    int ntups = 0;
    int i_relnameoid = 0;
    int i_nspnameoid = 0;
    char analyzeCommand[MAXPGPATH];
    int retval = 0;
    int i = 0;

    conn = connectToServer(cluster, dbname);
    res = executeQuery(conn, executeSqlCommand);

    ntups = PQntuples(res);
    if (ntups > 0) {
        fp = fopen(dumpFile, "a+");
        if (NULL == fp) {
            PQclear(res);
            PQfinish(conn);

            report_status(PG_REPORT, "*failure*");
            pg_log(PG_FATAL, "Could not open dump statistics file \"%s\"\n", dumpFile);
        }

        fputs("\n--\n-- EXECUTE ANALYZE\n--\n", fp);
        fprintf(fp, "\\c %s \n", dbname);
        i_relnameoid = PQfnumber(res, "relname");
        i_nspnameoid = PQfnumber(res, "nspname");
        for (i = 0; i < ntups; i++) {
            retval = memset_s(analyzeCommand, MAXPGPATH, 0, MAXPGPATH);
            securec_check_c(retval, "\0", "\0");

            retval = snprintf_s(analyzeCommand,
                MAXPGPATH,
                MAXPGPATH - 1,
                "analyze %s.%s;\n",
                PQgetvalue(res, i, i_nspnameoid),
                PQgetvalue(res, i, i_relnameoid));
            securec_check_ss_c(retval, "\0", "\0");

            fputs(analyzeCommand, fp);
        }

        fflush(fp);
        fclose(fp);
    }

    PQclear(res);
    PQfinish(conn);
}
