/*
 *	tablespace.c
 *
 *	tablespace functions
 *
 *	Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *	contrib/pg_upgrade/tablespace.c
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "pg_upgrade.h"
#include "catalog/pg_tablespace.h"

static void get_tablespace_paths(void);
static void set_tablespace_directory_suffix(ClusterInfo* cluster);
bool is_column_exists(PGconn* conn, Oid relid, char* column_name);

void init_tablespaces(void)
{
    int nRet = 0;

    get_tablespace_paths();

    set_tablespace_directory_suffix(&old_cluster);
    set_tablespace_directory_suffix(&new_cluster);

    if (user_opts.inplace_upgrade) {
        char* new_tablespace_suffix = NULL;

        new_tablespace_suffix =
            (char*)pg_malloc(strlen(old_cluster.tablespace_suffix) + strlen("full_upgrade_bak") + 10);

        nRet = snprintf_s(new_tablespace_suffix,
            strlen(old_cluster.tablespace_suffix) + strlen("full_upgrade_bak") + 10,
            strlen(old_cluster.tablespace_suffix) + strlen("full_upgrade_bak") + 9,
            "/full_upgrade_bak%s",
            old_cluster.tablespace_suffix);
        securec_check_ss_c(nRet, "\0", "\0");
        free(old_cluster.tablespace_suffix);
        old_cluster.tablespace_suffix = new_tablespace_suffix;
    }

    if (os_info.num_tablespaces > 0 && strcmp(old_cluster.tablespace_suffix, new_cluster.tablespace_suffix) == 0)
        pg_log(PG_FATAL,
            "Cannot upgrade to/from the same system catalog version when\n"
            "using tablespaces.\n");
}

/*
 * get_tablespace_paths()
 *
 * Scans pg_tablespace and returns a malloc'ed array of all tablespace
 * paths. Its the caller's responsibility to free the array.
 */
static void get_tablespace_paths(void)
{
    PGconn* conn = connectToServer(&old_cluster, "template1");
    PGresult* res = NULL;
    int tblnum;
    int i_spclocation;
    int i_relative;
    char* relative = NULL;
    char* spclocation = NULL;
    char query[QUERY_ALLOC];
    bool is_exists = false;
    char* tblpath = NULL;
    int nRet = 0;
    errno_t rc = 0;
    int len = 0;

    is_exists = is_column_exists(conn, TableSpaceRelationId, "relative");

    nRet = snprintf_s(query,
        sizeof(query),
        sizeof(query) - 1,
        "SELECT	%s%s "
        "FROM	pg_catalog.pg_tablespace "
        "WHERE	spcname != 'pg_default' AND "
        "		spcname != 'pg_global'",
        /* 9.2 removed the spclocation column */
        (GET_MAJOR_VERSION(old_cluster.major_version) <= 901) ? "spclocation"
                                                              : "pg_catalog.pg_tablespace_location(oid) AS spclocation",
        is_exists ? ", relative" : "");
    securec_check_ss_c(nRet, "\0", "\0");

    res = executeQueryOrDie(conn, "%s", query);

    if ((os_info.num_tablespaces = PQntuples(res)) != 0) {
        os_info.tablespaces = (char**)pg_malloc(os_info.num_tablespaces * sizeof(char*));
        os_info.slavetablespaces = (char**)pg_malloc(os_info.num_tablespaces * sizeof(char*));
    } else {
        os_info.tablespaces = NULL;
        os_info.slavetablespaces = NULL;
    }
    i_spclocation = PQfnumber(res, "spclocation");
    if (is_exists)
        i_relative = PQfnumber(res, "relative");

    for (tblnum = 0; tblnum < os_info.num_tablespaces; tblnum++) {
        spclocation = PQgetvalue(res, tblnum, i_spclocation);
        if (is_exists) {
            relative = PQgetvalue(res, tblnum, i_relative);
            if (relative && *relative == 't') {
                len = strlen(old_cluster.pgdata) + strlen("pg_location") + strlen(spclocation) + 3;
                tblpath = (char*)pg_malloc(len);
                rc = memset_s(tblpath, len, 0, len);
                securec_check_c(rc, "\0", "\0");
                nRet = snprintf_s(tblpath, len, len - 1, "%s/pg_location/%s", old_cluster.pgdata, spclocation);
                securec_check_ss_c(nRet, "\0", "\0");
                os_info.tablespaces[tblnum] = pg_strdup(tblpath);
                free(tblpath);

                len = MAXPGPATH + strlen("pg_location") + strlen(spclocation) + 3;
                tblpath = (char*)pg_malloc(len);
                rc = memset_s(tblpath, len, 0, len);
                securec_check_c(rc, "\0", "\0");
                nRet = snprintf_s(tblpath, len, len - 1, "%s/pg_location/%s", user_opts.pgHAdata, spclocation);
                securec_check_ss_c(nRet, "\0", "\0");
                os_info.slavetablespaces[tblnum] = pg_strdup(tblpath);
                free(tblpath);
            } else {
                os_info.tablespaces[tblnum] = pg_strdup(spclocation);
                os_info.slavetablespaces[tblnum] = pg_strdup(spclocation);
            }
        } else {
            os_info.tablespaces[tblnum] = pg_strdup(spclocation);
            os_info.slavetablespaces[tblnum] = pg_strdup(spclocation);
        }
    }

    PQclear(res);

    PQfinish(conn);

    return;
}

void update_permissions_for_tablspace_dir(void)
{
    int i;
    for (i = 0; i < os_info.num_tablespaces; i++) {
        exec_prog(UTILITY_LOG_FILE,
            NULL,
            true,
            "chown -R --reference=\"%s/global/pg_control\" \"%supgrd/%s\"",
            new_cluster.pgdata,
            os_info.tablespaces[i],
            new_cluster.tablespace_suffix);
    }
}

void create_tablspace_directories(void)
{
    int i;
    char temp_path[MAXPGPATH];
    struct passwd* pwd;
    int nRet = 0;

    set_tablespace_directory_suffix(&new_cluster);

    if ((os_info.num_tablespaces <= 0) || !(os_info.is_root_user)) {
        return;
    }

    pwd = getpwnam(new_cluster.user);
    if (pwd == NULL) {
        pg_log(PG_WARNING, "Not able to set permission for tablespaces: %s\n", getErrorText(errno));
        return;
    }

    for (i = 0; i < os_info.num_tablespaces; i++) {
        nRet = snprintf_s(temp_path, MAXPGPATH, MAXPGPATH - 1, "%supgrd", os_info.tablespaces[i]);
        securec_check_ss_c(nRet, "\0", "\0");
        if (mkdir(temp_path, S_IRWXU) < 0) {
            if (errno != EEXIST) {
                pg_log(PG_WARNING, "Not able to create tablespaces directory: %s\n", getErrorText(errno));
                continue;
            }
        }

        if (chown(temp_path, pwd->pw_uid, pwd->pw_gid) < 0) {
            pg_log(PG_WARNING, "Not able to change permission to tablespaces directory: %s\n", getErrorText(errno));
        }
    }
}

static void set_tablespace_directory_suffix(ClusterInfo* cluster)
{
    char instancename[NAMEDATALEN + 1] = {0};
    int nRet = 0;

    if (GET_MAJOR_VERSION(cluster->major_version) <= 804)
        cluster->tablespace_suffix = pg_strdup("");
    else {
        /* This cluster has a version-specific subdirectory */
        cluster->tablespace_suffix =
            (char*)pg_malloc(4 + strlen(cluster->major_version_str) + NAMEDATALEN + 10 /* OIDCHARS */ + 1);
        if (0 == get_local_instancename_by_dbpath(cluster->pgdata, ((char*)instancename) + 1)) {
            instancename[0] = '_';
        }

        /* The leading slash is needed to start a new directory. */
        nRet = snprintf_s(cluster->tablespace_suffix,
            strlen(cluster->major_version_str) + NAMEDATALEN + 15,
            strlen(cluster->major_version_str) + NAMEDATALEN + 14,
            "/PG_%s_%d%s",
            cluster->major_version_str,
            cluster->controldata.cat_ver,
            instancename);
        securec_check_ss_c(nRet, "\0", "\0");
    }
}
