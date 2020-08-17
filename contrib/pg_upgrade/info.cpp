/*
 *	info.c
 *
 *	information support functions
 *
 *	Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *	contrib/pg_upgrade/info.c
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "pg_upgrade.h"

#include "access/transam.h"

#include "catalog/pg_tablespace.h"

static void create_rel_filename_map(const char* old_data, const char* new_data, const DbInfo* old_db,
    const DbInfo* new_db, const RelInfo* old_rel, const RelInfo* new_rel, FileNameMap* map);
static void get_db_infos(ClusterInfo* cluster);
static void get_rel_infos(ClusterInfo* cluster, DbInfo* dbinfo);
static void free_rel_infos(RelInfoArr* rel_arr);
static void print_db_infos(DbInfoArr* dbinfo);
static void print_rel_infos(RelInfoArr* arr);
bool is_column_exists(PGconn* conn, Oid relid, char* column_name);

/*
 * gen_db_file_maps()
 *
 * generates database mappings for "old_db" and "new_db". Returns a malloc'ed
 * array of mappings. nmaps is a return parameter which refers to the number
 * mappings.
 */
FileNameMap* gen_db_file_maps(
    DbInfo* old_db, DbInfo* new_db, int* nmaps, const char* old_pgdata, const char* new_pgdata)
{
    FileNameMap* maps = NULL;
    int relnum;
    int num_maps = 0;

    maps = (FileNameMap*)pg_malloc(sizeof(FileNameMap) * old_db->rel_arr.nrels);

    for (relnum = 0; relnum < Min(old_db->rel_arr.nrels, new_db->rel_arr.nrels); relnum++) {
        RelInfo* old_rel = &old_db->rel_arr.rels[relnum];
        RelInfo* new_rel = &new_db->rel_arr.rels[relnum];

        if (old_rel->reloid != new_rel->reloid)
            pg_log(PG_FATAL,
                "Mismatch of relation OID in database \"%s\": old OID %d, new OID %d\n",
                old_db->db_name,
                old_rel->reloid,
                new_rel->reloid);

        /*
         * TOAST table names initially match the heap pg_class oid. In
         * pre-8.4, TOAST table names change during CLUSTER; in pre-9.0, TOAST
         * table names change during ALTER TABLE ALTER COLUMN SET TYPE. In >=
         * 9.0, TOAST relation names always use heap table oids, hence we
         * cannot check relation names when upgrading from pre-9.0. Clusters
         * upgraded to 9.0 will get matching TOAST names.
         */
        if ((strcmp(old_rel->nspname, new_rel->nspname) != 0) ||
            (((strcmp(old_rel->nspname, "pg_toast") != 0) && (strcmp(old_rel->nspname, "cstore") != 0)) &&
                strcmp(old_rel->relname, new_rel->relname) != 0))
            pg_log(PG_FATAL,
                "Mismatch of relation names in database \"%s\": "
                "old name \"%s.%s\", new name \"%s.%s\"\n",
                old_db->db_name,
                old_rel->nspname,
                old_rel->relname,
                new_rel->nspname,
                new_rel->relname);

        create_rel_filename_map(old_pgdata, new_pgdata, old_db, new_db, old_rel, new_rel, maps + num_maps);
        num_maps++;
    }

    /* Do this check after the loop so hopefully we will produce a clearer error above */
    if (old_db->rel_arr.nrels != new_db->rel_arr.nrels)
        pg_log(PG_FATAL, "old and new databases \"%s\" have a different number of relations\n", old_db->db_name);

    *nmaps = num_maps;
    return maps;
}

/*
 * create_rel_filename_map()
 *
 * fills a file node map structure and returns it in "map".
 */
static void create_rel_filename_map(const char* old_data, const char* new_data, const DbInfo* old_db,
    const DbInfo* new_db, const RelInfo* old_rel, const RelInfo* new_rel, FileNameMap* map)
{
    int nRet = 0;
    if (strlen(old_rel->tablespace) == 0) {
        /*
         * relation belongs to the default tablespace, hence relfiles should
         * exist in the data directories.
         */
        nRet = snprintf_s(
            map->old_dir, sizeof(map->old_dir), sizeof(map->old_dir) - 1, "%s/base/%u", old_data, old_db->db_oid);
        securec_check_ss_c(nRet, "\0", "\0");
        nRet = snprintf_s(
            map->new_dir, sizeof(map->new_dir), sizeof(map->new_dir) - 1, "%s/base/%u", new_data, new_db->db_oid);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        /* relation belongs to a tablespace, so use the tablespace location */
        nRet = snprintf_s(map->old_dir,
            sizeof(map->old_dir),
            sizeof(map->old_dir) - 1,
            "%s%s/%u",
            old_rel->tablespace,
            old_cluster.tablespace_suffix,
            old_db->db_oid);
        securec_check_ss_c(nRet, "\0", "\0");
        nRet = snprintf_s(map->new_dir,
            sizeof(map->new_dir),
            sizeof(map->new_dir) - 1,
            "%s%s/%u",
            new_rel->tablespace,
            new_cluster.tablespace_suffix,
            new_db->db_oid);
        securec_check_ss_c(nRet, "\0", "\0");
    }

    /*
     * old_relfilenode might differ from pg_class.oid (and hence
     * new_relfilenode) because of CLUSTER, REINDEX, or VACUUM FULL.
     */
    map->old_relfilenode = old_rel->relfilenode;

    /* new_relfilenode will match old and new pg_class.oid */
    map->new_relfilenode = new_rel->relfilenode;

    /* used only for logging and error reporing, old/new are identical */
    nRet = snprintf_s(map->nspname, sizeof(map->nspname), sizeof(map->nspname) - 1, "%s", old_rel->nspname);
    securec_check_ss_c(nRet, "\0", "\0");
    nRet = snprintf_s(map->relname, sizeof(map->relname), sizeof(map->relname) - 1, "%s", old_rel->relname);
    securec_check_ss_c(nRet, "\0", "\0");
}

void print_maps(FileNameMap* maps, int n_maps, const char* db_name)
{
    if (log_opts.verbose) {
        int mapnum;

        pg_log(PG_VERBOSE, "mappings for database \"%s\":\n", db_name);

        for (mapnum = 0; mapnum < n_maps; mapnum++)
            pg_log(PG_VERBOSE,
                "%s.%s: %u to %u\n",
                maps[mapnum].nspname,
                maps[mapnum].relname,
                maps[mapnum].old_relfilenode,
                maps[mapnum].new_relfilenode);

        pg_log(PG_VERBOSE, "\n\n");
    }
}

/*
 * get_db_and_rel_infos()
 *
 * higher level routine to generate dbinfos for the database running
 * on the given "port". Assumes that server is already running.
 */
void get_db_and_rel_infos(ClusterInfo* cluster)
{
    int dbnum;

    if (cluster->dbarr.dbs != NULL)
        free_db_and_rel_infos(&cluster->dbarr);

    get_db_infos(cluster);

    for (dbnum = 0; dbnum < cluster->dbarr.ndbs; dbnum++)
        get_rel_infos(cluster, &cluster->dbarr.dbs[dbnum]);

    pg_log(PG_VERBOSE, "\n%s databases:\n", CLUSTER_NAME(cluster));
    if (log_opts.verbose)
        print_db_infos(&cluster->dbarr);
}

/*
 * get_db_infos()
 *
 * Scans pg_database system catalog and populates all user
 * databases.
 */
static void get_db_infos(ClusterInfo* cluster)
{
    PGconn* conn = connectToServer(cluster, "template1");
    PGresult* res = NULL;
    int ntups;
    int tupnum;
    DbInfo* dbinfos = NULL;
    int i_datname, i_oid, i_spclocation, i_relative;
    char query[QUERY_ALLOC];
    char* relative = NULL;
    int nRet = 0;
    int rc = 0;
    bool is_exists = false;

    is_exists = is_column_exists(conn, TableSpaceRelationId, "relative");
    nRet = snprintf_s(query,
        sizeof(query),
        sizeof(query) - 1,
        "SELECT d.oid, d.datname, %s%s "
        "FROM pg_catalog.pg_database d "
        " LEFT OUTER JOIN pg_catalog.pg_tablespace t "
        " ON d.dattablespace = t.oid "
        "WHERE d.datallowconn = true "
        /* we don't preserve pg_database.oid so we sort by name */
        "ORDER BY 2",
        /* 9.2 removed the spclocation column */
        (GET_MAJOR_VERSION(cluster->major_version) <= 901) ? "t.spclocation"
                                                           : "pg_catalog.pg_tablespace_location(t.oid) AS spclocation",
        is_exists ? ", t.relative " : "");

    securec_check_ss_c(nRet, "\0", "\0");

    res = executeQueryOrDie(conn, "%s", query);

    i_oid = PQfnumber(res, "oid");
    i_datname = PQfnumber(res, "datname");
    i_spclocation = PQfnumber(res, "spclocation");
    if (is_exists)
        i_relative = PQfnumber(res, "relative");

    ntups = PQntuples(res);
    dbinfos = (DbInfo*)pg_malloc(sizeof(DbInfo) * ntups);

    for (tupnum = 0; tupnum < ntups; tupnum++) {
        dbinfos[tupnum].db_oid = atooid(PQgetvalue(res, tupnum, i_oid));
        nRet = snprintf_s(dbinfos[tupnum].db_name,
            sizeof(dbinfos[tupnum].db_name),
            sizeof(dbinfos[tupnum].db_name) - 1,
            "%s",
            PQgetvalue(res, tupnum, i_datname));
        securec_check_ss_c(nRet, "\0", "\0");
        if (is_exists) {
            relative = PQgetvalue(res, tupnum, i_relative);
            if (relative && *relative == 't') {
                nRet = snprintf_s(dbinfos[tupnum].db_tblspace,
                    sizeof(dbinfos[tupnum].db_tblspace),
                    sizeof(dbinfos[tupnum].db_tblspace) - 1,
                    "%s/pg_location/%s",
                    cluster->pgdata,
                    PQgetvalue(res, tupnum, i_spclocation));
                securec_check_ss_c(nRet, "\0", "\0");

                nRet = snprintf_s(dbinfos[tupnum].db_relative_tblspace,
                    sizeof(dbinfos[tupnum].db_relative_tblspace),
                    sizeof(dbinfos[tupnum].db_relative_tblspace) - 1,
                    "%s",
                    PQgetvalue(res, tupnum, i_spclocation));
                securec_check_ss_c(nRet, "\0", "\0");

            } else {
                nRet = snprintf_s(dbinfos[tupnum].db_tblspace,
                    sizeof(dbinfos[tupnum].db_tblspace),
                    sizeof(dbinfos[tupnum].db_tblspace) - 1,
                    "%s",
                    PQgetvalue(res, tupnum, i_spclocation));
                securec_check_ss_c(nRet, "\0", "\0");

                rc = memset_s(dbinfos[tupnum].db_relative_tblspace,
                    sizeof(dbinfos[tupnum].db_relative_tblspace),
                    0,
                    sizeof(dbinfos[tupnum].db_relative_tblspace));
                securec_check_c(rc, "\0", "\0");
            }
        } else {
            nRet = snprintf_s(dbinfos[tupnum].db_tblspace,
                sizeof(dbinfos[tupnum].db_tblspace),
                sizeof(dbinfos[tupnum].db_tblspace) - 1,
                "%s",
                PQgetvalue(res, tupnum, i_spclocation));
            securec_check_ss_c(nRet, "\0", "\0");

            rc = memset_s(dbinfos[tupnum].db_relative_tblspace,
                sizeof(dbinfos[tupnum].db_relative_tblspace),
                0,
                sizeof(dbinfos[tupnum].db_relative_tblspace));
            securec_check_c(rc, "\0", "\0");
        }
    }
    PQclear(res);

    PQfinish(conn);

    cluster->dbarr.dbs = dbinfos;
    cluster->dbarr.ndbs = ntups;
}

/*
 * get_rel_infos()
 *
 * gets the relinfos for all the user tables of the database referred
 * by "db".
 *
 * NOTE: we assume that relations/entities with oids greater than
 * FirstNormalObjectId belongs to the user
 */
static void get_rel_infos(ClusterInfo* cluster, DbInfo* dbinfo)
{
    PGconn* conn = connectToServer(cluster, dbinfo->db_name);
    PGresult* res = NULL;
    RelInfo* relinfos = NULL;
    int ntups;
    int relnum;
    int num_rels = 0;
    char* nspname = NULL;
    char* relname = NULL;
    char* tblpath = NULL;
    char* relative = NULL;
    char* spclocation = NULL;
    int i_spclocation, i_nspname, i_relname, i_oid, i_relfilenode, i_reltablespace, i_relative;
    char query[QUERY_ALLOC];
    int nRet = 0;
    int len = 0;
    int rc = 0;
    bool is_exists = false;

    /*
     * pg_largeobject contains user data that does not appear in pg_dumpall
     * --schema-only output, so we have to copy that system table heap and
     * index.  We could grab the pg_largeobject oids from template1, but it is
     * easy to treat it as a normal table. Order by oid so we can join old/new
     * structures efficiently.
     */
    is_exists = is_column_exists(conn, TableSpaceRelationId, "relative");
    nRet = snprintf_s(query,
        sizeof(query),
        sizeof(query) - 1,
        "SELECT p.oid, n.nspname, p.relname, pg_catalog.pg_relation_filenode(p.oid) AS relfilenode, "
        "       p.reltablespace, pg_catalog.pg_tablespace_location(t.oid) AS spclocation %s"
        " FROM pg_catalog.pg_class p INNER JOIN pg_catalog.pg_namespace n ON (p.relnamespace = n.oid)"
        " LEFT OUTER JOIN pg_catalog.pg_tablespace t ON (p.reltablespace = t.oid)"
        " WHERE p.oid < 16384 AND"
        "       p.relkind IN ('r', 'm', 'i', 't') AND"
        "       p.relisshared= false "
        " ORDER BY 1",
        is_exists ? ", t.relative " : "");
    securec_check_ss_c(nRet, "\0", "\0");

    res = executeQueryOrDie(conn, "%s", query);

    ntups = PQntuples(res);

    relinfos = (RelInfo*)pg_malloc(sizeof(RelInfo) * ntups);

    i_oid = PQfnumber(res, "oid");
    i_nspname = PQfnumber(res, "nspname");
    i_relname = PQfnumber(res, "relname");
    i_relfilenode = PQfnumber(res, "relfilenode");
    i_reltablespace = PQfnumber(res, "reltablespace");
    i_spclocation = PQfnumber(res, "spclocation");
    if (is_exists)
        i_relative = PQfnumber(res, "relative");

    for (relnum = 0; relnum < ntups; relnum++) {
        RelInfo* curr = &relinfos[num_rels++];
        const char* tblspace = NULL;

        curr->reloid = atooid(PQgetvalue(res, relnum, i_oid));

        nspname = PQgetvalue(res, relnum, i_nspname);
        strlcpy(curr->nspname, nspname, sizeof(curr->nspname));

        relname = PQgetvalue(res, relnum, i_relname);
        strlcpy(curr->relname, relname, sizeof(curr->relname));

        curr->relfilenode = atooid(PQgetvalue(res, relnum, i_relfilenode));

        if (atooid(PQgetvalue(res, relnum, i_reltablespace)) != 0) {
            /* Might be "", meaning the cluster default location. */
            spclocation = PQgetvalue(res, relnum, i_spclocation);
            if (is_exists) {
                relative = PQgetvalue(res, relnum, i_relative);
                if (relative && *relative == 't') {
                    len = strlen(cluster->pgdata) + strlen("pg_location") + strlen(spclocation) + 3;
                    tblpath = (char*)pg_malloc(len);

                    rc = memset_s(tblpath, len, 0, len);
                    securec_check_c(rc, "\0", "\0");

                    nRet = snprintf_s(tblpath, len, len - 1, "%s/pg_location/%s", cluster->pgdata, spclocation);
                    securec_check_ss_c(nRet, "\0", "\0");

                    tblspace = pg_strdup(tblpath);

                    free(tblpath);
                } else {
                    tblspace = pg_strdup(spclocation);
                }
            } else {
                tblspace = pg_strdup(spclocation);
            }
        } else
            /* A zero reltablespace indicates the database tablespace. */
            tblspace = dbinfo->db_tblspace;

        strlcpy(curr->tablespace, tblspace, sizeof(curr->tablespace));
    }
    PQclear(res);

    PQfinish(conn);

    dbinfo->rel_arr.rels = relinfos;
    dbinfo->rel_arr.nrels = num_rels;
}

void free_db_and_rel_infos(DbInfoArr* db_arr)
{
    int dbnum;

    for (dbnum = 0; dbnum < db_arr->ndbs; dbnum++)
        free_rel_infos(&db_arr->dbs[dbnum].rel_arr);
    pg_free(db_arr->dbs);
    db_arr->dbs = NULL;
    db_arr->ndbs = 0;
}

static void free_rel_infos(RelInfoArr* rel_arr)
{
    pg_free(rel_arr->rels);
    rel_arr->nrels = 0;
}

static void print_db_infos(DbInfoArr* db_arr)
{
    int dbnum;

    for (dbnum = 0; dbnum < db_arr->ndbs; dbnum++) {
        pg_log(PG_VERBOSE, "Database: %s\n", db_arr->dbs[dbnum].db_name);
        print_rel_infos(&db_arr->dbs[dbnum].rel_arr);
        pg_log(PG_VERBOSE, "\n\n");
    }
}

static void print_rel_infos(RelInfoArr* arr)
{
    int relnum;

    for (relnum = 0; relnum < arr->nrels; relnum++)
        pg_log(PG_VERBOSE,
            "relname: %s.%s: reloid: %u reltblspace: %s\n",
            arr->rels[relnum].nspname,
            arr->rels[relnum].relname,
            arr->rels[relnum].reloid,
            arr->rels[relnum].tablespace);
}
