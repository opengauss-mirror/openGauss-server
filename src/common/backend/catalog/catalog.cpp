/* -------------------------------------------------------------------------
 *
 * catalog.cpp
 *		routines concerned with catalog naming conventions and other
 *		bits of hard-wired knowledge
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/catalog.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/genam.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "catalog/catalog.h"
#include "catalog/gs_obsscaninfo.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_directory.h"
#include "catalog/pg_job.h"
#include "catalog/pg_job_proc.h"
#include "catalog/gs_job_argument.h"
#include "catalog/gs_job_attribute.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_obsscaninfo.h"
#include "catalog/pg_pltemplate.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_recyclebin.h"
#include "catalog/pg_replication_origin.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_shdescription.h"
#include "catalog/pg_shseclabel.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_auth_history.h"
#include "catalog/pg_user_status.h"
#include "catalog/toasting.h"
#include "catalog/pgxc_node.h"
#include "catalog/gs_global_config.h"
#include "catalog/pgxc_group.h"
#include "catalog/pg_extension_data_source.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_snapshot.h"
#include "commands/tablespace.h"
#include "commands/directory.h"
#include "cstore.h"
#include "storage/custorage.h"
#include "threadpool/threadpool.h"
#include "catalog/pg_resource_pool.h"
#include "catalog/pg_workload_group.h"
#include "catalog/pg_app_workloadgroup_mapping.h"
#include "miscadmin.h"
#include "storage/smgr/fd.h"
#include "storage/smgr/segment.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

#define atooid(x) ((Oid)strtoul((x), NULL, 10))

/*
 * Lookup table of fork name by fork number.
 *
 * If you add a new entry, remember to update the errhint below, and the
 * documentation for pg_relation_size(). Also keep FORKNAMECHARS above
 * up-to-date.
 */
const char* forkNames[] = {
    "main", /* MAIN_FORKNUM */
    "fsm",  /* FSM_FORKNUM */
    "vm",   /* VISIBILITYMAP_FORKNUM */
    "bcm",  /* BCM_FORKNUM */
    "init"  /* INIT_FORKNUM */
};

/*
 * forkname_to_number - look up fork number by name
 */
ForkNumber forkname_to_number(char* forkName, BlockNumber* segno)
{
    ForkNumber forkNum;
    int iforkNum;

    if (forkName == NULL || *forkName == '\0')
        return InvalidForkNumber;

    for (iforkNum = 0; iforkNum <= MAX_FORKNUM; iforkNum++) {
        forkNum = (ForkNumber)iforkNum;
        if (strcmp(forkNames[forkNum], forkName) == 0)
            return forkNum;
    }

    /*
     * if the iforkNum more than MAX_FORKNUM , the table is a column data file
     * or a column data bcm file. example C1_bcm or C1_bcm.1. C1.0 is not in the
     * database and the caller function has checked the file name in *_bcm format
     */
    char* parsepath = forkName;
    char* token = NULL;
    char* tmptoken = NULL;
    size_t parselen = strlen(parsepath);

    token = strtok_r(parsepath, "_", &tmptoken);
    Assert(token != NULL);

    if (strlen(token) == parselen) {
        /* it is a column data file. C1.0 */
        char* subtoken = NULL;
        char* tmpsubtoken = NULL;

        subtoken = strtok_r(token, ".", &tmpsubtoken);
        Assert(subtoken != NULL);
        if (strlen(subtoken) == parselen) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("invalid fork name"),
                    errhint("Valid fork names are \"main\", \"fsm\", \"bcm\", and \"vm\".")));
        } else {
            /* skip 'C' */
            subtoken = subtoken + 1;
            forkNum = ColumnId2ColForkNum(atooid(subtoken));
            if (segno != NULL)
                *segno = (BlockNumber)atooid(tmpsubtoken);

            return forkNum;
        }
    } else {
        /* it is a column data bcm file. C1_bcm */
        /* skip 'C' */
        token = token + 1;
        forkNum = ColumnId2ColForkNum(atooid(token));

        /* bcm.1 */
        token = strtok_r(NULL, ".", &tmptoken);

        if (segno != NULL) {
            if (token != NULL)
                *segno = (BlockNumber)atooid(tmptoken);
            else
                *segno = 0;
        }

        return forkNum;
    }

    return InvalidForkNumber; /* keep compiler quiet */
}

/*
 * forkname_chars
 *		We use this to figure out whether a filename could be a relation
 *		fork (as opposed to an oddly named stray file that somehow ended
 *		up in the database directory).	If the passed string begins with
 *		a fork name (other than the main fork name), we return its length,
 *		and set *fork (if not NULL) to the fork number.  If not, we return 0.
 *
 * Note that the present coding assumes that there are no fork names which
 * are prefixes of other fork names.
 */
int forkname_chars(const char* str, ForkNumber* fork)
{
    ForkNumber forkNum;
    int iforkNum;
    for (iforkNum = 1; iforkNum <= MAX_FORKNUM; iforkNum++) {
        forkNum = (ForkNumber)iforkNum;
        int len = strlen(forkNames[forkNum]);
        if (strncmp(forkNames[forkNum], str, len) == 0) {
            if (fork != NULL)
                *fork = forkNum;
            return len;
        }
    }
    return 0;
}

/*
 * relpathbackend - construct path to a relation's file
 *
 * Result is a palloc'd string.
 */
char* relpathbackend(RelFileNode rnode, BackendId backend, ForkNumber forknum)
{
    int pathlen;
    char* path = NULL;

    // Column store
    if (IsValidColForkNum(forknum)) {
        path = (char*)palloc0(MAXPGPATH * sizeof(char));

        CFileNode cFileNode(rnode, ColForkNum2ColumnId(forknum), MAIN_FORKNUM);
        CUStorage cuStorage(cFileNode);
        cuStorage.GetBcmFileName(path, 0);
        cuStorage.Destroy();
    } else {
        errno_t rc = EOK;

        if (rnode.spcNode == GLOBALTABLESPACE_OID) {
            /* Shared system relations live in {datadir}/global */
            Assert(rnode.dbNode == 0);
            Assert(IsHeapFileNode(rnode));
            Assert(backend == InvalidBackendId);
            pathlen = 7 + OIDCHARS + 1 + FORKNAMECHARS + 1;
            path = (char*)palloc(pathlen);
            if (forknum != MAIN_FORKNUM) {
                rc = snprintf_s(path, pathlen, pathlen - 1, "global/%u_%s", rnode.relNode, forkNames[forknum]);
            } else {
                rc = snprintf_s(path, pathlen, pathlen - 1, "global/%u", rnode.relNode);
            }
            securec_check_ss(rc, "\0", "\0");
        } else if (rnode.spcNode == DEFAULTTABLESPACE_OID) {
            /* The default tablespace is {datadir}/base */
            if (backend == InvalidBackendId) {
                pathlen = 5 + OIDCHARS + 1 + OIDCHARS + 1 + OIDCHARS + 1 + FORKNAMECHARS + 1 + OIDCHARS + 2;
                path = (char*)palloc(pathlen);
                if (forknum != MAIN_FORKNUM) {
                    if (!IsBucketFileNode(rnode)) {
                        rc = snprintf_s(path, pathlen, pathlen - 1, "base/%u/%u_%s",
                            rnode.dbNode, rnode.relNode, forkNames[forknum]);
                    } else {
                        rc = snprintf_s(path, pathlen, pathlen - 1, "base/%u/%u_b%d_%s",
                            rnode.dbNode, rnode.relNode, rnode.bucketNode, forkNames[forknum]);
                    }
                } else {
                    if (!IsBucketFileNode(rnode)) {
                        rc = snprintf_s(path, pathlen, pathlen - 1, "base/%u/%u", rnode.dbNode, rnode.relNode);
                    } else {
                        rc = snprintf_s(path, pathlen, pathlen - 1, "base/%u/%u_b%d",
                            rnode.dbNode, rnode.relNode, rnode.bucketNode);
                    }
                }
                securec_check_ss(rc, "\0", "\0");
            } else {
                /* OIDCHARS will suffice for an integer, too */
                Assert(!IsBucketFileNode(rnode));
                pathlen = 5 + OIDCHARS + 2 + OIDCHARS + 1 + OIDCHARS + 1 + FORKNAMECHARS + 1;
                path = (char*)palloc(pathlen);
                if (forknum != MAIN_FORKNUM) {
                    rc = snprintf_s(path, pathlen, pathlen - 1, "base/%u/t%d_%u_%s",
                        rnode.dbNode, backend, rnode.relNode, forkNames[forknum]);
                } else {
                    rc = snprintf_s(path, pathlen, pathlen - 1, "base/%u/t%d_%u", rnode.dbNode, backend, rnode.relNode);
                }
                securec_check_ss(rc, "\0", "\0");
            }
        } else {
            /* All other tablespaces are accessed via symlinks */
            if (backend == InvalidBackendId) {
                pathlen = 9 + 1 + OIDCHARS + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1 +
#ifdef PGXC
                /* Postgres-XC tablespaces include node name */
                strlen(g_instance.attr.attr_common.PGXCNodeName) + 1 +
#endif
                OIDCHARS + 1 + OIDCHARS + 1 + OIDCHARS + 2 + OIDCHARS + 1 + FORKNAMECHARS + 1;
                path = (char*)palloc(pathlen);
#ifdef PGXC
                if (forknum != MAIN_FORKNUM) {
                    if (!IsBucketFileNode(rnode)) {
                        rc = snprintf_s(path, pathlen, pathlen - 1, "pg_tblspc/%u/%s_%s/%u/%u_%s",
                            rnode.spcNode, TABLESPACE_VERSION_DIRECTORY, g_instance.attr.attr_common.PGXCNodeName,
                            rnode.dbNode, rnode.relNode, forkNames[forknum]);
                    } else {
                        rc = snprintf_s(path, pathlen, pathlen - 1, "pg_tblspc/%u/%s_%s/%u/%u_b%d_%s",
                            rnode.spcNode, TABLESPACE_VERSION_DIRECTORY, g_instance.attr.attr_common.PGXCNodeName,
                            rnode.dbNode, rnode.relNode, rnode.bucketNode, forkNames[forknum]);
                    }
                } else {
                    if (!IsBucketFileNode(rnode)) {
                        rc = snprintf_s(path,
                            pathlen,
                            pathlen - 1,
                            "pg_tblspc/%u/%s_%s/%u/%u",
                            rnode.spcNode,
                            TABLESPACE_VERSION_DIRECTORY,
                            g_instance.attr.attr_common.PGXCNodeName,
                            rnode.dbNode,
                            rnode.relNode);
                    } else {
                        rc = snprintf_s(path,
                            pathlen,
                            pathlen - 1,
                            "pg_tblspc/%u/%s_%s/%u/%u_b%d",
                            rnode.spcNode,
                            TABLESPACE_VERSION_DIRECTORY,
                            g_instance.attr.attr_common.PGXCNodeName,
                            rnode.dbNode,
                            rnode.relNode,
                            rnode.bucketNode);
                    }
                }
                securec_check_ss(rc, "\0", "\0");
#else
                if (forknum != MAIN_FORKNUM)
                    rc = snprintf_s(path,
                        pathlen,
                        pathlen - 1,
                        "pg_tblspc/%u/%s/%u/%u_%s",
                        rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY,
                        rnode.dbNode,
                        rnode.relNode,
                        forkNames[forknum]);
                else
                    rc = snprintf_s(path,
                        pathlen,
                        pathlen - 1,
                        "pg_tblspc/%u/%s/%u/%u",
                        rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY,
                        rnode.dbNode,
                        rnode.relNode);
                securec_check_ss(rc, "\0", "\0");
#endif
            } else {
                /* OIDCHARS will suffice for an integer, too */
                pathlen = 9 + 1 + OIDCHARS + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1 + OIDCHARS + 2
#ifdef PGXC
                          + strlen(g_instance.attr.attr_common.PGXCNodeName) + 1
#endif
                          + OIDCHARS + 1 + OIDCHARS + 1 + FORKNAMECHARS + 1;
                path = (char*)palloc(pathlen);
#ifdef PGXC
                if (forknum != MAIN_FORKNUM)
                    rc = snprintf_s(path,
                        pathlen,
                        pathlen - 1,
                        "pg_tblspc/%u/%s_%s/%u/t%d_%u_%s",
                        rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY,
                        g_instance.attr.attr_common.PGXCNodeName,
                        rnode.dbNode,
                        backend,
                        rnode.relNode,
                        forkNames[forknum]);
                else
                    rc = snprintf_s(path,
                        pathlen,
                        pathlen - 1,
                        "pg_tblspc/%u/%s_%s/%u/t%d_%u",
                        rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY,
                        g_instance.attr.attr_common.PGXCNodeName,
                        rnode.dbNode,
                        backend,
                        rnode.relNode);
                securec_check_ss(rc, "\0", "\0");
#else
                if (forknum != MAIN_FORKNUM)
                    rc = snprintf_s(path,
                        pathlen,
                        pathlen - 1,
                        "pg_tblspc/%u/%s/%u/t%d_%u_%s",
                        rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY,
                        rnode.dbNode,
                        backend,
                        rnode.relNode,
                        forkNames[forknum]);
                else
                    rc = snprintf_s(path,
                        pathlen,
                        pathlen - 1,
                        "pg_tblspc/%u/%s/%u/t%d_%u",
                        rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY,
                        rnode.dbNode,
                        backend,
                        rnode.relNode);
                securec_check_ss(rc, "\0", "\0");
#endif
            }
        }
    }
    return path;
}

/*
 * Parse the backend and relfilenode from the path. Path is the rear part of one whole file path, which is
 *      stripped of the type prefix like "base", "pg_tablespc/pg_version"
 * Normal Table File Path:
 *  dbNode/relNode
 *  dbNode/relNode.1 (segno = 1)
 *  dbNode/relNode_forkName 
 * Tmp Table Path:
 *  dbNode/tbackendId_relNode
 *  dbNode/tbackendId_relNode_forkName
 */
static void relpath_parse_rnode(char *path, RelFileNodeForkNum &filenode) 
{
    char* parsepath = NULL;
    char* token = NULL;
    char* tmptoken = NULL;

    if (path == NULL || *path == '\0') {
        return;
    }
    
    parsepath = path;
    
    token = strtok_r(parsepath, "/", &tmptoken);
    Assert(token != NULL);
    filenode.rnode.node.dbNode = atooid(token);
    if (tmptoken == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
            errmsg("invalid relation file path %s for relpath_parse_rnode", path)));
    }
    if ('t' != *tmptoken) {
        filenode.rnode.backend = InvalidBackendId;
        token = strtok_r(NULL, "_", &tmptoken);
        if ('\0' == *tmptoken) {
            /* dbNode/relNode[.segno] */
            filenode.rnode.node.bucketNode =  InvalidBktId;
            filenode.forknumber = MAIN_FORKNUM;
            token = strtok_r(token, ".", &tmptoken);
            if (token == NULL) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
                    errmsg("invalid relation file path %s for relpath_parse_rnode", path)));
            }
            filenode.rnode.node.relNode = atooid(token);
            filenode.segno = ('\0' == *tmptoken) ? 0 : (BlockNumber)atooid(tmptoken);
        } else {
            /* 
             *   dbNode/relNode_forkName 
             */
            filenode.rnode.node.relNode = atooid(token); /* relNode */
            filenode.rnode.node.bucketNode = InvalidBktId;
            filenode.forknumber = forkname_to_number(tmptoken, &filenode.segno);
        }
    } else {
        tmptoken = tmptoken + 1; /* skip 't' */
    
        token = strtok_r(NULL, "_", &tmptoken);
        Assert(token != NULL);
        filenode.rnode.backend = atoi(token);
        token = strtok_r(NULL, "_", &tmptoken);
        if ('\0' == *tmptoken) {
            /* dbNode/tbackendId_relNode */
            filenode.forknumber = MAIN_FORKNUM;
            token = strtok_r(token, ".", &tmptoken);
            filenode.rnode.node.relNode = atooid(token);
            filenode.segno = ('\0' == *tmptoken) ? 0 : (BlockNumber)atooid(tmptoken);
        } else {
            /* dbNode/tbackendId_relNode_forkName */
            filenode.rnode.node.relNode = atooid(token);
            filenode.forknumber = forkname_to_number(tmptoken, &filenode.segno);
        }
    }
}

/*
 * parse relation path to relfilenode
 * argument path must not be bucket dir path, because it's same with normal table file path.
 */
RelFileNodeForkNum relpath_to_filenode(char* path)
{
    RelFileNodeForkNum filenode;
    char* parsepath = NULL;
    char* token = NULL;
    char* tmptoken = NULL;

    parsepath = pstrdup(path);
    token = strtok_r(parsepath, "/", &tmptoken);

    if (NULL == tmptoken || '\0' == *tmptoken) {
        pfree(parsepath);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid relation file path %s.", path)));
    }

    /* Set default value */
    errno_t rc = memset_s(&filenode, sizeof(RelFileNodeForkNum), 0, sizeof(RelFileNodeForkNum));
    securec_check(rc, "\0", "\0");
    filenode.rnode.node.spcNode = DEFAULTTABLESPACE_OID;
    filenode.rnode.node.bucketNode = InvalidBktId;
    filenode.rnode.node.opt = 0;
    filenode.rnode.backend = InvalidBackendId;

    if (0 == strncmp(token, "global", 7)) {
        filenode.rnode.node.spcNode = GLOBALTABLESPACE_OID;
        filenode.rnode.node.dbNode = 0;
        filenode.rnode.node.bucketNode = InvalidBktId;
        filenode.rnode.backend = InvalidBackendId;

        token = strtok_r(NULL, "_", &tmptoken);
        if ('\0' == *tmptoken) {
            /* global/relNode */
            filenode.rnode.node.relNode = atooid(token);
            filenode.forknumber = MAIN_FORKNUM;
        } else {
            /* global/relNode_forkName */
            filenode.rnode.node.relNode = atooid(token);
            filenode.forknumber = forkname_to_number(tmptoken, &filenode.segno);
        }
    } else if (0 == strncmp(token, "base", 5)) {
       /*
        *   Normal Table Path:
        *      base/dbNode/relNode
        *      base/dbNode/relNode_forkName 
        *   Tmp Table Path:
        *      base/dbNode/tbackendId_relNode
        *      base/dbNode/tbackendId_relNode_forkName
        */
        filenode.rnode.node.spcNode = DEFAULTTABLESPACE_OID;
        relpath_parse_rnode(tmptoken, filenode);
    } else if (0 == strncmp(token, "pg_tblspc", 10)) {
        /*
         *   Normal Table Path:
         *      pg_tblspc/spcNode/version_dir/dbNode/relNode
         *      pg_tblspc/spcNode/version_dir/dbNode/relNode_forkName
         *   Tmp Table File Path:
         *      pg_tblspc/spcNode/version_dir/dbNode/tbackendId_relNode
         *      pg_tblspc/spcNode/version_dir/dbNode/tbackendId_relNode_forkName
         */
        token = strtok_r(NULL, "/", &tmptoken);
        filenode.rnode.node.spcNode = atooid(token);

        /* check tablespace version directory */
        token = strtok_r(NULL, "/", &tmptoken);
        if (NULL == token) {
            pfree(parsepath);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid relation file path %s.", path)));
        }

        char tblspcversiondir[MAXPGPATH];
        int errorno = snprintf_s(tblspcversiondir,
            MAXPGPATH,
            MAXPGPATH - 1,
            "%s_%s",
            TABLESPACE_VERSION_DIRECTORY,
            g_instance.attr.attr_common.PGXCNodeName);
        securec_check_ss(errorno, "\0", "\0");
        /* skip tablespaces which not belong to us. */
        if (0 != strncmp(token, tblspcversiondir, strlen(tblspcversiondir) + 1)) {
            pfree(parsepath);
            return filenode;
        }
        
        relpath_parse_rnode(tmptoken, filenode);
    } else {
        pfree(parsepath);
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid relation file path %s: %m", path)));
    }

    pfree(parsepath);
    return filenode;
}

/*
 * GetDatabasePath			- construct path to a database dir
 *
 * Result is a palloc'd string.
 *
 * XXX this must agree with relpath()!
 */
char* GetDatabasePath(Oid dbNode, Oid spcNode)
{
    int pathlen;
    char* path = NULL;
    errno_t rc = EOK;

    if (spcNode == GLOBALTABLESPACE_OID) {
        /* Shared system relations live in {datadir}/global */
        Assert(dbNode == 0);
        pathlen = 6 + 1;
        path = (char*)palloc(pathlen);
        rc = snprintf_s(path, pathlen, pathlen - 1, "global");
        securec_check_ss(rc, "\0", "\0");
    } else if (spcNode == DEFAULTTABLESPACE_OID) {
        /* The default tablespace is {datadir}/base */
        pathlen = 5 + OIDCHARS + 1;
        path = (char*)palloc(pathlen);
        rc = snprintf_s(path, pathlen, pathlen - 1, "base/%u", dbNode);
        securec_check_ss(rc, "\0", "\0");
    } else {
        /* All other tablespaces are accessed via symlinks */
        pathlen = 9 + 1 + OIDCHARS + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) +
#ifdef PGXC
                  /* Postgres-XC tablespaces include node name in path */
                  strlen(g_instance.attr.attr_common.PGXCNodeName) + 1 +
#endif
                  1 + OIDCHARS + 1;
        path = (char*)palloc(pathlen);
#ifdef PGXC
        rc = snprintf_s(path,
            pathlen,
            pathlen - 1,
            "pg_tblspc/%u/%s_%s/%u",
            spcNode,
            TABLESPACE_VERSION_DIRECTORY,
            g_instance.attr.attr_common.PGXCNodeName,
            dbNode);
#else
        rc =
            snprintf_s(path, pathlen, pathlen - 1, "pg_tblspc/%u/%s/%u", spcNode, TABLESPACE_VERSION_DIRECTORY, dbNode);
#endif
        securec_check_ss(rc, "\0", "\0");
    }
    return path;
}

/*
 * IsSystemRelation
 *		True iff the relation is a system catalog relation.
 *
 *		NB: TOAST relations are considered system relations by this test
 *		for compatibility with the old IsSystemRelationName function.
 *		This is appropriate in many places but not all.  Where it's not,
 *		also check IsToastRelation.
 *
 *		We now just test if the relation is in the system catalog namespace;
 *		so it's no longer necessary to forbid user relations from having
 *		names starting with pg_.
 */
bool IsSystemRelation(Relation relation)
{
    return IsSystemNamespace(RelationGetNamespace(relation)) || IsToastNamespace(RelationGetNamespace(relation));
}

/*
 * IsSystemClass
 *		Like the above, but takes a Form_pg_class as argument.
 *		Used when we do not want to open the relation and have to
 *		search pg_class directly.
 */
bool IsSystemClass(Form_pg_class reltuple)
{
    Oid relnamespace = reltuple->relnamespace;

    return IsSystemNamespace(relnamespace) || IsToastNamespace(relnamespace) || IsPackageSchemaOid(relnamespace);
}

bool IsSysSchema(Oid namespaceId)
{
    if (namespaceId == PG_PUBLIC_NAMESPACE) {
        return false;
    }
    return namespaceId < FirstNormalObjectId;
}

/*
 * IsCatalogRelation
 *     True iff the relation is a system catalog, or the toast table for
 *     a system catalog.  By a system catalog, we mean one that created
 *     in the pg_catalog schema during initdb.  As with IsSystemRelation(),
 *     user-created relations in pg_catalog don't count as system catalogs.
 *
 *     Note that IsSystemRelation() returns true for ALL toast relations,
 *     but this function returns true only for toast relations of system
 *     catalogs.
 */
bool IsCatalogRelation(Relation relation)
{
    return IsCatalogClass(RelationGetRelid(relation), relation->rd_rel);
}

/*
 * IsCatalogClass
 *     True iff the relation is a system catalog relation.
 *
 * Check IsCatalogRelation() for details.
 */
bool IsCatalogClass(Oid relid, Form_pg_class reltuple)
{
    Oid relnamespace = reltuple->relnamespace;

    /*
     * Never consider relations outside pg_catalog/pg_toast to be catalog
     * relations.
     */
    if (!IsSystemNamespace(relnamespace) && !IsToastNamespace(relnamespace))
        return false;

    /* ----
     * Check whether the oid was assigned during initdb, when creating the
     * initial template database. Minus the relations in information_schema
     * excluded above, these are integral part of the system.
     * We could instead check whether the relation is pinned in pg_depend, but
     * this is noticeably cheaper and doesn't require catalog access.
     *
     * This test is safe since even an oid wraparound will preserve this
     * property (c.f. GetNewObjectId()) and it has the advantage that it works
     * correctly even if a user decides to create a relation in the pg_catalog
     * namespace.
     * ----
     */
    return relid < FirstNormalObjectId;
}

/*
 * IsToastRelation
 *		True iff relation is a TOAST support relation (or index).
 */
bool IsToastRelation(Relation relation)
{
    return IsToastNamespace(RelationGetNamespace(relation));
}

/*
 * IsToastClass
 *		Like the above, but takes a Form_pg_class as argument.
 *		Used when we do not want to open the relation and have to
 *		search pg_class directly.
 */
bool IsToastClass(Form_pg_class reltuple)
{
    Oid relnamespace = reltuple->relnamespace;

    return IsToastNamespace(relnamespace);
}

/*
 * IsSystemNamespace
 *		True iff namespace is pg_catalog.
 *
 * NOTE: the reason this isn't a macro is to avoid having to include
 * catalog/pg_namespace.h in a lot of places.
 */
bool IsSystemNamespace(Oid namespaceId)
{
    return namespaceId == PG_CATALOG_NAMESPACE;
}

/*
 * IsToastNamespace
 *		True iff namespace is pg_toast or my temporary-toast-table namespace.
 *
 * Note: this will return false for temporary-toast-table namespaces belonging
 * to other backends.  Those are treated the same as other backends' regular
 * temp table namespaces, and access is prevented where appropriate.
 */
bool IsToastNamespace(Oid namespaceId)
{
    return (namespaceId == PG_TOAST_NAMESPACE) || isTempToastNamespace(namespaceId);
}

bool IsCStoreNamespace(Oid namespaceId)
{
    return (namespaceId == CSTORE_NAMESPACE);
}

/*
 * IsPerformanceNamespace
 *		True iff namespace is dbe_perf.
 *
 * NOTE: the reason this isn't a macro is to avoid having to include
 * catalog/pg_namespace.h in a lot of places.
 */
bool IsPerformanceNamespace(Oid namespaceId)
{
    return namespaceId == PG_DBEPERF_NAMESPACE;
}

/*
 * IsSnapshotNamespace
 *		True iff namespace is snapshot.
 *
 * NOTE: the reason this isn't a macro is to avoid having to include
 * catalog/pg_namespace.h in a lot of places.
 */
bool IsSnapshotNamespace(Oid namespaceId)
{
    return namespaceId == PG_SNAPSHOT_NAMESPACE;
}

bool IsMonitorSpace(Oid namespaceId)
{
    return IsPerformanceNamespace(namespaceId) || IsSnapshotNamespace(namespaceId);
}
/*
 * IsReservedName
 *		True iff name starts with the pg_ prefix.
 *
 *		For some classes of objects, the prefix pg_ is reserved for
 *		system objects only.  As of 8.0, this is only true for
 *		schema and tablespace names.
 */
bool IsReservedName(const char* name)
{
    /* ugly coding for speed */
    return (name[0] == 'p' && name[1] == 'g' && name[2] == '_');
}

/*
 * IsSharedRelation
 *		Given the OID of a relation, determine whether it's supposed to be
 *		shared across an entire database cluster.
 *
 * Hard-wiring this list is pretty grotty, but we really need it so that
 * we can compute the locktag for a relation (and then lock it) without
 * having already read its pg_class entry.	If we try to retrieve relisshared
 * from pg_class with no pre-existing lock, there is a race condition against
 * anyone who is concurrently committing a change to the pg_class entry:
 * since we read system catalog entries under SnapshotNow, it's possible
 * that both the old and new versions of the row are invalid at the instants
 * we scan them.  We fix this by insisting that updaters of a pg_class
 * row must hold exclusive lock on the corresponding rel, and that users
 * of a relation must hold at least AccessShareLock on the rel *before*
 * trying to open its relcache entry.  But to lock a rel, you have to
 * know if it's shared.  Fortunately, the set of shared relations is
 * fairly static, so a hand-maintained list of their OIDs isn't completely
 * impractical.
 */
bool IsSharedRelation(Oid relationId)
{
    /* These are the shared catalogs (look for BKI_SHARED_RELATION) */
    if (relationId == AuthIdRelationId || relationId == AuthMemRelationId || relationId == DatabaseRelationId ||
        relationId == PLTemplateRelationId || relationId == SharedDescriptionRelationId ||
        relationId == SharedDependRelationId || relationId == SharedSecLabelRelationId ||
        relationId == TableSpaceRelationId ||
        /* Database Security: Support password complexity */
        /* add AuthHistoryRelationId in shared catalogs */
        relationId == AuthHistoryRelationId ||
        /* add UserStatusRelationId in shared catalogs */
        relationId == UserStatusRelationId ||
#ifdef PGXC
        relationId == PgxcGroupRelationId || relationId == PgxcNodeRelationId || relationId == ResourcePoolRelationId ||
        relationId == WorkloadGroupRelationId || relationId == AppWorkloadGroupMappingRelationId ||
        relationId == GsGlobalConfigRelationId ||
#endif
        relationId == DbRoleSettingRelationId || relationId == PgJobRelationId || relationId == PgJobProcRelationId ||
        relationId == DataSourceRelationId || relationId == GSObsScanInfoRelationId ||
        relationId == SubscriptionRelationId || relationId == ReplicationOriginRelationId)
        return true;
    /* These are their indexes (see indexing.h) */
    if (relationId == AuthIdRolnameIndexId || relationId == AuthIdOidIndexId || relationId == AuthMemRoleMemIndexId ||
        relationId == AuthMemMemRoleIndexId || relationId == DatabaseNameIndexId || relationId == DatabaseOidIndexId ||
        relationId == PLTemplateNameIndexId || relationId == SharedDescriptionObjIndexId ||
        relationId == SharedDependDependerIndexId || relationId == SharedDependReferenceIndexId ||
        relationId == SharedSecLabelObjectIndexId || relationId == TablespaceOidIndexId ||
        relationId == TablespaceNameIndexId ||
        /* Database Security: Support password complexity */
        relationId == AuthHistoryIndexId || relationId == AuthHistoryOidIndexId ||
        relationId == UserStatusRoleidIndexId || relationId == UserStatusOidIndexId ||
#ifdef PGXC
        relationId == PgxcNodeNodeNameIndexId || relationId == PgxcNodeNodeNameIndexIdOld ||
        relationId == PgxcNodeNodeIdIndexId || relationId == PgxcNodeOidIndexId ||
        relationId == PgxcGroupGroupNameIndexId || relationId == PgxcGroupOidIndexId ||
        relationId == PgxcGroupToastTable || relationId == PgxcGroupToastIndex ||
        relationId == ResourcePoolPoolNameIndexId || relationId == ResourcePoolOidIndexId ||
        relationId == WorkloadGroupGroupNameIndexId || relationId == WorkloadGroupOidIndexId ||
        relationId == AppWorkloadGroupMappingNameIndexId || relationId == AppWorkloadGroupMappingOidIndexId ||
#endif
        relationId == DbRoleSettingDatidRolidIndexId ||
        /* Add job system table indexs */
        relationId == PgJobOidIndexId || relationId == PgJobIdIndexId || relationId == PgJobProcOidIndexId ||
        relationId == PgJobProcIdIndexId || relationId == DataSourceOidIndexId ||
        relationId == DataSourceNameIndexId || relationId == SubscriptionObjectIndexId ||
        relationId == SubscriptionNameIndexId || relationId == ReplicationOriginIdentIndex ||
        relationId == ReplicationOriginNameIndex)
        return true;
    /* These are their toast tables and toast indexes (see toasting.h) */
    if (relationId == PgShdescriptionToastTable || relationId == PgShdescriptionToastIndex ||
        relationId == PgDbRoleSettingToastTable || relationId == PgDbRoleSettingToastIndex)
        return true;

    ListCell* cell = NULL;
    foreach (cell, u_sess->upg_cxt.new_shared_catalog_list) {
        Oid relOid = lfirst_oid(cell);
        if (relOid == relationId)
            return true;
    }

    return false;
}

/*
 * GetNewOid
 *		Generate a new OID that is unique within the given relation.
 *
 * Caller must have a suitable lock on the relation.
 *
 * Uniqueness is promised only if the relation has a unique index on OID.
 * This is true for all system catalogs that have OIDs, but might not be
 * true for user tables.  Note that we are effectively assuming that the
 * table has a relatively small number of entries (much less than 2^32)
 * and there aren't very long runs of consecutive existing OIDs.  Again,
 * this is reasonable for system catalogs but less so for user tables.
 *
 * Since the OID is not immediately inserted into the table, there is a
 * race condition here; but a problem could occur only if someone else
 * managed to cycle through 2^32 OIDs and generate the same OID before we
 * finish inserting our row.  This seems unlikely to be a problem.	Note
 * that if we had to *commit* the row to end the race condition, the risk
 * would be rather higher; therefore we use SnapshotAny in the test, so that
 * so that we will see uncommitted rows.  (We used to use SnapshotDirty, but that has
 * the disadvantage that it ignores recently-deleted rows, creating a risk
 * of transient conflicts for as long as our own MVCC snapshots think a
 * recently-deleted row is live.  The risk is far higher when selecting TOAST
 * OIDs, because SnapshotToast considers dead rows as active indefinitely.)
 */
Oid GetNewOid(Relation relation)
{
    Oid oidIndex;

    /* If relation doesn't have OIDs at all, caller is confused */
    Assert(relation->rd_rel->relhasoids);

    /* In bootstrap mode, we don't have any indexes to use */
    if (IsBootstrapProcessingMode())
        return GetNewObjectId();

    /* The relcache will cache the identity of the OID index for us */
    oidIndex = RelationGetOidIndex(relation);

    /* If no OID index, just hand back the next OID counter value */
    if (!OidIsValid(oidIndex)) {
        /*
         * System catalogs that have OIDs should *always* have a unique OID
         * index; we should only take this path for user tables. Give a
         * warning if it looks like somebody forgot an index.
         */
        if (IsSystemRelation(relation))
            ereport(
                WARNING, (errmsg("generating possibly-non-unique OID for \"%s\"", RelationGetRelationName(relation))));

        return GetNewObjectId();
    }

    /* Otherwise, use the index to find a nonconflicting OID */
    return GetNewOidWithIndex(relation, oidIndex, ObjectIdAttributeNumber);
}

/*
 * GetNewOidWithIndex
 *		Guts of GetNewOid: use the supplied index
 *
 * This is exported separately because there are cases where we want to use
 * an index that will not be recognized by RelationGetOidIndex: TOAST tables
 * and pg_largeobject have indexes that are usable, but have multiple columns
 * and are on ordinary columns rather than a true OID column.  This code
 * will work anyway, so long as the OID is the index's first column.  The
 * caller must pass in the actual heap attnum of the OID column, however.
 *
 * Caller must have a suitable lock on the relation.
 */
Oid GetNewOidWithIndex(Relation relation, Oid indexId, AttrNumber oidcolumn)
{
    Oid newOid;
    SysScanDesc scan;
    ScanKeyData key;
    Snapshot snapshot;
    SnapshotData snapshotDirty;
    bool collides = false;
    bool isToastRel = IsToastNamespace(RelationGetNamespace(relation));

    InitDirtySnapshot(snapshotDirty);

    /*
     * See notes in GetNewOid about using SnapshotAny.
     * Exception: SnapshotDirty is used for upgrading non-TOAST catalog rels, because the time qual used
     * for catalogs is SnapshotNow, rather than MVCC or SnapshotToast.
     */
    snapshot = (u_sess->attr.attr_common.IsInplaceUpgrade && !isToastRel) ? &snapshotDirty : SnapshotAny;

    /* Generate new OIDs until we find one not in the table */
    do {
        CHECK_FOR_INTERRUPTS();

        newOid = GetNewObjectId(isToastRel);

        ScanKeyInit(&key, oidcolumn, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(newOid));

        scan = systable_beginscan(relation, indexId, true, snapshot, 1, &key);

        collides = HeapTupleIsValid(systable_getnext(scan));

        systable_endscan(scan);
    } while (collides);

    return newOid;
}

/*
 * GetNewRelFileNode
 *		Generate a new relfilenode number that is unique within the given
 *		tablespace.
 *
 * If the relfilenode will also be used as the relation's OID, pass the
 * opened pg_class catalog, and this routine will guarantee that the result
 * is also an unused OID within pg_class.  If the result is to be used only
 * as a relfilenode for an existing relation, pass NULL for pg_class.
 *
 * As with GetNewOid, there is some theoretical risk of a race condition,
 * but it doesn't seem worth worrying about.
 *
 * Note: we don't support using this in bootstrap mode.  All relations
 * created by bootstrap have preassigned OIDs, so there's no need.
 */
Oid GetNewRelFileNode(Oid reltablespace, Relation pg_class, char relpersistence)
{
    RelFileNodeBackend rnode;
    char* rpath = NULL;
    int fd;
    bool collides = false;
    BackendId    backend;

    switch (relpersistence) {
        case RELPERSISTENCE_GLOBAL_TEMP:
            backend = BackendIdForTempRelations;
            break;
        case RELPERSISTENCE_TEMP:
        case RELPERSISTENCE_UNLOGGED:
        case RELPERSISTENCE_PERMANENT:
            backend = InvalidBackendId;
            break;
        default:
            elog(ERROR, "invalid relpersistence: %c", relpersistence);
            return InvalidOid;    /* placate compiler */
    }  


    /* This logic should match RelationInitPhysicalAddr */
    rnode.node.spcNode = ConvertToRelfilenodeTblspcOid(reltablespace);
    rnode.node.dbNode = (rnode.node.spcNode == GLOBALTABLESPACE_OID) ? InvalidOid : GetMyDatabaseId();
    rnode.node.bucketNode = InvalidBktId;

    /*
     * The relpath will vary based on the backend ID, so we must initialize
     * that properly here to make sure that any collisions based on filename
     * are properly detected.
     */
    rnode.backend = backend;

    do {
        CHECK_FOR_INTERRUPTS();

        /* Generate the OID */
        if (pg_class)
            rnode.node.relNode = GetNewOid(pg_class);
        else
            rnode.node.relNode = GetNewObjectId();

        /* Check for existing file of same name */
        rpath = relpath(rnode, MAIN_FORKNUM);
        fd = BasicOpenFile(rpath, O_RDONLY | PG_BINARY, 0);
        if (fd >= 0) {
            /* definite collision */
            close(fd);
            collides = true;
        } else {
            /*
             * Here we have a little bit of a dilemma: if errno is something
             * other than ENOENT, should we declare a collision and loop? In
             * particular one might think this advisable for, say, EPERM.
             * However there really shouldn't be any unreadable files in a
             * tablespace directory, and if the EPERM is actually complaining
             * that we can't read the directory itself, we'd be in an infinite
             * loop.  In practice it seems best to go ahead regardless of the
             * errno.  If there is a colliding file we will get an smgr
             * failure when we attempt to create the new relation file.
             */
            collides = false;
        }
        HTAB* relfilenode_hashtbl = g_instance.bgwriter_cxt.unlink_rel_hashtbl;
        bool found = false;
        LWLockAcquire(g_instance.bgwriter_cxt.rel_hashtbl_lock, LW_SHARED);
        if (hash_search(relfilenode_hashtbl, &rnode, HASH_FIND, &found) !=  NULL) {
            collides = true;
        }
        LWLockRelease(g_instance.bgwriter_cxt.rel_hashtbl_lock);

        pfree(rpath);
        rpath = NULL;
    } while (collides);

    return rnode.node.relNode;
}

bool IsPackageSchemaOid(Oid relnamespace)
{
    const char* packageSchemaList[] = {
        "dbe_lob",
        "dbe_random",
        "dbe_output",
        "dbe_raw",
        "dbe_task",
        "dbe_scheduler",
        "dbe_sql",
        "dbe_file",
        "pkg_service",
        "pkg_util",
        "dbe_match",
        "dbe_perf",
        "dbe_session"
    };
    int schemaNum = 10;
    char* schemaName = get_namespace_name(relnamespace);
    if (schemaName == NULL) {
        return false;
    }
    for (int i = 0; i < schemaNum; ++i) {
        if (strcmp(schemaName, packageSchemaList[i]) == 0) {
            pfree_ext(schemaName);
            return true;
        }
    }
    return false;
}

bool IsPackageSchemaName(const char* schemaName)
{
    const char* packageSchemaList[] = {
        "dbe_lob",
        "dbe_random",
        "dbe_output",
        "dbe_raw",
        "dbe_task",
        "dbe_scheduler",
        "dbe_sql",
        "dbe_file",
        "pkg_service",
        "pkg_util"
    };
    int schemaNum = 10;
    for (int i = 0; i < schemaNum; ++i) {
        if (strcmp(schemaName, packageSchemaList[i]) == 0) {
            return true;
        }
    }
    return false;
}
