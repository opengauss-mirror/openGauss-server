/* -------------------------------------------------------------------------
 *
 * tablespace.cpp
 *	  Commands to manipulate table spaces
 *
 * Tablespaces in openGauss are designed to allow users to determine
 * where the data file(s) for a given database object reside on the file
 * system.
 *
 * A tablespace represents a directory on the file system. At tablespace
 * creation time, the directory must be empty. To simplify things and
 * remove the possibility of having file name conflicts, we isolate
 * files within a tablespace into database-specific subdirectories.
 *
 * To support file access via the information given in RelFileNode, we
 * maintain a symbolic-link map in $PGDATA/pg_tblspc. The symlinks are
 * named by tablespace OIDs and point to the actual tablespace directories.
 * There is also a per-cluster version directory in each tablespace.
 * Thus the full path to an arbitrary file is
 *			$PGDATA/pg_tblspc/spcoid/PG_MAJORVER_CATVER/dboid/relfilenode
 * e.g.
 *			$PGDATA/pg_tblspc/20981/PG_9.0_201002161/719849/83292814
 *
 * There are two tablespaces created at initdb time: pg_global (for shared
 * tables) and pg_default (for everything else).  For backwards compatibility
 * and to remain functional on platforms without symlinks, these tablespaces
 * are accessed specially: they are respectively
 *			$PGDATA/global/relfilenode
 *			$PGDATA/base/dboid/relfilenode
 *
 * To allow CREATE DATABASE to give a new database a default tablespace
 * that's different from the template database's default, we make the
 * provision that a zero in pg_class.reltablespace means the database's
 * default tablespace.	Without this, CREATE DATABASE would have to go in
 * and munge the system catalogs of the new database.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/tablespace.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/multi_redo_api.h"
#include "access/extreme_rto/standby_read/standby_read_delay_ddl.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "commands/seclabel.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/makefuncs.h"
#include "postmaster/bgwriter.h"
#include "storage/smgr/fd.h"
#include "storage/standby.h"
#include "storage/smgr/segment.h"
#include "storage/file/fio_device.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "workload/workload.h"
#ifdef PGXC
#include "pgxc/execRemote.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolmgr.h"
#include "pgxc/pgxc.h"
#endif
#include "replication/replicainternal.h"
#include "replication/slot.h"
#include "postmaster/rbcleaner.h"
#include "storage/tcap.h"

static void create_tablespace_directories(const char* location, const Oid tablespaceoid);
static bool destroy_tablespace_directories(Oid tablespaceoid, bool redo, bool is_exrto_read = false);
static void createtbspc_abort_callback(bool isCommit, const void* arg);

Datum CanonicalizeTablespaceOptions(Datum datum);

#define CHECK_PATH_RETRY_COUNT 100

#define CANONICALIZE_PATH(path)         \
    do {                                \
        if (NULL != (path)) {           \
            path = pstrdup(path);       \
            canonicalize_path(path);    \
        }                               \
    } while (0)

/*
 * Each database using a table space is isolated into its own name space
 * by a subdirectory named for the database OID.  On first creation of an
 * object in the tablespace, create the subdirectory.  If the subdirectory
 * already exists, fall through quietly.
 *
 * isRedo indicates that we are creating an object during WAL replay.
 * In this case we will cope with the possibility of the tablespace
 * directory not being there either --- this could happen if we are
 * replaying an operation on a table in a subsequently-dropped tablespace.
 * We handle this by making a directory in the place where the tablespace
 * symlink would normally be.  This isn't an exact replay of course, but
 * it's the best we can do given the available information.
 *
 * If tablespaces are not supported, we still need it in case we have to
 * re-create a database subdirectory (of $PGDATA/base) during WAL replay.
 */
void TablespaceCreateDbspace(Oid spcNode, Oid dbNode, bool isRedo)
{
    struct stat st;
    char* dir = NULL;

    /*
     * The global tablespace doesn't have per-database subdirectories, so
     * nothing to do for it.
     */
    if (spcNode == GLOBALTABLESPACE_OID)
        return;

    Assert(OidIsValid(spcNode));
    Assert(OidIsValid(dbNode));

    dir = GetDatabasePath(dbNode, spcNode);
    errno = 0;

    if (stat(dir, &st) < 0) {
        /* Directory does not exist? */
        if (FILE_POSSIBLY_DELETED(errno)) {
            /*
             * Acquire TablespaceCreateLock to ensure that no DROP TABLESPACE
             * or TablespaceCreateDbspace is running concurrently.
             */
            (void)LWLockAcquire(TablespaceCreateLock, LW_EXCLUSIVE);

            /*
             * Recheck to see if someone created the directory while we were
             * waiting for lock.
             */
            if (stat(dir, &st) == 0 && S_ISDIR(st.st_mode)) {
                /* Directory was created */
            } else {
                /* Directory creation failed? */
                if (mkdir(dir, S_IRWXU) < 0) {
                    char* parentdir = NULL;

                    /* Failure other than not exists or not in WAL replay? */
                    if (!FILE_POSSIBLY_DELETED(errno) || !isRedo)
                        ereport(
                            ERROR, (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", dir)));

                    /*
                     * Parent directories are missing during WAL replay, so
                     * continue by creating simple parent directories rather
                     * than a symlink.
                     */

                    /* create two parents up if not exist */
                    parentdir = pstrdup(dir);
                    /* create the first parent */
                    get_parent_directory(parentdir);
                    /* create the second parent */
                    get_parent_directory(parentdir);
                    /* Can't create parent and it doesn't already exist? */
                    if (mkdir(parentdir, S_IRWXU) < 0 && !FILE_ALREADY_EXIST(errno))
                        ereport(ERROR,
                            (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", parentdir)));
                    pfree_ext(parentdir);

                    /* create one parent up if not exist */
                    parentdir = pstrdup(dir);
                    get_parent_directory(parentdir);
                    /* Can't create parent and it doesn't already exist? */
                    if (mkdir(parentdir, S_IRWXU) < 0 && !FILE_ALREADY_EXIST(errno))
                        ereport(ERROR,
                            (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", parentdir)));
                    pfree_ext(parentdir);

                    /* Create database directory */
                    if (mkdir(dir, S_IRWXU) < 0 && !FILE_ALREADY_EXIST(errno))
                        ereport(
                            ERROR, (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", dir)));
                }
            }
            LWLockRelease(TablespaceCreateLock);
        } else {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat directory \"%s\": %m", dir)));
        }
    } else {
        /* Is it not a directory? */
        if (!S_ISDIR(st.st_mode))
            ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" exists but is not a directory", dir)));
    }

    pfree_ext(dir);
}

#define KB_PER_MB 1024          /* 2^10 */
#define KB_PER_GB 1048576       /* 2^20 */
#define KB_PER_TB 1073741824    /* 2^30 */
#define KB_PER_PB 1099511627776 /* 2^30 */

// we use int64 to store the limitation, here
// compute the uplimit with different units.
#define MAX_KB_VALUE (INT64_MAX >> 10)
#define MAX_MB_VALUE (INT64_MAX >> 20)
#define MAX_GB_VALUE (INT64_MAX >> 30)
#define MAX_TB_VALUE (INT64_MAX >> 40)
#define MAX_PB_VALUE (INT64_MAX >> 50)

static bool parse_maxsize(const char* value, int64* result, const char** hintmsg)
{
    int64 val = 0;
    char* endptr = NULL;
    StringInfoData buf;
    int tmpErrNo = 0;

    Assert(hintmsg != NULL);
    Assert(result != NULL);

    *result = 0;
    *hintmsg = NULL;
    initStringInfo(&buf);

    /* We assume here that int64 is at least as wide as long */
    errno = 0;
    val = strtol(value, &endptr, 0);
    /* remember the returned error code instantly. */
    tmpErrNo = errno;

    /* no HINT for integer syntax error */
    if (endptr == value) {
        return false;
    }

    /* until here, this string consists of some digits and unit.
     * then one of the followings maybe happen:
     * 1. these digits without unit overflow.
     * 2. these digits with unit overflow.
     * 3. they are ok.
     */
    if (tmpErrNo == ERANGE || val <= 0) {
        /* allow whitespace between integer and unit */
        while (isspace((unsigned char)*endptr))
            endptr++;

        /* if it's without unit info, we treat this as syntax error,
         * no HINT for this error.
         */
        if (*endptr == '\0') {
            appendStringInfo(&buf, "lost valid unit");
            *hintmsg = buf.data;
            return false;
        }

        if (val == 0)
            appendStringInfo(&buf, "Value is equal to 0");
        else if (*endptr == 'K' || *endptr == 'k')
            appendStringInfo(&buf, "Value exceeds max size %ld with unit KB", MAX_KB_VALUE);
        else if (*endptr == 'M' || *endptr == 'm')
            appendStringInfo(&buf, "Value exceeds max size %ld with unit MB", MAX_MB_VALUE);
        else if (*endptr == 'G' || *endptr == 'g')
            appendStringInfo(&buf, "Value exceeds max size %ld with unit GB", MAX_GB_VALUE);
        else if (*endptr == 'T' || *endptr == 't')
            appendStringInfo(&buf, "Value exceeds max size %ld with unit TB", MAX_TB_VALUE);
        else if (*endptr == 'P' || *endptr == 'p')
            appendStringInfo(&buf, "Value exceeds max size %ld with unit PB", MAX_PB_VALUE);
        else
            appendStringInfo(&buf, "Valid units are \"k/K\", \"m/M\", \"g/G\", \"t/T\", and \"p/P\".");

        *hintmsg = buf.data;
        return false;
    }

    /* allow whitespace between integer and unit */
    while (isspace((unsigned char)*endptr))
        endptr++;

    /* Handle possible unit */
    if (*endptr != '\0') {
        if (*endptr == 'K' || *endptr == 'k') {
            if (val > MAX_KB_VALUE) {
                appendStringInfo(&buf, "Value exceeds max size %ld with unit KB", MAX_KB_VALUE);
                *hintmsg = buf.data;

                return false;
            }

            endptr += 1;
        } else if (*endptr == 'M' || *endptr == 'm') {
            if (val > MAX_MB_VALUE) {
                appendStringInfo(&buf, "Value exceeds max size %ld with unit MB", MAX_MB_VALUE);
                *hintmsg = buf.data;
                return false;
            }

            endptr += 1;
            val *= KB_PER_MB;
        } else if (*endptr == 'G' || *endptr == 'g') {
            if (val > MAX_GB_VALUE) {
                appendStringInfo(&buf, "Value exceeds max size %ld with unit GB", MAX_GB_VALUE);
                *hintmsg = buf.data;

                return false;
            }

            endptr += 1;
            val *= KB_PER_GB;
        } else if (*endptr == 'T' || *endptr == 't') {
            if (val > MAX_TB_VALUE) {
                appendStringInfo(&buf, "Value exceeds max size %ld with unit TB", MAX_TB_VALUE);
                *hintmsg = buf.data;

                return false;
            }

            endptr += 1;
            val *= KB_PER_TB;
        } else if (*endptr == 'P' || *endptr == 'p') {
            if (val > MAX_PB_VALUE) {
                appendStringInfo(&buf, "Value exceeds max size %ld with unit PB", MAX_PB_VALUE);
                *hintmsg = buf.data;

                return false;
            }

            endptr += 1;
            val *= KB_PER_PB;
        }

        /* allow whitespace after unit */
        while (isspace((unsigned char)*endptr))
            endptr++;

        /* appropriate hint, if any, already set */
        if (*endptr != '\0') {
            /* Set hint for use if no match or trailing garbage */
            appendStringInfo(&buf, "Valid units are \"k/K\", \"m/M\", \"g/G\", \"t/T\", and \"p/P\".");
            *hintmsg = buf.data;

            return false;
        }
    } else {
        appendStringInfo(&buf, "lost valid unit");
        *hintmsg = buf.data;
        return false;
    }

    *result = val;

    return true;
}

/*
 * parseTableSpaceMaxSize
 *
 * Given a string that is supposed to a limited disk space, such as '200kB' or
 * 'unlimited', parse the string and convert it to a uint64 value in bytes
 * 1. return 0 if it is unlimited, or return actual value
 * 2. if it is unlimited and unlimited is not null, *unlimited  is set to be true
 * 3. if it isnot unlimited and newMaxSize is not null, *newMaxSize is set to
 *     be a suitable message to express the limited value
 */
uint64 parseTableSpaceMaxSize(char* maxSize, bool* unlimited, char** newMaxSize)
{
    int64 parsedMaxSize;
    const char* hintmsg = NULL;

    /* skip ahead whitespace */
    while (isspace((unsigned char)*maxSize))
        maxSize++;

    /* check if it is unlimited */
    const int len1 = strlen(TABLESPACE_UNLIMITED_STRING);
    const int len2 = strlen(maxSize);
    /* 1. has the same length */
    /* 2. has the same contents */
    if (len1 == len2 && !pg_strncasecmp(maxSize, TABLESPACE_UNLIMITED_STRING, len1)) {
        if (newMaxSize != NULL)
            *newMaxSize = NULL;

        if (unlimited != NULL)
            *unlimited = true;

        return 0;
    }

    if (unlimited != NULL)
        *unlimited = false;

    /* parse the message if it is limited */
    if (!parse_maxsize(maxSize, &parsedMaxSize, &hintmsg)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid value for tablespace maxsize: \"%s\"", maxSize),
                errhint("%s", (hintmsg ? hintmsg : "Unknown tablespace size"))));
    }

    if (newMaxSize != NULL) {
        int size = MAX_TABLESPACE_LIMITED_STRING_LEN * sizeof(char);
        errno_t rc = EOK;

        *newMaxSize = (char*)palloc0(size);
        rc = snprintf_s(*newMaxSize, size, size - 1, "%ld K", parsedMaxSize);
        securec_check_ss(rc, "\0", "\0");
    }

    return ((uint64)parsedMaxSize) << 10;
}

#define IsIllegalCharacter(c) ((c) != '/' && !isdigit((c)) && !isalpha((c)) && (c) != '_' && (c) != '-')

bool IsLegalAbsoluteLocation(const char* location)
{
    int NBytes = strlen(location);
    for (int i = 0; i < NBytes; i++) {
        if (IsIllegalCharacter(location[i]))
            return false;
    }
    return true;
}

bool IsLegalRelativeLocation(const char* location)
{
    int numSlash = 0;
    int NBytes = strlen(location);
    if (NBytes > 0 && location[0] == '/') {
        return false;
    }
    for (int i = 0; i < NBytes; i++) {
        if (IsIllegalCharacter(location[i]))
            return false;

        if (location[i] == '/') {
            numSlash++;
        }
    }
    /*
     * We only allow 2 level directory, for example:
     * sda/tbs1/data is illegal
     */
    if (numSlash > 1) {
        return false;
    }
    return true;
}

const char *const ReserveEnvPath[] = {
    "GAUSSHOME",
    "GAUSSLOG",
    "PGHOST"
};

static void CheckSpecificDirectory(const char *location, const char *data_directory, const char *errDesc)
{
    if (location == NULL || data_directory == NULL) {
        return;
    }
    if ((0 == strncmp(location, data_directory, strlen(data_directory))) &&
        ((strlen(location) > strlen(data_directory) && location[strlen(data_directory)] == '/') ||
        (strlen(location) == strlen(data_directory))))
        ereport(ERROR, (errmodule(MOD_TBLSPC), errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
            errmsg("tablespace cannot be created under %s directory", errDesc)));
}

static char* GetEnvRealPath(const char *env)
{
    char *envPath = gs_getenv_r(env);
    char realEnvPath[PATH_MAX + 1] = {'\0'};
    if (envPath == NULL || realpath(envPath, realEnvPath) == NULL) {
        ereport(LOG, (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
            errmsg("Get environment of %s failed.\n", env)));
        return NULL;
    }
    envPath = NULL;
    check_backend_env(realEnvPath);
    char *realPathRes = (char *)palloc0(strlen(realEnvPath) + 1);
    errno_t rc = strcpy_s(realPathRes, strlen(realEnvPath) + 1, realEnvPath);
    securec_check(rc, "\0", "\0");
    return realPathRes;
}

static void CheckLocationDataPath(const char *location)
{
    CheckSpecificDirectory(location, t_thrd.proc_cxt.DataDir, "data");
    for (uint32 i = 0; i < lengthof(ReserveEnvPath); i++) {
        char *envRealPath = GetEnvRealPath(ReserveEnvPath[i]);
        CheckSpecificDirectory(location, envRealPath, ReserveEnvPath[i]);
        pfree_ext(envRealPath);
    }
}

static void CheckAbsoluteLocationDataPath(const char *location)
{
    char realLocationPath[PATH_MAX + 1] = {'\0'};
    if (realpath(location, realLocationPath) == NULL) {
        ereport(ERROR, (errmodule(MOD_TBLSPC), errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
            errmsg("fail to get tablespace absolute location data path")));    
    }
    check_backend_env(realLocationPath);
    CheckLocationDataPath(realLocationPath);
}

/*
 * Create a table space
 *
 * Only users with sysadmin privilege or the member of gs_role_tablespace role can create a tablespace.
 * This seems a reasonable restriction since we're determining the system layout and, anyway, we probably have
 * root if we're doing this kind of activity
 */
Oid CreateTableSpace(CreateTableSpaceStmt* stmt)
{
#ifndef ENABLE_FINANCE_MODE
#ifdef HAVE_SYMLINK
    Relation rel;
    Datum values[Natts_pg_tablespace];
    bool nulls[Natts_pg_tablespace];
    HeapTuple tuple;
    Oid tablespaceoid;
    char* location = NULL;
    Oid ownerId;
    char* maxSizeStr = NULL;
    Datum newOptions;
    bool relative = stmt->relative;
    char* relativeLocation = NULL;

    if (isSecurityMode) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to create tablespace in security mode")));
    }

    if (!relative && !u_sess->attr.attr_sql.enable_absolute_tablespace)
        ereport(ERROR,
            (errmodule(MOD_TBLSPC),
                errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("Create tablespace with absolute location can't be allowed")));
    
    if (!relative && ENABLE_DSS) {
        ereport(ERROR,
            (errmodule(MOD_TBLSPC),
                errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("Can not create tablespace with absolute location in shared storage mode")));
    }

    /* Must be users with sysadmin privilege or the member of gs_role_tablespace role */
    if (!superuser() && !is_member_of_role(GetUserId(), DEFAULT_ROLE_TABLESPACE)) {
        ereport(ERROR,
            (errmodule(MOD_TBLSPC),
                errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("Permission denied to create tablespace \"%s\".", stmt->tablespacename),
                errhint("Must be system admin or a member of the gs_role_tablespace role to create a tablespace.")));
    }

    /* However, the eventual owner of the tablespace need not be */
    if (stmt->owner)
        ownerId = get_role_oid(stmt->owner, false);
    else
        ownerId = GetUserId();

    /* Unix-ify the offered path, and strip any trailing slashes */
    location = pstrdup(stmt->location);
    canonicalize_path(location);

    /* disallow quotes, else CREATE DATABASE would be at risk */
    if (strchr(location, '\''))
        ereport(ERROR,
            (errmodule(MOD_TBLSPC),
                errcode(ERRCODE_INVALID_NAME),
                errmsg("tablespace location cannot contain single quotes")));

    if (!relative) {
        /*
         * Allowing relative paths seems risky
         *
         * this also helps us ensure that location is not empty or whitespace
         */
        if (!is_absolute_path(location))
            ereport(ERROR,
                (errmodule(MOD_TBLSPC),
                    errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("tablespace location must be an absolute path")));
        /* Tablespace cannot be created under reserved directory:data, gausshome, gausslog, pghost. */
        CheckLocationDataPath(location);

        if (!IsLegalAbsoluteLocation(location))
            ereport(ERROR,
                (errmodule(MOD_TBLSPC),
                    errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("tablespace location can only be formed of 'a~z', 'A~Z', '0~9', '-', '_'")));
    } else {
        if (!IsLegalRelativeLocation(location))
            ereport(ERROR,
                (errmodule(MOD_TBLSPC),
                    errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("relative location can only be formed of 'a~z', 'A~Z', '0~9', '-', '_' and two level "
                           "directory at most")));

        if (strlen(location) <= 0)
            ereport(ERROR,
                (errmodule(MOD_TBLSPC),
                    errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("The relative location can not be null")));

        /* We need reform location for relative mode */
        int len;
        errno_t rc = EOK;
        relativeLocation = pstrdup(location);
        pfree_ext(location);

        if (ENABLE_DSS) {
            len = (int)strlen(PG_LOCATION_DIR) + 1 + (int)strlen(relativeLocation) + 1;
            location = (char*)palloc(len);
            rc = snprintf_s(location, len, len - 1, "%s/%s", PG_LOCATION_DIR, relativeLocation);
        } else {
            if (t_thrd.proc_cxt.DataDir[strlen(t_thrd.proc_cxt.DataDir)] == '/') {
                len = (int)strlen(t_thrd.proc_cxt.DataDir) + (int)strlen(PG_LOCATION_DIR) +
                    1 + (int)strlen(relativeLocation) + 1;
                location = (char*)palloc(len);
                rc = snprintf_s(
                    location, len, len - 1, "%s%s/%s", t_thrd.proc_cxt.DataDir, PG_LOCATION_DIR, relativeLocation);
            } else {
                len = (int)strlen(t_thrd.proc_cxt.DataDir) + 1 + (int)strlen(PG_LOCATION_DIR) +
                    1 + (int)strlen(relativeLocation) + 1;
                location = (char*)palloc(len);
                rc = snprintf_s(
                    location, len, len - 1, "%s/%s/%s", t_thrd.proc_cxt.DataDir, PG_LOCATION_DIR, relativeLocation);
            }
        }
        securec_check_ss(rc, "\0", "\0");
    }

    /*
     * Check that location isn't too long. Remember that we're going to append
     * 'PG_XXX/<dboid>/<relid>.<nnn>'.	FYI, we never actually reference the
     * whole path, but mkdir() uses the first two parts.
     */
    if (strlen(location) + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1 +
#ifdef PGXC
            /*
             * In Postgres-XC, node name is added in the tablespace folder name to
             * insure unique names for nodes sharing the same server.
             * So real format is PG_XXX_<nodename>/<dboid>/<relid>.<nnn>''
             */
            strlen(g_instance.attr.attr_common.PGXCNodeName) + 1 +
#endif
            OIDCHARS + 1 + OIDCHARS + 1 + OIDCHARS >
        MAXPGPATH)
        ereport(ERROR,
            (errmodule(MOD_TBLSPC),
                errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("tablespace location \"%s\" is too long", relative ? relativeLocation : location)));

    /*
     * Disallow creation of tablespaces named "pg_xxx"; we reserve this
     * namespace for system purposes.
     */
    if (!g_instance.attr.attr_common.allowSystemTableMods && !u_sess->attr.attr_common.IsInplaceUpgrade &&
        IsReservedName(stmt->tablespacename))
        ereport(ERROR,
            (errmodule(MOD_TBLSPC),
                errcode(ERRCODE_RESERVED_NAME),
                errmsg("unacceptable tablespace name \"%s\"", stmt->tablespacename),
                errdetail("The prefix \"pg_\" is reserved for system tablespaces.")));

    /*
     * Check that there is no other tablespace by this name.  (The unique
     * index would catch this anyway, but might as well give a friendlier
     * message.)
     */
    if (OidIsValid(get_tablespace_oid(stmt->tablespacename, true)))
        ereport(ERROR,
            (errmodule(MOD_TBLSPC),
                errcode(ERRCODE_DUPLICATE_OBJECT),
                errmsg("tablespace \"%s\" already exists", stmt->tablespacename)));

    /*
     * Acquire TablespaceCreateLock to ensure 'check_create_dir' is safe.
     */
    (void)LWLockAcquire(TablespaceCreateLock, LW_EXCLUSIVE);

    rel = heap_open(TableSpaceRelationId, RowExclusiveLock);

    check_create_dir(location);
    
    /* Tablespace can't be created under reserved directory:data, gausshome, gausslog, pghost. Check the real path. */
    if (!relative) {
        CheckAbsoluteLocationDataPath(location);
    }

    errno_t rc = EOK;
    rc = memset_s(nulls, Natts_pg_tablespace, false, Natts_pg_tablespace);
    securec_check(rc, "\0", "\0");

    values[Anum_pg_tablespace_spcname - 1] = DirectFunctionCall1(namein, CStringGetDatum(stmt->tablespacename));
    values[Anum_pg_tablespace_spcowner - 1] = ObjectIdGetDatum(ownerId);
    nulls[Anum_pg_tablespace_spcacl - 1] = true;

    /* Generate new proposed spcoptions (text array) */
    newOptions = transformRelOptions((Datum)0, stmt->options, NULL, NULL, false, false);
    (void)tablespace_reloptions(newOptions, true);
    if (newOptions != (Datum)0) {
        newOptions = CanonicalizeTablespaceOptions(newOptions);
        values[Anum_pg_tablespace_spcoptions - 1] = newOptions;
    } else {
        nulls[Anum_pg_tablespace_spcoptions - 1] = true;
    }

    if (stmt->maxsize) {
        bool unLimited = false;

        (void)parseTableSpaceMaxSize(stmt->maxsize, &unLimited, &maxSizeStr);

        if (unLimited) {
            nulls[Anum_pg_tablespace_maxsize - 1] = true;
        } else {
            values[Anum_pg_tablespace_maxsize - 1] = DirectFunctionCall1(textin, CStringGetDatum(maxSizeStr));
        }
    } else
        nulls[Anum_pg_tablespace_maxsize - 1] = true;

    values[Anum_pg_tablespace_relative - 1] = relative;

    tuple = heap_form_tuple(rel->rd_att, values, nulls);

    tablespaceoid = simple_heap_insert(rel, tuple);

    CatalogUpdateIndexes(rel, tuple);

    heap_freetuple(tuple);

    /* Record dependency on owner */
    recordDependencyOnOwner(TableSpaceRelationId, tablespaceoid, ownerId);

    /* Post creation hook for new tablespace */
    InvokeObjectAccessHook(OAT_POST_CREATE, TableSpaceRelationId, tablespaceoid, 0, NULL);

    /*
     * Check the validity of options in order to keep consistency.
     * if we do not check the validity and do not get dfs connector, the
     * local directory has been created, but failed to create the dfs directory.
     */
    create_tablespace_directories(location, tablespaceoid);

#ifdef PGXC
    /*
     * Even if we have succeeded, the transaction can be aborted because of
     * failure on other nodes. So register for cleanup.
     */
    set_dbcleanup_callback(createtbspc_abort_callback, &tablespaceoid, sizeof(tablespaceoid));
#endif

    /* Record the filesystem change in XLOG */
    {
        /*
         * for relative location, xlog must record relative location
         * because maybe standby DN data directory is not the same.
         */
        char* locationPtr = relative ? relativeLocation : location;
        xl_tblspc_create_rec xlrec;

        xlrec.ts_id = tablespaceoid;

        XLogBeginInsert();
        XLogRegisterData((char*)&xlrec, offsetof(xl_tblspc_create_rec, ts_path));
        XLogRegisterData((char*)locationPtr, strlen(locationPtr) + 1);

        /*
         * if we expand xl_tblspc_create_rec the upgrade must require checkpoint first,
         * So We use different xlog info to mark relative
         */
        (void)XLogInsert(RM_TBLSPC_ID, relative ? XLOG_TBLSPC_RELATIVE_CREATE : XLOG_TBLSPC_CREATE);
    }

    /*
     * We force a checkpoint before committing.  This effectively means
     * that committed XLOG_TBLSPC_CREATE operations will never need to be
     * replayed (at least not in ordinary crash recovery; we still have to
     * make the XLOG entry for the benefit of PITR operations). This
     * avoids two nasty scenarios:
     *
     * We don't XLOG the data of bulkload when we turn on data replicate
     * or column table, we only log a logical XLOG record under those scenes;
     * therefore the drop-and-recreate-whole-directory behavior of TBLSPC_CREATE
     * replay would lose such data.
     *
     * In MPPDB, we do not support PITR recovery. so it's not necessary to
     * take that into consideration.
     *
     * Perhaps if we ever implement CREATE TABLE in a less cheesy way,
     * we can avoid this.
     */
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

    /*
     * Wait for last checkpoint sync to standby and then flush the latest lsn to disk;
     */
    WaitCheckpointSync();
    CheckPointReplicationSlots();

    /*
     * Force synchronous commit, to minimize the window between creating the
     * symlink on-disk and marking the transaction committed.  It's not great
     * that there is any window at all, but definitely we don't want to make
     * it larger than necessary.
     */
    ForceSyncCommit();

    LWLockRelease(TablespaceCreateLock);
    pfree_ext(location);

    if (relativeLocation != NULL)
        pfree_ext(relativeLocation);

    if (maxSizeStr != NULL)
        pfree_ext(maxSizeStr);

    /* We keep the lock on pg_tablespace until commit */
    heap_close(rel, NoLock);
    return tablespaceoid;
#else  /* !HAVE_SYMLINK */
    ereport(ERROR,
        (errmodule(MOD_TBLSPC),
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("tablespaces are not supported on this platform")));
#endif /* HAVE_SYMLINK */   
#else /* ENABLE_FINANCE_MODE */
    ereport(ERROR,
        (errmodule(MOD_TBLSPC),
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("create tablespaces is incorrect, not work on finance mode")));
#endif /* ENABLE_FINANCE_MODE */ 
}

/*
 * Drop a table space
 *
 * Be careful to check that the tablespace is empty.
 */
void DropTableSpace(DropTableSpaceStmt* stmt)
{
#ifdef HAVE_SYMLINK
    char* tablespacename = stmt->tablespacename;
    TableScanDesc scandesc;
    Relation rel;
    HeapTuple tuple;
    ScanKeyData entry[1];
    Oid tablespaceoid;
    TableScanDesc scan;
    ScanKeyData scankey[1];
    Relation partrel = NULL;

    /*
     * Find the target tuple
     */
    rel = heap_open(TableSpaceRelationId, RowExclusiveLock);

    ScanKeyInit(
        &entry[0], Anum_pg_tablespace_spcname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(tablespacename));
    scandesc = tableam_scan_begin(rel, SnapshotNow, 1, entry);
    tuple = (HeapTuple) tableam_scan_getnexttuple(scandesc, ForwardScanDirection);
    if (!HeapTupleIsValid(tuple)) {
        if (!stmt->missing_ok) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Tablespace \"%s\" does not exist.", tablespacename)));
        } else {
            ereport(NOTICE, (errmsg("Tablespace \"%s\" does not exist, skipping.", tablespacename)));
            /* XXX I assume I need one or both of these next two calls */
            tableam_scan_end(scandesc);
            heap_close(rel, NoLock);
        }
        return;
    }

    tablespaceoid = HeapTupleGetOid(tuple);
    /* Must be tablespace owner or have drop privileges of the target object. */
    AclResult aclresult = pg_tablespace_aclcheck(tablespaceoid, GetUserId(), ACL_DROP);
    if (aclresult != ACLCHECK_OK && !pg_tablespace_ownercheck(tablespaceoid, GetUserId())
        && (!is_member_of_role(GetUserId(), DEFAULT_ROLE_TABLESPACE) || !DB_IS_CMPT(B_FORMAT))) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_TABLESPACE, tablespacename);
    }

    /* Disallow drop of the standard tablespaces, even by superuser */
    if (tablespaceoid == GLOBALTABLESPACE_OID || tablespaceoid == DEFAULTTABLESPACE_OID)
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_TABLESPACE, tablespacename);

    /* DROP hook for the tablespace being removed */
    if (object_access_hook) {
        ObjectAccessDrop drop_arg;
        errno_t rc = EOK;

        rc = memset_s(&drop_arg, sizeof(ObjectAccessDrop), 0, sizeof(ObjectAccessDrop));
        securec_check(rc, "\0", "\0");
        InvokeObjectAccessHook(OAT_DROP, TableSpaceRelationId, tablespaceoid, 0, &drop_arg);
    }

    /*
     * Remove the pg_tablespace tuple (this will roll back if we fail below)
     */
    simple_heap_delete(rel, &tuple->t_self);

    tableam_scan_end(scandesc);

    partrel = heap_open(PartitionRelationId, RowExclusiveLock);
    ScanKeyInit(&scankey[0],
        Anum_pg_partition_parttype,
        BTEqualStrategyNumber,
        F_CHAREQ,
        CharGetDatum(PART_OBJ_TYPE_PARTED_TABLE));

    scan = tableam_scan_begin(partrel, SnapshotNow, 1, scankey);
    while (PointerIsValid(tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection))) {
        Datum spcdatum;
        Datum tspdatum;
        bool isnull = false;
        oidvector* spcvector = NULL;
        int counter = 0;
        Oid tsp = InvalidOid;

        tspdatum = heap_getattr(tuple, Anum_pg_partition_reltablespace, RelationGetDescr(partrel), &isnull);
        Assert(!isnull);
        tsp = DatumGetObjectId(tspdatum);
        if (tsp == tablespaceoid) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("tablespace \"%s\" is used by partitioned table \"%s\"",
                        tablespacename,
                        NameStr(((Form_pg_partition)GETSTRUCT(tuple))->relname))));
        }

        spcdatum = heap_getattr(tuple, Anum_pg_partition_intablespace, RelationGetDescr(partrel), &isnull);
        spcvector = (oidvector*)DatumGetPointer(spcdatum);
        if (!PointerIsValid(spcvector)) {
            Assert(isnull);
            continue;
        }

        for (counter = 0; counter < spcvector->dim1; counter++) {
            if (spcvector->values[counter] == tablespaceoid) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("tablespace \"%s\" is used by partitioned table \"%s\"",
                            tablespacename,
                            NameStr(((Form_pg_partition)GETSTRUCT(tuple))->relname))));
            }
        }
    }
    tableam_scan_end(scan);
    heap_close(partrel, NoLock);

    /*
     * Remove any comments or security labels on this tablespace.
     */
    DeleteSharedComments(tablespaceoid, TableSpaceRelationId);
    DeleteSharedSecurityLabel(tablespaceoid, TableSpaceRelationId);

    /*
     * Remove dependency on owner.
     */
    deleteSharedDependencyRecordsFor(TableSpaceRelationId, tablespaceoid, 0);


    /*
     * Purge the recyclebin relations.
     */
    RbCltPurgeSpace(tablespaceoid);

    /*
     * Acquire TablespaceCreateLock to ensure that no TablespaceCreateDbspace
     * is running concurrently.
     */
    (void)LWLockAcquire(TablespaceCreateLock, LW_EXCLUSIVE);

    /*
     * Try to remove the physical infrastructure.
     */
    if (!destroy_tablespace_directories(tablespaceoid, false)) {
        /*
         * Not all files deleted?  However, there can be lingering empty files
         * in the directories, left behind by for example DROP TABLE, that
         * have been scheduled for deletion at next checkpoint (see comments
         * in mdunlink() for details).	We could just delete them immediately,
         * but we can't tell them apart from important data files that we
         * mustn't delete.  So instead, we force a checkpoint which will clean
         * out any lingering files, and try again.
         */
        RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

        if (!destroy_tablespace_directories(tablespaceoid, false)) {
            /* Still not empty, the files must be important then */
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("tablespace \"%s\" is not empty", tablespacename)));
        }
    }

    /* Record the filesystem change in XLOG */
    {
        xl_tblspc_drop_rec xlrec;

        xlrec.ts_id = tablespaceoid;

        XLogBeginInsert();
        XLogRegisterData((char*)&xlrec, sizeof(xl_tblspc_drop_rec));

        (void)XLogInsert(RM_TBLSPC_ID, XLOG_TBLSPC_DROP);
    }

    /*
     * Note: because we checked that the tablespace was empty, there should be
     * no need to worry about flushing shared buffers or free space map
     * entries for relations in the tablespace.
     */
    /*
     * Force synchronous commit, to minimize the window between removing the
     * files on-disk and marking the transaction committed.  It's not great
     * that there is any window at all, but definitely we don't want to make
     * it larger than necessary.
     */
    ForceSyncCommit();

    /*
     * Allow TablespaceCreateDbspace again.
     */
    LWLockRelease(TablespaceCreateLock);

    /* We keep the lock on pg_tablespace until commit */
    heap_close(rel, NoLock);
#else  /* !HAVE_SYMLINK */
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("tablespaces are not supported on this platform")));
#endif /* HAVE_SYMLINK */
}

/*
 * @Description: check tablespac symlink, if pg_tblspc have no  symlink or symlink link to the same path, return error
 * @IN location: tablespac location
 * @See also:
 */
static void check_tablespace_symlink(const char* location)
{
    const char* tbs_path = "pg_tblspc";
    DIR* dir = NULL;
    struct dirent* dent = NULL;
    char tmppath[MAXPGPATH + 2];
    char linkpath[MAXPGPATH + 2];
    errno_t rc = EOK;

    // We don't do symlink check during recovery
    //
    if (t_thrd.xlog_cxt.InRecovery)
        return;

    Assert(location != NULL);

    dir = AllocateDir(TBLSPCDIR);
    if (dir == NULL) {
        ereport(ERROR,
            (errmodule(MOD_TBLSPC),
                errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("could not open pg_tblspc directory")));
    }
    while ((dent = ReadDir(dir, tbs_path)) != NULL) {
        if (strcmp(dent->d_name, ".") == 0 || strcmp(dent->d_name, "..") == 0)
            continue;

        rc = snprintf_s(tmppath, MAXPGPATH + 2, MAXPGPATH + 1, "%s/%s", TBLSPCDIR, dent->d_name);
        securec_check_ss(rc, "\0", "\0");

        /* get file status */
        struct stat st;
        if (lstat(tmppath, &st) < 0) {
            ereport(ERROR,
                (errmodule(MOD_TBLSPC),
                    errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("could not get \"%s\" status", tmppath)));
        }

        /* only symbolic link  */
        if (!S_ISLNK(st.st_mode)) {
            ereport(ERROR,
                (errmodule(MOD_TBLSPC),
                    errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("\"%s\" is not symlink, please check and clean the remains in \"%s\"", tmppath, TBLSPCDIR)));
        }

        /* get target directory */
        int rllen = readlink(tmppath, linkpath, sizeof(linkpath));
        if (rllen < 0) {
            ereport(ERROR,
                (errmodule(MOD_TBLSPC),
                    errcode_for_file_access(),
                    errmsg("could not read symbolic link \"%s\": %m", tmppath)));
        }
        if (rllen >= MAXPGPATH) {
            ereport(ERROR,
                (errmodule(MOD_TBLSPC),
                    errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("symbolic link \"%s\" target is too long", tmppath)));
        }
        linkpath[rllen] = '\0';
        canonicalize_path(linkpath);

        /* test target directory */
        struct stat linkst;
        if (lstat(linkpath, &linkst) < 0) {
            ereport(ERROR,
                (errmodule(MOD_TBLSPC),
                    errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("target of symbolic link \"%s\" doesn't exist", tmppath)));
        }

        /* do not support symbolic link ->  symbolic link */
        if (!S_ISDIR(linkst.st_mode)) {
            ereport(ERROR,
                (errmodule(MOD_TBLSPC),
                    errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("target of symbolic link \"%s\" isn't directory", tmppath)));
        }

        /* match file name */
        rc = snprintf_s(tmppath, MAXPGPATH + 2, MAXPGPATH + 1, "%s/", location);
        securec_check_ss(rc, "\0", "\0");
        linkpath[rllen] = '/';
        linkpath[rllen + 1] = '\0';
        if (0 == strncmp(tmppath, linkpath, strlen(linkpath)) || 0 == strncmp(tmppath, linkpath, strlen(tmppath))) {
            linkpath[rllen] = '\0';
            ereport(ERROR,
                (errmodule(MOD_TBLSPC),
                    errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("find conflict linkpath in pg_tblspc, try a different path.")));
        }
    }

    (void)FreeDir(dir);

    return;
}

/*
 * create_tablespace_directories
 *
 *	Attempt to create filesystem infrastructure linking $PGDATA/pg_tblspc/
 *	to the specified directory
 */
static void create_tablespace_directories(const char* location, const Oid tablespaceoid)
{
    char* linkloc = (char*)palloc(strlen(TBLSPCDIR) + OIDCHARS + 2);
    char* locationWithTempDir = NULL;
    int locationWithTempDirLen = 0;
#ifdef PGXC
    char* location_with_version_dir = NULL;
    if (ENABLE_DSS) {
        location_with_version_dir = (char *)palloc(strlen(location) + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1);
    } else {
        location_with_version_dir =
            (char*)palloc(strlen(location) + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1 + PGXC_NODENAME_LENGTH + 1);
    }
#else
    char* location_with_version_dir = palloc(strlen(location) + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1);
#endif
    int rc = 0;

    rc = sprintf_s(linkloc, strlen(TBLSPCDIR) + 1 + OIDCHARS + 1, "%s/%u", TBLSPCDIR, tablespaceoid);
    securec_check_ss(rc, "\0", "\0");
#ifdef PGXC
    /*
     * In Postgres-XC a suffix based on node name is added at the end
     * of TABLESPACE_VERSION_DIRECTORY. Node name unicity in Postgres-XC
     * cluster insures unicity of tablespace.
     */
    if (ENABLE_DSS) {
        rc = sprintf_s(location_with_version_dir,
            strlen(location) + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1,
            "%s/%s",
            location,
            TABLESPACE_VERSION_DIRECTORY);
        securec_check_ss(rc, "\0", "\0");
    } else {
        rc = sprintf_s(location_with_version_dir,
            strlen(location) + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1 + PGXC_NODENAME_LENGTH + 1,
            "%s/%s_%s",
            location,
            TABLESPACE_VERSION_DIRECTORY,
            g_instance.attr.attr_common.PGXCNodeName);
        securec_check_ss(rc, "\0", "\0");
    }
#else
    rc = sprintf_s(location_with_version_dir,
        strlen(location) + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1,
        "%s/%s",
        location,
        TABLESPACE_VERSION_DIRECTORY);
    securec_check_ss(rc, "\0", "\0");
#endif

    // We want to create PG_TEMP_FILES_DIR when create tablespace
    //
    locationWithTempDirLen = strlen(location_with_version_dir) + 1 + strlen(PG_TEMP_FILES_DIR) + 1;
    locationWithTempDir = (char*)palloc(locationWithTempDirLen);
    rc = snprintf_s(locationWithTempDir,
        locationWithTempDirLen,
        locationWithTempDirLen - 1,
        "%s/%s",
        location_with_version_dir,
        PG_TEMP_FILES_DIR);
    securec_check_ss(rc, "\0", "\0");

    /*
     * Attempt to coerce target directory to safe permissions.	If this fails,
     * it doesn't exist or has the wrong owner.
     */
    if (chmod(location, S_IRWXU) != 0) {
        if (FILE_POSSIBLY_DELETED(errno))
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FILE),
                    errmsg("directory \"%s\" does not exist", location),
                    t_thrd.xlog_cxt.InRecovery ? errhint("Create this directory for the tablespace before "
                    "restarting the server.") : 0));
        else
            ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not set permissions on directory \"%s\": %m", location)));
    }

    if (t_thrd.xlog_cxt.InRecovery) {
        struct stat st;

        /*
         * Our theory for replaying a CREATE is to forcibly drop the target
         * subdirectory if present, and then recreate it. This may be more
         * work than needed, but it is simple to implement.
         */
        if (stat(location_with_version_dir, &st) == 0 && S_ISDIR(st.st_mode) && !IsRoachRestore()) {
            if (!rmtree(location_with_version_dir, true))
                /* If this failed, mkdir() below is going to error. */
                ereport(WARNING,
                    (errmsg("some useless files may be left behind in old database directory \"%s\"",
                        location_with_version_dir)));
        }
    }

    /*
     * The creation of the version directory prevents more than one tablespace
     * in a single location.
     */
    if (mkdir(location_with_version_dir, S_IRWXU) < 0) {
        if (FILE_ALREADY_EXIST(errno)) {
            if (!IsRoachRestore())
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_IN_USE),
                        errmsg("directory \"%s\" already in use as a tablespace", location_with_version_dir)));
        } else {
            ereport(ERROR,
                (errcode_for_file_access(),
                    errmsg("could not create directory \"%s\": %m", location_with_version_dir)));
        }
    }

    // Create PG_TEMP_FILES_DIR directory
    //
    if (mkdir(locationWithTempDir, S_IRWXU) < 0) {
        if (FILE_ALREADY_EXIST(errno)) {
            if (!IsRoachRestore())
                ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_IN_USE),
                        errmsg("directory \"%s\" already in use as a tablespace", locationWithTempDir)));
        } else {
            ereport(ERROR,
                (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", locationWithTempDir)));
        }
    }

    /* Remove old symlink in recovery, in case it points to the wrong place */
    if (t_thrd.xlog_cxt.InRecovery) {
        struct stat st;

        if (lstat(linkloc, &st) < 0) {
            if (!FILE_POSSIBLY_DELETED(errno))
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", linkloc)));
        } else if (S_ISDIR(st.st_mode)) {
            if (rmdir(linkloc) < 0 && !FILE_POSSIBLY_DELETED(errno))
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not remove directory \"%s\": %m", linkloc)));
        } else if (unlink(linkloc) < 0 && !FILE_POSSIBLY_DELETED(errno)) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not remove symbolic link \"%s\": %m", linkloc)));
        }
    }
    /* do not support symbolic link ->  symbolic link */
    struct stat st;
    if (lstat(location, &st) == 0) {
        if (S_ISLNK(st.st_mode)) {
            ereport(ERROR,
                (errmodule(MOD_TBLSPC),
                    errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("location \"%s\" is symbolic link", location)));
        }
    }
    /*
     * Create the symlink under PGDATA
     */
    if (symlink(location, linkloc) < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create symbolic link \"%s\": %m", linkloc)));

    pfree_ext(linkloc);
    pfree_ext(location_with_version_dir);
    pfree_ext(locationWithTempDir);
}

/*
 * @Description:  canonicalize path which in tablespace options
 * @IN datum: tablespace options
 * @Return: tablespace options
 */
Datum CanonicalizeTablespaceOptions(Datum datum)
{
    List* optionList = NIL;
    ListCell* optionCell = NULL;
    char* optionDefName = NULL;
    char* path = NULL;

    if ((Datum)0 == datum) {
        return (Datum)0;
    }

    // transfer to options list
    optionList = untransformRelOptions(datum);
    foreach (optionCell, optionList) {
        DefElem* optionDef = (DefElem*)lfirst(optionCell);
        optionDefName = optionDef->defname;

        if (0 == pg_strncasecmp(optionDefName, TABLESPACE_OPTION_CFGPATH, strlen(TABLESPACE_OPTION_CFGPATH)) ||
            0 == pg_strncasecmp(optionDefName, TABLESPACE_OPTION_STOREPATH, strlen(TABLESPACE_OPTION_STOREPATH))) {
            // canonicalize path
            path = defGetString(optionDef);
            CANONICALIZE_PATH(path);

            char* defName = pstrdup(optionDefName);
            Node* defVal = (Node*)makeString(path);
            DefElem* newDef = makeDefElem(defName, defVal);

            // update option
            lfirst(optionCell) = newDef;
        }
    }

    // back to datum
    datum = (Datum)optionListToArray(optionList);
    Assert(datum != (Datum)0);
    list_free(optionList);

    return datum;
}

/*
 * Brief        : Whether or not the tablespace is specified tablespace.
 * Input        : spcOid, the tablespace Oid.
 *              : specifedTblspc, the specified tablespace type.
 * Output       : None.
 * Return Value : Return true if the tablepsace is specified tablespace type,
 *                return false otherwise.
 * Notes        : None.
 */
bool IsSpecifiedTblspc(Oid spcOid, const char* specifedTblspc)
{
    bool isSpecified = false;
    char* filesystem = NULL;

    if (InvalidOid == spcOid) {
        /*
         * For example, when default_tablespace value is empty string, spcOid would be an invalidOid.
         */
        return false;
    }
    filesystem = GetTablespaceOptionValue(spcOid, TABLESPACE_OPTION_FILESYSTEM);
    if (filesystem == NULL) {
        if (0 == pg_strncasecmp(specifedTblspc, FILESYSTEM_GENERAL, strlen(specifedTblspc))) {
            isSpecified = true;
        }
    } else if (0 == pg_strncasecmp(filesystem, specifedTblspc, strlen(filesystem))) {
        isSpecified = true;
    }

    return isSpecified;
}

#ifdef PGXC

/*
 * createtbspc_abort_callback: Error cleanup callback for create-tablespace.
 * This function should be executed only on successful creation of tablespace
 * directory structure. This way we are sure that the directory and the symlink
 * that we are removing are created by the same transaction, and are not
 * pre-existing. Otherwise, we might delete any pre-existing directories.
 */
static void createtbspc_abort_callback(bool isCommit, const void* arg)
{
    Oid tablespaceoid = *(Oid*)arg;
    char* linkloc_with_version_dir = NULL;
    char* linkloc = NULL;
    struct stat st;
    errno_t rc = EOK;
    int len = 0;
    if (ENABLE_DSS) {
        len = strlen(TBLSPCDIR) + 1 + OIDCHARS + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1;
    } else {
        len = strlen(TBLSPCDIR) + 1 + OIDCHARS + 1 + strlen(g_instance.attr.attr_common.PGXCNodeName) + 1 +
              strlen(TABLESPACE_VERSION_DIRECTORY) + 1;
    }

    if (isCommit)
        return;

    linkloc_with_version_dir = (char*)palloc(len);
    if (ENABLE_DSS) {
        rc = sprintf_s(linkloc_with_version_dir,
            len,
            "%s/%u/%s",
            TBLSPCDIR,
            tablespaceoid,
            TABLESPACE_VERSION_DIRECTORY);
        securec_check_ss(rc, "\0", "\0");
    } else {
        rc = sprintf_s(linkloc_with_version_dir,
            len,
            "%s/%u/%s_%s",
            TBLSPCDIR,
            tablespaceoid,
            TABLESPACE_VERSION_DIRECTORY,
            g_instance.attr.attr_common.PGXCNodeName);
        securec_check_ss(rc, "\0", "\0");
    }

    /* First, remove version directory */
    if (!rmtree(linkloc_with_version_dir, true)) {
        ereport(WARNING,
            (errcode_for_file_access(), errmsg("could not remove directory \"%s\": %m", linkloc_with_version_dir)));
        pfree_ext(linkloc_with_version_dir);
        return;
    }

    /*
     * Now remove the symlink.
     * This has been borrowed from destroy_tablespace_directories().
     */
    linkloc = pstrdup(linkloc_with_version_dir);
    get_parent_directory(linkloc);
    if (lstat(linkloc, &st) == 0 && S_ISDIR(st.st_mode)) {
        /*
         * We are here possibly because this is Windows, and lstat has identified
         * the junction point as a directory.
         */
        if (rmdir(linkloc) < 0)
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not remove directory \"%s\": %m", linkloc)));
    } else {
        if (unlink(linkloc) < 0)
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not remove symbolic link \"%s\": %m", linkloc)));
    }

    pfree_ext(linkloc_with_version_dir);
    pfree_ext(linkloc);
}
#endif

/*
 * destroy_tablespace_directories
 *
 * Attempt to remove filesystem infrastructure for the tablespace.
 *
 * 'redo' indicates we are redoing a drop from XLOG; in that case we should
 * not throw an ERROR for problems, just LOG them.	The worst consequence of
 * not removing files here would be failure to release some disk space, which
 * does not justify throwing an error that would require manual intervention
 * to get the database running again.
 *
 * Returns TRUE if successful, FALSE if some subdirectory is not empty
 */
static bool destroy_tablespace_directories(Oid tablespaceoid, bool redo, bool is_exrto_read)
{
    char* linkloc = NULL;
    char* linkloc_with_version_dir = NULL;
    DIR* dirdesc = NULL;
    struct dirent* de = NULL;
    char* subfile = NULL;
    struct stat st;
    errno_t rc = EOK;

#ifdef PGXC
    int len = 0;
    if (ENABLE_DSS) {
        len = strlen(TBLSPCDIR) + 1 + OIDCHARS + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1;
        linkloc_with_version_dir = (char*)palloc(len);
        rc = sprintf_s(linkloc_with_version_dir,
            len,
            "%s/%u/%s",
            TBLSPCDIR,
            tablespaceoid,
            TABLESPACE_VERSION_DIRECTORY);
        securec_check_ss(rc, "\0", "\0");
    } else {
        len = strlen(TBLSPCDIR) + 1 + OIDCHARS + 1 + strlen(g_instance.attr.attr_common.PGXCNodeName) + 1 +
                strlen(TABLESPACE_VERSION_DIRECTORY) + 1;
        linkloc_with_version_dir = (char*)palloc(len);
        rc = sprintf_s(linkloc_with_version_dir,
            len,
            "%s/%u/%s_%s",
            TBLSPCDIR,
            tablespaceoid,
            TABLESPACE_VERSION_DIRECTORY,
            g_instance.attr.attr_common.PGXCNodeName);
        securec_check_ss(rc, "\0", "\0");
    }
#else
    int len = strlen(TBLSPCDIR) + 1 + OIDCHARS + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1;
    linkloc_with_version_dir = (char*)palloc(len);
    rc = sprintf_s(linkloc_with_version_dir,
        len,
        "%s/%u/%s",
        TBLSPCDIR,
        tablespaceoid,
        TABLESPACE_VERSION_DIRECTORY);
    securec_check_ss(rc, "\0", "\0");
#endif

    /*
     * Check if the tablespace still contains any files.  We try to rmdir each
     * per-database directory we find in it.  rmdir failure implies there are
     * still files in that subdirectory, so give up.  (We do not have to worry
     * about undoing any already completed rmdirs, since the next attempt to
     * use the tablespace from that database will simply recreate the
     * subdirectory via TablespaceCreateDbspace.)
     *
     * Since we hold TablespaceCreateLock, no one else should be creating any
     * fresh subdirectories in parallel. It is possible that new files are
     * being created within subdirectories, though, so the rmdir call could
     * fail.  Worst consequence is a less friendly error message.
     *
     * If redo is true then ENOENT is a likely outcome here, and we allow it
     * to pass without comment.  In normal operation we still allow it, but
     * with a warning.	This is because even though ProcessUtility disallows
     * DROP TABLESPACE in a transaction block, it's possible that a previous
     * DROP failed and rolled back after removing the tablespace directories
     * and/or symlink.	We want to allow a new DROP attempt to succeed at
     * removing the catalog entries (and symlink if still present), so we
     * should not give a hard error here.
     */
    dirdesc = AllocateDir(linkloc_with_version_dir);
    if (dirdesc == NULL) {
        if (FILE_POSSIBLY_DELETED(errno)) {
            if (!redo)
                ereport(WARNING,
                    (errcode_for_file_access(),
                        errmsg("could not open directory \"%s\": %m", linkloc_with_version_dir)));
            /* The symlink might still exist, so go try to remove it */
            goto remove_symlink;
        } else if (redo) {
            /* in redo, just log other types of error */
            ereport(LOG,
                (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", linkloc_with_version_dir)));
            pfree_ext(linkloc_with_version_dir);
            return false;
        }
        /* else let ReadDir report the error */
    }

    while ((de = ReadDir(dirdesc, linkloc_with_version_dir)) != NULL) {
        SegSpace *spc = NULL;
        len = strlen(linkloc_with_version_dir) + 1 + strlen(de->d_name) + 1;
        rc = EOK;

        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        subfile = (char*)palloc(len);
        rc = sprintf_s(subfile, len, "%s/%s", linkloc_with_version_dir, de->d_name);
        securec_check_ss(rc, "\0", "\0");

        /* remove segment file */
        if (!redo && strcmp(de->d_name, "pgsql_tmp") != 0) {
            Oid dbNode = atoi(de->d_name);
            spc = spc_drop(tablespaceoid, dbNode, redo);
        }

        /* This check is just to deliver a friendlier error message */
        if (!redo && !directory_is_empty(subfile)) {
            FreeDir(dirdesc);
            pfree_ext(subfile);
            pfree_ext(linkloc_with_version_dir);
            return false;
        }
        /* remove empty directory */
        if (spc) {
            spc_lock(spc);
        }
        if (rmdir(subfile) < 0)
            ereport(redo ? LOG : ERROR,
                (errcode_for_file_access(), errmsg("could not remove directory \"%s\": %m", subfile)));
        if (is_exrto_read) {
            rmtree(subfile, true);
        }
        if (spc) {
            spc_unlock(spc);
        }
        pfree_ext(subfile);
    }

    FreeDir(dirdesc);
    /* remove version directory */
    if (rmdir(linkloc_with_version_dir) < 0) {
        ereport(redo ? LOG : ERROR,
            (errcode_for_file_access(), errmsg("could not remove directory \"%s\": %m", linkloc_with_version_dir)));
        pfree_ext(linkloc_with_version_dir);
        return false;
    }

    /*
     * Try to remove the symlink.  We must however deal with the possibility
     * that it's a directory instead of a symlink --- this could happen during
     * WAL replay (see TablespaceCreateDbspace), and it is also the case on
     * Windows where junction points lstat() as directories.
     *
     * Note: in the redo case, we'll return true if this final step fails;
     * there's no point in retrying it.  Also, ENOENT should provoke no more
     * than a warning.
     */
remove_symlink:
    linkloc = pstrdup(linkloc_with_version_dir);
    get_parent_directory(linkloc);
    if (lstat(linkloc, &st) == 0 && S_ISDIR(st.st_mode)) {
        if (rmdir(linkloc) < 0)
            ereport(redo ? LOG : ERROR,
                (errcode_for_file_access(), errmsg("could not remove directory \"%s\": %m", linkloc)));
    } else {
        if (unlink(linkloc) < 0)
            ereport(redo ? LOG : (FILE_POSSIBLY_DELETED(errno) ? WARNING : ERROR),
                (errcode_for_file_access(), errmsg("could not remove symbolic link \"%s\": %m", linkloc)));
    }

    pfree_ext(linkloc_with_version_dir);
    pfree_ext(linkloc);

    /*
     * drop HDFS tablesapce, first drop local path. when exist empty HDFS table,
     * whether can drop HDFS table or not in local.
     */
    return true;
}

/*
 * Check if a directory is empty.
 *
 * This probably belongs somewhere else, but not sure where...
 */
bool directory_is_empty(const char* path)
{
    DIR* dirdesc = NULL;
    struct dirent* de = NULL;

    dirdesc = AllocateDir(path);

    while ((de = ReadDir(dirdesc, path)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;
        FreeDir(dirdesc);
        return false;
    }

    FreeDir(dirdesc);
    return true;
}

/*
 *  remove_tablespace_symlink
 *
 * This function removes symlinks in pg_tblspc.  On Windows, junction points
 * act like directories so we must be able to apply rmdir.  This function
 * works like the symlink removal code in destroy_tablespace_directories,
 * except that failure to remove is always an ERROR.  But if the file doesn't
 * exist at all, that's OK.
 */
void remove_tablespace_symlink(const char* linkloc)
{
    struct stat st;

    if (lstat(linkloc, &st) < 0) {
        if (FILE_POSSIBLY_DELETED(errno))
            return;
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", linkloc)));
    }

    if (S_ISDIR(st.st_mode)) {
        /*
         * This will fail if the directory isn't empty, but not if it's a
         * junction point.
         */
        if (rmdir(linkloc) < 0 && !FILE_POSSIBLY_DELETED(errno))
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not remove directory \"%s\": %m", linkloc)));
    }
#ifdef S_ISLNK
    else if (S_ISLNK(st.st_mode)) {
        if (unlink(linkloc) < 0 && !FILE_POSSIBLY_DELETED(errno))
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not remove symbolic link \"%s\": %m", linkloc)));
    }
#endif
    else {
        /* Refuse to remove anything that's not a directory or symlink */
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("\"%s\" is not a directory or symbolic link", linkloc)));
    }
}

/*
 * Rename a tablespace
 */
ObjectAddress RenameTableSpace(const char* oldname, const char* newname)
{
    Oid      tspId;    
    Relation rel;
    ScanKeyData entry[1];
    TableScanDesc scan;
    HeapTuple tup;
    HeapTuple newtuple;
    Form_pg_tablespace newform;
    ObjectAddress address;

    if (isSecurityMode) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to alter tablespace in security mode")));
    }

    /* Search pg_tablespace */
    rel = heap_open(TableSpaceRelationId, RowExclusiveLock);

    ScanKeyInit(&entry[0], Anum_pg_tablespace_spcname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(oldname));
    scan = tableam_scan_begin(rel, SnapshotNow, 1, entry);
    tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("tablespace \"%s\" does not exist", oldname)));

    tspId = HeapTupleGetOid(tup);
    newtuple = heap_copytuple(tup);
    newform = (Form_pg_tablespace)GETSTRUCT(newtuple);

    tableam_scan_end(scan);

    /* Must be owner or have alter privilege of the target object. */
    AclResult aclresult = pg_tablespace_aclcheck(HeapTupleGetOid(newtuple), GetUserId(), ACL_ALTER);
    if (aclresult != ACLCHECK_OK && !pg_tablespace_ownercheck(HeapTupleGetOid(newtuple), GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_TABLESPACE, oldname);
    }

    /* Validate new name */
    if (!g_instance.attr.attr_common.allowSystemTableMods && !u_sess->attr.attr_common.IsInplaceUpgrade &&
        IsReservedName(newname))
        ereport(ERROR,
            (errcode(ERRCODE_RESERVED_NAME),
                errmsg("unacceptable tablespace name \"%s\"", newname),
                errdetail("The prefix \"pg_\" is reserved for system tablespaces.")));

    /* Make sure the new name doesn't exist */
    ScanKeyInit(&entry[0], Anum_pg_tablespace_spcname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(newname));
    scan = tableam_scan_begin(rel, SnapshotNow, 1, entry);
    tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    if (HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("tablespace \"%s\" already exists", newname)));

    tableam_scan_end(scan);

    /* OK, update the entry */
    (void)namestrcpy(&(newform->spcname), newname);

    simple_heap_update(rel, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(rel, newtuple);
    
    ObjectAddressSet(address, TableSpaceRelationId, tspId);
    heap_close(rel, NoLock);
    return address;
}

/*
 * Change tablespace owner
 */
ObjectAddress AlterTableSpaceOwner(const char* name, Oid newOwnerId)
{
    Relation rel;
    ScanKeyData entry[1];
    TableScanDesc scandesc;
    Form_pg_tablespace spcForm;
    HeapTuple tup;
    Oid tsId;
    ObjectAddress address;

    if (isSecurityMode) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to alter tablespace in security mode")));
    }

    /* Search pg_tablespace */
    rel = heap_open(TableSpaceRelationId, RowExclusiveLock);

    ScanKeyInit(&entry[0], Anum_pg_tablespace_spcname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(name));
    scandesc = tableam_scan_begin(rel, SnapshotNow, 1, entry);
    tup = (HeapTuple) tableam_scan_getnexttuple(scandesc, ForwardScanDirection);
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("tablespace \"%s\" does not exist", name)));

    tsId = HeapTupleGetOid(tup);
    spcForm = (Form_pg_tablespace)GETSTRUCT(tup);
    /*
     * If the new owner is the same as the existing owner, consider the
     * command to have succeeded.  This is for dump restoration purposes.
     */
    if (spcForm->spcowner != newOwnerId) {
        Datum repl_val[Natts_pg_tablespace];
        bool repl_null[Natts_pg_tablespace];
        bool repl_repl[Natts_pg_tablespace];
        Acl* newAcl = NULL;
        Datum aclDatum;
        bool isNull = false;
        HeapTuple newtuple;
        errno_t rc = EOK;

        /* Otherwise, must be owner of the existing object */
        if (!pg_tablespace_ownercheck(HeapTupleGetOid(tup), GetUserId()))
            aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_TABLESPACE, name);

        /* Must be able to become new owner */
        check_is_member_of_role(GetUserId(), newOwnerId);

        /*
         * Normally we would also check for create permissions here, but there
         * are none for tablespaces so we follow what rename tablespace does
         * and omit the create permissions check.
         *
         * NOTE: Only superusers may create tablespaces to begin with and so
         * initially only a superuser would be able to change its ownership
         * anyway.
         */
        rc = memset_s(repl_null, sizeof(repl_null), 0, sizeof(repl_null));
        securec_check(rc, "\0", "\0");
        rc = memset_s(repl_repl, sizeof(repl_repl), 0, sizeof(repl_repl));
        securec_check(rc, "\0", "\0");

        repl_repl[Anum_pg_tablespace_spcowner - 1] = true;
        repl_val[Anum_pg_tablespace_spcowner - 1] = ObjectIdGetDatum(newOwnerId);

        /*
         * Determine the modified ACL for the new owner.  This is only
         * necessary when the ACL is non-null.
         */
        aclDatum = heap_getattr(tup, Anum_pg_tablespace_spcacl, RelationGetDescr(rel), &isNull);
        if (!isNull) {
            newAcl = aclnewowner(DatumGetAclP(aclDatum), spcForm->spcowner, newOwnerId);
            repl_repl[Anum_pg_tablespace_spcacl - 1] = true;
            repl_val[Anum_pg_tablespace_spcacl - 1] = PointerGetDatum(newAcl);
        }

        newtuple = (HeapTuple) tableam_tops_modify_tuple(tup, RelationGetDescr(rel), repl_val, repl_null, repl_repl);

        simple_heap_update(rel, &newtuple->t_self, newtuple);
        CatalogUpdateIndexes(rel, newtuple);

        heap_freetuple(newtuple);

        /* Update owner dependency reference */
        changeDependencyOnOwner(TableSpaceRelationId, HeapTupleGetOid(tup), newOwnerId);
    }

    ObjectAddressSet(address, TableSpaceRelationId, tsId);
    tableam_scan_end(scandesc);
    heap_close(rel, NoLock);
    return address;
}

/*
 * Alter table space options
 */
Oid AlterTableSpaceOptions(AlterTableSpaceOptionsStmt* stmt)
{
    Relation rel;
    ScanKeyData entry[1];
    TableScanDesc scandesc;
    HeapTuple tup;
    Datum datum;
    Datum newOptions;
    Datum repl_val[Natts_pg_tablespace];
    bool isnull = false;
    bool repl_null[Natts_pg_tablespace];
    bool repl_repl[Natts_pg_tablespace];
    HeapTuple newtuple;
    char* maxsize = NULL;
    bool unlimited = false;
    Oid spc_oid = InvalidOid;

    if (isSecurityMode) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to alter tablespace in security mode")));
    }

    /* Search pg_tablespace */
    rel = heap_open(TableSpaceRelationId, RowExclusiveLock);

    ScanKeyInit(
        &entry[0], Anum_pg_tablespace_spcname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(stmt->tablespacename));
    scandesc = tableam_scan_begin(rel, SnapshotNow, 1, entry);
    tup = (HeapTuple) tableam_scan_getnexttuple(scandesc, ForwardScanDirection);
    if (!HeapTupleIsValid(tup))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("tablespace \"%s\" does not exist", stmt->tablespacename)));

    spc_oid = HeapTupleGetOid(tup);
    /*
     * It is unsupported to alter tablespace option for HDFS tablespace except
     * seq_page_cost and random_page_cost options.
     */
    if (IsSpecifiedTblspc(spc_oid, FILESYSTEM_HDFS) && stmt->options != NULL) {
        ListCell* optionCell = NULL;
        foreach (optionCell, stmt->options) {
            DefElem* optionDef = (DefElem*)lfirst(optionCell);
            char* optionDefName = optionDef->defname;

            if (0 != pg_strcasecmp(optionDefName, TABLESPACE_OPTION_SEQ_PAGE_COST) &&
                0 != pg_strcasecmp(optionDefName, TABLESPACE_OPTION_RANDOM_PAGE_COST)) {
                tableam_scan_end(scandesc);
                heap_close(rel, NoLock);
                ereport(ERROR,
                    (errmodule(MOD_TBLSPC),
                        errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg(
                            "It is unsupported to alter tablespace option \"%s\" for DFS tablespace.", optionDefName)));
            }
        }
    }

    if (IsSpecifiedTblspc(spc_oid, FILESYSTEM_GENERAL) && stmt->options != NULL) {
        ListCell* optionCell = NULL;
        foreach (optionCell, stmt->options) {
            DefElem* optionDef = (DefElem*)lfirst(optionCell);
            char* optionDefName = optionDef->defname;

            if (pg_strcasecmp(optionDefName, TABLESPACE_OPTION_FILESYSTEM) == 0) {
                if (stmt->isReset) {
                    tableam_scan_end(scandesc);
                    heap_close(rel, NoLock);
                    ereport(ERROR,
                        (errmodule(MOD_TBLSPC),
                            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("It is unsupported to reset \"filesystem\" option.")));
                } else {
                    if (optionDef->arg != NULL && pg_strcasecmp(defGetString(optionDef), FILESYSTEM_HDFS) == 0) {
                        tableam_scan_end(scandesc);
                        heap_close(rel, NoLock);
                        ereport(ERROR,
                            (errmodule(MOD_TBLSPC),
                                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("It is unsupported to alter general tablespace to hdfs tablespace.")));
                    }
                }
            }
        }
    }

    /* Must be owner or have alter privilege of the existing object */
    AclResult aclresult = pg_tablespace_aclcheck(spc_oid, GetUserId(), ACL_ALTER);
    if (aclresult != ACLCHECK_OK && !pg_tablespace_ownercheck(spc_oid, GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_TABLESPACE, stmt->tablespacename);
    }

    /* Build new tuple. */
    errno_t rc = EOK;

    /* Zero out possible results from swapped_relation_files */
    rc = memset_s(repl_null, sizeof(repl_null), false, sizeof(repl_null));
    securec_check(rc, "\0", "\0");
    rc = memset_s(repl_repl, sizeof(repl_repl), false, sizeof(repl_repl));
    securec_check(rc, "\0", "\0");

    if (stmt->maxsize) {
        if (IsReservedName(stmt->tablespacename)) {
            ereport(ERROR,
                (errcode(ERRCODE_RESERVED_NAME), errmsg("unchangeable tablespace \"%s\"", stmt->tablespacename)));
        }

        (void)parseTableSpaceMaxSize(stmt->maxsize, &unlimited, &maxsize);

        if (unlimited) {
            repl_null[Anum_pg_tablespace_maxsize - 1] = true;
        } else {
            repl_val[Anum_pg_tablespace_maxsize - 1] = DirectFunctionCall1(textin, CStringGetDatum(maxsize));
        }

        repl_repl[Anum_pg_tablespace_maxsize - 1] = true;
    } else {
        /* Generate new proposed spcoptions (text array) */
        datum = heap_getattr(tup, Anum_pg_tablespace_spcoptions, RelationGetDescr(rel), &isnull);
        newOptions = transformRelOptions(isnull ? (Datum)0 : datum, stmt->options, NULL, NULL, false, stmt->isReset);
        (void)tablespace_reloptions(newOptions, true);

        if (newOptions != (Datum)0)
            repl_val[Anum_pg_tablespace_spcoptions - 1] = newOptions;
        else
            repl_null[Anum_pg_tablespace_spcoptions - 1] = true;
        repl_repl[Anum_pg_tablespace_spcoptions - 1] = true;
    }

    newtuple = (HeapTuple) tableam_tops_modify_tuple(tup, RelationGetDescr(rel), repl_val, repl_null, repl_repl);

    /* Update system catalog. */
    simple_heap_update(rel, &newtuple->t_self, newtuple);
    CatalogUpdateIndexes(rel, newtuple);
    heap_freetuple(newtuple);

    /* Conclude heap scan. */
    tableam_scan_end(scandesc);
    heap_close(rel, NoLock);

    if (NULL != maxsize)
        pfree_ext(maxsize);

    return spc_oid;
}

/*
 * Routines for handling the GUC variable 'default_tablespace'.
 */
/* check_hook: validate new default_tablespace */
bool check_default_tablespace(char** newval, void** extra, GucSource source)
{
    /*
     * If we aren't inside a transaction, we cannot do database access so
     * cannot verify the name.	Must accept the value on faith.
     */
    if (IsTransactionState()) {
        if ((!ENABLE_STATELESS_REUSE) && **newval != '\0' && !OidIsValid(get_tablespace_oid(*newval, true))) {
            /*
             * When source == PGC_S_TEST, we are checking the argument of an
             * ALTER DATABASE SET or ALTER USER SET command.  pg_dumpall dumps
             * all roles before tablespaces, so if we're restoring a
             * pg_dumpall script the tablespace might not yet exist, but will
             * be created later.  Because of that, issue a NOTICE if source ==
             * PGC_S_TEST, but accept the value anyway.
             */
            if (source == PGC_S_TEST) {
                ereport(
                    NOTICE, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("tablespace \"%s\" does not exist", *newval)));
            } else {
                GUC_check_errdetail("Tablespace \"%s\" does not exist.", *newval);
                return false;
            }
        }
    }

    return true;
}

/*
 * GetDefaultTablespace -- get the OID of the current default tablespace
 *
 * Temporary objects have different default tablespaces, hence the
 * relpersistence parameter must be specified.
 *
 * May return InvalidOid to indicate "use the database's default tablespace".
 *
 * Note that caller is expected to check appropriate permissions for any
 * result other than InvalidOid.
 *
 * This exists to hide (and possibly optimize the use of) the
 * default_tablespace GUC variable.
 */
Oid GetDefaultTablespace(char relpersistence)
{
    Oid result;

    /* The temp-table case is handled elsewhere */
    if (relpersistence == RELPERSISTENCE_TEMP) {
        PrepareTempTablespaces();
        return GetNextTempTableSpace();
    }

    /* Fast path for u_sess->attr.attr_storage.default_tablespace == "" */
    if (u_sess->attr.attr_storage.default_tablespace == NULL || u_sess->attr.attr_storage.default_tablespace[0] == '\0')
        return InvalidOid;

    /*
     * It is tempting to cache this lookup for more speed, but then we would
     * fail to detect the case where the tablespace was dropped since the GUC
     * variable was set.  Note also that we don't complain if the value fails
     * to refer to an existing tablespace; we just silently return InvalidOid,
     * causing the new object to be created in the database's tablespace.
     */
    result = get_tablespace_oid(u_sess->attr.attr_storage.default_tablespace, true);

    /*
     * Allow explicit specification of database's default tablespace in
     * u_sess->attr.attr_storage.default_tablespace without triggering permissions checks.
     */
    return ConvertToPgclassRelTablespaceOid(result);
}

/*
 * Brief        : Get the Specified optioin value.
 * Input        : spcNode, tablespace oid.
 *                optionName, specified option name.
 * Output       : None.
 * Return Value : Return the Specified optioin value.
 * Notes        : None.
 */
char* GetTablespaceOptionValue(Oid spcNode, const char* optionName)
{
    List* optionList = NIL;
    ListCell* optionCell = NULL;
    char* optionValue = NULL;
    Assert(optionName != NULL);
    if (!OidIsValid(spcNode)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FILE), errmsg("Tablespace \"%u\" does not exist.", spcNode)));
    }

    optionList = GetTablespaceOptionValues(spcNode);
    foreach (optionCell, optionList) {
        DefElem* optionDef = (DefElem*)lfirst(optionCell);
        char* optionDefName = optionDef->defname;

        if (0 == pg_strncasecmp(optionDefName, optionName, strlen(optionName))) {
            optionValue = defGetString(optionDef);
            break;
        }
    }
    list_free(optionList);

    return optionValue;
}

/*
 * Brief        : Get all values of specified tablespace options.
 * Input        : spcNode, tableapce oid.
 * Output       : None.
 * Return Value : Return all value List of specified tablespace options.
 * Notes        : None.
 */
List* GetTablespaceOptionValues(Oid spcNode)
{
    HeapTuple tp;
    Datum datum;
    bool isnull = false;
    List* options = NIL;

    tp = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(spcNode));
    if (!HeapTupleIsValid(tp)) {
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for tablespace %u.", spcNode)));
    }

    /*
     * Extract the tablespace options.
     */
    datum = SysCacheGetAttr(TABLESPACEOID, tp, Anum_pg_tablespace_spcoptions, &isnull);

    if (isnull) {
        options = NIL;
    } else {
        options = untransformRelOptions(datum);
    }

    ReleaseSysCache(tp);

    return options;
}

/*
 * Routines for handling the GUC variable 'temp_tablespaces'.
 */
typedef struct {
    int numSpcs;
    Oid tblSpcs[1]; /* VARIABLE LENGTH ARRAY */
} temp_tablespaces_extra;

/* check_hook: validate new temp_tablespaces */
bool check_temp_tablespaces(char** newval, void** extra, GucSource source)
{
    char* rawname = NULL;
    List* namelist = NULL;

    /* Need a modifiable copy of string */
    rawname = pstrdup(*newval);
    /* Parse string into list of identifiers */
    if (!SplitIdentifierString(rawname, ',', &namelist)) {
        /* syntax error in name list */
        GUC_check_errdetail("List syntax is invalid.");
        pfree_ext(rawname);
        list_free(namelist);
        return false;
    }

    /*
     * If we aren't inside a transaction, we cannot do database access so
     * cannot verify the individual names.	Must accept the list on faith.
     * Fortunately, there's then also no need to pass the data to fd.c.
     */
    if (IsTransactionState()) {
        temp_tablespaces_extra* myextra = NULL;
        Oid* tblSpcs = NULL;
        int numSpcs;
        ListCell* l = NULL;
        errno_t rc = 0;

        /* temporary workspace until we are done verifying the list */
        tblSpcs = (Oid*)palloc(list_length(namelist) * sizeof(Oid));
        numSpcs = 0;
        foreach (l, namelist) {
            char* curname = (char*)lfirst(l);
            Oid curoid;
            AclResult aclresult;

            /* Allow an empty string (signifying database default) */
            if (curname[0] == '\0') {
                tblSpcs[numSpcs++] = InvalidOid;
                continue;
            }

            /*
             * In an interactive SET command, we ereport for bad info.	When
             * source == PGC_S_TEST, we are checking the argument of an ALTER
             * DATABASE SET or ALTER USER SET command.	pg_dumpall dumps all
             * roles before tablespaces, so if we're restoring a pg_dumpall
             * script the tablespace might not yet exist, but will be created
             * later.  Because of that, issue a NOTICE if source ==
             * PGC_S_TEST, but accept the value anyway.  Otherwise, silently
             * ignore any bad list elements.
             */
            curoid = get_tablespace_oid(curname, source <= PGC_S_TEST);
            if (curoid == InvalidOid) {
                if (source == PGC_S_TEST)
                    ereport(NOTICE,
                        (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("tablespace \"%s\" does not exist", curname)));
                continue;
            }

            /*
             * Allow explicit specification of database's default tablespace
             * in temp_tablespaces without triggering permissions checks.
             */
            if (curoid == u_sess->proc_cxt.MyDatabaseTableSpace) {
                tblSpcs[numSpcs++] = InvalidOid;
                continue;
            }

            /* Check permissions, similarly complaining only if interactive */
            aclresult = pg_tablespace_aclcheck(curoid, GetUserId(), ACL_CREATE);
            if (aclresult != ACLCHECK_OK) {
                if (source >= PGC_S_INTERACTIVE)
                    aclcheck_error(aclresult, ACL_KIND_TABLESPACE, curname);
                continue;
            }

            tblSpcs[numSpcs++] = curoid;
        }

        /* Now prepare an "extra" struct for assign_temp_tablespaces */
        myextra =
            (temp_tablespaces_extra*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER),
                (size_t)(offsetof(temp_tablespaces_extra, tblSpcs) + numSpcs * sizeof(Oid)));
        if (myextra == NULL)
            return false;
        myextra->numSpcs = numSpcs;
        if (numSpcs != 0) {
            rc = memcpy_s(myextra->tblSpcs, numSpcs * sizeof(Oid), tblSpcs, numSpcs * sizeof(Oid));
            securec_check(rc, "\0", "\0");
        }
        *extra = (void*)myextra;

        pfree_ext(tblSpcs);
    }

    pfree_ext(rawname);
    list_free(namelist);

    return true;
}

/* assign_hook: do extra actions as needed */
void assign_temp_tablespaces(const char* newval, void* extra)
{
    temp_tablespaces_extra* myextra = (temp_tablespaces_extra*)extra;

    /*
     * If check_temp_tablespaces was executed inside a transaction, then pass
     * the list it made to fd.c.  Otherwise, clear fd.c's list; we must be
     * still outside a transaction, or else restoring during transaction exit,
     * and in either case we can just let the next PrepareTempTablespaces call
     * make things sane.
     */
    if (myextra != NULL)
        SetTempTablespaces(myextra->tblSpcs, myextra->numSpcs);
    else
        SetTempTablespaces(NULL, 0);
}

/*
 * PrepareTempTablespaces -- prepare to use temp tablespaces
 *
 * If we have not already done so in the current transaction, parse the
 * temp_tablespaces GUC variable and tell fd.c which tablespace(s) to use
 * for temp files.
 */
void PrepareTempTablespaces(void)
{
    char* rawname = NULL;
    List* namelist = NIL;
    Oid* tblSpcs = NULL;
    int numSpcs;
    ListCell* l = NULL;

    /* No work if already done in current transaction */
    if (TempTablespacesAreSet())
        return;

    /*
     * Can't do catalog access unless within a transaction.  This is just a
     * safety check in case this function is called by low-level code that
     * could conceivably execute outside a transaction.  Note that in such a
     * scenario, fd.c will fall back to using the current database's default
     * tablespace, which should always be OK.
     */
    if (!IsTransactionState())
        return;

    /* Need a modifiable copy of string */
    rawname = pstrdup(u_sess->attr.attr_storage.temp_tablespaces);
    /* Parse string into list of identifiers */
    if (!SplitIdentifierString(rawname, ',', &namelist)) {
        /* syntax error in name list */
        SetTempTablespaces(NULL, 0);
        pfree_ext(rawname);
        list_free(namelist);
        return;
    }

    /* Store tablespace OIDs in an array in u_sess->top_transaction_mem_cxt */
    tblSpcs = (Oid*)MemoryContextAlloc(u_sess->top_transaction_mem_cxt, list_length(namelist) * sizeof(Oid));
    numSpcs = 0;
    foreach (l, namelist) {
        char* curname = (char*)lfirst(l);
        Oid curoid;
        AclResult aclresult;

        /* Allow an empty string (signifying database default) */
        if (curname[0] == '\0') {
            tblSpcs[numSpcs++] = InvalidOid;
            continue;
        }

        /* Else verify that name is a valid tablespace name */
        curoid = get_tablespace_oid(curname, true);
        if (curoid == InvalidOid) {
            /* Skip any bad list elements */
            continue;
        }

        /*
         * Allow explicit specification of database's default tablespace in
         * temp_tablespaces without triggering permissions checks.
         */
        if (curoid == u_sess->proc_cxt.MyDatabaseTableSpace) {
            tblSpcs[numSpcs++] = InvalidOid;
            continue;
        }

        /* Check permissions similarly */
        aclresult = pg_tablespace_aclcheck(curoid, GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            continue;

        tblSpcs[numSpcs++] = curoid;
    }

    SetTempTablespaces(tblSpcs, numSpcs);

    pfree_ext(rawname);
    list_free(namelist);
}

/*
 * get_tablespace_oid - given a tablespace name, look up the OID
 *
 * If missing_ok is false, throw an error if tablespace name not found.  If
 * true, just return InvalidOid.
 */
Oid get_tablespace_oid(const char* tablespacename, bool missing_ok)
{
    Oid result;
    Relation rel;
    TableScanDesc scandesc;
    HeapTuple tuple;
    ScanKeyData entry[1];

    /*
     * Search pg_tablespace.  We use a heapscan here even though there is an
     * index on name, on the theory that pg_tablespace will usually have just
     * a few entries and so an indexed lookup is a waste of effort.
     */
    rel = heap_open(TableSpaceRelationId, AccessShareLock);

    ScanKeyInit(
        &entry[0], Anum_pg_tablespace_spcname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(tablespacename));
    scandesc = tableam_scan_begin(rel, SnapshotNow, 1, entry);
    tuple = (HeapTuple) tableam_scan_getnexttuple(scandesc, ForwardScanDirection);
    /* We assume that there can be at most one matching tuple */
    if (HeapTupleIsValid(tuple))
        result = HeapTupleGetOid(tuple);
    else
        result = InvalidOid;

    tableam_scan_end(scandesc);
    heap_close(rel, AccessShareLock);

    if (!OidIsValid(result) && !missing_ok)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("tablespace \"%s\" does not exist", tablespacename)));

    return result;
}

Datum tablespace_oid_name(PG_FUNCTION_ARGS)
{
    Oid tspaceoid;
    char* tsname = NULL;

    tspaceoid = PG_GETARG_OID(0);
    tsname = get_tablespace_name(tspaceoid);
    if (tsname == NULL)  // invalid tablespace oid
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache look up failed for tablespace %u", tspaceoid)));

    PG_RETURN_NAME(tsname);
}

/*
 * get_tablespace_name - given a tablespace OID, look up the name
 *
 * Returns a palloc'd string, or NULL if no such tablespace.
 */
char* get_tablespace_name(Oid spc_oid)
{
    char* result = NULL;
    Relation rel;
    TableScanDesc scandesc;
    HeapTuple tuple;
    ScanKeyData entry[1];

    /*
     * Search pg_tablespace.  We use a heapscan here even though there is an
     * index on oid, on the theory that pg_tablespace will usually have just a
     * few entries and so an indexed lookup is a waste of effort.
     */
    rel = heap_open(TableSpaceRelationId, AccessShareLock);

    ScanKeyInit(&entry[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(spc_oid));
    scandesc = tableam_scan_begin(rel, SnapshotNow, 1, entry);
    tuple = (HeapTuple) tableam_scan_getnexttuple(scandesc, ForwardScanDirection);
    /* We assume that there can be at most one matching tuple */
    if (HeapTupleIsValid(tuple))
        result = pstrdup(NameStr(((Form_pg_tablespace)GETSTRUCT(tuple))->spcname));
    else
        result = NULL;

    tableam_scan_end(scandesc);
    heap_close(rel, AccessShareLock);

    return result;
}

bool IsPathContainsSymlink(char* path)
{
    struct stat statbuf;
    errno_t rc;
    char* ptr = path;

    if (*ptr == '/') {
        ++ptr;
    }

    for (bool isLast = false; !isLast; ++ptr) {
        if (*ptr == '\0') {
            isLast = true;
        } else if (*ptr != '/') {
            continue;
        }

        if (!isLast && ptr[1] == '\0') {
            isLast = true;
        }

        *ptr = '\0';
        rc = memset_s(&statbuf, sizeof(statbuf), 0, sizeof(statbuf));
        securec_check(rc, "\0", "\0");

        if (lstat(path, &statbuf) == 0 && S_ISLNK(statbuf.st_mode)) {
            if (!isLast) {
                *ptr = '/';
            }
            return true;
        }

        if (!isLast) {
            *ptr = '/';
        }
    }
    
    return false;
}

/* check if the dir(location) is exist, if not create it */
void check_create_dir(char* location)
{
    int ret;
    int retryCount = 0;
    bool hasLink = IsPathContainsSymlink(location);

recheck:
    if (hasLink) {
        ++retryCount;
    }
    /* We believe that the location we got from the record is credible. */
    switch (ret = pg_check_dir(location)) {
        case 0: {
            char* tmplocation = pstrdup(location);
            /* Not exist, create */
            if (pg_mkdir_p_used_by_gaussdb(tmplocation, S_IRWXU) == -1) {
                if (errno == EEXIST) {
                    pfree_ext(tmplocation);

                    if (hasLink && retryCount > CHECK_PATH_RETRY_COUNT) {
                        ereport(ERROR, (errmodule(MOD_TBLSPC),
                            errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                            errmsg("recheck location \"%s\" exeed max times.", location),
                            errdetail("the location contains symbolic link, the linked path likely has been deleted.")));
                    }

                    goto recheck;
                } else
                    ereport(ERROR,
                        (errcode_for_file_access(),
                            errmsg("could not create tablespace directory \"%s\": %m", location)));
            }
            pfree_ext(tmplocation);
            break;
        }
        case 1:
        case 2:
            /* Exist, use directly */
            break;
        default:
            /* Trouble accessing directory */
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not access directory \"%s\": %m", location)));
    }

    check_tablespace_symlink(location);
}

void xlog_create_tblspc(Oid tsId, char* tsPath, bool isRelativePath)
{
    int len;
    char* location = tsPath;
    errno_t rc = EOK;

    if (isRelativePath) {
        if (ENABLE_DSS) {
            len = (int)strlen(PG_LOCATION_DIR) + 1 + (int)strlen(tsPath) + 1;
            location = (char*)palloc(len);
            rc = snprintf_s(location, len, len - 1, "%s/%s", PG_LOCATION_DIR, tsPath);
        } else {
            if (t_thrd.proc_cxt.DataDir[strlen(t_thrd.proc_cxt.DataDir) - 1] == '/') {
                len = strlen(t_thrd.proc_cxt.DataDir) + strlen(PG_LOCATION_DIR) + 1 + strlen(tsPath) + 1;
                location = (char*)palloc(len);
                rc = snprintf_s(
                    location, len, len - 1, "%s%s/%s", t_thrd.proc_cxt.DataDir, PG_LOCATION_DIR, tsPath);
            } else {
                len = strlen(t_thrd.proc_cxt.DataDir) + 1 + strlen(PG_LOCATION_DIR) + 1 + strlen(tsPath) + 1;
                location = (char*)palloc(len);
                rc = snprintf_s(
                    location, len, len - 1, "%s/%s/%s", t_thrd.proc_cxt.DataDir, PG_LOCATION_DIR, tsPath);
            }
        }
        securec_check_ss(rc, "\0", "\0");
    }
    check_create_dir(location);
    create_tablespace_directories(location, tsId);
    if (isRelativePath) {
        pfree_ext(location);
    }
}

void xlog_drop_tblspc(Oid tsId)
{
     /*
     * If we issued a WAL record for a drop tablespace it implies that
     * there were no files in it at all when the DROP was done. That means
     * that no permanent objects can exist in it at this point.
     *
     * It is possible for standby users to be using this tablespace as a
     * location for their temporary files, so if we fail to remove all
     * files then do conflict processing and try again, if currently
     * enabled.
     *
     * Other possible reasons for failure include bollixed file
     * permissions on a standby server when they were okay on the primary,
     * etc etc. There's not much we can do about that, so just remove what
     * we can and press on.
     */

    if (!destroy_tablespace_directories(tsId, true)) {
        ResolveRecoveryConflictWithTablespace(tsId);
        if (IS_EXRTO_READ) {
            delete_by_table_space(tsId);
        }
        /*
         * If we did recovery processing then hopefully the backends who
         * wrote temp files should have cleaned up and exited by now.  So
         * retry before complaining.  If we fail again, this is just a LOG
         * condition, because it's not worth throwing an ERROR for (as
         * that would crash the database and require manual intervention
         * before we could get past this WAL record on restart).
         */
        if (!destroy_tablespace_directories(tsId, true, true))
            ereport(LOG,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("directories for tablespace %u could not be removed", tsId),
                    errhint("You can remove the directories manually if necessary.")));
    }
}

/*
 * TABLESPACE resource manager's routines
 */
void tblspc_redo(XLogReaderState* record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    /* Backup blocks are not used in tblspc records */
    Assert(!XLogRecHasAnyBlockRefs(record));

    if (info == XLOG_TBLSPC_CREATE) {
        xl_tblspc_create_rec* xlrec = (xl_tblspc_create_rec*)XLogRecGetData(record);

        xlog_create_tblspc(xlrec->ts_id, xlrec->ts_path, false);
    } else if (info == XLOG_TBLSPC_RELATIVE_CREATE) {
        xl_tblspc_create_rec* xlrec = (xl_tblspc_create_rec*)XLogRecGetData(record);

        /* We need reform location for relative mode */
        xlog_create_tblspc(xlrec->ts_id, xlrec->ts_path, true);
    } else if (info == XLOG_TBLSPC_DROP) {
        xl_tblspc_drop_rec* xlrec = (xl_tblspc_drop_rec*)XLogRecGetData(record);
        xlog_drop_tblspc(xlrec->ts_id);
    } else {
        ereport(PANIC, 
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("tblspc_redo: unknown op code %u", (uint)info)));
    }

    t_thrd.xlog_cxt.needImmediateCkp = true;
}

int TableSpaceUsageManager::ShmemSize()
{
    return sizeof(TableSpaceUsageStruct);
}

void TableSpaceUsageManager::Init()
{
    bool found = false;
    Size bucketSize = sizeof(TableSpaceUsageBucket);
    TableSpaceUsageBucket* bucket = NULL;

    u_sess->cmd_cxt.TableSpaceUsageArray = (TableSpaceUsageStruct*)ShmemInitStruct(
        "TableSpace Usage Information Array", TableSpaceUsageManager::ShmemSize(), &found);
    u_sess->cmd_cxt.l_tableSpaceOid = InvalidOid;
    u_sess->cmd_cxt.l_maxSize = 0;
    u_sess->cmd_cxt.l_isLimit = false;

    if (!found) {
        for (uint32 counter = 0; counter < TABLESPACE_USAGE_SLOT_NUM; counter++) {
            bucket = &u_sess->cmd_cxt.TableSpaceUsageArray->m_tab[counter];
            errno_t rc = EOK;
            rc = memset_s(bucket, bucketSize, 0, bucketSize);
            securec_check(rc, "\0", "\0");
            SpinLockInit(&bucket->mutex);
        }
    }
}

bool TableSpaceUsageManager::IsLimited(Oid tableSpaceOid, uint64* maxSize)
{
    Relation relation = heap_open(TableSpaceRelationId, AccessShareLock);
    TableScanDesc scandesc;
    HeapTuple tuple;
    ScanKeyData entry[1];
    Datum datum;
    bool isNull = false;
    bool isLimited = false;
    int getCount = 0;
    const int retryTimes = 3;

    Assert(PointerIsValid(maxSize));
    *maxSize = 0;

    ScanKeyInit(&entry[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(tableSpaceOid));
    scandesc = tableam_scan_begin(relation, SnapshotNow, 1, entry);
    /*
     * Note: when we fall off the end of the scan in either direction, we reset rs_inited.
     * So we can restart the scan in heap scan.
     */
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scandesc, ForwardScanDirection)) == NULL) {
        getCount++;
        if (getCount >= retryTimes) {
            ereport(ERROR,
                (errcode(ERRCODE_NO_DATA_FOUND),
                    errmsg("Can not get tablespace size with SnapshotNow after try 3 times."),
                    errhint("Please retry.")));
        }
    }
    Assert(HeapTupleIsValid(tuple));
    datum = heap_getattr(tuple, Anum_pg_tablespace_maxsize, RelationGetDescr(relation), &isNull);
    if (!isNull) {
        char* maxSizeString = DatumGetCString(DirectFunctionCall1(textout, datum));
        *maxSize = parseTableSpaceMaxSize(maxSizeString, NULL, NULL);
        isLimited = true;

        pfree_ext(maxSizeString);
    }

    tableam_scan_end(scandesc);
    heap_close(relation, AccessShareLock);

    return isLimited;
}

inline int TableSpaceUsageManager::GetBucketIndex(Oid tableSpaceOid)
{
    Assert(0 == (TABLESPACE_USAGE_SLOT_NUM & (TABLESPACE_USAGE_SLOT_NUM - 1)));

    return (tableSpaceOid & (TABLESPACE_USAGE_SLOT_NUM - 1));
}

inline void TableSpaceUsageManager::ResetUsageSlot(TableSpaceUsageSlot* info)
{
    errno_t rc = EOK;
    rc = memset_s(info, sizeof(TableSpaceUsageSlot), 0, sizeof(TableSpaceUsageSlot));
    securec_check(rc, "\0", "\0");
}

/*
 * reset the slot in special bucket if the slot is not locked
 * 1. this function is called when there is no usable slot in the special bucket
 * 2. the bucket must have been locked
 */
inline void TableSpaceUsageManager::ResetBucket(TableSpaceUsageBucket* bucket)
{
    for (int counter = 0; counter < TABLESPACE_BUCKET_CONFLICT_LISTLEN; counter++) {
        TableSpaceUsageManager::ResetUsageSlot(&bucket->spcUsage[counter]);
    }

    bucket->count = 0;
}

inline bool TableSpaceUsageManager::WithinLimit(TableSpaceUsageSlot* slot, uint64 maxSize, uint64 requestSize)
{
    return (slot->maxSize <= maxSize && slot->thresholdSize > slot->currentSize + requestSize);
}

inline bool TableSpaceUsageManager::IsFull(uint64 maxSize, uint64 currentSize, uint64 requestSize)
{
    return (maxSize < currentSize + requestSize);
}

/*
 * Get threshold size from current size and maxsize
 * 1. to void deviation between the actual size and statistical size, we recalculate actual size
 *     when the increase size beyonds (the rest size * TABLESPACE_THRESHOLD_RATE)
 * 2. to void frequent calculation, we give up recalculation if the rest size is small enough, here
 *     is CRITICA_POINT_VALUE(100MB)
 */
inline uint64 TableSpaceUsageManager::GetThresholdSize(uint64 maxSize, uint64 currentSize)
{
    if (maxSize > currentSize) {
        uint64 diff = maxSize - currentSize;
        return (diff > CRITICA_POINT_VALUE) ? (currentSize + TABLESPACE_THRESHOLD_RATE * diff) : maxSize;
    }
    return maxSize;
}

static inline bool IgnoreTableSpaceCheck(Oid tableSpaceOid, uint64 requestSize, bool segment)
{
    /*
     * Limitations:
     * 1. In ordinary cluster with slaves, only PRIMARY datanodes check tablespace used;
     * 2. In cluster without slaves, all the datenodes are in NORMAL mode and they will
     *    do checking;
     * 3. But If this datanode is in recovery, its mode either PENDING_MODE or STANDBY_MODE.
     *    Ignore checking and ensure a successful recovery.
     */
    if ((requestSize == 0 && !segment) || t_thrd.xlog_cxt.InRecovery || (t_thrd.postmaster_cxt.HaShmData == NULL) ||
        (t_thrd.postmaster_cxt.HaShmData->current_mode != PRIMARY_MODE &&
            t_thrd.postmaster_cxt.HaShmData->current_mode != NORMAL_MODE)) {
        return true;
    }

    /*
     * skip pg_default and pg_global since it is initialized
     * as unlimited and unchangeable.
     */
    if (tableSpaceOid == DEFAULTTABLESPACE_OID || tableSpaceOid == GLOBALTABLESPACE_OID)
        return true;

    return false;
}

DataSpaceType RelationUsesSpaceType(char relpersistence)
{
    if (u_sess->attr.attr_common.max_query_retry_times != 0) {
        /* if cn_retry is turned on, the unlogged table will be defined as permanent table */
        if (relpersistence == RELPERSISTENCE_TEMP) {
            return SP_TEMP;
        } else {
            return SP_PERM;
        }
    } else {
        /* if cn_retry is truned off, the unlogged table does't write xlog */
        if (relpersistence == RELPERSISTENCE_PERMANENT) {
            return SP_PERM;
        } else {
            return SP_TEMP;
        }
    }
}

/*
 * @Description: table space is exeed max size
 * @IN/OUT tableSpaceOid: table space for check
 * @IN/OUT requestSize: request size for table space
 *
 * Important: this founction will process  SI message queue .
 *                  after call this founction must reopen smgr if it set smgr owner
 */
void TableSpaceUsageManager::IsExceedMaxsize(Oid tableSpaceOid, uint64 requestSize, bool segment)
{
    int slotIndex = -1;
    int bucketIndex = -1;
    int freeSlotIndex = -1;
    bool isLimited = false;
    uint64 maxSize = 0;
    uint64 currentSize = 0;
    TableSpaceUsageBucket* bucket = NULL;
    TableSpaceUsageSlot* slot = NULL;

    /* skip it while initdb */
    if (IsInitdb) {
        u_sess->cmd_cxt.l_tableSpaceOid = tableSpaceOid;
        u_sess->cmd_cxt.l_isLimit = false;
        return;
    }

    /*
     * Segment-page storage calls IsExceedMaxsize is often caused by 'smgrextend', which does physical file
     * extension. However, smgrextend may be invoked in ReadBuffer_common_ReadBlock that after invoking
     * StartBufferIO. TableSpaceUsageManager::IsLimited may also invoke StartBufferIO because it has to 
     * scan the pg_tablespace system table. It forbids invoking 'StartBufferIO' twice in one call stack.
     *
     * Thus, we try to read tablespace's limit before entering any BufferIO, and store the limit info in 
     * thread local variables. 
     * requestSize == 0 means probing MaxSize info.
     * requestSize != 0 means real ExceedMaxSize test.
    */
    if (segment && requestSize == 0) {
        u_sess->cmd_cxt.l_tableSpaceOid = tableSpaceOid;
        u_sess->cmd_cxt.l_isLimit =
            TableSpaceUsageManager::IsLimited(tableSpaceOid, &u_sess->cmd_cxt.l_maxSize);
    }

    if (IgnoreTableSpaceCheck(tableSpaceOid, requestSize, segment))
        return;

    bucketIndex = TableSpaceUsageManager::GetBucketIndex(tableSpaceOid);
    bucket = &u_sess->cmd_cxt.TableSpaceUsageArray->m_tab[bucketIndex];

    for (;;) {
        freeSlotIndex = -1;
        if (segment) {
            if (u_sess->cmd_cxt.l_tableSpaceOid == tableSpaceOid) {
                isLimited = u_sess->cmd_cxt.l_isLimit;
                maxSize = u_sess->cmd_cxt.l_maxSize;
            } else {
                /*
                 * Tablespace limist is not cached; we can not read system relation to avoid invalidating SMgrRelation
                 * objects.
                 */
                return;
            }
        } else {
            isLimited = TableSpaceUsageManager::IsLimited(tableSpaceOid, &maxSize);
        }
        SpinLockAcquire(&bucket->mutex);

        /* skip if the tablespace is unlimited and the special bucket is empty */
        if (!isLimited && !bucket->count) {
            SpinLockRelease(&bucket->mutex);
            return;
        }

        /* try to get usage slot for the tablespace if it existes, or get a free slot */
        for (slotIndex = 0; slotIndex < TABLESPACE_BUCKET_CONFLICT_LISTLEN; slotIndex++) {
            if (likely(bucket->spcUsage[slotIndex].tableSpaceOid == tableSpaceOid))
                break;
            else if (InvalidOid == bucket->spcUsage[slotIndex].tableSpaceOid && -1 == freeSlotIndex)
                freeSlotIndex = slotIndex;
        }

        if (segment && requestSize != 0 && slotIndex == TABLESPACE_BUCKET_CONFLICT_LISTLEN) {
            return;
        }

        if (unlikely(slotIndex == TABLESPACE_BUCKET_CONFLICT_LISTLEN && -1 < freeSlotIndex)) {
            /* reset the usage slot in the bucket if there is no usable slot */
            TableSpaceUsageManager::ResetBucket(bucket);
            freeSlotIndex = 0;
        }

        if (likely(isLimited)) {
            if (likely(slotIndex < TABLESPACE_BUCKET_CONFLICT_LISTLEN)) {
                slot = &bucket->spcUsage[slotIndex];

                if (unlikely(currentSize))
                    slot->currentSize = currentSize;
            } else {
                Assert(freeSlotIndex >= 0);

                slot = &bucket->spcUsage[freeSlotIndex];
                slot->maxSize = maxSize;
                slot->tableSpaceOid = tableSpaceOid;
                slot->currentSize = currentSize;
                slot->thresholdSize = 0;
                bucket->count++;
            }
        } else {
            /*
             * the tablespace is changed to be unlimited
             */
            if (slotIndex < TABLESPACE_BUCKET_CONFLICT_LISTLEN) {
                TableSpaceUsageManager::ResetUsageSlot(&bucket->spcUsage[slotIndex]);
                bucket->count--;
            }

            SpinLockRelease(&bucket->mutex);
            return;
        }

        /* just refresh currentSize if it is within limit */
        if (unlikely(currentSize) || (segment && requestSize != 0)) {
            if (unlikely(TableSpaceUsageManager::IsFull(maxSize, currentSize, requestSize)) &&
                !u_sess->attr.attr_common.IsInplaceUpgrade) {
                /* 
                 * if space is not enough, purge some objs in RB and retry. 
                 * We can not do DML when segment is on, because we can not read any buffer now.
                 */
                if (!segment && RbCltPurgeSpaceDML(tableSpaceOid)) {
                    SpinLockRelease(&bucket->mutex);
                    currentSize = pg_cal_tablespace_size_oid(tableSpaceOid);
                    continue;
                }
                SpinLockRelease(&bucket->mutex);
                ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                        errmsg("Insufficient storage space for tablespace \"%s\"", get_tablespace_name(tableSpaceOid)),
                        errhint("Limited size is %lu, current size is %lu, request size is %lu",
                            maxSize,
                            currentSize,
                            requestSize)));
            } else {
                slot->currentSize += requestSize;
                slot->thresholdSize = TableSpaceUsageManager::GetThresholdSize(slot->maxSize, slot->currentSize);

                SpinLockRelease(&bucket->mutex);
                return;
            }
        } else {
            if (likely(TableSpaceUsageManager::WithinLimit(slot, maxSize, requestSize))) {
                slot->maxSize = maxSize;
                slot->currentSize += requestSize;
                SpinLockRelease(&bucket->mutex);

                return;
            }
        }

        /*
         * we have to release to release the spinlock when we try to calculate the special
         * tablespace, we lock the uasge slot with paramater lockcCount to prevent it is reset
         */
        SpinLockRelease(&bucket->mutex);
        Assert(!segment || requestSize == 0);
        currentSize = pg_cal_tablespace_size_oid(tableSpaceOid);
    }
}

/*
 * @Description: if it's equal to the default tablespce of this database,
 *   InvalidOid will be returned.
 * @Param[IN] tblspc: tablespace oid, maybe it's 0.
 * @Return: returned value will be written into pg_class.reltablespce.
 * @See also: ConvertToRelfilenodeTblspcOid()
 */
Oid ConvertToPgclassRelTablespaceOid(Oid tblspc)
{
    return (u_sess->proc_cxt.MyDatabaseTableSpace == tblspc) ? InvalidOid : tblspc;
}

/*
 * @Description: if it's InvalidOid, then it means that
 *   the default tablespce of this database will be used.
 * @Param[IN] tblspc: tablespace oid, which maybe from pg_class.reltablespce.
 * @Return: the real tablespace oid, which is greater than 0.
 * @See also: ConvertToPgclassRelTablespaceOid()
 */
Oid ConvertToRelfilenodeTblspcOid(Oid tblspc)
{
    Assert(CheckMyDatabaseMatch());
    return (InvalidOid == tblspc) ? u_sess->proc_cxt.MyDatabaseTableSpace : tblspc;
}
