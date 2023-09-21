/* -------------------------------------------------------------------------
 *
 * miscinit.c
 *	  miscellaneous initialization support stuff
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/miscinit.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/param.h>
#include <signal.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <grp.h>
#include <pwd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#ifdef HAVE_UTIME_H
#include <utime.h>
#endif
#include <sys/utsname.h>

#include "catalog/pg_authid.h"
#include "commands/user.h"
#include "job/job_scheduler.h"
#include "job/job_worker.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/postmaster.h"
#include "postmaster/snapcapturer.h"
#include "postmaster/cfs_shrinker.h"
#include "postmaster/rbcleaner.h"
#include "storage/smgr/fd.h"
#include "storage/ipc.h"
#include "storage/pg_shmem.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "libpq/libpq.h"
#include "auditfuncs.h"
#include "replication/walsender.h"
#include "alarm/alarm.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "catalog/pgxc_group.h"
#include "optimizer/nodegroups.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "gs_policy/policy_common.h"
#include "storage/file/fio_device.h"
#include "ddes/dms/ss_reform_common.h"

#ifdef ENABLE_MULTIPLE_NODES
#include "tsdb/compaction/compaction_entry.h"
#endif   /* ENABLE_MULTIPLE_NODES */
#include "access/ustore/knl_undoworker.h"

#define DIRECTORY_LOCK_FILE "postmaster.pid"

#define INIT_SESSION_MAX_INT32_BUFF 20

#define InvalidPid ((pid_t)(-1))

Alarm alarmItemTooManyDbUserConn[1] = {ALM_AI_Unknown, ALM_AS_Normal, 0, 0, 0, 0, {0}, {0}, NULL};

/* ----------------------------------------------------------------
 *		ignoring system indexes support stuff
 *
 * NOTE: "ignoring system indexes" means we do not use the system indexes
 * for lookups (either in hardwired catalog accesses or in planner-generated
 * plans).	We do, however, still update the indexes when a catalog
 * modification is made.
 * ----------------------------------------------------------------
 */

void ReportAlarmTooManyDbUserConn(const char* roleName)
{
    AlarmAdditionalParam tempAdditionalParam;

    // Initialize the alarm item
    AlarmItemInitialize(alarmItemTooManyDbUserConn,
        ALM_AI_TooManyDbUserConn,
        alarmItemTooManyDbUserConn->stat,
        NULL,
        alarmItemTooManyDbUserConn->lastReportTime,
        alarmItemTooManyDbUserConn->reportCount);
    // fill the alarm message
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
        g_instance.attr.attr_common.PGXCNodeName,
        "AllDatabases",
        const_cast<char*>(roleName),
        alarmItemTooManyDbUserConn,
        ALM_AT_Fault,
        const_cast<char*>(roleName));
    // report the alarm
    AlarmReporter(alarmItemTooManyDbUserConn, ALM_AT_Fault, &tempAdditionalParam);
}

void ReportResumeTooManyDbUserConn(const char* roleName)
{
    AlarmAdditionalParam tempAdditionalParam;

    // Initialize the alarm item
    AlarmItemInitialize(alarmItemTooManyDbUserConn,
        ALM_AI_TooManyDbUserConn,
        alarmItemTooManyDbUserConn->stat,
        NULL,
        alarmItemTooManyDbUserConn->lastReportTime,
        alarmItemTooManyDbUserConn->reportCount);
    // fill the resume message
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
        g_instance.attr.attr_common.PGXCNodeName,
        "AllDatabases",
        const_cast<char*>(roleName),
        alarmItemTooManyDbUserConn,
        ALM_AT_Resume);
    // report the alarm
    AlarmReporter(alarmItemTooManyDbUserConn, ALM_AT_Resume, &tempAdditionalParam);
}

void ReportAlarmDataInstLockFileExist()
{
    Alarm alarmItem[1];
    AlarmAdditionalParam tempAdditionalParam;

    // Initialize the alarm item
    AlarmItemInitialize(alarmItem, ALM_AI_DataInstLockFileExist, ALM_AS_Reported, NULL);
    // fill the alarm message
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
        g_instance.attr.attr_common.PGXCNodeName,
        "",
        "",
        alarmItem,
        ALM_AT_Fault,
        g_instance.attr.attr_common.PGXCNodeName);
    // report the alarm
    AlarmReporter(alarmItem, ALM_AT_Fault, &tempAdditionalParam);
}

void ReportResumeDataInstLockFileExist()
{
    Alarm alarmItem[1];
    AlarmAdditionalParam tempAdditionalParam;

    // Initialize the alarm item
    AlarmItemInitialize(alarmItem, ALM_AI_DataInstLockFileExist, ALM_AS_Normal, NULL);
    // fill the alarm message
    WriteAlarmAdditionalInfo(
        &tempAdditionalParam, g_instance.attr.attr_common.PGXCNodeName, "", "", alarmItem, ALM_AT_Resume);
    // report the alarm
    AlarmReporter(alarmItem, ALM_AT_Resume, &tempAdditionalParam);
}

/* ----------------------------------------------------------------
 *				database path / name support stuff
 * ----------------------------------------------------------------
 */

void SetDatabasePath(const char* path)
{
    /* This should happen only once per process */
    Assert(!u_sess->proc_cxt.DatabasePath);
    u_sess->proc_cxt.DatabasePath =
        MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), path);
}

/*
 * Set data directory, but make sure it's an absolute path.  Use this,
 * never set t_thrd.proc_cxt.DataDir directly.
 */
void SetDataDir(const char* dir)
{
    AssertArg(dir);

    /* If presented path is relative, convert to absolute */
    char* newm = make_absolute_path(dir);
    char real_newm[PATH_MAX + 1] = {'\0'};
    char* DataDir = (char*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), MAXPGPATH);

    if (realpath(newm, real_newm) == NULL) {
        ereport(ERROR, (errcode(ERRCODE_FILE_READ_FAILED),errmsg("invalid path:%s", dir)));
    }
    errno_t rc = strncpy_s(DataDir, MAXPGPATH, real_newm, MAXPGPATH - 1);
    securec_check(rc, "\0", "\0");
    pfree(newm);

    if (t_thrd.proc_cxt.DataDir) {
#ifdef FRONTEND
        free(t_thrd.proc_cxt.DataDir);
#else
        pfree(t_thrd.proc_cxt.DataDir);
#endif
    }

    t_thrd.proc_cxt.DataDir = DataDir;
}

/*
 * Change working directory to t_thrd.proc_cxt.DataDir.  Most of the postmaster and backend
 * code assumes that we are in t_thrd.proc_cxt.DataDir so it can use relative paths to access
 * stuff in and under the data directory.  For convenience during path
 * setup, however, we don't force the chdir to occur during SetDataDir.
 */
void ChangeToDataDir(void)
{
    AssertState(t_thrd.proc_cxt.DataDir);

    if (chdir(t_thrd.proc_cxt.DataDir) < 0)
        ereport(FATAL,
            (errcode_for_file_access(), errmsg("could not change directory to \"%s\": %m", t_thrd.proc_cxt.DataDir)));
}

/*
 * If the given pathname isn't already absolute, make it so, interpreting
 * it relative to the current working directory.
 *
 * Also canonicalizes the path.  The result is always a malloc'd copy.
 *
 * Note: interpretation of relative-path arguments during postmaster startup
 * should happen before doing ChangeToDataDir(), else the user will probably
 * not like the results.
 */
char* make_absolute_path(const char* path)
{
    char* newm = NULL;
    size_t tmplen;

    /* Returning null for null input is convenient for some callers */
    if (path == NULL) {
        return NULL;
    }

    if (!is_absolute_path(path)) {
        char* buf = NULL;
        size_t buflen;

        buflen = MAXPGPATH;

        for (;;) {
#ifdef FRONTEND
            buf = (char*)malloc(buflen);
#else
            buf = (char*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), buflen);
#endif

            if (buf == NULL)
                ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

            if (getcwd(buf, buflen) != NULL) {
                break;
            }
            else if (errno == ERANGE) {
#ifdef FRONTEND
                free(buf);
#else
                pfree(buf);
#endif
                buflen *= 2;
                continue;
            } else {
#ifdef FRONTEND
                free(buf);
#else
                pfree(buf);
#endif
                ereport(FATAL, (errmsg("could not get current working directory: %m")));
            }
        }

        tmplen = strlen(buf) + strlen(path) + 2;
#ifdef FRONTEND
        newm = (char*)malloc(tmplen);
#else
        newm = (char*)MemoryContextAlloc(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), tmplen);
#endif

        if (newm == NULL)
            ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

        int rcs = snprintf_s(newm, tmplen, tmplen - 1, "%s/%s", buf, path);
        securec_check_ss(rcs, "\0", "\0");
#ifdef FRONTEND
        free(buf);
#else
        pfree(buf);
#endif
    } else {
        newm = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), path);

        if (newm == NULL)
            ereport(FATAL, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
    }

    /* Make sure punctuation is canonical, too */
    canonicalize_path(newm);

    return newm;
}

/*
 * GetAuthenticatedUserId - get the authenticated user ID.
 */
Oid GetAuthenticatedUserId(void)
{
    return u_sess->misc_cxt.AuthenticatedUserId;
}

/*
 * GetUserId - get the current effective user ID.
 *
 * Note: there's no SetUserId() anymore; use SetUserIdAndSecContext().
 */
Oid GetUserId(void)
{
    if (!OidIsValid(u_sess->misc_cxt.CurrentUserId)) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("Current user id is invalid. Please try later.")));
    }
    return u_sess->misc_cxt.CurrentUserId;
}

/* Database Security: Support database audit */
/*
 * Brief		: get the current effective user ID.
 * Description	: called by all process.
 */
Oid GetCurrentUserId(void)
{
    return u_sess->misc_cxt.CurrentUserId;
}

Oid GetOldUserId(bool isReceive)
{
    if (isReceive) {
        return u_sess->misc_cxt.RecOldUserId;
    } else {
        return u_sess->misc_cxt.SendOldUserId;
    }
}

void SetOldUserId(Oid userId, bool isReceive)
{
    if (isReceive) {
        u_sess->misc_cxt.RecOldUserId = userId;
    } else {
        u_sess->misc_cxt.SendOldUserId = userId;
    }
}

/*
 * GetOuterUserId/SetOuterUserId - get/set the outer-level user ID.
 */
Oid GetOuterUserId(void)
{
    AssertState(OidIsValid(u_sess->misc_cxt.OuterUserId));
    return u_sess->misc_cxt.OuterUserId;
}

static void SetOuterUserId(Oid userid)
{
    AssertState(u_sess->misc_cxt.SecurityRestrictionContext == 0);
    AssertArg(OidIsValid(userid));
    u_sess->misc_cxt.OuterUserId = userid;

    /* We force the effective user ID to match, too */
    u_sess->misc_cxt.CurrentUserId = userid;
}

/*
 * GetSessionUserId/SetSessionUserId - get/set the session user ID.
 */
Oid GetSessionUserId(void)
{
    AssertState(OidIsValid(u_sess->misc_cxt.SessionUserId));
    return u_sess->misc_cxt.SessionUserId;
}

static void SetSessionUserId(Oid userid, bool is_superuser)
{
    AssertState(u_sess->misc_cxt.SecurityRestrictionContext == 0);
    AssertArg(OidIsValid(userid));
    u_sess->misc_cxt.SessionUserId = userid;
    u_sess->misc_cxt.SessionUserIsSuperuser = is_superuser;
    u_sess->misc_cxt.SetRoleIsActive = false;

    /* We force the effective user IDs to match, too */
    u_sess->misc_cxt.OuterUserId = userid;
    u_sess->misc_cxt.CurrentUserId = userid;

    /* update user id in MyBEEntry */
    if (t_thrd.shemem_ptr_cxt.MyBEEntry != NULL)
        t_thrd.shemem_ptr_cxt.MyBEEntry->st_userid = userid;
}

/*
 * modify_nodegroup_mode - get current node group mode;
 * The valid current_nodegroup_mode value is NG_COMMON and NG_LOGIC.
 * In datanode, current_nodegroup_mode value is always NG_COMMON because
 * pgxc_group has no record in datanode;
 */
void modify_nodegroup_mode()
{
    if (t_thrd.proc_cxt.postgres_initialized && (IS_PGXC_COORDINATOR || isRestoreMode) &&
        u_sess->misc_cxt.current_nodegroup_mode == NG_UNKNOWN) {
        HeapTuple htup;
        u_sess->misc_cxt.current_nodegroup_mode = NG_COMMON;
        htup = SearchSysCache1(PGXCGROUPNAME, CStringGetDatum(VNG_OPTION_ELASTIC_GROUP));
        if (HeapTupleIsValid(htup)) {
            u_sess->misc_cxt.current_nodegroup_mode = NG_LOGIC;
            ReleaseSysCache(htup);
        }
    }
}

/*
 * in_logic_cluster - judge whether current session is in logic cluster mode.
 */
bool in_logic_cluster()
{
    modify_nodegroup_mode();
    return u_sess->misc_cxt.current_nodegroup_mode == NG_LOGIC;
}

/*
 * exist_logic_cluster - judge whether elastic_group exist.
 * The method is simiar as in_logic_cluster, but the overhead is greater than  in_logic_cluster.
 * The function is independent of inval message.
 */
bool exist_logic_cluster()
{
    if (!IS_PGXC_COORDINATOR)
        return false;

    HeapTuple htup;
    htup = SearchSysCache1(PGXCGROUPNAME, CStringGetDatum(VNG_OPTION_ELASTIC_GROUP));
    if (HeapTupleIsValid(htup)) {
        ReleaseSysCache(htup);
        return true;
    }
    return false;
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * show_nodegroup_mode - return node group mode as sting.
 * The function is only used in guc.cpp.
 */
const char* show_nodegroup_mode(void)
{
    modify_nodegroup_mode();

    switch (u_sess->misc_cxt.current_nodegroup_mode) {
        case NG_COMMON:
            return "node group";
        case NG_LOGIC:
            return "logic cluster";
        default:
            return "unknown";
    }
}

#endif

#ifndef ENABLE_MULTIPLE_NODES
const int GetCustomParserId()
{
    int id = 0;
    switch (u_sess->attr.attr_sql.sql_compatibility) {
        case A_FORMAT:
            id = DB_CMPT_A;
            break;
        case B_FORMAT:
            id = DB_CMPT_B;
            break;
        case C_FORMAT:
            id = DB_CMPT_C;
            break;
        case PG_FORMAT:
            id = DB_CMPT_PG;
            break;
        default:
            ereport(WARNING, (errmsg("Unknown sql compatibility: %d", u_sess->attr.attr_sql.sql_compatibility)));
            return -1;
    }
    Assert(id >= 0 && id < DB_CMPT_MAX);
    return id;
}
#endif

/*
 * get_current_lcgroup_name - get current logic group name.
 * The function return NULL in datanode because datanode don't see pgxc_group.
 */
const char* get_current_lcgroup_name()
{
    if (IS_PGXC_COORDINATOR && u_sess->attr.attr_common.current_logic_cluster_name == NULL &&
        OidIsValid(u_sess->misc_cxt.current_logic_cluster) && t_thrd.proc_cxt.postgres_initialized) {
        Form_pgxc_group rform;
        HeapTuple groupTup = SearchSysCache1(PGXCGROUPOID, ObjectIdGetDatum(u_sess->misc_cxt.current_logic_cluster));

        if (HeapTupleIsValid(groupTup)) {
            rform = (Form_pgxc_group)GETSTRUCT(groupTup);
            u_sess->attr.attr_common.current_logic_cluster_name = MemoryContextStrdup(
                SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), NameStr(rform->group_name));
            ReleaseSysCache(groupTup);
        }
    }
    return u_sess->attr.attr_common.current_logic_cluster_name;
}

/*
 * set_current_lcgroup_oid - set current logic group oid.
 * The function is only used when SET ROLE and the session user is admin user.
 */
static void set_current_lcgroup_oid(Oid group_oid)
{
    if (u_sess->misc_cxt.current_logic_cluster != group_oid &&
        u_sess->attr.attr_common.current_logic_cluster_name != NULL) {
        pfree(u_sess->attr.attr_common.current_logic_cluster_name);
        u_sess->attr.attr_common.current_logic_cluster_name = NULL;
    }
    u_sess->misc_cxt.current_logic_cluster = group_oid;
    modify_nodegroup_mode();
    (void)get_current_lcgroup_name();
}

/*
 * show_show_lcgroup_name - show current logic group name.
 * The function is only used in guc.cpp.
 */
const char* show_lcgroup_name()
{
    const char* name = get_current_lcgroup_name();
    return (name == NULL) ? "" : name;
}

/*
 * get_current_lcgroup_oid - get current logic group oid.
 * The logic group oid will not be changed once login for common user.
 * sysadmin user (superuser) is not attached to any logic group, so the
 * logic group oid is InvalidOid when sysadmin user login.
 * SET ROLE will change to session logic group oid and
 * RESET ROLE will restore to InvalidOid for sysadmin user.
 */
Oid get_current_lcgroup_oid()
{
    return u_sess->misc_cxt.current_logic_cluster;
}

/*
 * is_lcgroup_admin - judge whether current user is logic cluster administrator.
 * Notice logic cluster administrator is not system admin user.
 */
bool is_lcgroup_admin()
{
    if (IS_PGXC_COORDINATOR &&
        (!OidIsValid(u_sess->misc_cxt.CurrentUserId) || !OidIsValid(u_sess->misc_cxt.current_logic_cluster))) {
        return false;
    }
    return has_rolvcadmin(u_sess->misc_cxt.CurrentUserId);
}

/*
 * is_logic_cluster - judge whether the node group is logic cluster.
 * The logic cluster's group_kind is 'v' or 'e';
 * NOTICE:  the function think elastic_group as logic cluster.
 */
bool is_logic_cluster(Oid group_id)
{
    Datum datum;
    bool isNull = false;
    HeapTuple tup;
    char group_kind;

    tup = SearchSysCache1(PGXCGROUPOID, ObjectIdGetDatum(group_id));

    if (HeapTupleIsValid(tup)) {
        datum = SysCacheGetAttr(PGXCGROUPOID, tup, Anum_pgxc_group_kind, &isNull);
        if (!isNull) {
            group_kind = DatumGetChar(datum);
            if (group_kind == 'v' || group_kind == 'e') {
                ReleaseSysCache(tup);
                return true;
            }
        }

        ReleaseSysCache(tup);
    }
    return false;
}

/*
 * get_pgxc_logic_groupoid
 *		Obtain PGXC Logic Group Oid for roleid
 *		Return Invalid Oid if group does not exist
 */
Oid get_pgxc_logic_groupoid(Oid roleid)
{
    bool isNull = true;
    Datum aclDatum;
    Oid group_id;
    HeapTuple roleTup;

    roleTup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));

    if (!HeapTupleIsValid(roleTup)) {
        return InvalidOid;
    }

    aclDatum = SysCacheGetAttr(AUTHOID, roleTup, Anum_pg_authid_rolnodegroup, &isNull);

    group_id = isNull ? InvalidOid : DatumGetObjectId(aclDatum);

    ReleaseSysCache(roleTup);

    return group_id;
}

/*
 * get_pgxc_logic_groupoid
 *		Obtain PGXC Logic Group Oid for rolename
 *		Return Invalid Oid if group does not exist
 */
Oid get_pgxc_logic_groupoid(const char* rolename)
{
    bool isNull = false;
    Datum aclDatum;
    Oid group_id;
    HeapTuple roleTup;

    roleTup = SearchUserHostName(rolename, NULL);

    if (!HeapTupleIsValid(roleTup)) {
        return InvalidOid;
    }

    isNull = true;

    aclDatum = SysCacheGetAttr(AUTHOID, roleTup, Anum_pg_authid_rolnodegroup, &isNull);

    group_id = isNull ? InvalidOid : DatumGetObjectId(aclDatum);

    ReleaseSysCache(roleTup);

    return group_id;
}

/*
 * NodeGroupCallback
 *		Syscache inval callback function
 */
static void SessionNodeGroupCallback(Datum arg, int cacheid, uint32 hashvalue)
{
    u_sess->misc_cxt.current_nodegroup_mode = NG_UNKNOWN;
    if (!EnableLocalSysCache()) {
        RelationCacheInvalidateBuckets();
    }
}

/*
 * RegisterNodeGroupCacheCallback
 *		Register pgxc_group change callback, only for coordinator.
 */
static void RegisterNodeGroupCacheCallback()
{
    if (IS_PGXC_COORDINATOR && !u_sess->misc_cxt.nodegroup_callback_registered) {
        CacheRegisterSessionSyscacheCallback(PGXCGROUPOID, SessionNodeGroupCallback, (Datum)0);
        u_sess->misc_cxt.nodegroup_callback_registered = true;
    }
}

/*
 * GetUserIdAndSecContext/SetUserIdAndSecContext - get/set the current user ID
 * and the u_sess->misc_cxt.SecurityRestrictionContext flags.
 *
 * Currently there are two valid bits in u_sess->misc_cxt.SecurityRestrictionContext:
 *
 * SECURITY_LOCAL_USERID_CHANGE indicates that we are inside an operation
 * that is temporarily changing u_sess->misc_cxt.CurrentUserId via these functions.	This is
 * needed to indicate that the actual value of u_sess->misc_cxt.CurrentUserId is not in sync
 * with guc.c's internal state, so SET ROLE has to be disallowed.
 *
 * SECURITY_RESTRICTED_OPERATION indicates that we are inside an operation
 * that does not wish to trust called user-defined functions at all.  The
 * policy is to use this before operations, e.g. autovacuum and REINDEX, that
 * enumerate relations of a database or schema and run functions associated
 * with each found relation.  The relation owner is the new user ID.  Set this
 * as soon as possible after locking the relation.  Restore the old user ID as
 * late as possible before closing the relation; restoring it shortly after
 * close is also tolerable.  If a command has both relation-enumerating and
 * non-enumerating modes, e.g. ANALYZE, both modes set this bit.  This bit
 * prevents not only SET ROLE, but various other changes of session state that
 * normally is unprotected but might possibly be used to subvert the calling
 * session later.  An example is replacing an existing prepared statement with
 * new code, which will then be executed with the outer session's permissions
 * when the prepared statement is next used.  These restrictions are fairly
 * draconian, but the functions called in relation-enumerating operations are
 * really supposed to be side-effect-free anyway.
 *
 * Unlike GetUserId, GetUserIdAndSecContext does *not* Assert that the current
 * value of u_sess->misc_cxt.CurrentUserId is valid; nor does SetUserIdAndSecContext require
 * the new value to be valid.  In fact, these routines had better not
 * ever throw any kind of error.  This is because they are used by
 * StartTransaction and AbortTransaction to save/restore the settings,
 * and during the first transaction within a backend, the value to be saved
 * and perhaps restored is indeed invalid.	We have to be able to get
 * through AbortTransaction without asserting in case InitPostgres fails.
 */
void GetUserIdAndSecContext(Oid* userid, int* sec_context)
{
    *userid = u_sess->misc_cxt.CurrentUserId;
    *sec_context = u_sess->misc_cxt.SecurityRestrictionContext;
}

void SetUserIdAndSecContext(Oid userid, int sec_context)
{
    u_sess->misc_cxt.CurrentUserId = userid;
    u_sess->misc_cxt.SecurityRestrictionContext = sec_context;
}

/*
 * InLocalUserIdChange - are we inside a local change of u_sess->misc_cxt.CurrentUserId?
 */
bool InLocalUserIdChange(void)
{
    return (u_sess->misc_cxt.SecurityRestrictionContext & SECURITY_LOCAL_USERID_CHANGE) != 0;
}

/*
 * InSecurityRestrictedOperation - are we inside a security-restricted command?
 */
bool InSecurityRestrictedOperation(void)
{
    return (u_sess->misc_cxt.SecurityRestrictionContext & SECURITY_RESTRICTED_OPERATION) != 0;
}

/*
 * InReceivingLocalUserIdChange - are we inside a dn get cn's userid change?
 */
bool InReceivingLocalUserIdChange()
{
    return (u_sess->misc_cxt.SecurityRestrictionContext & RECEIVER_LOCAL_USERID_CHANGE) != 0;
}

/*
 * InSendingLocalUserIdChange - are we inside cn send user to dn change?
 */
bool InSendingLocalUserIdChange()
{
    return (u_sess->misc_cxt.SecurityRestrictionContext & SENDER_LOCAL_USERID_CHANGE) != 0;
}
/*
 * These are obsolete versions of Get/SetUserIdAndSecContext that are
 * only provided for bug-compatibility with some rather dubious code in
 * pljava.	We allow the userid to be set, but only when not inside a
 * security restriction context.
 */
void GetUserIdAndContext(Oid* userid, bool* sec_def_context)
{
    *userid = u_sess->misc_cxt.CurrentUserId;
    *sec_def_context = InLocalUserIdChange();
}

void SetUserIdAndContext(Oid userid, bool sec_def_context)
{
    /* We throw the same error SET ROLE would. */
    if (InSecurityRestrictedOperation())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("cannot set parameter \"%s\" within security-restricted operation", "role")));

    u_sess->misc_cxt.CurrentUserId = userid;

    if (sec_def_context)
        u_sess->misc_cxt.SecurityRestrictionContext |= SECURITY_LOCAL_USERID_CHANGE;
    else
        u_sess->misc_cxt.SecurityRestrictionContext &= ~SECURITY_LOCAL_USERID_CHANGE;
}

/*
 * Check whether specified role has explicit REPLICATION privilege
 */
bool has_rolreplication(Oid roleid)
{
    bool result = false;
    HeapTuple utup;

    utup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));

    if (HeapTupleIsValid(utup)) {
        result = ((Form_pg_authid)GETSTRUCT(utup))->rolreplication;
        ReleaseSysCache(utup);
    }

    return result;
}

/*
 * Check whether specified role has explicit vcadmin privilege
 */
bool has_rolvcadmin(Oid roleid)
{
    bool isNull = false;
    bool result = false;
    HeapTuple utup;
    Datum datum;

    utup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));

    if (HeapTupleIsValid(utup)) {
        datum = SysCacheGetAttr(AUTHOID, utup, Anum_pg_authid_rolkind, &isNull);
        if (!isNull) {
            result = (DatumGetChar(datum) == 'v');
        }
        ReleaseSysCache(utup);
    }

    return result;
}

static void DecreaseUserCountReuse(Oid roleid, bool ispoolerreuse)
{
    if (ispoolerreuse == true && IS_THREAD_POOL_WORKER) {
        DecreaseUserCount(roleid);
    }
}

/*
 * Initialize user identity during normal backend startup
 */
void InitializeSessionUserId(const char* rolename, bool ispoolerreuse, Oid useroid)
{
    HeapTuple roleTup;
    Form_pg_authid rform;
    Oid roleid;
    /* Audit user login*/
    char details[PGAUDIT_MAXLENGTH];

    /*
     * Don't do scans if we're bootstrapping, none of the system catalogs
     * exist yet, and they should be owned by openGauss anyway.
     */
    if (IsBootstrapProcessingMode()) {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("IsBootstrapProcessingMode")));
    }

    /* In pooler stateless reuse mode, to reset session userid */
    if (!ENABLE_STATELESS_REUSE) {
        /* call only once */
        bool isUserOidInvalid = !(OidIsValid(u_sess->misc_cxt.AuthenticatedUserId));
        if (!isUserOidInvalid) {
            AssertState(false);
            ereport(FATAL,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("Abnormal process. UserOid has been reseted. Current userOid[%u], reset username is %s,"
                           "useroid is %u",
                        u_sess->misc_cxt.AuthenticatedUserId, rolename, useroid)));
        }
    }

    if (rolename != NULL) {
        roleTup = SearchUserHostName(rolename, NULL);
    } else {
        roleTup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(useroid));
    }

#ifndef ENABLE_MULTIPLE_NODES
    /*
     * In opengauss, we allow twophasecleaner to connect to database
     * as superusers for cleaning up temporary tables.
     */
    if (!HeapTupleIsValid(roleTup) && u_sess->proc_cxt.IsInnerMaintenanceTools) {
        roleTup = SearchSysCache1(AUTHOID, UInt32GetDatum(BOOTSTRAP_SUPERUSERID));

        char userName[NAMEDATALEN];
        MemoryContext oldcontext = NULL;
        oldcontext = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
        if (u_sess->proc_cxt.MyProcPort->user_name)
            pfree_ext(u_sess->proc_cxt.MyProcPort->user_name);
        u_sess->proc_cxt.MyProcPort->user_name = pstrdup((char*)GetSuperUserName((char*)userName));
        (void)MemoryContextSwitchTo(oldcontext);
        rolename = u_sess->proc_cxt.MyProcPort->user_name;
    }
#endif

    if (!HeapTupleIsValid(roleTup)) {
        char roleIdStr[INIT_SESSION_MAX_INT32_BUFF] = {0};
        if (rolename == NULL) {
            int rc = sprintf_s(roleIdStr, INIT_SESSION_MAX_INT32_BUFF, "%u", useroid);
            securec_check_ss(rc, "", "");
            rolename = roleIdStr;
        }
        /* 
         * Audit user login 
         * it's unsafe to deal with plugins hooks as dynamic lib may be released 
         */
        if (!(g_instance.status > NoShutdown) && user_login_hook) {
            user_login_hook(u_sess->proc_cxt.MyProcPort->database_name, rolename, false, true);
        }
        int rcs = snprintf_truncated_s(details,
            sizeof(details),
            "login db(%s) failed-the role(%s)does not exist",
            u_sess->proc_cxt.MyProcPort->database_name,
            rolename);
        securec_check_ss(rcs, "\0", "\0");
        pgaudit_user_login(FALSE, u_sess->proc_cxt.MyProcPort->database_name, details);

        ereport(FATAL,
            (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION), errmsg("Invalid username/password,login denied.")));
    }

    rform = (Form_pg_authid)GETSTRUCT(roleTup);
    roleid = HeapTupleGetOid(roleTup);
    if (rolename == NULL) {
        rolename = NameStr(rform->rolname);
    }

    u_sess->misc_cxt.AuthenticatedUserId = roleid;
    u_sess->misc_cxt.AuthenticatedUserIsSuperuser = rform->rolsuper;
    u_sess->proc_cxt.MyRoleId = roleid;

    /* This sets u_sess->misc_cxt.OuterUserId/u_sess->misc_cxt.CurrentUserId too */
    SetSessionUserId(roleid, u_sess->misc_cxt.AuthenticatedUserIsSuperuser);

    RegisterNodeGroupCacheCallback();

    if (IS_PGXC_COORDINATOR) {
        Datum groupDatum;
        bool isNull = true;

        groupDatum = SysCacheGetAttr(AUTHOID, roleTup, Anum_pg_authid_rolnodegroup, &isNull);

        u_sess->misc_cxt.current_logic_cluster = isNull ? InvalidOid : DatumGetObjectId(groupDatum);
    }

    /* Also mark our PGPROC entry with the authenticated user id */
    /* (We assume this is an atomic store so no lock is needed) */
    DecreaseUserCountReuse(t_thrd.proc->roleId, ispoolerreuse);
    t_thrd.proc->roleId = roleid;

    /*
     * These next checks are not enforced when in standalone mode, so that
     * there is a way to recover from sillinesses like "UPDATE pg_authid SET
     * rolcanlogin = false;".
     */
    if (IsUnderPostmaster) {
        /*
         * Is role allowed to login at all?
         */
        if (IS_THREAD_POOL_WORKER) {
            IncreaseUserCount(roleid);
        }

        if (!rform->rolcanlogin)
            ereport(FATAL,
                (errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
                    errmsg("role \"%s\" is not permitted to login", rolename)));

        /*
         * Check connection limit for this role.
         *
         * There is a race condition here --- we create our PGPROC before
         * checking for other PGPROCs.	If two backends did this at about the
         * same time, they might both think they were over the limit, while
         * ideally one should succeed and one fail.  Getting that to work
         * exactly seems more trouble than it is worth, however; instead we
         * just document that the connection limit is approximate.
         */
        if (rform->rolconnlimit >= 0 && !u_sess->misc_cxt.AuthenticatedUserIsSuperuser &&
            CountUserBackends(roleid) > rform->rolconnlimit) {
            ReportAlarmTooManyDbUserConn(rolename);
            ereport(FATAL,
                (errcode(ERRCODE_TOO_MANY_CONNECTIONS), errmsg("too many connections for role \"%s\"", rolename)));
        } else if (!u_sess->misc_cxt.AuthenticatedUserIsSuperuser) {
            ReportResumeTooManyDbUserConn(rolename);
        }
    }

    /* Record username and superuser status as GUC settings too */
    SetConfigOption("session_authorization", rolename, PGC_BACKEND, PGC_S_OVERRIDE);
    SetConfigOption(
        "is_sysadmin", u_sess->misc_cxt.AuthenticatedUserIsSuperuser ? "on" : "off", PGC_INTERNAL, PGC_S_OVERRIDE);

    ReleaseSysCache(roleTup);
}

/*
 * Initialize user identity during special backend startup
 */
void InitializeSessionUserIdStandalone(void)
{
    /*
     * This function should only be called in single-user mode and in
     * autovacuum workers.
     */
#ifdef ENABLE_MULTIPLE_NODES
    AssertState(!IsUnderPostmaster || IsAutoVacuumWorkerProcess() || IsJobSchedulerProcess() || IsJobWorkerProcess() ||
        AM_WAL_SENDER || IsTxnSnapCapturerProcess() || IsTxnSnapWorkerProcess() || IsUndoWorkerProcess() ||  IsCfsShrinkerProcess() ||
        CompactionProcess::IsTsCompactionProcess() || IsRbCleanerProcess() || IsRbWorkerProcess() ||
        t_thrd.role == PARALLEL_DECODE || t_thrd.role == LOGICAL_READ_RECORD);
#else   /* ENABLE_MULTIPLE_NODES */
    AssertState(!IsUnderPostmaster || IsAutoVacuumWorkerProcess() || IsJobSchedulerProcess() || IsJobWorkerProcess() ||
                AM_WAL_SENDER || IsTxnSnapCapturerProcess() || IsTxnSnapWorkerProcess() || IsUndoWorkerProcess() ||
                IsRbCleanerProcess() || IsCfsShrinkerProcess() ||
                IsRbWorkerProcess() || t_thrd.role == PARALLEL_DECODE || t_thrd.role == LOGICAL_READ_RECORD);
#endif   /* ENABLE_MULTIPLE_NODES */

    /* In pooler stateless reuse mode, to reset session userid */
    if (!ENABLE_STATELESS_REUSE) {
        /* call only once */
        AssertState(!OidIsValid(u_sess->misc_cxt.AuthenticatedUserId));
    }

    u_sess->misc_cxt.AuthenticatedUserId = BOOTSTRAP_SUPERUSERID;
    u_sess->misc_cxt.AuthenticatedUserIsSuperuser = true;

    SetSessionUserId(BOOTSTRAP_SUPERUSERID, true);

    RegisterNodeGroupCacheCallback();
}

/*
 * Change session auth ID while running
 *
 * Only a superuser may set auth ID to something other than himself.  Note
 * that in case of multiple SETs in a single session, the original userid's
 * superuserness is what matters.  But we set the GUC variable is_superuser
 * to indicate whether the *current* session userid is a superuser.
 *
 * Note: this is not an especially clean place to do the permission check.
 * It's OK because the check does not require catalog access and can't
 * fail during an end-of-transaction GUC reversion, but we may someday
 * have to push it up into assign_session_authorization.
 */
void SetSessionAuthorization(Oid userid, bool is_superuser)
{
    /* Must have authenticated already, else can't make permission check */
    AssertState(OidIsValid(u_sess->misc_cxt.AuthenticatedUserId));

    if (!t_thrd.xact_cxt.bInAbortTransaction && userid != u_sess->misc_cxt.AuthenticatedUserId &&
        !u_sess->misc_cxt.AuthenticatedUserIsSuperuser && !superuser())
        ereport(
            ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("permission denied to set session authorization")));

    SetSessionUserId(userid, is_superuser);

    SetConfigOption("is_sysadmin", is_superuser ? "on" : "off", PGC_INTERNAL, PGC_S_OVERRIDE);
}

/*
 * Report current role id
 *		This follows the semantics of SET ROLE, ie return the outer-level ID
 *		not the current effective ID, and return InvalidOid when the setting
 *		is logically SET ROLE NONE.
 */
Oid GetCurrentRoleId(void)
{
    if (u_sess->misc_cxt.SetRoleIsActive)
        return u_sess->misc_cxt.OuterUserId;
    else
        return InvalidOid;
}

/*
 * Change Role ID while running (SET ROLE)
 *
 * If roleid is InvalidOid, we are doing SET ROLE NONE: revert to the
 * session user authorization.	In this case the is_superuser argument
 * is ignored.
 *
 * When roleid is not InvalidOid, the caller must have checked whether
 * the session user has permission to become that role.  (We cannot check
 * here because this routine must be able to execute in a failed transaction
 * to restore a prior value of the ROLE GUC variable.)
 */
void SetCurrentRoleId(Oid roleid, bool is_superuser)
{
    /*
     * Get correct info if it's SET ROLE NONE
     *
     * If u_sess->misc_cxt.SessionUserId hasn't been set yet, just do nothing --- the eventual
     * SetSessionUserId call will fix everything.  This is needed since we
     * will get called during GUC initialization.
     */
    if (!OidIsValid(roleid)) {
        if (!OidIsValid(u_sess->misc_cxt.SessionUserId))
            return;

        roleid = u_sess->misc_cxt.SessionUserId;
        is_superuser = u_sess->misc_cxt.SessionUserIsSuperuser;

        u_sess->misc_cxt.SetRoleIsActive = false;
    } else
        u_sess->misc_cxt.SetRoleIsActive = true;

    SetOuterUserId(roleid);

    SetConfigOption("is_sysadmin", is_superuser ? "on" : "off", PGC_INTERNAL, PGC_S_OVERRIDE);

    if (u_sess->misc_cxt.SessionUserIsSuperuser) {
        set_current_lcgroup_oid(OidIsValid(roleid) ? get_pgxc_logic_groupoid(roleid) : InvalidOid);
    }
}

/*
 * Get user name from user oid
 */
char* GetUserNameFromId(Oid roleid)
{
    HeapTuple tuple;
    char* result = NULL;

    tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));

    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("invalid role OID: %u", roleid)));

    result = pstrdup(NameStr(((Form_pg_authid)GETSTRUCT(tuple))->rolname));

    ReleaseSysCache(tuple);
    return result;
}

char* GetUserNameById(Oid roleid)
{
    HeapTuple tuple;
    char* result = NULL;

    tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));

    if (!HeapTupleIsValid(tuple))
        return NULL;

    result = pstrdup(NameStr(((Form_pg_authid)GETSTRUCT(tuple))->rolname));

    ReleaseSysCache(tuple);
    return result;
}


/* -------------------------------------------------------------------------
 *				Interlock-file support
 *
 * These routines are used to create both a data-directory lockfile
 * ($DATADIR/postmaster.pid) and a Unix-socket-file lockfile ($SOCKFILE.lock).
 * Both kinds of files contain the same info initially, although we can add
 * more information to a data-directory lockfile after it's created, using
 * AddToDataDirLockFile().	See miscadmin.h for documentation of the contents
 * of these lockfiles.
 *
 * On successful lockfile creation, a proc_exit callback to remove the
 * lockfile is automatically created.
 * -------------------------------------------------------------------------
 */

/*
 * proc_exit callback to remove a lockfile.
 */
static void UnlinkLockFile(int status, Datum filename)
{
    char* fname = (char*)DatumGetPointer(filename);

    if (fname != NULL) {
        if (unlink(fname) != 0) {
            /* Should we complain if the unlink fails? */
            ereport(LOG, (errmsg("unlink lockfile %s fails.", fname)));
        }

        pfree(fname);
    }
}

/*
 * proc_exit callback to unlock a lockfile.
 */
static void UnLockPidLockFile(int status, Datum fileDes)
{
    int fd = DatumGetInt32(fileDes);

    if (fd != -1) {
        close(fd);
    }
}

static void CreatePidLockFile(const char* filename)
{
    int fd = -1;
    char pid_lock_file[MAXPGPATH] = {0};
    int rc = snprintf_s(pid_lock_file, MAXPGPATH, MAXPGPATH - 1, "%s.lock", filename);
    securec_check_ss(rc, "", "");

    if ((fd = open(pid_lock_file, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR)) == -1) {
        ereport(FATAL, (errcode_for_file_access(), errmsg("could not create or open lock file \"%s\": %m", pid_lock_file)));
    }

    if (flock(fd, LOCK_EX | LOCK_NB) == -1) {
        close(fd);
        ereport(FATAL, (errcode_for_file_access(), errmsg("could not lock file \"%s\": %m", pid_lock_file)));
    }

    on_proc_exit(UnLockPidLockFile, Int32GetDatum(fd));
}

/* Get tgid of input process id */
pid_t getProcessTgid(pid_t pid)
{
#define TGID_ITEM_NUM 2
    char pid_path[MAXPGPATH];
    FILE *fp = NULL;
    char getBuff[MAXPGPATH];
    char paraName[MAXPGPATH];
    pid_t tgid = InvalidPid;
    int rc;

    rc = snprintf_s(pid_path, MAXPGPATH, MAXPGPATH - 1, "/proc/%d/status", pid);
    securec_check_ss(rc, "\0", "\0");

    /* may fail because of ENOENT or privilege */
    fp = fopen(pid_path, "r");
    if (fp == NULL) {
        return InvalidPid;
    }

    /* parse process's status file */
    while (fgets(getBuff, MAXPGPATH, fp) != NULL) {
        if (strstr(getBuff, "Tgid:") != NULL &&
            sscanf_s(getBuff, "%s   %d", paraName, MAXPGPATH, &tgid) == TGID_ITEM_NUM) {
            break;
        } else {
            tgid = InvalidPid;
        }
    }

    (void)fclose(fp);

    return tgid;
}

/*
 * Create a lockfile.
 *
 * filename is the name of the lockfile to create.
 * amPostmaster is used to determine how to encode the output PID.
 * isDDLock and refName are used to determine what error message to produce.
 */
static void CreateLockFile(const char* filename, bool amPostmaster, bool isDDLock, const char* refName)
{
    int fd = -1;
    char buffer[MAXPGPATH * 2 + 256];
    int ntries;
    int len;
    int encoded_pid;
    pid_t other_pid;
    pid_t other_tgid;
    pid_t my_pid, my_p_pid, my_gp_pid;
    const char* envvar = NULL;

    /* Grab a file lock to establish our priority to process postmaster.pid */
    if (isDDLock) {
        CreatePidLockFile(filename);
    }

    /*
     * If the PID in the lockfile is our own PID or our parent's or
     * grandparent's PID, then the file must be stale (probably left over from
     * a previous system boot cycle).  We need to check this because of the
     * likelihood that a reboot will assign exactly the same PID as we had in
     * the previous reboot, or one that's only one or two counts larger and
     * hence the lockfile's PID now refers to an ancestor shell process.  We
     * allow pg_ctl to pass down its parent shell PID (our grandparent PID)
     * via the environment variable PG_GRANDPARENT_PID; this is so that
     * launching the postmaster via pg_ctl can be just as reliable as
     * launching it directly.  There is no provision for detecting
     * further-removed ancestor processes, but if the init script is written
     * carefully then all but the immediate parent shell will be root-owned
     * processes and so the kill test will fail with EPERM.  Note that we
     * cannot get a false negative this way, because an existing postmaster
     * would surely never launch a competing postmaster or pg_ctl process
     * directly.
     */
    my_pid = getpid();

#ifndef WIN32
    my_p_pid = getppid();
#else

    /*
     * Windows hasn't got getppid(), but doesn't need it since it's not using
     * real kill() either...
     */
    my_p_pid = 0;
#endif

    envvar = gs_getenv_r("PG_GRANDPARENT_PID");

    if (envvar != NULL) {
        check_backend_env(envvar);
        my_gp_pid = atoi(envvar);
    } else {
        my_gp_pid = 0;
    }

    /*
     * We need a loop here because of race conditions.	But don't loop forever
     * (for example, a non-writable $PGDATA directory might cause a failure
     * that won't go away).  100 tries seems like plenty.
     */
    for (ntries = 0;; ntries++) {
        /*
         * Try to create the lock file --- O_EXCL makes this atomic.
         *
         * Think not to make the file protection weaker than 0600.	See
         * comments below.
         */
        fd = open(filename, O_RDWR | O_CREAT | O_EXCL, 0600);

        if (fd >= 0)
            break; /* Success; exit the retry loop */

        /*
         * Couldn't create the pid file. Probably it already exists.
         */
        if ((errno != EEXIST && errno != EACCES) || ntries > 100)
            ereport(FATAL, (errcode_for_file_access(), errmsg("could not create lock file \"%s\": %m", filename)));

        /*
         * Read the file to get the old owner's PID.  Note race condition
         * here: file might have been deleted since we tried to create it.
         */
        fd = open(filename, O_RDONLY, 0600);

        if (fd < 0) {
            if (errno == ENOENT)
                continue; /* race condition; try again */

            ereport(FATAL, (errcode_for_file_access(), errmsg("could not open lock file \"%s\": %m", filename)));
        }

        pgstat_report_waitevent(WAIT_EVENT_LOCK_FILE_CREATE_READ);
        if ((len = read(fd, buffer, sizeof(buffer) - 1)) < 0)
            ereport(FATAL, (errcode_for_file_access(), errmsg("could not read lock file \"%s\": %m", filename)));
        pgstat_report_waitevent(WAIT_EVENT_END);
        close(fd);

        buffer[len] = '\0';
        encoded_pid = atoi(buffer);

        /* if pid < 0, the pid is for openGauss, not postmaster */
        other_pid = (pid_t)((encoded_pid < 0) ? -encoded_pid : encoded_pid);

        if (other_pid <= 0) {
            struct stat filenameStat;

            if (lstat(filename, &filenameStat) >= 0) {
                if (0 == filenameStat.st_size) {
                    if (remove(filename) < 0)
                        ereport(FATAL,
                            (errcode_for_file_access(),
                                errmsg("bogus lock file \"%s\",could not unlink it : %m", filename)));

                    continue;
                }
            }
            ereport(FATAL,
                (errmsg("bogus data in lock file \"%s\": \"%s\", please kill the "
                        "instance process, than remove the damaged lock file",
                    filename,
                    buffer)));
        }

        other_tgid = getProcessTgid(other_pid);

        /*
         * Check to see if the other process still exists
         *
         * Per discussion above, my_pid, my_p_pid, and my_gp_pid can be
         * ignored as false matches.
         *
         * Normally kill() will fail with ESRCH if the given PID doesn't
         * exist.
         *
         * We can treat the EPERM-error case as okay because that error
         * implies that the existing process has a different userid than we
         * do, which means it cannot be a competing postmaster.  A postmaster
         * cannot successfully attach to a data directory owned by a userid
         * other than its own.	(This is now checked directly in
         * checkDataDir(), but has been true for a long time because of the
         * restriction that the data directory isn't group- or
         * world-accessible.)  Also, since we create the lockfiles mode 600,
         * we'd have failed above if the lockfile belonged to another userid
         * --- which means that whatever process kill() is reporting about
         * isn't the one that made the lockfile.  (NOTE: this last
         * consideration is the only one that keeps us from blowing away a
         * Unix socket file belonging to an instance of openGauss being run by
         * someone else, at least on machines where /tmp hasn't got a
         * stickybit.)
         */
        if (other_pid != my_pid && other_pid != my_p_pid && other_pid != my_gp_pid &&
        (other_tgid == InvalidPid || (other_tgid != my_pid && other_tgid != my_p_pid && other_tgid != my_gp_pid))) {
            if (kill(other_pid, 0) == 0 || (errno != ESRCH && errno != EPERM)) {
/* lockfile belongs to a live process */
#ifndef WIN32
                if (IsMyPostmasterPid(other_pid, t_thrd.proc_cxt.DataDir))
#endif
                {
                    ReportAlarmDataInstLockFileExist();

                    ereport(FATAL,
                        (errcode(ERRCODE_LOCK_FILE_EXISTS),
                            errmsg("lock file \"%s\" already exists", filename),
                            isDDLock
                                ? ((encoded_pid < 0)
                                          ? errhint("Is another openGauss (PID %d) running in data directory \"%s\"?",
                                                (int)other_pid,
                                                refName)
                                          : errhint("Is another postmaster (PID %d) running in data directory \"%s\"?",
                                                (int)other_pid,
                                                refName))
                                : ((encoded_pid < 0) ? errhint("Is another openGauss (PID %d) \
                                                                using socket file \"%s\"?",
                                                         (int)other_pid,
                                                         refName)
                                                   : errhint("Is another postmaster (PID %d) using socket file \"%s\"?",
                                                         (int)other_pid,
                                                         refName))));
                }
            }
        }

        /*
         * No, the creating process did not exist.	However, it could be that
         * the postmaster crashed (or more likely was kill -9'd by a clueless
         * admin) but has left orphan backends behind.	Check for this by
         * looking to see if there is an associated shmem segment that is
         * still in use.
         *
         * Note: because postmaster.pid is written in multiple steps, we might
         * not find the shmem ID values in it; we can't treat that as an
         * error.
         */
        if (isDDLock != false) {
            char* ptr = buffer;
            unsigned long id1, id2;
            int lineno;

            for (lineno = 1; lineno < LOCK_FILE_LINE_SHMEM_KEY; lineno++) {
                if ((ptr = strchr(ptr, '\n')) == NULL)
                    break;

                ptr++;
            }

            if (ptr != NULL && sscanf_s(ptr, "%lu %lu", &id1, &id2) == 2) {
                if (PGSharedMemoryIsInUse(id1, id2)) {
                    ereport(FATAL,
                        (errcode(ERRCODE_LOCK_FILE_EXISTS),
                            errmsg("pre-existing shared memory block "
                                   "(key %lu, ID %lu) is still in use",
                                id1,
                                id2),
                            errhint("If you're sure there are no old "
                                    "server processes still running, remove "
                                    "the shared memory block "
                                    "or just delete the file \"%s\".",
                                filename)));
                }
            }
        }

        /*
         * Looks like nobody's home.  Unlink the file and try again to create
         * it.	Need a loop because of possible race condition against other
         * would-be creators.
         */
        if (unlink(filename) < 0)
            ereport(FATAL,
                (errcode_for_file_access(),
                    errmsg("could not remove old lock file \"%s\": %m", filename),
                    errhint("The file seems accidentally left over, but "
                            "it could not be removed. Please remove the file "
                            "by hand and try again.")));
    }

    ReportResumeDataInstLockFileExist();

    /*
     * Successfully created the file, now fill it.	See comment in miscadmin.h
     * about the contents.  Note that we write the same first five lines into
     * both datadir and socket lockfiles; although more stuff may get added to
     * the datadir lockfile later.
     */
    char* unixSocketDir = NULL;
    char* pghost = gs_getenv_r("PGHOST");
    if (pghost != NULL) {
        check_backend_env(pghost);
    }

    if (*g_instance.attr.attr_network.UnixSocketDir != '\0') {
        unixSocketDir = g_instance.attr.attr_network.UnixSocketDir;
    } else {
        if (pghost != NULL && *(pghost) != '\0') {
            unixSocketDir = pghost;
        } else {
            unixSocketDir = DEFAULT_PGSOCKET_DIR;
        }
    }

    int rc = snprintf_s(buffer,
        sizeof(buffer),
        sizeof(buffer) - 1,
        "%d\n%s\n%ld\n%d\n%s\n",
        amPostmaster ? (int)my_pid : -((int)my_pid),
        t_thrd.proc_cxt.DataDir,
        (long)t_thrd.proc_cxt.MyStartTime,
        g_instance.attr.attr_network.PostPortNumber,
#ifdef HAVE_UNIX_SOCKETS
        unixSocketDir
#else
        ""
#endif
    );

    securec_check_ss(rc, "\0", "\0");

    /*
     * In a standalone backend, the next line (LOCK_FILE_LINE_LISTEN_ADDR)
     * will never receive data, so fill it in as empty now.
     */
    if (isDDLock && !amPostmaster)
        strlcat(buffer, "\n", sizeof(buffer));

    errno = 0;

    pgstat_report_waitevent(WAIT_EVENT_LOCK_FILE_CREATE_WRITE);
    if (strlen(buffer) > 0) {
        if ((unsigned int)(write(fd, buffer, strlen(buffer))) != strlen(buffer)) {
        int save_errno = errno;

        close(fd);
        (void)unlink(filename);
        /* if write didn't set errno, assume problem is no disk space */
        errno = save_errno ? save_errno : ENOSPC;
        ereport(FATAL, (errcode_for_file_access(), errmsg("could not write lock file \"%s\": %m", filename)));
        }
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    pgstat_report_waitevent(WAIT_EVENT_LOCK_FILE_CREATE_SYNC);
    if (pg_fsync(fd) != 0) {
        int save_errno = errno;

        close(fd);
        (void)unlink(filename);
        errno = save_errno;
        ereport(FATAL, (errcode_for_file_access(), errmsg("could not write lock file \"%s\": %m", filename)));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    if (close(fd) != 0) {
        int save_errno = errno;

        (void)unlink(filename);
        errno = save_errno;
        ereport(FATAL, (errcode_for_file_access(), errmsg("could not write lock file \"%s\": %m", filename)));
    }

    /*
     * Arrange for automatic removal of lockfile at proc_exit.
     */
    {
        char* ptr = NULL;
        ptr = MemoryContextStrdup(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), filename);
        on_proc_exit(UnlinkLockFile, PointerGetDatum(ptr));
    }
}

/*
 * Create the data directory lockfile.
 *
 * When this is called, we must have already switched the working
 * directory to t_thrd.proc_cxt.DataDir, so we can just use a relative path.  This
 * helps ensure that we are locking the directory we should be.
 */
void CreateDataDirLockFile(bool amPostmaster)
{
    CreateLockFile(DIRECTORY_LOCK_FILE, amPostmaster, true, t_thrd.proc_cxt.DataDir);
}

/*
 * Create a lockfile for the specified Unix socket file.
 */
void CreateSocketLockFile(const char* socketfile, bool amPostmaster, bool is_create_psql_sock)
{
    char lockfile[MAXPGPATH];

    int rc = snprintf_s(lockfile, sizeof(lockfile), sizeof(lockfile) - 1, "%s.lock", socketfile);
    securec_check_ss(rc, "\0", "\0");

    CreateLockFile(lockfile, amPostmaster, false, socketfile);
    /* Save name of lockfile for TouchSocketLockFile */
    errno_t rcs = strcpy_s((is_create_psql_sock ? u_sess->misc_cxt.socketLockFile : u_sess->misc_cxt.hasocketLockFile),
        MAXPGPATH,
        lockfile);
    securec_check_c(rcs, "\0", "\0");
}

/*
 * TouchSocketLockFileInternel & TouchSocketLockFile -- mark socket lock file as recently accessed
 *
 * This routine should be called every so often to ensure that the lock file
 * has a recent mod or access date.  That saves it
 * from being removed by overenthusiastic /tmp-directory-cleaner daemons.
 * (Another reason we should never have put the socket file in /tmp...)
 */
void TouchSocketLockFileInternel(const char* socketLockFile)
{
    /* Do nothing if we did not create a socket... */
    if (socketLockFile[0] != '\0') {
        /*
         * utime() is POSIX standard, utimes() is a common alternative; if we
         * have neither, fall back to actually reading the file (which only
         * sets the access time not mod time, but that should be enough in
         * most cases).  In all paths, we ignore errors.
         */
#ifdef HAVE_UTIME
        utime(socketLockFile, NULL);
#else /* !HAVE_UTIME */
#ifdef HAVE_UTIMES
        utimes(socketLockFile, NULL);
#else /* !HAVE_UTIMES */
        int fd = -1;
        char buffer[1];

        fd = open(socketLockFile, O_RDONLY | PG_BINARY, 0);

        if (fd >= 0) {
            read(fd, buffer, sizeof(buffer));
            close(fd);
        }

#endif /* HAVE_UTIMES */
#endif /* HAVE_UTIME */
    }
}

void TouchSocketLockFile(void)
{
    TouchSocketLockFileInternel(u_sess->misc_cxt.socketLockFile);
    TouchSocketLockFileInternel(u_sess->misc_cxt.hasocketLockFile);
}

/*
 * Add (or replace) a line in the data directory lock file.
 * The given string should not include a trailing newline.
 *
 * Caution: this erases all following lines.  In current usage that is OK
 * because lines are added in order.  We could improve it if needed.
 */
void AddToDataDirLockFile(int target_line, const char* str)
{
    int fd = -1;
    int len;
    int lineno;
    char* ptr = NULL;
    char buffer[BLCKSZ];
    char temp[BLCKSZ] = {0};
    char new_file[MAXPGPATH];
    bool has_split = false;

    fd = open(DIRECTORY_LOCK_FILE, O_RDWR | PG_BINARY, 0);

    if (fd < 0) {
        ereport(LOG, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", DIRECTORY_LOCK_FILE)));
        return;
    }

    pgstat_report_waitevent(WAIT_EVENT_LOCK_FILE_ADDTODATADIR_READ);
    len = read(fd, buffer, sizeof(buffer) - 1);
    pgstat_report_waitevent(WAIT_EVENT_END);

    if (len < 0) {
        ereport(LOG, (errcode_for_file_access(), errmsg("could not read from file \"%s\": %m", DIRECTORY_LOCK_FILE)));
        close(fd);
        return;
    }

    buffer[len] = '\0';

    /*
     * Skip over lines we are not supposed to rewrite.
     */
    ptr = buffer;

    for (lineno = 1; lineno < target_line; lineno++) {
        if ((ptr = strchr(ptr, '\n')) == NULL) {
            ereport(LOG,
                (errmsg("incomplete data in \"%s\": found only %d newlines while trying to add line %d",
                    DIRECTORY_LOCK_FILE,
                    lineno - 1,
                    target_line)));
            close(fd);
            return;
        }

        ptr++;
    }

#ifndef ENABLE_MULTIPLE_NODES
    /* If there are extra info, we should copy the string to right place in buffer */
    if (target_line == LOCK_FILE_LINE_LISTEN_ADDR && ptr != NULL && strlen(ptr) != 0) {
        char *end = ptr;
        end = strchr(end, '\n');

        if (end != NULL) {
            end++;
            /* set last character to '\0' */
            char *invalid = strchr(end, '\n');
            if (invalid != NULL) {
                *(invalid + 1) = '\0';
            }
            int rcs = strcpy_s(temp, BLCKSZ - 1, end);
            securec_check(rcs, "\0", "\0");
            has_split = true;
        }
    }
#endif

    /*
     * Write or rewrite the target line.
     */
    int rcs = snprintf_s(ptr, buffer + sizeof(buffer) - ptr, buffer + sizeof(buffer) - ptr - 1, "%s\n", str);
    securec_check_ss(rcs, "\0", "\0");

    if (has_split) {
        /* reload listen_addresses, we will write IP to postmaster.pid.new and then rename it to postmaster.pid */
        size_t str_len = strlen(str);
        rcs = snprintf_s(ptr + str_len + 1, buffer + sizeof(buffer) - ptr - str_len - 1,
            buffer + sizeof(buffer) - ptr - str_len - 1 - 1, "%s", temp);
        securec_check_ss(rcs, "\0", "\0");

        rcs = snprintf_s(new_file, MAXPGPATH, MAXPGPATH - 1, "%s.new", DIRECTORY_LOCK_FILE);
        securec_check_ss(rcs, "\0", "\0");
        close(fd);
        fd = open(new_file, O_RDWR | O_CREAT, 0600);

        if (fd < 0) {
            ereport(LOG, (errcode_for_file_access(),
                errmsg("could not open new temp file \"%s\": %m", new_file)));
            return;
        }
        pgstat_report_waitevent(WAIT_EVENT_LOCK_FILE_ADDTODATADIR_WRITE);
        if (ftruncate(fd, (off_t)0) != 0) {
            pgstat_report_waitevent(WAIT_EVENT_END);
            ereport(LOG, (errcode_for_file_access(),
                errmsg("could not clear file \"%s\": %m", DIRECTORY_LOCK_FILE)));
            close(fd);
            return;
        }
        pgstat_report_waitevent(WAIT_EVENT_END);
    }
    /*
     * And rewrite the data.  Since we write in a single kernel call, this
     * update should appear atomic to onlookers.
     */
    len = strlen(buffer);
    errno = 0;

    pgstat_report_waitevent(WAIT_EVENT_LOCK_FILE_ADDTODATADIR_WRITE);
    if (lseek(fd, (off_t)0, SEEK_SET) != 0 || (int)write(fd, buffer, len) != len) {
        pgstat_report_waitevent(WAIT_EVENT_END);
        /* if write didn't set errno, assume problem is no disk space */
        if (errno == 0)
            errno = ENOSPC;

        ereport(LOG, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", DIRECTORY_LOCK_FILE)));
        close(fd);
        return;
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    pgstat_report_waitevent(WAIT_EVENT_LOCK_FILE_ADDTODATADIR_SYNC);
    if (pg_fsync(fd) != 0) {
        ereport(LOG, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", DIRECTORY_LOCK_FILE)));
    }
    pgstat_report_waitevent(WAIT_EVENT_END);

    if (close(fd) != 0) {
        ereport(LOG, (errcode_for_file_access(), errmsg("could not write to file \"%s\": %m", DIRECTORY_LOCK_FILE)));
    }

    if (has_split) {
        if (rename(new_file, DIRECTORY_LOCK_FILE)) {
            ereport(LOG, (errcode_for_file_access(),
                errmsg("failed to rename file \"%s\": %m", new_file)));
        }
    }
}

/* -------------------------------------------------------------------------
 *				Version checking support
 * -------------------------------------------------------------------------
 */

/*
 * Determine whether the PG_VERSION file in directory `path' indicates
 * a data version compatible with the version of this program.
 *
 * If compatible, return. Otherwise, ereport(FATAL).
 */
void ValidatePgVersion(const char* path)
{
    char full_path[MAXPGPATH];
    FILE* file = NULL;
    int ret;
    long file_major, file_minor;
    long my_major = 0, my_minor = 0;
    char* endptr = NULL;
    const char* version_string = PG_VERSION;
    errno_t rc;

    // skip in dss mode
    if (ENABLE_DSS) {
        return;
    }

    my_major = strtol(version_string, &endptr, 10);

    if (*endptr == '.')
        my_minor = strtol(endptr + 1, NULL, 10);

    rc = snprintf_s(full_path, sizeof(full_path), sizeof(full_path) - 1, "%s/PG_VERSION", path);
    securec_check_intval(rc, , );

    file = AllocateFile(full_path, "r");

    if (file == NULL) {
        if (errno == ENOENT)
            ereport(FATAL,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("\"%s\" is not a valid data directory", path),
                    errdetail("File \"%s\" is missing.", full_path)));
        else
            ereport(FATAL, (errcode_for_file_access(), errmsg("could not open file \"%s\": %m", full_path)));
    }

    ret = fscanf_s(file, "%ld.%ld", &file_major, &file_minor);

    if (ret != 2)
        ereport(FATAL,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("\"%s\" is not a valid data directory", path),
                errdetail("File \"%s\" does not contain valid data.", full_path),
                errhint("You might need to initdb.")));

    FreeFile(file);

    if (my_major != file_major || my_minor != file_minor)
        ereport(FATAL,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("database files are incompatible with server"),
                errdetail("The data directory was initialized by PostgreSQL version %ld.%ld, "
                          "which is not compatible with this version %s.",
                    file_major,
                    file_minor,
                    version_string)));
}

/* -------------------------------------------------------------------------
 *				Library preload support
 * -------------------------------------------------------------------------
 */
/*
 * load the shared libraries listed in 'libraries'
 *
 * 'gucname': name of GUC variable, for error reports
 * 'restricted': if true, force libraries to be in $libdir/plugins/
 */
static void load_libraries(const char* libraries, const char* gucname, bool restricted)
{
    char* rawstring = NULL;
    List* elemlist = NULL;
    int elevel;
    ListCell* l = NULL;

    if (libraries == NULL || libraries[0] == '\0') {
        return; /* nothing to do */
    }

    /* Need a modifiable copy of string */
    rawstring = pstrdup(libraries);

    /* Parse string into list of identifiers */
    if (!SplitIdentifierString(rawstring, ',', &elemlist)) {
        /* syntax error in list */
        pfree(rawstring);
        list_free(elemlist);
        ereport(LOG, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid list syntax in parameter \"%s\"", gucname)));
        return;
    }

    /*
     * Choose notice level: avoid repeat messages when re-loading a library
     * that was preloaded into the postmaster.	(Only possible in EXEC_BACKEND
     * configurations)
     */
#ifdef EXEC_BACKEND

    if (IsUnderPostmaster && u_sess->misc_cxt.process_shared_preload_libraries_in_progress)
        elevel = DEBUG2;
    else
#endif
        elevel = LOG;

    foreach (l, elemlist) {
        char* tok = (char*)lfirst(l);
        char* filename = NULL;
        errno_t rc;

        filename = pstrdup(tok);
        if (strcmp(filename, "security_plugin") == 0 && WorkingGrandVersionNum  < 92076) {
            continue;
        }        
        canonicalize_path(filename);

        /* If restricting, insert $libdir/plugins if not mentioned already */
        if (restricted && first_dir_separator(filename) == NULL) {
            char* expanded = NULL;

            expanded = (char*)palloc(strlen("$libdir/plugins/") + strlen(filename) + 1);
            rc = strcpy_s(expanded, strlen("$libdir/plugins/") + strlen(filename) + 1, "$libdir/plugins/");
            securec_check_c(rc, "\0", "\0");
            rc = strcat_s(expanded, strlen("$libdir/plugins/") + strlen(filename) + 1, filename);
            securec_check_c(rc, "\0", "\0");
            pfree(filename);
            filename = expanded;
        }

        load_file(filename, restricted);
        ereport(elevel, (errmsg("loaded library \"%s\"", filename)));
        pfree(filename);
    }

    pfree(rawstring);
    list_free(elemlist);
}

/*
 * process shared preloaded libraries internal
 */
void
process_shared_preload_libraries_internal(void)
{
#ifdef ENABLE_MULTIPLE_NODES
    if (is_streaming_engine_available()) {
	   load_libraries("streaming", "shared_preload_libraries", false);
    }
#endif
    return;
}

/*
 * process any libraries that should be preloaded at postmaster start
 */
void process_shared_preload_libraries(void)
{
    u_sess->misc_cxt.process_shared_preload_libraries_in_progress = true;
    load_libraries(g_instance.attr.attr_common.shared_preload_libraries_string, "shared_preload_libraries", false);
    process_shared_preload_libraries_internal();
    u_sess->misc_cxt.process_shared_preload_libraries_in_progress = false;
}

/*
 * process any libraries that should be preloaded at backend start
 */
void process_local_preload_libraries(void)
{
    load_libraries(u_sess->attr.attr_common.local_preload_libraries_string, "local_preload_libraries", true);
}

void pg_bindtextdomain(const char* domain)
{
#ifdef ENABLE_NLS

    if (my_exec_path[0] != '\0') {
        char locale_path[MAXPGPATH];

        get_locale_path(my_exec_path, locale_path);
        bindtextdomain(domain, locale_path);
        pg_bind_textdomain_codeset(domain);
    }

#endif
}

// reset u_sess->misc_cxt.Pseudo_CurrentUserId's reference to u_sess->misc_cxt.CurrentUserId, always called
// when the stored procedure completed.
void Reset_Pseudo_CurrentUserId(void)
{
    u_sess->misc_cxt.Pseudo_CurrentUserId = &u_sess->misc_cxt.CurrentUserId;
}

/*
 * The backend_version is recorded in session_param.
 * During connection obtaining, the agent_send_connection_params_parallel function
 * is used to synchronize the version number.
 */
void register_backend_version(uint32 backend_version){
    if (IsBootstrapProcessingMode() || IsInitProcessingMode() || !IS_PGXC_COORDINATOR) {
        return;
    }
    char sql_tmp[CHAR_SIZE];
    errno_t rc;
    int ret = 0;
    rc = memset_s(sql_tmp, CHAR_SIZE, 0, CHAR_SIZE);
    securec_check_c(rc, "\0", "\0");
    if (backend_version > 0) {
        ret = snprintf_s(sql_tmp, CHAR_SIZE, CHAR_SIZE - 1, "set backend_version = %d;", backend_version);
    } else {
        ereport(ERROR, (errcode(ERRCODE_SET_QUERY), errmsg("backend_version is a error value: %d", backend_version)));
    }
    securec_check_ss_c(ret, "\0", "\0");
    if (PoolManagerSetCommand(POOL_CMD_GLOBAL_SET, sql_tmp, "backend_version") < 0){
        ereport(ERROR, (errmodule(MOD_TRANS_HANDLE), errcode(ERRCODE_SET_QUERY), errmsg("ERROR SET backend_version")));
    }
}

/*
 * Check whether the version contains the backend_version parameter.
 */
bool contain_backend_version(uint32 version_number) {
    return ((version_number >= BACKEND_VERSION_PRE_INCLUDE_NUM &&
             version_number < BACKEND_VERSION_PRE_END_NUM) ||
            (version_number >= BACKEND_VERSION_INCLUDE_NUM));
}

void ss_initdwsubdir(char *dssdir, int instance_id)
{
    int rc;

    /* file correspanding to double write directory */
    rc = snprintf_s(g_instance.datadir_cxt.dw_subdir_cxt.dwOldPath, MAXPGPATH, MAXPGPATH - 1,
        "%s/pg_doublewrite%d/pg_dw", dssdir, instance_id);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.dw_subdir_cxt.dwPathPrefix, MAXPGPATH, MAXPGPATH - 1,
        "%s/pg_doublewrite%d/pg_dw_", dssdir, instance_id);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.dw_subdir_cxt.dwSinglePath, MAXPGPATH, MAXPGPATH - 1,
        "%s/pg_doublewrite%d/pg_dw_single", dssdir, instance_id);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.dw_subdir_cxt.dwBuildPath, MAXPGPATH, MAXPGPATH - 1,
        "%s/pg_doublewrite%d/pg_dw.build", dssdir, instance_id);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.dw_subdir_cxt.dwUpgradePath, MAXPGPATH, MAXPGPATH - 1,
        "%s/pg_doublewrite%d/dw_upgrade", dssdir, instance_id);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.dw_subdir_cxt.dwBatchUpgradeMetaPath, MAXPGPATH, MAXPGPATH - 1,
        "%s/pg_doublewrite%d/dw_batch_upgrade_meta", dssdir, instance_id);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.dw_subdir_cxt.dwBatchUpgradeFilePath, MAXPGPATH, MAXPGPATH - 1,
        "%s/pg_doublewrite%d/dw_batch_upgrade_files", dssdir, instance_id);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.dw_subdir_cxt.dwMetaPath, MAXPGPATH, MAXPGPATH - 1,
        "%s/pg_doublewrite%d/pg_dw_meta", dssdir, instance_id);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.dw_subdir_cxt.dwExtChunkPath, MAXPGPATH, MAXPGPATH - 1,
        "%s/pg_doublewrite%d/pg_dw_ext_chunk", dssdir, instance_id);
    securec_check_ss(rc, "", "");

    g_instance.datadir_cxt.dw_subdir_cxt.dwStorageType = (uint8)DEV_TYPE_DSS;
}

void initDssPath(char *dssdir)
{
    errno_t rc = EOK;

    rc = snprintf_s(g_instance.datadir_cxt.baseDir, MAXPGPATH, MAXPGPATH - 1, "%s/base", dssdir);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.globalDir, MAXPGPATH, MAXPGPATH - 1, "%s/global", dssdir);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.locationDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_location", dssdir);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.tblspcDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_tblspc", dssdir);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.clogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_clog", dssdir);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.csnlogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_csnlog", dssdir);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.serialDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_serial", dssdir);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.twophaseDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_twophase", dssdir);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.multixactDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_multixact", dssdir);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.xlogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog%d", dssdir,
        g_instance.attr.attr_storage.dms_attr.instance_id);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.controlPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_control", dssdir);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.controlBakPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_control.backup",
        dssdir);
    securec_check_ss(rc, "", "");

    rc = snprintf_s(g_instance.datadir_cxt.controlInfoPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_replication/pg_ss_ctl_info",
        dssdir);
    securec_check_ss(rc, "", "");

    ss_initdwsubdir(dssdir, g_instance.attr.attr_storage.dms_attr.instance_id);
}

void initDSSConf(void)
{
    if (!ENABLE_DSS) {
        return;
    }

    // check whether dss connect is successful.
    struct stat st;
    if (stat(g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name, &st) != 0 || !S_ISDIR(st.st_mode)) {
        ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg("Could not connect dssserver, vgname: \"%s\", socketpath: \"%s\"",
            g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name,
            g_instance.attr.attr_storage.dss_attr.ss_dss_conn_path),
            errhint("Check vgname and socketpath and restart later.")));
    } else {
        char *dssdir = g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name;

        // do not overwrite
        if (strncmp(g_instance.datadir_cxt.baseDir, dssdir, strlen(dssdir)) != 0) {
            initDssPath(dssdir);
        }
    }

    /* set xlog seg size to 1GB */
    XLogSegmentSize = DSS_XLOG_SEG_SIZE;
}
