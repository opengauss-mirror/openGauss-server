/* -------------------------------------------------------------------------
 *
 * misc.c
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/misc.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/file.h>
#include <signal.h>
#include <dirent.h>
#include <math.h>

#include "access/sysattr.h"
#include "catalog/catalog.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/tablespace.h"
#include "commands/user.h"
#include "funcapi.h"
#include "instruments/gs_stat.h"
#include "miscadmin.h"
#include "parser/keywords.h"
#include "pgstat.h"
#include "postmaster/syslogger.h"
#include "rewrite/rewriteHandler.h"
#include "replication/replicainternal.h"
#include "storage/smgr/fd.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/file/fio_device.h"
#include "utils/lsyscache.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "gssignal/gs_signal.h"
#include "workload/workload.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

extern void cancel_backend(ThreadId pid);
#define atooid(x) ((Oid)strtoul((x), NULL, 10))

/*
 * current_database()
 *	Expose the current database to the user
 */
Datum current_database(PG_FUNCTION_ARGS)
{
    Name db;

    db = (Name)palloc(NAMEDATALEN);

    namestrcpy(db, get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true));
    PG_RETURN_NAME(db);
}

/*
 * current_query()
 *	Expose the current query to the user (useful in stored procedures)
 *	We might want to use ActivePortal->sourceText someday.
 */
Datum current_query(PG_FUNCTION_ARGS)
{
    /* there is no easy way to access the more concise 'query_string' */
    if (t_thrd.postgres_cxt.debug_query_string) {
        char *mask_string = maskPassword(t_thrd.postgres_cxt.debug_query_string);
        if (mask_string == NULL) {
            mask_string = (char *)t_thrd.postgres_cxt.debug_query_string;
        }
        PG_RETURN_TEXT_P(cstring_to_text(mask_string));
    } else {
        PG_RETURN_NULL();
    }
}

/*
 * Send a signal to another backend.
 *
 * The signal is delivered for users with sysadmin privilege or the member of gs_role_signal_backend role
 * or the owner of the database or the same user as the backend being signaled.
 * For "dangerous" signals, an explicit check for superuser needs to be done prior to calling this function.
 *
 * Returns 0 on success, 1 on general failure, and 2 on permission error.
 * In the event of a general failure (return code 1), a warning message will
 * be emitted. For permission errors, doing that is the responsibility of
 * the caller.
 */
#define SIGNAL_BACKEND_SUCCESS 0
#define SIGNAL_BACKEND_ERROR 1
#define SIGNAL_BACKEND_NOPERMISSION 2
static int pg_signal_backend(ThreadId pid, int sig, bool checkPermission)
{
    PGPROC* proc = NULL;

    if (checkPermission && !superuser()) {
        /*
         * Since the user is not superuser, check for matching roles. Trust
         * that BackendPidGetProc will return NULL if the pid isn't valid,
         * even though the check for whether it's a backend process is below.
         * The IsBackendPid check can't be relied on as definitive even if it
         * was first. The process might end between successive checks
         * regardless of their order. There's no way to acquire a lock on an
         * arbitrary process to prevent that. But since so far all the callers
         * of this mechanism involve some request for ending the process
         * anyway, that it might end on its own first is not a problem.
         */
        proc = BackendPidGetProc(pid);

        if (proc == NULL) {
            return SIGNAL_BACKEND_NOPERMISSION;
        } else {
            bool rolePermission = is_member_of_role(GetUserId(), DEFAULT_ROLE_SIGNAL_BACKENDID) &&
                (proc->roleId != BOOTSTRAP_SUPERUSERID && !is_role_persistence(proc->roleId));
            if (!pg_database_ownercheck(proc->databaseId, u_sess->misc_cxt.CurrentUserId) &&
                proc->roleId != GetUserId() && !rolePermission) {
                return SIGNAL_BACKEND_NOPERMISSION;
            }
        }
    }

    if (!IsBackendPid(pid)) {
        /*
         * This is just a warning so a loop-through-resultset will not abort
         * if one backend terminated on its own during the run.
         */
        ereport(WARNING, (errmsg("PID %lu is not a gaussdb server thread", pid)));
        return SIGNAL_BACKEND_ERROR;
    }

    /*
     * Can the process we just validated above end, followed by the pid being
     * recycled for a new process, before reaching here?  Then we'd be trying
     * to kill the wrong thing.  Seems near impossible when sequential pid
     * assignment and wraparound is used.  Perhaps it could happen on a system
     * where pid re-use is randomized.	That race condition possibility seems
     * too unlikely to worry about.
     */

    if (gs_signal_send(pid, sig)) {
        /* Again, just a warning to allow loops */
        ereport(WARNING, (errmsg("could not send signal to process %lu: %m", pid)));
        return SIGNAL_BACKEND_ERROR;
    }
    return SIGNAL_BACKEND_SUCCESS;
}

uint64 get_query_id_beentry(ThreadId  tid)
{
    uint64 queryId = 0;
    PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.BackendStatusArray + BackendStatusArray_size - 1;
    PgBackendStatus* localentry = (PgBackendStatus*)palloc0(sizeof(PgBackendStatus));

    /*
     * We go through BackendStatusArray from back to front, because
     * in thread pool mode, both an active session and its thread pool worker
     * will have the same procpid in their entry and in most cases what we want
     * is session's entry, which will be returned first in such searching direction.
     */
    for (int i = 1; i <= BackendStatusArray_size; i++) {
        /*
         * Follow the protocol of retrying if st_changecount changes while we
         * copy the entry, or if it's odd.  (The check for odd is needed to
         * cover the case where we are able to completely copy the entry while
         * the source backend is between increment steps.)	We use a volatile
         * pointer here to ensure the compiler doesn't try to get cute.
         */
        if (gs_stat_encap_status_info(localentry, beentry)) {
            if (localentry->st_procpid == tid) {
                queryId = localentry->st_queryid;
                break;
            }
        }
        beentry--;
    }

    pfree_ext(localentry->st_appname);
    pfree_ext(localentry->st_clienthostname);
    pfree_ext(localentry->st_conninfo);
    pfree_ext(localentry->st_activity);

    pfree(localentry);

    return queryId;
}

/*
 * Signal to cancel a backend process.
 * This is allowed if you have sysadmin privilege or have the same user as the process being canceled
 * or you are the member of gs_role_signal_backend role or the owner of the database.
 */
Datum pg_cancel_backend(PG_FUNCTION_ARGS)
{
    int r = 0;
    ThreadId  tid = PG_GETARG_INT64(0);

    /*
     * It is forbidden to kill backend in the online expansion to protect
     * the lock session from being interrupted by external applications.
     */
    if (u_sess->attr.attr_sql.enable_online_ddl_waitlock && !u_sess->attr.attr_common.xc_maintenance_mode)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("kill backend is prohibited during online expansion."))));

    r = pg_signal_backend(tid, SIGINT, true);

    if (r == SIGNAL_BACKEND_NOPERMISSION) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("must have sysadmin privilege or a member of the gs_role_signal_backend role or the "
                    "owner of the database or the same user to cancel queries running in other server processes"))));
    }

    if (t_thrd.proc && t_thrd.proc->workingVersionNum >= 92060) {
        uint64 query_id = get_query_id_beentry(tid);
        (void)gs_close_all_stream_by_debug_id(query_id);
    }
    PG_RETURN_BOOL(r == SIGNAL_BACKEND_SUCCESS);
}

Datum pg_cancel_session(PG_FUNCTION_ARGS)
{
    int r = -1;
    ThreadId  tid = PG_GETARG_INT64(0);
    uint64 sid = PG_GETARG_INT64(1);
    if (tid == sid) {
        if (u_sess->attr.attr_sql.enable_online_ddl_waitlock && !u_sess->attr.attr_common.xc_maintenance_mode)
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    (errmsg("kill backend is prohibited during online expansion."))));

        r = pg_signal_backend(tid, SIGINT, true);

        if (r == SIGNAL_BACKEND_NOPERMISSION)
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    (errmsg("must be system admin or have the same role to cancel queries running in other server "
                            "processes"))));

        if (t_thrd.proc && t_thrd.proc->workingVersionNum >= 92060) {
            uint64 query_id = get_query_id_beentry(tid);
            (void)gs_close_all_stream_by_debug_id(query_id);
        }
    } else if (ENABLE_THREAD_POOL) {
        ThreadPoolSessControl *sess_ctrl = g_threadPoolControler->GetSessionCtrl();
        int ctrl_idx = sess_ctrl->FindCtrlIdxBySessId(sid);
        r = sess_ctrl->SendSignal((int)ctrl_idx, SIGINT);
    }
    PG_RETURN_BOOL(r == 0);

}

/*
 * Signal to cancel queries running in backends connected to demoted GTM.
 * This is allowed if you are a superuser.
 */
Datum pg_cancel_invalid_query(PG_FUNCTION_ARGS)
{
#ifndef  ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unsupported proc in single node mode.")));
    PG_RETURN_NULL();
#else
    if (!superuser()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("must be system admin to cancel invalid queries running in all server processes"))));
        PG_RETURN_BOOL(false);
    } else {
        proc_cancel_invalid_gtm_lite_conn();
        PG_RETURN_BOOL(true);
    }
#endif
}

int kill_backend(ThreadId tid, bool checkPermission)
{
    /*
     * It is forbidden to kill backend in the online expansion to protect
     * the lock session from being interrupted by external applications.
     */
    if (u_sess->attr.attr_sql.enable_online_ddl_waitlock && !u_sess->attr.attr_common.xc_maintenance_mode)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("kill backend is prohibited during online expansion."))));
 
    int r = pg_signal_backend(tid, SIGTERM, checkPermission);
    if (r == SIGNAL_BACKEND_NOPERMISSION) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                (errmsg("must have sysadmin privilege or a member of the gs_role_signal_backend role or "
                    "the owner of the database or the same user to terminate other backend"))));
    }
    
    if (t_thrd.proc && t_thrd.proc->workingVersionNum >= 92060) {
        uint64 query_id = get_query_id_beentry(tid);
        (void)gs_close_all_stream_by_debug_id(query_id);
    }

    return r;
}

/*
 * Signal to terminate a backend process.
 * This is allowed for users have sysadmin privilege or a member of the gs_role_signal_backend role or
 * the owner of the database or the same user as the process being terminated.
 */
Datum pg_terminate_backend(PG_FUNCTION_ARGS)
{
    ThreadId tid = PG_GETARG_INT64(0);
    int r = kill_backend(tid, true);
    PG_RETURN_BOOL(r == SIGNAL_BACKEND_SUCCESS);
}

Datum pg_terminate_session(PG_FUNCTION_ARGS)
{
    ThreadId tid = PG_GETARG_INT64(0);
    uint64 sid = PG_GETARG_INT64(1);
    int r = -1;

    if (tid == sid) {
        r = kill_backend(tid, true);
    } else if (ENABLE_THREAD_POOL) {
        ThreadPoolSessControl *sess_ctrl = g_threadPoolControler->GetSessionCtrl();
        int ctrl_idx = sess_ctrl->FindCtrlIdxBySessId(sid);
        r = sess_ctrl->SendSignal((int)ctrl_idx, SIGTERM);
    }

    PG_RETURN_BOOL(r == 0);
}

/*
 * function name: pg_wlm_jump_queue
 * description  : wlm jump the queue with thread id.
 */
Datum pg_wlm_jump_queue(PG_FUNCTION_ARGS)
{
    ThreadId tid = PG_GETARG_INT64(0);

    if (g_instance.wlm_cxt->dynamic_workload_inited)
        dywlm_client_jump_queue(&g_instance.wlm_cxt->MyDefaultNodeGroup.climgr, tid);
    else
        WLMJumpQueue(&g_instance.wlm_cxt->MyDefaultNodeGroup.parctl, tid);

    PG_RETURN_BOOL(true);
}

/*
 * function name: gs_wlm_switch_cgroup
 * description  : wlm switch cgroup to new cgroup with session id.
 */
Datum gs_wlm_switch_cgroup(PG_FUNCTION_ARGS)
{
    uint64 sess_id = PG_GETARG_INT64(0);
    char* cgroup = PG_GETARG_CSTRING(1);

    bool ret = false;

    if (g_instance.attr.attr_common.enable_thread_pool) {
        ereport(WARNING, (errmsg("Cannot switch control group in thread pool mode.")));
    } else if (g_instance.wlm_cxt->gscgroup_init_done > 0) {
        /* Only superuser or the owner of the query can change the control group. */
        if (IS_PGXC_COORDINATOR && !superuser())
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("the user have no right to "
                           "change cgroup for session [%lu]!",
                        sess_id)));

        /* If the connection is not from coordinator, data node cannot changed cgroup itself. */
        if (IS_PGXC_COORDINATOR || IsConnFromCoord())
            ret = WLMAjustCGroupByCNSessid(&g_instance.wlm_cxt->MyDefaultNodeGroup, sess_id, cgroup);
    } else
        ereport(WARNING, (errmsg("Switch failed because of cgroup without initialization.")));

    PG_RETURN_BOOL(ret);
}

/*
 * function name: gs_wlm_node_recover
 * description  : recover node for dynamic workload.
 */
Datum gs_wlm_node_recover(PG_FUNCTION_ARGS)
{
    bool isForce = PG_GETARG_BOOL(0);
    int count = 0;
    if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("must be superuser account to recover node for wlm")));
    }

    while (!WLMIsInfoInit()) {
        CHECK_FOR_INTERRUPTS();
        ++count;
        sleep(1);
        if (count >= 3) {
            ereport(WARNING, (errmsg("node recovering for dynamic workload failed.")));
            PG_RETURN_BOOL(true);
        }
    }

    if (!g_instance.wlm_cxt->dynamic_workload_inited) {
        ereport(NOTICE, (errmsg("dynamic workload disable, need not recover.")));
    } else {
        dywlm_node_recover(isForce);
    }

    PG_RETURN_BOOL(true);
}

/*
 * function name: gs_wlm_node_recover
 * description  : recover node for dynamic workload.
 */
Datum gs_wlm_node_clean(PG_FUNCTION_ARGS)
{
    char* nodename = PG_GETARG_CSTRING(0);

    bool ret = false;
    if (!superuser()) {
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("must be superuser account to clean node for wlm")));
    }

    if (!g_instance.wlm_cxt->dynamic_workload_inited) {
        ereport(NOTICE, (errmsg("dynamic workload disable, need not clean.")));
    } else {
        (void)dywlm_server_clean(nodename);
        ret = true;
    }

    PG_RETURN_BOOL(ret);
}

/*
 * function name: gs_wlm_rebuild_user_resource_pool
 * description  : rebuild user and resource pool htab.
 */
Datum gs_wlm_rebuild_user_resource_pool(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC,
            "gs_wlm_rebuild_user_resource_pool");
    }

    /* rebuild user resource pool hash table */
    if (!BuildUserRPHash())
        ereport(LOG, (errmsg("call 'gs_wlm_rebuild_user_resource_pool' to build user hash failed")));
    else {
        ereport(LOG, (errmsg("call 'gs_wlm_rebuild_user_resource_pool' to build user hash finished")));
    }

    PG_RETURN_BOOL(true);
}

/*
 * @Description: readjust user total space
 * @IN roleid: user oid
 * @Return: success or not
 * @See also:
 */
Datum gs_wlm_readjust_user_space(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);

    int count = 0;

    while (!WLMIsInfoInit()) {
        CHECK_FOR_INTERRUPTS();
        ++count;
        sleep(5);

        if (count >= 3)
            PG_RETURN_TEXT_P(cstring_to_text("Exec Failed"));
    }

    /* verify all user space */
    WLMReadjustAllUserSpace(roleid);

    PG_RETURN_TEXT_P(cstring_to_text("Exec Success"));
}

/*
 * @Description: readjust user total space through username
 * @IN username: user name
 * @Return: success or not
 * @See also:
 */
Datum gs_wlm_readjust_user_space_through_username(PG_FUNCTION_ARGS)
{
    char* username = PG_GETARG_CSTRING(0);
    int count = 0;

    while (!WLMIsInfoInit()) {
        CHECK_FOR_INTERRUPTS();
        ++count;
        sleep(5);

        if (count >= 3)
            PG_RETURN_TEXT_P(cstring_to_text("Exec Failed"));
    }

    /* verify user space in all databases */
    WLMReadjustUserSpaceThroughAllDbs(username);

    PG_RETURN_TEXT_P(cstring_to_text("Exec Success"));
}

/*
 * @Description: readjust user total space with reset flag,
 *      if flag is true, means calculate used space from zero,
 *      if flag is false, means calculate used space one by one.
 * @IN username: user name
 * @Return: success or not
 * @See also:
 */
Datum gs_wlm_readjust_user_space_with_reset_flag(PG_FUNCTION_ARGS)
{
    char* username = PG_GETARG_CSTRING(0);
    bool isReset = PG_GETARG_BOOL(1);
    int count = 0;

    while (!WLMIsInfoInit()) {
        CHECK_FOR_INTERRUPTS();
        ++count;
        sleep(5);

        if (count >= 3)
            PG_RETURN_TEXT_P(cstring_to_text("Exec Failed"));
    }

    /* verify all user space */
    WLMReadjustUserSpaceByNameWithResetFlag(username, isReset);

    PG_RETURN_TEXT_P(cstring_to_text("Exec Success"));
}

/*
 * function name: gs_get_role_name
 * description  :get role name with user oid.
 */
Datum gs_get_role_name(PG_FUNCTION_ARGS)
{
    Oid roleid = PG_GETARG_OID(0);

    char rolname[NAMEDATALEN] = {0};

    if (!superuser() && !isMonitoradmin(GetUserId())) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to get role name"),
                errhint("Must be system admin or monitor admin can get role name.")));
    }

    if (OidIsValid(roleid))
        (void)GetRoleName(roleid, rolname, sizeof(rolname));

    PG_RETURN_TEXT_P(cstring_to_text(rolname));
}

/*
 * Signal to reload the database configuration
 */
Datum pg_reload_conf(PG_FUNCTION_ARGS)
{
    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be system admin to signal the postmaster"))));

    if (gs_signal_send(PostmasterPid, SIGHUP)) {
        ereport(WARNING, (errmsg("failed to send signal to postmaster: %m")));
        PG_RETURN_BOOL(false);
    }

    PG_RETURN_BOOL(true);
}

/*
 * Rotate log file
 */
Datum pg_rotate_logfile(PG_FUNCTION_ARGS)
{
    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("must be system admin to rotate log files"))));

    if (!g_instance.attr.attr_common.Logging_collector) {
        ereport(WARNING, (errmsg("rotation not possible because log collection not active")));
        PG_RETURN_BOOL(false);
    }

    SendPostmasterSignal(PMSIGNAL_ROTATE_LOGFILE);
    PG_RETURN_BOOL(true);
}

/* Function to find out which databases make use of a tablespace */

typedef struct {
    char* location;
    DIR* dirdesc;
} ts_db_fctx;

Datum pg_tablespace_databases(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    struct dirent* de = NULL;
    ts_db_fctx* fctx = NULL;
    int location_len, ss_rc;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        Oid tablespaceOid = PG_GETARG_OID(0);

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        fctx = (ts_db_fctx*)palloc(sizeof(ts_db_fctx));

        if (tablespaceOid == GLOBALTABLESPACE_OID) {
            fctx->dirdesc = NULL;
            ereport(WARNING, (errmsg("global tablespace never has databases")));
        } else {
            if (tablespaceOid == DEFAULTTABLESPACE_OID) {
                location_len = (int)strlen(DEFTBSDIR) + 1;
                fctx->location = (char*)palloc(location_len);
                ss_rc = sprintf_s(fctx->location, (size_t)location_len, "%s", DEFTBSDIR);
            } else if (ENABLE_DSS) {
                location_len = (int)strlen(TBLSPCDIR) + 1 + OIDCHARS + 1 +
                               (int)strlen(TABLESPACE_VERSION_DIRECTORY) + 1;
                fctx->location = (char*)palloc(location_len);
                ss_rc = sprintf_s(fctx->location,
                    location_len,
                    "%s/%u/%s",
                    TBLSPCDIR,
                    tablespaceOid,
                    TABLESPACE_VERSION_DIRECTORY);
            } else {
#ifdef PGXC
                /* openGauss tablespaces also include node name in path */
                location_len = (int)strlen(TBLSPCDIR) + 1 + OIDCHARS + 1 +
                               (int)strlen(TABLESPACE_VERSION_DIRECTORY) + 1 +
                               (int)strlen(g_instance.attr.attr_common.PGXCNodeName) + 1;
                fctx->location = (char*)palloc(location_len);
                ss_rc = sprintf_s(fctx->location,
                    location_len,
                    "%s/%u/%s_%s",
                    TBLSPCDIR,
                    tablespaceOid,
                    TABLESPACE_VERSION_DIRECTORY,
                    g_instance.attr.attr_common.PGXCNodeName);
#else
                location_len = (int)strlen(TBLSPCDIR) + 1 + OIDCHARS + 1 +
                               (int)strlen(TABLESPACE_VERSION_DIRECTORY) + 1;
                fctx->location = (char*)palloc(location_len);
                ss_rc = sprintf_s(fctx->location,
                    location_len,
                    "%s/%u/%s",
                    TBLSPCDIR,
                    tablespaceOid,
                    TABLESPACE_VERSION_DIRECTORY);
#endif
            }
            securec_check_ss(ss_rc, "\0", "\0");
            fctx->dirdesc = AllocateDir(fctx->location);

            if (fctx->dirdesc == NULL) {
                /* the only expected error is ENOENT */
                if (errno != ENOENT)
                    ereport(ERROR,
                        (errcode_for_file_access(), errmsg("could not open directory \"%s\": %m", fctx->location)));
                ereport(WARNING, (errmsg("%u is not a tablespace OID", tablespaceOid)));
            }
        }
        funcctx->user_fctx = fctx;
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    fctx = (ts_db_fctx*)funcctx->user_fctx;

    if (fctx->dirdesc == NULL) /* not a tablespace */
        SRF_RETURN_DONE(funcctx);

    while ((de = ReadDir(fctx->dirdesc, fctx->location)) != NULL) {
        char* subdir = NULL;
        DIR* dirdesc = NULL;
        Oid datOid = atooid(de->d_name);

        /* this test skips . and .., but is awfully weak */
        if (!datOid)
            continue;

        /* if database subdir is empty, don't report tablespace as used */

        /* size = path length + dir sep char + file name + terminator */
        int sub_len = (int)strlen(fctx->location) + 1 + (int)strlen(de->d_name) + 1;
        subdir = (char*)palloc(sub_len);
        ss_rc = sprintf_s(subdir, sub_len, "%s/%s", fctx->location, de->d_name);
        securec_check_ss(ss_rc, "\0", "\0");
        dirdesc = AllocateDir(subdir);
        while ((de = ReadDir(dirdesc, subdir)) != NULL) {
            if (strcmp(de->d_name, ".") != 0 && strcmp(de->d_name, "..") != 0)
                break;
        }
        FreeDir(dirdesc);
        pfree_ext(subdir);

        if (de == NULL)
            continue; /* indeed, nothing in it */

        SRF_RETURN_NEXT(funcctx, ObjectIdGetDatum(datOid));
    }

    FreeDir(fctx->dirdesc);
    SRF_RETURN_DONE(funcctx);
}

/*
 * pg_tablespace_location - get location for a tablespace
 */
Datum pg_tablespace_location(PG_FUNCTION_ARGS)
{
    Oid tablespaceOid = PG_GETARG_OID(0);
    char sourcepath[MAXPGPATH];
    char targetpath[MAXPGPATH];
    int rllen;

    /*
     * It's useful to apply this function to pg_class.reltablespace, wherein
     * zero means "the database's default tablespace".	So, rather than
     * throwing an error for zero, we choose to assume that's what is meant.
     */
    tablespaceOid = ConvertToRelfilenodeTblspcOid(tablespaceOid);

    /*
     * Return empty string for the cluster's default tablespaces. If the
     * currrent user is not superuser, we should not expose the absolute
     * path to the client.
     */
    if (tablespaceOid == DEFAULTTABLESPACE_OID || tablespaceOid == GLOBALTABLESPACE_OID || !superuser()) {
        PG_RETURN_TEXT_P(cstring_to_text(""));
    }

#if defined(HAVE_READLINK) || defined(WIN32)
    /*
     * Find the location of the tablespace by reading the symbolic link that
     * is in pg_tblspc/<oid>.
     */
    errno_t ss_rc = snprintf_s(sourcepath, sizeof(sourcepath), sizeof(sourcepath) - 1,
                               "%s/%u", TBLSPCDIR, tablespaceOid);
    securec_check_ss(ss_rc, "\0", "\0");

    rllen = readlink(sourcepath, targetpath, sizeof(targetpath));
    if (rllen < 0)
        ereport(
            ERROR, (errcode(ERRCODE_FILE_READ_FAILED), errmsg("could not read symbolic link \"%s\": %m", sourcepath)));
    else if ((unsigned int)(rllen) >= sizeof(targetpath))
        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("symbolic link \"%s\" target is too long", sourcepath)));
    targetpath[rllen] = '\0';

    /* relative location will contain t_thrd.proc_cxt.DataDir */
   size_t dataDirLength = strlen(t_thrd.proc_cxt.DataDir);
    if (0 == strncmp(targetpath, t_thrd.proc_cxt.DataDir, dataDirLength) && (size_t)rllen > dataDirLength &&
        targetpath[dataDirLength] == '/') {
        /*
         * The position is  not '/' when skip t_thrd.proc_cxt.DataDir. the relative location can't start from '/'
         * We only need get the relative location, so remove the common prefix
         */
        int pos = (t_thrd.proc_cxt.DataDir[strlen(t_thrd.proc_cxt.DataDir)] == '/')
                      ? strlen(t_thrd.proc_cxt.DataDir)
                      : (strlen(t_thrd.proc_cxt.DataDir) + 1);
        pos += strlen(PG_LOCATION_DIR) + 1;
        errno_t rc = memmove_s(targetpath, MAXPGPATH, targetpath + pos, strlen(targetpath) - pos);
        securec_check(rc, "\0", "\0");
        targetpath[strlen(targetpath) - pos] = '\0';
    }
    PG_RETURN_TEXT_P(cstring_to_text(targetpath));
#else
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("tablespaces are not supported on this platform")));
    PG_RETURN_NULL();
#endif
}

/*
 * pg_sleep - delay for N seconds
 */
Datum pg_sleep(PG_FUNCTION_ARGS)
{
    float8 secs = PG_GETARG_FLOAT8(0);
    float8 endtime;

    /*
     * We break the requested sleep into segments of no more than 1 second, to
     * put an upper bound on how long it will take us to respond to a cancel
     * or die interrupt.  (Note that pg_usleep is interruptible by signals on
     * some platforms but not others.)	Also, this method avoids exposing
     * pg_usleep's upper bound on allowed delays.
     *
     * By computing the intended stop time initially, we avoid accumulation of
     * extra delay across multiple sleeps.	This also ensures we won't delay
     * less than the specified time if pg_usleep is interrupted by other
     * signals such as SIGHUP.
     */

#ifdef HAVE_INT64_TIMESTAMP
#define GetNowFloat() ((float8)GetCurrentTimestamp() / 1000000.0)
#else
#define GetNowFloat() GetCurrentTimestamp()
#endif

    endtime = GetNowFloat() + secs;

    for (;;) {
        float8 delay;

        CHECK_FOR_INTERRUPTS();
        delay = endtime - GetNowFloat();
        if (delay >= 1.0)
            pg_usleep(1000000L);
        else if (delay > 0.0)
            pg_usleep((long)ceil(delay * 1000000.0));
        else
            break;
    }

    PG_RETURN_VOID();
}

Datum pg_get_nodename(PG_FUNCTION_ARGS)
{
    if (g_instance.attr.attr_common.PGXCNodeName == NULL)
        g_instance.attr.attr_common.PGXCNodeName = "Error Node";

    PG_RETURN_TEXT_P(cstring_to_text(g_instance.attr.attr_common.PGXCNodeName));
}

/* Function to return the list of grammar keywords */
Datum pg_get_keywords(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;

    if (SRF_IS_FIRSTCALL()) {
        MemoryContext oldcontext;
        TupleDesc tupdesc;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(3, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "word", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "catcode", CHAROID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "catdesc", TEXTOID, -1, 0);

        funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < (uint32)ScanKeywords.num_keywords) {
        char* values[3];
        HeapTuple tuple;

        /* cast-away-const is ugly but alternatives aren't much better */
        values[0] = (char *)GetScanKeyword(funcctx->call_cntr,
										   &ScanKeywords);

        switch (ScanKeywordCategories[funcctx->call_cntr]) {
            case UNRESERVED_KEYWORD:
                values[1] = "U";
                values[2] = _("unreserved");
                break;
            case COL_NAME_KEYWORD:
                values[1] = "C";
                values[2] = _("unreserved (cannot be function or type name)");
                break;
            case TYPE_FUNC_NAME_KEYWORD:
                values[1] = "T";
                values[2] = _("reserved (can be function or type name)");
                break;
            case RESERVED_KEYWORD:
                values[1] = "R";
                values[2] = _("reserved");
                break;
            default: /* shouldn't be possible */
                values[1] = NULL;
                values[2] = NULL;
                break;
        }

        tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
    }

    SRF_RETURN_DONE(funcctx);
}

/*
 * Return the type of the argument.
 */
Datum pg_typeof(PG_FUNCTION_ARGS)
{
    PG_RETURN_OID(get_fn_expr_argtype(fcinfo->flinfo, 0));
}

/*
 * Implementation of the COLLATE FOR expression; returns the collation
 * of the argument.
 */
Datum pg_collation_for(PG_FUNCTION_ARGS)
{
    Oid typeId;
    Oid collid;

    typeId = get_fn_expr_argtype(fcinfo->flinfo, 0);
    if (!typeId)
        PG_RETURN_NULL();
    if (!type_is_collatable(typeId) && typeId != UNKNOWNOID)
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("collations are not supported by type %s", format_type_be(typeId))));

    collid = PG_GET_COLLATION();
    if (!collid)
        PG_RETURN_NULL();
    PG_RETURN_TEXT_P(cstring_to_text(generate_collation_name(collid)));
}

Datum pg_test_err_contain_err(PG_FUNCTION_ARGS)
{
    u_sess->utils_cxt.test_err_type = PG_GETARG_INT32(0);

    switch (u_sess->utils_cxt.test_err_type) {
        case 1: {
            PG_TRY();
            {
                ereport(ERROR, (errcode(ERRCODE_DIAGNOSTICS_EXCEPTION), errmsg_internal("ERROR RETHROW")));
            }
            PG_CATCH();
            {
                PG_RE_THROW();
            }
            PG_END_TRY();

            break;
        }

        case 2: {
            PG_TRY();
            {
                ereport(ERROR, (errcode(ERRCODE_DIAGNOSTICS_EXCEPTION), errmsg_internal("ERROR")));
            }
            PG_CATCH();
            {
                ereport(ERROR, (errcode(ERRCODE_DIAGNOSTICS_EXCEPTION), errmsg_internal("ERROR CATCH")));
            }
            PG_END_TRY();

            break;
        }

        case 3: {
            PG_TRY();
            {
                ereport(ERROR, (errcode(ERRCODE_DIAGNOSTICS_EXCEPTION), errmsg_internal("ERR ERR RETHOW")));
            }
            PG_CATCH();
            {
                PG_RE_THROW();
            }
            PG_END_TRY();

            break;
        }

        case 4: {
            PG_TRY();
            {
                ereport(ERROR, (errcode(ERRCODE_DIAGNOSTICS_EXCEPTION), errmsg_internal("ERR ERR RETHOW")));
            }
            PG_CATCH();
            {
                ereport(ERROR, (errcode(ERRCODE_DIAGNOSTICS_EXCEPTION), errmsg_internal("ERR ERR CATCH")));
            }
            PG_END_TRY();

            break;
        }

        default:
            break;
    }

    PG_RETURN_VOID();
}

// Signal to cancel a backend process. This is allowed if you are superuser
// or have the same role as the process being canceled.
void cancel_backend(ThreadId       pid)
{
    int sig_return = 0;

    sig_return = pg_signal_backend(pid, SIGINT, true);

    if (sig_return == SIGNAL_BACKEND_NOPERMISSION) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("fail to drop the user"),
                errhint("fail to cancel backend process for privilege")));
    }
}

/*
 * SQL wrapper around RelationGetReplicaIndex().
 */
Datum pg_get_replica_identity_index(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);
    Oid idxoid;
    Relation rel;

    rel = heap_open(reloid, AccessShareLock);
    idxoid = RelationGetReplicaIndex(rel);
    heap_close(rel, AccessShareLock);

    if (OidIsValid(idxoid))
        PG_RETURN_OID(idxoid);
    else
        PG_RETURN_NULL();
}

/* Compatible with ROW_COUNT functions of B database */
Datum b_database_row_count(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(u_sess->statement_cxt.last_row_count);
}

/*
 * pg_relation_is_updatable - determine which update events the specified
 * relation supports.
 *
 * This relies on relation_is_updatable() in rewriteHandler.c, which see
 * for additional information.
 */
Datum pg_relation_is_updatable(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);
    bool include_triggers = PG_GETARG_BOOL(1);

    PG_RETURN_INT32(relation_is_updatable(reloid, include_triggers, NULL));
}

/*
 * pg_column_is_updatable - determine whether a column is updatable
 *
 * This function encapsulates the decision about just what
 * information_schema.columns.is_updatable actually means.	It's not clear
 * whether deletability of the column's relation should be required, so
 * we want that decision in C code where we could change it without initdb.
 */
Datum pg_column_is_updatable(PG_FUNCTION_ARGS)
{
    Oid reloid = PG_GETARG_OID(0);
    AttrNumber attnum = PG_GETARG_INT16(1);
    AttrNumber col = attnum - FirstLowInvalidHeapAttributeNumber;
    bool include_triggers = PG_GETARG_BOOL(2);
    int events;

    /* System columns are never updatable */
    if (attnum <= 0)
        PG_RETURN_BOOL(false);

    events = relation_is_updatable(reloid, include_triggers, bms_make_singleton(col));

    /* We require both updatability and deletability of the relation */
#define REQ_EVENTS ((1 << CMD_UPDATE) | (1 << CMD_DELETE))

    PG_RETURN_BOOL((events & REQ_EVENTS) == REQ_EVENTS);
}
