/* -------------------------------------------------------------------------
 *
 * superuser.c
 *	  The superuser() function.  Determines if user has superuser privilege.
 *
 * All code should use either of these two functions to find out
 * whether a given user is a superuser, rather than examining
 * pg_authid.rolsuper directly, so that the escape hatch built in for
 * the single-user case works.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/misc/superuser.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "catalog/pg_authid.h"
#include "utils/inval.h"
#include "utils/syscache.h"
#include "miscadmin.h"

static void RoleidCallback(Datum arg, int cacheid, uint32 hashvalue);
static void cacheSuperOrSysadmin(Oid roleid);

/* Database Security:  Support separation of privilege. */
/*
 * when privileges separation is used,current user is or superuser or sysdba,if not,current user is superuser.
 */
bool superuser(void)
{
    return superuser_arg(GetUserId()) || systemDBA_arg(GetUserId());
}

/*
 * current user is real superuser.when  privileges separation is used and you don't want sysdba to entitle to operate,use
 * it.
 */
bool isRelSuperuser(void)
{
    return superuser_arg(GetUserId());
}

/*
 * current user is inital user.
 */
bool initialuser(void)
{
    return superuser_arg_no_seperation(GetUserId());
}

/*
 * The specified role has Postgres superuser privileges, not counting priviledge seperation.
 */
bool superuser_arg_no_seperation(Oid roleid)
{
    if (OidIsValid(u_sess->utils_cxt.last_roleid) && u_sess->utils_cxt.last_roleid == roleid) {
        return u_sess->utils_cxt.last_roleid_is_super;
    }

    /* Special escape path in case you deleted all your users. */
    if (!IsUnderPostmaster && roleid == BOOTSTRAP_SUPERUSERID)
        return true;

    /* Quick out for cache hit */
    cacheSuperOrSysadmin(roleid);

    return u_sess->utils_cxt.last_roleid_is_super;
}

/*
 * The specified role has Postgres superuser privileges
 */
bool superuser_arg(Oid roleid)
{
    if (OidIsValid(u_sess->utils_cxt.last_roleid) && u_sess->utils_cxt.last_roleid == roleid) {
        if (!g_instance.attr.attr_security.enablePrivilegesSeparate)
            return u_sess->utils_cxt.last_roleid_is_super || u_sess->utils_cxt.last_roleid_is_sysdba;
        else
            return u_sess->utils_cxt.last_roleid_is_super;
    }

    /* Special escape path in case you deleted all your users. */
    if (!IsUnderPostmaster && roleid == BOOTSTRAP_SUPERUSERID)
        return true;

    /* Quick out for cache hit */
    cacheSuperOrSysadmin(roleid);

    if (!g_instance.attr.attr_security.enablePrivilegesSeparate)
        return u_sess->utils_cxt.last_roleid_is_super || u_sess->utils_cxt.last_roleid_is_sysdba;
    else
        return u_sess->utils_cxt.last_roleid_is_super;
}

/*
 * The specified role has Postgres sysdba privileges
 */
bool systemDBA_arg(Oid roleid)
{
    if (OidIsValid(u_sess->utils_cxt.last_roleid) && u_sess->utils_cxt.last_roleid == roleid) {
        return u_sess->utils_cxt.last_roleid_is_sysdba;
    }

    /* Special escape path in case you deleted all your users. */
    if (!IsUnderPostmaster && roleid == BOOTSTRAP_SUPERUSERID)
        return true;

    /* Quick out for cache hit */
    cacheSuperOrSysadmin(roleid);

    return u_sess->utils_cxt.last_roleid_is_sysdba;
}

/*
 * @Description: check whether a security admin.
 * @in roleid : the role oid need check.
 * @return : return true if the role is a security admin, otherwise return false.
 */
bool isSecurityadmin(Oid roleid)
{
    if (OidIsValid(u_sess->utils_cxt.last_roleid) && u_sess->utils_cxt.last_roleid == roleid) {
        return u_sess->utils_cxt.last_roleid_is_securityadmin;
    }

    /* Special escape path in case you deleted all your users. */
    if (!IsUnderPostmaster && roleid == BOOTSTRAP_SUPERUSERID)
        return true;

    /* Quick out for cache hit */
    cacheSuperOrSysadmin(roleid);

    return u_sess->utils_cxt.last_roleid_is_securityadmin;
}

/*
 * @Description: check whether an audit admin.
 * @in roleid : the role oid need check.
 * @return : return true if the role is an audit admin, otherwise return false.
 */
bool isAuditadmin(Oid roleid)
{
    if (OidIsValid(u_sess->utils_cxt.last_roleid) && u_sess->utils_cxt.last_roleid == roleid) {
        return u_sess->utils_cxt.last_roleid_is_auditadmin;
    }

    /* Special escape path in case you deleted all your users. */
    if (!IsUnderPostmaster && roleid == BOOTSTRAP_SUPERUSERID)
        return true;

    /* Quick out for cache hit */
    cacheSuperOrSysadmin(roleid);

    return u_sess->utils_cxt.last_roleid_is_auditadmin;
}

/*
 * @Description: check whether an user have privilege to use execute direct.
 * @in query : use for check auditor query
 * @return : return true if the role is an superuser or auditor use pg_query_audit(), otherwise return false.
 */
bool CheckExecDirectPrivilege(const char* query)
{
    bool isAuditFunc = true;
    char* auditFuncPos = NULL;
    char* tmp_query = NULL;
    int offset;

    /* access for superuser */
    if (superuser()) {
        return true;
    }

    /* reject for common user */
    if (!isAuditadmin(GetUserId())) {
        return false;
    }

    tmp_query = pstrdup(query);
    tmp_query = pg_strtolower(tmp_query);
    auditFuncPos = strstr(tmp_query, " pg_query_audit");
    offset = strlen(" pg_query_audit");

    if (auditFuncPos != NULL) {
        auditFuncPos += offset;
        while (*auditFuncPos != '\0' && *auditFuncPos != '(') {
            /* only allow space, tab, enter appear */
            if (*auditFuncPos == ' ' || *auditFuncPos == '\n' || *auditFuncPos == '\t') {
                auditFuncPos++;
                continue;
            }

            isAuditFunc = false;
            break;
        }
        /* check wether it is a function name */
        if (*auditFuncPos == '\0') {
            isAuditFunc = false;
        }
    } else {
        isAuditFunc = false;
    }
    pfree(tmp_query);

    return isAuditFunc;
}

static void cacheSuperOrSysadmin(Oid roleid)
{
    HeapTuple rtup = NULL;

    u_sess->utils_cxt.last_roleid_is_super = false;
    u_sess->utils_cxt.last_roleid_is_sysdba = false;
    u_sess->utils_cxt.last_roleid_is_auditadmin = false;
    u_sess->utils_cxt.last_roleid_is_securityadmin = false;

    /* OK, look up the information in pg_authid */
    rtup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
    if (HeapTupleIsValid(rtup)) {
        if (((Form_pg_authid)GETSTRUCT(rtup))->rolsuper) {
            u_sess->utils_cxt.last_roleid_is_super = true;
        }

        if (((Form_pg_authid)GETSTRUCT(rtup))->rolsystemadmin) {
            u_sess->utils_cxt.last_roleid_is_sysdba = true;
        }

        if (((Form_pg_authid)GETSTRUCT(rtup))->rolcreaterole) {
            u_sess->utils_cxt.last_roleid_is_securityadmin = true;
        }

        if (((Form_pg_authid)GETSTRUCT(rtup))->rolauditadmin) {
            u_sess->utils_cxt.last_roleid_is_auditadmin = true;
        }

        ReleaseSysCache(rtup);
    }

    /* If first time through, set up callback for cache flushes */
    if (!u_sess->utils_cxt.roleid_callback_registered) {
        CacheRegisterSyscacheCallback(AUTHOID, RoleidCallback, (Datum)0);
        u_sess->utils_cxt.roleid_callback_registered = true;
    }

    /* Cache the result for next time */
    u_sess->utils_cxt.last_roleid = roleid;
}

/*
 * RoleidCallback
 *		Syscache inval callback function
 */
static void RoleidCallback(Datum arg, int cacheid, uint32 hashvalue)
{
    /* Invalidate our local cache in case role's superuserness changed */
    u_sess->utils_cxt.last_roleid = InvalidOid;
}
