/* -------------------------------------------------------------------------
 *
 * packagecmds.cpp
 *
 *	  Routines for CREATE and DROP PACKAGE commands and CREATE and DROP
 *	  CAST commands.
 *
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/functioncmds.cpp
 *
 * DESCRIPTION
 *	  These routines take the parse tree and pick out the
 *	  appropriate arguments/flags, and pass the results to the
 *	  corresponding "FooDefine" routines (in src/catalog) that do
 *	  the actual catalog-munging.  These routines also verify permission
 *	  of the user to execute the command.
 *
 * NOTES
 *	  These things must be defined and committed in the following order:
 *		"create package":
 *				input/output, recv/send procedures
 *		"create type":
 *				type
 *		"create operator":
 *				operators
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_control.h"
#include "catalog/pg_namespace.h"
#include "catalog/gs_package.h"
#include "catalog/gs_package_fn.h"
#include "commands/defrem.h"
#include "pgxc/pgxc.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "tcop/utility.h"


void CreatePackageCommand(CreatePackageStmt* stmt, const char* queryString)
{
#ifdef ENABLE_MULTIPLE_NODES
        ereport(ERROR, (errcode(ERRCODE_INVALID_PACKAGE_DEFINITION),
                    errmsg("not support create package in distributed database")));
#endif
    Oid packageId;
    Oid namespaceId;
    char* pkgname = NULL;
    char* pkgspecsrc = NULL;
    Oid pkgOwner;
    AclResult aclresult;

    /* Convert list of names to a name and namespace */
    namespaceId = QualifiedNameGetCreationNamespace(stmt->pkgname, &pkgname);
    if (!OidIsValid(namespaceId)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PACKAGE_DEFINITION),
                    errmsg("Invalid namespace"))); 
    }

    if (u_sess->attr.attr_sql.sql_compatibility != A_FORMAT) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PACKAGE_DEFINITION),
                    errmsg("Package only allowed create in A compatibility"))); 
    }
    /*
     * isAlter is true, change the owner of the objects as the owner of the
     * namespace, if the owner of the namespce has the same name as the namescpe
     */
    bool isAlter = false;

    /* Check we have creation rights in target namespace */
    aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(namespaceId));

    //@Temp Table. Lock Cluster after determine whether is a temp object,
    // so we can decide if locking other coordinator
    pgxc_lock_for_utility_stmt((Node*)stmt, namespaceId == u_sess->catalog_cxt.myTempNamespace);

    pkgspecsrc = stmt->pkgspec;

    if (u_sess->attr.attr_sql.enforce_a_behavior) {
        pkgOwner = GetUserIdFromNspId(namespaceId);

        if (!OidIsValid(pkgOwner))
            pkgOwner = GetUserId();

        else if (pkgOwner != GetUserId())
            isAlter = true;

        if (isAlter) {
            aclresult = pg_namespace_aclcheck(namespaceId, pkgOwner, ACL_CREATE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(namespaceId));
        }
    } else {
        pkgOwner = GetUserId();
    }

    /* Create the package */
    if (pkgname == NULL) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL), errcode(ERRCODE_INVALID_NAME),
                errmsg("package name is null"),
                errdetail("empty package name is not allowed"),
                errcause("package name is null"),
                erraction("rename package")));
    }
    packageId = PackageSpecCreate(namespaceId, pkgname, pkgOwner, pkgspecsrc, stmt->replace);

    /* Advance cmd counter to make the package visible */
    CommandCounterIncrement();
}


void CreatePackageBodyCommand(CreatePackageBodyStmt* stmt, const char* queryString)
{
#ifdef ENABLE_MULTIPLE_NODES
        ereport(ERROR, (errcode(ERRCODE_INVALID_PACKAGE_DEFINITION),
                    errmsg("not support create package in distributed database")));
#endif
    //Oid packageId;
    Oid namespaceId;
    char* pkgname = NULL;
    char* pkgBodySrc = NULL;
    Oid pkgOwner;
    AclResult aclresult;

    /* Convert list of names to a name and namespace */
    namespaceId = QualifiedNameGetCreationNamespace(stmt->pkgname, &pkgname);

    /*
     * isAlter is true, change the owner of the objects as the owner of the
     * namespace, if the owner of the namespce has the same name as the namescpe
     */
    bool isAlter = false;

    /* Check we have creation rights in target namespace */
    aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(namespaceId));

    //@Temp Table. Lock Cluster after determine whether is a temp object,
    // so we can decide if locking other coordinator
    pgxc_lock_for_utility_stmt((Node*)stmt, namespaceId == u_sess->catalog_cxt.myTempNamespace);

    pkgBodySrc = stmt->pkgbody;

    if (u_sess->attr.attr_sql.enforce_a_behavior) {
        pkgOwner = GetUserIdFromNspId(namespaceId);

        if (!OidIsValid(pkgOwner))
            pkgOwner = GetUserId();
        else if (pkgOwner != GetUserId())
            isAlter = true;

        if (isAlter) {
            aclresult = pg_namespace_aclcheck(namespaceId, pkgOwner, ACL_CREATE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(namespaceId));
        }
    } else {
        pkgOwner = GetUserId();
    }

    /* Create the package */
    PackageBodyCreate(namespaceId, pkgname, pkgOwner, pkgBodySrc , stmt->pkginit, stmt->replace);

    /* Advance cmd counter to make the package visible */
    CommandCounterIncrement();
}
