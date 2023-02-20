/* -------------------------------------------------------------------------
 *
 * pl_funcs.c		- Misc functions for the PL/pgSQL
 *			  procedural language
 *
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/pl/plpgsql/src/pl_funcs.c
 *
 * -------------------------------------------------------------------------
 */

#include "utils/plpgsql_domain.h"
#include "utils/plpgsql.h"
#include "optimizer/pgxcship.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pl_package.h"
#include "catalog/gs_package.h"
/* ----------
 * plpgsql_ns_init			Initialize namespace processing for a new function
 * ----------
 */
void plpgsql_ns_init(void)
{
    u_sess->plsql_cxt.curr_compile_context->ns_top = NULL;
}

/* ----------
 * plpgsql_ns_push			Create a new namespace level
 * ----------
 */
void plpgsql_ns_push(const char* label)
{
    if (label == NULL) {
        label = "";
    }
    plpgsql_ns_additem(PLPGSQL_NSTYPE_LABEL, 0, label);
}

void add_pkg_compile()
{
    if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL)  {
        plpgsql_add_pkg_ns(u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package);
    }
}
/*
 * add package variable to namespace when compile a function whcih in package
 * it will add all package variable,including private variable, so it can't use
 * in other function's compile.
 */
void plpgsql_add_pkg_ns(PLpgSQL_package* pkg) 
{
    PLpgSQL_datum			*datum;
    int                     varno;
    int                     ndatums;

    if (pkg->ndatums == 0) {
        ndatums = pkg->public_ndatums;
    } else {
         ndatums = pkg->ndatums;
    }

    for (int i = 0;i < ndatums; i++) {
        datum = pkg->datums[i];
        if (datum == NULL) {
            ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_NONEXISTANT_VARIABLE),
                errmsg("unrecognized package variable")));
        }
        char* pkgname = pkg->pkg_signature;
        char* objname = ((PLpgSQL_var*) datum)->refname;
        if (datum != NULL) {
            datum->ispkg = true;
            varno = plpgsql_adddatum(datum, false);
            switch (datum->dtype) {
                case PLPGSQL_DTYPE_VAR:
                    if (((PLpgSQL_var*)datum)->addNamespace) {
                        plpgsql_ns_additem(PLPGSQL_NSTYPE_VAR, varno, objname, pkgname);
                    }
                    break;
                case PLPGSQL_DTYPE_ROW:
                    if (((PLpgSQL_row*)datum)->addNamespace) {
                        plpgsql_ns_additem(PLPGSQL_NSTYPE_ROW, varno, objname, pkgname);
                    }
                    break;
                case PLPGSQL_DTYPE_REC:
                    if (((PLpgSQL_rec*)datum)->addNamespace) {
                        plpgsql_ns_additem(PLPGSQL_NSTYPE_REC, varno, objname, pkgname);
                    }
                    break;
                case PLPGSQL_DTYPE_RECORD:
                    if (((PLpgSQL_rec*)datum)->addNamespace) {
                        plpgsql_ns_additem(PLPGSQL_NSTYPE_ROW, varno, objname, pkgname);
                    }
                    break;
                case PLPGSQL_DTYPE_RECORD_TYPE:
                    if (((PLpgSQL_rec_type*)datum)->addNamespace) {
                        plpgsql_ns_additem(PLPGSQL_NSTYPE_RECORD, varno, objname, pkgname);
                    }
                    break;
                case PLPGSQL_DTYPE_VARRAY:
                    plpgsql_ns_additem(PLPGSQL_NSTYPE_VARRAY, varno, objname, pkgname);
                    break;
                case PLPGSQL_DTYPE_TABLE:
                    plpgsql_ns_additem(PLPGSQL_NSTYPE_TABLE, varno, objname, pkgname);
                    break;
                case PLPGSQL_DTYPE_UNKNOWN:
                    plpgsql_ns_additem(PLPGSQL_NSTYPE_UNKNOWN, varno, objname, pkgname);
                    break;
                case PLPGSQL_DTYPE_COMPOSITE:
                    break;    
                default:
                    ereport(ERROR, (errmodule(MOD_PLSQL), errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized ttype: %d, when build variable in PLSQL, this situation should not occur.",
                            datum->dtype)));
            }
        }

    }
}

/* ----------
 * plpgsql_ns_pop			Pop entries back to (and including) the last label
 * ----------
 */
void plpgsql_ns_pop(void)
{
    AssertEreport(u_sess->plsql_cxt.curr_compile_context->ns_top != NULL, MOD_PLSQL, "ns_top should not be null.");
    while (u_sess->plsql_cxt.curr_compile_context->ns_top->itemtype != PLPGSQL_NSTYPE_LABEL) {
        u_sess->plsql_cxt.curr_compile_context->ns_top = u_sess->plsql_cxt.curr_compile_context->ns_top->prev;
    }
    u_sess->plsql_cxt.curr_compile_context->ns_top = u_sess->plsql_cxt.curr_compile_context->ns_top->prev;
}

/* ----------
 * plpgsql_ns_top			Fetch the current namespace chain end
 * ----------
 */
PLpgSQL_nsitem* plpgsql_ns_top(void)
{
    return u_sess->plsql_cxt.curr_compile_context->ns_top;
}

/* ----------
 * plpgsql_ns_additem		Add an item to the current namespace chain
 * ----------
 */
void plpgsql_ns_additem(int itemtype, int itemno, const char* name, const char* pkgname,  const char* schemaName)
{
    PLpgSQL_nsitem* nse = NULL;
    errno_t rc;
    MemoryContext temp = NULL;

    AssertEreport(name != NULL, MOD_PLSQL, "name should not be null");
    Assert(u_sess->plsql_cxt.curr_compile_context);
    PLpgSQL_compile_context* curr_compile = u_sess->plsql_cxt.curr_compile_context;
    /* first item added must be a label */
    AssertEreport(curr_compile->ns_top != NULL || itemtype == PLPGSQL_NSTYPE_LABEL, MOD_PLSQL, "");

    if (curr_compile->plpgsql_curr_compile_package != NULL) {
        checkCompileMemoryContext(curr_compile->plpgsql_curr_compile_package->pkg_cxt);
        temp = MemoryContextSwitchTo(curr_compile->plpgsql_curr_compile_package->pkg_cxt);
    }

    size_t nameLength = strlen(name);
    size_t nseNameSize = nameLength + 1;
    nse = (PLpgSQL_nsitem*)palloc(offsetof(PLpgSQL_nsitem, name) + (nseNameSize) * sizeof(char));
    nse->itemtype = itemtype;
    nse->itemno = itemno;
    nse->prev = curr_compile->ns_top;
    rc = strncpy_s(nse->name, nseNameSize, name, nameLength);
    securec_check_c(rc, "\0", "\0");

    if (pkgname != NULL) {
        size_t pkgNameLength = strlen(pkgname);
        size_t nsePkgNameSize = pkgNameLength + 1;
        nse->pkgname = (char*)palloc(sizeof(char) * nsePkgNameSize);
        rc = strncpy_s(nse->pkgname, nsePkgNameSize, pkgname, pkgNameLength);
        securec_check_c(rc, "\0", "\0");
    } else {
        nse->pkgname = NULL;
    }
    
    if (schemaName != NULL) {
        size_t schemaNameLength = strlen(schemaName);
        size_t nseSchemaNameSize = schemaNameLength + 1;
        nse->schemaName = (char*)palloc(sizeof(char) * nseSchemaNameSize);
        rc = strncpy_s(nse->schemaName, nseSchemaNameSize, schemaName, schemaNameLength);
        securec_check_c(rc, "\0", "\0");
    } else if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package) {
        PLpgSQL_package* curr_package = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
        if (!OidIsValid(curr_package->namespaceOid)) {
            Oid pkg_oid = curr_package->pkg_oid;
            Assert(OidIsValid(pkg_oid));
            curr_package->namespaceOid = GetPackageNamespace(pkg_oid);
        }
        nse->schemaName = get_namespace_name(curr_package->namespaceOid);
    } else {
        nse->schemaName = NULL;
    }

    curr_compile->ns_top = nse;
    if (curr_compile->plpgsql_curr_compile_package != NULL) {
        temp = MemoryContextSwitchTo(temp);
    }
}

static bool IsSameSchema(const char* schemaName, const char* currCompilePackageSchemaName)
{
    if (currCompilePackageSchemaName && strcmp(schemaName, currCompilePackageSchemaName) == 0) {
        return true;
    }
    return false;
}

static char* GetPackageSchemaName()
{
    Oid packageSchemaOid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->namespaceOid;
    char* packageSchemaName = get_namespace_name(packageSchemaOid);
    return packageSchemaName;
}

char* GetPackageSchemaName(Oid packageOid)
{
    Oid packageSchemaOid = GetPackageNamespace(packageOid);
    char* packageSchemaName = get_namespace_name(packageSchemaOid);
    return packageSchemaName;
}

/* ----------
 * plpgsql_ns_lookup		Lookup an identifier in the given namespace chain
 *
 * Note that this only searches for variables, not labels.
 *
 * If localmode is TRUE, only the topmost block level is searched.
 *
 * name1 must be non-NULL.	Pass NULL for name2 and/or name3 if parsing a name
 * with fewer than three components.
 *
 * If names_used isn't NULL, *names_used receives the number of names
 * matched: 0 if no match, 1 if name1 matched an unqualified variable name,
 * 2 if name1 and name2 matched a block label + variable name.
 *
 * Note that name3 is never directly matched to anything.  However, if it
 * isn't NULL, we will disregard qualified matches to scalar variables.
 * Similarly, if name2 isn't NULL, we disregard unqualified matches to
 * scalar variables.
 * ----------
 *
 * name1.name2.name3
 * name1:
 *      (var_name)
 * name1.name2:
 *      (package_name.var_name)
 *      (record_name.column_name)
 * name1.name2.name3:
 *      (schema_name.package_name.var_name)
 *      (schema_name.record_name.column_name)
 */
PLpgSQL_nsitem* plpgsql_ns_lookup(
    PLpgSQL_nsitem* ns_cur, bool localmode, const char* name1, const char* name2, const char* name3, int* names_used)
{
    /* Outer loop iterates once per block level in the namespace chain */
    int rc = 0;
    char* currCompilePackageName = NULL;
    char* currCompilePackageSchemaName = NULL;
    rc = CompileWhich();
    if (OidIsValid(u_sess->plsql_cxt.running_pkg_oid)) {
        char* CompilePackageName = GetPackageName(u_sess->plsql_cxt.running_pkg_oid);
        currCompilePackageName = CompilePackageName;
        currCompilePackageSchemaName = GetPackageSchemaName(u_sess->plsql_cxt.running_pkg_oid);
    } else if (rc == PLPGSQL_COMPILE_PACKAGE_PROC || rc == PLPGSQL_COMPILE_PACKAGE) {
        currCompilePackageName = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_signature;
        currCompilePackageSchemaName = GetPackageSchemaName();
    }
    while (ns_cur != NULL) {
        PLpgSQL_nsitem* nsitem = NULL;
        /* Check this level for unqualified match to variable name */
        for (nsitem = ns_cur; nsitem->itemtype != PLPGSQL_NSTYPE_LABEL; nsitem = nsitem->prev) {
            if (name1 == NULL) {
                continue;
            }
            bool isSamePackage = false;
            if (currCompilePackageName != NULL && nsitem->pkgname != NULL) {
                if (strcmp(currCompilePackageName, nsitem->pkgname) == 0) {
                    if (nsitem->schemaName == NULL) {
                        /* By default, we assume package is same if schema is empty. */
                        isSamePackage = true;
                    } else {
                        isSamePackage = IsSameSchema(nsitem->schemaName, currCompilePackageSchemaName);
                    } 
                }
            }
            if (strcmp(nsitem->name, name1) == 0) {
                bool isContinue = nsitem->pkgname != NULL && !isSamePackage;
                if (isContinue) {
                    continue;
                }
                if (name2 == NULL || nsitem->itemtype != PLPGSQL_NSTYPE_VAR) {
                    if (names_used != NULL) {
                        *names_used = 1;
                    }
                    pfree_ext(currCompilePackageSchemaName);
                    return nsitem;
                }
            }
            if (name2 == NULL) {
                continue;
            }
            if (nsitem->pkgname != NULL && strcmp(nsitem->pkgname, name1) == 0) {
                if (strcmp(nsitem->name, name2) == 0 && (name3 == NULL ||
                    nsitem->itemtype != PLPGSQL_NSTYPE_VAR)) {
                    if (names_used != NULL) {
                        *names_used = 2;
                    }
                    pfree_ext(currCompilePackageSchemaName);
                    return nsitem;
                }
            }
            if (name3 == NULL) {
                continue;
            }
            if (nsitem->schemaName != NULL && (strcmp(nsitem->schemaName, name1) == 0)) {
                if (nsitem->pkgname == NULL) {
                    continue;
                } else if (!(strcmp(nsitem->pkgname, name2) == 0)) {
                    continue;
                }
                if (strcmp(nsitem->name, name3) == 0) {
                    if (names_used != NULL) {
                        *names_used = 3;
                    }
                    pfree_ext(currCompilePackageSchemaName);
                    return nsitem;
                }
            }
        }

        /* Check this level for qualified match to variable name */

        if (name1 != NULL && name2 != NULL && strcmp(nsitem->name, name1) == 0) {
            for (nsitem = ns_cur; nsitem->itemtype != PLPGSQL_NSTYPE_LABEL; nsitem = nsitem->prev) {
                if (strcmp(nsitem->name, name2) == 0) {
                    if (name3 == NULL || nsitem->itemtype != PLPGSQL_NSTYPE_VAR) {
                        if (names_used != NULL) {
                            *names_used = 2;
                        }
                        pfree_ext(currCompilePackageSchemaName);
                        return nsitem;
                    }
                }
            }
        }

        if (localmode) {
            break; /* do not look into upper levels */
        }

        ns_cur = nsitem->prev;
    }

    /* This is just to suppress possibly-uninitialized-variable warnings */
    if (names_used != NULL) {
        *names_used = 0;
    }
    pfree_ext(currCompilePackageSchemaName);
    return NULL; /* No match found */
}

PLpgSQL_datum* plpgsql_lookup_datum(
    bool localmode, const char* name1, const char* name2, const char* name3, int* names_used)
{
    if (u_sess->plsql_cxt.curr_compile_context == NULL) {
        return NULL;
    }

    PLpgSQL_nsitem* ns = plpgsql_ns_lookup(plpgsql_ns_top(), localmode, name1, name2, name3, names_used);
    if (ns == NULL) {
        /* find from pkg */
        if (u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL) {
            PLpgSQL_package* pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
            ns = plpgsql_ns_lookup(pkg->public_ns, localmode, name1, name2, name3, names_used);
            if (ns == NULL) {
                ns = plpgsql_ns_lookup(pkg->private_ns, localmode, name1, name2, name3, names_used);
            }
            if (ns != NULL) {
                return pkg->datums[ns->itemno];
            }
        }
    } else {
        return u_sess->plsql_cxt.curr_compile_context->plpgsql_Datums[ns->itemno];
    }

    return NULL;
}
/* ----------
 * plpgsql_ns_lookup_label		Lookup a label in the given namespace chain
 * ----------
 */
PLpgSQL_nsitem* plpgsql_ns_lookup_label(PLpgSQL_nsitem* ns_cur, const char* name)
{
    while (ns_cur != NULL) {
        if (ns_cur->itemtype == PLPGSQL_NSTYPE_LABEL && strcmp(ns_cur->name, name) == 0) {
            return ns_cur;
        }
        ns_cur = ns_cur->prev;
    }

    return NULL; /* label not found */
}

static const char* plpgsql_savepoint_typename(PLpgSQL_stmt_savepoint *stmt)
{
    switch (stmt->opType) {
        case PLPGSQL_SAVEPOINT_CREATE:
            return "SAVEPOINT";
        case PLPGSQL_SAVEPOINT_ROLLBACKTO:
            return "ROLLBACK TO SAVEPOINT";
        case PLPGSQL_SAVEPOINT_RELEASE:
            return "RELEASE SAVEPOINT";
        default:
            return "unknown savepoint";
    }
}

/*
 * Statement type as a string, for use in error messages etc.
 */
const char* plpgsql_stmt_typename(PLpgSQL_stmt* stmt)
{
    switch ((enum PLpgSQL_stmt_types)stmt->cmd_type) {
        case PLPGSQL_STMT_BLOCK:
            return _("statement block");
        case PLPGSQL_STMT_ASSIGN:
            return _("assignment");
        case PLPGSQL_STMT_IF:
            return "IF";
        case PLPGSQL_STMT_GOTO:
            return "GOTO";
        case PLPGSQL_STMT_CASE:
            return "CASE";
        case PLPGSQL_STMT_LOOP:
            return "LOOP";
        case PLPGSQL_STMT_WHILE:
            if(((PLpgSQL_stmt_while*)stmt)->condition)
                return "WHILE";
            else
                return "REPEAT";
        case PLPGSQL_STMT_FORI:
            return _("FOR with integer loop variable");
        case PLPGSQL_STMT_FORS:
            return _("FOR over SELECT rows");
        case PLPGSQL_STMT_FORC:
            return _("FOR over cursor");
        case PLPGSQL_STMT_FOREACH_A:
            return _("FOREACH over array");
        case PLPGSQL_STMT_EXIT:
            return ((PLpgSQL_stmt_exit*)stmt)->is_exit ? "EXIT" : "CONTINUE";
        case PLPGSQL_STMT_RETURN:
            return "RETURN";
        case PLPGSQL_STMT_RETURN_NEXT:
            return "RETURN NEXT";
        case PLPGSQL_STMT_RETURN_QUERY:
            return "RETURN QUERY";
        case PLPGSQL_STMT_RAISE:
            return "RAISE";
        case PLPGSQL_STMT_EXECSQL:
            return _("SQL statement");
        case PLPGSQL_STMT_DYNEXECUTE:
            return _("EXECUTE statement");
        case PLPGSQL_STMT_DYNFORS:
            return _("FOR over EXECUTE statement");
        case PLPGSQL_STMT_GETDIAG:
            return "GET DIAGNOSTICS";
        case PLPGSQL_STMT_OPEN:
            return "OPEN";
        case PLPGSQL_STMT_FETCH:
            return ((PLpgSQL_stmt_fetch*)stmt)->is_move ? "MOVE" : "FETCH";
        case PLPGSQL_STMT_CLOSE:
            return "CLOSE";
        case PLPGSQL_STMT_PERFORM:
            return "PERFORM";
        case PLPGSQL_STMT_NULL:
            return "NULL";
        case PLPGSQL_STMT_COMMIT:
            return "COMMIT";
        case PLPGSQL_STMT_ROLLBACK:
            return "ROLLBACK";
        case PLPGSQL_STMT_SAVEPOINT:
            return plpgsql_savepoint_typename((PLpgSQL_stmt_savepoint*)stmt);
        case PLPGSQL_STMT_SIGNAL:
            return "SIGNAL";
        case PLPGSQL_STMT_RESIGNAL:
            return "RESIGNAL";
        default:
            break;
    }

    return "unknown";
}

/*
 * GET DIAGNOSTICS item name as a string, for use in error messages etc.
 */
const char* plpgsql_getdiag_kindname(int kind)
{
    switch (kind) {
        case PLPGSQL_GETDIAG_ROW_COUNT:
            return "ROW_COUNT";
        case PLPGSQL_GETDIAG_RESULT_OID:
            return "RESULT_OID";
        case PLPGSQL_GETDIAG_ERROR_CONTEXT:
            return "PG_EXCEPTION_CONTEXT";
        case PLPGSQL_GETDIAG_ERROR_DETAIL:
            return "PG_EXCEPTION_DETAIL";
        case PLPGSQL_GETDIAG_ERROR_HINT:
            return "PG_EXCEPTION_HINT";
        case PLPGSQL_GETDIAG_RETURNED_SQLSTATE:
            return "RETURNED_SQLSTATE";
        case PLPGSQL_GETDIAG_MESSAGE_TEXT:
            return "MESSAGE_TEXT";
        default:
            break;
    }

    return "unknown";
}

/**********************************************************************
 * Release memory when a PL/pgSQL function is no longer needed
 *
 * The code for recursing through the function tree is really only
 * needed to locate PLpgSQL_expr nodes, which may contain references
 * to saved SPI Plans that must be freed.  The function tree itself,
 * along with subsidiary data, is freed in one swoop by freeing the
 * function's permanent memory context.
 **********************************************************************/
static void free_stmt(PLpgSQL_stmt* stmt);
static void free_block(PLpgSQL_stmt_block* block);
static void free_assign(PLpgSQL_stmt_assign* stmt);
static void free_if(PLpgSQL_stmt_if* stmt);
static void free_goto(PLpgSQL_stmt_goto* stmt);
static void free_case(PLpgSQL_stmt_case* stmt);
static void free_loop(PLpgSQL_stmt_loop* stmt);
static void free_while(PLpgSQL_stmt_while* stmt);
static void free_fori(PLpgSQL_stmt_fori* stmt);
static void free_fors(PLpgSQL_stmt_fors* stmt);
static void free_forc(PLpgSQL_stmt_forc* stmt);
static void free_foreach_a(PLpgSQL_stmt_foreach_a* stmt);
static void free_exit(PLpgSQL_stmt_exit* stmt);
static void free_return(PLpgSQL_stmt_return* stmt);
static void free_return_next(PLpgSQL_stmt_return_next* stmt);
static void free_return_query(PLpgSQL_stmt_return_query* stmt);
static void free_raise(PLpgSQL_stmt_raise* stmt);
static void free_execsql(PLpgSQL_stmt_execsql* stmt);
static void free_dynexecute(PLpgSQL_stmt_dynexecute* stmt);
static void free_dynfors(PLpgSQL_stmt_dynfors* stmt);
static void free_getdiag(PLpgSQL_stmt_getdiag* stmt);
static void free_open(PLpgSQL_stmt_open* stmt);
static void free_fetch(PLpgSQL_stmt_fetch* stmt);
static void free_close(PLpgSQL_stmt_close* stmt);
static void free_null(PLpgSQL_stmt_null* stmt);
static void free_perform(PLpgSQL_stmt_perform* stmt);
static void free_commit(PLpgSQL_stmt_commit *stmt);
static void free_rollback(PLpgSQL_stmt_rollback *stmt);
static void free_savepoint(PLpgSQL_stmt_savepoint *stmt);
static void free_signal(PLpgSQL_stmt_signal *stmt);

static void free_stmt(PLpgSQL_stmt* stmt)
{
    switch ((enum PLpgSQL_stmt_types)stmt->cmd_type) {
        case PLPGSQL_STMT_BLOCK:
            free_block((PLpgSQL_stmt_block*)stmt);
            break;
        case PLPGSQL_STMT_ASSIGN:
            free_assign((PLpgSQL_stmt_assign*)stmt);
            break;
        case PLPGSQL_STMT_IF:
            free_if((PLpgSQL_stmt_if*)stmt);
            break;
        case PLPGSQL_STMT_GOTO:
            free_goto((PLpgSQL_stmt_goto*)stmt);
            break;
        case PLPGSQL_STMT_CASE:
            free_case((PLpgSQL_stmt_case*)stmt);
            break;
        case PLPGSQL_STMT_LOOP:
            free_loop((PLpgSQL_stmt_loop*)stmt);
            break;
        case PLPGSQL_STMT_WHILE:
            free_while((PLpgSQL_stmt_while*)stmt);
            break;
        case PLPGSQL_STMT_FORI:
            free_fori((PLpgSQL_stmt_fori*)stmt);
            break;
        case PLPGSQL_STMT_FORS:
            free_fors((PLpgSQL_stmt_fors*)stmt);
            break;
        case PLPGSQL_STMT_FORC:
            free_forc((PLpgSQL_stmt_forc*)stmt);
            break;
        case PLPGSQL_STMT_FOREACH_A:
            free_foreach_a((PLpgSQL_stmt_foreach_a*)stmt);
            break;
        case PLPGSQL_STMT_EXIT:
            free_exit((PLpgSQL_stmt_exit*)stmt);
            break;
        case PLPGSQL_STMT_RETURN:
            free_return((PLpgSQL_stmt_return*)stmt);
            break;
        case PLPGSQL_STMT_RETURN_NEXT:
            free_return_next((PLpgSQL_stmt_return_next*)stmt);
            break;
        case PLPGSQL_STMT_RETURN_QUERY:
            free_return_query((PLpgSQL_stmt_return_query*)stmt);
            break;
        case PLPGSQL_STMT_RAISE:
            free_raise((PLpgSQL_stmt_raise*)stmt);
            break;
        case PLPGSQL_STMT_EXECSQL:
            free_execsql((PLpgSQL_stmt_execsql*)stmt);
            break;
        case PLPGSQL_STMT_DYNEXECUTE:
            free_dynexecute((PLpgSQL_stmt_dynexecute*)stmt);
            break;
        case PLPGSQL_STMT_DYNFORS:
            free_dynfors((PLpgSQL_stmt_dynfors*)stmt);
            break;
        case PLPGSQL_STMT_GETDIAG:
            free_getdiag((PLpgSQL_stmt_getdiag*)stmt);
            break;
        case PLPGSQL_STMT_OPEN:
            free_open((PLpgSQL_stmt_open*)stmt);
            break;
        case PLPGSQL_STMT_FETCH:
            free_fetch((PLpgSQL_stmt_fetch*)stmt);
            break;
        case PLPGSQL_STMT_CLOSE:
            free_close((PLpgSQL_stmt_close*)stmt);
            break;
        case PLPGSQL_STMT_PERFORM:
            free_perform((PLpgSQL_stmt_perform*)stmt);
            break;
        case PLPGSQL_STMT_COMMIT:
            free_commit((PLpgSQL_stmt_commit *) stmt);
            break;
        case PLPGSQL_STMT_ROLLBACK:
            free_rollback((PLpgSQL_stmt_rollback *) stmt);
            break;
        case PLPGSQL_STMT_NULL:
            free_null((PLpgSQL_stmt_null*)stmt);
            break;
        case PLPGSQL_STMT_SAVEPOINT:
            free_savepoint((PLpgSQL_stmt_savepoint*)stmt);
            break;
        case PLPGSQL_STMT_SIGNAL:
        case PLPGSQL_STMT_RESIGNAL:
            free_signal((PLpgSQL_stmt_signal*)stmt);
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized cmd_type: %d, when free statement", stmt->cmd_type)));
            break;
    }
}

static void free_stmts(List* stmts)
{
    ListCell* s = NULL;

    foreach (s, stmts) {
        free_stmt((PLpgSQL_stmt*)lfirst(s));
    }
}

static void free_block(PLpgSQL_stmt_block* block)
{
    free_stmts(block->body);
    if (block->exceptions != NULL) {
        ListCell* e = NULL;

        foreach (e, block->exceptions->exc_list) {
            PLpgSQL_exception* exc = (PLpgSQL_exception*)lfirst(e);

            free_stmts(exc->action);
        }
    }
}

static void free_assign(PLpgSQL_stmt_assign* stmt)
{
    free_expr(stmt->expr);
}

void free_assignlist(List* assignlist)
{
    ListCell* lc = NULL;
    foreach(lc, assignlist) {
        Node* n = (Node*)lfirst(lc);
        if (IsA(n, A_Indices)) {
            free_expr((PLpgSQL_expr*)(((A_Indices*)n)->uidx));
        }
    }
}

static void free_if(PLpgSQL_stmt_if* stmt)
{
    ListCell* l = NULL;

    free_expr(stmt->cond);
    free_stmts(stmt->then_body);
    foreach (l, stmt->elsif_list) {
        PLpgSQL_if_elsif* elif = (PLpgSQL_if_elsif*)lfirst(l);

        free_expr(elif->cond);
        free_stmts(elif->stmts);
    }
    free_stmts(stmt->else_body);
}

static void free_goto(PLpgSQL_stmt_goto* stmt)
{
    /* For GOTO, there is nothing to do */
    ereport(DEBUG1, (errmsg("There is nothing to do for free GOTO statement")));
}

static void free_case(PLpgSQL_stmt_case* stmt)
{
    ListCell* l = NULL;

    free_expr(stmt->t_expr);
    foreach (l, stmt->case_when_list) {
        PLpgSQL_case_when* cwt = (PLpgSQL_case_when*)lfirst(l);

        free_expr(cwt->expr);
        free_stmts(cwt->stmts);
    }
    free_stmts(stmt->else_stmts);
}

static void free_loop(PLpgSQL_stmt_loop* stmt)
{
    free_stmts(stmt->body);
}

static void free_while(PLpgSQL_stmt_while* stmt)
{
    free_expr(stmt->cond);
    free_stmts(stmt->body);
}

static void free_fori(PLpgSQL_stmt_fori* stmt)
{
    free_expr(stmt->lower);
    free_expr(stmt->upper);
    free_expr(stmt->step);
    free_stmts(stmt->body);
}

static void free_fors(PLpgSQL_stmt_fors* stmt)
{
    free_stmts(stmt->body);
    free_expr(stmt->query);
}

static void free_forc(PLpgSQL_stmt_forc* stmt)
{
    free_stmts(stmt->body);
    free_expr(stmt->argquery);
}

static void free_foreach_a(PLpgSQL_stmt_foreach_a* stmt)
{
    free_expr(stmt->expr);
    free_stmts(stmt->body);
}

static void free_open(PLpgSQL_stmt_open* stmt)
{
    ListCell* lc = NULL;

    free_expr(stmt->argquery);
    free_expr(stmt->query);
    free_expr(stmt->dynquery);
    foreach (lc, stmt->params) {
        free_expr((PLpgSQL_expr*)lfirst(lc));
    }
}

static void free_fetch(PLpgSQL_stmt_fetch* stmt)
{
    free_expr(stmt->expr);
}

static void free_close(PLpgSQL_stmt_close* stmt)
{}

static void free_null(PLpgSQL_stmt_null* stmt)
{
    ereport(DEBUG1, (errmsg("There is nothing to do for free NULL statement")));
}

static void free_perform(PLpgSQL_stmt_perform* stmt)
{
    free_expr(stmt->expr);
}

static void free_commit(PLpgSQL_stmt_commit *stmt)
{
}

static void free_rollback(PLpgSQL_stmt_rollback *stmt)
{
}

static void free_savepoint(PLpgSQL_stmt_savepoint *stmt)
{
}

static void free_signal(PLpgSQL_stmt_signal *stmt)
{
    ListCell* lc = NULL;

    foreach (lc, stmt->cond_info_item) {
        PLpgSQL_signal_info_item *item = (PLpgSQL_signal_info_item *)lfirst(lc);

        free_expr(item->expr);
    }
}

static void free_exit(PLpgSQL_stmt_exit* stmt)
{
    free_expr(stmt->cond);
}

static void free_return(PLpgSQL_stmt_return* stmt)
{
    free_expr(stmt->expr);
}

static void free_return_next(PLpgSQL_stmt_return_next* stmt)
{
    free_expr(stmt->expr);
}

static void free_return_query(PLpgSQL_stmt_return_query* stmt)
{
    ListCell* lc = NULL;

    free_expr(stmt->query);
    free_expr(stmt->dynquery);
    foreach (lc, stmt->params) {
        free_expr((PLpgSQL_expr*)lfirst(lc));
    }
}

static void free_raise(PLpgSQL_stmt_raise* stmt)
{
    ListCell* lc = NULL;

    foreach (lc, stmt->params) {
        free_expr((PLpgSQL_expr*)lfirst(lc));
    }
    foreach (lc, stmt->options) {
        PLpgSQL_raise_option* opt = (PLpgSQL_raise_option*)lfirst(lc);

        free_expr(opt->expr);
    }
}

static void free_execsql(PLpgSQL_stmt_execsql* stmt)
{
    free_expr(stmt->sqlstmt);
}

static void free_dynexecute(PLpgSQL_stmt_dynexecute* stmt)
{
    ListCell* lc = NULL;

    free_expr(stmt->query);
    foreach (lc, stmt->params) {
        free_expr((PLpgSQL_expr*)lfirst(lc));
    }
}

static void free_dynfors(PLpgSQL_stmt_dynfors* stmt)
{
    ListCell* lc = NULL;

    free_stmts(stmt->body);
    free_expr(stmt->query);
    foreach (lc, stmt->params) {
        free_expr((PLpgSQL_expr*)lfirst(lc));
    }
}

static void free_getdiag(PLpgSQL_stmt_getdiag* stmt)
{}

void free_expr(PLpgSQL_expr* expr)
{
    if (expr != NULL && expr->plan != NULL) {
        SPI_freeplan(expr->plan);
        expr->plan = NULL;
    }
}

void plpgsql_free_function_memory(PLpgSQL_function* func, bool fromPackage)
{
    int i;

    /* Better not call this on an in-use function */
    AssertEreport(func->use_count == 0, MOD_PLSQL, "Better not call this on an in-use function");
    /*
     * function which in package not free memory alone 
     */
    if (OidIsValid(func->pkg_oid) && !fromPackage) {
        return;
    }

    /* Release plans associated with variable declarations */
    for (i = 0; i < func->ndatums; i++) {
        PLpgSQL_datum* d = func->datums[i];
        if (d != NULL) {
            if (!func->datum_need_free[i]) {
                continue;
            }
        } else {
            continue;
        }
        switch (d->dtype) {
            case PLPGSQL_DTYPE_VARRAY:
            case PLPGSQL_DTYPE_TABLE:
            case PLPGSQL_DTYPE_VAR: {
                PLpgSQL_var* var = (PLpgSQL_var*)d;

                free_expr(var->default_val);
                free_expr(var->cursor_explicit_expr);
                if (var->tableOfIndex != NULL) {
                    hash_destroy(var->tableOfIndex);
                    var->tableOfIndex = NULL;
                }
            } break;
            case PLPGSQL_DTYPE_RECORD:
            case PLPGSQL_DTYPE_ROW: {
                PLpgSQL_row* row = (PLpgSQL_row*)d;
                free_expr(row->default_val);
            } break;
            case PLPGSQL_DTYPE_REC:
                break;
            case PLPGSQL_DTYPE_RECFIELD:
                break;
            case PLPGSQL_DTYPE_ARRAYELEM:
                free_expr(((PLpgSQL_arrayelem*)d)->subscript);
                break;
            case PLPGSQL_DTYPE_TABLEELEM:
                free_expr(((PLpgSQL_tableelem*)d)->subscript);
                break;
            case PLPGSQL_DTYPE_ASSIGNLIST:
                free_assignlist(((PLpgSQL_assignlist*)d)->assignlist);
            case PLPGSQL_DTYPE_UNKNOWN:
                break;
            case PLPGSQL_DTYPE_COMPOSITE:
                break;
            case PLPGSQL_DTYPE_RECORD_TYPE:
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_PLSQL),
                        errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized data type: %d when free function memory.", d->dtype)));
                break;
        }
    }
    func->ndatums = 0;
#ifndef ENABLE_MULTIPLE_NODES
    /* clean up debug info if necessary */
    if (func->debug) {
        clean_up_debug_server(func->debug, false, true);
        func->debug = NULL;
    }
#endif
    /* Release plans in statement tree */
    if (func->action != NULL) {
        free_block(func->action);
    }
    func->action = NULL;

    /* Release goto_labels */
    if (func->goto_labels != NIL) {
        if (func->goto_labels->length > 0) {
            list_free_ext(func->goto_labels);
            func->goto_labels = NIL;
        }
    }

    // Release searchpath
    if (func->fn_searchpath != NULL && !OidIsValid(func->pkg_oid)) {
        if (func->fn_searchpath->schemas && func->fn_searchpath->schemas->length > 0) {
            list_free_ext(func->fn_searchpath->schemas);
        }
        pfree_ext(func->fn_searchpath);
    }
    if (func->invalItems != NULL) {
        list_free_deep(func->invalItems);
        func->invalItems = NULL;
    }

    /*
     * And finally, release all memory except the PLpgSQL_function struct
     * itself (which has to be kept around because there may be multiple
     * fn_extra pointers to it).
     */
    if (u_sess->plsql_cxt.curr_compile_context != NULL &&
        func->fn_cxt == u_sess->plsql_cxt.curr_compile_context->compile_cxt)
        u_sess->plsql_cxt.curr_compile_context->compile_cxt = NULL;
    if (!OidIsValid(func->pkg_oid)) {
        if (func->fn_cxt) {
            MemoryContextDelete(func->fn_cxt);
        }
        func->fn_cxt = NULL;
    } else {
        func->fn_cxt = NULL;
    }
}

/*
 * free package memory here.
 */
void plpgsql_free_package_memory(PLpgSQL_package* pkg)
{
    int i;
    /* Release plans associated with variable declarations */
    for (i = 0; i < pkg->ndatums; i++) {
        if (!pkg->datum_need_free[i]) {
            continue;
        }
        PLpgSQL_datum* d = pkg->datums[i];
        switch (d->dtype) {
            case PLPGSQL_DTYPE_VARRAY:
            case PLPGSQL_DTYPE_TABLE:
            case PLPGSQL_DTYPE_VAR: {
                PLpgSQL_var* var = (PLpgSQL_var*)d;
                free_expr(var->default_val);
                free_expr(var->cursor_explicit_expr);
                if (var->tableOfIndex != NULL) {
                    hash_destroy(var->tableOfIndex);
                    var->tableOfIndex = NULL;
                }
            } break;
            case PLPGSQL_DTYPE_UNKNOWN:
                break;
            case PLPGSQL_DTYPE_RECORD:
            case PLPGSQL_DTYPE_ROW: {
                PLpgSQL_row* row = (PLpgSQL_row*)d;
                free_expr(row->default_val);
            } break;
            case PLPGSQL_DTYPE_REC:
                break;
            case PLPGSQL_DTYPE_RECFIELD:
                break;
            case PLPGSQL_DTYPE_ARRAYELEM:
                free_expr(((PLpgSQL_arrayelem*)d)->subscript);
                break;
            case PLPGSQL_DTYPE_TABLEELEM:
                free_expr(((PLpgSQL_tableelem*)d)->subscript);
                break;
            case PLPGSQL_DTYPE_ASSIGNLIST:
                free_assignlist(((PLpgSQL_assignlist*)d)->assignlist);
            case PLPGSQL_DTYPE_COMPOSITE:
                break;
            case PLPGSQL_DTYPE_RECORD_TYPE:
                break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_PLSQL),
                        errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized data type: %d when free function memory.", d->dtype)));
                break;
        }
    }
    pkg->ndatums = 0;
    // Release searchpath
    if (pkg->pkg_searchpath != NULL) {
        if (pkg->pkg_searchpath->schemas && pkg->pkg_searchpath->schemas->length > 0) {
            list_free_ext(pkg->pkg_searchpath->schemas);
        }
        pfree_ext(pkg->pkg_searchpath);
    }

    if (pkg->invalItems != NULL) {
        list_free_deep(pkg->invalItems);
        pkg->invalItems = NULL;
    }

    /*
     * And finally, release all memory except the PLpgSQL_function struct
     * itself (which has to be kept around because there may be multiple
     * fn_extra pointers to it).
     */
    if (u_sess->plsql_cxt.curr_compile_context != NULL &&
        pkg->pkg_cxt == u_sess->plsql_cxt.curr_compile_context->compile_cxt)
        u_sess->plsql_cxt.curr_compile_context->compile_cxt = NULL;
    if (pkg->pkg_cxt) {
        MemoryContextDelete(pkg->pkg_cxt);
    }
    pkg->pkg_cxt = NULL;
}

/**********************************************************************
 * Debug functions for analyzing the compiled code
 **********************************************************************/

static void dump_ind(void);
static void dump_stmt(PLpgSQL_stmt* stmt);
static void dump_block(PLpgSQL_stmt_block* block);
static void dump_assign(PLpgSQL_stmt_assign* stmt);
static void dump_if(PLpgSQL_stmt_if* stmt);
static void dump_goto(PLpgSQL_stmt_goto* stmt);
static void dump_case(PLpgSQL_stmt_case* stmt);
static void dump_loop(PLpgSQL_stmt_loop* stmt);
static void dump_while(PLpgSQL_stmt_while* stmt);
static void dump_fori(PLpgSQL_stmt_fori* stmt);
static void dump_fors(PLpgSQL_stmt_fors* stmt);
static void dump_forc(PLpgSQL_stmt_forc* stmt);
static void dump_foreach_a(PLpgSQL_stmt_foreach_a* stmt);
static void dump_exit(PLpgSQL_stmt_exit* stmt);
static void dump_return(PLpgSQL_stmt_return* stmt);
static void dump_return_next(PLpgSQL_stmt_return_next* stmt);
static void dump_return_query(PLpgSQL_stmt_return_query* stmt);
static void dump_raise(PLpgSQL_stmt_raise* stmt);
static void dump_execsql(PLpgSQL_stmt_execsql* stmt);
static void dump_dynexecute(PLpgSQL_stmt_dynexecute* stmt);
static void dump_dynfors(PLpgSQL_stmt_dynfors* stmt);
static void dump_getdiag(PLpgSQL_stmt_getdiag* stmt);
static void dump_open(PLpgSQL_stmt_open* stmt);
static void dump_fetch(PLpgSQL_stmt_fetch* stmt);
static void dump_cursor_direction(PLpgSQL_stmt_fetch* stmt);
static void dump_close(PLpgSQL_stmt_close* stmt);
static void dump_perform(PLpgSQL_stmt_perform* stmt);
static void dump_commit(PLpgSQL_stmt_commit *stmt);
static void dump_rollback(PLpgSQL_stmt_rollback *stmt);
static void dump_null(PLpgSQL_stmt_null* stmt);
static void dump_expr(PLpgSQL_expr* expr);
static void dump_savepoint(PLpgSQL_stmt_savepoint *stmt);

static void dump_ind(void)
{
    int i;

    for (i = 0; i < u_sess->plsql_cxt.dump_indent; i++) {
        printf(" ");
    }
}

static void dump_stmt(PLpgSQL_stmt* stmt)
{
    printf("%3d:", stmt->lineno);
    switch ((enum PLpgSQL_stmt_types)stmt->cmd_type) {
        case PLPGSQL_STMT_BLOCK:
            dump_block((PLpgSQL_stmt_block*)stmt);
            break;
        case PLPGSQL_STMT_ASSIGN:
            dump_assign((PLpgSQL_stmt_assign*)stmt);
            break;
        case PLPGSQL_STMT_IF:
            dump_if((PLpgSQL_stmt_if*)stmt);
            break;
        case PLPGSQL_STMT_GOTO:
            dump_goto((PLpgSQL_stmt_goto*)stmt);
            break;
        case PLPGSQL_STMT_CASE:
            dump_case((PLpgSQL_stmt_case*)stmt);
            break;
        case PLPGSQL_STMT_LOOP:
            dump_loop((PLpgSQL_stmt_loop*)stmt);
            break;
        case PLPGSQL_STMT_WHILE:
            dump_while((PLpgSQL_stmt_while*)stmt);
            break;
        case PLPGSQL_STMT_FORI:
            dump_fori((PLpgSQL_stmt_fori*)stmt);
            break;
        case PLPGSQL_STMT_FORS:
            dump_fors((PLpgSQL_stmt_fors*)stmt);
            break;
        case PLPGSQL_STMT_FORC:
            dump_forc((PLpgSQL_stmt_forc*)stmt);
            break;
        case PLPGSQL_STMT_FOREACH_A:
            dump_foreach_a((PLpgSQL_stmt_foreach_a*)stmt);
            break;
        case PLPGSQL_STMT_EXIT:
            dump_exit((PLpgSQL_stmt_exit*)stmt);
            break;
        case PLPGSQL_STMT_RETURN:
            dump_return((PLpgSQL_stmt_return*)stmt);
            break;
        case PLPGSQL_STMT_RETURN_NEXT:
            dump_return_next((PLpgSQL_stmt_return_next*)stmt);
            break;
        case PLPGSQL_STMT_RETURN_QUERY:
            dump_return_query((PLpgSQL_stmt_return_query*)stmt);
            break;
        case PLPGSQL_STMT_RAISE:
            dump_raise((PLpgSQL_stmt_raise*)stmt);
            break;
        case PLPGSQL_STMT_EXECSQL:
            dump_execsql((PLpgSQL_stmt_execsql*)stmt);
            break;
        case PLPGSQL_STMT_DYNEXECUTE:
            dump_dynexecute((PLpgSQL_stmt_dynexecute*)stmt);
            break;
        case PLPGSQL_STMT_DYNFORS:
            dump_dynfors((PLpgSQL_stmt_dynfors*)stmt);
            break;
        case PLPGSQL_STMT_GETDIAG:
            dump_getdiag((PLpgSQL_stmt_getdiag*)stmt);
            break;
        case PLPGSQL_STMT_OPEN:
            dump_open((PLpgSQL_stmt_open*)stmt);
            break;
        case PLPGSQL_STMT_FETCH:
            dump_fetch((PLpgSQL_stmt_fetch*)stmt);
            break;
        case PLPGSQL_STMT_CLOSE:
            dump_close((PLpgSQL_stmt_close*)stmt);
            break;
        case PLPGSQL_STMT_PERFORM:
            dump_perform((PLpgSQL_stmt_perform*)stmt);
            break;
        case PLPGSQL_STMT_COMMIT:
            dump_commit((PLpgSQL_stmt_commit *) stmt);
            break;
        case PLPGSQL_STMT_ROLLBACK:
            dump_rollback((PLpgSQL_stmt_rollback *) stmt);
            break;
        case PLPGSQL_STMT_NULL:
            dump_null((PLpgSQL_stmt_null*)stmt);
            break;
        case PLPGSQL_STMT_SAVEPOINT:
            dump_savepoint((PLpgSQL_stmt_savepoint*)stmt);
            break;
        default:
            ereport(ERROR,
                (errmodule(MOD_PLSQL),
                    errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized cmd_type: %d, when dump PL/PGSQL statement", stmt->cmd_type)));
            break;
    }
}

static void dump_stmts(List* stmts)
{
    ListCell* s = NULL;

    u_sess->plsql_cxt.dump_indent += 2;
    foreach (s, stmts) {
        dump_stmt((PLpgSQL_stmt*)lfirst(s));
    }
    u_sess->plsql_cxt.dump_indent -= 2;
}

static void dump_block(PLpgSQL_stmt_block* block)
{
    const char* name = NULL;

    if (block->label == NULL) {
        name = "*unnamed*";
    } else {
        name = block->label;
    }

    dump_ind();
    printf("BLOCK <<%s>>\n", name);

    dump_stmts(block->body);

    if (block->exceptions != NULL) {
        ListCell* e = NULL;

        foreach (e, block->exceptions->exc_list) {
            PLpgSQL_exception* exc = (PLpgSQL_exception*)lfirst(e);
            PLpgSQL_condition* cond = NULL;

            dump_ind();
            printf("    EXCEPTION WHEN ");
            for (cond = exc->conditions; cond; cond = cond->next) {
                if (cond != exc->conditions) {
                    printf(" OR ");
                }
                printf("%s", cond->condname);
            }
            printf(" THEN\n");
            dump_stmts(exc->action);
        }
    }

    dump_ind();
    printf("    END -- %s\n", name);
}

static void dump_assign(PLpgSQL_stmt_assign* stmt)
{
    dump_ind();
    printf("ASSIGN var %d := ", stmt->varno);
    dump_expr(stmt->expr);
    printf("\n");
}

static void dump_if(PLpgSQL_stmt_if* stmt)
{
    ListCell* l = NULL;

    dump_ind();
    printf("IF ");
    dump_expr(stmt->cond);
    printf(" THEN\n");
    dump_stmts(stmt->then_body);
    foreach (l, stmt->elsif_list) {
        PLpgSQL_if_elsif* elif = (PLpgSQL_if_elsif*)lfirst(l);

        dump_ind();
        printf("    ELSIF ");
        dump_expr(elif->cond);
        printf(" THEN\n");
        dump_stmts(elif->stmts);
    }

    if (stmt->else_body != NIL) {
        dump_ind();
        printf("    ELSE\n");
        dump_stmts(stmt->else_body);
    }
    dump_ind();
    printf("    ENDIF\n");
}

static void dump_goto(PLpgSQL_stmt_goto* stmt)
{
    dump_ind();
    printf("GOTO %s\n", stmt->label);
}

static void dump_case(PLpgSQL_stmt_case* stmt)
{
    ListCell* l = NULL;

    dump_ind();
    printf("CASE %d ", stmt->t_varno);
    if (stmt->t_expr != NULL)
        dump_expr(stmt->t_expr);
    printf("\n");
    u_sess->plsql_cxt.dump_indent += 6;
    foreach (l, stmt->case_when_list) {
        PLpgSQL_case_when* cwt = (PLpgSQL_case_when*)lfirst(l);

        dump_ind();
        printf("WHEN ");
        dump_expr(cwt->expr);
        printf("\n");
        dump_ind();
        printf("THEN\n");
        u_sess->plsql_cxt.dump_indent += 2;
        dump_stmts(cwt->stmts);
        u_sess->plsql_cxt.dump_indent -= 2;
    }

    if (stmt->have_else) {
        dump_ind();
        printf("ELSE\n");
        u_sess->plsql_cxt.dump_indent += 2;
        dump_stmts(stmt->else_stmts);
        u_sess->plsql_cxt.dump_indent -= 2;
    }
    u_sess->plsql_cxt.dump_indent -= 6;
    dump_ind();
    printf("    ENDCASE\n");
}

static void dump_loop(PLpgSQL_stmt_loop* stmt)
{
    dump_ind();
    printf("LOOP\n");

    dump_stmts(stmt->body);

    dump_ind();
    printf("    ENDLOOP\n");
}

static void dump_while(PLpgSQL_stmt_while* stmt)
{
    if(stmt->condition)
    {
        dump_ind();
        printf("WHILE ");
        dump_expr(stmt->cond);
        printf("\n");

        dump_stmts(stmt->body);

        dump_ind();
        printf("    ENDWHILE\n");
    }
    else
    {
        dump_ind();
        printf("REPEAT ");
        dump_stmts(stmt->body);
        printf("\n");
        printf("UNTIL ");
        dump_expr(stmt->cond);
        dump_ind();
        printf("    ENDREPEAT\n");
    }
}

static void dump_fori(PLpgSQL_stmt_fori* stmt)
{
    dump_ind();
    printf("FORI %s %s\n", stmt->var->refname, (stmt->reverse) ? "REVERSE" : "NORMAL");

    u_sess->plsql_cxt.dump_indent += 2;
    dump_ind();
    printf("    lower = ");
    dump_expr(stmt->lower);
    printf("\n");
    dump_ind();
    printf("    upper = ");
    dump_expr(stmt->upper);
    printf("\n");
    if (stmt->step != NULL) {
        dump_ind();
        printf("    step = ");
        dump_expr(stmt->step);
        printf("\n");
    }
    u_sess->plsql_cxt.dump_indent -= 2;

    dump_stmts(stmt->body);

    dump_ind();
    printf("    ENDFORI\n");
}

static void dump_fors(PLpgSQL_stmt_fors* stmt)
{
    dump_ind();
    printf("FORS %s ", (stmt->rec != NULL) ? stmt->rec->refname : stmt->row->refname);
    dump_expr(stmt->query);
    printf("\n");

    dump_stmts(stmt->body);

    dump_ind();
    printf("    ENDFORS\n");
}

static void dump_forc(PLpgSQL_stmt_forc* stmt)
{
    dump_ind();
    printf("FORC %s ", stmt->rec->refname);
    printf("curvar=%d\n", stmt->curvar);

    u_sess->plsql_cxt.dump_indent += 2;
    if (stmt->argquery != NULL) {
        dump_ind();
        printf("  arguments = ");
        dump_expr(stmt->argquery);
        printf("\n");
    }
    u_sess->plsql_cxt.dump_indent -= 2;

    dump_stmts(stmt->body);

    dump_ind();
    printf("    ENDFORC\n");
}

static void dump_foreach_a(PLpgSQL_stmt_foreach_a* stmt)
{
    dump_ind();
    printf("FOREACHA var %d ", stmt->varno);
    if (stmt->slice != 0) {
        printf("SLICE %d ", stmt->slice);
    }
    printf("IN ");
    dump_expr(stmt->expr);
    printf("\n");

    dump_stmts(stmt->body);

    dump_ind();
    printf("    ENDFOREACHA");
}

static void dump_open(PLpgSQL_stmt_open* stmt)
{
    dump_ind();
    printf("OPEN curvar=%d\n", stmt->curvar);

    u_sess->plsql_cxt.dump_indent += 2;
    if (stmt->argquery != NULL) {
        dump_ind();
        printf("  arguments = '");
        dump_expr(stmt->argquery);
        printf("'\n");
    }
    if (stmt->query != NULL) {
        dump_ind();
        printf("  query = '");
        dump_expr(stmt->query);
        printf("'\n");
    }
    if (stmt->dynquery != NULL) {
        dump_ind();
        printf("  execute = '");
        dump_expr(stmt->dynquery);
        printf("'\n");

        if (stmt->params != NIL) {
            ListCell* lc = NULL;
            int i;

            u_sess->plsql_cxt.dump_indent += 2;
            dump_ind();
            printf("    USING\n");
            u_sess->plsql_cxt.dump_indent += 2;
            i = 1;
            foreach (lc, stmt->params) {
                dump_ind();
                printf("    parameter $%d: ", i++);
                dump_expr((PLpgSQL_expr*)lfirst(lc));
                printf("\n");
            }
            u_sess->plsql_cxt.dump_indent -= 4;
        }
    }
    u_sess->plsql_cxt.dump_indent -= 2;
}

static void dump_fetch(PLpgSQL_stmt_fetch* stmt)
{
    dump_ind();

    if (!stmt->is_move) {
        printf("FETCH curvar=%d\n", stmt->curvar);
        dump_cursor_direction(stmt);

        u_sess->plsql_cxt.dump_indent += 2;
        if (stmt->rec != NULL) {
            dump_ind();
            printf("    target = %d %s\n", stmt->rec->dno, stmt->rec->refname);
        }
        if (stmt->row != NULL) {
            dump_ind();
            printf("    target = %d %s\n", stmt->row->dno, stmt->row->refname);
        }
        u_sess->plsql_cxt.dump_indent -= 2;
    } else {
        printf("MOVE curvar=%d\n", stmt->curvar);
        dump_cursor_direction(stmt);
    }
}

static void dump_cursor_direction(PLpgSQL_stmt_fetch* stmt)
{
    u_sess->plsql_cxt.dump_indent += 2;
    dump_ind();
    switch (stmt->direction) {
        case FETCH_FORWARD:
            printf("    FORWARD ");
            break;
        case FETCH_BACKWARD:
            printf("    BACKWARD ");
            break;
        case FETCH_ABSOLUTE:
            printf("    ABSOLUTE ");
            break;
        case FETCH_RELATIVE:
            printf("    RELATIVE ");
            break;
        default:
            printf("??? unknown cursor direction %d", stmt->direction);
            break;
    }

    if (stmt->expr != NULL) {
        dump_expr(stmt->expr);
        printf("\n");
    } else
        printf("%ld\n", stmt->how_many);

    u_sess->plsql_cxt.dump_indent -= 2;
}

static void dump_close(PLpgSQL_stmt_close* stmt)
{
    dump_ind();
    printf("CLOSE curvar=%d\n", stmt->curvar);
}

static void dump_perform(PLpgSQL_stmt_perform* stmt)
{
    dump_ind();
    printf("PERFORM expr = ");
    dump_expr(stmt->expr);
    printf("\n");
}

static void dump_commit(PLpgSQL_stmt_commit *stmt)
{
    dump_ind();
    printf("COMMIT\n");
    return;
}

static void dump_rollback(PLpgSQL_stmt_rollback *stmt)
{
    dump_ind();
    printf("ROLLBACK\n");
}

static void dump_null(PLpgSQL_stmt_null* stmt)
{
    dump_ind();
    printf("NULL\n");
}

static void dump_savepoint(PLpgSQL_stmt_savepoint *stmt)
{
    switch (stmt->opType) {
        case PLPGSQL_SAVEPOINT_CREATE:
            printf("SAVEPOINT %s;", stmt->spName);
            break;
        case PLPGSQL_SAVEPOINT_ROLLBACKTO:
            printf("ROLLBACK TO %s", stmt->spName);
            break;
        case PLPGSQL_SAVEPOINT_RELEASE:
            printf("RELEASE %s", stmt->spName);
            break;
        default:
            printf("??? unknown savepoint operate: %s", stmt->spName);
            break;
    }
}

static void dump_exit(PLpgSQL_stmt_exit* stmt)
{
    dump_ind();
    printf("%s", stmt->is_exit ? "EXIT" : "CONTINUE");
    if (stmt->label != NULL) {
        printf(" label='%s'", stmt->label);
    }
    if (stmt->cond != NULL) {
        printf(" WHEN ");
        dump_expr(stmt->cond);
    }
    printf("\n");
}

static void dump_return(PLpgSQL_stmt_return* stmt)
{
    dump_ind();
    printf("RETURN ");
    if (stmt->retvarno >= 0) {
        printf("variable %d", stmt->retvarno);
    } else if (stmt->expr != NULL) {
        dump_expr(stmt->expr);
    } else {
        printf("NULL");
    }
    printf("\n");
}

static void dump_return_next(PLpgSQL_stmt_return_next* stmt)
{
    dump_ind();
    printf("RETURN NEXT ");
    if (stmt->retvarno >= 0) {
        printf("variable %d", stmt->retvarno);
    } else if (stmt->expr != NULL) {
        dump_expr(stmt->expr);
    } else {
        printf("NULL");
    }
    printf("\n");
}

static void dump_return_query(PLpgSQL_stmt_return_query* stmt)
{
    dump_ind();
    if (stmt->query != NULL) {
        printf("RETURN QUERY ");
        dump_expr(stmt->query);
        printf("\n");
    } else {
        printf("RETURN QUERY EXECUTE ");
        dump_expr(stmt->dynquery);
        printf("\n");
        if (stmt->params != NIL) {
            ListCell* lc = NULL;
            int i;

            u_sess->plsql_cxt.dump_indent += 2;
            dump_ind();
            printf("    USING\n");
            u_sess->plsql_cxt.dump_indent += 2;
            i = 1;
            foreach (lc, stmt->params) {
                dump_ind();
                printf("    parameter $%d: ", i++);
                dump_expr((PLpgSQL_expr*)lfirst(lc));
                printf("\n");
            }
            u_sess->plsql_cxt.dump_indent -= 4;
        }
    }
}

static void dump_raise(PLpgSQL_stmt_raise* stmt)
{
    ListCell* lc = NULL;
    int i = 0;

    dump_ind();
    printf("RAISE level=%d", stmt->elog_level);
    if (stmt->condname != NULL) {
        printf(" condname='%s'", stmt->condname);
    }
    if (stmt->message != NULL) {
        printf(" message='%s'", stmt->message);
    }
    printf("\n");
    u_sess->plsql_cxt.dump_indent += 2;
    foreach (lc, stmt->params) {
        dump_ind();
        printf("    parameter %d: ", i++);
        dump_expr((PLpgSQL_expr*)lfirst(lc));
        printf("\n");
    }
    if (stmt->options != NIL) {
        dump_ind();
        printf("    USING\n");
        u_sess->plsql_cxt.dump_indent += 2;
        foreach (lc, stmt->options) {
            PLpgSQL_raise_option* opt = (PLpgSQL_raise_option*)lfirst(lc);

            dump_ind();
            switch (opt->opt_type) {
                case PLPGSQL_RAISEOPTION_ERRCODE:
                    printf("    ERRCODE = ");
                    break;
                case PLPGSQL_RAISEOPTION_MESSAGE:
                    printf("    MESSAGE = ");
                    break;
                case PLPGSQL_RAISEOPTION_DETAIL:
                    printf("    DETAIL = ");
                    break;
                case PLPGSQL_RAISEOPTION_HINT:
                    printf("    HINT = ");
                    break;
                default:
                    break;
            }
            dump_expr(opt->expr);
            printf("\n");
        }
        u_sess->plsql_cxt.dump_indent -= 2;
    }
    u_sess->plsql_cxt.dump_indent -= 2;
}

static void dump_execsql(PLpgSQL_stmt_execsql* stmt)
{
    dump_ind();
    printf("EXECSQL ");
    dump_expr(stmt->sqlstmt);
    printf("\n");

    u_sess->plsql_cxt.dump_indent += 2;
    if (stmt->rec != NULL) {
        dump_ind();
        printf("    INTO%s target = %d %s\n", stmt->strict ? " STRICT" : "", stmt->rec->dno, stmt->rec->refname);
    }
    if (stmt->row != NULL) {
        dump_ind();
        printf("    INTO%s target = %d %s\n", stmt->strict ? " STRICT" : "", stmt->row->dno, stmt->row->refname);
    }
    u_sess->plsql_cxt.dump_indent -= 2;
}

static void dump_dynexecute(PLpgSQL_stmt_dynexecute* stmt)
{
    dump_ind();
    printf("EXECUTE ");
    dump_expr(stmt->query);
    printf("\n");

    u_sess->plsql_cxt.dump_indent += 2;
    if (stmt->rec != NULL) {
        dump_ind();
        printf("    INTO%s target = %d %s\n", stmt->strict ? " STRICT" : "", stmt->rec->dno, stmt->rec->refname);
    }
    if (stmt->row != NULL) {
        dump_ind();
        printf("    INTO%s target = %d %s\n", stmt->strict ? " STRICT" : "", stmt->row->dno, stmt->row->refname);
    }
    if (stmt->params != NIL) {
        ListCell* lc = NULL;
        int i;

        dump_ind();
        printf("    USING\n");
        u_sess->plsql_cxt.dump_indent += 2;
        i = 1;
        foreach (lc, stmt->params) {
            dump_ind();
            printf("    parameter %d: ", i++);
            dump_expr((PLpgSQL_expr*)lfirst(lc));
            printf("\n");
        }
        u_sess->plsql_cxt.dump_indent -= 2;
    }
    u_sess->plsql_cxt.dump_indent -= 2;
}

static void dump_dynfors(PLpgSQL_stmt_dynfors* stmt)
{
    dump_ind();
    printf("FORS %s EXECUTE ", (stmt->rec != NULL) ? stmt->rec->refname : stmt->row->refname);
    dump_expr(stmt->query);
    printf("\n");
    if (stmt->params != NIL) {
        ListCell* lc = NULL;
        int i;

        u_sess->plsql_cxt.dump_indent += 2;
        dump_ind();
        printf("    USING\n");
        u_sess->plsql_cxt.dump_indent += 2;
        i = 1;
        foreach (lc, stmt->params) {
            dump_ind();
            printf("    parameter $%d: ", i++);
            dump_expr((PLpgSQL_expr*)lfirst(lc));
            printf("\n");
        }
        u_sess->plsql_cxt.dump_indent -= 4;
    }
    dump_stmts(stmt->body);
    dump_ind();
    printf("    ENDFORS\n");
}

static void dump_getdiag(PLpgSQL_stmt_getdiag* stmt)
{
    ListCell* lc = NULL;

    dump_ind();
    printf("GET %s DIAGNOSTICS ", stmt->is_stacked ? "STACKED" : "CURRENT");
    foreach (lc, stmt->diag_items) {
        PLpgSQL_diag_item* diag_item = (PLpgSQL_diag_item*)lfirst(lc);

        if (lc != list_head(stmt->diag_items)) {
            printf(", ");
        }

        printf("{var %d} = %s", diag_item->target, plpgsql_getdiag_kindname(diag_item->kind));
    }
    printf("\n");
}

static void dump_expr(PLpgSQL_expr* expr)
{
    printf("'%s'", expr->query);
}

void plpgsql_dumptree(PLpgSQL_function* func)
{
    int i;
    PLpgSQL_datum* d = NULL;

    printf("\nExecution tree of successfully compiled PL/pgSQL function %s:\n", func->fn_signature);

    printf("\nFunction's data area:\n");
    for (i = 0; i < func->ndatums; i++) {
        d = func->datums[i];

        printf("    entry %d: ", i);
        switch (d->dtype) {
            case PLPGSQL_DTYPE_VAR: {
                PLpgSQL_var* var = (PLpgSQL_var*)d;

                printf("VAR %-16s type %s (typoid %u) atttypmod %d\n",
                    var->refname,
                    var->datatype->typname,
                    var->datatype->typoid,
                    var->datatype->atttypmod);
                if (var->isconst) {
                    printf("                                  CONSTANT\n");
                }
                if (var->notnull) {
                    printf("                                  NOT NULL\n");
                }
                if (var->default_val != NULL) {
                    printf("                                  DEFAULT ");
                    dump_expr(var->default_val);
                    printf("\n");
                }
                if (var->cursor_explicit_expr != NULL) {
                    if (var->cursor_explicit_argrow >= 0) {
                        printf(
                            "                                  CURSOR argument row %d\n", var->cursor_explicit_argrow);
                    }

                    printf("                                  CURSOR IS ");
                    dump_expr(var->cursor_explicit_expr);
                    printf("\n");
                }
            } break;
            case PLPGSQL_DTYPE_ROW: {
                PLpgSQL_row* row = (PLpgSQL_row*)d;
                int j;

                printf("ROW %-16s fields", row->refname);
                for (j = 0; j < row->nfields; j++) {
                    if (row->fieldnames[j]) {
                        printf(" %s=var %d", row->fieldnames[j], row->varnos[j]);
                    }
                }
                if (row->default_val != NULL) {
                    printf("                                  DEFAULT ");
                    dump_expr(row->default_val);
                    printf("\n");
                }
                printf("\n");
            } break;
            case PLPGSQL_DTYPE_REC:
                printf("REC %s\n", ((PLpgSQL_rec*)d)->refname);
                break;
            case PLPGSQL_DTYPE_RECFIELD:
                printf("RECFIELD %-16s of REC %d\n",
                    ((PLpgSQL_recfield*)d)->fieldname,
                    ((PLpgSQL_recfield*)d)->recparentno);
                break;
            case PLPGSQL_DTYPE_ARRAYELEM:
                printf("ARRAYELEM of VAR %d subscript ", ((PLpgSQL_arrayelem*)d)->arrayparentno);
                dump_expr(((PLpgSQL_arrayelem*)d)->subscript);
                printf("\n");
                break;
            default:
                printf("??? unknown data type %d\n", d->dtype);
                break;
        }
    }
    printf("\nFunction's statements:\n");
    /* ??? */
    u_sess->plsql_cxt.dump_indent = 0;
    printf("%3d:", func->action->lineno);
    dump_block(func->action);
    printf("\nEnd of execution tree of function %s\n\n", func->fn_signature);
    fflush(stdout);
}

/**********************************************************************
 * Traverse functions to detect shippability of the compiled code
 * Notice : Whenever add PLpgSQL_stmt_types need adapt function here
 **********************************************************************/
bool traverse_stmt(PLpgSQL_function* func, PLpgSQL_stmt* stmt);
bool traverse_block(PLpgSQL_function* func, PLpgSQL_stmt_block* block);
bool traverse_assign(PLpgSQL_function* func, PLpgSQL_stmt_assign* stmt);
bool traverse_if(PLpgSQL_function* func, PLpgSQL_stmt_if* stmt);
bool traverse_case(PLpgSQL_function* func, PLpgSQL_stmt_case* stmt);
bool traverse_loop(PLpgSQL_function* func, PLpgSQL_stmt_loop* stmt);
bool traverse_while(PLpgSQL_function* func, PLpgSQL_stmt_while* stmt);
bool traverse_fori(PLpgSQL_function* func, PLpgSQL_stmt_fori* stmt);
bool traverse_fors(PLpgSQL_function* func, PLpgSQL_stmt_fors* stmt);
bool traverse_forc(PLpgSQL_function* func, PLpgSQL_stmt_forc* stmt);
bool traverse_foreach_a(PLpgSQL_function* func, PLpgSQL_stmt_foreach_a* stmt);
bool traverse_exit(PLpgSQL_function* func, PLpgSQL_stmt_exit* stmt);
bool traverse_return(PLpgSQL_function* func, PLpgSQL_stmt_return* stmt);
bool traverse_return_next(PLpgSQL_function* func, PLpgSQL_stmt_return_next* stmt);
bool traverse_return_query(PLpgSQL_function* func, PLpgSQL_stmt_return_query* stmt);
bool traverse_raise(PLpgSQL_function* func, PLpgSQL_stmt_raise* stmt);
bool traverse_execsql(PLpgSQL_function* func, PLpgSQL_stmt_execsql* stmt);
bool traverse_dynexecute(PLpgSQL_function* func, PLpgSQL_stmt_dynexecute* stmt);
bool traverse_dynfors(PLpgSQL_function* func, PLpgSQL_stmt_dynfors* stmt);
bool traverse_getdiag(PLpgSQL_stmt_getdiag* stmt);
bool traverse_open(PLpgSQL_function* func, PLpgSQL_stmt_open* stmt);
bool traverse_fetch(PLpgSQL_function* func, PLpgSQL_stmt_fetch* stmt);
bool traverse_cursor_direction(PLpgSQL_function* func, PLpgSQL_stmt_fetch* stmt);
bool traverse_close(PLpgSQL_stmt_close* stmt);
bool traverse_perform(PLpgSQL_function* func, PLpgSQL_stmt_perform* stmt);
bool traverse_expr(PLpgSQL_function* func, PLpgSQL_expr* expr);
bool traverse_savepoint(PLpgSQL_function* func, PLpgSQL_stmt_savepoint *stmt);
bool traverse_commit(PLpgSQL_function* func, PLpgSQL_stmt_commit *stmt);
bool traverse_rollback(PLpgSQL_function* func, PLpgSQL_stmt_rollback *stmt);

/*
 * @Description : traverse statements in trigger body for checking shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmts : statements in the trigger body.
 * @return : all statements can shippable or not.
 */
bool traverse_stmts(PLpgSQL_function* func, List* stmts)
{
    ListCell* stmt = NULL;
    bool is_shippable = false;

    foreach (stmt, stmts) {
        is_shippable = traverse_stmt(func, (PLpgSQL_stmt*)lfirst(stmt));

        if ((enum PLpgSQL_stmt_types)((PLpgSQL_stmt*)lfirst(stmt))->cmd_type == PLPGSQL_STMT_EXECSQL) {
            elog(DEBUG1,
                "traverse_stmt [%s] '%s' is_shippable: %s",
                plpgsql_stmt_typename((PLpgSQL_stmt*)lfirst(stmt)),
                ((PLpgSQL_expr*)((PLpgSQL_stmt_execsql*)lfirst(stmt))->sqlstmt)->query,
                is_shippable ? "true" : "false");
        } else {
            elog(DEBUG1,
                "traverse_stmt [%s] '' is_shippable: %s",
                plpgsql_stmt_typename((PLpgSQL_stmt*)lfirst(stmt)),
                is_shippable ? "true" : "false");
        }

        if (!is_shippable) {
            return false;
        }
    }
    return true;
}

/*
 * @Description : check the shippalbe for each statement in trigger body.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : single statement in the trigger body.
 * @return : the statement can shippable or not.
 */
bool traverse_stmt(PLpgSQL_function* func, PLpgSQL_stmt* stmt)
{
    bool is_shippable = false;

    switch ((enum PLpgSQL_stmt_types)stmt->cmd_type) {
        case PLPGSQL_STMT_BLOCK:
            is_shippable = traverse_block(func, (PLpgSQL_stmt_block*)stmt);
            break;
        case PLPGSQL_STMT_ASSIGN:
            is_shippable = traverse_assign(func, (PLpgSQL_stmt_assign*)stmt);
            break;
        case PLPGSQL_STMT_IF:
            is_shippable = traverse_if(func, (PLpgSQL_stmt_if*)stmt);
            break;
        case PLPGSQL_STMT_CASE:
            is_shippable = traverse_case(func, (PLpgSQL_stmt_case*)stmt);
            break;
        case PLPGSQL_STMT_LOOP:
            is_shippable = traverse_loop(func, (PLpgSQL_stmt_loop*)stmt);
            break;
        case PLPGSQL_STMT_WHILE:
            is_shippable = traverse_while(func, (PLpgSQL_stmt_while*)stmt);
            break;
        case PLPGSQL_STMT_FORI:
            is_shippable = traverse_fori(func, (PLpgSQL_stmt_fori*)stmt);
            break;
        case PLPGSQL_STMT_FORS:
            is_shippable = traverse_fors(func, (PLpgSQL_stmt_fors*)stmt);
            break;
        case PLPGSQL_STMT_FORC:
            is_shippable = traverse_forc(func, (PLpgSQL_stmt_forc*)stmt);
            break;
        case PLPGSQL_STMT_FOREACH_A:
            is_shippable = traverse_foreach_a(func, (PLpgSQL_stmt_foreach_a*)stmt);
            break;
        case PLPGSQL_STMT_EXIT:
            is_shippable = traverse_exit(func, (PLpgSQL_stmt_exit*)stmt);
            break;
        case PLPGSQL_STMT_RETURN:
            is_shippable = traverse_return(func, (PLpgSQL_stmt_return*)stmt);
            break;
        case PLPGSQL_STMT_RETURN_NEXT:
            is_shippable = traverse_return_next(func, (PLpgSQL_stmt_return_next*)stmt);
            break;
        case PLPGSQL_STMT_RETURN_QUERY:
            is_shippable = traverse_return_query(func, (PLpgSQL_stmt_return_query*)stmt);
            break;
        case PLPGSQL_STMT_RAISE:
            is_shippable = traverse_raise(func, (PLpgSQL_stmt_raise*)stmt);
            break;
        case PLPGSQL_STMT_EXECSQL:
            is_shippable = traverse_execsql(func, (PLpgSQL_stmt_execsql*)stmt);
            break;
        case PLPGSQL_STMT_DYNEXECUTE:
            is_shippable = traverse_dynexecute(func, (PLpgSQL_stmt_dynexecute*)stmt);
            break;
        case PLPGSQL_STMT_DYNFORS:
            is_shippable = traverse_dynfors(func, (PLpgSQL_stmt_dynfors*)stmt);
            break;
        case PLPGSQL_STMT_GETDIAG:
            is_shippable = traverse_getdiag((PLpgSQL_stmt_getdiag*)stmt);
            break;
        case PLPGSQL_STMT_OPEN:
            is_shippable = traverse_open(func, (PLpgSQL_stmt_open*)stmt);
            break;
        case PLPGSQL_STMT_FETCH:
            is_shippable = traverse_fetch(func, (PLpgSQL_stmt_fetch*)stmt);
            break;
        case PLPGSQL_STMT_CLOSE:
            is_shippable = traverse_close((PLpgSQL_stmt_close*)stmt);
            break;
        case PLPGSQL_STMT_PERFORM:
            is_shippable = traverse_perform(func, (PLpgSQL_stmt_perform*)stmt);
            break;
        case PLPGSQL_STMT_SAVEPOINT:
            is_shippable = traverse_savepoint(func, (PLpgSQL_stmt_savepoint*)stmt);
            break;
        case PLPGSQL_STMT_COMMIT:
            is_shippable = traverse_commit(func, (PLpgSQL_stmt_commit*)stmt);
            break;
        case PLPGSQL_STMT_ROLLBACK:
            is_shippable = traverse_rollback(func, (PLpgSQL_stmt_rollback*)stmt);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("unrecognized cmd_type: %d", stmt->cmd_type)));
            break;
    }
    return is_shippable;
}

/*
 * @Description : traverse the BLOCK struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the BLOCK struct of trigger body.
 * @return : the BLOCK struct can shippable or not.
 */
bool traverse_block(PLpgSQL_function* func, PLpgSQL_stmt_block* stmt)
{
    if (stmt == NULL) {
        return true;
    }

    if (stmt->body && !traverse_stmts(func, stmt->body)) {
        return false;
    }

    if (stmt->exceptions != NULL) {
        ListCell* exception_list = NULL;

        foreach (exception_list, stmt->exceptions->exc_list) {
            PLpgSQL_exception* exc = (PLpgSQL_exception*)lfirst(exception_list);
            if (exc->action && !traverse_stmts(func, exc->action)) {
                return false;
            }
        }
    }
    return true;
}

/*
 * @Description : traverse the ASSIGN struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the ASSIGN struct of trigger body.
 * @return : the ASSIGN struct can shippable or not.
 */
bool traverse_assign(PLpgSQL_function* func, PLpgSQL_stmt_assign* stmt)
{
    if (stmt == NULL || stmt->expr == NULL) {
        return true;
    }

    return traverse_expr(func, stmt->expr);
}

/*
 * @Description : traverse the IF struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the IF struct of trigger body.
 * @return : the IF struct can shippable or not.
 */
bool traverse_if(PLpgSQL_function* func, PLpgSQL_stmt_if* stmt)
{
    ListCell* elseif_cell = NULL;

    if (stmt == NULL) {
        return true;
    }

    if (stmt->cond && !traverse_expr(func, stmt->cond)) {
        return false;
    }
    if (stmt->then_body && !traverse_stmts(func, stmt->then_body)) {
        return false;
    }

    foreach (elseif_cell, stmt->elsif_list) {
        PLpgSQL_if_elsif* elif = (PLpgSQL_if_elsif*)lfirst(elseif_cell);
        if (elif->cond && !traverse_expr(func, elif->cond)) {
            return false;
        }
        if (elif->stmts && !traverse_stmts(func, elif->stmts)) {
            return false;
        }
    }

    if (stmt->else_body && !traverse_stmts(func, stmt->else_body)) {
        return false;
    }

    return true;
}

/*
 * @Description : traverse the CASE struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the CASE struct of trigger body.
 * @return : the CASE struct can shippable or not.
 */
bool traverse_case(PLpgSQL_function* func, PLpgSQL_stmt_case* stmt)
{
    ListCell* case_cell = NULL;

    if (stmt == NULL) {
        return true;
    }

    if (stmt->t_expr && !traverse_expr(func, stmt->t_expr)) {
        return false;
    }

    foreach (case_cell, stmt->case_when_list) {
        PLpgSQL_case_when* cwt = (PLpgSQL_case_when*)lfirst(case_cell);

        if (cwt->expr && !traverse_expr(func, cwt->expr)) {
            return false;
        }
        if (cwt->stmts && !traverse_stmts(func, cwt->stmts)) {
            return false;
        }
    }

    if (stmt->have_else && !traverse_stmts(func, stmt->else_stmts)) {
        return false;
    }

    return true;
}

/*
 * @Description : traverse the LOOP struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the LOOP struct of trigger body.
 * @return : the LOOP struct can shippable or not.
 */
bool traverse_loop(PLpgSQL_function* func, PLpgSQL_stmt_loop* stmt)
{
    if (stmt == NULL || stmt->body == NULL) {
        return true;
    }

    return traverse_stmts(func, stmt->body);
}

/*
 * @Description : traverse the WHILE struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the WHILE struct of trigger body.
 * @return : the WHILE struct can shippable or not.
 */
bool traverse_while(PLpgSQL_function* func, PLpgSQL_stmt_while* stmt)
{
    if (stmt == NULL) {
        return true;
    }

    if (stmt->cond && !traverse_expr(func, stmt->cond)) {
        return false;
    }

    return traverse_stmts(func, stmt->body);
}

/*
 * @Description : traverse the FOR IN struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the FOR IN struct of trigger body.
 * @return : the FOR IN struct can shippable or not.
 */
bool traverse_fori(PLpgSQL_function* func, PLpgSQL_stmt_fori* stmt)
{
    if (stmt == NULL) {
        return true;
    }

    if (stmt->lower && !traverse_expr(func, stmt->lower)) {
        return false;
    }
    if (stmt->upper && !traverse_expr(func, stmt->upper)) {
        return false;
    }

    if (stmt->step && !traverse_expr(func, stmt->step)) {
        return false;
    }

    return traverse_stmts(func, stmt->body);
}

/*
 * @Description : traverse FOR struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the FOR struct of trigger body.
 * @return : the FOR struct can shippable or not.
 */
bool traverse_fors(PLpgSQL_function* func, PLpgSQL_stmt_fors* stmt)
{
    if (stmt == NULL) {
        return true;
    }

    if (stmt->query && !traverse_expr(func, stmt->query)) {
        return false;
    }
    if (stmt->body && !traverse_stmts(func, stmt->body)) {
        return false;
    }

    return true;
}

/*
 * @Description : traverse the FOR CURSOR struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the FOR CURSOR struct of trigger body.
 * @return : the FOR CURSOR struct can shippable or not.
 */
bool traverse_forc(PLpgSQL_function* func, PLpgSQL_stmt_forc* stmt)
{
    if (stmt == NULL) {
        return true;
    }

    if (stmt->argquery && !traverse_expr(func, stmt->argquery)) {
        return false;
    }

    return traverse_stmts(func, stmt->body);
}

/*
 * @Description : traverse the FOREACH struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the FOREACH struct of trigger body.
 * @return : the FOREACH struct can shippable or not.
 */
bool traverse_foreach_a(PLpgSQL_function* func, PLpgSQL_stmt_foreach_a* stmt)
{
    if (stmt == NULL) {
        return true;
    }

    if (stmt->expr && !traverse_expr(func, stmt->expr)) {
        return false;
    }
    if (stmt->body && !traverse_stmts(func, stmt->body)) {
        return false;
    }

    return true;
}

/*
 * @Description : traverse the OPEN struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the OPEN struct of trigger body.
 * @return : the OPEN struct can shippable or not.
 */
bool traverse_open(PLpgSQL_function* func, PLpgSQL_stmt_open* stmt)
{
    if (stmt == NULL) {
        return true;
    }

    if (stmt->argquery && !traverse_expr(func, stmt->argquery)) {
        return false;
    }

    if (stmt->query && !traverse_expr(func, stmt->query)) {
        return false;
    }

    if (stmt->dynquery != NULL) {
        if (!traverse_expr(func, stmt->dynquery)) {
            return false;
        }

        if (stmt->params != NIL) {
            ListCell* param_cell = NULL;

            foreach (param_cell, stmt->params) {
                if (!traverse_expr(func, (PLpgSQL_expr*)lfirst(param_cell)))
                    return false;
            }
        }
    }

    return true;
}

/*
 * @Description : traverse the FETCH struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the FETCH struct of trigger body.
 * @return : the FETCH struct can shippable or not.
 */
bool traverse_fetch(PLpgSQL_function* func, PLpgSQL_stmt_fetch* stmt)
{
    if (stmt == NULL) {
        return true;
    }

    return traverse_cursor_direction(func, stmt);
}

/*
 * @Description : traverse the FETCH EXPR struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the FETCH EXPR struct of trigger body.
 * @return : the FETCH EXPR struct can shippable or not.
 */
bool traverse_cursor_direction(PLpgSQL_function* func, PLpgSQL_stmt_fetch* stmt)
{
    if (stmt == NULL || stmt->expr == NULL) {
        return true;
    }

    return traverse_expr(func, stmt->expr);
}

/*
 * @Description : traverse the CLOSE struct to check shippable.
 * @in stmt : the CLOSE struct of trigger body.
 * @return : just return true now.
 */
bool traverse_close(PLpgSQL_stmt_close* stmt)
{
    return true;
}

/*
 * @Description : traverse the PERFORM struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the PERFORM struct of trigger body.
 * @return : the PERFORM struct can shippable or not.
 */
bool traverse_perform(PLpgSQL_function* func, PLpgSQL_stmt_perform* stmt)
{
    if (stmt == NULL || stmt->expr == NULL) {
        return true;
    }

    return traverse_expr(func, stmt->expr);
}

/*
 * @Description : traverse the EXIT struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the EXIT struct of trigger body.
 * @return : the EXIT struct can shippable or not.
 */
bool traverse_exit(PLpgSQL_function* func, PLpgSQL_stmt_exit* stmt)
{
    if (stmt == NULL || stmt->cond == NULL) {
        return true;
    }

    return traverse_expr(func, stmt->cond);
}

/*
 * @Description : traverse the RETURN struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the RETURN struct of trigger body.
 * @return : the RETURN struct can shippable or not.
 */
bool traverse_return(PLpgSQL_function* func, PLpgSQL_stmt_return* stmt)
{
    if (stmt == NULL || stmt->expr == NULL) {
        return true;
    }

    return traverse_expr(func, stmt->expr);
}

/*
 * @Description : traverse the RETURN NEXT struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the RETURN NEXT struct of trigger body.
 * @return : the RETURN NEXT struct can shippable or not.
 */
bool traverse_return_next(PLpgSQL_function* func, PLpgSQL_stmt_return_next* stmt)
{
    if (stmt == NULL || stmt->expr == NULL) {
        return true;
    }

    return traverse_expr(func, stmt->expr);
}

/*
 * @Description : traverse the RETURN QUERY struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the RETURN QUERY struct of trigger body.
 * @return : the RETURN QUERY struct can shippable or not.
 */
bool traverse_return_query(PLpgSQL_function* func, PLpgSQL_stmt_return_query* stmt)
{
    if (stmt == NULL) {
        return true;
    }

    if (stmt->query && !traverse_expr(func, stmt->query)) {
        return false;
    }

    if (stmt->dynquery != NULL) {
        if (!traverse_expr(func, stmt->dynquery)) {
            return false;
        }

        if (stmt->params != NIL) {
            ListCell* param_cell = NULL;

            foreach (param_cell, stmt->params) {
                if (!traverse_expr(func, (PLpgSQL_expr*)lfirst(param_cell))) {
                    return false;
                }
            }
        }
    }

    return true;
}

/*
 * @Description : traverse the RAISE struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the RAISE struct of trigger body.
 * @return : the RAISE struct can shippable or not.
 */
bool traverse_raise(PLpgSQL_function* func, PLpgSQL_stmt_raise* stmt)
{
    ListCell* cell = NULL;

    if (stmt == NULL) {
        return true;
    }

    if (stmt->params != NIL) {
        foreach (cell, stmt->params) {
            if (!traverse_expr(func, (PLpgSQL_expr*)lfirst(cell))) {
                return false;
            }
        }
    }

    if (stmt->options != NIL) {
        foreach (cell, stmt->options) {
            PLpgSQL_raise_option* opt = (PLpgSQL_raise_option*)lfirst(cell);

            if (opt->expr && !traverse_expr(func, opt->expr)) {
                return false;
            }
        }
    }

    return true;
}

/*
 * @Description : traverse the EXECSQL struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the EXECSQL struct of trigger body.
 * @return : the EXECSQL struct can shippable or not.
 */
bool traverse_execsql(PLpgSQL_function* func, PLpgSQL_stmt_execsql* stmt)
{
    if (stmt == NULL || stmt->sqlstmt == NULL) {
        return true;
    }

    return traverse_expr(func, stmt->sqlstmt);
}

/*
 * @Description : traverse the DYNAMIC EXECUTE struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the DYNAMIC EXECUTE struct of trigger body.
 * @return : the DYNAMIC EXECUTE struct can shippable or not.
 */
bool traverse_dynexecute(PLpgSQL_function* func, PLpgSQL_stmt_dynexecute* stmt)
{
    if (stmt == NULL) {
        return true;
    }

    if (stmt->query && !traverse_expr(func, stmt->query)) {
        return false;
    }

    if (stmt->params != NIL) {
        ListCell* param_cell = NULL;

        foreach (param_cell, stmt->params) {
            if (!traverse_expr(func, (PLpgSQL_expr*)lfirst(param_cell))) {
                return false;
            }
        }
    }

    return true;
}

/*
 * @Description : traverse the DYNAMIC FOR struct to check shippable.
 * @in func : the function of trigger body which need be checked.
 * @in stmt : the DYNAMIC FOR struct of trigger body.
 * @return : the DYNAMIC FOR struct can shippable or not.
 */
bool traverse_dynfors(PLpgSQL_function* func, PLpgSQL_stmt_dynfors* stmt)
{
    ListCell* param_cell = NULL;

    if (stmt == NULL) {
        return true;
    }

    if (stmt->query && !traverse_expr(func, stmt->query)) {
        return false;
    }

    if (stmt->params != NULL) {
        foreach (param_cell, stmt->params) {
            if (!traverse_expr(func, (PLpgSQL_expr*)lfirst(param_cell))) {
                return false;
            }
        }
    }

    return traverse_stmts(func, stmt->body);
}

/*
 * @Description : traverse the GET DIAGNOSTICS struct to check shippable.
 * @in stmt : the GET DIAGNOSTICS struct of trigger body.
 * @return : just return true now.
 */
bool traverse_getdiag(PLpgSQL_stmt_getdiag* stmt)
{
    return true;
}

/*
 * @Description : main function to check shippable by expr in trigger body.
 * @in func : the function of trigger body which need be checked.
 * @in expr : the expr in trigger body.
 * @return : judge whether shippable by the expr.
 */
bool traverse_expr(PLpgSQL_function* func, PLpgSQL_expr* expr)
{
    if (expr == NULL) {
        return true;
    }

    expr->func = func;
    return pgxc_is_query_shippable_in_trigger(expr);
}

bool traverse_savepoint(PLpgSQL_function* func, PLpgSQL_stmt_savepoint *stmt)
{
    return false;
}

bool traverse_commit(PLpgSQL_function* func, PLpgSQL_stmt_commit *stmt)
{
    return false;
}

bool traverse_rollback(PLpgSQL_function* func, PLpgSQL_stmt_rollback *stmt)
{
    return false;
}

/*
 * @Description : main entry function for check whether trigger body is shippable.
 * @in func : the function of trigger body which need be checked.
 * @return : true for shippable and false for unshippable.
 */
bool plpgsql_is_trigger_shippable(PLpgSQL_function* func)
{
    PLpgSQL_rec* rec_new = NULL;
    PLpgSQL_rec* rec_old = NULL;

    /*
     * Put the OLD and NEW tuples into record variables
     *
     * We make the tupdescs available in both records even though only one may
     * have a value.  This allows parsing of record references to succeed in
     * functions that are used for multiple trigger types.  For example, we
     * might have a test like "if (TG_OP = 'INSERT' and NEW.foo = 'xyz')",
     * which should parse regardless of the current trigger type.
     */
    rec_new = (PLpgSQL_rec*)(func->datums[func->new_varno]);
    rec_new->freetup = false;
    rec_new->tupdesc = func->tg_relation->rd_att;
    rec_new->freetupdesc = false;
    rec_old = (PLpgSQL_rec*)(func->datums[func->old_varno]);
    rec_old->freetup = false;
    rec_old->tupdesc = func->tg_relation->rd_att;
    rec_old->freetupdesc = false;
    return traverse_block(func, func->action);
}
