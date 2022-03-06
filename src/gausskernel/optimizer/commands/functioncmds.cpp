/* -------------------------------------------------------------------------
 *
 * functioncmds.cpp
 *
 *	  Routines for CREATE and DROP FUNCTION commands and CREATE and DROP
 *	  CAST commands.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
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
 *		"create function":
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

#include "access/genam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/gs_encrypted_proc.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_object.h"
#include "catalog/pg_proc.h"
#include "catalog/gs_package.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_fn.h"
#include "catalog/gs_db_privilege.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "commands/proclang.h"
#include "commands/typecmds.h"
#include "executor/executor.h"
#include "gs_policy/gs_policy_masking.h"
#include "miscadmin.h"
#include "optimizer/var.h"
#include "optimizer/nodegroups.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "storage/tcap.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/pl_package.h"
#include "catalog/pg_class.h"
#include "access/transam.h"
#include "pgxc/execRemote.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"
#include "tsearch/ts_type.h"

typedef struct PendingLibraryDelete {
    char* filename; /* library file name. */
    bool atCommit;  /* T=delete at commit; F=delete at abort */
} PendingLibraryDelete;

static void AlterFunctionOwner_internal(Relation rel, HeapTuple tup, Oid newOwnerId);
static void checkAllowAlter(HeapTuple tup);
static int2vector* GetDefaultArgPos(List* defargpos);

/*
 *	 Examine the RETURNS clause of the CREATE FUNCTION statement
 *	 and return information about it as *prorettype_p and *returnsSet.
 *
 * This is more complex than the average typename lookup because we want to
 * allow a shell type to be used, or even created if the specified return type
 * doesn't exist yet.  (Without this, there's no way to define the I/O procs
 * for a new type.)  But SQL function creation won't cope, so error out if
 * the target language is SQL.	(We do this here, not in the SQL-function
 * validator, so as not to produce a NOTICE and then an ERROR for the same
 * condition.)
 */
static void compute_return_type(
    TypeName* returnType, Oid languageOid, Oid* prorettype_p, bool* returnsSet_p, bool fenced, int startLineNumber)
{
    Oid rettype;
    Type typtup;
    AclResult aclresult;
    Oid typowner = InvalidOid;
    /*
     * isalter is true, change the owner of the objects as the owner of the
     * namespace, if the owner of the namespce has the same name as the namescpe
     */
    bool isalter = false;

    typtup = LookupTypeName(NULL, returnType, NULL);

    /*
     * If the type is relation, then we check
     * whether the table is in installation group
     */
    if (!in_logic_cluster() && !IsTypeTableInInstallationGroup(typtup)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("return type '%s' must be in installation group", TypeNameToString(returnType))));
    }

    if (typtup) {
        if (!((Form_pg_type)GETSTRUCT(typtup))->typisdefined) {
            if (languageOid == SQLlanguageId)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                        errmsg("SQL function cannot return shell type %s", TypeNameToString(returnType))));
            else if (fenced)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                        errmsg("fencedmode function cannot accept shell type %s", TypeNameToString(returnType))));
            else
                ereport(NOTICE,
                    (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("return type %s is only a shell", TypeNameToString(returnType))));
        }

        /* if table of type, find its array type */
        if (((Form_pg_type)GETSTRUCT(typtup))->typtype == TYPTYPE_TABLEOF) {
            rettype = ((Form_pg_type)GETSTRUCT(typtup))->typelem;
            if (!OidIsValid(rettype)) {
                ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("table of type %s typelem does not exist", TypeNameToString(returnType))));
            }

            if (((Form_pg_type)GETSTRUCT(typtup))->typcategory == TYPCATEGORY_TABLEOF_VARCHAR ||
                ((Form_pg_type)GETSTRUCT(typtup))->typcategory == TYPCATEGORY_TABLEOF_INTEGER) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmodule(MOD_PLSQL),
                            errmsg("table of index type is not supported as function return type."),
                            errdetail("N/A"),
                            errcause("feature not supported"),
                            erraction("check define of funtion")));
            }
        } else {
            rettype = typeTypeId(typtup);
        }

        ReleaseSysCache(typtup);
    } else {
        char* typnam = TypeNameToString(returnType);
        Oid namespaceId;
        char* typname = NULL;

        /*
         * Only C-coded functions can be I/O functions.  We enforce this
         * restriction here mainly to prevent littering the catalogs with
         * shell types due to simple typos in user-defined function
         * definitions.
         * At present, we only allow auto-created shell types during initdb.
         */
        if ((!IsInitdb && !u_sess->exec_cxt.extension_is_valid) ||
            (languageOid != INTERNALlanguageId && languageOid != ClanguageId)) {
            const char* message = "type does not exist";
            InsertErrorMessage(message, startLineNumber);
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type \"%s\" does not exist", typnam)));
        }

        /* Reject if there's typmod decoration, too */
        if (returnType->typmods != NIL)
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("type modifier cannot be specified for shell type \"%s\"", typnam)));

        /* Otherwise, go ahead and make a shell type */
        ereport(NOTICE,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("type \"%s\" is not yet defined", typnam),
                errdetail("Creating a shell type definition.")));
        namespaceId = QualifiedNameGetCreationNamespace(returnType->names, &typname);

        if (u_sess->attr.attr_sql.enforce_a_behavior) {
            typowner = GetUserIdFromNspId(namespaceId);

            if (!OidIsValid(typowner))
                typowner = GetUserId();
            else if (typowner != GetUserId())
                isalter = true;
        } else {
            typowner = GetUserId();
        }
        aclresult = pg_namespace_aclcheck(namespaceId, GetUserId(), ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(namespaceId));
        if (isalter) {
            aclresult = pg_namespace_aclcheck(namespaceId, typowner, ACL_CREATE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(namespaceId));
        }
        rettype = TypeShellMake(typname, namespaceId, typowner);
        Assert(OidIsValid(rettype));
    }

    aclresult = pg_type_aclcheck(rettype, GetUserId(), ACL_USAGE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error_type(aclresult, rettype);
    if (isalter) {
        aclresult = pg_type_aclcheck(rettype, typowner, ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error_type(aclresult, rettype);
    }

    *prorettype_p = rettype;
    *returnsSet_p = returnType->setof;
}

/*
 * Interpret the parameter list of the CREATE FUNCTION statement.
 *
 * Results are stored into output parameters.  parameterTypes must always
 * be created, but the other arrays are set to NULL if not needed.
 * requiredResultType is set to InvalidOid if there are no OUT parameters,
 * else it is set to the OID of the implied result type.
 */
static void examine_parameter_list(List* parameters, Oid languageOid, const char* queryString,
    oidvector** parameterTypes, ArrayType** allParameterTypes, ArrayType** parameterModes, ArrayType** parameterNames,
    List** parameterDefaults, Oid* requiredResultType, List** defargpos, bool fenced)
{
    int parameterCount = list_length(parameters);
    Oid* inTypes = NULL;
    int inCount = 0;
    Datum* allTypes = NULL;
    Datum* paramModes = NULL;
    Datum* paramNames = NULL;
    int outCount = 0;
    int varCount = 0;
    bool have_names = false;
    ListCell* x = NULL;
    int i;
    ParseState* pstate = NULL;

    *requiredResultType = InvalidOid; /* default result */

    if ((INT_MAX / sizeof(Oid) < (uint)parameterCount) || (INT_MAX / sizeof(Datum) < (uint)parameterCount)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("parameterCount is invalid %d", parameterCount)));
    }

    /* parameterCount will be zero when someone create a function without paramters. */
    if (parameterCount != 0) {
        inTypes = (Oid*)palloc(parameterCount * sizeof(Oid));
        allTypes = (Datum*)palloc(parameterCount * sizeof(Datum));
        paramModes = (Datum*)palloc(parameterCount * sizeof(Datum));
        paramNames = (Datum*)palloc0(parameterCount * sizeof(Datum));
    }

    *parameterDefaults = NIL;

    /* may need a pstate for parse analysis of default exprs */
    pstate = make_parsestate(NULL);
    pstate->p_sourcetext = queryString;

    /* Scan the list and extract data into work arrays */
    i = 0;
    foreach (x, parameters) {
        FunctionParameter* fp = (FunctionParameter*)lfirst(x);
        TypeName* t = fp->argType;
        bool isinput = false;
        Oid toid = InvalidOid;
        Type typtup;
        AclResult aclresult;
        char* objname = NULL;
        objname = strVal(linitial(t->names));
        typtup = LookupTypeName(NULL, t, NULL);
        if (!HeapTupleIsValid(typtup)) {
            toid = findPackageParameter(objname);
        }
        /*
         * If the type is relation, then we check
         * whether the table is in installation group
         */
        if (!in_logic_cluster() && !IsTypeTableInInstallationGroup(typtup)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("argument type '%s' must be in installation group", TypeNameToString(t))));
        }

        if (typtup) {
            if (!((Form_pg_type)GETSTRUCT(typtup))->typisdefined) {
                /* As above, hard error if language is SQL */
                if (languageOid == SQLlanguageId)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                            errmsg("SQL function cannot accept shell type %s", TypeNameToString(t))));
                else if (fenced)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                            errmsg("fencedmode function cannot accept shell type %s", TypeNameToString(t))));
                else
                    ereport(NOTICE,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("argument type %s is only a shell", TypeNameToString(t))));
            }

            toid = typeTypeId(typtup);
            ReleaseSysCache(typtup);
        } else if (!OidIsValid(toid)) {
            int rc = 0;
            rc = CompileWhich();
            if (rc == PLPGSQL_COMPILE_PACKAGE ||
                rc == PLPGSQL_COMPILE_PACKAGE_PROC) {
                Oid packageOid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
                char message[MAXSTRLEN];
                errno_t rc = 0;
                rc = sprintf_s(message, MAXSTRLEN, "type %s does not exist.", TypeNameToString(t));
                securec_check_ss_c(rc, "", "");
                InsertErrorMessage(message, 0, true);
                InsertError(packageOid);
            }
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type %s does not exist", TypeNameToString(t))));
            toid = InvalidOid; /* keep compiler quiet */
        }

        aclresult = pg_type_aclcheck(toid, GetUserId(), ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error_type(aclresult, toid);

        if (t->setof)
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), errmsg("functions cannot accept set arguments")));

        /* handle input parameters */
        if (fp->mode != FUNC_PARAM_OUT && fp->mode != FUNC_PARAM_TABLE) {
            /* other input parameters can't follow a VARIADIC parameter */
            if (varCount > 0)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                        errmsg("VARIADIC parameter must be the last input parameter")));
            inTypes[inCount++] = toid;
            isinput = true;
        }

        /* handle output parameters */
        if (fp->mode != FUNC_PARAM_IN && fp->mode != FUNC_PARAM_VARIADIC) {
            if (outCount == 0) /* save first output param's type */
                *requiredResultType = toid;
            outCount++;
        }

        if (fp->mode == FUNC_PARAM_VARIADIC) {
            varCount++;
            /* validate variadic parameter type */
            switch (toid) {
                case ANYARRAYOID:
                case ANYOID:
                    /* okay */
                    break;
                default:
                    if (!OidIsValid(get_element_type(toid)))
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                                errmsg("VARIADIC parameter must be an array")));
                    break;
            }
        }

        allTypes[i] = ObjectIdGetDatum(toid);

        paramModes[i] = CharGetDatum(fp->mode);

        if (fp->name && fp->name[0]) {
            ListCell* px = NULL;

            /*
             * As of Postgres 9.0 we disallow using the same name for two
             * input or two output function parameters.  Depending on the
             * function's language, conflicting input and output names might
             * be bad too, but we leave it to the PL to complain if so.
             */
            foreach (px, parameters) {
                FunctionParameter* prevfp = (FunctionParameter*)lfirst(px);

                if (prevfp == fp)
                    break;
                /* pure in doesn't conflict with pure out */
                if ((fp->mode == FUNC_PARAM_IN || fp->mode == FUNC_PARAM_VARIADIC) &&
                    (prevfp->mode == FUNC_PARAM_OUT || prevfp->mode == FUNC_PARAM_TABLE))
                    continue;
                if ((prevfp->mode == FUNC_PARAM_IN || prevfp->mode == FUNC_PARAM_VARIADIC) &&
                    (fp->mode == FUNC_PARAM_OUT || fp->mode == FUNC_PARAM_TABLE))
                    continue;
                if (prevfp->name && prevfp->name[0] && strcmp(prevfp->name, fp->name) == 0)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                            errmsg("parameter name \"%s\" used more than once", fp->name)));
            }

            paramNames[i] = CStringGetTextDatum(fp->name);
            have_names = true;
        }
		
        if (fp->defexpr) {
#ifndef ENABLE_MULTIPLE_NODES
            if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT 
                && (fp->mode == FUNC_PARAM_OUT || fp->mode == FUNC_PARAM_INOUT)) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                    errmsg("The out/inout Parameter can't have default value.")));
            }
#endif		
            Node* def = NULL;

            if (!isinput)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                        errmsg("only input parameters can have default values")));

            def = transformExpr(pstate, fp->defexpr);
            def = coerce_to_specific_type(pstate, def, toid, "DEFAULT");
            assign_expr_collations(pstate, def);

            /*
             * Make sure no variables are referred to.
             */
            if (list_length(pstate->p_rtable) != 0 || contain_var_clause(def))
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                        errmsg("cannot use table references in parameter default value")));

            /*
             * It can't return a set either --- but coerce_to_specific_type
             * already checked that for us.
             *
             * No subplans or aggregates, either...
             *
             * Note: the point of these restrictions is to ensure that an
             * expression that, on its face, hasn't got subplans, aggregates,
             * etc cannot suddenly have them after function default arguments
             * are inserted.
             */
            if (pstate->p_hasSubLinks)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot use subquery in parameter default value")));
            if (pstate->p_hasAggs)
                ereport(ERROR,
                    (errcode(ERRCODE_GROUPING_ERROR),
                        errmsg("cannot use aggregate function in parameter default value")));
            if (pstate->p_hasWindowFuncs)
                ereport(ERROR,
                    (errcode(ERRCODE_WINDOWING_ERROR),
                        errmsg("cannot use window function in parameter default value")));

            *parameterDefaults = lappend(*parameterDefaults, def);

            /*
             * record the position of the argument with default value.
             */
            if (defargpos != NULL)
                *defargpos = lappend_int(*defargpos, i);
        }

        i++;
    }

    free_parsestate(pstate);

    /* Now construct the proper outputs as needed */

    /* if there are no paramters (parameterCount is 0), we make a InvalidOidVector */
    *parameterTypes = buildoidvector(inTypes, inCount);

    if (outCount > 0 || varCount > 0) {
        *allParameterTypes = construct_array(allTypes, parameterCount, OIDOID, sizeof(Oid), true, 'i');
        *parameterModes = construct_array(paramModes, parameterCount, CHAROID, 1, true, 'c');
        if (outCount > 1)
            *requiredResultType = RECORDOID;
        /* otherwise we set requiredResultType correctly above */
    } else {
        *allParameterTypes = NULL;
        *parameterModes = NULL;
    }

    if (have_names) {
        for (i = 0; i < parameterCount; i++) {
            if (paramNames[i] == PointerGetDatum(NULL))
                paramNames[i] = CStringGetTextDatum("");
        }
        *parameterNames = construct_array(paramNames, parameterCount, TEXTOID, -1, false, 'i');
    } else {
        *parameterNames = NULL;
    }
}

/*
 * Recognize one of the options that can be passed to both CREATE
 * FUNCTION and ALTER FUNCTION and return it via one of the out
 * parameters. Returns true if the passed option was recognized. If
 * the out parameter we were going to assign to points to non-NULL,
 * raise a duplicate-clause error.	(We don't try to detect duplicate
 * SET parameters though --- if you're redundant, the last one wins.)
 */
static bool compute_common_attribute(DefElem* defel, DefElem** volatility_item, DefElem** strict_item,
    DefElem** security_item, DefElem** leakproof_item, List** set_items, DefElem** cost_item, DefElem** rows_item,
    DefElem** fencedItem, DefElem** shippable_item, DefElem** package_item)
{
    if (strcmp(defel->defname, "volatility") == 0) {
        if (*volatility_item)
            goto duplicate_error;

        *volatility_item = defel;
    } else if (strcmp(defel->defname, "strict") == 0) {
        if (*strict_item)
            goto duplicate_error;

        *strict_item = defel;
    } else if (strcmp(defel->defname, "security") == 0) {
        if (*security_item)
            goto duplicate_error;

        *security_item = defel;
    } else if (strcmp(defel->defname, "leakproof") == 0) {
        if (*leakproof_item)
            goto duplicate_error;

        *leakproof_item = defel;
    } else if (strcmp(defel->defname, "set") == 0) {
        *set_items = lappend(*set_items, defel->arg);
    } else if (strcmp(defel->defname, "cost") == 0) {
        if (*cost_item)
            goto duplicate_error;

        *cost_item = defel;
    } else if (strcmp(defel->defname, "rows") == 0) {
        if (*rows_item)
            goto duplicate_error;

        *rows_item = defel;
    } else if (strcmp(defel->defname, "fenced") == 0) {
        if (*fencedItem)
            goto duplicate_error;

        *fencedItem = defel;
    } else if (strcmp(defel->defname, "shippable") == 0) {
        if (*shippable_item)
            goto duplicate_error;

        *shippable_item = defel;
    } else if (strcmp(defel->defname, "package") == 0) {
        if (*package_item)
            goto duplicate_error;

        *package_item = defel;
    } else
        return false;

    /* Recognized an option */
    return true;

duplicate_error:
    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
    return false; /* keep compiler quiet */
}

static char interpret_func_volatility(DefElem* defel)
{
    char* str = strVal(defel->arg);

    if (strcmp(str, "immutable") == 0)
        return PROVOLATILE_IMMUTABLE;
    else if (strcmp(str, "stable") == 0)
        return PROVOLATILE_STABLE;
    else if (strcmp(str, "volatile") == 0)
        return PROVOLATILE_VOLATILE;
    else {
        ereport(ERROR, (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmsg("invalid volatility \"%s\"", str)));
        return 0; /* keep compiler quiet */
    }
}

/*
 * Update a proconfig value according to a list of VariableSetStmt items.
 *
 * The input and result may be NULL to signify a null entry.
 */
static ArrayType* update_proconfig_value(ArrayType* a, const List* set_items)
{
    ListCell* l = NULL;

    foreach (l, set_items) {
        VariableSetStmt* sstmt = (VariableSetStmt*)lfirst(l);

        Assert(IsA(sstmt, VariableSetStmt));
        if (sstmt->kind == VAR_RESET_ALL)
            a = NULL;
        else {
            char* valuestr = ExtractSetVariableArgs(sstmt);

            if (valuestr != NULL)
                a = GUCArrayAdd(a, sstmt->name, valuestr);
            else /* RESET */
                a = GUCArrayDelete(a, sstmt->name);
        }
    }

    return a;
}

/*
 * Dissect the list of options assembled in gram.y into function
 * attributes.
 */
static void compute_attributes_sql_style(const List* options, List** as, char** language, bool* windowfunc_p,
    char* volatility_p, bool* strict_p, bool* security_definer, bool* leakproof_p, ArrayType** proconfig,
    float4* procost, float4* prorows, bool* fenced, bool* shippable, bool* package)
{
    ListCell* option = NULL;
    DefElem* as_item = NULL;
    DefElem* language_item = NULL;
    DefElem* windowfunc_item = NULL;
    DefElem* volatility_item = NULL;
    DefElem* strict_item = NULL;
    DefElem* security_item = NULL;
    DefElem* leakproof_item = NULL;
    List* set_items = NIL;
    DefElem* cost_item = NULL;
    DefElem* rows_item = NULL;
    DefElem* fencedItem = NULL;
    DefElem* shippable_item = NULL;
    DefElem* package_item = NULL;

    foreach (option, options) {
        DefElem* defel = (DefElem*)lfirst(option);

        if (strcmp(defel->defname, "as") == 0) {
            if (as_item != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            as_item = defel;
        } else if (strcmp(defel->defname, "language") == 0) {
            if (language_item != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            language_item = defel;
        } else if (strcmp(defel->defname, "window") == 0) {
            if (windowfunc_item != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            windowfunc_item = defel;
        } else if (compute_common_attribute(defel,
                       &volatility_item,
                       &strict_item,
                       &security_item,
                       &leakproof_item,
                       &set_items,
                       &cost_item,
                       &rows_item,
                       &fencedItem,
                       &shippable_item,
                       &package_item)) {
            /* recognized common option */
            continue;
        } else
            ereport(ERROR,
                (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmsg("option \"%s\" not recognized", defel->defname)));
    }

    /* process required items */
    if (as_item != NULL)
        *as = (List*)as_item->arg;
    else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), errmsg("no function body specified")));
        *as = NIL; /* keep compiler quiet */
    }

    if (language_item != NULL)
        *language = strVal(language_item->arg);
    else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), errmsg("no language specified")));
        *language = NULL; /* keep compiler quiet */
    }

    /* process optional items */
    if (windowfunc_item != NULL)
        *windowfunc_p = intVal(windowfunc_item->arg);
    if (volatility_item != NULL)
        *volatility_p = interpret_func_volatility(volatility_item);
    if (strict_item != NULL)
        *strict_p = intVal(strict_item->arg);
    if (security_item != NULL)
        *security_definer = intVal(security_item->arg);
    if (leakproof_item != NULL)
        *leakproof_p = intVal(leakproof_item->arg);
    if (set_items != NULL)
        *proconfig = update_proconfig_value(NULL, (const List*)set_items);
    if (cost_item != NULL) {
        *procost = defGetNumeric(cost_item);
        if (*procost <= 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("COST must be positive")));
    }
    if (rows_item != NULL) {
        *prorows = defGetNumeric(rows_item);
        if (*prorows <= 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("ROWS must be positive")));
    }

    if (strcmp(*language, "c") != 0 && strcmp(*language, "java") != 0 && 
            strncmp(*language, "plpython", strlen("plpython")) != 0) {
        if (fencedItem == NULL) {
            *fenced = false;
        } else if (intVal(fencedItem->arg) == 1) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("fencedmode could not be fenced")));
        }
    }
    if (fencedItem != NULL) {
        *fenced = intVal(fencedItem->arg);
    }
    if (shippable_item != NULL) {
        *shippable = intVal(shippable_item->arg);
        if (!*shippable && *volatility_p == PROVOLATILE_IMMUTABLE) {
            elog(NOTICE, "Immutable function will be shippable anyway.");
        }
    }

    if (package_item != NULL) {
        *package = intVal(package_item->arg);
    }
    list_free(set_items);
}

/* -------------
 *	 Interpret the parameters *parameters and return their contents via
 *	 *isStrict_p and *volatility_p.
 *
 *	These parameters supply optional information about a function.
 *	All have defaults if not specified. Parameters:
 *
 *	 * isStrict means the function should not be called when any NULL
 *	   inputs are present; instead a NULL result value should be assumed.
 *
 *	 * volatility tells the optimizer whether the function's result can
 *	   be assumed to be repeatable over multiple evaluations.
 * ------------
 */
static void compute_attributes_with_style(const List* parameters, bool* isStrict_p, char* volatility_p)
{
    ListCell* pl = NULL;

    foreach (pl, parameters) {
        DefElem* param = (DefElem*)lfirst(pl);

        if (pg_strcasecmp(param->defname, "isstrict") == 0)
            *isStrict_p = defGetBoolean(param);
        else if (pg_strcasecmp(param->defname, "iscachable") == 0) {
            /* obsolete spelling of isImmutable */
            if (defGetBoolean(param))
                *volatility_p = PROVOLATILE_IMMUTABLE;
        } else
            ereport(WARNING,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("unrecognized function attribute \"%s\" ignored", param->defname)));
    }
}

/*
 * For a dynamically linked C language object, the form of the clause is
 *
 *	   AS <object file name> [, <link symbol name> ]
 *
 * In all other cases
 *
 *	   AS <object reference, or sql code>
 */
static void interpret_AS_clause(
    Oid languageOid, const char* languageName, char* funcname, List* as, char** prosrc_str_p, char** probin_str_p)
{
    Assert(as != NIL);

    if (languageOid == ClanguageId) {
        /*
         * For "C" language, store the file name in probin and, when given,
         * the link symbol name in prosrc.	If link symbol is omitted,
         * substitute procedure name.  We also allow link symbol to be
         * specified as "-", since that was the habit in PG versions before
         * 8.4, and there might be dump files out there that don't translate
         * that back to "omitted".
         */
        *probin_str_p = strVal(linitial(as));
        if (list_length(as) == 1)
            *prosrc_str_p = funcname;
        else {
            *prosrc_str_p = strVal(lsecond(as));
            if (strcmp(*prosrc_str_p, "-") == 0)
                *prosrc_str_p = funcname;
        }
    } else {
        /* Everything else wants the given string in prosrc. */
        *prosrc_str_p = strVal(linitial(as));
        *probin_str_p = NULL;

        if (list_length(as) != 1)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                    errmsg("only one AS item needed for language \"%s\"", languageName)));

        if (languageOid == INTERNALlanguageId) {
            /*
             * In PostgreSQL versions before 6.5, the SQL name of the created
             * function could not be different from the internal name, and
             * "prosrc" wasn't used.  So there is code out there that does
             * CREATE FUNCTION xyz AS '' LANGUAGE internal. To preserve some
             * modicum of backwards compatibility, accept an empty "prosrc"
             * value as meaning the supplied SQL function name.
             */
            if (strlen(*prosrc_str_p) == 0)
                *prosrc_str_p = funcname;
        }
    }
}

/*
 * @Description: check user define window function is valid
 * @oid languageOid - language oid
 * @char* prosrc_str - function body with internal language
 * @return -void
 */
static void CheckWindowFuncValid(Oid languageOid, const char* prosrc_str)
{
    if (languageOid != INTERNALlanguageId) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("User defined window function only support INTERNAL language.")));
    } else {
        Oid funcoid = InvalidOid;
        bool is_window = false;
        if (prosrc_str != NULL)
            funcoid = fmgr_internal_function(prosrc_str);

        if (OidIsValid(funcoid)) {
            is_window = get_func_iswindow(funcoid);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                    errmsg("there is no built-in function named \"%s\"", prosrc_str)));
        }

        if (is_window == false) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("User defined window function only support INTERNAL window function")));
        }
    }
}

/*
 * CreateFunction
 *	 Execute a CREATE FUNCTION utility statement.
 */
void CreateFunction(CreateFunctionStmt* stmt, const char* queryString, Oid pkg_oid)
{
    char* probin_str = NULL;
    char* prosrc_str = NULL;
    Oid prorettype;
    bool returnsSet = false;
    char* language = NULL;
    Oid languageOid;
    Oid languageValidator;
    char* funcname = NULL;
    Oid namespaceId = InvalidOid;
    AclResult aclresult;
    oidvector* parameterTypes = NULL;
    ArrayType* allParameterTypes = NULL;
    ArrayType* parameterModes = NULL;
    ArrayType* parameterNames = NULL;
    List* parameterDefaults = NIL;
    Oid requiredResultType;
    bool isWindowFunc = false;
    bool isStrict = false;
    bool security = false;
    bool isLeakProof = false;
    char volatility;
    ArrayType* proconfig = NULL;
    float4 procost;
    float4 prorows;
    HeapTuple languageTuple;
    Form_pg_language languageStruct;
    List* as_clause = NIL;
    List* defargpos = NIL;
    int2vector* prodefaultargpos = NULL;
    Oid proowner = InvalidOid;
    bool fenced = IS_SINGLE_NODE ? false : true;
    bool shippable = false;
    bool package = false;
    bool proIsProcedure = stmt->isProcedure;
    if (!OidIsValid(pkg_oid)) {
        u_sess->plsql_cxt.debug_query_string = pstrdup(queryString);
    }
    if (PLSQL_SECURITY_DEFINER && u_sess->attr.attr_common.upgrade_mode == 0 && OidIsValid(pkg_oid)) {
        bool isnull = false;
        HeapTuple pkgTuple = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(pkg_oid));
        Datum pkgSecDefDatum = SysCacheGetAttr(PACKAGEOID, pkgTuple, Anum_gs_package_pkgsecdef, &isnull);
        if (isnull) {
            security = false;
        } else {
            bool pkgSecDef = DatumGetBool(pkgSecDefDatum);
            if (!pkgSecDef) {
                security = false;
            } else {
                security = true;
            }
        }
        ReleaseSysCache(pkgTuple);
    } else if (PLSQL_SECURITY_DEFINER && u_sess->attr.attr_common.upgrade_mode == 0) {
        security = true;
    }
    probin_str = NULL;
    prosrc_str = NULL;
    int rc = 0;
    rc = CompileWhich();
    if (rc == PLPGSQL_COMPILE_PACKAGE) {
        u_sess->plsql_cxt.procedure_start_line = stmt->startLineNumber;
        u_sess->plsql_cxt.procedure_first_line = stmt->firstLineNumber;
    }
    u_sess->plsql_cxt.isCreateFunction = true;
    /*
     * isalter is true, change the owner of the objects as the owner of the
     * namespace, if the owner of the namespce has the same name as the namescpe
     */
    bool isalter = false;
    /* Convert list of names to a name and namespace */
    if (!OidIsValid(pkg_oid)) {
        namespaceId = QualifiedNameGetCreationNamespace(stmt->funcname, &funcname);
    } else {
        char *schemaname = NULL;
        DeconstructQualifiedName(stmt->funcname, &schemaname, &funcname);
        HeapTuple tuple = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(pkg_oid));
        if (HeapTupleIsValid(tuple)) {
            Form_gs_package pkgform = (Form_gs_package)GETSTRUCT(tuple);
            namespaceId = pkgform->pkgnamespace;
        } else {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_PACKAGE), errmsg("package not found")));
        }
        ReleaseSysCache(tuple);
    }

    if (!IsInitdb && !u_sess->attr.attr_common.IsInplaceUpgrade && IsPackageSchemaOid(namespaceId)) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Permission denied to create function \"%s\"", funcname),
                errdetail("Object creation is not supported for %s schema.", get_namespace_name(namespaceId)),
                errcause("The schema in the package does not support object creation.."),
                erraction("Please create an object in another schema.")));
    }
    bool anyResult = CheckCreatePrivilegeInNamespace(namespaceId, GetUserId(), CREATE_ANY_FUNCTION);
    if (namespaceId == PG_PUBLIC_NAMESPACE && !isRelSuperuser()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to create function \"%s\"", funcname),
                errhint("must be %s to create a function in public schema.",
                    g_instance.attr.attr_security.enablePrivilegesSeparate  ? "initial user" : "sysadmin")));
    }

    //@Temp Table. Lock Cluster after determine whether is a temp object,
    // so we can decide if locking other coordinator
    pgxc_lock_for_utility_stmt((Node*)stmt, namespaceId == u_sess->catalog_cxt.myTempNamespace);

    if (u_sess->attr.attr_sql.enforce_a_behavior) {
        proowner = GetUserIdFromNspId(namespaceId, false, anyResult);

        if (!OidIsValid(proowner))
            proowner = GetUserId();
        else if (proowner != GetUserId())
            isalter = true;

        if (isalter) {
            (void)CheckCreatePrivilegeInNamespace(namespaceId, proowner, CREATE_ANY_FUNCTION);
        }
    } else {
        proowner = GetUserId();
    }
    /* default attributes */
    volatility = PROVOLATILE_VOLATILE;
    procost = -1; /* indicates not set */
    prorows = -1; /* indicates not set */
    shippable = false;

    /* override attributes from explicit list */
    compute_attributes_sql_style((const List*)stmt->options, &as_clause, &language, &isWindowFunc, &volatility,
        &isStrict, &security, &isLeakProof, &proconfig, &procost, &prorows, &fenced, &shippable, &package);

    /* Look up the language and validate permissions */
    languageTuple = SearchSysCache1(LANGNAME, PointerGetDatum(language));
    if (!HeapTupleIsValid(languageTuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("language \"%s\" does not exist", language),
                (PLTemplateExists(language) ? errhint("Use CREATE LANGUAGE to load the language into the database.")
                                            : 0)));

    languageOid = HeapTupleGetOid(languageTuple);

#ifdef ENABLE_MULTIPLE_NODES
    if (languageOid == JavalanguageId) {
        /*
         * single node dose not support Java UDF or other fenced functions.
         * check it here because users may not know the Java UDF is fenced by default,
         * so it's better to report detailed error messages for different senarios.
         */
        if (IS_SINGLE_NODE) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("JAVA UDF is not yet supported in current version.")));
        }

        /* only support fenced mode Java UDF */
        if (!fenced) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Java UDF dose not support NOT FENCED functions.")));
        }
    }
#endif

    languageStruct = (Form_pg_language)GETSTRUCT(languageTuple);

    /* Check user's privilege regardless of whether langugae is a trust type */
    aclresult = pg_language_aclcheck(languageOid, GetUserId(), ACL_USAGE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_LANGUAGE, NameStr(languageStruct->lanname));
    /*
     * Actually we should do the following owner privilege check to both trust and untrust language,
     * but consider compatibility with older versions, we can't check for untrust language like c
     * and java here, it is ugly but with no solution.
     */
    if (languageStruct->lanpltrusted && isalter) {
        aclresult = pg_language_aclcheck(languageOid, proowner, ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_LANGUAGE, NameStr(languageStruct->lanname));
    }

    languageValidator = languageStruct->lanvalidator;

    ReleaseSysCache(languageTuple);

    /*
     * Only superuser is allowed to create leakproof functions because it
     * possibly allows unprivileged users to reference invisible tuples to be
     * filtered out using views for row-level security.
     */
    if (isLeakProof && !superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("only system admin can define a leakproof function")));

    /*
     * Convert remaining parameters of CREATE to form wanted by
     * ProcedureCreate.
     */
    examine_parameter_list(stmt->parameters, languageOid, queryString, &parameterTypes, &allParameterTypes,
        &parameterModes, &parameterNames, &parameterDefaults, &requiredResultType, &defargpos, fenced);

    prodefaultargpos = GetDefaultArgPos(defargpos);

    if (stmt->returnType) {
        /* explicit RETURNS clause */
        compute_return_type(stmt->returnType, languageOid, &prorettype, &returnsSet, fenced, stmt->startLineNumber);
    } else if (OidIsValid(requiredResultType)) {
        /* default RETURNS clause from OUT parameters */
        prorettype = requiredResultType;
        returnsSet = false;
    } else {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), errmsg("function result type must be specified")));
        /* Alternative possibility: default to RETURNS VOID */
        prorettype = VOIDOID;
        returnsSet = false;
    }

    if (returnsSet) {
        Oid typerelid = typeidTypeRelid(prorettype);

        if (typerelid > FirstNormalObjectId) {
            HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(typerelid));

            if (HeapTupleIsValid(tp)) {
                Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);
                if (reltup->relkind == RELKIND_VIEW || reltup->relkind == RELKIND_CONTQUERY) {
                    ReleaseSysCache(tp);
                    ereport(ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                            errmsg("function result type cannot be a view.")));
                }
                ReleaseSysCache(tp);
            }
        }
    }
    compute_attributes_with_style((const List*)stmt->withClause, &isStrict, &volatility);
    interpret_AS_clause(languageOid, language, funcname, as_clause, &prosrc_str, &probin_str);

    /*
     * Set default values for COST and ROWS depending on other parameters;
     * reject ROWS if it's not returnsSet.  NB: pg_dump knows these default
     * values, keep it in sync if you change them.
     */
    if (procost < 0) {
        /* SQL and PL-language functions are assumed more expensive */
        if (languageOid == INTERNALlanguageId || languageOid == ClanguageId)
            procost = 1;
        else
            procost = 100;
    }
    if (prorows < 0) {
        if (returnsSet)
            prorows = 1000;
        else
            prorows = 0; /* dummy value if not returnsSet */
    } else if (!returnsSet)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("ROWS is not applicable when function does not return a set")));

    if (isWindowFunc) {
        CheckWindowFuncValid(languageOid, prosrc_str);
    }

    /*
     * And now that we have all the parameters, and know we're permitted to do
     * so, go ahead and create the function.
     */
    ProcedureCreate(funcname, namespaceId, pkg_oid, stmt->isOraStyle, stmt->replace,
                    returnsSet, prorettype, proowner, languageOid, languageValidator,
        prosrc_str, /* converted to text later */
        probin_str, /* converted to text later */
        false,      /* not an aggregate */
        isWindowFunc, security, isLeakProof, isStrict, volatility, parameterTypes,
        PointerGetDatum(allParameterTypes), PointerGetDatum(parameterModes),
        PointerGetDatum(parameterNames),
        parameterDefaults,
        PointerGetDatum(proconfig),
        procost,
        prorows,
        prodefaultargpos,
        fenced,
        shippable,
        package,
        proIsProcedure,
        stmt->inputHeaderSrc,
        stmt->isPrivate);
    u_sess->plsql_cxt.procedure_start_line = 0;
    u_sess->plsql_cxt.procedure_first_line = 0;
    if (u_sess->plsql_cxt.debug_query_string != NULL && !OidIsValid(pkg_oid)) {
        pfree_ext(u_sess->plsql_cxt.debug_query_string);
    }
}

/*
 * @Description: Close library handle and delete library file.
 * @in tup: pg_proc tuple.
 * @return: If is user-defined C-function return true else return false.
 */
bool PrepareCFunctionLibrary(HeapTuple tup)
{
    /* To user-defined C function, we need close it's file handle and drop library file.*/
    bool isnull = false;
    Datum probinattr;
    char* probinstring = NULL;
    probinattr = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_probin, &isnull);

    if (!isnull) {
        probinstring = TextDatumGetCString(probinattr);

        /* If is user-define function.*/
        if (strncmp(probinstring, "$libdir/pg_plugin/", strlen("$libdir/pg_plugin/")) == 0) {
            InsertIntoPendingLibraryDelete(probinstring, true);
            pfree_ext(probinstring);
            return true;
        }
        pfree_ext(probinstring);
    }

    return false;
}

/*
 * @desctiption: Remove library file.
 */
void removeLibrary(const char* filename)
{
    char* fullname = expand_dynamic_library_name(filename);
    unlink(fullname);
}

/*
 * @Description: Append library file into pendingLibraryDeletes.
 * @filename: Library file name.
 * @in atCommit: Ture or false.
 */
void InsertIntoPendingLibraryDelete(const char* filename, bool atCommit)
{
    AutoContextSwitch newContext(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));

    PendingLibraryDelete* pending = (PendingLibraryDelete*)palloc(sizeof(PendingLibraryDelete));
    pending->filename = pstrdup(filename);

    pending->atCommit = atCommit;

    u_sess->cmd_cxt.PendingLibraryDeletes = lappend(u_sess->cmd_cxt.PendingLibraryDeletes, pending);
}

/*
 * @Description: Delete library file.
 * @in isCommit: True or false.
 */
void libraryDoPendingDeletes(bool isCommit)
{
    if (u_sess->cmd_cxt.PendingLibraryDeletes == NIL) {
        return;
    }

    /*
     * protect process variable file_list and CFuncHash, avoid they be
     * concurrent changed of different thread.
     */
    AutoMutexLock libraryLock(&dlerror_lock);
    libraryLock.lock();

    ListCell* lc = NULL;
    foreach (lc, u_sess->cmd_cxt.PendingLibraryDeletes) {
        PendingLibraryDelete* del_file = (PendingLibraryDelete*)lfirst(lc);

        /* delete files according to status of transaction */
        if (del_file->atCommit == isCommit) {
            /* Close library handle and delete this file.*/
            delete_file_handle(del_file->filename);

            removeLibrary(del_file->filename);
        }
    }

    /* Reset PendingLibraryDeletes.*/
    ResetPendingLibraryDelete();
    libraryLock.unLock();
}

/*
 * @Description: Reset PendingLibraryDeletes
 * @in dele_list: Need delete PendingLibraryDelete list.
 */
void ResetPendingLibraryDelete()
{
    ListCell* lc = NULL;
    foreach (lc, u_sess->cmd_cxt.PendingLibraryDeletes) {
        PendingLibraryDelete* del_library_file = (PendingLibraryDelete*)lfirst(lc);

        pfree_ext(del_library_file->filename);
        pfree_ext(del_library_file);
    }

    list_free_ext(u_sess->cmd_cxt.PendingLibraryDeletes);
    u_sess->cmd_cxt.PendingLibraryDeletes = NIL;
}

/*
 * Guts of function deletion.
 *
 * Note: this is also used for aggregate deletion, since the OIDs of
 * both functions and aggregates point to pg_proc.
 */
void RemoveFunctionById(Oid funcOid)
{
    Relation relation;
    HeapTuple tup;
    bool isagg = false;

    /*
     * Delete the pg_proc tuple.
     */
    relation = heap_open(ProcedureRelationId, RowExclusiveLock);

    /* if the function is a builtin function, its Oid is less than 10000.
     * we can't allow removing the builtin functions
     */
    if (IsSystemObjOid(funcOid) && u_sess->attr.attr_common.IsInplaceUpgrade == false) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("the builtin function can not be removed,its function oid is \"%u\"", funcOid)));
    }

    tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcOid));
    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcOid)));

    Form_pg_proc procedureStruct = (Form_pg_proc)GETSTRUCT(tup);
    isagg = procedureStruct->proisagg;

    if (procedureStruct->prolang == ClanguageId) {
        PrepareCFunctionLibrary(tup);
    }

    simple_heap_delete(relation, &tup->t_self);

    ReleaseSysCache(tup);

    heap_close(relation, RowExclusiveLock);

    /*
     * If there's a pg_aggregate tuple, delete that too.
     */
    if (isagg) {
        relation = heap_open(AggregateRelationId, RowExclusiveLock);

        tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(funcOid));
        if (!HeapTupleIsValid(tup)) /* should not happen */
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for pg_aggregate tuple for function %u", funcOid)));

        simple_heap_delete(relation, &tup->t_self);

        ReleaseSysCache(tup);

        heap_close(relation, RowExclusiveLock);
    }

    /* Recode time of delete function. */
    if (funcOid != InvalidOid) {
        DeletePgObject(funcOid, OBJECT_TYPE_PROC);
    }
    DropErrorByOid(PLPGSQL_PROC, funcOid); 
        ce_cache_refresh_type |= 0x20; /* refresh proc cache */
}
/*
 * Guts of function deletion.
 *
 * Note: this is also used for aggregate deletion, since the OIDs of
 * both functions and aggregates point to pg_proc.
 */
void remove_encrypted_proc_by_id(Oid funcOid)
{
    Relation relation;
    HeapTuple tup;

    /*
     * Delete the pg_proc tuple.
     */
    relation = heap_open(ClientLogicProcId, RowExclusiveLock);


    tup = SearchSysCache1(GSCLPROCOID, ObjectIdGetDatum(funcOid));
    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcOid)));

    simple_heap_delete(relation, &tup->t_self);

    ReleaseSysCache(tup);

    heap_close(relation, RowExclusiveLock);
}

/*
 * delete a package by package oid
 */
void RemovePackageById(Oid pkgOid, bool isBody)
{
    Relation relation;
    /*
     * Delete the pg_proc tuple.
     */
    relation = heap_open(PackageRelationId, RowExclusiveLock);

    HeapTuple pkgtup = SearchSysCache1(PACKAGEOID, ObjectIdGetDatum(pkgOid));
    if (!HeapTupleIsValid(pkgtup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for package %u", pkgOid)));
    if (!isBody) {
        /*
            if replace package specification,delete all function in this package first.
        */
        DropErrorByOid(PLPGSQL_PACKAGE, pkgOid); 
        simple_heap_delete(relation, &pkgtup->t_self);
    } else {
        bool nulls[Natts_gs_package];
        Datum values[Natts_gs_package];
        bool replaces[Natts_gs_package];
        for (int i = 0; i < Natts_gs_package; i++) {
            nulls[i] = false;
            values[i] = (Datum)NULL;
            replaces[i] = false;
        }
        replaces[Anum_gs_package_pkgbodyinitsrc - 1] = true;
        nulls[Anum_gs_package_pkgbodyinitsrc - 1] = true;
        replaces[Anum_gs_package_pkgbodydeclsrc - 1] = true;
        nulls[Anum_gs_package_pkgbodydeclsrc - 1] = true;
        HeapTuple newtup = heap_modify_tuple(pkgtup, RelationGetDescr(relation), values, nulls, replaces);
        DropErrorByOid(PLPGSQL_PACKAGE_BODY, pkgOid); 
        simple_heap_update(relation, &newtup->t_self, newtup);
    }
    ReleaseSysCache(pkgtup);

    heap_close(relation, RowExclusiveLock);

    /*
     * If there's a pg_aggregate tuple, delete that too.
     */

    /* Recode time of delete package. */
    if (pkgOid != InvalidOid) {
        DeletePgObject(pkgOid, OBJECT_TYPE_PKGSPEC);
    }
}

void DeleteFunctionByPackageOid(Oid package_oid) 
{
    if (!OidIsValid(package_oid)) {
        return;
    }

    HeapTuple oldtup;
    ScanKeyData entry;
    ScanKeyInit(&entry, Anum_pg_proc_packageid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(package_oid));
    Relation pg_proc_rel = heap_open(ProcedureRelationId, RowExclusiveLock);
    SysScanDesc scan = systable_beginscan(pg_proc_rel, InvalidOid, false, NULL, 1, &entry);
    while ((oldtup = systable_getnext(scan)) != NULL) {
        HeapTuple proctup = heap_copytuple(oldtup);
        Oid funcOid = InvalidOid;
        if (HeapTupleIsValid(proctup)) {
            funcOid = HeapTupleGetOid(proctup);
            if (!OidIsValid(funcOid)) {
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmodule(MOD_PLSQL),
                        errmsg("cache lookup failed for relid %u", funcOid)));
            }
        } else {
            ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmodule(MOD_PLSQL),
                        errmsg("cache lookup failed for relid %u", funcOid)));
        }
        (void)deleteDependencyRecordsFor(ProcedureRelationId, funcOid, true);
        DeleteTypesDenpendOnPackage(ProcedureRelationId, funcOid);
        /* the 'shared dependencies' also change when update. */
        deleteSharedDependencyRecordsFor(ProcedureRelationId, funcOid, 0);

        /* send invalid message for for relation holding replaced function as trigger */
        InvalidRelcacheForTriggerFunction(funcOid, ((Form_pg_proc)GETSTRUCT(proctup))->prorettype);
        RemoveFunctionById(funcOid);
        heap_freetuple(proctup);
    }    
    systable_endscan(scan);
    heap_close(pg_proc_rel, RowExclusiveLock);
}
/*
 * Rename function
 */
void RenameFunction(List* name, List* argtypes, const char* newname)
{
    Oid procOid;
    Oid namespaceOid;
    HeapTuple tup;
    Form_pg_proc procForm;
    Relation rel;
    AclResult aclresult;
    rel = heap_open(ProcedureRelationId, RowExclusiveLock);
    procOid = LookupFuncNameTypeNames(name, argtypes, false);

    /* if the function is a builtin function, its Oid is less than 10000.
     * we can't allow renaming the builtin functions
     */
    if (IsSystemObjOid(procOid) && u_sess->attr.attr_common.IsInplaceUpgrade == false) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("the builtin function can not be renamed,its function oid is \"%u\"", procOid)));
    }
    /* if the function is a masking function, we can't allow to rename it. */
    if (IsMaskingFunctionOid(procOid) && !u_sess->attr.attr_common.IsInplaceUpgrade) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("the masking function \"%s\" can not be renamed", get_func_name(procOid))));
    }

    tup = SearchSysCacheCopy1(PROCOID, ObjectIdGetDatum(procOid));
    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", procOid)));
    procForm = (Form_pg_proc)GETSTRUCT(tup);
    TrForbidAccessRbObject(ProcedureRelationId, procOid, NameStr(procForm->proname));
    if (procForm->proisagg)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%s\" is an aggregate function", NameListToString(name)),
                errhint("Use ALTER AGGREGATE to rename aggregate functions.")));
    namespaceOid = procForm->pronamespace;

    checkAllowAlter(tup);
    oidvector* proargs = ProcedureGetArgTypes(tup);
    /* make sure the new name doesn't exist */
#ifdef ENABLE_MULTIPLE_NODES
    if (SearchSysCacheExists3(PROCNAMEARGSNSP,
            CStringGetDatum(newname),
            PointerGetDatum(&procForm->proargtypes),
            ObjectIdGetDatum(namespaceOid))) {
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_FUNCTION),
                errmsg("function %s already exists in schema \"%s\"",
                    funcname_signature_string(newname, procForm->pronargs, NIL, proargs->values),
                    get_namespace_name(namespaceOid))));
    }
#else
    if (t_thrd.proc->workingVersionNum < 92470) {
        if (SearchSysCacheExists3(PROCNAMEARGSNSP,
                CStringGetDatum(newname),
                PointerGetDatum(&procForm->proargtypes),
                ObjectIdGetDatum(namespaceOid))) {
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_FUNCTION),
                    errmsg("function %s already exists in schema \"%s\"",
                    funcname_signature_string(newname, procForm->pronargs, NIL, proargs->values),
                    get_namespace_name(namespaceOid))));
        }
    } else {
        Datum packageOidDatum;
        bool isNull = false;
        packageOidDatum = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_packageid, &isNull);
        Datum allargtypes = ProcedureGetAllArgTypes(tup, &isNull);
        Datum argmodes = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_proargmodes, &isNull);
        if (SearchSysCacheExistsForProcAllArgs(
                CStringGetDatum(newname),
                allargtypes,
                ObjectIdGetDatum(namespaceOid),
                packageOidDatum,
                argmodes)) {
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_FUNCTION),
                    errmsg("function %s already exists in schema \"%s\"",
                    funcname_signature_string(newname, procForm->pronargs, NIL, proargs->values),
                    get_namespace_name(namespaceOid))));
        }

    } 
#endif
    /* Must be owner or have alter privilege of the target object. */
    aclresult = pg_proc_aclcheck(procOid, GetUserId(), ACL_ALTER);
    if (aclresult != ACLCHECK_OK && !pg_proc_ownercheck(procOid, GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, NameListToString(name));
    }

    /* must have CREATE privilege on namespace */
    aclresult = pg_namespace_aclcheck(namespaceOid, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(namespaceOid));

    /* rename */
    (void)namestrcpy(&(procForm->proname), newname);
    simple_heap_update(rel, &tup->t_self, tup);
    CatalogUpdateIndexes(rel, tup);

    /* Recode time of rename the proc. */
    if (OidIsValid(procOid)) {
        UpdatePgObjectMtime(procOid, OBJECT_TYPE_PROC);
    }

    heap_close(rel, NoLock);
    tableam_tops_free_tuple(tup);
}

/*
 * Change function owner by name and args
 */
void AlterFunctionOwner(List* name, List* argtypes, Oid newOwnerId)
{
    Relation rel;
    Oid procOid;
    HeapTuple tup;
    rel = heap_open(ProcedureRelationId, RowExclusiveLock);

    procOid = LookupFuncNameTypeNames(name, argtypes, false);

    /* if the function is a builtin function, its Oid is less than 10000.
     * we can't allow change the ownerId of the builtin functions
     */
    if (IsSystemObjOid(procOid) && u_sess->attr.attr_common.IsInplaceUpgrade == false) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("function \"%s\" is a builtin function,its owner can not be changed", NameListToString(name))));
    }
    /* if the function is a masking function, we can't allow to change it's ownerId. */
    if (IsMaskingFunctionOid(procOid) && !u_sess->attr.attr_common.IsInplaceUpgrade) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("function \"%s\" is a masking function,its owner can not be changed", NameListToString(name))));
    }

    TrForbidAccessRbObject(ProcedureRelationId, procOid);

    tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(procOid));

    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", procOid)));
    checkAllowAlter(tup);

    if (((Form_pg_proc)GETSTRUCT(tup))->proisagg)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%s\" is an aggregate function", NameListToString(name)),
                errhint("Use ALTER AGGREGATE to change owner of aggregate functions.")));

    AlterFunctionOwner_internal(rel, tup, newOwnerId);

    /* Recode time of change the funciton owner. */
    UpdatePgObjectMtime(procOid, OBJECT_TYPE_PROC);

    heap_close(rel, NoLock);
}

/*
 * Change function owner by Oid
 * in byPackage: means whether called by Alter Package Owner
 */
void AlterFunctionOwner_oid(Oid procOid, Oid newOwnerId, bool byPackage)
{
    Relation rel;
    HeapTuple tup;
    /* if the function is a builtin function, its Oid is less than 10000.
     * we can't allow change the ownerId of the builtin functions
     */
    if (IsSystemObjOid(procOid) && u_sess->attr.attr_common.IsInplaceUpgrade == false) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("ownerId change failed for function %u, because it is a builtin function.", procOid)));
    }
    /* if the function is a masking function, we can't allow to change it's ownerId. */
    if (IsMaskingFunctionOid(procOid) && !u_sess->attr.attr_common.IsInplaceUpgrade) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("ownerId change failed for function \"%s\", because it is a masking function.",
                    get_func_name(procOid))));
    }

    rel = heap_open(ProcedureRelationId, RowExclusiveLock);

    tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(procOid));

    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", procOid)));
    if (!byPackage) {
        checkAllowAlter(tup);
    }
    AlterFunctionOwner_internal(rel, tup, newOwnerId);

    /* Recode time of change the funciton owner. */
    UpdatePgObjectMtime(procOid, OBJECT_TYPE_PROC);

    heap_close(rel, NoLock);
}

/*
 * Change function owner by package Oid, called by Alter Package Owner
 */
void AlterFunctionOwnerByPkg(Oid packageOid, Oid newOwnerId)
{
    if (!OidIsValid(packageOid)) {
        return;
    }

    HeapTuple oldtup;
    ScanKeyData entry;
    ScanKeyInit(&entry, Anum_pg_proc_packageid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(packageOid));
    Relation pg_proc_rel = heap_open(ProcedureRelationId, RowExclusiveLock);
    SysScanDesc scan = systable_beginscan(pg_proc_rel, InvalidOid, false, NULL, 1, &entry);
    while ((oldtup = systable_getnext(scan)) != NULL) {
        HeapTuple proctup = heap_copytuple(oldtup);
        Oid funcOid = InvalidOid;
        if (HeapTupleIsValid(proctup)) {
            funcOid = HeapTupleGetOid(proctup);
            if (!OidIsValid(funcOid)) {
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmodule(MOD_PLSQL),
                        errmsg("cache lookup failed for function id %u", funcOid)));
            }
        } else {
            ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmodule(MOD_PLSQL),
                        errmsg("cache lookup failed for function %u", funcOid)));
        }
        AlterFunctionOwner_oid(funcOid, newOwnerId, true);
    }
    systable_endscan(scan);
    heap_close(pg_proc_rel, NoLock);
}

/*
 * @Description: Alter function owner.
 * @in rel: pg_proc relation.
 * @in tup: pg_proc Tuple of this funtion.
 * @in newOwnerId: New owner oid.
 */
static void AlterFunctionOwner_internal(Relation rel, HeapTuple tup, Oid newOwnerId)
{
    Form_pg_proc procForm;
    AclResult aclresult;
    Oid procOid;

    Assert(RelationGetRelid(rel) == ProcedureRelationId);
    Assert(tup->t_tableOid == ProcedureRelationId);

    procForm = (Form_pg_proc)GETSTRUCT(tup);
    procOid = HeapTupleGetOid(tup);

    if (IsSystemObjOid(procOid) && u_sess->attr.attr_common.IsInplaceUpgrade == false) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("ownerId change failed for function %u, because it is a builtin function.", procOid)));
    }

    /*
     * If the new owner is the same as the existing owner, consider the
     * command to have succeeded.  This is for dump restoration purposes.
     */
    if (procForm->proowner != newOwnerId) {
        Datum repl_val[Natts_pg_proc];
        bool repl_null[Natts_pg_proc];
        bool repl_repl[Natts_pg_proc];
        Acl* newAcl = NULL;
        Datum aclDatum;
        bool isNull = false;
        HeapTuple newtuple;

        /* Superusers can always do it */
        if (!superuser()) {
            /* Otherwise, must be owner of the existing object */
            if (!pg_proc_ownercheck(procOid, GetUserId()))
                aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameStr(procForm->proname));

            /* Must be able to become new owner */
            check_is_member_of_role(GetUserId(), newOwnerId);

            /* New owner must have CREATE privilege on namespace */
            aclresult = pg_namespace_aclcheck(procForm->pronamespace, newOwnerId, ACL_CREATE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(procForm->pronamespace));
        }

        errno_t errorno = EOK;
        errorno = memset_s(repl_null, sizeof(repl_null), false, sizeof(repl_null));
        securec_check(errorno, "\0", "\0");
        errorno = memset_s(repl_repl, sizeof(repl_repl), false, sizeof(repl_repl));
        securec_check(errorno, "\0", "\0");

        repl_repl[Anum_pg_proc_proowner - 1] = true;
        repl_val[Anum_pg_proc_proowner - 1] = ObjectIdGetDatum(newOwnerId);

        /*
         * Determine the modified ACL for the new owner.  This is only
         * necessary when the ACL is non-null.
         */
        aclDatum = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_proacl, &isNull);
        if (!isNull) {
            newAcl = aclnewowner(DatumGetAclP(aclDatum), procForm->proowner, newOwnerId);
            repl_repl[Anum_pg_proc_proacl - 1] = true;
            repl_val[Anum_pg_proc_proacl - 1] = PointerGetDatum(newAcl);
        }

        newtuple = (HeapTuple) tableam_tops_modify_tuple(tup, RelationGetDescr(rel), repl_val, repl_null, repl_repl);

        simple_heap_update(rel, &newtuple->t_self, newtuple);
        CatalogUpdateIndexes(rel, newtuple);

        tableam_tops_free_tuple(newtuple);

        /* Update owner dependency reference */
        changeDependencyOnOwner(ProcedureRelationId, procOid, newOwnerId);
        /* Update owner of function type build in pg_type */
        AlterTypeOwnerByFunc(procOid, newOwnerId);
    }

    ReleaseSysCache(tup);
}

bool IsFunctionTemp(AlterFunctionStmt* stmt)
{
    HeapTuple tup;
    Oid funcOid;
    Form_pg_proc procForm;
    Relation rel;

    rel = heap_open(ProcedureRelationId, RowExclusiveLock);
    funcOid = LookupFuncNameTypeNames(stmt->func->funcname, stmt->func->funcargs, false);
    tup = SearchSysCacheCopy1(PROCOID, ObjectIdGetDatum(funcOid));
    if (!HeapTupleIsValid(tup)) {
        /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcOid)));
    }

    procForm = (Form_pg_proc)GETSTRUCT(tup);

    if (procForm->pronamespace == u_sess->catalog_cxt.myTempNamespace) {
        heap_close(rel, RowExclusiveLock);
        tableam_tops_free_tuple(tup);
        return true;
    }

    heap_close(rel, RowExclusiveLock);
    tableam_tops_free_tuple(tup);
    return false;
}

/*
 * Implements the ALTER FUNCTION utility command (except for the
 * RENAME and OWNER clauses, which are handled as part of the generic
 * ALTER framework).
 */
void AlterFunction(AlterFunctionStmt* stmt)
{
    HeapTuple tup;
    Oid funcOid;
    Form_pg_proc procForm;
    Relation rel;
    ListCell* l = NULL;
    DefElem* volatility_item = NULL;
    DefElem* strict_item = NULL;
    DefElem* security_def_item = NULL;
    DefElem* leakproof_item = NULL;
    List* set_items = NIL;
    DefElem* cost_item = NULL;
    DefElem* rows_item = NULL;
    DefElem* fencedItem = NULL;
    DefElem* shippable_item = NULL;
    DefElem* package_item = NULL;
    bool isNull = false;
    rel = heap_open(ProcedureRelationId, RowExclusiveLock);

    funcOid = LookupFuncNameTypeNames(stmt->func->funcname, stmt->func->funcargs, false);
    /* if the function is a builtin function, its Oid is less than 10000.
     * we can't allow alter the builtin functions
     */
    if (IsSystemObjOid(funcOid) && u_sess->attr.attr_common.IsInplaceUpgrade == false) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("function \"%s\" is a builtin function,it can not be altered",
                    NameListToString(stmt->func->funcname))));
    }
    /* if the function is a masking function, we can't allow to alter it. */
    if (IsMaskingFunctionOid(funcOid) && !u_sess->attr.attr_common.IsInplaceUpgrade) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("function \"%s\" is a masking function, it can not be altered",
                    NameListToString(stmt->func->funcname))));
    }

    tup = SearchSysCacheCopy1(PROCOID, ObjectIdGetDatum(funcOid));
    Datum packageOidDatum = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_packageid, &isNull);
    Oid packageoid = DatumGetObjectId(packageOidDatum);
    if (OidIsValid(packageoid)) {
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("\"%s\" is a function in package.", NameListToString(stmt->func->funcname)),
                    errhint("Use ALTER PACKAGE to alter functions.")));               
    }
    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcOid)));

    procForm = (Form_pg_proc)GETSTRUCT(tup);

    TrForbidAccessRbObject(ProcedureRelationId, funcOid, NameStr(procForm->proname));

    /* Must be owner or have alter privilege of the target object. */
    AclResult aclresult = pg_proc_aclcheck(funcOid, GetUserId(), ACL_ALTER);
    if (aclresult != ACLCHECK_OK && !pg_proc_ownercheck(funcOid, GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_PROC, NameListToString(stmt->func->funcname));
    }

    if (procForm->proisagg)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("\"%s\" is an aggregate function", NameListToString(stmt->func->funcname))));

    if (procForm->pronamespace == u_sess->catalog_cxt.myTempNamespace)
        ExecSetTempObjectIncluded();

    /* Examine requested actions. */
    foreach (l, stmt->actions) {
        DefElem* defel = (DefElem*)lfirst(l);

        if (compute_common_attribute(defel,
                &volatility_item,
                &strict_item,
                &security_def_item,
                &leakproof_item,
                &set_items,
                &cost_item,
                &rows_item,
                &fencedItem,
                &shippable_item,
                &package_item) == false)
            ereport(ERROR,
                (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmsg("option \"%s\" not recognized", defel->defname)));
    }

    if (volatility_item != NULL)
        procForm->provolatile = interpret_func_volatility(volatility_item);
    if (strict_item != NULL)
        procForm->proisstrict = intVal(strict_item->arg);
    if (security_def_item != NULL)
        procForm->prosecdef = intVal(security_def_item->arg);
    if (leakproof_item != NULL) {
        procForm->proleakproof = intVal(leakproof_item->arg);
        if (procForm->proleakproof && !superuser())
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("only system admin can define a leakproof function")));
    }
    if (cost_item != NULL) {
        procForm->procost = defGetNumeric(cost_item);
        if (procForm->procost <= 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("COST must be positive")));
    }
    if (rows_item != NULL) {
        procForm->prorows = defGetNumeric(rows_item);
        if (procForm->prorows <= 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("ROWS must be positive")));
        if (!procForm->proretset)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("ROWS is not applicable when function does not return a set")));
    }

    if (package_item != NULL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Do not support package for ALTER FUNCTION.")));
    }

    if (set_items != NULL || fencedItem != NULL || shippable_item != NULL) {
        Datum datum;
        bool isnull = false;
        Datum repl_val[Natts_pg_proc];
        bool repl_null[Natts_pg_proc];
        bool repl_repl[Natts_pg_proc];
        errno_t rc = EOK;

        rc = memset_s(repl_repl, sizeof(repl_repl), false, sizeof(repl_repl));
        securec_check(rc, "\0", "\0");

        if (set_items != NULL) {
            ArrayType* a = NULL;
            /* extract existing proconfig setting */
            datum = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_proconfig, &isnull);
            a = isnull ? NULL : DatumGetArrayTypeP(datum);

            /* update according to each SET or RESET item, left to right */
            a = update_proconfig_value(a, (const List*)set_items);

            /* update the tuple */
            repl_repl[Anum_pg_proc_proconfig - 1] = true;

            if (a == NULL) {
                repl_val[Anum_pg_proc_proconfig - 1] = (Datum)0;
                repl_null[Anum_pg_proc_proconfig - 1] = true;
            } else {
                repl_val[Anum_pg_proc_proconfig - 1] = PointerGetDatum(a);
                repl_null[Anum_pg_proc_proconfig - 1] = false;
            }
        }

        if (fencedItem != NULL) {
            bool setToFenced = intVal(fencedItem->arg);
            if (!setToFenced && procForm->prolang == JavalanguageId)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Java UDF dose not support NOT FENCED functions.")));

            /* update the tuple */
            repl_repl[Anum_pg_proc_fenced - 1] = true;
            repl_val[Anum_pg_proc_fenced - 1] = BoolGetDatum(intVal(fencedItem->arg));
            repl_null[Anum_pg_proc_fenced - 1] = false;
        }

        if (shippable_item != NULL) {
            repl_repl[Anum_pg_proc_shippable - 1] = true;
            repl_val[Anum_pg_proc_shippable - 1] = BoolGetDatum(intVal(shippable_item->arg));
            repl_null[Anum_pg_proc_shippable - 1] = false;
            if (!intVal(shippable_item->arg) &&
                ((volatility_item && interpret_func_volatility(volatility_item) == PROVOLATILE_IMMUTABLE) ||
                    (volatility_item == NULL && func_volatile(funcOid) == PROVOLATILE_IMMUTABLE))) {
                elog(NOTICE, "Immutable function will be shippable anyway.");
            }
        }
        tup = (HeapTuple) tableam_tops_modify_tuple(tup, RelationGetDescr(rel), repl_val, repl_null, repl_repl);
    }

    /* Do the update */
    simple_heap_update(rel, &tup->t_self, tup);
    CatalogUpdateIndexes(rel, tup);

    /* Recode time of alter funciton. */
    if (OidIsValid(funcOid)) {
        UpdatePgObjectMtime(funcOid, OBJECT_TYPE_PROC);
    }

    /*
     * Send invalid message for for relation holding replaced function as trigger if 
     * volatality or shippability changes.
     */
    if (shippable_item != NULL || volatility_item != NULL) {
        InvalidRelcacheForTriggerFunction(funcOid, procForm->prorettype);
    }

    heap_close(rel, NoLock);
    tableam_tops_free_tuple(tup);
}

/*
 * SetFunctionReturnType - change declared return type of a function
 *
 * This is presently only used for adjusting legacy functions that return
 * OPAQUE to return whatever we find their correct definition should be.
 * The caller should emit a suitable warning explaining what we did.
 */
void SetFunctionReturnType(Oid funcOid, Oid newRetType)
{
    Relation pg_proc_rel;
    HeapTuple tup;
    Form_pg_proc procForm;

    /* if the function is a builtin function, its Oid is less than 10000.
     * we can't allow setting return type for the builtin functions
     */
    if (IsSystemObjOid(funcOid) && u_sess->attr.attr_common.IsInplaceUpgrade == false) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("set return type failed for function %u, because it is a builtin function.", funcOid)));
    }

    pg_proc_rel = heap_open(ProcedureRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(PROCOID, ObjectIdGetDatum(funcOid));
    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcOid)));
    procForm = (Form_pg_proc)GETSTRUCT(tup);

    if (procForm->prorettype != OPAQUEOID) /* caller messed up */
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("function %u doesn't return OPAQUE", funcOid)));

    /* okay to overwrite copied tuple */
    procForm->prorettype = newRetType;

    /* update the catalog and its indexes */
    simple_heap_update(pg_proc_rel, &tup->t_self, tup);

    CatalogUpdateIndexes(pg_proc_rel, tup);

    heap_close(pg_proc_rel, RowExclusiveLock);
}

/*
 * SetFunctionArgType - change declared argument type of a function
 *
 * As above, but change an argument's type.
 */
void SetFunctionArgType(Oid funcOid, int argIndex, Oid newArgType)
{
    Relation pg_proc_rel;
    HeapTuple tup;
    Form_pg_proc procForm;

    /* if the function is a builtin function, its Oid is less than 10000.
     * we can't allow setting argument type for the builtin functions
     */
    if (IsSystemObjOid(funcOid) && u_sess->attr.attr_common.IsInplaceUpgrade == false) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("set argument type failed for function %u, because it is a builtin function.", funcOid)));
    }
    pg_proc_rel = heap_open(ProcedureRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(PROCOID, ObjectIdGetDatum(funcOid));
    if (!HeapTupleIsValid(tup)) /* should not happen */
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcOid)));
    procForm = (Form_pg_proc)GETSTRUCT(tup);

    // No need to check Anum_pg_proc_proargtypesext attribute, because this function is called for
    // procedures/functions that have less than or equal to FUNC_MAX_ARGS_INROW parameters
    Assert(procForm->pronargs <= FUNC_MAX_ARGS_INROW);

    if (argIndex < 0 || argIndex >= procForm->pronargs || procForm->proargtypes.values[argIndex] != OPAQUEOID)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("function %u doesn't return OPAQUE", funcOid)));

    /* okay to overwrite copied tuple */
    procForm->proargtypes.values[argIndex] = newArgType;

    /* update the catalog and its indexes */
    simple_heap_update(pg_proc_rel, &tup->t_self, tup);

    CatalogUpdateIndexes(pg_proc_rel, tup);

    heap_close(pg_proc_rel, RowExclusiveLock);
}

/*
 * CREATE CAST
 */
void CreateCast(CreateCastStmt* stmt)
{
    Oid sourcetypeid;
    Oid targettypeid;
    char sourcetyptype;
    char targettyptype;
    Oid funcid;
    Oid castid;
    Oid ownerid;
    int nargs;
    char castcontext;
    char castmethod;
    Relation relation;
    HeapTuple tuple;
    Datum values[Natts_pg_cast];
    bool nulls[Natts_pg_cast];
    ObjectAddress myself, referenced;
    AclResult aclresult;

    sourcetypeid = typenameTypeId(NULL, stmt->sourcetype);
    targettypeid = typenameTypeId(NULL, stmt->targettype);
    sourcetyptype = get_typtype(sourcetypeid);
    targettyptype = get_typtype(targettypeid);

    /* No pseudo-types allowed */
    if (sourcetyptype == TYPTYPE_PSEUDO)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("source data type %s is a pseudo-type", TypeNameToString(stmt->sourcetype))));

    if (targettyptype == TYPTYPE_PSEUDO)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("target data type %s is a pseudo-type", TypeNameToString(stmt->targettype))));

    /* Permission check */
    if (!pg_type_ownercheck(sourcetypeid, GetUserId()) && !pg_type_ownercheck(targettypeid, GetUserId()))
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be owner of type %s or type %s",
                    format_type_be(sourcetypeid),
                    format_type_be(targettypeid))));

    aclresult = pg_type_aclcheck(sourcetypeid, GetUserId(), ACL_USAGE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error_type(aclresult, sourcetypeid);

    aclresult = pg_type_aclcheck(targettypeid, GetUserId(), ACL_USAGE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error_type(aclresult, targettypeid);

    /* Domains are allowed for historical reasons, but we warn */
    if (sourcetyptype == TYPTYPE_DOMAIN)
        ereport(WARNING,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cast will be ignored because the source data type is a domain")));

    else if (targettyptype == TYPTYPE_DOMAIN)
        ereport(WARNING,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cast will be ignored because the target data type is a domain")));

    /* Detemine the cast method */
    if (stmt->func != NULL)
        castmethod = COERCION_METHOD_FUNCTION;
    else if (stmt->inout)
        castmethod = COERCION_METHOD_INOUT;
    else
        castmethod = COERCION_METHOD_BINARY;

    if (castmethod == COERCION_METHOD_FUNCTION) {
        Form_pg_proc procstruct;

        funcid = LookupFuncNameTypeNames(stmt->func->funcname, stmt->func->funcargs, false);

        tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
        if (!HeapTupleIsValid(tuple))
            ereport(
                ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", funcid)));

        procstruct = (Form_pg_proc)GETSTRUCT(tuple);
        nargs = procstruct->pronargs;

        // No need to check Anum_pg_proc_proargtypesext attribute, because cast function
        // will not have more than FUNC_MAX_ARGS_INROW parameters
        Assert(nargs <= FUNC_MAX_ARGS_INROW);

        if (nargs < 1 || nargs > 3)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("cast function must take one to three arguments")));
        if (!IsBinaryCoercible(sourcetypeid, procstruct->proargtypes.values[0]))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("argument of cast function must match or be binary-coercible from source data type")));
        if (nargs > 1 && procstruct->proargtypes.values[1] != INT4OID)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("second argument of cast function must be type integer")));
        if (nargs > 2 && procstruct->proargtypes.values[2] != BOOLOID)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("third argument of cast function must be type boolean")));
        if (!IsBinaryCoercible(procstruct->prorettype, targettypeid))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("return data type of cast function must match or be binary-coercible to target data type")));

            /*
             * Restricting the volatility of a cast function may or may not be a
             * good idea in the abstract, but it definitely breaks many old
             * user-defined types.	Disable this check --- tgl 2/1/03
             */
#ifdef NOT_USED
        if (procstruct->provolatile == PROVOLATILE_VOLATILE)
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("cast function must not be volatile")));
#endif
        if (procstruct->proisagg)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("cast function must not be an aggregate function")));
        if (procstruct->proiswindow)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("cast function must not be a window function")));
        if (procstruct->proretset)
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("cast function must not return a set")));

        ReleaseSysCache(tuple);
    } else {
        funcid = InvalidOid;
        nargs = 0;
    }

    if (castmethod == COERCION_METHOD_BINARY) {
        int16 typ1len;
        int16 typ2len;
        bool typ1byval = false;
        bool typ2byval = false;
        char typ1align;
        char typ2align;

        /*
         * Must be superuser to create binary-compatible casts, since
         * erroneous casts can easily crash the backend.
         */
        if (!superuser())
            ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    errmsg("must be system admin to create a cast WITHOUT FUNCTION")));

        /*
         * Also, insist that the types match as to size, alignment, and
         * pass-by-value attributes; this provides at least a crude check that
         * they have similar representations.  A pair of types that fail this
         * test should certainly not be equated.
         */
        get_typlenbyvalalign(sourcetypeid, &typ1len, &typ1byval, &typ1align);
        get_typlenbyvalalign(targettypeid, &typ2len, &typ2byval, &typ2align);
        if (typ1len != typ2len || typ1byval != typ2byval || typ1align != typ2align)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("source and target data types are not physically compatible")));

        /*
         * We know that composite, enum and array types are never binary-
         * compatible with each other.	They all have OIDs embedded in them.
         *
         * Theoretically you could build a user-defined base type that is
         * binary-compatible with a composite, enum, or array type.  But we
         * disallow that too, as in practice such a cast is surely a mistake.
         * You can always work around that by writing a cast function.
         */
        if (sourcetyptype == TYPTYPE_COMPOSITE || targettyptype == TYPTYPE_COMPOSITE)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("composite data types are not binary-compatible")));

        if (sourcetyptype == TYPTYPE_ENUM || targettyptype == TYPTYPE_ENUM)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("enum data types are not binary-compatible")));

        if (OidIsValid(get_element_type(sourcetypeid)) || OidIsValid(get_element_type(targettypeid)))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("array data types are not binary-compatible")));

        /*
         * We also disallow creating binary-compatibility casts involving
         * domains.  Casting from a domain to its base type is already
         * allowed, and casting the other way ought to go through domain
         * coercion to permit constraint checking.	Again, if you're intent on
         * having your own semantics for that, create a no-op cast function.
         *
         * NOTE: if we were to relax this, the above checks for composites
         * etc. would have to be modified to look through domains to their
         * base types.
         */
        if (sourcetyptype == TYPTYPE_DOMAIN || targettyptype == TYPTYPE_DOMAIN)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("domain data types must not be marked binary-compatible")));
    }

    /*
     * Allow source and target types to be same only for length coercion
     * functions.  We assume a multi-arg function does length coercion.
     */
    if (sourcetypeid == targettypeid && nargs < 2)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("source data type and target data type are the same")));

    /* convert CoercionContext enum to char value for castcontext */
    switch (stmt->context) {
        case COERCION_IMPLICIT:
            castcontext = COERCION_CODE_IMPLICIT;
            break;
        case COERCION_ASSIGNMENT:
            castcontext = COERCION_CODE_ASSIGNMENT;
            break;
        case COERCION_EXPLICIT:
            castcontext = COERCION_CODE_EXPLICIT;
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized CoercionContext: %d", stmt->context)));
            castcontext = 0; /* keep compiler quiet */
            break;
    }

    relation = heap_open(CastRelationId, RowExclusiveLock);

    /*
     * Check for duplicate.  This is just to give a friendly error message,
     * the unique index would catch it anyway (so no need to sweat about race
     * conditions).
     */
    tuple = SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(sourcetypeid), ObjectIdGetDatum(targettypeid));
    if (HeapTupleIsValid(tuple))
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT),
                errmsg("cast from type %s to type %s already exists",
                    format_type_be(sourcetypeid),
                    format_type_be(targettypeid))));

    errno_t ss_rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(ss_rc, "", "");

    /* ready to go */
    values[Anum_pg_cast_castsource - 1] = ObjectIdGetDatum(sourcetypeid);
    values[Anum_pg_cast_casttarget - 1] = ObjectIdGetDatum(targettypeid);
    values[Anum_pg_cast_castfunc - 1] = ObjectIdGetDatum(funcid);
    values[Anum_pg_cast_castcontext - 1] = CharGetDatum(castcontext);
    values[Anum_pg_cast_castmethod - 1] = CharGetDatum(castmethod);
    ownerid = GetUserId();
    if (OidIsValid(ownerid)) {
        values[Anum_pg_cast_castowner - 1] = ObjectIdGetDatum(ownerid);
    } else {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("invalid current user oid for cast")));
    }

    ss_rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(ss_rc, "", "");

    tuple = heap_form_tuple(RelationGetDescr(relation), values, nulls);

    castid = simple_heap_insert(relation, tuple);

    CatalogUpdateIndexes(relation, tuple);

    /* make dependency entries */
    myself.classId = CastRelationId;
    myself.objectId = castid;
    myself.objectSubId = 0;

    /* dependency on source type */
    referenced.classId = TypeRelationId;
    referenced.objectId = sourcetypeid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    /* dependency on target type */
    referenced.classId = TypeRelationId;
    referenced.objectId = targettypeid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    /* dependency on function */
    if (OidIsValid(funcid)) {
        referenced.classId = ProcedureRelationId;
        referenced.objectId = funcid;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    /* Post creation hook for new cast */
    InvokeObjectAccessHook(OAT_POST_CREATE, CastRelationId, castid, 0, NULL);

    tableam_tops_free_tuple(tuple);

    heap_close(relation, RowExclusiveLock);
}

/*
 * get_cast_oid - given two type OIDs, look up a cast OID
 *
 * If missing_ok is false, throw an error if the cast is not found.  If
 * true, just return InvalidOid.
 */
Oid get_cast_oid(Oid sourcetypeid, Oid targettypeid, bool missing_ok)
{
    Oid oid;

    oid = GetSysCacheOid2(CASTSOURCETARGET, ObjectIdGetDatum(sourcetypeid), ObjectIdGetDatum(targettypeid));
    if (!OidIsValid(oid) && !missing_ok)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("cast from type %s to type %s does not exist",
                    format_type_be(sourcetypeid),
                    format_type_be(targettypeid))));
    return oid;
}

void DropCastById(Oid castOid)
{
    Relation relation;
    ScanKeyData scankey;
    SysScanDesc scan;
    HeapTuple tuple;

    relation = heap_open(CastRelationId, RowExclusiveLock);

    ScanKeyInit(&scankey, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(castOid));
    scan = systable_beginscan(relation, CastOidIndexId, true, NULL, 1, &scankey);

    tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("could not find tuple for cast %u", castOid)));
    simple_heap_delete(relation, &tuple->t_self);

    systable_endscan(scan);
    heap_close(relation, RowExclusiveLock);
}

/*
 * Execute ALTER FUNCTION/AGGREGATE SET SCHEMA
 *
 * These commands are identical except for the lookup procedure, so share code.
 */
void AlterFunctionNamespace(List* name, List* argtypes, bool isagg, const char* newschema)
{
    Oid procOid;
    Oid nspOid;

    /* get function OID */
    if (isagg)
        procOid = LookupAggNameTypeNames(name, argtypes, false);
    else
        procOid = LookupFuncNameTypeNames(name, argtypes, false);

    /* get schema OID and check its permissions */
    nspOid = LookupCreationNamespace(newschema);

    TrForbidAccessRbObject(ProcedureRelationId, nspOid);

    (void)AlterFunctionNamespace_oid(procOid, nspOid);
}

Oid AlterFunctionNamespace_oid(Oid procOid, Oid nspOid)
{
    Oid oldNspOid;
    HeapTuple tup;
    Relation procRel;
    Form_pg_proc proc;

    /* if the function is a builtin function, its Oid is less than 10000.
     * we can't allow change the namespace of the builtin functions
     */
    if (IsSystemObjOid(procOid) && u_sess->attr.attr_common.IsInplaceUpgrade == false) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("namespace change failed for function %u, because it is a builtin function.", procOid)));
    }
    /* if the function is a masking function, we can't allow to alter it's namespace oid. */
    if (IsMaskingFunctionOid(procOid) && !u_sess->attr.attr_common.IsInplaceUpgrade) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                errmsg("namespace change failed for function \"%s\", because it is a masking function.",
                    get_func_name(procOid))));
    }

    procRel = heap_open(ProcedureRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(PROCOID, ObjectIdGetDatum(procOid));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for function %u", procOid)));
    checkAllowAlter(tup);
    proc = (Form_pg_proc)GETSTRUCT(tup);

    /* check permissions on function */
    if (!pg_proc_ownercheck(procOid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameStr(proc->proname));

    oldNspOid = proc->pronamespace;

    /* common checks on switching namespaces */
    CheckSetNamespace(oldNspOid, nspOid, ProcedureRelationId, procOid);

    /* disallow move objects into public schemas for non-admin user */
    if (nspOid == PG_PUBLIC_NAMESPACE && !isRelSuperuser()) {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), 
                 errmsg("permission denied to move objects into public schema"),
                 errhint("must be %s to move objects into public schema.",
                         g_instance.attr.attr_security.enablePrivilegesSeparate ? "initial user" : "sysadmin")));
    }

    /* check for duplicate name (more friendly than unique-index failure) */
#ifdef ENABLE_MULTIPLE_NODES
    if (SearchSysCacheExists3(PROCNAMEARGSNSP,
            CStringGetDatum(NameStr(proc->proname)),
            PointerGetDatum(&proc->proargtypes),
            ObjectIdGetDatum((nspOid)))) {
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_FUNCTION),
                errmsg("function \"%s\" already exists in schema \"%s\"",
                    NameStr(proc->proname),
                    get_namespace_name(nspOid))));
    }    
#else
    if (t_thrd.proc->workingVersionNum < 92470) { 
        if (SearchSysCacheExists3(PROCNAMEARGSNSP,
                CStringGetDatum(NameStr(proc->proname)),
                PointerGetDatum(&proc->proargtypes),
                ObjectIdGetDatum((nspOid)))) {
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_FUNCTION),
                    errmsg("function \"%s\" already exists in schema \"%s\"",
                        NameStr(proc->proname),
                        get_namespace_name(nspOid))));
        }    
    } else {
        Datum packageOidDatum;
        Oid packageOid = InvalidOid;
        bool isNull = false;
        packageOidDatum = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_packageid, &isNull);
        packageOid = ObjectIdGetDatum(packageOidDatum);
        if (!OidIsValid(packageOid)) {
            packageOidDatum = ObjectIdGetDatum(InvalidOid);
        }    

        Datum allargtypes = ProcedureGetAllArgTypes(tup, &isNull);
        Datum argmodes = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_proargmodes, &isNull);
        if (SearchSysCacheExistsForProcAllArgs(
                CStringGetDatum(NameStr(proc->proname)),
                allargtypes,
                ObjectIdGetDatum(nspOid),
                packageOidDatum,
                argmodes)) {
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_FUNCTION),
                    errmsg("function \"%s\" already exists in schema \"%s\"",
                    NameStr(proc->proname),
                    get_namespace_name(nspOid))));
        }
    }    
#endif
    /* OK, modify the pg_proc row */
    /* tup is a copy, so we can scribble directly on it */
    proc->pronamespace = nspOid;

    simple_heap_update(procRel, &tup->t_self, tup);
    CatalogUpdateIndexes(procRel, tup);

    /* Recode time of alter function namespace. */
    UpdatePgObjectMtime(procOid, OBJECT_TYPE_PROC);

    /* Update dependency on schema */
    if (changeDependencyFor(ProcedureRelationId, procOid, NamespaceRelationId, oldNspOid, nspOid) != 1)
        ereport(ERROR,
            (errcode(ERRCODE_CHECK_VIOLATION),
                errmsg("failed to change schema dependency for function \"%s\"", NameStr(proc->proname))));

    tableam_tops_free_tuple(tup);

    heap_close(procRel, RowExclusiveLock);

    return oldNspOid;
}

/*
 * ExecuteDoStmt
 *		Execute inline procedural-language code
 */
void ExecuteDoStmt(DoStmt* stmt, bool atomic)
{
    InlineCodeBlock* codeblock = makeNode(InlineCodeBlock);
    ListCell* arg = NULL;
    DefElem* as_item = NULL;
    DefElem* language_item = NULL;
    char* language = NULL;
    Oid laninline;
    HeapTuple languageTuple;
    Form_pg_language languageStruct;

    /* Process options we got from gram.y */
    foreach (arg, stmt->args) {
        DefElem* defel = (DefElem*)lfirst(arg);

        if (strcmp(defel->defname, "as") == 0) {
            if (as_item != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            as_item = defel;
        } else if (strcmp(defel->defname, "language") == 0) {
            if (language_item != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            language_item = defel;
        } else
            ereport(ERROR,
                (errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION), errmsg("option \"%s\" not recognized", defel->defname)));
    }

    if (as_item != NULL)
        codeblock->source_text = strVal(as_item->arg);
    else
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("no inline code specified")));

    /* if LANGUAGE option wasn't specified, use the default */
    if (language_item != NULL)
        language = strVal(language_item->arg);
    else
        language = "plpgsql";

    /* Look up the language and validate permissions */
    languageTuple = SearchSysCache1(LANGNAME, PointerGetDatum(language));
    if (!HeapTupleIsValid(languageTuple))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("language \"%s\" does not exist", language),
                (PLTemplateExists(language) ? errhint("Use CREATE LANGUAGE to load the language into the database.")
                                            : 0)));

    codeblock->langOid = HeapTupleGetOid(languageTuple);
    languageStruct = (Form_pg_language)GETSTRUCT(languageTuple);
    codeblock->langIsTrusted = languageStruct->lanpltrusted;

    if (languageStruct->lanpltrusted) {
        /* if trusted language, need USAGE privilege */
        AclResult aclresult;

        aclresult = pg_language_aclcheck(codeblock->langOid, GetUserId(), ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_LANGUAGE, NameStr(languageStruct->lanname));
    } else {
        /* if untrusted language, must be superuser */
        if (!superuser())
            aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_LANGUAGE, NameStr(languageStruct->lanname));
    }

    /* get the handler function's OID */
    laninline = languageStruct->laninline;
    codeblock->atomic = atomic;
    if (!OidIsValid(laninline))
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("language \"%s\" does not support inline code execution", NameStr(languageStruct->lanname))));

    ReleaseSysCache(languageTuple);

    /* execute the inline handler */
    OidFunctionCall1(laninline, PointerGetDatum(codeblock));
}

// get a int2vector from a list for argument with default value
static int2vector* GetDefaultArgPos(List* defargpos)
{
    int2vector* argpos = NULL;
    int2* poslist = NULL;
    int length = 0;
    ListCell* cell = NULL;
    int count = 0;

    if (defargpos == NIL)
        return NULL;

    length = defargpos->length;

    poslist = (int2*)palloc(length * sizeof(int2));

    foreach (cell, defargpos) {
        poslist[count++] = lfirst_int(cell);
    }

    argpos = buildint2vector(poslist, length);

    list_free_ext(defargpos);
    pfree_ext(poslist);

    return argpos;
}

/*
 * @Description: Get pending library filename to string.
 * @in forCommit: True or false, mean commit or rollback.
 * @out str_ptr: Filename string.
 * @return: File number.
 */
int libraryGetPendingDeletes(bool forCommit, char** str_ptr, int* libraryLen)
{
    int nlibrary = 0;
    int over_length = 0;
    int int_size = sizeof(int);
    char* library_path = NULL;
    int offset = 0;

    ListCell* lc = NULL;
    foreach (lc, u_sess->cmd_cxt.PendingLibraryDeletes) {
        PendingLibraryDelete* del_file = (PendingLibraryDelete*)lfirst(lc);

        /* delete files according to status of transaction */
        if (del_file->atCommit == forCommit) {
            over_length += int_size + strlen(del_file->filename);
            nlibrary++;
        }
    }

    if (nlibrary == 0) {
        *str_ptr = NULL;
        return 0;
    }

    errno_t rc = 0;
    char* str = (char*)palloc(over_length);

    foreach (lc, u_sess->cmd_cxt.PendingLibraryDeletes) {
        PendingLibraryDelete* del_file = (PendingLibraryDelete*)lfirst(lc);

        if (del_file->atCommit == forCommit) {
            library_path = del_file->filename;

            int string_len = strlen(library_path);

            /* write filename length. */
            rc = memcpy_s(str + offset, int_size, (char*)(&string_len), int_size);
            securec_check_c(rc, "\0", "\0");
            offset += int_size;

            /* write file path. */
            rc = memcpy_s(str + offset, string_len, library_path, string_len);
            securec_check_c(rc, "\0", "\0");
            offset += string_len;
        }
    }

    Assert(over_length == offset);

    *libraryLen = over_length;
    *str_ptr = str;
    return nlibrary;
}

/*
 * GetFunctionNodeGroup
 *	 Get dependent NodeGroup oid of the function if arguments or return value including table type.
 *     If neither arguments nor return value include table type, return InvalidOid;
 *     If arguments and return value include different NodeGroup oid, return InvalidOid also;
 *     If arguments and return value include same NodeGroup oid,return the oid.
 */
Oid GetFunctionNodeGroup(CreateFunctionStmt* stmt, bool* multi_group)
{
    ListCell* param = NULL;
    Oid goid;
    Type typtup;
    Form_pg_type typeForm;
    FunctionParameter* fp = NULL;
    int param_count;
    int count;
    int i;

    Oid groupoid = InvalidOid;
    if (IS_PGXC_DATANODE) {
        return InvalidOid;
    }

    param_count = (stmt->parameters == NULL) ? 0 : list_length(stmt->parameters);
    count = (stmt->returnType == NULL) ? param_count : (param_count + 1);

    if (count == 0)
        return InvalidOid;

    if (param_count > 0)
        param = list_head(stmt->parameters);

    for (i = 0; i < count; i++) {
        if (i == 0 && stmt->returnType != NULL) {
            typtup = LookupTypeName(NULL, stmt->returnType, NULL, false);
        } else if (param != NULL) {
            fp = (FunctionParameter*)lfirst(param);
            typtup = LookupTypeName(NULL, fp->argType, NULL, false);
            param = lnext(param);
        } else
            break;

        if (typtup == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("type \"%s\" does not exist",
                        (stmt->returnType == NULL) ? TypeNameToString(fp->argType)
                                                   : TypeNameToString(stmt->returnType))));
        }

        typeForm = (Form_pg_type)GETSTRUCT(typtup);

        if (OidIsValid(typeForm->typrelid)) {
            char relkind = get_rel_relkind(typeForm->typrelid);
            bool flag = RELKIND_VIEW != relkind && RELKIND_CONTQUERY != relkind;
            if (flag) { 
                goid = ng_get_baserel_groupoid(typeForm->typrelid, relkind);
                if (OidIsValid(goid)) {
                    if (groupoid == InvalidOid)
                        groupoid = goid;
                    else if (groupoid != InvalidOid && groupoid != goid) {
                        if (multi_group != NULL)
                            *multi_group = true;

                        ReleaseSysCache(typtup);
                        return InvalidOid;
                    }
                }
            }
        }
        ReleaseSysCache(typtup);
    }
    return groupoid;
}

/*
 * GetFunctionNodeGroupByFuncid
 *	 Get dependent NodeGroup oid of the function if arguments or return value including table type.
 *     If neither arguments nor return value include table type, return InvalidOid;
 *     If arguments and return value include different NodeGroup oid, return InvalidOid also;
 *     If arguments and return value include same NodeGroup oid,return the oid.
 *     The function is used for those functions already created in database, but GetFunctionNodeGroup
 *     is used for new function not created in database.
 */
Oid GetFunctionNodeGroupByFuncid(Oid funcid)
{
    Oid* argstype = NULL;
    int nargs;
    Oid rettype = InvalidOid;
    Oid typOid;
    Oid goid;
    Oid groupoid = InvalidOid;
    Relation rel;
    Type typtup;
    Form_pg_type typeForm;
    int i;

    rel = heap_open(ProcedureRelationId, AccessShareLock);

    /* Fetch function signatures */
    rettype = get_func_signature(funcid, &argstype, &nargs);

    for (i = 0; i < nargs + 1; i++) {
        typOid = (i == 0) ? rettype : argstype[i - 1];

        typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typOid));
        if (!HeapTupleIsValid(typtup))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type with OID %u does not exist", typOid)));

        typeForm = (Form_pg_type)GETSTRUCT(typtup);

        if (OidIsValid(typeForm->typrelid)) {
            char relkind = get_rel_relkind(typeForm->typrelid);

            if (RELKIND_VIEW != relkind && RELKIND_CONTQUERY != relkind) {
                goid = ng_get_baserel_groupoid(typeForm->typrelid, relkind);
                if (OidIsValid(goid)) {
                    /* Only need to find the first relation type. */
                    groupoid = goid;
                    ReleaseSysCache(typtup);
                    break;
                }
            }
        }
        ReleaseSysCache(typtup);
    }

    pfree_ext(argstype);
    heap_close(rel, AccessShareLock);

    return groupoid;
}

/*
 * GetFunctionNodeGroup
 *	 Get dependent NodeGroup oid of the function if arguments or return value including table type.
 *     See GetFunctionNodeGroupByFuncid for details.
 */
Oid GetFunctionNodeGroup(AlterFunctionStmt* stmt)
{
    Oid funcid;

    funcid = LookupFuncNameTypeNames(stmt->func->funcname, stmt->func->funcargs, false);

    return GetFunctionNodeGroupByFuncid(funcid);
}


static void checkAllowAlter(HeapTuple tup) {
    Datum packageOidDatum;
    Oid packageOid = InvalidOid;
    bool isnull = false;
    packageOidDatum = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_packageid, &isnull);
    if (!isnull) {
        packageOid = DatumGetObjectId(packageOidDatum);
    }
    if (OidIsValid(packageOid)) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL), errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("alter function in package is not allowed"),
                errdetail("please rebuild package"),
                errcause("package is one object,not allow alter function in package"),
                erraction("rebuild package")));
    }
}
