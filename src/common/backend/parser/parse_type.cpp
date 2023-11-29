/* -------------------------------------------------------------------------
 *
 * parse_type.cpp
 *		handle type operations for parser
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/parser/parse_type.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "gaussdb_version.h"

#include "access/transam.h"
#include "catalog/namespace.h"
#include "catalog/gs_package.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "optimizer/nodegroups.h"
#include "parser/parser.h"
#include "parser/parse_type.h"
#include "pgxc/groupmgr.h"
#include "pgxc/pgxc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datum.h"
#include "utils/fmgrtab.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/syscache.h"
#include "utils/pl_package.h"
#include "catalog/gs_collation.h"
#include "parser/parse_utilcmd.h"
#include "catalog/pg_object.h"
#include "catalog/gs_dependencies_fn.h"
#include "catalog/pg_type_fn.h"

static int32 typenameTypeMod(ParseState* pstate, const TypeName* typname, Type typ);

static bool IsTypeInBlacklist(Oid typoid);

Oid LookupPctTypeInPackage(RangeVar* rel, Oid pkgOid, const char* field)
{
    char* castTypeName = CastPackageTypeName(rel->relname, pkgOid, true);
    char* oldTypName = rel->relname;
    rel->relname = castTypeName;
    Oid relid = RangeVarGetRelidExtended(rel, NoLock, true, false, false, true, NULL, NULL, NULL, NULL);
    if (!OidIsValid(relid)) {
        rel->relname = oldTypName;
        pfree_ext(castTypeName);
        return InvalidOid;
    }
    AttrNumber attnum = get_attnum(relid, field);
    if (attnum == InvalidAttrNumber) {
        rel->relname = oldTypName;
        pfree_ext(castTypeName);
        return InvalidOid;
    }
    Oid typoid = get_atttype(relid, attnum);
    rel->relname = oldTypName;
    pfree_ext(castTypeName);
    if (OidIsValid(typoid)) {
        return relid;
    } else {
        return InvalidOid;
    }
}

Type LookupTypeNameSupportUndef(ParseState *pstate, const TypeName *typeName, int32 *typmod_p, bool print_notice)
{
    Type typtup = NULL;
    CreatePlsqlType oldCreatePlsqlType = u_sess->plsql_cxt.createPlsqlType;
    PG_TRY();
    {
        set_create_plsql_type_not_check_nsp_oid();
        TypeDependExtend* dependExt = NULL;
        InstanceTypeNameDependExtend(&dependExt);
        typtup = LookupTypeName(pstate, typeName, typmod_p, print_notice, dependExt);
        pfree_ext(dependExt);
    }
    PG_CATCH();
    {
        set_create_plsql_type(oldCreatePlsqlType);
        PG_RE_THROW();
    }
    PG_END_TRY();
    set_create_plsql_type(oldCreatePlsqlType);
    return typtup;
}

/*
 * LookupTypeName
 *       Wrapper for typical case.
 */
Type LookupTypeName(ParseState *pstate, const TypeName *typeName, int32 *typmod_p, bool print_notice, TypeDependExtend* dependExtend)
{
       return LookupTypeNameExtended(pstate, typeName, typmod_p, true, print_notice, dependExtend);
}

/*
 * LookupTypeNameExtended
 *		Given a TypeName object, lookup the pg_type syscache entry of the type.
 *		Returns NULL if no such type can be found.	If the type is found,
 *		the typmod value represented in the TypeName struct is computed and
 *		stored into *typmod_p.
 *
 * NB: on success, the caller must ReleaseSysCache the type tuple when done
 * with it.
 *
 * NB: direct callers of this function MUST check typisdefined before assuming
 * that the type is fully valid.  Most code should go through typenameType
 * or typenameTypeId instead.
 *
 * typmod_p can be passed as NULL if the caller does not care to know the
 * typmod value, but the typmod decoration (if any) will be validated anyway,
 * except in the case where the type is not found.	Note that if the type is
 * found but is a shell, and there is typmod decoration, an error will be
 * thrown --- this is intentional.
 *
 * If temp_ok is false, ignore types in the temporary namespace.  Pass false
 * when the caller will decide, using goodness of fit criteria, whether the
 * typeName is actually a type or something else.  If typeName always denotes
 * a type (or denotes nothing), pass true.
 *
 * pstate is only used for error location info, and may be NULL.
 */
Type LookupTypeNameExtended(ParseState* pstate, const TypeName* typname, int32* typmod_p, bool temp_ok,
                            bool print_notice, TypeDependExtend* dependExtend)
{
    Oid typoid = InvalidOid;
    HeapTuple tup = NULL;
    int32 typmod = -1;
    Oid pkgOid = InvalidOid;
    bool notPkgType = false;
    char* schemaname = NULL;
    char* typeName = NULL;
    char* pkgName = NULL;
    if (typname->names == NIL) {
        /* We have the OID already if it's an internally generated TypeName */
        typoid = typname->typeOid;
    } else if (typname->pct_type) {
        /* Handle %TYPE reference to type of an existing field */
        RangeVar* rel = makeRangeVar(NULL, NULL, typname->location);
        char* field = NULL;
        Oid relid = InvalidOid;
        AttrNumber attnum = InvalidAttrNumber;
        char* pkgName = NULL;
        char* schemaName = NULL;
        /* deconstruct the name list */
        int typTupStatus = InvalidTypeTup;
        switch (list_length(typname->names)) {
            case 1:
                tup = getPLpgsqlVarTypeTup(strVal(linitial(typname->names)));
                typTupStatus = GetTypeTupStatus(tup);
                if (typTupStatus == NormalTypeTup) {
                    return tup;
                }
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg(
                            "improper %%TYPE reference (too few dotted names): %s", NameListToString(typname->names)),
                        parser_errposition(pstate, typname->location)));
                break;
            case 2:
                tup = FindPkgVariableType(pstate, typname, typmod_p, dependExtend);
                typTupStatus = GetTypeTupStatus(tup);
                if (typTupStatus == NormalTypeTup) {
                    return (Type)tup;
                }
                rel->relname = strVal(linitial(typname->names));
                field = strVal(lsecond(typname->names));
                break;
            case 3:
                tup = FindPkgVariableType(pstate, typname, typmod_p, dependExtend);
                typTupStatus = GetTypeTupStatus(tup);
                if (typTupStatus == NormalTypeTup) {
                    return (Type)tup;
                }
                pkgName = strVal(linitial(typname->names));
                pkgOid = PackageNameGetOid(pkgName, InvalidOid);
                if (OidIsValid(pkgOid)) {
                    rel->relname = CastPackageTypeName(strVal(lsecond(typname->names)), pkgOid, true);
                } else {
                    rel->schemaname = strVal(linitial(typname->names));
                    rel->relname = strVal(lsecond(typname->names));
                }
                field = strVal(lthird(typname->names));
                break;
            case 4:
                tup = FindPkgVariableType(pstate, typname, typmod_p, dependExtend);
                typTupStatus = GetTypeTupStatus(tup);
                if (typTupStatus == NormalTypeTup) {
                    return (Type)tup;
                }
                pkgName = strVal(lsecond(typname->names));
                schemaName = strVal(linitial(typname->names));
                pkgOid = PackageNameGetOid(pkgName, get_namespace_oid(schemaName, true));
                if (OidIsValid(pkgOid)) {
                    rel->relname = CastPackageTypeName(strVal(lthird(typname->names)), pkgOid, true);
                    rel->schemaname = schemaName;
                } else {
                    rel->catalogname = strVal(linitial(typname->names));
                    rel->schemaname = strVal(lsecond(typname->names));
                    rel->relname = strVal(lthird(typname->names));
                }
                field = strVal(lfourth(typname->names));
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg(
                            "improper %%TYPE reference (too many dotted names): %s", NameListToString(typname->names)),
                        parser_errposition(pstate, typname->location)));
                break;
        }

        /*
         * Look up the field.
         *
         * XXX: As no lock is taken here, this might fail in the presence of
         * concurrent DDL.	But taking a lock would carry a performance
         * penalty and would also require a permissions check.
         */
        if (u_sess->plsql_cxt.curr_compile_context != NULL &&
            u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package != NULL) {
            relid = LookupPctTypeInPackage(rel,
                u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid, field);
        }
        if (!OidIsValid(relid)) {
            if (enable_plpgsql_undefined()) {
                relid = RangeVarGetRelidExtended(rel, NoLock, true, false, false, true, NULL, NULL, NULL, NULL);
                if (!OidIsValid(relid) && HeapTupleIsValid(tup)) {
                    if (NULL != dependExtend) {
                        dependExtend->dependUndefined = true;
                    }
                    if (GetCurrCompilePgObjStatus() &&
                        u_sess->plsql_cxt.functionStyleType != FUNCTION_STYLE_TYPE_REFRESH_HEAD) {
                        ereport(WARNING,
                            (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("TYPE %s does not exist in type.", rel->relname)));
                    }
                    InvalidateCurrCompilePgObj();
                    return tup;
                }
            } else {
                relid = RangeVarGetRelidExtended(rel, NoLock, false, false, false, true, NULL, NULL, NULL, NULL);
            }
        }
        attnum = get_attnum(relid, field);
        if (attnum == InvalidAttrNumber) {
            if (enable_plpgsql_undefined()) {
                if (NULL != dependExtend) {
                    dependExtend->dependUndefined = true;
                }
                return SearchSysCache1(TYPEOID, ObjectIdGetDatum(UNDEFINEDOID));
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_COLUMN),
                        errmsg("column \"%s\" of relation \"%s\" does not exist", field, rel->relname),
                        parser_errposition(pstate, typname->location)));
            }
        }
        typoid = get_atttype(relid, attnum);

        if (IsClientLogicType(typoid)) {
            typoid = get_atttypmod(relid, attnum);
        } else {
            typmod = get_atttypmod(relid, attnum);
        }

        if (enable_plpgsql_undefined() && UndefineTypeTup == typTupStatus && NULL != dependExtend) {
            gsplsql_delete_unrefer_depend_obj_oid(dependExtend->undefDependObjOid, false);
            dependExtend->undefDependObjOid = InvalidOid;
            ReleaseSysCache(tup);
            tup = NULL;
        }
        if (enable_plpgsql_gsdependency() && NULL != dependExtend) {
            dependExtend->typeOid = get_rel_type_id(relid);
        }

        /* If an array reference, return the array type instead */
        if (typname->arrayBounds != NIL) {
            typoid = get_array_type(typoid);
        }

        /* emit nuisance notice (intentionally not errposition'd) */
        if (print_notice) {
            ereport(NOTICE,
                (errmsg("type reference %s converted to %s", TypeNameToString(typname), format_type_be(typoid))));
        }
        if (OidIsValid(pkgOid)) {
            pfree_ext(rel->relname);
        }
    } else {
        /* Normal reference to a type name */
        /* Handle %ROWTYPE reference to type of an existing table. */
        if (typname->pct_rowtype) {
            RangeVar* relvar = NULL;
            /* deconstruct the name list */
            switch (list_length(typname->names)) {
                case 1:
                    relvar = makeRangeVar(NULL, strVal(linitial(typname->names)), -1);
                    break;
                case 2:
                    relvar = makeRangeVar(strVal(linitial(typname->names)), strVal(lsecond(typname->names)), -1);
                    break;
                case 3:
                    relvar = makeRangeVar(strVal(lsecond(typname->names)), strVal(lthird(typname->names)), -1);
                    relvar->catalogname = strVal(linitial(typname->names));
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("improper %%ROWTYPE reference"),
                            errdetail("improper %%ROWTYPE reference (too many dotted names): %s",
                                NameListToString(typname->names)),
                            errcause("syntax error"),
                            erraction("check the relation name for %%ROWTYPE")));
                    break;
            }
            Oid class_oid = RangeVarGetRelidExtended(relvar, NoLock, true, false, false, true, NULL, NULL);
            if (!OidIsValid(class_oid)) {
                /* if case: cursor%rowtype */
                tup = getCursorTypeTup(strVal(linitial(typname->names)));
                if (HeapTupleIsValid(tup)) {
                    return (Type)tup;
                }
                if (enable_plpgsql_undefined() && NULL != dependExtend) {
                    Oid undefRefObjOid = gsplsql_try_build_exist_schema_undef_table(relvar);
                    if (OidIsValid(undefRefObjOid)) {
                        dependExtend->undefDependObjOid = undefRefObjOid;
                        dependExtend->dependUndefined = true;
                        InvalidateCurrCompilePgObj();
                        tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(UNDEFINEDOID));
                        if (typmod_p != NULL) {
                            *typmod_p = -1;
                        }
                    }
                }
                pfree_ext(relvar);
                ereport(NULL != tup ? WARNING : ERROR,
                    (errmodule(MOD_PARSER),
                        errcode(ERRCODE_UNDEFINED_TABLE),
                        errmsg("relation does not exist when parse word."),
                        errdetail(" relation \"%s\" referenced by %%ROWTYPE does not exist.",
                                NameListToString(typname->names)),
                        errcause("incorrectly referencing relation"),
                        erraction("check the relation name for %%ROWTYPE")));
                return (Type)tup;
            }
            char relkind = get_rel_relkind(class_oid);
            /* onyl table is allowed for %ROWTYPE. */
            if ((relkind != 'r') && (relkind != 'm')) {
                pfree_ext(relvar);
                ereport(ERROR,
                    (errmodule(MOD_PARSER),
                        errcode(ERRCODE_UNDEFINED_TABLE),
                        errmsg("relation does not exist when parse word."),
                        errdetail(" relation \"%s\" referenced by %%ROWTYPE does not exist.",
                                NameListToString(typname->names)),
                        errcause("incorrectly referencing relation"),
                        erraction("check the relation name for %%ROWTYPE")));

            }
            pfree_ext(relvar);
        }

        /* deconstruct the name list */
        DeconstructQualifiedName(typname->names, &schemaname, &typeName, &pkgName);
        Oid namespaceId = InvalidOid;
        if (schemaname != NULL) {
            namespaceId = LookupExplicitNamespace(schemaname);
        }
        bool isPkgType = (pkgName != NULL) &&
            !(list_length(typname->names) == 2 && schemaname != NULL && strcmp(pkgName, typeName) == 0);
        if (isPkgType) {
            /* type is package defined, get the cast type name */
            pkgOid = PackageNameGetOid(pkgName, namespaceId);
        }
        if (schemaname != NULL) {
            /* Look in package type */
            if (isPkgType) {
                typoid = LookupTypeInPackage(typname->names, typeName, pkgOid, namespaceId);
            } else {
                /* Look in specific schema only */
                typoid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(typeName), ObjectIdGetDatum(namespaceId));
            }
            if (!OidIsValid(typoid)) {
                typoid = TryLookForSynonymType(typeName, namespaceId);
                notPkgType = true; /* should also track type dependency, fix when refactoring */
            }
        } else {
            if (pkgName == NULL) {
                /* find type in current packgae first */
                typoid = LookupTypeInPackage(typname->names, typeName);
            }
            if (enable_plpgsql_gsdependency_guc()) {
                if (isPkgType) {
                    typoid = LookupTypeInPackage(typname->names, typeName, pkgOid);
                } else if (!OidIsValid(typoid)) {
                    /* Unqualified type name, so search the search path */
                    typoid = TypenameGetTypidExtended(typeName, temp_ok);
                    notPkgType = true; /* should also track type dependency, fix when refactoring */
                }
            } else {
                if (isPkgType) {
                    typoid = LookupTypeInPackage(typname->names, typeName, pkgOid);
                }
                if (!OidIsValid(typoid)) {
                    /* Unqualified type name, so search the search path */
                    typoid = TypenameGetTypidExtended(typeName, temp_ok);
                    notPkgType = true; /* should also track type dependency, fix when refactoring */
                }
            }
        }

        /* If an array reference, return the array type instead */
        if (typname->arrayBounds != NIL) {
            typoid = get_array_type(typoid);
        }
    }

    if (u_sess->plsql_cxt.need_pkg_dependencies && OidIsValid(pkgOid) && !notPkgType) {
        MemoryContext temp = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
        u_sess->plsql_cxt.pkg_dependencies =
            list_append_unique_oid(u_sess->plsql_cxt.pkg_dependencies, pkgOid);
        MemoryContextSwitchTo(temp);
    }

    if (!OidIsValid(typoid)) {
        if (typmod_p != NULL) {
            *typmod_p = -1;
        }
        if (enable_plpgsql_undefined() && NULL != dependExtend) {
            if (NULL != schemaname && NULL == pkgName && !OidIsValid(get_namespace_oid(schemaname, true))) {
                pkgName = schemaname;
                schemaname = NULL;
            }
            GsDependObjDesc objDesc;
            objDesc.schemaName = schemaname;
            char* activeSchemaName = NULL;
            if (schemaname == NULL) {
                activeSchemaName = get_namespace_name(get_compiled_object_nspoid());
                objDesc.schemaName = activeSchemaName;
            }
            objDesc.packageName = pkgName;
            objDesc.name = typeName;
            objDesc.type = GSDEPEND_OBJECT_TYPE_TYPE;
            if (u_sess->plsql_cxt.functionStyleType != FUNCTION_STYLE_TYPE_REFRESH_HEAD) {
                dependExtend->undefDependObjOid = gsplsql_flush_undef_ref_depend_obj(&objDesc);
            } else {
                dependExtend->undefDependObjOid = InvalidOid;
            }
            dependExtend->dependUndefined = true;
            pfree_ext(activeSchemaName);
            if (GetCurrCompilePgObjStatus() &&
                u_sess->plsql_cxt.functionStyleType != FUNCTION_STYLE_TYPE_REFRESH_HEAD) {
                    ereport(WARNING,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("Type %s does not exist.", typeName)));
            }
            InvalidateCurrCompilePgObj();
            tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(UNDEFINEDOID));
        }
    } else {
        /* Don't support the type in blacklist. */
        bool is_unsupported_type = !u_sess->attr.attr_common.IsInplaceUpgrade && IsTypeInBlacklist(typoid);
        if (is_unsupported_type) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("type %s is not yet supported.", format_type_be(typoid))));
        }

        tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));

        /* should not happen */
        if (!HeapTupleIsValid(tup)) {
            ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typoid)));
        }
        if (!typname->pct_type) {
            typmod = typenameTypeMod(pstate, typname, (Type)tup);
        }
        if (typmod_p != NULL) {
            *typmod_p = typmod;
        }
    }
    return (Type)tup;
}

/*
 * typenameType - given a TypeName, return a Type structure and typmod
 *
 * This is equivalent to LookupTypeName, except that this will report
 * a suitable error message if the type cannot be found or is not defined.
 * Callers of this can therefore assume the result is a fully valid type.
 */
Type typenameType(ParseState* pstate, const TypeName* typname, int32* typmod_p, TypeDependExtend* dependExtend)
{
    Type tup;

    tup = LookupTypeName(pstate, typname, typmod_p, true, dependExtend);

    /*
     * If the type is relation, then we check
     * whether the table is in installation group
     */
    if (!in_logic_cluster() && !IsTypeTableInInstallationGroup(tup)) {
        InsertErrorMessage("type must be in installation group", u_sess->plsql_cxt.plpgsql_yylloc);
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("type '%s' must be in installation group", TypeNameToString(typname))));
    }

    if (tup == NULL) {
        InsertErrorMessage("type does not exist", u_sess->plsql_cxt.plpgsql_yylloc);
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("type \"%s\" does not exist", TypeNameToString(typname)),
                parser_errposition(pstate, typname->location)));
    }
        
    if (!((Form_pg_type)GETSTRUCT(tup))->typisdefined) {
        InsertErrorMessage("type is only a shell", u_sess->plsql_cxt.plpgsql_yylloc);
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("type \"%s\" is only a shell", TypeNameToString(typname)),
                parser_errposition(pstate, typname->location)));
    }
    return tup;
}

/*
 * typenameTypeId - given a TypeName, return the type's OID
 *
 * This is similar to typenameType, but we only hand back the type OID
 * not the syscache entry.
 */
Oid typenameTypeId(ParseState* pstate, const TypeName* typname)
{
    Oid typoid;
    Type tup;

    tup = typenameType(pstate, typname, NULL);
    typoid = HeapTupleGetOid(tup);
    ReleaseSysCache(tup);

    return typoid;
}

/*
 * typenameTypeIdAndMod - given a TypeName, return the type's OID and typmod
 *
 * This is equivalent to typenameType, but we only hand back the type OID
 * and typmod, not the syscache entry.
 */
void typenameTypeIdAndMod(ParseState* pstate, const TypeName* typname, Oid* typeid_p, int32* typmod_p, TypeDependExtend* dependExtend)
{
    Type tup;

    tup = typenameType(pstate, typname, typmod_p, dependExtend);
    *typeid_p = HeapTupleGetOid(tup);
    ReleaseSysCache(tup);
}

/*
 * typenameTypeMod - given a TypeName, return the internal typmod value
 *
 * This will throw an error if the TypeName includes type modifiers that are
 * illegal for the data type.
 *
 * The actual type OID represented by the TypeName must already have been
 * looked up, and is passed as "typ".
 *
 * pstate is only used for error location info, and may be NULL.
 */
static int32 typenameTypeMod(ParseState* pstate, const TypeName* typname, Type typ)
{
    int32 result;
    Oid typmodin;
    Datum* datums = NULL;
    int n;
    ListCell* l = NULL;
    ArrayType* arrtypmod = NULL;
    ParseCallbackState pcbstate;

    /* Return prespecified typmod if no typmod expressions */
    if (typname->typmods == NIL) {
        return typname->typemod;
    }

    /*
     * Else, type had better accept typmods.  We give a special error message
     * for the shell-type case, since a shell couldn't possibly have a
     * typmodin function.
     */
    if (!((Form_pg_type)GETSTRUCT(typ))->typisdefined) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("type modifier cannot be specified for shell type \"%s\"", TypeNameToString(typname)),
                parser_errposition(pstate, typname->location)));
    }

    typmodin = ((Form_pg_type)GETSTRUCT(typ))->typmodin;

    if (typmodin == InvalidOid) {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("type modifier is not allowed for type \"%s\"", TypeNameToString(typname)),
                parser_errposition(pstate, typname->location)));
    }

    /*
     * Convert the list of raw-grammar-output expressions to a cstring array.
     * Currently, we allow simple numeric constants, string literals, and
     * identifiers; possibly this list could be extended.
     */
    datums = (Datum*)palloc(list_length(typname->typmods) * sizeof(Datum));
    n = 0;
    foreach (l, typname->typmods) {
        Node* tm = (Node*)lfirst(l);
        char* cstr = NULL;

        if (IsA(tm, A_Const)) {
            A_Const* ac = (A_Const*)tm;

            if (IsA(&ac->val, Integer)) {
                const int len = 32;
                cstr = (char*)palloc0(len);
                errno_t rc = snprintf_s(cstr, len, len - 1, "%ld", (long)ac->val.val.ival);
                securec_check_ss(rc, "", "");
            } else if (IsA(&ac->val, Float) || IsA(&ac->val, String)) {
                /* we can just use the str field directly. */
                cstr = ac->val.val.str;
            }
        } else if (IsA(tm, ColumnRef)) {
            ColumnRef* cr = (ColumnRef*)tm;

            if (list_length(cr->fields) == 1 && IsA(linitial(cr->fields), String)) {
                cstr = strVal(linitial(cr->fields));
            }
        }
        if (cstr == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("type modifiers must be simple constants or identifiers"),
                    parser_errposition(pstate, typname->location)));
        }
        datums[n++] = CStringGetDatum(cstr);
    }

    /* hardwired knowledge about cstring's representation details here */
    arrtypmod = construct_array(datums, n, CSTRINGOID, -2, false, 'c');

    /* arrange to report location if type's typmodin function fails */
    setup_parser_errposition_callback(&pcbstate, pstate, typname->location);

    result = DatumGetInt32(OidFunctionCall1(typmodin, PointerGetDatum(arrtypmod)));

    cancel_parser_errposition_callback(&pcbstate);

    pfree_ext(datums);
    pfree_ext(arrtypmod);

    return result;
}

/*
 * appendTypeNameToBuffer
 *		Append a string representing the name of a TypeName to a StringInfo.
 *		This is the shared guts of TypeNameToString and TypeNameListToString.
 *
 * NB: this must work on TypeNames that do not describe any actual type;
 * it is mostly used for reporting lookup errors.
 */
static void appendTypeNameToBuffer(const TypeName* typname, StringInfo string)
{
    if (typname->names != NIL) {
        /* Emit possibly-qualified name as-is */
        ListCell* l = NULL;

        foreach (l, typname->names) {
            if (l != list_head(typname->names)) {
                appendStringInfoChar(string, '.');
            }
            appendStringInfoString(string, strVal(lfirst(l)));
        }
    } else {
        /* Look up internally-specified type */
        appendStringInfoString(string, format_type_be(typname->typeOid));
    }

    /*
     * Add decoration as needed, but only for fields considered by
     * LookupTypeName
     */
    if (typname->pct_type) {
        appendStringInfoString(string, "%TYPE");
    }

    if (typname->arrayBounds != NIL) {
        appendStringInfoString(string, "[]");
    }
}

/*
 * TypeNameToString
 *		Produce a string representing the name of a TypeName.
 *
 * NB: this must work on TypeNames that do not describe any actual type;
 * it is mostly used for reporting lookup errors.
 */
char* TypeNameToString(const TypeName* typname)
{
    StringInfoData string;

    initStringInfo(&string);
    appendTypeNameToBuffer(typname, &string);
    return string.data;
}

/*
 * TypeNameListToString
 *		Produce a string representing the name(s) of a List of TypeNames
 */
char* TypeNameListToString(List* typenames)
{
    StringInfoData string;
    ListCell* l = NULL;

    initStringInfo(&string);
    foreach (l, typenames) {
        TypeName* typname = (TypeName*)lfirst(l);

        AssertEreport(IsA(typname, TypeName), MOD_OPT, "");
        if (l != list_head(typenames)) {
            appendStringInfoChar(&string, ',');
        }
        appendTypeNameToBuffer(typname, &string);
    }
    return string.data;
}

/*
 * LookupCollation
 *
 * Look up collation by name, return OID, with support for error location.
 */
Oid LookupCollation(ParseState* pstate, List* collnames, int location)
{
    Oid colloid;
    ParseCallbackState pcbstate;

    if (pstate != NULL) {
        setup_parser_errposition_callback(&pcbstate, pstate, location);
    }

    colloid = get_collation_oid(collnames, false);

    if (pstate != NULL) {
        cancel_parser_errposition_callback(&pcbstate);
    }

    return colloid;
}

Oid get_column_def_collation_b_format(ColumnDef* coldef, Oid typeOid, Oid typcollation,
    bool is_bin_type, Oid rel_coll_oid)
{
    if (coldef->typname->charset != PG_INVALID_ENCODING && !IsSupportCharsetType(typeOid) && !type_is_enum(typeOid) && !type_is_set(typeOid)) {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("type %s not support set charset", format_type_be(typeOid))));
    }

    Oid result = InvalidOid;
    if (!OidIsValid(typcollation) && !is_bin_type && !type_is_set(typeOid)) {
        return InvalidOid;
    } else if (OidIsValid(coldef->collOid)) {
        /* Precooked collation spec, use that */
        return coldef->collOid;
    }

    char* schemaname = NULL;
    char* collate = NULL;
    if (coldef->collClause) {
        DeconstructQualifiedName(coldef->collClause->collname, &schemaname, &collate);
        if (schemaname != NULL && strcmp(schemaname, "pg_catalog") != 0) {
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA),
                    errmsg("error schema name for collate")));
        }
    }
    /* For binary type, if the table's default collation is not "binary", the rel_coll_oid is not inherited. */
    if (is_bin_type) {
        rel_coll_oid = InvalidOid;
    }
    result = transform_default_collation(collate, coldef->typname->charset, rel_coll_oid, true);
    if (!OidIsValid(result)) {
        if (!USE_DEFAULT_COLLATION) {
            result = typcollation;
        } else if (is_bin_type) {
            result = BINARY_COLLATION_OID;
        } else {
            result = get_default_collation_by_charset(GetDatabaseEncoding());
        }
    }
    return result;
}

/*
 * GetColumnDefCollation
 *
 * Get the collation to be used for a column being defined, given the
 * ColumnDef node and the previously-determined column type OID.
 *
 * pstate is only used for error location purposes, and can be NULL.
 */
Oid GetColumnDefCollation(ParseState* pstate, ColumnDef* coldef, Oid typeOid, Oid rel_coll_oid)
{
    Oid result;
    Oid typcollation = get_typcollation(typeOid);
    int location = -1;
    bool is_bin_type = IsBinaryType(typeOid);

    if (DB_IS_CMPT(B_FORMAT)) {
        result = get_column_def_collation_b_format(coldef, typeOid, typcollation, is_bin_type, rel_coll_oid);
    } else if (coldef->collClause) {
        /* We have a raw COLLATE clause, so look up the collation */
        location = coldef->collClause->location;
        result = LookupCollation(pstate, coldef->collClause->collname, location);
    } else if (OidIsValid(coldef->collOid)) {
        /* Precooked collation spec, use that */
        result = coldef->collOid;
    } else {
        /* Use the type's default collation if any */
        result = typcollation;
    }

    if (coldef->collClause) {
        check_binary_collation(result, typeOid);
    }
    /* Complain if COLLATE is applied to an uncollatable type */
    if (OidIsValid(result) && !OidIsValid(typcollation) && !is_bin_type && !type_is_set(typeOid)) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("collations are not supported by type %s", format_type_be(typeOid)),
                parser_errposition(pstate, location)));
    }

    return result;
}

/* return a Type structure, given a type id */
/* NB: caller must ReleaseSysCache the type tuple when done with it */
Type typeidType(Oid id)
{
    HeapTuple tup;

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(id));
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", id)));
    }
    return (Type)tup;
}

/* given type (as type struct), return the type OID */
Oid typeTypeId(Type tp)
{
    if (tp == NULL)  { /* probably useless */ 
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("typeTypeId() called with NULL type struct")));
    }
    return HeapTupleGetOid(tp);
}

/* given type (as type struct), return the length of type */
int16 typeLen(Type t)
{
    Form_pg_type typ;

    typ = (Form_pg_type)GETSTRUCT(t);
    return typ->typlen;
}

/* given type (as type struct), return its 'byval' attribute */
bool typeByVal(Type t)
{
    Form_pg_type typ;

    typ = (Form_pg_type)GETSTRUCT(t);
    return typ->typbyval;
}

/* given type (as type struct), return the type's name */
char* typeTypeName(Type t)
{
    Form_pg_type typ;

    typ = (Form_pg_type)GETSTRUCT(t);
    /* pstrdup here because result may need to outlive the syscache entry */
    return pstrdup(NameStr(typ->typname));
}

/* given type (as type struct), return its 'typrelid' attribute */
Oid typeTypeRelid(Type typ)
{
    Form_pg_type typtup;

    typtup = (Form_pg_type)GETSTRUCT(typ);
    return typtup->typrelid;
}

/* given type (as type struct), return its 'typcollation' attribute */
Oid typeTypeCollation(Type typ)
{
    Form_pg_type typtup;

    typtup = (Form_pg_type)GETSTRUCT(typ);
    return typtup->typcollation;
}

/*
 * Given a type structure and a string, returns the internal representation
 * of that string.	The "string" can be NULL to perform conversion of a NULL
 * (which might result in failure, if the input function rejects NULLs).
 *
 * With param can_ignore == true, truncation or transformation may be cast
 * for input string if string is invalid for target type.
 */
Datum stringTypeDatum(Type tp, char* string, int32 atttypmod, bool can_ignore)
{
    Form_pg_type typform = (Form_pg_type)GETSTRUCT(tp);
    Oid typinput = typform->typinput;
    Oid typioparam = getTypeIOParam(tp);
    Datum result;

    switch (typinput) {
    case F_DATE_IN:
        result = input_date_in(string, can_ignore);
        break;
    case F_BPCHARIN:
        result = input_bpcharin(string, typioparam, atttypmod);
        break;
    case F_VARCHARIN:
        result = input_varcharin(string, typioparam, atttypmod);
        break;
    case F_TIMESTAMP_IN:
        result = input_timestamp_in(string, typioparam, atttypmod, can_ignore);
        break;
    default:
        result = OidInputFunctionCall(typinput, string, typioparam, atttypmod, can_ignore);
    }

#ifdef RANDOMIZE_ALLOCATED_MEMORY

    /*
     * For pass-by-reference data types, repeat the conversion to see if the
     * input function leaves any uninitialized bytes in the result.  We can
     * only detect that reliably if RANDOMIZE_ALLOCATED_MEMORY is enabled, so
     * we don't bother testing otherwise.  The reason we don't want any
     * instability in the input function is that comparison of Const nodes
     * relies on bytewise comparison of the datums, so if the input function
     * leaves garbage then subexpressions that should be identical may not get
     * recognized as such.	See pgsql-hackers discussion of 2008-04-04.
     */
    if (string && !typform->typbyval) {
        Datum result2;

        result2 = OidInputFunctionCall(typinput, string, typioparam, atttypmod);
        if (!datumIsEqual(result, result2, typform->typbyval, typform->typlen)) {
            elog(WARNING, "type %s has unstable input conversion for \"%s\"", NameStr(typform->typname), string);
        }
    }
#endif

    return result;
}

/* given a typeid, return the type's typrelid (associated relation, if any) */
Oid typeidTypeRelid(Oid type_id)
{
    HeapTuple typeTuple;
    Form_pg_type type;
    Oid result;

    typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_id));
    if (!HeapTupleIsValid(typeTuple))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", type_id)));

    type = (Form_pg_type)GETSTRUCT(typeTuple);
    result = type->typrelid;
    ReleaseSysCache(typeTuple);
    return result;
}

/*
 * error context callback for parse failure during parseTypeString()
 */
static void pts_error_callback(void* arg)
{
    const char* str = (const char*)arg;

    errcontext("invalid type name \"%s\"", str);

    /*
     * Currently we just suppress any syntax error position report, rather
     * than transforming to an "internal query" error.	It's unlikely that a
     * type name is complex enough to need positioning.
     */
    errposition(0);
}

/*
 * Given a string that is supposed to be a SQL-compatible type declaration,
 * such as "int4" or "integer" or "character varying(32)", parse
 * the string and convert it to a type OID and type modifier.
 */
void parseTypeString(const char* str, Oid* typeid_p, int32* typmod_p, TypeDependExtend* dependExtend)
{
    StringInfoData buf;
    buf.data = NULL;
    List* raw_parsetree_list = NIL;
    SelectStmt* stmt = NULL;
    ResTarget* restarget = NULL;
    TypeCast* typecast = NULL;
    TypeName* typname = NULL;
    ErrorContextCallback ptserrcontext;

    /* make sure we give useful error for empty input */
    if (strspn(str, " \t\n\r\f") == strlen(str)) {
        goto fail;
    }

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT NULL::%s", str);

    /*
     * Setup error traceback support in case of ereport() during parse
     */
    ptserrcontext.callback = pts_error_callback;
    ptserrcontext.arg = (void*)str;
    ptserrcontext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &ptserrcontext;

    raw_parsetree_list = raw_parser(buf.data);

    t_thrd.log_cxt.error_context_stack = ptserrcontext.previous;

    /*
     * Make sure we got back exactly what we expected and no more; paranoia is
     * justified since the string might contain anything.
     */
    if (list_length(raw_parsetree_list) != 1)
        goto fail;
    stmt = (SelectStmt*)linitial(raw_parsetree_list);
    if (stmt == NULL || !IsA(stmt, SelectStmt) || stmt->distinctClause != NIL || stmt->intoClause != NULL ||
        stmt->fromClause != NIL || stmt->whereClause != NULL || stmt->groupClause != NIL ||
        stmt->havingClause != NULL || stmt->windowClause != NIL || stmt->withClause != NULL ||
        stmt->valuesLists != NIL || stmt->sortClause != NIL || stmt->limitOffset != NULL || stmt->limitCount != NULL ||
        stmt->lockingClause != NIL || stmt->op != SETOP_NONE) {
        goto fail;
    }
    if (list_length(stmt->targetList) != 1) {
        goto fail;
    }
    restarget = (ResTarget*)linitial(stmt->targetList);
    if (restarget == NULL || !IsA(restarget, ResTarget) || restarget->name != NULL || restarget->indirection != NIL) {
        goto fail;
    }
    typecast = (TypeCast*)restarget->val;
    if (typecast == NULL || !IsA(typecast, TypeCast) || typecast->arg == NULL || !IsA(typecast->arg, A_Const)) {
        goto fail;
    }
    typname = typecast->typname;
    if (typname == NULL || !IsA(typname, TypeName)) {
        goto fail;
    }
    if (typname->setof) {
        goto fail;
    }

    typenameTypeIdAndMod(NULL, typname, typeid_p, typmod_p, dependExtend);

    pfree_ext(buf.data);

    return;

fail:
    pfree_ext(buf.data);
    InsertErrorMessage("invalid type name", u_sess->plsql_cxt.plpgsql_yylloc);
    if (enable_plpgsql_undefined()) {
        InvalidateCurrCompilePgObj();
        *typeid_p = UNDEFINEDOID;
        ereport(WARNING, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid type name \"%s\"", str)));
    } else {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid type name \"%s\"", str)));
    }
}

/*
 * Given a string that is supposed to be a SQL-compatible type declaration,
 * such as "int4" or "integer" or "character varying(32)", parse
 * the string and return the result as a TypeName.
 * If the string cannot be parsed as a type, an error is raised.
 */
TypeName * typeStringToTypeName(const char *str)
{
    StringInfoData buf;
    buf.data = NULL;
    List* raw_parsetree_list = NIL;
    SelectStmt* stmt = NULL;
    ResTarget* restarget = NULL;
    TypeCast* typecast = NULL;
    TypeName* typname = NULL;
    ErrorContextCallback ptserrcontext;

    /* make sure we give useful error for empty input */
    if (strspn(str, " \t\n\r\f") == strlen(str)) {
        goto fail;
    }

    initStringInfo(&buf);
    appendStringInfo(&buf, "SELECT NULL::%s", str);

    /*
     * Setup error traceback support in case of ereport() during parse
     */
    ptserrcontext.callback = pts_error_callback;
    ptserrcontext.arg = (void*)str;
    ptserrcontext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &ptserrcontext;

    raw_parsetree_list = raw_parser(buf.data);

    t_thrd.log_cxt.error_context_stack = ptserrcontext.previous;

    /*
     * Make sure we got back exactly what we expected and no more; paranoia is
     * justified since the string might contain anything.
     */
    if (list_length(raw_parsetree_list) != 1)
        goto fail;
    stmt = (SelectStmt*)linitial(raw_parsetree_list);
    if (stmt == NULL || !IsA(stmt, SelectStmt) || stmt->distinctClause != NIL || stmt->intoClause != NULL ||
        stmt->fromClause != NIL || stmt->whereClause != NULL || stmt->groupClause != NIL ||
        stmt->havingClause != NULL || stmt->windowClause != NIL || stmt->withClause != NULL ||
        stmt->valuesLists != NIL || stmt->sortClause != NIL || stmt->limitOffset != NULL || stmt->limitCount != NULL ||
        stmt->lockingClause != NIL || stmt->op != SETOP_NONE) {
        goto fail;
    }
    if (list_length(stmt->targetList) != 1) {
        goto fail;
    }
    restarget = (ResTarget*)linitial(stmt->targetList);
    if (restarget == NULL || !IsA(restarget, ResTarget) || restarget->name != NULL || restarget->indirection != NIL) {
        goto fail;
    }
    typecast = (TypeCast*)restarget->val;
    if (typecast == NULL || !IsA(typecast, TypeCast) || typecast->arg == NULL || !IsA(typecast->arg, A_Const)) {
        goto fail;
    }
    typname = typecast->typname;
    if (typname == NULL || !IsA(typname, TypeName)) {
        goto fail;
    }
    if (typname->setof) {
        goto fail;
    }
    pfree_ext(buf.data);
 
    return typname;
 
fail:
    pfree_ext(buf.data);
    ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("invalid type name \"%s\"", str)));
    return NULL;
}

/*
 * IsTypeSupportedByCStore
 *      Return true if the type is supported by column store
 *
 * The performance of this function relies on compiler to flat the branches. But
 * it is ok if compiler failed to do its job as it is not in critical code path.
 */
bool IsTypeSupportedByCStore(Oid typeOid)
{
    switch (typeOid) {
        case BOOLOID:
        case HLL_OID: // same as BYTEA
        case BYTEAOID:
        case CHAROID:
        case HLL_HASHVAL_OID: // same as INT8
        case INT8OID:
        case INT2OID:
        case INT4OID:
        case INT1OID:
        case NUMERICOID:
        case BPCHAROID:
        case VARCHAROID:
        case NVARCHAR2OID:
        case SMALLDATETIMEOID:
        case TEXTOID:
        case OIDOID:
        case FLOAT4OID:
        case FLOAT8OID:
        case ABSTIMEOID:
        case RELTIMEOID:
        case TINTERVALOID:
        case INETOID:
        case DATEOID:
        case TIMEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        case INTERVALOID:
        case TIMETZOID:
        case CASHOID:
        case CIDROID:
        case BITOID:
        case VARBITOID:
        case CLOBOID:
        case BOOLARRAYOID: // array
        case HLL_ARRAYOID:
        case BYTEARRAYOID:
        case CHARARRAYOID:
        case HLL_HASHVAL_ARRAYOID:
        case INT8ARRAYOID:
        case INT2ARRAYOID:
        case INT4ARRAYOID:
        case INT1ARRAYOID:
        case ARRAYNUMERICOID:
        case BPCHARARRAYOID:
        case VARCHARARRAYOID:
        case NVARCHAR2ARRAYOID:
        case SMALLDATETIMEARRAYOID:
        case TEXTARRAYOID:
        case FLOAT4ARRAYOID:
        case FLOAT8ARRAYOID:
        case ABSTIMEARRAYOID:
        case RELTIMEARRAYOID:
        case ARRAYTINTERVALOID:
        case INETARRAYOID:
        case DATEARRAYOID:
        case TIMEARRAYOID:
        case TIMESTAMPARRAYOID:
        case TIMESTAMPTZARRAYOID:
        case ARRAYINTERVALOID:
        case ARRAYTIMETZOID:
        case CASHARRAYOID:
        case CIDRARRAYOID:
        case BITARRAYOID:
        case VARBITARRAYOID:
        case BYTEAWITHOUTORDERCOLOID:
        case BYTEAWITHOUTORDERWITHEQUALCOLOID:
            return true;
        default:
            break;
    }

    return false;
}

bool IsTypeSupportedByVectorEngine(Oid typeOid)
{
    if (IsTypeSupportedByCStore(typeOid)) {
        return true;
    }

    switch (typeOid) {
        /* Name, MacAddr and UUID also be processed in rowtovec and vectorow,
         * but it may cause result not correct. So do not support it here.
         */
        case VOIDOID:
        case UNKNOWNOID:
        case CSTRINGOID: {
            return true;
        }
        default:
            break;
    }

    ereport(DEBUG2, (errmodule(MOD_OPT_PLANNER),
        errmsg("Vectorize plan failed due to unsupport type: %u", typeOid)));

    return false;
}

/*
 * IsTypeSupportedByORCRelation
 * Return true if the type is supported by ORC format relation.
 */
bool IsTypeSupportedByORCRelation(_in_ Oid typeOid)
{
    /* we don't support user defined type */
    if (typeOid >= FirstNormalObjectId) {
        return false;
    }

    static Oid supportType[] = {BOOLOID,
        OIDOID,
        INT8OID,
        INT2OID,
        INT4OID,
        INT1OID,
        NUMERICOID,
        CHAROID,
        BPCHAROID,
        VARCHAROID,
        NVARCHAR2OID,
        TEXTOID,
        CLOBOID,
        FLOAT4OID,
        FLOAT8OID,
        DATEOID,
        TIMESTAMPOID,
        INTERVALOID,
        TINTERVALOID,
        TIMESTAMPTZOID,
        TIMEOID,
        TIMETZOID,
        SMALLDATETIMEOID,
        CASHOID};
    if (DATEOID == typeOid && C_FORMAT == u_sess->attr.attr_sql.sql_compatibility) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmodule(MOD_HDFS),
                errmsg("Date type is unsupported for hdfs table in C-format database.")));
    }

    for (uint32 i = 0; i < sizeof(supportType) / sizeof(Oid); ++i) {
        if (supportType[i] == typeOid) {
            return true;
		}
    }
    return false;
}

static bool is_support_old_ts_style(int kvtype, Oid typeOid)
{
    if (kvtype == ATT_KV_TAG || kvtype == ATT_KV_HIDE) {       
        return typeOid == TEXTOID;
    } else if (kvtype == ATT_KV_FIELD) {
        return (typeOid == NUMERICOID || typeOid == TEXTOID);
    } else if (kvtype == ATT_KV_TIMETAG) {
        return (typeOid == TIMESTAMPTZOID || typeOid == TIMESTAMPOID);
    } else {
        /* unrecognized data type */
        return false;
    }
}

static bool is_support_new_ts_style(int kvtype, Oid typeOid)
{
    static Oid support_type[] = {BOOLOID,
        INT8OID,
        INT4OID,
        NUMERICOID,
        BPCHAROID,
        TEXTOID,
        FLOAT4OID,
        FLOAT8OID};
    /* not support numeric */
    static Oid tag_support_type[] = {BOOLOID,
        INT8OID,
        INT4OID,
        BPCHAROID,
        TEXTOID};
    if (kvtype == ATT_KV_TAG) {
        for (uint32 i = 0; i < sizeof(tag_support_type) / sizeof(Oid); ++i) {           
            if (tag_support_type[i] == typeOid) {
                return true;
            }
        }
        return false;
    } else if (kvtype == ATT_KV_FIELD) {
        for (uint32 i = 0; i < sizeof(support_type) / sizeof(Oid); ++i) {           
            if (support_type[i] == typeOid) {
                return true;
            }
        }
        return false;
    } else if (kvtype == ATT_KV_TIMETAG) {
        return (typeOid == TIMESTAMPTZOID || typeOid == TIMESTAMPOID);
    } else if (kvtype == ATT_KV_HIDE) {
        /* hidetag column only support type char */
        if (typeOid == CHAROID) {
            return true;
        } else {
            ereport(LOG, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Invalid hide column type: %u", typeOid)));
            return false;
        }
    } else {
        /* unrecognized data type */
        return false;
    }
}

/*
 * Used in tsdb. Return true if the type is supported by tsdb.
 * Supported types for tags: text
 * Supported types for fields and tag: numeric, text, bool, int, float, double
 * for upgrade, if create the new table using new support data type before commit after all nodes are new version,
 * after rollback to old version, insert will core, so if we support new data type, we must use version number to
 * control only can create the table after commit 
 * Parameters:
 *  - kvtype: whether the column is a tag or a field
 *  - typeOid: oid of the data type
 */
bool IsTypeSupportedByTsStore(_in_ int kvtype, _in_ Oid typeOid)
{
    const int support_type_number = 92257;
    if (pg_atomic_read_u32(&WorkingGrandVersionNum) >= support_type_number) {
        return is_support_new_ts_style(kvtype, typeOid);
    } else {
        return is_support_old_ts_style(kvtype, typeOid);
    }
}

/*
 * IsTypeSupportedByUStore
 *    Return true if the type is supported by UStore
 *
 * The performance of this function relies on compiler to flat the branches. But
 * it is ok if compiler failed to do its job as it is not in critical code path.
 */
bool
IsTypeSupportedByUStore(_in_ const Oid typeOid, _in_ const int32 typeMod)
{
    /* we don't support user defined type. */
    if (typeOid >= FirstNormalObjectId)
        return false;

    static Oid supportType[] = {
        BOOLOID,
        BYTEAOID,
        CHAROID,
        INT8OID,
        INT2OID,
        INT4OID,
        INT1OID,
        NUMERICOID,
        BPCHAROID,
        VARCHAROID,
        NVARCHAR2OID,
        SMALLDATETIMEOID,
        TEXTOID,
        OIDOID,
        FLOAT4OID,
        FLOAT8OID,
        ABSTIMEOID,
        RELTIMEOID,
        TINTERVALOID,
        INETOID,
        DATEOID,
        TIMEOID,
        TIMESTAMPOID,
        TIMESTAMPTZOID,
        INTERVALOID,
        TIMETZOID,
        CASHOID,
        CIDROID,
        BITOID,
        VARBITOID,
	NAMEOID,
        RAWOID,
        BLOBOID,
        CIRCLEOID,
        MACADDROID,
        UUIDOID,
        TSVECTOROID,
        TSQUERYOID,
        POINTOID,
        LSEGOID,
        BOXOID,
        PATHOID,
        POLYGONOID,
        INT4ARRAYOID
    };

    for(uint32 i = 0; i < sizeof(supportType)/sizeof(Oid); ++i) {
        if(supportType[i] == typeOid) {
            return true;
        }
    }

    return false;
}

/* Check whether the type is in blacklist. */
bool IsTypeInBlacklist(Oid typoid)
{
    bool isblack = false;

    switch (typoid) {
#ifdef ENABLE_MULTIPLE_NODES
        case XMLOID:
#endif /* ENABLE_MULTIPLE_NODES */
        case LINEOID:
        case PGNODETREEOID:
            isblack = true;
            break;
        default:
            break;
    }

    return isblack;
}

/* Check whether the type is in installation group. */
bool IsTypeTableInInstallationGroup(const Type type_tup)
{
    if (type_tup && !IsInitdb && IS_PGXC_COORDINATOR) {
        Form_pg_type typeForm = (Form_pg_type)GETSTRUCT(type_tup);
        char* groupname = NULL;
        Oid groupoid = InvalidOid;

        if (OidIsValid(typeForm->typrelid)) {
            char relkind = get_rel_relkind(typeForm->typrelid);
            if (RELKIND_VIEW != relkind && RELKIND_CONTQUERY != relkind) {
                groupoid = ng_get_baserel_groupoid(typeForm->typrelid, relkind);
            }

            if (OidIsValid(groupoid)) {
                groupname = get_pgxc_groupname(groupoid);
            }

            char* installation_groupname = PgxcGroupGetInstallationGroup();
            if (groupname != NULL && installation_groupname != NULL && strcmp(groupname, installation_groupname) != 0) {
                return false;
            }
        }
    }
    return true;
}

char* CastPackageTypeName(const char* typName, Oid objOid, bool isPackage, bool isPublic)
{
    StringInfoData  castTypName;
    initStringInfo(&castTypName);

    /* private type name cast '$' in the beginning */
    if (isPackage) {
        if (!isPublic) {
            appendStringInfoString(&castTypName, "$");
        }
    }

    /* cast package or procedure oid */
    char* oidStr = NULL;
    const int oidStrLen = 12;
    oidStr = (char *)palloc0(oidStrLen * sizeof(char));
    pg_ltoa(objOid, oidStr);
    appendStringInfoString(&castTypName, oidStr);
    pfree_ext(oidStr);

    /* cast type name */
    appendStringInfoString(&castTypName, ".");
    appendStringInfoString(&castTypName, typName);

    return castTypName.data;
}

char* ParseTypeName(const char* typName, Oid pkgOid)
{
    if (!OidIsValid(pkgOid)) {
        return NULL;
    }
    char* oldStr = NULL;
    const int oldStrLen  =12;
    oldStr = (char*)palloc0(oldStrLen * sizeof(char));
    pg_ltoa(pkgOid, oldStr);
    int len = strlen(oldStr);
    char* pos = strstr((char*)typName, oldStr);
    pfree_ext(oldStr);
    if (NULL == pos) {
        return NULL;
    }
    pos +=len;
    if (*pos != '.') {
        return NULL;
    }
    return pstrdup(++pos);
}

/* find if %type ref a package variable type */
HeapTuple FindPkgVariableType(ParseState* pstate, const TypeName* typname, int32* typmod_p,
    TypeDependExtend* depend_extend)
{
    HeapTuple tup = NULL;

#ifdef ENABLE_MULTIPLE_NODES
    return tup;
#else
    int32 typmod = -1;
    if (!enable_plpgsql_gsdependency_guc() && u_sess->plsql_cxt.curr_compile_context == NULL) {
        return tup;
    }

    /* handle var.col%TYPE firsr */
    tup = FindRowVarColType(typname->names, NULL, NULL, typmod_p);
    if (tup != NULL) {
        return tup;
    }

    /* find package.var%TYPE second */
    if (list_length(typname->names) <= 1) {
        return tup;
    }
    if (list_length(typname->names) >= (enable_plpgsql_gsdependency_guc() ? 5 :4)) {
        return tup;
    }
    PLpgSQL_datum* datum = GetPackageDatum(typname->names);
    if (datum != NULL && datum->dtype == PLPGSQL_DTYPE_VAR) {
        Oid typOid =  ((PLpgSQL_var*)datum)->datatype->typoid;
        tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typOid));
        /* should not happen */
        if (!HeapTupleIsValid(tup)) {
            ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for type %u", typOid)));
        }
        typmod = typenameTypeMod(pstate, typname, (Type)tup);
        if (typmod_p != NULL) {
            *typmod_p = typmod;
        }
        if (enable_plpgsql_gsdependency() && NULL != depend_extend) {
            DeconstructQualifiedName(typname->names, &depend_extend->schemaName,
                                     &depend_extend->objectName, &depend_extend->packageName);
        }
    } else if (enable_plpgsql_undefined() && NULL != depend_extend) {
        Oid undefRefObjOid = gsplsql_try_build_exist_pkg_undef_var(typname->names);
        if (OidIsValid(undefRefObjOid)) {
            depend_extend->undefDependObjOid = undefRefObjOid;
            tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(UNDEFINEDOID));
            if (typmod_p != NULL) {
                *typmod_p = -1;
            }
        }
    }
    return tup;
#endif
}

static void check_record_nest_tableof_index_type(const char* typeName, List* typeNames)
{
    PLpgSQL_datum* datum = NULL;
    if (typeName != NULL) {
        PLpgSQL_nsitem* ns = NULL;
        PLpgSQL_package* pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
        ns = plpgsql_ns_lookup(pkg->public_ns, false, typeName, NULL, NULL, NULL);
        if (ns == NULL) {
            ns = plpgsql_ns_lookup(pkg->private_ns, false, typeName, NULL, NULL, NULL);
        }

        if (ns == NULL || ns->itemtype != PLPGSQL_NSTYPE_RECORD) {
            return ;
        }
        datum = pkg->datums[ns->itemno];
    } else {
        datum = GetPackageDatum(typeNames);
    }

    if (datum != NULL && datum->dtype == PLPGSQL_DTYPE_RECORD_TYPE) {
        PLpgSQL_rec_type* var_type = (PLpgSQL_rec_type*)datum;
        PLpgSQL_type* type = NULL;
        for (int i = 0; i < var_type->attrnum; i++) {
            type = var_type->types[i];
            if (type->ttype == PLPGSQL_TTYPE_SCALAR && OidIsValid(type->tableOfIndexType)) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("record nested table of index type do not support in out args")));
            }
        }
    }
}

/* find the type if it is a package type */
Oid LookupTypeInPackage(List* typeNames, const char* typeName, Oid pkgOid, Oid namespaceId)
{
    Oid typOid = InvalidOid;
    char* castTypeName = NULL;

    /* pkgOid is invalid, try to find the type in current compile package */
    if (!OidIsValid(pkgOid)) {
        if (enable_plpgsql_gsdependency_guc() &&
            u_sess->plsql_cxt.functionStyleType == FUNCTION_STYLE_TYPE_REFRESH_HEAD &&
            OidIsValid(u_sess->plsql_cxt.currRefreshPkgOid)) {
            pkgOid = u_sess->plsql_cxt.currRefreshPkgOid;
        } else {
            /* if not compiling packgae, just return invalid oid */
            if (u_sess->plsql_cxt.curr_compile_context == NULL ||
                u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package == NULL) {
                return typOid;
            }
            pkgOid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
        }
        /* find public package type first */
        castTypeName = CastPackageTypeName(typeName, pkgOid, true, true);
        typOid = TypenameGetTypidExtended(castTypeName, false);
        pfree_ext(castTypeName);
        if (!OidIsValid(typOid)) {
            /* try to find private package type */
            castTypeName = CastPackageTypeName(typeName, pkgOid, true, false);
            typOid = TypenameGetTypidExtended(castTypeName, false);
            pfree_ext(castTypeName);
        }

        if (OidIsValid(typOid)) {
            check_record_nest_tableof_index_type(typeName, NULL);
        }
        return typOid;
    }

    /* pkgOid is valid, just to find the given pkg type, public first */
    castTypeName = CastPackageTypeName(typeName, pkgOid, true, true);

    if (OidIsValid(namespaceId)) {
        typOid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(castTypeName), ObjectIdGetDatum(namespaceId));
        if (!OidIsValid(typOid)) {
            typOid = TryLookForSynonymType(castTypeName, namespaceId);
        }
    } else {
        typOid = TypenameGetTypidExtended(castTypeName, false);
    }

    pfree_ext(castTypeName);

    if (OidIsValid(typOid)) {
        bool pkgValid = true;
        if (enable_plpgsql_gsdependency_guc()) {
            pkgValid = GetPgObjectValid(pkgOid, OBJECT_TYPE_PKGSPEC);
        } 
        if (pkgValid) {
            // check_record_nest_tableof_index_type(NULL, typeNames);
        }
        return typOid;
    }

    /*
     * find private pkg type, if compile package is the same pkg
     * if not compiling packgae, just return invalid oid
     */
    if (u_sess->plsql_cxt.curr_compile_context == NULL ||
        u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package == NULL) {
        return typOid;
    }

    /* not same package, return */
    if (pkgOid != u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid) {
        return typOid;
    }

    castTypeName = CastPackageTypeName(typeName, pkgOid, true, false);

    if (OidIsValid(namespaceId)) {
        typOid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(castTypeName), ObjectIdGetDatum(namespaceId));
        if (!OidIsValid(typOid)) {
            typOid = TryLookForSynonymType(castTypeName, namespaceId);
        }
    } else {
        typOid = TypenameGetTypidExtended(castTypeName, false);
    }

    pfree_ext(castTypeName);

    return typOid;


}

bool IsBinaryType(Oid typid)
{
    return ((typid) == BLOBOID ||
            (typid) == BYTEAOID);
}

void check_type_supports_multi_charset(Oid typid, bool allow_array)
{
    switch (typid) {
        case XMLOID:
        case JSONOID:
        case TSVECTOROID:
        case GTSVECTOROID:
        case TSQUERYOID:
        case RECORDOID:
        case HLL_OID:
        case HLL_HASHVAL_OID:
        case HLL_TRANS_OID:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("multi character set for datatype '%s' is not supported", get_typename(typid))));
        default:
            break;
    }

    if (!allow_array && type_is_array(typid)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("multi character set for datatype '%s' is not supported", get_typename(typid))));
    }
}

TypeTupStatus GetTypeTupStatus(Type typ)
{
    if (HeapTupleIsValid(typ)) {
        return (UNDEFINEDOID == HeapTupleGetOid(typ) ? UndefineTypeTup : NormalTypeTup);
    }
    return InvalidTypeTup;
}