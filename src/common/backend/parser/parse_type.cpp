/* -------------------------------------------------------------------------
 *
 * parse_type.cpp
 *		handle type operations for parser
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

const static Oid cstoreSupportType[] = {BOOLOID,
    HLL_OID, // same as BYTEA
    BYTEAOID,
    CHAROID,
    HLL_HASHVAL_OID, // same as INT8
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
    CLOBOID,
    BOOLARRAYOID, // array
    HLL_ARRAYOID,
    BYTEARRAYOID,
    CHARARRAYOID,
    HLL_HASHVAL_ARRAYOID,
    INT8ARRAYOID,
    INT2ARRAYOID,
    INT4ARRAYOID,
    INT1ARRAYOID,
    ARRAYNUMERICOID,
    BPCHARARRAYOID,
    VARCHARARRAYOID,
    NVARCHAR2ARRAYOID,
    SMALLDATETIMEARRAYOID,
    TEXTARRAYOID,
    FLOAT4ARRAYOID,
    FLOAT8ARRAYOID,
    ABSTIMEARRAYOID,
    RELTIMEARRAYOID,
    ARRAYTINTERVALOID,
    INETARRAYOID,
    DATEARRAYOID,
    TIMEARRAYOID,
    TIMESTAMPARRAYOID,
    TIMESTAMPTZARRAYOID,
    ARRAYINTERVALOID,
    ARRAYTIMETZOID,
    CASHARRAYOID,
    CIDRARRAYOID,
    BITARRAYOID,
    VARBITARRAYOID,
    BYTEAWITHOUTORDERCOLOID,
    BYTEAWITHOUTORDERWITHEQUALCOLOID
};

static int32 typenameTypeMod(ParseState* pstate, const TypeName* typname, Type typ);

static bool IsTypeInBlacklist(Oid typoid);

/*
 * LookupTypeName
 *       Wrapper for typical case.
 */
Type LookupTypeName(ParseState *pstate, const TypeName *typeName, int32 *typmod_p, bool print_notice)
{
       return LookupTypeNameExtended(pstate, typeName, typmod_p, true, print_notice);
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
                            bool print_notice)
{
    Oid typoid;
    HeapTuple tup;
    int32 typmod;

    if (typname->names == NIL) {
        /* We have the OID already if it's an internally generated TypeName */
        typoid = typname->typeOid;
    } else if (typname->pct_type) {
        /* Handle %TYPE reference to type of an existing field */
        RangeVar* rel = makeRangeVar(NULL, NULL, typname->location);
        char* field = NULL;
        Oid relid;
        AttrNumber attnum;

        /* deconstruct the name list */
        switch (list_length(typname->names)) {
            case 1:
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg(
                            "improper %%TYPE reference (too few dotted names): %s", NameListToString(typname->names)),
                        parser_errposition(pstate, typname->location)));
                break;
            case 2:
                rel->relname = strVal(linitial(typname->names));
                field = strVal(lsecond(typname->names));
                break;
            case 3:
                rel->schemaname = strVal(linitial(typname->names));
                rel->relname = strVal(lsecond(typname->names));
                field = strVal(lthird(typname->names));
                break;
            case 4:
                rel->catalogname = strVal(linitial(typname->names));
                rel->schemaname = strVal(lsecond(typname->names));
                rel->relname = strVal(lthird(typname->names));
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
        relid = RangeVarGetRelid(rel, NoLock, false);
        attnum = get_attnum(relid, field);
        if (attnum == InvalidAttrNumber) {
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("column \"%s\" of relation \"%s\" does not exist", field, rel->relname),
                    parser_errposition(pstate, typname->location)));
        }
        typoid = get_atttype(relid, attnum);

        /* this construct should never have an array indicator */
        Assert(typname->arrayBounds == NIL);

        /* emit nuisance notice (intentionally not errposition'd) */
        if (print_notice) {
            ereport(NOTICE,
                (errmsg("type reference %s converted to %s", TypeNameToString(typname), format_type_be(typoid))));
        }
    } else {
        /* Normal reference to a type name */
        char* schemaname = NULL;
        char* typeName = NULL;

        /* deconstruct the name list */
        DeconstructQualifiedName(typname->names, &schemaname, &typeName);

        if (schemaname != NULL) {
            /* Look in specific schema only */
            Oid namespaceId;

            namespaceId = LookupExplicitNamespace(schemaname);
            typoid = GetSysCacheOid2(TYPENAMENSP, PointerGetDatum(typeName), ObjectIdGetDatum(namespaceId));
        } else {
            /* Unqualified type name, so search the search path */
            typoid = TypenameGetTypidExtended(typeName, temp_ok);
        }

        /* If an array reference, return the array type instead */
        if (typname->arrayBounds != NIL) {
            typoid = get_array_type(typoid);
        }
    }

    if (!OidIsValid(typoid)) {
        if (typmod_p != NULL) {
            *typmod_p = -1;
        }
        return NULL;
    }

    /* Don't support the type in blacklist. */
    if (!u_sess->attr.attr_common.IsInplaceUpgrade && IsTypeInBlacklist(typoid)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("type %s is not yet supported.", format_type_be(typoid))));
    }

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));

    /* should not happen */
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typoid)));
    }

    typmod = typenameTypeMod(pstate, typname, (Type)tup);

    if (typmod_p != NULL) {
        *typmod_p = typmod;
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
Type typenameType(ParseState* pstate, const TypeName* typname, int32* typmod_p)
{
    Type tup;

    tup = LookupTypeName(pstate, typname, typmod_p);

    /*
     * If the type is relation, then we check
     * whether the table is in installation group
     */
    if (!in_logic_cluster() && !IsTypeTableInInstallationGroup(tup)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("type '%s' must be in installation group", TypeNameToString(typname))));
    }

    if (tup == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("type \"%s\" does not exist", TypeNameToString(typname)),
                parser_errposition(pstate, typname->location)));
    }
        
    if (!((Form_pg_type)GETSTRUCT(tup))->typisdefined) {
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
void typenameTypeIdAndMod(ParseState* pstate, const TypeName* typname, Oid* typeid_p, int32* typmod_p)
{
    Type tup;

    tup = typenameType(pstate, typname, typmod_p);
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

/*
 * GetColumnDefCollation
 *
 * Get the collation to be used for a column being defined, given the
 * ColumnDef node and the previously-determined column type OID.
 *
 * pstate is only used for error location purposes, and can be NULL.
 */
Oid GetColumnDefCollation(ParseState* pstate, ColumnDef* coldef, Oid typeOid)
{
    Oid result;
    Oid typcollation = get_typcollation(typeOid);
    int location = -1;

    if (coldef->collClause) {
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

    /* Complain if COLLATE is applied to an uncollatable type */
    if (OidIsValid(result) && !OidIsValid(typcollation)) {
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
 */
Datum stringTypeDatum(Type tp, char* string, int32 atttypmod)
{
    Form_pg_type typform = (Form_pg_type)GETSTRUCT(tp);
    Oid typinput = typform->typinput;
    Oid typioparam = getTypeIOParam(tp);
    Datum result;

    result = OidInputFunctionCall(typinput, string, typioparam, atttypmod);

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
void parseTypeString(const char* str, Oid* typeid_p, int32* typmod_p)
{
    StringInfoData buf;
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

    typenameTypeIdAndMod(NULL, typname, typeid_p, typmod_p);

    pfree_ext(buf.data);

    return;

fail:
    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid type name \"%s\"", str)));
}

/*
 * IsTypeSupportedByCStore
 *      Return true if the type is supported by column store
 *
 * The performance of this function relies on compiler to flat the branches. But
 * it is ok if compiler failed to do its job as it is not in critical code path.
 */
bool IsTypeSupportedByCStore(_in_ Oid typeOid, _in_ int32 typeMod)
{
    // we don't support user defined type.
    //
    if (typeOid >= FirstNormalObjectId) {
        return false;
    }

    for (uint32 i = 0; i < sizeof(cstoreSupportType) / sizeof(Oid); ++i) {
        if (cstoreSupportType[i] == typeOid) {
            return true;
		}
    }
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
