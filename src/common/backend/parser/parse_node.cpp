/* -------------------------------------------------------------------------
 *
 * parse_node.cpp
 *	  various routines that make nodes for querytrees
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/parser/parse_node.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "parser/parser.h" // Needed for TSQL_HEX_CONST_TYPMOD
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/varbit.h"

static void pcb_error_callback(void* arg);
static Oid lookup_varbinary_oid();
static Oid lookup_varbinaryin_funcoid();

static Oid varbinary_oid = InvalidOid;
static PGFunction varbinaryin_funcaddr = NULL;

/*
 * make_parsestate
 *		Allocate and initialize a new ParseState.
 *
 * Caller should eventually release the ParseState via free_parsestate().
 */
ParseState* make_parsestate(ParseState* parentParseState)
{
    ParseState* pstate = NULL;

    pstate = (ParseState*)palloc0(sizeof(ParseState));

    pstate->parentParseState = parentParseState;

    pstate->isAliasReplace = true;

    /* Fill in fields that don't start at null/false/zero */
    pstate->p_next_resno = 1;
    pstate->p_star_start = NIL;
    pstate->p_star_end = NIL;
    pstate->p_star_only = NIL;
    pstate->p_resolve_unknowns = true;
    pstate->ignoreplus = false;
    pstate->p_plusjoin_rte_info = NULL;
    pstate->p_rawdefaultlist = NIL;
    pstate->p_has_ignore = false;
    pstate->p_indexhintLists = NIL;
    pstate->p_is_flt_frame = false;

    if (parentParseState != NULL) {
        pstate->p_sourcetext = parentParseState->p_sourcetext;
        /* all hooks are copied from parent */
        pstate->p_pre_columnref_hook = parentParseState->p_pre_columnref_hook;
        pstate->p_post_columnref_hook = parentParseState->p_post_columnref_hook;
        pstate->p_paramref_hook = parentParseState->p_paramref_hook;
        pstate->p_coerce_param_hook = parentParseState->p_coerce_param_hook;
        pstate->p_ref_hook_state = parentParseState->p_ref_hook_state;
        pstate->p_create_proc_operator_hook = parentParseState->p_create_proc_operator_hook;
        pstate->p_create_proc_insert_hook = parentParseState->p_create_proc_insert_hook;
        pstate->p_cl_hook_state = parentParseState->p_cl_hook_state;
        pstate->p_bind_variable_columnref_hook = parentParseState->p_bind_variable_columnref_hook;
        pstate->p_bind_hook_state = parentParseState->p_bind_hook_state;
        pstate->p_bind_describe_hook = parentParseState->p_bind_describe_hook;
        pstate->p_describeco_hook_state = parentParseState->p_describeco_hook_state;
        pstate->p_has_ignore = parentParseState->p_has_ignore;
        pstate->transform_outer_columnref_as_param_hook = parentParseState->transform_outer_columnref_as_param_hook;
        pstate->has_rotate = parentParseState->has_rotate;
    }

    return pstate;
}

/*
 * free_parsestate
 *		Release a ParseState and any subsidiary resources.
 */
void free_parsestate(ParseState* pstate)
{
    Assert(pstate != NULL);
    /*
     * Check that we did not produce too many resnos; at the very least we
     * cannot allow more than 2^16, since that would exceed the range of a
     * AttrNumber. It seems safest to use MaxTupleAttributeNumber.
     */
    if (pstate->p_next_resno - 1 > MaxTupleAttributeNumber) {
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("target lists can have at most %d entries", MaxTupleAttributeNumber)));
    }
    foreach_cell (l, pstate->p_target_relation) {
        Relation r = (Relation)lfirst(l);
        heap_close(r, NoLock);
    }
    pfree_ext(pstate);
}

/*
 * parser_errposition
 *		Report a parse-analysis-time cursor position, if possible.
 *
 * This is expected to be used within an ereport() call.  The return value
 * is a dummy (always 0, in fact).
 *
 * The locations stored in raw parsetrees are byte offsets into the source
 * string.	We have to convert them to 1-based character indexes for reporting
 * to clients.	(We do things this way to avoid unnecessary overhead in the
 * normal non-error case: computing character indexes would be much more
 * expensive than storing token offsets.)
 */
int parser_errposition(ParseState* pstate, int location)
{
    int pos;

    /* No-op if location was not provided */
    if (location < 0) {
        return 0;
    }
    /* Can't do anything if source text is not available */
    if (pstate == NULL || pstate->p_sourcetext == NULL) {
        return 0;
    }
    /* Convert offset to character number */
    pos = pg_mbstrlen_with_len(pstate->p_sourcetext, location) + 1;
    /* And pass it to the ereport mechanism */
    return errposition(pos);
}

/*
 * setup_parser_errposition_callback
 *		Arrange for non-parser errors to report an error position
 *
 * Sometimes the parser calls functions that aren't part of the parser
 * subsystem and can't reasonably be passed a ParseState; yet we would
 * like any errors thrown in those functions to be tagged with a parse
 * error location.	Use this function to set up an error context stack
 * entry that will accomplish that.  Usage pattern:
 *
 *		declare a local variable "ParseCallbackState pcbstate"
 *		...
 *		setup_parser_errposition_callback(&pcbstate, pstate, location);
 *		call function that might throw error;
 *		cancel_parser_errposition_callback(&pcbstate);
 */
void setup_parser_errposition_callback(ParseCallbackState* pcbstate, ParseState* pstate, int location)
{
    /* Setup error traceback support for ereport() */
    pcbstate->pstate = pstate;
    pcbstate->location = location;
    pcbstate->errcontext.callback = pcb_error_callback;
    pcbstate->errcontext.arg = (void*)pcbstate;
    pcbstate->errcontext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &pcbstate->errcontext;
}

/*
 * Cancel a previously-set-up errposition callback.
 */
void cancel_parser_errposition_callback(ParseCallbackState* pcbstate)
{
    /* Pop the error context stack */
    t_thrd.log_cxt.error_context_stack = pcbstate->errcontext.previous;
}

/*
 * Error context callback for inserting parser error location.
 *
 * Note that this will be called for *any* error occurring while the
 * callback is installed.  We avoid inserting an irrelevant error location
 * if the error is a query cancel --- are there any other important cases?
 */
static void pcb_error_callback(void* arg)
{
    ParseCallbackState* pcbstate = (ParseCallbackState*)arg;

    if (geterrcode() != ERRCODE_QUERY_CANCELED) {
        (void)parser_errposition(pcbstate->pstate, pcbstate->location);
    }
}

/*
 * Get Oid of sys.varbinary from pg_catalog.pg_type.
 * Return InvalidOid if it doesn't exist.
 */
static Oid lookup_varbinary_oid()
{
    int rc;
    bool snapshot_set;
    char *query;
    TupleDesc tupdesc;
    HeapTuple row;
    bool isnull;
    Oid varbinary_oid;

    /*
     * Some statement type (i.e. CallStmt) does not captrue the active snapshot.
     * (please see (analyze_requires_snapshot().) It may cause a crash while
     * excuting a varbinary oid lookup query internally via SPI_execute().
     * If there is no active snapshot, captrue it.
     */
    snapshot_set = false;
    if (!ActiveSnapshotSet()) {
        PushActiveSnapshot(GetTransactionSnapshot());
        snapshot_set = true;
    }

    /* Connect to the SPI manager */
    if ((rc = SPI_connect()) < 0)
        elog(ERROR, "SPI_connect() failed in Parse Analyzer "
                    "with return code %d", rc);

    query = "SELECT T.oid FROM pg_catalog.pg_type T "
            "JOIN pg_catalog.pg_namespace N ON N.oid = T.typnamespace "
            "WHERE N.nspname = 'sys' AND T.typname = 'varbinary'";
    rc = SPI_execute(query, true, 0);
    if (rc != SPI_OK_SELECT)
        elog(ERROR, "SPI_execute() failed in Parse Analyzer "
                    "with return code %d", rc);

    Assert(SPI_processed <= 1);
    if (SPI_processed == 1) {
        tupdesc = SPI_tuptable->tupdesc;
        row = SPI_tuptable->vals[0];
        varbinary_oid = DatumGetObjectId(SPI_getbinval(row, tupdesc, 1, &isnull));
    } else {
        /* sys.varbinary does not exist in pg_type catalog */
        varbinary_oid = InvalidOid;
    }

    /* Cleanup and done */
    SPI_finish();

    if (snapshot_set) {
        PopActiveSnapshot();
    }

    return varbinary_oid;
}

/*
 * Get Oid of sys.varbinary from pg_catalog.pg_type.
 * Return InvalidOid if it doesn't exist.
 */
static Oid lookup_varbinaryin_funcoid()
{
    int rc;
    bool snapshot_set;
    char *query;
    TupleDesc tupdesc;
    HeapTuple row;
    bool isnull;
    Oid func_oid;

    /*
     * Some statement type (i.e. CallStmt) does not captrue the active snapshot.
     * (please see (analyze_requires_snapshot().) It may cause a crash while
     * excuting a varbinary oid lookup query internally via SPI_execute().
     * If there is no active snapshot, captrue it.
     */
    snapshot_set = false;
    if (!ActiveSnapshotSet()) {
        PushActiveSnapshot(GetTransactionSnapshot());
        snapshot_set = true;
    }

    /* Connect to the SPI manager */
    if ((rc = SPI_connect()) < 0)
        elog(ERROR, "SPI_connect() failed in Parse Analyzer "
                    "with return code %d", rc);

    query = "SELECT P.oid  FROM pg_catalog.pg_proc P "
            "JOIN pg_catalog.pg_namespace N ON N.oid = P.pronamespace "
            "WHERE N.nspname = 'sys' AND P.proname = 'varbinaryin'";
    rc = SPI_execute(query, true, 0);
    if (rc != SPI_OK_SELECT)
        elog(ERROR, "SPI_execute() failed in Parse Analyzer "
                    "with return code %d", rc);

    Assert(SPI_processed <= 1);
    if (SPI_processed == 1) {
        tupdesc = SPI_tuptable->tupdesc;
        row = SPI_tuptable->vals[0];
        func_oid = DatumGetObjectId(SPI_getbinval(row, tupdesc, 1, &isnull));
    } else {
        /* sys.varbinary does not exist in pg_type catalog */
        func_oid = InvalidOid;
    }
    /* Cleanup and done */
    SPI_finish();

    if (snapshot_set) {
        PopActiveSnapshot();
    }

    return func_oid;
}

/* 
 * For timeseries table to identify hidden column type and return NULL or
 * build a Var node for an attribute identified by RTE and attrno for others.
 */
Var* ts_make_var(ParseState* pstate, RangeTblEntry* rte, int attrno, int location)
{
    Oid var_type_id;
    int32 type_mod;
    Oid var_collid;
    int kv_type = ATT_KV_UNDEFINED;
    get_rte_attribute_type(rte, attrno, &var_type_id, &type_mod, &var_collid, &kv_type);
    if (kv_type == ATT_KV_HIDE) {
        return NULL;
    }

    Var* result = NULL;
    int vnum, sublevels_up;
    vnum = RTERangeTablePosn(pstate, rte, &sublevels_up);
    result = makeVar(vnum, attrno, var_type_id, type_mod, var_collid, sublevels_up);
    result->location = location;
    return result;
}

/*
 * make_var
 *		Build a Var node for an attribute identified by RTE and attrno
 */
Var* make_var(ParseState* pstate, RangeTblEntry* rte, int attrno, int location)
{
    Var* result = NULL;
    int vnum, sublevels_up;
    Oid vartypeid;
    int32 type_mod;
    Oid varcollid;

    vnum = RTERangeTablePosn(pstate, rte, &sublevels_up);
    get_rte_attribute_type(rte, attrno, &vartypeid, &type_mod, &varcollid);
    result = makeVar(vnum, attrno, vartypeid, type_mod, varcollid, sublevels_up);
    result->location = location;
    return result;
}

/*
 * transformArrayType()
 *		Identify the types involved in a subscripting operation
 *
 * On entry, arrayType/arrayTypmod identify the type of the input value
 * to be subscripted (which could be a domain type).  These are modified
 * if necessary to identify the actual array type and typmod, and the
 * array's element type is returned.  An error is thrown if the input isn't
 * an array type.
 */
Oid transformArrayType(Oid* arrayType, int32* arrayTypmod)
{
    Oid origArrayType = *arrayType;
    Oid elementType;
    HeapTuple type_tuple_array;
    Form_pg_type type_struct_array;

    /*
     * If the input is a domain, smash to base type, and extract the actual
     * typmod to be applied to the base type.  Subscripting a domain is an
     * operation that necessarily works on the base array type, not the domain
     * itself.	(Note that we provide no method whereby the creator of a
     * domain over an array type could hide its ability to be subscripted.)
     */
    *arrayType = getBaseTypeAndTypmod(*arrayType, arrayTypmod);

    /* Get the type tuple for the array */
    type_tuple_array = SearchSysCache1(TYPEOID, ObjectIdGetDatum(*arrayType));
    if (!HeapTupleIsValid(type_tuple_array)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", *arrayType)));
    }
    type_struct_array = (Form_pg_type)GETSTRUCT(type_tuple_array);

    /* needn't check typisdefined since this will fail anyway */

    elementType = type_struct_array->typelem;
    if (elementType == InvalidOid) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("cannot subscript type %s because it is not an array", format_type_be(origArrayType))));
    }
    ReleaseSysCache(type_tuple_array);

    return elementType;
}

/*
 * transformArraySubscripts()
 *		Transform array subscripting.  This is used for both
 *		array fetch and array assignment.
 *
 * In an array fetch, we are given a source array value and we produce an
 * expression that represents the result of extracting a single array element
 * or an array slice.
 *
 * In an array assignment, we are given a destination array value plus a
 * source value that is to be assigned to a single element or a slice of
 * that array.	We produce an expression that represents the new array value
 * with the source data inserted into the right part of the array.
 *
 * For both cases, if the source array is of a domain-over-array type,
 * the result is of the base array type or its element type; essentially,
 * we must fold a domain to its base type before applying subscripting.
 *
 * pstate		Parse state
 * arrayBase	Already-transformed expression for the array as a whole
 * arrayType	OID of array's datatype (should match type of arrayBase,
 *				or be the base type of arrayBase's domain type)
 * elementType	OID of array's element type (fetch with transformArrayType,
 *				or pass InvalidOid to do it here)
 * arrayTypMod	typmod for the array (which is also typmod for the elements)
 * indirection	Untransformed list of subscripts (must not be NIL)
 * assignFrom	NULL for array fetch, else transformed expression for source.
 */
ArrayRef* transformArraySubscripts(ParseState* pstate, Node* arrayBase, Oid arrayType, Oid elementType,
    int32 arrayTypMod, List* indirection, Node* assignFrom)
{
    bool isSlice = false;
    List* upperIndexpr = NIL;
    List* lowerIndexpr = NIL;
    ListCell* idx = NULL;
    ArrayRef* aref = NULL;
    bool isIndexByVarchar = false;

    /*
     * Caller may or may not have bothered to determine elementType.  Note
     * that if the caller did do so, arrayType/arrayTypMod must be as modified
     * by transformArrayType, ie, smash domain to base type.
     */
    if (!OidIsValid(elementType)) {
        elementType = transformArrayType(&arrayType, &arrayTypMod);
    }
    /*
     * A list containing only single subscripts refers to a single array
     * element.  If any of the items are double subscripts (lower:upper), then
     * the subscript expression means an array slice operation. In this case,
     * we supply a default lower bound of 1 for any items that contain only a
     * single subscript.  We have to prescan the indirection list to see if
     * there are any double subscripts.
     */
    foreach (idx, indirection) {
        A_Indices* ai = (A_Indices*)lfirst(idx);

        if (ai->lidx != NULL) {
            isSlice = true;
            break;
        }
    }

    /*
     * Transform the subscript expressions.
     */
    int i = 0;
    foreach (idx, indirection) {
        A_Indices* ai = (A_Indices*)lfirst(idx);
        Node* subexpr = NULL;

        AssertEreport(IsA(ai, A_Indices), MOD_OPT, "");
        if (isSlice) {
            if (ai->lidx) {
                subexpr = transformExpr(pstate, ai->lidx, pstate->p_expr_kind);
                /* If it's not int4 already, try to coerce */
                subexpr = coerce_to_target_type(
                    pstate, subexpr, exprType(subexpr), INT4OID, -1, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST,
                    NULL, NULL, -1);
                if (subexpr == NULL) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("array subscript must have type integer"),
                            parser_errposition(pstate, exprLocation(ai->lidx))));
                }
            } else {
                /* Make a constant 1 */
                subexpr = (Node*)makeConst(
                    INT4OID, -1, InvalidOid, sizeof(int32), Int32GetDatum(1), false, true); /* pass by value */
            }
            lowerIndexpr = lappend(lowerIndexpr, subexpr);
        }
        subexpr = transformExpr(pstate, ai->uidx, pstate->p_expr_kind);
        if (get_typecategory(arrayType) == TYPCATEGORY_TABLEOF_VARCHAR) {
            isIndexByVarchar = true;
        }
        if ((nodeTag(arrayBase) == T_Param && list_length(((Param*)arrayBase)->tableOfIndexTypeList) > i
             && list_nth_oid(((Param*)arrayBase)->tableOfIndexTypeList, i) == VARCHAROID)
            || isIndexByVarchar) {
            /* subcript type is varchar */
            subexpr = coerce_to_target_type(pstate, subexpr, exprType(subexpr),
                                            list_nth_oid(((Param*)arrayBase)->tableOfIndexTypeList, i),
                                            -1, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, NULL, NULL, -1);
        } else {
             /* If it's not int4 already, try to coerce */
            subexpr = coerce_to_target_type(
                pstate, subexpr, exprType(subexpr), INT4OID, -1, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST,
                NULL, NULL, -1);
        }
       
        if (subexpr == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("array subscript must have type integer"),
                    parser_errposition(pstate, exprLocation(ai->uidx))));
        }
        upperIndexpr = lappend(upperIndexpr, subexpr);
        i++;
    }

    /*
     * If doing an array store, coerce the source value to the right type.
     * (This should agree with the coercion done by transformAssignedExpr.)
     */
    if (assignFrom != NULL) {
        Oid typesource = exprType(assignFrom);
        Oid typeneeded = isSlice ? arrayType : elementType;
        Node* newFrom = NULL;

        newFrom = coerce_to_target_type(
            pstate, assignFrom, typesource, typeneeded, arrayTypMod, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST,
            NULL, NULL, -1);
        if (newFrom == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("array assignment requires type %s"
                           " but expression is of type %s",
                        format_type_be(typeneeded),
                        format_type_be(typesource)),
                    errhint("You will need to rewrite or cast the expression."),
                    parser_errposition(pstate, exprLocation(assignFrom))));
        }
        assignFrom = newFrom;
    }

    /*
     * Ready to build the ArrayRef node.
     */
    aref = makeNode(ArrayRef);
    aref->refarraytype = arrayType;
    aref->refelemtype = elementType;
    aref->reftypmod = arrayTypMod;
    /* refcollid will be set by parse_collate.c */
    aref->refupperindexpr = upperIndexpr;
    aref->reflowerindexpr = lowerIndexpr;
    aref->refexpr = (Expr*)arrayBase;
    aref->refassgnexpr = (Expr*)assignFrom;

    return aref;
}

/*
 * make_const
 *
 *	Convert a Value node (as returned by the grammar) to a Const node
 *	of the "natural" type for the constant.  Note that this routine is
 *	only used when there is no explicit cast for the constant, so we
 *	have to guess what type is wanted.
 *
 *	For string literals we produce a constant of type UNKNOWN ---- whose
 *	representation is the same as cstring, but it indicates to later type
 *	resolution that we're not sure yet what type it should be considered.
 *	Explicit "NULL" constants are also typed as UNKNOWN.
 *
 *	For integers and floats we produce int4, int8, or numeric depending
 *	on the value of the number.  XXX We should produce int2 as well,
 *	but additional cleanup is needed before we can do that; there are
 *	too many examples that fail if we try.
 */
Const* make_const(ParseState* pstate, Value* value, int location)
{
    Const* con = NULL;
    Datum val;
    int64 val64;
    Oid typid;
    Oid collid = InvalidOid;
    int typelen;
    bool typebyval = false;
    ParseCallbackState pcbstate;

    switch (nodeTag(value)) {
        case T_Integer:
            val = Int32GetDatum(intVal(value));

            typid = INT4OID;
            typelen = sizeof(int32);
            typebyval = true;
            break;

        case T_Float:
            /* could be an oversize integer as well as a float ... */
            if (scanint8(strVal(value), true, &val64)) {
                /*
                 * It might actually fit in int32. Probably only INT_MIN can
                 * occur, but we'll code the test generally just to be sure.
                 */
                int32 val32 = (int32)val64;

                if (val64 == (int64)val32) {
                    val = Int32GetDatum(val32);

                    typid = INT4OID;
                    typelen = sizeof(int32);
                    typebyval = true;
                } else {
                    val = Int64GetDatum(val64);

                    typid = INT8OID;
                    typelen = sizeof(int64);
                    typebyval = FLOAT8PASSBYVAL; /* int8 and float8 alike */
                }
            } else {
                /* arrange to report location if numeric_in() fails */
                setup_parser_errposition_callback(&pcbstate, pstate, location);
                val = DirectFunctionCall3(
                    numeric_in, CStringGetDatum(strVal(value)), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
                cancel_parser_errposition_callback(&pcbstate);

                typid = NUMERICOID;
                typelen = -1; /* variable len */
                typebyval = false;
            }
            break;

        case T_String:

            /*
             * We assume here that UNKNOWN's internal representation is the
             * same as CSTRING
             */
            val = CStringGetDatum(strVal(value));

            typid = UNKNOWNOID; /* will be coerced later */
            typelen = -2;       /* cstring-style varwidth type */
            typebyval = false;
            if (OidIsValid(GetCollationConnection())) {
                collid = GetCollationConnection();
            }
            break;

        case T_BitString:
            /* arrange to report location if bit_in() fails */
            setup_parser_errposition_callback(&pcbstate, pstate, location);
            val = DirectFunctionCall3(
                bit_in, CStringGetDatum(strVal(value)), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
            cancel_parser_errposition_callback(&pcbstate);
            typid = BITOID;
            typelen = -1;
            typebyval = false;
            break;
        /* Unquoted hex input such as 0x1F, process it as type sys.VARBINARY */
        case T_TSQL_HexString:
            /* Lookup Oid of type sys.varbinary and the input function sys.varbinaryin */
            if (varbinary_oid == InvalidOid) {
                if ((varbinary_oid = lookup_varbinary_oid()) != InvalidOid) {
                    Oid varbinaryin_funcoid = lookup_varbinaryin_funcoid();
                    varbinaryin_funcaddr =
                            lookup_C_func_by_oid(varbinaryin_funcoid, "$libdir/shark", "varbinaryin");
                }
            }

            if (varbinary_oid == InvalidOid || varbinaryin_funcaddr == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                        errmsg("Type VARBINARY is not supported for input %s.", value->val.str),
                        errhint("Install shark extension to support Type VARBINARY")));
            }

            /* arrange to report location if the input function fails */
            setup_parser_errposition_callback(&pcbstate, pstate, location);
            val = DirectFunctionCall3(varbinaryin_funcaddr,
                                      CStringGetDatum(value->val.str),
                                      ObjectIdGetDatum(InvalidOid),
                                      Int32GetDatum(TSQL_HEX_CONST_TYPMOD)
                                      );
            cancel_parser_errposition_callback(&pcbstate);
            typid = varbinary_oid;
            typelen = -1;
            typebyval = false;
            break;

        case T_Null:
            /* return a null const */
            con = makeConst(UNKNOWNOID, -1, InvalidOid, -2, (Datum)0, true, false);
            con->location = location;
            return con;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)nodeTag(value))));
            return NULL; /* keep compiler quiet */
    }

    con = makeConst(typid,
        -1,         /* typmod -1 is OK for all cases */
        collid, /* all cases are uncollatable types */
        typelen,
        val,
        false,
        typebyval);
    con->location = location;

    return con;
}
