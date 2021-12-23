/* -------------------------------------------------------------------------
 *
 * makefuncs.cpp
 *	  creator functions for primitive nodes. The functions here are for
 *	  the most frequently created nodes.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/nodes/makefuncs.cpp
 *
 * -------------------------------------------------------------------------
 */
#ifndef FRONTEND_PARSER
#include "postgres.h"
#include "knl/knl_variable.h"
#else
#include "postgres_fe.h"
#endif /* FRONTEND_PARSER */

#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "storage/item/itemptr.h"

#ifndef FRONTEND_PARSER
#include "utils/lsyscache.h"
#endif /* FRONTEND_PARSER */

extern TypeName* SystemTypeName(char* name);

/*
 * makeA_Expr -
 *		makes an A_Expr node
 */
A_Expr* makeA_Expr(A_Expr_Kind kind, List* name, Node* lexpr, Node* rexpr, int location)
{
    A_Expr* a = makeNode(A_Expr);

    a->kind = kind;
    a->name = name;
    a->lexpr = lexpr;
    a->rexpr = rexpr;
    a->location = location;
    return a;
}

/*
 * makeSimpleA_Expr -
 *		As above, given a simple (unqualified) operator name
 */
A_Expr* makeSimpleA_Expr(A_Expr_Kind kind, char* name, Node* lexpr, Node* rexpr, int location)
{
    A_Expr* a = makeNode(A_Expr);

    a->kind = kind;
    a->name = list_make1(makeString((char*)name));
    a->lexpr = lexpr;
    a->rexpr = rexpr;
    a->location = location;
    return a;
}

/*
 * makeVar -
 *	  creates a Var node
 */
Var* makeVar(Index varno, AttrNumber varattno, Oid vartype, int32 vartypmod, Oid varcollid, Index varlevelsup)
{
    Var* var = makeNode(Var);

    var->varno = varno;
    var->varattno = varattno;
    var->vartype = vartype;
    var->vartypmod = vartypmod;
    var->varcollid = varcollid;
    var->varlevelsup = varlevelsup;

    /*
     * Since few if any routines ever create Var nodes with varnoold/varoattno
     * different from varno/varattno, we don't provide separate arguments for
     * them, but just initialize them to the given varno/varattno. This
     * reduces code clutter and chance of error for most callers.
     */
    var->varnoold = varno;
    var->varoattno = varattno;

    /* Likewise, we just set location to "unknown" here */
    var->location = -1;

    return var;
}


#ifndef FRONTEND_PARSER
/*
 * makeVarFromTargetEntry -
 *		convenience function to create a same-level Var node from a
 *		TargetEntry
 */
Var* makeVarFromTargetEntry(Index varno, TargetEntry* tle)
{
    return makeVar(varno,
        tle->resno,
        exprType((Node*)tle->expr),
        exprTypmod((Node*)tle->expr),
        exprCollation((Node*)tle->expr),
        0);
}

/*
 * makeWholeRowVar -
 *	  creates a Var node representing a whole row of the specified RTE
 *
 * A whole-row reference is a Var with varno set to the correct range
 * table entry, and varattno == 0 to signal that it references the whole
 * tuple.  (Use of zero here is unclean, since it could easily be confused
 * with error cases, but it's not worth changing now.)  The vartype indicates
 * a rowtype; either a named composite type, or RECORD.  This function
 * encapsulates the logic for determining the correct rowtype OID to use.
 *
 * If allowScalar is true, then for the case where the RTE is a function
 * returning a non-composite result type, we produce a normal Var referencing
 * the function's result directly, instead of the single-column composite
 * value that the whole-row notation might otherwise suggest.
 */
Var* makeWholeRowVar(RangeTblEntry* rte, Index varno, Index varlevelsup, bool allowScalar)
{
    Var* result = NULL;
    Oid toid;

    switch (rte->rtekind) {
        case RTE_RELATION:
            /* relation: the rowtype is a named composite type */
            toid = get_rel_type_id(rte->relid);
            if (!OidIsValid(toid)) {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("could not find type OID for relation %u", rte->relid)));
            }
            result = makeVar(varno, InvalidAttrNumber, toid, -1, InvalidOid, varlevelsup);
            break;
        case RTE_FUNCTION:
            toid = exprType(rte->funcexpr);
            if (type_is_rowtype(toid)) {
                /* func returns composite; same as relation case */
                result = makeVar(varno, InvalidAttrNumber, toid, -1, InvalidOid, varlevelsup);
            } else if (allowScalar) {
                /* func returns scalar; just return its output as-is */
                result = makeVar(varno, 1, toid, -1, exprCollation(rte->funcexpr), varlevelsup);
            } else {
                /* func returns scalar, but we want a composite result */
                result = makeVar(varno, InvalidAttrNumber, RECORDOID, -1, InvalidOid, varlevelsup);
            }
            break;
#ifdef PGXC
        case RTE_REMOTE_DUMMY:
            result = NULL;
            ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Invalid RTE found")));
            break;
#endif /* PGXC */
        default:

            /*
             * RTE is a join, subselect, or VALUES.  We represent this as a
             * whole-row Var of RECORD type. (Note that in most cases the Var
             * will be expanded to a RowExpr during planning, but that is not
             * our concern here.)
             */
            result = makeVar(varno, InvalidAttrNumber, RECORDOID, -1, InvalidOid, varlevelsup);
            break;
    }

    return result;
}

/*
 * makeTargetEntry -
 *	  creates a TargetEntry node
 */
TargetEntry* makeTargetEntry(Expr* expr, AttrNumber resno, char* resname, bool resjunk)
{
    TargetEntry* tle = makeNode(TargetEntry);

    tle->expr = expr;
    tle->resno = resno;
    tle->resname = resname;

    /*
     * We always set these fields to 0. If the caller wants to change them he
     * must do so explicitly.  Few callers do that, so omitting these
     * arguments reduces the chance of error.
     */
    tle->ressortgroupref = 0;
    tle->resorigtbl = InvalidOid;
    tle->resorigcol = 0;

    tle->resjunk = resjunk;

    return tle;
}

/*
 * flatCopyTargetEntry -
 *	  duplicate a TargetEntry, but don't copy substructure
 *
 * This is commonly used when we just want to modify the resno or substitute
 * a new expression.
 */
TargetEntry* flatCopyTargetEntry(TargetEntry* src_tle)
{
    TargetEntry* tle = makeNode(TargetEntry);

    Assert(IsA(src_tle, TargetEntry));
    errno_t rc = memcpy_s(tle, sizeof(TargetEntry), src_tle, sizeof(TargetEntry));
    securec_check(rc, "\0", "\0");
    return tle;
}
#endif /* !FRONTEND_PARSER */

/*
 * makeFromExpr -
 *	  creates a FromExpr node
 */
FromExpr* makeFromExpr(List* fromlist, Node* quals)
{
    FromExpr* f = makeNode(FromExpr);

    f->fromlist = fromlist;
    f->quals = quals;
    return f;
}

/*
 * makeColumnRef -
 *      creates a ColumnRef node
 */
ColumnRef* makeColumnRef(char* relname, char* colname, int location)
{
    ColumnRef* c = makeNode(ColumnRef);
    c->fields = list_make2((Node*)makeString(relname), (Node*)makeString(colname));
    c->location = location;
    return c;
}

/*
 * makeConst -
 *	  creates a Const node
 */
Const* makeConst(Oid consttype, int32 consttypmod, Oid constcollid, int constlen, Datum constvalue, bool constisnull,
    bool constbyval, Cursor_Data* cur)
{
    Const* cnst = makeNode(Const);

    cnst->consttype = consttype;
    cnst->consttypmod = consttypmod;
    cnst->constcollid = constcollid;
    cnst->constlen = constlen;
    cnst->constvalue = constvalue;
    cnst->constisnull = constisnull;
    cnst->constbyval = constbyval;
    cnst->location = -1; /* "unknown" */
    cnst->ismaxvalue = false;
#ifndef FRONTEND_PARSER
    if (cur != NULL) {
        CopyCursorInfoData(&cnst->cursor_data, cur);
    } else {
        cnst->cursor_data.cur_dno = -1;
    }
#endif
    return cnst;
}
/*
 * makeNullTest -
 *	  creates a Null Test expr like "expr is (NOT) NULL"
 */
NullTest* makeNullTest(NullTestType type, Expr* expr)
{
    NullTest* n = makeNode(NullTest);

    n->nulltesttype = type;
    n->arg = expr;

    return n;
}

Node* makeNullAConst(int location)
{
    A_Const* n = makeNode(A_Const);

    n->val.type = T_Null;
    n->location = location;

    return (Node*)n;
}

#ifndef FRONTEND_PARSER
/*
 * makeNullConst -
 *	  creates a Const node representing a NULL of the specified type/typmod
 *
 * This is a convenience routine that just saves a lookup of the type's
 * storage properties.
 */
Const* makeNullConst(Oid consttype, int32 consttypmod, Oid constcollid)
{
    int16 typLen;
    bool typByVal = false;

    get_typlenbyval(consttype, &typLen, &typByVal);
    return makeConst(consttype, consttypmod, constcollid, (int)typLen, (Datum)0, true, typByVal);
}
#endif /* FRONTEND_PARSER */

/*
 * makeBoolConst -
 *	  creates a Const node representing a boolean value (can be NULL too)
 */
Node* makeBoolConst(bool value, bool isnull)
{
    /* note that pg_type.h hardwires size of bool as 1 ... duplicate it */
    return (Node*)makeConst(BOOLOID, -1, InvalidOid, 1, BoolGetDatum(value), isnull, true);
}

#ifndef FRONTEND_PARSER
/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: creates a Const node representing a max value
 * Description	:
 * Notes		:
 */
Const* makeMaxConst(Oid consttype, int32 consttypmod, Oid constcollid)
{
    Const* cnst = NULL;
    int16 typLen;
    bool typByVal = false;

    if (InvalidOid == consttype) {
        cnst = makeConst(InvalidOid, -1, InvalidOid, 0, (Datum)0, false, true);
    } else {
        get_typlenbyval(consttype, &typLen, &typByVal);
        cnst = makeConst(consttype, consttypmod, constcollid, (int)typLen, (Datum)0, false, true);
    }

    cnst->ismaxvalue = true;

    return cnst;
}
#endif /* FRONTEND_PARSER */

/*
 * makeBoolExpr -
 *	  creates a BoolExpr node
 */
Expr* makeBoolExpr(BoolExprType boolop, List* args, int location)
{
    BoolExpr* b = makeNode(BoolExpr);

    b->boolop = boolop;
    b->args = args;
    b->location = location;

    return (Expr*)b;
}

/*
 * makeBoolExpr -
 *	  creates a BoolExpr tree node.
 */
Expr* makeBoolExprTreeNode(BoolExprType boolop, List* args)
{
    Node* node = NULL;
    ListCell* lc = NULL;

    foreach (lc, args) {
        BoolExpr* b = NULL;

        if (node == NULL) {
            node = (Node*)lfirst(lc);
            continue;
        }

        b = makeNode(BoolExpr);
        b->boolop = boolop;
        b->args = list_make2(node, lfirst(lc));
        b->location = 0;
        node = (Node*)b;
    }

    return (Expr*)node;
}

/*
 * makeAlias -
 *	  creates an Alias node
 *
 * NOTE: the given name is copied, but the colnames list (if any) isn't.
 */
Alias* makeAlias(const char* aliasname, List* colnames)
{
    Alias* a = makeNode(Alias);

#ifndef FRONTEND_PARSER
    a->aliasname = pstrdup(aliasname);
#else
    a->aliasname = strdup(aliasname);
#endif
    a->colnames = colnames;

    return a;
}

/*
 * makeRelabelType -
 *	  creates a RelabelType node
 */
RelabelType* makeRelabelType(Expr* arg, Oid rtype, int32 rtypmod, Oid rcollid, CoercionForm rformat)
{
    RelabelType* r = makeNode(RelabelType);

    r->arg = arg;
    r->resulttype = rtype;
    r->resulttypmod = rtypmod;
    r->resultcollid = rcollid;
    r->relabelformat = rformat;
    r->location = -1;

    return r;
}

/*
 * makeRangeVar -
 *	  creates a RangeVar node (rather oversimplified case)
 */
RangeVar* makeRangeVar(char* schemaname, char* relname, int location)
{
    RangeVar* r = makeNode(RangeVar);

    r->catalogname = NULL;
    r->schemaname = schemaname;
    r->relname = relname;
    r->partitionname = NULL;
    r->subpartitionname = NULL;
    r->inhOpt = INH_DEFAULT;
    r->relpersistence = RELPERSISTENCE_PERMANENT;
    r->alias = NULL;
    r->location = location;
    r->ispartition = false;
    r->issubpartition = false;
    r->partitionKeyValuesList = NIL;
    r->isbucket = false;
    r->buckets = NIL;
    r->length = 0;
    r->withVerExpr = false;

    return r;
}

/*
 * makeTypeName -
 *	build a TypeName node for an unqualified name.
 *
 * typmod is defaulted, but can be changed later by caller.
 */
TypeName* makeTypeName(char* typnam)
{
    return makeTypeNameFromNameList(list_make1(makeString(typnam)));
}

/*
 * makeTypeNameFromNameList -
 *	build a TypeName node for a String list representing a qualified name.
 *
 * typmod is defaulted, but can be changed later by caller.
 */
TypeName* makeTypeNameFromNameList(List* names)
{
    TypeName* n = makeNode(TypeName);

    n->names = names;
    n->typmods = NIL;
    n->typemod = -1;
    n->location = -1;
    n->pct_rowtype = false;
    return n;
}

/*
 * makeTypeNameFromOid -
 *	build a TypeName node to represent a type already known by OID/typmod.
 */
TypeName* makeTypeNameFromOid(Oid typeOid, int32 typmod)
{
    TypeName* n = makeNode(TypeName);

    n->typeOid = typeOid;
    n->typemod = typmod;
    n->location = -1;
    return n;
}

/*
 * makeFuncExpr -
 *	build an expression tree representing a function call.
 *
 * The argument expressions must have been transformed already.
 */
FuncExpr* makeFuncExpr(Oid funcid, Oid rettype, List* args, Oid funccollid, Oid inputcollid, CoercionForm fformat)
{
    FuncExpr* funcexpr = NULL;

    funcexpr = makeNode(FuncExpr);
    funcexpr->funcid = funcid;
    funcexpr->funcresulttype = rettype;
    funcexpr->funcresulttype_orig = -1;
    funcexpr->funcretset = false; /* only allowed case here */
    funcexpr->funcformat = fformat;
    funcexpr->funccollid = funccollid;
    funcexpr->inputcollid = inputcollid;
    funcexpr->args = args;
    funcexpr->location = -1;

    return funcexpr;
}

/*
 * makeDefElem -
 *	build a DefElem node
 *
 * This is sufficient for the "typical" case with an unqualified option name
 * and no special action.
 */
DefElem* makeDefElem(char* name, Node* arg)
{
    DefElem* res = makeNode(DefElem);

    res->defnamespace = NULL;
    res->defname = name;
    res->arg = arg;
    res->defaction = DEFELEM_UNSPEC;
    res->begin_location = -1;
    res->end_location = -1;

    return res;
}

/*
 * makeDefElem -
 *	build a DefElem node
 *
 * similar to makeDefElem, add begin_location and end_location params
 */
DefElem* MakeDefElemWithLoc(char* name, Node* arg, int begin_loc, int end_loc)
{
    DefElem* res = makeDefElem(name, arg);

    res->begin_location = begin_loc;
    res->end_location = end_loc;
    
    return res;    
}


/*
 * makeDefElemExtended -
 *	build a DefElem node with all fields available to be specified
 */
DefElem* makeDefElemExtended(char* nameSpace, char* name, Node* arg, DefElemAction defaction)
{
    DefElem* res = makeNode(DefElem);

    res->defnamespace = nameSpace;
    res->defname = name;
    res->arg = arg;
    res->defaction = defaction;
    res->begin_location = -1;
    res->end_location = -1;

    return res;
}

/*
 * makeHashFilter -
 *	  creates a Hash Filter expr for replicated node to filter tuple using hash
 */
HashFilter* makeHashFilter(List* arg, List* typeOidlist, List* nodelist)
{
    HashFilter* n = makeNode(HashFilter);

    n->arg = arg;
    n->typeOids = typeOidlist;
    n->nodeList = nodelist;

    return n;
}

/*
 * makeGroupingSet
 *
 */
GroupingSet* makeGroupingSet(GroupingSetKind kind, List* content, int location)
{
    GroupingSet* n = makeNode(GroupingSet);

    n->kind = kind;
    n->content = content;
    n->location = location;
    return n;
}

/*
 * makeTidConst -
 *	  creates a Const node representing a Tid value
 */
Node* makeTidConst(ItemPointer item)
{
    return (Node*)makeConst(TIDOID, -1, InvalidOid, TID_TYPE_LEN, PointerGetDatum(item), false, false);
}

/*
 * makeFuncCall -
 *	  creates a FuncCall node
 */
FuncCall* makeFuncCall(List* funcname, List* args, int location)
{
    FuncCall* funcCall = NULL;
    funcCall = (FuncCall*)makeNode(FuncCall);
    funcCall->funcname = funcname;
    funcCall->colname = NULL;
    funcCall->args = args;
    funcCall->agg_star = FALSE;
    funcCall->func_variadic = FALSE;
    funcCall->agg_distinct = FALSE;
    funcCall->agg_order = NIL;
    funcCall->over = NULL;
    funcCall->location = location;

    return funcCall;
}

/*
 * Param -
 *	  creates a Param node
 */
Param* makeParam(ParamKind paramkind, int paramid, Oid paramtype, int32 paramtypmod, Oid paramcollid, int location)
{
    Param* argp = NULL;
    argp = makeNode(Param);

    argp->paramkind = paramkind;
    argp->paramid = paramid;
    argp->paramtype = paramtype;
    argp->paramtypmod = paramtypmod;
    argp->paramcollid = paramcollid;
    argp->location = location;

    return argp;
}
#ifndef FRONTEND_PARSER
/*
 * makeIndexInfo
 *	  create an IndexInfo node
 */
IndexInfo* makeIndexInfo(int numattrs, List* expressions, List* predicates, bool unique, bool isready, bool concurrent)
{
    IndexInfo* n = makeNode(IndexInfo);

    n->ii_NumIndexAttrs = numattrs;
    n->ii_Unique = unique;
    n->ii_ReadyForInserts = isready;
    n->ii_Concurrent = concurrent;

    /* expressions */
    n->ii_Expressions = expressions;
    n->ii_ExpressionsState = NIL;

    /* predicates  */
    n->ii_Predicate = predicates;
    n->ii_PredicateState = NULL;

    /* exclusion constraints */
    n->ii_ExclusionOps = NULL;
    n->ii_ExclusionProcs = NULL;
    n->ii_ExclusionStrats = NULL;

    /* initialize index-build state to default */
    n->ii_BrokenHotChain = false;
    n->ii_PgClassAttrId = 0;
    n->ii_ParallelWorkers = 0;

    return n;
}
#endif
