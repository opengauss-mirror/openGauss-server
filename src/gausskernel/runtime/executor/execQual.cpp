/* -------------------------------------------------------------------------
*
* execQual.cpp
*	  Routines to evaluate qualification and targetlist expressions
*
* Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
* Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
* Portions Copyright (c) 1994, Regents of the University of California
*
*
* IDENTIFICATION
*	  src/gausskernel/runtime/executor/execQual.cpp
*
* -------------------------------------------------------------------------
*/
/*
*	 INTERFACE ROUTINES
*		ExecEvalExpr	- (now a macro) evaluate an expression, return a datum
*		ExecEvalExprSwitchContext - same, but switch into eval memory context
*		ExecQual		- return true/false if qualification is satisfied
*		ExecProject		- form a new tuple by projecting the given tuple
*
*	 NOTES
*		The more heavily used ExecEvalExpr routines, such as ExecEvalScalarVar,
*		are hotspots. Making these faster will speed up the entire system.
*
*		ExecProject() is used to make tuple projections.  Rather then
*		trying to speed it up, the execution plan should be pre-processed
*		to facilitate attribute sharing between nodes wherever possible,
*		instead of doing needless copying.	-cim 5/31/91
*
*		During expression evaluation, we check_stack_depth only in
*		ExecMakeFunctionResult (and substitute routines) rather than at every
*		single node.  This is a compromise that trades off precision of the
*		stack limit setting to gain speed.
*/
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/nbtree.h"
#include "access/tupconvert.h"
#include "access/tableam.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_type.h"
#include "commands/typecmds.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeSubplan.h"
#include "executor/node/nodeAgg.h"
#include "executor/node/nodeCtescan.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "utils/xml.h"
#include "access/hash.h"
#include "access/transam.h"
#ifdef PGXC
#include "pgxc/groupmgr.h"
#include "pgxc/pgxc.h"
#endif
#include "optimizer/streamplan.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/executer_gstrace.h"
#include "commands/trigger.h"
#include "db4ai/gd.h"
#include "catalog/pg_proc_fn.h"
#include "access/tuptoaster.h"
#include "parser/parse_expr.h"
#include "auditfuncs.h"
#include "rewrite/rewriteHandler.h"

/* static function decls */
static bool isAssignmentIndirectionExpr(ExprState* exprstate);
static Datum ExecEvalAggref(AggrefExprState* aggref, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalWindowFunc(WindowFuncExprState* wfunc, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalScalarVar(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalScalarVarFast(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalWholeRowVar(
   WholeRowVarExprState* wrvstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalWholeRowFast(
   WholeRowVarExprState* wrvstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalWholeRowSlow(
   WholeRowVarExprState* wrvstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalConst(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalParamExec(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalParamExtern(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static bool isVectorEngineSupportSetFunc(Oid funcid);
template <bool vectorized>
void init_fcache(
   Oid foid, Oid input_collation, FuncExprState* fcache, MemoryContext fcacheCxt, bool allowSRF, bool needDescForSets);
void ShutdownFuncExpr(Datum arg);
static TupleDesc get_cached_rowtype(Oid type_id, int32 typmod, TupleDesc* cache_field, ExprContext* econtext);
static void ShutdownTupleDescRef(Datum arg);

template <bool has_refcursor>
static ExprDoneCond ExecEvalFuncArgs(
   FunctionCallInfo fcinfo, List* argList, ExprContext* econtext, int* plpgsql_var_dno = NULL);
void ExecPrepareTuplestoreResult(
   FuncExprState* fcache, ExprContext* econtext, Tuplestorestate* resultStore, TupleDesc resultDesc);
static void tupledesc_match(TupleDesc dst_tupdesc, TupleDesc src_tupdesc);

template <bool has_refcursor, bool has_cursor_return, bool isSetReturnFunc>
static Datum ExecMakeFunctionResult(FuncExprState* fcache, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);

template <bool has_refcursor, bool has_cursor_return>
static Datum ExecMakeFunctionResultNoSets(
   FuncExprState* fcache, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalFunc(FuncExprState* fcache, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalOper(FuncExprState* fcache, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalDistinct(FuncExprState* fcache, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalScalarArrayOp(
   ScalarArrayOpExprState* sstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalNot(BoolExprState* notclause, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalOr(BoolExprState* orExpr, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalAnd(BoolExprState* andExpr, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalConvertRowtype(
   ConvertRowtypeExprState* cstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalCase(CaseExprState* caseExpr, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalCaseTestExpr(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalArray(ArrayExprState* astate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalRow(RowExprState* rstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalRowCompare(RowCompareExprState* rstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalCoalesce(
   CoalesceExprState* coalesceExpr, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalMinMax(MinMaxExprState* minmaxExpr, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalXml(XmlExprState* xmlExpr, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalNullIf(FuncExprState* nullIfExpr, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalNullTest(NullTestState* nstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalHashFilter(HashFilterState* hstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalBooleanTest(GenericExprState* bstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalCoerceToDomain(
   CoerceToDomainState* cstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalCoerceToDomainValue(
   ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalFieldSelect(FieldSelectState* fstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalFieldStore(FieldStoreState* fstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalRelabelType(
   GenericExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalCoerceViaIO(CoerceViaIOState* iostate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalArrayCoerceExpr(
   ArrayCoerceExprState* astate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalCurrentOfExpr(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalGroupingFuncExpr(
   GroupingFuncExprState* gstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
static Datum ExecEvalGroupingIdExpr(
   GroupingIdExprState* gstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone);
bool func_has_refcursor_args(Oid Funcid, FunctionCallInfoData* fcinfo);
extern struct varlena *heap_tuple_fetch_and_copy(Relation rel, struct varlena *attr, bool needcheck);
static void check_huge_clob_paramter(FunctionCallInfoData* fcinfo, bool is_have_huge_clob);

THR_LOCAL PLpgSQL_execstate* plpgsql_estate = NULL;

/* ----------------------------------------------------------------
*		ExecEvalExpr routines
*
*		Recursively evaluate a targetlist or qualification expression.
*
* Each of the following routines having the signature
*		Datum ExecEvalFoo(ExprState *expression,
*						  ExprContext *econtext,
*						  bool *isNull,
*						  ExprDoneCond *isDone);
* is responsible for evaluating one type or subtype of ExprState node.
* They are normally called via the ExecEvalExpr macro, which makes use of
* the function pointer set up when the ExprState node was built by
* ExecInitExpr.  (In some cases, we change this pointer later to avoid
* re-executing one-time overhead.)
*
* Note: for notational simplicity we declare these functions as taking the
* specific type of ExprState that they work on.  This requires casting when
* assigning the function pointer in ExecInitExpr.	Be careful that the
* function signature is declared correctly, because the cast suppresses
* automatic checking!
*
*
* All these functions share this calling convention:
*
* Inputs:
*		expression: the expression state tree to evaluate
*		econtext: evaluation context information
*
* Outputs:
*		return value: Datum value of result
*		*isNull: set to TRUE if result is NULL (actual return value is
*				 meaningless if so); set to FALSE if non-null result
*		*isDone: set to indicator of set-result status
*
* A caller that can only accept a singleton (non-set) result should pass
* NULL for isDone; if the expression computes a set result then an error
* will be reported via ereport.  If the caller does pass an isDone pointer
* then *isDone is set to one of these three states:
*		ExprSingleResult		singleton result (not a set)
*		ExprMultipleResult		return value is one element of a set
*		ExprEndResult			there are no more elements in the set
* When ExprMultipleResult is returned, the caller should invoke
* ExecEvalExpr() repeatedly until ExprEndResult is returned.  ExprEndResult
* is returned after the last real set element.  For convenience isNull will
* always be set TRUE when ExprEndResult is returned, but this should not be
* taken as indicating a NULL element of the set.  Note that these return
* conventions allow us to distinguish among a singleton NULL, a NULL element
* of a set, and an empty set.
*
* The caller should already have switched into the temporary memory
* context econtext->ecxt_per_tuple_memory.  The convenience entry point
* ExecEvalExprSwitchContext() is provided for callers who don't prefer to
* do the switch in an outer loop.	We do not do the switch in these routines
* because it'd be a waste of cycles during nested expression evaluation.
* ----------------------------------------------------------------
*/
/* ----------
*	  ExecEvalArrayRef
*
*	   This function takes an ArrayRef and returns the extracted Datum
*	   if it's a simple reference, or the modified array value if it's
*	   an array assignment (i.e., array element or slice insertion).
*
* NOTE: if we get a NULL result from a subscript expression, we return NULL
* when it's an array reference, or raise an error when it's an assignment.
*
* NOTE: we deliberately refrain from applying DatumGetArrayTypeP() here,
* even though that might seem natural, because this code needs to support
* both varlena arrays and fixed-length array types.  DatumGetArrayTypeP()
* only works for the varlena kind.  The routines we call in arrayfuncs.c
* have to know the difference (that's what they need refattrlength for).
* ----------
*/
Datum ExecEvalArrayRef(ArrayRefExprState* astate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   ArrayRef* arrayRef = (ArrayRef*)astate->xprstate.expr;
   ArrayType* array_source = NULL;
   ArrayType* resultArray = NULL;
   bool isAssignment = (arrayRef->refassgnexpr != NULL);
   bool eisnull = false;
   ListCell* l = NULL;
   int i = 0;
   int j = 0;
   IntArray upper, lower;
   int* lIndex = NULL;
   Oid typOid = astate->xprstate.resultType;

   array_source = (ArrayType*)DatumGetPointer(ExecEvalExpr(astate->refexpr, econtext, isNull, isDone));
   /*
    * If refexpr yields NULL, and it's a fetch, then result is NULL. In the
    * assignment case, we'll cons up something below.
    */
   if (*isNull) {
       if (isDone && *isDone == ExprEndResult)
           return (Datum)NULL; /* end of set result */
       if (!isAssignment)
           return (Datum)NULL;
   }
   int returnNestTableLayer = 0;
    ExecTableOfIndexInfo execTableOfIndexInfo;
    initExecTableOfIndexInfo(&execTableOfIndexInfo, econtext);
    ExecEvalParamExternTableOfIndex((Node*)astate->refexpr->expr, &execTableOfIndexInfo);
    if (u_sess->SPI_cxt.cur_tableof_index != NULL) {
        u_sess->SPI_cxt.cur_tableof_index->tableOfIndexType = execTableOfIndexInfo.tableOfIndexType;
        u_sess->SPI_cxt.cur_tableof_index->tableOfIndex = execTableOfIndexInfo.tableOfIndex;
        u_sess->SPI_cxt.cur_tableof_index->tableOfGetNestLayer = list_length(astate->refupperindexpr);
    }

   foreach (l, astate->refupperindexpr) {
       ExprState* eltstate = (ExprState*)lfirst(l);

       if (i >= MAXDIM)
           ereport(ERROR,
                   (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)", i + 1, MAXDIM)));
       if (OidIsValid(execTableOfIndexInfo.tableOfIndexType) || execTableOfIndexInfo.isnestedtable) {
           bool isTran = false;
           PLpgSQL_execstate* old_estate = plpgsql_estate;
           Datum exprValue = (Datum)ExecEvalExpr(eltstate, econtext, &eisnull, NULL);
           plpgsql_estate = old_estate;
            if (unlikely(execTableOfIndexInfo.tableOfIndexType))
           if (execTableOfIndexInfo.tableOfIndexType == VARCHAROID && !eisnull && VARATT_IS_1B(exprValue)) {
               exprValue = transVaratt1BTo4B(exprValue);
               isTran = true;
           }
           TableOfIndexKey key;
           PLpgSQL_var* node = NULL;
           key.exprtypeid = execTableOfIndexInfo.tableOfIndexType;
           key.exprdatum = exprValue;
           int index = getTableOfIndexByDatumValue(key, execTableOfIndexInfo.tableOfIndex, &node);
           if (isTran) {
               pfree(DatumGetPointer(exprValue));
           }
           if (execTableOfIndexInfo.isnestedtable) {
               /* for nested table, we should take inner table's array and skip current indx */
               if (node == NULL || index == -1) {
                   eisnull = true;
               } else {
                   PLpgSQL_var* var = node;
                   execTableOfIndexInfo.isnestedtable  = (var->nest_table != NULL);
                   array_source = (ArrayType*)DatumGetPointer(var->value);
                   execTableOfIndexInfo.tableOfIndexType = var->datatype->tableOfIndexType;
                   execTableOfIndexInfo.tableOfIndex = var->tableOfIndex;
                   eisnull = var->isnull;
                    returnNestTableLayer = var->nest_layers;
                   if (plpgsql_estate)
                       plpgsql_estate->curr_nested_table_type = var->datatype->typoid;
                   continue;
               }
            } else {
                returnNestTableLayer = 0;
           }
           if (index == -1) {
               eisnull = true;
           } else {
               upper.indx[i++] = index;
           }
       } else {
            PLpgSQL_execstate* old_estate = plpgsql_estate;
           upper.indx[i++] = DatumGetInt32(ExecEvalExpr(eltstate, econtext, &eisnull, NULL));
            plpgsql_estate = old_estate;
            returnNestTableLayer = 0;
       }

       /* If any index expr yields NULL, result is NULL or error */
       if (eisnull) {
           if (isAssignment)
               ereport(ERROR,
                       (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("array subscript in assignment must not be null")));
           *isNull = true;
           return (Datum)NULL;
       }
   }

   if (astate->reflowerindexpr != NIL) {
       foreach (l, astate->reflowerindexpr) {
           ExprState* eltstate = (ExprState*)lfirst(l);

           if (j >= MAXDIM)
               ereport(ERROR,
                       (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)", j + 1, MAXDIM)));

           if (execTableOfIndexInfo.tableOfIndexType == VARCHAROID || execTableOfIndexInfo.isnestedtable) {
               ereport(ERROR,
                       (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                        errmsg("index by varchar or nested table don't support two subscripts")));
           } else {
               PLpgSQL_execstate* old_estate = plpgsql_estate;
                lower.indx[j++] = DatumGetInt32(ExecEvalExpr(eltstate, econtext, &eisnull, NULL));
                plpgsql_estate = old_estate;
           }
           /* If any index expr yields NULL, result is NULL or error */
           if (eisnull) {
               if (isAssignment)
                   ereport(ERROR,
                           (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                            errmsg("array subscript in assignment must not be null")));
               *isNull = true;
               return (Datum)NULL;
           }
       }
       /* this can't happen unless parser messed up */
       if (i != j)
           ereport(ERROR,
                   (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                    errmodule(MOD_EXECUTOR),
                    (errmsg("upper and lower index lists are not same length (%d, %d)", i, j))));
       lIndex = lower.indx;
   } else
       lIndex = NULL;

   if (isAssignment) {
       Datum sourceData;
       Datum save_datum;
       bool save_isNull = false;

       /*
        * We might have a nested-assignment situation, in which the
        * refassgnexpr is itself a FieldStore or ArrayRef that needs to
        * obtain and modify the previous value of the array element or slice
        * being replaced.	If so, we have to extract that value from the
        * array and pass it down via the econtext's caseValue.  It's safe to
        * reuse the CASE mechanism because there cannot be a CASE between
        * here and where the value would be needed, and an array assignment
        * can't be within a CASE either.  (So saving and restoring the
        * caseValue is just paranoia, but let's do it anyway.)
        *
        * Since fetching the old element might be a nontrivial expense, do it
        * only if the argument appears to actually need it.
        */
       save_datum = econtext->caseValue_datum;
       save_isNull = econtext->caseValue_isNull;

       if (isAssignmentIndirectionExpr(astate->refassgnexpr)) {
           if (*isNull) {
               /* whole array is null, so any element or slice is too */
               econtext->caseValue_datum = (Datum)0;
               econtext->caseValue_isNull = true;
           } else if (lIndex == NULL) {
               econtext->caseValue_datum = array_ref(array_source,
                                                     i,
                                                     upper.indx,
                                                     astate->refattrlength,
                                                     astate->refelemlength,
                                                     astate->refelembyval,
                                                     astate->refelemalign,
                                                     &econtext->caseValue_isNull);
           } else {
               resultArray = array_get_slice(array_source,
                                             i,
                                             upper.indx,
                                             lower.indx,
                                             astate->refattrlength,
                                             astate->refelemlength,
                                             astate->refelembyval,
                                             astate->refelemalign);
               econtext->caseValue_datum = PointerGetDatum(resultArray);
               econtext->caseValue_isNull = false;
           }
       } else {
           /* argument shouldn't need caseValue, but for safety set it null */
           econtext->caseValue_datum = (Datum)0;
           econtext->caseValue_isNull = true;
       }

       /*
        * Evaluate the value to be assigned into the array.
        */
       sourceData = ExecEvalExpr(astate->refassgnexpr, econtext, &eisnull, NULL);

       econtext->caseValue_datum = save_datum;
       econtext->caseValue_isNull = save_isNull;

       /*
        * For an assignment to a fixed-length array type, both the original
        * array and the value to be assigned into it must be non-NULL, else
        * we punt and return the original array.
        */
       if (astate->refattrlength > 0) /* fixed-length array? */
           if (eisnull || *isNull)
               return PointerGetDatum(array_source);

       /*
        * For assignment to varlena arrays, we handle a NULL original array
        * by substituting an empty (zero-dimensional) array; insertion of the
        * new element will result in a singleton array value.	It does not
        * matter whether the new element is NULL.
        */
       if (*isNull) {
           array_source = construct_empty_array(arrayRef->refelemtype);
           *isNull = false;
       }

       if (lIndex == NULL)
           resultArray = array_set(array_source,
                                   i,
                                   upper.indx,
                                   sourceData,
                                   eisnull,
                                   astate->refattrlength,
                                   astate->refelemlength,
                                   astate->refelembyval,
                                   astate->refelemalign);
       else
           resultArray = array_set_slice(array_source,
                                         i,
                                         upper.indx,
                                         lower.indx,
                                         (ArrayType*)DatumGetPointer(sourceData),
                                         eisnull,
                                         astate->refattrlength,
                                         astate->refelemlength,
                                         astate->refelembyval,
                                         astate->refelemalign);
       return PointerGetDatum(resultArray);
   }
   /* for nested table, if get inner table's elem, need cover elem type */
   if (list_length(astate->refupperindexpr) > i && i > 0 && plpgsql_estate) {
       if (plpgsql_estate->curr_nested_table_type != typOid) {
           plpgsql_estate->curr_nested_table_type = ARR_ELEMTYPE(array_source);
           get_typlenbyvalalign(plpgsql_estate->curr_nested_table_type,
                                &astate->refelemlength,
                                &astate->refelembyval,
                                &astate->refelemalign);
       }
   }
    if (plpgsql_estate) {
        plpgsql_estate->curr_nested_table_layers = returnNestTableLayer;
    }
   if (lIndex == NULL) {
       if (unlikely(i == 0)) {
           /* get nested table's inner table */
           *isNull = eisnull;
           return (Datum)array_source;
       } else {
           return array_ref(array_source,
                            i,
                            upper.indx,
                            astate->refattrlength,
                            astate->refelemlength,
                            astate->refelembyval,
                            astate->refelemalign,
                            isNull);
       }
   } else {
       resultArray = array_get_slice(array_source,
                                     i,
                                     upper.indx,
                                     lower.indx,
                                     astate->refattrlength,
                                     astate->refelemlength,
                                     astate->refelembyval,
                                     astate->refelemalign);
       return PointerGetDatum(resultArray);
   }
}

/*
* Helper for ExecEvalArrayRef: is expr a nested FieldStore or ArrayRef
* that might need the old element value passed down?
*
* (We could use this in ExecEvalFieldStore too, but in that case passing
* the old value is so cheap there's no need.)
*/
static bool isAssignmentIndirectionExpr(ExprState* exprstate)
{
   if (exprstate == NULL)
       return false; /* just paranoia */
   if (IsA(exprstate, FieldStoreState)) {
       FieldStore* fstore = (FieldStore*)exprstate->expr;

       if (fstore->arg && IsA(fstore->arg, CaseTestExpr))
           return true;
   } else if (IsA(exprstate, ArrayRefExprState)) {
       ArrayRef* arrayRef = (ArrayRef*)exprstate->expr;

       if (arrayRef->refexpr && IsA(arrayRef->refexpr, CaseTestExpr))
           return true;
   }
   return false;
}

/* ----------------------------------------------------------------
*		ExecEvalAggref
*
*		Returns a Datum whose value is the value of the precomputed
*		aggregate found in the given expression context.
* ----------------------------------------------------------------
*/
static Datum ExecEvalAggref(AggrefExprState* aggref, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   if (isDone != NULL)
       *isDone = ExprSingleResult;

   if (econtext->ecxt_aggvalues == NULL) /* safety check */
       ereport(ERROR,
               (errcode(ERRCODE_INVALID_AGG),
                errmodule(MOD_EXECUTOR),
                errmsg("no aggregates in this expression context")));

   *isNull = econtext->ecxt_aggnulls[aggref->aggno];
   return econtext->ecxt_aggvalues[aggref->aggno];
}

/* ----------------------------------------------------------------
*		ExecEvalWindowFunc
*
*		Returns a Datum whose value is the value of the precomputed
*		window function found in the given expression context.
* ----------------------------------------------------------------
*/
static Datum ExecEvalWindowFunc(WindowFuncExprState* wfunc, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   if (isDone != NULL)
       *isDone = ExprSingleResult;

   if (econtext->ecxt_aggvalues == NULL) /* safety check */
       ereport(ERROR,
               (errcode(ERRCODE_WINDOWING_ERROR),
                errmodule(MOD_EXECUTOR),
                errmsg("no window functions in this expression context")));

   *isNull = econtext->ecxt_aggnulls[wfunc->wfuncno];
   return econtext->ecxt_aggvalues[wfunc->wfuncno];
}

/* ----------------------------------------------------------------
*		ExecEvalScalarVar
*
*		Returns a Datum whose value is the value of a scalar (not whole-row)
*		range variable with respect to given expression context.
*
* Note: ExecEvalScalarVar is executed only the first time through in a given
* plan; it changes the ExprState's function pointer to pass control directly
* to ExecEvalScalarVarFast after making one-time checks.
* ----------------------------------------------------------------
*/
static Datum ExecEvalScalarVar(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   Var* variable = (Var*)exprstate->expr;
   TupleTableSlot* slot = NULL;
   AttrNumber attnum;

   if (isDone != NULL)
       *isDone = ExprSingleResult;

   /* Get the input slot and attribute number we want */
   switch (variable->varno) {
       case INNER_VAR: /* get the tuple from the inner node */
           slot = econtext->ecxt_innertuple;
           break;

       case OUTER_VAR: /* get the tuple from the outer node */
           slot = econtext->ecxt_outertuple;
           break;

           /* INDEX_VAR is handled by default case */
       default: /* get the tuple from the relation being scanned */
           slot = econtext->ecxt_scantuple;
           if (u_sess->parser_cxt.in_userset) {
               u_sess->parser_cxt.has_set_uservar = true;
           }
           break;
   }

    attnum = variable->varattno;

    /* This was checked by ExecInitExpr */
    Assert(attnum != InvalidAttrNumber);

    RightRefState* refState = econtext->rightRefState;
    int index = attnum - 1;
    if (refState && refState->values &&
        ((slot == nullptr && IS_ENABLE_INSERT_RIGHT_REF(refState)) ||
         (IS_ENABLE_UPSERT_RIGHT_REF(refState) && refState->hasExecs[index] && index < refState->colCnt))) {
        *isNull = refState->isNulls[index];
        return refState->values[index];
    }

    if (slot == nullptr) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_ATTRIBUTE), errmodule(MOD_EXECUTOR),
                        errmsg("attribute number %d does not exists.", attnum)));
    }

    /*
     * If it's a user attribute, check validity (bogus system attnums will be
     * caught inside table's getattr).  What we have to check for here is the
     * possibility of an attribute having been changed in type since the plan
     * tree was created.  Ideally the plan will get invalidated and not
     * re-used, but just in case, we keep these defenses.  Fortunately it's
     * sufficient to check once on the first time through.
     *
     * Note: we allow a reference to a dropped attribute.  table's getattr will
     * force a NULL result in such cases.
     *
     * Note: ideally we'd check typmod as well as typid, but that seems
     * impractical at the moment: in many cases the tupdesc will have been
     * generated by ExecTypeFromTL(), and that can't guarantee to generate an
     * accurate typmod in all cases, because some expression node types don't
     * carry typmod.
     */
    if (attnum > 0) {
        TupleDesc slot_tupdesc = slot->tts_tupleDescriptor;
        Form_pg_attribute attr;

       if (attnum > slot_tupdesc->natts) /* should never happen */
           ereport(ERROR,
                   (errcode(ERRCODE_INVALID_ATTRIBUTE),
                    errmodule(MOD_EXECUTOR),
                    errmsg("attribute number %d exceeds number of columns %d", attnum, slot_tupdesc->natts)));

       attr = &slot_tupdesc->attrs[attnum - 1];

       /* can't check type if dropped, since atttypid is probably 0 */
       if (!attr->attisdropped) {
           if (variable->vartype != attr->atttypid)
               ereport(ERROR,
                       (errcode(ERRCODE_INVALID_ATTRIBUTE),
                        errmodule(MOD_EXECUTOR),
                        errmsg("attribute %d has wrong type", attnum),
                        errdetail("Table has type %s, but query expects %s.",
                                  format_type_be(attr->atttypid),
                                  format_type_be(variable->vartype))));
       }
   }

   /* Skip the checking on future executions of node */
   exprstate->evalfunc = ExecEvalScalarVarFast;

   /* Fetch the value from the slot */
   return tableam_tslot_getattr(slot, attnum, isNull);
}

/* ----------------------------------------------------------------
*		ExecEvalScalarVarFast
*
*		Returns a Datum for a scalar variable.
* ----------------------------------------------------------------
*/
static Datum ExecEvalScalarVarFast(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   Var* variable = (Var*)exprstate->expr;
   TupleTableSlot* slot = NULL;
   AttrNumber attnum;

   if (isDone != NULL)
       *isDone = ExprSingleResult;

   /* Get the input slot and attribute number we want */
   switch (variable->varno) {
       case INNER_VAR: /* get the tuple from the inner node */
           slot = econtext->ecxt_innertuple;
           break;

       case OUTER_VAR: /* get the tuple from the outer node */
           slot = econtext->ecxt_outertuple;
           break;

           /* INDEX_VAR is handled by default case */
       default: /* get the tuple from the relation being scanned */
           slot = econtext->ecxt_scantuple;
           if (u_sess->parser_cxt.in_userset) {
               u_sess->parser_cxt.has_set_uservar = true;
           }
           break;
   }

   attnum = variable->varattno;

   Assert(slot != NULL);
   /* Fetch the value from the slot */
   return tableam_tslot_getattr(slot, attnum, isNull);
}

/* ----------------------------------------------------------------
*		ExecEvalWholeRowVar
*
*		Returns a Datum whose value is the value of a whole-row range
*		variable with respect to given expression context.
*
* Note: ExecEvalWholeRowVar is executed only the first time through in a
* given plan; it changes the ExprState's function pointer to pass control
* directly to ExecEvalWholeRowFast or ExecEvalWholeRowSlow after making
* one-time checks.
* ----------------------------------------------------------------
*/
static Datum ExecEvalWholeRowVar(
   WholeRowVarExprState* wrvstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   Var* variable = (Var*)wrvstate->xprstate.expr;
   TupleTableSlot* slot = NULL;
   bool needslow = false;

   if (isDone != NULL)
       *isDone = ExprSingleResult;

   /* This was checked by ExecInitExpr */
   Assert(variable->varattno == InvalidAttrNumber);

   /* Get the input slot we want */
   switch (variable->varno) {
       case INNER_VAR: /* get the tuple from the inner node */
           slot = econtext->ecxt_innertuple;
           break;

       case OUTER_VAR: /* get the tuple from the outer node */
           slot = econtext->ecxt_outertuple;
           break;

           /* INDEX_VAR is handled by default case */
       default: /* get the tuple from the relation being scanned */
           slot = econtext->ecxt_scantuple;
           break;
   }

   /*
    * If the input tuple came from a subquery, it might contain "resjunk"
    * columns (such as GROUP BY or ORDER BY columns), which we don't want to
    * keep in the whole-row result.  We can get rid of such columns by
    * passing the tuple through a JunkFilter --- but to make one, we have to
    * lay our hands on the subquery's targetlist.  Fortunately, there are not
    * very many cases where this can happen, and we can identify all of them
    * by examining our parent PlanState.  We assume this is not an issue in
    * standalone expressions that don't have parent plans.  (Whole-row Vars
    * can occur in such expressions, but they will always be referencing
    * table rows.)
    */
   if (wrvstate->parent) {
       PlanState* subplan = NULL;

       switch (nodeTag(wrvstate->parent)) {
           case T_SubqueryScanState:
               subplan = ((SubqueryScanState*)wrvstate->parent)->subplan;
               break;
           case T_CteScanState:
               subplan = ((CteScanState*)wrvstate->parent)->cteplanstate;
               break;
           default:
               break;
       }

       if (subplan != NULL) {
           bool junk_filter_needed = false;
           ListCell* tlist = NULL;

           /* Detect whether subplan tlist actually has any junk columns */
           foreach (tlist, subplan->plan->targetlist) {
               TargetEntry* tle = (TargetEntry*)lfirst(tlist);

               if (tle->resjunk) {
                   junk_filter_needed = true;
                   break;
               }
           }

           /* If so, build the junkfilter in the query memory context */
           if (junk_filter_needed) {
               MemoryContext oldcontext;

               oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
               wrvstate->wrv_junkFilter = ExecInitJunkFilter(subplan->plan->targetlist,
                                                             ExecGetResultType(subplan)->tdhasoid,
                                                             ExecInitExtraTupleSlot(wrvstate->parent->state),
                    TableAmHeap);
                MemoryContextSwitchTo(oldcontext);
            }
        }
    }

   /* Apply the junkfilter if any */
   if (wrvstate->wrv_junkFilter != NULL)
       slot = ExecFilterJunk(wrvstate->wrv_junkFilter, slot);

   /*
    * If the Var identifies a named composite type, we must check that the
    * actual tuple type is compatible with it.
    */
   if (variable->vartype != RECORDOID) {
       TupleDesc var_tupdesc;
       TupleDesc slot_tupdesc;
       int i;

       /*
        * We really only care about numbers of attributes and data types.
        * Also, we can ignore type mismatch on columns that are dropped in
        * the destination type, so long as (1) the physical storage matches
        * or (2) the actual column value is NULL.	Case (1) is helpful in
        * some cases involving out-of-date cached plans, while case (2) is
        * expected behavior in situations such as an INSERT into a table with
        * dropped columns (the planner typically generates an INT4 NULL
        * regardless of the dropped column type).	If we find a dropped
        * column and cannot verify that case (1) holds, we have to use
        * ExecEvalWholeRowSlow to check (2) for each row.
        */
       var_tupdesc = lookup_rowtype_tupdesc(variable->vartype, -1);

       slot_tupdesc = slot->tts_tupleDescriptor;

       if (var_tupdesc->natts != slot_tupdesc->natts)
           ereport(ERROR,
                   (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("table row type and query-specified row type do not match"),
                    errdetail_plural("Table row contains %d attribute, but query expects %d.",
                                     "Table row contains %d attributes, but query expects %d.",
                                     slot_tupdesc->natts,
                                     slot_tupdesc->natts,
                                     var_tupdesc->natts)));

       for (i = 0; i < var_tupdesc->natts; i++) {
           Form_pg_attribute vattr = &var_tupdesc->attrs[i];
           Form_pg_attribute sattr = &slot_tupdesc->attrs[i];

           if (vattr->atttypid == sattr->atttypid)
               continue; /* no worries */
           if (!vattr->attisdropped)
               ereport(ERROR,
                       (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("table row type and query-specified row type do not match"),
                        errdetail("Table has type %s at ordinal position %d, but query expects %s.",
                                  format_type_be(sattr->atttypid),
                                  i + 1,
                                  format_type_be(vattr->atttypid))));

           if (vattr->attlen != sattr->attlen || vattr->attalign != sattr->attalign)
               needslow = true; /* need runtime check for null */
       }

       ReleaseTupleDesc(var_tupdesc);
   }

   /* Skip the checking on future executions of node */
   if (needslow)
       wrvstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalWholeRowSlow;
   else
       wrvstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalWholeRowFast;

   /* Fetch the value */
   return (*wrvstate->xprstate.evalfunc)((ExprState*)wrvstate, econtext, isNull, isDone);
}

/* ----------------------------------------------------------------
*		ExecEvalWholeRowFast
*
*		Returns a Datum for a whole-row variable.
* ----------------------------------------------------------------
*/
static Datum ExecEvalWholeRowFast(
   WholeRowVarExprState* wrvstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   Var* variable = (Var*)wrvstate->xprstate.expr;
   TupleTableSlot* slot = NULL;
   TupleDesc slot_tupdesc;
   HeapTuple tuple;
   TupleDesc tupleDesc;
   HeapTupleHeader dtuple;
   errno_t rc = EOK;

   if (isDone != NULL)
       *isDone = ExprSingleResult;
   *isNull = false;

   /* Get the input slot we want */
   switch (variable->varno) {
       case INNER_VAR: /* get the tuple from the inner node */
           slot = econtext->ecxt_innertuple;
           break;

       case OUTER_VAR: /* get the tuple from the outer node */
           slot = econtext->ecxt_outertuple;
           break;

           /* INDEX_VAR is handled by default case */
       default: /* get the tuple from the relation being scanned */
           slot = econtext->ecxt_scantuple;
           break;
   }

   /* Apply the junkfilter if any */
   if (wrvstate->wrv_junkFilter != NULL)
       slot = ExecFilterJunk(wrvstate->wrv_junkFilter, slot);

   /*
    * If it's a RECORD Var, we'll use the slot's type ID info.  It's likely
    * that the slot's type is also RECORD; if so, make sure it's been
    * "blessed", so that the Datum can be interpreted later.
    */
   slot_tupdesc = slot->tts_tupleDescriptor;
   if (variable->vartype == RECORDOID) {
       if (slot_tupdesc->tdtypeid == RECORDOID && slot_tupdesc->tdtypmod < 0)
           assign_record_type_typmod(slot_tupdesc);
   }

   tuple = ExecFetchSlotTuple(slot);
   tupleDesc = slot->tts_tupleDescriptor;
    /*
     * If it's a RECORD Var, we'll use the slot's type ID info.  It's likely
     * that the slot's type is also RECORD; if so, make sure it's been
     * "blessed", so that the Datum can be interpreted later.  (Note: we must
     * do this here, not in ExecEvalWholeRowVar, because some plan trees may
     * return different slots at different times.  We have to be ready to
     * bless additional slots during the run.)
     */
    if (variable->vartype == RECORDOID &&
        tupleDesc->tdtypeid == RECORDOID &&
        tupleDesc->tdtypmod < 0)
        assign_record_type_typmod(tupleDesc);

   /*
    * We have to make a copy of the tuple so we can safely insert the Datum
    * overhead fields, which are not set in on-disk tuples.
    */
   dtuple = (HeapTupleHeader)palloc(tuple->t_len);
   rc = memcpy_s((char*)dtuple, tuple->t_len, (char*)tuple->t_data, tuple->t_len);
   securec_check(rc, "\0", "\0");

   HeapTupleHeaderSetDatumLength(dtuple, tuple->t_len);

   /*
    * If the Var identifies a named composite type, label the tuple with that
    * type; otherwise use what is in the tupleDesc.
    */
   if (variable->vartype != RECORDOID) {
       HeapTupleHeaderSetTypeId(dtuple, variable->vartype);
       HeapTupleHeaderSetTypMod(dtuple, variable->vartypmod);
   } else {
       HeapTupleHeaderSetTypeId(dtuple, tupleDesc->tdtypeid);
       HeapTupleHeaderSetTypMod(dtuple, tupleDesc->tdtypmod);
   }

   return PointerGetDatum(dtuple);
}

/* ----------------------------------------------------------------
*		ExecEvalWholeRowSlow
*
*		Returns a Datum for a whole-row variable, in the "slow" case where
*		we can't just copy the subplan's output.
* ----------------------------------------------------------------
*/
static Datum ExecEvalWholeRowSlow(
   WholeRowVarExprState* wrvstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   Var* variable = (Var*)wrvstate->xprstate.expr;
   TupleTableSlot* slot = NULL;
   HeapTuple tuple;
   TupleDesc tupleDesc;
   TupleDesc var_tupdesc;
   HeapTupleHeader dtuple;
   int i;
   errno_t rc = EOK;

   if (isDone != NULL)
       *isDone = ExprSingleResult;
   *isNull = false;

   /* Get the input slot we want */
   switch (variable->varno) {
       case INNER_VAR: /* get the tuple from the inner node */
           slot = econtext->ecxt_innertuple;
           break;

       case OUTER_VAR: /* get the tuple from the outer node */
           slot = econtext->ecxt_outertuple;
           break;

           /* INDEX_VAR is handled by default case */
       default: /* get the tuple from the relation being scanned */
           slot = econtext->ecxt_scantuple;
           break;
   }

   /* Apply the junkfilter if any */
   if (wrvstate->wrv_junkFilter != NULL)
       slot = ExecFilterJunk(wrvstate->wrv_junkFilter, slot);

   tuple = ExecFetchSlotTuple(slot);
   tupleDesc = slot->tts_tupleDescriptor;

   Assert(variable->vartype != RECORDOID);
   var_tupdesc = lookup_rowtype_tupdesc(variable->vartype, -1);

   /* Check to see if any dropped attributes are non-null */
   for (i = 0; i < var_tupdesc->natts; i++) {
       Form_pg_attribute vattr = &var_tupdesc->attrs[i];
       Form_pg_attribute sattr = &tupleDesc->attrs[i];

       if (!vattr->attisdropped)
           continue; /* already checked non-dropped cols */
       if (tableam_tops_tuple_attisnull(tuple, i + 1, tupleDesc))
           continue; /* null is always okay */
       if (vattr->attlen != sattr->attlen || vattr->attalign != sattr->attalign)
           ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                           errmsg("table row type and query-specified row type do not match"),
                           errdetail("Physical storage mismatch on dropped attribute at ordinal position %d.", i + 1)));
   }

   /*
    * We have to make a copy of the tuple so we can safely insert the Datum
    * overhead fields, which are not set in on-disk tuples.
    */
   dtuple = (HeapTupleHeader)palloc(tuple->t_len);
   rc = memcpy_s((char*)dtuple, tuple->t_len, (char*)tuple->t_data, tuple->t_len);
   securec_check(rc, "\0", "\0");

   HeapTupleHeaderSetDatumLength(dtuple, tuple->t_len);
   HeapTupleHeaderSetTypeId(dtuple, variable->vartype);
   HeapTupleHeaderSetTypMod(dtuple, variable->vartypmod);

   ReleaseTupleDesc(var_tupdesc);

   return PointerGetDatum(dtuple);
}

/* ----------------------------------------------------------------
*		ExecEvalConst
*
*		Returns the value of a constant.
*
*		Note that for pass-by-ref datatypes, we return a pointer to the
*		actual constant node.  This is one of the reasons why functions
*		must treat their input arguments as read-only.
* ----------------------------------------------------------------
*/
static Datum ExecEvalConst(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   Const* con = NULL;
    if (IsA(exprstate->expr, UserVar)) {
        bool found = false;
        UserVar *uservar = (UserVar *)exprstate->expr;
        GucUserParamsEntry *entry = (GucUserParamsEntry *)hash_search(u_sess->utils_cxt.set_user_params_htab, uservar->name, HASH_FIND, &found);

        /* if not found, return a null const */
        if (found) {
            Oid target_type = InvalidOid;
            if (IsA(uservar->value, CoerceViaIO)) {
                target_type = ((CoerceViaIO *)uservar->value)->resulttype;
            } else {
                target_type = ((Const *)uservar->value)->consttype;
            }
            if (target_type == UNKNOWNOID && ((Const *)uservar->value)->constisnull) {
                con = entry->value;
            } else {
                Node *node = coerce_type(NULL, (Node *)entry->value, entry->value->consttype, ((Const *)uservar->value)->consttype,
                    -1, COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
                node = eval_const_expression_value(NULL, node, NULL);
                if (nodeTag(node) != T_Const) {
                    ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION),
                        errmsg("The value of a user_defined variable must be convertible to a constant.")));
                }
                con = (Const *)node;
            }
        } else {
            con = makeConst(UNKNOWNOID, -1, InvalidOid, -2, (Datum)0, true, false);
        }
        if (u_sess->parser_cxt.in_userset) {
            u_sess->parser_cxt.has_set_uservar = true;
        }
    } else if (IsA(exprstate->expr, SetVariableExpr)) {
        SetVariableExpr* setvar = (SetVariableExpr*)transformSetVariableExpr((SetVariableExpr*)exprstate->expr);
        con = (Const*)setvar->value;
    } else {
        con = (Const*)exprstate->expr;
    }

   if (isDone != NULL)
       *isDone = ExprSingleResult;

   *isNull = con->constisnull;

   /* if a const cursor, copy cursor option data to econtext */
   if (econtext->is_cursor && con->consttype == REFCURSOROID) {
       CopyCursorInfoData(&econtext->cursor_data, &con->cursor_data);
       econtext->dno = con->cursor_data.cur_dno;
   }

   return con->constvalue;
}

/* ----------------------------------------------------------------
* ExecEvalRownum: Returns the rownum
* ----------------------------------------------------------------
*/
static Datum ExecEvalRownum(RownumState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   if (isDone != NULL)
       *isDone = ExprSingleResult;
   *isNull = false;

   if (ROWNUM_TYPE_COMPAT) {
       return DirectFunctionCall1(int8_numeric, Int64GetDatum(exprstate->ps->ps_rownum + 1));
   } else {
       return Int64GetDatum(exprstate->ps->ps_rownum + 1);
   }
}

/*----------------------------------------------------------------
* find_uservar_in_expr: A recursive function 
* For UserSetElemnt like @var := sin(@var), already remember root
* as Sin's exprState, and this function is used to find wheter @var
* is used in here.
* if_use : true means this @var is used inside a correct expression
*/
static void find_uservar_in_expr(ExprState *root, char *return_name, bool *if_use)
{
    if(root == NULL) {
        return;
    }
    switch(root->type) {
        case T_FuncExprState: {
        FuncExprState* parent = (FuncExprState*)root;
        ListCell* arg = NULL;
            foreach(arg,parent->args) {
                ExprState* child = (ExprState*)lfirst(arg);
                find_uservar_in_expr(child, return_name, if_use);
            }
        } break;
        case T_ExprState: {
            if (root->expr != NULL && root->expr->type == T_UserVar) {
                UserVar* temp = (UserVar*)root->expr;
                char* usename = temp->name;
                if(strcmp(return_name, usename) == 0) {
                    *if_use = true;
                }
            }
        } break;
        case T_AggrefExprState: {
            AggrefExprState* parent = (AggrefExprState*)root;
            ListCell* arg = NULL;
            foreach(arg, parent->args) {
                ExprState* child = (ExprState*)lfirst(arg);
                find_uservar_in_expr(child, return_name, if_use);
            }
        } break;
        case T_MinMaxExprState: {
            MinMaxExprState* parent = (MinMaxExprState*)root;
            ListCell* arg = NULL;
            foreach(arg, parent->args) {
                ExprState* child = (ExprState*)lfirst(arg);
                find_uservar_in_expr(child, return_name, if_use);
            }
        } break;
        case T_GenericExprState: {
            GenericExprState* parent = (GenericExprState*)root;
            find_uservar_in_expr(parent->arg, return_name, if_use);
        } break;
        case T_CaseExprState: {
            CaseExprState* parent = (CaseExprState*)root;
            ListCell* arg = NULL;
            foreach(arg, parent->args) {
                ExprState* child = (ExprState*)lfirst(arg);
                find_uservar_in_expr(child, return_name, if_use);
            }
        } break;
        case T_CaseWhenState: {
            CaseWhenState* parent = (CaseWhenState*)root;
            find_uservar_in_expr(parent->expr, return_name, if_use);
            find_uservar_in_expr(parent->result, return_name, if_use);
        } break;
        case T_WindowFuncExprState: {
            WindowFuncExprState* parent = (WindowFuncExprState*)root;
            ListCell* arg = NULL;
            foreach(arg, parent->args) {
                ExprState* child =(ExprState*)lfirst(arg);
                find_uservar_in_expr(child, return_name, if_use);
            }
        } break;
        case T_BoolExprState: {
            BoolExprState* parent = (BoolExprState*)root;
            ListCell* arg = NULL;
            foreach(arg, parent->args) {
                ExprState* child = (ExprState*)lfirst(arg);
                find_uservar_in_expr(child, return_name, if_use);
            }
        } break;
        case T_CoalesceExprState: {
            CoalesceExprState* parent = (CoalesceExprState*)root;
            ListCell* arg = NULL;
            foreach(arg, parent->args) {
                ExprState* child = (ExprState*)lfirst(arg);
                find_uservar_in_expr(child, return_name, if_use);
            }
        } break;
        case T_List: {
            List* parent = (List*)root;
            ListCell* arg = NULL;
            foreach(arg, parent) {
                ExprState* child = (ExprState*)lfirst(arg);
                find_uservar_in_expr(child, return_name, if_use);
            }
        } break;
        case T_NullTestState: {
            NullTestState* parent = (NullTestState*)root;
            find_uservar_in_expr(parent->arg,return_name, if_use);
        } break;
        case T_SubPlanState: 
            break;
        default: {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                errmsg("Unsupported expr type for select @i:= expr.")));
        }
    }
}


static char* CStringFromDatum(Oid typeoid, Datum d) 
{
    bool isVarlena;
    Oid outOid = InvalidOid;
    getTypeOutputInfo(typeoid, &outOid, &isVarlena);
    char* outStr = OidOutputFunctionCall(outOid, d);
    if (outStr == NULL)
        return "";
    return outStr;
}

/* ----------------------------------------------------------------
 * ExecEvalUserSetElm: set and Returns the user_define variable value
 * ----------------------------------------------------------------
 */
static Datum ExecEvalUserSetElm(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
    UserSetElemState* usestate = (UserSetElemState*)exprstate;
    UserSetElem* elem = usestate->use;
    UserSetElem elemcopy;
    Oid collid = exprCollation((Node*)elem->val);
    elemcopy.xpr = elem->xpr;
    elemcopy.name = elem->name;

    if (isDone != NULL)
        *isDone = ExprSingleResult;
    Assert(isNull);
    *isNull = false;

    bool is_in_table = false;
    if (econtext->ecxt_innertuple != NULL || econtext->ecxt_outertuple != NULL ||
        econtext->ecxt_scantuple != NULL) {
        is_in_table = true;
    }

    Const* con = NULL;
    Node* res = NULL;
    char* value  =  NULL;

    if (nodeTag(usestate->instate) != T_CaseExprState && DB_IS_CMPT(B_FORMAT))
        u_sess->parser_cxt.in_userset = true;

    Datum result = ExecEvalExpr(usestate->instate, econtext, isNull, isDone);

    if (*isNull) {
        con = makeConst(UNKNOWNOID, -1, collid, -2, result, true, false);
        res = (Node*)con;
        elemcopy.val = (Expr*)const_expression_to_const(res);
    } else {
        bool found = false;
        GucUserParamsEntry *entry = NULL;
        if (u_sess->utils_cxt.set_user_params_htab != NULL) {
            UserVar *uservar = (UserVar*)linitial(elem->name);
            entry = (GucUserParamsEntry*)hash_search(u_sess->utils_cxt.set_user_params_htab,
                uservar->name, HASH_FIND, &found);
        }

        Oid atttypid = InvalidOid;
        if (!found) {
            atttypid = exprType((Node *)usestate->instate->expr);
        } else {
            atttypid = exprType((Node *)elem->val);
        }

        value = CStringFromDatum(atttypid, result);
        con = processResToConst(value, atttypid, collid);
        if (atttypid == BOOLOID)
            res = (Node*)con;
        else
            res = type_transfer((Node *)con, atttypid, true);
        elemcopy.val = (Expr*)const_expression_to_const(res);
    }

    check_set_user_message(&elemcopy);
    u_sess->parser_cxt.in_userset = false;

    return result;
}
/* ----------------------------------------------------------------
*		ExecEvalParamExec
*
*		Returns the value of a PARAM_EXEC parameter.
* ----------------------------------------------------------------
*/
static Datum ExecEvalParamExec(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   Param* expression = (Param*)exprstate->expr;
   int thisParamId = expression->paramid;
   ParamExecData* prm = NULL;

   if (isDone != NULL)
       *isDone = ExprSingleResult;
    
    if (u_sess->parser_cxt.in_userset) {
        u_sess->parser_cxt.has_set_uservar = true;
    }

   /*
    * PARAM_EXEC params (internal executor parameters) are stored in the
    * ecxt_param_exec_vals array, and can be accessed by array index.
    */
   prm = &(econtext->ecxt_param_exec_vals[thisParamId]);
   if (prm->execPlan != NULL) {
       /* Parameter not evaluated yet, so go do it */
       ExecSetParamPlan((SubPlanState*)prm->execPlan, econtext);
       /* ExecSetParamPlan should have processed this param... */
       if (!u_sess->parser_cxt.has_set_uservar || !DB_IS_CMPT(B_FORMAT)) {
            Assert(prm->execPlan == NULL);
       }
       prm->isConst = true;
       prm->valueType = expression->paramtype;
   }
   *isNull = prm->isnull;
   prm->isChanged = true;
   return prm->value;
}

/* ----------------------------------------------------------------
*		ExecEvalParamExtern
*
*		Returns the value of a PARAM_EXTERN parameter.
* ----------------------------------------------------------------
*/
static Datum ExecEvalParamExtern(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   Param* expression = (Param*)exprstate->expr;
   int thisParamId = expression->paramid;
   ParamListInfo paramInfo = econtext->ecxt_param_list_info;

   if (isDone != NULL)
       *isDone = ExprSingleResult;

   /*
    * PARAM_EXTERN parameters must be sought in ecxt_param_list_info.
    */
   if (paramInfo && thisParamId > 0 && thisParamId <= paramInfo->numParams) {
       ParamExternData* prm = &paramInfo->params[thisParamId - 1];

       /* give hook a chance in case parameter is dynamic */
       if (!OidIsValid(prm->ptype) && paramInfo->paramFetch != NULL)
           (*paramInfo->paramFetch)(paramInfo, thisParamId);

       if (OidIsValid(prm->ptype)) {
           /* safety check in case hook did something unexpected */
           if (prm->ptype != expression->paramtype)
               ereport(ERROR,
                       (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("type of parameter %d (%s) does not match that when preparing the plan (%s)",
                               thisParamId,
                               format_type_be(prm->ptype),
                               format_type_be(expression->paramtype))));

           *isNull = prm->isnull;
            if (prm->tabInfo && prm->tabInfo->isnestedtable && plpgsql_estate) {
                plpgsql_estate->curr_nested_table_type = prm->ptype;
                plpgsql_estate->curr_nested_table_layers = prm->tabInfo->tableOfLayers;
            }
           /* copy cursor option from param to econtext */
           if (econtext->is_cursor && prm->ptype == REFCURSOROID) {
               CopyCursorInfoData(&econtext->cursor_data, &prm->cursor_data);
               econtext->dno = thisParamId - 1;
           }
           return prm->value;
       }
   }

   ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("no value found for parameter %d", thisParamId)));
   return (Datum)0; /* keep compiler quiet */
}

void initExecTableOfIndexInfo(ExecTableOfIndexInfo* execTableOfIndexInfo, ExprContext* econtext)
{
   execTableOfIndexInfo->econtext = econtext;
   execTableOfIndexInfo->tableOfIndex = NULL;
   execTableOfIndexInfo->tableOfIndexType = InvalidOid;
   execTableOfIndexInfo->isnestedtable = false;
   execTableOfIndexInfo->tableOfLayers = 0;
   execTableOfIndexInfo->paramid = -1;
   execTableOfIndexInfo->paramtype = InvalidOid;
}

/* this function is only used for getting table of index inout param */
static bool get_tableofindex_param(Node* node, ExecTableOfIndexInfo* execTableOfIndexInfo)
{
   if (node == NULL)
       return false;
   if (IsA(node, Param)) {
       execTableOfIndexInfo->paramid = ((Param*)node)->paramid;
       execTableOfIndexInfo->paramtype = ((Param*)node)->paramtype;
       return true;
   }
   return false;
}

static bool IsTableOfFunc(Oid funcOid)
{
   const Oid array_function_start_oid = 7881;
   const Oid array_function_end_oid = 7892;
   const Oid array_indexby_delete_oid = 7896;
   return (funcOid >= array_function_start_oid && funcOid <= array_function_end_oid) ||
          funcOid == array_indexby_delete_oid;
}

/* ----------------------------------------------------------------
*		ExecEvalParamExternTableOfIndex
*
*		Returns the value of a PARAM_EXTERN table of index and type parameter .
* ----------------------------------------------------------------
*/
void ExecEvalParamExternTableOfIndex(Node* node, ExecTableOfIndexInfo* execTableOfIndexInfo)
{
   if (get_tableofindex_param(node, execTableOfIndexInfo)) {
       ExecEvalParamExternTableOfIndexById(execTableOfIndexInfo);
   }
}

bool ExecEvalParamExternTableOfIndexById(ExecTableOfIndexInfo* execTableOfIndexInfo)
{
   if (execTableOfIndexInfo->paramid == -1) {
       return false;
   }

   int thisParamId = execTableOfIndexInfo->paramid;
   ParamListInfo paramInfo = execTableOfIndexInfo->econtext->ecxt_param_list_info;

   /*
    * PARAM_EXTERN parameters must be sought in ecxt_param_list_info.
    */
   if (paramInfo && thisParamId > 0 && thisParamId <= paramInfo->numParams) {
       ParamExternData* prm = &paramInfo->params[thisParamId - 1];

       /* give hook a chance in case parameter is dynamic */
       if (!OidIsValid(prm->ptype) && paramInfo->paramFetch != NULL)
           (*paramInfo->paramFetch)(paramInfo, thisParamId);

       if (OidIsValid(prm->ptype) && prm->tabInfo != NULL &&
           prm->tabInfo->tableOfIndex != NULL && OidIsValid(prm->tabInfo->tableOfIndexType)) {
           /* safety check in case hook did something unexpected */
           if (prm->ptype != execTableOfIndexInfo->paramtype)
               ereport(ERROR,
                       (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("type of parameter %d (%s) does not match that when preparing the plan (%s)",
                               thisParamId,
                               format_type_be(prm->ptype),
                               format_type_be(execTableOfIndexInfo->paramtype))));
           execTableOfIndexInfo->tableOfIndexType = prm->tabInfo->tableOfIndexType;
           execTableOfIndexInfo->isnestedtable = prm->tabInfo->isnestedtable;
           execTableOfIndexInfo->tableOfLayers = prm->tabInfo->tableOfLayers;
           execTableOfIndexInfo->tableOfIndex = prm->tabInfo->tableOfIndex;
           return true;
       }
   }
   return false;
}


/* ----------------------------------------------------------------
*		ExecEvalOper / ExecEvalFunc support routines
* ----------------------------------------------------------------
*/
/*
*		GetAttributeByName
*		GetAttributeByNum
*
*		These functions return the value of the requested attribute
*		out of the given tuple Datum.
*		C functions which take a tuple as an argument are expected
*		to use these.  Ex: overpaid(EMP) might call GetAttributeByNum().
*		Note: these are actually rather slow because they do a typcache
*		lookup on each call.
*/
Datum GetAttributeByNum(HeapTupleHeader tuple, AttrNumber attrno, bool* isNull)
{
   Datum result;
   Oid tupType;
   int32 tupTypmod;
   TupleDesc tupDesc;
   HeapTupleData tmptup;

   if (!AttributeNumberIsValid(attrno))
       ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("invalid attribute number %d", attrno)));

   if (isNull == NULL)
       ereport(ERROR,
               (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("a NULL isNull pointer was passed when get attribute by number.")));

   if (tuple == NULL) {
       /* Kinda bogus but compatible with old behavior... */
       *isNull = true;
       return (Datum)0;
   }

   tupType = HeapTupleHeaderGetTypeId(tuple);
   tupTypmod = HeapTupleHeaderGetTypMod(tuple);
   tupDesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

   /*
    * heap_getattr needs a HeapTuple not a bare HeapTupleHeader.  We set all
    * the fields in the struct just in case user tries to inspect system
    * columns.
    */
   tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
   ItemPointerSetInvalid(&(tmptup.t_self));
   tmptup.t_tableOid = InvalidOid;
   tmptup.t_bucketId = InvalidBktId;
   HeapTupleSetZeroBase(&tmptup);
#ifdef PGXC
   tmptup.t_xc_node_id = 0;
#endif
   tmptup.t_data = tuple;

   if (attrno == -3 || attrno == -5) {
       elog(WARNING, "system attribute xmin or xmax,  the results about this attribute are untrustworthy.");
   }
   result = tableam_tops_tuple_getattr(&tmptup, attrno, tupDesc, isNull);

   ReleaseTupleDesc(tupDesc);

   return result;
}

Datum GetAttributeByName(HeapTupleHeader tuple, const char* attname, bool* isNull)
{
   AttrNumber attrno;
   Datum result;
   Oid tupType;
   int32 tupTypmod;
   TupleDesc tupDesc;
   HeapTupleData tmptup;
   int i;

   if (attname == NULL)
       ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid null attribute name")));

   if (isNull == NULL)
       ereport(ERROR,
               (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("a NULL isNull pointer was passed when get attribute by name.")));

   if (tuple == NULL) {
       /* Kinda bogus but compatible with old behavior... */
       *isNull = true;
       return (Datum)0;
   }

   tupType = HeapTupleHeaderGetTypeId(tuple);
   tupTypmod = HeapTupleHeaderGetTypMod(tuple);
   tupDesc = lookup_rowtype_tupdesc(tupType, tupTypmod);

   attrno = InvalidAttrNumber;
   for (i = 0; i < tupDesc->natts; i++) {
       if (namestrcmp(&(tupDesc->attrs[i].attname), attname) == 0) {
           attrno = tupDesc->attrs[i].attnum;
           break;
       }
   }

   if (attrno == InvalidAttrNumber)
       ereport(ERROR,
               (errcode(ERRCODE_INVALID_ATTRIBUTE),
                errmodule(MOD_EXECUTOR),
                errmsg("attribute \"%s\" does not exist", attname)));

   /*
    * heap_getattr needs a HeapTuple not a bare HeapTupleHeader.  We set all
    * the fields in the struct just in case user tries to inspect system
    * columns.
    */
   tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
   ItemPointerSetInvalid(&(tmptup.t_self));
   tmptup.t_tableOid = InvalidOid;
   tmptup.t_bucketId = InvalidBktId;
   HeapTupleSetZeroBase(&tmptup);
#ifdef PGXC
   tmptup.t_xc_node_id = 0;
#endif
   tmptup.t_data = tuple;

   if (attrno == -3 || attrno == -5) {
       elog(WARNING, "system attribute \"%s\",  the results about this attribute are untrustworthy.", attname);
   }
   result = tableam_tops_tuple_getattr(&tmptup, attrno, tupDesc, isNull);

   ReleaseTupleDesc(tupDesc);

   return result;
}

/*
* Find the real function return type based on the actual func args' types.
* @inPara arg_num: the number of func's args.
* @inPara actual_arg_types: the type array of actual func args'.
* @inPara fcache: the FuncExprState of this functin.
* @return Oid: the real func return type.
*/
static Oid getRealFuncRetype(int arg_num, Oid* actual_arg_types, FuncExprState* fcache)
{
   Oid funcid = fcache->func.fn_oid;
   Oid rettype = fcache->func.fn_rettype;

   /* Find the declared arg types in PROCOID by funcid. */
   HeapTuple proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
   if (!HeapTupleIsValid(proctup))
       ereport(ERROR,
               (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmodule(MOD_EXECUTOR),
                errmsg("cache lookup failed for function %u", funcid)));

   oidvector* proargs = ProcedureGetArgTypes(proctup);
   Oid* declared_arg_types = proargs->values;

   /* Find the real return type based on the declared arg types and actual arg types.*/
   rettype = enforce_generic_type_consistency(actual_arg_types, declared_arg_types, arg_num, rettype, false);

   ReleaseSysCache(proctup);
   return rettype;
}

/*
* Check whether the function is a set function supported by the vector engine.
*/
static bool isVectorEngineSupportSetFunc(Oid funcid)
{
   switch (funcid) {
       case OID_REGEXP_SPLIT_TO_TABLE:                // regexp_split_to_table
       case OID_REGEXP_SPLIT_TO_TABLE_NO_FLAG:        // regexp_split_to_table
       case OID_ARRAY_UNNEST:                         // unnest
           return true;
           break;
       default:
           return false;
           break;
   }
}

/*
* init_fcache - initialize a FuncExprState node during first use
*/
template <bool vectorized>
static void init_fcache(
   Oid foid, Oid input_collation, FuncExprState* fcache, MemoryContext fcacheCxt, bool allowSRF, bool needDescForSRF)
{
   AclResult aclresult;
   MemoryContext oldcontext;
    Form_pg_proc procStruct;
    HeapTuple procTup;
    Oid definer = GetUserId();

    if (u_sess->attr.attr_sql.sql_compatibility ==  B_FORMAT)
    {
        /* Get the function's pg_proc entry */
        procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(foid));
        if (!HeapTupleIsValid(procTup)) {
            ereport(ERROR, (errmodule(MOD_EXECUTOR), errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for function %u", foid)));
        }
        procStruct = (Form_pg_proc)GETSTRUCT(procTup);
        if (procStruct->prosecdef)
            definer = procStruct->proowner;
        ReleaseSysCache(procTup);
    }

   /* Check permission to call function */
   aclresult = pg_proc_aclcheck(foid, definer, ACL_EXECUTE);
   if (aclresult != ACLCHECK_OK)
       aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(foid));

   /*
    * Safety check on nargs.  Under normal circumstances this should never
    * fail, as parser should check sooner.  But possibly it might fail if
    * server has been compiled with FUNC_MAX_ARGS smaller than some functions
    * declared in pg_proc?
    */
   if (list_length(fcache->args) > FUNC_MAX_ARGS)
       ereport(ERROR,
               (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                errmsg_plural("cannot pass more than %d argument to a function",
                              "cannot pass more than %d arguments to a function",
                              FUNC_MAX_ARGS,
                              FUNC_MAX_ARGS)));

   /* Set up the primary fmgr lookup information */
   fmgr_info_cxt(foid, &(fcache->func), fcacheCxt);
   fmgr_info_set_expr((Node*)fcache->xprstate.expr, &(fcache->func));

    /* palloc args in fcache's context  */
    oldcontext = MemoryContextSwitchTo(fcacheCxt);

    /* If function returns set, check if that's allowed by caller */
    if (fcache->xprstate.is_flt_frame) {
        if (fcache->func.fn_retset && !allowSRF)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("set-valued function called in context that cannot accept a set")));

        /* Otherwise, ExecInitExpr should have marked the fcache correctly */
        Assert(fcache->func.fn_retset == fcache->funcReturnsSet);
    }

    /* Initialize the function call parameter struct as well */
    if (vectorized)
        InitVecFunctionCallInfoData(
            &fcache->fcinfo_data, &(fcache->func), list_length(fcache->args), input_collation, NULL, NULL);
    else
        InitFunctionCallInfoData(
            fcache->fcinfo_data, &(fcache->func), list_length(fcache->args), input_collation, NULL, NULL);

   if (vectorized) {
       int nargs = list_length(fcache->args);
       ListCell* cell = NULL;
       GenericFunRuntime* genericRuntime = NULL;
       errno_t rc;

       if (fcache->fcinfo_data.flinfo->genericRuntime == NULL) {
           genericRuntime = (GenericFunRuntime*)palloc0(sizeof(GenericFunRuntime));
           InitGenericFunRuntimeInfo(*genericRuntime, nargs);
           fcache->fcinfo_data.flinfo->genericRuntime = genericRuntime;
       } else {
           genericRuntime = fcache->fcinfo_data.flinfo->genericRuntime;

           /* if internalFinfo is not null, release the internalFinfo's memory and set the pointer to null */
           if (genericRuntime->internalFinfo != NULL) {
               FreeFunctionCallInfoData(*(genericRuntime->internalFinfo));
               genericRuntime->internalFinfo = NULL;
           }

           /* reset the memory for reuse */
           rc = memset_s(genericRuntime->args,
                         sizeof(GenericFunRuntimeArg) * genericRuntime->compacity,
                         0,
                         sizeof(GenericFunRuntimeArg) * genericRuntime->compacity);
           securec_check(rc, "\0", "\0");

           rc = memset_s(genericRuntime->inputargs,
                         sizeof(Datum) * genericRuntime->compacity,
                         0,
                         sizeof(Datum) * genericRuntime->compacity);
           securec_check(rc, "\0", "\0");

           rc = memset_s(genericRuntime->nulls,
                         sizeof(bool) * genericRuntime->compacity,
                         0,
                         sizeof(bool) * genericRuntime->compacity);
           securec_check(rc, "\0", "\0");

           /* we have to adjust the GenericFunRuntimeArg when
            * a) nargs is larger than genericRuntime->compacity, which means the allocated memory is not enough to hold
            *    all the argumnets here, we should enlarge the memory.
            * b) nargs is less than VECTOR_GENERIC_FUNCTION_PREALLOCED_ARGS while the allocated memory is much more
            * than that. As VECTOR_GENERIC_FUNCTION_PREALLOCED_ARGS is already enough in most senerios, we should
            * reduce the memory.
            *
            * NOTE: To avoid memory wasting and memory fragments, we free and initilized a new GenericFunRuntimeArg.
            */
           if (unlikely(nargs > genericRuntime->compacity) ||
               (unlikely(genericRuntime->compacity > VECTOR_GENERIC_FUNCTION_PREALLOCED_ARGS) &&
                nargs <= VECTOR_GENERIC_FUNCTION_PREALLOCED_ARGS)) {
               FreeGenericFunRuntimeInfo(*genericRuntime);
               InitGenericFunRuntimeInfo(*genericRuntime, nargs);
           }
       }

       ScalarVector* pVector = New(CurrentMemoryContext) ScalarVector[nargs];

       int i = 0;
       if (fcache->args && fcache->args->length > 0) {
           Oid* actual_arg_types = (Oid*)palloc0(fcache->args->length * sizeof(Oid));

           foreach (cell, fcache->args) {
               ExprState* argstate = (ExprState*)lfirst(cell);
               Oid funcrettype;
               TupleDesc tupdesc;
               ScalarDesc desc;

               (void)get_expr_result_type((Node*)argstate->expr, &funcrettype, &tupdesc);

               desc.typeId = funcrettype;
               desc.encoded = COL_IS_ENCODE(funcrettype);
               fcache->fcinfo_data.flinfo->genericRuntime->args[i].argType = funcrettype;

               pVector[i].init(CurrentMemoryContext, desc);
               /* Record the real arg types from sub functions. */
               actual_arg_types[i] = funcrettype;
               i++;
           }

           /* Find the real return type for func with return type like ANYELEMENT. */
           fcache->fcinfo_data.flinfo->fn_rettype = getRealFuncRetype(i, actual_arg_types, fcache);
           pfree_ext(actual_arg_types);
       }
       fcache->fcinfo_data.argVector = pVector;
   }
   (void)MemoryContextSwitchTo(oldcontext);

   /* If function returns set, check if that's allowed by caller */
   if (fcache->xprstate.is_flt_frame) {
       if (fcache->func.fn_retset && !allowSRF)
           ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("set-valued function called in context that cannot accept a set")));
       /* Otherwise, ExecInitExpr should have marked the fcache correctly */
       Assert(fcache->func.fn_retset == fcache->funcReturnsSet);
   }

   if (vectorized) {
       if (fcache->func.fn_retset == true) {
           if (!isVectorEngineSupportSetFunc(fcache->func.fn_oid)) {
               ereport(ERROR,
                       (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmodule(MOD_EXECUTOR),
                        errmsg("set-return function not supported in vector eninge")));
           }
       }
       fcache->funcResultDesc = NULL;
   } else {
       /* If function returns set, prepare expected tuple descriptor */
       if (fcache->func.fn_retset && needDescForSRF) {
           TypeFuncClass functypclass;
           Oid funcrettype;
           TupleDesc tupdesc;
           MemoryContext oldmemcontext;

           functypclass = get_expr_result_type(fcache->func.fn_expr, &funcrettype, &tupdesc);

           /* Must save tupdesc in fcache's context */
           oldmemcontext = MemoryContextSwitchTo(fcacheCxt);

           if (functypclass == TYPEFUNC_COMPOSITE) {
               /* Composite data type, e.g. a table's row type */
               Assert(tupdesc);
               /* Must copy it out of typcache for safety */
               fcache->funcResultDesc = CreateTupleDescCopy(tupdesc);
               fcache->funcReturnsTuple = true;
           } else if (functypclass == TYPEFUNC_SCALAR) {
               /* Base data type, i.e. scalar */
               tupdesc = CreateTemplateTupleDesc(1, false, TableAmHeap);
               TupleDescInitEntry(tupdesc, (AttrNumber)1, NULL, funcrettype, -1, 0);
               fcache->funcResultDesc = tupdesc;
               fcache->funcReturnsTuple = false;
           } else if (functypclass == TYPEFUNC_RECORD) {
               /* This will work if function doesn't need an expectedDesc */
               fcache->funcResultDesc = NULL;
               fcache->funcReturnsTuple = true;
           } else {
               /* Else, we will fail if function needs an expectedDesc */
               fcache->funcResultDesc = NULL;
           }

           MemoryContextSwitchTo(oldmemcontext);
       } else
           fcache->funcResultDesc = NULL;
   }

   /* Initialize additional state */
   fcache->funcResultStore = NULL;
   fcache->funcResultSlot = NULL;
   fcache->setArgsValid = false;
   fcache->shutdown_reg = false;
   fcache->setArgByVal = false;
   if(fcache->xprstate.is_flt_frame){
        fcache->is_plpgsql_func_with_outparam = is_function_with_plpgsql_language_and_outparam(fcache->func.fn_oid);
        fcache->has_refcursor = func_has_refcursor_args(fcache->func.fn_oid, &fcache->fcinfo_data);
   }

}

void initVectorFcache(Oid foid, Oid input_collation, FuncExprState* fcache, MemoryContext fcacheCxt)
{
   init_fcache<true>(foid, input_collation, fcache, fcacheCxt, false, false);
}

/*
* callback function in case a FuncExpr returning a set needs to be shut down
* before it has been run to completion
*/
extern void ShutdownFuncExpr(Datum arg)
{
   FuncExprState* fcache = (FuncExprState*)DatumGetPointer(arg);

   /* If we have a slot, make sure it's let go of any tuplestore pointer */
   if (fcache->funcResultSlot)
       (void)ExecClearTuple(fcache->funcResultSlot);

   /* Release any open tuplestore */
   if (fcache->funcResultStore)
       tuplestore_end(fcache->funcResultStore);
   fcache->funcResultStore = NULL;

   /* Clear any active set-argument state */
   fcache->setArgsValid = false;
   fcache->setArgByVal = false;

   /* execUtils will deregister the callback... */
   fcache->shutdown_reg = false;
}

/*
* get_cached_rowtype: utility function to lookup a rowtype tupdesc
*
* type_id, typmod: identity of the rowtype
* cache_field: where to cache the TupleDesc pointer in expression state node
*		(field must be initialized to NULL)
* econtext: expression context we are executing in
*
* NOTE: because the shutdown callback will be called during plan rescan,
* must be prepared to re-do this during any node execution; cannot call
* just once during expression initialization
*/
static TupleDesc get_cached_rowtype(Oid type_id, int32 typmod, TupleDesc* cache_field, ExprContext* econtext)
{
   TupleDesc tupDesc = *cache_field;

   /* Do lookup if no cached value or if requested type changed */
   if (tupDesc == NULL || type_id != tupDesc->tdtypeid || typmod != tupDesc->tdtypmod) {
       tupDesc = lookup_rowtype_tupdesc(type_id, typmod);

       if (*cache_field) {
           /* Release old tupdesc; but callback is already registered */
           ReleaseTupleDesc(*cache_field);
       } else {
           /* Need to register shutdown callback to release tupdesc */
           RegisterExprContextCallback(econtext, ShutdownTupleDescRef, PointerGetDatum(cache_field));
       }
       *cache_field = tupDesc;
   }
   return tupDesc;
}

/*
* Callback function to release a tupdesc refcount at expression tree shutdown
*/
void ShutdownTupleDescRef(Datum arg)
{
   TupleDesc* cache_field = (TupleDesc*)DatumGetPointer(arg);

   if (*cache_field)
       ReleaseTupleDesc(*cache_field);
   *cache_field = NULL;
}

/*
* Evaluate arguments for a function.
*/
template <bool has_refcursor>
static ExprDoneCond ExecEvalFuncArgs(
   FunctionCallInfo fcinfo, List* argList, ExprContext* econtext, int* plpgsql_var_dno)
{
   ExprDoneCond argIsDone;
   int i;
   ListCell* arg = NULL;

   argIsDone = ExprSingleResult; /* default assumption */

   i = 0;
   econtext->is_cursor = false;
   u_sess->plsql_cxt.func_tableof_index = NIL;
   bool is_have_huge_clob = false;
   foreach (arg, argList) {
       ExprState* argstate = (ExprState*)lfirst(arg);
       ExprDoneCond thisArgIsDone;

       if (has_refcursor && argstate->resultType == REFCURSOROID)
           econtext->is_cursor = true;
       fcinfo->arg[i] = ExecEvalExpr(argstate, econtext, &fcinfo->argnull[i], &thisArgIsDone);
       ExecTableOfIndexInfo execTableOfIndexInfo;
       initExecTableOfIndexInfo(&execTableOfIndexInfo, econtext);
       ExecEvalParamExternTableOfIndex((Node*)argstate->expr, &execTableOfIndexInfo);
       if (execTableOfIndexInfo.tableOfIndex != NULL) {
           MemoryContext oldCxt = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
           PLpgSQL_func_tableof_index* func_tableof =
               (PLpgSQL_func_tableof_index*)palloc0(sizeof(PLpgSQL_func_tableof_index));
           func_tableof->varno = i;
           func_tableof->tableOfIndexType = execTableOfIndexInfo.tableOfIndexType;
           func_tableof->tableOfIndex = copyTableOfIndex(execTableOfIndexInfo.tableOfIndex);
           u_sess->plsql_cxt.func_tableof_index = lappend(u_sess->plsql_cxt.func_tableof_index, func_tableof);
           MemoryContextSwitchTo(oldCxt);
       }

       if (has_refcursor && econtext->is_cursor && plpgsql_var_dno != NULL) {
           plpgsql_var_dno[i] = econtext->dno;
           CopyCursorInfoData(&fcinfo->refcursor_data.argCursor[i], &econtext->cursor_data);
       }
       fcinfo->argTypes[i] = argstate->resultType;
       econtext->is_cursor = false;
       if (is_huge_clob(fcinfo->argTypes[i], fcinfo->argnull[i], fcinfo->arg[i])) {
           is_have_huge_clob = true;
       }

       if (thisArgIsDone != ExprSingleResult) {
           /*
            * We allow only one argument to have a set value; we'd need much
            * more complexity to keep track of multiple set arguments (cf.
            * ExecTargetList) and it doesn't seem worth it.
            */
           if (argIsDone != ExprSingleResult)
               ereport(ERROR,
                       (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("functions and operators can take at most one set argument")));
           argIsDone = thisArgIsDone;
       }
       i++;
   }
   check_huge_clob_paramter(fcinfo, is_have_huge_clob);

   Assert(i == fcinfo->nargs);

   return argIsDone;
}

/*
*		ExecPrepareTuplestoreResult
*
* Subroutine for ExecMakeFunctionResult: prepare to extract rows from a
* tuplestore function result.	We must set up a funcResultSlot (unless
* already done in a previous call cycle) and verify that the function
* returned the expected tuple descriptor.
*/
extern void ExecPrepareTuplestoreResult(
   FuncExprState* fcache, ExprContext* econtext, Tuplestorestate* resultStore, TupleDesc resultDesc)
{
   fcache->funcResultStore = resultStore;

   if (fcache->funcResultSlot == NULL) {
       /* Create a slot so we can read data out of the tuplestore */
       TupleDesc slotDesc;
       MemoryContext oldcontext;

       oldcontext = MemoryContextSwitchTo(fcache->func.fn_mcxt);

       /*
        * If we were not able to determine the result rowtype from context,
        * and the function didn't return a tupdesc, we have to fail.
        */
       if (fcache->funcResultDesc)
           slotDesc = fcache->funcResultDesc;
       else if (resultDesc) {
           /* don't assume resultDesc is long-lived */
           slotDesc = CreateTupleDescCopy(resultDesc);
       } else {
           ereport(ERROR,
                   (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("function returning setof record called in context that cannot accept type record")));
           slotDesc = NULL; /* keep compiler quiet */
       }

       fcache->funcResultSlot = MakeSingleTupleTableSlot(slotDesc);
       MemoryContextSwitchTo(oldcontext);
   }

   /*
    * If function provided a tupdesc, cross-check it.	We only really need to
    * do this for functions returning RECORD, but might as well do it always.
    */
   if (resultDesc) {
       if (fcache->funcResultDesc)
           tupledesc_match(fcache->funcResultDesc, resultDesc);

       /*
        * If it is a dynamically-allocated TupleDesc, free it: it is
        * typically allocated in a per-query context, so we must avoid
        * leaking it across multiple usages.
        */
       if (resultDesc->tdrefcount == -1)
           FreeTupleDesc(resultDesc);
   }

   /* Register cleanup callback if we didn't already */
   if (!fcache->shutdown_reg) {
       RegisterExprContextCallback(econtext, ShutdownFuncExpr, PointerGetDatum(fcache));
       fcache->shutdown_reg = true;
   }
}

/*
* Check that function result tuple type (src_tupdesc) matches or can
* be considered to match what the query expects (dst_tupdesc). If
* they don't match, ereport.
*
* We really only care about number of attributes and data type.
* Also, we can ignore type mismatch on columns that are dropped in the
* destination type, so long as the physical storage matches.  This is
* helpful in some cases involving out-of-date cached plans.
*/
static void tupledesc_match(TupleDesc dst_tupdesc, TupleDesc src_tupdesc)
{
   int i;

   if (dst_tupdesc->natts != src_tupdesc->natts)
       ereport(ERROR,
               (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("function return row and query-specified return row do not match"),
                errdetail_plural("Returned row contains %d attribute, but query expects %d.",
                                 "Returned row contains %d attributes, but query expects %d.",
                                 src_tupdesc->natts,
                                 src_tupdesc->natts,
                                 dst_tupdesc->natts)));

   for (i = 0; i < dst_tupdesc->natts; i++) {
       Form_pg_attribute dattr = &dst_tupdesc->attrs[i];
       Form_pg_attribute sattr = &src_tupdesc->attrs[i];

       if (IsBinaryCoercible(sattr->atttypid, dattr->atttypid))
           continue; /* no worries */
       if (!dattr->attisdropped)
           ereport(ERROR,
                   (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("function return row and query-specified return row do not match"),
                    errdetail("Returned type %s at ordinal position %d, but query expects %s.",
                              format_type_be(sattr->atttypid),
                              i + 1,
                              format_type_be(dattr->atttypid))));

       if (dattr->attlen != sattr->attlen || dattr->attalign != sattr->attalign)
           ereport(ERROR,
                   (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("function return row and query-specified return row do not match"),
                    errdetail("Physical storage mismatch on dropped attribute at ordinal position %d.", i + 1)));
   }
}

void set_result_for_plpgsql_language_function_with_outparam(FuncExprState *fcache, Datum *result, bool *isNull)
{
    if (!IsA(fcache->xprstate.expr, FuncExpr)) {
        return;
    }
    FuncExpr *func = (FuncExpr *)fcache->xprstate.expr;
    if (!is_function_with_plpgsql_language_and_outparam(func->funcid)) {
        return;
    }
    HeapTupleHeader td = DatumGetHeapTupleHeader(*result);
    TupleDesc tupdesc;
    PG_TRY();
    {
        tupdesc = lookup_rowtype_tupdesc_copy(HeapTupleHeaderGetTypeId(td), HeapTupleHeaderGetTypMod(td));
    }
    PG_CATCH();
    {
        int ecode = geterrcode();
        if (ecode == ERRCODE_CACHE_LOOKUP_FAILED) {
            ereport(ERROR, (errcode(ERRCODE_PLPGSQL_ERROR), errmodule(MOD_PLSQL),
                           errmsg("tuple is null"),
                           errdetail("it may be because change guc behavior_compat_options in one session")));
        } else {
            PG_RE_THROW();
        }
    }
    PG_END_TRY();
    HeapTupleData tup;
    tup.t_len = HeapTupleHeaderGetDatumLength(td);
    tup.t_data = td;
    Datum *values = (Datum *)palloc(sizeof(Datum) * tupdesc->natts);
    bool *nulls = (bool *)palloc(sizeof(bool) * tupdesc->natts);
    heap_deform_tuple(&tup, tupdesc, values, nulls);
    *result = values[0];
    *isNull = nulls[0];
    pfree(values);
    pfree(nulls);
}

bool ExecSetArgIsByValue(FunctionCallInfo fcinfo)
{
    for (int i = 0; i < fcinfo->nargs; i++) {
        if (!fcinfo->argnull[i] && !get_typbyval(fcinfo->argTypes[i])) {
            return false;
        }
    }
    return true;
}

/*
*		ExecMakeFunctionResult
*
* Evaluate the arguments to a function and then the function itself.
* init_fcache is presumed already run on the FuncExprState.
*
* This function handles the most general case, wherein the function or
* one of its arguments can return a set.
*
* Note: This function use template parameter can compile different function,
* reduce the assembly instructions so as to improve performance.
*
* Template parameter:
* @bool has_cursor_return - need store out-args cursor info.
* @bool has_refcursor - need store in-args cursor info.
* @bool isSetReturnFunc - indicate function returns a set.
*/
template <bool has_refcursor, bool has_cursor_return, bool isSetReturnFunc>
static Datum ExecMakeFunctionResult(FuncExprState* fcache, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   List* arguments = NIL;
   Datum result;
   FunctionCallInfo fcinfo;
   PgStat_FunctionCallUsage fcusage;
   ReturnSetInfo rsinfo; /* for functions returning sets */
   ExprDoneCond argDone;
   bool hasSetArg = false;
   int i;
   int* var_dno = NULL;
   int func_encoding = PG_INVALID_ENCODING;
   int db_encoding = PG_INVALID_ENCODING;

   econtext->plpgsql_estate = plpgsql_estate;
   plpgsql_estate = NULL;

restart:

   /* Guard against stack overflow due to overly complex expressions */
   check_stack_depth();

   if (fcache->xprstate.is_flt_frame) {
        /*
        * Initialize function cache if first time through.  The expression node
        * could be either a FuncExpr or an OpExpr.
        */
        if (fcache->func.fn_oid == InvalidOid) {
            if (IsA(fcache->xprstate.expr, FuncExpr)) {
                FuncExpr *func = (FuncExpr *)fcache->xprstate.expr;

                init_fcache<false>(func->funcid, func->inputcollid, fcache, econtext->ecxt_per_query_memory, true, true);
            } else if (IsA(fcache->xprstate.expr, OpExpr)) {
                OpExpr *op = (OpExpr *)fcache->xprstate.expr;

                init_fcache<false>(op->opfuncid, op->inputcollid, fcache, econtext->ecxt_per_query_memory, true, true);
            } else
                elog(ERROR, "unrecognized node type: %d", (int)nodeTag(fcache->xprstate.expr));
        }
    }
   /*
    * If a previous call of the function returned a set result in the form of
    * a tuplestore, continue reading rows from the tuplestore until it's
    * empty.
    */
   if (fcache->funcResultStore) {
       /* it was provided before ... */
       if (unlikely(isDone == NULL)) {
           ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                           errmsg("set-valued function called in context that cannot accept a set")));
       }
       econtext->hasSetResultStore = true;
        if (tuplestore_gettupleslot(fcache->funcResultStore, true, false, fcache->funcResultSlot)) {
            *isDone = ExprMultipleResult;
            if (fcache->funcReturnsTuple) {
                /* We must return the whole tuple as a Datum. */
                *isNull = false;
                return ExecFetchSlotTupleDatum(fcache->funcResultSlot);
            } else {
                /* Extract the first column and return it as a scalar. */
                Assert(fcache->funcResultSlot != NULL);
                /* Get the Table Accessor Method*/
                return tableam_tslot_getattr(fcache->funcResultSlot, 1, isNull);
            }
        }
        /* Exhausted the tuplestore, so clean up */
        tuplestore_end(fcache->funcResultStore);
        fcache->funcResultStore = NULL;
        /* We are done unless there was a set-valued argument */
        if (!fcache->setHasSetArg) {
            *isDone = ExprEndResult;
            *isNull = true;
            return (Datum)0;
        }
        /* If there was, continue evaluating the argument values */
        Assert(!fcache->setArgsValid);
    }

   /*
    * arguments is a list of expressions to evaluate before passing to the
    * function manager.  We skip the evaluation if it was already done in the
    * previous call (ie, we are continuing the evaluation of a set-valued
    * function).  Otherwise, collect the current argument values into fcinfo.
    */
   fcinfo = &fcache->fcinfo_data;

   if (has_cursor_return) {
       /* init returnCursor to store out-args cursor info on ExprContext*/
       fcinfo->refcursor_data.returnCursor =
           (Cursor_Data*)palloc0(sizeof(Cursor_Data) * fcinfo->refcursor_data.return_number);
   } else {
       fcinfo->refcursor_data.returnCursor = NULL;
   }

   if (has_refcursor) {
       /* init argCursor to store in-args cursor info on ExprContext*/
       fcinfo->refcursor_data.argCursor = (Cursor_Data*)palloc0(sizeof(Cursor_Data) * fcinfo->nargs);
       var_dno = (int*)palloc0(sizeof(int) * fcinfo->nargs);
       for (i = 0; i < fcinfo->nargs; i++) {
           var_dno[i] = -1;
       }
   }

   arguments = fcache->args;
   if (!fcache->setArgsValid) {
       if (has_refcursor)
           argDone = ExecEvalFuncArgs<true>(fcinfo, arguments, econtext, var_dno);
       else
           argDone = ExecEvalFuncArgs<false>(fcinfo, arguments, econtext);
       if (fcache->xprstate.is_flt_frame) {
            if (argDone != ExprSingleResult) {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context that cannot accept a set")));
                return (Datum)0;
            }
        } else {
            if (argDone == ExprEndResult) {
                /* input is an empty set, so return an empty set. */
                *isNull = true;
                if (isDone != NULL)
                    *isDone = ExprEndResult;
                else
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("set-valued function called in context that cannot accept a set")));
                return (Datum)0;
            }
        }
        if (fcache->xprstate.is_flt_frame) {
            hasSetArg = false;
        } else {
            hasSetArg = (argDone != ExprSingleResult);
        }
        fcache->setArgByVal = ExecSetArgIsByValue(fcinfo);
   } else {
       /* Re-use callinfo from previous evaluation */
       hasSetArg = fcache->setHasSetArg;
       /* Reset flag (we may set it again below) */
       fcache->setArgsValid = false;
   }

    if (DB_IS_CMPT(B_FORMAT)) {
        func_encoding = get_valid_charset_by_collation(fcinfo->fncollation);
        db_encoding = GetDatabaseEncoding();
    }

   /*
    * Now call the function, passing the evaluated parameter values.
    */
   if (fcache->func.fn_retset || hasSetArg) {
       /*
        * We need to return a set result.	Complain if caller not ready to
        * accept one.
        */
       if (isDone == NULL)
           ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("set-valued function called in context that cannot accept a set")));

       /*
        * Prepare a resultinfo node for communication.  If the function
        * doesn't itself return set, we don't pass the resultinfo to the
        * function, but we need to fill it in anyway for internal use.
        */
       if (fcache->func.fn_retset)
           fcinfo->resultinfo = (Node*)&rsinfo;
       rsinfo.type = T_ReturnSetInfo;
       rsinfo.econtext = econtext;
       rsinfo.expectedDesc = fcache->funcResultDesc;
       rsinfo.allowedModes = (int)(SFRM_ValuePerCall | SFRM_Materialize);
       /* note we do not set SFRM_Materialize_Random or _Preferred */
       rsinfo.returnMode = SFRM_ValuePerCall;
       /* isDone is filled below */
       rsinfo.setResult = NULL;
       rsinfo.setDesc = NULL;

       /*
        * This loop handles the situation where we have both a set argument
        * and a set-valued function.  Once we have exhausted the function's
        * value(s) for a particular argument value, we have to get the next
        * argument value and start the function over again. We might have to
        * do it more than once, if the function produces an empty result set
        * for a particular input value.
        */
       for (;;) {
           /*
            * If function is strict, and there are any NULL arguments, skip
            * calling the function (at least for this set of args).
            */
           bool callit = true;

           if (fcache->func.fn_strict) {
               for (i = 0; i < fcinfo->nargs; i++) {
                   if (fcinfo->argnull[i]) {
                       callit = false;
                       break;
                   }
               }
           }

           if (callit) {
               pgstat_init_function_usage(fcinfo, &fcusage);

               fcinfo->isnull = false;
               rsinfo.isDone = ExprSingleResult;

               if (func_encoding != db_encoding) {
                    DB_ENCODING_SWITCH_TO(func_encoding);
                    result = FunctionCallInvoke(fcinfo);
                    DB_ENCODING_SWITCH_BACK(db_encoding);
               } else {
                    result = FunctionCallInvoke(fcinfo);
               }
               if (AUDIT_SYSTEM_EXEC_ENABLED) {
                    audit_system_function(fcinfo, AUDIT_OK);
                }
                *isNull = fcinfo->isnull;
                *isDone = rsinfo.isDone;

               pgstat_end_function_usage(&fcusage, rsinfo.isDone != ExprMultipleResult);
           } else if (isSetReturnFunc) {
               /*
                * For a strict SRF, result for NULL is an empty set
                * If SRF is strict and has any NULL arguments, this SRF
                * need return empty set, so such rows were omitted entirely
                * from the result set.
                */
               result = (Datum)0;
               *isNull = true;
               *isDone = ExprEndResult;
           } else {
               /*
                * For a strict non-SRF, result for NULL is a NULL.
                * This branch in order to deal strict nested functions
                * like "select plain_function(set_returning_function(...))".
                * If some of the SRF outputs are NULL, and the plain function
                * is strict, we expect to get NULL results for such rows
                */
               result = (Datum)0;
               *isNull = true;
               *isDone = ExprSingleResult;
           }

           if (has_refcursor && econtext->plpgsql_estate != NULL) {
               PLpgSQL_execstate* estate = econtext->plpgsql_estate;
               /* copy in-args cursor option info */
               for (i = 0; i < fcinfo->nargs; i++) {
                   if (var_dno[i] >= 0) {
                       int dno = var_dno[i];
                       Cursor_Data* cursor_data = &fcinfo->refcursor_data.argCursor[i];
#ifdef USE_ASSERT_CHECKING
                       PLpgSQL_datum* datum = estate->datums[dno];
#endif
                       Assert(datum->dtype == PLPGSQL_DTYPE_VAR);
                       Assert(((PLpgSQL_var*)datum)->datatype->typoid == REFCURSOROID);

                       ExecCopyDataToDatum(estate->datums, dno, cursor_data);
                   }
               }

               if (fcinfo->refcursor_data.return_number > 0) {
                   /* copy function returns cursor option info.
                    * for simple expr in exec_eval_expr, we can not get the result type,
                    * so cursor_return_data mallocs here.
                    */
                   if (estate->cursor_return_data == NULL && estate->tuple_store_cxt != NULL) {
                       MemoryContext oldcontext = MemoryContextSwitchTo(estate->tuple_store_cxt);
                       estate->cursor_return_data =
                           (Cursor_Data*)palloc0(sizeof(Cursor_Data) * fcinfo->refcursor_data.return_number);
                       estate->cursor_return_numbers = fcinfo->refcursor_data.return_number;
                       (void)MemoryContextSwitchTo(oldcontext);
                   }

                   if (estate->cursor_return_data != NULL) {
                       for (i = 0; i < fcinfo->refcursor_data.return_number; i++) {
                           int rc = memcpy_s(&estate->cursor_return_data[i], sizeof(Cursor_Data),
                                             &fcinfo->refcursor_data.returnCursor[i], sizeof(Cursor_Data));
                           securec_check(rc, "\0", "\0");
                       }
                   }
               }
           }

           /* Which protocol does function want to use? */
           if (rsinfo.returnMode == SFRM_ValuePerCall) {
               if (*isDone != ExprEndResult) {
                   /*
                    * Got a result from current argument. If function itself
                    * returns set, save the current argument values to re-use
                    * on the next call.
                    */
                   if (fcache->func.fn_retset && *isDone == ExprMultipleResult) {
                       fcache->setHasSetArg = hasSetArg;
                       fcache->setArgsValid = true;
                        /* arg not by value, memory can not be reset */
                        if (!fcache->setArgByVal) {
                            econtext->hasSetResultStore = true;
                        }
                       /* Register cleanup callback if we didn't already */
                       if (!fcache->shutdown_reg) {
                           RegisterExprContextCallback(econtext, ShutdownFuncExpr, PointerGetDatum(fcache));
                           fcache->shutdown_reg = true;
                       }
                   }

                   /*
                    * Make sure we say we are returning a set, even if the
                    * function itself doesn't return sets.
                    */
                   if (hasSetArg) {
                       *isDone = ExprMultipleResult;
                   }
                   break;
               }
           } else if (rsinfo.returnMode == SFRM_Materialize) {
               /* check we're on the same page as the function author */
               if (rsinfo.isDone != ExprSingleResult)
                   ereport(ERROR,  (errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
                                   errmsg("table-function protocol for materialize mode was not followed")));
               if (rsinfo.setResult != NULL) {
                   /* prepare to return values from the tuplestore */
                   ExecPrepareTuplestoreResult(fcache, econtext, rsinfo.setResult, rsinfo.setDesc);
                   /* remember whether we had set arguments */
                   fcache->setHasSetArg = hasSetArg;
                   /* loop back to top to start returning from tuplestore */
                   goto restart;
               }
               /* if setResult was left null, treat it as empty set */
               *isDone = ExprEndResult;
               *isNull = true;
               result = (Datum)0;
           } else {
               ereport(ERROR, (errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
                               errmsg("unrecognized table-function returnMode: %d", (int)rsinfo.returnMode)));
           }

           /* Else, done with this argument */
           if (!hasSetArg) {
               break; /* input not a set, so done */
           }

           /* Re-eval args to get the next element of the input set */
           if (has_refcursor) {
               argDone = ExecEvalFuncArgs<true>(fcinfo, arguments, econtext, var_dno);
           } else {
               argDone = ExecEvalFuncArgs<false>(fcinfo, arguments, econtext);
           }

           if (argDone != ExprMultipleResult) {
               /* End of argument set, so we're done. */
               *isNull = true;
               *isDone = ExprEndResult;
               result = (Datum)0;
               break;
           }

           /*
            * If we reach here, loop around to run the function on the new
            * argument.
            */
       }
   } else {
       /*
        * Non-set case: much easier.
        *
        * In common cases, this code path is unreachable because we'd have
        * selected ExecMakeFunctionResultNoSets instead.  However, it's
        * possible to get here if an argument sometimes produces set results
        * and sometimes scalar results.  For example, a CASE expression might
        * call a set-returning function in only some of its arms.
        */
       if (isDone != NULL)
           *isDone = ExprSingleResult;

       /*
        * If function is strict, and there are any NULL arguments, skip
        * calling the function and return NULL.
        */
       if (fcache->func.fn_strict) {
           for (i = 0; i < fcinfo->nargs; i++) {
               if (fcinfo->argnull[i]) {
                   *isNull = true;
                   return (Datum)0;
               }
           }
       }

       pgstat_init_function_usage(fcinfo, &fcusage);

       fcinfo->isnull = false;
       if (func_encoding != db_encoding) {
            DB_ENCODING_SWITCH_TO(func_encoding);
            result = FunctionCallInvoke(fcinfo);
            DB_ENCODING_SWITCH_BACK(db_encoding);
        } else {
            result = FunctionCallInvoke(fcinfo);
        }
       *isNull = fcinfo->isnull;

       pgstat_end_function_usage(&fcusage, true);
   }

   if (has_refcursor) {
       pfree_ext(fcinfo->refcursor_data.argCursor);
       pfree_ext(var_dno);
   }

   set_result_for_plpgsql_language_function_with_outparam(fcache, &result, isNull);

   return result;
}

/*
*		ExecMakeFunctionResultNoSets
*
* Simplified version of ExecMakeFunctionResult that can only handle
* non-set cases.  Hand-tuned for speed.
*
* Note: This function use template parameter can compile different function,
* reduce the assembly instructions so as to improve performance.
*
* Template parameter:
* @bool has_cursor_return - need store out-args cursor info.
* @bool has_refcursor - need store in-args cursor info.
*/
template <bool has_refcursor, bool has_cursor_return>
static Datum ExecMakeFunctionResultNoSets(
   FuncExprState* fcache, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   ListCell* arg = NULL;
   Datum result;
   FunctionCallInfo fcinfo;
   PgStat_FunctionCallUsage fcusage;
   int i;
   int* var_dno = NULL;

   FunctionScanState *node = NULL;
   FuncExpr *fexpr = NULL;

   bool savedIsSTP = u_sess->SPI_cxt.is_stp;
   bool savedProConfigIsSet = u_sess->SPI_cxt.is_proconfig_set;
   bool proIsProcedure = false;
   bool supportTranaction = false;
   bool is_have_huge_clob = false;
   int func_encoding = PG_INVALID_ENCODING;
   int db_encoding = PG_INVALID_ENCODING;

#ifdef ENABLE_MULTIPLE_NODES
   if (IS_PGXC_COORDINATOR && (t_thrd.proc->workingVersionNum  >= STP_SUPPORT_COMMIT_ROLLBACK)) {
       supportTranaction = true;
   }
#else
   supportTranaction = true;
#endif
   bool needResetErrMsg = (u_sess->SPI_cxt.forbidden_commit_rollback_err_msg[0] == '\0');

   /* Only allow commit at CN, therefore only need to set atomic and
    * relevant check at CN level.
    */
   if (supportTranaction && IsA(fcache->xprstate.expr, FuncExpr)) {
       fexpr = (FuncExpr *) fcache->xprstate.expr;
       node = makeNode(FunctionScanState);
       if (!u_sess->SPI_cxt.is_allow_commit_rollback) {
           node->atomic = true;
       }
       else if (IsAfterTriggerBegin()) {
           node->atomic = true;
           stp_set_commit_rollback_err_msg(STP_XACT_AFTER_TRIGGER_BEGIN);
       }
       /*
        * If proconfig is set we can't allow transaction commands because of the
        * way the GUC stacking works: The transaction boundary would have to pop
        * the proconfig setting off the stack.  That restriction could be lifted
        * by redesigning the GUC nesting mechanism a bit.
        */
       if (!fcache->prokind) {
           bool isNullSTP = false;
           HeapTuple tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(fexpr->funcid));
           if (!HeapTupleIsValid(tp)) {
               ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                               errmsg("cache lookup failed for function %u", fexpr->funcid)));
           }
           if (!heap_attisnull(tp, Anum_pg_proc_proconfig, NULL) || u_sess->SPI_cxt.is_proconfig_set) {
               u_sess->SPI_cxt.is_proconfig_set = true;
               node->atomic = true;
               stp_set_commit_rollback_err_msg(STP_XACT_GUC_IN_OPT_CLAUSE);
           }
           /* immutable or stable function should not support commit/rollback */
           bool isNullVolatile = false;
           Datum provolatile = SysCacheGetAttr(PROCOID, tp, Anum_pg_proc_provolatile, &isNullVolatile);
           if (!isNullVolatile && CharGetDatum(provolatile) != PROVOLATILE_VOLATILE) {
               node->atomic = true;
               stp_set_commit_rollback_err_msg(STP_XACT_IMMUTABLE);
           }

           Datum datum = SysCacheGetAttr(PROCOID, tp, Anum_pg_proc_prokind, &isNullSTP);
           proIsProcedure = PROC_IS_PRO(CharGetDatum(datum));
           if (proIsProcedure) {
               fcache->prokind = 'p';
           } else {
               fcache->prokind = 'f';
           }

           /* if proIsProcedure is ture means it was a stored procedure */
           u_sess->SPI_cxt.is_stp = savedIsSTP;
           ReleaseSysCache(tp);
       } else {
           proIsProcedure = PROC_IS_PRO(fcache->prokind);
           u_sess->SPI_cxt.is_stp = savedIsSTP;
       }
   }

   /* Guard against stack overflow due to overly complex expressions */
   check_stack_depth();

   if (isDone != NULL)
       *isDone = ExprSingleResult;

   econtext->plpgsql_estate = plpgsql_estate;
   plpgsql_estate = NULL;

   /* inlined, simplified version of ExecEvalFuncArgs */
   fcinfo = &fcache->fcinfo_data;

   /* init the number of arguments to a function*/
   InitFunctionCallInfoArgs(*fcinfo, list_length(fcache->args), 1);

   /* Only allow commit at CN, therefore need to set callcontext in CN only */
   if (supportTranaction) {
       fcinfo->context = (Node *)node;
   }

   if (econtext) {
        fcinfo->can_ignore = econtext->can_ignore || (econtext->ecxt_estate && econtext->ecxt_estate->es_plannedstmt &&
                 econtext->ecxt_estate->es_plannedstmt->hasIgnore);
    }

    /*
     * Incause of connet_by_root() and sys_connect_by_path() we need get the
     * current scan tuple slot so attach the econtext here
     *
     * NOTE: Have to revisit!! so I don't have better solution to handle the case
     *       where scantuple is available in built in funct
     */
    if (fcinfo->flinfo->fn_oid == CONNECT_BY_ROOT_FUNCOID ||
                fcinfo->flinfo->fn_oid == SYS_CONNECT_BY_PATH_FUNCOID) {
        fcinfo->swinfo.sw_econtext = (Node *)econtext;
        fcinfo->swinfo.sw_exprstate = (Node *)linitial(fcache->args);
        fcinfo->swinfo.sw_is_flt_frame = false;
    }

   if (has_cursor_return) {
       /* init returnCursor to store out-args cursor info on ExprContext*/
       fcinfo->refcursor_data.returnCursor =
           (Cursor_Data*)palloc0(sizeof(Cursor_Data) * fcinfo->refcursor_data.return_number);
   } else {
       fcinfo->refcursor_data.returnCursor = NULL;
   }

   if (has_refcursor) {
       /* init argCursor to store in-args cursor info on ExprContext */
       fcinfo->refcursor_data.argCursor = (Cursor_Data*)palloc0(sizeof(Cursor_Data) * fcinfo->nargs);
       var_dno = (int*)palloc0(sizeof(int) * fcinfo->nargs);
       for (i = 0; i < fcinfo->nargs; i++) {
           var_dno[i] = -1;
       }
   }

   i = 0;
   econtext->is_cursor = false;
   u_sess->plsql_cxt.func_tableof_index = NIL;
   foreach (arg, fcache->args) {
       ExprState* argstate = (ExprState*)lfirst(arg);

       fcinfo->argTypes[i] = argstate->resultType;
       if (has_refcursor && fcinfo->argTypes[i] == REFCURSOROID)
           econtext->is_cursor = true;
       fcinfo->arg[i] = ExecEvalExpr(argstate, econtext, &fcinfo->argnull[i], NULL);
       if (is_huge_clob(fcinfo->argTypes[i], fcinfo->argnull[i], fcinfo->arg[i])) {
           is_have_huge_clob = true;
       }
       ExecTableOfIndexInfo execTableOfIndexInfo;
       initExecTableOfIndexInfo(&execTableOfIndexInfo, econtext);
       ExecEvalParamExternTableOfIndex((Node*)argstate->expr, &execTableOfIndexInfo);
       if (execTableOfIndexInfo.tableOfIndex != NULL) {
           if (!IsTableOfFunc(fcache->func.fn_oid)) {
               MemoryContext oldCxt = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
               PLpgSQL_func_tableof_index* func_tableof =
                   (PLpgSQL_func_tableof_index*)palloc0(sizeof(PLpgSQL_func_tableof_index));
               func_tableof->varno = i;
               func_tableof->tableOfIndexType = execTableOfIndexInfo.tableOfIndexType;
               func_tableof->tableOfIndex = copyTableOfIndex(execTableOfIndexInfo.tableOfIndex);
               u_sess->plsql_cxt.func_tableof_index = lappend(u_sess->plsql_cxt.func_tableof_index, func_tableof);
               MemoryContextSwitchTo(oldCxt);
           }

           u_sess->SPI_cxt.cur_tableof_index->tableOfIndexType = execTableOfIndexInfo.tableOfIndexType;
           u_sess->SPI_cxt.cur_tableof_index->tableOfIndex = execTableOfIndexInfo.tableOfIndex;
           u_sess->SPI_cxt.cur_tableof_index->tableOfNestLayer = execTableOfIndexInfo.tableOfLayers;
           /* for nest table of output, save layer of this var tableOfGetNestLayer in ExecEvalArrayRef,
           or set to zero for get whole nest table. */
           u_sess->SPI_cxt.cur_tableof_index->tableOfGetNestLayer = -1;
       }

       if (has_refcursor && econtext->is_cursor) {
           var_dno[i] = econtext->dno;
           CopyCursorInfoData(&fcinfo->refcursor_data.argCursor[i], &econtext->cursor_data);
       }
       econtext->is_cursor = false;
       i++;
   }

   /*
    * If function is strict, and there are any NULL arguments, skip calling
    * the function and return NULL.
    */
   if (fcache->func.fn_strict) {
       while (--i >= 0) {
           if (fcinfo->argnull[i]) {
               *isNull = true;
               u_sess->SPI_cxt.is_stp = savedIsSTP;
               u_sess->SPI_cxt.is_proconfig_set = savedProConfigIsSet;
               if (needResetErrMsg) {
                   stp_reset_commit_rolback_err_msg();
               }
               return (Datum)0;
           }
       }
   }

   if (DB_IS_CMPT(B_FORMAT)) {
        func_encoding = get_valid_charset_by_collation(fcinfo->fncollation);
        db_encoding = GetDatabaseEncoding();
    }

   pgstat_init_function_usage(fcinfo, &fcusage);

   fcinfo->isnull = false;
   check_huge_clob_paramter(fcinfo, is_have_huge_clob);
   if (u_sess->instr_cxt.global_instr != NULL && fcinfo->flinfo->fn_addr == plpgsql_call_handler) {
       StreamInstrumentation* save_global_instr = u_sess->instr_cxt.global_instr;
       u_sess->instr_cxt.global_instr = NULL;
        if (func_encoding != db_encoding) {
            DB_ENCODING_SWITCH_TO(func_encoding);
            result = FunctionCallInvoke(fcinfo);   // node will be free at here or else;
            DB_ENCODING_SWITCH_BACK(db_encoding);
        } else {
            result = FunctionCallInvoke(fcinfo);   // node will be free at here or else;
        }
       u_sess->instr_cxt.global_instr = save_global_instr;
   } else {
       if (fcinfo->argTypes[0] == CLOBOID && fcinfo->argTypes[1] == CLOBOID && fcinfo->flinfo->fn_addr == textcat) {
           bool is_null = false;
           if (fcinfo->arg[0] != 0 && VARATT_IS_EXTERNAL_LOB(fcinfo->arg[0])) {
               struct varatt_lob_pointer* lob_pointer = (varatt_lob_pointer*)(VARDATA_EXTERNAL(fcinfo->arg[0]));
               fcinfo->arg[0] = fetch_lob_value_from_tuple(lob_pointer, InvalidOid, &is_null);
           }
           if (fcinfo->arg[1] != 0 && VARATT_IS_EXTERNAL_LOB(fcinfo->arg[1])) {
               struct varatt_lob_pointer* lob_pointer = (varatt_lob_pointer*)(VARDATA_EXTERNAL(fcinfo->arg[1]));
               fcinfo->arg[1] = fetch_lob_value_from_tuple(lob_pointer, InvalidOid, &is_null);
           }
       }
        if (func_encoding != db_encoding) {
            DB_ENCODING_SWITCH_TO(func_encoding);
            result = FunctionCallInvoke(fcinfo);
            DB_ENCODING_SWITCH_BACK(db_encoding);
        } else {
            result = FunctionCallInvoke(fcinfo);
        }
   }
   *isNull = fcinfo->isnull;
    if (AUDIT_SYSTEM_EXEC_ENABLED) {
        audit_system_function(fcinfo, AUDIT_OK);
    }

   if (has_refcursor && econtext->plpgsql_estate != NULL) {
       PLpgSQL_execstate* estate = econtext->plpgsql_estate;
       for (i = 0; i < fcinfo->nargs; i++) {
           /* copy in-args cursor option info */
           if (var_dno[i] >= 0) {
               int dno = var_dno[i];
               Cursor_Data* cursor_data = &fcinfo->refcursor_data.argCursor[i];
#ifdef USE_ASSERT_CHECKING
               PLpgSQL_datum* datum = estate->datums[dno];
#endif
               Assert(datum->dtype == PLPGSQL_DTYPE_VAR);
               Assert(((PLpgSQL_var*)datum)->datatype->typoid == REFCURSOROID);

               ExecCopyDataToDatum(estate->datums, dno, cursor_data);
           }
       }

       if (fcinfo->flinfo->fn_rettype == REFCURSOROID) {
           /* copy function returns cursor option info.
            * for simple expr in exec_eval_expr, we can not get the result type,
            * so cursor_return_data mallocs here.
            */
           if (estate->cursor_return_data == NULL) {
               estate->cursor_return_data = (Cursor_Data*)palloc0(sizeof(Cursor_Data));
               estate->cursor_return_numbers = 1;
           }
           int rc = memcpy_s(estate->cursor_return_data,
                             sizeof(Cursor_Data),
                             fcinfo->refcursor_data.returnCursor,
                             sizeof(Cursor_Data));
           securec_check(rc, "\0", "\0");
       }
   }

   pgstat_end_function_usage(&fcusage, true);

   if (has_refcursor) {
       if (fcinfo->refcursor_data.argCursor != NULL)
           pfree_ext(fcinfo->refcursor_data.argCursor);
       if (var_dno != NULL)
           pfree_ext(var_dno);
   }

   u_sess->SPI_cxt.is_stp = savedIsSTP;
   u_sess->SPI_cxt.is_proconfig_set = savedProConfigIsSet;
   if (needResetErrMsg) {
       stp_reset_commit_rolback_err_msg();
   }

   set_result_for_plpgsql_language_function_with_outparam(fcache, &result, isNull);

   return result;
}

/*
* @Description: jugde function has parameter that is refcursor or return type is refcursor
* @in Funcid - function oid
* @in fcinfo - function call info
* @return - has refcursor
*/
extern bool func_has_refcursor_args(Oid Funcid, FunctionCallInfoData* fcinfo)
{
   HeapTuple proctup = NULL;
   Form_pg_proc procStruct;
   int allarg;
   Oid* p_argtypes = NULL;
   char** p_argnames = NULL;
   char* p_argmodes = NULL;
   bool use_cursor = false;
   bool return_refcursor = false;
   int out_count = 0; /* out arg count */

   fcinfo->refcursor_data.return_number = 0;
   fcinfo->refcursor_data.returnCursor = NULL;

   if (IsSystemObjOid(Funcid) && Funcid != CURSORTOXMLOID && Funcid != CURSORTOXMLSCHEMAOID) {
        return false;
   }

   proctup = SearchSysCache(PROCOID, ObjectIdGetDatum(Funcid), 0, 0, 0);

   /*
    * function may be deleted after clist be searched.
    */
   if (!HeapTupleIsValid(proctup)) {
       ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION), errmsg("function doesn't exist ")));
   }

   /* get the all args informations, only "in" parameters if p_argmodes is null */
   allarg = get_func_arg_info(proctup, &p_argtypes, &p_argnames, &p_argmodes);
   procStruct = (Form_pg_proc)GETSTRUCT(proctup);

   for (int i = 0; i < allarg; i++) {
       if (p_argmodes != NULL && (p_argmodes[i] == 'o' || p_argmodes[i] == 'b')) {
           out_count++;
           if (p_argtypes[i] == REFCURSOROID)
               return_refcursor = true;
       } else {
           if (p_argtypes[i] == REFCURSOROID)
               use_cursor = true;
       }
   }

   if (procStruct->prorettype == REFCURSOROID) {
       use_cursor = true;
       fcinfo->refcursor_data.return_number = 1;
   } else if (return_refcursor) {
       fcinfo->refcursor_data.return_number = out_count;
   }
    /* func_has_out_param means whether a func with out param and with GUC proc_outparam_override. */
    bool func_has_out_param = is_function_with_plpgsql_language_and_outparam((fcinfo->flinfo)->fn_oid);
    if (func_has_out_param && (return_refcursor || procStruct->prorettype == REFCURSOROID)) {
        fcinfo->refcursor_data.return_number = out_count + 1;
    }
    
   ReleaseSysCache(proctup);
   return use_cursor;
}
/*
*		ExecMakeTableFunctionResult
*
* Evaluate a table function, producing a materialized result in a Tuplestore
* object.
*/
Tuplestorestate* ExecMakeTableFunctionResult(
   ExprState* funcexpr, ExprContext* econtext, TupleDesc expectedDesc, bool randomAccess, FunctionScanState* node)
{
   Tuplestorestate* tupstore = NULL;
   TupleDesc tupdesc = NULL;
   Oid funcrettype;
   bool returnsTuple = false;
   bool returnsSet = false;
   FunctionCallInfoData fcinfo;
   PgStat_FunctionCallUsage fcusage;
   ReturnSetInfo rsinfo;
   HeapTupleData tmptup;
   MemoryContext callerContext;
   MemoryContext oldcontext;
   bool direct_function_call = false;
   bool first_time = true;
   int* var_dno = NULL;
   bool has_refcursor = false;
   bool has_out_param = false;

   FuncExpr *fexpr = NULL;
   bool savedIsSTP = u_sess->SPI_cxt.is_stp;
   bool savedProConfigIsSet = u_sess->SPI_cxt.is_proconfig_set;
   bool proIsProcedure = false;
   bool supportTranaction = false;
#ifdef ENABLE_MULTIPLE_NODES
   if (IS_PGXC_COORDINATOR && (t_thrd.proc->workingVersionNum  >= STP_SUPPORT_COMMIT_ROLLBACK)) {
       supportTranaction = true;
   }
#else
   supportTranaction = true;
#endif
   bool needResetErrMsg = (u_sess->SPI_cxt.forbidden_commit_rollback_err_msg[0] == '\0');

   /* Only allow commit at CN, therefore only need to set atomic and relevant check at CN level. */
   if (supportTranaction && IsA(funcexpr->expr, FuncExpr)) {
       fexpr = (FuncExpr*)funcexpr->expr;
       char prokind = (reinterpret_cast<FuncExprState*>(funcexpr))->prokind;
       if (!u_sess->SPI_cxt.is_allow_commit_rollback) {
           node->atomic = true;
       }
       else if (IsAfterTriggerBegin()) {
           node->atomic = true;
           stp_set_commit_rollback_err_msg(STP_XACT_AFTER_TRIGGER_BEGIN);
       }
       /*
        * If proconfig is set we can't allow transaction commands because of the
        * way the GUC stacking works: The transaction boundary would have to pop
        * the proconfig setting off the stack.  That restriction could be lifted
        * by redesigning the GUC nesting mechanism a bit.
        */
       if (!prokind) {
           HeapTuple tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(fexpr->funcid));
           bool isNull = false;
           if (!HeapTupleIsValid(tp)) {
               elog(ERROR, "cache lookup failed for function %u", fexpr->funcid);
           }

           /* immutable or stable function do not support commit/rollback */
           bool isNullVolatile = false;
           Datum provolatile = SysCacheGetAttr(PROCOID, tp, Anum_pg_proc_provolatile, &isNullVolatile);
           if (!isNullVolatile && CharGetDatum(provolatile) != PROVOLATILE_VOLATILE) {
               node->atomic = true;
               stp_set_commit_rollback_err_msg(STP_XACT_IMMUTABLE);
           }

           Datum datum = SysCacheGetAttr(PROCOID, tp, Anum_pg_proc_prokind, &isNull);
           proIsProcedure = PROC_IS_PRO(CharGetDatum(datum));
           if (proIsProcedure) {
               (reinterpret_cast<FuncExprState*>(funcexpr))->prokind = 'p';
           } else {
               (reinterpret_cast<FuncExprState*>(funcexpr))->prokind = 'f';
           }
           /* if proIsProcedure means it was a stored procedure */
           u_sess->SPI_cxt.is_stp = savedIsSTP;
           if (!heap_attisnull(tp, Anum_pg_proc_proconfig, NULL) || u_sess->SPI_cxt.is_proconfig_set) {
               u_sess->SPI_cxt.is_proconfig_set = true;
               node->atomic = true;
               stp_set_commit_rollback_err_msg(STP_XACT_GUC_IN_OPT_CLAUSE);
           }
           ReleaseSysCache(tp);
       } else {
           proIsProcedure = PROC_IS_PRO(prokind);
           u_sess->SPI_cxt.is_stp = savedIsSTP;
       }
   }

   callerContext = CurrentMemoryContext;

   if (unlikely(funcexpr == NULL)) {
       ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("The input function expression is NULL.")));
   }
   funcrettype = exprType((Node*)funcexpr->expr);

   returnsTuple = type_is_rowtype(funcrettype);
   econtext->plpgsql_estate = plpgsql_estate;
   plpgsql_estate = NULL;

   /*
    * Prepare a resultinfo node for communication.  We always do this even if
    * not expecting a set result, so that we can pass expectedDesc.  In the
    * generic-expression case, the expression doesn't actually get to see the
    * resultinfo, but set it up anyway because we use some of the fields as
    * our own state variables.
    */
   rsinfo.type = T_ReturnSetInfo;
   rsinfo.econtext = econtext;
   rsinfo.expectedDesc = expectedDesc;
   rsinfo.allowedModes = (int)(SFRM_ValuePerCall | SFRM_Materialize | SFRM_Materialize_Preferred);
   if (randomAccess)
       rsinfo.allowedModes |= (int)SFRM_Materialize_Random;
   rsinfo.returnMode = SFRM_ValuePerCall;
   /* isDone is filled below */
   rsinfo.setResult = NULL;
   rsinfo.setDesc = NULL;

   /*
    * Normally the passed expression tree will be a FuncExprState, since the
    * grammar only allows a function call at the top level of a table
    * function reference.	However, if the function doesn't return set then
    * the planner might have replaced the function call via constant-folding
    * or inlining.  So if we see any other kind of expression node, execute
    * it via the general ExecEvalExpr() code; the only difference is that we
    * don't get a chance to pass a special ReturnSetInfo to any functions
    * buried in the expression.
    */
   if (funcexpr && IsA(funcexpr, FuncExprState) && IsA(funcexpr->expr, FuncExpr)) {
       FuncExprState* fcache = (FuncExprState*)funcexpr;
       ExprDoneCond argDone;

       /*
        * This path is similar to ExecMakeFunctionResult.
        */
       direct_function_call = true;

       /*
        * Initialize function cache if first time through
        */
       if (!funcexpr->is_flt_frame && (fcache->func.fn_oid == InvalidOid)) {
           FuncExpr* func = (FuncExpr*)fcache->xprstate.expr;

           init_fcache<false>(func->funcid, func->inputcollid, fcache, econtext->ecxt_per_query_memory, true, false);
       }
       returnsSet = fcache->func.fn_retset;
       InitFunctionCallInfoData(fcinfo,
                                &(fcache->func),
                                list_length(fcache->args),
                                fcache->fcinfo_data.fncollation,
                                (Node*)node,
                                (Node*)&rsinfo);

       has_refcursor = func_has_refcursor_args(fcinfo.flinfo->fn_oid, &fcinfo);

       has_out_param = (is_function_with_plpgsql_language_and_outparam(fcinfo.flinfo->fn_oid) != InvalidOid);
       if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && has_out_param) {
           returnsTuple = type_is_rowtype(RECORDOID);
       }

       int cursor_return_number = fcinfo.refcursor_data.return_number;
       if (cursor_return_number > 0) {
           /* init returnCursor to store out-args cursor info on FunctionScan context*/
           fcinfo.refcursor_data.returnCursor = (Cursor_Data*)palloc0(sizeof(Cursor_Data) * cursor_return_number);
       } else {
           fcinfo.refcursor_data.returnCursor = NULL;
       }

       if (has_refcursor) {
           /* init argCursor to store in-args cursor info on FunctionScan context*/
           fcinfo.refcursor_data.argCursor = (Cursor_Data*)palloc0(sizeof(Cursor_Data) * fcinfo.nargs);
           var_dno = (int*)palloc0(sizeof(int) * fcinfo.nargs);
           int rc = memset_s(var_dno, sizeof(int) * fcinfo.nargs, -1, sizeof(int) * fcinfo.nargs);
           securec_check(rc, "\0", "\0");
       }

       /*
        * Evaluate the function's argument list.
        *
        * Note: ideally, we'd do this in the per-tuple context, but then the
        * argument values would disappear when we reset the context in the
        * inner loop.	So do it in caller context.  Perhaps we should make a
        * separate context just to hold the evaluated arguments?
        */
       if (has_refcursor)
           argDone = ExecEvalFuncArgs<true>(&fcinfo, fcache->args, econtext, var_dno);
       else
           argDone = ExecEvalFuncArgs<false>(&fcinfo, fcache->args, econtext);
       /* We don't allow sets in the arguments of the table function */
       if (!funcexpr->is_flt_frame && (argDone != ExprSingleResult))
           ereport(ERROR,
                   (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("set-valued function called in context that cannot accept a set")));

       /*
        * If function is strict, and there are any NULL arguments, skip
        * calling the function and act like it returned NULL (or an empty
        * set, in the returns-set case).
        */
       if (fcache->func.fn_strict) {
           int i;

           for (i = 0; i < fcinfo.nargs; i++) {
               if (fcinfo.argnull[i])
                   goto no_function_result;
           }
       }
   } else {
       /* Treat funcexpr as a generic expression */
       direct_function_call = false;
       InitFunctionCallInfoData(fcinfo, NULL, 0, InvalidOid, (Node*)node, NULL);
   }

   /*
    * Switch to short-lived context for calling the function or expression.
    */
   MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

   /*
    * Loop to handle the ValuePerCall protocol (which is also the same
    * behavior needed in the generic ExecEvalExpr path).
    */
   for (;;) {
       Datum result;

       CHECK_FOR_INTERRUPTS();

       /*
        * reset per-tuple memory context before each call of the function or
        * expression. This cleans up any local memory the function may leak
        * when called.
        */
       ResetExprContext(econtext);

       /* Call the function or expression one time */
       if (direct_function_call) {
           pgstat_init_function_usage(&fcinfo, &fcusage);

           fcinfo.isnull = false;
           rsinfo.isDone = ExprSingleResult;
           result = FunctionCallInvoke(&fcinfo);
            if (AUDIT_SYSTEM_EXEC_ENABLED) {
                audit_system_function(&fcinfo, AUDIT_OK);
            }

           if (econtext->plpgsql_estate != NULL) {
               PLpgSQL_execstate* estate = econtext->plpgsql_estate;
               bool isVaildReturn = (fcinfo.refcursor_data.return_number > 0 &&
                                     estate->cursor_return_data != NULL && fcinfo.refcursor_data.returnCursor != NULL);
               if (isVaildReturn) {
                   bool isVaildReturnNum = (fcinfo.refcursor_data.return_number > estate->cursor_return_numbers);
                   if (isVaildReturnNum) {
                       pgstat_end_function_usage(&fcusage, rsinfo.isDone != ExprMultipleResult);
                       ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_PLSQL),
                                       errmsg("The expected output of the cursor:%d and function:%d does not match",
                                              estate->cursor_return_numbers, fcinfo.refcursor_data.return_number)));
                   }
                   for (int i = 0; i < fcinfo.refcursor_data.return_number; i++) {
                       CopyCursorInfoData(&estate->cursor_return_data[i], &fcinfo.refcursor_data.returnCursor[i]);
                   }
               }

               if (var_dno != NULL) {
                   for (int i = 0; i < fcinfo.nargs; i++) {
                       if (var_dno[i] >= 0) {
                           int dno = var_dno[i];
                           Cursor_Data* cursor_data = &fcinfo.refcursor_data.argCursor[i];
                           PLpgSQL_execstate* execstate = econtext->plpgsql_estate;
#ifdef USE_ASSERT_CHECKING
                           PLpgSQL_datum* datum = execstate->datums[dno];
#endif
                           Assert(datum->dtype == PLPGSQL_DTYPE_VAR);
                           Assert(((PLpgSQL_var*)datum)->datatype->typoid == REFCURSOROID);

                           ExecCopyDataToDatum(execstate->datums, dno, cursor_data);
                       }
                   }
               }
           }

           pgstat_end_function_usage(&fcusage, rsinfo.isDone != ExprMultipleResult);
       } else {
           result = ExecEvalExpr(funcexpr, econtext, &fcinfo.isnull, &rsinfo.isDone);
       }

       /* Which protocol does function want to use? */
       if (rsinfo.returnMode == SFRM_ValuePerCall) {
           /*
            * Check for end of result set.
            */
           if (rsinfo.isDone == ExprEndResult) {
               break;
           }

           /*
            * Can't do anything very useful with NULL rowtype values. For a
            * function returning set, we consider this a protocol violation
            * (but another alternative would be to just ignore the result and
            * "continue" to get another row).	For a function not returning
            * set, we fall out of the loop; we'll cons up an all-nulls result
            * row below.
            */
           if (returnsTuple && fcinfo.isnull && !has_out_param) {
               if (!returnsSet) {
                   break;
               }
               ereport(ERROR,
                       (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("function returning set of rows cannot return null value")));
           }

           /*
            * If first time through, build tupdesc and tuplestore for result
            */
           if (first_time) {
               oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
               if (returnsTuple) {
                   /*
                    * Use the type info embedded in the rowtype Datum to look
                    * up the needed tupdesc.  Make a copy for the query.
                    */
                   HeapTupleHeader td;

                   td = DatumGetHeapTupleHeader(result);
                   if (IsA(funcexpr->expr, Const)) {
                       tupdesc = lookup_rowtype_tupdesc_copy(
                           ((Const*)funcexpr->expr)->consttype, HeapTupleHeaderGetTypMod(td));
                   } else {
                       tupdesc =
                           lookup_rowtype_tupdesc_copy(HeapTupleHeaderGetTypeId(td), HeapTupleHeaderGetTypMod(td));
                   }
               } else {
                   /*
                    * Scalar type, so make a single-column descriptor
                    */
                   tupdesc = CreateTemplateTupleDesc(1, false, TableAmHeap);
                   TupleDescInitEntry(tupdesc, (AttrNumber)1, "column", funcrettype, -1, 0);
               }
               tupstore = tuplestore_begin_heap(randomAccess, false, u_sess->attr.attr_memory.work_mem);
               MemoryContextSwitchTo(oldcontext);
               rsinfo.setResult = tupstore;
               rsinfo.setDesc = tupdesc;
           }

           /*
            * Store current resultset item.
            */
           if (returnsTuple) {
               HeapTupleHeader td;

               td = DatumGetHeapTupleHeader(result);

               /*
                * Verify all returned rows have same subtype; necessary in
                * case the type is RECORD.
                */
               if ((HeapTupleHeaderGetTypeId(td) != tupdesc->tdtypeid ||
                    HeapTupleHeaderGetTypMod(td) != tupdesc->tdtypmod) &&
                   nodeTag(funcexpr->expr) != T_Const) {
                   ereport(ERROR,
                           (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("rows returned by function are not all of the same row type"),
                            errdetail("return type id %u, tuple decription id %u, return typmod %d  "
                                      "tuple decription, typmod %d",
                                      HeapTupleHeaderGetTypeId(td),
                                      tupdesc->tdtypeid,
                                      HeapTupleHeaderGetTypMod(td),
                                      tupdesc->tdtypmod)));
               }

               /*
                * tuplestore_puttuple needs a HeapTuple not a bare
                * HeapTupleHeader, but it doesn't need all the fields.
                */
               tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
               tmptup.t_data = td;

               tuplestore_puttuple(tupstore, &tmptup);
           } else {
               tuplestore_putvalues(tupstore, tupdesc, &result, &fcinfo.isnull);
           }

           /*
            * Are we done?
            */
           if (rsinfo.isDone != ExprMultipleResult) {
               break;
           }
       } else if (rsinfo.returnMode == SFRM_Materialize) {
           /* check we're on the same page as the function author */
           if (!first_time || rsinfo.isDone != ExprSingleResult) {
               ereport(ERROR,
                       (errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
                        errmsg("table-function protocol for materialize mode was not followed")));
           }
           /* Done evaluating the set result */
           break;
       } else {
           ereport(ERROR,
                   (errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED),
                    errmsg("unrecognized table-function returnMode: %d", (int)rsinfo.returnMode)));
       }

       first_time = false;
   }

no_function_result:

   /*
    * If we got nothing from the function (ie, an empty-set or NULL result),
    * we have to create the tuplestore to return, and if it's a
    * non-set-returning function then insert a single all-nulls row.
    */
   if (rsinfo.setResult == NULL) {
       MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
       tupstore = tuplestore_begin_heap(randomAccess, false, u_sess->attr.attr_memory.work_mem);
       rsinfo.setResult = tupstore;
       if (!returnsSet) {
           int natts = expectedDesc->natts;
           Datum* nulldatums = NULL;
           bool* nullflags = NULL;
           errno_t rc = EOK;

           MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
           nulldatums = (Datum*)palloc0(natts * sizeof(Datum));
           nullflags = (bool*)palloc(natts * sizeof(bool));
           rc = memset_s(nullflags, natts * sizeof(bool), true, natts * sizeof(bool));
           securec_check(rc, "\0", "\0");
           MemoryContextSwitchTo(econtext->ecxt_per_query_memory);
           tuplestore_putvalues(tupstore, expectedDesc, nulldatums, nullflags);
       }
   }

   /*
    * If function provided a tupdesc, cross-check it.	We only really need to
    * do this for functions returning RECORD, but might as well do it always.
    */
   if (rsinfo.setDesc) {
       tupledesc_match(expectedDesc, rsinfo.setDesc);

       /*
        * If it is a dynamically-allocated TupleDesc, free it: it is
        * typically allocated in a per-query context, so we must avoid
        * leaking it across multiple usages.
        */
       if (rsinfo.setDesc->tdrefcount == -1)
           FreeTupleDesc(rsinfo.setDesc);
   }

   MemoryContextSwitchTo(callerContext);
   econtext->plpgsql_estate = NULL;

   if (has_refcursor) {
       if (fcinfo.refcursor_data.argCursor != NULL)
           pfree_ext(fcinfo.refcursor_data.argCursor);
       if (fcinfo.refcursor_data.returnCursor != NULL)
           pfree_ext(fcinfo.refcursor_data.returnCursor);
       if (var_dno != NULL)
           pfree_ext(var_dno);
   }

   /* reset the u_sess->SPI_cxt.is_stp, u_sess->SPI_cxt.is_proconfig_set
      and error message value */
   u_sess->SPI_cxt.is_stp = savedIsSTP;
   u_sess->SPI_cxt.is_proconfig_set = savedProConfigIsSet;
   if (needResetErrMsg) {
       stp_reset_commit_rolback_err_msg();
   }

   /* All done, pass back the tuplestore */
   return rsinfo.setResult;
}

/* ----------------------------------------------------------------
*		ExecEvalFunc
*		ExecEvalOper
*
*		Evaluate the functional result of a list of arguments by calling the
*		function manager.
* ----------------------------------------------------------------
*/
/* ----------------------------------------------------------------
*		ExecEvalFunc
* ----------------------------------------------------------------
*/
static Datum ExecEvalFunc(FuncExprState *fcache, ExprContext *econtext, bool *isNull, ExprDoneCond *isDone)
{
    /* This is called only the first time through */
    FuncExpr* func = (FuncExpr*)fcache->xprstate.expr;
    Oid target_type = InvalidOid;
    Oid source_type = InvalidOid;
    bool has_refcursor = false;
    int cursor_return_number = 0;

    if (fcache->xprstate.is_flt_frame) {
       init_fcache<false>(func->funcid, func->inputcollid, fcache, econtext->ecxt_per_query_memory, false, false);
       has_refcursor = func_has_refcursor_args(func->funcid, &fcache->fcinfo_data);
       cursor_return_number = fcache->fcinfo_data.refcursor_data.return_number;

       Assert(!fcache->func.fn_retset);

       if (has_refcursor) {
           if (cursor_return_number > 0) {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<true, true>;
               return ExecMakeFunctionResultNoSets<true, true>(fcache, econtext, isNull, isDone);
           } else {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<true, false>;
               return ExecMakeFunctionResultNoSets<true, false>(fcache, econtext, isNull, isDone);
           }
       } else {
           if (cursor_return_number > 0) {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<false, true>;
               return ExecMakeFunctionResultNoSets<false, true>(fcache, econtext, isNull, isDone);
           } else {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<false, false>;
               return ExecMakeFunctionResultNoSets<false, false>(fcache, econtext, isNull, isDone);
           }
       }
    }

    /* Initialize function lookup info */
    init_fcache<false>(func->funcid, func->inputcollid, fcache, econtext->ecxt_per_query_memory, false, true);

    has_refcursor = func_has_refcursor_args(func->funcid, &fcache->fcinfo_data);
    cursor_return_number = fcache->fcinfo_data.refcursor_data.return_number;

    if (func->funcformat == COERCE_EXPLICIT_CAST || func->funcformat == COERCE_IMPLICIT_CAST) {

        HeapTuple proc_tuple = SearchSysCache(PROCOID, ObjectIdGetDatum(func->funcid), 0, 0, 0);
        if (HeapTupleIsValid(proc_tuple)) {
            Form_pg_proc proc_struct = (Form_pg_proc)GETSTRUCT(proc_tuple);
            source_type = proc_struct->proargtypes.values[0];
            ReleaseSysCache(proc_tuple);
            target_type = func->funcresulttype;
            HeapTuple cast_tuple = SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(source_type),
                                                   ObjectIdGetDatum(target_type));
            if (HeapTupleIsValid(cast_tuple)) {
               bool isnull = false;
               Datum datum = SysCacheGetAttr(CASTSOURCETARGET, cast_tuple, Anum_pg_cast_castowner, &isnull);
               if (!isnull) {
                   u_sess->exec_cxt.cast_owner = DatumGetObjectId(datum);
               } else {
                   u_sess->exec_cxt.cast_owner = InvalidCastOwnerId;
               }
               ReleaseSysCache(cast_tuple);
            }
        }
    }

   /*
    * We need to invoke ExecMakeFunctionResult if either the function itself
    * or any of its input expressions can return a set.  Otherwise, invoke
    * ExecMakeFunctionResultNoSets.  In either case, change the evalfunc
    * pointer to go directly there on subsequent uses.
    */
   if (fcache->func.fn_retset) {
       if (has_refcursor) {
           if (cursor_return_number > 0) {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<true, true, true>;
               return ExecMakeFunctionResult<true, true, true>(fcache, econtext, isNull, isDone);
           } else {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<true, false, true>;
               return ExecMakeFunctionResult<true, false, true>(fcache, econtext, isNull, isDone);
           }
       } else {
           if (cursor_return_number > 0) {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<false, true, true>;
               return ExecMakeFunctionResult<false, true, true>(fcache, econtext, isNull, isDone);
           } else {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<false, false, true>;
               return ExecMakeFunctionResult<false, false, true>(fcache, econtext, isNull, isDone);
           }
       }
   } else if (expression_returns_set((Node *)func->args)) {
       if (has_refcursor) {
           if (cursor_return_number > 0) {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<true, true, false>;
               return ExecMakeFunctionResult<true, true, false>(fcache, econtext, isNull, isDone);
           } else {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<true, false, false>;
               return ExecMakeFunctionResult<true, false, false>(fcache, econtext, isNull, isDone);
           }
       } else {
           if (cursor_return_number > 0) {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<false, true, false>;
               return ExecMakeFunctionResult<false, true, false>(fcache, econtext, isNull, isDone);
           } else {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<false, false, false>;
               return ExecMakeFunctionResult<false, false, false>(fcache, econtext, isNull, isDone);
           }
       }
   } else {
       if (has_refcursor) {
           if (cursor_return_number > 0) {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<true, true>;
               return ExecMakeFunctionResultNoSets<true, true>(fcache, econtext, isNull, isDone);
           } else {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<true, false>;
               return ExecMakeFunctionResultNoSets<true, false>(fcache, econtext, isNull, isDone);
           }
       } else {
           if (cursor_return_number > 0) {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<false, true>;
               return ExecMakeFunctionResultNoSets<false, true>(fcache, econtext, isNull, isDone);
           } else {
               fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<false, false>;
               return ExecMakeFunctionResultNoSets<false, false>(fcache, econtext, isNull, isDone);
           }
       }
   }
}

/* ----------------------------------------------------------------
*		ExecEvalOper
* ----------------------------------------------------------------
*/
static Datum ExecEvalOper(FuncExprState* fcache, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
    /* This is called only the first time through */
    OpExpr* op = (OpExpr*)fcache->xprstate.expr;
    bool has_refcursor = false;
    int cursor_return_number = 0;
   if (fcache->xprstate.is_flt_frame) {
        init_fcache<false>(op->opfuncid, op->inputcollid, fcache, econtext->ecxt_per_query_memory, false, false);
        has_refcursor = func_has_refcursor_args(op->opfuncid, &fcache->fcinfo_data);
        cursor_return_number = fcache->fcinfo_data.refcursor_data.return_number;
        Assert(!fcache->func.fn_retset);
        if (has_refcursor) {
            if (cursor_return_number > 0) {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<true, true>;
                return ExecMakeFunctionResultNoSets<true, true>(fcache, econtext, isNull, isDone);
            } else {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<true, false>;
                return ExecMakeFunctionResultNoSets<true, false>(fcache, econtext, isNull, isDone);
            }
        } else {
            if (cursor_return_number > 0) {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<false, true>;
                return ExecMakeFunctionResultNoSets<false, true>(fcache, econtext, isNull, isDone);
            } else {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<false, false>;
                return ExecMakeFunctionResultNoSets<false, false>(fcache, econtext, isNull, isDone);
            }
        }
    }

    /* Initialize function lookup info */
    init_fcache<false>(op->opfuncid, op->inputcollid, fcache, econtext->ecxt_per_query_memory, false, true);
    has_refcursor = func_has_refcursor_args(op->opfuncid, &fcache->fcinfo_data);
    cursor_return_number = fcache->fcinfo_data.refcursor_data.return_number;

    /*
     * We need to invoke ExecMakeFunctionResult if either the function itself
     * or any of its input expressions can return a set.  Otherwise, invoke
     * ExecMakeFunctionResultNoSets.  In either case, change the evalfunc
     * pointer to go directly there on subsequent uses.
     */
     if (fcache->func.fn_retset) {
        if (has_refcursor) {
            if (cursor_return_number > 0) {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<true, true, true>;
                return ExecMakeFunctionResult<true, true, true>(fcache, econtext, isNull, isDone);
            } else {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<true, false, true>;
                return ExecMakeFunctionResult<true, false, true>(fcache, econtext, isNull, isDone);
            }
        } else {
            if (cursor_return_number > 0) {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<false, true, true>;
                return ExecMakeFunctionResult<false, true, true>(fcache, econtext, isNull, isDone);
            } else {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<false, false, true>;
                return ExecMakeFunctionResult<false, false, true>(fcache, econtext, isNull, isDone);
            }
        }
    } else if (expression_returns_set((Node*)op->args)) {
        if (has_refcursor) {
            if (cursor_return_number > 0) {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<true, true, false>;
                return ExecMakeFunctionResult<true, true, false>(fcache, econtext, isNull, isDone);
            } else {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<true, false, false>;
                return ExecMakeFunctionResult<true, false, false>(fcache, econtext, isNull, isDone);
            }
        } else {
            if (cursor_return_number > 0) {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<false, true, false>;
                return ExecMakeFunctionResult<false, true, false>(fcache, econtext, isNull, isDone);
            } else {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResult<false, false, false>;
                return ExecMakeFunctionResult<false, false, false>(fcache, econtext, isNull, isDone);
            }
        }
    } else {
        if (has_refcursor) {
            if (cursor_return_number > 0) {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<true, true>;
                return ExecMakeFunctionResultNoSets<true, true>(fcache, econtext, isNull, isDone);
            } else {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<true, false>;
                return ExecMakeFunctionResultNoSets<true, false>(fcache, econtext, isNull, isDone);
            }
        } else {
            if (cursor_return_number > 0) {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<false, true>;
                return ExecMakeFunctionResultNoSets<false, true>(fcache, econtext, isNull, isDone);
            } else {
                fcache->xprstate.evalfunc = (ExprStateEvalFunc)ExecMakeFunctionResultNoSets<false, false>;
                return ExecMakeFunctionResultNoSets<false, false>(fcache, econtext, isNull, isDone);
            }
        }
    }
}

/* ----------------------------------------------------------------
*		ExecEvalDistinct
*
* IS DISTINCT FROM must evaluate arguments to determine whether
* they are NULL; if either is NULL then the result is already
* known. If neither is NULL, then proceed to evaluate the
* function. Note that this is *always* derived from the equals
* operator, but since we need special processing of the arguments
* we can not simply reuse ExecEvalOper() or ExecEvalFunc().
* ----------------------------------------------------------------
*/
static Datum ExecEvalDistinct(FuncExprState* fcache, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   Datum result;
   FunctionCallInfo fcinfo;
   ExprDoneCond argDone;

   /* Set default values for result flags: non-null, not a set result */
   *isNull = false;
   if (isDone != NULL)
       *isDone = ExprSingleResult;

    /*
     * Initialize function cache if first time through
     */
    if (fcache->func.fn_oid == InvalidOid) {
        DistinctExpr* op = (DistinctExpr*)fcache->xprstate.expr;
        if (fcache->xprstate.is_flt_frame) {
            init_fcache<false>(op->opfuncid, op->inputcollid, fcache, econtext->ecxt_per_query_memory, false, false);
        } else {
            init_fcache<false>(op->opfuncid, op->inputcollid, fcache, econtext->ecxt_per_query_memory, false, true);
            Assert(!fcache->func.fn_retset);
        }
    }

   /*
    * Evaluate arguments
    */
   fcinfo = &fcache->fcinfo_data;
   argDone = ExecEvalFuncArgs<false>(fcinfo, fcache->args, econtext);
   if (argDone != ExprSingleResult)
       ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("IS DISTINCT FROM does not support set arguments")));
   Assert(fcinfo->nargs == 2);

   if (fcinfo->argnull[0] && fcinfo->argnull[1]) {
       /* Both NULL? Then is not distinct... */
       result = BoolGetDatum(FALSE);
   } else if (fcinfo->argnull[0] || fcinfo->argnull[1]) {
       /* Only one is NULL? Then is distinct... */
       result = BoolGetDatum(TRUE);
   } else {
       fcinfo->isnull = false;
       result = FunctionCallInvoke(fcinfo);
       *isNull = fcinfo->isnull;
       /* Must invert result of "=" */
       result = BoolGetDatum(!DatumGetBool(result));
   }

   return result;
}

/*
* ExecEvalScalarArrayOp
*
* Evaluate "scalar op ANY/ALL (array)".  The operator always yields boolean,
* and we combine the results across all array elements using OR and AND
* (for ANY and ALL respectively).	Of course we short-circuit as soon as
* the result is known.
*/
static Datum ExecEvalScalarArrayOp(
   ScalarArrayOpExprState* sstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   ScalarArrayOpExpr* opexpr = (ScalarArrayOpExpr*)sstate->fxprstate.xprstate.expr;
   bool useOr = opexpr->useOr;
   ArrayType* arr = NULL;
   int nitems;
   Datum result;
   bool resultnull = false;
   FunctionCallInfo fcinfo;
   ExprDoneCond argDone;
   int i;
   int16 typlen;
   bool typbyval = false;
   char typalign;
   char* s = NULL;
   bits8* bitmap = NULL;
   int bitmask;

   /* Set default values for result flags: non-null, not a set result */
   *isNull = false;
   if (isDone != NULL)
       *isDone = ExprSingleResult;

   /*
    * Initialize function cache if first time through
    */
   if (sstate->fxprstate.func.fn_oid == InvalidOid) {
       if (sstate->fxprstate.xprstate.is_flt_frame) {
            init_fcache<false>(opexpr->opfuncid, opexpr->inputcollid, &sstate->fxprstate,
                               econtext->ecxt_per_query_memory, false, false);
        } else {
            init_fcache<false>(opexpr->opfuncid, opexpr->inputcollid, &sstate->fxprstate,
                               econtext->ecxt_per_query_memory, false, true);
            Assert(!sstate->fxprstate.func.fn_retset);
        }
    }

   /*
    * Evaluate arguments
    */
   fcinfo = &sstate->fxprstate.fcinfo_data;
   /* init the number of arguments to a function. */
   InitFunctionCallInfoArgs(*fcinfo, 2, 1);
   argDone = ExecEvalFuncArgs<false>(fcinfo, sstate->fxprstate.args, econtext);
   if (argDone != ExprSingleResult)
       ereport(
           ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("op ANY/ALL (array) does not support set arguments")));
   Assert(fcinfo->nargs == 2);

   /*
    * If the array is NULL then we return NULL --- it's not very meaningful
    * to do anything else, even if the operator isn't strict.
    */
   if (fcinfo->argnull[1]) {
       *isNull = true;
       return (Datum)0;
   }
   /* Else okay to fetch and detoast the array */
   arr = DatumGetArrayTypeP(fcinfo->arg[1]);

   /*
    * If the array is empty, we return either FALSE or TRUE per the useOr
    * flag.  This is correct even if the scalar is NULL; since we would
    * evaluate the operator zero times, it matters not whether it would want
    * to return NULL.
    */
   nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
   if (nitems <= 0)
       return BoolGetDatum(!useOr);

   /*
    * If the scalar is NULL, and the function is strict, return NULL; no
    * point in iterating the loop.
    */
   if (fcinfo->argnull[0] && sstate->fxprstate.func.fn_strict) {
       *isNull = true;
       return (Datum)0;
   }

   /*
    * We arrange to look up info about the element type only once per series
    * of calls, assuming the element type doesn't change underneath us.
    */
   if (sstate->element_type != ARR_ELEMTYPE(arr)) {
       get_typlenbyvalalign(ARR_ELEMTYPE(arr), &sstate->typlen, &sstate->typbyval, &sstate->typalign);
       sstate->element_type = ARR_ELEMTYPE(arr);
   }
   typlen = sstate->typlen;
   typbyval = sstate->typbyval;
   typalign = sstate->typalign;

   result = BoolGetDatum(!useOr);
   resultnull = false;

   /* Loop over the array elements */
   s = (char*)ARR_DATA_PTR(arr);
   bitmap = ARR_NULLBITMAP(arr);
   bitmask = 1;

   for (i = 0; i < nitems; i++) {
       Datum elt;
       Datum thisresult;

        /* Get array element, checking for NULL */
        if (bitmap && (*bitmap & bitmask) == 0) {
            fcinfo->arg[1] = (Datum)0;
            fcinfo->argnull[1] = true;
        } else {
            elt = fetch_att(s, typbyval, typlen);
            s = att_addlength_pointer(s, typlen, s);
            s = (char*)att_align_nominal(s, typalign);
            fcinfo->arg[1] = elt;
            fcinfo->argnull[1] = false;
            fcinfo->argTypes[1] = ARR_ELEMTYPE(arr);
        }

       /* Call comparison function */
       if (fcinfo->argnull[1] && sstate->fxprstate.func.fn_strict) {
           fcinfo->isnull = true;
           thisresult = (Datum)0;
       } else {
           fcinfo->isnull = false;
           thisresult = FunctionCallInvoke(fcinfo);
       }

       /* Combine results per OR or AND semantics */
       if (fcinfo->isnull)
           resultnull = true;
       else if (useOr) {
           if (DatumGetBool(thisresult)) {
               result = BoolGetDatum(true);
               resultnull = false;
               break; /* needn't look at any more elements */
           }
       } else {
           if (!DatumGetBool(thisresult)) {
               result = BoolGetDatum(false);
               resultnull = false;
               break; /* needn't look at any more elements */
           }
       }

       /* advance bitmap pointer if any */
       if (bitmap != NULL) {
           bitmask <<= 1;
           if (bitmask == 0x100) {
               bitmap++;
               bitmask = 1;
           }
       }
   }

   *isNull = resultnull;
   return result;
}

/* ----------------------------------------------------------------
*		ExecEvalNot
*		ExecEvalOr
*		ExecEvalAnd
*
*		Evaluate boolean expressions, with appropriate short-circuiting.
*
*		The query planner reformulates clause expressions in the
*		qualification to conjunctive normal form.  If we ever get
*		an AND to evaluate, we can be sure that it's not a top-level
*		clause in the qualification, but appears lower (as a function
*		argument, for example), or in the target list.	Not that you
*		need to know this, mind you...
* ----------------------------------------------------------------
*/
static Datum ExecEvalNot(BoolExprState* notclause, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   ExprState* clause = (ExprState*)linitial(notclause->args);
   Datum expr_value;

   if (isDone != NULL)
       *isDone = ExprSingleResult;

   expr_value = ExecEvalExpr(clause, econtext, isNull, NULL);

   /*
    * if the expression evaluates to null, then we just cascade the null back
    * to whoever called us.
    */
   if (*isNull)
       return expr_value;

   /*
    * evaluation of 'not' is simple.. expr is false, then return 'true' and
    * vice versa.
    */
   return BoolGetDatum(!DatumGetBool(expr_value));
}

/* ----------------------------------------------------------------
*		ExecEvalOr
* ----------------------------------------------------------------
*/
static Datum ExecEvalOr(BoolExprState* orExpr, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   List* clauses = orExpr->args;
   ListCell* clause = NULL;
   bool AnyNull = false;

   if (isDone != NULL)
       *isDone = ExprSingleResult;

   AnyNull = false;

   /*
    * If any of the clauses is TRUE, the OR result is TRUE regardless of the
    * states of the rest of the clauses, so we can stop evaluating and return
    * TRUE immediately.  If none are TRUE and one or more is NULL, we return
    * NULL; otherwise we return FALSE.  This makes sense when you interpret
    * NULL as "don't know": if we have a TRUE then the OR is TRUE even if we
    * aren't sure about some of the other inputs. If all the known inputs are
    * FALSE, but we have one or more "don't knows", then we have to report
    * that we "don't know" what the OR's result should be --- perhaps one of
    * the "don't knows" would have been TRUE if we'd known its value.  Only
    * when all the inputs are known to be FALSE can we state confidently that
    * the OR's result is FALSE.
    */
   foreach (clause, clauses) {
       ExprState* clausestate = (ExprState*)lfirst(clause);
       Datum clause_value;

       clause_value = ExecEvalExpr(clausestate, econtext, isNull, NULL);

       /*
        * if we have a non-null true result, then return it.
        */
       if (*isNull)
           AnyNull = true; /* remember we got a null */
       else if (DatumGetBool(clause_value))
           return clause_value;
   }

   /* AnyNull is true if at least one clause evaluated to NULL */
   *isNull = AnyNull;
   return BoolGetDatum(false);
}

/* ----------------------------------------------------------------
*		ExecEvalAnd
* ----------------------------------------------------------------
*/
static Datum ExecEvalAnd(BoolExprState* andExpr, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   List* clauses = andExpr->args;
   ListCell* clause = NULL;
   bool AnyNull = false;

   if (isDone != NULL)
       *isDone = ExprSingleResult;

   AnyNull = false;

   /*
    * If any of the clauses is FALSE, the AND result is FALSE regardless of
    * the states of the rest of the clauses, so we can stop evaluating and
    * return FALSE immediately.  If none are FALSE and one or more is NULL,
    * we return NULL; otherwise we return TRUE.  This makes sense when you
    * interpret NULL as "don't know", using the same sort of reasoning as for
    * OR, above.
    */
   foreach (clause, clauses) {
       ExprState* clausestate = (ExprState*)lfirst(clause);
       Datum clause_value;

       clause_value = ExecEvalExpr(clausestate, econtext, isNull, NULL);

       /*
        * if we have a non-null false result, then return it.
        */
       if (*isNull)
           AnyNull = true; /* remember we got a null */
       else if (!DatumGetBool(clause_value))
           return clause_value;
   }

   /* AnyNull is true if at least one clause evaluated to NULL */
   *isNull = AnyNull;
   return BoolGetDatum(!AnyNull);
}

/* ----------------------------------------------------------------
*		ExecEvalConvertRowtype
*
*		Evaluate a rowtype coercion operation.	This may require
*		rearranging field positions.
* ----------------------------------------------------------------
*/
static Datum ExecEvalConvertRowtype(
   ConvertRowtypeExprState* cstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   ConvertRowtypeExpr* convert = (ConvertRowtypeExpr*)cstate->xprstate.expr;
   HeapTuple result;
   Datum tupDatum;
   HeapTupleHeader tuple;
   HeapTupleData tmptup;

   tupDatum = ExecEvalExpr(cstate->arg, econtext, isNull, isDone);

   /* this test covers the isDone exception too: */
   if (*isNull)
       return tupDatum;

   tuple = DatumGetHeapTupleHeader(tupDatum);

   /* Lookup tupdescs if first time through or after rescan */
   if (cstate->indesc == NULL) {
       get_cached_rowtype(exprType((Node*)convert->arg), -1, &cstate->indesc, econtext);
       cstate->initialized = false;
   }
   if (cstate->outdesc == NULL) {
       get_cached_rowtype(convert->resulttype, -1, &cstate->outdesc, econtext);
       cstate->initialized = false;
   }

   Assert(HeapTupleHeaderGetTypeId(tuple) == cstate->indesc->tdtypeid);
   Assert(HeapTupleHeaderGetTypMod(tuple) == cstate->indesc->tdtypmod);

   /* if first time through, initialize conversion map */
   if (!cstate->initialized) {
       MemoryContext old_cxt;

       /* allocate map in long-lived memory context */
       old_cxt = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

       /* prepare map from old to new attribute numbers */
       cstate->map =
           convert_tuples_by_name(cstate->indesc, cstate->outdesc, gettext_noop("could not convert row type"));
       cstate->initialized = true;

       MemoryContextSwitchTo(old_cxt);
   }

   /*
    * No-op if no conversion needed (not clear this can happen here).
    */
   if (cstate->map == NULL)
       return tupDatum;

   /*
    * do_convert_tuple needs a HeapTuple not a bare HeapTupleHeader.
    */
   tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
   tmptup.t_data = tuple;

   result = do_convert_tuple(&tmptup, cstate->map);

   return HeapTupleGetDatum(result);
}

/* ----------------------------------------------------------------
*		ExecEvalCase
*
*		Evaluate a CASE clause. Will have boolean expressions
*		inside the WHEN clauses, and will have expressions
*		for results.
*		- thomas 1998-11-09
* ----------------------------------------------------------------
*/
static Datum ExecEvalCase(CaseExprState* caseExpr, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   List* clauses = caseExpr->args;
   ListCell* clause = NULL;
   Datum save_datum;
   bool save_isNull = false;

   if (isDone != NULL)
       *isDone = ExprSingleResult;

   /*
    * If there's a test expression, we have to evaluate it and save the value
    * where the CaseTestExpr placeholders can find it. We must save and
    * restore prior setting of econtext's caseValue fields, in case this node
    * is itself within a larger CASE.Furthermore, don't assign to the
    * econtext fields until after returning from evaluation of the test
    * expression.  We used to pass &econtext->caseValue_isNull to the
    * recursive call, but that leads to aliasing that variable within said
    * call, which can (and did) produce bugs when the test expression itself
    * contains a CASE.
    *
    * If there's no test expression, we don't actually need to save and
    * restore these fields; but it's less code to just do so unconditionally.
    */
   save_datum = econtext->caseValue_datum;
   save_isNull = econtext->caseValue_isNull;

   if (caseExpr->arg) {
       bool arg_isNull = false;
       econtext->caseValue_datum = ExecEvalExpr(caseExpr->arg, econtext, &arg_isNull, NULL);
       econtext->caseValue_isNull = arg_isNull;
   }

   /*
    * we evaluate each of the WHEN clauses in turn, as soon as one is true we
    * return the corresponding result. If none are true then we return the
    * value of the default clause, or NULL if there is none.
    */
   foreach (clause, clauses) {
       CaseWhenState* wclause = (CaseWhenState*)lfirst(clause);
       Datum clause_value;
       bool clause_isNull = false;

       clause_value = ExecEvalExpr(wclause->expr, econtext, &clause_isNull, NULL);

       /*
        * if we have a true test, then we return the result, since the case
        * statement is satisfied.	A NULL result from the test is not
        * considered true.
        */
       if (DatumGetBool(clause_value) && !clause_isNull) {
           econtext->caseValue_datum = save_datum;
           econtext->caseValue_isNull = save_isNull;
           return ExecEvalExpr(wclause->result, econtext, isNull, isDone);
       }
   }

   econtext->caseValue_datum = save_datum;
   econtext->caseValue_isNull = save_isNull;

   if (caseExpr->defresult) {
       return ExecEvalExpr(caseExpr->defresult, econtext, isNull, isDone);
   }

   *isNull = true;
   return (Datum)0;
}

/*
* ExecEvalCaseTestExpr
*
* Return the value stored by CASE.
*/
static Datum ExecEvalCaseTestExpr(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   if (isDone != NULL)
       *isDone = ExprSingleResult;
   *isNull = econtext->caseValue_isNull;
   return econtext->caseValue_datum;
}

/*
* ExecEvalGroupingFuncExpr
*
* Return a bitmask with a bit for each (unevaluated) argument expression
* (rightmost arg is least significant bit).
*
* A bit is set if the corresponding expression is NOT part of the set of
* grouping expressions in the current grouping set.
*/
static Datum ExecEvalGroupingFuncExpr(
   GroupingFuncExprState* gstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   int result = 0;
   int attnum = 0;
   Bitmapset* grouped_cols = gstate->aggstate->grouped_cols;
   ListCell* lc = NULL;

   if (isDone != NULL)
       *isDone = ExprSingleResult;

   *isNull = false;

   foreach (lc, (gstate->clauses)) {
       attnum = lfirst_int(lc);

       result = (uint32)result << 1;

       if (!bms_is_member(attnum, grouped_cols))
           result = (uint32)result | 1;
   }

   return (Datum)result;
}

/* ----------------------------------------------------------------
*		ExecEvalArray - ARRAY[] expressions
* ----------------------------------------------------------------
*/
static Datum ExecEvalArray(ArrayExprState* astate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   ArrayExpr* arrayExpr = (ArrayExpr*)astate->xprstate.expr;
   ArrayType* result = NULL;
   ListCell* element = NULL;
   Oid element_type = arrayExpr->element_typeid;
   int ndims = 0;
   int dims[MAXDIM];
   int lbs[MAXDIM];

   /* Set default values for result flags: non-null, not a set result */
   *isNull = false;
   if (isDone != NULL)
       *isDone = ExprSingleResult;

   if (!arrayExpr->multidims) {
       /* Elements are presumably of scalar type */
       int nelems;
       Datum* dvalues = NULL;
       bool* dnulls = NULL;
       int i = 0;

       ndims = 1;
       nelems = list_length(astate->elements);

       /* Shouldn't happen here, but if length is 0, return empty array */
       if (nelems == 0)
           return PointerGetDatum(construct_empty_array(element_type));

       dvalues = (Datum*)palloc(nelems * sizeof(Datum));
       dnulls = (bool*)palloc(nelems * sizeof(bool));

       /* loop through and build array of datums */
       foreach (element, astate->elements) {
           ExprState* e = (ExprState*)lfirst(element);

           dvalues[i] = ExecEvalExpr(e, econtext, &dnulls[i], NULL);
           i++;
       }

       /* setup for 1-D array of the given length */
       dims[0] = nelems;
       lbs[0] = 1;

       result = construct_md_array(
           dvalues, dnulls, ndims, dims, lbs, element_type, astate->elemlength, astate->elembyval, astate->elemalign);
   } else {
       /* Must be nested array expressions */
       int nbytes = 0;
       int nitems = 0;
       int outer_nelems = 0;
       int elem_ndims = 0;
       int* elem_dims = NULL;
       int* elem_lbs = NULL;
       bool firstone = true;
       bool havenulls = false;
       bool haveempty = false;
       char** subdata;
       bits8** subbitmaps;
       int* subbytes = NULL;
       int* subnitems = NULL;
       int i;
       int32 dataoffset;
       char* dat = NULL;
       int iitem;
       errno_t rc = 0;

       i = list_length(astate->elements);
       subdata = (char**)palloc(i * sizeof(char*));
       subbitmaps = (bits8**)palloc(i * sizeof(bits8*));
       subbytes = (int*)palloc(i * sizeof(int));
       subnitems = (int*)palloc(i * sizeof(int));

       /* loop through and get data area from each element */
       foreach (element, astate->elements) {
           ExprState* e = (ExprState*)lfirst(element);
           bool eisnull = false;
           Datum arraydatum;
           ArrayType* array = NULL;
           int this_ndims;

           arraydatum = ExecEvalExpr(e, econtext, &eisnull, NULL);
           /* temporarily ignore null subarrays */
           if (eisnull) {
               haveempty = true;
               continue;
           }

           array = DatumGetArrayTypeP(arraydatum);

           /* run-time double-check on element type */
           if (element_type != ARR_ELEMTYPE(array))
               ereport(ERROR,
                       (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("cannot merge incompatible arrays"),
                        errdetail("Array with element type %s cannot be "
                                  "included in ARRAY construct with element type %s.",
                                  format_type_be(ARR_ELEMTYPE(array)),
                                  format_type_be(element_type))));

           this_ndims = ARR_NDIM(array);
           /* temporarily ignore zero-dimensional subarrays */
           if (this_ndims <= 0) {
               haveempty = true;
               continue;
           }

           if (firstone) {
               /* Get sub-array details from first member */
               elem_ndims = this_ndims;
               ndims = elem_ndims + 1;
               if (ndims <= 0 || ndims > MAXDIM)
                   ereport(ERROR,
                           (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                            errmsg("number of array dimensions (%d) exceeds "
                                   "the maximum allowed (%d)",
                                   ndims,
                                   MAXDIM)));

               elem_dims = (int*)palloc(elem_ndims * sizeof(int));
               rc = memcpy_s(elem_dims, elem_ndims * sizeof(int), ARR_DIMS(array), elem_ndims * sizeof(int));
               securec_check(rc, "\0", "\0");

               elem_lbs = (int*)palloc(elem_ndims * sizeof(int));
               rc = memcpy_s(elem_lbs, elem_ndims * sizeof(int), ARR_LBOUND(array), elem_ndims * sizeof(int));
               securec_check(rc, "\0", "\0");

               firstone = false;
           } else {
               /* Check other sub-arrays are compatible */
               if (elem_ndims != this_ndims || memcmp(elem_dims, ARR_DIMS(array), elem_ndims * sizeof(int)) != 0 ||
                   memcmp(elem_lbs, ARR_LBOUND(array), elem_ndims * sizeof(int)) != 0)
                   ereport(ERROR,
                           (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                            errmsg("multidimensional arrays must have array expressions with matching dimensions")));
           }

           subdata[outer_nelems] = ARR_DATA_PTR(array);
           subbitmaps[outer_nelems] = ARR_NULLBITMAP(array);
           subbytes[outer_nelems] = ARR_SIZE(array) - ARR_DATA_OFFSET(array);
           nbytes += subbytes[outer_nelems];
           subnitems[outer_nelems] = ArrayGetNItems(this_ndims, ARR_DIMS(array));
           nitems += subnitems[outer_nelems];
           if (ARR_HASNULL(array))
               havenulls = true;
           outer_nelems++;
       }

       /*
        * If all items were null or empty arrays, return an empty array;
        * otherwise, if some were and some weren't, raise error.  (Note: we
        * must special-case this somehow to avoid trying to generate a 1-D
        * array formed from empty arrays.	It's not ideal...)
        */
       if (haveempty) {
           if (ndims == 0) /* didn't find any nonempty array */
               return PointerGetDatum(construct_empty_array(element_type));
           ereport(ERROR,
                   (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                    errmsg("multidimensional arrays must have array expressions with matching dimensions")));
       }

       /* setup for multi-D array */
       dims[0] = outer_nelems;
       lbs[0] = 1;
       for (i = 1; i < ndims; i++) {
           dims[i] = elem_dims[i - 1];
           lbs[i] = elem_lbs[i - 1];
       }

       if (havenulls) {
           dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nitems);
           nbytes += dataoffset;
       } else {
           dataoffset = 0; /* marker for no null bitmap */
           nbytes += ARR_OVERHEAD_NONULLS(ndims);
       }

       result = (ArrayType*)palloc(nbytes);
       SET_VARSIZE(result, nbytes);
       result->ndim = ndims;
       result->dataoffset = dataoffset;
       result->elemtype = element_type;
       rc = memcpy_s(ARR_DIMS(result), ndims * sizeof(int), dims, ndims * sizeof(int));
       securec_check(rc, "\0", "\0");
       rc = memcpy_s(ARR_LBOUND(result), ndims * sizeof(int), lbs, ndims * sizeof(int));
       securec_check(rc, "\0", "\0");

       dat = ARR_DATA_PTR(result);

       int len = (nbytes - ARR_DATA_OFFSET(result));
       iitem = 0;
       for (i = 0; i < outer_nelems; i++) {
           /* make sure the destMax of memcpy_s should never be zero. */
           if (subbytes[i] != 0) {
               rc = memcpy_s(dat, len, subdata[i], subbytes[i]);
               securec_check(rc, "\0", "\0");
           }

           dat += subbytes[i];
           len -= subbytes[i];
           if (havenulls)
               array_bitmap_copy(ARR_NULLBITMAP(result), iitem, subbitmaps[i], 0, subnitems[i]);
           iitem += subnitems[i];
       }
   }

   return PointerGetDatum(result);
}

/* ----------------------------------------------------------------
*		ExecEvalRow - ROW() expressions
* ----------------------------------------------------------------
*/
static Datum ExecEvalRow(RowExprState* rstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   HeapTuple tuple;
   Datum* values = NULL;
   bool* isnull = NULL;
   int natts;
   ListCell* arg = NULL;
   int i;
   errno_t rc = EOK;

   /* Set default values for result flags: non-null, not a set result */
   *isNull = false;
   if (isDone != NULL)
       *isDone = ExprSingleResult;

   /* Allocate workspace */
   natts = rstate->tupdesc->natts;
   values = (Datum*)palloc0(natts * sizeof(Datum));
   isnull = (bool*)palloc(natts * sizeof(bool));

   /* preset to nulls in case rowtype has some later-added columns */
   rc = memset_s(isnull, natts * sizeof(bool), true, natts * sizeof(bool));
   securec_check(rc, "\0", "\0");

   /* Evaluate field values */
   i = 0;
   foreach (arg, rstate->args) {
       ExprState* e = (ExprState*)lfirst(arg);

       values[i] = ExecEvalExpr(e, econtext, &isnull[i], NULL);
       i++;
   }

   tuple = (HeapTuple)tableam_tops_form_tuple(rstate->tupdesc, values, isnull);

   pfree_ext(values);
   pfree_ext(isnull);

   return HeapTupleGetDatum(tuple);
}

/* ----------------------------------------------------------------
*		ExecEvalRowCompare - ROW() comparison-op ROW()
* ----------------------------------------------------------------
*/
static Datum ExecEvalRowCompare(RowCompareExprState* rstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   bool result = false;
   RowCompareType rctype = ((RowCompareExpr*)rstate->xprstate.expr)->rctype;
   int32 cmpresult = 0;
   ListCell* l = NULL;
   ListCell* r = NULL;
   int i;

   if (isDone != NULL)
       *isDone = ExprSingleResult;
   *isNull = true; /* until we get a result */

   i = 0;
   forboth(l, rstate->largs, r, rstate->rargs)
   {
       ExprState* le = (ExprState*)lfirst(l);
       ExprState* re = (ExprState*)lfirst(r);
       FunctionCallInfoData locfcinfo;

       InitFunctionCallInfoData(locfcinfo, &(rstate->funcs[i]), 2, rstate->collations[i], NULL, NULL);
       locfcinfo.arg[0] = ExecEvalExpr(le, econtext, &locfcinfo.argnull[0], NULL);
       locfcinfo.arg[1] = ExecEvalExpr(re, econtext, &locfcinfo.argnull[1], NULL);
       if (rstate->funcs[i].fn_strict && (locfcinfo.argnull[0] || locfcinfo.argnull[1]))
           return (Datum)0; /* force NULL result */
       locfcinfo.isnull = false;
       cmpresult = DatumGetInt32(FunctionCallInvoke(&locfcinfo));
       if (locfcinfo.isnull)
           return (Datum)0; /* force NULL result */
       if (cmpresult != 0)
           break; /* no need to compare remaining columns */
       i++;
   }

   switch (rctype) {
           /* EQ and NE cases aren't allowed here */
       case ROWCOMPARE_LT:
           result = (cmpresult < 0);
           break;
       case ROWCOMPARE_LE:
           result = (cmpresult <= 0);
           break;
       case ROWCOMPARE_GE:
           result = (cmpresult >= 0);
           break;
       case ROWCOMPARE_GT:
           result = (cmpresult > 0);
           break;
       default:
           ereport(ERROR,
                   (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_EXECUTOR),
                    errmsg("unrecognized RowCompareType: %d", (int)rctype)));
           result = 0; /* keep compiler quiet */
           break;
   }

   *isNull = false;
   return BoolGetDatum(result);
}

/* ----------------------------------------------------------------
*		ExecEvalCoalesce
* ----------------------------------------------------------------
*/
static Datum ExecEvalCoalesce(
   CoalesceExprState* coalesceExpr, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   ListCell* arg = NULL;

   if (isDone != NULL)
       *isDone = ExprSingleResult;

   /* Simply loop through until something NOT NULL is found */
   foreach (arg, coalesceExpr->args) {
       ExprState* e = (ExprState*)lfirst(arg);
       Datum value;

       value = ExecEvalExpr(e, econtext, isNull, NULL);
       if (!*isNull)
           return value;
   }

   /* Else return NULL */
   *isNull = true;
   return (Datum)0;
}

/* ----------------------------------------------------------------
*		ExecEvalMinMax
* ----------------------------------------------------------------
*/
static Datum ExecEvalMinMax(MinMaxExprState* minmaxExpr, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   Datum result = (Datum)0;
   MinMaxExpr* minmax = (MinMaxExpr*)minmaxExpr->xprstate.expr;
   Oid collation = minmax->inputcollid;
   MinMaxOp op = minmax->op;
   FunctionCallInfoData locfcinfo;
   ListCell* arg = NULL;

   if (isDone != NULL)
       *isDone = ExprSingleResult;
   *isNull = true; /* until we get a result */

   InitFunctionCallInfoData(locfcinfo, &minmaxExpr->cfunc, 2, collation, NULL, NULL);
   locfcinfo.argnull[0] = false;
   locfcinfo.argnull[1] = false;

   foreach (arg, minmaxExpr->args) {
       ExprState* e = (ExprState*)lfirst(arg);
       Datum value;
       bool valueIsNull = false;
       int32 cmpresult;

       value = ExecEvalExpr(e, econtext, &valueIsNull, NULL);
       if (valueIsNull)
           continue; /* ignore NULL inputs */

       if (*isNull) {
           /* first nonnull input, adopt value */
           result = value;
           *isNull = false;
       } else {
           /* apply comparison function */
           locfcinfo.arg[0] = result;
           locfcinfo.arg[1] = value;
           locfcinfo.isnull = false;
           cmpresult = DatumGetInt32(FunctionCallInvoke(&locfcinfo));
           if (locfcinfo.isnull) /* probably should not happen */
               continue;
           if (cmpresult > 0 && op == IS_LEAST)
               result = value;
           else if (cmpresult < 0 && op == IS_GREATEST)
               result = value;
       }
   }

   return result;
}

/* ----------------------------------------------------------------
*		ExecEvalXml
* ----------------------------------------------------------------
*/
static Datum ExecEvalXml(XmlExprState* xmlExpr, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   XmlExpr* xexpr = (XmlExpr*)xmlExpr->xprstate.expr;
   Datum value;
   bool isnull = false;
   ListCell* arg = NULL;
   ListCell* narg = NULL;

   if (isDone != NULL)
       *isDone = ExprSingleResult;
   *isNull = true; /* until we get a result */

   switch (xexpr->op) {
       case IS_XMLCONCAT: {
           List* values = NIL;

           foreach (arg, xmlExpr->args) {
               ExprState* e = (ExprState*)lfirst(arg);

               value = ExecEvalExpr(e, econtext, &isnull, NULL);
               if (!isnull)
                   values = lappend(values, DatumGetPointer(value));
           }

           if (list_length(values) > 0) {
               *isNull = false;
               return PointerGetDatum(xmlconcat(values));
           } else
               return (Datum)0;
       } break;

       case IS_XMLFOREST: {
           StringInfoData buf;

           initStringInfo(&buf);
           forboth(arg, xmlExpr->named_args, narg, xexpr->arg_names)
           {
               ExprState* e = (ExprState*)lfirst(arg);
               char* argname = strVal(lfirst(narg));

               value = ExecEvalExpr(e, econtext, &isnull, NULL);
               if (!isnull) {
                   appendStringInfo(&buf,
                                    "<%s>%s</%s>",
                                    argname,
                                    map_sql_value_to_xml_value(value, exprType((Node*)e->expr), true),
                                    argname);
                   *isNull = false;
               }
           }

           if (*isNull) {
               pfree_ext(buf.data);
               return (Datum)0;
           } else {
               text* result = NULL;

               result = cstring_to_text_with_len(buf.data, buf.len);
               pfree_ext(buf.data);

               return PointerGetDatum(result);
           }
       } break;

       case IS_XMLELEMENT:
           *isNull = false;
           return PointerGetDatum(xmlelement(xmlExpr, econtext));
           break;

       case IS_XMLPARSE: {
           ExprState* e = NULL;
           text* data = NULL;
           bool preserve_whitespace = false;

           /* arguments are known to be text, bool */
           Assert(list_length(xmlExpr->args) == 2);

           e = (ExprState*)linitial(xmlExpr->args);
           value = ExecEvalExpr(e, econtext, &isnull, NULL);
           if (isnull)
               return (Datum)0;
           data = DatumGetTextP(value);

           e = (ExprState*)lsecond(xmlExpr->args);
           value = ExecEvalExpr(e, econtext, &isnull, NULL);
           if (isnull) /* probably can't happen */
               return (Datum)0;
           preserve_whitespace = DatumGetBool(value);

           *isNull = false;

           return PointerGetDatum(xmlparse(data, xexpr->xmloption, preserve_whitespace));
       } break;

       case IS_XMLPI: {
           ExprState* e = NULL;
           text* argument = NULL;

           /* optional argument is known to be text */
           Assert(list_length(xmlExpr->args) <= 1);

           if (xmlExpr->args) {
               e = (ExprState*)linitial(xmlExpr->args);
               value = ExecEvalExpr(e, econtext, &isnull, NULL);
               if (isnull)
                   argument = NULL;
               else
                   argument = DatumGetTextP(value);
           } else {
               argument = NULL;
               isnull = false;
           }

           return PointerGetDatum(xmlpi(xexpr->name, argument, isnull, isNull));
       } break;

       case IS_XMLROOT: {
           ExprState* e = NULL;
           xmltype* data = NULL;
           text* version = NULL;
           int standalone;

           /* arguments are known to be xml, text, int */
           Assert(list_length(xmlExpr->args) == 3);

           e = (ExprState*)linitial(xmlExpr->args);
           value = ExecEvalExpr(e, econtext, &isnull, NULL);
           if (isnull)
               return (Datum)0;
           data = DatumGetXmlP(value);

           e = (ExprState*)lsecond(xmlExpr->args);
           value = ExecEvalExpr(e, econtext, &isnull, NULL);
           if (isnull)
               version = NULL;
           else
               version = DatumGetTextP(value);

           e = (ExprState*)lthird(xmlExpr->args);
           value = ExecEvalExpr(e, econtext, &isnull, NULL);
           standalone = DatumGetInt32(value);

           *isNull = false;

           return PointerGetDatum(xmlroot(data, version, standalone));
       } break;

       case IS_XMLSERIALIZE: {
           ExprState* e = NULL;

           /* argument type is known to be xml */
           Assert(list_length(xmlExpr->args) == 1);

           e = (ExprState*)linitial(xmlExpr->args);
           value = ExecEvalExpr(e, econtext, &isnull, NULL);
           if (isnull)
               return (Datum)0;

           *isNull = false;

           return PointerGetDatum(xmltotext_with_xmloption(DatumGetXmlP(value), xexpr->xmloption));
       } break;

       case IS_DOCUMENT: {
           ExprState* e = NULL;

           /* optional argument is known to be xml */
           Assert(list_length(xmlExpr->args) == 1);

           e = (ExprState*)linitial(xmlExpr->args);
           value = ExecEvalExpr(e, econtext, &isnull, NULL);
           if (isnull)
               return (Datum)0;
           else {
               *isNull = false;
               return BoolGetDatum(xml_is_document(DatumGetXmlP(value)));
           }
       } break;
       default:
           break;
   }

   ereport(ERROR,
           (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
            errmodule(MOD_EXECUTOR),
            errmsg("unrecognized XML operation %d", xexpr->op)));
   return (Datum)0;
}

/* ----------------------------------------------------------------
*		ExecEvalNullIf
*
* Note that this is *always* derived from the equals operator,
* but since we need special processing of the arguments
* we can not simply reuse ExecEvalOper() or ExecEvalFunc().
* ----------------------------------------------------------------
*/
static Datum ExecEvalNullIf(FuncExprState* nullIfExpr, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   Datum result;
   FunctionCallInfo fcinfo;
   ExprDoneCond argDone;

   if (isDone != NULL)
       *isDone = ExprSingleResult;

    /*
     * Initialize function cache if first time through
     */
    if (nullIfExpr->func.fn_oid == InvalidOid) {
        NullIfExpr* op = (NullIfExpr*)nullIfExpr->xprstate.expr;
        if (nullIfExpr->xprstate.is_flt_frame) {
            init_fcache<false>(op->opfuncid, op->inputcollid, nullIfExpr,
                               econtext->ecxt_per_query_memory, false, false);
        } else {
            init_fcache<false>(op->opfuncid, op->inputcollid, nullIfExpr,
                               econtext->ecxt_per_query_memory, false, true);
            Assert(!nullIfExpr->func.fn_retset);
        }
    }

   /*
    * Evaluate arguments
    */
   fcinfo = &nullIfExpr->fcinfo_data;
   argDone = ExecEvalFuncArgs<false>(fcinfo, nullIfExpr->args, econtext);
   if (argDone != ExprSingleResult)
       ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("NULLIF does not support set arguments")));
   Assert(fcinfo->nargs == 2);

   /* if either argument is NULL they can't be equal */
   if (!fcinfo->argnull[0] && !fcinfo->argnull[1]) {
       fcinfo->isnull = false;
       result = FunctionCallInvoke(fcinfo);
       /* if the arguments are equal return null */
       if (!fcinfo->isnull && DatumGetBool(result)) {
           *isNull = true;
           return (Datum)0;
       }
   }

   /* else return first argument */
   *isNull = fcinfo->argnull[0];
   return fcinfo->arg[0];
}

static Datum CheckRowTypeIsNull(TupleDesc tupDesc, HeapTupleData tmptup, NullTest *ntest)
{
   int att;

   for (att = 1; att <= tupDesc->natts; att++) {
       /* ignore dropped columns */
       if (tupDesc->attrs[att - 1].attisdropped)
           continue;
       if (tableam_tops_tuple_attisnull(&tmptup, att, tupDesc)) {
           /* null field disproves IS NOT NULL */
           if (ntest->nulltesttype == IS_NOT_NULL)
               return BoolGetDatum(false);
       } else {
           /* non-null field disproves IS NULL */
           if (ntest->nulltesttype == IS_NULL)
               return BoolGetDatum(false);
       }
   }

   return BoolGetDatum(true);
}

static Datum CheckRowTypeIsNullForAFormat(TupleDesc tupDesc, HeapTupleData tmptup, NullTest *ntest)
{
   int att;

   for (att = 1; att <= tupDesc->natts; att++) {
       /* ignore dropped columns */
       if (tupDesc->attrs[att - 1].attisdropped)
           continue;
       if (!tableam_tops_tuple_attisnull(&tmptup, att, tupDesc)) {
           /* non-null field disproves IS NULL */
           if (ntest->nulltesttype == IS_NULL) {
               return BoolGetDatum(false);
           } else {
               return BoolGetDatum(true);
           }
       }
   }

   /* non-null field disproves IS NULL */
   if (ntest->nulltesttype == IS_NULL) {
       return BoolGetDatum(true);
   } else {
       return BoolGetDatum(false);
   }
}

/* ----------------------------------------------------------------
*		ExecEvalNullTest
*
*		Evaluate a NullTest node.
* ----------------------------------------------------------------
*/
static Datum ExecEvalNullTest(NullTestState* nstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   NullTest* ntest = (NullTest*)nstate->xprstate.expr;
   Datum result;

   result = ExecEvalExpr(nstate->arg, econtext, isNull, isDone);

   if (isDone && *isDone == ExprEndResult)
       return result; /* nothing to check */

   if (ntest->argisrow && !(*isNull)) {
       /*
        * The SQL standard defines IS [NOT] NULL for a non-null rowtype
        * argument as:
        *
        * "R IS NULL" is true if every field is the null value.
        *
        * "R IS NOT NULL" is true if no field is the null value.
        *
        * This definition is (apparently intentionally) not recursive; so our
        * tests on the fields are primitive attisnull tests, not recursive
        * checks to see if they are all-nulls or no-nulls rowtypes.
        *
        * The standard does not consider the possibility of zero-field rows,
        * but here we consider them to vacuously satisfy both predicates.
        *
        * e.g.
        *            r      | isnull | isnotnull
        *      -------------+--------+-----------
        *       (1,"(1,2)") | f      | t
        *       (1,"(,)")   | f      | t
        *       (1,)        | f      | f
        *       (,"(1,2)")  | f      | f
        *       (,"(,)")    | f      | f
        *       (,)         | t      | f
        *
        */
       HeapTupleHeader tuple;
       Oid tupType;
       int32 tupTypmod;
       TupleDesc tupDesc;
       HeapTupleData tmptup;

       tuple = DatumGetHeapTupleHeader(result);

       tupType = HeapTupleHeaderGetTypeId(tuple);
       tupTypmod = HeapTupleHeaderGetTypMod(tuple);

       /* Lookup tupdesc if first time through or if type changes */
       tupDesc = get_cached_rowtype(tupType, tupTypmod, &nstate->argdesc, econtext);

       /*
        * heap_attisnull needs a HeapTuple not a bare HeapTupleHeader.
        */
       tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
       tmptup.t_data = tuple;

       if (AFORMAT_NULL_TEST_MODE) {
           return CheckRowTypeIsNullForAFormat(tupDesc, tmptup, ntest);
       } else {
           return CheckRowTypeIsNull(tupDesc, tmptup, ntest);
       }
   } else {
       /* Simple scalar-argument case, or a null rowtype datum */
       switch (ntest->nulltesttype) {
           case IS_NULL:
               if (*isNull) {
                   *isNull = false;
                   return BoolGetDatum(true);
               } else
                   return BoolGetDatum(false);
           case IS_NOT_NULL:
               if (*isNull) {
                   *isNull = false;
                   return BoolGetDatum(false);
               } else
                   return BoolGetDatum(true);
           default:
               ereport(ERROR,
                       (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_EXECUTOR),
                        errmsg("unrecognized nulltesttype: %d", (int)ntest->nulltesttype)));
               return (Datum)0; /* keep compiler quiet */
       }
   }
}

/* ----------------------------------------------------------------
*		ExecEvalHashFilter
*
*		Evaluate a HashFilter node.
* ----------------------------------------------------------------
*/
static Datum ExecEvalHashFilter(HashFilterState* hstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   HashFilter* htest = (HashFilter*)hstate->xprstate.expr;
   Datum result = 0;
   Datum value = 0;
   uint64 hashValue = 0;
   int modulo = 0;
   int nodeIndex = 0;
   ListCell *distkey = NULL;
   ListCell *vartypes = NULL;
   bool isFirst = true;
   bool hasNonNullValue = false;

   if (isDone != NULL)
       *isDone = ExprSingleResult;
   *isNull = true; /* until we get a result */

   /* Get every distribute key in arg and compute hash value */
   forboth(distkey, hstate->arg, vartypes, htest->typeOids)
   {
       ExprState* e = (ExprState*)lfirst(distkey);
       Oid vartype = (Oid)lfirst_oid(vartypes);
       value = ExecEvalExpr(e, econtext, isNull, isDone);

       int null_value_dn_index = (hstate->nodelist != NULL) ? hstate->nodelist[0]
                                                            : /* fetch first dn in group's dn list */
                                     0;                       /* fetch first dn index */

       if (*isNull) {
           if (null_value_dn_index == u_sess->pgxc_cxt.PGXCNodeId) {
               *isNull = false;
               result = BoolGetDatum(true);
           } else
               result = BoolGetDatum(false);
       } else {
           if (isFirst) {
               hashValue = compute_hash(vartype, value, LOCATOR_TYPE_HASH);
               isFirst = false;
           } else {
               hashValue = (hashValue << 1) | ((hashValue & 0x80000000) ? 1 : 0);
               hashValue ^= compute_hash(vartype, value, LOCATOR_TYPE_HASH);
           }

           hasNonNullValue = true;
       }
   }

   /* If has non null value, it should get nodeId and deside if need filter the value or not. */
   if (hasNonNullValue) {
       modulo = hstate->bucketMap[abs((int)hashValue) & (hstate->bucketCnt - 1)];
       nodeIndex = hstate->nodelist[modulo];

       /* If there are null value and non null value, and the last value in distkey is null,
           we should set isNull is false. */
       *isNull = false;
       /* Look into the handles and return correct position in array */
       if (nodeIndex == u_sess->pgxc_cxt.PGXCNodeId)
           return BoolGetDatum(true);
       else
           return BoolGetDatum(false);
   } else /* If all the value is null, return result. */
       return result;
}

/* ----------------------------------------------------------------
*		ExecEvalBooleanTest
*
*		Evaluate a BooleanTest node.
* ----------------------------------------------------------------
*/
static Datum ExecEvalBooleanTest(GenericExprState* bstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   BooleanTest* btest = (BooleanTest*)bstate->xprstate.expr;
   Datum result;

   result = ExecEvalExpr(bstate->arg, econtext, isNull, isDone);

   if (isDone && *isDone == ExprEndResult)
       return result; /* nothing to check */

   switch (btest->booltesttype) {
       case IS_TRUE:
           if (*isNull) {
               *isNull = false;
               return BoolGetDatum(false);
           } else if (DatumGetBool(result))
               return BoolGetDatum(true);
           else
               return BoolGetDatum(false);
       case IS_NOT_TRUE:
           if (*isNull) {
               *isNull = false;
               return BoolGetDatum(true);
           } else if (DatumGetBool(result))
               return BoolGetDatum(false);
           else
               return BoolGetDatum(true);
       case IS_FALSE:
           if (*isNull) {
               *isNull = false;
               return BoolGetDatum(false);
           } else if (DatumGetBool(result))
               return BoolGetDatum(false);
           else
               return BoolGetDatum(true);
       case IS_NOT_FALSE:
           if (*isNull) {
               *isNull = false;
               return BoolGetDatum(true);
           } else if (DatumGetBool(result))
               return BoolGetDatum(true);
           else
               return BoolGetDatum(false);
       case IS_UNKNOWN:
           if (*isNull) {
               *isNull = false;
               return BoolGetDatum(true);
           } else
               return BoolGetDatum(false);
       case IS_NOT_UNKNOWN:
           if (*isNull) {
               *isNull = false;
               return BoolGetDatum(false);
           } else
               return BoolGetDatum(true);
       default:
           ereport(ERROR,
                   (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_EXECUTOR),
                    errmsg("unrecognized booltesttype: %d", (int)btest->booltesttype)));
           return (Datum)0; /* keep compiler quiet */
   }
}

/*
* ExecEvalCoerceToDomain
*
* Test the provided data against the domain constraint(s).  If the data
* passes the constraint specifications, pass it through (return the
* datum) otherwise throw an error.
*/
static Datum ExecEvalCoerceToDomain(
   CoerceToDomainState* cstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   CoerceToDomain* ctest = (CoerceToDomain*)cstate->xprstate.expr;
   Datum result;
   ListCell* l = NULL;

   result = ExecEvalExpr(cstate->arg, econtext, isNull, isDone);

   if (isDone && *isDone == ExprEndResult)
       return result; /* nothing to check */

   foreach (l, cstate->constraints) {
       DomainConstraintState* con = (DomainConstraintState*)lfirst(l);

       switch (con->constrainttype) {
           case DOM_CONSTRAINT_NOTNULL:
               if (*isNull)
                   ereport(ERROR,
                           (errcode(ERRCODE_NOT_NULL_VIOLATION),
                            errmsg("domain %s does not allow null values", format_type_be(ctest->resulttype))));
               break;
           case DOM_CONSTRAINT_CHECK: {
               Datum conResult;
               bool conIsNull = false;
               Datum save_datum;
               bool save_isNull = false;

               /*
                * Set up value to be returned by CoerceToDomainValue
                * nodes. We must save and restore prior setting of
                * econtext's domainValue fields, in case this node is
                * itself within a check expression for another domain.
                */
               save_datum = econtext->domainValue_datum;
               save_isNull = econtext->domainValue_isNull;

               econtext->domainValue_datum = result;
               econtext->domainValue_isNull = *isNull;

               conResult = ExecEvalExpr(con->check_expr, econtext, &conIsNull, NULL);

               if (!conIsNull && !DatumGetBool(conResult))
                   ereport(ERROR,
                           (errcode(ERRCODE_CHECK_VIOLATION),
                            errmsg("value for domain %s violates check constraint \"%s\"",
                                   format_type_be(ctest->resulttype),
                                   con->name)));
               econtext->domainValue_datum = save_datum;
               econtext->domainValue_isNull = save_isNull;

               break;
           }
           default:
               ereport(ERROR,
                       (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmodule(MOD_EXECUTOR),
                        errmsg("unrecognized constraint type: %d", (int)con->constrainttype)));
               break;
       }
   }

   /* If all has gone well (constraints did not fail) return the datum */
   return result;
}

/*
* ExecEvalCoerceToDomainValue
*
* Return the value stored by CoerceToDomain.
*/
static Datum ExecEvalCoerceToDomainValue(
   ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   if (isDone != NULL)
       *isDone = ExprSingleResult;
   *isNull = econtext->domainValue_isNull;
   return econtext->domainValue_datum;
}

/* ----------------------------------------------------------------
*		ExecEvalFieldSelect
*
*		Evaluate a FieldSelect node.
* ----------------------------------------------------------------
*/
static Datum ExecEvalFieldSelect(FieldSelectState* fstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   FieldSelect* fselect = (FieldSelect*)fstate->xprstate.expr;
   AttrNumber fieldnum = fselect->fieldnum;
   Datum result;
   Datum tupDatum;
   HeapTupleHeader tuple;
   Oid tupType;
   int32 tupTypmod;
   TupleDesc tupDesc;
   Form_pg_attribute attr;
   HeapTupleData tmptup;

   tupDatum = ExecEvalExpr(fstate->arg, econtext, isNull, isDone);

   /* this test covers the isDone exception too: */
   if (*isNull)
       return tupDatum;

   tuple = DatumGetHeapTupleHeader(tupDatum);

   tupType = HeapTupleHeaderGetTypeId(tuple);
   tupTypmod = HeapTupleHeaderGetTypMod(tuple);

   /* Lookup tupdesc if first time through or if type changes */
   tupDesc = get_cached_rowtype(tupType, tupTypmod, &fstate->argdesc, econtext);

   /*
    * Find field's attr record.  Note we don't support system columns here: a
    * datum tuple doesn't have valid values for most of the interesting
    * system columns anyway.
    */
   if (fieldnum <= 0) /* should never happen */
       ereport(ERROR,
               (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmodule(MOD_EXECUTOR),
                errmsg("unsupported reference to system column %d in FieldSelect", fieldnum)));
   if (fieldnum > tupDesc->natts) /* should never happen */
       ereport(ERROR,
               (errcode(ERRCODE_INVALID_ATTRIBUTE),
                errmodule(MOD_EXECUTOR),
                errmsg("attribute number %d exceeds number of columns %d", fieldnum, tupDesc->natts)));
   attr = &tupDesc->attrs[fieldnum - 1];

   /* Check for dropped column, and force a NULL result if so */
   if (attr->attisdropped) {
       *isNull = true;
       return (Datum)0;
   }

   /* Check for type mismatch --- possible after ALTER COLUMN TYPE? */
   /* As in ExecEvalScalarVar, we should but can't check typmod */
   if (fselect->resulttype != attr->atttypid)
       ereport(ERROR,
               (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("attribute %d has wrong type", fieldnum),
                errdetail("Table has type %s, but query expects %s.",
                          format_type_be(attr->atttypid),
                          format_type_be(fselect->resulttype))));

   /* heap_getattr needs a HeapTuple not a bare HeapTupleHeader */
   tmptup.t_len = HeapTupleHeaderGetDatumLength(tuple);
   tmptup.t_data = tuple;

   result = tableam_tops_tuple_getattr(&tmptup, fieldnum, tupDesc, isNull);
   return result;
}

/* ----------------------------------------------------------------
*		ExecEvalFieldStore
*
*		Evaluate a FieldStore node.
* ----------------------------------------------------------------
*/
static Datum ExecEvalFieldStore(FieldStoreState* fstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   FieldStore* fstore = (FieldStore*)fstate->xprstate.expr;
   HeapTuple tuple;
   Datum tupDatum;
   TupleDesc tupDesc;
   Datum* values = NULL;
   bool* isnull = NULL;
   Datum save_datum;
   bool save_isNull = false;
   ListCell* l1 = NULL;
   ListCell* l2 = NULL;
   errno_t rc = EOK;

   tupDatum = ExecEvalExpr(fstate->arg, econtext, isNull, isDone);

   if (isDone != NULL && *isDone == ExprEndResult)
       return tupDatum;

   /* Lookup tupdesc if first time through or after rescan */
   tupDesc = get_cached_rowtype(fstore->resulttype, -1, &fstate->argdesc, econtext);

   /* Allocate workspace */
   values = (Datum*)palloc(tupDesc->natts * sizeof(Datum));
   isnull = (bool*)palloc(tupDesc->natts * sizeof(bool));

   if (!*isNull) {
       /*
        * heap_deform_tuple needs a HeapTuple not a bare HeapTupleHeader. We
        * set all the fields in the struct just in case.
        */
       HeapTupleHeader tuphdr;
       HeapTupleData tmptup;

       tuphdr = DatumGetHeapTupleHeader(tupDatum);
       tmptup.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
       ItemPointerSetInvalid(&(tmptup.t_self));
       tmptup.t_tableOid = InvalidOid;
       tmptup.t_bucketId = InvalidBktId;
#ifdef PGXC
       tmptup.t_xc_node_id = 0;
#endif
       HeapTupleSetZeroBase(&tmptup);
       tmptup.t_data = tuphdr;

       tableam_tops_deform_tuple(&tmptup, tupDesc, values, isnull);
   } else {
       /* Convert null input tuple into an all-nulls row */
       rc = memset_s(isnull, tupDesc->natts * sizeof(bool), true, tupDesc->natts * sizeof(bool));
       securec_check(rc, "\0", "\0");
   }

   /* Result is never null */
   *isNull = false;

   save_datum = econtext->caseValue_datum;
   save_isNull = econtext->caseValue_isNull;

   forboth(l1, fstate->newvals, l2, fstore->fieldnums)
   {
       ExprState* newval = (ExprState*)lfirst(l1);
       AttrNumber fieldnum = lfirst_int(l2);

       Assert(fieldnum > 0 && fieldnum <= tupDesc->natts);

       /*
        * Use the CaseTestExpr mechanism to pass down the old value of the
        * field being replaced; this is needed in case the newval is itself a
        * FieldStore or ArrayRef that has to obtain and modify the old value.
        * It's safe to reuse the CASE mechanism because there cannot be a
        * CASE between here and where the value would be needed, and a field
        * assignment can't be within a CASE either.  (So saving and restoring
        * the caseValue is just paranoia, but let's do it anyway.)
        */
       econtext->caseValue_datum = values[fieldnum - 1];
       econtext->caseValue_isNull = isnull[fieldnum - 1];

       values[fieldnum - 1] = ExecEvalExpr(newval, econtext, &isnull[fieldnum - 1], NULL);
   }

   econtext->caseValue_datum = save_datum;
   econtext->caseValue_isNull = save_isNull;

   tuple = (HeapTuple)tableam_tops_form_tuple(tupDesc, values, isnull);

   pfree_ext(values);
   pfree_ext(isnull);

   return HeapTupleGetDatum(tuple);
}

/* ----------------------------------------------------------------
*		ExecEvalRelabelType
*
*		Evaluate a RelabelType node.
* ----------------------------------------------------------------
*/
static Datum ExecEvalRelabelType(GenericExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   return ExecEvalExpr(exprstate->arg, econtext, isNull, isDone);
}

/* ----------------------------------------------------------------
*		ExecEvalCoerceViaIO
*
*		Evaluate a CoerceViaIO node.
* ----------------------------------------------------------------
*/
static Datum ExecEvalCoerceViaIO(CoerceViaIOState* iostate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   Datum result;
   Datum inputval;
   char* string = NULL;

   inputval = ExecEvalExpr(iostate->arg, econtext, isNull, isDone);

   if (isDone && *isDone == ExprEndResult)
       return inputval; /* nothing to do */

   if (*isNull)
       string = NULL; /* output functions are not called on nulls */
   else
       string = OutputFunctionCall(&iostate->outfunc, inputval);

   result = InputFunctionCall(&iostate->infunc, string, iostate->intypioparam, -1);

   /* The input function cannot change the null/not-null status */
   return result;
}

/* ----------------------------------------------------------------
*		ExecEvalArrayCoerceExpr
*
*		Evaluate an ArrayCoerceExpr node.
* ----------------------------------------------------------------
*/
static Datum ExecEvalArrayCoerceExpr(
   ArrayCoerceExprState* astate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   ArrayCoerceExpr* acoerce = (ArrayCoerceExpr*)astate->xprstate.expr;
   Datum result;
   ArrayType* array = NULL;
   FunctionCallInfoData locfcinfo;

   result = ExecEvalExpr(astate->arg, econtext, isNull, isDone);

   if (isDone && *isDone == ExprEndResult)
       return result; /* nothing to do */
   if (*isNull)
       return result; /* nothing to do */

   /*
    * If it's binary-compatible, modify the element type in the array header,
    * but otherwise leave the array as we received it.
    */
   if (!OidIsValid(acoerce->elemfuncid)) {
       /* Detoast input array if necessary, and copy in any case */
       array = DatumGetArrayTypePCopy(result);
       ARR_ELEMTYPE(array) = astate->resultelemtype;
       PG_RETURN_ARRAYTYPE_P(array);
   }

   /* Detoast input array if necessary, but don't make a useless copy */
   array = DatumGetArrayTypeP(result);

   /* Initialize function cache if first time through */
   if (astate->elemfunc.fn_oid == InvalidOid) {
       AclResult aclresult;

       /* Check permission to call function */
       aclresult = pg_proc_aclcheck(acoerce->elemfuncid, GetUserId(), ACL_EXECUTE);
       if (aclresult != ACLCHECK_OK)
           aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(acoerce->elemfuncid));

       /* Set up the primary fmgr lookup information */
       fmgr_info_cxt(acoerce->elemfuncid, &(astate->elemfunc), econtext->ecxt_per_query_memory);
       fmgr_info_set_expr((Node*)acoerce, &(astate->elemfunc));
   }

   /*
    * Use array_map to apply the function to each array element.
    *
    * We pass on the desttypmod and isExplicit flags whether or not the
    * function wants them.
    *
    * Note: coercion functions are assumed to not use collation.
    */
   InitFunctionCallInfoData(locfcinfo, &(astate->elemfunc), 3, InvalidOid, NULL, NULL);
   locfcinfo.arg[0] = PointerGetDatum(array);
   locfcinfo.arg[1] = Int32GetDatum(acoerce->resulttypmod);
   locfcinfo.arg[2] = BoolGetDatum(acoerce->isExplicit);
   locfcinfo.argnull[0] = false;
   locfcinfo.argnull[1] = false;
   locfcinfo.argnull[2] = false;

   return array_map(&locfcinfo, ARR_ELEMTYPE(array), astate->resultelemtype, astate->amstate);
}

/* ----------------------------------------------------------------
*		ExecEvalCurrentOfExpr
*
* The planner must convert CURRENT OF into a TidScan qualification.
* So, we have to be able to do ExecInitExpr on a CurrentOfExpr,
* but we shouldn't ever actually execute it.
* ----------------------------------------------------------------
*/
static Datum ExecEvalCurrentOfExpr(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   ereport(ERROR,
           (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_EXECUTOR), errmsg("CURRENT OF cannot be executed")));
   return 0; /* keep compiler quiet */
}

/* ----------------------------------------------------------------
 *		ExecEvalPrefixText
 * ----------------------------------------------------------------
 */
static Datum ExecEvalPrefixText(PrefixKeyState* state, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
    PrefixKey* pkey = (PrefixKey*)state->xprstate.expr;
    Datum result = ExecEvalExpr(state->arg, econtext, isNull, isDone);

    if (*isNull) {
        return (Datum)0;
    }

    return PointerGetDatum(text_substring_with_encoding(result, 1, pkey->length, false, state->encoding));
}
/* ----------------------------------------------------------------
 *		ExecEvalPrefixBytea
 * ----------------------------------------------------------------
 */
static Datum ExecEvalPrefixBytea(PrefixKeyState* state, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
    PrefixKey* pkey = (PrefixKey*)state->xprstate.expr;
    Datum result = ExecEvalExpr(state->arg, econtext, isNull, isDone);

    if (*isNull) {
        return (Datum)0;
    }

    return PointerGetDatum(bytea_substring(result, 1, pkey->length, false));
}

/*
* ExecInitExpr: prepare an expression tree for execution
*
* This function builds and returns an ExprState tree paralleling the given
* Expr node tree.	The ExprState tree can then be handed to ExecEvalExpr
* for execution.  Because the Expr tree itself is read-only as far as
* ExecInitExpr and ExecEvalExpr are concerned, several different executions
* of the same plan tree can occur concurrently.
*
* This must be called in a memory context that will last as long as repeated
* executions of the expression are needed.  Typically the context will be
* the same as the per-query context of the associated ExprContext.
*
* Any Aggref, WindowFunc, or SubPlan nodes found in the tree are added to the
* lists of such nodes held by the parent PlanState. Otherwise, we do very
* little initialization here other than building the state-node tree.	Any
* nontrivial work associated with initializing runtime info for a node should
* happen during the first actual evaluation of that node.	(This policy lets
* us avoid work if the node is never actually evaluated.)
*
* Note: there is no ExecEndExpr function; we assume that any resource
* cleanup needed will be handled by just releasing the memory context
* in which the state tree is built.  Functions that require additional
* cleanup work can register a shutdown callback in the ExprContext.
*
*	'node' is the root of the expression tree to examine
*	'parent' is the PlanState node that owns the expression.
*
* 'parent' may be NULL if we are preparing an expression that is not
* associated with a plan tree.  (If so, it can't have aggs or subplans.)
* This case should usually come through ExecPrepareExpr, not directly here.
*/
ExprState* ExecInitExpr(Expr* node, PlanState* parent){
    ExprState* state = NULL;
    bool is_flt_frame = (parent != NULL) ?
        parent->state->es_is_flt_frame :
        (u_sess->attr.attr_common.enable_expr_fusion && u_sess->attr.attr_sql.query_dop_tmp ==1);
    if(is_flt_frame) {
        state = ExecInitExprByFlatten(node, parent);
    } else {
        state = ExecInitExprByRecursion(node, parent);
    }
    return state;
}
ExprState* ExecInitExprByRecursion(Expr* node, PlanState* parent)
{
   if (u_sess->hook_cxt.execInitExprHook != NULL) {
        ExprState* expr = ((execInitExprFunc)(u_sess->hook_cxt.execInitExprHook))(node, parent);
        if (expr != NULL)
            return expr;
    }
    ExprState* state = NULL;

   gstrace_entry(GS_TRC_ID_ExecInitExpr);
   if (node == NULL) {
       gstrace_exit(GS_TRC_ID_ExecInitExpr);
       return NULL;
   }

   /* Guard against stack overflow due to overly complex expressions */
   check_stack_depth();

   switch (nodeTag(node)) {
       case T_Var:
           /* varattno == InvalidAttrNumber means it's a whole-row Var */
           if (((Var*)node)->varattno == InvalidAttrNumber) {
               WholeRowVarExprState* wstate = makeNode(WholeRowVarExprState);

                wstate->parent = parent;
                wstate->wrv_junkFilter = NULL;
                state = (ExprState*)wstate;
                state->evalfunc = (ExprStateEvalFunc)ExecEvalWholeRowVar;
            } else {
                state = (ExprState*)makeNode(ExprState);
                state->evalfunc = ExecEvalScalarVar;
            }
            break;
        case T_Const:
        case T_UserVar:
        case T_SetVariableExpr:
            state = (ExprState*)makeNode(ExprState);
           state->is_flt_frame = false;
           state->evalfunc = ExecEvalConst;
           break;
       case T_Param:
           state = (ExprState*)makeNode(ExprState);
           state->is_flt_frame = false;
           switch (((Param*)node)->paramkind) {
               case PARAM_EXEC:
                   state->evalfunc = ExecEvalParamExec;
                   break;
               case PARAM_EXTERN:
                   state->evalfunc = ExecEvalParamExtern;
                   break;
               default:
                   ereport(ERROR,
                           (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmodule(MOD_EXECUTOR),
                            errmsg("unrecognized paramkind: %d", (int)((Param*)node)->paramkind)));
                   break;
           }
           break;
       case T_CoerceToDomainValue:
           state = (ExprState*)makeNode(ExprState);
           state->evalfunc = ExecEvalCoerceToDomainValue;
           break;
       case T_CaseTestExpr:
           state = (ExprState*)makeNode(ExprState);
           state->is_flt_frame = false;
           state->evalfunc = ExecEvalCaseTestExpr;
           break;
       case T_Aggref: {
           Aggref* aggref = (Aggref*)node;
           AggrefExprState* astate = makeNode(AggrefExprState);

           astate->xprstate.is_flt_frame = false;
           astate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalAggref;
           if (parent && (IsA(parent, AggState) || IsA(parent, VecAggState))) {
               AggState* aggstate = (AggState*)parent;
               int naggs;

               aggstate->aggs = lcons(astate, aggstate->aggs);
               naggs = ++aggstate->numaggs;

               astate->aggdirectargs = (List*)ExecInitExprByRecursion((Expr*)aggref->aggdirectargs, parent);

               astate->args = (List*)ExecInitExprByRecursion((Expr*)aggref->args, parent);

               /*
                * Complain if the aggregate's arguments contain any
                * aggregates; nested agg functions are semantically
                * nonsensical.  (This should have been caught earlier,
                * but we defend against it here anyway.)
                */
               if (naggs != aggstate->numaggs)
                   ereport(ERROR,
                           (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("aggregate function calls cannot be nested")));
           } else {
               /* planner messed up */
               ereport(ERROR,
                       (errcode(ERRCODE_INVALID_AGG), errmodule(MOD_OPT), errmsg("Aggref found in non-Agg plan node")));
           }
           state = (ExprState*)astate;
       } break;
       case T_GroupingFunc: {
           GroupingFunc* grp_node = (GroupingFunc*)node;
           GroupingFuncExprState* grp_state = makeNode(GroupingFuncExprState);
           grp_state->xprstate.is_flt_frame = false;
           Agg* agg = NULL;

           if (parent && (IsA(parent, AggState) || IsA(parent, VecAggState))) {
               grp_state->aggstate = (AggState*)parent;

               agg = (Agg*)(parent->plan);

               if (agg->groupingSets)
                   grp_state->clauses = grp_node->cols;
               else
                   grp_state->clauses = NIL;

               state = (ExprState*)grp_state;
               state->evalfunc = (ExprStateEvalFunc)ExecEvalGroupingFuncExpr;
           } else
               ereport(ERROR,
                       (errcode(ERRCODE_PLAN_PARENT_NOT_FOUND),
                        errmodule(MOD_OPT),
                        errmsg("parent of GROUPING is not Agg node")));
       } break;
       case T_GroupingId: {
           GroupingIdExprState* grp_id_state = makeNode(GroupingIdExprState);
           grp_id_state->xprstate.is_flt_frame = false;
           if (parent == NULL || !IsA(parent, AggState) || !IsA(parent->plan, Agg)) {
               ereport(ERROR,
                       (errcode(ERRCODE_PLAN_PARENT_NOT_FOUND),
                        errmodule(MOD_OPT),
                        errmsg("parent of GROUPINGID is not Agg node")));
           }
           grp_id_state->aggstate = (AggState*)parent;
           state = (ExprState*)grp_id_state;
           state->evalfunc = (ExprStateEvalFunc)ExecEvalGroupingIdExpr;
       } break;
       case T_WindowFunc: {
           WindowFunc* wfunc = (WindowFunc*)node;
           WindowFuncExprState* wfstate = makeNode(WindowFuncExprState);
           wfstate->xprstate.is_flt_frame = false;

           wfstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalWindowFunc;
           if (parent && (IsA(parent, WindowAggState) || IsA(parent, VecWindowAggState))) {
               WindowAggState* winstate = (WindowAggState*)parent;
               int nfuncs;

               winstate->funcs = lcons(wfstate, winstate->funcs);
               nfuncs = ++winstate->numfuncs;
               if (wfunc->winagg)
                   winstate->numaggs++;

               wfstate->args = (List*)ExecInitExprByRecursion((Expr*)wfunc->args, parent);

               /*
                * Complain if the windowfunc's arguments contain any
                * windowfuncs; nested window functions are semantically
                * nonsensical.  (This should have been caught earlier,
                * but we defend against it here anyway.)
                */
               if (nfuncs != winstate->numfuncs)
                   ereport(
                       ERROR, (errcode(ERRCODE_WINDOWING_ERROR), errmsg("window function calls cannot be nested")));
           } else {
               /* planner messed up */
               ereport(
                   ERROR, (errcode(ERRCODE_WINDOWING_ERROR), errmsg("WindowFunc found in non-WindowAgg plan node")));
           }
           state = (ExprState*)wfstate;
       } break;
       case T_ArrayRef: {
           ArrayRef* aref = (ArrayRef*)node;
           ArrayRefExprState* astate = makeNode(ArrayRefExprState);
           astate->xprstate.is_flt_frame = false;

           astate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalArrayRef;
           astate->refupperindexpr = (List*)ExecInitExprByRecursion((Expr*)aref->refupperindexpr, parent);
           astate->reflowerindexpr = (List*)ExecInitExprByRecursion((Expr*)aref->reflowerindexpr, parent);
           astate->refexpr = ExecInitExprByRecursion(aref->refexpr, parent);
           astate->refassgnexpr = ExecInitExprByRecursion(aref->refassgnexpr, parent);
           /* do one-time catalog lookups for type info */
           astate->refattrlength = get_typlen(aref->refarraytype);
           get_typlenbyvalalign(
               aref->refelemtype, &astate->refelemlength, &astate->refelembyval, &astate->refelemalign);
           state = (ExprState*)astate;
       } break;
       case T_FuncExpr: {
           FuncExpr* funcexpr = (FuncExpr*)node;
           FuncExprState* fstate = makeNode(FuncExprState);
           fstate->xprstate.is_flt_frame = false;
           fstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalFunc;


           fstate->args = (List*)ExecInitExprByRecursion((Expr*)funcexpr->args, parent);
           fstate->func.fn_oid = InvalidOid; /* not initialized */
           fstate->funcReturnsSet = false;
           state = (ExprState*)fstate;
       } break;
       case T_OpExpr: {
           OpExpr* opexpr = (OpExpr*)node;
           FuncExprState* fstate = makeNode(FuncExprState);
           fstate->xprstate.is_flt_frame = false;

           fstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalOper;
           fstate->args = (List*)ExecInitExprByRecursion((Expr*)opexpr->args, parent);
           fstate->func.fn_oid = InvalidOid; /* not initialized */
           fstate->funcReturnsSet = false;
           state = (ExprState*)fstate;
       } break;
       case T_DistinctExpr: {
           DistinctExpr* distinctexpr = (DistinctExpr*)node;
           FuncExprState* fstate = makeNode(FuncExprState);
           fstate->xprstate.is_flt_frame = false;

           fstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalDistinct;
           fstate->args = (List*)ExecInitExprByRecursion((Expr*)distinctexpr->args, parent);
           fstate->func.fn_oid = InvalidOid; /* not initialized */
           state = (ExprState*)fstate;
       } break;
       case T_NullIfExpr: {
           NullIfExpr* nullifexpr = (NullIfExpr*)node;
           FuncExprState* fstate = makeNode(FuncExprState);
           fstate->xprstate.is_flt_frame = false;

           fstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalNullIf;
           fstate->args = (List*)ExecInitExprByRecursion((Expr*)nullifexpr->args, parent);
           fstate->func.fn_oid = InvalidOid; /* not initialized */
           state = (ExprState*)fstate;
       } break;
       case T_ScalarArrayOpExpr: {
           ScalarArrayOpExpr* opexpr = (ScalarArrayOpExpr*)node;
           ScalarArrayOpExprState* sstate = makeNode(ScalarArrayOpExprState);
           sstate->fxprstate.xprstate.is_flt_frame = false;

           sstate->fxprstate.xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalScalarArrayOp;
           sstate->fxprstate.args = (List*)ExecInitExprByRecursion((Expr*)opexpr->args, parent);
           sstate->fxprstate.func.fn_oid = InvalidOid; /* not initialized */
           sstate->element_type = InvalidOid;          /* ditto */
           state = (ExprState*)sstate;
       } break;
       case T_BoolExpr: {
           BoolExpr* boolexpr = (BoolExpr*)node;
           BoolExprState* bstate = makeNode(BoolExprState);
           bstate->xprstate.is_flt_frame = false;

           switch (boolexpr->boolop) {
               case AND_EXPR:
                   bstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalAnd;
                   break;
               case OR_EXPR:
                   bstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalOr;
                   break;
               case NOT_EXPR:
                   bstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalNot;
                   break;
               default:
                   ereport(ERROR,
                           (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmodule(MOD_OPT),
                            errmsg("unrecognized boolop: %d", (int)boolexpr->boolop)));
                   break;
           }
           bstate->args = (List*)ExecInitExprByRecursion((Expr*)boolexpr->args, parent);
           state = (ExprState*)bstate;
       } break;
       case T_SubPlan: {
           SubPlan* subplan = (SubPlan*)node;
           SubPlanState* sstate = NULL;

           if (parent == NULL)
               ereport(ERROR,
                       (errcode(ERRCODE_PLAN_PARENT_NOT_FOUND),
                        errmodule(MOD_OPT),
                        errmsg("SubPlan found with no parent plan")));

           sstate = ExecInitSubPlan(subplan, parent);

           /* Add SubPlanState nodes to parent->subPlan */
           parent->subPlan = lappend(parent->subPlan, sstate);

           state = (ExprState*)sstate;
       } break;
       case T_AlternativeSubPlan: {
           AlternativeSubPlan* asplan = (AlternativeSubPlan*)node;
           AlternativeSubPlanState* asstate = NULL;

           if (parent == NULL)
               ereport(ERROR,
                       (errcode(ERRCODE_PLAN_PARENT_NOT_FOUND),
                        errmodule(MOD_OPT),
                        errmsg("AlternativeSubPlan found with no parent plan")));

           asstate = ExecInitAlternativeSubPlan(asplan, parent);

           state = (ExprState*)asstate;
       } break;
       case T_FieldSelect: {
           FieldSelect* fselect = (FieldSelect*)node;
           FieldSelectState* fstate = makeNode(FieldSelectState);
           fstate->xprstate.is_flt_frame = false;

           fstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalFieldSelect;
           fstate->arg = ExecInitExprByRecursion(fselect->arg, parent);
           fstate->argdesc = NULL;
           state = (ExprState*)fstate;
       } break;
       case T_FieldStore: {
           FieldStore* fstore = (FieldStore*)node;
           FieldStoreState* fstate = makeNode(FieldStoreState);
           fstate->xprstate.is_flt_frame = false;

           fstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalFieldStore;
           fstate->arg = ExecInitExprByRecursion(fstore->arg, parent);
           fstate->newvals = (List*)ExecInitExprByRecursion((Expr*)fstore->newvals, parent);
           fstate->argdesc = NULL;
           state = (ExprState*)fstate;
       } break;
       case T_RelabelType: {
           RelabelType* relabel = (RelabelType*)node;
           GenericExprState* gstate = makeNode(GenericExprState);
           gstate->xprstate.is_flt_frame = false;

           gstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalRelabelType;
           gstate->arg = ExecInitExprByRecursion(relabel->arg, parent);
           state = (ExprState*)gstate;
       } break;
       case T_CoerceViaIO: {
           CoerceViaIO* iocoerce = (CoerceViaIO*)node;
           CoerceViaIOState* iostate = makeNode(CoerceViaIOState);
           iostate->xprstate.is_flt_frame = false;
           Oid iofunc;
           bool typisvarlena = false;

           iostate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalCoerceViaIO;
           iostate->arg = ExecInitExprByRecursion(iocoerce->arg, parent);
           /* lookup the result type's input function */
           getTypeInputInfo(iocoerce->resulttype, &iofunc, &iostate->intypioparam);
           fmgr_info(iofunc, &iostate->infunc);
           /* lookup the input type's output function */
           getTypeOutputInfo(exprType((Node*)iocoerce->arg), &iofunc, &typisvarlena);
           fmgr_info(iofunc, &iostate->outfunc);
           state = (ExprState*)iostate;
       } break;
       case T_ArrayCoerceExpr: {
           ArrayCoerceExpr* acoerce = (ArrayCoerceExpr*)node;
           ArrayCoerceExprState* astate = makeNode(ArrayCoerceExprState);
           astate->xprstate.is_flt_frame = false;

           astate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalArrayCoerceExpr;
           astate->arg = ExecInitExprByRecursion(acoerce->arg, parent);
           astate->resultelemtype = get_element_type(acoerce->resulttype);
           if (astate->resultelemtype == InvalidOid)
               ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("target type is not an array")));
           /* Arrays over domains aren't supported yet */
           Assert(getBaseType(astate->resultelemtype) == astate->resultelemtype);
           astate->elemfunc.fn_oid = InvalidOid; /* not initialized */
           astate->amstate = (ArrayMapState*)palloc0(sizeof(ArrayMapState));
           state = (ExprState*)astate;
       } break;
       case T_ConvertRowtypeExpr: {
           ConvertRowtypeExpr* convert = (ConvertRowtypeExpr*)node;
           ConvertRowtypeExprState* cstate = makeNode(ConvertRowtypeExprState);
           cstate->xprstate.is_flt_frame = false;

           cstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalConvertRowtype;
           cstate->arg = ExecInitExprByRecursion(convert->arg, parent);
           state = (ExprState*)cstate;
       } break;
       case T_CaseExpr: {
           CaseExpr* caseexpr = (CaseExpr*)node;
           CaseExprState* cstate = makeNode(CaseExprState);
           cstate->xprstate.is_flt_frame = false;
           List* outlist = NIL;
           ListCell* l = NULL;

           cstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalCase;
           cstate->arg = ExecInitExprByRecursion(caseexpr->arg, parent);
           foreach (l, caseexpr->args) {
               CaseWhen* when = (CaseWhen*)lfirst(l);
               CaseWhenState* wstate = makeNode(CaseWhenState);

               Assert(IsA(when, CaseWhen));
               wstate->xprstate.evalfunc = NULL; /* not used */
               wstate->xprstate.expr = (Expr*)when;
               wstate->xprstate.is_flt_frame = false;
               wstate->expr = ExecInitExprByRecursion(when->expr, parent);
               wstate->result = ExecInitExprByRecursion(when->result, parent);
               outlist = lappend(outlist, wstate);
           }
           cstate->args = outlist;
           cstate->defresult = ExecInitExprByRecursion(caseexpr->defresult, parent);
           state = (ExprState*)cstate;
       } break;
       case T_ArrayExpr: {
           ArrayExpr* arrayexpr = (ArrayExpr*)node;
           ArrayExprState* astate = makeNode(ArrayExprState);
           astate->xprstate.is_flt_frame = false;
           List* outlist = NIL;
           ListCell* l = NULL;

           astate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalArray;
           foreach (l, arrayexpr->elements) {
               Expr* e = (Expr*)lfirst(l);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }
           astate->elements = outlist;
           /* do one-time catalog lookup for type info */
           get_typlenbyvalalign(
               arrayexpr->element_typeid, &astate->elemlength, &astate->elembyval, &astate->elemalign);
           state = (ExprState*)astate;
       } break;
       case T_RowExpr: {
           RowExpr* rowexpr = (RowExpr*)node;
           RowExprState* rstate = makeNode(RowExprState);
           rstate->xprstate.is_flt_frame = false;
           FormData_pg_attribute* attrs = NULL;
           List* outlist = NIL;
           ListCell* l = NULL;
           int i;

           rstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalRow;
           /* Build tupdesc to describe result tuples */
           if (rowexpr->row_typeid == RECORDOID) {
               /* generic record, use runtime type assignment */
               rstate->tupdesc = ExecTypeFromExprList(rowexpr->args, rowexpr->colnames);
               BlessTupleDesc(rstate->tupdesc);
               /* we won't need to redo this at runtime */
           } else {
               /* it's been cast to a named type, use that */
               rstate->tupdesc = lookup_rowtype_tupdesc_copy(rowexpr->row_typeid, -1);
           }
           /* Set up evaluation, skipping any deleted columns */
           Assert(list_length(rowexpr->args) <= rstate->tupdesc->natts);
           attrs = rstate->tupdesc->attrs;
           i = 0;
           foreach (l, rowexpr->args) {
               Expr* e = (Expr*)lfirst(l);
               ExprState* estate = NULL;

               if (!attrs[i].attisdropped) {
                   /*
                    * Guard against ALTER COLUMN TYPE on rowtype since
                    * the RowExpr was created.  XXX should we check
                    * typmod too?	Not sure we can be sure it'll be the
                    * same.
                    */
                   if (exprType((Node*)e) != attrs[i].atttypid)
                       ereport(ERROR,
                               (errcode(ERRCODE_DATATYPE_MISMATCH),
                                errmsg("ROW() column has type %s instead of type %s",
                                       format_type_be(exprType((Node*)e)),
                                       format_type_be(attrs[i].atttypid))));
               } else {
                   /*
                    * Ignore original expression and insert a NULL. We
                    * don't really care what type of NULL it is, so
                    * always make an int4 NULL.
                    */
                   e = (Expr*)makeNullConst(INT4OID, -1, InvalidOid);
               }
               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
               i++;
           }
           rstate->args = outlist;
           state = (ExprState*)rstate;
       } break;
       case T_RowCompareExpr: {
           RowCompareExpr* rcexpr = (RowCompareExpr*)node;
           RowCompareExprState* rstate = makeNode(RowCompareExprState);
           rstate->xprstate.is_flt_frame = false;
           int nopers = list_length(rcexpr->opnos);
           List* outlist = NIL;
           ListCell* l = NULL;
           ListCell* l2 = NULL;
           ListCell* l3 = NULL;
           int i;

           rstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalRowCompare;
           Assert(list_length(rcexpr->largs) == nopers);
           outlist = NIL;
           foreach (l, rcexpr->largs) {
               Expr* e = (Expr*)lfirst(l);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }
           rstate->largs = outlist;
           Assert(list_length(rcexpr->rargs) == nopers);
           outlist = NIL;
           foreach (l, rcexpr->rargs) {
               Expr* e = (Expr*)lfirst(l);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }
           rstate->rargs = outlist;
           Assert(list_length(rcexpr->opfamilies) == nopers);
           rstate->funcs = (FmgrInfo*)palloc(nopers * sizeof(FmgrInfo));
           rstate->collations = (Oid*)palloc(nopers * sizeof(Oid));
           i = 0;
           forthree(l, rcexpr->opnos, l2, rcexpr->opfamilies, l3, rcexpr->inputcollids)
           {
               Oid opno = lfirst_oid(l);
               Oid opfamily = lfirst_oid(l2);
               Oid inputcollid = lfirst_oid(l3);
               int strategy;
               Oid lefttype;
               Oid righttype;
               Oid proc;

               get_op_opfamily_properties(opno, opfamily, false, &strategy, &lefttype, &righttype);
               proc = get_opfamily_proc(opfamily, lefttype, righttype, BTORDER_PROC);

               /*
                * If we enforced permissions checks on index support
                * functions, we'd need to make a check here.  But the
                * index support machinery doesn't do that, and neither
                * does this code.
                */
               fmgr_info(proc, &(rstate->funcs[i]));
               rstate->collations[i] = inputcollid;
               i++;
           }
           state = (ExprState*)rstate;
       } break;
       case T_CoalesceExpr: {
           CoalesceExpr* coalesceexpr = (CoalesceExpr*)node;
           CoalesceExprState* cstate = makeNode(CoalesceExprState);
           cstate->xprstate.is_flt_frame = false;
           List* outlist = NIL;
           ListCell* l = NULL;

           cstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalCoalesce;
           foreach (l, coalesceexpr->args) {
               Expr* e = (Expr*)lfirst(l);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }
           cstate->args = outlist;
           state = (ExprState*)cstate;
       } break;
       case T_MinMaxExpr: {
           MinMaxExpr* minmaxexpr = (MinMaxExpr*)node;
           MinMaxExprState* mstate = makeNode(MinMaxExprState);
           mstate->xprstate.is_flt_frame = false;
           List* outlist = NIL;
           ListCell* l = NULL;
           TypeCacheEntry* typentry = NULL;

           mstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalMinMax;
           foreach (l, minmaxexpr->args) {
               Expr* e = (Expr*)lfirst(l);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }
           mstate->args = outlist;
           /* Look up the btree comparison function for the datatype */
           typentry = lookup_type_cache(minmaxexpr->minmaxtype, TYPECACHE_CMP_PROC);
           if (!OidIsValid(typentry->cmp_proc))
               ereport(ERROR,
                       (errcode(ERRCODE_UNDEFINED_FUNCTION),
                        errmsg("could not identify a comparison function for type %s",
                               format_type_be(minmaxexpr->minmaxtype))));

           /*
            * If we enforced permissions checks on index support
            * functions, we'd need to make a check here.  But the index
            * support machinery doesn't do that, and neither does this
            * code.
            */
           fmgr_info(typentry->cmp_proc, &(mstate->cfunc));
           state = (ExprState*)mstate;
       } break;
       case T_XmlExpr: {
           XmlExpr* xexpr = (XmlExpr*)node;
           XmlExprState* xstate = makeNode(XmlExprState);
           xstate->xprstate.is_flt_frame = false;
           List* outlist = NIL;
           ListCell* arg = NULL;

           xstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalXml;
           outlist = NIL;
           foreach (arg, xexpr->named_args) {
               Expr* e = (Expr*)lfirst(arg);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }
           xstate->named_args = outlist;

           outlist = NIL;
           foreach (arg, xexpr->args) {
               Expr* e = (Expr*)lfirst(arg);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }
           xstate->args = outlist;

           state = (ExprState*)xstate;
       } break;
       case T_NullTest: {
           NullTest* ntest = (NullTest*)node;
           NullTestState* nstate = makeNode(NullTestState);
           nstate->xprstate.is_flt_frame = false;

           nstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalNullTest;
           nstate->arg = ExecInitExprByRecursion(ntest->arg, parent);
           nstate->argdesc = NULL;
           state = (ExprState*)nstate;
       } break;
       case T_HashFilter: {
           HashFilter* htest = (HashFilter*)node;
           HashFilterState* hstate = makeNode(HashFilterState);
           hstate->xprstate.is_flt_frame = false;
           List* outlist = NIL;
           ListCell* l = NULL;
           int idx = 0;

           hstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalHashFilter;

           foreach (l, htest->arg) {
               Expr* e = (Expr*)lfirst(l);
               ExprState* estate = NULL;

               estate = ExecInitExprByRecursion(e, parent);
               outlist = lappend(outlist, estate);
           }

           hstate->arg = outlist;
           hstate->bucketMap = get_bucketmap_by_execnode(parent->plan->exec_nodes,
                                                         parent->state->es_plannedstmt,
                                                         &hstate->bucketCnt);
           hstate->nodelist = (uint2*)palloc(list_length(htest->nodeList) * sizeof(uint2));
           foreach (l, htest->nodeList)
               hstate->nodelist[idx++] = lfirst_int(l);

           state = (ExprState*)hstate;
       } break;
       case T_BooleanTest: {
           BooleanTest* btest = (BooleanTest*)node;
           GenericExprState* gstate = makeNode(GenericExprState);
           gstate->xprstate.is_flt_frame = false;

           gstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalBooleanTest;
           gstate->arg = ExecInitExprByRecursion(btest->arg, parent);
           state = (ExprState*)gstate;
       } break;
       case T_CoerceToDomain: {
           CoerceToDomain* ctest = (CoerceToDomain*)node;
           CoerceToDomainState* cstate = makeNode(CoerceToDomainState);
           cstate->xprstate.is_flt_frame = false;

           cstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalCoerceToDomain;
           cstate->arg = ExecInitExprByRecursion(ctest->arg, parent);
           cstate->constraints = GetDomainConstraints(ctest->resulttype);
           state = (ExprState*)cstate;
       } break;
       case T_CurrentOfExpr:
           state = (ExprState*)makeNode(ExprState);
           state->is_flt_frame = false;
           state->evalfunc = ExecEvalCurrentOfExpr;
           break;
       case T_TargetEntry: {
           TargetEntry* tle = (TargetEntry*)node;
           GenericExprState* gstate = makeNode(GenericExprState);
           gstate->xprstate.is_flt_frame = false;

           gstate->xprstate.evalfunc = NULL; /* not used */
           gstate->arg = ExecInitExprByRecursion(tle->expr, parent);
           state = (ExprState*)gstate;
       } break;
       case T_List: {
           List* outlist = NIL;
           ListCell* l = NULL;

           foreach (l, (List*)node) {
               outlist = lappend(outlist, ExecInitExprByRecursion((Expr*)lfirst(l), parent));
           }
           /* Don't fall through to the "common" code below */
           gstrace_exit(GS_TRC_ID_ExecInitExpr);
           return (ExprState*)outlist;
       }
       case T_Rownum: {
           RownumState* rnstate = (RownumState*)makeNode(RownumState);
           rnstate->xprstate.is_flt_frame = false;
           rnstate->ps = parent;
           state = (ExprState*)rnstate;
           state->evalfunc = (ExprStateEvalFunc)ExecEvalRownum;
       } break;
       case T_PrefixKey: {
            PrefixKey* pkey = (PrefixKey*)node;
            PrefixKeyState* pstate = makeNode(PrefixKeyState);
            Oid argtype = exprType((Node*)pkey->arg);

            if (argtype == BYTEAOID || argtype == RAWOID || argtype == BLOBOID) {
                pstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalPrefixBytea;
                pstate->encoding = PG_INVALID_ENCODING;
            } else {
                Oid argcollation = exprCollation((Node*)pkey->arg);
                pstate->xprstate.evalfunc = (ExprStateEvalFunc)ExecEvalPrefixText;
                pstate->encoding = get_valid_charset_by_collation(argcollation);
            }
            pstate->arg = ExecInitExpr(pkey->arg, parent);
            
            state = (ExprState*)pstate;
        } break;
        case T_UserSetElem: {
            UserSetElem* useexpr = (UserSetElem*)node;
            UserSetElemState* usestate = (UserSetElemState*)makeNode(UserSetElemState);
            usestate->use = useexpr;
            state = (ExprState*)usestate;
            state->evalfunc = (ExprStateEvalFunc)ExecEvalUserSetElm;
            usestate->instate = ExecInitExpr((Expr *)useexpr->val, parent);
        } break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized node type: %d when initializing expression.", (int)nodeTag(node))));
           state = NULL; /* keep compiler quiet */
           break;
   }

   /* Common code for all state-node types */
   state->expr = node;

   if (nodeTag(node) != T_TargetEntry)
       state->resultType = exprType((Node*)node);

   gstrace_exit(GS_TRC_ID_ExecInitExpr);
   return state;
}

/*
 * Call ExecInitExpr() on a list of expressions, return a list of ExprStates.
 */
List* ExecInitExprList(List *nodes, PlanState *parent)
{
   List	   *result = NIL;
   ListCell   *lc;

   foreach(lc, nodes)
   {
       Expr	   *e = (Expr*)lfirst(lc);

       result = lappend(result, ExecInitExpr(e, parent));
   }

   return result;
}

/*
 * Call ExecInitExpr() on a list of expressions, return a list of ExprStates.
 */
List* ExecInitExprListByRecursion(List *nodes, PlanState *parent)
{
   List	   *result = NIL;
   ListCell   *lc;

   foreach(lc, nodes)
   {
       Expr	   *e = (Expr*)lfirst(lc);

       result = lappend(result, ExecInitExprByRecursion(e, parent));
   }

   return result;
}


/*
 * Call ExecInitExpr() on a list of expressions, return a list of ExprStates.
 */
List* ExecInitExprListByFlatten(List *nodes, PlanState *parent)
{
   List	   *result = NIL;
   ListCell   *lc;

   foreach(lc, nodes)
   {
       Expr	   *e = (Expr*)lfirst(lc);

       result = lappend(result, ExecInitExprByFlatten(e, parent));
   }

   return result;
}

/*
* ExecPrepareExpr --- initialize for expression execution outside a normal
* Plan tree context.
*
* This differs from ExecInitExpr in that we don't assume the caller is
* already running in the EState's per-query context.  Also, we run the
* passed expression tree through expression_planner() to prepare it for
* execution.  (In ordinary Plan trees the regular planning process will have
* made the appropriate transformations on expressions, but for standalone
* expressions this won't have happened.)
*/
ExprState* ExecPrepareExpr(Expr* node, EState* estate)
{
   ExprState* result = NULL;
   MemoryContext oldcontext;

   oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

   node = expression_planner(node);

   result = ExecInitExpr(node, NULL);

   MemoryContextSwitchTo(oldcontext);

   return result;
}

/* ----------------------------------------------------------------
*					 ExecQual / ExecTargetList / ExecProject
* ----------------------------------------------------------------
*/
/* ----------------------------------------------------------------
*		ExecQual
*
*		Evaluates a conjunctive boolean expression (qual list) and
*		returns true iff none of the subexpressions are false.
*		(We also return true if the list is empty.)
*
*	If some of the subexpressions yield NULL but none yield FALSE,
*	then the result of the conjunction is NULL (ie, unknown)
*	according to three-valued boolean logic.  In this case,
*	we return the value specified by the "resultForNull" parameter.
*
*	Callers evaluating WHERE clauses should pass resultForNull=FALSE,
*	since SQL specifies that tuples with null WHERE results do not
*	get selected.  On the other hand, callers evaluating constraint
*	conditions should pass resultForNull=TRUE, since SQL also specifies
*	that NULL constraint conditions are not failures.
*
*	NOTE: it would not be correct to use this routine to evaluate an
*	AND subclause of a boolean expression; for that purpose, a NULL
*	result must be returned as NULL so that it can be properly treated
*	in the next higher operator (cf. ExecEvalAnd and ExecEvalOr).
*	This routine is only used in contexts where a complete expression
*	is being evaluated and we know that NULL can be treated the same
*	as one boolean result or the other.
*
* ----------------------------------------------------------------
*/
bool ExecQual(List* qual, ExprContext* econtext, bool resultForNull){
    bool is_flt_frame = (qual && IsA(qual, ExprState)) ?
        ((ExprState *)qual)->is_flt_frame : false;
    if(is_flt_frame) {
        return ExecQualByFlatten((ExprState*)qual, econtext);
    } else {
        return ExecQualByRecursion(qual, econtext, resultForNull);
    }
}
bool ExecQualByRecursion(List* qual, ExprContext* econtext, bool resultForNull)
{
   bool result = false;
   MemoryContext oldContext;
   ListCell* l = NULL;

   /*
    * debugging stuff
    */
   EV_printf("ExecQual: qual is ");
   EV_nodeDisplay(qual);
   EV_printf("\n");

   /*
    * Run in short-lived per-tuple context while computing expressions.
    */
   oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

   /*
    * Evaluate the qual conditions one at a time.	If we find a FALSE result,
    * we can stop evaluating and return FALSE --- the AND result must be
    * FALSE.  Also, if we find a NULL result when resultForNull is FALSE, we
    * can stop and return FALSE --- the AND result must be FALSE or NULL in
    * that case, and the caller doesn't care which.
    *
    * If we get to the end of the list, we can return TRUE.  This will happen
    * when the AND result is indeed TRUE, or when the AND result is NULL (one
    * or more NULL subresult, with all the rest TRUE) and the caller has
    * specified resultForNull = TRUE.
    */
   result = true;

   foreach (l, qual) {
       ExprState* clause = (ExprState*)lfirst(l);
       Datum expr_value;
       bool isNull = false;

       expr_value = ExecEvalExpr(clause, econtext, &isNull, NULL);

       if (isNull) {
           if (resultForNull == false) {
               result = false; /* treat NULL as FALSE */
               break;
           }
       } else {
           if (!DatumGetBool(expr_value)) {
               result = false; /* definitely FALSE */
               break;
           }
       }
   }

   MemoryContextSwitchTo(oldContext);

   return result;
}

template<bool resultForNull>
static Datum ExecEvalQual(ExprState* exprstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone){
    List* qual = (List*)exprstate->expr;
    bool result = true;
    ListCell* l = NULL;

    foreach (l, qual){
        ExprState* clause = (ExprState*)lfirst(l);
        Datum expr_value;

        expr_value = ExecEvalExpr(clause, econtext, isNull, isDone);

        if(*isNull){
            if(resultForNull == false) {
                result = false;
            }
        } else {
            if (!DatumGetBool(expr_value)) {
                result =false;
                break;
            }
        }
    }
    return Datum(result);
}

ExprState *ExecInitQualByRecursion(Expr *node, PlanState * parent, bool resultForNull){
    ExprState* qualState = makeNode(ExprState);
    qualState->is_flt_frame = false;
    if(resultForNull)
        qualState->evalfunc = ExecEvalQual<true>;
    else
        qualState->evalfunc = ExecEvalQual<false>;
    qualState->expr =(Expr*)ExecInitExprByRecursion(node, parent);
    return qualState;
}

/*
* Number of items in a tlist (including any resjunk items!)
*/
int ExecTargetListLength(List* targetlist)
{
   /* This used to be more complex, but fjoins are dead */
   return list_length(targetlist);
}

/*
* Number of items in a tlist, not including any resjunk items
*/
int ExecCleanTargetListLength(List* targetlist)
{
   int len = 0;
   ListCell* tl = NULL;

   foreach (tl, targetlist) {
       TargetEntry* curTle = (TargetEntry*)lfirst(tl);

       Assert(IsA(curTle, TargetEntry));
       if (!curTle->resjunk)
           len++;
   }
   return len;
}

static HeapTuple get_tuple(Relation relation, ItemPointer tid)
{
   Buffer user_buf = InvalidBuffer;
   HeapTuple tuple = NULL;
   HeapTuple new_tuple = NULL;

   /* alloc mem for old tuple and set tuple id */
   tuple = (HeapTupleData *)heaptup_alloc(BLCKSZ);
   tuple->t_data = (HeapTupleHeader)((char *)tuple + HEAPTUPLESIZE);
   Assert(tid != NULL);
   tuple->t_self = *tid;

   if (heap_fetch(relation, SnapshotAny, tuple, &user_buf, false, NULL)) {
       new_tuple = heapCopyTuple((HeapTuple)tuple, relation->rd_att, NULL);
       ReleaseBuffer(user_buf);
   } else {
       ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("The tuple is not found"),
                       errdetail("Another user is getting tuple or the datum is NULL")));
   }

   heap_freetuple(tuple);
   return new_tuple;
}

static void check_huge_clob_paramter(FunctionCallInfoData* fcinfo, bool is_have_huge_clob)
{
   if (!is_have_huge_clob || IsSystemObjOid(fcinfo->flinfo->fn_oid)) {
       return;
   }
   Oid schema_oid = get_func_namespace(fcinfo->flinfo->fn_oid);
   if (IsPackageSchemaOid(schema_oid)) {
       return;
   }
   ereport(ERROR,
           (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("huge clob do not support as function in parameter")));
}


bool is_external_clob(Oid type_oid, bool is_null, Datum value)
{
   if (type_oid == CLOBOID && !is_null && VARATT_IS_EXTERNAL_LOB(value)) {
       return true;
   }
   return false;
}

bool is_huge_clob(Oid type_oid, bool is_null, Datum value)
{
   if (!is_external_clob(type_oid, is_null, value)) {
       return false;
   }

   struct varatt_lob_pointer* lob_pointer = (varatt_lob_pointer*)(VARDATA_EXTERNAL(value));
   bool is_huge_clob = false;
   /* get relation by relid */
   ItemPointerData tuple_ctid;
   tuple_ctid.ip_blkid.bi_hi = lob_pointer->bi_hi;
   tuple_ctid.ip_blkid.bi_lo = lob_pointer->bi_lo;
   tuple_ctid.ip_posid = lob_pointer->ip_posid;
   Relation relation = heap_open(lob_pointer->relid, RowExclusiveLock);
   HeapTuple origin_tuple = get_tuple(relation, &tuple_ctid);
   if (!HeapTupleIsValid(origin_tuple)) {
       ereport(ERROR,
               (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for tuple from relation %u", lob_pointer->relid)));
   }
   bool attr_is_null = false;
   Datum attr = fastgetattr(origin_tuple, lob_pointer->columid, relation->rd_att, &attr_is_null);
   if (!attr_is_null && VARATT_IS_HUGE_TOAST_POINTER(attr)) {
       is_huge_clob = true;
   }
   heap_close(relation, NoLock);
   heap_freetuple(origin_tuple);
   return is_huge_clob;
}

Datum fetch_lob_value_from_tuple(varatt_lob_pointer* lob_pointer, Oid update_oid, bool* is_null)
{
   /* get relation by relid */
   ItemPointerData tuple_ctid;
   tuple_ctid.ip_blkid.bi_hi = lob_pointer->bi_hi;
   tuple_ctid.ip_blkid.bi_lo = lob_pointer->bi_lo;
   tuple_ctid.ip_posid = lob_pointer->ip_posid;
   Relation relation = heap_open(lob_pointer->relid, RowExclusiveLock);
   HeapTuple origin_tuple = get_tuple(relation, &tuple_ctid);
   if (!HeapTupleIsValid(origin_tuple)) {
       ereport(ERROR,
               (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for tuple from relation %u", lob_pointer->relid)));
   }

   Datum attr = fastgetattr(origin_tuple, lob_pointer->columid, relation->rd_att, is_null);


   if (!OidIsValid(update_oid)) {
       heap_close(relation, NoLock);
       return attr;
   }
   Datum new_attr = (Datum)0;
   if (*is_null) {
       new_attr = (Datum)0;
   } else {
       if (VARATT_IS_HUGE_TOAST_POINTER(attr)) {
           if (unlikely(origin_tuple->tupTableType == UHEAP_TUPLE)) {
               ereport(ERROR,
                       (errcode(ERRCODE_INVALID_NAME),
                        errmsg("UStore cannot update clob column that larger than 1GB")));
           }
           Relation update_rel = heap_open(update_oid, RowExclusiveLock);
           struct varlena *old_value = (struct varlena *)DatumGetPointer(attr);
           struct varlena *new_value = heap_tuple_fetch_and_copy(update_rel, old_value, false);
           new_attr = PointerGetDatum(new_value);
           heap_close(update_rel, NoLock);
       } else if (VARATT_IS_SHORT(attr) || VARATT_IS_EXTERNAL(attr) || VARATT_IS_4B(attr)) {
           new_attr = PointerGetDatum(attr);
       } else {
           ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR),
                           errmsg("lob value which fetch from tuple type is not recognized."),
                           errdetail("lob type is not one of the existing types")));
       }
   }
   heap_close(relation, NoLock);
   return new_attr;
}

/*
 * Return true if objid is a partition oid, and set relationid to objid's parent relation oid.
 * Return false if objid is a relation oid.
 */
static bool getRelIdForPartition(Oid objid, Oid *relationid)
{
    HeapTuple reltuple = SearchSysCache1(RELOID, ObjectIdGetDatum(objid));
    if (HeapTupleIsValid(reltuple)) {
        ReleaseSysCache(reltuple);
        /* is relation oid */
        return false;
    } else {
        HeapTuple partuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(objid));
        if (HeapTupleIsValid(partuple)) {
            /* is partition oid */
            Form_pg_partition partition = (Form_pg_partition)GETSTRUCT(partuple);
            Oid relid = InvalidOid;
            if (partition->parttype == PART_OBJ_TYPE_TABLE_PARTITION) {
                relid = partition->parentid;
                ReleaseSysCache(partuple);
            } else if (partition->parttype == PART_OBJ_TYPE_TABLE_SUB_PARTITION) {
                Oid partid = partition->parentid;
                ReleaseSysCache(partuple);
                HeapTuple partuple_subparent = SearchSysCache1(PARTRELID, ObjectIdGetDatum(partid));
                if (HeapTupleIsValid(partuple_subparent)) {
                    relid = ((Form_pg_partition)GETSTRUCT(partuple_subparent))->parentid;
                    ReleaseSysCache(partuple_subparent);
                } else {
                    /* wrong sub partition oid */
                    ereport(ERROR, (errcode(ERRCODE_RELATION_OPEN_ERROR),
                                    errmsg("could not fine relation for subpartition OID %u", objid)));
                }
            }
            HeapTuple parenttuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
            if (HeapTupleIsValid(parenttuple)) {
                ReleaseSysCache(parenttuple);
                *relationid = relid;
                return true;
            } else {
                /* wrong partition oid */
                ereport(ERROR, (errcode(ERRCODE_RELATION_OPEN_ERROR),
                                errmsg("could not fine relation for partition OID %u", objid)));
            }
        }
    }
    /* is relation oid */
    return false;
}

/*
* ExecTargetList
*		Evaluates a targetlist with respect to the given
*		expression context.  Returns TRUE if we were able to create
*		a result, FALSE if we have exhausted a set-valued expression.
*
* Results are stored into the passed values and isnull arrays.
* The caller must provide an itemIsDone array that persists across calls.
*
* As with ExecEvalExpr, the caller should pass isDone = NULL if not
* prepared to deal with sets of result tuples.  Otherwise, a return
* of *isDone = ExprMultipleResult signifies a set element, and a return
* of *isDone = ExprEndResult signifies end of the set of tuple.
* We assume that *isDone has been initialized to ExprSingleResult by caller.
*/
static bool ExecTargetList(List* targetlist, ExprContext* econtext, Datum* values, bool* isnull,
                          ExprDoneCond* itemIsDone, ExprDoneCond* isDone)
{
    MemoryContext oldContext;
    bool haveDoneSets = false;

   /*
    * Run in short-lived per-tuple context while computing expressions.
    */
   oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

   RightRefState* refState = econtext->rightRefState;
    int targetCount = list_length(targetlist);
    GenericExprState* targetArr[targetCount];

    int colCnt = (IS_ENABLE_RIGHT_REF(refState) && refState->colCnt > 0) ? refState->colCnt : 1;
    bool hasExecs[colCnt];

    SortTargetListAsArray(refState, targetlist, targetArr);

    InitOutputValues(refState, values, isnull, hasExecs);

    /*
     * evaluate all the expressions in the target list
     */
    haveDoneSets = false; /* any exhausted set exprs in tlist? */

   for (GenericExprState* gstate : targetArr) {
       TargetEntry* tle = (TargetEntry*)gstate->xprstate.expr;
       AttrNumber resind = tle->resno - 1;

       ELOG_FIELD_NAME_START(tle->resname);

       values[resind] = ExecEvalExpr(gstate->arg, econtext, &isnull[resind], &itemIsDone[resind]);
        if (IS_ENABLE_RIGHT_REF(refState) && resind < refState->colCnt) {
            hasExecs[resind] = true;
        }

       if (T_Var == nodeTag(tle->expr) && !isnull[resind]) {
           Var *var = (Var *)tle->expr;
           if (var->vartype == TIDOID) {
               Assert(ItemPointerIsValid((ItemPointer)values[resind]));
           }
       }


        bool isClobAndNotNull = false;
        isClobAndNotNull = (IsA(tle->expr, Param)) && (!isnull[resind]) && (((Param*)tle->expr)->paramtype == CLOBOID
            || ((Param*)tle->expr)->paramtype == BLOBOID);
        if (isClobAndNotNull && econtext->ecxt_scantuple != NULL) {
           /* if is big lob, fetch and copy from toast */
           if (VARATT_IS_HUGE_TOAST_POINTER(values[resind])) {
               Datum new_attr = (Datum)0;
               Oid update_oid = ((HeapTuple)(econtext->ecxt_scantuple->tts_tuple))->t_tableOid;
                Oid parent_oid = InvalidOid;
               Relation parent_rel = NULL;
                Relation part_rel = NULL;
                Partition part = NULL;
                bool ispartition = getRelIdForPartition(update_oid, &parent_oid);
                if (ispartition) {
                    parent_rel = heap_open(parent_oid, RowExclusiveLock);
                    part = partitionOpen(parent_rel, update_oid, RowExclusiveLock);
                    part_rel = partitionGetRelation(parent_rel, part);
                } else {
                    parent_rel = heap_open(update_oid, RowExclusiveLock);
               }
                struct varlena *old_value = (struct varlena *)DatumGetPointer(values[resind]);
                struct varlena *new_value = heap_tuple_fetch_and_copy(part_rel == NULL ? parent_rel : part_rel,
                                                                      old_value, false);
                if (new_value == NULL) {
                    isnull[resind] = true;
               }
                new_attr = PointerGetDatum(new_value);
                if (ispartition) {
                    releaseDummyRelation(&part_rel);
                    partitionClose(parent_rel, part, NoLock);
                    heap_close(parent_rel, NoLock);
                } else {
                    heap_close(parent_rel, NoLock);
                }
               values[resind] = new_attr;
           }
       }
       ELOG_FIELD_NAME_END;

       if (itemIsDone[resind] != ExprSingleResult) {
           /* We have a set-valued expression in the tlist */
           if (isDone == NULL)
               ereport(ERROR,
                       (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("set-valued function called in context when calculate targetlist that cannot accept a "
                               "set")));
           if (itemIsDone[resind] == ExprMultipleResult) {
               /* we have undone sets in the tlist, set flag */
               *isDone = ExprMultipleResult;
           } else {
               /* we have done sets in the tlist, set flag for that */
               haveDoneSets = true;
           }
       }
   }

   if (haveDoneSets) {
       /*
        * note: can't get here unless we verified isDone != NULL
        */
       if (*isDone == ExprSingleResult) {
           /*
            * all sets are done, so report that tlist expansion is complete.
            */
           *isDone = ExprEndResult;
           MemoryContextSwitchTo(oldContext);
           return false;
       } else {
           /*
            * We have some done and some undone sets.	Restart the done ones
            * so that we can deliver a tuple (if possible).
            */
           for (GenericExprState* gstate : targetArr) {
               TargetEntry* tle = (TargetEntry*)gstate->xprstate.expr;
               AttrNumber resind = tle->resno - 1;

               if (itemIsDone[resind] == ExprEndResult) {
                   values[resind] = ExecEvalExpr(gstate->arg, econtext, &isnull[resind], &itemIsDone[resind]);

                   if (itemIsDone[resind] == ExprEndResult) {
                       /*
                        * Oh dear, this item is returning an empty set. Guess
                        * we can't make a tuple after all.
                        */
                       *isDone = ExprEndResult;
                       break;
                   }
               }
           }

           /*
            * If we cannot make a tuple because some sets are empty, we still
            * have to cycle the nonempty sets to completion, else resources
            * will not be released from subplans etc.
            *
            * XXX is that still necessary?
            */
           if (*isDone == ExprEndResult) {
               for (GenericExprState* gstate : targetArr) {
                   TargetEntry* tle = (TargetEntry*)gstate->xprstate.expr;
                   AttrNumber resind = tle->resno - 1;

                   while (itemIsDone[resind] == ExprMultipleResult) {
                       values[resind] = ExecEvalExpr(gstate->arg, econtext, &isnull[resind], &itemIsDone[resind]);
                   }
               }

               MemoryContextSwitchTo(oldContext);
               return false;
           }
       }
   }


    if (IS_ENABLE_RIGHT_REF(econtext->rightRefState)) {
        econtext->rightRefState->values = nullptr;
        econtext->rightRefState->isNulls = nullptr;
        econtext->rightRefState->hasExecs = nullptr;
    }

    /* Report success */
    MemoryContextSwitchTo(oldContext);

   return true;
}

/*
* ExecProject
*
*		projects a tuple based on projection info and stores
*		it in the previously specified tuple table slot.
*
*		Note: the result is always a virtual tuple; therefore it
*		may reference the contents of the exprContext's scan tuples
*		and/or temporary results constructed in the exprContext.
*		If the caller wishes the result to be valid longer than that
*		data will be valid, he must call ExecMaterializeSlot on the
*		result slot.
*/
TupleTableSlot* ExecProject(ProjectionInfo* projInfo, ExprDoneCond* isDone){
    if (projInfo->pi_state.is_flt_frame){
        return ExecProjectByFlatten(projInfo, isDone);
    } else {
        return ExecProjectByRecursion(projInfo, isDone);
    }
}
TupleTableSlot* ExecProjectByRecursion(ProjectionInfo* projInfo, ExprDoneCond* isDone)
{
   /*
    * sanity checks
    */
   Assert(projInfo != NULL);

   /*
    * get the projection info we want
    */
   TupleTableSlot *slot = projInfo->pi_slot;
   ExprContext *econtext = projInfo->pi_exprContext;

   /* Assume single result row until proven otherwise */
   if (isDone != NULL)
       *isDone = ExprSingleResult;

   /*
    * Clear any former contents of the result slot.  This makes it safe for
    * us to use the slot's Datum/isnull arrays as workspace. (Also, we can
    * return the slot as-is if we decide no rows can be projected.)
    */
   (void)ExecClearTuple(slot);

   /*
    * Force extraction of all input values that we'll need.  The
    * Var-extraction loops below depend on this, and we are also prefetching
    * all attributes that will be referenced in the generic expressions.
    */
   if (projInfo->pi_lastInnerVar > 0) {
       tableam_tslot_getsomeattrs(econtext->ecxt_innertuple, projInfo->pi_lastInnerVar);
   }

   if (projInfo->pi_lastOuterVar > 0) {
       tableam_tslot_getsomeattrs(econtext->ecxt_outertuple, projInfo->pi_lastOuterVar);
   }

   if (projInfo->pi_lastScanVar > 0 && econtext->ecxt_scantuple) {
       tableam_tslot_getsomeattrs(econtext->ecxt_scantuple, projInfo->pi_lastScanVar);
   }

   /*
    * Assign simple Vars to result by direct extraction of fields from source
    * slots ... a mite ugly, but fast ...
    */
   int numSimpleVars = projInfo->pi_numSimpleVars;
   if (numSimpleVars > 0 && !IS_ENABLE_RIGHT_REF(projInfo->pi_exprContext->rightRefState)) {
       Datum* values = slot->tts_values;
       bool* isnull = slot->tts_isnull;
       int* varSlotOffsets = projInfo->pi_varSlotOffsets;
       int* varNumbers = projInfo->pi_varNumbers;
       int i;

       if (projInfo->pi_directMap) {
           /* especially simple case where vars go to output in order */
           for (i = 0; i < numSimpleVars; i++) {
               char* slotptr = ((char*)econtext) + varSlotOffsets[i];
               TupleTableSlot* varSlot = *((TupleTableSlot**)slotptr);
               int varNumber = varNumbers[i] - 1;

               Assert (varNumber < varSlot->tts_tupleDescriptor->natts);
               Assert (i < slot->tts_tupleDescriptor->natts);
               values[i] = varSlot->tts_values[varNumber];
               isnull[i] = varSlot->tts_isnull[varNumber];
           }
       } else {
           /* we have to pay attention to varOutputCols[] */
           int* varOutputCols = projInfo->pi_varOutputCols;

           for (i = 0; i < numSimpleVars; i++) {
               char* slotptr = ((char*)econtext) + varSlotOffsets[i];
               TupleTableSlot* varSlot = *((TupleTableSlot**)slotptr);
               int varNumber = varNumbers[i] - 1;
               int varOutputCol = varOutputCols[i] - 1;

               Assert (varNumber < varSlot->tts_tupleDescriptor->natts);
               Assert (varOutputCol < slot->tts_tupleDescriptor->natts);
               values[varOutputCol] = varSlot->tts_values[varNumber];
               isnull[varOutputCol] = varSlot->tts_isnull[varNumber];
           }
       }
   }

   /*
    * If there are any generic expressions, evaluate them.  It's possible
    * that there are set-returning functions in such expressions; if so and
    * we have reached the end of the set, we return the result slot, which we
    * already marked empty.
    */
   if (projInfo->pi_targetlist) {
       if (IS_ENABLE_RIGHT_REF(econtext->rightRefState)) {
            econtext->rightRefState->isUpsert = projInfo->isUpsertHasRightRef;
        }
        bool flag = !ExecTargetList(projInfo->pi_targetlist, econtext, slot->tts_values,
                                    slot->tts_isnull, projInfo->pi_itemIsDone, isDone);

        if (econtext->rightRefState) {
            econtext->rightRefState->isUpsert = false;
        }

        if (flag) {
            return slot; /* no more result rows, return empty slot */
        }
    }

   /*
    * Successfully formed a result row.  Mark the result slot as containing a
    * valid virtual tuple.
    */
   return ExecStoreVirtualTuple(slot);
}

static Datum ExecEvalGroupingIdExpr(
   GroupingIdExprState* gstate, ExprContext* econtext, bool* isNull, ExprDoneCond* isDone)
{
   int groupingId = 0;

   if (isDone != NULL) {
       *isDone = ExprSingleResult;
   }

   *isNull = false;

   for (int i = 0; i < gstate->aggstate->current_phase; i++) {
       groupingId += gstate->aggstate->phases[i].numsets;
   }
   groupingId += gstate->aggstate->projected_set + 1;

   return (Datum)groupingId;
}

/*
* @Description: copy cursor data from estate->datums to target_cursor
* @in datums - estate->datums
* @in dno - varno in datums
* @in target_cursor - target cursor data
* @return -void
*/
void ExecCopyDataFromDatum(PLpgSQL_datum** datums, int dno, Cursor_Data* target_cursor)
{
   PLpgSQL_var *cursor_var = (PLpgSQL_var *)(datums[dno]);

   /* only copy cursor option to refcursor */
   if (cursor_var->datatype->typoid != REFCURSOROID) {
       return;
   }

   cursor_var = (PLpgSQL_var*)(datums[dno + CURSOR_ISOPEN]);
   target_cursor->is_open = DatumGetBool(cursor_var->value);
   cursor_var = (PLpgSQL_var*)(datums[dno + CURSOR_FOUND]);
   target_cursor->found = DatumGetBool(cursor_var->value);
   target_cursor->null_fetch = cursor_var->isnull;
   cursor_var = (PLpgSQL_var*)(datums[dno + CURSOR_NOTFOUND]);
   target_cursor->not_found = DatumGetBool(cursor_var->value);
   cursor_var = (PLpgSQL_var*)(datums[dno + CURSOR_ROWCOUNT]);
   target_cursor->row_count = DatumGetInt32(cursor_var->value);
   target_cursor->null_open = cursor_var->isnull;
   target_cursor->cur_dno = dno;
}

/*
* @Description: copy cursor data to estate->datums
* @in datums - estate->datums
* @in dno - varno in datums
* @in target_cursor - source cursor data
* @return -void
*/
void ExecCopyDataToDatum(PLpgSQL_datum** datums, int dno, Cursor_Data* source_cursor)
{
   PLpgSQL_var *cursor_var = (PLpgSQL_var *)(datums[dno]);

   /* only copy cursor option to refcursor */
   if (cursor_var->datatype->typoid != REFCURSOROID) {
       return;
   }

   cursor_var = (PLpgSQL_var*)(datums[dno + CURSOR_ISOPEN]);
   cursor_var->value = BoolGetDatum(source_cursor->is_open);
   cursor_var = (PLpgSQL_var*)(datums[dno + CURSOR_FOUND]);
   cursor_var->value = BoolGetDatum(source_cursor->found);
   cursor_var->isnull = source_cursor->null_fetch;
   cursor_var = (PLpgSQL_var*)(datums[dno + CURSOR_NOTFOUND]);
   cursor_var->value = BoolGetDatum(source_cursor->not_found);
   cursor_var->isnull = source_cursor->null_fetch;
   cursor_var = (PLpgSQL_var*)(datums[dno + CURSOR_ROWCOUNT]);
   cursor_var->value = Int32GetDatum(source_cursor->row_count);
   cursor_var->isnull = source_cursor->null_open;
}

#ifdef USE_SPQ
bool IsJoinExprNull(List *joinExpr, ExprContext *econtext)
{
    ListCell *lc;
    bool joinkeys_null = true;
 
    Assert(joinExpr != nullptr);
 
    foreach(lc, joinExpr) {
        ExprState *keyexpr = (ExprState *) lfirst(lc);
        bool isNull = false;
 
        /*
         * Evaluate the current join attribute value of the tuple
         */
        ExecEvalExpr(keyexpr, econtext, &isNull, NULL);
 
        if (!isNull) {
            /* Found at least one non-null join expression, we're done */
            joinkeys_null = false;
            break;
        }
    }
 
    return joinkeys_null;
}
#endif
