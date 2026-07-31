/*-------------------------------------------------------------------------
 *
 * jsonpath_exec.cpp
 *     Routines for SQL/JSON path execution.
 *
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 * Copyright (c) 2019-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/common/backend/utils/adt/jsonpath_exec.cpp
 *
 *-------------------------------------------------------------------------
 */

 /* Jsonpath is executed in the global context stored in JsonPathExecContext,
 * which is passed to almost every function involved into execution.  Entry
 * point for jsonpath execution is executeJsonPath() function, which
 * initializes execution context including initial JsonPathItem and JsonbValue,
 * flags, stack for calculation of @ in filters.
 *
 * The result of jsonpath query execution is enum JsonPathExecResult and
 * if succeeded sequence of JsonbValue, written to JsonValueList *found, which
 * is passed through the jsonpath items.  When found == NULL, we're inside
 * exists-query and we're interested only in whether result is empty.  In this
 * case execution is stopped once first result item is found, and the only
 * execution result is JsonPathExecResult.  The values of JsonPathExecResult
 * are following:
 * - JPER_OK            -- result sequence is not empty
 * - JPER_NOT_FOUND    -- result sequence is empty
 * - JPER_ERROR        -- error occurred during execution
 *
 * Jsonpath is executed recursively (see executeItem()) starting form the
 * first path item (which in turn might be, for instance, an arithmetic
 * expression evaluated separately).  On each step single JsonbValue obtained
 * from previous path item is processed.  The result of processing is a
 * sequence of JsonbValue (probably empty), which is passed to the next path
 * item one by one.  When there is no next path item, then JsonbValue is added
 * to the 'found' list.  When found == NULL, then execution functions just
 * return JPER_OK (see executeNextItem()).
 *
 * Many of jsonpath operations require automatic unwrapping of arrays in lax
 * mode.  So, if input value is array, then corresponding operation is
 * processed not on array itself, but on all of its members one by one.
 * executeItemOptUnwrapTarget() function have 'unwrap' argument, which indicates
 * whether unwrapping of array is needed.  When unwrap == true, each of array
 * members is passed to executeItemOptUnwrapTarget() again but with unwrap == false
 * in order to avoid subsequent array unwrapping.
 *
 * All boolean expressions (predicates) are evaluated by executeBoolItem()
 * function, which returns tri-state JsonPathBool.  When error is occurred
 * during predicate execution, it returns JPB_UNKNOWN.  According to standard
 * predicates can be only inside filters.  But we support their usage as
 * jsonpath expression.  This helps us to implement @@ operator.  In this case
 * resulting JsonPathBool is transformed into jsonb bool or null.
 *
 * Arithmetic and boolean expression are evaluated recursively from expression
 * tree top down to the leaves.  Therefore, for binary arithmetic expressions
 * we calculate operands first.  Then we check that results are numeric
 * singleton lists, calculate the result and pass it to the next path item.
 */

#include "postgres.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/formatting.h"
#include "utils/int8.h"
#include "utils/numeric.h"
#include "utils/json.h"
#include "utils/jsonpath.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/jsonpath_exec.h"

static inline bool JspStrictAbsenceOfErrors(JsonPathExecContext* cxt)
{
    return (!(cxt)->laxMode);
}

static inline bool JperIsError(JsonPathExecResult jper)
{
    return ((jper) == JPER_ERROR);
}

static inline bool JspAutoUnwrap(JsonPathExecContext* cxt)
{
    return (!DB_IS_CMPT(A_FORMAT) && ((cxt)->laxMode)) ||
            (DB_IS_CMPT(A_FORMAT) && (cxt->timesLaxed < 1));
}

static inline bool JspAutoWrap(JsonPathExecContext* cxt)
{
    return ((cxt)->laxMode);
}

static inline bool JspIgnoreStructuralErrors(JsonPathExecContext* cxt)
{
    return ((cxt)->ignoreStructuralErrors);
}

static inline bool JspThrowErrors(JsonPathExecContext* cxt)
{
    return ((cxt)->throwErrors);
}

static inline void JspResetAFormatLaxMode(JsonPathExecContext* cxt)
{
    cxt->timesLaxed = 0;
}

static inline void JspSetAFormatLaxMode(JsonPathExecContext* cxt)
{
    if (DB_IS_CMPT(A_FORMAT)) {
        cxt->timesLaxed++;
    }
}

static inline bool JsonbValueIsScalar(JsonbValue* jb)
{
    return ((jb->type != jbvBinary) && (jb->type != jbvArray)
        && (jb->type != jbvObject));
}

/* Convenience macro: return or throw error depending on context */
#define RETURN_ERROR(throw_error) \
do { \
    if (JspThrowErrors(cxt)) { \
        throw_error; \
    } else { \
        return JPER_ERROR; \
    } \
} while (0)

/* for json_textcontains */
static List* SplitRawTarget(char* raw);
static List* CollectScalarJsonbValue(JsonPathExecContext* cxt, JsonValueList* jvl);
static bool MatchTargetInJBValueList(List* jbvs, List* targets);

/****************** User interface to JsonPath executor ********************/

/*
 * SQL function json_exists for A compatibility
 * returns true if there's data in json under specified pathStr
 */
Datum json_path_exists(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(1)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("the json path expression is not of text type")));
    }

    char* pathStr = text_to_cstring(PG_GETARG_TEXT_P(1));
    JsonPath* jp = CstringToJsonpath(pathStr);

    int argnum = 2;
    if (PG_ARGISNULL(0) || PG_ARGISNULL(argnum)) {
        PG_RETURN_NULL();
    }

    text* json = PG_GETARG_TEXT_P(0);
    OnErrorType onError = (OnErrorType)PG_GETARG_INT32(2);
    JsonPathExecResult res = JPER_OK;

    if (!IsJsonText(json)) {
        switch (onError) {
            case FALSE_ON_ERROR:
                PG_RETURN_BOOL(false);
            case TRUE_ON_ERROR:
                PG_RETURN_BOOL(true);
            case ERROR_ON_ERROR:
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("the input is not a well-formed json data")));
                break;
            default:
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("unrecognized ON ERROR option: %d", onError)));
                break;
        }
    } else {
        Jsonb* jb = DatumGetJsonb(
            DirectInputFunctionCall(JSONBOID, text_to_cstring(json), -1));
        JsonPathExecContext cxt;

        initJsonPathExecContext(jp, &cxt);

        res = executeJsonPath(jp, &cxt, jb, NULL);
        if (JperIsError(res)) {
            PG_RETURN_NULL();
        }
        PG_RETURN_BOOL(res == JPER_OK);
    }
}

/*
 * SQL function json_textcontains for A compatibility
 * returns true if the json contains target under specified pathStr
 */
static bool json_textcontains_internal(text* json, char* pathStr, char* raw)
{
    if (!IsJsonText(json)) {
        return false;
    }

    JsonPath* jp = CstringToJsonpath(pathStr);
    /* compat with A's behavior, keep the original order, don't sort */
    u_sess->parser_cxt.disable_jsonb_auto_sort = DB_IS_CMPT(A_FORMAT);
    Jsonb* jb = DatumGetJsonb(
        DirectInputFunctionCall(JSONBOID, text_to_cstring(json), -1));
    JsonPathExecContext cxt;
    JsonValueList jvlFound = {0};
    bool bres = false;

    initJsonPathExecContext(jp, &cxt);
    cxt.unlimitedKeyLax = true;

    JsonPathExecResult res = executeJsonPath(jp, &cxt, jb, &jvlFound);
    u_sess->parser_cxt.disable_jsonb_auto_sort = false;
    if (!JsonValueListIsEmpty(&jvlFound)) {
        char* copyRaw = pstrdup(raw);
        List* targetStrList = SplitRawTarget(copyRaw);
        if (targetStrList != NIL)  {
            List* jbvStrList = CollectScalarJsonbValue(&cxt, &jvlFound);
            bres = MatchTargetInJBValueList(jbvStrList, targetStrList);

            ListCell* lc;
            foreach (lc, targetStrList) {
                list_free_deep((List*)lfirst(lc));
            }
            list_free(targetStrList);
            list_free_deep(jbvStrList);
        }
        if (copyRaw != NULL) {
            pfree(copyRaw);
        }
    }

    return bres;
}

Datum json_textcontains(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(1)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("the json path expression is not of text type")));
    }

    int argnum = 2;
    if (PG_ARGISNULL(0) || PG_ARGISNULL(argnum)) {
        PG_RETURN_NULL();
    }

    text* json = PG_GETARG_TEXT_P(0);
    char* pathStr = text_to_cstring(PG_GETARG_TEXT_P(1));
    char* raw = PG_GETARG_CSTRING(2);
    bool bres = json_textcontains_internal(json, pathStr, raw);

    PG_RETURN_BOOL(bres);
}

Datum json_textcontains_text(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(1)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("the json path expression is not of text type")));
    }
    int argnum = 2;
    if (PG_ARGISNULL(0) || PG_ARGISNULL(argnum)) {
        PG_RETURN_NULL();
    }

    text* json = PG_GETARG_TEXT_P(0);
    char* pathStr = text_to_cstring(PG_GETARG_TEXT_P(1));
    char* raw = text_to_cstring(PG_GETARG_TEXT_P(2));
    bool bres = json_textcontains_internal(json, pathStr, raw);

    PG_RETURN_BOOL(bres);
}

JsonPath* CstringToJsonpath(char* s)
{
    return DatumGetJsonPathP(DirectInputFunctionCall(JSONPATHOID, s, -1));
}

/*
 * jsonb_path_exists
 *        Returns true if jsonpath returns at least one item for the specified
 *        jsonb value.  This function and jsonb_path_match() are used to
 *        implement @? and @@ operators, which in turn are intended to have an
 *        index support.  Thus, it's desirable to make it easier to achieve
 *        consistency between index scan results and sequential scan results.
 *        So, we throw as few errors as possible.  Regarding this function,
 *        such behavior also matches behavior of JSON_EXISTS() clause of
 *        SQL/JSON.  Regarding jsonb_path_match(), this function doesn't have
 *        an analogy in SQL/JSON, so we define its behavior on our own.
 */
static Datum jsonb_path_exists_internal(FunctionCallInfo fcinfo, bool tz)
{
    Jsonb *jb = PG_GETARG_JSONB(0);
    JsonPath *jp = PG_GETARG_JSONPATH_P(1);
    JsonPathExecResult res = JPER_OK;
    Jsonb *vars = PG_GETARG_JSONB(2);
    bool silent = PG_GETARG_BOOL(3);
    JsonPathExecContext cxt;

    initJsonPathExecContext(jp, &cxt);
    cxt.vars = vars;
    cxt.lastGeneratedObjectId += countVariablesFromJsonb(vars);
    cxt.throwErrors = !silent;

    res = executeJsonPath(jp, &cxt, jb, NULL);
    if (JperIsError(res)) {
        PG_RETURN_NULL();
    }

    PG_RETURN_BOOL(res == JPER_OK);
}

Datum jsonb_path_exists(PG_FUNCTION_ARGS)
{
    return jsonb_path_exists_internal(fcinfo, false);
}

static Datum jsonb_path_query_first_internal(FunctionCallInfo fcinfo, bool tz)
{
    Jsonb *jb = PG_GETARG_JSONB(0);
    JsonPath *jp = PG_GETARG_JSONPATH_P(1);
    JsonValueList found = {0};
    Jsonb *vars = PG_GETARG_JSONB(2);
    bool silent = PG_GETARG_BOOL(3);
    JsonPathExecContext cxt;

    initJsonPathExecContext(jp, &cxt);
    cxt.vars = vars;
    cxt.lastGeneratedObjectId += countVariablesFromJsonb(vars);
    cxt.throwErrors = !silent;

    (void)executeJsonPath(jp, &cxt, jb, &found);
    if (JsonValueListLength(&found) >= 1) {
        PG_RETURN_JSONB(JsonbValueToJsonb(JsonValueListHead(&found)));
    } else {
        PG_RETURN_NULL();
    }
}

Datum jsonb_path_query_first(PG_FUNCTION_ARGS)
{
    return jsonb_path_query_first_internal(fcinfo, false);
}

/********************Execute functions for JsonPath**************************/

/*
 * Interface to jsonpath executor
 *
 * 'path' - jsonpath to be executed
 * 'vars' - variables to be substituted to jsonpath
 * 'getVar' - callback used by getJsonPathVariable() to extract variables from
 *        'vars'
 * 'countVars' - callback to count the number of jsonpath variables in 'vars'
 * 'json' - target document for jsonpath evaluation
 * 'throwErrors' - whether we should throw suppressible errors
 * 'result' - list to store result items into
 *
 * Returns an error if a recoverable error happens during processing, or NULL
 * on no error.
 *
 * Note, jsonb and jsonpath values should be available and untoasted during
 * work because JsonPathItem, JsonbValue and result item could have pointers
 * into input values.  If caller needs to just check if document matches
 * jsonpath, then it doesn't provide a result arg.  In this case executor
 * works till first positive result and does not check the rest if possible.
 * In other case it tries to find all the satisfied result items.
 */
JsonPathExecResult executeJsonPath(
    JsonPath *path, JsonPathExecContext* cxt, Jsonb *json, JsonValueList *result)
{
    JsonPathExecResult res;
    JsonPathItem jsp;
    JsonbValue jbv;

    JspInit(&jsp, path);

    if (!JsonbExtractScalar(json, &jbv)) {
        JsonbInitBinary(&jbv, json);
    }

    cxt->root = &jbv;
    cxt->current = &jbv;
    if (JspStrictAbsenceOfErrors(cxt) && !result) {
        /*
         * In strict mode we must get a complete list of values to check that
         * there are no errors at all.
         */
        JsonValueList vals = {0};
        res = executeItem(cxt, &jsp, &jbv, &vals);
        if (JperIsError(res)) {
            return res;
        }

        return JsonValueListIsEmpty(&vals) ? JPER_NOT_FOUND : JPER_OK;
    }
    res = executeItem(cxt, &jsp, &jbv, result);

    Assert(!cxt->throwErrors || !JperIsError(res) || DB_IS_CMPT(A_FORMAT));

    return res;
}

/*
 * Execute jsonpath with automatic unwrapping of current item in lax mode.
 */
JsonPathExecResult executeItem(
    JsonPathExecContext *cxt, JsonPathItem *jsp,
    JsonbValue *jb, JsonValueList *found)
{
    return executeItemOptUnwrapTarget(cxt, jsp, jb, found, JspAutoUnwrap(cxt));
}


/*
 * Main jsonpath executor function: walks on jsonpath structure, finds
 * relevant parts of jsonb and evaluates expressions over them.
 * When 'unwrap' is true current SQL/JSON item is unwrapped if it is an array.
 */
JsonPathExecResult executeItemOptUnwrapTarget(
    JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
    JsonValueList *found, bool unwrap)
{
    check_stack_depth();
    CHECK_FOR_INTERRUPTS();

    /* only trailing [0] or [*] can be laxed multiple times in A */
    if (DB_IS_CMPT(A_FORMAT) && (cxt->timesLaxed > 1) && !(cxt->unlimitedKeyLax) &&
        (jsp->type != JPI_ANY_ARRAY) && (jsp->type != JPI_INDEX_ARRAY)) {
        return JPER_ERROR;
    }

    switch (jsp->type) {
        case JPI_NULL:
        case JPI_BOOL:
        case JPI_NUMERIC:
        case JPI_STRING:
        case JPI_VARIABLE:
            return executeScarlarAndVariable(cxt, jsp, jb, found);
        case JPI_AND:
        case JPI_OR:
        case JPI_NOT:
        case JPI_EQUAL:
        case JPI_NOT_EQUAL:
        case JPI_LESS:
        case JPI_GREATER:
        case JPI_LESS_OR_EQUAL:
        case JPI_GREATER_OR_EQUAL:
        case JPI_EXISTS:
        case JPI_STARTS_WITH:
        case JPI_LIKE_REGEX:
        case JPI_IS_UNKNOWN:
            return appendBoolResult(cxt, jsp, found, executeBoolItem(cxt, jsp, jb, true));
        case JPI_ADD:
            return executeBinaryArithmExpr(cxt, jsp, jb, numeric_add_opt_error, found);
        case JPI_SUB:
            return executeBinaryArithmExpr(cxt, jsp, jb, numeric_sub_opt_error, found);
        case JPI_MUL:
            return executeBinaryArithmExpr(cxt, jsp, jb, numeric_mul_opt_error, found);
        case JPI_DIV:
            return executeBinaryArithmExpr(cxt, jsp, jb, numeric_div_opt_error, found);
        case JPI_MOD:
            return executeBinaryArithmExpr(cxt, jsp, jb, numeric_mod_opt_error, found);
        case JPI_PLUS:
            return executeUnaryArithmExpr(cxt, jsp, jb, NULL, found);
        case JPI_MINUS:
            return executeUnaryArithmExpr(cxt, jsp, jb, numeric_uminus, found);
        case JPI_ANY_ARRAY:
            return executeAnyArray(cxt, jsp, jb, found);
        case JPI_ANY_KEY:
            return executeAnyKey(cxt, jsp, jb, found, unwrap);
        case JPI_INDEX_ARRAY:
            return executeIndexArray(cxt, jsp, jb, found);
        case JPI_ANY:
            return executeAny(cxt, jsp, jb, found);
        case JPI_KEY:
            return executeKey(cxt, jsp, jb, found, unwrap);
        case JPI_ROOT:
            return executeRoot(cxt, jsp, jb, found);
        case JPI_FILTER:
            return executeFilter(cxt, jsp, jb, found, unwrap);
        case JPI_ABS:
        case JPI_FLOOR:
        case JPI_CEILING:
            return executeNumericItemMethod(cxt, jsp, jb, unwrap, found);
        case JPI_SIZE:
            return executeSizeMethod(cxt, jsp, jb, found);
        case JPI_TYPE:
            return executeTypeMethod(cxt, jsp, jb, found);
        case JPI_KEYVALUE:
            return executeKeyValueMethodExt(cxt, jsp, jb, found, unwrap);
        case JPI_CURRENT:
            return executeNextItem(cxt, jsp, NULL, cxt->current, found, true);
        case JPI_LAST:
            return executeLast(cxt, jsp, jb, found);
        case JPI_DECIMAL:
        case JPI_NUMBER:
            return executedDecimalNumberMethod(cxt, jsp, jb, found, unwrap);
        case JPI_STRING_FUNC:
            return executeStringMethod(cxt, jsp, jb, found, unwrap);
        case JPI_DOUBLE:
            return executeDoubleMethod(cxt, jsp, jb, found, unwrap);
        case JPI_BOOLEAN:
            return executeBooleanMethod(cxt, jsp, jb, found, unwrap);
        case JPI_BIGINT:
            return executeBigintMethod(cxt, jsp, jb, found, unwrap);
        case JPI_INTEGER:
            return executeIntegerMethod(cxt, jsp, jb, found, unwrap);
        default:
            elog(ERROR, "unrecognized jsonpath item type: %d", jsp->type);
    }
}

/*
 * Unwrap current array item and execute jsonpath for each of its elements.
 */
JsonPathExecResult executeItemUnwrapTargetArray(
    JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found,
    bool unwrapElements)
{
    if (jb->type != jbvBinary) {
        Assert(jb->type != jbvArray);
        elog(ERROR, "invalid jsonb array value type: %d", jb->type);
    }

    JsonPathExecAnyItemContext eaicxt;
    eaicxt.cxt = cxt;
    eaicxt.first = 1;
    eaicxt.last = 1;
    eaicxt.level = 1;
    eaicxt.ignoreStructuralErrors = false;
    eaicxt.unwrapNext = unwrapElements;

    return executeAnyItem
        (&eaicxt, jsp, jb->binary.data, found);
}

/*
 * Execute next jsonpath item if exists.  Otherwise put "v" to the "found"
 * list if provided.
 */
JsonPathExecResult executeNextItem(JsonPathExecContext *cxt,
                JsonPathItem *cur, JsonPathItem *next,
                JsonbValue *v, JsonValueList *found, bool copy)
{
    JsonPathItem elem;
    bool hasNext;

    if (!cur) {
        hasNext = next != NULL;
    } else if (next) {
        hasNext = jspHasNext(cur);
    } else {
        next = &elem;
        hasNext = jspGetNext(cur, next);
    }

    if (hasNext) {
        return executeItem(cxt, next, v, found);
    }

    if (found) {
        JsonValueListAppend(found, copy ? copyJsonbValue(v) : v);
    }

    return JPER_OK;
}

/*
 * Convert jsonpath's scalar or variable node to actual jsonb value.
 *
 * If node is a variable then its id returned, otherwise 0 returned.
 */
void getJsonPathItem(
    JsonPathExecContext *cxt, JsonPathItem *item, JsonbValue *value)
{
    switch (item->type) {
        case JPI_NULL:
            value->type = jbvNull;
            value->estSize = sizeof(JEntry);
            break;
        case JPI_BOOL:
            value->type = jbvBool;
            value->boolean = jspGetBool(item);
            value->estSize = sizeof(JEntry);
            break;
        case JPI_NUMERIC: {
            int scale = 2;
            value->type = jbvNumeric;
            value->numeric = jspGetNumeric(item);
            value->estSize = scale * sizeof(JEntry) + VARSIZE_ANY(value->numeric);
            break;
        }
        case JPI_STRING:
            value->type = jbvString;
            value->string.val = jspGetString(item,
                                                 &value->string.len);
            value->estSize = sizeof(JEntry) + value->string.len;
            break;
        case JPI_VARIABLE:
            getJsonPathVariable(cxt, item, value);
            return;
        default:
            elog(ERROR, "unexpected jsonpath item type");
    }
}

/**************** Support functions for JsonPath execution *****************/

/*
 * Initialize some of JsonPathExecContext's default values.
 */

void initJsonPathExecContext(JsonPath* jp, JsonPathExecContext* cxt)
{
    cxt->laxMode = (jp->header & JSONPATH_LAX) != 0;
    cxt->ignoreStructuralErrors = cxt->laxMode;
    cxt->timesLaxed = 0;
    cxt->throwErrors = true;
    cxt->unlimitedKeyLax = false;
    cxt->vars = NULL;
    cxt->getVar = getJsonPathVariableFromJsonb;
    cxt->innermostArraySize = -1;
    cxt->baseObject.id = 0;
    cxt->baseObject.sheader = NULL;
    cxt->lastGeneratedObjectId = 1;
}

/*
 * Returns the size of an array item, or -1 if item is not an array.
 */
int JsonbArraySize(JsonbValue *jb)
{
    Assert(jb->type != jbvArray);

    if (jb->type == jbvBinary) {
        JsonbSuperHeader sheader = jb->binary.data;

        if (JsonbSuperHeaderIsArray(sheader) && !JsonbSuperHeaderIsScalar(sheader)) {
            return JsonbSuperHeaderSize(sheader);
        }
    }

    return -1;
}

JsonbValue* copyJsonbValue(JsonbValue *src)
{
    JsonbValue *dst = (JsonbValue*)palloc(sizeof(*dst));

    *dst = *src;

    return dst;
}

/*
 * Execute array subscript expression and convert resulting numeric item to
 * the integer type with truncation.
 */
JsonPathExecResult getArrayIndex(
    JsonPathExecContext *cxt, JsonPathItem *jsp,
    JsonbValue *jb, int32 *index)
{
    JsonbValue *jbv;
    JsonValueList found = {0};
    int saveTimesLaxed = cxt->timesLaxed;
    cxt->timesLaxed = 0;
    JsonPathExecResult res = executeItem(cxt, jsp, jb, &found);
    cxt->timesLaxed = saveTimesLaxed;
    Datum numeric_index;
    bool haveError = false;

    if (JperIsError(res)) {
        return res;
    }

    if (JsonValueListLength(&found) != 1 ||
        !(jbv = getScalar(JsonValueListHead(&found), jbvNumeric))) {
        RETURN_ERROR(ereport(ERROR,
                             (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                              errmsg("jsonpath array subscript is not a single numeric value"))));
        }

    numeric_index = DirectFunctionCall2(numeric_trunc,
                                        NumericGetDatum(jbv->numeric),
                                        Int32GetDatum(0));

    int changed = DatumGetInt32(DirectFunctionCall2(
        numeric_cmp, numeric_index, NumericGetDatum(jbv->numeric)));
    *index = numeric_int4_opt_error(DatumGetNumeric(numeric_index), false,
                                    &haveError);
    if (DB_IS_CMPT(A_FORMAT) && (changed != 0 || *index < 0)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("invalid jsonpath array index: %s",
                DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(jbv->numeric)))),
            errdetail("A_FORMAT database only supports non-negative integer(s) as jsonpath array index")));
    }

    if (haveError) {
        RETURN_ERROR(ereport(ERROR,
                             (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                              errmsg("jsonpath array subscript is out of integer range"))));
    }

    return JPER_OK;
}

void JsonValueListAppend(JsonValueList *jvl, JsonbValue *jbv)
{
    if (jvl->singleton) {
        jvl->list = list_make2(jvl->singleton, jbv);
        jvl->singleton = NULL;
    } else if (!jvl->list) {
        jvl->singleton = jbv;
    } else {
        jvl->list = lappend(jvl->list, jbv);
    }
}

int JsonValueListLength(const JsonValueList *jvl)
{
    return jvl->singleton ? 1 : list_length(jvl->list);
}

bool JsonValueListIsEmpty(JsonValueList *jvl)
{
    return !jvl->singleton && (jvl->list == NIL);
}

JsonbValue* JsonValueListHead(JsonValueList *jvl)
{
    return jvl->singleton ? jvl->singleton : (JsonbValue*)linitial(jvl->list);
}

void JsonValueListInitIterator(const JsonValueList *jvl, JsonValueListIterator *it)
{
    if (jvl->singleton) {
        it->value = jvl->singleton;
        it->list = NIL;
        it->next = NULL;
    } else if (jvl->list != NIL) {
        it->value = (JsonbValue *) linitial(jvl->list);
        it->list = jvl->list;
        if (jvl->list && list_length(jvl->list) > 1) {
            it->next = lnext(list_head(jvl->list));
        } else {
            it->next = NULL;
        }
    } else {
        it->value = NULL;
        it->list = NIL;
        it->next = NULL;
    }
}

/*
 * Get the next item from the sequence advancing iterator.
 */
JsonbValue* JsonValueListNext(const JsonValueList *jvl, JsonValueListIterator *it)
{
    JsonbValue *result = it->value;

    if (it->next) {
        it->value = (JsonbValue*)lfirst(it->next);
        it->next = lnext(it->next);
    } else {
        it->value = NULL;
    }

    return result;
}

/*
 * Initialize a binary JsonbValue with the given jsonb container.
 */
JsonbValue* JsonbInitBinary(JsonbValue *jbv, Jsonb *jb)
{
    int32 scale = 2;
    jbv->type = jbvBinary;
    jbv->binary.data = (JsonbSuperHeader)(&jb->superheader);
    jbv->binary.len = VARSIZE_ANY_EXHDR(jb);
    jbv->estSize = jbv->binary.len + scale * sizeof(JEntry);

    return jbv;
}

/*
 * Returns jbv* type of JsonbValue. Note, it never returns jbvBinary as is.
 */
int JsonbType(JsonbValue *jb)
{
    int type = jb->type;

    if (jb->type == jbvBinary) {
        JsonbSuperHeader sheader = jb->binary.data;

        /* Scalars should be always extracted during jsonpath execution. */
        Assert(!JsonbSuperHeaderIsScalar(sheader));

        if (JsonbSuperHeaderIsObject(sheader)) {
            type = jbvObject;
        } else if (JsonbSuperHeaderIsArray(sheader)) {
            type = jbvArray;
        } else {
            elog(ERROR, "invalid jsonb container type: 0x%08x", (*(uint32 *)sheader));
        }
    }

    return type;
}

/* Get scalar of given type or NULL on type mismatch */
JsonbValue* getScalar(JsonbValue *scalar, enum jbvType type)
{
    /* Scalars should be always extracted during jsonpath execution. */
    Assert(scalar->type != jbvBinary ||
           !JsonbSuperHeaderIsScalar(scalar->binary.data));

    return scalar->type == type ? scalar : NULL;
}

Datum DirectInputFunctionCall(Oid typoid, char* str, int32 typmod)
{
    Oid typiofunc;
    Oid typioparam;
    FmgrInfo typinputproc;

    getTypeInputInfo(typoid, &typiofunc, &typioparam);
    fmgr_info_cxt(typiofunc, &typinputproc, CurrentMemoryContext);

    return InputFunctionCall(&typinputproc, str, typioparam, typmod);
}

inline bool ValidJsonChar(char c)
{
    return (('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') ||
            ('0' <= c && c <= '9') || ('\200' <= c && c <= '\377'));
}

static void TrimString(char** lptr, char** rptr)
{
    while (!ValidJsonChar(**lptr)) {
        (*lptr)++;
    }
    while (!ValidJsonChar(**rptr)) {
        (*rptr)--;
    }
}

static void SplitString(char* str, char delim, int strlen, List** results)
{
    char* lptr = str;
    char* rptr = str;

    while (*rptr != '\0' && strlen != 0) {
        if (*rptr != delim && strlen != 1) {
            strlen--;
            rptr++;
            continue;
        }
        if (rptr != lptr || strlen == 1) {
            char* tlptr = lptr;
            char* trptr = rptr;
            TrimString(&tlptr, &trptr);
            if (trptr < tlptr) {
                return;
            }
            int len = trptr - tlptr + 2;
            char* res = (char*)palloc(len * sizeof(char));
            int rc = memcpy_s(res, len, tlptr, len);
            securec_check(rc, "\0", "\0");
            res[len - 1] = '\0';
            *results = lappend(*results, res);
        }
        lptr = rptr + 1;
        strlen--;
        rptr++;
    }
}

static List* SplitRawTarget(char* raw)
{
    List* targetStrList = NIL;
    List* row = NIL;
    char* cxt;
    char* tok = strtok_s(raw, ",", &cxt);
    while (tok != NULL) {
        row = NIL;
        SplitString(tok, ' ', strlen(tok), &row);
        if (row != NIL) {
            targetStrList = lappend(targetStrList, row);
        }
        tok = strtok_s(NULL, ",", &cxt);
    }
    return targetStrList;
}

static char* TrimScalarJsonbValueStr(char* jbvStr, int len)
{
    char* lptr = jbvStr;
    char* rptr = lptr + len - 1;
    TrimString(&lptr, &rptr);

    int trimedLen = rptr - lptr + 2;
    char* res = (char*)palloc(trimedLen * sizeof(char));
    int rc = memcpy_s(res, trimedLen, lptr, trimedLen);
    securec_check(rc, "\0", "\0");
    res[trimedLen - 1] = '\0';
    return res;
}

void JsonbValueToString(JsonbValue* jbv, char** str, int* len)
{
    *len = -1;
    switch (jbv->type) {
        case jbvNull: {
            *str = pstrdup("null");
            break;
        }
        case jbvNumeric: {
            *str = pstrdup(DatumGetCString(DirectFunctionCall1(numeric_out,
                NumericGetDatum(jbv->numeric))));
            break;
        }
        case jbvBool: {
            *str = (jbv->boolean) ? pstrdup("true") : pstrdup("false");
            break;
        }
        default:
            elog(ERROR, "unexpected jsonb value type: %d", jbv->type);
    }
    *len = *len == -1 ? strlen(*str) : *len;
}

static List* CollectScalarJsonbValue(JsonPathExecContext* cxt, JsonValueList* jvl)
{
    JsonValueListIterator it1;
    JsonbValue* jbv;
    List* res = NIL;

    JsonValueListInitIterator(jvl, &it1);
    while ((jbv = JsonValueListNext(jvl, &it1))) {
        if (jbv->type == jbvBinary) {
            JsonValueList result = {0};
            uint32 lastLevel = PG_UINT32_MAX;
            JsonPathExecAnyItemContext eaicxt;
            eaicxt.cxt = cxt;
            eaicxt.level = 1;
            eaicxt.first = lastLevel;
            eaicxt.last = lastLevel;
            eaicxt.ignoreStructuralErrors = true;
            eaicxt.unwrapNext = JspAutoUnwrap(cxt);
            executeAnyItem(&eaicxt, NULL, jbv->binary.data, &result);

            List* subResultList = CollectScalarJsonbValue(cxt, &result);
            res = list_concat(res, subResultList);
        } else if (jbv->type == jbvString) {
            List* subResultList = NIL;
            SplitString(jbv->string.val, ' ', jbv->string.len, &subResultList);
            res = list_concat(res, subResultList);
        } else {
            char* strJVal;
            int len;
            char* trimed;
            JsonbValueToString(jbv, &strJVal, &len);
            trimed = TrimScalarJsonbValueStr(strJVal, len);
            pfree(strJVal);
            res = lappend(res, trimed);
        }
    }
    return res;
}

static bool MatchTargetInJBValueList(List* jbvs, List* targets)
{
    ListCell* lc;
    foreach (lc, targets) {
        List* row = (List*)lfirst(lc);
        int rowLen = list_length(row);
        int jbvcnt = list_length(jbvs);
        int jbvLpos = 0;
        ListCell* rowptr = list_head(row);
        ListCell* jbvLptr = list_head(jbvs);
        ListCell* jbvRptr = jbvLptr;
        /* abort match for current target when not enough jbv left */
        while (jbvLpos <= jbvcnt - rowLen) {
            char* jbvStr = (char*)lfirst(jbvRptr);
            char* targetStr = (char*)lfirst(rowptr);
            if (pg_strcasecmp(jbvStr, targetStr) == 0) {
                jbvRptr = lnext(jbvRptr);
                rowptr = lnext(rowptr);
                if (rowptr == NULL) {
                    /* successfully matched all values in a row */
                    return true;
                } else {
                    /* match in progress, continue next round */
                    continue;
                }
            }
            /* Match failed, retry from next start point */
            jbvLpos++;
            jbvLptr = lnext(jbvLptr);
            jbvRptr = jbvLptr;
            rowptr = list_head(row);
        }
    }
    return false;
}

/* Implementation of JPI_ROOT ($) */
JsonPathExecResult executeRoot(JsonPathExecContext* cxt, JsonPathItem* jsp,
    JsonbValue* jb, JsonValueList* found)
{
    jb = cxt->root;
    JsonBaseObjectInfo baseObject = setBaseObject(cxt, jb, 0);
    JsonPathExecResult res = executeNextItem(cxt, jsp, NULL, jb, found, true);
    cxt->baseObject = baseObject;
    return res;
}

/* Implementation of nodes of scalar and variable types:
 *  - JPI_NULL (NULL literal)
 *  - JPI_STRING (string literal)
 *  - JPI_NUMERIC (numeric literal)
 *  - JPI_BOOL (boolean literal)
 *  - JPI_VARIABLE ($variable)
 */
JsonPathExecResult executeScarlarAndVariable(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found)
{
    JsonbValue vbuf;
    JsonbValue *v;
    JsonPathItem elem;
    bool hasNext = jspGetNext(jsp, &elem);

    JspResetAFormatLaxMode(cxt);

    if (!hasNext && !found && jsp->type != JPI_VARIABLE) {
        /*
            * Skip evaluation, but not for variables.  We must
            * trigger an error for the missing variable.
            */
        return JPER_OK;
    }

    v = hasNext ? &vbuf : (JsonbValue*)palloc(sizeof(*v));

    JsonBaseObjectInfo baseObject = cxt->baseObject;
    getJsonPathItem(cxt, jsp, v);

    JsonPathExecResult res = executeNextItem(cxt, jsp, &elem,
                            v, found, hasNext);
    cxt->baseObject = baseObject;
    return res;
}

/* Implementation of JPI_ANY_ARRAY ([*] accessor) */
JsonPathExecResult executeAnyArray(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found)
{
    JsonPathItem elem;
    JsonPathExecResult res = JPER_NOT_FOUND;
    if (JsonbValueIsScalar(jb) && DB_IS_CMPT(A_FORMAT)) {
        JspResetAFormatLaxMode(cxt);
    }
    if (JsonbType(jb) == jbvArray) {
        bool hasNext = jspGetNext(jsp, &elem);

        JspResetAFormatLaxMode(cxt);
        res = executeItemUnwrapTargetArray(cxt, hasNext ? &elem : NULL,
                                            jb, found, JspAutoUnwrap(cxt));
    } else if (JspAutoWrap(cxt)) {
        JspSetAFormatLaxMode(cxt);
        res = executeNextItem(cxt, jsp, NULL, jb, found, true);
    } else if (DB_IS_CMPT(A_FORMAT)) {
        return JPER_ERROR;
    } else if (!JspIgnoreStructuralErrors(cxt)) {
        RETURN_ERROR(ereport(ERROR,
                                (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("jsonpath wildcard array accessor can only be applied to an array"))));
    }
    return res;
}

/* Implementation of JPI_ANY(.** accessor) */
JsonPathExecResult executeAny(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found)
{
    JsonPathItem elem;
    JsonPathExecResult res = JPER_NOT_FOUND;
    bool hasNext = jspGetNext(jsp, &elem);

    /* first try without any intermediate steps */
    if (jsp->content.anybounds.first == 0) {
        bool savedIgnoreStructuralErrors;

        savedIgnoreStructuralErrors = cxt->ignoreStructuralErrors;
        cxt->ignoreStructuralErrors = true;
        res = executeNextItem(cxt, jsp, &elem,
                                jb, found, true);
        cxt->ignoreStructuralErrors = savedIgnoreStructuralErrors;

        if (res == JPER_OK && !found) {
            return res;
        }
    }

    if (jb->type == jbvBinary) {
        JsonPathExecAnyItemContext eaicxt;
        eaicxt.cxt = cxt;
        eaicxt.level = 1;
        eaicxt.first = jsp->content.anybounds.first;
        eaicxt.last = jsp->content.anybounds.last;
        eaicxt.ignoreStructuralErrors = true;
        eaicxt.unwrapNext = JspAutoUnwrap(cxt);
        res = executeAnyItem(&eaicxt, hasNext ? &elem : NULL, jb->binary.data, found);
    }
    return res;
}

/* Implementation of JPI_ANY_KEY (.* accessor) */
JsonPathExecResult executeAnyKey(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap)
{
    JsonPathItem elem;
    JsonPathExecResult res = JPER_NOT_FOUND;

    if (JsonbType(jb) == jbvObject) {
        bool        hasNext = jspGetNext(jsp, &elem);

        if (jb->type != jbvBinary) {
            elog(ERROR, "invalid jsonb object type: %d", jb->type);
        }
        JspResetAFormatLaxMode(cxt);

        JsonPathExecAnyItemContext eaicxt;
        eaicxt.cxt = cxt;
        eaicxt.first = 1;
        eaicxt.last = 1;
        eaicxt.level = 1;
        eaicxt.ignoreStructuralErrors = false;
        eaicxt.unwrapNext = JspAutoUnwrap(cxt);
        return executeAnyItem (&eaicxt, hasNext ? &elem : NULL,
                jb->binary.data, found);
    } else if ((unwrap || (DB_IS_CMPT(A_FORMAT) && cxt->unlimitedKeyLax))
            && JsonbType(jb) == jbvArray) {
        JspSetAFormatLaxMode(cxt);
        return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
    } else if ((!JspIgnoreStructuralErrors(cxt)) && !DB_IS_CMPT(A_FORMAT)) {
        if (!JspIgnoreStructuralErrors(cxt)) {
            Assert(found);
        }
        RETURN_ERROR(ereport(ERROR,
                                (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("jsonpath wildcard member accessor can only be applied to an object"))));
    }

    return res;
}

/* Implementation of JPI_INDEX_ARRAY ([subscript, ...] accessor) */
JsonPathExecResult executeIndexArray(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found)
{
    JsonPathExecResult res = JPER_NOT_FOUND;
    int jsonbType = JsonbType(jb);
    if (JsonbValueIsScalar(jb) && DB_IS_CMPT(A_FORMAT)) {
        JspResetAFormatLaxMode(cxt);
    }
    if (jsonbType == jbvArray || JspAutoWrap(cxt)) {
        if (jsonbType != jbvArray) {
            JspSetAFormatLaxMode(cxt);
        }
        res = traverseIndexArrayItem(cxt, jsp, jb, found);
    } else if ((!JspIgnoreStructuralErrors(cxt)) && !DB_IS_CMPT(A_FORMAT)) {
        RETURN_ERROR(ereport(ERROR,
                                (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("jsonpath array accessor can only be applied to an array"))));
    }
    return res;
}

JsonPathExecResult traverseIndexArrayItem(
    JsonPathExecContext* cxt, JsonPathItem* jsp,
    JsonbValue* jb, JsonValueList* found)
{
    JsonPathItem elem;
    JsonPathExecResult res = JPER_NOT_FOUND;
    bool hasNext = jspGetNext(jsp, &elem);
    int innermostArraySize = cxt->innermostArraySize;
    int size = JsonbArraySize(jb) < 0 ? 1 : JsonbArraySize(jb);
    bool singleton = JsonbArraySize(jb) < 0;
    bool containsZero = false;

    cxt->innermostArraySize = size;

    for (int i = 0; i < jsp->content.array.nelems; i++) {
        JsonPathItem from;
        JsonPathItem to;
        JsonPathItem* fromto[2] = {&from, &to};
        int32 index;
        int32 index_from;
        int32 index_to;
        int32* indexes[2] = {&index_from, &index_to};
        bool range = jspGetArraySubscript(jsp, &from, &to, i);

        JsonPathExecResult tmpres =
            processArrayIndexFromTo(cxt, jb, fromto, indexes, range);
        if (JperIsError(tmpres)) {
            break;
        }
        containsZero = (index_from == 0) ? true : containsZero;

        for (index = index_from; index <= index_to; index++) {
            JsonbValue *v;
            bool copy = singleton;
            if (singleton) {
                v = jb;
            } else {
                v = getIthJsonbValueFromSuperHeader(jb->binary.data,
                                                    (uint32) index);
            }
            if (!singleton && v == NULL) {
                continue;
            }
            if (!hasNext && !found) {
                return JPER_OK;
            }

            res = executeNextItem(cxt, jsp, &elem, v, found, copy);
            if ((JperIsError(res)) || (res == JPER_OK && !found)) {
                break;
            }
        }
        if ((JperIsError(res)) || (res == JPER_OK && !found)) {
            break;
        }
    }
    cxt->innermostArraySize = innermostArraySize;
    return ((cxt->timesLaxed > 1 && !containsZero)) ? JPER_ERROR : res;
}

JsonPathExecResult processArrayIndexFromTo(JsonPathExecContext* cxt,
    JsonbValue* jb, JsonPathItem** fromto, int32** indexes, bool range)
{
    JsonPathItem* from = fromto[0];
    JsonPathItem* to = fromto[1];
    int32* index_from = indexes[0];
    int32* index_to = indexes[1];
    JsonPathExecResult res = JPER_NOT_FOUND;
    int size = JsonbArraySize(jb);
    bool singleton = size < 0;
    if (singleton) {
        size = 1;
    }

    res = getArrayIndex(cxt, from, jb, index_from);
    if (JperIsError(res)) {
        return res;
    }

    if (range) {
        res = getArrayIndex(cxt, to, jb, index_to);
        if (JperIsError(res)) {
            return res;
        }
    } else {
        *index_to = *index_from;
    }

    if (DB_IS_CMPT(A_FORMAT) && *index_from > *index_to) {
        int tmp = *index_from;
        *index_from = *index_to;
        *index_to = tmp;
    }

    if (!JspIgnoreStructuralErrors(cxt) &&
        (*index_from < 0 || *index_from > *index_to ||
        *index_to >= size)) {
        RETURN_ERROR(ereport(ERROR,
                                (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                                errmsg("jsonpath array subscript is out of bounds"))));
        }

    if (*index_from < 0) {
        *index_from = 0;
    }

    if (*index_to >= size) {
        *index_to = size - 1;
    }
    return JPER_OK;
}

/* Implementation of JPI_KEY (.key accessor) */
JsonPathExecResult executeKey(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap)
{
    JsonPathItem elem;
    JsonPathExecResult res = JPER_NOT_FOUND;
    if (JsonbType(jb) == jbvObject) {
        JsonbValue *v;
        JsonbValue key;

        key.type = jbvString;
        key.string.val = jspGetString(jsp, &key.string.len);

        JspResetAFormatLaxMode(cxt);

        v = u_sess->parser_cxt.disable_jsonb_auto_sort ?
                FindJsonbValueFromUnsortedObjects(jb->binary.data, &key) :
                findJsonbValueFromSuperHeader(jb->binary.data, JB_FOBJECT, NULL, &key);
        if (v != NULL) {
            res = executeNextItem(cxt, jsp, NULL,
                                    v, found, false);
            /* free value if it was not added to found list */
            if (jspHasNext(jsp) || !found) {
                pfree(v);
            }
        } else if (!JspIgnoreStructuralErrors(cxt)) {
            Assert(found);
            if (!JspThrowErrors(cxt)) {
                return JPER_ERROR;
            }
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT), \
                        errmsg("JSON object does not contain key \"%s\"",
                            pnstrdup(key.string.val,
                                        key.string.len))));
        }
    } else if ((unwrap || (DB_IS_CMPT(A_FORMAT) && cxt->unlimitedKeyLax))
            && JsonbType(jb) == jbvArray) {
        JspSetAFormatLaxMode(cxt);
        return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
    } else if ((!JspIgnoreStructuralErrors(cxt)) && !DB_IS_CMPT(A_FORMAT)) {
        if (!JspIgnoreStructuralErrors(cxt)) {
            Assert(found);
        }
        RETURN_ERROR(ereport(ERROR,
                                (errcode(ERRCODE_UNDEFINED_OBJECT),
                                errmsg("jsonpath member accessor can only be applied to an object"))));
    }
    return res;
}


/*
 * Implementation of several jsonpath nodes:
 *  - JPI_ANY (.** accessor),
 *  - JPI_ANY_KEY (.* accessor),
 *  - JPI_ANY_ARRAY ([*] accessor)
 */
JsonPathExecResult executeAnyItem(
    JsonPathExecAnyItemContext *eaicxt, JsonPathItem *jsp,
    JsonbSuperHeader sheader, JsonValueList *found)
{
    uint32 level = eaicxt->level;
    uint32 first = eaicxt->first;
    uint32 last = eaicxt->last;
    JsonPathExecResult res = JPER_NOT_FOUND;
    JsonbIterator *it;
    int32 r;
    JsonbValue v;

    check_stack_depth();

    if (level > last) {
        return res;
    }

    it = JsonbIteratorInit(sheader);
    /*
     * Recursively iterate over jsonb objects/arrays
     */
    while ((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE) {
        if (r == WJB_KEY) {
            r = JsonbIteratorNext(&it, &v, true);
            Assert(r == WJB_VALUE);
        } else if (r != WJB_VALUE && r != WJB_ELEM) {
            continue;
        }

        if (level >= first ||
            (first == PG_UINT32_MAX && last == PG_UINT32_MAX &&
                v.type != jbvBinary)) {  /* leaves only requested */
            res = executeAnyNext(eaicxt, jsp, v, found);
            if ((JperIsError(res)) || (res == JPER_OK && !found)) {
                break;
            }
        }

        if (level < last && v.type == jbvBinary) {
            eaicxt->level++;
            res = executeAnyItem(eaicxt, jsp, v.binary.data, found);
            if (JperIsError(res)) {
                break;
            } else if (res == JPER_OK && found == NULL) {
                break;
            }
        }
    }

    return res;
}

/* call next path node's execution of JPI_ANY */
JsonPathExecResult executeAnyNext(
    JsonPathExecAnyItemContext *eaicxt, JsonPathItem* jsp,
    JsonbValue v, JsonValueList *found)
{
    JsonPathExecContext* cxt = eaicxt->cxt;
    bool unwrapNext = eaicxt->unwrapNext;
    JsonPathExecResult res = JPER_NOT_FOUND;
    /* check expression */
    if (jsp) {
        if (eaicxt->ignoreStructuralErrors) {
            bool savedIgnoreStructuralErrors;

            savedIgnoreStructuralErrors = cxt->ignoreStructuralErrors;
            cxt->ignoreStructuralErrors = true;
            res = executeItemOptUnwrapTarget(cxt, jsp, &v, found, unwrapNext);
            cxt->ignoreStructuralErrors = savedIgnoreStructuralErrors;
        } else {
            res = executeItemOptUnwrapTarget(cxt, jsp, &v, found, unwrapNext);
        }

        if ((JperIsError(res)) || (res == JPER_OK && !found)) {
            return res;
        }
    } else if (found) {
        JsonValueListAppend(found, copyJsonbValue(&v));
    } else {
        return JPER_OK;
    }
    return res;
}

/* Implementation of JPI_LAST( keyword LAST as an array subscript) */
JsonPathExecResult executeLast(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found)
{
    JsonPathItem elem;
    JsonPathExecResult res = JPER_NOT_FOUND;
    JsonbValue tmpjbv;
    JsonbValue *lastjbv;
    int last;
    int scale = 2;
    bool hasNext = jspGetNext(jsp, &elem);

    if (cxt->innermostArraySize < 0) {
        elog(ERROR, "evaluating jsonpath LAST outside of array subscript");
    }

    if (!hasNext && !found) {
        res = JPER_OK;
        return res;
    }

    last = cxt->innermostArraySize - 1;
    lastjbv = hasNext ? &tmpjbv : (JsonbValue*)palloc(sizeof(*lastjbv));
    lastjbv->type = jbvNumeric;
    lastjbv->numeric = int64_to_numeric(last);
    lastjbv->estSize = scale * sizeof(JEntry) + VARSIZE_ANY(lastjbv->numeric);

    res = executeNextItem(cxt, jsp, &elem, lastjbv, found, hasNext);
    return res;
}

/*
 * Same as executeItem(), but when "unwrap == true" automatically unwraps
 * each array item from the resulting sequence in lax mode.
 */
JsonPathExecResult executeItemOptUnwrapResult(
    JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
    bool unwrap, JsonValueList *found)
{
    if (unwrap && JspAutoUnwrap(cxt)) {
        JsonValueList seq = {0};
        JsonValueListIterator it;
        JsonPathExecResult res = executeItem(cxt, jsp, jb, &seq);
        JsonbValue *item;

        if (JperIsError(res)) {
            return res;
        }

        JsonValueListInitIterator(&seq, &it);
        while ((item = JsonValueListNext(&seq, &it))) {
            Assert(item->type != jbvArray);

            if (JsonbType(item) == jbvArray) {
                executeItemUnwrapTargetArray(cxt, NULL, item, found, false);
            } else {
                JsonValueListAppend(found, item);
            }
        }

        return JPER_OK;
    }

    return executeItem(cxt, jsp, jb, found);
}

/*
 * Execute unary arithmetic expression for each numeric item in its operand's
 * sequence.  Array operand is automatically unwrapped in lax mode.
 */
JsonPathExecResult executeUnaryArithmExpr(
    JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb, PGFunction func,
    JsonValueList *found)
{
    JsonPathExecResult jper;
    JsonPathExecResult jper2;
    JsonPathItem elem;
    JsonValueList seq = {0};
    JsonValueListIterator it;
    JsonbValue *val;
    bool hasNext;

    JspGetArg(jsp, &elem);
    jper = executeItemOptUnwrapResult(cxt, &elem, jb, true, &seq);
    if (JperIsError(jper)) {
        return jper;
    }

    jper = JPER_NOT_FOUND;

    hasNext = jspGetNext(jsp, &elem);

    JsonValueListInitIterator(&seq, &it);
    while ((val = JsonValueListNext(&seq, &it))) {
        if ((val = getScalar(val, jbvNumeric))) {
            if (!found && !hasNext) {
                return JPER_OK;
            }
        } else {
            if (!found && !hasNext) {
                continue;        /* skip non-numerics processing */
            }

            RETURN_ERROR(ereport(ERROR,
                                 (errcode(ERRCODE_UNDEFINED_OBJECT),
                                  errmsg("operand of unary jsonpath operator %s is not a numeric value",
                                         JspOperationName(jsp->type)))));
        }

        if (func) {
            val->numeric =
                DatumGetNumeric(DirectFunctionCall1(func,
                                                    NumericGetDatum(val->numeric)));
        }

        jper2 = executeNextItem(cxt, jsp, &elem, val, found, false);
        if (JperIsError(jper2)) {
            return jper2;
        } else if (jper2 == JPER_OK) {
            if (!found) {
                return JPER_OK;
            }
            jper = JPER_OK;
        }
    }

    return jper;
}

/*
 * Execute binary arithmetic expression on singleton numeric operands.
 * Array operands are automatically unwrapped in lax mode.
 */
JsonPathExecResult executeBinaryArithmExpr(
    JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
    BinaryArithmFunc func, JsonValueList *found)
{
    JsonPathExecResult jper;
    JsonPathItem elem;
    JsonValueList lseq = {0};
    JsonValueList rseq = {0};
    JsonbValue *lval;
    JsonbValue *rval;
    Numeric res;

    JspGetLeftArg(jsp, &elem);

    /*
     * XXX: By standard only operands of multiplicative expressions are
     * unwrapped.  We extend it to other binary arithmetic expressions too.
     */
    jper = executeItemOptUnwrapResult(cxt, &elem, jb, true, &lseq);
    if (JperIsError(jper)) {
        return jper;
    }

    JspGetRightArg(jsp, &elem);

    jper = executeItemOptUnwrapResult(cxt, &elem, jb, true, &rseq);
    if (JperIsError(jper)) {
        return jper;
    }

    if (JsonValueListLength(&lseq) != 1 ||
        !(lval = getScalar(JsonValueListHead(&lseq), jbvNumeric))) {
        RETURN_ERROR(ereport(ERROR,
                             (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                              errmsg("left operand of jsonpath operator %s is not a single numeric value",
                                     JspOperationName(jsp->type)))));
    }

    if (JsonValueListLength(&rseq) != 1 ||
        !(rval = getScalar(JsonValueListHead(&rseq), jbvNumeric))) {
        RETURN_ERROR(ereport(ERROR,
                             (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                              errmsg("right operand of jsonpath operator %s is not a single numeric value",
                                     JspOperationName(jsp->type)))));
    }

    if (JspThrowErrors(cxt)) {
        res = func(lval->numeric, rval->numeric, NULL);
    } else {
        bool        error = false;
        res = func(lval->numeric, rval->numeric, &error);
        if (error) {
            return JPER_ERROR;
        }
    }

    if (!jspGetNext(jsp, &elem) && !found) {
        return JPER_OK;
    }

    int scale = 2;
    lval = (JsonbValue*)palloc(sizeof(*lval));
    lval->type = jbvNumeric;
    lval->numeric = res;
    lval->estSize = scale * sizeof(JEntry) + VARSIZE_ANY(lval->numeric);

    return executeNextItem(cxt, jsp, &elem, lval, found, false);
}

/*
 * Same as executeItemOptUnwrapResult(), but with error suppression.
 */
JsonPathExecResult executeItemOptUnwrapResultNoThrow(
    JsonPathExecContext *cxt, JsonPathItem *jsp,
    JsonbValue *jb, bool unwrap, JsonValueList *found)
{
    JsonPathExecResult res;
    bool throwErrors = cxt->throwErrors;
    cxt->throwErrors = false;
    res = executeItemOptUnwrapResult(cxt, jsp, jb, unwrap, found);
    cxt->throwErrors = throwErrors;
    return res;
}

/*
 * Implementation of JPI_FILTER, executes predicates.
 * Array operands are automatically unwrapped in lax mode.
 */
JsonPathExecResult executeFilter(JsonPathExecContext* cxt, JsonPathItem* jsp,
    JsonbValue* jb, JsonValueList* found, bool unwrap)
{
    JsonPathBool st;
    JsonPathItem elem;

    if (unwrap && JsonbType(jb) == jbvArray) {
        return executeItemUnwrapTargetArray(cxt, jsp, jb, found,
                                            false);
    }

    JspGetArg(jsp, &elem);
    st = executeNestedBoolItem(cxt, &elem, jb);
    if (st != JPB_TRUE) {
        return JPER_NOT_FOUND;
    } else {
        return executeNextItem(cxt, jsp, NULL,
                                jb, found, true);
    }
}

/*
 * Execute nested (filters etc.) boolean expression pushing current SQL/JSON
 * item onto the stack.
 */
JsonPathBool executeNestedBoolItem(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb)
{
    JsonbValue *prev;
    JsonPathBool res;

    prev = cxt->current;
    cxt->current = jb;
    res = executeBoolItem(cxt, jsp, jb, false);
    cxt->current = prev;
    return res;
}

/* Execute boolean-valued jsonpath expression. */
JsonPathBool executeBoolItem(
    JsonPathExecContext *cxt, JsonPathItem *jsp,
    JsonbValue *jb, bool canHaveNext)
{
    switch (jsp->type) {
        case JPI_AND:
        case JPI_OR:
        case JPI_NOT:
            return executeLogicalOperator(cxt, jsp, jb);
        case JPI_EQUAL:
        case JPI_NOT_EQUAL:
        case JPI_LESS:
        case JPI_GREATER:
        case JPI_LESS_OR_EQUAL:
        case JPI_GREATER_OR_EQUAL:
        case JPI_STARTS_WITH:
            return executePredicateExt(cxt, jsp, jb);
            break;
        case JPI_IS_UNKNOWN:
            return executeIsUnknown(cxt, jsp, jb);
        case JPI_LIKE_REGEX:
            return executeLikeRegexExt(cxt, jsp, jb);
        case JPI_EXISTS:
            return executeExists(cxt, jsp, jb);
        default:
            elog(ERROR, "invalid boolean jsonpath item type: %d", jsp->type);
    }
    return JPB_UNKNOWN;
}

/*
 * Execute logical expression
 * with and(&&) / or(||) / not(!) as the operand.
 */
JsonPathBool executeLogicalOperator(
    JsonPathExecContext* cxt, JsonPathItem* jsp, JsonbValue* jb)
{
    JsonPathItem larg;
    JsonPathItem rarg;
    JsonPathBool res;
    JsonPathBool res2;
    JsonPathItemType type = jsp->type;

    JspGetLeftArg(jsp, &larg);
    res = executeBoolItem(cxt, &larg, jb, false);
    if ((res == JPB_FALSE && type == JPI_AND) ||
        (res == JPB_TRUE && type == JPI_OR) ||
        (res == JPB_UNKNOWN && type == JPI_NOT)) {
        return res;
    }
    if (type == JPI_NOT) {
        return res == JPB_TRUE ? JPB_FALSE : JPB_TRUE;
    }
    /*
    * SQL/JSON says that we should check second arg in case of
    * jperError
    */

    JspGetRightArg(jsp, &rarg);
    res2 = executeBoolItem(cxt, &rarg, jb, false);
    return type == JPI_AND ? (res2 == JPB_TRUE ? res : res2) :
            type == JPI_OR ? (res2 == JPB_FALSE ? res : res2) : JPB_UNKNOWN;
}

/*
 * Execute unary or binary predicate.
 *
 * Predicates have existence semantics, because their operands are item
 * sequences.  Pairs of items from the left and right operand's sequences are
 * checked.  TRUE returned only if any pair satisfying the condition is found.
 * In strict mode, even if the desired pair has already been found, all pairs
 * still need to be examined to check the absence of errors.  If any error
 * occurs, UNKNOWN (analogous to SQL NULL) is returned.
 */

JsonPathBool executePredicateExt(
    JsonPathExecContext* cxt, JsonPathItem* jsp, JsonbValue* jb)
{
    JsonPathItem larg;
    JsonPathItem rarg;
    JsonPathExecPredicateContext pcxt;

    JspGetLeftArg(jsp, &larg);
    JspGetRightArg(jsp, &rarg);
    pcxt.larg = &larg;
    pcxt.rarg = &rarg;
    pcxt.unwrapRightTarget = false;
    pcxt.exec = ((jsp->type == JPI_STARTS_WITH) ? executeStartsWith : executeComparison);
    pcxt.param = ((jsp->type == JPI_STARTS_WITH) ? NULL : cxt);
    return executePredicate(cxt, jsp, &pcxt, jb);
}

JsonPathBool executePredicate(
    JsonPathExecContext *cxt, JsonPathItem* pred,
    JsonPathExecPredicateContext* pcxt, JsonbValue* jb)
{
    JsonPathExecResult res;
    JsonValueListIterator lseqit;
    JsonValueList lseq = {0};
    JsonValueList rseq = {0};
    JsonbValue *lval;
    bool error = false;
    bool found = false;

    /* Left argument is always auto-unwrapped. */
    res = executeItemOptUnwrapResultNoThrow(cxt, pcxt->larg, jb, true, &lseq);
    if (JperIsError(res)) {
        return JPB_UNKNOWN;
    }

    if (pcxt->rarg) {
        /* Right argument is conditionally auto-unwrapped. */
        res = executeItemOptUnwrapResultNoThrow(cxt, pcxt->rarg, jb,
                                                pcxt->unwrapRightTarget, &rseq);
        if (JperIsError(res)) {
            return JPB_UNKNOWN;
        }
    }

    JsonValueListInitIterator(&lseq, &lseqit);
    while ((lval = JsonValueListNext(&lseq, &lseqit))) {
        JsonValueListIterator rseqit;
        JsonbValue *rval;
        bool first = true;

        JsonValueListInitIterator(&rseq, &rseqit);
        rval = pcxt->rarg ? JsonValueListNext(&rseq, &rseqit) : NULL;

        /* Loop over right arg sequence or do single pass otherwise */
        while (pcxt->rarg ? (rval != NULL) : first) {
            JsonPathBool res2 = (pcxt->exec)(pred, lval, rval, pcxt->param);
            if (res2 == JPB_UNKNOWN && JspStrictAbsenceOfErrors(cxt)) {
                return JPB_UNKNOWN;
            } else if (res2 == JPB_TRUE && !JspStrictAbsenceOfErrors(cxt)) {
                return JPB_TRUE;
            }
            found = res2 == JPB_TRUE ? true : found;
            error = res2 == JPB_UNKNOWN ? true : error;
            first = false;
            if (pcxt->rarg) {
                rval = JsonValueListNext(&rseq, &rseqit);
            }
        }
    }
    return found ? JPB_TRUE :
           error ? JPB_UNKNOWN : JPB_FALSE;
}

/* Comparison predicate callback. */
JsonPathBool executeComparison(
    JsonPathItem *cmp, JsonbValue *lv, JsonbValue *rv, void *p)
{
    JsonPathExecContext *cxt = (JsonPathExecContext *) p;
    return CompareItems(cmp->type, lv, rv);
}

/*
 * Execute LIKE_REGEX predicate.
 */
JsonPathBool executeLikeRegexExt(JsonPathExecContext* cxt, JsonPathItem* jsp, JsonbValue* jb)
{
    JsonLikeRegexContext lrcxt = {0};
    JsonPathExecPredicateContext pcxt;
    JsonPathItem larg;

    jspInitByBuffer(&larg, jsp->base, jsp->content.like_regex.expr);
    pcxt.larg = &larg;
    pcxt.rarg = NULL;
    pcxt.unwrapRightTarget = false;
    pcxt.exec = executeLikeRegex;
    pcxt.param = &lrcxt;

    return executePredicate(cxt, jsp, &pcxt, jb);
}

/*
 * LIKE_REGEX predicate callback.
 *
 * Check if the string matches regex pattern.
 */
JsonPathBool executeLikeRegex(JsonPathItem *jsp, JsonbValue *str, JsonbValue *rarg, void* param)
{
    JsonLikeRegexContext *cxt = (JsonLikeRegexContext*)param;

    if (!(str = getScalar(str, jbvString))) {
        return JPB_UNKNOWN;
    }

    /* Cache regex text and converted flags. */
    if (!cxt->regex) {
        cxt->regex =
            cstring_to_text_with_len(jsp->content.like_regex.pattern,
                                     jsp->content.like_regex.patternlen);
        (void) JspConvertRegexFlags(jsp->content.like_regex.flags, &(cxt->cflags));
    }

    if (RE_compile_and_execute_ext(cxt->regex, str->string.val,
                               str->string.len,
                               cxt->cflags, DEFAULT_COLLATION_OID, 0, NULL)) {
        return JPB_TRUE;
    }
    return JPB_FALSE;
}

/*
 * Execute EXISTS (expr) predicate.
 */
JsonPathBool executeExists(JsonPathExecContext* cxt, JsonPathItem* jsp, JsonbValue* jb)
{
    JsonPathItem larg;
    JspGetArg(jsp, &larg);

    if (JspStrictAbsenceOfErrors(cxt)) {
        /*
            * In strict mode we must get a complete list of values to
            * check that there are no errors at all.
            */
        JsonValueList vals = {0};
        JsonPathExecResult res = executeItemOptUnwrapResultNoThrow(cxt, &larg, jb, false, &vals);
        if (JperIsError(res)) {
            return JPB_UNKNOWN;
        }
        return JsonValueListIsEmpty(&vals) ? JPB_FALSE : JPB_TRUE;
    } else {
        JsonPathExecResult res = executeItemOptUnwrapResultNoThrow(cxt, &larg, jb, false, NULL);
        if (JperIsError(res)) {
            return JPB_UNKNOWN;
        }
        return res == JPER_OK ? JPB_TRUE : JPB_FALSE;
    }
}

/*
 * Execute '(predicate) IS UNKNOWN' expression.
 */
JsonPathBool executeIsUnknown(JsonPathExecContext* cxt, JsonPathItem* jsp, JsonbValue* jb)
{
    JsonPathItem larg;
    JsonPathBool res;

    JspGetArg(jsp, &larg);
    res = executeBoolItem(cxt, &larg, jb, false);
    return res == JPB_UNKNOWN ? JPB_TRUE : JPB_FALSE;
}

/*
 * STARTS_WITH predicate callback.
 *
 * Check if the 'whole' string starts from 'initial' string.
 */
JsonPathBool executeStartsWith(JsonPathItem *jsp, JsonbValue *whole, JsonbValue *initial, void *param)
{
    if (!(whole = getScalar(whole, jbvString)) ||
        !(initial = getScalar(initial, jbvString))) {
        return JPB_UNKNOWN;        /* error */
    }
    if (whole->string.len >= initial->string.len &&
        !memcmp(whole->string.val, initial->string.val, initial->string.len)) {
        return JPB_TRUE;
    }
    return JPB_FALSE;
}

/*
 * Execute numeric item methods (.abs(), .floor(), .ceil()) using the specified
 * user function 'func'.
 */
JsonPathExecResult executeNumericItemMethod(
    JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb, bool unwrap, JsonValueList *found)
{
    JsonPathItem next;
    Datum datum;

    if (unwrap && JsonbType(jb) == jbvArray) {
        return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
    }

    if (!(jb = getScalar(jb, jbvNumeric))) {
        RETURN_ERROR(ereport(ERROR,
                             (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                              errmsg("jsonpath item method .%s() can only be applied to a numeric value",
                                     JspOperationName(jsp->type)))));
    }

    PGFunction func = DecideActualFunction(jsp->type);
    datum = DirectFunctionCall1(func, NumericGetDatum(jb->numeric));

    if (!jspGetNext(jsp, &next) && !found) {
        return JPER_OK;
    }

    int scale = 2;
    jb = (JsonbValue*)palloc(sizeof(*jb));
    jb->type = jbvNumeric;
    jb->numeric = DatumGetNumeric(datum);
    jb->estSize = scale * sizeof(JEntry) + VARSIZE_ANY(jb->numeric);

    return executeNextItem(cxt, jsp, &next, jb, found, false);
}

PGFunction DecideActualFunction(JsonPathItemType type)
{
    switch (type) {
        case JPI_ABS:
            return numeric_abs;
        case JPI_FLOOR:
            return numeric_floor;
        case JPI_CEILING:
            return numeric_ceil;
        default:
            elog(ERROR, "unrecognized jsonpath numeric item method type: %d", type);
    }
}

/*
 * Implementation of JPI_TYPE (.type() item method)
 */
JsonPathExecResult executeTypeMethod(JsonPathExecContext *cxt,
    JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found)
{
    JsonbValue *jbv = (JsonbValue*)palloc(sizeof(JsonbValue));

    jbv->type = jbvString;
    jbv->string.val = pstrdup(JsonbTypeName(jb));
    jbv->string.len = strlen(jbv->string.val);
    jbv->estSize = sizeof(JEntry) + jbv->string.len;

    return executeNextItem(cxt, jsp, NULL, jbv, found, false);
}

/*
 * Implementation of JPI_SIZE (.size() item method)
 */
JsonPathExecResult executeSizeMethod(JsonPathExecContext *cxt,
    JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found)
{
    int size = JsonbArraySize(jb);
    if (size < 0) {
        if (!JspAutoWrap(cxt)) {
            if (!JspIgnoreStructuralErrors(cxt)) {
                RETURN_ERROR(ereport(ERROR,
                                        (errcode(ERRCODE_UNDEFINED_OBJECT),
                                        errmsg("jsonpath item method .%s() can only be applied to an array",
                                                JspOperationName(jsp->type)))));
            }
        }
        size = 1;
    }

    int scale = 2;
    jb = (JsonbValue*)palloc(sizeof(*jb));
    jb->type = jbvNumeric;
    jb->numeric = int64_to_numeric(size);
    jb->estSize = scale * sizeof(JEntry) + VARSIZE_ANY(jb->numeric);
    return executeNextItem(cxt, jsp, NULL, jb, found, false);
}

/*
 * Implementation of .keyvalue() method.
 *
 * .keyvalue() method returns a sequence of object's key-value pairs in the
 * following format: '{ "key": key, "value": value, "id": id }'.
 *
 * "id" field is an object identifier which is constructed from the two parts:
 * base object id and its binary offset in base object's jsonb:
 * id = 10000000000 * base_object_id + obj_offset_in_base_object
 *
 * 10000000000 (10^10) -- is a first round decimal number greater than 2^32
 * (maximal offset in jsonb).  Decimal multiplier is used here to improve the
 * readability of identifiers.
 *
 * Base object is usually a root object of the path: context item '$' or path
 * variable '$var', literals can't produce objects for now.  But if the path
 * contains generated objects (.keyvalue() itself, for example), then they
 * become base object for the subsequent .keyvalue().
 *
 * Id of '$' is 0. Id of '$var' is its ordinal (positive) number in the list
 * of variables (see getJsonPathVariable()).  Ids for generated objects
 * are assigned using global counter JsonPathExecContext.lastGeneratedObjectId.
 */
JsonPathExecResult executeKeyValueMethodExt(JsonPathExecContext *cxt,
    JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found, bool unwrap)
{
    if (unwrap && JsonbType(jb) == jbvArray) {
        return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
    }
    return executeKeyValueMethod(cxt, jsp, jb, found);
}

JsonPathExecResult executeKeyValueMethod(JsonPathExecContext *cxt,
    JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found)
{
    if (JsonbType(jb) != jbvObject || jb->type != jbvBinary) {
        RETURN_ERROR(ereport(ERROR,
                             (errcode(ERRCODE_UNDEFINED_OBJECT),
                              errmsg("jsonpath item method .%s() can only be applied to an object",
                                     JspOperationName(jsp->type)))));
    }
    if (!JsonbSuperHeaderSize(jb->binary.data)) {
        return JPER_NOT_FOUND;    /* no key-value pairs */
    }
    return constructKeyValueInfo(cxt, jsp, jb, found);
}

void constructObjectIdVal(JsonPathExecContext* cxt, JsonbValue* jb, JsonbValue* idval)
{
    /* construct object id from its base object and offset inside that */
    int64 id = jb->type != jbvBinary ? 0 :
        (int64) ((char *)(jb->binary.data) - (char *) cxt->baseObject.sheader);
    int64 offset = 10000000000;
    id += (int64) cxt->baseObject.id * INT64CONST(offset);

    idval->type = jbvNumeric;
    idval->numeric = int64_to_numeric(id);
    idval->estSize = sizeof(JEntry) + sizeof(JEntry) + VARSIZE_ANY(idval->numeric);
}

void constructKeyValueInfoTitle(JsonbValue* keystr, JsonbValue* valstr, JsonbValue* idstr)
{
    keystr->type = jbvString;
    keystr->string.val = "key";
    keystr->string.len = strlen("key");
    keystr->estSize = sizeof(JEntry) + keystr->string.len;

    valstr->type = jbvString;
    valstr->string.val = "value";
    valstr->string.len = strlen("value");
    valstr->estSize = sizeof(JEntry) + valstr->string.len;

    idstr->type = jbvString;
    idstr->string.val = "id";
    idstr->string.len = strlen("id");
    idstr->estSize = sizeof(JEntry) + idstr->string.len;
}

JsonPathExecResult constructKeyValueInfo(JsonPathExecContext *cxt,
    JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found)
{
    int tok;
    JsonbValue key;
    JsonbValue val;
    JsonPathItem next;
    JsonbValue keystr;
    JsonbValue valstr;
    JsonbValue idstr;
    JsonbValue idval;
    JsonPathExecResult res = JPER_NOT_FOUND;
    JsonbIterator *it = JsonbIteratorInit(jb->binary.data);
    bool hasNext = jspGetNext(jsp, &next);
    constructKeyValueInfoTitle(&keystr, &valstr, &idstr);
    constructObjectIdVal(cxt, jb, &idval);

    while ((tok = JsonbIteratorNext(&it, &key, true)) != WJB_DONE) {
        JsonbValue obj;
        JsonbParseState *ps = NULL;

        if (tok != WJB_KEY) {
            continue;
        }

        res = JPER_OK;

        if (!hasNext && !found) {
            break;
        }

        tok = JsonbIteratorNext(&it, &val, true);
        Assert(tok == WJB_VALUE);

        pushJsonbValue(&ps, WJB_BEGIN_OBJECT, NULL);

        pushJsonbValue(&ps, WJB_KEY, &keystr);
        pushJsonbValue(&ps, WJB_VALUE, &key);

        pushJsonbValue(&ps, WJB_KEY, &valstr);
        pushJsonbValue(&ps, WJB_VALUE, &val);

        pushJsonbValue(&ps, WJB_KEY, &idstr);
        pushJsonbValue(&ps, WJB_VALUE, &idval);

        JsonbValue *keyval = pushJsonbValue(&ps, WJB_END_OBJECT, NULL);
        JsonbInitBinary(&obj, JsonbValueToJsonb(keyval));
        JsonBaseObjectInfo baseObject = setBaseObject(cxt, &obj, cxt->lastGeneratedObjectId++);
        res = executeNextItem(cxt, jsp, &next, &obj, found, true);
        cxt->baseObject = baseObject;
        if (JperIsError(res) || (res == JPER_OK && !found)) {
            return res;
        }
    }
    return res;
}

/*
 * Implementation of JPI_STRING_FUNC (.string() item method)
 */
JsonPathExecResult executeStringMethod(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap)
{
    JsonbValue jbv;
    char *tmp = NULL;

    if (unwrap && JsonbType(jb) == jbvArray) {
        return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
    }

    switch (JsonbType(jb)) {
        case jbvString:
            /*
             * Value is not necessarily null-terminated, so we do
             * pnstrdup() here.
             */
            tmp = pnstrdup(jb->string.val, jb->string.len);
            break;
        case jbvNumeric:
            tmp = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(jb->numeric)));
            break;
        case jbvBool:
            tmp = (jb->boolean) ? pstrdup("true") : pstrdup("false");
            break;
        case jbvNull:
        case jbvArray:
        case jbvObject:
        case jbvBinary:
            RETURN_ERROR(ereport(ERROR,
                                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                    errmsg("jsonpath item method .%s() can only be applied to"
                                           " a boolean, string, numeric, or datetime value",
                                            JspOperationName(jsp->type)))));
    }

    jb = &jbv;
    Assert(tmp != NULL);    /* We must have set tmp above */
    jb->string.val = tmp;
    jb->string.len = strlen(jb->string.val);
    jb->type = jbvString;
    jb->estSize = sizeof(JEntry) + jb->string.len;
    return executeNextItem(cxt, jsp, NULL, jb, found, true);
}

/*
 * Implementation of:
 * - JPI_DECIMAL (.decimal( [p[,s]]) item method)
 * - JPI_NUMBER (.number() item method)
 */
JsonPathExecResult executedDecimalNumberMethod(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap)
{
    JsonbValue jbv;
    Numeric num;
    char *numstr = NULL;

    if (unwrap && JsonbType(jb) == jbvArray) {
        return executeItemUnwrapTargetArray(cxt, jsp, jb, found,
                                            false);
    }

    JsonPathExecResult res = getNumericFromJsonbValue(cxt, jsp, jb, &num, &numstr);
    if (JperIsError(res)) {
        return res;
    } else if (res == JPER_NOT_FOUND) {
        RETURN_ERROR(ereport(ERROR,
                                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("jsonpath item method .%s() can only be applied to a string or numeric value",
                                        JspOperationName(jsp->type)))));
    }
    /*
     * If we have arguments, then they must be the precision and
     * optional scale used in .decimal().  Convert them to the
     * typmod equivalent and then truncate the numeric value per
     * this typmod details.
     */
    if (jsp->type == JPI_DECIMAL && jsp->content.args.left) {
        res = getNumericWithTypmod(cxt, jsp, numstr, &num);
        if (JperIsError(res)) {
            return res;
        }
    }
    jb = &jbv;
    jb->type = jbvNumeric;
    jb->numeric = num;
    jb->estSize = sizeof(JEntry) + sizeof(JEntry) + VARSIZE_ANY(jb->numeric);

    return executeNextItem(cxt, jsp, NULL, jb, found, true);
}

int32 getNumericPrecisionScale(JsonPathExecContext* cxt, JsonPathItem* jsp, bool isPrec)
{
    bool haveError;
    JsonPathItem elem;

    if (isPrec) {
        JspGetLeftArg(jsp, &elem);
    } else {
        JspGetRightArg(jsp, &elem);
    }

    if (elem.type != JPI_NUMERIC) {
        elog(ERROR, "invalid jsonpath item type for .decimal() precision");
    }
    int32 result = numeric_int4_opt_error(jspGetNumeric(&elem), false, &haveError);
    if (haveError) {
        RETURN_ERROR(ereport(ERROR,
                                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("%s of jsonpath item method .%s() is out of range for type integer",
                                        isPrec ? "precision" : "scale", JspOperationName(jsp->type)))));
    }
    return result;
}

JsonPathExecResult checkNumericIsNaN(JsonPathExecContext* cxt, JsonPathItem* jsp, Numeric num)
{
    if (numeric_is_nan(num)) {
        RETURN_ERROR(ereport(ERROR,
                                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("NaN or Infinity is not allowed for jsonpath item method .%s()",
                                        JspOperationName(jsp->type)))));
    }
    return JPER_OK;
}

JsonPathExecResult getNumericWithTypmod(JsonPathExecContext* cxt,
    JsonPathItem* jsp, char* numstr, Numeric* num)
{
    Datum numdatum;
    int32 scale = 0;
    char pstr[12];    /* sign, 10 digits and '\0' */
    char sstr[12];    /* sign, 10 digits and '\0' */
    MemoryContext oldcxt = CurrentMemoryContext;
    JsonPathItem elem;

    int32 precision = getNumericPrecisionScale(cxt, jsp, true);
    if (jsp->content.args.right) {
        scale = getNumericPrecisionScale(cxt, jsp, false);
        if (scale > NUMERIC_MAX_SCALE || scale < NUMERIC_MIN_SCALE) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("NUMERIC scale must be between -84 and 1000")));
        }
    }

    /*
     * numerictypmodin() takes the precision and scale in the
     * form of CString arrays.
     */
    Datum datums[2];
    int cnt = 2;
    pg_ltoa(precision, pstr);
    datums[0] = CStringGetDatum(pstr);
    pg_ltoa(scale, sstr);
    datums[1] = CStringGetDatum(sstr);
    ArrayType* arrtypmod = construct_array_builtin(datums, cnt, CSTRINGOID);
    Datum dtypmod = DirectFunctionCall1(numerictypmodin, PointerGetDatum(arrtypmod));
    JsonPathExecResult res = JPER_OK;

    /* Convert numstr to Numeric with typmod */
    Assert(numstr != NULL);
    PG_TRY();
    {
        numdatum = DirectInputFunctionCall(NUMERICOID, numstr, dtypmod);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcxt);
        FlushErrorState();
        if (JspThrowErrors(cxt)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("argument \"%s\" of jsonpath item method .%s() is invalid for type %s",
                        numstr, JspOperationName(jsp->type), "numeric")));
        } else {
            res = JPER_ERROR;
        }
    }
    PG_END_TRY();

    *num = DatumGetNumeric(numdatum);
    pfree(arrtypmod);
    return res;
}

JsonPathExecResult getNumericFromJsonbValue(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, Numeric* num, char** numstr)
{
    JsonPathExecResult res = JPER_NOT_FOUND;
    if (jb->type == jbvNumeric) {
        *num = jb->numeric;
        res = checkNumericIsNaN(cxt, jsp, *num);
        if (jsp->type == JPI_DECIMAL && !JperIsError(res)) {
            *numstr = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(*num)));
        }
    } else if (jb->type == jbvString) {
        /* cast string as number */
        Datum datum;
        MemoryContext oldcxt = CurrentMemoryContext;

        *numstr = pnstrdup(jb->string.val, jb->string.len);

        PG_TRY();
        {
            datum = DirectInputFunctionCall(NUMERICOID, *numstr, -1);
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(oldcxt);
            FlushErrorState();
            if (JspThrowErrors(cxt)) {
                ereport(ERROR,
                         (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                          errmsg("argument \"%s\" of jsonpath item method .%s() is invalid for type %s",
                            *numstr, JspOperationName(jsp->type), "numeric")));
            } else {
                res = JPER_ERROR;
            }
        }
        PG_END_TRY();
        if (!JperIsError(res)) {
            *num = DatumGetNumeric(datum);
            res = checkNumericIsNaN(cxt, jsp, *num);
        }
    }
    return res;
}

/*
 * Implementation of JPI_DOUBLE (.double() item method)
 */
JsonPathExecResult executeDoubleMethod(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap)
{
    JsonPathExecResult res = JPER_NOT_FOUND;
    if (unwrap && JsonbType(jb) == jbvArray) {
        return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
    }
    if (jb->type == jbvNumeric) {
        res = executeNumericToDouble(cxt, jsp, jb);
    } else if (jb->type == jbvString) {
        res = executeStringToDouble(cxt, jsp, &jb);
    }
    if (res != JPER_OK) {
        RETURN_ERROR(ereport(ERROR,
                             (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                              errmsg("jsonpath item method .%s() can only be applied to a string or numeric value",
                                    JspOperationName(jsp->type)))));
    }
    return executeNextItem(cxt, jsp, NULL, jb, found, true);
}

/*
 * Implementation of JPI_DOUBLE (.double() item method)
 * converts jbvNumeric to double
 */
JsonPathExecResult executeNumericToDouble(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb)
{
    double val;
    bool hasError = false;
    MemoryContext oldcxt = CurrentMemoryContext;
    char *tmp = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum((jb)->numeric)));
    JsonPathExecResult res = JPER_OK;

    PG_TRY();
    {
        val = float8in_internal(tmp, NULL, &hasError);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcxt);
        FlushErrorState();
        if (JspThrowErrors(cxt)) {
            ereport(ERROR,
                     (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("argument \"%s\" of jsonpath item method .%s() is invalid for type %s",
                            tmp, JspOperationName(jsp->type), "double precision")));
        } else {
            res = JPER_ERROR;
        }
    }
    PG_END_TRY();

    if (hasError) {
        RETURN_ERROR(
            ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("argument \"%s\" of jsonpath item method .%s() is invalid for type double precision",
                                tmp, JspOperationName(jsp->type)))));
    }
    if (isinf(val) || isnan(val)) {
        RETURN_ERROR(ereport(ERROR,
                                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("NaN or Infinity is not allowed for jsonpath item method .%s()",
                                        JspOperationName(jsp->type)))));
    }
    return res;
}

/*
 * Implementation of JPI_DOUBLE (.double() item method)
 * converts jbvString to double
 */
JsonPathExecResult executeStringToDouble(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue** jb)
{
    /* cast string as double */
    double val;
    JsonbValue jbv;
    bool hasError = false;
    MemoryContext oldcxt = CurrentMemoryContext;
    char *tmp = pnstrdup((*jb)->string.val, (*jb)->string.len);
    JsonPathExecResult res = JPER_OK;

    PG_TRY();
    {
        val = float8in_internal(tmp, NULL, &hasError);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcxt);
        FlushErrorState();
        if (JspThrowErrors(cxt)) {
            ereport(ERROR,
                     (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("argument \"%s\" of jsonpath item method .%s() is invalid for type %s",
                        tmp, JspOperationName(jsp->type), "double precision")));
        } else {
            res = JPER_ERROR;
        }
    }
    PG_END_TRY();
    if (JperIsError(res)) {
        return res;
    }

    if (hasError) {
        RETURN_ERROR(
            ereport(ERROR,
                     (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("argument \"%s\" of jsonpath item method .%s() is invalid for type double precision",
                            tmp, JspOperationName(jsp->type)))));
    }
    if (isinf(val) || isnan(val)) {
        RETURN_ERROR(ereport(ERROR,
                                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("NaN or Infinity is not allowed for jsonpath item method .%s()",
                                        JspOperationName(jsp->type)))));
    }

    (*jb)->type = jbvNumeric;
    (*jb)->numeric = DatumGetNumeric(DirectFunctionCall1(float8_numeric, Float8GetDatum(val)));
    (*jb)->estSize = sizeof(JEntry) + sizeof(JEntry) + VARSIZE_ANY((*jb)->numeric);
    return JPER_OK;
}

/*
 * Implementation of JPI_BOOLEAN (.boolean() item method)
 */
JsonPathExecResult executeBooleanMethod(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap)
{
    JsonbValue jbv;
    bool bval;
    JsonPathExecResult res = JPER_NOT_FOUND;

    if (unwrap && JsonbType(jb) == jbvArray) {
        return executeItemUnwrapTargetArray(cxt, jsp, jb, found,
                                            false);
    }

    if (jb->type == jbvBool) {
        bval = jb->boolean;
        res = JPER_OK;
    } else if (jb->type == jbvNumeric) {
        res = executeNumericToBool(cxt, jsp, jb, &bval);
    } else if (jb->type == jbvString) {
        res = executeStringToBool(cxt, jsp, jb, &bval);
    }

    if (res != JPER_OK) {
        RETURN_ERROR(
            ereport(ERROR,
                     (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("jsonpath item method .%s() can only be applied to a boolean, string, or numeric value",
                        JspOperationName(jsp->type)))));
    }

    jb = &jbv;
    jb->type = jbvBool;
    jb->boolean = bval;
    jb->estSize = sizeof(JEntry);

    return executeNextItem(cxt, jsp, NULL, jb, found, true);
}

/*
 * Implementation of JPI_BOOLEAN (.boolean() item method)
 * converts jbvNumeric to bool
 */
JsonPathExecResult executeNumericToBool(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, bool* bval)
{
    int ival;
    Datum datum;
    char *tmp =
        DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(jb->numeric)));
    MemoryContext oldcxt = CurrentMemoryContext;
    JsonPathExecResult res = JPER_OK;

    PG_TRY();
    {
        datum = DirectInputFunctionCall(INT4OID, tmp, -1);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcxt);
        FlushErrorState();
        if (JspThrowErrors(cxt)) {
            ereport(ERROR,
                     (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("argument \"%s\" of jsonpath item method .%s() is invalid for type %s",
                            tmp, JspOperationName(jsp->type), "boolean")));
        } else {
            res = JPER_ERROR;
        }
    }
    PG_END_TRY();

    if (JperIsError(res)) {
        return res;
    }

    ival = DatumGetInt32(datum);
    if (ival == 0) {
        *bval = false;
    } else {
        *bval = true;
    }

    return JPER_OK;
}

/*
 * Implementation of JPI_BOOLEAN (.boolean() item method)
 * converts jbvString to bool
 */
JsonPathExecResult executeStringToBool(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, bool* bval)
{
    /* cast string as boolean */
    char *tmp = pnstrdup(jb->string.val, jb->string.len);
    if (!parse_bool(tmp, bval)) {
        RETURN_ERROR(ereport(ERROR,
                             (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                              errmsg("argument \"%s\" of jsonpath item method .%s() is invalid for type boolean",
                                     tmp, JspOperationName(jsp->type)))));
    }
    return JPER_OK;
}

/* Implementation of JPI_BIGINT (.bigint() item method) */
JsonPathExecResult executeBigintMethod(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap)
{
    JsonbValue jbv;
    Datum datum;
    JsonPathExecResult res = JPER_NOT_FOUND;

    if (unwrap && JsonbType(jb) == jbvArray) {
        return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
    }
    if (jb->type == jbvNumeric) {
        res = executeNumericToBigint(cxt, jsp, jb, &datum);
    } else if (jb->type == jbvString) {
        res = executeStringToBigint(cxt, jsp, jb, &datum);
    }
    if (res != JPER_OK) {
        RETURN_ERROR(ereport(ERROR,
                             (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                              errmsg("jsonpath item method .%s() can only be applied to a string or numeric value",
                                    JspOperationName(jsp->type)))));
    }
    jb = &jbv;
    jb->type = jbvNumeric;
    jb->numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric, datum));
    jb->estSize = sizeof(JEntry) + sizeof(JEntry) + VARSIZE_ANY(jb->numeric);

    return executeNextItem(cxt, jsp, NULL, jb, found, true);
}

/*
 * Implementation of JPI_BIGINT (.bigint() item method)
 * converts jbvNumeric to int64
 */
JsonPathExecResult executeNumericToBigint(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, Datum* datum)
{
    bool haveError;
    int64 val;

    val = numeric_int8_opt_error(jb->numeric, false, &haveError);
    if (haveError) {
        RETURN_ERROR(ereport(ERROR,
                             (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                              errmsg("argument \"%s\" of jsonpath item method .%s() is invalid for type bigint",
                                    DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(jb->numeric))),
                                    JspOperationName(jsp->type)))));
    }

    *datum = Int64GetDatum(val);
    return JPER_OK;
}

/*
 * Implementation of JPI_BIGINT (.bigint() item method)
 * converts jbvString to int64
 */
JsonPathExecResult executeStringToBigint(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, Datum* datum)
{
    /* cast string as bigint */
    char *tmp = pnstrdup(jb->string.val, jb->string.len);
    MemoryContext oldcxt = CurrentMemoryContext;
    JsonPathExecResult res = JPER_OK;
    PG_TRY();
    {
        *datum = DirectInputFunctionCall(INT8OID, tmp, -1);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcxt);
        FlushErrorState();
        if (JspThrowErrors(cxt)) {
            ereport(ERROR,
                     (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("argument \"%s\" of jsonpath item method .%s() is invalid for type %s",
                        tmp, JspOperationName(jsp->type), "bigint")));
        } else {
            res = JPER_ERROR;
        }
    }
    PG_END_TRY();
    return res;
}

/*
 * Implementation of JPI_INTEGER (.integer() item method)
 */
JsonPathExecResult executeIntegerMethod(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap)
{
    JsonbValue jbv;
    Datum datum;
    JsonPathExecResult res = JPER_NOT_FOUND;

    if (unwrap && JsonbType(jb) == jbvArray) {
        return executeItemUnwrapTargetArray(cxt, jsp, jb, found, false);
    }
    if (jb->type == jbvNumeric) {
        res = executeNumericToInteger(cxt, jsp, jb, &datum);
    } else if (jb->type == jbvString) {
        res = executeStringToInteger(cxt, jsp, jb, &datum);
    }
    if (res != JPER_OK) {
        RETURN_ERROR(ereport(ERROR,
                             (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                              errmsg("jsonpath item method .%s() can only be applied to a string or numeric value",
                                    JspOperationName(jsp->type)))));
    }

    jb = &jbv;
    jb->type = jbvNumeric;
    jb->numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric, datum));
    jb->estSize = sizeof(JEntry) + sizeof(JEntry) + VARSIZE_ANY(jb->numeric);

    return executeNextItem(cxt, jsp, NULL, jb, found, true);
}

/*
 * Implementation of JPI_INTEGER (.integer() item method)
 * Convert jbvNumeric to int32.
 */
JsonPathExecResult executeNumericToInteger(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, Datum* datum)
{
    bool haveError;
    int32 val;

    val = numeric_int4_opt_error(jb->numeric, false, &haveError);
    if (haveError) {
        RETURN_ERROR(ereport(ERROR,
                             (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                              errmsg("argument \"%s\" of jsonpath item method .%s() is invalid for type integer",
                                    DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(jb->numeric))),
                                    JspOperationName(jsp->type)))));
    }

    *datum = Int32GetDatum(val);
    return JPER_OK;
}

/*
 * Implementation of JPI_INTEGER (.integer() item method)
 * Convert jbvString to double.
 */
JsonPathExecResult executeStringToInteger(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, Datum* datum)
{
    /* cast string as integer */
    char *tmp = pnstrdup(jb->string.val, jb->string.len);
    MemoryContext oldcxt = CurrentMemoryContext;
    JsonPathExecResult res = JPER_OK;

    PG_TRY();
    {
        *datum = DirectInputFunctionCall(INT4OID, tmp, -1);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcxt);
        FlushErrorState();
        if (JspThrowErrors(cxt)) {
            ereport(ERROR,
                     (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("argument \"%s\" of jsonpath item method .%s() is invalid for type %s",
                        tmp, JspOperationName(jsp->type), "integer")));
        } else {
            res = JPER_ERROR;
        }
    }
    PG_END_TRY();
    return res;
}

/*
 * Compare two SQL/JSON items using comparison operation 'op'.
 */
JsonPathBool CompareItems(int32 op, JsonbValue *jb1, JsonbValue *jb2)
{
    bool res;

    if (jb1->type != jb2->type) {
        if (jb1->type == jbvNull || jb2->type == jbvNull) {
            /*
             * Equality and order comparison of nulls to non-nulls returns
             * always false, but inequality comparison returns true.
             */
            return op == JPI_NOT_EQUAL ? JPB_TRUE : JPB_FALSE;
        }
        /* Non-null items of different types are not comparable. */
        return JPB_UNKNOWN;
    }

    int cmp = CompareJsonbValue(op, jb1, jb2);
    switch (op) {
        case JPI_EQUAL:
            res = (cmp == 0);
            break;
        case JPI_NOT_EQUAL:
            res = (cmp != 0);
            break;
        case JPI_LESS:
            res = (cmp < 0);
            break;
        case JPI_GREATER:
            res = (cmp > 0);
            break;
        case JPI_LESS_OR_EQUAL:
            res = (cmp <= 0);
            break;
        case JPI_GREATER_OR_EQUAL:
            res = (cmp >= 0);
            break;
        default:
            elog(ERROR, "unrecognized jsonpath operation: %d", op);
            return JPB_UNKNOWN;
    }
    return res ? JPB_TRUE : JPB_FALSE;
}

int CompareJsonbValue(int32 op, JsonbValue* jb1, JsonbValue* jb2)
{
    int cmp;
    switch (jb1->type) {
        case jbvNull:
            cmp = 0;
            break;
        case jbvBool:
            cmp = jb1->boolean == jb2->boolean ? 0 :
                jb1->boolean ? 1 : -1;
            break;
        case jbvNumeric:
            cmp = CompareNumeric(jb1->numeric, jb2->numeric);
            break;
        case jbvString:
            if (op == JPI_EQUAL) {
                return jb1->string.len != jb2->string.len ||
                    memcmp(jb1->string.val,
                           jb2->string.val,
                           jb1->string.len) ? 1 : 0;
            }

            cmp = CompareStrings(jb1->string.val, jb1->string.len,
                                 jb2->string.val, jb2->string.len);
            break;

        case jbvBinary:
        case jbvArray:
        case jbvObject:
            return JPB_UNKNOWN;    /* non-scalars are not comparable */

        default:
            elog(ERROR, "invalid jsonb value type %d", jb1->type);
    }
    return cmp;
}

/* Compare two numerics */
int CompareNumeric(Numeric a, Numeric b)
{
    return DatumGetInt32(
        DirectFunctionCall2(numeric_cmp, NumericGetDatum(a), NumericGetDatum(b)));
}

/*
 * Perform per-byte comparison of two strings.
 */
int BinaryCompareStrings(const char *s1, int len1,
                     const char *s2, int len2)
{
    int cmp;

    cmp = memcmp(s1, s2, Min(len1, len2));
    if (cmp != 0) {
        return cmp;
    }
    if (len1 == len2) {
        return 0;
    }
    return len1 < len2 ? -1 : 1;
}

/*
 * Compare two strings in the current server encoding using Unicode codepoint
 * collation.
 */
int CompareStrings(const char *mbstr1, int mblen1,
    const char *mbstr2, int mblen2)
{
    if (GetDatabaseEncoding() == PG_SQL_ASCII ||
        GetDatabaseEncoding() == PG_UTF8) {
        /*
         * It's known property of UTF-8 strings that their per-byte comparison
         * result matches codepoints comparison result.  ASCII can be
         * considered as special case of UTF-8.
         */
        return BinaryCompareStrings(mbstr1, mblen1, mbstr2, mblen2);
    } else {
        char *utf8str1;
        char *utf8str2;
        int cmp;
        int utf8len1;
        int utf8len2;

        /*
         * We have to convert other encodings to UTF-8 first, then compare.
         * Input strings may be not null-terminated and pg_server_to_any() may
         * return them "as is".  So, use strlen() only if there is real
         * conversion.
         */
        utf8str1 = pg_server_to_any(mbstr1, mblen1, PG_UTF8);
        utf8str2 = pg_server_to_any(mbstr2, mblen2, PG_UTF8);
        utf8len1 = (mbstr1 == utf8str1) ? mblen1 : strlen(utf8str1);
        utf8len2 = (mbstr2 == utf8str2) ? mblen2 : strlen(utf8str2);

        cmp = BinaryCompareStrings(utf8str1, utf8len1, utf8str2, utf8len2);

        /*
         * If pg_server_to_any() did no real conversion, then we actually
         * compared original strings.  So, we already done.
         */
        if (mbstr1 == utf8str1 && mbstr2 == utf8str2) {
            return cmp;
        }

        /* Free memory if needed */
        if (mbstr1 != utf8str1) {
            pfree(utf8str1);
        }
        if (mbstr2 != utf8str2) {
            pfree(utf8str2);
        }

        /*
         * When all Unicode codepoints are equal, return result of binary
         * comparison.  In some edge cases, same characters may have different
         * representations in encoding.  Then our behavior could diverge from
         * standard.  However, that allow us to do simple binary comparison
         * for "==" operator, which is performance critical in typical cases.
         * In future to implement strict standard conformance, we can do
         * normalization of input JSON strings.
         */
        if (cmp == 0) {
            return BinaryCompareStrings(mbstr1, mblen1, mbstr2, mblen2);
        } else {
            return cmp;
        }
    }
}

/*
 * Convert boolean execution status 'res' to a boolean JSON item and execute
 * next jsonpath.
 */
JsonPathExecResult appendBoolResult(
    JsonPathExecContext *cxt, JsonPathItem *jsp,
    JsonValueList *found, JsonPathBool res)
{
    JsonPathItem next;
    JsonbValue jbv;

    if (!jspGetNext(jsp, &next) && !found) {
        return JPER_OK;            /* found singleton boolean value */
    }

    if (res == JPB_UNKNOWN) {
        jbv.type = jbvNull;
        jbv.estSize = sizeof(JEntry);
    } else {
        jbv.type = jbvBool;
        jbv.boolean = res == JPB_TRUE;
        jbv.estSize = sizeof(JEntry);
    }

    return executeNextItem(cxt, jsp, &next, &jbv, found, true);
}

/* Save base object and its id needed for the execution of .keyvalue(). */
JsonBaseObjectInfo setBaseObject(JsonPathExecContext *cxt, JsonbValue *jbv, int32 id)
{
    JsonBaseObjectInfo baseObject = cxt->baseObject;

    cxt->baseObject.sheader = jbv->type != jbvBinary ? NULL :
        (JsonbSuperHeader) jbv->binary.data;
    cxt->baseObject.id = id;

    return baseObject;
}

/*
 * Definition of JsonPathGetVarCallback for when JsonPathExecContext.vars
 * is specified as a jsonb value.
 */
JsonbValue *getJsonPathVariableFromJsonb(
    Jsonb* vars, char *varName, int varNameLength, JsonbValue *baseObject, int *baseObjectId)
{
    JsonbValue tmp;
    JsonbValue *result;

    tmp.type = jbvString;
    tmp.string.val = varName;
    tmp.string.len = varNameLength;
    result = findJsonbValueFromSuperHeader((JsonbSuperHeader)(&vars->superheader), JB_FOBJECT, NULL, &tmp);
    if (result == NULL) {
        *baseObjectId = -1;
        return NULL;
    }
    *baseObjectId = 1;
    JsonbInitBinary(baseObject, vars);

    return result;
}

/*
 * Get the value of variable passed to jsonpath executor
 */
void getJsonPathVariable(
    JsonPathExecContext *cxt, JsonPathItem *variable, JsonbValue *value)
{
    char *varName;
    int varNameLength;
    JsonbValue baseObject;
    int baseObjectId;
    JsonbValue *v;

    Assert(variable->type == JPI_VARIABLE);
    varName = jspGetString(variable, &varNameLength);
    if (cxt->vars == NULL ||
        (v = cxt->getVar(cxt->vars, varName, varNameLength, &baseObject, &baseObjectId)) == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("could not find jsonpath variable \"%s\"",
                        pnstrdup(varName, varNameLength))));
    }
    if (baseObjectId > 0) {
        *value = *v;
        setBaseObject(cxt, &baseObject, baseObjectId);
    }
}

/*
 * Definition of JsonPathCountVarsCallback for when JsonPathExecContext.vars
 * is specified as a jsonb value.
 */
int countVariablesFromJsonb(Jsonb* vars)
{
    if (vars && !JsonbSuperHeaderIsObject((JsonbSuperHeader)(&vars->superheader))) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("\"vars\" argument is not an object"),
                errdetail("Jsonpath parameters should be encoded as key-value pairs of \"vars\" object.")));
    }

    /* count of base objects */
    return vars != NULL ? 1 : 0;
}