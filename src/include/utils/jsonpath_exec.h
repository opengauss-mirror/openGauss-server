/*-------------------------------------------------------------------------
 *
 * jsonpath_exec.h
 *    Definitions for jsonpath execution
 *
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 *
 * IDENTIFICATION
 *    src/include/utils/jsonpath_exec.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef JSONPATH_EXEC_H
#define JSONPATH_EXEC_H

#include "fmgr.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "utils/jsonb.h"
#include "utils/jsonpath.h"

/* Result of jsonpath expression evaluation */
typedef enum JsonPathExecResult {
    JPER_OK = 0,
    JPER_NOT_FOUND = 1,
    JPER_ERROR = 2
} JsonPathExecResult;

/*
 * Represents "base object" and it's "id" for .keyvalue() evaluation.
 */
typedef struct JsonBaseObjectInfo {
    JsonbSuperHeader sheader;
    int id;
} JsonBaseObjectInfo;

/* Callbacks for executeJsonPath() */
typedef JsonbValue *(*JsonPathGetVarCallback) (Jsonb* vars, char *varName, int varNameLen,
    JsonbValue *baseObject, int *baseObjectId);
typedef Numeric (*BinaryArithmFunc) (Numeric num1, Numeric num2, bool *error);

/*
 * Context of jsonpath execution.
 */
typedef struct JsonPathExecContext {
    JsonbValue *root;            /* for $ evaluation */
    JsonbValue *current;            /* for $ evaluation */
    bool        laxMode;        /* true for "lax" mode, false for "strict"
                                 * mode */
    bool        ignoreStructuralErrors; /* with "true" structural errors such
                                         * as absence of required json item or
                                         * unexpected json item type are
                                         * ignored */
    int         timesLaxed;     /* for A compatibility */
    bool        unlimitedKeyLax;        /* for json_textcontains' A compatibility,
                                         * which can tolerate array accessor applied on
                                         * an object, without limits on the numer of times */
    bool        throwErrors;    /* with "false" all suppressible errors are
                                 * suppressed */
    Jsonb* vars;            /* variables to substitute into jsonpath */
    JsonPathGetVarCallback getVar;    /* callback to extract a given variable
                                     * from 'vars' */
    int innermostArraySize; /* for LAST array index evaluation */
    JsonBaseObjectInfo baseObject; /* "base object" for .keyvalue() * evaluation */
    int lastGeneratedObjectId; /* "id" counter for .keyvalue() * evaluation */
} JsonPathExecContext;

typedef struct JsonPathExecAnyItemContext {
    JsonPathExecContext* cxt;
    uint32 level;
    uint32 first;
    uint32 last;
    bool ignoreStructuralErrors;
    bool unwrapNext;
} JsonPathExecAnyItemContext;

/* Context for LIKE_REGEX execution. */
typedef struct JsonLikeRegexContext {
    text *regex;
    int cflags;
} JsonLikeRegexContext;

/* Result of jsonpath predicate evaluation */
typedef enum JsonPathBool {
    JPB_FALSE = 0,
    JPB_TRUE = 1,
    JPB_UNKNOWN = 2
} JsonPathBool;

typedef JsonPathBool (*JsonPathPredicateCallback)
    (JsonPathItem *jsp, JsonbValue *larg, JsonbValue *rarg, void *param);

typedef struct JsonPathExecPredicateContext {
    JsonPathItem* larg;
    JsonPathItem* rarg;
    bool unwrapRightTarget;
    JsonPathPredicateCallback exec;
    void *param;
} JsonPathExecPredicateContext;

typedef enum {
    FALSE_ON_ERROR = 0, /* default */
    TRUE_ON_ERROR,
    ERROR_ON_ERROR
} OnErrorType;

/*
 * List of jsonb values with shortcut for single-value list.
 */
typedef struct JsonValueList {
    JsonbValue *singleton;
    List       *list;
} JsonValueList;

typedef struct JsonValueListIterator {
    JsonbValue *value;
    List       *list;
    ListCell   *next;
} JsonValueListIterator;


JsonPathExecResult executeJsonPath(
    JsonPath *path, JsonPathExecContext* cxt, Jsonb *json, JsonValueList *result);
JsonPathExecResult executeItem(JsonPathExecContext *cxt,
                                      JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found);
JsonPathExecResult executeItemOptUnwrapTarget(JsonPathExecContext *cxt,
                                                     JsonPathItem *jsp, JsonbValue *jb,
                                                     JsonValueList *found, bool unwrap);
JsonPathExecResult executeScarlarAndVariable(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found);
JsonPathExecResult executeRoot(JsonPathExecContext* cxt, JsonPathItem* jsp,
    JsonbValue* jb, JsonValueList* found);
JsonPathExecResult executeAnyArray(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found);
JsonPathExecResult executeAnyKey(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap);
JsonPathExecResult executeIndexArray(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found);
JsonPathExecResult traverseIndexArrayItem(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found);
JsonPathExecResult processArrayIndexFromTo(JsonPathExecContext* cxt,
    JsonbValue* jb, JsonPathItem** fromto, int32** indexes, bool range);
JsonPathExecResult executeAny(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found);
JsonPathExecResult executeKey(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap);
JsonPathExecResult executeLast(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found);
JsonPathExecResult executeItemUnwrapTargetArray(JsonPathExecContext *cxt,
                                                       JsonPathItem *jsp, JsonbValue *jb,
                                                       JsonValueList *found, bool unwrapElements);
JsonPathExecResult executeNextItem(JsonPathExecContext *cxt,
                                          JsonPathItem *cur, JsonPathItem *next,
                                          JsonbValue *v, JsonValueList *found, bool copy);
JsonPathExecResult executeItemOptUnwrapResult(
    JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb, bool unwrap, JsonValueList *found);
JsonPathExecResult executeItemOptUnwrapResultNoThrow(JsonPathExecContext *cxt, JsonPathItem *jsp,
                                                            JsonbValue *jb, bool unwrap, JsonValueList *found);
JsonPathExecResult executeFilter(JsonPathExecContext* cxt, JsonPathItem* jsp,
    JsonbValue* jb, JsonValueList* found, bool unwrap);
JsonPathBool executeNestedBoolItem(JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb);
JsonPathBool executeBoolItem(JsonPathExecContext *cxt,
                                    JsonPathItem *jsp, JsonbValue *jb, bool canHaveNext);
JsonPathBool executeLogicalOperator(JsonPathExecContext* cxt, JsonPathItem* jsp, JsonbValue* jb);
JsonPathBool executePredicateExt(JsonPathExecContext* cxt, JsonPathItem* jsp, JsonbValue* jb);
JsonPathBool executePredicate(JsonPathExecContext *cxt, JsonPathItem* pred,
    JsonPathExecPredicateContext* pcxt, JsonbValue* jb);
JsonPathBool executeComparison(JsonPathItem *cmp, JsonbValue *lv, JsonbValue *rv, void *p);
JsonPathBool CompareItems(int32 op, JsonbValue *jb1, JsonbValue *jb2);
int CompareJsonbValue(int32 op, JsonbValue* jb1, JsonbValue* jb2);
int CompareNumeric(Numeric a, Numeric b);
int CompareStrings(const char *mbstr1, int mblen1, const char *mbstr2, int mblen2);
JsonPathBool executeStartsWith(JsonPathItem *jsp, JsonbValue *whole, JsonbValue *initial, void *param);
JsonPathBool executeLikeRegexExt(JsonPathExecContext* cxt, JsonPathItem* jsp, JsonbValue* jb);
JsonPathBool executeLikeRegex(JsonPathItem *jsp, JsonbValue *str, JsonbValue *rarg, void* param);
JsonPathBool executeExists(JsonPathExecContext* cxt, JsonPathItem* jsp, JsonbValue* jb);
JsonPathBool executeIsUnknown(JsonPathExecContext* cxt, JsonPathItem* jsp, JsonbValue* jb);
JsonPathExecResult appendBoolResult(JsonPathExecContext *cxt,
                                           JsonPathItem *jsp, JsonValueList *found, JsonPathBool res);
JsonPathExecResult executeAnyItem(JsonPathExecAnyItemContext *eaicxt, JsonPathItem *jsp,
                                         JsonbSuperHeader sheader, JsonValueList *found);
JsonPathExecResult executeAnyNext(JsonPathExecAnyItemContext *eaicxt, JsonPathItem* jsp,
                                         JsonbValue v, JsonValueList *found);
JsonPathExecResult executeUnaryArithmExpr(
    JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb, PGFunction func,
    JsonValueList *found);
JsonPathExecResult executeBinaryArithmExpr(
    JsonPathExecContext *cxt, JsonPathItem *jsp, JsonbValue *jb,
    BinaryArithmFunc func, JsonValueList *found);
JsonPathExecResult executeNumericItemMethod(JsonPathExecContext *cxt, JsonPathItem *jsp,
                         JsonbValue *jb, bool unwrap, JsonValueList *found);
PGFunction DecideActualFunction(JsonPathItemType type);
JsonPathExecResult executeTypeMethod(JsonPathExecContext *cxt,
    JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found);
JsonPathExecResult executeSizeMethod(JsonPathExecContext *cxt,
    JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found);
JsonPathExecResult executeKeyValueMethodExt(JsonPathExecContext *cxt,
    JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found, bool unwrap);
JsonPathExecResult executeKeyValueMethod(JsonPathExecContext *cxt,
    JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found);
JsonPathExecResult constructKeyValueInfo(JsonPathExecContext *cxt,
    JsonPathItem *jsp, JsonbValue *jb, JsonValueList *found);
void constructKeyValueInfoTitle(JsonbValue* keystr, JsonbValue* valstr, JsonbValue* idstr);
void constructObjectIdVal(JsonPathExecContext* cxt, JsonbValue* jb, JsonbValue* idval);
JsonPathExecResult executeStringMethod(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap);
JsonPathExecResult executedDecimalNumberMethod(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap);
JsonPathExecResult getNumericFromJsonbValue(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, Numeric* num, char** numstr);
JsonPathExecResult getNumericWithTypmod(JsonPathExecContext* cxt,
    JsonPathItem* jsp, char* numstr, Numeric* num);
JsonPathExecResult checkNumericIsNaN(JsonPathExecContext* cxt, JsonPathItem* jsp, Numeric num);
int32 getNumericPrecisionScale(JsonPathExecContext* cxt, JsonPathItem* jsp, bool isPrec);
JsonPathExecResult executeDoubleMethod(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap);
JsonPathExecResult executeNumericToDouble(JsonPathExecContext* cxt, JsonPathItem* jsp, JsonbValue* jb);
JsonPathExecResult executeStringToDouble(JsonPathExecContext* cxt, JsonPathItem* jsp, JsonbValue** jb);
JsonPathExecResult executeBooleanMethod(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap);
JsonPathExecResult executeNumericToBool(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, bool* bval);
JsonPathExecResult executeStringToBool(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, bool* bval);
JsonPathExecResult executeBigintMethod(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap);
JsonPathExecResult executeNumericToBigint(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, Datum* datum);
JsonPathExecResult executeStringToBigint(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, Datum* datum);
JsonPathExecResult executeIntegerMethod(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, JsonValueList* found, bool unwrap);
JsonPathExecResult executeNumericToInteger(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, Datum* datum);
JsonPathExecResult executeStringToInteger(JsonPathExecContext* cxt,
    JsonPathItem* jsp, JsonbValue* jb, Datum* datum);
void getJsonPathItem(JsonPathExecContext *cxt, JsonPathItem *item, JsonbValue *value);
void getJsonPathVariable(JsonPathExecContext *cxt,
                                JsonPathItem *variable, JsonbValue *value);
int countVariablesFromJsonb(Jsonb* vars);
void initJsonPathExecContext(JsonPath* jp, JsonPathExecContext* cxt);
int    JsonbArraySize(JsonbValue *jb);
JsonbValue *copyJsonbValue(JsonbValue *src);
JsonPathExecResult getArrayIndex(JsonPathExecContext *cxt,
                                        JsonPathItem *jsp, JsonbValue *jb, int32 *index);
JsonBaseObjectInfo setBaseObject(JsonPathExecContext *cxt, JsonbValue *jbv, int32 id);
void JsonValueListAppend(JsonValueList *jvl, JsonbValue *jbv);
int JsonValueListLength(const JsonValueList *jvl);
bool JsonValueListIsEmpty(JsonValueList *jvl);
JsonbValue *JsonValueListHead(JsonValueList *jvl);
void JsonValueListInitIterator(const JsonValueList *jvl, JsonValueListIterator *it);
JsonbValue *JsonValueListNext(const JsonValueList *jvl, JsonValueListIterator *it);
JsonbValue *JsonbInitBinary(JsonbValue *jbv, Jsonb *jb);
int JsonbType(JsonbValue *jb);
JsonbValue *getScalar(JsonbValue *scalar, enum jbvType type);

Datum DirectInputFunctionCall(Oid typoid, char* str, int32 typmod);
JsonbValue *getJsonPathVariableFromJsonb(
    Jsonb* vars, char *varName, int varNameLength, JsonbValue *baseObject, int *baseObjectId);

#endif