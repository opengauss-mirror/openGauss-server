/* ---------------------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996, 2003 VIA Networking Technologies, Inc.
 *
 *  dfs_query.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/dfs_query.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "access/dfs/dfs_insert.h"
#include "access/dfs/dfs_query.h"
#include "access/dfs/dfs_query_check.h"
#include "access/dfs/dfs_query_reader.h"
#include "access/dfs/dfs_wrapper.h"
#include "access/sysattr.h"
#include "catalog/pg_am.h"
#include "catalog/pg_operator.h"
#include "catalog/indexing.h"
#include "catalog/catalog.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pgxc_node.h"
#include "catalog/dfsstore_ctlg.h"
#include "commands/defrem.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/var.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"
#include "optimizer/predtest.h"
#include "parser/parse_type.h"
#include "utils/bloom_filter.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "access/heapam.h"
#include "utils/int8.h"
#include "executor/executor.h"

static Oid GetOperatorByTypeId(Oid typeID, Oid accessMethodId, int16 strategyNumber);
/**
 * @Description: Filter the supported clause by the rules. We can now push down two
 * types of expression: OpExpr and NullTest. For opExpr, var in one side and Const
 * in one side is required. For null test,  the argument must be var.
 * @in clause: The expression to be checked.
 * @return Return true: The clause can be pushed down; False: the clause can not be pushed down.
 */
static bool clause_hdfs_pushdown(Expr *clause);
/**
 * @Description: Whether the left node and the right node are satisfy
 * pushdown qual on DFS table.
 * @in leftop, the left node of expression.
 * @in rightop, the right node of expression.
 * @return If all node satisfy, return true, otherwise return false.
 */
static bool exprSatisfyPushDown(Node *leftop, Node *rightop);
static bool IsVarInNullTest(Expr *arg);
static bool IsOperatorPushdown(Oid opno);
static int GetVarNoFromClause(Expr *clause);
static bool PushDownSupportVar(Var *var);

/*
 * Make a basic Operator expression node.
 * @_in param variable: The var according to whose type we build the base constraint.
 * @_in param minValue: The pointer of the min value, which is obtained from the file footer.
 * @_in param maxValue: The pointer of the max value, which is obtained from the file footer
 * @_in param hasMinimun: if the hasMinimun is ture, the value of minValue is not NULL, otherwise
 *                        the minValue is NULL.
 * @_in param hasMaximum: if the hasMaximum is ture, the value of maxValue is not NULL, otherwise
 *                        the maxValue is NULL.
 * @return Return and OpExpression.
 */
Node *MakeBaseConstraint(Var *variable, Datum minValue, Datum maxValue, bool hasMinimum, bool hasMaximum)
{
    OpExpr *lessThanExpr = NULL;
    OpExpr *greaterThanExpr = NULL;

    /* Make these expressions with only one argument for now */
    lessThanExpr = MakeOperatorExpression(variable, BTLessEqualStrategyNumber);
    greaterThanExpr = MakeOperatorExpression(variable, BTGreaterEqualStrategyNumber);

    return MakeBaseConstraintWithExpr(lessThanExpr, greaterThanExpr, minValue, maxValue, hasMinimum, hasMaximum);
}

Node *MakeBaseConstraintWithExpr(OpExpr *lessThanExpr, OpExpr *greaterThanExpr, Datum minValue, Datum maxValue,
                                 bool hasMinimum, bool hasMaximum)
{
    Node *baseConstraint = NULL;
    Node *minNode = NULL;
    Node *maxNode = NULL;
    Const *minConstant = NULL;
    Const *maxConstant = NULL;

    /* Return NULL if both lessThanExpr and greaterThanExpr are NULL. */
    if (lessThanExpr == NULL && greaterThanExpr == NULL) {
        return NULL;
    }

    if (lessThanExpr != NULL) {
        maxNode = get_rightop((Expr *)lessThanExpr); /* right op */
        Assert(IsA(maxNode, Const));
        maxConstant = (Const *)maxNode;

        if (maxConstant != NULL) {
            maxConstant->constvalue = maxValue;
            maxConstant->constisnull = hasMaximum ? false : true;
        } else {
            return NULL;
        }
    }

    if (greaterThanExpr != NULL && minConstant != NULL) {
        /* set real constraint */
        minNode = get_rightop((Expr *)greaterThanExpr); /* right op */
        Assert(IsA(minNode, Const));
        minConstant = (Const *)minNode;
        if (minConstant != NULL) {
            minConstant->constvalue = minValue;
            minConstant->constisnull = hasMinimum ? false : true;
        } else {
            return NULL;
        }
    }

    /* Make base constaint as an and of two qual conditions */
    baseConstraint = make_and_qual((Node *)lessThanExpr, (Node *)greaterThanExpr);

    return baseConstraint;
}

/*
 * Build a new null test expr with the given type.
 *
 * @_in param variable: The value based on which we build a NullTest expression.
 * @_in param type: is null or is not null.
 * @return Return the NullTest expression we build here.
 */
Node *BuildNullTestConstraint(Var *variable, NullTestType type)
{
    return (Node *)makeNullTest(type, (Expr *)variable);
}

/*
 * Fill the expression with the const value.
 * @_in param equalExpr: the expression of the constraint.
 * @_in param value: Value to be filled for the right op.
 * @_in param isNull: Whether the value is null.
 * @return Return the constraint.
 */
void BuildConstraintConst(Expr *equalExpr, Datum value, bool isNull)
{
    Const *constant = (Const *)get_rightop(equalExpr);

    if (constant != NULL) {
        constant->constvalue = value;
        constant->constisnull = isNull ? true : false;
    } else {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                        errmsg("Rigth operation in constraint expression cannot be NULL.")));
    }
    return;
}

OpExpr *MakeOperatorExpression(Var *variable, int16 strategyNumber)
{
    Oid typeID = variable->vartype;
    Oid typeModId = variable->vartypmod;
    Oid collationId = variable->varcollid;

    Oid accessMethodId = BTREE_AM_OID; 
    Oid OpId = InvalidOid;
    Const *ConstValue = NULL;
    OpExpr *expr = NULL;
    Expr *leftop = (Expr *)variable;

    /* varchar causes problem, change it with text and add relable expr. */
    if (typeID == NVARCHAR2OID || typeID == VARCHAROID || typeID == CLOBOID) {
        typeID = TEXTOID;
        leftop = (Expr *)makeRelabelType(leftop, typeID, -1, collationId, COERCE_IMPLICIT_CAST);
    }

    /* Loading the operator from catalogs */
    ConstValue = makeNullConst(typeID, typeModId, collationId);

    OpId = GetOperatorByTypeId(typeID, accessMethodId, strategyNumber);

    /* Build the expression with the given variable and a null constant */
    expr = (OpExpr *)make_opclause(OpId, InvalidOid, /* no result type yet */
                                   false,            /* no return set */
                                   leftop, (Expr *)ConstValue, InvalidOid, C_COLLATION_OID);

    /* Build up implementing function id and result type */
    expr->opfuncid = get_opcode(OpId);
    expr->opresulttype = get_func_rettype(expr->opfuncid);

    return expr;
}

/* Returns operator oid for the given type, access method, and strategy number. */
static Oid GetOperatorByTypeId(Oid typeID, Oid accessMethodId, int16 strategyNumber)
{
    /* Get default operator class from pg_opclass */
    Oid operatorClassId = GetDefaultOpClass(typeID, accessMethodId);
    if (InvalidOid == operatorClassId) {
        ereport(ERROR,
                (errcode(ERRCODE_CASE_NOT_FOUND), errmodule(MOD_DFS), errmsg("Invalid Oid for operator %u.", typeID)));
    }

    Oid operatorFamily = get_opclass_family(operatorClassId);

    Oid operatorId = get_opfamily_member(operatorFamily, typeID, typeID, strategyNumber);

    return operatorId;
}

/*
 * Parse the fileNames string from the split List.
 * @_in_out param splitList: point to the original split List, which may contain multiple files.
 * @_in param currentFileName: point to the first file or the only file like '/user/file1.orc' (new buffer).
 * @return Return the split parsed from the list.
 */
SplitInfo *ParseFileSplitList(List **splitList, char **currentFileName)
{
    Assert(list_length(*splitList) > 0);
    SplitInfo *split = (SplitInfo *)linitial(*splitList);
    *currentFileName = split->filePath;
    *splitList = list_delete_first(*splitList);

    return split;
}

/*
 * @Description: find a split from the split list by file ID
 * @IN splitList: the list of all splits
 * @IN fileID: the file id
 * @Return: the split if found, or return NULL
 * @See also:
 */
SplitInfo *FindFileSplitByID(List *splitList, int fileID)
{
    ListCell *cell = NULL;

    foreach (cell, splitList) {
        SplitInfo *split = (SplitInfo *)lfirst(cell);
        char *substr = strrchr(split->filePath, '.');
        if (unlikely(substr == nullptr)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmodule(MOD_DFS),
                            errmsg("Invalid file path:%s", split->filePath)));
        }
        int currentFileID = pg_strtoint32(substr + 1);
        if (currentFileID == fileID) {
            return split;
        }
    }

    return NULL;
}

/*
 * Convert a char* to Datum according to the data type oid.
 *
 * @_in param typeOid: The oid of the type in pg_type catalog.
 * @_in param typeMod: The mod of data type.
 * @_in param value: The string value which need to be converted to datum.
 * @return Return the datum converted from String.
 */
Datum GetDatumFromString(Oid typeOid, int4 typeMod, char *value)
{
    Datum datumValue = (Datum)0;

    switch (typeOid) {
        /* Numeric datatype */
        /* 1. Towards to TINYINT */
        case INT1OID: {
            datumValue = UInt8GetDatum(pg_atoi(value, sizeof(uint8), '\0'));
            break;
        }

        /* 2. Towards to SMALLINT */
        case INT2OID: {
            datumValue = Int16GetDatum(pg_strtoint16(value));
            break;
        }

        /* 3. Towards to INTEGER */
        case INT4OID: {
            datumValue = Int32GetDatum(pg_strtoint32(value));
            break;
        }

        /* 4. Towards to BIGINT */
        case INT8OID: {
            int64 result = 0;
            (void)scanint8(value, false, &result);
            datumValue = Int64GetDatum(result);
            break;
        }

        /* 5. Towards to NUMERIC./DECIMAL */
        case NUMERICOID: {
            datumValue = DirectFunctionCall3(numeric_in, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* Textual type conversion
         *
         * 6. Towards to CHAR
         */
        case CHAROID: {
            datumValue = DirectFunctionCall3(charin, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));

            break;
        }

        /* 7. Towards to CHAR() */
        case BPCHAROID: {
            datumValue = DirectFunctionCall3(bpcharin, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* 8. Towards to VARCHAR(x) */
        case VARCHAROID: {
            datumValue = DirectFunctionCall3(varcharin, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* 9. Towards to NVARCHAR2 */
        case NVARCHAR2OID: {
            datumValue = DirectFunctionCall3(nvarchar2in, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* 10. Towards to TEXT */
        case CLOBOID:
        case TEXTOID: {
            datumValue = CStringGetTextDatum(value);
            break;
        }

        /* Temporal related type conversion */
        /* 11. Towards to DATE */
        case DATEOID: {
            datumValue = DirectFunctionCall1(date_in, CStringGetDatum(value));
            break;
        }

        /* 12. Towards to TIME WITHOUT TIME ZONE */
        case TIMEOID: {
            datumValue = DirectFunctionCall1(time_in, CStringGetDatum(value));
            break;
        }

        /* 13. Towards to TIME WITH TIME ZONE */
        case TIMETZOID: {
            datumValue = DirectFunctionCall1(timetz_in, CStringGetDatum(value));
            break;
        }

        /* 14. Towards to TIMESTAMP WITHOUT TIME ZONE */
        case TIMESTAMPOID: {
            datumValue = DirectFunctionCall3(timestamp_in, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* 15. Towards to TIMESTAMP WITH TIME ZONE */
        case TIMESTAMPTZOID: {
            datumValue = DirectFunctionCall3(timestamptz_in, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* 16. Towards to SMALLDATETIME */
        case SMALLDATETIMEOID: {
            datumValue = DirectFunctionCall3(smalldatetime_in, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* 17. Towards to INTERVAL */
        case INTERVALOID: {
            datumValue = DirectFunctionCall3(interval_in, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* 18. Towards to float4 */
        case FLOAT4OID: {
            datumValue = DirectFunctionCall1(float4in, CStringGetDatum(value));
            break;
        }

        /* 19. Towards to float8 */
        case FLOAT8OID: {
            datumValue = DirectFunctionCall1(float8in, CStringGetDatum(value));
            break;
        }

        default: {
            /*
             * As we already blocked any un-supported datatype at table-creation
             * time, so we shouldn't get here, otherwise the catalog information
             * may gets corrupted.
             */
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                            errmsg("Unsupported data type on typeoid:%u when converting string to datum.", typeOid)));
        }
    }

    return datumValue;
}

/*
 * Convert a Datum to char* according to the data type oid.
 *
 * @_in param typeOid: The oid of the type in pg_type catalog.
 * @_in param typeMod: The mod of data type.
 * @_in param value: The string value which need to be converted to datum.
 * @return Return the datum converted from String.
 */
void GetStringFromDatum(Oid typeOid, int4 typeMod, Datum data, StringInfo string)
{
    char *raw_valuestr = NULL;
    char *encoded_valuestr = NULL;

    switch (typeOid) {
        /* 1. Towards to TINYINT */
        case INT1OID: {
            appendStringInfo(string, "%d", DatumGetUInt8(data));

            break;
        }

        /* 2. Towards to SMALLINT */
        case INT2OID: {
            appendStringInfo(string, "%d", DatumGetInt16(data));
            break;
        }

        /* 3. Towards to INTEGER */
        case INT4OID: {
            appendStringInfo(string, "%d", DatumGetInt32(data));
            break;
        }

        /* 4. Towards to BIGINT */
        case INT8OID: {
            appendStringInfo(string, "%lu", Int64GetDatum(data));
            break;
        }

        /* 5. Towards to NUMERIC./DECIMAL */
        case NUMERICOID: {
            raw_valuestr = DatumGetCString(DirectFunctionCall1(numeric_out, data));
            appendStringInfo(string, "%s", raw_valuestr);

            break;
        }

        /* Textual type conversion
         *
         * 6. Towards to CHAR
         */
        case CHAROID: {
            raw_valuestr = DatumGetCString(DirectFunctionCall1(charout, data));
            encoded_valuestr = UriEncode(raw_valuestr);
            appendStringInfo(string, "%s", encoded_valuestr);

            break;
        }

        /* 7. Towards to CHAR() */
        case BPCHAROID: {
            raw_valuestr = DatumGetCString(DirectFunctionCall1(varcharout, data));
            encoded_valuestr = UriEncode(raw_valuestr);
            appendStringInfo(string, "%s", encoded_valuestr);
            break;
        }

        /* 8. Towards to VARCHAR(x) */
        case VARCHAROID: {
            raw_valuestr = DatumGetCString(DirectFunctionCall1(varcharout, data));
            encoded_valuestr = UriEncode(raw_valuestr);
            appendStringInfo(string, "%s", encoded_valuestr);
            break;
        }

        /* 9. Towards to NVARCHAR2 */
        case NVARCHAR2OID: {
            raw_valuestr = DatumGetCString(DirectFunctionCall1(nvarchar2out, data));
            encoded_valuestr = UriEncode(raw_valuestr);
            appendStringInfo(string, "%s", encoded_valuestr);
            break;
        }

        /* 10. Towards to TEXT */
        case TEXTOID: {
            raw_valuestr = DatumGetCString(DirectFunctionCall1(textout, data));
            encoded_valuestr = UriEncode(raw_valuestr);
            appendStringInfo(string, "%s", encoded_valuestr);
            break;
        }

        /* Temporal related type conversion */
        /* 11. Towards to DATE */
        case DATEOID: {
            raw_valuestr = (char *)DirectFunctionCall1(date_out, data);
            encoded_valuestr = UriEncode(raw_valuestr);
            appendStringInfo(string, "%s", encoded_valuestr);
            break;
        }

        /* 12. Towards to TIME WITHOUT TIME ZONE */
        case TIMEOID: {
            raw_valuestr = (char *)DirectFunctionCall1(time_out, data);
            encoded_valuestr = UriEncode(raw_valuestr);
            appendStringInfo(string, "%s", encoded_valuestr);
            break;
        }

        /* 13. Towards to TIME WITH TIME ZONE */
        case TIMETZOID: {
            raw_valuestr = (char *)DirectFunctionCall1(timetz_out, data);
            encoded_valuestr = UriEncode(raw_valuestr);
            appendStringInfo(string, "%s", encoded_valuestr);
            break;
        }

        /* 14. Towards to TIMESTAMP WITHOUT TIME ZONE */
        case TIMESTAMPOID: {
            raw_valuestr = (char *)DirectFunctionCall1(timestamp_out, data);
            encoded_valuestr = UriEncode(raw_valuestr);
            appendStringInfo(string, "%s", encoded_valuestr);
            break;
        }

        /* 15. Towards to TIMESTAMP WITH TIME ZONE */
        case TIMESTAMPTZOID: {
            raw_valuestr = (char *)DirectFunctionCall1(timestamptz_out, data);
            encoded_valuestr = UriEncode(raw_valuestr);
            appendStringInfo(string, "%s", encoded_valuestr);
            break;
        }

        /* 16. Towards to SMALLDATETIME */
        case SMALLDATETIMEOID: {
            raw_valuestr = (char *)DirectFunctionCall1(smalldatetime_out, data);
            encoded_valuestr = UriEncode(raw_valuestr);
            appendStringInfo(string, "%s", encoded_valuestr);
            break;
        }

        /* 17. Towards to INTERVAL */
        case INTERVALOID: {
            raw_valuestr = (char *)DirectFunctionCall1(interval_out, data);
            encoded_valuestr = UriEncode(raw_valuestr);
            appendStringInfo(string, "%s", encoded_valuestr);
            break;
        }

        default: {
            /*
             * As we already blocked any un-supported datatype at table-creation
             * time, so we shouldn't get here, otherwise the catalog information
             * may gets corrupted.
             */
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                            errmsg("Unsupported data type on typeoid:%u when converting datum to string.", typeOid)));
        }
    }

    if (raw_valuestr != NULL) {
        pfree_ext(raw_valuestr);
    }

    if (encoded_valuestr != NULL) {
        pfree_ext(encoded_valuestr);
    }
}

/**
 * @Description: whether or not juge the gien opno is equal to the folowing
 * operator oid. The function is used to check operaor on partition column.
 * @in opno, the given operator Oid.
 * @return return true, if the given opno euqal the following opno,
 * otherwise return false.
 */
bool isEquivalentExpression(Oid opno)
{
    switch (opno) {
        case INT1EQOID:
        case INT2EQOID:
        case INT4EQOID:
        case INT8EQOID:
        case NUMERICEQOID:
        case CHAREQOID:
        case BPCHAREQOID:
        case TEXTEQOID:
        case INTERVALOID: {
            return true;
        }
        default: {
            return false;
        }
    }
}

/*
 * Find the last position of the special character in a string.
 * @_in param seperator: The sepcial character.
 * @_in param srcString: The source string.
 * return Return the position if it is found or return -1 if it is not found.
 */
int FindLastCharPos(char seperator, const char *srcString)
{
    if (srcString == NULL) {
        return -1;
    }

    int end = strlen(srcString);

    for (int i = end - 1; i >= 0; i--) {
        if (seperator == srcString[i]) {
            return i;
        }
    }

    return -1;
}

/*
 * Initialize a folder split by the folder name, filename(both folder name and filename comprise the absolute file
 * path), columnNo list of partition and column value list of partition.
 *
 * @_in param fileName: name of the file.
 * @_in param partContentList: the list of value in partition column.
 * @return Return the constructed split.
 */
SplitInfo *InitFolderSplit(char *fileName, List *partContentList, int64 objectSize)
{
    SplitInfo *split = makeNode(SplitInfo);
    split->filePath = fileName;
    int pos = FindLastCharPos('/', fileName);
    errno_t rc;
    if (-1 == pos) {
        split->fileName = NULL;
    } else {
        Assert(pos <= (int)strlen(fileName) - 1);
        int len = strlen(fileName) - pos; /* include '\0' */
        split->fileName = (char *)palloc0(len);
        rc = memcpy_s(split->fileName, len, fileName + pos + 1, len);
        securec_check(rc, "", "");
    }

    split->partContentList = partContentList;
    split->ObjectSize = objectSize;

    return split;
}

/*
 * Initialize a file split by the file path, columnNo list of partition and column value list of partition. This is
 * different from InitFolderSplit for it use the absolute file path as the param and has no file name which is used for
 * partition only.
 *
 * @_in param filePath: the absolute path of the split file.
 * @_in param partContentList: the list of value in partition column.
 * @return Return the constructed split.
 */
SplitInfo *InitFileSplit(char *filePath, List *partContentList, int64 fileSize)
{
    SplitInfo *split = makeNode(SplitInfo);

    split->filePath = filePath;
    split->fileName = NULL;
    split->partContentList = partContentList;
    split->ObjectSize = fileSize;

    return split;
}

/*
 * Destroy all the structors in the split.
 * @_in param split: The split need to to clean and is not used any more.
 */
void DestroySplit(SplitInfo *split)
{
    if (split == NULL) {
        return;
    }

    if (split->fileName != NULL) {
        pfree_ext(split->fileName);
    }

    if (split->filePath != NULL) {
        pfree_ext(split->filePath);
    }

    if (split->partContentList != NIL) {
        list_free_deep(split->partContentList);
        split->partContentList = NIL;
    }

    pfree_ext(split);
}

/*
 * Fetch the Var according to the column no from the columnList, used in partition filter.
 *
 * @_in param columnList: The column list from which we find the var.
 * @_in param colNo: The column index by which we find the var.
 * @return Return null: we don't find the var in the list with the colno; not null: we find the var and return it.
 */
Var *GetVarFromColumnList(List *columnList, int colNo)
{
    ListCell *lc = NULL;

    foreach (lc, columnList) {
        Var *value = (Var *)lfirst(lc);
        if (value->varattno == colNo) {
            return value;
        }
    }

    return NULL;
}

/*
 * Return the digits of a int value. like 0 return 1, 10 return 2.
 */
uint64 GetDigitOfInt(uint64 x)
{
    uint64 digit = 0;

    if (x == 0) {
        return 1;
    }

    while (x >= 1) {
        x = x / 10;
        digit++;
    }

    return digit;
}

/**
 * @Description: Identify the qual which could be pushed down to
 * file level(e.g. ORC reader level). If the qual could be pushed
 * down, the qual will be appended to hdfsPushDownQual and deleted
 * from primitiveQual. Otherwise do nothing.
 */
List *fix_pushdown_qual(List **hdfsPushDownQual, List **primitiveQual, const List *part_list)
{
    ListCell *lc = NULL;
    List *hdfsQualColumn = NIL;

    if (0 == list_length(*primitiveQual)) {
        return NIL;
    }

    /*
     * Here only support andExpr clause.
     * And when Filter_hdfs_clause return true, means the clause can be pushed down,
     * then we will delete the clause from the primitiveQual because we need not use it to
     * filter the batch/tuple upper anymore.
     */
    List *copyQual = list_copy(*primitiveQual);
    foreach (lc, copyQual) {
        Expr *primitiveClause = (Expr *)lfirst(lc);
        Expr *clause = (Expr *)copyObject(primitiveClause);

        if (!clause_hdfs_pushdown(clause)) {
            continue;
        }

        int varno = GetVarNoFromClause(clause);
        /*
         * For restriction clause of partition column, we do not keep this clause
         * on hdfsPushDownQual because we can not build HdfsScanPredicate on partition
         * column.
         * Note: for value-partition table, we need keep the partition column clause in
         * primitiveQual for precise filtering.
         * description: Delete pushdown partition restriction from primitiveQual. Only filter predicate
         * by using Static Partition Pruning.
         */
        if (!list_member_int(part_list, varno)) {
            hdfsQualColumn = lappend_int(hdfsQualColumn, varno);
            *hdfsPushDownQual = lappend(*hdfsPushDownQual, clause);
            *primitiveQual = list_delete(*primitiveQual, primitiveClause);
            *primitiveQual = lappend(*primitiveQual, clause);
        }
    }

    list_free(copyQual);
    copyQual = NIL;

    return hdfsQualColumn;
}

/*
 * We assume that the clause has be filtered by fix_hdfs_fscan_qual.
 * @_in_param clause: The clause either OpExpr or NullTest.
 * @return -1 if a proper var is not found, or the var no .
 */
static int GetVarNoFromClause(Expr *clause)
{
    Var *var = NULL;
    int varno = -1;

    if (clause == NULL) {
        return varno;
    }

    if (IsA(clause, NullTest)) {
        var = (Var *)((NullTest *)clause)->arg;
    } else if (IsA(clause, OpExpr)) {
        var = (Var *)get_leftop(clause);
    }

    if (var == NULL) {
        return -1;
    }

    if (IsA((Expr *)var, RelabelType)) {
        var = (Var *)((RelabelType *)var)->arg;
    }

    if (var != NULL) {
        varno = var->varattno;
    } else {
        varno = -1;
    }

    return varno;
}

inline static bool PushDownSupportVar(Var *var)
{
    bool ret = false;
    switch (var->vartype) {
        case BOOLOID:
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case DATEOID:
        case FLOAT4OID:
        case FLOAT8OID:
        case VARCHAROID:
        case CLOBOID:
        case TEXTOID:
        case BPCHAROID:
        case TIMESTAMPOID: {
            ret = true;
            break;
        }
        case NUMERICOID: {
            unsigned int typemod = (unsigned int)(var->vartypmod - VARHDRSZ);
            if (var->vartypmod != -1) {
                int precision = (typemod >> 16) & 0xffff;
                if (precision > 0 && precision <= 38) {
                    ret = true;
                }
            }
            break;
        }
        default: {
            ret = false;
            break;
        }
    }

    return ret;
}

/*
 * strip the zero from left, and get the number of valid digit in short integer.
 * for example: 1234 => 4, 234 => 3, 34 => 2, 4 => 1
 */
int left_strip_zero(short v)
{
    Assert(v != 0);

    short base = 1000;
    int n = 0;
    while (v != 0) {
        if (v / base != 0) {
            break;
        }
        v %= base;
        base /= 10;
        n++;
    }
    return DEC_DIGITS - n;
}

/*
 * strip the zero from right, and get the number of valid digit in short integer.
 * for example: 1234 => 4, 1230 => 3, 1200 => 2, 1000 => 1, 100(0100) => 2...
 */
int right_strip_zero(short v)
{
    Assert(v != 0);

    int n = DEC_DIGITS;
    while (v != 0) {
        if (v % 10 != 0) {
            break;
        }
        v /= 10;
        n--;
    }
    return n;
}

/**
 * @Description: Whether the left node and the right node are satisfy
 * pushdown qual on DFS table.
 * @in leftop, the left node of expression.
 * @in rightop, the right node of expression.
 * @return If all node satisfy, return true, otherwise return false.
 */
static bool exprSatisfyPushDown(Node *leftop, Node *rightop)
{
    Var *var = NULL;

    if (IsA(leftop, RelabelType)) {
        leftop = (Node *)((RelabelType *)leftop)->arg;
    }
    var = (Var *)leftop;

    if (PushDownSupportVar(var)) {
        if (rightop && (IsA(rightop, Const) || IsParamConst(rightop))) {
            Oid rightType = 0;
            int32 rightTypeMod = 0;

            if (IsA(rightop, Const)) {
                rightType = ((Const *)rightop)->consttype;
                rightTypeMod = ((Const *)rightop)->consttypmod;
            }

            if (IsParamConst(rightop)) {
                rightType = ((Param *)rightop)->paramtype;
                rightTypeMod = ((Param *)rightop)->paramtypmod;
            }

            /* for bpchar var(or const), type and typemod must be the same. */
            if (var->vartype == BPCHAROID || rightType == BPCHAROID) {
                if (var->vartype != rightType || (var->vartypmod != rightTypeMod && rightTypeMod != -1)) {
                    return false;
                }
            }

            if (var->vartype != NUMERICOID) {
                return true;
            }

            /* Param const will not be pushed down for numeric type. */
            if (IsParamConst(rightop)) {
                return false;
            }

            /*
             * For numeric type, const value can not exceed the limit of definition
             * of the short numeric while it is pushed down.
             */
            uint32 typmod = (uint32)(var->vartypmod - VARHDRSZ);
            int32 varPrecision = (typmod >> 16) & 0xffff;
            int32 varScale = typmod & 0xffff;

            /*
             * the precision of the const value > 38 if the format of the const
             * value is NOT short numeric
             */
            Numeric value = DatumGetNumeric(((Const *)rightop)->constvalue);
            if (!NUMERIC_IS_SHORT(value)) {
                return false;
            }

            short *data = SHORT_NUMERIC_DIGITS(value);
            int ndigits = SHORT_NUMERIC_NDIGITS(value);
            if (ndigits <= 0) {
                return true;
            }

            int leftdigits = 0;
            int rightdigits = 0;

            /* get the accurate number of valid digit in non-scale of numeric */
            int constWeight = NUMERIC_WEIGHT(value);
            if (constWeight >= 0) {
                leftdigits = left_strip_zero(data[0]);
                if (0 != constWeight) {
                    leftdigits += constWeight * DEC_DIGITS;
                }

                if (leftdigits > (varPrecision - varScale)) {
                    return false;
                }
            }

            /* get the accurate number of valid digit in scale of numeric */
            int constAscale = ndigits - (constWeight + 1);
            if (constAscale > 0) {
                rightdigits = right_strip_zero(data[ndigits - 1]);
                if (constAscale > 1) {
                    rightdigits += (constAscale - 1) * DEC_DIGITS;
                }

                if (rightdigits > varScale) {
                    return false;
                }
            }

            if ((leftdigits + rightdigits) > varPrecision) {
                return false;
            }

            return true;
        }
    }

    return false;
}

/**
 * @Description: Filter the supported clause by the rules. We can now push down two
 * types of expression: OpExpr and NullTest. For opExpr, var in one side and Const
 * in one side is required. For null test,  the argument must be var.
 */
static bool clause_hdfs_pushdown(Expr *clause)
{
    bool plain_op = false;
    Var *var = NULL;

    /* Here only support the and clause. If the qual is or clause, the tag of clause is T_BoolExpr */
    if (IsA(clause, OpExpr)) {
        Node *leftop = NULL;
        Node *rightop = NULL;
        OpExpr *op_clause = (OpExpr *)clause;

        if (list_length(op_clause->args) != 2 || !IsOperatorPushdown(op_clause->opno)) {
            return plain_op;
        }

        leftop = get_leftop(clause);
        rightop = get_rightop(clause);

        if (leftop == NULL) {
            ereport(ERROR, (errmodule(MOD_DFS), errmsg("The leftop is null")));
        }
        if (rightop == NULL) {
            ereport(ERROR, (errmodule(MOD_DFS), errmsg("The rightop is null")));
        }

        /* Support var op const, const op var */
        if (leftop && IsVarNode(leftop)) {
            plain_op = exprSatisfyPushDown(leftop, rightop);
        } else if (rightop && IsVarNode(rightop)) {
            CommuteOpExpr(op_clause);
            set_opfuncid(op_clause);
            plain_op = exprSatisfyPushDown(rightop, leftop);
        }
        ereport(DEBUG5, (errmodule(MOD_DFS), errmsg("hdfs pushdown result: %s", plain_op ? "true" : "false")));
    } else if (IsA(clause, NullTest) && !((NullTest *)clause)->argisrow && IsVarInNullTest(((NullTest *)clause)->arg)) {
        /* When IS (NOT) NULL act in the simple way and arg is Var, we can push it down */
        var = (Var *)((NullTest *)clause)->arg;

        if (IsA((Expr *)var, RelabelType)) {
            var = (Var *)((RelabelType *)var)->arg;
        }

        if (PushDownSupportVar(var)) {
            plain_op = true;
        }
    }

    return plain_op;
}

/*
 * check if the node is Var.
 *
 * @_in param node: The node to be checkd.
 * @return Return true: the node is a var; False: the node is not a var.
 */
bool IsVarNode(Node *node)
{
    bool is_var = false;

    if (IsA(node, Var) && ((Var *)node)->varattno > 0) {
        is_var = true;
    } else if (IsA(node, RelabelType)) {
        RelabelType *reltype = (RelabelType *)node;

        if (IsA(reltype->arg, Var) && ((Var *)reltype->arg)->varattno > 0) {
            is_var = true;
        }
    }

    return is_var;
}

/*
 * Check if the node is extern param or exec param.
 *
 * @_in param node: The node to be checkd.
 * @return Return true: the node is a extern or exec param; False: the node is not a extern or exec param.
 */
bool IsParamConst(Node *node)
{
    bool is_param_const = false;

    if (node && nodeTag(node) == T_Param) {
        Param *param = (Param *)node;
        if (param->paramkind == PARAM_EXTERN || param->paramkind == PARAM_EXEC) {
            is_param_const = true;
        }
    }

    return is_param_const;
}

/*
 * Check if the arg of the Null test expr is Var.
 *
 * @_in param arg: Null test expression to be checked.
 * @return Return true: the argument of the null test is var; False: the argument of the null test is not var.
 */
static bool IsVarInNullTest(Expr *arg)
{
    if (IsA(arg, RelabelType)) {
        arg = ((RelabelType *)arg)->arg;
    }

    if (IsA(arg, Var)) {
        return true;
    }

    return false;
}

/*
 * Search the catalog and check if the operation can be pushed down, now we support <,>,=,>=,<=,!=, <>, 'is null', 'is
 * not null'.
 *
 * @_in param opno: The oid of the operator.
 * @return Return true: the operator can be pushed down; False: the operator can not be pushed down.
 */
static bool IsOperatorPushdown(Oid opno)
{
    bool is_pushdown = true;

    HeapTuple tp = NULL;
    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if (HeapTupleIsValid(tp)) {
        Form_pg_operator optup = (Form_pg_operator)GETSTRUCT(tp);
        if (HDFS_QUERY_INVALID == HdfsGetQueryOperator(NameStr(optup->oprname))) {
            is_pushdown = false;
        }
    } else {
        ereport(ERROR,
                (errcode(ERRCODE_CASE_NOT_FOUND), errmodule(MOD_DFS), errmsg("cache lookup failed for type %u", opno)));
    }

    ReleaseSysCache(tp);

    return is_pushdown;
}

/*
 * Construct a node of DfsPrivateItem.
 */
DfsPrivateItem *MakeDfsPrivateItem(List *columnList, List *targetList, List *restrictColList, List *opExpressionList,
                                   List *dnTask, List *hdfsQual, double *selectivity, int colNum, List *partList)
{
    DfsPrivateItem *item = makeNode(DfsPrivateItem);
    item->columnList = columnList;
    item->targetList = targetList;
    item->restrictColList = restrictColList;
    item->opExpressionList = (List *)copyObject(opExpressionList);
    item->dnTask = dnTask;
    item->hdfsQual = hdfsQual;
    item->selectivity = selectivity;
    item->colNum = colNum;
    item->partList = partList;
    /*
     * Compare the size of strings by using "C" format in coarse filter.
     */
    ListCell *lc = NULL;
    List *opExprList = pull_opExpr((Node *)item->opExpressionList);
    foreach (lc, opExprList) {
        OpExpr *opExpr = (OpExpr *)lfirst(lc);
        opExpr->inputcollid = C_COLLATION_OID;
    }
    list_free_ext(opExprList);
    return item;
}

/**
 * @Description: Set all the rows of current ScalarVector with the same value transfered.
 * The value can be null.
 * @in vec, The scalar vector to which we set the value.
 * @in rowsToRead, The number of rows to set.
 * @in value: The fixed value to set all the rows.
 * @in isNull, Whether the column value is null.
 * @return None.
 */
void SetAllValue(ScalarVector *vec, int rows, Datum value, bool isNull)
{
    if (isNull) {
        errno_t rc = memset_s(&vec->m_flag[vec->m_rows], BatchMaxSize - vec->m_rows, 1, rows - vec->m_rows);
        securec_check(rc, "\0", "\0");
    } else {
        ScalarValue val = vec->DatumToScalar(value, vec->m_desc.typeId, false);
        for (int i = vec->m_rows; i < rows; i++) {
            vec->m_vals[i] = val;
        }
    }
}

/* Brief: Get the information of the columns which are in the restriction.
 *        Returns them in a new list.
 * input param @rel: List of restriction columns.
 */
List *GetRestrictColumns(List *neededList, int count)
{
    List *usedList = NIL;
    ListCell *restrictCell = NULL;

    /* then walk over all restriction clauses, and pull up any used columns */
    foreach (restrictCell, neededList) {
        Node *clause = (Node *)lfirst(restrictCell);
        List *clauseList = NIL;

        /* recursively pull up any columns used in the restriction clause */
        clauseList = pull_var_clause(clause, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);

        usedList = MergeList(usedList, clauseList, count);
        list_free_ext(clauseList);
    }

    return usedList;
}

List *MergeList(List *targetList, List *restrictList, int count)
{
    List *results = NIL;
    AttrNumber i = 1;  // columnIndex
    Var *usedColumn = NULL;
    ListCell *usedCell = NULL;

    /* walk over all column definitions, and de-duplicate column list */
    for (i = 1; i <= count; i++) {
        bool found = false;

        /* look for this column in the needed column list */
        foreach (usedCell, targetList) {
            Node *columnNode = (Node *)lfirst(usedCell);
            Assert(IsA(columnNode, Var));
            usedColumn = (Var *)columnNode;
            if (usedColumn != NULL && usedColumn->varattno == i) {
                results = lappend(results, usedColumn);
                found = true;
                break;
            }
        }

        if (!found) {
            foreach (usedCell, restrictList) {
                usedColumn = (Var *)lfirst(usedCell);
                if (usedColumn != NULL && usedColumn->varattno == i) {
                    results = lappend(results, usedColumn);
                    break;
                }
            }
        }
    }

    return results;
}

/*
 * @Description: build the columns list for dfsscan, which comes from reltargetlist
 *	 	and exclude the index columns.
 * @IN reltargetlist: the primitive target list
 * @IN excludedColList: the columns which should be abandoned.
 * @Return: the final reader list for dfs scan
 * @See also:
 */
List *build_dfs_reader_tlist(List *reltargetlist, List *excludedColList)
{
    List *indexColList = NIL;
    List *tlist = NIL;
    ListCell *cell = NULL;

    /* Fill the index column no into indexColList. */
    foreach (cell, excludedColList) {
        TargetEntry *tle = (TargetEntry *)lfirst(cell);
        Assert(IsA(tle->expr, Var));
        indexColList = lappend_int(indexColList, ((Var *)tle->expr)->varattno);
    }

    /* Build the reader target list from reltargetlist and exclude the index columns. */
    foreach (cell, reltargetlist) {
        /* Do we really need to copy here?	Not sure */
        TargetEntry *tle = (TargetEntry *)lfirst(cell);

        /* Pull vars from  the targetlist . */
        ListCell *lc = NULL;
        List *vars = pull_var_clause((Node *)tle, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);

        foreach (lc, vars) {
            Var *var = (Var *)lfirst(lc);
            int varattno = (int)var->varattno;

            if (varattno >= 0 && !list_member_int(indexColList, varattno)) {
                tlist = lappend(tlist, var);
            }
        }
    }
    if (indexColList != NIL) {
        list_free(indexColList);
        indexColList = NIL;
    }

    return tlist;
}

template <typename wrapper, typename baseType>
void SetRunTimePredicate(dfs::reader::ReaderState *readerState, RunTimeParamPredicateInfo *runTimeParamPredicate,
                         Datum scanValue, int arrPosition, bool isNull)
{
    HdfsScanPredicate<wrapper, baseType> *hdfsScanPredicate =
        (HdfsScanPredicate<wrapper, baseType> *)list_nth(readerState->hdfsScanPredicateArr[arrPosition],
                                                         runTimeParamPredicate->paramPosition - 1);
    Node *baseRestriction = NULL;

    if (isNull) {
        hdfsScanPredicate->setKeepFalse(true);

        /* Simulate a restriction which will be unsatisfied generally. */
        Var *var = GetVarFromColumnList(readerState->allColumnList, hdfsScanPredicate->m_attno);
        baseRestriction = BuildNullTestConstraint(var, IS_NULL);
    } else {
        hdfsScanPredicate->Init(scanValue, runTimeParamPredicate->datumType, runTimeParamPredicate->typeMod);
        hdfsScanPredicate->setKeepFalse(false);

        if (runTimeParamPredicate->opExpr != NULL) {
            BuildConstraintConst(runTimeParamPredicate->opExpr, scanValue, false);
            baseRestriction = (Node *)(runTimeParamPredicate->opExpr);
        }
    }

    if (baseRestriction != NULL) {
        readerState->runtimeRestrictionList = lappend(readerState->runtimeRestrictionList, baseRestriction);
    }
}

/**
 * @Description: fill the BloomFilter to reader object.
 * @in scanState, the given scanState.
 * @in blf, the blf to be filled.
 * @in var, the current column, on which fill bloomfilter info.
 * @return none.
 */
void setForeignScanBf(ScanState *scanState, filter::BloomFilter *blf, Var *var)
{
    ForeignScanState *foreignScanState = (ForeignScanState *)scanState;
    FdwRoutine *fdwRoutine = foreignScanState->fdwroutine;
    if (fdwRoutine->GetFdwType && HDFS_ORC == fdwRoutine->GetFdwType() && fdwRoutine->BuildRuntimePredicate != NULL) {
        fdwRoutine->BuildRuntimePredicate(foreignScanState, blf, var->varoattno - 1, HDFS_BLOOM_FILTER);
    }
}

/*
 * brief: Build internal executor parameter predicate for pushdown of dfs scan.
 * input param @readerState: Includes all the params which are used during reading.
 */
void BuildRunTimePredicates(dfs::reader::ReaderState *readerState)
{
    AutoContextSwitch newContext(readerState->rescanCtx);
    ScanState *scanState = readerState->scanstate;
    ListCell *cell = NULL;
    ExprContext *econtext = scanState->ps.ps_ExprContext;

    /* Reset the last runtime restrictions. */
    readerState->runtimeRestrictionList = NIL;

    /* Build */
    foreach (cell, scanState->runTimeParamPredicates) {
        RunTimeParamPredicateInfo *runTimeParamPredicate = (RunTimeParamPredicateInfo *)lfirst(cell);
        int arrPosition = runTimeParamPredicate->varNoPos;

        /* We want to keep the key values in per-tuple memory */
        MemoryContext oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
        Datum scanvalue;
        bool isNull = false;
        scanvalue = ExecEvalExpr(runTimeParamPredicate->paraExecExpr, econtext, &isNull, NULL);
        MemoryContextSwitchTo(oldContext);

        switch (runTimeParamPredicate->varTypeOid) {
            case BOOLOID:
            case INT1OID:
            case INT2OID:
            case INT4OID:
            case INT8OID: {
                SetRunTimePredicate<Int64Wrapper, int64>(readerState, runTimeParamPredicate, scanvalue, arrPosition,
                                                         isNull);
                break;
            }
            case NUMERICOID: {
                /*
                 * PushDownSupportVar() make sure that precision <= 38 for numeric
                 */
                uint32 typmod = runTimeParamPredicate->typeMod - VARHDRSZ;
                uint32 precision = uint32((typmod >> 16) & 0xffff);
                if (precision <= 18) {
                    SetRunTimePredicate<Int64Wrapper, int64>(readerState, runTimeParamPredicate, scanvalue, arrPosition,
                                                             isNull);
                } else {
                    SetRunTimePredicate<Int128Wrapper, int128>(readerState, runTimeParamPredicate, scanvalue,
                                                               arrPosition, isNull);
                }
                break;
            }
            case FLOAT4OID:
            case FLOAT8OID: {
                SetRunTimePredicate<Float8Wrapper, double>(readerState, runTimeParamPredicate, scanvalue, arrPosition,
                                                           isNull);
                break;
            }
            case VARCHAROID:
            case CLOBOID:
            case TEXTOID:
            case BPCHAROID: {
                SetRunTimePredicate<StringWrapper, char *>(readerState, runTimeParamPredicate, scanvalue, arrPosition,
                                                           isNull);
                break;
            }
            case DATEOID:
            case TIMESTAMPOID: {
                SetRunTimePredicate<TimestampWrapper, Timestamp>(readerState, runTimeParamPredicate, scanvalue,
                                                                 arrPosition, isNull);
                break;
            }
            default: {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                                errmsg("Data type %u has not been supported for predicate push down.",
                                       runTimeParamPredicate->varTypeOid)));
            }
        }
    }

    /*
     * if fScan->not_use_bloomfilter == true, some error occurs in the compute pool,
     * so run in local. for right result, not use bloom filter for this case.
     */
    bool use_bloom_filter = true;
    Plan *plan = readerState->scanstate->ps.plan;
    if (IsA(plan, VecForeignScan) || IsA(plan, ForeignScan)) {
        ForeignScan *fScan = (ForeignScan *)plan;
        if (fScan->not_use_bloomfilter == true) {
            use_bloom_filter = false;
        }
    }

    if (u_sess->attr.attr_sql.enable_bloom_filter && use_bloom_filter) {
        EState *estate = readerState->scanstate->ps.state;
        List *varList = plan->var_list;
        List *bfIndex = plan->filterIndexList;
        int bfCount = list_length(varList);
        Assert(bfCount == list_length(bfIndex));
        bool pushdowComputePool = false;
        ForeignScan *fScan = NULL;

        if (IsA(plan, VecForeignScan)) {
            fScan = (ForeignScan *)plan;
            if (fScan->bloomFilterSet != NULL) {
                pushdowComputePool = true;
            }
        }

        /* Copy static bloom filters first. */
        if (IsA(plan, DfsScan)) {
            ((DfsScanState *)scanState)->m_fileReader->copyBloomFilter();
        }

        for (int i = 0; i < bfCount; i++) {
            Var *var = (Var *)list_nth(varList, i);
            int idx = list_nth_int(bfIndex, i);
            filter::BloomFilter *blf = NULL;
            if (pushdowComputePool && fScan->bloomFilterSet[i] != NULL) {
                blf = filter::createBloomFilter(fScan->bloomFilterSet[i]);
            } else {
                blf = estate->es_bloom_filter.bfarray[idx];
            }

            if (blf != NULL) {
                if (IsA(plan, DfsScan)) {
                    ((DfsScanState *)scanState)->m_fileReader->addBloomFilter(blf, var->varoattno - 1, true);
                } else if (IsA(plan, VecForeignScan)) {
                    setForeignScanBf(scanState, blf, var);
                }
            }
        }
    }

    scanState->runTimePredicatesReady = true;
}

/*
 * Calculate the predicate selectivity of each column.
 */
void CalculateWeightByColumns(PlannerInfo *root, List *hdfsQualColumn, List *hdfsQual, double *selectivity, int colNum)
{
    int i = 0;
    List **qualArr = (List **)palloc0(sizeof(List *) * colNum);
    Assert(list_length(hdfsQualColumn) == list_length(hdfsQual));

    for (i = 0; i < list_length(hdfsQualColumn); i++) {
        int colNo = list_nth_int(hdfsQualColumn, i);
        Expr *clause = (Expr *)list_nth(hdfsQual, i);
        qualArr[colNo - 1] = lappend(qualArr[colNo - 1], clause);
    }

    for (i = 0; i < colNum; i++) {
        if (qualArr[i] == NULL) {
            selectivity[i] = 0.0;
        } else {
            selectivity[i] = clauselist_selectivity(root, qualArr[i], 0, JOIN_INNER, NULL);
        }
    }

    /* Clean the temporary structure. */
    for (i = 0; i < colNum; i++) {
        if (qualArr[i] != NULL) {
            list_free(qualArr[i]);
        }
    }
    pfree_ext(qualArr);
}

/*
 * @Description: build the list of columns which are needed for dfs scan.
 * @IN attributes: the attributes of the current relation
 * @IN columnCount: the size of @colIdx array
 * @IN colIdx: the array of columns from which we build the column list
 * @Return: the list of columns which are needed for dfs scan
 * @See also:
 */
List *CreateColList(Form_pg_attribute *attributes, const int columnCount, const int16 *colIdx)
{
    List *columnList = NIL;
    int16 columnIndex = 0;

    for (int i = 0; i < columnCount; i++) {
        Var *column = makeNode(Var);
        if (colIdx == NULL) {
            columnIndex = i;
        } else {
            /* Exclude system columns in the column list. */
            if (colIdx[i] <= 0) {
                continue;
            }

            columnIndex = colIdx[i] - 1;
        }

        /*
         * If the column is dropped, it is not necessary to add this column
         * into column list.
         */
        if (attributes[columnIndex]->attisdropped) {
            continue;
        }

        /* only assign required fields for column mapping hash */
        column->varattno = columnIndex + 1;
        column->vartype = attributes[columnIndex]->atttypid;
        column->vartypmod = attributes[columnIndex]->atttypmod;

        columnList = lappend(columnList, column);
    }

    return columnList;
}

int64_t datumGetInt64ByVar(Datum datumValue, Var *colVar)
{
    int64_t value = 0;
    Oid datumType = colVar->vartype;
    uint32 typeMod = (uint32)(colVar->vartypmod - VARHDRSZ);
    if (datumType == INT1OID) {
        value = DatumGetChar(datumValue);
    } else if (datumType == INT2OID) {
        value = DatumGetInt16(datumValue);
    } else if (datumType == INT4OID) {
        value = DatumGetInt32(datumValue);
    } else if (datumType == NUMERICOID) {
        uint32 scale = typeMod & 0xffff;
        uint32 precision = (typeMod >> 16) & 0xffff;
        if (precision > 0 && precision <= 18) {
            value = convert_short_numeric_to_int64_byscale(DatumGetNumeric(datumValue), scale);
        } else {
            /*
             * It is not possible to reach here.
             */
            ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE), errmodule(MOD_DFS),
                            errmsg("Please select other functions to parse deciaml data.")));
        }
    } else {
        value = DatumGetInt64(datumValue);
    }
    return value;
}

__int128 datumGetInt128ByVar(Datum datumValue, Var *colVar)
{
    __int128 value = 0;
    uint32 typeMod = (uint32)(colVar->vartypmod - VARHDRSZ);
    Assert(colVar->vartype == NUMERICOID);
    uint32 scale = typeMod & 0xffff;
    uint32 precision = (typeMod >> 16) & 0xffff;
    if (precision > 18 && precision <= 38) {
        convert_short_numeric_to_int128_byscale(DatumGetNumeric(datumValue), scale, value);
    } else {
        /*
         * It is not posssible to reach this code.
         */
        ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_DATA_TYPE), errmodule(MOD_DFS), errmsg("The incorrect numeric format.")));
    }

    return value;
}

double datumGetFloat8ByVar(Datum datumValue, Var *colVar)
{
    double value;
    Oid datumType = colVar->vartype;

    if (FLOAT4OID == datumType) {
        value = (float8)DatumGetFloat4(datumValue);
    } else {
        value = DatumGetFloat8(datumValue);
    }
    return value;
}

char *datumGetStringByVar(Datum datumValue, Var *colVar)
{
    char *value = NULL;
    Oid datumType = colVar->vartype;
    int32 typeMod = colVar->vartypmod;

    /*
     * When the type is like char(20), it needs to add some blanks in the tail.
     */
    if (datumType == BPCHAROID) {
        Datum bpchar = DirectFunctionCall3(bpcharin, CStringGetDatum(TextDatumGetCString(datumValue)),
                                           ObjectIdGetDatum(InvalidOid), Int32GetDatum(typeMod));
        value = TextDatumGetCString(bpchar);
    } else {
        value = TextDatumGetCString(datumValue);
    }
    return value;
}

void list_delete_list(List **src, List *del)
{
    if (0 == list_length(*src) || 0 == list_length(del)) {
        return;
    }

    ListCell *lc = NULL;
    foreach (lc, del) {
        *src = list_delete(*src, lfirst(lc));
    }
}

char *pg_to_server_withfailure(char *src, int64 length, int32 encoding, int32 checkEncodingLevel, bool &meetError)
{
    char *ret = NULL;
    if (checkEncodingLevel == LOW_ENCODING_CHECK) {
        PG_TRY();
        {
            ret = pg_any_to_server(src, length, encoding);
        }
        PG_CATCH();
        {
            ret = src;
            errno_t rc = memset_s(ret, length, '?', length);
            securec_check(rc, "\0", "\0");
            meetError = true;
            FlushErrorState();
        }
        PG_END_TRY();
    } else {
        ret = pg_any_to_server(src, length, encoding);
    }

    return ret;
}

bool defColSatisfyPredicates(bool is_null, Datum datumValue, Var *colVar, List *predicate_list)
{
    bool filtered = false;
    switch (colVar->vartype) {
        case BOOLOID:
        case INT1OID:
        case INT2OID:
        case INT4OID:
        case INT8OID: {
            int64 data = 0;
            if (!is_null) {
                data = datumGetInt64ByVar(datumValue, colVar);
            }
            filtered = HdfsPredicateCheck<Int64Wrapper, int64>(is_null, data, predicate_list);
            break;
        }
        case FLOAT4OID:
        case FLOAT8OID: {
            double data = 0;
            if (!is_null) {
                data = datumGetFloat8ByVar(datumValue, colVar);
            }
            filtered = HdfsPredicateCheck<Float8Wrapper, double>(is_null, data, predicate_list);

            break;
        }
        case VARCHAROID:
        case CLOBOID:
        case TEXTOID:
        case BPCHAROID: {
            char *data = 0;
            if (!is_null) {
                data = datumGetStringByVar(datumValue, colVar);
            }
            filtered = HdfsPredicateCheck<StringWrapper, char *>(is_null, data, predicate_list);
            break;
        }
        case DATEOID:
        case TIMESTAMPOID: {
            Timestamp data = 0;
            if (!is_null) {
                data = DatumGetTimestamp(datumValue);
            }
            filtered = HdfsPredicateCheck<TimestampWrapper, Timestamp>(is_null, data, predicate_list);
            break;
        }
        case NUMERICOID: {
            uint32 precision = ((uint32)(colVar->vartypmod - VARHDRSZ) >> 16) & 0xffff;
            if (!is_null) {
                if (precision <= 18) {
                    int64 data = 0;
                    data = datumGetInt64ByVar(datumValue, colVar);
                    filtered = HdfsPredicateCheck<Int64Wrapper, int64>(is_null, data, predicate_list);
                } else {
                    int64 data = 0;

                    data = datumGetInt128ByVar(datumValue, colVar);
                    filtered = HdfsPredicateCheck<Int128Wrapper, int64>(is_null, data, predicate_list);
                }
            }

            break;
        }
        default: {
            /*
             * Never occur here.
             */
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                            errmsg("Data type %u has not been supported for predicate push down.", colVar->vartype)));
        }
    }
    return filtered;
}

/*
 * @hdfs
 * brief: Calculate the foreign table size.
 * input param @fileName: the file names of the foreign table;
 */
int64 GetForeignTableTotalSize(List *const fileName)
{
    int64 totalSize = 0;
    ListCell *fileCell = NULL;

    Assert(fileName != NULL);

    /* Iterate the fileName list to get each file size and add them one by one. */
    foreach (fileCell, fileName) {
        int64 size = 0;
        void *data = lfirst(fileCell);
        if (IsA(data, SplitInfo)) {
            SplitInfo *fileInfo = (SplitInfo *)data;
            size = fileInfo->ObjectSize;
        } else {
            /* for txt/csv format obs foreign table. */
            DistFdwFileSegment *fileSegment = (DistFdwFileSegment *)data;
            size = fileSegment->ObjectSize;
        }

        totalSize += size < 0 ? 0 : size;
    }
    return totalSize;
}

BlockNumber getPageCountForFt(void *additionalData)
{
    BlockNumber totalPageCount = 0;

    /*
     * Get table total size. The table may have many files. We add each file size together. File list in additionalData
     * comes from CN scheduler.
     */
    List *fileList = NIL;
    if (IsA(additionalData, SplitMap)) {
        SplitMap *splitMap = (SplitMap *)additionalData;
        fileList = splitMap->splits;
    } else {
        /* for dist obs foreign table. */
        DistFdwDataNodeTask *dnTask = (DistFdwDataNodeTask *)additionalData;
        fileList = dnTask->task;
    }
    double totalSize = GetForeignTableTotalSize(fileList);

    /*
     * description: BLSCKZ value may change
     */
    totalPageCount = (uint32)(totalSize + (BLCKSZ - 1)) / BLCKSZ;
    if (totalPageCount < 1) {
        totalPageCount = 1;
    }

    return totalPageCount;
}
