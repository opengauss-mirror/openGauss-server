/* ---------------------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996, 2003 VIA Networking Technologies, Inc.
 *
 *  dfs_query_reader.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/dfs/dfs_query_reader.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "access/dfs/dfs_common.h"
#include "access/dfs/dfs_insert.h"
#include "access/dfs/dfs_query.h"
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
#include "utils/int8.h"
#include "executor/executor.h"

/*
 * Parse the datum value from extern param according to the expr.
 *
 * @_in param expr: The extern param expression.
 * @_in param ps: PlanState from which we can get the value of ParamExternData.
 * @return Return the point to ParamExternData.
 */
static ParamExternData *HdfsGetParamExtern(Expr *expr, PlanState *ps)
{
    Param *expression = (Param *)expr;
    int thisParamId = expression->paramid;
    ParamListInfo paramInfo = ps->state->es_param_list_info;

    /*
     * PARAM_EXTERN parameters must be sought in ecxt_param_list_info.
     */
    if (paramInfo && thisParamId > 0 && thisParamId <= paramInfo->numParams) {
        ParamExternData *prm = &paramInfo->params[thisParamId - 1];

        if (NULL != prm && OidIsValid(prm->ptype)) {
            return prm;
        }
    }

    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmodule(MOD_DFS),
                    errmsg("no value found for parameter %d", thisParamId)));
    return NULL;
}

/*
 * @Description: Build the op expression according to the strategy and var.
 * @IN strategy: the operator type
 * @IN var: the column var
 * @Return: the op expression
 * @See also:
 */
static Expr *BuildExprByStrategy(HdfsQueryOperator strategy, Var *var)
{
    Expr *expr = NULL;
    switch (strategy) {
        case HDFS_QUERY_EQ: {
            expr = (Expr *)MakeOperatorExpression(var, BTEqualStrategyNumber);
            break;
        }
        case HDFS_QUERY_LT: {
            expr = (Expr *)MakeOperatorExpression(var, BTLessStrategyNumber);
            break;
        }
        case HDFS_QUERY_GT: {
            expr = (Expr *)MakeOperatorExpression(var, BTGreaterStrategyNumber);
            break;
        }
        case HDFS_QUERY_LTE: {
            expr = (Expr *)MakeOperatorExpression(var, BTLessEqualStrategyNumber);
            break;
        }
        case HDFS_QUERY_GTE: {
            expr = (Expr *)MakeOperatorExpression(var, BTGreaterEqualStrategyNumber);
            break;
        }
        default: {
            /* no process */
            break;
        }
    }

    return expr;
}

template <typename T, typename baseType>
bool HdfsScanPredicate<T, baseType>::BuildHdfsScanPredicateFromClause(Expr *expr, PlanState *ps, ScanState *scanstate,
                                            AttrNumber varNoPos, int predicateArrPos)
{
    Expr *rightop = NULL;
    Expr *leftop = NULL;
    Datum datumValue = (Datum)0;
    Oid datumType = InvalidOid;
    int32 typeMod = 0;
    bool runningTimeSet = false;

    if (IsA(expr, OpExpr)) {
        /* Here the leftop must be not null and is either RelabelType or Var type. */
        leftop = (Expr *)get_leftop(expr);
        if (leftop == NULL) {
            ereport(ERROR, (errmodule(MOD_DFS), errmsg("The leftop is null")));
        }

        if (IsA(leftop, RelabelType)) {
            leftop = ((RelabelType *)leftop)->arg;
        }
        Assert(IsA(leftop, Var));

        typeMod = ((Var *)leftop)->vartypmod;

        /* Rightop should be const.Has been checked before. */
        rightop = (Expr *)get_rightop(expr);
        if (rightop == NULL) {
            return runningTimeSet;
        }

#ifdef ENABLE_LLVM_COMPILE
	/* Build IR according to expr node. */
        if (CodeGenThreadObjectReady()) {
            (void)ForeignScanExprCodeGen(expr, NULL, &m_jittedFunc);
        }
#endif

        if (IsA(rightop, Const)) {
            datumValue = ((Const *)rightop)->constvalue;
            datumType = ((Const *)rightop)->consttype;
        } else if (IsA(rightop, RelabelType)) {
            rightop = ((RelabelType *)rightop)->arg;
            Assert(rightop != NULL);
            datumValue = ((Const *)rightop)->constvalue;
            datumType = ((Const *)rightop)->consttype;
        } else if (nodeTag(rightop) == T_Param && ((Param *)rightop)->paramkind == PARAM_EXTERN) {
            ParamExternData *prm = HdfsGetParamExtern(rightop, ps);
            datumValue = prm->value;
            datumType = prm->ptype;
        } else if (nodeTag(rightop) == T_Param && ((Param *)rightop)->paramkind == PARAM_EXEC) {
            RunTimeParamPredicateInfo *runTimeParamPredicate =
                (RunTimeParamPredicateInfo *)palloc0(sizeof(RunTimeParamPredicateInfo));
            Param *parameter = (Param *)rightop;
            runTimeParamPredicate->varNoPos = varNoPos;
            runTimeParamPredicate->paraExecExpr = ExecInitExpr(rightop, NULL);
            runTimeParamPredicate->opExpr = BuildExprByStrategy(m_strategy, (Var *)leftop);
            runTimeParamPredicate->typeMod = typeMod;
            runTimeParamPredicate->datumType = parameter->paramtype;
            runTimeParamPredicate->paramPosition = predicateArrPos;
            runTimeParamPredicate->varTypeOid = ((Var *)leftop)->vartype;
            scanstate->runTimeParamPredicates = lappend(scanstate->runTimeParamPredicates, runTimeParamPredicate);

            runningTimeSet = true;
        } else if (nodeTag(rightop) == T_Param) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                            errmsg("Not support pushing predicate with sublink param now!")));
        } else {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                            errmsg("Not support pushing predicate with non-const")));
        }
    } else if (IsA(expr, NullTest)) {
    } else {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                        errmsg("We only support pushing down opExpr and null test predicates.")));
    }

    /*
     * If the predicate is running time predicate, do not init it here.
     * Must maintain the predicate in the future, if we add external logic.
     */
    if (!runningTimeSet) {
        Init(datumValue, datumType, typeMod);
    }

    return runningTimeSet;
}

/*
 * Get the HdfsQueryOperator according to the opratorName, here we only support seven types of operators and return
 * -1 for the unsupported operator.
 *
 * @_in param operatorName: the name of the operator like '<>'.
 * @return Return an HdfsQueryOperator of which the meaning can be found in the defination of HdfsQueryOperator.
 */
HdfsQueryOperator HdfsGetQueryOperator(const char *OpName)
{
    HdfsQueryOperator hdfsOpName = HDFS_QUERY_INVALID;
    int32 OpNameIndex = 0;
    int32 OpNameCount;
    static const char *nameMappings[] = { "=", "<", ">", "<=", ">=", "<>", "!=" };

    OpNameCount = sizeof(nameMappings) / sizeof(nameMappings[0]);
    for (OpNameIndex = 0; OpNameIndex < OpNameCount; OpNameIndex++) {
        const char *pgOpName = nameMappings[OpNameIndex];
        if (strncmp(pgOpName, OpName, NAMEDATALEN) == 0) {
            hdfsOpName = (HdfsQueryOperator)OpNameIndex;
            break;
        }
    }

    return hdfsOpName;
}

/*
 * Search the pg_operate catalog by operation oid, and return the operator strategy.
 *
 * @_in param opno: the oid of the operator.
 * @return Return an HdfsQueryOperator: HDFS_QUERY_INVALID means the current strategy is not supported,
 *     other value's meaning can be found in the defination of HdfsQueryOperator.
 */
static HdfsQueryOperator GetHdfsScanStrategyNumber(Oid opno)
{
    HeapTuple tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmodule(MOD_DFS),
                        errmsg("could not find operator by oid %u", opno)));
    }
    Form_pg_operator fpo = (Form_pg_operator)GETSTRUCT(tuple);

    HdfsQueryOperator strategy_number = HDFS_QUERY_INVALID;
    strategy_number = HdfsGetQueryOperator(NameStr(fpo->oprname));

    ReleaseSysCache(tuple);

    return strategy_number;
}

/*
 * @Description: Create one Restriction info for one partition, and also bind PartColValue
 * in current split.
 * @in rel: target relation
 * @in partVar: partition column's var
 * @in scanrelid: Index relid of the target relation
 * @in partExpr: string format of this partition directory
 * @out/in @si: current splitinfo
 * @in equalExpr: the prepared equal expression.
 * @return return one partition restriction.
 */
static Node *CreateOnePartitionRestriction(Relation rel, Var *partVar, Index scanrelid, const char *partExpr,
                                           SplitInfo *si, Expr *equalExpr)
{
    Node *baseRestriction = NULL;
    Datum datumValue;
    const char *pos = strchr(partExpr, '=');

    if (pos == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_DFS),
                        errmsg("Invalid partition expression:%s", partExpr)));
    }

    const char *partColValue = pos + 1;
    /*
     * If the value is NULL, means that no existence of predicate restriction.
     */
    if (partVar == NULL) {
        si->partContentList = lappend(si->partContentList, makeString(UriDecode(partColValue)));
        return NULL;
    }

    /*
     * First check if the partitioning column with value *NULL* and build
     * IS_NULL restriction check
     */
    if (strncmp(partColValue, DEFAULT_HIVE_NULL, DEFAULT_HIVE_NULL_LENGTH) == 0) {
        baseRestriction = BuildNullTestConstraint(partVar, IS_NULL);
    } else {
        Assert(equalExpr != NULL);
        datumValue = GetDatumFromString(partVar->vartype, partVar->vartypmod, UriDecode(partColValue));
        BuildConstraintConst(equalExpr, datumValue, false);
        baseRestriction = (Node *)equalExpr;
    }

    si->partContentList = lappend(si->partContentList, makeString(UriDecode(partColValue)));

    return baseRestriction;
}

/*
 * Sort the columns to read order by selectivity in asce.
 */
static void SortReadColsByWeight(dfs::reader::ReaderState *readerState, double *selectivity)
{
#define DFS_SWAP(T, A, B) do { \
    T t_ = (A);       \
    A = (B);          \
    B = t_;           \
} while (0)

    if (selectivity == NULL) {
        return;
    }

    for (uint32 i = 0; i < readerState->relAttrNum; i++) {
        double minWeight = 1.0;
        uint32 minCol = 0;
        int flag = 0;

        for (uint32 j = i; j < readerState->relAttrNum; j++) {
            if (selectivity[j] != 0 && selectivity[j] < minWeight) {
                minWeight = selectivity[j];
                minCol = j;
                flag = 1;
            }
        }

        if (flag == 1) {
            DFS_SWAP(float, selectivity[minCol], selectivity[i]);
            DFS_SWAP(uint32, readerState->orderedCols[minCol], readerState->orderedCols[i]);
        } else {
            break;
        }
    }
}

/*
 * @Description: Pruning unnecessary splits(partition) based on the restriction
 * of scan clauses (Static Partition Pruning).
 * @in ss: A ScanState struct.
 * @in rs: A ReaderState struct.
 * @in col_var_list: The var list of all the target columns and the
 * restriction columns.
 * @return return the remain file list.
 */
static List *PruningUnnecessaryPartitions(ScanState *ss, dfs::reader::ReaderState *rs, List *col_var_list)
{
    Relation rel = ss->ss_currentRelation;
    rs->partList = ((ValuePartitionMap *)rel->partMap)->partList;
    List *splitList = rs->splitList;
    List *prunedSplitList = NULL;
    int total_partitions = list_length(rs->splitList);
    ListCell *c = NULL;
    int pruned_partitions = 0;
    bool pruned = false;
    Index scanrelid = ((DfsScan *)ss->ps.plan)->scanrelid;
    char *partsigs = (char *)palloc(MAX_PARSIGS_LENGTH);
    char *curPartExpr = (char *)palloc(MAX_PARSIG_LENGTH);
    int32_t fileNameOffset = strlen(getDfsStorePath(rel)->data) + 1;
    int partNum = list_length(rs->partList);
    Expr **equalExpr = (Expr **)palloc0(sizeof(Expr *) * partNum);
    Var **partVars = (Var **)palloc0(sizeof(Var *) * partNum);

    /* prepare the equal expressions for all the partition columns. */
    for (int i = 0; i < partNum; i++) {
        AttrNumber partColId = list_nth_int(rs->partList, i);
        partVars[i] = GetVarFromColumnList(col_var_list, partColId);
        if (partVars[i] != NULL) {
            equalExpr[i] = (Expr *)MakeOperatorExpression(partVars[i], BTEqualStrategyNumber);
        }
    }

    errno_t rc;
    /* Check each SplitInfo element from raw scanning list */
    foreach (c, splitList) {
        SplitInfo *si = (SplitInfo *)lfirst(c);
        char *curpos = NULL;
        Node *baseRestriction = NULL;
        List *partRestriction = NIL;
        char *fileName = si->filePath + fileNameOffset;

        rc = memset_s(partsigs, MAX_PARSIGS_LENGTH, 0, MAX_PARSIGS_LENGTH);
        securec_check(rc, "\0", "\0");

        /* create parsigs without last '/' */
        int64 baseNameLen = basename_len(fileName, '/');
        if (unlikely(baseNameLen < 0)) {
            continue;
        }
        rc = strncpy_s(partsigs, MAX_PARSIGS_LENGTH, fileName, baseNameLen);
        CHECK_PARTITION_SIGNATURE(rc, fileName);
        curpos = partsigs;

        /* Process each partition level */
        for (int i = 0; i < partNum; i++) {
            /* Process last partition */
            if (i == partNum - 1) {
                baseRestriction = CreateOnePartitionRestriction(rel, partVars[i], scanrelid, curpos, si, equalExpr[i]);
                if (baseRestriction != NULL) {
                    partRestriction = lappend(partRestriction, baseRestriction);
                }

                break;
            }

            /* Buffer for current partition expression */
            rc = memset_s(curPartExpr, MAX_PARSIG_LENGTH, 0, MAX_PARSIG_LENGTH);
            securec_check(rc, "\0", "\0");
            int slashPos = (int)strpos(curpos, "/");
            if (unlikely(slashPos < 0)) {
                continue;
            }
            rc = strncpy_s(curPartExpr, MAX_PARSIG_LENGTH, curpos, slashPos);
            CHECK_PARTITION_SIGNATURE(rc, curpos);

            /*
             * Create predicate restriction for current value-partition level
             * elimination
             */
            baseRestriction = CreateOnePartitionRestriction(rel, partVars[i], scanrelid, curPartExpr, si, equalExpr[i]);
            if (baseRestriction != NULL) {
                partRestriction = lappend(partRestriction, baseRestriction);
            }

            curpos = (char *)curpos + strlen(curPartExpr) + 1;
        }

        /*
         * Pruning unncessary partitions by evaluating parititon predicates
         * through the primitive restrictions.
         */
        pruned = predicate_refuted_by(partRestriction, rs->queryRestrictionList, true);

        ereport(DEBUG2,
                (errmodule(MOD_DFS), errmsg("partition:%s should be pruned[%s]", partsigs, pruned ? "YES" : "NO")));

        if (pruned) {
            pruned_partitions++;
        } else {
            prunedSplitList = lappend(prunedSplitList, si);
        }

        if (partRestriction != NIL) {
            list_free(partRestriction);
            partRestriction = NIL;
        }
    }

    /* count the number of partitions which is pruned static */
    rs->staticPruneFiles += pruned_partitions;
    ereport(DEBUG1, (errmodule(MOD_DFS),
                     errmsg("Pruning partitions on relation %s with SPP optimization *%s* pruned:%d, total:%d",
                            RelationGetRelationName(ss->ss_currentRelation),
                            u_sess->attr.attr_sql.enable_valuepartition_pruning ? "ON" : "OFF", pruned_partitions,
                            total_partitions)));

    pfree_ext(partsigs);
    pfree_ext(curPartExpr);
    pfree_ext(equalExpr);
    pfree_ext(partVars);

    /* If SPP is disabled, we return the original splitlist directly */
    if (!u_sess->attr.attr_sql.enable_valuepartition_pruning) {
        return splitList;
    }

    return prunedSplitList;
}

/*
 * brief: The initialization process of partition table when scan begins.
 * input param @readerState: Execution state for specific dfs scan.
 */
static void PartitionTblInit(dfs::reader::ReaderState *readerState)
{
    uint32 *partColNoArr = NULL;
    uint32 adapt = 0;
    uint32 i = 0;
    List *partList = readerState->partList;

    /* Array to store mapping from relation column no to orc reader column index. */
    readerState->colNoMapArr = (uint32 *)palloc0(sizeof(uint32) * readerState->relAttrNum);

    /* Array to store partition column value if exists. */
    readerState->partitionColValueArr = (char **)palloc0(sizeof(char *) * readerState->relAttrNum);

    /* Fill the partColNoArr according to partList. */
    readerState->partNum = list_length(partList);

    /*
     * partColNoArr is an array to store partition column info which is used to indicate
     * which column is partition column(1 => partition column, 0 => non-partition column)
     * when building colNoMapArr and removing partition column from column list.
     * Remember to pfree it in the end of beginScan.
     */
    partColNoArr = (uint32 *)palloc0(sizeof(uint32) * readerState->relAttrNum);
    for (i = 0; i < readerState->partNum; i++) {
        int IndexColNum = list_nth_int(partList, i);
        partColNoArr[IndexColNum - 1] = 1;
    }

    /*
     * Construct the colNoMapArr, when meet partition column all the columns after it need to decrease
     * its mapping number. For example, the normal column array is [1,2,3,4,5,6,7], partition column is
     * [3,5], then the colNoMapArr will be [1,2,3,3,4,4,5].
     */
    for (i = 0; i < readerState->relAttrNum; i++) {
        readerState->colNoMapArr[i] = i - adapt + 1;
        if (partColNoArr[i] == 1) {
            adapt++;
        }
    }

    pfree_ext(partColNoArr);
}

/*
 * @Description: Since the params of the dynamic restrictions will not be set here, so we pick them out.
 * @IN opExpressionList: the complete restrictions' list.
 * @Return: the filtered restrictions' list
 * @See also:
 */
static List *ExtractNonParamRestriction(List *opExpressionList)
{
    ListCell *lc = NULL;
    Expr *expr = NULL;
    List *retRestriction = NIL;

    foreach (lc, opExpressionList) {
        expr = (Expr *)lfirst(lc);
        if (IsA(expr, OpExpr)) {
            Node *leftop = get_leftop(expr);
            Node *rightop = get_rightop(expr);
            if (rightop == NULL) {
                continue;
            }

            if ((IsVarNode(leftop) && IsParamConst(rightop)) || (IsVarNode(rightop) && IsParamConst(leftop))) {
                continue;
            }
        }
        retRestriction = lappend(retRestriction, expr);
    }

    return retRestriction;
}

/*
 * Extract the attribute type, attribute no and operator from the expression.
 * @_in param expr: The expression to be parsed.
 * @_out param strategy: The operator strategy of the expression.
 * return the var in the predicate.
 */
static Var *GetHdfsPrediacateVarType(Expr *expr, HdfsQueryOperator &strategy)
{
    Expr *leftop = NULL;
    Expr *arg = NULL;
    Oid opno = InvalidOid;
    Var *var = NULL;

    if (IsA(expr, OpExpr)) {
        opno = ((OpExpr *)expr)->opno;

        /* Get strategy number. */
        strategy = GetHdfsScanStrategyNumber(opno);

        /* Leftop should be var. Has been checked */
        leftop = (Expr *)get_leftop(expr);
        if (leftop && IsA(leftop, RelabelType)) {
            leftop = ((RelabelType *)leftop)->arg;
        }
        if (leftop == NULL) {
            ereport(ERROR, (errmodule(MOD_DFS), errmsg("The leftop is null")));
        }

        var = (Var *)leftop;
    } else if (IsA(expr, NullTest)) {
        /* Leftop should be var. Has been checked */
        arg = ((NullTest *)expr)->arg;
        if (arg && IsA(arg, RelabelType)) {
            arg = ((RelabelType *)arg)->arg;
        }
        if (arg == NULL) {
            ereport(ERROR, (errmodule(MOD_DFS), errmsg("The arg is null")));
        }

        var = (Var *)arg;

        if (((NullTest *)expr)->nulltesttype == IS_NULL) {
            strategy = HDFS_QUERY_ISNULL;
        } else {
            strategy = HDFS_QUERY_ISNOTNULL;
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                        errmsg("We only support pushing down opExpr and null test predicate.")));
    }

    return var;
}

/*
 * brief: Build Predicate for pushdown of dfs scan.
 * input param @strategy:the operator strategy of the expression;
 * input param @var: the information of the attribute;
 * input param @expr: expression which has been filtered by the optimizer;
 * input param @ps: execution state for specific scan;
 * input_out param @readerState: includes colNoMap to adjust the column
 *      index with ORC file and store the predicate for each column;
 */
template <class typeClass, typename baseType>
static void BuildHdfsPredicate(HdfsQueryOperator strategy, Var *var, Expr *expr, PlanState *ps,
                               dfs::reader::ReaderState *readerState)
{
    List **hdfsScanPredicateArr = readerState->hdfsScanPredicateArr;
    uint32 *colNoMap = readerState->colNoMapArr;
    AttrNumber attNo = var->varattno;
    Oid attType = var->vartype;
    Oid collation = var->varcollid;
    int predicateArrPos = 0;
    bool runningTimeSet = false;
    if (colNoMap == NULL) {
        predicateArrPos = attNo - 1;
    } else {
        predicateArrPos = colNoMap[attNo - 1] - 1;
    }

    HdfsScanPredicate<typeClass, baseType> *predicate =
        New(CurrentMemoryContext) HdfsScanPredicate<typeClass, baseType>(attNo, attType, strategy, collation,
                                                                         var->vartypmod);

    runningTimeSet =
        predicate->BuildHdfsScanPredicateFromClause(expr, ps, readerState->scanstate, predicateArrPos,
                                                    list_length(hdfsScanPredicateArr[predicateArrPos]) + 1);
    hdfsScanPredicateArr[predicateArrPos] = lappend(hdfsScanPredicateArr[predicateArrPos], predicate);

    /* For the equal operator, generate the bloomfilter used to check stride skipping. */
    if (u_sess->attr.attr_sql.enable_bloom_filter && !runningTimeSet && strategy == HDFS_QUERY_EQ &&
        SATISFY_BLOOM_FILTER(attType)) {
        filter::BloomFilter *bloomFilter = filter::createBloomFilter(attType, var->vartypmod, collation,
                                                                     EQUAL_BLOOM_FILTER,
                                                                     DEFAULT_ORC_BLOOM_FILTER_ENTRIES, false);
        if (SECUREC_UNLIKELY(bloomFilter == NULL)) {
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS), errmsg("bloomFilter create failed")));
        }
        bloomFilter->addValue(predicate->m_argument->getValue());
        readerState->bloomFilters[attNo - 1] = bloomFilter;
    }
}

/*
 * brief: Initialize the hdfs predicate which is going to push down.
 * input param @readerState: The state of the reader which includes the informations needed.
 * input param @ps: The state of foreign scan which includes the all the information about plan.
 * input param @hdfsQual: The list of predicate pushed down.
 */
static void InitHdfsScanPredicateArr(dfs::reader::ReaderState *readerState, PlanState *ps, List *hdfsQual)
{
    ListCell *lc = NULL;
    HdfsQueryOperator strategy = HDFS_QUERY_INVALID;

    foreach (lc, hdfsQual) {
        Expr *expr = (Expr *)lfirst(lc);
        Var *var = GetHdfsPrediacateVarType(expr, strategy);

        switch (var->vartype) {
            case BOOLOID:
            case INT1OID:
            case INT2OID:
            case INT4OID:
            case INT8OID: {
                BuildHdfsPredicate<Int64Wrapper, int64>(strategy, var, expr, ps, readerState);
                break;
            }
            case NUMERICOID: {
                /*
                 * PushDownSupportVar() make sure that precision <= 38 for numeric
                 */
                uint32 precision = uint32(((uint32)(var->vartypmod - VARHDRSZ) >> 16) & 0xffff);
                if (precision <= 18) {
                    BuildHdfsPredicate<Int64Wrapper, int64>(strategy, var, expr, ps, readerState);
                } else {
                    BuildHdfsPredicate<Int128Wrapper, int128>(strategy, var, expr, ps, readerState);
                }
                break;
            }
            case FLOAT4OID:
            case FLOAT8OID: {
                BuildHdfsPredicate<Float8Wrapper, double>(strategy, var, expr, ps, readerState);
                break;
            }
            case VARCHAROID:
            case CLOBOID:
            case TEXTOID:
            case BPCHAROID: {
                BuildHdfsPredicate<StringWrapper, char *>(strategy, var, expr, ps, readerState);
                break;
            }
            case DATEOID:
            case TIMESTAMPOID: {
                BuildHdfsPredicate<TimestampWrapper, Timestamp>(strategy, var, expr, ps, readerState);
                break;
            }
            default: {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                                errmsg("Data type %u has not been supported for predicate push down.", var->vartype)));
            }
        }
    }
}

/*
 * Construct three arraies which indicate the required columns, target columns and
 * restriction columns by each.
 * _out_param readerState: The state for reading.
 * _in_param qual: The predicates pushed down to reader.
 * _in_param columnList: The list of all the required columns.
 * _in_param targetList: The list of the target columns.
 * _in_param restrictColList: The list of the restriction columns.
 */
static void InitRequiredCols(dfs::reader::ReaderState *readerState, List *qual, List *columnList, List *targetList,
                             List *restrictColList)
{
    ListCell *lc = NULL;
    Var *variable = NULL;
    readerState->isRequired = (bool *)palloc0(sizeof(bool) * readerState->relAttrNum);
    readerState->targetRequired = (bool *)palloc0(sizeof(bool) * readerState->relAttrNum);
    readerState->restrictRequired = (bool *)palloc0(sizeof(bool) * readerState->relAttrNum);

    foreach (lc, columnList) {
        variable = (Var *)lfirst(lc);
        readerState->isRequired[variable->varattno - 1] = true;
    }

    foreach (lc, targetList) {
        variable = (Var *)lfirst(lc);
        readerState->targetRequired[variable->varattno - 1] = true;
    }

    /* Add the restrict columns which can not be pushed down into the target list. */
    if (list_length(qual) > 0) {
        List *usedList = NIL;
        foreach (lc, qual) {
            Node *clause = (Node *)lfirst(lc);
            List *clauseList = NIL;

            /* recursively pull up any columns used in the restriction clause */
            clauseList = pull_var_clause(clause, PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS);

            usedList = MergeList(usedList, clauseList, readerState->relAttrNum);
            list_free_ext(clauseList);
        }
        foreach (lc, usedList) {
            variable = (Var *)lfirst(lc);
            readerState->targetRequired[variable->varattno - 1] = true;
        }
        list_free_ext(usedList);
    }

    foreach (lc, restrictColList) {
        variable = (Var *)lfirst(lc);
        readerState->restrictRequired[variable->varattno - 1] = true;
    }
}

void FillReaderState(dfs::reader::ReaderState *readerState, ScanState *ss, DfsPrivateItem *item, Snapshot snapshot)
{
    uint32 i = 0;
    Plan *plan = ss->ps.plan;
    Assert(readerState != NULL);
    if (readerState->persistCtx == NULL) {
        readerState->persistCtx = AllocSetContextCreate(CurrentMemoryContext, "dfs reader context",
                                                        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                        ALLOCSET_DEFAULT_MAXSIZE);
    }
    readerState->rescanCtx = AllocSetContextCreate(CurrentMemoryContext, "dfs rescan context", ALLOCSET_DEFAULT_MINSIZE,
                                                   ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    /* To indicate which column will be used in the current query. */
    readerState->relAttrNum = ss->ss_currentRelation->rd_att->natts;
    readerState->queryRestrictionList = ExtractNonParamRestriction(item->opExpressionList);
    readerState->runtimeRestrictionList = NIL;
    readerState->staticPruneFiles = 0;
    readerState->dynamicPrunFiles = 0;
    readerState->bloomFilterRows = 0;
    readerState->bloomFilterBlocks = 0;
    readerState->minmaxFilterRows = 0;

    /* init min/max statistics info */
    readerState->minmaxCheckFiles = 0;
    readerState->minmaxFilterFiles = 0;
    readerState->minmaxCheckStripe = 0;
    readerState->minmaxFilterStripe = 0;
    readerState->minmaxCheckStride = 0;
    readerState->minmaxFilterStride = 0;

    readerState->orcMetaCacheBlockCount = 0;
    readerState->orcMetaLoadBlockCount = 0;
    readerState->orcDataCacheBlockCount = 0;
    readerState->orcDataLoadBlockCount = 0;
    readerState->orcMetaCacheBlockSize = 0;
    readerState->orcMetaLoadBlockSize = 0;
    readerState->orcDataCacheBlockSize = 0;
    readerState->orcDataLoadBlockSize = 0;

    readerState->currentFileID = 0;
    readerState->currentFileSize = 0;
    readerState->localBlock = 0;
    readerState->remoteBlock = 0;
    readerState->nnCalls = 0;
    readerState->dnCalls = 0;
    readerState->fdwEncoding = INVALID_ENCODING;
    readerState->checkEncodingLevel = NO_ENCODING_CHECK;
    readerState->incompatibleCount = 0;
    readerState->dealWithCount = 0;
    readerState->scanstate = ss;
    readerState->snapshot = (snapshot == NULL) ? GetActiveSnapshot() : snapshot;
    Assert(readerState->snapshot != NULL);
    Assert(ss != NULL);
    ss->runTimePredicatesReady = false;
    ss->runTimeParamPredicates = NIL;
    readerState->orderedCols = (uint32 *)palloc(sizeof(uint32) * readerState->relAttrNum);
    for (i = 0; i < readerState->relAttrNum; i++) {
        readerState->orderedCols[i] = i;
    }
    InitRequiredCols(readerState, plan->qual, item->columnList, item->targetList, item->restrictColList);
    readerState->allColumnList = item->columnList;

    /* Allocate and initialize the variables if the table is partitioned. */
    if (((Scan *)plan)->isPartTbl) {
        if (RelationIsForeignTable(ss->ss_currentRelation)) { /* value partition for hdfs foreign table */
            readerState->partList = item->partList;
            PartitionTblInit(readerState);
        } else { /* value partition for hdfs table */
            List *prunedSplitList = NULL;

            /* try to prune necessary partitions here */
            readerState->partList = item->partList;
            prunedSplitList = PruningUnnecessaryPartitions(ss, readerState, item->columnList);

            /* update scan list */
            readerState->splitList = prunedSplitList;
            PartitionTblInit(readerState);
        }
    } else {
        readerState->partNum = 0;
        readerState->partList = NIL;
        readerState->colNoMapArr = NULL;
        readerState->partitionColValueArr = NULL;
    }

    /*
     * Because the ORC file dose not include partition column, not only must correct this
     * hdfsScanPredicateArr number, but also call PartitionTblInit function.
     * description: The partition table is CU format, do not repair this hdfsScanPredicateArr number
     * and do not call PartitionTblInit function.
     */
    readerState->hdfsScanPredicateArr =
        (List **)palloc0(sizeof(List *) * (readerState->relAttrNum - readerState->partNum));
    if (u_sess->attr.attr_sql.enable_hdfs_predicate_pushdown && list_length(item->hdfsQual) > 0) {
        readerState->bloomFilters =
            (filter::BloomFilter **)palloc0(sizeof(filter::BloomFilter *) * readerState->relAttrNum);
        InitHdfsScanPredicateArr(readerState, &(ss->ps), item->hdfsQual);
        SortReadColsByWeight(readerState, item->selectivity);
    }
}
