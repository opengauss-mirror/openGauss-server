/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * sqladvisor.h
 *		prototypes for sqladvisor.c.
 * 
 *
 * IDENTIFICATION
 *      src/include/commands/sqladvisor.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SQLADVISOR_H
#define SQLADVISOR_H

#include "lib/stringinfo.h"
#include "commands/dbcommands.h"
#include "nodes/parsenodes.h"
#include "utils/palloc.h"
#include "tcop/dest.h"

#define GWC_NUM_OF_BUCKETS (64)
#define GWC_HTAB_SIZE (64)
#define TOTAL_COMBINATION_THRESHOLD (100000)
#define DEFAULT_TOP_ATTR_NUMBER (5)
#define MIN_TOP_ATTR_NUMBER (2)
#define MAX_COMBINATION_NUM (100)
#define FILETER_ATTR_DISTRIBUTION_NUM_THRESHOLD (10)
#define SORT_ATTR_THRESHOLD (2)
#define FILETER_ATTR_NUM (2)
#define MAX_FINDELIST_SAME_TABLE_NUM (2)

/* this check function can only be used to check online is running or not. */
inline bool isOnlineRunningOn()
{
    /* if is same as 1 then set 1 and return true, if is different then do nothing and return false */
    uint32 expect = 1;
    return pg_atomic_compare_exchange_u32((volatile uint32*)&g_instance.adv_cxt.isOnlineRunning, &expect, 1);
}

/* this check function can only be used to check GWC is using or not. */
inline bool isUsingGWCOn()
{
    /* if is same as 1 then set 1 and return true, if is different then do nothing and return false */
    uint32 expect = 1;
    return pg_atomic_compare_exchange_u32((volatile uint32*)&g_instance.adv_cxt.isUsingGWC, &expect, 1);
}

typedef struct {
    List* ctes;   /* List of CommonTableExpr nodes */
    /* Remaining fields are used only when deparsing a Plan tree: */
    Plan* plan;       /* immediate parent of current expression */
    List* ancestors;            /* ancestors of plan */
    Plan* outerPlan; /* outer subplan state, or NULL if none */
    Plan* innerPlan; /* inner subplan state, or NULL if none */
    List* outerTlist;          /* referent for OUTER_VAR Vars */
    List* innerTlist;          /* referent for INNER_VAR Vars */
    List* indexTlist;          /* referent for INDEX_VAR Vars */
#ifdef PGXC
    bool remotequery; /* deparse context for remote query */
#endif
} RecursiveExtractNamespace;

/* Context info needed for invoking a recursive querytree display routine */
typedef struct {
    List* namespaces;         /* List of RecursiveExtractNamespace nodes */
    List* rtable;             /* List of RangeTblEntry nodes */
    List* subplans;
    double rowsWeight;
    List* args;
    List* expr;
    List* exprs;
} RecursiveExtractContext;

typedef struct {
    Bitmapset* virtualTables;
    Cost cost;
} VirtualTableGroupInfo;

typedef struct {
    Oid oid;
    int natts;
    AttrNumber* attrNums;
    List* attrNames;
    List* joinQuals;
    List* quals;

    int joinCount;
    int groupbyCount;
    int qualCount;
    double groupbyWeight;
    double weight;
} VirtualAttrGroupInfo;

typedef struct {
    Oid oid;
    char* tableName;
    List* totalCandidateAttrs;
    List* originDistributionKey;
    int tableGroupOffset;
    List* selecteCandidateAttrs;
    VirtualAttrGroupInfo* currDistributionKey;
    bool marked;
    bool isAssignRelication;
    bool enableAdvise;
} AdviseTable;

/* copy from catalog */
typedef struct {
    List* activeSearchPath;
    Oid activeCreationNamespace;
    bool activeTempCreationPending;
    List* baseSearchPath;
    Oid baseCreationNamespace;
    bool baseTempCreationPending;
    Oid namespaceUser;
    bool baseSearchPathValid;
    List* overrideStack;
    bool overrideStackValid;
} AdviseSearchPath;

typedef struct {
    char* query;
    ParamListInfo boundParams;
    int cursorOptions;
    AdviseSearchPath* searchPath;
    int frequence;
    Cost originCost;
    List* tableOids;

    Bitmapset* referencedTables;
    List* virtualTableGroupInfos;
    bool isUse;
} AdviseQuery;

typedef struct {
    char* schemaName;
    char* tableName;
    char* attrName;
    char* operatorName;
    int count;
} AnalyzedResultview;

typedef struct {
    char* dbName;
    char* schemaName;
    char* tableName;
    char* distributioType;
    char* distributionKey;
    TimestampTz startTime;
    TimestampTz endTime;
    double costImpove;
    char* comment;
} DistributionResultview;

typedef struct {
    Cost minCost;
    Cost originCost;
    List* bestCombineResult;

    int currentCombineIndex; 
    List* replicationTableList;
    List* candicateCombines;
    List* candicateQueries;
    HTAB* candicateTables;
} AdviseGroup;

typedef struct {
    Oid currOid;
    Oid joinWithOid;
    int joinNattr;
    Node* expr;
    double joinWeight;
} JoinCell;

typedef struct {
    JoinCell** data; 
    int size; 
    int capacity;
} JoinMaxHeap;

typedef struct {
    DestReceiver pub;
    MemoryContext mxct;
    List *tuples;
    Oid *atttypids;   /* for sublink in user-defined variables */
    Oid *collids;      /* for variables collation ids*/
    bool *isnulls;     /* for sublink in user-defined variables */
} StmtResult;

typedef struct {
    List* currentSearchPathResult;
    List* allSearchPathResults;
    List* replicationTableList;
    double tempWeight;
    double maxWeight;
    bool onlyBestResult;
    long tableNum;
    JoinMaxHeap* maxHeap;
    int topN;
} MaxNeighborVariable;

typedef struct {
    uint32 querylength;
    char *queryString;
} SQLStatementKey;

typedef struct {
    SQLStatementKey key;
    List* paramList;
} SQLStatementEntry;

typedef struct {
    ParamListInfo boundParams;
    int cursorOptions;
    uint32 freqence;
    AdviseSearchPath* searchPath;
} SQLStatementParam;

typedef struct {
    bool isHasTable;
    bool isHasTempTable;
    bool isHasFunction;
} TableConstraint;

/* Hook for index_advisor */                                                       
typedef void (*get_info_from_plan_hook_type) (Node* node, List* rtable);

extern void analyzeQuery(AdviseQuery* adviseQuery, bool isFistAnalyze, Cost* cost);
extern bool checkAdivsorState();
extern bool checkCommandTag(const char* commandTag);
extern bool checkPlan(List* stmtList);
extern bool checkParsetreeTag(Node* parsetree);
extern bool checkSPIPlan(SPIPlanPtr plan);
extern void checkSessAdvMemSize();
extern void checkQuery(List* querytreeList, TableConstraint* tableConstraint);
extern void checkRtable(List* rtable, TableConstraint* tableConstraint);
extern AdviseSearchPath* copyAdviseSearchPath(AdviseSearchPath* src);
extern List* copyOverrideStack(List* src);
extern void collectDynWithArgs(const char *src, ParamListInfo srcParamLI, int cursor_options);
extern bool compareAttrs(AttrNumber* attnumA, int numA, AttrNumber* attnumB, int numB);
extern char* concatAttrNames(List* attrNames, int natts);
extern ParamListInfo copyDynParam(ParamListInfo srcParamLI);
extern char* printDistributeKey(Oid relOid);
extern void extractNode(Plan* plan, List* ancestors, List* rtable, List* subplans);
extern VirtualAttrGroupInfo* getVirtualAttrGroupInfo(AttrNumber* var_attno, int natts, List* attrs_list);
extern void initCandicateTables(MemoryContext context, HTAB** hash_table);
extern void initTableConstraint(TableConstraint* tableConstraint);
extern bool runWithHeuristicMethod();
extern bool runWithCostMethod();
extern uint32 SQLStmtHashFunc(const void *key, Size keysize);
extern Datum init(PG_FUNCTION_ARGS);
extern Datum set_cost_params(PG_FUNCTION_ARGS);
extern Datum set_weight_params(PG_FUNCTION_ARGS);
extern Datum assign_table_type(PG_FUNCTION_ARGS);
extern Datum clean(PG_FUNCTION_ARGS);
extern Datum clean_workload(PG_FUNCTION_ARGS);
extern Datum analyze_query(PG_FUNCTION_ARGS);
extern Datum get_analyzed_result(PG_FUNCTION_ARGS);
extern Datum get_distribution_key(PG_FUNCTION_ARGS);
extern Datum run(PG_FUNCTION_ARGS);
extern Datum start_collect_workload(PG_FUNCTION_ARGS);
extern Datum end_collect_workload(PG_FUNCTION_ARGS);
extern Datum analyze_workload(PG_FUNCTION_ARGS);
extern bool checkSelectIntoParse(SelectStmt* stmt);
extern PLpgSQL_datum* copypPlpgsqlDatum(PLpgSQL_datum* datum);
extern StmtResult *execute_stmt(const char *query_string, bool need_result = false);

#endif /* SQLADVISOR_H */
