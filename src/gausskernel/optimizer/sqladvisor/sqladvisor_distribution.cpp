/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * sqladvisor_distribution.cpp
 *		sqladvisor of distribution includes heurisitic model and whatif model.
 * 
 *
 * IDENTIFICATION
 *      src/gausskernel/optimizer/commands/sqladvisor_distribution.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "nodes/pg_list.h"
#include "funcapi.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "optimizer/randomplan.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "commands/sqladvisor.h"
#include "commands/dbcommands.h"
#include "nodes/makefuncs.h"
#include "utils/typcache.h"
#include "catalog/pg_statistic.h"
#include "catalog/indexing.h"
#include "utils/fmgroids.h"
#include "executor/lightProxy.h"
#include "pgxc/route.h"
#include "lib/stringinfo.h"

static List* addAdviseGroup(List* groups, AdviseGroup* adviseGroup);
static void addTableOids(List* tableOids, Oid relid);
static AttrNumber* copyAttrNums(int natts, AttrNumber* src);
static void copyAdviseTable(AdviseTable* dest, AdviseTable* src);
static List* copyVirtualAttrGroupInfo(List* src);
static List* copyAttrNames(List* src);
static List* copyJoinQuals(List* src);
static AdviseQuery* copyAdviseQuery(AdviseQuery* src);
static List* copyTableOids(List* src);
static AdviseGroup* concatAdviseGroup(List* groups);
static List* copyList(List* src);
static List* copyArray2List(VirtualAttrGroupInfo** attrInfoArray, int num);
static void tableDivideGroup();
static void tempAddAndClearSearchPath(AdviseGroup* adviseGroup, MaxNeighborVariable* maxNeiVar,
    VirtualAttrGroupInfo* attrInfo, AdviseTable* adviseTable, int pos, double tempWeight);
static List* addSearchPathResult(List* allSearchPathResults, List* currentSearchPathResult);
static void fillinGroupWithTable(AdviseGroup* adviseGroup, List* tableOids);
static void fillinGroupWithQuery(AdviseGroup* adviseGroup, AdviseQuery* adviseQuery);
static AdviseGroup* createAdviseGroup();
static void initMaxNeighborVariable(MaxNeighborVariable* maxNeiVar, bool onlyBestResult, long tableNum,
                                    JoinMaxHeap* maxHeap, List* replicationTableList);
static float4 getAttrStadistinct(Oid relid, AttrNumber attnum);
static List* getIndexIndkeyList(Oid tableoid);
static float8 getRelReltuples(Oid relid);
static AdviseGroup* getAdviseGroup(Oid relid);
static List* getAdviseGroups(List* tableOids);
static List* getTableOids(AdviseTable* adviseTable);
static void getTopTableAndAttr(AdviseGroup* adviseGroup, MaxNeighborVariable* maxNeiVar, List* findedList,
                               AdviseTable** maxAttrTable, VirtualAttrGroupInfo** maxAttrInfo, bool isFileterJoin);
static VirtualAttrGroupInfo* getTopAttr(AdviseTable* adviseTable, MaxNeighborVariable* maxNeiVar, List* findedList,
                                        double* topAttrWeight, bool isOnlyJoinWeight, bool isFileterJoin);
static void generateCandidateAttrWithHeu(AdviseTable* adviseTable);
static int queryCostCmp(const void* a, const void* b);
static void resetAdviseQuery();
static void filterQuery();
static int virtualAttrWeightCmp(const void* a, const void* b);
static int virtualAttrOidCmp(const void* a, const void* b);
static List* analyzeGroupHeuristic(AdviseGroup* adviseGroup, List** replicationTableList, bool onlyBestResult);
static void advisorGroupDesc2Tuple(AdviseGroup* adviseGroup);
static void advisorAttrDesc2Tuple(List* result, double costImprove);
static void advisorTableDesc2Tuple(List* result, double costImprove);
static void searchJoinGraphWithMaxNeighbor(AdviseGroup* adviseGroup, MaxNeighborVariable* maxNeiVar);
static void addJoinEqualsToMaxHeap(List* currentSearchPathResult, JoinMaxHeap* maxHeap, VirtualAttrGroupInfo* attrInfo);
static AttrNumber getJoinWithAttrs(JoinCell* joinCell);
static void clearTableMarked(AdviseGroup* adviseGroup);
static JoinMaxHeap* createJoinMaxHeap(int capacity);
static bool pushJoinEqual(JoinMaxHeap* maxHeap, JoinCell* cell);
static JoinCell* popMaxJoinEqual(JoinMaxHeap* maxHeap);
static void percDownJoinMaxHeap(JoinMaxHeap* maxHeap, int n);
static bool joinMaxHeapCmp(const JoinCell* a, const JoinCell* b);
static void clearJoinMaxHeap(JoinMaxHeap* maxHeap);
static void clearPartResult(AdviseGroup* adviseGroup, MaxNeighborVariable* maxNeiVar, int pos, double tempWeight);
static void analyzeGroupCost(AdviseGroup* adviseGroup);
static Cost runCombineWithCost(AdviseGroup* adviseGroup, Bitmapset* combineBitmap);
static bool compareReferencedTableBitmap(Bitmapset* a, Bitmapset* b, Bitmapset* referencedTables);
static void initTableOffset(AdviseGroup* adviseGroup);
static Bitmapset* generateCombineBitmap(AdviseGroup* adviseGroup, List* result, bool isFillinTable);
static int getVirtualAttrIndex(List* totalCandidateAttrs, VirtualAttrGroupInfo* attrInfo);
static void getOriginTypeAndKey(Oid relid, char** typeString, char** keyString);
static void generateQueryBitmap(AdviseGroup* adviseGroup);
static void generateCombineWalker(List* disTableList, List* combineResult, AdviseGroup* adviseGroup);
static void generateCandicateCombines(AdviseGroup* adviseGroup);
static List* generateCombineResult(AdviseGroup* adviseGroup, Bitmapset* combineBitmap);
static Bitmapset* generateBitmap(List* bitmapOffsets);
static int getDistrKeyPosition(AdviseTable* adviseTable);
static void queryDivideGroup();
static void generateCandidateAttrWithCost(AdviseGroup* adviseGroup);
static bool isCandidateAttrSatisfiedStadistinct(AdviseTable* adviseTable, VirtualAttrGroupInfo* attrInfo,
                                                float8 reltuples, int numDatanodes);
static void getAdviseTableList(AdviseGroup* adviseGroup, List** distributionTableList, List** replicationTableList);
static bool isChangeDisKey(VirtualAttrGroupInfo* attrInfo);
static bool isAttrMarked(Oid joinWithOid, MaxNeighborVariable* maxNeiVar);
static void resizeMaxHeap(JoinMaxHeap* maxHeap);

void checkSessAdvMemSize()
{
    if (u_sess->adv_cxt.maxMemory > 0 && u_sess->adv_cxt.SQLAdvisorContext) {
        int64 totalsize = ((AllocSet)u_sess->adv_cxt.SQLAdvisorContext)->totalSpace;
        if ((int64)u_sess->adv_cxt.maxMemory * 1024 * 1024 < totalsize) {
            ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                    errmsg("sql advisor out of memory")));
        }
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("sql advisor memory not init")));
    }
}

void initCandicateTables(MemoryContext context, HTAB** hash_table)
{
    /* First time through: initialize the hash table */
    HASHCTL ctl;
    errno_t rc;
    MemoryContext oldcxt = MemoryContextSwitchTo(context);

    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "\0", "\0");
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(AdviseTable);
    ctl.hash = oid_hash;
    ctl.hcxt = context;
    *hash_table = hash_create("sql_advisor_table", 128, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    (void)MemoryContextSwitchTo(oldcxt);
    checkSessAdvMemSize();
}

char* concatAttrNames(List* attrNames, int natts)
{
    StringInfoData attr_num_buf;
    ListCell* lc = NULL;
    int i;

    initStringInfo(&attr_num_buf);
    lc = list_head(attrNames);

    for (i = 0; i < natts; i++) {
        if (i)
            appendStringInfoString(&attr_num_buf, ", ");
        appendStringInfo(&attr_num_buf, "%s", (char*)lfirst(lc));
        lc = lnext(lc);
    }
    /* return palloc'd string buffer */
    char* attrNum = pstrdup(attr_num_buf.data);
    pfree_ext(attr_num_buf.data);
    return attrNum;
}

bool runWithHeuristicMethod()
{
    tableDivideGroup();
    ListCell* cell = NULL;
    AdviseGroup* adviseGroup = NULL;
    List* result = NIL;
    List* replicationTableList = NIL;

    foreach (cell, u_sess->adv_cxt.candicateAdviseGroups) {
        CHECK_FOR_INTERRUPTS();
        adviseGroup = (AdviseGroup*)lfirst(cell);
        result = (List*)linitial(analyzeGroupHeuristic(adviseGroup, &replicationTableList, true));
        advisorAttrDesc2Tuple(result, 0.0);
        list_free_ext(result);
    }

    advisorTableDesc2Tuple(replicationTableList, 0.0);

    return true;
}

static void advisorGroupDesc2Tuple(AdviseGroup* adviseGroup)
{
    MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->adv_cxt.SQLAdvisorContext);

    AdviseTable* adviseTable = NULL;
    HASH_SEQ_STATUS hash_seq;

    hash_seq_init(&hash_seq, adviseGroup->candicateTables);
    /* traverse all hash table element. */
    while ((adviseTable = (AdviseTable*)hash_seq_search(&hash_seq)) != NULL) {
        DistributionResultview* resultView = (DistributionResultview*)MemoryContextAlloc(
            u_sess->adv_cxt.SQLAdvisorContext, sizeof(DistributionResultview));
        resultView->dbName = pstrdup(get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true));
        resultView->schemaName = get_namespace_name(get_rel_namespace(adviseTable->oid));
        resultView->tableName = get_rel_name(adviseTable->oid);
        char* typeString = NULL;
        char* keyString = NULL;
        getOriginTypeAndKey(adviseTable->oid, &typeString, &keyString);
        resultView->distributioType = pstrdup(typeString);
        if (keyString == NULL) {
            resultView->distributionKey = NULL;
        } else {
            resultView->distributionKey = keyString;
        }
        resultView->startTime = u_sess->adv_cxt.startTime;
        resultView->endTime = GetCurrentTimestamp();
        resultView->costImpove = 0.0;
        resultView->comment = pstrdup("current distribution is best");

        u_sess->adv_cxt.result = lappend(u_sess->adv_cxt.result, resultView); 
    }

    (void)MemoryContextSwitchTo(oldcxt);
}

static void getOriginTypeAndKey(Oid relid, char** typeString, char** keyString)
{
    char locatorType = LOCATOR_TYPE_NONE;
    RelationLocInfo* locInfo = GetRelationLocInfo(relid);
    if (locInfo != NULL) {
        locatorType = locInfo->locatorType;
        FreeRelationLocInfo(locInfo);
    }

    if (locatorType == LOCATOR_TYPE_HASH) {
        *typeString = "Hash";
        *keyString = printDistributeKey(relid);
    } else if (locatorType == LOCATOR_TYPE_REPLICATED) {
        *typeString = "Replication";
    } else if (locatorType == LOCATOR_TYPE_RROBIN) {
        *typeString = "Roundrobin";
    } else if (locatorType == LOCATOR_TYPE_MODULO) {
        *typeString = "Modulo";
        *keyString = printDistributeKey(relid);
    } else if (locatorType == LOCATOR_TYPE_RANGE) {
        *typeString = "Range";
        *keyString = printDistributeKey(relid);
    } else {
        /* LOCATOR_TYPE_LIST */
        *typeString = "List";
        *keyString = printDistributeKey(relid);
    }
}

static void advisorTableDesc2Tuple(List* result, double costImprove)
{
    MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->adv_cxt.SQLAdvisorContext);
    ListCell* cell = NULL;
    AdviseTable* adviseTable = NULL;

    foreach (cell, result) {
        adviseTable = (AdviseTable*)lfirst(cell);
        DistributionResultview* resultView = (DistributionResultview*)MemoryContextAlloc(
            u_sess->adv_cxt.SQLAdvisorContext, sizeof(DistributionResultview));
        resultView->dbName = pstrdup(get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true));
        resultView->schemaName = get_namespace_name(get_rel_namespace(adviseTable->oid));
        resultView->tableName = get_rel_name(adviseTable->oid);

        char* typeString = NULL;
        char* keyString = NULL;
        getOriginTypeAndKey(adviseTable->oid, &typeString, &keyString);
        if (adviseTable->enableAdvise) {
            resultView->distributioType = pstrdup("Replication");
            if (strcmp(typeString, "Replication") == 0) {
                resultView->comment = pstrdup("current distribution is best");
            } else {
                resultView->comment = NULL;
            }
            
            resultView->distributionKey = NULL;
        } else {
            resultView->distributioType = pstrdup(typeString);
            if (keyString == NULL) {
                resultView->distributionKey = NULL;
            } else {
                resultView->distributionKey = keyString;
            }
            resultView->comment = pstrdup("primary key constraint can't advise");
        }
        
        resultView->startTime = u_sess->adv_cxt.startTime;
        resultView->endTime = GetCurrentTimestamp();
        resultView->costImpove = costImprove;

        u_sess->adv_cxt.result = lappend(u_sess->adv_cxt.result, resultView); 
    }
   
    (void)MemoryContextSwitchTo(oldcxt);
}

static void advisorAttrDesc2Tuple(List* result, double costImprove)
{
    MemoryContext oldcxt = MemoryContextSwitchTo(u_sess->adv_cxt.SQLAdvisorContext);
    ListCell* cell = NULL;
    VirtualAttrGroupInfo* attrInfo = NULL;

    foreach (cell, result) {
        attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
        DistributionResultview* resultView = (DistributionResultview*)MemoryContextAlloc(
            u_sess->adv_cxt.SQLAdvisorContext, sizeof(DistributionResultview));
        resultView->dbName = pstrdup(get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true));
        resultView->schemaName = get_namespace_name(get_rel_namespace(attrInfo->oid));
        resultView->tableName = get_rel_name(attrInfo->oid);
        resultView->distributioType = pstrdup("Hash");
        resultView->distributionKey = concatAttrNames(attrInfo->attrNames, attrInfo->natts);
        resultView->startTime = u_sess->adv_cxt.startTime;
        resultView->endTime = GetCurrentTimestamp();
        resultView->costImpove = costImprove;
        if (isChangeDisKey(attrInfo)) {
            resultView->comment = pstrdup("current distribution is best");
        } else {
            resultView->comment = NULL;
        }
        
        u_sess->adv_cxt.result = lappend(u_sess->adv_cxt.result, resultView); 
    }
   
    (void)MemoryContextSwitchTo(oldcxt);
}

static bool isChangeDisKey(VirtualAttrGroupInfo* attrInfo)
{
    AdviseTable* adviseTable = (AdviseTable*)hash_search(
        u_sess->adv_cxt.candicateTables, (void*)&(attrInfo->oid), HASH_FIND, NULL);
    
    if (adviseTable) {
        if (list_length(adviseTable->originDistributionKey) == 1 &&
            linitial_int(adviseTable->originDistributionKey) == attrInfo->attrNums[0]) {
            return true;
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("relid not in hash table")));
    }

    return false;
}

static void sortSelecteCandidateAttrs(AdviseTable* adviseTable, bool isFilter)
{
    if (adviseTable->selecteCandidateAttrs == NULL || 
        list_length(adviseTable->selecteCandidateAttrs) < SORT_ATTR_THRESHOLD) {
        return;
    }

    int attrNum = list_length(adviseTable->selecteCandidateAttrs);
    ListCell* cell = NULL;
    VirtualAttrGroupInfo* attrInfo = NULL;
    VirtualAttrGroupInfo** attrInfoArray = 
        (VirtualAttrGroupInfo**)palloc(attrNum * sizeof(VirtualAttrGroupInfo*));
    int index = 0;
    foreach (cell, adviseTable->selecteCandidateAttrs) {
        attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
        attrInfo->weight = 0.0;
        if (u_sess->adv_cxt.adviseMode == AM_HEURISITICS) {
            attrInfo->weight = attrInfo->joinCount * u_sess->adv_cxt.joinWeight +
                            attrInfo->groupbyCount * u_sess->adv_cxt.groupbyWeight +
                            attrInfo->qualCount * u_sess->adv_cxt.qualWeight;
        } else {
            ListCell* lc = NULL;
            foreach (lc, attrInfo->joinQuals) {
                JoinCell* joinCell = (JoinCell*)lfirst(lc);
                if (joinCell->joinWeight > 0.0) {
                    attrInfo->weight += joinCell->joinWeight;
                }
            }
            attrInfo->weight += attrInfo->groupbyWeight +
                                attrInfo->qualCount * u_sess->adv_cxt.qualWeight;
        }
        
        attrInfoArray[index++] = attrInfo;
    }
    list_free_ext(adviseTable->selecteCandidateAttrs);
    qsort(attrInfoArray, attrNum, sizeof(VirtualAttrGroupInfo*), virtualAttrWeightCmp);
    
    if (isFilter) {
        /* only need 2 attr */
        adviseTable->selecteCandidateAttrs = lappend(adviseTable->selecteCandidateAttrs, attrInfoArray[0]);
        adviseTable->selecteCandidateAttrs = lappend(adviseTable->selecteCandidateAttrs, attrInfoArray[1]);
    } else {
        for (index = 0; index < attrNum; index++) {
            adviseTable->selecteCandidateAttrs = lappend(adviseTable->selecteCandidateAttrs, attrInfoArray[index]);
        }
    }
    
    pfree_ext(attrInfoArray);
}

static List* analyzeGroupHeuristic(AdviseGroup* adviseGroup, List** replicationTableList, bool onlyBestResult)
{
    AdviseTable* adviseTable = NULL;
    HASH_SEQ_STATUS hash_seq;
    VirtualAttrGroupInfo* attrInfo = NULL;
    ListCell* cell = NULL;
    long candicateTableCount = hash_get_num_entries(adviseGroup->candicateTables);
    JoinMaxHeap* maxHeap= createJoinMaxHeap(candicateTableCount * 10);
    hash_seq_init(&hash_seq, adviseGroup->candicateTables);
    /* traverse all hash table element. */
    while ((adviseTable = (AdviseTable*)hash_seq_search(&hash_seq)) != NULL) {
        if (adviseTable->isAssignRelication || !adviseTable->enableAdvise) {
            candicateTableCount -= 1;
            *replicationTableList = lappend(*replicationTableList, adviseTable);
            continue;
        }
        if (u_sess->adv_cxt.adviseMode == AM_HEURISITICS) {
            generateCandidateAttrWithHeu(adviseTable);
        }
        
        if (!adviseTable->selecteCandidateAttrs) {
            candicateTableCount -= 1;
            *replicationTableList = lappend(*replicationTableList, adviseTable);
        }
        sortSelecteCandidateAttrs(adviseTable, false);
    }

    MaxNeighborVariable* maxNeiVar = (MaxNeighborVariable*)palloc(sizeof(MaxNeighborVariable));
    initMaxNeighborVariable(maxNeiVar, onlyBestResult, candicateTableCount, maxHeap, *replicationTableList);
    hash_seq_init(&hash_seq, adviseGroup->candicateTables);
    while ((adviseTable = (AdviseTable*)hash_seq_search(&hash_seq)) != NULL) {
        if (adviseTable->selecteCandidateAttrs == NULL) {
            continue;
        }

        foreach(cell, adviseTable->selecteCandidateAttrs) {
            CHECK_FOR_INTERRUPTS();
            adviseTable->marked = true;
            attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
            maxNeiVar->currentSearchPathResult = lappend(maxNeiVar->currentSearchPathResult, attrInfo);
            addJoinEqualsToMaxHeap(maxNeiVar->currentSearchPathResult, maxNeiVar->maxHeap, attrInfo);
            searchJoinGraphWithMaxNeighbor(adviseGroup, maxNeiVar);
            list_free_ext(maxNeiVar->currentSearchPathResult);
            maxNeiVar->tempWeight = 0.0;
            clearTableMarked(adviseGroup);
            clearJoinMaxHeap(maxNeiVar->maxHeap);
        }
    }
    if (onlyBestResult) {
        return list_make1(maxNeiVar->allSearchPathResults); 
    } else {
        return maxNeiVar->allSearchPathResults;
    }
}

static void initMaxNeighborVariable(MaxNeighborVariable* maxNeiVar, bool onlyBestResult, long tableNum,
                                    JoinMaxHeap* maxHeap, List* replicationTableList)
{
    maxNeiVar->currentSearchPathResult = NIL;
    maxNeiVar->allSearchPathResults = NIL;
    maxNeiVar->replicationTableList = replicationTableList;
    maxNeiVar->tempWeight = 0.0;
    maxNeiVar->maxWeight = -1.0;
    maxNeiVar->onlyBestResult = onlyBestResult;
    maxNeiVar->tableNum = tableNum;
    maxNeiVar->maxHeap = maxHeap;
    maxNeiVar->topN = DEFAULT_TOP_ATTR_NUMBER;
}

static List* recombineSearchPathResult(AdviseGroup* adviseGroup, MaxNeighborVariable* maxNeiVar)
{
    ListCell* lc1 = NULL;
    List* tempResult = NIL;
    bool isChangeResult = false;
    foreach(lc1, maxNeiVar->currentSearchPathResult) {
        VirtualAttrGroupInfo* attrInfo = (VirtualAttrGroupInfo*)lfirst(lc1);
        bool isChangeAttr = true;
        ListCell* lc2 = NULL;
        foreach(lc2, attrInfo->joinQuals) {
            JoinCell* joinCell = (JoinCell*)lfirst(lc2);
            ListCell* lc3 = NULL;
            VirtualAttrGroupInfo* joinAttrInfo = NULL;
            AttrNumber joinWithAttrs = getJoinWithAttrs(joinCell);

            foreach(lc3, maxNeiVar->currentSearchPathResult) {
                joinAttrInfo = (VirtualAttrGroupInfo*)lfirst(lc3);
                if (joinCell->joinWithOid == joinAttrInfo->oid &&
                    compareAttrs(joinAttrInfo->attrNums, joinAttrInfo->natts, &joinWithAttrs, joinCell->joinNattr)) {
                    break;
                }
            }
            if (joinAttrInfo != NULL) {
                isChangeAttr = false;
                break;
            }
        }

        if (isChangeAttr) {
            double topAttrWeight = -1.0;
            bool found = false;
            AdviseTable* adviseTable = (AdviseTable*)hash_search(
                adviseGroup->candicateTables, (void*)&(attrInfo->oid), HASH_FIND, &found);
            
            if (found) {
                VirtualAttrGroupInfo* topAttrInfo =
                    getTopAttr(adviseTable, maxNeiVar, NULL, &topAttrWeight, false, false);
                if (topAttrInfo != NULL &&
                    !compareAttrs(attrInfo->attrNums, attrInfo->natts, topAttrInfo->attrNums, topAttrInfo->natts)) {
                    isChangeResult = true;
                    tempResult = lappend(tempResult, topAttrInfo);
                    continue;
                }
            }
        }
        tempResult = lappend(tempResult, attrInfo);
    }
    if (isChangeResult) {
        return tempResult;
    } else {
        list_free_ext(tempResult);
        return NULL;
    }
}

static List* addSearchPathResult(List* allSearchPathResults, List* currentSearchPathResult)
{
    int index = 0;
    int tableNum = list_length(currentSearchPathResult);
    ListCell* cell = NULL;
    ListCell* attrCell = NULL;
    VirtualAttrGroupInfo* attrInfo = NULL;
    VirtualAttrGroupInfo** attrInfoArray = 
        (VirtualAttrGroupInfo**)palloc(tableNum * sizeof(VirtualAttrGroupInfo*));
    foreach (cell, currentSearchPathResult) {
        attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
        attrInfoArray[index++] = attrInfo;
    }
    qsort(attrInfoArray, tableNum, sizeof(VirtualAttrGroupInfo*), virtualAttrOidCmp);
    
    if (allSearchPathResults == NULL) {
        allSearchPathResults = lappend(allSearchPathResults, copyArray2List(attrInfoArray, tableNum));
        pfree_ext(attrInfoArray);
        return allSearchPathResults;
    }

    foreach (cell, allSearchPathResults) {
        List* comebine = (List*)lfirst(cell);
        index = 0;
        foreach (attrCell, comebine) {
            attrInfo = (VirtualAttrGroupInfo*)lfirst(attrCell);
            if (!compareAttrs(attrInfo->attrNums, attrInfo->natts, attrInfoArray[index]->attrNums,
                              attrInfoArray[index]->natts)) {
                break;
            }
            index++;
        }

        if (attrCell == NULL) {
            pfree_ext(attrInfoArray);
            return allSearchPathResults;
        }
    }

    allSearchPathResults = lappend(allSearchPathResults, copyArray2List(attrInfoArray, tableNum));
    pfree_ext(attrInfoArray);
    return allSearchPathResults;
}

static List* copyArray2List(VirtualAttrGroupInfo** attrInfoArray, int num)
{
    List* attrInfoList = NIL;
    
    for (int i = 0; i < num; i++) {
        attrInfoList = lappend(attrInfoList, attrInfoArray[i]);
        ereport(DEBUG1, (errmodule(MOD_ADVISOR), errmsg("Table %s attr %s",
            get_rel_name(attrInfoArray[i]->oid),
                concatAttrNames(attrInfoArray[i]->attrNames, attrInfoArray[i]->natts))));
    }

    return attrInfoList;
}

static void saveSearchPathResult(AdviseGroup* adviseGroup, MaxNeighborVariable* maxNeiVar)
{
    ereport(DEBUG1, (errmodule(MOD_ADVISOR), errmsg("temp total weight is %f", maxNeiVar->tempWeight / 2)));
    if (maxNeiVar->onlyBestResult) {
        if (maxNeiVar->maxWeight < maxNeiVar->tempWeight) {
            maxNeiVar->maxWeight = maxNeiVar->tempWeight;
            list_free_ext(maxNeiVar->allSearchPathResults);
            maxNeiVar->allSearchPathResults = copyList(maxNeiVar->currentSearchPathResult);
        }
    } else {
        List* recombineResultList = recombineSearchPathResult(adviseGroup, maxNeiVar);
        if (recombineResultList != NULL) {
            maxNeiVar->allSearchPathResults =
                addSearchPathResult(maxNeiVar->allSearchPathResults, recombineResultList);
            list_free_ext(recombineResultList);
        }
        maxNeiVar->allSearchPathResults =
            addSearchPathResult(maxNeiVar->allSearchPathResults, maxNeiVar->currentSearchPathResult);
        
        /* Limit the number of generated combinations */
        if (list_length(maxNeiVar->allSearchPathResults) >= MAX_COMBINATION_NUM) {
            maxNeiVar->topN = MIN_TOP_ATTR_NUMBER;
        }
    }
}

static void searchJoinGraphWithMaxNeighbor(AdviseGroup* adviseGroup, MaxNeighborVariable* maxNeiVar)
{
    bool isRecursionEnd =  maxNeiVar->tableNum == (long)list_length(maxNeiVar->currentSearchPathResult) &&
        maxNeiVar->maxHeap->size == 0;
    if (isRecursionEnd) {
        saveSearchPathResult(adviseGroup, maxNeiVar);
        return;
    }
    CHECK_FOR_INTERRUPTS();
    AttrNumber joinWithAttrs;
    JoinCell* currJoinCell = NULL;
    AdviseTable* adviseTable = NULL;
    VirtualAttrGroupInfo* attrInfo = NULL;
    if (maxNeiVar->maxHeap->size != 0) {
        currJoinCell = popMaxJoinEqual(maxNeiVar->maxHeap);
        joinWithAttrs = getJoinWithAttrs(currJoinCell);
        bool found = false;
        adviseTable = (AdviseTable*)hash_search(
            adviseGroup->candicateTables, (void*)&(currJoinCell->joinWithOid), HASH_FIND, &found);
        
        if (found) {
            if (!adviseTable->marked) {
                attrInfo = getVirtualAttrGroupInfo(&joinWithAttrs, 1, adviseTable->selecteCandidateAttrs);
                if (attrInfo != NULL) {
                    addJoinEqualsToMaxHeap(maxNeiVar->currentSearchPathResult, maxNeiVar->maxHeap, attrInfo);
                    ereport(DEBUG2, (errmodule(MOD_ADVISOR), errmsg("Table %s join attr %s join weight is %f",
                        get_rel_name(currJoinCell->currOid), concatAttrNames(attrInfo->attrNames, attrInfo->natts),
                            currJoinCell->joinWeight)));
                    maxNeiVar->currentSearchPathResult = lappend(maxNeiVar->currentSearchPathResult, attrInfo);
                    maxNeiVar->tempWeight += currJoinCell->joinWeight;
                    adviseTable->marked = true;
                }
            } else {
                ListCell* lc = NULL;
                VirtualAttrGroupInfo* joinAttrInfo = NULL;
                foreach (lc, maxNeiVar->currentSearchPathResult) {
                    joinAttrInfo = (VirtualAttrGroupInfo*)lfirst(lc);
                    if (joinAttrInfo->oid == currJoinCell->joinWithOid) {
                        break;
                    }
                }
                
                if (lc == NULL) {
                    ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("There is must an attr in list")));
                } else {
                    OpExpr* opexpr = (OpExpr*)currJoinCell->expr;
                    Var* arg1 = (Var*)linitial(opexpr->args);
                    Var* arg2 = (Var*)lsecond(opexpr->args);
                    bool isAddWeight = joinAttrInfo->natts == 1 &&
                        ((arg1->varno == adviseTable->oid && joinAttrInfo->attrNums[0] == arg1->varattno) ||
                        (arg2->varno == adviseTable->oid && joinAttrInfo->attrNums[0] == arg2->varattno));
                    if (isAddWeight) {
                        ereport(DEBUG2, (errmodule(MOD_ADVISOR), errmsg("Table %s attr %s join weight %f",
                            get_rel_name(currJoinCell->currOid),
                                concatAttrNames(joinAttrInfo->attrNames, joinAttrInfo->natts),
                                    currJoinCell->joinWeight)));
                        maxNeiVar->tempWeight += currJoinCell->joinWeight;
                    }
                }
            }
        }
        searchJoinGraphWithMaxNeighbor(adviseGroup, maxNeiVar);
    } else {
        List* findedList = NIL;
        AdviseTable* maxAttrTable = NULL;
        VirtualAttrGroupInfo* maxAttrInfo = NULL;
        int pos = list_length(maxNeiVar->currentSearchPathResult);
        double tempWeight = maxNeiVar->tempWeight;
        
        if (maxNeiVar->onlyBestResult) {
            /* we choose the top 1 total weight and don't clean path. */
            getTopTableAndAttr(adviseGroup, maxNeiVar, findedList, &maxAttrTable, &maxAttrInfo, false);
            if (maxAttrInfo != NULL) {
                tempAddAndClearSearchPath(adviseGroup, maxNeiVar, maxAttrInfo, maxAttrTable, pos, tempWeight);
            }
        } else {
            /* we choose top n weight and clean path */
            int num = 1;
            while (num <= maxNeiVar->topN) {
                getTopTableAndAttr(adviseGroup, maxNeiVar, findedList, &maxAttrTable, &maxAttrInfo, true);
                if (maxAttrInfo == NULL) {
                    getTopTableAndAttr(adviseGroup, maxNeiVar, findedList, &maxAttrTable, &maxAttrInfo, false);
                    if (maxAttrInfo != NULL) {
                        tempAddAndClearSearchPath(adviseGroup, maxNeiVar, maxAttrInfo, maxAttrTable, pos, tempWeight);
                    }
                    break;
                }
                findedList = lappend(findedList, maxAttrInfo);
                tempAddAndClearSearchPath(adviseGroup, maxNeiVar, maxAttrInfo, maxAttrTable, pos, tempWeight);
                num += 1;
                maxAttrTable = NULL;
                maxAttrInfo = NULL;
            }
            list_free_ext(findedList);
        }
    }
}

static void getTopTableAndAttr(AdviseGroup* adviseGroup, MaxNeighborVariable* maxNeiVar, List* findedList,
                                AdviseTable** maxAttrTable, VirtualAttrGroupInfo** maxAttrInfo, bool isFileterJoin)
{
    HASH_SEQ_STATUS hashSeq;
    hash_seq_init(&hashSeq, adviseGroup->candicateTables);
    double maxAttrWeight = -1.0;
    AdviseTable* adviseTable = NULL;
    VirtualAttrGroupInfo* attrInfo = NULL;
    /* find a table which is not marked. */
    while ((adviseTable = (AdviseTable*)hash_seq_search(&hashSeq)) != NULL) {
        if (!adviseTable->marked && adviseTable->selecteCandidateAttrs != NIL) {
            int attrNum = 0;
            ListCell* lc = NULL;
            VirtualAttrGroupInfo* findedAttrInfo = NULL;
            foreach(lc, findedList) {
                findedAttrInfo = (VirtualAttrGroupInfo*)lfirst(lc);
                if (findedAttrInfo->oid == adviseTable->oid) {
                    attrNum += 1;
                }
            }
            if (attrNum >= MAX_FINDELIST_SAME_TABLE_NUM) {
                continue;
            }
            double topAttrWeight = -1.0;
            attrInfo = getTopAttr(adviseTable, maxNeiVar, findedList, &topAttrWeight, true, isFileterJoin);
            if (attrInfo != NULL && maxAttrWeight < topAttrWeight) {
                *maxAttrInfo = attrInfo;
                *maxAttrTable = adviseTable;
                maxAttrWeight = topAttrWeight;
            }
        }
    }
}

static VirtualAttrGroupInfo* getTopAttr(AdviseTable* adviseTable, MaxNeighborVariable* maxNeiVar, List* findedList,
                                        double* topAttrWeight, bool isOnlyJoinWeight, bool isFileterJoin)
{
    ListCell* cell = NULL;
    VirtualAttrGroupInfo* attrInfo = NULL;
    VirtualAttrGroupInfo* maxAttrInfo = NULL;
    foreach (cell, adviseTable->selecteCandidateAttrs) {
        attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
        if (isFileterJoin && attrInfo->joinQuals == NULL) {
            continue;
        }

        ListCell* lc = NULL;
        VirtualAttrGroupInfo* findedAttrInfo = NULL;
        foreach(lc, findedList) {
            findedAttrInfo = (VirtualAttrGroupInfo*)lfirst(lc);
            if (findedAttrInfo == attrInfo) {
                continue;
            }
        }

        /* recompute this attr join weight */
        double tempAttrJoinWeight = 0.0;
        foreach(lc, attrInfo->joinQuals) {
            JoinCell* joinCell = (JoinCell*)lfirst(lc);
            /* t1(a,b), t2(a) t1.a = t2.a, if t1.b is choosed, we don't sum the joinweight t1.a = t2.a. */
            if (joinCell->joinWeight > 0.0) {
                if (isOnlyJoinWeight && isAttrMarked(joinCell->joinWithOid, maxNeiVar)) {
                    continue;
                }
                tempAttrJoinWeight += joinCell->joinWeight;
            }
        }

        double tempAttrWeight;
        if (u_sess->adv_cxt.adviseMode == AM_HEURISITICS) {
            tempAttrWeight = tempAttrJoinWeight * u_sess->adv_cxt.joinWeight +
                            attrInfo->groupbyCount * u_sess->adv_cxt.groupbyWeight +
                            attrInfo->qualCount * u_sess->adv_cxt.qualWeight;
        } else {
            if (isOnlyJoinWeight) {
                tempAttrWeight = tempAttrJoinWeight;
            } else {
                tempAttrWeight = tempAttrJoinWeight + attrInfo->groupbyWeight +
                            attrInfo->qualCount * u_sess->adv_cxt.qualWeight;
            }
        }

        if (*topAttrWeight < tempAttrWeight) {
            *topAttrWeight = tempAttrWeight;
            maxAttrInfo = attrInfo;
        }
    }

    return maxAttrInfo;
}

static bool isAttrMarked(Oid joinWithOid, MaxNeighborVariable* maxNeiVar)
{
    ListCell* cell = NULL;
    foreach (cell, maxNeiVar->currentSearchPathResult) {
        VirtualAttrGroupInfo* attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
        if (joinWithOid == attrInfo->oid) {
            return true;
        }
    }

    foreach (cell, maxNeiVar->replicationTableList) {
        AdviseTable* adviseTable = (AdviseTable*)lfirst(cell);
        if (joinWithOid == adviseTable->oid) {
            return true;
        }
    }
    return false;
}

static void tempAddAndClearSearchPath(AdviseGroup* adviseGroup, MaxNeighborVariable* maxNeiVar,
    VirtualAttrGroupInfo* attrInfo, AdviseTable* adviseTable, int pos, double tempWeight)
{
    addJoinEqualsToMaxHeap(maxNeiVar->currentSearchPathResult, maxNeiVar->maxHeap, attrInfo);
    maxNeiVar->currentSearchPathResult = lappend(maxNeiVar->currentSearchPathResult, attrInfo);
    adviseTable->marked = true;
    searchJoinGraphWithMaxNeighbor(adviseGroup, maxNeiVar);
    if (!maxNeiVar->onlyBestResult) {
        clearPartResult(adviseGroup, maxNeiVar, pos, tempWeight);
    }
}

static void addJoinEqualsToMaxHeap(List* currentSearchPathResult, JoinMaxHeap* maxHeap, VirtualAttrGroupInfo* attrInfo)
{
    ListCell* cell = NULL;
    JoinCell* join_with_cell = NULL;

    foreach (cell, attrInfo->joinQuals) {
        join_with_cell = (JoinCell*)lfirst(cell);
        if (join_with_cell->joinWeight > 0.0) {
            ListCell* lc = NULL;
            VirtualAttrGroupInfo* joinAttrInfo = NULL;
            foreach (lc, currentSearchPathResult) {
                joinAttrInfo = (VirtualAttrGroupInfo*)lfirst(lc);
                if (joinAttrInfo->oid == join_with_cell->joinWithOid) {
                    break;
                }
            }
            
            if (lc == NULL) {
                pushJoinEqual(maxHeap, join_with_cell);
            } else {
                OpExpr* opexpr = (OpExpr*)join_with_cell->expr;
                Var* arg1 = (Var*)linitial(opexpr->args);
                Var* arg2 = (Var*)lsecond(opexpr->args);
                bool isAddCell = joinAttrInfo->natts == 1 &&
                        ((arg1->varno == join_with_cell->joinWithOid && joinAttrInfo->attrNums[0] == arg1->varattno) ||
                        (arg2->varno == join_with_cell->joinWithOid && joinAttrInfo->attrNums[0] == arg2->varattno));
                if (isAddCell) {
                    pushJoinEqual(maxHeap, join_with_cell);
                }
            }
        }
    }
}

static AttrNumber getJoinWithAttrs(JoinCell* joinCell)
{
    AttrNumber joinWithAttr = 0;

    if (IsA(joinCell->expr, OpExpr)) {
        OpExpr* opexpr = (OpExpr*)joinCell->expr;
        Var* arg1 = (Var*)linitial(opexpr->args);
        Var* arg2 = (Var*)lsecond(opexpr->args);

        if (Oid(arg1->varno) == joinCell->joinWithOid) {
            joinWithAttr = arg1->varattno;
        } else {
            joinWithAttr = arg2->varattno;
        }
    } else {
        ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), 
            errmsg("current node type cant equal: %d", (int)nodeTag(joinCell->expr))));
    }

    return joinWithAttr;
}

static void clearPartResult(AdviseGroup* adviseGroup, MaxNeighborVariable* maxNeiVar, int pos, double tempWeight)
{
    ListCell* cell = NULL;
    int i = 1; 
    List* delete_list = NIL;
    maxNeiVar->tempWeight = tempWeight;

    foreach (cell, maxNeiVar->currentSearchPathResult) {
        if (i > pos) {
            VirtualAttrGroupInfo* attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
            delete_list = lappend(delete_list, attrInfo);
            
            bool found = false;
            AdviseTable* adviseTable = (AdviseTable*)hash_search(
                adviseGroup->candicateTables, (void*)&(attrInfo->oid), HASH_FIND, &found);
            if (found) {
                adviseTable->marked = false;
            } else {
                ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("relid not in hash table")));
            }
        }

        i += 1;
    }
    
    foreach (cell, delete_list) {
        maxNeiVar->currentSearchPathResult = list_delete(maxNeiVar->currentSearchPathResult, lfirst(cell));
    }
    list_free_ext(delete_list);
}
static void clearTableMarked(AdviseGroup* adviseGroup)
{
    HASH_SEQ_STATUS hash_seq;
    AdviseTable* adviseTable = NULL;
    hash_seq_init(&hash_seq, adviseGroup->candicateTables);

    while ((adviseTable = (AdviseTable*)hash_seq_search(&hash_seq)) != NULL) {
        adviseTable->marked = false;
    }
}

/* 
 * if table has unique key or primary key, we should intersect them.
 * Because distributiuon must include unique key and primary key 
 */
static Bitmapset* generateTableKeyBitmap(Oid relid)
{
    List* indkey_list = getIndexIndkeyList(relid);
    ListCell* cell = NULL;
    Bitmapset* intersectRes = NULL;
    foreach (cell, indkey_list) {
        List* indexkey = (List*)lfirst(cell);
        Bitmapset* indkey_bitmap = generateBitmap(indexkey);
        if (intersectRes == NULL) {
            intersectRes = indkey_bitmap;
        } else {
            intersectRes = bms_intersect(intersectRes, indkey_bitmap);
        }
    }

    return intersectRes;
}

static void generateCandidateAttrWithHeu(AdviseTable* adviseTable)
{
    ListCell* cell = NULL;
    VirtualAttrGroupInfo* attrInfo = NULL;
    Bitmapset* intersectRes = NULL;
    adviseTable->selecteCandidateAttrs = NIL;
    if (u_sess->adv_cxt.isConstraintPrimaryKey) {
        intersectRes = generateTableKeyBitmap(adviseTable->oid);
    }

    foreach (cell, adviseTable->totalCandidateAttrs) {
        attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
        attrInfo->weight = attrInfo->joinCount * u_sess->adv_cxt.joinWeight +
                            attrInfo->groupbyCount * u_sess->adv_cxt.groupbyWeight +
                            attrInfo->qualCount * u_sess->adv_cxt.qualWeight;

        if (attrInfo->natts == 1 && attrInfo->joinQuals != NULL) {
            if (bms_is_empty(intersectRes)) {
                adviseTable->selecteCandidateAttrs = lappend(adviseTable->selecteCandidateAttrs, attrInfo);
            } else if (bms_is_member(attrInfo->attrNums[0], intersectRes)) {
                adviseTable->selecteCandidateAttrs = lappend(adviseTable->selecteCandidateAttrs, attrInfo);
            }
        }
    }
    
    /* This table don't have join attrs, we must select one clounm according to weight */
    if (!adviseTable->selecteCandidateAttrs) {
        int total_attr_num = list_length(adviseTable->totalCandidateAttrs);
        VirtualAttrGroupInfo** attrInfo_array = 
            (VirtualAttrGroupInfo**)palloc(total_attr_num * sizeof(VirtualAttrGroupInfo*));

        int index = 0;
        foreach (cell, adviseTable->totalCandidateAttrs) {
            attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
            attrInfo_array[index++] = attrInfo;
        }
        
        qsort(attrInfo_array, total_attr_num, sizeof(VirtualAttrGroupInfo*), virtualAttrWeightCmp);

        index = 0;
        while (index < total_attr_num) {
            if (attrInfo_array[index]->natts == 1) {
                if (bms_is_empty(intersectRes)) {
                    adviseTable->selecteCandidateAttrs =
                        lappend(adviseTable->selecteCandidateAttrs, attrInfo_array[index]);
                    break;
                } else if (bms_is_member(attrInfo_array[index]->attrNums[0], intersectRes)) {
                    adviseTable->selecteCandidateAttrs =
                        lappend(adviseTable->selecteCandidateAttrs, attrInfo_array[index]);
                    break;
                }   
            }
            
            index++;
        }

        if (!adviseTable->selecteCandidateAttrs) {
            adviseTable->enableAdvise = false;
        }

        pfree_ext(attrInfo_array);
    }

    bms_free(intersectRes);
}

static int virtualAttrWeightCmp(const void* a, const void* b)
{
    VirtualAttrGroupInfo* attrInfo_a = *((VirtualAttrGroupInfo**)a);
    VirtualAttrGroupInfo* attrInfo_b = *((VirtualAttrGroupInfo**)b);

    if (attrInfo_a->weight == attrInfo_b->weight)
        return 0;

    return (attrInfo_b->weight > attrInfo_a->weight) ? 1 : -1;
}

static int virtualAttrOidCmp(const void* a, const void* b)
{
    VirtualAttrGroupInfo* attrInfoA = *((VirtualAttrGroupInfo**)a);
    VirtualAttrGroupInfo* attrInfoB = *((VirtualAttrGroupInfo**)b);

    if (attrInfoA->oid == attrInfoB->oid)
        return 0;

    return (attrInfoA->oid > attrInfoB->oid) ? 1 : -1;
}

static int queryCostCmp(const void* a, const void* b)
{
    AdviseQuery* query_a = *((AdviseQuery**)a);
    AdviseQuery* query_b = *((AdviseQuery**)b);

    if (query_a->originCost == query_b->originCost)
        return 0;

    return (query_a->originCost > query_b->originCost) ? 1 : -1;
}

static void tableDivideGroup()
{
    HASH_SEQ_STATUS hash_seq;
    AdviseGroup* adviseGroup = NULL;
    AdviseTable* adviseTable = NULL;
    List* tableOids = NIL;
    List* groups = NIL;
    u_sess->adv_cxt.candicateAdviseGroups = NULL;
    hash_seq_init(&hash_seq, u_sess->adv_cxt.candicateTables);
    /* traverse all hash table element. */
    while ((adviseTable = (AdviseTable*)hash_seq_search(&hash_seq)) != NULL) {
        CHECK_FOR_INTERRUPTS();
        tableOids = getTableOids(adviseTable);
        if (tableOids == NIL) {
            continue;
        }
        groups = getAdviseGroups(tableOids);
        if (groups == NULL) {
            adviseGroup = createAdviseGroup();
            u_sess->adv_cxt.candicateAdviseGroups =
                lappend(u_sess->adv_cxt.candicateAdviseGroups, adviseGroup);
        } else if (list_length(groups) == 1) {
            adviseGroup = (AdviseGroup*)linitial(groups);
        } else {
            adviseGroup = concatAdviseGroup(groups);
        }
        fillinGroupWithTable(adviseGroup, tableOids);
    }

    list_free(tableOids);
    list_free_ext(groups);
}

static AdviseGroup* concatAdviseGroup(List* groups)
{
    AdviseTable* adviseTable = NULL;
    AdviseTable* del_adviseTable = NULL;
    AdviseGroup* adviseGroup = NULL;
    AdviseGroup* del_adviseGroup = NULL;
    HASH_SEQ_STATUS hash_seq;
    bool found = false;

    ListCell* cell = list_head(groups);
    adviseGroup = (AdviseGroup*)lfirst(cell);
    cell = lnext(cell);

    while(cell != NULL) {
        del_adviseGroup = (AdviseGroup*)lfirst(cell);

        hash_seq_init(&hash_seq, del_adviseGroup->candicateTables);
        /* traverse all hash table element. */
        while ((del_adviseTable = (AdviseTable*)hash_seq_search(&hash_seq)) != NULL) {
            adviseTable = (AdviseTable*)hash_search(
                adviseGroup->candicateTables, (void*)&(del_adviseTable->oid), HASH_ENTER, &found);

            if (!found) {
                adviseTable->oid = del_adviseTable->oid;
                adviseTable->tableName = del_adviseTable->tableName;
                adviseTable->totalCandidateAttrs = del_adviseTable->totalCandidateAttrs;
                adviseTable->originDistributionKey = del_adviseTable->originDistributionKey;
                adviseTable->currDistributionKey = del_adviseTable->currDistributionKey;
                adviseTable->tableGroupOffset = del_adviseTable->tableGroupOffset;
                adviseTable->selecteCandidateAttrs = del_adviseTable->selecteCandidateAttrs;
                adviseTable->marked = del_adviseTable->marked;
                adviseTable->isAssignRelication = del_adviseTable->isAssignRelication;
                adviseTable->enableAdvise = del_adviseTable->enableAdvise;
            }
        }

        adviseGroup->candicateQueries = list_concat(adviseGroup->candicateQueries, del_adviseGroup->candicateQueries);
        adviseGroup->originCost += del_adviseGroup->originCost;

        cell = lnext(cell);
        u_sess->adv_cxt.candicateAdviseGroups =
            list_delete_ptr(u_sess->adv_cxt.candicateAdviseGroups, del_adviseGroup);   
    }
    
    return adviseGroup;
}

static List* getTableOids(AdviseTable* adviseTable)
{
    ListCell* attr_cell = NULL;
    ListCell* expr_cell = NULL;
    JoinCell* joinCell = NULL;
    VirtualAttrGroupInfo* attrInfo = NULL;
    List* tableOids = NIL;
    tableOids = lappend_oid(tableOids, adviseTable->oid);
    
    foreach (attr_cell, adviseTable->totalCandidateAttrs) {
        attrInfo = (VirtualAttrGroupInfo*)lfirst(attr_cell);
        foreach (expr_cell, attrInfo->joinQuals) {
            joinCell = (JoinCell*)lfirst(expr_cell);
            addTableOids(tableOids, joinCell->joinWithOid);
        }
    }

    return tableOids;
}

static void addTableOids(List* tableOids, Oid relid)
{
    ListCell* cell = NULL;
    Oid rel;

    foreach (cell, tableOids) {
        rel = lfirst_oid(cell);
        if (relid == rel) {
            return ;
        }
    }

    tableOids = lappend_oid(tableOids, relid);;
}

static void fillinGroupWithTable(AdviseGroup* adviseGroup, List* tableOids)
{
    ListCell* cell = NULL;
    AdviseTable* adviseTable = NULL;
    Oid relid = 0;
    bool found = false;

    foreach (cell, tableOids) {
        relid = lfirst_oid(cell);
        bool srcFound = false;
        AdviseTable* srcAdviseTable = (AdviseTable*)hash_search(
            u_sess->adv_cxt.candicateTables, (void*)&(relid), HASH_FIND, &srcFound);
        /* select * from t1 , t1 doesn't in hash table.  */
        if (srcFound) {
            adviseTable = (AdviseTable*)hash_search(
                adviseGroup->candicateTables, (void*)&(relid), HASH_ENTER, &found);
            if (!found) {
                copyAdviseTable(adviseTable, srcAdviseTable);
            }
        }
    }
    checkSessAdvMemSize();
}

static List* copyList(List* src)
{
    List* dest = NIL;
    ListCell* cell = NULL;
    foreach (cell, src) {
        dest = lappend(dest, lfirst(cell));
    }

    return dest;
}

static void copyAdviseTable(AdviseTable* dest, AdviseTable* src)
{
    dest->oid = src->oid;
    dest->tableName = pstrdup(src->tableName);
    dest->totalCandidateAttrs = copyVirtualAttrGroupInfo(src->totalCandidateAttrs);
    dest->originDistributionKey = (List*)copyObject(src->originDistributionKey);
    dest->currDistributionKey = src->currDistributionKey;
    dest->tableGroupOffset = src->tableGroupOffset;
    dest->selecteCandidateAttrs = src->selecteCandidateAttrs;
    dest->marked = src->marked;
    dest->isAssignRelication = src->isAssignRelication;
    dest->enableAdvise = src->enableAdvise;
}

static List* copyVirtualAttrGroupInfo(List* src)
{
    if (src == NIL) {
        return NIL;
    }

    List* dest = NIL;
    VirtualAttrGroupInfo* destAttrInfo = NULL;
    VirtualAttrGroupInfo* srcAttrInfo = NULL;
    ListCell* cell = NULL;
    
    foreach (cell, src) {
        srcAttrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
        destAttrInfo = (VirtualAttrGroupInfo*)MemoryContextAlloc(CurrentMemoryContext, sizeof(VirtualAttrGroupInfo));

        destAttrInfo->oid = srcAttrInfo->oid;
        destAttrInfo->natts = srcAttrInfo->natts;
        destAttrInfo->attrNums = copyAttrNums(srcAttrInfo->natts, srcAttrInfo->attrNums);
        destAttrInfo->attrNames = copyAttrNames(srcAttrInfo->attrNames);
        destAttrInfo->joinQuals = copyJoinQuals(srcAttrInfo->joinQuals);
        destAttrInfo->quals = (List*)copyObject(srcAttrInfo->quals);
        destAttrInfo->joinCount = srcAttrInfo->joinCount;
        destAttrInfo->groupbyCount = srcAttrInfo->groupbyCount;
        destAttrInfo->qualCount = srcAttrInfo->qualCount;
        destAttrInfo->weight = srcAttrInfo->weight;

        dest = lappend(dest, destAttrInfo);
    }

    return dest;
}

static List* copyJoinQuals(List* src)
{
    ListCell* cell = NULL;
    List* dest = NIL;
    JoinCell* srcJoinCell = NULL;
    JoinCell* destJoinCell = NULL;

    foreach (cell, src) {
        srcJoinCell = (JoinCell*)lfirst(cell);
        destJoinCell = (JoinCell*)MemoryContextAlloc(CurrentMemoryContext, sizeof(JoinCell));
        destJoinCell->currOid = srcJoinCell->currOid;
        destJoinCell->joinWithOid = srcJoinCell->joinWithOid;
        destJoinCell->joinNattr = srcJoinCell->joinNattr;
        destJoinCell->joinWeight = srcJoinCell->joinWeight;
        destJoinCell->expr = (Node*)copyObject(srcJoinCell->expr);

        dest = lappend(dest, destJoinCell);
    }

    return dest;
}

static AttrNumber* copyAttrNums(int natts, AttrNumber* src)
{
    AttrNumber* dest = (AttrNumber*)palloc(sizeof(AttrNumber) * natts);
    for (int i = 0; i < natts; i++) {
        dest[i] = src[i];
    }

    return dest;
}

static List* copyAttrNames(List* src)
{
    if (src == NIL) {
        return NIL;
    }
    char* destName = NULL;
    char* srcName = NULL;
    List* dest = NIL;
    ListCell* cell = NULL;

    foreach (cell, src) {
        srcName = (char*)lfirst(cell);
        destName = pstrdup(srcName);

        dest = lappend(dest, destName);
    }

    return dest;
}

/* find the group by relid */
static List* getAdviseGroups(List* tableOids)
{
    if (u_sess->adv_cxt.candicateAdviseGroups == NULL) {
        return NULL;
    }

    List* groups = NIL;
    AdviseGroup* adviseGroup = NULL;
    ListCell* cell = NULL;
    Oid relid = 0;

    foreach (cell, tableOids) {
        relid = lfirst_oid(cell);
        adviseGroup = getAdviseGroup(relid);
        if (adviseGroup != NULL) {
            groups = addAdviseGroup(groups, adviseGroup);
        }
    }

    return groups;
}

static List* addAdviseGroup(List* groups, AdviseGroup* adviseGroup)
{
    ListCell* cell = NULL;
    foreach (cell, groups) {
        if (adviseGroup == lfirst(cell)) {
            return groups;
        }
    }

    groups = lappend(groups, adviseGroup);

    return groups;
}

static AdviseGroup* getAdviseGroup(Oid relid)
{
    AdviseGroup* adviseGroup = NULL;
    bool found = false;
    ListCell* cell;

    foreach(cell, u_sess->adv_cxt.candicateAdviseGroups) {
        adviseGroup = (AdviseGroup*)lfirst(cell);

        (void*)hash_search(adviseGroup->candicateTables, (void*)&(relid), HASH_FIND, &found);
        
        if (found) {
            return adviseGroup;
        }
    }

    return NULL;
}

static AdviseGroup* createAdviseGroup()
{
    AdviseGroup* adviseGroup = (AdviseGroup*)MemoryContextAlloc(CurrentMemoryContext, sizeof(AdviseGroup));

    adviseGroup->minCost = 0.0;
    adviseGroup->originCost = 0.0;
    adviseGroup->bestCombineResult = NIL;
    adviseGroup->currentCombineIndex = -1;
    adviseGroup->candicateCombines = NIL;

    adviseGroup->candicateQueries = NIL;
    initCandicateTables(CurrentMemoryContext, &adviseGroup->candicateTables);

    return adviseGroup;
}


static JoinMaxHeap* createJoinMaxHeap(int capacity)
{
    JoinMaxHeap* maxHeap = (JoinMaxHeap*)palloc(sizeof(JoinMaxHeap));
    maxHeap->data = (JoinCell**)palloc((capacity + 1) * sizeof(JoinCell*));
    maxHeap->size = 0; 
    maxHeap->capacity = capacity;

    for (int i = 0; i <= capacity ; i++) {
        maxHeap->data[i] = NULL;
    }

    checkSessAdvMemSize();
    return maxHeap;
}

/* if maxHeap is full, resize the maxHeap */
static void resizeMaxHeap(JoinMaxHeap* maxHeap)
{
    int capacity = maxHeap->capacity * 2;  
    JoinCell** newData = (JoinCell**)palloc((capacity + 1) * sizeof(JoinCell*));
    for (int i = 0; i <= capacity; i++) {
        if (i <= maxHeap->capacity) {
            newData[i] = maxHeap->data[i];
        } else {
            newData[i] = NULL;
        }
    }

    maxHeap->capacity = capacity;
    pfree_ext(maxHeap->data);
    maxHeap->data = newData;
}

static bool pushJoinEqual(JoinMaxHeap* maxHeap, JoinCell* cell)
{
    if (maxHeap->size == maxHeap->capacity) {
        resizeMaxHeap(maxHeap);
    }

    int index = ++maxHeap->size;

    while (index != 1 && joinMaxHeapCmp(cell, maxHeap->data[index/2])) {
        maxHeap->data[index] = maxHeap->data[index/2];
        index /= 2;
    }

    maxHeap->data[index] = cell;
    return true;
}

static JoinCell* popMaxJoinEqual(JoinMaxHeap* maxHeap)
{
    if (maxHeap->size == 0) {
        return NULL;
    }

    JoinCell* cell = maxHeap->data[1];
    maxHeap->data[1] = maxHeap->data[maxHeap->size];
    maxHeap->data[maxHeap->size] = NULL;
    maxHeap->size -= 1;

    percDownJoinMaxHeap(maxHeap, 1);

    return cell;
}

static void percDownJoinMaxHeap(JoinMaxHeap* maxHeap, int n)
{
    int parent;
    int child;
    JoinCell* tmp = maxHeap->data[n];

    for (parent = n; parent * 2 <= maxHeap->size; parent = child) {
        child = 2 * parent;
        if (child != maxHeap->size &&
            joinMaxHeapCmp(maxHeap->data[child + 1], maxHeap->data[child])) {
            child++;
        }

        if (joinMaxHeapCmp(maxHeap->data[child], tmp)) {
            maxHeap->data[parent] = maxHeap->data[child];
        } else {
            break;
        }
    }

    maxHeap->data[parent] = tmp;
}

/* a > b return true */
static bool joinMaxHeapCmp(const JoinCell* a, const JoinCell* b)
{
    if (a->joinWeight == b->joinWeight)
        return false;

    return a->joinWeight > b->joinWeight;
}

static void clearJoinMaxHeap(JoinMaxHeap* maxHeap)
{
    if (maxHeap->size == 0) {
        return;
    }

    for (int i = 1; i <= maxHeap->size; i++) {
        maxHeap->data[i] = NULL;
    }
    maxHeap->size = 0;
}

/* 
 * What If steps
 * 1. Grouping by join relationship
 * 2. filter query which cost is too high or too low.
 * 3. Grouping by query
 * 4. generate reference bitmap for each group
 * 5. generate combinations according different adviseCompressLevel
 * 6. loop each group get every combine result
 * 7. choose the best combination
 */
bool runWithCostMethod()
{
    tableDivideGroup();
    if (u_sess->adv_cxt.adviseCostMode == ACM_MEDCOSTLOW) {
        filterQuery();
    }
    queryDivideGroup();

    ListCell* cell = NULL;
    AdviseGroup* adviseGroup = NULL;
    int index = 1;
    int totalCombines = 0;
    foreach (cell, u_sess->adv_cxt.candicateAdviseGroups) {
        CHECK_FOR_INTERRUPTS();
        adviseGroup = (AdviseGroup*)lfirst(cell);
        generateCandicateCombines(adviseGroup);
        ereport(DEBUG1, (errmodule(MOD_ADVISOR), errmsg("Group %d has %d combines", index,
            list_length(adviseGroup->candicateCombines))));
        totalCombines += list_length(adviseGroup->candicateCombines);
    }
    ereport(NOTICE, (errmsg("There are %d combinations", totalCombines)));
    u_sess->adv_cxt.isCostModeRunning = true;
    int flag = -1;
    int currentNum = 0;
    while (totalCombines != 0) {
        index = 1;

        foreach (cell, u_sess->adv_cxt.candicateAdviseGroups) {
            CHECK_FOR_INTERRUPTS();
            adviseGroup = (AdviseGroup*)lfirst(cell);
            u_sess->adv_cxt.currGroupIndex = index;
            analyzeGroupCost(adviseGroup);
            index += 1;
        }

        if (flag != currentNum * 10 / totalCombines) {
            flag = currentNum * 10 / totalCombines;
            ereport(NOTICE, (errmsg("Current progress is %d0%%", flag)));
        }

        if (currentNum == totalCombines) {
            break;
        }

        if (u_sess->adv_cxt.maxTime > 0 && u_sess->adv_cxt.endTime <= GetCurrentTimestamp()) {
            ereport(NOTICE, (errmsg("Time is over.")));
            break;
        }
        currentNum += 1;
    }
    u_sess->adv_cxt.isCostModeRunning = false;
    foreach (cell, u_sess->adv_cxt.candicateAdviseGroups) {
        CHECK_FOR_INTERRUPTS();
        adviseGroup = (AdviseGroup*)lfirst(cell);
        if (adviseGroup->bestCombineResult) {
            if (adviseGroup->minCost > 0) {
                double costImprove = 1.0 - adviseGroup->minCost / adviseGroup->originCost;
                advisorAttrDesc2Tuple(adviseGroup->bestCombineResult, costImprove);
                advisorTableDesc2Tuple(adviseGroup->replicationTableList, costImprove);
            } else {
                advisorAttrDesc2Tuple(adviseGroup->bestCombineResult, 0.0);
                advisorTableDesc2Tuple(adviseGroup->replicationTableList, 0.0);
            }
        } else {
            advisorGroupDesc2Tuple(adviseGroup);
        }
    }
    resetAdviseQuery();
    return true;
}

static void filterQuery()
{
    int totalNum = list_length(u_sess->adv_cxt.candicateQueries);
    AdviseQuery** queryArray = (AdviseQuery**)palloc(totalNum * sizeof(AdviseQuery*));
    AdviseQuery* adviseQuery = NULL;
    ListCell* cell = NULL;
    int index = 0;

    foreach (cell, u_sess->adv_cxt.candicateQueries) {
        adviseQuery = (AdviseQuery*)lfirst(cell);
        queryArray[index++] = adviseQuery;
    }
    
    qsort(queryArray, totalNum, sizeof(AdviseQuery*), queryCostCmp);
    int startPos = totalNum / 4;
    int endPos = totalNum * 3 / 4;
    Cost highCost = queryArray[startPos]->originCost * 10;
    Cost lowCost = queryArray[endPos]->originCost / 10;

    foreach (cell, u_sess->adv_cxt.candicateQueries) {
        adviseQuery = (AdviseQuery*)lfirst(cell);
        if (adviseQuery->originCost > highCost || adviseQuery->originCost < lowCost) {
            adviseQuery->isUse = false;
        }
    }

    pfree_ext(queryArray);
}

static void resetAdviseQuery()
{
    AdviseQuery* adviseQuery = NULL;
    ListCell* cell = NULL;

    foreach (cell, u_sess->adv_cxt.candicateQueries) {
        adviseQuery = (AdviseQuery*)lfirst(cell);
        adviseQuery->isUse = true;
    }
}

static void generateCandicateCombines(AdviseGroup* adviseGroup)
{
    List* repTableList = NIL;

    generateCandidateAttrWithCost(adviseGroup);
    initTableOffset(adviseGroup);
    generateQueryBitmap(adviseGroup);

    switch (u_sess->adv_cxt.adviseCompressLevel) {
        case ACL_HIGH: {
            List* disCombines = analyzeGroupHeuristic(adviseGroup, &repTableList, true);
            adviseGroup->candicateCombines = disCombines;
            adviseGroup->replicationTableList = repTableList;
        } break;
        case ACL_MED: {
            List* disCombines = analyzeGroupHeuristic(adviseGroup, &repTableList, false);
            adviseGroup->candicateCombines = disCombines;
            adviseGroup->replicationTableList = repTableList;
        } break;
        case ACL_LOW: {
            List* disTableList = NIL;
            getAdviseTableList(adviseGroup, &disTableList, &repTableList);
            adviseGroup->replicationTableList = repTableList;
            generateCombineWalker(disTableList, NIL, adviseGroup);
        } break;
        default:
            ereport(ERROR, (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized advise compress Level.")));
            break;
    }
    ereport(DEBUG1, (errmodule(MOD_ADVISOR), errmsg("Origin total cost: %f", adviseGroup->originCost)));
    adviseGroup->minCost = adviseGroup->originCost;
    adviseGroup->currentCombineIndex = 0;
}

static void analyzeGroupCost(AdviseGroup* adviseGroup)
{
    if (list_length(adviseGroup->candicateCombines) == adviseGroup->currentCombineIndex) {
        return;
    }
    List* combineResult = NIL;
    Bitmapset* combineBitmap = NULL;
    Cost cost = 0.0;
    ereport(DEBUG1, (errmodule(MOD_ADVISOR), errmsg("Current group is running, %d / %d",
        adviseGroup->currentCombineIndex, list_length(adviseGroup->candicateCombines))));
    switch (u_sess->adv_cxt.adviseCompressLevel) {
        case ACL_HIGH: {
            adviseGroup->bestCombineResult = (List*)linitial(adviseGroup->candicateCombines);
            adviseGroup->minCost = -1.0;
        } break;
        case ACL_MED: {
            combineResult = (List*)list_nth(adviseGroup->candicateCombines, adviseGroup->currentCombineIndex);
            combineBitmap = generateCombineBitmap(adviseGroup, combineResult, true);
            cost = runCombineWithCost(adviseGroup, combineBitmap);
            if (adviseGroup->minCost > cost) {
                adviseGroup->minCost = cost;
                adviseGroup->bestCombineResult = combineResult;
            }
        } break;
        case ACL_LOW: {
            combineBitmap = (Bitmapset*)list_nth(adviseGroup->candicateCombines, adviseGroup->currentCombineIndex);
            combineResult = generateCombineResult(adviseGroup, combineBitmap);
            cost = runCombineWithCost(adviseGroup, combineBitmap);
            if (adviseGroup->minCost > cost) {
                adviseGroup->minCost = cost;
                adviseGroup->bestCombineResult = combineResult;
            } else {
                list_free_ext(combineResult);
            }
        } break;
        default:
            ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("unrecognized advise compress Level.")));
            break;
    }
    adviseGroup->currentCombineIndex += 1;
}

static void generateCombineWalker(List* disTableList, List* combineResult, AdviseGroup* adviseGroup)
{
    if (list_length(disTableList) == list_length(combineResult)) {
        Bitmapset* combineBitmap = generateCombineBitmap(adviseGroup, combineResult, false);
        adviseGroup->candicateCombines = lappend(adviseGroup->candicateCombines, combineBitmap);
        return;
    }

    ListCell* cell = NULL;
    VirtualAttrGroupInfo* attrInfo = NULL;
    int index = list_length(combineResult);
    AdviseTable* adviseTable = (AdviseTable*)list_nth(disTableList, index);

    foreach (cell, adviseTable->selecteCandidateAttrs) {
        attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
        combineResult = lcons(attrInfo, combineResult);
        generateCombineWalker(disTableList, combineResult, adviseGroup);
        combineResult = list_delete_first(combineResult);
    }
}

static void getAdviseTableList(AdviseGroup* adviseGroup, List** distributionTableList, List** replicationTableList)
{
    HASH_SEQ_STATUS hashSeq;
    AdviseTable* adviseTable = NULL;
    hash_seq_init(&hashSeq, adviseGroup->candicateTables);

    while ((adviseTable = (AdviseTable*)hash_seq_search(&hashSeq)) != NULL) {
        if (adviseTable->isAssignRelication || !adviseTable->enableAdvise || !adviseTable->selecteCandidateAttrs) {
            *replicationTableList = lappend(*replicationTableList, adviseTable);
        } else {
            *distributionTableList = lappend(*distributionTableList, adviseTable);
        }
    }
}

static char* printGroupCombine(AdviseGroup* adviseGroup, Bitmapset* combineBitmap)
{
    HASH_SEQ_STATUS hash_seq;
    int tableGroupOffset = 0;
    AdviseTable* adviseTable = NULL;
    StringInfoData result;
    initStringInfo(&result);
    hash_seq_init(&hash_seq, adviseGroup->candicateTables);
    /* tablename(hash attr/replication) tablename(hash attr/replication) ... */
    while ((adviseTable = (AdviseTable*)hash_seq_search(&hash_seq)) != NULL) {
        if (adviseTable->selecteCandidateAttrs == NULL) {
            appendStringInfo(&result, "%s(replication) ", adviseTable->tableName);
            continue;
        } else {
            tableGroupOffset = adviseTable->tableGroupOffset;
            VirtualAttrGroupInfo* attrInfo = NULL;
            for (int i = 1; i <= list_length(adviseTable->selecteCandidateAttrs); i++) {
                if (bms_is_member(tableGroupOffset + i, combineBitmap)) {
                    attrInfo = (VirtualAttrGroupInfo*)list_nth(adviseTable->selecteCandidateAttrs, i - 1);
                    appendStringInfo(&result, "%s(hash %s) ", adviseTable->tableName,
                        concatAttrNames(attrInfo->attrNames, attrInfo->natts));
                }
            }
        }
    }
    return result.data;
}

static Cost runCombineWithCost(AdviseGroup* adviseGroup, Bitmapset* combineBitmap)
{
    ListCell* cell = NULL;
    AdviseQuery* adviseQuery = NULL;
    Cost tempCost = 0.0;
    VirtualTableGroupInfo* virTleGrpInfo = NULL;
    bool oldEnableFqs = u_sess->attr.attr_sql.enable_fast_query_shipping;
    u_sess->attr.attr_sql.enable_fast_query_shipping = false;

    foreach (cell, adviseGroup->candicateQueries) {
        CHECK_FOR_INTERRUPTS();
        adviseQuery = (AdviseQuery*)lfirst(cell);
        ereport(DEBUG2, (errmodule(MOD_ADVISOR), errmsg("SQL %s ", adviseQuery->query)));
        if (!adviseQuery->isUse) {
            continue;
        }

        ListCell* lc = NULL;
        foreach (lc, adviseQuery->virtualTableGroupInfos) {
            virTleGrpInfo = (VirtualTableGroupInfo*)lfirst(lc);
            if (compareReferencedTableBitmap(virTleGrpInfo->virtualTables, combineBitmap,
                                             adviseQuery->referencedTables)) {
                break;
            }
        }

        if (lc != NULL) {
            ereport(DEBUG2, (errmodule(MOD_ADVISOR), errmsg("bitmap cost %f", virTleGrpInfo->cost)));
            tempCost += virTleGrpInfo->cost * adviseQuery->frequence;
        } else {
            Cost cost = 0.0;
            /* get cost, if has error, throw the error */
            analyzeQuery(adviseQuery, false, &cost);
            ereport(DEBUG2, (errmodule(MOD_ADVISOR), errmsg("generate cost %f", cost)));
            tempCost += cost * adviseQuery->frequence;
            VirtualTableGroupInfo* newVirTleGrpInfo = (VirtualTableGroupInfo*)palloc(sizeof(VirtualTableGroupInfo));
            newVirTleGrpInfo->virtualTables = combineBitmap;
            newVirTleGrpInfo->cost = cost;
            adviseQuery->virtualTableGroupInfos = lappend(adviseQuery->virtualTableGroupInfos, newVirTleGrpInfo);
        }
    }
    u_sess->attr.attr_sql.enable_fast_query_shipping = oldEnableFqs;
    ereport(DEBUG1, (errmodule(MOD_ADVISOR), errmsg("Current combine total cost: %f %s",
        tempCost, printGroupCombine(adviseGroup, combineBitmap))));
    return tempCost;
}

static bool compareReferencedTableBitmap(Bitmapset* a, Bitmapset* b, Bitmapset* referencedTables)
{
    Bitmapset* unionA = bms_intersect(a, referencedTables);
    Bitmapset* unionB = bms_intersect(b, referencedTables);
    
    Bitmapset* differenceAB = bms_difference(unionA, unionB);
    return bms_is_empty(differenceAB);
}

static void filterSelecteCandidateAttrs(AdviseGroup* adviseGroup)
{
    HASH_SEQ_STATUS hashSeq;
    AdviseTable* adviseTable = NULL;
    int numReplication = 0;
    /* Each table only choose top 2 attr */
    hash_seq_init(&hashSeq, adviseGroup->candicateTables);
    while ((adviseTable = (AdviseTable*)hash_seq_search(&hashSeq)) != NULL) {
        if (adviseTable->selecteCandidateAttrs == NULL || adviseTable->isAssignRelication) {
            numReplication += 1;
        } else {
            sortSelecteCandidateAttrs(adviseTable, true);
        }
    }

    VirtualAttrGroupInfo* attrInfo = NULL;
    long numTotalTables = hash_get_num_entries(adviseGroup->candicateTables);
    /* if distribution table is too much, we delete  */
    if (numTotalTables - numReplication > FILETER_ATTR_DISTRIBUTION_NUM_THRESHOLD) {
        hash_seq_init(&hashSeq, adviseGroup->candicateTables);
        while ((adviseTable = (AdviseTable*)hash_seq_search(&hashSeq)) != NULL) {
            if (list_length(adviseTable->selecteCandidateAttrs) == FILETER_ATTR_NUM) {
                attrInfo = (VirtualAttrGroupInfo*)lsecond(adviseTable->selecteCandidateAttrs);
                if (attrInfo->joinQuals == NULL) {
                    adviseTable->selecteCandidateAttrs = list_delete_ptr(adviseTable->selecteCandidateAttrs, attrInfo);
                }
            }
        }
    }
}

static void generateCandidateAttrWithCost(AdviseGroup* adviseGroup)
{
    HASH_SEQ_STATUS hash_seq;
    AdviseTable* adviseTable = NULL;
    int num_coords = 0;
    int num_datanodes = 0;
    int64 totalCombines = 1;

    PgxcNodeCount(&num_coords, &num_datanodes);
    hash_seq_init(&hash_seq, adviseGroup->candicateTables);
    while ((adviseTable = (AdviseTable*)hash_seq_search(&hash_seq)) != NULL) {
        if (adviseTable->isAssignRelication != true) {
            ListCell* cell = NULL;
            float8 reltuples = getRelReltuples(adviseTable->oid);
            Bitmapset* intersectRes = NULL;
            if (u_sess->adv_cxt.isConstraintPrimaryKey) {
                intersectRes = generateTableKeyBitmap(adviseTable->oid);
            }
            
            VirtualAttrGroupInfo* attrInfo = NULL;
            if (bms_is_empty(intersectRes)) {
                foreach (cell, adviseTable->totalCandidateAttrs) {
                    attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
                    if (attrInfo->natts == 1 &&
                        isCandidateAttrSatisfiedStadistinct(adviseTable, attrInfo, reltuples, num_datanodes)) {
                        adviseTable->selecteCandidateAttrs = lappend(adviseTable->selecteCandidateAttrs, attrInfo);
                    }
                }
            } else {
                foreach (cell, adviseTable->totalCandidateAttrs) {
                    attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
                    if (attrInfo->natts == 1 && bms_is_member(attrInfo->attrNums[0], intersectRes) &&
                        isCandidateAttrSatisfiedStadistinct(adviseTable, attrInfo, reltuples, num_datanodes)) {
                        adviseTable->selecteCandidateAttrs = lappend(adviseTable->selecteCandidateAttrs, attrInfo);
                    }
                }
            }
            bms_free(intersectRes);
            if (adviseTable->selecteCandidateAttrs != NULL) {
                totalCombines *= list_length(adviseTable->selecteCandidateAttrs);
            }
        }
    } 

    /* if total Combines > threshold, we should filter some selecteCandidateAttrs */
    if (u_sess->adv_cxt.adviseCompressLevel == ACL_LOW && totalCombines > TOTAL_COMBINATION_THRESHOLD) {
        filterSelecteCandidateAttrs(adviseGroup);
    }
}

static void initTableOffset(AdviseGroup* adviseGroup)
{
    HASH_SEQ_STATUS hash_seq;
    int tableGroupOffset = 0;
    AdviseTable* adviseTable = NULL;
    hash_seq_init(&hash_seq, adviseGroup->candicateTables);
    while ((adviseTable = (AdviseTable*)hash_seq_search(&hash_seq)) != NULL) {
        adviseTable->tableGroupOffset = tableGroupOffset;        
        if (adviseTable->isAssignRelication || !adviseTable->selecteCandidateAttrs) {
            tableGroupOffset += 1;
        } else {
            tableGroupOffset += 1 + list_length(adviseTable->selecteCandidateAttrs);
        }
    }
}

static List* generateCombineResult(AdviseGroup* adviseGroup, Bitmapset* combineBitmap)
{
    HASH_SEQ_STATUS hash_seq;
    int tableGroupOffset = 0;
    AdviseTable* adviseTable = NULL;
    List* combineResult = NIL;
    hash_seq_init(&hash_seq, adviseGroup->candicateTables);
    while ((adviseTable = (AdviseTable*)hash_seq_search(&hash_seq)) != NULL) {
        if (adviseTable->isAssignRelication || !adviseTable->enableAdvise || !adviseTable->selecteCandidateAttrs) {
            adviseTable->currDistributionKey = NULL;
            continue;
        } else {
            tableGroupOffset = adviseTable->tableGroupOffset;
            VirtualAttrGroupInfo* attrInfo = NULL;
            for (int i = 1; i <= list_length(adviseTable->selecteCandidateAttrs); i++) {
                if (bms_is_member(tableGroupOffset + i, combineBitmap)) {
                    attrInfo = (VirtualAttrGroupInfo*)list_nth(adviseTable->selecteCandidateAttrs, i - 1);
                    adviseTable->currDistributionKey = attrInfo;
                    combineResult = lappend(combineResult, attrInfo);
                    ereport(DEBUG1, (errmodule(MOD_ADVISOR), errmsg("Table %s attr %s", get_rel_name(attrInfo->oid),
                        concatAttrNames(attrInfo->attrNames, attrInfo->natts))));
                }
            }
        }
    }

    return combineResult;
}

static Bitmapset* generateCombineBitmap(AdviseGroup* adviseGroup, List* result, bool isFillinTable)
{
    ListCell* cell = NULL;
    List* bitmapOffsets = NULL;
    VirtualAttrGroupInfo* attrInfo = NULL;
    AdviseTable* adviseTable = NULL;
    bool found = true;

    foreach (cell, result) {
        CHECK_FOR_INTERRUPTS();
        attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
        adviseTable = (AdviseTable*)hash_search(
            adviseGroup->candicateTables, (void*)&(attrInfo->oid), HASH_FIND, &found);
        
        if (found) {
            int index = getVirtualAttrIndex(adviseTable->selecteCandidateAttrs, attrInfo);
            ereport(DEBUG1, (errmodule(MOD_ADVISOR), errmsg("Table %s attr %s", get_rel_name(attrInfo->oid),
                concatAttrNames(attrInfo->attrNames, attrInfo->natts))));
            if (index != -1) {
                bitmapOffsets = lappend_int(bitmapOffsets, adviseTable->tableGroupOffset + index);
                if (isFillinTable) {
                    adviseTable->currDistributionKey = attrInfo;
                }
            } else {
                ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("The attr %d in table %d dosen't find.", attrInfo->attrNums[0], attrInfo->oid)));
            }
        } else {
            ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("The table %d dosen't find.", attrInfo->oid)));
        }
    }
    
    foreach (cell, adviseGroup->replicationTableList) {
        adviseTable = (AdviseTable*)lfirst(cell);
        if (adviseTable->enableAdvise) {
            ereport(DEBUG1, (errmodule(MOD_ADVISOR), errmsg("replication Table %s ",
                get_rel_name(adviseTable->oid))));
            bitmapOffsets = lappend_int(bitmapOffsets, adviseTable->tableGroupOffset);
        }
    }

    Bitmapset* bitmap = generateBitmap(bitmapOffsets);
    list_free_ext(bitmapOffsets);
    return bitmap;
}

static int getVirtualAttrIndex(List* totalCandidateAttrs, VirtualAttrGroupInfo* attrInfo)
{
    ListCell* cell = NULL;
    VirtualAttrGroupInfo* attrCell = NULL;
    int pos = 1;
    foreach (cell, totalCandidateAttrs) {
        attrCell = (VirtualAttrGroupInfo*)lfirst(cell);
        if (compareAttrs(attrInfo->attrNums, attrInfo->natts, attrCell->attrNums, attrCell->natts)) {
            return pos;
        }
        pos += 1;
    }

    return -1;
}

static void generateQueryBitmap(AdviseGroup* adviseGroup)
{
    ListCell* cell = NULL;
    bool found = true;

    foreach (cell, adviseGroup->candicateQueries) {
        AdviseQuery* adviseQuery = (AdviseQuery*)lfirst(cell);
        ListCell* lc = NULL;
        List* referencedBitmapOffsets = NIL;
        List* originBitmapOffsets = NIL;
        bool isGenerateOriginBitmap = true;
        foreach (lc, adviseQuery->tableOids) {
            AdviseTable* adviseTable = (AdviseTable*)hash_search(
                adviseGroup->candicateTables, (void*)&(lfirst_oid(lc)), HASH_FIND, &found);

            if (found) {
                char tableType = GetLocatorType(adviseTable->oid);
                if (!adviseTable->enableAdvise) {
                    isGenerateOriginBitmap = false;
                } else if (adviseTable->isAssignRelication || !adviseTable->selecteCandidateAttrs) {
                    referencedBitmapOffsets = lappend_int(referencedBitmapOffsets, adviseTable->tableGroupOffset);
                    if (tableType == LOCATOR_TYPE_REPLICATED) {
                        originBitmapOffsets = lappend_int(originBitmapOffsets, adviseTable->tableGroupOffset);
                    } else {
                        isGenerateOriginBitmap = false;
                    }
                } else {
                    for (int i = 1; i <= list_length(adviseTable->selecteCandidateAttrs); i++) {
                        referencedBitmapOffsets =
                            lappend_int(referencedBitmapOffsets, adviseTable->tableGroupOffset + i);
                    }
                    if (tableType == LOCATOR_TYPE_HASH) {
                        int pos = getDistrKeyPosition(adviseTable);
                        if (pos != -1) {
                            originBitmapOffsets =
                                lappend_int(originBitmapOffsets, adviseTable->tableGroupOffset + pos);
                        } else {
                            isGenerateOriginBitmap = false;
                        }
                    } else {
                        isGenerateOriginBitmap = false;
                    } 
                }
            }
        }
        adviseQuery->referencedTables = generateBitmap(referencedBitmapOffsets);

        if (isGenerateOriginBitmap) {
            VirtualTableGroupInfo* virTleGrpInfo = (VirtualTableGroupInfo*)MemoryContextAlloc(
                CurrentMemoryContext, sizeof(VirtualTableGroupInfo));
            virTleGrpInfo->virtualTables = generateBitmap(originBitmapOffsets);
            virTleGrpInfo->cost = adviseQuery->originCost;
            adviseQuery->virtualTableGroupInfos = lappend(adviseQuery->virtualTableGroupInfos, virTleGrpInfo);
        }

        list_free_ext(originBitmapOffsets);
        list_free_ext(referencedBitmapOffsets);
    }
}

static Bitmapset* generateBitmap(List* bitmapOffsets)
{
    Bitmapset* bitmap = NULL;
    ListCell* cell = NULL;

    foreach (cell, bitmapOffsets) {
        if (bitmap == NULL) {
            bitmap = bms_make_singleton(lfirst_int(cell));
        } else {
            bitmap = bms_add_member(bitmap, lfirst_int(cell));
        }
    }

    return bitmap;
}

static int getDistrKeyPosition(AdviseTable* adviseTable)
{
    ListCell* cell = NULL;
    int pos = 1;
    if (adviseTable->originDistributionKey == NIL || list_length(adviseTable->originDistributionKey) > 1) {
        return -1;
    }
    AttrNumber distributeKey = linitial_int(adviseTable->originDistributionKey);

    foreach (cell, adviseTable->totalCandidateAttrs) {
        VirtualAttrGroupInfo* attrInfo = (VirtualAttrGroupInfo*)lfirst(cell);
        if (attrInfo->natts == 1 && attrInfo->attrNums[0] == distributeKey) {
            return pos;
        }

        pos += 1;
    }

    return -1;
}

/*
 * query: A JOIN B SUBLINK C 
 */
static void queryDivideGroup()
{
    AdviseGroup* adviseGroup = NULL;
    List* tableOids = NIL;
    List* groups = NIL;
    ListCell* cell = NULL;

    foreach (cell, u_sess->adv_cxt.candicateQueries) {
        CHECK_FOR_INTERRUPTS();
        AdviseQuery* adviseQuery = (AdviseQuery*)lfirst(cell);
        tableOids = adviseQuery->tableOids;
        if (tableOids == NIL || !adviseQuery->isUse) {
            continue;
        }
        groups = getAdviseGroups(tableOids);
        if (groups == NULL) {
            adviseGroup = createAdviseGroup();
            u_sess->adv_cxt.candicateAdviseGroups =
                lappend(u_sess->adv_cxt.candicateAdviseGroups, adviseGroup);
        } else if (list_length(groups) == 1) {
            adviseGroup = (AdviseGroup*)linitial(groups);
        } else {
            adviseGroup = concatAdviseGroup(groups);
        }
        fillinGroupWithTable(adviseGroup, tableOids);
        fillinGroupWithQuery(adviseGroup, adviseQuery);
    }

    list_free_ext(groups);
}

static AdviseQuery* copyAdviseQuery(AdviseQuery* src)
{
    AdviseQuery* dest= (AdviseQuery*)MemoryContextAlloc(CurrentMemoryContext, sizeof(AdviseQuery));
    dest->query = pstrdup(src->query);
    dest->boundParams = copyDynParam(src->boundParams);
    dest->cursorOptions = src->cursorOptions;
    dest->searchPath = copyAdviseSearchPath(src->searchPath);
    dest->frequence = src->frequence;
    dest->originCost = src->originCost;
    dest->tableOids = copyTableOids(src->tableOids);
    dest->referencedTables = src->referencedTables;
    dest->virtualTableGroupInfos = src->virtualTableGroupInfos;
    dest->isUse = src->isUse;
    return dest;
}

static List* copyTableOids(List* src)
{
    List* dest = NIL;
    ListCell* cell = NULL;
    foreach (cell, src) {
        dest = lappend_oid(dest, lfirst_oid(cell));
    }

    return dest;
}

static void fillinGroupWithQuery(AdviseGroup* adviseGroup, AdviseQuery* adviseQuery)
{
    AdviseQuery* copyQuery = copyAdviseQuery(adviseQuery);
    checkSessAdvMemSize();
    adviseGroup->candicateQueries = lappend(adviseGroup->candicateQueries, copyQuery);
    adviseGroup->originCost += copyQuery->originCost * copyQuery->frequence;
}

static bool isCandidateAttrSatisfiedStadistinct(AdviseTable* adviseTable, VirtualAttrGroupInfo* attrInfo,
                                                float8 reltuples, int numDatanodes)
{
    float4 stadistinct = getAttrStadistinct(adviseTable->oid, attrInfo->attrNums[0]);
    if (stadistinct == 0) {
        return false;
    } else if (stadistinct > 0) {
        if (stadistinct > 100 * numDatanodes) {
            return true;
        }
    } else {
        if (-reltuples * stadistinct > 100 * numDatanodes) {
            return true;
        }
    }
    return false;
}

/* if has select into clause, return true. */
bool checkSelectIntoParse(SelectStmt* stmt)
{
    if (stmt != NULL) {
        /* don't support select into... */
        if (stmt->intoClause != NULL) {
            return true;
        } else {
            return checkSelectIntoParse(stmt->larg) || checkSelectIntoParse(stmt->rarg);
        }
    }
    return false;
}

/* sql advisor only support DML or DQL query. */ 
bool checkParsetreeTag(Node* parsetree)
{
    bool result = false;
    switch (nodeTag(parsetree)) {
         /* raw plannable queries */
        case T_InsertStmt:
        case T_DeleteStmt:
        case T_UpdateStmt:
        case T_MergeStmt: {
            result = true;
        } break;
        case T_SelectStmt: {
            if (checkSelectIntoParse((SelectStmt*)parsetree)) {
                result = false;
            } else {
                result = true;
            }
        } break;
        default:
            result = false;
            break;
    }

    return result;
}

bool checkCommandTag(const char* commandTag)
{
    if (strcmp(commandTag, "SELECT") == 0 ||
        strcmp(commandTag, "UPDATE") == 0 ||
        strcmp(commandTag, "INSERT") == 0 ||
        strcmp(commandTag, "DELETE") == 0 ||
        strcmp(commandTag, "MERGE") == 0) {
        return true;
    }
    return false;
}

bool compareAttrs(AttrNumber* attnumA, int numA, AttrNumber* attnumB, int numB)
{
    if (numA != numB) {
        return false;
    }

    for (int i = 0; i < numA; i++) {
        if (attnumA[i] != attnumB[i]) {
            return false;
        }
    }

    return true;
}

static float4 getAttrStadistinct(Oid relid, AttrNumber attnum)
{
    HeapTuple tp;
    float4 stadistinct;
    Relation rel = relation_open(relid, NoLock);
    bool ispartition = RelationIsPartition(rel);
    char stakind = ispartition ? STARELKIND_PARTITION : STARELKIND_CLASS;

    if (!ispartition && get_rel_persistence(relid) == RELPERSISTENCE_GLOBAL_TEMP) {
        relation_close(rel, NoLock);
        return 0;
    }

    tp = SearchSysCache4(
        STATRELKINDATTINH, ObjectIdGetDatum(relid), CharGetDatum(stakind), Int16GetDatum(attnum), BoolGetDatum(false));
    if (!HeapTupleIsValid(tp)) {
        relation_close(rel, NoLock);
        return 0;
    }
    
    stadistinct = ((Form_pg_statistic)GETSTRUCT(tp))->stadistinct;
    relation_close(rel, NoLock);
    ReleaseSysCache(tp);
    return stadistinct;
}

/*
 * getRelReltuples
 *
 *		Returns the reltuples associated with a given relation.
 */
static float8 getRelReltuples(Oid relid)
{
    HeapTuple tp;
    tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);
        float8 result;
        result = reltup->reltuples;
        ReleaseSysCache(tp);
        return result;
    } else {
        return 0.0;
    }
}

static List* getIndexIndkeyList(Oid tableoid)
{
    Relation indrel = NULL;
    SysScanDesc indscan = NULL;
    ScanKeyData skey;
    HeapTuple htup = NULL;
    List* indkey_list = NIL;

    /* Prepare to scan pg_index for entries having indrelid = this rel. */
    ScanKeyInit(&skey, Anum_pg_index_indrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(tableoid));

    indrel = heap_open(IndexRelationId, AccessShareLock);
    indscan = systable_beginscan(indrel, IndexIndrelidIndexId, true, SnapshotNow, 1, &skey);

    while (HeapTupleIsValid(htup = systable_getnext(indscan))) {
        List* indkey = NIL;
        Form_pg_index index = (Form_pg_index)GETSTRUCT(htup);
        /*
         * Ignore any indexes that are currently being dropped.  This will
         * prevent them from being searched, inserted into, or considered in
         * HOT-safety decisions.  It's unsafe to touch such an index at all
         * since its catalog entries could disappear at any instant.
         */
        if (!IndexIsLive(index))
            continue;

        if ((index->indisunique || index->indisprimary) && heap_attisnull(htup, Anum_pg_index_indexprs, NULL) &&
            heap_attisnull(htup, Anum_pg_index_indpred, NULL)) {
            for (int i = 0; i < index->indnatts; i++) {
                if (index->indkey.values[i] >= 0) {
                    indkey = lappend_int(indkey, index->indkey.values[i]);
                }
            }
            indkey_list = lappend(indkey_list, indkey);
        }
    }

    systable_endscan(indscan);
    heap_close(indrel, AccessShareLock);

    return indkey_list;
}
