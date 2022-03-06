#include <stdio.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include "access/dfs/dfs_common.h"
#include "access/dfs/dfs_query.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "access/dfs/carbondata_index_reader.h"
#endif
#include "access/dfs/dfs_stream.h"
#include "access/dfs/dfs_stream_factory.h"
#include "hdfs_fdw.h"
#include "scheduler.h"
#include "access/hash.h"
#include "access/relscan.h"
#include "catalog/pgxc_node.h"
#include "catalog/pg_partition_fn.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "nodes/nodes.h"
#include "optimizer/cost.h"
#include "optimizer/predtest.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/pgxc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "access/heapam.h"
#include "utils/syscache.h"
#include "dfs_adaptor.h"

typedef struct PartitionInfoCacheKey {
    Oid relOid;
    uint32 scanClauseHashValue;
} PartitionInfoCacheKey;

typedef struct PartitionInfoCacheEntry {
    PartitionInfoCacheKey key;
    int64 lastModifyTime;
    List* splitList;
    List* prunningResult;
} PartitionInfoCacheEntry;

typedef struct partition_context {
    /*It is used to store the restirction on partition column. */
    List* partClauseList;
    /*It is used to store varattno list of partition columns. */
    List* partColList;
} partition_context;

typedef struct partition_string_context {
    /* It is used to store built partition path for partition restriction. */
    List* partColStrList;
    /*
     * It is used to store varattno of partition columns, each listcell of
     * partColStrList and partColNoList is one by one correspondence.
     */
    List* partColNoList;
    Oid foreignTblOid;
} partition_string_context;

extern char* tcp_link_addr;

static void AssignReplicaNode(HTAB* htab, const Oid* dnOids, const uint32_t nodeNum, const List* fileList);
static bool AssignRemoteNode(HTAB* htab, int nodeNum, Oid* dnOids, SplitInfo* currentFile, bool isAnalyze);
static dnWork* AssignRequestFilesToDn(HTAB* htab, List* fileList, int filesNum, dfs::DFSConnector* conn);
static char* parseMultiFileNames(char** fileNames, bool checkRootDir, char delimiter);
static int getAnalyzeFilesNum(int dataNodeNum, int totalFilesNum);
static bool isNodeLocalToFile(Form_pgxc_node nodeForm, const char* blLocation);
static List* GetAllFiles(dfs::DFSConnector* conn, Oid foreignTableId, ServerTypeOption srvType, List* columnList = NIL,
    List* scanClauseList = NIL);
static List* GetObsAllFiles(dfs::DFSConnector* conn, Oid foreignTableId, List* columnList, List*& prunningResult,
    List*& partList, List* scanClauses);
static List* GetHdfsAllFiles(dfs::DFSConnector* conn, Oid foreignTableId, List* columnList, List*& prunningResult,
    List*& partList, List* scanClauses);
static List* GetSubFiles(dfs::DFSConnector* conn, SplitInfo* split, int colNo);
static List* DigFiles(dfs::DFSConnector* conn, SplitInfo* split);
static List* PartitionPruneProcess(dfs::DFSConnector* conn, List* partitionRelatedList, List* scanClauses,
    Oid foreignTableId, List*& prunningResult, List*& partList, ServerTypeOption srvType);
static void CheckPartitionColNumber(
    dfs::DFSConnector* conn, List* partList, List* fileList, Oid foreignTableId, ServerTypeOption srvType);
static SplitInfo* CheckOneSubSplit(dfs::DFSConnector* conn, SplitInfo* split, bool isLastPartition, Oid foreignTableId);
static bool PartitionFilterClause(SplitInfo* split, List* scanClauses, Var* value, Expr* equalExpr);
static void CollectPartPruneInfo(List*& prunningResult, int sum, int notprunning, int colno, Oid relOid);
static List* DrillDown(dfs::DFSConnector* conn, List* fileList);
static bool AssignLocalNode(
    HTAB* htab, uint64* Locations, const char* pChached, uint32 LocationSize, SplitInfo* currentFile, bool isAnalyze);
static int GetDnIpAddrByOid(Oid* DnOid, uint32 OidSize, uint64* OidIp, uint32 OidIpSize);
static int StatDn(uint64* dnInfo, uint32 dnCnt, dnInfoStat* statDnInfo, uint32 statDnCnt);
static int CompareByLowerInt32(const void* Elem1, const void* Elem2);
static int CompareByIp(const void* Elem1, const void* Elem2);
static Value* getPartitionValue(dfs::DFSConnector* conn, char* partitionStr, char* ObjectStr);
static void obsFileScheduling(HTAB* htab, List* FileList, Oid* dnOids, int numOfNodes, char locatorType);
static void hdfsFileScheduling(
    dfs::DFSConnector* conn, HTAB* htab, List* FileList, Oid* dnOids, int numOfNodes, char locatorType, bool isAnalyze);
static void SpillToDisk(Index relId, List* allTask, dfs::DFSConnector* conn);
static char* getPrefixPath(dfs::DFSConnector* conn);
static void flushToRemote(SplitMap* dnTask, const char* buffer, dfs::DFSConnector* conn);
static void loadDiskSplits(SplitMap* dnTask, dfs::DFSConnector* conn);
void scan_expression_tree_walker(Node* node, bool (*walker)(), void* context);
void getPartitionClause(Node* node, partition_context* context);
void getPartitionString(Node* node, partition_string_context* context);
bool isEquivalentExpression(Oid opno);

extern List* CNSchedulingForDistOBSFt(Oid foreignTableId);

#ifdef ENABLE_MULTIPLE_NODES
static List* ExtractNonParamRestriction(List* opExpressionList);
List* CarbonDataFile(dfs::DFSConnector* conn, List* fileList, List* allColumnList, List* restrictColumnList,
                     List* scanClauses, int16 attrNum);
#endif

uint32 best_effort_use_cahce = 0;  // not use

List* CNScheduling(Oid foreignTableId, Index relId, List* columnList, List* scanClauses, List*& prunningResult,
    List*& partList, char locatorType, bool isAnalyze, List* allColumnList, int16 attrNum, int64* fileNum)
{
    int numOfNodes = 0;

    Oid* dnOids = NULL;
    HTAB* HTab = NULL;
    HASHCTL HashCtl;

    List* FileList = NIL;
    List* PartitionRelatedList = NIL;
    errno_t rc = EOK;

    ServerTypeOption srvType = T_INVALID;

    if (IS_PGXC_DATANODE) {
        QUERY_NOT_SUPPORT(foreignTableId,
            "Query on datanode is not "
            "supported currently for the foreign table:%s.");
    }

    numOfNodes = get_pgxc_classnodes(foreignTableId, &dnOids);
    Assert(NULL != dnOids && numOfNodes > 0);

    srvType = getServerType(foreignTableId);

    /* initialize the hash table which is used for storing dn's assigned files */
    rc = memset_s(&HashCtl, sizeof(HashCtl), 0, sizeof(HashCtl));
    securec_check(rc, "\0", "\0");
    HashCtl.keysize = sizeof(Oid);
    HashCtl.entrysize = sizeof(dnWork);
    HashCtl.hash = oid_hash;
    HashCtl.hcxt = CurrentMemoryContext;
    HTab = hash_create("SchedulerHashTable", 128, &HashCtl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    dfs::DFSConnector* conn = dfs::createConnector(CurrentMemoryContext, foreignTableId);

    /* get file list */
    switch (srvType) {
        case T_OBS_SERVER: {
            /* get all obs files need to be schedule */
            FileList = GetObsAllFiles(conn, foreignTableId, columnList, prunningResult, partList, scanClauses);
            break;
        }
        case T_HDFS_SERVER: {
            /* get all hdfs files need to be schedule */
            FileList = GetHdfsAllFiles(conn, foreignTableId, columnList, prunningResult, partList, scanClauses);
            break;
        }
        default: {
            Assert(0);
            break;
        }
    }

    if (0 == list_length(FileList)) {
        delete (conn);
        conn = NULL;
        return NIL;
    }

    /* Start to process partition info */
    PartitionRelatedList = list_make2(FileList, columnList);

    /* Process the diretories of each layer of partition in order. */
    FileList = PartitionPruneProcess(
        conn, PartitionRelatedList, scanClauses, foreignTableId, prunningResult, partList, srvType);

#ifdef ENABLE_MULTIPLE_NODES
    char* format = HdfsGetOptionValue(foreignTableId, OPTION_NAME_FORMAT);
    /* check data format is carbondata, if is carbondata, analysis and filter */
    if (0 == pg_strcasecmp(format, DFS_FORMAT_CARBONDATA)) {
        FileList = CarbonDataFile(conn, FileList, allColumnList, columnList, scanClauses, attrNum);
    }
#endif

    if (0 == list_length(FileList)) {
        delete (conn);
        conn = NULL;
        return NIL;
    }

    if (NULL != fileNum) {
        *fileNum = list_length(FileList);
    }

    /* file schedule */
    switch (srvType) {
        case T_OBS_SERVER: {
            /* Check if the file list is empty again after the partition prunning. */
            if (0 == list_length(FileList)) {
                delete (conn);
                conn = NULL;
                return NIL;
            }

            obsFileScheduling(HTab, FileList, dnOids, numOfNodes, locatorType);
            break;
        }
        case T_HDFS_SERVER: {
            /*
             * Sometimes the hive partition layers is more than ours partition defination, so here we need to dig
             * down to find all the files.
             */
            FileList = DrillDown(conn, FileList);

            /* Check if the file list is empty again after the partition prunning and drilling. */
            if (0 == list_length(FileList)) {
                delete (conn);
                conn = NULL;
                return NIL;
            }

            hdfsFileScheduling(conn, HTab, FileList, dnOids, numOfNodes, locatorType, isAnalyze);
            break;
        }
        default: {
            Assert(0);
            break;
        }
    }

    List* allTask = NIL;
    dnWork* Item = NULL;
    HASH_SEQ_STATUS ScanStatus;

    hash_seq_init(&ScanStatus, HTab);
    ereport(LOG, (errmodule(MOD_HDFS), errmsg("Total %d files, %d datanodes", list_length(FileList), numOfNodes)));

    while ((Item = (dnWork*)hash_seq_search(&ScanStatus)) != NULL) {
        ListCell* lc = NULL;
        SplitMap* dnTask = makeNode(SplitMap);

        dnTask->nodeId = PGXCNodeGetNodeId(Item->nodeOid, PGXC_NODE_DATANODE);
        dnTask->locatorType = locatorType;
        dnTask->splits = Item->toDoList;
        dnTask->fileNums = list_length(dnTask->splits);
        foreach (lc, dnTask->splits) {
            SplitInfo* split = (SplitInfo*)lfirst(lc);
            dnTask->totalSize += split->ObjectSize;
        }

        allTask = lappend(allTask, dnTask);
        ereport(DEBUG1,
            (errmodule(MOD_HDFS),
                errmsg(
                    "Datanode %s, assigned %d files", get_pgxc_nodename(Item->nodeOid), list_length(Item->toDoList))));
    }

    hash_destroy(HTab);
    HTab = NULL;

    Assert(allTask != NIL);

    /* check the allTask, If the size of splits */
    if ((!t_thrd.postgres_cxt.mark_explain_only && !isAnalyze) &&
        list_length(FileList) >= u_sess->attr.attr_sql.schedule_splits_threshold) {
        SpillToDisk(relId, allTask, conn);
    }

    delete (conn);
    conn = NULL;

    /* free early will have problem */
    list_free(FileList);
    FileList = NIL;

    return allTask;
}

/**
 * @Description: scheduler OBS objects for datanodes.
 * @in htab, the hash table.
 * @in FileList, the objects to be scheduled.
 * @in dnOids, the datanode arrary.
 * @in numOfNodes, the datanode number.
 # @in locatorType, distribute type of the given table .
 * @return node.
 */
void obsFileScheduling(HTAB* htab, List* FileList, Oid* dnOids, int numOfNodes, char locatorType)
{
    if (LOCATOR_TYPE_REPLICATED == locatorType) {
        AssignReplicaNode(htab, dnOids, numOfNodes, FileList);
    } else {
        ListCell* FileCell = NULL;
        int num_processed = 0;
        int taskCnt = 0;
        int fileCnt = list_length(FileList);

        taskCnt = MIN(fileCnt, numOfNodes);

        if (taskCnt == 0)
            taskCnt = 1;

        foreach (FileCell, FileList) {
            // filter object size is zero
            SplitInfo* splitinfo = (SplitInfo*)lfirst(FileCell);
            bool found = false;
            CHECK_FOR_INTERRUPTS();

            Oid nodeOid = dnOids[num_processed % taskCnt];
            dnWork* item = (dnWork*)hash_search(htab, &nodeOid, HASH_ENTER, &found);
            if (!found) {
                item->toDoList = NIL;
            }

            item->toDoList = lappend(item->toDoList, splitinfo);

            num_processed++;
            ereport(DEBUG1,
                (errmodule(MOD_OBS),
                    errmsg("Assign object %s to datanode:%s",
                        ((SplitInfo*)lfirst(FileCell))->filePath,
                        get_pgxc_nodename(nodeOid))));
        }
    }
}

/**
 * @Description: scheduler hdfs files for datanodes.
 * @in htab, the hash table.
 * @in FileList, the objects to be scheduled.
 * @in dnOids, the datanode arrary.
 * @in numOfNodes, the datanode number.
 # @in locatorType, distribute type of the given table .
 * @in isAnalyze, if the isAnalyze is true, we are executing an analyze command.
 * @return node.
 */
static void hdfsFileScheduling(
    dfs::DFSConnector* conn, HTAB* htab, List* FileList, Oid* dnOids, int numOfNodes, char locatorType, bool isAnalyze)
{
    ListCell* FileCell = NULL;
    SplitInfo* Split = NULL;
    char szIp[32] = {0};
    uint64* dnInfo = NULL;
    dnInfoStat* statDnInfo = NULL;

    /*get dn nodes ip and combo with oid*/
    dnInfo = (uint64*)palloc0(numOfNodes * sizeof(uint64));
    statDnInfo = (dnInfoStat*)palloc0(numOfNodes * sizeof(dnInfoStat));
    int dnCnt = GetDnIpAddrByOid(dnOids, numOfNodes, dnInfo, numOfNodes);

    /*sorted by lower int32 for using bsearch*/
    ::qsort(dnInfo, dnCnt, sizeof(uint64), CompareByLowerInt32);

    /*stat by ip*/
    int statDnCnt = StatDn(dnInfo, dnCnt, statDnInfo, numOfNodes);

    /* used for generate a random start position in AssignLocalNode */
    ::srand((unsigned)time(NULL));

    if (LOCATOR_TYPE_REPLICATED == locatorType) {
        AssignReplicaNode(htab, dnOids, numOfNodes, FileList);
    } else {
        uint64* pLocal = (uint64*)palloc0(sizeof(uint64) * MAX_ROUNDROBIN_AVAILABLE_DN_NUM);
        char* pCached = (char*)palloc0(sizeof(char) * MAX_ROUNDROBIN_AVAILABLE_DN_NUM);
        bool needPredicate = true;
        int fileCount = 0;
        int localFileCount = 0;
        errno_t Ret = EOK;

        Ret = memset_s(pCached, MAX_ROUNDROBIN_AVAILABLE_DN_NUM, 0, MAX_ROUNDROBIN_AVAILABLE_DN_NUM);
        securec_check(Ret, "", "");
        foreach (FileCell, FileList) {
            CHECK_FOR_INTERRUPTS();
            Split = (SplitInfo*)lfirst(FileCell);
            char* CurrentFileName = Split->filePath;
            uint32 LocalCnt = 0;

            if (needPredicate) {
                dfs::DFSBlockInfo* BlInf = conn->getBlockLocations(CurrentFileName);
                Assert(BlInf != NULL);
                int ReplNum = BlInf->getNumOfReplica();
                const char* pName = NULL;

                for (int Loop = 0; Loop < ReplNum; Loop++) {
                    pName = BlInf->getNames(0, Loop);
                    Assert(pName != NULL);
                    Ret = strcpy_s(szIp, (sizeof(szIp) - 1), pName);
                    securec_check(Ret, "", "");
                    /*remove port info*/
                    char* pStr = ::strrchr(szIp, ':');
                    if (pStr != NULL) {
                        *pStr = '\0';
                    }

                    uint64 TmpVal = (uint64)inet_addr(szIp);
                    dnInfoStat* pdnInfoStat =
                        (dnInfoStat*)::bsearch(&TmpVal, statDnInfo, statDnCnt, sizeof(dnInfoStat), CompareByIp);

                    if (pdnInfoStat != NULL) /*is in dn*/
                    {
                        /* save all local */
                        Ret = memcpy_s(&pLocal[LocalCnt],
                            ((MAX_ROUNDROBIN_AVAILABLE_DN_NUM - LocalCnt) * sizeof(uint64)),
                            &dnInfo[pdnInfoStat->Start],
                            pdnInfoStat->Cnt * sizeof(uint64));
                        securec_check(Ret, "", "");
                        LocalCnt += pdnInfoStat->Cnt;

                        if (BlInf->isCached(0, Loop)) {
                            Ret = memset_s(&pCached[LocalCnt], pdnInfoStat->Cnt, 1, pdnInfoStat->Cnt);
                            securec_check(Ret, "", "");
                        }
                    }
                }
                delete (BlInf);
                BlInf = NULL;
            }

            /*check whether all locations are remote*/
            if (LocalCnt != 0) {
                (void)AssignLocalNode(htab, pLocal, pCached, LocalCnt, Split, isAnalyze);
                localFileCount++;
            } else {
                /* choose one mpp dn to handle the split */
                if (!AssignRemoteNode(htab, numOfNodes, dnOids, Split, isAnalyze)) {
                    delete (conn);
                    conn = NULL;
                    ereport(ERROR,
                        (errcode(ERRCODE_NO_DATA_FOUND),
                            errmodule(MOD_HDFS),
                            errmsg("No datanode is assigned for this split: %s", CurrentFileName)));
                }
            }
            fileCount++;

            /* If the local ratio is less than 1/10 in the first 256 files, we take all the others as remote scan. */
            if (fileCount == (int)MAX_ROUNDROBIN_AVAILABLE_DN_NUM &&
                localFileCount < (int)(MAX_ROUNDROBIN_AVAILABLE_DN_NUM / 10)) {
                needPredicate = false;
            }
        }

        pfree_ext(pLocal);
        pfree_ext(pCached);
    }

    pfree_ext(dnInfo);
    pfree_ext(statDnInfo);
}

static void SpillToDisk(Index relId, List* allTask, dfs::DFSConnector* conn)
{
    ListCell* lc = NULL;
    foreach (lc, allTask) {
        SplitMap* dnTask = (SplitMap*)lfirst(lc);

        /* when the number of files is less than u_sess->attr.attr_sql.schedule_splits_threshold / 1000, don't spill to
         * disk. */
        if (list_length(dnTask->splits) <= (int)(u_sess->attr.attr_sql.schedule_splits_threshold / 1000))
            continue;

        /* serialize the splits */
        char* tmpBuffer = nodeToString(dnTask->splits);
        dnTask->lengths = lappend_int(dnTask->lengths, strlen(tmpBuffer));

        /* construct the tmp file name */
        StringInfoData tmpFileName;
        initStringInfo(&tmpFileName);
        char* prefix = getPrefixPath(conn);
        uint32 queryHashID = ((t_thrd.postgres_cxt.debug_query_string == NULL)
                                  ? 0
                                  : hash_any((unsigned char*)t_thrd.postgres_cxt.debug_query_string,
                                        strlen(t_thrd.postgres_cxt.debug_query_string)));
        appendStringInfo(&tmpFileName,
            "%s/.%lu_%u_%u_%ld_%d_%ld",
            prefix,
            gs_thread_self(),
            queryHashID,
            relId,
            (int64)allTask,
            dnTask->nodeId,
            gs_random());

        dnTask->downDiskFilePath = tmpFileName.data;

        /* flush the serial buffer to share file system. */
        flushToRemote(dnTask, tmpBuffer, conn);
        ereport(DEBUG1,
            (errmodule(MOD_HDFS),
                errmsg("Coordinate %s, spill %d splits to dfs %s.",
                    get_pgxc_nodename(dnTask->nodeId),
                    list_length(dnTask->splits),
                    dnTask->downDiskFilePath)));

        /* clean tmp memory */
        pfree_ext(tmpBuffer);
        if (prefix != NULL)
            pfree_ext(prefix);
        list_free_deep(dnTask->splits);
        dnTask->splits = NIL;
    }
}

static char* getPrefixPath(dfs::DFSConnector* conn)
{
    if (conn->getType() == HDFS_CONNECTOR) {
        /* hdfs */
        StringInfoData prefix;
        initStringInfo(&prefix);
        appendStringInfo(&prefix, "/tmp");
        return prefix.data;
    } else {
        /* obs */
        const char* bucket = conn->getValue("bucket", NULL);
        StringInfoData prefix;
        initStringInfo(&prefix);
        appendStringInfo(&prefix, "%s", bucket);
        return prefix.data;
    }
}

static void flushToRemote(SplitMap* dnTask, const char* buffer, dfs::DFSConnector* conn)
{
    if (conn->openFile(dnTask->downDiskFilePath, O_WRONLY) == -1 ||
        conn->writeCurrentFile(buffer, linitial_int(dnTask->lengths)) == -1 || conn->flushCurrentFile() == -1) {
        delete (conn);
        conn = NULL;
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
                errmodule(MOD_HDFS),
                errmsg("Failed to flush the splits into disk when the count is"
                       "too much, detail can be found in log of %s",
                    g_instance.attr.attr_common.PGXCNodeName)));
    }
}

static void loadDiskSplits(SplitMap* dnTask, dfs::DFSConnector* conn)
{
    MemoryContext oldCtx = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
    int length = linitial_int(dnTask->lengths);
    char* buffer = (char*)palloc0(length + 1);
    if (conn->openFile(dnTask->downDiskFilePath, O_RDONLY) == -1 ||
        conn->readCurrentFileFully(buffer, length, 0) == -1) {
        delete (conn);
        conn = NULL;
        (void)MemoryContextSwitchTo(oldCtx);
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
                errmodule(MOD_HDFS),
                errmsg("Failed to load the splits from disk when the count is"
                       "too much, detail can be found in log of %s",
                    g_instance.attr.attr_common.PGXCNodeName)));
    }
    ereport(DEBUG1,
        (errmodule(MOD_HDFS),
            errmsg("Datanode %s, load %d splits from dfs %s.",
                get_pgxc_nodename(dnTask->nodeId),
                list_length(dnTask->splits),
                dnTask->downDiskFilePath)));

    /* deserialize the splits */
    dnTask->splits = (List*)stringToNode(buffer);
    pfree_ext(buffer);
    (void)MemoryContextSwitchTo(oldCtx);

    /* clear the down disk objects */
    conn->closeCurrentFile();
    (void)conn->deleteFile(dnTask->downDiskFilePath, 0);
    pfree(dnTask->downDiskFilePath);
    dnTask->downDiskFilePath = NULL;
    list_free(dnTask->lengths);
    dnTask->lengths = NIL;
}

static int GetDnIpAddrByOid(Oid* DnOid, uint32 OidSize, uint64* OidIp, uint32 OidIpSize)
{
    uint32 Loop = 0;
    uint32 OidIpIdx = 0;
    HeapTuple tuple = NULL;

    Assert(OidIpSize >= OidSize);

    for (Loop = 0; (Loop < OidSize) && (OidIpIdx < OidIpSize); Loop++) {
        tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(DnOid[Loop]));

        if (!HeapTupleIsValid(tuple))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmodule(MOD_HDFS),
                    errmsg("cache lookup failed for node %u", DnOid[Loop])));

        Form_pgxc_node NodeForm = (Form_pgxc_node)GETSTRUCT(tuple);
        in_addr_t TmpVal = 0;

        /*handle local host and local ip*/
        if ((strncmp(NodeForm->node_host.data, LOCAL_IP, ::strlen(LOCAL_IP)) == 0) ||
            (strncmp(NodeForm->node_host.data, LOCAL_HOST, ::strlen(LOCAL_HOST)) == 0)) {

            TmpVal = inet_addr(tcp_link_addr);
        } else {
            TmpVal = inet_addr(NodeForm->node_host.data);
        }

        OidIp[OidIpIdx] = (((uint64)DnOid[Loop]) << 32) + ((uint64)TmpVal);
        OidIpIdx += 1;

        ReleaseSysCache(tuple);
    }

    return OidIpIdx;
}

static int StatDn(uint64* dnInfo, uint32 dnCnt, dnInfoStat* statDnInfo, uint32 statDnCnt)
{
    uint64 Tmp = (dnCnt > 0) ? dnInfo[0] : 0;
    uint32 Cnt = (dnCnt > 0) ? 1 : 0;
    uint32 Start = 0;
    uint32 Loop = (dnCnt > 0) ? 1 : 0;
    uint32 dnInfoStatIdx = 0;

    Assert((dnCnt <= statDnCnt) && (dnInfo != NULL) && (statDnInfo != NULL));

    for (; (Loop < dnCnt) && (dnInfoStatIdx < statDnCnt); Loop++) {
        if (GETIP(Tmp) == GETIP(dnInfo[Loop])) {
            Cnt += 1;
        } else {
            statDnInfo[dnInfoStatIdx].ipAddr = GETIP(Tmp);
            statDnInfo[dnInfoStatIdx].Start = Start;
            statDnInfo[dnInfoStatIdx].Cnt = Cnt;
            dnInfoStatIdx += 1;

            Tmp = dnInfo[Loop];
            Start = Loop;
            Cnt = 1;
        }
    }

    if (Cnt > 0) {
        statDnInfo[dnInfoStatIdx].ipAddr = GETIP(Tmp);
        statDnInfo[dnInfoStatIdx].Start = Start;
        statDnInfo[dnInfoStatIdx].Cnt = Cnt;
    }

    return int(dnInfoStatIdx + 1);
}

/*quick sort & bsearch*/
static int CompareByLowerInt32(const void* Elem1, const void* Elem2)
{
    int Ret = 0;
    const uint64* P1 = (const uint64*)Elem1;
    const uint64* P2 = (const uint64*)Elem2;
    uint32 Ip1 = GETIP(*P1);
    uint32 Ip2 = GETIP(*P2);

    Ret = (Ip1 > Ip2) ? 1 : ((Ip1 < Ip2) ? -1 : 0);

    return Ret;
}

static int CompareByIp(const void* Elem1, const void* Elem2)
{
    int Ret = 0;
    const uint64* P1 = (const uint64*)Elem1;
    uint32 Ip1 = GETIP(*P1);
    const dnInfoStat* P2 = (dnInfoStat*)Elem2;
    uint32 Ip2 = P2->ipAddr;

    Ret = (Ip1 > Ip2) ? 1 : ((Ip1 < Ip2) ? -1 : 0);

    return Ret;
}

List* CNSchedulingForAnalyze(unsigned int* totalFilesNum, unsigned int* numOfDns, Oid foreignTableId, bool isglbstats)
{
    errno_t rc = EOK;
    int filesToRead = 0;
    HTAB* htab = NULL;
    HASHCTL hash_ctl;
    dnWork* item = NULL;
    List* fileList = NIL;
    HASH_SEQ_STATUS scan_status;
    List* partitionRelatedList = NIL;
    List* columnList = NIL;
    Relation relation = RelationIdGetRelation(foreignTableId);
    TupleDesc tupleDescriptor = NULL;
    List* prunningResult = NIL;
    List* partList = NIL;
    RelationLocInfo* rel_loc_info = GetRelationLocInfo(foreignTableId);
    List* allTask = NIL;
    ServerTypeOption srvType = T_INVALID;

    if (!RelationIsValid(relation)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmodule(MOD_HDFS),
                errmsg("could not open relation with OID %u", foreignTableId)));
    }

    srvType = getServerType(foreignTableId);
    tupleDescriptor = RelationGetDescr(relation);
    columnList = CreateColList((Form_pg_attribute*)tupleDescriptor->attrs, tupleDescriptor->natts);

    RelationClose(relation);

    *numOfDns = get_pgxc_classnodes(foreignTableId, NULL);
    Assert(*numOfDns > 0);

    /* we should get all dn task for global stats. */
    if (isglbstats) {
        if (IS_OBS_CSV_TXT_FOREIGN_TABLE(foreignTableId)) {
            /* for dist obs foreign table.*/
#ifndef ENABLE_LITE_MODE
            allTask = CNSchedulingForDistOBSFt(foreignTableId);
#else
            FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
        } else {
            if (rel_loc_info == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmodule(MOD_HDFS),
                        errmsg("could not get locator information for relation with OID %u", foreignTableId)));
            }
            allTask = CNScheduling(foreignTableId,
                0,
                columnList,
                NULL,
                prunningResult,
                partList,
                rel_loc_info->locatorType,
                true,
                columnList,
                tupleDescriptor->natts,
                NULL);
        }
        pfree_ext(rel_loc_info);
        return allTask;
    }

    /* used for generate a random start position in AssignLocalNode */
    srand((unsigned)time(NULL));

    dfs::DFSConnector* conn = dfs::createConnector(CurrentMemoryContext, foreignTableId);

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(dnWork);
    hash_ctl.hash = oid_hash;
    hash_ctl.hcxt = CurrentMemoryContext;
    htab = hash_create("SchedulerHashTable", 128, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    /* Get the string of file names into allFiles */
    fileList = GetAllFiles(conn, foreignTableId, srvType);

    if (0 == list_length(fileList)) {
        delete (conn);
        conn = NULL;
        list_free_deep(columnList);
        return NIL;
    }

    /* Start to process partition info */
    partitionRelatedList = list_make2(fileList, columnList);

    /* Process the diretories of each layer of partition in order. */
    fileList =
        PartitionPruneProcess(conn, partitionRelatedList, NIL, foreignTableId, prunningResult, partList, srvType);

    /*
     * Sometimes the hive partition layers is more than ours partition defination, so here we need to dig
     * down to find all the files.
     */
    fileList = DrillDown(conn, fileList);

    /* Check if the file list is empty again after the partition prunning and drilling. */
    if (0 == list_length(fileList)) {
        delete (conn);
        conn = NULL;
        return NIL;
    }

    *totalFilesNum = list_length(fileList);

    /*
     * acquire the num of files needs to analyze
     * at least analyze one file
     */
    if (IsLocatorReplicated(GetLocatorType(foreignTableId))) {
        filesToRead = *totalFilesNum;
    } else {
        filesToRead = getAnalyzeFilesNum(*numOfDns, *totalFilesNum);
    }
    Assert(filesToRead > 0);

    /*
     * find the suitable dn which has the request files number.
     * we will choose local read files priorityly
     * if none of the dns has the enough local read files,fill the others with remote read files
     */
    item = AssignRequestFilesToDn(htab, fileList, filesToRead, conn);
    Assert(NULL != item);
    if (item == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmodule(MOD_HDFS),
                errmsg("could not assign request files to dn")));
    }
    delete (conn);
    conn = NULL;

    SplitMap* taskMap = makeNode(SplitMap);
    taskMap->nodeId = PGXCNodeGetNodeId(item->nodeOid, PGXC_NODE_DATANODE);
    taskMap->locatorType = LOCATOR_TYPE_NONE;
    taskMap->splits = list_copy(item->toDoList);
    allTask = lappend(allTask, taskMap);

    hash_seq_init(&scan_status, htab);
    while ((item = (dnWork*)hash_seq_search(&scan_status)) != NULL) {
        list_free(item->toDoList);
        item->toDoList = NIL;
    }

    hash_destroy(htab);
    list_free(fileList);
    fileList = NIL;

    return allTask;
}

void AssignSplits(List* splitToDnMap, dfs::reader::ReaderState* readerState, dfs::DFSConnector* conn)
{
    ListCell* splitToDnMapCell = NULL;

    List* fileList = NIL;

    foreach (splitToDnMapCell, splitToDnMap) {
        SplitMap* dnTask = (SplitMap*)lfirst(splitToDnMapCell);

        if (u_sess->pgxc_cxt.PGXCNodeId == dnTask->nodeId || LOCATOR_TYPE_REPLICATED == dnTask->locatorType) {
            /* If the splits is spilled to disk ,then load it here. */
            if ((dnTask->splits == NULL && dnTask->downDiskFilePath != NULL) && conn != NULL) {
                loadDiskSplits(dnTask, conn);
            }

            if (NIL != dnTask->splits) {
                fileList = (List*)copyObject(dnTask->splits);
                break;
            }
        }
    }

    readerState->splitList = fileList;
}

/*
 * Find a dn which has the enough local-read files to fulfill the request files number
 * if there's not enough files, take some remote-read files
 */
static dnWork* AssignRequestFilesToDn(HTAB* htab, List* fileList, int filesNum, dfs::DFSConnector* conn)
{
    HeapTuple tuple = NULL;
    int maxFilesAssigned = 0;
    Form_pgxc_node dataNodeForm;
    dnWork* recordDn = NULL;
    bool found = false;
    ListCell* fileCell = NULL;
    dnWork* item = NULL;
    int dnIdx = 0;
    Oid* dnOids = NULL;
    int totalDnNum;
    int numOfDns;
    int replIdx;
    int startNode;

    PgxcNodeGetOids(NULL, &dnOids, NULL, &numOfDns, false);
    Assert(numOfDns > 0 && NULL != dnOids);

    totalDnNum = numOfDns;
    startNode = (int)gs_random() % totalDnNum;
    dnIdx = startNode;

    /* traversal all the dn to assign files,and return the first dn that satifies. */
    while (numOfDns--) {
        bool assigned = false;

        Oid nodeOid = dnOids[dnIdx];
        tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeOid));

        Form_pgxc_node nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);

        /* initialize the number of files that is assigned to zero */
        int filesAssgnied = 0;

        /* traversal all the files */
        foreach (fileCell, fileList) {
            SplitInfo* split = (SplitInfo*)lfirst(fileCell);
            char* currentFileName = split->filePath;
            int numOfReplica;

            dfs::DFSBlockInfo* bl = conn->getBlockLocations(currentFileName);
            numOfReplica = bl->getNumOfReplica();

            /* traversal the replication ,if any of the replication satify local read to current datanode, add it to
             * splitmap */
            for (replIdx = 0; replIdx < numOfReplica; replIdx++) {
                if (isNodeLocalToFile(nodeForm, bl->getNames(0, replIdx))) {
                    if (!assigned) {
                        assigned = true;

                        item = (dnWork*)hash_search(htab, &nodeOid, HASH_ENTER, &found);
                        item->toDoList = NIL;

                        Assert(!found);
                    }

                    filesAssgnied++;
                    item->toDoList = lappend(item->toDoList, split);

                    if (filesAssgnied == filesNum) {
                        ReleaseSysCache(tuple);
                        delete (bl);
                        bl = NULL;
                        return item;
                    }

                    break;
                }
            }
            delete (bl);
            bl = NULL;
        }

        /* record the dn which has the most local read files */
        if (filesAssgnied > maxFilesAssigned) {
            maxFilesAssigned = filesAssgnied;
            recordDn = item;
        }

        ReleaseSysCache(tuple);

        dnIdx++;
        if (dnIdx == totalDnNum)
            dnIdx = 0;
    }

    /* after traversal all files,we don't get enough local files,
     * so choose the dn which has the most local files ,and add some remote files to it
     */
    if (NULL == recordDn) {
        recordDn = (dnWork*)hash_search(htab, &dnOids[startNode], HASH_ENTER, &found);
        recordDn->toDoList = NIL;
    }
    tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(recordDn->nodeOid));
    dataNodeForm = (Form_pgxc_node)GETSTRUCT(tuple);

    foreach (fileCell, fileList) {
        SplitInfo* split = (SplitInfo*)lfirst(fileCell);
        char* currentFileName = split->filePath;
        int numOfReplica;

        dfs::DFSBlockInfo* bl = conn->getBlockLocations(currentFileName);
        numOfReplica = bl->getNumOfReplica();

        for (replIdx = 0; replIdx < numOfReplica; replIdx++) {
            if (isNodeLocalToFile(dataNodeForm, bl->getNames(0, replIdx))) {
                /* it has already in the toDo list */
                break;
            }
        }
        delete (bl);
        bl = NULL;

        if (replIdx >= numOfReplica) {
            maxFilesAssigned++;
            recordDn->toDoList = lappend(recordDn->toDoList, split);

            if (maxFilesAssigned == filesNum) {
                ReleaseSysCache(tuple);
                return recordDn;
            }
        }
    }

    ReleaseSysCache(tuple);

    if (dnOids != NULL)
        pfree_ext(dnOids);
    return NULL;
}

static int getAnalyzeFilesNum(int dataNodeNum, int totalFilesNum)
{
    double accurateAssignedFiles;
    double rawAssignedFiles;
    int filesToRead;

    if (dataNodeNum == 0)
        dataNodeNum = 1;

    accurateAssignedFiles = (double)totalFilesNum / dataNodeNum;
    rawAssignedFiles = (double)totalFilesNum / dataNodeNum;

    if (totalFilesNum <= dataNodeNum)
        filesToRead = 1;
    else if (accurateAssignedFiles - rawAssignedFiles > 0.5)
        filesToRead = (int)rawAssignedFiles + 1;
    else
        filesToRead = (int)rawAssignedFiles;

    Assert(filesToRead >= 1 && filesToRead <= totalFilesNum / dataNodeNum + 1 &&
           filesToRead >= totalFilesNum / dataNodeNum);

    return filesToRead;
}

static bool isNodeLocalToFile(Form_pgxc_node nodeForm, const char* blLocation)
{

    /*
     *   if node ip is not local address, compare directly
     */
    if (strncmp(nodeForm->node_host.data, blLocation, strlen(nodeForm->node_host.data)) == 0) {
        return true;
    }

    /*
     *   if node ip is local address, compare sctp_link_addr
     */
    if (((strncmp(nodeForm->node_host.data, LOCAL_IP, strlen(LOCAL_IP)) == 0) ||
            (strncmp(nodeForm->node_host.data, LOCAL_HOST, strlen(LOCAL_HOST)) == 0)) &&
        (strncmp(tcp_link_addr, blLocation, strlen(blLocation)) == 0)) {
        return true;
    }

    return false;
}

static void AssignReplicaNode(HTAB* htab, const Oid* dnOids, const uint32_t nodeNum, const List* fileList)
{
    bool found = false;
    for (uint32_t dnIdx = 0; dnIdx < nodeNum; dnIdx++) {
        Oid nodeOid = dnOids[dnIdx];
        dnWork* item = (dnWork*)hash_search(htab, &nodeOid, HASH_ENTER, &found);
        if (0 == dnIdx) {
            item->toDoList = (List*)copyObject(fileList);
        } else {
            item->toDoList = NIL;
        }
    }
}

/*
 *  select a lowest workload dn in (cached&local) or  (uncached&local)  dns
 */
static bool AssignLocalNode(
    HTAB* htab, uint64* Locations, const char* pChached, uint32 LocationSize, SplitInfo* currentFile, bool isAnalyze)
{
    bool bFound = false;
    Oid NodeOid = 0;
    uint32 nodeTaskLength = 0;
    uint64 TempU64 = 0;
    uint32 MinWl = MAX_UINT32;
    uint32 NextIdx = 0;
    uint32 UseCachedFactor = 1;
    uint32 Sel;

    Assert((Locations != NULL) && (LocationSize > 0));

    dnWork* pMinWorkload[MAX_ROUNDROBIN_AVAILABLE_DN_NUM] = {NULL};

    for (uint32 Loop = 0; Loop < LocationSize; Loop++) {
        TempU64 = Locations[Loop];
        UseCachedFactor = ((pChached[Loop] != 0) && (best_effort_use_cahce == 1)) ? 2 : 1;
        NodeOid = GETOID(TempU64);

        /* use the oid of each node in the pgxc_node is efficient and is coveient to get information
         *  with oid
         */
        dnWork* Item = (dnWork*)hash_search(htab, &NodeOid, HASH_ENTER, &bFound);

        if (bFound) {
            nodeTaskLength = (list_length(Item->toDoList) + (UseCachedFactor - 1)) / UseCachedFactor;

        } else /* indicate the mpp datanode did not has any work */
        {
            nodeTaskLength = 0;
            Item->toDoList = NIL;
        }

        if (nodeTaskLength < MinWl) {
            pMinWorkload[0] = Item;
            MinWl = nodeTaskLength;
            NextIdx = 1;
        } else if (nodeTaskLength == MinWl) {
            pMinWorkload[NextIdx] = Item;
            NextIdx += 1;
        } else {
            /*nothing to do*/
        }
    }

    Assert((NextIdx > 0) && (NextIdx < MAX_ROUNDROBIN_AVAILABLE_DN_NUM));
    if (NextIdx == 0)
        NextIdx = 1;

    if (!isAnalyze) {
        /* select one randomly from all low workload dns */
        Sel = gs_random() % NextIdx;
    } else /* get a determinded dn to get single stats for global stats.  */
        Sel = (NextIdx - 1) % MAX_ROUNDROBIN_AVAILABLE_DN_NUM;

    pMinWorkload[Sel]->toDoList = lappend(pMinWorkload[Sel]->toDoList, currentFile);

    return true;
}

static bool AssignRemoteNode(HTAB* htab, int nodeNum, Oid* dnOids, SplitInfo* currentFile, bool isAnalyze)
{
    HeapTuple tuple = NULL;
    int nodeTaskLength = 0;
    bool found = false;
    dnWork* recordDn = NULL;
    Oid nodeOid;
    int dnIdx;
    int totalDnNum = nodeNum;

    if (!isAnalyze)
        dnIdx = (int)gs_random() % totalDnNum;
    else /* the dn id often changed for anlyze of global stats, result in the totalRowCnts is error, so we should set
            the dn id is 0. */
        dnIdx = 0;

    while (nodeNum-- > 0) {
        nodeOid = dnOids[dnIdx];
        tuple = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(nodeOid));

        if (!HeapTupleIsValid(tuple))
            ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmodule(MOD_HDFS),
                    errmsg("cache lookup failed for node %u", nodeOid)));

        Form_pgxc_node nodeForm = (Form_pgxc_node)GETSTRUCT(tuple);

        /* Take definition for given node type */
        if (nodeForm->node_type != PGXC_NODE_COORDINATOR) {
            /*
             * use the oid of each node in the pgxc_node is efficient and is coveient to get information
             * with oid
             */
            dnWork* item = (dnWork*)hash_search(htab, &nodeOid, HASH_ENTER, &found);

            // and it's not in the map yet
            if (!found || 0 == list_length(item->toDoList)) {
                item->toDoList = NIL;
                item->toDoList = lappend(item->toDoList, currentFile);

                ReleaseSysCache(tuple);

                return true;
            } else {
                Assert(list_length(item->toDoList) > 0);
                if (0 == nodeTaskLength || nodeTaskLength > list_length(item->toDoList)) {
                    nodeTaskLength = list_length(item->toDoList);
                    recordDn = item;
                }
            }
        }
        ReleaseSysCache(tuple);

        dnIdx++;

        if (dnIdx == totalDnNum)
            dnIdx = 0;
    }

    if (0 != nodeTaskLength) {
        recordDn->toDoList = lappend(recordDn->toDoList, currentFile);
        return true;
    }

    return false;
}

static char* parseMultiFileNames(char** fileNames, bool checkRootDir, char delimiter)
{
    char* currentFileName = NULL;
    char* semicolon = strchr(*fileNames, delimiter);

    if (semicolon == NULL) {
        /* NOT FOUND */
        char* tmp = *fileNames;
        char* fileNameBegin = NULL;
        char* fileNameEnd = NULL;

        /* detele ' ' before path */
        while (' ' == *tmp)
            tmp++;
        fileNameBegin = tmp;

        /* detele ' ' after path */
        tmp++;
        while (' ' != *tmp && '\0' != *tmp)
            tmp++;
        fileNameEnd = tmp;

        int indexOfSemicolon = (int)(fileNameEnd - fileNameBegin);
        currentFileName = (char*)palloc0(indexOfSemicolon + 1);
        errno_t rc = memcpy_s(currentFileName, (indexOfSemicolon + 1), fileNameBegin, indexOfSemicolon);
        securec_check(rc, "", "");
        currentFileName[indexOfSemicolon] = '\0';
        *fileNames = NULL; /* reset to NULL as an end scan indicator */
    } else {
        /* delete ' ' */
        char* tmp = *fileNames;
        char* fileNameBegin = 0;
        char* fileNameEnd = 0;

        /* detele ' ' before path */
        while (' ' == *tmp)
            tmp++;
        fileNameBegin = tmp;

        /* detele ' ' after path */
        tmp++;
        while (' ' != *tmp && tmp < semicolon)
            tmp++;
        fileNameEnd = tmp;

        int indexOfSemicolon = (int)(fileNameEnd - fileNameBegin);
        int indexOfFile = (int)(semicolon - *fileNames);
        currentFileName = (char*)palloc0(indexOfSemicolon + 1);
        errno_t rc = memcpy_s(currentFileName, (indexOfSemicolon + 1), fileNameBegin, indexOfSemicolon);
        securec_check(rc, "", "");
        currentFileName[indexOfSemicolon] = '\0';
        *fileNames += (long)indexOfFile + 1;
    }

    if (checkRootDir && currentFileName[0] != '/') {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_INVALID_STRING_FORMAT),
                errmodule(MOD_HDFS),
                errmsg("file path need to start with root '/', but it is:  %s", currentFileName)));
    }

    return currentFileName;
}

/**
 * @Description: traverse the scan expression tree. It is a base function.
 * when we search specified expression(for example,var, partColumnRestiction), need use it.
 * @in node, the given expression.
 * @in walker, the implementation function.
 * @out context, the stateless struct, the caller build this structer.
 * @return none.
 */
void scan_expression_tree_walker(Node* node, void (*walker)(), void* context)
{
    bool (*p2walker)(void*, void*) = (bool (*)(void*, void*))walker;
    /* Guard against stack overflow due to overly complex expressions. */
    check_stack_depth();

    if (NULL == node) {
        return;
    }
    switch (nodeTag(node)) {
        case T_BoolExpr: {
            BoolExpr* expr = (BoolExpr*)node;
            scan_expression_tree_walker((Node*)expr->args, walker, context);
            break;
        }
        case T_OpExpr:
        case T_NullTest: {
            p2walker(node, context);
            break;
        }
        case T_List: {
            ListCell* temp = NULL;
            foreach (temp, (List*)node) {
                scan_expression_tree_walker((Node*)lfirst(temp), walker, context);
            }
            break;
        }

        default: {
            break;
        }
    }
}

/**
 * @Description: get the restrictions about the given partition column.
 * @in node, we get the restriction from this node, which is a restriction
 * expression list.
 * @in/out partition_context, the element partColList of context store
 * all partition column number. The element partClauseList will store
 * restriction clause.
 * @return
 */
static void GetPartitionClauseOpExpr(Node* node, partition_context* context)
{
    OpExpr* op_clause = (OpExpr*)node;
    Node* leftop = NULL;
    Node* rightop = NULL;
    Var* var = NULL;
    if (list_length(op_clause->args) != 2) {
        return;
    }
    leftop = get_leftop((Expr*)op_clause);
    rightop = get_rightop((Expr*)op_clause);
    
    Assert(NULL != rightop);
    Assert(NULL != leftop);
    
    if (rightop == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
                errmodule(MOD_HDFS),
                errmsg("The right operate expression of partition column cannot be NULL.")));
        return;
    }

    if (leftop == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
                errmodule(MOD_HDFS),
                errmsg("The left operate expression of partition column cannot be NULL.")));
        return;
    }
    
    if (IsVarNode(rightop) && IsA(leftop, Const)) {
        if (IsA(rightop, RelabelType)) {
            rightop = (Node*)((RelabelType*)rightop)->arg;
        }
        var = (Var*)rightop;
    } else if (IsVarNode(leftop) && IsA(rightop, Const)) {
        if (IsA(leftop, RelabelType)) {
            leftop = (Node*)((RelabelType*)leftop)->arg;
        }
        var = (Var*)leftop;
    }
    
    if (NULL != var) {
        ListCell* cell = NULL;
        foreach (cell, context->partColList) {
            if (lfirst_int(cell) == var->varattno) {
                /* we find one partition restriction calues and put it into partColList. */
                context->partClauseList = lappend(context->partClauseList, node);
                break;
            }
        }
    }
}

static void GetPartitionClauseNullTest(Node* node, partition_context* context)
{
    NullTest* nullExpr = (NullTest*)node;
    if (IS_NOT_NULL == nullExpr->nulltesttype) {
        return;
    }
    if (!IsA(nullExpr->arg, Var)) {
        return;
    }
    Var* var = (Var*)nullExpr->arg;
    if (NULL != var) {
        ListCell* cell = NULL;
        foreach (cell, context->partColList) {
            if (lfirst_int(cell) == var->varattno) {
                context->partClauseList = lappend(context->partClauseList, node);
                break;
            }
        }
    }
}

void getPartitionClause(Node* node, partition_context* context)
{
    if (NULL == node) {
        return;
    }
    switch (nodeTag(node)) {
        case T_BoolExpr: {
            ereport(
                ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE), errmodule(MOD_HDFS), errmsg("can not reach here.")));

            break;
        }
        case T_OpExpr: {
            GetPartitionClauseOpExpr(node, context);
            break;
        }
        case T_NullTest: {
            /* Only optimize the "column Is NULL" NullTest expression. */
            GetPartitionClauseNullTest(node, context);
            break;
        }
        default: {
            scan_expression_tree_walker(node, (void (*)())getPartitionClause, context);
            break;
        }
    }
}

/**
 * @Description: build the partition dirctory for partition restriction.
 * for example, /b=123/c=456/.
 * @in node, the restriction.
 * @in partition_string_context, it include the partition column.
 * the partition restirction to be obtained will store partColStrList
 * of context.
 * @return none.
 */
static void getPartitionStringOpExpr(Node* node, partition_string_context* context)
{
    OpExpr* op_clause = (OpExpr*)node;
    bool equalExpr = isEquivalentExpression(op_clause->opno);
    Var* var = NULL;
    Const* constant = NULL;
    Node* leftop = get_leftop((Expr*)op_clause);
    Node* rightop = get_rightop((Expr*)op_clause);
    if (equalExpr) {
        if (rightop && IsVarNode(rightop) && leftop && IsA(leftop, Const)) {
            if (IsA(rightop, RelabelType)) {
                rightop = (Node*)((RelabelType*)rightop)->arg;
            }
            var = (Var*)rightop;
            constant = (Const*)leftop;
        } else if (leftop && IsVarNode(leftop) && rightop && IsA(rightop, Const)) {
            if (IsA(leftop, RelabelType)) {
                leftop = (Node*)((RelabelType*)leftop)->arg;
            }
            var = (Var*)leftop;
            constant = (Const*)rightop;
        }
        if (NULL == var) {
            return;
        }
    
        char* relName = get_relid_attribute_name(context->foreignTblOid, var->varattno);
        StringInfo partitionDir = makeStringInfo();
        appendStringInfo(partitionDir, "%s=", relName);
        GetStringFromDatum(constant->consttype, constant->consttypmod, constant->constvalue, partitionDir);
        appendStringInfo(partitionDir, "/");
        context->partColNoList = lappend_int(context->partColNoList, var->varattno);
        context->partColStrList = lappend(context->partColStrList, partitionDir);
    }
}

static void getPartitionStringNullTest(Node* node, partition_string_context* context)
{
    NullTest* nullExpr = (NullTest*)node;
    if (IS_NOT_NULL == nullExpr->nulltesttype) {
        return;
    }
    if (!IsA(nullExpr->arg, Var)) {
        return;
    }
    
    Var* var = (Var*)nullExpr->arg;
    if (NULL != var) {
        char* relName = get_relid_attribute_name(context->foreignTblOid, var->varattno);
        StringInfo partitionDir = makeStringInfo();
        appendStringInfo(partitionDir, "%s=%s", relName, DEFAULT_HIVE_NULL);
        appendStringInfo(partitionDir, "/");
        context->partColNoList = lappend_int(context->partColNoList, var->varattno);
        context->partColStrList = lappend(context->partColStrList, partitionDir);
    }
}

void getPartitionString(Node* node, partition_string_context* context)
{
    if (NULL == node) {
        return;
    }
    switch (nodeTag(node)) {
        case T_OpExpr: {
            getPartitionStringOpExpr(node, context);
            break;
        }
        case T_NullTest: {
            getPartitionStringNullTest(node, context);
            break;
        }
        default: {
            scan_expression_tree_walker(node, (void (*)())getPartitionString, context);
            break;
        }
    }
}

/**
 * @Description: whether or check the give expression include bool expression.
 * @in node, the given expression.
 * @return return true, if find the bool expression, otherwise return false.
 */
bool hasBoolExpr(Node* node)
{
    check_stack_depth();
    bool returnValue = false;

    if (node == NULL)
        return false;

    switch (nodeTag(node)) {
        case T_BoolExpr: {
            returnValue = true;
            break;
        }
        case T_OpExpr: {
            ListCell* temp = NULL;
            OpExpr* expr = (OpExpr*)node;
            foreach (temp, expr->args) {
                if (hasBoolExpr((Node*)lfirst(temp))) {
                    returnValue = true;
                    break;
                }
            }
            break;
        }
        case T_List: {
            ListCell* temp = NULL;
            foreach (temp, (List*)node) {
                if (hasBoolExpr((Node*)lfirst(temp))) {
                    returnValue = true;
                    break;
                }
            }
            break;
        }

        default: {
            break;
        }
    }

    return returnValue;
}

/**
 * @Description: add patition dirctory path from the given restriction.
 * @in prefix, the foldername option value.
 * @in foreignTableId, the given foreign table oid.
 * @in scanClauseList, the given restriction.
 * @return return the modified prefix.
 */
List* addPartitionPath(Oid foreignTableId, List* scanClauseList, char* prefix)
{
    List* partList = GetPartitionList(foreignTableId);
    bool hasBExpr = false;
    List* partStrPathList = NIL;
    partition_string_context context;
    StringInfo str = NULL;

    hasBExpr = hasBoolExpr((Node*)scanClauseList);

    /*now, only optimize the opExpr. */
    if (hasBExpr) {
        return NIL;
    }

    context.partColNoList = NIL;
    context.partColStrList = NIL;
    context.foreignTblOid = foreignTableId;

    partition_context part_context;
    part_context.partClauseList = NIL;
    part_context.partColList = partList;

    getPartitionClause((Node*)scanClauseList, &part_context);

    /* get the partition restriction. */
    getPartitionString((Node*)part_context.partClauseList, &context);

    /* Bulid the partition path. */
    for (int i = 0; i < list_length(partList); i++) {
        AttrNumber varattno = list_nth_int(partList, i);
        bool findPartCol = false;
        if (hasBExpr && i >= 1) {
            break;
        }
        for (int partIndex = 0; partIndex < list_length(context.partColNoList); partIndex++) {
            int partColNo = list_nth_int(context.partColNoList, partIndex);

            if (partColNo == varattno) {
                findPartCol = true;
                StringInfo partStringDir = (StringInfo)list_nth(context.partColStrList, partIndex);

                /* we need palloc memory. */

                if (hasBExpr) {
                    str = makeStringInfo();
                    appendStringInfo(str, "%s", prefix);
                    appendStringInfo(str, "%s", partStringDir->data);
                    partStrPathList = lappend(partStrPathList, str);
                    ereport(LOG, (errmodule(MOD_DFS), errmsg("active pruning remain file: %s", str->data)));

                    /*As for boolExpr, more then one restriction is existed for one column. */
                    continue;
                } else {
                    if (0 == i) {
                        str = makeStringInfo();
                        appendStringInfo(str, "%s", prefix);
                    }
                    appendStringInfo(str, "%s", partStringDir->data);
                    ereport(LOG, (errmodule(MOD_DFS), errmsg("active pruning remain file: %s", str->data)));
                    /* only one restriction for one column. */
                    break;
                }
            }
        }

        if (!findPartCol) {
            break;
        }
    }

    if (!hasBExpr && NULL != str) {
        partStrPathList = lappend(partStrPathList, str);
    }
    return partStrPathList;
}

static List* GetAllFiles(
    dfs::DFSConnector* conn, Oid foreignTableId, ServerTypeOption srvType, List* columnList, List* scanClauseList)
{
    List* fileList = NIL;
    char* currentFile = NULL;
    List* entryList = NIL;

    switch (srvType) {
        case T_OBS_SERVER: {
            char* multiBucketsFolder = HdfsGetOptionValue(foreignTableId, OPTION_NAME_FOLDERNAME);
            char* multiBucketsRegion = HdfsGetOptionValue(foreignTableId, OPTION_NAME_LOCATION);

            while (NULL != multiBucketsFolder || NULL != multiBucketsRegion) {
                if (NULL != multiBucketsFolder) {
                    currentFile = parseMultiFileNames(&multiBucketsFolder, false, ',');
                } else {
                    /* As for region option, each region path will stat from "obs://".  So we add strlen(obs:/) length
                     * for  multiBuckets.  */
                    multiBucketsRegion = multiBucketsRegion + strlen("obs:/");
                    currentFile = parseMultiFileNames(&multiBucketsRegion, false, '|');
                }

                List* fixedPathList = NIL;
                if (u_sess->attr.attr_sql.enable_valuepartition_pruning) {
                    fixedPathList = addPartitionPath(foreignTableId, scanClauseList, currentFile);
                }

                if (0 == list_length(fixedPathList)) {
                    fileList = list_concat(fileList, conn->listObjectsStat(currentFile, currentFile));
                } else {
                    ListCell* cell = NULL;
                    foreach (cell, fixedPathList) {
                        StringInfo fixedPath = (StringInfo)lfirst(cell);
                        fileList = list_concat(fileList, conn->listObjectsStat(fixedPath->data, currentFile));
                    }
                }
                list_free_ext(fixedPathList);
            }

            break;
        }
        case T_HDFS_SERVER: {
            HdfsFdwOptions* options = HdfsGetOptions(foreignTableId);
            if (options->foldername) {
                /*
                 * If the foldername is a file path, the funtion hdfsListDirectory do not validity check,
                 * so calling IsHdfsFile to judge whether the foldername is not a file path.
                 */
                if (conn->isDfsFile(options->foldername)) {
                    delete (conn);
                    conn = NULL;
                    ereport(ERROR,
                        (errcode(ERRCODE_FDW_INVALID_OPTION_DATA),
                            errmodule(MOD_HDFS),
                            errmsg("The foldername option cannot be a file path.")));
                }

                fileList = conn->listObjectsStat(options->foldername);
            } else {
                while (NULL != options->filename) {
                    currentFile = parseMultiFileNames(&options->filename, true, ',');

                    /* If the option use filenames, then all the entries defined must be file. */
                    if (!conn->isDfsFile(currentFile)) {
                        delete (conn);
                        conn = NULL;
                        ereport(ERROR,
                            (errcode(ERRCODE_FDW_INVALID_OPTION_DATA),
                                errmodule(MOD_HDFS),
                                errmsg("The entries in the options fileNames must be file!")));
                    }

                    fileList = list_concat(fileList, conn->listObjectsStat(currentFile));
                }
            }
            break;
        }
        default: {
            Assert(0);
            break;
        }
    }

    list_free(entryList);
    entryList = NIL;
    return fileList;
}

static List* GetObsAllFiles(dfs::DFSConnector* conn, Oid foreignTableId, List* columnList, List*& prunningResult,
    List*& partList, List* scanClauses)
{
    List* fileList = NIL;
    char* currentFile = NULL;

    char* multiBucketsFolder = HdfsGetOptionValue(foreignTableId, OPTION_NAME_FOLDERNAME);
    char* multiBucketsRegion = HdfsGetOptionValue(foreignTableId, OPTION_NAME_LOCATION);

    while (NULL != multiBucketsFolder || NULL != multiBucketsRegion) {
        if (NULL != multiBucketsFolder) {
            currentFile = parseMultiFileNames(&multiBucketsFolder, false, ',');
        } else {
            /* As for region option, each region path will stat from "obs://".	So we add strlen(obs:/) length for
             * multiBuckets.  */
            multiBucketsRegion = multiBucketsRegion + strlen("obs:/");
            currentFile = parseMultiFileNames(&multiBucketsRegion, false, '|');
        }

        List* fixedPathList = NIL;
        if (u_sess->attr.attr_sql.enable_valuepartition_pruning) {
            fixedPathList = addPartitionPath(foreignTableId, scanClauses, currentFile);
        }

        if (0 == list_length(fixedPathList)) {
            fileList = list_concat(fileList, conn->listObjectsStat(currentFile, currentFile));
        } else {
            ListCell* cell = NULL;
            foreach (cell, fixedPathList) {
                StringInfo fixedPath = (StringInfo)lfirst(cell);
                fileList = list_concat(fileList, conn->listObjectsStat(fixedPath->data, currentFile));
            }
        }
        list_free_ext(fixedPathList);
    }

    if (0 == list_length(fileList)) {
        return NIL;
    }
    return fileList;
}

static List* GetHdfsAllFiles(dfs::DFSConnector* conn, Oid foreignTableId, List* columnList, List*& prunningResult,
    List*& partList, List* scanClauses)
{
    List* fileList = NIL;
    char* currentFile = NULL;

    HdfsFdwOptions* options = HdfsGetOptions(foreignTableId);
    if (options->foldername) {
        /*
         * If the foldername is a file path, the funtion hdfsListDirectory do not validity check,
         * so calling IsHdfsFile to judge whether the foldername is not a file path.
         */
        if (conn->isDfsFile(options->foldername)) {
            delete (conn);
            conn = NULL;
            ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_DATA),
                    errmodule(MOD_HDFS),
                    errmsg("The foldername option cannot be a file path.")));
        }

        fileList = conn->listObjectsStat(options->foldername);
    } else {
        while (NULL != options->filename) {
            currentFile = parseMultiFileNames(&options->filename, true, ',');

            /* If the option use filenames, then all the entries defined must be file. */
            if (!conn->isDfsFile(currentFile)) {
                delete (conn);
                conn = NULL;
                ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_DATA),
                        errmodule(MOD_HDFS),
                        errmsg("The entries in the options fileNames must be file!")));
            }

            fileList = list_concat(fileList, conn->listObjectsStat(currentFile));
        }
    }

    if (0 == list_length(fileList)) {
        return NIL;
    }
    return fileList;
}

static Value* getPartitionValue(dfs::DFSConnector* conn, char* partitionStr, char* ObjectStr)
{
    const char* partContent = strchr(partitionStr, '=');

    if (NULL == partContent) {
        delete (conn);
        conn = NULL;
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
                errmodule(MOD_HDFS),
                errmsg("Something wrong with the partition directory name of file %s.", ObjectStr)));
    }

    Value* partValue = makeString(UriDecode(partContent + 1));
    return partValue;
}

/*
 * Get all the files in the current directory and store the partition column value in the new split if need.
 * This is only used for partition table and is different from DigFiles although both two functions search sub
 * files for a given path.
 *
 * @_in param conn: the handler of hdfs connect.
 * @_in param split: The split from whose path we get sub files.
 * @_in param colNo: the partition column index of the current split.
 * @return Return a list of sub files, or null for a empty directory.
 */
static List* GetSubFiles(dfs::DFSConnector* conn, SplitInfo* split, int colNo)
{
    List* fileList = NIL;
    char* fileName = split->fileName;
    char* folderName = split->filePath;
    List* partContentList = split->partContentList;
    List* entryList = NIL;
    SplitInfo* newsplit = NULL;

    entryList = conn->listObjectsStat(folderName);

    if (entryList == NIL) {
        return NIL;
    }

    List* newPartContentList = list_copy(partContentList);
    Value* partValue = getPartitionValue(conn, fileName, folderName);
    newPartContentList = lappend(newPartContentList, partValue);

    for (int i = 0; i < list_length(entryList); i++) {
        SplitInfo* splitInfo = (SplitInfo*)list_nth(entryList, i);
        newsplit = InitFolderSplit(splitInfo->filePath, newPartContentList, splitInfo->ObjectSize);
        fileList = lappend(fileList, newsplit);
    }

    pfree_ext(split->fileName);
    pfree_ext(split->filePath);
    list_free(newPartContentList);
    list_free(entryList);
    entryList = NIL;

    return fileList;
}

/*
 * Dig the file split input. This is a general function to get list of file/directories for a given file path.
 * (GetSubFiles is only for partition prunning.) If the path is a file, add itself to the list and return;
 * else if the file is a directory and no file is found under it, return NIL; else if the directory has sub
 * files/directories then add all the sub ones into the list and return.
 *
 * @_in param conn: The hdfs connect handle.
 * @_in param split: The split file/directory to dig in.
 * @return Return a list of one or more files. NIL means it is a empty directory.
 */
static List* DigFiles(dfs::DFSConnector* conn, SplitInfo* split)
{
    List* fileList = NIL;
    char* filePath = split->filePath;
    SplitInfo* newsplit = NULL;
    List* partContent = split->partContentList;
    List* entryList = conn->listObjectsStat(filePath);

    if (entryList == NIL) {
        return NIL;
    }

    for (int i = 0; i < list_length(entryList); i++) {
        SplitInfo* splitInfo = (SplitInfo*)list_nth(entryList, i);
        newsplit = InitFolderSplit(splitInfo->filePath, partContent, splitInfo->ObjectSize);
        fileList = lappend(fileList, newsplit);
    }

    list_free(entryList);
    entryList = NIL;
    return fileList;
}

List* GetPartitionList(Oid relid)
{
    HeapTuple partTuple = NULL;
    int2vector* partVec = NULL;
    List* partList = NULL;
    Datum datum;
    bool isnull = false;

    /* Check if the current foreign table is partitioned. */
    if (!isPartitionedObject(relid, RELKIND_FOREIGN_TABLE, true))
        return NIL;

    /* Search the tuple related to the current foreign table in pg_partition. */
    partTuple = searchPgPartitionByParentIdCopy(PART_OBJ_TYPE_PARTED_TABLE, relid);
    if (!HeapTupleIsValid(partTuple))
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmodule(MOD_HDFS),
                errmsg("cache lookup failed for relid %u", relid)));

    datum = SysCacheGetAttr(PARTRELID, partTuple, Anum_pg_partition_partkey, &isnull);
    if (isnull) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmodule(MOD_HDFS),
                errmsg("Error happens when search the record in pg_partition for a partition table. ")));
    } else {
        partVec = (int2vector*)DatumGetPointer(datum);
    }

    /* Build the partition list from the partVec stored in tuple. */
    for (int i = 0; i < partVec->dim1; i++) {
        partList = lappend_int(partList, partVec->values[i]);
    }

    heap_freetuple(partTuple);
    partTuple = NULL;
    return partList;
}

/**
 * @Description: fill the partition value into partContentList in order to
 * read it by setPartinfoAndDesc function.
 * @in conn, the DFS connextor.
 * @in splitObject, store the partition value, we get partition value from it.
 * @return none.
 */
void fillPartitionValueInSplitInfo(dfs::DFSConnector* conn, SplitInfo* splitObject, int partColNum)
{
    int ibegin = find_Nth(splitObject->filePath, partColNum, "/");
    int iend = find_Nth(splitObject->filePath, partColNum + 1, "/");
    char* partitionStr = (char*)palloc0(iend - ibegin);
    error_t rc = EOK;
    rc = memcpy_s(partitionStr, iend - ibegin, splitObject->filePath + ibegin + 1, iend - ibegin - 1);
    securec_check(rc, "\0", "\0");

    splitObject->fileName = partitionStr;
    Value* partitionValue = getPartitionValue(conn, partitionStr, splitObject->filePath);
    splitObject->partContentList = lappend(splitObject->partContentList, partitionValue);
}

/*
 * The function handle the whole process of partition prunning based on the partition column list,
 * scanClauses and so on.
 *
 * @_in param conn: The handler of hdfs connect.
 * @_in param partitionRelatedList: Includes partition list, file list and column list.
 * @_in param scanClauses: The expression clauses of foreign scan for the prunning;
 * @_in param foreignTableId: the relation oid of the current foreign table.
 * @_out param prunningResult: Statistic of the partition prunning information for each layer.
 * @_out param partList: The list of the partition column.
 * @return Return the file list after the partition prunning.
 */
static List* PartitionPruneProcess(dfs::DFSConnector* conn, List* partitionRelatedList, List* scanClauses,
    Oid foreignTableId, List*& prunningResult, List*& partList, ServerTypeOption srvType)
{
    partList = GetPartitionList(foreignTableId);

    ListCell* fileCell = NULL;
    SplitInfo* split = NULL;
    bool partitionSkipped = false;
    List* fileList = (List*)linitial(partitionRelatedList);
    List* columnList = (List*)lsecond(partitionRelatedList);

    CheckPartitionColNumber(conn, partList, fileList, foreignTableId, srvType);

    for (int i = 0; i < list_length(partList); i++) {
        List* newFileList = NIL;
        int partCol = list_nth_int(partList, i);
        int sum = list_length(fileList);

        /*
         * If the fileList is empty, then all the files has been pruned and return immediately.
         * In this case , the list of part info in the split is not complete, but it is ok because
         * if we return NIL here, there will no task to scheduler and it will return in the begining
         * of scan(don't read any orc file).
         */
        if (0 == sum)
            return NIL;

        int notprunning = 0;

        /* Fetch the current partition column var includes type and no. */
        Var* value = GetVarFromColumnList(columnList, partCol);

        /* The flag to control if we need to call PartitionFilterClause, true means not. */
        bool skipPartitionFilter = ((NULL == value) || (0 == list_length(scanClauses)));

        /*
         * If the partition column is not required(value ==null) or the i exceed the max number of
         * partition layers we can prune or the scanClauses is null, we will just fetch the partition
         * column value without prunning.
         */
        if (skipPartitionFilter) {
            /* process all the directories in the fileList(they must be directory other than file) */
            foreach (fileCell, fileList) {
                split = (SplitInfo*)lfirst(fileCell);
                if (T_HDFS_SERVER == srvType) {
                    newFileList = list_concat(newFileList, GetSubFiles(conn, split, partCol));
                } else {
                    fillPartitionValueInSplitInfo(conn, split, i + split->prefixSlashNum);
                    newFileList = lappend(newFileList, split);
                }
            }
        } else {
            Expr* equalExpr = (Expr*)MakeOperatorExpression(value, BTEqualStrategyNumber);

            /* process all the directories in the fileList(they must be directory other than file) */
            ListCell* prev = NULL;
            ListCell* next = NULL;
            for (fileCell = list_head(fileList); fileCell != NULL; fileCell = next) {
                CHECK_FOR_INTERRUPTS();
                split = (SplitInfo*)lfirst(fileCell);

                /* next cell */
                next = lnext(fileCell);

                if (T_OBS_SERVER == srvType) {
                    fillPartitionValueInSplitInfo(conn, split, i + split->prefixSlashNum);
                }

                /* Partition pruning by scanClauses. */
                partitionSkipped = false;

                partitionSkipped = PartitionFilterClause(split, scanClauses, value, equalExpr);

                /*
                 * If we can not skip the current one, we need to Add the files under the directory to
                 * new file List which will be the fileList when we arrive to the next partition layer.
                 */
                if (!partitionSkipped) {
                    if (T_HDFS_SERVER == srvType) {
                        newFileList = list_concat(newFileList, GetSubFiles(conn, split, partCol));
                    } else {
                        newFileList = lappend(newFileList, split);
                    }
                    notprunning++;

                    /* prev cell */
                    prev = fileCell;
                } else {
                    if (T_OBS_SERVER == srvType) {
                        pfree_ext(split->fileName);
                        pfree_ext(split->filePath);
                        pfree_ext(split);

                        /* remove from fileList */
                        fileList = list_delete_cell(fileList, fileCell, prev);
                    } else {
                        /* prev cell */
                        prev = fileCell;
                    }
                }
            }

            /* collect the partition prunning statistic for the current partition layer. */
            CollectPartPruneInfo(prunningResult, sum, notprunning, partCol, foreignTableId);
        }

        /* Clean the list if needed. */
        if (T_HDFS_SERVER == srvType) {
            list_free_deep(fileList);
        } else {
            list_free(fileList);
        }
        fileList = newFileList;
    }

    return fileList;
}

/*
 * Check if the number of partition column is larger than in hdfs. The number of
 * partition column defined in MPPDB can be smaller, but can never be larger.
 *
 * .e.g
 * If we create a foreign table like: create foreign table hdfs_tab ~ partitioned by
 * (c1, c2), then we will check if all the file paths include c1 and c2. Because we must
 * ensure that the tree consisted of paths is absolutely balanceable which
 * means all the paths have the same length. But there can be no files in the last
 * partition directory, so we need to process this condition specially.
 *
 * @_in param conn: The handler of hdfs connect.
 * @_in param partList: The list of partition columns.
 * @_in param fileList: The file list defined when we create the foreign table.
 * @_in param foreignTableId: The oid of the foreign table in catalog.
 */
static void CheckPartitionColNumber(
    dfs::DFSConnector* conn, List* partList, List* fileList, Oid foreignTableId, ServerTypeOption srvType)
{
    int length = list_length(partList);
    if (0 == length) {
        return;
    }
    if (T_HDFS_SERVER == srvType) {
        SplitInfo* split = (SplitInfo*)linitial(fileList);
        SplitInfo* curSplit = (SplitInfo*)copyObject(split);

        for (int i = 0; i < length; i++) {
            curSplit = CheckOneSubSplit(conn, curSplit, (i == length - 1), foreignTableId);
        }
        DestroySplit(curSplit);
    } else {
        ListCell* cell = NULL;
        foreach (cell, fileList) {
            SplitInfo* curSplit = (SplitInfo*)lfirst(cell);
            if (find_Nth(curSplit->filePath, length + curSplit->prefixSlashNum, "/") == -1) {
                QUERY_NOT_SUPPORT(foreignTableId,
                    "The number of partition columns "
                    "defined of foreign table %s is larger than it should be.");
            }
        }
    }
}

/*
 * We only check one split for each layer here when checking if the number of partition
 * column is larger than defined in hdfs.
 * @_in param conn: The handler of hdfs connect.
 * @_in param split: The split to be checked.
 * @_in param isLastPartition: Indicate if the split is of the last partition.
 * @_in param foreignTableId: The oid of the foreign table in catalog.
 * @return Return the first split under the current folder.
 */
static SplitInfo* CheckOneSubSplit(dfs::DFSConnector* conn, SplitInfo* split, bool isLastPartition, Oid foreignTableId)
{
    char* folderName = split->filePath;
    SplitInfo* newsplit = NULL;
    List* entryList = NIL;

    if (conn->isDfsFile(folderName)) {
        delete (conn);
        conn = NULL;
        QUERY_NOT_SUPPORT(foreignTableId,
            "The number of partition columns "
            "defined of foreign table %s is larger than it should be.");
    }

    entryList = conn->listDirectory(folderName);

    /*
     * Here if the numEntries equals zero, we may need log a error. If we define N
     * partition columns, then there must be N layers of folder.
     */
    if (0 == list_length(entryList)) {
        if (!isLastPartition) {
            delete (conn);
            conn = NULL;
            ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                    errmodule(MOD_HDFS),
                    errmsg("Error occur when open partition folder: %s, "
                           "it is empty.",
                        folderName)));
        }
    } else {
        newsplit = InitFolderSplit((char*)list_nth(entryList, 0), NIL, 0);
    }

    /* Clean the former split. */
    DestroySplit(split);
    list_free(entryList);
    entryList = NIL;

    return newsplit;
}

/*
 * Check if we can skip the current split by the scanClauses.
 * @_in param split: The split to check if it matches the scanclauses.
 * @_in param scanClauses: The clauses generated from the optimizer.
 * @_in param value: The var of the partition column.
 * @_in_param equalExpr: The expression of the restriction to be built.
 * @return Return true: we can skip the current split; false: we need to keep the split.
 */
static bool PartitionFilterClause(SplitInfo* split, List* scanClauses, Var* value, Expr* equalExpr)
{
    Node* baseRestriction = NULL;
    char* partValue = NULL;
    List* partRestriction = NIL;
    bool partSkipped = false;
    char* fileName = split->fileName;
    Assert(fileName != NULL);
    Datum datumValue;

    /* fetch the value of the current partition column. */
    partValue = strchr(fileName, '=');
    if (NULL == partValue) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_INVALID_OPTION_DATA),
                errmodule(MOD_HDFS),
                errmsg("Something wrong with the partition directory name of file %s.", split->filePath)));
    }

    if (0 == strncmp(partValue + 1, DEFAULT_HIVE_NULL, 26)) {
        /* __HIVE_DEFAULT_PARTITION__ means the current value is NULL */
        baseRestriction = BuildNullTestConstraint(value, IS_NULL);
    } else {
        /* Convert the string value to datum value. */
        datumValue = GetDatumFromString(value->vartype, value->vartypmod, UriDecode(partValue + 1));
        BuildConstraintConst(equalExpr, datumValue, false);
        baseRestriction = (Node*)equalExpr;
    }

    partRestriction = lappend(partRestriction, baseRestriction);

    /*
     * Compare the size of strings by using "C" format in coarse filter.
     */
    List* tempScanClauses = (List*)copyObject(scanClauses);
    List* opExprList = pull_opExpr((Node*)tempScanClauses);
    ListCell* lc = NULL;
    foreach (lc, opExprList) {
        OpExpr* opExpr = (OpExpr*)lfirst(lc);
        opExpr->inputcollid = C_COLLATION_OID;
    }

    /*
     * Call the function afforded by PG to try if we can refute the predicate, if partSkipped
     * is true then we can skip the current split(file), otherwise we need it.
     */
    partSkipped = predicate_refuted_by(partRestriction, tempScanClauses, true);
    list_free_ext(opExprList);
    list_free_ext(partRestriction);
    list_free_deep(tempScanClauses);
    tempScanClauses = NIL;

    return partSkipped;
}

/*
 * Collect the partition prunning result which includes the total number of files and the left number
 * of files for each layer.
 *
 * @_out param prunningResult: The result list of partition prunning.
 * @_in param sum: The total number of files for prunning.
 * @_in param notprunning: The left number of files after prunning.
 * @_in param colno: The no of partition column.
 * @_in param relOid: The oid of the partitioned foreign table.
 */
static void CollectPartPruneInfo(List*& prunningResult, int sum, int notprunning, int colno, Oid relOid)
{
    Relation rel = heap_open(relOid, AccessShareLock);
    char* attName = NameStr(rel->rd_att->attrs[colno - 1]->attname);

    /*
     * Add 16 here because we need to add some description words, three separate characters
     * (';', '(', ')') and termination '\0'.
     */
    int length = strlen(attName) + GetDigitOfInt(sum) + GetDigitOfInt(notprunning) + 16;
    char* tmp = (char*)palloc0(sizeof(char) * length);
    int ret = 0;
    ret = snprintf_s(
        tmp, sizeof(char) * length, sizeof(char) * length - 1, "%s(total %d; left %d)", attName, sum, notprunning);
    securec_check_ss(ret, "\0", "\0");
    heap_close(rel, AccessShareLock);

    Value* v = makeString(tmp);
    prunningResult = lappend(prunningResult, v);
}

/*
 * Drill down until there is only file in the file list(no directories).
 *
 * @_in param conn: The handler of hdfs connect.
 * @_in param fileList: The list of files before the drill.
 * @return Return the list of files after drilling.
 */
static List* DrillDown(dfs::DFSConnector* conn, List* fileList)
{
    ListCell* fileCell = NULL;
    SplitInfo* split = NULL;
    List* nextFileList = NIL;
    List* fileListList = NIL;

    while (0 != list_length(fileList)) {
        bool isAllFile = true;

        /* Dig in each file in the file list. */
        foreach (fileCell, fileList) {
            CHECK_FOR_INTERRUPTS();
            split = (SplitInfo*)lfirst(fileCell);
            if (split->ObjectSize > 0) {
                fileListList = lappend(fileListList, split);
                continue;
            } else {
                isAllFile = false;
                nextFileList = list_concat(nextFileList, DigFiles(conn, split));
            }
        }

        /*
         * If all the split is file, we have digged into the bottom
         * and it is time to break the loop and return.
         */
        if (isAllFile) {
            break;
        } else {
            list_free(fileList);
            fileList = nextFileList;
            nextFileList = NIL;
        }
    }

    return fileListList;
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * @Description: filter *.carbondata from fileList
 * @IN conn: dfs connection
 * @IN fileList: all file list to be filter
 * @IN allColumnList: column list for query
 * @IN restrictColumnList: restrict column list
 * @IN scanClauses: scan clause
 * @IN attrNum: attr num
 * @Return: *.carbondata file list for query
 */
List* CarbonDataFile(dfs::DFSConnector* conn, List* fileList, List* allColumnList, List* restrictColumnList,
    List* scanClauses, int16 attrNum)
{
    Assert(NIL != fileList);

    if (0 == list_length(fileList)) {
        return NIL;
    }

    /* if no where condition, do not read *.carbonindex file */
    /* if (enable_indexscan == false), do not read *.carbonindex file */
    if ((0 == list_length(restrictColumnList)) || (u_sess->attr.attr_sql.enable_indexscan == false)) {
        List* dataFileList = dfs::CarbonFileFilter(fileList, CARBONDATA_DATA);
        list_free(fileList);

        ereport(DEBUG1, (errmodule(MOD_CARBONDATA), errmsg("Ignore *.carbonindex file.")));

        return dataFileList;
    }

    /* get .carbonindex file from obs file list */
    List* indexFileList = dfs::CarbonFileFilter(fileList, CARBONDATA_INDEX);

    /* init readerState for inputstream */
    dfs::reader::ReaderState* readerState = (dfs::reader::ReaderState*)palloc0(sizeof(dfs::reader::ReaderState));

    readerState->persistCtx = AllocSetContextCreate(CurrentMemoryContext,
        "carbon index reader context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /* switch MemoryContext */
    MemoryContext oldcontext = MemoryContextSwitchTo(readerState->persistCtx);

    /* init restrictRequired and readRequired */
    bool* restrictRequired = (bool*)palloc0(sizeof(bool) * attrNum);
    bool* readRequired = (bool*)palloc0(sizeof(bool) * attrNum);

    ListCell* lc = NULL;
    Var* variable = NULL;

    foreach (lc, restrictColumnList) {
        variable = (Var*)lfirst(lc);
        Assert(variable->varattno <= attrNum);
        restrictRequired[variable->varattno - 1] = true;
    }

    foreach (lc, allColumnList) {
        variable = (Var*)lfirst(lc);
        Assert(variable->varattno <= attrNum);
        readRequired[variable->varattno - 1] = true;
    }

    /* init queryRestrictionList */
    List* queryRestrictionList = ExtractNonParamRestriction((List*)copyObject(scanClauses));

    /* get .carbondata file from .carbonindex file */
    List* dataFileList = NIL;
    ListCell* cell = NULL;

    for (cell = list_head(indexFileList); cell != NULL; cell = lnext(cell)) {
        void* data = lfirst(cell);
        if (IsA(data, SplitInfo)) {
            SplitInfo* splitinfo = (SplitInfo*)data;
            char* filePath = splitinfo->filePath;

            readerState->currentSplit = splitinfo;
            readerState->currentFileSize = splitinfo->ObjectSize;
            readerState->currentFileID = -1;

            std::unique_ptr<dfs::GSInputStream> gsInputStream =
                dfs::InputStreamFactory(conn, filePath, readerState, false);

            dfs::reader::CarbondataIndexReader indexFileReader(
                allColumnList, queryRestrictionList, restrictRequired, readRequired, attrNum);

            indexFileReader.init(std::move(gsInputStream));

            indexFileReader.readIndex();

            /* Data File Deduplication from indexfile  */
            dataFileList = list_concat(dataFileList, indexFileReader.getDataFileDeduplication());
        }
    }

    fileList = dfs::CarbonDataFileMatch(fileList, dataFileList);

    /* release memory */
    (void)MemoryContextSwitchTo(oldcontext);
    if (NULL != readerState->persistCtx && readerState->persistCtx != CurrentMemoryContext) {
        MemoryContextDelete(readerState->persistCtx);
        pfree(readerState);
        readerState = NULL;
    }

    return fileList;
}

/*
 * @Description: extract non-param restriction
 * @IN opExpressionList: operate expression list
 * @Return: non-param restriction list
 */
static List* ExtractNonParamRestriction(List* opExpressionList)
{
    ListCell* lc = NULL;
    Expr* expr = NULL;
    List* retRestriction = NIL;

    foreach (lc, opExpressionList) {
        expr = (Expr*)lfirst(lc);
        if (IsA(expr, OpExpr)) {
            Node* leftop = get_leftop(expr);
            Node* rightop = get_rightop(expr);
            Assert(leftop != NULL);
            Assert(rightop != NULL);
            if ((IsVarNode(leftop) && IsParamConst(rightop)) || (IsVarNode(rightop) && IsParamConst(leftop))) {
                continue;
            }
        }
        retRestriction = lappend(retRestriction, expr);
    }

    List* opExprList = pull_opExpr((Node*)retRestriction);
    foreach (lc, opExprList) {
        OpExpr* opExpr = (OpExpr*)lfirst(lc);
        opExpr->inputcollid = C_COLLATION_OID;
    }
    list_free_ext(opExprList);
    return retRestriction;
}
#endif
