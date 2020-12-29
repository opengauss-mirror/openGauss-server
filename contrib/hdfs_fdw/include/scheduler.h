#ifndef SCHEDULER_H_
#define SCHEDULER_H_
#include "hdfs_fdw.h"
#include "foreign/foreign.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"

#include <stdio.h>
#include <stdlib.h>

#define PGXC_NODE_ID 9015
#define MAX_FOREIGN_PARTITIONKEY_NUM 4

#define MASK_HIGH_UINT32(x) (x & (uint64)0xFFFFFFFF)
#define GETIP(x) ((uint32)(x & (uint64)0xFFFFFFFF))
#define GETOID(x) ((uint32)(x >> 32))

#define LOCAL_IP "127.0.0.1"
#define LOCAL_HOST "localhost"
#define MAX_HOST_NAME_LENGTH 255
#define EXTRA_IP_NUM 32
#define MAX_ROUNDROBIN_AVAILABLE_DN_NUM 256
#define MAX_UINT32 (0xFFFFFFFF)

typedef struct dnWork {
    Oid nodeOid;
    List* toDoList;
} dnWork;

typedef struct dnInfoStat {
    uint32 ipAddr;
    uint32 Start;
    uint32 Cnt;
} dnInfoStat;

/*
 * This is executed on CN to schedule the files to each dn for load balance and use cache.
 * add param isAnalyze because the func CNScheduling has called by foreign scan which get random dn for schedule,
 * the random dn also changed, we should get a determinded dn to get single stats for global stats in order to get
 * stable stats.
 */
List* CNScheduling(Oid foreignTableId, Index relId, List* columnList, List* scanClauses, List*& prunningResult,
    List*& partList, char locatorType, bool isAnalyze, List* allColumnList, int16 attrNum, int64* fileNum);

/* CNSchedulingForAnalyze
 * firstly calculate the files to be analyzed,and because we just do analyze in one datanode,
 * the analyze files is about (total files) /(datanode number)
 * we will find the suitable datanode to do the anlayze and return
 * add param isglbstats in order to discriminate the processing of single stats and global stats.
 * we only get files in one dn for single stats, and we must get files in all dns for global stats.
 */
List* CNSchedulingForAnalyze(unsigned int* totalFilesNum, unsigned int* numOfDns, Oid foreignTableId, bool isglbstats);

/*
 * Assign the list of splits scheduled by CN to the current data node.
 * @_in_param splitToDnMap: The list of all the splits which map to each data node.
 * @_in_out_param readerState: The state stores the splits of the current data node.
 * @_in_param conn: The connector of dfs(hdfs or obs)
 */
void AssignSplits(List* splitToDnMap, dfs::reader::ReaderState* readerState, dfs::DFSConnector* conn);

/*
 * Search the catalog pg_partition to build a partition list to return.
 *
 * @_in param relid: The oid of the foreign table by which we search the partition list from the
 *      catalog.
 * @return Return the partition list we get.
 */
List* GetPartitionList(Oid relid);

#endif /* SCHEDULER_H_ */
