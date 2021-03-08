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
 * dfs_am.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/access/dfs/dfs_am.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DFS_AM_H
#define DFS_AM_H

#include "storage/dfs/dfs_connector.h"
#include "dfsdesc.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "utils/bloom_filter.h"
#include "vecexecutor/vectorbatch.h"
#include "pgxc/locator.h"

#define INVALID_ENCODING (-1) /* For hdfs table, there is no encoding */

extern void CleanupDfsHandlers(bool isTop);
extern void ResetDfsHandlerPtrs(void);
extern void RemoveDfsReadHandler(const void *handler);
extern void RemoveDfsWriteHandler(const void *handler);

struct IndexInsertInfo;

extern char *HdfsGetOptionValue(Oid foreignTableId, const char *optionName);
extern DefElem *HdfsGetOptionDefElem(Oid foreignTableId, const char *optionName);

struct DfsScanState;
namespace dfs {
typedef uint64 uint64_t;
typedef int64 int64_t;

/* The type of file to be read, there is only ORC currently. */
enum FileType {
    ORC = 0,
    PARQUET = 1,
    TEXT = 2,
    CSV = 3,
    CARBONDATA = 4
};

namespace reader {

typedef struct FileOffset {
    uint32 rowIndexInFile[BatchMaxSize];
    uint32 numelements;
} FileOffset;

/*
 * The interface which provides some basic functions.
 */
class Reader : public BaseObject {
public:
    virtual ~Reader()
    {
    }

    /*
     * The reserved interface to add finish work when there is no rows
     * to read.
     */
    virtual void Destroy() = 0;

    /*
     * Fetch the next one tuple.
     * @_in_out_param tuple: The tuple to be filled by the reader.
     */
    virtual void nextTuple(TupleTableSlot *tuple) = 0;

    /*
     * Fetch the next 1000 or less rows in a vector batch.
     * @_in_out_param batch: The vector batch to be filled by the reader.
     */
    virtual void nextBatch(VectorBatch *batch) = 0;

    /*
     * @Description: Fetch the next batch according to the tid vector batch.
     * @IN/OUT batch: vector batch
     * @IN tids: tid vector batch
     * @See also:
     */
    virtual void nextBatchByTids(VectorBatch *batch, VectorBatch *tidBatch) = 0;

    /*
     * Fetch the next batch according to the tid vector batch which includes all the columns we need.
     * @OUT batch: the target vector batch
     * @IN tidBatch: the tid vector batch
     * @IN prepareCols: the prepared cols list in the tid vector batch
     */
    virtual void nextBatchByTidsForIndexOnly(VectorBatch *batch, VectorBatch *tidBatch, List *prepareCols) = 0;

    /*
     * Add a bloom filter into the reader.
     * @_in_param bloomFilter: The bloom filter to be added.
     * @_in_param colIdx: The index of the column on which to add the bloom filter.
     * @_in_param is_runtime: Is between runtime or not.
     */
    virtual void addBloomFilter(filter::BloomFilter *bloomFilter, int colIdx, bool is_runtime) = 0;

    /*
     * Copy bloomFilter to runTimeBloomFilter
     */
    virtual void copyBloomFilter() = 0;

    /*
     * The reserved interface to add prepared processes before calling
     * @_in_param conn: The connector handler to hdfs.
     * @_in_param type: The type of the file to read.
     * the nextBatch.
     */
    virtual void begin(dfs::DFSConnector *conn, FileType type) = 0;

    /*
     * Handle the tailing works which are not destroyers.
     */
    virtual void end() = 0;

    /* Get DFS table name */
    virtual const char *getRelName() = 0;

    /* Get the current reader's transaction ID. */
    virtual const int getCurrentTransactionLevel() = 0;

    /* Just before the first execution iterator, we assign splits for current worker. */
    virtual void dynamicAssignSplits() = 0;
};

/*
 * Includes all the params which are used during reading.
 */
typedef struct ReaderState {
    /* DFS storage type  */
    int dfsType;

    /* The number of the columns defined in the relation. */
    uint32_t relAttrNum;

    /* The number of parition columns. */
    uint32_t partNum;

    /*
     * The mapping array from mpp column index to file column index, this
     * has been adjusted for partition and default columns.
     */
    uint32_t *readColIDs;

    /*
     * When create ReaderImpl object, initialize the globalColIds, It represnets
     * a possible column order in file. The partition column index has any meaning.
     * The globalColIds must used in conjunction with globalReadRequired.
     * It starts from 0.
     */
    uint32_t *globalColIds;

    /*
     * The mapping array from the column id defined in relation to column id
     * stored in file, this start at 1 and will be null for non-partition table.
     */
    uint32_t *colNoMapArr;

    /*
     * The ordered column id according to the selectivity. Use the
     * column id defined in relation to visit it.
     */
    uint32_t *orderedCols;

    /* Count up the number of files wich are static pruned */
    uint64_t staticPruneFiles;

    /* Count up the number of files which are pruned dynamically. */
    uint64_t dynamicPrunFiles;

    /* Count up the number of rows which are filtered by the bloom filter. */
    uint64_t bloomFilterRows;

    /* Count up the number of strides which are filtered by the bloom filter. */
    uint64_t bloomFilterBlocks;

    /* Count up the number of rows which are filtered by the min/max. */
    uint64_t minmaxFilterRows;

    /* min/max filtering data */
    uint64_t minmaxCheckFiles;   /* the number of checking files totally */
    uint64_t minmaxFilterFiles;  /* the number of filtered files */
    uint64_t minmaxCheckStripe;  /* the number of checking stripes totally */
    uint64_t minmaxFilterStripe; /* the number of filtered stripes, orc is stripe and parquet is row group */
    uint64_t minmaxCheckStride;  /* the number of checking strides totally */
    uint64_t minmaxFilterStride; /* the number of filtered strides, orc is stride and parquet is chunk page */

    /* Count up the number of cache hint of orc data */
    uint64 orcDataCacheBlockCount;
    uint64 orcDataCacheBlockSize;
    /* Count up the number of  load of orc data */
    uint64 orcDataLoadBlockCount;
    uint64 orcDataLoadBlockSize;
    /* Count up the number of  cache hint of orc meta data */
    uint64 orcMetaCacheBlockCount;
    uint64 orcMetaCacheBlockSize;
    /* Count up the number of oad of orc meta data */
    uint64 orcMetaLoadBlockCount;
    uint64 orcMetaLoadBlockSize;

    /*
     * The array stores the the columns which is required by the query
     * and includes the satisfied partition columns.
     */
    bool *isRequired;

    /*
     * The array to indicate which columns need to be send back
     * and includes the satisfied partition columns.
     */
    bool *targetRequired;

    /*
     * The array to indicate which columns have restrictions. Normally we ignore the
     * partition columns unless the restriction comes from the run time predicate.
     */
    bool *restrictRequired;

    /*
     * The array to indicate which columns need to be read from the file
     * and excludes the partition columns with the non-exist columns.
     */
    bool *readRequired;
    /*
     * When create ReaderImpl object, initialize the globalReadRequired. it represents
     * which column may be read from file. not include partition column.
     */
    bool *globalReadRequired;

    /* The list of files to be read. */
    List *splitList;

    /* The list of the required columns including the partition columns. */
    List *allColumnList;

    /* The list of partition columns. */
    List *partList;

    /* The list of restrictions for block skip. */
    List *queryRestrictionList;
    List *runtimeRestrictionList;

    /*
     * The list array stores the predicates upon each column. Use the
     * column id stored in file to visit it.
     */
    List **hdfsScanPredicateArr;

    /*
     * The array of string stores value of partition column for each file.
     * Use the column id defined in relation to visit it.
     */
    char **partitionColValueArr;

    /* The array of bloom filters on each column. */
    filter::BloomFilter **bloomFilters;

    /* The current file to be read. */
    SplitInfo *currentSplit;

    /*
     * The pointer to the file reader which need to be cleared when error occurs,
     * So i add it here to avoid resource leak.
     */
    Reader *fileReader;

    /* hdfs table <= 0, forigen table < 0(-1) */
    int currentFileID;

    /* The size of the current file, this is 0 for foreign table. */
    int64_t currentFileSize;

    /* The memory context to store the persist state when reading the file. */
    MemoryContext persistCtx;

    /* The memory contex which will be reset when rescaning. */
    MemoryContext rescanCtx;

    /* Scan state. */
    ScanState *scanstate;

    /* The snapshot used while reading. */
    Snapshot snapshot;

    /* Count up the number of hdfs local/remote block read by dn. */
    uint64_t localBlock;
    uint64_t remoteBlock;

    /* Count the number of intersections with dfs namenode/datanode. */
    uint64_t nnCalls;
    uint64_t dnCalls;

    /* Encoding type for the dfs foreign table. */
    int fdwEncoding;

    /* The level of encoding check,2 = high, 1 = low, 0 = no check */
    int checkEncodingLevel;

    /* Count how many elements are invalid for the encoding and are converted to ? automatically. */
    int incompatibleCount;
    int dealWithCount;

    /* special case for obs scan time */
    instr_time obsScanTime;
} ReaderState;

/*
 * @Description: The factory function to create a reader for ORC file.
 * @IN readerState: the state information for reading
 * @IN conn: the connector to dfs
 * @IN skipSysCol: skip reading system columns if true
 * @Return: the reader pointer
 * @See also:
 */
Reader *createOrcReader(ReaderState *readerState, dfs::DFSConnector *conn, bool skipSysCol);

/*
 * @Description: The factory function to create a reader for Parquet file.
 * @IN readerState: the state information for reading
 * @IN conn: the connector to dfs
 * @IN skipSysCol: skip reading system columns if true
 * @Return: the reader pointer
 * @See also:
 */
Reader *createParquetReader(ReaderState *readerState, dfs::DFSConnector *conn, bool skipSysCol);

/*
 * @Description: The factory function to create a reader for Carbondata file.
 * @IN readerState: the state information for reading
 * @IN conn: the connector to dfs
 * @IN skipSysCol: skip reading system columns if true
 * @Return: the reader pointer
 * @See also:
 */
Reader* createCarbondataReader(ReaderState *readerState, dfs::DFSConnector *conn, bool skipSysCol);

/*
 * @Description: The factory function to create a reader for TEXT file.
 * @IN readerState: the state information for reading
 * @IN conn: the connector to dfs
 * @IN skipSysCol: skip reading system columns if true
 * @Return: the reader pointer
 * @See also:
 */
Reader *createTextReader(ReaderState *readerState, dfs::DFSConnector *conn, bool skipSysCol);

/*
 * @Description: The factory function to create a reader for CSV file.
 * @IN readerState: the state information for reading
 * @IN conn: the connector to dfs
 * @IN skipSysCol: skip reading system columns if true
 * @Return: the reader pointer
 * @See also:
 */
Reader *createCsvReader(ReaderState *readerState, dfs::DFSConnector *conn, bool skipSysCol);

/*
 * @Description: Initialize the neccessary variables for analyze or copy to on dfs table(not foreign table).
 * @IN rel: The relation to scan.
 * @IN splitList: The list of file to scan.
 * @IN colNum: The number of the columns for scanning, set it 0 if colIdx is null.
 * @IN colIdx: The target column list((with system column at tail)1,2,..,-1) for scanning, scan all the columns if this
 * is null.
 * @IN snapshot: The snapshot(NULL, mvcc or snapshotNow).
 * @Return: the DfsScanState which is build inside the function.
 * @See also:
 */
DfsScanState *DFSBeginScan(Relation rel, List *splitList, int colNum, int16 *colIdx, Snapshot snapshot = NULL);

/*
 * Fetch the next vector batch for analyze or copy to on dfs table(not foreign table).
 * _in_param scanState: The state for scaning which is returned by DFSBeginScan.
 * @return the vector batch.
 */
VectorBatch *DFSGetNextBatch(DfsScanState *scanState);

/*
 * Fetch the next tuple of dfs table for analyze operator. The table to be scanned is not
 * foreign HDFS table.
 * _in_param scanState: The state for scaning which is returned by DFSBeginScan.
 * _out_param scanTupleSlot: The TupleTableSlot struct in which the scanned tuple is filled.
 * @return None.
 */
void DFSGetNextTuple(DfsScanState *scanState, TupleTableSlot *scanTupleSlot);

/*
 * Destroy the neccessary variables when the scan is over.
 * @_in_param scanState: The state for scaning which is returned by DFSBeginScan.
 */
void DFSEndScan(DfsScanState *scanState);

}  // namespace reader

namespace writer {

class Writer : public BaseObject {
public:
    virtual ~Writer()
    {
    }

    /* Initialize the class member, this must be called just after construct a new object. */
    virtual void init(IndexInsertInfo *indexInsertInfo) = 0;

    /* Add a batch into the buffer. */
    virtual void appendBatch(const VectorBatch *batch) = 0;

    /* Add a tuple into the buffer. */
    virtual void appendTuple(Datum *values, bool *nulls) = 0;

    /* Check if the buffer is available to hold special number of rows. */
    virtual bool canAppend(uint64 size) = 0;

    /* Spill the buffer into the current file.  */
    virtual int spillToCurFile() = 0;

    /* Spill the buffer into a new file. */
    virtual int spillToNewFile(const char *filePath) = 0;

    /* Spill the buffer into delta table. */
    virtual void deltaInsert(int option) = 0;

    /* Set the file ID of the current writer. */
    virtual void setFileID(uint64 fileID) = 0;

    /* Get the file ID of the current writer. */
    virtual uint64 getFileID() = 0;

    /* Get the rows in the hdfs file, this function must be called after the m_orcWriter is closed. */
    virtual uint64 getTotalRows() = 0;

    /* Get the totoal number of serialized rows of the current writer. */
    virtual uint64 getSerializedRows() = 0;

    /* Get the totoal number of buffer rows of the current writer. */
    virtual uint64 getBufferRows() = 0;

    /*
     * @Description: Acquire the min max value of the column. Return false when there is no min/max.
     * @IN colId: the column id
     * @OUT minStr: the string of min value if exist
     * @OUT maxStr: the string of max value if exist
     * @OUT hasMin: whether the min value exist
     * @OUT hasMax: whether the max value exist
     * @Return: false if there is no min or max value of the column, else return true.
     * @See also:
     */
    virtual bool getMinMax(uint32 colId, char *&minstr, char *&maxstr, bool &hasMin, bool &hasMax) = 0;

    /* Close the current writer and flush the data and return the file size. */
    virtual int64 closeCurWriter(char *filePath) = 0;

    /* Clean the class member. */
    virtual void Destroy() = 0;

    /* Get DFS table name */
    virtual const char *getRelName() = 0;

    /* Deal the clean works on dfs writer. */
    virtual void handleTail() = 0;

    /* Get the current writer's transaction ID. */
    virtual const int getCurrentTransactionLevel() = 0;
};

/*
 * @Description: Create an instance of orc writer
 * @IN context: memory context
 * @IN rel: the current relation
 * @IN DestRel: the dest relation(used for vacuum)
 * @IN IndexInsertInfo: includes index information
 * @IN parsig: dirctory information which is used for value partition table
 * @Return: writer pointer
 * @See also:
 */
Writer *createOrcWriter(MemoryContext context, Relation rel, Relation DestRel, IndexInsertInfo *indexInsertInfo,
                        const char *parsig);

}  // namespace writer
}  // namespace dfs

List *BuildSplitList(Relation rel, StringInfo RootDir, Snapshot snapshot = NULL);

#endif
