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
 * dfs_insert.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/access/dfs/dfs_insert.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DFS_INSERT_H
#define DFS_INSERT_H

#include "access/dfs/dfs_am.h"
#include "access/cstore_insert.h"
#include "access/cstore_psort.h"
#include "access/cstore_vector.h"
#include "catalog/heap.h"
#include "catalog/pg_partition.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "vecexecutor/vectorbatch.h"

class DfsInsertInter;

/*
 * ScalarValue => Datum, Datum should point to data without length header
 * for varing length data type.
 */
extern Datum convertScalarToDatum(Oid typid, ScalarValue val);

/*
 * Create the insertion object for dfs table.
 */
extern DfsInsertInter *CreateDfsInsert(Relation rel, bool is_update, Relation dataDestRel = NULL, Plan *plan = NULL,
                                       MemInfoArg *m_dfsInsertMemInfo = NULL);

/* The globle variable which contains the insert files' info. */
extern HTAB *g_dfsSpaceCache;

typedef void (*InsertPendingFunc)(const char *, Oid, uint64);

/* options for cstore_insert_mode */
#define TO_AUTO 1  /* means enable_delta_store = true, tail data to delta table */
#define TO_MAIN 2  /* means enable_delta_store = false, all data to hdfs store */
#define TO_DELTA 3 /* new option, all data to delta table */

#define MAX_PARSIG_LENGTH (u_sess->attr.attr_storage.dfs_max_parsig_length + 1)
#define MAX_PARSIGS_LENGTH (VALUE_PARTKEYMAXNUM * u_sess->attr.attr_storage.dfs_max_parsig_length + 1)

#define MAX_ACTIVE_PARNUM ((u_sess->attr.attr_storage.partition_max_cache_size * 1024L) / (64 * 1024 * 1024))

typedef struct IndexInsertInfo {
    /* psort index need */
    Relation *idxRelation;
    int *idxKeyNum;
    InsertArg *idxInsertArgs; /* index inserting arguments */
    CStoreInsert **idxInsert;
    int **idxKeyAttr;
    int maxKeyNum;
    int indexNum;
} IndexInsertInfo;

/* Construct the struct of index insert information. */
extern IndexInsertInfo *BuildIndexInsertInfo(Relation rel, ResultRelInfo *resultRelInfo);

/*
 * The Dfs table insert handler, it provides basic interface for Dfs insert protocol
 * > BeginBatchInsert()
 * > BatchInsert()/TupleInsert()
 * > SetEnd()
 * > Destroy()
 *
 * Which is exposed to executor/vecexcetutor level
 */
class DfsInsertInter : public BaseObject {
public:
    virtual ~DfsInsertInter(){};

    /* Prepare the members which are needed while inserting. */
    virtual void BeginBatchInsert(int type, ResultRelInfo *resultRelInfo = NULL) = 0;

    /* Insert a vector batch into DFS. */
    virtual void BatchInsert(VectorBatch *batch, int option) = 0;

    /* Insert a tuple into DFS. */
    virtual void TupleInsert(Datum *values, bool *nulls, int option) = 0;

    /* Clean the member. */
    virtual void Destroy() = 0;

    /* empty function, just for FLUSH_DATA */
    virtual void EndBatchInsert() = 0;

    /* Set the end flag. */
    inline void SetEndFlag()
    {
        m_end = true;
    };

    /* Get the end flag */
    inline bool isEnd()
    {
        return m_end;
    };

    /* data redistribution for DFS table. */
    /* Set dataDestRel. */
    inline void setDataDestRel(Relation dataDestRel)
    {
        m_dataDestRelation = dataDestRel;
    };

    /* Set the index insert object. */
    inline void setIndexInsertInfo(IndexInsertInfo *indexInsertInfo)
    {
        m_indexInsertInfo = indexInsertInfo;
        m_indexInfoOuterDestroy = true;
    }

    /* Get dataDestRel. */
    inline Relation getDataDestRel()
    {
        return m_dataDestRelation;
    };

    /* register function which can insert new file name to pendingDfsDelete */
    inline void RegisterInsertPendingFunc(InsertPendingFunc fn)
    {
        m_insert_fn = fn;
    }

    /* Clean the object of index insert if it is not set outer. */
    inline void clearIndexInsert()
    {
        if (NULL != m_indexInsertInfo && !m_indexInfoOuterDestroy) {
            for (int i = 0; i < m_indexInsertInfo->indexNum; ++i) {
                if (m_indexInsertInfo->idxInsert[i] != NULL) {
                    m_indexInsertInfo->idxInsert[i]->SetEndFlag();
                    m_indexInsertInfo->idxInsert[i]->BatchInsert((bulkload_rows *)NULL, 0);
                    DELETE_EX(m_indexInsertInfo->idxInsert[i]);
                    CStoreInsert::DeInitInsertArg(m_indexInsertInfo->idxInsertArgs[i]);
                }
                relation_close(m_indexInsertInfo->idxRelation[i], RowExclusiveLock);
            }
        }
    }

public:
    /*
     * The m_dataDestRelation keeps the data path destination when execute insert operation.
     * If the m_dataDestRelation is NULL, the data path destination would be acquired from
     * m_relation.
     * In the process of data redistribution, when execute the following command
     * "insert into dataDestRelation select * form redistribution_table",there are two
     * file lists in HDFS directory of redistribution table. The new files that
     * are produced by the above command will be existed in HDFS directory of
     * redistribution table. The old files is data of redistribution table.
     * The two file lists must keep it until finish commit or abort transaction.
     * If commit, delete the old file(file of redistribution table),
     * otherwise delete new file.
     */
    Relation m_dataDestRelation;

protected:
    /* The end flag which must be set outside. */
    bool m_end;

    /*
     * The columns bitmap, it excludes dropped columns.
     * When insert data to new file, must insert column bitmap to DESC table.
     * Every file has different columns, only get the real columns by colmap of DESC table.
     */
    char *m_colMap;

    /* used to insert new orc file name to pendingDfsDelete */
    InsertPendingFunc m_insert_fn;

    /* Index cstore insert info */
    IndexInsertInfo *m_indexInsertInfo;
    bool m_indexInfoOuterDestroy;
};

class DfsInsert : public DfsInsertInter {
public:
    DfsInsert(Relation relation, bool is_update, const char *parsigs = NULL, Plan *plan = NULL,
              MemInfoArg *ArgmemInfo = NULL);
    virtual ~DfsInsert(){};

    /* prepare the memory for insert and sort to use. plan is for insert condition, ArgmemInfo is for update condition. */
    void InitInsertMemArg(Plan *plan, MemInfoArg *ArgmemInfo);

    /* Prepare the members which are needed while inserting. */
    void BeginBatchInsert(int type, ResultRelInfo *resultRelInfo = NULL);

    /* Insert a vector batch into DFS. */
    void BatchInsert(VectorBatch *batch, int option);

    /* Insert a tuple into DFS. */
    void TupleInsert(Datum *values, bool *nulls, int option);

    /**
     * @Description: Set the column map for DFS table. Get the all columns
     * from pg_attribute, but excludes dropped columns.
     * @return None.
     */
    void setColMap();

    /**
     * @Description: Get the column map for DFS table.
     * @return Node.
     */
    const char *getColMap()
    {
        Assert(NULL != m_colMap);
        return m_colMap;
    }

    /* Initialize the dfs space cache. */
    static void InitDfsSpaceCache();

    /* Remove the file space alloc cache. */
    static void InvalidSpaceAllocCache(Oid relid);

    /* Remove all the elements in the space cache. */
    static void ResetDfsSpaceCache(void);

    /* Clean the member. */
    virtual void Destroy();

    /* empty function, just for FLUSH_DATA */
    void EndBatchInsert(){};

private:
    /* The internal entry for batch insert. */
    void batchInsertInternal(VectorBatch *batch, int option, bool isEnd);

    /* THe internal entry for tuple insert. */
    void tupleInsertInternal(Datum *values, bool *nulls, int option, bool isEnd);

    /* Initialze the fixed prefix of file path. */
    void initPathPrefix();

    /* Acquire the absolute path according to the file id. */
    char *getFilePath(uint64 offset) const;

    /* Acquire the file name of the path according to the file id. */
    char *getFileName(uint64 offset) const;

    /* Get the max file ID stored in btree index. */
    uint32 GetMaxIndexFileID(Relation heapRel);

    /* Acquire the max file id stored in space cache for rel. */
    uint64 getMaxFileID(Relation rel) const;

    /* Check if the space cache already exist. */
    bool dfsSpaceCacheExist(Oid relOid) const;

    /* Build the space cache according to maxFileID with entry "relid". */
    void buildSpaceAllocCache(Oid relid, uint64 maxFileID) const;

    /*
     * Initialize the file space alloc cache to provide unique file id
     * according to desc table of rel concurrently.
     */
    void initSpaceAllocCache(Relation rel);

    /* Get the next file id which must be unique from global hashtable with entry "relOid". */
    uint64 allocNextSpace(Oid relOid) const;

    /* Spill the buffer into DFS. */
    void spillOut(int option);

    /* Deal with the tail data when the insert process ends. */
    void handleTailData(int option);

    /* Insert a record into desc table. */
    void descInsert(int option);

    inline bool NeedPartialSort()
    {
        return m_sorter != NULL;
    }

private:
    Relation m_relation;
    bool m_isUpdate;
    TupleDesc m_desc;

    /* The common prefix of all the file path. */
    StringInfo m_filePathPrefix;

    /* The writer to store the buffer and save the file. */
    dfs::writer::Writer *m_writer;

    /* If relation has cluster key, it will work for partial sort. */
    CStorePSort *m_sorter;

    /* The memory context which will be cleaned for each insert iteration. */
    MemoryContext m_iterContext;

    /* The flag which indicates whether we need to create a new file while spilling in the next time. */
    bool m_nextFile;

    /* valid iff TO_DELTA == cstore_insert_mode */
    Relation m_delta;
    Datum *m_values;
    bool *m_nulls;

    /* dfs insert partition memory info. */
    MemInfoArg *m_dfsInsertMemInfo;

public:
    /* partitioned ONLY */
    StringInfo m_parsigs;
};

typedef struct PartitionStagingFile {
    StringInfo parsig;
    BufFile *sfile;
} PartitionStagingFile;

class DfsPartitionInsert : public DfsInsertInter {
public:
    DfsPartitionInsert(Relation relation, bool is_update, Plan *plan = NULL, MemInfoArg *ArgmemInfo = NULL);
    virtual ~DfsPartitionInsert(){};

    /* Prepare the members which are needed while inserting. */
    void BeginBatchInsert(int type, ResultRelInfo *resultRelInfo = NULL);

    /* empty function, just for FLUSH_DATA */
    void EndBatchInsert(){};

    /* Insert a vector batch into DFS. */
    void BatchInsert(VectorBatch *batch, int option);

    /* Insert a tuple into DFS. */
    void TupleInsert(Datum *values, bool *nulls, int option);

    /* Clean the member. */
    virtual void Destroy();

    /* init insert memory info. */
    void InitInsertPartMemArg(Plan *plan, MemInfoArg *ArgmemInfo);

private:
    /*
     * Get One partition's DfsInsert by scanning p_pWriters[], if there is no writer slot
     * available in current DfsPartitionInsert, return NULL to let caller to proper
     * do action like swap tuple content to disk as temp files
     */
    inline DfsInsert *GetDfsInsert(const char *parsig);

    /*
     * Form a partition signature targeting to identify a parition directory for
     * real data file layout. e.g c1=1/c2=1/c3=5/
     *
     * Note: Per-Tuple invoking so INLINE defined
     */
    inline void GetCStringFromDatum(Oid typeOid, int typeMode, Datum value, const char *attname, char *buf) const;

    /*
     * Create a partition signature with current tuple(values/nulls)
     */
    inline char *FormPartitionSignature(Datum *values, const bool *nulls);

    /*
     * Flush buffer tuples into ORCfile or delta table also including those swapped
     * in staging file
     */
    void HandleTailData(int option);

    /*
     * ------- member values -------------------------------
     * Pre-allocated partition-signatures to buffer the content of which is 'c1=1/c2=1/c3=1/',
     * allocated at DfsPartitionInsert constructor
     */
    char *m_parsigs;

    /* The relation of the partition table. */
    Relation m_relation;
    bool m_isUpdate;

    /* The tuple description of the m_relation. */
    TupleDesc m_desc;

    /* The information of index. */
    ResultRelInfo *m_resultRelInfo;

    /*
     * The common prefix of all the file path.
     * e.g /user/tzhong/30_tzhong_mppdb.db/postgres/public.t1p/
     */
    StringInfo m_tableDirPath;

    /*
     * Temp files hold tuples whose DfsInsert is not in active list when number of
     * partition exceeds MAX allowed partitions.
     */
    PartitionStagingFile **m_partitionStagingFiles;

    /* The context for partition buffer files. */
    MemoryContext m_partitionCtx;

    /* cache of <parsig, PartitionStagingFile> for spilled partitions */
    HTAB *m_SpilledPartitionSearchCache;

    /* cache of <parsig, DfsPartition> for active partitions */
    HTAB *m_ActivePartitionSearchCache;

    /* TUPLE_SORT or BATCH_SORT */
    int m_type;

    /*
     * Array length of m_parttitionStagingFiles
     */
    int m_numPartitionStagingFiles;

    /*
     * Get paritition staging file handler with given partition signature
     */
    PartitionStagingFile *GetPartitionStagingFile(const char *parsig);

    /* valid iff TO_DELTA == cstore_insert_mode */
    Relation m_delta;

    /* Array for values and null flags. */
    Datum *m_values;
    bool *m_nulls;

    /*
     * Active 'patition writer' which is used to do TupleInsert()
     */
    DfsInsert **m_pDfsInserts;

    /* The max number of writers which can be used for one partition writer */
    int m_maxPartitionWriters;

    /* The number of active writers for partition table currently. */
    int m_activePartitionWriters;

    /* dfs insert partition memory info. */
    MemInfoArg *dfsPartMemInfo;
};
#endif
