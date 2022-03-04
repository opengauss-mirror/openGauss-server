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
 *
 *  dfs_am.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/dfs_am.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "orc/orc_rw.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "carbondata/carbondata_reader.h"
#endif
#include "parquet/parquet_reader.h"
#include "access/dfs/dfs_common.h"
#include "access/dfs/dfs_query.h"
#include "access/dfs/dfs_query_reader.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/tableam.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dfsstore_ctlg.h"
#include "catalog/pg_type.h"
#include "common_reader.h"
#include "commands/tablespace.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "pgxc/pgxc.h"
#include "replication/walsender.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/snapmgr.h"
#include "access/heapam.h"
#include "dfs_adaptor.h"
#include "executor/executor.h"

void CleanupDfsHandlers(bool isTop)
{
    /* nothing to clean */
    if (list_length(t_thrd.dfs_cxt.pending_free_reader_list) == 0 &&
        list_length(t_thrd.dfs_cxt.pending_free_writer_list) == 0) {
        return;
    }

    /* don't need to clean up if shutdown */
    if (g_instance.status > NoShutdown) {
        ResetDfsHandlerPtrs();
        return;
    }

    ListCell *cell = NULL;
    ListCell *next = NULL;
    int level = GetCurrentTransactionNestLevel();

    /* clean up DFS read handlers */
    for (cell = list_head(t_thrd.dfs_cxt.pending_free_reader_list); cell; cell = next) {
        dfs::reader::Reader *handler = (dfs::reader::Reader *)lfirst(cell);
        next = lnext(cell);
        if (handler->getCurrentTransactionLevel() == level || isTop) {
            t_thrd.dfs_cxt.pending_free_reader_list = list_delete_ptr(t_thrd.dfs_cxt.pending_free_reader_list, handler);
            ereport(DEBUG1,
                    (errmodule(MOD_DFS), errmsg("Cleanup DFS read handler for table %s", handler->getRelName())));
            DELETE_EX(handler);
        }
    }

    if (isTop) {
        list_free(t_thrd.dfs_cxt.pending_free_reader_list);
        t_thrd.dfs_cxt.pending_free_reader_list = NIL;
    }

    /* clean up DFS write handlers */
    for (cell = list_head(t_thrd.dfs_cxt.pending_free_writer_list); cell; cell = next) {
        dfs::writer::Writer *handler = (dfs::writer::Writer *)lfirst(cell);
        next = lnext(cell);
        if (handler->getCurrentTransactionLevel() == level || isTop) {
            t_thrd.dfs_cxt.pending_free_writer_list = list_delete_ptr(t_thrd.dfs_cxt.pending_free_writer_list, handler);
            ereport(DEBUG1,
                    (errmodule(MOD_DFS), errmsg("Cleanup DFS write handler for table %s", handler->getRelName())));
            DELETE_EX(handler);
        }
    }
    if (isTop) {
        list_free(t_thrd.dfs_cxt.pending_free_writer_list);
        t_thrd.dfs_cxt.pending_free_writer_list = NIL;
    }
}

void RemoveDfsReadHandler(const void *handler)
{
    /* we should have the handler in pending_free_reader_list */
    if (!list_member_ptr(t_thrd.dfs_cxt.pending_free_reader_list, handler))
        ereport(ERROR, (errcode(ERRCODE_NONEXISTANT_VARIABLE), errmodule(MOD_DFS),
                        errmsg("read handler is not in pending_free_reader_list")));

    t_thrd.dfs_cxt.pending_free_reader_list = list_delete_ptr(t_thrd.dfs_cxt.pending_free_reader_list, handler);
}

void RemoveDfsWriteHandler(const void *handler)
{
    /* we should have the handler in pending_free_reader_list */
    if (!list_member_ptr(t_thrd.dfs_cxt.pending_free_writer_list, handler))
        ereport(ERROR, (errcode(ERRCODE_NONEXISTANT_VARIABLE), errmodule(MOD_DFS),
                        errmsg("write handler is not in pending_free_writer_list")));

    t_thrd.dfs_cxt.pending_free_writer_list = list_delete_ptr(t_thrd.dfs_cxt.pending_free_writer_list, handler);
}

void ResetDfsHandlerPtrs(void)
{
    t_thrd.dfs_cxt.pending_free_writer_list = NIL;
    t_thrd.dfs_cxt.pending_free_reader_list = NIL;
}

namespace dfs {
/*
 * Transform the ScalarVector into the array of datums.
 */
typedef Datum *(*transformScalarVector)(ScalarVector *vec, bool *isNullOut);
template <Oid type>
Datum *transformScalarVectorT(ScalarVector *vec, bool *isNullOut)
{
    int rows = vec->m_rows;
    ScalarValue *vals = vec->m_vals;
    uint8 *isNull = vec->m_flag;
    Datum *pDatum = NULL;
    switch (type) {
        case TIMETZOID:
        case UNKNOWNOID: {
            pDatum = (Datum *)palloc(sizeof(Datum) * rows);
            for (int i = 0; i < rows; i++) {
                if (NOT_NULL(isNull[i])) {
                    pDatum[i] = convertScalarToDatumT<type>(vals[i]);
                    isNullOut[i] = false;
                } else {
                    isNullOut[i] = true;
                }
            }
            break;
        }
        default: {
            pDatum = (Datum *)vals;
            for (int i = 0; i < rows; i++) {
                if (NOT_NULL(isNull[i])) {
                    isNullOut[i] = false;
                } else {
                    isNullOut[i] = true;
                }
            }
            break;
        }
    }
    return pDatum;
}

namespace reader {
/*
 * THe proxy class includes different readers according to the file type.
 */
class ReaderImpl : public Reader {
public:
    ReaderImpl(ReaderState *readerState, bool _skipSysCol, int transactionLevel);
    virtual ~ReaderImpl();

    /* split files according to smp */
    void dynamicAssignSplits() override;

    /*
     * Fetch the next batch according to the tid vector batch which includes all the columns we need.
     * @OUT batch: the target vector batch
     * @IN tidBatch: the tid vector batch
     * @IN prepareCols: the prepared cols list in the tid vector batch
     */
    void nextBatchByTidsForIndexOnly(VectorBatch *batch, VectorBatch *tidBatch, List *prepareCols);

    /*
     * Add a bloom filter into the reader.
     * @_in_param bloomFilter: The bloom filter to be added.
     * @_in_param colIdx: The index of the column on which to add the bloom filter.
     * @_in_param is_runtime: Is between runtime or not.
     */
    void addBloomFilter(filter::BloomFilter *bloomFilter, int colIdx, bool is_runtime) override
    {
        reader->addBloomFilter(bloomFilter, colIdx, is_runtime);
    }

    /*
     * Fetch the next 1000 or less rows in a vector batch.
     * @_in_out_param batch: The vector batch to be filled by the reader.
     */
    void nextBatch(VectorBatch *batch) override;

    /*
     * The reserved interface to add prepared processes before calling
     * @_in_param conn: The connector handler to hdfs.
     * @_in_param type: The type of the file to read.
     * the nextBatch.
     */
    void begin(dfs::DFSConnector *conn, FileType type) override;

    /*
     * Copy bloomFilter to runTimeBloomFilter
     */
    void copyBloomFilter() override
    {
        reader->copyBloomFilter();
    }

    /*
     * The reserved interface to add finish work when there is no rows
     * to read.
     */
    void Destroy() override;

    /* Get the current reader's transaction ID. */
    const int getCurrentTransactionLevel() override
    {
        return transactionLevel;
    }

    /*
     * @Description: Fetch the next batch according to the tid vector batch.
     * @IN/OUT batch: vector batch
     * @IN tids: tid vector batch
     * @See also:
     */
    void nextBatchByTids(VectorBatch *batch, VectorBatch *tidBatch) override;

    /*
     * Handle the tailing works which are not destroyers.
     */
    void end() override;

    /*
     * Fetch the next one tuple.
     * @_in_out_param tuple: The tuple to be filled by the reader.
     */
    void nextTuple(TupleTableSlot *tuple) override;

    /* Get DFS table name */
    const char *getRelName() override;

private:
    void ResetSystemVector(VectorBatch *batch);
    /*
     * Set all kinds of required columns array for ORC reader according to the required
     * columns of the relation and partition columns' information. Especially, partition
     * columns are not stored in the ORC file so they are not required by ORC reader.
     */
    void setRequired();

    /*
     * set encoding
     */
    void setEncoding();

    /*
     * Initialize the member used for 'nextTuple' interface one the first call.
     * @_in_param tuple: The tuple to be filled.
     */
    void initBatchForRowReader(TupleTableSlot *tuple);

    /*
     * The nextTuple function depends on the nextBatch function, as we always read a
     * vector batch from the file each time and then fetch tuples one by one
     * from the vector batch.
     * @_out_param tuple: The fetched tuple from the vector batch to deal.
     */
    void FetchOneTupleFromBatch(TupleTableSlot *tuple);

    /*
     * Load a new file when the last file is read over.
     * @return true: A new file which matches the restriction is loaded successfully.
     *             false: No more files to be read.
     */
    bool loadNextFile();

    /**
     * @Description: Skip the file by judging default column predicates and lod the file.
     * @in currentFileName, the current file bo be load.
     * @return If load file successfully, return true, otherwise return false.
     */
    bool checkAndLoadFile(char *currentFileName);

    /**
     * @Description: Initialize the column map on ORC file level. Now the DFS table supports
     * the "drop column" feature, each file will has different column counts. When load each file,
     * must initialize the column map. In this function, we have to do the following things:
     * 1. Initialize readerState->readRequired, it represents which column will be needed to read
     * from file.
     * 2. Initialize readerState->readColIDs, it represents the mapping array from mpp column
     * index to file column index.
     * @in currentFileName, initialize column map for this file.
     * @return None.
     */
    void initColIndexByDesc(char *currentFileName);

    /**
     * @Description: Skip the file by judging default column predicates. If the default
     * column predicate qual is pushdown, we must judge the qual on file level(
     * the default column value is stored in catalog, so we do not read default
     * column from ORC file).
     * @return If the default column value satisfies the default column predicates,
     * return true, otherwise return false.
     */
    bool skipFileByDefColPredicates();

    /**
     * @Description: Whether the column is existed in ORC file.
     * @in colNo, the column nomber, the number starts from 1.
     * @return If the column is existed, return true, otherwise return false.
     */
    bool colNonexistenceInFile(int colNo);

    /*
     * @Description: load a new dfs file according to the fileID.
     * @IN fileID: the file id to be load
     * @Return: true if the target file is satisfied, or return false
     * @See also: loadNextFile
     */
    bool loadFile(int fileID);

    /*
     * @Description: check whether the file is satisfied.
     * @IN itemPtr: the file id to be load
     * @Return: true if the target file is satisfied, or return false
     * @See also:
     */
    inline bool checkFileStatus(const ItemPointer itemPtr);

    /*
     * @Description: set the partition information and build the dfs desc of the
     *		current file.
     * @IN currentFileName: the path of the current file to be read
     * @See also:
     */
    void setPartinfoAndDesc(char *currentFileName);

    /*
     * Check if the current file meets the end.
     * @return true: The current file is finished.
     *             false: The current file is not finished.
     */
    bool fileEOF() const;

    /* Clear the memory context and dfsdesc of the last file. */
    void resetFile();

    /*
     * Load the desc info from dfs desctable according to the file path.
     */
    void setDescByFilePath(char *filePath);

    /*
     * Build the restriction according to the min/max of file stored in desc table.
     */
    List *buildRestrictionFromDesc();

    /*
     * Fill the values of the fileOffset array arrording to the current
     * offset and the number of rows to read.
     */
    void fillFileOffset(uint64_t rowsToRead, uint64_t rowsReadInFile);

    /*
     * Fill the required columns in the vector batch.
     * @_in_out_param batch: The batch to be filled.
     */
    void fillRequiredColumns(VectorBatch *batch);

    /*
     * Fill the system junk columns including xmin,ctid,relid,nodeid.
     */
    void fillSystemColumns(VectorBatch *batch);

    /*
     * Fill specified the system column.
     * @_in_param colIdx: The index of the system column.
     * @_in_out_param vec: The scalar vector to be filled.
     * @_in_param rows: The number of rows to be filled.
     */
    void fillSysCol(int colIdx, ScalarVector *vec, int rows);

    /*
     * Fill other columns(unrequired column, partition column).
     * @IN batch: The vector batch to be filled.
     */
    void fillOtherColumns(VectorBatch *batch);

    /* Provide all the params which is needed by the reader. */
    ReaderState *readerState;

    /* The vector batch from which nextTuple function fetches tuples. */
    VectorBatch *batchForRowReader;

    /* The number of system columns. */
    int sysColNum;

    /* The index array of the system columns */
    int *sysColId;

    /* transaction level ID. */
    int transactionLevel;

    /* The flag of the skipping status which is used for index scan. */
    bool skipFileStatus;

    /* The ID of the current file */
    uint64_t currentFileID;

    /* The total number of rows in the current file. */
    uint64_t rowsInFile;

    /* The rows already read in the current file. */
    uint64_t rowsReadInFile;

    /* The number of rows which has not been fetched out from the vector batch. */
    uint64_t rowsUnread;

    /*
     * The key pointer of this class which will be initialized according to
     * the file type and provides the ability to read from file.
     */
    DFSReader *reader;

    /* The function pointer to convert scalar to datum. */
    ScalarToDatum *scalarToDatumFunc;

    /*
     * The context used by nextTuple function which includes the data of
     * batchForRowReader and is cleaned whenever read a new vector batch.
     */
    MemoryContext tupleContext;

    /* The context specially to be used for loading and saving desc information. */
    MemoryContext fileContext;

    /* The byte map for delete elements. */
    DFSDesc *dfsDesc;

    /* The struct to record the tid for each row required. */
    FileOffset *fileOffset;

    /*
     * noRequireCol indicates whether there is at least one column to be
     * read from ORC file or not.
     */
    bool noRequireCol;

    /* Flag indicates whether this reader is used for foreign table or native table. */
    bool isForeignTable;

    /* Flag indicates whether we need to read system columns. */
    bool skipSysCol;
};

ReaderImpl::ReaderImpl(ReaderState *_readerState, bool _skipSysCol, int _transactionLevel)
    : readerState(_readerState),
      batchForRowReader(NULL),
      sysColNum(0),
      sysColId(NULL),
      transactionLevel(_transactionLevel),
      skipFileStatus(false),
      currentFileID(0),
      rowsInFile(0),
      rowsReadInFile(0),
      rowsUnread(0),
      reader(NULL),
      scalarToDatumFunc(NULL),
      tupleContext(NULL),
      fileContext(NULL),
      dfsDesc(NULL),
      fileOffset(NULL),
      noRequireCol(true),
      isForeignTable(false),
      skipSysCol(_skipSysCol)
{
}

ReaderImpl::~ReaderImpl()
{
    /*
     * Here do not call Destroy because all this object should
     * be deleted by DELETE_EX.
     */
    Assert(reader == NULL);
    scalarToDatumFunc = NULL;
    tupleContext = NULL;
    dfsDesc = NULL;
    batchForRowReader = NULL;
    reader = NULL;
    sysColId = NULL;
    fileContext = NULL;
    readerState = NULL;
    fileOffset = NULL;
}

const char *ReaderImpl::getRelName()
{
    return (readerState && readerState->scanstate) ? RelationGetRelationName(readerState->scanstate->ss_currentRelation)
                                                    : NULL;
}

void ReaderImpl::Destroy()
{
    if (batchForRowReader != NULL) {
        delete (batchForRowReader);
        batchForRowReader = NULL;
    }

    if (reader != NULL) {
        DELETE_EX(reader);
    }

    if (scalarToDatumFunc != NULL) {
        pfree_ext(scalarToDatumFunc);
    }

    if (tupleContext != NULL && tupleContext != CurrentMemoryContext) {
        MemoryContextDelete(tupleContext);
    }

    if (fileContext != NULL && fileContext != CurrentMemoryContext) {
        MemoryContextDelete(fileContext);
    }
}

void ReaderImpl::begin(dfs::DFSConnector *conn, FileType type)
{
    setRequired();
    setEncoding();

    /* notice:  readerState should init ready before create file reader */
    switch (type) {
#ifndef ENABLE_LITE_MODE
        case ORC: {
            reader = New(readerState->persistCtx) OrcReaderImpl(readerState, conn);
            reader->begin();
            break;
        }
        case PARQUET: {
            reader = New(readerState->persistCtx) ParquetReader(readerState, conn);
            reader->begin();
            break;
        }
#endif
        case TEXT: {
            reader = New(readerState->persistCtx) CommonReader(readerState, conn, DFS_TEXT);
            reader->begin();
            break;
        }
        case CSV: {
            reader = New(readerState->persistCtx) CommonReader(readerState, conn, DFS_CSV);
            reader->begin();
            break;
        }
#ifdef ENABLE_MULTIPLE_NODES
        case CARBONDATA: {
            reader = New(readerState->persistCtx) CarbondataReader(readerState, conn);
            reader->begin();
            break;
        }
#endif
        default: {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                            errmsg(
#ifdef ENABLE_MULTIPLE_NODES
                                "Only ORC/PARQUET/CARBONDATA/CSV/TEXT is supported for now."
#else
#ifndef ENABLE_LITE_MODE
                                "Only ORC/PARQUET/CSV/TEXT is supported for now."
#else
                                "Only CSV/TEXT is supported for now."
#endif
#endif
            )));
        }
    }

    fileContext = AllocSetContextCreate(readerState->persistCtx, "file level context for dfs reader",
                                        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    if (!skipSysCol) {
        AutoContextSwitch newMemCnxt(readerState->persistCtx);

        fileOffset = (FileOffset *)palloc0(sizeof(FileOffset));
        ProjectionInfo *proj = readerState->scanstate->ps.ps_ProjInfo;
        if (proj != NULL && proj->pi_sysAttrList != NIL) {
            int i = 0;
            ListCell *cell = NULL;
            List *pSysList = proj->pi_sysAttrList;
            sysColNum = list_length(pSysList);
            if (sysColNum == 0) {
                skipSysCol = true;
            } else {
                sysColId = (int *)palloc(sizeof(int) * sysColNum);
                foreach (cell, pSysList) {
                    sysColId[i++] = lfirst_int(cell);
                }
            }
        }
    }
}

void ReaderImpl::end()
{
    if (reader != NULL) {
        reader->end();
    }
}

void ReaderImpl::nextTuple(TupleTableSlot *tuple)
{
    if (batchForRowReader == NULL) {
        initBatchForRowReader(tuple);
    }

    /* When there is no rows to be read, we need to read the next vector batch. */
    if (rowsUnread == 0) {
        MemoryContextReset(tupleContext);
        MemoryContext oldContext = MemoryContextSwitchTo(tupleContext);
        nextBatch(batchForRowReader);
        (void)MemoryContextSwitchTo(oldContext);
        if (BatchIsNull(batchForRowReader)) {
            return;
        } else {
            rowsUnread = batchForRowReader->m_rows;
        }
    }

    FetchOneTupleFromBatch(tuple);
    (void)ExecStoreVirtualTuple(tuple);
    rowsUnread--;
}

void ReaderImpl::nextBatch(VectorBatch *batch)
{
    Assert(batch != NULL);
    batch->Reset(true);
    ResetSystemVector(batch);
    do {
        CHECK_FOR_INTERRUPTS();

        /* When the current file meets eof, we need to load a new file, and
         * if there is no more files, break the loop and return the collected
         * vector batch.
         */
        if (fileEOF() && !loadNextFile()) {
            rowsReadInFile = 0;
            break;
        }

        fillRequiredColumns(batch);
    } while (BatchIsNull(batch) && unlikely(false == executorEarlyStop()));

    if (!BatchIsNull(batch)) {
        /* Fill system columns. */
        fillSystemColumns(batch);

        /* Set null for the unrequired columns and set value for the patition columns. */
        fillOtherColumns(batch);
    }
}

void ReaderImpl::nextBatchByTidsForIndexOnly(VectorBatch *batch, VectorBatch *tidBatch, List *prepareCols)
{
    int tidOffset = 0;
    int batchOffset = 0;
    int tidRows = tidBatch->m_rows;
    uint64_t curRowsInFile = 0;
    int fileID = 0;
    ScalarVector *tids = &(tidBatch->m_arr[tidBatch->m_cols - 1]);

    /* Clean the batch for each iteration. */
    batch->Reset(true);

    /* fill required columns */
    for (tidOffset = 0; tidOffset < tidRows; tidOffset++) {
        Datum tid = tids->m_vals[tidOffset];

        /* Check if the current file is satisfied. */
        ItemPointer itemPtr = (ItemPointer)(&tid);
        fileID = DfsItemPointerGetFileId(itemPtr);
        curRowsInFile = DfsItemPointerGetOffset(itemPtr);

        if (readerState->currentFileID != fileID) {
            resetFile();
            AutoContextSwitch newMemCnxt(fileContext);

            /* Find the split according to the fileID from the split list. */
            readerState->currentSplit = FindFileSplitByID(readerState->splitList, fileID);
            readerState->currentFileID = fileID;
            if (readerState->currentSplit == NULL) {
                /* this branch means meet the uncommited insert files. */
                skipFileStatus = true;
                continue;
            }

            char *currentFileName = readerState->currentSplit->filePath;
            setDescByFilePath(currentFileName);
            skipFileStatus = false;
        } else if (skipFileStatus) {
            continue;
        }

        /* If it is not deleted, shallow copy index scan into batch. */
        if (!dfsDesc->IsDeleted(curRowsInFile - 1)) {
            for (int indexCol = 0; indexCol < tidBatch->m_cols - 1; indexCol++) {
                int heapCol = list_nth_int(prepareCols, indexCol) - 1;
                if (NOT_NULL(tidBatch->m_arr[indexCol].m_flag[tidOffset])) {
                    batch->m_arr[heapCol].m_vals[batchOffset] = tidBatch->m_arr[indexCol].m_vals[tidOffset];
                } else {
                    batch->m_arr[heapCol].SetNull(batchOffset);
                }
            }
            batchOffset++;
        }
    }
    batch->m_rows = batchOffset;

    /* fill no required columns and set m_rows for scalar vector */
    for (int i = 0; i < batch->m_cols; i++) {
        ScalarVector *vec = &(batch->m_arr[i]);
        if (!list_member_int(prepareCols, i + 1)) {
            SetAllValue(vec, batch->m_rows, (Datum)0, true);
        }
        vec->m_rows = batch->m_rows;
    }
}

void ReaderImpl::nextBatchByTids(VectorBatch *batch, VectorBatch *tidBatch)
{
    int i = 0;
    int rows = tidBatch->m_rows;
    uint64_t curRowsInFile = 0;
    uint64_t lastRowsInFile = 0;
    uint64_t continueTids[BatchMaxSize];
    uint64_t continueLen = 0;
    ScalarVector *tids = &(tidBatch->m_arr[tidBatch->m_cols - 1]);

    /* Clean the batch for each iteration. */
    batch->Reset(true);
    ResetSystemVector(batch);

    for (i = 0; i < rows; i++) {
        Datum tid = tids->m_vals[i];

        /* Check if the current file is satisfied. */
        ItemPointer itemPtr = (ItemPointer)(&tid);
        int tmpFileID = DfsItemPointerGetFileId(itemPtr);
        curRowsInFile = DfsItemPointerGetOffset(itemPtr);

        /* Inside the same file */
        if (readerState->currentFileID == tmpFileID) {
            if (skipFileStatus) {
                /* If the current file should be skiped, then just continue. */
                continue;
            } else {
                /* check the continuous tids or this is the start of a new iterator scan */
                if (continueLen == 0 || curRowsInFile - lastRowsInFile == 1) {
                    continueTids[continueLen] = curRowsInFile;
                    continueLen++;
                    lastRowsInFile = curRowsInFile;
                    continue;
                } else {
                    /* deal with the prepared tids list */
                    fileOffset->numelements = 0;
                    reader->nextBatchByContinueTids(continueTids[0] - rowsReadInFile - 1, continueLen, batch,
                                                    continueTids[0] - 1, dfsDesc, fileOffset);

                    /* fill system columns */
                    fillSystemColumns(batch);

                    /*
                     * Once we read inside a file, the start offset of the next
                     * skip is the last rows of this read.
                     */
                    rowsReadInFile = continueTids[continueLen - 1];

                    /* start a new continue tids list */
                    continueLen = 0;
                    continueTids[continueLen] = curRowsInFile;
                    continueLen++;
                }
            }
        } else {
            /* deal with the prepared tids list of the last file if exists */
            if (continueLen > 0) {
                fileOffset->numelements = 0;
                reader->nextBatchByContinueTids(continueTids[0] - rowsReadInFile - 1, continueLen, batch,
                                                continueTids[0] - 1, dfsDesc, fileOffset);

                /* fill system columns */
                fillSystemColumns(batch);

                /*
                 * fill the other columns for the current file, including partition
                 * value, default columns, no required columns
                 */
                fillOtherColumns(batch);

                /* clear the continue tids list */
                rowsReadInFile = 0;
                continueLen = 0;
            }

            /* need to open new file, the continyeLen must be 0 when reach here. */
            if (loadFile(tmpFileID)) {
                /* for any new file, the skip offset should be reset to 0. */
                rowsReadInFile = 0;

                /* start a new continue tids list */
                continueTids[continueLen] = curRowsInFile;
                continueLen++;

                skipFileStatus = false;
            } else {
                /* The current file is not satisfied. */
                skipFileStatus = true;
                continue;
            }
        }

        lastRowsInFile = curRowsInFile;
    }

    if (continueLen > 0) {
        /* deal with the prepared tids list */
        fileOffset->numelements = 0;
        reader->nextBatchByContinueTids(continueTids[0] - rowsReadInFile - 1, continueLen, batch, continueTids[0] - 1,
                                        dfsDesc, fileOffset);

        /* fill system columns */
        fillSystemColumns(batch);

        /*
         * fill the other columns for the current file, including partition
         * value, default columns, no required columns
         */
        fillOtherColumns(batch);

        /*
         * Once we read inside a file, the start offset of the next
         * skip is the last rows of this read.
         */
        rowsReadInFile = continueTids[continueLen - 1];
    }
}

void ReaderImpl::ResetSystemVector(VectorBatch *batch)
{
    for (int i = 0; i < sysColNum; ++i) {
        int sysColIdx = sysColId[i];
        ScalarVector *sysVec = batch->GetSysVector(sysColIdx);
        sysVec->m_rows = 0;
    }
}

void ReaderImpl::dynamicAssignSplits()
{
    readerState->currentFileID = 0;
    if (list_length(readerState->splitList) == 0) {
        return;
    }

    /* foreach split, dynamic prun partition */
    List *splits = list_copy(readerState->splitList);
    List *partList = readerState->partList;
    ListCell *lc1 = NULL;
    ListCell *lc2 = NULL;
    ListCell *lc3 = NULL;
    int count = 0;

    foreach (lc1, splits) {
        bool skip = false;
        SplitInfo *sp = (SplitInfo *)lfirst(lc1);
        List *partColValueList = sp->partContentList;
        forboth(lc2, partList, lc3, partColValueList)
        {
            uint32_t colID = lfirst_int(lc2) - 1;
            Value *partValue = (Value *)lfirst(lc3);
            char *colValue = partValue->val.str;
            if (reader->checkBFPruning(colID, colValue)) {
                skip = true;
                break;
            }
        }

        if (skip) {
            readerState->splitList = list_delete(readerState->splitList, sp);
        } else if (u_sess->stream_cxt.producer_dop > 1) {
            /* split again for SMP */
            if ((count % u_sess->stream_cxt.producer_dop) != u_sess->stream_cxt.smp_id) {
                readerState->splitList = list_delete(readerState->splitList, sp);
            }
            count++;
        }
    }

    list_free(splits);
    splits = NIL;

    /* tune the ordered read column list, put the columns which have pck first. */
    Relation rel = readerState->scanstate->ss_currentRelation;
    TupleConstr *constr = rel->rd_att->constr;
    if (constr != NULL && constr->clusterKeyNum > 0) {
        bool *restrictRequired = readerState->restrictRequired;
        uint32_t *orderedCols = readerState->orderedCols;
        uint32 bound = 0;
        uint32 offset = 0;
        uint16 sortKeyNum = constr->clusterKeyNum;
        AttrNumber *sortKeys = constr->clusterKeys;
        for (uint32 i = 0; i < sortKeyNum; i++) {
            AttrNumber pckColIdx = sortKeys[i] - 1;
            if (restrictRequired[pckColIdx]) {
                for (bound = 0; bound < readerState->relAttrNum; bound++) {
                    if (((int)(orderedCols[bound])) == pckColIdx) {
                        break;
                    }
                }

                Assert(((int)(orderedCols[bound])) == pckColIdx);
                orderedCols[bound] = orderedCols[offset];
                orderedCols[offset] = pckColIdx;
                offset++;
            }
        }
        elog(LOG, "Reset the order of read columns, the first one is column %u of relation %s", orderedCols[0],
             RelationGetRelationName(rel));
    }
}

/* The following functions are private. */
void ReaderImpl::setRequired()
{
    bool *required = readerState->isRequired;
    List *partList = readerState->partList;
    uint32_t *colNoMapArr = readerState->colNoMapArr;
    bool *restrictRequired = readerState->restrictRequired;

    MemoryContext oldContext = MemoryContextSwitchTo(readerState->persistCtx);
    readerState->readRequired = (bool *)palloc0(sizeof(bool) * readerState->relAttrNum);
    readerState->globalReadRequired = (bool *)palloc0(sizeof(bool) * readerState->relAttrNum);
    readerState->readColIDs = (uint32_t *)palloc0(sizeof(uint32_t) * readerState->relAttrNum);
    readerState->globalColIds = (uint32_t *)palloc0(sizeof(uint32_t) * readerState->relAttrNum);
    (void)MemoryContextSwitchTo(oldContext);

    for (uint32_t i = 0; i < readerState->relAttrNum; i++) {
        /*
         * The column is required by ORC reader when it is required by the
         * query and it is not a partition column.
         */
        if (required[i] && (partList == NULL || !list_member_int(partList, i + 1))) {
            readerState->globalReadRequired[i] = true;
            noRequireCol = false;
        } else {
            restrictRequired[i] = false;
        }

        if (readerState->partNum > 0) {
            readerState->globalColIds[i] = colNoMapArr[i] - 1;
        } else {
            readerState->globalColIds[i] = i;
        }
    }
}

void ReaderImpl::setEncoding()
{
    ScanState *state = readerState->scanstate;
    Assert(state != NULL);
    if (RelationIsForeignTable(state->ss_currentRelation)) {
        const char *encoding = NULL;
        const char *checkEncodingLevel = NULL;
        isForeignTable = true;
        skipSysCol = true;

        ForeignScanState *fss = (ForeignScanState *)state;
        if (!fss->options) {
            encoding = HdfsGetOptionValue(RelationGetRelid(state->ss_currentRelation), OPTION_NAME_ENCODING);
            checkEncodingLevel = HdfsGetOptionValue(RelationGetRelid(state->ss_currentRelation),
                                                    OPTION_NAME_CHECK_ENCODING);
        } else {
            encoding = getFTOptionValue(fss->options->fOptions, OPTION_NAME_ENCODING);
            checkEncodingLevel = getFTOptionValue(fss->options->fOptions, OPTION_NAME_CHECK_ENCODING);
        }

        if (encoding == NULL) {
            readerState->fdwEncoding = PG_UTF8;
        } else {
            readerState->fdwEncoding = pg_char_to_encoding(encoding);
        }

        if (checkEncodingLevel == NULL) {
            readerState->checkEncodingLevel = LOW_ENCODING_CHECK;
        } else {
            if (0 == pg_strcasecmp(checkEncodingLevel, "high")) {
                readerState->checkEncodingLevel = HIGH_ENCODING_CHECK;
            } else {
                readerState->checkEncodingLevel = LOW_ENCODING_CHECK;
            }
        }
    } else {
        readerState->checkEncodingLevel = NO_ENCODING_CHECK;
    }
}

void ReaderImpl::initBatchForRowReader(TupleTableSlot *tuple)
{
    MemoryContext oldContext = MemoryContextSwitchTo(readerState->persistCtx);
    batchForRowReader = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, tuple->tts_tupleDescriptor);
    scalarToDatumFunc = (ScalarToDatum *)palloc0(sizeof(ScalarToDatum) * batchForRowReader->m_cols);
    tupleContext = AllocSetContextCreate(CurrentMemoryContext, "hdfs_fdw row reader context", ALLOCSET_DEFAULT_MINSIZE,
                                         ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    /* Initialize the template function pointer to convert data. */
    for (uint32_t i = 0; i < static_cast<uint32_t>(batchForRowReader->m_cols); i++) {
        switch (batchForRowReader->m_arr[i].m_desc.typeId) {
            case BPCHAROID:
            case TEXTOID:
            case VARCHAROID: {
                scalarToDatumFunc[i] = convertScalarToDatumT<VARCHAROID>;
                break;
            }
            case TIMETZOID:
            case TINTERVALOID:
            case INTERVALOID:
            case NAMEOID: {
                scalarToDatumFunc[i] = convertScalarToDatumT<TIMETZOID>;
                break;
            }
            case UNKNOWNOID:
            case CSTRINGOID: {
                scalarToDatumFunc[i] = convertScalarToDatumT<UNKNOWNOID>;
                break;
            }
            default: {
                scalarToDatumFunc[i] = convertScalarToDatumT<-2>;
                break;
            }
        }
    }
    (void)MemoryContextSwitchTo(oldContext);
}

void ReaderImpl::FetchOneTupleFromBatch(TupleTableSlot *tuple)
{
    errno_t rc = EOK;
    uint32_t rowIdx = batchForRowReader->m_rows - rowsUnread;
    bool *columnNulls = tuple->tts_isnull;
    Datum *columnValues = tuple->tts_values;
    rc = memset_s(columnNulls, batchForRowReader->m_cols * sizeof(bool), true,
                  batchForRowReader->m_cols * sizeof(bool));
    securec_check(rc, "\0", "\0");
    for (int i = 0; i < batchForRowReader->m_cols; i++) {
        /* Only the target columns are needed to return. */
        if (readerState->targetRequired[i]) {
            ScalarVector *vec = &batchForRowReader->m_arr[i];
            if (NOT_NULL(vec->m_flag[rowIdx])) {
                columnNulls[i] = false;
                columnValues[i] = scalarToDatumFunc[i](vec->m_vals[rowIdx]);
            }
        }
    }
}

void ReaderImpl::fillRequiredColumns(VectorBatch *batch)
{
    if (noRequireCol) {
        /*
         * When there is no columns need to be read from ORC, either no
         * required column like count(*) or all the required columns are
         * partition columns, we just set the rows of batch without reading.
         */
        uint64_t rowsToRead = Min(BatchMaxSize, rowsInFile - rowsReadInFile);
        if (skipSysCol) {
            batch->m_rows = rowsToRead;
        } else {
            fileOffset->numelements = 0;
            fillFileOffset(rowsToRead, rowsReadInFile);
            batch->m_rows = fileOffset->numelements;
        }
        rowsReadInFile += rowsToRead;
    } else {
        if (skipSysCol) {
            rowsReadInFile += reader->nextBatch(batch, rowsReadInFile, dfsDesc, NULL);
        } else {
            fileOffset->numelements = 0;
            rowsReadInFile += reader->nextBatch(batch, rowsReadInFile, dfsDesc, fileOffset);
            Assert(fileOffset->numelements == (uint32)batch->m_rows);
        }
    }
}

void ReaderImpl::fillFileOffset(uint64_t rowsToRead, uint64_t rowsInfile)
{
    uint32 *offset = fileOffset->rowIndexInFile;
    uint32 start = fileOffset->numelements;
    for (uint64_t i = 0; i < rowsToRead; i++) {
        uint64_t rowIndex = rowsInfile + i;
        if (!dfsDesc->IsDeleted(rowIndex)) {
            offset[start++] = rowIndex + 1;
        }
    }
    fileOffset->numelements = start;
}

void ReaderImpl::fillSystemColumns(VectorBatch *batch)
{
    for (int i = 0; i < sysColNum; ++i) {
        int sysColIdx = sysColId[i];
        ScalarVector *sysVec = batch->GetSysVector(sysColIdx);
        fillSysCol(sysColIdx, sysVec, batch->m_rows);
    }
}

void ReaderImpl::fillSysCol(int colIdx, ScalarVector *vec, int rows)
{
    int offset = vec->m_rows;

    /* this system column has been filled */
    if (rows == offset) {
        return;
    }

    uint32_t fileID = dfsDesc->GetDescId();
    uint32_t *fileOffsetIdx = fileOffset->rowIndexInFile;
    Assert(rows - offset == (int)fileOffset->numelements);
    ScalarValue *values = vec->m_vals;
    errno_t rc = memset_s(vec->m_flag + offset, sizeof(uint8) * (rows - offset), 0, sizeof(uint8) * (rows - offset));
    securec_check(rc, "", "");

    for (int i = offset; i < rows; i++) {
        switch (colIdx) {
            case SelfItemPointerAttributeNumber: {
                vec->m_desc.typeId = INT8OID;
                values[i] = 0;
                ItemPointer itemPtr = (ItemPointer)&values[i];
                DfsItemPointerSet(itemPtr, fileID, fileOffsetIdx[i - offset]);
                break;
            }
            case XC_NodeIdAttributeNumber: {
                values[i] = u_sess->pgxc_cxt.PGXCNodeIdentifier;
                break;
            }
            case TableOidAttributeNumber: {
                values[i] = RelationGetRelid(readerState->scanstate->ss_currentRelation);
                break;
            }
            case MinTransactionIdAttributeNumber: {
                values[i] = dfsDesc->GetMagic();
                break;
            }
            default:
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                                errmsg("Column store don't support")));
                break;
        }
    }
    vec->m_rows = rows;
}

void ReaderImpl::fillOtherColumns(VectorBatch *batch)
{
    bool *targetRequired = readerState->targetRequired;
    char **partitionColValueArr = readerState->partitionColValueArr;
    bool *orcReadRequired = readerState->readRequired;
    for (int i = 0; i < batch->m_cols; i++) {
        ScalarVector *vec = &(batch->m_arr[i]);
        Datum datumValue = 0;
        bool isNull = false;
        bool needFill = false;

        /* Fill the unquired columns with NULL. */
        if (!targetRequired[i]) {
            isNull = true;
            needFill = true;
        } else if (partitionColValueArr != NULL && partitionColValueArr[i] != NULL) {
            /* Fill the partition columns with value of filename. */
            isNull = pg_strncasecmp(partitionColValueArr[i], DEFAULT_HIVE_NULL, DEFAULT_HIVE_NULL_LENGTH) ? false
                                                                                                            : true;
            if (!isNull) {
                datumValue = GetDatumFromString(vec->m_desc.typeId, vec->m_desc.typeMod, partitionColValueArr[i]);
            }
            needFill = true;
        } else if (!orcReadRequired[i]) {
            /*
             * Belong to the target columns, but can not read from file, so set the default value.
             * There are two situations:
             * (1) for foreign table, the file column's number can be naturally
             *     less or bigger than the relation defined.
             * (2) for non-foreign table, the column may be dropped.
             */
            ScanState *scanState = readerState->scanstate;  // Get the default column values from catalog.
            datumValue = heapGetInitDefVal(i + 1, scanState->ss_currentRelation->rd_att, &isNull);
            needFill = true;
            ereport(DEBUG1,
                    (errmodule(MOD_DFS), errmsg("Read default column %s from catalog for relation %s.",
                                                get_attname(RelationGetRelid(scanState->ss_currentRelation), i + 1),
                                                RelationGetRelationName(scanState->ss_currentRelation))));
        }

        if (needFill) {
            SetAllValue(vec, batch->m_rows, datumValue, isNull);
        }

        vec->m_rows = batch->m_rows;
    }
}

/*
 * @Description: set the partition information and build the dfs desc of the current file.
 * @IN currentFileName: the path of the current file to be read
 */
void ReaderImpl::setPartinfoAndDesc(char *currentFileName)
{
    /*
     * Fill the partitionColValueArr according to partColValueList of
     * the current split.
     */
    List *partColValueList = readerState->currentSplit->partContentList;
    for (uint32_t i = 0; i < readerState->partNum; i++) {
        int IndexColNum = list_nth_int(readerState->partList, i);
        Value *partValue = (Value *)list_nth(partColValueList, i);
        readerState->partitionColValueArr[IndexColNum - 1] = partValue->val.str;
    }

    /* Build desc object. */
    if (!isForeignTable) {
        setDescByFilePath(currentFileName);
        Assert(dfsDesc != NULL);
    }
}

/*
 * @Description: load a new dfs file according to the fileID.
 * @IN fileID: the file id to be load
 * @Return: true if the target file is satisfied, or return false
 * @See also: loadNextFile
 */
bool ReaderImpl::loadFile(int fileID)
{
    char *currentFileName = NULL;

    resetFile();
    AutoContextSwitch newMemCnxt(fileContext);

    /* Find the split according to the fileID from the split list. */
    readerState->currentSplit = FindFileSplitByID(readerState->splitList, fileID);
    readerState->currentFileID = fileID;
    if (readerState->currentSplit == NULL) {
        return false;
    }

    currentFileName = readerState->currentSplit->filePath;
    setPartinfoAndDesc(currentFileName);
    reader->setBatchCapacity(1);

    /* Try to load a new file and check restrictions. */
    if (!checkAndLoadFile(currentFileName)) {
        return false;
    }

    /* Reset file status. */
    if (noRequireCol) {
        rowsInFile = reader->getNumberOfRows();
    }
    rowsReadInFile = 0;

    return true;
}

bool ReaderImpl::checkAndLoadFile(char *currentFileName)
{
    bool satisfied = true;

    if (skipFileByDefColPredicates()) {
        ereport(DEBUG1, (errmodule(MOD_DFS),
                         errmsg("Pruning on relation %s with default column predicates. file:\"%s\".",
                                RelationGetRelationName(readerState->scanstate->ss_currentRelation), currentFileName)));
        satisfied = false;
    } else {
        /* Initialize the column map on ORC file level. */
        initColIndexByDesc(currentFileName);
    }

    /* get file size from dfsDes if it a internal dfs table */
    if (dfsDesc != NULL) {
        readerState->currentFileSize = dfsDesc->GetFileSize();
    } else {
        Assert(readerState->currentSplit != NULL);
        readerState->currentFileSize = readerState->currentSplit->ObjectSize;
    }

    /*
     * If do not filter this file by default column predicates, load this file.
     * If failed to load this file, set satisfied to false, it means that
     * the file dose not satisfy pushdown predicates and need load new file.
     */
    if (satisfied && !reader->loadFile(currentFileName, buildRestrictionFromDesc())) {
        satisfied = false;
    }

    return satisfied;
}

bool ReaderImpl::loadNextFile()
{
    char *currentFileName = NULL;

    resetFile();
    AutoContextSwitch newMemCnxt(fileContext);

    do {
        /* Free the last split before we fetch a new one. */
        if (readerState->currentSplit) {
            DestroySplit(readerState->currentSplit);
            readerState->currentSplit = NULL;
        }

        /* There is no more files to be processed by this datanode */
        if (0 == list_length(readerState->splitList)) {
            return false;
        }

        readerState->currentSplit = ParseFileSplitList(&readerState->splitList, &currentFileName);
        if (!isForeignTable) {
            if (currentFileName == NULL) {
                continue;
            }
            char *substr = strrchr(currentFileName, '.');
            if (substr == NULL) {
                ereport(ERROR, (errmodule(MOD_DFS), errmsg("Failed to get currentFileName !")));
            }
            readerState->currentFileID = pg_strtoint32(substr + 1);
        } else {
            readerState->currentFileID = -1;
        }
        setPartinfoAndDesc(currentFileName);
    } while (!checkAndLoadFile(currentFileName));

    if (noRequireCol) {
        rowsInFile = reader->getNumberOfRows();
    }
    rowsReadInFile = 0;
    return true;
}

void ReaderImpl::resetFile()
{
    if (dfsDesc != NULL) {
        DELETE_EX(dfsDesc);
    }

    MemoryContextReset(fileContext);
}

void ReaderImpl::setDescByFilePath(char *filePath)
{
    dfsDesc = New(CurrentMemoryContext) DFSDesc();
    ScanKeyData Key[1];

    char *fileName = NULL;
    Relation rel = readerState->scanstate->ss_currentRelation;
    Oid descOid = rel->rd_rel->relcudescrelid;

    /* we need adjust the fileName for Value-Partition case */
    if (RelationIsValuePartitioned(rel)) {
        fileName = (char *)filePath + strlen(getDfsStorePath(rel)->data);
    } else {
        fileName = strrchr(filePath, '/');
    }
    if (fileName == NULL) {
        ereport(ERROR, (errmodule(MOD_DFS), errmsg("setDescByFilePath cannot find filename")));
    }

    /* Search in the desc table by the fileName. */
    ScanKeyInit(&Key[0], (AttrNumber)Anum_pg_dfsdesc_relativelyfilename, BTEqualStrategyNumber, F_TEXTEQ,
                CStringGetTextDatum(fileName + 1));
    Relation descHeapRel = heap_open(descOid, AccessShareLock);
    TupleDesc descTupDesc = RelationGetDescr(descHeapRel);
    TableScanDesc descScan = tableam_scan_begin(descHeapRel, readerState->snapshot, 1, Key);

    HeapTuple tup = NULL;
    if ((tup = (HeapTuple) tableam_scan_getnexttuple(descScan, ForwardScanDirection)) != NULL) {
        DFSDescHandler handler(1, RelationGetNumberOfAttributes(rel), rel);
        handler.TupleToDfsDesc(tup, descTupDesc, dfsDesc);
    }

    tableam_scan_end(descScan);
    heap_close(descHeapRel, AccessShareLock);
}

/*
 * Build the restriction according to the min/max of file stored in desc table.
 */
List *ReaderImpl::buildRestrictionFromDesc()
{
    /* There is no desc table for foreign table. */
    if (isForeignTable) {
        return NIL;
    }

    bool *orcRequired = readerState->readRequired;
    bool *restrictRequired = readerState->restrictRequired;
    List *fileRestriction = NIL;

    for (uint32_t col = 0; col < readerState->relAttrNum; col++) {
        /* For optimization, we only build restrictions on columns which have quals. */
        if (dfsDesc != NULL && restrictRequired[col] && orcRequired[col]) {
            Var *var = GetVarFromColumnList(readerState->allColumnList, col + 1);
            bool hasMin = false;
            bool hasMax = false;
            stMinMax *minVal = dfsDesc->GetMinVal(col);
            stMinMax *maxVal = dfsDesc->GetMaxVal(col);
            Datum minValue = 0;
            Datum maxValue = 0;
            OpExpr *greaterThanExpr = NULL;
            OpExpr *lessThanExpr = NULL;

            /* Build greater than expr */
            if (minVal != NULL && var != NULL) {
                hasMin = true;
                greaterThanExpr = MakeOperatorExpression(var, BTGreaterEqualStrategyNumber);
                minValue = GetDatumFromString(var->vartype, var->vartypmod, minVal->szContent);
            }

            /* Build less than expr */
            if (maxVal != NULL && var != NULL) {
                hasMax = true;
                lessThanExpr = MakeOperatorExpression(var, BTLessEqualStrategyNumber);
                maxValue = GetDatumFromString(var->vartype, var->vartypmod, maxVal->szContent);
            }

            /* Build restriction by min/max. */
            Node *baseRestriction = MakeBaseConstraintWithExpr(lessThanExpr, greaterThanExpr, minValue, maxValue,
                                                               hasMin, hasMax);
            if (baseRestriction != NULL) {
                fileRestriction = lappend(fileRestriction, baseRestriction);
            }
        }
    }

    return fileRestriction;
}

/*
 * @Description: check whether the file is satisfied.
 * @IN itemPtr: the file id to be load
 * @IN/OUT skipFile: flag indicates whether the file is skipped
 * @Return: true if the target file is satisfied, or return false
 * @See also:
 */
inline bool ReaderImpl::checkFileStatus(const ItemPointer itemPtr)
{
    uint32 fileID = DfsItemPointerGetFileId(itemPtr);
    if (currentFileID != fileID) {
        /* If the tid is inside a new file */
        currentFileID = fileID;
        if (loadFile(fileID)) {
            skipFileStatus = false;
        } else {
            /* The current file is not satisfied. */
            skipFileStatus = true;
        }
    }

    return !skipFileStatus;
}

// handle table define not equal globalReadRequired -> readRequired
void ReaderImpl::initColIndexByDesc(char *currentFileName)
{
    int placeholderCnt = 0;

    for (uint32_t i = 0; i < readerState->relAttrNum; i++) {
        bool nonexistence = colNonexistenceInFile(i + 1);
        if (nonexistence) {
            /*
             * If the column is not existed in ORC file, we will do not get the column
             * data from ORC file. The column data may be stored in catalog or partition
             * file name.
             */
            placeholderCnt++;
            readerState->readRequired[i] = false;
            continue;
        } else if (readerState->globalReadRequired[i]) {
            /* If this column is dropped, dose not reach here. */
            readerState->readRequired[i] = true;
            readerState->readColIDs[i] = readerState->globalColIds[i] - placeholderCnt;
            ereport(DEBUG1,
                    (errmodule(MOD_DFS),
                     errmsg("Prepare to read column %s from file \"%s\" for relation %s.",
                            get_attname(RelationGetRelid(readerState->scanstate->ss_currentRelation), i + 1),
                            currentFileName, RelationGetRelationName(readerState->scanstate->ss_currentRelation))));
        } else {
            readerState->readRequired[i] = false;
        }
    }
}

bool ReaderImpl::skipFileByDefColPredicates()
{
    List **predicateArr = readerState->hdfsScanPredicateArr;
    bool skipFile = false;
    if (isForeignTable) {
        return skipFile;
    }

    /*
     * We have to check each default column with predicates.
     * Once one column value dose not satisfy the predicates,
     * it is not necessary to load the current file.
     */
    List *partList = readerState->partList;
    for (uint32_t i = 0; i < readerState->relAttrNum; i++) {
        /* skip partition column which can not be default column. */
        if (partList == NULL || !list_member_int(partList, i + 1)) {
            uint32_t arrayIndex = readerState->globalColIds[i];
            if (predicateArr[arrayIndex] && readerState->restrictRequired[i] && colNonexistenceInFile(i + 1)) {
                bool isNull = false;
                ScanState *scanState = readerState->scanstate;
                Var *var = GetVarFromColumnList(readerState->allColumnList, i + 1);
                Datum result = heapGetInitDefVal(i + 1, scanState->ss_currentRelation->rd_att, &isNull);
                if (var == NULL || false == defColSatisfyPredicates(isNull, result, var, predicateArr[arrayIndex])) {
                    skipFile = true;
                    break;
                }
            }
        }
    }

    return skipFile;
}

bool ReaderImpl::colNonexistenceInFile(int colNo)
{
    if (dfsDesc != NULL && dfsDesc->isDefaultColumn(colNo)) {
        return true;
    }
    return false;
}

inline bool ReaderImpl::fileEOF() const
{
    return rowsReadInFile == 0 || reader->fileEOF(rowsReadInFile);
}

/*
 * @Description: The factory function to create a reader for ORC file.
 * @IN readerState: the state information for reading
 * @IN conn: the connector to dfs
 * @IN skipSysCol: skip reading system columns if true
 * @Return: the reader pointer
 * @See also:
 */
Reader *createOrcReader(ReaderState *readerState, dfs::DFSConnector *conn, bool skipSysCol)
{
    Reader *reader = New(readerState->persistCtx) ReaderImpl(readerState, skipSysCol, GetCurrentTransactionNestLevel());
    reader->begin(conn, ORC);

    /*
     * Register the read handler to pending free list, the pending reader list
     * allocated in Executor context
     */
    AutoContextSwitch newMemCnxt(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    t_thrd.dfs_cxt.pending_free_reader_list = lappend(t_thrd.dfs_cxt.pending_free_reader_list, reader);
    return reader;
}

/*
 * @Description: The factory function to create a reader for Parquet file.
 * @IN readerState: the state information for reading
 * @IN conn: the connector to dfs
 * @IN skipSysCol: skip reading system columns if true
 * @Return: the reader pointer
 * @See also:
 */
Reader *createParquetReader(ReaderState *readerState, dfs::DFSConnector *conn, bool skipSysCol)
{
    Reader *reader = New(readerState->persistCtx) ReaderImpl(readerState, skipSysCol, GetCurrentTransactionNestLevel());
    reader->begin(conn, PARQUET);

    /*
     * Register the read handler to pending free list, the pending reader list
     * allocated in Executor context
     */
    AutoContextSwitch newMemCnxt(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    t_thrd.dfs_cxt.pending_free_reader_list = lappend(t_thrd.dfs_cxt.pending_free_reader_list, reader);
    return reader;
}

#ifdef ENABLE_MULTIPLE_NODES
/*
 * @Description: The factory function to create a reader for Carbondata file.
 * @IN readerState: the state information for reading
 * @IN conn: the connector to dfs
 * @IN skipSysCol: skip reading system columns if true
 * @Return: the reader pointer
 * @See also:
 */
Reader *createCarbondataReader(ReaderState *readerState, dfs::DFSConnector *conn, bool skipSysCol)
{
    Reader *reader = New(readerState->persistCtx) ReaderImpl(readerState, skipSysCol, GetCurrentTransactionNestLevel());
    reader->begin(conn, CARBONDATA);

    /*
     * Register the read handler to pending free list, the pending reader list
     * allocated in Executor context
     */
    AutoContextSwitch newMemCnxt(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    t_thrd.dfs_cxt.pending_free_reader_list = lappend(t_thrd.dfs_cxt.pending_free_reader_list, reader);
    return reader;
}
#endif

/*
 * @Description: The factory function to create a reader for TEXT file.
 * @IN readerState: the state information for reading
 * @IN conn: the connector to dfs
 * @IN skipSysCol: skip reading system columns if true
 * @Return: the reader pointer
 * @See also:
 */
Reader *createTextReader(ReaderState *readerState, dfs::DFSConnector *conn, bool skipSysCol)
{
    Reader *reader = New(readerState->persistCtx) ReaderImpl(readerState, skipSysCol, GetCurrentTransactionNestLevel());
    reader->begin(conn, TEXT);

    /*
     * Register the read handler to pending free list, the pending reader list
     * allocated in Executor context
     */
    AutoContextSwitch newMemCnxt(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    t_thrd.dfs_cxt.pending_free_reader_list = lappend(t_thrd.dfs_cxt.pending_free_reader_list, reader);
    return reader;
}

/*
 * @Description: The factory function to create a reader for CSV file.
 * @IN readerState: the state information for reading
 * @IN conn: the connector to dfs
 * @IN skipSysCol: skip reading system columns if true
 * @Return: the reader pointer
 * @See also:
 */
Reader *createCsvReader(ReaderState *readerState, dfs::DFSConnector *conn, bool skipSysCol)
{
    Reader *reader = New(readerState->persistCtx) ReaderImpl(readerState, skipSysCol, GetCurrentTransactionNestLevel());
    reader->begin(conn, CSV);

    /*
     * Register the read handler to pending free list, the pending reader list
     * allocated in Executor context
     */
    AutoContextSwitch newMemCnxt(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    t_thrd.dfs_cxt.pending_free_reader_list = lappend(t_thrd.dfs_cxt.pending_free_reader_list, reader);
    return reader;
}

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
DfsScanState *DFSBeginScan(Relation rel, List *splitList, int colNum, int16 *colIdx, Snapshot snapshot)
{
    DfsScanState *scanState = NULL;
    DfsScan *dfsScan = NULL;
    List *columnList = NIL;
    List *partList = NIL;
    List *privateList = NIL;
    TupleDesc tupDesc = rel->rd_att;
    bool isPartTbl = false;

    /* for value partition */
    if (RelationIsValuePartitioned(rel)) {
        isPartTbl = true;
        partList = ((ValuePartitionMap *)rel->partMap)->partList;
    }

    if (colIdx == NULL) {
        /* Scan all the columns by default. */
        columnList = CreateColList((Form_pg_attribute *)tupDesc->attrs, tupDesc->natts);
    } else {
        /* Scan the special list of columns. */
        columnList = CreateColList((Form_pg_attribute *)tupDesc->attrs, colNum, colIdx);
    }
    DfsPrivateItem *item = MakeDfsPrivateItem(columnList, columnList, NIL, NIL, NIL, NIL, NULL, tupDesc->natts,
                                              partList);
    privateList = list_make1(makeDefElem(DFS_PRIVATE_ITEM, (Node *)item));

    dfsScan = makeNode(DfsScan);
    Plan *plan = &dfsScan->plan;
    plan->targetlist = columnList;
    plan->qual = NIL;
    plan->lefttree = NULL;
    plan->righttree = NULL;
    plan->vec_output = true;
    dfsScan->isPartTbl = isPartTbl;
    dfsScan->scanrelid = 1;
    dfsScan->privateData = privateList;

    /* setup scan state */
    scanState = makeNode(DfsScanState);
    scanState->ss_currentRelation = rel;
    scanState->m_pScanBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, tupDesc);
    scanState->m_scanCxt = AllocSetContextCreate(CurrentMemoryContext, "Dfs Scan", ALLOCSET_DEFAULT_MINSIZE,
                                                 ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    scanState->ps.plan = (Plan *)dfsScan;

    /* Check if we need to scan the system columns. */
    if (colIdx != NULL) {
        List *sysAttrList = NIL;
        scanState->ps.ps_ProjInfo = makeNode(ProjectionInfo);

        for (int i = 0; i < colNum; ++i) {
            if (colIdx[i] < 0) {
                sysAttrList = lappend_int(sysAttrList, colIdx[i]);
            }
        }
        scanState->ps.ps_ProjInfo->pi_sysAttrList = sysAttrList;
        scanState->m_pScanBatch->CreateSysColContainer(CurrentMemoryContext, sysAttrList);
    }

    ReaderState *readerState = (ReaderState *)palloc0(sizeof(ReaderState));

    /* if splitList is NULL, call BuildSplitList() to provide the file list */
    if (splitList != NULL) {
        readerState->splitList = splitList;
    } else {
        readerState->splitList = BuildSplitList(rel, getDfsStorePath(rel), snapshot);
    }

    FillReaderState(readerState, scanState, item, snapshot);
    scanState->m_splitList = readerState->splitList;

    DfsSrvOptions *srvOptions = GetDfsSrvOptions(rel->rd_rel->reltablespace);
    DFSConnector *conn = createConnector(readerState->persistCtx, srvOptions, rel->rd_rel->reltablespace);

    Reader *fileReader = createOrcReader(readerState, conn, false);

    scanState->m_readerState = readerState;
    scanState->m_fileReader = fileReader;
    scanState->m_readerState->fileReader = fileReader;

    return scanState;
}

VectorBatch *DFSGetNextBatch(DfsScanState *scanState)
{
    if (scanState == NULL) {
        return NULL;
    }
    VectorBatch *vecBatch = scanState->m_pScanBatch;
    if (vecBatch == NULL) {
        return NULL;
    }
    vecBatch->Reset();
    if (scanState->m_fileReader == NULL) {
        return NULL;
    }
    MemoryContext context = scanState->m_scanCxt;
    MemoryContextReset(context);
    MemoryContext oldContext = MemoryContextSwitchTo(context);
    scanState->m_fileReader->nextBatch(vecBatch);
    (void)MemoryContextSwitchTo(oldContext);
    return vecBatch;
}

void DFSGetNextTuple(DfsScanState *scanState, TupleTableSlot *scanTupleSlot)
{
    Assert(scanTupleSlot != NULL);
    (void)ExecClearTuple(scanTupleSlot);

    if (scanState != NULL && scanState->m_fileReader) {
        scanState->m_fileReader->nextTuple(scanTupleSlot);
    }
}

void DFSEndScan(DfsScanState *scanState)
{
    if (scanState == NULL) {
        return;
    }

    if (scanState->m_scanCxt != NULL && scanState->m_scanCxt != CurrentMemoryContext) {
        MemoryContextDelete(scanState->m_scanCxt);
    }

    if (scanState->m_pScanBatch != NULL) {
        delete scanState->m_pScanBatch;
        scanState->m_pScanBatch = NULL;
    }

    if (scanState->m_fileReader != NULL) {
        RemoveDfsReadHandler(scanState->m_fileReader);
        DELETE_EX(scanState->m_fileReader);
    }

    pfree_ext(scanState->ps.plan);

    dfs::reader::ReaderState *readerState = scanState->m_readerState;
    if (readerState != NULL) {
        MemoryContextDelete(readerState->persistCtx);
        pfree_ext(readerState);
        scanState->m_readerState = NULL;
    }
}
}  // namespace reader

namespace writer {
class WriterImpl : public Writer {
public:
    WriterImpl(MemoryContext ctx, Relation relation, Relation DestRel, FileType type, const char *parsig,
               int transactionLevel);
    virtual ~WriterImpl();

    const char *getRelName() override;

    uint64 getFileID() override;
    void appendBatch(const VectorBatch *batch) override;
    const int getCurrentTransactionLevel() override;
    void init(IndexInsertInfo *indexInsertInfo) override;
    uint64 getSerializedRows() override;
    void Destroy() override;
    int64 closeCurWriter(char *filePath) override;
    void deltaInsert(int option) override;
    int spillToCurFile() override;
    int spillToNewFile(const char *filePath) override;
    void handleTail();
    uint64 getBufferRows() override;
    void setFileID(uint64 fileID) override;
    uint64 getTotalRows() override;
    bool canAppend(uint64 size) override;
    void appendTuple(Datum *values, bool *nulls) override;
    bool getMinMax(uint32 colId, char *&minstr, char *&maxstr, bool &hasMin, bool &hasMax) override;

private:
    void bindTransformFunc();

    /* the current relation */
    Relation m_relation;

    /* the dest relation(used for vacuum) */
    Relation m_DestRel;

    /* column writer */
    ColumnWriter *m_colWriter;

    /* function pointer for transform */
    transformScalarVector *m_transformScalarFunc;

    /* memory context used inside */
    MemoryContext m_ctx;

    /* the current file id */
    uint64 m_fileID;

    /* the format type */
    FileType m_type;

    /*
     * Writer for partitioned table ONLY, and also indicates how this partition is
     * defined, NULL if it is not for partition table.
     */
    const char *m_parsig;

    /* array of is null flags */
    bool *m_isNull;

    /* transaction level */
    int m_transactionLevel;
};

WriterImpl::WriterImpl(MemoryContext ctx, Relation relation, Relation DestRel, FileType type, const char *parsig,
                       int transactionLevel)
    : m_relation(relation),
      m_DestRel(DestRel),
      m_colWriter(NULL),
      m_transformScalarFunc(NULL),
      m_ctx(ctx),
      m_fileID(0),
      m_type(type),
      m_parsig(parsig),
      m_isNull(NULL),
      m_transactionLevel(transactionLevel)
{
}

WriterImpl::~WriterImpl()
{
    m_isNull = NULL;
    m_parsig = NULL;
    m_colWriter = NULL;
    m_relation = NULL;
    m_transformScalarFunc = NULL;
    m_ctx = NULL;
    m_DestRel = NULL;
}

void WriterImpl::init(IndexInsertInfo *indexInsertInfo)
{
#ifndef ENABLE_LITE_MODE
    /* Initialize the connector info. */
    Oid tbsOid = m_relation->rd_node.spcNode;
    DFSConnector *conn = createConnector(m_ctx, GetDfsSrvOptions(tbsOid), tbsOid);

    /* Initialize the column writer. */
    switch (m_type) {
        case ORC: {
            m_colWriter = createORCColWriter(m_ctx, m_relation, m_DestRel, indexInsertInfo, conn, m_parsig);
            break;
        }
        case PARQUET:
        default: {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                            errmsg("Only ORC is supported for now.")));
        }
    }
    Assert(m_colWriter != NULL);

    m_transformScalarFunc = (transformScalarVector *)palloc0(sizeof(transformScalarVector) * m_relation->rd_att->natts);
    m_isNull = (bool *)palloc0(sizeof(bool) * BatchMaxSize);
    bindTransformFunc();
#else
    FEATURE_ON_LITE_MODE_NOT_SUPPORTED();
#endif
}

const char *WriterImpl::getRelName()
{
    return RelationGetRelationName(m_relation);
}

void WriterImpl::bindTransformFunc()
{
    int cols = m_relation->rd_att->natts;
    Form_pg_attribute *attrs = m_relation->rd_att->attrs;
    for (int i = 0; i < cols; i++) {
        if (IS_DROPPED_COLUMN(m_relation->rd_att->attrs[i])) {
            /*
             * If the column is dropped, do not write data into file for
             * this column.
             */
            continue;
        }
        switch (attrs[i]->atttypid) {
            case BPCHAROID:
            case TEXTOID:
            case VARCHAROID:
                m_transformScalarFunc[i] = transformScalarVectorT<VARCHAROID>;
                break;
            case TIMETZOID:
            case TINTERVALOID:
            case INTERVALOID:
            case NAMEOID:
            case MACADDROID:
                m_transformScalarFunc[i] = transformScalarVectorT<TIMETZOID>;
                break;
            case UNKNOWNOID:
            case CSTRINGOID:
                m_transformScalarFunc[i] = transformScalarVectorT<UNKNOWNOID>;
                break;
            default:
                m_transformScalarFunc[i] = transformScalarVectorT<0>;
                break;
        }
    }
}

void WriterImpl::Destroy()
{
    if (m_colWriter != NULL) {
        DELETE_EX(m_colWriter);
    }
}

const int WriterImpl::getCurrentTransactionLevel()
{
    return m_transactionLevel;
}

void WriterImpl::appendBatch(const VectorBatch *batch)
{
    Assert(m_colWriter != NULL);
    ScalarVector *vecs = batch->m_arr;
    for (int i = 0; i < batch->m_cols; i++) {
        if (IS_DROPPED_COLUMN(m_relation->rd_att->attrs[i])) {
            /*
             * If the column is dropped, do not write data into file for
             * this column.
             */
            continue;
        }

        Datum *pDatum = m_transformScalarFunc[i](&vecs[i], m_isNull);

        m_colWriter->appendColDatum(i, pDatum, m_isNull, batch->m_rows);
    }
    m_colWriter->incrementNumber(batch->m_rows);
}

void WriterImpl::appendTuple(Datum *values, bool *nulls)
{
    Assert(m_colWriter != NULL);
    for (int i = 0; i < m_relation->rd_att->natts; i++) {
        if (IS_DROPPED_COLUMN(m_relation->rd_att->attrs[i])) {
            /*
             * If the column is dropped, do not write data into file for
             * this column.
             */
            continue;
        }
        /*
         * We only append the none-partitioning columns for value-partition table,
         * also the underlying storage format do not store partitioning coloumn,
         * so we need adjust the colid to index underlying ORCColWriter, which is
         * handled in lower ORCColWriter
         */
        m_colWriter->appendColDatum(i, &values[i], &nulls[i], 1);
    }
    m_colWriter->incrementNumber(1);
}

bool WriterImpl::canAppend(uint64 size)
{
    Assert(m_colWriter != NULL);
    return size <= m_colWriter->getBufferCapacity() - m_colWriter->getBufferRows();
}

int WriterImpl::spillToCurFile()
{
    Assert(m_colWriter != NULL);
    return m_colWriter->spill(m_fileID);
}

int WriterImpl::spillToNewFile(const char *filePath)
{
    Assert(m_colWriter != NULL);
    StandbyOrSecondaryIsAlive();
    m_colWriter->openNewFile(filePath);
    return m_colWriter->spill(m_fileID);
}

void WriterImpl::deltaInsert(int option)
{
    Assert(m_colWriter != NULL);
    m_colWriter->deltaInsert(option);
}

void WriterImpl::setFileID(uint64 fileID)
{
    m_fileID = fileID;
}

uint64 WriterImpl::getFileID()
{
    return m_fileID;
}

uint64 WriterImpl::getTotalRows()
{
    return m_colWriter->getTotalRows();
}

uint64 WriterImpl::getSerializedRows()
{
    return m_colWriter->getSerializedRows();
}

uint64 WriterImpl::getBufferRows()
{
    return m_colWriter->getBufferRows();
}

bool WriterImpl::getMinMax(uint32 colId, char *&minstr, char *&maxstr, bool &hasMin, bool &hasMax)
{
    return m_colWriter->getMinMax(colId, minstr, maxstr, hasMin, hasMax);
}

int64_t WriterImpl::closeCurWriter(char *filePath)
{
    m_colWriter->closeCurWriter();
    return m_colWriter->verifyFile(filePath);
}

void WriterImpl::handleTail()
{
    m_colWriter->handleTail();
}

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
                        const char *parsig)
{
    Writer *writer = New(context) WriterImpl(context, rel, DestRel, ORC, parsig, GetCurrentTransactionNestLevel());
    writer->init(indexInsertInfo);

    /*
     * Register the write handler to pending free list , the pending writer list
     * allocated in ModifyTable context
     */
    AutoContextSwitch newMemCnxt(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    t_thrd.dfs_cxt.pending_free_writer_list = lappend(t_thrd.dfs_cxt.pending_free_writer_list, writer);

    return writer;
}
}  // namespace writer
}  // namespace dfs
