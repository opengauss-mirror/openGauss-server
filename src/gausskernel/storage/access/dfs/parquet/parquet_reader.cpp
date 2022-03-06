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
 * -------------------------------------------------------------------------
 *
 * parquet_reader.cpp
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/parquet/parquet_reader.cpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef ENABLE_LITE_MODE
#include "parquet/platform.h"
#include "parquet/types.h"
#endif

#include "parquet_reader.h"
#include "parquet_file_reader.h"

#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/memutils.h"
#include "optimizer/predtest.h"
#include "access/dfs/dfs_common.h"
#include "access/dfs/dfs_query.h"
#include "access/dfs/dfs_stream_factory.h"

namespace dfs {
namespace reader {
// ParquetReaderImpl
class ParquetReaderImpl {
public:
    ParquetReaderImpl(ReaderState *readerState, dfs::DFSConnector *conn);
    ~ParquetReaderImpl();

    void begin();
    void end();
    void Destroy();
    bool loadFile(char *file_path, List *file_restriction);
    uint64_t nextBatch(VectorBatch *batch, uint64_t cur_rows_in_file, DFSDesc *dfs_desc, FileOffset *file_offset);
    void nextBatchByContinueTids(const uint64_t rowsSkip, const uint64_t rowsToRead, const VectorBatch *batch,
                                 const uint64_t startOffset, const DFSDesc *dfsDesc,
                                 const FileOffset *fileOffset) const;
    void addBloomFilter(filter::BloomFilter *bloomFilter, int colIdx, bool is_runtime);
    void copyBloomFilter();
    bool checkBFPruning(uint32_t colID, char *colValue);
    const uint64_t getNumberOfRows() const;
    void setBatchCapacity(uint64_t capacity) const;
    bool fileEOF(uint64_t rows_read_in_file) const;

private:
    void collectFileReadingStatus();
    void fixOrderedColumnList(uint32_t colIdx);
    void buildStructReader();
    /* For foreign table, remove the non-exist columns in file. */
    void setNonExistColumns();
    void resetParquetFileReader();

    ReaderState *m_readerState;
    dfs::DFSConnector *m_conn;
    char *m_currentFilePath;
    uint64_t m_numberOfRows;
    uint64_t m_numberOfColumns;

    bool hasBloomFilter;
    filter::BloomFilter **bloomFilters;
    filter::BloomFilter **staticBloomFilters;

    dfs::GSInputStream *m_gsInputStream;
    uint64_t m_maxRowsReadOnce;

    ParquetFileReader *m_fileReader;
    MemoryContext m_internalContext;
};

// ParquetReaderImpl
ParquetReaderImpl::ParquetReaderImpl(ReaderState *readerState, dfs::DFSConnector *conn)
    : m_readerState(readerState),
      m_conn(conn),
      m_currentFilePath(NULL),
      m_numberOfRows(0),
      m_numberOfColumns(0),
      hasBloomFilter(false),
      bloomFilters(NULL),
      staticBloomFilters(NULL),
      m_gsInputStream(NULL),
      m_maxRowsReadOnce(0),
      m_fileReader(NULL),
      m_internalContext(NULL)
{
}

ParquetReaderImpl::~ParquetReaderImpl()
{
    pfree_ext(bloomFilters);
    pfree_ext(staticBloomFilters);
    /*
     * The conn is not created in this class but we need to free it here,
     * because once the conn is transfered into then the owner of it is changed.
     */
    if (m_conn != NULL) {
        delete (m_conn);
        m_conn = NULL;
    }
    m_gsInputStream = NULL;
    m_internalContext = NULL;
    m_currentFilePath = NULL;
    m_readerState = NULL;
    m_fileReader = NULL;
}

void ParquetReaderImpl::begin()
{
    m_maxRowsReadOnce = static_cast<uint64_t>(BatchMaxSize * 0.5);
    m_internalContext =
        AllocSetContextCreate(m_readerState->persistCtx, "Internal batch reader level context for parquet dfs reader",
                              ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    /* The number of columns stored in PARQUET file. */
    m_numberOfColumns = m_readerState->relAttrNum;

    MemoryContext oldContext = MemoryContextSwitchTo(m_readerState->persistCtx);
    bloomFilters = (filter::BloomFilter **)palloc0(sizeof(filter::BloomFilter *) * m_numberOfColumns);
    staticBloomFilters = (filter::BloomFilter **)palloc0(sizeof(filter::BloomFilter *) * m_numberOfColumns);
    (void)MemoryContextSwitchTo(oldContext);
}

void ParquetReaderImpl::end()
{
    /* collect the statictics for the last reading */
    collectFileReadingStatus();
}

void ParquetReaderImpl::Destroy()
{
    /* Clean bloom filter. */
    pfree(bloomFilters);
    pfree(staticBloomFilters);
    bloomFilters = NULL;
    staticBloomFilters = NULL;

    /* Reset Parquet file reader and column readers within it */
    resetParquetFileReader();

    /*
     * The conn is not created in this class but we need to free it here,
     * because once the conn is transfered into then the owner of it is changed.
     */
    if (m_conn != NULL) {
        delete (m_conn);
        m_conn = NULL;
    }

    /* Clear the internal memory context. */
    if (m_internalContext != NULL && m_internalContext != CurrentMemoryContext) {
        MemoryContextDelete(m_internalContext);
    }
}

bool ParquetReaderImpl::loadFile(char *file_path, List *fileRestriction)
{
    /* the total number of acquiring loading files */
    m_readerState->minmaxCheckFiles++;

    /*
     * Before open the file, check if the file can be skipped according to
     * min/max information stored in desc table if exist.
     */
    if (fileRestriction != NIL &&
        (predicate_refuted_by(fileRestriction, m_readerState->queryRestrictionList, false) ||
         predicate_refuted_by(fileRestriction, m_readerState->runtimeRestrictionList, true))) {
        /* filter by min/max info in desc table */
        m_readerState->minmaxFilterFiles++;
        return false;
    }

    m_currentFilePath = file_path;

    /* Collect the statictics of the previous reading. */
    collectFileReadingStatus();

    /* Reset Parquet file reader and column readers within it */
    resetParquetFileReader();

    /* Construct the struct reader to read the meta data. */
    buildStructReader();  // open file here

    if (m_fileReader->getNumRows() == 0 || m_fileReader->getNumRowGroups() == 0) {
        return false;
    }

    /* Initlize the column readers. */
    m_fileReader->initializeColumnReaders(m_gsInputStream);

    /* Transfer the bloom filter informations to the required columns.  */
    if (hasBloomFilter) {
        m_fileReader->setColBloomFilter(bloomFilters);
    }

    return true;
}

uint64_t ParquetReaderImpl::nextBatch(VectorBatch *batch, uint64_t cur_rows_in_file, DFSDesc *dfs_desc,
                                      FileOffset *file_offset)
{
    uint64_t rowsInFile = 0;
    uint64_t rowsCross = 0;
    uint64_t rowsSkip = 0;

    MemoryContextReset(m_internalContext);
    AutoContextSwitch newMemCnxt(m_internalContext);

    while (m_fileReader->isCurrentRowGroupValid() && (batch->m_rows + m_maxRowsReadOnce <= BatchMaxSize)) {
        (void)m_fileReader->readRows(batch, m_maxRowsReadOnce, cur_rows_in_file, rowsInFile, rowsSkip, rowsCross,
                                     file_offset);
    }

    return rowsCross;
}

void ParquetReaderImpl::nextBatchByContinueTids(const uint64_t rowsSkip, const uint64_t rowsToRead,
                                                const VectorBatch *batch, const uint64_t startOffset,
                                                const DFSDesc *dfsDesc, const FileOffset *fileOffset) const
{
}

void ParquetReaderImpl::addBloomFilter(filter::BloomFilter *_bloomFilter, int colIdx, bool is_runtime)
{
    if (_bloomFilter == NULL) {
        return;
    }

    Var *var = GetVarFromColumnList(m_readerState->allColumnList, colIdx + 1);

    /* BF only work when the type match and for bpchar type, typemod must be the same too. */
    if ((NULL != var && _bloomFilter->getDataType() == var->vartype) &&
        ((var->vartype != BPCHAROID) || (var->vartypmod == _bloomFilter->getTypeMod()))) {
        hasBloomFilter = true;

        if (!is_runtime) {
            /* In the init time, we store the static bloom filter. */
            hasBloomFilter = true;
            if (staticBloomFilters[static_cast<uint32_t>(colIdx)] == NULL) {
                staticBloomFilters[static_cast<uint32_t>(colIdx)] = _bloomFilter;
            } else {
                staticBloomFilters[static_cast<uint32_t>(colIdx)]->intersectAll(*_bloomFilter);
            }
            bloomFilters[static_cast<uint32_t>(colIdx)] = staticBloomFilters[static_cast<uint32_t>(colIdx)];
        } else {
            /*
             * In the run time, we need to combine the static bloom filter and dynamic one.
             * And the bloomFilters have already copy to runTimeBloomFilters in BuildRunTimePredicates().
             */
            if (bloomFilters[static_cast<uint32_t>(colIdx)] == staticBloomFilters[static_cast<uint32_t>(colIdx)]) {
                bloomFilters[static_cast<uint32_t>(colIdx)] = _bloomFilter;
                if (staticBloomFilters[static_cast<uint32_t>(colIdx)] != NULL)
                    bloomFilters[static_cast<uint32_t>(colIdx)]->intersectAll(
                        *staticBloomFilters[static_cast<uint32_t>(colIdx)]);
            } else {
                if (bloomFilters[static_cast<uint32_t>(colIdx)] != NULL)
                    bloomFilters[static_cast<uint32_t>(colIdx)]->intersectAll(*_bloomFilter);
            }
        }

        /*
         * For bloom filter generated from predicate like "a = 5", we need not to add restrictions.
         * Here only works when hashjoin qual is pushed down.
         */
        if (_bloomFilter->getType() != EQUAL_BLOOM_FILTER && _bloomFilter->hasMinMax()) {
            Node *minMax = MakeBaseConstraint(var, _bloomFilter->getMin(), _bloomFilter->getMax(),
                                              _bloomFilter->hasMinMax(), _bloomFilter->hasMinMax());
            if (minMax != NULL) {
                m_readerState->runtimeRestrictionList = lappend(m_readerState->runtimeRestrictionList, minMax);
            }
            m_readerState->restrictRequired[colIdx] = true;

            if (_bloomFilter->getType() == HASHJOIN_BLOOM_FILTER) {
                fixOrderedColumnList((uint32_t)(colIdx));
            }
        }
    }
}

void ParquetReaderImpl::copyBloomFilter()
{
    for (uint64_t i = 0; i < m_numberOfColumns; i++) {
        bloomFilters[static_cast<uint32_t>(i)] = staticBloomFilters[static_cast<uint32_t>(i)];
    }
}

bool ParquetReaderImpl::checkBFPruning(uint32_t colID, char *colValue)
{
    if (bloomFilters[colID]) {
        Var *var = GetVarFromColumnList(m_readerState->allColumnList, (int)(colID + 1));
        if (NULL == var || 0 == strncmp(colValue, DEFAULT_HIVE_NULL, DEFAULT_HIVE_NULL_LENGTH)) {
            return false;
        } else {
            Datum datumValue = GetDatumFromString(var->vartype, var->vartypmod, colValue);
            if (!bloomFilters[colID]->includeDatum(datumValue)) {
                m_readerState->dynamicPrunFiles++;
                return true;
            }
        }
    }

    return false;
}

const uint64_t ParquetReaderImpl::getNumberOfRows() const
{
    return m_numberOfRows;
}

void ParquetReaderImpl::setBatchCapacity(const uint64_t capacity) const
{
}

bool ParquetReaderImpl::fileEOF(uint64_t rows_read_in_file) const
{
    return rows_read_in_file >= getNumberOfRows();
}

void ParquetReaderImpl::collectFileReadingStatus()
{
    if (m_gsInputStream == NULL) {
        return;
    }

    uint64_t local = 0;
    uint64_t remote = 0;
    uint64_t nnCalls = 0;
    uint64_t dnCalls = 0;

    m_gsInputStream->getStat(&local, &remote, &nnCalls, &dnCalls);
    if (remote > 0) {
        m_readerState->remoteBlock++;
    } else {
        m_readerState->localBlock++;
    }

    m_readerState->nnCalls += nnCalls;
    m_readerState->dnCalls += dnCalls;
}

void ParquetReaderImpl::fixOrderedColumnList(uint32_t colIdx)
{
    uint32_t bound = 0;
    uint32_t *orderedCols = m_readerState->orderedCols;
    bool *restrictRequired = m_readerState->restrictRequired;

    for (bound = 0; bound < m_numberOfColumns; bound++) {
        if (orderedCols[bound] == colIdx) {
            break;
        }
    }
    for (uint32_t i = 0; i < bound; i++) {
        if (!restrictRequired[orderedCols[i]]) {
            orderedCols[bound] = orderedCols[i];
            orderedCols[i] = colIdx;
            break;
        }
    }
}

void ParquetReaderImpl::buildStructReader()
{
    /* Start to load the file. */
    DFS_TRY()
    {
        std::unique_ptr<dfs::GSInputStream> gsInputStream = dfs::InputStreamFactory(m_conn, m_currentFilePath,
                                                                                    m_readerState, false);
        m_gsInputStream = gsInputStream.get();

        m_fileReader = ParquetFileReader::create(m_readerState, std::move(gsInputStream));
    }
    DFS_CATCH();

    DFS_ERRREPORT_WITHARGS(
        "Error occurs while creating an parquet reader for file %s because of %s, "
        "detail can be found in dn log of %s.",
        MOD_PARQUET, m_currentFilePath);

    Assert(m_fileReader != NULL);

    /* check the number of rows first */
    m_numberOfRows = m_fileReader->getNumRows();
    if (getNumberOfRows() == 0 || m_fileReader->getNumRowGroups() == 0) {
        return;
    }

    /*
     * For foreign table, the number of columns in the file can be bigger or less than the relation defined.
     */
    if (RelationIsForeignTable(m_readerState->scanstate->ss_currentRelation)) {
        setNonExistColumns();
    }
}

void ParquetReaderImpl::setNonExistColumns()
{
    bool *readRequired = m_readerState->readRequired;
    List **predicateArr = m_readerState->hdfsScanPredicateArr;
    for (uint32_t i = 0; i < m_readerState->relAttrNum; i++) {
        if (readRequired[i] && (m_readerState->readColIDs[i] + 1 > m_fileReader->getNumColumns())) {
            /* modify here, and will impact dfs_am layer. */
            readRequired[i] = false;

            uint32_t arrayIndex = m_readerState->globalColIds[i];
            if (predicateArr[arrayIndex] != NULL) {
                bool isNull = true;
                ScanState *scanState = m_readerState->scanstate;
                Var *var = GetVarFromColumnList(m_readerState->allColumnList, (int)(i + 1));
                Datum result = heapGetInitDefVal((int)(i + 1), scanState->ss_currentRelation->rd_att, &isNull);
                if (NULL == var || false == defColSatisfyPredicates(isNull, result, var, predicateArr[arrayIndex])) {
                    m_numberOfRows = 0;
                    break;
                }
            }
        }
    }
}

void ParquetReaderImpl::resetParquetFileReader()
{
    if (m_fileReader != NULL) {
        DELETE_EX(m_fileReader);
    }
}

// ParquetReader
ParquetReader::ParquetReader(ReaderState *readerState, dfs::DFSConnector *conn)
    : m_pImpl(NULL), m_readerState(readerState), m_conn(conn)
{
}

ParquetReader::~ParquetReader()
{
    if (m_pImpl != NULL) {
        delete m_pImpl;
        m_pImpl = NULL;
    }
    m_conn = NULL;
    m_readerState = NULL;
}

void ParquetReader::begin()
{
    m_pImpl = new ParquetReaderImpl(m_readerState, m_conn);
    m_pImpl->begin();
}

void ParquetReader::end()
{
    m_pImpl->end();
}

void ParquetReader::Destroy()
{
    m_pImpl->Destroy();
}

bool ParquetReader::loadFile(char *file_path, List *file_restriction)
{
    return m_pImpl->loadFile(file_path, file_restriction);
}

uint64_t ParquetReader::nextBatch(VectorBatch *batch, uint64_t cur_rows_in_file, DFSDesc *dfs_desc,
                                  FileOffset *file_offset)
{
    return m_pImpl->nextBatch(batch, cur_rows_in_file, dfs_desc, file_offset);
}

void ParquetReader::nextBatchByContinueTids(uint64_t rowsSkip, uint64_t rowsToRead, VectorBatch *batch,
                                            uint64_t startOffset, DFSDesc *dfsDesc, FileOffset *fileOffset)
{
    m_pImpl->nextBatchByContinueTids(rowsSkip, rowsToRead, batch, startOffset, dfsDesc, fileOffset);
}

void ParquetReader::addBloomFilter(filter::BloomFilter *bloomFilter, int colIdx, bool is_runtime)
{
    m_pImpl->addBloomFilter(bloomFilter, colIdx, is_runtime);
}

void ParquetReader::copyBloomFilter()
{
    m_pImpl->copyBloomFilter();
}

bool ParquetReader::checkBFPruning(uint32_t colID, char *colValue)
{
    return m_pImpl->checkBFPruning(colID, colValue);
}

uint64_t ParquetReader::getNumberOfRows()
{
    return m_pImpl->getNumberOfRows();
}

void ParquetReader::setBatchCapacity(uint64_t capacity)
{
    m_pImpl->setBatchCapacity(capacity);
}

bool ParquetReader::fileEOF(uint64_t rows_read_in_file)
{
    return m_pImpl->fileEOF(rows_read_in_file);
}
}  // namespace reader
}  // namespace dfs
