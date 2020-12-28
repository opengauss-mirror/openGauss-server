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
 * parquet_file_reader.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/parquet/parquet_file_reader.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "parquet_file_reader.h"
#include "parquet_input_stream_adapter.h"

#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "access/dfs/dfs_common.h"
#include "access/dfs/dfs_query.h"
#include "optimizer/predtest.h"

namespace dfs {
namespace reader {
ParquetFileReader *ParquetFileReader::create(ReaderState *readerState, std::unique_ptr<GSInputStream> gsInputStream)
{
    std::unique_ptr<parquet::RandomAccessSource> source(
        new ParquetInputStreamAdapter(readerState->persistCtx, std::move(gsInputStream)));

    std::unique_ptr<parquet::ParquetFileReader> realParquetFileReader =
        parquet::ParquetFileReader::Open(std::move(source));

    auto p = New(readerState->persistCtx) ParquetFileReader(readerState, std::move(realParquetFileReader));
    p->begin();

    return p;
}

ParquetFileReader::ParquetFileReader(ReaderState *readerState,
                                     std::unique_ptr<parquet::ParquetFileReader> realParquetFileReader)
    : m_readerState(readerState),
      m_realParquetFileReader(std::move(realParquetFileReader)),
      m_fileMetaData(m_realParquetFileReader->metadata()),
      isSelected(NULL),
      m_num_rows(0),
      m_num_columns(0),
      m_num_row_groups(0),
      m_numberOfColumns(0),
      m_numberOfFields(0),
      m_currentRowGroupIndex(0),
      m_totalRowsInCurrentRowGroup(0),
      m_rowsReadInCurrentRowGroup(0),
      hasBloomFilter(false),
      bloomFilters(NULL)
{
}

ParquetFileReader::~ParquetFileReader()
{
    pfree_ext(isSelected);
    bloomFilters = NULL;
    m_readerState = NULL;
}

void ParquetFileReader::begin()
{
    m_num_rows = static_cast<uint64_t>(m_fileMetaData->num_rows());
    m_num_columns = static_cast<uint>(m_fileMetaData->num_columns());
    m_num_row_groups = static_cast<uint>(m_fileMetaData->num_row_groups());

    /* The number of columns stored in PARQUET file. */
    m_numberOfColumns = m_readerState->relAttrNum;
    m_numberOfFields = m_numberOfColumns - m_readerState->partNum;
    m_columnReaders.resize(static_cast<int32_t>(m_numberOfFields));

    MemoryContext oldContext = MemoryContextSwitchTo(m_readerState->persistCtx);
    isSelected = (bool *)palloc0(sizeof(bool) * BatchMaxSize);
    (void)MemoryContextSwitchTo(oldContext);
}

void ParquetFileReader::Destroy()
{
    /* Clean isSelected flag array. */
    if (isSelected != NULL) {
        pfree(isSelected);
        isSelected = NULL;
    }

    resetColumnReaders();

    m_realParquetFileReader.reset(NULL);
}

uint64_t ParquetFileReader::getNumRows() const
{
    return m_num_rows;
}

uint64_t ParquetFileReader::getNumColumns() const
{
    return m_num_columns;
}

uint64_t ParquetFileReader::getNumRowGroups() const
{
    return m_num_row_groups;
}

uint64_t ParquetFileReader::getNumFields() const
{
    return static_cast<uint64_t>(m_numberOfFields);
}

bool ParquetFileReader::isCurrentRowGroupValid() const
{
    return m_currentRowGroupIndex < m_num_row_groups;
}

int64_t ParquetFileReader::readRows(VectorBatch *batch, uint64_t maxRowsReadOnce, uint64_t curRowsInFile,
                                    uint64_t &rowsInFile, uint64_t &rowsSkip, uint64_t &rowsCross,
                                    FileOffset *fileOffset)
{
    if (tryToSkipCurrentRowGroup(rowsSkip, rowsCross)) {
        return rowsCross;
    }

    createRowGroupReaderWhenNecessary();
    rowsInFile = curRowsInFile + rowsCross;

    uint64_t numRowsToRead = calculateRowsToRead(maxRowsReadOnce);
    if (!readAndFilter(rowsSkip, numRowsToRead)) {
        fillVectorBatch(batch, numRowsToRead, fileOffset, rowsInFile);
    }

    trySwitchToNextRowGroup(numRowsToRead);

    rowsCross += numRowsToRead;
    return numRowsToRead;
}

void ParquetFileReader::initializeColumnReaders(GSInputStream *gsInputStream)
{
    uint32_t *mppColIDs = m_readerState->globalColIds;

    for (uint32_t i = 0; i < m_readerState->relAttrNum; i++) {
        if (!isColumnRequiredToRead(i)) {
            continue;
        }

        std::unique_ptr<dfs::GSInputStream> gsInputStreamCopy;
        DFS_TRY()
        {
            gsInputStreamCopy = gsInputStream->copy();
        }
        DFS_CATCH();
        DFS_ERRREPORT_WITHARGS(
            "Error occurs while opening hdfs file %s because of %s, "
            "detail can be found in dn log of %s.",
            MOD_PARQUET, m_readerState->currentSplit->filePath);

        const Var *var = GetVarFromColumnList(m_readerState->allColumnList, static_cast<int32_t>(i + 1));

        auto columnDescriptor = m_fileMetaData->schema()->Column(columnIndexInFile(i));
        auto columnReader = createParquetColumnReader(columnDescriptor, columnIndexInFile(i), mppColIDs[i] + 1,
                                                      m_readerState, var);
        columnReader->begin(std::move(gsInputStreamCopy), m_fileMetaData);

        m_columnReaders[columnIndexInFile(i)] = columnReader;
    }
}

void ParquetFileReader::resetColumnReaders()
{
    /* Reset the array of column reader. */
    for (uint32_t i = 0; i < (uint32_t)m_columnReaders.size(); i++) {
        if (m_columnReaders[i] != NULL) {
            DELETE_EX(m_columnReaders[i]);
        }
    }
}

void ParquetFileReader::setRowGroupIndexForColumnReaders()
{
    for (uint32_t i = 0; i < numberOfColumns(); i++) {
        if (isColumnRequiredToRead(i)) {
            m_columnReaders[columnIndexInFile(i)]->setRowGroupIndex(m_currentRowGroupIndex);
        }
    }
}

void ParquetFileReader::createRowGroupReaderWhenNecessary()
{
    if ((m_groupMetaData == NULL) && (isCurrentRowGroupValid())) {
        m_groupMetaData = m_fileMetaData->RowGroup(m_currentRowGroupIndex);
        m_totalRowsInCurrentRowGroup = m_groupMetaData->num_rows();
        m_readerState->orcMetaLoadBlockCount++;

        setRowGroupIndexForColumnReaders();
    }
}

bool ParquetFileReader::tryToSkipCurrentRowGroup(uint64_t &rowsSkip, uint64_t &rowsCross)
{
    if (isStartOfCurrentRowGroup()) {
        if (hasRestriction() && !checkPredicateOnCurrentRowGroup()) {
            skipCurrentRowGroup(rowsSkip, rowsCross);
            return true;
        }
    }

    return false;
}

const bool ParquetFileReader::isStartOfCurrentRowGroup() const
{
    return m_rowsReadInCurrentRowGroup == 0;
}

bool ParquetFileReader::checkPredicateOnCurrentRowGroup()
{
    List *restrictions = buildRestriction(ROW_GROUP, m_currentRowGroupIndex);
    bool ret = (!predicate_refuted_by(restrictions, m_readerState->queryRestrictionList, false) &&
                !predicate_refuted_by(restrictions, m_readerState->runtimeRestrictionList, true));
    if (!ret) {
        /* these rows will be skipped, so count them */
        m_readerState->minmaxFilterRows += m_totalRowsInCurrentRowGroup;
        /* this stripe will be skipped, so count it */
        m_readerState->minmaxFilterStripe++;
    }
    m_readerState->minmaxCheckStripe++;
    list_free_ext(restrictions);
    return ret && checkBloomFilter();
}

bool ParquetFileReader::checkBloomFilter() const
{
    if (!hasBloomFilter) {
        return true;
    }

    uint32_t *orcColIDs = m_readerState->readColIDs;
    bool *readRequired = m_readerState->readRequired;

    for (uint32_t i = 0; i < m_numberOfColumns; i++) {
        if (NULL != bloomFilters[i]) {
            /*
             * Here, we may skip a case where a column with bloomfilter assigned
             * but marked as 'None-Required', for example a partitioned column.
             *
             * For this kind of BF-filtering, we handle partition column at ORC
             * file level rather than each stride.
             */
            if (readRequired[i] && !m_columnReaders[orcColIDs[i]]->checkBloomFilter(m_currentRowGroupIndex)) {
                ++(m_readerState->bloomFilterBlocks);
                m_readerState->bloomFilterRows += m_totalRowsInCurrentRowGroup - m_rowsReadInCurrentRowGroup;
                return false;
            }
        }
    }

    return true;
}

List *ParquetFileReader::buildRestriction(RestrictionType type, uint64_t rowGroupIndex)
{
    List *fileRestrictionList = NULL;
    bool *restrictRequired = m_readerState->restrictRequired;
    Assert(rowGroupIndex < m_num_row_groups);

    uint64_t numberOfColumns = m_readerState->relAttrNum;
    for (uint32_t i = 0; i < numberOfColumns; i++) {
        Node *baseRestriction = NULL;

        if (restrictRequired[i] && isColumnRequiredToRead(i)) {
            baseRestriction = m_columnReaders[columnIndexInFile(i)]->buildColRestriction(type,
                                                                                         m_realParquetFileReader.get(),
                                                                                         rowGroupIndex);
            if (baseRestriction != NULL) {
                fileRestrictionList = lappend(fileRestrictionList, baseRestriction);
            }
        }
    }

    return fileRestrictionList;
}

uint64_t ParquetFileReader::calculateRowsToRead(uint64_t maxRowsReadOnce) const
{
    uint64_t rowsLeftInCurrentRowGroup = m_totalRowsInCurrentRowGroup - m_rowsReadInCurrentRowGroup;
    return Min(rowsLeftInCurrentRowGroup, maxRowsReadOnce);
}

bool ParquetFileReader::readAndFilter(uint64_t rowsSkip, uint64_t numRowsToRead)
{
    uint32_t mppColID = 0;
    uint32_t fileColID = 0;

    bool skipBatch = false;
    bool checkSkipBatch = false;

    errno_t rc = memset_s(isSelected, sizeof(bool) * BatchMaxSize, (int)(true), numRowsToRead);
    securec_check(rc, "\0", "\0");

    for (uint32_t i = 0; i < m_readerState->relAttrNum; i++) {
        mppColID = m_readerState->orderedCols[i];

        if (isColumnRequiredToRead(mppColID)) {
            fileColID = columnIndexInFile(mppColID);

            /* Read or skip the column batch rows. */
            skipBatch = skipBatch ? true : (checkSkipBatch ? canSkipBatch(numRowsToRead) : false);
            if (skipBatch) {
                m_columnReaders[fileColID]->skip(numRowsToRead);
            } else {
                m_columnReaders[fileColID]->nextInternal(numRowsToRead);
            }

            /* Filter the column batch with the predicate. */
            checkSkipBatch = false;
            if (!skipBatch && m_columnReaders[fileColID]->hasPredicate()) {
                m_columnReaders[fileColID]->predicateFilter(numRowsToRead, isSelected);
                checkSkipBatch = true;
            }
        }
    }

    /* Check if the batch can be skipped in the end. */
    skipBatch = skipBatch ? true : (checkSkipBatch ? canSkipBatch(numRowsToRead) : false);

    return skipBatch;
}

void ParquetFileReader::fillVectorBatch(VectorBatch *batch, uint64_t numRowsToRead, FileOffset *fileOffset,
                                        uint64_t rowsInFile)
{
    uint32_t mppColID = 0;
    uint32_t selectedRows = 0;
    bool *readRequired = m_readerState->readRequired;
    bool *targetRequired = m_readerState->targetRequired;

    /* Set the m_rows according to the isSelected array. */
    for (uint32_t i = 0; i < numRowsToRead; i++) {
        if (isSelected[i]) {
            selectedRows++;
        }
    }

    batch->m_rows += selectedRows;

    for (uint32_t i = 0; i < m_readerState->relAttrNum; i++) {
        mppColID = m_readerState->orderedCols[i];

        if (readRequired[mppColID] && targetRequired[mppColID]) {
            ScalarVector *scalorVector = &batch->m_arr[mppColID];
            (void)m_columnReaders[columnIndexInFile(mppColID)]->fillScalarVector(numRowsToRead, isSelected,
                                                                                 scalorVector);
        }
    }

    /* Fill file offset array. */
    if (fileOffset != NULL) {
        fillFileOffset(rowsInFile, numRowsToRead, fileOffset);
    }
}

void ParquetFileReader::fillFileOffset(uint64_t rowsInFile, uint64_t rowsToRead, FileOffset *fileOffset) const
{
    uint32_t *offset = fileOffset->rowIndexInFile;
    uint32_t start = fileOffset->numelements;
    Assert(start < BatchMaxSize);

    for (uint64_t i = 0; i < rowsToRead; i++) {
        if (isSelected[i]) {
            offset[start++] = rowsInFile + i + 1;
        }
    }

    Assert(start <= BatchMaxSize);
    fileOffset->numelements = start;
}

void ParquetFileReader::setColBloomFilter(filter::BloomFilter **_bloomFilters)
{
    hasBloomFilter = true;
    this->bloomFilters = _bloomFilters;

    for (uint32_t i = 0; i < m_numberOfColumns; i++) {
        filter::BloomFilter *bloomFilter = this->bloomFilters[i];
        if (bloomFilter != NULL) {
            if (m_readerState->readRequired[i]) {
                uint32_t orcColID = m_readerState->readColIDs[i];
                m_columnReaders[orcColID]->setBloomFilter(bloomFilter);
            }
        }
    }
}

void ParquetFileReader::skipCurrentRowGroup(uint64_t &rowsSkip, uint64_t &rowsCross)
{
    auto groupMetaData = m_fileMetaData->RowGroup((int32_t)m_currentRowGroupIndex);
    rowsSkip += (uint64_t)groupMetaData->num_rows();
    rowsCross += (uint64_t)groupMetaData->num_rows();

    switchToNextRowGroup();
}

void ParquetFileReader::trySwitchToNextRowGroup(uint64_t numRowsRead)
{
    m_rowsReadInCurrentRowGroup += numRowsRead;
    Assert(m_rowsReadInCurrentRowGroup <= m_totalRowsInCurrentRowGroup);

    if (m_rowsReadInCurrentRowGroup == m_totalRowsInCurrentRowGroup) {
        switchToNextRowGroup();
    }
}

const bool ParquetFileReader::hasRestriction() const
{
    return m_readerState->queryRestrictionList != NULL;
}
}  // namespace reader
}  // namespace dfs
