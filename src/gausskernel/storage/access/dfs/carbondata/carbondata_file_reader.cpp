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
 * carbondata_file_reader.cpp
 *
 *
 * IDENTIFICATION
 *         src/gausskernel/storage/access/dfs/carbondata/carbondata_file_reader.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "access/dfs/dfs_common.h"
#include "access/dfs/dfs_query.h"

#include "carbondata_file_reader.h"
#include "carbondata_stream_adapter.h"

#include "optimizer/predtest.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

namespace dfs {
namespace reader {
CarbondataFileReader *CarbondataFileReader::create(ReaderState *readerState,
                                                   std::unique_ptr<GSInputStream> gsInputStream)
{
    std::unique_ptr<carbondata::InputStream> source(
        new carbondata::CarbondataInputStreamAdapter(std::move(gsInputStream)));

    std::unique_ptr<carbondata::CarbondataFileReader> realCarbondataFileReader(
        new carbondata::CarbondataFileReader(std::move(source)));

    auto p = New(readerState->persistCtx) CarbondataFileReader(readerState, std::move(realCarbondataFileReader));

    return p;
}

CarbondataFileReader::CarbondataFileReader(ReaderState *readerState,
                                           std::unique_ptr<carbondata::CarbondataFileReader> realCarbondataFileReader)
    : m_readerState(readerState),
      m_realCarbondataFileReader(std::move(realCarbondataFileReader)),
      m_blockletInfo3(NULL),
      isSelected(NULL),
      m_schemaOrdinalList(NULL),
      m_num_rows(0),
      m_num_columns(0),
      m_num_blocklets(0),
      m_numberOfColumns(0),
      m_numberOfFields(0),
      m_currentBlockletIndex(0),
      m_totalRowsInCurrentBlocklet(0),
      m_rowsReadInCurrentBlocklet(0),
      m_currentPageIndex(0),
      m_totalPagesInCurrentBlocklet(0),
      m_rowsReadInCurrentPage(0),
      m_totalRowsInCurrentPage(0),
      m_hasRowCountInPage(false)
{
}

CarbondataFileReader::~CarbondataFileReader()
{
}

uint64_t CarbondataFileReader::getFooterSize()
{
    return m_realCarbondataFileReader->FooterSize();
}

void CarbondataFileReader::getFooterCache(unsigned char *footerCache)
{
    m_realCarbondataFileReader->FooterCache(footerCache);
}

uint64_t CarbondataFileReader::getHeaderSize()
{
    return m_realCarbondataFileReader->HeaderSize();
}

void CarbondataFileReader::getHeaderCache(unsigned char *headerCache)
{
    m_realCarbondataFileReader->HeaderCache(headerCache);
}

void CarbondataFileReader::setFileMetaData(const unsigned char *footer, const unsigned char *header,
                                           uint64_t footerSize, uint64_t headerSize)
{
    uint64_t fileSize = m_realCarbondataFileReader->GetFileSize();
    m_realCarbondataFileReader->SetFileMetaData(footer, header, footerSize, headerSize, (fileSize - footerSize));
}

void CarbondataFileReader::begin()
{
    m_fileMetaData = m_realCarbondataFileReader->FileMetaData();
    m_num_rows = static_cast<uint64_t>(m_fileMetaData.m_numOfRows);
    m_num_columns = static_cast<uint64_t>(m_fileMetaData.m_numOfColumns);
    m_num_blocklets = static_cast<uint64_t>(m_fileMetaData.m_blockletInfoList3.size());

    /* The number of columns stored in CARBONDATA file. */
    m_numberOfColumns = m_readerState->relAttrNum;
    m_numberOfFields = m_numberOfColumns - m_readerState->partNum;
    m_columnReaders.resize(static_cast<int32_t>(m_numberOfFields));

    MemoryContext oldContext = MemoryContextSwitchTo(m_readerState->persistCtx);
    isSelected = (bool *)palloc0(sizeof(bool) * BatchMaxSize);
    initSchemaOrdinalList();
    (void)MemoryContextSwitchTo(oldContext);
}

void CarbondataFileReader::Destroy()
{
    if (NULL != isSelected) {
        pfree(isSelected);
        isSelected = NULL;
    }

    destroyColumnReaders();

    m_realCarbondataFileReader.reset(NULL);
}

uint64_t CarbondataFileReader::getNumRows() const
{
    return m_num_rows;
}

uint64_t CarbondataFileReader::getNumColumns() const
{
    return m_num_columns;
}

uint64_t CarbondataFileReader::getNumBlocklets() const
{
    return m_num_blocklets;
}

uint64_t CarbondataFileReader::getNumFields() const
{
    return static_cast<uint64_t>(m_numberOfFields);
}

bool CarbondataFileReader::isCurrentBlockletValid() const
{
    return m_currentBlockletIndex < m_num_blocklets;
}

bool CarbondataFileReader::isCurrentPageValid() const
{
    return m_currentPageIndex < m_totalPagesInCurrentBlocklet;
}

int64_t CarbondataFileReader::readRows(VectorBatch *batch, uint64_t maxRowsReadOnce, uint64_t curRowsInFile,
                                       uint64_t &rowsInFile, uint64_t &rowsSkip, uint64_t &rowsCross,
                                       FileOffset *fileOffset)
{
    /* try to skip blocklet */
    if ((m_rowsReadInCurrentBlocklet == 0) && isCurrentBlockletValid()) {
        m_blockletInfo3 = &m_fileMetaData.m_blockletInfoList3[m_currentBlockletIndex];
        m_totalRowsInCurrentBlocklet = m_blockletInfo3->num_rows;
        m_totalPagesInCurrentBlocklet = m_blockletInfo3->number_number_of_pages;

        if (m_blockletInfo3->__isset.row_count_in_page &&
            m_totalPagesInCurrentBlocklet == m_blockletInfo3->row_count_in_page.size()) {
            m_hasRowCountInPage = true;
        } else {
            m_hasRowCountInPage = false;
        }

        /* Blocklet min max value skip */
        if (tryToSkipCurrentBlocklet(rowsSkip, rowsCross)) {
            return rowsCross;
        }

        /* set column readers to current blocklet  */
        setBlockletIndexForColumnReaders();
    }

    /* try to skip page */
    if ((m_rowsReadInCurrentPage == 0) && (isCurrentPageValid())) {
        if (m_hasRowCountInPage) {
            m_totalRowsInCurrentPage = m_blockletInfo3->row_count_in_page[m_currentPageIndex];

            /* Page min max value skip */
            if (tryToSkipCurrentPage(rowsSkip, rowsCross)) {
                return rowsCross;
            }
        } else {
            m_totalRowsInCurrentPage = 0;
        }

        /* set column readers to current page  */
        setPageIndexForColumnReaders();
    }
    rowsInFile = curRowsInFile + rowsCross;

    uint64_t numRowsToRead = calculateRowsToRead(maxRowsReadOnce);
    if (!readAndFilter(rowsSkip, numRowsToRead)) {
        fillVectorBatch(batch, numRowsToRead, fileOffset, rowsInFile);
    }

    if (!trySwitchToNextBlocklet(numRowsToRead)) {
        trySwitchToNextPage(numRowsToRead);
    }
    rowsCross += numRowsToRead;
    return numRowsToRead;
}

void CarbondataFileReader::initializeColumnReaders(GSInputStream *gsInputStream)
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
            MOD_CARBONDATA, m_readerState->currentSplit->filePath);

        const Var *var = GetVarFromColumnList(m_readerState->allColumnList, static_cast<int32_t>(i + 1));

        auto columnSpec = &m_fileMetaData.m_columnSpecList[m_schemaOrdinalList[columnIndexInFile(i)]];
        auto columnReader = createCarbondataColumnReader(columnSpec, m_schemaOrdinalList[columnIndexInFile(i)],
                                                         mppColIDs[i] + 1, m_readerState, var,
                                                         m_realCarbondataFileReader->GetFooterOffset());
        columnReader->begin(std::move(gsInputStreamCopy), &m_fileMetaData);

        m_columnReaders[m_schemaOrdinalList[columnIndexInFile(i)]] = columnReader;
    }
}

void CarbondataFileReader::destroyColumnReaders()
{
    /* Destroy the array of column reader. */
    for (uint32_t i = 0; i < (uint32_t)m_columnReaders.size(); i++) {
        if (NULL != m_columnReaders[i]) {
            DELETE_EX(m_columnReaders[i]);
        }
    }
}

void CarbondataFileReader::setBlockletIndexForColumnReaders()
{
    for (uint32_t i = 0; i < numberOfColumns(); i++) {
        if (isColumnRequiredToRead(i)) {
            m_columnReaders[m_schemaOrdinalList[columnIndexInFile(i)]]->setBlockletIndex(m_currentBlockletIndex);
        }
    }
}

void CarbondataFileReader::setPageIndexForColumnReaders()
{
    for (uint32_t i = 0; i < numberOfColumns(); i++) {
        if (isColumnRequiredToRead(i)) {
            m_totalRowsInCurrentPage =
                m_columnReaders[m_schemaOrdinalList[columnIndexInFile(i)]]->setPageIndex(m_currentPageIndex);
        }
    }
}

uint64_t CarbondataFileReader::calculateRowsToRead(uint64_t maxRowsReadOnce)
{
    if (m_hasRowCountInPage) {
        uint64_t rowsLeftInCurrentPage = m_totalRowsInCurrentPage - m_rowsReadInCurrentPage;
        return Min(rowsLeftInCurrentPage, maxRowsReadOnce);
    } else {
        return maxRowsReadOnce;
    }
}

bool CarbondataFileReader::readAndFilter(uint64_t rowsSkip, uint64_t numRowsToRead)
{
    uint32_t mppColID = 0;
    uint32_t fileColID = 0;

    bool skipBatch = false;
    bool checkSkipBatch = false;

    resetIsSelectedBuffer(numRowsToRead);

    for (uint32_t i = 0; i < m_readerState->relAttrNum; i++) {
        mppColID = m_readerState->orderedCols[i];

        if (isColumnRequiredToRead(mppColID)) {
            fileColID = m_schemaOrdinalList[columnIndexInFile(mppColID)];

            m_columnReaders[fileColID]->setCurrentRowsInPage(m_rowsReadInCurrentPage);

            /* Filter the column batch with the predicate. */
            checkSkipBatch = false;
            if (!skipBatch && m_columnReaders[fileColID]->hasPredicate()) {
                m_columnReaders[fileColID]->predicateFilter(numRowsToRead, isSelected);
                checkSkipBatch = true;
            }

            /* Check if the batch can be skipped in the end. */
            skipBatch = skipBatch ? true : (checkSkipBatch ? canSkipBatch(numRowsToRead) : false);
            if (skipBatch) {
                return skipBatch;
            }
        }
    }
    return skipBatch;
}

void CarbondataFileReader::fillVectorBatch(VectorBatch *batch, uint64_t numRowsToRead, FileOffset *fileOffset,
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
            m_columnReaders[m_schemaOrdinalList[columnIndexInFile(mppColID)]]->fillScalarVector(numRowsToRead,
                                                                                                isSelected,
                                                                                                scalorVector);
        }
    }

    /* Fill file offset array. */
    if (NULL != fileOffset) {
        fillFileOffset(rowsInFile, numRowsToRead, fileOffset);
    }
}

void CarbondataFileReader::fillFileOffset(uint64_t rowsInFile, uint64_t rowsToRead, FileOffset *fileOffset)
{
    uint32_t *offsetPos = fileOffset->rowIndexInFile;
    uint32_t startPos = fileOffset->numelements;

    Assert(startPos < BatchMaxSize);
    for (uint64_t i = 0; i < rowsToRead; i++) {
        if (isSelected[i]) {
            offsetPos[startPos++] = rowsInFile + i + 1;
        }
    }
    Assert(startPos <= BatchMaxSize);

    fileOffset->numelements = startPos;
}

bool CarbondataFileReader::tryToSkipCurrentBlocklet(uint64_t &rowsSkip, uint64_t &rowsCross)
{
    if (isStartOfCurrentBlocklet()) {
        if (hasRestriction() && !checkPredicateOnCurrentBlocklet()) {
            skipCurrentBlocklet(rowsSkip, rowsCross);
            return true;
        }
    }

    return false;
}

bool CarbondataFileReader::tryToSkipCurrentPage(uint64_t &rowsSkip, uint64_t &rowsCross)
{
    if (isStartOfCurrentPage()) {
        if (hasRestriction() && !checkPredicateOnCurrentPage()) {
            skipCurrentPage(rowsSkip, rowsCross);

            return true;
        }
    }
    return false;
}

bool CarbondataFileReader::isStartOfCurrentBlocklet()
{
    return m_rowsReadInCurrentBlocklet == 0;
}

bool CarbondataFileReader::isStartOfCurrentPage()
{
    return m_rowsReadInCurrentPage == 0;
}

bool CarbondataFileReader::checkPredicateOnCurrentBlocklet()
{
    List *restrictions = buildRestriction(BLOCKLET, m_currentBlockletIndex);

    bool ret = (!predicate_refuted_by(restrictions, m_readerState->queryRestrictionList, false) &&
                !predicate_refuted_by(restrictions, m_readerState->runtimeRestrictionList, true));
    if (!ret) {
        /* these rows will be skipped, so count them */
        m_readerState->minmaxFilterRows += m_totalRowsInCurrentBlocklet;
        /* this stripe(blocklet) will be skipped, so count it */
        m_readerState->minmaxFilterStripe++;
    }
    m_readerState->minmaxCheckStripe++;
    return ret;
}

bool CarbondataFileReader::checkPredicateOnCurrentPage()
{
    List *restrictions = buildRestriction(PAGE, m_currentPageIndex);

    bool ret = (!predicate_refuted_by(restrictions, m_readerState->queryRestrictionList, false) &&
                !predicate_refuted_by(restrictions, m_readerState->runtimeRestrictionList, true));
    if (!ret) {
        /* these rows will be skipped, so count them */
        m_readerState->minmaxFilterRows += m_totalRowsInCurrentPage;
        /* this stride(page) will be skipped, so count it */
        m_readerState->minmaxFilterStride++;
    }
    m_readerState->minmaxCheckStride++;
    return ret;
}

List *CarbondataFileReader::buildRestriction(CarbonRestrictionType type, uint64_t index)
{
    List *fileRestrictionList = NULL;
    bool *restrictRequired = m_readerState->restrictRequired;

    uint64_t numberOfColumns = m_readerState->relAttrNum;
    for (uint32_t i = 0; i < numberOfColumns; i++) {
        Node *baseRestriction = NULL;

        if (restrictRequired[i] && isColumnRequiredToRead(i)) {
            baseRestriction = m_columnReaders[m_schemaOrdinalList[columnIndexInFile(i)]]->buildColRestriction(type,
                                                                                                              index);
            if (NULL != baseRestriction) {
                fileRestrictionList = lappend(fileRestrictionList, baseRestriction);
            }
        }
    }

    return fileRestrictionList;
}

void CarbondataFileReader::skipCurrentBlocklet(uint64_t &rowsSkip, uint64_t &rowsCross)
{
    rowsSkip += m_totalRowsInCurrentBlocklet;
    rowsCross += m_totalRowsInCurrentBlocklet;

    trySwitchToNextBlocklet(m_totalRowsInCurrentBlocklet);
}

void CarbondataFileReader::skipCurrentPage(uint64_t &rowsSkip, uint64_t &rowsCross)
{
    rowsSkip += m_totalRowsInCurrentPage;
    rowsCross += m_totalRowsInCurrentPage;

    if (!trySwitchToNextBlocklet(m_totalRowsInCurrentPage)) {
        trySwitchToNextPage(m_totalRowsInCurrentPage);
    }
}

bool CarbondataFileReader::trySwitchToNextBlocklet(uint64_t numRowsRead)
{
    m_rowsReadInCurrentBlocklet += numRowsRead;
    Assert(m_rowsReadInCurrentBlocklet <= m_totalRowsInCurrentBlocklet);

    if (m_rowsReadInCurrentBlocklet == m_totalRowsInCurrentBlocklet) {
        switchToNextBlocklet();
        return true;
    }
    return false;
}

void CarbondataFileReader::trySwitchToNextPage(uint64_t numRowsRead)
{
    m_rowsReadInCurrentPage += numRowsRead;
    Assert(m_rowsReadInCurrentPage <= m_totalRowsInCurrentPage);

    if (m_rowsReadInCurrentPage == m_totalRowsInCurrentPage) {
        switchToNextPage();
    }
}

bool CarbondataFileReader::hasRestriction()
{
    return m_readerState->queryRestrictionList != NULL;
}
}  // namespace reader
}  // namespace dfs
