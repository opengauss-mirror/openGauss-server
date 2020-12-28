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
 * carbondata_reader.cpp
 *
 *
 * IDENTIFICATION
 *         src/gausskernel/storage/access/dfs/carbondata/carbondata_reader.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "storage/dfs/dfscache_mgr.h"
#include "postgres.h"
#include "access/dfs/dfs_common.h"
#include "access/dfs/dfs_query.h"

#include "carbondata_file_reader.h"
#include "carbondata_reader.h"
#include "carbondata_stream_adapter.h"

#include "access/dfs/dfs_stream_factory.h"
#include "optimizer/predtest.h"
#include "pgstat.h"
#include "utils/memutils.h"

#define FOREIGNTABLEFILEID (-1)

THR_LOCAL bool enable_carbondata_cache = true;

namespace dfs {
namespace reader {
/* CarbondataReaderImpl */
class CarbondataReaderImpl {
public:
    CarbondataReaderImpl(ReaderState *readerState, dfs::DFSConnector *conn);
    ~CarbondataReaderImpl();

    void begin();
    void end();
    void Destroy();
    bool loadFile(char *file_path, List *file_restriction);
    uint64_t nextBatch(VectorBatch *batch, uint64_t cur_rows_in_file, DFSDesc *dfs_desc, FileOffset *file_offset);
    void nextBatchByContinueTids(uint64_t rowsSkip, uint64_t rowsToRead, VectorBatch *batch, uint64_t startOffset,
                                 DFSDesc *dfsDesc, FileOffset *fileOffset);
    void addBloomFilter(filter::BloomFilter *bloomFilter, int colIdx, bool is_runtime);
    void copyBloomFilter();
    bool checkBFPruning(uint32_t colID, char *colValue);
    uint64_t getNumberOfRows();
    void setBatchCapacity(uint64_t capacity);
    bool fileEOF(uint64_t rows_read_in_file);

private:
    inline void createStructReaderWithCacheForInnerTable(std::unique_ptr<GSInputStream> gsInputStream);
    inline void createStructReaderWithCacheForForeignTable(std::unique_ptr<GSInputStream> gsInputStream);
    void createStructReaderWithCache();
    void buildStructReader();
    void collectFileReadingStatus();
    void fixOrderedColumnList(uint32_t colIdx);
    /* For foreign table, remove the non-exist columns in file. */
    void setNonExistColumns();
    void resetCarbondataFileReader();

    ReaderState *m_readerState;
    dfs::DFSConnector *m_conn;
    char *m_currentFilePath;
    uint64_t m_numberOfRows;
    uint64_t m_numberOfFields;
    uint64_t m_numberOfColumns;
    uint64_t m_numberOfBlocklets;

    bool hasBloomFilter;
    filter::BloomFilter **bloomFilters;
    filter::BloomFilter **staticBloomFilters;

    dfs::GSInputStream *m_gsInputStream;
    uint64_t m_maxRowsReadOnce;

    CarbondataFileReader *m_fileReader;
    MemoryContext m_internalContext;
};

/* CarbondataReader */
CarbondataReaderImpl::CarbondataReaderImpl(ReaderState *readerState, dfs::DFSConnector *conn)
    : m_readerState(readerState),
      m_conn(conn),
      m_currentFilePath(NULL),
      m_numberOfRows(0),
      m_numberOfFields(0),
      m_numberOfColumns(0),
      m_numberOfBlocklets(0),
      hasBloomFilter(false),
      bloomFilters(NULL),
      staticBloomFilters(NULL),
      m_gsInputStream(NULL),
      m_maxRowsReadOnce(0),
      m_fileReader(NULL),
      m_internalContext(NULL)
{
}

CarbondataReaderImpl::~CarbondataReaderImpl()
{
}

void CarbondataReaderImpl::begin()
{
    /* static_cast */
    m_maxRowsReadOnce = static_cast<uint64_t>(BatchMaxSize);
    m_internalContext = AllocSetContextCreate(m_readerState->persistCtx,
                                              "Internal batch reader level context for carbondata dfs reader",
                                              ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                              ALLOCSET_DEFAULT_MAXSIZE);

    /* The number of columns stored in Carbondata file. */
    m_numberOfColumns = m_readerState->relAttrNum;

    MemoryContext oldContext = MemoryContextSwitchTo(m_readerState->persistCtx);
    bloomFilters = (filter::BloomFilter **)palloc0(sizeof(filter::BloomFilter *) * m_numberOfColumns);
    staticBloomFilters = (filter::BloomFilter **)palloc0(sizeof(filter::BloomFilter *) * m_numberOfColumns);
    (void)MemoryContextSwitchTo(oldContext);
}

void CarbondataReaderImpl::end()
{
    /* collect the statictics for the last reading */
    collectFileReadingStatus();
}

void CarbondataReaderImpl::Destroy()
{
    /* Clean bloom filter. */
    pfree(bloomFilters);
    pfree(staticBloomFilters);
    bloomFilters = NULL;
    staticBloomFilters = NULL;

    /* Clean the array of column reader. */
    resetCarbondataFileReader();

    /*
     * The conn is not created in this class but we need to free it here,
     * because once the conn is transfered into then the owner of it is changed.
     */
    if (NULL != m_conn) {
        delete (m_conn);
        m_conn = NULL;
    }

    /* Clear the internal memory context. */
    if (NULL != m_internalContext && m_internalContext != CurrentMemoryContext) {
        MemoryContextDelete(m_internalContext);
    }
}

bool CarbondataReaderImpl::loadFile(char *file_path, List *fileRestriction)
{
    /* the total number of acquiring loading files */
    m_readerState->minmaxCheckFiles++;

    /*
     * Before open the file, check if the file can be skipped according to
     * min/max information stored in desc table if exist.
     */
    if (NIL != fileRestriction &&
        (predicate_refuted_by(fileRestriction, m_readerState->queryRestrictionList, false) ||
         predicate_refuted_by(fileRestriction, m_readerState->runtimeRestrictionList, true))) {
        /* filter by min/max info in desc table */
        m_readerState->minmaxFilterFiles++;
        return false;
    }

    m_currentFilePath = file_path;

    /* Collect the statictics of the previous reading. */
    collectFileReadingStatus();

    /* Reset Carbondata file reader and column readers within it */
    resetCarbondataFileReader();

    /* Construct the struct reader to read the meta data. */
    buildStructReader(); /* open file here */

    if (0 == m_fileReader->getNumRows() || 0 == m_fileReader->getNumBlocklets()) {
        return false;
    }

    /* Initlize the column readers. */
    m_fileReader->initializeColumnReaders(m_gsInputStream);

    return true;
}

uint64_t CarbondataReaderImpl::nextBatch(VectorBatch *batch, uint64_t curRowsInFile, DFSDesc *dfsDesc,
                                         FileOffset *fileOffset)
{
    uint64_t rowsInFile = 0;
    uint64_t rowsCross = 0;
    uint64_t rowsSkip = 0;

    MemoryContextReset(m_internalContext);
    AutoContextSwitch newMemCnxt(m_internalContext);

    DFS_TRY()
    {
        while (m_fileReader->isCurrentBlockletValid() && (batch->m_rows < BatchMaxSize)) {
            (void)m_fileReader->readRows(batch, BatchMaxSize - batch->m_rows, curRowsInFile, rowsInFile, rowsSkip,
                                         rowsCross, fileOffset);
        }
    }
    DFS_CATCH();
    DFS_ERRREPORT_WITHOUTARGS(
        "Error occurs while readRows  because of %s, "
        "detail can be found in dn log of %s.",
        MOD_CARBONDATA);

    return rowsCross;
}

void CarbondataReaderImpl::nextBatchByContinueTids(uint64_t rowsSkip, uint64_t rowsToRead, VectorBatch *batch,
                                                   uint64_t startOffset, DFSDesc *dfsDesc, FileOffset *fileOffset)
{
}

void CarbondataReaderImpl::addBloomFilter(filter::BloomFilter *bloomFilter, int colIdx, bool is_runtime)
{
    if (NULL == bloomFilter) {
        return;
    }
}

void CarbondataReaderImpl::copyBloomFilter()
{
}

bool CarbondataReaderImpl::checkBFPruning(uint32_t colID, char *colValue)
{
    return false;
}

uint64_t CarbondataReaderImpl::getNumberOfRows()
{
    return m_numberOfRows;
}

void CarbondataReaderImpl::setBatchCapacity(uint64_t capacity)
{
}

bool CarbondataReaderImpl::fileEOF(uint64_t rows_read_in_file)
{
    return rows_read_in_file >= getNumberOfRows();
}

void CarbondataReaderImpl::collectFileReadingStatus()
{
    if (NULL == m_gsInputStream) {
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

void CarbondataReaderImpl::fixOrderedColumnList(uint32_t colIdx)
{
    uint32_t bound = 0;
    uint32_t *orderedColumns = m_readerState->orderedCols;
    bool *requiredRestrict = m_readerState->restrictRequired;

    for (bound = 0; bound < m_numberOfColumns; bound++) {
        if (orderedColumns[bound] == colIdx) {
            break;
        }
    }

    for (uint32_t i = 0; i < bound; i++) {
        if (!requiredRestrict[orderedColumns[i]]) {
            orderedColumns[bound] = orderedColumns[i];
            orderedColumns[i] = colIdx;
            break;
        }
    }
}

void CarbondataReaderImpl::setNonExistColumns()
{
    bool *readRequired = m_readerState->readRequired;
    List **predicateArr = m_readerState->hdfsScanPredicateArr;
    for (uint32_t i = 0; i < m_readerState->relAttrNum; i++) {
        if (readRequired[i] && (m_readerState->readColIDs[i] + 1 > m_fileReader->getNumColumns())) {
            /* modify here, and will impact dfs_am layer. */
            readRequired[i] = false;

            uint32_t arrayIndex = m_readerState->globalColIds[i];
            if (predicateArr[arrayIndex] != NULL) {
                bool isNull = false;
                ScanState *scanState = m_readerState->scanstate;
                Var *var = GetVarFromColumnList(m_readerState->allColumnList, i + 1);
                if (NULL == var) {
                    continue;
                }
                Datum result = heapGetInitDefVal(i + 1, scanState->ss_currentRelation->rd_att, &isNull);
                if (false == defColSatisfyPredicates(isNull, result, var, predicateArr[arrayIndex])) {
                    m_numberOfRows = 0;
                    break;
                }
            }
        }
    }
}

void CarbondataReaderImpl::resetCarbondataFileReader()
{
    if (NULL != m_fileReader) {
        DELETE_EX(m_fileReader);
    }
}

/*
 * @Description: creat struct reader with cache for inner table
 * @IN stream: input stream
 * @See also:
 */
inline void CarbondataReaderImpl::createStructReaderWithCacheForInnerTable(std::unique_ptr<GSInputStream> gsInputStream)
{
    pgstat_count_buffer_read(m_readerState->scanstate->ss_currentRelation);
}

/*
 * @Description: creat struct reader with cache for foreign table
 * @IN stream: input stream
 * @See also:
 */
inline void
CarbondataReaderImpl::createStructReaderWithCacheForForeignTable(std::unique_ptr<GSInputStream> gsInputStream)
{
    CarbonMetadataValue *metaData = NULL;
    CacheSlotId_t slotId = CACHE_BLOCK_INVALID_IDX;
    RelFileNode fileNode = m_readerState->scanstate->ss_currentRelation->rd_node;
    std::string fileName = gsInputStream->getName();
    uint32 stripeID = 0;
    uint32 columnID = 0;
    int32 prefixNameHash = string_hash((void *)fileName.c_str(), fileName.length() + 1);
    bool hasFound = false;
    bool collision = false;
    bool renewCollision = false;
    bool dataChg = false;

    pgstat_count_buffer_read(m_readerState->scanstate->ss_currentRelation);

    m_fileReader = CarbondataFileReader::create(m_readerState, std::move(gsInputStream));

    slotId = MetaCacheAllocBlock((RelFileNodeOld *)&fileNode, (int32_t)prefixNameHash, stripeID, columnID, hasFound,
                                 CACHE_CARBONDATA_METADATA);
    metaData = CarbonMetaCacheGetBlock(slotId);
    if (hasFound) {
        Assert(metaData->fileFooter);
        Assert(metaData->fileHeader);
        Assert(metaData->fileName);
        Assert(m_readerState->currentSplit->eTag);

        size_t fileNameLen = fileName.length();
        size_t dataDNALen = strlen(m_readerState->currentSplit->eTag);

        /* judge if we collision */
        if (strncmp(fileName.c_str(), metaData->fileName, fileNameLen))
            collision = true;
        else {
            if (strncmp(m_readerState->currentSplit->eTag, metaData->dataDNA, dataDNALen))
                dataChg = true;
            else {
                m_fileReader->setFileMetaData(metaData->fileFooter, metaData->fileHeader, metaData->footerSize,
                                              metaData->headerSize);

                m_readerState->orcMetaCacheBlockCount++;
                m_readerState->orcMetaCacheBlockSize += CarbonMetaCacheGetBlockSize(slotId);
                pgstat_count_buffer_hit(m_readerState->scanstate->ss_currentRelation);

                ReleaseMetaBlock(slotId);
            }
        }
    }
    /* OBS file content has changed, refresh it */
    if (dataChg && !MetaCacheRenewBlock(slotId))
        renewCollision = true;

    /* not find in cache and eTag doesn't match when we need update cache */
    if (!hasFound || (dataChg && !renewCollision)) {
        uint64_t footerSize = m_fileReader->getFooterSize();
        unsigned char *fileFooter = (unsigned char *)CStoreMemAlloc::Palloc(footerSize, false);
        if (NULL == fileFooter) {
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_CARBONDATA),
                            errmsg("Failed to alloc the space to fileFooter.")));
        }
        m_fileReader->getFooterCache(fileFooter);
        Assert(NULL != fileFooter);

        uint64_t headerSize = m_fileReader->getHeaderSize();
        unsigned char *fileHeader = (unsigned char *)CStoreMemAlloc::Palloc(headerSize, false);
        if (NULL == fileHeader) {
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_CARBONDATA),
                            errmsg("Failed to alloc the space to fileHeader.")));
        }
        m_fileReader->getHeaderCache(fileHeader);
        Assert(NULL != fileHeader);

        CarbonMetaCacheSetBlock(slotId, headerSize, footerSize, fileHeader, fileFooter, fileName.c_str(),
                                m_readerState->currentSplit->eTag);

        m_readerState->orcMetaLoadBlockCount++;
        m_readerState->orcMetaLoadBlockSize += CarbonMetaCacheGetBlockSize(slotId);
        ReleaseMetaBlock(slotId);
        CStoreMemAlloc::Pfree(fileFooter, false);
        CStoreMemAlloc::Pfree(fileHeader, false);
    } else if (collision || (dataChg && renewCollision)) /* renewCollision = ture */
    {
        ReleaseMetaBlock(slotId);
        m_readerState->orcMetaLoadBlockCount++;
        m_readerState->orcMetaLoadBlockSize += 0;
    }
    m_fileReader->begin();
}

void CarbondataReaderImpl::createStructReaderWithCache()
{
    std::unique_ptr<dfs::GSInputStream> gsInputStream =
        dfs::InputStreamFactory(m_conn, m_currentFilePath, m_readerState, g_instance.attr.attr_sql.enable_orc_cache);

    m_gsInputStream = gsInputStream.get();

    int32_t connect_type = m_conn->getType();

    if (m_readerState->currentFileID >= 0 && g_instance.attr.attr_sql.enable_orc_cache) {
        createStructReaderWithCacheForInnerTable(std::move(gsInputStream));
    }
    /*
     * here we go, OBS foreign table.
     */
    else if (FOREIGNTABLEFILEID == m_readerState->currentFileID && OBS_CONNECTOR == connect_type &&
             g_instance.attr.attr_sql.enable_orc_cache) {
        createStructReaderWithCacheForForeignTable(std::move(gsInputStream));
    } else {
        m_fileReader = CarbondataFileReader::create(m_readerState, std::move(gsInputStream));

        uint64_t footerSize = m_fileReader->getFooterSize();
        unsigned char *fileFooter = (unsigned char *)CStoreMemAlloc::Palloc(footerSize, false);
        m_fileReader->getFooterCache(fileFooter);

        uint64_t headerSize = m_fileReader->getHeaderSize();
        unsigned char *fileHeader = (unsigned char *)CStoreMemAlloc::Palloc(headerSize, false);
        m_fileReader->getHeaderCache(fileHeader);

        m_fileReader->begin();
    }
    return;
}

void CarbondataReaderImpl::buildStructReader()
{
    /* Start to load the file. */
    DFS_TRY()
    {
        createStructReaderWithCache();
    }
    DFS_CATCH();
    DFS_ERRREPORT_WITHARGS(
        "Error occurs while creating an Carbondata reader for file %s because of %s, "
        "detail can be found in dn log of %s.",
        MOD_CARBONDATA, m_currentFilePath);

    if (NULL == m_fileReader) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_CARBONDATA),
                        errmsg("Failed to alloc the space to fileReader.")));
    }
    Assert(NULL != m_fileReader);

    /* check the number of rows first */
    m_numberOfRows = m_fileReader->getNumRows();
    if (0 == getNumberOfRows() || 0 == m_fileReader->getNumBlocklets()) {
        return;
    }
#if 1
    if (RelationIsForeignTable(m_readerState->scanstate->ss_currentRelation)) {
        setNonExistColumns();
    }
#endif
}

/* CarbondataReader */
CarbondataReader::CarbondataReader(ReaderState *readerState, dfs::DFSConnector *conn)
    : m_cImpl(new CarbondataReaderImpl(readerState, conn))
{
}

CarbondataReader::~CarbondataReader()
{
    if (NULL != m_cImpl) {
        delete m_cImpl;
        m_cImpl = NULL;
    }
}

void CarbondataReader::begin()
{
    m_cImpl->begin();
}

void CarbondataReader::end()
{
    m_cImpl->end();
}

void CarbondataReader::Destroy()
{
    m_cImpl->Destroy();
}

bool CarbondataReader::loadFile(char *file_path, List *file_restriction)
{
    return m_cImpl->loadFile(file_path, file_restriction);
}

uint64_t CarbondataReader::nextBatch(VectorBatch *batch, uint64_t cur_rows_in_file, DFSDesc *dfs_desc,
                                     FileOffset *file_offset)
{
    return m_cImpl->nextBatch(batch, cur_rows_in_file, dfs_desc, file_offset);
}

void CarbondataReader::nextBatchByContinueTids(uint64_t rowsSkip, uint64_t rowsToRead, VectorBatch *batch,
                                               uint64_t startOffset, DFSDesc *dfsDesc, FileOffset *fileOffset)
{
    m_cImpl->nextBatchByContinueTids(rowsSkip, rowsToRead, batch, startOffset, dfsDesc, fileOffset);
}

void CarbondataReader::addBloomFilter(filter::BloomFilter *bloomFilter, int colIdx, bool is_runtime)
{
    m_cImpl->addBloomFilter(bloomFilter, colIdx, is_runtime);
}

void CarbondataReader::copyBloomFilter()
{
    m_cImpl->copyBloomFilter();
}

bool CarbondataReader::checkBFPruning(uint32_t colID, char *colValue)
{
    return m_cImpl->checkBFPruning(colID, colValue);
}

uint64_t CarbondataReader::getNumberOfRows()
{
    return m_cImpl->getNumberOfRows();
}

void CarbondataReader::setBatchCapacity(uint64_t capacity)
{
    m_cImpl->setBatchCapacity(capacity);
}

bool CarbondataReader::fileEOF(uint64_t rows_read_in_file)
{
    return m_cImpl->fileEOF(rows_read_in_file);
}
}  // namespace reader
}  // namespace dfs
