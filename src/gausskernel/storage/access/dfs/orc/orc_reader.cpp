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
 * orc_reader.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/orc/orc_reader.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "orc_rw.h"
#ifndef ENABLE_LITE_MODE
#include "orc/Adaptor.hh"
#include "orc/Exceptions.hh"
#include "orc/OrcFile.hh"
#endif
#include "OrcObsFile.h"
#ifndef ENABLE_LITE_MODE
#include "orc/Reader.hh"
#endif
#include "storage/dfs/dfscache_mgr.h"
#include "storage/cucache_mgr.h"
#include "access/dfs/dfs_common.h"
#include "access/dfs/dfs_query.h"
#include "access/dfs/dfs_query_check.h"
#include "access/dfs/dfs_wrapper.h"
#include "optimizer/predtest.h"
#include "pgstat.h"
#include "dfs_adaptor.h"
#include "access/dfs/dfs_stream_factory.h"
#include "orc_stream_adapter.h"

#define READER_SETSTRIPEFOOTER(reader, stripeIndex, stripeFooter) do { \
    DFS_TRY()                                                              \
    {                                                                      \
        (reader)->setStripeFooter(stripeIndex, stripeFooter);              \
    }                                                                      \
    DFS_CATCH();                                                           \
    DFS_ERRREPORT_WITHARGS(                                                \
        "Error occurs while set stripe footer of "                         \
        "orc file %s because of %s, detail can be found in dn log of %s.", \
        MOD_ORC, readerState->currentSplit->filePath);                     \
} while (0)

namespace dfs {
namespace reader {
#define NOT_STRING(oid) (((oid) != VARCHAROID && (oid) != BPCHAROID) && ((oid) != TEXTOID && (oid) != CLOBOID))

/*
 * @Description: get orc meta data  from cache value
 * @IN slotId: meta cache slot id
 * @IN metaData: meta data value
 * @OUT ps:  postscript
 * @OUT footer:  footer
 */
static void getFromMetadataValue(CacheSlotId_t slotId, OrcMetadataValue *metaData, orc::proto::PostScript *ps,
                                 orc::proto::Footer *footer)
{
    Assert(metaData && ps && footer);

    if (!ps->ParseFromString(*(metaData->postScript))) {
        ReleaseMetaBlock(slotId);
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_ORC),
                        errmsg("Failed to parse the postscript from string")));
    }
    if (!footer->ParseFromString(*(metaData->fileFooter))) {
        ReleaseMetaBlock(slotId);
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_ORC),
                        errmsg("Failed to parse the filefooter from string")));
    }
}

/*
 * The class which implements all the read-related operations upon the special column.
 */
template <FieldKind fieldKind>
class OrcColumnReaderImpl : public OrcColumnReader {
public:
    OrcColumnReaderImpl(int64_t _columnIdx, int64_t _mppColumnIdx, ReaderState *_readerState, const Var *_var);

    virtual ~OrcColumnReaderImpl() override;

    /* Initialization works */
    void begin(std::unique_ptr<orc::InputStream> stream, const orc::Reader *structReader, const uint64_t maxBatchSize);

    /* Clear the memory and resource. */
    void Destroy() override;

    /*
     * Load and set the row index and bloom filter of the current columns
     * if they exist and are needed.
     * @_in_param stripeIndex: The index of the current stripe to be read.
     * @_in_param hasCheck: Indicate whether the current column has
     *		restrictions. The row index stream is always needed because we
     *		 need it to seek a special stride. The bloom filter stream
     *		is needed only when there are restrictions. Of course, there
     *		is no bloom filter stream in the orc file unless you set the
     *		orc.bloom.filter.columns when importing the data.
     */
    void setRowIndex(uint64_t stripeIndex, bool hasCheck, const orc::proto::StripeFooter *stripeFooter) override;

    /*
     * Read a special number of rows in one column of ORC file.
     * @_in_param numValues: The number of rows to read.
     */
    void nextInternal(uint64_t numValues) override;

    /*
     * Skip a special number of rows in one column of ORC file.
     * @_in_param numValues: The number of rows to read.
     */
    void skip(uint64_t numValues) override;

    /*
     * Set the bloom filter in the column reader.
     * @_in_param bloomFilter: The bloom filter to be set.
     */
    void setBloomFilter(filter::BloomFilter *bloomFilter) override;

    /*
     * Check if the current column of ORC file has predicates.
     * @return true: The predicate of the column exists;
     *			   false: The predicate of the column does not exists;
     */
    bool hasPredicate() const override;

    /*
     * Filter the obtained data with the predicates on the column
     * and set isSelected flags.
     * @_in_param numValues: The number of rows to be filtered.
     * @_in_out_param isSelected: The flag array to indicate which row is selected or not.
     */
    void predicateFilter(uint64_t numValues, bool *isSelected) override;

    /*
     * Convert the primitive data to datum with which the scalar
     * vector will be filled.
     * @_in_param numValues: The number of rows to fill the scalar vector.
     * @_in_out_param isSelected: The flag array to indicate which row is selected or not.
     * @_out_param vec: The scalar vector to be filled.
     * @return the number of selected rows of the filled scalar vector.
     */
    int32_t fillScalarVector(uint64_t numValues, bool *isSelected, ScalarVector *vec) override;

    /*
     * Check if the equal op restrict(we generate a bloom filter) matches
     * the bloomfilter of the orc file.
     * @_in_param strideIdx: The index of the stride as the bloom filter
     *		is created for each stride in ORC file.
     * @return true only if the two bloom filters both exist and match while
     *		one stores in file and the other is generated by the restrict.
     *		Otherwise return false.
     */
    bool checkBloomFilter(uint64_t strideIdx) const override;

    /*
     * Build the column restriction node of file/stripe/stride with the min/max
     * information stored in ORC file. Dynamic parition prunning is also handled
     * here.
     * @_in_param type: Indicates the restriction's level:file, stripe
     *							 or stride.
     * @_in_param structReader: the orc reader for structure
     * @_in_param stripeIdx: When the type is STRIPE, this is used
     *								   to identify the special stripe.
     * @_in_param strideIdx: When the type is STRIDE, this is used
     *								   to identifythe special stride.
     * @return the built restriction of file, stripe or stride.
     */
    Node *buildColRestriction(RestrictionType type, orc::Reader *structReader, uint64_t stripeIdx,
                              uint64_t strideIdx) const override;

private:
    /*
     * Make a nulltest filter before the value filter.
     * @return true if the row is null or the row is already unselected,
     *		else return false.
     */
    bool nullFilter(bool hasNull, const char *notNull, bool *isSelected, uint64_t rowIdx);

    /*
     * Check if the field type of current column matches the var type defined in
     * relation. For now, we use one-to-one type mapping mechanism.
     * @return true if The two type matches or return false.
     */
    bool matchOrcWithPsql() const;

    /*
     * Initialize a column reader when construct a new OrcColumnReaderImpl.
     * @_in_param _execState: Includes the neccessary params.
     * @_in_param stream: The stream to read from hdfs file.
     * @_in_param structReader: Provides the information of the file meta data.
     * @_in_param maxBatchSize: The max number of rows to read once from ORC file.
     * @_in_param opts: Includes the selected column(s) and customized memory pool.
     */
    void initColumnReader(std::unique_ptr<orc::InputStream> stream, const orc::Reader *structReader,
                          const uint64_t maxBatchSize, const orc::ReaderOptions &opts);

    /*
     * Get the statistics of the current columns, the level can be file, stripe or
     * stride that is decided by the statistics transfered in.
     * @_in_param statistics: The column statistics of a file, stripe or stride.
     * @_out_param minDatum: Set the Datum of minimal value if exists.
     * @_out_param hasMinimum: Indicates if the minimal value exists.
     * @_out_param maxDatum: Set the Datum of maximal value if exists.
     * @_out_param hasMaximum: Indicates if the maximal value exists.
     * @return 0: there is no minimum nor maximum.
     *			   1: there is as least one extremum.
     */
    int32_t getStatistics(const orc::ColumnStatistics *statistics, Datum *minDatum, bool *hasMinimum, Datum *maxDatum,
                          bool *hasMaximum) const;

    template <bool decimal128>
    void bindConvertFunc();

    /*
     * Template function and convert the primitive data to datum with which the scalar
     * vector will be filled.
     * @_in_param numValues: The number of rows to fill the scalar vector.
     * @_in_out_param isSelected: The flag array to indicate which row is selected or not.
     * @_out_param vec: The scalar vector to be filled.
     */
    template <bool hasNull, bool encoded>
    void fillScalarVectorInternal(uint64_t numValues, bool *isSelected, ScalarVector *vec);

private:
    /* set row index for inner table */
    void setRowIndexForInnerTable(uint64_t stripeIndex, bool hasCheck, const orc::proto::StripeFooter *stripeFooter);

    /* set row index for foreign table */
    void setRowIndexForForeignTable(uint64_t stripeIndex, bool hasCheck, const orc::proto::StripeFooter *stripeFooter);

    ReaderState *readerState;
    /* The exprs for building the restriction according to the min/max value. */
    OpExpr *lessThanExpr;
    OpExpr *greaterThanExpr;

    /*
     * The index of the column stored in ORC file which can be different from
     * the one defined in relation.
     */
    int64_t columnIdx;
    /*
     * The value of "mppColumnIdx[i] -1" matches the hdfsScanPredicateArr[i] value.
     * we need to find the predicates of this column on hdfsScanPredicateArr list by
     * mppColumnIdx.
     * It starts from 1.
     */
    int64_t mppColumnIdx;

    /* The object to read ORC file provided by Apache ORC open source code. */
    std::unique_ptr<orc::Reader> reader;

    /* The structure to store the primitive data batch. */
    std::unique_ptr<orc::ColumnVectorBatch> primitiveBatch;

    /* The var of the corresponding column define in relation. */
    const Var *var;

    /* The predicates list which is pushed down into the current column. */
    List *predicate;

    /* Flag shows whether the decimal format of the column use Int128 as value. */
    bool useDecimal128;

    /* The level of encoding check,2 = high, 1 = low, 0 = no check */
    const int32_t checkEncodingLevel;

    /* The buffer of rows to be skipped. */
    uint64_t skipRowsBuffer;

    /* The difference between the default epoch offset in orc and in pg. */
    int64_t epochOffsetDiff;

    /*
     * The bloom filter of the current column which is generated by the
     * (dynamic/static) predicate.
     */
    filter::BloomFilter *bloomFilter;

    /* The function pointer to convert different data type to datum. */
    convertToDatum convertToDatumFunc;

    /* MemoryContext for orc column reader */
    MemoryContext columnReaderCtx;

    /* The scale of the decimal data type. */
    int32_t scale;

    /* Flag to indicate whether we need to check bloom filter/predicate on row level. */
    bool checkBloomFilterOnRow;
    bool checkPredicateOnRow;

    /* used in OBS foreign table */
    char *fileName;
};

/* do nothing */
OrcReaderImpl::OrcReaderImpl(ReaderState *_readerState, dfs::DFSConnector *_conn) : currentFilePath(NULL),
    numberOfColumns(0), numberOfRows(0), numberOfOrcCols(0), numberOfFields(0), numberOfStripes(0), numberOfStrides(0),
    rowsReadInCurrentStripe(0), currentStripeIdx(0), currentStrideIdx(0), rowsInStride(10000), rowsInCurrentStripe(0),
    readerState(_readerState), maxRowsReadOnce(0), isSelected(NULL), internalContext(NULL), hasBloomFilter(false),
    conn(_conn), bloomFilters(NULL), staticBloomFilters(NULL), newStream(NULL)
{}

OrcReaderImpl::~OrcReaderImpl()
{
    /*
     * Here do not call Destroy because all this object should
     * be deleted by DELETE_EX.
     */
    pfree_ext(staticBloomFilters);
    pfree_ext(bloomFilters);
    Assert(NULL == bloomFilters);
    Assert(NULL == staticBloomFilters);
    currentFilePath = NULL;
    readerState = NULL;
    newStream = NULL;
    isSelected = NULL;
    internalContext = NULL;
    conn = NULL;
}

void OrcReaderImpl::begin()
{
    /* The number of columns stored in ORC file. */
    numberOfColumns = readerState->relAttrNum;
    numberOfFields = numberOfColumns - readerState->partNum;

    /*
     * For none-partitioned table, we have to have num of to-be read attributes
     * greater than 0, but it is not true for partitioned table e.g all table
     *	columns are paritioning columns.
     */
    Assert(RelationIsValuePartitioned(readerState->scanstate->ss_currentRelation) ||
           (!RelationIsValuePartitioned(readerState->scanstate->ss_currentRelation) && (numberOfFields > 0)));
    columnReader.resize(static_cast<int32_t>(numberOfFields));
    maxRowsReadOnce = static_cast<uint64_t>(BatchMaxSize * 0.5);
    internalContext =
        AllocSetContextCreate(readerState->persistCtx, "Internal batch reader level context for orc reader",
                              ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext oldContext = MemoryContextSwitchTo(readerState->persistCtx);
    isSelected = (bool *)palloc0(sizeof(bool) * BatchMaxSize);
    bloomFilters = (filter::BloomFilter **)palloc0(sizeof(filter::BloomFilter *) * numberOfColumns);
    staticBloomFilters = (filter::BloomFilter **)palloc0(sizeof(filter::BloomFilter *) * numberOfColumns);
    (void)MemoryContextSwitchTo(oldContext);
}

void OrcReaderImpl::end()
{
    /* collect the statictics for the last reading */
    collectFileReadingStatus();
}

void OrcReaderImpl::Destroy()
{
    /* Clean isSelected flag array. */
    if (NULL != isSelected) {
        pfree(isSelected);
        isSelected = NULL;
    }

    /* Clean bloom filter. */
    pfree(bloomFilters);
    pfree(staticBloomFilters);
    bloomFilters = NULL;
    staticBloomFilters = NULL;

    /* Clean the array of column reader. */
    resetOrcColumnReaders();

    /*
     * The conn is not created in this class but we need to free it here,
     * because once the conn is transfered into then the owner of it is changed.
     */
    if (NULL != conn) {
        delete (conn);
        conn = NULL;
    }

    /* Clear the internal memory context. */
    if (NULL != internalContext && internalContext != CurrentMemoryContext) {
        MemoryContextDelete(internalContext);
    }
}

/*
 * when false returns, the file is skipped and we need to load the next one if exists.
 */
bool OrcReaderImpl::loadFile(char *filePath, List *fileRestriction)
{
    /* the total number of acquiring loading files */
    readerState->minmaxCheckFiles++;

    /*
     * Before open the file, check if the file can be skipped according to
     * min/max information stored in desc table if exist.
     */
    if (NIL != fileRestriction && (predicate_refuted_by(fileRestriction, readerState->queryRestrictionList, false) ||
                                   predicate_refuted_by(fileRestriction, readerState->runtimeRestrictionList, true))) {
        /* filter by min/max info in desc table */
        readerState->minmaxFilterFiles++;
        return false;
    }

    currentFilePath = filePath;

    /* Reset each column ORC readers */
    resetOrcColumnReaders();

    /* Collect the statictics of the previous reading. */
    collectFileReadingStatus();

    /* Construct the struct reader to read the meta data. */
    buildStructReader();
    if (0 == numberOfRows || 0 == numberOfStripes) {
        return false;
    }

    /* Initlize the column readers. */
    initializeColumnReaders();

    /*
     * If the min/max info is not stored in desc table, then we need to check the file
     * statistics after opening the file.
     */
    if (!checkPredicateOnCurrentFile(fileRestriction)) {
        return false;
    }

    /* Transfer the bloom filter informations to the required columns.	 */
    if (hasBloomFilter) {
        setColBloomFilter();
    }

    return true;
}

void OrcReaderImpl::resetOrcColumnReaders()
{
    /* Reset the array of column reader. */
    for (uint32_t i = 0; i < (uint32_t)columnReader.size(); i++) {
        if (NULL != columnReader[i]) {
            DELETE_EX(columnReader[i]);
        }
    }
}

uint64_t OrcReaderImpl::nextBatch(VectorBatch *batch, uint64_t curRowsInFile, DFSDesc *dfsDesc, FileOffset *fileOffset)
{
    uint64_t rowsInFile = 0;
    uint64_t rowsCross = 0;
    uint64_t rowsSkip = 0;
    uint64_t rowsToRead = 0;

    MemoryContextReset(internalContext);
    AutoContextSwitch newMemCnxt(internalContext);

    /* loop until arriving at the end of the file or the batch size is enough. */
    while (currentStripeIdx < numberOfStripes && batch->m_rows + maxRowsReadOnce <= BatchMaxSize) {
        /* Try stripe skip. */
        if (tryStripeSkip(rowsSkip, rowsCross))
            continue;

        /* Try stride skip. */
        if (tryStrideSkip(rowsSkip, rowsCross))
            continue;

        rowsInFile = curRowsInFile + rowsCross;
        rowsToRead = computeRowsToRead(maxRowsReadOnce);

        /* combine the delete map with the isSelect array for the first column. */
        combineDeleteMap(dfsDesc, rowsInFile, rowsToRead);

        if (!readAndFilter(rowsSkip, rowsToRead)) {
            fillVectorBatch(rowsToRead, batch, fileOffset, rowsInFile);
        }

        refreshState(rowsToRead, rowsSkip, rowsCross);
    }

    return rowsCross;
}

void OrcReaderImpl::nextBatchByContinueTids(uint64_t rowsSkip, uint64_t rowsToRead, VectorBatch *batch,
                                            uint64_t startOffset, DFSDesc *dfsDesc, FileOffset *fileOffset)
{
    MemoryContextReset(internalContext);
    AutoContextSwitch newMemCnxt(internalContext);

    while (rowsToRead > 0) {
        /* Set the stripe/stride status after skiping the special number of rows. */
        resetStripeAndStrideStat(rowsSkip);

        /* compute the number of rows to read this time */
        uint64_t onceRead = computeRowsToRead(rowsToRead);

        /* filter the deleted rows */
        combineDeleteMap(dfsDesc, startOffset, onceRead);

        /* Read from the orc file */
        if (!readAndFilter(rowsSkip, onceRead)) {
            fillVectorBatch(onceRead, batch, fileOffset, startOffset);
        }

        /* refresh the state after read one value. */
        rowsSkip = 0;
        rowsToRead -= onceRead;
        startOffset += onceRead;
        rowsReadInCurrentStripe += onceRead;
        currentStrideIdx = rowsReadInCurrentStripe / rowsInStride;
        tryStripeEnd();
    }
}

void OrcReaderImpl::copyBloomFilter()
{
    for (uint64_t i = 0; i < numberOfColumns; i++) {
        bloomFilters[static_cast<uint32_t>(i)] = staticBloomFilters[static_cast<uint32_t>(i)];
    }
}

bool OrcReaderImpl::checkBFPruning(uint32_t colID, char *colValue)
{
    if (bloomFilters[colID]) {
        Var *var = GetVarFromColumnList(readerState->allColumnList, colID + 1);
        if (var == NULL || 0 == strncmp(colValue, DEFAULT_HIVE_NULL, DEFAULT_HIVE_NULL_LENGTH)) {
            return false;
        } else {
            Datum datumValue = GetDatumFromString(var->vartype, var->vartypmod, colValue);
            if (!bloomFilters[colID]->includeDatum(datumValue)) {
                readerState->dynamicPrunFiles++;
                return true;
            }
        }
    }

    return false;
}

void OrcReaderImpl::addBloomFilter(filter::BloomFilter *_bloomFilter, int32_t colIdx, bool is_runtime)
{
    if (NULL == _bloomFilter) {
        return;
    }

    Var *var = GetVarFromColumnList(readerState->allColumnList, colIdx + 1);

    /* BF only work when the type match and for bpchar type, typemod must be the same too. */
    if ((NULL != var && _bloomFilter->getDataType() == var->vartype) &&
        ((var->vartype != BPCHAROID) || (var->vartypmod == _bloomFilter->getTypeMod()))) {
        hasBloomFilter = true;

        if (!is_runtime) {
            /* In the init time, we store the static bloom filter. */
            hasBloomFilter = true;
            if (NULL == staticBloomFilters[static_cast<uint32_t>(colIdx)]) {
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
                if (NULL != staticBloomFilters[static_cast<uint32_t>(colIdx)])
                    bloomFilters[static_cast<uint32_t>(colIdx)]->intersectAll(
                        *staticBloomFilters[static_cast<uint32_t>(colIdx)]);
            } else {
                if (NULL != bloomFilters[static_cast<uint32_t>(colIdx)])
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
            if (NULL != minMax) {
                readerState->runtimeRestrictionList = lappend(readerState->runtimeRestrictionList, minMax);
            }
            readerState->restrictRequired[colIdx] = true;

            if (_bloomFilter->getType() == HASHJOIN_BLOOM_FILTER) {
                fixOrderedColumnList(colIdx);
            }
        }
    }
}

bool OrcReaderImpl::tryDynamicPrunning()
{
    ListCell *lc = NULL;

    /* Return false as can-not-skip when DPP is disabled */
    if (!u_sess->attr.attr_sql.enable_valuepartition_pruning)
        return false;

    foreach (lc, readerState->partList) {
        uint32_t colID = lfirst_int(lc) - 1;
        if (bloomFilters[colID]) {
            Var *var = GetVarFromColumnList(readerState->allColumnList, colID + 1);
            char *colValue = readerState->partitionColValueArr[colID];
            if (var == NULL || 0 == strncmp(colValue, DEFAULT_HIVE_NULL, DEFAULT_HIVE_NULL_LENGTH)) {
                continue;
            } else {
                Datum datumValue = GetDatumFromString(var->vartype, var->vartypmod, colValue);
                if (!bloomFilters[colID]->includeDatum(datumValue)) {
                    readerState->dynamicPrunFiles++;
                    return true;
                }
            }
        }
    }
    return false;
}

/*
 * @Description: creat struct reader with cache for inner table
 * @IN stream: input stream
 * @See also:
 */
inline void OrcReaderImpl::createStructReaderWithCacheForInnerTable(std::unique_ptr<orc::InputStream> &stream)
{
    orc::ReaderOptions opts;
    OrcMetadataValue *metaData = NULL;
    RelFileNode fileNode = readerState->scanstate->ss_currentRelation->rd_node;
    int32_t fileID = readerState->currentFileID;
    uint32_t stripeID = 0;
    uint32_t columnID = 0;
    CacheSlotId_t slotId = CACHE_BLOCK_INVALID_IDX;
    bool hasFound = false;

    pgstat_count_buffer_read(readerState->scanstate->ss_currentRelation);
    slotId = MetaCacheAllocBlock((RelFileNodeOld *)&fileNode, fileID, stripeID, columnID, hasFound, CACHE_ORC_INDEX);
    ereport(DEBUG1,
            (errmodule(MOD_ORC),
             errmsg("build struct reader: slotID(%d), spcID(%u), dbID(%u), relID(%u), fileID(%d), stripeID(%u), "
                 "columnID(%u), found(%d)",
                 slotId, fileNode.spcNode, fileNode.dbNode, fileNode.relNode, fileID, stripeID, columnID, hasFound)));

    metaData = OrcMetaCacheGetBlock(slotId);
    if (hasFound) {
        orc::proto::PostScript *ps = new orc::proto::PostScript();
        orc::proto::Footer *footer = new orc::proto::Footer();
        getFromMetadataValue(slotId, metaData, ps, footer);

        structReader = orc::createReader(std::move(stream), opts, std::unique_ptr<orc::proto::PostScript>(ps),
                                         std::unique_ptr<orc::proto::Footer>(footer), metaData->footerStart);

        readerState->orcMetaCacheBlockCount++;
        readerState->orcMetaCacheBlockSize += OrcMetaCacheGetBlockSize(slotId);
        pgstat_count_buffer_hit(readerState->scanstate->ss_currentRelation);
    } else {
        structReader = orc::createReader(std::move(stream), opts);
        OrcMetaCacheSetBlock(slotId, structReader->getFooterStart(), structReader->getPostScript().get(),
                             structReader->getFooter().get(), NULL, NULL, NULL, NULL);

        readerState->orcMetaLoadBlockCount++;
        readerState->orcMetaLoadBlockSize += OrcMetaCacheGetBlockSize(slotId);
    }
    ReleaseMetaBlock(slotId);
}

/*
 * @Description: creat struct reader with cache for foreign table
 * @IN stream: input stream
 * @See also:
 */
inline void OrcReaderImpl::createStructReaderWithCacheForForeignTable(std::unique_ptr<orc::InputStream> &stream)
{
    orc::ReaderOptions opts;
    OrcMetadataValue *metaData = NULL;
    CacheSlotId_t slotId = CACHE_BLOCK_INVALID_IDX;
    RelFileNode fileNode = readerState->scanstate->ss_currentRelation->rd_node;
    std::string fileName = stream.get()->getName();
    uint32_t stripeID = 0;
    uint32_t columnID = 0;
    uint32_t prefixNameHash = string_hash((void *)fileName.c_str(), fileName.length() + 1);
    bool hasFound = false;
    bool collision = false;
    bool renewCollision = false;
    bool dataChg = false;
    pgstat_count_buffer_read(readerState->scanstate->ss_currentRelation);

    /*
     * we use RelFileNode & prefixNameHash as the hash key. If the RelFileNode has files
     * more than 4294967295, it could happen collision problem. To stop this happen, we
     * store OBS file prefixname at the front of metadata. When find the block, we must
     * test if collision happens. If so, we read the info directly from OBS file and not
     * update the info into cache.
     */
    slotId = MetaCacheAllocBlock((RelFileNodeOld *)&fileNode, (int32_t)prefixNameHash, stripeID, columnID, hasFound,
                                 CACHE_ORC_INDEX);

    metaData = OrcMetaCacheGetBlock(slotId);

    if (hasFound) {
        Assert(metaData->fileName);
        Assert(metaData->dataDNA);
        Assert(readerState->currentSplit->eTag);

        size_t fileNameLen = fileName.length();
        size_t dataDNALen = strlen(readerState->currentSplit->eTag);

        /* judge if we collision */
        if (strncmp(fileName.c_str(), metaData->fileName, fileNameLen))
            collision = true;
        else {
            if (strncmp(readerState->currentSplit->eTag, metaData->dataDNA, dataDNALen))
                dataChg = true;
            else {
                orc::proto::PostScript *ps = new orc::proto::PostScript();
                orc::proto::Footer *footer = new orc::proto::Footer();
                getFromMetadataValue(slotId, metaData, ps, footer);

                structReader = orc::createReader(std::move(stream), opts, std::unique_ptr<orc::proto::PostScript>(ps),
                                                 std::unique_ptr<orc::proto::Footer>(footer), metaData->footerStart);

                readerState->orcMetaCacheBlockCount++;
                readerState->orcMetaCacheBlockSize += OrcMetaCacheGetBlockSize(slotId);
                pgstat_count_buffer_hit(readerState->scanstate->ss_currentRelation);

                ReleaseMetaBlock(slotId);
            }
        }
    }

    /* OBS file content has changed, refresh it */
    if (dataChg && !MetaCacheRenewBlock(slotId))
        renewCollision = true;

    /* not find in cache and eTag doesn't match when we need update cache */
    if (!hasFound || (dataChg && !renewCollision)) {
        structReader = orc::createReader(std::move(stream), opts);
        OrcMetaCacheSetBlock(slotId, structReader->getFooterStart(), structReader->getPostScript().get(),
                             structReader->getFooter().get(), NULL, NULL, fileName.c_str(),
                             readerState->currentSplit->eTag);
        readerState->orcMetaLoadBlockCount++;
        readerState->orcMetaLoadBlockSize += OrcMetaCacheGetBlockSize(slotId);
        ReleaseMetaBlock(slotId);
    } else if (collision || (dataChg && renewCollision)) { /* renewCollision is ture */
        ReleaseMetaBlock(slotId);
        structReader = orc::createReader(std::move(stream), opts);
        readerState->orcMetaLoadBlockCount++;
        readerState->orcMetaLoadBlockSize += 0;
    }
}

void OrcReaderImpl::createStructReaderWithCache()
{
    orc::ReaderOptions opts;
    std::unique_ptr<dfs::GSInputStream> stream;
    stream = dfs::InputStreamFactory(conn, currentFilePath, readerState, g_instance.attr.attr_sql.enable_orc_cache);

    newStream = stream.get();

    std::unique_ptr<orc::InputStream> orc_stream(new orc::OrcInputStreamAdapter(std::move(stream)));

    int32_t connect_type = conn->getType();

    if (readerState->currentFileID >= 0 && g_instance.attr.attr_sql.enable_orc_cache) {
        createStructReaderWithCacheForInnerTable(orc_stream);
    } else if (FOREIGNTABLEFILEID == readerState->currentFileID && OBS_CONNECTOR == connect_type &&
               g_instance.attr.attr_sql.enable_orc_cache) { /* here we go, OBS foreign table. */
        createStructReaderWithCacheForForeignTable(orc_stream);
    } else {
        structReader = orc::createReader(std::move(orc_stream), opts);
        readerState->orcMetaLoadBlockCount++;
    }

    return;
}

void OrcReaderImpl::buildStructReader()
{
    /* Start to load the file. */
    DFS_TRY()
    {
        createStructReaderWithCache();
    }
    DFS_CATCH();
    DFS_ERRREPORT_WITHARGS(
        "Error occurs while creating an orc reader for file %s because of %s, detail can be found in dn log of %s.",
        MOD_ORC, currentFilePath);
    Assert(NULL != structReader.get());

    /* check the number of rows first */
    numberOfRows = structReader->getNumberOfRows();
    numberOfOrcCols = structReader->getType().getSubtypeCount();
    numberOfStripes = structReader->getNumberOfStripes();
    if (0 == numberOfRows || 0 == numberOfStripes) {
        return;
    }

    /*
     * For foreign table, the number of columns in the file can be bigger or less than the relation defined.
     */
    if (RelationIsForeignTable(readerState->scanstate->ss_currentRelation)) {
        setNonExistColumns();
    } else if (numberOfFields < numberOfOrcCols) {
        /*
         * Now, the "Drop/Add column" feature is supported, the numberOfFields should be
         * greater than or equal to getSelectedColumns().size() - 1.
         */
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_ORC),
                        errmsg("Column count in table definition does not match with ORC file %s.", currentFilePath)));
    }

    rowsReadInCurrentStripe = 0;
    currentStripeIdx = 0;
    currentStrideIdx = 0;
    rowsInStride = structReader->getRowIndexStride();
}

void OrcReaderImpl::setNonExistColumns()
{
    bool *orcReadRequired = readerState->readRequired;
    List **predicateArr = readerState->hdfsScanPredicateArr;
    for (uint32_t i = 0; i < readerState->relAttrNum; i++) {
        if (orcReadRequired[i] && (readerState->readColIDs[i] + 1 > numberOfOrcCols)) {
            /* modify here, and will impact dfs_am layer. */
            orcReadRequired[i] = false;

            uint32_t arrayIndex = readerState->globalColIds[i];
            if (predicateArr[arrayIndex] != NULL) {
                bool isNull = false;
                ScanState *scanState = readerState->scanstate;
                Var *var = GetVarFromColumnList(readerState->allColumnList, i + 1);
                Datum result = heapGetInitDefVal(i + 1, scanState->ss_currentRelation->rd_att, &isNull);
                if (var == NULL || false == defColSatisfyPredicates(isNull, result, var, predicateArr[arrayIndex])) {
                    numberOfRows = 0;
                    break;
                }
            }
        }
    }
}

void OrcReaderImpl::initializeColumnReaders()
{
    uint32_t *orcColIDs = readerState->readColIDs;
    uint32_t *mppColIDs = readerState->globalColIds;
    bool *orcRequired = readerState->readRequired;

    for (uint32_t i = 0; i < numberOfColumns; i++) {
        if (orcRequired[i]) {
            std::unique_ptr<dfs::GSInputStream> streamCopy;
            DFS_TRY()
            {
                streamCopy = newStream->copy();
            }
            DFS_CATCH();
            DFS_ERRREPORT_WITHARGS(
                "Error occurs while opening hdfs file %s because of %s, detail can be found in dn log of %s.",
                MOD_ORC, currentFilePath);

            const Var *var = GetVarFromColumnList(readerState->allColumnList, static_cast<int32_t>(i + 1));
            OrcColumnReader *readerImpl = NULL;

            /*
             * The orcColIDs maps the column index defined in MPP to
             * the column index stored in ORC file. Here add one because
             * the real column index Stored in ORC starts with 1 instead
             * of 0.
             */
            switch (structReader->getType().getSubtype(orcColIDs[i]).getKind()) {
                case orc::LONG: {
                    readerImpl = New(readerState->persistCtx) OrcColumnReaderImpl<orc::LONG>(orcColIDs[i] + 1,
                        mppColIDs[i] + 1, readerState, var);
                    break;
                }
                case orc::INT: {
                    readerImpl = New(readerState->persistCtx) OrcColumnReaderImpl<orc::INT>(orcColIDs[i] + 1,
                        mppColIDs[i] + 1, readerState, var);
                    break;
                }
                case orc::SHORT: {
                    readerImpl = New(readerState->persistCtx) OrcColumnReaderImpl<orc::SHORT>(orcColIDs[i] + 1,
                        mppColIDs[i] + 1, readerState, var);
                    break;
                }
                case orc::BYTE: {
                    readerImpl = New(readerState->persistCtx) OrcColumnReaderImpl<orc::BYTE>(orcColIDs[i] + 1,
                        mppColIDs[i] + 1, readerState, var);
                    break;
                }
                case orc::FLOAT: {
                    readerImpl = New(readerState->persistCtx) OrcColumnReaderImpl<orc::FLOAT>(orcColIDs[i] + 1,
                        mppColIDs[i] + 1, readerState, var);
                    break;
                }
                case orc::DOUBLE: {
                    readerImpl = New(readerState->persistCtx) OrcColumnReaderImpl<orc::DOUBLE>(orcColIDs[i] + 1,
                        mppColIDs[i] + 1, readerState, var);
                    break;
                }
                case orc::BOOLEAN: {
                    readerImpl = New(readerState->persistCtx) OrcColumnReaderImpl<orc::BOOLEAN>(orcColIDs[i] + 1,
                        mppColIDs[i] + 1, readerState, var);
                    break;
                }
                case orc::CHAR: {
                    readerImpl = New(readerState->persistCtx) OrcColumnReaderImpl<orc::CHAR>(orcColIDs[i] + 1,
                        mppColIDs[i] + 1, readerState, var);
                    break;
                }
                case orc::VARCHAR: {
                    readerImpl = New(readerState->persistCtx) OrcColumnReaderImpl<orc::VARCHAR>(orcColIDs[i] + 1,
                        mppColIDs[i] + 1, readerState, var);
                    break;
                }
                case orc::STRING: {
                    readerImpl = New(readerState->persistCtx) OrcColumnReaderImpl<orc::STRING>(orcColIDs[i] + 1,
                        mppColIDs[i] + 1, readerState, var);
                    break;
                }
                case orc::DATE: {
                    readerImpl = New(readerState->persistCtx) OrcColumnReaderImpl<orc::DATE>(orcColIDs[i] + 1,
                        mppColIDs[i] + 1, readerState, var);
                    break;
                }
                case orc::TIMESTAMP: {
                    readerImpl = New(readerState->persistCtx) OrcColumnReaderImpl<orc::TIMESTAMP>(orcColIDs[i] + 1,
                        mppColIDs[i] + 1,  readerState, var);
                    break;
                }
                case orc::DECIMAL: {
                    readerImpl = New(readerState->persistCtx) OrcColumnReaderImpl<orc::DECIMAL>(orcColIDs[i] + 1,
                    mppColIDs[i] + 1, readerState, var);
                    break;
                }
                default: {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_ORC),
                                    errmsg("Unsupported orc type : %u.",
                                           structReader->getType().getSubtype(orcColIDs[i]).getKind())));
                    break;
                }
            }
            std::unique_ptr<orc::InputStream> orc_stream(new orc::OrcInputStreamAdapter(std::move(streamCopy)));
            readerImpl->begin(std::move(orc_stream), structReader.get(), maxRowsReadOnce);
            columnReader[orcColIDs[i]] = readerImpl;
        }
    }
}

inline uint64_t OrcReaderImpl::computeRowsToRead(uint64_t maxRows)
{
    uint64_t rowsToRead = Min(maxRows, rowsInCurrentStripe - rowsReadInCurrentStripe);
    errno_t rc = memset_s(isSelected, maxRows, true, rowsToRead);
    securec_check(rc, "\0", "\0");
    return rowsToRead;
}

/*
 * @Description: get stripe footer from cache for inner table
 */
inline const orc::proto::StripeFooter *OrcReaderImpl::getStripeFooterFromCacheForInnerTable()
{
    const orc::proto::StripeFooter *stripeFooter = NULL;
    RelFileNode fileNode = readerState->scanstate->ss_currentRelation->rd_node;
    int32_t fileID = readerState->currentFileID;
    uint32_t stripeID = currentStripeIdx + 1;
    uint32_t columnID = 0;
    bool found = false;

    pgstat_count_buffer_read(readerState->scanstate->ss_currentRelation);
    CacheSlotId_t slotId = MetaCacheAllocBlock((RelFileNodeOld *)&fileNode, fileID, stripeID, columnID, found,
                                               CACHE_ORC_INDEX);
    ereport(DEBUG1,
            (errmodule(MOD_ORC), errmsg("set stripe footer : slotID(%d), spcID(%u), dbID(%u), " 
                "relID(%u), fileID(%d), stripeID(%u), columnID(%u), found(%d)",
                slotId, fileNode.spcNode, fileNode.dbNode, fileNode.relNode, fileID, stripeID, columnID, found)));

    OrcMetadataValue *metaData = OrcMetaCacheGetBlock(slotId);
    if (found) {
        READER_SETSTRIPEFOOTER(structReader, currentStripeIdx, metaData->stripeFooter);
        stripeFooter = structReader->getCurrentStripeFooter();
        readerState->orcMetaCacheBlockCount++;
        readerState->orcMetaCacheBlockSize += OrcMetaCacheGetBlockSize(slotId);
        pgstat_count_buffer_hit(readerState->scanstate->ss_currentRelation);
    } else {
        READER_SETSTRIPEFOOTER(structReader, currentStripeIdx, NULL);
        stripeFooter = structReader->getCurrentStripeFooter();
        OrcMetaCacheSetBlock(slotId, 0, NULL, NULL, stripeFooter, NULL, NULL, NULL);
        readerState->orcMetaLoadBlockCount++;
        readerState->orcMetaLoadBlockSize += OrcMetaCacheGetBlockSize(slotId);
    }
    ReleaseMetaBlock(slotId);

    return stripeFooter;
}

/*
 * @Description: get stripe footer from cache for forign table
 */
inline const orc::proto::StripeFooter *OrcReaderImpl::getStripeFooterFromCacheForForeignTable()
{
    const orc::proto::StripeFooter *stripeFooter = NULL;

    RelFileNode fileNode = readerState->scanstate->ss_currentRelation->rd_node;
    uint32_t stripeID = currentStripeIdx + 1;
    uint32_t columnID = 0;
    bool found = false;
    bool collision = false;
    std::string fileName = newStream->getName();

    pgstat_count_buffer_read(readerState->scanstate->ss_currentRelation);
    uint32_t prefixNameHash = string_hash((void *)fileName.c_str(), fileName.length() + 1);
    CacheSlotId_t slotId = MetaCacheAllocBlock((RelFileNodeOld *)&fileNode, (int32_t)prefixNameHash, stripeID, columnID,
                                               found, CACHE_ORC_INDEX);

    /*
     * we use RelFileNode & prefixNameHash as the hash key. If the RelFileNode has files
     * more than 4294967295, it could happen collision problem. To stop this happen, we
     * store OBS file prefixname at the front of metadata. When find the block, we must
     * test if collision happens. If so, we read the info directly from OBS file and not
     * update the info into cache.
     */
    OrcMetadataValue *metaData = OrcMetaCacheGetBlock(slotId);
    bool dataChg = false;
    bool renewCollision = false;

    if (found) {
        Assert(metaData->fileName);
        Assert(metaData->dataDNA);
        Assert(readerState->currentSplit->eTag);

        size_t fileNameLen = fileName.length();
        size_t dataDNALen = strlen(readerState->currentSplit->eTag);

        /* judge if we collision */
        if (strncmp(fileName.c_str(), metaData->fileName, fileNameLen))
            collision = true;
        else {
            if (strncmp(readerState->currentSplit->eTag, metaData->dataDNA, dataDNALen))
                dataChg = true;
            else {
                READER_SETSTRIPEFOOTER(structReader, currentStripeIdx, metaData->stripeFooter);
                stripeFooter = structReader->getCurrentStripeFooter();
                readerState->orcMetaCacheBlockCount++;
                readerState->orcMetaCacheBlockSize += OrcMetaCacheGetBlockSize(slotId);
                pgstat_count_buffer_hit(readerState->scanstate->ss_currentRelation);
                ReleaseMetaBlock(slotId);
            }
        }
    }

    /* OBS file content has changed, refresh it */
    if (dataChg && !MetaCacheRenewBlock(slotId))
        renewCollision = true;

    /* not find in cache and eTag doesn't match when we need update cache */
    if (!found || (dataChg && !renewCollision)) {
        READER_SETSTRIPEFOOTER(structReader, currentStripeIdx, NULL);
        stripeFooter = structReader->getCurrentStripeFooter();
        OrcMetaCacheSetBlock(slotId, 0, NULL, NULL, stripeFooter, NULL, fileName.c_str(),
                             readerState->currentSplit->eTag);

        readerState->orcMetaLoadBlockCount++;
        readerState->orcMetaLoadBlockSize += OrcMetaCacheGetBlockSize(slotId);
        ReleaseMetaBlock(slotId);
    } else if (collision || (dataChg && renewCollision)) {
        ReleaseMetaBlock(slotId);
        READER_SETSTRIPEFOOTER(structReader, currentStripeIdx, NULL);
        stripeFooter = structReader->getCurrentStripeFooter();
        readerState->orcMetaLoadBlockCount++;
        readerState->orcMetaLoadBlockSize += 0;
    }

    return stripeFooter;
}

/*
 * @Description: Load the row index's information from file for the current stripe.
 * @IN loadBF: flag indicates whether we should load the bloom filter information
 */
inline void OrcReaderImpl::setStripeRowIndex(bool loadBF)
{
    uint32_t *orcColIDs = readerState->readColIDs;
    bool *orcRequired = readerState->readRequired;

    int32_t fileID = readerState->currentFileID;
    const orc::proto::StripeFooter *stripeFooter = NULL;

    int32_t connect_type = conn->getType();

    if (fileID >= 0 && g_instance.attr.attr_sql.enable_orc_cache) {
        stripeFooter = getStripeFooterFromCacheForInnerTable();
    } else if (FOREIGNTABLEFILEID == fileID && g_instance.attr.attr_sql.enable_orc_cache &&
               OBS_CONNECTOR == connect_type) {
        stripeFooter = getStripeFooterFromCacheForForeignTable();
    } else {
        READER_SETSTRIPEFOOTER(structReader, currentStripeIdx, NULL);
        stripeFooter = structReader->getCurrentStripeFooter();
        readerState->orcMetaLoadBlockCount++;
    }

    for (uint64_t i = 0; i < numberOfColumns; i++) {
        if (orcRequired[i]) {
            columnReader[orcColIDs[i]]->setRowIndex(currentStripeIdx, (loadBF && (NULL != bloomFilters[i])),
                                                    stripeFooter);
        }
    }
}

const dfs::DFSConnector *OrcReaderImpl::getConn() const
{
    return conn;
}

/*
 * @Description: For index tid scan, we skip the process of stripe/stride skip, just
 *		reset the status after skipping some rows.
 * @IN rowsSkip: the rows to skip firstly
 */
void OrcReaderImpl::resetStripeAndStrideStat(uint64_t rowsSkip)
{
    bool newStripe = false;

    /* For the first stripe. */
    if (0 == rowsReadInCurrentStripe) {
        rowsInCurrentStripe = structReader->getStripe(currentStripeIdx)->getNumberOfRows();
        newStripe = true;
    }

    /* Skip rows until we find the target stripe which includes the tid. */
    while (rowsReadInCurrentStripe + rowsSkip >= rowsInCurrentStripe) {
        rowsSkip -= rowsInCurrentStripe - rowsReadInCurrentStripe;
        currentStripeIdx++;
        rowsReadInCurrentStripe = 0;
        rowsInCurrentStripe = structReader->getStripe(currentStripeIdx)->getNumberOfRows();
        newStripe = true;
    }

    /* Refresh the stripe status. */
    numberOfStrides = (rowsInCurrentStripe + (rowsInStride - 1)) / rowsInStride;
    rowsReadInCurrentStripe += rowsSkip;
    currentStrideIdx = rowsReadInCurrentStripe / rowsInStride;

    /* Set the row index of the current stripe. */
    if (newStripe) {
        setStripeRowIndex(false);
    }
}

bool OrcReaderImpl::tryStripeSkip(uint64_t &rowsSkip, uint64_t &rowsCross)
{
    if (stripeEOF()) {
        rowsInCurrentStripe = structReader->getStripe(currentStripeIdx)->getNumberOfRows();
        numberOfStrides = (rowsInCurrentStripe + (rowsInStride - 1)) / rowsInStride;
        Assert(numberOfStrides > 0);

        if (hasRestriction() && !checkPredicateOnCurrentStripe()) {
            skipCurrentStripe(rowsSkip, rowsCross);
            return true;
        }

        setStripeRowIndex(true);
    }

    return false;
}

inline bool OrcReaderImpl::tryStrideSkip(uint64_t &rowsSkip, uint64_t &rowsCross)
{
    if (hasRestriction() && strideEOF() && !checkPredicateOnCurrentStride()) {
        skipCurrentStride(rowsSkip, rowsCross);
        tryStripeEnd();
        return true;
    }
    return false;
}

void OrcReaderImpl::combineDeleteMap(DFSDesc *dfsDesc, uint64_t rowsInFile, uint64_t rowsToRead)
{
    if (NULL != dfsDesc) {
        for (uint64_t i = 0; i < rowsToRead; i++) {
            isSelected[i] = !dfsDesc->IsDeleted(rowsInFile + i);
        }
    }
}

bool OrcReaderImpl::readAndFilter(uint64_t rowsSkip, uint64_t rowsToRead)
{
    uint32_t i = 0;
    uint32_t mppColID = 0;
    uint32_t orcColID = 0;
    uint32_t *orderedCols = readerState->orderedCols;
    uint32_t *orcColIDs = readerState->readColIDs;
    bool skipBatch = false;
    bool checkSkipBatch = false;
    bool *orcRequired = readerState->readRequired;

    for (i = 0; i < numberOfColumns; i++) {
        mppColID = orderedCols[i];
        if (orcRequired[mppColID]) {
            orcColID = orcColIDs[mppColID];

            /* Skip the rows of skipped stripes and strides for the column. */
            if (rowsSkip > 0) {
                columnReader[orcColID]->skip(rowsSkip);
            }

            /* Read or skip the column batch rows. */
            skipBatch = skipBatch ? true : (checkSkipBatch ? canSkipBatch(rowsToRead) : false);
            if (skipBatch) {
                columnReader[orcColID]->skip(rowsToRead);
            } else {
                columnReader[orcColID]->nextInternal(rowsToRead);
            }

            /* Filter the column batch with the predicate. */
            checkSkipBatch = false;
            if (!skipBatch && columnReader[orcColID]->hasPredicate()) {
                columnReader[orcColID]->predicateFilter(rowsToRead, isSelected);
                checkSkipBatch = true;
            }
        }
    }

    /* Check if the batch can be skipped in the end. */
    skipBatch = skipBatch ? true : (checkSkipBatch ? canSkipBatch(rowsToRead) : false);

    return skipBatch;
}

void OrcReaderImpl::fillVectorBatch(uint64_t rowsToRead, VectorBatch *batch, FileOffset *fileOffset,
                                    uint64_t rowsInFile)
{
    uint32_t i = 0;
    uint32_t mppColID = 0;
    uint32_t orcColID = 0;
    uint32_t selectedRows = 0;
    uint32_t *orderedCols = readerState->orderedCols;
    uint32_t *orcColIDs = readerState->readColIDs;
    bool *orcRequired = readerState->readRequired;
    bool *targetRequired = readerState->targetRequired;

    /* Set the m_rows according to the isSelected array. */
    for (i = 0; i < rowsToRead; i++) {
        if (isSelected[i]) {
            selectedRows++;
        }
    }
    batch->m_rows += selectedRows;

    /* Fill the scalarVector column by column. */
    for (i = 0; i < numberOfColumns; i++) {
        mppColID = orderedCols[i];
        if (orcRequired[mppColID] && targetRequired[mppColID]) {
            orcColID = orcColIDs[mppColID];
            (void)columnReader[orcColID]->fillScalarVector(rowsToRead, isSelected, &batch->m_arr[mppColID]);
        }
    }

    /* Fill file offset array. */
    if (NULL != fileOffset) {
        fillFileOffset(rowsInFile, rowsToRead, fileOffset);
    }
}

void OrcReaderImpl::fillFileOffset(uint64_t rowsInFile, uint64_t rowsToRead, FileOffset *fileOffset) const
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

inline void OrcReaderImpl::refreshState(uint64_t rowsToRead, uint64_t &rowsSkip, uint64_t &rowsCross)
{
    rowsReadInCurrentStripe += rowsToRead;
    currentStrideIdx = rowsReadInCurrentStripe / rowsInStride;
    rowsCross += rowsToRead;
    rowsSkip = 0;
    tryStripeEnd();
}

void OrcReaderImpl::fixOrderedColumnList(uint32_t colIdx)
{
    uint32_t bound = 0;
    uint32_t *orderedCols = readerState->orderedCols;
    bool *restrictRequired = readerState->restrictRequired;

    for (bound = 0; bound < numberOfColumns; bound++) {
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

void OrcReaderImpl::setColBloomFilter()
{
    for (uint32_t i = 0; i < numberOfColumns; i++) {
        filter::BloomFilter *bloomFilter = bloomFilters[i];
        if (NULL != bloomFilter) {
            if (readerState->readRequired[i]) {
                uint32_t orcColID = readerState->readColIDs[i];
                columnReader[orcColID]->setBloomFilter(bloomFilter);
            }
        }
    }
}

inline void OrcReaderImpl::skipCurrentStripe(uint64_t &rowsSkip, uint64_t &rowsCross)
{
    currentStripeIdx++;
    rowsReadInCurrentStripe = 0;
    currentStrideIdx = 0;
    rowsSkip += rowsInCurrentStripe;
    rowsCross += rowsInCurrentStripe;
}

inline void OrcReaderImpl::skipCurrentStride(uint64_t &rowsSkip, uint64_t &rowsCross)
{
    currentStrideIdx++;
    uint64_t realSkip = Min(rowsInStride, rowsInCurrentStripe - rowsReadInCurrentStripe);
    rowsReadInCurrentStripe += realSkip;
    rowsSkip += realSkip;
    rowsCross += realSkip;
}

inline void OrcReaderImpl::tryStripeEnd()
{
    if (rowsReadInCurrentStripe == rowsInCurrentStripe) {
        currentStripeIdx++;
        rowsReadInCurrentStripe = 0;
        currentStrideIdx = 0;
    }
}

inline bool OrcReaderImpl::checkPredicateOnCurrentFile(List *descFileRestriction)
{
    List *fileRestriction = buildRestriction(FILE);
    List *fileRestrictionOrigin = fileRestriction;  // store the pointer in order to free the memory
    fileRestriction = list_difference(fileRestriction, descFileRestriction);
    bool ret = (!predicate_refuted_by(fileRestriction, readerState->queryRestrictionList, false) &&
                !predicate_refuted_by(fileRestriction, readerState->runtimeRestrictionList, true));
    if (!ret) {
        /* these rows will be skipped, so count them */
        readerState->minmaxFilterRows += numberOfRows;
        /* this file will be skipped, so count it */
        readerState->minmaxFilterFiles++;
    }
    list_free_ext(fileRestriction);
    list_free_ext(fileRestrictionOrigin);
    return ret;
}

inline bool OrcReaderImpl::checkPredicateOnCurrentStripe()
{
    /*
     * ORC 0.11 don't support storing min/max on stripe level.
     * When the number of stripe equals 1, the min/max of file is the same
     * with the min/max of stripe. So we skip the min/max check for stripe
     * if the number of stripe is 1.
     */
    if (structReader->getFormatVersion() == "0.11" || 1 == numberOfStripes)
        return true;

    List *stripeRestriction = buildRestriction(STRIPE, currentStripeIdx);
    bool ret = (!predicate_refuted_by(stripeRestriction, readerState->queryRestrictionList, false) &&
                !predicate_refuted_by(stripeRestriction, readerState->runtimeRestrictionList, true));
    if (!ret) {
        /* these rows will be skipped, so count them */
        readerState->minmaxFilterRows += rowsInCurrentStripe;
        /* this stripe will be skipped, so count it */
        readerState->minmaxFilterStripe++;
    }
    readerState->minmaxCheckStripe++;
    list_free_ext(stripeRestriction);
    return ret;
}

inline bool OrcReaderImpl::checkPredicateOnCurrentStride()
{
    /*
     * If there is only one stride in the stripe, the min/max is equal to the min/max
     * of the stripe, so we can skip the stride check here.
     */
    if (1 == numberOfStrides)
        return true;

    List *strideRestriction = buildRestriction(STRIDE, currentStripeIdx, currentStrideIdx);
    bool ret = (!predicate_refuted_by(strideRestriction, readerState->queryRestrictionList, false) &&
                !predicate_refuted_by(strideRestriction, readerState->runtimeRestrictionList, true));
    if (!ret) {
        /* these rows will be skipped, so count them */
        readerState->minmaxFilterRows += Min(rowsInCurrentStripe - rowsReadInCurrentStripe, rowsInStride);
        /* this stride will be skipped, so count it */
        readerState->minmaxFilterStride++;
    }
    readerState->minmaxCheckStride++;
    list_free_ext(strideRestriction);
    return ret && checkBloomFilter();
}

bool OrcReaderImpl::checkBloomFilter() const
{
    if (!hasBloomFilter) {
        return true;
    }

    uint32_t *orcColIDs = readerState->readColIDs;
    bool *readRequired = readerState->readRequired;

    for (uint32_t i = 0; i < numberOfColumns; i++) {
        if (NULL != bloomFilters[i]) {
            /*
             * Here, we may skip a case where a column with bloomfilter assigned
             * but marked as 'None-Required', for example a partitioned column.
             *
             * For this kind of BF-filtering, we handle partition column at ORC
             * file level rather than each stride.
             */
            if (readRequired[i] && !columnReader[orcColIDs[i]]->checkBloomFilter(currentStrideIdx)) {
                ++(readerState->bloomFilterBlocks);
                readerState->bloomFilterRows += Min(rowsInCurrentStripe - rowsReadInCurrentStripe, rowsInStride);
                return false;
            }
        }
    }

    return true;
}

List *OrcReaderImpl::buildRestriction(RestrictionType type, uint64_t stripeIdx, uint64_t strideIdx)
{
    List *fileRestrictionList = NULL;
    uint32_t *orcColIDs = readerState->readColIDs;
    bool *orcRequired = readerState->readRequired;
    bool *restrictRequired = readerState->restrictRequired;
    Assert(stripeIdx < numberOfStripes);

    for (uint32_t i = 0; i < numberOfColumns; i++) {
        Node *baseRestriction = NULL;

        if (restrictRequired[i] && orcRequired[i]) {
            baseRestriction = columnReader[orcColIDs[i]]->buildColRestriction(type, structReader.get(), stripeIdx,
                                                                              strideIdx);
            if (NULL != baseRestriction) {
                fileRestrictionList = lappend(fileRestrictionList, baseRestriction);
            }
        }
    }

    return fileRestrictionList;
}

inline bool OrcReaderImpl::canSkipBatch(uint64_t rowsToRead) const
{
    for (uint64_t i = 0; i < rowsToRead; i++) {
        if (isSelected[i]) {
            return false;
        }
    }
    return true;
}

inline bool OrcReaderImpl::stripeEOF() const
{
    return rowsReadInCurrentStripe == 0;
}

inline bool OrcReaderImpl::strideEOF() const
{
    return rowsReadInCurrentStripe % rowsInStride == 0;
}

inline bool OrcReaderImpl::hasRestriction() const
{
    return readerState->queryRestrictionList != NULL;
}

/* Collec the statistics while reading file */
void OrcReaderImpl::collectFileReadingStatus()
{
    if (NULL != newStream && NULL != structReader.get()) {
        uint64_t local = 0;
        uint64_t remote = 0;
        uint64_t nnCalls = 0;
        uint64_t dnCalls = 0;

        newStream->getStat(&local, &remote, &nnCalls, &dnCalls);
        if (remote > 0) {
            readerState->remoteBlock++;
        } else {
            readerState->localBlock++;
        }

        readerState->nnCalls += nnCalls;
        readerState->dnCalls += dnCalls;
    }
}

/* columnNo starts from 1 */
template <FieldKind fieldKind>
OrcColumnReaderImpl<fieldKind>::OrcColumnReaderImpl(int64_t _columnIdx, int64_t _mppColumnIdx,
                                                    ReaderState *_readerState, const Var *_var)
    : readerState(_readerState), lessThanExpr(NULL), greaterThanExpr(NULL), columnIdx(_columnIdx),
      mppColumnIdx(_mppColumnIdx), var(_var), predicate(NIL), useDecimal128(false),
      checkEncodingLevel(readerState->checkEncodingLevel), skipRowsBuffer(0), epochOffsetDiff(0), bloomFilter(NULL),
      convertToDatumFunc(NULL), columnReaderCtx(NULL), scale(0), checkBloomFilterOnRow(false),
      checkPredicateOnRow(false), fileName(NULL)
{
    /* do nothing */
}

template <FieldKind fieldKind>
void OrcColumnReaderImpl<fieldKind>::begin(std::unique_ptr<orc::InputStream> stream, const orc::Reader *structReader,
                                           const uint64_t maxBatchSize)
{
    orc::ReaderOptions opts;
    std::list<int64_t> cols;
    cols.push_back(columnIdx);
    (void)opts.include(cols);
    if (readerState->currentFileID == FOREIGNTABLEFILEID)
        (void)opts.forcedScaleForElkDecimal(false);  // foreign table
    else
        (void)opts.forcedScaleForElkDecimal(true);  // native hdfs table

    /* The epoch offset between hive and PG is different. */
    epochOffsetDiff = (opts.getEpochOffset() - PG_EPOCH_OFFSET_DEFAULT);
    predicate = readerState->hdfsScanPredicateArr[mppColumnIdx - 1];
    checkPredicateOnRow = list_length(predicate) > 0 ? true : false;
    columnReaderCtx = AllocSetContextCreate(readerState->persistCtx, "orc column reader context",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext oldContext = MemoryContextSwitchTo(columnReaderCtx);
    lessThanExpr = MakeOperatorExpression(const_cast<Var *>(var), BTLessEqualStrategyNumber);
    greaterThanExpr = MakeOperatorExpression(const_cast<Var *>(var), BTGreaterEqualStrategyNumber);

    /* be careful: we must append stream->getName to fileName before move operation */
    fileName = (char *)palloc0(stream->getName().length() + 1);
    error_t rc = memcpy_s(fileName, stream->getName().length(), stream->getName().c_str(), stream->getName().length());
    securec_check(rc, "", "");

    initColumnReader(std::move(stream), structReader, maxBatchSize, opts);

    (void)MemoryContextSwitchTo(oldContext);
}

template <FieldKind fieldKind>
void OrcColumnReaderImpl<fieldKind>::initColumnReader(std::unique_ptr<orc::InputStream> stream,
    const orc::Reader *structReader, const uint64_t maxBatchSize, const orc::ReaderOptions &opts)
{
    DFS_TRY()
    {
        std::unique_ptr<orc::proto::PostScript> postscript = structReader->getPostScript();
        std::unique_ptr<orc::proto::Footer> footer = structReader->getFooter();
        reader = orc::createReader(std::move(stream), opts, std::move(postscript), std::move(footer),
                                   structReader->getFooterStart());
        primitiveBatch = reader->createRowBatch(maxBatchSize);
    }
    DFS_CATCH();
    DFS_ERRREPORT_WITHARGS(
        "Error occurs while creating an orc reader for file %s because of %s, detail can be found in dn log of %s.",
        MOD_ORC, readerState->currentSplit->filePath);

    scale = reader->getType().getScale();
    if (!matchOrcWithPsql()) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_ORC),
                        errmsg("Error occurred while reading column %d: ORC and mpp "
                               "types do not match, ORC type is %s and mpp type is %s.",
                               var->varattno, reader->getType().toString().c_str(),
                               format_type_with_typemod(var->vartype, var->vartypmod))));
    }

    if (orc::DECIMAL == reader->getType().getKind() &&
        (reader->getType().getPrecision() == 0 || reader->getType().getPrecision() > 18)) {
        useDecimal128 = true;
        bindConvertFunc<true>();
    } else {
        useDecimal128 = false;
        bindConvertFunc<false>();
    }
}

template <FieldKind fieldKind>
template <bool decimal128>
void OrcColumnReaderImpl<fieldKind>::bindConvertFunc()
{
    switch (var->vartype) {
        case INTERVALOID:
            convertToDatumFunc = &convertToDatumT<fieldKind, INTERVALOID, READER>;
            break;
        case OIDOID:
            convertToDatumFunc = &convertToDatumT<fieldKind, OIDOID, READER>;
            break;
        case TINTERVALOID:
            convertToDatumFunc = &convertToDatumT<fieldKind, TINTERVALOID, READER>;
            break;
        case TIMESTAMPTZOID:
            convertToDatumFunc = &convertToDatumT<fieldKind, TIMESTAMPTZOID, READER>;
            break;
        case TIMEOID:
            convertToDatumFunc = &convertToDatumT<fieldKind, TIMEOID, READER>;
            break;
        case TIMETZOID:
            convertToDatumFunc = &convertToDatumT<fieldKind, TIMETZOID, READER>;
            break;
        case CHAROID:
            convertToDatumFunc = &convertToDatumT<fieldKind, CHAROID, READER>;
            break;
        case NVARCHAR2OID:
            convertToDatumFunc = &convertToDatumT<fieldKind, NVARCHAR2OID, READER>;
            break;
        case NUMERICOID:
            if (orc::DECIMAL == fieldKind) {
                if (u_sess->attr.attr_sql.enable_fast_numeric)
                    convertToDatumFunc = &convertDecimalToDatumT<decimal128, true>;
                else
                    convertToDatumFunc = &convertDecimalToDatumT<decimal128, false>;
            } else
                convertToDatumFunc = &convertToDatumT<fieldKind, NUMERICOID, READER>;
            break;
        case SMALLDATETIMEOID:
            convertToDatumFunc = &convertToDatumT<fieldKind, SMALLDATETIMEOID, READER>;
            break;
        case CASHOID:
            convertToDatumFunc = &convertToDatumT<fieldKind, CASHOID, READER>;
            break;
        default:
            convertToDatumFunc = &convertToDatumT<fieldKind, InvalidOid, READER>;
            break;
    }
}

template <FieldKind fieldKind>
OrcColumnReaderImpl<fieldKind>::~OrcColumnReaderImpl()
{
    /*
     * Here do not call Destroy because all this object should
     * be deleted by DELETE_EX.
     */
    greaterThanExpr = NULL;
    lessThanExpr = NULL;
    predicate = NULL;
    var = NULL;
    readerState = NULL;
}

template <FieldKind fieldKind>
void OrcColumnReaderImpl<fieldKind>::Destroy()
{
    if (lessThanExpr != NULL)
        pfree_ext(lessThanExpr);
    if (greaterThanExpr != NULL)
        pfree_ext(greaterThanExpr);
    if (fileName != NULL) {
        pfree(fileName);
        fileName = NULL;
    }

    if (NULL != columnReaderCtx && columnReaderCtx != CurrentMemoryContext) {
        MemoryContextDelete(columnReaderCtx);
    }
}

template <FieldKind fieldKind>
inline void OrcColumnReaderImpl<fieldKind>::setRowIndexForInnerTable(uint64_t stripeIndex, bool hasCheck,
                                                                     const orc::proto::StripeFooter *stripeFooter)
{
    OrcMetadataValue *metaData = NULL;
    RelFileNode fileNode = readerState->scanstate->ss_currentRelation->rd_node;
    int32_t fileID = readerState->currentFileID;
    uint32_t stripeID = stripeIndex + 1;
    uint32_t columnID = mppColumnIdx + 1;
    CacheSlotId_t slotId = CACHE_BLOCK_INVALID_IDX;
    bool found = false;

    pgstat_count_buffer_read(readerState->scanstate->ss_currentRelation);
    slotId = MetaCacheAllocBlock((RelFileNodeOld *)&fileNode, fileID, stripeID, columnID, found, CACHE_ORC_INDEX);
    ereport(DEBUG1,
            (errmodule(MOD_ORC), errmsg("set row index : slotID(%d), spcID(%u), dbID(%u), relID(%u), "
                "fileID(%d), stripeID(%u), columnID(%u), found(%d)",
                slotId, fileNode.spcNode, fileNode.dbNode, fileNode.relNode, fileID, stripeID, columnID, found)));

    metaData = OrcMetaCacheGetBlock(slotId);
    if (found) {
        reader->setRowIndex(stripeIndex, hasCheck, stripeFooter, metaData->rowIndex);
        readerState->orcMetaCacheBlockCount++;
        readerState->orcMetaCacheBlockSize += OrcMetaCacheGetBlockSize(slotId);
        pgstat_count_buffer_hit(readerState->scanstate->ss_currentRelation);
    } else {
        reader->setRowIndex(stripeIndex, hasCheck, stripeFooter, NULL);
        OrcMetaCacheSetBlock(slotId, 0, NULL, NULL, NULL, reader->getCurrentRowIndex(), NULL, NULL);
        readerState->orcMetaLoadBlockCount++;
        readerState->orcMetaLoadBlockSize += OrcMetaCacheGetBlockSize(slotId);
    }
    ReleaseMetaBlock(slotId);
}

template <FieldKind fieldKind>
inline void OrcColumnReaderImpl<fieldKind>::setRowIndexForForeignTable(uint64_t stripeIndex, bool hasCheck,
                                                                       const orc::proto::StripeFooter *stripeFooter)
{
    bool found = false;
    bool dataChg = false;
    bool collision = false;
    bool renewCollision = false;
    CacheSlotId_t slotId = CACHE_BLOCK_INVALID_IDX;
    int32_t fileID = readerState->currentFileID;
    OrcMetadataValue *metaData = NULL;
    RelFileNode fileNode = readerState->scanstate->ss_currentRelation->rd_node;
    uint32_t stripeID = stripeIndex + 1;
    uint32_t columnID = mppColumnIdx + 1;
    uint32_t prefixNameHash = string_hash((void *)fileName, strlen(fileName) + 1);
    size_t fileNameLen = strlen(fileName);

    pgstat_count_buffer_read(readerState->scanstate->ss_currentRelation);

    /*
     * we use RelFileNode & prefixNameHash as the hash key. If the RelFileNode has files
     * more than 4294967295, it could happen collision problem. To stop this happen, we
     * store OBS file prefixname at the front of metadata. When find the block, we must
     * test if collision happens. If so, we read the info directly from OBS file and not
     * update the info into cache.
     */
    slotId = MetaCacheAllocBlock((RelFileNodeOld *)&fileNode, (int32_t)prefixNameHash, stripeID, columnID, found,
                                 CACHE_ORC_INDEX);
    ereport(DEBUG1, (errmodule(MOD_ORC),
             errmsg("set row index : slotID(%d), spcID(%u), dbID(%u), relID(%u), fileID(%d), stripeID(%u), "
                    "columnID(%u), found(%d)",
                    slotId, fileNode.spcNode, fileNode.dbNode, fileNode.relNode, fileID, stripeID, columnID, found)));

    metaData = OrcMetaCacheGetBlock(slotId);

    if (found) {
        Assert(metaData->fileName);
        Assert(metaData->dataDNA);
        Assert(readerState->currentSplit->eTag);

        if (strncmp(fileName, metaData->fileName, fileNameLen)) {
            collision = true;
        } else {
            size_t dataDNALen = strlen(readerState->currentSplit->eTag);
            if (strncmp(readerState->currentSplit->eTag, metaData->dataDNA, dataDNALen))
                dataChg = true;
            else {
                reader->setRowIndex(stripeIndex, hasCheck, stripeFooter, metaData->rowIndex);
                readerState->orcMetaCacheBlockCount++;
                readerState->orcMetaCacheBlockSize += OrcMetaCacheGetBlockSize(slotId);
                pgstat_count_buffer_hit(readerState->scanstate->ss_currentRelation);
                ReleaseMetaBlock(slotId);
            }
        }
    }

    /* OBS file content has changed, refresh it */
    if (dataChg && !MetaCacheRenewBlock(slotId))
        renewCollision = true;

    if (!found || (dataChg && !renewCollision)) {
        reader->setRowIndex(stripeIndex, hasCheck, stripeFooter, NULL);
        OrcMetaCacheSetBlock(slotId, 0, NULL, NULL, NULL, reader->getCurrentRowIndex(), fileName,
                             readerState->currentSplit->eTag);
        readerState->orcMetaLoadBlockCount++;
        readerState->orcMetaLoadBlockSize += OrcMetaCacheGetBlockSize(slotId);
        ReleaseMetaBlock(slotId);
    } else if (collision || (dataChg && renewCollision)) {
        ReleaseMetaBlock(slotId);
        reader->setRowIndex(stripeIndex, hasCheck, stripeFooter, NULL);
        readerState->orcMetaLoadBlockCount++;
        readerState->orcMetaLoadBlockSize += 0;
    }
}

template <FieldKind fieldKind>
void OrcColumnReaderImpl<fieldKind>::setRowIndex(uint64_t stripeIndex, bool hasCheck,
                                                 const orc::proto::StripeFooter *stripeFooter)
{
    Assert(NULL != stripeFooter);

    DFS_TRY()
    {
        if (readerState->currentFileID >= 0 && g_instance.attr.attr_sql.enable_orc_cache) {
            setRowIndexForInnerTable(stripeIndex, hasCheck, stripeFooter);
        } else if (FOREIGNTABLEFILEID == readerState->currentFileID && g_instance.attr.attr_sql.enable_orc_cache &&
                   readerState->currentSplit->eTag != NULL) {
            setRowIndexForForeignTable(stripeIndex, hasCheck, stripeFooter);
        } else {
            reader->setRowIndex(stripeIndex, hasCheck, stripeFooter, NULL);
        }
    }
    DFS_CATCH();
    DFS_ERRREPORT_WITHARGS("Error occurs while read row index of orc file %s because of %s, "
        "detail can be found in dn log of %s.",
        MOD_ORC, readerState->currentSplit->filePath);
}

template <FieldKind fieldKind>
void OrcColumnReaderImpl<fieldKind>::nextInternal(uint64_t numValues)
{
    int32_t skipRecords = skipRowsBuffer;
    DFS_TRY()
    {
        if (skipRowsBuffer > 0) {
            (void)reader->skip(skipRowsBuffer);
            skipRowsBuffer = 0;
        }

        primitiveBatch->resize(numValues);
        primitiveBatch->numElements = numValues;
        (void)reader->next(*primitiveBatch);
    }
    DFS_CATCH();
    DFS_ERRREPORT_WITHARGS("Error occurs while reading orc file %s because of %s, "
        "detail can be found in dn log of %s.",
        MOD_ORC, readerState->currentSplit->filePath);

    if (errOccur) {
        ereport(WARNING, (errmodule(MOD_ORC), 
            errmsg("Exception detail is relation: %s, file: %s, column id: "
                "%d, skip number: %d, read number: %d.",
                RelationGetRelationName(readerState->scanstate->ss_currentRelation),
                readerState->currentSplit->filePath, (int32_t)columnIdx, skipRecords, (int32_t)numValues)));
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_ORC),
                        errmsg("Error occurs while reading orc file %s, detail can be found in dn log of %s.",
                               readerState->currentSplit->filePath, g_instance.attr.attr_common.PGXCNodeName)));
    }
}

template <FieldKind fieldKind>
void OrcColumnReaderImpl<fieldKind>::skip(uint64_t numValues)
{
    skipRowsBuffer += numValues;
}

template <FieldKind fieldKind>
void OrcColumnReaderImpl<fieldKind>::setBloomFilter(filter::BloomFilter *_bloomFilter)
{
    Assert(_bloomFilter != NULL);
    bloomFilter = _bloomFilter;

    /* We do not check bloom filter on each row for string type, since the cost is too big,
     * unless there is only one element in the bloom filter.
     */
    if (bloomFilter->getType() != EQUAL_BLOOM_FILTER &&
        (NOT_STRING(bloomFilter->getDataType()) || 1 == bloomFilter->getNumValues())) {
        checkBloomFilterOnRow = true;
    }
}

template <FieldKind fieldKind>
bool OrcColumnReaderImpl<fieldKind>::hasPredicate() const
{
    return checkPredicateOnRow || checkBloomFilterOnRow;
}

template <FieldKind fieldKind>
bool OrcColumnReaderImpl<fieldKind>::nullFilter(bool hasNull, const char *notNull, bool *isSelected, uint64_t rowIdx)
{
    bool filter = false;

    if (!isSelected[rowIdx]) {
        filter = true;
    } else if (hasNull && !notNull[rowIdx]) { /* Check the null value. */
        isSelected[rowIdx] = HdfsPredicateCheckNull<NullWrapper>(predicate);
        filter = true;
    }
    return filter;
}

template <FieldKind fieldKind>
void OrcColumnReaderImpl<fieldKind>::predicateFilter(uint64_t numValues, bool *isSelected)
{
    bool hasNull = primitiveBatch->hasNulls;
    char *notNull = primitiveBatch->notNull.data();

    switch (fieldKind) {
        case orc::BOOLEAN:
        case orc::SHORT:
        case orc::INT:
        case orc::LONG:
        case orc::BYTE: {
            int64_t *data = static_cast<orc::LongVectorBatch *>(primitiveBatch.get())->data.data();

            for (uint64_t i = 0; i < numValues; i++) {
                if (!nullFilter(hasNull, notNull, isSelected, i)) {
                    int64_t tmpValue = data[i];
                    if (checkPredicateOnRow) {
#ifdef ENABLE_LLVM_COMPILE
                        isSelected[i] = HdfsPredicateCheckValueIntForLlvm<Int64Wrapper, int64_t>(tmpValue, predicate);
#else
                        isSelected[i] = HdfsPredicateCheckValue<Int64Wrapper, int64_t>(tmpValue, predicate);
#endif
                    }

                    if (checkBloomFilterOnRow && isSelected[i]) {
                        if (!bloomFilter->includeValue(tmpValue)) {
                            isSelected[i] = false;
                            this->readerState->bloomFilterRows += 1;
                        }
                    }
                }
            }
            break;
        }
        case orc::FLOAT:
        case orc::DOUBLE: {
            double *data = static_cast<orc::DoubleVectorBatch *>(primitiveBatch.get())->data.data();

            for (uint64_t i = 0; i < numValues; i++) {
                if (!nullFilter(hasNull, notNull, isSelected, i)) {
                    double tmpValue = data[i];
                    if (checkPredicateOnRow) {
#ifdef ENABLE_LLVM_COMPILE
                        isSelected[i] = HdfsPredicateCheckValueDoubleForLlvm<Float8Wrapper, double>(tmpValue,
                                                                                                    predicate);
#else
                        isSelected[i] = HdfsPredicateCheckValue<Float8Wrapper, double>(tmpValue, predicate);
#endif
                    }

                    if (checkBloomFilterOnRow && isSelected[i]) {
                        if (!bloomFilter->includeValue(tmpValue)) {
                            isSelected[i] = false;
                            this->readerState->bloomFilterRows += 1;
                        }
                    }
                }
            }

            break;
        }
        case orc::CHAR:
        case orc::VARCHAR:
        case orc::STRING: {
            char **data = static_cast<orc::StringVectorBatch *>(primitiveBatch.get())->data.data();
            int64_t *lengths = static_cast<orc::StringVectorBatch *>(primitiveBatch.get())->length.data();

            for (uint64_t i = 0; i < numValues; i++) {
                if (!nullFilter(hasNull, notNull, isSelected, i)) {
                    char lastChar = '\0';
                    char *tmpValue = data[i];
                    int64_t length = lengths[i];
                    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && 0 == length) {
                        tmpValue = NULL;
                    } else {
                        lastChar = tmpValue[length];
                        tmpValue[length] = '\0';
                    }

                    if (checkPredicateOnRow) {
                        if (NULL == tmpValue) {
                            notNull[i] = false;
                            isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(predicate);
                        } else {
                            if (var->vartype == NUMERICOID) {
                                uint32_t typmod = uint32_t(var->vartypmod - VARHDRSZ);
                                uint32_t precision = (typmod >> 16) & 0xffff;
                                uint32_t local_scale = typmod & 0xffff;
                                if (precision <= 18) {
                                    Datum result = DirectFunctionCall3(numeric_in, CStringGetDatum(tmpValue),
                                        ObjectIdGetDatum(InvalidOid), Int32GetDatum(var->vartypmod));
                                    int64 value = convert_short_numeric_to_int64_byscale(DatumGetNumeric(result),
                                                                                         local_scale);
                                    isSelected[i] = HdfsPredicateCheckValue<Int64Wrapper, int64_t>(value, predicate);
                                } else if (precision <= 38) {
                                    int128 value = 0;
                                    Datum result = DirectFunctionCall3(numeric_in, CStringGetDatum(tmpValue),
                                        ObjectIdGetDatum(InvalidOid), Int32GetDatum(var->vartypmod));
                                    convert_short_numeric_to_int128_byscale(DatumGetNumeric(result), local_scale,
                                                                            value);
                                    isSelected[i] = HdfsPredicateCheckValue<Int128Wrapper, int128>(value, predicate);
                                } else { /* must NOT run here, decimal(p>38) doesn't support predicate pushdown. */
                                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_ORC),
                                             errmsg("The type of decimal(precision>38) doesn't support predicate "
                                                    "pushdown.")));
                                }
                            } else {
                                isSelected[i] = HdfsPredicateCheckValue<StringWrapper, char *>(tmpValue, predicate);
                            }
                        }
                    }

                    if (checkBloomFilterOnRow && isSelected[i]) {
                        if (tmpValue && !bloomFilter->includeValue(tmpValue)) {
                            isSelected[i] = false;
                            this->readerState->bloomFilterRows += 1;
                        }
                    }

                    if (NULL != tmpValue && *tmpValue != '\0') {
                        tmpValue[length] = lastChar;
                    }
                }
            }

            break;
        }
        case orc::DATE: {
            int64_t *data = static_cast<orc::LongVectorBatch *>(primitiveBatch.get())->data.data();
            for (uint64_t i = 0; i < numValues; i++) {
                if (!nullFilter(hasNull, notNull, isSelected, i)) {
                    Timestamp tmpValue = date2timestamp(static_cast<DateADT>(data[i] - ORC_PSQL_EPOCH_IN_DAYS));
                    isSelected[i] = HdfsPredicateCheckValue<TimestampWrapper, Timestamp>(tmpValue, predicate);
                }
            }

            break;
        }
        case orc::TIMESTAMP: {
            int64_t *seconds = static_cast<orc::TimestampVectorBatch *>(primitiveBatch.get())->data.data();
            int64_t *nanoSeconds = static_cast<orc::TimestampVectorBatch *>(primitiveBatch.get())->nanoseconds.data();
            for (uint64_t i = 0; i < numValues; i++) {
                if (!nullFilter(hasNull, notNull, isSelected, i)) {
                    Timestamp tmpValue = (Timestamp)(((seconds[i] - epochOffsetDiff) * MICROSECONDS_PER_SECOND) +
                                                     (nanoSeconds[i] / NANOSECONDS_PER_MICROSECOND));
                    isSelected[i] = HdfsPredicateCheckValue<TimestampWrapper, Timestamp>(tmpValue, predicate);
                }
            }

            break;
        }
        case orc::DECIMAL: {
            /* Only short numeric will be pushed down, so we just handle the Decimal64VectorBatch here. */
            uint32_t typmod = uint32_t(var->vartypmod - VARHDRSZ);
            uint32_t precision = (typmod >> 16) & 0xffff;
            if (precision <= 18) {
                int64_t *data = static_cast<orc::Decimal64VectorBatch *>(primitiveBatch.get())->values.data();
                for (uint64_t i = 0; i < numValues; i++) {
                    if (!nullFilter(hasNull, notNull, isSelected, i)) {
                        isSelected[i] = HdfsPredicateCheckValue<Int64Wrapper, int64_t>(data[i], predicate);
                    }
                }
            } else {
                orc::Int128 *data = static_cast<orc::Decimal128VectorBatch *>(primitiveBatch.get())->values.data();
                for (uint64_t i = 0; i < numValues; i++) {
                    if (!nullFilter(hasNull, notNull, isSelected, i)) {
                        uint128 tmpVal = 0;
                        tmpVal = (tmpVal | (uint128)(uint64_t)data[i].getHighBits()) << 64;
                        tmpVal = tmpVal | (uint128)data[i].getLowBits();
                        int128 tmp_val = (int128)tmpVal;
                        isSelected[i] = HdfsPredicateCheckValue<Int128Wrapper, int128>(tmp_val, predicate);
                    }
                }
            }

            break;
        }
        default: {
            break;
        }
    }
}

template <FieldKind fieldKind>
template <bool hasNull, bool encoded>
void OrcColumnReaderImpl<fieldKind>::fillScalarVectorInternal(uint64_t numValues, bool *isSelected, ScalarVector *vec)
{
    char *notNull = primitiveBatch->notNull.data();
    int32_t encoding = readerState->fdwEncoding;
    int32_t offset = vec->m_rows;
    int32_t errorCount = 0;
    bool meetError = false;
    bool internalNull = false;

    for (uint64_t i = 0; i < numValues; i++) {
        if (isSelected[i]) {
            internalNull = false;
            meetError = false;

            Assert(offset < BatchMaxSize);
            if (hasNull && !notNull[i]) {
                vec->SetNull(offset);
            } else if (encoded) {
                Datum dValue = convertToDatumFunc(primitiveBatch.get(), i, var->vartypmod, scale, epochOffsetDiff,
                                                  internalNull, encoding, checkEncodingLevel, meetError);
                if (internalNull) {
                    vec->SetNull(offset);
                } else {
                    (void)vec->AddVar(dValue, offset);
                }
            } else {
                Datum dValue = convertToDatumFunc(primitiveBatch.get(), i, var->vartypmod, scale, epochOffsetDiff,
                                                  internalNull, encoding, checkEncodingLevel, meetError);
                if (internalNull) {
                    vec->SetNull(offset);
                } else {
                    vec->m_vals[static_cast<uint32_t>(offset)] = dValue;
                }
            }

            offset++;
            if (meetError) {
                errorCount++;
            }
        }
    }
    readerState->dealWithCount += offset - vec->m_rows;
    readerState->incompatibleCount += errorCount;
    vec->m_rows = offset;
}

template <>
template <bool hasNull, bool encoded>
void OrcColumnReaderImpl<orc::DECIMAL>::fillScalarVectorInternal(uint64_t numValues, bool *isSelected,
                                                                 ScalarVector *vec)
{
    char *notNull = primitiveBatch->notNull.data();
    int32_t offset = vec->m_rows;

    if (useDecimal128) {
        orc::Int128 *values = static_cast<orc::Decimal128VectorBatch *>(primitiveBatch.get())->values.data();
        for (uint64_t i = 0; i < numValues; i++) {
            if (isSelected[i]) {
                Assert(offset < BatchMaxSize);
                if (hasNull && !notNull[i]) {
                    vec->SetNull(offset);
                } else {
                    orc::Int128 inVal = values[i];
                    uint128 tmpVal = 0;

                    /* Convert from orc int128 format to gcc int128 format */
                    tmpVal = (tmpVal | (uint128)(uint64)inVal.getHighBits()) << 64;
                    tmpVal = tmpVal | (uint128)inVal.getLowBits();

                    /*  Add the big decimal directly into vectorbatch */
                    (void)vec->AddBigNumericWithoutHeader((int128)tmpVal, scale, offset);
                }

                offset++;
            }
        }
    } else {
        int64 *values = static_cast<orc::Decimal64VectorBatch *>(primitiveBatch.get())->values.data();
        for (uint64_t i = 0; i < numValues; i++) {
            if (isSelected[i]) {
                Assert(offset < BatchMaxSize);
                if (hasNull && !notNull[i]) {
                    vec->SetNull(offset);
                } else {
                    int64 value64 = values[i];

                    /* Add the short decimal directly into vector batch */
                    (void)vec->AddShortNumericWithoutHeader(value64, scale, offset);
                }

                offset++;
            }
        }
    }

    vec->m_rows = offset;
}

template <>
template <bool hasNull, bool encoded>
void OrcColumnReaderImpl<orc::CHAR>::fillScalarVectorInternal(uint64_t numValues, bool *isSelected, ScalarVector *vec)
{
    char *notNull = primitiveBatch->notNull.data();
    char **values = static_cast<orc::StringVectorBatch *>(primitiveBatch.get())->data.data();
    int64_t *lengths = static_cast<orc::StringVectorBatch *>(primitiveBatch.get())->length.data();

    /* The encoding defined in foreign table, which is -1 for non-foreign table */
    int32_t encoding = readerState->fdwEncoding;

    /* the offset in the current vector batch to be filled */
    int32_t offset = vec->m_rows;

    /* count that how many invalid strings are found */
    int32_t errorCount = 0;

    /* the typemod of the current var */
    int32_t atttypmod = var->vartypmod;

    /* The varchar length defined in the table DDL */
    int32_t maxLen = 0;

    /* temp value to store whether the invalid string is found */
    bool meetError = false;

    for (uint64_t i = 0; i < numValues; i++) {
        if (isSelected[i]) {
            Assert(offset < BatchMaxSize);
            if (hasNull && !notNull[i]) {
                vec->SetNull(offset);
            } else {
                int64_t length = lengths[i];

                /* Check compatibility and convert '' into null if the db is A_FORMAT. */
                if (A_FORMAT == u_sess->attr.attr_sql.sql_compatibility && 0 == length) {
                    vec->SetNull(offset);
                } else {
                    char *tmpValue = values[i];

                    switch (checkEncodingLevel) {
                        case NO_ENCODING_CHECK: {
                            Assert(-1 == var->vartypmod || length == var->vartypmod - VARHDRSZ);
                            (void)vec->AddBPCharWithoutHeader(tmpValue, length, length, offset);
                            break;
                        }
                        case LOW_ENCODING_CHECK:
                        case HIGH_ENCODING_CHECK: {
                            meetError = false;

                            /* check and convert */
                            char *cvt = pg_to_server_withfailure(tmpValue, length, encoding, checkEncodingLevel,
                                                                 meetError);

                            /* count the error number */
                            if (meetError) {
                                errorCount++;
                            }

                            if (atttypmod < (int32_t)VARHDRSZ) {
                                maxLen = length;
                            } else {
                                maxLen = atttypmod - VARHDRSZ;
                                if (length > maxLen) {
                                    /* Verify that extra characters are spaces, and clip them off */
                                    int32_t mbmaxlen = pg_mbcharcliplen(cvt, length, maxLen);
                                    int32_t j;

                                    /*
                                     * at this point, len is the actual BYTE length of the input
                                     * string, maxlen is the max number of CHARACTERS allowed for this
                                     * bpchar type, mbmaxlen is the length in BYTES of those chars.
                                     */
                                    for (j = mbmaxlen; j < length; j++) {
                                        if (cvt[j] != ' ')
                                            ereport(ERROR,
                                                    (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                                                     errmsg("value too long for type character(%d)", (int)maxLen)));
                                    }

                                    /*
                                     * Now we set maxlen to the necessary byte length, not the number
                                     * of CHARACTERS!
                                     */
                                    maxLen = length = mbmaxlen;
                                }
                            }
                            /* put into the vector batch */
                            (void)vec->AddBPCharWithoutHeader(cvt, maxLen, length, offset);

                            /* clean the temporary memory */
                            if (cvt != tmpValue) {
                                pfree_ext(cvt);
                            }
                            break;
                        }
                        default: {
                            break;
                        }
                    }
                }
            }
            offset++;
        }
    }
    readerState->dealWithCount += offset - vec->m_rows;
    readerState->incompatibleCount += errorCount;
    vec->m_rows = offset;
}

template <>
template <bool hasNull, bool encoded>
void OrcColumnReaderImpl<orc::VARCHAR>::fillScalarVectorInternal(uint64_t numValues, bool *isSelected,
                                                                 ScalarVector *vec)
{
    char *notNull = primitiveBatch->notNull.data();
    char **values = static_cast<orc::StringVectorBatch *>(primitiveBatch.get())->data.data();
    int64_t *lengths = static_cast<orc::StringVectorBatch *>(primitiveBatch.get())->length.data();

    /* The encoding defined in foreign table, which is -1 for non-foreign table */
    int32_t encoding = readerState->fdwEncoding;

    /* the offset in the current vector batch to be filled */
    int32_t offset = vec->m_rows;

    /* count that how many invalid strings are found */
    int32_t errorCount = 0;

    /* the typemod of the current var */
    int32_t atttypmod = var->vartypmod;

    /* The varchar length defined in the table DDL */
    int32_t maxLen = atttypmod - VARHDRSZ;
    ;

    /* temp value to store whether the invalid string is found */
    bool meetError = false;

    for (uint64_t i = 0; i < numValues; i++) {
        if (isSelected[i]) {
            Assert(offset < BatchMaxSize);
            if (hasNull && !notNull[i]) {
                vec->SetNull(offset);
            } else {
                int64_t length = lengths[i];

                /* Check compatibility and convert '' into null if the db is A_FORMAT. */
                if (A_FORMAT == u_sess->attr.attr_sql.sql_compatibility && 0 == length) {
                    vec->SetNull(offset);
                } else {
                    char *tmpValue = values[i];

                    switch (checkEncodingLevel) {
                        case NO_ENCODING_CHECK: {
                            Assert(-1 == var->vartypmod || length <= var->vartypmod - VARHDRSZ);
                            (void)vec->AddVarCharWithoutHeader(tmpValue, length, offset);
                            break;
                        }
                        case LOW_ENCODING_CHECK:
                        case HIGH_ENCODING_CHECK: {
                            meetError = false;

                            /* check and convert */
                            char *cvt = pg_to_server_withfailure(tmpValue, length, encoding, checkEncodingLevel,
                                                                 meetError);

                            /* count the error number */
                            if (meetError) {
                                errorCount++;
                            }

                            if (atttypmod >= (int32_t)VARHDRSZ && length > maxLen) {
                                /* Verify that extra characters are spaces, and clip them off */
                                int32_t mbmaxlen = pg_mbcharcliplen(cvt, length, maxLen);
                                int32_t j;

                                for (j = mbmaxlen; j < length; j++) {
                                    if (cvt[j] != ' ')
                                        ereport(ERROR,
                                                (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                                                 errmsg("value too long for type character varying(%d)", (int)maxLen)));
                                }

                                length = mbmaxlen;
                            }

                            /* put into the vector batch */
                            (void)vec->AddVarCharWithoutHeader(cvt, length, offset);

                            /* clean the temporary memory */
                            if (cvt != tmpValue) {
                                pfree_ext(cvt);
                            }
                            break;
                        }
                        default: {
                            break;
                        }
                    }
                }
            }

            offset++;
        }
    }
    readerState->dealWithCount += offset - vec->m_rows;
    readerState->incompatibleCount += errorCount;
    vec->m_rows = offset;
}

template <FieldKind fieldKind>
int32_t OrcColumnReaderImpl<fieldKind>::fillScalarVector(uint64_t numValues, bool *isSelected, ScalarVector *vec)
{
    ScalarDesc desc = vec->m_desc;
    bool hasNull = primitiveBatch->hasNulls;

    if (hasNull) {
        if (desc.encoded) {
            fillScalarVectorInternal<true, true>(numValues, isSelected, vec);
        } else {
            fillScalarVectorInternal<true, false>(numValues, isSelected, vec);
        }
    } else {
        if (desc.encoded) {
            fillScalarVectorInternal<false, true>(numValues, isSelected, vec);
        } else {
            fillScalarVectorInternal<false, false>(numValues, isSelected, vec);
        }
    }

    return vec->m_rows;
}

template <FieldKind fieldKind>
bool OrcColumnReaderImpl<fieldKind>::checkBloomFilter(uint64_t strideIdx) const
{
    uint64_t num = reader->getCurNumHashFunctions(strideIdx);
    if (0 == num || num != bloomFilter->getNumHashFunctions()) {
        return true;
    }

    uint64_t size = reader->getCurBitsetSize(strideIdx);
    if (size != bloomFilter->getLength()) {
        return true;
    }

    const uint64_t *bitset = reader->getCurBitset(strideIdx);
    return bloomFilter->includedIn(bitset);
}

template <FieldKind fieldKind>
Node *OrcColumnReaderImpl<fieldKind>::buildColRestriction(RestrictionType type, orc::Reader *structReader,
                                                          uint64_t stripeIdx, uint64_t strideIdx) const
{
    Datum minValue = 0;
    Datum maxValue = 0;
    bool hasMaximum = false;
    bool hasMinimum = false;
    Node *baseRestriction = NULL;
    int32_t hasStatistics = 0;

    try {
        switch (type) {
            case FILE: {
                hasStatistics = getStatistics(structReader->getColumnStatistics(columnIdx).get(), &minValue,
                                              &hasMinimum, &maxValue, &hasMaximum);
                break;
            }
            case STRIPE: {
                hasStatistics =
                    getStatistics(structReader->getStripeStatistics(stripeIdx)->getColumnStatistics(columnIdx),
                                  &minValue, &hasMinimum, &maxValue, &hasMaximum);
                break;
            }
            case STRIDE: {
                hasStatistics = getStatistics(reader->getStrideStatOfCurStripe(strideIdx).get(), &minValue, &hasMinimum,
                                              &maxValue, &hasMaximum);
                break;
            }
            default: {
                break;
            }
        }
    } catch (abi::__forced_unwind &) {
        throw;
    } catch (std::exception &ex) {
        hasStatistics = false;
    } catch (...) {
    }

    if (hasStatistics) {
        baseRestriction = MakeBaseConstraintWithExpr(lessThanExpr, greaterThanExpr, minValue, maxValue, hasMinimum,
                                                     hasMaximum);
    }

    return baseRestriction;
}

/*
 * Reads the min/max value from the file footer into datum pointers
 *
 * @return 1 for success, 0 when min/max value cannot be obtained
 */
template <FieldKind fieldKind>
int32_t OrcColumnReaderImpl<fieldKind>::getStatistics(const orc::ColumnStatistics *statistics, Datum *minDatum,
                                                      bool *hasMinimum, Datum *maxDatum, bool *hasMaximum) const
{
#define SET_SIMPLE_MIN_MAX_STATISTICS(state, func, minvalue, maxValue) do { \
    *hasMinimum = (state)->hasMinimum();                           \
    *hasMaximum = (state)->hasMaximum();                           \
    if (*hasMinimum || *hasMaximum) {                              \
        if (*hasMinimum) {                                         \
            *minDatum = func(minvalue);                            \
        }                                                          \
        if (*hasMaximum) {                                         \
            *maxDatum = func(maxValue);                            \
        }                                                          \
        ret = 1;                                                   \
    }                                                              \
} while (0)

#define SET_COMPLEX_MIN_MAX_STATISTICS(state, func, minvalue, maxValue) do { \
    *hasMinimum = (state)->hasMinimum();                                                                   \
    *hasMaximum = (state)->hasMaximum();                                                                   \
    if (*hasMinimum || *hasMaximum) {                                                                      \
        if (*hasMinimum) {                                                                                 \
            *minDatum = DirectFunctionCall3(func, CStringGetDatum(minvalue), ObjectIdGetDatum(InvalidOid), \
                                            Int32GetDatum(var->vartypmod));                                \
        }                                                                                                  \
        if (*hasMaximum) {                                                                                 \
            *maxDatum = DirectFunctionCall3(func, CStringGetDatum(maxValue), ObjectIdGetDatum(InvalidOid), \
                                            Int32GetDatum(var->vartypmod));                                \
        }                                                                                                  \
        ret = 1;                                                                                           \
    }                                                                                                      \
} while (0)

    int32_t ret = 0;
    if (NULL == statistics)
        return ret;

    switch (fieldKind) {
        case orc::BYTE:
        case orc::SHORT:
        case orc::INT:
        case orc::LONG: {
            const orc::IntegerColumnStatistics *tmpStat = static_cast<const orc::IntegerColumnStatistics *>(statistics);
            SET_SIMPLE_MIN_MAX_STATISTICS(tmpStat, Int64GetDatum, tmpStat->getMinimum(), tmpStat->getMaximum());

            break;
        }
        case orc::FLOAT: {
            const orc::DoubleColumnStatistics *tmpStat = static_cast<const orc::DoubleColumnStatistics *>(statistics);
            SET_SIMPLE_MIN_MAX_STATISTICS(tmpStat, Float4GetDatum, static_cast<float4>(tmpStat->getMinimum()),
                                          static_cast<float4>(tmpStat->getMaximum()));
            break;
        }
        case orc::DOUBLE: {
            const orc::DoubleColumnStatistics *tmpStat = static_cast<const orc::DoubleColumnStatistics *>(statistics);
            SET_SIMPLE_MIN_MAX_STATISTICS(tmpStat, Float8GetDatum, tmpStat->getMinimum(), tmpStat->getMaximum());
            break;
        }
        case orc::CHAR: {
            const orc::StringColumnStatistics *tmpStat = static_cast<const orc::StringColumnStatistics *>(statistics);
            SET_COMPLEX_MIN_MAX_STATISTICS(tmpStat, bpcharin, tmpStat->getMinimum().c_str(),
                                           tmpStat->getMaximum().c_str());

            break;
        }
        case orc::VARCHAR: {
            const orc::StringColumnStatistics *tmpStat = static_cast<const orc::StringColumnStatistics *>(statistics);
            SET_COMPLEX_MIN_MAX_STATISTICS(tmpStat, varcharin, tmpStat->getMinimum().c_str(),
                                           tmpStat->getMaximum().c_str());

            break;
        }
        case orc::STRING: {
            if (var->vartype == INTERVALOID || var->vartype == TINTERVALOID || var->vartype == TIMETZOID ||
                var->vartype == CHAROID || var->vartype == NVARCHAR2OID || var->vartype == NUMERICOID) {
                return 0;
            }
            const orc::StringColumnStatistics *tmpStat = static_cast<const orc::StringColumnStatistics *>(statistics);
            SET_SIMPLE_MIN_MAX_STATISTICS(tmpStat, CStringGetTextDatum, tmpStat->getMinimum().c_str(),
                                          tmpStat->getMaximum().c_str());

            break;
        }
        case orc::DATE: {
            /* Version 0.11 min/max of date/timestamp is not correct, so we don't use it. */
            if (reader->getFormatVersion() == "0.11") {
                ret = 0;
            } else {
                const orc::DateColumnStatistics *tmpStat = static_cast<const orc::DateColumnStatistics *>(statistics);
                SET_SIMPLE_MIN_MAX_STATISTICS(tmpStat, TimestampGetDatum,
                                              date2timestamp(tmpStat->getMinimum() - ORC_PSQL_EPOCH_IN_DAYS),
                                              date2timestamp(tmpStat->getMaximum() - ORC_PSQL_EPOCH_IN_DAYS));
            }
            break;
        }
        case orc::DECIMAL: {
            if (reader->getFormatVersion() == "0.11") {
                ret = 0;
            } else {
                const orc::DecimalColumnStatistics *tmpStat =
                    static_cast<const orc::DecimalColumnStatistics *>(statistics);
                SET_COMPLEX_MIN_MAX_STATISTICS(tmpStat, numeric_in, tmpStat->getMinimum().toString().c_str(),
                                               tmpStat->getMaximum().toString().c_str());
            }

            break;
        }
        case orc::TIMESTAMP: {
            const orc::TimestampColumnStatistics *tmpStat =
                static_cast<const orc::TimestampColumnStatistics *>(statistics);
            SET_SIMPLE_MIN_MAX_STATISTICS(tmpStat, TimestampGetDatum,
                tmpStat->getMinimum() * NANOSECONDS_PER_MICROSECOND - (epochOffsetDiff * MICROSECONDS_PER_SECOND),
                tmpStat->getMaximum() * NANOSECONDS_PER_MICROSECOND - (epochOffsetDiff * MICROSECONDS_PER_SECOND));

            break;
        }
        case orc::BOOLEAN:
        default: {
            break;
        }
    }

    return ret;
}

template <FieldKind fieldKind>
bool OrcColumnReaderImpl<fieldKind>::matchOrcWithPsql() const
{
    bool matches = false;
    Oid varType = var->vartype;

    switch (fieldKind) {
        case orc::LONG: {
            matches = (varType == INT8OID || varType == OIDOID || varType == TIMESTAMPTZOID || varType == TIMEOID ||
                       varType == SMALLDATETIMEOID || varType == CASHOID);
            break;
        }
        case orc::INT: {
            matches = varType == INT4OID;
            break;
        }
        case orc::SHORT: {
            matches = varType == INT2OID;
            break;
        }
        case orc::BYTE: {
            matches = varType == INT1OID;
            break;
        }
        case orc::FLOAT: {
            matches = varType == FLOAT4OID;
            break;
        }
        case orc::DOUBLE: {
            matches = varType == FLOAT8OID;
            break;
        }
        case orc::BOOLEAN: {
            matches = varType == BOOLOID;
            break;
        }
        case orc::CHAR: {
            matches = varType == BPCHAROID;
            break;
        }
        case orc::VARCHAR: {
            matches = varType == VARCHAROID;
            break;
        }
        case orc::STRING: {
            /*
             * If there is no precision or (0 == precision || precision > 18),
             * we use string to store numeric data for HDFS table.
             */
            if (varType == NUMERICOID) {
                uint32_t typemod = (uint32_t)(var->vartypmod - VARHDRSZ);
                uint32_t varPrecision = (typemod >> 16) & 0xffff;
                uint32_t varScale = typemod & 0xffff;
                matches = (-1 == (int32_t)var->vartypmod ||
                           !(varPrecision > 0 && varPrecision <= 18 && varScale <= 16));
            } else {
                matches = (varType == TEXTOID || varType == CLOBOID ||
                           (var->vartypmod < 0 && (varType == VARCHAROID || varType == BPCHAROID)) ||
                           varType == INTERVALOID || varType == TINTERVALOID || varType == TIMETZOID ||
                           varType == CHAROID || varType == NVARCHAR2OID);
            }

            break;
        }
        case orc::TIMESTAMP:
        case orc::DATE: {
            matches = varType == TIMESTAMPOID;
            break;
        }
        case orc::DECIMAL: {
            if (varType == NUMERICOID) {
                /* no precision */
                if (-1 == var->vartypmod) {
                    matches = true;
                } else { /* Both scale and precision need to be matched when precision is defined. */
                    uint32_t typemod = (uint32_t)(var->vartypmod - VARHDRSZ);
                    uint32_t varPrecision = (typemod >> 16) & 0xffff;
                    uint32_t varScale = typemod & 0xffff;
                    matches = ((varScale == (uint32_t)scale) &&
                               (varPrecision == (uint32_t)reader->getType().getPrecision()));
                }
            }
            break;
        }
        default: {
            break;
        }
    }

    return matches;
}
}  // namespace reader
}  // namespace dfs
