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
 * carbondata_column_reader.cpp
 *
 *
 * IDENTIFICATION
 *         src/gausskernel/storage/access/dfs/carbondata/carbondata_column_reader.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <type_traits>

#include "access/dfs/dfs_query.h"
#include "access/dfs/dfs_query_check.h"
#include "access/dfs/dfs_wrapper.h"
#include "carbondata_column_reader.h"
#include "carbondata_stream_adapter.h"
#include "carbondata_memory_adapter.h"
#include "carbondata/column_reader.h"
#include "carbondata/decimal.h"
#include "carbondata/memory.h"
#include "carbondata/minmax.h"
#include "carbondata/carbondata_timestamp.h"
#include "carbondata/tostr.h"

#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/elog.h"
#include "utils/memutils.h"

namespace dfs {
namespace reader {
#define NOT_STRING(oid) ((oid != VARCHAROID && oid != BPCHAROID) && (oid != TEXTOID && oid != CLOBOID))

#define TYPE_ERROR(varType, carbondataTypeStr, carbondataType)                                               \
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_CARBONDATA),                       \
                    errmsg("Unsupported var type %u from data type %s(%u).", (varType), (carbondataTypeStr), \
                           (carbondataType))));

static void CheckConvertType(Oid varType, Oid mustType, carbondata::DataType::type carbondataType,
                             const char *carbondataTypeStr)
{
    if (varType != mustType) {
        TYPE_ERROR(varType, carbondataTypeStr, carbondataType);
    }
}

Datum ConvertBinaryToDatum(void *primitiveBatch, uint64 rowId, carbondata::DataType::type carbondataType, Oid varType,
                           int32_t precision, int32 scale, int32_t typeLength, bool &isNull, int32 encoding,
                           int32 checkEncodingLevel, bool &meetError, const char *carbondata_type)
{
    Datum result = 0;

    CheckConvertType(varType, BYTEAOID, carbondataType, carbondata_type);

    carbondata::Batch *tmpBatch = static_cast<carbondata::Batch *>(primitiveBatch);
    uint64_t tmpOffset = (tmpBatch->Offset())[rowId];
    uint64_t tmpLength = tmpBatch->GetRowLength(rowId);
    unsigned char *tmpValue = tmpBatch->Data() + tmpOffset;

    if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && tmpLength == 0) {
        isNull = true;
    } else {
        result = CStringGetByteaDatum((char *)tmpValue, tmpLength);
    }

    return result;
}

Datum ConvertStringToDatum(void *primitiveBatch, uint64 rowId, carbondata::DataType::type carbondataType, Oid varType,
                           int32_t precision, int32 scale, int32_t typeLength, bool &isNull, int32 encoding,
                           int32 checkEncodingLevel, bool &meetError, const char *carbondata_type)
{
    Datum result = 0;

    CheckConvertType(varType, TEXTOID, carbondataType, carbondata_type);
    carbondata::Batch *tmpBatch = static_cast<carbondata::Batch *>(primitiveBatch);
    uint64_t tmpOffset = (tmpBatch->Offset())[rowId];
    uint64_t tmpLength = tmpBatch->GetRowLength(rowId);

    unsigned char *tmpValue = tmpBatch->Data() + tmpOffset;

    if (tmpLength == 0 && u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
        isNull = true;
    } else {
        text *cstring = cstring_to_text_with_len((char *)tmpValue, tmpLength);
        result = PointerGetDatum(cstring);
    }

    return result;
}

Datum ConvertShortToDatum(void *primitiveBatch, uint64 rowId, carbondata::DataType::type carbondataType, Oid varType,
                          int32_t precision, int32 scale, int32_t typeLength, bool &isNull, int32 encoding,
                          int32 checkEncodingLevel, bool &meetError, const char *carbondata_type)
{
    Datum result = 0;

    carbondata::Batch *tempBatch = static_cast<carbondata::Batch *>(primitiveBatch);
    int16_t tempValue = ((int16_t *)(tempBatch->Data()))[rowId];
    switch (varType) {
        case INT2OID: {
            result = Int16GetDatum(tempValue);
            break;
        }
        case INT4OID: {
            result = Int32GetDatum((int32_t)tempValue);
            break;
        }
        case INT8OID: {
            result = Int64GetDatum((int64_t)tempValue);
            break;
        }
        default: {
            TYPE_ERROR(varType, carbondata_type, carbondataType);
            break;
        }
    }
    return result;
}

Datum ConvertIntToDatum(void *primitiveBatch, uint64 rowId, carbondata::DataType::type carbondataType, Oid varType,
                        int32_t precision, int32 scale, int32_t typeLength, bool &isNull, int32 encoding,
                        int32 checkEncodingLevel, bool &meetError, const char *carbondataTypeStr)
{
    Datum result = 0;

    carbondata::Batch *tmpBatch = static_cast<carbondata::Batch *>(primitiveBatch);
    int32_t tmpValue = ((int32_t *)(tmpBatch->Data()))[rowId];
    switch (varType) {
        case INT4OID: {
            result = Int32GetDatum(tmpValue);
            break;
        }
        case INT8OID: {
            result = Int64GetDatum((int64_t)tmpValue);
            break;
        }
        default: {
            TYPE_ERROR(varType, carbondataTypeStr, carbondataType);
            break;
        }
    }

    return result;
}

Datum ConvertLongToDatum(void *primitiveBatch, uint64 rowId, carbondata::DataType::type carbondataType, Oid varType,
                         int32_t precision, int32 scale, int32_t typeLength, bool &isNull, int32 encoding,
                         int32 checkEncodingLevel, bool &meetError, const char *carbondata_type)
{
    Datum result = 0;

    CheckConvertType(varType, INT8OID, carbondataType, carbondata_type);

    carbondata::Batch *tmpBatch = static_cast<carbondata::Batch *>(primitiveBatch);
    int64_t tmpValue = ((int64_t *)(tmpBatch->Data()))[rowId];
    result = Int64GetDatum(tmpValue);

    return result;
}

Datum ConvertDoubleToDatum(void *primitiveBatch, uint64 rowId, carbondata::DataType::type carbondataType, Oid varType,
                           int32_t precision, int32 scale, int32_t typeLength, bool &isNull, int32 encoding,
                           int32 checkEncodingLevel, bool &meetError, const char *carbondata_type)
{
    Datum result = 0;

    CheckConvertType(varType, FLOAT8OID, carbondataType, carbondata_type);

    carbondata::Batch *tmpBatch = static_cast<carbondata::Batch *>(primitiveBatch);
    double tmpValue = ((double *)(tmpBatch->Data()))[rowId];
    result = Float8GetDatum(tmpValue);

    return result;
}

Datum ConvertTimestampToDatum(void *primitiveBatch, uint64 rowId, carbondata::DataType::type carbondataType,
                              Oid varType, int32_t precision, int32 scale, int32_t typeLength, bool &isNull,
                              int32 encoding, int32 checkEncodingLevel, bool &meetError,
                              const char *carbondataTypeString)
{
    Datum result = 0;

    switch (varType) {
        case TIMESTAMPOID:
        case TIMESTAMPTZOID: {
            carbondata::Batch *batch = static_cast<carbondata::Batch *>(primitiveBatch);
            int64_t tmpValue = ((int64_t *)(batch->Data()))[rowId];

            Timestamp timestamp = static_cast<Timestamp>(
                tmpValue - CARBONDATA_PSQL_EPOCH_IN_DAYS * CARBONDATA_MICROSECOND_IN_EACH_DAY + epochOffsetDiff);
            struct pg_tm tt;
            struct pg_tm *ptm = &tt;
            fsec_t fsec;
            if (timestamp2tm(timestamp, NULL, ptm, &fsec, NULL, NULL) == -1) {
                isNull = true;
                break;
            }
            timestamp = Carbondata::adjustTimestampForJulian(timestamp);
            result = TimestampGetDatum(timestamp);
            break;
        }
        default: {
            TYPE_ERROR(varType, carbondataTypeString, carbondataType);
            break;
        }
    }

    return result;
}

Datum ConvertDateToDatum(void *primitiveBatch, uint64 rowId, carbondata::DataType::type carbondataType, Oid varType,
                         int32_t precision, int32 scale, int32_t typeLength, bool &isNull, int32 encoding,
                         int32 checkEncodingLevel, bool &meetError, const char *strCarbondataType)
{
    Datum result = 0;

    carbondata::Batch *tmpBatch = static_cast<carbondata::Batch *>(primitiveBatch);
    int32_t tmpValue = ((int32_t *)(tmpBatch->Data()))[rowId];
    switch (varType) {
        case DATEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID: {
            Timestamp timestamp =
                static_cast<Timestamp>((tmpValue - CARBONDATA_PSQL_EPOCH_IN_DAYS) * CARBONDATA_MICROSECOND_IN_EACH_DAY);

            struct pg_tm tm;
            fsec_t fsec;
            if (timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL) == -1) {
                isNull = true;
                break;
            }
            timestamp = Carbondata::adjustTimestampForJulian(timestamp);
            result = TimestampGetDatum(timestamp);
            break;
        }
        default: {
            TYPE_ERROR(varType, strCarbondataType, carbondataType);
            break;
        }
    }

    return result;
}

Datum ConvertBooleanToDatum(void *primitiveBatch, uint64 rowId, carbondata::DataType::type carbondataType, Oid varType,
                            int32_t precision, int32 scale, int32_t typeLength, bool &isNull, int32 encoding,
                            int32 checkEncodingLevel, bool &meetError, const char *typeStr)
{
    Datum result = 0;

    carbondata::Batch *tmpBatch = static_cast<carbondata::Batch *>(primitiveBatch);
    bool tmpValue = ((bool *)(tmpBatch->Data()))[rowId];
    switch (varType) {
        case BOOLOID: {
            result = BoolGetDatum(tmpValue);
            break;
        }
        case INT1OID: {
            result = Int8GetDatum((int8_t)tmpValue);
            break;
        }
        default: {
            TYPE_ERROR(varType, typeStr, carbondataType);
            break;
        }
    }

    return result;
}

Datum ConvertFloatToDatum(void *primitiveBatch, uint64 rowId, carbondata::DataType::type carbondataType, Oid varType,
                          int32_t precision, int32 scale, int32_t typeLength, bool &isNull, int32 encoding,
                          int32 checkEncodingLevel, bool &meetError, const char *typeString)
{
    Datum result = 0;

    carbondata::Batch *tmpBatch = static_cast<carbondata::Batch *>(primitiveBatch);
    float tmpValue = ((float *)(tmpBatch->Data()))[rowId];
    switch (varType) {
        case FLOAT4OID: {
            result = Float4GetDatum(tmpValue);
            break;
        }
        case FLOAT8OID: {
            result = Float8GetDatum((float8)tmpValue);
            break;
        }
        default: {
            TYPE_ERROR(varType, typeString, carbondataType);
            break;
        }
    }

    return result;
}

Datum ConvertByteToDatum(void *primitiveBatch, uint64 rowId, carbondata::DataType::type carbondataType, Oid varType,
                         int32_t precision, int32 scale, int32_t typeLength, bool &isNull, int32 encoding,
                         int32 checkEncodingLevel, bool &meetError, const char *stringCarbondataType)
{
    Datum result = 0;

    carbondata::Batch *tmpBatch = static_cast<carbondata::Batch *>(primitiveBatch);
    int8_t tmpValue = ((int8_t *)(tmpBatch->Data()))[rowId];
    switch (varType) {
        case INT2OID: {
            result = Int16GetDatum((int16_t)tmpValue);
            break;
        }
        case INT4OID: {
            result = Int32GetDatum((int32_t)tmpValue);
            break;
        }
        case INT8OID: {
            result = Int64GetDatum((int64_t)tmpValue);
            break;
        }
        default: {
            TYPE_ERROR(varType, stringCarbondataType, carbondataType)
            break;
        }
    }

    return result;
}

Datum ConvertDecimalToDatum(void *primitiveBatch, uint64 rowId, carbondata::DataType::type carbondataType, Oid varType,
                            int32_t precision, int32 scale, int32_t typeLength, bool &isNull, int32 encoding,
                            int32 checkEncodingLevel, bool &meetError, const char *carbondata_type)
{
    Datum result = 0;

    CheckConvertType(varType, NUMERICOID, carbondataType, carbondata_type);

    carbondata::Batch *batch = static_cast<carbondata::Batch *>(primitiveBatch);
    if (precision > 0 && precision <= 18) {
        int64_t tmpValue = ((int64_t *)(batch->Data()))[rowId];
        result = makeNumeric64(tmpValue, scale);
    } else {
        __int128 tmpValue = ((__int128 *)(batch->Data()))[rowId];
        result = makeNumeric128(tmpValue, scale);
    }

    return result;
}

Datum convertToDatum(void *primitiveBatch, uint64 rowId, carbondata::DataType::type carbondataType, Oid varType,
                     int32_t precision, int32 scale, int32_t typeLength, bool &isNull, int32 encoding,
                     int32 checkEncodingLevel, bool &meetError)
{
    Datum result = 0;
    const char *carbondata_type = carbondata::typeToString(carbondataType);
    switch (carbondataType) {
        case carbondata::DataType::BINARY: {
            result = ConvertBinaryToDatum(primitiveBatch, rowId, carbondataType, varType, precision, scale, typeLength,
                                          isNull, encoding, checkEncodingLevel, meetError, carbondata_type);
            break;
        }
        case carbondata::DataType::STRING:
        case carbondata::DataType::VARCHAR: {
            result = ConvertStringToDatum(primitiveBatch, rowId, carbondataType, varType, precision, scale, typeLength,
                                          isNull, encoding, checkEncodingLevel, meetError, carbondata_type);

            break;
        }
        case carbondata::DataType::SHORT: {
            result = ConvertShortToDatum(primitiveBatch, rowId, carbondataType, varType, precision, scale, typeLength,
                                         isNull, encoding, checkEncodingLevel, meetError, carbondata_type);
            break;
        }
        case carbondata::DataType::INT: {
            result = ConvertIntToDatum(primitiveBatch, rowId, carbondataType, varType, precision, scale, typeLength,
                                       isNull, encoding, checkEncodingLevel, meetError, carbondata_type);
            break;
        }
        case carbondata::DataType::LONG: {
            result = ConvertLongToDatum(primitiveBatch, rowId, carbondataType, varType, precision, scale, typeLength,
                                        isNull, encoding, checkEncodingLevel, meetError, carbondata_type);
            break;
        }
        case carbondata::DataType::DOUBLE: {
            result = ConvertDoubleToDatum(primitiveBatch, rowId, carbondataType, varType, precision, scale, typeLength,
                                          isNull, encoding, checkEncodingLevel, meetError, carbondata_type);
            break;
        }
        case carbondata::DataType::TIMESTAMP: {
            result = ConvertTimestampToDatum(primitiveBatch, rowId, carbondataType, varType, precision, scale,
                                             typeLength, isNull, encoding, checkEncodingLevel, meetError,
                                             carbondata_type);
            break;
        }
        case carbondata::DataType::DATE: {
            result = ConvertDateToDatum(primitiveBatch, rowId, carbondataType, varType, precision, scale, typeLength,
                                        isNull, encoding, checkEncodingLevel, meetError, carbondata_type);
            break;
        }
        case carbondata::DataType::BOOLEAN: {
            result = ConvertBooleanToDatum(primitiveBatch, rowId, carbondataType, varType, precision, scale, typeLength,
                                           isNull, encoding, checkEncodingLevel, meetError, carbondata_type);
            break;
        }
        case carbondata::DataType::FLOAT: {
            result = ConvertFloatToDatum(primitiveBatch, rowId, carbondataType, varType, precision, scale, typeLength,
                                         isNull, encoding, checkEncodingLevel, meetError, carbondata_type);
            break;
        }
        case carbondata::DataType::BYTE: {
            result = ConvertByteToDatum(primitiveBatch, rowId, carbondataType, varType, precision, scale, typeLength,
                                        isNull, encoding, checkEncodingLevel, meetError, carbondata_type);
            break;
        }
        case carbondata::DataType::DECIMAL: {
            result = ConvertDecimalToDatum(primitiveBatch, rowId, carbondataType, varType, precision, scale, typeLength,
                                           isNull, encoding, checkEncodingLevel, meetError, carbondata_type);
            break;
        }
        default: {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_CARBONDATA),
                            errmsg("Unsupported data type : %s.", carbondata_type)));
            break;
        }
    }
    return result;
}

template <typename ReaderType>
class CarbondataColumnReaderImpl : public CarbondataColumnReader {
public:
    CarbondataColumnReaderImpl(carbondata::ColumnSpec *columnSpec, uint32_t columnIndex, uint32_t mppColumnIndex,
                               ReaderState *readerState, const Var *var, uint64_t footerOffset)
        : m_batch(NULL),
          m_readerState(readerState),
          m_columnSpec(columnSpec),
          m_columnReader(NULL),
          m_memMgr(NULL),
          m_fileMetaData(NULL),
          m_blockletInfo3(NULL),
          m_lessThanExpr(NULL),
          m_greaterThanExpr(NULL),
          m_internalContext(NULL),
          m_var(var),
          m_predicate(NULL),
          m_checkPredicateOnRow(false),
          m_checkEncodingLevel(readerState->checkEncodingLevel),
          m_columnIndex(columnIndex),
          m_mppColumnIndex(mppColumnIndex),
          m_numNullValuesRead(0),
          m_notNullValues(NULL),
          m_num_rows(0),
          m_num_blocklets(0),
          m_currentBlockletIndex(0),
          m_totalRowsInCurrentBlocklet(0),
          m_rowsReadInCurrentBlocklet(0),
          m_currentPageIndex(0),
          m_totalRowsInCurrentPage(0),
          m_rowsReadInCurrentPage(0),
          m_rowBufferSize(0),
          m_footerOffset(footerOffset),
          m_rowBuffer(NULL),
          m_isInverted(false),
          m_invertedIndexReverse(NULL),
          m_tempStringBuffer(NULL)
    {
    }

    ~CarbondataColumnReaderImpl()
    {
    }

    void begin(std::unique_ptr<GSInputStream> gsInputStream, const carbondata::CarbondataFileMeta *fileMetaData)
    {
        m_predicate = m_readerState->hdfsScanPredicateArr[m_mppColumnIndex - 1];
        m_checkPredicateOnRow = list_length(m_predicate) > 0;

        m_internalContext = AllocSetContextCreate(m_readerState->persistCtx,
                                                  "carbondata column reader context for carbondata dfs reader",
                                                  ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                                  ALLOCSET_DEFAULT_MAXSIZE);

        MemoryContext oldContext = MemoryContextSwitchTo(m_internalContext);
        m_memMgr = new carbondata::CarbondataMemoryAdapter(m_internalContext);
        m_lessThanExpr = MakeOperatorExpression(const_cast<Var *>(m_var), BTLessEqualStrategyNumber);
        m_greaterThanExpr = MakeOperatorExpression(const_cast<Var *>(m_var), BTGreaterEqualStrategyNumber);

        initColumnReader(std::move(gsInputStream), fileMetaData);

        m_tempStringBuffer = makeStringInfo();

        (void)MemoryContextSwitchTo(oldContext);
    }

    void Destroy()
    {
        pfree_ext(m_lessThanExpr);
        pfree_ext(m_greaterThanExpr);

        if (NULL != m_tempStringBuffer) {
            resetStringInfo(m_tempStringBuffer);
            pfree(m_tempStringBuffer->data);
        }

        /* delete sdk object before clear the internal memory context. */
        if (NULL != m_columnReader) {
            DELETE_EX_TYPE(m_columnReader, ReaderType);
        }

        if (NULL != m_memMgr) {
            delete m_memMgr;
        }

        /* Clear the internal memory context. */
        if (NULL != m_internalContext && m_internalContext != CurrentMemoryContext) {
            MemoryContextDelete(m_internalContext);
        }
    }

    void setBlockletIndex(uint64_t blockletIndex)
    {
        m_currentBlockletIndex = blockletIndex;
        m_blockletInfo3 = &m_fileMetaData->m_blockletInfoList3[m_currentBlockletIndex];

        m_totalRowsInCurrentBlocklet = m_blockletInfo3->num_rows;
        m_totalRowsInCurrentPage = 0;
        m_rowsReadInCurrentBlocklet = 0;
        m_currentPageIndex = 0;
        m_rowsReadInCurrentPage = 0;

        m_columnReader->SetBlockletIndex(m_currentBlockletIndex, getBlockletSize());
    }

    uint64_t setPageIndex(uint64_t pageIndex)
    {
        m_currentPageIndex = pageIndex;
        m_columnReader->SetPageIndex(m_currentPageIndex);
        m_rowsReadInCurrentPage = 0;
        readNewPage();
        return m_totalRowsInCurrentPage;
    }

    void setCurrentRowsInPage(uint64_t currentRowsInPage)
    {
        m_rowsReadInCurrentPage = currentRowsInPage;
    }

    void readNewPage()
    {
        auto typedReader = static_cast<ReaderType *>(m_columnReader);
        m_totalRowsInCurrentPage = typedReader->ReadNewPage();
        m_batch = typedReader->GetBatch();
        m_notNullValues = (bool *)m_batch->NotNull();
        m_isInverted = typedReader->GetIsInverted();
        m_invertedIndexReverse = typedReader->GetInvertedReverse();
    }

    /*
     * Check if the field type of current column matches the var type defined in
     * relation. For now, we use one-to-one type mapping mechanism.
     * @return true if The two type matches or return false.
     */
    bool matchCarbonWithPsql() const;

    /*
     * Filter the obtained data with the predicates on the column
     * and set isSelected flags.
     * @_in_param numValues: The number of rows to be filtered.
     * @_in_param currentRowsInPage: The current rows in page.
     * @_in_out_param isSelected: The flag array to indicate which row is selected or not.
     */
    void predicateFilter(uint64_t numValues, bool *isSelected) override;

    int32_t fillScalarVector(uint64_t numRowsToRead, const bool *isSelected, ScalarVector *scalarVector)
    {
        ScalarDesc desc = scalarVector->m_desc;
        if (hasNullValues()) {
            if (desc.encoded) {
                fillScalarVectorInternal<true, true>(numRowsToRead, isSelected, scalarVector);
            } else {
                fillScalarVectorInternal<true, false>(numRowsToRead, isSelected, scalarVector);
            }
        } else {
            if (desc.encoded) {
                fillScalarVectorInternal<false, true>(numRowsToRead, isSelected, scalarVector);
            } else {
                fillScalarVectorInternal<false, false>(numRowsToRead, isSelected, scalarVector);
            }
        }
        return scalarVector->m_rows;
    }

    /*
     * Build the column restriction node of blocklet/page with the min/max
     * information stored in CARBONDATA file.
     * @_in_param type: Indicates the restriction's level: blocklet or page.
     * @_in_param index: When the type is blocklet, this is blocklet index,
     *					 When the type is page, this is page index.
     * @return the built restriction of blocklet or page.
     */
    Node *buildColRestriction(CarbonRestrictionType type, uint64_t index) const override
    {
        Datum minValue = 0;
        Datum maxValue = 0;
        bool hasMinMaximum = false;
        Node *baseRestriction = NULL;
        int32_t hasStatistics = 0;

        const carbondata::format::BlockletMinMaxIndex *blockletMinMaxIndex = NULL;

        switch (type) {
            case BLOCKLET: {
                if (index >= (uint64_t)m_num_blocklets)
                    return NULL;

                const carbondata::format::BlockletIndex *blockletIndex = &m_fileMetaData->m_blockletIndexList[index];

                blockletMinMaxIndex = &(blockletIndex->min_max_index);
                break;
            }
            case PAGE: {
                if (index >= (uint64_t)m_blockletInfo3->number_number_of_pages)
                    return NULL;

                std::shared_ptr<carbondata::format::DataChunk3> dataChunk = m_columnReader->GetDataChunk();
                blockletMinMaxIndex = &(dataChunk->data_chunk_list[index].min_max);

                break;
            }
            default: {
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_CARBONDATA),
                                errmsg("Indicates the restriction's level is undefined.")));
            }
        }

        try {
            hasStatistics = GetMinMaxStatistics(type, blockletMinMaxIndex, &minValue, &maxValue, &hasMinMaximum);
        } catch (abi::__forced_unwind &) {
            throw;
        } catch (std::exception &ex) {
            hasStatistics = false;
        } catch (...) {
            hasStatistics = false;
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_CARBONDATA),
                            errmsg("get min max statistics false.")));
        }

        if (hasStatistics) {
            baseRestriction = MakeBaseConstraintWithExpr(m_lessThanExpr, m_greaterThanExpr, minValue, maxValue,
                                                         hasMinMaximum, hasMinMaximum);
        }
        return baseRestriction;
    }

private:
    /*
     * Get the statistics of the current columns, the level can be blocklet or
     * page that is decided by the statistics transfered in.
     * @_in_param statistics: The column statistics of a blocklet or page.
     * @_in_param blockletMinMaxIndex: Carbondata minmax index struct.
     * @_out_param minDatum: Set the Datum of minimal value if exists.
     * @_out_param maxDatum: Set the Datum of maximal value if exists.
     * @_out_param hasMinMax: Indicates if the minimal and maximal value exists.
     * @return 0: there is no minimum nor maximum.
     *		   1: there is as least one extremum.
     */
    int32_t GetMinMaxStatisticsString(const std::string &minValueStr, const std::string &maxValueStr, Datum *minDatum,
                                      Datum *maxDatum) const
    {
        const char *minvalue = (const char *)minValueStr.c_str();
        const char *maxvalue = (const char *)maxValueStr.c_str();
        *minDatum = CStringGetTextDatum(minvalue);
        *maxDatum = CStringGetTextDatum(maxvalue);

        return 1;
    }

    int32_t GetMinMaxStatisticsVarchar(const std::string &minValueStr, const std::string &maxValueStr, Datum *minDatum,
                                       Datum *maxDatum) const
    {
        const char *minvalue = (const char *)minValueStr.c_str();
        const char *maxvalue = (const char *)maxValueStr.c_str();
        *minDatum = DirectFunctionCall3(varcharin, CStringGetDatum(minvalue), ObjectIdGetDatum(InvalidOid),
                                        Int32GetDatum(m_var->vartypmod));
        *maxDatum = DirectFunctionCall3(varcharin, CStringGetDatum(maxvalue), ObjectIdGetDatum(InvalidOid),
                                        Int32GetDatum(m_var->vartypmod));

        return 1;
    }

    int32_t GetMinMaxStatisticsBoolean(const std::string &minValueStr, const std::string &maxValueStr, Datum *minDatum,
                                       Datum *maxDatum) const
    {
        bool minvalue = false;
        bool maxvalue = false;
        carbondata::ReadMinMaxBool(minvalue, (const unsigned char *)minValueStr.c_str());
        carbondata::ReadMinMaxBool(maxvalue, (const unsigned char *)maxValueStr.c_str());
        *minDatum = BoolGetDatum(minvalue);
        *maxDatum = BoolGetDatum(maxvalue);

        return 1;
    }

    int32_t GetMinMaxStatisticsByte(const std::string &minValueStr, const std::string &maxValueStr, Datum *minDatum,
                                    Datum *maxDatum) const
    {
        int32_t ret = 0;
        int8_t minvalue = 0;
        int8_t maxvalue = 0;
        if (m_columnSpec->m_isDimension) {
            /* because of carbondata byte sort bug, do not minmax filter for byte sort. */
        } else {
            carbondata::ReadMinMaxByte(minvalue, (const unsigned char *)minValueStr.c_str());
            carbondata::ReadMinMaxByte(maxvalue, (const unsigned char *)maxValueStr.c_str());
            *minDatum = Int16GetDatum((int16)minvalue);
            *maxDatum = Int16GetDatum((int16)maxvalue);
            ret = 1;
        }

        return ret;
    }

    int32_t GetMinMaxStatisticsShort(const std::string &minValueStr, const std::string &maxValueStr, Datum *minDatum,
                                     Datum *maxDatum) const
    {
        int16 minvalue = 0;
        int16 maxvalue = 0;
        if (m_columnSpec->m_isDimension) {
            carbondata::ReadMinMaxBySort<int16>(minvalue, (const unsigned char *)minValueStr.c_str());
            carbondata::ReadMinMaxBySort<int16>(maxvalue, (const unsigned char *)maxValueStr.c_str());
        } else {
            carbondata::ReadMinMaxT<int16>(minvalue, (const unsigned char *)minValueStr.c_str());
            carbondata::ReadMinMaxT<int16>(maxvalue, (const unsigned char *)maxValueStr.c_str());
        }
        *minDatum = Int16GetDatum(minvalue);
        *maxDatum = Int16GetDatum(maxvalue);

        return 1;
    }

    int32_t GetMinMaxStatisticsInt(const std::string &minValueStr, const std::string &maxValueStr, Datum *minDatum,
                                   Datum *maxDatum) const
    {
        int32 minvalue = 0;
        int32 maxvalue = 0;
        if (m_columnSpec->m_isDimension) {
            carbondata::ReadMinMaxBySort<int32>(minvalue, (const unsigned char *)minValueStr.c_str());
            carbondata::ReadMinMaxBySort<int32>(maxvalue, (const unsigned char *)maxValueStr.c_str());
        } else {
            carbondata::ReadMinMaxT<int32>(minvalue, (const unsigned char *)minValueStr.c_str());
            carbondata::ReadMinMaxT<int32>(maxvalue, (const unsigned char *)maxValueStr.c_str());
        }
        *minDatum = Int32GetDatum(minvalue);
        *maxDatum = Int32GetDatum(maxvalue);
        return 1;
    }

    int32_t GetMinMaxStatisticsLong(const std::string &minValueStr, const std::string &maxValueStr, Datum *minDatum,
                                    Datum *maxDatum) const
    {
        int64 minvalue = 0;
        int64 maxvalue = 0;
        if (m_columnSpec->m_isDimension) {
            carbondata::ReadMinMaxBySort<int64>(minvalue, (const unsigned char *)minValueStr.c_str());
            carbondata::ReadMinMaxBySort<int64>(maxvalue, (const unsigned char *)maxValueStr.c_str());
        } else {
            carbondata::ReadMinMaxT<int64>(minvalue, (const unsigned char *)minValueStr.c_str());
            /* because of carbondata long minvalue bug, set minvalue is INT64_MIN. */
            minvalue = PG_INT64_MIN;
            carbondata::ReadMinMaxT<int64>(maxvalue, (const unsigned char *)maxValueStr.c_str());
        }
        *minDatum = Int64GetDatum(minvalue);
        *maxDatum = Int64GetDatum(maxvalue);

        return 1;
    }

    int32_t GetMinMaxStatisticsFloat(const std::string &minValueStr, const std::string &maxValueStr, Datum *minDatum,
                                     Datum *maxDatum) const
    {
        float minvalue = 0.0;
        float maxvalue = 0.0;
        carbondata::ReadMinMaxFloat(minvalue, (const unsigned char *)minValueStr.c_str());
        carbondata::ReadMinMaxFloat(maxvalue, (const unsigned char *)maxValueStr.c_str());
        *minDatum = Float4GetDatum(minvalue);
        *maxDatum = Float4GetDatum(maxvalue);

        return 1;
    }

    int32_t GetMinMaxStatisticsDouble(const std::string &minValueStr, const std::string &maxValueStr, Datum *minDatum,
                                      Datum *maxDatum) const
    {
        double minvalue = 0.0;
        double maxvalue = 0.0;
        carbondata::ReadMinMaxDouble(minvalue, (const unsigned char *)minValueStr.c_str());
        carbondata::ReadMinMaxDouble(maxvalue, (const unsigned char *)maxValueStr.c_str());
        *minDatum = Float8GetDatum(minvalue);
        *maxDatum = Float8GetDatum(maxvalue);

        return 1;
    }

    int32_t GetMinMaxStatisticsTimestamp(const std::string &minValueStr, const std::string &maxValueStr,
                                         Datum *minDatum, Datum *maxDatum) const
    {
        int64 minvalue = 0;
        int64 maxvalue = 0;
        carbondata::ReadMinMaxTime(minvalue, (const unsigned char *)minValueStr.c_str());
        carbondata::ReadMinMaxTime(maxvalue, (const unsigned char *)maxValueStr.c_str());
        Timestamp adjustMinTimestamp = static_cast<Timestamp>(
            minvalue - CARBONDATA_PSQL_EPOCH_IN_DAYS * CARBONDATA_MICROSECOND_IN_EACH_DAY + epochOffsetDiff);
        adjustMinTimestamp = Carbondata::adjustTimestampForJulian(adjustMinTimestamp);
        *minDatum = TimestampGetDatum(static_cast<Timestamp>(adjustMinTimestamp));
        Timestamp adjustMaxTimestamp = static_cast<Timestamp>(
            maxvalue - CARBONDATA_PSQL_EPOCH_IN_DAYS * CARBONDATA_MICROSECOND_IN_EACH_DAY + epochOffsetDiff);
        adjustMaxTimestamp = Carbondata::adjustTimestampForJulian(adjustMaxTimestamp);
        *maxDatum = TimestampGetDatum(static_cast<Timestamp>(adjustMaxTimestamp));

        return 1;
    }

    int32_t GetMinMaxStatisticsDate(const std::string &minValueStr, const std::string &maxValueStr, Datum *minDatum,
                                    Datum *maxDatum) const
    {
        int32 minvalue = 0;
        int32 maxvalue = 0;
        carbondata::ReadMinMaxDate(minvalue, (const unsigned char *)minValueStr.c_str());
        carbondata::ReadMinMaxDate(maxvalue, (const unsigned char *)maxValueStr.c_str());
        Timestamp adjustMinTimestamp =
            static_cast<Timestamp>((minvalue - CARBONDATA_PSQL_EPOCH_IN_DAYS) * CARBONDATA_MICROSECOND_IN_EACH_DAY);
        adjustMinTimestamp = Carbondata::adjustTimestampForJulian(adjustMinTimestamp);
        *minDatum = TimestampGetDatum(static_cast<TimeADT>(adjustMinTimestamp));
        Timestamp adjustMaxTimestamp =
            static_cast<Timestamp>((maxvalue - CARBONDATA_PSQL_EPOCH_IN_DAYS) * CARBONDATA_MICROSECOND_IN_EACH_DAY);
        adjustMaxTimestamp = Carbondata::adjustTimestampForJulian(adjustMaxTimestamp);
        *maxDatum = TimestampGetDatum(static_cast<TimeADT>(adjustMaxTimestamp));

        return 1;
    }

    int32_t GetMinMaxStatisticsDecimal(const std::string &minValueStr, const std::string &maxValueStr, Datum *minDatum,
                                       Datum *maxDatum) const
    {
        uint32 minlength = minValueStr.length() - 1;
        uint32 maxlength = maxValueStr.length() - 1;
        carbondata::BigInt minvalue = carbondata::ByteToBigInt(minValueStr.c_str() + 1, minlength,
                                                               m_columnSpec->m_precision);
        carbondata::BigInt maxvalue = carbondata::ByteToBigInt(maxValueStr.c_str() + 1, maxlength,
                                                               m_columnSpec->m_precision);
        uint8 minscale = (uint8)minValueStr.c_str()[0];
        uint8 maxscale = (uint8)maxValueStr.c_str()[0];
        if (m_columnSpec->m_precision > 0 && m_columnSpec->m_precision <= 18) {
            *minDatum = makeNumeric64(minvalue.bigInt64, minscale);
            *maxDatum = makeNumeric64(maxvalue.bigInt64, maxscale);
        } else {
            *minDatum = makeNumeric128(minvalue.bigInt128, minscale);
            *maxDatum = makeNumeric128(maxvalue.bigInt128, maxscale);
        }

        return 1;
    }

    int32_t GetMinMaxStatistics(CarbonRestrictionType type,
                                const carbondata::format::BlockletMinMaxIndex *blockletMinMaxIndex, Datum *minDatum,
                                Datum *maxDatum, bool *hasMinMax) const
    {
        int32 ret = 0;
        int32 minmaxIndex = 0;

        if (type == BLOCKLET)
            minmaxIndex = m_columnIndex;
        else if (type == PAGE)
            minmaxIndex = 0;
        else
            return ret;

        *hasMinMax = blockletMinMaxIndex->min_max_presence[minmaxIndex];
        if (!(*hasMinMax))
            return ret;

        std::string minValueStr = blockletMinMaxIndex->min_values[minmaxIndex];
        std::string maxValueStr = blockletMinMaxIndex->max_values[minmaxIndex];

        switch (m_columnSpec->m_dataType) {
            case carbondata::format::DataType::STRING: {
                ret = GetMinMaxStatisticsString(minValueStr, maxValueStr, minDatum, maxDatum);
                break;
            }
            case carbondata::format::DataType::VARCHAR: {
                ret = GetMinMaxStatisticsVarchar(minValueStr, maxValueStr, minDatum, maxDatum);
                break;
            }

            case carbondata::format::DataType::BOOLEAN: {
                ret = GetMinMaxStatisticsBoolean(minValueStr, maxValueStr, minDatum, maxDatum);
                break;
            }

            case carbondata::format::DataType::BYTE: {
                ret = GetMinMaxStatisticsByte(minValueStr, maxValueStr, minDatum, maxDatum);
                break;
            }

            case carbondata::format::DataType::SHORT: {
                ret = GetMinMaxStatisticsShort(minValueStr, maxValueStr, minDatum, maxDatum);
                break;
            }

            case carbondata::format::DataType::INT: {
                ret = GetMinMaxStatisticsInt(minValueStr, maxValueStr, minDatum, maxDatum);
                break;
            }

            case carbondata::format::DataType::LONG: {
                ret = GetMinMaxStatisticsLong(minValueStr, maxValueStr, minDatum, maxDatum);
                break;
            }

            case carbondata::format::DataType::FLOAT: {
                ret = GetMinMaxStatisticsFloat(minValueStr, maxValueStr, minDatum, maxDatum);
                break;
            }

            case carbondata::format::DataType::DOUBLE: {
                ret = GetMinMaxStatisticsDouble(minValueStr, maxValueStr, minDatum, maxDatum);
                break;
            }

            case carbondata::format::DataType::TIMESTAMP: {
                ret = GetMinMaxStatisticsTimestamp(minValueStr, maxValueStr, minDatum, maxDatum);
                break;
            }

            case carbondata::format::DataType::DATE: {
                ret = GetMinMaxStatisticsDate(minValueStr, maxValueStr, minDatum, maxDatum);
                break;
            }

            case carbondata::format::DataType::DECIMAL: {
                ret = GetMinMaxStatisticsDecimal(minValueStr, maxValueStr, minDatum, maxDatum);
                break;
            }

            case carbondata::format::DataType::BINARY: {
                ret = 0;
                break;
            }

            /* there are some datatypes not support min-max filter. */
            case carbondata::format::DataType::ARRAY:
            case carbondata::format::DataType::STRUCT:
            case carbondata::format::DataType::MAP:
            default: {
                const char *strCarbondataType = carbondata::typeToString(m_columnSpec->m_dataType);
                ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_CARBONDATA),
                                errmsg("Unsupported data type for min-max filter: %s(%u).", strCarbondataType,
                                       m_columnSpec->m_dataType)));
                break;
            }
        }
        return ret;
    }

    void initColumnReader(std::unique_ptr<GSInputStream> gsInputStream,
                          const carbondata::CarbondataFileMeta *fileMetaData)
    {
        std::unique_ptr<carbondata::InputStream> source(
            new carbondata::CarbondataInputStreamAdapter(std::move(gsInputStream)));
        m_columnReader = new ReaderType(std::move(source), (carbondata::ColumnSpec *)m_columnSpec,
                                        fileMetaData->m_blockletInfoList3, m_memMgr);

        m_num_rows = fileMetaData->m_numOfRows;
        m_num_blocklets = fileMetaData->m_blockletInfoList3.size();
        m_fileMetaData = fileMetaData;

        if (!matchCarbonWithPsql()) {
            const char *carbondata_type = carbondata::typeToString(m_columnSpec->m_dataType);
            const char *kerner_type = format_type_with_typemod(m_var->vartype, m_var->vartypmod);
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_CARBONDATA),
                            errmsg("Error occurred while reading column %d: carbondata and kernel "
                                   "types do not match, carbondata type is %s and kernel type is %s.",
                                   m_var->varattno, carbondata_type, kerner_type)));
        }
    }

    template <bool hasNull, bool encoded>
    void fillScalarVectorInternal(uint64_t numRowsToRead, const bool *isSelected, ScalarVector *scalorVector);

    void fillScalarVectorInternalForOthers(uint64_t numRowsToRead, const bool *isSelected, ScalarVector *vec);

    void fillScalarVectorInternalForVarchar(uint64_t numRowsToRead, const bool *isSelected, ScalarVector *vec);

    void PredicateFilterString(uint64_t numValues, bool *isSelected, carbondata::Batch *batch);

    void PredicateFilterByte(uint64_t numValues, bool *isSelected, carbondata::Batch *batch);

    void PredicateFilterBoolean(uint64_t numValues, bool *isSelected, carbondata::Batch *batch);

    void PredicateFilterShort(uint64_t numValues, bool *isSelected, carbondata::Batch *batch);

    void PredicateFilterInt(uint64_t numValues, bool *isSelected, carbondata::Batch *batch);

    void PredicateFilterLong(uint64_t numValues, bool *isSelected, carbondata::Batch *batch);

    void PredicateFilterFloat(uint64_t numValues, bool *isSelected, carbondata::Batch *batch);

    void PredicateFilterDouble(uint64_t numValues, bool *isSelected, carbondata::Batch *batch);

    void PredicateFilterTimestamp(uint64_t numValues, bool *isSelected, carbondata::Batch *batch);

    void PredicateFilterDate(uint64_t numValues, bool *isSelected, carbondata::Batch *batch);

    void PredicateFilterDecimal(uint64_t numValues, bool *isSelected, carbondata::Batch *batch);

    inline bool hasNullValues()
    {
        return m_numNullValuesRead > 0;
    }

    inline bool isNullValue(uint64_t index)
    {
        return !m_notNullValues[index];
    }

    inline void setAsNullValue(uint64_t index)
    {
        m_notNullValues[index] = false;
    }

    inline uint64 getRealRowId(uint64_t index)
    {
        if (m_isInverted) {
            return m_invertedIndexReverse[index + m_rowsReadInCurrentPage];
        } else {
            return (index + m_rowsReadInCurrentPage);
        }
    }

    uint64_t getBlockletSize()
    {
        if ((m_currentBlockletIndex + 1) < m_fileMetaData->m_blockletInfoList3.size()) {
            return (uint64_t)(
                m_fileMetaData->m_blockletInfoList3[m_currentBlockletIndex + 1].column_data_chunks_offsets[0] -
                m_fileMetaData->m_blockletInfoList3[m_currentBlockletIndex].column_data_chunks_offsets[0]);
        } else {
            return (
                m_footerOffset -
                (uint64_t)m_fileMetaData->m_blockletInfoList3[m_currentBlockletIndex].column_data_chunks_offsets[0]);
        }
    }

    /*
     * Check if the current column of ORC file has predicates.
     * @return true: The predicate of the column exists;
     *             false: The predicate of the column does not exists;
     */
    inline bool hasPredicate() const override
    {
        return m_checkPredicateOnRow;
    }

    carbondata::Batch *m_batch;
    ReaderState *m_readerState;
    const carbondata::ColumnSpec *m_columnSpec;
    carbondata::ColumnReader *m_columnReader;

    carbondata::MemoryMgr *m_memMgr;
    const carbondata::CarbondataFileMeta *m_fileMetaData;
    const carbondata::format::BlockletInfo3 *m_blockletInfo3;

    /* The exprs for building the restriction according to the min/max value. */
    OpExpr *m_lessThanExpr;
    OpExpr *m_greaterThanExpr;

    MemoryContext m_internalContext;
    /* The var of the corresponding column define in relation. */
    const Var *m_var;

    /* The predicates list which is pushed down into the current column. */
    List *m_predicate;

    /* Flag to indicate whether we need to check predicate on row level. */
    bool m_checkPredicateOnRow;

    /* The level of encoding check,2 = high, 1 = low, 0 = no check */
    const int32_t m_checkEncodingLevel;

    const uint32_t m_columnIndex;
    const uint32_t m_mppColumnIndex;
    int64_t m_numNullValuesRead;
    bool *m_notNullValues;
    int64_t m_num_rows;
    int64_t m_num_blocklets;
    uint64_t m_currentBlockletIndex;
    uint64_t m_totalRowsInCurrentBlocklet;
    uint64_t m_rowsReadInCurrentBlocklet;
    uint64_t m_currentPageIndex;
    uint64_t m_totalRowsInCurrentPage;
    uint64_t m_rowsReadInCurrentPage;
    int32_t m_rowBufferSize;
    uint64_t m_footerOffset;
    char *m_rowBuffer;
    bool m_isInverted;
    int32_t *m_invertedIndexReverse;

    int16_t m_defLevels[BatchMaxSize] = {0};
    int16_t m_repLevels[BatchMaxSize] = {0};
    StringInfo m_tempStringBuffer;
};

template <typename ReaderType>
template <bool hasNull, bool encoded>
void CarbondataColumnReaderImpl<ReaderType>::fillScalarVectorInternal(uint64_t numRowsToRead, const bool *isSelected,
                                                                      ScalarVector *vec)
{
    fillScalarVectorInternalForOthers(numRowsToRead, isSelected, vec);
}

template <>
template <bool hasNull, bool encoded>
void CarbondataColumnReaderImpl<carbondata::StringColumnReader>::fillScalarVectorInternal(uint64_t numRowsToRead,
                                                                                          const bool *isSelected,
                                                                                          ScalarVector *vec)
{
    Oid varType = m_var->vartype;
    switch (varType) {
        case VARCHAROID:
            fillScalarVectorInternalForVarchar(numRowsToRead, isSelected, vec);
            break;
        case BYTEAOID:
        case TEXTOID:
            fillScalarVectorInternalForOthers(numRowsToRead, isSelected, vec);
            break;
        default:
            const char *carbondata_type = carbondata::typeToString(m_columnSpec->m_dataType);
            const char *unsupported_type = format_type_with_typemod(m_var->vartype, m_var->vartypmod);
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_CARBONDATA),
                            errmsg("Unsupported var type %s from data type %s.", unsupported_type, carbondata_type)));
            break;
    }
}

template <typename ReaderType>
void CarbondataColumnReaderImpl<ReaderType>::fillScalarVectorInternalForOthers(uint64_t numRowsToRead,
                                                                               const bool *isSelected,
                                                                               ScalarVector *vec)
{
    int32_t encoding = m_readerState->fdwEncoding;
    /* the offset in the current vector batch to be filled */
    int32_t offset = vec->m_rows;
    int32_t errorCount = 0;
    bool meetError = false;
    bool internalNull = false;
    int32_t rowId = 0;

    for (uint64_t i = 0; i < numRowsToRead; ++i) {
        rowId = getRealRowId(i);

        if (isSelected[i]) {
            Assert(offset < BatchMaxSize);

            internalNull = false;
            meetError = false;

            if (isNullValue(rowId)) {
                vec->SetNull(offset);
            } else {
                Datum dValue = convertToDatum(m_batch, rowId, m_columnSpec->m_dataType, m_var->vartype,
                                              m_columnSpec->m_precision, m_columnSpec->m_scale,
                                              m_columnSpec->m_valueSize, internalNull, encoding, m_checkEncodingLevel,
                                              meetError);

                if (internalNull) {
                    vec->SetNull(offset);
                } else {
                    vec->m_vals[offset] = dValue;
                }
            }

            offset++;
            if (meetError) {
                errorCount++;
            }
        }
    }

    m_readerState->dealWithCount += offset - vec->m_rows;
    m_readerState->incompatibleCount += errorCount;
    m_rowsReadInCurrentBlocklet += numRowsToRead;
    vec->m_rows = offset;
}

template <typename ReaderType>
void CarbondataColumnReaderImpl<ReaderType>::fillScalarVectorInternalForVarchar(uint64_t numRowsToRead,
                                                                                const bool *isSelected,
                                                                                ScalarVector *vec)
{
    /* The encoding defined in foreign table, which is -1 for non-foreign table */
    int32_t encoding = m_readerState->fdwEncoding;

    /* the offset in the current vector batch to be filled */
    int32_t offset = vec->m_rows;

    /* count that how many invalid strings are found */
    int32_t errorCount = 0;

    /* temp value to store whether the invalid string is found */
    bool meetError = false;

    /* the typemod of the current var */
    int32_t atttypmod = m_var->vartypmod;

    /* The varchar length defined in the table DDL */
    int32_t maxLen = atttypmod - VARHDRSZ;

    int32_t rowId = 0;
    for (uint64_t i = 0; i < numRowsToRead; ++i) {
        if (isSelected[i]) {
            Assert(offset < BatchMaxSize);

            rowId = getRealRowId(i);
            if (isNullValue(rowId)) {
                vec->SetNull(offset);
            } else {
                carbondata::Batch *tmpBatch = static_cast<carbondata::Batch *>(m_batch);
                uint64_t tmpOffset = (tmpBatch->Offset())[rowId];
                int32 tmpLength = (int32)tmpBatch->GetRowLength(rowId);

                char *tmpValue = (char *)(tmpBatch->Data() + tmpOffset);

                /* Check compatibility and convert '' into null if the db is A_FORMAT. */
                if (A_FORMAT == u_sess->attr.attr_sql.sql_compatibility && 0 == tmpLength) {
                    vec->SetNull(offset);
                } else {
                    switch (m_checkEncodingLevel) {
                        case NO_ENCODING_CHECK: {
                            Assert(-1 == m_var->vartypmod || tmpLength <= (m_var->vartypmod - VARHDRSZ));
                            (void)vec->AddVarCharWithoutHeader(tmpValue, tmpLength, offset);
                            break;
                        }
                        case LOW_ENCODING_CHECK:
                        case HIGH_ENCODING_CHECK: {
                            meetError = false;

                            /* check and convert */
                            char *cvt = pg_to_server_withfailure(tmpValue, tmpLength, encoding, m_checkEncodingLevel,
                                                                 meetError);

                            /* count the error number */
                            if (meetError) {
                                errorCount++;
                            }

                            if (atttypmod >= (int32_t)VARHDRSZ && tmpLength > maxLen) {
                                /* Verify that extra characters are spaces, and clip them off */
                                int32_t mbmaxlen = pg_mbcharcliplen(cvt, tmpLength, maxLen);

                                for (int32_t j = mbmaxlen; j < tmpLength; j++) {
                                    if (cvt[j] != ' ') {
                                        ereport(ERROR,
                                                (errcode(ERRCODE_STRING_DATA_RIGHT_TRUNCATION),
                                                 errmsg("value too long for type character varying(%d)", (int)maxLen)));
                                    }
                                }
                                tmpLength = mbmaxlen;
                            }
                            /* put into the vector batch */
                            (void)vec->AddVarCharWithoutHeader(cvt, tmpLength, offset);

                            /* clean the temporary memory */
                            if (cvt != tmpValue) {
                                pfree(cvt);
                            }
                            break;
                        }
                        default: {
                        }
                    }
                }
            }
            offset++;
        }
    }

    m_readerState->dealWithCount += offset - vec->m_rows;
    m_readerState->incompatibleCount += errorCount;
    m_rowsReadInCurrentBlocklet += numRowsToRead;
    vec->m_rows = offset;
}

template <typename ReaderType>
bool CarbondataColumnReaderImpl<ReaderType>::matchCarbonWithPsql() const
{
    bool matches = false;

    Oid varType = m_var->vartype;
    carbondata::DataType::type carbondataType = m_columnSpec->m_dataType;

    switch (carbondataType) {
        case carbondata::format::DataType::STRING: {
            matches = (varType == TEXTOID || varType == VARCHAROID || varType == BPCHAROID);
            break;
        }
        case carbondata::format::DataType::VARCHAR: {
            matches = (varType == VARCHAROID);
            break;
        }
        case carbondata::format::DataType::BINARY: {
            matches = (varType == BYTEAOID);
            break;
        }
        case carbondata::format::DataType::BOOLEAN: {
            matches = (varType == BOOLOID || varType == INT1OID);
            break;
        }
        case carbondata::format::DataType::BYTE: {
            matches = (varType == INT2OID || varType == INT4OID || varType == INT8OID);
            break;
        }
        case carbondata::format::DataType::SHORT: {
            matches = (varType == INT2OID || varType == INT4OID || varType == INT8OID);
            break;
        }
        case carbondata::format::DataType::INT: {
            matches = (varType == INT4OID || varType == INT8OID);
            break;
        }
        case carbondata::format::DataType::LONG: {
            matches = (varType == INT8OID);
            break;
        }
        case carbondata::format::DataType::FLOAT: {
            matches = (varType == FLOAT4OID || varType == FLOAT8OID);
            break;
        }
        case carbondata::format::DataType::DOUBLE: {
            matches = (varType == FLOAT8OID);
            break;
        }
        case carbondata::format::DataType::TIMESTAMP: {
            matches = (varType == TIMESTAMPOID || varType == TIMESTAMPTZOID);
            break;
        }
        case carbondata::format::DataType::DATE: {
            matches = (varType == DATEOID || varType == TIMESTAMPOID || varType == TIMESTAMPTZOID);
            break;
        }
        case carbondata::format::DataType::DECIMAL: {
            /* precision =-1		:no precision
             *			 =0			:variable precision
             *			 0< && <=18	:64 dicimal
             *			 18< && <=38:128 dicimal
             */
            if (varType == NUMERICOID) {
                /* no precision */
                if (-1 == m_var->vartypmod) {
                    matches = (m_columnSpec->m_precision >= 0);
                }
                /* Both scale and precision need to be matched when precision is defined. */
                else {
                    uint32_t typemod = (uint32_t)(m_var->vartypmod - VARHDRSZ);
                    uint32_t varPrecision = (typemod >> 16) & 0xffff;
                    uint32_t varScale = typemod & 0xffff;
                    matches = ((varScale == (uint32_t)m_columnSpec->m_scale) &&
                               (varPrecision == (uint32_t)m_columnSpec->m_precision));
                }

                if (!matches) {
                    const char *carbondata_type = carbondata::typeToString(m_columnSpec->m_dataType);
                    const char *kerner_type = format_type_with_typemod(m_var->vartype, m_var->vartypmod);
                    ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_CARBONDATA),
                                    errmsg("Error occurred while reading column %d: carbondata and kernel "
                                           "types do not match, carbondata type is %s(%d,%d) and kernel type is %s.",
                                           m_var->varattno, carbondata_type, m_columnSpec->m_precision,
                                           m_columnSpec->m_scale, kerner_type)));
                }
            }
            break;
        }
        default: {
            const char *carbondata_type = carbondata::typeToString(carbondataType);
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_CARBONDATA),
                            errmsg("Unsupported var type %u from data type %s(%u).", varType, carbondata_type,
                                   carbondataType)));
            break;
        }
    }
    return matches;
}

template <typename ReaderType>
void CarbondataColumnReaderImpl<ReaderType>::PredicateFilterString(uint64_t numValues, bool *isSelected,
                                                                   carbondata::Batch *batch)
{
    uint64 rowId = 0;
    uint64_t *tmpData = (batch->Offset());
    for (uint64_t i = 0; i < numValues; i++) {
        if (isSelected[i]) {
            rowId = getRealRowId(i);
            if (isNullValue(rowId)) {
                isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(m_predicate);
            } else {
                uint64_t tmpOffset = tmpData[rowId];
                uint64_t tmpLength = batch->GetRowLength(rowId);
                char *tmpValue = (char *)batch->Data() + tmpOffset;
                /* Check compatibility and convert '' into null if the db is A_FORMAT. */
                if (0 == tmpLength) {
                    if (A_FORMAT == u_sess->attr.attr_sql.sql_compatibility) {
                        tmpValue = NULL;
                    } else {
                        tmpValue = "";
                    }
                } else {
                    resetStringInfo(m_tempStringBuffer);
                    appendBinaryStringInfo(m_tempStringBuffer, tmpValue, tmpLength);
                    tmpValue = m_tempStringBuffer->data;
                }
                if (m_checkPredicateOnRow) {
                    if (NULL == tmpValue) {
                        setAsNullValue(rowId);
                        isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(m_predicate);
                    } else {
                        isSelected[i] = HdfsPredicateCheckValue<StringWrapper, char *>(tmpValue, m_predicate);
                    }
                }
            }
        }
    }
}

template <typename ReaderType>
void CarbondataColumnReaderImpl<ReaderType>::PredicateFilterByte(uint64_t numValues, bool *isSelected,
                                                                 carbondata::Batch *batch)
{
    uint64 rowId = 0;
    int8_t *pData = (int8_t *)(batch->Data());

    for (uint64_t i = 0; i < numValues; i++) {
        if (isSelected[i]) {
            rowId = getRealRowId(i);
            if (isNullValue(rowId)) {
                isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(m_predicate);
            } else if (m_checkPredicateOnRow) {
                int64_t tmpValue = (int64_t)pData[rowId];
#ifdef ENABLE_LLVM_COMPILE
                isSelected[i] = HdfsPredicateCheckValueIntForLlvm<Int64Wrapper, int64_t>(tmpValue, m_predicate);
#else
                isSelected[i] = HdfsPredicateCheckValue<Int64Wrapper, int64_t>(tmpValue, m_predicate);
#endif
            }
        }
    }
}

template <typename ReaderType>
void CarbondataColumnReaderImpl<ReaderType>::PredicateFilterBoolean(uint64_t numValues, bool *isSelected,
                                                                    carbondata::Batch *batch)
{
    uint64 rowId = 0;
    bool *tmpData = (bool *)(batch->Data());

    for (uint64_t i = 0; i < numValues; i++) {
        if (isSelected[i]) {
            rowId = getRealRowId(i);
            if (isNullValue(rowId)) {
                isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(m_predicate);
            } else if (m_checkPredicateOnRow) {
#ifdef ENABLE_LLVM_COMPILE
                isSelected[i] = HdfsPredicateCheckValueIntForLlvm<BoolWrapper, bool>(tmpData[rowId], m_predicate);
#else
                isSelected[i] = HdfsPredicateCheckValue<BoolWrapper, bool>(tmpData[rowId], m_predicate);
#endif
            }
        }
    }
}

template <typename ReaderType>
void CarbondataColumnReaderImpl<ReaderType>::PredicateFilterShort(uint64_t numValues, bool *isSelected,
                                                                  carbondata::Batch *batch)
{
    uint64 rowId = 0;
    int16_t *tmpData = (int16_t *)(batch->Data());

    for (uint64_t i = 0; i < numValues; i++) {
        if (isSelected[i]) {
            rowId = getRealRowId(i);
            if (isNullValue(rowId)) {
                isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(m_predicate);
            } else if (m_checkPredicateOnRow) {
                int64_t intValue = (int64_t)tmpData[rowId];
#ifdef ENABLE_LLVM_COMPILE
                isSelected[i] = HdfsPredicateCheckValueIntForLlvm<Int64Wrapper, int64_t>(intValue, m_predicate);
#else
                isSelected[i] = HdfsPredicateCheckValue<Int64Wrapper, int64_t>(intValue, m_predicate);
#endif
            }
        }
    }
}

template <typename ReaderType>
void CarbondataColumnReaderImpl<ReaderType>::PredicateFilterInt(uint64_t numValues, bool *isSelected,
                                                                carbondata::Batch *batch)
{
    uint64 rowId = 0;
    int32_t *pData = (int32_t *)(batch->Data());

    for (uint64_t i = 0; i < numValues; i++) {
        if (isSelected[i]) {
            rowId = getRealRowId(i);
            if (isNullValue(rowId)) {
                isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(m_predicate);
            } else if (m_checkPredicateOnRow) {
                int64_t valueInt = (int64_t)pData[rowId];
#ifdef ENABLE_LLVM_COMPILE
                isSelected[i] = HdfsPredicateCheckValueIntForLlvm<Int64Wrapper, int64_t>(valueInt, m_predicate);
#else
                isSelected[i] = HdfsPredicateCheckValue<Int64Wrapper, int64_t>(valueInt, m_predicate);
#endif
            }
        }
    }
}

template <typename ReaderType>
void CarbondataColumnReaderImpl<ReaderType>::PredicateFilterLong(uint64_t numValues, bool *isSelected,
                                                                 carbondata::Batch *batch)
{
    uint64 rowId = 0;
    int64_t *dataPtr = (int64_t *)(batch->Data());

    for (uint64_t i = 0; i < numValues; i++) {
        if (isSelected[i]) {
            rowId = getRealRowId(i);
            if (isNullValue(rowId)) {
                isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(m_predicate);
            } else if (m_checkPredicateOnRow) {
#ifdef ENABLE_LLVM_COMPILE
                isSelected[i] = HdfsPredicateCheckValueIntForLlvm<Int64Wrapper, int64_t>(dataPtr[rowId], m_predicate);
#else
                isSelected[i] = HdfsPredicateCheckValue<Int64Wrapper, int64_t>(dataPtr[rowId], m_predicate);
#endif
            }
        }
    }
}

template <typename ReaderType>
void CarbondataColumnReaderImpl<ReaderType>::PredicateFilterFloat(uint64_t numValues, bool *isSelected,
                                                                  carbondata::Batch *batch)
{
    uint64 rowId = 0;
    float *tmpData = (float *)(batch->Data());

    for (uint64_t i = 0; i < numValues; i++) {
        if (isSelected[i]) {
            rowId = getRealRowId(i);
            if (isNullValue(rowId)) {
                isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(m_predicate);
            } else if (m_checkPredicateOnRow) {
                double tmpValue = static_cast<double>(tmpData[rowId]);
#ifdef ENABLE_LLVM_COMPILE
                isSelected[i] = HdfsPredicateCheckValueDoubleForLlvm<Float8Wrapper, double>(tmpValue, m_predicate);
#else
                isSelected[i] = HdfsPredicateCheckValue<Float8Wrapper, double>(tmpValue, m_predicate);
#endif
            }
        }
    }
}

template <typename ReaderType>
void CarbondataColumnReaderImpl<ReaderType>::PredicateFilterDouble(uint64_t numValues, bool *isSelected,
                                                                   carbondata::Batch *batch)
{
    uint64 rowId = 0;
    double *tmpData = (double *)(batch->Data());

    for (uint64_t i = 0; i < numValues; i++) {
        if (isSelected[i]) {
            rowId = getRealRowId(i);
            if (isNullValue(rowId)) {
                isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(m_predicate);
            } else if (m_checkPredicateOnRow) {
                double doubleValue = tmpData[rowId];
#ifdef ENABLE_LLVM_COMPILE
                isSelected[i] = HdfsPredicateCheckValueDoubleForLlvm<Float8Wrapper, double>(doubleValue, m_predicate);
#else
                isSelected[i] = HdfsPredicateCheckValue<Float8Wrapper, double>(doubleValue, m_predicate);
#endif
            }
        }
    }
}

template <typename ReaderType>
void CarbondataColumnReaderImpl<ReaderType>::PredicateFilterTimestamp(uint64_t numValues, bool *isSelected,
                                                                      carbondata::Batch *batch)
{
    uint64 rowId = 0;
    int64_t *pData = (int64_t *)(batch->Data());
    for (uint64_t i = 0; i < numValues; i++) {
        if (isSelected[i]) {
            rowId = getRealRowId(i);
            if (isNullValue(rowId)) {
                isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(m_predicate);
            } else if (m_checkPredicateOnRow) {
                Timestamp timeValue = static_cast<Timestamp>(
                    pData[rowId] - CARBONDATA_PSQL_EPOCH_IN_DAYS * CARBONDATA_MICROSECOND_IN_EACH_DAY +
                    epochOffsetDiff);
                struct pg_tm tm;
                fsec_t fsec;
                if (timestamp2tm(timeValue, NULL, &tm, &fsec, NULL, NULL) == -1) {
                    setAsNullValue(rowId);
                    isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(m_predicate);
                } else {
                    timeValue = Carbondata::adjustTimestampForJulian(timeValue);
                    isSelected[i] = HdfsPredicateCheckValue<TimestampWrapper, Timestamp>(timeValue, m_predicate);
                }
            }
        }
    }
}

template <typename ReaderType>
void CarbondataColumnReaderImpl<ReaderType>::PredicateFilterDate(uint64_t numValues, bool *isSelected,
                                                                 carbondata::Batch *batch)
{
    uint64 rowId = 0;
    int32_t *tmpData = (int32_t *)(batch->Data());
    for (uint64_t i = 0; i < numValues; i++) {
        if (isSelected[i]) {
            rowId = getRealRowId(i);
            if (isNullValue(rowId)) {
                isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(m_predicate);
            } else if (m_checkPredicateOnRow) {
                Timestamp tsValue = static_cast<Timestamp>((tmpData[rowId] - CARBONDATA_PSQL_EPOCH_IN_DAYS) *
                                                           CARBONDATA_MICROSECOND_IN_EACH_DAY);

                struct pg_tm timep;
                fsec_t fsec;

                if (timestamp2tm(tsValue, NULL, &timep, &fsec, NULL, NULL) == -1) {
                    setAsNullValue(rowId);
                    isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(m_predicate);
                } else {
                    tsValue = Carbondata::adjustTimestampForJulian(tsValue);
                    isSelected[i] = HdfsPredicateCheckValue<TimestampWrapper, Timestamp>(tsValue, m_predicate);
                }
            }
        }
    }
}

template <typename ReaderType>
void CarbondataColumnReaderImpl<ReaderType>::PredicateFilterDecimal(uint64_t numValues, bool *isSelected,
                                                                    carbondata::Batch *batch)
{
    uint64 rowId = 0;
    uint32_t typmod = (uint32_t)m_var->vartypmod - VARHDRSZ;
    uint32_t precision = (typmod >> 16) & 0xffff;

    /* Decimal64 */
    if (precision <= 18) {
        int64 *tmpData = (int64 *)(batch->Data());

        for (uint64_t i = 0; i < numValues; i++) {
            if (isSelected[i]) {
                rowId = getRealRowId(i);
                if (isNullValue(rowId)) {
                    isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(m_predicate);
                } else if (m_checkPredicateOnRow) {
#ifdef ENABLE_LLVM_COMPILE
                    isSelected[i] = HdfsPredicateCheckValueIntForLlvm<Int64Wrapper, int64_t>(tmpData[rowId],
                                                                                             m_predicate);
#else
                    isSelected[i] = HdfsPredicateCheckValue<Int64Wrapper, int64_t>(tmpData[rowId], m_predicate);
#endif
                }
            }
        }
    }
    /* Decimal128 */
    else {
        int128 *tmpData = (int128 *)(batch->Data());

        for (uint64_t i = 0; i < numValues; i++) {
            if (isSelected[i]) {
                rowId = getRealRowId(i);
                if (isNullValue(rowId)) {
                    isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(m_predicate);
                } else if (m_checkPredicateOnRow) {
#ifdef ENABLE_LLVM_COMPILE
                    isSelected[i] = HdfsPredicateCheckValueIntForLlvm<Int128Wrapper, int128>(tmpData[rowId],
                                                                                             m_predicate);
#else
                    isSelected[i] = HdfsPredicateCheckValue<Int128Wrapper, int128>(tmpData[rowId], m_predicate);
#endif
                }
            }
        }
    }
}

template <typename ReaderType>
void CarbondataColumnReaderImpl<ReaderType>::predicateFilter(uint64_t numValues, bool *isSelected)
{
    carbondata::DataType::type carbondataType = m_columnSpec->m_dataType;
    carbondata::Batch *batch = static_cast<carbondata::Batch *>(m_batch);

    switch (carbondataType) {
        case carbondata::format::DataType::STRING:
        case carbondata::format::DataType::VARCHAR: {
            PredicateFilterString(numValues, isSelected, batch);
            break;
        }

        case carbondata::format::DataType::BYTE: {
            PredicateFilterByte(numValues, isSelected, batch);
            break;
        }

        case carbondata::format::DataType::BOOLEAN: {
            PredicateFilterBoolean(numValues, isSelected, batch);
            break;
        }

        case carbondata::format::DataType::SHORT: {
            PredicateFilterShort(numValues, isSelected, batch);
            break;
        }

        case carbondata::format::DataType::INT: {
            PredicateFilterInt(numValues, isSelected, batch);
            break;
        }

        case carbondata::format::DataType::LONG: {
            PredicateFilterLong(numValues, isSelected, batch);
            break;
        }

        case carbondata::format::DataType::FLOAT: {
            PredicateFilterFloat(numValues, isSelected, batch);
            break;
        }

        case carbondata::format::DataType::DOUBLE: {
            PredicateFilterDouble(numValues, isSelected, batch);
            break;
        }

        case carbondata::format::DataType::TIMESTAMP: {
            PredicateFilterTimestamp(numValues, isSelected, batch);
            break;
        }

        case carbondata::format::DataType::DATE: {
            PredicateFilterDate(numValues, isSelected, batch);
            break;
        }

        case carbondata::format::DataType::DECIMAL: {
            PredicateFilterDecimal(numValues, isSelected, batch);
            break;
        }
        default: {
            const char *carbondata_type = carbondata::typeToString(carbondataType);
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_CARBONDATA),
                            errmsg("Unsupported data type : %s for predicate filter.", carbondata_type)));
            break;
        }
    }
}

CarbondataColumnReader *createCarbondataColumnReader(const carbondata::ColumnSpec *columnSpec, uint32_t columnIndex,
                                                     uint32_t mppColumnIndex, ReaderState *readerState, const Var *var,
                                                     uint64_t footerOffset)
{
    CarbondataColumnReader *columnReader = NULL;
    switch (columnSpec->m_dataType) {
        case carbondata::DataType::BINARY:
        case carbondata::DataType::STRING: {
            columnReader = New(readerState->persistCtx) CarbondataColumnReaderImpl<carbondata::StringColumnReader>(
                (carbondata::ColumnSpec *)columnSpec, columnIndex, mppColumnIndex, readerState, var, footerOffset);
            break;
        }
        case carbondata::DataType::SHORT: {
            columnReader =
                New(readerState->persistCtx) CarbondataColumnReaderImpl<carbondata::NumberColumnReader<int16_t> >(
                    (carbondata::ColumnSpec *)columnSpec, columnIndex, mppColumnIndex, readerState, var, footerOffset);
            break;
        }
        case carbondata::DataType::INT: {
            columnReader =
                New(readerState->persistCtx) CarbondataColumnReaderImpl<carbondata::NumberColumnReader<int32_t> >(
                    (carbondata::ColumnSpec *)columnSpec, columnIndex, mppColumnIndex, readerState, var, footerOffset);
            break;
        }
        case carbondata::DataType::LONG: {
            columnReader =
                New(readerState->persistCtx) CarbondataColumnReaderImpl<carbondata::NumberColumnReader<int64_t> >(
                    (carbondata::ColumnSpec *)columnSpec, columnIndex, mppColumnIndex, readerState, var, footerOffset);
            break;
        }
        case carbondata::DataType::DOUBLE: {
            columnReader =
                New(readerState->persistCtx) CarbondataColumnReaderImpl<carbondata::NumberColumnReader<double> >(
                    (carbondata::ColumnSpec *)columnSpec, columnIndex, mppColumnIndex, readerState, var, footerOffset);
            break;
        }
        case carbondata::DataType::TIMESTAMP: {
            columnReader =
                New(readerState->persistCtx) CarbondataColumnReaderImpl<carbondata::NumberColumnReader<int64_t> >(
                    (carbondata::ColumnSpec *)columnSpec, columnIndex, mppColumnIndex, readerState, var, footerOffset);
            break;
        }
        case carbondata::DataType::DATE: {
            columnReader =
                New(readerState->persistCtx) CarbondataColumnReaderImpl<carbondata::NumberColumnReader<int32_t> >(
                    (carbondata::ColumnSpec *)columnSpec, columnIndex, mppColumnIndex, readerState, var, footerOffset);
            break;
        }
        case carbondata::DataType::BOOLEAN: {
            columnReader =
                New(readerState->persistCtx) CarbondataColumnReaderImpl<carbondata::NumberColumnReader<int8_t> >(
                    (carbondata::ColumnSpec *)columnSpec, columnIndex, mppColumnIndex, readerState, var, footerOffset);
            break;
        }
        case carbondata::DataType::VARCHAR: {
            columnReader = New(readerState->persistCtx) CarbondataColumnReaderImpl<carbondata::StringColumnReader>(
                (carbondata::ColumnSpec *)columnSpec, columnIndex, mppColumnIndex, readerState, var, footerOffset);
            break;
        }
        case carbondata::DataType::FLOAT: {
            columnReader =
                New(readerState->persistCtx) CarbondataColumnReaderImpl<carbondata::NumberColumnReader<float> >(
                    (carbondata::ColumnSpec *)columnSpec, columnIndex, mppColumnIndex, readerState, var, footerOffset);
            break;
        }
        case carbondata::DataType::BYTE: {
            columnReader =
                New(readerState->persistCtx) CarbondataColumnReaderImpl<carbondata::NumberColumnReader<int8_t> >(
                    (carbondata::ColumnSpec *)columnSpec, columnIndex, mppColumnIndex, readerState, var, footerOffset);
            break;
        }
        case carbondata::DataType::DECIMAL: {
            columnReader = New(readerState->persistCtx) CarbondataColumnReaderImpl<carbondata::DecimalColumnReader>(
                (carbondata::ColumnSpec *)columnSpec, columnIndex, mppColumnIndex, readerState, var, footerOffset);
            break;
        }
        default: {
            const char *carbondata_type = carbondata::typeToString(columnSpec->m_dataType);
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_CARBONDATA),
                            errmsg("Unsupported carbondata type : %s.", carbondata_type)));
            break;
        }
    }
    return columnReader;
}
}  // namespace reader
}  // namespace dfs
