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
 * parquet_column_reader.cpp
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/parquet/parquet_column_reader.cpp
 *
 * -------------------------------------------------------------------------
 */
#ifndef ENABLE_LITE_MODE
#include "parquet/platform.h"
#include "parquet/statistics.h"
#include "parquet/types.h"
#endif

#include "parquet_column_reader.h"
#include "parquet_input_stream_adapter.h"
#include "mb/pg_wchar.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "access/dfs/dfs_query.h"
#include "access/dfs/dfs_query_check.h"
#include "access/dfs/dfs_wrapper.h"
#ifndef ENABLE_LITE_MODE
#include "arrow/util/bit-util.h"
#endif

namespace dfs {
namespace reader {
#define NOT_STRING(oid) (((oid) != VARCHAROID && (oid) != BPCHAROID) && ((oid) != TEXTOID && (oid) != CLOBOID))

void convertBufferToDatum(const uint8_t *buf, int32_t length, int32_t scale, Datum &result)
{
    errno_t rc = EOK;

    if (length <= (int32_t)sizeof(int64_t)) {
        uint8_t buf64[sizeof(int64)] = {0};
        rc = memcpy_s(buf64 + sizeof(int64) - length, (uint32)length, buf, (uint32)length);
        securec_check(rc, "\0", "\0");
        if ((int8_t)(buf[0]) < 0) {
            rc = memset_s(buf64, sizeof(int64) - length, 0xFF, sizeof(int64) - length);
            securec_check(rc, "\0", "\0");
        }

        int64_t tmpVal = arrow::BitUtil::FromBigEndian(reinterpret_cast<const int64_t *>(buf64)[0]);

        result = makeNumeric64(tmpVal, scale);
    } else {
        uint8_t buf128[sizeof(int128)] = {0};
        rc = memcpy_s(buf128 + sizeof(int128) - length, (uint32_t)length, buf, (uint32_t)length);
        securec_check(rc, "\0", "\0");
        if ((int8_t)(buf[0]) < 0) {
            rc = memset_s(buf128, sizeof(int128) - length, 0xFF, sizeof(int128) - length);
            securec_check(rc, "\0", "\0");
        }

        int64_t highBits = arrow::BitUtil::FromBigEndian(reinterpret_cast<const int64_t *>(buf128)[0]);
        uint64_t lowBits = arrow::BitUtil::FromBigEndian(reinterpret_cast<const uint64_t *>(buf128)[1]);

        /* Convert from parquet int128 format to gcc int128 format */
        int128 tmpVal = 0;
        tmpVal = ((uint128)(tmpVal | highBits)) << 64;
        tmpVal = tmpVal | lowBits;

        result = makeNumeric128(tmpVal, scale);
    }
}

/* Convert a value in the Parquet column into datum. */
template <Oid varType>
Datum convertToDatumT(void *primitiveBatch, uint64 rowId, parquet::Type::type physicalType, int32 scale,
                      int32_t typeLength, bool &isNull, int32 encoding, int32 checkEncodingLevel, bool &meetError)
{
    Datum result = 0;
    switch (physicalType) {
        case parquet::Type::BOOLEAN: {
            bool tmpValue = (static_cast<bool *>(primitiveBatch))[rowId];
            result = BoolGetDatum(tmpValue);
            break;
        }
        case parquet::Type::INT32: {
            int32_t tmpValue = (static_cast<int32_t *>(primitiveBatch))[rowId];
            switch (varType) {
                case INT1OID:
                    result = Int8GetDatum(tmpValue);
                    break;
                case INT2OID:
                    result = Int16GetDatum(tmpValue);
                    break;
                case INT4OID:
                    result = Int32GetDatum(tmpValue);
                    break;
                case DATEOID:
                case TIMESTAMPOID:
                    result =
                        TimestampGetDatum(date2timestamp(static_cast<DateADT>(tmpValue - PARQUET_PSQL_EPOCH_IN_DAYS)));
                    break;
                case NUMERICOID:
                    result = makeNumeric64((int64_t)tmpValue, scale);
                    break;
                default:
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_PARQUET),
                                    errmsg("Unsupported var type %u from data type %s(%u).", varType,
                                           parquet::TypeToString(physicalType).c_str(), physicalType)));
            }
            break;
        }
        case parquet::Type::INT64: {
            int64_t tmpValue = (static_cast<int64_t *>(primitiveBatch))[rowId];
            /*
             * For hdfs table, oid,timestamptz,time,smalldatetime and cash, they are
             * all stored by int64.
             */
            switch (varType) {
                case OIDOID:
                    result = UInt32GetDatum(tmpValue);
                    break;
                case CASHOID:
                    result = CashGetDatum(tmpValue);
                    break;
                case INT8OID:
                    result = Int64GetDatum(tmpValue);
                    break;
                case NUMERICOID:
                    result = makeNumeric64(tmpValue, scale);
                    break;
                default:
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_PARQUET),
                                    errmsg("Unsupported var type %u from data type %s(%u).", varType,
                                           parquet::TypeToString(physicalType).c_str(), physicalType)));
            }
            break;
        }
        case parquet::Type::INT96: {
            parquet::Int96 tmpValue = (static_cast<parquet::Int96 *>(primitiveBatch))[rowId];

            switch (varType) {
                case TIMESTAMPOID:
                case TIMESTAMPTZOID: {
                    auto nanoSeconds = dfs::Int96GetNanoSeconds(tmpValue);
                    Timestamp timestamp = nanoSecondsToPsqlTimestamp(nanoSeconds);
                    result = TimestampGetDatum(timestamp);
                    break;
                }
                default:
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_PARQUET),
                                    errmsg("Unsupported var type %u from data type %s(%u).", varType,
                                           parquet::TypeToString(physicalType).c_str(), physicalType)));
            }
            break;
        }
        case parquet::Type::FLOAT: {
            float tmpValue = (static_cast<float *>(primitiveBatch))[rowId];
            result = Float4GetDatum(tmpValue);
            break;
        }
        case parquet::Type::DOUBLE: {
            double tmpValue = (static_cast<double *>(primitiveBatch))[rowId];
            result = Float8GetDatum(tmpValue);
            break;
        }
        case parquet::Type::BYTE_ARRAY: {
            auto baValue = (static_cast<parquet::ByteArray *>(primitiveBatch))[rowId];
            char *tmpValue = (char *)baValue.ptr;
            int64_t length = static_cast<int64_t>(baValue.len);

            /* Check compatibility and convert '' into null if the db is A_FORMAT. */
            if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && length == 0) {
                isNull = true;
                break;
            }

            /* Set the terminate flag and change it back after using. */
            char lastChar = tmpValue[length];
            tmpValue[length] = '\0';
            char *cvt = NULL;
            if (encoding != INVALID_ENCODING) {
                cvt = pg_to_server_withfailure(tmpValue, length, encoding, checkEncodingLevel, meetError);
            } else {
                cvt = tmpValue;
            }

            /*
             * For hdfs table, interval,tinterval,timetz,char(not bpchar),nvarchar2
             * and numeric, they are all stored by string.
             */
            switch (varType) {
                case TEXTOID: {
                    result = CStringGetTextDatum(cvt);
                    break;
                }
                default: {
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_PARQUET),
                                    errmsg("Unsupported var type %u from data type %s(%u).", varType,
                                           parquet::TypeToString(physicalType).c_str(), physicalType)));
                }
            }

            if (cvt != tmpValue) {
                pfree(cvt);
            }

            tmpValue[length] = lastChar;
            break;
        }
        case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
            auto flbaValue = (static_cast<parquet::FixedLenByteArray *>(primitiveBatch))[rowId];
            const uint8_t *buf = flbaValue.ptr;

            switch (varType) {
                case NUMERICOID: {
                    convertBufferToDatum(buf, typeLength, scale, result);
                    break;
                }
                default:
                    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_PARQUET),
                                    errmsg("Unsupported var type %u from data type %s(%u).", varType,
                                           parquet::TypeToString(physicalType).c_str(), physicalType)));
            }
            break;
        }
        default: {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_PARQUET),
                            errmsg("Unsupported data type : %s.", parquet::TypeToString(physicalType).c_str())));
        }
    }
    return result;
}

/*
 * The class which implements all the read-related operations upon the special column.
 */
template <typename ReaderType>
class ParquetColumnReaderImpl : public ParquetColumnReader {
public:
    ParquetColumnReaderImpl(const parquet::ColumnDescriptor *desc, uint32_t columnIndex, uint32_t mppColumnIndex,
                            ReaderState *readerState, const Var *var)
        : m_desc(desc),
          m_readerState(readerState),
          m_lessThanExpr(NULL),
          m_greaterThanExpr(NULL),
          m_internalContext(NULL),
          m_var(var),
          predicate(NULL),
          bloomFilter(NULL),
          checkBloomFilterOnRow(false),
          checkPredicateOnRow(false),
          m_convertToDatumFunc(NULL),
          m_checkEncodingLevel(readerState->checkEncodingLevel),
          m_skipRowsBuffer(0),
          m_columnIndex(columnIndex),
          m_mppColumnIndex(mppColumnIndex),
          m_numNullValuesRead(0),
          m_num_rows(0),
          m_num_row_groups(0),
          m_currentRowGroupIndex(0),
          m_totalRowsInCurrentRowGroup(0),
          m_rowsReadInCurrentRowGroup(0),
          m_rowBufferSize(0),
          m_rowBuffer(NULL)
    {
    }

    ~ParquetColumnReaderImpl() override
    {
        m_desc = NULL;
        m_readerState = NULL;
        m_lessThanExpr = NULL;
        m_greaterThanExpr = NULL;
        m_var = NULL;
        predicate = NULL;
        m_rowBuffer = NULL;
        m_convertToDatumFunc = NULL;
        m_internalContext = NULL;
        bloomFilter = NULL;
    }

    void begin(std::unique_ptr<GSInputStream> gsInputStream, const std::shared_ptr<parquet::FileMetaData> &fileMetaData)
    {
        predicate = m_readerState->hdfsScanPredicateArr[m_mppColumnIndex - 1];
        checkPredicateOnRow = list_length(predicate) > 0;

        m_internalContext =
            AllocSetContextCreate(m_readerState->persistCtx, "parquet column reader context for parquet dfs reader",
                                  ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

        MemoryContext oldContext = MemoryContextSwitchTo(m_internalContext);

        if ((m_var->vartype == BPCHAROID) && (m_var->vartypmod > static_cast<int32_t>(VARHDRSZ))) {
            m_rowBufferSize = m_var->vartypmod - static_cast<int32_t>(VARHDRSZ);
            m_rowBuffer = (char *)palloc0(sizeof(char) * (m_rowBufferSize + 1));
        }

        m_lessThanExpr = MakeOperatorExpression(const_cast<Var *>(m_var), BTLessEqualStrategyNumber);
        m_greaterThanExpr = MakeOperatorExpression(const_cast<Var *>(m_var), BTGreaterEqualStrategyNumber);

        initColumnReader(std::move(gsInputStream), fileMetaData);

        (void)MemoryContextSwitchTo(oldContext);
    }

    void Destroy() override
    {
        m_realParquetFileReader.reset(NULL);

        if (m_rowBuffer != NULL) {
            pfree(m_rowBuffer);
            m_rowBuffer = NULL;
        }

        releaseDataBuffers();

        /* Clear the internal memory context. */
        if (m_internalContext != NULL && m_internalContext != CurrentMemoryContext) {
            MemoryContextDelete(m_internalContext);
        }
    }

    void setRowGroupIndex(const uint64_t rowGroupIndex)
    {
        DFS_TRY()
        {
            m_currentRowGroupIndex = rowGroupIndex;
            m_rowGroupReader = m_realParquetFileReader->RowGroup(rowGroupIndex);
            m_columnReader = m_rowGroupReader->Column(m_columnIndex);
            m_skipRowsBuffer = 0;
            m_totalRowsInCurrentRowGroup = static_cast<uint64_t>(m_rowGroupReader->metadata()->num_rows());
            m_rowsReadInCurrentRowGroup = 0;
        }
        DFS_CATCH();
        DFS_ERRREPORT_WITHARGS(
            "Error occurs while read row index of parquet file %s because of %s, "
            "detail can be found in dn log of %s.",
            MOD_PARQUET, m_readerState->currentSplit->filePath);
    }

    void skip(const uint64_t numValues)
    {
        m_skipRowsBuffer += numValues;
    }

    void nextInternal(const uint64_t numRowsToRead)
    {
        Assert(numRowsToRead <= (m_totalRowsInCurrentRowGroup - m_rowsReadInCurrentRowGroup));

        releaseDataBuffers();

        uint64_t skipRecords = m_skipRowsBuffer;
        DFS_TRY()
        {
            auto typedReader = static_cast<ReaderType *>(m_columnReader.get());

            if (m_skipRowsBuffer > 0) {
                (void)typedReader->Skip((int64_t)m_skipRowsBuffer);
                m_skipRowsBuffer = 0;
            }

            m_numNullValuesRead = 0;
            errno_t rc = memset_s(m_notNullValues, BatchMaxSize, 1, BatchMaxSize);
            securec_check(rc, "\0", "\0");

            uint64_t numRowsRead = 0;
            int64_t numValuesRead = 0;
            int64_t numValuesCached = 0;

            while ((m_rowsReadInCurrentRowGroup < m_totalRowsInCurrentRowGroup) && (numRowsRead < numRowsToRead)) {
                if (numValuesRead > 0) {
                    cacheValuesDataInPreviousPage(m_values + numValuesCached, numValuesRead);
                    numValuesCached += numValuesRead;
                }

                numValuesRead = 0;

                auto rowsRead = (uint64_t)typedReader->ReadBatch(numRowsToRead - numRowsRead, m_defLevels, m_repLevels,
                                                                 m_values + numRowsRead - m_numNullValuesRead,
                                                                 &numValuesRead);

                for (uint64_t i = 0; i < rowsRead; i++) {
                    m_notNullValues[numRowsRead + i] = m_defLevels[i] == m_desc->max_definition_level();
                }

                numRowsRead += rowsRead;
                m_rowsReadInCurrentRowGroup += rowsRead;
                m_numNullValuesRead += (rowsRead - numValuesRead);
            }
            Assert(numRowsRead == numRowsToRead);

            // Add spacing for null entries. As we have filled the buffer from the front,
            // we need to add the spacing from the back.
            int valuesToMove = numRowsToRead - m_numNullValuesRead;
            for (int i = numRowsRead - 1; i >= 0; i--) {
                if (!isNullValue(i)) {
                    m_values[i] = m_values[--valuesToMove];
                }
            }
        }
        DFS_CATCH();
        DFS_ERRREPORT_WITHARGS(
            "Error occurs while reading parquet file %s because of %s, "
            "detail can be found in dn log of %s.",
            MOD_PARQUET, m_readerState->currentSplit->filePath);

        if (errOccur) {
            ereport(WARNING,
                    (errmodule(MOD_PARQUET),
                     errmsg("Exception detail is relation: %s, file: %s, column id: "
                            "%u, skip number: %lu, read number: %lu.",
                            RelationGetRelationName(m_readerState->scanstate->ss_currentRelation),
                            m_readerState->currentSplit->filePath, m_columnIndex, skipRecords, numRowsToRead)));
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_PARQUET),
                            errmsg("Error occurs while reading parquet file %s, "
                                   "detail can be found in dn log of %s.",
                                   m_readerState->currentSplit->filePath, g_instance.attr.attr_common.PGXCNodeName)));
        }
    }

    int32_t fillScalarVector(uint64_t numRowsToRead, const bool *isSelected, ScalarVector *scalorVector)
    {
        ScalarDesc desc = scalorVector->m_desc;

        if (hasNullValues()) {
            if (desc.encoded) {
                fillScalarVectorInternal<true, true>(numRowsToRead, isSelected, scalorVector);
            } else {
                fillScalarVectorInternal<true, false>(numRowsToRead, isSelected, scalorVector);
            }
        } else {
            if (desc.encoded) {
                fillScalarVectorInternal<false, true>(numRowsToRead, isSelected, scalorVector);
            } else {
                fillScalarVectorInternal<false, false>(numRowsToRead, isSelected, scalorVector);
            }
        }

        return scalorVector->m_rows;
    }

    bool checkBloomFilter(uint64_t index) const override
    {
        /*
         * Since libparquet doesn't support BloomFilter yet, we have to return true to
         * indicate that these rows cannot be skipped.
         */
        return true;
    }

    Node *buildColRestriction(RestrictionType type, parquet::ParquetFileReader *fileReader,
                              uint64_t rowGroupIndex) const override
    {
        Datum minValue = 0;
        Datum maxValue = 0;
        bool hasMaximum = false;
        bool hasMinimum = false;
        Node *baseRestriction = NULL;
        int32_t hasStatistics = 0;

        try {
            switch (type) {
                case ROW_GROUP: {
                    hasStatistics = getStatistics(fileReader, rowGroupIndex, &minValue, &hasMinimum, &maxValue,
                                                  &hasMaximum);
                    break;
                }
                default: {
                }
            }
        } catch (abi::__forced_unwind &) {
            throw;
        } catch (std::exception &ex) {
            hasStatistics = 0;
        } catch (...) {
        }

        if (hasStatistics != 0) {
            baseRestriction = MakeBaseConstraintWithExpr(m_lessThanExpr, m_greaterThanExpr, minValue, maxValue,
                                                         hasMinimum, hasMaximum);
        }

        return baseRestriction;
    }

private:
    void initColumnReader(std::unique_ptr<GSInputStream> gsInputStream,
                          const std::shared_ptr<parquet::FileMetaData> &fileMetaData)
    {
        std::unique_ptr<parquet::RandomAccessSource> source(
            new ParquetInputStreamAdapter(m_internalContext, std::move(gsInputStream)));
        m_realParquetFileReader = parquet::ParquetFileReader::Open(std::move(source),
                                                                   parquet::default_reader_properties(), fileMetaData);

        m_num_rows = m_realParquetFileReader->metadata()->num_rows();
        m_num_row_groups = m_realParquetFileReader->metadata()->num_row_groups();

        if (!matchDataTypeWithPsql()) {
            auto physicalTypeString = parquet::TypeToString(m_desc->physical_type());
            auto logicalTypeString = parquet::ConvertedTypeToString(m_desc->converted_type());

            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_PARQUET),
                            errmsg("Error occurred while reading column %d: PARQUET and mpp "
                                   "types do not match, PARQUET type is %s(%s) and mpp type is %s.",
                                   m_var->varattno, physicalTypeString.c_str(), logicalTypeString.c_str(),
                                   format_type_with_typemod(m_var->vartype, m_var->vartypmod))));
        }

        bindConvertFunc<true>();
    }

    bool match_data_type_with_psql_int32(const Oid varType) const
    {
        bool matches = false;
        if (varType == NUMERICOID) {
            /* no precision */
            if (-1 == m_var->vartypmod) {
                matches = true;
            } else { /* Both scale and precision need to be matched when precision is defined. */
                int32_t typemod = m_var->vartypmod - VARHDRSZ;
                int32_t varPrecision = (int32_t)((((uint32_t)typemod) >> 16) & 0xffff);
                int32_t varScale = typemod & 0xffff;
                matches = ((varScale == m_desc->type_scale()) && (varPrecision == m_desc->type_precision()));
            }
        } else {
            matches = (varType == INT1OID || varType == INT2OID || varType == INT4OID || varType == DATEOID ||
                       varType == TIMESTAMPOID);
        }
        return matches;
    }

    bool match_data_type_with_psql_int64(const Oid varType) const
    {
        bool matches = false;
        if (varType == NUMERICOID) {
            /* no precision */
            if (-1 == m_var->vartypmod) {
                matches = true;
            } else { /* Both scale and precision need to be matched when precision is defined. */
                int32_t typemod = m_var->vartypmod - VARHDRSZ;
                int32_t varPrecision = (int32_t)((((uint32_t)typemod) >> 16) & 0xffff);
                int32_t varScale = typemod & 0xffff;
                matches = ((varScale == m_desc->type_scale()) && (varPrecision == m_desc->type_precision()));
            }
        } else {
            matches = (varType == INT8OID || varType == OIDOID || varType == CASHOID);
        }
        return matches;
    }

    bool match_data_type_with_psql_fixed_array(const Oid varType) const
    {
        bool matches = false;
        if (varType == NUMERICOID) {
            /* no precision */
            if (-1 == m_var->vartypmod) {
                matches = true;
            } else { /* Both scale and precision need to be matched when precision is defined. */
                int32_t typemod = m_var->vartypmod - VARHDRSZ;
                int32_t varPrecision = (int32_t)((((uint32_t)typemod) >> 16) & 0xffff);
                int32_t varScale = typemod & 0xffff;
                matches = ((varScale == m_desc->type_scale()) && (varPrecision == m_desc->type_precision()));
            }
        }
        return matches;
    }

    /*
     * Check if the field type of current column matches the var type defined in
     * relation. For now, we use one-to-one type mapping mechanism.
     * @return true if The two type matches or return false.
     */
    bool matchDataTypeWithPsql() const
    {
        bool matches = false;
        Oid varType = m_var->vartype;

        switch (m_desc->physical_type()) {
            case parquet::Type::BOOLEAN: {
                matches = varType == BOOLOID;
                break;
            }
            case parquet::Type::INT32: {
                matches = match_data_type_with_psql_int32(varType);
                break;
            }
            case parquet::Type::INT64: {
                matches = match_data_type_with_psql_int64(varType);
                break;
            }
            case parquet::Type::INT96: {
                matches = (varType == TIMESTAMPOID || varType == TIMESTAMPTZOID);
                break;
            }
            case parquet::Type::FLOAT: {
                matches = varType == FLOAT4OID;
                break;
            }
            case parquet::Type::DOUBLE: {
                matches = varType == FLOAT8OID;
                break;
            }
            case parquet::Type::BYTE_ARRAY: {
                matches = (varType == BPCHAROID || varType == VARCHAROID || varType == TEXTOID);
                break;
            }
            case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
                matches = match_data_type_with_psql_fixed_array(varType);
                break;
            }
            default: {
                break;
            }
        }

        return matches;
    }

    template <bool decimal128>
    void bindConvertFunc()
    {
        switch (m_var->vartype) {
            case INT1OID:
                m_convertToDatumFunc = &convertToDatumT<INT1OID>;
                break;
            case INT2OID:
                m_convertToDatumFunc = &convertToDatumT<INT2OID>;
                break;
            case INT4OID:
                m_convertToDatumFunc = &convertToDatumT<INT4OID>;
                break;
            case INT8OID:
                m_convertToDatumFunc = &convertToDatumT<INT8OID>;
                break;
            case INTERVALOID:
                m_convertToDatumFunc = &convertToDatumT<INTERVALOID>;
                break;
            case OIDOID:
                m_convertToDatumFunc = &convertToDatumT<OIDOID>;
                break;
            case TINTERVALOID:
                m_convertToDatumFunc = &convertToDatumT<TINTERVALOID>;
                break;
            case TIMESTAMPOID:
                m_convertToDatumFunc = &convertToDatumT<TIMESTAMPOID>;
                break;
            case TIMESTAMPTZOID:
                m_convertToDatumFunc = &convertToDatumT<TIMESTAMPTZOID>;
                break;
            case DATEOID:
                m_convertToDatumFunc = &convertToDatumT<DATEOID>;
                break;
            case TIMEOID:
                m_convertToDatumFunc = &convertToDatumT<TIMEOID>;
                break;
            case TIMETZOID:
                m_convertToDatumFunc = &convertToDatumT<TIMETZOID>;
                break;
            case CHAROID:
                m_convertToDatumFunc = &convertToDatumT<CHAROID>;
                break;
            case NVARCHAR2OID:
                m_convertToDatumFunc = &convertToDatumT<NVARCHAR2OID>;
                break;
            case NUMERICOID:
                m_convertToDatumFunc = &convertToDatumT<NUMERICOID>;
                break;
            case SMALLDATETIMEOID:
                m_convertToDatumFunc = &convertToDatumT<SMALLDATETIMEOID>;
                break;
            case CASHOID:
                m_convertToDatumFunc = &convertToDatumT<CASHOID>;
                break;
            case TEXTOID:
                m_convertToDatumFunc = &convertToDatumT<TEXTOID>;
                break;
            default:
                m_convertToDatumFunc = &convertToDatumT<InvalidOid>;
        }
    }

    template <bool hasNull, bool encoded>
    void fillScalarVectorInternal(uint64_t numRowsToRead, const bool *isSelected, ScalarVector *scalorVector);

    void predicateFilter(uint64_t numValues, bool *isSelected) override;

private:
    void fillScalarVectorInternalForChar(uint64_t numRowsToRead, const bool *isSelected, ScalarVector *vec);
    void fillScalarVectorInternalForVarchar(uint64_t numRowsToRead, const bool *isSelected, ScalarVector *vec);
    void fillScalarVectorInternalForOthers(uint64_t numRowsToRead, const bool *isSelected, ScalarVector *vec);

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

    void setBloomFilter(filter::BloomFilter *_bloomFilter) override
    {
        Assert(_bloomFilter != NULL);
        bloomFilter = _bloomFilter;

        /* We do not check bloom filter on each row for string type, since the cost is too big,
         * unless there is only one element in the bloom filter.
         */
        if (bloomFilter->getType() != EQUAL_BLOOM_FILTER &&
            (NOT_STRING(bloomFilter->getDataType()) || bloomFilter->getNumValues() == 1)) {
            checkBloomFilterOnRow = true;
        }
    }

    /*
     * Check if the current column of ORC file has predicates.
     * @return true: The predicate of the column exists;
     *             false: The predicate of the column does not exists;
     */
    bool hasPredicate() const override
    {
        return checkPredicateOnRow || checkBloomFilterOnRow;
    }

    void cacheValuesDataInPreviousPage(typename ReaderType::T *values, int64_t count)
    {
        int64_t totalSize = 0;
        int64_t cachedSize = 0;

        if (parquet::Type::BYTE_ARRAY == m_desc->physical_type()) {
            auto typedValues = (struct parquet::ByteArray *)values;

            for (int64_t i = 0; i < count; i++) {
                totalSize += typedValues[i].len;
            }

            AutoContextSwitch newMemCnxt(m_internalContext);
            uint8_t *dataBuffer = (uint8_t *)palloc0(totalSize);
            m_dataBuffers.push_back(dataBuffer);

            for (int64_t i = 0; i < count; i++) {
                errno_t rc = memcpy_s(dataBuffer + cachedSize, totalSize - cachedSize, typedValues[i].ptr,
                                      typedValues[i].len);
                securec_check(rc, "\0", "\0");
                typedValues[i].ptr = dataBuffer + cachedSize;
                cachedSize += typedValues[i].len;
            }
        } else if (parquet::Type::FIXED_LEN_BYTE_ARRAY == m_desc->physical_type()) {
            auto typedValues = (struct parquet::FixedLenByteArray *)values;
            auto typeLength = m_desc->type_length();

            totalSize = typeLength * count;

            AutoContextSwitch newMemCnxt(m_internalContext);
            uint8_t *dataBuffer = (uint8_t *)palloc0(totalSize);
            m_dataBuffers.push_back(dataBuffer);

            for (int64_t i = 0; i < count; i++) {
                errno_t rc = memcpy_s(dataBuffer + cachedSize, totalSize - cachedSize, typedValues[i].ptr, typeLength);
                securec_check(rc, "\0", "\0");

                typedValues[i].ptr = dataBuffer + cachedSize;
                cachedSize += typeLength;
            }
        }
    }

    void releaseDataBuffers()
    {
        if ((parquet::Type::BYTE_ARRAY != m_desc->physical_type()) &&
            (parquet::Type::FIXED_LEN_BYTE_ARRAY != m_desc->physical_type())) {
            return;
        }

        for (auto const dataBuffer : m_dataBuffers) {
            pfree(dataBuffer);
        }
        m_dataBuffers.clear();
    }

    bool nullFilter(bool *isSelected, uint64_t rowIdx)
    {
        bool filter = false;

        if (!isSelected[rowIdx]) {
            filter = true;
        } else if (hasNullValues() && isNullValue(rowIdx)) { /* Check the null value. */
            isSelected[rowIdx] = HdfsPredicateCheckNull<NullWrapper>(predicate);
            filter = true;
        }
        return filter;
    }

    int32_t getStatistics(parquet::ParquetFileReader *fileReader, uint64_t currentRowGroupIndex, Datum *minDatum,
                          bool *hasMinimum, Datum *maxDatum, bool *hasMaximum) const
    {
#define SET_SIMPLE_MIN_MAX_STATISTICS(state, func, minvalue, maxValue) do { \
    *hasMinimum = *hasMaximum = (state)->HasMinMax();              \
    if ((state)->HasMinMax()) {                                    \
        *minDatum = func(minvalue);                                \
        *maxDatum = func(maxValue);                                \
        ret = 1;                                                   \
    }                                                              \
} while (0)

#define SET_COMPLEX_MIN_MAX_STATISTICS(state, func, minvalue, maxValue) do { \
    *hasMinimum = *hasMaximum = (state)->HasMinMax();                                                  \
    if ((state)->HasMinMax()) {                                                                        \
        *minDatum = DirectFunctionCall3(func, CStringGetDatum(minvalue), ObjectIdGetDatum(InvalidOid), \
                                        Int32GetDatum(m_var->vartypmod));                              \
        *maxDatum = DirectFunctionCall3(func, CStringGetDatum(maxValue), ObjectIdGetDatum(InvalidOid), \
                                        Int32GetDatum(m_var->vartypmod));                              \
        ret = 1;                                                                                       \
    }                                                                                                  \
} while (0)

        const parquet::RowGroupStatistics *statistics = NULL;
        auto fileMetadata = fileReader->metadata();
        auto rowGroupMetadata = fileMetadata->RowGroup(currentRowGroupIndex);
        auto columnChunk = rowGroupMetadata->ColumnChunk(m_columnIndex);

        std::shared_ptr<parquet::RowGroupStatistics> stats = columnChunk->statistics();
        statistics = stats.get();

        int32_t ret = 0;
        if (statistics == NULL || !statistics->HasMinMax()) {
            return ret;
        }

        switch (m_desc->physical_type()) {
            case parquet::Type::BOOLEAN: {
                const parquet::BoolStatistics *tmpStat = (const parquet::BoolStatistics *)statistics;
                SET_SIMPLE_MIN_MAX_STATISTICS(tmpStat, BoolGetDatum, tmpStat->min(), tmpStat->max());
                break;
            }
            case parquet::Type::INT32: {
                const parquet::Int32Statistics *tmpStat = (const parquet::Int32Statistics *)statistics;

                switch (m_var->vartype) {
                    case INT1OID: {
                        SET_SIMPLE_MIN_MAX_STATISTICS(tmpStat, Int8GetDatum, (uint8)tmpStat->min(),
                                                      (uint8)tmpStat->max());
                        break;
                    }
                    case INT2OID: {
                        SET_SIMPLE_MIN_MAX_STATISTICS(tmpStat, Int16GetDatum, (uint16)tmpStat->min(),
                                                      (uint16)tmpStat->max());
                        break;
                    }
                    case DATEOID:
                    case TIMESTAMPOID: {
                        SET_SIMPLE_MIN_MAX_STATISTICS(
                            tmpStat, TimestampGetDatum,
                            date2timestamp(static_cast<DateADT>(tmpStat->min() - PARQUET_PSQL_EPOCH_IN_DAYS)),
                            date2timestamp(static_cast<DateADT>(tmpStat->max() - PARQUET_PSQL_EPOCH_IN_DAYS)));
                        break;
                    }
                    case NUMERICOID: {
                        *hasMinimum = *hasMaximum = tmpStat->HasMinMax();
                        if (tmpStat->HasMinMax()) {
                            *minDatum = makeNumeric64((int64_t)tmpStat->min(), m_desc->type_scale());
                            *maxDatum = makeNumeric64((int64_t)tmpStat->max(), m_desc->type_scale());
                        }
                        break;
                    }
                    default: {
                        SET_SIMPLE_MIN_MAX_STATISTICS(tmpStat, Int32GetDatum, (uint32)tmpStat->min(),
                                                      (uint32)tmpStat->max());
                        break;
                    }
                }
                ret = 1;
                break;
            }
            case parquet::Type::INT64: {
                const parquet::Int64Statistics *tmpStat = static_cast<const parquet::Int64Statistics *>(statistics);

                switch (m_var->vartype) {
                    case NUMERICOID: {
                        *hasMinimum = *hasMaximum = tmpStat->HasMinMax();
                        if (tmpStat->HasMinMax()) {
                            *minDatum = makeNumeric64(tmpStat->min(), m_desc->type_scale());
                            *maxDatum = makeNumeric64(tmpStat->max(), m_desc->type_scale());
                        }
                        break;
                    }
                    default: {
                        SET_SIMPLE_MIN_MAX_STATISTICS(tmpStat, Int64GetDatum, tmpStat->min(), tmpStat->max());
                        break;
                    }
                }
                ret = 1;
                break;
            }
            case parquet::Type::INT96: {
                const parquet::Int96Statistics *tmpStat = static_cast<const parquet::Int96Statistics *>(statistics);

                switch (m_var->vartype) {
                    case TIMESTAMPOID: {
                        SET_SIMPLE_MIN_MAX_STATISTICS(tmpStat, TimestampGetDatum,
                                                      nanoSecondsToPsqlTimestamp(dfs::
                                                                                 Int96GetNanoSeconds(tmpStat->min())),
                                                      nanoSecondsToPsqlTimestamp(dfs::
                                                                                 Int96GetNanoSeconds(tmpStat->max())));
                        ret = 1;
                        break;
                    }
                    default: {
                        ret = 0;
                        break;
                    }
                }
                break;
            }
            case parquet::Type::FLOAT: {
                const parquet::FloatStatistics *tmpStat = static_cast<const parquet::FloatStatistics *>(statistics);
                SET_SIMPLE_MIN_MAX_STATISTICS(tmpStat, Float4GetDatum, static_cast<float>(tmpStat->min()),
                                              static_cast<float>(tmpStat->max()));
                break;
            }
            case parquet::Type::DOUBLE: {
                const parquet::DoubleStatistics *tmpStat = static_cast<const parquet::DoubleStatistics *>(statistics);
                SET_SIMPLE_MIN_MAX_STATISTICS(tmpStat, Float8GetDatum, static_cast<double>(tmpStat->min()),
                                              static_cast<double>(tmpStat->max()));
                break;
            }
            case parquet::Type::BYTE_ARRAY: {
                const parquet::ByteArrayStatistics *tmpStat =
                    static_cast<const parquet::ByteArrayStatistics *>(statistics);

                auto minValuePtr = (char *)tmpStat->min().ptr;
                auto maxValuePtr = (char *)tmpStat->max().ptr;
                if (NULL == minValuePtr || maxValuePtr == NULL) {
                    ret = 0;
                    break;
                }

                switch (m_var->vartype) {
                    case BPCHAROID: {
                        SET_COMPLEX_MIN_MAX_STATISTICS(tmpStat, bpcharin, minValuePtr, maxValuePtr);
                        break;
                    }
                    case VARCHAROID: {
                        SET_COMPLEX_MIN_MAX_STATISTICS(tmpStat, varcharin, minValuePtr, maxValuePtr);
                        break;
                    }
                    case TEXTOID: {
                        SET_SIMPLE_MIN_MAX_STATISTICS(tmpStat, CStringGetTextDatum, minValuePtr, maxValuePtr);
                        break;
                    }
                    default: {
                        ret = 0;
                        break;
                    }
                }
                break;
            }
            case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
                const parquet::FLBAStatistics *tmpStat = static_cast<const parquet::FLBAStatistics *>(statistics);

                switch (m_var->vartype) {
                    case NUMERICOID: {
                        *hasMinimum = *hasMaximum = tmpStat->HasMinMax();
                        if (tmpStat->HasMinMax()) {
                            convertBufferToDatum(tmpStat->min().ptr, m_desc->type_length(), m_desc->type_scale(),
                                                 *minDatum);
                            convertBufferToDatum(tmpStat->max().ptr, m_desc->type_length(), m_desc->type_scale(),
                                                 *maxDatum);
                            ret = 1;
                        }
                        break;
                    }
                    default: {
                        ret = 0;
                        break;
                    }
                }
                break;
            }
            default: {
                break;
            }
        }
        return ret;
    }

    inline void increaseBloomFilterRows(bool *isSelected, const bool tmpValue)
    {
        if (checkBloomFilterOnRow && (*isSelected)) {
            if (!bloomFilter->includeValue(tmpValue)) {
                *isSelected = false;
                m_readerState->bloomFilterRows += 1;
            }
        }
    }

private:
    const parquet::ColumnDescriptor *m_desc;
    ReaderState *m_readerState;
    /* The exprs for building the restriction according to the min/max value. */
    OpExpr *m_lessThanExpr;
    OpExpr *m_greaterThanExpr;
    MemoryContext m_internalContext;
    /* The var of the corresponding column define in relation. */
    const Var *m_var;

    /* The predicates list which is pushed down into the current column. */
    List *predicate;

    /*
     * The bloom filter of the current column which is generated by the
     * (dynamic/static) predicate.
     */
    filter::BloomFilter *bloomFilter;

    /* Flag to indicate whether we need to check bloom filter/predicate on row level. */
    bool checkBloomFilterOnRow;
    bool checkPredicateOnRow;

    /* The function pointer to convert different data type to datum. */
    convertToDatum m_convertToDatumFunc;

    /* The level of encoding check,2 = high, 1 = low, 0 = no check */
    const int32_t m_checkEncodingLevel;

    /* The buffer of rows to be skipped. */
    uint64_t m_skipRowsBuffer;

    const uint32_t m_columnIndex;
    const uint32_t m_mppColumnIndex;
    int64_t m_numNullValuesRead;
    int64_t m_num_rows;
    int64_t m_num_row_groups;
    uint64_t m_currentRowGroupIndex;
    uint64_t m_totalRowsInCurrentRowGroup;
    uint64_t m_rowsReadInCurrentRowGroup;
    typename ReaderType::T m_values[BatchMaxSize];
    int32_t m_rowBufferSize;
    char *m_rowBuffer;
    bool m_notNullValues[BatchMaxSize];
    int16_t m_defLevels[BatchMaxSize];
    int16_t m_repLevels[BatchMaxSize];

    std::list<uint8_t *> m_dataBuffers;
    std::unique_ptr<parquet::ParquetFileReader> m_realParquetFileReader;
    std::shared_ptr<parquet::RowGroupReader> m_rowGroupReader;
    std::shared_ptr<parquet::ColumnReader> m_columnReader;
};

template <typename ReaderType>
template <bool hasNull, bool encoded>
void ParquetColumnReaderImpl<ReaderType>::fillScalarVectorInternal(uint64_t numRowsToRead, 
    const bool *isSelected, ScalarVector *vec)
{
    fillScalarVectorInternalForOthers(numRowsToRead, isSelected, vec);
}

template <>
template <bool hasNull, bool encoded>
void ParquetColumnReaderImpl<parquet::ByteArrayReader>::fillScalarVectorInternal(uint64_t numRowsToRead,
    const bool *isSelected, ScalarVector *vec)
{
    auto varType = m_var->vartype;
    switch (varType) {
        case BPCHAROID:
            fillScalarVectorInternalForChar(numRowsToRead, isSelected, vec);
            break;
        case VARCHAROID:
            fillScalarVectorInternalForVarchar(numRowsToRead, isSelected, vec);
            break;
        case TEXTOID:
            fillScalarVectorInternalForOthers(numRowsToRead, isSelected, vec);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_PARQUET),
                            errmsg("Unsupported var type %u from data type %s(%u).", varType,
                                   parquet::TypeToString(m_desc->physical_type()).c_str(), m_desc->physical_type())));
    }
}

template <typename ReaderType>
void ParquetColumnReaderImpl<ReaderType>::fillScalarVectorInternalForChar(
    uint64_t numRowsToRead, const bool *isSelected, ScalarVector *vec)
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
    int32_t maxLen = 0;

    for (uint64_t i = 0; i < numRowsToRead; ++i) {
        if (isSelected[i]) {
            Assert(offset < BatchMaxSize);

            if (hasNullValues() && isNullValue(i)) {
                vec->SetNull(offset);
            } else {
                auto value = m_values[i];
                auto length = (int32_t)value.len;

                /* Check compatibility and convert '' into null if the db is A_FORMAT. */
                if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && length == 0) {
                    vec->SetNull(offset);
                } else {
                    char *tmpValue = (char *)value.ptr;

                    switch (m_checkEncodingLevel) {
                        case NO_ENCODING_CHECK: {
                            Assert(m_var->vartypmod == -1 || length == m_var->vartypmod - VARHDRSZ);
                            (void)vec->AddBPCharWithoutHeader(tmpValue, length, length, offset);
                            break;
                        }
                        case LOW_ENCODING_CHECK:
                        case HIGH_ENCODING_CHECK: {
                            meetError = false;

                            /* check and convert */
                            char *cvt = pg_to_server_withfailure(tmpValue, length, encoding, m_checkEncodingLevel,
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
    vec->m_rows = offset;
}

template <typename ReaderType>
void ParquetColumnReaderImpl<ReaderType>::fillScalarVectorInternalForVarchar(uint64_t numRowsToRead, 
    const bool *isSelected, ScalarVector *vec)
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

    for (uint64_t i = 0; i < numRowsToRead; ++i) {
        if (isSelected[i]) {
            Assert(offset < BatchMaxSize);

            if (hasNullValues() && isNullValue(i)) {
                vec->SetNull(offset);
            } else {
                auto value = m_values[i];
                auto length = (int32_t)value.len;

                /* Check compatibility and convert '' into null if the db is A_FORMAT. */
                if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && length == 0) {
                    vec->SetNull(offset);
                } else {
                    char *tmpValue = (char *)value.ptr;

                    switch (m_checkEncodingLevel) {
                        case NO_ENCODING_CHECK: {
                            Assert(m_var->vartypmod == -1 || length <= m_var->vartypmod - VARHDRSZ);
                            (void)vec->AddVarCharWithoutHeader(tmpValue, length, offset);
                            break;
                        }
                        case LOW_ENCODING_CHECK:
                        case HIGH_ENCODING_CHECK: {
                            meetError = false;

                            /* check and convert */
                            char *cvt = pg_to_server_withfailure(tmpValue, length, encoding, m_checkEncodingLevel,
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
    vec->m_rows = offset;
}

template <typename ReaderType>
void ParquetColumnReaderImpl<ReaderType>::fillScalarVectorInternalForOthers(uint64_t numRowsToRead, 
    const bool *isSelected, ScalarVector *vec)
{
    int32_t encoding = m_readerState->fdwEncoding;
    /* the offset in the current vector batch to be filled */
    int32_t offset = vec->m_rows;
    int32_t errorCount = 0;
    bool meetError = false;
    bool internalNull = false;

    for (uint64_t i = 0; i < numRowsToRead; ++i) {
        if (isSelected[i]) {
            Assert(offset < BatchMaxSize);

            internalNull = false;
            meetError = false;

            if (hasNullValues() && isNullValue(i)) {
                vec->SetNull(offset);
            } else {
                Datum dValue = m_convertToDatumFunc(m_values, i, m_desc->physical_type(), m_desc->type_scale(),
                                                    m_desc->type_length(), internalNull, encoding, m_checkEncodingLevel,
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
    vec->m_rows = offset;
}

template <typename ReaderType>
void ParquetColumnReaderImpl<ReaderType>::predicateFilter(uint64_t numValues, bool *isSelected)
{
    Oid varType = m_var->vartype;
    void *primitiveBatch = m_values;

    switch (m_desc->physical_type()) {
        case parquet::Type::BOOLEAN: {
            bool *data = static_cast<bool *>(primitiveBatch);
            for (uint64_t i = 0; i < numValues; i++) {
                if (!nullFilter(isSelected, i)) {
                    bool tmpValue = data[i];
                    if (checkPredicateOnRow) {
#ifdef ENABLE_LLVM_COMPILE
                        isSelected[i] = HdfsPredicateCheckValueIntForLlvm<BoolWrapper, bool>(tmpValue, predicate);
#else
                        isSelected[i] = HdfsPredicateCheckValue<BoolWrapper, bool>(tmpValue, predicate);
#endif
                    }

                    increaseBloomFilterRows(&isSelected[i], tmpValue);
                }
            }
            break;
        }
        case parquet::Type::INT32: {
            int32_t *data = static_cast<int32_t *>(primitiveBatch);
            switch (varType) {
                case DATEOID:
                case TIMESTAMPOID: {
                    for (uint64_t i = 0; i < numValues; i++) {
                        if (!nullFilter(isSelected, i)) {
                            Timestamp tmpValue =
                                date2timestamp(static_cast<DateADT>(data[i] - PARQUET_PSQL_EPOCH_IN_DAYS));
                            isSelected[i] = HdfsPredicateCheckValue<TimestampWrapper, Timestamp>(tmpValue, predicate);
                        }
                    }
                    break;
                }
                default: {
                    for (uint64_t i = 0; i < numValues; i++) {
                        if (!nullFilter(isSelected, i)) {
                            int64_t tmpValue = static_cast<int64_t>(data[i]);
                            if (checkPredicateOnRow) {
#ifdef ENABLE_LLVM_COMPILE
                                isSelected[i] = HdfsPredicateCheckValueIntForLlvm<Int64Wrapper, int64_t>(tmpValue,
                                                                                                         predicate);
#else
                                isSelected[i] = HdfsPredicateCheckValue<Int64Wrapper, int64_t>(tmpValue, predicate);
#endif
                            }

                            increaseBloomFilterRows(&isSelected[i], tmpValue);
                        }
                    }
                    break;
                }
            }
            break;
        }
        case parquet::Type::INT64: {
            int64_t *data = static_cast<int64_t *>(primitiveBatch);
            for (uint64_t i = 0; i < numValues; i++) {
                if (!nullFilter(isSelected, i)) {
                    int64_t tmpValue = static_cast<int64_t>(data[i]);
                    if (checkPredicateOnRow) {
#ifdef ENABLE_LLVM_COMPILE
                        isSelected[i] = HdfsPredicateCheckValueIntForLlvm<Int64Wrapper, int64_t>(tmpValue, predicate);
#else
                        isSelected[i] = HdfsPredicateCheckValue<Int64Wrapper, int64_t>(tmpValue, predicate);
#endif
                    }

                    increaseBloomFilterRows(&isSelected[i], tmpValue);
                }
            }
            break;
        }
        case parquet::Type::INT96: {
            switch (varType) {
                case TIMESTAMPOID:
                case TIMESTAMPTZOID: {
                    parquet::Int96 *data = static_cast<parquet::Int96 *>(primitiveBatch);
                    for (uint64_t i = 0; i < numValues; i++) {
                        if (!nullFilter(isSelected, i)) {
                            parquet::Int96 tmpValue = data[i];

                            auto nanoSeconds = dfs::Int96GetNanoSeconds(tmpValue);
                            nanoSeconds -= (PARQUET_PSQL_EPOCH_IN_DAYS * NANOSECONDS_PER_DAY);
                            nanoSeconds += epochOffsetDiff;
                            Timestamp timestamp = (Timestamp)(nanoSeconds / NANOSECONDS_PER_MICROSECOND);

                            isSelected[i] = HdfsPredicateCheckValue<TimestampWrapper, Timestamp>(timestamp, predicate);
                        }
                    }

                    break;
                }
                default:
                    break;
            }
            break;
        }
        case parquet::Type::FLOAT: {
            float *data = static_cast<float *>(primitiveBatch);
            for (uint64_t i = 0; i < numValues; i++) {
                if (!nullFilter(isSelected, i)) {
                    double tmpValue = static_cast<double>(data[i]);
                    if (checkPredicateOnRow) {
#ifdef ENABLE_LLVM_COMPILE
                        isSelected[i] = HdfsPredicateCheckValueIntForLlvm<Float8Wrapper, double>(tmpValue, predicate);
#else
                        isSelected[i] = HdfsPredicateCheckValue<Float8Wrapper, double>(tmpValue, predicate);
#endif
                    }

                    increaseBloomFilterRows(&isSelected[i], tmpValue);
                }
            }
            break;
        }
        case parquet::Type::DOUBLE: {
            double *data = static_cast<double *>(primitiveBatch);
            for (uint64_t i = 0; i < numValues; i++) {
                if (!nullFilter(isSelected, i)) {
                    double tmpValue = static_cast<double>(data[i]);
                    if (checkPredicateOnRow) {
#ifdef ENABLE_LLVM_COMPILE
                        isSelected[i] = HdfsPredicateCheckValueIntForLlvm<Float8Wrapper, double>(tmpValue, predicate);
#else
                        isSelected[i] = HdfsPredicateCheckValue<Float8Wrapper, double>(tmpValue, predicate);
#endif
                    }

                    increaseBloomFilterRows(&isSelected[i], tmpValue);
                }
            }
            break;
        }
        case parquet::Type::BYTE_ARRAY: {
            auto data = static_cast<parquet::ByteArray *>(primitiveBatch);

            switch (varType) {
                case BPCHAROID: {
                    for (uint64_t i = 0; i < numValues; i++) {
                        if (!nullFilter(isSelected, i)) {
                            auto baValue = data[i];
                            char *tmpValue = (char *)baValue.ptr;
                            int64_t length = static_cast<int64_t>(baValue.len);

                            /* Check compatibility and convert '' into null if the db is A_FORMAT. */
                            if (A_FORMAT == u_sess->attr.attr_sql.sql_compatibility && length == 0) {
                                tmpValue = NULL;
                            }

                            if (checkPredicateOnRow) {
                                if (tmpValue == NULL) {
                                    setAsNullValue(i);
                                    isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(predicate);
                                } else {
                                    errno_t rc = memcpy_s(m_rowBuffer, m_rowBufferSize, tmpValue, length);
                                    securec_check(rc, "\0", "\0");
                                    if (length < m_rowBufferSize) {
                                        rc = memset_s(m_rowBuffer + length, m_rowBufferSize - length, 0x20,
                                                      m_rowBufferSize - length);
                                        securec_check(rc, "\0", "\0");
                                    }

                                    isSelected[i] = HdfsPredicateCheckValue<StringWrapper, char *>(m_rowBuffer,
                                                                                                   predicate);
                                }
                            }

                            increaseBloomFilterRows(&isSelected[i], tmpValue);
                        }
                    }
                    break;
                }
                default: {
                    for (uint64_t i = 0; i < numValues; i++) {
                        if (!nullFilter(isSelected, i)) {
                            auto baValue = data[i];
                            char *tmpValue = (char *)baValue.ptr;
                            int64_t length = static_cast<int64_t>(baValue.len);
                            char lastChar = '\0';

                            /* Check compatibility and convert '' into null if the db is A_FORMAT. */
                            if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && length == 0) {
                                tmpValue = NULL;
                            } else {
                                lastChar = tmpValue[length];
                                tmpValue[length] = '\0';
                            }

                            if (checkPredicateOnRow) {
                                if (tmpValue == NULL) {
                                    setAsNullValue(i);
                                    isSelected[i] = HdfsPredicateCheckNull<NullWrapper>(predicate);
                                } else {
                                    isSelected[i] = HdfsPredicateCheckValue<StringWrapper, char *>(tmpValue, predicate);
                                }
                            }

                            increaseBloomFilterRows(&isSelected[i], tmpValue);

                            if (tmpValue != NULL) {
                                tmpValue[length] = lastChar;
                            }
                        }
                    }
                    break;
                }
            }
            break;
        }
        case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
            auto length = m_desc->type_length();
            auto data = static_cast<parquet::FixedLenByteArray *>(primitiveBatch);

            switch (varType) {
                case NUMERICOID: {
                    /* Only short numeric will be pushed down, so we just handle the Decimal64VectorBatch here. */
                    int32_t typmod = m_var->vartypmod - VARHDRSZ;
                    uint32_t precision = uint32_t((((uint32_t)typmod) >> 16) & 0xffff);
                    if (precision <= 18) {
                        for (uint64_t i = 0; i < numValues; i++) {
                            auto flbaValue = data[i];

                            if (!nullFilter(isSelected, i)) {
                                const uint8_t *buf = flbaValue.ptr;

                                uint8_t buf64[sizeof(int64_t)] = {0};
                                auto bufOffset = sizeof(int64_t) - length;
                                errno_t rc = memcpy_s(buf64 + bufOffset, length, buf, length);
                                securec_check(rc, "\0", "\0");
                                if ((int8_t)(buf[0]) < 0) {
                                    rc = memset_s(buf64, bufOffset, 0xFF, bufOffset);
                                    securec_check(rc, "\0", "\0");
                                }

                                int64_t tmpVal =
                                    arrow::BitUtil::FromBigEndian(reinterpret_cast<const int64_t *>(buf64)[0]);
                                isSelected[i] = HdfsPredicateCheckValue<Int64Wrapper, int64_t>(tmpVal, predicate);
                            }
                        }
                    } else {
                        for (uint64_t i = 0; i < numValues; i++) {
                            auto flbaValue = data[i];

                            if (!nullFilter(isSelected, i)) {
                                const uint8_t *buf = flbaValue.ptr;

                                uint8_t buf128[sizeof(int128)] = {0};
                                auto bufOffset = sizeof(int128) - length;
                                errno_t rc = memcpy_s(buf128 + bufOffset, length, buf, length);
                                securec_check(rc, "\0", "\0");
                                if ((int8_t)(buf[0]) < 0) {
                                    rc = memset_s(buf128, bufOffset, 0xFF, bufOffset);
                                    securec_check(rc, "\0", "\0");
                                }

                                int64_t highBits =
                                    arrow::BitUtil::FromBigEndian(reinterpret_cast<const int64_t *>(buf128)[0]);
                                uint64_t lowBits =
                                    arrow::BitUtil::FromBigEndian(reinterpret_cast<const uint64_t *>(buf128)[1]);

                                /* Convert from parquet int128 format to gcc int128 format */
                                int128 tmpVal = 0;
                                tmpVal = ((uint128)(tmpVal | highBits)) << 64;
                                tmpVal = tmpVal | lowBits;

                                isSelected[i] = HdfsPredicateCheckValue<Int128Wrapper, int128>(tmpVal, predicate);
                            }
                        }
                    }
                    break;
                }
                default:
                    break;
            }

            break;
        }
        default: {
            break;
        }
    }
}

ParquetColumnReader *createParquetColumnReader(const parquet::ColumnDescriptor *desc, uint32_t columnIndex,
                                               uint32_t mppColumnIndex, ReaderState *readerState, const Var *var)
{
    ParquetColumnReader *columnReader = NULL;

    switch (desc->physical_type()) {
        case parquet::Type::BOOLEAN: {
            columnReader = New(readerState->persistCtx) ParquetColumnReaderImpl<parquet::BoolReader>(desc, columnIndex,
                                                                                                     mppColumnIndex,
                                                                                                     readerState, var);
            break;
        }
        case parquet::Type::INT32: {
            columnReader = New(readerState->persistCtx) ParquetColumnReaderImpl<parquet::Int32Reader>(desc, columnIndex,
                                                                                                      mppColumnIndex,
                                                                                                      readerState, var);
            break;
        }
        case parquet::Type::INT64: {
            columnReader = New(readerState->persistCtx) ParquetColumnReaderImpl<parquet::Int64Reader>(desc, columnIndex,
                                                                                                      mppColumnIndex,
                                                                                                      readerState, var);
            break;
        }
        case parquet::Type::INT96: {
            columnReader = New(readerState->persistCtx) ParquetColumnReaderImpl<parquet::Int96Reader>(desc, columnIndex,
                                                                                                      mppColumnIndex,
                                                                                                      readerState, var);
            break;
        }
        case parquet::Type::FLOAT: {
            columnReader = New(readerState->persistCtx) ParquetColumnReaderImpl<parquet::FloatReader>(desc, columnIndex,
                                                                                                      mppColumnIndex,
                                                                                                      readerState, var);
            break;
        }
        case parquet::Type::DOUBLE: {
            columnReader =
                New(readerState->persistCtx) ParquetColumnReaderImpl<parquet::DoubleReader>(desc, columnIndex,
                                                                                            mppColumnIndex, readerState,
                                                                                            var);
            break;
        }
        case parquet::Type::BYTE_ARRAY: {
            columnReader =
                New(readerState->persistCtx) ParquetColumnReaderImpl<parquet::ByteArrayReader>(desc, columnIndex,
                                                                                               mppColumnIndex,
                                                                                               readerState, var);
            break;
        }
        case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
            columnReader = New(
                readerState->persistCtx) ParquetColumnReaderImpl<parquet::FixedLenByteArrayReader>(desc, columnIndex,
                                                                                                   mppColumnIndex,
                                                                                                   readerState, var);
            break;
        }
        default: {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_PARQUET),
                            errmsg("Unsupported parquet type : %u.", desc->physical_type())));
            break;
        }
    }
    return columnReader;
}
}  // namespace reader
}  // namespace dfs
