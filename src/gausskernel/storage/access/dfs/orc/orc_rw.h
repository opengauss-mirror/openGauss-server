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
 * orc_rw.h
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/orc/orc_rw.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef ORC_RW_H
#define ORC_RW_H

#include "pg_config.h"

#include <string>
#include "sstream"

#ifndef ENABLE_LITE_MODE
#include "orc/orc-config.hh"
#include "orc_proto.pb.h"
#include "orc/OrcFile.hh"
#include "orc/Writer.hh"
#endif
#include "../dfs_reader.h"
#include "../dfs_writer.h"
#include "access/dfs/dfs_stream.h"
#include "access/cstore_insert.h"
#include "mb/pg_wchar.h"
#include "utils/dfs_vector.h"
#include "utils/date.h"
#include "utils/builtins.h"
#include "utils/cash.h"
#include "utils/guc.h"
#include "utils/nabstime.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"

extern char *pg_to_server_withfailure(char *src, int64 length, int32 encoding, int32 checkEncodingLevel,
                                      bool &meetError);

#define FOREIGNTABLEFILEID (-1)

namespace dfs {
/* timestamp related values */
#define SECONDS_PER_DAY 86400                   // the number of seconds in a day
#define MICROSECONDS_PER_SECOND 1000000L        // the number of microseconds in a second
#define NANOSECONDS_PER_MICROSECOND 1000        // the number of nanoseconds in a microseconds
#define POSTGRESQL_EPOCH_IN_SECONDS 946677600L  // 2000-01-01 00:00:00 timestamp
#define ORC_EPOCH_IN_SECONDS 1420063200L        // 2015-01-01 00:00:00 timestamp
#define ORC_DIFF_POSTGRESQL 473385600L          // ORC_EPOCH_IN_SECONDS - POSTGRESQL_EPOCH_IN_SECONDS
#define ORC_PSQL_EPOCH_IN_DAYS 10957            // the days base's difference between orc and pg

#define STRIPE_SIZE (64 * 1024 * 1024)
#define IS_DROPPED_COLUMN(attribute) (true == (attribute)->attisdropped)

const int64_t PG_EPOCH_OFFSET_DEFAULT = ORC_EPOCH_IN_SECONDS - POSTGRESQL_EPOCH_IN_SECONDS;

#ifndef ENABLE_LITE_MODE
typedef orc::TypeKind FieldKind;

typedef void (*appendDatum)(orc::ColumnVectorBatch *colBatch, Datum *val, bool *isNull, uint32 length,
                            int64 eppchOffsetDiff, int64 &bufferSize, int64 maxBufferSize);
typedef Datum (*convertToDatum)(orc::ColumnVectorBatch *, uint64, int32, int32, int64, bool &, int32, int32, bool &);

typedef enum {
    READER,
    WRITER
} IOTYPE;

template <bool decimal128, bool fast_numeric>
Datum convertDecimalToDatumT(orc::ColumnVectorBatch *primitiveBatch, uint64 rowId, int32 typmode, int32 scale,
                             int64 epochOffsetDiff, bool &isNull, int32 encoding, int32 checkEncodingLevel,
                             bool &meetError)
{
    Datum result = 0;

    if (decimal128) {
        orc::Int128 inVal = static_cast<orc::Decimal128VectorBatch *>(primitiveBatch)->values[rowId];
        uint128 tmpVal = 0;
        tmpVal = (tmpVal | (uint128)(uint64_t)inVal.getHighBits()) << 64;
        tmpVal = tmpVal | (uint128)inVal.getLowBits();
        if (fast_numeric) {
            result = makeNumeric128((int128)tmpVal, scale);
        } else {
            /*
             * 28=sizeof(NumericData)=sizeof(int32)+sizeof(uint16)+11*sizeof(int16)
             * 11 means 11*DEC_DIGITS(4) > 38, 28->32 align to 8byte border
             */
            char *decimalValue = (char *)palloc(sizeof(char) * 32);

            (void)convert_int128_to_short_numeric_byscale(decimalValue, (int128)tmpVal, typmode, scale);
            result = NumericGetDatum(decimalValue);
        }
    } else {
        int64 value64 = static_cast<orc::Decimal64VectorBatch *>(primitiveBatch)->values[rowId];

        if (fast_numeric) {
            result = makeNumeric64(value64, scale);
        } else {
            /* 18 equals to the size of NumericData
             * equals to (the size of int32 + the size of uint16 + 6 * the size of int16)
             */
            char *decimalValue = (char *)palloc0(sizeof(char) * 18);
            (void)convert_int64_to_short_numeric_byscale(decimalValue, value64, typmode, scale);
            result = NumericGetDatum(decimalValue);
        }
    }

    return result;
}

/* Convert a value in the ColumnVectorBatch into datum. */
template <FieldKind fieldKind, Oid varType, IOTYPE type>
Datum convertToDatumT(orc::ColumnVectorBatch *primitiveBatch, uint64 rowId, int32 typmode, int32 scale,
                      int64 epochOffsetDiff, bool &isNull, int32 encoding, int32 checkEncodingLevel, bool &meetError)
{
    Datum result = 0;
    switch (fieldKind) {
        case orc::BOOLEAN: {
            if (type == READER) {
                int64_t tmpValue = static_cast<orc::LongVectorBatch *>(primitiveBatch)->data[rowId];
                result = BoolGetDatum(tmpValue);
            } else {
                bool tmpValue = static_cast<orc::NumberVectorBatch<bool> *>(primitiveBatch)->data[rowId];
                result = BoolGetDatum(tmpValue);
            }

            break;
        }
        case orc::BYTE: {
            if (type == READER) {
                int64_t tmpValue = static_cast<orc::LongVectorBatch *>(primitiveBatch)->data[rowId];
                result = Int8GetDatum(tmpValue);
            } else {
                int8 tmpValue = static_cast<orc::NumberVectorBatch<int8> *>(primitiveBatch)->data[rowId];
                result = Int8GetDatum((uint8)tmpValue);
            }

            break;
        }
        case orc::SHORT: {
            if (type == READER) {
                int64_t tmpValue = static_cast<orc::LongVectorBatch *>(primitiveBatch)->data[rowId];
                result = Int16GetDatum(tmpValue);
            } else {
                int16 tmpValue = static_cast<orc::NumberVectorBatch<int16> *>(primitiveBatch)->data[rowId];
                result = Int16GetDatum(tmpValue);
            }

            break;
        }
        case orc::INT: {
            if (type == READER) {
                int64_t tmpValue = static_cast<orc::LongVectorBatch *>(primitiveBatch)->data[rowId];
                result = Int32GetDatum(tmpValue);
            } else {
                int32 tmpValue = static_cast<orc::NumberVectorBatch<int32> *>(primitiveBatch)->data[rowId];
                result = Int32GetDatum(tmpValue);
            }

            break;
        }
        case orc::LONG: {
            int64_t tmpValue = static_cast<orc::LongVectorBatch *>(primitiveBatch)->data[rowId];

            /*
             * For hdfs table, oid,timestamptz,time,smalldatetime and cash, they are
             * all stored by int64.
             */
            switch (varType) {
                case OIDOID:
                    result = UInt32GetDatum(tmpValue);
                    break;
                case TIMESTAMPTZOID:
                    result = TimestampTzGetDatum(tmpValue);
                    break;
                case TIMEOID:
                    result = TimeADTGetDatum(tmpValue);
                    break;
                case SMALLDATETIMEOID:
                    result = TimestampGetDatum(tmpValue);
                    break;
                case CASHOID:
                    result = CashGetDatum(tmpValue);
                    break;
                default:
                    result = Int64GetDatum(tmpValue);
            }

            break;
        }
        case orc::FLOAT: {
            if (type == READER) {
                double tmpValue = static_cast<orc::DoubleVectorBatch *>(primitiveBatch)->data[rowId];
                result = Float4GetDatum(static_cast<float>(tmpValue));
            } else {
                float4 tmpValue = static_cast<orc::NumberVectorBatch<float4> *>(primitiveBatch)->data[rowId];
                result = Float4GetDatum(tmpValue);
            }

            break;
        }
        case orc::DOUBLE: {
            double tmpValue = static_cast<orc::DoubleVectorBatch *>(primitiveBatch)->data[rowId];
            result = Float8GetDatum(tmpValue);
            break;
        }
        case orc::CHAR: {
            char *tmpValue = static_cast<orc::StringVectorBatch *>(primitiveBatch)->data[rowId];
            int64_t length = static_cast<orc::StringVectorBatch *>(primitiveBatch)->length[rowId];

            /* Check compatibility and convert '' into null if the db is A_FORMAT. */
            if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && length == 0) {
                isNull = true;
                break;
            }

            /* Set the terminate flag and change it back after using. */
            char lastChar = tmpValue[length];
            tmpValue[length] = '\0';

            /* For foreign table */
            if (encoding != INVALID_ENCODING) {
                char *cvt = pg_to_server_withfailure(tmpValue, length, encoding, checkEncodingLevel, meetError);
                result = DirectFunctionCall3(bpcharin, CStringGetDatum(cvt), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typmode));
                if (cvt != tmpValue) {
                    pfree(cvt);
                }
            } else { /* For non foreign table */
                result = DirectFunctionCall3(bpcharin, CStringGetDatum(tmpValue), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typmode));
            }

            tmpValue[length] = lastChar;
            break;
        }
        case orc::VARCHAR: {
            char *tmpValue = static_cast<orc::StringVectorBatch *>(primitiveBatch)->data[rowId];
            int64_t length = static_cast<orc::StringVectorBatch *>(primitiveBatch)->length[rowId];

            /* Check compatibility and convert '' into null if the db is A_FORMAT. */
            if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && length == 0) {
                isNull = true;
                break;
            }

            /* Set the terminate flag and change it back after using. */
            char lastChar = tmpValue[length];
            tmpValue[length] = '\0';

            /* For foreign table */
            if (encoding != INVALID_ENCODING) {
                char *cvt = pg_to_server_withfailure(tmpValue, length, encoding, checkEncodingLevel, meetError);
                result = DirectFunctionCall3(varcharin, CStringGetDatum(cvt), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typmode));
                if (cvt != tmpValue) {
                    pfree(cvt);
                }
            } else { /* For non foreign table */
                result = DirectFunctionCall3(varcharin, CStringGetDatum(tmpValue), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typmode));
            }

            tmpValue[length] = lastChar;
            break;
        }
        case orc::STRING: {
            char *tmpValue = static_cast<orc::StringVectorBatch *>(primitiveBatch)->data[rowId];
            int64_t length = static_cast<orc::StringVectorBatch *>(primitiveBatch)->length[rowId];

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
                case INTERVALOID: {
                    result = DirectFunctionCall3(interval_in, CStringGetDatum(cvt), ObjectIdGetDatum(InvalidOid),
                                                 Int32GetDatum(typmode));
                    break;
                }
                case TINTERVALOID: {
                    result = DirectFunctionCall1(tintervalin, CStringGetDatum(cvt));
                    break;
                }
                case TIMETZOID: {
                    result = DirectFunctionCall3(timetz_in, CStringGetDatum(cvt), ObjectIdGetDatum(InvalidOid),
                                                 Int32GetDatum(typmode));
                    break;
                }
                case CHAROID: {
                    result = DirectFunctionCall1(charin, CStringGetDatum(cvt));
                    break;
                }
                case NVARCHAR2OID: {
                    result = DirectFunctionCall3(nvarchar2in, CStringGetDatum(cvt), ObjectIdGetDatum(InvalidOid),
                                                 Int32GetDatum(typmode));
                    break;
                }
                case NUMERICOID: {
                    result = DirectFunctionCall3(numeric_in, CStringGetDatum(cvt), ObjectIdGetDatum(InvalidOid),
                                                 Int32GetDatum(typmode));
                    break;
                }
                default: {
                    result = CStringGetTextDatum(cvt);
                }
            }

            if (cvt != tmpValue) {
                pfree(cvt);
            }

            tmpValue[length] = lastChar;
            break;
        }
        case orc::DATE: {
            int64_t tmpValue = static_cast<orc::LongVectorBatch *>(primitiveBatch)->data[rowId];
            result = TimestampGetDatum(date2timestamp(static_cast<DateADT>(tmpValue - ORC_PSQL_EPOCH_IN_DAYS)));
            break;
        }
        case orc::TIMESTAMP: {
            int64_t seconds = static_cast<orc::TimestampVectorBatch *>(primitiveBatch)->data[rowId] - epochOffsetDiff;
            int64_t nanoSeconds = static_cast<orc::TimestampVectorBatch *>(primitiveBatch)->nanoseconds[rowId];
            Timestamp tmpValue =
                (Timestamp)(seconds * MICROSECONDS_PER_SECOND + nanoSeconds / NANOSECONDS_PER_MICROSECOND);
            result = TimestampGetDatum(tmpValue);
            break;
        }
        default: {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_ORC),
                            errmsg("Unsupported data type : %u.", fieldKind)));
        }
    }

    return result;
}

namespace reader {
/* The restriction types of ORC file:file, stripe, stride. */
enum RestrictionType {
    FILE = 0,
    STRIPE = 1,
    STRIDE = 2
};

/*
 * The abstract class which provides the interfaces to read a special
 * column in the ORC file and related functions.
 */
class OrcColumnReader : public BaseObject {
public:
    virtual ~OrcColumnReader()
    {
    }

    /* Initialization works */
    virtual void begin(DFS_UNIQUE_PTR<orc::InputStream> stream, const orc::Reader *structReader,
                       const uint64_t maxBatchSize) = 0;

    virtual void Destroy() = 0;

    /*
     * Load and set the row index and bloom filter of the current columns
     * if they exist and are needed.
     * @_in_param stripeIndex: The index of the current stripe to be read.
     * @_in_param hasCheck: Indicate whether the current column has
     *      restrictions. The row index stream is always needed because we
     *       need it to seek a special stride. The bloom filter stream
     *      is needed only when there are restrictions. Of course, there
     *      is no bloom filter stream in the orc file unless you set the
     *      orc.bloom.filter.columns when importing the data.
     * @_in_param stripeFooter: The stripe footer for all the columns which
     *      will not be null here.
     */
    virtual void setRowIndex(uint64_t stripeIndex, bool hasCheck, const orc::proto::StripeFooter *stripeFooter) = 0;

    /*
     * Read a special number of rows in one column of ORC file.
     * @_in_param numValues: The number of rows to read.
     */
    virtual void nextInternal(uint64_t numValues) = 0;

    /*
     * Skip a special number of rows in one column of ORC file.
     * @_in_param numValues: The number of rows to read.
     */
    virtual void skip(uint64_t numValues) = 0;

    /*
     * Set the bloom filter in the column reader.
     * @_in_param bloomFilter: The bloom filter to be set.
     */
    virtual void setBloomFilter(filter::BloomFilter *bloomFilter) = 0;

    /*
     * Check if the current column of ORC file has predicates.
     * @return true: The predicate of the column exists;
     *      false: The predicate of the column does not exists;
     */
    virtual bool hasPredicate() const = 0;

    /*
     * Filter the obtained data with the predicates on the column
     * and set isSelected flags.
     * @_in_param numValues: The number of rows to be filtered.
     * @_in_out_param isSelected: The flag array to indicate which
     *      row is selected or not.
     */
    virtual void predicateFilter(uint64_t numValues, bool *isSelected) = 0;

    /*
     * Convert the primitive data to datum with which the scalar
     * vector will be filled.
     * @_in_param numValues: The number of rows to fill the scalar vector.
     * @_in_out_param isSelected: The flag array to indicate which row is
     *      selected or not.
     * @_out_param vec: The scalar vector to be filled.
     * @return the number of selected rows of the filled scalar vector.
     */
    virtual int fillScalarVector(uint64_t numValues, bool *isSelected, ScalarVector *vec) = 0;

    /*
     * Check if the equal op restrict(we generate a bloom filter) matches
     * the bloomfilter of the orc file.
     * @_in_param strideIdx: The index of the stride as the bloom filter
     *      is created for each stride in ORC file.
     * @return true only if the two bloom filters both exist and match while
     *      one stores in file and the other is generated by the restrict.
     *      Otherwise return false.
     */
    virtual bool checkBloomFilter(uint64_t strideIdx) const = 0;

    /*
     * Build the column restriction node of file/stripe/stride with the min/max
     * information stored in ORC file. Dynamic parition prunning is also handled
     * here.
     * @_in_param type: Indicates the restriction's level:file, stripe
     *      or stride.
     * @_in_param structReader: the orc reader for structure
     * @_in_param stripeIdx: When the type is STRIPE, this is used
     *      to identify the special stripe.
     * @_in_param strideIdx: When the type is STRIDE, this is used
     *      to identifythe special stride.
     * @return the built restriction of file, stripe or stride.
     */
    virtual Node *buildColRestriction(RestrictionType type, orc::Reader *structReader, uint64_t stripeIdx,
                                      uint64_t strideIdx) const = 0;
};

/* The realistic implementation class to read ORC file. */
class OrcReaderImpl : public DFSReader {
public:
    OrcReaderImpl(ReaderState *_readerState, dfs::DFSConnector *conn);
    virtual ~OrcReaderImpl();
    void begin();
    void end() DFS_OVERRIDE;

    /* Return conn info to judge OBS/HDFS/others connection. */
    const dfs::DFSConnector *getConn() const;

    /*
     * Load the file transfered in.
     * @_in_param filePath: The path of the file to be load.
     * @return true if the current file is load successfully and can not
     *      be skipped.Otherwise the current file is skipped and we need load
     *      another one.
     */
    bool loadFile(char *filePath, List *fileRestriction) DFS_OVERRIDE;

    /*
     * Fetch the next batch including 1000 or less(reach the end of file)
     * rows which meets the predicates.
     * @_in_param batch: The batch to be filled with the proper tuples.
     * @return rows: The number of rows which has been read in the file,
     *      it is greater than or equal to the size of batch returned.
     */
    uint64_t nextBatch(VectorBatch *batch, uint64_t curRowsInFile, DFSDesc *dfsDesc,
                       FileOffset *fileOffset) DFS_OVERRIDE;

    /*
     * @Description: Fetch the next batch by a list of continue tids
     * @IN rowsSkip: the rows to be skipped before reading
     * @IN rowsToRead: the number of continue tids
     * @OUT batch: vector batch to be filled
     * @IN startOffset: the start offset of the first tid(start from 0)
     * @IN desc: the furrent file record in the desc table
     * @OUT fileoffset: the offset of the selected tids
     * @See also:
     */
    void nextBatchByContinueTids(uint64_t rowsSkip, uint64_t rowsToRead, VectorBatch *batch, uint64_t startOffset,
                                 DFSDesc *dfsDesc, FileOffset *fileOffset) DFS_OVERRIDE;

    /*
     * Add a bloom filter into the reader.
     * @_in_param bloomFilter: The bloom filter to be added.
     * @_in_param colIdx: The index of the column on which to add the bloom filter.
     * @_in_param is_runtime: Is between runtime or not.
     */
    void addBloomFilter(filter::BloomFilter *bloomFilter, int colIdx, bool is_runtime) DFS_OVERRIDE;

    /*
     * Copy bloomFilter to runTimeBloomFilter
     */
    void copyBloomFilter() DFS_OVERRIDE;

    /* Check bloom filter for partition column, return true if this partition can be skipped */
    bool checkBFPruning(uint32_t colID, char *colValue) DFS_OVERRIDE;

    /*
     * Get the number of all the rows in the current file.
     * @return rows: The number of rows of file.
     */
    uint64_t getNumberOfRows() DFS_OVERRIDE
    {
        return numberOfRows;
    }

    /*
     * @Description: Set the buffer size of the reader
     * @IN capacity: the buffer size(number of rows)
     * @See also:
     */
    void setBatchCapacity(uint64_t capacity) DFS_OVERRIDE
    {
        maxRowsReadOnce = capacity;
    }

    /*
     * @Description: file is EOF
     * @IN rowsReadInFile: rows read in file
     * @Return: true if tid file is EOF, or return false
     * @See also:
     */
    bool fileEOF(uint64_t rowsReadInFile)
    {
        return rowsReadInFile >= getNumberOfRows();
    }

    void Destroy() DFS_OVERRIDE;

private:
    /*
     * Reset each column readers to initial status, we call this fucntion after
     * processing one ORC file
     */
    void resetOrcColumnReaders();

    /* For partition table, try the dynamic prunning according to the bloom filter if exist. */
    bool tryDynamicPrunning();

    /* Create the struct reader with meta cache which read only meta data. */
    void createStructReaderWithCache();
    void createStructReaderWithCacheForInnerTable(DFS_UNIQUE_PTR<orc::InputStream> &stream);
    void createStructReaderWithCacheForForeignTable(DFS_UNIQUE_PTR<orc::InputStream> &stream);

    /* Build the struct reader which read only meta data. */
    void buildStructReader();

    /* For foreign table, remove the non-exist columns in file. */
    void setNonExistColumns();

    /* Build orc readers for each required columns. */
    void initializeColumnReaders();

    /* *
     * @Description: Initialize the column map on ORC file level. Now the DFS table supports
     * the "drop column" feature, each file will has different column counts. When load each file,
     * must initialize the column map. In this function, we have to do the following things:
     * 1. Initialize readerState->readRequired, it represents which column will be needed to read
     * from file.
     * 2. Initialize readerState->readColIDs, it represents the mapping array from mpp column
     * index to file column index.
     * @return None.
     */
    void initColIndexByDesc();
    /* Compute the rows to read this time. */
    uint64_t computeRowsToRead(uint64_t maxRows);

    /* Read rows from the orc file and filter them by the predicates. */
    bool readAndFilter(uint64_t rowsSkip, uint64_t rowsToRead);

    /* Fill the vector batch with the selected column data. */
    void fillVectorBatch(uint64_t rowsToRead, VectorBatch *batch, FileOffset *fileOffset, uint64_t rowsInFile);

    /* Set the bloom filter for the proper column reader. */
    void setColBloomFilter();

    /*
     * @Description: Load the row index's information from file for the current stripe.
     * @IN loadBF: flag indicates whether we should load the bloom filter information
     */
    void setStripeRowIndex(bool loadBF);

    /*
     * @Description: get stripe footer from cache
     */
    const orc::proto::StripeFooter *getStripeFooterFromCacheForInnerTable();
    const orc::proto::StripeFooter *getStripeFooterFromCacheForForeignTable();

    /*
     * @Description: For index tid scan, we skip the process of stripe/stride skip, just
     * 		reset the status after skipping some rows.
     * @IN rowsSkip: the rows to skip firstly
     */
    void resetStripeAndStrideStat(uint64_t rowsSkip);

    /* Try if we can skip the next stripe when arrive at the stripe boundary. */
    bool tryStripeSkip(uint64_t &rowsSkip, uint64_t &rowsCross);

    /*
     * Skip the current stripe.
     * @_out_param rowsSkip: The actual rows skipped when skipping the
     * current stripe.
     * @_out_param rowsCross: The actual rows which have been dealed
     * when skipping the current stripe.
     */
    void skipCurrentStripe(uint64_t &rowsSkip, uint64_t &rowsCross);

    /* Try if we can skip the next stride when arrive at the stride boundary. */
    bool tryStrideSkip(uint64_t &rowsSkip, uint64_t &rowsCross);

    /*
     * Skip the current stride.
     * @_out_param rowsSkip: The actual rows skipped when skipping the
     * current stride.
     * @_out_param rowsCross: The actual rows which have been dealed
     * when skipping the current stride.
     */
    void skipCurrentStride(uint64_t &rowsSkip, uint64_t &rowsCross);

    /*
     * Check if the current stripe meets the end and reset some values if so. This
     * is called in the end of each loop when we have read or skipped some rows.
     */
    void tryStripeEnd();

    /*
     * Refresh the reader state after reading a vector batch from orc file.
     * @_in_param rowsToRead: The number of rows which have been read in one loop.
     * @_in_out_param rowsSkip: The number of rows which have been skipped in one loop.
     * @_in_out_param rowsCross: The number of rows which have been read or skipped in the iteration.
     */
    void refreshState(uint64_t rowsToRead, uint64_t &rowsSkip, uint64_t &rowsCross);

    /*
     * Check if the current file can be skipped wholely by the predicates.
     * @_in_param descFileRestriction: The list of restrictions which are built by desc.
     * @return true: The current file can be skipped.
     * false: The current file can not be skipped.
     */
    bool checkPredicateOnCurrentFile(List *descFileRestriction);

    /*
     * Check if the current stripe can be skipped wholely by the predicates.
     * @return true: The current stripe can be skipped.
     * false: The current stripe can not be skipped.
     */
    bool checkPredicateOnCurrentStripe();

    /*
     * Check if the current stride can be skipped wholely by the predicates.
     * @return true: The current stride can be skipped.
     * false: The current stride can not be skipped.
     */
    bool checkPredicateOnCurrentStride();

    /*
     * Call the checkBloomFilter of each column reader to find if we can skip
     * the current stride.
     */
    bool checkBloomFilter() const;

    /*
     * Build the restriction list of file, stripe or stride according to the type.
     * @_in_param type: Indicates the restriction's level:file, stripe
     * or stride.
     * @_in_param stripeIdx: When the type is STRIPE, this is used
     * to identify the special stripe.
     * @_in_param strideIdx: When the type is STRIDE, this is used
     * to identifythe special stride.
     * @return the restriction list of file, stripe or stride.
     */
    List *buildRestriction(RestrictionType type, uint64_t stripeIdx = 0, uint64_t strideIdx = 0);

    /*
     * Check if the current batch can be skipped wholely that happens when
     * all the flag in isSelected array is false.
     * @_in_param rowsToRead: The number of rows in the batch to check.
     * @return true: The batch can be skipped;
     * false: The batch can not be skipped.
     */
    bool canSkipBatch(uint64_t rowsToRead) const;

    /*
     * Check is the current stripe meets the end when starting a new loop to read
     * next batch.
     * @return true: The stripe can be skipped;
     * false: The stripe can not be skipped.
     */
    bool stripeEOF() const;

    /*
     * Check is the current stride meets the end when starting a new loop to read
     * next batch.
     * @return true: The stride can be skipped;
     * false: The stride can not be skipped.
     */
    bool strideEOF() const;

    /*
     * Check if there are restrictions for Foreign scan.
     * @return true if the queryRestrictionList is not NULL or return false.
     */
    bool hasRestriction() const;

    /*
     * Retune the order of the columns to be read when there is dynamic predicate
     * from vechashjoin. Put the column on which the join depends just behind the
     * last column with static predicates and top it if there is no normal restrictions.
     * @_in_param colIdx: The index of the column on which there is a dynamic predicate.
     */
    void fixOrderedColumnList(uint32_t colIdx);

    /*
     * Before reading the data, use the delete map stored in desc table to
     * initialize the isSelected array.
     * @_in_param dfsDesc: The tuple content of the desc table.
     * @_in_param rowsInFile: The row offset of the current file.
     * @_in_param rowsToRead: The number of rows to read.
     */
    void combineDeleteMap(DFSDesc *dfsDesc, uint64_t rowsInFile, uint64_t rowsToRead);

    /*
     * Fill the fileOffset array from the rowsInFile according to the rowsToRead
     * and isSelected array.
     * @_in_param rowsInFile: The start offset in the file.
     * @_in_param rowsToRead: The number of rows to fill the offset.
     * @_out_param fileOffset: The array to be filled.
     */
    void fillFileOffset(uint64_t rowsInFile, uint64_t rowsToRead, FileOffset *fileOffset) const;

    /*
     * Collec the statistics while reading file.
     */
    void collectFileReadingStatus();

private:
    char *currentFilePath;
    uint64_t numberOfColumns;
    uint64_t numberOfRows;
    uint64_t numberOfOrcCols;
    uint64_t numberOfFields;
    uint64_t numberOfStripes;
    uint64_t numberOfStrides;
    uint64_t rowsReadInCurrentStripe;
    uint64_t currentStripeIdx;
    uint64_t currentStrideIdx;
    uint64_t rowsInStride;
    uint64_t rowsInCurrentStripe;
    ReaderState *readerState;
    uint64_t maxRowsReadOnce;
    bool *isSelected;
    MemoryContext internalContext;
    bool hasBloomFilter;
    dfs::DFSConnector *conn;
    filter::BloomFilter **bloomFilters;
    filter::BloomFilter **staticBloomFilters;
    Vector<OrcColumnReader *> columnReader;
    DFS_UNIQUE_PTR<orc::Reader> structReader;
    dfs::GSInputStream *newStream;
};
}  // namespace reader

namespace writer {
class ORCColWriter : public ColumnWriter {
public:
    ORCColWriter(MemoryContext ctx, Relation relation, Relation destRel, DFSConnector *conn, const char *parsig);
    ~ORCColWriter();

    /* Initialization works. */
    void init(IndexInsertInfo *indexInsertInfo) DFS_OVERRIDE;

    /* Clean the objects. */
    void Destroy() DFS_OVERRIDE;

    /* Create a new file stream and orc writer according to the file path. */
    void openNewFile(const char *filePath) DFS_OVERRIDE;

    /*
     * @Description: Spill the buffer data into serialize stream.
     * @IN fileID: the current file id
     * @Return: Return 0 means there is no left rows in buffer;
     *      Return non-0 means the current file is full and we need to create a new
     *      file in the next time.
     * @See also:
     */
    int spill(uint64 fileID) DFS_OVERRIDE;

    /* Add numbers of tuple into the buffer. */
    void appendColDatum(int colID, Datum *vals, bool *isNull, uint32 size) DFS_OVERRIDE;

    /* Intert the remaining rows into delta table. */
    void deltaInsert(int option) DFS_OVERRIDE;

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
    bool getMinMax(uint32 colId, char *&minStr, char *&maxStr, bool &hasMin, bool &hasMax) DFS_OVERRIDE;

    /* Close the current serialize stream. */
    void closeCurWriter() DFS_OVERRIDE;

    /* Check if the file is valid (size is bigger than 0) and return the file size. */
    int64_t verifyFile(char *filePath) DFS_OVERRIDE;

    /* Get the estimated number of rows in the buffer */
    uint64 getBufferCapacity() DFS_OVERRIDE
    {
        return m_bufferCapacity;
    }

    /* Get the remaining space in the buffer. */
    uint64 getBufferRows() DFS_OVERRIDE
    {
        return m_columnVectorBatch->numElements;
    }

    /* Get the rows in the hdfs file, this function must be called after the m_orcWriter is closed. */
    uint64 getTotalRows() DFS_OVERRIDE
    {
        return m_orcWriter.get() != NULL ? m_orcWriter->getTotalSize() : 0;
    }

    /* Get the rows in the serialize stream. */
    uint64 getSerializedRows() DFS_OVERRIDE
    {
        return m_orcWriter.get() != NULL ? m_orcWriter->getSerializeSize() : 0;
    }

    /* Increment the number of rows in the buffer with 1. */
    void incrementNumber(int size) DFS_OVERRIDE
    {
        m_columnVectorBatch->numElements += size;
    }

    /* Deal the clean works on dfs writer. */
    void handleTail() DFS_OVERRIDE;

private:
    /*
     * Assigning real ORC file index into m_attribute, which is special to value
     * partitioned HDFS table as the actual columns we are going to store are
     * not including the partition columns
     *
     * Return the actual # of column writers we are going to process
     */
    int AssignRealColAttrIndex();

    /*
     * @Description: Initialize the members for index
     * @See also:
     */
    void initIndexInsertInfo(IndexInsertInfo *indexInsertInfo);

    /* Check if we need to set the lable and fetch the lable name if neccessary. */
    void initLabelInfo();

    /* Accquire the favorite nodes' list. */
    void initFavoriteNodeInfo();

    /* Set the compresstion kind of m_opts for orc writer according to the reloption. */
    void setCompressKind();

    /* Set the block size of m_opts for orc writer according to the config file. */
    void setBlockSize();

    /* *
     * @Description: Whether the column is dropped.
     * @in attNo, The column number, the number start 0.
     * @return If the column is dropped, return true, otherwise return false.
     */
    bool isDroppedCol(int attNo) const
    {
        Assert(m_desc->natts > attNo);

        if (m_desc->attrs[attNo]->attisdropped) {
            return true;
        } else {
            return false;
        }
    }

    /*
     * @Description: Extract a column's datums from the orc column batch.
     * @IN col: the column id(count from 0)
     * @OUT values: the output datums
     * @OUT nulls the output nulls
     * @See also: extractDatumFromOrcBatchByRow
     */
    void extractDatumFromOrcBatchByColumn(int col, Datum *values, bool *nulls);

    /*
     * @Description: Extract a row's datums from the orc column batch.
     * @IN rowId: the row id(count from 0)
     * @OUT values: the output datums
     * @OUT nulls the output nulls
     * @See also: extractDatumFromOrcBatchByColumn
     */
    void extractDatumFromOrcBatchByRow(int rowId, Datum *values, bool *nulls);

    /* *
     * @Description: Set Null value into dropped columns.
     * @in values, The output datums.
     * @in nulls, The output nulls.
     * @return None.
     */
    void setNullIntoDroppedCol(Datum *values, bool *nulls);

    /*
     * @Description: Insert the datums into the m_idxBatchRow
     * @IN startOffset: the start offset in the orc column batch
     * @IN fileID: the current file ID
     * @See also: extractDatumFromOrcBatchByColumn
     */
    void insert2Batchrow(uint64 startOffset, uint64 fileID);

    /*
     * @Description: Flush the m_idxBatchRow into index relation.
     * @IN rowCount: the number of rows to be flush
     * @See also:
     */
    void flush2Index(int32 rowCount);

private:
    uint64_t m_bufferCapacity;
    int64_t m_epochOffsetDiff;
    int64_t m_fixedBufferSize;
    int64_t m_totalBufferSize;
    int64_t m_maxBufferSize;
    int32_t *m_scales;
    MemoryContext m_ctx;
    Relation m_rel;
    Relation m_destRel;
    TupleDesc m_desc;
    DFSConnector *m_conn;
    appendDatum *m_appendDatumFunc;
    convertToDatum *m_convertToDatumFunc;
    DFS_UNIQUE_PTR<orc::Writer> m_orcWriter;
    DFS_UNIQUE_PTR<orc::ColumnVectorBatch> m_columnVectorBatch;
    std::vector<std::string> *m_favorNodes;
    StringInfo m_dfsLabel;
    orc::WriterOptions m_opts;
    bool m_sort;

    /* partitioned table support */
    const char *m_parsig;

    /*
     * fieldID for ORC file col, when loading data into value partition table, we
     * only have to write un-partitioned columns into ORC file, so orcFieldId[N]
     * is the array to indicating which col of ORC file for this attno, the reason
     * that we put it here is to avoid calculating the relative col each time when
     * processing one loaded-tuple.
     *
     * Example:
     * A table has columns [1,2,3,4,5,6,7,8] and partitioned as [3,5] then the value
     * mapped in orcFieldId array is [0,1,-1,2,-1,3,4,5,6]
     */
    int *m_colAttrIndex;

    /* psort index insert variables */
    IndexInsertInfo *m_indexInsertInfo;
    bulkload_rows *m_idxBatchRow;
    bulkload_rows *m_idxInsertBatchRow;
    Datum *m_tmpValues;
    bool *m_tmpNulls;
    bool *m_idxChooseKey;
    bool m_indexInfoReady;
};
}  // namespace writer
#endif

extern uint64 EstimateBufferRows(TupleDesc desc);
}  // namespace dfs

#endif
