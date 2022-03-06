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
 * parquet_file_reader.h
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/parquet/parquet_file_reader.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PQRQUET_FILE_READER_H
#define PQRQUET_FILE_READER_H

#ifndef ENABLE_LITE_MODE
#include "parquet/api/reader.h"
#include "parquet/types.h"
#include "parquet/deprecated_io.h"
#endif
#include "utils/dfs_vector.h"
#include "utils/bloom_filter.h"
#include "vecexecutor/vectorbatch.h"
#include "access/dfs/dfs_stream.h"
#include "access/dfs/dfs_am.h"

#include "parquet_column_reader.h"

namespace dfs {
namespace reader {
class ParquetFileReader : public BaseObject {
public:
    static ParquetFileReader *create(ReaderState *readerState, std::unique_ptr<GSInputStream> gsInputStream);

    ~ParquetFileReader();

    void begin();
    void Destroy();

    uint64_t getNumRows() const;
    uint64_t getNumColumns() const;
    uint64_t getNumRowGroups() const;
    uint64_t getNumFields() const;

    bool isCurrentRowGroupValid() const;

    int64_t readRows(VectorBatch *batch, uint64_t maxRowsReadOnce, uint64_t curRowsInFile, uint64_t &rowsInFile,
                     uint64_t &rowsSkip, uint64_t &rowsCross, FileOffset *fileOffset);
    void initializeColumnReaders(GSInputStream *gsInputStream);
    void setColBloomFilter(filter::BloomFilter **bloomFilters);

private:
    ParquetFileReader(ReaderState *readerState, std::unique_ptr<parquet::ParquetFileReader> realParquetFileReader);

    void resetColumnReaders();
    void setRowGroupIndexForColumnReaders();

    void createRowGroupReaderWhenNecessary();
    bool tryToSkipCurrentRowGroup(uint64_t &rowsSkip, uint64_t &rowsCross);
    const bool isStartOfCurrentRowGroup() const;
    uint64_t calculateRowsToRead(const uint64_t maxRowsReadOnce) const;
    bool checkPredicateOnCurrentRowGroup();
    bool checkBloomFilter() const;
    List *buildRestriction(RestrictionType type, uint64_t rowGroupIndex);
    bool readAndFilter(uint64_t rowsSkip, uint64_t numRowsToRead);
    void fillVectorBatch(VectorBatch *batch, uint64_t numRowsToRead, FileOffset *fileOffset, uint64_t rowsInFile);
    void fillFileOffset(uint64_t rowsInFile, uint64_t rowsToRead, FileOffset *fileOffset) const;
    void skipCurrentRowGroup(uint64_t &rowsSkip, uint64_t &rowsCross);
    void trySwitchToNextRowGroup(uint64_t numRowsRead);
    const bool hasRestriction() const;

    inline uint32_t numberOfColumns() const
    {
        return m_readerState->relAttrNum;
    }

    inline bool isColumnRequiredToRead(uint32_t columnIndexInRead) const
    {
        return m_readerState->readRequired[columnIndexInRead];
    }

    inline uint32_t columnIndexInFile(uint32_t columnIndexInRead) const
    {
        return m_readerState->readColIDs[columnIndexInRead];
    }

    inline void switchToNextRowGroup()
    {
        m_currentRowGroupIndex++;
        m_rowsReadInCurrentRowGroup = 0;
        m_groupMetaData = NULL;
    }

    inline bool canSkipBatch(uint64_t rowsToRead) const
    {
        for (uint64_t i = 0; i < rowsToRead; i++) {
            if (isSelected[i]) {
                return false;
            }
        }
        return true;
    }

    ReaderState *m_readerState;
    std::unique_ptr<parquet::ParquetFileReader> m_realParquetFileReader;
    std::shared_ptr<parquet::FileMetaData> m_fileMetaData;
    std::unique_ptr<parquet::RowGroupMetaData> m_groupMetaData;
    Vector<ParquetColumnReader *> m_columnReaders;
    bool *isSelected;
    uint64_t m_num_rows;
    uint64_t m_num_columns;
    uint64_t m_num_row_groups;
    uint64_t m_numberOfColumns;
    uint64_t m_numberOfFields;
    uint64_t m_currentRowGroupIndex;
    uint64_t m_totalRowsInCurrentRowGroup;
    uint64_t m_rowsReadInCurrentRowGroup;

    bool hasBloomFilter;
    filter::BloomFilter **bloomFilters;
};
}  // namespace reader
}  // namespace dfs
#endif
