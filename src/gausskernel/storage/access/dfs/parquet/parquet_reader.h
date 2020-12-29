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
 * parquet_reader.h
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/parquet/parquet_reader.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PQRQUET_READER_H
#define PQRQUET_READER_H

#include <string>
#include "sstream"

#include "../dfs_reader.h"
#include "access/dfs/dfs_stream.h"

namespace dfs {
namespace reader {
class ParquetReaderImpl;

class ParquetReader : public DFSReader {
public:
    ParquetReader(ReaderState *reader_state, dfs::DFSConnector *conn);
    virtual ~ParquetReader() override;

    /* Initialization works */
    void begin() override;

    /* Handle the tailing works which are not destroyers. */
    void end() override;

    /* Clear the memory and resource. */
    void Destroy() override;

    /*
     * Load the file transfered in.
     * @_in_param filePath: The path of the file to be load.
     * @_in_param fileRestriction: The restrictions built by the desc table.
     * @return true: The current file is load successfully and can not be skipped.
     *             false: The current file is skipped so we need load another one.
     */
    bool loadFile(char *file_path, List *file_restriction) override;

    /*
     * Fetch the next batch including 1000 or less(reach the end of file)
     * rows which meets the predicates.
     * @_in_param batch: The batch to be filled with the proper tuples.
     * @return rows: The number of rows which has been read in the file,
     *                     it isgreater than or equal to the size of batch returned.
     */
    uint64_t nextBatch(VectorBatch *batch, uint64_t cur_rows_in_file, DFSDesc *dfs_desc,
                       FileOffset *file_offset) override;

    /*
     * @Description: Fetch the next batch by tid
     * @IN batch: vector batch
     * @IN rowsSkip: the rows to be skipped before reading
     * @Return: true if tid row is satisfied, or return false
     * @See also:
     */
    void nextBatchByContinueTids(uint64_t rowsSkip, uint64_t rowsToRead, VectorBatch *batch, uint64_t startOffset,
                                 DFSDesc *dfsDesc, FileOffset *fileOffset) override;

    /*
     * Add a bloom filter into the reader.
     * @_in_param bloomFilter: The bloom filter to be added.
     * @_in_param colIdx: The index of the column on which to add the bloom filter.
     */
    void addBloomFilter(filter::BloomFilter *bloomFilter, int colidx, bool is_runtime) override;

    /*
     * Copy bloomFilter to runTimeBloomFilter
     */
    void copyBloomFilter() override;

    /*
     * Retune the order of the columns to be read when there is dynamic predicate
     * from vechashjoin. Put the column on which the join depends just behind the
     * last column with static predicates and top it if there is no normal restrictions.
     * @_in_param colIdx: The index of the column on which there is a dynamic predicate.
     */
    void fixOrderedColumnList(uint32_t colIdx);

    /* Check bloom filter for partition column, return true if this partition can be skipped */
    bool checkBFPruning(uint32_t colID, char *colValue) override;

    /*
     * Get the number of all the rows in the current file.
     * @return rows: The number of rows of file.
     */
    uint64_t getNumberOfRows() override;

    /*
     * @Description: Set the buffer size of the reader
     * @IN capacity: the buffer size(number of rows)
     * @See also:
     */
    void setBatchCapacity(const uint64_t capacity) override;

    /*
     * @Description: file is EOF
     * @IN rowsReadInFile: rows read in file
     * @Return: true if tid file is EOF, or return false
     * @See also:
     */
    bool fileEOF(uint64_t rows_read_in_file) override;

private:
    void collectFileReadingStatus();

    ParquetReaderImpl *m_pImpl;

    ReaderState *m_readerState;

    dfs::DFSConnector *m_conn;
};
}  // namespace reader
}  // namespace dfs
#endif
