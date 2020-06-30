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
 * dfs_reader.h
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/dfs_reader.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DFS_READER_H
#define DFS_READER_H

#include "access/dfs/dfs_am.h"

namespace dfs {
namespace reader {
/* The interface for all kinds of reader with diffrent file types. */
class DFSReader : public BaseObject {
public:
    virtual ~DFSReader()
    {
    }

    /* Initialization works */
    virtual void begin() = 0;

    /* Handle the tailing works which are not destroyers. */
    virtual void end() = 0;

    /* Clear the memory and resource. */
    virtual void Destroy() = 0;

    /*
     * Load the file transfered in.
     * @_in_param filePath: The path of the file to be load.
     * @_in_param fileRestriction: The restrictions built by the desc table.
     * @return true: The current file is load successfully and can not be skipped.
     *             false: The current file is skipped so we need load another one.
     */
    virtual bool loadFile(char *filePath, List *fileRestriction) = 0;

    /*
     * Fetch the next batch including 1000 or less(reach the end of file)
     * rows which meets the predicates.
     * @_in_param batch: The batch to be filled with the proper tuples.
     * @return rows: The number of rows which has been read in the file,
     *                     it isgreater than or equal to the size of batch returned.
     */
    virtual uint64_t nextBatch(VectorBatch *batch, uint64_t curRowsInFile, DFSDesc *dfsDesc,
                               FileOffset *fileOffset) = 0;

    /*
     * @Description: Fetch the next batch by a list of continue tids, index scan
     * @IN rowsSkip: the rows to be skipped before reading
     * @IN rowsToRead: the number of continue tids
     * @OUT batch: vector batch to be filled
     * @IN startOffset: the start offset of the first tid(start from 0)
     * @IN desc: the furrent file record in the desc table
     * @OUT fileoffset: the offset of the selected tids
     * @See also:
     */
    virtual void nextBatchByContinueTids(uint64_t rowsSkip, uint64_t rowsToRead, VectorBatch *batch,
                                         uint64_t startOffset, DFSDesc *dfsDesc, FileOffset *fileOffset) = 0;

    /*
     * Add a bloom filter into the reader.
     * @_in_param bloomFilter: The bloom filter to be added.
     * @_in_param colIdx: The index of the column on which to add the bloom filter.
     */
    virtual void addBloomFilter(filter::BloomFilter *bloomFilter, int colIdx, bool is_runtime) = 0;

    /*
     * Copy bloomFilter to runTimeBloomFilter
     */
    virtual void copyBloomFilter() = 0;

    /* Check bloom filter for partition column, return true if this partition can be skipped */
    virtual bool checkBFPruning(uint32_t colID, char *colValue) = 0;

    /*
     * Get the number of all the rows in the current file.
     * @return rows: The number of rows of file.
     */
    virtual uint64_t getNumberOfRows() = 0;

    /*
     * @Description: Set the buffer size of the reader
     * @IN capacity: the buffer size(number of rows)
     * @See also:
     */
    virtual void setBatchCapacity(uint64_t capacity) = 0;

    /*
     * @Description: file is EOF
     * @IN rowsReadInFile: rows read in file
     * @Return: true if tid file is EOF, or return false
     * @See also:
     */
    virtual bool fileEOF(uint64_t rowsReadInFile) = 0;
};
}  // namespace reader
}  // namespace dfs

#endif
