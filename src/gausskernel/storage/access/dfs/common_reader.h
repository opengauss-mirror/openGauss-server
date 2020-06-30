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
 * common_reader.h
 *    As for text, csv format file, we have some common function that can read data from file.
 *    we extract these funtions and put them into class.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/common_reader.h
 *
 * -------------------------------------------------------------------------
 */


#ifndef COMMON_READER_H
#define COMMON_READER_H

#include <string>
#include <sstream>

#include "postgres.h"
#include "knl/knl_variable.h"

#include "dfs_reader.h"
#include "access/dfs/dfs_stream.h"
#include "common_parser.h"

namespace dfs {
namespace reader {
class CommonReader : public DFSReader {
public:
    CommonReader(ReaderState *reader_state, dfs::DFSConnector *conn, DFSFileType fileFormat);
    virtual ~CommonReader();

    /* Initialization works */
    virtual void begin();

    /* Handle the tailing works which are not destroyers. */
    virtual void end();

    /* Clear the memory and resource. */
    virtual void Destroy();

    /*
     * Load the file transfered in.
     * @_in_param filePath: The path of the file to be load.
     * @_in_param fileRestriction: The restrictions built by the desc table.
     * @return true: The current file is load successfully and can not be skipped.
     *			   false: The current file is skipped so we need load another one.
     */
    virtual bool loadFile(char *file_path, List *file_restriction);

    /*
     * Fetch the next batch including 1000 or less(reach the end of file)
     * rows which meets the predicates.
     * @_in_param batch: The batch to be filled with the proper tuples.
     * @return rows: The number of rows which has been read in the file,
     *					   it isgreater than or equal to the size of batch returned.
     */
    virtual uint64_t nextBatch(VectorBatch *batch, uint64_t cur_rows_in_file, DFSDesc *dfs_desc,
                               FileOffset *file_offset);

    /*
     * @Description: Fetch the next batch by tid
     * @IN batch: vector batch
     * @IN rowsSkip: the rows to be skipped before reading
     * @Return: true if tid row is satisfied, or return false
     * @See also:
     */
    virtual void nextBatchByContinueTids(uint64_t rowsSkip, uint64_t rowsToRead, VectorBatch *batch,
                                         uint64_t startOffset, DFSDesc *dfsDesc, FileOffset *fileOffset);

    /*
     * Add a bloom filter into the reader.
     * @_in_param bloomFilter: The bloom filter to be added.
     * @_in_param colIdx: The index of the column on which to add the bloom filter.
     */
    virtual void addBloomFilter(filter::BloomFilter *bloomfilter, int colidx, bool is_runtime);

    /*
     * Copy bloomFilter to runTimeBloomFilter
     */
    virtual void copyBloomFilter();

    /* Check bloom filter for partition column, return true if this partition can be skipped */
    virtual bool checkBFPruning(uint32_t colID, char *colValue);

    /*
     * Get the number of all the rows in the current file.
     * @return rows: The number of rows of file.
     */
    virtual uint64_t getNumberOfRows();

    /*
     * @Description: Set the buffer size of the reader
     * @IN capacity: the buffer size(number of rows)
     * @See also:
     */
    virtual void setBatchCapacity(uint64_t capacity);

    /*
     * @Description: file is EOF
     * @IN rowsReadInFile: rows read in file
     * @Return: true if tid file is EOF, or return false
     * @See also:
     */
    virtual bool fileEOF(uint64_t rows_read_in_file);

    void collectFileReadingStatus();

private:
    void parserFields(char **raw_fields, int fields);
    void fillVectorBatch(VectorBatch *batch, const Datum *values, const bool *nulls, int row_index);
    void checkMissingFields(int field_no, int fields, int attr_index);
    void checkExtraFields(int max_field_no, int fields) const;
    void trimEol(char *buf, int &len) const;
    static void ErrorCallback(void *arg);

private:
    ReaderState *m_reader_state;

    dfs::DFSConnector *m_conn;

    MemoryContext m_memory_context;

    MemoryContext m_memory_context_parser;

    char *m_cur_file_path;

    FmgrInfo *m_in_functions;

    Oid *m_typioparams;
    /* whether column can accept empty string. */
    bool *m_accept_empty_str;

    BaseParser *m_parser_impl;

    LineBuffer *m_line_buffer;

    char **m_raw_fields;

    Datum *m_values;

    bool *m_nulls;

    const char *m_line_converted;

    const char *m_cur_attname;

    const char *m_cur_attval;

    /* already read row number. */
    uint64_t m_read_rows;

    /* the total rows in current file. */
    uint64_t m_total_rows;

    bool m_is_eof;

    /* it stores some option of foreign table in order to parser line buffer data. */
    BaseParserOption *m_parser_options;

    DFS_UNIQUE_PTR<dfs::GSInputStream> m_inputstream;

    DFSFileType m_fileFormat;
};
}  // namespace reader
}  // namespace dfs

#endif /* COMMON_READER_H */
