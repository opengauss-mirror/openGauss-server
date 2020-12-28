#ifndef CARBONDATA_READER_H
#define CARBONDATA_READER_H

#include <string>
#include "sstream"

#include "../dfs_reader.h"
#include "access/dfs/dfs_stream.h"

namespace dfs {
namespace reader {
class CarbondataReaderImpl;

class CarbondataReader : public DFSReader {
public:
    CarbondataReader(ReaderState *reader_state, dfs::DFSConnector *conn);
    virtual ~CarbondataReader();

    /* Initialization works */
    void begin() DFS_OVERRIDE;

    /* Handle the tailing works which are not destroyers. */
    void end() DFS_OVERRIDE;

    /* Clear the memory and resource. */
    void Destroy() DFS_OVERRIDE;

    /*
     * Load the file transfered in.
     * @_in_param filePath: The path of the file to be load.
     * @_in_param fileRestriction: The restrictions built by the desc table.
     * @return true: The current file is load successfully and can not be skipped.
     * 			false: The current file is skipped so we need load another one.
     */
    bool loadFile(char *file_path, List *file_restriction) DFS_OVERRIDE;

    /*
     * Fetch the next batch including 1000 or less(reach the end of file)
     * rows which meets the predicates.
     * @_in_param batch: The batch to be filled with the proper tuples.
     * @return rows: The number of rows which has been read in the file,
     *                     it isgreater than or equal to the size of batch returned.
     */
    uint64_t nextBatch(VectorBatch *batch, uint64_t curRowsInFile, DFSDesc *dfsDesc,
                       FileOffset *fileOffset) DFS_OVERRIDE;

    /*
     * @Description: Fetch the next batch by tid
     * @IN batch: vector batch
     * @IN rowsSkip: the rows to be skipped before reading
     * @Return: true if tid row is satisfied, or return false
     * @See also:
     */
    void nextBatchByContinueTids(uint64_t rowsSkip, uint64_t rowsToRead, VectorBatch *batch, uint64_t startOffset,
                                 DFSDesc *dfsDesc, FileOffset *fileOffset) DFS_OVERRIDE;

    /*
     * Add a bloom filter into the reader.
     * @_in_param bloomFilter: The bloom filter to be added.
     * @_in_param colIdx: The index of the column on which to add the bloom filter.
     */
    void addBloomFilter(filter::BloomFilter *bloomFilter, int colidx, bool is_runtime) DFS_OVERRIDE;

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
    uint64_t getNumberOfRows() DFS_OVERRIDE;

    /*
     * @Description: Set the buffer size of the reader
     * @IN capacity: the buffer size(number of rows)
     * @See also:
     */
    void setBatchCapacity(uint64_t capacity) DFS_OVERRIDE;

    /*
     * @Description: file is EOF
     * @IN rowsReadInFile: rows read in file
     * @Return: true if tid file is EOF, or return false
     * @See also:
     */
    bool fileEOF(uint64_t rowsReadInFile) DFS_OVERRIDE;

    /*
     * Retune the order of the columns to be read when there is dynamic predicate
     * from vechashjoin. Put the column on which the join depends just behind the
     * last column with static predicates and top it if there is no normal restrictions.
     * @_in_param colIdx: The index of the column on which there is a dynamic predicate.
     */
    void fixOrderedColumnList(uint32_t colIdx);

private:
    void collectFileReadingStatus();
    CarbondataReaderImpl *m_cImpl;
};
}  // namespace reader
}  // namespace dfs

#endif
