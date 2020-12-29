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
 * dfs_writer.h
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/dfs/dfs_writer.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DFS_WRITER_H
#define DFS_WRITER_H

#include "access/dfs/dfs_am.h"
#include "access/cstore_psort.h"

namespace dfs {
namespace writer {
/* The interface for all kinds of reader with diffrent file types. */
class ColumnWriter : public BaseObject {
public:
    virtual ~ColumnWriter()
    {
    }

    /* Initialize the class member. */
    virtual void init(IndexInsertInfo *indexInsertInfo) = 0;

    /* Clean the class member. */
    virtual void Destroy() = 0;

    /* Open a new file which means create a new orc writer. */
    virtual void openNewFile(const char *filePath) = 0;

    /*
     * @Description: Spill the buffer into the current writer stream.
     * @IN fileID: the ID of the file for spilling
     * @Return: the left number of rows in the buffer after spilling
     * @See also:
     */
    virtual int spill(uint64 fileID) = 0;

    /* Add a datum of one column into buffer. */
    virtual void appendColDatum(int colID, Datum *vals, bool *isNull, uint32 length) = 0;

    /* Insert the buffer into delta table. */
    virtual void deltaInsert(int option) = 0;

    /* Get the number of buffer rows. */
    virtual uint64 getBufferCapacity() = 0;

    /* Acquire the remaining number of rows to hold in the buffer. */
    virtual uint64 getBufferRows() = 0;

    /* Get the rows in the hdfs file, this function must be called after the m_orcWriter is closed. */
    virtual uint64 getTotalRows() = 0;

    /* Get the totoal number of serialized rows of the current writer. */
    virtual uint64 getSerializedRows() = 0;

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
    virtual bool getMinMax(uint32 colId, char *&minStr, char *&maxStr, bool &hasMin, bool &hasMax) = 0;

    /* Close the current writer stream. */
    virtual void closeCurWriter() = 0;

    /* Check if the file is valid (size is bigger than 0) and return the file size. */
    virtual int64_t verifyFile(char *filePath) = 0;

    /* Increament the number of elments in the buffer. */
    virtual void incrementNumber(int size) = 0;

    /* Deal the clean works on dfs writer. */
    virtual void handleTail() = 0;
};

/*
 * @Description: Create a orc column writer
 * @IN context: memory context
 * @IN rel: the current relation
 * @IN destRel: the dest relation
 * @IN indexInsertInfo: includes index information
 * @IN conn: the connector to dfs
 * @IN parsig: the partition information
 * @Return: a column writer pointer
 * @See also:
 */
ColumnWriter *createORCColWriter(MemoryContext context, Relation rel, Relation destRel,
                                 IndexInsertInfo *indexInsertInfo, DFSConnector *conn, char const *parsig);
}  // namespace writer
}  // namespace dfs

#endif
