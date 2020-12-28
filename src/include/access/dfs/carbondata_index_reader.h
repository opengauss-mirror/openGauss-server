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
 * carbondata_index_reader.h
 *
 * IDENTIFICATION
 *	  src/include/access/dfs/carbondata_index_reader.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CARBONDATA_INDEX_READER_H_
#define CARBONDATA_INDEX_READER_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>

#include "carbondata/format/carbondata_types.h"
#include "carbondata/format/carbondata_index_types.h"
#include "carbondata/format/schema_types.h"
#include "carbondata/index_file_reader.h"
#include "carbondata/inputstream.h"
#include "carbondata/memory.h"
#include "carbondata/thrift_adapter.h"

#include "postgres.h"

#include "access/dfs/dfs_am.h"
#include "access/dfs/dfs_stream.h"
#include "nodes/pg_list.h"
#include "pgxc/locator.h"

extern THR_LOCAL char* PGXCNodeName;

namespace dfs {
#define CARBONDATA_FILENUMBER 1024
#define OBS_OBJECT_NAME_MAXSIZE 1024

#define CARBONDATA_INDEX ".carbonindex"
#define CARBONDATA_DATA ".carbondata"

/*
 * get file name from path
 * @IN FilePath
 * @return: file name
 */
extern const char* FileNameFromPath(const char* filePath);

/*
 * filter file name to filetype
 * @IN fileList
 * @IN fileType
 * @return: fileList after filter
 */
extern List* CarbonFileFilter(List* fileList, const char* fileType);

/*
 * Get the data filename list after match obs filelist.
 * @IN fileList: obs data fileList
 * @IN dataFileList: carbonindex analysis data fileList
 * @return filename list: data filename need to be schedule.
 */
extern List* CarbonDataFileMatch(List* fileList, List* dataFileList);

namespace reader {

class CarbondataIndexReader : public BaseObject {
public:
    CarbondataIndexReader(
        List* allColumnList, List* queryRestrictionList, bool* restrictRequired, bool* readRequired, int16 attrNum);

    ~CarbondataIndexReader()
    {}

    /* Initialization */
    void init(std::unique_ptr<GSInputStream> gsInputStream);

    /* read index file */
    void readIndex();

    /* Clear the memory and resource. */
    void Destroy();

    /*
     * Get the data filename list from carbonindex file.
     * @return filename list: data filename list.
     */
    List* getDataFileList();

    /*
     * Get the data filepath list from carbonindex file.
     * @return filepath list: data filepath list.
     */
    List* getDataFilePathList();

    /*
     * Get the not repeat data filename list from carbonindex file.
     * @return filename list: data filename list only.
     */
    List* getDataFileDeduplication();

private:
    /*
     * Min-max filter, try to skip carbondata blocklet.
     * @IN currentBlocklet: current blocklet index
     * @Out bool: true : skip current blocklet;
     *			  false: not skip current blocklet.
     */
    bool tryToSkipCurrentBlocklet(uint64 currentBlocklet);

    bool checkPredicateOnCurrentBlocklet(uint64 currentBlocklet);

    List* buildRestriction(uint64 rowGroupIndex);

    Node* buildColRestriction(int32 columnIndex, uint64 blockletIndex);

    int32_t GetBlockletMinMax(
        int32 columnIndex, uint64 blockletIndex, bool* hasMinMax, Datum* minDatum, Datum* maxDatum);

    int32_t GetBlockletMinMaxString(carbondata::format::BlockletMinMaxIndex& blockMinMaxIndex,
        int32 realColumnIndex, uint64 blockletIndex, bool* hasMinMax, Datum* minDatum, Datum* maxDatum);

    int32_t GetBlockletMinMaxVarchar(carbondata::format::BlockletMinMaxIndex& blockMinMaxIndex,
        int32 realColumnIndex, uint64 blockletIndex, bool* hasMinMax, Datum* minDatum, Datum* maxDatum);

    int32_t GetBlockletMinMaxBoolean(carbondata::format::BlockletMinMaxIndex& blockMinMaxIndex,
        int32 realColumnIndex, uint64 blockletIndex, bool* hasMinMax, Datum* minDatum, Datum* maxDatum);

    int32_t GetBlockletMinMaxByte(carbondata::format::BlockletMinMaxIndex& blockMinMaxIndex,
        int32 realColumnIndex, uint64 blockletIndex, bool* hasMinMax, Datum* minDatum, Datum* maxDatum);

    int32_t GetBlockletMinMaxShort(carbondata::format::BlockletMinMaxIndex& blockMinMaxIndex,
        int32 realColumnIndex, uint64 blockletIndex, bool* hasMinMax, Datum* minDatum, Datum* maxDatum);

    int32_t GetBlockletMinMaxInt(carbondata::format::BlockletMinMaxIndex& blockMinMaxIndex,
        int32 realColumnIndex, uint64 blockletIndex, bool* hasMinMax, Datum* minDatum, Datum* maxDatum);

    int32_t GetBlockletMinMaxLong(carbondata::format::BlockletMinMaxIndex& blockMinMaxIndex,
        int32 realColumnIndex, uint64 blockletIndex, bool* hasMinMax, Datum* minDatum, Datum* maxDatum);

    int32_t GetBlockletMinMaxFloat(carbondata::format::BlockletMinMaxIndex& blockMinMaxIndex,
        int32 realColumnIndex, uint64 blockletIndex, bool* hasMinMax, Datum* minDatum, Datum* maxDatum);

    int32_t GetBlockletMinMaxDouble(carbondata::format::BlockletMinMaxIndex& blockMinMaxIndex,
        int32 realColumnIndex, uint64 blockletIndex, bool* hasMinMax, Datum* minDatum, Datum* maxDatum);

    int32_t GetBlockletMinMaxTimestamp(carbondata::format::BlockletMinMaxIndex& blockMinMaxIndex,
        int32 realColumnIndex, uint64 blockletIndex, bool* hasMinMax, Datum* minDatum, Datum* maxDatum);

    int32_t GetBlockletMinMaxDate(carbondata::format::BlockletMinMaxIndex& blockMinMaxIndex,
        int32 realColumnIndex, uint64 blockletIndex, bool* hasMinMax, Datum* minDatum, Datum* maxDatum);

    int32_t GetBlockletMinMaxDecimal(carbondata::format::BlockletMinMaxIndex& blockMinMaxIndex,
        int32 realColumnIndex, uint64 blockletIndex, bool* hasMinMax, Datum* minDatum, Datum* maxDatum);

    /*
     * filepath cut to path and name
     * @IN FilePath: filePath need to be cut
     * @OUT Path: path
     * @OUT Name: Name
     */
    void FilePathCut(char*& path, char*& name, const char* filePath);

    /*
     * path and name joint to filepath
     * @IN name: file name
     * @IN path: file path
     * @OUT filePath: filepath
     */
    void FilePathJoint(char*& filePath, const char* name, const char* path);

    inline bool isColumnRequiredToRead(int32 columnIndexInRead)
    {
        return m_readRequired != NULL ? m_readRequired[columnIndexInRead] : false;
    }

    inline bool hasRestriction()
    {
        return m_queryRestrictionList != NULL;
    }

    inline int32 getSchemaOrdinal(int32 columnIndex)
    {
        return m_schemaOrdinalList[columnIndex];
    }

private:
    HTAB* m_dataname_hashtbl;

    char* m_filePath;
    char* m_fileName;
    uint64 m_block_size;

    const Var* m_var;
    OpExpr* m_lessThanExpr;
    OpExpr* m_greaterThanExpr;

    List* m_allColumnList;
    List* m_queryRestrictionList;

    bool* m_restrictRequired;
    bool* m_readRequired;

    /* carbondata columns schema ordinal list. */
    int32* m_schemaOrdinalList;

    int32 m_relAttrNum;

    carbondata::IndexFileReader m_indexfile_reader;

    carbondata::format::IndexHeader m_indexheader;

    std::vector<carbondata::format::BlockIndex> m_vec_blockindex;
};
}  // namespace reader
}  // namespace dfs
#endif /* CARBONDATA_INDEX_READER_H_ */
