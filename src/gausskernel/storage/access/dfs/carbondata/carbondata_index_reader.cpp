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
 * carbondata_index_reader.cpp
 *
 *
 * IDENTIFICATION
 *         src/gausskernel/storage/access/dfs/carbondata/carbondata_index_reader.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <utility>
#include <string>

#include "access/dfs/dfs_common.h"
#include "access/dfs/dfs_query.h"

#include "access/dfs/carbondata_index_reader.h"

#include "carbondata_stream_adapter.h"
#include "carbondata_column_reader.h"
#include "carbondata/decimal.h"
#include "carbondata/type.h"
#include "carbondata/tostr.h"
#include "carbondata/minmax.h"
#include "carbondata/carbondata_timestamp.h"

#include "optimizer/clauses.h"
#include "optimizer/predtest.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

namespace dfs {
/*
 * @Description: get file name from file path
 * @IN filePath: file path
 * @Return: file name
 */
const char *FileNameFromPath(const char *filePath)
{
    if (NULL == filePath) {
        return NULL;
    }
    const char ch = '/';

    char *file_name;

    file_name = strrchr((char *)filePath, ch);
    if (file_name != NULL) {
        return file_name + 1;
    } else {
        return filePath;
    }
}

/*
 * @Description: get file list by file suffix , deep copy
 * @IN fileList: file list
 * @IN fileType: suffix
 * @Return: copyed file list  which file in list filter by suffix
 */
List *CarbonFileFilter(List *fileList, const char *fileType)
{
    Assert(NULL != fileType);

    if (0 == list_length(fileList))
        return NIL;

    List *filteredList = NIL;
    ListCell *cell = NULL;
    ListCell *prev = NULL;
    ListCell *nextcell = NULL;

    int endC = strlen(fileType);

    for (cell = list_head(fileList); cell != NULL;) {
        bool is_suffix_same = false;

        void *data = lfirst(cell);

        if (IsA(data, SplitInfo)) {
            SplitInfo *splitinfo = (SplitInfo *)data;
            char *fileName = splitinfo->filePath;

            int endF = strlen(fileName);
            if (endC <= endF) {
                /* compare suffix */
                if (0 == pg_strcasecmp((fileName + (endF - endC)), fileType))
                    is_suffix_same = true;
            }

            if (is_suffix_same) {
                /* move to filtered list */
                fileList->length--;
                nextcell = cell->next;

                if (NULL != prev)
                    prev->next = cell->next;
                else
                    fileList->head = cell->next;

                if (fileList->tail == cell)
                    fileList->tail = prev;

                filteredList = lappend2(filteredList, cell);

                cell = nextcell;
            } else {
                prev = cell;
                cell = lnext(cell);
            }
        }
    }
    return filteredList;
}

/*
 * @Description: remove not match file in '*.carbondata file list' from  'all file list'
 * @IN/OUT fileList: 'all file list',  and file in  '*.carbondata file list' will be removed
 * @IN dataFileList: '*.carbondata file list'
 * @Return: remove 'all file list'
 */
List *CarbonDataFileMatch(List *fileList, List *dataFileList)
{
    if (0 == list_length(fileList))
        return NIL;

    ListCell *cell = NULL;
    ListCell *prev = NULL;
    ListCell *nextcell = NULL;

    char *datafilepath = NULL;
    bool found = false;

    HTAB *hashtbl = NULL;
    HASHCTL hash_ctl;

    errno_t errval = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check_errval(errval, , LOG);

    hash_ctl.keysize = OBS_OBJECT_NAME_MAXSIZE;
    hash_ctl.hcxt = CurrentMemoryContext;
    hash_ctl.hash = string_hash;
    hash_ctl.entrysize = OBS_OBJECT_NAME_MAXSIZE;

    hashtbl = hash_create("carbondata datafile match hash table", CARBONDATA_FILENUMBER, &hash_ctl,
                          HASH_CONTEXT | HASH_ELEM | HASH_FUNCTION);

    /* set datafile from analysis .carbonindex file into hashtable */
    for (cell = list_head(dataFileList); cell != NULL; cell = lnext(cell)) {
        void *data = lfirst(cell);

        found = false;
        datafilepath = (char *)data;
        const char *fileName = FileNameFromPath(datafilepath);

        char *name = (char *)hash_search(hashtbl, (const void *)fileName, HASH_ENTER, &found);

        if (!found) {
            /* entry must have key in first elemnt */
            /* name = datafilepath without any effect */
            name = datafilepath;
        }
    }

    /* judge filename from obs filelist is in hashtable
     * if is in hashtable, remove hashtable this key;
     * else if not is in hashtable, remove filelist.
     */
    for (cell = list_head(fileList); cell != NULL; cell = nextcell) {
        void *data = lfirst(cell);
        nextcell = cell->next;

        if (IsA(data, SplitInfo)) {
            SplitInfo *splitinfo = (SplitInfo *)data;
            datafilepath = splitinfo->filePath;

            const char *fileName = FileNameFromPath(datafilepath);

            found = false;

            (char *)hash_search(hashtbl, fileName, HASH_REMOVE, &found);

            if (!found) {
                fileList = list_delete_cell(fileList, cell, prev);
            } else {
                prev = cell;
            }
        }
    }

    /* the remaining datafile in hashtable is not found */
    if (hash_get_num_entries(hashtbl) != 0) {
        HASH_SEQ_STATUS hash_seq;

        hash_seq_init(&hash_seq, hashtbl);
        char *entry = NULL;

        while ((entry = (char *)hash_seq_search(&hash_seq)) != NULL) {
            ereport(LOG, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_CARBONDATA),
                          errmsg("Can't find carbondata file : %s.", entry)));
        }

        ereport(ERROR,
                (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_CARBONDATA), errmsg("Can't find .carbondata file.")));

        return NIL;
    }

    return fileList;
}

namespace reader {
CarbondataIndexReader::CarbondataIndexReader(List *allColumnList, List *queryRestrictionList, bool *restrictRequired,
                                             bool *readRequired, int16 attrNum)
    : m_dataname_hashtbl(NULL),
      m_filePath(NULL),
      m_fileName(NULL),
      m_block_size(0),
      m_var(NULL),
      m_lessThanExpr(NULL),
      m_greaterThanExpr(NULL),
      m_allColumnList(allColumnList),
      m_queryRestrictionList(queryRestrictionList),
      m_restrictRequired(restrictRequired),
      m_readRequired(readRequired),
      m_schemaOrdinalList(NULL),
      m_relAttrNum(attrNum)
{
}

/*
 * @Description: Initialization
 * @IN gsInputStream: input stream
 */
void CarbondataIndexReader::init(std::unique_ptr<GSInputStream> gsInputStream)
{
    FilePathCut(m_filePath, m_fileName, gsInputStream->getName().c_str());
    std::unique_ptr<carbondata::InputStream> carbon_stream(
        new carbondata::CarbondataInputStreamAdapter(std::move(gsInputStream)));

    m_indexfile_reader.Open(std::move(carbon_stream));
}

/*
 * @Description:  read carbonindex file
 */
void CarbondataIndexReader::readIndex()
{
    DFS_TRY()
    {
        /* read index header */
        m_indexfile_reader.ReadIndexHeader(m_indexheader);

        /* read all blocklet index */
        m_indexfile_reader.ReadBlockIndex(m_vec_blockindex);

        m_block_size = (uint64)m_vec_blockindex.size();

        /* init carbondata columns schema ordinal. */
        int32 tableColumnSize = m_indexheader.table_columns.size();
        if (0 < tableColumnSize) {
            m_schemaOrdinalList = (int32 *)palloc0(sizeof(int32) * m_relAttrNum);
            for (int32 it = 0; it < tableColumnSize; it++) {
                int32 schemaOrdinal = m_indexheader.table_columns[it].schemaOrdinal;
                if (schemaOrdinal != -1 && schemaOrdinal < m_relAttrNum) {
                    m_schemaOrdinalList[schemaOrdinal] = it;
                }
            }
        }
    }
    DFS_CATCH();

    DFS_ERRREPORT_WITHARGS(
        "Error occurs while reading carbondata index file %s because of %s, "
        "detail can be found in cn log of %s.",
        MOD_CARBONDATA, m_filePath);
}

/*
 * @Description: Destroy()
 */
void CarbondataIndexReader::Destroy()
{
    pfree_ext(m_lessThanExpr);
    pfree_ext(m_greaterThanExpr);

    pfree_ext(m_filePath);
    pfree_ext(m_fileName);

    /* Hash table reset */
    hash_destroy(m_dataname_hashtbl);

    m_indexfile_reader.Close();
    m_vec_blockindex.clear();
}

/*
 * @Description: get carbondata file list from carbonindex file
 * @Return: arbondata file list
 */
List *CarbondataIndexReader::getDataFileList()
{
    List *dataFileList = NIL;
    if (0 == m_block_size) {
        return NIL;
    }

    for (carbondata::format::BlockIndex blockindex : m_vec_blockindex) {
        char *dataFileName = pstrdup(blockindex.file_name.c_str());

        dataFileList = lappend(dataFileList, dataFileName);
    }
    return dataFileList;
}

/*
 * @Description: deduplicate carbondata file list
 * @Return:  carbondata file list
 */
List *CarbondataIndexReader::getDataFileDeduplication()
{
    if (0 == m_block_size) {
        return NIL;
    }

    HASHCTL hash_ctl;

    errno_t errval = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check_errval(errval, , LOG);

    List *dataFileList = NIL;
    const char *dataName = NULL;

    hash_ctl.keysize = OBS_OBJECT_NAME_MAXSIZE;
    hash_ctl.hcxt = CurrentMemoryContext;
    hash_ctl.hash = string_hash;
    hash_ctl.entrysize = OBS_OBJECT_NAME_MAXSIZE;

    m_dataname_hashtbl = hash_create("carbondata datafile name hash table", CARBONDATA_FILENUMBER, &hash_ctl,
                                     HASH_CONTEXT | HASH_ELEM | HASH_FUNCTION);

    uint64 currentBlockletIndex = 0;

    for (carbondata::format::BlockIndex blockindex : m_vec_blockindex) {
        dataName = blockindex.file_name.c_str();
        if (m_dataname_hashtbl == NULL || dataName == NULL) {
            return NIL;
        }

        bool found = false;

        /* find .carbondata file is in filelist */
        char *name = (char *)hash_search(m_dataname_hashtbl, (const void *)dataName, HASH_FIND, &found);

        /* if current .carbondata file is not in schedule file list, try to skip current blocklet for minmax,
         * if current .carbondata file is in schedule file list, don't need to analysis blocklet minmax. */
        if (!found) {
            /* filter blocklet for minmax value */
            if (!tryToSkipCurrentBlocklet(currentBlockletIndex)) {
                name = (char *)hash_search(m_dataname_hashtbl, (const void *)dataName, HASH_ENTER, &found);
                name = (char *)dataName;

                char *filePath = NULL;

                /* joint filepath and filename */
                FilePathJoint(filePath, dataName, m_filePath);

                dataFileList = lappend(dataFileList, filePath);
            }
        }
        currentBlockletIndex++;
    }

    return dataFileList;
}

/*
 * @Description: try to skip current blocklet
 * @IN  currentBlocklet: bolcklet number
 * @Return: true for skip
 */
bool CarbondataIndexReader::tryToSkipCurrentBlocklet(uint64 currentBlocklet)
{
    if (hasRestriction() && !checkPredicateOnCurrentBlocklet(currentBlocklet)) {
        return true;
    }
    return false;
}

/*
 * @Description: check predicate blocklet
 * @IN currentBlocklet:bolcklet number
 * @Return: false for skip
 */
bool CarbondataIndexReader::checkPredicateOnCurrentBlocklet(uint64 currentBlocklet)
{
    List *restrictions = buildRestriction(currentBlocklet);
    bool ret = !predicate_refuted_by(restrictions, m_queryRestrictionList, false);

    return ret;
}

/*
 * @Description:  build blocklet restriction
 * @IN currentBlocklet: bolcklet number
 * @Return: restriction list
 */
List *CarbondataIndexReader::buildRestriction(uint64 currentBlocklet)
{
    List *fileRestrictionList = NIL;

    if (m_restrictRequired == NULL)
        return fileRestrictionList;

    Assert(currentBlocklet < m_block_size);

    for (int32 columnIndex = 0; columnIndex < m_relAttrNum; columnIndex++) {
        Node *baseRestriction = NULL;

        if (m_restrictRequired[columnIndex] && isColumnRequiredToRead(columnIndex)) {
            baseRestriction = buildColRestriction(columnIndex, currentBlocklet);
            if (NULL != baseRestriction) {
                fileRestrictionList = lappend(fileRestrictionList, baseRestriction);
            }
        }
    }

    return fileRestrictionList;
}

/*
 * @Description: build column restriction
 * @IN columnIndex: column number
 * @IN blockletIndex: bolcklet number
 * @Return: restriction
 */
Node *CarbondataIndexReader::buildColRestriction(int32 columnIndex, uint64 blockletIndex)
{
    m_var = GetVarFromColumnList(m_allColumnList, static_cast<int32>(columnIndex + 1));
    if (m_var == NULL) {
        return NULL;
    }

    m_lessThanExpr = MakeOperatorExpression(const_cast<Var *>(m_var), BTLessEqualStrategyNumber);
    m_greaterThanExpr = MakeOperatorExpression(const_cast<Var *>(m_var), BTGreaterEqualStrategyNumber);

    Datum minValue = 0;
    Datum maxValue = 0;
    bool hasMinMax = false;
    Node *baseRestriction = NULL;
    int32 hasStatistics = 0;

    try {
        hasStatistics = GetBlockletMinMax(columnIndex, blockletIndex, &hasMinMax, &minValue, &maxValue);
    } catch (abi::__forced_unwind &) {
        throw;
    } catch (std::exception &ex) {
        hasStatistics = false;
    } catch (...) {
    }

    if (hasStatistics) {
        baseRestriction = MakeBaseConstraintWithExpr(m_lessThanExpr, m_greaterThanExpr, minValue, maxValue, hasMinMax,
                                                     hasMinMax);
    }

    return baseRestriction;
}

/*
 * @Description: get blocklet min max value
 * @IN columnIndex: column number
 * @IN blockletIndex: blocklet number
 * @IOUT hasMinMax: has min max value
 * @OUT minDatum: min value datum
 * @OUT maxDatum: max value datum
 * @Return: 1 for secuess, 0 for fail
 * @See also:
 */

int32 CarbondataIndexReader::GetBlockletMinMaxString(carbondata::format::BlockletMinMaxIndex &blockMinMaxIndex,
                                                     int32 realColumnIndex, uint64 blockletIndex, bool *hasMinMax,
                                                     Datum *minDatum, Datum *maxDatum)
{
    const char *minvalue = (const char *)blockMinMaxIndex.min_values[realColumnIndex].c_str();
    const char *maxvalue = (const char *)blockMinMaxIndex.max_values[realColumnIndex].c_str();
    *minDatum = CStringGetTextDatum(minvalue);
    *maxDatum = CStringGetTextDatum(maxvalue);

    return 1;
}

int32 CarbondataIndexReader::GetBlockletMinMaxVarchar(carbondata::format::BlockletMinMaxIndex &blockMinMaxIndex,
                                                      int32 realColumnIndex, uint64 blockletIndex, bool *hasMinMax,
                                                      Datum *minDatum, Datum *maxDatum)
{
    const char *minvalue = (const char *)blockMinMaxIndex.min_values[realColumnIndex].c_str();
    const char *maxvalue = (const char *)blockMinMaxIndex.max_values[realColumnIndex].c_str();
    *minDatum = DirectFunctionCall3(varcharin, CStringGetDatum(minvalue), ObjectIdGetDatum(InvalidOid),
                                    Int32GetDatum(m_var->vartypmod));
    *maxDatum = DirectFunctionCall3(varcharin, CStringGetDatum(maxvalue), ObjectIdGetDatum(InvalidOid),
                                    Int32GetDatum(m_var->vartypmod));

    return 1;
}

int32 CarbondataIndexReader::GetBlockletMinMaxBoolean(carbondata::format::BlockletMinMaxIndex &blockMinMaxIndex,
                                                      int32 realColumnIndex, uint64 blockletIndex, bool *hasMinMax,
                                                      Datum *minDatum, Datum *maxDatum)
{
    bool minvalue = false;
    bool maxvalue = false;
    carbondata::ReadMinMaxBool(minvalue, (const unsigned char *)blockMinMaxIndex.min_values[realColumnIndex].c_str());
    carbondata::ReadMinMaxBool(maxvalue, (const unsigned char *)blockMinMaxIndex.max_values[realColumnIndex].c_str());
    *minDatum = BoolGetDatum(minvalue);
    *maxDatum = BoolGetDatum(maxvalue);

    return 1;
}

int32 CarbondataIndexReader::GetBlockletMinMaxByte(carbondata::format::BlockletMinMaxIndex &blockMinMaxIndex,
                                                   int32 realColumnIndex, uint64 blockletIndex, bool *hasMinMax,
                                                   Datum *minDatum, Datum *maxDatum)
{
    bool dimension = m_indexheader.table_columns[realColumnIndex].dimension;
    int32 ret = 0;

    int8_t minvalue = 0;
    int8_t maxvalue = 0;
    if (dimension) {
        /* because of carbondata byte sort bug, do not minmax filter for byte sort. */
    } else {
        carbondata::ReadMinMaxByte(minvalue,
                                   (const unsigned char *)blockMinMaxIndex.min_values[realColumnIndex].c_str());
        carbondata::ReadMinMaxByte(maxvalue,
                                   (const unsigned char *)blockMinMaxIndex.max_values[realColumnIndex].c_str());

        *minDatum = Int16GetDatum((int16)minvalue);
        *maxDatum = Int16GetDatum((int16)maxvalue);
        ret = 1;
    }

    return ret;
}

int32 CarbondataIndexReader::GetBlockletMinMaxShort(carbondata::format::BlockletMinMaxIndex &blockMinMaxIndex,
                                                    int32 realColumnIndex, uint64 blockletIndex, bool *hasMinMax,
                                                    Datum *minDatum, Datum *maxDatum)
{
    bool dimension = m_indexheader.table_columns[realColumnIndex].dimension;

    int16 minvalue = 0;
    int16 maxvalue = 0;
    if (dimension) {
        carbondata::ReadMinMaxBySort<int16>(minvalue,
                                            (const unsigned char *)blockMinMaxIndex.min_values[realColumnIndex].c_str());
        carbondata::ReadMinMaxBySort<int16>(maxvalue,
                                            (const unsigned char *)blockMinMaxIndex.max_values[realColumnIndex].c_str());
    } else {
        carbondata::ReadMinMaxT<int16>(minvalue,
                                       (const unsigned char *)blockMinMaxIndex.min_values[realColumnIndex].c_str());
        carbondata::ReadMinMaxT<int16>(maxvalue,
                                       (const unsigned char *)blockMinMaxIndex.max_values[realColumnIndex].c_str());
    }
    *minDatum = Int16GetDatum(minvalue);
    *maxDatum = Int16GetDatum(maxvalue);

    return 1;
}

int32 CarbondataIndexReader::GetBlockletMinMaxInt(carbondata::format::BlockletMinMaxIndex &blockMinMaxIndex,
                                                  int32 realColumnIndex, uint64 blockletIndex, bool *hasMinMax,
                                                  Datum *minDatum, Datum *maxDatum)
{
    bool dimension = m_indexheader.table_columns[realColumnIndex].dimension;

    int32 minvalue = 0;
    int32 maxvalue = 0;
    if (dimension) {
        carbondata::ReadMinMaxBySort<int32>(minvalue,
                                            (const unsigned char *)blockMinMaxIndex.min_values[realColumnIndex].c_str());
        carbondata::ReadMinMaxBySort<int32>(maxvalue,
                                            (const unsigned char *)blockMinMaxIndex.max_values[realColumnIndex].c_str());
    } else {
        carbondata::ReadMinMaxT<int32>(minvalue,
                                       (const unsigned char *)blockMinMaxIndex.min_values[realColumnIndex].c_str());
        carbondata::ReadMinMaxT<int32>(maxvalue,
                                       (const unsigned char *)blockMinMaxIndex.max_values[realColumnIndex].c_str());
    }
    *minDatum = Int32GetDatum(minvalue);
    *maxDatum = Int32GetDatum(maxvalue);

    return 1;
}

int32 CarbondataIndexReader::GetBlockletMinMaxLong(carbondata::format::BlockletMinMaxIndex &blockMinMaxIndex,
                                                   int32 realColumnIndex, uint64 blockletIndex, bool *hasMinMax,
                                                   Datum *minDatum, Datum *maxDatum)
{
    bool dimension = m_indexheader.table_columns[realColumnIndex].dimension;

    int64 minvalue = 0;
    int64 maxvalue = 0;
    if (dimension) {
        carbondata::ReadMinMaxBySort<int64>(minvalue,
                                            (const unsigned char *)blockMinMaxIndex.min_values[realColumnIndex].c_str());
        carbondata::ReadMinMaxBySort<int64>(maxvalue,
                                            (const unsigned char *)blockMinMaxIndex.max_values[realColumnIndex].c_str());
    } else {
        carbondata::ReadMinMaxT<int64>(minvalue,
                                       (const unsigned char *)blockMinMaxIndex.min_values[realColumnIndex].c_str());
        /* because of carbondata long minvalue bug, set minvalue is INT64_MIN. */
        minvalue = PG_INT64_MIN;
        carbondata::ReadMinMaxT<int64>(maxvalue,
                                       (const unsigned char *)blockMinMaxIndex.max_values[realColumnIndex].c_str());
    }
    *minDatum = Int64GetDatum(minvalue);
    *maxDatum = Int64GetDatum(maxvalue);

    return 1;
}

int32 CarbondataIndexReader::GetBlockletMinMaxFloat(carbondata::format::BlockletMinMaxIndex &blockMinMaxIndex,
                                                    int32 realColumnIndex, uint64 blockletIndex, bool *hasMinMax,
                                                    Datum *minDatum, Datum *maxDatum)
{
    float minvalue = 0.0;
    float maxvalue = 0.0;
    carbondata::ReadMinMaxFloat(minvalue, (const unsigned char *)blockMinMaxIndex.min_values[realColumnIndex].c_str());
    carbondata::ReadMinMaxFloat(maxvalue, (const unsigned char *)blockMinMaxIndex.max_values[realColumnIndex].c_str());
    *minDatum = Float4GetDatum(minvalue);
    *maxDatum = Float4GetDatum(maxvalue);

    return 1;
}

int32 CarbondataIndexReader::GetBlockletMinMaxDouble(carbondata::format::BlockletMinMaxIndex &blockMinMaxIndex,
                                                     int32 realColumnIndex, uint64 blockletIndex, bool *hasMinMax,
                                                     Datum *minDatum, Datum *maxDatum)
{
    double minvalue = 0.0;
    double maxvalue = 0.0;
    carbondata::ReadMinMaxDouble(minvalue, (const unsigned char *)blockMinMaxIndex.min_values[realColumnIndex].c_str());
    carbondata::ReadMinMaxDouble(maxvalue, (const unsigned char *)blockMinMaxIndex.max_values[realColumnIndex].c_str());
    *minDatum = Float8GetDatum(minvalue);
    *maxDatum = Float8GetDatum(maxvalue);

    return 1;
}

int32 CarbondataIndexReader::GetBlockletMinMaxTimestamp(carbondata::format::BlockletMinMaxIndex &blockMinMaxIndex,
                                                        int32 realColumnIndex, uint64 blockletIndex, bool *hasMinMax,
                                                        Datum *minDatum, Datum *maxDatum)
{
    int64 minvalue = 0;
    int64 maxvalue = 0;
    carbondata::ReadMinMaxTime(minvalue, (const unsigned char *)blockMinMaxIndex.min_values[realColumnIndex].c_str());
    carbondata::ReadMinMaxTime(maxvalue, (const unsigned char *)blockMinMaxIndex.max_values[realColumnIndex].c_str());
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

int32 CarbondataIndexReader::GetBlockletMinMaxDate(carbondata::format::BlockletMinMaxIndex &blockMinMaxIndex,
                                                   int32 realColumnIndex, uint64 blockletIndex, bool *hasMinMax,
                                                   Datum *minDatum, Datum *maxDatum)
{
    int32 minvalue = 0;
    int32 maxvalue = 0;
    carbondata::ReadMinMaxDate(minvalue, (const unsigned char *)blockMinMaxIndex.min_values[realColumnIndex].c_str());
    carbondata::ReadMinMaxDate(maxvalue, (const unsigned char *)blockMinMaxIndex.max_values[realColumnIndex].c_str());
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

int32 CarbondataIndexReader::GetBlockletMinMaxDecimal(carbondata::format::BlockletMinMaxIndex &blockMinMaxIndex,
                                                      int32 realColumnIndex, uint64 blockletIndex, bool *hasMinMax,
                                                      Datum *minDatum, Datum *maxDatum)
{
    int32 precision = m_indexheader.table_columns[realColumnIndex].precision;
    uint32 minlength = blockMinMaxIndex.min_values[realColumnIndex].length() - 1;
    uint32 maxlength = blockMinMaxIndex.max_values[realColumnIndex].length() - 1;
    carbondata::BigInt minvalue = carbondata::ByteToBigInt(blockMinMaxIndex.min_values[realColumnIndex].c_str() + 1,
                                                           minlength, precision);
    carbondata::BigInt maxvalue = carbondata::ByteToBigInt(blockMinMaxIndex.max_values[realColumnIndex].c_str() + 1,
                                                           maxlength, precision);
    uint8 minScale = (uint8)blockMinMaxIndex.min_values[realColumnIndex].c_str()[0];
    uint8 maxScale = (uint8)blockMinMaxIndex.max_values[realColumnIndex].c_str()[0];
    if (precision > 0 && precision <= 18) {
        *minDatum = makeNumeric64(minvalue.bigInt64, minScale);
        *maxDatum = makeNumeric64(maxvalue.bigInt64, maxScale);
    } else {
        *minDatum = makeNumeric128(minvalue.bigInt128, minScale);
        *maxDatum = makeNumeric128(maxvalue.bigInt128, maxScale);
    }

    return 1;
}

int32 CarbondataIndexReader::GetBlockletMinMax(int32 columnIndex, uint64 blockletIndex, bool *hasMinMax,
                                               Datum *minDatum, Datum *maxDatum)
{
    int32 ret = 0;
    if (m_block_size <= blockletIndex)
        return ret;

    carbondata::format::BlockletMinMaxIndex &blockMinMaxIndex =
        m_vec_blockindex[blockletIndex].block_index.min_max_index;

    int32 realColumnIndex = getSchemaOrdinal(columnIndex);
    *hasMinMax = blockMinMaxIndex.min_max_presence[realColumnIndex];

    if (!(*hasMinMax))
        return ret;

    carbondata::format::DataType::type currentDataType = m_indexheader.table_columns[realColumnIndex].data_type;

    switch (currentDataType) {
        case carbondata::format::DataType::STRING: {
            ret = GetBlockletMinMaxString(blockMinMaxIndex, realColumnIndex, blockletIndex, hasMinMax, minDatum,
                                          maxDatum);
            break;
        }

        case carbondata::format::DataType::VARCHAR: {
            ret = GetBlockletMinMaxVarchar(blockMinMaxIndex, realColumnIndex, blockletIndex, hasMinMax, minDatum,
                                           maxDatum);
            break;
        }

        case carbondata::format::DataType::BOOLEAN: {
            ret = GetBlockletMinMaxBoolean(blockMinMaxIndex, realColumnIndex, blockletIndex, hasMinMax, minDatum,
                                           maxDatum);
            break;
        }

        case carbondata::format::DataType::BYTE: {
            ret = GetBlockletMinMaxByte(blockMinMaxIndex, realColumnIndex, blockletIndex, hasMinMax, minDatum,
                                        maxDatum);
            break;
        }

        case carbondata::format::DataType::SHORT: {
            ret = GetBlockletMinMaxShort(blockMinMaxIndex, realColumnIndex, blockletIndex, hasMinMax, minDatum,
                                         maxDatum);
            break;
        }

        case carbondata::format::DataType::INT: {
            ret = GetBlockletMinMaxInt(blockMinMaxIndex, realColumnIndex, blockletIndex, hasMinMax, minDatum, maxDatum);
            break;
        }

        case carbondata::format::DataType::LONG: {
            ret = GetBlockletMinMaxLong(blockMinMaxIndex, realColumnIndex, blockletIndex, hasMinMax, minDatum,
                                        maxDatum);
            break;
        }

        case carbondata::format::DataType::FLOAT: {
            ret = GetBlockletMinMaxFloat(blockMinMaxIndex, realColumnIndex, blockletIndex, hasMinMax, minDatum,
                                         maxDatum);
            break;
        }

        case carbondata::format::DataType::DOUBLE: {
            ret = GetBlockletMinMaxDouble(blockMinMaxIndex, realColumnIndex, blockletIndex, hasMinMax, minDatum,
                                          maxDatum);
            break;
        }

        case carbondata::format::DataType::TIMESTAMP: {
            ret = GetBlockletMinMaxTimestamp(blockMinMaxIndex, realColumnIndex, blockletIndex, hasMinMax, minDatum,
                                             maxDatum);
            break;
        }

        case carbondata::format::DataType::DATE: {
            ret = GetBlockletMinMaxDate(blockMinMaxIndex, realColumnIndex, blockletIndex, hasMinMax, minDatum,
                                        maxDatum);
            break;
        }

        case carbondata::format::DataType::DECIMAL: {
            ret = GetBlockletMinMaxDecimal(blockMinMaxIndex, realColumnIndex, blockletIndex, hasMinMax, minDatum,
                                           maxDatum);
            break;
        }
        case carbondata::format::DataType::BINARY: {
            ret = 0;
            break;
        }
        case carbondata::format::DataType::ARRAY:
        case carbondata::format::DataType::STRUCT:
        case carbondata::format::DataType::MAP:
        default: {
            const char *carbondataTypeStr = carbondata::typeToString(currentDataType);
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_CARBONDATA),
                     errmsg("Unsupported data type for min-max filter: %s(%u).", carbondataTypeStr, currentDataType)));
            break;
        }
    }
    return ret;
}

/*
 * @Description: split full file path to path and file name
 * @OUT path: path
 * @OUT name: file name
 * @IN filePath: full file path
 */
void CarbondataIndexReader::FilePathCut(char *&path, char *&name, const char *filePath)
{
    errno_t rc = EOK;
    if (NULL == filePath) {
        return;
    }
    Size str_size = strlen(filePath);

    const char *filename = FileNameFromPath(filePath);
    Size name_size = strlen(filename);

    name = pstrdup(filename);

    Size path_size = str_size - name_size;

    path = (char *)palloc0(sizeof(char) * (path_size + 1));
    rc = strncpy_s(path, path_size + 1, filePath, path_size);
    securec_check(rc, "", "");

    path[path_size] = '\0';

    return;
}

/*
 * @Description: combine file name and path to full file path
 * @OUT filePath: full file path
 * @IN name: file name
 * @IN path: path
 */
void CarbondataIndexReader::FilePathJoint(char *&filePath, const char *name, const char *path)
{
    errno_t rc = EOK;
    if (NULL == name || NULL == path) {
        return;
    }

    Size file_path_size = strlen(name) + strlen(path) + 1;

    filePath = (char *)palloc0(sizeof(char) * (file_path_size));
    filePath[0] = '\0';

    rc = strcat_s(filePath, file_path_size, path);
    securec_check(rc, "", "");
    rc = strcat_s(filePath, file_path_size, name);
    securec_check(rc, "", "");

    return;
}
}  // namespace reader
}  // namespace dfs
