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
 * ---------------------------------------------------------------------------------------
 * 
 * dfsdesc.h
 *        classes & routines to support dfsdesc
 * 
 * 
 * IDENTIFICATION
 *        src/include/dfsdesc.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DFSDESC_H
#define DFSDESC_H

#include "securec.h"
#include "securec_check.h"
#include "nodes/pg_list.h"
#include "utils/snapshot.h"
#include "access/htup.h"
#include "access/tupdesc.h"

#define DfsDescMaxAttrNum 11

#define Anum_pg_dfsdesc_duid 1
#define Anum_pg_dfsdesc_partid 2
#define Anum_pg_dfsdesc_rowcount 3
#define Anum_pg_dfsdesc_magic 4
#define Anum_pg_dfsdesc_min 5
#define Anum_pg_dfsdesc_max 6
#define Anum_pg_dfsdesc_deletemap 7
#define Anum_pg_dfsdesc_relativelyfilename 8
#define Anum_pg_dfsdesc_extra 9
#define Anum_pg_dfsdesc_columnmap 10
#define Anum_pg_dfsdesc_filesize 11

#define MAX_LOADED_DFSDESC 100
#define MAX_DFS_FILE_NAME_SIZE 256

#define MIN_MAX_SIZE 32
#define BITS_IN_BYTE 8
#define MEGA_SHIFT 20

#define DFSDESC_MAX_COL DfsDescMaxAttrNum
#define NON_PARTITION_TALBE_PART_NUM 0xFFFF
#define NULL_MAGIC 255

#define DfsDescIndexMaxAttrNum 1

/*
 * Since the attno starts at 1, so the first bit of the map is never used.
 * so we need add 1 here to avoid memory overflow.
 */
#define GET_COLMAP_LENGTH(nCols) (((uint32)(int32)(nCols) >> 3) + 1)

/*
 * we need to get the delete map, column map, min value, max value from
 * Desc table. the logic of getting delete map is same to the logic of
 * getting column. Between the logic of getting min value and  logic of
 * getting max value are same.
 * So build a enum for function parameter.
 */
typedef enum { DELETE_MAP_ATTR = 0, COLUMN_MAP_ATTR, MIN_COLUMN_ATTR, MAX_COLUMN_ATTR } ColumnEnum;

typedef struct {
    int Len;
    char szContent[MIN_MAX_SIZE];

} stMinMax;

class DFSDesc : public BaseObject {
public:
    DFSDesc();
    DFSDesc(uint32 PartId, uint32 DescId, uint32 ColCnt);

    void Destroy();
    virtual ~DFSDesc();

public:
    /*
     * get column count
     */
    uint32 GetColCnt() const
    {
        return m_ColCnt;
    }
    /*
     * set column count
     */
    void SetColCnt(uint32 ColCnt)
    {
        m_ColCnt = ColCnt;
    }

    /*
     * get transaction Id that alway is tuple xmin if has set
     */
    uint32 GetXId() const
    {
        return m_Xmin;
    }

    /*
     * set transaction Id (tuple xmin)
     */
    void SetXId(uint32 XId)
    {
        m_Xmin = XId;
    }

    /*
     * get the descriptor Id
     */
    uint32 GetDescId() const
    {
        return m_DescId;
    }

    /*
     * set the descriptor Id
     */
    void SetDescId(uint32 DescId)
    {
        m_DescId = DescId;
    }

    /*
     * get the row count include those deleted
     */
    uint32 GetRowCnt() const
    {
        return m_RowCnt;
    }

    /*
     * get the row count include those deleted
     */
    void SetRowCnt(uint32 RowCnt)
    {
        m_RowCnt = RowCnt;
    }

    /* get the size of hdfs file */
    int64 GetFileSize() const
    {
        return m_FileSize;
    }

    /* set the size of hdfs file */
    void SetFileSize(int64 FileSize)
    {
        m_FileSize = FileSize;
    }

    /*
     * get the magic that may be the current transaction Id(cu use that)
     */
    uint32 GetMagic() const
    {
        return m_Magic;
    }

    /*
     * set the magic that may be the current transaction Id(cu use that)
     */
    void SetMagic(uint32 Magic)
    {
        m_Magic = Magic;
    }

    /*
     * get  file name
     */
    const char* GetFileName() const
    {
        return m_FileName;
    }

    /*
     * set file name
     */
    void SetFileName(const char* pFileName)
    {
        if (pFileName != NULL) {
            size_t len = strlen(pFileName);
            if (0 == m_name_len) {
                m_FileName = (char*)palloc(len + 1);
                m_name_len = len + 1;
            }

            if ((len + 1) > m_name_len) {
                m_FileName = (char*)repalloc(m_FileName, len + 1);
                m_name_len = len + 1;
            }

            errno_t rc = strcpy_s(m_FileName, m_name_len, pFileName);
            securec_check(rc, "", "");
        }

        return;
    }

    /**
     * @Description: Set the column map.
     * @in colMap, The column map string.
     * @in length, The length of colMap string.
     * @return None.
     */
    void setColMap(const char* colMap, int length)
    {
        if (colMap != NULL) {
            errno_t rc = EOK;
            m_colMap = (char*)palloc0(length + 1);
            rc = memcpy_s(m_colMap, length + 1, colMap, length);
            securec_check(rc, "", "");
            m_colMapLen = length;
        } else {
            /*
             * Never occur here.
             */
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN), errmsg("The table has no column.")));
        }
    }

    /**
     * @Description: Get the column map.
     * @return return the column map.
     */
    const char* getColMap()
    {
        return m_colMap;
    }

    /**
     * @Description: Get the length of column map binary.
     * @return the length.
     */
    uint32 getColMapLength()
    {
        return m_colMapLen;
    }

    /**
     * @Description: Whether the column is default column. The default column means
     * that the column value is stored in pg_attribute.
     * @in attNo, The attribute number of the column. It starts from 1.
     * @return If the column is default column, return true, otherwise retuen false.
     */
    bool isDefaultColumn(uint32 attNo)
    {
        Assert(NULL != m_colMap);
        bool defaultCol = true;
        if ((attNo >> 3) > m_colMapLen) {
            defaultCol = true;
        } else if ((unsigned char)m_colMap[attNo >> 3] & (1 << (attNo % BITS_IN_BYTE))) {
            defaultCol = false;
        }
        return defaultCol;
    }

    /*
     * get the table partition id
     */
    uint32 GetPartId() const
    {
        return m_PartId;
    }

    /*
     * set the table partition id
     */
    void SetPartId(uint32 PartId)
    {
        m_PartId = PartId;
    }

    /*
     * @Description: get the min value according to the column number
     * @IN Col:  the column number
     * @Return: the min value or NULL
     * @See also:
     */
    stMinMax* GetMinVal(uint32 Col) const
    {
        if ((Col >= m_ColCnt) || (m_MinVal == NULL)) {
            return NULL;
        }

        return m_MinVal[Col];
    }

    /*
     * @Description: set the min value
     * @IN Col: column id
     * @IN pMinVal: the string of min value
     * @IN Size: the size of string
     * @Return: 0 if success, 1 if fail
     * @See also:
     */
    int SetMinVal(uint32 Col, const char* pMinVal, uint32 Size);

    /*
     * @Description: get the max value according to the column number
     * @IN Col:  the column number
     * @Return: the max value or NULL
     * @See also:
     */
    stMinMax* GetMaxVal(uint32 Col) const
    {
        if ((Col >= m_ColCnt) || (m_MinVal == NULL)) {
            return NULL;
        }

        return m_MaxVal[Col];
    }

    /*
     * @Description: set the max value
     * @IN Col: column id
     * @IN pMinVal: the string of max value
     * @IN Size: the size of string
     * @Return: 0 if success, 1 if fail
     * @See also:
     */
    int SetMaxVal(uint32 Col, const char* pMaxVal, uint32 Size);

    /*
     * set the delete map value,Just copy
     */
    int MaskDelMap(const char* pDelMap, uint32 Size, bool ChkDelHasDone = true);

    /*
     * get the delete map value
     */
    const char* GetDelMap() const
    {
        return m_DelMap;
    }

    /*tell whether is deleted by row number*/
    bool IsDeleted(uint32 Row)
    {
        bool Ret = false;

        /*not set delmap means none was deleted. exceed row scope also tell false*/
        if ((m_DelMap == NULL) || (Row >= m_RowCnt)) {
            Ret = false;
        } else {
            if ((unsigned char)m_DelMap[Row >> 3] & (1 << (Row % 8))) {
                Ret = true;
            }
        }

        return Ret;
    }

    void Reset();

    /* tid for this desc tuple */
    ItemPointerData tid;

private:
    int AllocMinMax();
    void DeallocMinMax();
    int AllocDelMap();
    void DeallocDelMap();

private:
    /*
     * DfsDesc Id
     */
    uint32 m_DescId;

    /*
     * row count include those deleted
     */
    uint32 m_RowCnt;

    /* file size */
    int64 m_FileSize;

    /*
     * column count of the table that described
     */
    uint32 m_ColCnt;

    /*
     *delete map buffer size in bytes
     */
    uint32 m_DelMapBaseSize;
    /*
     *min&max buffer size in bytes (one column)
     */
    uint32 m_MinMaxBaseSize;

    /*
     *partition id of the table that described
     */
    uint32 m_PartId;

    /*
     * magic id
     */
    uint32 m_Magic;

    /*
     * transation id
     */
    uint32 m_Xmin;

    /*
     * Orc file name
     */
    char* m_FileName;
    size_t m_name_len;

public:
    /*
    * Min values array
    * the column min value format is (head byte - total length
                                          other bytes - actual min value)
       for example the column is INT type and the min value is 1
       then the head byte store the length is 5, and next 4 bytes is the 1 values

    */
    stMinMax** m_MinVal;

    /*
    * Max values array
    * the column  max value format is (head byte - length
                                          other bytes - actual max value)
       for example the column is INT type and the max value is 1
       then the head byte store the length is 5 and next 4 bytes is the 1 values
    */
    stMinMax** m_MaxVal;

private:
    /*
     * delete map values
     */
    char* m_DelMap;

    /**
     * It stores the columns by bitmap method. When read data, the m_colMap stores
     * the columns which is included in the corresponding file. When insert data,
     * the m_colMap stores all columns, but it excludeds dropped columns.
     */
    char* m_colMap;
    uint32 m_colMapLen;
};

class DFSDescHandler : public BaseObject {

public:
    /**
     * @Description: The construct function.
     * @in MaxBatCnt, Max load count in one batch.
     * @in SrcRelCols, The column count of described table.
     * @in mainRel, The main table.
     * @return None.
     */
    DFSDescHandler(uint32 MaxBatCnt, int SrcRelCols, Relation mainRel);

    /*
     * deconstruction
     */
    virtual ~DFSDescHandler();

public:
    /*
     * load dfs descriptors in rows
     * pDFSDescArray [in/out] - DFSDesc array to Contain descriptors
     * Size[in] -size of DFSDesc array
     * Partid[in] - which table partition to load (partition table use it actual id,
     *             non-partition table use NON_PARTITION_TALBE_PART_NUM)
     * StartDescId [in]- the descriptor id to start load
     * SnapShot [in] - which snapshot, fill NULL is to use GetActiveSnapshot
     * pLoadCnt [out] - contain the number of exact loaded
     * pLastId[out] - the last descriptor id loaded
     *
     * return 0 if success
     */
    int Load(DFSDesc* pDFSDescArray, uint32 Size, uint32 PartId, uint32 StartDescId, Snapshot SnapShot,
        uint32* pLoadCnt, uint32* pLastId);

    /*
     * We always generate xlog for dfsdesc tuple
     */
    /*
     * add dfs descriptors in rows
     * pDFSDescArray [in] - DFSDesc array to append
     * Size[in] -size of DFSDesc array
     * CmdId[in] - the command id
     * options [in]-  the options heap_insert,  (advice generate xlog for dfsdesc tuple)
     *
     * return 0 if success
     */
    int Add(DFSDesc* pDFSDescArray, uint32 Cnt, CommandId CmdId, int Options);

    /*
     * delete rows by mask the delete map buffer
     * Tid [in] - file id & row offset
     * return none
     */
    uint32 SetDelMap(ItemPointer Tid, bool reportError);

    /*
     * flush the delete map buffer to descriptor
     * return none
     */
    void FlushDelMap();

    /*
     * get how many rows not deleted
     */
    uint64 GetLivedRowNumbers(int64* deadrows);

    /*
     * dfsdesc to tuple
     * pDFSDesc[in] - which descriptor to handle
     * pTupVals[in/out] - the values of columns
     * pTupNulls[in/out] - mark which column is NULL
     * DfsDescTupDesc[in] - description of the heap tuple
     * return the heap tuple
     */
    HeapTuple DfsDescToTuple(DFSDesc* pDFSDesc, Datum* pTupVals, bool* pTupNulls, TupleDesc DfsDescTupDesc);

    /*
     * dfsdesc to tuple
     * HeapTuple [in]- which heap tuple to handle
     * DfsDescTupDesc[in] - description of the heap tuple
     * pDFSDesc[out] - the DFSDesc changed from heap tuple
     */
    void TupleToDfsDesc(HeapTuple pCudescTup, TupleDesc DfsDescTupDesc, DFSDesc* pDFSDesc);

    /*
     * get list of desc tuples in which some row(s) was deleted
     */
    List* GetDescsToBeMerged(Snapshot _snapshot);

    /*
     * get all desc tuples
     */
    List* GetAllDescs(Snapshot _snapshot);

    /*
     * test whether row is deleted in one desc tuple
     */
    bool ContainDeleteRow(const char* all_valid_bitmap, int bitmap_length, const char* delmap, int rows) const;

    /*
     * delete one desc tuple
     */
    void DeleteDesc(DFSDesc* desc) const;

private:
    /*
     * get list of desc tuples in which some row(s) was deleted or not
     */
    List* GetDescTuples(Snapshot _snapshot, bool only_tuples_with_invalid_data);

    /**
     * @Description: Set minimum/maximun value of the each column in current file from Desc table.
     * @in pCudescTup, The HeapTuple struct.
     * @in DfsDescTupDesc, The TupleDesc struct.
     * @out DFSDesc, The DFSDesc struct.
     * @out columnAttr, the column to be got value, the column is min value column
     * or max value column. So value of the columnAttr only is MIN_COLUMN_ATTR or
     * MAX_COLUMN_ATTR.
     * @return
     */
    void setMinMaxByHeapTuple(HeapTuple pCudescTup, TupleDesc DfsDescTupDesc, DFSDesc* pDFSDesc, ColumnEnum columnAttr);

    /**
     * @Description: Set map value in current file from Desc table. The map has two
     * kind types, one is delete map, the other one is column map.
     * @in pCudescTup, The HeapTuple struct.
     * @in DfsDescTupDesc, The TupleDesc struct.
     * @out DFSDesc, The DFSDesc struct.
     * @out columnAttr, the column to be got value, the column is deletemap column
     * or colmap column. So value of the columnAttr only is DELETE_MAP_ATTR or
     * COLUMN_MAP_ATTR.
     * @return
     */
    void setMapColumnByHeapTuple(
        HeapTuple pCudescTup, TupleDesc DfsDescTupDesc, DFSDesc* pDFSDesc, ColumnEnum columnAttr);
    /*
     * dfs descriptor table id
     */
    Oid m_TableOid;

    /*
     * dfs descriptor index table id(the index by partition id and desc id)
     */
    Oid m_IndexOid;
    /*
     * The main relation.
     */
    Relation m_mainRel;

    /*
     * the column count of described table
     */
    uint32 m_SrcRelCols;

    /*
     * max batch rows
     */
    uint32 m_MaxBatCnt;

    /*the file id to set delmap*/
    int32 m_DelMapFileId;

    /*the delmap cache for a fileid*/
    char* m_pDelMap;

    /*the delmap cache size*/
    uint32 m_DelMapSize;
};

#endif
