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
 * vsonicpartition.cpp
 * 		Routines to handle vector sonic hash partitions.
 * 		It includes a basic class named SonicHashPartition,
 * 		and two extened classes named SonicHashMemPartition and SonicHashFilePartition.
 * 		SonicHashPartition handles the basic interface such as freeing resources,
 * 		SonicHashMemPartition handles partition stored in the memory with SonicDatumArray.
 * 		SonicHashFilePartition handles partition stored in the disk with SonicHashFileSource.
 *
 * IDENTIFICATION
 *      Code/src/gausskernel/runtime/vecexecutor/vectorsonic/vsonicpartition.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "vectorsonic/vsonicpartition.h"
#include "storage/buf/buffile.h"
#include "storage/lz4_file.h"
#include "vectorsonic/vsonicnumeric.h"
#include "vectorsonic/vsonicint.h"
#include "vectorsonic/vsonicchar.h"
#include "vectorsonic/vsonicencodingchar.h"
#include "vectorsonic/vsonicfixlen.h"
#include <algorithm>

SonicHashPartition::SonicHashPartition(const char* cxtname, uint16 cols, int64 workMem) : m_cols(cols)
{
    m_rows = 0;
    m_size = 0;
    m_fileRecords = NULL;

    m_context = AllocSetContextCreate(CurrentMemoryContext,
        cxtname,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        STANDARD_CONTEXT,
        workMem);

    m_status = partitionStatusInitial;
}

SonicHashMemPartition::SonicHashMemPartition(const char* cxtname, bool hasHash, TupleDesc tupleDesc, int64 workMem)
    : SonicHashPartition(cxtname, tupleDesc->natts, workMem)
{
    MemoryContext old_ctx = MemoryContextSwitchTo(m_context);
    m_data = (SonicDatumArray**)palloc0(sizeof(SonicDatumArray*) * m_cols);
    m_segHashTable = false;
    m_hash = NULL;
    m_bucket = NULL;
    m_next = NULL;
    m_segBucket = NULL;
    m_segNext = NULL;
    m_hashSize = 0;
    m_bucketTypeSize = 0;
    m_mask = 0;

    if (hasHash) {
        /* init m_hash to store the hash value */
        DatumDesc* desc = (DatumDesc*)palloc0(sizeof(DatumDesc));
        getDataDesc(desc, 4, NULL, false);
        m_hash = New(m_context) SonicIntTemplateDatumArray<uint32>(m_context, INIT_DATUM_ARRAY_SIZE, false, desc);
    }

    (void)MemoryContextSwitchTo(old_ctx);

    m_status = partitionStatusMemory;
}

/*
 * @Description: init m_data according to desc per column
 * @return - void.
 */
void SonicHashMemPartition::init(uint16 colIdx, DatumDesc* desc)
{

    switch (desc->dataType) {
        case SONIC_INT_TYPE:
            m_data[colIdx] = AllocateIntArray(m_context, m_context, INIT_DATUM_ARRAY_SIZE, true, desc);
            break;

        case SONIC_NUMERIC_COMPRESS_TYPE:
            m_data[colIdx] = New(m_context) SonicNumericDatumArray(m_context, INIT_DATUM_ARRAY_SIZE, true, desc);
            break;

        case SONIC_VAR_TYPE:
            m_data[colIdx] = New(m_context) SonicStackEncodingDatumArray(m_context, INIT_DATUM_ARRAY_SIZE, true, desc);
            break;

        case SONIC_FIXLEN_TYPE:
            m_data[colIdx] = New(m_context) SonicFixLenDatumArray(m_context, INIT_DATUM_ARRAY_SIZE, true, desc);
            break;

        case SONIC_CHAR_DIC_TYPE:
            m_data[colIdx] = New(m_context) SonicCharDatumArray(m_context, INIT_DATUM_ARRAY_SIZE, true, desc);
            break;

        default:
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("[SonicHash] Unrecognize desc type %d with type oid %u", desc->dataType, desc->typeId)));
            /* keep compiler quiet */
            break;
    }
}

/*
 * @Description: put a bacth into SonicDatumArray
 * @in batch - batch to put
 */
void SonicHashMemPartition::putBatch(VectorBatch* batch)
{
    Assert(batch->IsValid());

    for (int col_idx = 0; col_idx < batch->m_cols; ++col_idx) {
        m_data[col_idx]->putArray(batch->m_arr[col_idx].m_vals, batch->m_arr[col_idx].m_flag, batch->m_rows);
    }
    m_rows += batch->m_rows;

    Assert(isValid());
}

/*
 * @Description: flush nrows in the atom to the disk
 * @in arrIdx - the arrIdx of this atom
 * @in partIdx - pointer to all the partition indexes,
 * 	each is a partition index a row belongs to
 * @in filePartitions - pointer to the file partitions to store the data
 * @in nrows - number of rows to flush
 */
void SonicHashMemPartition::flushPartition(int arrIdx, uint32* partIdx, SonicHashPartition** filePartitions, int nrows)
{
    CHECK_FOR_INTERRUPTS();
    size_t written = 0;
    int atom_idx = 0;
    if (u_sess->attr.attr_sql.enable_sonic_optspill) {
        if (unlikely(arrIdx == 0)) {
            ++atom_idx;
            ++partIdx;
        }

        /*
         * init flag and data destination in current atom.
         * m_tmpCurData and m_tmpflag increase progressively with the increase of atom_idx.
         */
        for (uint16 col_idx = 0; col_idx < m_cols; ++col_idx) {
            m_data[col_idx]->m_curData = m_data[col_idx]->m_arr[arrIdx]->data + atom_idx * m_data[col_idx]->m_atomTypeSize;
            m_data[col_idx]->m_curFlag = (uint8*)(m_data[col_idx]->m_arr[arrIdx]->nullFlag) + atom_idx;
        }

        for (; atom_idx < nrows; ++atom_idx, ++partIdx) {
            for (uint16 col_idx = 0; col_idx < m_cols; ++col_idx) {
                /*
                 * flush current row of elements in the atom into disk,
                 * the elements should flush by row, and load in the same way.
                 */
                written =
                    m_data[col_idx]->flushDatumFlagArrayByRow(arrIdx, partIdx, (void**)filePartitions, col_idx, atom_idx);
                pgstat_increase_session_spill_size(written);
            }
        }

        /* fix the m_rows of a file partition */
        int64 row_idx = 0;
        partIdx--;
        if (unlikely(arrIdx == 0)) {
            ++row_idx;
        }
        /* now, partIdx is in the last position */
        for (; row_idx < nrows; ++row_idx, --partIdx) {
            filePartitions[*partIdx]->m_rows += 1;
        }
    } else {
        for (uint16 col_idx = 0; col_idx < m_cols; ++col_idx) {
            /*
             * flush nrows of elements in the atom into disk,
             * each element will put into the file partition,
             * whose partition index is the row belonged to.
             */
            written = m_data[col_idx]->flushDatumFlagArray(arrIdx, partIdx, (void**)filePartitions, col_idx, nrows);
            pgstat_increase_session_spill_size(written);
        }

        /* fix the m_rows of a file partition */
        int64 row_idx = 0;
        if (unlikely(arrIdx == 0)) {
            partIdx++;
            row_idx++;
        }

        for (; row_idx < nrows; ++row_idx, ++partIdx) {
            filePartitions[*partIdx]->m_rows += 1;
        }
    }
}

/*
 * @Description: load data from the disk, and put the data into atom
 * @in partition - the partition which stores the data in the disk
 */
void SonicHashMemPartition::loadPartition(void* partition)
{
    SonicHashFilePartition* file_partition = (SonicHashFilePartition*)partition;
    Assert(file_partition->isValid());
    /*
     * There will be two restrictions during this stage:
     * First, the maxium of m_arrSize is 2^31,
     * so the total rows should be less than 2^31*2^14 = 2^45
     * Second, the maxium hash table size is 2^32,
     * so the total rows should be less than the number which is the maximun prime number under 2^32,
     * that is SONIC_MAX_ROWS.
     * Thus, the total rows should be less than SONIC_MAX_ROWS.
     */
    if ((m_rows + file_partition->m_rows) > SONIC_MAX_ROWS) {
        ereport(ERROR,
            (errmodule(MOD_VEC_EXECUTOR),
                errcode(ERRCODE_TOO_MANY_ROWS),
                errmsg("[SonicHash] The number of rows is greater than SONIC_MAX_ROWS %u, "
                       "which is not supported for hash table construction.",
                    (uint32)SONIC_MAX_ROWS)));
    }

    if (u_sess->attr.attr_sql.enable_sonic_optspill) {
        /*
         * init flag and data destination in current atom.
         * m_tmpCurData and m_tmpflag increase progressively with the increase of m_atomIdx
         */
        for (uint16 col_idx = 0; col_idx < file_partition->m_cols; ++col_idx) {
            m_data[col_idx]->m_curData =
                m_data[col_idx]->m_curAtom->data + m_data[col_idx]->m_atomIdx * m_data[col_idx]->m_atomTypeSize;
            m_data[col_idx]->m_curFlag =
                (uint8*)(m_data[col_idx]->m_curAtom->nullFlag) + m_data[col_idx]->m_atomIdx * sizeof(uint8);
        }
        for (int file_idx = 0; file_idx < SONIC_TOTAL_TYPES; ++file_idx) {
            CHECK_FOR_INTERRUPTS();
            SonicHashFileSource* file = file_partition->m_files[file_idx];
            if (file != NULL) {
                uint32 max_cols = file->getMaxCols();
                for (int row_idx = 0; row_idx < file_partition->m_rows; ++row_idx) {
                    for (uint32 col_idx = 0; col_idx < max_cols; ++col_idx) {
                        int partition_colIdx = file->m_fileInfo[col_idx].partColIdx;
                        m_data[partition_colIdx]->loadArrayByRow((void*)file_partition->m_files[file_idx]);
                    }
                }
            }
        }
    } else {

        for (int col_idx = 0; col_idx < m_cols; ++col_idx) {
            /* load the data in the col_idx-th file in file partition into the atom of the col_idx-th m_data */
            m_data[col_idx]->loadArray((void*)file_partition->m_files[col_idx], file_partition->m_rows);
        }
    }
    m_rows += file_partition->m_rows;

    Assert(isValid());
}

/*
 * @Description: load all hash values from the disk, and put the data into m_hash
 * @in partition - the partition which stores the hash values in the disk
 */
void SonicHashMemPartition::loadHash(void* partition)
{
    SonicHashFilePartition* file_partition = (SonicHashFilePartition*)partition;
    /*
     * hash values are stored in the (m_fileNum-1)-th file in the file partition,
     * so load the data in this file and put into the atom in m_hash
     */
    m_hash->loadIntValArray((void*)(file_partition->m_files[file_partition->m_fileNum - 1]), file_partition->m_rows);
}

/*
 * @Description: put hash values into SonicIntTemplateDatumArray
 * @in hashValue - pointer of hash values
 * @in nrows - rows to put
 */
void SonicHashMemPartition::putHash(uint32* hashValues, uint64 nrows)
{
    ((SonicIntTemplateDatumArray<uint32>*)m_hash)->putIntValArray(hashValues, nrows);
}

/*
 * @Description: free context, file buffer and close file handler
 */
void SonicHashMemPartition::freeResources()
{
    if (m_context != NULL) {
        /* reset the m_context */
        MemoryContextReset(m_context);
        /* Delete child context for m_hashContext */
        MemoryContextDelete(m_context);
        m_context = NULL;
    }
}

/* @Description: check if data is valid
 * @return - true if every m_data[column] has the same number of rows.
 */
bool SonicHashMemPartition::isValid()
{
    for (int col_idx = 0; col_idx < m_cols; ++col_idx) {
        if (m_data[col_idx]->getRows() != m_rows)
            return false;
    }
    return true;
}

SonicHashFilePartition::SonicHashFilePartition(const char* cxtname, bool hasHash, TupleDesc tupleDesc, int64 workMem)
    : SonicHashPartition(cxtname, tupleDesc->natts, workMem), m_varSize(0)
{
    if (u_sess->attr.attr_sql.enable_sonic_optspill)
        m_fileNum = hasHash ? (SONIC_TOTAL_TYPES + 1) : SONIC_TOTAL_TYPES;
    else
        m_fileNum = hasHash ? (m_cols + 1) : m_cols;

    MemoryContext old_cxt = MemoryContextSwitchTo(m_context);
    m_files = (SonicHashFileSource**)palloc0(sizeof(SonicHashFileSource*) * m_fileNum);
    m_fileRecords = (uint64*)palloc0(sizeof(uint64) * m_fileNum);
    m_filetype = (int*)palloc0(sizeof(int) * (m_cols + 1));

    if (hasHash) {
        init(m_fileNum - 1, NULL);
    }

    m_batch = New(m_context) VectorBatch(m_context, tupleDesc);

    m_status = partitionStatusFile;

    m_varSize = 0;

    (void)MemoryContextSwitchTo(old_cxt);
}

SonicHashFilePartition::SonicHashFilePartition(const char* cxtname, VectorBatch* out_batch, int64 workMem)
    : SonicHashPartition(cxtname, out_batch->m_cols, workMem)
{
    if (u_sess->attr.attr_sql.enable_sonic_optspill)
        m_fileNum = SONIC_TOTAL_TYPES;
    else
        m_fileNum = m_cols;

    MemoryContext old_cxt = MemoryContextSwitchTo(m_context);
    m_files = (SonicHashFileSource**)palloc0(sizeof(SonicHashFileSource*) * m_fileNum);
    m_fileRecords = (uint64*)palloc0(sizeof(uint64) * m_fileNum);

    m_batch = out_batch;

    m_status = partitionStatusFile;

    m_filetype = (int*)palloc0(sizeof(int) * (m_cols + 1));

    m_varSize = 0;

    (void)MemoryContextSwitchTo(old_cxt);
}

/*
 * @Description: init the temp file source according to desc,
 * @in fileIdx - the temp file index to be initialized.
 * @in desc - DatumDesc of the column to put in the temp file,
 * 	set NULL when to put hash value.
 * @return - void
 */
void SonicHashFilePartition::init(uint16 fileIdx, DatumDesc* desc)
{
    if (desc == NULL) {
        m_files[fileIdx] = New(m_context) SonicHashFileSource(m_context);
        return;
    }
    if (u_sess->attr.attr_sql.enable_sonic_optspill) {
        switch (desc->dataType) {
            case SONIC_INT_TYPE: {
                SonicHashIntFileSource* file_source = (m_files[SONIC_INT_TYPE] != NULL)
                                                         ? (SonicHashIntFileSource*)m_files[SONIC_INT_TYPE]
                                                         : New(m_context) SonicHashIntFileSource(m_context, m_cols);
                file_source->init(desc, fileIdx);
                m_files[SONIC_INT_TYPE] = file_source;
                m_filetype[fileIdx] = SONIC_INT_TYPE;
            } break;
            case SONIC_FIXLEN_TYPE: {
                SonicHashFixLenFileSource* file_source =
                    (m_files[SONIC_FIXLEN_TYPE] != NULL) ? (SonicHashFixLenFileSource*)m_files[SONIC_FIXLEN_TYPE]
                                                         : New(m_context) SonicHashFixLenFileSource(m_context, m_cols);

                file_source->init(desc, fileIdx);
                m_files[SONIC_FIXLEN_TYPE] = file_source;
                m_filetype[fileIdx] = SONIC_FIXLEN_TYPE;
            } break;
            case SONIC_CHAR_DIC_TYPE: {
                SonicHashCharFileSource* file_source =
                    (m_files[SONIC_CHAR_DIC_TYPE] != NULL)
                        ? (SonicHashCharFileSource*)m_files[SONIC_CHAR_DIC_TYPE]
                        : New(m_context) SonicHashCharFileSource(m_context, desc->typeSize, m_cols);

                file_source->init(desc, fileIdx);
                m_files[SONIC_CHAR_DIC_TYPE] = file_source;
                m_filetype[fileIdx] = SONIC_CHAR_DIC_TYPE;
            } break;
            case SONIC_VAR_TYPE: {
                SonicHashEncodingFileSource* file_source =
                    (m_files[SONIC_VAR_TYPE] != NULL) ? (SonicHashEncodingFileSource*)m_files[SONIC_VAR_TYPE]
                                                      : New(m_context) SonicHashEncodingFileSource(m_context, m_cols);

                file_source->init(desc, fileIdx);
                m_files[SONIC_VAR_TYPE] = file_source;
                m_filetype[fileIdx] = SONIC_VAR_TYPE;
                m_files[SONIC_VAR_TYPE]->m_varSize = &m_varSize;
            } break;
            case SONIC_NUMERIC_COMPRESS_TYPE: {
                SonicHashNumericFileSource* file_source =
                    (m_files[SONIC_NUMERIC_COMPRESS_TYPE] != NULL)
                        ? (SonicHashNumericFileSource*)m_files[SONIC_NUMERIC_COMPRESS_TYPE]
                        : New(m_context) SonicHashNumericFileSource(m_context, m_cols);

                file_source->init(desc, fileIdx);
                m_files[SONIC_NUMERIC_COMPRESS_TYPE] = file_source;
                m_filetype[fileIdx] = SONIC_NUMERIC_COMPRESS_TYPE;
                m_files[SONIC_NUMERIC_COMPRESS_TYPE]->m_varSize = &m_varSize;
            } break;
            default:
                Assert(false);
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errmsg("[SonicHash] Unrecognize desc type %d with type oid %u", desc->dataType, desc->typeId)));
                /* keep compiler quiet. */
                break;
        }
    } else {
        switch (desc->dataType) {
            case SONIC_INT_TYPE:
                m_files[fileIdx] = New(m_context) SonicHashIntFileSource(m_context, desc);
                break;

            case SONIC_NUMERIC_COMPRESS_TYPE:
                m_files[fileIdx] = New(m_context) SonicHashNumericFileSource(m_context, desc);
                m_files[fileIdx]->m_varSize = &m_varSize;
                break;

            case SONIC_VAR_TYPE:
                m_files[fileIdx] = New(m_context) SonicHashEncodingFileSource(m_context, desc);
                m_files[fileIdx]->m_varSize = &m_varSize;
                break;

            case SONIC_FIXLEN_TYPE:
                m_files[fileIdx] = New(m_context) SonicHashFixLenFileSource(m_context, desc);
                break;

            case SONIC_CHAR_DIC_TYPE:
                m_files[fileIdx] = New(m_context) SonicHashCharFileSource(m_context, desc);
                break;

            default:
                Assert(false);
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errcode(ERRCODE_WRONG_OBJECT_TYPE),
                        errmsg("[SonicHash] Unrecognize desc type %d with type oid %u", desc->dataType, desc->typeId)));
                /* keep compiler quiet. */
                break;
        }
    }
}

/*
 * @Description: read from temp files and assamble the data into batch.
 * @return - assambled batch.
 */
VectorBatch* SonicHashFilePartition::getBatch()
{
    CHECK_FOR_INTERRUPTS();

    if (m_rows == 0) {
        return NULL;
    }

    MemoryContext old_cxt = MemoryContextSwitchTo(m_context);

    uint16 col_idx = 0;
    int64 row_idx = 0;
    ScalarVector* vector = NULL;
    ScalarValue* val = NULL;
    uint8* flag = NULL;
    int64 batch_size = (BatchMaxSize > m_rows) ? m_rows : BatchMaxSize;

    m_batch->Reset();

    if (u_sess->attr.attr_sql.enable_sonic_optspill) {
        /*
         * read data to batch by file,
         * in order to prepare Buffer once in the start of read.
         */
        for (int file_idx = 0; file_idx < SONIC_TOTAL_TYPES; file_idx++) {
            SonicHashFileSource* file = m_files[file_idx];
            if (file != NULL) {
                row_idx = 0;
                uint16 max_cols = file->getMaxCols();
                file->setBuffer();
                while (row_idx < batch_size) {
                    for (col_idx = 0; col_idx < max_cols; ++col_idx) {
                        uint16 partition_colIdx = file->m_fileInfo[col_idx].partColIdx;
                        vector = &m_batch->m_arr[partition_colIdx];
                        val = vector->m_vals + row_idx;
                        flag = vector->m_flag + row_idx;
                        m_files[file_idx]->readVal(val, flag);
                    }

                    ++row_idx;
                }
            }
        }
    } else {
        /* read data to batch */
        
        for (col_idx = 0; col_idx < m_cols; ++col_idx) {
            row_idx = 0;
            /*
             * prepare the buffer to store the temporary data
             * such as the real data of a data pointer,
             * or header need to assmeble a ScalarValue
             */
            m_files[col_idx]->setBuffer();
            vector = &m_batch->m_arr[col_idx];
            val = vector->m_vals;
            flag = vector->m_flag;
            while (row_idx < batch_size) {
                m_files[col_idx]->readVal(val, flag);
                ++val;
                ++flag;
                ++row_idx;
            }
        }
    }

    m_rows -= batch_size;

    /* fix the row number of the batch */
    m_batch->FixRowCount(batch_size);

    (void)MemoryContextSwitchTo(old_cxt);
    return m_batch;
}

/*
 * @Description: put one hash value into the temp file.
 * @in hashVal - pointer of the hash value to put.
 * @in nrows - number of rows to put.
 */
void SonicHashFilePartition::putHash(uint32* hashValues, uint64 nrows)
{
    size_t written = m_files[m_fileNum - 1]->write((void*)hashValues, sizeof(uint32) * nrows);
    /* fix row number */
    m_fileRecords[m_fileNum - 1]++;
    /* record written size */
    m_size += written;
    pgstat_increase_session_spill_size(written);
}

/*
 * @Description: read the hash values stored from temp files.
 * @out hashValues - pointer to put the hash values.
 * @in nrows - number of rows to read.
 */
void SonicHashFilePartition::getHash(uint32* hashValues, uint64 nrows)
{
    Assert(hashValues != NULL);
    m_files[m_fileNum - 1]->read((void*)hashValues, sizeof(uint32) * nrows);
}

/*
 * @Description: rewind all the file pointers to the beginning in the partition.
 * @return - void.
 */
void SonicHashFilePartition::rewindFiles()
{
    if (m_files == NULL) {
        return;
    }

    for (int idx = 0; idx < m_fileNum; ++idx) {
        if (m_files[idx] != NULL)
            m_files[idx]->rewind();
    }
}

/*
 * @Description: rewind all the file handlers in the partition.
 * @return - void.
 */
void SonicHashFilePartition::closeFiles()
{
    if (m_files == NULL) {
        return;
    }

    for (int idx = 0; idx < m_fileNum; ++idx) {
        if (m_files[idx] != NULL)
            m_files[idx]->close();
    }
}

/*
 * @Description: allocate file buffers for all the files in the partition.
 * @return - void.
 */
void SonicHashFilePartition::prepareFileHandlerBuffer()
{
    if (m_files == NULL) {
        return;
    }

    for (int idx = 0; idx < m_fileNum; ++idx) {
        if (m_files[idx] != NULL)
            m_files[idx]->prepareFileHandlerBuffer();
    }
}

/*
 * @Description: free all the file buffers in the partition
 * @return - void
 *
 * 	Note:
 * 	After run releaseFileHandlerBuffer(), the caller should call function
 * 	prepareFileHandlerBuffer() to re-allocate buffer before read/write data
 * 	from/into temp files.
 */
void SonicHashFilePartition::releaseFileHandlerBuffer()
{
    if (m_files == NULL) {
        return;
    }

    for (int idx = 0; idx < m_fileNum; ++idx) {
        if (m_files[idx] != NULL)
            m_files[idx]->releaseFileHandlerBuffer();
    }
}

/*
 * @Description: free context, file buffer and close file handler
 */
void SonicHashFilePartition::freeResources()
{
    /* close file handler */
    ((SonicHashFilePartition*)this)->releaseFileHandlerBuffer();
    ((SonicHashFilePartition*)this)->closeFiles();

    if (m_context != NULL) {
        /* reset the m_context */
        MemoryContextReset(m_context);
        /* Delete child context for m_hashContext */
        MemoryContextDelete(m_context);
        m_context = NULL;
    }
}

/*
 * @Description: check if file is valid
 * @return - true if every m_file is not NULL.
 */
bool SonicHashFilePartition::isValid()
{
    if (u_sess->attr.attr_sql.enable_sonic_optspill) {
        for (int idx = 0; idx < SONIC_TOTAL_TYPES; ++idx) {
            SonicHashFileSource* file = m_files[idx];
            if (file != NULL && (int64)m_fileRecords[idx] != m_rows * file->m_cols)
                return false;
        }
    } else {
        for (int idx = 0; idx < m_fileNum; ++idx) {
            if ((m_files[idx] == NULL) || ((int64)m_fileRecords[idx] != m_rows))
                return false;
        }
    }
    return true;
}
