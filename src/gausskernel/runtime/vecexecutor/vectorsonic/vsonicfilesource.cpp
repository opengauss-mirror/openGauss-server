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
 * vsonicfilesource.cpp
 * 	 Routines to handle vector sonic file source.
 * 		It provides both write and read inerfaces,
 * 		to save a SclarValue from a batch into temp files,
 * 		or to load data from temp files and assmenble into a batch.
 * 		Different from origional hash file source, each column (other than a partition)
 * 		will point to a seperate file.
 * 		Each SONIC data type has its own class to handle the read and write.
 *
 * IDENTIFICATION
 *     Code/src/gausskernel/runtime/vecexecutor/vectorsonic/vsonicfilesource.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "vectorsonic/vsonicpartition.h"
#include "storage/buf/buffile.h"
#include "storage/lz4_file.h"
#include <algorithm>

SonicHashFileSource::SonicHashFileSource(MemoryContext context)
    : m_context(context), m_cols(0), m_colWidth(0), m_nextColIdx(0), m_fileInfo(NULL), m_varSize(NULL)
{
    if (u_sess->attr.attr_sql.enable_compress_spill) {
        /* create temp file along with file buffer */
        m_file = (void*)LZ4FileCreate(false);
        /* set function pointers to handle a compress file */
        m_rewind = &SonicHashFileSource::rewindCompress;
        m_close = &SonicHashFileSource::closeCompress;
        m_writeTempFile = &SonicHashFileSource::writeCompress;
        m_readTempFile = &SonicHashFileSource::readCompress;
    } else {
        /* create temp file along with file buffer */
        m_file = (void*)BufFileCreateTemp(false);

        /* set function pointers to handle a non compress file */
        m_rewind = &SonicHashFileSource::rewindNoCompress;
        m_close = &SonicHashFileSource::closeNoCompress;
        m_writeTempFile = &SonicHashFileSource::writeNoCompress;
        m_readTempFile = &SonicHashFileSource::readNoCompress;
    }

    if (u_sess->attr.attr_sql.enable_sonic_optspill) {
        /* set function pointers to write and read a Scalarvalue */
        m_writeScalarValue = &SonicHashFileSource::writeScalarOpt;
        m_readScalarValue = &SonicHashFileSource::readScalarOpt;
    } else {
        /* set function pointers to write and read a Scalarvalue */
        m_writeScalarValue = &SonicHashFileSource::writeScalar;
        m_readScalarValue = &SonicHashFileSource::readScalar;
    }
}

SonicHashFileSource::SonicHashFileSource(MemoryContext context, DatumDesc* desc)
    : m_context(context), m_desc(*desc), m_cols(0), m_colWidth(0), m_nextColIdx(0), m_fileInfo(NULL), m_varSize(NULL)
{
    if (u_sess->attr.attr_sql.enable_compress_spill) {
        /* create temp file along with file buffer */
        m_file = (void*)LZ4FileCreate(false);

        /* set function pointers to handle a compress file */
        m_rewind = &SonicHashFileSource::rewindCompress;
        m_close = &SonicHashFileSource::closeCompress;
        m_writeTempFile = &SonicHashFileSource::writeCompress;
        m_readTempFile = &SonicHashFileSource::readCompress;
    } else {
        /* create temp file along with file buffer */
        m_file = (void*)BufFileCreateTemp(false);

        /* set function pointers to handle a non compress file */
        m_rewind = &SonicHashFileSource::rewindNoCompress;
        m_close = &SonicHashFileSource::closeNoCompress;
        m_writeTempFile = &SonicHashFileSource::writeNoCompress;
        m_readTempFile = &SonicHashFileSource::readNoCompress;
    }

    /* set function pointers to write and read a Scalarvalue */
    m_writeScalarValue = &SonicHashFileSource::writeScalar;
    m_readScalarValue = &SonicHashFileSource::readScalar;
}

/*
 * @Description: write value in scalar value into temp file
 * @in val - ScalarValue from a batch to put
 * @in flag - null flag to put
 * @return - bytes written
 */
size_t SonicHashFileSource::writeVal(ScalarValue* val, uint8 flag)
{
    return InvokeFp(m_writeScalarValue)(val, flag);
}

/*
 * @Description: read value from temp file and form as a ScalarValue
 * @out val - assembled ScalarValue read from temp file
 * @out flag - null flag of this val
 * @return - bytes read
 */
size_t SonicHashFileSource::readVal(ScalarValue* val, uint8* flag)
{
    return InvokeFp(m_readScalarValue)(val, flag);
}

/*
 * @Description: move to the head of the temp file
 * @return - void
 */
void SonicHashFileSource::rewind()
{
    InvokeFp(m_rewind)();
}

/*
 * @Description: close file handler
 * @return - void
 */
void SonicHashFileSource::close()
{
    InvokeFp(m_close)();
}

/*
 * @Description: prepare the buffer for read/write data from/into the temp file
 * @return - void
 *
 * 	Note:
 * 	1) In a LZ4File handler, the buffer in the handler consists of
 * 		srcBuf: LZ4FileSrcBufSize = BLCKSZ * 8 = 64KB
 * 		and
 * 		compressBuf: [0, n] KB,  n ~ 64 KB
 * 	To avoid memory being allocated but unused at present, we can
 * 	release the buffer.
 *
 * 	2) After run releaseFileHandlerBuffer(), the caller should call function
 * 	prepareFileHandlerBuffer() to re-allocate buffer before read/write data
 * 	from/into the temp file.
 */
void SonicHashFileSource::prepareFileHandlerBuffer()
{
    /* In no_compress mode, nothing to do */
    if (u_sess->attr.attr_sql.enable_compress_spill == false || m_file == NULL)
        return;

    /* We only prepare the buffer for compress mode */
    LZ4File* lz4_file = (LZ4File*)m_file;

    if (lz4_file != NULL) {
        MemoryContext old_cxt = MemoryContextSwitchTo(m_context);

        /* allocate the srcBuf and compressBuf */
        if (lz4_file->srcBuf == NULL) {
            lz4_file->srcBuf = (char*)palloc(LZ4FileSrcBufSize);
            lz4_file->srcDataSize = 0;
        }
        if (lz4_file->compressBuf == NULL) {
            lz4_file->compressBuf = (char*)palloc(LZ4FileSrcBufSize);
            lz4_file->compressBufSize = LZ4FileSrcBufSize;
        }

        (void)MemoryContextSwitchTo(old_cxt);
    }
}

/*
 * @Description: free the buffer in the file handler
 * @return - void
 */
void SonicHashFileSource::releaseFileHandlerBuffer()
{
    /* In no_compress mode, nothing to do */
    if (u_sess->attr.attr_sql.enable_compress_spill == false || m_file == NULL)
        return;

    /* We only release the buffer in compress mode */
    LZ4File* lz4_file = (LZ4File*)m_file;

    if (lz4_file != NULL) {
        MemoryContext old_cxt = MemoryContextSwitchTo(m_context);

        /* First, flush the data(in buffer), if any, into disk */
        LZ4FileClearBuffer(lz4_file);

        /* Then, free the buffer */
        if (lz4_file->srcBuf) {
            pfree(lz4_file->srcBuf);
            lz4_file->srcBuf = NULL;
        }
        if (lz4_file->compressBuf) {
            pfree(lz4_file->compressBuf);
            lz4_file->compressBuf = NULL;
            lz4_file->compressBufSize = 0;
        }

        (void)MemoryContextSwitchTo(old_cxt);
    }
}

/*
 * @Description: move to the head of the compressed file
 * @return - void
 */
void SonicHashFileSource::rewindCompress()
{
    /* move to the head of the file */
    LZ4FileRewind((LZ4File*)m_file);
}

/*
 * @Description: move to the head of the non-compressed file
 * @return - void
 */
void SonicHashFileSource::rewindNoCompress()
{
    /* move to the head of the file */
    if (BufFileSeek((BufFile*)m_file, 0, 0L, SEEK_SET)) {
        ereport(ERROR,
            (errcode_for_file_access(), errmsg("[SonicHash] could not rewind sonic hash-join temporary file: %m")));
    }
}

/*
 * @Description: close the compressed file
 * @return - void
 */
void SonicHashFileSource::closeCompress()
{
    if (m_file) {
        LZ4FileClose((LZ4File*)m_file);
    }
    m_file = NULL;
}

/*
 * @Description: close the non-compressed file
 * @return - void
 */
void SonicHashFileSource::closeNoCompress()
{
    if (m_file) {
        BufFileClose((BufFile*)m_file);
    }
    m_file = NULL;
}

/*
 * @Description: write data into temp file
 * @in data - data to put
 * @in size - size of data to put
 * @return - bytes written
 */
size_t SonicHashFileSource::write(void* data, size_t size)
{
    return InvokeFp(m_writeTempFile)(m_file, data, size);
}

/*
 * @Description: write data into compressed temp file
 * @in file - file to write
 * @in data - data to put
 * @in size - size of data to put
 * @return - bytes written
 */
size_t SonicHashFileSource::writeCompress(void* file, void* data, size_t size)
{
    return LZ4FileWrite((LZ4File*)file, (char*)data, size);
}

/*
 * @Description: write data into non-compressed temp file
 * @in file - file to write
 * @in data - data to put
 * @in size - size of data to put
 * @return - bytes written
 */
size_t SonicHashFileSource::writeNoCompress(void* file, void* data, size_t size)
{
    return BufFileWrite((BufFile*)file, data, size);
}

/*
 * @Description: read data from temp file
 * @out data - store the read data
 * @in size - size of data to put
 * @return - bytes read
 */
size_t SonicHashFileSource::read(void* data, size_t size)
{
    return InvokeFp(m_readTempFile)(m_file, data, size);
}

/*
 * @Description: read data from compressed temp file
 * @in file - file to read
 * @out data - store the read data
 * @in size - size of data to put
 * @return - bytes read
 */
size_t SonicHashFileSource::readCompress(void* file, void* data, size_t size)
{
    return LZ4FileRead((LZ4File*)file, (char*)data, size);
}

/*
 * @Description: read data from non-compressed temp file
 * @in file - file to read
 * @out data - store the read data
 * @in size - size of data to put
 * @return - bytes read
 */
size_t SonicHashFileSource::readNoCompress(void* file, void* data, size_t size)
{
    return BufFileRead((BufFile*)file, data, size);
}

/*
 * @Decription: write or read SONIC_INT_TYPE datum
 */
SonicHashIntFileSource::SonicHashIntFileSource(MemoryContext context, DatumDesc* desc, bool hasFlag)
    : SonicHashFileSource(context, desc), m_writeIntScalarValueFuncs(NULL), m_readIntScalarValueFuncs(NULL)
{
    /* set function pointers according to the bits of the int to be stored and loaded */
    switch (m_desc.typeSize) {
        case 1: {
            m_writeIntScalarValue = &SonicHashIntFileSource::write1ByteScalar;
            m_readIntScalarValue = &SonicHashIntFileSource::read1ByteScalar;
            break;
        }
        case 2: {
            m_writeIntScalarValue = &SonicHashIntFileSource::write2ByteScalar;
            m_readIntScalarValue = &SonicHashIntFileSource::read2ByteScalar;
            break;
        }
        case 4: {
            m_writeIntScalarValue = &SonicHashIntFileSource::write4ByteScalar;
            m_readIntScalarValue = &SonicHashIntFileSource::read4ByteScalar;
            break;
        }
        case 8: {
            m_writeIntScalarValue = &SonicHashIntFileSource::write8ByteScalar;
            m_readIntScalarValue = &SonicHashIntFileSource::read8ByteScalar;
            break;
        }
        default: {
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR),
                    errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("[SonicHash] Int has unknown type size of %d", m_desc.typeSize)));
            break;
        }
    }
}

/*
 * @Decription: write or read SONIC_INT_TYPE datum
 */
SonicHashIntFileSource::SonicHashIntFileSource(MemoryContext context, uint32 maxCols, bool hasFlag)
    : SonicHashFileSource(context)
{
    Assert(CurrentMemoryContext == m_context);
    m_writeIntScalarValueFuncs = (pWriteIntScalarValue*)palloc0(sizeof(pWriteIntScalarValue) * maxCols);
    m_readIntScalarValueFuncs = (pReadIntScalarValue*)palloc0(sizeof(pReadIntScalarValue) * maxCols);

    /* intfileInfo */
    m_fileInfo = (FileInfo*)palloc0(maxCols * sizeof(FileInfo));
    m_writeIntScalarValue = 0;
    m_readIntScalarValue = 0;
}

/*
 * @Decription: init m_fileInfo ,m_colWidth, m_cols.
 *    ready for multicolumns in one file.
 */
void SonicHashIntFileSource::init(DatumDesc* desc, uint16 fileIdx)
{
    /* set function pointers according to the bits of the int to be stored and loaded */
    switch (desc->typeSize) {
        case 1: {
            m_writeIntScalarValueFuncs[m_nextColIdx] = &SonicHashIntFileSource::write1ByteScalarOpt;
            m_readIntScalarValueFuncs[m_nextColIdx] = &SonicHashIntFileSource::read1ByteScalarOpt;
            break;
        }
        case 2: {
            m_writeIntScalarValueFuncs[m_nextColIdx] = &SonicHashIntFileSource::write2ByteScalarOpt;
            m_readIntScalarValueFuncs[m_nextColIdx] = &SonicHashIntFileSource::read2ByteScalarOpt;
            break;
        }
        case 4: {
            m_writeIntScalarValueFuncs[m_nextColIdx] = &SonicHashIntFileSource::write4ByteScalarOpt;
            m_readIntScalarValueFuncs[m_nextColIdx] = &SonicHashIntFileSource::read4ByteScalarOpt;
            break;
        }
        case 8: {
            m_writeIntScalarValueFuncs[m_nextColIdx] = &SonicHashIntFileSource::write8ByteScalarOpt;
            m_readIntScalarValueFuncs[m_nextColIdx] = &SonicHashIntFileSource::read8ByteScalarOpt;
            break;
        }
        default: {
            ereport(ERROR,
                (errmodule(MOD_VEC_EXECUTOR), errmsg("[SonicHash] Int has unknown type size of %d", m_desc.typeSize)));
            break;
        }
    }
    /* record funcidx */
    m_fileInfo[m_nextColIdx].partColIdx = fileIdx;
    m_fileInfo[m_nextColIdx].typeSize = desc->typeSize;
    m_nextColIdx++;
    m_colWidth += desc->typeSize;
    m_cols = m_nextColIdx;
}

/*
 * @Description: write ScalarValue info temp file.
 * 	Write null flag,
 * 	then write typeSize bytes of the varible.
 * @in val - ScalarValue from a batch to put
 * @in flag - null flag to put
 * @return- bytes written
 */
size_t SonicHashIntFileSource::writeScalar(ScalarValue* val, uint8 flag)
{
    return InvokeFp(m_writeIntScalarValue)(val, flag);
}

/*
 * @Description: write ScalarValue info temp file.
 * 	firstly, get the right intScalarValueFuncs,
 * 	then write type_size bytes of the varible.
 * @out val - assembled ScalarValue read from file
 * @out flag - null flag
 * @return - bytes read
 */
size_t SonicHashIntFileSource::writeScalarOpt(ScalarValue* val, uint8 flag)
{
    return RuntimeBinding(m_writeIntScalarValueFuncs, m_nextColIdx++ % m_cols)(val, flag);
}

/* @Description: write ScalarValue info temp file.
 * 	The ScalarValue stores 1 byte integer,
 * 	use GET_1_BYTE to get the varible.
 * @in val - ScalarValue from a batch to put
 * @in flag - null flag to put
 * @return - bytes written
 */
size_t SonicHashIntFileSource::write1ByteScalar(ScalarValue* val, uint8 flag)
{
    size_t written = 0;
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&flag, sizeof(uint8));

    if (NOT_NULL(flag)) {
        uint8 value = GET_1_BYTE(*val);
        written += InvokeFp(m_writeTempFile)(m_file, (void*)&value, int_typeSize_1Byte);
    }
    return written;
}

/*
 * @Description: the same function as write1ByteScalar.
 *  The only difference is whatever flag is null or not,
 *   write the value(which is not used in join).
 */
size_t SonicHashIntFileSource::write1ByteScalarOpt(ScalarValue* val, uint8 flag)
{
    size_t written = 0;
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&flag, sizeof(uint8));

    uint8 value = GET_1_BYTE(*val);
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&value, int_typeSize_1Byte);
    return written;
}

/*
 * @Description: write ScalarValue info temp file.
 * 	The ScalarValue stores 2 byte integer,
 * 	use GET_2_BYTES to get the varible.
 * @in val - ScalarValue from a batch to put
 * @in flag - null flag to put
 * @return - bytes written
 */
size_t SonicHashIntFileSource::write2ByteScalar(ScalarValue* val, uint8 flag)
{
    size_t written = 0;
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&flag, sizeof(uint8));

    if (NOT_NULL(flag)) {
        uint16 value = GET_2_BYTES(*val);
        written += InvokeFp(m_writeTempFile)(m_file, (void*)&value, int_typeSize_2Byte);
    }
    return written;
}

/*
 * @Description: the same function as write2ByteScalar.
 *  The only difference is whatever flag is null or not,
 *   write the value(which is not used in join).
 */
size_t SonicHashIntFileSource::write2ByteScalarOpt(ScalarValue* val, uint8 flag)
{
    size_t written = 0;
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&flag, sizeof(uint8));

    uint16 value = GET_2_BYTES(*val);
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&value, int_typeSize_2Byte);
    return written;
}

/*
 * @Description: write ScalarValue info temp file.
 * 	The ScalarValue stores 4 byte integer,
 * 	use GET_4_BYTES to get the varible.
 * @in val - ScalarValue from a batch to put
 * @in flag - null flag to put
 * @return - bytes written
 */
size_t SonicHashIntFileSource::write4ByteScalar(ScalarValue* val, uint8 flag)
{
    size_t written = 0;
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&flag, sizeof(uint8));

    if (NOT_NULL(flag)) {
        uint32 value = GET_4_BYTES(*val);
        written += InvokeFp(m_writeTempFile)(m_file, (void*)&value, int_typeSize_4Byte);
    }
    return written;
}

/*
 * @Description: the same function as write4ByteScalar.
 *  The only difference is whatever flag is null or not,
 *   write the value(which is not used in join).
 */
size_t SonicHashIntFileSource::write4ByteScalarOpt(ScalarValue* val, uint8 flag)
{
    size_t written = 0;
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&flag, sizeof(uint8));

    uint32 value = GET_4_BYTES(*val);
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&value, int_typeSize_4Byte);
    return written;
}

/*
 * @Description: write ScalarValue info temp file.
 * 	The ScalarValue stores 8 byte integer,
 * 	use GET_8_BYTES to get the varible.
 * @in val - ScalarValue from a batch to put
 * @in flag - null flag to put
 * @return - bytes written
 */
size_t SonicHashIntFileSource::write8ByteScalar(ScalarValue* val, uint8 flag)
{
    size_t written = 0;
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&flag, sizeof(uint8));

    if (NOT_NULL(flag)) {
        uint64 value = GET_8_BYTES(*val);
        written += InvokeFp(m_writeTempFile)(m_file, (void*)&value, int_typeSize_8Byte);
    }
    return written;
}

/*
 * @Description: the same function as write8ByteScalar.
 *  The only difference is whatever flag is null or not,
 *   write the value(which is not used in join).
 */
size_t SonicHashIntFileSource::write8ByteScalarOpt(ScalarValue* val, uint8 flag)
{
    size_t written = 0;
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&flag, sizeof(uint8));

    uint64 value = GET_8_BYTES(*val);
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&value, int_typeSize_8Byte);
    return written;
}

/*
 * @Description: read the data from temp file, and form as a ScalarValue.
 * 	Read null flag,
 * 	then read type_size bytes of the varible, and transfer into ScalarValue.
 * @out val - assembled ScalarValue read from file
 * @out flag - null flag
 * @return - bytes read
 */
size_t SonicHashIntFileSource::readScalar(ScalarValue* val, uint8* flag)
{
    return InvokeFp(m_readIntScalarValue)(val, flag);
}

/*
 * @Description: read the data from temp file, and form as a ScalarValue.
 * 	firstly, get the right intScalarValueFuncs,
 * 	then read type_size bytes of the varible, and transfer into ScalarValue.
 * @out val - assembled ScalarValue read from file
 * @out flag - null flag
 * @return - bytes read
 */
size_t SonicHashIntFileSource::readScalarOpt(ScalarValue* val, uint8* flag)
{
    return RuntimeBinding(m_readIntScalarValueFuncs, m_nextColIdx++ % m_cols)(val, flag);
}

/*
 * @Description: get ScalarValue from temp file.
 * 	The ScalarValue stores 1 byte integer,
 * 	use Int8GetDatum to transfer char stream into a ScalarValue.
 * @out val - assembled ScalarValue read from file
 * @out flag - null flag
 * @return - bytes read
 */
size_t SonicHashIntFileSource::read1ByteScalar(ScalarValue* val, uint8* flag)
{
    size_t nread = 0;
    uint8 tmp = 0;
    nread = readValue((char*)&tmp, flag);
    *val = Int8GetDatum(tmp);
    return nread;
}

/*
 * @Description: the same function as read1ByteScalar.
 *  The only difference is whatever flag is null or not,
 *   read a temp value(which is not used in join).
 */
size_t SonicHashIntFileSource::read1ByteScalarOpt(ScalarValue* val, uint8* flag)
{
    size_t nread = 0;
    uint8 tmp = 0;
    size_t has_read = 0;
    nread = InvokeFp(m_readTempFile)(m_file, (void*)flag, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    has_read = InvokeFp(m_readTempFile)(m_file, (void*)&tmp, int_typeSize_1Byte);
    CheckReadIsValid(has_read, int_typeSize_1Byte);
    nread += has_read;
    *val = Int8GetDatum(tmp);
    return nread;
}

/*
 * @Description: get ScalarValue from temp file.
 * 	The ScalarValue stores 2 byte integer,
 * 	use Int16GetDatum to transfer char stream into a ScalarValue.
 * @out val - assembled ScalarValue read from file
 * @out flag - null flag
 * @return - bytes read
 */
size_t SonicHashIntFileSource::read2ByteScalar(ScalarValue* val, uint8* flag)
{
    size_t nread = 0;
    uint16 tmp = 0;
    nread = readValue((char*)&tmp, flag);
    *val = Int16GetDatum(tmp);
    return nread;
}

/*
 * @Description: the same function as read2ByteScalar.
 *  The only difference is whatever flag is null or not,
 *   read a temp value(which is not used in join).
 */
size_t SonicHashIntFileSource::read2ByteScalarOpt(ScalarValue* val, uint8* flag)
{
    size_t nread = 0;
    uint16 tmp = 0;
    size_t has_read = 0;
    nread = InvokeFp(m_readTempFile)(m_file, (void*)flag, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    has_read = InvokeFp(m_readTempFile)(m_file, (void*)&tmp, int_typeSize_2Byte);
    CheckReadIsValid(has_read, int_typeSize_2Byte);
    nread += has_read;
    *val = Int16GetDatum(tmp);
    return nread;
}

/*
 * @Description: get ScalarValue from temp file.
 * 	The ScalarValue stores 4 byte integer,
 * 	use Int32GetDatum to transfer char stream into a ScalarValue.
 * @out val - assembled ScalarValue read from file
 * @out flag - null flag
 * @return - bytes read
 */
size_t SonicHashIntFileSource::read4ByteScalar(ScalarValue* val, uint8* flag)
{
    size_t nread = 0;
    uint32 tmp = 0;
    nread = readValue((char*)&tmp, flag);
    *val = Int32GetDatum(tmp);
    return nread;
}

/*
 * @Description: the same function as read4ByteScalar.
 *  The only difference is whatever flag is null or not,
 *   read a temp value(which is not used in join).
 */
size_t SonicHashIntFileSource::read4ByteScalarOpt(ScalarValue* val, uint8* flag)
{
    size_t nread = 0;
    uint32 tmp = 0;
    size_t has_read = 0;
    nread = InvokeFp(m_readTempFile)(m_file, (void*)flag, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    has_read = InvokeFp(m_readTempFile)(m_file, (void*)&tmp, int_typeSize_4Byte);
    CheckReadIsValid(has_read, int_typeSize_4Byte);
    nread += has_read;
    *val = Int32GetDatum(tmp);
    return nread;
}

/*
 * @Description: get ScalarValue from temp file.
 * 	The ScalarValue stores 84 byte integer,
 * 	use Int64GetDatum to transfer char stream into a ScalarValue.
 * @out val - assembled ScalarValue read from file
 * @out flag - null flag
 * @return - bytes read
 */
size_t SonicHashIntFileSource::read8ByteScalar(ScalarValue* val, uint8* flag)
{
    size_t nread = 0;
    uint64 tmp = 0;
    nread = readValue((char*)&tmp, flag);
    *val = Int64GetDatum(tmp);
    return nread;
}

/*
 * @Description: the same function as read8ByteScalar.
 *  The only difference is whatever flag is null or not,
 *   read a temp value(which is not used in join).
 */
size_t SonicHashIntFileSource::read8ByteScalarOpt(ScalarValue* val, uint8* flag)
{
    size_t nread = 0;
    uint64 tmp = 0;
    size_t has_read = 0;
    nread = InvokeFp(m_readTempFile)(m_file, (void*)flag, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    has_read = InvokeFp(m_readTempFile)(m_file, (void*)&tmp, int_typeSize_8Byte);
    CheckReadIsValid(has_read, int_typeSize_8Byte);
    nread += has_read;
    *val = Int64GetDatum(tmp);
    return nread;
}

/*
 * @Description: read integer stream from temp file.
 * @out val - data read from file
 * @out flag - null flag
 * @return - bytes read
 * 	Read null flag first if m_wirteFlag is true,
 * 	then read type_size bytes of the varible.
 * 	Note: m_writeFlag is set true when writing hash value only,
 * 		in this case, all the varible will not be null.
 */
size_t SonicHashIntFileSource::readValue(char* val, uint8* flag)
{
    size_t nread = 0;
    size_t has_read = 0;
    nread = InvokeFp(m_readTempFile)(m_file, (void*)flag, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    if (NOT_NULL(*flag)) {
        has_read = InvokeFp(m_readTempFile)(m_file, (void*)val, m_desc.typeSize);
        CheckReadIsValid(has_read, (size_t)(m_desc.typeSize));
        nread += has_read;
    }
    return nread;
}

/*
 * @Decription: write or read SONIC_NUMERIC_COMPRESS_TYPE datum
 */
SonicHashNumericFileSource::SonicHashNumericFileSource(MemoryContext context, DatumDesc* desc)
    : SonicHashFileSource(context, desc)
{
    Assert(CurrentMemoryContext == m_context);
    /* temp buffer to store one numeric read from temp file. */
    m_packBuf = (char*)palloc0(sizeof(n8pack));
    m_buf = NULL;
}

/*
 * @Decription: write or read SONIC_NUMERIC_COMPRESS_TYPE datum
 */
SonicHashNumericFileSource::SonicHashNumericFileSource(MemoryContext context, uint32 maxCols)
    : SonicHashFileSource(context)
{
    m_buf = NULL;

    Assert(CurrentMemoryContext == m_context);
    /* temp buffer to store one numeric read from temp file. */
    m_packBuf = (char*)palloc0(sizeof(n8pack));

    m_fileInfo = (FileInfo*)palloc0(maxCols * sizeof(FileInfo));
}

/*
 * @Description: set typesize for every SONIC_NUMERIC_COMPRESS_TYPE column
 */
void SonicHashNumericFileSource::init(DatumDesc* desc, uint16 fileIdx)
{
    m_fileInfo[m_nextColIdx].partColIdx = fileIdx;
    m_fileInfo[m_nextColIdx++].typeSize = desc->typeSize;
    m_colWidth += desc->typeSize;
    m_cols = m_nextColIdx;
}

/*
 * @Description: allocate or reset buffer for temporarily storing numeric,
 * @return: void.
 */
void SonicHashNumericFileSource::setBuffer()
{
    Assert(CurrentMemoryContext == m_context);

    if (m_buf == NULL) {
        /* buffer to store 128 numeric data from temp file. */
        m_buf = New(m_context) VarBuf(m_context);
    } else {
        m_buf->Reset();
    }
}

/*
 * @Description: write ScalarValue into the temp file
 * For 64 numeric, transfer the ScalarValue into nXpack,
 * then write the pack size, and the pack.
 * For 128 numeric, write the size of the the pointer,
 * then write the datum size, and the datum.
 * @in val - ScalarValue from a batch to put
 * @in flag - null flag to put
 * @return: bytes written.
 */
size_t SonicHashNumericFileSource::writeScalar(ScalarValue* val, uint8 flag)
{
    size_t written = 0;
    uint8 value_size = 0;

    written += InvokeFp(m_writeTempFile)(m_file, (void*)&flag, sizeof(uint8));

    if (IS_NULL(flag)) {
        return written;
    }

    if (!VARATT_IS_SHORT(*val) && NUMERIC_IS_BI64((Numeric)(*val))) {
        /* SclarValue is a BI64, so transfer it into nXpack before writing the nXpack */
        uint64 numeric_val = (uint64)NUMERIC_64VALUE((Numeric)*val);
        uint8 scale = NUMERIC_BI_SCALE((Numeric)*val);
        char* value = m_packBuf;
        if (numeric_val <= 0xFF) {
            ((n1pack*)value)->scale = scale;
            ((n1pack*)value)->val = (uint8)numeric_val;
            value_size = 2;
        } else if (numeric_val <= 0xFFFF) {
            ((n2pack*)value)->scale = scale;
            ((n2pack*)value)->val = (uint16)numeric_val;
            value_size = 3;
        } else if (numeric_val <= 0xFFFFFFFF) {
            ((n4pack*)value)->scale = scale;
            ((n4pack*)value)->val = (uint32)numeric_val;
            value_size = 5;
        } else {
            ((n8pack*)value)->scale = scale;
            ((n8pack*)value)->val = (uint64)numeric_val;
            value_size = 9;
        }
        written += InvokeFp(m_writeTempFile)(m_file, (void*)&value_size, sizeof(uint8));
        written += InvokeFp(m_writeTempFile)(m_file, (void*)value, value_size);
        /* record the size */
        *m_varSize += value_size;
    } else {
        /* the ScalarValue is a pointer,
         * so first write the size of a pointer,
         * then write the datum_size of the real datum and then the datum.
         */
        value_size = 8;
        written += InvokeFp(m_writeTempFile)(m_file, (void*)&value_size, sizeof(uint8));

        Datum datum = ScalarVector::Decode(*val);
        uint32 datum_size = VARSIZE_ANY(datum);
        written += InvokeFp(m_writeTempFile)(m_file, (void*)&datum_size, sizeof(uint32));
        written += InvokeFp(m_writeTempFile)(m_file, (void*)datum, datum_size);
        /* record the size */
        *m_varSize += (datum_size + 8);
    }
    return written;
}

/*
 * @Description: the same function as writeScalar.
 */
size_t SonicHashNumericFileSource::writeScalarOpt(ScalarValue* val, uint8 flag)
{
    return writeScalar(val, flag);
}

/*
 * @Description: read from temp file and form into a ScalarValue.
 * 	After read the null flag,
 * 	read the uint8 head which store the size that should read.
 * 	if the size is 8, means it should be form into a datum pointer first,
 * 	then we read the actual datum size, then read the actual datum and store into m_buf,
 * 	then we put the datum pointer into val.
 * 	If the size is 2, 3, 5, 9, means it should be form into a nXpack first,
 * 	then we read the size (2,3,5,9) of the data and put into m_packBuf,
 * 	then we make a 64 numeric and put the pointer into val.
 * @out val - assembled ScalarValue read from file
 * @out flag - null flag
 * @return - bytes read.
 */
size_t SonicHashNumericFileSource::readScalar(ScalarValue* val, uint8* flag)
{
    Assert(CurrentMemoryContext == m_context);
    size_t nread = 0;
    size_t read = 0;
    uint8 atom_size = 0;
    uint32 datum_size = 0;
    char* addr = NULL;

    nread = InvokeFp(m_readTempFile)(m_file, (void*)flag, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    if (NOT_NULL(*flag)) {
        read = InvokeFp(m_readTempFile)(m_file, (void*)&atom_size, sizeof(uint8));
        CheckReadIsValid(read, sizeof(uint8));
        nread += read;

        if (atom_size == 8) {
            /* It is a pointer, so get the real datum size before reading the real datum */
            read = InvokeFp(m_readTempFile)(m_file, (void*)&datum_size, sizeof(uint32));
            CheckReadIsValid(read, sizeof(uint32));
            nread += read;

            /* allocate space to store the real datum */
            addr = m_buf->Allocate(datum_size);
            /* load the real datum into the space */
            read = InvokeFp(m_readTempFile)(m_file, addr, datum_size);
            CheckReadIsValid(read, datum_size);
            nread += read;

            *val = PointerGetDatum(addr);
        } else {
            /* It is a nXpack, first read the size of the nXpack */
            read = InvokeFp(m_readTempFile)(m_file, (void*)m_packBuf, atom_size);
            CheckReadIsValid(read, atom_size);
            nread += read;

            /*
             * transfer the nXpack into NUMERIC_BI64
             * first allocate the space to store the real numeric scalar after transformation
             */
            addr = m_buf->Allocate(NUMERIC_64SZ);
            switch (atom_size) {
                case 2: {
                    n1pack* pack = (n1pack*)m_packBuf;
                    MAKE_NUMERIC64(addr, pack->val, pack->scale);
                    break;
                }
                case 3: {
                    n2pack* pack = (n2pack*)m_packBuf;
                    MAKE_NUMERIC64(addr, pack->val, pack->scale);
                    break;
                }
                case 5: {
                    n4pack* pack = (n4pack*)m_packBuf;
                    MAKE_NUMERIC64(addr, pack->val, pack->scale);
                    break;
                }
                case 9: {
                    n8pack* pack = (n8pack*)m_packBuf;
                    MAKE_NUMERIC64(addr, pack->val, pack->scale);
                    break;
                }
                default: {
                    ereport(ERROR,
                        (errmodule(MOD_VEC_EXECUTOR),
                            errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("[VecSonicHashJoin: reading numeric occurs error.]")));
                    break;
                }
            }
            *val = PointerGetDatum(addr);
        }
    } else {
        *val = 0;
    }

    return nread;
}

/*
 * @Description: the same function as readScalar.
 */
size_t SonicHashNumericFileSource::readScalarOpt(ScalarValue* val, uint8* flag)
{
    return readScalar(val, flag);
}

/*
 * @Decription: write or read SONIC_VAR_TYPE datum
 */
SonicHashEncodingFileSource::SonicHashEncodingFileSource(MemoryContext context, DatumDesc* desc)
    : SonicHashFileSource(context, desc)
{
    m_buf = NULL;
}

/*
 * @Decription: write or read SONIC_VAR_TYPE datum when u_sess->attr.attr_sql.enable_sonic_optspill is true.
 */
SonicHashEncodingFileSource::SonicHashEncodingFileSource(MemoryContext context, uint32 maxCols)
    : SonicHashFileSource(context)
{
    m_buf = NULL;

    Assert(CurrentMemoryContext == m_context);

    m_fileInfo = (FileInfo*)palloc0(maxCols * sizeof(FileInfo));
}

/*
 * @Description: set typesize for every SONIC_VAR_TYPE column
 */
void SonicHashEncodingFileSource::init(DatumDesc* desc, uint16 fileIdx)
{
    m_fileInfo[m_nextColIdx].partColIdx = fileIdx;
    m_fileInfo[m_nextColIdx++].typeSize = desc->typeSize;
    m_colWidth += desc->typeSize;
    m_cols = m_nextColIdx;
}

/*
 * @Description: allocate or reset buffer for temporarily storing datum,
 * @return - void.
 */
void SonicHashEncodingFileSource::setBuffer()
{
    if (m_buf == NULL) {
        /* buffer to store real data */
        m_buf = New(m_context) VarBuf(m_context);
    } else {
        m_buf->Reset();
    }
}

/*
 * @Description: write a ScalarValue info temp file.
 * 	Write the null flag,
 * 	and then write the actual datum size, then write the datum.
 * @in val - ScalarValue from a batch to put
 * @in flag - null flag to put
 * @return - bytes written
 */
size_t SonicHashEncodingFileSource::writeScalar(ScalarValue* val, uint8 flag)
{
    size_t written = 0;
    Datum datum;
    uint32 datum_size = 0;

    written += InvokeFp(m_writeTempFile)(m_file, (void*)&flag, sizeof(uint8));

    if (NOT_NULL(flag)) {
        /* Decode the ScalarValue to get the actual datum. */
        datum = ScalarVector::Decode(*val);
        datum_size = VARSIZE_ANY(datum);
        written += InvokeFp(m_writeTempFile)(m_file, (void*)&datum_size, sizeof(uint32));
        written += InvokeFp(m_writeTempFile)(m_file, (void*)datum, datum_size);
        /* record the size */
        *m_varSize += datum_size;
    }
    return written;
}

/*
 * @Description: the same function as writeScalar.
 */
size_t SonicHashEncodingFileSource::writeScalarOpt(ScalarValue* val, uint8 flag)
{
    return writeScalar(val, flag);
}

/*
 * @Description: read a datum from temp file and form into a ScalarValue.
 * @out val - assembled ScalarValue read from file
 * @out flag - null flag
 * @return - bytes read
 */
size_t SonicHashEncodingFileSource::readScalar(ScalarValue* val, uint8* flag)
{
    Assert(CurrentMemoryContext == m_context);
    size_t nread = 0;
    size_t read = 0;
    uint32 datum_size = 0;
    char* addr = NULL;

    nread = InvokeFp(m_readTempFile)(m_file, (void*)flag, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    if (NOT_NULL(*flag)) {
        /* get the real size of the datum */
        read = InvokeFp(m_readTempFile)(m_file, (void*)&datum_size, sizeof(uint32));
        CheckReadIsValid(read, sizeof(uint32));
        nread += read;

        /* allocate space to store the real datum */
        addr = m_buf->Allocate(datum_size);
        read = InvokeFp(m_readTempFile)(m_file, addr, datum_size);
        CheckReadIsValid(read, datum_size);
        nread += read;

        *val = PointerGetDatum(addr);
    } else {
        /* set any value */
        *val = 0;
    }

    return nread;
}

/*
 * @Description: the same function as readScalar.
 */
size_t SonicHashEncodingFileSource::readScalarOpt(ScalarValue* val, uint8* flag)
{
    return readScalar(val, flag);
}

/*
 * @Decription: write or read SONIC_CHAR_DIC_TYPE datum
 */
SonicHashCharFileSource::SonicHashCharFileSource(MemoryContext context, DatumDesc* desc)
    : SonicHashFileSource(context, desc)
{
    /*
     * record the actual header size.
     * 1 Byte header (remove symbolic bits, max is 127)
     * includes data length and headler length,
     * the max data length shoule be 126 bytes,
     * because the header length is 1 byte.
     */
    m_varheadlen = ((desc->typeSize + VARHDRSZ_SHORT) > VARATT_SHORT_MAX) ? VARHDRSZ : VARHDRSZ_SHORT;
    m_batchBuf = NULL;
    m_buf = NULL;
}

/*
 * @Decription: write or read SONIC_CHAR_DIC_TYPE datum
 */
SonicHashCharFileSource::SonicHashCharFileSource(MemoryContext context, int type_size, uint32 maxCols)
    : SonicHashFileSource(context)
{
    /* record the actual header size,do not need m_varheadlen. */
    m_varheadlen = 0;
    m_batchBuf = NULL;
    m_buf = NULL;

    Assert(CurrentMemoryContext == m_context);

    m_fileInfo = (FileInfo*)palloc0(maxCols * sizeof(FileInfo));
}

/*
 * @Description: set typesize for every SONIC_CHAR_DIC_TYPE column
 */
void SonicHashCharFileSource::init(DatumDesc* desc, uint16 fileIdx)
{
    m_fileInfo[m_nextColIdx].partColIdx = fileIdx;
    m_fileInfo[m_nextColIdx].typeSize = desc->typeSize;

    /* set varheadlen for every column */
    m_fileInfo[m_nextColIdx].varheadlen =
        ((desc->typeSize + VARHDRSZ_SHORT) > VARATT_SHORT_MAX) ? VARHDRSZ : VARHDRSZ_SHORT;
    m_nextColIdx++;
    m_colWidth += desc->typeSize;
    m_cols = m_nextColIdx;
}

/*
 * @Description: allocate or reset buffer for temporarily storing datum,
 * @return - void.
 */
void SonicHashCharFileSource::setBuffer()
{
    /* init the buffer to store a batch */
    if (m_batchBuf == NULL) {
        initBatchBuf();
    }
    /* reset the m_buf pointer */
    m_buf = m_batchBuf;
}

/*
 * @Description: write the ScalarValue into temp file,
 * 	First write null flag,
 * 	then write the size of the datum, then write the datum.
 * 	NOTE: the header can be a VARHDRSZ_SHORT or VARHDRSZ.
 * @in val - ScalarValue from a batch to put
 * @in flag - null flag to put
 * @return - bytes written.
 */
size_t SonicHashCharFileSource::writeScalar(ScalarValue* val, uint8 flag)
{
    char* value = NULL;
    size_t written = 0;
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&flag, sizeof(uint8));

    if (NOT_NULL(flag)) {
        /* do not store the datum header */
        if (VARATT_IS_SHORT(*val)) {
            value = (char*)(DatumGetPointer(*val) + VARHDRSZ_SHORT);
        } else {
            value = (char*)(DatumGetPointer(*val) + VARHDRSZ);
        }
        written += InvokeFp(m_writeTempFile)(m_file, (void*)value, m_desc.typeSize);
    }

    return written;
}

/*
 * @Description: write the ScalarValue into temp file,
 * 	First write null flag,
 * 	then write the size of the datum, then write the datum.
 * 	NOTE: the header can be a VARHDRSZ_SHORT or VARHDRSZ.
 * @in val - ScalarValue from a batch to put
 * @in flag - null flag to put
 * @return - bytes written.
 */
size_t SonicHashCharFileSource::writeScalarOpt(ScalarValue* val, uint8 flag)
{
    char* value = NULL;
    size_t written = 0;
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&flag, sizeof(uint8));

    int type_size = m_fileInfo[m_nextColIdx % m_cols].typeSize;
    if (NOT_NULL(flag)) {
        /* do not store the datum header */
        if (VARATT_IS_SHORT(*val)) {
            value = (char*)(DatumGetPointer(*val) + VARHDRSZ_SHORT);
        } else {
            value = (char*)(DatumGetPointer(*val) + VARHDRSZ);
        }
        written += InvokeFp(m_writeTempFile)(m_file, (void*)value, type_size);
    }
    m_nextColIdx++;

    return written;
}

/*
 * @Description: read a datum from temp file and form into a ScalarValue.
 * @out val - assembled ScalarValue read from file
 * @out flag - null flag
 * @return - bytes read.
 */
size_t SonicHashCharFileSource::readScalar(ScalarValue* val, uint8* flag)
{
    size_t nread = 0;
    size_t read = 0;
    nread = InvokeFp(m_readTempFile)(m_file, (void*)flag, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    if (NOT_NULL(*flag)) {
        read = InvokeFp(m_readTempFile)(m_file, (void*)(m_buf + m_varheadlen), m_desc.typeSize);
        CheckReadIsValid(read, (size_t)m_desc.typeSize);
        nread += read;

        *val = PointerGetDatum(m_buf);
        m_buf += m_desc.typeSize + m_varheadlen;
    } else {
        *val = 0;
    }
    return nread;
}

/*
 * @Description: read a datum from temp file and form into a ScalarValue.
 * @out val - assembled ScalarValue read from file
 * @out flag - null flag
 * @return - bytes read.
 */
size_t SonicHashCharFileSource::readScalarOpt(ScalarValue* val, uint8* flag)
{
    size_t nread = 0;
    size_t read = 0;
    nread = InvokeFp(m_readTempFile)(m_file, (void*)flag, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    FileInfo file_info = m_fileInfo[m_nextColIdx % m_cols];
    int type_size = file_info.typeSize;
    uint8 varheadlen = file_info.varheadlen;
    if (NOT_NULL(*flag)) {
        read = InvokeFp(m_readTempFile)(m_file, (void*)(m_buf + varheadlen), type_size);
        CheckReadIsValid(read, (size_t)type_size);
        nread += read;

        *val = PointerGetDatum(m_buf);
    } else {
        *val = 0;
    }
    m_buf += type_size + varheadlen;
    m_nextColIdx++;

    return nread;
}

/*
 * @Description: init the buffer for a batch.
 * 	set the header for every datum in the batch.
 */
void SonicHashCharFileSource::initBatchBuf()
{
    if (u_sess->attr.attr_sql.enable_sonic_optspill) {
        Assert(CurrentMemoryContext == m_context);

        int type_size = 0;
        for (uint32 i = 0; i < m_cols; i++) {
            type_size += m_fileInfo[i].typeSize + m_fileInfo[i].varheadlen;
        }
        Size totalSize = mul_size(BatchMaxSize, (Size)type_size);
        m_batchBuf = (char*)palloc0(totalSize);
        /* prepare the header info for the fixed data type. */
        m_buf = m_batchBuf;

        for (uint32 j = 0; j < BatchMaxSize; j++) {
            for (uint32 i = 0; i < m_cols; i++) {
                /*
                 * 1 Byte header (remove symbolic bits, max is 127)
                 * includes data length and headler length,
                 * the max data length shoule be 126 bytes,
                 * because the header length is 1 byte.
                 */
                if (m_fileInfo[i].typeSize + m_fileInfo[i].varheadlen > VARATT_SHORT_MAX) {
                    SET_VARSIZE(m_buf, m_fileInfo[i].typeSize + m_fileInfo[i].varheadlen);
                } else {
                    SET_VARSIZE_SHORT(m_buf, m_fileInfo[i].typeSize + m_fileInfo[i].varheadlen);
                }
                m_buf += m_fileInfo[i].typeSize + m_fileInfo[i].varheadlen;
            }
        }
    } else {
        Assert(CurrentMemoryContext == m_context);

        m_batchBuf = (char*)palloc0((m_desc.typeSize + m_varheadlen) * BatchMaxSize);
        m_buf = m_batchBuf;

        /* prepare the header info for the data type. */
        for (int i = 0; i < BatchMaxSize; i++) {
            /*
             * 1 Byte header (remove symbolic bits, max is 127)
             * includes data length and headler length,
             * the max data length shoule be 126 bytes,
             * because the header length is 1 byte.
             */
            if (m_desc.typeSize + m_varheadlen > VARATT_SHORT_MAX) {
                SET_VARSIZE(m_buf, m_desc.typeSize + m_varheadlen);
            } else {
                SET_VARSIZE_SHORT(m_buf, m_desc.typeSize + m_varheadlen);
            }
            m_buf += m_desc.typeSize + m_varheadlen;
        }
    }
}

/*
 * @Decription: write or read SONIC_FIXLEN_TYPE datum
 */
SonicHashFixLenFileSource::SonicHashFixLenFileSource(MemoryContext context, DatumDesc* desc)
    : SonicHashFileSource(context, desc)
{
    m_varheadlen = VARHDRSZ_SHORT;
    m_batchBuf = NULL;
    m_buf = NULL;
    m_tmpvalue = NULL;
}

/*
 * @Decription: write or read SONIC_FIXLEN_TYPE datum
 */
SonicHashFixLenFileSource::SonicHashFixLenFileSource(MemoryContext context, uint32 maxCols)
    : SonicHashFileSource(context)
{
    m_varheadlen = VARHDRSZ_SHORT;
    m_batchBuf = NULL;
    m_buf = NULL;

    Assert(CurrentMemoryContext == m_context);

    m_fileInfo = (FileInfo*)palloc0(maxCols * sizeof(FileInfo));

    /* 16 means max typeSize for fixlen type */
    m_tmpvalue = (char*)palloc0(maxCols * 16);
}

/*
 * @Description: set typesize for every SONIC_FIXLEN_TYPE column
 */
void SonicHashFixLenFileSource::init(DatumDesc* desc, uint16 fileIdx)
{
    Assert(CurrentMemoryContext == m_context);
    m_fileInfo[m_nextColIdx].partColIdx = fileIdx;
    m_fileInfo[m_nextColIdx++].typeSize = desc->typeSize;
    m_colWidth += desc->typeSize;
    m_cols = m_nextColIdx;
}

/*
 * @Description: allocate or reset buffer for temporarily storing datum,
 * @return - void.
 */
void SonicHashFixLenFileSource::setBuffer()
{
    /* init the buffer for a batch */
    if (m_batchBuf == NULL) {
        initBatchBuf();
    }
    m_buf = m_batchBuf;
}

/*
 * @Description: write a datum info temp file.
 * 	Write the null flag,
 * 	and then write the typeSize of datum.
 * @in val - ScalarValue from a batch to put
 * @in flag - null flag to put
 * @return - bytes written
 */
size_t SonicHashFixLenFileSource::writeScalar(ScalarValue* val, uint8 flag)
{
    size_t written = 0;
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&flag, sizeof(uint8));

    if (NOT_NULL(flag)) {
        written += InvokeFp(m_writeTempFile)(m_file, (void*)(DatumGetPointer(*val) + m_varheadlen), m_desc.typeSize);
    }
    return written;
}

/*
 * @Description: write a datum info temp file.
 *  Write the null flag, and then write the typeSize of datum.
 *  whether flag is null or not.
 * @in val - ScalarValue from a batch to put
 * @in flag - null flag to put
 * @return - bytes written
 */
size_t SonicHashFixLenFileSource::writeScalarOpt(ScalarValue* val, uint8 flag)
{
    size_t written = 0;
    int type_size = 0;
    written += InvokeFp(m_writeTempFile)(m_file, (void*)&flag, sizeof(uint8));

    type_size = m_fileInfo[m_nextColIdx % m_cols].typeSize;
    /* if flag is not null, pull m_tmpvalue(0) in */
    if (NOT_NULL(flag))
        written += InvokeFp(m_writeTempFile)(m_file, (void*)(DatumGetPointer(*val) + m_varheadlen), type_size);
    else {
        written += InvokeFp(m_writeTempFile)(m_file, (void*)(DatumGetPointer(m_tmpvalue)), type_size);
    }
    m_nextColIdx++;
    return written;
}

/*
 * @Description: read a datum from temp file and form as a ScalarValue.
 * 	Read the null flag,
 * 	and then read the type_size of datum.
 * @out val - assembled ScalarValue read from file
 * @out flag - null flag
 * @return - bytes read
 */
size_t SonicHashFixLenFileSource::readScalar(ScalarValue* val, uint8* flag)
{
    size_t nread = 0;
    size_t read = 0;
    nread = InvokeFp(m_readTempFile)(m_file, (void*)flag, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    if (NOT_NULL(*flag)) {
        read = InvokeFp(m_readTempFile)(m_file, (void*)(m_buf + m_varheadlen), m_desc.typeSize);
        CheckReadIsValid(read, (size_t)m_desc.typeSize);
        nread += read;

        *val = PointerGetDatum(m_buf);
        m_buf += m_desc.typeSize + m_varheadlen;
    }
    return nread;
}

/*
 * @Description: read a datum from temp file and form as a ScalarValue.
 *  Read the null flag, and then read the type_size of datum.
 *  whether flag is null or not.
 * @out val - assembled ScalarValue read from file
 * @out flag - null flag
 * @return - bytes read
 */
size_t SonicHashFixLenFileSource::readScalarOpt(ScalarValue* val, uint8* flag)
{
    size_t nread = 0;
    size_t read = 0;
    nread = InvokeFp(m_readTempFile)(m_file, (void*)flag, sizeof(uint8));
    CheckReadIsValid(nread, sizeof(uint8));

    /* whether flag is null or not, read it. */
    int type_size = m_fileInfo[m_nextColIdx % m_cols].typeSize;
    read = InvokeFp(m_readTempFile)(m_file, (void*)(m_buf + m_varheadlen), type_size);
    CheckReadIsValid(read, (size_t)type_size);
    nread += read;

    *val = PointerGetDatum(m_buf);
    m_buf += type_size + m_varheadlen;
    m_nextColIdx++;

    return nread;
}

/*
 * @Description: init the buffer for a batch.
 * 	set the header for every datum in the batch.
 */
void SonicHashFixLenFileSource::initBatchBuf()
{
    if (u_sess->attr.attr_sql.enable_sonic_optspill) {
        Assert(CurrentMemoryContext == m_context);

        int type_size = 0;
        for (uint32 i = 0; i < m_cols; i++) {
            type_size += m_fileInfo[i].typeSize + m_varheadlen;
        }
        m_batchBuf = (char*)palloc0(type_size * BatchMaxSize);
        /* prepare the header info for the fixed data type. */
        m_buf = m_batchBuf;

        for (uint32 j = 0; j < BatchMaxSize; j++) {
            for (uint32 i = 0; i < m_cols; i++) {
                SET_VARSIZE_SHORT(m_buf, m_fileInfo[i].typeSize + m_varheadlen);
                m_buf += m_fileInfo[i].typeSize + m_varheadlen;
            }
        }
    } else {
        Assert(CurrentMemoryContext == m_context);

        m_batchBuf = (char*)palloc0((m_desc.typeSize + m_varheadlen) * BatchMaxSize);
        m_buf = m_batchBuf;

        /* prepare the header info for the fixed data type. */
        for (int i = 0; i < BatchMaxSize; i++) {
            SET_VARSIZE_SHORT(m_buf, m_desc.typeSize + m_varheadlen);
            m_buf += m_desc.typeSize + m_varheadlen;
        }
    }
}
