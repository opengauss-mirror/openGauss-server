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
 * vsonicfilesource.h
 *     sonic file source class and class member declare
 * 
 * IDENTIFICATION
 *        src/include/vectorsonic/vsonicfilesource.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_VECTORSONIC_VSONICFILESOURCE_H_
#define SRC_INCLUDE_VECTORSONIC_VSONICFILESOURCE_H_

#include "postgres.h"
#include "knl/knl_variable.h"
#include "vectorsonic/vsonicarray.h"

#define CheckReadIsValid(nread, toread)                                                                                \
    if ((nread) != (toread)) {                                                                                         \
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from sonic hash-join temporary file: %m"))); \
    }

/* file info for every file type */
typedef struct FileInfo {
    /* record the original position in targetlist. */
    uint32 partColIdx;

    /* typeSize of current column in file */
    int typeSize;

    /* record the varheadlen of difference column in same file */
    uint8 varheadlen;
} FileInfo;

class SonicHashFileSource : public BaseObject {
public:
    SonicHashFileSource(MemoryContext context);

    SonicHashFileSource(MemoryContext context, DatumDesc* desc);

    virtual ~SonicHashFileSource()
    {}

    /* memory context */
    MemoryContext m_context;

    /* data description */
    DatumDesc m_desc;

    /* pointer of temp file */
    void* m_file;

    /* columns include */
    uint32 m_cols;

    /* total column width */
    size_t m_colWidth;

    /* colIdx in current file */
    uint32 m_nextColIdx;

    /* fileinfo include colIdx in targetlist and typeSize of current colIdx */
    FileInfo* m_fileInfo;

    /*
     * record the size of varible data which will not contained within the Atom's allocated space
     * when loading into memory, such as SONIC_NUMERIC_COMPRESS_TYPE and SONIC_VAR_TYPE,
     * whose data may stored in buffers (eg. VarBuf) other than Atom.
     * The Atom only holds the pointers of the data.
     */
    size_t* m_varSize;

    /* write and read functions to and from temp file*/
    size_t (SonicHashFileSource::*m_writeScalarValue)(ScalarValue* val, uint8 flag);

    size_t (SonicHashFileSource::*m_readScalarValue)(ScalarValue* val, uint8* flag);

    size_t (SonicHashFileSource::*m_readTempFile)(void* file, void* data, size_t size);

    size_t (SonicHashFileSource::*m_writeTempFile)(void* file, void* data, size_t size);

    /* rewind function */
    void (SonicHashFileSource::*m_rewind)();

    /* close function */
    void (SonicHashFileSource::*m_close)();

    void prepareFileHandlerBuffer();

    void releaseFileHandlerBuffer();

    size_t write(void* data, size_t size);

    size_t read(void* data, size_t size);

public:
    virtual void setBuffer()
    {
        return;
    }

    /* get typeSize in current column */
    size_t getTypeSize(int colIdx)
    {
        return (size_t)m_fileInfo[colIdx].typeSize;
    }

    /* m_cols means max column number in filesource */
    uint32 getMaxCols()
    {
        return m_cols;
    }

    size_t getColWidth()
    {
        return m_colWidth;
    }
    size_t writeVal(char* val, uint8 flag = V_NOTNULL_MASK);

    size_t writeVal(ScalarValue* val, uint8 flag);

    size_t readVal(char* val, uint8* flag = NULL);

    size_t readVal(ScalarValue* val, uint8* flag);

    void rewind();

    void close();

private:
    virtual size_t writeScalar(ScalarValue* val, uint8 flag)
    {
        return 0;
    }

    virtual size_t readScalar(ScalarValue* val, uint8* flag)
    {
        return 0;
    }

    virtual size_t writeScalarOpt(ScalarValue* val, uint8 flag)
    {
        return 0;
    }

    virtual size_t readScalarOpt(ScalarValue* val, uint8* flag)
    {
        return 0;
    }

    virtual void init(DatumDesc* desc, uint16 fileIdx)
    {
        Assert(false);
    }

    size_t writeCompress(void* file, void* data, size_t size);

    size_t writeNoCompress(void* file, void* data, size_t size);

    size_t readCompress(void* file, void* data, size_t size);

    size_t readNoCompress(void* file, void* data, size_t size);

    void rewindCompress();

    void rewindNoCompress();

    void closeCompress();

    void closeNoCompress();
};

class SonicHashIntFileSource : public SonicHashFileSource {

private:
    /* write function according to the integer bytes */
    size_t (SonicHashIntFileSource::*m_writeIntScalarValue)(ScalarValue* val, uint8 flag);

    /* read function according to the integer bytes */
    size_t (SonicHashIntFileSource::*m_readIntScalarValue)(ScalarValue* val, uint8* flag);

    /* write function according to the integer bytes */
    typedef size_t (SonicHashIntFileSource::*pWriteIntScalarValue)(ScalarValue* val, uint8 flag);

    /* read function according to the integer bytes */
    typedef size_t (SonicHashIntFileSource::*pReadIntScalarValue)(ScalarValue* val, uint8* flag);

    pWriteIntScalarValue* m_writeIntScalarValueFuncs;

    pReadIntScalarValue* m_readIntScalarValueFuncs;

public:
    SonicHashIntFileSource(MemoryContext context, DatumDesc* desc, bool hasFlag = true);

    ~SonicHashIntFileSource(){};

    SonicHashIntFileSource(MemoryContext context, uint32 maxCols, bool hasFlag = true);

    size_t writeScalar(ScalarValue* val, uint8 flag);

    size_t writeScalarOpt(ScalarValue* val, uint8 flag);

    size_t write1ByteScalar(ScalarValue* val, uint8 flag);

    size_t write1ByteScalarOpt(ScalarValue* val, uint8 flag);

    size_t write2ByteScalar(ScalarValue* val, uint8 flag);

    size_t write2ByteScalarOpt(ScalarValue* val, uint8 flag);

    size_t write4ByteScalar(ScalarValue* val, uint8 flag);

    size_t write4ByteScalarOpt(ScalarValue* val, uint8 flag);

    size_t write8ByteScalar(ScalarValue* val, uint8 flag);

    size_t write8ByteScalarOpt(ScalarValue* val, uint8 flag);

    size_t readScalar(ScalarValue* val, uint8* flag);

    size_t readScalarOpt(ScalarValue* val, uint8* flag);

    size_t read1ByteScalar(ScalarValue* val, uint8* flag);

    size_t read1ByteScalarOpt(ScalarValue* val, uint8* flag);

    size_t read2ByteScalar(ScalarValue* val, uint8* flag);

    size_t read2ByteScalarOpt(ScalarValue* val, uint8* flag);

    size_t read4ByteScalar(ScalarValue* val, uint8* flag);

    size_t read4ByteScalarOpt(ScalarValue* val, uint8* flag);

    size_t read8ByteScalar(ScalarValue* val, uint8* flag);

    size_t read8ByteScalarOpt(ScalarValue* val, uint8* flag);

    /* init typeSize and m_cols in SONIC_INT_TYPE file */
    void init(DatumDesc* desc, uint16 fileIdx);

private:
    size_t readValue(char* val, uint8* flag);
};

class SonicHashNumericFileSource : public SonicHashFileSource {
public:
    SonicHashNumericFileSource(MemoryContext context, DatumDesc* desc);

    SonicHashNumericFileSource(MemoryContext context, uint32 maxCols);

    ~SonicHashNumericFileSource(){};

    void init(DatumDesc* desc, uint16 fileIdx);

    void setBuffer();

    size_t writeScalar(ScalarValue* val, uint8 flag);

    size_t writeScalarOpt(ScalarValue* val, uint8 flag);

    size_t readScalar(ScalarValue* val, uint8* flag);

    size_t readScalarOpt(ScalarValue* val, uint8* flag);

private:
    /* temp buffer to store one numeric */
    char* m_packBuf;

    /* buffer to store 128 numeric data */
    VarBuf* m_buf;
};

class SonicHashEncodingFileSource : public SonicHashFileSource {
public:
    SonicHashEncodingFileSource(MemoryContext context, DatumDesc* desc);

    SonicHashEncodingFileSource(MemoryContext context, uint32 maxCols);

    ~SonicHashEncodingFileSource(){};

    void init(DatumDesc* desc, uint16 fileIdx);

    void setBuffer();

    size_t writeScalar(ScalarValue* val, uint8 flag);

    size_t writeScalarOpt(ScalarValue* val, uint8 flag);

    size_t readScalar(ScalarValue* val, uint8* flag);

    size_t readScalarOpt(ScalarValue* val, uint8* flag);

private:
    /* buffer to store real data */
    VarBuf* m_buf;
};

class SonicHashCharFileSource : public SonicHashFileSource {

private:
    /* temp pointer to buffer */
    char* m_buf;

    /* buffer to store a batch */
    char* m_batchBuf;

    /* actual header size */
    uint8 m_varheadlen;

public:
    SonicHashCharFileSource(MemoryContext context, DatumDesc* desc);

    SonicHashCharFileSource(MemoryContext context, int typeSize, uint32 maxCols);

    ~SonicHashCharFileSource(){};

    void init(DatumDesc* desc, uint16 fileIdx);

    void setBuffer();

    size_t writeScalar(ScalarValue* val, uint8 flag);

    size_t writeScalarOpt(ScalarValue* val, uint8 flag);

    size_t readScalar(ScalarValue* val, uint8* flag);

    size_t readScalarOpt(ScalarValue* val, uint8* flag);

private:
    void initBatchBuf();
};

class SonicHashFixLenFileSource : public SonicHashFileSource {

private:
    /* actual header size */
    uint8 m_varheadlen;

    /* buffer to store a batch */
    char* m_batchBuf;

    /* temp pointer to buffer */
    char* m_buf;

    char* m_tmpvalue;

public:
    SonicHashFixLenFileSource(MemoryContext context, DatumDesc* desc);

    ~SonicHashFixLenFileSource(){};

    SonicHashFixLenFileSource(MemoryContext context, uint32 maxCols);

    void init(DatumDesc* desc, uint16 fileIdx);

    void setBuffer();

    size_t writeScalar(ScalarValue* val, uint8 flag);

    size_t writeScalarOpt(ScalarValue* val, uint8 flag);

    size_t readScalar(ScalarValue* val, uint8* flag);

    size_t readScalarOpt(ScalarValue* val, uint8* flag);

private:
    void initBatchBuf();
};

#endif /* SRC_INCLUDE_VECTORSONIC_VSONICFILESOURCE_H_ */
