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
 * lz4_file.cpp
 *        All interface of LZ4 compress and write file
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/file/lz4_file.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include <fcntl.h>

#include "access/xact.h"
#include "miscadmin.h"
#include "storage/smgr/fd.h"
#include "lz4.h"
#include "storage/lz4_file.h"
#include "utils/memutils.h" /* For MemoryContext stuff */
#include "executor/instrument.h"

#define COMPRESS_DATA_SIZE 8                 /* compressed data size  */
#define COMPRESS_FILESIZE 0x7FFFFFFFFFFFFFFF /* max file size */

/*
 * @Description:  create the temp file
 * @in interXact -  to mark the temp file will automatically deleted at end of transaction of not
 * @return -  file pointer
 */
LZ4File* LZ4FileCreate(bool interXact)
{
    LZ4File* lz4File = (LZ4File*)palloc(sizeof(LZ4File));
    lz4File->Reset();
    lz4File->srcBuf = (char*)palloc(LZ4FileSrcBufSize);

    lz4File->file = OpenTemporaryFile(interXact);
    return lz4File;
}

/*
 * @Description:  flush the data to file with lz4 compress
 * @in lz4File -  file pointer
 * @return - void
 */
static void LZ4FileFlush(LZ4File* lz4File)
{
    /* Compress src data with  LZ4Compres and write file  */
    int boundSize = LZ4_COMPRESSBOUND(lz4File->srcDataSize);
    if (boundSize > lz4File->compressBufSize - COMPRESS_DATA_SIZE) {
        if (lz4File->compressBufSize > 0)
            pfree(lz4File->compressBuf);
        lz4File->compressBuf = (char*)palloc((Size)(boundSize + COMPRESS_DATA_SIZE));
        lz4File->compressBufSize = boundSize + COMPRESS_DATA_SIZE;
    }

    /*
     *	 lz4File->compressBuf
     *	4 bytes,  compressed data size
     *	4 bytes,  src data size
     *	lz4File->compressBuf + 8,  compressed data
     */
    int outSize = LZ4_compress_default(lz4File->srcBuf,
                                       lz4File->compressBuf + COMPRESS_DATA_SIZE,
                                       lz4File->srcDataSize,
                                       LZ4_compressBound(lz4File->srcDataSize));

    *(int*)lz4File->compressBuf = outSize;

    *(int*)(lz4File->compressBuf + sizeof(int)) = lz4File->srcDataSize;

    /* If the file size exceeds the max size, do not write file any more and return an error */
    if (lz4File->curOffset > COMPRESS_FILESIZE - outSize - COMPRESS_DATA_SIZE) {
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not write to temporary file: the file size exceeds the max size: %ldBYTE",
                        COMPRESS_FILESIZE)));
    }

    int bytestowrite =
        FilePWrite(lz4File->file, lz4File->compressBuf, outSize + COMPRESS_DATA_SIZE, lz4File->curOffset);
    if (bytestowrite != outSize + COMPRESS_DATA_SIZE) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not write to temporary file: %m")));
    }
    u_sess->instr_cxt.pg_buffer_usage->temp_blks_written++;
    lz4File->curOffset += bytestowrite;
}

/*
 * @Description:  write data to file
 * @in lz4File -  file pointer
 * @in buffer -  the data to be written
 * @in size - data size
 * @return - written size
 */
size_t LZ4FileWrite(LZ4File* lz4File, char* buffer, size_t size)
{
    Assert(lz4File);
    int rc = EOK;
    size_t nwritten = 0;
    size_t nthistime;

    while (size > 0) {
        if (lz4File->srcDataSize >= LZ4FileSrcBufSize) {
            /* Buffer full, dump it out */
            LZ4FileFlush(lz4File);
            lz4File->srcDataSize = 0;
        }

        nthistime = (size_t)(LZ4FileSrcBufSize - lz4File->srcDataSize);
        if (nthistime > size)
            nthistime = size;
        Assert(nthistime > 0);

        rc = memcpy_s(lz4File->srcBuf + lz4File->srcDataSize, nthistime, buffer, nthistime);
        securec_check_ss(rc, "", "");

        lz4File->srcDataSize += (int)nthistime;

        buffer = buffer + nthistime;
        size -= nthistime;
        nwritten += nthistime;
    }
    return nwritten;
}

/*
 * @Description: read data from file
 * @in lz4File -  file pointer
 * @in buffer -  the read data to be storage
 * @in size - data size
 * @return - read size
 */
size_t LZ4FileRead(LZ4File* lz4File, char* buffer, size_t size)
{
    size_t nread = 0;
    size_t nthistime;

    while (size > 0) {
        /* srcBuf is end, we need read from file */
        if (lz4File->readOffset >= lz4File->srcDataSize) {
            int len[2];
            /* Try to load more data into buffer. */
            int nbytes = FilePRead(lz4File->file, (char*)len, COMPRESS_DATA_SIZE, lz4File->curOffset);
            /* no more data available */
            if (0 == nbytes)
                return nread;

            if (COMPRESS_DATA_SIZE != nbytes) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from temporary file: %m")));
            }

            lz4File->curOffset += COMPRESS_DATA_SIZE;
            int compressSize = len[0];
            int srcSize = len[1];

            if (lz4File->compressBufSize < compressSize) {
                lz4File->compressBuf = (char*)repalloc(lz4File->compressBuf, (Size)compressSize);
                lz4File->compressBufSize = compressSize;
            }

            if (FilePRead(lz4File->file, lz4File->compressBuf, compressSize, lz4File->curOffset) != compressSize) {
                ereport(ERROR, (errcode_for_file_access(), errmsg("could not read from temporary file: %m")));
            }

            int decompressedSize = LZ4_decompress_safe(lz4File->compressBuf, lz4File->srcBuf, compressSize, srcSize);
            if (decompressedSize != srcSize) {
                Assert(false);
            }

            lz4File->readOffset = 0;
            lz4File->srcDataSize = srcSize;

            u_sess->instr_cxt.pg_buffer_usage->temp_blks_read++;
            lz4File->curOffset += compressSize;
        }

        nthistime = (size_t)(lz4File->srcDataSize - lz4File->readOffset);
        if (nthistime > size)
            nthistime = size;
        Assert(nthistime > 0);

        int rc = EOK;
        rc = memcpy_s(buffer, nthistime, lz4File->srcBuf + lz4File->readOffset, nthistime);
        securec_check_ss(rc, "", "");

        lz4File->readOffset += (off_t)nthistime;
        buffer = buffer + nthistime;
        size -= nthistime;
        nread += nthistime;
    }

    return nread;
}

/*
 * @Description: close the temp file
 * @in lz4File -  file pointer
 * @return - void
 */
void LZ4FileClose(LZ4File* lz4File)
{
    if (lz4File->file > 0) {
        FileClose(lz4File->file);
        lz4File->file = FILE_INVALID;
        if (lz4File->srcBuf) {
            pfree(lz4File->srcBuf);
            lz4File->srcBuf = NULL;
        }
        if (lz4File->compressBuf) {
            pfree(lz4File->compressBuf);
            lz4File->compressBuf = NULL;
        }
    }

    pfree(lz4File);
}

/*
 * @Description: seek to the start of the file
 * @in lz4File -  file pointer
 * @return - void
 */
void LZ4FileRewind(LZ4File* lz4File)
{
    if (lz4File->srcDataSize > 0) {
        LZ4FileFlush(lz4File);
    }

    lz4File->readOffset = 0;
    lz4File->srcDataSize = 0;
    lz4File->curOffset = 0;
}

/*
 * @Description: just flush the buffer-data into disk
 * @in lz4File: file handler
 * @return: void
 *
 * Note:
 * 1) Differs from LZ4FileSeek():
 * 	After calling LZ4FileClearBuffer(), we can still write/read
 * 	from the last position of the file.
 *
 * 2) Aim: to prepare for release the buffer
 */
void LZ4FileClearBuffer(LZ4File* lz4File)
{
    Assert(lz4File != NULL);

    if (lz4File->srcDataSize > 0) {
        /* there are data in buffer, dump it out to disk */
        LZ4FileFlush(lz4File);
    }

    /* to keep the position untouched, do not reset the curOffset */
    lz4File->readOffset = 0;
    lz4File->srcDataSize = 0;
}

