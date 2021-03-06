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
 * lz4_file.h
 *        All interface of LZ4 compress and write file
 * 
 * IDENTIFICATION
 *        src/include/storage/lz4_file.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef LZ4FILE_H
#define LZ4FILE_H

#define LZ4FileSrcBufSize (BLCKSZ * 8)

typedef struct LZ4File {
    File file;
    off_t readOffset;
    int compressBufSize;
    int srcDataSize;
    char* srcBuf;      /*  a temporary buffer for writing data.  */
    char* compressBuf; /*  a temporary buffer for compress data.  */
    off_t curOffset;   /* next read/write position in buffer */

public:
    void Reset()
    {
        file = FILE_INVALID;
        readOffset = 0;
        srcDataSize = 0;
        srcBuf = NULL;
        compressBuf = NULL;
        compressBufSize = 0;
        curOffset = 0;
    }
} LZ4File;

extern LZ4File* LZ4FileCreate(bool interXact);
extern size_t LZ4FileWrite(LZ4File* lz4File, char* buffer, size_t size);
extern size_t LZ4FileRead(LZ4File* lz4File, char* buffer, size_t size);
extern void LZ4FileClose(LZ4File* lz4File);
extern void LZ4FileRewind(LZ4File* lz4File);
extern void LZ4FileClearBuffer(LZ4File* lz4File);

#endif
