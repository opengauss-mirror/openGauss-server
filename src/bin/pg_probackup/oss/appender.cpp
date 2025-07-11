/*-------------------------------------------------------------------------
 *
 * appender.cpp: Appender used by Backup/Recovery manager.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "include/appender.h"
#include "include/buffer.h"
#include "workload/gscgroup.h"
#include "include/restore.h"
#include "common/fe_memutils.h"

void initFileAppender(FileAppender* appender, NO_VERSION_SEG_TYPE type, uint32 minFileNo, uint32 maxFileNo)
{
    appender->fileNo = maxFileNo;
    appender->minFileNo = minFileNo;
    appender->maxFileNo = maxFileNo;
    appender->currFileSize = 0;
    appender->filePtr = NULL;
    appender->currFileName = getAppendFileName(appender->baseFileName, appender->fileNo);
    appender->filePtr = openWriteBufferFile(appender->currFileName, "wb");
    ParallelFileAppenderSegHeader header;
    setSegHeaderVersion(&header, SEG_HEADER_PARALLEL_VERSION);
    SetSegHeaderType(&header, type);
    Assert(getSegHeaderVersion(&header) == SEG_HEADER_PARALLEL_VERSION);
    header.size = 0;
    header.permission = 0;
    header.filesize = 0;
    header.crc = 0;
    header.external_dir_num = 0;
    header.file_type = DEV_TYPE_INVALID;
    header.threadId = 0;
    WriteHeader(&header, appender);
}

void initSegDescriptor(FileAppenderSegDescriptor** segDesc)
{
    FileAppenderSegDescriptor* desc = (FileAppenderSegDescriptor*)palloc(sizeof(FileAppenderSegDescriptor));
    if (desc == NULL) {
        elog(ERROR, "Failed to allocate memory for seg descriptor.");
        return;
    }
    desc->header = (char *)palloc(SEG_HEADER_MAX_SIZE);
    if (desc->header == NULL) {
        elog(ERROR, "desc->header allocate failed: out of memory");
    }
    SetSegHeaderType(desc->header, FILE_APPEND_TYPE_UNKNOWN);
    setSegHeaderVersion(desc->header, SEG_HEADER_DEFAULT_VERSION);

    desc->header_offset = -1;
    desc->payload_offset = -1;
    desc->payload = NULL;
    for (int i = 0; i < MAX_BACKUP_THREAD; i++) {
        desc->inputFile[i] = NULL;
        desc->outputFile[i] = NULL;
        desc->crc[i] = 0;
    }
    *segDesc = desc;
}

void getSegDescriptor(FileAppenderSegDescriptor* desc, char** buffOffset, size_t* remainBuffLen,
                      BufferCxt* cxt, uint32 headerVersion, size_t segHdrLen)
{
    errno_t rc;
    *remainBuffLen = *remainBuffLen - getSegHeaderSize(headerVersion);

    /* The header may span across two buffs.
     * So, we cannot directly copy the header from the buffer.
     */
    if (likely(desc->header_offset == -1)) {
        rc = memcpy_s(desc->header, segHdrLen, *buffOffset, segHdrLen);
        securec_check(rc, "\0", "\0");
        desc->payload = *buffOffset + segHdrLen;
        *buffOffset = desc->payload;
    } else {
        rc = memcpy_s(desc->header, desc->header_offset, *buffOffset, desc->header_offset);
        securec_check(rc, "\0", "\0");
        rc = memcpy_s(desc->header + desc->header_offset, (segHdrLen - desc->header_offset),
                      cxt->bufData, (segHdrLen - desc->header_offset));
        securec_check(rc, "\0", "\0");
        desc->payload = cxt->bufData + (segHdrLen - desc->header_offset);
        *buffOffset = desc->payload;
        desc->header_offset = -1;
    }
}

void parseSegDescriptor(FileAppenderSegDescriptor* desc, char** buffOffset, size_t* remainBuffLen, char* tempBuffer,
                        BufferCxt* cxt, pgBackup* dest_backup, bool isValidate, validate_files_arg* arg) {
    error_t rc = 0;
    parray* files = dest_backup->files;
    uint32 payloadSize = ((FileAppenderSegHeader*)(desc->header))->size;
    *remainBuffLen = *remainBuffLen - payloadSize;
    uint32 version = desc->version;
    uint32 fileIndex = 0;
    if (version >= SEG_HEADER_PARALLEL_VERSION) {
        fileIndex = ((ParallelFileAppenderSegHeader*)(desc->header))->threadId;
    }

    FileAppenderSegHeader* header = (FileAppenderSegHeader*)(desc->header);
    Assert(desc->version == getSegHeaderVersion(desc->header));
    if (desc->version != getSegHeaderVersion(desc->header)) {
        elog(ERROR, "Check segheader version failed, expect version: %d, segheader version: %d."
            "seg header info: type: %u, size: %u, permission: %u, filesize: %u, crc: %u, "
            "external_dir_num: %u, file_type: %u.",
            desc->version, getSegHeaderVersion(desc->header), GetSegHeaderType(desc->header), header->size,
            header->permission, header->filesize, header->crc, header->external_dir_num, header->file_type);
    }

    if (desc->payload_offset == -1) {
        rc = memcpy_s(tempBuffer, payloadSize, desc->payload, payloadSize);
        securec_check(rc, "\0", "\0");
        *buffOffset = desc->payload + payloadSize;
    } else {
        if (desc->payload_offset > 0) {
            rc = memcpy_s(tempBuffer, desc->payload_offset, desc->payload, desc->payload_offset);
            securec_check(rc, "\0", "\0");
        }
        rc = memcpy_s(tempBuffer + desc->payload_offset, payloadSize - desc->payload_offset,
                      cxt->bufData, (payloadSize - desc->payload_offset));
        securec_check(rc, "\0", "\0");
        *buffOffset = cxt->bufData + (payloadSize - desc->payload_offset);
        desc->payload_offset = -1;
    }
    if (GetSegHeaderType(desc->header) == FILE_APPEND_TYPE_FILES_END ||
        GetSegHeaderType(desc->header) == FILE_APPEND_TYPE_FILES) {
        return;
    } else if (GetSegHeaderType(desc->header) == FILE_APPEND_TYPE_DIR) {
        restoreDir(tempBuffer, desc, dest_backup, files, isValidate);
    } else if (GetSegHeaderType(desc->header) == FILE_APPEND_TYPE_FILE) {
        openRestoreFile(tempBuffer, desc, dest_backup, files, isValidate, arg);
    } else if (GetSegHeaderType(desc->header) == FILE_APPEND_TYPE_FILE_CONTENT) {
        writeOrValidateRestoreFile(tempBuffer, desc, isValidate, arg);
    } else if (GetSegHeaderType(desc->header) == FILE_APPEND_TYPE_FILE_END) {
        closeRestoreFile(desc);
    } else {
        if (isValidate) {
            arg->corrupted = true;
        } else {
            elog(ERROR, "Unknown file type: %d, when restore file: %s",
                GetSegHeaderType(desc->header), desc->inputFile[fileIndex]->rel_path);
        }
    }
}

void destorySegDescriptor(FileAppenderSegDescriptor** descriptor)
{
    FileAppenderSegDescriptor* desc = *descriptor;
    pfree_ext(desc->header);
    pfree_ext(desc);
}

void closeFileAppender(FileAppender* appender)
{
    if (!appender) {
        return;
    }
    ParallelFileAppenderSegHeader header;
    setSegHeaderVersion(&header, SEG_HEADER_PARALLEL_VERSION);
    SetSegHeaderType(&header, FILE_APPEND_TYPE_FILES_END);
    header.size = 0;
    header.permission = 0;
    header.filesize = 0;
    header.crc = 0;
    header.external_dir_num = 0;
    header.file_type = DEV_TYPE_INVALID;
    header.threadId = 0;
    ((BufferCxt *)appender->filePtr)->fileEnd.store(true);
    WriteHeader(&header, appender);
}

void destoryFileAppender(FileAppender** retAppender)
{
    FileAppender* appender = *retAppender;
    if (appender != NULL) {
        if (appender->baseFileName != NULL) {
            pfree_ext(appender->baseFileName);
        }
        if (appender->currFileName != NULL) {
            pfree_ext(appender->currFileName);
        }
        pfree_ext(appender);
        appender = NULL;
    }
}

char* getAppendFileName(const char* baseFileName, uint32 fileNo)
{
    char* fileName = NULL;
    if (baseFileName == NULL) {
        return NULL;
    }
    size_t nameLen = strlen(baseFileName) + APPEND_FILENAME_END_SIZE + APPEND_FILENAME_END_DIGIT;
    fileName = (char*)palloc(nameLen);
    if (fileName == NULL) {
        elog(ERROR, "Failed to allocate memory for file name");
    }
    errno_t rc = snprintf_s(fileName, nameLen, (nameLen - 1), "%s/file-%u.pbk", baseFileName, fileNo);
    securec_check_ss_c(rc, "\0", "\0");
    return fileName;
}

/* no used */
void constructHeader(FileAppenderSegHeader* header, FILE_APPEND_SEG_TYPE type,
                     uint32 size, off_t filesize, pgFile* file)
{
    header->type = type;
    header->size = size;
    header->permission = file->mode;
    header->filesize = filesize;
    header->crc = file->crc;
    header->external_dir_num = file->external_dir_num;
    header->file_type = file->type;
}

void constructParallelHeader(ParallelFileAppenderSegHeader* header, NO_VERSION_SEG_TYPE type,
                             uint32 size, off_t filesize, pgFile* file, short readerIndexId)
{
    setSegHeaderVersion(header, SEG_HEADER_PARALLEL_VERSION);
    SetSegHeaderType(header, type);
    header->size = size;
    header->permission = file->mode;
    header->filesize = filesize;
    header->crc = file->crc;
    header->external_dir_num = file->external_dir_num;
    header->file_type = file->type;
    header->threadId = readerIndexId;
}

void WriteHeader(ParallelFileAppenderSegHeader* header, FileAppender* appender)
{
    size_t writeLen = 0;

    /* only header with 0 size can be written alone, or use WriteDataBlock */
    Assert(header->size == 0);
    Assert(GetSegHeaderType(header) < FILE_APPEND_TYPE_MAX);
    Assert(getSegHeaderVersion(header) == SEG_HEADER_PARALLEL_VERSION);

    if (getSegHeaderVersion(header) != SEG_HEADER_PARALLEL_VERSION) {
        elog(ERROR, "WriteHeader check segheader version failed, expect version: %d, segheader version: %d."
            "seg header info: type: %u, size: %u, permission: %u, filesize: %u, crc: %u, "
            "external_dir_num: %u, file_type: %u.",
            SEG_HEADER_PARALLEL_VERSION, getSegHeaderVersion(header), GetSegHeaderType(header), header->size,
            header->permission, header->filesize, header->crc, header->external_dir_num, header->file_type);
    }

    if (appender == NULL || ((appender->currFileSize + PARALLEL_APPEND_FILE_HEADER_SIZE) > APPEND_FILE_MAX_SIZE)) {
        elog(ERROR, "Write header failed.");
    }

    if (GetSegHeaderType(header) != FILE_APPEND_TYPE_FILES &&
        GetSegHeaderType(header) != FILE_APPEND_TYPE_FILES_END) {
        pthread_spin_lock(&appender->lock);
    }
    if (GetSegHeaderType(header) != FILE_APPEND_TYPE_FILES_END &&
        (appender->currFileSize + PARALLEL_APPEND_FILE_HEADER_SIZE + header->size) >
        (APPEND_FILE_MAX_SIZE - PARALLEL_APPEND_FILE_HEADER_SIZE)) {
        uint32 minFileNo = appender->minFileNo;
        uint32 maxFileNo = appender->maxFileNo;
        closeFileAppender(appender);
        initFileAppender(appender, FILE_APPEND_TYPE_FILES, minFileNo, maxFileNo + 1);
    }
    /* filePtr is a buffer context */
    writeLen = writeToCompFile((char*)header, PARALLEL_APPEND_FILE_HEADER_SIZE, appender->filePtr);
    if (writeLen != PARALLEL_APPEND_FILE_HEADER_SIZE) {
        elog(ERROR, "Write header failed, write length: %lu, except length: %lu.",
            writeLen, PARALLEL_APPEND_FILE_HEADER_SIZE);
    }
    appender->currFileSize += writeLen;

    if (GetSegHeaderType(header) != FILE_APPEND_TYPE_FILES &&
        GetSegHeaderType(header) != FILE_APPEND_TYPE_FILES_END) {
        pthread_spin_unlock(&appender->lock);
    }
}

size_t writeToCompFile(const char* data, size_t len, void* file)
{
    if (writeToBuffer(data, len, file) != len) {
        return 0;
    }
    return len;
}

/* write header and payload */
void WriteDataBlock(ParallelFileAppenderSegHeader* header, const char* payload, size_t payloadSize,
                    FileAppender* appender)
{
    size_t writeLen = 0;
    size_t blockSize = PARALLEL_APPEND_FILE_HEADER_SIZE + payloadSize;
    errno_t rc;

    Assert(GetSegHeaderType(header) < FILE_APPEND_TYPE_MAX);
    Assert(getSegHeaderVersion(header) == SEG_HEADER_PARALLEL_VERSION);
    if (getSegHeaderVersion(header) != SEG_HEADER_PARALLEL_VERSION) {
        elog(ERROR, "WriteDataBlock check segheader version failed, expect version: %d, segheader version: %d."
            "seg header info: type: %u, size: %u, permission: %u, filesize: %u, crc: %u, "
            "external_dir_num: %u, file_type: %u.",
            SEG_HEADER_PARALLEL_VERSION, getSegHeaderVersion(header), GetSegHeaderType(header), header->size,
            header->permission, header->filesize, header->crc, header->external_dir_num, header->file_type);
    }

    if (appender == NULL || (appender->currFileSize + PARALLEL_APPEND_FILE_HEADER_SIZE) > APPEND_FILE_MAX_SIZE) {
        elog(ERROR, "Write header failed.");
    }

    /* construct the buffer to write */
    char* buffer = (char*)palloc(blockSize);
    if (buffer == NULL) {
        elog(ERROR, "Failed to allocate memory for buffer, size: %d", blockSize);
    }
    rc = memcpy_s(buffer, blockSize, header, PARALLEL_APPEND_FILE_HEADER_SIZE);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(buffer + PARALLEL_APPEND_FILE_HEADER_SIZE, blockSize - PARALLEL_APPEND_FILE_HEADER_SIZE,
                  payload, payloadSize);
    securec_check(rc, "\0", "\0");

    pthread_spin_lock(&appender->lock);
    if (GetSegHeaderType(header) != FILE_APPEND_TYPE_FILES_END &&
        appender->currFileSize + blockSize > APPEND_FILE_MAX_SIZE - PARALLEL_APPEND_FILE_HEADER_SIZE) {
        uint32 minFileNo = appender->minFileNo;
        uint32 maxFileNo = appender->maxFileNo;
        closeFileAppender(appender);
        initFileAppender(appender, FILE_APPEND_TYPE_FILES, minFileNo, maxFileNo + 1);
    }

    /* filePtr is a buffer context */
    writeLen = writeToCompFile(buffer, blockSize, appender->filePtr);
    if (writeLen != blockSize) {
        pthread_spin_unlock(&appender->lock);
        pfree_ext(buffer);
        elog(ERROR, "Write data block failed, write length: %lu, expect length: %lu.", writeLen, blockSize);
    }
    appender->currFileSize += writeLen;
    pthread_spin_unlock(&appender->lock);
    pfree_ext(buffer);
}