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

void initFileAppender(FileAppender* appender, FILE_APPEND_SEG_TYPE type, uint32 minFileNo, uint32 maxFileNo)
{
    appender->fileNo = maxFileNo;
    appender->minFileNo = minFileNo;
    appender->maxFileNo = maxFileNo;
    appender->type = type;
    appender->currFileSize = 0;
    appender->filePtr = NULL;
    appender->currFileName = getAppendFileName(appender->baseFileName, appender->fileNo);
    appender->filePtr = openWriteBufferFile(appender->currFileName, "wb");
    FileAppenderSegHeader header;
    header.type = type;
    header.size = 0;
    header.permission = 0;
    header.filesize = 0;
    header.crc = 0;
    header.external_dir_num = 0;
    header.file_type = DEV_TYPE_INVALID;
    writeHeader(&header,appender);
}

void initSegDescriptor(FileAppenderSegDescriptor** segDesc)
{
    FileAppenderSegDescriptor* desc = (FileAppenderSegDescriptor*)palloc(sizeof(FileAppenderSegDescriptor));
    if (desc == NULL) {
        elog(ERROR, "Failed to allocate memory for seg descriptor.");
        return;
    }
    desc->header.type = FILE_APPEND_TYPE_UNKNOWN;
    desc->header_offset = -1;
    desc->payload_offset = -1;
    desc->crc = 0;
    desc->payload = NULL;
    desc->outputFile = NULL;
    desc->inputFile = NULL;
    *segDesc = desc;
}

void getSegDescriptor(FileAppenderSegDescriptor* desc, char** buffOffset, size_t* remainBuffLen, BufferCxt* cxt)
{
    errno_t rc;
    *remainBuffLen = *remainBuffLen - sizeof(FileAppenderSegHeader);
    /* The header may span across two buffs.
     * So, we cannot directly copy the header from the buffer.
     */
    if (likely(desc->header_offset == -1)) {
        rc = memcpy_s(&desc->header, sizeof(FileAppenderSegHeader), *buffOffset, sizeof(FileAppenderSegHeader));
        securec_check(rc, "\0", "\0");
        desc->payload = *buffOffset + sizeof(FileAppenderSegHeader);
        *buffOffset = desc->payload;
    } else {
        rc = memcpy_s(&desc->header, desc->header_offset, *buffOffset, desc->header_offset);
        securec_check(rc, "\0", "\0");
        rc = memcpy_s(&desc->header + desc->header_offset, (sizeof(FileAppenderSegHeader) - desc->header_offset),
                      cxt->bufData, (sizeof(FileAppenderSegHeader) - desc->header_offset));
        securec_check(rc, "\0", "\0");
        desc->payload = cxt->bufData + (sizeof(FileAppenderSegHeader) - desc->header_offset);
        *buffOffset = desc->payload;
    }
}

void parseSegDescriptor(FileAppenderSegDescriptor* desc, char** buffOffset, size_t* remainBuffLen, char* tempBuffer,
                        BufferCxt* cxt, pgBackup* dest_backup, bool isValidate, validate_files_arg* arg) {
    error_t rc = 0;
    parray* files = dest_backup->files;
    *remainBuffLen = *remainBuffLen - desc->header.size;

    if (desc->payload_offset == -1) {
        rc = memcpy_s(tempBuffer, desc->header.size, desc->payload, desc->header.size);
        securec_check(rc, "\0", "\0");
        *buffOffset = desc->payload + desc->header.size;
    } else {
        rc = memcpy_s(tempBuffer, desc->payload_offset, desc->payload, desc->payload_offset);
        securec_check(rc, "\0", "\0");
        rc = memcpy_s(tempBuffer + desc->payload_offset, (desc->header.size - desc->payload_offset),
                      cxt->bufData, (desc->header.size - desc->payload_offset));
        securec_check(rc, "\0", "\0");
        *buffOffset = cxt->bufData + (desc->header.size - desc->payload_offset);
        desc->payload_offset = -1;
    }

    if (desc->header.type == FILE_APPEND_TYPE_FILES_END || desc->header.type == FILE_APPEND_TYPE_FILES) {
        return;
    } else if (desc->header.type == FILE_APPEND_TYPE_DIR) {
        restoreDir(tempBuffer, desc, dest_backup, files, isValidate);
    } else if (desc->header.type == FILE_APPEND_TYPE_FILE) {
        openRestoreFile(tempBuffer, desc, dest_backup, files, isValidate, arg);
    } else if (desc->header.type == FILE_APPEND_TYPE_FILE_CONTENT) {
        writeOrValidateRestoreFile(tempBuffer, desc, isValidate, arg);
    } else if (desc->header.type == FILE_APPEND_TYPE_FILE_END) {
        closeRestoreFile(desc);
    } else {
        if (isValidate) {
            arg->corrupted = true;
        } else {
            elog(ERROR, "Unknown file type: %d, when restore file: %s", desc->header.type, desc->inputFile->rel_path);
        }
    }
}

void destorySegDescriptor(FileAppenderSegDescriptor** descriptor)
{
    FileAppenderSegDescriptor* desc = *descriptor;
    pfree_ext(desc);
}

void closeFileAppender(FileAppender* appender)
{
    if (!appender) {
        return;
    }
    FileAppenderSegHeader header;
    header.type = FILE_APPEND_TYPE_FILES_END;
    header.size = 0;
    header.permission = 0;
    header.filesize = 0;
    header.crc = 0;
    header.external_dir_num = 0;
    header.file_type = DEV_TYPE_INVALID;
    ((BufferCxt *)appender->filePtr)->fileEnd = true;
    writeHeader(&header, appender);
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

void writeHeader(FileAppenderSegHeader* header, FileAppender* appender)
{
    size_t writeLen = 0;
    if (!appender || ((appender->currFileSize + APPEND_FILE_HEADER_SIZE) > APPEND_FILE_MAX_SIZE)) {
        elog(ERROR, "Write header failed.");
    }
    if (header->type != FILE_APPEND_TYPE_FILES_END && (appender->currFileSize + APPEND_FILE_HEADER_SIZE + header->size) >
        (APPEND_FILE_MAX_SIZE - APPEND_FILE_HEADER_SIZE)) {
        uint32 minFileNo = appender->minFileNo;
        uint32 maxFileNo = appender->maxFileNo;
        closeFileAppender(appender);
        initFileAppender(appender, FILE_APPEND_TYPE_FILES, minFileNo, maxFileNo + 1);
    }
    /* filePtr is a buffer context*/
    writeLen = writeToCompFile((char*)header, sizeof(FileAppenderSegHeader), appender->filePtr);
    if (writeLen != sizeof(FileAppenderSegHeader)) {
        elog(ERROR, "Write header failed, write length: %lu.", writeLen);
    }
    appender->currFileSize += writeLen;
}


size_t writeToCompFile(const char* data, size_t len, void* file)
{
    if (writeToBuffer(data, len, file) != len) {
        return 0;
    }
    return len;
}

void writePayload(const char* data, size_t len, FileAppender* appender)
{
    if (appender->currFileSize + len > (APPEND_FILE_MAX_SIZE - APPEND_FILE_HEADER_SIZE)) {
        uint32 minFileNo = appender->minFileNo;
        uint32 maxFileNo = appender->maxFileNo;
        closeFileAppender(appender);
        initFileAppender(appender, FILE_APPEND_TYPE_FILES, minFileNo, maxFileNo + 1);
    }
    size_t writeLen = writeToCompFile(data, len, appender->filePtr);
    if (writeLen != len) {
        elog(ERROR, "Write payload data failed, write length: %lu.", writeLen);
    }
    appender->currFileSize += writeLen;
}