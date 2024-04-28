/*-------------------------------------------------------------------------
 *
 * appender.h: File appender used by Backup/Restore manager.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#ifndef FILE_APPEND_H
#define FILE_APPEND_H

#include "../../pg_probackup.h"
#include "buffer.h"

/* Data Structure Definition*/

typedef struct FileAppenderSegHeader
{
    FILE_APPEND_SEG_TYPE type;  /* seg type */
    uint32 size;                /* payload size */
    uint32 permission;
    off_t filesize;
    pg_crc32 crc;
    int external_dir_num;
    device_type_t file_type;
} FileAppenderSegHeader;

typedef struct FileAppenderSegDescriptor
{
    FileAppenderSegHeader header;
    char* payload;
    int header_offset; /* set value only when header spans across two buffs and a rewind exists. */
    int payload_offset; /* set value only when payload spans across two buffs and a rewind exists. */
    FILE* outputFile;
    pgFile* inputFile;
    pg_crc32 crc;
    pg_crc32 size;
} FileAppenderSegDescriptor;

/* Constants Definition */

#define READ_BUFFER_BLOCK_COUNT 2
#define APPEND_FILENAME_END_SIZE 10
#define APPEND_FILENAME_END_DIGIT 6
#define APPEND_FILE_MAX_SIZE 536870912 // 536870912, 512MB; 1073741824, 1GB
#define APPEND_FILE_HEADER_SIZE (sizeof(FileAppenderSegHeader))

/* API Function */

extern void initFileAppender(FileAppender* appender, FILE_APPEND_SEG_TYPE type, uint32 minFileNo, uint32 maxFileNo);

extern void initSegDescriptor(FileAppenderSegDescriptor** segDesc);

extern void getSegDescriptor(FileAppenderSegDescriptor* desc, char** buffOffset, size_t* remainBuffLen, BufferCxt* cxt);

extern void parseSegDescriptor(FileAppenderSegDescriptor* segDesc, char** buffOffset, size_t* remainBuffLen, char* tempBuffer,
                        BufferCxt* cxt, pgBackup* dest_backup,
                        bool isValidate = false, validate_files_arg* arg = NULL);

extern void destorySegDescriptor(FileAppenderSegDescriptor** descriptor);

extern void closeFileAppender(FileAppender* retAppender);

extern void destoryFileAppender(FileAppender** retAppender);

extern char* getAppendFileName(const char* baseFileName, uint32 fileNo);

extern void constructHeader(FileAppenderSegHeader* header, FILE_APPEND_SEG_TYPE type,
                     uint32 size, off_t filesize, pgFile* file);

extern void writeHeader(FileAppenderSegHeader* header, FileAppender* appender);

extern size_t writeToCompFile(const char* data, size_t len, void* file);

extern void writePayload(const char* data, size_t len, FileAppender* appender);

#endif /* FILE_APPEND_H */

