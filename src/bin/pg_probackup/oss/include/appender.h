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

/* default seg headr, version = 0 */
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

/* version = 1 */
typedef struct ParallelFileAppenderSegHeader
{
    FILE_APPEND_SEG_TYPE type;  /* high 16 bits used as the version, low 16 bits used as the type */
    uint32 size;                /* payload size */
    uint32 permission;
    off_t filesize;
    pg_crc32 crc;
    int external_dir_num;
    device_type_t file_type;
    short threadId; /* thread id */
} ParallelFileAppenderSegHeader;

#define SEG_HEADER_DEFAULT_VERSION 0 // FileAppenderSegHeader
#define SEG_HEADER_PARALLEL_VERSION 1 // ParallelFileAppenderSegHeader
#define SEG_HEADER_VERSION_MAX 2 // ParallelFileAppenderSegHeader

#define SEG_HEADER_VERSION_OFFSET 16 // high 16 bits of type used to store the version of header
#define SEG_HEADER_VERSION_MASK (0xFFFF0000) // low 16 bits of type used to store the type of header
#define SEG_HEADER_TYPE_MASK (0xFFFF) // high 16 bits of type used to store the type of header

inline uint32 getSegHeaderSize(uint32 version)
{
    switch (version) {
        case SEG_HEADER_DEFAULT_VERSION:
            return sizeof(FileAppenderSegHeader);
        case SEG_HEADER_PARALLEL_VERSION:
            return sizeof(ParallelFileAppenderSegHeader);
        default:
            break;
    }
    Assert(false);
    return -1;
}

inline uint32 getSegHeaderVersion(void *header)
{
    uint32 version = (uint32)((*(uint32*)header) >> SEG_HEADER_VERSION_OFFSET);
    Assert(version < SEG_HEADER_VERSION_MAX);
    return version;
}

inline NO_VERSION_SEG_TYPE GetSegHeaderType(void* header)
{
    NO_VERSION_SEG_TYPE type = (NO_VERSION_SEG_TYPE)(((FileAppenderSegHeader *)header)->type & SEG_HEADER_TYPE_MASK);
    Assert(type < FILE_APPEND_TYPE_MAX);
    return type;
}

inline void setSegHeaderVersion(void* header, uint32 version)
{
    Assert(version < SEG_HEADER_VERSION_MAX);
    ((FileAppenderSegHeader *)header)->type = (FILE_APPEND_SEG_TYPE)((((FileAppenderSegHeader *)header)->type &
                                              SEG_HEADER_TYPE_MASK) + (version << SEG_HEADER_VERSION_OFFSET));
}

inline void SetSegHeaderType(void* header, NO_VERSION_SEG_TYPE type)
{
    Assert(type < FILE_APPEND_TYPE_MAX);
    ((FileAppenderSegHeader *)header)->type =
        (FILE_APPEND_SEG_TYPE)((((FileAppenderSegHeader *)header)->type & SEG_HEADER_VERSION_MASK) + type);
}

#define MAX_BACKUP_THREAD 11
typedef struct FileAppenderSegDescriptor
{
    char* header; /* FileAppenderSegHeader or ParallelFileAppenderSegHeader  according to version */
    char* payload;
    int header_offset; /* set value only when header spans across two buffs and a rewind exists. */
    int payload_offset; /* set value only when payload spans across two buffs and a rewind exists. */
    FILE* outputFile[MAX_BACKUP_THREAD];
    pgFile* inputFile[MAX_BACKUP_THREAD];
    pg_crc32 crc[MAX_BACKUP_THREAD];
    uint32 version; /* the version of header */
} FileAppenderSegDescriptor;

/* Constants Definition */

#define READ_BUFFER_BLOCK_COUNT 2
#define APPEND_FILENAME_END_SIZE 10
#define APPEND_FILENAME_END_DIGIT 6
constexpr int APPEND_FILE_MAX_SIZE = 1073741824; // 1073741824, 1GB
#define APPEND_FILE_HEADER_SIZE (sizeof(FileAppenderSegHeader))
#define PARALLEL_APPEND_FILE_HEADER_SIZE (sizeof(ParallelFileAppenderSegHeader))
#define SEG_HEADER_MAX_SIZE (PARALLEL_APPEND_FILE_HEADER_SIZE)

/* API Function */

extern void initFileAppender(FileAppender* appender, FILE_APPEND_SEG_TYPE type, uint32 minFileNo, uint32 maxFileNo);

extern void initSegDescriptor(FileAppenderSegDescriptor** segDesc);

extern void getSegDescriptor(FileAppenderSegDescriptor* desc, char** buffOffset, size_t* remainBuffLen,
                             BufferCxt* cxt, uint32 headerVersion, size_t segHdrLen);

extern void parseSegDescriptor(FileAppenderSegDescriptor* segDesc, char** buffOffset, size_t* remainBuffLen, char* tempBuffer,
                        BufferCxt* cxt, pgBackup* dest_backup,
                        bool isValidate = false, validate_files_arg* arg = NULL);

extern void destorySegDescriptor(FileAppenderSegDescriptor** descriptor);

extern void closeFileAppender(FileAppender* retAppender);

extern void destoryFileAppender(FileAppender** retAppender);

extern char* getAppendFileName(const char* baseFileName, uint32 fileNo);

extern void constructHeader(FileAppenderSegHeader* header, FILE_APPEND_SEG_TYPE type,
                            uint32 size, off_t filesize, pgFile* file);

extern void constructParallelHeader(ParallelFileAppenderSegHeader* header, FILE_APPEND_SEG_TYPE type,
                                    uint32 size, off_t filesize, pgFile* file, short readerIndexId = 0);

extern void WriteHeader(ParallelFileAppenderSegHeader* header, FileAppender* appender);

extern size_t writeToCompFile(const char* data, size_t len, void* file);

extern void WriteDataBlock(ParallelFileAppenderSegHeader* header, const char* payload,
                           size_t payloadSize, FileAppender* appender);

#endif /* FILE_APPEND_H */

