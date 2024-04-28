/*-------------------------------------------------------------------------
 *
 * backup.h: Backup utils used by Backup/Restore manager.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#ifndef BACKUP_H
#define BACKUP_H

#include "../../pg_probackup.h"
#include "appender.h"
#include "thread.h"

/* API Function */

extern void performBackup(backup_files_arg* arg);

extern void initPerformBackup(backup_files_arg* arg, backupReaderThreadArgs* thread_args);

extern void backupDataFiles(backup_files_arg* arg);

extern void backupDirectories(FileAppender* appender, backup_files_arg* arg);

extern void backupFiles(FileAppender* appender, backup_files_arg* arg);

extern void appendDir(FileAppender* appender, const char* dirPath, uint32 permission,
               int external_dir_num, device_type_t type);

extern void appendDataFile(FileAppender* appender, char* fileBuffer, pgFile* file,
                    FILE_APPEND_SEG_TYPE type, char* from_fullpath, backup_files_arg* arg);

extern void appendNonDataFile(FileAppender* appender, char* fileBuffer, pgFile* file,
                       FILE_APPEND_SEG_TYPE type, char* from_fullpath, backup_files_arg* arg);

extern void appendPgControlFile(FileAppender* appender, const char *from_fullpath, fio_location from_location, pgFile *file);

extern int writeDataFile(FileAppender* appender, char* fileBuffer, pgFile* file, char* from_fullpath, backup_files_arg* arg);

#endif /* BACKUP_H */

