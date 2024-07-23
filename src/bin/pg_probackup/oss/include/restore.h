/*-------------------------------------------------------------------------
 *
 * restore.h: Restore utils used by Backup/Restore manager.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2009-2013, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 2015-2018, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */
#ifndef RESTORE_H
#define RESTORE_H

#include "../../pg_probackup.h"
#include "appender.h"

/* API Function */

extern void performValidate(pgBackup *backup, pgRestoreParams *params);

extern void performRestoreOrValidate(pgBackup *dest_backup, bool isValidate = false);

extern void restoreDir(const char* path, FileAppenderSegDescriptor* desc, pgBackup* dest_backup,
                     parray* files, bool isValidate);

extern void restoreDataFile(const char* data, FileAppenderSegDescriptor* desc, parray *parent_chain, bool use_bitmap,
                     bool use_headers);

extern void restoreNonDataFile(const char* data, FileAppenderSegDescriptor* desc, parray *parent_chain, pgBackup *dest_backup,
                        bool isValidate, validate_files_arg* arg);

extern void openRestoreFile(const char* path, FileAppenderSegDescriptor* desc, pgBackup* dest_backup,
                     parray* files, bool isValidate = false, validate_files_arg* arg = NULL);

extern void writeOrValidateRestoreFile(const char* data, FileAppenderSegDescriptor* desc,
                                bool isValidate, validate_files_arg* arg);

extern void closeRestoreFile(FileAppenderSegDescriptor* desc);

extern void restoreConfigDir();

extern void restoreConfigFile(const char* path, bool errorOk = false);

extern void uploadConfigFile(const char* local_path, const char* object_name);

#endif /* RESTORE_H */