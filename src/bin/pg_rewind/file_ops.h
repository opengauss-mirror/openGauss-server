/* -------------------------------------------------------------------------
 *
 * file_ops.h
 *	  Helper functions for operating on files
 *
 * Copyright (c) 2013-2015, PostgreSQL Global Development Group
 *
 * -------------------------------------------------------------------------
 */
#ifndef FILE_OPS_H
#define FILE_OPS_H

#include "filemap.h"
#include "compressed_common.h"
#include "PageCompression.h"
extern char* pg_data;

extern void open_target_file(const char* path, bool trunc);
extern void write_target_range(char* buf, off_t begin, size_t size, int space, bool compressed = false);
extern void close_target_file(void);
extern void truncate_target_file(const char* path, off_t newsize);
extern void create_target(file_entry_t* t);
extern void remove_target(file_entry_t* t);
extern void remove_target_file(const char* path, bool missingok);

extern char* slurpFile(const char* datadir, const char* path, size_t* filesize);

extern bool is_dir_path(const char* path);
extern bool is_file_path(const char* path);
extern bool is_file_exist(const char* path);

extern void create_backup_filepath(const char* fullpath, char* backupPath);
extern void backup_target(file_entry_t* entry, const char* lastoff);
extern void backup_target_dir(const char* path);
extern void backup_target_file(char* path, const char* lastoff);
extern void backup_target_symlink(const char* path);
extern void backup_fake_target_file(const char* path);
extern bool isOldXlog(const char* path, const char* lastoff);
extern bool is_in_restore_process(const char* pg_data);
extern void delete_all_file(const char* path, bool remove_top);
extern bool restore_target_dir(const char* datadir_target, bool remove_from);
extern void delete_target_file(const char* file);
extern bool isPathInFilemap(const char* path);
extern bool tablespaceDataIsValid(const char* path);
extern void copy_file(const char* fromfile, char* tofile);

extern void CompressedFileTruncate(const char* path, const RewindCompressInfo* rewindCompressInfo);
extern void FetchCompressedFile(char* buf, BlockNumber begin, int32 size);
extern void CompressFileClose();
extern void CompressedFileInit(const char* fileName, int32 chunkSize, int32 algorithm, bool rebuild);
extern bool FileProcessErrorReport(const char *path, COMPRESS_ERROR_STATE errorState);
#endif /* FILE_OPS_H */

