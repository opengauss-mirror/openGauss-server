/* -------------------------------------------------------------------------
 *
 * fd.h
 *	  Virtual file descriptor definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/fd.h
 *
 * -------------------------------------------------------------------------
 */

/*
 * calls:
 *
 *	File {Close, Read, Write, Seek, Tell, Sync}
 *	{Path Name Open, Allocate, Free} File
 *
 * These are NOT JUST RENAMINGS OF THE UNIX ROUTINES.
 * Use them for all file activity...
 *
 *	File fd;
 *	fd = FilePathOpenFile("foo", O_RDONLY, 0600);
 *
 *	AllocateFile();
 *	FreeFile();
 *
 * Use AllocateFile, not fopen, if you need a stdio file (FILE*); then
 * use FreeFile, not fclose, to close it.  AVOID using stdio for files
 * that you intend to hold open for any length of time, since there is
 * no way for them to share kernel file descriptors with other files.
 *
 * Likewise, use AllocateDir/FreeDir, not opendir/closedir, to allocate
 * open directories (DIR*), and OpenTransientFile/CloseTransient File for an
 * unbuffered file descriptor.
 */
#ifndef FD_H
#define FD_H

#include <dirent.h>
#include "utils/hsearch.h"
#include "storage/smgr/relfilenode.h"
#include "storage/page_compression.h"
#include "postmaster/aiocompleter.h"

/*
 * FileSeek uses the standard UNIX lseek(2) flags.
 */

typedef char* FileName;

typedef int File;

#define FILE_INVALID (-1)

typedef struct DataFileIdCacheEntry {
    /* key field */
    RelFileNodeForkNum dbfid; /* file id */
    /* the following are setted in runtime */
    int fd;
    int refcount;
    int repaired_fd;
} DataFileIdCacheEntry;

enum FileExistStatus { FILE_EXIST, FILE_NOT_EXIST, FILE_NOT_REG };

/*
 * On Windows, we have to interpret EACCES as possibly meaning the same as
 * ENOENT, because if a file is unlinked-but-not-yet-gone on that platform,
 * that's what you get.  Ugh.  This code is designed so that we don't
 * actually believe these cases are okay without further evidence (namely,
 * a pending fsync request getting canceled ... see mdsync).
 */
#ifndef WIN32
#define FILE_POSSIBLY_DELETED(err) ((err) == ENOENT)
#else
#define FILE_POSSIBLY_DELETED(err) ((err) == ENOENT || (err) == EACCES)
#endif

/*
 * prototypes for functions in fd.c
 */

/* Operations on virtual Files --- equivalent to Unix kernel file ops */
extern File PathNameOpenFile(FileName fileName, int fileFlags, int fileMode, File file = FILE_INVALID);
extern File OpenTemporaryFile(bool interXact);
extern void FileClose(File file);
extern void FileCloseWithThief(File file);
extern int FilePrefetch(File file, off_t offset, int amount, uint32 wait_event_info = 0);
extern int FileSync(File file, uint32 wait_event_info = 0);
extern off_t FileSeek(File file, off_t offset, int whence);
extern int FileTruncate(File file, off_t offset, uint32 wait_event_info = 0);
extern void FileWriteback(File file, off_t offset, off_t nbytes);
extern char* FilePathName(File file);

extern void FileAsyncCUClose(File* vfdList, int32 vfdnum);
extern int FileAsyncRead(AioDispatchDesc_t** dList, int32 dn);
extern int FileAsyncWrite(AioDispatchDesc_t** dList, int32 dn);
extern int FileAsyncCURead(AioDispatchCUDesc_t** dList, int32 dn);
extern int FileAsyncCUWrite(AioDispatchCUDesc_t** dList, int32 dn);
extern void FileFastExtendFile(File file, uint32 offset, uint32 size, bool keep_size);
extern int FileRead(File file, char* buffer, int amount);
extern int FileWrite(File file, const char* buffer, int amount, off_t offset, int fastExtendSize = 0);

// Threading virtual files IO interface, using pread() / pwrite()
//
extern int FilePRead(File file, char* buffer, int amount, off_t offset, uint32 wait_event_info = 0);
extern int FilePWrite(File file, const char *buffer, int amount, off_t offset, uint32 wait_event_info = 0,
    int fastExtendSize = 0);

extern int AllocateSocket(const char* ipaddr, int port);
extern int FreeSocket(int sockfd);

/* Operations used for sharing named temporary files */
extern File PathNameCreateTemporaryFile(char *name, bool error_on_failure);
extern File PathNameOpenTemporaryFile(char *name);
extern bool PathNameDeleteTemporaryFile(const char *name, bool error_on_failure);
extern void PathNameCreateTemporaryDir(const char *base, const char *name);
extern void PathNameDeleteTemporaryDir(const char *name);
extern void TempTablespacePath(char *path, Oid tablespace);

/* Operations that allow use of regular stdio --- USE WITH CAUTION */
extern FILE* AllocateFile(const char* name, const char* mode);
extern int FreeFile(FILE* file);
extern void GlobalStatsCleanupFiles();

extern File OpenCacheFile(const char* pathname, bool unlink_owner);
extern void UnlinkCacheFile(const char* pathname);

/* Operations to allow use of the <dirent.h> library routines */
extern DIR* AllocateDir(const char* dirname);
extern struct dirent* ReadDir(DIR* dir, const char* dirname);
extern int FreeDir(DIR* dir);
/* Operations to allow use of a plain kernel FD, with automatic cleanup */
extern int OpenTransientFile(FileName fileName, int fileFlags, int fileMode);
extern int CloseTransientFile(int fd);
/* If you've really really gotta have a plain kernel FD, use this */
extern int BasicOpenFile(FileName fileName, int fileFlags, int fileMode);

/* Miscellaneous support routines */
extern void InitFileAccess(void);
extern void set_max_safe_fds(void);
extern void CloseGaussPidDir(void);
extern void closeAllVfds(void);
extern void SetTempTablespaces(Oid* tableSpaces, int numSpaces);
extern bool TempTablespacesAreSet(void);
extern int GetTempTablespaces(Oid *tableSpaces, int numSpaces);
extern Oid GetNextTempTableSpace(void);
extern void AtEOXact_Files(void);
extern void AtEOSubXact_Files(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid);
extern void AtProcExit_Files(int code, Datum arg);
extern void RemovePgTempFiles(void);

extern void RemoveErrorCacheFiles();
extern int FileFd(File file);

extern int pg_fsync(int fd);
extern int pg_fsync_no_writethrough(int fd);
extern int pg_fsync_writethrough(int fd);
extern int pg_fdatasync(int fd);
extern void pg_flush_data(int fd, off_t offset, off_t amount);
extern void DestroyAllVfds(void);

extern void InitDataFileIdCache(void);
extern Size DataFileIdCacheSize(void);
extern File DataFileIdOpenFile(
    FileName fileName, const RelFileNodeForkNum& fileNode, int fileFlags, int fileMode, File file = FILE_INVALID);

extern RelFileNodeForkNum RelFileNodeForkNumFill(
    const RelFileNodeBackend& rnode, ForkNumber forkNum, BlockNumber segno);

extern RelFileNodeForkNum RelFileNodeForkNumFill(RelFileNode* rnode,
    BackendId backend, ForkNumber forknum, BlockNumber segno);

extern void FreeAllAllocatedDescs(void);
extern void GetFdGlobalVariables(void*** global_VfdCache, Size** global_SizeVfdCache);
extern void SwitchToGlobalVfdCache(void** vfd, Size* vfd_size);
extern void ResetToLocalVfdCache();

extern int data_sync_elevel(int elevel);

extern bool FdRefcntIsZero(SMgrRelation reln, ForkNumber forkNum);
extern FileExistStatus CheckFileExists(const char* path);
extern bool repair_deleted_file_check(RelFileNodeForkNum fileNode, int fd);

/* Page compression support routines */
extern void SetupPageCompressMemoryMap(File file, RelFileNode node, const RelFileNodeForkNum& relFileNodeForkNum);
extern PageCompressHeader *GetPageCompressMemoryMap(File file, uint32 chunk_size);

/* Filename components for OpenTemporaryFile */
// Note that this macro must be the same to macro in initdb.cpp
// If you change it, you must also change initdb.cpp
//
#define PG_TEMP_FILES_DIR "pgsql_tmp"
#define PG_TEMP_FILE_PREFIX "pgsql_tmp"

#endif /* FD_H */
