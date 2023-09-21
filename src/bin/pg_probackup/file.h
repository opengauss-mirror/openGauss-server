/*-------------------------------------------------------------------------
 *
 * file.h
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#ifndef __FILE__H__
#define __FILE__H__

#include "storage/buf/bufpage.h"
#include <stdio.h>
#include <sys/stat.h>
#include <dirent.h>

#include "PageCompression.h"

#ifdef HAVE_LIBZ
#include <zlib.h>
#endif

typedef enum
{
    /* message for compatibility check */
    FIO_AGENT_VERSION, /* never move this */
    FIO_OPEN,
    FIO_CLOSE,
    FIO_WRITE,
    FIO_SYNC,
    FIO_RENAME,
    FIO_SYMLINK,
    FIO_UNLINK,
    FIO_MKDIR,
    FIO_CHMOD,
    FIO_SEEK,
    FIO_TRUNCATE,
    FIO_DELETE,
    FIO_PREAD,
    FIO_READ,
    FIO_LOAD,
    FIO_STAT,
    FIO_SEND,
    FIO_ACCESS,
    FIO_OPENDIR,
    FIO_READDIR,
    FIO_CLOSEDIR,
    FIO_PAGE,
    FIO_WRITE_COMPRESSED,
    FIO_COSTRUCT_COMPRESSED,
    FIO_GET_CRC32,
    /* used for incremental restore */
    FIO_GET_CHECKSUM_MAP,
    FIO_GET_LSN_MAP,
     /* used in fio_send_pages */
    FIO_SEND_PAGES,
    FIO_ERROR,
    FIO_SEND_FILE,
    //	FIO_CHUNK,
    FIO_SEND_FILE_EOF,
    FIO_SEND_FILE_CORRUPTION,
    FIO_SEND_FILE_HEADERS,
    /* messages for closing connection */
    FIO_DISCONNECT,
    FIO_DISCONNECTED,
    FIO_LIST_DIR,
    FIO_CHECK_POSTMASTER
} fio_operations;

typedef enum
{
    FIO_LOCAL_HOST,  /* data is locate at local host */
    FIO_DB_HOST,     /* data is located at Postgres server host */
    FIO_BACKUP_HOST, /* data is located at backup host */
    FIO_REMOTE_HOST, /* date is located at remote host */
    FIO_DSS_HOST     /* data is located at dss storage, it can be visit in local host */
} fio_location;

#define FIO_FDMAX 64
#define FIO_PIPE_MARKER 0x40000000

#define SYS_CHECK(cmd) do if ((cmd) < 0) { fprintf(stderr, "%s:%d: (%s) %s\n", __FILE__, __LINE__, #cmd, strerror(errno)); exit(EXIT_FAILURE); } while (0)
#define IO_CHECK(cmd, size) do { int _rc = (cmd); if (_rc != (int)(size)) fio_error(_rc, size, __FILE__, __LINE__); } while (0)

#define FILE_PERMISSIONS 0600
#define DSS_BLCKSZ  2097152    //2M , In dss mode, the size of operation(read and write) from a DSS file is 2M

typedef struct
{
    //  fio_operations cop;
    //  16
    unsigned cop    : 32;
    unsigned handle : 32;
    unsigned size   : 32;
    unsigned arg;
} fio_header;

extern fio_location MyLocation;
extern __thread int fio_stdout;
extern __thread int fio_stdin;

/* Check if FILE handle is local or remote (created by FIO) */
#define fio_is_remote_file(file) ((size_t)(file) <= FIO_FDMAX)
extern bool    fio_is_dss(fio_location location);
extern ssize_t fio_read_all(int fd, void* buf, size_t size);
extern ssize_t fio_write_all(int fd, void const* buf, size_t size);
extern void    fio_redirect(int in, int out, int err);
extern void    fio_communicate(int in, int out);

extern int     fio_get_agent_version(void);
extern FILE*   fio_fopen(char const* name, char const* mode, fio_location location);
extern size_t  fio_fwrite(FILE* f, void const* buf, size_t size);
extern void fio_construct_compressed(void const* buf, size_t size);
extern ssize_t fio_fwrite_compressed(FILE* f, void const* buf, size_t size, int compress_alg, const char *to_fullpath, char *preWriteBuf, int *preWriteOff, int *targetSize);
extern ssize_t fio_fread(FILE* f, void* buf, size_t size);
extern int     fio_pread(FILE* f, void* buf, off_t offs, PageCompression* pageCompression, int size);
extern int     fio_fprintf(FILE* f, char const* arg, ...);// pg_attribute_printf(2, 3);
extern int     fio_fflush(FILE* f);
extern int     fio_fseek(FILE* f, off_t offs);
extern int     fio_ftruncate(FILE* f, off_t size);
extern int     fio_fclose(FILE* f);
extern int     fio_ffstat(FILE* f, struct stat* st);
extern void    fio_error(int rc, int size, char const* file, int line);

extern int     fio_open(char const* name, int mode, fio_location location);
extern ssize_t fio_write(int fd, void const* buf, size_t size);
extern ssize_t fio_read(int fd, void* buf, size_t size);
extern int     fio_flush(int fd);
extern int     fio_seek(int fd, off_t offs);
extern int     fio_fstat(int fd, struct stat* st);
extern int     fio_truncate(int fd, off_t size);
extern int     fio_close(int fd);
extern void    fio_disconnect(void);
extern int     fio_sync(char const* path, fio_location location);
extern pg_crc32 fio_get_crc32(const char *file_path, fio_location location, bool decompress);

extern int     fio_rename(char const* old_path, char const* new_path, fio_location location);
extern int     fio_symlink(char const* target, char const* link_path, bool overwrite, fio_location location);
extern int     fio_unlink(char const* path, fio_location location);
extern int     fio_mkdir(char const* path, int mode, fio_location location);
extern int     fio_chmod(char const* path, int mode, fio_location location);
extern int     fio_access(char const* path, int mode, fio_location location);
extern int     fio_stat(char const* path, struct stat* st, bool follow_symlinks, fio_location location);
extern DIR*    fio_opendir(char const* path, fio_location location);
extern struct dirent * fio_readdir(DIR *dirp);
extern int     fio_closedir(DIR *dirp);
extern FILE*   fio_open_stream(char const* name, fio_location location);
extern int     fio_close_stream(FILE* f);

struct CompressCommunicate {
    char path[MAXPGPATH];
    BlockNumber segmentNo;
    int chunkSize;
    int algorithm;
};

#ifdef HAVE_LIBZ
extern gzFile  fio_gzopen(char const* path, char const* mode, int level, fio_location location);
extern int     fio_gzclose(gzFile file);
extern int     fio_gzread(gzFile f, void *buf, unsigned size);
extern int     fio_gzwrite(gzFile f, void * buf, unsigned size);
extern int     fio_gzeof(gzFile f);
extern z_off_t fio_gzseek(gzFile f, z_off_t offset, int whence);
extern const char* fio_gzerror(gzFile file, int *errnum);
#endif

extern char check_logical_replslot_dir(const char *rel_path);

#endif

