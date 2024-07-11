/*-------------------------------------------------------------------------
 *
 * file.c
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "pg_probackup.h"
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

#include "file.h"
#include "storage/checksum.h"
#include "storage/file/fio_device.h"
#include "common/fe_memutils.h"

#define PRINTF_BUF_SIZE  1024


static __thread unsigned long fio_fdset = 0;
static __thread void* fio_stdin_buffer;
 __thread int fio_stdout = 0;
__thread int fio_stdin = 0;
static __thread int fio_stderr = 0;

fio_location MyLocation;

typedef struct
{
    BlockNumber nblocks;
    BlockNumber segmentno;
    XLogRecPtr  horizonLsn;
    uint32      checksumVersion;
    int         calg;
    int         clevel;
    int         bitmapsize;
    int         path_len;
} fio_send_request;


typedef struct
{
    char path[MAXPGPATH];
    bool exclude;
    bool follow_symlink;
    bool add_root;
    bool backup_logs;
    bool exclusive_backup;
    bool skip_hidden;
    int  external_dir_num;
    bool backup_replslots;
} fio_list_dir_request;

typedef struct
{
    mode_t  mode;
    size_t  size;
    time_t  mtime;
    bool    is_datafile;
    bool    compressed_file;
    uint16  compressed_chunk_size;
    uint8   compressed_algorithm;
    bool    is_database;
    Oid     tblspcOid;
    Oid     dbOid;
    Oid     relOid;
    ForkName   forkName;
    int     segno;
    int     external_dir_num;
    int     linked_len;
} fio_pgFile;

typedef struct
{
    BlockNumber n_blocks;
    BlockNumber segmentno;
    XLogRecPtr  stop_lsn;
    uint32      checksumVersion;
} fio_checksum_map_request;

typedef struct
{
    BlockNumber n_blocks;
    BlockNumber segmentno;
    XLogRecPtr  shift_lsn;
    uint32      checksumVersion;
} fio_lsn_map_request;


/* Convert FIO pseudo handle to index in file descriptor array */
#define fio_fileno(f) (((size_t)f - 1) | FIO_PIPE_MARKER)

#if defined(WIN32)
#undef open(a, b, c)
#undef fopen(a, b)
#endif

/* Use specified file descriptors as stdin/stdout for FIO functions */
void fio_redirect(int in, int out, int err)
{
    fio_stdin = in;
    fio_stdout = out;
    fio_stderr = err;
}

void fio_error(int rc, int size, char const* file, int line)
{
    if (remote_agent)
    {
        fprintf(stderr, "%s:%d: processed %d bytes instead of %d: %s\n", file, line, rc, size, rc >= 0 ? "end of data" :  strerror(errno));
        exit(EXIT_FAILURE);
    }
    else
    {
        char buf[PRINTF_BUF_SIZE+1];
        
        int err_size = read(fio_stderr, buf, PRINTF_BUF_SIZE);
        if (err_size > 0)
        {
            buf[err_size] = '\0';
            elog(ERROR, "Agent error: %s", buf);
        }
        else
            elog(ERROR, "Communication error: %s", rc >= 0 ? "end of data" :  strerror(errno));
    }
}

/* Check if file descriptor is local or remote (created by FIO) */
static bool fio_is_remote_fd(int fd)
{
    return (fd & FIO_PIPE_MARKER) != 0;
}

#ifdef WIN32

#undef stat

/*
 * The stat() function in win32 is not guaranteed to update the st_size
 * field when run. So we define our own version that uses the Win32 API
 * to update this field.
 */
static int
fio_safestat(const char *path, struct stat *buf)
{
    int            r;
    WIN32_FILE_ATTRIBUTE_DATA attr;

    r = stat(path, buf);
    if (r < 0)
        return r;

    if (!GetFileAttributesEx(path, GetFileExInfoStandard, &attr))
    {
        errno = ENOENT;
        return -1;
    }

    /*
     * XXX no support for large files here, but we don't do that in general on
     * Win32 yet.
     */
    buf->st_size = attr.nFileSizeLow;

    return 0;
}

#define stat(x, y) fio_safestat(x, y)

/* TODO: use real pread on Linux */
static ssize_t pread(int fd, void* buf, size_t size, off_t off)
{
    off_t rc = lseek(fd, off, SEEK_SET);
    if (rc != off)
        return -1;
    return read(fd, buf, size);
}
static int remove_file_or_dir(char const* path)
{
    int rc = remove(path);
#ifdef WIN32
    if (rc < 0 && errno == EACCESS)
        rc = rmdir(path);
#endif
    return rc;
}
#else
#define remove_file_or_dir(path) remove(path)
#endif

/* Check if specified location is local for current node */
bool fio_is_remote(fio_location location)
{
    bool is_remote = MyLocation != FIO_LOCAL_HOST
                                && location != FIO_LOCAL_HOST
                                && location != MyLocation;
    if (is_remote && !fio_stdin && !launch_agent())
        elog(ERROR, "Failed to establish SSH connection: %s", strerror(errno));
    return is_remote;
}

/* Check if specified location is local for current node */
bool fio_is_remote_simple(fio_location location)
{
    bool is_remote = MyLocation != FIO_LOCAL_HOST
                                && location != FIO_LOCAL_HOST
                                && location != MyLocation;
    return is_remote;
}

/* Check if specified location is  for current node */
bool fio_is_dss(fio_location location)
{
    return location == FIO_DSS_HOST;
}

/* Try to read specified amount of bytes unless error or EOF are encountered */
ssize_t fio_read_all(int fd, void* buf, size_t size)
{
    size_t offs = 0;
    while (offs < size)
    {
        ssize_t rc = read(fd, (char*)buf + offs, size - offs);
        if (rc < 0)
        {
            if (errno == EINTR)
                continue;
            elog(ERROR, "fio_read_all error, fd %i: %s", fd, strerror(errno));
            return rc;
        }
        else if (rc == 0)
            break;

        offs += rc;
    }
    return offs;
}

/* Try to write specified amount of bytes unless error is encountered */
ssize_t fio_write_all(int fd, void const* buf, size_t size)
{
    size_t offs = 0;
    while (offs < size)
    {
        ssize_t rc = write(fd, (const char*)buf + offs, size - offs);
        if (rc <= 0)
        {
            if (errno == EINTR)
                continue;

            elog(ERROR, "fio_write_all error, fd %i: %s", fd, strerror(errno));

            return rc;
        }
        offs += rc;
    }
    return offs;
}

/* Get version of remote agent */
int fio_get_agent_version(void)
{
    fio_header hdr;
    hdr.cop = FIO_AGENT_VERSION;
    hdr.size = 0;

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

    return hdr.arg;
}

/* Open input stream. Remote file is fetched to the in-memory buffer and then accessed through Linux fmemopen */
FILE* fio_open_stream(char const* path, fio_location location)
{
    FILE* f;
    if (fio_is_remote(location))
    {
        fio_header hdr;
        hdr.cop = FIO_LOAD;
        hdr.size = strlen(path) + 1;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
        Assert(hdr.cop == FIO_SEND);
        if (hdr.size > 0)
        {
            Assert(fio_stdin_buffer == NULL);
            fio_stdin_buffer = pgut_malloc(hdr.size);
            IO_CHECK(fio_read_all(fio_stdin, fio_stdin_buffer, hdr.size), hdr.size);
#ifdef WIN32
            f = tmpfile();
            IO_CHECK(fwrite(f, 1, hdr.size, fio_stdin_buffer), hdr.size);
            SYS_CHECK(fseek(f, 0, SEEK_SET));
#else
            f = fmemopen(fio_stdin_buffer, hdr.size, "r");
#endif
        }
        else
        {
            f = NULL;
        }
    }
    else
    {
        f = fopen(path, "rt");
    }
    return f;
}

/* Close input stream */
int fio_close_stream(FILE* f)
{
    if (fio_stdin_buffer)
    {
        free(fio_stdin_buffer);
        fio_stdin_buffer = NULL;
    }
    return fclose(f);
}

/* Open directory */
DIR* fio_opendir(char const* path, fio_location location)
{
    DIR* dir;
    if (fio_is_remote(location))
    {
        int i;
        fio_header hdr;
        unsigned long mask;

        mask = fio_fdset;
        for (i = 0; (mask & 1) != 0; i++, mask >>= 1);
        if (i == FIO_FDMAX) {
                elog(ERROR, "Descriptor pool for remote files is exhausted, "
                    "probably too many remote directories are opened");
        }
        hdr.cop = FIO_OPENDIR;
        hdr.handle = i;
        hdr.size = strlen(path) + 1;
        fio_fdset |= 1 << i;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

        if (hdr.arg != 0)
        {
            errno = hdr.arg;
            fio_fdset &= ~(1 << hdr.handle);
            return NULL;
        }
        dir = (DIR*)(size_t)(i + 1);
    }
    else
    {
        dir = opendir(path);
    }
    return dir;
}

/* Get next directory entry */
struct dirent* fio_readdir(DIR *dir)
{
    if (fio_is_remote_file((FILE*)dir))
    {
        fio_header hdr;
        static __thread struct dirent entry;

        hdr.cop = FIO_READDIR;
        hdr.handle = (size_t)dir - 1;
        hdr.size = 0;
        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
        Assert(hdr.cop == FIO_SEND);
        if (hdr.size) {
            Assert(hdr.size == sizeof(entry));
            IO_CHECK(fio_read_all(fio_stdin, &entry, sizeof(entry)), sizeof(entry));
        }

        return hdr.size ? &entry : NULL;
    }
    else
    {
        return readdir(dir);
    }
}

/* Close directory */
int fio_closedir(DIR *dir)
{
    if (fio_is_remote_file((FILE*)dir))
    {
        fio_header hdr;
        hdr.cop = FIO_CLOSEDIR;
        hdr.handle = (size_t)dir - 1;
        hdr.size = 0;
        fio_fdset &= ~(1 << hdr.handle);

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        return 0;
    }
    else
    {
        return closedir(dir);
    }
}

/* Open file */
int fio_open(char const* path, int mode, fio_location location)
{
    int fd;
    if (fio_is_remote(location))
    {
        int i;
        fio_header hdr;
        unsigned long mask;

        mask = fio_fdset;
        for (i = 0; (mask & 1) != 0; i++, mask >>= 1);
        if (i == FIO_FDMAX)
            elog(ERROR, "Descriptor pool for remote files is exhausted, "
                "probably too many remote files are opened");

        hdr.cop = FIO_OPEN;
        hdr.handle = i;
        hdr.size = strlen(path) + 1;
        hdr.arg = mode;
        
        fio_fdset |= 1 << i;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, path, hdr.size), hdr.size);

        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

        if (hdr.arg != 0)
        {
            errno = hdr.arg;
            fio_fdset &= ~(1 << hdr.handle);
            return -1;
        }
        fd = i | FIO_PIPE_MARKER;
    }
    else
    {
        fd = open(path, mode, FILE_PERMISSIONS);
    }
    return fd;
}


/* Close ssh session */
void
fio_disconnect(void)
{
    if (fio_stdin)
    {
        fio_header hdr;
        hdr.cop = FIO_DISCONNECT;
        hdr.size = 0;
        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
        Assert(hdr.cop == FIO_DISCONNECTED);
        SYS_CHECK(close(fio_stdin));
        SYS_CHECK(close(fio_stdout));
        SYS_CHECK(close(fio_stderr));
        fio_stdin = 0;
        fio_stdout = 0;
        fio_stderr = 0;
        wait_ssh();
    }
}

/* Open stdio file */
FILE* fio_fopen(char const* path, char const* mode, fio_location location)
{
    FILE     *f = NULL;

    if (fio_is_remote(location))
    {
        int flags = 0;
        int fd;
        if (strcmp(mode, PG_BINARY_W) == 0) {
            flags = O_TRUNC|PG_BINARY|O_RDWR|O_CREAT;
        } else if (strcmp(mode, "w") == 0) {
            flags = O_TRUNC|O_RDWR|O_CREAT;
        } else if (strcmp(mode, PG_BINARY_R) == 0) {
            flags = O_RDONLY|PG_BINARY;
        } else if (strcmp(mode, "r") == 0) {
            flags = O_RDONLY;
        } else if (strcmp(mode, PG_BINARY_R "+") == 0) {
        /* stdio fopen("rb+") actually doesn't create unexisted file, but probackup frequently
        * needs to open existed file or create new one if not exists.
        * In stdio it can be done using two fopen calls: fopen("r+") and if failed then fopen("w").
        * But to eliminate extra call which especially critical in case of remote connection
        * we change r+ semantic to create file if not exists.
        */
            flags = O_RDWR|O_CREAT|PG_BINARY;
        } else if (strcmp(mode, "r+") == 0) { /* see comment above */
            flags |= O_RDWR|O_CREAT;
        } else if (strcmp(mode, "a") == 0) {
            flags |= O_CREAT|O_RDWR|O_APPEND;
        } else {
            Assert(false);
        }
        fd = fio_open(path, flags, location);
        if (fd >= 0)
            f = (FILE*)(size_t)((fd + 1) & ~FIO_PIPE_MARKER);
    }
    else
    {
        f = fopen(path, mode);
        if (f == NULL && strcmp(mode, PG_BINARY_R "+") == 0)
            f = fopen(path, PG_BINARY_W);
    }
    return f;
}

int fio_fprintf(FILE* f, char const* format, ...) __attribute__ ((format (printf, 2, 3)));
static char *ProcessErrorIn(int out, fio_header &hdr, const char *fromFullpath);
/* Format output to file stream */
int fio_fprintf(FILE* f, char const* format, ...)
{
    int rc;
    va_list args;
    va_start (args, format);
    if (fio_is_remote_file(f))
    {
        char buf[PRINTF_BUF_SIZE];
#ifdef HAS_VSNPRINTF
        rc = vsnprintf_s(buf, sizeof(buf), sizeof(buf) - 1, format,  args);
        securec_check_ss_c(rc, "\0", "\0");
#else
        rc = vsnprintf_s(buf, sizeof(buf), sizeof(buf) - 1, format,  args);
        securec_check_ss_c(rc, "\0", "\0");
#endif
        if (rc > 0) {
            fio_fwrite(f, buf, rc);
        }
    }
    else
    {
        rc = vfprintf(f, format, args);
    }
    va_end (args);
    return rc;
}

/* Flush stream data (does nothing for remote file and dss file) */
int fio_fflush(FILE* f)
{
    int rc = 0;
    if (!fio_is_remote_file(f))
        rc = fflush(f);
    return rc;
}

/* Sync file to the disk (does nothing for remote file) */
int fio_flush(int fd)
{
    return fio_is_remote_fd(fd) ? 0 : fsync(fd);
}

/* Close output stream */
int fio_fclose(FILE* f)
{
    return fio_is_remote_file(f)
            ? fio_close(fio_fileno(f))
            : fclose(f);
}

/* Close file */
int fio_close(int fd)
{
    if (fio_is_remote_fd(fd))
    {
        fio_header hdr;

        hdr.cop = FIO_CLOSE;
        hdr.handle = fd & ~FIO_PIPE_MARKER;
        hdr.size = 0;
        fio_fdset &= ~(1 << hdr.handle);

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        /* Note, that file is closed without waiting for confirmation */

        return 0;
    }
    else
    {
        return close(fd);
    }
}

/* Truncate stdio file */
int fio_ftruncate(FILE* f, off_t size)
{
    return fio_is_remote_file(f)
    ? fio_truncate(fio_fileno(f), size)
    : ftruncate(fileno(f), size);
}

/* Truncate file */
int fio_truncate(int fd, off_t size)
{
    if (fio_is_remote_fd(fd))
    {
        fio_header hdr;

        hdr.cop = FIO_TRUNCATE;
        hdr.handle = fd & ~FIO_PIPE_MARKER;
        hdr.size = 0;
        hdr.arg = size;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

        return 0;
    }
    else
    {
        return ftruncate(fd, size);
    }
}


/*
 * Read file from specified location.
 */
int fio_pread(FILE* f, void* buf, off_t offs, PageCompression* pageCompression, int size)
{
    if (fio_is_remote_file(f))
    {
        int fd = fio_fileno(f);
        fio_header hdr;

        hdr.cop = FIO_PREAD;
        hdr.handle = fd & ~FIO_PIPE_MARKER;
        hdr.size = 0;
        hdr.arg = offs;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
        Assert(hdr.cop == FIO_SEND);
        if (hdr.size != 0 && hdr.size <= BLCKSZ)
        IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);

        /* TODO: error handling */

        return hdr.arg;
    }
    else
    {
        /* For local file, opened by fopen, we should use stdio functions */
        if (pageCompression) {
            return (int)pageCompression->ReadCompressedBuffer((BlockNumber)(offs / BLCKSZ), (char*)buf, size, true);
        } else {
            int rc = fseek(f, offs, SEEK_SET);
            if (rc < 0) {
                return rc;
            }
            return fread(buf, 1, size, f);
        }
    }
}

/* Set position in stdio file */
int fio_fseek(FILE* f, off_t offs)
{
    return fio_is_remote_file(f)
            ? fio_seek(fio_fileno(f), offs)
            : fseek(f, offs, SEEK_SET);
}

/* Set position in file */
int fio_seek(int fd, off_t offs)
{
    if (fio_is_remote_fd(fd))
    {
        fio_header hdr;

        hdr.cop = FIO_SEEK;
        hdr.handle = fd & ~FIO_PIPE_MARKER;
        hdr.size = 0;
        hdr.arg = offs;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

        return 0;
    }
    else
    {
        return lseek(fd, offs, SEEK_SET);
    }
}

/* Write data to stdio file */
size_t fio_fwrite(FILE* f, void const* buf, size_t size)
{
    if (fio_is_remote_file(f))
    {
        return (size_t)fio_write(fio_fileno(f), buf, size);
    }
    else if (is_dss_file_dec(f))
    {
        /* size must be multiples of ALIGNOF_BUFFER in dss */
        char align_buf[size] __attribute__((__aligned__(ALIGNOF_BUFFER))); /* need to be aligned */
        errno_t rc = memcpy_s(align_buf, size, buf, size);
        securec_check_c(rc, "\0", "\0");
        return dss_fwrite_file(align_buf, 1, size, f);
    }
    else
    {
        return fwrite(buf, 1, size, f);
    }
}

/* Write data to the file */
ssize_t fio_write(int fd, void const* buf, size_t size)
{
    if (fio_is_remote_fd(fd))
    {
        fio_header hdr;

        hdr.cop = FIO_WRITE;
        hdr.handle = fd & ~FIO_PIPE_MARKER;
        hdr.size = size;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, buf, size), size);

        return size;
    }
    else
    {
        return write(fd, buf, size);
    }
}

int32
fio_decompress(void* dst, void const* src, size_t size, int compress_alg)
{
    const char *errormsg = NULL;
    int32 uncompressed_size = do_decompress(dst, BLCKSZ, src, size, (CompressAlg)compress_alg, &errormsg);
    if (uncompressed_size < 0 && errormsg != NULL)
    {
        elog(WARNING, "An error occured during decompressing block: %s", errormsg);
        return -1;
    }

    if (uncompressed_size != BLCKSZ)
    {
        elog(ERROR, "Page uncompressed to %d bytes != BLCKSZ",
        uncompressed_size);
        return -1;
    }
    return uncompressed_size;
}

/* Write data to the file */
ssize_t fio_fwrite_compressed(FILE* f, void const* buf, size_t size, int compress_alg, const char *to_fullpath, char *preWriteBuf, int *preWriteOff, int *targetSize)
{
    if (fio_is_remote_file(f))
    {
        fio_header hdr;

        hdr.cop = FIO_WRITE_COMPRESSED;
        hdr.handle = fio_fileno(f) & ~FIO_PIPE_MARKER;
        hdr.size = size;
        hdr.arg = compress_alg;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, buf, size), size);

        return size;
    }
    else
    {
        if (preWriteBuf != NULL)
        {
            int32 uncompressed_size = fio_decompress(preWriteBuf + (*preWriteOff), buf, size, compress_alg);
            *preWriteOff += uncompressed_size;
            if (*preWriteOff > DSS_BLCKSZ)
            {
                pg_free(preWriteBuf);
                elog(ERROR, "Offset %d is bigger than preWriteBuf size %d", *preWriteOff, DSS_BLCKSZ);
            }
            if (*preWriteOff == DSS_BLCKSZ)
            {
                int write_len = fio_fwrite(f, preWriteBuf, DSS_BLCKSZ);
                if (write_len != DSS_BLCKSZ)
                {
                    pg_free(preWriteBuf);
                    elog(ERROR, "Cannot write block of \"%s\": %s, size: %u",
                        to_fullpath, strerror(errno), DSS_BLCKSZ);
                }
                *preWriteOff = 0;
                *targetSize -= DSS_BLCKSZ;
            }
            return uncompressed_size;
        }
        else
        {
            char uncompressed_buf[BLCKSZ];
            int32 uncompressed_size = fio_decompress(uncompressed_buf, buf, size, compress_alg);
            return (uncompressed_size < 0)
                ? uncompressed_size
                : fio_fwrite(f, uncompressed_buf, uncompressed_size);
        }
    }
}

void fio_construct_compressed(void const *buf, size_t size)
{
    fio_header hdr;
    hdr.cop = (uint32)FIO_COSTRUCT_COMPRESSED;
    hdr.size = (unsigned)size;
    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, buf, size), size);
}

static ssize_t
fio_write_compressed_impl(int fd, void const* buf, size_t size, int compress_alg)
{
    char uncompressed_buf[BLCKSZ];
    int32 uncompressed_size = fio_decompress(uncompressed_buf, buf, size, compress_alg);
    return fio_write_all(fd, uncompressed_buf, uncompressed_size);
}

/* Read data from stdio file */
ssize_t fio_fread(FILE* f, void* buf, size_t size)
{
    size_t rc;
    if (fio_is_remote_file(f))
        return fio_read(fio_fileno(f), buf, size);
    rc = fread(buf, 1, size, f);
        return rc == 0 && !feof(f) ? -1 : rc;
}

/* Read data from file */
ssize_t fio_read(int fd, void* buf, size_t size)
{
    if (fio_is_remote_fd(fd))
    {
        fio_header hdr;

        hdr.cop = FIO_READ;
        hdr.handle = fd & ~FIO_PIPE_MARKER;
        hdr.size = 0;
        hdr.arg = size;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));

        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
        Assert(hdr.cop == FIO_SEND);
        if (hdr.size > 0 && hdr.size <= size)
        IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);

        return hdr.size;
    }
    else
    {
        return read(fd, buf, size);
    }
}

/* Get information about file */
int fio_stat(char const* path, struct stat* st, bool follow_symlink, fio_location location)
{
    if (fio_is_remote(location))
    {
        fio_header hdr;
        size_t path_len = strlen(path) + 1;

        hdr.cop = FIO_STAT;
        hdr.handle = -1;
        hdr.arg =(unsigned) follow_symlink;
        hdr.size = path_len;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
        Assert(hdr.cop == FIO_STAT);
        IO_CHECK(fio_read_all(fio_stdin, st, sizeof(*st)), sizeof(*st));

        if (hdr.arg != 0)
        {
            errno = hdr.arg;
            return -1;
        }
        return 0;
    }
    else
    {
        return follow_symlink ? stat(path, st) : lstat(path,  st);
    }
}

/* Check presence of the file */
int fio_access(char const* path, int mode, fio_location location)
{
    if (fio_is_remote(location))
    {
        fio_header hdr;
        size_t path_len = strlen(path) + 1;
        hdr.cop = FIO_ACCESS;
        hdr.handle = -1;
        hdr.size = path_len;
        hdr.arg = mode;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
        Assert(hdr.cop == FIO_ACCESS);

        if (hdr.arg != 0)
        {
            errno = hdr.arg;
            return -1;
        }
        return 0;
    }
    else
    {
        return access(path, mode);
    }
}

/* Create symbolic link */
int fio_symlink(char const* target, char const* link_path, bool overwrite, fio_location location)
{
    if (fio_is_remote(location))
    {
        fio_header hdr;
        size_t target_len = strlen(target) + 1;
        size_t link_path_len = strlen(link_path) + 1;
        hdr.cop = FIO_SYMLINK;
        hdr.handle = -1;
        hdr.size = target_len + link_path_len;
        hdr.arg = overwrite ? 1 : 0;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, target, target_len), target_len);
        IO_CHECK(fio_write_all(fio_stdout, link_path, link_path_len), link_path_len);

        return 0;
    }
    else
    {
        if (overwrite)
            remove_file_or_dir(link_path);

        return symlink(target, link_path);
    }
}

static void fio_symlink_impl(int out, char *buf, bool overwrite)
{
    char *linked_path = buf;
    char *link_path = buf + strlen(buf) + 1;

    if (overwrite)
        remove_file_or_dir(link_path);

    if (symlink(linked_path, link_path))
        elog(ERROR, "Could not create symbolic link \"%s\": %s",
            link_path, strerror(errno));
}

/* Rename file */
int fio_rename(char const* old_path, char const* new_path, fio_location location)
{
    if (fio_is_remote(location))
    {
        fio_header hdr;
        size_t old_path_len = strlen(old_path) + 1;
        size_t new_path_len = strlen(new_path) + 1;
        hdr.cop = FIO_RENAME;
        hdr.handle = -1;
        hdr.size = old_path_len + new_path_len;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, old_path, old_path_len), old_path_len);
        IO_CHECK(fio_write_all(fio_stdout, new_path, new_path_len), new_path_len);

        //TODO: wait for confirmation.

        return 0;
    }
    else
    {
        return rename(old_path, new_path);
    }
}

/* Sync file to disk */
int fio_sync(char const* path, fio_location location)
{
    if (fio_is_remote(location))
    {
        fio_header hdr;
        size_t path_len = strlen(path) + 1;
        hdr.cop = FIO_SYNC;
        hdr.handle = -1;
        hdr.size = path_len;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);
        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

        if (hdr.arg != 0)
        {
            errno = hdr.arg;
            return -1;
        }

        return 0;
    }
    else if (is_dss_file(path))
    {
        /* nothing to do in dss mode, data are already sync to disk */
        return 0;
    }
    else
    {
        int fd;

        fd = open(path, O_WRONLY | PG_BINARY, FILE_PERMISSIONS);
        if (fd < 0)
        return -1;

        if (fsync(fd) < 0)
        {
            close(fd);
            return -1;
        }
        close(fd);

        return 0;
    }
}

/* Get crc32 of file */
pg_crc32 fio_get_crc32(const char *file_path, fio_location location, bool decompress)
{
    if (fio_is_remote(location))
    {
        fio_header hdr;
        size_t path_len = strlen(file_path) + 1;
        pg_crc32 crc = 0;
        hdr.cop = FIO_GET_CRC32;
        hdr.handle = -1;
        hdr.size = path_len;
        hdr.arg = 0;

        if (decompress)
            hdr.arg = 1;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, file_path, path_len), path_len);
        IO_CHECK(fio_read_all(fio_stdin, &crc, sizeof(crc)), sizeof(crc));

        return crc;
    }
    else
    {
#ifdef HAVE_LIBZ                        
        if (decompress && !IsDssMode())
            return pgFileGetCRCgz(file_path, true, true);
        else
#endif
            return pgFileGetCRC(file_path, true, true);
    }
}

/* Remove file */
int fio_unlink(char const* path, fio_location location)
{
    if (fio_is_remote(location))
    {
        fio_header hdr;
        size_t path_len = strlen(path) + 1;
        hdr.cop = FIO_UNLINK;
        hdr.handle = -1;
        hdr.size = path_len;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

        // TODO: error is swallowed ?
        return 0;
    }
    else
    {
        return remove_file_or_dir(path);
    }
}

/* Create directory */
int fio_mkdir(const char* path, int mode, fio_location location)
{
    if (fio_is_remote(location))
    {
        fio_header hdr;
        size_t path_len = strlen(path) + 1;
        hdr.cop = FIO_MKDIR;
        hdr.handle = -1;
        hdr.size = path_len;
        hdr.arg = mode;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
        Assert(hdr.cop == FIO_MKDIR);

        return hdr.arg;
    }
    else
    {
        /* operate is same in local mode and dss mode */
        return dir_create_dir(path, mode);
    }
}

/* Change file mode */
int fio_chmod(char const* path, int mode, fio_location location)
{
    if (fio_is_remote(location))
    {
        fio_header hdr;
        size_t path_len = strlen(path) + 1;
        hdr.cop = FIO_CHMOD;
        hdr.handle = -1;
        hdr.size = path_len;
        hdr.arg = mode;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, path, path_len), path_len);

        return 0;
    }
    else
    {
        return chmod(path, mode);
    }
}


/* Send file content
 * Note: it should not be used for large files.
 */
static void fio_load_file(int out, char const* path)
{
    int fd = open(path, O_RDONLY, 0);
    fio_header hdr;
    void* buf = NULL;

    hdr.cop = FIO_SEND;
    hdr.size = 0;

    if (fd >= 0)
    {
        off_t size = lseek(fd, 0, SEEK_END);
        buf = pgut_malloc(size);
        lseek(fd, 0, SEEK_SET);
        IO_CHECK(fio_read_all(fd, buf, size), size);
        hdr.size = size;
        SYS_CHECK(close(fd));
    }
    IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
    if (buf)
    {
        IO_CHECK(fio_write_all(out, buf, hdr.size), hdr.size);
        free(buf);
    }
}

/*
 * Return number of actually(!) readed blocks, attempts or
 * half-readed block are not counted.
 * Return values in case of error:
 *  FILE_MISSING
 *  OPEN_FAILED
 *  READ_ERROR
 *  PAGE_CORRUPTION
 *  WRITE_FAILED
 *
 * If none of the above, this function return number of blocks
 * readed by remote agent.
 *
 * In case of DELTA mode horizonLsn must be a valid lsn,
 * otherwise it should be set to InvalidXLogRecPtr.
 */
int fio_send_pages(const char *to_fullpath, const char *from_fullpath, pgFile *file,
                                    XLogRecPtr horizonLsn, int calg, int clevel, uint32 checksum_version,
                                    bool use_pagemap, BlockNumber* err_blknum, char **errormsg,
                                    BackupPageHeader2 **headers)
{
    FILE *out = NULL;
    char *out_buf = NULL;
    struct {
        fio_header hdr;
        fio_send_request arg;
    } req;
    BlockNumber n_blocks_read = 0;
    BlockNumber blknum = 0;
    int nRet = 0;

    /* send message with header

    8bytes       24bytes             var        var
    --------------------------------------------------------------
    | fio_header | fio_send_request | FILE PATH | BITMAP(if any) |
    --------------------------------------------------------------
    */

    req.hdr.cop = FIO_SEND_PAGES;

    if (use_pagemap)
    {
        req.hdr.size = sizeof(fio_send_request) + (*file).pagemap.bitmapsize + strlen(from_fullpath) + 1;
        req.arg.bitmapsize = (*file).pagemap.bitmapsize;

        /* TODO: add optimization for the case of pagemap
        * containing small number of blocks with big serial numbers:
        * https://github.com/postgrespro/pg_probackup/blob/remote_page_backup/src/utils/file.c#L1211
        */
    }
    else
    {
        req.hdr.size = sizeof(fio_send_request) + strlen(from_fullpath) + 1;
        req.arg.bitmapsize = 0;
    }

    req.arg.nblocks = file->size/BLCKSZ;
    req.arg.segmentno = file->segno * RELSEG_SIZE;
    req.arg.horizonLsn = horizonLsn;
    req.arg.checksumVersion = checksum_version;
    req.arg.calg = calg;
    req.arg.clevel = clevel;
    req.arg.path_len = strlen(from_fullpath) + 1;

    file->compress_alg = (CompressAlg)calg; /* TODO: wtf? why here? */



    /* send header */
    IO_CHECK(fio_write_all(fio_stdout, &req, sizeof(req)), sizeof(req));

    /* send file path */
    IO_CHECK(fio_write_all(fio_stdout, from_fullpath, req.arg.path_len), req.arg.path_len);

    /* send pagemap if any */
    if (use_pagemap)
        IO_CHECK(fio_write_all(fio_stdout, (*file).pagemap.bitmap, (*file).pagemap.bitmapsize), (*file).pagemap.bitmapsize);

    while (true)
    {
        fio_header hdr;
        char buf[BLCKSZ + sizeof(BackupPageHeader)];
        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

        if (interrupted)
            elog(ERROR, "Interrupted during page reading");

        if (hdr.cop == FIO_ERROR)
        {
            bool valid = hdr.size > 0 && hdr.size <= sizeof(buf);
            /* FILE_MISSING, OPEN_FAILED and READ_FAILED */
            if (valid)
            {
                IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);
                *errormsg = (char *)pgut_malloc(hdr.size);
                nRet = snprintf_s(*errormsg, hdr.size, hdr.size - 1, "%s", buf);
                securec_check_ss_c(nRet, "\0", "\0");
            }

            return hdr.arg;
        }
        else if (hdr.cop == FIO_SEND_FILE_CORRUPTION)
        {
            *err_blknum = hdr.arg;
            bool valid = hdr.size > 0 && hdr.size <= sizeof(buf);
            if (valid)
            {
                IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);
                *errormsg = (char *)pgut_malloc(hdr.size);
                nRet = snprintf_s(*errormsg, hdr.size, hdr.size - 1, "%s", buf);
                securec_check_ss_c(nRet, "\0", "\0");
            }
            return PAGE_CORRUPTION;
        }
        else if (hdr.cop == FIO_SEND_FILE_EOF)
        {
            /* n_blocks_read reported by EOF */
            n_blocks_read = hdr.arg;

            /* receive headers if any */
            if (hdr.size > 0)
            {
                *headers = (BackupPageHeader2 *)pgut_malloc(hdr.size);
                IO_CHECK(fio_read_all(fio_stdin, *headers, hdr.size), hdr.size);
                file->n_headers = (hdr.size / sizeof(BackupPageHeader2)) -1;
            }

            break;
        }
        else if (hdr.cop == FIO_PAGE)
        {
            blknum = hdr.arg;

            Assert(hdr.size <= sizeof(buf));
            if (hdr.size > sizeof(buf)) {
                hdr.size = sizeof(buf);
            }
            IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);

            COMP_FILE_CRC32(true, file->crc, buf, hdr.size);

            /* lazily open backup file */
            if (!out)
                out = open_local_file_rw(to_fullpath, &out_buf, STDIO_BUFSIZE);

            if (fio_fwrite(out, buf, hdr.size) != hdr.size)
            {
                fio_fclose(out);
                *err_blknum = blknum;
                return WRITE_FAILED;
            }
            file->write_size += hdr.size;
            file->uncompressed_size += BLCKSZ;
        }
        else
            elog(ERROR, "Remote agent returned message of unexpected type: %i", hdr.cop);
        }

        if (out)
            fclose(out);
        pg_free(out_buf);

        return n_blocks_read;
}

/* TODO: read file using large buffer
 * Return codes:
 *  FIO_ERROR:
 *  FILE_MISSING (-1)
 *  OPEN_FAILED  (-2)
 *  READ_FAILED  (-3)

 *  FIO_SEND_FILE_CORRUPTION
 *  FIO_SEND_FILE_EOF
 */
static void fio_send_pages_impl(int out, char* buf)
{
    FILE        *in = NULL;
    BlockNumber  blknum = 0;
    int          current_pos = 0;
    BlockNumber  n_blocks_read = 0;
    PageState    page_st;
    char         read_buffer[BLCKSZ+1];
    char         in_buf[STDIO_BUFSIZE];
    fio_header   hdr;
    int hdr_num;
    parray *harray = NULL;
    fio_send_request *req = (fio_send_request*) buf;
    char             *from_fullpath = (char*) buf + sizeof(fio_send_request);
    bool with_pagemap = req->bitmapsize > 0;
    /* error reporting */
    char *errormsg = NULL;
    /* parse buffer */
    datapagemap_t *map = NULL;
    datapagemap_iterator_t *iter = NULL;
    /* page headers */
    int32       cur_pos_out = 0;
    BackupPageHeader2 *headers = NULL;
    PageCompression* pageCompression = NULL;
    int nRet = 0;

    if (PageCompression::SkipCompressedFile(from_fullpath, strlen(from_fullpath))) {
        /* init pageCompression and return pcdFd for error check */
        pageCompression = new(std::nothrow) PageCompression();
        if (pageCompression == NULL) {
            elog(ERROR, "Decompression page init failed");
            return;
        }
        COMPRESS_ERROR_STATE result = pageCompression->Init(from_fullpath, req->segmentno / RELSEG_SIZE);
        if (result == SUCCESS) {
            in = pageCompression->GetCompressionFile();
        } else {
            delete pageCompression;
            pageCompression = NULL;
            elog(ERROR, "Decompression page init failed \"%s\": %d", from_fullpath, (int)result);
            return;
        }
    } else {
        /* open source file */
        in = fopen(from_fullpath, PG_BINARY_R);
    }

    if (!in)
    {
        errormsg = ProcessErrorIn(out, hdr, from_fullpath);
        goto cleanup;
    }

    if (with_pagemap)
    {
        map = (datapagemap_t *)pgut_malloc(sizeof(datapagemap_t));
        map->bitmapsize = req->bitmapsize;
        map->bitmap = (unsigned char*) buf + sizeof(fio_send_request) + req->path_len;

        /* get first block */
        iter = datapagemap_iterate(map);
        datapagemap_next(iter, &blknum);

        setvbuf(in, NULL, _IONBF, BUFSIZ);
    }
    else
        setvbuf(in, in_buf, _IOFBF, STDIO_BUFSIZE);

    /* TODO: what is this barrier for? */
    read_buffer[BLCKSZ] = 1; /* barrier */

    harray = parray_new();
    while (blknum < req->nblocks)
    {
        int    rc = 0;
        size_t read_len = 0;
        int    retry_attempts = PAGE_READ_ATTEMPTS;

        /* TODO: handle signals on the agent */
        if (interrupted)
            elog(ERROR, "Interrupted during remote page reading");

        /* read page, check header and validate checksumms */
        for (;;)
        {
            if (pageCompression) {
                read_len = pageCompression->ReadCompressedBuffer(blknum, read_buffer, BLCKSZ, true);
                if (read_len > MIN_COMPRESS_ERROR_RT) {
                    elog(ERROR, "can not read actual block %u, error code: %lu,", blknum, read_len);
                }
            } else {
                /*
                * Optimize stdio buffer usage, fseek only when current position
                * does not match the position of requested block.
                */
                if (current_pos != (int)(blknum * BLCKSZ))
                {
                    current_pos = blknum*BLCKSZ;
                    if (fseek(in, current_pos, SEEK_SET) != 0)
                        elog(ERROR, "fseek to position %u is failed on remote file '%s': %s",
                             current_pos, from_fullpath, strerror(errno));
                }

                read_len = fread(read_buffer, 1, BLCKSZ, in);

                current_pos += read_len;
            }

            /* report error */
            if (ferror(in))
            {
                hdr.cop = FIO_ERROR;
                hdr.arg = READ_FAILED;

                errormsg = (char *)pgut_malloc(ERRMSG_MAX_LEN);
                /* Construct the error message */
                nRet = snprintf_s(errormsg, ERRMSG_MAX_LEN,ERRMSG_MAX_LEN - 1, "Cannot read block %u of '%s': %s",
                                blknum, from_fullpath, strerror(errno));
                securec_check_ss_c(nRet, "\0", "\0");
                hdr.size = strlen(errormsg) + 1;

                /* send header and message */
                IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
                IO_CHECK(fio_write_all(out, errormsg, hdr.size), hdr.size);
                goto cleanup;
            }

            if (read_len == BLCKSZ)
            {
                rc = validate_one_page(read_buffer, req->segmentno + blknum,
                                                        InvalidXLogRecPtr, &page_st,
                                                        req->checksumVersion);
                if (rc == PAGE_MAYBE_COMPRESSED && pageCompression != NULL) {
                    rc = PAGE_IS_VALID;
                }

                /* TODO: optimize copy of zeroed page */
                if (rc == PAGE_IS_ZEROED)
                    break;
                else if (rc == PAGE_IS_VALID)
                    break;
            }

            if (feof(in))
                goto eof;
            //  else /* readed less than BLKSZ bytes, retry */

            /* File is either has insane header or invalid checksum,
            * retry. If retry attempts are exhausted, report corruption.
            */
            if (--retry_attempts == 0)
            {
                hdr.cop = FIO_SEND_FILE_CORRUPTION;
                hdr.arg = blknum;

                /* Construct the error message */
                if (rc == PAGE_HEADER_IS_INVALID)
                    get_header_errormsg(read_buffer, &errormsg);
                else if (rc == PAGE_CHECKSUM_MISMATCH)
                    get_checksum_errormsg(read_buffer, &errormsg,
                        req->segmentno + blknum);

                /* if error message is not empty, set payload size to its length */
                hdr.size = errormsg ? strlen(errormsg) + 1 : 0;

                /* send header */
                IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));

                /* send error message if any */
                if (errormsg)
                    IO_CHECK(fio_write_all(out, errormsg, hdr.size), hdr.size);

                goto cleanup;
            }
        }

        n_blocks_read++;

        /*
        * horizonLsn is not 0 only in case of delta backup.
        * As far as unsigned number are always greater or equal than zero,
        * there is no sense to add more checks.
        */
        if ((req->horizonLsn == InvalidXLogRecPtr) ||                 /* full, page, ptrack */
            (page_st.lsn == InvalidXLogRecPtr) ||                     /* zeroed page */
            (req->horizonLsn > 0 && page_st.lsn > req->horizonLsn))   /* delta */
        {
            int  compressed_size = 0;
            char write_buffer[BLCKSZ*2];
            BackupPageHeader* bph = (BackupPageHeader*)write_buffer;

            /* compress page */
            hdr.cop = FIO_PAGE;
            hdr.arg = blknum;

            compressed_size = do_compress(write_buffer + sizeof(BackupPageHeader),
                                                                sizeof(write_buffer) - sizeof(BackupPageHeader),
                                                                read_buffer, BLCKSZ, (CompressAlg)req->calg, req->clevel,
                                                                NULL);

            if (compressed_size <= 0 || compressed_size >= BLCKSZ)
            {
                /* Do not compress page */
                errno_t rtc;
                rtc = memcpy_s(write_buffer + sizeof(BackupPageHeader), BLCKSZ, read_buffer, BLCKSZ);
                securec_check_c(rtc, "\0", "\0");
                compressed_size = BLCKSZ;
            }
            bph->block = blknum;
            bph->compressed_size = compressed_size;

            hdr.size = compressed_size + sizeof(BackupPageHeader);

            IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
            IO_CHECK(fio_write_all(out, write_buffer, hdr.size), hdr.size);

            /* set page header for this file */
            BackupPageHeader2 *header = pgut_new(BackupPageHeader2);
            header->block = blknum;
            header->lsn = page_st.lsn;
            header->checksum = page_st.checksum;
            header->pos = cur_pos_out;
            parray_append(harray, header);

            cur_pos_out += hdr.size;
        }

        /* next block */
        if (with_pagemap)
        {
            /* exit if pagemap is exhausted */
            if (!datapagemap_next(iter, &blknum))
                break;
        }
        else
            blknum++;
    }

eof:
    /* We are done, send eof */
    hdr.cop = FIO_SEND_FILE_EOF;
    hdr.arg = n_blocks_read;
    hdr.size = 0;

    hdr_num = parray_num(harray);
    if (hdr_num > 0) {
        hdr.size = (hdr_num + 1) * sizeof(BackupPageHeader2);
        headers = (BackupPageHeader2 *)pgut_malloc((hdr_num + 1) * sizeof(BackupPageHeader2));
        for (int i = 0; i < hdr_num; i++) {
            auto *header = (BackupPageHeader2 *)parray_get(harray, i);
            headers[i] = *header;
            pg_free(header);
        }
        headers[hdr_num].pos = cur_pos_out;
    }
    parray_free(harray);

    IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));

    if (headers)
        IO_CHECK(fio_write_all(out, headers, hdr.size), hdr.size);

    cleanup:
    pg_free(map);
    pg_free(iter);
    pg_free(errormsg);
    pg_free(headers);
    if (pageCompression) {
        /* in will be closed */
        delete pageCompression;
    } else {
        if (in)
            fclose(in);
    }
    return;
}

static char *ProcessErrorIn(int out, fio_header &hdr, const char *fromFullpath)
{
    char *errormsg = NULL;
    hdr.cop = FIO_ERROR;

    /* do not send exact wording of ENOENT error message
     * because it is a very common error in our case, so
     * error code is enough.
     */
    if (errno == ENOENT) {
        hdr.arg = FILE_MISSING;
        hdr.size = 0;
    } else {
        hdr.arg = OPEN_FAILED;
        errormsg = (char *)pgut_malloc(ERRMSG_MAX_LEN);
        /* Construct the error message */
        error_t nRet = snprintf_s(errormsg, ERRMSG_MAX_LEN, ERRMSG_MAX_LEN - 1, "Cannot open file \"%s\": %s",
                                  fromFullpath, strerror(errno));
        securec_check_ss_c(nRet, "\0", "\0");
        hdr.size = strlen(errormsg) + 1;
    }

    /* send header and message */
    IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
    if (errormsg)
        IO_CHECK(fio_write_all(out, errormsg, hdr.size), hdr.size);
    return errormsg;
}

/* Receive chunks of data and write them to destination file.
 * Return codes:
 *   SEND_OK       (0)
 *   FILE_MISSING (-1)
 *   OPEN_FAILED  (-2)
 *   READ_FAILED  (-3)
 *   WRITE_FAILED (-4)
 *
 * OPEN_FAILED and READ_FAIL should also set errormsg.
 * If pgFile is not NULL then we must calculate crc and read_size for it.
 */
int fio_send_file(const char *from_fullpath, const char *to_fullpath, FILE* out,
                                pgFile *file, char **errormsg)
{
    fio_header hdr;
    int exit_code = SEND_OK;
    size_t path_len = strlen(from_fullpath) + 1;
    char *buf = (char *)pgut_malloc(CHUNK_SIZE);    /* buffer */
    int nRet = 0;

    hdr.cop = FIO_SEND_FILE;
    hdr.size = path_len;

    

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, from_fullpath, path_len), path_len);

    for (;;)
    {
        /* receive data */
        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

        if (hdr.cop == FIO_SEND_FILE_EOF)
        {
            break;
        }
        else if (hdr.cop == FIO_ERROR)
        {
            /* handle error, reported by the agent */
            if (hdr.size > 0 && hdr.size <= CHUNK_SIZE)
            {
                IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);
                *errormsg = (char *)pgut_malloc(hdr.size);
                nRet = snprintf_s(*errormsg, hdr.size, hdr.size - 1, "%s", buf);
                securec_check_ss_c(nRet, "\0", "\0");
            }
            exit_code = hdr.arg;
            break;
        }
        else if (hdr.cop == FIO_PAGE)
        {
            Assert(hdr.size <= CHUNK_SIZE);
            if (hdr.size > CHUNK_SIZE) {
                hdr.size = CHUNK_SIZE;
            }
            IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);

            /* We have received a chunk of data data, lets write it out */
            if (fwrite(buf, 1, hdr.size, out) != hdr.size)
            {
                exit_code = WRITE_FAILED;
                break;
            }

            if (file)
            {
                file->read_size += hdr.size;
                COMP_FILE_CRC32(true, file->crc, buf, hdr.size);
            }
        }
        else
        {
            /* TODO: fio_disconnect may get assert fail when running after this */
            elog(ERROR, "Remote agent returned message of unexpected type: %i", hdr.cop);
        }
    }

    if (exit_code < OPEN_FAILED)
        fio_disconnect(); /* discard possible pending data in pipe */

    pg_free(buf);
    return exit_code;
}

/* Send file content
 * On error we return FIO_ERROR message with following codes
 *  FIO_ERROR:
 *      FILE_MISSING (-1)
 *      OPEN_FAILED  (-2)
 *      READ_FAILED  (-3)
 *
 *  FIO_PAGE
 *  FIO_SEND_FILE_EOF
 *
 */
static void fio_send_file_impl(int out, char const* path)
{
    FILE      *fp;
    fio_header hdr;
    char      *buf = (char *)pgut_malloc(CHUNK_SIZE);
    size_t  read_len = 0;
    char      *errormsg = NULL;
    int nRet = 0;

    /* open source file for read */
    /* TODO: check that file is regular file */
    fp = fopen(path, PG_BINARY_R);
    if (!fp)
    {
        hdr.cop = FIO_ERROR;

        /* do not send exact wording of ENOENT error message
        * because it is a very common error in our case, so
        * error code is enough.
        */
        if (errno == ENOENT)
        {
            hdr.arg = FILE_MISSING;
            hdr.size = 0;
        }
        else
        {
            hdr.arg = OPEN_FAILED;
            errormsg = (char *)pgut_malloc(ERRMSG_MAX_LEN);
            /* Construct the error message */
            nRet = snprintf_s(errormsg, ERRMSG_MAX_LEN, ERRMSG_MAX_LEN - 1, "Cannot open file '%s': %s", path, strerror(errno));
            securec_check_ss_c(nRet, "\0", "\0");
            hdr.size = strlen(errormsg) + 1;
        }

        /* send header and message */
        IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
        if (errormsg)
            IO_CHECK(fio_write_all(out, errormsg, hdr.size), hdr.size);

        goto cleanup;
    }

    /* disable stdio buffering */
    setvbuf(fp, NULL, _IONBF, BUFSIZ);

    /* copy content */
    for (;;)
    {
        read_len = fread(buf, 1, CHUNK_SIZE, fp);

        /* report error */
        if (ferror(fp))
        {
            hdr.cop = FIO_ERROR;
            errormsg = (char *)pgut_malloc(ERRMSG_MAX_LEN);
            hdr.arg = READ_FAILED;
            /* Construct the error message */
            nRet = snprintf_s(errormsg, ERRMSG_MAX_LEN, ERRMSG_MAX_LEN -1,  "Cannot read from file '%s': %s", path, strerror(errno));
            securec_check_ss_c(nRet, "\0", "\0");
            hdr.size = strlen(errormsg) + 1;
            /* send header and message */
            IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
            IO_CHECK(fio_write_all(out, errormsg, hdr.size), hdr.size);

            goto cleanup;
        }

        if (read_len > 0)
        {
            /* send chunk */
            hdr.cop = FIO_PAGE;
            hdr.size = read_len;
            IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
            IO_CHECK(fio_write_all(out, buf, read_len), read_len);
        }

        if (feof(fp))
            break;
    }

    /* we are done, send eof */
    hdr.cop = FIO_SEND_FILE_EOF;
    IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));

cleanup:
    if (fp)
        fclose(fp);
    pg_free(buf);
    pg_free(errormsg);
    return;
}

/* Compile the array of files located on remote machine in directory root */
void fio_list_dir(parray *files, const char *root, bool exclude,
                            bool follow_symlink, bool add_root, bool backup_logs,
                            bool skip_hidden, int external_dir_num, bool backup_replslots)
{
    fio_header hdr;
    fio_list_dir_request req;
    int nRet = 0;
    char *buf = (char *)pgut_malloc(CHUNK_SIZE);

    /* Send to the agent message with parameters for directory listing */
    nRet = snprintf_s(req.path, MAXPGPATH,MAXPGPATH - 1, "%s", root);
    securec_check_ss_c(nRet, "\0", "\0");
    req.exclude = exclude;
    req.follow_symlink = follow_symlink;
    req.add_root = add_root;
    req.backup_logs = backup_logs;
    req.exclusive_backup = exclusive_backup;
    req.skip_hidden = skip_hidden;
    req.external_dir_num = external_dir_num;
    req.backup_replslots = backup_replslots;

    hdr.cop = FIO_LIST_DIR;
    hdr.size = sizeof(req);

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, &req, hdr.size), hdr.size);

    for (;;)
    {
        /* receive data */
        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

        if (hdr.cop == FIO_SEND_FILE_EOF)
        {
            /* the work is done */
            break;
        }
        else if (hdr.cop == FIO_SEND_FILE)
        {
            pgFile *file = NULL;
            fio_pgFile  fio_file;

            if (hdr.size > CHUNK_SIZE) {
                hdr.size = CHUNK_SIZE;
            }
            /* receive rel_path */
            IO_CHECK(fio_read_all(fio_stdin, buf, hdr.size), hdr.size);
            file = pgFileInit(buf);

            /* receive metainformation */
            IO_CHECK(fio_read_all(fio_stdin, &fio_file, sizeof(fio_file)), sizeof(fio_file));

            file->mode = fio_file.mode;
            file->size = fio_file.size;
            file->mtime = fio_file.mtime;
            file->is_datafile = fio_file.is_datafile;
            file->is_database = fio_file.is_database;
            file->tblspcOid = fio_file.tblspcOid;
            file->dbOid = fio_file.dbOid;
            file->relOid = fio_file.relOid;
            file->forkName = fio_file.forkName;
            file->segno = fio_file.segno;
            file->external_dir_num = fio_file.external_dir_num;
            file->compressed_file = fio_file.compressed_file;
            file->compressed_chunk_size = fio_file.compressed_chunk_size;
            file->compressed_algorithm = fio_file.compressed_algorithm;

            if (fio_file.linked_len > 0)
            {
                if (fio_file.linked_len > CHUNK_SIZE) {
                    fio_file.linked_len = CHUNK_SIZE;
                }
                IO_CHECK(fio_read_all(fio_stdin, buf, fio_file.linked_len), fio_file.linked_len);

                file->linked = (char *)pgut_malloc(fio_file.linked_len);
                nRet = snprintf_s(file->linked, fio_file.linked_len, fio_file.linked_len - 1,  "%s", buf);
                securec_check_ss_c(nRet, "\0", "\0");
            }

            /* 
             * Check file that under pg_replslot and judge whether it
             * belonged to logical replication slots for subscriptions.
             */
            if (backup_replslots && strcmp(buf, PG_REPLSLOT_DIR) != 0 &&
                path_is_prefix_of_path(PG_REPLSLOT_DIR, buf) && check_logical_replslot_dir(file->rel_path) != 1) {
                continue;
            }

            parray_append(files, file);
        }
        else
        {
            /* TODO: fio_disconnect may get assert fail when running after this */
            elog(ERROR, "Remote agent returned message of unexpected type: %i", hdr.cop);
        }
    }

    pg_free(buf);
}


/*
 * To get the arrays of files we use the same function dir_list_file(),
 * that is used for local backup.
 * After that we iterate over arrays and for every file send at least
 * two messages to main process:
 * 1. rel_path
 * 2. metainformation (size, mtime, etc)
 * 3. link path (optional)
 *
 * TODO: replace FIO_SEND_FILE and FIO_SEND_FILE_EOF with dedicated messages
 */
static void fio_list_dir_impl(int out, char* buf)
{
    int i;
    fio_header hdr;
    fio_list_dir_request *req = (fio_list_dir_request*) buf;
    parray *file_files = parray_new();

    /*
    * Disable logging into console any messages with exception of ERROR messages,
    * because currently we have no mechanism to notify the main process
    * about then message been sent.
    * TODO: correctly send elog messages from agent to main process.
    */
    instance_config.logger.log_level_console = ERROR;
    exclusive_backup = req->exclusive_backup;

    dir_list_file(file_files, req->path, req->exclude, req->follow_symlink,
                        req->add_root, req->backup_logs, req->skip_hidden,
                        req->external_dir_num, FIO_LOCAL_HOST, req->backup_replslots);

    /* send information about files to the main process */
    for (i = 0; i < (int)parray_num(file_files); i++)
    {
        fio_pgFile  fio_file;
        pgFile  *file = (pgFile *) parray_get(file_files, i);

        fio_file.mode = file->mode;
        fio_file.size = file->size;
        fio_file.mtime = file->mtime;
        fio_file.is_datafile = file->is_datafile;
        fio_file.is_database = file->is_database;
        fio_file.tblspcOid = file->tblspcOid;
        fio_file.dbOid = file->dbOid;
        fio_file.relOid = file->relOid;
        fio_file.forkName = file->forkName;
        fio_file.segno = file->segno;
        fio_file.external_dir_num = file->external_dir_num;
        fio_file.compressed_file = file->compressed_file;
        fio_file.compressed_chunk_size = file->compressed_chunk_size;
        fio_file.compressed_algorithm = file->compressed_algorithm;

        if (file->linked)
            fio_file.linked_len = strlen(file->linked) + 1;
        else
            fio_file.linked_len = 0;

        hdr.cop = FIO_SEND_FILE;
        hdr.size = strlen(file->rel_path) + 1;

        /* send rel_path first */
        IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(out, file->rel_path, hdr.size), hdr.size);

        /* now send file metainformation */
        IO_CHECK(fio_write_all(out, &fio_file, sizeof(fio_file)), sizeof(fio_file));

        /* If file is a symlink, then send link path */
        if (file->linked)
            IO_CHECK(fio_write_all(out, file->linked, fio_file.linked_len), fio_file.linked_len);

        pgFileFree(file);
    }

    parray_free(file_files);
    hdr.cop = FIO_SEND_FILE_EOF;
    IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
}

PageState *
fio_get_checksum_map(const char *fullpath, uint32 checksum_version, int n_blocks,
                                                XLogRecPtr dest_stop_lsn, BlockNumber segmentno, fio_location location)
{
    errno_t rc = 0;
    
    if (fio_is_remote(location))
    {
        fio_header hdr;
        fio_checksum_map_request req_hdr;
        PageState *checksum_map = NULL;
        size_t path_len = strlen(fullpath) + 1;

        req_hdr.n_blocks = n_blocks;
        req_hdr.segmentno = segmentno;
        req_hdr.stop_lsn = dest_stop_lsn;
        req_hdr.checksumVersion = checksum_version;

        hdr.cop = FIO_GET_CHECKSUM_MAP;
        hdr.size = sizeof(req_hdr) + path_len;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, &req_hdr, sizeof(req_hdr)), sizeof(req_hdr));
        IO_CHECK(fio_write_all(fio_stdout, fullpath, path_len), path_len);

        /* receive data */
        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

        if (hdr.size > 0)
        {
            checksum_map = (PageState *)pgut_malloc(n_blocks * sizeof(PageState));
            rc = memset_s(checksum_map, n_blocks * sizeof(PageState), 0, n_blocks * sizeof(PageState));
            securec_check(rc, "\0", "\0");
            if (hdr.size > (unsigned)n_blocks) {
                hdr.size = (unsigned)n_blocks;
            }
            IO_CHECK(fio_read_all(fio_stdin, checksum_map, hdr.size * sizeof(PageState)), hdr.size * sizeof(PageState));
        }

        return checksum_map;
    }
    else
    {

        return get_checksum_map(fullpath, checksum_version,
            n_blocks, dest_stop_lsn, segmentno);
    }
}

static void fio_get_checksum_map_impl(int out, char *buf)
{
    fio_header  hdr;
    PageState  *checksum_map = NULL;
    char       *fullpath = (char*) buf + sizeof(fio_checksum_map_request);
    fio_checksum_map_request *req = (fio_checksum_map_request*) buf;

    checksum_map = get_checksum_map(fullpath, req->checksumVersion,
                                                                req->n_blocks, req->stop_lsn, req->segmentno);
    hdr.size = req->n_blocks;

    /* send array of PageState`s to main process */
    IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
    if (hdr.size > 0)
        IO_CHECK(fio_write_all(out, checksum_map, hdr.size * sizeof(PageState)), hdr.size * sizeof(PageState));

    pg_free(checksum_map);
}

datapagemap_t *
fio_get_lsn_map(const char *fullpath, uint32 checksum_version,
                                int n_blocks, XLogRecPtr shift_lsn, BlockNumber segmentno,
                                fio_location location)
{
    datapagemap_t* lsn_map = NULL;
    errno_t rc = 0;

    if (fio_is_remote(location))
    {
        fio_header hdr;
        fio_lsn_map_request req_hdr;
        size_t path_len = strlen(fullpath) + 1;

        req_hdr.n_blocks = n_blocks;
        req_hdr.segmentno = segmentno;
        req_hdr.shift_lsn = shift_lsn;
        req_hdr.checksumVersion = checksum_version;

        hdr.cop = FIO_GET_LSN_MAP;
        hdr.size = sizeof(req_hdr) + path_len;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, &req_hdr, sizeof(req_hdr)), sizeof(req_hdr));
        IO_CHECK(fio_write_all(fio_stdout, fullpath, path_len), path_len);

        /* receive data */
        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

        if (hdr.size > 0)
        {
            lsn_map = (datapagemap_t *)pgut_malloc(sizeof(datapagemap_t));
            rc = memset_s(lsn_map,  sizeof(datapagemap_t), 0, sizeof(datapagemap_t));
            securec_check(rc, "\0", "\0");

            lsn_map->bitmap = (unsigned char *)pgut_malloc(hdr.size);
            lsn_map->bitmapsize = hdr.size;

            IO_CHECK(fio_read_all(fio_stdin, lsn_map->bitmap, hdr.size), hdr.size);
        }
    }
    else
    {
        /* operate is same in local mode and dss mode */
        lsn_map = get_lsn_map(fullpath, checksum_version, n_blocks, shift_lsn, segmentno);
    }

    return lsn_map;
}

static void fio_get_lsn_map_impl(int out, char *buf)
{
    fio_header     hdr;
    datapagemap_t *lsn_map = NULL;
    char          *fullpath = (char*) buf + sizeof(fio_lsn_map_request);
    fio_lsn_map_request *req = (fio_lsn_map_request*) buf;

    lsn_map = get_lsn_map(fullpath, req->checksumVersion, req->n_blocks,
    req->shift_lsn, req->segmentno);
    if (lsn_map)
        hdr.size = lsn_map->bitmapsize;
    else
        hdr.size = 0;

    /* send bitmap to main process */
    IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
    if (hdr.size > 0)
        IO_CHECK(fio_write_all(out, lsn_map->bitmap, hdr.size), hdr.size);

    if (lsn_map)
    {
        pg_free(lsn_map->bitmap);
        pg_free(lsn_map);
    }
}

/*
 * Go to the remote host and get postmaster pid from file postmaster.pid
 * and check that process is running, if process is running, return its pid number.
 */
pid_t fio_check_postmaster(const char *pgdata, fio_location location)
{
    if (fio_is_remote(location))
    {
        fio_header hdr;

        hdr.cop = FIO_CHECK_POSTMASTER;
        hdr.size = strlen(pgdata) + 1;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, pgdata, hdr.size), hdr.size);

        /* receive result */
        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));
        return hdr.arg;
    }
    else
        /* operate is same in local mode and dss mode */
        return check_postmaster(pgdata);
}

static void fio_check_postmaster_impl(int out, char *buf)
{
    fio_header  hdr;
    pid_t       postmaster_pid;
    char       *pgdata = (char*) buf;

    postmaster_pid = check_postmaster(pgdata);

    /* send arrays of checksums to main process */
    hdr.arg = postmaster_pid;
    IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
}

/*
 * Delete file pointed by the pgFile.
 * If the pgFile points directory, the directory must be empty.
 */
void
fio_delete(mode_t mode, const char *fullpath, fio_location location)
{
    if (fio_is_remote(location))
    {
        fio_header  hdr;

        hdr.cop = FIO_DELETE;
        hdr.size = strlen(fullpath) + 1;
        hdr.arg = mode;

        IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
        IO_CHECK(fio_write_all(fio_stdout, fullpath, hdr.size), hdr.size);

    }
    else
        /* operate is same in local mode and dss mode */
        pgFileDelete(mode, fullpath);
}

static void
fio_delete_impl(mode_t mode, char *buf)
{
    char  *fullpath = (char*) buf;

    pgFileDelete(mode, fullpath);
}

/* Execute commands at remote host */
void fio_communicate(int in, int out)
{
    /*
    * Map of file and directory descriptors.
    * The same mapping is used in agent and master process, so we
    * can use the same index at both sides.
    */
    int fd[FIO_FDMAX];
    DIR* dir[FIO_FDMAX];
    struct dirent* entry;
    size_t buf_size = 128*1024;
    char* buf = (char*)pgut_malloc(buf_size);
    fio_header hdr;
    struct stat st;
    int rc;
    int tmp_fd;
    pg_crc32 crc;

#ifdef WIN32
    SYS_CHECK(setmode(in, _O_BINARY));
    SYS_CHECK(setmode(out, _O_BINARY));
#endif

    /* Main loop until end of processing all master commands */
    while ((rc = fio_read_all(in, &hdr, sizeof(hdr))) == sizeof(hdr)) {
        if (hdr.size != 0) {
            if (hdr.size > buf_size) {
                /* Extend buffer on demand */
                size_t oldSize = buf_size;
                buf_size = hdr.size;
                buf = (char*)pgut_realloc(buf, oldSize, buf_size);
            }
            IO_CHECK(fio_read_all(in, buf, hdr.size), hdr.size);
        }
        switch (hdr.cop) {
            case FIO_LOAD: /* Send file content */
                fio_load_file(out, buf);
                break;
            case FIO_OPENDIR: /* Open directory for traversal */
                dir[hdr.handle] = opendir(buf);
                hdr.arg = dir[hdr.handle] == NULL ? errno : 0;
                hdr.size = 0;
                IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
                break;
            case FIO_READDIR: /* Get next directory entry */
                hdr.cop = FIO_SEND;
                entry = readdir(dir[hdr.handle]);
                if (entry != NULL)
                {
                    hdr.size = sizeof(*entry);
                    IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
                    IO_CHECK(fio_write_all(out, entry, hdr.size), hdr.size);
                }
                else
                {
                    hdr.size = 0;
                    IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
                }
                break;
            case FIO_CLOSEDIR: /* Finish directory traversal */
                SYS_CHECK(closedir(dir[hdr.handle]));
                break;
            case FIO_OPEN: /* Open file */
                fd[hdr.handle] = open(buf, hdr.arg, FILE_PERMISSIONS);
                hdr.arg = fd[hdr.handle] < 0 ? errno : 0;
                hdr.size = 0;
                IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
                break;
            case FIO_CLOSE: /* Close file */
                SYS_CHECK(close(fd[hdr.handle]));
                break;
            case FIO_WRITE: /* Write to the current position in file */
                IO_CHECK(fio_write_all(fd[hdr.handle], buf, hdr.size), hdr.size);
                break;
            case FIO_WRITE_COMPRESSED: /* Write to the current position in file */
                IO_CHECK(fio_write_compressed_impl(fd[hdr.handle], buf, hdr.size, hdr.arg), BLCKSZ);
                break;
            case FIO_COSTRUCT_COMPRESSED: {
                CompressCommunicate *cm = (CompressCommunicate *)(void *)buf;
                COMPRESS_ERROR_STATE result = ConstructCompressedFile(cm->path,
                                                                      (uint16)cm->chunkSize,
                                                                      (uint8)cm->algorithm);
                IO_CHECK((int)result, (int)SUCCESS);
                break;
            }
            case FIO_READ: /* Read from the current position in file */
                if ((size_t)hdr.arg > buf_size) {
                    size_t oldSize = buf_size;
                    buf_size = hdr.arg;
                    buf = (char*)pgut_realloc(buf, oldSize, buf_size);
                }
                rc = read(fd[hdr.handle], buf, hdr.arg);
                hdr.cop = FIO_SEND;
                hdr.size = rc > 0 ? rc : 0;
                IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
                if (hdr.size != 0)
                    IO_CHECK(fio_write_all(out, buf, hdr.size), hdr.size);
                break;
            case FIO_PREAD: /* Read from specified position in file, ignoring pages beyond horizon of delta backup */
                rc = pread(fd[hdr.handle], buf, BLCKSZ, hdr.arg);
                hdr.cop = FIO_SEND;
                hdr.arg = rc;
                hdr.size = rc >= 0 ? rc : 0;
                IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
                if (hdr.size != 0)
                    IO_CHECK(fio_write_all(out, buf, hdr.size),  hdr.size);
                break;
            case FIO_AGENT_VERSION:
                hdr.arg = AGENT_PROTOCOL_VERSION;
                IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
                break;
            case FIO_STAT: /* Get information about file with specified path */
                hdr.size = sizeof(st);
                rc = hdr.arg ? stat(buf, &st) : lstat(buf, &st);
                hdr.arg = rc < 0 ? errno : 0;
                IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
                IO_CHECK(fio_write_all(out, &st, sizeof(st)), sizeof(st));
                break;
            case FIO_ACCESS: /* Check presence of file with specified name */
                hdr.size = 0;
                hdr.arg = access(buf, hdr.arg) < 0 ? errno  : 0;
                IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
                break;
            case FIO_RENAME: /* Rename file */
                SYS_CHECK(rename(buf, buf + strlen(buf) + 1));
                break;
            case FIO_SYMLINK: /* Create symbolic link */
                fio_symlink_impl(out, buf, hdr.arg > 0 ? true : false);
                break;
            case FIO_UNLINK: /* Remove file or directory (TODO: Win32) */
                SYS_CHECK(remove_file_or_dir(buf));
                break;
            case FIO_MKDIR:  /* Create directory */
                hdr.size = 0;
                hdr.arg = dir_create_dir(buf, hdr.arg);
                IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
                break;
            case FIO_CHMOD:  /* Change file mode */
                SYS_CHECK(chmod(buf, hdr.arg));
                break;
            case FIO_SEEK:   /* Set current position in file */
                SYS_CHECK(lseek(fd[hdr.handle], hdr.arg, SEEK_SET));
                break;
            case FIO_TRUNCATE: /* Truncate file */
                SYS_CHECK(ftruncate(fd[hdr.handle], hdr.arg));
                break;
            case FIO_LIST_DIR:
                fio_list_dir_impl(out, buf);
                break;
            case FIO_SEND_PAGES:
                // buf contain fio_send_request header and bitmap.
                fio_send_pages_impl(out, buf);
                break;
            case FIO_SEND_FILE:
                fio_send_file_impl(out, buf);
                break;
            case FIO_SYNC:
                /* open file and fsync it */
                tmp_fd = open(buf, O_WRONLY | PG_BINARY, FILE_PERMISSIONS);
                if (tmp_fd < 0)
                    hdr.arg = errno;
                else
                {
                    if (fsync(tmp_fd) == 0)
                        hdr.arg = 0;
                    else
                        hdr.arg = errno;
                    
                    close(tmp_fd);
                }                

                IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
                break;
            case FIO_GET_CRC32:
                /* calculate crc32 for a file */
#ifdef HAVE_LIBZ
                if (hdr.arg == 1)
                    crc = pgFileGetCRCgz(buf, true, true);
                else
#endif
                    crc = pgFileGetCRC(buf, true, true);
                IO_CHECK(fio_write_all(out, &crc, sizeof(crc)), sizeof(crc));
                break;
            case FIO_GET_CHECKSUM_MAP:
                /* calculate crc32 for a file */
                fio_get_checksum_map_impl(out, buf);
                break;
            case FIO_GET_LSN_MAP:
                /* calculate crc32 for a file */
                fio_get_lsn_map_impl(out, buf);
                break;
            case FIO_CHECK_POSTMASTER:
                /* calculate crc32 for a file */
                fio_check_postmaster_impl(out, buf);
                break;
            case FIO_DELETE:
                /* delete file */
                fio_delete_impl(hdr.arg, buf);
                break;
            case FIO_DISCONNECT:
                hdr.cop = FIO_DISCONNECTED;
                IO_CHECK(fio_write_all(out, &hdr, sizeof(hdr)), sizeof(hdr));
                free(buf);
                return;
            default:
                Assert(false);
        }
    }
    free(buf);
    if (rc != 0) { /* Not end of stream: normal pipe close */
        perror("read");
    exit(EXIT_FAILURE);
    }
}
