/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * fio_device.h
 *  Storage Adapter Header File.
 *
 *
 * IDENTIFICATION
 *        src/include/storage/file/fio_device.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef FIO_DEVICE_H
#define FIO_DEVICE_H

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h> 
#include "storage/dss/fio_dss.h"
#include "storage/file/fio_device_com.h"

bool is_dss_type(device_type_t type);
bool is_dss_file(const char *name);
bool is_dss_file_dec(FILE *stream);
bool is_dss_fd(int handle);
bool is_file_delete(int err);
bool is_file_exist(int err);
device_type_t fio_device_type(const char *name);

typedef struct {
    uint64 magic_head;
    char filename[MAX_FILE_NAME_LEN];
    dss_dir_handle dir_handle;
    struct dirent ret; /* Used to return to caller */
} DSS_DIR;

typedef struct {
    uint64 magic_head;
    off_t fsize;
    int errcode;
    int handle;
} DSS_STREAM;

static inline int rename_dev(const char *oldpath, const char *newpath)
{
    if (is_dss_file(oldpath)) {
        if (dss_rename_file(oldpath, newpath) != GS_SUCCESS) {
            return -1;
        }
        return 0;
    } else {
        return rename(oldpath, newpath);
    }
}

static inline int open_dev(const char *pathname, int flags, mode_t mode)
{
    int handle;
    if (is_dss_file(pathname)) {
        if (dss_open_file(pathname, flags, mode, &handle) != GS_SUCCESS) {
            return -1;
        }
        return handle;
    } else {
        return open(pathname, flags, mode);
    }
}

static inline int close_dev(int fd)
{
    if (is_dss_fd(fd)) {
        if (dss_close_file(fd) != GS_SUCCESS) {
            return -1;
        }
        return 0;
    } else {
        return close(fd);
    }
}

typedef struct g_dss_io_stat {
    unsigned long long read_bytes;
    unsigned long long write_bytes;
    unsigned int read_write_count;
    bool is_ready_for_stat;
    pthread_mutex_t lock;
    g_dss_io_stat() {
        pthread_mutex_init(&lock, NULL);
        is_ready_for_stat = false;
        read_bytes = 0;
        write_bytes = 0;
        read_write_count = 0;
    }
    ~g_dss_io_stat() {
        pthread_mutex_destroy(&lock);
    }
} g_dss_io_stat;

extern g_dss_io_stat g_dss_io_stat_var;
/*Initialize global variable*/
static inline void init_dss_io_stat()
{
    pthread_mutex_lock(&g_dss_io_stat_var.lock);
    g_dss_io_stat_var.is_ready_for_stat = true;
    g_dss_io_stat_var.read_bytes = 0;
    g_dss_io_stat_var.write_bytes = 0;
    g_dss_io_stat_var.read_write_count = 0;
    
}

#define KB 1024
/*
* duration: statistical duration
* kB_read: total read kilobyte during the time
* kB_write: total write kilobyte during the time
* io_count: total read and write count
*/
static inline void get_dss_io_stat(int duration, unsigned long long *kB_read, unsigned long long *kB_write, unsigned int *io_count)
{
    sleep(duration);
    if (kB_read) {
        *kB_read = g_dss_io_stat_var.read_bytes / duration / KB;
    }
    if (kB_write) {
        *kB_write = g_dss_io_stat_var.write_bytes / duration / KB;
    }
    if (io_count) {
        *io_count = g_dss_io_stat_var.read_write_count;
    }
    g_dss_io_stat_var.is_ready_for_stat = false;
    pthread_mutex_unlock(&g_dss_io_stat_var.lock);
}

static inline ssize_t read_dev(int fd, void *buf, size_t count)
{
    ssize_t ret = 0;
    if (is_dss_fd(fd)) {
        ret = dss_read_file(fd, buf, count);
    } else {
        ret = read(fd, buf, count);
    }
    if (g_dss_io_stat_var.is_ready_for_stat) {
        g_dss_io_stat_var.read_bytes += count;
        g_dss_io_stat_var.read_write_count += 1;
    }
    return ret;
}

static inline ssize_t pread_dev(int fd, void *buf, size_t count, off_t offset)
{
    ssize_t ret = 0;
    if (is_dss_fd(fd)) {
        ret = dss_pread_file(fd, buf, count, offset);
    } else {
        ret = pread(fd, buf, count, offset);
    }
    if (g_dss_io_stat_var.is_ready_for_stat) {
        g_dss_io_stat_var.read_bytes += count;
        g_dss_io_stat_var.read_write_count += 1;
    }
    return ret;
}

static inline ssize_t write_dev(int fd, const void *buf, size_t count)
{
    ssize_t ret = 0;
    if (is_dss_fd(fd)) {
        ret = dss_write_file(fd, buf, count);
    } else {
        ret = write(fd, buf, count);
    }
    if (g_dss_io_stat_var.is_ready_for_stat) {
        g_dss_io_stat_var.write_bytes += count;
        g_dss_io_stat_var.read_write_count += 1;
    }
    return ret;
}

static inline ssize_t pwrite_dev(int fd, const void *buf, size_t count, off_t offset)
{
    ssize_t ret = 0;
    if (is_dss_fd(fd)) {
        ret = dss_pwrite_file(fd, buf, count, offset);
    } else {
        ret = pwrite(fd, buf, count, offset);
    }
    if (g_dss_io_stat_var.is_ready_for_stat) {
        g_dss_io_stat_var.write_bytes += count;
        g_dss_io_stat_var.read_write_count += 1;
    }
    return ret;
}

static inline off_t lseek_dev(int fd, off_t offset, int whence)
{
    if (is_dss_fd(fd)) {
        return dss_seek_file(fd, offset, whence);
    } else {
        return lseek(fd, offset, whence);
    }
}

static inline int fsync_dev(int fd)
{
    if (is_dss_fd(fd)) {
        if (dss_sync_file(fd) != GS_SUCCESS) {
            return -1;
        }
        return 0;
    } else {
        return fsync(fd);
    }
}

static inline int fallocate_dev(int fd, int mode, off_t offset, off_t len)
{
    if (is_dss_fd(fd)) {
        if (dss_fallocate_file(fd, mode, offset, len) != GS_SUCCESS) {
            return -1;
        }
        return 0;
    } else {
        return fallocate(fd, mode, offset, len);
    }
}

static inline int access_dev(const char *pathname, int mode)
{
    if (is_dss_file(pathname)) {
        if (dss_access_file(pathname, mode) != GS_SUCCESS) {
            return -1;
        }
        return 0;
    } else {
        return access(pathname, mode);
    }
}

static inline int mkdir_dev(const char *pathname, mode_t mode)
{
    if (is_dss_file(pathname)) {
        if (dss_create_dir(pathname, mode) != GS_SUCCESS) {
            return -1;
        }
        return 0;
    } else {
        return mkdir(pathname, mode);
    }
}

static inline int rmdir_dev(const char *pathname)
{
    if (is_dss_file(pathname)) {
        if (dss_remove_dir(pathname) != GS_SUCCESS) {
            return -1;
        }
        return 0;
    } else {
        return rmdir(pathname);
    }
}


static inline int symlink_dev(const char *target, const char *linkpath)
{
    if (is_dss_file(target)) {
        if (dss_link(target, linkpath) != GS_SUCCESS) {
            return -1;
        }
        return 0;
    } else {
        return symlink(target, linkpath);
    }
}

static inline ssize_t readlink_dev(const char *pathname, char *buf, size_t bufsiz)
{
    if (is_dss_file(pathname)) {
        struct stat st;
        if (dss_lstat_file(pathname, &st) != GS_SUCCESS || !S_ISLNK(st.st_mode)) {
            return -1;
        }

        return dss_read_link(pathname, buf, bufsiz);
    } else {
        return readlink(pathname, buf, bufsiz);
    }
}

static inline int chmod_dev(const char *pathname, mode_t mode)
{
    if (is_dss_file(pathname)) {
        return dss_chmod_file(pathname, mode);
    } else {
        return chmod(pathname, mode);
    }
}

static inline int unlink_dev(const char *pathname)
{
    if (is_dss_file(pathname)) {
        if (dss_remove_dev(pathname) != GS_SUCCESS) {
            return -1;
        }
        return 0;
    } else {
        return unlink(pathname);
    }
}

static inline int lstat_dev(const char * pathname, struct stat * statbuf)
{
    if (is_dss_file(pathname)) {
        if (dss_lstat_file(pathname, statbuf) != GS_SUCCESS) {
            if (errno == ERR_DSS_FILE_NOT_EXIST) {
                errno = ENOENT;
            }
            return -1;
        }
        return GS_SUCCESS;
    } else {
        return lstat(pathname, statbuf);
    }
}

static inline int stat_dev(const char *pathname, struct stat *statbuf)
{
    if (is_dss_file(pathname)) {
        if (dss_stat_file(pathname, statbuf) != GS_SUCCESS) {
            if (errno == ERR_DSS_FILE_NOT_EXIST) {
                errno = ENOENT;
            }
            return -1;
        }
        return GS_SUCCESS;
    } else {
        return stat(pathname, statbuf);
    }
}

static inline int fstat_dev(int fd, struct stat *statbuf)
{
    if (is_dss_fd(fd)) {
        return dss_fstat_file(fd, statbuf);
    } else {
        return fstat(fd, statbuf);
    }
}

static inline FILE *fopen_dev(const char *pathname, const char *mode)
{
    if (unlikely(is_dss_file(pathname))) {
        FILE *stream = NULL;
        if (dss_fopen_file(pathname, mode, &stream) != GS_SUCCESS) {
            return NULL;
        }
        return stream;
    } else {
        return fopen(pathname, mode);
    }
}

static inline int remove_dev(const char *pathname)
{
    if (is_dss_file(pathname)) {
        if (dss_remove_dev(pathname) != GS_SUCCESS) {
            return -1;
        }
        return 0;
    } else {
        return remove(pathname);
    }
}

static inline int fclose_dev(FILE *stream)
{
    DSS_STREAM *dss_stream = (DSS_STREAM *)stream;
    if (unlikely(dss_stream->magic_head == DSS_MAGIC_NUMBER)) {
        if (dss_fclose_file(stream) != GS_SUCCESS) {
            return -1;
        }
        return 0;
    } else {
        return fclose(stream);
    }
}

static inline size_t fread_dev(void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    DSS_STREAM *dss_stream = (DSS_STREAM *)stream;
    if (unlikely(dss_stream->magic_head == DSS_MAGIC_NUMBER)) {
        return dss_fread_file(ptr, size, nmemb, stream);
    } else {
        return fread(ptr, size, nmemb, stream);
    }
}

static inline size_t fwrite_dev(const void *ptr, size_t size, size_t nmemb, FILE *stream)
{
    DSS_STREAM *dss_stream = (DSS_STREAM *)stream;
    if (unlikely(dss_stream->magic_head == DSS_MAGIC_NUMBER)) {
        return dss_fwrite_file(ptr, size, nmemb, stream);
    } else {
        return fwrite(ptr, size, nmemb, stream);
    }
}

static inline int fseek_dev(FILE *stream, long offset, int whence)
{
    DSS_STREAM *dss_stream = (DSS_STREAM *)stream;
    if (unlikely(dss_stream->magic_head == DSS_MAGIC_NUMBER)) {
        return dss_fseek_file(stream, offset, whence);
    } else {
        return fseek(stream, offset, whence);
    }
}

static inline int fseeko_dev(FILE *stream, long offset, int whence)
{
    DSS_STREAM *dss_stream = (DSS_STREAM *)stream;
    if (unlikely(dss_stream->magic_head == DSS_MAGIC_NUMBER)) {
        return dss_fseek_file(stream, offset, whence);
    } else {
        return fseeko(stream, offset, whence);
    }
}

static inline long ftell_dev(FILE *stream)
{
    DSS_STREAM *dss_stream = (DSS_STREAM *)stream;
    if (unlikely(dss_stream->magic_head == DSS_MAGIC_NUMBER)) {
        return dss_ftell_file(stream);
    } else {
        return ftell(stream);
    }
}

static inline int fflush_dev(FILE *stream)
{
    DSS_STREAM *dss_stream = (DSS_STREAM *)stream;
    if (unlikely(dss_stream->magic_head == DSS_MAGIC_NUMBER)) {
        /* nothing to do, because DSS will enable O_SYNC and O_DIRECT for all IO */
        return 0;
    } else {
        return fflush(stream);
    }
}

static inline int ftruncate_dev(int fd, off_t length)
{
    if (unlikely(is_dss_fd(fd))) {
        return dss_truncate_file(fd, length);
    } else {
        return ftruncate(fd, length);
    }
}

static inline int feof_dev(FILE *stream)
{
    DSS_STREAM *dss_stream = (DSS_STREAM *)stream;
    if (unlikely(dss_stream->magic_head == DSS_MAGIC_NUMBER)) {
        return dss_feof(stream);
    } else {
        return feof(stream);
    }
}

static inline int ferror_dev(FILE *stream)
{
    DSS_STREAM *dss_stream = (DSS_STREAM *)stream;
    if (unlikely(dss_stream->magic_head == DSS_MAGIC_NUMBER)) {
        return dss_ferror(stream);
    } else {
        return ferror(stream);
    }
}

static inline int fileno_dev(FILE *stream)
{
    DSS_STREAM *dss_stream = (DSS_STREAM *)stream;
    if (unlikely(dss_stream->magic_head == DSS_MAGIC_NUMBER)) {
        return dss_fileno(stream);
    } else {
        return fileno(stream);
    }
}

static inline int setvbuf_dev(FILE *stream, char *buf, int type, size_t size)
{
    DSS_STREAM *dss_stream = (DSS_STREAM *)stream;
    if (unlikely(dss_stream->magic_head == DSS_MAGIC_NUMBER)) {
        /* nothing to do in DSS */
        return 0;
    } else {
        return setvbuf(stream, buf, type, size);
    }
}

static inline void rewind_dev(FILE *stream)
{
    DSS_STREAM *dss_stream = (DSS_STREAM *)stream;
    if (unlikely(dss_stream->magic_head == DSS_MAGIC_NUMBER)) {
        dss_rewind_file(stream);
    } else {
        rewind(stream);
    }
}

static inline DIR *opendir_dev(const char *name)
{
    if (unlikely(is_dss_file(name))) {
        DIR *dir = NULL;
        if (dss_open_dir(name, &dir) != GS_SUCCESS) {
            return NULL;
        }
        return dir;
    } else {
        return opendir(name);
    }
}

static inline struct dirent *readdir_dev(DIR *dirp)
{
    DSS_DIR *dss_dir = (DSS_DIR *)dirp;

    if (dirp == NULL) {
        return NULL;
    }

    if (unlikely(dss_dir->magic_head == DSS_MAGIC_NUMBER)) {
        struct dirent *de = NULL;
        (void)dss_read_dir(dirp, &de);
        return de;
    } else {
        return readdir(dirp);
    }
}

static inline int readdir_r_dev(DIR *dirp, struct dirent *entry, struct dirent **result)
{
    DSS_DIR *dss_dir = (DSS_DIR *)dirp;
    if (unlikely(dss_dir->magic_head == DSS_MAGIC_NUMBER)) {
        if (dss_read_dir(dirp, result) != GS_SUCCESS) {
            return 1;
        }
        return 0;
    } else {
        /**
         * In arm environment, readdir_r warning about deprecated-declarations,
         * but for thread safe keep using readdir_r.
         */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
        return readdir_r(dirp, entry, result);
#pragma GCC diagnostic pop
    }
}

static inline int closedir_dev(DIR *dirp)
{
    DSS_DIR *dss_dir = (DSS_DIR *)dirp;
    if (unlikely(dss_dir->magic_head == DSS_MAGIC_NUMBER)) {
        if (dss_close_dir(dirp) != GS_SUCCESS) {
            return -1;
        }
        return 0;
    } else {
        return closedir(dirp);
    }
}

/* declare macros to replace file system API */
#define rename(oldpath, newpath) rename_dev((oldpath), (newpath))
#define open(pathname, flags, mode) open_dev((pathname), (flags), (mode))
#define close(fd) close_dev(fd)
#define read(fd, buf, count) read_dev((fd), (buf), (count))
#define pread(fd, buf, count, offset) pread_dev((fd), (buf), (count), (offset))
#define pread64(fd, buf, count, offset) pread_dev((fd), (buf), (count), (offset))
#define write(fd, buf, count) write_dev((fd), (buf), (count))
#define pwrite(fd, buf, count, offset) pwrite_dev((fd), (buf), (count), (offset))
#define pwrite64(fd, buf, count, offset) pwrite_dev((fd), (buf), (count), (offset))
#define lseek(fd, offset, whence) lseek_dev((fd), (offset), (whence))
#define lseek64(fd, offset, whence) lseek_dev((fd), (offset), (whence))
#define fsync(fd) fsync_dev(fd)
#define fallocate(fd, mode, offset, len) fallocate_dev((fd), (mode), (offset), (len))
#define access(pathname, mode) access_dev((pathname), (mode))
#define mkdir(pathname, mode) mkdir_dev((pathname), (mode))
#define rmdir(pathname) rmdir_dev(pathname)
#define symlink(target, linkpath) symlink_dev((target), (linkpath))
#define readlink(pathname, buf, bufsiz) readlink_dev((pathname), (buf), (bufsiz))
#define chmod(pathname, mode) chmod_dev((pathname), (mode))
#define unlink(pathname) unlink_dev(pathname)
#define stat(pathname, statbuf) stat_dev(pathname, statbuf)
#define lstat(pathname, statbuf) lstat_dev(pathname, statbuf)
#define ftruncate(fd, length) ftruncate_dev((fd), (length))
#define fopen(pathname, mode) fopen_dev((pathname), (mode))
#define fclose(stream) fclose_dev((stream))
#define fread(ptr, size, nmemb, stream) fread_dev((ptr), (size), (nmemb), (stream))
#define fwrite(ptr, size, nmemb, stream) fwrite_dev((ptr), (size), (nmemb), (stream))
#define fseek(stream, offset, whence) fseek_dev((stream), (offset), (whence))
#define fseeko(stream, offset, whence) fseeko_dev((stream), (offset), (whence))
#define ftell(stream) ftell_dev((stream))
#define fflush(stream) fflush_dev((stream))
#define feof(stream) feof_dev((stream))
#define ferror(stream) ferror_dev((stream))
#define fileno(stream) fileno_dev((stream))
#define setvbuf(stream, buf, type, size) setvbuf_dev((stream), (buf), (type), (size))
#define rewind(stream) rewind_dev((stream))
#define opendir(name) opendir_dev((name))
#define readdir(dirp) readdir_dev((dirp))
#define readdir_r(dirp, entry, result) readdir_r_dev((dirp), (entry), (result))
#define closedir(dirp) closedir_dev((dirp))
#define fstat(fd, statbuf) fstat_dev(fd, statbuf)
#define remove(pathname) remove_dev(pathname)

#endif /* FIO_DEVICE_H */
