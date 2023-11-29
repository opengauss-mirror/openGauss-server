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
 * fio_dss.cpp
 *  DSS File System Adapter Interface.
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/dss/fio_dss.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <dirent.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include "securec.h"
#include "securec_check.h"
#include "storage/file/fio_device.h"

static char zero_area[FILE_EXTEND_STEP_SIZE + ALIGNOF_BUFFER] = { 0 };

ssize_t buffer_align(char **unalign_buff, char **buff, size_t size);
ssize_t dss_align_read(int handle, void *buf, size_t size, off_t offset, bool use_p);
int dss_pwrite_file_by_zero(int handle, off_t offset, off_t len);
int dss_get_file_name(int handle, char *fname, size_t fname_size);

// interface for register raw device callback function
dss_device_op_t g_dss_device_op;
bool g_enable_dss = false;

/* Xlog Segment Size */
uint64 XLogSegmentSize = XLOG_SEG_SIZE;

void dss_device_register(dss_device_op_t *dss_device_op, bool enable_dss)
{
    g_dss_device_op = *dss_device_op;
    g_enable_dss = enable_dss;
}

bool is_dss_file(const char *name)
{
    if (g_enable_dss) {
        return (name[0] == '+') ? true : false;
    }

    return false;
}

bool is_dss_file_dec(FILE *stream)
{
    DSS_STREAM *dss_stream = (DSS_STREAM *)stream;
    if (dss_stream->magic_head == DSS_MAGIC_NUMBER) {
        return true;
    }
    return false;
}

bool is_dss_fd(int handle)
{
    if (handle >= (int)DSS_HANDLE_BASE) {
        return true;
    }

    return false;
}

int parse_errcode_from_errormsg(const char* errormsg) {
    const char *errcode_str = strstr(errormsg, "errcode:");
    if (errcode_str) {
        errcode_str += strlen("errcode:");
        return atoi(errcode_str);
    }
    return ERR_DSS_PROCESS_REMOTE;
}

void dss_set_errno(int *errcode, const char **errmsg)
{
    int errorcode = 0;
    const char *errormsg = NULL;

    g_dss_device_op.dss_get_error(&errorcode, &errormsg);
    if (errorcode == ERR_DSS_PROCESS_REMOTE) {
        errno = parse_errcode_from_errormsg(errormsg);
    } else {
        errno = errorcode;
    }

    if (errcode != NULL) {
        *errcode = errorcode;
    }

    if (errmsg != NULL) {
        *errmsg = errormsg;
    }
}

int dss_access_file(const char *file_name, int mode)
{
    struct stat statbuf = {0};
    return dss_stat_file(file_name, &statbuf);
}

int dss_create_dir(const char *name, mode_t mode)
{
    if (g_dss_device_op.dss_create_dir(name) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

int dss_open_dir(const char *name, DIR **dir_handle)
{
    DSS_DIR *dss_dir = NULL;

    /* dss_dir_t will be free in dss_close_dir */
    dss_dir = (DSS_DIR*)malloc(sizeof(DSS_DIR));
    dss_dir->dir_handle = g_dss_device_op.dss_open_dir(name);
    if (dss_dir->dir_handle == NULL) {
        dss_set_errno(NULL);
        free(dss_dir);
        return GS_ERROR;
    }

    dss_dir->magic_head = DSS_MAGIC_NUMBER;
    *dir_handle = (DIR*)dss_dir;
    return GS_SUCCESS;
}

int dss_read_dir(DIR *dir_handle, struct dirent **result)
{
    dss_dirent_t dirent_t;
    dss_dir_item_t item_t;
    errno_t rc;
    DSS_DIR *dss_dir = (DSS_DIR*)dir_handle;

    *result = NULL;
    if (g_dss_device_op.dss_read_dir(dss_dir->dir_handle, &dirent_t, &item_t) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return GS_ERROR;
    }

    if (item_t == NULL) {
        dss_set_errno(NULL);
        return GS_SUCCESS;
    }

    rc = strcpy_s(dss_dir->filename, MAX_FILE_NAME_LEN, dirent_t.d_name);
    securec_check_c(rc, "\0", "\0");
    rc = strcpy_s(dss_dir->ret.d_name, MAX_FILE_NAME_LEN, dirent_t.d_name);
    securec_check_c(rc, "\0", "\0");

    *result = &dss_dir->ret;

    return GS_SUCCESS;
}

int dss_close_dir(DIR *dir_handle)
{
    DSS_DIR *dss_dir_t = (DSS_DIR*)dir_handle;
    int result = GS_SUCCESS;

    if (g_dss_device_op.dss_close_dir(dss_dir_t->dir_handle) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        result = GS_ERROR;
    }
    free(dss_dir_t);
    return result;
}

int dss_remove_dir(const char *name)
{
    if (g_dss_device_op.dss_remove_dir(name) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

int dss_rename_file(const char *src, const char *dst)
{
    if (g_dss_device_op.dss_rename(src, dst) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

int dss_remove_file(const char *name)
{
    if (g_dss_device_op.dss_remove(name) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

int dss_open_file(const char *name, int flags, mode_t mode, int *handle)
{
    struct stat st;
    if ((flags & O_CREAT) != 0 && dss_stat_file(name, &st) != GS_SUCCESS) {
        // file not exists, create it first.
        if (errno == ERR_DSS_FILE_NOT_EXIST && g_dss_device_op.dss_create(name, flags) != DSS_SUCCESS) {
            dss_set_errno(NULL);
            return GS_ERROR;
        }
    }

    if (g_dss_device_op.dss_open(name, flags, handle) != DSS_SUCCESS) {
        *handle = -1;
        dss_set_errno(NULL);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

int dss_fopen_file(const char *name, const char* mode, FILE **stream)
{
    int openmode = 0;
    int handle = -1;
    off_t fsize = INVALID_DEVICE_SIZE;
    DSS_STREAM *dss_fstream = NULL;

    // check open mode
    if (strstr(mode, "r+")) {
        openmode |= O_RDWR;
    } else if (strchr(mode, 'r')) {
        openmode |= O_RDONLY;
    }

    if (strstr(mode, "w+")) {
        openmode |= O_RDWR | O_CREAT | O_TRUNC;
    } else if (strchr(mode, 'w')) {
        openmode |= O_WRONLY | O_CREAT | O_TRUNC;
    }

    if (strstr(mode, "a+")) {
        openmode |= O_RDWR | O_CREAT | O_APPEND;
    } else if (strchr(mode, 'a')) {
        openmode |= O_WRONLY | O_CREAT | O_APPEND;
    }

    // get handle and fsize of open file
    if (dss_open_file(name, openmode, 0, &handle) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if ((fsize = dss_get_file_size(name)) == INVALID_DEVICE_SIZE) {
        return GS_ERROR;
    }

    // init dss stream handle
    dss_fstream = (DSS_STREAM*)malloc(sizeof(DSS_STREAM));
    dss_fstream->fsize = fsize;
    dss_fstream->errcode = 0;
    dss_fstream->handle = handle;

    dss_fstream->magic_head = DSS_MAGIC_NUMBER;
    *stream = (FILE*)dss_fstream;
    return GS_SUCCESS;
}

int dss_close_file(int handle)
{
    if (g_dss_device_op.dss_close(handle) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

int dss_fclose_file(FILE *stream)
{
    // set errcode of stream in close is meaningless
    int status = dss_close_file(dss_fileno(stream));
    free(stream);
    return status;
}

ssize_t dss_read_file(int handle, void *buf, size_t size)
{
    return dss_align_read(handle, buf, size, -1, false);
}

ssize_t dss_pread_file(int handle, void *buf, size_t size, off_t offset)
{
    return dss_align_read(handle, buf, size, offset, true);
}

ssize_t dss_align_read(int handle, void *buf, size_t size, off_t offset, bool use_p)
{
    size_t newSize = size;
    char* unalign_buff = NULL;
    char* buff = NULL;
    bool address_align = false;
    int ret = DSS_ERROR;
    int r_size = 0;
    
    if ((((uint64)buf) % ALIGNOF_BUFFER) == 0) {
        address_align = true;
    }

    if ((!address_align) || ((size % ALIGNOF_BUFFER) != 0)) {
        newSize = buffer_align(&unalign_buff, &buff, size);
    } else {
        buff = (char*)buf;
    }

    if (use_p) {
        ret = g_dss_device_op.dss_pread(handle, buff, newSize, offset, &r_size);
    } else {
        ret = g_dss_device_op.dss_read(handle, buff, newSize, &r_size);
    }

    if (ret != DSS_SUCCESS) {
        dss_set_errno(NULL);
        if (unalign_buff != NULL) {
            free(unalign_buff);
        }
        return -1;
    }

    if (unalign_buff != NULL) {
        int move = (int)size - (int)newSize;
        errno_t rc = memcpy_s(buf, size, buff, size);
        securec_check_c(rc, "\0", "\0");
        free(unalign_buff);
        // change current access position to correct point
        if (move < 0 && dss_seek_file(handle, move, SEEK_CUR) < 0) {
            return -1;
        }
    }

    return (((ssize_t)(r_size)) < ((ssize_t)(size)) ? ((ssize_t)(r_size)) : ((ssize_t)(size)));
}

size_t dss_fread_file(void *buf, size_t size, size_t nmemb, FILE *stream)
{
    ssize_t r_size = 0;
    DSS_STREAM *dss_fstream = (DSS_STREAM*)stream;
    if ((r_size = dss_align_read(dss_fstream->handle, buf, size * nmemb, -1, false)) == -1) {
        dss_set_errno(&dss_fstream->errcode);
        return 0;
    }
    return (size_t)r_size;
}

ssize_t dss_write_file(int handle, const void *buf, size_t size)
{
    if (g_dss_device_op.dss_write(handle, buf, size) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return -1;
    }
    return (ssize_t)size;
}

ssize_t dss_pwrite_file(int handle, const void *buf, size_t size, off_t offset)
{
    if (g_dss_device_op.dss_pwrite(handle, buf, size, offset) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return -1;
    }
    return (ssize_t)size;
}

size_t dss_fwrite_file(const void *buf, size_t size, size_t count, FILE *stream)
{
    DSS_STREAM *dss_fstream = (DSS_STREAM*)stream;
    if (g_dss_device_op.dss_write(dss_fstream->handle, buf, size * count) != DSS_SUCCESS) {
        dss_set_errno(&dss_fstream->errcode);
        return (size_t)-1;
    }
    return count;
}

off_t dss_seek_file(int handle, off_t offset, int origin)
{
    if (origin == SEEK_END) {
        origin = DSS_SEEK_MAXWR;
    }
    off_t size = (off_t)g_dss_device_op.dss_seek(handle, offset, origin);
    if (size == -1) {
        dss_set_errno(NULL);
    }
    return size;
}

int dss_fseek_file(FILE *stream, long offset, int whence)
{
    if (whence == SEEK_END) {
        whence = DSS_SEEK_MAXWR;
    }
    DSS_STREAM *dss_fstream = (DSS_STREAM*)stream;
    off_t size = (off_t)g_dss_device_op.dss_seek(dss_fstream->handle, offset, whence);
    if (size == -1) {
        dss_set_errno(&dss_fstream->errcode);
        return -1;
    }
    return (int)size;
}

long dss_ftell_file(FILE *stream)
{
    DSS_STREAM *dss_fstream = (DSS_STREAM*)stream;
    off_t size = (off_t)g_dss_device_op.dss_seek(dss_fstream->handle, 0, SEEK_CUR);
    if (size == -1) {
        dss_set_errno(&dss_fstream->errcode);
    }
    return size;
}

void dss_rewind_file(FILE *stream)
{
    DSS_STREAM *dss_fstream = (DSS_STREAM*)stream;
    off_t size = (off_t)g_dss_device_op.dss_seek(dss_fstream->handle, 0, SEEK_SET);
    if (size == -1) {
        dss_set_errno(&dss_fstream->errcode);
    }
}

int dss_fflush_file(FILE *stream)
{
    /* nothing to do, because DSS will enable O_SYNC and O_DIRECT for all IO */
    return GS_SUCCESS;
}

int dss_sync_file(int handle)
{
    /* nothing to do, because DSS will enable O_SYNC and O_DIRECT for all IO */
    return GS_SUCCESS;
}

int dss_truncate_file(int handle, off_t keep_size)
{
    /* not guarantee fill zero */
    if (g_dss_device_op.dss_truncate(handle, keep_size) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

int dss_ftruncate_file(FILE *stream, off_t keep_size)
{
    /* file stream do not have truncate function, so it's ok we do not set errcode in stream */
    return dss_truncate_file(dss_fileno(stream), keep_size);
}

int dss_get_file_name(int handle, char *fname, size_t fname_size)
{
    if (g_dss_device_op.dss_fname(handle, fname, fname_size) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

off_t dss_get_file_size(const char *fname)
{
    off_t fsize = INVALID_DEVICE_SIZE;

    g_dss_device_op.dss_fsize(fname, &fsize);
    if (fsize == INVALID_DEVICE_SIZE) {
        dss_set_errno(NULL);
    }

    return fsize;
}

int dss_fallocate_file(int handle, int mode, off_t offset, off_t len)
{
    return dss_pwrite_file_by_zero(handle, offset, len);
}

int dss_link(const char *src, const char *dst)
{
    if (g_dss_device_op.dss_link(src, dst) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

int dss_unlink_target(const char *name)
{
    if (g_dss_device_op.dss_unlink(name) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

ssize_t dss_read_link(const char *path, char *buf, size_t buf_size)
{
    ssize_t result = (ssize_t)g_dss_device_op.dss_read_link(path, buf, buf_size);
    if (result == -1) {
        dss_set_errno(NULL);
    }
    return result;
}

int dss_setvbuf(FILE *stream, char *buf, int mode, size_t size)
{
    /* nothing to do in dss mode */
    return 0;
}

int dss_feof(FILE *stream)
{
    DSS_STREAM *dss_fstream = (DSS_STREAM*)stream;
    if (dss_ftell_file(stream) < dss_fstream->fsize) {
        return 0;
    }
    return 1;
}

int dss_ferror(FILE *stream)
{
    DSS_STREAM *dss_fstream = (DSS_STREAM*)stream;
    if (dss_fstream->errcode) {
        return 1;
    }
    return 0;
}

int dss_fileno(FILE *stream)
{
    DSS_STREAM *dss_fstream = (DSS_STREAM*)stream;
    return dss_fstream->handle;
}

int dss_stat_file(const char *path, struct stat *buf)
{
    dss_stat_t st;
    if (g_dss_device_op.dss_stat(path, &st) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return -1;
    }

    // file type and mode
    switch (st.type) {
        case DSS_PATH:
            buf->st_mode = S_IFDIR;
            break;
        case DSS_FILE:
            buf->st_mode = S_IFREG;
            break;
        case DSS_LINK:
        /* fall-through */
        default:
            return -1;
    }
    // total size, in bytes
    buf->st_size = (long)st.written_size;
    // time of last modification
    buf->st_mtime = st.update_time;

    return 0;
}

int dss_fstat_file(int handle, struct stat *buf)
{
    dss_stat_t st;
    if (g_dss_device_op.dss_fstat(handle, &st) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return -1;
    }

    // file type and mode
    switch (st.type) {
        case DSS_PATH:
            buf->st_mode = S_IFDIR;
            break;
        case DSS_FILE:
            buf->st_mode = S_IFREG;
            break;
        case DSS_LINK:
        /* fall-through */
        default:
            return -1;
    }
    // total size, in bytes
    buf->st_size = (long)st.written_size;
    // time of last modification
    buf->st_mtime = st.update_time;

    return 0;
}

// return information of link itself when path is link
int dss_lstat_file(const char *path, struct stat *buf)
{
    dss_stat_t st;
    if (g_dss_device_op.dss_lstat(path, &st) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return -1;
    }

    // file type and mode
    switch (st.type) {
        case DSS_PATH:
            buf->st_mode = S_IFDIR;
            break;
        case DSS_FILE:
            buf->st_mode = S_IFREG;
            break;
        case DSS_LINK:
            buf->st_mode = S_IFLNK;
            break;
        default:
            return -1;
    }
    // total size, in bytes
    buf->st_size = (long)st.written_size;
    // time of last modification
    buf->st_mtime = st.update_time;

    return 0;
}

int dss_chmod_file(const char* path, mode_t mode)
{
    // dss do not have mode
    return 0;
}

ssize_t buffer_align(char **unalign_buff, char **buff, size_t size)
{
    size_t newSize = size;
    size_t size_mod = ALIGNOF_BUFFER - (newSize % ALIGNOF_BUFFER);
    size_t size_move = 0;

    if ((size % ALIGNOF_BUFFER) != 0) {
        newSize = BUFFERALIGN(size);
        size_move += size_mod;
    }

    size_move += ALIGNOF_BUFFER;

    *unalign_buff = (char*)malloc(size + size_move);
    *buff = (char*)BUFFERALIGN(*unalign_buff);

    return (ssize_t)newSize;
}

int dss_pwrite_file_by_zero(int handle, off_t offset, off_t len)
{
    char *zero_area_aligned = (char *)(((uintptr_t)zero_area + ALIGNOF_BUFFER - 1) & (~(ALIGNOF_BUFFER - 1)));
    off_t remain_size = len;
    ssize_t write_size;
    while (remain_size > 0) {
        write_size = (remain_size > FILE_EXTEND_STEP_SIZE) ? FILE_EXTEND_STEP_SIZE : (ssize_t)remain_size;
        if (dss_pwrite_file(handle, zero_area_aligned, write_size, offset) != write_size) {
            return GS_ERROR;
        }

        offset += write_size;
        remain_size -= write_size;
    }

    return GS_SUCCESS;
}

int dss_set_server_status_wrapper()
{
    return g_dss_device_op.dss_set_main_inst();
}

int dss_remove_dev(const char *name)
{
    struct stat st;
    int ret = lstat(name, &st);
    if (ret == 0 && S_ISREG(st.st_mode)) {
        return dss_remove_file(name);
    } else if (ret == 0 && S_ISLNK(st.st_mode)) {
        return dss_unlink_target(name);
    } else {
        return GS_SUCCESS;
    }
}

int dss_get_addr(int handle, long long offset, char *poolname, char *imagename, char *objAddr,
    unsigned int *objId, unsigned long int *objOffset)
{
    if (g_dss_device_op.dss_get_addr(handle, offset, poolname, imagename, objAddr, objId, objOffset) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return -1;
    }
    return GS_SUCCESS;
}

int dss_compare_size(const char *vg_name, long long *au_size)
{
    if (g_dss_device_op.dss_compare_size(vg_name, au_size) != DSS_SUCCESS) {
        dss_set_errno(NULL);
        return -1;
    }
    return GS_SUCCESS;
}

int dss_aio_prep_pwrite(void *iocb, int fd, void *buf, size_t count, long long offset)
{
    return g_dss_device_op.dss_aio_pwrite(iocb, fd, buf, count, offset);
}

int dss_aio_prep_pread(void *iocb, int fd, void *buf, size_t count, long long offset)
{
    return g_dss_device_op.dss_aio_pread(iocb, fd, buf, count, offset);
}
