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
 * fio_dss.h
 *  DSS File System Adapter Header File.
 *
 *
 * IDENTIFICATION
 *        src/include/storage/dss/fio_dss.h
 *
 * ---------------------------------------------------------------------------------------
 */
 
#ifndef FIO_DSS_H
#define FIO_DSS_H

#include "c.h"
#include <dirent.h>
#include <sys/stat.h>
#include <stdio.h>
#include "storage/dss/dss_adaptor.h"

void dss_device_register(dss_device_op_t *dss_device_op, bool enable_dss);
void dss_set_errno(int *errcode, const char **errmsg = NULL);
int dss_access_file(const char *file_name, int mode);
int dss_create_dir(const char *name, mode_t mode);
int dss_open_dir(const char *name, DIR **dir_handle);
int dss_read_dir(DIR *dir_handle, struct dirent **result);
int dss_close_dir(DIR *dir_handle);
int dss_remove_dir(const char *name);
int dss_rename_file(const char *src, const char *dst);
int dss_remove_file(const char *name);
int dss_open_file(const char *name, int flags, mode_t mode, int *handle);
int dss_fopen_file(const char *name, const char* mode, FILE **stream);
int dss_close_file(int handle);
int dss_fclose_file(FILE *stream);
ssize_t dss_read_file(int handle, void *buf, size_t size);
ssize_t dss_pread_file(int handle, void *buf, size_t size, off_t offset);
size_t dss_fread_file(void *buf, size_t size, size_t nmemb, FILE *stream);
ssize_t dss_write_file(int handle, const void *buf, size_t size);
ssize_t dss_pwrite_file(int handle, const void *buf, size_t size, off_t offset);
size_t dss_fwrite_file(const void *buf, size_t size, size_t count, FILE *stream);
off_t dss_seek_file(int handle, off_t offset, int origin);
int dss_fseek_file(FILE *stream, long offset, int whence);
long dss_ftell_file(FILE *stream);
void dss_rewind_file(FILE *stream);
int dss_fflush_file(FILE *stream);
int dss_sync_file(int handle);
int dss_truncate_file(int handle, off_t keep_size);
int dss_ftruncate_file(FILE *stream, off_t keep_size);
off_t dss_get_file_size(const char *fname);
int dss_fallocate_file(int handle, int mode, off_t offset, off_t len);
int dss_link(const char *src, const char *dst);
int dss_unlink_target(const char *name);
ssize_t dss_read_link(const char *path, char *buf, size_t buf_size);
int dss_setvbuf(FILE *stream, char *buf, int mode, size_t size);
int dss_feof(FILE *stream);
int dss_ferror(FILE *stream);
int dss_fileno(FILE *stream);
int dss_stat_file(const char *path, struct stat *buf);
int dss_lstat_file(const char *path, struct stat *buf);
int dss_fstat_file(int handle, struct stat *buf);
int dss_chmod_file(const char* path, mode_t mode);
int dss_set_server_status_wrapper();
int dss_remove_dev(const char *name);
int dss_get_addr(int handle, long long offset, char *poolname, char *imagename, char *objAddr,
    unsigned int *objId, unsigned long int *objOffset);
int dss_compare_size(const char *vg_name, long long *au_size);
int dss_aio_prep_pwrite(void *iocb, int fd, void *buf, size_t count, long long offset);
int dss_aio_prep_pread(void *iocb, int fd, void *buf, size_t count, long long offset);

#endif // FIO_DSS_H
