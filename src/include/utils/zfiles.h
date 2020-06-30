/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
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
 * zfiles.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/zfiles.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef _LOG_FDW_ZFILES_H_
#define _LOG_FDW_ZFILES_H_

#include "ioapi.h"
#include "unzip.h"

typedef struct opaque_vfd {
    int m_tmp_vfd;   /* virtual fd */
    int m_errno;     /* io error no */
    off_t m_cur_off; /* file offset */
    long m_max_off;  /* file max offset */
} opaque_vfd;

typedef struct zip_reader {
    unzFile m_unz;
    unz_global_info64 m_unz_gi;
    ZPOS64_T m_unz_curfile;
    opaque_vfd m_vfd;
} zip_reader;

typedef struct gz_reader {
    gzFile m_gzf;
    opaque_vfd* m_vfd;
} gz_reader;

extern void pm_set_unzip_memfuncs(void);

/* unzip postgresql-2018-03-31_000000.zip log file and read its contents */
extern void set_unzip_filefuncs(zlib_filefunc64_def* funcs, voidpf opaque);
extern void* unzip_open(const char* zipfile);
extern int unzip_read(void* zr, char* buf, int bufsize);
extern void unzip_close(void* zr);

extern void* vfd_file_open(const char* logfile);
extern int vfd_file_read(void* fobj, char* buf, int bufsize);
extern void vfd_file_close(void* fobj);
extern void vfd_file_close_with_thief(void* fobj);

extern void* gz_open(const char* gzfile);
extern int gz_read(void* gzfd, char* buf, int bufsize);
extern void gz_close(void* gzfd);

#endif /* _LOG_FDW_ZFILES_H_ */
