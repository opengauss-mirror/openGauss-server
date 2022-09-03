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
 *
 *
 * IDENTIFICATION
 *        src/include/gs_tar_const.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GS_TAR_CONST_H_
#define GS_TAR_CONST_H_

#ifdef HAVE_LIBZ
#include "zlib.h"
#endif


/* gs_tar offset */
static const int TAR_LEN_LEFT = 1048;
static const int TAR_FILE_PADDING = 511; /*  All files are padded up to 512 bytes */
static const int TAR_FILE_TYPE = 1080;   /* file type */
static const int TAR_BLOCK_SIZE = 2560;  /* gs_tar block size */
static const int TAR_FILE_MODE = 1024;

/* fileStream result */
static const int TAR_READ_ERROR = -2;

/* file type */
static const char TAR_TYPE_DICTORY = '5';
static const int FILE_PERMISSION = 16832;

#define PADDING_LEFT(current_len_left) (((current_len_left + TAR_FILE_PADDING) & ~TAR_FILE_PADDING) - current_len_left);

#define CLOSE_AND_RETURN(tarfile) \
    do {                          \
        fclose(tarfile);          \
        return -1;                \
    } while (0)

#define CLOSE_AND_SET_NULL(file) \
    do {                         \
        fclose(file);            \
        file = NULL;             \
    } while (0)

#ifdef HAVE_LIBZ
static const char* get_gz_error(gzFile gzf)
{
    int errnum;
    const char* errmsg = NULL;

    errmsg = gzerror(gzf, &errnum);
    if (errnum == Z_ERRNO)
        return strerror(errno);
    else
        return errmsg;
}

static gzFile openGzFile(const char* filename, int compresslevel, const char* mode = "wb")
{
    gzFile ztarfile = gzopen(filename, mode);
    if (gzsetparams(ztarfile, compresslevel, Z_DEFAULT_STRATEGY) != Z_OK) {
        return NULL;
    }
    return ztarfile;
}

static bool writeGzFile(gzFile ztarfile, char* copybuf, int buf_size)
{
    if (gzwrite(ztarfile, copybuf, buf_size) != buf_size) {
        return false;
    }
    return true;
}
#endif

#endif /* GS_TAR_CONST_H_ */