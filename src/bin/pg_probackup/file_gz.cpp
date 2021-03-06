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
#include "common/fe_memutils.h"

#ifdef HAVE_LIBZ

#define ZLIB_BUFFER_SIZE     (64*1024)
#define MAX_WBITS            15 /* 32K LZ77 window */
#define DEF_MEM_LEVEL        8
#define FIO_GZ_REMOTE_MARKER 1

typedef struct fioGZFile
{
    z_stream strm;
    int      fd;
    int      errnum;
    bool     compress;
    bool     eof;
    Bytef    buf[ZLIB_BUFFER_SIZE];
} fioGZFile;

/* On error returns NULL and errno should be checked */
gzFile
fio_gzopen(char const* path, char const* mode, int level, fio_location location)
{
    int rc;
    if (fio_is_remote(location))
    {
        fioGZFile* gz = (fioGZFile*) pgut_malloc(sizeof(fioGZFile));
        rc = memset_s(&gz->strm, sizeof(gz->strm), 0, sizeof(gz->strm));
        securec_check(rc, "\0", "\0");
        gz->eof = 0;
        gz->errnum = Z_OK;
        /* check if file opened for writing */
        if (strcmp(mode, PG_BINARY_W) == 0) /* compress */
        {
            gz->strm.next_out = gz->buf;
            gz->strm.avail_out = ZLIB_BUFFER_SIZE;
            rc = deflateInit2(&gz->strm,
                                    level,
                                    Z_DEFLATED,
                                    MAX_WBITS + 16, DEF_MEM_LEVEL,
                                    Z_DEFAULT_STRATEGY);
            if (rc == Z_OK)
            {
                gz->compress = 1;
                gz->fd = fio_open(path, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, location);
                if (gz->fd < 0)
                {
                    free(gz);
                    return NULL;
                }
            }
        }
        else
        {
            gz->strm.next_in = gz->buf;
            gz->strm.avail_in = ZLIB_BUFFER_SIZE;
            rc = inflateInit2(&gz->strm, 15 + 16);
            gz->strm.avail_in = 0;
            if (rc == Z_OK)
            {
                gz->compress = 0;
                gz->fd = fio_open(path, O_RDONLY | PG_BINARY, location);
                if (gz->fd < 0)
                {
                    free(gz);
                    return NULL;
                }
            }
        }
        if (rc != Z_OK)
        {
            elog(ERROR, "zlib internal error when opening file %s: %s",
            path, gz->strm.msg);
        }
        return (gzFile)((size_t)gz + FIO_GZ_REMOTE_MARKER);
    }
    else
    {
        gzFile file;
        /* check if file opened for writing */
        if (strcmp(mode, PG_BINARY_W) == 0)
        {
            int fd = open(path, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, FILE_PERMISSIONS);
            if (fd < 0)
                return NULL;
            file = gzdopen(fd, mode);
        }
        else
            file = gzopen(path, mode);
        if (file != NULL && level != Z_DEFAULT_COMPRESSION)
        {
            if (gzsetparams(file, level, Z_DEFAULT_STRATEGY) != Z_OK)
                elog(ERROR, "Cannot set compression level %d: %s",
                    level, strerror(errno));
        }
        return file;
    }
}

int
fio_gzread(gzFile f, void *buf, unsigned size)
{
    if ((size_t)f & FIO_GZ_REMOTE_MARKER)
    {
        int rc;
        fioGZFile* gz = (fioGZFile*)((size_t)f - FIO_GZ_REMOTE_MARKER);

        if (gz->eof)
        {
            return 0;
        }

        gz->strm.next_out = (Bytef *)buf;
        gz->strm.avail_out = size;

        while (1)
        {
            if (gz->strm.avail_in != 0) /* If there is some data in receiver buffer, then decompress it */
            {
                rc = inflate(&gz->strm, Z_NO_FLUSH);
                if (rc == Z_STREAM_END)
                {
                    gz->eof = 1;
                }
                else if (rc != Z_OK)
                {
                    gz->errnum = rc;
                    return -1;
                }
                if (gz->strm.avail_out != size)
                {
                    return size - gz->strm.avail_out;
                }
                if (gz->strm.avail_in == 0)
                {
                    gz->strm.next_in = gz->buf;
                }
            }
            else
            {
                gz->strm.next_in = gz->buf;
            }
            rc = fio_read(gz->fd, gz->strm.next_in + gz->strm.avail_in,
            gz->buf + ZLIB_BUFFER_SIZE - gz->strm.next_in - gz->strm.avail_in);
            if (rc > 0)
            {
                gz->strm.avail_in += rc;
            }
            else
            {
                if (rc == 0)
                {
                    gz->eof = 1;
                }
                return rc;
            }
        }
    }
    else
    {
        return gzread(f, buf, size);
    }
}

int
fio_gzwrite(gzFile f, void * buf, unsigned size)
{
    if ((size_t)f & FIO_GZ_REMOTE_MARKER)
    {
        int rc;
        fioGZFile* gz = (fioGZFile*)((size_t)f - FIO_GZ_REMOTE_MARKER);

        gz->strm.next_in = (Bytef  *)buf;
        gz->strm.avail_in = size;

        do
        {
            if (gz->strm.avail_out == ZLIB_BUFFER_SIZE) /* Compress buffer is empty */
            {
                gz->strm.next_out = gz->buf; /* Reset pointer to the  beginning of buffer */

                if (gz->strm.avail_in != 0) /* Has something in input buffer */
                {
                    rc = deflate(&gz->strm, Z_NO_FLUSH);
                    Assert(rc == Z_OK);
                    gz->strm.next_out = gz->buf; /* Reset pointer to the  beginning of buffer */
                }
                else
                {
                    break;
                }
            }
            rc = fio_write(gz->fd, gz->strm.next_out, ZLIB_BUFFER_SIZE - gz->strm.avail_out);
            if (rc >= 0)
            {
                gz->strm.next_out += rc;
                gz->strm.avail_out += rc;
            }
            else
            {
                return rc;
            }
        } while (gz->strm.avail_out != ZLIB_BUFFER_SIZE || gz->strm.avail_in != 0);

        return size;
    }
    else
    {
        return gzwrite(f, buf, size);
    }
}

int
fio_gzclose(gzFile f)
{
    if ((size_t)f & FIO_GZ_REMOTE_MARKER)
    {
        fioGZFile* gz = (fioGZFile*)((size_t)f - FIO_GZ_REMOTE_MARKER);
        int rc;
        if (gz->compress)
        {
            gz->strm.next_out = gz->buf;
            rc = deflate(&gz->strm, Z_FINISH);
            Assert(rc == Z_STREAM_END && gz->strm.avail_out != ZLIB_BUFFER_SIZE);
            deflateEnd(&gz->strm);
            rc = fio_write(gz->fd, gz->buf, ZLIB_BUFFER_SIZE - gz->strm.avail_out);
            if (rc != (int)(ZLIB_BUFFER_SIZE - gz->strm.avail_out))
            {
                return -1;
            }
        }
        else
        {
            inflateEnd(&gz->strm);
        }
        rc = fio_close(gz->fd);
        free(gz);
        return rc;
    }
    else
    {
        return gzclose(f);
    }
}

int fio_gzeof(gzFile f)
{
    if ((size_t)f & FIO_GZ_REMOTE_MARKER)
    {
        fioGZFile* gz = (fioGZFile*)((size_t)f - FIO_GZ_REMOTE_MARKER);
        return (int)gz->eof;
    }
    else
    {
        return gzeof(f);
    }
}

const char* fio_gzerror(gzFile f, int *errnum)
{
    if ((size_t)f & FIO_GZ_REMOTE_MARKER)
    {
        fioGZFile* gz = (fioGZFile*)((size_t)f - FIO_GZ_REMOTE_MARKER);
        if (errnum)
            *errnum = gz->errnum;
        return gz->strm.msg;
    }
    else
    {
        return gzerror(f, errnum);
    }
}

z_off_t fio_gzseek(gzFile f, z_off_t offset, int whence)
{
    Assert(!((size_t)f & FIO_GZ_REMOTE_MARKER));
    return gzseek(f, offset, whence);
}


/* Receive chunks of compressed data, decompress them and write to
 * destination file.
 * Return codes:
 *   FILE_MISSING (-1)
 *   OPEN_FAILED  (-2)
 *   READ_FAILED  (-3)
 *   WRITE_FAILED (-4)
 *   ZLIB_ERROR   (-5)
 *   REMOTE_ERROR (-6)
 */
int fio_send_file_gz(const char *from_fullpath, const char *to_fullpath, FILE* out, char **errormsg)
{
    fio_header hdr;
    int exit_code = SEND_OK;
    char *in_buf = (char *)pgut_malloc(CHUNK_SIZE);    /* buffer for compressed data */
    char *out_buf = (char *)pgut_malloc(OUT_BUF_SIZE); /* 1MB buffer for decompressed data */
    size_t path_len = strlen(from_fullpath) + 1;
    /* decompressor */
    z_stream *strm = NULL;
    int nRet = 0;

    hdr.cop = FIO_SEND_FILE;
    hdr.size = path_len;

    

    IO_CHECK(fio_write_all(fio_stdout, &hdr, sizeof(hdr)), sizeof(hdr));
    IO_CHECK(fio_write_all(fio_stdout, from_fullpath, path_len), (ssize_t)path_len);

    for (;;)
    {
        fio_header hdr;
        IO_CHECK(fio_read_all(fio_stdin, &hdr, sizeof(hdr)), sizeof(hdr));

        if (hdr.cop == FIO_SEND_FILE_EOF)
        {
            break;
        }
        else if (hdr.cop == FIO_ERROR)
        {
            bool valid = hdr.size > 0 && hdr.size <= CHUNK_SIZE;
            /* handle error, reported by the agent */
            if (valid)
            {
                IO_CHECK(fio_read_all(fio_stdin, in_buf, hdr.size), (ssize_t)hdr.size);
                *errormsg = (char *)pgut_malloc(hdr.size);
                nRet = snprintf_s(*errormsg, hdr.size, hdr.size - 1, "%s", in_buf);
                securec_check_ss_c(nRet, "\0", "\0");
            }
            exit_code = hdr.arg;
            goto cleanup;
        }
        else if (hdr.cop == FIO_PAGE)
        {
            int rc;
            Assert(hdr.size <= CHUNK_SIZE);
            if (hdr.size > CHUNK_SIZE) {
                hdr.size = CHUNK_SIZE;
            }
            IO_CHECK(fio_read_all(fio_stdin, in_buf, hdr.size), (ssize_t)hdr.size);

            /* We have received a chunk of compressed data, lets decompress it */
            if (strm == NULL)
            {
                /* Initialize decompressor */
                strm = (z_stream *)pgut_malloc(sizeof(z_stream));
                rc = memset_s(strm, sizeof(z_stream), 0, sizeof(z_stream));
                securec_check(rc, "\0", "\0");

                /* The fields next_in, avail_in initialized before init */
                strm->next_in = (Bytef *)in_buf;
                strm->avail_in = hdr.size;

                rc = inflateInit2(strm, 15 + 16);

                if (rc != Z_OK)
                {
                    *errormsg = (char *)pgut_malloc(ERRMSG_MAX_LEN);
                    nRet = snprintf_s(*errormsg, ERRMSG_MAX_LEN, ERRMSG_MAX_LEN - 1,
                                    "Failed to initialize decompression stream for file '%s': %i: %s",
                                    from_fullpath, rc, strm->msg);
                    securec_check_ss_c(nRet, "\0", "\0");
                    exit_code = ZLIB_ERROR;
                    goto cleanup;
                }
            }
            else
            {
                strm->next_in = (Bytef *)in_buf;
                strm->avail_in = hdr.size;
            }

            strm->next_out = (Bytef *)out_buf; /* output buffer */
            strm->avail_out = OUT_BUF_SIZE; /* free space in output buffer */

            /*
            * From zlib documentation:
            * The application must update next_in and avail_in when avail_in
            * has dropped to zero. It must update next_out and avail_out when
            * avail_out has dropped to zero.
            */
            while (strm->avail_in != 0) /* while there is data in input buffer, decompress it */
            {
                /* decompress until there is no data to decompress,
                * or buffer with uncompressed data is full
                */
                rc = inflate(strm, Z_NO_FLUSH);
                if (rc == Z_STREAM_END)
                    /* end of stream */
                    break;
                else if (rc != Z_OK)
                {
                    /* got an error */
                    *errormsg = (char *)pgut_malloc(ERRMSG_MAX_LEN);
                    nRet = snprintf_s(*errormsg, ERRMSG_MAX_LEN,ERRMSG_MAX_LEN - 1,
                                    "Decompression failed for file '%s': %i: %s",
                                    from_fullpath, rc, strm->msg);
                    securec_check_ss_c(nRet, "\0", "\0");
                    exit_code = ZLIB_ERROR;
                    goto cleanup;
                }

                if (strm->avail_out == 0)
                {
                    /* Output buffer is full, write it out */
                    if (fwrite(out_buf, 1, OUT_BUF_SIZE, out) != OUT_BUF_SIZE)
                    {
                        exit_code = WRITE_FAILED;
                        goto cleanup;
                    }

                    strm->next_out = (Bytef *)out_buf; /* output buffer */
                    strm->avail_out = OUT_BUF_SIZE;
                }
            }

            /* write out leftovers if any */
            if (strm->avail_out != OUT_BUF_SIZE)
            {
                int len = OUT_BUF_SIZE - strm->avail_out;

                if (fwrite(out_buf, 1, len, out) != (size_t)len)
                {
                    exit_code = WRITE_FAILED;
                    goto cleanup;
                }
            }
        }
        else
            elog(ERROR, "Remote agent returned message of unexpected type: %i", hdr.cop);
    }

    cleanup:
    if (exit_code < OPEN_FAILED)
        fio_disconnect(); /* discard possible pending data in pipe */

    if (strm)
    {
        inflateEnd(strm);
        pg_free(strm);
    }

    pg_free(in_buf);
    pg_free(out_buf);
    return exit_code;
}
#endif


