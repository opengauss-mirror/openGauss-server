/* -------------------------------------------------------------------------
 *
 * compress_io.c
 *	 Routines for archivers to write an uncompressed or compressed data
 *	 stream.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * This file includes two APIs for dealing with compressed data. The first
 * provides more flexibility, using callbacks to read/write data from the
 * underlying stream. The second API is a wrapper around fopen/gzopen and
 * friends, providing an interface similar to those, but abstracts away
 * the possible compression. Both APIs use libz for the compression, but
 * the second API uses gzip headers, so the resulting files can be easily
 * manipulated with the gzip utility.
 *
 * Compressor API
 * --------------
 *
 *	The interface for writing to an archive consists of three functions:
 *	AllocateCompressor, WriteDataToArchive and EndCompressor. First you call
 *	AllocateCompressor, then write all the data by calling WriteDataToArchive
 *	as many times as needed, and finally EndCompressor. WriteDataToArchive
 *	and EndCompressor will call the WriteFunc that was provided to
 *	AllocateCompressor for each chunk of compressed data.
 *
 *	The interface for reading an archive consists of just one function:
 *	ReadDataFromArchive. ReadDataFromArchive reads the whole compressed input
 *	stream, by repeatedly calling the given ReadFunc. ReadFunc returns the
 *	compressed data chunk at a time, and ReadDataFromArchive decompresses it
 *	and passes the decompressed data to ahwrite(), until ReadFunc returns 0
 *	to signal EOF.
 *
 *	The interface is the same for compressed and uncompressed streams.
 *
 * Compressed stream API
 * ----------------------
 *
 *	The compressed stream API is a wrapper around the C standard fopen() and
 *	libz's gzopen() APIs. It allows you to use the same functions for
 *	compressed and uncompressed streams. cfopen_read() first tries to open
 *	the file with given name, and if it fails, it tries to open the same
 *	file with the .gz suffix. cfopen_write() opens a file for writing, an
 *	extra argument specifies if the file should be compressed, and adds the
 *	.gz suffix to the filename if so. This allows you to easily handle both
 *	compressed and uncompressed files.
 *
 * IDENTIFICATION
 *	   src/bin/pg_dump/compress_io.c
 *
 * -------------------------------------------------------------------------
 */

#include "compress_io.h"
#include "dumpmem.h"
#include <sys/file.h>

#ifdef GAUSS_SFT_TEST
#include "gauss_sft.h"
#endif

/* ----------------------
 * Compressor API
 * ----------------------
 */

/* typedef appears in compress_io.h */
struct CompressorState {
    CompressionAlgorithm comprAlg;
    WriteFunc writeF;

#ifdef HAVE_LIBZ
    z_streamp zp;
    char* zlibOut;
    size_t zlibOutSize;
#endif
};

/* translator: this is a module name */
static const char* modulename = gettext_noop("compress_io");

static void ParseCompressionOption(int compression, CompressionAlgorithm* alg, int* level);

/* Routines that support zlib compressed data I/O */
#ifdef HAVE_LIBZ
static void InitCompressorZlib(CompressorState* cs, int level);
static void DeflateCompressorZlib(ArchiveHandle* AH, CompressorState* cs, bool flush);
static void ReadDataFromArchiveZlib(ArchiveHandle* AH, ReadFunc readF);
static size_t WriteDataToArchiveZlib(ArchiveHandle* AH, CompressorState* cs, const char* data, size_t dLen);
static void EndCompressorZlib(ArchiveHandle* AH, CompressorState* cs);
#endif

/* Routines that support uncompressed data I/O */
static void ReadDataFromArchiveNone(ArchiveHandle* AH, ReadFunc readF);
static size_t WriteDataToArchiveNone(ArchiveHandle* AH, CompressorState* cs, const char* data, size_t dLen);

/*
 * Interprets a numeric 'compression' value. The algorithm implied by the
 * value (zlib or none at the moment), is returned in *alg, and the
 * zlib compression level in *level.
 */
static void ParseCompressionOption(int compression, CompressionAlgorithm* alg, int* level)
{
    if (compression == Z_DEFAULT_COMPRESSION || (compression > 0 && compression <= 9)) {
        *alg = COMPR_ALG_LIBZ;
    } else if (compression == 0) {
        *alg = COMPR_ALG_NONE;
    } else {
        exit_horribly(modulename, "invalid compression code: %d\n", compression);
        *alg = COMPR_ALG_NONE; /* keep compiler quiet */
    }

    /* The level is just the passed-in value. */
    if (level != NULL)
        *level = compression;
}

#ifdef ENABLE_UT
void uttest_ParseCompressionOption(int compression, CompressionAlgorithm* alg, int* level)
{
    ParseCompressionOption(compression, alg, level);
}
#endif

/* Public interface routines */
/* Allocate a new compressor */
CompressorState* AllocateCompressor(int compression, WriteFunc writeF)
{
    CompressorState* cs = NULL;
    CompressionAlgorithm alg = COMPR_ALG_NONE;
    int level = 0;

    ParseCompressionOption(compression, &alg, &level);

#ifndef HAVE_LIBZ
    if (alg == COMPR_ALG_LIBZ)
        exit_horribly(modulename, "not built with zlib support\n");
#endif

    cs = (CompressorState*)pg_calloc(1, sizeof(CompressorState));
    cs->writeF = writeF;
    cs->comprAlg = alg;

    /*
     * Perform compression algorithm specific initialization.
     */
#ifdef HAVE_LIBZ
    if (alg == COMPR_ALG_LIBZ)
        InitCompressorZlib(cs, level);
#endif

    return cs;
}

/*
 * Read all compressed data from the input stream (via readF) and print it
 * out with ahwrite().
 */
void ReadDataFromArchive(ArchiveHandle* AH, int compression, ReadFunc readF)
{
    CompressionAlgorithm alg = COMPR_ALG_NONE;

    ParseCompressionOption(compression, &alg, NULL);

    if (alg == COMPR_ALG_NONE) {
        ReadDataFromArchiveNone(AH, readF);
    }
    if (alg == COMPR_ALG_LIBZ) {
#ifdef HAVE_LIBZ
        ReadDataFromArchiveZlib(AH, readF);
#else
        exit_horribly(modulename, "not built with zlib support\n");
#endif
    }
}

/*
 * Compress and write data to the output stream (via writeF).
 */
size_t WriteDataToArchive(ArchiveHandle* AH, CompressorState* cs, const void* data, size_t dLen)
{
    switch (cs->comprAlg) {
        case COMPR_ALG_LIBZ:
#ifdef HAVE_LIBZ
            return WriteDataToArchiveZlib(AH, cs, (const char*)data, dLen);
#else
            exit_horribly(modulename, "not built with zlib support\n");
#endif
        // case COMPR_ALG_NONE:
        default:  // COMPR_ALG_NONE
            return WriteDataToArchiveNone(AH, cs, (const char*)data, dLen);
    }
    /* keep compiler quiet */
}

/*
 * Terminate compression library context and flush its buffers.
 */
void EndCompressor(ArchiveHandle* AH, CompressorState* cs)
{
#ifdef HAVE_LIBZ
    if (cs->comprAlg == COMPR_ALG_LIBZ)
        EndCompressorZlib(AH, cs);
#endif
    free(cs);
    cs = NULL;
}

/* Private routines, specific to each compression method. */
#ifdef HAVE_LIBZ
/*
 * Functions for zlib compressed output.
 */

static void InitCompressorZlib(CompressorState* cs, int level)
{
    z_streamp zp;

    zp = cs->zp = (z_streamp)pg_malloc(sizeof(z_stream));
    zp->zalloc = Z_NULL;
    zp->zfree = Z_NULL;
    zp->opaque = Z_NULL;

    /*
     * zlibOutSize is the buffer size we tell zlib it can output to.  We
     * actually allocate one extra byte because some routines want to append a
     * trailing zero byte to the zlib output.
     */
    cs->zlibOut = (char*)pg_malloc(ZLIB_OUT_SIZE + 1);
    cs->zlibOutSize = ZLIB_OUT_SIZE;

    if (deflateInit(zp, level) != Z_OK)
        exit_horribly(modulename, "could not initialize compression library: %s\n", zp->msg);

    /* Just be paranoid - maybe End is called after Start, with no Write */
    zp->next_out = (Bytef*)cs->zlibOut;
    zp->avail_out = cs->zlibOutSize;
}

static void EndCompressorZlib(ArchiveHandle* AH, CompressorState* cs)
{
    z_streamp zp = cs->zp;

    zp->next_in = NULL;
    zp->avail_in = 0;

    /* Flush any remaining data from zlib buffer */
    DeflateCompressorZlib(AH, cs, true);

    if (deflateEnd(zp) != Z_OK)
        exit_horribly(modulename, "could not close compression stream: %s\n", zp->msg);

    free(cs->zlibOut);
    cs->zlibOut = NULL;
    free(cs->zp);
    cs->zp = NULL;
}

static void DeflateCompressorZlib(ArchiveHandle* AH, CompressorState* cs, bool flush)
{
    z_streamp zp = cs->zp;
    char* out = cs->zlibOut;
    int res = Z_OK;

    while (cs->zp->avail_in != 0 || flush) {
        res = deflate(zp, flush ? Z_FINISH : Z_NO_FLUSH);
        if (res == Z_STREAM_ERROR)
            exit_horribly(modulename, "could not compress data: %s\n", zp->msg);
        if ((flush && (zp->avail_out < cs->zlibOutSize)) || (zp->avail_out == 0) || (zp->avail_in != 0)) {
            /*
             * Extra paranoia: avoid zero-length chunks, since a zero length
             * chunk is the EOF marker in the custom format. This should never
             * happen but...
             */
            if (zp->avail_out < cs->zlibOutSize) {
                /*
                 * Any write function shoud do its own error checking but to
                 * make sure we do a check here as well...
                 */
                size_t len = cs->zlibOutSize - zp->avail_out;

                if (cs->writeF(AH, out, len) != len)
                    exit_horribly(modulename, "could not write to output file: %s\n", strerror(errno));
            }
            zp->next_out = (Bytef*)out;
            zp->avail_out = cs->zlibOutSize;
        }

        if (res == Z_STREAM_END)
            break;
    }
}

static size_t WriteDataToArchiveZlib(ArchiveHandle* AH, CompressorState* cs, const char* data, size_t dLen)
{
    cs->zp->next_in = (Bytef*)data;
    cs->zp->avail_in = dLen;
    DeflateCompressorZlib(AH, cs, false);

    /*
     * we have either succeeded in writing dLen bytes or we have called
     * exit_horribly()
     */
    return dLen;
}

static void ReadDataFromArchiveZlib(ArchiveHandle* AH, ReadFunc readF)
{
    char* out = NULL;
    int res = Z_OK;
    size_t cnt = 0;
    char* buf = NULL;

    z_streamp zp = (z_streamp)pg_malloc(sizeof(z_stream));
    zp->zalloc = Z_NULL;
    zp->zfree = Z_NULL;
    zp->opaque = Z_NULL;

    buf = (char*)pg_malloc(ZLIB_IN_SIZE);
    size_t buflen = ZLIB_IN_SIZE;

    out = (char*)pg_malloc(ZLIB_OUT_SIZE + 1);

    if (inflateInit(zp) != Z_OK)
        exit_horribly(modulename, "could not initialize compression library: %s\n", zp->msg);

    /* no minimal chunk size for zlib */
    while ((cnt = readF(AH, &buf, &buflen))) {
        zp->next_in = (Bytef*)buf;
        zp->avail_in = cnt;

        while (zp->avail_in > 0) {
            zp->next_out = (Bytef*)out;
            zp->avail_out = ZLIB_OUT_SIZE;

            res = inflate(zp, 0);
            if (res != Z_OK && res != Z_STREAM_END)
                exit_horribly(modulename, "could not uncompress data: %s\n", zp->msg);

            out[ZLIB_OUT_SIZE - zp->avail_out] = '\0';
            ahwrite(out, 1, ZLIB_OUT_SIZE - zp->avail_out, AH);
        }
    }

    zp->next_in = NULL;
    zp->avail_in = 0;
    while (res != Z_STREAM_END) {
        zp->next_out = (Bytef*)out;
        zp->avail_out = ZLIB_OUT_SIZE;
        res = inflate(zp, 0);
        if (res != Z_OK && res != Z_STREAM_END)
            exit_horribly(modulename, "could not uncompress data: %s\n", zp->msg);

        out[ZLIB_OUT_SIZE - zp->avail_out] = '\0';
        ahwrite(out, 1, ZLIB_OUT_SIZE - zp->avail_out, AH);
    }

    if (inflateEnd(zp) != Z_OK)
        exit_horribly(modulename, "could not close compression library: %s\n", zp->msg);

    free(buf);
    buf = NULL;
    free(out);
    out = NULL;
    free(zp);
    zp = NULL;
}
#endif /* HAVE_LIBZ */

/*
 * Functions for uncompressed output.
 */

static void ReadDataFromArchiveNone(ArchiveHandle* AH, ReadFunc readF)
{
    size_t cnt;
    char* buf = NULL;
    size_t buflen;

    buf = (char*)pg_malloc(ZLIB_OUT_SIZE);
    buflen = ZLIB_OUT_SIZE;

    while ((cnt = readF(AH, &buf, &buflen))) {
        (void)ahwrite(buf, 1, cnt, AH);
    }

    free(buf);
    buf = NULL;
}

static size_t WriteDataToArchiveNone(ArchiveHandle* AH, CompressorState* cs, const char* data, size_t dLen)
{
    /*
     * Any write function should do its own error checking but to make sure we
     * do a check here as well...
     */
    if (cs->writeF(AH, data, dLen) != dLen)
        exit_horribly(modulename, "could not write to output file: %s\n", strerror(errno));
    return dLen;
}

/* ----------------------
 * Compressed stream API
 * ----------------------
 */

/*
 * cfp represents an open stream, wrapping the underlying FILE or gzFile
 * pointer. This is opaque to the callers.
 */
struct cfp {
    FILE* uncompressedfp;
#ifdef HAVE_LIBZ
    gzFile compressedfp;
#endif
    FILE* lock;
};

#ifdef HAVE_LIBZ
static int hasSuffix(const char* filename, const char* suffix);
#endif

/* free() without changing errno; useful in several places below */
static void free_keep_errno(void* p)
{
    int save_errno = errno;

    free(p);
    p = NULL;
    errno = save_errno;
}

/*
 * Open a file for reading. 'path' is the file to open, and 'mode' should
 * be either "r" or "rb".
 *
 * If the file at 'path' does not exist, we append the ".gz" suffix (if 'path'
 * doesn't already have it) and try again. So if you pass "foo" as 'path',
 * this will open either "foo" or "foo.gz".
 *
 * On failure, return NULL with an error code in errno.
 */
cfp* cfopen_read(const char* path, const char* mode)
{
    cfp* fp = NULL;

#ifdef HAVE_LIBZ
    if (hasSuffix(path, ".gz"))
        fp = cfopen(path, mode, 1);
    else
#endif
    {
        fp = cfopen(path, mode, 0);
#ifdef HAVE_LIBZ
        if (fp == NULL) {
            int fnamelen = strlen(path) + 4;
            char* fname = (char*)pg_malloc(fnamelen);
            int nRet = 0;

            nRet = snprintf_s(fname, fnamelen, fnamelen - 1, "%s%s", path, ".gz");
            securec_check_ss_c(nRet, fname, "\0");
            fp = cfopen(fname, mode, 1);
            free_keep_errno(fname);
            fname = NULL;
        }
#endif
    }
    return fp;
}

/*
 * Open a file for writing. 'path' indicates the path name, and 'mode' must
 * be a filemode as accepted by fopen() and gzopen() that indicates writing
 * ("w", "wb", "a", or "ab").
 *
 * If 'compression' is non-zero, a gzip compressed stream is opened, and
 * 'compression' indicates the compression level used. The ".gz" suffix
 * is automatically added to 'path' in that case.
 *
 * On failure, return NULL with an error code in errno.
 */
cfp* cfopen_write(const char* path, const char* mode, int compression)
{
    cfp* fp = NULL;

    if (compression == 0)
        fp = cfopen(path, mode, 0);
    else {
#ifdef HAVE_LIBZ
        int fnamelen = strlen(path) + 4;
        char* fname = (char*)pg_malloc(fnamelen);
        int nRet = 0;

        nRet = snprintf_s(fname, fnamelen, fnamelen - 1, "%s%s", path, ".gz");
        securec_check_ss_c(nRet, fname, "\0");
        fp = cfopen(fname, mode, compression);
        free_keep_errno(fname);
        fname = NULL;
#else
        fp = NULL; /* keep compiler quiet */
        exit_horribly(modulename, "not built with zlib support\n");
#endif
    }
    return fp;
}

cfp* cfopen_write4Lock(const char* path, const char* mode, int compression)
{
    cfp* fp = NULL;

    if (compression == 0)
        fp = cfopen4Lock(path, mode, 0);
    else {
#ifdef HAVE_LIBZ
        int fnamelen = strlen(path) + 4;
        char* fname = (char*)pg_malloc(fnamelen);
        int nRet = 0;

        nRet = snprintf_s(fname, fnamelen, fnamelen - 1, "%s%s", path, ".gz");
        securec_check_ss_c(nRet, fname, "\0");
        fp = cfopen4Lock(fname, mode, compression);
        free_keep_errno(fname);
        fname = NULL;
#else
        fp = NULL; /* keep compiler quiet */
        exit_horribly(modulename, "not built with zlib support\n");
#endif
    }
    return fp;
}
/*
 * Opens file 'path' in 'mode'. If 'compression' is non-zero, the file
 * is opened with libz gzopen(), otherwise with plain fopen().
 *
 * On failure, return NULL with an error code in errno.
 */
cfp* cfopen(const char* path, const char* mode, int compression)
{
    cfp* fp = (cfp*)pg_malloc(sizeof(cfp));

    if (compression != 0) {
#ifdef HAVE_LIBZ
        char mode_compression[32] = {0};
        int nRet = 0;

        nRet = snprintf_s(mode_compression,
            sizeof(mode_compression) / sizeof(char),
            sizeof(mode_compression) / sizeof(char) - 1,
            "%s%d",
            mode,
            compression);
        securec_check_ss_c(nRet, "\0", "\0");
        fp->compressedfp = gzopen(path, mode_compression);
        fp->uncompressedfp = NULL;
        if (fp->compressedfp == NULL) {
            free_keep_errno(fp);
            fp = NULL;
        }
#else
        exit_horribly(modulename, "not built with zlib support\n");
#endif
    } else {
#ifdef HAVE_LIBZ
        fp->compressedfp = NULL;
#endif
        fp->uncompressedfp = fopen(path, mode);
        if (fp->uncompressedfp == NULL) {
            free_keep_errno(fp);
            fp = NULL;
        }
    }

    /* update file permission */
    if (chmod(path, FILE_PERMISSION) == -1) {
        exit_horribly(modulename, "changing permissions for file \"%s\" failed with: %s\n", path, strerror(errno));
    }

    return fp;
}

cfp* cfopen4Lock(const char* path, const char* mode, int compression)
{
    cfp* fp = (cfp*)pg_malloc(sizeof(cfp));

    if (compression != 0) {
#ifdef HAVE_LIBZ
        char mode_compression[32] = {0};
        int nRet = 0;

        nRet = snprintf_s(mode_compression,
            sizeof(mode_compression) / sizeof(char),
            sizeof(mode_compression) / sizeof(char) - 1,
            "%s%d",
            mode,
            compression);
        securec_check_ss_c(nRet, "\0", "\0");
        fp->lock = fopen(path, mode);
        if (fp->lock == NULL) {
            free_keep_errno(fp);
            fp = NULL;
        }
        fp->compressedfp = gzdopen(fileno(fp->lock), mode_compression);
        fp->uncompressedfp = NULL;
        if (fp->compressedfp == NULL) {
            free_keep_errno(fp);
            fp = NULL;
        }
#else
        exit_horribly(modulename, "not built with zlib support\n");
#endif
    } else {
#ifdef HAVE_LIBZ
        fp->compressedfp = NULL;
#endif
        fp->uncompressedfp = fopen(path, mode);
        if (fp->uncompressedfp == NULL) {
            free_keep_errno(fp);
            fp = NULL;
        }
        fp->lock = fp->uncompressedfp;
    }

    /* update file permission */
    if (chmod(path, FILE_PERMISSION) == -1) {
        exit_horribly(modulename, "changing permissions for file \"%s\" failed with: %s\n", path, strerror(errno));
    }

    return fp;
}
int cfread(void* ptr, int size, cfp* fp)
{
    size_t read_len;
#ifdef HAVE_LIBZ
    if (fp->compressedfp != NULL) {
        read_len = gzread_file(ptr, size, fp->compressedfp);
    } else
#endif
        read_len = fread_file(ptr, 1, size, fp->uncompressedfp);
    return read_len;
}

int cfwrite(const void* ptr, int size, cfp* fp)
{
#ifdef HAVE_LIBZ
    if (fp->compressedfp != NULL)
        return gzwrite(fp->compressedfp, ptr, size);
    else
#endif
        return fwrite(ptr, 1, size, fp->uncompressedfp);
}

int cfgetc(cfp* fp)
{
#ifdef HAVE_LIBZ
    if (fp->compressedfp != NULL)
        return gzgetc(fp->compressedfp);
    else
#endif
        return fgetc(fp->uncompressedfp);
}

char* cfgets(cfp* fp, char* buf, int len)
{
#ifdef HAVE_LIBZ
    if (fp->compressedfp != NULL)
        return gzgets(fp->compressedfp, buf, len);
    else
#endif
        return fgets(buf, len, fp->uncompressedfp);
}

int cfclose(cfp* fp)
{
    int result;

    if (fp == NULL) {
        errno = EBADF;
        return EOF;
    }
#ifdef HAVE_LIBZ
    if (fp->compressedfp != NULL) {
        result = gzclose(fp->compressedfp);
        fp->compressedfp = NULL;
    } else
#endif
    {
        result = fclose(fp->uncompressedfp);
        fp->uncompressedfp = NULL;
    }
    free_keep_errno(fp);

    return result;
}

int cfeof(cfp* fp)
{
#ifdef HAVE_LIBZ
    if (fp->compressedfp != NULL)
        return gzeof(fp->compressedfp);
    else
#endif
        return feof(fp->uncompressedfp);
}

#ifdef HAVE_LIBZ
static int hasSuffix(const char* filename, const char* suffix)
{
    int filenamelen = strlen(filename);
    int suffixlen = strlen(suffix);

    if (filenamelen < suffixlen)
        return 0;

    return memcmp(&filename[filenamelen - suffixlen], suffix, suffixlen) == 0;
}
#endif
