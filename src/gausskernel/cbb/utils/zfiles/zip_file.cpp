/* ----------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1998-2004 Gilles Vollant
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/zfiles/zip_file.cpp
 *
 * ----------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/smgr/fd.h"
#include "utils/palloc.h"
#include "utils/zfiles.h"

/* MALLOC api for struct unz_memfunc */
static voidp memcnxt_palloc(size_t s)
{
    return palloc(s);
}

/* FREE api for struct unz_memfunc */
static void memcnxt_pfree(voidp p)
{
    pfree(p);
}

/*
 * Postmaster will call this function at startup time
 * to set memory API for MINIUNZ lib.
 * struct unz_memfunc has two members:
 * 1) MALLOC api whose type is:  typedef voidp (* malloc_pf) (size_t);
 * 2) FREE api whose type is:    typedef void  (* free_pf) (voidp);
 *
 * see also:
 *     3rd_src/zlib1.2.11/unzip_alloc_hook.patch
 *     3rd_src/zlib1.2.11/unzip_alloc_hook.patch2
 */
void pm_set_unzip_memfuncs(void)
{
    setUnzipMemoryFunc(memcnxt_palloc, memcnxt_pfree);
}

/* open a log file using vfd method */
void* vfd_file_open(const char* logfile)
{
    opaque_vfd* vfd = (opaque_vfd*)palloc(sizeof(opaque_vfd));
    vfd->m_tmp_vfd = FILE_INVALID;
    vfd->m_cur_off = 0;
    vfd->m_max_off = -1; /* unassigned offset */
    vfd->m_errno = 0;

    vfd->m_tmp_vfd = PathNameOpenFile((FileName)logfile, O_RDWR | PG_BINARY, 0);
    vfd->m_errno = errno;
    /* immediately report error if open failed */
    if (FILE_INVALID == vfd->m_tmp_vfd) {
        ereport(ERROR, (errmsg("vfd_file_open failed: vfd(-1), errno %d, file \"%s\" ", vfd->m_errno, logfile)));
        return NULL; /* make complier silent */
    }
    return (void*)vfd;
}

/* read some data from log file using vfd method */
int vfd_file_read(void* fobj, char* buf, int bufsize)
{
    opaque_vfd* vfd = (opaque_vfd*)fobj;
    int read_size = FilePRead(vfd->m_tmp_vfd, buf, bufsize, vfd->m_cur_off);
    vfd->m_errno = errno;
    if (read_size > 0) {
        vfd->m_cur_off += read_size;
    }
    return read_size;
}

void vfd_file_close(void* fobj)
{
    opaque_vfd* vfd = (opaque_vfd*)fobj;
    FileClose(vfd->m_tmp_vfd);
    pfree(vfd);
}

/*
 * the thief (like gzclose_r) will close the plain kernel fd before
 * this function is called. here we just close the vfd itself,
 * more details see FileCloseWithThief(). gz_close api is from the
 * third opensource, and its behavior CANNOT be changed arbitrary.
 * so customize this function and it's the caller's responsibility
 * for closing kernel fd first if you want to call this function.
 */
void vfd_file_close_with_thief(void* fobj)
{
    opaque_vfd* vfd = (opaque_vfd*)fobj;
    FileCloseWithThief(vfd->m_tmp_vfd);
    pfree(vfd);
}

/*
 * the prototype of this function is required by
 * zlib_filefunc64_def::zopen64_file. it's the
 * same with vfd_file_open().
 */
static voidp vfd_file_open2(voidp opaque, const void* filename, int mode)
{
    opaque_vfd* vfd = (opaque_vfd*)opaque;
    vfd->m_cur_off = 0;
    vfd->m_max_off = -1; /* unassigned offset */
    vfd->m_tmp_vfd = FILE_INVALID;
    vfd->m_tmp_vfd = PathNameOpenFile((FileName)filename, O_RDWR | PG_BINARY, 0);
    vfd->m_errno = errno;
    /* immediately report error if open failed */
    if (FILE_INVALID == vfd->m_tmp_vfd) {
        ereport(ERROR,
            (errmsg("vfd_file_open2 failed: vfd(-1), errno %d, file \"%s\" ", vfd->m_errno, (FileName)filename)));
        return NULL; /* make complier silent */
    }
    return (voidp)vfd;
}

/*
 * the prototype of this function is required by
 * zlib_filefunc64_def::zread_file. it's the
 * same with vfd_file_read().
 */
static uLong vfd_file_read2(voidpf opaque, voidpf stream, void* buf, uLong size)
{
    Assert(size < (int)(1024 * 1024 * 1024));
    int read_size = vfd_file_read(opaque, (char*)buf, (int)size);
    if (read_size >= 0) {
        return read_size;
    } else {
        opaque_vfd* vfd = (opaque_vfd*)opaque;
        ereport(ERROR, (errmsg("vfd_file_read2 failed: errno %d, returned size %d", vfd->m_errno, read_size)));
        return 0; /* make complier silent */
    }
}

/*
 * the prototype of this function is required by
 * zlib_filefunc64_def::zwrite_file.
 */
static uLong vfd_file_write2(voidpf opaque, voidpf stream, const void* buf, uLong size)
{
    opaque_vfd* vfd = (opaque_vfd*)opaque;
    Assert(size < (int)(1024 * 1024 * 1024));
    int wsize = FilePWrite(vfd->m_tmp_vfd, (char*)buf, (int)size, vfd->m_cur_off);
    vfd->m_errno = errno;
    if (wsize >= 0) {
        vfd->m_cur_off += (off_t)wsize;
        return wsize;
    } else {
        ereport(ERROR, (errmsg("vfd_file_write2 failed: errno %d, returned size %d", vfd->m_errno, wsize)));
        return 0; /* make complier silent */
    }
}

/*
 * the prototype of this function is required by
 * zlib_filefunc64_def::zclose_file. it's the
 * same with vfd_file_close().
 */
static int vfd_file_close2(voidpf opaque, voidpf stream)
{
    opaque_vfd* vfd = (opaque_vfd*)opaque;
    FileClose(vfd->m_tmp_vfd);
    return 0;
}

static int vfd_file_tellerror(voidpf opaque, voidpf stream)
{
    opaque_vfd* vfd = (opaque_vfd*)opaque;
    return vfd->m_errno;
}

static ZPOS64_T vfd_file_tell(voidpf opaque, voidpf stream)
{
    opaque_vfd* vfd = (opaque_vfd*)opaque;
    return vfd->m_cur_off;
}

/* convert origin in MINIZIP to whence needed by lseek */
static inline int convert_to_whence(int origin)
{
    int fseek_origin = 0;
    switch (origin) {
        case ZLIB_FILEFUNC_SEEK_CUR:
            fseek_origin = SEEK_CUR;
            break;
        case ZLIB_FILEFUNC_SEEK_END:
            fseek_origin = SEEK_END;
            break;
        case ZLIB_FILEFUNC_SEEK_SET:
            fseek_origin = SEEK_SET;
            break;
        default:
            return -1;
    }
    return fseek_origin;
}

/*
 * the prototype of this function is required by
 * zlib_filefunc64_def::zseek64_file.
 * notice becuase FileSeek() is not uspported in multithread
 * env, so in this function we just update the file offset data.
 */
static long vfd_file_seek(voidpf opaque, voidpf stream, ZPOS64_T offset, int origin)
{
    opaque_vfd* vfd = (opaque_vfd*)opaque;
    int whence = convert_to_whence(origin);
    int result = -1; /*  0 means success; -1 means fail */

    switch (whence) {
        case SEEK_CUR:
            vfd->m_cur_off += offset; /* offset is based current position */
            result = 0;
            break;
        case SEEK_END:
            if (vfd->m_max_off < 0) {
                /* remember this file's max offset */
                off_t off = FileSeek(vfd->m_tmp_vfd, 0, SEEK_END);
                vfd->m_errno = errno;
                if (off == ((off_t)-1)) { /* -1 meass FileUnknownPos */
                    vfd->m_cur_off = ((off_t)-1) /* FileUnknownPos */;
                    break;
                }
                vfd->m_max_off = off;
            }
            vfd->m_cur_off = vfd->m_max_off + offset; /* offset is based the max position */
            result = 0;
            break;
        case SEEK_SET:
            vfd->m_cur_off = offset; /* offset is based 0 position */
            result = 0;
            break;
        default:
            break;
    }
    return result;
}

void set_unzip_filefuncs(zlib_filefunc64_def* funcs, voidpf opaque)
{
    funcs->zopen64_file = vfd_file_open2;
    funcs->zread_file = vfd_file_read2;
    funcs->zwrite_file = vfd_file_write2;
    funcs->zclose_file = vfd_file_close2;
    funcs->ztell64_file = vfd_file_tell;
    funcs->zerror_file = vfd_file_tellerror;
    funcs->zseek64_file = vfd_file_seek;
    funcs->opaque = opaque;
}

void* unzip_open(const char* zipfile)
{
    zip_reader* zr = (zip_reader*)palloc0(sizeof(zip_reader));

    /*
     * set IO functions for unzip action
     * opaque --> opaque_vfd* type
     */
    zlib_filefunc64_def io_funcs;
    set_unzip_filefuncs(&io_funcs, (voidpf) & (zr->m_vfd));

    /*
     * open an ZIP file for unzip.
     * unzGoToFirstFile() will be called in OPEN function.
     */
    unzFile uf = unzOpen2_64(zipfile, &io_funcs);
    zr->m_unz = uf;
    if (NULL == uf) {
        ereport(ERROR, (errmsg("unzOpen2_64 failed: file \"%s\"", zipfile)));
    }

    /* set the global info for UNZIP file */
    int err = unzGetGlobalInfo64(zr->m_unz, &zr->m_unz_gi);
    if (UNZ_OK != err) {
        ereport(ERROR, (errmsg("unzGetGlobalInfo64 failed: file \"%s\", err %d", zipfile, err)));
    }

    /* open the first file */
    err = unzOpenCurrentFile(zr->m_unz);
    if (UNZ_OK != err) {
        ereport(ERROR, (errmsg("unzOpenCurrentFile failed: file \"%s\", err %d", zipfile, err)));
    }
    return (void*)zr;
}

int unzip_read(void* obj, char* buf, int bufsize)
{
    zip_reader* zr = (zip_reader*)obj;
    int err = UNZ_OK;
    bool open_next = false;

    do {
        /* read current file */
        err = unzReadCurrentFile(zr->m_unz, buf, bufsize);
        if (err > UNZ_OK) {
            break;
        } else if (UNZ_OK == err) {
            /* close this file */
            int tmperr = unzCloseCurrentFile(zr->m_unz);
            if (UNZ_OK != tmperr) {
                ereport(ERROR, (errmsg("unzCloseCurrentFile failed: err %d", tmperr)));
            }
        } else {
            ereport(ERROR, (errmsg("unzReadCurrentFile failed: err %d", err)));
        }

        zr->m_unz_curfile++;
        if (zr->m_unz_curfile < zr->m_unz_gi.number_entry) {
            /* locate to next file */
            err = unzGoToNextFile(zr->m_unz);
            if (UNZ_OK != err) {
                ereport(ERROR, (errmsg("unzGoToNextFile failed: err %d", err)));
            }
            /* open the next file */
            err = unzOpenCurrentFile(zr->m_unz);
            if (UNZ_OK != err) {
                ereport(ERROR, (errmsg("unzOpenCurrentFile failed: err %d", err)));
            }
            open_next = true;
        }
    } while (open_next);

    return err;
}

void unzip_close(void* obj)
{
    zip_reader* zr = (zip_reader*)obj;
    if (NULL != zr) {
        if (zr->m_unz) {
            /*
             * unzCloseCurrentFile() will be called in close action.
             * only when zr->m_unz is null, unzClose() will return error number.
             * and this doesn't hold, so we ignore its returned result.
             */
            (void)unzClose(zr->m_unz);
        }
        pfree(zr);
    }
}
