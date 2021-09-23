/* -------------------------------------------------------------------------
 *
 * pg_backup_directory.c
 *
 *	A directory format dump is a directory, which contains a "toc.dat" file
 *	for the TOC, and a separate file for each data entry, named "<oid>.dat".
 *	Large objects (BLOBs) are stored in separate files named "blob_<uid>.dat",
 *	and there's a plain-text TOC file for them called "blobs.toc". If
 *	compression is used, each data file is individually compressed and the
 *	".gz" suffix is added to the filenames. The TOC files are never
 *	compressed by pg_dump, however they are accepted with the .gz suffix too,
 *	in case the user has manually compressed them with 'gzip'.
 *
 *	NOTE: This format is identical to the files written in the tar file in
 *	the 'tar' format, except that we don't write the restore.sql file ,
 *	and the tar format doesn't support compression. Please keep the formats in
 *	sync.
 *
 *
 *	Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *	Portions Copyright (c) 1994, Regents of the University of California
 *	Portions Copyright (c) 2000, Philip Warner
 *
 *	Rights are granted to use this software in any way so long
 *	as this notice is not removed.
 *
 *	The author is not responsible for loss or damages that may
 *	result from it's use.
 *
 * IDENTIFICATION
 *		src/bin/pg_dump/pg_backup_directory.c
 *
 * -------------------------------------------------------------------------
 */

#include "compress_io.h"
#include "dumpmem.h"

#ifdef GAUSS_SFT_TEST
#include "gauss_sft.h"
#endif

typedef struct {
    /*
     * Our archive location. This is basically what the user specified as his
     * backup file but of course here it is a directory.
     */
    char* directory;

    cfp* dataFH; /* currently open data file */

    cfp* blobsTocFH; /* file handle for blobs.toc */
} lclContext;

typedef struct {
    char* filename; /* filename excluding the directory (basename) */
} lclTocEntry;

/* translator: this is a module name */
static const char* modulename = gettext_noop("directory archiver");

/* prototypes for private functions */
static void _ArchiveEntry(ArchiveHandle* AH, TocEntry* te);
static void _StartData(ArchiveHandle* AH, TocEntry* te);
static void _EndData(ArchiveHandle* AH, TocEntry* te);
static size_t _WriteData(ArchiveHandle* AH, const void* data, size_t dLen);
static int _WriteByte(ArchiveHandle* AH, const int i);
static int _ReadByte(ArchiveHandle*);
static size_t _WriteBuf(ArchiveHandle* AH, const void* buf, size_t len);
static size_t _ReadBuf(ArchiveHandle* AH, void* buf, size_t len);
static void _CloseArchive(ArchiveHandle* AH);
static void _PrintTocData(ArchiveHandle* AH, TocEntry* te, RestoreOptions* ropt);

static void _WriteExtraToc(ArchiveHandle* AH, TocEntry* te);
static void _ReadExtraToc(ArchiveHandle* AH, TocEntry* te);
static void _PrintExtraToc(ArchiveHandle* AH, TocEntry* te);

static void _StartBlobs(ArchiveHandle* AH, TocEntry* te);
static void _StartBlob(ArchiveHandle* AH, TocEntry* te, Oid oid);
static void _EndBlob(ArchiveHandle* AH, TocEntry* te, Oid oid);
static void _EndBlobs(ArchiveHandle* AH, TocEntry* te);
static void _LoadBlobs(ArchiveHandle* AH, RestoreOptions* ropt);

static char* prependDirectory(ArchiveHandle* AH, const char* relativeFilename);

/*
 *	Init routine required by ALL formats. This is a global routine
 *	and should be declared in pg_backup_archiver.h
 *
 *	Its task is to create any extra archive context (using AH->formatData),
 *	and to initialize the supported function pointers.
 *
 *	It should also prepare whatever its input source is for reading/writing,
 *	and in the case of a read mode connection, it should load the Header & TOC.
 */
void InitArchiveFmt_Directory(ArchiveHandle* AH)
{
    lclContext* ctx = NULL;

    /* Assuming static functions, this can be copied for each format. */
    AH->ArchiveEntryptr = _ArchiveEntry;
    AH->StartDataptr = _StartData;
    AH->WriteDataptr = _WriteData;
    AH->EndDataptr = _EndData;
    AH->WriteByteptr = _WriteByte;
    AH->ReadByteptr = _ReadByte;
    AH->WriteBufptr = _WriteBuf;
    AH->ReadBufptr = _ReadBuf;
    AH->Closeptr = _CloseArchive;
    AH->Reopenptr = NULL;
    AH->PrintTocDataptr = _PrintTocData;
    AH->ReadExtraTocptr = _ReadExtraToc;
    AH->WriteExtraTocptr = _WriteExtraToc;
    AH->PrintExtraTocptr = _PrintExtraToc;

    AH->StartBlobsptr = _StartBlobs;
    AH->StartBlobptr = _StartBlob;
    AH->EndBlobptr = _EndBlob;
    AH->EndBlobsptr = _EndBlobs;

    AH->Cloneptr = NULL;
    AH->DeCloneptr = NULL;

    /* Set up our private context */
    ctx = (lclContext*)pg_calloc(1, sizeof(lclContext));
    AH->formatData = (void*)ctx;

    ctx->dataFH = NULL;
    ctx->blobsTocFH = NULL;

    /* Initialize LO buffering */
    AH->lo_buf_size = LOBBUFSIZE;
    AH->lo_buf = (void*)pg_malloc(LOBBUFSIZE);

    /*
     * Now open the TOC file
     */

    if ((AH->fSpec == NULL) || strcmp(AH->fSpec, "") == 0)
        exit_horribly(modulename, "no output directory specified\n");

    ctx->directory = AH->fSpec;

    if (AH->mode == archModeWrite) {
        if (mkdir(ctx->directory, 0700) < 0)
            exit_horribly(modulename, "could not create directory \"%s\": %s\n", ctx->directory, strerror(errno));
    } else { /* Read Mode */
        char* fname = NULL;
        cfp* tocFH = NULL;

        fname = prependDirectory(AH, "toc.dat");

        tocFH = cfopen_read(fname, PG_BINARY_R);
        if (tocFH == NULL)
            exit_horribly(modulename, "could not open input file \"%s\": %s\n", fname, strerror(errno));

        ctx->dataFH = tocFH;

        /*
         * The TOC of a directory format dump shares the format code of the
         * tar format.
         */
        AH->format = archTar;
        ReadHead(AH);
        AH->format = archDirectory;
        ReadToc(AH);

        /* Nothing else in the file, so close it again... */
        if (cfclose(tocFH) != 0)
            exit_horribly(modulename, "could not close TOC file: %s\n", strerror(errno));
        ctx->dataFH = NULL;
    }
}

/*
 * Called by the Archiver when the dumper creates a new TOC entry.
 *
 * We determine the filename for this entry.
 */
static void _ArchiveEntry(ArchiveHandle* AH, TocEntry* te)
{
    lclTocEntry* tctx = NULL;
    char fn[MAXPGPATH] = {0};
    int nRet = 0;

    tctx = (lclTocEntry*)pg_calloc(1, sizeof(lclTocEntry));
    if ((te->dataDumper) != NULL) {
        nRet = sprintf_s(fn, MAXPGPATH, "%d.dat", te->dumpId);
        securec_check_ss_c(nRet, "\0", "\0");
        tctx->filename = gs_strdup(fn);
    } else if (strcmp(te->desc, "BLOBS") == 0)
        tctx->filename = gs_strdup("blobs.toc");
    else
        tctx->filename = NULL;

    te->formatData = (void*)tctx;
}

#ifdef ENABLE_UT
void uttest_dir_ArchiveEntry(ArchiveHandle* AH, TocEntry* te)
{
    _ArchiveEntry(AH, te);
}
#endif

/*
 * Called by the Archiver to save any extra format-related TOC entry
 * data.
 *
 * Use the Archiver routines to write data - they are non-endian, and
 * maintain other important file information.
 */
static void _WriteExtraToc(ArchiveHandle* AH, TocEntry* te)
{
    lclTocEntry* tctx = (lclTocEntry*)te->formatData;
#ifdef HAVE_LIBZ
    char* fname = NULL;
    size_t flen = 0;
    int nRet = 0;
#endif

    /*
     * A dumpable object has set tctx->filename, any other object has not.
     * (see _ArchiveEntry).
     */
    if ((tctx->filename) != NULL) {
#ifdef HAVE_LIBZ

        if (AH->compression != 0) {
            flen = strlen(tctx->filename) + strlen(".gz") + 1;
            fname = (char*)pg_malloc(flen);
            nRet = sprintf_s(fname, flen, "%s.gz", tctx->filename);
            securec_check_ss_c(nRet, fname, "\0");
            WriteStr(AH, fname);
            free(fname);
            fname = NULL;
        } else {
            WriteStr(AH, tctx->filename);
        }

#else
        (void)WriteStr(AH, tctx->filename);
#endif
    } else
        (void)WriteStr(AH, "");
}

/*
 * Called by the Archiver to read any extra format-related TOC data.
 *
 * Needs to match the order defined in _WriteExtraToc, and should also
 * use the Archiver input routines.
 */
static void _ReadExtraToc(ArchiveHandle* AH, TocEntry* te)
{
    lclTocEntry* tctx = (lclTocEntry*)te->formatData;

    if (tctx == NULL) {
        tctx = (lclTocEntry*)pg_calloc(1, sizeof(lclTocEntry));
        te->formatData = (void*)tctx;
    }

    tctx->filename = ReadStr(AH);
    if ((tctx->filename != NULL) && (strlen(tctx->filename) == 0)) {
        free(tctx->filename);
        tctx->filename = NULL;
    }
}

/*
 * Called by the Archiver when restoring an archive to output a comment
 * that includes useful information about the TOC entry.
 */
static void _PrintExtraToc(ArchiveHandle* AH, TocEntry* te)
{
    lclTocEntry* tctx = (lclTocEntry*)te->formatData;

    if (AH->publicArc.verbose && (tctx->filename != NULL))
        (void)ahprintf(AH, "-- File: %s\n", tctx->filename);
}

/*
 * Called by the archiver when saving TABLE DATA (not schema). This routine
 * should save whatever format-specific information is needed to read
 * the archive back.
 *
 * It is called just prior to the dumper's 'DataDumper' routine being called.
 *
 * We create the data file for writing.
 */
static void _StartData(ArchiveHandle* AH, TocEntry* te)
{
    lclTocEntry* tctx = (lclTocEntry*)te->formatData;
    lclContext* ctx = (lclContext*)AH->formatData;
    char* fname = NULL;

    fname = prependDirectory(AH, tctx->filename);

    ctx->dataFH = cfopen_write(fname, PG_BINARY_W, AH->compression);
    if (ctx->dataFH == NULL)
        exit_horribly(modulename, "could not open output file \"%s\": %s\n", fname, strerror(errno));
}

/*
 * Called by archiver when dumper calls WriteData. This routine is
 * called for both BLOB and TABLE data; it is the responsibility of
 * the format to manage each kind of data using StartBlob/StartData.
 *
 * It should only be called from within a DataDumper routine.
 *
 * We write the data to the open data file.
 */
static size_t _WriteData(ArchiveHandle* AH, const void* data, size_t dLen)
{
    lclContext* ctx = (lclContext*)AH->formatData;

    if (dLen == 0)
        return 0;

    return (size_t)cfwrite(data, (int)dLen, ctx->dataFH);
}

/*
 * Called by the archiver when a dumper's 'DataDumper' routine has
 * finished.
 *
 * We close the data file.
 */
static void _EndData(ArchiveHandle* AH, TocEntry* te)
{
    lclContext* ctx = (lclContext*)AH->formatData;

    /* Close the file */
    (void)cfclose(ctx->dataFH);

    ctx->dataFH = NULL;
}

/*
 * Print data for a given file (can be a BLOB as well)
 */
static void _PrintFileData(ArchiveHandle* AH, const char* filename, RestoreOptions* ropt)
{
    size_t cnt = 0;
    char* buf = NULL;
    size_t buflen = 0;
    cfp* pstCfp = NULL;

    if (filename == NULL)
        return;

    pstCfp = cfopen_read(filename, PG_BINARY_R);
    if (pstCfp == NULL)
        exit_horribly(modulename, "could not open input file \"%s\": %s\n", filename, strerror(errno));

    buf = (char*)pg_malloc(ZLIB_OUT_SIZE);
    buflen = ZLIB_OUT_SIZE;

    while ((cnt = cfread(buf, buflen, pstCfp)))
        (void)ahwrite(buf, 1, cnt, AH);

    free(buf);
    buf = NULL;
    if (cfclose(pstCfp) != 0)
        exit_horribly(modulename, "could not close data file: %s\n", strerror(errno));
}

/*
 * Print data for a given TOC entry
 */
static void _PrintTocData(ArchiveHandle* AH, TocEntry* te, RestoreOptions* ropt)
{
    lclTocEntry* tctx = (lclTocEntry*)te->formatData;

    if ((tctx->filename) == NULL)
        return;

    if (strcmp(te->desc, "BLOBS") == 0)
        _LoadBlobs(AH, ropt);
    else {
        char* fname = prependDirectory(AH, tctx->filename);

        _PrintFileData(AH, fname, ropt);
    }
}

static void _LoadBlobs(ArchiveHandle* AH, RestoreOptions* ropt)
{
    Oid oid;
    lclContext* ctx = (lclContext*)AH->formatData;
    char* fname = NULL;
    char line[MAXPGPATH] = {0};
    int nRet = 0;

    StartRestoreBlobs(AH);

    fname = prependDirectory(AH, "blobs.toc");

    ctx->blobsTocFH = cfopen_read(fname, PG_BINARY_R);

    if (ctx->blobsTocFH == NULL)
        exit_horribly(
            modulename, "could not open large object TOC file \"%s\" for input: %s\n", fname, strerror(errno));

    /* Read the blobs TOC file line-by-line, and process each blob */
    while ((cfgets(ctx->blobsTocFH, line, MAXPGPATH)) != NULL) {
        char path[MAXPGPATH] = {0};

        if (sscanf_s(line, "%u %s\n", &oid, fname, MAXPGPATH) != 2)
            exit_horribly(modulename, "invalid line in large object TOC file \"%s\": \"%s\"\n", fname, line);

        StartRestoreBlob(AH, oid, (ropt->dropSchema ? true : false));
        nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", ctx->directory, fname);
        securec_check_ss_c(nRet, "\0", "\0");
        _PrintFileData(AH, path, ropt);
        EndRestoreBlob(AH, oid);
    }
    if (!cfeof(ctx->blobsTocFH))
        exit_horribly(modulename, "error reading large object TOC file \"%s\"\n", fname);

    if (cfclose(ctx->blobsTocFH) != 0)
        exit_horribly(modulename, "could not close large object TOC file \"%s\": %s\n", fname, strerror(errno));

    ctx->blobsTocFH = NULL;

    EndRestoreBlobs(AH);
}

/*
 * Write a byte of data to the archive.
 * Called by the archiver to do integer & byte output to the archive.
 * These routines are only used to read & write the headers & TOC.
 */
static int _WriteByte(ArchiveHandle* AH, const int i)
{
    unsigned char c = (unsigned char)i;
    lclContext* ctx = (lclContext*)AH->formatData;

    if (cfwrite(&c, 1, ctx->dataFH) != 1)
        exit_horribly(modulename, "could not write byte\n");

    return 1;
}

/*
 * Read a byte of data from the archive.
 * Called by the archiver to read bytes & integers from the archive.
 * These routines are only used to read & write headers & TOC.
 * EOF should be treated as a fatal error.
 */
static int _ReadByte(ArchiveHandle* AH)
{
    lclContext* ctx = (lclContext*)AH->formatData;
    int res;

    res = cfgetc(ctx->dataFH);
    if (res == EOF)
        exit_horribly(modulename, "unexpected end of file\n");

    return res;
}

/*
 * Write a buffer of data to the archive.
 * Called by the archiver to write a block of bytes to the TOC or a data file.
 */
static size_t _WriteBuf(ArchiveHandle* AH, const void* buf, size_t len)
{
    lclContext* ctx = (lclContext*)AH->formatData;
    size_t res;

    res = cfwrite(buf, len, ctx->dataFH);
    if (res != len)
        exit_horribly(modulename, "could not write to output file: %s\n", strerror(errno));

    return res;
}

/*
 * Read a block of bytes from the archive.
 *
 * Called by the archiver to read a block of bytes from the archive
 */
static size_t _ReadBuf(ArchiveHandle* AH, void* buf, size_t len)
{
    lclContext* ctx = (lclContext*)AH->formatData;
    size_t res;

    res = cfread(buf, len, ctx->dataFH);

    return res;
}

/*
 * Close the archive.
 *
 * When writing the archive, this is the routine that actually starts
 * the process of saving it to files. No data should be written prior
 * to this point, since the user could sort the TOC after creating it.
 *
 * If an archive is to be written, this routine must call:
 *		WriteHead			to save the archive header
 *		WriteToc			to save the TOC entries
 *		WriteDataChunks		to save all DATA & BLOBs.
 */
static void _CloseArchive(ArchiveHandle* AH)
{
    lclContext* ctx = (lclContext*)AH->formatData;

    if (AH->mode == archModeWrite) {
        cfp* tocFH = NULL;
        char* fname = prependDirectory(AH, "toc.dat");

        /* The TOC is always created uncompressed */
        tocFH = cfopen_write(fname, PG_BINARY_W, 0);
        if (tocFH == NULL)
            exit_horribly(modulename, "could not open output file \"%s\": %s\n", fname, strerror(errno));
        ctx->dataFH = tocFH;

        /*
         * Write 'tar' in the format field of the toc.dat file. The directory
         * is compatible with 'tar', so there's no point having a different
         * format code for it.
         */
        AH->format = archTar;
        WriteHead(AH);
        AH->format = archDirectory;
        WriteToc(AH);
        if (cfclose(tocFH) != 0)
            exit_horribly(modulename, "could not close TOC file: %s\n", strerror(errno));
        WriteDataChunks(AH);
    }
    AH->FH = NULL;
}

/*
 * BLOB support
 */

/*
 * Called by the archiver when starting to save all BLOB DATA (not schema).
 * It is called just prior to the dumper's DataDumper routine.
 *
 * We open the large object TOC file here, so that we can append a line to
 * it for each blob.
 */
static void _StartBlobs(ArchiveHandle* AH, TocEntry* te)
{
    lclContext* ctx = (lclContext*)AH->formatData;
    char* fname = NULL;

    fname = prependDirectory(AH, "blobs.toc");

    /* The blob TOC file is never compressed */
    ctx->blobsTocFH = cfopen_write(fname, "ab", 0);
    if (ctx->blobsTocFH == NULL)
        exit_horribly(modulename, "could not open output file \"%s\": %s\n", fname, strerror(errno));
}

/*
 * Called by the archiver when we're about to start dumping a blob.
 *
 * We create a file to write the blob to.
 */
static void _StartBlob(ArchiveHandle* AH, TocEntry* te, Oid oid)
{
    lclContext* ctx = (lclContext*)AH->formatData;
    char fname[MAXPGPATH] = {0};
    int nRet = 0;

    nRet = sprintf_s(fname, MAXPGPATH, "%s/blob_%u.dat", ctx->directory, oid);
    securec_check_ss_c(nRet, "\0", "\0");

    ctx->dataFH = cfopen_write(fname, PG_BINARY_W, AH->compression);

    if (ctx->dataFH == NULL)
        exit_horribly(modulename, "could not open output file \"%s\": %s\n", fname, strerror(errno));
}

/*
 * Called by the archiver when the dumper is finished writing a blob.
 *
 * We close the blob file and write an entry to the blob TOC file for it.
 */
static void _EndBlob(ArchiveHandle* AH, TocEntry* te, Oid oid)
{
    lclContext* ctx = (lclContext*)AH->formatData;
    char buf[50] = {0};
    int len;

    /* Close the BLOB data file itself */
    (void)cfclose(ctx->dataFH);
    ctx->dataFH = NULL;

    /* register the blob in blobs.toc */
    len = snprintf_s(buf, sizeof(buf) / sizeof(char), sizeof(buf) / sizeof(char) - 1, "%u blob_%u.dat\n", oid, oid);
    securec_check_ss_c(len, "\0", "\0");
    if (cfwrite(buf, len, ctx->blobsTocFH) != len)
        exit_horribly(modulename, "could not write to blobs TOC file\n");
}

/*
 * Called by the archiver when finishing saving all BLOB DATA.
 *
 * We close the blobs TOC file.
 */
static void _EndBlobs(ArchiveHandle* AH, TocEntry* te)
{
    lclContext* ctx = (lclContext*)AH->formatData;

    (void)cfclose(ctx->blobsTocFH);
    ctx->blobsTocFH = NULL;
}

static char* prependDirectory(ArchiveHandle* AH, const char* relativeFilename)
{
    lclContext* ctx = (lclContext*)AH->formatData;
    static char buf[MAXPGPATH] = {0};
    char* dname = NULL;
    errno_t rc = 0;

    dname = ctx->directory;

    if (strlen(dname) + 1 + strlen(relativeFilename) + 1 > MAXPGPATH)
        exit_horribly(modulename, "file name too long: \"%s\"\n", dname);

    rc = memset_s(buf, MAXPGPATH, 0, MAXPGPATH);
    securec_check_c(rc, "\0", "\0");

    rc = strncpy_s(buf, MAXPGPATH, dname, strlen(dname));
    securec_check_c(rc, "\0", "\0");

    rc = strncat_s(buf, MAXPGPATH, "/", strlen("/"));
    securec_check_c(rc, "\0", "\0");

    rc = strncat_s(buf, MAXPGPATH, relativeFilename, strlen(relativeFilename));
    securec_check_c(rc, "\0", "\0");

    buf[MAXPGPATH - 1] = '\0';

    return buf;
}
