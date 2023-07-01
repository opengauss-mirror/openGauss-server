/* -------------------------------------------------------------------------
 *
 * pg_backup_archiver.h
 *
 *	Private interface to the pg_dump archiver routines.
 *	It is NOT intended that these routines be called by any
 *	dumper directly.
 *
 *	See the headers to pg_restore for more details.
 *
 * Copyright (c) 2000, Philip Warner
 *		Rights are granted to use this software in any way so long
 *		as this notice is not removed.
 *
 *	The author is not responsible for loss or damages that may
 *	result from it's use.
 *
 *
 * IDENTIFICATION
 *		src/bin/pg_dump/pg_backup_archiver.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __PG_BACKUP_ARCHIVE__
#define __PG_BACKUP_ARCHIVE__

#include "postgres_fe.h"

#include <time.h>

#include "pg_backup.h"

#include "libpq/libpq-fe.h"
#include "libpq/pqexpbuffer.h"

/* Database Security: Data importing/dumping support AES128. */
#include "utils/aes.h"

#define LOBBUFSIZE 16384

typedef unsigned char GS_UCHAR;
#if defined(__LP64__) || defined(__64BIT__)
typedef unsigned int GS_UINT32;
#else
typedef unsigned long GS_UINT32;
#endif

/*
 * Note: zlib.h must be included *after* libpq-fe.h, because the latter may
 * include ssl.h, which has a naming conflict with zlib.h.
 */
#ifdef HAVE_LIBZ
#include "zlib.h"
#define GZCLOSE(fh) gzclose(fh)
#define GZWRITE(p, s, n, fh) gzwrite(fh, p, (n) * (s))
#define GZREAD(p, s, n, fh) gzread(fh, p, (n) * (s))
#else
#define GZCLOSE(fh) fclose(fh)
#define GZWRITE(p, s, n, fh) (fwrite(p, s, n, fh) * (s))
#define GZREAD(p, s, n, fh) fread(p, s, n, fh)
/* this is just the redefinition of a libz constant */
#define Z_DEFAULT_COMPRESSION (-1)

typedef struct _z_stream {
    void* next_in;
    void* next_out;
    size_t avail_in;
    size_t avail_out;
} z_stream;
typedef z_stream* z_streamp;
#endif

/* Current archive version number (the format we can output) */
#define K_VERS_MAJOR 1
#define K_VERS_MINOR 12
#define K_VERS_REV 0

/* Data block types */
#define BLK_DATA 1
#define BLK_BLOBS 3

/* Historical version numbers (checked in code) */
#define K_VERS_1_0 (((1 * 256 + 0) * 256 + 0) * 256 + 0)
#define K_VERS_1_2 (((1 * 256 + 2) * 256 + 0) * 256 + 0) /* Allow No ZLIB */
#define K_VERS_1_3 (((1 * 256 + 3) * 256 + 0) * 256 + 0) /* BLOBs */
#define K_VERS_1_4 (((1 * 256 + 4) * 256 + 0) * 256 + 0) /* Date & name in header */
#define K_VERS_1_5 (((1 * 256 + 5) * 256 + 0) * 256 + 0) /* Handle dependencies */
#define K_VERS_1_6 (((1 * 256 + 6) * 256 + 0) * 256 + 0) /* Schema field in TOCs */
#define K_VERS_1_7                                               \
    (((1 * 256 + 7) * 256 + 0) * 256 + 0) /* File Offset size in \
                                           * header */
#define K_VERS_1_8                                                 \
    (((1 * 256 + 8) * 256 + 0) * 256 + 0) /* change interpretation \
                                           * of ID numbers and     \
                                           * dependencies */
#define K_VERS_1_9                                                                  \
    (((1 * 256 + 9) * 256 + 0) * 256 + 0)                  /* add default_with_oids \
                                                            * tracking */
#define K_VERS_1_10 (((1 * 256 + 10) * 256 + 0) * 256 + 0) /* add tablespace */
#define K_VERS_1_11                                           \
    (((1 * 256 + 11) * 256 + 0) * 256 + 0) /* add toc section \
                                            * indicator */
#define K_VERS_1_12                                             \
    (((1 * 256 + 12) * 256 + 0) * 256 + 0) /* add separate BLOB \
                                            * entries */

/* Newest format we can read */
#define K_VERS_MAX (((1 * 256 + 12) * 256 + 255) * 256 + 0)

/* Flags to indicate disposition of offsets stored in files */
#define K_OFFSET_POS_NOT_SET 1
#define K_OFFSET_POS_SET 2
#define K_OFFSET_NO_DATA 3

struct _archiveHandle;
struct _tocEntry;

typedef void (*ClosePtr)(struct _archiveHandle* AH);
typedef void (*ReopenPtr)(struct _archiveHandle* AH);
typedef void (*ArchiveEntryPtr)(struct _archiveHandle* AH, struct _tocEntry* te);

typedef void (*StartDataPtr)(struct _archiveHandle* AH, struct _tocEntry* te);
typedef size_t (*WriteDataPtr)(struct _archiveHandle* AH, const void* data, size_t dLen);
typedef void (*EndDataPtr)(struct _archiveHandle* AH, struct _tocEntry* te);

typedef void (*StartBlobsPtr)(struct _archiveHandle* AH, struct _tocEntry* te);
typedef void (*StartBlobPtr)(struct _archiveHandle* AH, struct _tocEntry* te, Oid oid);
typedef void (*EndBlobPtr)(struct _archiveHandle* AH, struct _tocEntry* te, Oid oid);
typedef void (*EndBlobsPtr)(struct _archiveHandle* AH, struct _tocEntry* te);

typedef int (*WriteBytePtr)(struct _archiveHandle* AH, const int i);
typedef int (*ReadBytePtr)(struct _archiveHandle* AH);
typedef size_t (*WriteBufPtr)(struct _archiveHandle* AH, const void* c, size_t len);
typedef size_t (*ReadBufPtr)(struct _archiveHandle* AH, void* buf, size_t len);
typedef void (*SaveArchivePtr)(struct _archiveHandle* AH);
typedef void (*WriteExtraTocPtr)(struct _archiveHandle* AH, struct _tocEntry* te);
typedef void (*ReadExtraTocPtr)(struct _archiveHandle* AH, struct _tocEntry* te);
typedef void (*PrintExtraTocPtr)(struct _archiveHandle* AH, struct _tocEntry* te);
typedef void (*PrintTocDataPtr)(struct _archiveHandle* AH, struct _tocEntry* te, RestoreOptions* ropt);

typedef void (*ClonePtr)(struct _archiveHandle* AH);
typedef void (*DeClonePtr)(struct _archiveHandle* AH);

typedef size_t (*CustomOutPtr)(struct _archiveHandle* AH, const void* buf, size_t len);

typedef enum {
    SQL_SCAN = 0,        /* normal */
    SQL_IN_SINGLE_QUOTE, /* '...' literal */
    SQL_IN_DOUBLE_QUOTE  /* "..." identifier */
} sqlparseState;

typedef struct {
    sqlparseState state; /* see above */
    bool backSlash;      /* next char is backslash quoted? */
    PQExpBuffer curCmd;  /* incomplete line (NULL if not created) */
} sqlparseInfo;

typedef enum { STAGE_NONE = 0, STAGE_INITIALIZING, STAGE_PROCESSING, STAGE_FINALIZING } ArchiverStage;

typedef enum {
    OUTPUT_SQLCMDS = 0, /* emitting general SQL commands */
    OUTPUT_COPYDATA,    /* writing COPY data */
    OUTPUT_OTHERDATA    /* writing data as INSERT commands */
} ArchiverOutput;

typedef enum {
    REQ_SCHEMA = 0x01, /* want schema */
    REQ_DATA = 0x02,   /* want data */
    REQ_SPECIAL = 0x04 /* for special TOC entries */
} teReqs;

typedef struct _archiveHandle {
    Archive publicArc; /* Public part of archive */
    char vmaj;         /* Version of file */
    char vmin;
    char vrev;
    int version; /* Conveniently formatted version */

    char* archiveRemoteVersion; /* When reading an archive, the
                                 * version of the dumped DB */
    char* archiveDumpVersion;   /* When reading an archive, the
                                 * version of the dumper */

    int debugLevel;       /* Used for logging (currently only by
                           * --verbose) */
    size_t intSize;       /* Size of an integer in the archive */
    size_t offSize;       /* Size of a file offset in the archive -
                           * Added V1.7 */
    ArchiveFormat format; /* Archive format */

    sqlparseInfo sqlparse; /* state for parsing INSERT data */

    time_t createDate; /* Date archive created */

    /*
     * Fields used when discovering header. A format can always get the
     * previous read bytes from here...
     */
    int readHeader;       /* Used if file header has been read already */
    char* lookahead;      /* Buffer used when reading header to discover
                           * format */
    size_t lookaheadSize; /* Size of allocated buffer */
    size_t lookaheadLen;  /* Length of data in lookahead */
    pgoff_t lookaheadPos; /* Current read position in lookahead buffer */

    ArchiveEntryPtr ArchiveEntryptr;   /* Called for each metadata object */
    StartDataPtr StartDataptr;         /* Called when table data is about to be
                                        * dumped */
    WriteDataPtr WriteDataptr;         /* Called to send some table data to the
                                        * archive */
    EndDataPtr EndDataptr;             /* Called when table data dump is finished */
    WriteBytePtr WriteByteptr;         /* Write a byte to output */
    ReadBytePtr ReadByteptr;           /* Read a byte from an archive */
    WriteBufPtr WriteBufptr;           /* Write a buffer of output to the archive */
    ReadBufPtr ReadBufptr;             /* Read a buffer of input from the archive */
    ClosePtr Closeptr;                 /* Close the archive */
    ReopenPtr Reopenptr;               /* Reopen the archive */
    WriteExtraTocPtr WriteExtraTocptr; /* Write extra TOC entry data
                                        * associated with the current archive
                                        * format */
    ReadExtraTocPtr ReadExtraTocptr;   /* Read extr info associated with
                                        * archie format */
    PrintExtraTocPtr PrintExtraTocptr; /* Extra TOC info for format */
    PrintTocDataPtr PrintTocDataptr;

    StartBlobsPtr StartBlobsptr;
    EndBlobsPtr EndBlobsptr;
    StartBlobPtr StartBlobptr;
    EndBlobPtr EndBlobptr;

    ClonePtr Cloneptr;     /* Clone format-specific fields */
    DeClonePtr DeCloneptr; /* Clean up cloned fields */

    CustomOutPtr CustomOutptr; /* Alternative script output routine */

    /* Stuff for direct DB connection */
    char* archdbname; /* DB name *read* from archive */
    enum trivalue promptPassword;
    char* savedPassword; /* password for ropt->username, if known */
    PGconn* connection;
    int connectToDB;           /* Flag to indicate if direct DB connection is
                                * required */
    ArchiverOutput outputKind; /* Flag for what we're currently writing */
    bool pgCopyIn;             /* Currently in libpq 'COPY IN' mode. */

    int loFd;        /* BLOB fd */
    int writingBlob; /* Flag */
    int blobCount;   /* # of blobs restored */

    char* fSpec; /* Archive File Spec */
    FILE* FH;    /* General purpose file handle */
    void* OF;
    int gzOut; /* Output file */

    struct _tocEntry* toc; /* Header of circular list of TOC entries */
    int tocCount;          /* Number of TOC entries */
    DumpId maxDumpId;      /* largest DumpId among all TOC entries */

    /* arrays created after the TOC list is complete: */
    struct _tocEntry** tocsByDumpId; /* TOCs indexed by dumpId */
    DumpId* tableDataId;             /* TABLE DATA ids, indexed by table dumpId */

    struct _tocEntry* currToc; /* Used when dumping data */
    int compression;           /* Compression requested on open Possible
                                * values for compression: -1
                                * Z_DEFAULT_COMPRESSION 0	COMPRESSION_NONE
                                * 1-9 levels for gzip compression */
    ArchiveMode mode;          /* File mode - r or w */
    void* formatData;          /* Header data specific to file format */

    RestoreOptions* ropt; /* Used to check restore options in ahwrite
                           * etc */

    /* these vars track state to avoid sending redundant SET commands */
    char* currUser;       /* current username, or NULL if unknown */
    char* currSchema;     /* current schema, or NULL */
    char* currTablespace; /* current tablespace, or NULL */
    bool currWithOids;    /* current default_with_oids setting */

    void* lo_buf;
    size_t lo_buf_used;
    size_t lo_buf_size;

    int noTocComments;
    ArchiverStage stage;
    ArchiverStage lastErrorStage;
    struct _tocEntry* currentTE;
    struct _tocEntry* lastErrorTE;
} ArchiveHandle;

typedef struct _tocEntry {
    struct _tocEntry* prev;
    struct _tocEntry* next;
    CatalogId catalogId;
    DumpId dumpId;
    teSection section;
    bool hadDumper;   /* Archiver was passed a dumper routine (used
                       * in restore) */
    char* tag;        /* index tag */
    char* nmspace;    /* null or empty string if not in a schema */
    char* tablespace; /* null if not in a tablespace; empty string
                       * means use database default */
    char* owner;
    bool withOids; /* Used only by "TABLE" tags */
    char* desc;
    char* defn;
    char* dropStmt;
    char* copyStmt;
    DumpId* dependencies; /* dumpIds of objects this one depends on */
    int nDeps;            /* number of dependencies */

    DataDumperPtr dataDumper; /* Routine to dump data for object */
    void* dataDumperArg;      /* Arg for above routine */
    void* formatData;         /* TOC Entry data specific to file format */

    /* working state while dumping/restoring */
    teReqs reqs;  /* do we need schema and/or data of object */
    bool created; /* set for DATA member if TABLE was created */

    /* working state (needed only for parallel restore) */
    struct _tocEntry* par_prev; /* list links for pending/ready items; */
    struct _tocEntry* par_next; /* these are NULL if not in either list */
    int depCount;               /* number of dependencies not yet restored */
    DumpId* revDeps;            /* dumpIds of objects depending on this one */
    int nRevDeps;               /* number of such dependencies */
    DumpId* lockDeps;           /* dumpIds of objects this one needs lock on */
    int nLockDeps;              /* number of such dependencies */
} TocEntry;

extern void on_exit_close_archive(Archive* AHX);

extern void warn_or_exit_horribly(ArchiveHandle* AH, const char* modulename, const char* fmt, ...)
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));

extern void WriteTOC(ArchiveHandle* AH);
extern void ReadTOC(ArchiveHandle* AH);
extern void WriteHead(ArchiveHandle* AH);
extern void ReadHead(ArchiveHandle* AH);
extern void WriteToc(ArchiveHandle* AH);
extern void ReadToc(ArchiveHandle* AH);
extern void WriteDataChunks(ArchiveHandle* AH);

extern teReqs TocIDRequired(ArchiveHandle* AH, DumpId id);
extern bool checkSeek(FILE* fp);

#define appendStringLiteralAHX(buf, str, AH) \
    appendStringLiteral(buf, str, (AH)->publicArc.encoding, (AH)->publicArc.std_strings)

#define appendByteaLiteralAHX(buf, str, len, AH) appendByteaLiteral(buf, str, len, (AH)->publicArc.std_strings)

/*
 * Mandatory routines for each supported format
 */

extern size_t WriteInt(ArchiveHandle* AH, int i);
extern int ReadInt(ArchiveHandle* AH);
extern char* ReadStr(ArchiveHandle* AH);
extern size_t WriteStr(ArchiveHandle* AH, const char* s);

int ReadOffset(ArchiveHandle*, pgoff_t*);
size_t WriteOffset(ArchiveHandle*, pgoff_t, int);

extern void StartRestoreBlobs(ArchiveHandle* AH);
extern void StartRestoreBlob(ArchiveHandle* AH, Oid oid, bool drop);
extern void EndRestoreBlob(ArchiveHandle* AH, Oid oid);
extern void EndRestoreBlobs(ArchiveHandle* AH);

extern void InitArchiveFmt_Custom(ArchiveHandle* AH);
extern void InitArchiveFmt_Null(ArchiveHandle* AH);
extern void InitArchiveFmt_Directory(ArchiveHandle* AH);
extern void InitArchiveFmt_Tar(ArchiveHandle* AH);

extern bool isValidTarHeader(const char* header);

extern int ReconnectToServer(ArchiveHandle* AH, const char* dbname, const char* newUser);
extern void DropBlobIfExists(ArchiveHandle* AH, Oid oid);

int ahwrite(const void* ptr, size_t size, size_t nmemb, ArchiveHandle* AH);
int ahprintf(ArchiveHandle* AH, const char* fmt, ...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

void ahlog(ArchiveHandle* AH, int level, const char* fmt, ...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 3, 4)));

/* Database Security: Data importing/dumping support AES128. */
extern void check_encrypt_parameters(Archive* fout, const char* encrypt_mode, const char* encrypt_key);
extern void decryptArchive(Archive* fout, const ArchiveFormat fmt);
extern void decryptSimpleFile(const char* fileName, const char* decryptedFileName, unsigned char Key[]);

extern void encryptArchive(Archive* fout, const ArchiveFormat fmt);
extern void encryptSimpleFile(
    const char* fileName, const char* encryptedFileName, unsigned char Key[], unsigned char* init_rand);

extern bool getAESLabelFile(const char* dirName, const char* labelName, const char* fMode);
extern bool checkAndCreateDir(const char* dirName);
extern bool CheckIfStandby(struct Archive *fout);
extern size_t fread_file(void *buf, size_t size, size_t nmemb, FILE *fh);
extern bool findDBCompatibility(Archive* fout, const char* databasename);
extern bool hasSpecificExtension(Archive* fout, const char* databasename);

#ifdef HAVE_LIBZ
extern size_t gzread_file(void *buf, unsigned len, gzFile fp);
#endif

#endif

