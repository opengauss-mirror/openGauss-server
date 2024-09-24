/* -------------------------------------------------------------------------
 *
 * pg_backup.h
 *
 *	Public interface to the pg_dump archiver routines.
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
 *		src/bin/pg_dump/pg_backup.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PG_BACKUP_H
#define PG_BACKUP_H

#include "postgres_fe.h"

#include "pg_dump.h"
#include "dumputils.h"

#include "libpq/libpq-fe.h"

/* Database Security: Data importing/dumping support AES128. */
#include "utils/aes.h"

#define atooid(x) ((Oid)strtoul((x), NULL, 10))
#define oidcmp(x, y) (((x) < (y) ? -1 : ((x) > (y)) ? 1 : 0))
#define oideq(x, y) ((x) == (y))
#define oidle(x, y) ((x) <= (y))
#define oidge(x, y) ((x) >= (y))
#define oidzero(x) ((x) == 0)

#define CRYPTO_MODULE_PARAMS_MAX_LEN 1024
#define CRYPTO_MODULE_ENC_TYPE_MAX_LEN 32

enum trivalue { TRI_DEFAULT, TRI_NO, TRI_YES };

typedef enum _archiveFormat {
    archUnknown = 0,
    archCustom = 1,
    archTar = 3,
    archNull = 4,
    archDirectory = 5
} ArchiveFormat;

typedef enum _archiveMode { archModeAppend, archModeWrite, archModeRead } ArchiveMode;

typedef enum _teSection {
    SECTION_NONE = 1, /* COMMENTs, ACLs, etc; can be anywhere */
    SECTION_PRE_DATA, /* stuff to be processed before data */
    SECTION_DATA,     /* TABLE DATA, BLOBS, BLOB COMMENTS */
    SECTION_POST_DATA /* stuff to be processed after data */
} teSection;

typedef struct {
    void *moduleSession;
    void *key_ctx;
    void *hmac_ctx;
}CryptoModuleCtx;

/*
 *	We may want to have some more user-readable data, but in the mean
 *	time this gives us some abstraction and type checking.
 */
struct Archive {
    int verbose;
    char* remoteVersionStr; /* server's version string */
    int remoteVersion;      /* same in numeric form */
    int workingVersionNum;  /* working_version_num value */

    int minRemoteVersion; /* allowable range */
    int maxRemoteVersion;

    /* info needed for string escaping */
    int encoding;     /* libpq code for client_encoding */
    bool std_strings; /* standard_conforming_strings */

    /* error handling */
    bool exit_on_error; /* whether to exit on SQL errors... */
    int n_errors;       /* number of errors (if no die) */

    /* Database Security: Data importing/dumping support AES128. */
    bool encryptfile;
    unsigned char Key[KEY_MAX_LEN];
    int keylen;

    /* Data importing/dumping support AES128 through OPENSSL */
    unsigned char rand[RANDOM_LEN + 1];

    int key_type;
    char crypto_type[CRYPTO_MODULE_ENC_TYPE_MAX_LEN];
    char crypto_module_params[CRYPTO_MODULE_PARAMS_MAX_LEN];
    CryptoModuleCtx cryptoModuleCtx;

    /* get hash bucket info. */
    bool getHashbucketInfo;
    /* The rest is private */
};

typedef int (*DataDumperPtr)(Archive* AH, void* userArg);

typedef struct _restoreOptions {
    int createDB;           /* Issue commands to create the database */
    int noOwner;            /* Don't try to match original object owner */
    int noTablespace;       /* Don't issue tablespace-related commands */
    int disable_triggers;   /* disable triggers during data-only
                             * restore */
    int use_setsessauth;    /* Use SET SESSION AUTHORIZATION commands
                             * instead of OWNER TO */
    int no_security_labels; /* Skip security label entries */
    int no_subscriptions;   /* Skip subscription entries */
    int no_publications;    /* Skip publication entries */
    char* superuser;        /* Username to use as superuser */
    char* use_role;         /* Issue SET ROLE to this */
    char* rolepassword;     /* password for use_role */
    int dropSchema;
    const char* filename;
    int dataOnly;
    int schemaOnly;
    int dumpSections;
    int verbose;
    int aclsSkip;
    int tocSummary;
    char* tocFile;
    int format;
    char* formatName;

    int selTypes;
    int selIndex;
    int selFunction;
    int selTrigger;
    int selTable;
    int selNamespace;
    SimpleStringList indexNames;
    SimpleStringList functionNames;
    SimpleStringList tableNames;
    SimpleStringList schemaNames;
    SimpleStringList triggerNames;

    int useDB;
    char* dbname;
    char* pgport;
    char* pghost;
    char* username;
    int noDataForFailedTables;
    enum trivalue promptPassword;
    int exit_on_error;
    int compression;
    int suppressDumpWarnings; /* Suppress output of WARNING entries
                               * to stderr */
    bool single_txn;
    bool targetV1;
    bool targetV5;
    int number_of_jobs;
#if defined(USE_ASSERT_CHECKING) || defined(FASTCHECK)
    bool disable_progress;
#endif
    bool* idWanted; /* array showing which dump IDs to emit */
} RestoreOptions;

typedef struct {
    char* module_params;
    char* mode;
    char* key;
    char* salt;
    bool genkey;
}CryptoModuleCheckParam;

/*
 * Main archiver interface.
 */

extern char* ConnectDatabase(Archive* AH, const char* dbname, const char* pghost, const char* pgport,
    const char* username, enum trivalue prompt_password, bool exitOnError = true);
extern void DisconnectDatabase(Archive* AHX);

#ifdef DUMPSSYSLOG
extern PGconn* get_dumplog_conn(
    const char* dbname, const char* pghost, const char* pgport, const char* username, const char* pwd);
#endif

extern PGconn* GetConnection(Archive* AHX);

/* Called to add a TOC entry */
extern void ArchiveEntry(Archive* AHX, CatalogId catalogId, DumpId dumpId, const char* tag, const char* nmspace,
    const char* tablespace, const char* owner, bool withOids, const char* desc, teSection section, const char* defn,
    const char* dropStmt, const char* copyStmt, const DumpId* deps, int nDeps, DataDumperPtr dumpFn, void* dumpArg);

/* Called to write *data* to the archive */
extern size_t WriteData(Archive* AH, const void* data, size_t dLen);

extern int StartBlob(Archive* AH, Oid oid);
extern int EndBlob(Archive* AH, Oid oid);

extern void CloseArchive(Archive* AH);

extern void SetArchiveRestoreOptions(Archive* AH, RestoreOptions* ropt);

extern void RestoreArchive(Archive* AH);

/* Open an existing archive */
extern Archive* OpenArchive(const char* FileSpec, const ArchiveFormat fmt, CryptoModuleCheckParam* cryptoModuleCheckParam = NULL);

/* Create a new archive */
extern Archive* CreateArchive(const char* FileSpec, const ArchiveFormat fmt, const int compression, ArchiveMode mode);

/* The --list option */
extern void PrintTOCSummary(Archive* AH, RestoreOptions* ropt);

extern RestoreOptions* NewRestoreOptions(void);

/* Rearrange and filter TOC entries */
extern void SortTocFromFile(Archive* AHX, RestoreOptions* ropt);

/* Convenience functions used only when writing DATA */
extern int archputs(const char* s, Archive* AH);
extern int archprintf(Archive* AH, const char* fmt, ...)
    /* This extension allows gcc to check the format string */
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

#define appendStringLiteralAH(buf, str, AH) appendStringLiteral(buf, str, (AH)->encoding, (AH)->std_strings)

#endif /* PG_BACKUP_H */
