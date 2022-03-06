/* -------------------------------------------------------------------------
 *
 * pg_backup_archiver.c
 *
 *	Private implementation of the archiver routines.
 *
 *	See the headers to pg_restore for more details.
 *
 * Copyright (c) 2000, Philip Warner
 *	Rights are granted to use this software in any way so long
 *	as this notice is not removed.
 *
 *	The author is not responsible for loss or damages that may
 *	result from its use.
 *
 *
 * IDENTIFICATION
 *		src/bin/pg_dump/pg_backup_archiver.c
 *
 * -------------------------------------------------------------------------
 */

#include "pg_backup_db.h"
#include "dumpmem.h"
#include "dumputils.h"

/* Database Security: Data importing/dumping support AES128. */
#include "compress_io.h"

#include <sys/wait.h>
#include "postgres.h"
#include "knl/knl_variable.h"

#ifdef WIN32
#include <windows.h>
#include <io.h>
#endif

#include "libpq/libpq-fs.h"
#ifdef GAUSS_SFT_TEST
#include "gauss_sft.h"
#endif

#ifdef WIN32
static struct tm* localtime_r(const time_t* timep, struct tm* result)
{
    (void)localtime_s(result, timep);

    return result;
}
#endif

/*
 * Special exit values from worker children.  We reserve 0 for normal
 * success; 1 and other small values should be interpreted as crashes.
 */
#define WORKER_CREATE_DONE 10
#define WORKER_INHIBIT_DATA 11
#define WORKER_IGNORED_ERRORS 12

#define MAX_AES_BUFF_LEN 1024
#define AES_TEMP_DECRYPT_LABEL "tmpaes.label"
#define AES_ENCRYPT_LABEL "aes.label"
#define TEMP_ARCHIVE_HEADER ".tmpCrypt"

/*
 * Unix uses exit to return result from worker child, so function is void.
 * Windows thread result comes via function return.
 */
#ifndef WIN32
#define parallel_restore_result void
#else
#define parallel_restore_result DWORD
#endif

/* IDs for worker children are either PIDs or thread handles */
#ifndef WIN32
#define thandle pid_t
#else
#define thandle HANDLE
#endif

typedef struct ParallelStateEntry {
#ifdef WIN32
    unsigned int threadId;
#else
    pid_t pid;
#endif
    ArchiveHandle* AH;
} ParallelStateEntry;

typedef struct ParallelState {
    int numWorkers;
    ParallelStateEntry* pse;
} ParallelState;

/* Arguments needed for a worker child */
typedef struct _restore_args {
    ArchiveHandle* AH;
    TocEntry* te;
    ParallelStateEntry* pse;
} RestoreArgs;

/* State for each parallel activity slot */
typedef struct _parallel_slot {
    thandle child_id;
    RestoreArgs* args;
} ParallelSlot;

typedef struct ShutdownInformation {
    ParallelState* pstate;
    Archive* AHX;
} ShutdownInformation;

static ShutdownInformation shutdown_info;
const char* encrypt_salt;

#define NO_SLOT (-1)
static const int RESTORE_READ_CNT = 100;

#define PATCH_LEN 50
#define TEXT_DUMP_HEADER "--\n-- openGauss database dump\n--\n\n"
#define TEXT_DUMPALL_HEADER "--\n-- openGauss database cluster dump\n--\n\n"

/* state needed to save/restore an archive's output target */
typedef struct _outputContext {
    void* OF;
    int gzOut;
} OutputContext;

/* translator: this is a module name */
static const char* modulename = gettext_noop("archiver");

static ArchiveHandle* _allocAH(const char* FileSpec, const ArchiveFormat fmt, const int compression, ArchiveMode mode);
static void _getObjectDescription(PQExpBuffer buf, TocEntry* te, ArchiveHandle* AH);
static void _printTocEntry(ArchiveHandle* AH, TocEntry* te, RestoreOptions* ropt, bool isData, bool acl_pass);
static char* replace_line_endings(const char* str);
static void _doSetFixedOutputState(ArchiveHandle* AH);
static void _doSetSessionAuth(ArchiveHandle* AH, const char* user);
static void _doResetSessionAuth(ArchiveHandle* AH);
static void _doSetWithOids(ArchiveHandle* AH, const bool withOids);
static void _reconnectToDB(ArchiveHandle* AH, const char* dbname);
static void _becomeUser(ArchiveHandle* AH, const char* user);
static void _becomeOwner(ArchiveHandle* AH, TocEntry* te);
static void _selectOutputSchema(ArchiveHandle* AH, const char* schemaName);
static void _selectTablespace(ArchiveHandle* AH, const char* tablespace);
static void processEncodingEntry(ArchiveHandle* AH, TocEntry* te);
static void processStdStringsEntry(ArchiveHandle* AH, TocEntry* te);
static teReqs _tocEntryRequired(TocEntry* te, teSection curSection, RestoreOptions* ropt);
static bool _tocEntryIsACL(TocEntry* te);
static void _disableTriggersIfNecessary(ArchiveHandle* AH, TocEntry* te, RestoreOptions* ropt);
static void _enableTriggersIfNecessary(ArchiveHandle* AH, TocEntry* te, RestoreOptions* ropt);
static void buildTocEntryArrays(ArchiveHandle* AH);
static TocEntry* getTocEntryByDumpId(ArchiveHandle* AH, DumpId id);
static void _moveBefore(ArchiveHandle* AH, TocEntry* pos, TocEntry* te);
static int _discoverArchiveFormat(ArchiveHandle* AH);

static int RestoringToDB(ArchiveHandle* AH);
static void dump_lo_buf(ArchiveHandle* AH);
static void dumpTimestamp(ArchiveHandle* AH, const char* msg, time_t tim);
static void SetOutput(ArchiveHandle* AH, const char* filename, int compression);
static OutputContext SaveOutput(ArchiveHandle* AH);
static void RestoreOutput(ArchiveHandle* AH, OutputContext savedContext);

static int restore_toc_entry(ArchiveHandle* AH, TocEntry* te, RestoreOptions* ropt, bool is_parallel);
static void restore_toc_entries_parallel(ArchiveHandle* AH);
static thandle spawn_restore(RestoreArgs* args);
static thandle reap_child(ParallelSlot* slots, int n_slots, int* work_status);
static bool work_in_progress(ParallelSlot* slots, int n_slots);
static int get_next_slot(ParallelSlot* slots, int n_slots);
static void par_list_header_init(TocEntry* l);
static void par_list_append(TocEntry* l, TocEntry* te);
static void par_list_remove(TocEntry* te);
static TocEntry* get_next_work_item(ArchiveHandle* AH, TocEntry* ready_list, ParallelSlot* slots, int n_slots);
static parallel_restore_result parallel_restore(RestoreArgs* args);
static void mark_work_done(
    ArchiveHandle* AH, TocEntry* ready_list, thandle worker, int status, ParallelSlot* slots, int n_slots);
static void fix_dependencies(ArchiveHandle* AH);
static bool has_lock_conflicts(TocEntry* te1, TocEntry* te2);
static void repoint_table_dependencies(ArchiveHandle* AH);
static void identify_locking_dependencies(ArchiveHandle* AH, TocEntry* te);
static void reduce_dependencies(ArchiveHandle* AH, TocEntry* te, TocEntry* ready_list);
static void mark_create_done(ArchiveHandle* AH, TocEntry* te);
static void inhibit_data_for_failed_table(ArchiveHandle* AH, TocEntry* te);
static ArchiveHandle* CloneArchive(ArchiveHandle* AH);
static void DeCloneArchive(ArchiveHandle* AH);

static void setProcessIdentifier(ParallelStateEntry* pse, ArchiveHandle* AH);
static void unsetProcessIdentifier(ParallelStateEntry* pse);
static ParallelStateEntry* GetMyPSEntry(ParallelState* pstate);
static void archive_close_connection(int code, void* arg);
static void take_down_nsname_in_drop_stmt(const char *stmt, char *result, int len);
static void get_role_password(RestoreOptions* opts);

/*
 *	Wrapper functions.
 *
 *	The objective it to make writing new formats and dumpers as simple
 *	as possible, if necessary at the expense of extra function calls etc.
 *
 */
/* Create a new archive */
/* Public */
Archive* CreateArchive(const char* FileSpec, const ArchiveFormat fmt, const int compression, ArchiveMode mode)

{
    ArchiveHandle* AH = _allocAH(FileSpec, fmt, compression, mode);

    return (Archive*)AH;
}

/* Open an existing archive */
/* Public */
Archive* OpenArchive(const char* FileSpec, const ArchiveFormat fmt)
{
    ArchiveHandle* AH = _allocAH(FileSpec, fmt, 0, archModeRead);

    return (Archive*)AH;
}

/* Public */
void CloseArchive(Archive* AHX)
{
    int res = 0;
    ArchiveHandle* AH = (ArchiveHandle*)AHX;

    (*AH->Closeptr)(AH);

    if (NULL != (AH->publicArc.remoteVersionStr)) {
        free(AH->publicArc.remoteVersionStr);
        AH->publicArc.remoteVersionStr = NULL;
    }
    if (NULL != (AH->archiveDumpVersion)) {
        free(AH->archiveDumpVersion);
        AH->archiveDumpVersion = NULL;
    }
    /* Close the output */
    if (AH->gzOut)
#ifdef HAVE_LIBZ
        res = GZCLOSE((gzFile)AH->OF);
#else
        res = GZCLOSE((FILE*)AH->OF);
#endif
    else if (AH->OF != stdout)
        res = fclose((FILE*)AH->OF);

    if (res != 0)
        exit_horribly(modulename, "could not close output file: %s\n", strerror(errno));
}

/* Public */
void SetArchiveRestoreOptions(Archive* AHX, RestoreOptions* ropt)
{
    ArchiveHandle* AH = (ArchiveHandle*)AHX;
    TocEntry* te = NULL;
    teSection curSection;

    /* Save options for later access */
    AH->ropt = ropt;

    /* Decide which TOC entries will be dumped/restored, and mark them */
    curSection = SECTION_PRE_DATA;
    for (te = AH->toc->next; te != AH->toc; te = te->next) {
        /*
         * When writing an archive, we also take this opportunity to check
         * that we have generated the entries in a sane order that respects
         * the section divisions.  When reading, don't complain, since buggy
         * old versions of pg_dump might generate out-of-order archives.
         */
        if (AH->mode != archModeRead) {
            switch (te->section) {
                case SECTION_NONE:
                    /* ok to be anywhere */
                    break;
                case SECTION_PRE_DATA:
                    if (curSection != SECTION_PRE_DATA)
                        write_msg(modulename, "WARNING: archive items not in correct section order\n");
                    break;
                case SECTION_DATA:
                    if (curSection == SECTION_POST_DATA)
                        write_msg(modulename, "WARNING: archive items not in correct section order\n");
                    break;
                case SECTION_POST_DATA:
                    /* ok no matter which section we were in */
                    break;
                default:
                    exit_horribly(modulename, "unexpected section code %d\n", (int)te->section);
                    break;
            }
        }

        if (te->section != SECTION_NONE) {
            curSection = te->section;
        }
        te->reqs = _tocEntryRequired(te, curSection, ropt);
    }
}

static void out_drop_stmt(ArchiveHandle* AH, const TocEntry* te)
{
    RestoreOptions* ropt = AH->ropt;

    if (ropt->targetV1 || ropt->targetV5) {
        char *result = (char *)pg_malloc(strlen(te->dropStmt) + PATCH_LEN);
        errno_t tnRet = 0;
        tnRet = memset_s(result, strlen(te->dropStmt) + PATCH_LEN, 0, strlen(te->dropStmt) + PATCH_LEN);
        securec_check_c(tnRet, "\0", "\0");
        take_down_nsname_in_drop_stmt(te->dropStmt, result, strlen(te->dropStmt) + PATCH_LEN);
        
        ahprintf(AH, "%s", result);
        free(result);
    } else {
        ahprintf(AH, "%s", te->dropStmt);
    }
}
/* Public */
void RestoreArchive(Archive* AHX)
{
    ArchiveHandle* AH = (ArchiveHandle*)AHX;
    RestoreOptions* ropt = AH->ropt;
    bool parallel_mode = false;
    TocEntry* te = NULL;
    OutputContext sav;
    char* p = NULL;
    errno_t rc = 0;
    AH->stage = STAGE_INITIALIZING;

    /*
     * Check for nonsensical option combinations.
     *
     * -C is not compatible with -1, because we can't create a database inside
     * a transaction block.
     */
    if (ropt->createDB && ropt->single_txn)
        exit_horribly(modulename, "-C and -1 are incompatible options\n");

    /*
     * If we're going to do parallel restore, there are some restrictions.
     */
    parallel_mode = (ropt->number_of_jobs > 1 && ropt->useDB);
    if (parallel_mode) {
        /* We haven't got round to making this work for all archive formats */
        if (AH->Cloneptr == NULL || AH->Reopenptr == NULL)
            exit_horribly(modulename, "parallel restore is not supported with this archive file format\n");

        /* Doesn't work if the archive represents dependencies as OIDs */
        if (AH->version < K_VERS_1_8)
            exit_horribly(modulename, "parallel restore is not supported with archives made by pre-8.0 gs_dump\n");

        /*
         * It's also not gonna work if we can't reopen the input file, so
         * let's try that immediately.
         */
        (AH->Reopenptr)(AH);
    }

    /*
     * Make sure we won't need (de)compression we haven't got
     */
#ifndef HAVE_LIBZ
    if (AH->compression != 0 && AH->PrintTocDataptr != NULL) {
        for (te = AH->toc->next; te != AH->toc; te = te->next) {
            if (te->hadDumper && (te->reqs & REQ_DATA) != 0)
                exit_horribly(modulename,
                    "cannot restore from compressed archive (compression not supported in this installation)\n");
        }
    }
#endif

    /*
     * Prepare index arrays, so we can assume we have them throughout restore.
     * It's possible we already did this, though.
     */
    if (AH->tocsByDumpId == NULL) {
        buildTocEntryArrays(AH);
    }

    /*
     * If we're using a DB connection, then connect it.
     */
    if (ropt->useDB) {
        ahlog(AH, 1, "connecting to database for restore\n");
        if (AH->version < K_VERS_1_3)
            exit_horribly(modulename, "direct database connections are not supported in pre-1.3 archives\n");

        /*
         * We don't want to guess at whether the dump will successfully
         * restore; allow the attempt regardless of the version of the restore
         * target.
         */
        AHX->minRemoteVersion = 0;
        AHX->maxRemoteVersion = 999999;

        (void)ConnectDatabase(AHX, ropt->dbname, ropt->pghost, ropt->pgport, ropt->username, ropt->promptPassword);

        if (CheckIfStandby(AHX)) {
            exit_horribly(NULL, "%s is not supported on standby or cascade standby\n", progname);
        }
        /*
         * If we're talking to the DB directly, don't send comments since they
         * obscure SQL when displaying errors
         */
        AH->noTocComments = 1;

        if ((ropt->use_role != NULL) && (ropt->rolepassword == NULL)) {
            get_role_password(ropt);
        }
    }

    /*
     * Work out if we have an implied data-only restore. This can happen if
     * the dump was data only or if the user has used a toc list to exclude
     * all of the schema data. All we do is look for schema entries - if none
     * are found then we set the dataOnly flag.
     *
     * We could scan for wanted TABLE entries, but that is not the same as
     * dataOnly. At this stage, it seems unnecessary (6-Mar-2001).
     */
    if (!ropt->dataOnly) {
        int impliedDataOnly = 1;

        for (te = AH->toc->next; te != AH->toc; te = te->next) {
            if ((te->reqs & REQ_SCHEMA) != 0) { /* It's schema, and it's wanted */
                impliedDataOnly = 0;
                break;
            }
        }
        if (impliedDataOnly) {
            ropt->dataOnly = impliedDataOnly;
            ahlog(AH, 1, "implied data-only restore\n");
        }
    }

    /*
     * Setup the output file if necessary.
     */
    sav = SaveOutput(AH);
    if ((ropt->filename != NULL) || ropt->compression)
        SetOutput(AH, ropt->filename, ropt->compression);

    /*
     * Put the rand value to encrypt file for decrypt.
     */
    if ((true == AHX->encryptfile) && (NULL == encrypt_salt)) {
        p = (char*)pg_malloc(RANDOM_LEN + 1);
        rc = memset_s(p, RANDOM_LEN + 1, 0, RANDOM_LEN + 1);
        securec_check_c(rc, "\0", "\0");
        rc = memcpy_s(p, RANDOM_LEN, AH->publicArc.rand, RANDOM_LEN);
        securec_check_c(rc, "\0", "\0");
        (void)ahwrite(p, 1, RANDOM_LEN, AH);
        free(p);
        p = NULL;
    }

    (void)ahprintf(AH, "--\n-- openGauss database dump\n--\n\n");

    if (AH->publicArc.verbose) {
        if (AH->archiveRemoteVersion != NULL) {
            (void)ahprintf(AH, "-- Dumped from database version %s\n", AH->archiveRemoteVersion);
        }
        if (AH->archiveDumpVersion != NULL) {
            (void)ahprintf(AH, "-- Dumped by gs_dump version %s\n", AH->archiveDumpVersion);
        }
        dumpTimestamp(AH, "Started on", AH->createDate);
    }

    if (ropt->single_txn) {
        if (AH->connection != NULL)
            StartTransaction(AH);
        else
            (void)ahprintf(AH, "START TRANSACTION;\n\n");
    }

    /*
     * Establish important parameter values right away.
     */
    _doSetFixedOutputState(AH);

    AH->stage = STAGE_PROCESSING;

    /*
     * Drop the items at the start, in reverse order
     */
    if (ropt->dropSchema) {
        for (te = AH->toc->prev; te != AH->toc; te = te->prev) {
            AH->currentTE = te;

            /*
             * In createDB mode, issue a DROP *only* for the database as a
             * whole.  Issuing drops against anything else would be wrong,
             * because at this point we're connected to the wrong database.
             * Conversely, if we're not in createDB mode, we'd better not
             * issue a DROP against the database at all.
             */
            if (ropt->createDB) {
                if (strcmp(te->desc, "DATABASE") != 0)
                    continue;
            } else {
                if (strcmp(te->desc, "DATABASE") == 0)
                    continue;
            }

            /* Otherwise, drop anything that's selected and has a dropStmt */
            if (((te->reqs & (REQ_SCHEMA | REQ_DATA)) != 0) && (te->dropStmt != NULL) && (strlen(te->dropStmt) != 0)) {
                /* Show namespace if available */
                if (te->nmspace) {
                    ahlog(AH, 1, "dropping %s \"%s.%s\"\n", te->desc, te->nmspace, te->tag);
                } else {
                    ahlog(AH, 1, "dropping %s \"%s\"\n", te->desc, te->tag);
                }
                /* Select owner and schema as necessary */
                _becomeOwner(AH, te);
                _selectOutputSchema(AH, te->nmspace);
                /* Drop it */
                out_drop_stmt(AH, te);
            }
        }

        /*
         * _selectOutputSchema may have set currSchema to reflect the effect
         * of a "SET search_path" command it emitted.  However, by now we may
         * have dropped that schema; or it might not have existed in the first
         * place.  In either case the effective value of search_path will not
         * be what we think.  Forcibly reset currSchema so that we will
         * re-establish the search_path setting when needed (after creating
         * the schema).
         *
         * If we treated users as pg_dump'able objects then we'd need to reset
         * currUser here too.
         */
        if (AH->currSchema != NULL)
            free(AH->currSchema);
        AH->currSchema = NULL;
    }

    /*
     * In serial mode, we now process each non-ACL TOC entry.
     *
     * In parallel mode, turn control over to the parallel-restore logic.
     */
    if (parallel_mode) {
        restore_toc_entries_parallel(AH);
    } else {
        int sqlCnt = 0;
        if (RestoringToDB(AH)) {
            write_msg(NULL, "start restore operation ...\n");
        }

        for (te = AH->toc->next; te != AH->toc; te = te->next) {
            (void)restore_toc_entry(AH, te, ropt, false);

            sqlCnt++;
            if (RestoringToDB(AH) && (sqlCnt != 0) && ((sqlCnt % RESTORE_READ_CNT) == 0)) {
                write_msg(NULL, "%d SQL statements read in !\n", sqlCnt);
            }
        }

        if (RestoringToDB(AH)) {
            write_msg(NULL, "Finish reading %d SQL statements!\n", sqlCnt);
            write_msg(NULL, "end restore operation ...\n");
        }
    }

    /*
     * Scan TOC again to output ownership commands and ACLs
     */
    for (te = AH->toc->next; te != AH->toc; te = te->next) {
        AH->currentTE = te;

        /* Both schema and data objects might now have ownership/ACLs */
        if ((te->reqs & (REQ_SCHEMA | REQ_DATA)) != 0) {
            /* Show namespace if available */
            if (te->nmspace) {
                ahlog(AH, 1, "setting owner and privileges for %s \"%s.%s\"\n", te->desc, te->nmspace, te->tag);
            } else {
                ahlog(AH, 1, "setting owner and privileges for %s \"%s\"\n", te->desc, te->tag);
            }
            _printTocEntry(AH, te, ropt, false, true);
        }
    }

    if (ropt->single_txn) {
        if (NULL != (AH->connection))
            CommitTransaction(AH);
        else
            (void)ahprintf(AH, "COMMIT;\n\n");
    }

    if (AH->publicArc.verbose)
        dumpTimestamp(AH, "Completed on", time(NULL));

    (void)ahprintf(AH, "--\n-- openGauss database dump complete\n--\n\n");

    /*
     * Clean up & we're done.
     */
    AH->stage = STAGE_FINALIZING;

    if ((NULL != ropt->filename) || ropt->compression)
        RestoreOutput(AH, sav);

    if (ropt->useDB)
        DisconnectDatabase(&AH->publicArc);
}

/*
 * take_down_nsname_in_drop_stmt:
 *	  for dump v5 to restore in v1, objects in drop stmt has nsname,
 *	  this can make restore error, so take down the nsname of drop stmt.
 * stmt : input string
 * result : output result
 */
static void take_down_nsname_in_drop_stmt(const char *stmt, char *result, int len)
{
    char *first = strdup(stmt);	
    char *p = NULL;
    char *last = NULL;
    int ret = 0;
	
    /* seperate string by ' ', if substring has '.', then drop the subsubstring before '.' */
    while ((p = strsep(&first, " ")) != NULL) {
        if (strchr(p, '.') != NULL) {
            /* for cast object, should left the '(' */
            if (strchr(p, '(') != NULL) {
                p = strchr(p, '.');
                *p = '(';
            } else {
                p = strchr(p, '.') + 1;
            }
        }
        ret = strcat_s(result, len, p);
        securec_check_c(ret, "\0", "\0");
        ret = strcat_s(result, len, " ");
        securec_check_c(ret, "\0", "\0");
    }
	
    last = result + strlen(result) - 1;
    *last = '\0';
}

/*
 * Restore a single TOC item.  Used in both parallel and non-parallel restore;
 * is_parallel is true if we are in a worker child process.
 *
 * Returns 0 normally, but WORKER_CREATE_DONE or WORKER_INHIBIT_DATA if
 * the parallel parent has to make the corresponding status update.
 */
static int restore_toc_entry(ArchiveHandle* AH, TocEntry* te, RestoreOptions* ropt, bool is_parallel)
{
    int retval = 0;
    teReqs reqs;
    bool defnDumped = false;

    AH->currentTE = te;

    /* Work out what, if anything, we want from this entry */
    if (_tocEntryIsACL(te)) {
        reqs = (teReqs)0; /* ACLs are never restored here */
    } else {
        reqs = te->reqs;
    }

    /*
     * Ignore DATABASE entry unless we should create it.  We must check this
     * here, not in _tocEntryRequired, because the createDB option should not
     * affect emitting a DATABASE entry to an archive file.
     */
    if (!ropt->createDB && strcmp(te->desc, "DATABASE") == 0)
        reqs = (teReqs)0;

    /* Dump any relevant dump warnings to stderr */
    if (!ropt->suppressDumpWarnings && strcmp(te->desc, "WARNING") == 0) {
        if (!ropt->dataOnly && te->defn != NULL && strlen(te->defn) != 0)
            write_msg(modulename, "warning from original dump file: %s\n", te->defn);
        else if (te->copyStmt != NULL && strlen(te->copyStmt) != 0)
            write_msg(modulename, "warning from original dump file: %s\n", te->copyStmt);
    }

    defnDumped = false;

    if ((reqs & REQ_SCHEMA) != 0) { /* We want the schema */
        /* Show namespace if available */
        if (te->nmspace) {
            ahlog(AH, 1, "creating %s \"%s.%s\"\n", te->desc, te->nmspace, te->tag);
        } else {
            ahlog(AH, 1, "creating %s \"%s\"\n", te->desc, te->tag);
        }

        _printTocEntry(AH, te, ropt, false, false);
        defnDumped = true;

        if (strcmp(te->desc, "TABLE") == 0) {
            if (AH->lastErrorTE == te) {
                /*
                 * We failed to create the table. If
                 * --no-data-for-failed-tables was given, mark the
                 * corresponding TABLE DATA to be ignored.
                 *
                 * In the parallel case this must be done in the parent, so we
                 * just set the return value.
                 */
                if (ropt->noDataForFailedTables) {
                    if (is_parallel) {
                        retval = WORKER_INHIBIT_DATA;
                    } else {
                        inhibit_data_for_failed_table(AH, te);
                    }
                }
            } else {
                /*
                 * We created the table successfully.  Mark the corresponding
                 * TABLE DATA for possible truncation.
                 *
                 * In the parallel case this must be done in the parent, so we
                 * just set the return value.
                 */
                if (is_parallel) {
                    retval = WORKER_CREATE_DONE;
                } else {
                    mark_create_done(AH, te);
                }
            }
        }

        /* If we created a DB, connect to it... */
        if (strcmp(te->desc, "DATABASE") == 0) {
            ahlog(AH, 1, "connecting to new database \"%s\"\n", te->tag);
            _reconnectToDB(AH, te->tag);

            if (ropt->dbname != NULL)
                free(ropt->dbname);

            ropt->dbname = gs_strdup(te->tag);
        }
    }

    /*
     * If we have a data component, then process it
     */
    if ((reqs & REQ_DATA) != 0) {
        /*
         * hadDumper will be set if there is genuine data component for this
         * node. Otherwise, we need to check the defn field for statements
         * that need to be executed in data-only restores.
         */
        if (te->hadDumper) {
            /*
             * If we can output the data, then restore it.
             */
            if (AH->PrintTocDataptr != NULL) {
                _printTocEntry(AH, te, ropt, true, false);

                if (strcmp(te->desc, "BLOBS") == 0 || strcmp(te->desc, "BLOB COMMENTS") == 0) {
                    ahlog(AH, 1, "restoring %s\n", te->desc);

                    _selectOutputSchema(AH, "pg_catalog");

                    (*AH->PrintTocDataptr)(AH, te, ropt);
                } else {
                    _disableTriggersIfNecessary(AH, te, ropt);

                    /* Select owner and schema as necessary */
                    _becomeOwner(AH, te);
                    _selectOutputSchema(AH, te->nmspace);
                    /* Show namespace if available */
                    if (te->nmspace) {
                        ahlog(AH, 1, "restoring data for table \"%s.%s\"\n", te->nmspace, te->tag);
                    } else {
                        ahlog(AH, 1, "restoring data for table \"%s\"\n", te->tag);
                    }

                    /*
                     * In parallel restore, if we created the table earlier in
                     * the run then we wrap the COPY in a transaction and
                     * precede it with a TRUNCATE.	If archiving is not on
                     * this prevents WAL-logging the COPY.	This obtains a
                     * speedup similar to that from using single_txn mode in
                     * non-parallel restores.
                     */
                    if (is_parallel && te->created) {
                        /*
                         * Parallel restore is always talking directly to a
                         * server, so no need to see if we should issue BEGIN.
                         */
                        StartTransaction(AH);

                        /*
                         * If the server version is >= 8.4, make sure we issue
                         * TRUNCATE with ONLY so that child tables are not
                         * wiped.
                         */
                        (void)ahprintf(AH,
                            "TRUNCATE TABLE %s%s;\n\n",
                            (PQserverVersion(AH->connection) >= 80400 ? "ONLY " : ""),
                            fmtId(te->tag));
                    }

                    /*
                     * If we have a copy statement, use it.
                     */
                    if ((te->copyStmt != NULL) && strlen(te->copyStmt) > 0) {
                        (void)ahprintf(AH, "%s", te->copyStmt);
                        AH->outputKind = OUTPUT_COPYDATA;
                    } else
                        AH->outputKind = OUTPUT_OTHERDATA;

                    (*AH->PrintTocDataptr)(AH, te, ropt);

                    /*
                     * Terminate COPY if needed.
                     */
                    if (AH->outputKind == OUTPUT_COPYDATA && RestoringToDB(AH)) {
                        EndDBCopyMode(AH, te);
                    }
                    AH->outputKind = OUTPUT_SQLCMDS;

                    /* close out the transaction started above */
                    if (is_parallel && te->created) {
                        CommitTransaction(AH);
                    }

                    _enableTriggersIfNecessary(AH, te, ropt);
                }
            }
        } else if (!defnDumped) {
            /* If we haven't already dumped the defn part, do so now */
            ahlog(AH, 1, "executing %s %s\n", te->desc, te->tag);
            _printTocEntry(AH, te, ropt, false, false);
        }
    }

    return retval;
}

/*
 * Allocate a new RestoreOptions block.
 * This is mainly so we can initialize it, but also for future expansion,
 */
RestoreOptions* NewRestoreOptions(void)
{
    RestoreOptions* opts = NULL;

    opts = (RestoreOptions*)pg_calloc(1, sizeof(RestoreOptions));

    /* set any fields that shouldn't default to zeroes */
    opts->format = archUnknown;
    opts->promptPassword = TRI_DEFAULT;
    opts->dumpSections = DUMP_UNSECTIONED;
    opts->number_of_jobs = 1;

    return opts;
}

static void _disableTriggersIfNecessary(ArchiveHandle* AH, TocEntry* te, RestoreOptions* ropt)
{
    /* This hack is only needed in a data-only restore */
    if (!ropt->dataOnly || !ropt->disable_triggers)
        return;

    ahlog(AH, 1, "disabling triggers for %s\n", te->tag);

    /*
     * Become superuser if possible, since they are the only ones who can
     * disable constraint triggers.  If -S was not given, assume the initial
     * user identity is a superuser.  (XXX would it be better to become the
     * table owner?)
     */
    _becomeUser(AH, ropt->superuser);

    /*
     * Disable them.
     */
    _selectOutputSchema(AH, te->nmspace);

    (void)ahprintf(AH, "ALTER TABLE %s DISABLE TRIGGER ALL;\n\n", fmtId(te->tag));
}

static void _enableTriggersIfNecessary(ArchiveHandle* AH, TocEntry* te, RestoreOptions* ropt)
{
    /* This hack is only needed in a data-only restore */
    if (!ropt->dataOnly || !ropt->disable_triggers)
        return;

    ahlog(AH, 1, "enabling triggers for %s\n", te->tag);

    /*
     * Become superuser if possible, since they are the only ones who can
     * disable constraint triggers.  If -S was not given, assume the initial
     * user identity is a superuser.  (XXX would it be better to become the
     * table owner?)
     */
    _becomeUser(AH, ropt->superuser);

    /*
     * Enable them.
     */
    _selectOutputSchema(AH, te->nmspace);

    (void)ahprintf(AH, "ALTER TABLE %s ENABLE TRIGGER ALL;\n\n", fmtId(te->tag));
}

/*
 * This is a routine that is part of the dumper interface, hence the 'Archive*' parameter.
 */
/* Public */
size_t WriteData(Archive* AHX, const void* data, size_t dLen)
{
    ArchiveHandle* AH = (ArchiveHandle*)AHX;

    if (NULL == (AH->currToc))
        exit_horribly(
            modulename, "internal error -- WriteData cannot be called outside the context of a DataDumper routine\n");

    return (*AH->WriteDataptr)(AH, data, dLen);
}

/*
 * Create a new TOC entry. The TOC was designed as a TOC, but is now the
 * repository for all metadata. But the name has stuck.
 */
/* Public */
void ArchiveEntry(Archive* AHX, CatalogId catalogId, DumpId dumpId, const char* tag, const char* nmspace,
    const char* tablespace, const char* owner, bool withOids, const char* desc, teSection section, const char* defn,
    const char* dropStmt, const char* copyStmt, const DumpId* deps, int nDeps, DataDumperPtr dumpFn, void* dumpArg)
{
    ArchiveHandle* AH = (ArchiveHandle*)AHX;
    TocEntry* newToc = NULL;
    errno_t rc = 0;

    newToc = (TocEntry*)pg_calloc(1, sizeof(TocEntry));

    AH->tocCount++;
    if (dumpId > AH->maxDumpId)
        AH->maxDumpId = dumpId;

    newToc->prev = AH->toc->prev;
    newToc->next = AH->toc;
    AH->toc->prev->next = newToc;
    AH->toc->prev = newToc;

    newToc->catalogId = catalogId;
    newToc->dumpId = dumpId;
    newToc->section = section;

    newToc->tag = gs_strdup(tag);
    newToc->nmspace = nmspace != NULL ? gs_strdup(nmspace) : NULL;
    newToc->tablespace = tablespace != NULL ? gs_strdup(tablespace) : NULL;
    newToc->owner = gs_strdup(owner);
    newToc->withOids = withOids;
    newToc->desc = gs_strdup(desc);
    newToc->defn = gs_strdup(defn);
    newToc->dropStmt = gs_strdup(dropStmt);
    newToc->copyStmt = copyStmt != NULL ? gs_strdup(copyStmt) : NULL;

    if (nDeps > 0) {
        newToc->dependencies = (DumpId*)pg_malloc(nDeps * sizeof(DumpId));
        rc = memcpy_s(newToc->dependencies, (nDeps * sizeof(DumpId)), deps, nDeps * sizeof(DumpId));
        securec_check_c(rc, "\0", "\0");
        newToc->nDeps = nDeps;
    } else {
        newToc->dependencies = NULL;
        newToc->nDeps = 0;
    }

    newToc->dataDumper = dumpFn;
    newToc->dataDumperArg = dumpArg;
    newToc->hadDumper = dumpFn != NULL ? true : false;

    newToc->formatData = NULL;

    if (AH->ArchiveEntryptr != NULL) {
        (*AH->ArchiveEntryptr)(AH, newToc);
    }
}

/* Public */
void PrintTOCSummary(Archive* AHX, RestoreOptions* ropt)
{
    ArchiveHandle* AH = (ArchiveHandle*)AHX;
    TocEntry* te = NULL;
    teSection curSection;
    OutputContext sav;
    const char* fmtName = NULL;
    char* ah_archdbname = NULL;

    sav = SaveOutput(AH);
    if (ropt->filename != NULL)
        SetOutput(AH, ropt->filename, 0 /* no compression */);

    (void)ahprintf(AH, ";\n; Archive created at %s", ctime(&AH->createDate));
    ah_archdbname = replace_line_endings(AH->archdbname);
    (void)ahprintf(AH,
        ";     dbname: %s\n;     TOC Entries: %d\n;     Compression: %d\n",
        ah_archdbname,
        AH->tocCount,
        AH->compression);
    free(ah_archdbname);
    ah_archdbname = NULL;

    switch (AH->format) {
        case archCustom:
            fmtName = "CUSTOM";
            break;
        case archTar:
            fmtName = "TAR";
            break;
        case archDirectory:
            fmtName = "DIRECTORY";
            break;
        default:
            fmtName = "UNKNOWN";
            break;
    }

    (void)ahprintf(AH, ";     Dump Version: %d.%d-%d\n", AH->vmaj, AH->vmin, AH->vrev);
    (void)ahprintf(AH, ";     Format: %s\n", fmtName);
    (void)ahprintf(AH, ";     Integer: %d bytes\n", (int)AH->intSize);
    (void)ahprintf(AH, ";     Offset: %d bytes\n", (int)AH->offSize);
    if (NULL != (AH->archiveRemoteVersion)) {
        (void)ahprintf(AH, ";     Dumped from database version: %s\n", AH->archiveRemoteVersion);
    }
    if (NULL != (AH->archiveDumpVersion)) {
        (void)ahprintf(AH, ";     Dumped by gs_dump version: %s\n", AH->archiveDumpVersion);
    }

    (void)ahprintf(AH, ";\n;\n; Selected TOC Entries:\n;\n");

    curSection = SECTION_PRE_DATA;
    for (te = AH->toc->next; te != AH->toc; te = te->next) {
        if (te->section != SECTION_NONE)
            curSection = te->section;
        if (ropt->verbose || ((uint32)_tocEntryRequired(te, curSection, ropt) & (REQ_SCHEMA | REQ_DATA)) != 0) {
            char* sanitized_name = NULL;
            char* sanitized_schema = NULL;
            char* sanitized_owner = NULL;
            /*
             * As in _printTocEntry(), sanitize strings that might contain
             * newlines, to ensure that each logical output line is in fact
             * one physical output line. This prevents confusion when the
             * file is read by "pg_restore -L". Note that we currently don't
             * bother to quote names, meaning that the name fields aren't
             * automatically parseable. "pg_restore -L" doesn't care because
             * it only examines the dumpId field, but someday we might want to
             * try harder.
             */
            sanitized_name = replace_line_endings(te->tag);
            if (NULL != (te->nmspace)) {
                sanitized_schema = replace_line_endings(te->nmspace);
            } else {
                sanitized_schema = gs_strdup("-");
            }
            sanitized_owner = replace_line_endings(te->owner);

            (void)ahprintf(AH,
                "%d; %u %u %s %s %s %s\n",
                te->dumpId,
                te->catalogId.tableoid,
                te->catalogId.oid,
                te->desc,
                sanitized_schema,
                sanitized_name,
                sanitized_owner);
            free(sanitized_name);
            sanitized_name = NULL;
            free(sanitized_schema);
            sanitized_schema = NULL;
            free(sanitized_owner);
            sanitized_owner = NULL;
        }
        if (ropt->verbose && te->nDeps > 0) {
            int i;

            (void)ahprintf(AH, ";\tdepends on:");
            for (i = 0; i < te->nDeps; i++)
                (void)ahprintf(AH, " %d", te->dependencies[i]);
            (void)ahprintf(AH, "\n");
        }
    }

    if (NULL != (ropt->filename))
        RestoreOutput(AH, sav);
}

/***********
 * BLOB Archival
 ***********/
/* Called by a dumper to signal start of a BLOB */
int StartBlob(Archive* AHX, Oid oid)
{
    ArchiveHandle* AH = (ArchiveHandle*)AHX;

    if (NULL == (AH->StartBlobptr))
        exit_horribly(modulename, "large-object output not supported in chosen format\n");

    (*AH->StartBlobptr)(AH, AH->currToc, oid);

    return 1;
}

/* Called by a dumper to signal end of a BLOB */
int EndBlob(Archive* AHX, Oid oid)
{
    ArchiveHandle* AH = (ArchiveHandle*)AHX;

    if (NULL != (AH->EndBlobptr))
        (*AH->EndBlobptr)(AH, AH->currToc, oid);

    return 1;
}

/**********
 * BLOB Restoration
 **********/
/*
 * Called by a format handler before any blobs are restored
 */
void StartRestoreBlobs(ArchiveHandle* AH)
{
    if (!AH->ropt->single_txn) {
        if (NULL != (AH->connection))
            StartTransaction(AH);
        else
            (void)ahprintf(AH, "START TRANSACTION;\n\n");
    }

    AH->blobCount = 0;
}

/*
 * Called by a format handler after all blobs are restored
 */
void EndRestoreBlobs(ArchiveHandle* AH)
{
    if (!AH->ropt->single_txn) {
        if (NULL != (AH->connection))
            CommitTransaction(AH);
        else
            (void)ahprintf(AH, "COMMIT;\n\n");
    }

    ahlog(AH, 1, ngettext("restored %d large object\n", "restored %d large objects\n", AH->blobCount), AH->blobCount);
}

/*
 * Called by a format handler to initiate restoration of a blob
 */
void StartRestoreBlob(ArchiveHandle* AH, Oid oid, bool drop)
{
    bool old_blob_style = (AH->version < K_VERS_1_12);
    Oid loOid;

    AH->blobCount++;

    /* Initialize the LO Buffer */
    AH->lo_buf_used = 0;

    ahlog(AH, 1, "restoring large object with OID %u\n", oid);

    /* With an old archive we must do drop and create logic here */
    if (old_blob_style && drop)
        DropBlobIfExists(AH, oid);

    if (NULL != (AH->connection)) {
        if (old_blob_style) {
            loOid = lo_create(AH->connection, oid);
            if (loOid == 0 || loOid != oid)
                exit_horribly(modulename, "could not create large object %u: %s", oid, PQerrorMessage(AH->connection));
        }
        AH->loFd = lo_open(AH->connection, oid, INV_WRITE);
        if (AH->loFd == -1)
            exit_horribly(modulename, "could not open large object %u: %s", oid, PQerrorMessage(AH->connection));
    } else {
        if (old_blob_style)
            (void)ahprintf(AH, "SELECT pg_catalog.lo_open(pg_catalog.lo_create('%u'), %d);\n", oid, INV_WRITE);
        else
            (void)ahprintf(AH, "SELECT pg_catalog.lo_open('%u', %d);\n", oid, INV_WRITE);
    }

    AH->writingBlob = 1;
}

void EndRestoreBlob(ArchiveHandle* AH, Oid oid)
{
    if (AH->lo_buf_used > 0) {
        /* Write remaining bytes from the LO buffer */
        dump_lo_buf(AH);
    }

    AH->writingBlob = 0;

    if (NULL != (AH->connection)) {
        (void)lo_close(AH->connection, AH->loFd);
        AH->loFd = -1;
    } else {
        (void)ahprintf(AH, "SELECT pg_catalog.lo_close(0);\n\n");
    }
}

/***********
 * Sorting and Reordering
 ***********/
void SortTocFromFile(Archive* AHX, RestoreOptions* ropt)
{
    ArchiveHandle* AH = (ArchiveHandle*)AHX;
    FILE* fh = NULL;
    char buf[100] = {0};
    bool incomplete_line = false;

    /* Allocate space for the 'wanted' array, and init it */
    ropt->idWanted = (bool*)pg_malloc(sizeof(bool) * AH->maxDumpId);
    errno_t rc = memset_s(ropt->idWanted, (sizeof(bool) * AH->maxDumpId), 0, sizeof(bool) * AH->maxDumpId);
    securec_check_c(rc, "\0", "\0");

    /* Setup the file */
    canonicalize_path(ropt->tocFile);
    fh = fopen(ropt->tocFile, PG_BINARY_R);
    if (fh == NULL)
        exit_horribly(modulename, "could not open TOC file \"%s\": %s\n", ropt->tocFile, strerror(errno));

    incomplete_line = false;
    while (fgets(buf, sizeof(buf), fh) != NULL) {
        bool prev_incomplete_line = incomplete_line;
        int buflen = 0;
        char* cmnt = NULL;
        char* endptr = NULL;
        DumpId id = 0;
        TocEntry* te = NULL;

        /*
         * Some lines in the file might be longer than sizeof(buf).  This is
         * no problem, since we only care about the leading numeric ID which
         * can be at most a few characters; but we have to skip continuation
         * bufferloads when processing a long line.
         */
        buflen = strlen(buf);
        if (buflen > 0 && buf[buflen - 1] == '\n')
            incomplete_line = false;
        else
            incomplete_line = true;
        if (prev_incomplete_line)
            continue;

        /* Truncate line at comment, if any */
        cmnt = strchr(buf, ';');
        if (cmnt != NULL)
            cmnt[0] = '\0';

        /* Ignore if all blank */
        if (strspn(buf, " \t\r\n") == strlen(buf))
            continue;

        /* Get an ID, check it's valid and not already seen */
        id = strtol(buf, &endptr, 10);
        if (endptr == buf || id <= 0 || id > AH->maxDumpId || ropt->idWanted[id - 1]) {
            write_msg(modulename, "WARNING: line ignored: %s\n", buf);
            continue;
        }

        /* Find TOC entry */
        te = getTocEntryByDumpId(AH, id);
        if (te == NULL)
            exit_horribly(modulename, "could not find entry for ID %d\n", id);

        /* Mark it wanted */
        ropt->idWanted[id - 1] = true;

        /*
         * Move each item to the end of the list as it is selected, so that
         * they are placed in the desired order.  Any unwanted items will end
         * up at the front of the list, which may seem unintuitive but it's
         * what we need.  In an ordinary serial restore that makes no
         * difference, but in a parallel restore we need to mark unrestored
         * items' dependencies as satisfied before we start examining
         * restorable items.  Otherwise they could have surprising
         * side-effects on the order in which restorable items actually get
         * restored.
         */
        _moveBefore(AH, AH->toc, te);
    }

    if (fclose(fh) != 0)
        exit_horribly(modulename, "could not close TOC file: %s\n", strerror(errno));
}

/**********************
 * 'Convenience functions that look like standard IO functions
 * for writing data when in dump mode.
 **********************/
/* Public */
int archputs(const char* s, Archive* AH)
{
    size_t ret = WriteData(AH, s, strlen(s));
    if (ret != strlen(s)) {
        write_msg(NULL, "could not write to output file: %s\n", strerror(errno));
        exit_nicely(1);
    }
    return ret;
}

/* Public */
int archprintf(Archive* AH, const char* fmt, ...)
{
    char* p = NULL;
    va_list ap;
    int bSize = strlen(fmt) + 256;
    int cnt = -1;
    size_t ret = 0;

    /*
     * This is paranoid: deal with the possibility that vsnprintf is willing
     * to ignore trailing null or returns > 0 even if string does not fit. It
     * may be the case that it returns cnt = bufsize
     */
    while (cnt < 0 || cnt >= (bSize - 1)) {
        if (p != NULL) {
            free(p);
            p = NULL;
        }
        bSize *= 2;
        p = (char*)pg_malloc(bSize);
        (void)va_start(ap, fmt);
        cnt = vsnprintf_s(p, bSize, bSize - 1, _(fmt), ap);
        va_end(ap);
    }

    ret = WriteData(AH, p, cnt);
    if (ret != (size_t)cnt) {
        write_msg(NULL, "could not write to output file: %s\n", strerror(errno));
        exit_nicely(1);
    }
    free(p);
    p = NULL;
    return cnt;
}

static int set_fn(ArchiveHandle* AH, const char* filename)
{
    int fn;
    if (NULL != filename) {
        fn = -1;
    } else if (NULL != (AH->FH)) {
        fn = fileno(AH->FH);
    } else if (NULL != (AH->fSpec)) {
        fn = -1;
        filename = AH->fSpec;
    } else {
        fn = fileno(stdout);
    }
    return fn;
}

/*******************************
 * Stuff below here should be 'private' to the archiver routines
 *******************************/
static void SetOutput(ArchiveHandle* AH, const char* filename, int compression)
{
    int fn = 0;
    int fn_copy = 0;

    fn = set_fn(AH, filename);

        /* If compression explicitly requested, use gzopen */
#ifdef HAVE_LIBZ
    if (compression != 0) {
        char fmode[10];
        int nRet = 0;

        /* Don't use PG_BINARY_x since this is zlib */
        nRet = snprintf_s(fmode, sizeof(fmode) / sizeof(char), sizeof(fmode) / sizeof(char) - 1, "wb%d", compression);
        securec_check_ss_c(nRet, "\0", "\0");

        if (fn >= 0) {
            fn_copy = dup(fn);
            if (fn_copy < 0) {
                exit_horribly(modulename, "could not open output file: %s\n", strerror(errno));
            }
            AH->OF = gzdopen(fn_copy, fmode);
        } else
            AH->OF = gzopen(filename, fmode);
        AH->gzOut = 1;
    } else
#endif
    { /* Use fopen */
        if (AH->mode == archModeAppend) {
            if (fn >= 0) {
                fn_copy = dup(fn);
                if (fn_copy < 0) {
                    exit_horribly(modulename, "could not open output file: %s\n", strerror(errno));
                }

                AH->OF = fdopen(fn_copy, PG_BINARY_A);
            } else {
                AH->OF = fopen(filename, PG_BINARY_A);
            }
        } else {
            if (fn >= 0) {
                fn_copy = dup(fn);
                if (fn_copy < 0) {
                    exit_horribly(modulename, "could not open output file: %s\n", strerror(errno));
                }

                AH->OF = fdopen(fn_copy, PG_BINARY_W);
            } else {
                AH->OF = fopen(filename, PG_BINARY_W);
            }
        }
        AH->gzOut = 0;
    }

    if (NULL == AH->OF) {
        if (NULL != filename)
            exit_horribly(modulename, "could not open output file \"%s\": %s\n", filename, strerror(errno));
        else
            exit_horribly(modulename, "could not open output file: %s\n", strerror(errno));
    }
}

static OutputContext SaveOutput(ArchiveHandle* AH)
{
    OutputContext sav;

    sav.OF = AH->OF;
    sav.gzOut = AH->gzOut;

    return sav;
}

static void RestoreOutput(ArchiveHandle* AH, OutputContext savedContext)
{
    int res;

    if (AH->gzOut)
#ifdef HAVE_LIBZ
        res = GZCLOSE((gzFile)(AH->OF));
#else
        res = GZCLOSE((FILE*)(AH->OF));
#endif
    else
        res = fclose((FILE*)AH->OF);

    if (res != 0)
        exit_horribly(modulename, "could not close output file: %s\n", strerror(errno));

    AH->gzOut = savedContext.gzOut;
    AH->OF = savedContext.OF;
}

/*
 *	Print formatted text to the output file (usually stdout).
 */
int ahprintf(ArchiveHandle* AH, const char* fmt, ...)
{
    char* p = NULL;
    va_list ap;
    int bSize = strlen(fmt) + 256; /* Usually enough */
    int cnt = -1;

    /*
     * This is paranoid: deal with the possibility that vsnprintf is willing
     * to ignore trailing null or returns > 0 even if string does not fit. It
     * may be the case that it returns cnt = bufsize.
     */
    while (cnt < 0 || cnt >= (bSize - 1)) {
        if (p != NULL) {
            free(p);
            p = NULL;
        }
        bSize *= 2;
        p = (char*)pg_malloc(bSize);
        (void)va_start(ap, fmt);
        cnt = vsnprintf(p, bSize, fmt, ap);
        va_end(ap);
    }
    (void)ahwrite(p, 1, cnt, AH);
    free(p);
    p = NULL;
    return cnt;
}

void ahlog(ArchiveHandle* AH, int level, const char* fmt, ...)
{
    va_list ap;

    if (AH->debugLevel < level && (!AH->publicArc.verbose || level > 1))
        return;

    (void)va_start(ap, fmt);
    vwrite_msg(NULL, fmt, ap);
    va_end(ap);
}

/*
 * Single place for logic which says 'We are restoring to a direct DB connection'.
 */
static int RestoringToDB(ArchiveHandle* AH)
{
    return ((AH->ropt != NULL) && AH->ropt->useDB && (AH->connection != NULL));
}

/*
 * Dump the current contents of the LO data buffer while writing a BLOB
 */
static void dump_lo_buf(ArchiveHandle* AH)
{
    if (NULL != (AH->connection)) {
        size_t res;

        res = lo_write(AH->connection, AH->loFd, (const char*)AH->lo_buf, AH->lo_buf_used);
        ahlog(AH,
            5,
            ngettext("wrote %lu byte of large object data (result = %lu)\n",
                "wrote %lu bytes of large object data (result = %lu)\n",
                AH->lo_buf_used),
            (unsigned long)(long)AH->lo_buf_used,
            (unsigned long)(long)res);
        if (res != AH->lo_buf_used)
            exit_horribly(modulename,
                "could not write to large object (result: %lu, expected: %lu)\n",
                (unsigned long)(long)res,
                (unsigned long)(long)AH->lo_buf_used);
    } else {
        PQExpBuffer buf = createPQExpBuffer();

        (void)appendByteaLiteralAHX(buf, (const unsigned char*)AH->lo_buf, AH->lo_buf_used, AH);

        /* Hack: turn off writingBlob so ahwrite doesn't recurse to here */
        AH->writingBlob = 0;
        (void)ahprintf(AH, "SELECT pg_catalog.lowrite(0, %s);\n", buf->data);
        AH->writingBlob = 1;

        (void)destroyPQExpBuffer(buf);
    }
    AH->lo_buf_used = 0;
}

/*
 *	Write buffer to the output file (usually stdout). This is used for
 *	outputting 'restore' scripts etc. It is even possible for an archive
 *	format to create a custom output routine to 'fake' a restore if it
 *	wants to generate a script (see TAR output).
 */
int ahwrite(const void* ptr, size_t size, size_t nmemb, ArchiveHandle* AH)
{
    size_t res;
    /* Database Security: Data importing/dumping support AES128. */
    bool encrypt_result = false;
    errno_t rc = 0;

    if (AH->writingBlob) {
        size_t remaining = size * nmemb;

        while (AH->lo_buf_used + remaining > AH->lo_buf_size) {
            size_t avail = AH->lo_buf_size - AH->lo_buf_used;

            rc = memcpy_s((char*)AH->lo_buf + AH->lo_buf_used, avail, ptr, avail);
            securec_check_c(rc, "\0", "\0");
            ptr = (const void*)((const char*)ptr + avail);
            remaining -= avail;
            AH->lo_buf_used += avail;
            dump_lo_buf(AH);
        }

        rc = memcpy_s((char*)AH->lo_buf + AH->lo_buf_used, remaining, ptr, remaining);
        securec_check_c(rc, "\0", "\0");
        AH->lo_buf_used += remaining;

        return size * nmemb;
    } else if (AH->gzOut) {
#ifdef HAVE_LIBZ
        res = GZWRITE(ptr, size, nmemb, (gzFile)AH->OF);
#else
        res = GZWRITE(ptr, size, nmemb, (FILE*)AH->OF);
#endif
        if (res != (nmemb * size))
            exit_horribly(modulename, "could not write to output file: %s\n", strerror(errno));
        return res;
    } else if (NULL != (AH->CustomOutptr)) {
        res = AH->CustomOutptr(AH, ptr, size * nmemb);
        if (res != (nmemb * size))
            exit_horribly(modulename, "could not write to custom output routine\n");
        return res;
    } else {
        /*
         * If we're doing a restore, and it's direct to DB, and we're
         * connected then send it to the DB.
         */
        if (RestoringToDB(AH)) {
            return ExecuteSqlCommandBuf(AH, (const char*)ptr, size * nmemb);
        } else {
            /* Database Security: Data importing/dumping support AES128. */
            if (true == AH->publicArc.encryptfile) {
                res = size * nmemb;
                encrypt_result = writeFileAfterEncryption((FILE*)AH->OF,
                    (char*)ptr,
                    (size * nmemb),
                    MAX_DECRYPT_BUFF_LEN,
                    AH->publicArc.Key,
                    AH->publicArc.rand);
                if (!encrypt_result)
                    exit_horribly(modulename, "Encryption failed: %s\n", strerror(errno));
            } else {
                res = fwrite(ptr, size, nmemb, (FILE*)AH->OF);
                if (res != nmemb)
                    exit_horribly(modulename, "could not write to output file: %s\n", strerror(errno));
            }
            return res;
        }
    }
}

/* on some error, we may decide to go on... */
void warn_or_exit_horribly(ArchiveHandle* AH, const char* pchmodulename, const char* fmt, ...)
{
    va_list ap;

    switch (AH->stage) {
        case STAGE_NONE:
            /* Do nothing special */
            break;

        case STAGE_INITIALIZING:
            if (AH->stage != AH->lastErrorStage)
                write_msg(pchmodulename, "Error while INITIALIZING:\n");
            break;

        case STAGE_PROCESSING:
            if (AH->stage != AH->lastErrorStage)
                write_msg(pchmodulename, "Error while PROCESSING TOC:\n");
            break;

        case STAGE_FINALIZING:
            if (AH->stage != AH->lastErrorStage)
                write_msg(pchmodulename, "Error while FINALIZING:\n");
            break;
        default:
            break;
    }
    if (AH->currentTE != NULL && AH->currentTE != AH->lastErrorTE) {
        write_msg(pchmodulename,
            "Error from TOC entry %d; %u %u %s %s %s\n",
            AH->currentTE->dumpId,
            AH->currentTE->catalogId.tableoid,
            AH->currentTE->catalogId.oid,
            AH->currentTE->desc ? AH->currentTE->desc : "(no desc)",
            AH->currentTE->tag ? AH->currentTE->tag : "(no tag)",
            AH->currentTE->owner ? AH->currentTE->owner : "(no owner)");
    }
    AH->lastErrorStage = AH->stage;
    AH->lastErrorTE = AH->currentTE;

    (void)va_start(ap, fmt);
    vwrite_msg(pchmodulename, fmt, ap);
    va_end(ap);

    if (AH->publicArc.exit_on_error)
        exit_nicely(1);
    else
        AH->publicArc.n_errors++;
}

#ifdef NOT_USED

static void _moveAfter(ArchiveHandle* AH, TocEntry* pos, TocEntry* te)
{
    /* Unlink te from list */
    te->prev->next = te->next;
    te->next->prev = te->prev;

    /* and insert it after "pos" */
    te->prev = pos;
    te->next = pos->next;
    pos->next->prev = te;
    pos->next = te;
}
#endif

static void _moveBefore(ArchiveHandle* AH, TocEntry* pos, TocEntry* te)
{
    ddr_Assert(te != NULL);
    /* Unlink te from list */
    te->prev->next = te->next;
    te->next->prev = te->prev;

    /* and insert it before "pos" */
    te->prev = pos->prev;
    te->next = pos;
    pos->prev->next = te;
    pos->prev = te;
}

/*
 * Build index arrays for the TOC list
 *
 * This should be invoked only after we have created or read in all the TOC
 * items.
 *
 * The arrays are indexed by dump ID (so entry zero is unused).  Note that the
 * array entries run only up to maxDumpId.	We might see dependency dump IDs
 * beyond that (if the dump was partial); so always check the array bound
 * before trying to touch an array entry.
 */
static void buildTocEntryArrays(ArchiveHandle* AH)
{
    DumpId maxDumpId = AH->maxDumpId;
    TocEntry* te = NULL;

    AH->tocsByDumpId = (TocEntry**)pg_calloc(maxDumpId + 1, sizeof(TocEntry*));
    AH->tableDataId = (DumpId*)pg_calloc(maxDumpId + 1, sizeof(DumpId));

    for (te = AH->toc->next; te != AH->toc; te = te->next) {
        /* this check is purely paranoia, maxDumpId should be correct */
        if (te->dumpId <= 0 || te->dumpId > maxDumpId)
            exit_horribly(modulename, "bad dumpId\n");

        /* tocsByDumpId indexes all TOCs by their dump ID */
        AH->tocsByDumpId[te->dumpId] = te;

        /*
         * tableDataId provides the TABLE DATA item's dump ID for each TABLE
         * TOC entry that has a DATA item.	We compute this by reversing the
         * TABLE DATA item's dependency, knowing that a TABLE DATA item has
         * just one dependency and it is the TABLE item.
         */
        if (strcmp(te->desc, "TABLE DATA") == 0 && te->nDeps > 0) {
            DumpId tableId = te->dependencies[0];

            /*
             * The TABLE item might not have been in the archive, if this was
             * a data-only dump; but its dump ID should be less than its data
             * item's dump ID, so there should be a place for it in the array.
             */
            if (tableId <= 0 || tableId > maxDumpId)
                exit_horribly(modulename, "bad table dumpId for TABLE DATA item\n");

            AH->tableDataId[tableId] = te->dumpId;
        }
    }
}

static TocEntry* getTocEntryByDumpId(ArchiveHandle* AH, DumpId id)
{
    /* build index arrays if we didn't already */
    if (AH->tocsByDumpId == NULL) {
        buildTocEntryArrays(AH);
    }

    if (id > 0 && id <= AH->maxDumpId) {
        return AH->tocsByDumpId[id];
    }
    return NULL;
}

teReqs TocIDRequired(ArchiveHandle* AH, DumpId id)
{
    TocEntry* te = getTocEntryByDumpId(AH, id);

    if (NULL == te) {
        return (teReqs)0;
    }
    return te->reqs;
}

size_t WriteOffset(ArchiveHandle* AH, pgoff_t o, int wasSet)
{
    int off;

    /* Save the flag */
    (void)(*AH->WriteByteptr)(AH, wasSet);

    /* Write out pgoff_t smallest byte first, prevents endian mismatch */
    for (off = 0; (unsigned int)(off) < sizeof(pgoff_t); off++) {
        (void)(*AH->WriteByteptr)(AH, o & 0xFF);
        o >>= 8;
    }
    return sizeof(pgoff_t) + 1;
}

int ReadOffset(ArchiveHandle* AH, pgoff_t* o)
{
    int i;
    size_t off;
    int offsetFlg;

    /* Initialize to zero */
    *o = 0;

    /* Check for old version */
    if (AH->version < K_VERS_1_7) {
        /* Prior versions wrote offsets using WriteInt */
        i = ReadInt(AH);
        /* -1 means not set */
        if (i < 0) {
            return K_OFFSET_POS_NOT_SET;
        } else if (i == 0) {
            return K_OFFSET_NO_DATA;
        }

        /* Cast to pgoff_t because it was written as an int. */
        *o = (pgoff_t)i;
        return K_OFFSET_POS_SET;
    }

    /*
     * Read the flag indicating the state of the data pointer. Check if valid
     * and die if not.
     *
     * This used to be handled by a negative or zero pointer, now we use an
     * extra byte specifically for the state.
     */
    offsetFlg = (uint32)((*AH->ReadByteptr)(AH)) & 0xFF;

    switch (offsetFlg) {
        case K_OFFSET_POS_NOT_SET:
        case K_OFFSET_NO_DATA:
        case K_OFFSET_POS_SET:

            break;

        default:
            exit_horribly(modulename, "unexpected data offset flag %d\n", offsetFlg);
    }

    uint64 oTemp = *o;
    /*
     * Read the bytes
     */
    for (off = 0; off < AH->offSize; off++) {
        if (off < sizeof(pgoff_t))
            oTemp |= ((uint64)((*AH->ReadByteptr)(AH))) << (off * 8);
        else {
            if ((*AH->ReadByteptr)(AH) != 0)
                exit_horribly(modulename, "file offset in dump file is too large\n");
        }
    }

    *o = oTemp;
    return offsetFlg;
}

size_t WriteInt(ArchiveHandle* AH, int i)
{
    size_t b;

    /*
     * This is a bit yucky, but I don't want to make the binary format very
     * dependent on representation, and not knowing much about it, I write out
     * a sign byte. If you change this, don't forget to change the file
     * version #, and modify readInt to read the new format AS WELL AS the old
     * formats.
     */
    /* SIGN byte */
    if (i < 0) {
        (void)(*AH->WriteByteptr)(AH, 1);
        i = -i;
    } else
        (void)(*AH->WriteByteptr)(AH, 0);

    for (b = 0; b < AH->intSize; b++) {
        (void)(*AH->WriteByteptr)(AH, i & 0xFF);
        i >>= 8;
    }

    return AH->intSize + 1;
}

int ReadInt(ArchiveHandle* AH)
{
    int res = 0;
    int bv;
    size_t b;
    int sign = 0; /* Default positive */
    int bitShift = 0;

    if (AH->version > K_VERS_1_0) {
        /* Read a sign byte */
        sign = (*AH->ReadByteptr)(AH);
    }
    for (b = 0; b < AH->intSize; b++) {
        bv = (*AH->ReadByteptr)(AH)&0xFF;
        if (bv != 0) {
            res = res + (bv << bitShift);
        }
        bitShift += 8;
    }

    if (sign) {
        res = -res;
    }

    return res;
}

size_t WriteStr(ArchiveHandle* AH, const char* c)
{
    size_t res;

    if (c != NULL) {
        res = WriteInt(AH, strlen(c));
        res += (*AH->WriteBufptr)(AH, c, strlen(c));
    } else {
        res = WriteInt(AH, -1);
    }

    return res;
}

char* ReadStr(ArchiveHandle* AH)
{
    char* buf = NULL;
    int l;

    l = ReadInt(AH);
    if (l < 0) {
        buf = NULL;
    } else {
        buf = (char*)pg_malloc((unsigned int)l + 1);
        if ((*AH->ReadBufptr)(AH, (void*)buf, l) != (unsigned int)(l))
            exit_horribly(modulename, "unexpected end of file\n");

        buf[l] = '\0';
    }

    return buf;
}

static int _discoverArchiveFormat(ArchiveHandle* AH)
{
    FILE* fh = NULL;
    char sig[6] = {0}; /* More than enough */
    int wantClose = 0;

    if (NULL != (AH->lookahead))
        free(AH->lookahead);

    AH->lookaheadSize = 512;
    AH->lookahead = (char*)pg_calloc(1, AH->lookaheadSize);
    AH->lookaheadLen = 0;
    AH->lookaheadPos = 0;

    if (NULL != (AH->fSpec)) {
        struct stat st;

        wantClose = 1;

        /*
         * Check if the specified archive is a directory. If so, check if
         * there's a "toc.dat" (or "toc.dat.gz") file in it.
         */
        if (stat(AH->fSpec, &st) == 0 && S_ISDIR(st.st_mode)) {
            char buf[MAXPGPATH] = {0};

            if (sprintf_s(buf, MAXPGPATH, "%s/toc.dat", AH->fSpec) == -1)
                exit_horribly(modulename, "directory name too long: \"%s\"\n", AH->fSpec);
            if (stat(buf, &st) == 0 && S_ISREG(st.st_mode)) {
                AH->format = archDirectory;
                return AH->format;
            }

#ifdef HAVE_LIBZ
            if (sprintf_s(buf, MAXPGPATH, "%s/toc.dat.gz", AH->fSpec) == -1)
                exit_horribly(modulename, "directory name too long: \"%s\"\n", AH->fSpec);
            if (stat(buf, &st) == 0 && S_ISREG(st.st_mode)) {
                AH->format = archDirectory;
                return AH->format;
            }
#endif
            exit_horribly(modulename,
                "directory \"%s\" does not appear to be a valid archive (\"toc.dat\" does not exist)\n",
                AH->fSpec);
            fh = NULL; /* keep compiler quiet */
        } else {
            fh = fopen(AH->fSpec, PG_BINARY_R);
            if (NULL == fh)
                exit_horribly(modulename, "could not open input file \"%s\": %s\n", AH->fSpec, strerror(errno));
        }
    } else {
        fh = stdin;
        if (fh == NULL)
            exit_horribly(modulename, "could not open input file: %s\n", strerror(errno));
    }

    size_t cnt = fread(sig, 1, 5, fh);
    if (cnt != 5) {
        if (ferror(fh)) {
            exit_horribly(modulename, "could not read input file: %s\n", strerror(errno));
        } else {
            exit_horribly(modulename, "input file is too short (read %lu, expected 5)\n", (unsigned long)(long)cnt);
        }
    }

    /* Save it, just in case we need it later */
    errno_t rc = strncpy_s(&AH->lookahead[0], AH->lookaheadSize, sig, 5);
    securec_check_c(rc, "\0", "\0");
    AH->lookaheadLen = 5;

    if (strncmp(sig, "PGDMP", 5) == 0) {
        /*
         * Finish reading (most of) a custom-format header.
         *
         * NB: this code must agree with ReadHead().
         */
        AH->vmaj = fgetc(fh);
        if (AH->vmaj == EOF) {
            exit_horribly(modulename, "unexpected end of file\n");
        }
        AH->vmin = fgetc(fh);
        if (AH->vmin == EOF) {
            exit_horribly(modulename, "unexpected end of file\n");
        }

        /* Save these too... */
        AH->lookahead[AH->lookaheadLen++] = AH->vmaj;
        AH->lookahead[AH->lookaheadLen++] = AH->vmin;

        /* Check header version; varies from V1.0 */
        if (AH->vmaj > 1 || ((AH->vmaj == 1) && (AH->vmin > 0))) { /* Version > 1.0 */
            AH->vrev = fgetc(fh);
            if (AH->vrev == EOF) {
                exit_horribly(modulename, "unexpected end of file\n");
            }
            AH->lookahead[AH->lookaheadLen++] = AH->vrev;
        } else {
            AH->vrev = 0;
        }

        /* Make a convenient integer <maj><min><rev>00 */
        AH->version = ((AH->vmaj * 256 + AH->vmin) * 256 + AH->vrev) * 256 + 0;

        AH->intSize = fgetc(fh);
        if ((int)AH->intSize == EOF) {
            exit_horribly(modulename, "unexpected end of file\n");
        }
        AH->lookahead[AH->lookaheadLen++] = AH->intSize;

        if (AH->version >= K_VERS_1_7) {
            AH->offSize = fgetc(fh);
            if ((int)AH->offSize == EOF) {
                exit_horribly(modulename, "unexpected end of file\n");
            }
            AH->lookahead[AH->lookaheadLen++] = AH->offSize;
        } else {
            AH->offSize = AH->intSize;
        }

        AH->format = (ArchiveFormat)fgetc(fh);
        if (AH->format == EOF) {
            exit_horribly(modulename, "unexpected end of file\n");
        }
        AH->lookahead[AH->lookaheadLen++] = AH->format;
    } else {
        /*
         * *Maybe* we have a tar archive format file or a text dump ... So,
         * read first 512 byte header...
         */
        cnt = fread(&AH->lookahead[AH->lookaheadLen], 1, 512 - AH->lookaheadLen, fh);
        AH->lookaheadLen += cnt;

        if (AH->lookaheadLen >= strlen(TEXT_DUMPALL_HEADER) &&
            (strncmp(AH->lookahead, TEXT_DUMP_HEADER, strlen(TEXT_DUMP_HEADER)) == 0 ||
                strncmp(AH->lookahead, TEXT_DUMPALL_HEADER, strlen(TEXT_DUMPALL_HEADER)) == 0)) {
            /*
             * looks like it's probably a text format dump. so suggest they
             * try psql
             */
            exit_horribly(modulename, "input file appears to be a text format dump. Please use gsql.\n");
        }

        if (AH->lookaheadLen != 512)
            exit_horribly(modulename, "input file does not appear to be a valid archive (too short?)\n");

        if (!isValidTarHeader(AH->lookahead))
            exit_horribly(modulename, "input file does not appear to be a valid archive\n");

        AH->format = archTar;
    }

    /* If we can't seek, then mark the header as read */
    if (fseeko(fh, 0, SEEK_SET) != 0) {
        /*
         * NOTE: Formats that use the lookahead buffer can unset this in their
         * Init routine.
         */
        AH->readHeader = 1;
    } else {
        AH->lookaheadLen = 0; /* Don't bother since we've reset the file */
    }

    /* Close the file */
    if (wantClose)
        if (fclose(fh) != 0)
            exit_horribly(modulename, "could not close input file: %s\n", strerror(errno));

    return AH->format;
}

/*
 * Allocate an archive handle
 */
static ArchiveHandle* _allocAH(const char* FileSpec, const ArchiveFormat fmt, const int compression, ArchiveMode mode)
{
    ArchiveHandle* AH = NULL;

    AH = (ArchiveHandle*)pg_calloc(1, sizeof(ArchiveHandle));

    /* AH debugLevel was 100; */
    AH->vmaj = K_VERS_MAJOR;
    AH->vmin = K_VERS_MINOR;
    AH->vrev = K_VERS_REV;

    /* Make a convenient integer <maj><min><rev>00 */
    AH->version = ((AH->vmaj * 256 + AH->vmin) * 256 + AH->vrev) * 256 + 0;

    /* initialize for backwards compatible string processing */
    AH->publicArc.encoding = 0; /* PG_SQL_ASCII */
    AH->publicArc.std_strings = false;

    /* sql error handling */
    AH->publicArc.exit_on_error = true;
    AH->publicArc.n_errors = 0;
    AH->publicArc.getHashbucketInfo = false;

    AH->archiveDumpVersion = gs_strdup(PG_VERSION);

    AH->createDate = time(NULL);

    AH->intSize = sizeof(int);
    AH->offSize = sizeof(pgoff_t);
    if (FileSpec != NULL) {
        AH->fSpec = gs_strdup(FileSpec);

        /*
         * Not used; maybe later....
         *
         * AH->workDir = gs_strdup(FileSpec); for(i=strlen(FileSpec) ; i > 0 ;
         * i--) if (AH->workDir[i-1] == '/')
         */
    } else {
        AH->fSpec = NULL;
    }

    AH->connection = NULL;
    AH->currUser = NULL;         /* unknown */
    AH->currSchema = NULL;       /* ditto */
    AH->currTablespace = NULL;   /* ditto */
    AH->currWithOids = (bool)-1; /* force SET */

    AH->toc = (TocEntry*)pg_calloc(1, sizeof(TocEntry));

    AH->toc->next = AH->toc;
    AH->toc->prev = AH->toc;

    AH->mode = mode;
    AH->compression = compression;

    errno_t rc = memset_s(&(AH->sqlparse), sizeof(AH->sqlparse), 0, sizeof(AH->sqlparse));
    securec_check_c(rc, "\0", "\0");

    /* Open stdout with no compression for AH output handle */
    AH->gzOut = 0;
    AH->OF = stdout;

    /*
     * On Windows, we need to use binary mode to read/write non-text archive
     * formats.  Force stdin/stdout into binary mode if that is what we are
     * using.
     */
#ifdef WIN32
    if (fmt != archNull && (AH->fSpec == NULL || strcmp(AH->fSpec, "") == 0)) {
        if (mode == archModeWrite)
            setmode(fileno(stdout), O_BINARY);
        else
            setmode(fileno(stdin), O_BINARY);
    }
#endif

    if (fmt == archUnknown)
        AH->format = (ArchiveFormat)_discoverArchiveFormat(AH);
    else
        AH->format = fmt;

    AH->promptPassword = TRI_DEFAULT;

    switch (AH->format) {
        case archCustom:
            InitArchiveFmt_Custom(AH);
            break;

        case archNull:
            InitArchiveFmt_Null(AH);
            break;

        case archDirectory:
            InitArchiveFmt_Directory(AH);
            break;

        case archTar:
            InitArchiveFmt_Tar(AH);
            break;

        default:
            exit_horribly(modulename, "unrecognized file format \"%d\"\n", fmt);
    }

    return AH;
}

void WriteDataChunks(ArchiveHandle* AH)
{
    TocEntry* te = NULL;
    StartDataPtr startPtr = NULL;
    EndDataPtr endPtr = NULL;

    for (te = AH->toc->next; te != AH->toc; te = te->next) {
        if (te->dataDumper != NULL && (te->reqs & REQ_DATA) != 0) {
            AH->currToc = te;

            if (strcmp(te->desc, "BLOBS") == 0) {
                startPtr = AH->StartBlobsptr;
                endPtr = AH->EndBlobsptr;
            } else {
                startPtr = AH->StartDataptr;
                endPtr = AH->EndDataptr;
            }

            if (startPtr != NULL) {
                (*startPtr)(AH, te);
            }

            /*
             * The user-provided DataDumper routine needs to call
             * AH->WriteData
             */
            (void)(*te->dataDumper)((Archive*)AH, te->dataDumperArg);

            if (endPtr != NULL) {
                (*endPtr)(AH, te);
            }
            AH->currToc = NULL;
        }
    }
}

void WriteToc(ArchiveHandle* AH)
{
    TocEntry* te = NULL;
    char workbuf[32] = {0};
    int tocCount;
    int i;
    int nRet = 0;

    /* count entries that will actually be dumped */
    tocCount = 0;
    for (te = AH->toc->next; te != AH->toc; te = te->next) {
        if ((te->reqs & (REQ_SCHEMA | REQ_DATA | REQ_SPECIAL)) != 0) {
            tocCount++;
        }
    }

    (void)WriteInt(AH, tocCount);

    for (te = AH->toc->next; te != AH->toc; te = te->next) {
        if ((te->reqs & (REQ_SCHEMA | REQ_DATA | REQ_SPECIAL)) == 0) {
            continue;
        }

        (void)WriteInt(AH, te->dumpId);
        (void)WriteInt(AH, te->dataDumper != NULL ? 1 : 0);

        /* OID is recorded as a string for historical reasons */
        nRet = snprintf_s(
            workbuf, sizeof(workbuf) / sizeof(char), sizeof(workbuf) / sizeof(char) - 1, "%u", te->catalogId.tableoid);
        securec_check_ss_c(nRet, "\0", "\0");
        (void)WriteStr(AH, workbuf);
        nRet = snprintf_s(
            workbuf, sizeof(workbuf) / sizeof(char), sizeof(workbuf) / sizeof(char) - 1, "%u", te->catalogId.oid);
        securec_check_ss_c(nRet, "\0", "\0");
        (void)WriteStr(AH, workbuf);

        (void)WriteStr(AH, te->tag);
        (void)WriteStr(AH, te->desc);
        (void)WriteInt(AH, te->section);
        (void)WriteStr(AH, te->defn);
        (void)WriteStr(AH, te->dropStmt);
        (void)WriteStr(AH, te->copyStmt);
        (void)WriteStr(AH, te->nmspace);
        (void)WriteStr(AH, te->tablespace);
        (void)WriteStr(AH, te->owner);
        (void)WriteStr(AH, te->withOids ? "true" : "false");

        /* Dump list of dependencies */
        for (i = 0; i < te->nDeps; i++) {
            nRet = snprintf_s(
                workbuf, sizeof(workbuf) / sizeof(char), sizeof(workbuf) / sizeof(char) - 1, "%d", te->dependencies[i]);
            securec_check_ss_c(nRet, "\0", "\0");
            (void)WriteStr(AH, workbuf);
        }
        (void)WriteStr(AH, NULL); /* Terminate List */

        if (NULL != (AH->WriteExtraTocptr)) {
            (*AH->WriteExtraTocptr)(AH, te);
        }
    }
}

void ReadToc(ArchiveHandle* AH)
{
    int i = 0;
    char* tmp = NULL;
    DumpId* deps = NULL;
    int depIdx = 0;
    int depSize = 0;
    TocEntry* te = NULL;
    int ret = 0;

    AH->tocCount = ReadInt(AH);
    AH->maxDumpId = 0;

    for (i = 0; i < AH->tocCount; i++) {
        te = (TocEntry*)pg_calloc(1, sizeof(TocEntry));
        te->dumpId = ReadInt(AH);

        if (te->dumpId > AH->maxDumpId) {
            AH->maxDumpId = te->dumpId;
        }

        /* Sanity check */
        if (te->dumpId <= 0) {
            exit_horribly(modulename, "entry ID %d out of range -- perhaps a corrupt TOC\n", te->dumpId);
        }

        te->hadDumper = false;
        if (ReadInt(AH)) {
            te->hadDumper = true;
        }

        if (AH->version >= K_VERS_1_8) {
            tmp = ReadStr(AH);
            ret = sscanf_s(tmp, "%u", &te->catalogId.tableoid);
            securec_check_for_sscanf_s(ret, 1, "\0", "\0");
            free(tmp);
            tmp = NULL;
        } else {
            te->catalogId.tableoid = InvalidOid;
        }

        tmp = ReadStr(AH);
        ret = sscanf_s(tmp, "%u", &te->catalogId.oid);
        securec_check_for_sscanf_s(ret, 1, "\0", "\0");
        free(tmp);
        tmp = NULL;

        te->tag = ReadStr(AH);
        te->desc = ReadStr(AH);

        if (AH->version >= K_VERS_1_11) {
            te->section = (teSection)ReadInt(AH);
        } else {
            /*
             * Rules for pre-8.4 archives wherein pg_dump hasn't classified
             * the entries into sections.  This list need not cover entry
             * types added later than 8.4.
             */
            if (strcmp(te->desc, "COMMENT") == 0 || strcmp(te->desc, "ACL") == 0 ||
                strcmp(te->desc, "ACL LANGUAGE") == 0)
                te->section = SECTION_NONE;
            else if (strcmp(te->desc, "TABLE DATA") == 0 || strcmp(te->desc, "BLOBS") == 0 ||
                     strcmp(te->desc, "BLOB COMMENTS") == 0)
                te->section = SECTION_DATA;
            else if (strcmp(te->desc, "CONSTRAINT") == 0 || strcmp(te->desc, "CHECK CONSTRAINT") == 0 ||
                     strcmp(te->desc, "FK CONSTRAINT") == 0 || strcmp(te->desc, "INDEX") == 0 ||
                     strcmp(te->desc, "RULE") == 0 || strcmp(te->desc, "TRIGGER") == 0)
                te->section = SECTION_POST_DATA;
            else
                te->section = SECTION_PRE_DATA;
        }

        te->defn = ReadStr(AH);
        te->dropStmt = ReadStr(AH);

        if (AH->version >= K_VERS_1_3) {
            te->copyStmt = ReadStr(AH);
        }
        if (AH->version >= K_VERS_1_6) {
            te->nmspace = ReadStr(AH);
        }
        if (AH->version >= K_VERS_1_10) {
            te->tablespace = ReadStr(AH);
        }
        te->owner = ReadStr(AH);
        if (AH->version >= K_VERS_1_9) {
            tmp = ReadStr(AH);
            if (strncmp(tmp, "true", strlen("true")) == 0) {
                te->withOids = true;
            } else {
                te->withOids = false;
            }

            free(tmp);
            tmp = NULL;
        } else
            te->withOids = true;

        /* Read TOC entry dependencies */
        if (AH->version >= K_VERS_1_5) {
            depSize = 100;
            deps = (DumpId*)pg_malloc(sizeof(DumpId) * depSize);
            depIdx = 0;
            for (;;) {
                tmp = ReadStr(AH);
                if (tmp == NULL) {
                    break; /* end of list */
                }
                if (depIdx >= depSize) {
                    depSize *= 2;
                    deps = (DumpId*)pg_realloc(deps, sizeof(DumpId) * depSize);
                }
                ret = sscanf_s(tmp, "%d", &deps[depIdx]);
                securec_check_for_sscanf_s(ret, 1, "\0", "\0");
                free(tmp);
                tmp = NULL;
                depIdx++;
            }

            if (depIdx > 0) { /* We have a non-null entry */
                deps = (DumpId*)pg_realloc(deps, sizeof(DumpId) * depIdx);
                te->dependencies = deps;
                te->nDeps = depIdx;
            } else {
                free(deps);
                deps = NULL;
                te->dependencies = NULL;
                te->nDeps = 0;
            }
        } else {
            te->dependencies = NULL;
            te->nDeps = 0;
        }

        if (NULL != (AH->ReadExtraTocptr)) {
            (*AH->ReadExtraTocptr)(AH, te);
        }
        ahlog(AH, 3, "read TOC entry %d (ID %d) for %s %s\n", i, te->dumpId, te->desc, te->tag);

        /* link completed entry into TOC circular list */
        te->prev = AH->toc->prev;
        AH->toc->prev->next = te;
        AH->toc->prev = te;
        te->next = AH->toc;

        /* special processing immediately upon read for some items */
        if (strcmp(te->desc, "ENCODING") == 0) {
            processEncodingEntry(AH, te);
        } else if (strcmp(te->desc, "STDSTRINGS") == 0) {
            processStdStringsEntry(AH, te);
        }
    }
}

static void processEncodingEntry(ArchiveHandle* AH, TocEntry* te)
{
    /* te->defn should have the form SET client_encoding = 'foo'; */
    char* defn = gs_strdup(te->defn);
    char* ptr1 = NULL;
    char* ptr2 = NULL;
    int encoding;

    ptr1 = strchr(defn, '\'');
    if (ptr1 != NULL) {
        ptr2 = strchr(++ptr1, '\'');
    }
    if (ptr2 != NULL) {
        *ptr2 = '\0';
        encoding = pg_char_to_encoding(ptr1);
        if (encoding < 0) {
            exit_horribly(modulename, "unrecognized encoding \"%s\"\n", ptr1);
        }
        AH->publicArc.encoding = encoding;
    } else {
        exit_horribly(modulename, "invalid ENCODING item: %s\n", te->defn);
    }
    free(defn);
    defn = NULL;
}

static void processStdStringsEntry(ArchiveHandle* AH, TocEntry* te)
{
    /* te->defn should have the form SET standard_conforming_strings = 'x'; */
    char* ptr1 = NULL;

    ptr1 = strchr(te->defn, '\'');
    if ((ptr1 != NULL) && strncmp(ptr1, "'on'", 4) == 0) {
        AH->publicArc.std_strings = true;
    } else if ((ptr1 != NULL) && strncmp(ptr1, "'off'", 5) == 0) {
        AH->publicArc.std_strings = false;
    } else {
        exit_horribly(modulename, "invalid STDSTRINGS item: %s\n", te->defn);
    }
}

static teReqs _tocEntryRequired(TocEntry* te, teSection curSection, RestoreOptions* ropt)
{
    teReqs res = (teReqs)(REQ_SCHEMA | REQ_DATA);

    /* ENCODING and STDSTRINGS items are treated specially */
    if (strcmp(te->desc, "ENCODING") == 0 || strcmp(te->desc, "STDSTRINGS") == 0)
        return REQ_SPECIAL;

    /* If it's an ACL, maybe ignore it */
    if (ropt->aclsSkip && _tocEntryIsACL(te))
        return (teReqs)0;

    /* If it's security labels, maybe ignore it */
    if (ropt->no_security_labels && strcmp(te->desc, "SECURITY LABEL") == 0)
        return (teReqs)0;

    /* If it's a subcription, maybe ignore it */
    if (ropt->no_subscriptions && strcmp(te->desc, "SUBSCRIPTION") == 0) {
        return (teReqs)0;
    }

    /* If it's a publication, maybe ignore it */
    if (ropt->no_publications && strcmp(te->desc, "PUBLICATION") == 0) {
        return (teReqs)0;
    }

    /* Ignore it if section is not to be dumped/restored */
    switch (curSection) {
        case SECTION_PRE_DATA:
            if (!(ropt->dumpSections & DUMP_PRE_DATA))
                return (teReqs)0;
            break;
        case SECTION_DATA:
            if (!(ropt->dumpSections & DUMP_DATA))
                return (teReqs)0;
            break;
        case SECTION_POST_DATA:
            if (!(ropt->dumpSections & DUMP_POST_DATA))
                return (teReqs)0;
            break;
        default:
            /* shouldn't get here, really, but ignore it */
            return (teReqs)0;
    }

    /* Check options for selective dump/restore */
    if (ropt->schemaNames.head != NULL) {
        /* If no nmspace is specified, it means all. */
        if (!ropt->selNamespace)
            return (teReqs)0;

        if (0 == strncmp("SCHEMA", te->desc, strlen("SCHEMA"))) {
            if (!(simple_string_list_member(&ropt->schemaNames, te->tag)))
                return (teReqs)0;
        } else if (te->nmspace != NULL) {
            if (!(simple_string_list_member(&ropt->schemaNames, te->nmspace)))
                return (teReqs)0;
        }
    }

    if (ropt->selTypes) {
        if (strcmp(te->desc, "TABLE") == 0 || strcmp(te->desc, "TABLE DATA") == 0) {
            if (!ropt->selTable) {
                return (teReqs)0;
            }
            if (ropt->tableNames.head != NULL && (!(simple_string_list_member(&ropt->tableNames, te->tag)))) {
                return (teReqs)0;
            }
        } else if (strcmp(te->desc, "INDEX") == 0) {
            if (!ropt->selIndex)
                return (teReqs)0;
            if (ropt->indexNames.head != NULL && (!(simple_string_list_member(&ropt->indexNames, te->tag))))
                return (teReqs)0;
        } else if (strcmp(te->desc, "FUNCTION") == 0) {
            if (!ropt->selFunction)
                return (teReqs)0;
            if (ropt->functionNames.head != NULL && (!(simple_string_list_member(&ropt->functionNames, te->tag))))
                return (teReqs)0;
        } else if (strcmp(te->desc, "TRIGGER") == 0) {
            if (!ropt->selTrigger)
                return (teReqs)0;
            if (ropt->triggerNames.head != NULL && (!(simple_string_list_member(&ropt->triggerNames, te->tag))))
                return (teReqs)0;
        } else {
            return (teReqs)0;
        }
    }

    /*
     * Check if we had a dataDumper. Indicates if the entry is schema or data
     */
    if (!te->hadDumper) {
        /*
         * Special Case: If 'SEQUENCE SET' or anything to do with BLOBs, then
         * it is considered a data entry.  We don't need to check for the
         * BLOBS entry or old-style BLOB COMMENTS, because they will have
         * hadDumper = true ... but we do need to check new-style BLOB
         * comments.
         */
        if (strcmp(te->desc, "SEQUENCE SET") == 0 || strcmp(te->desc, "BLOB") == 0 ||
            (strcmp(te->desc, "ACL") == 0 && strncmp(te->tag, "LARGE OBJECT ", 13) == 0) ||
            (strcmp(te->desc, "COMMENT") == 0 && strncmp(te->tag, "LARGE OBJECT ", 13) == 0) ||
            (strcmp(te->desc, "SECURITY LABEL") == 0 && strncmp(te->tag, "LARGE OBJECT ", 13) == 0))
            res = (teReqs)(res & REQ_DATA);
        else
            res = (teReqs)(res & ~REQ_DATA);
    }

    /*
     * Special case: <Init> type with <Max OID> tag; this is obsolete and we
     * always ignore it.
     */
    if ((strcmp(te->desc, "<Init>") == 0) && (strcmp(te->tag, "Max OID") == 0))
        return (teReqs)0;

    /* Mask it if we only want schema */
    if (ropt->schemaOnly)
        res = (teReqs)(res & REQ_SCHEMA);

    /* Mask it if we only want data */
    if (ropt->dataOnly)
        res = (teReqs)(res & REQ_DATA);

    /* Mask it if we don't have a schema contribution */
    if (NULL == (te->defn) || strlen(te->defn) == 0)
        res = (teReqs)(res & ~REQ_SCHEMA);

    /* Finally, if there's a per-ID filter, limit based on that as well */
    if ((ropt->idWanted != NULL) && !(ropt->idWanted[te->dumpId - 1]))
        return (teReqs)0;

    return res;
}

/*
 * Identify TOC entries that are ACLs.
 */
static bool _tocEntryIsACL(TocEntry* te)
{
    /* "ACL LANGUAGE" was a crock emitted only in PG 7.4 */
    if (strcmp(te->desc, "ACL") == 0 || strcmp(te->desc, "ACL LANGUAGE") == 0 || strcmp(te->desc, "DEFAULT ACL") == 0)
        return true;
    return false;
}

/*
 * Issue SET commands for parameters that we want to have set the same way
 * at all times during execution of a restore script.
 */
static void _doSetFixedOutputState(ArchiveHandle* AH)
{
    errno_t rc;
    /* Disable statement_timeout in archive for pg_restore/psql  */
    (void)ahprintf(AH, "SET statement_timeout = 0;\n");

    /* Ensure that all valid XML data will be accepted */
    (void)ahprintf(AH, "SET xmloption = content;\n");

    /* Select the correct character set encoding */
    (void)ahprintf(AH, "SET client_encoding = '%s';\n", pg_encoding_to_char(AH->publicArc.encoding));

    /* Select the correct string literal syntax */
    (void)ahprintf(AH, "SET standard_conforming_strings = %s;\n", AH->publicArc.std_strings ? "on" : "off");

    /* Select the role to be used during restore */
    if ((AH->ropt != NULL) && (AH->ropt->use_role != NULL)) {
        PGconn* conn = GetConnection(&AH->publicArc);
        char* retrolepasswd = PQescapeLiteral(conn, AH->ropt->rolepassword, strlen(AH->ropt->rolepassword));
        if (retrolepasswd == NULL) {
            write_msg(NULL, "Failed to escapes a string for an SQL command: %s\n", PQerrorMessage(conn));
            exit_nicely(1);
        }

        (void)ahprintf(AH, "SET ROLE %s PASSWORD %s;\n", fmtId(AH->ropt->use_role), retrolepasswd);
        rc = memset_s(retrolepasswd, strlen(retrolepasswd), 0, strlen(retrolepasswd));
        securec_check_c(rc, "\0", "\0");
        PQfreemem(retrolepasswd);
    }

    /* Make sure function checking is disabled */
    (void)ahprintf(AH, "SET check_function_bodies = false;\n");

    /* Avoid annoying notices etc */
    (void)ahprintf(AH, "SET client_min_messages = warning;\n");
    if (!AH->publicArc.std_strings)
        (void)ahprintf(AH, "SET escape_string_warning = off;\n");

    (void)ahprintf(AH, "\n");
}

/*
 * Issue a SET SESSION AUTHORIZATION command.  Caller is responsible
 * for updating state if appropriate.  If user is NULL or an empty string,
 * the specification DEFAULT will be used.
 */
static void _doSetSessionAuth(ArchiveHandle* AH, const char* user)
{
    if (RestoringToDB(AH)) {
        _doResetSessionAuth(AH);
    }
    PQExpBuffer cmd = createPQExpBuffer();
    char* rolepassword = NULL;

    (void)appendPQExpBuffer(cmd, "SET SESSION AUTHORIZATION ");
    /*
     * SQL requires a string literal here.	Might as well be correct.
     */
    if ((user != NULL) && *user) {
        (void)appendStringLiteralAHX(cmd, user, AH);
        rolepassword = getRolePassword(AH->connection, user);
        (void)appendPQExpBuffer(cmd, " PASSWORD %s", rolepassword);
        errno_t rc = memset_s(rolepassword, strlen(rolepassword), 0, strlen(rolepassword));
        securec_check_c(rc, "\0", "\0");
        free(rolepassword);
        rolepassword = NULL;
    } else {
        (void)appendPQExpBuffer(cmd, "DEFAULT");
    }
    (void)appendPQExpBuffer(cmd, ";");

    if (RestoringToDB(AH)) {
        PGresult* res = NULL;

        res = PQexec(AH->connection, cmd->data);
        if ((res == NULL) || PQresultStatus(res) != PGRES_COMMAND_OK)
            /* NOT warn_or_exit_horribly... use -O instead to skip this. */
            exit_horribly(modulename, "could not set session user to \"%s\": %s", user, PQerrorMessage(AH->connection));

        PQclear(res);
    } else {
        (void)ahprintf(AH, "%s\n\n", cmd->data);
    }

    (void)destroyPQExpBuffer(cmd);
}

/*
 * Issue a RESET SESSION AUTHORIZATION command. Only used before _doSetSessionAuth when gs_restoring.
 */
static void _doResetSessionAuth(ArchiveHandle* AH) {
    PQExpBuffer cmd = createPQExpBuffer();
    (void)appendPQExpBuffer(cmd, "RESET SESSION AUTHORIZATION;");
    if (RestoringToDB(AH)) {
        PGresult* res = NULL;

        res = PQexec(AH->connection, cmd->data);
        if ((res == NULL) || PQresultStatus(res) != PGRES_COMMAND_OK)
            /* NOT warn_or_exit_horribly... use -O instead to skip this. */
            exit_horribly(modulename, "could not reset session: %s", PQerrorMessage(AH->connection));
        PQclear(res);
    } else {
        // not used
        (void)ahprintf(AH, "%s\n\n", cmd->data);
    }

    (void)destroyPQExpBuffer(cmd);
}

/*
 * Issue a SET default_with_oids command.  Caller is responsible
 * for updating state if appropriate.
 */
static void _doSetWithOids(ArchiveHandle* AH, const bool withOids)
{
    PQExpBuffer cmd = createPQExpBuffer();

    (void)appendPQExpBuffer(cmd, "SET default_with_oids = %s;", withOids ? "true" : "false");

    if (RestoringToDB(AH)) {
        PGresult* res = NULL;

        res = PQexec(AH->connection, cmd->data);
        if ((res == NULL) || PQresultStatus(res) != PGRES_COMMAND_OK)
            warn_or_exit_horribly(
                AH, modulename, "could not set default_with_oids: %s", PQerrorMessage(AH->connection));

        PQclear(res);
    } else {
        (void)ahprintf(AH, "%s\n\n", cmd->data);
    }

    (void)destroyPQExpBuffer(cmd);
}

/*
 * Issue the commands to connect to the specified database.
 *
 * If we're currently restoring right into a database, this will
 * actually establish a connection. Otherwise it puts a \connect into
 * the script output.
 *
 * NULL dbname implies reconnecting to the current DB (pretty useless).
 */
static void _reconnectToDB(ArchiveHandle* AH, const char* dbname)
{
    if (RestoringToDB(AH)) {
        (void)ReconnectToServer(AH, dbname, NULL);
    } else {
        PQExpBuffer qry = createPQExpBuffer();

        (void)appendPQExpBuffer(qry, "\\connect %s\n\n", dbname != NULL ? fmtId(dbname) : "-");
        (void)ahprintf(AH, "%s", qry->data);
        (void)destroyPQExpBuffer(qry);
    }

    /*
     * NOTE: currUser keeps track of what the imaginary session user in our
     * script is.  It's now effectively reset to the original userID.
     */
    if (NULL != (AH->currUser))
        free(AH->currUser);
    AH->currUser = NULL;

    /* don't assume we still know the output schema, tablespace, etc either */
    if (NULL != (AH->currSchema))
        free(AH->currSchema);
    AH->currSchema = NULL;
    if (NULL != (AH->currTablespace))
        free(AH->currTablespace);
    AH->currTablespace = NULL;
    AH->currWithOids = (bool)-1;

    /* re-establish fixed state */
    _doSetFixedOutputState(AH);
}

/*
 * Become the specified user, and update state to avoid redundant commands
 *
 * NULL or empty argument is taken to mean restoring the session default
 */
static void _becomeUser(ArchiveHandle* AH, const char* user)
{
    if (NULL == user) {
        user = ""; /* avoid null pointers */
    }

    if ((AH->currUser != NULL) && strcmp(AH->currUser, user) == 0)
        return; /* no need to do anything */

    _doSetSessionAuth(AH, user);

    /*
     * NOTE: currUser keeps track of what the imaginary session user in our
     * script is
     */
    if (NULL != (AH->currUser))
        free(AH->currUser);
    AH->currUser = gs_strdup(user);
}

/*
 * Become the owner of the given TOC entry object.	If
 * changes in ownership are not allowed, this doesn't do anything.
 */
static void _becomeOwner(ArchiveHandle* AH, TocEntry* te)
{
    if ((AH->ropt != NULL) && (AH->ropt->noOwner || !AH->ropt->use_setsessauth))
        return;

    _becomeUser(AH, te->owner);
}

/*
 * Set the proper default_with_oids value for the table.
 */
static void _setWithOids(ArchiveHandle* AH, TocEntry* te)
{
    if (AH->currWithOids != te->withOids) {
        _doSetWithOids(AH, te->withOids);
        AH->currWithOids = te->withOids;
    }
}

/*
 * Issue the commands to select the specified schema as the current schema
 * in the target database.
 */
static void _selectOutputSchema(ArchiveHandle* AH, const char* schemaName)
{
    PQExpBuffer qry;

    if ((schemaName == NULL) || *schemaName == '\0' ||
        ((AH->currSchema != NULL) && strcmp(AH->currSchema, schemaName) == 0))
        return; /* no need to do anything */

    /*
     * It is invalid to set pg_temp or pg_catalog behind other schemas in search path explicitly.
     * The priority order is pg_temp, pg_catalog and other schemas.
     */
    qry = createPQExpBuffer();
    (void)appendPQExpBuffer(qry, "SET search_path = %s", fmtId(schemaName));

    if (RestoringToDB(AH)) {
        PGresult* res = NULL;

        res = PQexec(AH->connection, qry->data);
        if ((res == NULL) || PQresultStatus(res) != PGRES_COMMAND_OK)
            warn_or_exit_horribly(
                AH, modulename, "could not set search_path to \"%s\": %s", schemaName, PQerrorMessage(AH->connection));

        PQclear(res);
    } else {
        (void)ahprintf(AH, "%s;\n\n", qry->data);
    }

    if (NULL != (AH->currSchema))
        free(AH->currSchema);
    AH->currSchema = gs_strdup(schemaName);

    (void)destroyPQExpBuffer(qry);
}

/*
 * Issue the commands to select the specified tablespace as the current one
 * in the target database.
 */
static void _selectTablespace(ArchiveHandle* AH, const char* tablespace)
{
    PQExpBuffer qry;
    const char* want = NULL;
    const char* have = NULL;

    /* do nothing in --no-tablespaces mode */
    if (AH->ropt->noTablespace)
        return;

    have = AH->currTablespace;
    want = tablespace;

    /* no need to do anything for non-tablespace object */
    if (NULL == want)
        return;

    if ((have != NULL) && strcmp(want, have) == 0)
        return; /* no need to do anything */

    qry = createPQExpBuffer();

    if (strcmp(want, "") == 0) {
        /* We want the tablespace to be the database's default */
        (void)appendPQExpBuffer(qry, "SET default_tablespace = ''");
    } else {
        /* We want an explicit tablespace */
        (void)appendPQExpBuffer(qry, "SET default_tablespace = %s", fmtId(want));
    }

    if (RestoringToDB(AH)) {
        PGresult* res = NULL;

        res = PQexec(AH->connection, qry->data);
        if ((res == NULL) || PQresultStatus(res) != PGRES_COMMAND_OK)
            warn_or_exit_horribly(AH,
                modulename,
                "could not set default_tablespace to %s: %s",
                fmtId(want),
                PQerrorMessage(AH->connection));

        PQclear(res);
    } else
        (void)ahprintf(AH, "%s;\n\n", qry->data);

    if (NULL != (AH->currTablespace))
        free(AH->currTablespace);
    AH->currTablespace = gs_strdup(want);

    (void)destroyPQExpBuffer(qry);
}

/*
 * Extract an object description for a TOC entry, and append it to buf.
 *
 * This is not quite as general as it may seem, since it really only
 * handles constructing the right thing to put into ALTER ... OWNER TO.
 *
 * The whole thing is pretty grotty, but we are kind of stuck since the
 * information used is all that's available in older dump files.
 */
static void _getObjectDescription(PQExpBuffer buf, TocEntry* te, ArchiveHandle* AH)
{
    const char* type = te->desc;

    /* objects named by a schema and name */
    if (strcmp(type, "COLLATION") == 0 || strcmp(type, "CONVERSION") == 0 || strcmp(type, "DOMAIN") == 0 ||
        strcmp(type, "TABLE") == 0 || strcmp(type, "SYNONYM") == 0 || strcmp(type, "VIEW") == 0 ||
        strcmp(type, "SEQUENCE") == 0 || strcmp(type, "TYPE") == 0 || strcmp(type, "FOREIGN TABLE") == 0 ||
        strcmp(type, "TEXT SEARCH DICTIONARY") == 0 || strcmp(type, "TEXT SEARCH CONFIGURATION") == 0 ||
        strcmp(type, "MATERIALIZED VIEW") == 0 || strcmp(type, "LARGE SEQUENCE") == 0) {
        (void)appendPQExpBuffer(buf, "%s ", type);
        if ((te->nmspace != NULL) && te->nmspace[0]) /* is null pre-7.3 */
            (void)appendPQExpBuffer(buf, "%s.", fmtId(te->nmspace));

        /*
         * Pre-7.3 pg_dump would sometimes (not always) put a fmtId'd name
         * into te->tag for an index. This check is heuristic, so make its
         * scope as narrow as possible.
         */
        if (AH->version < K_VERS_1_7 && te->tag[0] == '"' && te->tag[strlen(te->tag) - 1] == '"' &&
            strcmp(type, "INDEX") == 0)
            (void)appendPQExpBuffer(buf, "%s", te->tag);
        else
            (void)appendPQExpBuffer(buf, "%s", fmtId(te->tag));
        return;
    }

    /* objects named by just a name */
    if (strcmp(type, "DATABASE") == 0 || strcmp(type, "PROCEDURAL LANGUAGE") == 0 || strcmp(type, "SCHEMA") == 0 ||
        strcmp(type, "DIRECTORY") == 0 || strcmp(type, "FOREIGN DATA WRAPPER") == 0 || strcmp(type, "SERVER") == 0 ||
        strcmp(type, "USER MAPPING") == 0 || strcmp(type, "PUBLICATION") == 0 || strcmp(type, "SUBSCRIPTION") == 0) {
        (void)appendPQExpBuffer(buf, "%s %s", type, fmtId(te->tag));
        return;
    }

    /* BLOBs just have a name, but it's numeric so must not use fmtId */
    if (strcmp(type, "BLOB") == 0) {
        (void)appendPQExpBuffer(buf, "LARGE OBJECT %s", te->tag);
        return;
    }

    /*
     * These object types require additional decoration.  Fortunately, the
     * information needed is exactly what's in the DROP command.
     *
     * Now function option not support "ALTER FUNCTION XX IF EXISTS" grammer
     */
    if (strcmp(type, "AGGREGATE") == 0 
            || strcmp(type, "FUNCTION") == 0 
            || strcmp(type, "PROCEDURE") == 0 
            || strcmp(type, "OPERATOR") == 0 
            || strcmp(type, "OPERATOR CLASS") == 0 
            || strcmp(type, "OPERATOR FAMILY") == 0) {
        /* Chop "DROP " off the front and make a modifiable copy */
        int len = (int)strlen("DROP ");
        char* first = NULL;
        char* last = NULL;

        if (strcmp(type, "FUNCTION") == 0) {
            len += (int)strlen("FUNCTION IF EXISTS ");
        }

        if (strcmp(type, "PROCEDURE") == 0) {
            len += (int)strlen("PROCEDURE IF EXISTS ");
        }

        first = gs_strdup(te->dropStmt + len);
        /* point to last character in string */
        last = first + strlen(first) - 1;

        /* Strip off any ';' or '\n' at the end */
        while (last >= first && (*last == '\n' || *last == ';'))
            last--;
        *(last + 1) = '\0';

        if (strcmp(type, "FUNCTION") == 0) {
            (void)appendPQExpBufferStr(buf, "FUNCTION ");
        }

        if (strcmp(type, "PROCEDURE") == 0) {
            (void)appendPQExpBufferStr(buf, "PROCEDURE ");
        }

        (void)appendPQExpBufferStr(buf, first);

        free(first);
        first = NULL;
        return;
    } else if (strcmp(type, "PACKAGE") == 0 || strcmp(type, "PACKAGE BODY") == 0) {
        return;
    }

    write_msg(modulename, "WARNING: don't know how to set owner for object type %s\n", type);
}

static void _printTocEntry(ArchiveHandle* AH, TocEntry* te, RestoreOptions* ropt, bool isData, bool acl_pass)
{
    /* ACLs are dumped only during acl pass */
    if (acl_pass) {
        if (!_tocEntryIsACL(te)) {
            return;
        }
    } else {
        if (_tocEntryIsACL(te)) {
            return;
        }
    }

    /*
     * Avoid dumping the public schema, as it will already be created ...
     * unless we are using --clean mode (and *not* --create mode), in which
     * case we've previously issued a DROP for it so we'd better recreate it.
     *
     * Likewise for its comment, if any. (We could try issuing the COMMENT
     * command anyway; but it'd fail if the restore is done as non-super-user,
     * so let's not.)
     *
     * XXX it looks pretty ugly to hard-wire the public schema like this, but
     * it sits in a sort of no-mans-land between being a system object and a
     * user object, so it really is special in a way.
     */
    if (!(ropt->dropSchema && !ropt->createDB)) {
        if (strcmp(te->desc, "SCHEMA") == 0 && strcmp(te->tag, "public") == 0)
            return;

        if (strcmp(te->desc, "COMMENT") == 0 && strcmp(te->tag, "SCHEMA public") == 0)
            return;
    }

    /* Select owner, schema, and tablespace as necessary */
    _becomeOwner(AH, te);
    _selectOutputSchema(AH, te->nmspace);
    _selectTablespace(AH, te->tablespace);

    /* Set up OID mode too */
    if (strcmp(te->desc, "TABLE") == 0)
        _setWithOids(AH, te);

    /* Emit header comment for item */
    if (!AH->noTocComments) {
        const char* pfx = NULL;
        char* sanitized_name = NULL;
        char* sanitized_schema = NULL;
        char* sanitized_owner = NULL;

        if (isData) {
            pfx = "Data for ";
        } else {
            pfx = "";
        }
        (void)ahprintf(AH, "--\n");
        if (AH->publicArc.verbose) {
            (void)ahprintf(
                AH, "-- TOC entry %d (class %u OID %u)\n", te->dumpId, te->catalogId.tableoid, te->catalogId.oid);
            if (te->nDeps > 0) {
                int i;

                (void)ahprintf(AH, "-- Dependencies:");
                for (i = 0; i < te->nDeps; i++)
                    (void)ahprintf(AH, " %d", te->dependencies[i]);
                (void)ahprintf(AH, "\n");
            }
        }

        /*
         * Zap any line endings embedded in user-supplied fields, to prevent
         * corruption of the dump (which could, in the worst case, present an
         * SQL injection vulnerability if someone were to incautiously load a
         * dump containing objects with maliciously crafted names).
         */
        sanitized_name = replace_line_endings(te->tag);
        if (NULL != (te->nmspace)) {
            sanitized_schema = replace_line_endings(te->nmspace);
        } else {
            sanitized_schema = gs_strdup("-");
        }
        if (!ropt->noOwner) {
            sanitized_owner = replace_line_endings(te->owner);
        } else {
            sanitized_owner = gs_strdup("-");
        }

        (void)ahprintf(AH,
            "-- %sName: %s; Type: %s; Schema: %s; Owner: %s",
            pfx,
            sanitized_name,
            te->desc,
            sanitized_schema,
            sanitized_owner);

        free(sanitized_name);
        sanitized_name = NULL;
        free(sanitized_schema);
        sanitized_schema = NULL;
        free(sanitized_owner);
        sanitized_owner = NULL;

        if (NULL != (te->tablespace) && !ropt->noTablespace) {
            char* sanitized_tablespace = NULL;

            sanitized_tablespace = replace_line_endings(te->tablespace);
            (void)ahprintf(AH, "; Tablespace: %s", sanitized_tablespace);
            free(sanitized_tablespace);
            sanitized_tablespace = NULL;
        }
        (void)ahprintf(AH, "\n");

        if (AH->PrintExtraTocptr != NULL) {
            (*AH->PrintExtraTocptr)(AH, te);
        }
        (void)ahprintf(AH, "--\n\n");
    }

    /*
     * Actually print the definition.
     *
     * Really crude hack for suppressing AUTHORIZATION clause that old pg_dump
     * versions put into CREATE SCHEMA.  We have to do this when --no-owner
     * mode is selected.  This is ugly, but I see no other good way ...
     */
    if (ropt->noOwner && strcmp(te->desc, "SCHEMA") == 0) {
        (void)ahprintf(AH, "CREATE SCHEMA %s;\n\n\n", fmtId(te->tag));
    } else if ((RestoringToDB(AH)) && strcmp(te->desc, "TEXT SEARCH CONFIGURATION") == 0) {
        if (strlen(te->defn) > 0) {
            /* parse the defination execute one by one command */
            char* start = te->defn;
            do {
                char* end = strstr(start, "\n--\n--ELEMENT\n--\n");
                if (NULL != end) {
                    end[1] = '\0';
                }

                (void)ahprintf(AH, "%s", start);

                if (NULL != end) {
                    end[1] = '-';
                    end = end + strlen("\n--\n--ELEMENT\n--\n");
                }

                start = end;
            } while (NULL != start);
        }
    } else {
        if (strlen(te->defn) > 0) {
            (void)ahprintf(AH, "%s\n\n", te->defn);
        }
    }

    /*
     * If we aren't using SET SESSION AUTH to determine ownership, we must
     * instead issue an ALTER OWNER command.  We assume that anything without
     * a DROP command is not a separately ownable object.  All the categories
     * with DROP commands must appear in one list or the other.
     */
    if (!ropt->noOwner && !ropt->use_setsessauth && strlen(te->owner) > 0 && strlen(te->dropStmt) > 0) {
        if (strcmp(te->desc, "AGGREGATE") == 0 
                || strcmp(te->desc, "BLOB") == 0 
                || strcmp(te->desc, "COLLATION") == 0 
                || strcmp(te->desc, "CONVERSION") == 0 
                || strcmp(te->desc, "DATABASE") == 0 
                || strcmp(te->desc, "DOMAIN") == 0 
                || strcmp(te->desc, "FOREIGN TABLE") == 0 
                || strcmp(te->desc, "FUNCTION") == 0 
                || strcmp(te->desc, "PROCEDURE") == 0 
                || strcmp(te->desc, "DIRECTORY") == 0 
                || strcmp(te->desc, "OPERATOR") == 0 
                || strcmp(te->desc, "OPERATOR CLASS") == 0 
                || strcmp(te->desc, "OPERATOR FAMILY") == 0 
                || strcmp(te->desc, "PROCEDURAL LANGUAGE") == 0 
                || strcmp(te->desc, "SCHEMA") == 0 
                || strcmp(te->desc, "TABLE") == 0 
                || strcmp(te->desc, "SYNONYM") == 0 
                || strcmp(te->desc, "TYPE") == 0 
                || strcmp(te->desc, "VIEW") == 0 
                || strcmp(te->desc, "MATERIALIZED VIEW") == 0
                || strcmp(te->desc, "SEQUENCE") == 0 
                || strcmp(te->desc, "LARGE SEQUENCE") == 0
                || strcmp(te->desc, "TEXT SEARCH DICTIONARY") == 0 
                || strcmp(te->desc, "TEXT SEARCH CONFIGURATION") == 0 
                || strcmp(te->desc, "FOREIGN DATA WRAPPER") == 0 
                || strcmp(te->desc, "SERVER") == 0 ||
                strcmp(te->desc, "PUBLICATION") == 0 ||
                strcmp(te->desc, "SUBSCRIPTION") == 0) {
            PQExpBuffer temp = createPQExpBuffer();
            (void)appendPQExpBuffer(temp, "ALTER ");
            _getObjectDescription(temp, te, AH);

            if ((binary_upgrade_oldowner != NULL) && (0 == strncmp(te->owner, binary_upgrade_oldowner, NAMEDATALEN))) {
                (void)appendPQExpBuffer(temp, " OWNER TO %s;", fmtId(binary_upgrade_newowner));
            } else {
                (void)appendPQExpBuffer(temp, " OWNER TO %s;", fmtId(te->owner));
            }

            (void)ahprintf(AH, "%s\n\n", temp->data);
            (void)destroyPQExpBuffer(temp);
        } else if (strcmp(te->desc, "CAST") == 0 || strcmp(te->desc, "CHECK CONSTRAINT") == 0 ||
                   strcmp(te->desc, "CONSTRAINT") == 0 || strcmp(te->desc, "DEFAULT") == 0 ||
                   strcmp(te->desc, "FK CONSTRAINT") == 0 || strcmp(te->desc, "INDEX") == 0 ||
                   strcmp(te->desc, "RULE") == 0 || strcmp(te->desc, "TRIGGER") == 0 ||
                   strcmp(te->desc, "USER MAPPING") == 0 || strcmp(te->desc, "PACKAGE BODY") ||
                   strcmp(te->desc, "PACKAGE")) {
            /* these object types don't have separate owners */
        } else {
            write_msg(modulename, "WARNING: don't know how to set owner for object type %s\n", te->desc);
        }
    }

    /*
     * If it's an ACL entry, it might contain SET SESSION AUTHORIZATION
     * commands, so we can no longer assume we know the current auth setting.
     */
    if (acl_pass) {
        if (NULL != (AH->currUser))
            free(AH->currUser);
        AH->currUser = NULL;
    }
}

/*
 * Sanitize a string to be included in an SQL comment or TOC listing,
 * by replacing any newlines with spaces.
 * The result is a freshly malloc'd string.
 */
static char* replace_line_endings(const char* str)
{
    char* result = NULL;
    char* s = NULL;

    result = gs_strdup(str);

    for (s = result; *s != '\0'; s++) {
        if (*s == '\n' || *s == '\r') {
            *s = ' ';
        }
    }

    return result;
}

void WriteHead(ArchiveHandle* AH)
{
    struct tm crtm;

    (void)(*AH->WriteBufptr)(AH, "PGDMP", 5); /* Magic code */
    (void)(*AH->WriteByteptr)(AH, AH->vmaj);
    (void)(*AH->WriteByteptr)(AH, AH->vmin);
    (void)(*AH->WriteByteptr)(AH, AH->vrev);
    (void)(*AH->WriteByteptr)(AH, AH->intSize);
    (void)(*AH->WriteByteptr)(AH, AH->offSize);
    (void)(*AH->WriteByteptr)(AH, AH->format);

#ifndef HAVE_LIBZ
    if (AH->compression != 0)
        write_msg(modulename,
            "WARNING: requested compression not available in this "
            "installation -- archive will be uncompressed\n");

    AH->compression = 0;
#endif

    (void)WriteInt(AH, AH->compression);

    if (localtime_r(&AH->createDate, &crtm) != NULL) {
        crtm = *localtime_r(&AH->createDate, &crtm);
        (void)WriteInt(AH, crtm.tm_sec);
        (void)WriteInt(AH, crtm.tm_min);
        (void)WriteInt(AH, crtm.tm_hour);
        (void)WriteInt(AH, crtm.tm_mday);
        (void)WriteInt(AH, crtm.tm_mon);
        (void)WriteInt(AH, crtm.tm_year);
        (void)WriteInt(AH, crtm.tm_isdst);
        (void)WriteStr(AH, PQdb(AH->connection));
        (void)WriteStr(AH, AH->publicArc.remoteVersionStr);
        (void)WriteStr(AH, PG_VERSION);
    }
}

void ReadHead(ArchiveHandle* AH)
{
    char tmpMag[7];
    int fmt;
    struct tm crtm;

    /*
     * If we haven't already read the header, do so.
     *
     * NB: this code must agree with _discoverArchiveFormat().	Maybe find a
     * way to unify the cases?
     */
    if (!AH->readHeader) {
        if ((*AH->ReadBufptr)(AH, tmpMag, 5) != 5)
            exit_horribly(modulename, "unexpected end of file\n");

        if (strncmp(tmpMag, "PGDMP", 5) != 0) {
            if (getAESLabelFile(AH->fSpec, AES_TEMP_DECRYPT_LABEL, PG_BINARY_R)) {
                if (!rmtree(AH->fSpec, true))
                    exit_horribly(modulename, "failed to remove archive %s.\n", AH->fSpec);
                exit_horribly(modulename, "the archive is encrpyted, please check the decrypt key\n");
            }
            exit_horribly(modulename, "did not find magic string in file header, the archive may be encrypted\n");
        }

        AH->vmaj = (*AH->ReadByteptr)(AH);
        AH->vmin = (*AH->ReadByteptr)(AH);

        if (AH->vmaj > 1 || ((AH->vmaj == 1) && (AH->vmin > 0))) { /* Version > 1.0 */
            AH->vrev = (*AH->ReadByteptr)(AH);
        } else {
            AH->vrev = 0;
        }

        AH->version = ((AH->vmaj * 256 + AH->vmin) * 256 + AH->vrev) * 256 + 0;

        if (AH->version < K_VERS_1_0 || AH->version > K_VERS_MAX)
            exit_horribly(modulename, "unsupported version (%d.%d) in file header\n", AH->vmaj, AH->vmin);

        AH->intSize = (*AH->ReadByteptr)(AH);
        if (AH->intSize > 32)
            exit_horribly(modulename, "sanity check on integer size (%lu) failed\n", (unsigned long)(long)AH->intSize);

        if (AH->intSize > sizeof(int))
            write_msg(modulename,
                "WARNING: archive was made on a machine with larger integers, some operations might fail\n");

        if (AH->version >= K_VERS_1_7) {
            AH->offSize = (*AH->ReadByteptr)(AH);
        } else {
            AH->offSize = AH->intSize;
        }

        fmt = (*AH->ReadByteptr)(AH);
        if (AH->format != fmt)
            exit_horribly(modulename, "expected format (%d) differs from format found in file (%d)\n", AH->format, fmt);
    }

    if (AH->version >= K_VERS_1_2) {
        if (AH->version < K_VERS_1_4) {
            AH->compression = (*AH->ReadByteptr)(AH);
        } else {
            AH->compression = ReadInt(AH);
        }
    } else {
        AH->compression = Z_DEFAULT_COMPRESSION;
    }

#ifndef HAVE_LIBZ
    if (AH->compression != 0)
        write_msg(modulename,
            "WARNING: archive is compressed, but this installation does not support compression -- no data will be "
            "available\n");
#endif

    if (AH->version >= K_VERS_1_4) {
        crtm.tm_sec = ReadInt(AH);
        crtm.tm_min = ReadInt(AH);
        crtm.tm_hour = ReadInt(AH);
        crtm.tm_mday = ReadInt(AH);
        crtm.tm_mon = ReadInt(AH);
        crtm.tm_year = ReadInt(AH);
        crtm.tm_isdst = ReadInt(AH);

        AH->archdbname = ReadStr(AH);

        AH->createDate = mktime(&crtm);

        if (AH->createDate == (time_t)-1)
            write_msg(modulename, "WARNING: invalid creation date in header\n");
    }

    if (AH->version >= K_VERS_1_10) {
        AH->archiveRemoteVersion = ReadStr(AH);
        if (AH->archiveDumpVersion != NULL)
            free(AH->archiveDumpVersion);
        AH->archiveDumpVersion = ReadStr(AH);
    }
}

/*
 * checkSeek
 *	  check to see if ftell/fseek can be performed.
 */
bool checkSeek(FILE* fp)
{
    pgoff_t tpos;

    /*
     * If pgoff_t is wider than long, we must have "real" fseeko and not an
     * emulation using fseek.  Otherwise report no seek capability.
     */
#ifndef HAVE_FSEEKO
    if (sizeof(pgoff_t) > sizeof(long))
        return false;
#endif

    /* Check that ftello works on this file */
    errno = 0;
    tpos = ftello(fp);
    if (errno)
        return false;

    /*
     * Check that fseeko(SEEK_SET) works, too.	NB: we used to try to test
     * this with fseeko(fp, 0, SEEK_CUR).  But some platforms treat that as a
     * successful no-op even on files that are otherwise unseekable.
     */
    if (fseeko(fp, tpos, SEEK_SET) != 0)
        return false;

    return true;
}

/*
 * dumpTimestamp
 */
static void dumpTimestamp(ArchiveHandle* AH, const char* msg, time_t tim)
{
    char buf[256];
    struct tm stTm;

    /*
     * We don't print the timezone on Win32, because the names are long and
     * localized, which means they may contain characters in various random
     * encodings; this has been seen to cause encoding errors when reading the
     * dump script.
     */
    if ((localtime_r(&tim, &stTm) != NULL) && (strftime(buf,
                                                   sizeof(buf),
#ifndef WIN32
                                                   "%Y-%m-%d %H:%M:%S %Z",
#else
                                                   "%Y-%m-%d %H:%M:%S",
#endif
                                                   localtime_r(&tim, &stTm)) != 0))
        (void)ahprintf(AH, "-- %s %s\n\n", msg, buf);
}

static void setProcessIdentifier(ParallelStateEntry* pse, ArchiveHandle* AH)
{
#ifdef WIN32
    pse->threadId = GetCurrentThreadId();
#else
    pse->pid = getpid();
#endif
    pse->AH = AH;
}

static void unsetProcessIdentifier(ParallelStateEntry* pse)
{
#ifdef WIN32
    pse->threadId = 0;
#else
    pse->pid = 0;
#endif
    pse->AH = NULL;
}

static ParallelStateEntry* GetMyPSEntry(ParallelState* pstate)
{
    int i;

    for (i = 0; i < pstate->numWorkers; i++)
#ifdef WIN32
        if (pstate->pse[i].threadId == GetCurrentThreadId())
#else
        if (pstate->pse[i].pid == getpid())
#endif
            return &(pstate->pse[i]);

    return NULL;
}

static void archive_close_connection(int code, void* arg)
{
    ShutdownInformation* si = (ShutdownInformation*)arg;

    if (NULL != (si->pstate)) {
        ParallelStateEntry* entry = GetMyPSEntry(si->pstate);

        if (entry != NULL && (entry->AH != NULL))
            DisconnectDatabase(&(entry->AH->publicArc));
    } else if (NULL != (si->AHX))
        DisconnectDatabase(si->AHX);
}

void on_exit_close_archive(Archive* AHX)
{
    shutdown_info.AHX = AHX;
    on_exit_nicely(archive_close_connection, &shutdown_info);
}

/*
 * Main engine for parallel restore.
 *
 * Work is done in three phases.
 * First we process all SECTION_PRE_DATA tocEntries, in a single connection,
 * just as for a standard restore.	Second we process the remaining non-ACL
 * steps in parallel worker children (threads on Windows, processes on Unix),
 * each of which connects separately to the database.  Finally we process all
 * the ACL entries in a single connection (that happens back in
 * RestoreArchive).
 */
static void restore_toc_entries_parallel(ArchiveHandle* AH)
{
    RestoreOptions* ropt = AH->ropt;
    int n_slots = ropt->number_of_jobs;
    ParallelSlot* slots = NULL;
    int work_status = 0;
    uint32 work_status_temp = 0;
    int next_slot = 0;
    bool skipped_some = false;
    TocEntry pending_list;
    TocEntry ready_list;
    TocEntry* next_work_item = NULL;
    TocEntry* te = NULL;
    ParallelState* pstate = NULL;
    int i = 0;

    ahlog(AH, 2, "entering restore_toc_entries_parallel\n");

    slots = (ParallelSlot*)pg_calloc(n_slots, sizeof(ParallelSlot));
    pstate = (ParallelState*)pg_malloc(sizeof(ParallelState));
    pstate->pse = (ParallelStateEntry*)pg_calloc(n_slots, sizeof(ParallelStateEntry));
    pstate->numWorkers = ropt->number_of_jobs;
    for (i = 0; i < pstate->numWorkers; i++)
        unsetProcessIdentifier(&(pstate->pse[i]));

    /* Adjust dependency information */
    fix_dependencies(AH);

    /*
     * Do all the early stuff in a single connection in the parent. There's no
     * great point in running it in parallel, in fact it will actually run
     * faster in a single connection because we avoid all the connection and
     * setup overhead.  Also, pre-9.2 pg_dump versions were not very good
     * about showing all the dependencies of SECTION_PRE_DATA items, so we do
     * not risk trying to process them out-of-order.
     *
     * Note: as of 9.2, it should be guaranteed that all PRE_DATA items appear
     * before DATA items, and all DATA items before POST_DATA items.  That is
     * not certain to be true in older archives, though, so this loop is coded
     * to not assume it.
     */
    skipped_some = false;

    int sqlCnt = 0;
    if (RestoringToDB(AH)) {
        write_msg(NULL, "start restore operation ...\n");
    }

    for (next_work_item = AH->toc->next; next_work_item != AH->toc; next_work_item = next_work_item->next) {
        /* NB: process-or-continue logic must be the inverse of loop below */
        if (next_work_item->section != SECTION_PRE_DATA) {
            /* DATA and POST_DATA items are just ignored for now */
            if (next_work_item->section == SECTION_DATA || next_work_item->section == SECTION_POST_DATA) {
                skipped_some = true;
                continue;
            } else {
                /*
                 * SECTION_NONE items, such as comments, can be processed now
                 * if we are still in the PRE_DATA part of the archive.  Once
                 * we've skipped any items, we have to consider whether the
                 * comment's dependencies are satisfied, so skip it for now.
                 */
                if (skipped_some) {
                    continue;
                }
            }
        }

        ahlog(AH, 1, "processing item %d %s %s\n", next_work_item->dumpId, next_work_item->desc, next_work_item->tag);

        (void)restore_toc_entry(AH, next_work_item, ropt, false);

        sqlCnt++;
        if (RestoringToDB(AH) && (sqlCnt != 0) && ((sqlCnt % RESTORE_READ_CNT) == 0)) {
            write_msg(NULL, "%d SQL statements read in !\n", sqlCnt);
        }

        /* there should be no touch of ready_list here, so pass NULL */
        reduce_dependencies(AH, next_work_item, NULL);
    }

    /*
     * Now close parent connection in prep for parallel steps.	We do this
     * mainly to ensure that we don't exceed the specified number of parallel
     * connections.
     */
    DisconnectDatabase(&AH->publicArc);

    /*
     * Set the pstate in the shutdown_info. The exit handler uses pstate if
     * set and falls back to AHX otherwise.
     */
    shutdown_info.pstate = pstate;

    /* blow away any transient state from the old connection */
    if (NULL != (AH->currUser))
        free(AH->currUser);
    AH->currUser = NULL;
    if (NULL != (AH->currSchema))
        free(AH->currSchema);
    AH->currSchema = NULL;
    if (NULL != (AH->currTablespace))
        free(AH->currTablespace);
    AH->currTablespace = NULL;
    AH->currWithOids = (bool)-1;

    /*
     * Initialize the lists of pending and ready items.  After this setup, the
     * pending list is everything that needs to be done but is blocked by one
     * or more dependencies, while the ready list contains items that have no
     * remaining dependencies.	Note: we don't yet filter out entries that
     * aren't going to be restored.  They might participate in dependency
     * chains connecting entries that should be restored, so we treat them as
     * live until we actually process them.
     */
    par_list_header_init(&pending_list);
    par_list_header_init(&ready_list);
    skipped_some = false;
    for (next_work_item = AH->toc->next; next_work_item != AH->toc; next_work_item = next_work_item->next) {
        /* NB: process-or-continue logic must be the inverse of loop above */
        if (next_work_item->section == SECTION_PRE_DATA) {
            /* All PRE_DATA items were dealt with above */
            continue;
        }
        if (next_work_item->section == SECTION_DATA || next_work_item->section == SECTION_POST_DATA) {
            /* set this flag at same point that previous loop did */
            skipped_some = true;
        } else {
            /* SECTION_NONE items must be processed if previous loop didn't */
            if (!skipped_some) {
                continue;
            }
        }

        if (next_work_item->depCount > 0) {
            par_list_append(&pending_list, next_work_item);
        } else {
            par_list_append(&ready_list, next_work_item);
        }
    }

    /*
     * main parent loop
     *
     * Keep going until there is no worker still running AND there is no work
     * left to be done.
     */
    ahlog(AH, 1, "entering main parallel loop\n");

    while ((next_work_item = get_next_work_item(AH, &ready_list, slots, n_slots)) != NULL ||
           work_in_progress(slots, n_slots)) {
        if (next_work_item != NULL) {
            /* If not to be restored, don't waste time launching a worker */
            if ((next_work_item->reqs & (REQ_SCHEMA | REQ_DATA)) == 0 || _tocEntryIsACL(next_work_item)) {
                ahlog(AH,
                    1,
                    "skipping item %d %s %s\n",
                    next_work_item->dumpId,
                    next_work_item->desc,
                    next_work_item->tag);

                par_list_remove(next_work_item);
                reduce_dependencies(AH, next_work_item, &ready_list);

                continue;
            }

            if ((next_slot = get_next_slot(slots, n_slots)) != NO_SLOT) {
                /* There is work still to do and a worker slot available */
                RestoreArgs* args = NULL;

                ahlog(AH,
                    1,
                    "launching item %d %s %s\n",
                    next_work_item->dumpId,
                    next_work_item->desc,
                    next_work_item->tag);

                par_list_remove(next_work_item);

                /* this memory is dealloced in mark_work_done() */
                args = (RestoreArgs*)pg_malloc(sizeof(RestoreArgs));
                args->AH = CloneArchive(AH);
                args->te = next_work_item;
                args->pse = &pstate->pse[next_slot];

                /* run the step in a worker child */
                thandle child = spawn_restore(args);

                sqlCnt++;
                if ((ropt->useDB) && (sqlCnt != 0) && ((sqlCnt % RESTORE_READ_CNT) == 0)) {
                    write_msg(NULL, "%d SQL statements read in !\n", sqlCnt);
                }

                slots[next_slot].child_id = child;
                slots[next_slot].args = args;

                continue;
            }
        }

        /*
         * If we get here there must be work being done.  Either there is no
         * work available to schedule (and work_in_progress returned true) or
         * there are no slots available.  So we wait for a worker to finish,
         * and process the result.
         */
        thandle ret_child = reap_child(slots, n_slots, &work_status);

        work_status_temp = (uint32)work_status;
        if (WIFEXITED(work_status_temp)) {
            work_status = (int)(WEXITSTATUS(work_status_temp));
            mark_work_done(AH, &ready_list, ret_child, work_status, slots, n_slots);
            work_status = (int)work_status_temp;
        } else {
            work_status = (int)work_status_temp;
            exit_horribly(modulename, "worker process crashed: status %d\n", work_status);
        }
    }

    ahlog(AH, 1, "finished main parallel loop\n");

    /* free pstate memory */
    free(pstate->pse);
    pstate->pse = NULL;
    free(pstate);
    pstate = NULL;
    /*
     * Remove the pstate again, so the exit handler will now fall back to
     * closing AH->connection again.
     */
    shutdown_info.pstate = NULL;

    /*
     * Now reconnect the single parent connection.
     */
    (void)ConnectDatabase((Archive*)AH, ropt->dbname, ropt->pghost, ropt->pgport, ropt->username, ropt->promptPassword);

    _doSetFixedOutputState(AH);

    /*
     * Make sure there is no non-ACL work left due to, say, circular
     * dependencies, or some other pathological condition. If so, do it in the
     * single parent connection.
     */
    for (te = pending_list.par_next; te != &pending_list; te = te->par_next) {
        ahlog(AH, 1, "processing missed item %d %s %s\n", te->dumpId, te->desc, te->tag);
        (void)restore_toc_entry(AH, te, ropt, false);

        sqlCnt++;
        if (RestoringToDB(AH) && (sqlCnt != 0) && ((sqlCnt % RESTORE_READ_CNT) == 0)) {
            write_msg(NULL, "%d SQL statements read in !\n", sqlCnt);
        }
    }

    if (RestoringToDB(AH)) {
        write_msg(NULL, "Finish reading %d SQL statements!\n", sqlCnt);
        write_msg(NULL, "end restore operation ...\n");
    }

    /* The ACLs will be handled back in RestoreArchive. */
    /* free memory the slots before retring from it  */
    if (slots != NULL) {
        free(slots);
        slots = NULL;
    }
}

/*
 * create a worker child to perform a restore step in parallel
 */
static thandle spawn_restore(RestoreArgs* args)
{
    /* Ensure stdio state is quiesced before forking */
    (void)fflush(NULL);

#ifndef WIN32
    thandle child = fork();
    if (child == 0) {
        /* in child process */
        parallel_restore(args);
    } else if (child < 0) {
        /* fork failed */
        exit_horribly(modulename, "could not create worker process: %s\n", strerror(errno));
    }
#else
    thandle child = (HANDLE)_beginthreadex(NULL, 0, (unsigned int(__stdcall*)(void*))parallel_restore, args, 0, NULL);
    if (child == 0)
        exit_horribly(modulename, "could not create worker thread: %s\n", strerror(errno));
#endif

    return child;
}

/*
 *	collect status from a completed worker child
 */
static thandle reap_child(ParallelSlot* slots, int n_slots, int* work_status)
{
#ifndef WIN32
    /* Unix is so much easier ... */
    return wait(work_status);
#else
    static HANDLE* handles = NULL;
    int hindex, snum, tnum;
    thandle ret_child;
    DWORD res;

    /* first time around only, make space for handles to listen on */
    if (handles == NULL)
        handles = (HANDLE*)pg_calloc(sizeof(HANDLE), n_slots);

    /* set up list of handles to listen to */
    for (snum = 0, tnum = 0; snum < n_slots; snum++)
        if (slots[snum].child_id != 0)
            handles[tnum++] = slots[snum].child_id;

    /* wait for one to finish */
    hindex = WaitForMultipleObjects(tnum, handles, false, INFINITE);

    /* get handle of finished thread */
    ret_child = handles[hindex - WAIT_OBJECT_0];

    /* get the result */
    GetExitCodeThread(ret_child, &res);
    *work_status = res;

    /* dispose of handle to stop leaks */
    CloseHandle(ret_child);

    return ret_child;
#endif
}

/*
 * are we doing anything now?
 */
static bool work_in_progress(ParallelSlot* slots, int n_slots)
{
    int i;

    for (i = 0; i < n_slots; i++) {
        if (slots[i].child_id != 0)
            return true;
    }
    return false;
}

/*
 * find the first free parallel slot (if any).
 */
static int get_next_slot(ParallelSlot* slots, int n_slots)
{
    int i;

    for (i = 0; i < n_slots; i++) {
        if (slots[i].child_id == 0) {
            return i;
        }
    }
    return NO_SLOT;
}

/*
 * Check if te1 has an exclusive lock requirement for an item that te2 also
 * requires, whether or not te2's requirement is for an exclusive lock.
 */
static bool has_lock_conflicts(TocEntry* te1, TocEntry* te2)
{
    int j, k;

    for (j = 0; j < te1->nLockDeps; j++) {
        for (k = 0; k < te2->nDeps; k++) {
            if (te1->lockDeps[j] == te2->dependencies[k])
                return true;
        }
    }
    return false;
}

/*
 * Initialize the header of a parallel-processing list.
 *
 * These are circular lists with a dummy TocEntry as header, just like the
 * main TOC list; but we use separate list links so that an entry can be in
 * the main TOC list as well as in a parallel-processing list.
 */
static void par_list_header_init(TocEntry* l)
{
    l->par_prev = l->par_next = l;
}

/* Append te to the end of the parallel-processing list headed by l */
static void par_list_append(TocEntry* l, TocEntry* te)
{
    te->par_prev = l->par_prev;
    l->par_prev->par_next = te;
    l->par_prev = te;
    te->par_next = l;
}

/* Remove te from whatever parallel-processing list it's in */
static void par_list_remove(TocEntry* te)
{
    te->par_prev->par_next = te->par_next;
    te->par_next->par_prev = te->par_prev;
    te->par_prev = NULL;
    te->par_next = NULL;
}

/*
 * Find the next work item (if any) that is capable of being run now.
 *
 * To qualify, the item must have no remaining dependencies
 * and no requirements for locks that are incompatible with
 * items currently running.  Items in the ready_list are known to have
 * no remaining dependencies, but we have to check for lock conflicts.
 *
 * Note that the returned item has *not* been removed from ready_list.
 * The caller must do that after successfully dispatching the item.
 *
 * pref_non_data is for an alternative selection algorithm that gives
 * preference to non-data items if there is already a data load running.
 * It is currently disabled.
 */
static TocEntry* get_next_work_item(ArchiveHandle* AH, TocEntry* ready_list, ParallelSlot* slots, int n_slots)
{
    TocEntry* te = NULL;
    int i;

    /*
     * Search the ready_list until we find a suitable item.
     */
    for (te = ready_list->par_next; te != ready_list; te = te->par_next) {
        bool conflicts = false;

        /*
         * Check to see if the item would need exclusive lock on something
         * that a currently running item also needs lock on, or vice versa. If
         * so, we don't want to schedule them together.
         */
        for (i = 0; i < n_slots && !conflicts; i++) {
            TocEntry* running_te = NULL;

            if (slots[i].args == NULL) {
                continue;
            }
            running_te = slots[i].args->te;

            if (has_lock_conflicts(te, running_te) || has_lock_conflicts(running_te, te)) {
                conflicts = true;
                break;
            }
        }

        if (conflicts) {
            continue;
        }

        /* passed all tests, so this item can run */
        return te;
    }

    ahlog(AH, 2, "no item ready\n");
    return NULL;
}

/*
 * Restore a single TOC item in parallel with others
 *
 * this is the procedure run as a thread (Windows) or a
 * separate process (everything else).
 */
static parallel_restore_result parallel_restore(RestoreArgs* args)
{
    ArchiveHandle* AH = args->AH;
    TocEntry* te = args->te;
    RestoreOptions* ropt = AH->ropt;
    int retval;

    setProcessIdentifier(args->pse, AH);

    /*
     * Close and reopen the input file so we have a private file pointer that
     * doesn't stomp on anyone else's file pointer, if we're actually going to
     * need to read from the file. Otherwise, just close it except on Windows,
     * where it will possibly be needed by other threads.
     *
     * Note: on Windows, since we are using threads not processes, the reopen
     * call *doesn't* close the original file pointer but just open a new one.
     */
    if (te->section == SECTION_DATA)
        (AH->Reopenptr)(AH);
#ifndef WIN32
    else
        (AH->Closeptr)(AH);
#endif

    if (NULL != (AH->connection)) {
        PQfinish(AH->connection);
        AH->connection = NULL;
    }

    /*
     * We need our own database connection, too
     */
    (void)ConnectDatabase((Archive*)AH, ropt->dbname, ropt->pghost, ropt->pgport, ropt->username, ropt->promptPassword);

    _doSetFixedOutputState(AH);

    /* Restore the TOC item */
    retval = restore_toc_entry(AH, te, ropt, true);

    /* And clean up */
    DisconnectDatabase((Archive*)AH);
    unsetProcessIdentifier(args->pse);

    /* If we reopened the file, we are done with it, so close it now */
    if (te->section == SECTION_DATA)
        (AH->Closeptr)(AH);

    if (retval == 0 && AH->publicArc.n_errors)
        retval = WORKER_IGNORED_ERRORS;

#ifndef WIN32
    exit(retval);
#else
    return retval;
#endif
}

/*
 * Housekeeping to be done after a step has been parallel restored.
 *
 * Clear the appropriate slot, free all the extra memory we allocated,
 * update status, and reduce the dependency count of any dependent items.
 */
static void mark_work_done(
    ArchiveHandle* AH, TocEntry* ready_list, thandle worker, int status, ParallelSlot* slots, int n_slots)
{
    TocEntry* te = NULL;
    int i;

    for (i = 0; i < n_slots; i++) {
        if (slots[i].child_id == worker) {
            slots[i].child_id = 0;
            te = slots[i].args->te;
            DeCloneArchive(slots[i].args->AH);
            free(slots[i].args);
            slots[i].args = NULL;

            break;
        }
    }

    if (te == NULL)
        exit_horribly(modulename, "could not find slot of finished worker\n");

    ahlog(AH, 1, "finished item %d %s %s\n", te->dumpId, te->desc, te->tag);

    if (status == WORKER_CREATE_DONE) {
        mark_create_done(AH, te);
    } else if (status == WORKER_INHIBIT_DATA) {
        inhibit_data_for_failed_table(AH, te);
        AH->publicArc.n_errors++;
    } else if (status == WORKER_IGNORED_ERRORS) {
        AH->publicArc.n_errors++;
    } else if (status != 0) {
        exit_horribly(modulename, "worker process failed: exit code %d\n", status);
    }
    reduce_dependencies(AH, te, ready_list);
}

/*
 * Process the dependency information into a form useful for parallel restore.
 *
 * This function takes care of fixing up some missing or badly designed
 * dependencies, and then prepares subsidiary data structures that will be
 * used in the main parallel-restore logic, including:
 * 1. We build the revDeps[] arrays of incoming dependency dumpIds.
 * 2. We set up depCount fields that are the number of as-yet-unprocessed
 * dependencies for each TOC entry.
 *
 * We also identify locking dependencies so that we can avoid trying to
 * schedule conflicting items at the same time.
 */
static void fix_dependencies(ArchiveHandle* AH)
{
    TocEntry* te = NULL;
    int i;

    /*
     * Initialize the depCount/revDeps/nRevDeps fields, and make sure the TOC
     * items are marked as not being in any parallel-processing list.
     */
    for (te = AH->toc->next; te != AH->toc; te = te->next) {
        te->depCount = te->nDeps;
        te->revDeps = NULL;
        te->nRevDeps = 0;
        te->par_prev = NULL;
        te->par_next = NULL;
    }

    /*
     * POST_DATA items that are shown as depending on a table need to be
     * re-pointed to depend on that table's data, instead.  This ensures they
     * won't get scheduled until the data has been loaded.
     */
    repoint_table_dependencies(AH);

    /*
     * Pre-8.4 versions of pg_dump neglected to set up a dependency from BLOB
     * COMMENTS to BLOBS.  Cope.  (We assume there's only one BLOBS and only
     * one BLOB COMMENTS in such files.)
     */
    if (AH->version < K_VERS_1_11) {
        for (te = AH->toc->next; te != AH->toc; te = te->next) {
            if (strcmp(te->desc, "BLOB COMMENTS") == 0 && te->nDeps == 0) {
                TocEntry* te2 = NULL;

                for (te2 = AH->toc->next; te2 != AH->toc; te2 = te2->next) {
                    if (strcmp(te2->desc, "BLOBS") == 0) {
                        te->dependencies = (DumpId*)pg_malloc(sizeof(DumpId));
                        te->dependencies[0] = te2->dumpId;
                        te->nDeps++;
                        te->depCount++;
                        break;
                    }
                }
                break;
            }
        }
    }

    /*
     * At this point we start to build the revDeps reverse-dependency arrays,
     * so all changes of dependencies must be complete.
     */
    /*
     * Count the incoming dependencies for each item.  Also, it is possible
     * that the dependencies list items that are not in the archive at all
     * (that should not happen in 9.2 and later, but is highly likely in
     * older archives).  Subtract such items from the depCounts.
     */
    for (te = AH->toc->next; te != AH->toc; te = te->next) {
        for (i = 0; i < te->nDeps; i++) {
            DumpId depid = te->dependencies[i];

            if (depid <= AH->maxDumpId && AH->tocsByDumpId[depid] != NULL)
                AH->tocsByDumpId[depid]->nRevDeps++;
            else
                te->depCount--;
        }
    }

    /*
     * Allocate space for revDeps[] arrays, and reset nRevDeps so we can use
     * it as a counter below.
     */
    for (te = AH->toc->next; te != AH->toc; te = te->next) {
        if (te->nRevDeps > 0)
            te->revDeps = (DumpId*)pg_malloc(te->nRevDeps * sizeof(DumpId));
        te->nRevDeps = 0;
    }

    /*
     * Build the revDeps[] arrays of incoming-dependency dumpIds.  This had
     * better agree with the loops above.
     */
    for (te = AH->toc->next; te != AH->toc; te = te->next) {
        for (i = 0; i < te->nDeps; i++) {
            DumpId depid = te->dependencies[i];

            if (depid <= AH->maxDumpId && AH->tocsByDumpId[depid] != NULL) {
                TocEntry* otherte = AH->tocsByDumpId[depid];

                otherte->revDeps[otherte->nRevDeps++] = te->dumpId;
            }
        }
    }

    /*
     * Lastly, work out the locking dependencies.
     */
    for (te = AH->toc->next; te != AH->toc; te = te->next) {
        te->lockDeps = NULL;
        te->nLockDeps = 0;
        identify_locking_dependencies(AH, te);
    }
}

/*
 * Change dependencies on table items to depend on table data items instead,
 * but only in POST_DATA items.
 */
static void repoint_table_dependencies(ArchiveHandle* AH)
{
    TocEntry* te = NULL;
    int i;
    DumpId olddep;

    for (te = AH->toc->next; te != AH->toc; te = te->next) {
        if (te->section != SECTION_POST_DATA)
            continue;
        for (i = 0; i < te->nDeps; i++) {
            olddep = te->dependencies[i];
            if (olddep <= AH->maxDumpId && AH->tableDataId[olddep] != 0) {
                te->dependencies[i] = AH->tableDataId[olddep];
                ahlog(AH, 2, "transferring dependency %d -> %d to %d\n", te->dumpId, olddep, AH->tableDataId[olddep]);
            }
        }
    }
}

/*
 * Identify which objects we'll need exclusive lock on in order to restore
 * the given TOC entry (*other* than the one identified by the TOC entry
 * itself).  Record their dump IDs in the entry's lockDeps[] array.
 */
static void identify_locking_dependencies(ArchiveHandle* AH, TocEntry* te)
{
    DumpId* lockids = NULL;
    int i = 0;

    /* Quick exit if no dependencies at all */
    if (te->nDeps == 0)
        return;

    /* Exit if this entry doesn't need exclusive lock on other objects */
    if (!(strcmp(te->desc, "CONSTRAINT") == 0 || strcmp(te->desc, "CHECK CONSTRAINT") == 0 ||
            strcmp(te->desc, "FK CONSTRAINT") == 0 || strcmp(te->desc, "RULE") == 0 ||
            strcmp(te->desc, "TRIGGER") == 0))
        return;

    /*
     * We assume the item requires exclusive lock on each TABLE DATA item
     * listed among its dependencies.  (This was originally a dependency on
     * the TABLE, but fix_dependencies repointed it to the data item. Note
     * that all the entry types we are interested in here are POST_DATA, so
     * they will all have been changed this way.)
     */
    lockids = (DumpId*)pg_malloc(te->nDeps * sizeof(DumpId));
    int nlockids = 0;
    for (i = 0; i < te->nDeps; i++) {
        DumpId depid = te->dependencies[i];

        if (depid <= AH->maxDumpId && AH->tocsByDumpId[depid] != NULL &&
            strcmp(AH->tocsByDumpId[depid]->desc, "TABLE DATA") == 0)
            lockids[nlockids++] = depid;
    }

    if (nlockids == 0) {
        free(lockids);
        lockids = NULL;
        return;
    }

    te->lockDeps = (DumpId*)pg_realloc(lockids, nlockids * sizeof(DumpId));
    te->nLockDeps = nlockids;
}

/*
 * Remove the specified TOC entry from the depCounts of items that depend on
 * it, thereby possibly making them ready-to-run.  Any pending item that
 * becomes ready should be moved to the ready list.
 */
static void reduce_dependencies(ArchiveHandle* AH, TocEntry* te, TocEntry* ready_list)
{
    int i;

    ddr_Assert(te != NULL);
    ahlog(AH, 2, "reducing dependencies for %d\n", te->dumpId);

    for (i = 0; i < te->nRevDeps; i++) {
        TocEntry* otherte = AH->tocsByDumpId[te->revDeps[i]];
        if (NULL == otherte) {
            exit_horribly(NULL, "otherte cannot be NULL.\n");
        }

        otherte->depCount--;
        if (otherte->depCount == 0 && otherte->par_prev != NULL) {
            /* It must be in the pending list, so remove it ... */
            par_list_remove(otherte);
            /* ... and add to ready_list */
            if (ready_list != NULL) {
                par_list_append(ready_list, otherte);
            }
        }
    }
}

/*
 * Set the created flag on the DATA member corresponding to the given
 * TABLE member
 */
static void mark_create_done(ArchiveHandle* AH, TocEntry* te)
{
    ddr_Assert(te != NULL);
    if (AH->tableDataId[te->dumpId] != 0) {
        TocEntry* ted = AH->tocsByDumpId[AH->tableDataId[te->dumpId]];

        ted->created = true;
    }
}

/*
 * Mark the DATA member corresponding to the given TABLE member
 * as not wanted
 */
static void inhibit_data_for_failed_table(ArchiveHandle* AH, TocEntry* te)
{
    ddr_Assert(te != NULL);
    ahlog(AH, 1, "table \"%s\" could not be created, will not restore its data\n", te->tag);

    if (AH->tableDataId[te->dumpId] != 0) {
        TocEntry* ted = AH->tocsByDumpId[AH->tableDataId[te->dumpId]];

        ted->reqs = (teReqs)0;
    }
}

/*
 * Clone and de-clone routines used in parallel restoration.
 *
 * Enough of the structure is cloned to ensure that there is no
 * conflict between different threads each with their own clone.
 *
 * These could be public, but no need at present.
 */
static ArchiveHandle* CloneArchive(ArchiveHandle* AH)
{
    ArchiveHandle* pstClone = NULL;

    /* Make a "flat" copy */
    pstClone = (ArchiveHandle*)pg_malloc(sizeof(ArchiveHandle));
    errno_t rc = memcpy_s(pstClone, sizeof(ArchiveHandle), AH, sizeof(ArchiveHandle));
    securec_check_c(rc, "\0", "\0");

    /* Handle format-independent fields */
    rc = memset_s(&(pstClone->sqlparse), sizeof(pstClone->sqlparse), 0, sizeof(pstClone->sqlparse));
    securec_check_c(rc, "\0", "\0");

    /* The clone will have its own connection, so disregard connection state */
    pstClone->connection = NULL;
    pstClone->currUser = NULL;
    pstClone->currSchema = NULL;
    pstClone->currTablespace = NULL;
    pstClone->currWithOids = (bool)-1;
    pstClone->publicArc.remoteVersionStr = NULL;

    /* savedPassword must be local in case we change it while connecting */
    if (NULL != (pstClone->savedPassword))
        pstClone->savedPassword = gs_strdup(pstClone->savedPassword);

    /* clone has its own error count, too */
    pstClone->publicArc.n_errors = 0;

    /*
     * Connect our new clone object to the database: In parallel restore the
     * parent is already disconnected, because we can connect the worker
     * processes independently to the database (no snapshot sync required). In
     * parallel backup we clone the parent's existing connection.
     */
    if (AH->mode == archModeRead) {
        RestoreOptions* ropt = AH->ropt;

        ddr_Assert(AH->connection == NULL);
        /* this also sets clone->connection */
        (void)ConnectDatabase(
            (Archive*)pstClone, ropt->dbname, ropt->pghost, ropt->pgport, ropt->username, ropt->promptPassword);
    } else {
        char* dbname = NULL;
        char* pghost = NULL;
        char* pgport = NULL;
        char* username = NULL;
        const char* encname = NULL;

        ddr_Assert(AH->connection != NULL);
        /*
         * Even though we are technically accessing the parent's database
         * object here, these functions are fine to be called like that
         * because all just return a pointer and do not actually send/receive
         * any data to/from the database.
         */
        dbname = PQdb(AH->connection);
        pghost = PQhost(AH->connection);
        pgport = PQport(AH->connection);
        username = PQuser(AH->connection);
        encname = pg_encoding_to_char(AH->publicArc.encoding);

        /* this also sets clone->connection */
        (void)ConnectDatabase((Archive*)pstClone, dbname, pghost, pgport, username, TRI_NO);

        /*
         * Set the same encoding, whatever we set here is what we got from
         * pg_encoding_to_char(), so we really shouldn't run into an error
         * setting that very same value. Also see the comment in
         * SetupConnection().
         */
        (void)PQsetClientEncoding(pstClone->connection, encname);
    }

    /* Let the format-specific code have a chance too */
    (pstClone->Cloneptr)(pstClone);

    ddr_Assert(pstClone->connection != NULL);
    return pstClone;
}

/*
 * Release clone-local storage.
 *
 * Note: we assume any clone-local connection was already closed.
 */
static void DeCloneArchive(ArchiveHandle* AH)
{
    errno_t rc;
    /* Clear format-specific state */
    (AH->DeCloneptr)(AH);

    /* Clear state allocated by CloneArchive */
    if (AH->sqlparse.curCmd != NULL)
        (void)destroyPQExpBuffer(AH->sqlparse.curCmd);

    /* Clear any connection-local state */
    if (NULL != AH->currUser) {
        free(AH->currUser);
        AH->currUser = NULL;
    }
    if (NULL != AH->currSchema) {
        free(AH->currSchema);
        AH->currSchema = NULL;
    }
    if (NULL != AH->currTablespace) {
        free(AH->currTablespace);
        AH->currTablespace = NULL;
    }
    if (NULL != AH->savedPassword) {
        rc = memset_s(AH->savedPassword, strlen(AH->savedPassword), 0, strlen(AH->savedPassword));
        securec_check_c(rc, "\0", "\0");
        free(AH->savedPassword);
        AH->savedPassword = NULL;
    }
    if (NULL != AH->publicArc.remoteVersionStr) {
        free(AH->publicArc.remoteVersionStr);
        AH->publicArc.remoteVersionStr = NULL;
    }
    if (NULL != AH->connection) {
        PQfinish(AH->connection);
        AH->connection = NULL;
    }

    free(AH);
    AH = NULL;
}

/* Database Security: Data importing/dumping support AES128. */
/* Funcation  : check_encrypt_parameters
 * Description: check the input parameters,encrypt_mode must be AES128 ,currently only AES128 is supported,encrypt_key
 * must be 16Bytes length.
 */
void check_encrypt_parameters(Archive* fout, const char* encrypt_mode, const char* encrypt_key)
{
    if (NULL == encrypt_mode && NULL == encrypt_key) {
        fout->encryptfile = false;
        return;
    }
    if (NULL == encrypt_mode) {
        exit_horribly(NULL, "No encryption method,only AES128 is available\n");
    }
    if (0 != strcmp(encrypt_mode, "AES128")) {
        exit_horribly(NULL, "%s is not supported,only AES128 is available\n", encrypt_mode);
    }
    if (NULL == encrypt_key) {
        exit_horribly(NULL, "No key for encryption,please input the key\n");
    }
    if (!check_input_password(encrypt_key)) {
        exit_horribly(NULL, "The input key must be %d~%d bytes and "
            "contain at least three kinds of characters!\n",
            MIN_KEY_LEN, MAX_KEY_LEN);
    }

    errno_t rc = memset_s(fout->Key, KEY_MAX_LEN, 0, KEY_MAX_LEN);
    securec_check_c(rc, "\0", "\0");
    rc = strncpy_s((char*)fout->Key, KEY_MAX_LEN, encrypt_key, KEY_MAX_LEN - 1);
    securec_check_c(rc, "\0", "\0");
    fout->encryptfile = true;
}

/*
 * encrypt for archive files.
 */
void encryptArchive(Archive* fout, const ArchiveFormat fmt)
{
    ArchiveHandle* AH = (ArchiveHandle*)fout;
    char* tempEncryptDir = NULL;
    char* fileSpec = NULL;
    char pathbuf[MAXPGPATH] = {0};
    char newpathbuf[MAXPGPATH] = {0};
    DIR* dir = NULL;
    struct dirent* de = NULL;
    struct stat st;
    int rd = 0;
    int nRet = 0;
    if (!fout->encryptfile)
        return;

    /* for plain format, encrypted in previous process. */
    if (fmt != archDirectory)
        return;

    fileSpec = gs_strdup(AH->fSpec);
    if (fileSpec[strlen(fileSpec) - 1] == '/' || fileSpec[strlen(fileSpec) - 1] == '\\')
        fileSpec[strlen(fileSpec) - 1] = '\0';
    int fileSpeclen = strlen(fileSpec);
    int tempEncryptDirLen = fileSpeclen + strlen(TEMP_ARCHIVE_HEADER) + 1;
    tempEncryptDir = (char*)pg_malloc(tempEncryptDirLen);
    if (fileSpeclen > 0) {
        rd = strncpy_s(tempEncryptDir, tempEncryptDirLen, fileSpec, fileSpeclen);
        securec_check(rd, "\0", "\0");
        tempEncryptDir[fileSpeclen] = '\0';
    }
    errno_t rc = strncat_s(tempEncryptDir,
        (strlen(fileSpec) + strlen(TEMP_ARCHIVE_HEADER) + 1),
        TEMP_ARCHIVE_HEADER,
        strlen(TEMP_ARCHIVE_HEADER));
    securec_check_c(rc, "\0", "\0");

    if (!checkAndCreateDir(tempEncryptDir)) {
        if (!rmtree(tempEncryptDir, false))
            exit_horribly(
                modulename, "failed to remove temp encrypt dir \"%s\": %s\n", tempEncryptDir, gs_strerror(errno));
    }

    dir = opendir(fileSpec);
    if (NULL == dir)
        exit_horribly(modulename, "could not open directory \"%s\"\n", fileSpec);

    while ((de = gs_readdir(dir)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        nRet = snprintf_s(pathbuf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", fileSpec, de->d_name);
        securec_check_ss_c(nRet, "\0", "\0");
        if (lstat(pathbuf, &st) != 0) {
            if (errno != ENOENT)
                exit_horribly(modulename, "could not stat file or directory \"%s\"\n", pathbuf);
            continue;
        }
        if (S_ISREG(st.st_mode)) {
            nRet = snprintf_s(newpathbuf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", tempEncryptDir, de->d_name);
            securec_check_ss_c(nRet, "\0", "\0");
            encryptSimpleFile(pathbuf, newpathbuf, fout->Key, fout->rand);
        }
    }

    (void)closedir(dir);

    if (!getAESLabelFile(tempEncryptDir, AES_ENCRYPT_LABEL, PG_BINARY_W))
        exit_horribly(modulename, "failed to create aes label file in \"%s\".\n", tempEncryptDir);

    if (!rmtree(fileSpec, true))
        exit_horribly(modulename, "failed to remove dir %s.\n", fileSpec);

    if (rename(tempEncryptDir, fileSpec) < 0)
        exit_horribly(modulename, "failed to rename dir %s.\n", tempEncryptDir);

    free(fileSpec);
    fileSpec = NULL;
    free(tempEncryptDir);
    tempEncryptDir = NULL;
}

/*
 * Encrypt simple file.
 */
void encryptSimpleFile(const char* fileName, const char* encryptedFileName, unsigned char Key[], GS_UCHAR* init_rand)
{
    FILE* fr = NULL;
    FILE* fw = NULL;
    struct stat statbuf;
    char inputstr[MAX_AES_BUFF_LEN + 1] = {0};
    int nread = 0;
    int errCode = 0;
    errno_t rc = 0;

    ddr_Assert(strncmp(fileName, encryptedFileName, strlen(fileName)) != 0);

    /*
     * we first caculate the length which would be filled into the encrypt file
     * by aes encrypt algorithm, then write it to the header of the file. Because
     * it's really default for decrypter to check the actual size without this.
     */
    if (lstat(fileName, &statbuf) != 0) {
        exit_horribly(modulename, "could not stat encrypt archive file \"%s\".\n", fileName);
        return;
    }

    fr = fopen(fileName, PG_BINARY_R);
    if (fr == NULL) {
        exit_horribly(modulename, "could not open encrypt archive file.\n");
        return;
    }

    fw = fopen(encryptedFileName, PG_BINARY_W);
    if (fw == NULL) {
        fclose(fr);
        exit_horribly(modulename, "could not open encrypt archive file.\n");
        return;
    }

    fprintf(fw, "%s", init_rand);

    while (!feof(fr)) {
        nread = fread(inputstr, 1, MAX_AES_BUFF_LEN, fr);
        errCode = ferror(fr);
        if (errCode) {
            (void)memset_s(inputstr, MAX_AES_BUFF_LEN + 1, '\0', MAX_AES_BUFF_LEN + 1);
            fclose(fr);
            fclose(fw);
            exit_horribly(modulename, "could not read archive file. errCode: %d readSize: %d\n", errCode, nread);
        }
        if (!nread)
            break;

        if (!writeFileAfterEncryption(fw, inputstr, nread, MAX_DECRYPT_BUFF_LEN, Key, init_rand)) {
            (void)memset_s(inputstr, MAX_AES_BUFF_LEN + 1, '\0', MAX_AES_BUFF_LEN + 1);
            fclose(fr);
            fclose(fw);
            exit_horribly(modulename, "Encryption failed.");
        }
        rc = memset_s(inputstr, MAX_AES_BUFF_LEN + 1, '\0', MAX_AES_BUFF_LEN + 1);
        securec_check_c(rc, "\0", "\0");
    }

    /*
     * fsync the encryptedFileName file immediately, in case of an unfortunate system crash.
     */
    if (fsync(fileno(fw)) != 0) {
        fclose(fr);
        fclose(fw);
        exit_horribly(modulename, "Failed to fsync file %s.\n", encryptedFileName);
    }

    fclose(fr);
    fclose(fw);
}

/*
 * Decrypt for archive files.
 */
void decryptArchive(Archive* fout, const ArchiveFormat fmt)
{
    ArchiveHandle* AH = (ArchiveHandle*)fout;
    char* tempDecryptDir = NULL;
    char* fileSpec = NULL;
    char pathbuf[MAXPGPATH] = {0};
    char newpathbuf[MAXPGPATH] = {0};
    DIR* dir = NULL;
    struct dirent* de = NULL;
    struct stat st;
    int rd = 0;
    int nRet = 0;

    fileSpec = gs_strdup(AH->fSpec);
    if (fileSpec[strlen(fileSpec) - 1] == '/' || fileSpec[strlen(fileSpec) - 1] == '\\')
        fileSpec[strlen(fileSpec) - 1] = '\0';

    if (!(archUnknown == fmt || archDirectory == fmt) || !fout->encryptfile ||
        !getAESLabelFile(fileSpec, AES_ENCRYPT_LABEL, PG_BINARY_R)) {
        free(fileSpec);
        fileSpec = NULL;
        return;
    }
    int fileSpeclen = strlen(fileSpec);
    int tempDecryptDirLen = fileSpeclen + strlen(TEMP_ARCHIVE_HEADER) + 1;
    tempDecryptDir = (char*)pg_malloc(tempDecryptDirLen);
    if (fileSpeclen > 0) {
        rd = strncpy_s(tempDecryptDir, tempDecryptDirLen, fileSpec, fileSpeclen);
        securec_check(rd, "\0", "\0");
        tempDecryptDir[fileSpeclen] = '\0';
    }
    errno_t rc = strncat_s(tempDecryptDir,
        (strlen(fileSpec) + strlen(TEMP_ARCHIVE_HEADER) + 1),
        TEMP_ARCHIVE_HEADER,
        strlen(TEMP_ARCHIVE_HEADER));
    securec_check_c(rc, "\0", "\0");

    if (!checkAndCreateDir(tempDecryptDir)) {
        if (!rmtree(tempDecryptDir, false))
            exit_horribly(
                modulename, "failed to remove temp encrypt dir \"%s\": %s\n", tempDecryptDir, gs_strerror(errno));
    }

    dir = opendir(fileSpec);
    if (NULL == dir)
        exit_horribly(modulename, "could not open directory \"%s\"\n", fileSpec);

    while ((de = gs_readdir(dir)) != NULL) {
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        if (strcmp(de->d_name, AES_ENCRYPT_LABEL) == 0)
            continue;

        nRet = snprintf_s(pathbuf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", fileSpec, de->d_name);
        securec_check_ss_c(nRet, "\0", "\0");
        if (lstat(pathbuf, &st) != 0) {
            if (errno != ENOENT)
                exit_horribly(modulename, "could not stat file or directory \"%s\"\n", pathbuf);
            continue;
        }
        if (S_ISREG(st.st_mode)) {
            nRet = snprintf_s(newpathbuf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", tempDecryptDir, de->d_name);
            securec_check_ss_c(nRet, "\0", "\0");
            decryptSimpleFile(pathbuf, newpathbuf, fout->Key);
        }
    }

    (void)closedir(dir);

    if (!getAESLabelFile(tempDecryptDir, AES_TEMP_DECRYPT_LABEL, PG_BINARY_W))
        exit_horribly(modulename,
            "failed to create aes temp decrypt label file in \"%s\": %s\n",
            tempDecryptDir,
            gs_strerror(errno));

    free(AH->fSpec);
    AH->fSpec = gs_strdup(tempDecryptDir);
    free(fileSpec);
    fileSpec = NULL;
    free(tempDecryptDir);
    tempDecryptDir = NULL;
}

static bool decryptFromFile(
     FILE* source, unsigned char* Key, GS_UCHAR** decryptBuff, int* plainlen, bool* randget, GS_UINT32* decryptBufflen)
{
    int nread = 0;
    int errnum;
    GS_UINT32 cipherlen = 0;
    GS_UCHAR cipherleninfo[RANDOM_LEN + 1] = {0};
    char* endptr = NULL;
    GS_UCHAR* ciphertext = NULL;
    GS_UCHAR* outputstr = NULL;
    bool decryptstatus = false;
    bool isCurrLineProcess = false;
    static unsigned char aucRand[RANDOM_LEN + 1] = {0};
    errno_t rc = 0;

    if (!feof(source) && (false == isCurrLineProcess)) {
        nread = (int)fread((void*)cipherleninfo, 1, RANDOM_LEN, source);
        errnum = ferror(source);
        if (errnum) {
            printf("could not read from input file: %s\n", strerror(errnum));
            return false;
        }
        if (!nread) {
            return true;
        }

        /* get the rand value from encryptfile first */
        if (!(*randget)) {
            rc = memcpy_s(aucRand, RANDOM_LEN, cipherleninfo, RANDOM_LEN);
            securec_check_c(rc, "\0", "\0");
            *randget = true;

            /* get the cipher length for decrypt */
            nread = (int)fread((void*)cipherleninfo, 1, RANDOM_LEN, source);
            if (!nread) {
                return false;
            }
        }

        cipherlen = (GS_UINT32)strtol((char*)cipherleninfo, &endptr, 10);
        if (endptr == (char*)cipherleninfo || cipherlen == 0 ) {
            write_msg(modulename, "WARNING: line ignored: %s, cipher length is %u\n", (char*)cipherleninfo, cipherlen);
            return false;
        }

        /* ciphertext contains the real cipher and salt vector used to encrypt/decrypt */
        ciphertext = (GS_UCHAR*)malloc((size_t)cipherlen);
        if (NULL == ciphertext) {
            printf("memery alloc failed!\n");
            return false;
        }
        outputstr = (GS_UCHAR*)malloc((size_t)cipherlen);
        if (NULL == outputstr) {
            printf("memery alloc failed!\n");
            free(ciphertext);
            ciphertext = NULL;
            return false;
        }
        *decryptBufflen = cipherlen;

        rc = memset_s(ciphertext, cipherlen, '\0', cipherlen);
        securec_check_c(rc, "\0", "\0");
        rc = memset_s(outputstr, cipherlen, '\0', cipherlen);
        securec_check_c(rc, "\0", "\0");

        /* read ciphertext from encrypt file, which contains the cipher part and salt part. */
        nread = (int)fread((void*)ciphertext, cipherlen, 1, source);
        if (!nread) {
            memset_s(ciphertext, cipherlen, '\0', cipherlen);
            free(ciphertext);
            ciphertext = NULL;
            free(outputstr);
            outputstr = NULL;
            return false;
        }

        /* the real decrypt operation */
        decryptstatus = aes128Decrypt(
            ciphertext, cipherlen, (GS_UCHAR*)Key, strlen((const char*)Key), aucRand, outputstr, (GS_UINT32*)plainlen);
        if (!decryptstatus) {
            memset_s(ciphertext, cipherlen, '\0', cipherlen);
            free(ciphertext);
            ciphertext = NULL;
            free(outputstr);
            outputstr = NULL;
            return false;
        }

        *decryptBuff = outputstr;
        memset_s(ciphertext, cipherlen, '\0', cipherlen);
        free(ciphertext);
        ciphertext = NULL;
    } else if (feof(source) && (false == isCurrLineProcess)) {
        return false;
    }

    return true;
}
/*
 * Decrypt simple file.
 */
void decryptSimpleFile(const char* fileName, const char* decryptedFileName, unsigned char Key[])
{
    FILE* fr = NULL;
    FILE* fw = NULL;
    GS_UCHAR* outputptr = NULL;
    int nwrite = 0;
    int plainlen = 0;
    bool randget = false;
    int fd = 0;
    GS_UINT32 decryptBufflen = 0;
    errno_t rc = EOK;

    ddr_Assert(strncmp(fileName, decryptedFileName, strlen(fileName)) != 0);

    fr = fopen(fileName, PG_BINARY_R);
    if (NULL == fr) {
        exit_horribly(modulename, "could not open encrypt archive file.\n");
        return;
    }
    fd = fileno(fr);
    fchmod(fd, 0600);

    fw = fopen(decryptedFileName, PG_BINARY_W);
    if (fw == NULL) {
        fclose(fr);
        exit_horribly(modulename, "could not open encrypt archive file.\n");
        return;
    }

    while (!feof(fr)) {
        if (!decryptFromFile(fr, Key, &outputptr, &plainlen, &randget, &decryptBufflen)) {
            fclose(fr);
            fclose(fw);
            exit_horribly(modulename, "Decryption failed.\n");
        }

        if (NULL != outputptr && decryptBufflen != 0) {
            nwrite = fwrite(outputptr, 1, plainlen, fw);
            if (nwrite != plainlen) {
                fclose(fr);
                fclose(fw);
                exit_horribly(modulename, "could not write archive file.\n");
            }
            rc = memset_s(outputptr, decryptBufflen, '\0', decryptBufflen);
            securec_check_c(rc, "\0", "\0");
            free(outputptr);
            outputptr = NULL;
        }
    }

    fclose(fr);
    fclose(fw);
}

/*
 * get the encryption or temp decryption label file.
 */
bool getAESLabelFile(const char* dirName, const char* labelName, const char* fMode)
{
    char aesLabelFile[MAXPGPATH] = {0};
    FILE* fp = NULL;
    int fd = 0;

    if (NULL == dirName) {
        return false;
    }

    int nRet = snprintf_s(aesLabelFile, MAXPGPATH, MAXPGPATH - 1, "%s/%s", dirName, labelName);
    securec_check_ss_c(nRet, "\0", "\0");

    fp = fopen(aesLabelFile, fMode);
    if (NULL == fp) {
        return false;
    }
    fd = fileno(fp);
    fchmod(fd, 0600);

    fclose(fp);

    return true;
}

bool checkAndCreateDir(const char* dirName)
{
    struct stat st;
    bool is_empty = false;

    if (NULL == dirName) {
        return false;
    }

    /* we accept an empty existing directory */
    if (stat(dirName, &st) == 0 && S_ISDIR(st.st_mode)) {
        DIR* dir = opendir(dirName);

        if (NULL != dir) {
            struct dirent* d;

            is_empty = true;
            while ((d = gs_readdir(dir)) != NULL) {
                if (strcmp(d->d_name, ".") != 0 && strcmp(d->d_name, "..") != 0) {
                    is_empty = false;
                    break;
                }
            }
            (void)closedir(dir);
        }
    }

    if (!is_empty && mkdir(dirName, 0700) < 0)
        return false;

    return true;
}

bool CheckIfStandby(struct Archive *fout)
{
    bool isStandby = false;
    const char *query = "select local_role from pg_stat_get_stream_replications()";
    PGresult* res = NULL;
    int ntups;

    res = ExecuteSqlQuery(fout, query, PGRES_TUPLES_OK);

    ntups = PQntuples(res);
    if (ntups != 1)
        exit_horribly(NULL,
            ngettext(
                "query returned %d row instead of one: %s\n", "query returned %d rows instead of one: %s\n", ntups),
            ntups,
            query);

    isStandby = strstr(PQgetvalue(res, 0, 0), "Standby") != NULL;
    PQclear(res);
    
    return isStandby;
}

static void get_role_password(RestoreOptions* opts) {
    GS_FREE(opts->rolepassword);
    opts->rolepassword = simple_prompt("Role Password: ", 100, false);
    if (opts->rolepassword == NULL) {
        exit_horribly(NULL, "out of memory\n");
    }
}
