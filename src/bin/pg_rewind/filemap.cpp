/* -------------------------------------------------------------------------
 *
 * filemap.c
 *	  A data structure for keeping track of files that have changed.
 *
 * Copyright (c) 2013-2015, PostgreSQL Global Development Group
 *
 * -------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include "datapagemap.h"
#include "filemap.h"
#include "logging.h"
#include "pg_rewind.h"
#include "file_ops.h"
#include "fetch.h"
#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"
#include "common/fe_memutils.h"
#include "storage/cu.h"
#include "storage/fd.h"

#define REWIND_LABLE_FILE "rewind_lable"
#define BLOCKSIZE (8 * 1024)
#define BUILD_PATH_LEN 2560 /* (MAXPGPATH*2 + 512) */
#define MAX_UINT32_LEN 10
const int FILE_NAME_MAX_LEN = 1024;
const int MATCH_ONE = 1;
const int MATCH_TWO = 2;
const int MATCH_THREE = 3;
const int MATCH_FOUR = 4;
const int MATCH_FIVE = 5;
const int MATCH_SIX = 6;

filemap_t* filemap = NULL;
filemap_t* filemaptarget = NULL;

static const char* forkNames_t[] = {
    "main", /* MAIN_FORKNUM */
    "fsm",  /* FSM_FORKNUM */
    "vm",   /* VISIBILITYMAP_FORKNUM */
    "bcm",  /* BCM_FORKNUM */
    "init"  /* INIT_FORKNUM */
};

extern char pgxcnodename[MAX_VALUE_LEN];

/*
 * The max size for single data file. copy from custorage.cpp.
 */
const uint64 MAX_FILE_SIZE = (uint64)RELSEG_SIZE * BLCKSZ;

static char* datasegpath(RelFileNode rnode, ForkNumber forknum, BlockNumber segno);
static int path_cmp(const void* a, const void* b);
static int final_filemap_cmp(const void* a, const void* b);
static void filemap_list_to_array(filemap_t* map);
static char* relpathbackend_t(RelFileNode rnode, BackendId backend, ForkNumber forknum);
static bool check_abs_tblspac_path(const char *fname, unsigned int *segNo, RelFileNode *rnode);
static bool check_base_path(const char *fname, unsigned int *segNo, RelFileNode *rnode);
static bool check_rel_tblspac_path(const char *fname, unsigned int *segNo, RelFileNode *rnode);
extern char divergeXlogFileName[MAXFNAMELEN];
extern char lastxlogfileName[MAXFNAMELEN];
static pthread_t targetfilestatpid;

/*
 * Create a new file map (stored in the global pointer "filemap").
 */
filemap_t* filemap_create(void)
{
    filemap_t* map = NULL;

    map = (filemap_t*)pg_malloc(sizeof(filemap_t));
    map->first = map->last = NULL;
    map->nlist = 0;
    map->array = NULL;
    map->narray = 0;
    return map;
}

void filemapInit(void)
{
    Assert(filemap == NULL);
    Assert(filemaptarget == NULL);
    filemap = filemap_create();
    filemaptarget = filemap_create();
}

void processTargetFileMap(const char* path, file_type_t type, size_t oldsize, const char* link_target)
{
    file_entry_t* entry = NULL;
    filemap_t* map = filemaptarget;

    /* Create a new entry for this file */
    entry = (file_entry_t*)pg_malloc(sizeof(file_entry_t));
    entry->path = pg_strdup(path);
    entry->type = type;
    entry->oldsize = oldsize;
    check_env_value_c(link_target);
    entry->link_target = link_target != NULL ? pg_strdup(link_target) : NULL;
    entry->next = NULL;
    entry->pagemap.bitmap = NULL;
    entry->pagemap.bitmapsize = 0;

    if (map->last != NULL) {
        map->last->next = entry;
        map->last = entry;
    } else {
        map->first = map->last = entry;
    }
    map->nlist++;
}

void* createTargetFilemap(void* data)
{
    filemap_t* map = filemaptarget;
    traverse_datadir(datadir_target, &processTargetFileMap);
    if (increment_return_code != BUILD_SUCCESS) {
        pthread_exit((void*)increment_return_code);
    }

    filemap_list_to_array(map);
    qsort(map->array, map->narray, sizeof(file_entry_t*), path_cmp);
    pthread_exit(NULL);
}

BuildErrorCode targetFileStatThread(void)
{
    if (pthread_create(&targetfilestatpid, NULL, createTargetFilemap, NULL) == 0) {
        pg_log(PG_PROGRESS, "targetFileStatThread success pid %lu.\n", targetfilestatpid);
        return BUILD_SUCCESS;
    }
    return BUILD_ERROR;
}

BuildErrorCode waitEndTargetFileStatThread(void)
{
    void* ret = NULL;
    pthread_join(targetfilestatpid, &ret);
    if (ret != NULL) {
        pg_log(PG_ERROR, "waitEndTargetFileStatThread not return success.\n");
        return BUILD_ERROR;
    }
    pg_log(PG_PROGRESS, "targetFileStatThread return success.\n");
    return BUILD_SUCCESS;
}

void filemapPrint(filemap_t* map)
{
    file_entry_t* entry = NULL;
    int i;

    for (i = 0; i < map->narray; i++) {
        entry = map->array[i];
        printf("path %s,type %d,size %lu,link %s\n",
            entry->path,
            entry->type,
            entry->oldsize,
            (entry->link_target != NULL) ? entry->link_target : "NULL");
    }
}

BuildErrorCode targetFilemapProcess(void)
{
    file_entry_t* entry = NULL;
    int i;
    filemap_t* map = filemaptarget;
    for (i = 0; i < map->narray; i++) {
        entry = map->array[i];
        process_target_file(entry->path, entry->type, entry->oldsize, entry->link_target);
    }
    return BUILD_SUCCESS;
}

int targetFilemapSearch(const char* path, file_entry_t* entry)
{
    filemap_t* map = filemaptarget;
    file_entry_t key;
    file_entry_t* key_ptr = NULL;
    file_entry_t** e;

    if (path == NULL || entry == NULL) {
        pg_log(PG_ERROR, "invalid input path=%s.\n", path);
        return -1;
    }
    key.path = (char*)path;
    key_ptr = &key;
    e = (file_entry_t**)bsearch(&key_ptr, map->array, map->narray, sizeof(file_entry_t*), path_cmp);
    if (e == NULL) {
        pg_log(PG_DEBUG, "path %s is not bsearch in current map\n", path);
        return -1;
    }
    if (*e == NULL) {
        pg_log(PG_DEBUG, "path %s is not valid in current map\n", path);
        return -1;
    }
    entry->path = (*e)->path;
    entry->type = (*e)->type;
    entry->oldsize = (*e)->oldsize;
    entry->link_target = (*e)->link_target;

    pg_log(PG_DEBUG,
        "path %s,type %d,size %lu,link %s\n",
        entry->path,
        entry->type,
        entry->oldsize,
        (entry->link_target != NULL) ? entry->link_target : "NULL");

    return 0;
}

/*
 * Check if is valid tablespace
 *
 * Consider that all the tablespace in directory 'pg_tblspc' must symbol link,
 * so the path is not valid if the type of the path getting from primary if not symbol link.
 */
bool check_valid_tablspace(const char* path, file_type_t type)
{
    char buf[1024];
    RelFileNode rnode;
    unsigned int segNo;
    int columnid;
    int nmatch = 0;
    nmatch = sscanf_s(path,
        "pg_tblspc/%u/%[^/]/%u/%u_C%d.%u",
        &rnode.spcNode,
        buf,
        sizeof(buf),
        &rnode.dbNode,
        &rnode.relNode,
        &columnid,
        &segNo);
    /* only check the oid */
    if (nmatch == 1) {
        if (type != FILE_TYPE_SYMLINK) {
            return false;
        }
    }
    return true;
}

/*
 * Callback for processing source file list.
 *
 * This is called once for every file in the source server. We decide what
 * action needs to be taken for the file, depending on whether the file
 * exists in the target and whether the size matches.
 */
void process_source_file(const char* path, file_type_t type, size_t newsize, const char* link_target)
{
    bool exists = false;
    char localpath[MAXPGPATH];
    file_entry_t statbuf;
    filemap_t* map = filemap;
    file_action_t action = FILE_ACTION_NONE;
    size_t oldsize = 0;
    file_entry_t* entry = NULL;
    int ss_c = 0;
    bool isreldatafile = false;
    Assert(map->array == NULL);

    /*
     * Completely ignore some special files in source and destination.
     */
    if (strcmp(path, "postmaster.pid") == 0 || strcmp(path, "postmaster.opts") == 0)
        return;

    /* skip configure file and other state files */
    if (strncmp(path, "postgresql.conf", 15) == 0 || strncmp(path, "postgresql.conf.lock", 20) == 0 ||
        strncmp(path, "postgresql.conf.bak", 19) == 0 || strncmp(path, "pg_ctl.lock", 11) == 0 ||
        strncmp(path, "build_completed.start", 21) == 0 || strncmp(path, "backup_label", 12) == 0 ||
        strncmp(path, "client.crt", 10) == 0 || strncmp(path, "client.key", 10) == 0 ||
        strncmp(path, "pg_replslot", 11) == 0 || strncmp(path, "gs_build.pid", 12) == 0 ||
        strncmp(path, "gaussdb.state", 13) == 0 || strncmp(path, "pg_errorinfo", 12) == 0 ||
        strncmp(path, "pg_location", 11) == 0 || strncmp(path, "config_exec_params", 18) == 0 ||
        strncmp(path, "delay_xlog_recycle", 18) == 0 || strncmp(path, "delay_ddl_recycle", 17) == 0 ||
        strstr(path, "pg_rewind_bak") != NULL || strstr(path, "build_completed.done") != NULL ||
        strncmp(path, "barrier_lsn", strlen("barrier_lsn")) == 0 || strncmp(path, "pg_dw", strlen("pg_dw")) == 0 ||
        strncmp(path, "pg_dw.build", strlen("pg_dw.build")) == 0)
        return;

    /* Pretend that pg_xlog is a directory, even if it's really a symlink.
     * We don't want to mess with the symlink itself, nor complain if it's a
     * symlink in source but not in target or vice versa.
     */
    if (strcmp(path, "pg_xlog") == 0 && type == FILE_TYPE_SYMLINK)
        type = FILE_TYPE_DIRECTORY;

    /*
     * Skip temporary files, .../pgsql_tmp/... and .../pgsql_tmp.* in source.
     * This has the effect that all temporary files in the destination will be
     * removed.
     */
    if (strstr(path, "/" PG_TEMP_FILE_PREFIX) != NULL)
        return;
    if (strstr(path, "/" PG_TEMP_FILES_DIR "/") != NULL)
        return;

    pg_log(PG_DEBUG, "process_source_file:path=%s.\n", path);

    /*
     * Skip other datanode tblspc
     */
    if (strstr(path, TABLESPACE_VERSION_DIRECTORY) != NULL && strstr(path, pgxcnodename) == NULL)
        return;
    /*
     * Skip invalid tblspc oid
     */
    if (strstr(path, "pg_tblspc/") != NULL && check_valid_tablspace(path, type) == false) {
        pg_log(PG_PROGRESS, "invalid tablespace in primary:path=%s.\n", path);
        return;
    }

    /* skip mot checkpoint files */
    if (strstr(path, "mot.ctrl") != NULL || strstr(path, "chkpt_") != NULL) {
        return;
    }

    /*
     * sanity check: a filename that looks like a data file better be a
     * regular file
     */
    isreldatafile = isRelDataFile(path);
    if (type != FILE_TYPE_REGULAR && isreldatafile) {
        pg_fatal("data file \"%s\" in source is not a regular file\n", path);
        return;
    }

    ss_c = snprintf_s(localpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir_target, path);
    securec_check_ss_c(ss_c, "\0", "\0");

    /* Does the corresponding file exist in the target data filemap? */
    if (targetFilemapSearch(path, &statbuf) < 0) {
        exists = false;
    } else {
        exists = true;
    }

    switch (type) {
        case FILE_TYPE_DIRECTORY:
            if (exists && !PG_F_ISDIR(statbuf.type) && strcmp(path, "pg_xlog") != 0 &&
                strncmp(path, "pg_tblspc", strlen("pg_tblspc")) != 0) {
                /* it's a directory in source, but not in target. Strange.. */
                pg_fatal("\"%s\" is not a directory\n", localpath);
                return;
            }

            if (!exists)
                action = FILE_ACTION_CREATE;
            else
                action = FILE_ACTION_NONE;
            oldsize = 0;
            break;

        case FILE_TYPE_SYMLINK:
            if (exists &&
#ifndef WIN32
                !PG_F_ISLNK(statbuf.type)
#else
                !pgwin32_is_junction(localpath)
#endif
            ) {
                /*
                 * It's a symbolic link in source, but not in target.
                 * Strange..
                 */
                pg_fatal("\"%s\" is not a symbolic link\n", localpath);
                return;
            }

            if (!exists)
                action = FILE_ACTION_CREATE;
            else
                action = FILE_ACTION_NONE;
            oldsize = 0;
            break;

        case FILE_TYPE_REGULAR:
            if (exists && !PG_F_ISREG(statbuf.type)) {
                pg_fatal("\"%s\" is not a regular file\n", localpath);
                return;
            }

            if (!exists || !isRelDataFile(path)) {
                /*
                 * File exists in source, but not in target. Or it's a
                 * non-data file that we have no special processing for. Copy
                 * it in toto.
                 *
                 */
                action = FILE_ACTION_COPY;
                oldsize = 0;
                /*
                 * Don't copy nonuse xlog
                 */
                if (NULL != strstr(path, "pg_xlog")) {
                    if (isOldXlog(path, lastxlogfileName)) {
                        action = FILE_ACTION_NONE;
                        oldsize = 0;
                    }
                }
            } else {
                /*
                 * It's a data file that exists in both.
                 *
                 * If it's larger in target, we can truncate it. There will
                 * also be a WAL record of the truncation in the source
                 * system, so WAL replay would eventually truncate the target
                 * too, but we might as well do it now.
                 *
                 * If it's smaller in the target, it means that it has been
                 * truncated in the target, or enlarged in the source, or
                 * both. If it was truncated in the target, we need to copy
                 * the missing tail from the source system. If it was enlarged
                 * in the source system, there will be WAL records in the
                 * source system for the new blocks, so we wouldn't need to
                 * copy them here. But we don't know which scenario we're
                 * dealing with, and there's no harm in copying the missing
                 * blocks now, so do it now.
                 *
                 * If it's the same size, do nothing here. Any blocks modified
                 * in the target will be copied based on parsing the target
                 * system's WAL, and any blocks modified in the source will be
                 * updated after rewinding, when the source system's WAL is
                 * replayed.
                 */
                /* mod blocksize 8k to avoid half page write */
                oldsize = statbuf.oldsize - (statbuf.oldsize % BLOCKSIZE);
                if (oldsize < newsize)
                    action = FILE_ACTION_COPY_TAIL;
                else if (oldsize > newsize)
                    action = FILE_ACTION_TRUNCATE;
                else
                    action = FILE_ACTION_NONE;
            }
            break;
        default:
            break;
    }

    /* Create a new entry for this file */
    entry = (file_entry_t*)pg_malloc(sizeof(file_entry_t));
    entry->path = pg_strdup(path);
    entry->type = type;
    entry->action = action;
    entry->oldsize = oldsize;
    entry->newsize = newsize;
    check_env_value_c(link_target);
    entry->link_target = link_target != NULL ? pg_strdup(link_target) : NULL;
    entry->next = NULL;
    entry->pagemap.bitmap = NULL;
    entry->pagemap.bitmapsize = 0;
    entry->isrelfile = isreldatafile;

    if (map->last != NULL) {
        map->last->next = entry;
        map->last = entry;
    } else
        map->first = map->last = entry;
    map->nlist++;
}

/*
 * Callback for processing target file list.
 *
 * All source files must be already processed before calling this. This only
 * marks target data directory's files that didn't exist in the source for
 * deletion.
 */
void process_target_file(const char* path, file_type_t type, size_t oldsize, const char* link_target)
{
    bool exists = false;
    file_entry_t key;
    file_entry_t* key_ptr = NULL;
    filemap_t* map = filemap;
    file_entry_t* entry = NULL;
    bool reserved = false;

    if (map->array == NULL) {
        /* on first call, initialize lookup array */
        if (map->nlist == 0) {
            /* should not happen */
            pg_fatal("source file list is empty\n");
            return;
        }

        filemap_list_to_array(map);

        Assert(map->array != NULL);

        qsort(map->array, map->narray, sizeof(file_entry_t*), path_cmp);
    }

    /*
     * Completely ignore some special files
     */
    if (strcmp(path, "postmaster.pid") == 0 || strcmp(path, "postmaster.opts") == 0)
        return;

    /* skip configure file and other state files */
    if (strncmp(path, "postgresql.conf", 15) == 0 || strncmp(path, "postgresql.conf.lock", 20) == 0 ||
        strncmp(path, "pg_ctl.lock", 11) == 0 || strncmp(path, "build_completed.start", 21) == 0 ||
        strncmp(path, "backup_label", 12) == 0 || strncmp(path, "client.crt", 10) == 0 ||
        strncmp(path, "client.key", 10) == 0 || strncmp(path, "gs_build.pid", 12) == 0 ||
        strncmp(path, "pg_replslot", 11) == 0 || strncmp(path, "gaussdb.state", 13) == 0 ||
        strncmp(path, "pg_errorinfo", 12) == 0 || strncmp(path, "pg_location", 11) == 0 ||
        strncmp(path, "config_exec_params", 18) == 0 || strncmp(path, "delay_xlog_recycle", 18) == 0 ||
        strncmp(path, "delay_ddl_recycle", 17) == 0 ||
        strncmp(path, REWIND_LABLE_FILE, strlen(REWIND_LABLE_FILE)) == 0 ||
        strncmp(path, "barrier_lsn", strlen("barrier_lsn")) == 0 || strncmp(path, "pg_dw", strlen("pg_dw")) == 0 ||
        strncmp(path, "pg_dw.build", strlen("pg_dw.build")) == 0)
        return;

    pg_log(PG_DEBUG, "process_target_file:path=%s.\n", path);

    /*
     * Skip other datanode tblspc
     */
    if (strstr(path, TABLESPACE_VERSION_DIRECTORY) != NULL && strstr(path, pgxcnodename) == NULL)
        return;

    /*
     * Like in process_source_file, pretend that xlog is always a  directory.
     */
    if (strcmp(path, "pg_xlog") == 0 && type == FILE_TYPE_SYMLINK)
        type = FILE_TYPE_DIRECTORY;

    key.path = (char*)path;
    key_ptr = &key;
    exists = (bsearch(&key_ptr, map->array, map->narray, sizeof(file_entry_t*), path_cmp) != NULL);

    /* Remove any file or folder that doesn't exist in the source system. */
    if (!exists) {
        /*
         * Don't delete old xlog, maybe remote server has been checkpointed and deleted.
         */
        if (strncmp(path, "pg_xlog/", strlen("pg_xlog/")) == 0) {
            if (isOldXlog(path, divergeXlogFileName)) {
                reserved = true;
            }
        }

        entry = (file_entry_t*)pg_malloc(sizeof(file_entry_t));
        entry->path = pg_strdup(path);
        entry->type = type;
        if (!reserved) {
            entry->action = FILE_ACTION_REMOVE;
        } else {
            entry->action = FILE_ACTION_NONE;
        }

        entry->oldsize = oldsize;
        entry->newsize = 0;
        check_env_value_c(link_target);
        entry->link_target = link_target != NULL ? pg_strdup(link_target) : NULL;
        entry->next = NULL;
        entry->pagemap.bitmap = NULL;
        entry->pagemap.bitmapsize = 0;
        entry->isrelfile = isRelDataFile(path);

        if (map->last == NULL)
            map->first = entry;
        else
            map->last->next = entry;
        map->last = entry;
        map->nlist++;
    } else {
        /*
         * We already handled all files that exist in the source system in
         * process_source_file.
         */
    }
}

/*
 * This callback gets called while we read the WAL in the target, for every
 * block that have changed in the target system. It makes note of all the
 * changed blocks in the pagemap of the file.
 */
void process_block_change(ForkNumber forknum, RelFileNode rnode, BlockNumber blkno)
{
    char* path = NULL;
    file_entry_t key;
    file_entry_t* key_ptr = NULL;
    file_entry_t* entry = NULL;
    BlockNumber blkno_inseg;
    int segno;
    filemap_t* map = filemap;
    file_entry_t** e;
    bool processed = false;

    Assert(map->array);

    segno = blkno / RELSEG_SIZE;
    blkno_inseg = blkno % RELSEG_SIZE;

    path = datasegpath(rnode, forknum, segno);

    key.path = (char*)path;
    key_ptr = &key;

    e = (file_entry_t**)bsearch(&key_ptr, map->array, map->narray, sizeof(file_entry_t*), path_cmp);
    if (e != NULL)
        entry = *e;
    else
        entry = NULL;
    pg_free(path);

    if (entry != NULL) {
        Assert(entry->isrelfile);

        switch (entry->action) {
            case FILE_ACTION_NONE:
            case FILE_ACTION_TRUNCATE:
                /* skip if we're truncating away the modified block anyway */
                if ((blkno_inseg + 1) * BLCKSZ <= entry->newsize) {
                    datapagemap_add(&entry->pagemap, blkno_inseg);
                    processed = true;
                }
                break;

            case FILE_ACTION_COPY_TAIL:
                /*
                 * skip the modified block if it is part of the "tail" that
                 * we're copying anyway.
                 */
                if ((blkno_inseg + 1) * BLCKSZ <= entry->oldsize) {
                    datapagemap_add(&entry->pagemap, blkno_inseg);
                    processed = true;
                }
                break;

            case FILE_ACTION_COPY:
            case FILE_ACTION_REMOVE:
                break;

            case FILE_ACTION_CREATE:
                pg_fatal("unexpected page modification for directory or symbolic link \"%s\"\n", entry->path);
            default:
                break;
        }
        if (processed) {
            pg_log(PG_DEBUG, "add data page map(%s): rel %u/%u/%u forknum %u blkno %u\n", action_to_str(entry->action),
                rnode.spcNode, rnode.dbNode, rnode.relNode, forknum, blkno);
        }
    } else {
        /*
         * If we don't have any record of this file in the file map, it means
         * that it's a relation that doesn't exist in the source system, and
         * it was subsequently removed in the target system, too. We can
         * safely ignore it.
         */
        pg_log(PG_DEBUG, "no entry to be processed: rel %u/%u/%u forknum %u blkno %u\n",
            rnode.spcNode,
            rnode.dbNode,
            rnode.relNode,
            forknum,
            blkno);
    }
}

/*
 * This callback gets called while we read the WAL in the target, for every
 * replication data block need to be synchronized in the target system. It makes note of all the
 * replication data blocks in the pagemap of the file.
 */
void process_waldata_change(
    ForkNumber forknum, RelFileNode rnode, StorageEngine store, off_t file_offset, size_t data_size)
{
    char* path = NULL;
    file_entry_t key;
    file_entry_t* key_ptr = NULL;
    file_entry_t* entry = NULL;
    int file_segno;
    filemap_t* map = filemap;
    file_entry_t** e;
    uint64 max_file_size = 0;
    uint32 cu_slice_index = 0;

    Assert(map->array);

    /* for ROW_STORE the file_offset means blkno, for COLUMN STORE the file_offset means cu_offset */
    if (store == ROW_STORE)
        max_file_size = RELSEG_SIZE;
    else
        max_file_size = MAX_FILE_SIZE;

    file_segno = file_offset / max_file_size;

    /* for COLUMN_STORE the forknum = MAX_FORKNUM + attrid */
    if (store == COLUMN_STORE)
        Assert(forknum > MAX_FORKNUM);

    path = datasegpath(rnode, forknum, file_segno);

    key.path = (char*)path;
    key_ptr = &key;

    e = (file_entry_t**)bsearch(&key_ptr, map->array, map->narray, sizeof(file_entry_t*), path_cmp);
    if (e != NULL)
        entry = *e;
    else
        entry = NULL;

    if (entry != NULL)
        Assert(entry->isrelfile);
    else {
        /* Create a new entry for this file */
        entry = (file_entry_t*)pg_malloc(sizeof(file_entry_t));
        entry->path = pg_strdup(path);
        entry->type = FILE_TYPE_REGULAR;
        entry->action = FILE_ACTION_CREATE;
        entry->oldsize = 0;
        entry->newsize = 0;
        entry->link_target = NULL;
        entry->next = NULL;
        entry->pagemap.bitmap = NULL;
        entry->pagemap.bitmapsize = 0;
        entry->isrelfile = isRelDataFile(path);

        if (map->last != NULL) {
            map->last->next = entry;
            map->last = entry;
        } else
            map->first = map->last = entry;
        map->nlist++;
    }

    pg_free(path);
    path = NULL;

    if (store == ROW_STORE)
        pg_fatal("The Row Store Heap SHOULD BE NOT synchronized in the WAL Streaming.\n");
    else
        entry->block_size = ALIGNOF_CUSIZE;

    Assert(data_size % ALIGNOF_CUSIZE == 0);
    if (data_size % ALIGNOF_CUSIZE != 0)
        pg_fatal("unexpected CU data size %lu bytes which isn't aligned with the required CU data size.\n", data_size);
    /* Here we just follow the ALIGNOF_CUSIZE(8192) of CU */
    for (cu_slice_index = 0; cu_slice_index < data_size / ALIGNOF_CUSIZE; cu_slice_index++)
        datapagemap_add(&entry->pagemap, cu_slice_index);
    return;
}

/*
 * Convert the linked list of entries in map->first/last to the array,
 * map->array.
 */
static void filemap_list_to_array(filemap_t* map)
{
    int narray;
    file_entry_t* entry = NULL;
    file_entry_t* next = NULL;

    map->array = (file_entry_t**)pg_realloc(map->array, (map->nlist + map->narray) * sizeof(file_entry_t*));

    narray = map->narray;
    for (entry = map->first; entry != NULL; entry = next) {
        map->array[narray++] = entry;
        next = entry->next;
        entry->next = NULL;
    }
    Assert(narray == map->nlist + map->narray);
    map->narray = narray;
    map->nlist = 0;
    map->first = map->last = NULL;
}

void filemap_finalize(void)
{
    filemap_t* map = filemap;

    filemap_list_to_array(map);
    qsort(map->array, map->narray, sizeof(file_entry_t*), final_filemap_cmp);
    /*
     * This array is used for path searching because btree has been changed
     * before by other sort method.
     */
    map->arrayForSearch = (file_entry_t**)pg_malloc((map->nlist + map->narray) * sizeof(file_entry_t*));
    for (int narray = 0; narray < map->narray; narray++) {
        map->arrayForSearch[narray] = map->array[narray];
    }
    qsort(map->arrayForSearch, map->narray, sizeof(file_entry_t*), path_cmp);
}

const char* action_to_str(file_action_t action)
{
    switch (action) {
        case FILE_ACTION_NONE:
            return "NONE";
        case FILE_ACTION_COPY:
            return "COPY";
        case FILE_ACTION_TRUNCATE:
            return "TRUNCATE";
        case FILE_ACTION_COPY_TAIL:
            return "COPY_TAIL";
        case FILE_ACTION_CREATE:
            return "CREATE";
        case FILE_ACTION_REMOVE:
            return "REMOVE";

        default:
            return "unknown";
    }
}

/*
 * Calculate the totals needed for progress reports.
 */
void calculate_totals(void)
{
    file_entry_t* entry = NULL;
    int i;
    filemap_t* map = filemap;

    map->total_size = 0;
    map->fetch_size = 0;

    for (i = 0; i < map->narray; i++) {
        entry = map->array[i];

        if (entry->type != FILE_TYPE_REGULAR)
            continue;

        map->total_size += entry->newsize;

        if (entry->action == FILE_ACTION_COPY) {
            map->fetch_size += entry->newsize;
            continue;
        }

        if (entry->action == FILE_ACTION_COPY_TAIL)
            map->fetch_size += (entry->newsize - entry->oldsize);

        if (entry->pagemap.bitmapsize > 0) {
            datapagemap_iterator_t* iter = NULL;
            BlockNumber blk;

            iter = datapagemap_iterate(&entry->pagemap);
            while (datapagemap_next(iter, &blk))
                map->fetch_size += BLCKSZ;

            pg_free(iter);
        }
    }
}

void print_filemap(void)
{
    filemap_t* map = filemap;
    file_entry_t* entry = NULL;
    int i;

    for (i = 0; i < map->narray; i++) {
        entry = map->array[i];
        pg_log(PG_DEBUG,
            /* ------
               translator: first %s is a file path, second is a keyword such as COPY */
            "%s (%s) old(%ld) new(%ld)\n",
            entry->path,
            action_to_str(entry->action),
            entry->oldsize,
            entry->newsize);

        if (entry->pagemap.bitmapsize > 0)
            datapagemap_print(&entry->pagemap);
    }
    (void)fflush(stdout);
}

void print_filemap_to_file(FILE* file)
{
    filemap_t* map = filemap;
    file_entry_t* entry = NULL;
    int i;

    for (i = 0; i < map->narray; i++) {
        entry = map->array[i];
        fprintf(file, "%s (%s) old(%ld) new(%ld)\n",
            entry->path, action_to_str(entry->action),
            entry->oldsize, entry->newsize);
        if (entry->pagemap.bitmapsize > 0) {
            datapagemap_iterator_t* iter = NULL;
            BlockNumber blocknum;
            iter = datapagemap_iterate(&entry->pagemap);
            while (datapagemap_next(iter, &blocknum))
                fprintf(file, "  block %u\n", blocknum);
            pg_free(iter);
        }
    }
}

/*
 * Does it look like a relation data file?
 *
 * For our purposes, only files belonging to the main fork are considered
 * relation files. Other forks are always copied in toto, because we cannot
 * reliably track changes to them, because WAL only contains block references
 * for the main fork.
 */
bool isRelDataFile(const char* path)
{
    RelFileNode rnode;
    unsigned int segNo;
    int forknum;
    int nmatch;
    bool matched = false;
	char *fname = NULL;

    /* ----
     * Relation data files can be in one of the following directories:
     *
     * global/
     *		shared relations
     *
     * base/<db oid>/
     *		regular relations, default tablespace
     *
     * pg_tblspc/<tblspc oid>/PG_9.4_201403261/
     *		within a non-default tablespace (the name of the directory
     *		depends on version)
     *
     * And the relation data files themselves have a filename like:
     *
     * <oid>.<segment number>
     *
     * ----
     */
    rnode.spcNode = InvalidOid;
    rnode.dbNode = InvalidOid;
    rnode.relNode = InvalidOid;
    segNo = 0;
    nmatch = 0;
    forknum = 0;

    nmatch = sscanf_s(path, "global/%u.%u", &rnode.relNode, &segNo);
    if (nmatch == 1 || nmatch == 2) {
        rnode.spcNode = GLOBALTABLESPACE_OID;
        rnode.dbNode = 0;
        matched = true;
    }  else if ((fname = strstr((char*)path, "base/")) != NULL) {
        matched =check_base_path(fname, &segNo, &rnode);
    } else if ((fname = strstr((char*)path, "pg_tblspc/")) != NULL) {
        matched = check_rel_tblspac_path(fname, &segNo, &rnode);
    } else if ((fname = strstr((char*)path, "PG_9.2_201611171")) != NULL) {
        matched = check_abs_tblspac_path(fname, &segNo, &rnode);
    } else {
        matched = false;
    } 

    /*
     * The sscanf tests above can match files that have extra characters at
     * the end, and the last check can also match a path belonging to a
     * different version (different TABLESPACE_VERSION_DIRECTORY). To make
     * eliminate such cases, cross-check that GetRelationPath creates the
     * exact same filename, when passed the RelFileNode information we
     * extracted from the filename.
     */
    if (matched) {
        char* check_path = datasegpath(rnode, forknum, segNo);

        Assert(check_path != NULL);
        if (strcmp(check_path, path) != 0)
            matched = false;

        pg_free(check_path);
    }

    return matched;
}

/*
 * A helper function to create the path of a relation file and segment.
 *
 * The returned path is palloc'd
 */
static char* datasegpath(RelFileNode rnode, ForkNumber forknum, BlockNumber segno)
{
    char* path = NULL;
    char* segpath = NULL;

    path = relpathbackend_t((rnode), InvalidBackendId, (forknum));
    if (segno > 0 || forknum > MAX_FORKNUM) {
        /*
         * add 2 more bytes, 1 for '.', 1 for '\0'.
         * the max length of path is MAXPGPATH, so segPathLen will not overflow.
         */
        int segPathLen = strlen(path) + MAX_UINT32_LEN + 2;
        segpath = (char*)pg_malloc(segPathLen);
        int rc = snprintf_s(segpath, segPathLen, segPathLen - 1, "%s.%u", path, segno);
        securec_check_ss_c(rc, "", "");
        pg_free(path);
        return segpath;
    } else {
        return path;
    }
}

static int path_cmp(const void* a, const void* b)
{
    file_entry_t* fa = *((file_entry_t**)a);
    file_entry_t* fb = *((file_entry_t**)b);

    return strcmp(fa->path, fb->path);
}

/*
 * In the final stage, the filemap is sorted so that removals come last.
 * From disk space usage point of view, it would be better to do removals
 * first, but for now, safety first. If a whole directory is deleted, all
 * files and subdirectories inside it need to removed first. On creation,
 * parent directory needs to be created before files and directories inside
 * it. To achieve that, the file_action_t enum is ordered so that we can
 * just sort on that first. Furthermore, sort REMOVE entries in reverse
 * path order, so that "foo/bar" subdirectory is removed before "foo".
 */
static int final_filemap_cmp(const void* a, const void* b)
{
    file_entry_t* fa = *((file_entry_t**)a);
    file_entry_t* fb = *((file_entry_t**)b);

    if ((fa->type == FILE_TYPE_REGULAR && strstr(fa->path, "pg_xlog") != NULL) &&
        (fb->type != FILE_TYPE_REGULAR || strstr(fb->path, "pg_xlog") == NULL)) {
        return 1;
    }
    if ((fb->type == FILE_TYPE_REGULAR && strstr(fb->path, "pg_xlog") != NULL) &&
        (fa->type != FILE_TYPE_REGULAR || strstr(fa->path, "pg_xlog") == NULL)) {
        return -1;
    }

    if (fa->action > fb->action)
        return 1;
    if (fa->action < fb->action)
        return -1;

    if (fa->action == FILE_ACTION_REMOVE)
        return -strcmp(fa->path, fb->path);
    else
        return strcmp(fa->path, fb->path);
}

/*
 * relpathbackend_t - construct path to a relation's file
 */
static char* relpathbackend_t(RelFileNode rnode, BackendId backend, ForkNumber forknum)
{
    int pathlen = 0;
    char* path = NULL;
    int ss_c = 0;

    /* Column store file path, e.g: 16384_C1.0, 16384_C1_bcm */
    if (forknum > MAX_FORKNUM) {
        char attr_name[32];
        int attid = forknum - MAX_FORKNUM;

        path = (char*)calloc(MAXPGPATH, sizeof(char));
        if (path == NULL)
            pg_fatal("could not alloc MAXPGPATH memory for path.\n");

        ss_c = snprintf_s(attr_name, sizeof(attr_name), sizeof(attr_name) - 1, "C%d", attid);
        securec_check_ss_c(ss_c, "\0", "\0");

        if (rnode.spcNode == GLOBALTABLESPACE_OID) {
            /* Shared system relations live in {datadir}/global */
            Assert(rnode.dbNode == 0);
            pathlen = strlen("global") + 1 + OIDCHARS + 1 + strlen(attr_name) + 1;
            ss_c = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "global/%u_%s", rnode.relNode, attr_name);
            securec_check_ss_c(ss_c, "\0", "\0");
        } else if (rnode.spcNode == DEFAULTTABLESPACE_OID) {
            /* The default tablespace is {datadir}/base */
            pathlen = strlen("base") + 1 + OIDCHARS + 1 + OIDCHARS + 1 + strlen(attr_name) + 1;
            ss_c = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "base/%u/%u_%s", rnode.dbNode, rnode.relNode, attr_name);
            securec_check_ss_c(ss_c, "\0", "\0");
        } else {
            /* All other tablespaces are accessed via symlinks */
            pathlen = 9 + 1 + OIDCHARS + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1 + strlen(pgxcnodename) + 1 +
                      OIDCHARS + 1 + OIDCHARS + 1 + strlen(attr_name) + 1;
            ss_c = snprintf_s(path,
                MAXPGPATH,
                MAXPGPATH - 1,
                "pg_tblspc/%u/%s_%s/%u/%u_%s",
                rnode.spcNode,
                TABLESPACE_VERSION_DIRECTORY,
                pgxcnodename,
                rnode.dbNode,
                rnode.relNode,
                attr_name);
            securec_check_ss_c(ss_c, "\0", "\0");
        }
    } else {
        if (rnode.spcNode == GLOBALTABLESPACE_OID) {
            /* Shared system relations live in {datadir}/global */
            Assert(rnode.dbNode == 0);
            Assert(backend == InvalidBackendId);
            pathlen = 7 + OIDCHARS + 1 + FORKNAMECHARS + 1;
            path = (char*)pg_malloc(pathlen);
            if (forknum != MAIN_FORKNUM)
                ss_c = snprintf_s(path, pathlen, pathlen - 1, "global/%u_%s", rnode.relNode, forkNames_t[forknum]);
            else
                ss_c = snprintf_s(path, pathlen, pathlen - 1, "global/%u", rnode.relNode);
            securec_check_ss_c(ss_c, "\0", "\0");
        } else if (rnode.spcNode == DEFAULTTABLESPACE_OID) {
            /* The default tablespace is {datadir}/base */
            if (backend == InvalidBackendId) {
                pathlen = 5 + OIDCHARS + 1 + OIDCHARS + 1 + FORKNAMECHARS + 1;
                path = (char*)pg_malloc(pathlen);
                if (forknum != MAIN_FORKNUM) {
                    if (rnode.bucketNode == InvalidBktId) {
                        ss_c = snprintf_s(
                            path, pathlen, pathlen - 1, "base/%u/%u_%s", rnode.dbNode, rnode.relNode,
                            forkNames_t[forknum]);
                    } else {
                        ss_c = snprintf_s(
                            path, pathlen, pathlen - 1, "base/%u/%u_b%d_%s", rnode.dbNode, rnode.relNode, 
                            rnode.bucketNode, forkNames_t[forknum]);
                    }
                } else {
                    if (rnode.bucketNode == InvalidBktId) {
                        ss_c = snprintf_s(path, pathlen, pathlen - 1, "base/%u/%u", rnode.dbNode, rnode.relNode);
                    } else {
                        ss_c = snprintf_s(path, pathlen, pathlen - 1, "base/%u/%u_b%d", 
                            rnode.dbNode, rnode.relNode, rnode.bucketNode);
                    }
                }
                securec_check_ss_c(ss_c, "\0", "\0");
            } else {
                /* OIDCHARS will suffice for an integer, too */
                Assert(rnode.bucketNode == InvalidBktId);
                pathlen = 5 + OIDCHARS + 2 + OIDCHARS + 1 + OIDCHARS + 1 + FORKNAMECHARS + 1;
                path = (char*)pg_malloc(pathlen);
                if (forknum != MAIN_FORKNUM)
                    ss_c = snprintf_s(path,
                        pathlen,
                        pathlen - 1,
                        "base/%u/t%d_%u_%s",
                        rnode.dbNode,
                        backend,
                        rnode.relNode,
                        forkNames_t[forknum]);
                else
                    ss_c =
                        snprintf_s(path, pathlen, pathlen - 1, "base/%u/t%d_%u", rnode.dbNode, backend, rnode.relNode);
                securec_check_ss_c(ss_c, "\0", "\0");
            }
        } else {
            /* All other tablespaces are accessed via symlinks */
            if (backend == InvalidBackendId) {
                pathlen = 9 + 1 + OIDCHARS + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1 + OIDCHARS +
                          1
#ifdef PGXC
                          /* Postgres-XC tablespaces include node name */
                          + strlen(pgxcnodename) + 1
#endif
                          + OIDCHARS + 1 + FORKNAMECHARS + 1;
                path = (char*)pg_malloc(pathlen);
#ifdef PGXC
                if (forknum != MAIN_FORKNUM) {
                    if (rnode.bucketNode == InvalidBktId) {
                        ss_c = snprintf_s(path,
                            pathlen,
                            pathlen - 1,
                            "pg_tblspc/%u/%s_%s/%u/%u_%s",
                            rnode.spcNode,
                            TABLESPACE_VERSION_DIRECTORY,
                            pgxcnodename,
                            rnode.dbNode,
                            rnode.relNode,
                            forkNames_t[forknum]);
                    } else {
                        ss_c = snprintf_s(path,
                            pathlen,
                            pathlen - 1,
                            "pg_tblspc/%u/%s_%s/%u/%u_b%d_%s",
                            rnode.spcNode,
                            TABLESPACE_VERSION_DIRECTORY,
                            pgxcnodename,
                            rnode.dbNode,
                            rnode.relNode,
                            rnode.bucketNode,
                            forkNames_t[forknum]);
                    }
                } else {
                    if (rnode.bucketNode == InvalidBktId) {
                        ss_c = snprintf_s(path,
                            pathlen,
                            pathlen - 1,
                            "pg_tblspc/%u/%s_%s/%u/%u",
                            rnode.spcNode,
                            TABLESPACE_VERSION_DIRECTORY,
                            pgxcnodename,
                            rnode.dbNode,
                            rnode.relNode);
                    } else {
                        ss_c = snprintf_s(path,
                            pathlen,
                            pathlen - 1,
                            "pg_tblspc/%u/%s_%s/%u/%u_b%d",
                            rnode.spcNode,
                            TABLESPACE_VERSION_DIRECTORY,
                            pgxcnodename,
                            rnode.dbNode,
                            rnode.relNode,
                            rnode.bucketNode);
                    }
                }
#else
                if (forknum != MAIN_FORKNUM) {
                    ss_c = snprintf_s(path,
                        pathlen,
                        pathlen - 1,
                        "pg_tblspc/%u/%s/%u/%u_%s",
                        rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY,
                        rnode.dbNode,
                        rnode.relNode,
                        forkNames_t[forknum]);
                } else {
                    ss_c = snprintf_s(path,
                        pathlen,
                        pathlen - 1,
                        "pg_tblspc/%u/%s/%u/%u",
                        rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY,
                        rnode.dbNode,
                        rnode.relNode);
                }
#endif
                securec_check_ss_c(ss_c, "\0", "\0");
            } else {
                /* OIDCHARS will suffice for an integer, too */
                pathlen = 9 + 1 + OIDCHARS + 1 + strlen(TABLESPACE_VERSION_DIRECTORY) + 1 + OIDCHARS + 2
#ifdef PGXC
                          + strlen(pgxcnodename) + 1
#endif
                          + OIDCHARS + 1 + OIDCHARS + 1 + FORKNAMECHARS + 1;
                path = (char*)pg_malloc(pathlen);
#ifdef PGXC
                if (forknum != MAIN_FORKNUM)
                    ss_c = snprintf_s(path,
                        pathlen,
                        pathlen - 1,
                        "pg_tblspc/%u/%s_%s/%u/t%d_%u_%s",
                        rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY,
                        pgxcnodename,
                        rnode.dbNode,
                        backend,
                        rnode.relNode,
                        forkNames_t[forknum]);
                else
                    ss_c = snprintf_s(path,
                        pathlen,
                        pathlen - 1,
                        "pg_tblspc/%u/%s_%s/%u/t%d_%u",
                        rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY,
                        pgxcnodename,
                        rnode.dbNode,
                        backend,
                        rnode.relNode);
#else
                if (forknum != MAIN_FORKNUM)
                    ss_c = snprintf_s(path,
                        pathlen,
                        pathlen - 1,
                        "pg_tblspc/%u/%s/%u/t%d_%u_%s",
                        rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY,
                        rnode.dbNode,
                        backend,
                        rnode.relNode,
                        forkNames_t[forknum]);
                else
                    ss_c = snprintf_s(path,
                        pathlen,
                        pathlen - 1,
                        "pg_tblspc/%u/%s/%u/t%d_%u",
                        rnode.spcNode,
                        TABLESPACE_VERSION_DIRECTORY,
                        rnode.dbNode,
                        backend,
                        rnode.relNode);
#endif
                securec_check_ss_c(ss_c, "\0", "\0");
            }
        }
    }
    return path;
}

bool check_base_path(const char *fname, unsigned int *segNo, RelFileNode *rnode)
{
    int         columnid = 0;
    int         nmatch;

    rnode->spcNode = DEFAULTTABLESPACE_OID;
    rnode->dbNode = InvalidOid;
    rnode->relNode = InvalidOid;
    rnode->bucketNode = InvalidBktId;

    nmatch = sscanf_s(fname, "base/%u/%u_C%d.%u",
                    &rnode->dbNode, &rnode->relNode, &columnid, segNo);
    if (nmatch == MATCH_FOUR) {
        return false;
    }
    nmatch = sscanf_s(fname, "base/%u/%u_b%d.%u",
                      &rnode->dbNode, &rnode->relNode, &rnode->bucketNode, segNo);
    if (nmatch == MATCH_THREE || nmatch == MATCH_FOUR) {
        return true;
    }

    nmatch = sscanf_s(fname, "base/%u/%u_b%d_vm.%u",
                      &rnode->dbNode, &rnode->relNode, &rnode->bucketNode, segNo);
    if (nmatch == MATCH_THREE || nmatch == MATCH_FOUR) {
        return true;
    }
    nmatch = sscanf_s(fname, "base/%u/%u_b%d_fsm.%u",
                      &rnode->dbNode, &rnode->relNode, &rnode->bucketNode, segNo);
    if (nmatch == MATCH_THREE || nmatch == MATCH_FOUR) {
        return true;
    }

    nmatch = sscanf_s(fname, "base/%u/%u.%u",
                      &rnode->dbNode, &rnode->relNode, segNo);
    if (nmatch == MATCH_TWO || nmatch == MATCH_THREE) {
        return true;
    }

    nmatch = sscanf_s(fname, "base/%u/%u_vm.%u",
                      &rnode->dbNode, &rnode->relNode, segNo);
    if (nmatch == MATCH_TWO || nmatch == MATCH_THREE) {
        return true;
    }
    nmatch = sscanf_s(fname, "base/%u/%u_fsm.%u",
                      &rnode->dbNode, &rnode->relNode, segNo);
    if (nmatch == MATCH_TWO || nmatch == MATCH_THREE) {
        return true;
    }
    return false;
}

bool check_rel_tblspac_path(const char *fname, unsigned int *segNo, RelFileNode *rnode)
{
    char        buf[FILE_NAME_MAX_LEN] = {0};
    int         columnid = 0;
    int         nmatch;

    rnode->spcNode = InvalidOid;
    rnode->dbNode = InvalidOid;
    rnode->relNode = InvalidOid;
    rnode->bucketNode = InvalidBktId;
    nmatch = sscanf_s(fname, "pg_tblspc/%u/%[^/]/%u/%u_C%d.%u",
                      &rnode->spcNode, buf, sizeof(buf), &rnode->dbNode, &rnode->relNode, &columnid, segNo);
    if (nmatch == MATCH_SIX) {
        return false;
    }

    nmatch = sscanf_s(fname, "pg_tblspc/%u/%[^/]/%u/%u_b%d.%u",
                      &rnode->spcNode, buf, sizeof(buf), &rnode->dbNode, &rnode->relNode, &rnode->bucketNode, segNo);
    if (nmatch == MATCH_FIVE || nmatch == MATCH_SIX) {
        return true;
    }

    nmatch = sscanf_s(fname, "pg_tblspc/%u/%[^/]/%u/%u_b%d_fsm.%u",
                      &rnode->spcNode, buf, sizeof(buf), &rnode->dbNode, &rnode->relNode, &rnode->bucketNode, segNo);
    if (nmatch == MATCH_FIVE || nmatch == MATCH_SIX) {
        return true;
    }

    nmatch = sscanf_s(fname, "pg_tblspc/%u/%[^/]/%u/%u_b%d_vm.%u",
                      &rnode->spcNode, buf, sizeof(buf), &rnode->dbNode, &rnode->relNode, &rnode->bucketNode, segNo);
    if (nmatch == MATCH_FIVE || nmatch == MATCH_SIX) {
        return true;
    }

    nmatch = sscanf_s(fname, "pg_tblspc/%u/%[^/]/%u/%u.%u",
                      &rnode->spcNode, buf, sizeof(buf), &rnode->dbNode, &rnode->relNode, segNo);
    if (nmatch == MATCH_FOUR || nmatch == MATCH_FIVE) {
        return true;
    }

    nmatch = sscanf_s(fname, "pg_tblspc/%u/%[^/]/%u/%u_fsm.%u",
                      &rnode->spcNode, buf, sizeof(buf), &rnode->dbNode, &rnode->relNode, segNo);
    if (nmatch == MATCH_FOUR || nmatch == MATCH_FIVE) {
        return true;
    }

    nmatch = sscanf_s(fname, "pg_tblspc/%u/%[^/]/%u/%u_vm.%u",
                      &rnode->spcNode, buf, sizeof(buf), &rnode->dbNode, &rnode->relNode, segNo);
    if (nmatch == MATCH_FOUR || nmatch == MATCH_FIVE) {
        return true;
    }

    return false;
}

bool check_abs_tblspac_path(const char *fname, unsigned int *segNo, RelFileNode *rnode)
{
    char        buf[FILE_NAME_MAX_LEN] = {0};
    int         columnid = 0;
    int         nmatch;
    unsigned int bktno = 0;

    rnode->spcNode = InvalidOid;
    rnode->dbNode = InvalidOid;
    rnode->relNode = InvalidOid;
    rnode->bucketNode = InvalidBktId;
    nmatch = sscanf_s(fname, "PG_9.2_201611171_%[^/]/%u/%u_C%d.%u",
                      buf, sizeof(buf), &rnode->dbNode, &rnode->relNode, &columnid, segNo);
    if (nmatch == MATCH_SIX) {
        return false;
    }
    nmatch = sscanf_s(fname, "PG_9.2_201611171_%[^/]/%u/%u_b%u.%u",
                      buf, sizeof(buf), &rnode->dbNode, &rnode->relNode, &bktno, segNo);
    if (nmatch == MATCH_FIVE || nmatch == MATCH_SIX) {
        return true;
    }
    nmatch = sscanf_s(fname, "PG_9.2_201611171_%[^/]/%u/%u_b%u_fsm.%u",
                      buf, sizeof(buf), &rnode->dbNode, &rnode->relNode, &bktno, segNo);
    if (nmatch == MATCH_FIVE || nmatch == MATCH_SIX) {
        return true;
    }
    nmatch = sscanf_s(fname, "PG_9.2_201611171_%[^/]/%u/%u_b%u_vm.%u",
                      buf, sizeof(buf), &rnode->dbNode, &rnode->relNode, &bktno, segNo);
    if (nmatch == MATCH_FIVE || nmatch == MATCH_SIX) {
        return true;
    }

    nmatch = sscanf_s(fname, "PG_9.2_201611171_%[^/]/%u/%u.%u",
                      buf, sizeof(buf), &rnode->dbNode, &rnode->relNode, segNo);
    if (nmatch == MATCH_FOUR || nmatch == MATCH_FIVE) {
        return true;
    }

    nmatch = sscanf_s(fname, "PG_9.2_201611171_%[^/]/%u/%u_fsm.%u",
                      buf, sizeof(buf), &rnode->dbNode, &rnode->relNode, segNo);
    if (nmatch == MATCH_FOUR || nmatch == MATCH_FIVE) {
        return true;
    }

    nmatch = sscanf_s(fname, "PG_9.2_201611171_%[^/]/%u/%u_vm.%u",
                      buf, sizeof(buf), &rnode->dbNode, &rnode->relNode, segNo);
    if (nmatch == MATCH_FOUR || nmatch == MATCH_FIVE) {
        return true;
    }

    return false;
}

bool isPathInFilemap(const char* path)
{
    filemap_t* map = filemap;
    file_entry_t key;
    file_entry_t* key_ptr = NULL;
    file_entry_t** e = NULL;

    if (path == NULL) {
        pg_log(PG_ERROR, "invalid input path=%s.\n", path);
        return false;
    }
    key.path = (char*)path;
    key_ptr = &key;
    e = (file_entry_t**)bsearch(&key_ptr, map->arrayForSearch, map->narray, sizeof(file_entry_t*), path_cmp);
    if (e == NULL) {
        pg_log(PG_DEBUG, "path %s is not bsearch in current map\n", path);
        return false;
    }
    if (*e == NULL) {
        pg_log(PG_DEBUG, "path %s is not valid in current map\n", path);
        return false;
    }
    pg_log(PG_DEBUG,
        "path %s,type %d,size %lu,link %s\n",
        (*e)->path,
        (*e)->type,
        (*e)->oldsize,
        ((*e)->link_target != NULL) ? (*e)->link_target : "NULL");
    return true;
}

