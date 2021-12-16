/* -------------------------------------------------------------------------
 *
 * filemap.h
 *
 * Copyright (c) 2013-2015, PostgreSQL Global Development Group
 * -------------------------------------------------------------------------
 */
#ifndef FILEMAP_H
#define FILEMAP_H

#include "compressed_common.h"
#include "storage/smgr/relfilenode.h"
#include "storage/buf/block.h"

#include "datapagemap.h"

/*
 * For every file found in the local or remote system, we have a file entry
 * which says what we are going to do with the file. For relation files,
 * there is also a page map, marking pages in the file that were changed
 * locally.
 *
 * The enum values are sorted in the order we want actions to be processed.
 */
typedef enum {
    FILE_ACTION_CREATE,    /* create local directory or symbolic link */
    FILE_ACTION_COPY,      /* copy whole file, overwriting if exists */
    FILE_ACTION_COPY_TAIL, /* copy tail from 'oldsize' to 'newsize' */
    FILE_ACTION_NONE,      /* no action (we might still copy modified
                            * blocks based on the parsed WAL) */
    FILE_ACTION_TRUNCATE,  /* truncate local file to 'newsize' bytes */
    FILE_ACTION_REMOVE     /* remove local file / directory / symlink */
} file_action_t;

typedef enum { FILE_TYPE_REGULAR, FILE_TYPE_DIRECTORY, FILE_TYPE_SYMLINK } file_type_t;
#define PG_F_ISDIR(type) (type == FILE_TYPE_DIRECTORY)
#define PG_F_ISREG(type) (type == FILE_TYPE_REGULAR)
#define PG_F_ISLNK(type) (type == FILE_TYPE_SYMLINK)

typedef struct file_entry_t {
    char* path;
    file_type_t type;

    file_action_t action;

    /* for compressed table */
    RewindCompressInfo rewindCompressInfo;

    /* for a regular file */
    size_t oldsize;
    size_t newsize;
    bool isrelfile; /* is it a relation data file? */

    datapagemap_t pagemap;

    /* for a symlink */
    char* link_target;

    size_t block_size; /* for ROW_STORE is BLCKSZ, for COLUMN_STORE is ALIGNOF_CUSIZE */

    struct file_entry_t* next;
} file_entry_t;

typedef struct filemap_t {
    /*
     * New entries are accumulated to a linked list, in process_source_file
     * and process_target_file.
     */
    file_entry_t* first;
    file_entry_t* last;
    int nlist; /* number of entries currently in list */

    /*
     * After processing all the remote files, the entries in the linked list
     * are moved to this array. After processing local files, too, all the
     * local entries are added to the array by filemap_finalize, and sorted in
     * the final order. After filemap_finalize, all the entries are in the
     * array, and the linked list is empty.
     */
    file_entry_t** array;
    int narray; /* current length of array */

    /*
     * Summary information. total_size is the total size of the source
     * cluster, and fetch_size is the number of bytes that needs to be copied.
     */
    uint64 total_size;
    uint64 fetch_size;
    file_entry_t** arrayForSearch;
} filemap_t;

#define MAX_VALUE_LEN 1024

extern filemap_t* filemap;
extern char pgxcnodename[MAX_VALUE_LEN];

extern void filemapInit(void);
extern void calculate_totals(void);
extern void print_filemap(void);
extern void print_filemap_to_file(FILE* file);

/* Functions for populating the filemap */
extern void process_source_file(const char* path, file_type_t type, size_t newsize, const char* link_target,
    RewindCompressInfo* rewindCompressInfo = nullptr);
extern void process_target_file(const char* path, file_type_t type, size_t newsize, const char* link_target,
    const RewindCompressInfo* rewindCompressInfo = nullptr);
extern void process_block_change(ForkNumber forknum, RelFileNode rnode, BlockNumber blkno);
extern void process_waldata_change(
    ForkNumber forknum, RelFileNode rnode, StorageEngine store, off_t file_offset, size_t data_size);
extern void filemap_finalize(void);
extern int targetFilemapSearch(const char* path, file_entry_t* entry);
extern bool isRelDataFile(const char* path);
extern const char* action_to_str(file_action_t action);

#endif /* FILEMAP_H */
