/* ---------------------------------------------------------------------------------------
 * 
 * vfd.h
 *      Virtual file descriptor definitions.
 * 
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/include/storage/vfd.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef VFD_H
#define VFD_H

#include <dirent.h>
#include "utils/resowner.h"
#include "storage/page_compression.h"
#include "storage/smgr/relfilenode.h"

#include "storage/file/fio_device_com.h"
typedef struct vfd {
    int fd;                 /* current FD, or VFD_CLOSED if none */
    unsigned short fdstate; /* bitflags for VFD's state */
    ResourceOwner resowner; /* owner, for automatic cleanup */
    File nextFree;          /* link to next free VFD, if in freelist */
    File lruMoreRecently;   /* doubly linked recency-of-use list */
    File lruLessRecently;
    off_t seekPos;  /* current logical file position */
    off_t fileSize; /* current size of file (0 if not temporary) */
    char* fileName; /* name of file, or NULL for unused VFD */
    bool infdCache; /* true if in fd cache */
    /* NB: fileName is malloc'd, and must be free'd when closing the VFD */
    int fileFlags;               /* open(2) flags for (re)opening the file */
    int fileMode;                /* mode to pass to open(2) */
    RelFileNodeForkNum fileNode; /* current logical file node */
    bool with_pcmap;        /* is page compression relation */
    PageCompressHeader *pcmap;    /* memory map of page compression address file */
} Vfd;

#endif /* VFD_H */
