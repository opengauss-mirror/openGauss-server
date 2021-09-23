/*-------------------------------------------------------------------------
 *
 * sharedfileset.h
 *    Shared temporary file management.
 *
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/sharedfilespace.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SHAREDFILESET_H
#define SHAREDFILESET_H

#include "storage/smgr/fd.h"
#include "storage/spin.h"
#include "gs_thread.h"

/*
 * A set of temporary files that can be shared by multiple backends.
 */
typedef struct SharedFileSet {
    ThreadId creator_pid;  /* PID of the creating process */
    uint32 number;      /* per-PID identifier */
    slock_t mutex;      /* mutex protecting the reference count */
    int refcnt;         /* number of attached backends */
    int ntablespaces;   /* number of tablespaces to use */
    Oid tablespaces[8]; /* OIDs of tablespaces to use. Assumes that
                         * it's rare that there more than temp
                         * tablespaces. */
} SharedFileSet;

extern void SharedFileSetInit(SharedFileSet *fileset);
extern void SharedFileSetAttach(SharedFileSet *fileset);
extern File SharedFileSetCreate(SharedFileSet *fileset, const char *name);
extern File SharedFileSetOpen(SharedFileSet *fileset, const char *name);
extern bool SharedFileSetDelete(SharedFileSet *fileset, const char *name, bool errorOnFailure);
extern void SharedFileSetDeleteAll(const SharedFileSet *fileset);

#endif
