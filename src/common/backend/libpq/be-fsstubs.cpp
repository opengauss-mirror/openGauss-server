/* -------------------------------------------------------------------------
 *
 * be-fsstubs.cpp
 *	  Builtin functions for open/close/read/write operations on large objects
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/libpq/be-fsstubs.cpp
 *
 * NOTES
 *	  This should be moved to a more appropriate place.  It is here
 *	  for lack of a better place.
 *
 *	  These functions store LargeObjectDesc structs in a private MemoryContext,
 *	  which means that large object descriptors hang around until we destroy
 *	  the context at transaction end.  It'd be possible to prolong the lifetime
 *	  of the context so that LO FDs are good across transactions (for example,
 *	  we could release the context only if we see that no FDs remain open).
 *	  But we'd need additional state in order to do the right thing at the
 *	  end of an aborted transaction.  FDs opened during an aborted xact would
 *	  still need to be closed, since they might not be pointing at valid
 *	  relations at all.  Locking semantics are also an interesting problem
 *	  if LOs stay open across transactions.  For now, we'll stick with the
 *	  existing documented semantics of LO FDs: they're only good within a
 *	  transaction.
 *
 *	  As of PostgreSQL 8.0, much of the angst expressed above is no longer
 *	  relevant, and in fact it'd be pretty easy to allow LO FDs to stay
 *	  open across transactions.  (Snapshot relevancy would still be an issue.)
 *	  However backwards compatibility suggests that we should stick to the
 *	  status quo.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "libpq/be-fsstubs.h"
#include "libpq/libpq-fs.h"
#include "miscadmin.h"
#include "storage/smgr/fd.h"
#include "storage/large_object.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#define BUFSIZE 8192

#define CreateFSContext()                                                        \
    do {                                                                         \
        if (u_sess->libpq_cxt.fscxt == NULL)                                     \
            u_sess->libpq_cxt.fscxt = AllocSetContextCreate(u_sess->top_mem_cxt, \
                "Filesystem",                                                    \
                ALLOCSET_DEFAULT_MINSIZE,                                        \
                ALLOCSET_DEFAULT_INITSIZE,                                       \
                ALLOCSET_DEFAULT_MAXSIZE);                                       \
    } while (0)

static int newLOfd(LargeObjectDesc* lobjCookie);
static void deleteLOfd(int fd);
static Oid lo_import_internal(text* filename, Oid lobjOid);

/*****************************************************************************
 *	File Interfaces for Large Objects
 *****************************************************************************/

Datum lo_open(PG_FUNCTION_ARGS)
{
    Oid lobjId = PG_GETARG_OID(0);
    int32 mode = PG_GETARG_INT32(1);
    LargeObjectDesc* lobjDesc = NULL;
    int fd;

#ifdef PGXC
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("openGauss does not support large object yet"),
            errdetail("The feature is not currently supported")));
#endif

    CreateFSContext();

    lobjDesc = inv_open(lobjId, mode, u_sess->libpq_cxt.fscxt);

    if (lobjDesc == NULL) { /* lookup failed */
        PG_RETURN_INT32(-1);
    }

    fd = newLOfd(lobjDesc);

    PG_RETURN_INT32(fd);
}

Datum lo_close(PG_FUNCTION_ARGS)
{
    int32 fd = PG_GETARG_INT32(0);

#ifdef PGXC
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("openGauss does not support large object yet"),
            errdetail("The feature is not currently supported")));
#endif

    if (fd < 0 || fd >= u_sess->libpq_cxt.cookies_size || u_sess->libpq_cxt.cookies[fd] == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("invalid large-object descriptor: %d", fd)));

    inv_close(u_sess->libpq_cxt.cookies[fd]);

    deleteLOfd(fd);

    PG_RETURN_INT32(0);
}

/*****************************************************************************
 *	Bare Read/Write operations --- these are not fmgr-callable!
 *
 *	We assume the large object supports byte oriented reads and seeks so
 *	that our work is easier.
 *
 *****************************************************************************/

int lo_read(int fd, char* buf, int len)
{
    int status = -1;

#ifdef PGXC
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("openGauss does not support large object yet"),
            errdetail("The feature is not currently supported")));
#endif

    if (fd < 0 || fd >= u_sess->libpq_cxt.cookies_size || u_sess->libpq_cxt.cookies[fd] == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("invalid large-object descriptor: %d", fd)));

    /* Permission checks */
    if (!u_sess->attr.attr_sql.lo_compat_privileges &&
        pg_largeobject_aclcheck_snapshot(
            u_sess->libpq_cxt.cookies[fd]->id, GetUserId(), ACL_SELECT, u_sess->libpq_cxt.cookies[fd]->snapshot) !=
            ACLCHECK_OK)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied for large object %u", u_sess->libpq_cxt.cookies[fd]->id)));

    status = inv_read(u_sess->libpq_cxt.cookies[fd], buf, len);

    return status;
}

int lo_write(int fd, const char* buf, int len)
{
    int status = -1;

#ifdef PGXC
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("openGauss does not support large object yet"),
            errdetail("The feature is not currently supported")));
#endif

    if (fd < 0 || fd >= u_sess->libpq_cxt.cookies_size || u_sess->libpq_cxt.cookies[fd] == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("invalid large-object descriptor: %d", fd)));

    if (((unsigned int)u_sess->libpq_cxt.cookies[fd]->flags & IFS_WRLOCK) == 0)
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("large object descriptor %d was not opened for writing", fd)));

    /* Permission checks */
    if (!u_sess->attr.attr_sql.lo_compat_privileges &&
        pg_largeobject_aclcheck_snapshot(
            u_sess->libpq_cxt.cookies[fd]->id, GetUserId(), ACL_UPDATE, u_sess->libpq_cxt.cookies[fd]->snapshot) !=
            ACLCHECK_OK)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied for large object %u", u_sess->libpq_cxt.cookies[fd]->id)));

    status = inv_write(u_sess->libpq_cxt.cookies[fd], buf, len);

    return status;
}

Datum lo_lseek(PG_FUNCTION_ARGS)
{
    int32 fd = PG_GETARG_INT32(0);
    int32 offset = PG_GETARG_INT32(1);
    int32 whence = PG_GETARG_INT32(2);
    int status;

#ifdef PGXC
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("openGauss does not support large object yet"),
            errdetail("The feature is not currently supported")));
#endif

    if (fd < 0 || fd >= u_sess->libpq_cxt.cookies_size || u_sess->libpq_cxt.cookies[fd] == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("invalid large-object descriptor: %d", fd)));

    status = inv_seek(u_sess->libpq_cxt.cookies[fd], offset, whence);

    PG_RETURN_INT32(status);
}

Datum lo_creat(PG_FUNCTION_ARGS)
{
    Oid lobjId;

#ifdef PGXC
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("openGauss does not support large object yet"),
            errdetail("The feature is not currently supported")));
#endif

    /*
     * We don't actually need to store into fscxt, but create it anyway to
     * ensure that AtEOXact_LargeObject knows there is state to clean up
     */
    CreateFSContext();

    lobjId = inv_create(InvalidOid);

    PG_RETURN_OID(lobjId);
}

Datum lo_create(PG_FUNCTION_ARGS)
{
    Oid lobjId = PG_GETARG_OID(0);

#ifdef PGXC
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("openGauss does not support large object yet"),
            errdetail("The feature is not currently supported")));
#endif

    /*
     * We don't actually need to store into fscxt, but create it anyway to
     * ensure that AtEOXact_LargeObject knows there is state to clean up
     */
    CreateFSContext();

    lobjId = inv_create(lobjId);

    PG_RETURN_OID(lobjId);
}

Datum lo_tell(PG_FUNCTION_ARGS)
{
    int32 fd = PG_GETARG_INT32(0);

#ifdef PGXC
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("openGauss does not support large object yet"),
            errdetail("The feature is not currently supported")));
#endif

    if (fd < 0 || fd >= u_sess->libpq_cxt.cookies_size || u_sess->libpq_cxt.cookies[fd] == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("invalid large-object descriptor: %d", fd)));

    PG_RETURN_INT32(inv_tell(u_sess->libpq_cxt.cookies[fd]));
}

Datum lo_unlink(PG_FUNCTION_ARGS)
{
    Oid lobjId = PG_GETARG_OID(0);

#ifdef PGXC
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("openGauss does not support large object yet"),
            errdetail("The feature is not currently supported")));
#endif

    /* Must be owner of the largeobject */
    if (!u_sess->attr.attr_sql.lo_compat_privileges && !pg_largeobject_ownercheck(lobjId, GetUserId()))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be owner of large object %u", lobjId)));

    /*
     * If there are any open LO FDs referencing that ID, close 'em.
     */
    if (u_sess->libpq_cxt.fscxt != NULL) {
        int i;

        for (i = 0; i < u_sess->libpq_cxt.cookies_size; i++) {
            if (u_sess->libpq_cxt.cookies[i] != NULL && u_sess->libpq_cxt.cookies[i]->id == lobjId) {
                inv_close(u_sess->libpq_cxt.cookies[i]);
                deleteLOfd(i);
            }
        }
    }

    /*
     * inv_drop does not create a need for end-of-transaction cleanup and
     * hence we don't need to have created fscxt.
     */
    PG_RETURN_INT32(inv_drop(lobjId));
}

/*****************************************************************************
 *	Read/Write using bytea
 *****************************************************************************/

Datum loread(PG_FUNCTION_ARGS)
{
    int32 fd = PG_GETARG_INT32(0);
    int32 len = PG_GETARG_INT32(1);
    bytea* retval = NULL;
    int totalread;

#ifdef PGXC
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("openGauss does not support large object yet"),
            errdetail("The feature is not currently supported")));
#endif

    if (len < 0)
        len = 0;

    retval = (bytea*)palloc(VARHDRSZ + len);
    totalread = lo_read(fd, VARDATA(retval), len);
    SET_VARSIZE(retval, totalread + VARHDRSZ);

    PG_RETURN_BYTEA_P(retval);
}

Datum lowrite(PG_FUNCTION_ARGS)
{
    int32 fd = PG_GETARG_INT32(0);
    bytea* wbuf = PG_GETARG_BYTEA_P(1);
    int bytestowrite;
    int totalwritten;

#ifdef PGXC
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("openGauss does not support large object yet"),
            errdetail("The feature is not currently supported")));
#endif

    bytestowrite = VARSIZE(wbuf) - VARHDRSZ;
    totalwritten = lo_write(fd, VARDATA(wbuf), bytestowrite);
    PG_RETURN_INT32(totalwritten);
}

/*****************************************************************************
 *	 Import/Export of Large Object
 *****************************************************************************/

/*
 * lo_import -
 *	  imports a file as an (inversion) large object.
 */
Datum lo_import(PG_FUNCTION_ARGS)
{
    text* filename = PG_GETARG_TEXT_PP(0);

#ifdef PGXC
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("openGauss does not support large object yet"),
            errdetail("The feature is not currently supported")));
#endif

    PG_RETURN_OID(lo_import_internal(filename, InvalidOid));
}

/*
 * lo_import_with_oid -
 *	  imports a file as an (inversion) large object specifying oid.
 */
Datum lo_import_with_oid(PG_FUNCTION_ARGS)
{
    text* filename = PG_GETARG_TEXT_PP(0);
    Oid oid = PG_GETARG_OID(1);

#ifdef PGXC
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("openGauss does not support large object yet"),
            errdetail("The feature is not currently supported")));
#endif

    PG_RETURN_OID(lo_import_internal(filename, oid));
}

static Oid lo_import_internal(text* filename, Oid lobjOid)
{
    File fd;
    int nbytes = 0;
    int tmp PG_USED_FOR_ASSERTS_ONLY;
    char buf[BUFSIZE];
    char fnamebuf[MAXPGPATH];
    LargeObjectDesc* lobj = NULL;
    Oid oid;
    int offset = 0;

#ifndef ALLOW_DANGEROUS_LO_FUNCTIONS
    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be system admin to use server-side lo_import()"),
                errhint("Anyone can use the client-side lo_import() provided by libpq.")));
#endif

    CreateFSContext();

    /*
     * open the file to be read in
     */
    text_to_cstring_buffer(filename, fnamebuf, sizeof(fnamebuf));
    fd = PathNameOpenFile(fnamebuf, O_RDONLY | PG_BINARY, S_IRWXU);
    if (fd < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not open server file \"%s\": %m", fnamebuf)));

    /*
     * create an inversion object
     */
    oid = inv_create(lobjOid);

    /*
     * read in from the filesystem and write to the inversion object
     */
    lobj = inv_open(oid, INV_WRITE, u_sess->libpq_cxt.fscxt);

    while ((nbytes = FilePRead(fd, buf, BUFSIZE, offset)) > 0) {
        tmp = inv_write(lobj, buf, nbytes);
        offset += nbytes;
        Assert(tmp == nbytes);
    }

    inv_close(lobj);
    FileClose(fd);

    if (nbytes < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read server file \"%s\": %m", fnamebuf)));

    return oid;
}

/*
 * lo_export -
 *	  exports an (inversion) large object.
 */
Datum lo_export(PG_FUNCTION_ARGS)
{
    Oid lobjId = PG_GETARG_OID(0);
    text* filename = PG_GETARG_TEXT_PP(1);
    File fd = -1;
    int nbytes = 0;
    int tmp = 0;
    char buf[BUFSIZE];
    char fnamebuf[MAXPGPATH];
    LargeObjectDesc* lobj = NULL;
    int offset = 0;
    bool dirIsExist = false;
    struct stat checkdir;
    mode_t oumask;

#ifdef PGXC
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("openGauss does not support large object yet"),
            errdetail("The feature is not currently supported")));
#endif

#ifndef ALLOW_DANGEROUS_LO_FUNCTIONS
    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be system admin to use server-side lo_export()"),
                errhint("Anyone can use the client-side lo_export() provided by libpq.")));
#endif

    CreateFSContext();

    /*
     * open the inversion object (no need to test for failure)
     */
    lobj = inv_open(lobjId, INV_READ, u_sess->libpq_cxt.fscxt);

    /*
     * open the file to be written to
     *
     * Note: we reduce backend's normal 077 umask to the slightly friendlier
     * 022. This code used to drop it all the way to 0, but creating
     * world-writable export files doesn't seem wise.
     */
    text_to_cstring_buffer(filename, fnamebuf, sizeof(fnamebuf));

    if (stat(fnamebuf, &checkdir) == 0)
        dirIsExist = true;

    oumask = umask(S_IWGRP | S_IWOTH);
    PG_TRY();
    {
        fd =
            PathNameOpenFile(fnamebuf, O_CREAT | O_WRONLY | O_TRUNC | PG_BINARY, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    }
    PG_CATCH();
    {
        (void)umask(oumask);
        PG_RE_THROW();
    }
    PG_END_TRY();
    (void)umask(oumask);

    if (fd < 0) {
        inv_close(lobj);
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not create server file \"%s\": %m", fnamebuf)));
    }

    if (!dirIsExist) {
        if (chmod(fnamebuf, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH) < 0) {
            FileClose(fd);
            inv_close(lobj);
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not chmod server file \"%s\": %m", fnamebuf)));
        }
    }

    /*
     * read in from the inversion file and write to the filesystem
     */
    while ((nbytes = inv_read(lobj, buf, BUFSIZE)) > 0) {
        tmp = FilePWrite(fd, buf, nbytes, offset);
        if (tmp != nbytes) {
            FileClose(fd);
            inv_close(lobj);
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not write server file \"%s\": %m", fnamebuf)));
        }
        offset += tmp;
    }

    FileClose(fd);
    inv_close(lobj);

    PG_RETURN_INT32(1);
}

/*
 * lo_truncate -
 *	  truncate a large object to a specified length
 */
Datum lo_truncate(PG_FUNCTION_ARGS)
{
    int32 fd = PG_GETARG_INT32(0);
    int32 len = PG_GETARG_INT32(1);

#ifdef PGXC
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("openGauss does not support large object yet"),
            errdetail("The feature is not currently supported")));
#endif

    if (fd < 0 || fd >= u_sess->libpq_cxt.cookies_size || u_sess->libpq_cxt.cookies[fd] == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("invalid large-object descriptor: %d", fd)));

    /* Permission checks */
    if (!u_sess->attr.attr_sql.lo_compat_privileges &&
        pg_largeobject_aclcheck_snapshot(
            u_sess->libpq_cxt.cookies[fd]->id, GetUserId(), ACL_UPDATE, u_sess->libpq_cxt.cookies[fd]->snapshot) !=
            ACLCHECK_OK)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied for large object %u", u_sess->libpq_cxt.cookies[fd]->id)));

    inv_truncate(u_sess->libpq_cxt.cookies[fd], len);

    PG_RETURN_INT32(0);
}

/*
 * AtEOXact_LargeObject -
 *		 prepares large objects for transaction commit
 */
void AtEOXact_LargeObject(bool isCommit)
{
    int i;

    if (u_sess->libpq_cxt.fscxt == NULL)
        return; /* no LO operations in this xact */

    /*
     * Close LO fds and clear cookies array so that LO fds are no longer good.
     * On abort we skip the close step.
     */
    for (i = 0; i < u_sess->libpq_cxt.cookies_size; i++) {
        if (u_sess->libpq_cxt.cookies[i] != NULL) {
            if (isCommit)
                inv_close(u_sess->libpq_cxt.cookies[i]);
            deleteLOfd(i);
        }
    }

    /* Needn't actually pfree since we're about to zap context */
    u_sess->libpq_cxt.cookies = NULL;
    u_sess->libpq_cxt.cookies_size = 0;

    /* Release the LO memory context to prevent permanent memory leaks. */
    MemoryContextDelete(u_sess->libpq_cxt.fscxt);
    u_sess->libpq_cxt.fscxt = NULL;

    /* Give inv_api.c a chance to clean up, too */
    close_lo_relation(isCommit);
}

/*
 * AtEOSubXact_LargeObject
 *		Take care of large objects at subtransaction commit/abort
 *
 * Reassign LOs created/opened during a committing subtransaction
 * to the parent subtransaction.  On abort, just close them.
 */
void AtEOSubXact_LargeObject(bool isCommit, SubTransactionId mySubid, SubTransactionId parentSubid)
{
    int i;

    if (u_sess->libpq_cxt.fscxt == NULL) /* no LO operations in this xact */
        return;

    for (i = 0; i < u_sess->libpq_cxt.cookies_size; i++) {
        LargeObjectDesc* lo = u_sess->libpq_cxt.cookies[i];

        if (lo != NULL && lo->subid == mySubid) {
            if (isCommit)
                lo->subid = parentSubid;
            else {
                /*
                 * Make sure we do not call inv_close twice if it errors out
                 * for some reason.  Better a leak than a crash.
                 */
                deleteLOfd(i);
                inv_close(lo);
            }
        }
    }
}

/*****************************************************************************
 *	Support routines for this file
 *****************************************************************************/

static int newLOfd(LargeObjectDesc* lobjCookie)
{
    int i, rc, newsize;

    /* Try to find a free slot */
    for (i = 0; i < u_sess->libpq_cxt.cookies_size; i++) {
        if (u_sess->libpq_cxt.cookies[i] == NULL) {
            u_sess->libpq_cxt.cookies[i] = lobjCookie;
            return i;
        }
    }

    /* No free slot, so make the array bigger */
    if (u_sess->libpq_cxt.cookies_size <= 0) {
        /* First time through, arbitrarily make 64-element array */
        i = 0;
        newsize = 64;
        u_sess->libpq_cxt.cookies =
            (LargeObjectDesc**)MemoryContextAllocZero(u_sess->libpq_cxt.fscxt, newsize * sizeof(LargeObjectDesc*));
        u_sess->libpq_cxt.cookies_size = newsize;
    } else {
        /* Double size of array */
        i = u_sess->libpq_cxt.cookies_size;
        newsize = u_sess->libpq_cxt.cookies_size * 2;
        u_sess->libpq_cxt.cookies =
            (LargeObjectDesc**)repalloc(u_sess->libpq_cxt.cookies, newsize * sizeof(LargeObjectDesc*));

        rc = memset_s(u_sess->libpq_cxt.cookies + u_sess->libpq_cxt.cookies_size,
            (newsize - u_sess->libpq_cxt.cookies_size) * sizeof(LargeObjectDesc*),
            0,
            (newsize - u_sess->libpq_cxt.cookies_size) * sizeof(LargeObjectDesc*));
        securec_check(rc, "\0", "\0");

        u_sess->libpq_cxt.cookies_size = newsize;
    }

    Assert(u_sess->libpq_cxt.cookies[i] == NULL);
    u_sess->libpq_cxt.cookies[i] = lobjCookie;
    return i;
}

static void deleteLOfd(int fd)
{
    u_sess->libpq_cxt.cookies[fd] = NULL;
}
