/*-------------------------------------------------------------------------
 *
 * dsm.c
 *	  manage dynamic shared memory segments
 *
 * This file provides a set of services to make programming with dynamic
 * shared memory segments more convenient.  Unlike the low-level
 * facilities provided by dsm_impl.h and dsm_impl.c, mappings and segments
 * created using this module will be cleaned up automatically.  Mappings
 * will be removed when the resource owner under which they were created
 * is cleaned up, unless dsm_pin_mapping() is used, in which case they
 * have session lifespan.  Segments will be removed when there are no
 * remaining mappings, or at postmaster shutdown in any case.  After a
 * hard postmaster crash, remaining segments will be removed, if they
 * still exist, at the next postmaster startup.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/dsm.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#ifndef WIN32
#include <sys/mman.h>
#endif
#include <sys/stat.h>
#ifdef HAVE_SYS_IPC_H
#include <sys/ipc.h>
#endif
#ifdef HAVE_SYS_SHM_H
#include <sys/shm.h>
#endif

#include "lib/ilist.h"
#include "miscadmin.h"

#include "extension.h"
#include "tsdb.h"
#include "storage/smgr/fd.h"
#include "storage/ipc.h"
#include "storage/lwlocknames.h"
#include "storage/lock/lwlock.h"
#include "storage/pg_shmem.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "postmaster/postmaster.h"
#include "tsdb_dsm.h"

#define PG_DYNSHMEM_CONTROL_MAGIC		0x9a503d32

/*
 * There's no point in getting too cheap here, because the minimum allocation
 * is one OS page, which is probably at least 4KB and could easily be as high
 * as 64KB.  Each currently sizeof(dsm_control_item), currently 8 bytes.
 */
#define PG_DYNSHMEM_FIXED_SLOTS			64
#define PG_DYNSHMEM_SLOTS_PER_BACKEND	2

#define INVALID_CONTROL_SLOT		((uint32) -1)



/* Backend-local tracking for on-detach callbacks. */
typedef struct dsm_segment_detach_callback
{
	on_dsm_detach_callback function;
	Datum		arg;
	slist_node	node;
} dsm_segment_detach_callback;

/* Shared-memory state for a dynamic shared memory segment. */
typedef struct dsm_control_item
{
	dsm_handle	handle;
	uint32		refcnt;			/* 2+ = active, 1 = moribund, 0 = gone */
} dsm_control_item;

/* Layout of the dynamic shared memory control segment. */
typedef struct dsm_control_header
{
	uint32		magic;
	uint32		nitems;
	uint32		maxitems;
	dsm_control_item item[FLEXIBLE_ARRAY_MEMBER];
} dsm_control_header;

static void dsm_cleanup_for_mmap(void);
static void dsm_postmaster_shutdown(int code, Datum arg);
static dsm_segment *dsm_create_descriptor(void);
static bool dsm_control_segment_sane(dsm_control_header *control,
						 Size mapped_size);
static uint64 dsm_control_bytes_needed(uint32 nitems);

/* Has this backend initialized the dynamic shared memory system yet? */
static bool dsm_init_done = false;

/*
 * List of dynamic shared memory segments used by this backend.
 *
 * At process exit time, we must decrement the reference count of each
 * segment we have attached; this list makes it possible to find all such
 * segments.
 *
 * This list should always be empty in the postmaster.  We could probably
 * allow the postmaster to map dynamic shared memory segments before it
 * begins to start child processes, provided that each process adjusted
 * the reference counts for those segments in the control segment at
 * startup time, but there's no obvious need for such a facility, which
 * would also be complex to handle in the EXEC_BACKEND case.  Once the
 * postmaster has begun spawning children, there's an additional problem:
 * each new mapping would require an update to the control segment,
 * which requires locking, in which the postmaster must not be involved.
 */
static dlist_head dsm_segment_list = DLIST_STATIC_INIT(dsm_segment_list);

/*
 * Control segment information.
 *
 * Unlike ordinary shared memory segments, the control segment is not
 * reference counted; instead, it lasts for the postmaster's entire
 * life cycle.  For simplicity, it doesn't have a dsm_segment object either.
 */
static dsm_handle dsm_control_handle;
static dsm_control_header *dsm_control;
static Size dsm_control_mapped_size = 0;
static void *dsm_control_impl_private = NULL;

/*
 * Start up the dynamic shared memory system.
 *
 * This is called just once during each cluster lifetime, at postmaster
 * startup time.
 */
void
dsm_postmaster_startup(PGShmemHeader *shim)
{
	void	   *dsm_control_address = NULL;
	uint32		maxitems;
	Size		segsize;

	Assert(!IsUnderPostmaster);

	/* If dynamic shared memory is disabled, there's nothing to do. */
	if (dynamic_shared_memory_type == DSM_IMPL_NONE)
		return;

	/*
	 * If we're using the mmap implementations, clean up any leftovers.
	 * Cleanup isn't needed on Windows, and happens earlier in startup for
	 * POSIX and System V shared memory, via a direct call to
	 * dsm_cleanup_using_control_segment.
	 */
	if (dynamic_shared_memory_type == DSM_IMPL_MMAP)
		dsm_cleanup_for_mmap();

	/* Determine size for new control segment. */
	maxitems = PG_DYNSHMEM_FIXED_SLOTS
		+ PG_DYNSHMEM_SLOTS_PER_BACKEND * g_instance.shmem_cxt.MaxBackends;
	elog(DEBUG2, "dynamic shared memory system will support %u segments",
		 maxitems);
	segsize = dsm_control_bytes_needed(maxitems);

	/*
	 * Loop until we find an unused identifier for the new control segment. We
	 * sometimes use 0 as a sentinel value indicating that no control segment
	 * is known to exist, so avoid using that value for a real control
	 * segment.
	 */
	for (;;)
	{
		Assert(dsm_control_address == NULL);
		Assert(dsm_control_mapped_size == 0);
		dsm_control_handle = random();
		if (dsm_control_handle == 0)
			continue;
		if (dsm_impl_op(DSM_OP_CREATE, dsm_control_handle, segsize,
						&dsm_control_impl_private, &dsm_control_address,
						&dsm_control_mapped_size, ERROR))
			break;
	}
	dsm_control =(dsm_control_header *) dsm_control_address;
	on_shmem_exit(dsm_postmaster_shutdown, PointerGetDatum(shim));
	elog(DEBUG2,
		 "created dynamic shared memory control segment %u (%zu bytes)",
		 dsm_control_handle, segsize);

	/* Initialize control segment. */
	dsm_control->magic = PG_DYNSHMEM_CONTROL_MAGIC;
	dsm_control->nitems = 0;
	dsm_control->maxitems = maxitems;
}

/*
 * Determine whether the control segment from the previous postmaster
 * invocation still exists.  If so, remove the dynamic shared memory
 * segments to which it refers, and then the control segment itself.
 */
void
dsm_cleanup_using_control_segment(dsm_handle old_control_handle)
{
	void	   *mapped_address = NULL;
	void	   *junk_mapped_address = NULL;
	void	   *impl_private = NULL;
	void	   *junk_impl_private = NULL;
	Size		mapped_size = 0;
	Size		junk_mapped_size = 0;
	uint32		nitems;
	uint32		i;
	dsm_control_header *old_control;

	/* If dynamic shared memory is disabled, there's nothing to do. */
	if (dynamic_shared_memory_type == DSM_IMPL_NONE)
		return;

	/*
	 * Try to attach the segment.  If this fails, it probably just means that
	 * the operating system has been rebooted and the segment no longer
	 * exists, or an unrelated process has used the same shm ID.  So just fall
	 * out quietly.
	 */
	if (!dsm_impl_op(DSM_OP_ATTACH, old_control_handle, 0, &impl_private,
					 &mapped_address, &mapped_size, DEBUG1))
		return;

	/*
	 * We've managed to reattach it, but the contents might not be sane. If
	 * they aren't, we disregard the segment after all.
	 */
	old_control = (dsm_control_header *) mapped_address;
	if (!dsm_control_segment_sane(old_control, mapped_size))
	{
		dsm_impl_op(DSM_OP_DETACH, old_control_handle, 0, &impl_private,
					&mapped_address, &mapped_size, LOG);
		return;
	}

	/*
	 * OK, the control segment looks basically valid, so we can use it to get
	 * a list of segments that need to be removed.
	 */
	nitems = old_control->nitems;
	for (i = 0; i < nitems; ++i)
	{
		dsm_handle	handle;
		uint32		refcnt;

		/* If the reference count is 0, the slot is actually unused. */
		refcnt = old_control->item[i].refcnt;
		if (refcnt == 0)
			continue;

		/* Log debugging information. */
		handle = old_control->item[i].handle;
		elog(DEBUG2, "cleaning up orphaned dynamic shared memory with ID %u (reference count %u)",
			 handle, refcnt);

		/* Destroy the referenced segment. */
		dsm_impl_op(DSM_OP_DESTROY, handle, 0, &junk_impl_private,
					&junk_mapped_address, &junk_mapped_size, LOG);
	}

	/* Destroy the old control segment, too. */
	elog(DEBUG2,
		 "cleaning up dynamic shared memory control segment with ID %u",
		 old_control_handle);
	dsm_impl_op(DSM_OP_DESTROY, old_control_handle, 0, &impl_private,
				&mapped_address, &mapped_size, LOG);
}

/*
 * When we're using the mmap shared memory implementation, "shared memory"
 * segments might even manage to survive an operating system reboot.
 * But there's no guarantee as to exactly what will survive: some segments
 * may survive, and others may not, and the contents of some may be out
 * of date.  In particular, the control segment may be out of date, so we
 * can't rely on it to figure out what to remove.  However, since we know
 * what directory contains the files we used as shared memory, we can simply
 * scan the directory and blow everything away that shouldn't be there.
 */
static void
dsm_cleanup_for_mmap(void)
{
	DIR		   *dir;
	struct dirent *dent;

	/* Open the directory; can't use AllocateDir in postmaster. */
	if ((dir = AllocateDir(PG_DYNSHMEM_DIR)) == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open directory \"%s\": %m",
						PG_DYNSHMEM_DIR)));

	/* Scan for something with a name of the correct format. */
	while ((dent = ReadDir(dir, PG_DYNSHMEM_DIR)) != NULL)
	{
		if (strncmp(dent->d_name, PG_DYNSHMEM_MMAP_FILE_PREFIX,
					strlen(PG_DYNSHMEM_MMAP_FILE_PREFIX)) == 0)
		{
			char		buf[MAXPGPATH];

			snprintf(buf, MAXPGPATH, PG_DYNSHMEM_DIR "/%s", dent->d_name);

			elog(DEBUG2, "removing file \"%s\"", buf);

			/* We found a matching file; so remove it. */
			if (unlink(buf) != 0)
			{
				int			save_errno;

				save_errno = errno;
				closedir(dir);
				errno = save_errno;

				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not remove file \"%s\": %m", buf)));
			}
		}
	}

	/* Cleanup complete. */
	FreeDir(dir);
}

/*
 * At shutdown time, we iterate over the control segment and remove all
 * remaining dynamic shared memory segments.  We avoid throwing errors here;
 * the postmaster is shutting down either way, and this is just non-critical
 * resource cleanup.
 */
static void
dsm_postmaster_shutdown(int code, Datum arg)
{
	uint32		nitems;
	uint32		i;
	void	   *dsm_control_address;
	void	   *junk_mapped_address = NULL;
	void	   *junk_impl_private = NULL;
	Size		junk_mapped_size = 0;
	PGShmemHeader *shim = (PGShmemHeader *) DatumGetPointer(arg);

	/*
	 * If some other backend exited uncleanly, it might have corrupted the
	 * control segment while it was dying.  In that case, we warn and ignore
	 * the contents of the control segment.  This may end up leaving behind
	 * stray shared memory segments, but there's not much we can do about that
	 * if the metadata is gone.
	 */
	nitems = dsm_control->nitems;
	if (!dsm_control_segment_sane(dsm_control, dsm_control_mapped_size))
	{
		ereport(LOG,
				(errmsg("dynamic shared memory control segment is corrupt")));
		return;
	}

	/* Remove any remaining segments. */
	for (i = 0; i < nitems; ++i)
	{
		dsm_handle	handle;

		/* If the reference count is 0, the slot is actually unused. */
		if (dsm_control->item[i].refcnt == 0)
			continue;

		/* Log debugging information. */
		handle = dsm_control->item[i].handle;
		elog(DEBUG2, "cleaning up orphaned dynamic shared memory with ID %u",
			 handle);

		/* Destroy the segment. */
		dsm_impl_op(DSM_OP_DESTROY, handle, 0, &junk_impl_private,
					&junk_mapped_address, &junk_mapped_size, LOG);
	}

	/* Remove the control segment itself. */
	elog(DEBUG2,
		 "cleaning up dynamic shared memory control segment with ID %u",
		 dsm_control_handle);
	dsm_control_address = dsm_control;
	dsm_impl_op(DSM_OP_DESTROY, dsm_control_handle, 0,
				&dsm_control_impl_private, &dsm_control_address,
				&dsm_control_mapped_size, LOG);
	dsm_control =(dsm_control_header *) dsm_control_address;
}

/*
 * Prepare this backend for dynamic shared memory usage.  Under EXEC_BACKEND,
 * we must reread the state file and map the control segment; in other cases,
 * we'll have inherited the postmaster's mapping and global variables.
 */
static void
dsm_backend_startup(void)
{
    if (!ts_extension_is_loaded()) {
        return;
    }
	/* If dynamic shared memory is disabled, reject this. */
	if (dynamic_shared_memory_type == DSM_IMPL_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Deleting plug-ins timescaledb is temporarily not supported"),
				 errhint("Set dynamic_shared_memory_type to a value other than \"none\".")));

#ifdef EXEC_BACKEND
	{
		void	   *control_address = NULL;

		/* Attach control segment. */
		Assert(dsm_control_handle != 0);
		dsm_impl_op(DSM_OP_ATTACH, dsm_control_handle, 0,
					&dsm_control_impl_private, &control_address,
					&dsm_control_mapped_size, ERROR);
		dsm_control =(dsm_control_header *) control_address;
		/* If control segment doesn't look sane, something is badly wrong. */
		if (!dsm_control_segment_sane(dsm_control, dsm_control_mapped_size))
		{
			dsm_impl_op(DSM_OP_DETACH, dsm_control_handle, 0,
						&dsm_control_impl_private, &control_address,
						&dsm_control_mapped_size, WARNING);
			ereport(FATAL,
					(errcode(ERRCODE_INTERNAL_ERROR),
			  errmsg("dynamic shared memory control segment is not valid")));
		}
	}
#endif

	dsm_init_done = true;
}

#ifdef EXEC_BACKEND
/*
 * When running under EXEC_BACKEND, we get a callback here when the main
 * shared memory segment is re-attached, so that we can record the control
 * handle retrieved from it.
 */
void
dsm_set_control_handle(dsm_handle h)
{
	Assert(dsm_control_handle == 0 && h != 0);
	dsm_control_handle = h;
}
#endif

/*
 * Create a new dynamic shared memory segment.
 */

#define DynamicSharedMemoryControlLock (&t_thrd.shemem_ptr_cxt.mainLWLockArray[34].lock)
dsm_segment *
dsm_create(Size size, int flags)
{
	dsm_segment *seg;
	uint32		i;
	uint32		nitems;

	/* Unsafe in postmaster (and pointless in a stand-alone backend). */
	Assert(IsUnderPostmaster);

	if (!dsm_init_done)
		dsm_backend_startup();

	/* Create a new segment descriptor. */
	seg = dsm_create_descriptor();

	/* Loop until we find an unused segment identifier. */
	for (;;)
	{
		Assert(seg->mapped_address == NULL && seg->mapped_size == 0);
		seg->handle = random();
		if (dsm_impl_op(DSM_OP_CREATE, seg->handle, size, &seg->impl_private,
						&seg->mapped_address, &seg->mapped_size, ERROR))
			break;
	}

	/* Lock the control segment so we can register the new segment. */
	LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);

	/* Search the control segment for an unused slot. */
	nitems = dsm_control->nitems;
	for (i = 0; i < nitems; ++i)
	{
		if (dsm_control->item[i].refcnt == 0)
		{
			dsm_control->item[i].handle = seg->handle;
			/* refcnt of 1 triggers destruction, so start at 2 */
			dsm_control->item[i].refcnt = 2;
			seg->control_slot = i;
			LWLockRelease(DynamicSharedMemoryControlLock);
			return seg;
		}
	}

	/* Verify that we can support an additional mapping. */
	if (nitems >= dsm_control->maxitems)
	{
		if ((flags & DSM_CREATE_NULL_IF_MAXSEGMENTS) != 0)
		{
			LWLockRelease(DynamicSharedMemoryControlLock);
			dsm_impl_op(DSM_OP_DESTROY, seg->handle, 0, &seg->impl_private,
						&seg->mapped_address, &seg->mapped_size, WARNING);
			if (seg->resowner != NULL)
				ResourceOwnerForgetDSM(seg->resowner, seg);
			dlist_delete(&seg->node);
			pfree(seg);
			return NULL;
		}
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("too many dynamic shared memory segments")));
	}

	/* Enter the handle into a new array slot. */
	dsm_control->item[nitems].handle = seg->handle;
	/* refcnt of 1 triggers destruction, so start at 2 */
	dsm_control->item[nitems].refcnt = 2;
	seg->control_slot = nitems;
	dsm_control->nitems++;
	LWLockRelease(DynamicSharedMemoryControlLock);

	return seg;
}

/*
 * Attach a dynamic shared memory segment.
 *
 * See comments for dsm_segment_handle() for an explanation of how this
 * is intended to be used.
 *
 * This function will return NULL if the segment isn't known to the system.
 * This can happen if we're asked to attach the segment, but then everyone
 * else detaches it (causing it to be destroyed) before we get around to
 * attaching it.
 */
dsm_segment *
dsm_attach(dsm_handle h)
{
	dsm_segment *seg;
	dlist_iter	iter;
	uint32		i;
	uint32		nitems;

	/* Unsafe in postmaster (and pointless in a stand-alone backend). */
	Assert(IsUnderPostmaster);

	if (!dsm_init_done)
		dsm_backend_startup();

	/*
	 * Since this is just a debugging cross-check, we could leave it out
	 * altogether, or include it only in assert-enabled builds.  But since the
	 * list of attached segments should normally be very short, let's include
	 * it always for right now.
	 *
	 * If you're hitting this error, you probably want to attempt to find an
	 * existing mapping via dsm_find_mapping() before calling dsm_attach() to
	 * create a new one.
	 */
	dlist_foreach(iter, &dsm_segment_list)
	{
		seg = dlist_container(dsm_segment, node, iter.cur);
		if (seg->handle == h)
			elog(ERROR, "can't attach the same segment more than once");
	}

	/* Create a new segment descriptor. */
	seg = dsm_create_descriptor();
	seg->handle = h;

	/* Bump reference count for this segment in shared memory. */
	LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
	nitems = dsm_control->nitems;
	for (i = 0; i < nitems; ++i)
	{
		/* If the reference count is 0, the slot is actually unused. */
		if (dsm_control->item[i].refcnt == 0)
			continue;

		/* If the handle doesn't match, it's not the slot we want. */
		if (dsm_control->item[i].handle != seg->handle)
			continue;

		/*
		 * If the reference count is 1, the slot is still in use, but the
		 * segment is in the process of going away.  Treat that as if we
		 * didn't find a match.
		 */
		if (dsm_control->item[i].refcnt == 1)
			break;

		/* Otherwise we've found a match. */
		dsm_control->item[i].refcnt++;
		seg->control_slot = i;
		break;
	}
	LWLockRelease(DynamicSharedMemoryControlLock);

	/*
	 * If we didn't find the handle we're looking for in the control segment,
	 * it probably means that everyone else who had it mapped, including the
	 * original creator, died before we got to this point. It's up to the
	 * caller to decide what to do about that.
	 */
	if (seg->control_slot == INVALID_CONTROL_SLOT)
	{
		dsm_detach(seg);
		return NULL;
	}

	/* Here's where we actually try to map the segment. */
	dsm_impl_op(DSM_OP_ATTACH, seg->handle, 0, &seg->impl_private,
				&seg->mapped_address, &seg->mapped_size, ERROR);

	return seg;
}

/*
 * At backend shutdown time, detach any segments that are still attached.
 * (This is similar to dsm_detach_all, except that there's no reason to
 * unmap the control segment before exiting, so we don't bother.)
 */
void
dsm_backend_shutdown(void)
{
	while (!dlist_is_empty(&dsm_segment_list))
	{
		dsm_segment *seg;

		seg = dlist_head_element(dsm_segment, node, &dsm_segment_list);
		dsm_detach(seg);
	}
}

/*
 * Detach all shared memory segments, including the control segments.  This
 * should be called, along with PGSharedMemoryDetach, in processes that
 * might inherit mappings but are not intended to be connected to dynamic
 * shared memory.
 */
void
dsm_detach_all(void)
{
	void	   *control_address = dsm_control;

	while (!dlist_is_empty(&dsm_segment_list))
	{
		dsm_segment *seg;

		seg = dlist_head_element(dsm_segment, node, &dsm_segment_list);
		dsm_detach(seg);
	}

	if (control_address != NULL)
		dsm_impl_op(DSM_OP_DETACH, dsm_control_handle, 0,
					&dsm_control_impl_private, &control_address,
					&dsm_control_mapped_size, ERROR);
}

/*
 * Resize an existing shared memory segment.
 *
 * This may cause the shared memory segment to be remapped at a different
 * address.  For the caller's convenience, we return the mapped address.
 */
void *
dsm_resize(dsm_segment *seg, Size size)
{
	Assert(seg->control_slot != INVALID_CONTROL_SLOT);
	dsm_impl_op(DSM_OP_RESIZE, seg->handle, size, &seg->impl_private,
				&seg->mapped_address, &seg->mapped_size, ERROR);
	return seg->mapped_address;
}

/*
 * Remap an existing shared memory segment.
 *
 * This is intended to be used when some other process has extended the
 * mapping using dsm_resize(), but we've still only got the initial
 * portion mapped.  Since this might change the address at which the
 * segment is mapped, we return the new mapped address.
 */
void *
dsm_remap(dsm_segment *seg)
{
	dsm_impl_op(DSM_OP_ATTACH, seg->handle, 0, &seg->impl_private,
				&seg->mapped_address, &seg->mapped_size, ERROR);

	return seg->mapped_address;
}

/*
 * Detach from a shared memory segment, destroying the segment if we
 * remove the last reference.
 *
 * This function should never fail.  It will often be invoked when aborting
 * a transaction, and a further error won't serve any purpose.  It's not a
 * complete disaster if we fail to unmap or destroy the segment; it means a
 * resource leak, but that doesn't necessarily preclude further operations.
 */
void
dsm_detach(dsm_segment *seg)
{
	/*
	 * Invoke registered callbacks.  Just in case one of those callbacks
	 * throws a further error that brings us back here, pop the callback
	 * before invoking it, to avoid infinite error recursion.
	 */
	while (!slist_is_empty(&seg->on_detach))
	{
		slist_node *node;
		dsm_segment_detach_callback *cb;
		on_dsm_detach_callback function;
		Datum		arg;

		node = slist_pop_head_node(&seg->on_detach);
		cb = slist_container(dsm_segment_detach_callback, node, node);
		function = cb->function;
		arg = cb->arg;
		pfree(cb);

		function(seg, arg);
	}

	/*
	 * Try to remove the mapping, if one exists.  Normally, there will be, but
	 * maybe not, if we failed partway through a create or attach operation.
	 * We remove the mapping before decrementing the reference count so that
	 * the process that sees a zero reference count can be certain that no
	 * remaining mappings exist.  Even if this fails, we pretend that it
	 * works, because retrying is likely to fail in the same way.
	 */
	if (seg->mapped_address != NULL)
	{
		dsm_impl_op(DSM_OP_DETACH, seg->handle, 0, &seg->impl_private,
					&seg->mapped_address, &seg->mapped_size, WARNING);
		seg->impl_private = NULL;
		seg->mapped_address = NULL;
		seg->mapped_size = 0;
	}

	/* Reduce reference count, if we previously increased it. */
	if (seg->control_slot != INVALID_CONTROL_SLOT)
	{
		uint32		refcnt;
		uint32		control_slot = seg->control_slot;

		LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
		Assert(dsm_control->item[control_slot].handle == seg->handle);
		Assert(dsm_control->item[control_slot].refcnt > 1);
		refcnt = --dsm_control->item[control_slot].refcnt;
		seg->control_slot = INVALID_CONTROL_SLOT;
		LWLockRelease(DynamicSharedMemoryControlLock);

		/* If new reference count is 1, try to destroy the segment. */
		if (refcnt == 1)
		{
			/*
			 * If we fail to destroy the segment here, or are killed before we
			 * finish doing so, the reference count will remain at 1, which
			 * will mean that nobody else can attach to the segment.  At
			 * postmaster shutdown time, or when a new postmaster is started
			 * after a hard kill, another attempt will be made to remove the
			 * segment.
			 *
			 * The main case we're worried about here is being killed by a
			 * signal before we can finish removing the segment.  In that
			 * case, it's important to be sure that the segment still gets
			 * removed. If we actually fail to remove the segment for some
			 * other reason, the postmaster may not have any better luck than
			 * we did.  There's not much we can do about that, though.
			 */
			if (dsm_impl_op(DSM_OP_DESTROY, seg->handle, 0, &seg->impl_private,
							&seg->mapped_address, &seg->mapped_size, WARNING))
			{
				LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
				Assert(dsm_control->item[control_slot].handle == seg->handle);
				Assert(dsm_control->item[control_slot].refcnt == 1);
				dsm_control->item[control_slot].refcnt = 0;
				LWLockRelease(DynamicSharedMemoryControlLock);
			}
		}
	}

	/* Clean up our remaining backend-private data structures. */
	if (seg->resowner != NULL)
		ResourceOwnerForgetDSM(seg->resowner, seg);
	dlist_delete(&seg->node);
	pfree(seg);
}

/*
 * Keep a dynamic shared memory mapping until end of session.
 *
 * By default, mappings are owned by the current resource owner, which
 * typically means they stick around for the duration of the current query
 * only.
 */
void
dsm_pin_mapping(dsm_segment *seg)
{
	if (seg->resowner != NULL)
	{
		ResourceOwnerForgetDSM(seg->resowner, seg);
		seg->resowner = NULL;
	}
}

/*
 * Arrange to remove a dynamic shared memory mapping at cleanup time.
 *
 * dsm_pin_mapping() can be used to preserve a mapping for the entire
 * lifetime of a process; this function reverses that decision, making
 * the segment owned by the current resource owner.  This may be useful
 * just before performing some operation that will invalidate the segment
 * for future use by this backend.
 */
void
dsm_unpin_mapping(dsm_segment *seg)
{
	Assert(seg->resowner == NULL);
	ResourceOwnerEnlargeDSMs(t_thrd.utils_cxt.CurrentResourceOwner);
	seg->resowner = t_thrd.utils_cxt.CurrentResourceOwner;
	ResourceOwnerRememberDSM(seg->resowner, seg);
}

/*
 * Keep a dynamic shared memory segment until postmaster shutdown.
 *
 * This function should not be called more than once per segment;
 * on Windows, doing so will create unnecessary handles which will
 * consume system resources to no benefit.
 *
 * Note that this function does not arrange for the current process to
 * keep the segment mapped indefinitely; if that behavior is desired,
 * dsm_pin_mapping() should be used from each process that needs to
 * retain the mapping.
 */
void
dsm_pin_segment(dsm_segment *seg)
{
	/*
	 * Bump reference count for this segment in shared memory. This will
	 * ensure that even if there is no session which is attached to this
	 * segment, it will remain until postmaster shutdown.
	 */
	LWLockAcquire(DynamicSharedMemoryControlLock, LW_EXCLUSIVE);
	dsm_control->item[seg->control_slot].refcnt++;
	LWLockRelease(DynamicSharedMemoryControlLock);

	dsm_impl_pin_segment(seg->handle, seg->impl_private);
}

/*
 * Find an existing mapping for a shared memory segment, if there is one.
 */
dsm_segment *
dsm_find_mapping(dsm_handle h)
{
	dlist_iter	iter;
	dsm_segment *seg;

	dlist_foreach(iter, &dsm_segment_list)
	{
		seg = dlist_container(dsm_segment, node, iter.cur);
		if (seg->handle == h)
			return seg;
	}

	return NULL;
}

/*
 * Get the address at which a dynamic shared memory segment is mapped.
 */
void *
dsm_segment_address(dsm_segment *seg)
{
	Assert(seg->mapped_address != NULL);
	return seg->mapped_address;
}

/*
 * Get the size of a mapping.
 */
Size
dsm_segment_map_length(dsm_segment *seg)
{
	Assert(seg->mapped_address != NULL);
	return seg->mapped_size;
}

/*
 * Get a handle for a mapping.
 *
 * To establish communication via dynamic shared memory between two backends,
 * one of them should first call dsm_create() to establish a new shared
 * memory mapping.  That process should then call dsm_segment_handle() to
 * obtain a handle for the mapping, and pass that handle to the
 * coordinating backend via some means (e.g. bgw_main_arg, or via the
 * main shared memory segment).  The recipient, once in possession of the
 * handle, should call dsm_attach().
 */
dsm_handle
dsm_segment_handle(dsm_segment *seg)
{
	return seg->handle;
}

/*
 * Register an on-detach callback for a dynamic shared memory segment.
 */
void
on_dsm_detach(dsm_segment *seg, on_dsm_detach_callback function, Datum arg)
{
	dsm_segment_detach_callback *cb;

	cb =(dsm_segment_detach_callback *) MemoryContextAlloc(TopMemoryContext,
							sizeof(dsm_segment_detach_callback));
	cb->function = function;
	cb->arg = arg;
	slist_push_head(&seg->on_detach, &cb->node);
}

/*
 * Unregister an on-detach callback for a dynamic shared memory segment.
 */
void
cancel_on_dsm_detach(dsm_segment *seg, on_dsm_detach_callback function,
					 Datum arg)
{
	slist_mutable_iter iter;

	slist_foreach_modify(iter, &seg->on_detach)
	{
		dsm_segment_detach_callback *cb;

		cb = slist_container(dsm_segment_detach_callback, node, iter.cur);
		if (cb->function == function && cb->arg == arg)
		{
			slist_delete_current(&iter);
			pfree(cb);
			break;
		}
	}
}

/*
 * Discard all registered on-detach callbacks without executing them.
 */
void
reset_on_dsm_detach(void)
{
	dlist_iter	iter;

	dlist_foreach(iter, &dsm_segment_list)
	{
		dsm_segment *seg = dlist_container(dsm_segment, node, iter.cur);

		/* Throw away explicit on-detach actions one by one. */
		while (!slist_is_empty(&seg->on_detach))
		{
			slist_node *node;
			dsm_segment_detach_callback *cb;

			node = slist_pop_head_node(&seg->on_detach);
			cb = slist_container(dsm_segment_detach_callback, node, node);
			pfree(cb);
		}

		/*
		 * Decrementing the reference count is a sort of implicit on-detach
		 * action; make sure we don't do that, either.
		 */
		seg->control_slot = INVALID_CONTROL_SLOT;
	}
}

/*
 * Create a segment descriptor.
 */
static dsm_segment *
dsm_create_descriptor(void)
{
	dsm_segment *seg;

	ResourceOwnerEnlargeDSMs(t_thrd.utils_cxt.CurrentResourceOwner);

	seg =(dsm_segment *) MemoryContextAlloc(TopMemoryContext, sizeof(dsm_segment));
	dlist_push_head(&dsm_segment_list, &seg->node);

	/* seg->handle must be initialized by the caller */
	seg->control_slot = INVALID_CONTROL_SLOT;
	seg->impl_private = NULL;
	seg->mapped_address = NULL;
	seg->mapped_size = 0;

	seg->resowner = t_thrd.utils_cxt.CurrentResourceOwner;
	ResourceOwnerRememberDSM(t_thrd.utils_cxt.CurrentResourceOwner, seg);

	slist_init(&seg->on_detach);

	return seg;
}

/*
 * Sanity check a control segment.
 *
 * The goal here isn't to detect everything that could possibly be wrong with
 * the control segment; there's not enough information for that.  Rather, the
 * goal is to make sure that someone can iterate over the items in the segment
 * without overrunning the end of the mapping and crashing.  We also check
 * the magic number since, if that's messed up, this may not even be one of
 * our segments at all.
 */
static bool
dsm_control_segment_sane(dsm_control_header *control, Size mapped_size)
{
	if (mapped_size < offsetof(dsm_control_header, item))
		return false;			/* Mapped size too short to read header. */
	if (control->magic != PG_DYNSHMEM_CONTROL_MAGIC)
		return false;			/* Magic number doesn't match. */
	if (dsm_control_bytes_needed(control->maxitems) > mapped_size)
		return false;			/* Max item count won't fit in map. */
	if (control->nitems > control->maxitems)
		return false;			/* Overfull. */
	return true;
}

/*
 * Compute the number of control-segment bytes needed to store a given
 * number of items.
 */
static uint64
dsm_control_bytes_needed(uint32 nitems)
{
	return offsetof(dsm_control_header, item)
		+sizeof(dsm_control_item) * (uint64) nitems;
}





#ifdef USE_DSM_POSIX
static bool dsm_impl_posix(dsm_op op, dsm_handle handle, Size request_size,
			   void **impl_private, void **mapped_address,
			   Size *mapped_size, int elevel);
#endif
#ifdef USE_DSM_SYSV
static bool dsm_impl_sysv(dsm_op op, dsm_handle handle, Size request_size,
			  void **impl_private, void **mapped_address,
			  Size *mapped_size, int elevel);
#endif
#ifdef USE_DSM_WINDOWS
static bool dsm_impl_windows(dsm_op op, dsm_handle handle, Size request_size,
				 void **impl_private, void **mapped_address,
				 Size *mapped_size, int elevel);
#endif
#ifdef USE_DSM_MMAP
static bool dsm_impl_mmap(dsm_op op, dsm_handle handle, Size request_size,
			  void **impl_private, void **mapped_address,
			  Size *mapped_size, int elevel);
#endif
static int	errcode_for_dynamic_shared_memory(void);

const struct config_enum_entry dynamic_shared_memory_options[] = {
#ifdef USE_DSM_POSIX
	{"posix", DSM_IMPL_POSIX, false},
#endif
#ifdef USE_DSM_SYSV
	{"sysv", DSM_IMPL_SYSV, false},
#endif
#ifdef USE_DSM_WINDOWS
	{"windows", DSM_IMPL_WINDOWS, false},
#endif
#ifdef USE_DSM_MMAP
	{"mmap", DSM_IMPL_MMAP, false},
#endif
	{"none", DSM_IMPL_NONE, false},
	{NULL, 0, false}
};

/* Implementation selector. */
int			dynamic_shared_memory_type;

/* Size of buffer to be used for zero-filling. */
#define ZBUFFER_SIZE				8192

#define SEGMENT_NAME_PREFIX			"Global/PostgreSQL"

/*------
 * Perform a low-level shared memory operation in a platform-specific way,
 * as dictated by the selected implementation.  Each implementation is
 * required to implement the following primitives.
 *
 * DSM_OP_CREATE.  Create a segment whose size is the request_size and
 * map it.
 *
 * DSM_OP_ATTACH.  Map the segment, whose size must be the request_size.
 * The segment may already be mapped; any existing mapping should be removed
 * before creating a new one.
 *
 * DSM_OP_DETACH.  Unmap the segment.
 *
 * DSM_OP_RESIZE.  Resize the segment to the given request_size and
 * remap the segment at that new size.
 *
 * DSM_OP_DESTROY.  Unmap the segment, if it is mapped.  Destroy the
 * segment.
 *
 * Arguments:
 *	 op: The operation to be performed.
 *	 handle: The handle of an existing object, or for DSM_OP_CREATE, the
 *	   a new handle the caller wants created.
 *	 request_size: For DSM_OP_CREATE, the requested size.  For DSM_OP_RESIZE,
 *	   the new size.  Otherwise, 0.
 *	 impl_private: Private, implementation-specific data.  Will be a pointer
 *	   to NULL for the first operation on a shared memory segment within this
 *	   backend; thereafter, it will point to the value to which it was set
 *	   on the previous call.
 *	 mapped_address: Pointer to start of current mapping; pointer to NULL
 *	   if none.  Updated with new mapping address.
 *	 mapped_size: Pointer to size of current mapping; pointer to 0 if none.
 *	   Updated with new mapped size.
 *	 elevel: Level at which to log errors.
 *
 * Return value: true on success, false on failure.  When false is returned,
 * a message should first be logged at the specified elevel, except in the
 * case where DSM_OP_CREATE experiences a name collision, which should
 * silently return false.
 *-----
 */
bool
dsm_impl_op(dsm_op op, dsm_handle handle, Size request_size,
			void **impl_private, void **mapped_address, Size *mapped_size,
			int elevel)
{
	Assert(op == DSM_OP_CREATE || op == DSM_OP_RESIZE || request_size == 0);
	Assert((op != DSM_OP_CREATE && op != DSM_OP_ATTACH) ||
		   (*mapped_address == NULL && *mapped_size == 0));

	switch (dynamic_shared_memory_type)
	{
#ifdef USE_DSM_POSIX
		case DSM_IMPL_POSIX:
			return dsm_impl_posix(op, handle, request_size, impl_private,
								  mapped_address, mapped_size, elevel);
#endif
#ifdef USE_DSM_SYSV
		case DSM_IMPL_SYSV:
			return dsm_impl_sysv(op, handle, request_size, impl_private,
								 mapped_address, mapped_size, elevel);
#endif
#ifdef USE_DSM_WINDOWS
		case DSM_IMPL_WINDOWS:
			return dsm_impl_windows(op, handle, request_size, impl_private,
									mapped_address, mapped_size, elevel);
#endif
#ifdef USE_DSM_MMAP
		case DSM_IMPL_MMAP:
			return dsm_impl_mmap(op, handle, request_size, impl_private,
								 mapped_address, mapped_size, elevel);
#endif
		default:
			elog(ERROR, "unexpected dynamic shared memory type: %d",
				 dynamic_shared_memory_type);
			return false;
	}
}

/*
 * Does the current dynamic shared memory implementation support resizing
 * segments?  (The answer here could be platform-dependent in the future,
 * since AIX allows shmctl(shmid, SHM_RESIZE, &buffer), though you apparently
 * can't resize segments to anything larger than 256MB that way.  For now,
 * we keep it simple.)
 */
bool
dsm_impl_can_resize(void)
{
	switch (dynamic_shared_memory_type)
	{
		case DSM_IMPL_NONE:
			return false;
		case DSM_IMPL_POSIX:
			return true;
		case DSM_IMPL_SYSV:
			return false;
		case DSM_IMPL_WINDOWS:
			return false;
		case DSM_IMPL_MMAP:
			return true;
		default:
			return false;		/* should not happen */
	}
}

#ifdef USE_DSM_POSIX
/*
 * Operating system primitives to support POSIX shared memory.
 *
 * POSIX shared memory segments are created and attached using shm_open()
 * and shm_unlink(); other operations, such as sizing or mapping the
 * segment, are performed as if the shared memory segments were files.
 *
 * Indeed, on some platforms, they may be implemented that way.  While
 * POSIX shared memory segments seem intended to exist in a flat namespace,
 * some operating systems may implement them as files, even going so far
 * to treat a request for /xyz as a request to create a file by that name
 * in the root directory.  Users of such broken platforms should select
 * a different shared memory implementation.
 */
static bool
dsm_impl_posix(dsm_op op, dsm_handle handle, Size request_size,
			   void **impl_private, void **mapped_address, Size *mapped_size,
			   int elevel)
{
	char		name[64];
	int			flags;
	int			fd;
	char	   *address;

	snprintf(name, 64, "/PostgreSQL.%u", handle);

	/* Handle teardown cases. */
	if (op == DSM_OP_DETACH || op == DSM_OP_DESTROY)
	{
		if (*mapped_address != NULL
			&& munmap(*mapped_address, *mapped_size) != 0)
		{
			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
				   errmsg("could not unmap shared memory segment \"%s\": %m",
						  name)));
			return false;
		}
		*mapped_address = NULL;
		*mapped_size = 0;
		if (op == DSM_OP_DESTROY && shm_unlink(name) != 0)
		{
			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
				  errmsg("could not remove shared memory segment \"%s\": %m",
						 name)));
			return false;
		}
		return true;
	}

	/*
	 * Create new segment or open an existing one for attach or resize.
	 *
	 * Even though we're not going through fd.c, we should be safe against
	 * running out of file descriptors, because of NUM_RESERVED_FDS.  We're
	 * only opening one extra descriptor here, and we'll close it before
	 * returning.
	 */
	flags = O_RDWR | (op == DSM_OP_CREATE ? O_CREAT | O_EXCL : 0);
	if ((fd = shm_open(name, flags, 0600)) == -1)
	{
		if (errno != EEXIST)
			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
					 errmsg("could not open shared memory segment \"%s\": %m",
							name)));
		return false;
	}

	/*
	 * If we're attaching the segment, determine the current size; if we are
	 * creating or resizing the segment, set the size to the requested value.
	 */
	if (op == DSM_OP_ATTACH)
	{
		struct stat st;

		if (fstat(fd, &st) != 0)
		{
			int			save_errno;

			/* Back out what's already been done. */
			save_errno = errno;
			close(fd);
			errno = save_errno;

			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
					 errmsg("could not stat shared memory segment \"%s\": %m",
							name)));
			return false;
		}
		request_size = st.st_size;
	}
	else if (*mapped_size != request_size && ftruncate(fd, request_size))
	{
		int			save_errno;

		/* Back out what's already been done. */
		save_errno = errno;
		close(fd);
		if (op == DSM_OP_CREATE)
			shm_unlink(name);
		errno = save_errno;

		ereport(elevel,
				(errcode_for_dynamic_shared_memory(),
				 errmsg("could not resize shared memory segment \"%s\" to %zu bytes: %m",
						name, request_size)));
		return false;
	}

	/*
	 * If we're reattaching or resizing, we must remove any existing mapping,
	 * unless we've already got the right thing mapped.
	 */
	if (*mapped_address != NULL)
	{
		if (*mapped_size == request_size)
			return true;
		if (munmap(*mapped_address, *mapped_size) != 0)
		{
			int			save_errno;

			/* Back out what's already been done. */
			save_errno = errno;
			close(fd);
			if (op == DSM_OP_CREATE)
				shm_unlink(name);
			errno = save_errno;

			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
				   errmsg("could not unmap shared memory segment \"%s\": %m",
						  name)));
			return false;
		}
		*mapped_address = NULL;
		*mapped_size = 0;
	}

	/* Map it. */
	address = mmap(NULL, request_size, PROT_READ | PROT_WRITE,
				   MAP_SHARED | MAP_HASSEMAPHORE | MAP_NOSYNC, fd, 0);
	if (address == MAP_FAILED)
	{
		int			save_errno;

		/* Back out what's already been done. */
		save_errno = errno;
		close(fd);
		if (op == DSM_OP_CREATE)
			shm_unlink(name);
		errno = save_errno;

		ereport(elevel,
				(errcode_for_dynamic_shared_memory(),
				 errmsg("could not map shared memory segment \"%s\": %m",
						name)));
		return false;
	}
	*mapped_address = address;
	*mapped_size = request_size;
	close(fd);

	return true;
}
#endif

#ifdef USE_DSM_SYSV
/*
 * Operating system primitives to support System V shared memory.
 *
 * System V shared memory segments are manipulated using shmget(), shmat(),
 * shmdt(), and shmctl().  There's no portable way to resize such
 * segments.  As the default allocation limits for System V shared memory
 * are usually quite low, the POSIX facilities may be preferable; but
 * those are not supported everywhere.
 */
static bool
dsm_impl_sysv(dsm_op op, dsm_handle handle, Size request_size,
			  void **impl_private, void **mapped_address, Size *mapped_size,
			  int elevel)
{
	key_t		key;
	int			ident;
	char	   *address;
	char		name[64];
	int		   *ident_cache;

	/* Resize is not supported for System V shared memory. */
	if (op == DSM_OP_RESIZE)
	{
		elog(elevel, "System V shared memory segments cannot be resized");
		return false;
	}

	/* Since resize isn't supported, reattach is a no-op. */
	if (op == DSM_OP_ATTACH && *mapped_address != NULL)
		return true;

	/*
	 * POSIX shared memory and mmap-based shared memory identify segments with
	 * names.  To avoid needless error message variation, we use the handle as
	 * the name.
	 */
	snprintf(name, 64, "%u", handle);

	/*
	 * The System V shared memory namespace is very restricted; names are of
	 * type key_t, which is expected to be some sort of integer data type, but
	 * not necessarily the same one as dsm_handle.  Since we use dsm_handle to
	 * identify shared memory segments across processes, this might seem like
	 * a problem, but it's really not.  If dsm_handle is bigger than key_t,
	 * the cast below might truncate away some bits from the handle the
	 * user-provided, but it'll truncate exactly the same bits away in exactly
	 * the same fashion every time we use that handle, which is all that
	 * really matters.  Conversely, if dsm_handle is smaller than key_t, we
	 * won't use the full range of available key space, but that's no big deal
	 * either.
	 *
	 * We do make sure that the key isn't negative, because that might not be
	 * portable.
	 */
	key = (key_t) handle;
	if (key < 1)				/* avoid compiler warning if type is unsigned */
		key = -key;

	/*
	 * There's one special key, IPC_PRIVATE, which can't be used.  If we end
	 * up with that value by chance during a create operation, just pretend it
	 * already exists, so that caller will retry.  If we run into it anywhere
	 * else, the caller has passed a handle that doesn't correspond to
	 * anything we ever created, which should not happen.
	 */
	if (key == IPC_PRIVATE)
	{
		if (op != DSM_OP_CREATE)
			elog(DEBUG4, "System V shared memory key may not be IPC_PRIVATE");
		errno = EEXIST;
		return false;
	}

	/*
	 * Before we can do anything with a shared memory segment, we have to map
	 * the shared memory key to a shared memory identifier using shmget(). To
	 * avoid repeated lookups, we store the key using impl_private.
	 */
	if (*impl_private != NULL)
	{
		ident_cache = (int *) *impl_private;
		ident = *ident_cache;
	}
	else
	{
		int			flags = IPCProtection;
		size_t		segsize;

		/*
		 * Allocate the memory BEFORE acquiring the resource, so that we don't
		 * leak the resource if memory allocation fails.
		 */
		ident_cache =(int *) MemoryContextAlloc(TopMemoryContext, sizeof(int));

		/*
		 * When using shmget to find an existing segment, we must pass the
		 * size as 0.  Passing a non-zero size which is greater than the
		 * actual size will result in EINVAL.
		 */
		segsize = 0;

		if (op == DSM_OP_CREATE)
		{
			flags |= IPC_CREAT | IPC_EXCL;
			segsize = request_size;
		}

		if ((ident = shmget(key, segsize, flags)) == -1)
		{
			if (errno != EEXIST)
			{
				int			save_errno = errno;

				pfree(ident_cache);
				errno = save_errno;
				ereport(elevel,
						(errcode_for_dynamic_shared_memory(),
						 errmsg("could not get shared memory segment: %m")));
			}
			return false;
		}

		*ident_cache = ident;
		*impl_private = ident_cache;
	}

	/* Handle teardown cases. */
	if (op == DSM_OP_DETACH || op == DSM_OP_DESTROY)
	{
		pfree(ident_cache);
		*impl_private = NULL;
		if (*mapped_address != NULL && shmdt(*mapped_address) != 0)
		{
			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
				   errmsg("could not unmap shared memory segment \"%s\": %m",
						  name)));
			return false;
		}
		*mapped_address = NULL;
		*mapped_size = 0;
		if (op == DSM_OP_DESTROY && shmctl(ident, IPC_RMID, NULL) < 0)
		{
			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
				  errmsg("could not remove shared memory segment \"%s\": %m",
						 name)));
			return false;
		}
		return true;
	}

	/* If we're attaching it, we must use IPC_STAT to determine the size. */
	if (op == DSM_OP_ATTACH)
	{
		struct shmid_ds shm;

		if (shmctl(ident, IPC_STAT, &shm) != 0)
		{
			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
					 errmsg("could not stat shared memory segment \"%s\": %m",
							name)));
			return false;
		}
		request_size = shm.shm_segsz;
	}

	/* Map it. */
	address =(char *) shmat(ident, NULL, PG_SHMAT_FLAGS);
	if (address == (void *) -1)
	{
		int			save_errno;

		/* Back out what's already been done. */
		save_errno = errno;
		if (op == DSM_OP_CREATE)
			shmctl(ident, IPC_RMID, NULL);
		errno = save_errno;

		ereport(elevel,
				(errcode_for_dynamic_shared_memory(),
				 errmsg("could not map shared memory segment \"%s\": %m",
						name)));
		return false;
	}
	*mapped_address = address;
	*mapped_size = request_size;

	return true;
}
#endif

#ifdef USE_DSM_WINDOWS
/*
 * Operating system primitives to support Windows shared memory.
 *
 * Windows shared memory implementation is done using file mapping
 * which can be backed by either physical file or system paging file.
 * Current implementation uses system paging file as other effects
 * like performance are not clear for physical file and it is used in similar
 * way for main shared memory in windows.
 *
 * A memory mapping object is a kernel object - they always get deleted when
 * the last reference to them goes away, either explicitly via a CloseHandle or
 * when the process containing the reference exits.
 */
static bool
dsm_impl_windows(dsm_op op, dsm_handle handle, Size request_size,
				 void **impl_private, void **mapped_address,
				 Size *mapped_size, int elevel)
{
	char	   *address;
	HANDLE		hmap;
	char		name[64];
	MEMORY_BASIC_INFORMATION info;

	/* Resize is not supported for Windows shared memory. */
	if (op == DSM_OP_RESIZE)
	{
		elog(elevel, "Windows shared memory segments cannot be resized");
		return false;
	}

	/* Since resize isn't supported, reattach is a no-op. */
	if (op == DSM_OP_ATTACH && *mapped_address != NULL)
		return true;

	/*
	 * Storing the shared memory segment in the Global\ namespace, can allow
	 * any process running in any session to access that file mapping object
	 * provided that the caller has the required access rights. But to avoid
	 * issues faced in main shared memory, we are using the naming convention
	 * similar to main shared memory. We can change here once issue mentioned
	 * in GetSharedMemName is resolved.
	 */
	snprintf(name, 64, "%s.%u", SEGMENT_NAME_PREFIX, handle);

	/*
	 * Handle teardown cases.  Since Windows automatically destroys the object
	 * when no references reamin, we can treat it the same as detach.
	 */
	if (op == DSM_OP_DETACH || op == DSM_OP_DESTROY)
	{
		if (*mapped_address != NULL
			&& UnmapViewOfFile(*mapped_address) == 0)
		{
			_dosmaperr(GetLastError());
			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
				   errmsg("could not unmap shared memory segment \"%s\": %m",
						  name)));
			return false;
		}
		if (*impl_private != NULL
			&& CloseHandle(*impl_private) == 0)
		{
			_dosmaperr(GetLastError());
			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
				  errmsg("could not remove shared memory segment \"%s\": %m",
						 name)));
			return false;
		}

		*impl_private = NULL;
		*mapped_address = NULL;
		*mapped_size = 0;
		return true;
	}

	/* Create new segment or open an existing one for attach. */
	if (op == DSM_OP_CREATE)
	{
		DWORD		size_high;
		DWORD		size_low;
		DWORD		errcode;

		/* Shifts >= the width of the type are undefined. */
#ifdef _WIN64
		size_high = request_size >> 32;
#else
		size_high = 0;
#endif
		size_low = (DWORD) request_size;

		/* CreateFileMapping might not clear the error code on success */
		SetLastError(0);

		hmap = CreateFileMapping(INVALID_HANDLE_VALUE,	/* Use the pagefile */
								 NULL,	/* Default security attrs */
								 PAGE_READWRITE,		/* Memory is read/write */
								 size_high,		/* Upper 32 bits of size */
								 size_low,		/* Lower 32 bits of size */
								 name);

		errcode = GetLastError();
		if (errcode == ERROR_ALREADY_EXISTS || errcode == ERROR_ACCESS_DENIED)
		{
			/*
			 * On Windows, when the segment already exists, a handle for the
			 * existing segment is returned.  We must close it before
			 * returning.  However, if the existing segment is created by a
			 * service, then it returns ERROR_ACCESS_DENIED. We don't do
			 * _dosmaperr here, so errno won't be modified.
			 */
			if (hmap)
				CloseHandle(hmap);
			return false;
		}

		if (!hmap)
		{
			_dosmaperr(errcode);
			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
				  errmsg("could not create shared memory segment \"%s\": %m",
						 name)));
			return false;
		}
	}
	else
	{
		hmap = OpenFileMapping(FILE_MAP_WRITE | FILE_MAP_READ,
							   FALSE,	/* do not inherit the name */
							   name);	/* name of mapping object */
		if (!hmap)
		{
			_dosmaperr(GetLastError());
			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
					 errmsg("could not open shared memory segment \"%s\": %m",
							name)));
			return false;
		}
	}

	/* Map it. */
	address = MapViewOfFile(hmap, FILE_MAP_WRITE | FILE_MAP_READ,
							0, 0, 0);
	if (!address)
	{
		int			save_errno;

		_dosmaperr(GetLastError());
		/* Back out what's already been done. */
		save_errno = errno;
		CloseHandle(hmap);
		errno = save_errno;

		ereport(elevel,
				(errcode_for_dynamic_shared_memory(),
				 errmsg("could not map shared memory segment \"%s\": %m",
						name)));
		return false;
	}

	/*
	 * VirtualQuery gives size in page_size units, which is 4K for Windows. We
	 * need size only when we are attaching, but it's better to get the size
	 * when creating new segment to keep size consistent both for
	 * DSM_OP_CREATE and DSM_OP_ATTACH.
	 */
	if (VirtualQuery(address, &info, sizeof(info)) == 0)
	{
		int			save_errno;

		_dosmaperr(GetLastError());
		/* Back out what's already been done. */
		save_errno = errno;
		UnmapViewOfFile(address);
		CloseHandle(hmap);
		errno = save_errno;

		ereport(elevel,
				(errcode_for_dynamic_shared_memory(),
				 errmsg("could not stat shared memory segment \"%s\": %m",
						name)));
		return false;
	}

	*mapped_address = address;
	*mapped_size = info.RegionSize;
	*impl_private = hmap;

	return true;
}
#endif

#ifdef USE_DSM_MMAP
/*
 * Operating system primitives to support mmap-based shared memory.
 *
 * Calling this "shared memory" is somewhat of a misnomer, because what
 * we're really doing is creating a bunch of files and mapping them into
 * our address space.  The operating system may feel obliged to
 * synchronize the contents to disk even if nothing is being paged out,
 * which will not serve us well.  The user can relocate the pg_dynshmem
 * directory to a ramdisk to avoid this problem, if available.
 */
static bool
dsm_impl_mmap(dsm_op op, dsm_handle handle, Size request_size,
			  void **impl_private, void **mapped_address, Size *mapped_size,
			  int elevel)
{
	char		name[64];
	int			flags;
	int			fd;
	char	   *address;

	snprintf(name, 64, PG_DYNSHMEM_DIR "/" PG_DYNSHMEM_MMAP_FILE_PREFIX "%u",
			 handle);

	/* Handle teardown cases. */
	if (op == DSM_OP_DETACH || op == DSM_OP_DESTROY)
	{
		if (*mapped_address != NULL
			&& munmap(*mapped_address, *mapped_size) != 0)
		{
			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
				   errmsg("could not unmap shared memory segment \"%s\": %m",
						  name)));
			return false;
		}
		*mapped_address = NULL;
		*mapped_size = 0;
		if (op == DSM_OP_DESTROY && unlink(name) != 0)
		{
			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
				  errmsg("could not remove shared memory segment \"%s\": %m",
						 name)));
			return false;
		}
		return true;
	}

	/* Create new segment or open an existing one for attach or resize. */
	flags = O_RDWR | (op == DSM_OP_CREATE ? O_CREAT | O_EXCL : 0);
	if ((fd = OpenTransientFile(name, flags, 0600)) == -1)
	{
		if (errno != EEXIST)
			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
					 errmsg("could not open shared memory segment \"%s\": %m",
							name)));
		return false;
	}

	/*
	 * If we're attaching the segment, determine the current size; if we are
	 * creating or resizing the segment, set the size to the requested value.
	 */
	if (op == DSM_OP_ATTACH)
	{
		struct stat st;

		if (fstat(fd, &st) != 0)
		{
			int			save_errno;

			/* Back out what's already been done. */
			save_errno = errno;
			CloseTransientFile(fd);
			errno = save_errno;

			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
					 errmsg("could not stat shared memory segment \"%s\": %m",
							name)));
			return false;
		}
		request_size = st.st_size;
	}
	else if (*mapped_size > request_size && ftruncate(fd, request_size))
	{
		int			save_errno;

		/* Back out what's already been done. */
		save_errno = errno;
		close(fd);
		if (op == DSM_OP_CREATE)
			unlink(name);
		errno = save_errno;

		ereport(elevel,
				(errcode_for_dynamic_shared_memory(),
				 errmsg("could not resize shared memory segment \"%s\" to %zu bytes: %m",
						name, request_size)));
		return false;
	}
	else if (*mapped_size < request_size)
	{
		/*
		 * Allocate a buffer full of zeros.
		 *
		 * Note: palloc zbuffer, instead of just using a local char array, to
		 * ensure it is reasonably well-aligned; this may save a few cycles
		 * transferring data to the kernel.
		 */
		char	   *zbuffer = (char *) palloc0(ZBUFFER_SIZE);
		uint32		remaining = request_size;
		bool		success = true;

		/*
		 * Zero-fill the file. We have to do this the hard way to ensure that
		 * all the file space has really been allocated, so that we don't
		 * later seg fault when accessing the memory mapping.  This is pretty
		 * pessimal.
		 */
		while (success && remaining > 0)
		{
			Size		goal = remaining;

			if (goal > ZBUFFER_SIZE)
				goal = ZBUFFER_SIZE;
			if (write(fd, zbuffer, goal) == goal)
				remaining -= goal;
			else
				success = false;
		}

		if (!success)
		{
			int			save_errno;

			/* Back out what's already been done. */
			save_errno = errno;
			CloseTransientFile(fd);
			if (op == DSM_OP_CREATE)
				unlink(name);
			errno = save_errno ? save_errno : ENOSPC;

			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
					 errmsg("could not resize shared memory segment \"%s\" to %zu bytes: %m",
							name, request_size)));
			return false;
		}
	}

	/*
	 * If we're reattaching or resizing, we must remove any existing mapping,
	 * unless we've already got the right thing mapped.
	 */
	if (*mapped_address != NULL)
	{
		if (*mapped_size == request_size)
			return true;
		if (munmap(*mapped_address, *mapped_size) != 0)
		{
			int			save_errno;

			/* Back out what's already been done. */
			save_errno = errno;
			CloseTransientFile(fd);
			if (op == DSM_OP_CREATE)
				unlink(name);
			errno = save_errno;

			ereport(elevel,
					(errcode_for_dynamic_shared_memory(),
				   errmsg("could not unmap shared memory segment \"%s\": %m",
						  name)));
			return false;
		}
		*mapped_address = NULL;
		*mapped_size = 0;
	}

	/* Map it. */
	address =(char *) mmap(NULL, request_size, PROT_READ | PROT_WRITE,
				   MAP_SHARED | MAP_HASSEMAPHORE | MAP_NOSYNC, fd, 0);
	if (address == MAP_FAILED)
	{
		int			save_errno;

		/* Back out what's already been done. */
		save_errno = errno;
		CloseTransientFile(fd);
		if (op == DSM_OP_CREATE)
			unlink(name);
		errno = save_errno;

		ereport(elevel,
				(errcode_for_dynamic_shared_memory(),
				 errmsg("could not map shared memory segment \"%s\": %m",
						name)));
		return false;
	}
	*mapped_address = address;
	*mapped_size = request_size;
	CloseTransientFile(fd);

	return true;
}
#endif

/*
 * Implementation-specific actions that must be performed when a segment
 * is to be preserved until postmaster shutdown.
 *
 * Except on Windows, we don't need to do anything at all.  But since Windows
 * cleans up segments automatically when no references remain, we duplicate
 * the segment handle into the postmaster process.  The postmaster needn't
 * do anything to receive the handle; Windows transfers it automatically.
 */
void
dsm_impl_pin_segment(dsm_handle handle, void *impl_private)
{
	switch (dynamic_shared_memory_type)
	{
#ifdef USE_DSM_WINDOWS
		case DSM_IMPL_WINDOWS:
			{
				HANDLE		hmap;

				if (!DuplicateHandle(GetCurrentProcess(), impl_private,
									 PostmasterHandle, &hmap, 0, FALSE,
									 DUPLICATE_SAME_ACCESS))
				{
					char		name[64];

					snprintf(name, 64, "%s.%u", SEGMENT_NAME_PREFIX, handle);
					_dosmaperr(GetLastError());
					ereport(ERROR,
							(errcode_for_dynamic_shared_memory(),
						  errmsg("could not duplicate handle for \"%s\": %m",
								 name)));
				}
				break;
			}
#endif
		default:
			break;
	}
}

static int
errcode_for_dynamic_shared_memory(void)
{
	if (errno == EFBIG || errno == ENOMEM)
		return errcode(ERRCODE_OUT_OF_MEMORY);
	else
		return errcode_for_file_access();
}