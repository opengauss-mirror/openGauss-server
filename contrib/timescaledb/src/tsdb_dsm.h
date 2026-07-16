/*-------------------------------------------------------------------------
 *
 * dsm.h
 *	  manage dynamic shared memory segments
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/dsm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DSM_H
#define DSM_H



/* A "name" for a dynamic shared memory segment. */
typedef uint32 dsm_handle;

#define DSM_CREATE_NULL_IF_MAXSEGMENTS			0x0001

struct dsm_segment
{
	dlist_node	node;			/* List link in dsm_segment_list. */
	ResourceOwner resowner;		/* Resource owner. */
	dsm_handle	handle;			/* Segment name. */
	uint32		control_slot;	/* Slot in control segment. */
	void	   *impl_private;	/* Implementation-specific private data. */
	void	   *mapped_address; /* Mapping address, or NULL if unmapped. */
	Size		mapped_size;	/* Size of our mapping. */
	slist_head	on_detach;		/* On-detach callbacks. */
}; 
typedef struct dsm_segment dsm_segment;
/* Startup and shutdown functions. */
struct PGShmemHeader;			/* avoid including pg_shmem.h */
extern void dsm_cleanup_using_control_segment(dsm_handle old_control_handle);
extern void dsm_postmaster_startup(struct PGShmemHeader *);
extern void dsm_backend_shutdown(void);
extern void dsm_detach_all(void);

#ifdef EXEC_BACKEND
extern void dsm_set_control_handle(dsm_handle h);
#endif

/* Functions that create, update, or remove mappings. */
extern dsm_segment *dsm_create(Size size, int flags);
extern dsm_segment *dsm_attach(dsm_handle h);
extern void *dsm_resize(dsm_segment *seg, Size size);
extern void *dsm_remap(dsm_segment *seg);
extern void dsm_detach(dsm_segment *seg);

/* Resource management functions. */
extern void dsm_pin_mapping(dsm_segment *seg);
extern void dsm_unpin_mapping(dsm_segment *seg);
extern void dsm_pin_segment(dsm_segment *seg);
extern dsm_segment *dsm_find_mapping(dsm_handle h);

/* Informational functions. */
extern void *dsm_segment_address(dsm_segment *seg);
extern Size dsm_segment_map_length(dsm_segment *seg);
extern dsm_handle dsm_segment_handle(dsm_segment *seg);

/* Cleanup hooks. */
typedef void (*on_dsm_detach_callback) (dsm_segment *, Datum arg);
extern void on_dsm_detach(dsm_segment *seg,
			  on_dsm_detach_callback function, Datum arg);
extern void cancel_on_dsm_detach(dsm_segment *seg,
					 on_dsm_detach_callback function, Datum arg);
extern void reset_on_dsm_detach(void);


/*-------------------------------------------------------------------------
 *
 * dsm_impl.h
 *	  low-level dynamic shared memory primitives
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/dsm_impl.h
 *
 *-------------------------------------------------------------------------
 */


/* Dynamic shared memory implementations. */
#define DSM_IMPL_NONE			0
#define DSM_IMPL_POSIX			1
#define DSM_IMPL_SYSV			2
#define DSM_IMPL_WINDOWS		3
#define DSM_IMPL_MMAP			4

/*
 * Determine which dynamic shared memory implementations will be supported
 * on this platform, and which one will be the default.
 */
#ifdef WIN32
#define USE_DSM_WINDOWS
#define DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE		DSM_IMPL_WINDOWS
#else
#ifdef HAVE_SHM_OPEN
#define USE_DSM_POSIX
#define DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE		DSM_IMPL_POSIX
#endif
#define USE_DSM_SYSV
#ifndef DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE
#define DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE		DSM_IMPL_SYSV
#endif
#define USE_DSM_MMAP
#endif

/* GUC. */
extern int	dynamic_shared_memory_type;

/*
 * Directory for on-disk state.
 *
 * This is used by all implementations for crash recovery and by the mmap
 * implementation for storage.
 */
#define PG_DYNSHMEM_DIR					"pg_dynshmem"
#define PG_DYNSHMEM_MMAP_FILE_PREFIX	"mmap."


/* All the shared-memory operations we know about. */
typedef enum
{
	DSM_OP_CREATE,
	DSM_OP_ATTACH,
	DSM_OP_DETACH,
	DSM_OP_RESIZE,
	DSM_OP_DESTROY
} dsm_op;

/* Create, attach to, detach from, resize, or destroy a segment. */
extern bool dsm_impl_op(dsm_op op, dsm_handle handle, Size request_size,
			void **impl_private, void **mapped_address, Size *mapped_size,
			int elevel);

/* Some implementations cannot resize segments.  Can this one? */
extern bool dsm_impl_can_resize(void);

/* Implementation-dependent actions required to keep segment until shutdown. */
extern void dsm_impl_pin_segment(dsm_handle handle, void *impl_private);


#define IPCProtection	(0600)	/* access/modify by user only */

#ifdef SHM_SHARE_MMU			/* use intimate shared memory on Solaris */
#define PG_SHMAT_FLAGS			SHM_SHARE_MMU
#else
#define PG_SHMAT_FLAGS			0
#endif

/* Linux prefers MAP_ANONYMOUS, but the flag is called MAP_ANON on other systems. */
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS			MAP_ANON
#endif

/* BSD-derived systems have MAP_HASSEMAPHORE, but it's not present (or needed) on Linux. */
#ifndef MAP_HASSEMAPHORE
#define MAP_HASSEMAPHORE		0
#endif

/*
 * BSD-derived systems use the MAP_NOSYNC flag to prevent dirty mmap(2)
 * pages from being gratuitously flushed to disk.
 */
#ifndef MAP_NOSYNC
#define MAP_NOSYNC			0
#endif

#define PG_MMAP_FLAGS			(MAP_SHARED|MAP_ANONYMOUS|MAP_HASSEMAPHORE)

/* Some really old systems don't define MAP_FAILED. */
#ifndef MAP_FAILED
#define MAP_FAILED ((void *) -1)
#endif 

#endif   /* DSM_H */
