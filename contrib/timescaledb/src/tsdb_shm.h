/*-------------------------------------------------------------------------
 *
 * shm_mq.h
 *	  single-reader, single-writer shared memory message queue
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/shm_mq.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHM_MQ_H
#define SHM_MQ_H

#include "postmaster/bgworker.h"

#include "storage/proc.h"
#include "utils/hsearch.h"
#include "tsdb_dsm.h"


/* The queue itself, in shared memory. */
struct shm_mq;
typedef struct shm_mq shm_mq;

/* Backend-private state. */
struct shm_mq_handle;
typedef struct shm_mq_handle shm_mq_handle;

struct BackgroundWorkerHandle;
typedef struct BackgroundWorkerHandle BackgroundWorkerHandle; 

struct dsm_segment;
typedef struct dsm_segment dsm_segment;
/* Descriptors for a single write spanning multiple locations. */
typedef struct
{
	const char *data;
	Size		len;
} shm_mq_iovec;

/* Possible results of a send or receive operation. */
typedef enum
{
	SHM_MQ_SUCCESS,				/* Sent or received a message. */
	SHM_MQ_WOULD_BLOCK,			/* Not completed; retry later. */
	SHM_MQ_DETACHED				/* Other process has detached queue. */
} shm_mq_result;



/*
 * Primitives to create a queue and set the sender and receiver.
 *
 * Both the sender and the receiver must be set before any messages are read
 * or written, but they need not be set by the same process.  Each must be
 * set exactly once.
 */
extern shm_mq *shm_mq_create(void *address, Size size);
extern void shm_mq_set_receiver(shm_mq *mq, PGPROC *);
extern void shm_mq_set_sender(shm_mq *mq, PGPROC *);

/* Accessor methods for sender and receiver. */
extern PGPROC *shm_mq_get_receiver(shm_mq *);
extern PGPROC *shm_mq_get_sender(shm_mq *);

/* Set up backend-local queue state. */
extern shm_mq_handle *shm_mq_attach(shm_mq *mq, dsm_segment *seg,
			  BackgroundWorkerHandle *handle);

/* Associate worker handle with shm_mq. */
extern void shm_mq_set_handle(shm_mq_handle *, BackgroundWorkerHandle *);

/* Break connection. */
extern void shm_mq_detach(shm_mq *);

/* Get the shm_mq from handle. */
extern shm_mq *shm_mq_get_queue(shm_mq_handle *mqh);

/* Send or receive messages. */
extern shm_mq_result shm_mq_send(shm_mq_handle *mqh,
			Size nbytes, const void *data, bool nowait);
extern shm_mq_result shm_mq_sendv(shm_mq_handle *mqh,
			 shm_mq_iovec *iov, int iovcnt, bool nowait);
extern shm_mq_result shm_mq_receive(shm_mq_handle *mqh,
			   Size *nbytesp, void **datap, bool nowait);

/* Wait for our counterparty to attach to the queue. */
extern shm_mq_result shm_mq_wait_for_attach(shm_mq_handle *mqh);

/* Smallest possible queue. */
extern PGDLLIMPORT const Size shm_mq_minimum_size;



/*-------------------------------------------------------------------------
 *
 * shm_toc.h
 *	  shared memory segment table of contents
 *
 * This is intended to provide a simple way to divide a chunk of shared
 * memory (probably dynamic shared memory allocated via dsm_create) into
 * a number of regions and keep track of the addresses of those regions or
 * key data structures within those regions.  This is not intended to
 * scale to a large number of keys and will perform poorly if used that
 * way; if you need a large number of pointers, store them within some
 * other data structure within the segment and only put the pointer to
 * the data structure itself in the table of contents.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/shm_toc.h
 *
 *-------------------------------------------------------------------------
 */


#include "storage/shmem.h"

struct shm_toc;
typedef struct shm_toc shm_toc;

extern shm_toc *shm_toc_create(uint64 magic, void *address, Size nbytes);
extern shm_toc *shm_toc_attach(uint64 magic, void *address);
extern void *shm_toc_allocate(shm_toc *toc, Size nbytes);
extern Size shm_toc_freespace(shm_toc *toc);
extern void shm_toc_insert(shm_toc *toc, uint64 key, void *address);
extern void *shm_toc_lookup(shm_toc *toc, uint64 key);

/*
 * Tools for estimating how large a chunk of shared memory will be needed
 * to store a TOC and its dependent objects.
 */
typedef struct
{
	Size		space_for_chunks;
	Size		number_of_keys;
} shm_toc_estimator;

#define shm_toc_initialize_estimator(e) \
	((e)->space_for_chunks = 0, (e)->number_of_keys = 0)
#define shm_toc_estimate_chunk(e, sz) \
	((e)->space_for_chunks = add_size((e)->space_for_chunks, \
		BUFFERALIGN((sz))))
#define shm_toc_estimate_keys(e, cnt) \
	((e)->number_of_keys = add_size((e)->number_of_keys, (cnt)))

extern Size shm_toc_estimate(shm_toc_estimator *);



#endif   /* SHMEM_H */
