/* -------------------------------------------------------------------------
 *
 * relscan.h
 *	  POSTGRES relation scan descriptor definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/relscan.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef RELSCAN_H
#define RELSCAN_H

#include "access/genam.h"
#include "access/heapam.h"
#include "access/itup.h"
#include "access/tupdesc.h"

#define PARALLEL_SCAN_GAP 100

/*
 * Shared state for parallel heap scan.
 *
 * Each backend participating in a parallel heap scan has its own
 * HeapScanDesc in backend-private memory, and those objects all contain
 * a pointer to this structure.  The information here must be sufficient
 * to properly initialize each new HeapScanDesc as workers join the scan,
 * and it must act as a font of block numbers for those workers.
 */
typedef struct ParallelHeapScanDescData {
    int plan_node_id;                /* used to identify speicific plan */
    Oid phs_relid;                   /* OID of relation to scan */
    BlockNumber phs_nblocks;         /* # blocks in relation at start of scan */
    slock_t phs_mutex;               /* mutual exclusion for setting startblock */
    BlockNumber phs_startblock;      /* starting block number */
    uint32 pscan_len;                /* total size of this struct, including phs_snapshot_data */
    pg_atomic_uint64 phs_nallocated; /* number of blocks allocated to workers so far. */
    bool phs_syncscan;               /* report location to syncscan logic? */
    bool phs_snapshot_any;           /* SnapshotAny, not phs_snapshot_data? */
    char phs_snapshot_data[FLEXIBLE_ARRAY_MEMBER];
} ParallelHeapScanDescData;

/* ----------------------------------------------------------------
 *				 Scan State Information
 * ----------------------------------------------------------------
 */
typedef struct SeqScanAccessor {
    BlockNumber sa_last_prefbf;  /* last prefetch block number */
    BlockNumber sa_pref_trigbf;  /* last triggle block number */
    uint32 sa_prefetch_quantity; /* preftch quantity*/
    uint32 sa_prefetch_trigger;  /*the prefetch-trigger distance bewteen last prefetched buffer and currently accessed
                                    buffer */
} SeqScanAccessor;

struct TableAm;

typedef struct ScanDescData {
	NodeTag type;
	union
	{
		/* the access methods for table scan */
		const struct TableAm *tblAm;
		const struct IndexAm *idxAm;
	};
} ScanDescData;

/* The ScanDescData for table (Tbl) */
typedef ScanDescData  AbsTblScanDescData;
typedef ScanDescData* AbsTblScanDesc;

/* The ScanDescData for index (Idx) */
typedef ScanDescData  AbsIdxScanDescData;
typedef ScanDescData* AbsIdxScanDesc;

#define IsValidScanDesc(sd)  (sd != NULL)

typedef struct HeapScanDescData {
    AbsTblScanDescData sd;
    /* scan parameters */
    Relation rs_rd;       /* heap relation descriptor */
    Snapshot rs_snapshot; /* snapshot to see */
    int rs_nkeys;         /* number of scan keys */
    ScanKey rs_key;       /* array of scan key descriptors */
    /*
     * Information about type and behaviour of the scan, a bitmask of members
     * of the ScanOptions enum (see tableam.h).
     */
    uint32 rs_flags;

    /* state set up at initscan time */
    BlockNumber rs_nblocks;           /* number of blocks to scan */
    BlockNumber rs_startblock;        /* block # to start at */
    BufferAccessStrategy rs_strategy; /* access strategy for reads */
    bool rs_syncscan;                 /* report location to syncscan logic? */

    /* scan current state */
    bool rs_inited;        /* false = scan not init'd yet */
    BlockNumber rs_cblock; /* current block # in scan, if any */
    TupleDesc rs_tupdesc;  /* heap tuple descriptor for rs_ctup */
    Buffer rs_cbuf;        /* current buffer in scan, if any */
    /* NB: if rs_cbuf is not InvalidBuffer, we hold a pin on that buffer */
    ItemPointerData rs_mctid; /* marked scan position, if any */
    ParallelHeapScanDesc rs_parallel; /* parallel scan information */

    /* these fields only used in page-at-a-time mode and for bitmap scans */
    int rs_cindex;                                   /* current tuple's index in vistuples */
    int rs_mindex;                                   /* marked tuple's saved index */
    int rs_ntuples;                                  /* number of visible tuples on page */
    OffsetNumber rs_vistuples[MaxHeapTuplesPerPage]; /* their offsets */
    SeqScanAccessor* rs_ss_accessor;                 /* adio use it to init prefetch quantity and trigger */
    int dop;                                         /* scan parallel degree */
    /* put decompressed tuple data into rs_ctbuf be careful  , when malloc memory  should give extra mem for
     *xs_ctbuf_hdr. t_bits which is varlength arr
     */
    HeapTupleData rs_ctup; /* current tuple in scan, if any */
    HeapTupleHeaderData rs_ctbuf_hdr;
} HeapScanDescData;

#define SizeofHeapScanDescData (offsetof(HeapScanDescData, rs_ctbuf_hdr) + SizeofHeapTupleHeader)

struct ScanState;

/*!
 * The ScanDescData for hash-bucket table
 */
typedef struct HBktTblScanDescData {
	AbsTblScanDescData sd;
	Relation rs_rd;  /* heap relation descriptor */
	struct ScanState* scanState;
	List*    hBktList;  /* hash bucket list that used to scan */
	int      curr_slot;
	Relation currBktRel;
	HeapScanDescData* currBktScan;
} HBktTblScanDescData;

typedef HBktTblScanDescData* HBktTblScanDesc;

/*
 * We use the same IndexScanDescData structure for both amgettuple-based
 * and amgetbitmap-based index scans.  Some fields are only relevant in
 * amgettuple-based scans.
 */
typedef struct IndexScanDescData {
    AbsIdxScanDescData sd;
    /* scan parameters */
    Relation heapRelation;   /* heap relation descriptor, or NULL */
    Relation indexRelation;  /* index relation descriptor */
    GPIScanDesc xs_gpi_scan; /* global partition index scan use information */
    Snapshot xs_snapshot;    /* snapshot to see */
    int numberOfKeys;        /* number of index qualifier conditions */
    int numberOfOrderBys;    /* number of ordering operators */
    ScanKey keyData;         /* array of index qualifier descriptors */
    ScanKey orderByData;     /* array of ordering op descriptors */
    bool xs_want_itup;       /* caller requests index tuples */
    bool xs_want_ext_oid;    /* global partition index need partition oid */
    bool xs_temp_snap;       /* unregister snapshot at scan end? */

    /* signaling to index AM about killing index tuples */
    bool kill_prior_tuple;      /* last-returned tuple is dead */
    bool ignore_killed_tuples;  /* do not return killed entries */
    bool xactStartedInRecovery; /* prevents killing/seeing killed
                                 * tuples */

    /* index access method's private state */
    void* opaque; /* access-method-specific info */

    /* in an index-only scan, this is valid after a successful amgettuple */
    IndexTuple xs_itup;    /* index tuple returned by AM */
    TupleDesc xs_itupdesc; /* rowtype descriptor of xs_itup */

    /* xs_ctup/xs_cbuf/xs_recheck are valid after a successful index_getnext */
    HeapTupleData xs_ctup; /* current heap tuple, if any */
    Buffer xs_cbuf;        /* current heap buffer in scan, if any */
    /* NB: if xs_cbuf is not InvalidBuffer, we hold a pin on that buffer */
    bool xs_recheck; /* T means scan keys must be rechecked */

    /* state data for traversing HOT chains in index_getnext */
    bool xs_continue_hot; /* T if must keep walking HOT chain */

    /* parallel index scan information, in shared memory */
    ParallelIndexScanDesc parallel_scan;

    /* put decompressed heap tuple data into xs_ctbuf_hdr be careful! when malloc memory  should give extra mem for
     *xs_ctbuf_hdr. t_bits which is varlength arr
     */
    HeapTupleHeaderData xs_ctbuf_hdr;
} IndexScanDescData;

#define SizeofIndexScanDescData (offsetof(IndexScanDescData, xs_ctbuf_hdr) + SizeofHeapTupleHeader)

/* Get partition heap oid for bitmap index scan */
#define IndexScanGetPartHeapOid(scan)                                                                           \
    ((scan)->indexRelation != NULL                                                                              \
            ? (RelationIsPartition((scan)->indexRelation) ? (scan)->indexRelation->rd_partHeapOid : InvalidOid) \
            : InvalidOid)

/*
 * When the global partition index is used for index scanning,
 * checks whether the partition table needs to be
 * switched each time an indextuple is obtained.
 */
#define IndexScanNeedSwitchPartRel(scan) \
    ((scan)->xs_want_ext_oid && GPIScanCheckPartOid((scan)->xs_gpi_scan, (scan)->heapRelation->rd_id))

/* Generic structure for parallel scans */
typedef struct ParallelIndexScanDescData {
    int plan_node_id; /* used to identify speicific plan */
    Oid ps_relid;
    Size ps_offset; /* Offset in bytes of am specific structure */
    uint32 pscan_len; /* total size of this struct, including phs_snapshot_data */
    char ps_snapshot_data[FLEXIBLE_ARRAY_MEMBER];
} ParallelIndexScanDescData;

typedef struct HBktIdxScanDescData {
    AbsIdxScanDescData sd;
    Relation rs_rd;  /* heap relation descriptor */
    Relation idx_rd;  /* index relation descriptor */
    struct ScanState* scanState;
    List*    hBktList;
    int      curr_slot;
    Relation currBktHeapRel;
    Relation currBktIdxRel;
    IndexScanDescData* currBktIdxScan;
} HBktIdxScanDescData;

typedef HBktIdxScanDescData* HBktIdxScanDesc;

/* Struct for heap-or-index scans of system tables */
typedef struct SysScanDescData {
    Relation heap_rel;   /* catalog being scanned */
    Relation irel;       /* NULL if doing heap scan */
    HeapScanDesc scan;   /* only valid in heap-scan case */
    IndexScanDesc iscan; /* only valid in index-scan case */
} SysScanDescData;

#endif /* RELSCAN_H */
