/* Query-local DiskANN filtering protocol. No persistent index fields. */
#ifndef DISKANNQUERYFILTER_H
#define DISKANNQUERYFILTER_H

#include "postgres.h"
#include "storage/item/itemptr.h"

/* Modes serialized by the query-local filtering protocol. */
typedef enum AnnFilterMode {
    ANN_FILTER_NONE = 0,
    ANN_FILTER_IN = 1,
    ANN_FILTER_POST = 2,
    ANN_FILTER_PRE = 3
} AnnFilterMode;

typedef enum DiskAnnFilterPlanMode {
    DISKANN_FILTER_AUTO = 0,
    DISKANN_FILTER_EXACT,
    DISKANN_FILTER_PRE,
    DISKANN_FILTER_IN,
    DISKANN_FILTER_POST
} DiskAnnFilterPlanMode;

typedef struct DiskAnnFilterCounters {
    uint64 visited_nodes;
    uint64 candidate_members;
    uint64 candidate_callbacks;
    uint64 accepted_members;
    uint64 rejected_members;
    uint64 visible_rows;
    uint64 probe_calls;
    uint64 probe_cache_hits;
    uint64 member_cache_hits;
    uint64 qualified_target_rows;
    uint64 fetch_attempts;
    uint64 key_mismatches;
    uint64 search_rounds;
    uint64 pre_build_runs;
    uint64 pre_source_rows;
    uint64 pre_seals;
    uint64 memory_peak;
} DiskAnnFilterCounters;

typedef struct DiskAnnQueryMember {
    Datum key; /* owned AM copy, valid until ClearFilter/ReScan */
    BlockNumber node_block;
    ItemPointerData owner_tid;
    ItemPointerData root_tid;
    ItemPointerData visible_tid;
    bool normalized;
} DiskAnnQueryMember;

/* Called only with owned key/TID copies and without index content locks.
 * The executor resolves HOT and returns the snapshot-visible identity. */
typedef bool (*DiskAnnQualifyMember)(void* context, const DiskAnnQueryMember* member,
    ItemPointerData* visible);

typedef struct DiskAnnFilterSpec {
    AnnFilterMode mode;
    uint64 epoch;
    int search_nodes;
    Size memory_limit;
    DiskAnnQualifyMember qualify;
    void* context;
    DiskAnnFilterCounters* counters;
    Size (*owned_memory)(void* context);
} DiskAnnFilterSpec;

#endif /* DISKANNQUERYFILTER_H */
