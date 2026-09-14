# LFC and Prefetch

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T12:07:17.386Z pushedAt=2026-07-30T12:23:58.590Z -->

## Feature Overview

Local File Cache (LFC) and Prefetch are designed to optimize the read path performance of openGauss Compute in data branching scenarios, with the goal of reducing the wait time for Compute to synchronously access Pageserver.

In the storage-compute disaggregated architecture, Compute does not store complete data files. When reading a page, if the required page is not present in the local shared buffer, Compute needs to send a `GetPage` request to Pageserver. For scenarios that access pages consecutively or in batches such as sequential scans, index scans, VACUUM, and ANALYZE, making synchronous requests page by page introduces significant remote access latency.

This feature optimizes the read path from two directions:

- Prefetch: Identifies pages that are likely to be accessed subsequently in advance, and asynchronously sends `GetPage` requests to Pageserver.
- LFC: Maintains a local file cache on Compute to store pages that have already been read, reducing repeated accesses to Pageserver.

The overall hit priority is as follows:

```text
prefetch ring -> LFC -> synchronous read from Pageserver
```

Both Prefetch and LFC are performance optimization capabilities. When a prefetch miss, response expiration, cache miss, or cache unavailability occurs, the system falls back to a normal synchronous `GetPage` read, without affecting the correctness of SQL query results.

## Applicable Scenarios

Prefetch is better suited for scenarios where the access path is predictable, such as sequential scans and index scans. LFC is more suitable for scenarios where hot data is reusable and the proportion of repeated reads is high.

## Prefetch

Prefetch is used to fetch pages from Pageserver in advance that are likely to be accessed in the future.

Each backend maintains a private prefetch ring to store asynchronous prefetch requests initiated by that backend and their responses. Each slot in the prefetch ring corresponds to a page-level `GetPage` request.

The capacity of the prefetch ring is controlled by `neon.readahead_buffer_size`. When registering a new prefetch request, the system first checks whether an available slot with the same `BufferTag` already exists, to avoid sending duplicate prefetch requests for the same page.

When the ring is full, responses that have been received but not yet consumed are released first. If the oldest request is still in the state of waiting for a response, it is necessary to wait for that request to complete or be cleaned up before reusing the corresponding slot.

Prefetch can be triggered by multiple types of access paths.

| Trigger Source | Description |
| --- | --- |
| Sequential Scan | Maintains a prefetch window based on the scan position and requests subsequent heap pages in advance |
| B-tree Index Scan | Prefetches heap pages based on the heap TIDs from subsequent index tuples |
| B-tree Index Only Scan | Prefetches subsequent index leaf pages |
| Bitmap Heap Scan | Prefetches subsequent heap blocks in bitmap order |
| VACUUM | Prefetches pages during phases such as heap scanning, dead tuple cleanup, and tail page truncation check |
| ANALYZE | Prefetches sample blocks in advance during row-store table sampling, without altering the random sampling sequence |

## LFC

LFC is a Compute-local shared file cache used to store data pages returned by Pageserver. LFC manages cache space in chunks, where each chunk contains multiple database blocks.

**Policies for Writing Prefetch Results to LFC**

Whether a prefetch response is immediately written to LFC is controlled by `neon.store_prefetch_result_in_lfc`.

### Default Policy: Write on Consumption

The default value is `off`. In this case, prefetch responses are only stored in the current backend's private prefetch ring.

When a subsequent actual read hits the prefetch response, the page is copied to the shared buffer and written to LFC under buffer lock protection. If a prefetch response is never consumed, it will not be written to LFC when evicted later.

This policy reduces un-consumed prefetch pages from being written to LFC, minimizing local disk writes and cache pollution.

### Immediate Write Policy

When set to `on`, prefetch responses are written to LFC immediately upon arrival, and a flag is set on the prefetch ring slot to avoid duplicate writes during subsequent actual reads.

This policy allows prefetched pages to enter the shared LFC earlier, enabling other backends on the same Compute node to potentially hit these pages. However, if there are many prefetch misjudgments, pages that will never be accessed may also be written to LFC, increasing local writes and cache pollution.

It is recommended to enable this policy in scenarios where the prefetch hit rate is high and hot pages are frequently reused across connections.

## Constraints

The following constraints must be observed when using LFC and Prefetch:

1. Neon prefetch processes only permanent tables.
2. Temporary tables, unlogged tables, locally built relations, and others still use openGauss's original local file path.
3. The prefetch depth is jointly affected by `effective_io_concurrency` and `neon.readahead_buffer_size`. The `target_prefetch_pages` converted from `effective_io_concurrency` must not exceed `neon.readahead_buffer_size`; otherwise, earlier prefetch requests may be discarded or forced to wait.

## Parameters

| Parameter Name | Description | Default Value | Restart Required |
| --- | --- | --- | --- |
| `effective_io_concurrency` | User-visible prefetch concurrency. openGauss internally converts it to `target_prefetch_pages` via an assign hook, which controls the prefetch window of the access method. Prefetch is enabled when `effective_io_concurrency > 1`. | `1` | No |
| `enable_seqscan_prefetch` | Controls whether to enable Neon prefetch for sequential scans. | `on` | No |
| `enable_indexscan_prefetch` | Controls whether to prefetch subsequent heap pages for B-tree index scans. | `on` | No |
| `enable_indexonlyscan_prefetch` | Controls whether to prefetch subsequent index leaf pages for B-tree index-only scans. | `on` | No |
| `neon.readahead_buffer_size` | Controls the capacity of each backend's private prefetch ring, i.e., the maximum number of in-flight or received prefetch requests retained. | `64` | No |
| `neon.readahead_getpage_pull_timeout` | Controls the interval at which the backend actively pulls `GetPage` responses that have arrived. `0` disables timed active pulling. | `50ms` | No |
| `neon.store_prefetch_result_in_lfc` | Controls whether to immediately write prefetch responses to LFC upon reception. | `off` | No |
| `neon.max_file_cache_size` | Hard upper limit of the LFC local file cache. `0` disables LFC. | `0KB` | Yes |
| `neon.file_cache_size_limit` | Current soft limit of LFC, controlling the actual usable capacity at runtime. Cannot exceed `neon.max_file_cache_size`. | `0KB` | No |
| `neon.file_cache_path` | Path to the LFC local cache file. | `file.cache` | Yes |
| `neon.file_cache_chunk_size` | LFC chunk size, in database blocks. Must be a power of 2. | `128 blocks` | Yes |

## Observability

The runtime performance of LFC and Prefetch can be observed through `neon_lfc_stats` and `neon_perf_counters`.

### LFC Runtime Status

`neon_lfc_stats` is used to observe LFC usage, including cache capacity, used space, hit count, miss count, write count, and hit ratio.

Common metrics to monitor:

| Metric | Description |
| --- | --- |
| LFC used | Currently used cache space |
| LFC hit | Number of pages hits from LFC |
| LFC miss | Number of pages misses in LFC |
| LFC writes | Number of pages written to LFC |
| LFC hit ratio | LFC hit ratio |

### Prefetch Statistics

`neon_perf_counters` is used to observe prefetch requests and response status.

Common metrics to monitor:

| Metric | Description |
| --- | --- |
| `getpage_prefetch_requests_total` | Number of prefetch requests initiated |
| `getpage_prefetch_misses_total` | Number of times the prefetch ring was missed during actual reads |
| `getpage_prefetch_discards_total` | Number of discarded prefetch responses |
| `getpage_prefetch_buffered_total` | Number of responses received and temporarily stored in the prefetch ring |
| LFC hit-related counters | Page hits from LFC |

If `getpage_prefetch_requests_total` increases after prefetch is enabled, but `miss` or `discard` counts are also high, it may indicate that the access path is not well-suited for the current prefetch depth, or that prefetched pages have expired before being actually read.

## Notes

- Excessively deep prefetch may cause prefetch ring eviction or waiting.
- `neon.store_prefetch_result_in_lfc=on` may increase the probability of cross-backend reuse, but may also increase invalid page writes.
- LFC provides more significant benefits in scenarios with obvious hot spots and high repeated read ratios.
