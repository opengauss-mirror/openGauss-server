# Heap Tables Support Prefetching

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-17T06:49:49.867Z -->

## Availability<a name="section1820817472142"></a>

This feature is introduced since openGauss 6.0.0-RC1.

## Feature Overview<a name="section595916321417"></a>

When performing sequential page reads during heap table scans, this feature reads multiple pages at a time to reduce the I/O overhead caused by frequent single-page reads, thereby improving the performance of sequential scans on heap tables.

## Customer Benefits<a name="section1889785041315"></a>

Improves performance in scenarios where customers frequently perform full-table sequential scans.

## Feature Description<a name="section3050790"></a>

When performing a sequential scan on a heap table in the database, the system reads pages from the disk into memory one by one. If the heap table to be scanned contains a large amount of data, frequent disk access can cause significant performance loss. To address this issue, the prefetching feature is introduced. Prefetching means that when scanning a disk file, the operating system reads multiple pages in a single disk I/O operation instead of reading pages one by one. This significantly reduces the frequent I/O overhead caused by single-page access. In a database environment, this feature also applies to sequential scans on heap tables, allowing multiple pages to be read into memory at once, thereby reducing the number of disk I/O operations. When performing lazy vacuum to clean up heap tables, the prefetching feature can also accelerate the scanning and cleanup process.

Users can decide whether to enable this feature based on their operating environment and business requirements, and adjust the parameter size accordingly. Experience shows that when processing heap tables with more than 10 GB of data, enabling prefetching can effectively improve the performance of sequential scans and lazy vacuum.

## Feature Enhancements<a name="section27457110"></a>

None.

## Feature Constraints<a name="section06531946143616"></a>

This feature is only available for sequential scan operations on non-compressed heap tables under the non-segment-page, row-store engine.

## Dependencies<a name="section45787398"></a>

None.