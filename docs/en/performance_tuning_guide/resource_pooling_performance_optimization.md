# Resource Pooling Performance Optimization

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-17T06:50:11.810Z -->

## Availability<a name="section15406143204715"></a>

This feature is introduced starting from openGauss 5.1.0 and applies only to the resource pooling architecture.

## Feature Description<a name="section740615433477"></a>

This feature includes the following three sub-features:

- Resource pooling standby visibility logic optimization: locally caches the CSN corresponding to the transaction XID retrieved from the primary, reducing network overhead and message interactions.
- Resource pooling primary retrieval of cluster oldestxmin logic optimization: the primary locally records the xmin of snapshots obtained by the standby in real time, and the standby periodically sends its local oldestxmin to the primary, reducing broadcast overhead.
- Resource pooling standby retrieval of snapshot logic optimization: the primary broadcasts the latest snapshot to the standby each time, and the standby retrieves the snapshot locally, reducing network overhead and message interactions.

## Customer Value <a name="section13406743164715"></a>

Under the resource pooling architecture, the optimization of related logic improves the performance of standby read-only scenarios. In a typical sysbench test with one primary and one standby, where the primary handles read/write and the standby handles read-only, the standby performance can be improved by 50% to 80%.

## Feature Description<a name="section16406154310471"></a>

This feature includes the following three sub-features:

- Resource pooling standby visibility logic optimization: In the original logic, when the standby determines tuple visibility, if the XID on the tuple is not marked as committed, it retrieves the real-time CSN corresponding to the current tuple XID from the primary to determine visibility. If the standby queries many pages and the query process is long, this interaction logic becomes very frequent, affecting the execution efficiency of both the primary and the standby. This feature establishes a two-level cache of transaction commit status in the standby's local memory. When the standby performs tuple visibility determination, it first retrieves from the local cache, and if not found, retrieves from the primary node and updates the local cache.
- Resource pooling primary retrieval of cluster oldestxmin logic optimization: In the original logic, each time the primary generates the latest snapshot information, it triggers a broadcast message to retrieve the oldestxmin from all standbys in the cluster, thereby updating the cluster's oldestxmin for VACUUM and heap tuple prune operations. This feature records the snapshot xmin in the primary memory when the standby retrieves a snapshot from the primary. Meanwhile, the standby periodically sends its local oldestxmin to the primary through a background thread, and the primary periodically cleans up invalid xmin information through a background thread.
- Resource pooling standby retrieval snapshot logic optimization: In the original logic, the standby needs to retrieve the real-time latest snapshot information from the primary for every read operation. When there are many standby read operations, the primary-standby interaction becomes frequent. This feature broadcasts the latest snapshot information to each standby in the cluster each time the primary generates a new snapshot. The standby caches the latest snapshot locally, and each read on the standby preferentially retrieves from the local latest snapshot, reducing the message interaction for standby snapshot retrieval when there are many standby read-only operations. At the same time, to reduce the impact of broadcast operations on the primary when there are many standbys, this feature can be controlled by a switch to enable or disable it. It is disabled by default. For details, see [ss_enable_bcast_snapshot](https://docs.opengauss.org/en/docs/latest/database_reference/resource_pooling_parameters.html#ss_enable_bcast_snapshot).

## Feature Enhancements<a name="section1340684315478"></a>

This feature enhances the original standby visibility determination, primary retrieval of oldestxmin, and standby retrieval of snapshots under the resource pooling architecture.

## Feature Constraints<a name="section06531946143616"></a>

None

## Dependencies<a name="section8406643144716"></a>

This feature depends on the resource pooling architecture.

## Basic Principles

The basic principles of the three features included in this feature are as follows:

- Resource pooling standby visibility logic optimization: This feature creates a secondary cache for storing transaction commit status in each service thread on the standby node. When the standby node performs tuple visibility judgment, it first retrieves the status from this cache. If the retrieval fails, it obtains the status from the primary node through DMS, and updates the cache with the retrieved information upon success. Meanwhile, to control the memory consumption of service threads, the cached information is evicted in real time using the `LRU` algorithm.
- Resource pooling primary retrieval of cluster `oldestxmin` logic optimization: This feature primarily optimizes the logic by which the primary node maintains the cluster `oldestxmin`. It allocates an array in the primary node's global memory to store the minimum read xmin currently known to each standby node, along with a hash table to store the timestamp at which each xmin was obtained. The primary node traverses the hash table to find the earliest xmin, compares it with the xmin values stored in the array, and takes the smallest one as the `oldestxmin` currently held by all standby nodes. This design ensures that xmin values involved in ongoing network requests are not missed, thereby preventing the cleanup of data tuples that standby nodes are still accessing.
- Resource pooling standby retrieval of snapshot logic optimization: This feature enables the primary node to actively broadcast the latest snapshot information to each standby node in the cluster whenever it generates a new snapshot. The standby nodes cache the latest received snapshot locally. When a read service on a standby node needs to obtain the latest snapshot information in the cluster, it first retrieves it from the local cache. If the local cache is invalid, it then obtains it from the primary node over the network. This reduces the number of message interactions for snapshot retrieval on standby nodes when there are many standby read-only services but few write services on the primary node. Whether this feature is enabled or disabled is controlled by a switch, and it is disabled by default. For details, see [ss_enable_bcast_snapshot](https://docs.opengauss.org/en/docs/latest/database_reference/resource_pooling_parameters.html#ss_enable_bcast_snapshot).

## Usage Guide

- Resource pooling standby visibility logic optimization: This feature is enabled by default and requires no additional operations.
- Resource pooling primary retrieval of cluster `oldestxmin` logic optimization: This feature is enabled by default and requires no additional operations.
- Resource pooling standby retrieval snapshot logic optimization: This feature can be controlled by a switch to determine whether to enable or disable it. It is disabled by default. For details, see [ss_enable_bcast_snapshot](https://docs.opengauss.org/en/docs/latest/database_reference/resource_pooling_parameters.html#ss_enable_bcast_snapshot). After being enabled, it takes effect internally in the database and is transparent to users.

## Use Cases

The use case for this feature is: scenarios where performance testing is required under resource pooling.