# GIN Tips and Tricks

Create vs. Insert

Insertion into a GIN index can be slow because many keys may be inserted for each item. For bulk insertions into a table, it is recommended to drop the GIN index first and rebuild it after the insertion is complete. The GUC parameters related to GIN index creation and query performance are as follows:

- maintenance\_work\_mem

    The build time of a GIN index is very sensitive to the setting of maintenance\_work\_mem.

- work\_mem

    During insertion into a GIN index with FASTUPDATE enabled, the system cleans up the pending list whenever its size exceeds work\_mem. To avoid observable large fluctuations in response time, it is preferable to have the pending list cleaned up in the background (for example, by autovacuum). Foreground cleanup operations can be avoided by increasing work\_mem or by running autovacuum. However, increasing work\_mem means that if a foreground cleanup does occur, its execution time will be longer.

- gin\_fuzzy\_search\_limit

    The primary purpose of developing the GIN index is to enable openGauss to support highly scalable full-text indexing, and it is common to encounter situations where a full-text index returns a huge number of results. Moreover, this often happens when querying high-frequency words, so such result sets are of little use. Because reading a large number of records from disk and sorting them consumes a large amount of resources, this is unacceptable in a production environment. To control this situation, the GIN index has a configurable parameter gin\_fuzzy\_search\_limit that acts as a soft limit on the number of returned result rows. The default value 0 means no limit. If a non-zero value is set, the returned results are a randomly selected subset of the complete result set. "Soft limit" means that the actual number of returned results may deviate from the specified limit, depending on the query and the quality of the system's random number generator.
