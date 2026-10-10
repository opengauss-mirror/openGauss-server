# Row-to-Vector Transformation

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-17T06:50:18.117Z -->

## Availability<a name="section15406143204715"></a>

This feature is introduced since openGauss 3.0.0.

## Feature Description<a name="section740615433477"></a>

Converts queries on row-store tables into vectorized execution plans for execution, improving the execution performance of complex queries.

## Customer Value<a name="section13406743164715"></a>

The row-store execution engine delivers unsatisfactory performance when executing complex queries that involve many expressions or join operations, whereas the vectorized execution engine excels in such scenarios. Therefore, converting row-store table queries into vectorized execution plans can effectively improve the query performance of complex queries.

## Feature Description<a name="section16406154310471"></a>

This feature adds a RowToVec operation to the scan operator, converting row-store table data into a vectorized format in memory. After this conversion, all upper-level operators can be transformed into corresponding vectorized operators, thereby leveraging the vectorized execution engine for computation. The scan operators that support row-to-column conversion include: SeqScan, IndexOnlyscan, IndexScan, BitmapScan, FunctionScan, ValueScan, and TidScan.

## Feature Enhancements<a name="section1340684315478"></a>

None.

## Feature Constraints <a name="section06531946143616"></a>

- Scenarios where vectorization is not supported include:
    - The targetList contains functions that return a set.
    - The targetList or qual contains expressions that do not support vectorization: array expression computation; multi-subquery expression computation; Field expression computation; system catalog columns.
    - Types that do not support vectorization: POINTOID; LSEGOID; BOXOID; LINEOID; CIRCLEOID; POLYGONOID; PATHOID; user-defined types.

- MOT tables do not support transition to vectorized execution.