# Vectorized Engine<a name="EN-US_TOPIC_0000001135302873"></a>

## Availability<a name="section3480125215575"></a>

This feature is available since openGauss 1.0.0.

## Introduction<a name="section5814521587"></a>

The vectorized execution engine, provided by openGauss, is usually used in OLAP data warehouse systems because analytical systems are usually data-intensive and access most data in a table in a sequential manner, perform calculation, and finally output a calculation result to an end user.

## Benefits<a name="section148987345811"></a>

Batch calculation greatly improves the performance of complex query.

## Description<a name="section117041846581"></a>

The traditional database query execution uses the tuple-based pipeline execution mode. In most time, the CPU is not used to actually process data, but to traverse the query operation tree. As a result, the effective utilization of the CPU is not high. This also results in low instruction cache performance and frequent jumps. Worse still, this approach does not take advantage of the new capabilities of the new hardware to speed up the execution of queries. In the execution engine, another solution is to change a tuple to a column at a time. This is also the basis of our vectorized execution engine.

The vectorized engine is bound to the column-store technology, because data of each column is stored together, and it may be considered that the data is stored in an array manner. Based on such a feature, when a same operation needs to be performed on the column data, calculation of each value of the data block may be efficiently completed by using a cycle.

The advantages of the vectorized execution engine are as follows:

- This reduces inter-node scheduling and improves CPU usage.
- Because the same type of data is put together, it is easier to leverage the new optimization features of hardware and compilation.

## Enhancements<a name="section21149265913"></a>

None

## Constraints<a name="section51513617597"></a>

None

## Dependencies<a name="section20491151513592"></a>

It depends on column store.
