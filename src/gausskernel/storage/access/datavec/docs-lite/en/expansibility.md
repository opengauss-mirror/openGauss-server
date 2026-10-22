# Extensibility

The GIN index interface implements a high-level abstraction, requiring that the access method implementer only needs to implement the semantics of the data type being accessed. The GIN layer itself handles concurrency, logging, and searching the tree structure.

All it takes to define a GIN access method is to implement several user-defined methods, which define the behavior of keys in the tree and the relationships between keys, the items to be indexed, and the queries that can use the index. In short, GIN combines extensibility with generality, code reuse, and a clean interface.

The operator class for implementing a GIN index has the following four methods:

- int compare(Datum a, Datum b)

    Compares two keys (not the indexed items) and returns an integer less than zero, zero, or greater than zero, indicating that the first key is less than, equal to, or greater than the second key. NULL is never passed to this function.

- Datum Datum \*extractValue\(Datum itemValue, int32 \*nkeys, bool \*\*nullFlags\)

    Given an item to be indexed, returns an array of corresponding keys. The number of returned keys must be stored in *nkeys. If any of the keys can be NULL, also allocate an array of *nkeys boolean elements, store its address in *nullFlags, and set the NULL values as needed. If all keys are non-NULL, *nullFlags can be left NULL (its initial value). If the item contains no keys, the return value can be NULL.

- Datum \*extractQuery\(Datum query, int32 \*nkeys, StrategyNumber n, bool \*\*pmatch, Pointer \*\*extra\_data, bool \*\*nullFlags, int32 \*searchMode\)

    Given a value to be queried, returns an array of corresponding keys. That is, query is the value on the right-hand side of an indexable operator whose left-hand side is the indexed field. n is the strategy number of the operator within the operator class. Often, extractQuery needs to consult n to determine the data type of query and the method for extracting key values. The number of returned keys must be stored in *nkeys. If any of the keys can be NULL, also allocate an array of *nkeys boolean elements, store its address in *nullFlags, and set the NULL values as needed. If all keys are non-NULL, *nullFlags can be left NULL (its initial value). If the query contains no keys, the return value can be NULL.

    searchMode is an output parameter that allows extractQuery to specify some details about how the search is to be performed. If *searchMode is set to GIN_SEARCH_MODE_DEFAULT (which is also the initial value of this parameter before the function is called), only those items that return at least one key are considered as candidate matches. If *searchMode is set to GIN_SEARCH_MODE_INCLUDE_EMPTY, in addition to items that contain at least one matching key, items that contain no keys at all are also considered as candidate matches. (This mode is useful for implementing operations such as "is a subset of".) If *searchMode is set to GIN_SEARCH_MODE_ALL, all non-NULL items in the index are considered as candidate matches, regardless of whether they match any of the returned keys.

    pmatch is an output parameter that allows support for partial match. If this parameter is used, extractQuery must allocate an array of *nkeys boolean elements and store the array address in *pmatch. Each element of the array should be set to TRUE if the corresponding key requires a partial match, and FALSE if no match is required. If *pmatch is set to NULL, it is assumed that GIN does not require partial match. This value is initialized to NULL before the function is called, so operator classes that do not support partial match can ignore this parameter.

    extra_data is an output parameter that allows extractQuery to pass additional data to the consistent and comparePartial methods. If it is used, extractQuery must allocate an array of *nkeys Pointer elements and store the array address in *extra_data, then store whatever it wants to attach in each individual pointer. This value is initialized to NULL before the function is called, so operator classes that do not need additional data can ignore this parameter. If *extra_data is set, the entire array is passed to the consistent method, and the appropriate element is passed to the comparePartial method.

- bool consistent(bool check[], StrategyNumber n, Datum query, int32 nkeys, Pointer extra_data[], bool *recheck, Datum queryKeys[], bool nullFlags[])

    Returns TRUE if the indexed item satisfies the query operator with StrategyNumber n. This function does not directly access the value of the indexed item, because GIN does not store the items exactly, but it needs to know which key values extracted from the query appear in the given indexed item. The check array has length nkeys, which is the same as the number of keys returned by the query's call to extractQuery. If the indexed item contains the corresponding query key, the corresponding element in the check array is TRUE. For example, if (check[i] == TRUE), it means that the i-th key of the result array of extractQuery appears in the indexed item. The original query is also passed in as a parameter, in case the consistent method needs to use it. The same applies to the queryKeys[] and nullFlags[] returned by the extractQuery function. extra_data is the additional data array returned by the extractQuery function, or NULL if there is none.

    When extractQuery returns a NULL key in queryKeys[], if the indexed item contains a NULL key, the corresponding element in check[] is TRUE. That is, the semantics of check[] are much like IS NOT DISTINCT FROM. If it is necessary to know whether it is a normal value match or a NULL match, the consistent function can examine the corresponding nullFlags[] element.

    On successful completion, *recheck should be set to TRUE if the heap tuple needs to be rechecked against the query operator, and FALSE if the index test is already exact. That is, a return value of FALSE guarantees that the heap tuple does not match the query; a return value of TRUE with *recheck set to FALSE guarantees that the heap tuple matches the query; a return value of TRUE with *recheck set to TRUE means that the heap tuple may match the query, so it needs to be fetched and rechecked against the query operator by directly comparing it with the original indexed item.

A GIN operator class can optionally provide a fifth function.

- int comparePartial(Datum partial_key, Datum key, StrategyNumber n, Pointer extra_data)

    Compares a partial match query key with an index key. Returns an integer whose sign indicates different meanings: less than zero means the index key does not match the query, but the index scan should continue; zero means the index key matches the query; greater than zero indicates that the index scan should be terminated because no more matches are possible. The strategy number n of the operator that generated the partially consistent query is provided here, in case the semantics are needed to determine when to end the scan. Likewise, extra_data is the corresponding element in the additional data array generated by extractQuery, or NULL if there is no corresponding element. NULL keys are never passed to this function.

To support "partial match" queries, an operator class must provide the comparePartial method, and when a partial match query is encountered, its extractQuery method must set the pmatch parameter. For details, see [partial match algorithm](./implement.md##zh-cn_topic_0283137368_zh-cn_topic_0237122201_zh-cn_topic_0059778495_s9dc41ea95b9144c38d709b0b9a43fe9e).

The actual data types of the various Datum values above vary depending on the operator class. The item value passed into extractValue is always the input type of the operator class, and all key value types must be the STORAGE type of this class. The type of the query parameter passed into extractQuery and consistent is the input type of the right operand of the class member operator identified by the strategy number. It does not need to be the same as the item type, as long as key values of the correct type can be extracted from it.
