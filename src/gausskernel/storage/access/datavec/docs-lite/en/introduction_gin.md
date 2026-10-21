# Introduction

GIN stands for generalized inverted index. It is designed to handle cases where the indexed item is a composite value, and queries need to search the index for specific element values that appear within the composite value. For example, a document is composed of multiple words, and it is necessary to query for documents that contain a specific word.

The term "item" is used to represent the composite value being indexed, and "key" represents an element value. GIN is used to store and search for keys, rather than items.

A GIN index stores a series of (key, posting list) key-value pairs, where the posting list is a set of row IDs in which the key appears. Since each item may contain multiple keys, the same row ID may appear in multiple posting lists, while each key value is stored only once. Therefore, when the same key appears multiple times within an item, the GIN index is very compact.

Because the access method of a GIN index does not require understanding of how it operates, the GIN index is generic. A GIN index uses strategies defined for specific data types. The strategies define how to extract keys from index options and query conditions, and how to determine whether a row that contains certain key values in the query actually satisfies the query condition.
