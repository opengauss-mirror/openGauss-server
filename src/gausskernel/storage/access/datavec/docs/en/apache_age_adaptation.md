# Apache AGE (incubating) for openGauss

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T12:07:37.961Z pushedAt=2026-07-30T12:23:58.594Z -->

## Before You Start

Apache AGE is used by creating an extension.

```bash
create extension age;
```

After AGE is installed, the `ag_catalog` schema is created by default. All built-in data types and functions of AGE are stored under `ag_catalog`. Therefore, when using AGE, especially when executing `Cypher` statements, you need to first run the following command:

```bash
SET search_path TO ag_catalog;
```

You also need to run the `load 'age'` command to ensure that all hooks of the AGE extension are loaded for graph data integrity.

```bash
load 'age';
```

>[!NOTE]
>
>Before creating and using the AGE extension, you need to disable the thread pool and set `enable_thread_pool = off`.
>

## 1. Graph Operations

A graph consists of a set of vertices and edges, where each vertex and edge has a property map. A vertex is the basic object of a graph, which can exist independently of anything else in the graph. An edge creates a directed connection between two vertices.

### 1.1 Creating a Graph

To create a graph, use `create_graph(graph_name)`:

```bash
SELECT * FROM ag_catalog.create_graph('graph_name');
```

### 1.2 Deleting a Graph

To delete a graph, use `drop_graph(graph_name, cascade)`.

> 1. The first parameter `graph_name` is the graph to be deleted, and the second parameter `cascade` is a boolean value indicating whether to delete graph label and data.
> 2. It is recommended to set `cascade` to `true`; otherwise, all contents in the graph must be manually deleted using SQL DDL commands
>

```bash
SELECT * FROM ag_catalog.drop_graph('graph_name', true);
```

### 1.3 Creating Vertex Labels in a Graph

To create a vertex label in a graph, use `create_vlabel(graph_name, label_name)`.

```bash
SELECT * FROM ag_catalog.create_vlabel('graph_name','label_name');
```

Whenever a vertex label is created using the `create_vlabel() function`, a corresponding table named `<label_name>` is generated within the namespace `new_graph`. The same behavior applies to `create_elabel()` for creating edge labels. These two functions are not mandatory. if a label table does not already exist when creating vertices or edges via Cypher, it will be created automatically.

### 1.4 Creating Edge Labels in a Graph

To create a graph, use `create_elabel(graph_name, label_name)`.

```bash
SELECT * FROM ag_catalog.create_elabel('graph_name','label_name');
```

## 2. Graph Storage

### 2.1 Graph

After a graph is created, the kernel creates a schema with the same name as the graph. Meanwhile, a record is inserted into the `ag_catalog.ag_graph` table, marking the newly created schema aas the storage location for the graph data.

```bash
SELECT create_graph('new_graph');

NOTICE:  graph "new_graph" has been created
 create_graph 
--------------

(1 row)

SELECT * FROM ag_catalog.ag_graph;

   name    | namespace 
-----------+-----------
 new_graph | new_graph
(1 row)
```

### Vertices and Edges in a Graph

```bash
-- After a graph is created, two tables, _ag_label_vertex and _ag_label_edge, are created in the schema corresponding to the graph as the default vertex table and edge table. Meanwhile, two records are inserted into ag_catalog.ag_label to associate the vertex table and edge table with the schema.
SELECT * FROM ag_catalog.ag_label;

       name       | graph | id | kind |          relation          
------------------+-------+----+------+----------------------------
 _ag_label_vertex | 68484 |  1 | v    | new_graph._ag_label_vertex 
 _ag_label_edge   | 68484 |  2 | e    | new_graph._ag_label_edge   
(2 rows)

-- Create a vertex table.
SELECT create_vlabel('new_graph', 'Person');
NOTICE:  VLabel "Person" has been created
 create_vlabel 
---------------
 
(1 row)

-- After a vertex table is created, a record is inserted into the ag_catalog.ag_label table to associate the vertex table with the graph. The kind 'v' represents a vertex table and the kind 'e' represents an edge table.
SELECT * FROM ag_catalog.ag_label;
       name       | graph | id | kind |          relation          
------------------+-------+----+------+----------------------------
 _ag_label_vertex | 68484 |  1 | v    | new_graph._ag_label_vertex 
 _ag_label_edge   | 68484 |  2 | e    | new_graph._ag_label_edge   
 Person           | 68484 |  3 | v    | new_graph."Person"         
(3 rows)
```

## 3. Cypher Query

Cypher statements cannot be executed directly in the database.Instead, they must be passed as arguments to the `cypher()` function, which then returns a `SETOF records`.

### 3.1 Introduction to cypher()

cypher(graph_name, query_string, parameters)
> `graph_name` is the graph to query. `query_string` is the Cypher statement. `parameters` is optional and can only be used with `Prepared Statements`; otherwise, an error will be thrown.

```bash
SELECT * FROM cypher('graph_name', $$ 
/* Cypher Query Here */ 
$$) AS (result1 agtype, result2 agtype);
```

**Note**
>
> 1. `(result1 agtype, result2 agtype)` after `AS`, i.e., `SETOF records`, must match the number of return values in the Cypher statement.
> 2. `SELECT * FROM cypher` cannot be written as `SELECT cypher`.
> 3. Before executing the statement, execute `load 'age'` and `set search_path = ag_catalog`.

## Data Types and Functions

The following provides a brief description of the data types, Cypher statements, and functions included in AGE.

### 1 Data Types

AGE creates two data types: `graphid` and `agtype`. `graphid` represents the unique ID identifier for vertices and edges. `agtype` is the core data type of AGE. For more details, refer to <https://age.apache.org/age-manual/master/intro/types.html>.

#### 1.1 Simple Data Types

Simple data types include `Null`, `Integer`, `Float`, `Numeric`, `Bool`, and `String`.

#### 1.2 Composite Data Types

Composite data types include `List` and `Map`.

- `List` operations

| No. | Supported Operation | Description |
|:--------|:---------|--------:|
| 1   |List in general | Ordinary list |
| 2   |NULL in a List | List with null |
| 3   |Access Individual Elements | Access an element of a list |
| 4   |MapElements in Lists | List elements containing map structures |
| 5   |Accessing Map Elements in Lists | Access the value of a map within a list element |
| 6   |Negative Index Access | Negative index access |
| 7   |Index Ranges  | Index ranges |
| 8   |Negative Index Ranges | Negative index ranges |
| 9   |Positive Slices | Positive slice  |
| 10  |Negative Slices | Negative slice  |

- `Map` operations

| No. | Supported Operation | Description |
|:--------|:---------|--------:|
|1 | Literal Maps with SimpleDataTypes | Ordinary map type |
|2 | Literal Maps with composite Data Types | Map containing composite data types |
|3 | Property Access of a map | Map property access |
|4 | Accessing List Elements in Maps | List element access in a map |

#### 1.3 Simple Entities

Simple entity types include `GraphId`, `Labels,` and `Properties`, and simple entity types can further compose `Vertex`, `Edge`, and `Composite Entities`.

#### 1.4 Vertex

A `Vertex` is the fundamental building block of a graph, representing a node.

#### 1.5 Edge

An `Edge` is the basic building block of a graph, representing an edge.

#### 1.6 Composite Entities

A `Path` composed of `Vertex` and `Edge`.

### 2 Cypher Statements

For detailed information on AGE's support for Cypher statements, refer to <https://age.apache.org/age-manual/master/clauses/match.html>.

#### 2.1 Match

| No. | Supported Operation | Description |
|:--------|:---------|:--------|
|1 | get all vertices | Obtain all vertices |
|2 | get all vertices with a label | Obtain all vertices of a certain label type |
|3 | related vertices | Obtain related vertices (neighbor vertices) through edges |
|4 | match with labels | Filter related vertices by label. |
|5 | Outgoing Edges | Outgoing edges support |
|6 | Directed Edges and variable | Directed edges and variable support |
|7 | Match on edge type | Filter edges by label type |
|8 | Match on edge type and use a variable | Filter edges by label type and set a variable |
|9 | Multiple Edges | Multiple edge matching |
|10 | Variable Length Edges | Variable-length path matching |

#### 2.2 WITH

| No. | Supported Operation | Description |
|:--------|:---------|:--------|
|1 |Filter on aggregate function results |Filter results through aggregate functions|
|2 |Sort results before using collect on them |Sort before collect|
|3 |Limit branching of a path search |Match paths, limit them to a certain number, and then use these paths as a basis for matching again|

#### 2.3 SKIP

| Number | Supported Operation | Description |
|:--------|:---------|:--------|
|1 |skip first three rows |Skip the first three rows|
|2 |Return middle tow rows |Return the middle tow rows, working with limit|
|3 |Using an expression with SKIP to return a subset of the rows |Use an expression with SKIP to return a subset of rows|

#### 2.4 LIMIT

| No. | Supported Operation | Description |
|:--------|:---------|:--------|
|1 |Return a subset of the rows |Return a subset of the query results|
|2 |Using an expression with LIMIT to return a subset of the rows |Use an expression with LIMIT to return a subset of rows|

#### 2.5 Return

| No. | Supported Operation | Description |
|:--------|:---------|:--------|
|1 | Return nodes | Return the queried nodes |
|2 | Return edges | Return the queried edges |
|3 | Return property | Return the property of a node or an edge |
|4 | Return all elements | Return all elements |
|5 | Variable with uncommon characters | Support variables with uncommon characters |
|6 | Aliasing a field | Alias a return value |
|7 | unique results | Return value of distinct |

#### 2.6 ORDER BY

| # | Supported Operation | Description |
|:--------|:---------|:--------|
|1 | Order nodes by property | Sort nodes by a single property |
|2 | Order nodes by multiple properties | Sort nodes by multiple properties |
|3 | Order nodes in descending order | Sort nodes in descending order |
|4 | Ordering null | Sort null values in ascending order |

#### 2.7 CREATE

| No. | Supported Operation | Description |
|:--------|:---------|:--------|
| 1 | Create single vertex | Create a single vertex |
| 2 | Create multiple vertices | Create multiple vertices |
| 3 | Create a vertex with a label | Create a vertex with a label |
| 4 | Create vertex and add labels and properties | Create a vertex with labels and properties |
| 5 | Return create node | Create and return a vertex |
| 6 | Create an edge between two nodes | Create an edge |
| 7 | Create an edge and set properties | Create an edge and set properties |
| 8 | Create a full path | Create a full path |

#### 2.8 SET

| No. | Supported Operation | Description |
|:--------|:---------|:--------|
|1 |Set a property |Set a single property|
|2 |Return created vertex |Return the modified vertex|
|3 |Remove a property |Remove a property|
|4 |Set multiple properties using one SET clause |Set multiple properties|

#### 2.9 REMOVE

| Name | Supported Operation | Description |
|:--------|:---------|:--------|
|1 |Remove a property |Remove a property|

#### 2.10 DELETE

| No. | Supported Operation | Description |
|:--------|:---------|:--------|
|1 |Delete single vertex |Delete a single vertex|
|2 |Delete all vertices and edges |Delete all vertices and edges|
|3 |Delete edges only |Delete edges|
|4 |Return a deleted vertex |Return a deleted vertex|

### 3 Functions

Functions primarily involve operations on `agtype` and the generation of expressions. For details, refer to <https://age.apache.org/age-manual/master/functions/predicate_functions.html>.

#### 3.1 Predicate Functions

| No. | Supported Operation | Description |
|:--------|:---------|:--------|
|1 |Exists(Property) |Check whether a property exists|
|2 |Exists(Path) |Check whether a query path exists|

#### 3.2 Scalar Functions

| No. | Supported Operation | Description |
|:--------|:---------|:--------|
|1 |id |Return the ID of a vertex or an edge|
|2 |start_id |Return the ID of the starting vertex of an edge|
|3 |end_id |Return the ID of the ending vertex of an edge|
|4 |type |Return the string representation of the edge type|
|5 |properties |Return an agtype map containing all properties of a vertex or an edge. If the argument is already a map, return it unchanged|
|6 |head |Return the first element in an agtype list|
|7 |last |Return the last element in an agtype list|
|8 |length |Return the length of a path|
|9 |size |Return the length of a list|
|10 |startNode |Return the starting node of an edge|
|11 |endNode |Return the ending node of an edge|
|12 |timestamp |Return the difference, measured in milliseconds, between the current time and midnight, January 1, 1970 UTC|
|13 |toBoolean |Convert a string value to a boolean value|
|14 |toFloat |Convert an integer or string value to a floating-point number|
|15 |toInteger |Convert a floating-point or string value to an integer value|
|15 |coalesce |Return the first non-null value in the given expression list|

#### 3.3 List Functions

| No. | Supported Operation | Description |
|:--------|:---------|:--------|
|1 |keys |Return a list containing the string representation of all property names of a vertex, edge, or map|
|2 |range |Return a list containing all integer values within the range bounded by a start value and end value|
|3 |labels |Return a list containing the string representation of all labels of a node|
|4 |relationships |Return a list containing all relationships in a path|
|5 |nodes |Return a list containing all vertices in a path|

#### 3.4 Numeric Functions

| No. | Supported Operation | Description |
|:--------|:---------|:--------|
|1 |rand |Return a random floating-point number in the range from 0 (inclusive) to 1 (exclusive); i.e., [0,1)|
|2 |abs |Return the absolute value of the given number|
|3 |ceil |Return the smallest floating-point number that is greater than or equal to the given number and equal to a mathematical integer|
|4 |floor |Return the largest floating-point number that is less than or equal to the given number and equal to a mathematical integer|
|5 |round |Return the value of the given number rounded to the nearest integer|
|6 |sign |Return the sign of the given number|

#### 3.5 Logarithmic Functions

| No. | Supported Operation | Description |
|:--------|:---------|:--------|
|1 |e |Return the base of the natural logarithm|
|2 |sqrt |Return the square root of a number|
|3 |exp |Return e^n, where e is the base of the natural logarithm and n is the value of the argument expression|
|4 |log |Return the natural logarithm of a number|
|5 |log10 |Return the common logarithm (base 10) of a number|

#### 3.6 Trigonometric Functions

| No. | Supported Operation | Description |
|:--------|:---------|:--------|
|1 |degrees |Convert radians to degrees|
|2 |radians |Convert degrees to radians|
|3 |pi |Return the mathematical constant pi|
|4 |sin |Return the sine of a number|
|5 |cos |Return the cosine of a number|
|6 |tan |Return the tangent of a number|
|7 |cot |Return the cotangent of a number|
|8 |asin |Return the arcsine of a number|
|9 |acos |Return the arccosine of a number|
|10| atan |Return the arctangent of a number|
|11| atan2 |Return the arctangent of a set of coordinates in radians|

#### 3.7 String Functions

| # | Supported Operation | Description |
|:--------|:---------|:--------|
|1 |replace |Return returns a string in which all occurrences of a specified string in the original string have been replaced by another (specified) string|
|2 |split |Return a list of strings obtained by splitting the original string based on matches of a given delimiter|
|3 |left |Return a string containing the specified number of leftmost characters of the original string|
|4 |right |Return a string containing the specified number of rightmost characters of the original string|
|5 |substring |Return a substring of the original string, starting at a 0-based index and with a specified length|
|6 |rTrim |Return the original string with trailing whitespace removed|
|7 |lTrim |Return the original string with leading whitespace removed|
|8 |trim |Return the original string with leading and trailing whitespace removed|
|9 |toLower |Return the original string in lowercase|
|10 |toUpper |Return the original string in uppercase|
|11 |reverse |Return a string in which the order of all characters in the original string has been reversed|

#### 3.8 Aggregation Functions

| No. | Supported Operation | Description |
|:--------|:---------|:--------|
|1 |min |Return the minimum value in a set of values|
|2 |max |Return the maximum value in a set of values|
|3 |stDev |Return the standard deviation of the given values in a group|
|4 |stDevP |Return the standard deviation of the given values in a group|
|5 |percentileCont |Return the percentile of the given values in a group|
|6 |percentileDisc |Return the percentile of the given values in a group|
|7 |count |Return the number of values or records|
|8 |avg |Return the average of a set of numeric values|
|9 |sum |Return the sum of a set of numeric values|
