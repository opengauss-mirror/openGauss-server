# Vector Data Types

**Table 1** Vector types  
The DataVec vector engine supports the following vector data types.

| Name | Dimension Limit | Description |
| ---- | --------------- | ----------- |
| `vector[(d)]` | 1–16,000 | A single-precision floating-point vector with an optional dimension `d`. |
| `bit[(d)]` | 1–83,886,080 | A bit vector with an optional dimension `d`. |
| `sparsevec[(d)]` | 1–1,000,000,000<br>Maximum number of non-zero elements: 16,000 | A sparse vector with an optional dimension `d`. |
| `halfvec[(d)]` | 1–16,000 | A half-precision floating-point vector with an optional dimension `d`. |

> [!NOTE]  
> The dimension limits above apply only to vector data storage and vector computation in `TOAST` tables. They do not include the dimension limits for non-TOAST tables (with the column storage mode set to `plain`) or vector indexes. For details about the dimension limits, see [Vector Indexes](./vector_index.md).
>
> Storage space and dimension limits for non-TOAST tables:
>
> | Name | Storage Space | Dimension Limit |
> | ---- | ------------- | --------------- |
> | `vector[(d)]` | `(4 * dimension + 8)` bytes | 1–2,028 |
> | `bit[(d)]` | `(dimension / 8 + 8)` bytes | 1–64,896 |
> | `sparsevec[(d)]` | `(8 * number of non-zero elements + 16)` bytes | 1–1,000,000,000<br>Maximum number of non-zero elements: 1,013 |
> | `halfvec[(d)]` | `(2 * dimension + 8)` bytes | 1–4,092 |

## Vector

Each `vector` occupies `4 * dimension + 8` bytes of storage space.  
Each element is a single-precision floating-point number, and all elements must be finite. That is, they cannot be `NaN`, `Infinity`, or `-Infinity`.  
The maximum dimension of `vector` is `16,000`.

### Format

```sql
['<ELEMENT>', ..., '<ELEMENT>']::vector
```

* `ELEMENT`: element.

**Example 1:**

```
openGauss=# SELECT '[1,2,3]'::vector;
 vector
---------
 [1,2,3]
(1 row)
```

> [!NOTE]  
> The effective number of decimal digits of a single-precision floating-point number is approximately $\log_{10}(2^{24}) \approx 7.225$.

## Bit

Each `bit` vector occupies `dimension / 8 + 8` bytes of storage space.  
The maximum dimension of `bit` is `83,886,080`.

### Format

```sql
B'0/1, ...'
```

**Example 2:**

```sql
openGauss=# SELECT B'110';
 ?column?
----------
 110
(1 row)
```

## Sparsevec

Each `sparsevec` occupies `8 * number of non-zero elements + 16` bytes of storage space.  
Each element is a single-precision floating-point number, and all elements must be finite. That is, they cannot be `NaN`, `Infinity`, or `-Infinity`.  
The maximum number of non-zero elements in `sparsevec` is `16,000`, and the maximum dimension is `1,000,000,000`.

### Format

```sql
{INDEX:NON-ZERO, INDEX:NON-ZERO}/<DIMENSION>::sparsevec
```

- `INDEX`: index of a non-zero element. The index must be less than the specified `DIMENSION`.
- `NON-ZERO`: non-zero element.
- `DIMENSION`: dimension.

**Example 3:**

```sql
openGauss=# SELECT '{1:1.5,3:3.5}/5'::sparsevec;
    sparsevec
-----------------
 {1:1.5,3:3.5}/5
(1 row)
```

## `Halfvec`

Each `halfvec` occupies `2 * dimension + 8` bytes of storage space.  
Each element is a half-precision floating-point number, and all elements must be finite. That is, they cannot be `NaN`, `Infinity`, or `-Infinity`.  
The maximum dimension of `halfvec` is `16,000`.

### Format

```sql
['<ELEMENT>', ..., '<ELEMENT>']::halfvec
```

- `ELEMENT`: element.

**Example 4:**

```sql
openGauss=# SELECT '[1,2,3]'::halfvec;
 halfvec
---------
 [1,2,3]
(1 row)
```

## Dimension Limits

When a vector data dimension is specified when creating a table, the dimension of inserted data must match the dimension specified for the table. If no dimension is specified, there is no dimension limit for `vector`, `sparsevec`, or `halfvec` data types, while the default dimension for `bit` is 1.

**Example 5:**

```
# Specify the dimension for vector
openGauss=# CREATE TABLE test1(val vector(3));
CREATE TABLE
openGauss=# INSERT INTO test1 (val) VALUES ('[1,2,3]');
INSERT 0 1
openGauss=# INSERT INTO test1 (val) VALUES ('[1,2,3,4]');
ERROR:  expected 3 dimensions, not 4
CONTEXT:  referenced column: val

# Do not specify the dimension for vector
openGauss=# CREATE TABLE test2(val vector);
CREATE TABLE
openGauss=# INSERT INTO test2 (val) VALUES ('[1,2,3]');
INSERT 0 1
openGauss=# INSERT INTO test2 (val) VALUES ('[1,2,3,4]');
INSERT 0 1
```

```
# Specify the dimension for bit
openGauss=# CREATE TABLE test1(val bit(3));
CREATE TABLE
openGauss=# INSERT INTO test1 (val) VALUES ('101');
INSERT 0 1
openGauss=# INSERT INTO test1 (val) VALUES ('1010');
ERROR:  bit string length 4 does not match type bit(3)
CONTEXT:  referenced column: val

# Do not specify the dimension for bit
openGauss=# CREATE TABLE test2(val bit);
CREATE TABLE
openGauss=# INSERT INTO test2 (val) VALUES ('1');
INSERT 0 1
openGauss=# INSERT INTO test2 (val) VALUES ('1010');
ERROR:  bit string length 4 does not match type bit(1)
CONTEXT:  referenced column: val
```

```
# Specify the dimension for sparsevec
openGauss=# CREATE TABLE test1(val sparsevec(3));
CREATE TABLE
openGauss=# INSERT INTO test1 (val) VALUES ('{1:1,2:1,3:1}/3');
INSERT 0 1
openGauss=# INSERT INTO test1 (val) VALUES ('{1:1,2:1,3:1,4:1}/4');
ERROR:  expected 3 dimensions, not 4
CONTEXT:  referenced column: val

# Do not specify the dimension for sparsevec
openGauss=# CREATE TABLE test2(val sparsevec);
CREATE TABLE
openGauss=# INSERT INTO test2 (val) VALUES ('{1:1,2:1,3:1}/3');
INSERT 0 1
openGauss=# INSERT INTO test2 (val) VALUES ('{1:1,2:1,3:1,4:1}/4');
INSERT 0 1
```

```
# Specify the dimension for halfvec
openGauss=# CREATE TABLE test1(val halfvec(3));
CREATE TABLE
openGauss=# INSERT INTO test1 (val) VALUES ('[1,2,3]');
INSERT 0 1
openGauss=# INSERT INTO test1 (val) VALUES ('[1,2,3,4]');
ERROR:  expected 3 dimensions, not 4
CONTEXT:  referenced column: val

# Do not specify the dimension for halfvec
openGauss=# CREATE TABLE test2(val halfvec);
CREATE TABLE
openGauss=# INSERT INTO test2 (val) VALUES ('[1,2,3]');
INSERT 0 1
openGauss=# INSERT INTO test2 (val) VALUES ('[1,2,3,4]');
INSERT 0 1
```

## Constraints

- The `vector`, `halfvec`, and `sparsevec` data types are currently not supported for storage in column-store tables.
- The `vector`, `bit`, `sparsevec`, and `halfvec` data types are currently not supported for storage in foreign tables.
- Vector data storage is supported in ordinary row-store tables, temporary tables, `TOAST` tables, unlogged tables, segment-page tables, and other table types.
