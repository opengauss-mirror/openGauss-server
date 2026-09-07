# Vector Functions and Operators

## Vector

### Vector Operators

| Operator | Description                   |
| ---------- | --------------------------- |
| `+`        | Element-wise addition       |
| `-`        | Element-wise subtraction    |
| `*`        | Element-wise multiplication |
| `=`        | Equal                       |
| `<>`       | Not equal                   |
| \|\|       | Vector concatenation        |
| `<->`      | Euclidean distance (L2)     |
| `<#>`      | Negative inner product      |
| `<=>`      | Cosine distance             |
| `<+>`      | Manhattan distance (L1)     |

- `+`

  Description: element-wise addition.

  Example:

  ```
  openGauss=> select '[1,2,3]'::vector + '[4,5,6]';
   ?column? 
  ----------
   [5,7,9]
  (1 row)
  ```

- `-`

  Description: element-wise subtraction.

  Example:

  ```
  openGauss=> select '[1,2,3]'::vector - '[4,5,6]';
    ?column?  
  ------------
   [-3,-3,-3]
  (1 row)
  ```

- `*`

  Description: element-wise multiplication.

  Example:

  ```
  openGauss=> select '[1,2,3]'::vector * '[4,5,6]';
   ?column?  
  -----------
   [4,10,18]
  (1 row)
  ```

- `=`

  Description: equal.

  Example:

  ```
  openGauss=# select '[1,2,3]'::vector = '[4,5,6]';
   ?column? 
  ----------
   f
  (1 row)
  ```

- `<>`

  Description: not equal.

  Example:

  ```
  openGauss=# select '[1,2,3]'::vector <> '[4,5,6]';
   ?column? 
  ----------
   t
  (1 row)
  ```

- `||`

  Description: vector concatenation.

  Example:

  ```
  openGauss=> select '[1,2,3]'::vector || '[4,5,6]';
     ?column?    
  ---------------
   [1,2,3,4,5,6]
  (1 row)
  ```

- `<->`

  Description: Euclidean distance (L2).

  Example:

  ```
  openGauss=# SELECT '[0,0]'::vector <-> '[3,4]';
   ?column? 
  ----------
          5
  (1 row)
  ```

- `<#>`

  Description: negative inner product.

  Example:

  ```
  openGauss=# SELECT '[1,2]'::vector <#> '[3,4]';
   ?column? 
  ----------
        -11
  (1 row)
  ```

- `<=>`

  Description: cosine distance.

  Example:

  ```
  openGauss=# SELECT '[1,2]'::vector <=> '[2,4]';
   ?column? 
  ----------
          0
  (1 row)
  ```

- `<+>`

  Description: Manhattan distance.

  Example:

  ```
  openGauss=# SELECT '[0,0]'::vector <+> '[3,4]';
   ?column? 
  ----------
          7
  (1 row)
  ```

### Vector Functions

| Function                                             | Description             |
| ---------------------------------------------------- | ----------------------- |
| `binary_quantize(vector) → bit`                      | Binary quantization     |
| `cosine_distance(vector, vector) → double precision` | Cosine distance         |
| `inner_product(vector, vector) → double precision`   | Inner product           |
| `l1_distance(vector, vector) → double precision`     | Manhattan distance (L1) |
| `l2_distance(vector, vector) → double precision`     | Euclidean distance (L2) |
| `l2_normalize(vector) → vector`                      | L2 normalization        |
| `subvector(vector, integer, integer) → vector`       | Subvector extraction    |
| `vector_dims(vector) → integer`                      | Number of dimensions    |
| `vector_norm(vector) → double precision`             | Euclidean norm          |

- `binary_quantize`

  Description: performs binary quantization. Elements greater than 0 are quantized to 1, and elements less than or equal to 0 are quantized to 0.

  Return type: `bit`

  Example:

  ```
  openGauss=# SELECT binary_quantize('[1,0,-1]'::vector);
   binary_quantize 
  -----------------
   100
  (1 row)
  ```

- `cosine_distance`

  Description: cosine distance.

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT cosine_distance('[1,2]'::vector, '[2,4]');
   cosine_distance 
  -----------------
                   0
  (1 row)
  ```

- `inner_product`

  Description: inner product.

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT inner_product('[1,2]'::vector, '[3,4]');
   inner_product 
  ---------------
              11
  (1 row)
  ```

- `l1_distance`

  Description: Manhattan distance (L1).

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT l1_distance('[0,0]'::vector, '[3,4]');
   l1_distance 
  -------------
             7
  (1 row)
  ```

- `l2_distance`

  Description: Euclidean distance (L2).

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT l2_distance('[0,0]'::vector, '[3,4]');
   l2_distance 
  -------------
             5
  (1 row)
  ```

- `l2_normalize`

  Description: L2 normalization.

  Return type: `vector`

  Example:

  ```
  openGauss=# SELECT l2_normalize('[3,4]'::vector);
   l2_normalize 
  --------------
   [0.6,0.8]
  (1 row)
  ```

- `subvector`

  Description: subvector extraction.

  Return type: `vector`

  Example:

  ```
  openGauss=# SELECT subvector('[1,2,3,4,5]'::vector, 1, 3);
   subvector 
  -----------
   [1,2,3]
  (1 row)
  ```

- `vector_dims`

  Description: number of dimensions.

  Return type: `int`

  Example:

  ```
  openGauss=# SELECT vector_dims('[1,2,3]'::vector);
   vector_dims 
  -------------
             3
  (1 row)
  ```

- `vector_norm`

  Description: Euclidean norm.

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT vector_norm('[3,4]');
   vector_norm 
  -------------
             5
  (1 row)
  ```

### Vector Aggregate Functions

| Function               | Description |
| ---------------------- | ----------- |
| `avg(vector) → vector` | Average     |
| `sum(vector) → vector` | Sum         |

- `avg`

  Description: average.

  Return type: `vector`

  Example:

  ```
  openGauss=# SELECT avg(v) FROM unnest(ARRAY['[1,2,3]'::vector, '[3,5,7]']) v;
      avg    
  -----------
   [2,3.5,5]
  (1 row)
  ```

- `sum`

  Description: sum.

  Return type: `vector`

  Example:

  ```
  openGauss=# SELECT sum(v) FROM unnest(ARRAY['[1,2,3]'::vector, '[3,5,7]', NULL]) v;
     sum    
  ----------
   [4,7,10]
  (1 row)
  ```

### Vector Type Conversion

- Format:

  ```
  SELECT ITEM::vector;

  SELECT vector(ITEM);

  SELECT cast(ITEM AS vector);
  ```

> [!NOTE]  
> `vector()` supports conversion only from `text` or `vector`.

#### Converting TEXT/VARCHAR to vector

- Example:

  ```
  openGauss=# SELECT '[1,2,3]'::vector;
   vector  
  ---------
   [1,2,3]
  (1 row)

  openGauss=# select vector('[4,5,6]');
   vector  
  ---------
   [4,5,6]
  (1 row)

  openGauss=# SELECT cast(ARRAY[1,2,3] AS vector);
    array  
  ---------
   [1,2,3]
  (1 row)
  ```

#### Converting an int array to vector

- Example:

  ```
  openGauss=# SELECT ARRAY[1,2,3]::vector;
    array  
  ---------
   [1,2,3]
  (1 row)
  ```

#### Converting a real array to vector

- Example:

  ```
  openGauss=# SELECT ARRAY[1,2,3]::float4[]::vector(3);
    array  
  ---------
   [1,2,3]
  (1 row)
  ```

#### Converting a double array to vector

- Example:

  ```
  openGauss=# SELECT ARRAY[1,2,3]::float8[]::vector;
    array  
  ---------
   [1,2,3]
  (1 row)
  ```

#### Converting a numeric array to vector

- Example:

  ```
  openGauss=# SELECT ARRAY[1,2,3]::numeric[]::vector;
    array  
  ---------
   [1,2,3]
  (1 row)
  ```

> [!NOTE]  
> The length of the source array is checked during conversion only when the dimension of the target vector type is specified, such as `ARRAY[1,2,3]::vector(3)`.

#### Converting vector to an int array

- Example:

  ```
  openGauss=# SELECT '[1,2,3]'::vector::int[];
    int4   
  ---------
   {1,2,3}
  (1 row)
  ```

#### Converting vector to a real array

- Example:

  ```
  openGauss=# SELECT '[1,2,3]'::vector::real[];
   float4  
  ---------
   {1,2,3}
  (1 row)
  ```

#### Converting vector to a double array

- Example:

  ```
  openGauss=# SELECT '[1,2,3]'::vector::float8[];
   float8  
  ---------
   {1,2,3}
  (1 row)
  ```

#### Converting vector to a numeric array

- Example:

  ```
  openGauss=# SELECT '[1,2,3]'::vector::numeric(10,3)[];
         numeric       
  ---------------------
   {1.000,2.000,3.000}
  (1 row)
  ```

#### Converting vector to a text array

- Example:

  ```
  openGauss=# SELECT '[1,2,3]'::vector::text[];
      text       
  ---------
   {1,2,3}
  (1 row)
  ```

#### Converting vector to a varchar array

- Example:

  ```
  openGauss=# SELECT '[1.21,2.32,3]'::vector::varchar(3)[];
      varchar       
  --------------
   {1.2,2.3,3}
  (1 row)
  ```

> [!NOTE]  
> Conversion from `vector` to a character array supports only `text` and `varchar`. Other character types are not currently supported.

## Bit

### Bit Operators

| Operator | Description      |
| -------- | ---------------- |
| `<~>`    | Hamming distance |
| `<%>`    | Jaccard distance |

- `<~>`

  Description: Hamming distance.

  Return type: `uint64`

  Example:

  ```
  openGauss=# SELECT '111' <~> '110';
   ?column? 
  ----------
          1
  (1 row)
  ```

- `<%>`

  Description: Jaccard distance.

  Return type: `double`

  Example:

  ```
  openGauss=# SELECT '1111' <%> '1000';
   ?column? 
  ----------
        .75
  (1 row)
  ```

### Bit Functions

| Function                                        | Description      |
| ----------------------------------------------- | ---------------- |
| `hamming_distance(bit, bit) → double precision` | Hamming distance |
| `jaccard_distance(bit, bit) → double precision` | Jaccard distance |

- `hamming_distance`

  Description: Hamming distance.

  Return type: `uint64`

  Example:

  ```
  openGauss=# SELECT hamming_distance('111', '110');
   hamming_distance 
  ------------------
                  1
  (1 row)
  ```

- `jaccard_distance`

  Description: Jaccard distance.

  Return type: `double`

  Example:

  ```
  openGauss=# SELECT jaccard_distance('1111', '1110');
   jaccard_distance 
  ------------------
                .25
  (1 row)
  ```

## Sparsevec

### Sparsevec Operators

| Operator | Description             |
| -------- | ----------------------- |
| `<->`    | Euclidean distance (L2) |
| `<#>`    | Negative inner product  |
| `<=>`    | Cosine distance         |
| `<+>`    | Manhattan distance (L1) |
| `=`      | Equal                   |
| `<>`     | Not equal               |

- `<->`

  Description: Euclidean distance (L2).

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT '{}/2'::sparsevec <-> '{1:3,2:4}/2';
   ?column? 
  ----------
          5
  (1 row)
  ```

- `<#>`

  Description: negative inner product.

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT '{1:1,2:2}/2'::sparsevec <#> '{1:3,2:4}/2';
   ?column? 
  ----------
        -11
  (1 row)
  ```

- `<=>`

  Description: cosine distance.

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT '{1:1,2:2}/2'::sparsevec <=> '{1:2,2:4}/2';
   ?column? 
  ----------
          0
  (1 row)
  ```

- `<+>`

  Description: Manhattan distance (L1).

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT '{}/2'::sparsevec <+> '{1:3,2:4}/2';
   ?column? 
  ----------
          7
  (1 row)
  ```

- `=`

  Description: equal.

  Example:

  ```
  openGauss=# SELECT '{1:1,2:2,3:3}/3'::sparsevec = '{1:1,2:2,3:3}/3';
   ?column? 
  ----------
   t
  (1 row)
  ```

- `<>`

  Description: not equal.

  Example:

  ```
  openGauss=# SELECT '{1:1,2:2,3:3}/3'::sparsevec <> '{1:1,2:2,3:3}/3';
   ?column? 
  ----------
   f
  (1 row)
  ```

### Sparsevec Functions

| Function                                                   | Description             |
| ---------------------------------------------------------- | ----------------------- |
| `cosine_distance(sparsevec, sparsevec) → double precision` | Cosine distance         |
| `inner_product(sparsevec, sparsevec) → double precision`   | Inner product           |
| `l1_distance(sparsevec, sparsevec) → double precision`     | Manhattan distance (L1) |
| `l2_distance(sparsevec, sparsevec) → double precision`     | Euclidean distance (L2) |
| `l2_norm(sparsevec) → double precision`                    | Euclidean norm          |
| `l2_normalize(sparsevec) → sparsevec`                      | L2 normalization        |

- `cosine_distance`

  Description: cosine distance.

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT cosine_distance('{1:1,2:2}/2'::sparsevec, '{1:2,2:4}/2');
   cosine_distance 
  -----------------
                   0
  (1 row)
  ```

- `inner_product`

  Description: inner product.

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT inner_product('{1:1,2:2}/2'::sparsevec, '{1:2,2:4}/2');
   inner_product 
  ---------------
              10
  (1 row)
  ```

- `l1_distance`

  Description: Manhattan distance (L1).

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT l1_distance('{}/2'::sparsevec, '{1:3,2:4}/2');
   l1_distance 
  -------------
             7
  (1 row)
  ```

- `l2_distance`

  Description: Euclidean distance (L2).

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT l2_distance('{}/2'::sparsevec, '{1:3,2:4}/2');
   l2_distance 
  -------------
             5
  (1 row)
  ```

- `l2_norm`

  Description: Euclidean norm.

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT l2_norm('{1:3,2:4}/2'::sparsevec);
   l2_norm 
  ---------
         5
  (1 row)
  ```

- `l2_normalize`

  Description: L2 normalization.

  Return type: `sparsevec`

  Example:

  ```
  openGauss=# SELECT l2_normalize('{1:3,2:4}/2'::sparsevec);
    l2_normalize   
  -----------------
   {1:0.6,2:0.8}/2
  (1 row)
  ```

### Sparsevec Type Conversion

- Format:

  ```
  SELECT ITEM::sparsevec;

  SELECT sparsevec(ITEM);

  SELECT cast(ITEM AS sparsevec);
  ```

> [!NOTE]  
> `sparsevec()` supports conversion only from `text` or `sparsevec`.

#### Converting TEXT/VARCHAR to sparsevec

- Example:

  ```
  openGauss=# SELECT '{1:1.5,3:3.5}/5'::sparsevec;
      sparsevec    
  -----------------
   {1:1.5,3:3.5}/5
  (1 row)
  ```

#### Converting vector to sparsevec

- Example:

  ```
  openGauss=# SELECT '[0,1.5,0,3.5,0]'::vector::sparsevec;
      sparsevec    
  -----------------
   {2:1.5,4:3.5}/5
  (1 row)
  ```

#### Converting sparsevec to vector

- Example:

  ```
  openGauss=# SELECT '{2:1.5,4:3.5}/5'::sparsevec::vector(5);
       vector      
  -----------------
   [0,1.5,0,3.5,0]
  (1 row)
  ```

## Halfvec

### Halfvec Operators

| Operator | Description                 |
| -------- | --------------------------- |
| `+`      | Element-wise addition       |
| `-`      | Element-wise subtraction    |
| `*`      | Element-wise multiplication |
| `=`      | Equal                       |
| `<>`     | Not equal                   |
| \|\|     | Vector concatenation        |
| `<->`    | Euclidean distance (L2)     |
| `<#>`    | Negative inner product      |
| `<=>`    | Cosine distance             |
| `<+>`    | Manhattan distance (L1)     |

- `+`

  Description: element-wise addition.

  Example:

  ```
  openGauss=> select '[1,2,3]'::halfvec + '[4,5,6]';
   ?column? 
  ----------
   [5,7,9]
  (1 row)
  ```

- `-`

  Description: element-wise subtraction.

  Example:

  ```
  openGauss=> select '[1,2,3]'::halfvec - '[4,5,6]';
    ?column?  
  ------------
   [-3,-3,-3]
  (1 row)
  ```

- `*`

  Description: element-wise multiplication.

  Example:

  ```
  openGauss=> select '[1,2,3]'::halfvec * '[4,5,6]';
   ?column?  
  -----------
   [4,10,18]
  (1 row)
  ```

- `=`

  Description: equal.

  Example:

  ```
  openGauss=# select '[1,2,3]'::halfvec = '[4,5,6]';
   ?column? 
  ----------
   f
  (1 row)
  ```

- `<>`

  Description: not equal.

  Example:

  ```
  openGauss=# select '[1,2,3]'::halfvec <> '[4,5,6]';
   ?column? 
  ----------
   t
  (1 row)
  ```

- `||`

  Description: vector concatenation.

  Example:

  ```
  openGauss=> select '[1,2,3]'::halfvec || '[4,5,6]';
     ?column?    
  ---------------
   [1,2,3,4,5,6]
  (1 row)
  ```

- `<->`

  Description: Euclidean distance (L2).

  Example:

  ```
  openGauss=# SELECT '[0,0]'::halfvec <-> '[3,4]';
   ?column? 
  ----------
          5
  (1 row)
  ```

- `<#>`

  Description: negative inner product.

  Example:

  ```
  openGauss=# SELECT '[1,2]'::halfvec <#> '[3,4]';
   ?column? 
  ----------
        -11
  (1 row)
  ```

- `<=>`

  Description: cosine distance.

  Example:

  ```
  openGauss=# SELECT '[1,2]'::halfvec <=> '[2,4]';
   ?column? 
  ----------
          0
  (1 row)
  ```

- `<+>`

  Description: Manhattan distance.

  Example:

  ```
  openGauss=# SELECT '[0,0]'::halfvec <+> '[3,4]';
   ?column? 
  ----------
          7
  (1 row)
  ```

### Halfvec Functions

| Function                                               | Description             |
| ------------------------------------------------------ | ----------------------- |
| `binary_quantize(halfvec) → bit`                       | Binary quantization     |
| `cosine_distance(halfvec, halfvec) → double precision` | Cosine distance         |
| `inner_product(halfvec, halfvec) → double precision`   | Inner product           |
| `l1_distance(halfvec, halfvec) → double precision`     | Manhattan distance (L1) |
| `l2_distance(halfvec, halfvec) → double precision`     | Euclidean distance (L2) |
| `l2_normalize(halfvec) → halfvec`                      | L2 normalization        |
| `subvector(halfvec, integer, integer) → halfvec`       | Subvector extraction    |
| `vector_dims(halfvec) → integer`                       | Number of dimensions    |

- `binary_quantize`

  Description: performs binary quantization. Elements greater than 0 are quantized to 1, and elements less than or equal to 0 are quantized to 0.

  Return type: `bit`

  Example:

  ```
  openGauss=# SELECT binary_quantize('[1,0,-1]'::halfvec);
   binary_quantize 
  -----------------
   100
  (1 row)
  ```

- `cosine_distance`

  Description: cosine distance.

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT cosine_distance('[1,2]'::halfvec, '[2,4]');
   cosine_distance 
  -----------------
                   0
  (1 row)
  ```

- `inner_product`

  Description: inner product.

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT inner_product('[1,2]'::halfvec, '[3,4]');
   inner_product 
  ---------------
              11
  (1 row)
  ```

- `l1_distance`

  Description: Manhattan distance (L1).

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT l1_distance('[0,0]'::halfvec, '[3,4]');
   l1_distance 
  -------------
             7
  (1 row)
  ```

- `l2_distance`

  Description: Euclidean distance (L2).

  Return type: `float8`

  Example:

  ```
  openGauss=# SELECT l2_distance('[0,0]'::halfvec, '[3,4]');
   l2_distance 
  -------------
             5
  (1 row)
  ```

- `l2_normalize`

  Description: L2 normalization.

  Return type: `halfvec`

  Example:

  ```
  openGauss=# SELECT l2_normalize('[3,4]'::halfvec);
   l2_normalize 
  --------------
   [0.6,0.8]
  (1 row)
  ```

- `subvector`

  Description: subvector extraction.

  Return type: `halfvec`

  Example:

  ```
  openGauss=# SELECT subvector('[1,2,3,4,5]'::halfvec, 1, 3);
   subvector 
  -----------
   [1,2,3]
  (1 row)
  ```

- `vector_dims`

  Description: number of dimensions.

  Return type: `int`

  Example:

  ```
  openGauss=# SELECT vector_dims('[1,2,3]'::halfvec);
   vector_dims 
  -------------
             3
  (1 row)
  ```

### Halfvec Aggregate Functions

| Function                 | Description |
| ------------------------ | ----------- |
| `avg(halfvec) → halfvec` | Average     |
| `sum(halfvec) → halfvec` | Sum         |

- `avg`

  Description: average.

  Return type: `halfvec`

  Example:

  ```
  openGauss=# SELECT avg(v) FROM unnest(ARRAY['[1,2,3]'::halfvec, '[3,5,7]']) v;
      avg    
  -----------
   [2,3.5,5]
  (1 row)
  ```

- `sum`

  Description: sum.

  Return type: `halfvec`

  Example:

  ```
  openGauss=# SELECT sum(v) FROM unnest(ARRAY['[1,2,3]'::halfvec, '[3,5,7]', NULL]) v;
     sum    
  ----------
   [4,7,10]
  (1 row)
  ```

### Halfvec Type Conversion

- Format:

  ```
  SELECT ITEM::halfvec;

  SELECT halfvec(ITEM);

  SELECT cast(ITEM AS halfvec);
  ```

#### Converting TEXT/VARCHAR to halfvec

- Example:

  ```
  openGauss=# SELECT '[1,2,3,4,5]'::halfvec;
     halfvec    
  -------------
   [1,2,3,4,5]
  (1 row)
  ```

#### Converting vector to halfvec

- Example:

  ```
  openGauss=# SELECT '[1,2,3,4,5]'::vector::halfvec;
     halfvec    
  -------------
   [1,2,3,4,5]
  (1 row)
  ```

#### Converting halfvec to vector

- Example:

  ```
  openGauss=# SELECT '[1,2,3,4,5]'::halfvec::vector(5);
      vector      
  -------------
   [1,2,3,4,5]
  (1 row)
  ```

> [!NOTE]  
> Vector data types use the same function names and operators as other data types. To ensure that vector operations are executed correctly, we recommend explicitly casting at least one argument, for example, `SELECT l2_distance('[0,0]'::vector, '[3,4]');`.
