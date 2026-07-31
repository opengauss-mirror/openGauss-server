# 向量函数和操作符

## Vector

### Vector 操作符

操作符 | 描述 
--- | --- 
\+ | 元素级加法
\- | 元素级减法
\* | 元素级乘法
\= | 等于
\<> | 不等于
\|\| | 向量拼接
<-> | 欧几里得距离 (L2)
<#> | 负内积
<=> | 余弦距离
<+> | 曼哈顿距离 (L1)

- \+

    描述：元素级加法。

    示例：

    ```
    openGauss=> select '[1,2,3]'::vector + '[4,5,6]';
     ?column? 
    ----------
     [5,7,9]
    (1 row)
    ```

- \-

    描述：元素级减法。

    示例：

    ```
    openGauss=> select '[1,2,3]'::vector - '[4,5,6]';
      ?column?  
    ------------
     [-3,-3,-3]
    (1 row)
    ```    

- \*

    描述：元素级乘法。

    示例：

    ```
    openGauss=> select '[1,2,3]'::vector * '[4,5,6]';
     ?column?  
    -----------
     [4,10,18]
    (1 row)
    ```    

- \=

    描述：等于。

    示例：

    ```
    openGauss=# select '[1,2,3]'::vector = '[4,5,6]';
     ?column? 
    ----------
     f
    (1 row)
    ``` 

- \<>

    描述：不等于。

    示例：

    ```
    openGauss=# select '[1,2,3]'::vector <> '[4,5,6]';
     ?column? 
    ----------
     t
    (1 row)
    ``` 

- \|\|

    描述：向量拼接。

    示例：

    ```
    openGauss=> select '[1,2,3]'::vector || '[4,5,6]';
       ?column?    
    ---------------
     [1,2,3,4,5,6]
    (1 row)
    ```         

- <->

    描述：欧几里得距离 (L2)。

    示例：

    ```
    openGauss=# SELECT '[0,0]'::vector <-> '[3,4]';
     ?column? 
    ----------
            5
    (1 row)
    ```      

- <#> 

    描述：负内积。

    示例：

    ```
    openGauss=# SELECT '[1,2]'::vector <#> '[3,4]';
     ?column? 
    ----------
          -11
    (1 row)
    ```        

- <=>

    描述：余弦距离。

    示例：

    ```
    openGauss=# SELECT '[1,2]'::vector <=> '[2,4]';
     ?column? 
    ----------
            0
    (1 row)
    ```      

- <+>

    描述：曼哈顿距离。

    示例：

    ```
    openGauss=# SELECT '[0,0]'::vector <+> '[3,4]';
     ?column? 
    ----------
            7
    (1 row)
    ```      

#### Vector 函数

函数 | 描述
--- | --- 
binary_quantize(vector) → bit | 二进制量化
cosine_distance(vector, vector) → double precision | 余弦距离
inner_product(vector, vector) → double precision | 内积
l1_distance(vector, vector) → double precision | 曼哈顿距离 (L1)
l2_distance(vector, vector) → double precision | 欧几里得距离 (L2)
l2_normalize(vector) → vector | 归一化（使用L2距离）
subvector(vector, integer, integer) → vector | 截取子向量
vector_dims(vector) → integer | 向量的维度数
vector_norm(vector) → double precision | 欧几里得范数

- binary_quantize

    描述：二进制量化，若元素大于0，则量化为1；若元素小于或等于0，则量化为0。

    返回类型：bit

    示例：

    ```
    openGauss=# SELECT binary_quantize('[1,0,-1]'::vector);
     binary_quantize 
    -----------------
     100
    (1 row)
    ```    

- cosine_distance

    描述：余弦距离。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT cosine_distance('[1,2]'::vector, '[2,4]');
     cosine_distance 
    -----------------
                   0
    (1 row)
    ```        

- inner_product

    描述：内积。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT inner_product('[1,2]'::vector, '[3,4]');
     inner_product 
    ---------------
                11
    (1 row)
    ```           

- l1_distance

    描述：曼哈顿距离 (L1)。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT l1_distance('[0,0]'::vector, '[3,4]');
     l1_distance 
    -------------
               7
    (1 row)
    ```      

- l2_distance

    描述：欧几里得距离 (L2)。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT l2_distance('[0,0]'::vector, '[3,4]');
     l2_distance 
    -------------
               5
    (1 row)
    ```       

- l2_normalize

    描述：归一化（使用L2距离）。

    返回类型：vector

    示例：

    ```
    openGauss=# SELECT l2_normalize('[3,4]'::vector);
     l2_normalize 
    --------------
     [0.6,0.8]
    (1 row)
    ```        

- subvector

    描述：截取子向量。

    返回类型：vector

    示例：

    ```
    openGauss=# SELECT subvector('[1,2,3,4,5]'::vector, 1, 3);
     subvector 
    -----------
     [1,2,3]
    (1 row)
    ```         

- vector_dims

    描述：向量的维度数。

    返回类型：int

    示例：

    ```
    openGauss=# SELECT vector_dims('[1,2,3]'::vector);
     vector_dims 
    -------------
               3
    (1 row)
    ```         

- vector_norm

    描述：欧几里得范数。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT vector_norm('[3,4]');
     vector_norm 
    -------------
               5
    (1 row)
    ```          

#### Vector 聚合函数

函数 | 描述
--- | --- 
avg(vector) → vector | 平均值 
sum(vector) → vector | 求和

- avg

    描述：平均值。

    返回类型：vector

    示例：

    ```
    openGauss=# SELECT avg(v) FROM unnest(ARRAY['[1,2,3]'::vector, '[3,5,7]']) v;
        avg    
    -----------
     [2,3.5,5]
    (1 row)
    ```     

- sum

    描述：求和。

    返回类型：vector

    示例：

    ```
    openGauss=# SELECT sum(v) FROM unnest(ARRAY['[1,2,3]'::vector, '[3,5,7]', NULL]) v;
       sum    
    ----------
     [4,7,10]
    (1 row)
    ```       

#### Vector 类型转换

- 格式：

```
SELECT ITEM::vetcor

SELECT vector(ITEM);

SELECT cast(ITEM AS vector);
```

>[!NOTE]说明
>vector()仅支持text或者同为vector类型转换

##### TEXT/VARCHAR 转 vector

- 示例：

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

##### Int Array 转 vector

- 示例：

```
openGauss=# SELECT ARRAY[1,2,3]::vector;
  array  
---------
 [1,2,3]
(1 row)
```

##### Real Array 转 vector

- 示例：

```
openGauss=# SELECT ARRAY[1,2,3]::float4[]::vector(3);
  array  
---------
 [1,2,3]
(1 row)
```

##### Double Array 转 vector

- 示例：

```
openGauss=# SELECT ARRAY[1,2,3]::float8[]::vector;
  array  
---------
 [1,2,3]
(1 row)
```

##### Numeric Array 转 vector

- 示例：

```
openGauss=# SELECT ARRAY[1,2,3]::numeric[]::vector;
  array  
---------
 [1,2,3]
(1 row)
```

>[!NOTE]说明
只有当注明转换向量类型维度时，如`ARRAY[1,2,3]::vector(3)`，会在转换时对前置类型进行维度效验。

##### Vector 转 Int Array

- 示例：

```
openGauss=# SELECT '[1,2,3]'::vector::int[];
  int4   
---------
 {1,2,3}
(1 row)
```

##### Vector 转 Real Array

- 示例：

```
openGauss=# SELECT '[1,2,3]'::vector::real[];
 float4  
---------
 {1,2,3}
(1 row)
```

##### Vector 转 Double Array

- 示例：

```
openGauss=# SELECT '[1,2,3]'::vector::float8[];
 float8  
---------
 {1,2,3}
(1 row)
```

##### Vector 转 Numeric Array

- 示例：

```
openGauss=# SELECT '[1,2,3]'::vector::numeric(10,3)[];
       numeric       
---------------------
 {1.000,2.000,3.000}
(1 row)
```

##### Vector 转 Text Array

- 示例：

```
openGauss=# SELECT '[1,2,3]'::vector::text[];
    text       
---------
 {1,2,3}
(1 row)
```

##### Vector 转 Varchar Array

- 示例：

```
openGauss=# SELECT '[1.21,2.32,3]'::vector::varchar(3)[];
    varchar       
--------------
 {1.2,2.3,3}
(1 row)
```

>[!NOTE]说明
vector转换字符数组仅支持text和varchar，其余字符类型暂未支持。

## Bit

### Bit 操作符

操作符 | 描述
--- | --- 
<~> | 汉明距离
<%> | 杰卡德距离

- <~>

    描述：汉明距离。

    返回类型：uint64

    示例：

    ```
    openGauss=# SELECT '111' <~> '110';
     ?column? 
    ----------
            1
    (1 row)
    ```

- <%>

    描述：杰卡德距离。

    返回类型：double

    示例：

    ```
    openGauss=# SELECT '1111' <%>  '1000';
     ?column? 
    ----------
          .75
    (1 row)
    ```    

#### Bit 函数

函数 | 描述
--- | --- 
hamming_distance(bit, bit) → double precision | 汉明距离
jaccard_distance(bit, bit) → double precision | 杰卡德距离

- hamming_distance

    描述：汉明距离。

    返回类型：uint64

    示例：

    ```
    openGauss=# SELECT hamming_distance('111', '110');
     hamming_distance 
    ------------------
                    1
    (1 row)
    ```   

- jaccard_distance

    描述：杰卡德距离。

    返回类型：double

    示例：

    ```
    openGauss=# SELECT jaccard_distance('1111', '1110');
     jaccard_distance 
    ------------------
                  .25
    (1 row)
    ```       

## Sparsevec

### Sparsevec 操作符

操作符 | 描述
--- | --- 
<-> | 欧几里得距离 (L2)
<#> | 负内积
<=> | 余弦距离
<+> | 曼哈顿距离 (L1)
\= | 等于
\<> | 不等于

- <->

    描述：欧几里得距离 (L2)。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT '{}/2'::sparsevec <-> '{1:3,2:4}/2';
     ?column? 
    ----------
            5
    (1 row)
    ```  

- <#>

    描述：负内积。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT '{1:1,2:2}/2'::sparsevec <#> '{1:3,2:4}/2';
     ?column? 
    ----------
          -11
    (1 row)
    ``` 

- <=>

    描述：余弦距离。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT '{1:1,2:2}/2'::sparsevec <=> '{1:2,2:4}/2';
     ?column? 
    ----------
            0
    (1 row)
    ``` 

- <+>

    描述：曼哈顿距离 (L1)。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT '{}/2'::sparsevec <+> '{1:3,2:4}/2';
     ?column? 
    ----------
            7
    (1 row)
    ```     

- \=

    描述：等于。

    示例：

    ```
    openGauss=# SELECT '{1:1,2:2,3:3}/3'::sparsevec = '{1:1,2:2,3:3}/3';
     ?column? 
    ----------
     t
    (1 row)
    ``` 

- \<>

    描述：不等于。

    示例：

    ```
    openGauss=# SELECT '{1:1,2:2,3:3}/3'::sparsevec <> '{1:1,2:2,3:3}/3';
     ?column? 
    ----------
     f
    (1 row)
    ```  

### Sparsevec 函数

函数 | 描述
--- | --- 
cosine_distance(sparsevec, sparsevec) → double precision | 余弦距离
inner_product(sparsevec, sparsevec) → double precision | 内积
l1_distance(sparsevec, sparsevec) → double precision | 曼哈顿距离 (L1)
l2_distance(sparsevec, sparsevec) → double precision | 欧几里得距离 (L2)
l2_norm(sparsevec) → double precision | 欧几里得范数
l2_normalize(sparsevec) → sparsevec | 归一化（使用L2距离）

- cosine_distance

    描述：余弦距离。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT cosine_distance('{1:1,2:2}/2'::sparsevec, '{1:2,2:4}/2');
     cosine_distance 
    -----------------
                   0
    (1 row)
    ```  

- inner_product

    描述：内积。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT inner_product('{1:1,2:2}/2'::sparsevec, '{1:2,2:4}/2');
     inner_product 
    ---------------
                10
    (1 row)
    ``` 

- l1_distance

    描述：曼哈顿距离 (L1)。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT l1_distance('{}/2'::sparsevec, '{1:3,2:4}/2');
     l1_distance 
    -------------
               7
    (1 row)
    ``` 

- l2_distance

    描述：欧几里得距离 (L2)。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT l2_distance('{}/2'::sparsevec, '{1:3,2:4}/2');
     l2_distance 
    -------------
               5
    (1 row)
    ```     

- l2_norm

    描述：欧几里得范数。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT l2_norm('{1:3,2:4}/2'::sparsevec);
     l2_norm 
    ---------
           5
    (1 row)
    ``` 

- l2_normalize

    描述：归一化（使用L2距离）。

    返回类型：sparsevec

    示例：

    ```
    openGauss=# SELECT l2_normalize('{1:3,2:4}/2'::sparsevec);
      l2_normalize   
    -----------------
     {1:0.6,2:0.8}/2
    (1 row)
    ``` 

### Sparsevec 类型转换

- 格式

```
SELECT ITEM::sparsevec

SELECT sparsevec(ITEM);

SELECT cast(ITEM AS sparsevec);
```

>[!NOTE]说明
>sparsevec()仅支持text或者同为sparsevec类型转换

#### TEXT/VARCHAR 转 sparsevec

- 示例：

```
openGauss=# SELECT '{1:1.5,3:3.5}/5'::sparsevec;
    sparsevec    
-----------------
 {1:1.5,3:3.5}/5
(1 row)
```

#### Vector 转 sparsevec

- 示例：

```
openGauss=# SELECT '[0,1.5,0,3.5,0]'::vector::sparsevec;
    sparsevec    
-----------------
 {2:1.5,4:3.5}/5
(1 row)
```

#### Sparsevec 转 Vector

- 示例：

```
openGauss=# SELECT '{2:1.5,4:3.5}/5'::sparsevec::vector(5);
     vector      
-----------------
 [0,1.5,0,3.5,0]
(1 row)
```

## Halfvec

### Halfvec 操作符

操作符 | 描述 
--- | --- 
\+ | 元素级加法
\- | 元素级减法
\* | 元素级乘法
\= | 等于
\<> | 不等于
\|\| | 向量拼接
<-> | 欧几里得距离 (L2)
<#> | 负内积
<=> | 余弦距离
<+> | 曼哈顿距离 (L1)

- \+

    描述：元素级加法。

    示例：

    ```
    openGauss=> select '[1,2,3]'::halfvec + '[4,5,6]';
     ?column? 
    ----------
     [5,7,9]
    (1 row)
    ```

- \-

    描述：元素级减法。

    示例：

    ```
    openGauss=> select '[1,2,3]'::halfvec - '[4,5,6]';
      ?column?  
    ------------
     [-3,-3,-3]
    (1 row)
    ```    

- \*

    描述：元素级乘法。

    示例：

    ```
    openGauss=> select '[1,2,3]'::halfvec * '[4,5,6]';
     ?column?  
    -----------
     [4,10,18]
    (1 row)
    ```    

- \=

    描述：等于。

    示例：

    ```
    openGauss=# select '[1,2,3]'::halfvec = '[4,5,6]';
     ?column? 
    ----------
     f
    (1 row)
    ``` 

- \<>

    描述：不等于。

    示例：

    ```
    openGauss=# select '[1,2,3]'::halfvec <> '[4,5,6]';
     ?column? 
    ----------
     t
    (1 row)
    ``` 

- \|\|

    描述：向量拼接。

    示例：

    ```
    openGauss=> select '[1,2,3]'::halfvec || '[4,5,6]';
       ?column?    
    ---------------
     [1,2,3,4,5,6]
    (1 row)
    ```         

- <->

    描述：欧几里得距离 (L2)。

    示例：

    ```
    openGauss=# SELECT '[0,0]'::halfvec <-> '[3,4]';
     ?column? 
    ----------
            5
    (1 row)
    ```      

- <#> 

    描述：负内积。

    示例：

    ```
    openGauss=# SELECT '[1,2]'::halfvec <#> '[3,4]';
     ?column? 
    ----------
          -11
    (1 row)
    ```        

- <=>

    描述：余弦距离。

    示例：

    ```
    openGauss=# SELECT '[1,2]'::halfvec <=> '[2,4]';
     ?column? 
    ----------
            0
    (1 row)
    ```      

- <+>

    描述：曼哈顿距离。

    示例：

    ```
    openGauss=# SELECT '[0,0]'::halfvec <+> '[3,4]';
     ?column? 
    ----------
            7
    (1 row)
    ```      

### Halfvec 函数

函数 | 描述
--- | --- 
binary_quantize(halfvec) → bit | 二进制量化
cosine_distance(halfvec, halfvec) → double precision | 余弦距离
inner_product(halfvec, halfvec) → double precision | 内积
l1_distance(halfvec, halfvec) → double precision | 曼哈顿距离 (L1)
l2_distance(halfvec, halfvec) → double precision | 欧几里得距离 (L2)
l2_normalize(halfvec) → halfvec | 归一化（使用L2距离）
subvector(halfvec, integer, integer) → halfvec | 截取子向量
vector_dims(halfvec) → integer | 向量的维度数

- binary_quantize

    描述：二进制量化，若元素大于0，则量化为1；若元素小于或等于0，则量化为0。

    返回类型：bit

    示例：

    ```
    openGauss=# SELECT binary_quantize('[1,0,-1]'::halfvec);
     binary_quantize 
    -----------------
     100
    (1 row)
    ```    

- cosine_distance

    描述：余弦距离。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT cosine_distance('[1,2]'::halfvec, '[2,4]');
     cosine_distance 
    -----------------
                   0
    (1 row)
    ```        

- inner_product

    描述：内积。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT inner_product('[1,2]'::halfvec, '[3,4]');
     inner_product 
    ---------------
                11
    (1 row)
    ```           

- l1_distance

    描述：曼哈顿距离 (L1)。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT l1_distance('[0,0]'::halfvec, '[3,4]');
     l1_distance 
    -------------
               7
    (1 row)
    ```      

- l2_distance

    描述：欧几里得距离 (L2)。

    返回类型：float8

    示例：

    ```
    openGauss=# SELECT l2_distance('[0,0]'::halfvec, '[3,4]');
     l2_distance 
    -------------
               5
    (1 row)
    ```       

- l2_normalize

    描述：归一化（使用L2距离）。

    返回类型：halfvec

    示例：

    ```
    openGauss=# SELECT l2_normalize('[3,4]'::halfvec);
     l2_normalize 
    --------------
     [0.6,0.8]
    (1 row)
    ```        

- subvector

    描述：截取子向量。

    返回类型：halfvec

    示例：

    ```
    openGauss=# SELECT subvector('[1,2,3,4,5]'::halfvec, 1, 3);
     subvector 
    -----------
     [1,2,3]
    (1 row)
    ```         

- vector_dims

    描述：向量的维度数。

    返回类型：int

    示例：

    ```
    openGauss=# SELECT vector_dims('[1,2,3]'::halfvec);
     vector_dims 
    -------------
               3
    (1 row)
    ```                

### Halfvec 聚合函数

函数 | 描述
--- | --- 
avg(halfvec) → halfvec | 平均值 
sum(halfvec) → halfvec | 求和

- avg

    描述：平均值。

    返回类型：halfvec

    示例：

    ```
    openGauss=# SELECT avg(v) FROM unnest(ARRAY['[1,2,3]'::halfvec, '[3,5,7]']) v;
        avg    
    -----------
     [2,3.5,5]
    (1 row)
    ```     

- sum

    描述：求和。

    返回类型：halfvec

    示例：

    ```
    openGauss=# SELECT sum(v) FROM unnest(ARRAY['[1,2,3]'::halfvec, '[3,5,7]', NULL]) v;
       sum    
    ----------
     [4,7,10]
    (1 row)
    ```       

### Halfvec 类型转换

- 格式：

```
SELECT ITEM::halfvec;

SELECT halfvec(ITEM);

SELECT cast(ITEM AS halfvec);
```

#### TEXT/VARCHAR 转 Halfvec

- 示例：

```
openGauss=# SELECT '[1,2,3,4,5]'::halfvec;
   halfvec    
-------------
 [1,2,3,4,5]
(1 row)
```

#### Vector 转 Halfvec

- 示例：

```
openGauss=# SELECT '[1,2,3,4,5]'::vector::halfvec;
   halfvec    
-------------
 [1,2,3,4,5]
(1 row)
```

#### Halfvec 转 Vector

- 示例：

```
openGauss=# SELECT '[1,2,3,4,5]'::halfvec::vector(5);
    vector      
-------------
 [1,2,3,4,5]
(1 row)
```

>[!NOTE]说明  
>向量数据类型会和其他类型使用同样的函数名、操作符，为确保执行向量操作无误，建议对至少一个入参进行显式类型转换，如 `SELECT l2_distance('[0,0]'::vector, '[3,4]');`。
