# Apache AGE (incubating) for openGauss 适配详情

## 说明

Apache AGE通过创建插件的形式使用。

```
create extension age;
```

AGE安装之后，会默认创建ag_catalog的schema，AGE内置的数据类型、函数均存储在ag_catalog下。因此在使用AGE的时候，特别是执行cypher语句时，需要先执行命令：

```
SET search_path TO ag_catalog;
```

同时需要执行 load 'age' 命令确保age插件的全部hook被加载，保证图数据的完整性。

```
load 'age';
```

>[!NOTE]说明
>
>创建使用age插件前需要关闭线程池，设置enable_thread_pool = off
>
## 1. 图的操作

图由一组顶点和边组成，其中每个单独的节点和边都具有属性映射。顶点是图的基本对象，它可以独立于图中的其他任何东西而存在。边在两个顶点之间创建有向连接。

### 1.1 创建图

创建图使用函数：create_graph(graph_name);

```
SELECT * FROM ag_catalog.create_graph('graph_name');
```

### 1.2 删除图

删除图使用函数：drop_graph(graph_name, cascade);
>
> 1. 第一个参数graph_name为要删除的图，第二个参数cascade为布尔值，表示是否删除依赖于图表的标签和数据
> 2. 建议将 cascade 选项设置为 true，否则必须使用 SQL DDL 命令手动删除图中的所有内容
>
```
SELECT * FROM ag_catalog.drop_graph('graph_name', true);
```

### 1.3 创建图中的点类型

创建图中的点类型使用函数：create_vlabel(graph_name, label_name);

```
SELECT * FROM ag_catalog.create_vlabel('graph_name','label_name');
```

每当使用create_vlabel()函数创建顶点标签时，会在new_graph的命名空间new_graph中生成一个新表"\<label_name>"。用于创建边类型的create_elabel()函数也是如此。这两个函数不是必须使用的，用Cypher语句创建顶点和边时，如果类型表不存在，会自动创建。

### 1.4 创建图中的边类型

创建图使用函数：create_elabel(graph_name, label_name);

```
SELECT * FROM ag_catalog.create_elabel('graph_name','label_name');
```

## 2. 图的存储

### 2.1 图

图创建之后，内核会创建一个与图名称相同的schema。同时在ag_catalog.ag_graph的表中会插入一条数据，标记新创建的schema用来存储图数据。

```
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

### 图中的点和边

```
-- 创建图之后，会在该图对应的schema中创建_ag_label_vertex和_ag_label_edge两张表作为默认的点表和边表，同时会在ag_catalog.ag_label中插入两条数据将点表和边表与schema关联起来。
SELECT * FROM ag_catalog.ag_label;

       name       | graph | id | kind |          relation          
------------------+-------+----+------+----------------------------
 _ag_label_vertex | 68484 |  1 | v    | new_graph._ag_label_vertex 
 _ag_label_edge   | 68484 |  2 | e    | new_graph._ag_label_edge   
(2 rows)

-- 创建点表
SELECT create_vlabel('new_graph', 'Person');
NOTICE:  VLabel "Person" has been created
 create_vlabel 
---------------
 
(1 row)

-- 创建点表之后，会在ag_catalog.ag_label表中插入一条数据将点表与图关联起来。kind为v代表点表kind为e代表边表
SELECT * FROM ag_catalog.ag_label;
       name       | graph | id | kind |          relation          
------------------+-------+----+------+----------------------------
 _ag_label_vertex | 68484 |  1 | v    | new_graph._ag_label_vertex 
 _ag_label_edge   | 68484 |  2 | e    | new_graph._ag_label_edge   
 Person           | 68484 |  3 | v    | new_graph."Person"         
(3 rows)
```

## 3. Cypher查询

Cypher语句无法在数据库中直接执行。需要通过cypher()函数构造，将Cypher语句作为cypher()函数的参数执行，cypher()函数返回一个SETOF records。

### 3.1 cypher()函数介绍

cypher(graph_name, query_string, parameters)
> 第一个参数graph_name为要查询的图，第二个参数query_string为Cypher语句，第三个参数parameters为可选参数只能与Prepared Statements一起使用。否则将抛出错误。

```
SELECT * FROM cypher('graph_name', $$ 
/* Cypher Query Here */ 
$$) AS (result1 agtype, result2 agtype);
```

**注意事项**
>
> 1. AS 后的 (result1 agtype, result2 agtype) 即SETOF records 需要和Cypher语句中的返回值个数相同
> 2. SELECT * FROM cypher 不能写成 SELECT cypher
> 3. 执行语句前需要执行 load 'age' 和 set search_path = ag_catalog

## 数据类型及函数说明

以下对AGE中包含的数据类型、Cypher语法、函数进行简要说明。

### 1 数据类型

AGE的会创建两种数据类型: graphid 和 agtype。其中graphid代表点和边的唯一id标识符。agtype为AGE的核心数据类型，具体介绍可参考 <https://age.apache.org/age-manual/master/intro/types.html>

#### 1.1 Simple DataTypes

简单数据类型包括：Null、Integer、Float、Numeric、Bool、String。

#### 1.2 Composite DataTypes

复合数据类型包括：List、Map。

- List支持以下操作

| 序号 | 功能支持 | 说明 |
|:--------|:---------|--------:|
| 1   |List in general | 普通list |
| 2   |NULL in a List | 带null的list |
| 3   |Access Individual Elements | 获取list的某个元素 |
| 4   |MapElements in Lists | list元素包含map结构 |
| 5   |Accessing Map Elements in Lists | 获取list元素中map的值 |
| 6   |Negative Index Access | 负值索引 |
| 7   |Index Ranges  | 区间索引 |
| 8   |Negative Index Ranges | 负值区间区间索引 |
| 9   |Positive Slices | 正切片操作 |
| 10  |Negative Slices | 负切片操作 |

- Map支持以下操作

| 序号 | 功能支持 | 说明 |
|:--------|:---------|--------:|
|1 | Literal Maps with SimpleDataTypes | 普通map类型 |
|2 | Literal Maps with composite Data Types | map中包含复合数据类型 |
|3 | Property Access of a map | map的属性值获取操作 |
|4 | Accessing List Elements in Maps | map中获取list元素操作 |

#### 1.3 Simple Entities

简单实体类型包括 GraphId、Labels、Properties。又简单实体类型组成Vertex、Edge、Composite Entities。

#### 1.4 Vertex

Vertex 是图的基本组成元素，代表节点。

#### 1.5 Edge

Edge 是图的基本组成元素，代表边。

#### 1.6 Composite Entities

由Vertex 和 Edge组成的Path类型。

### 2 Cypher语法

AGE对Cypher语法的支持具体介绍可参考：<https://age.apache.org/age-manual/master/clauses/match.html>

#### 2.1 Match

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 | get all vertices | 获取全部节点|
|2 | get all vertices with a label | 获取某label类型的全部节点|
|3 | related vertices | 通过边获取相关节点（邻居节点）|
|4 | match with labels | 通过label过滤相关节点|
|5 | Outgoing Edges |有向边支持|
|6 | Directed Edges and variable | 有向边变量支持|
|7 | Match on edge type |通过label type过滤边|
|8 | Match on edge type and use a variable | 通过label type过滤边同时设置变量|
|9 | Multiple Edges | 多条边匹配|
|10 | Variable Length Edges | 变长路径匹配|

#### 2.2 With

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 |Filter on aggregate function results |通过聚合函数过滤结果|
|2 |Sort results before using collect on them |collect前排序操作|
|3 |Limit branching of a path search |匹配路径、限制为一定数量，然后使用这些路径作为基础再次匹配|

#### 2.3 SKIP

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 |skip first three rows |跳过最开始的行|
|2 |Return middle tow rows |配合limit返回中间的行|
|3 |Using an expression with SKIP to return a subset of the rows |使用SKIP带有表达式返回行的子集|

#### 2.4 LIMIT

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 |Return a subset of the rows |返回查询结果的子集|
|2 |Using an expression with LIMIT to return a subset of the rows |limit带有表达式返回行的子集|

#### 2.5 Return

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 | Return nodes |返回查询的节点|
|2 | Return edges |返回查询的边|
|3 | Return property |返回节点或者边的属性|
|4 | Return all elements |返回全部元素|
|5 | Variable with uncommon characters |支持不通用的字符变量|
|6 | Aliasing a field |返回值起别名|
|7 | unique results |distinct 返回值|

#### 2.6 ORDER BY

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 | Order nodes by proerty |通过单个属性排序|
|2 | Order nodes by multiple properties |通过多个属性排序|
|3 | Order nodes in descending order |降序排序|
|4 | Ordering null |null的排序规则升序在最后|

#### 2.7 CREATE

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
| 1 | Create single vertex |创建单个节点|
| 2 | Create multiple vertices |创建多个节点|
| 3 | Create a vertex with a label |创建带标签的节点|
| 4 | Create vertex and add labels and properties |创建带标签和加属性的节点|
| 5 | Return create node |创建并返回节点|
| 6 | Create an edge between two nodes |创建边|
| 7 | Create an edge and set properties |创建边并赋属性值|
| 8 | Create a full path |创建一条路经|

#### 2.8 SET

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 |Set a property |修改单个属性|
|2 |Return created vertex |返回修改后的节点|
|3 |Remove a property |移除属性|
|4 |Set multiple properties using one SET clause |修改多个属性|

#### 2.9 REMOVE

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 |Remove a property |移除属性|

#### 2.10 DELETE

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 |Delete single vertex |删除单个节点|
|2 |Delete all vertices and edges |删除全部节点和边|
|3 |Delete edges only |删除边|
|4 |Return a deleted vertex |返回删除的节点|

### 3 函数支持

函数主要是对agtype的操作和表达式的生成，具体内容可参考：<https://age.apache.org/age-manual/master/functions/predicate_functions.html>

#### 3.1 Predicate Functions

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 |Exists(Property) |检测属性是否存在|
|2 |Exists(Path) | 检测查询路径是否存在|

#### 3.2 Scalar Functions

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 |id |返回顶点或边的 id|
|2 |start_id |返回边的起始顶点的 id|
|3 |end_id |返回边的终止顶点的 id|
|4 |type |返回边类型的字符串表示形式|
|5 |properties |返回包含顶点或边的所有属性的 agtype map。如果参数已经是map，则原封不动地返回|
|6 |head |返回 agtype 列表中的第一个元素|
|7 |last |返回 agtype 列表中的最后一个元素|
|8 |length |返回路径的长度|
|9 |size |返回列表的长度|
|10 |startNode |返回边的起始节点|
|11 |endNode |返回边的终点节点|
|12 |timestamp |返回当前时间与 UTC 1970 年 1 月 1 日午夜之间的差值（以毫秒为单位）|
|13 |toBoolean |将字符串值转换为布尔值|
|14 |toFloat |将整数或字符串值转换为浮点数|
|15 |toInteger |将浮点数或字符串值转换为整数值|
|15 |coalesce |返回给定表达式列表中的第一个非空值|

#### 3.3 List Functions

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 |keys |返回一个列表，其中包含顶点、边或map的所有属性名称的字符串表示形式|
|2 |range |返回一个列表，该列表包含起始值start和结束值end所限定范围内的所有整数值|
|3 |labels |返回一个包含节点所有标签的字符串表示形式的列表|
|4 |relationships |返回包含路径中所有关系的列表|
|5 |nodes |返回包含路径中所有顶点的列表|

#### 3.4 Numeric Functions

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 |rand |返回一个随机浮点数，范围从 0（含）到 1（不含）；即 [0,1]|
|2 |abs |返回给定数字的绝对值|
|3 |ceil |返回大于或等于给定数字且等于数学整数的最小浮点数|
|4 |floor |返回小于或等于给定数字且等于数学整数的最大浮点数|
|5 |round |返回四舍五入为最接近的整数的给定数字的值|
|6 |sign |返回给定数字的符号|

#### 3.5 Logarithmic Functions

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 |e |返回自然对数的底数，e|
|2 |sqrt |返回数字的平方根|
|3 |exp |返回 e^n，其中 e 是自然对数的底数，n 是参数表达式的值|
|4 |log |返回数字的自然对数|
|5 |log10 |返回一个数字的常用对数（底数为 10）|

#### 3.6 Trigonometric Functions

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 |degrees |将弧度转换为度|
|2 |radians |将度数转换为弧度|
|3 |pi |返回数学常数 pi|
|4 |sin |返回数字的正弦|
|5 |cos |返回数字的余弦|
|6 |tan |返回数字的正切|
|7 |cot |返回数字的余切|
|8 |asin |返回数字的反正弦|
|9 |acos |返回数字的反余弦|
|10| atan |返回数字的反正切|
|11| atan2 |以弧度返回一组坐标的反正切值|

#### 3.7 String Functions

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 |replace |返回一个字符串，其中原始字符串中出现的所有指定字符串都已被另一个（指定的）字符串替换
|2 |split |返回根据给定分隔符的匹配项对原始字符串进行拆分后得到的字符串列表|
|3 |left |返回包含原始字符串最左边指定数量字符的字符串。|
|4 |right |返回包含原始字符串最右边指定数量字符的字符串。|
|5 |substring |返回原始字符串的子字符串，以从 0 开始的索引开始和长度。|
|6 |rTrim |返回删除尾随空格后的原始字符串|
|7 |lTrim |返回删除前导空格后的原始字符串。|
|8 |trim |返回删除前导和尾随空格的原始字符串|
|9 |toLower |以小写形式返回原始字符串|
|10 |toUpper |以大写形式返回原始字符串|
|11 |reverse |返回原始字符串中所有字符的顺序均已反转的字符串。|

#### 3.8 Aggregation Functions

| 序号 | 功能支持 | 说明 |
|:--------|:---------|:--------|
|1 |min |返回一组值中的最小值|
|2 |max |返回一组值中的最大值|
|3 |stDev |返回给定值在一组中的标准偏差|
|4 |stDevP |返回给定值在一组中的标准偏差|
|5 |percentileCont |返回给定值在组中的百分位数|
|6 |percentileDisc |返回给定值在组中的百分位数|
|7 |count |返回值或记录的数量|
|8 |avg |返回一组数值的平均值|
|9 |sum |返回一组数值的总和|
