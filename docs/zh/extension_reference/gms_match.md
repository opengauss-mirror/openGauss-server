# gms_match

## gms_match概述

gms_match是一个基于openGauss的插件，用于比较两个字符串之间的相似度。目前支持的接口有：

- gms_match.edit_distance
- gms_match.edit_distance_similarity

## gms_match限制

- 仅支持create extension命令方式加载插件。
- gms_match插件不支持set schema，即执行`alter extension gms_match set schema new_name;`将提示报错。

## gms_match安装

openGauss打包编译时默认已经包含了gms_match, 可以在安装完openGauss后，直接通过`create extension gms_match;`加载插件。

## gms_match使用

### 创建Extension<a name="section21088306113"></a>

创建gms_match extension可直接使用create extension命令进行创建：

```
create extension gms_match;
```

### 使用Extension<a name="section107391050141118"></a>

#### gms_match.edit_distance

- gms_match.edit_distance(s1 in varchar2, s2 in varchar2) returns integer

  **描述**：此函数返回两个字符串之间的编辑距离，即计算从s1到s2所需要的最少变化数，一次变化指一次插入、删除或替换操作。

  **参数说明**：

  - `s1`: 第一个varchar2类型入参，源字符串
  - `s2`: 第一个varchar2类型入参，目标字符串

  **返回值**：integer数据类型，两个字符串之间的编辑距离

  **示例**：

  ```
  select gms_match.edit_distance(NULL, 'ff');
  edit_distance 
  ---------------
              -1
  (1 row)

  select gms_match.edit_distance('', '');
  edit_distance 
  ---------------
              -1
  (1 row)

  select gms_match.edit_distance('', 'ab');
  edit_distance 
  ---------------
              -1
  (1 row)

  select gms_match.edit_distance('ab', 'ab');
  edit_distance 
  ---------------
              0
  (1 row)

  select gms_match.edit_distance('00', 'ff');
  edit_distance 
  ---------------
              2
  (1 row)

  select gms_match.edit_distance('ssttten', 'sitting');
  edit_distance 
  ---------------
              4
  (1 row)
  ```

#### gms_match.edit_distance_similarity

- gms_match.edit_distance_similarity(s1 in varchar2, s2 in varchar2) returns integer

  **描述**：此函数返回两个字符串的相似度，即编辑距离的标准化值（0~100），数值越大代表相似度越高。标准化值的计算公式：(1 - 编辑距离 / 两个入参长度的最大值) * 100。

  **参数说明**：

  - `s1`: 第一个varchar2类型入参，源字符串
  - `s2`: 第一个varchar2类型入参，目标字符串

  **返回值**：integer数据类型，两个字符串之间的相似度

  **示例**：

  ```
  select gms_match.edit_distance_similarity(NULL, 'ff');
  edit_distance_similarity 
  --------------------------
                          0
  (1 row)

  select gms_match.edit_distance_similarity('', '');
  edit_distance_similarity 
  --------------------------
                        100
  (1 row)

  select gms_match.edit_distance_similarity('', 'ab');
  edit_distance_similarity 
  --------------------------
                          0
  (1 row)

  select gms_match.edit_distance_similarity('ab', 'ab');
  edit_distance_similarity 
  --------------------------
                        100
  (1 row)

  select gms_match.edit_distance_similarity('00', 'ff');
  edit_distance_similarity 
  --------------------------
                          0
  (1 row)

  select gms_match.edit_distance_similarity('ssttten', 'sitting');
  edit_distance_similarity 
  --------------------------
                        43
  (1 row)
  ```

### 删除Extension<a name="section1587441381220"></a>

在openGauss中删除gms_match extension的方法如下所示：

```
drop extension gms_match [cascade];
```

>[!NOTE]说明
>
>如果extension被其它对象依赖，需要加入cascade（级联）关键字，删除所有依赖对象。
