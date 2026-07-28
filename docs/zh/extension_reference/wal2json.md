# wal2json

## wal2json概述

wal2json是一个基于openGauss的逻辑解码输出插件，用于将WAL（Write-Ahead Log）中的数据变更转换为JSON格式输出。该插件支持捕获INSERT、UPDATE、DELETE和TRUNCATE操作，适用于实时数据变更捕获（CDC）场景，方便将数据库变更集成到下游系统。

wal2json支持两种输出格式版本：

- **格式版本1**（format-version 1）：每个事务输出一个JSON对象，事务内所有变更合并在一个`change`数组中。
- **格式版本2**（format-version 2）：每条变更记录输出一个独立的JSON对象，可选输出BEGIN/COMMIT事务标记。

## wal2json限制

- 使用wal2json前，需要在`postgresql.conf`中设置`wal_level = logical`，并适当配置`max_replication_slots`和`max_wal_senders`参数，修改后需重启数据库。
- wal2json通过逻辑复制槽工作，不支持通过`create extension`方式加载，而是作为逻辑复制输出插件在创建复制槽时指定。
- UPDATE和DELETE操作的旧值输出取决于表的`REPLICA IDENTITY`设置：
  - `DEFAULT`（主键）：仅输出主键列的旧值。
  - `FULL`：输出所有列的旧值。
  - `INDEX`：仅输出索引列的旧值。
  - `NOTHING`：不输出旧值，UPDATE/DELETE可能不会被记录。
- 无主键且未设置`REPLICA IDENTITY FULL`的表，在格式版本2中UPDATE和DELETE操作将产生警告且不输出变更数据。

## wal2json安装

openGauss编译时包含wal2json插件的编译，可在编译安装openGauss后直接使用。使用前需确保`postgresql.conf`中配置如下参数：

```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

修改参数后需重启数据库生效。

## wal2json使用

### 创建逻辑复制槽<a name="section_create_slot"></a>

使用`pg_create_logical_replication_slot`函数创建逻辑复制槽，并指定wal2json作为输出插件：

```sql
SELECT 'init' FROM pg_create_logical_replication_slot('test_slot', 'wal2json');
 ?column?
----------
 init
(1 row)
```

### 查看变更数据<a name="section_get_changes"></a>

wal2json支持两种方式查看变更数据：

- `pg_logical_slot_peek_changes()`：查看变更数据但不消费（可重复查看）。
- `pg_logical_slot_get_changes()`：查看变更数据并消费（查看后数据将被移除）。

两个函数的参数格式相同，以键值对方式传入插件参数：

```sql
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, '参数名1', '参数值1', '参数名2', '参数值2', ...);
```

### 插件参数说明<a name="section_parameters"></a>

#### format-version

- **描述**：指定输出格式版本。
- **可选值**：`1`（默认）、`2`
- **说明**：版本1按事务分组输出，版本2按单条变更输出。

**示例**：

```sql
-- 使用格式版本1（默认）
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'format-version', '1');

-- 使用格式版本2
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'format-version', '2');
```

#### include-xids

- **描述**：是否在输出中包含事务ID（xid）。
- **可选值**：`0`（默认）、`1`
- **说明**：启用后，版本1在顶层添加`xid`字段；版本2在每条变更中添加`xid`字段。

#### include-timestamp

- **描述**：是否在输出中包含事务提交的时间戳。
- **可选值**：`0`（默认）、`1`
- **说明**：启用后，输出中将包含`timestamp`字段。

#### include-schemas

- **描述**：是否在输出中包含schema名称。
- **可选值**：`0`、`1`（默认）
- **说明**：启用后，表名将带有schema前缀。

#### include-types

- **描述**：是否在输出中包含列的数据类型信息。
- **可选值**：`0`、`1`（默认）
- **说明**：启用后，版本1输出`columntypes`数组；版本2在每列中输出`type`字段。

#### include-typmod

- **描述**：是否在输出中包含类型修饰符（如varchar的长度）。
- **可选值**：`0`、`1`（默认）
- **说明**：启用后，类型显示为`varchar(30)`而非`varchar`，`numeric(5,3)`而非`numeric`。

#### include-type-oids

- **描述**：是否在输出中包含类型OID。
- **可选值**：`0`（默认）、`1`

#### include-not-null

- **描述**：是否在输出中包含NOT NULL约束信息。
- **可选值**：`0`（默认）、`1`
- **说明**：启用后，版本1输出`columnoptionals`数组；版本2在每列中输出`optional`字段。

#### include-default

- **描述**：是否在输出中包含列的默认值表达式。
- **可选值**：`0`（默认）、`1`
- **说明**：启用后，版本1输出`columndefaults`数组；版本2在每列中输出`default`字段。

**示例**：

```sql
CREATE TABLE w2j_default (a serial, b integer DEFAULT 6, c text DEFAULT 'wal2json', PRIMARY KEY(a));

SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'format-version', '2', 'include-default', '1');
{"action":"B"}
{"action":"I","schema":"public","table":"w2j_default","columns":[{"name":"a","type":"integer","value":1,"default":"nextval('w2j_default_a_seq'::regclass)"},{"name":"b","type":"integer","value":6,"default":"6"},{"name":"c","type":"text","value":"wal2json","default":"'wal2json'::text"}]}
{"action":"C"}
```

#### include-pk

- **描述**：是否在输出中包含主键信息（列名和类型）。
- **可选值**：`0`（默认）、`1`
- **说明**：启用后，版本1在变更中添加`pk`对象；版本2添加`pk`数组。

#### include-lsn

- **描述**：是否在输出中包含日志序列号（LSN）。
- **可选值**：`0`（默认）、`1`
- **说明**：启用后，版本1在顶层添加`nextlsn`字段；版本2在每条变更中添加`lsn`字段。

#### include-column-positions

- **描述**：是否在输出中包含列的位置编号（pg_attribute.attnum）。
- **可选值**：`0`（默认）、`1`

#### include-origin

- **描述**：是否在输出中包含复制来源信息。
- **可选值**：`0`（默认）、`1`

#### include-transaction

- **描述**：是否在格式版本2中输出BEGIN/COMMIT事务标记。
- **可选值**：`0`、`1`（默认）
- **说明**：仅对格式版本2有效。

#### pretty-print

- **描述**：是否对JSON输出进行格式化缩进。
- **可选值**：`0`（默认）、`1`
- **说明**：启用后，JSON输出将带有缩进和换行，便于阅读。

#### write-in-chunks

- **描述**：是否在每条变更后立即写入输出，而非按事务批量输出。
- **可选值**：`0`（默认）、`1`

#### actions

- **描述**：指定需要捕获的操作类型。
- **可选值**：`insert`、`update`、`delete`、`truncate`的逗号分隔组合
- **默认值**：版本1为`insert, update, delete`；版本2为`insert, update, delete, truncate`

**示例**：

```sql
-- 仅捕获INSERT操作
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'format-version', '2', 'actions', 'insert');

-- 捕获UPDATE和TRUNCATE操作
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'format-version', '2', 'actions', 'update, truncate');
```

#### filter-tables

- **描述**：排除指定表的变更记录。
- **格式**：`schema.table`，多个表用逗号分隔，区分大小写。
- **通配符**：`*.table`（任意schema下的指定表）、`schema.*`（指定schema下的所有表）
- **转义规则**：表名或schema名中的空格、单引号、逗号、句号、星号需用反斜杠转义。

**示例**：

```sql
-- 排除public.filter_table_1和filter_schema_2下的所有表
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL,
  'format-version', '1',
  'filter-tables', '*.filter_table_1, filter_schema_2.*');
```

#### add-tables

- **描述**：仅包含指定表的变更记录（白名单模式）。
- **格式**：与`filter-tables`相同。
- **默认值**：所有schema的所有表。

#### filter-origins

- **描述**：排除来自指定复制来源的变更记录。
- **格式**：逗号分隔的OID值。

#### filter-msg-prefixes

- **描述**：排除指定前缀的自定义消息。
- **格式**：逗号分隔的前缀字符串。

#### add-msg-prefixes

- **描述**：仅包含指定前缀的自定义消息（白名单模式）。
- **格式**：逗号分隔的前缀字符串。

#### include-domain-data-type

- **描述**：是否将域类型替换为其底层基础数据类型输出。
- **可选值**：`0`（默认）、`1`
- **说明**：默认输出域类型名称；启用后输出底层基础数据类型。

### 输出格式说明<a name="section_output_format"></a>

#### 格式版本1输出结构

每个事务输出一个JSON对象，所有变更合并在`change`数组中：

```json
{
  "xid": 123,
  "timestamp": "2020-03-01 08:09:00",
  "nextlsn": "0/ABC123",
  "change": [
    {
      "kind": "insert",
      "schema": "public",
      "table": "table_name",
      "columnnames": ["col1", "col2"],
      "columntypes": ["integer", "text"],
      "columnvalues": [1, "value"],
      "oldkeys": {
        "keynames": ["id"],
        "keytypes": ["integer"],
        "keyvalues": [1]
      }
    }
  ]
}
```

- `kind`：操作类型，值为`insert`、`update`或`delete`。
- `columnnames`：列名数组。
- `columntypes`：列类型数组。
- `columnvalues`：列值数组。
- `oldkeys`：仅在UPDATE和DELETE操作中出现（需要表有主键或REPLICA IDENTITY设置），包含旧值信息。

#### 格式版本2输出结构

每条变更输出一个独立的JSON对象：

```json
{"action":"B"}                           // 事务开始
{"action":"I","schema":"public","table":"t1","columns":[...]}  // INSERT
{"action":"U","schema":"public","table":"t1","columns":[...],"identity":[...]}  // UPDATE
{"action":"D","schema":"public","table":"t1","identity":[...]}  // DELETE
{"action":"T","schema":"public","table":"t1"}  // TRUNCATE
{"action":"C"}                           // 事务提交
```

- `action`：操作类型标识。`B`=Begin、`C`=Commit、`I`=Insert、`U`=Update、`D`=Delete、`T`=Truncate、`M`=Message。
- `columns`：列信息数组（用于INSERT和UPDATE），每个元素包含`name`、`type`、`value`。
- `identity`：标识列信息数组（用于UPDATE和DELETE），包含可唯一标识行的列值。

### 使用示例<a name="section_examples"></a>

#### 示例1：基本INSERT操作

```sql
-- 准备测试表
CREATE TABLE test_table (a integer PRIMARY KEY, b text);

-- 创建逻辑复制槽
SELECT 'init' FROM pg_create_logical_replication_slot('test_slot', 'wal2json');
 ?column?
----------
 init
(1 row)

-- 执行INSERT操作
INSERT INTO test_table VALUES (1, 'hello');
INSERT INTO test_table VALUES (2, 'world');

-- 查看格式版本1输出
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'format-version', '1', 'pretty-print', '1');
                          data
--------------------------------------------------------
 {                                                     +
         "change": [                                   +
                 {                                     +
                         "kind": "insert",             +
                         "schema": "public",           +
                         "table": "test_table",        +
                         "columnnames": ["a", "b"],    +
                         "columntypes": ["integer", "text"],+
                         "columnvalues": [1, "hello"]  +
                 }                                     +
         ]                                             +
 }

-- 查看格式版本2输出
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'format-version', '2');
                                                              data
---------------------------------------------------------------------------------------------------------------------------------
 {"action":"B"}
 {"action":"I","schema":"public","table":"test_table","columns":[{"name":"a","type":"integer","value":1},{"name":"b","type":"text","value":"hello"}]}
 {"action":"C"}
```

#### 示例2：UPDATE和DELETE操作

```sql
-- 有主键的表
CREATE TABLE table_with_pk (
  id serial PRIMARY KEY,
  name text
);

SELECT 'init' FROM pg_create_logical_replication_slot('test_slot', 'wal2json');

INSERT INTO table_with_pk (name) VALUES ('Alice');
UPDATE table_with_pk SET name = 'Bob' WHERE id = 1;
DELETE FROM table_with_pk WHERE id = 1;

-- 格式版本2输出
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'format-version', '2');
 {"action":"B"}
 {"action":"I","schema":"public","table":"table_with_pk","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"Alice"}]}
 {"action":"C"}
 {"action":"B"}
 {"action":"U","schema":"public","table":"table_with_pk","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"Bob"}],"identity":[{"name":"id","type":"integer","value":1}]}
 {"action":"C"}
 {"action":"B"}
 {"action":"D","schema":"public","table":"table_with_pk","identity":[{"name":"id","type":"integer","value":1}]}
 {"action":"C"}
```

#### 示例3：使用多种参数选项

```sql
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL,
  'format-version', '2',
  'include-xids', '1',
  'include-timestamp', '1',
  'include-lsn', '1',
  'include-pk', '1',
  'pretty-print', '1'
);
```

#### 示例4：表过滤

```sql
-- 排除指定表
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL,
  'format-version', '2',
  'filter-tables', 'public.log_table, public.temp_table'
);

-- 仅包含指定表
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL,
  'format-version', '2',
  'add-tables', 'public.important_table'
);
```

#### 示例5：操作类型过滤

```sql
-- 仅捕获INSERT操作
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL,
  'format-version', '2',
  'actions', 'insert'
);

-- 捕获INSERT和DELETE操作
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL,
  'format-version', '2',
  'actions', 'insert, delete'
);
```

### 数据类型处理说明<a name="section_data_types"></a>

wal2json对不同数据类型有特定的输出处理：

- **数值类型**（INT2、INT4、INT8、FLOAT4、FLOAT8、NUMERIC）：作为JSON原始数字输出，NaN和Infinity转换为JSON null。
- **布尔类型**（BOOL）：输出为`true`或`false`。
- **字节类型**（BYTEA）：以十六进制字符串形式输出（不含`\x`前缀）。
- **域类型**：默认输出域类型名称；启用`include-domain-data-type`后输出底层基础数据类型。
- **其他类型**：作为JSON转义字符串输出。

### 删除逻辑复制槽<a name="section_drop_slot"></a>

使用`pg_drop_replication_slot`函数删除逻辑复制槽：

```sql
SELECT 'stop' FROM pg_drop_replication_slot('test_slot');
 ?column?
----------
 stop
(1 row)
```

>[!NOTE]说明
>
>删除逻辑复制槽后，该槽内尚未消费的变更数据将被丢弃。在删除前请确保所有变更已被正确消费或不再需要。
