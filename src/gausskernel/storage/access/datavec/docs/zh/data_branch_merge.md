# 分支合并

## 一、功能概述

分支合并用于比较和合入两个数据分支之间的数据差异，主要包含两类能力：

- `branch diff`：比较源分支和目标分支的数据差异。
- `branch merge`：将源分支中的数据变更合入目标分支。

分支合并基于已有 openGauss endpoint 连接能力和 `postgres_fdw` 实现。目标 endpoint 会临时映射源 endpoint 中待比较的表，并通过 `neon` 扩展函数在目标 endpoint 内完成表结构校验、主键匹配、行级差异识别和合并。

## 二、适用场景

分支合并适用于以下场景：

- 比较两个数据分支之间的数据差异。
- 将开发分支或测试分支中的数据变更合入目标分支。
- 将源分支新增的普通表复制到目标分支。
- 将源分支新增行插入目标分支。
- 对同主键但非主键列不同的冲突行，按指定策略处理。

## 三、整体实现思路

分支合并采用“控制面编排 + 数据库内 SQL 执行”的方式实现。

1. `neon_local` 解析源分支、目标分支、endpoint、database、schema、用户和冲突策略等命令行参数。
2. `neon_local` 根据分支信息解析源 endpoint 和目标 endpoint，并连接目标 endpoint。
3. 目标 endpoint 通过 `postgres_fdw` 建立到源 endpoint 的临时外表 schema。
4. 源分支中待比较的表会被映射为目标 endpoint 可查询的 foreign table。
5. `neon` 扩展提供 `neon_branch_diff`、`neon_branch_merge`、`neon_branch_cleanup_source` 等函数。
6. diff/merge 在目标 endpoint 内完成表结构校验、主键匹配、行级差异识别和数据合并。

## 四、分支差异比较

### 1. diff 命令

使用 `neon_local branch diff` 比较源分支和目标分支之间的数据差异。

```bash
neon_local branch diff \
  --source-branch <source_branch> \
  --target-branch <target_branch>
```

常用可选参数如下：

```bash
neon_local branch diff \
  --source-branch <source_branch> \
  --target-branch <target_branch> \
  --source-endpoint <source_endpoint> \
  --target-endpoint <target_endpoint> \
  --source-schema <source_schema> \
  --target-schema <target_schema> \
  --database <database> \
  --user <user> \
  --fdw-schema <fdw_schema> \
  --fdw-server <fdw_server> \
  --keep-fdw
```

默认情况下，源 schema 和目标 schema 均为 `public`。

参数说明：

| 参数 | 说明 |
| --- | --- |
| `--source-branch` | 源分支名称。diff 时作为对比来源。 |
| `--target-branch` | 目标分支名称。diff 时作为对比目标。 |
| `--source-endpoint` | 源分支 endpoint 名称。同一源分支存在多个 running endpoint 时需要显式指定。 |
| `--target-endpoint` | 目标分支 endpoint 名称。同一目标分支存在多个 running endpoint 时需要显式指定。 |
| `--source-schema` | 源分支中参与比较的 schema，默认值为 `public`。 |
| `--target-schema` | 目标分支中参与比较的 schema，默认值为 `public`。 |
| `--database` | 连接源 endpoint 和目标 endpoint 使用的数据库名。 |
| `--user` | 连接源 endpoint 和目标 endpoint 使用的数据库用户。 |
| `--fdw-schema` | 在目标 endpoint 中创建的临时 FDW schema 名称，用于挂载源分支 foreign table。 |
| `--fdw-server` | 在目标 endpoint 中创建的临时 FDW server 名称，用于连接源 endpoint。 |
| `--keep-fdw` | 命令结束后保留临时 FDW schema、server 和 foreign table，便于调试。默认会自动清理。 |

### 2. diff 输出

`branch diff` 输出源分支和目标分支之间的表级、行级差异。输出内容包含：

| 字段 | 说明 |
| --- | --- |
| `schema_name` | 差异所在 schema |
| `table_name` | 差异所在表 |
| `diff_type` | 差异类型 |
| `row_data` | 差异行内容，使用 JSON 文本表示 |

`diff_type` 包含以下类型：

| 差异类型 | 说明 |
| --- | --- |
| `source_only` | 源侧存在，目标侧不存在的表或行 |
| `target_only` | 目标侧存在，源侧不存在的表或行 |
| `conflict` | 双方主键相同，但非主键列存在差异 |
| `schema_mismatch` | 双方表字段签名不一致 |
| `no_primary_key` | 目标表无主键，无法安全执行行级 diff |

## 五、分支合并

### 1. merge 命令

使用 `neon_local branch merge` 将源分支数据合入目标分支。

```bash
neon_local branch merge \
  --source-branch <source_branch> \
  --target-branch <target_branch>
```

指定冲突处理策略：

```bash
neon_local branch merge \
  --source-branch <source_branch> \
  --target-branch <target_branch> \
  --strategy fail
```

`branch merge` 支持与 `branch diff` 相同的分支、endpoint、schema、database 和 user 参数。

常用可选参数如下：

```bash
neon_local branch merge \
  --source-branch <source_branch> \
  --target-branch <target_branch> \
  --source-endpoint <source_endpoint> \
  --target-endpoint <target_endpoint> \
  --source-schema <source_schema> \
  --target-schema <target_schema> \
  --database <database> \
  --user <user> \
  --fdw-schema <fdw_schema> \
  --fdw-server <fdw_server> \
  --keep-fdw \
  --strategy fail|ours|theirs \
  --no-copy-source-only-tables
```

参数说明：

| 参数 | 说明 |
| --- | --- |
| `--source-branch` | 源分支名称。merge 时将该分支的数据变更合入目标分支。 |
| `--target-branch` | 目标分支名称。merge 的写入结果落在该分支上。 |
| `--source-endpoint` | 源分支 endpoint 名称。同一源分支存在多个 running endpoint 时需要显式指定。 |
| `--target-endpoint` | 目标分支 endpoint 名称。同一目标分支存在多个 running endpoint 时需要显式指定。 |
| `--source-schema` | 源分支中参与合并的 schema，默认值为 `public`。 |
| `--target-schema` | 目标分支中接收合并结果的 schema，默认值为 `public`。 |
| `--database` | 连接源 endpoint 和目标 endpoint 使用的数据库名。 |
| `--user` | 连接源 endpoint 和目标 endpoint 使用的数据库用户。 |
| `--fdw-schema` | 在目标 endpoint 中创建的临时 FDW schema 名称，用于挂载源分支 foreign table。 |
| `--fdw-server` | 在目标 endpoint 中创建的临时 FDW server 名称，用于连接源 endpoint。 |
| `--keep-fdw` | 命令结束后保留临时 FDW schema、server 和 foreign table，便于调试。默认会自动清理。 |
| `--strategy` | 冲突处理策略，取值为 `fail`、`ours` 或 `theirs`。默认值为 `fail`。 |
| `--no-copy-source-only-tables` | 不复制源分支独有表，只处理源分支和目标分支均存在的共有表。 |

### 2. merge 内容

`branch merge` 分为两类处理：

| 类型 | 说明 |
| --- | --- |
| 源独有表复制 | 源分支存在、目标分支不存在的普通表，可复制表结构和数据到目标分支 |
| 共有表合并 | 源分支和目标分支均存在的表，按主键匹配并执行行级合并 |

共有表合并时，目标表必须存在主键。主键可以是单列主键，也可以是复合主键。

### 3. merge 输出

`branch merge` 输出每张表的合并统计信息：

| 字段 | 说明 |
| --- | --- |
| `schema_name` | 合并表所在 schema |
| `table_name` | 合并表名 |
| `inserted_count` | 插入到目标分支的行数 |
| `updated_count` | 更新到目标分支的行数 |

## 六、冲突处理策略

`branch merge` 通过 `--strategy` 指定冲突处理策略。冲突指源分支和目标分支存在相同主键，但非主键列值不同。

| 策略 | 说明 |
| --- | --- |
| `fail` | 默认策略。发现冲突时合并失败，不自动覆盖目标分支数据 |
| `ours` | 保留目标分支中的冲突行，仅插入源侧新增行 |
| `theirs` | 使用源分支中的非主键列覆盖目标分支冲突行，并插入源侧新增行 |

示例：

```bash
neon_local branch merge \
  --source-branch dev \
  --target-branch main \
  --strategy fail
```

```bash
neon_local branch merge \
  --source-branch dev \
  --target-branch main \
  --strategy ours
```

```bash
neon_local branch merge \
  --source-branch dev \
  --target-branch main \
  --strategy theirs
```

## 七、约束与限制

使用分支合并时，需要满足以下约束：

1. 源分支和目标分支必须在同一 Tenant 下。
2. 源分支和目标分支均需存在可连接的 running endpoint。
3. 如果同一分支存在多个 running endpoint，需要通过 `--source-endpoint` 或 `--target-endpoint` 显式指定。
4. 源端和目标端使用同一 database 名和数据库用户连接。
5. 源端和目标端的 `pg_database.datcompatibility` 必须一致。
6. `branch diff/merge` 当前以 schema 为粒度执行，默认 schema 为 `public`。
7. 目标 endpoint 需要能够执行 `CREATE EXTENSION neon`。
8. 双方共有表的字段签名必须一致。
9. 字段签名包含字段名、类型 OID、typmod、`NOT NULL` 属性。
10. 共有表合并要求目标表存在主键。
11. `branch merge` 不删除目标侧独有行。
12. `branch merge` 不删除目标侧独有表。
13. 源独有表复制当前仅支持部分常见表对象。

源独有表复制支持：

- 普通行存表。
- 字段定义。
- `NOT NULL` 约束。
- `DEFAULT` 默认值。
- `PRIMARY KEY`。
- `UNIQUE` 约束。
- `CHECK` 约束。
- 普通索引。

源独有表复制暂不支持：

- 外键。
- sequence 或自增默认值。
- 视图。
- 物化视图。
- foreign table。
- 函数或存储过程。
- 表注释或列注释。
- 显式 ACL 或 owner 差异。
- 触发器。
- 非默认 tablespace。
- 非默认存储参数。
- 分区表。


## 八、示例：查看差异并合并

假设源分支 `dev` 和目标分支 `main` 中都存在 `public.items` 表，表结构如下：

```sql
CREATE TABLE items (
    id int primary key,
    name text,
    price int
);
```

源分支 `dev` 中的数据：

| id | name | price |
| --- | --- | --- |
| 1 | apple | 10 |
| 2 | banana | 20 |
| 3 | cherry | 30 |

目标分支 `main` 中的数据：

| id | name | price |
| --- | --- | --- |
| 1 | apple | 10 |
| 2 | banana | 25 |
| 4 | date | 40 |

其中：

- `id = 1`：源分支和目标分支一致。
- `id = 2`：源分支和目标分支主键相同，但 `price` 不同，属于冲突行。
- `id = 3`：只存在于源分支，属于源侧新增行。
- `id = 4`：只存在于目标分支，属于目标侧独有行。

### 1. 查看分支差异

执行 diff：

```bash
neon_local branch diff \
  --source-branch dev \
  --target-branch main \
  --source-schema public \
  --target-schema public
```

示例输出：

```text
schema_name | table_name | diff_type   | row_data
------------+------------+-------------+----------------------------------------
public      | items      | conflict    | {"id":2,"source":{"name":"banana","price":20},"target":{"name":"banana","price":25}}
public      | items      | source_only | {"id":3,"name":"cherry","price":30}
public      | items      | target_only | {"id":4,"name":"date","price":40}
```

diff 只展示差异数据，不修改源分支或目标分支。

### 2. 使用 `fail` 策略合并

执行 merge：

```bash
neon_local branch merge \
  --source-branch dev \
  --target-branch main \
  --strategy fail
```

由于 `id = 2` 存在冲突，合并失败，目标分支数据不变。

目标分支 `main` 仍为：

| id | name | price |
| --- | --- | --- |
| 1 | apple | 10 |
| 2 | banana | 25 |
| 4 | date | 40 |

### 3. 使用 `ours` 策略合并

执行 merge：

```bash
neon_local branch merge \
  --source-branch dev \
  --target-branch main \
  --strategy ours
```

示例输出：

```text
schema_name | table_name | inserted_count | updated_count
------------+------------+----------------+--------------
public      | items      | 1              | 0
```

`ours` 策略保留目标分支中的冲突行，只插入源分支新增行。合并后目标分支 `main` 为：

| id | name | price |
| --- | --- | --- |
| 1 | apple | 10 |
| 2 | banana | 25 |
| 3 | cherry | 30 |
| 4 | date | 40 |

### 4. 使用 `theirs` 策略合并

如果使用 `theirs` 策略：

```bash
neon_local branch merge \
  --source-branch dev \
  --target-branch main \
  --strategy theirs
```

示例输出：

```text
schema_name | table_name | inserted_count | updated_count
------------+------------+----------------+--------------
public      | items      | 1              | 1
```

`theirs` 策略会插入源分支新增行，并使用源分支数据覆盖目标分支中的冲突行。合并后目标分支 `main` 为：

| id | name | price |
| --- | --- | --- |
| 1 | apple | 10 |
| 2 | banana | 20 |
| 3 | cherry | 30 |
| 4 | date | 40 |
