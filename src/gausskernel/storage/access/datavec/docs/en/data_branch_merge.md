# Branch Merging

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T12:07:36.825Z pushedAt=2026-07-30T12:23:58.596Z -->

## 1. Feature Overview

Branch merging is used to compare and merge data differences between two data branches. It primarily includes two types of capabilities:

- `branch diff`: Compares data differences between the source branch and the target branch.
- `branch merge`: Merges data changes from the source branch into the target branch.

Branch merging is implemented based on the openGauss endpoint connection and `postgres_fdw`. The target endpoint temporarily maps the tables to be compared from the source endpoint, and completes table structure validation, primary key matching, row-level difference identification, and merging within the target endpoint through Neon extension functions.

## Applicable Scenarios

Branch merging is suitable for the following scenarios:

- Compare data differences between two data branches.
- Merge data changes from a development or test branch into the target branch.
- Copy new ordinary tables from the source branch to the target branch.
- Insert new rows from the source branch into the target branch.
- Handle conflicting rows with the same primary key but different non-primary-key columns according to specified strategies.

## Overall Implementation Approach

Branch merging is implemented through "control plane orchestration + in-database SQL execution" approach.

1. `neon_local` parses command-line parameters including the source branch, target branch, endpoint, database, schema, user, and conflict resolution strategy.
2. `neon_local` resolves the source and target endpoints based on branch information, and connects to the target endpoint.
3. The target endpoint creates a temporary foreign table schema pointing to the source endpoint via `postgres_fdw`.
4. The tables to be compared in the source branch are mapped as foreign tables that can be queried by the target endpoint.
5. The Neon extension provides functions such as `neon_branch_diff` `neon_branch_merge`, and `neon_branch_cleanup_source`.
6. The `diff/merge` operations perform table structure validation, primary key matching, row-level difference identification, and data merging within the target endpoint.

## Branch Difference Comparison

### `diff` Command

Use `neon_local branch diff` to compare data differences between the source branch and the target branch:

```bash
neon_local branch diff \
  --source-branch <source_branch> \
  --target-branch <target_branch>
```

Commonly used parameters are as follows:

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

By default, both the source schema and the target schema are `public`.

Parameter description:

| Name | Description |
| --- | --- |
| `--source-branch` | Name of the source branch. Used as the comparison source during `diff`. |
| `--target-branch` | Name of the target branch. Used as the comparison target during `diff`. |
| `--source-endpoint` | Name of the source branch endpoint. Must be explicitly specified when multiple running endpoints exist for the same source branch. |
| `--target-endpoint` | Name of the target branch endpoint. Must be explicitly specified when multiple running endpoints exist for the same target branch. |
| `--source-schema` | Schema in the source branch participating in comparison. The default value is `public`. |
| `--target-schema` | Schema in the target branch participating in comparison. The default value is `public`. |
| `--database` | Name of the database used to connect to the source endpoint and target endpoint. |
| `--user` | Database user used to connect to the source endpoint and target endpoint. |
| `--fdw-schema` | Name of the temporary FDW schema created in the target endpoint, used to mount foreign tables from the source branch. |
| `--fdw-server` | Name of the temporary FDW server created in the target endpoint, used to connect to the source endpoint. |
| `--keep-fdw` | Retain the temporary FDW schema, server, and foreign tables after the command completes, for debugging purposes. By default, they are automatically cleaned up. |

### `diff` Output

`branch diff` outputs table-level and row-level differences between the source branch and the target branch. The output includes:

| Field | Description |
| --- | --- |
| `schema_name` | The schema where the difference resides |
| `table_name` | The table where the difference resides |
| `diff_type` | The type of difference |
| `row_data` | The content of the differing row, represented in JSON text |

`diff_type` includes the following types:

| Difference Type | Description |
| --- | --- |
| `source_only` | A table or row that exists on the source side but not on the target side |
| `target_only` | A table or row that exists on the target side but not on the source side |
| `conflict` | Both sides have the same primary key, but non-primary key columns differ |
| `schema_mismatch` | The column signatures of the table differ between the two sides |
| `no_primary_key` | The target table has no primary key, making row-level `diff` unsafe to perform |

## Branch Merging

### `merge` Command

Use `neon_local branch merge` to merge source branch data into the target branch:

```bash
neon_local branch merge \
  --source-branch <source_branch> \
  --target-branch <target_branch>
```

Specify the conflict resolution strategy:

```bash
neon_local branch merge \
  --source-branch <source_branch> \
  --target-branch <target_branch> \
  --strategy fail
```

`branch merge` supports the same branch, endpoint, schema, database, and user parameters as `branch diff`.

Commonly used parameters are as follows:

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

Parameter description:

| Name | Description |
| --- | --- |
| `--source-branch` | Name of the source branch. Data changes from this branch are merged into the target branch during  the merge. |
| `--target-branch` | Name of the target branch. The merge result is written to this branch. |
| `--source-endpoint` | Name of the source branch endpoint. Must be explicitly specified when multiple running endpoints exist for the same source branch. |
| `--target-endpoint` | Name of the target branch endpoint. Must be explicitly specified when multiple running endpoints exist for the same target branch. |
| `--source-schema` | Schema in the source branch that participates in the merge. The default value is `public`. |
| `--target-schema` | Schema in the target branch that receives the merge result. The default value is `public`. |
| `--database` | Database name used to connect to the source endpoint and target endpoint. |
| `--user` | Database user used to connect to the source endpoint and target endpoint. |
| `--fdw-schema` | Name of the temporary FDW schema created in the target endpoint, used to mount foreign tables from the source branch. |
| `--fdw-server` | Name of the temporary FDW server created in the target endpoint, used to connect to the source endpoint. |
| `--keep-fdw` | Retain the temporary FDW schema, server, and foreign tables after the command completes, for debugging purposes. By default, they are automatically cleaned up. |
| `--strategy` | Conflict resolution strategy. Valid values are `fail`, `ours`, or `theirs`. The default value is `fail`. |
| `--no-copy-source-only-tables` | Do not copy source-only tables; only process common tables that exist in both the source branch and the target branch. |

### `merge` Type

`branch merge` involves two types:

| Type | Description |
| --- | --- |
| Source-only table copy | For regular tables that exist in the source branch but not in the target branch, the table structure and data can be copied to the target branch. |
| Common table merging | For tables that exist in both the source and target branches, row-level merging is performed based on primary key matching. |

For common table merging, the target table must have a primary key. The primary key can be a single-column primary key or a composite primary key.

### `merge` Output

`branch merge` outputs the merging statistics for each table:

| Name | Description |
| --- | --- |
| `schema_name` | Schema where the merged table resides |
| `table_name` | Name of the merged table |
| `inserted_count` | Number of rows inserted into the target branch |
| `updated_count` | Number of rows updated in the target branch |

## Conflict Resolution Strategies

`branch merge` specifies the conflict resolution strategy via `--strategy`. A conflict occurs when the source branch and target branch have rows with the same primary key but different non-primary key columns.

| Strategy | Description |
| --- | --- |
| `fail` | Default strategy. The merging fails when a conflict is detected, and target branch data is not automatically overwritten. |
| `ours` | Retain the conflict rows in the target branch and inserts only the new rows from the source side. |
| `theirs` | Overwrite the conflict rows in the target branch with the non-primary key columns from the source branch, and inserts the new rows from the source side. |

Example:

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

## Constraints

The following constraints must be met when using `branch merge`:

1. The source branch and target branch must be under the same Tenant.
2. Both the source branch and target branch must have a connectable running endpoint.
3. If multiple running endpoints exist for the same branch, they must be explicitly specified via `--source-endpoint` or `--target-endpoint`.
4. The source and target must connect using the same database name and database user.
5. The `pg_database.datcompatibility` of the source and target must be consistent.
6. `branch diff/merge` currently operates at the schema granularity, with the default schema being `public`.
7. The target endpoint must be able to execute `CREATE EXTENSION neon`.
8. The column signatures of common tables on both sides must be consistent.
9. The column signature includes the column name, type OID, `typmod`, and `NOT NULL`.
10. Common table merging requires that the target table has a primary key.
11. `branch merge` does not delete rows unique to the target side.
12. `branch merge` does not delete tables unique to the target side.
13. Source-only table copying currently supports only some common table objects.

Source-only table copying supports:

- Ordinary row-store table
- Column definition
- `NOT NULL` constraint
- `DEFAULT` value
- `PRIMARY KEY`
- `UNIQUE` constraint
- `CHECK` constraint
- Ordinary index

Source-only table copying does not yet support:

- Foreign keys
- Sequences or auto-increment default values
- Views
- Materialized views
- Foreign tables
- Functions or storage procedures
- Table comments or column comments
- Explicit ACL or owner differences
- Triggers
- Non-default tablespace
- Non-default storage parameters
- Partitioned tables

## Example: View Differences and Merge

Assume that the `public.items` table exists in both the source branch `dev` and the target branch `main`, with the following table structure:

```sql
CREATE TABLE items (
    id int primary key,
    name text,
    price int
);
```

Data in `dev`:

| id | name | price |
| --- | --- | --- |
| 1 | apple | 10 |
| 2 | banana | 20 |
| 3 | cherry | 30 |

Data in `main`:

| id | name | price |
| --- | --- | --- |
| 1 | apple | 10 |
| 2 | banana | 25 |
| 4 | date | 40 |

Where:

- `id = 1`: The source branch and target branch are consistent.
- `id = 2`: The source branch and target branch have the same primary key, but `price` differs, making it a conflict row.
- `id = 3`: Exists only in the source branch, making it a source-side row.
- `id = 4`: exists only in the target branch, making it a target-side row.

### View Branch Difference

Execute `diff`:

```bash
neon_local branch diff \
  --source-branch dev \
  --target-branch main \
  --source-schema public \
  --target-schema public
```

Example output:

```text
schema_name | table_name | diff_type   | row_data
------------+------------+-------------+----------------------------------------
public      | items      | conflict    | {"id":2,"source":{"name":"banana","price":20},"target":{"name":"banana","price":25}}
public      | items      | source_only | {"id":3,"name":"cherry","price":30}
public      | items      | target_only | {"id":4,"name":"date","price":40}
```

`diff` only displays difference data and does not modify the source branch or the target branch.

### `fail` for Merging

Execute `merge`:

```bash
neon_local branch merge \
  --source-branch dev \
  --target-branch main \
  --strategy fail
```

`id = 2` introduces a conflict, causing merging failure and the target branch data to remain unchanged.

The target branch `main` becomes:

| id | name | price |
| --- | --- | --- |
| 1 | apple | 10 |
| 2 | banana | 25 |
| 4 | date | 40 |

### `ours` for Merging

Execute `merge`:

```bash
neon_local branch merge \
  --source-branch dev \
  --target-branch main \
  --strategy ours
```

Example output:

```text
schema_name | table_name | inserted_count | updated_count
------------+------------+----------------+--------------
public      | items      | 1              | 0
```

The `ours` strategy retains conflict rows in the target branch and inserts only new rows from the source branch. After merging, the target branch `main` becomes:

| id | name | price |
| --- | --- | --- |
| 1 | apple | 10 |
| 2 | banana | 25 |
| 3 | cherry | 30 |
| 4 | date | 40 |

### `theirs` for Merging

Execute `theirs`:

```bash
neon_local branch merge \
  --source-branch dev \
  --target-branch main \
  --strategy theirs
```

Example output:

```text
schema_name | table_name | inserted_count | updated_count
------------+------------+----------------+--------------
public      | items      | 1              | 1
```

The `theirs` strategy inserts new rows from the source branch and overwrites conflict rows in the target branch with source branch data. After merging, the target branch `main` becomes:

| id | name | price |
| --- | --- | --- |
| 1 | apple | 10 |
| 2 | banana | 20 |
| 3 | cherry | 30 |
| 4 | date | 40 |
