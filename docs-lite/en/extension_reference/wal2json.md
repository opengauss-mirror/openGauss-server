# wal2json

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T03:37:19.749Z pushedAt=2026-09-29T02:34:56.717Z -->

## wal2json Overview

wal2json is a logical decoding output plugin based on openGauss. It converts data changes in the WAL (Write-Ahead Log) into JSON format for output. The plugin supports capturing INSERT, UPDATE, DELETE, and TRUNCATE operations. It applies to real-time change data capture (CDC) use cases, facilitating the integration of database changes into downstream systems.

wal2json supports two output format versions:

- **Format version 1** (format-version 1): Each transaction outputs one JSON object. All changes in the transaction are merged into a `change` array.
- **Format version 2** (format-version 2): Each change record outputs an independent JSON object. BEGIN/COMMIT transaction markers are output optionally.

## wal2json Constraints

- Before using wal2json, set `wal_level = logical` in `postgresql.conf`, and properly set `max_replication_slots` and `max_wal_senders`. Restart the database after the modification.
- wal2json works through logical replication slots. It cannot be loaded using `create extension`. Instead, it is specified as a logical replication output plugin when creating a replication slot.
- The output of old values for UPDATE and DELETE operations depends on the `REPLICA IDENTITY` setting of the table:
  - `DEFAULT` (primary key): outputs only the old values of primary key columns.
  - `FULL`: outputs the old values of all columns.
  - `INDEX`: outputs only the old values of index columns.
  - `NOTHING`: outputs no old values. UPDATE/DELETE operations may not be recorded.
- For a table without a primary key and without `REPLICA IDENTITY FULL` set, UPDATE and DELETE operations in format version 2 generate warnings and output no changed data.

## wal2json Installation

wal2json is compiled during openGauss compilation. The plugin can be used directly after openGauss is compiled and installed. Before use, ensure that the following parameters are configured in `postgresql.conf`:

```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

Restart the database for the parameter changes to take effect.

## wal2json in Use

### Creating a Logical Replication Slot<a name="section_create_slot"></a>

Use the `pg_create_logical_replication_slot` function to create a logical replication slot and specify wal2json as the output plugin:

```sql
SELECT 'init' FROM pg_create_logical_replication_slot('test_slot', 'wal2json');
 ?column?
----------
 init
(1 row)
```

### Viewing Changed Data<a name="section_get_changes"></a>

wal2json supports two ways to view changed data:

- `pg_logical_slot_peek_changes()`: Views changed data without consuming it (data can be viewed repeatedly).
- `pg_logical_slot_get_changes()`: Views changed data and consumes it (data is removed after viewing).

Both functions use the same parameter format. Plugin parameters are passed as key-value pairs:

```sql
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'parameter_name1', 'parameter_value1', 'parameter_name2', 'parameter_value2', ...);
```

### Plugin Parameters<a name="section_parameters"></a>

#### format-version

- **Description**: Specifies the output format version.
- **Value options**: `1` (default), `2`
- **Note**: Version 1 groups output by transaction. Version 2 outputs each change individually.

**Example**:

```sql
-- Use format version 1 (default).
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'format-version', '1');

-- Use format version 2.
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'format-version', '2');
```

#### include-xids

- **Description**: Whether to include the transaction ID (xid) in the output.
- **Value options**: `0` (default), `1`
- **Note**: When enabled, format version 1 adds the `xid` field at the top level; format version 2 adds the `xid` field to each change.

#### include-timestamp

- **Description**: Whether the output contains the transaction commit timestamp.
- **Value options**: `0` (default), `1`
- **Note**: When enabled, the output contains the `timestamp` field.

#### include-schemas

- **Description**: Whether to include schema names in the output.
- **Value options**: `0`, `1` (default)
- **Note**: When enabled, table names are prefixed with the schema name.

#### include-types

- **Description**: Whether to include column data type information in the output.
- **Value options**: `0`, `1` (default)
- **Note**: When enabled, format version 1 outputs the `columntypes` array; format version 2 outputs the `type` field in each column.

#### include-typmod

- **Description**: Whether to include type modifiers (such as the length of varchar) in the output.
- **Value options**: `0`, `1` (default)
- **Note**: When enabled, types are displayed as `varchar(30)` instead of `varchar`, and as `numeric(5,3)` instead of `numeric`.

#### include-type-oids

- **Description**: Whether to include type OIDs in the output.
- **Value options**: `0` (default), `1`

#### include-not-null

- **Description**: Whether to include NOT NULL constraint information in the output.
- **Value options**: `0` (default), `1`
- **Note**: When enabled, format version 1 outputs the `columnoptionals` array; format version 2 outputs the `optional` field for each column.

#### include-default

- **Description**: Whether to include the default value expression of a column in the output.
- **Value options**: `0` (default), `1`
- **Note**: When enabled, format version 1 outputs the `columndefaults` array, and format version 2 outputs the `default` field in each column.

**Example**:

```sql
CREATE TABLE w2j_default (a serial, b integer DEFAULT 6, c text DEFAULT 'wal2json', PRIMARY KEY(a));

SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'format-version', '2', 'include-default', '1');
{"action":"B"}
{"action":"I","schema":"public","table":"w2j_default","columns":[{"name":"a","type":"integer","value":1,"default":"nextval('w2j_default_a_seq'::regclass)"},{"name":"b","type":"integer","value":6,"default":"6"},{"name":"c","type":"text","value":"wal2json","default":"'wal2json'::text"}]}
{"action":"C"}
```

#### include-pk

- **Description**: Whether to include primary key information (column names and types) in the output.
- **Value options**: `0` (default), `1`
- **Note**: If enabled, format version 1 adds the `pk` object to changes; format version 2 adds the `pk` array.

#### include-lsn

- **Description**: Whether to include the log sequence number (LSN) in the output.
- **Value options**: `0` (default), `1`
- **Note**: When enabled, format version 1 adds the `nextlsn` field at the top level; format version 2 adds the `lsn` field to each change.

#### include-column-positions

- **Description**: Whether to include the column position number (pg_attribute.attnum) in the output.
- **Value options**: `0` (default), `1`

#### include-origin

- **Description**: Whether to include replication origin information in the output.
- **Value options**: `0` (default), `1`

#### include-transaction

- **Description**: Whether to output BEGIN/COMMIT transaction markers in format version 2.
- **Value options**: `0`, `1` (default)
- **Note**: Valid only for format version 2.

#### pretty-print

- **Description**: Whether to format the JSON output with indentation.
- **Value options**: `0` (default), `1`
- **NOTE**: When enabled, the JSON output contains indentation and line breaks for easy reading.

#### write-in-chunks

- **Description**: Whether to write the output immediately after each change instead of in batches per transaction.
- **Value options**: `0` (default), `1`

#### actions

- **Description**: Specifies the operation type to capture.
- **Value options**: comma-separated combination of `insert`, `update`, `delete`, and `truncate`
- **Default value**: `insert, update, delete` for format version 1; `insert, update, delete, truncate` for format version 2

**Example**:

```sql
-- Capture only INSERT operations.
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'format-version', '2', 'actions', 'insert');

-- Capture UPDATE and TRUNCATE operations.
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'format-version', '2', 'actions', 'update, truncate');
```

#### filter-tables

- **Description**: Excludes change records of specified tables.
- **Format**: `schema.table`. Multiple tables are comma-separated and case-sensitive.
- **Wildcards**: `*.table` (a specified table in any schema), `schema.*` (all tables in a specified schema)
- **Escape rules**: Spaces, single quotes, commas, periods, and asterisks in table names or schema names must be escaped with backslashes.

**Example**:

```sql
-- Exclude public.filter_table_1 and all tables under filter_schema_2.
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL,
  'format-version', '1',
  'filter-tables', '*.filter_table_1, filter_schema_2.*');
```

#### add-tables

- **Description**: Includes only change records of the specified tables (trustlist mode).
- **Format**: Same as `filter-tables`.
- **Default value**: All tables in all schemas.

#### filter-origins

- **Description**: Excludes change records from the specified replication origins.
- **Format**: Comma-separated OIDs.

#### filter-msg-prefixes

- **Description**: Excludes custom messages with the specified prefix.
- **Format**: Comma-separated prefix string.

#### add-msg-prefixes

- **Description**: Includes only custom messages with the specified prefixes (trustlist mode).
- **Format**: Comma-separated prefix string.

#### include-domain-data-type

- **Description**: Whether to replace domain types with their underlying base data types in the output.
- **Value options**: `0` (default), `1`
- **Note**: Outputs the domain type name by default; outputs the underlying base data type when enabled.

### Output Formats<a name="section_output_format"></a>

#### Format Version 1 Output Structure

Each transaction outputs one JSON object, and all changes are merged into the `change` array:

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

- `kind`: Operation type. The value is `insert`, `update`, or `delete`.
- `columnnames`: Array of column names.
- `columntypes`: Array of column types.
- `columnvalues`: Array of column values.
- `oldkeys`: Appears only in UPDATE and DELETE operations (the table must have a primary key or REPLICA IDENTITY configured). Contains information about old values.

#### Format Version 2 Output Structure

Each change is output as a separate JSON object:

```json
{"action":"B"}                           // Transaction starts.
{"action":"I","schema":"public","table":"t1","columns":[...]}  // INSERT
{"action":"U","schema":"public","table":"t1","columns":[...],"identity":[...]}  // UPDATE
{"action":"D","schema":"public","table":"t1","identity":[...]}  // DELETE
{"action":"T","schema":"public","table":"t1"}  // TRUNCATE
{"action":"C"}                           // Transaction committed.
```

- `action`: operation type identifier. `B`=Begin, `C`=Commit, `I`=Insert, `U`=Update, `D`=Delete, `T`=Truncate, `M`=Message.
- `columns`: array of column information (for INSERT and UPDATE). Each element contains `name`, `type`, and `value`.
- `identity`: array of identity column information (for UPDATE and DELETE). It contains the column values that uniquely identify a row.

### Usage Examples<a name="section_examples"></a>

#### Example 1: Basic INSERT Operation

```sql
-- Prepare the test table.
CREATE TABLE test_table (a integer PRIMARY KEY, b text);

-- Create a logical replication slot.
SELECT 'init' FROM pg_create_logical_replication_slot('test_slot', 'wal2json');
 ?column?
----------
 init
(1 row)

-- Execute the INSERT operation.
INSERT INTO test_table VALUES (1, 'hello');
INSERT INTO test_table VALUES (2, 'world');

-- View the format version 1 output.
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

-- View the format version 2 output.
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL, 'format-version', '2');
                                                              data
---------------------------------------------------------------------------------------------------------------------------------
 {"action":"B"}
 {"action":"I","schema":"public","table":"test_table","columns":[{"name":"a","type":"integer","value":1},{"name":"b","type":"text","value":"hello"}]}
 {"action":"C"}
```

#### Example 2: UPDATE and DELETE Operations

```sql
-- Table with a primary key.
CREATE TABLE table_with_pk (
  id serial PRIMARY KEY,
  name text
);

SELECT 'init' FROM pg_create_logical_replication_slot('test_slot', 'wal2json');

INSERT INTO table_with_pk (name) VALUES ('Alice');
UPDATE table_with_pk SET name = 'Bob' WHERE id = 1;
DELETE FROM table_with_pk WHERE id = 1;

-- Output of format version 2.
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

#### Example 3: Using Multiple Parameter Options

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

#### Example 4: Table Filtering

```sql
-- Exclude the specified tables.
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL,
  'format-version', '2',
  'filter-tables', 'public.log_table, public.temp_table'
);

-- Include only the specified tables.
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL,
  'format-version', '2',
  'add-tables', 'public.important_table'
);
```

#### Example 5: Operation Type Filtering

```sql
-- Capture only INSERT operations.
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL,
  'format-version', '2',
  'actions', 'insert'
);

-- Capture INSERT and DELETE operations.
SELECT data FROM pg_logical_slot_peek_changes('test_slot', NULL, NULL,
  'format-version', '2',
  'actions', 'insert, delete'
);
```

### Outputs for Different Data Types<a name="section_data_types"></a>

wal2json applies different output logic to different data types:

- **Numeric types** (INT2, INT4, INT8, FLOAT4, FLOAT8, NUMERIC): Output as raw JSON numbers. NaN and Infinity are converted to JSON null.
- **Boolean type** (BOOL): Output as `true` or `false`.
- **Byte type** (BYTEA): Output as a hexadecimal string (without the `\x` prefix).
- **Domain types**: Output the domain type name by default. When `include-domain-data-type` is enabled, output the underlying base data type.
- **Other types**: Output as JSON-escaped strings.

### Deleting a Logical Replication Slot<a name="section_drop_slot"></a>

Use the `pg_drop_replication_slot` function to delete a logical replication slot:

```sql
SELECT 'stop' FROM pg_drop_replication_slot('test_slot');
 ?column?
----------
 stop
(1 row)
```

>[!NOTE]
>
>After a logical replication slot is deleted, changed data that has not been consumed in the slot is discarded. Before deleting the slot, ensure that all changes have been properly consumed or are no longer needed.
