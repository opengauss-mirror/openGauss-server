# *DB4AI Snapshots* for relational dataset versioning

## 1. Introduction
The module *DB4AI Snapshots* provides a robust and efficient framework that allows any database user to manage multiple versions of relational datasets through a convenient API.

### 1.1 Compact storage with efficient access
Its main benefits are automated management of dependencies among large, versioned datasets and their compact storage. Therefore, *DB4AI Snapshots* leverages redundancies among different versions of data for providing a high level of compaction while minimizing adverse effects on performance. At the same time, snapshot data remains efficiently accessible using the full expressiveness of standard SQL.

### 1.2 Immutable snapshot data
The snapshot API prevents the user from changing versioned data. Similar to a code repository, every change of data will generate a new version, i.e. a new snapshot.

### 1.3 Performance
In addition to compact data representation, the primary goal of *DB4AI Snapshots* is high read performance, i.e. when used in highly repetitive and concurrent read operations for serving as training data sets for concurrent training of multiple ML models.

As a secondary performance goal, *DB4AI Snapshots* provides efficient data manipulation for creating and manipulation huge volumes of versioned relational data.

### 1.4 Documentation and Automation
In addition, *DB4AI Snapshots* maintains a full documentation of the origin of any dataset, providing lineage and provenance for data, e.g. to be used in the context of reproducible training of ML models. In addition, *DB4AI Snapshots* facilitates automation, i.e. when applying repetitive transformation steps in data cleansing, or for automatically updating an existing training dataset with new data.

## 2. Quick start
### 2.1 Setup

*DB4AI Snapshots* is automatically installed in every new database instance of openGauss.
Therefore the CREATE DATABASE procedure creates the database schema *db4ai* within the new database and populates it with objects required for managing snapshot data.

After successful database creation, any user may start exploring the snapshot functionality. No additional privileges are required.

### 2.2 Examples

Set snapshot mode to compact storage model *CSS* (**C**omputed **S**nap**S**hot mode).

    SET db4ai_snapshot_mode = CSS;

Create a snapshot 'm0@1.0.0' from existing data, where stored in table 'test_data'. The SQL statement may use arbitrary joins and mappings for defining the snapshot.

    CREATE SNAPSHOT m0 AS
        SELECT a1, a3, a6, a8, a2, pk, a1 b9, a7, a5 FROM test_data;

Create snapshot 'm0@2.0.0' from snapshot 'm0@1.0.0' by applying arbitrary DML and DDL statements.
The new version number indicates a snapshot schema revision by means of at least one ALTER statement.

    CREATE SNAPSHOT m0 FROM @1.0.0 USING (
        UPDATE SNAPSHOT SET a1 = 5 WHERE pk % 10 = 0;
        ALTER SNAPSHOT ADD " u+ " INTEGER, ADD "x <> y"INT DEFAULT 2, ADD t CHAR(10) DEFAULT '',
            DROP a2, DROP COLUMN IF EXISTS b9, DROP COLUMN IF EXISTS b10;
        UPDATE SNAPSHOT SET "x <> y" = 8 WHERE pk < 100;
        ALTER SNAPSHOT DROP " u+ ", DROP IF EXISTS " x+ ";
        DELETE FROM SNAPSHOT WHERE pk = 3
    );

Create snapshot 'm0@2.0.1' from snapshot 'm0@2.0.0' by UPDATE while using a reference to another table. The new version number indicates an update operation (minor data patch). This example uses an AS clause for introducing 'i' as the snapshot's custom correlation name for joining with tables during the UPDATE operation.

    CREATE SNAPSHOT m0 FROM @2.0.0 USING (
        UPDATE SNAPSHOT AS i SET a5 = o.a2 FROM test_data o
            WHERE i.pk = o.pk AND o.a3 % 8 = 0
    );

Create snapshot 'm0@2.1.0' from snapshot 'm0@2.0.1' by DELETE while using a reference to another table. The new version number indicates a data revision. This example uses the snapshot's default correlation name 'SNAPSHOT' for joining with another table.

    CREATE SNAPSHOT m0 FROM @2.0.1 USING (
        DELETE FROM SNAPSHOT USING test_data o
            WHERE SNAPSHOT.pk = o.pk AND o.A7 % 2 = 0
    );

Create snapshot 'm0@2.2.0' from snapshot 'm0@2.1.0' by inserting new data. The new version number indicates another data revision.

    CREATE SNAPSHOT m0 FROM @2.1.0 USING (
        INSERT INTO SNAPSHOT SELECT a1, a3, a6, a8, a2, pk+1000 pk, a7, a5, a4
            FROM test_data WHERE pk % 10 = 4
    );

The SQL syntax was extended with the new @ operator in relation names, allowing the user to specify a snapshot with version throughout SQL. Internally, snapshots are stored as views, where the actual name is generated according to GUC parameters on arbitrary level, e.g. on database level using the current setting of the version delimiter:

    -- DEFAULT db4ai_snapshot_version_delimiter IS '@'
    ALTER DATABASE <name> SET db4ai_snapshot_version_delimiter = '#';

Similarly the version separator can be changed by the user:

    -- DEFAULT db4ai_snapshot_version_separator IS ’.’
    ALTER DATABASE <name> SET db4ai_snapshot_version_separator = ’_’;

Independently from the GUC parameter settings mentioned above, any snapshot version can be accessed:

    -- standard version string @schema.revision.patch:
    SELECT * FROM public.data@1.2.3;
    -- user-defined version strings:
    SELECT * FROM accounting.invoice@2021;
    SELECT * FROM user.cars@cleaned;
    -- quoted identifier for blanks, keywords, special characters, etc.:
    SELECT * FROM user.cars@"rev 1.1";
    -- or string literal:
    SELECT * FROM user.cars@'rev 1.1';

Alternative, using internal name (depends on GUC settings):

    -- With internal view name, using default GUC settings
    SELECT * FROM public."data@1.2.3";

    -- With internal view name, using custom GUC settings, as above
    SELECT * FROM public.data#1_2_3;

## 3. Privileges
All members of role **PUBLIC** may use **DB4AI Snapshots**.

## 4. Supported Systems and SQL compatibility modes
The current version of *DB4AI Snapshots* is tested in openGauss SQL compatibility modes A, B and C.

## 5. Portability
*DB4AI Snapshots* uses standard SQL for implementing its functionality.

## 6. Dependencies
None.

## 7. Reference & Documentation

### 7.1 Configuration parameters

**DB4AI Snapshots** exposes several configuration parameters, via the system's global unified configuration (GUC) management.
Configuration parameters may be set on the scope of functions (CREATE FUNCTION), transactions (SET LOCAL), sessions (SET),
user (ALTER USER), database (ALTER DATABASE), or on system-wide scope (postgresql.conf).

    SET [SESSION | LOCAL] configuration_parameter { TO | = } { value | 'value' }
    CREATE FUNCTION <..> SET configuration_parameter { TO | = } { value | 'value' }
    ALTER DATABASE name SET configuration_parameter { TO | = } { value | 'value' }
    ALTER USER name [ IN DATABASE database_name ] SET configuration_parameter { TO | = } { value | 'value' }

The following snapshot configuration parameters are currently supported:

#### db4ai_snapshot_mode = { MSS | CSS }
This snapshot configuration parameter allows to switch between materialized snapshot (*MSS*) mode, where every new snapshot is created as compressed but fully
materialized copy of its parent's data, or computed snapshot (*CSS*) mode. In *CSS* mode, the system attempts to exploit redundancies among dependent snapshot versions
for minimizing storage requirements.

The setting of *db4ai_snapshot_mode* may be adjusted at any time and it will have effect on subsequent snapshot operations within the scope of the new setting.

Whenever *db4ai_snapshot_mode* is not set in the current scope, it defaults to *MSS*.

##### Example
    SET db4ai_snapshot_mode = CSS;

#### db4ai_snapshot_version_delimiter = value

This snapshot configuration parameter controls the character that delimits the *snapshot version* postfix within snapshot names.
In consequence, the character used as *db4ai_snapshot_version_delimiter* cannot be used in snapshot names, neither in the snapshot
name prefix, nor in the snapshot version postfix. Also, the setting of *db4ai_snapshot_version_delimiter* must be distinct from
*db4ai_snapshot_version_separator*. Whenever *db4ai_snapshot_version_delimiter* is not set in the current scope, it defaults to
the symbol '@' (At-sign).

*Note:* Snapshots created with different settings of *db4ai_snapshot_version_delimiter* are not compatible among each
other. Hence, it is advisable to ensure the setting is stable, i.e. by setting it permanently, e.g. on database scope.

##### Example
    ALTER DATABASE name SET db4ai_snapshot_version_delimiter = '#';

#### db4ai_snapshot_version_separator = value

This snapshot configuration parameter controls the character that separates the *snapshot version* within snapshot names. In consequence,
*db4ai_snapshot_version_separator* must not be set to any character representing a digit [0-9].
Also, the setting of *db4ai_snapshot_version_separator* must be distinct from *db4ai_snapshot_version_delimiter*.
Whenever *db4ai_snapshot_version_separator* is not set in the current scope, it defaults to punctuation mark '.' (period).

*Note:* Snapshots created with different settings of *db4ai_snapshot_version_separator* do not support automatic version number
generation among each other. Hence, it is advisable to ensure the setting is stable, i.e. by setting it permanently, e.g. on database scope.

##### Example
    ALTER DATABASE name SET db4ai_snapshot_version_separator = '_';

### 7.2 Accessing a snapshot

Independently from the GUC parameter settings mentioned above, any snapshot version can be accessed:

    SELECT … FROM […,]
        <snapshot_qualified_name> @ <vconst | ident | sconst>
        [WHERE …] [GROUP BY …] [HAVING …] [ORDER BY …];

Alternative, using standard version string as internal name (depends on GUC settings):

    SELECT … FROM […,] <snapshot_qualified_name> INTEGER
        <db4ai_snapshot_version_delimiter> INTEGER
        <db4ai_snapshot_version_delimiter> INTEGER
        [WHERE …] [GROUP BY …] [HAVING …] [ORDER BY …];

Alternative, using user-defined version string as internal name (depends on GUC settings):

    SELECT … FROM […,]
        <snapshot_qualified_name> <db4ai_snapshot_version_delimiter> <snapshot_version_string>
        [WHERE …] [GROUP BY …] [HAVING …] [ORDER BY …];

If any component of <snapshot_qualified_name>, <db4ai_snapshot_version_delimiter>, <db4ai_snapshot_version_separator>, or <snapshot_version_string> should contain special characters, then quoting of the snapshot name is required.

### 7.3 Creating a snapshot

    CREATE SNAPSHOT <qualified_name> [@ <version | ident | sconst>]
      [COMMENT IS <sconst>}
      AS <SelectStmt>;

The CREATE SNAPSHOT AS statement is invoked for creating a new snapshot. A caller provides the qualified_name for the snapshot to be created. The \<SelectStmt\> defines the content of the new snapshot in SQL. The optional @ operator allows to assign a custom version number or string to the new snapshot. The default version number is @ 1.0.0.
A snapshot may be annotated using the optional COMMENT IS clause.

**Example:**

    CREATE SNAPSHOT public.cars AS
      SELECT id, make, price, modified FROM cars_table;

The CREATE SNAPSHOT AS statement will create the snapshot 'public.cars@1.0.0' by selecting some columns of all the tuples in relation cars_table, which exists in the operational data store.
The created snapshot's name 'public.cars' is automatically extended with the suffix '@1.0.0' to the full snapshot name 'public.cars@1.0.0', thereby creating a unique, versioned identifier for the snapshot.

The DB4AI module of openGauss stores metadata associated with snapshots in a DB4AI catalog table *db4ai.snapshot*. The catalog exposes various metadata about the snapshot, particularly noteworthy is the field 'snapshot_definition' that provides documentation how the snapshot was generated. The DB4AI catalog serves for managing the life cycle of snapshots and allows exploring available snapshots in the system.

In summary, an invocation of the CREATE SNAPSHOT AS statement will create a corresponding entry in the DB4AI catalog, with a unique snapshot name and documentation of the snapshot's lineage. The new snapshot is in state 'published'. Initial snapshots serve as true and reusable copy of operational data, and as a starting point for subsequent data curation, therefore initial snapshots are already immutable. In addition, the system creates a view with the published snapshot's name, with grantable read-only privileges for the current user. The current user may access the snapshot, using arbitrary SQL statements against this view, or grant read-access privileges to other user for sharing the new snapshot for collaboration. Published snapshots may be used for model training, by using the new snapshot name as input parameter to the *db4ai.train* function of the DB4AI model warehouse. Other users may discover new snapshots by browsing the DB4AI catalog, and if corresponding read access privileges on the snapshot view are granted by the snapshot's creator, collaborative model training using this snapshot as training data can commence.

### 7.4 Creating a snapshot revision

    CREATE SNAPSHOT <qualified_name> [@ <version | ident | sconst>]
      FROM @ <version | ident | sconst>
      [COMMENT IS <sconst>}
      USING (
      { INSERT [INTO SNAPSHOT] …
        | UPDATE [SNAPSHOT] [AS <alias>] SET … [FROM …] [WHERE …]
        | DELETE [FROM SNAPSHOT] [AS <alias>] [USING …] [WHERE …]
        | ALTER [SNAPSHOT] { ADD … | DROP … } [, …]
      } [; …]
    );

The CREATE SNAPSHOT FROM statement serves for creating a modified and immutable snapshot based on an existing snapshot. The parent snapshot is specified by the qualified_name and the version provided in the FROM clause.
The new snapshot is created within the parent's schema and it also inherits the prefix of the parent's name, but without the parent's version number. The statements listed in the USING clause define how the parent snapshot shall be modified by means of a batch of SQL DDL and DML statements, i.e. ALTER, INSERT, UPDATE, and DELETE.

**Examples:**

    CREATE SNAPSHOT public.cars FROM @1.0.0 USING (
      ALTER ADD year int, DROP make;
      INSERT SELECT * FROM cars_table WHERE modified=CURRENT_DATE;
      UPDATE SET year=in.year FROM cars_table in WHERE SNAPSHOT.id=in.id;
      DELETE WHERE modified<CURRENT_DATE-30
    );   -- Example with 'short SQL' notation

    CREATE SNAPSHOT public.cars FROM @1.0.0 USING (
      ALTER SNAPSHOT ADD COLUMN year int, DROP COLUMN make;
      INSERT INTO SNAPSHOT SELECT * FROM cars_table WHERE modified=CURRENT_DATE;
      UPDATE SNAPSHOT SET year=in.year FROM cars_table in WHERE SNAPHOT.id=in.id;
      DELETE FROM SNAPSHOT WHERE modified<CURRENT_DATE-30
    };   -- Example with standard SQL syntax

In this example, the new snapshot version starts with the current state of snapshot 'public.cars@1.0.0' and adds a new column 'year' to the 'cars' snapshot, while dropping column 'make' that has become irrelevant for the user. This first example uses the short SQL notation, where the individual statements are provided by the user without explicitly stating the snapshot's correlation name. In addition to this syntax, openGauss also accepts standard SQL statements (second example) which tend to be slightly more verbose. Note that both variants allow the introduction of custom correlation names in UPDATE FROM and DELETE USING statements with the AS clause, e.g. `UPDATE AS s [...] WHERE s.id=in.id);` or `UPDATE SNAPSHOT AS s [...] WHERE s.id=in.id);` in the example above.

The INSERT operation shows an example for pulling fresh data from the operational data store into the new snapshot. The UPDATE operation exemplifies populating the newly added column 'year' with data coming from the operational data store, and finally the DELETE operation demonstrates how to remove obsolete data from a snapshot. The name of the resulting snapshot of this invocation is 'public.cars@2.0.0'. Similar as in CREATE SNAPSHOT AS, the user may override the default version numbering scheme that generates version number '@2.0.0'. The optional COMMENT IS clause allows the user to associate a descriptive textual 'comment' with the unit of work corresponding to this invocation of CREATE SNAPHOT FROM, for improving collaboration and documentation, as well as for change tracking purposes.

Since all snapshots are immutable by definition, CREATE SNAPSHOT FROM creates a separate snapshot 'public.cars@2.0.0', initially as a logical copy of the parent snapshot 'public.cars@1.0.0', and applies changes from the USING clause, which corresponds to an integral unit of work in data curation. Similar to a SQL script, the batch of operations is executed atomically and consecutively on the logical copy of the parent snapshot. The parent snapshot remains immutable and is completely unaffected by these changes.

Additionally, the system automatically records all applied changes in the DB4AI snapshot catalog, such that the catalog contains an accurate, complete, and consistent documentation of all changes applied to any snapshot. The catalog also stores the origin and lineage of the created snapshot as a reference to its parent, making provenance of data fully traceable and, as demonstrated later, serves as a repository of data curation operations for supporting automation in snapshot generation.

The operations themselves allow data scientists to remove columns from the parent snapshot, but also to add and populate new ones, e.g. for the purpose of data annotation. By INSERT, rows may be freely added, e.g. from the operational data source or from other snapshots. Inaccurate or irrelevant data can be deleted, as part of the data cleansing process, regardless of whether the data comes from an immutable parent snapshot or directly from the operational data store. Finally, UPDATE statements allow correction of inaccurate or corrupt data, serve for data imputation of missing data and allow normalization of numeric values to a common scale.

In summary, the CREATE SNAPSHOT FROM statement was designed for supporting the full spectrum of recurring tasks in data curation:
•	Data cleansing: Remove or correct irrelevant, inaccurate, or corrupt data
•	Data Imputation: Fill missing data
•	Labeling & Annotation: add immutable columns with computed values
•	Data normalization: Update existing columns to a common scale
•	Permutation: Support reordering of data for iterative model training
•	Indexing: Support random access for model training

Invoking CREATE SNAPSHOT FROM statement allows multiple users to collaborate concurrently in the process of data curation, where each user may break data curation tasks into a set of CREATE SNAPSHOT FROM operations, to be executed in atomic batches. This form of collaboration is similar to software engineers collaborating on a common code repository, but here the concept is extended to include code and data. One invocation of CREATE SNAPSHOT FROM corresponds to a commit operation in a git repository.

In summary, an invocation of the CREATE SNAPSHOT FROM statement will create a corresponding entry in the DB4AI catalog, with a unique snapshot name and documentation of the snapshot's lineage. The new snapshot remains in state 'unpublished', potentially awaiting further data curation. In addition, the system creates a view with the created snapshot's name, with grantable read-only privileges for the current user. Concurrent calls to CREATE SNAPSHOT FROM are permissive and result in separate new versions originating from the same parent (branches). The current user may access the snapshot using arbitrary, read-only SQL statements against this view, or grant read-access privileges to other user, for sharing the created snapshot and enabling collaboration in data curation. Unpublished snapshots may not participate in model training. Yet, other users may discover unpublished snapshots by browsing the DB4AI catalog, and if corresponding read access privileges on the snapshot view are granted by the snapshot's creator, collaborative data curation using this snapshot can commence.

### 7.5 Sampling snapshots

    SAMPLE SNAPSHOT <qualified_name> @ <version | ident | sconst>
      [STRATIFY BY attr_list]
      { AS <label> AT RATIO <num> [COMMENT IS <comment>] } [, …]

The SAMPLE SNAPSHOT statement is used to sample data from a given snapshot (original snapshot) into one or more descendant, but independent snapshots (branches), satisfying a condition given under the parameter 'ratio'.

**Example:**

    SAMPLE SNAPSHOT public.cars@2.0.0
      STRATIFY BY color
      AS _train AT RATIO .8,
      AS _test AT RATIO .2;

This invocation of SAMPLE SNAPSHOT creates two snapshots from the snapshot 'cars@2.0.0', one designated for ML model training purposes: 'cars_train@2.0.0' and the other for ML model testing: 'cars_test@2.0.0'. Note that descendant snapshots inherit the parent's schema, name prefix and version suffix, while each sample definition provides a name infix for making descendant snapshot names unique. The AT RATIO clause specifies the ratio of tuples qualifying for the resulting snapshots, namely 80% for training and 20% for testing. The STRATIFY BY clause specifies that the fraction of records for each car color (white, black, red…) is the same in all three participating snapshots.

### 7.6 Publishing snapshots

    PUBLISH SNAPSHOT <qualified_name> @ <version | ident | sconst>;

Whenever a snapshot is created with the CREATE SNAPSHOT FROM statement, it is initially unavailable for ML model training. Such snapshots allow users to collaboratively apply further changes in manageable units of work, for facilitating cooperation in data curation. A snapshot is finalized by publishing it via the PUBLISH SNAPSHOT statement. Published snapshots may be used for model training, by using the new snapshot name as input parameter to the *db4ai.train* function of the DB4AI model warehouse. Other users may discover new snapshots by browsing the DB4AI catalog, and if corresponding read access privileges on the snapshot view are granted by the snapshot's creator, collaborative model training using this snapshot as training data can commence.

**Example:**
    PUBLISH SNAPSHOT public.cars@2.0.0;

Above is an exemplary invocation, publishing snapshot 'public.cars@2.0.0'.

### 7.7 Archiving snapshots

    ARCHIVE SNAPSHOT <qualified_name> @ <version | ident | sconst>;

Archiving changes the state of any snapshot to 'archived', while the snapshot remains immutable and cannot participate in CREATE SNAPSHOT FROM or *db4ai.train* operations. Archived snapshots may be purged, permanently deleting their data and recovering occupied storage space, or they may be reactivated by invoking PUBLISH SNAPSHOT an archived snapshot.

**Example:**

    ARCHIVE SNAPSHOT public.cars@2.0.0;

The example above archives snapshot 'public.cars@2.0.0' that was previously in state 'published' or 'unpublished'.

### 7.8 Purging snapshots

    PURGE SNAPSHOT <qualified_name> @ <version | ident | sconst>;

The PURGE SNAPSHOT statement is used to permanently delete all data associated with a snapshot from the system. A prerequisite to purging is that the snapshot is not referenced by any existing trained model in the DB4AI model warehouse. Snapshots still referenced by trained models cannot be purged.

Purging snapshots without existing descendant snapshots, removes them completely and occupied storage space is recovered. If descendant snapshots exist, the purged snapshot will be merged into adjacent snapshots, such that no information on lineage is lost, but storage efficiency is improved. In any case, the purged snapshot's name becomes invalid and is removed from the system.

**Example:**

    PURGE SNAPSHOT public.cars@2.0.0;

The example above recovers storage space occupied by 'public.cars@2.0.0' by removing the snapshot completely.
