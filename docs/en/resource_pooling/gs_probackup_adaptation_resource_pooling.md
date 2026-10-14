# gs_probackup Adaptation for Resource Pooling

## Availability<a name="section15406143204715"></a>

This feature has been available since openGauss 6.0.0-RC1.

## Feature Overview<a name="section740615433477"></a>

This feature adapts the `gs_probackup` tool for the resource pool architecture. It supports full backup, incremental backup, merge, delete, restore, and validation functions.

## Customer Value<a name="section13406743164715"></a>

With `gs_probackup` adapted for resource pooling, database backup and restore capabilities are now available in resource pool deployment scenarios.

## Feature Description<a name="section16406154310471"></a>

The main functions of `gs_probackup` are as follows:

- **Backup**: `gs_probackup` performs physical backups, supporting full backup, incremental backup, and backup compression. Backing up the database enhances its resilience against failures.
- **Restore**: `gs_probackup` supports full and incremental restores, providing the ability to revert the database to a previous state. This restore capability is a critical guarantee for database security and reliability.
- **Validation**: `gs_probackup` provides functions to validate the integrity and consistency of backups, ensuring the reliability of backup data.

In addition to the core functions above, `gs_probackup` also offers features such as backup merging and backup management.

## Enhancements<a name="section1340684315478"></a>

This feature extends the applicable scenarios for the `gs_probackup` tool, enabling its use in resource pool environments.

## Constraints<a name="section06531946143616"></a>

- A successful connection to the openGauss database must be established.
- During restore, ensure the value of the `ulimit -s` parameter is no less than 8192.

## Dependencies<a name="section8406643144716"></a>

This feature depends on the resource pool disk array device.

## Basic Principles

The principle of database backup involves copying database data and metadata to a separate storage location. This ensures that the database can be restored to a previous state in the event of a failure or data loss. Database backups are categorized into physical backups and logical backups:

- **Physical Backup**:
  Physical backup is based on the database's binary files, directly copying database files. This method offers fast backup and restore speeds. It uses OS file copy commands or disk imaging tools to duplicate database files to the backup storage location.
- **Logical Backup**:
  Logical backup is based on the database's logical structure, exporting data and table definitions. While slower than physical backup, it produces smaller backup files and offers greater flexibility. Data and schemas are exported as SQL statements into backup files using database management tools or command-line utilities.

Building upon these backup foundations, openGauss implements three advanced functions: incremental backup, backup merging, and incremental restore:

- **Incremental Backup Principle**:
  1. Modify the xlog file backup path to store files in the `backup_path/instance/database/pg_xlog` directory specified by the `-B` parameter.
  2. Update the `cbmparesexlog` parsing logic to read xlog files from the modified path.
  3. Add a new member variable, `dssdata_bytes`, to the `pgBackup` struct to record file sizes in both `PGDATA` and `DSSDATA` at the start of the backup.

- **Backup Merge Principle**:
  1. In resource pool mode, merged DSS files are stored in the `backup_path/instance/dssdata` directory.
  2. Refactor shared functions used during both backup merging and restoration by introducing parameters to distinguish between the two processes.
  3. Modify error handling to prohibit merging external directory files.

- **Incremental Restore Principle**:
  1. During incremental restore, the `PGDATA` directory and the `+data` directory are permitted to be non-empty.
  2. Refactor variable names and add conditional branches to handle path differences between resource pool and non-resource pool modes.
  3. Exclude files belonging to other nodes when removing redundant files.
  4. Disable the write-ahead logging (WAL) pre-allocation feature during incremental restore.

## Usage Guide

Refer to the [gs_probackup Tool](https://atomgit.com/opengauss/docs/blob/master/docs/en/tool_and_commandreference/gs_probackup.md) usage guide.

## Application Scenarios

This feature supports incremental backup, full backup, restore, and validation functions in resource pool deployments, specifically in one-primary-multiple-standby architectures.
