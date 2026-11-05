# Common Causes and Analysis Methods for openGauss Failing to Start

## Symptom

During the startup process of an openGauss cluster, a specific node fails to start. The symptoms manifest as the openGauss process being unable to launch. In a resource pool deployment, executing `cm_ctl query -Cvdip` reveals that the `dms_res` resource on the affected node displays the status `Deleted`. In non-resource pool deployments, executing `cm_ctl query -Cvdip` or `gs_om -t status --detail` reveals that the affected node displays the status `Down`.

## Common Causes

There are numerous reasons why GaussDB might fail to start. The following enumerates several common causes unrelated to code defects or data corruption:

1.  **Incorrect Parameter Configuration in `postgresql.conf`**
    Error messages related to this category are typically printed directly to the screen during startup. In resource pool deployments, they are logged to `DBstart.log` within the DN directory. Log in to the affected node, navigate to the `$PGDATA` directory, and search for `FATAL` entries in `DBstart.log`. The following log snippet illustrates this scenario (in non-resource pool deployments using `gs_om`, errors are typically printed directly to the screen):

    ```shell
    2024-10-08 11:19:05.721 6704a4a9.10000 [unknown] 139630800800128 [unknown] 0 dn_6001_6002_6003 22023 0 [BACKEND] LOG:  invalid value for parameter "recovery_max_workers": "1as"
    2024-10-08 11:19:05.721 6704a4a9.10000 [unknown] 139630800800128 [unknown] 0 dn_6001_6002_6003 F0000 0 [BACKEND] FATAL:  configuration file "/xxx/postgresql.conf" contains errors
    ```

    The logs preceding this entry usually identify the specific parameter that failed validation. Correcting the parameter value to fall within the valid range typically resolves the issue.

2.  **Insufficient System Resources**

    a. **Memory:** During startup, the database allocates memory based on the parameters configured in `postgresql.conf`. If system memory is insufficient, startup may fail. Typical error logs include:

    ```shell
    2024-10-08 11:30:07.378 6704a73f.1 [unknown] 140397937584512 [unknown] 0 dn_6001_6002_6003 42809 0 [BACKEND] FATAL:  could not create shared memory segment: Cannot allocate memory
    ```

    This indicates the database cannot allocate the configured shared memory size. Adjusting the `shared_buffers` parameter in `postgresql.conf` can resolve this.

    b. **File Descriptors:** During the crash recovery phase, the database opens data files and WAL files. If the `open files` limit for the database OS user (verifiable via `ulimit -a`) is too low, startup may fail. Symptoms include `out of file descriptors` in the runtime log (`$GAUSSLOG/pg_log/dn_xxx/postgresql-XXXX-XX-XX_XXXXXX.log`) or `insufficient file descriptors available to start server process` in the startup log (`DBstart.log`). Increasing the system resource limits for the database user (via `ulimit -n xxx` in the user profile or by modifying `/etc/security/limits.conf`) typically resolves this.

3.  **Unix Socket File Exists or Directory Permission Anomalies**
    During startup, the database creates a `.s.PGSQL.XXXX` and `.s.PGSQL.XXXX.lock` file (where `XXXX` is the configured port number) in the `unix_socket_directory` (configurable in `postgresql.conf`, defaulting to `/tmp` or `$PGHOST`). If these files already exist, startup may fail. Logs typically contain `permission denied` messages referencing the socket file. Resolving directory permissions or manually deleting the existing socket files can fix this.

4.  **Missing Checkpoint Record**
    Logs containing `FATAL: could not locate required checkpoint record` indicate the database cannot locate the WAL file corresponding to the last checkpoint recorded in `pg_control` during startup. To diagnose:
    *   Determine the last checkpoint location using `pg_controldata <DN_DIR>`. Look for the `Latest checkpoint location` field.
    *   Convert this LSN to a WAL filename using the `pg_xlogfile_name` function (connect via `gsql` and execute `select pg_xlogfile_name('LSN_VALUE');`).
    *   Verify if the corresponding WAL file exists in the `pg_xlog` directory.
    *   If the file exists, it may be corrupted or the offset invalid. If backups are available, attempt recovery using them, though data loss is possible. If a full backup exists that aligns with the current WAL stream, Point-in-Time Recovery (PITR) may be attempted. Otherwise, contact technical support.

5.  **Other Causes**
    Refer to [Service Startup Failure](./service_startup_failure.md) for additional common causes.

## Analysis Methods

1.  **Log Correlation:** Analyze both startup logs (`DBstart.log`) and runtime logs (`postgresql-*.log`) concurrently. If startup logs are inaccessible, attempt a manual start using `gaussdb -D <DN_DIR>` and observe the console output.
2.  **Startup Phase Identification:** The startup process can be broadly categorized into parameter loading, resource allocation, and crash recovery phases. Monitoring runtime logs for phase-specific keywords helps narrow down the failure point. For instance, if `StartupXLOG` is not present in the logs, the failure occurred before the crash recovery phase (relating to causes 1-3 above).
3.  **Redo Anomalies:** Certain scenarios involve redo playback failures:
    *   **Invalid Page References:** Logs show `[REDO_LOG_TRACE]WAL contains references to invalid pages` followed by a PANIC. This occurs when the database cannot find a data page referenced by a WAL record during replay, or if page LSNs do not match. Restarting rarely resolves this. Restore from backup immediately if available, preserve all logs, and contact technical support.
    *   **LSN Mismatch:** Logs show `The Page's LSN[xx/xxxxxxxx] bigger than want set LSN [xx/xxxxxxxx]` followed by a PANIC. This indicates data page LSNs are higher than those achievable by replay, often caused by manual deletion or corruption of WAL files in `pg_xlog`. Restoring from a valid backup is the recommended recovery path.

## Information Collection

For startup failures, collect the following information:

1.  **Database Runtime Logs:** Collect logs from `$GAUSSLOG/pg_log` covering the failure time and the last successful run. Backing up all logs is recommended if feasible.
2.  **WAL Files (XLOG):** Collect WAL files from the redo start position to the latest. The redo start position can be found via the `redo record is at` keyword in runtime logs or the `Latest checkpoint's REDO location` field in `pg_controldata` output. Backing up all WAL files is recommended if feasible.
3.  **Transaction Status Logs:** Collect contents of the `pg_clog` and `pg_csnlog` directories. These are typically small; compress them if necessary.
4.  **Data Files:** If specific corrupted data files are identified in runtime logs, back up those files.
5.  **DMS Logs:** For resource pool deployments, collect DMS logs located in `$GAUSSLOG/pg_log/DMS`.
6.  **DSS Logs:** For resource pool deployments, collect DSS logs located in `$GAUSSLOG/pg_log/DSS` and `$DSS_HOME/log/run`.
