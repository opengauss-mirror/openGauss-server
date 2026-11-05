# Cluster Startup Failure Due to Abnormal Control File

## Symptom

The openGauss resource pool deployment cluster fails to start or restart, reporting the following error message:

```shell
cm_ctl: start cluster failed in (601)s!

HINT: Maybe the cluster is continually being started in the background.

You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
```

The startup process fails with a 10-minute timeout. Executing `cm_ctl query -Cvipd` reveals that all nodes in the `CMServer State` are normal. However, in the `Datanode State`, all nodes display the status `Down  Manually stopped`, and the overall `cluster_state` is reported as `Unavailable`.

## Troubleshooting Method

1. Navigate to the `$PGDATA` directory and inspect the `DBstart.log` file. The following error message will be present:

    ```shell
    2024-10-10 15:35:00.648 670783a4.1 [unknown] 281469404839952 [unknown] 0 dn_6001_6002 00000  0 [BACKEND] LOG:  base_page_saved_interval is 400, ori is 400.

    gaussdb: could not find the database system

    Expected to find it in the directory "/.../.../dn1",

    but could not open file "+data/pg_control": Unknown error 2190
    ```

    This log indicates that the `+data/pg_control` file is abnormal.

2. Stop the CM services using the `cm_ctl stop` command. Then, start the DSS service manually by executing `dssserver -M &`. The message `DSS SERVER STARTED.` confirms that the DSS service has launched successfully.

3. After successfully starting the DSS service, verify the status of the `pg_control` file using the `dsscmd ls -p +data` command. Inspect whether the file exists, is corrupted, or has been renamed to identify the root cause.

## Root Cause

The system control file `pg_control` is abnormal, preventing the cluster from starting or restarting.

## Solution

Depending on the specific condition of the `pg_control` file, apply the following solutions:

1. **File Missing or Corrupted:**
    If the `pg_control` file is missing or irreversibly corrupted, uninstall the cluster and perform a fresh reinstallation.

2. **File Renamed:**
    If the `pg_control` file exists and is intact but has been renamed, restore the original filename using the `dsscmd rename` command.

    ```shell
    # Restore the filename from pg_control.old to pg_control
    dsscmd rename -o +data/pg_control.old -n +data/pg_control

    # Stop the DSS service
    dsscmd stopdss

    # Start the cluster
    cm_ctl start
    ```
