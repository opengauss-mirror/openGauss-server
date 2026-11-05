# Cluster Startup Failure Due to Incorrect Parameters in pg_hba.conf

## Symptom

The openGauss resource pool deployment cluster fails to start or restart, reporting the following error message:

```shell
cm_ctl: start cluster failed in (601)s!

HINT: Maybe the cluster is continually being started in the background.
You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
```

The startup process fails with a 10-minute timeout. Executing `cm_ctl query -Cvipd` reveals that all nodes in the `CMServer State` are normal, while some nodes in the `Datanode State` display the status `Down`.

## Troubleshooting Method

1. Log in to the affected node and navigate to the `$PGDATA` directory. Inspect the `DBstart.log` file for the following error messages:

    ```shell
    2024-10-11 14:27:29.056 6708c550.1 [unknown] 281459392315408 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] LOG:  end-of-line before authentication method

    2024-10-11 14:27:29.056 6708c550.1 [unknown] 281459392315408 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] CONTEXT:  line 94 of configuration file "/.../.../dn1/pg_hba.conf"

    2024-10-11 14:27:29.056 6708c550.1 [unknown] 281459392315408 [unknown] 0 dn_6001_6002 42809  0 [BACKEND] FATAL:  could not load pg_hba.conf
    ```

    ```shell
    2024-10-11 14:46:31.121 6708c9c6.1 [unknown] 281464592334864 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] LOG:  invalid authentication method "sm9"

    2024-10-11 14:46:31.121 6708c9c6.1 [unknown] 281464592334864 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] CONTEXT:  line 94 of configuration file "/.../.../dn1/pg_hba.conf"

    2024-10-11 14:46:31.121 6708c9c6.1 [unknown] 281464592334864 [unknown] 0 dn_6001_6002 42809  0 [BACKEND] FATAL:  could not load pg_hba.conf
    ```

    These logs indicate that the authentication method at line 94 in the `pg_hba.conf` file is either missing or set to an unsupported value.

2. Navigate to the `$GAUSSLOG/pg_log/dn_xxx` directory and inspect the most recent `postgresql-xxx.log` file. The following error messages will be present:

    ```shell
    2024-10-11 16:14:42.190 6708de72.5065 postgres 281464533200800 dn_6002 0 dn_6001_6002 28000  0 [BACKEND] FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL on

    2024-10-11 16:14:42.216 6708de72.5065 postgres 281464533200800 dn_6002 0 dn_6001_6002 28000  0 [BACKEND] FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL off

    2024-10-11 16:14:42.233 6708de34.6257 [unknown] 281464591396768 dn_6002 0 dn_6001_6002 YY004  0 [BACKEND] ERROR:  Could not connect to the postgres, we have tried 10 times, the connection info: FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL on

            FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL off
    ```

3. Navigate to the `$GAUSSLOG/cm/cm_agent` directory and inspect the most recent `cm_agent` log file. The following error messages will be present:

    ```shell
    2024-10-11 16:49:55.955 tid=2425227  ERROR: get connect failed for dn(/.../.../dn1/postmaster.pid) phony dead check, errmsg is FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL off

    FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL off

    2024-10-11 16:49:55.955 tid=2425227  LOG: has found 51 times for instance(dn_6002) phony dead check.

    2024-10-11 16:49:56.431 tid=2425223  ERROR: failed to connect to datanode:/.../.../dn1

    2024-10-11 16:49:56.431 tid=2425223  ERROR: connection return errmsg : FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL off

    FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL off
    ```

    Errors in steps 2 and 3 indicate that the IP address configuration within the `pg_hba.conf` file is incorrect, preventing the cluster from starting or restarting.

## Root Cause

Incorrect parameter configurations within the system configuration file `pg_hba.conf` prevent the cluster from starting or restarting. For detailed parameter descriptions, refer to [Configuration File Reference](https://atomgit.com/opengauss/docs/blob/master/docs/en/database_administration_guide/configuration_file_reference.md).

## Solution

Identify the specific erroneous parameters by inspecting the `DBstart.log` and `postgresql-xxx.log` files. Modify the incorrect parameters to valid or recommended values, then restart the cluster.
