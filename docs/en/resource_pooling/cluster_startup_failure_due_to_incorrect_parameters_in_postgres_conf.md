# Cluster Startup Failure Due to Incorrect Parameters in postgresql.conf

## Symptom

When starting or restarting an openGauss resource pool deployment cluster, the following error message is reported:

```shell
cm_ctl: start cluster failed in (601)s!

HINT: Maybe the cluster is continually being started in the background.
You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
```

The startup process fails with a 10-minute timeout. Executing `cm_ctl query -Cvipd` reveals that all nodes in the `CMServer State` are normal. However, some nodes in the `Datanode State` display the status `Down  Manually stopped`, and the overall `cluster_state` is reported as `Degraded`.

## Troubleshooting Method

Log in to the affected node machine and navigate to the `$PGDATA` directory. Inspect the `DBstart.log` file for the following error messages:

**1. Out-of-Range Integer Values:**

```shell
0 [BACKEND] LOG:  1048576 is outside the valid range for parameter "max_process_memory" (2097152 .. 2147483647)
2024-10-11 09:23:11.594 67087dff.10000 [unknown] 281468814098448 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] FATAL:  configuration file "/.../.../dn1/postgresql.conf" contains errors
```

```shell
0 [BACKEND] LOG:  invalid value for parameter "max_process_memory": "2247482647"
0 [BACKEND] HINT:  Value exceeds integer range.
2024-10-11 09:55:28.257 67088590.10000 [unknown] 281468215689232 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] FATAL:  configuration file "/.../.../dn1/postgresql.conf" contains errors
```

These logs indicate that the value configured for the `max_process_memory` parameter is either too low or too high.

**2. Invalid Shared Buffers Configuration:**

```shell
0 [BACKEND] LOG:  15 is outside the valid range for parameter "shared_buffers" (16 .. 1073741823)
2024-10-11 10:23:10.891 67088c0e.10000 [unknown] 281466526957584 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] FATAL:  configuration file "/.../.../dn1/postgresql.conf" contains errors
```

```shell
0 [BACKEND] LOG:  1083741823 is outside the valid range for parameter "shared_buffers" (16 .. 1073741823)
2024-10-11 10:37:44.079 67088f78.10000 [unknown] 281460336885776 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] FATAL:  configuration file "/.../.../dn1/postgresql.conf" contains errors
```

These logs indicate that the value configured for the `shared_buffers` parameter is either too low or too high.

**3. Unsupported Feature Conflict:**

If the `enable_incremental_checkpoint` parameter is set to `off` and the cluster is subsequently restarted, there is a small probability that certain nodes will fail to start, resulting in a `Down` state. The `DBstart.log` will contain the following error:

```shell
2024-09-03 21:40:53.303 66d711e5.1 [unknown] 281457756798992 [unknown] 0 dn_6001_6002_6003 58000 0 [BACKEND] FATAL:  archive functions are not supported when DMS and DSS enabled
```

This indicates that archive functions are currently enabled but not supported in this configuration. Support for enabling XLOG archiving in this scenario is planned for version `7.0.0-RC1`. For reference, see the related community issue: [Setting GUC parameter enable_incremental_checkpoint to off causes probabilistic primary node startup failure](https://gitee.com/opengauss/openGauss-server/issues/IAQYW7?from=project-issue).

Issues regarding other non-compliant parameters manifest similarly, with specific error details logged in `DBstart.log`.

## Root Cause

The cluster fails to start or restart due to abnormal parameter configurations within the system configuration file `postgresql.conf`. For detailed parameter descriptions, refer to [GUC Parameter Usage](https://gitcode.com/opengauss/docs/blob/master/docs/en/database_reference/guc_parameter_usage.md).

## Solution

Identify the specific erroneous parameters by inspecting the `DBstart.log` file. Modify these parameters to valid or recommended values, then restart the cluster.
