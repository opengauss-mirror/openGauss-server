# Business Execution Failure Due to Insufficient Local Drive Space

## Symptom

In an openGauss resource pool deployment scenario, business execution may fail due to insufficient local drive space.

For example, when initializing a warehouse using the standard `BenchmarkSQL` tool, the creation of primary keys may fail. Navigate to the `$GAUSSLOG/pg_log` directory on the primary node and inspect the most recent logs. The following error message will be present:

```shell
No Space Left on Device.
```

This message confirms that the business failure was caused by insufficient drive space.

## Troubleshooting Method

Beyond the aforementioned log evidence, monitor the local drive capacity during business operations by periodically executing `df -h` in the terminal.

In scenarios similar to the example above, the drive hosting the `$PGDATA` directory will be observed approaching `100%` utilization at the exact timestamp of the error, followed immediately by business interruption.

## Root Cause

Although openGauss in a resource pool deployment utilizes shared storage for data files, the database writes `temporary files` to the local drive.

When local drive space is exhausted and `temporary files` cannot be written, the database reports a "No Space Left on Device" error, leading to business interruption.

To prevent this issue, ensure the local drive capacity is provisioned to meet the peak storage demands of the business workload.

## Solution

The following remediation measures are available:

1. During the installation phase, provision the local drive capacity according to the anticipated business storage requirements.
2. If the issue occurs, resolve it by cleaning up unnecessary files or expanding the capacity of the local drive.
