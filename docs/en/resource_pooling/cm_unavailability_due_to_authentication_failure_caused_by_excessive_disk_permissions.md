# Cluster Unavailability Due to Authentication Failure Caused by Excessive Disk Permissions

## Symptom

During database installation, if disk permissions are excessively restrictive, the installation may report success while simultaneously causing authentication failures that render the Cluster Manager (CM) unavailable. The following error message will be present:

```shell
LOG: open gaussdb state file "/.../.../dn1/gaussdb.state" failed, could not get the build information: Permission denied.
```

## Troubleshooting Method

Navigate to the `$GAUSSLOG/cm/cm_agent` directory and inspect the most recent `cm_agent` log file. The following error entries will be identified:

```shell
2024-10-09 11:07:58.966 tid=313678  ERROR: [get_connection: 1526]: fail to read pid file (/.../.../dn1/postmaster.pid).
2024-10-09 11:07:58.966 tid=313678  ERROR: failed to connect to datanode:/.../.../dn1
```

These logs indicate that the `CM` agent lacks sufficient permissions to access the target directory.

## Root Cause

For disk device files, overly strict permission settings can prevent the operating system and associated applications from accessing the storage normally. In this scenario, the installation environment was configured with excessively restrictive directory permissions, resulting in the `CM` agent being unable to access the necessary disk resources.

## Solution

Adjust the directory permissions to standard values using the `chmod` command. The following command recursively grants appropriate permissions to the data directory:

```shell
chmod -R 775 /dir
```

> [!WARNING]
> Ensure that the permissions align with your organization's security policies. Overly permissive settings (such as `777`) may introduce security vulnerabilities.
