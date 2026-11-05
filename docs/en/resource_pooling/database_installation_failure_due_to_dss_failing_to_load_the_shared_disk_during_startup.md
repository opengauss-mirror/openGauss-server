# Database Installation Failure Due to DSS Failing to Load Shared Disks During Startup

## Symptom

During the installation of a resource pool environment, if `dss load vg` fails, the following error message appears:

```shell
[GAUSS-51252] : Failed to start the DSS server. Please check the dss logs.
```

## Troubleshooting Method

Navigate to the `$DSS_HOME/log/run` directory and inspect the `DSS server` logs. Look for entries similar to the following:

```shell
UTC+8 2024-10-24 10:49:49.o24|dssserver|800864|ERROR>Scsi3 caw failed, addr 1032192, dev /dev/disk/by_id/wwn-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx, errno 0. [cm_dlock.c:327]
UTC+8 2024-10-24 10:49:49.o24|dssserver|800864|ERROR>Scsi3 caw failed, addr 1032192, dev /dev/disk/by_id/wwn-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx, errno 0. [cm_dlock.c:327]
UTC+8 2024-10-24 10:49:49.o24|dssserver|800864|ERROR>Failed to lock vg, entry_path /dev/disk/by_id/wwn-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx. [dss_diskgroup.c:938]
UTC+8 2024-10-24 10:49:49.o24|dssserver|800864|ERROR>Failed to lock vg:/dev/disk/by_id/wwn-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx. [dss_diskgroup.c:583]
UTC+8 2024-10-24 10:49:49.o24|dssserver|800864|ERROR>DSS instance failed to load vg:data ctrl! [dss_diskgroup.c:356]
UTC+8 2024-10-24 10:49:49.o24|dssserver|800864|ERROR>DSS instance failed to load vg:data! [dss_diskgroup.c:387]
UTC+8 2024-10-24 10:49:49.o24|dssserver|800864|ERROR>[RECOVERY]ABORT INFO: Failed to get vg info when instance start. [dss_instance.c:661]
```

The logs clearly indicate `DSS instance failed to load vg`, confirming that DSS failed to load the shared disks. Next, review the installation configuration file to verify the disk naming convention used.

## Root Cause

In Linux systems, `/dev` stores device files. The traditional naming scheme (`/dev/xxx`) relies on basic device names, which can change based on factors such as device addition, removal, or boot order. For example, adding a new disk might cause the original `sda` device to be renamed to `sdb`. This instability can break configurations that rely on static device names, such as those found in `/etc/fstab`.

In contrast, `/dev/disk/by-id` provides a stable device naming structure. Device files in this directory are named using unique identifiers derived from manufacturer data, model numbers, and serial numbers. As long as the physical characteristics of the device remain unchanged, the file name remains consistent regardless of boot order or hardware changes. This stability simplifies system management and troubleshooting.

The root cause of this issue is inconsistent disk naming across nodes in the `cluster_config.xml` file, specifically the use of the unstable `/dev/xxx` naming convention.

## Solution

First, inspect the `cluster_config.xml` file used during installation. If the `/dev/xxx` paths do not guarantee consistency across all nodes, replace them with the persistent `/dev/disk/by-id` paths.

Next, uninstall the existing deployment using the `gs_uninstall --delete-data` command. Then, re-run the pre-installation and installation procedures.