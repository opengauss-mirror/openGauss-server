# Resource Pool Storage Dual-Cluster Installation Failure Due to Abnormal Disk Array Write Permissions

## Symptom

The installation of a resource pool storage-based dual-cluster fails.

## Troubleshooting Method

During the installation of the standby cluster, the following error message is displayed:

```shell
dss failed to startup
```

Navigate to the `$DSS_HOME/log/run` directory and inspect the most recent `dssserver.rlog` file. The following error sequence indicates that the DSS instance cannot access the shared storage volumes:

```shell
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|ERROR>Failed to lock vg:/dev/xxx. [dss_diskgroup.c:301]
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|ERROR>DSS instance failed to load vg:data ctrl! [dss_diskgroup.c:301]
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|ERROR>DSS instance failed to initialized! [dss_instance.c:465]
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|ERROR>dss failed to startup. [dssserver.c:282]
```

The logs confirm that the failure is caused by an inability to lock or access the underlying storage devices.

## Root Cause

The LUNs configured for dual-cluster synchronization lacked write permissions on the standby cluster side. 

## Solution

Log in to the **DORADO** storage management platform and split the synchronization pair. Disable the resource protection on the secondary (standby) side to grant write permissions to the LUNs. 
