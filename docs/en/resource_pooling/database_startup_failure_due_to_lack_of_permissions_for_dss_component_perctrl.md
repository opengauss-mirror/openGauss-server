# Database Startup Failure Due to Lack of Permissions for DSS Component perctrl

## Symptom

During the startup of an openGauss resource pool cluster, one node fails to start because the DSS component fails to launch. This issue commonly occurs after a server reboot.

## Troubleshooting Method

Navigate to the `$DSS_HOME/log/run` directory and inspect the most recent `dsscmd.rlog`. Look for log entries similar to the following:

```shell
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|ERROR>[PERCTRL]rgister failed, result:255, Failed to invoke the SCSI interface, OS errno:17, OS error msg:File exists. [ddes_perctrl_api.c:284]
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|ERROR>Scsi3 register failed, rk 1, dev /dev/disk/by-id/wwn-0x6382028100c0772b315389ae0000004c, errno 17. [cm_iofence.c:48]
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|ERROR>Failed to register, rk: 0, dev: /dev/disk/by-id/wwn-0x6382028100c0772b315389ae0000004c, ret: -1. [dss_io_fence.c:249]
UTC+8 2024-10-09 16:06:51.827|dsscmd|1297590|INFO>free g_vgs_info. [dss_diskgroup.c:223]
```

These logs indicate that DSS failed to register the disk, preventing the DSS component from starting and causing the database startup to fail.

## Root Cause

`CAP_SYS_RAWIO` is a Linux capability that permits processes to perform raw I/O operations. Specifically, this capability allows processes to:

1. Access hardware devices directly<br>
Allows processes to interact directly with hardware devices without going through standard filesystem interfaces. This is essential for applications requiring direct hardware interaction, such as device drivers or certain networking applications.
2. Perform low-level I/O operations<br>
Permits processes to execute low-level I/O operations, such as directly reading from or writing to device registers or memory-mapped I/O. This is required for applications demanding high performance or low latency, such as real-time systems.

The DSS `perctrl` component requires the `CAP_SYS_RAWIO` capability to operate disks. However, the current user lacks this permission. Therefore, this capability must be explicitly granted to the `perctrl` process.

## Solution

```shell
sudo -i setcap CAP_SYS_RAWIO+ep absolute-path/perctrl(typically located in $GAUSSHOME/bin)
```
