# Cluster Startup Failure Due to Disk Array Being Busy and Inaccessible

## Symptom

During the startup process of an openGauss resource pool deployment cluster, a specific node fails to start and enters the `Manually stopped` state.

## Troubleshooting Method

Inspect the operating system logs located at `/var/log/messages`. The following records indicating disk access failures will be present:

```shell
Mar 7 11:36:24 host59 systemd-udevd[3348439]: Failed to process device, ignoring: Resource temporarily unavailable
Mar 7 11:36:24 host59 systemd-udevd[4114927]: Failed to process device, ignoring: Resource temporarily unavailable
Mar 7 11:36:27 host59 systemd-udevd[4114927]: Failed to process device, ignoring: Resource temporarily unavailable
Mar 7 11:36:27 host59 systemd-udevd[4114927]: Failed to process device, ignoring: Resource temporarily unavailable
Mar 7 11:36:31 host59 systemd-udevd[4114927]: Failed to process device, ignoring: Resource temporarily unavailable
Mar 7 11:36:36 host59 systemd-udevd[4114927]: Failed to process device, ignoring: Resource temporarily unavailable
Mar 7 11:36:38 host59 systemd-udevd[4114927]: Failed to process device, ignoring: Resource temporarily unavailable
```

These logs report the inability to access the underlying storage devices.

## Root Cause

The disk array is unresponsive, preventing the database from performing normal read and write operations.

## Solution

Reduce the I/O pressure on the disk array or increase the I/O bandwidth. Additionally, inspect the disk array hardware to determine whether a physical fault has occurred.
