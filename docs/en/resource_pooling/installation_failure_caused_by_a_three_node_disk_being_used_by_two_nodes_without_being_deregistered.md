
# Installation Failure Caused by a Three-Node Disk Being Used by Two Nodes Without Being Deregistered

## Symptom

During the installation of a resource pool environment, a disk is used by two nodes without being properly deregistered, reporting the following error message:

```shell
The disk is not registered correctly, unable to access a specific disk location.
```

## Troubleshooting Method

Use the following commands to verify whether the disk appears in the output lists:

1. Use the `lsblk` command to list block devices (including disks and partitions) and their mount points. If the disk has been deregistered, it will not appear in the output list.

    ```shell
    NAME   MAJ:MIN   RM  SIZE  RO TYPE MOUNTPOINT
     sda      8:0    0   50G   0    disk 
     ├─sda1   8:1    0   49G   0    part /
     └─sda2   8:2    0    1G   0    part [SWAP]
    ```

2. Use the `fdisk -l` command to list disk and partition information. Similarly, if the disk has been deregistered, it will not be displayed in the output.

    ```shell
    Disk /dev/sda: 50 GiB, 53687091200 bytes, 104857600 sectors
    Disk model: QEMU HARDDISK
    Units: sectors of 1 * 512 = 512 bytes
    Sector size (logical/physical): 512 bytes / 512 bytes
    I/O size (minimum/optimal): 512 bytes / 512 bytes
    Disklabel type: gpt
    Disk identifier: XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX

    Device      Start      End       Sectors   Size      Type
    /dev/sda1   2048    102399999   102397952  49G    Linux filesystem
    /dev/sda2 102400000 104855551   2455552    1.2G   Linux swap
    ```

## Root Cause

Several factors may lead to this issue:

1. **Insufficient User Permissions**: If the current user lacks sufficient privileges to deregister the disk, the operation will fail. Typically, administrative privileges are required to perform advanced operations such as disk deregistration.
2. **Hardware Issues**: If the disk itself or the hardware connecting it (such as cables or controllers) fails, deregistration may fail. Common issues include bad sectors, controller malfunctions, or loose cable connections.

## Solution

Use the `dsscmd unreghl` command to deregister the old disk, then restart the installation process.

```shell
dsscmd unreghl [-t type] [-D DSS_HOME]
```
