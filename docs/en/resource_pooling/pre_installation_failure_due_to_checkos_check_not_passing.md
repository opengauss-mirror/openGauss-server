# Pre-installation Failure Due to CheckOS Validation Errors

## Symptom

The pre-installation process fails with the following error message:

```shell
[GAUSS-52400] : Installation environment does not meet the desired result.
Please get more details by "gs_checkos -i A -h openGaussxxx,openGaussxxx -X /home/dbuser/1s1p.xml --detail".
```

## Troubleshooting Method

Use the `gs_checkos` command to identify items marked as **Abnormal**:

```shell
gs_checkos -i A -h openGaussxxx,openGaussxxx -X /home/dbuser/1s1p.xml --detail
```

## Root Cause

`gs_checkos` failures typically stem from inconsistencies or misconfigurations across cluster nodes, including unsupported OS versions, network issues, disk/memory anomalies, or mismatched timezone, time synchronization, and character set settings.

## Solution

Address each abnormal item as follows:

1.  **OS Version (A1)**: If Abnormal, replace the OS with a version supported within the openGauss compatibility matrix.
2.  **Kernel Version (A2)**: If Warning, note that kernel versions differ across nodes. This is permissible but not recommended for production.
3.  **Unicode Status (A3)**: If Abnormal, unify the character set on all nodes. Add `export LANG=XXX` (where XXX is the Unicode encoding) to `/etc/profile`.
4.  **Timezone Status (A4)**: If Abnormal, synchronize timezones across all nodes by copying the correct zone file to `/etc/localtime`:

    ```shell
    cp /usr/share/zoneinfo/$Region/$City /etc/localtime
    ```

5.  **Swap Memory Status (A5)**: If Abnormal, reduce the swap space or increase physical memory to ensure RAM exceeds swap.
6.  **System Control Parameters (A6)**: If Abnormal, remediate using either method:

    a. Auto-fix via `gs_checkos`:

    ```shell
    gs_checkos -i B1
    ```

    b. Manually edit `/etc/sysctl.conf` based on the error prompt, then run `sysctl -p` to apply the changes.
7.  **File System Configuration (A7)**: If Abnormal, auto-fix the configuration:

    ```shell
    gs_checkos -i B2
    ```

8.  **Disk Mount Options (A8)**: If Abnormal, configure XFS mounts with `rw,noatime,inode64,allocsize=16m`:

    a. Manual mount:

    ```shell
    rw,noatime,inode64,allocsize=16m
    ```

    b. Persistent configuration in `/etc/fstab`:

    ```shell
    /dev/data /data xfs rw,noatime,inode64,allocsize=16m 0 0
    ```

9.  **Read-ahead Block Size (A9)**: If Abnormal, auto-fix the setting:

    ```shell
    gs_checkos -i B3
    ```

10. **I/O Scheduler (A10)**: If Abnormal, auto-fix the scheduler setting:

    ```shell
    gs_checkos -i B4
    ```

11. **Network Card Configuration (A11)**: If Warning, optimize the NIC configuration:

    ```shell
    gs_checkos -i B5
    ```

12. **Time Consistency (A12)**: If Abnormal, ensure the time drift does not exceed **6 seconds** for resource pool clusters or **60 seconds** for non-pooled environments. Verify NTP service installation, status, and synchronization with the time source.
13. **Firewall Status (A13)**: If Abnormal, disable the firewall:

    a. SuSE

    ```shell
    SuSEfirewall2 stop
    ```

    b. RedHat7

    ```shell
    systemctl disable firewalld
    systemctl stop firewalld
    ```

    c. RedHat6

    ```shell
    service iptables stop
    ```

14. **THP Service (A14)**: If Abnormal, disable Transparent Huge Pages:

    ```shell
    gs_checkos -i B6
    ```
