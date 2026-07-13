# "Error:No space left on device" Is Displayed<a name="EN-US_TOPIC_0291615090"></a>

## Symptom<a name="section668912633713"></a>

The following error message is displayed when the cluster is being used:

```
Error:No space left on device
```

## Cause Analysis<a name="section1232620121376"></a>

The disk space is insufficient.

## Procedure<a name="section1643310207377"></a>

- Run the following command to check the disk usage. The  **Avail**  column indicates the available disk space, and the  **Use%**  column indicates the percentage of disk space that has been used.

```
[root@openeuler123 mnt]# df -h
Filesystem                  Size  Used Avail Use% Mounted on
devtmpfs                    255G     0  255G   0% /dev
tmpfs                       255G   35M  255G   1% /dev/shm
tmpfs                       255G   57M  255G   1% /run
tmpfs                       255G     0  255G   0% /sys/fs/cgroup
/dev/mapper/openeuler-root  196G  8.8G  178G   5% /
tmpfs                       255G  1.0M  255G   1% /tmp
/dev/sda2                   9.8G  144M  9.2G   2% /boot
/dev/sda1                    10G  5.8M   10G   1% /boot/efi
```

    The demand for remaining disk space depends on the increase in service data. Suggestions:

    -   Check the disk space usage status, ensuring that the remaining space is sufficient for the growth of disk space for over one year.

    -   If the disk space usage exceeds 60%, you must clear or expand the disk space.

- Run the following command to check the size of the data directory.

    ```
     du --max-depth=1 -h /mnt/ 
    ```

    The following information is displayed. The first column shows the sizes of directories or files, and the second column shows all the sub-directories or files under the  **/mnt/**  directory.

    ```
    [root@openGauss36 mnt]# du --max-depth=1 -h /mnt
    83G /mnt/data3
    71G /mnt/data2
    365G /mnt/data1
    518G /mnt
    ```

- Clean up the disk space. You are advised to periodically back up audit logs to other storage devices. The recommended log retention period is one month.  **pg\_log**  stores database process run logs which help database administrators locate faults. You can delete error logs if you view them every day and handle errors in time.

- Delete useless data. Back up data that is not used frequently or used for a certain period of time to storage media with lower costs, and clean the backup data to free up disk space.

- If the disk space is still insufficient, expand the disk capacity.
