# Core Dump During Full Backup in gs_probackup Due to Insufficient System Resources

## Symptom

During a `gs_probackup` full backup operation with a high concurrency setting of `-j 10000000`, a `core dumped` error occurs.

1. Create and initialize the backup directory:

```
gs_probackup init -B /home/zcz_ct/f_aler_rowcompress_0059
```

2. Initialize a new backup instance within the backup path:

```
gs_probackup add-instance -B /home/zcz_ct/f_aler_rowcompress_0059 -D /usr1/zczct_install/omm/openGauss/dn1 --instance=pro1
```

3. Execute the full backup:

```
gs_probackup backup -B /home/zcz_ct/f_aler_rowcompress_0059 --instance -b full -d postgres -j 10000000 --instance-id 1
```

4. The actual output terminates with a `core dumped` message:

```
[2024-05-16 12:24:18]: keepalive message is received
[2024-05-16 12:24:23]: keepalive message is received
[2024-05-16 12:24:28]: keepalive message is received
[2024-05-16 12:24:33]: keepalive message is received
[2024-05-16 12:24:38]: keepalive message is received
Segmentation fault (core dumped)
```

## Troubleshooting Method

1. By examining the error output of the full backup, it can be observed that the backup process was abruptly terminated.

```
gs_probackup backup -B /home/lyog/backup --instance backup
-b full -d postgres -j 1000000000 --instance-id 1
INFO: Backup start, gs_probackup version: 2.4.2, instance: backup, backup ID:
SDK7WK, backup mode: FULL, wal mode: STREAM, remote: false, compress algorithm:
none, compress-level: 1
LOG: Backup destination is initialized
LOG: This openGauss instance was initialized with data block checksums. Data
block corruption will be detected
LOG: Database backup start
LOG: started streaming WAL at 0/80000000 (timeline 1)
```

2. Through code analysis, it was discovered that in resource pool deployment scenarios, the number of `gs_probackup` threads is restricted. The system also checks the number of available CPU cores. If the specified thread count exceeds ten times this value, an error is raised.

## Root Cause

During a full backup, the `-j` parameter enables multi-threading. Each thread consumes system resources such as stack space and CPU. If the `-j` value is set excessively high, the system will run out of resources, resulting in a core dump that terminates the backup process. Therefore, an excessively high thread count degrades both backup efficiency and stability.

## Solution

Add a `check_threads_num_option` validation in the `main` function to limit the number of `gs_probackup` threads. The system should detect the number of available CPU cores and raise an error if the specified thread count exceeds ten times that value.
