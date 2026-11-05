# Remote Incremental Backup Failure Due to Residual Logical Replication Slots

## Symptom

The `gs_probackup` utility fails when performing a remote incremental backup.

1. Create and initialize a backup directory:
`gs_probackup init -B xxx`  
2. Initialize a new backup instance within the backup path:
`gs_probackup add-instance -B xxx --instance=instance2 -D xxx/cluster/dn1 --remote-proto=ssh --remote-host=xxx --remote-port=22 --remote-user=xxx --remote-path=xxx/cluster/app/bin --remote-libpath=xxx/cluster/app/lib`
3. Perform a remote full backup:
`gs_probackup backup -B xxx -b full --instance=instance2 -p xxx -d xxx -U xxx-W xxx --remote-proto=ssh --remote-host=xxx --remote-port=22 --remote-user=xxx --remote-path=xxx/app/bin --remote-libpath=xxx/app/lib --backup-pg-replslot`
4. Perform a remote incremental backup:
`gs_probackup backup -B xxx -b PTRACK --stream --instance=instance2 -p xxx -d xxx -U xxx-W xxx --remote-proto=ssh --remote-host=xxx --remote-port=22 --remote-user=xxx --remote-path=xxx/app/bin --remote-libpath=xxx/app/lib --backup-pg-replslot`  

**Error Message:**  

    ```shell
    LOG: Backup destination is initialized
    LOG: This openGauss instance was initialized with data block checksums. Data block corruption will be detected
    LOG: Start SSH client process, pid 2816081
    LOG: Database backup start
    WARNING: logical replication slots for subscriptions will be backed up. If dont use them after restoring, please drop them to avoid affecting xlog recycling.
    LOG: Latest valid FULL backup: RKZOTD
    INFO: Parent backup: RKZOTD
    LOG: started streaming WAL at 0/7000000 (timeline 1)
    [2022-11-07 17:25:41]: check identify system success
    [2022-11-07 17:25:41]: send START_REPLICATION Q/7000000 success
    [2022-11-07 17:25:41]: keepalive message is received
    LOG: SSH process 2816081 is terminated with status O
    INFO: PGDATA size: 644MB
    LOG: Current tli: 1
    LOG: Parent start_lsn: 0/776D5A8
    LOG: start lsn: 0/776D5A8
    INFO: Extracting pagemap of changed blocks
    INFO: change bitmap start lsn location is 0/776D5A8
    INFO: change bitmap end lsn location is 00000000/2E000028
    ERROR: query failed: ERROR: could not find valid CBM file that contains the merging start point 00000000/0776D5A8
    query was: SELECT path,changed_block number,changed_block_list FROM pg_cbm_get changed_block($1, $2)
    WARNING: backup in progress, stop backup
    INFO: wait for pg_stop backup()
    INFO: pg_stop backup() successfully executed
    WARNING: Backup RKZOUS is running, setting its status to ERROR
    ```

## Troubleshooting Method

1. The error message indicates that the remote incremental backup process failed to locate a valid CBM file, and the query path and page numbers are incorrect:

    ```shell
    ERROR: query failed: ERROR: could not find valid CBM file that contains the merging start point 00000000/0776D5A8
    query was: SELECT path,changed_block number,changed_block_list FROM pg_cbm_get changed_block($1, $2)
    ```

2. Incremental backups rely on `start_lsn` to query CBM files. Inspect the `start_lsn` recorded in each CBM file. 
Executing the following command reveals residual logical replication slots, two of which are in an **inactive (`false`)** state. This confirms that the erroneous `start_lsn` from these inactive slots caused the lookup of incorrect CBM files:

    ```shell
    select * from pg_recvlogical ('table_slot','pgoutput');
    ```

    <table>
        <tr>
            <th>slot_name</th>
            <th>plugin</th>
            <th>slot_type</th>
            <th>datoid</th>
            <th>active</th>
            <th>catalog_xmin</th>
            <th>restart_lsn</th>
            <th>dummy_standby</th>
            <th>confirmed_flush</th>
        </tr>
        <tr>
            <td>pg_16420_synv_16394</td>
            <td>pgoutput</td>
            <td>logical</td>
            <td>16384</td>
            <td>t</td>
            <td>436010</td>
            <td>0/776D5AB</td>
            <td>false</td>
            <td>0/776D5AB</td>
        </tr>
                <tr>
            <td>pg_16420_synv_16389</td>
            <td>pgoutput</td>
            <td>logical</td>
            <td>16384</td>
            <td>t</td>
            <td>436010</td>
            <td>0/776D700</td>
            <td>false</td>
            <td>0/776D700</td>
        </tr>
    </table>

## Root Cause

The environment contained residual logical replication slots used for publication/subscription. Two of these slots were in an inactive (`false`) state, causing their `restart_lsn` values to remain stagnant.

When the `--backup-pg-replslot` option is specified, `gs_probackup` traverses all replication slots to determine the oldest `restart_lsn`, which it then uses as the backup's `start_lsn`. Because these residual slots were never dropped, the system consistently selected the stale LSN `0/776D5A8`.

Incremental backups use this `start_lsn` to query the CBM (Change Bitmap) files via `pg_cbm_get_changed_block()`. Since the `restart_lsn` from the inactive slots pointed to a location that was too old or had been recycled, the CBM file containing that LSN no longer existed, resulting in the observed failure.

## Solution

1. Implement logic to clean up residual replication slots, ensuring that different publishers return the correct signals regarding slot status.
2. Manually drop the residual logical replication slots. After cleaning the environment, the remote incremental backup will succeed.
