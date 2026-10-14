# Connection Failure Due to Missing Database Socket File

## Symptom

Existing connections continue to operate normally, while new connection attempts fail with the following error message:

```shell
[carrotom@openGauss111 carrotom_mppdb]$ gsql -r
failed to connect /usr3/carrotom/cluster/om/carrotom_mppdb:15555.
```

## Troubleshooting Method

1. The error message explicitly specifies the path and port: `/usr3/carrotom/cluster/om/carrotom_mppdb:15555`.

2. Inspect the database parameter configuration file (`postgresql.conf`) to verify the runtime port and the Unix socket directory location. Confirm that these values match the information in the error message:

    ```shell
    [carrotom@openGauss111 dn1]$ cat postgresql.conf | grep -e port -e unix_socket_directory
    port = 15555
    unix_socket_directory = '/usr3/carrotom/cluster/om/carrotom_mppdb'
    ```

    > **Note:** Discrepancies between the configuration and the error message are acceptable. While these parameters can be modified in the configuration file at any time, they only take effect after a database restart. Always treat the error message as the authoritative source.

3. List the contents of the socket directory to inspect the socket files:

    ```shell
    [carrotom@openGauss111 cluster]$ ll /usr3/carrotom/cluster/om/carrotom_mppdb -a
    total 12K
    drwx------. 2 carrotom carrotom  152 Sep 27 15:00 .
    drwx------. 7 carrotom carrotom 4.0K Sep 27 11:42 ..
    srwx------. 1 carrotom carrotom    0 Sep 27 15:00 .gaussUDF.socket
    -rw-------. 1 carrotom carrotom   93 Sep 27 15:00 .s.PGSQL.15555.lock
    srwx------. 1 carrotom carrotom    0 Sep 27 15:00 .s.PGSQL.15556
    -rw-------. 1 carrotom carrotom   93 Sep 27 15:00 .s.PGSQL.15556.lock
    ```

    Observation: The socket file for port `15555` (`.s.PGSQL.15555`) is missing. Only the lock file (`.lock`) and the auxiliary port socket remain.

## Root Cause

Socket file loss or permission anomalies can stem from various sources, including but not limited to:

- System errors
- Insufficient disk space
- Lack of access permissions for the directory housing the socket files
- Misoperation by the database administrator
- Malware or unauthorized intrusion
- Other unforeseen system anomalies

## Solution

Restart the affected node or the entire cluster to restore the socket file. This operation has no other adverse effects on the database.

Apart from the port configured via the `port` parameter, openGauss reserves an auxiliary port incremented by one (e.g., `15556` as shown in the listing). If the corresponding socket file exists, connections can be established via this reserved port for maintenance purposes.
