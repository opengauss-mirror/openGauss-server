# Connection Failure Due to Abnormal Shared Disk Permissions

## Symptom

Attempts to connect to the database fail with the following error message:

```shell
[carrotom@openGauss111 ~]$ gsql -r
gsql: FATAL:  Could not connect dssserver, vgname: "+data", socketpath: "UDS:/usr3/carrotom/cluster/dss_home/.dss_unix_d_socket"
HINT:  Check vgname and socketpath and restart later.
```

## Troubleshooting Method

1.  **Inspect the Socket File:**
    Verify whether the Unix Domain Socket file exhibits any anomalies:

    ```shell
    [root@openGauss111 script]# ll /usr3/carrotom/cluster/dss_home/.dss_unix_d_socket
    srw-------. 1 carrotom carrotom 0 Sep 27 15:18 /usr3/carrotom/cluster/dss_home/.dss_unix_d_socket
    ```

2.  **Inspect the Shared Disk Permissions:**
    If the socket file appears normal, investigate the permissions of the shared disks. List the block devices to verify ownership:

    ```shell
    brw-rw----. 1 root        root         66,  16 Sep 27 16:40 sdah
    brw-rw----. 1 carrotom    carrotom     66,  32 Sep 27 16:40 sdai
    brw-rw----. 1 carrotom    carrotom     66,  48 Sep 27 16:40 sdaj
    brw-rw----. 1 carrotom    carrotom     66,  64 Sep 27 16:40 sdak
    ```

    Observation: In a resource pool deployment, the shared disks should be owned by the database runtime user (`carrotom`). However, the first device (`sdah`) is owned by `root`, indicating an ownership anomaly.

## Root Cause

The database does not typically modify shared disk permissions autonomously. Such anomalies are commonly attributed to administrator misoperation or unauthorized modifications by external processes.

## Solution

Restore normal database operation by correcting the ownership and permissions of the shared disks using the `chown` or `chmod` commands.
