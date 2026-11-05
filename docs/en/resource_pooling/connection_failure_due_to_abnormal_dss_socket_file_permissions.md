# Connection Failure Due to Abnormal DSS Socket File Permissions

## Symptom

Existing connections continue to operate normally, while new connection attempts fail with an error message indicating a socket-related issue:

```shell
[carrotom@openGauss111 cluster]$ gsql -r
gsql: FATAL:  Could not connect dssserver, vgname: "+data", socketpath: "UDS:/usr3/carrotom/cluster/dss_home/.dss_unix_d_socket"
HINT:  Check vgname and socketpath and restart later.
```

## Troubleshooting Method

The error message explicitly indicates an anomaly with the socket file. Inspect the corresponding file to verify whether its access permissions are normal:

```shell
[carrotom@openGauss111 carrotom_mppdb]$ ll /usr3/carrotom/cluster/dss_home/.dss_unix_d_socket
s---------. 1 carrotom carrotom 0 Sep 27 15:18 /usr3/carrotom/cluster/dss_home/.dss_unix_d_socket
```

Observation: The socket file exhibits abnormal permissions (access mode `0`), rendering it inaccessible.

## Root Cause

Common causes for socket file anomalies include, but are not limited to:

-   System errors
-   Insufficient disk space
-   Lack of access permissions for the directory housing the socket file
-   Misoperation by the database administrator
-   Malware or unauthorized intrusion
-   Other unforeseen system anomalies

## Solution

Rectify the issue by modifying the ownership and permissions of the socket file using the `chown` and `chmod` commands. This operation has no other adverse effects on the database.
