# Connection Failure Due to Missing DSS Socket File

## Symptom

Existing connections continue to operate normally, while new connection attempts fail with an error message indicating a socket-related issue:

```
[carrotom@openGauss111 cluster]$ gsql -r
gsql: FATAL:  Could not connect dssserver, vgname: "+data", socketpath: "UDS:/usr3/carrotom/cluster/dss_home/.dss_unix_d_socket"
HINT:  Check vgname and socketpath and restart later.
```

## Troubleshooting Method

The error message explicitly indicates a socket file anomaly. Verify whether the corresponding file exists:

```
[carrotom@openGauss111 carrotom_mppdb]$ ll /usr3/carrotom/cluster/dss_home/.dss_unix_d_socket
```

If the file is missing, the output will indicate that the file or directory does not exist.

## Root Cause

Common causes for socket file loss include, but are not limited to:

- System errors
- Insufficient disk space
- Lack of access permissions for the directory housing the socket file
- Misoperation by the database administrator
- Malware or unauthorized intrusion
- Other unforeseen system anomalies

## Solution

If the socket file is missing, restart the affected DSS node or the entire cluster to restore normal operation.

This operation has no other adverse effects on the database.
