# Installation Failure Due to Using Default Parameters During Pre-Installation

## Symptom

1. If `dataPortBase` and `cmServerPortBase` are not explicitly configured, the installation completes but the database fails to start when the default ports are occupied. The following are the startup failure message and the `cm_agent` log entries indicating that the default CM port is occupied.

Startup failure message:

```shell
Failed to start cluster  in (300)s.
It will continue to start in the background.
If you want to see the cluster status, please try command gs_om -t status.
If you want to stop the cluster, please try command gs_om -t stop.
```

`cm_agent` error log:

```
ERROR: 345: connect to cm_server failed, host=20.20.20.135 port=5000 localhost=20.20.20.135 connect_timeout=1 node_id=1 node_name=openGauss135 remote_type=7. could not connect to server:
        TCP/IP connections on port 5000?
```

2. If `cmDir` is not configured, the following error message appears during installation:

```
[GAUSS-50201] : The cm data directory [/cm_server] does not exist.
```

## Troubleshooting Method

During the pre-installation phase, ports and database directories are configured based on the XML file. If the database fails to start after installation, inspect the startup log (`$PGDATA/DBStart.log`) and the CM logs (`$GAUSSLOG/cm/cm_agent`).

- For port conflicts, use the `netstat -nap | grep <port>` or `lsof -i:<port>` command to verify whether `dataPortBase` or `cmServerPortBase` is occupied.
- If `cmDir` is missing, modify the XML configuration file according to the error message.

## Root Cause

1. When using default ports, there is a risk that the ports are already occupied by other processes. If the port is occupied, the database fails to start after installation.
2. During CM instance initialization, the system validates the existence of `cmDir`. If the directory does not exist, the installer throws an error and aborts the installation.

## Solution

1. Modify `dataPortBase` and `cmServerPortBase` to unused port numbers, then re-run the pre-installation and installation procedures.
2. Configure the correct `cmDir` path in the XML file, then re-run the installation.
