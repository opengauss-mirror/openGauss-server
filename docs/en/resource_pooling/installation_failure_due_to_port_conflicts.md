
# Installation Failure Due to Port Conflicts

## Symptom

This section describes an installation failure caused by port conflicts in a one-primary-one-standby resource pool environment.

The primary node has the IP address `A.B.C.D`, the standby node has the IP address `E.F.G.H`, and both use the same port number `xxx`. 
The following error message appears during database installation:

```shell
Checking instance port and IP
[GAUSS-50601] : The port [xxx] is occupied or the ip address is incorrectly configured.
```

## Troubleshooting Method

Use the `lsof -i:[port]` command to identify the process occupying the port. Then, modify the `xml` file to assign a non-conflicting port number.

## Root Cause

During installation, specific ports must be allocated to various components for `socket` communication. If the target port is already occupied by another process, the connection cannot be established, leading to installation failure. 
Therefore, ensure that assigned ports are exclusive to the database and do not overlap with system services or other applications.

## Solution

Identify the occupying process using the `lsof -i:[port]` command. Then, modify the corresponding port numbers in the `xml` file to resolve the conflict.

**Note:** Since the `xml` file contains multiple port configurations, ensure that the assigned port numbers are sufficiently far apart. Some components require a continuous range of ports; keeping ports too close may result in installation failure.

