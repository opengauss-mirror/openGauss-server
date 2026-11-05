# Failure to Set Parameters Due to Abnormal Hostname File Permissions

## Symptom

After successfully installing a resource pool environment, parameter setting fails on some nodes:

``` shell
    Total nodes: 3. Failed nodes: 2.
    Failed node names:
        [openGaussxxx]
        [openGaussxxx]
    ALL: Failure to perform gs_guc!
```

Additionally, attempting to connect to other nodes via SSH using the hostname under this user account results in immediate failure.

## Troubleshooting Method

The primary cause of this issue is an abnormal permission setting or missing `/etc/hostname` file, which disrupts tools that rely on SSH. Therefore, navigate to the `/etc` directory and verify whether the `hostname` file exists and whether its permissions are correct. The expected status is:

```shell
    -rw-r--r--. 1 root root  13 Nov 22 15:33 /etc/hostname
```

If the file is missing or the permissions are incorrect, recreate the file and reset the permissions accordingly.

## Root Cause

The `gs_guc` tool relies on SSH to set parameters across nodes; therefore, SSH functionality must be available and mutual trust must be established successfully. When the `hostname` file is missing or has incorrect permissions, users cannot establish trusted connections to target hosts via SSH, resulting in multi-node parameter configuration failures.

## Solution

First, verify that the `hostname` file exists and that its permissions are correct. Once confirmed, clear the SSH mutual trust information located in the `~/.ssh` directory of the database user on all three nodes. Then, use the `gs_sshexkey` tool located in the `scripts` directory of the installation package to re-establish mutual trust.
