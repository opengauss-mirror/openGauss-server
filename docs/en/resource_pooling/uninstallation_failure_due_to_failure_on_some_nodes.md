# Uninstallation Failure Due to Errors on Some Nodes

## Symptom

Running the `gs_uninstall --delete-data` command to uninstall the database fails on some nodes. The error message is as follows:

```shell
Checking uninstallation.
[GAUSS-51400] : Failed to execute the command: python3 'dbuser/script/local/CheckUninstall.py' -R $GAUSSHOME -U user -l gs_local.log -d. Result:{'openGaussxxx': 'Failure', 'openGaussxxx': 'Success', 'openGaussxxx': 'Success'}.
Error:
[FAILURE] openGaussxxx:
[SUCCESS] openGaussxxx:
Checking OS version.
Successfully checked OS version.
Skipping user check.
Checking installation path.
Successfully checked installation path.
[SUCCESS] openGaussxxx:
Checking OS version.
Successfully checked OS version.
Skipping user check.
Checking installation path.
Successfully checked installation path.
```

## Troubleshooting Method

Based on the error message, log in to the node where uninstallation failed and execute the relevant command to check the specific error details.

## Root Cause

If uninstallation succeeds on some nodes but fails on others, the cause is likely a breakdown in **mutual trust** between nodes. Check whether the `/etc/hosts` file on the failing node is missing mutual trust information for the cluster nodes.

## Solution

1. Execute the `gs_uninstall --delete-data -L` command on each node separately.
2. Restore the mutual trust information as shown in the figure below, then re-execute the `gs_uninstall --delete-data` command.
![!\[Alt text\](image-7.png)](image-6.png)
