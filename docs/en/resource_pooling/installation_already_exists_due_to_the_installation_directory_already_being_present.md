# Installation Already Exists Due to an Existing Installation Directory

## Symptom

The following error occurs when executing the `gs_install` command:

```shell
[GAUSS-50202] : The dbuser/install/app_206d898b must be empty.
```

## Troubleshooting Method

Check whether the target directory is empty according to the error message.

## Root Cause

The previous database uninstallation did not follow the standard procedure, leaving residual files in the installation directory. This prevents a new installation from proceeding.

## Solution

Delete the installation directory `/xxx/install/`, then re-run the pre-installation and installation commands.
