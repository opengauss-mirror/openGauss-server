# Uninstallation Failure Due to Lack of Permissions for Some Directories

## Symptom

The following error message is reported during database uninstallation:

```shell
[GAUSS-50315] : The user xxx is not matched with the owner of /usr1/xxx/install/app.
```

## Troubleshooting Method

Based on the error message, check whether the directory permissions belong to the current user.

## Root Cause

The directory permissions do not belong to the current user.

## Solution

Use the `chown` command to restore the directory ownership to the current user, then re-execute the uninstallation.