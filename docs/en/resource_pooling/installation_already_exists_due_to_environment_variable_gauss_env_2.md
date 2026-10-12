# Installation Already Exists Due to Environment Variable GAUSS_ENV=2

## Symptom

The following error occurs when executing the `gs_install` command:

```shell
[GAUSS-51806] : The cluster has been installed.
```

## Troubleshooting Method

1. First, check whether any database processes are running in the current environment by using the `ps ux` command. Look for processes such as `gaussdb`, `dssserver`, `cm_agent`, `cm_server`, and `om_monitor`. If these processes exist, uninstall the database using either the `gs_uninstall` command or the `kill -9` command. If no database processes are found, further check whether the environment variable `GAUSS_ENV` is set to `2`.

## Root Cause

The environment variable `GAUSS_ENV` indicates the installation state: `GAUSS_ENV=1` indicates that pre-installation was successful. `GAUSS_ENV=2` indicates that installation was successful. If a previously installed database was not properly uninstalled, the `GAUSS_ENV=2` setting persists. Consequently, executing the installation command again results in a conflict.

## Solution

Clear the environment variable using the following command, then re-run the pre-installation and installation commands:

```shell
unset GAUSS_ENV
```
