# Installation Failure Due to Excessively Small Limit Configuration

## Symptom

Node startup fails. During OM installation, the cluster fails to start at the final step, and the `om_monitor`, `cm_server`, and `cm_agent` processes are absent.

## Troubleshooting Method

1. First, check whether the `om_monitor`, `cm_server`, and `cm_agent` processes exist. Navigate to the `$GAUSSLOG/cm` directory and check whether new CM logs have been generated. If the processes do not exist, no logs will be generated.

2. Inspect the `om_monitor` logs in the `$GAUSSLOG/cm` directory. Look for error messages similar to `'limit value is less than 640000'`. If such a message appears, the failure is caused by insufficient limit settings.

## Root Cause

The `limits.conf` file is a Linux configuration file used to set resource limits for users and user groups. It is typically located at `/etc/security/limits.conf`. The basic format of the `limits.conf` file is as follows:

    ```shell
    <domain> <type> <item> <value>
    
    <domain>: a username, a group name (prefixed with @), or * (all users).
    <type>: hard (a rigid limit that the user cannot exceed) or soft (a flexible limit that the user can adjust within a certain range).
    <item>: type of resource to limit, such as nproc (number of processes), nofile (number of open files), memlock (locked memory), etc.
    <value>: specific value of the limit.
        ```

   The following are sample configurations: 

    ```
    1. Limit user alice to a maximum of 100 processes
    alice hard nproc 100

    2. Limit all users to a maximum of 1024 open files (soft) and 2048 (hard)
    * soft nofile 1024
    * hard nofile 2048

    3. Limit user group staff to a maximum memory usage of 1 GB
    @staff hard as 1048576
    ```

## Solution

Configure the `/etc/security/limits.conf` file appropriately based on the requirements described above.

    **Note:** After modifying the `limits.conf` file, log out and log back in, or restart the relevant services for the changes to take effect, and then retry starting the cluster or re-running the installation.
