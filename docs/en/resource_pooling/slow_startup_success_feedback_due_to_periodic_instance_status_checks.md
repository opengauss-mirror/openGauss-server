# Slow Startup Success Feedback Due to Periodic Instance Status Checks

## Symptom

1. Starting a single node using a command such as `cm_ctl start -n 1` takes an excessively long time to return success.

    ```shell
    cm_ctl start -n 1
    cm_ctl: start the node:1. 
    ............................................................
    ```

2. Checking the cluster status from another session shows the node is already `Normal`, but the startup command has not yet returned to the foreground.

## Root Cause

CM performs a final status check before reporting success to ensure the database is fully ready. This validation prevents false success reports when the instance is not yet available, which causes a slight delay in the command response.

## Solution

1. If `cm_ctl query` shows all nodes are `Normal`, the cluster is usable even if the start command is still waiting.
2. Future versions will optimize the check logic to reduce this delay.