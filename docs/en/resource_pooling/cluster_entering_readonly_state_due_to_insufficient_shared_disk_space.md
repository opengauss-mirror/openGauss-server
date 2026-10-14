# Cluster Entering ReadOnly State Due to Insufficient Shared Drive Space

## Symptom

During the operation of an openGauss resource pool deployment, the following conditions may occur:

1. Executing `cm_ctl query -Cvipdw` reveals that all nodes are in either `Primary ReadOnly` or `Standby ReadOnly` state.
2. During normal business workflow execution, the following error is reported:

    ```sql
    ERROR:  cannot execute INSERT in a read-only transaction
    ```

These states indicate that the cluster has entered the `ReadOnly` state, rendering write operations unavailable.

## Troubleshooting Method

The manifestation and troubleshooting procedure for this issue are as follows:

1. During write operations on the cluster, a business error is encountered:

    ```sql
    ERROR:  cannot execute INSERT in a read-only transaction
    ```

    This error indicates that write operations cannot be performed within a read-only transaction, confirming the cluster is in a read-only state.

2. Inspect the most recent log files within the `$GAUSSLOG/pg_log` directory. The following entries will be present:

    ```shell
    2024-10-08 16:46:45.522 6704f169.5069 test 281439387299744 gsql 0 dn_6001_6002_6003 25006  562949953464622 [BACKEND] ERROR:  cannot execute INSERT in a read-only transaction
    2024-10-08 16:46:45.522 6704f169.5069 test 281439387299744 gsql 0 dn_6001_6002_6003 25006  562949953464622 [BACKEND] CONTEXT:  SQL statement "INSERT INTO user_logs (user_data)"
    ```

    These logs corroborate that write operations failed due to the read-only transaction constraint.

3. Execute the `dsscmd lsvg -m G` command to inspect shared drive capacity. The output will resemble the following:

    ```shell
    vg_name       volume_count        size                 free                 used                 percent(%)          
    data          1                   30.00000             2.43750              27.56250             91.88               
    log           1                   50.00000             23.75000             26.25000             52.50   
    ```

    It can be seen that the usage percentage of the shared `data` volume reaches `91%`. Since this exceeds the default alarm threshold of `85%`, the cluster transitions into the `ReadOnly` state.

## Root Cause

By default, the cluster enforces a policy requiring shared drive usage to remain below `85%` to permit write operations. Consequently, when the available shared drive capacity falls below `15%`, the cluster automatically enters the `ReadOnly` state to protect data integrity, thereby blocking all write traffic.

## Solution

The following remediation measures are available:

1. **Reinstallation:** Re-deploy the cluster using a larger shared drive to prevent capacity exhaustion.
2. **Volume Expansion:** Expand the shared storage volume using the `dsscmd adv` command:

    ```shell
    dsscmd adv <-g vg_name> <-v vol_path> [-f] [-D DSS_HOME] [-U UDS:socket_domain] 
    ```

    The issue will be resolved upon successful expansion.

3. **Threshold Adjustment:** Reconfigure the `datastorage_threshold_value_check` parameter to adjust the capacity threshold using `cm_ctl`:

    ```shell
    cm_ctl set --param --server -k datastorage_threshold_value_check=xx
    cm_ctl reload --param --server
    ```

    After applying the new threshold, the cluster will revert to a normal operational state.