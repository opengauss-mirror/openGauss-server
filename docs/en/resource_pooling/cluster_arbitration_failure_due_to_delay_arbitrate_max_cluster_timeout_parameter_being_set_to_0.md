# Cluster Arbitration Failure Due to delay_arbitrate_max_cluster_timeout Parameter Being Set to 0

## Symptom

This issue presents no obvious symptoms. Generally, log analysis is required to retrospectively verify whether cluster arbitration has failed. The typical indicator is observing a significant fault on a specific node, yet failing to find the corresponding `kick out` entry in the `key-event` logs located in `$GAUSSLOG/cm/cm_server`. If the faulty node is not logged as being kicked out, cluster arbitration is suspected to have failed.

## Troubleshooting Method

1. Confirm whether the arbitration thread is functioning normally. Under normal operation, the arbitration thread periodically prints heartbeat matrices to the `cm_server` log in `$GAUSSLOG/cm/cm_server`, as shown below:

    ```shell
    2024-12-20 02:29:09.720 tid=309102 MaxClusterAb LOG: Network base_time:2024-12-20 02:29:09
    2024-12-20 02:29:09.720 tid=309102 MaxClusterAb LOG: [RHB] hb infos: |1970-01-01 08:00:00|2024-12-20 02:29:08|2024-12-20 02:29:08|
    2024-12-20 02:29:09.720 tid=309102 MaxClusterAb LOG: [RHB] hb infos: |2024-12-20 02:29:08|1970-01-01 08:00:00|2024-12-20 02:29:08|
    2024-12-20 02:29:09.720 tid=309102 MaxClusterAb LOG: [RHB] hb infos: |2024-12-20 02:29:09|2024-12-20 02:29:09|1970-01-01 08:00:00|
    ```

2. If the aforementioned matrix logs are absent for an extended period, the arbitration thread is likely inactive. While inactivity is often caused by the `delay_arbitrate_max_cluster_timeout` parameter, first verify that the arbitration thread has not exited. Search the `cm_server` log in `$GAUSSLOG/cm/cm_server` for the keyword `MaxNodeClusterArbitrateMain will exit`. If this keyword exists and is not followed by `MaxNodeClusterArbitrateMain will start`, the arbitration thread has exited and failed to restart. However, it is rare for the arbitration thread to exit independently while the `cm_server` process remains active.

    After ruling out thread exit, query the parameter value using the following command:

    ```shell
    cm_ctl list --param --server | grep 'delay_arbitrate_max_cluster_timeout'
    ```

    If `delay_arbitrate_max_cluster_timeout` is set to `0`, arbitration is effectively disabled.

## Root Cause

The `delay_arbitrate_max_cluster_timeout` parameter defines the maximum duration (in seconds) to delay cluster arbitration. The valid range is `0` to `1000`, with a default value of `300`. A value of `0` explicitly disables the arbitration mechanism.

## Solution

Re-enable cluster arbitration by setting the `delay_arbitrate_max_cluster_timeout` parameter to a valid value (e.g., the default `300`). The delay duration can be adjusted based on operational requirements:

```shell
cm_ctl set --param --server -k 'delay_arbitrate_max_cluster_timeout=300'
```
