# Database Anomaly in a One-Primary-Two-Standby Cluster After Shutting Down Two Nodes

## Symptom

In a one-primary-two-standby cluster, after shutting down two nodes using the `cm_ctl stop` command, the database enters an abnormal state. Executing `cm_ctl query -Cv` to query the cluster status fails to connect to the `cm_server`, returning the message: `cm_ctl: can't connect to cm_server`.

## Root Cause

In a one-primary-multi-standby cluster, the normal operation of cm_server relies on satisfying the majority principle, requiring at least half of the nodes' cm_server instances to be running.
 >[!NOTE]  
  >
  > - - In a one-primary-one-standby cluster, a single node (the cm_server primary node) is permitted to remain operational.
  
## Solution

Ensure that more than half of the nodes in the cluster are in a normal operational state.
