# Resource Pooling Active-Standby Dual-Cluster Disaster Recovery

## Feature Overview

This feature implements XLog synchronization between the primary and standby clusters, ensuring XLog consistency across the two clusters. It enhances disaster recovery (DR) capabilities, reduces storage overhead, and guarantees proper functionality for both intra-cluster and inter-cluster switchovers.

## Availability

- **Streaming-based dual-cluster DR** has been available since **openGauss 6.0.0-RC1**.
- **DORADO storage synchronous replication-based dual-cluster DR** has been available since **openGauss 5.1.0**.

## Customer Value

This feature provides data security and service availability under extreme disaster conditions. Multiple synchronization modes are supported to address diverse deployment scenarios.

## Feature Description

Resource pooling dual-cluster DR synchronizes XLogs from the primary cluster to the standby cluster through multiple synchronization methods. The standby cluster replays these XLogs to achieve data consistency, enabling rapid takeover of services when the primary cluster suffers a catastrophic failure.

Two synchronization modes are currently supported:
- **Network-based streaming replication**
- **DORADO storage-based synchronous replication**

## Feature Enhancements

### 1. Ultra-Low RTO Between Clusters

Achieves an inter-cluster RTO of less than **20 seconds** under a **300,000 tpmC** workload.

**Constraints:**
- Introduced in **openGauss 6.0.0-RC1**.
- Applicable only to **DORADO storage synchronous replication** deployments in resource pooling mode.
- Applicable only to **primary-standby cluster switchovers**, not intra-cluster role changes.
- When ultra-low RTO is enabled, the standby cluster **does not support read services**.

**Usage Instructions:**
- Configure ultra-low RTO parameters on the standby cluster and disable hot standby (read-only queries).
- Promote the standby cluster to primary in the event of a primary cluster failure.
- Under a 300,000 tpmC workload, the inter-cluster RTO remains below 20 seconds.

### 2. Ultra-Low RTO with Standby Readability (BETA)

**Constraints:**
- Introduced in **openGauss 7.0.0-RC1**.
- Applicable only to **resource pooling dual-cluster** deployments.
- When ultra-low RTO is enabled:
  - **Switchover** is not supported.
  - Cascaded standbys do not support read services.

**Usage Instructions:**
- Configure ultra-low RTO parameters on the standby cluster and enable `hot_standby` to allow queries on the primary standby node.
- After a cascaded standby is promoted to primary standby via failover, it becomes readable.
- Once the standby cluster is fully promoted to primary, all nodes support read services.

### 3. Primary Cluster On-Demand Replay

**Constraints:**
- Introduced in **openGauss 7.0.0-RC2**. Applicable only to the **primary cluster** in resource pooling deployments.
- Requires `hot_standby=off` because the ultra-low RTO standby readability feature is still in BETA.

**Usage Instructions:**
- When enabled on the primary cluster in a resource pooling deployment, the feature functions normally.
- Enabling this feature on the standby cluster has **no effect**.

## Feature Constraints

- **Concurrent usage of both dual-cluster modes (streaming and DORADO) is not supported.**

## Dependencies

- This feature depends on the openGauss resource pooling architecture.
- This feature depends on Huawei DORADO storage arrays.

## Basic Principles

Dual-cluster deployment utilizes the initialization and `build` functions to install and configure a functional dual-cluster system. The `build` function ensures initial data consistency between the primary and standby clusters.

- The primary cluster host starts as **primary**.
- The standby cluster host starts as a **traditional standby integrated with DMS and DSS**.
- Other nodes start as **normal**.

During normal operation, after the standby cluster host replays its local XLogs, it requests XLogs from the primary cluster host and replays them to maintain data consistency.

## Usage Guide

This feature supports two deployment modes:

1. **Network-based streaming replication mode**:  
   [Resource Pooling Network Dual-Cluster Deployment](https://atomgit.com/opengauss/docs/blob/master/docs/en/database_administration_guide/resource_pooling_network_dual_cluster_deployent.md)
2. **DORADO storage synchronous replication mode**:  
   [Resource Pooling DORADO Dual-Cluster Deployment](https://atomgit.com/opengauss/docs/blob/master/docs/en/database_administration_guide/resource_pooling_dorado_dual_cluster_deployment.md)

## Application Scenarios

- **High availability under extreme site failures**:  
  In single-cluster deployments, an extreme data center failure may result in prolonged downtime. In a dual-cluster deployment, the standby cluster can be quickly promoted to primary to take over services, ensuring business continuity. During normal primary cluster operation, the standby cluster can also serve as a compute offload target.

- **Streaming-based dual-cluster DR**:  
  Leveraging openGauss's native XLog physical replication, this mode offers flexible configuration of primary-standby replication relationships. It is suitable for **metro-area or geo-disaster recovery** scenarios.

- **DORADO-based dual-cluster DR**:  
  Utilizing DORADO storage synchronous replication, XLogs written on the primary cluster are immediately available on the standby cluster under normal link conditions. This mode is ideal for **metro-active dual-center deployments within 100 km**. It delivers high transmission speeds and, under typical workloads, achieves **RPO = 0** and **RTO < 30s** in the event of a full cluster failure.
