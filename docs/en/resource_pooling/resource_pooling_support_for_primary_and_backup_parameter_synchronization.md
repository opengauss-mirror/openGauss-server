# Resource Pooling Support for Primary-Backup Parameter Synchronization

## Availability<a name="section15406143204715"></a>

This feature has been available since **openGauss 7.0.0-RC1**.

## Feature Overview<a name="section740615433477"></a>

This feature enables automatic synchronization of configuration parameters between the primary and standby nodes in a resource pooling environment.

## Customer Value<a name="section13406743164715"></a>

In resource pooling scenarios, modifying the GUC configuration file on the primary node automatically propagates changes to the standby nodes without manual intervention. Additionally, newly scaled-out nodes automatically receive the synchronized configuration upon startup, significantly simplifying cluster maintenance.

## Feature Description<a name="section16406154310471"></a>

This feature supports the synchronization of two configuration files: `postgresql.conf` and `pg_hba.conf`. Upon successful synchronization, the system automatically performs a **SIGHUP-level reload** to apply the new parameters. Synchronization can be triggered in two ways:
- **Periodically**: The system automatically enters the synchronization workflow every **60 minutes**.
- **On-demand**: Synchronization is triggered when the configuration is reloaded (e.g., via `cm_ctl reload` or `ALTER SYSTEM`).

## Enhancements<a name="section1340684315478"></a>

None.

## Constraints<a name="section06531946143616"></a>

- **Reserved Parameters**: Certain GUC parameters are designated as **node-reserved** and are excluded from synchronization. In addition to the reserved parameters defined for traditional primary-standby deployments, the following resource pooling-specific parameters are treated as reserved and will not be synchronized: `ss_enable_dss`, `ss_enable_dms`, `ss_enable_catalog_centralized`, `ss_instance_id`, `ss_dss_vg_name`, `ss_dss_conn_path`, `ss_rdma_work_config`, `ss_ock_log_path`, `ss_scrlock_server_port`, `ss_enable_ondemand_recovery`, `ss_enable_ondemand_realtime_build`, and `ss_disaster_mode`

- Synchronization is strictly limited to the `postgresql.conf` and `pg_hba.conf` files.
- Parameter loading adheres to standard GUC rules. **Postmaster-level** parameters require a node restart to take effect; they can be synchronized to the standby's configuration file but cannot be dynamically loaded via SIGHUP.
- This feature only supports **SIGHUP-level dynamic reloads**. For Postmaster-level parameters:
  - If modified directly in the primary's configuration file (causing inconsistency), a full cluster restart is required to synchronize the value across the cluster. The standby will load the old value until the restart.
  - If modified directly on the standby's configuration file, the new value will be loaded upon standby restart but will subsequently be overwritten by the primary's configuration during the next synchronization cycle.
- During failover or switchover, the new primary typically inherits the latest synchronized parameters. However, in extreme scenarios (e.g., the primary crashes while updating the shared configuration), the shared configuration file may not be updated. In such cases, the user must manually reconfigure the parameters on the new primary.
- Synchronizing parameters such as `ss_interconnect_channel_count` and `ss_interconnect_url` is **not supported during a cluster restart**. These parameters require consistency before node startup. Since the synchronization workflow only activates after nodes are running, inconsistencies in these parameters will prevent the node from starting. Once the cluster is running normally, these parameters can be synchronized to the configuration files (though they will not be loaded until the next restart).

## Dependencies<a name="section8406643144716"></a>

None.

## Basic Principles<a name="section8406461564716"></a>

The system maintains a **shared configuration file** on the shared storage. Only the primary node has write permission to this file; standby nodes are read-only.

1. The primary node writes the configuration content to the shared storage.
2. The primary broadcasts a synchronization message to all standby nodes.
3. Upon receiving the message, each standby reads the shared configuration file, updates its local `postgresql.conf` and `pg_hba.conf`, and notifies the postmaster to perform a SIGHUP reload.
4. Following a failover or switchover, the new primary first updates its local configuration using the shared file content, then assumes the role of the synchronization master for subsequent operations.

## Usage Guide<a name="section8445461564354"></a>

The system performs synchronization every 60 minutes by default. Users can manually trigger synchronization via the following operations:

- Starting the cluster using `cm_ctl start`.
- Reloading parameters on the primary using the `gs_guc` tool (e.g., `gs_guc reload`).
- Executing `ALTER SYSTEM SET paraname TO value;` while connected to the primary database.
- Performing a switchover or failover (role transition). In this scenario, the new primary overwrites its local configuration with the shared file, and then all standbys follow suit.


> In resource pooling mode, it is strongly discouraged to use `gs_guc set`. The `set` operation only modifies the local configuration file and does not update the shared configuration on the storage. Consequently, during a subsequent switchover or failover, the new primary will overwrite its local configuration with the outdated shared file, causing your changes to be lost.
>
> Executing `gs_guc reload` directly on a standby node in resource pooling mode will not persist changes. The synchronization workflow will immediately overwrite the standby's local configuration with the shared file, reverting any manual modifications.
