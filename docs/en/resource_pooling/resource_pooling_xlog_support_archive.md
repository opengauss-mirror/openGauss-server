# Resource Pooling XLOG Archiving Support

## Feature Overview

This feature enables XLOG archiving for single-cluster resource pooling deployments. It supports backup and recovery operations, as well as Point-in-Time Recovery (PITR) using archived XLOG files.

## Availability

This feature has been available since **openGauss 7.0.0-RC1**.

## Customer Value

This feature enhances the maintainability of single-cluster resource pooling deployments and improves overall cluster stability and reliability.

## Feature Description

In a single-cluster resource pooling scenario, once archiving parameters are enabled, XLOG files are continuously archived. In the event of a failure, administrators can use the `gs_probackup` utility along with the archived XLOG files to perform PITR recovery.

## Enhancements

None.

## Constraints

- Archiving parameters can only be enabled on the **primary node**.
- Archiving cannot be enabled when `wal_level` is set to `minimal`.
- The system does **not** validate whether archived XLOG files are still required. If the archive destination disk becomes full, archiving will halt until sufficient free space (>1 GB, as XLOG segments in resource pooling are fixed at 1 GB) is available. Users must manually manage and clean up archived XLOG files.
- In resource pooling mode, the `archive_command` parameter **must** be configured as `dsscmd cp`. Only basic file copy operations are supported, and **relative paths are not permitted** for file path arguments. For details, see [dsscmd](https://atomgit.com/opengauss/docs/blob/master/docs/en/tool_and_commandreference/dsscmd.md).

## Dependencies

None.

## Basic Principles

In a resource pooling environment, XLOG files reside on the **shared log volume**. The primary node reads XLOG segments from the shared storage and determines whether archiving is required. A log segment switch triggers the archiving workflow, which occurs under the following conditions:

1. A user explicitly invokes the `pg_switch_xlog` function to force a log segment switch.
2. A log segment reaches its maximum size during normal insertion.
3. The `archive_timeout` interval elapses without a segment switch, triggering a forced switch to ensure timely archiving.

Before archiving begins, the primary node creates a `.ready` file in the archive status directory on the shared storage to mark the XLOG segment for archival. Upon successful completion, the primary renames the `.ready` file to `.done`, indicating that the segment has been archived.

During the archiving process, the primary node uses the `dsscmd` utility to invoke DSS interfaces, copying the XLOG segment from the shared storage to a local disk on the primary host.

During PITR recovery, the primary node utilizes the continuously archived XLOG files stored on its local disk. It copies these files back to the shared storage and performs recovery by replaying the XLOG segments.

## Usage Guide

Configuration of the `archive_command` parameter differs from traditional primary-standby deployments; all other archiving parameters follow the same restrictions as traditional deployments. For general archiving configuration, see [Archiving](https://atomgit.com/opengauss/docs/blob/master/docs/en/database_reference/archiving.md).

For PITR recovery procedures, refer to the [gs_probackup](https://atomgit.com/opengauss/docs/blob/master/docs/en/tool_and_commandreference/gs_probackup.md) usage guide.
