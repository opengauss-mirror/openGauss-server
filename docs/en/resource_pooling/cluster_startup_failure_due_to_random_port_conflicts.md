# Cluster Startup Failure Due to Random Port Conflicts

## Symptom

This section describes the issue of cluster startup or restart failure caused by port conflicts, using a resource pool deployment with one primary and one standby node as an example.

The primary node IP is aaa.aaa.aaa.aaa, the standby node IP is bbb.bbb.bbb.bbb, and the port number is xxx.

1. The openGauss resource pool deployment cluster starts or restarts successfully, but executing `cm_ctl query -Cvipd` reveals that the `state` of a node in the `CMServer State` section is `Down`.

2. The openGauss resource pool deployment cluster fails to start or restart, reporting the following error message:

    ```shell
    cm_ctl: start cluster failed in (601)s!

    HINT: Maybe the cluster is continually being started in the background.
    You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
    ```

    The report indicates cluster startup failure with a 10-minute timeout.

Additionally, the following error can be observed in the `$DSS_HOME/log/run/dss_instance.log` file:

    ```shell
    [mes]:Start tcp lsnr failed. Host_name: xxx.xxx.xxx.xxx, inst_id:0, port:xxxx, os error:98.
    ```

    This message indicates a port conflict, resulting in TCP connection creation failure.

## Troubleshooting Method

1. Based on the log files described in the Symptom section, determine whether TCP establishment failure due to port conflicts has occurred.

2. Use the conflicting port number and execute the `lsof` command to check whether any other process is currently using that port.

3. If no other process is using the port, execute the following command to check the system ephemeral port range. Then, cross-reference the XML configuration file to verify whether the installed ports fall within this range:

    ```bash
    cat /proc/sys/net/ipv4/ip_local_port_range
    ```

4. If the installed ports fall within the ephemeral port range, the issue can be identified as a random port conflict among pool-internal components.

## Root Cause

The ports used by certain components are specified in two ways:

1. Ports specified in the XML configuration file during installation. The usage ports for each component thread are allocated as offsets from the specified base port.
2. For certain worker threads, ports are randomly selected from the free port range defined by `/proc/sys/net/ipv4/ip_local_port_range`.

As a result, both methods may attempt to occupy the same port, causing a port conflict that leads to TCP connection failure.

## Solution

To resolve this issue, ensure that the ports specified in the XML configuration file do not fall within the free port range defined by `/proc/sys/net/ipv4/ip_local_port_range`. Users must verify this during the installation phase.
