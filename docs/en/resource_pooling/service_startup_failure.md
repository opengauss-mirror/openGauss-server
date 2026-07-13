# Service Startup Failure<a name="EN-US_TOPIC_0291613863"></a>

## Symptom<a name="section385257175213"></a>

The service startup failed.

## Cause Analysis<a name="section16300111295211"></a>

- Parameters are set to improper values, resulting in insufficient system resources in the database, or parameter settings do not meet the internal restrictions in the database.
- Some DNs are abnormal.
- Permissions to modify directories are insufficient. For example, users do not have sufficient permissions for the  **/tmp**  directory or the data directory in the database.
- The configured port has been occupied.
- The system firewall is enabled.
- The trust relationship between servers of the database is abnormal.
- The database control file is damaged.

## Procedure<a name="section7637151695218"></a>

- Check whether the parameter configurations are improper or meet internal constraints.

    - Log in to the node that cannot be started. Check the run logs and check whether the resources are insufficient or whether the parameter configurations meet internal constraints. For example, if the message "Out of memory" or the following error information is displayed, the resources are insufficient, the startup fails, or the configuration parameters do not meet the internal constraints.

        ```
        FATAL: hot standby is not possible because max_connections = 10 is a lower setting than on the master server (its value was 100)
        ```

    - Check whether the GUC parameters are set to proper values. For example, check parameters, such as **shared\_buffers**, **effective\_cache\_size**, and **bulk\_write\_ring\_size** that consume much resources, or parameter  **max\_connections**  that cannot be easily set to a value that is less than its last value. 

- Check whether some DNs are abnormal. Check the status of each primary and standby instance in the current database using  **gs\_om -t status --detail**.

    - If the status of all the instances on a host is abnormal, replace the host.

    - If the status of an instance is  **Unknown**,  **Pending**, or  **Down**, log in to the node where the instance resides as a database user to view the instance log and identify the cause. For example:

        ```
        2014-11-27 14:10:07.022 CST 140720185366288 FATAL:  database "postgres" does not exist 2014-11-27 14:10:07.022 CST 140720185366288 DETAIL:  The database subdirectory "base/ 13252" is missing.
        ```

        If the preceding information is displayed in a log, files stored in the data directory where the DN resides are damaged, and the instance cannot be queried. You cannot execute normal queries to this instance.

- Check whether users have sufficient directory permissions. For example, users do not have sufficient permissions for the  **/tmp**  directory or the data directory in the database.

    - Determine the directory for which users have insufficient permissions.

    - Run the  **chmod**  command to modify directory permissions as required. The database user must have read/write permissions for the  **/tmp**  directory. To modify permissions for data directories, refer to the settings for data directories with sufficient permissions.

- Check whether the configured ports have been occupied.

    - Log in to the node that cannot be started and check whether the instance process exists.

    - If the instance process does not exist, view the instance log to check the exception reasons. For example:

        ```
        2014-10-17 19:38:23.637 CST 139875904172320 LOG:  could not bind IPv4 socket at the 0 time: Address already in use 2014-10-17 19:38:23.637 CST 139875904172320 HINT:  Is another postmaster already running on port 40005? If not, wait a few seconds and retry.
        ```

        If the preceding information is displayed in a log, the TCP port on the DN has been occupied, and the instance cannot be started.

        ```
        2015-06-10 10:01:50 CST 140329975478400 [SCTP MODE] WARNING: (sctp bind)         bind(socket=9, [addr:0.0.0.0,port:1024]):Address already in use  --  attempt 10/10 2015-06-10 10:01:50 CST 140329975478400 [SCTP MODE] ERROR: (sctp bind)   Maximum bind() attempts. Die now...
        ```

        If the preceding information is displayed in a log, the SCTP port on the DN has been occupied, and the instance cannot be started.

- Run  **sysctl -a**  to view the  **net.ipv4.ip\_local\_port\_range**  parameter. If this port configured for this instance is within the range of the port number randomly occupied by the system, modify the value of  **net.ipv4.ip\_local\_port\_range**, ensuring that all the instance port numbers in the XML file are beyond this range. Check whether a port has been occupied:

    ```
    netstat -anop | grep Port number
    ```

    The following is an example:

    ```
    [root@openGauss36 ~]# netstat -anop | grep 15970
    tcp        0      0 127.0.0.1:15970         0.0.0.0:*               LISTEN      3920251/gaussdb      off (0.00/0/0)
    tcp6       0      0 ::1:15970               :::*                    LISTEN      3920251/gaussdb      off (0.00/0/0)
    unix  2      [ ACC ]     STREAM     LISTENING     197399441 3920251/gaussdb      /tmp/.s.PGSQL.15970
    unix  3      [ ]         STREAM     CONNECTED     197461142 3920251/gaussdb      /tmp/.s.PGSQL.15970
    ```

- Check whether the system firewall is enabled.

- Check whether the mutual trust relationship is abnormal. Reconfigure the mutual trust relationship between servers in the database.

- Check whether the database control file, for example,  **gaussdb.state**, is damaged or cleared. If the control file on the primary node is damaged, a failover can be triggered on the standby node, and then the original primary node can be restored by rebuilding. If the control file of the standby node is damaged, you can restore the standby node by rebuilding the control file.
