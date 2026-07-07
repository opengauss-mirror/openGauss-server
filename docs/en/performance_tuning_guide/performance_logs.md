# Performance Logs<a name="EN-US_TOPIC_0286058590"></a>

Performance logs focus on the access performance of external resources.

Performance logs are used to record the status of physical resources and the performance of access to external resources \(such as disks and OBS\). openGauss does not support OBS and Hadoop. Therefore, only the information of access to disks is available.

The information is mainly collected during the I/O read and write of disk files. For example, the file read I/O during file copy, and the pread system calling for accessing the OS table file during normal SQL execution.

When a performance issue occurs, you can locate the cause using performance logs, which greatly improves troubleshooting efficiency.

- Log Storage Directory

    The performance logs are stored in the directories under  *$GAUSSLOG***/gs\_profile**.

- Log Naming Rules

    The log file naming convention is as follows:  **postgresql-***Creation time***.prf**

    By default, a new log file is generated at 0:00 every day, or when the latest log file exceeds 20 MB or a database instance \(CN or DN\) is restarted.

- Log Content Description

    A line in a log is arranged in the following format:  *Host name*+*Date*+*Time*+*Instance name*+*Thread number*+*Log content*

## Parameters for Collecting Performance Logs<a name="section190710281529"></a>

- **logging\_collector**: specifies whether to enable the log collection function. The default value is  **on**, indicating that the log collection function is enabled.
- **plog\_merge\_age**: specifies the period for exporting performance log data, that is, the interval for aggregating performance logs. The unit is millisecond. The default value is  **3s**.

    If  **logging\_collector**  is set to  **on**,  **plog\_merge\_age**  is set to a value greater than 0, and the host is running properly, performance data is not collected during the restoration. Use the  **gs\_log**  tool to export the  **gs\_log.cpp**  file. The  **gs\_log**  tool will be released on March 30, 2021.
