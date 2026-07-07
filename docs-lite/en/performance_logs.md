# Performance Logs<a name="EN-US_TOPIC_0289900271"></a>

Performance logs focus on the access performance of external resources.

Performance logs are used to record the status of physical resources and the performance of access to external resources \(such as disks and OBS\). openGauss does not support OBS or Hadoop, and as a result only disk access information is available.

The information is mainly collected during the I/O read and write of disk files. For example, the file read I/O during file copy, and the pread system calling for accessing the OS table file during normal SQL execution.

When a performance issue occurs, you can locate the cause using performance logs, which greatly improves troubleshooting efficiency.

- Log storage directory

    The performance logs are stored in the directories under  *$GAUSSLOG***/gs\_profile**.

- Log naming rules

    The log file naming convention is as follows:  **postgresql-***Creation time***.prf**

    By default, a new log file is generated at 0:00 every day, or when the latest log file exceeds 20 MB or a database instance \(DN\) is restarted.

- Log content description

    A line in a log is arranged in the following format:  *Host name*+*Date*+*Time*+*Instance name*+*Thread number*+*Log content*

## Parameters for Collecting Performance Logs<a name="en-us_topic_0286058590_section190710281529"></a>

- **logging\_collector**: specifies whether to enable the log collection function. The default value is  **on**, indicating that the log collection function is enabled.
- **plog\_merge\_age**: specifies the period for exporting performance log data, that is, the interval for aggregating performance logs. The unit is millisecond. The default value is  **3s**.

    If  **logging\_collector**  is set to  **on**,  **plog\_merge\_age**  is set to a value greater than 0. When the host is running properly, performance data is not collected during the restoration.
