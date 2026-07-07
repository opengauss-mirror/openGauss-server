# Determining the Scope of Performance Tuning<a name="EN-US_TOPIC_0289900998"></a>

Database performance tuning often happens when users are not satisfied with the service execution efficiency and want to improve the efficiency. The database performance is affected by many factors as described in section  [Performance Elements](#en-us_topic_0283136943_en-us_topic_0237121484_en-us_topic_0073259659_en-us_topic_0040046511_section218827915473). Therefore, performance tuning is a complex process and sometimes cannot be systematically described or explained. It depends more on the database administrator's experience. However, this section still attempts to illustrate the performance tuning methods that can be referred to by application development personnel and new openGauss administrators.

## Performance Elements<a name="en-us_topic_0283136943_en-us_topic_0237121484_en-us_topic_0073259659_en-us_topic_0040046511_section218827915473"></a>

There are multiple performance factors that affect the database performance. Knowing these factors can help you identify and analyze performance-associated issues.

- System resources

    Database performance greatly relies on disk I/O and memory usage. To accurately set performance counters, you need to have a knowledge of the basic performance of the hardware deployed in openGauss. Performance of hardware, such as the CPU, hard disk, disk controller, memory, and network interfaces, greatly affects database running speed.

- Load

    The load indicates the total database system demands and it changes over time. The overall load contains user queries, applications, concurrent jobs, transactions, and system commands transferred at any time. For example, the system load increases if multiple users are executing multiple queries. The load will significantly affect the database performance. Identifying load peak hours helps improve resource utilization so that tasks are executed effectively.

- Throughput

    The data processing capability of a database is defined by its throughput. Database throughput is measured by the number of queries or processed transactions per second or by the average response time. The database processing capacity is closely related to the underlying system performance \(disk I/O, CPU speed, and storage bandwidth\). You need to know about the hardware performance before setting a target throughput.

- Competition

    Competition indicates that two or more load components try to use system resources in a conflicting way. For example, competition occurs when multiple queries attempt to update the same data at the same time, or when a large number of loads compete for system resources. When competition increases, the throughput decreases.

- Optimization

    The database optimization can affect the performance of the whole system. Before executing the SQL statements, configuring database parameters, designing tables, and performing data distribution, enable the database query optimizer can help you obtain the most efficient execution plan.

## Determining the Tuning Scope<a name="en-us_topic_0283136943_en-us_topic_0237121484_en-us_topic_0073259659_section6664793616450"></a>

Performance tuning depends on the usage of hardware resources, such as the CPU, memory, I/O, and network of each node in openGauss. Check whether these resources are fully utilized, and whether any bottlenecks exist, and then perform performance tuning as required.

- If a resource reaches the bottleneck:
    1. Check whether the key OS parameters and database parameters are properly set and perform  [System Optimization](optimizing_os_parameters.md).
    2. Find the resource consuming SQL statements by querying the most time-consuming SQL statements and unresponsive SQL statements, and then perform  [SQL Optimization](sql_optimization.md).
- If no resource reaches the bottleneck, the system performance can be improved. In this case, query the most time-consuming SQL statements and the unresponsive SQL statements, and then perform  [SQL Optimization](sql_optimization.md)  as required.
- **[Performance Logs](performance_logs.md)** 
- **[Analyzing Hardware Bottlenecks](cpu.md)**  
The CPU, memory, I/O, and network resource usage of each node in openGauss are obtained to check whether these resources are fully used and whether any bottleneck exists.
- **[Querying SQL Statements That Affect Performance Most](querying_sql_statements_that_affect_performance_most.md)**  
- **[Checking Blocked Statements](checking_blocked_statements.md)**  
