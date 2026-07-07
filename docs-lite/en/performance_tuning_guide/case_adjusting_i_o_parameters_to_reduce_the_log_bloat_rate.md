# Case: Adjusting I/O Parameters to Reduce the Log Bloat Rate<a name="EN-US_TOPIC_0000001102191372"></a>

- Parameter values before adjustment:
    - pagewriter\_sleep=2000ms
    - bgwriter\_delay=2000ms
    - max\_io\_capacity=500MB

- Parameter values after adjustment:
    - pagewriter\_sleep=100ms
    - bgwriter\_delay=1s
    - max\_io\_capacity=300MB

>[!NOTE]NOTE 
>
>- The  **max\_io\_capacity**  parameter is set to a small value because the I/O does not use the maximum value of the previous parameter. This parameter is used to limit the upper limit of the I/O usage of the backend write process.
>- Log recycling is triggered only when the number of logs reaches a certain value. The formula for calculating the value is as follows: Value of  **wal\_keep\_segments**  + Value of  **checkpoint\_segments**  x 2 + 1. If  **checkpoint\_segments**  is set to  **128**  and  **wal\_keep\_segments**  is set to  **128**, the number of logs is \(128 + 128 x 2 + 1\) x 16 MB = 6 GB.
>- Before the parameters are adjusted, the Xlogs of different data volumes bloat in different degrees in the TPC-C data export phase. As a result, GB-level logs bloat. The main cause is that dirty pages are not flushed to disks, the recovery point cannot be pushed forward, and logs cannot be recycled in time. After the parameters are adjusted, the log bloat rate decreases significantly.
>- Take the data warehouse 2000 as an example. Before the parameter adjustment, the log size bloats by 10 GB in the data export phase. After the parameter adjustment, the log size remains within the range of the minimum xlog value calculated based on the parameter setting.
