# Other Factors Affecting SMP Performance<a name="EN-US_TOPIC_0000001132033355"></a>

Besides resource factors, there are other factors that impact the SMP performance, such as uneven data distribution in a partitioned table and system parallelism degree.

- Impact of data skew on SMP performance

    Severe data skew deteriorates SMP performance. For example, if the data volume of a value in the join column is much more than that of other values, the data volume of a parallel thread will be much more than that of others after Hash-based data redistribution, resulting in the long-tail issue and poor SMP performance.

- Impact of system parallelism degree on the SMP performance

    The SMP feature uses more resources, and remaining resources are insufficient in a high concurrency scenario. Therefore, enabling the SMP function will result in severe resource competition among queries. Once resource competition occurs, no matter the CPU, I/O, or memory resources, all of them will result in entire performance deterioration. In the high concurrency scenario, enabling the SMP function will not improve the performance and even may cause performance deterioration.
