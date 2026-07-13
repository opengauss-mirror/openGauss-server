# Performance Deterioration Caused by Dirty Page Flushing Efficiency During TPC-C High-Concurrency Long-Term Stable Running<a name="EN-US_TOPIC_0289900262"></a>

## Symptom<a name="en-us_topic_0283136782_section526616331423"></a>

TPC-C performance deteriorates due to dirty page flushing efficiency during high-concurrency long-term stable running. The details are as follows: The initial performance is high. As the running time increases, the value of **tmpTotal** in the database decreases, the CPU usage of the WalWriter thread is 100%, and other CPUs are almost not loaded. In the WDR report, the waiting time for dirty page flushing accounts for the highest proportion.

## Cause<a name="en-us_topic_0283136782_section1710704115427"></a>

Generally, you can analyze the specific cause by checking the process status and operating system resource usage (such as CPU and I/O), or further analyze the root cause based on the WDR. In this scenario, dirty page refreshing is inefficient.

## Solution<a name="en-us_topic_0283136782_section12323144814214"></a>

1. Reduce the concurrency or increase the value of **shared_buffers**.
2. Adjust dirty page parameters. In scenarios where doublewrite is enabled, you can adjust parameters such as **pagewriter\_sleep** (downward adjustment) and **max\_io\_capacity** (upward adjustment) to improve dirty page elimination efficiency.
3. Replace high-performance disks (such as NVMe disks).

The resources occupied by the database must meet the service requirements. In a high-concurrency test, you need to add resources to ensure that database services are available.
