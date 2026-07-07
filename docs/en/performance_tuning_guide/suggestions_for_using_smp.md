# Suggestions for Using SMP<a name="EN-US_TOPIC_0000001085291122"></a>

## Limitations<a name="section545621551611"></a>

To use the SMP feature to improve the performance, ensure that the following conditions are met:

The CPU, memory, and I/O resources are sufficient. SMP is a solution that uses abundant resources to exchange time. After plan parallel is executed, resource consumption is increased. When these resources become a bottleneck, the SMP feature cannot improve the performance and even may deteriorate the performance. In the case of a resource bottleneck, you are advised to disable the SMP feature.

## Procedure<a name="section1076450151617"></a>

1. Observe the current system load situation. If resources are sufficient \(the resource usage is smaller than 50%\), perform step  [2](#li1174421213171). Otherwise, exit this system.
2. <a name="li1174421213171"></a>Set  **query\_dop**  to  **1**  \(default value\). Use  **explain**  to generate an execution plan and check whether the plan can be used in scenarios in  [SMP Application Scenarios and Restrictions](smp_application_scenarios_and_restrictions.md). If yes, go to step  [3](#li998191911172).
3. <a name="li998191911172"></a>Set  **query\_dop**  to  _value_. The parallelism degree is 1 or  _value_  regardless of the resource usage and plan characteristics.
4. Before the query statement is executed, set  **query\_dop**  to an appropriate value. After the statement is executed, set  **query\_dop**  to disable the query. The following provides an example:

    ```
    openGauss=# SET query_dop = 4;
    openGauss=# SELECT COUNT(*) FROM t1 GROUP BY a;
    ......
    openGauss=# SET query_dop = 1;
    ```

    >[!NOTE]NOTE 
    >- If resources are sufficient, the higher the parallelism degree is, the better the performance improvement effect is.
    >- The SMP parallelism degree supports session level settings. You are advised to enable the SMP feature before executing a query that meets the requirements. After the execution is complete, disable the SMP feature. Otherwise, SMP may affect services in peak hours.
