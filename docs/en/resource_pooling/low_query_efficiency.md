# Low Query Efficiency<a name="EN-US_TOPIC_0291615100"></a>

## Symptom<a name="section6698114504"></a>

A query task that used to take a few milliseconds to complete is now requiring several seconds, and that used to take several seconds is now requiring even half an hour. 

## Procedure<a name="section747619196010"></a>

Perform the following procedure to locate the cause.

1. Run the  **analyze**  command to analyze the database.

    Run the  **analyze**  command to update statistics such as data sizes and attributes in all tables. You are advised to perform the operation with light job load. If the query efficiency is improved or restored after the command execution, the  **autovacuum**  process does not function well that requires further analysis.

2. Check whether the query statement returns unnecessary information.

    For example, if a query statement is used to query all records in a table with the first 10 records being used, then it is quick to query a table with 50 records. However, if a table contains 50000 records, the query efficiency decreases. If an application requires only a part of data information but the query statement returns all information, add a LIMIT clause to the query statement to restrict the number of returned records. In this way, the database optimizer can optimize space and improve query efficiency.

3. Check whether the query statement still has a low response even when it is solely executed.

    Run the query statement when there are no or only a few other query requests in the database, and observe the query efficiency. If the efficiency is high, the previous issue is possibly caused by a heavily loaded host in the database system or

4. Check the same query statement repeatedly to check the query efficiency.

    One major cause of low query efficiency is that the required information is not cached in the memory or is replaced by other query requests due to insufficient memory resources. This can be verified by running the same query statement repeatedly and the query efficiency increases gradually.
