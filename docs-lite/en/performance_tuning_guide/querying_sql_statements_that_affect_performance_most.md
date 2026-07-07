# Querying SQL Statements That Affect Performance Most<a name="EN-US_TOPIC_0289900933"></a>

This section describes how to query SQL statements whose execution takes a long time, leading to poor system performance.

## Procedure<a name="en-us_topic_0283136757_en-us_topic_0237121490_en-us_topic_0073253542_en-us_topic_0040046535_section43790015111840"></a>

1. Log in as the OS user  **omm**  to a database node.
2. Run the following command to connect to the database:

    ```
    gsql -d postgres -p 8000
    ```

    **postgres**  is the name of the database to be connected, and  **8000**  is the port number of the database node.

    If information similar to the following is displayed, the connection succeeds:

    ```
    gsql((openGauss x.x.x build f521c606) compiled at 2021-09-16 14:55:22 commit 2935 last mr 6385 release)
    Non-SSL connection (SSL connection is recommended when requiring high-security)
    Type "help" for help.
    
    openGauss=# 
    ```

3. Query the statements that are run for a long time in the database.

    ```
    SELECT current_timestamp - query_start AS runtime, datname, usename, query FROM pg_stat_activity where state != 'idle' ORDER BY 1 desc;
    ```

    The command output lists the query statements in descending order by their execution duration length. The first record is the query statement that takes the longest time for execution. The returned result contains SQL statements invoked by the system and SQL statements run by users. Find the statements that were run by users and took a long time.

    Alternatively, you can set  **current\_timestamp - query\_start**  to be greater than a threshold to identify query statements that are executed for a duration longer than this threshold.

    ```
    SELECT query FROM pg_stat_activity WHERE current_timestamp - query_start > interval '1 days';
    ```

4. Set the parameter  **track\_activities**  to  **on**.

    ```
    SET track_activities = on;
    ```

    The database collects the running information about active queries only if the parameter is set to  **on**.

5. View the running query statements.

    The  **pg\_stat\_activity**  view is used as an example here.

    ```
    SELECT datname, usename, state FROM pg_stat_activity;
     datname  | usename | state  |
    ----------+---------+--------+
     postgres |   omm   | idle   |
     postgres |   omm   | active |
    (2 rows)
    ```

    If the  **state**  column is  **idle**, the connection is idle and requires a user to enter a command.

    To identify only active query statements, run the following command:

    ```
    SELECT datname, usename, state FROM pg_stat_activity WHERE state != 'idle';
    ```

6. Analyze the status of the query statements that were run for a long time.
    - If the query statement is normal, wait until the execution of the query statement is complete.
    - If a query statement is blocked, run the following command to view this query statement:

        ```
        SELECT datname, usename, state, query FROM pg_stat_activity WHERE waiting = true;
        ```

        The query statement is displayed. It is requesting a lock resource that may be held by another session, and is waiting for the lock resource to be released by the session.

        >[!NOTE]NOTE 
        >Only when the query is blocked by internal lock resources, the  **waiting**  column is  **true**. In most cases, blocks happen when query statements are waiting for lock resources to be released. However, query statements may be blocked due to write and timers operations. Such blocked queries are not displayed in the  **pg\_stat\_activity**  view.
