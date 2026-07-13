# Analyzing the Status of a Query Statement<a name="EN-US_TOPIC_0291615097"></a>

## Symptom<a name="section4792154194213"></a>

Some query statements are executed for an excessively long time in the system. You need to analyze the status of the query statements.

## Procedure<a name="section1587514811424"></a>

1. Log in to the host as the OS user  **omm**.
2. Run the following command to connect to the database:

    ```
    gsql -d postgres -p 8000
    ```

    **postgres**  is the name of the database, and  **8000**  is the port number.

3. Set the parameter  **track\_activities**  to  **on**.

    ```
    SET track_activities = on;
    ```

    The database collects the running information about active queries only if the parameter is set to  **on**.

4. View the running query statements. The  **pg\_stat\_activity**  view is used as an example here.

    ```
    SELECT datname, usename, state, query FROM pg_stat_activity; 
    datname  | usename | state  | query 
    ----------+---------+--------+-------
    postgres | omm     | idle   | 
    postgres | omm     | active | 
    (2 rows) 
    ```

    If the  **state**  column is  **idle**, the connection is idle and requires a user to enter a command. To identify only active query statements, run the following command:

    ```
    SELECT datname, usename, state, query FROM pg_stat_activity WHERE state != 'idle';
    ```

5. Analyze whether a query statement is in the active or blocked state. Run the following command to view a query statement in the block state:

    ```
    SELECT datname, usename, state, query FROM pg_stat_activity WHERE waiting = true;
    ```

    The query statement is displayed. It is requesting a lock resource that may be held by another session, and is waiting for the lock resource to be released by the session.
