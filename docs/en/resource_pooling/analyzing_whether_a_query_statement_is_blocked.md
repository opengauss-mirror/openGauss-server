# Analyzing Whether a Query Statement Is Blocked<a name="EN-US_TOPIC_0291615099"></a>

## Symptom<a name="section148995712711"></a>

During database running, query statements are blocked in some service scenarios. As a result, the query statements are executed for an excessively long time.

## Cause Analysis<a name="section1916631418712"></a>

A query statement uses a lock to protect the data objects that it wants to access. If the data objects have been locked by another session, the query statement will be blocked and wait for the session to complete operation and release the lock resource. The data objects requiring locks include tables and tuples.

## Procedure<a name="section16731125079"></a>

1. Log in to the host as the OS user  **omm**.
2. Run the following command to connect to the database:

    ```
    gsql -d postgres -p 8000
    ```

    **postgres**  is the name of the database, and  **8000**  is the port number.

3. Find the thread ID of the faulty session from the current active session view.

    ```
    SELECT w.query AS waiting_query, w.pid AS w_pid, w.usename AS w_user, l.query AS locking_query, l.pid AS l_pid, l.usename AS l_user, t.schemaname || '.' || t.relname AS tablename FROM pg_stat_activity w JOIN pg_locks l1 ON w.pid = l1.pid AND NOT l1.granted JOIN pg_locks l2 ON l1.relation = l2.relation AND l2.granted JOIN pg_stat_activity l ON l2.pid = l.pid JOIN pg_stat_user_tables t ON l1.relation = t.relid WHERE w.waiting = true;
    ```

4. Terminate the session using its thread ID.

    ```
    SELECT pg_terminate_backend(139834762094352);
    ```

    If information similar to the following is displayed, the session is successfully terminated:

    ```
    pg_terminate_backend 
    ---------------------
    t
    (1 row)
    ```

    If a command output similar to the following is displayed, a user is attempting to terminate the session, and the session will be reconnected rather than being terminated.

    ```
    FATAL:  terminating connection due to administrator command 
    FATAL:  terminating connection due to administrator command The connection to the server was lost. Attempting reset: Succeeded.
    ```
