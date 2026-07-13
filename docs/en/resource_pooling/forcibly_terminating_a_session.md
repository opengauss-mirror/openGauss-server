# Forcibly Terminating a Session<a name="EN-US_TOPIC_0291615098"></a>

## Symptom<a name="section14515143811220"></a>

In some cases, the administrator must forcibly terminate abnormal sessions to keep the system healthy.

## Procedure<a name="section1678174320128"></a>

1. Log in to the host as the OS user  **omm**.
2. Run the following command to connect to the database:

    ```
    gsql -d postgres -p 8000
    ```

    **postgres**  is the name of the database, and  **8000**  is the port number.

3. Find the thread ID of the faulty session from the current active session view.

    ```
    SELECT datid, pid, state, query FROM pg_stat_activity; 
    ```

    A command output similar to the following is displayed, where the pid value indicates the thread ID of the session.

    ```
    datid |       pid       | state  | query 
    -------+-----------------+--------+------ 
    13205 | 139834762094352 | active | 
    13205 | 139834759993104 | idle   | 
    (2 rows) 
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
