# Checking Blocked Statements<a name="EN-US_TOPIC_0289900192"></a>

During database running, query statements are blocked in some service scenarios and run for an excessively long time. In this case, you can forcibly terminate the faulty session.

## Procedure<a name="en-us_topic_0283137653_en-us_topic_0237121491_en-us_topic_0073253543_en-us_topic_0040046536_section19654526113952"></a>

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

3. View blocked query statements and details about the tables and schemas that block the query statements.

    ```
    SELECT w.query as waiting_query,
    w.pid as w_pid,
    w.usename as w_user,
    l.query as locking_query,
    l.pid as l_pid,
    l.usename as l_user,
    t.schemaname || '.' || t.relname as tablename
    from pg_stat_activity w join pg_locks l1 on w.pid = l1.pid
    and not l1.granted join pg_locks l2 on l1.relation = l2.relation
    and l2.granted join pg_stat_activity l on l2.pid = l.pid join pg_stat_user_tables t on l1.relation = t.relid
    where w.waiting;
    ```

    The thread ID, user details, query status, as well as details about the tables and schemas that block the query statements are returned.

4. Run the following command to terminate the required session, where  **139834762094352**  is the thread ID:

    ```
    SELECT PG_TERMINATE_BACKEND(139834762094352);
    ```

    If information similar to the following is displayed, the session is successfully terminated:

    ```
     PG_TERMINATE_BACKEND
    ----------------------
     t
    (1 row)
    ```

    If information similar to the following is displayed, a user is attempting to terminate the session, and the session will be reconnected rather than being terminated.

    ```
    FATAL:  terminating connection due to administrator command
    FATAL:  terminating connection due to administrator command
    The connection to the server was lost. Attempting reset: Succeeded.
    ```

    >[!NOTE]NOTE 
    >
    >If the  **PG\_TERMINATE\_BACKEND**  function is used to terminate the background threads of the session, the  **gsql**  client will be reconnected rather than be logged out.
