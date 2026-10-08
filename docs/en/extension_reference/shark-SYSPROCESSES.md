# SYSPROCESSES

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:15:29.382Z pushedAt=2026-09-29T02:34:56.679Z -->

Returns information about database sessions.

**Table 1** SYSPROCESSES

<table aria-label="表1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Column Name</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>spid</td>
            <td>bigint</td>
            <td>Session ID.</td>
        </tr>
        <tr>
            <td>kpid</td>
            <td>smallint</td>
            <td>Windows thread ID. Always NULL.</td>
        </tr>
        <tr>
            <td>blocked</td>
            <td>bigint</td>
            <td>ID of the session that is blocking the session corresponding to spid.</td>
        </tr>
        <tr>
            <td>waittype</td>
            <td>varbinary(2)</td>
            <td>Wait type. Always NULL.</td>
        </tr>
        <tr>
            <td>waittime</td>
            <td>bigint</td>
            <td>Wait time. Always 0.</td>
        </tr>
        <tr>
            <td>lastwaittype</td>
            <td>nchar(32)</td>
            <td>String indicating the name of the last or current wait type. Always NULL.</td>
        </tr>
        <tr>
            <td>waitresource</td>
            <td>nchar(256)</td>
            <td>Textual representation of the lock resource. Always NULL.</td>
        </tr>
        <tr>
            <td>dbid</td>
            <td>oid</td>
            <td>ID of the database currently used by the session.</td>
        </tr>
        <tr>
            <td>uid</td>
            <td>oid</td>
            <td>ID of the user who executed the command.</td>
        </tr>
        <tr>
            <td>cpu</td>
            <td>int</td>
            <td>Cumulative CPU time of the session. Always 0.</td>
        </tr>
        <tr>
            <td>physical_io</td>
            <td>bigint</td>
            <td>Cumulative disk reads and writes of the session. Always 0.</td>
        </tr>
        <tr>
            <td>memusage</td>
            <td>int</td>
            <td>Number of pages in the procedure cache currently allocated to this session. Always 0.</td>
        </tr>
        <tr>
            <td>login_time</td>
            <td>timestamp with time zone</td>
            <td>Time when the client process logged in to the server.</td>
        </tr>
        <tr>
            <td>last_batch</td>
            <td>timestamp with time zone</td>
            <td>Time when the client process last executed a stored procedure call or the EXECUTE statement.</td>
        </tr>
        <tr>
            <td>ecid</td>
            <td>smallint</td>
            <td>Execution context ID used to uniquely identify the subthread operating on behalf of a single process. Always 0.</td>
        </tr>
        <tr>
            <td>open_tran</td>
            <td>smallint</td>
            <td>Number of open transactions for the session. Always 0.</td>
        </tr>
        <tr>
            <td>status</td>
            <td>nchar(30)</td>
            <td>Session status. The value can be:<br>
            `active`: The backend is executing a query.<br>
            `idle`: The backend is waiting for a new client command.<br>
            `idle in transaction`: The backend is in a transaction, but no statement is being executed in the transaction.<br>
            `idle in transaction (aborted)`: The backend is in a transaction, but statement execution failures exist in the transaction.<br>
            `fastpath function call`: The backend is executing a fast-path function.<br>
            `disabled`: Reported if the backend disables `track_activities`.
            </td>
        </tr>
        <tr>
            <td>sid</td>
            <td>varbinary(86)</td>
            <td>User ID.</td>
        </tr>
        <tr>
            <td>hostname</td>
            <td>nchar(128)</td>
            <td>Host name of the client.</td>
        </tr>
        <tr>
            <td>program_name</td>
            <td>nchar(128)</td>
            <td>Name of the application.</td>
        </tr>
        <tr>
            <td>hostprocess</td>
            <td>nchar(10)</td>
            <td>ID of the client process. Always NULL.</td>
        </tr>
        <tr>
            <td>cmd</td>
            <td>text</td>
            <td>SQL command currently being executed.</td>
        </tr>
        <tr>
            <td>nt_domain</td>
            <td>nchar(128)</td>
            <td>Windows domain of the client. Always NULL.</td>
        </tr>
        <tr>
            <td>nt_username</td>
            <td>nchar(128)</td>
            <td>Windows user name of the client. Always NULL.</td>
        </tr>
        <tr>
            <td>net_address</td>
            <td>nchar(12)</td>
            <td>Unique identifier assigned to the network adapter on each user workstation. Always NULL.</td>
        </tr>
        <tr>
            <td>net_library</td>
            <td>nchar(12)</td>
            <td>Column that stores the client network library. Always NULL.</td>
        </tr>
        <tr>
            <td>loginame</td>
            <td>nchar(128)</td>
            <td>Username.</td>
        </tr>
        <tr>
            <td>context_info</td>
            <td>varbinary(128)</td>
            <td>Data stored in a batch by using the `set context_info` statement. Always NULL.</td>
        </tr>
        <tr>
            <td>sql_handle</td>
            <td>varbinary(20)</td>
            <td>Batch or object that is currently being executed. Always NULL.</td>
        </tr>
        <tr>
            <td>stmt_start</td>
            <td>int</td>
            <td>Start offset of the current SQL statement for the given sql_handle. Always 0.</td>
        </tr>
        <tr>
            <td>stmt_end</td>
            <td>int</td>
            <td>End offset of the current SQL statement for the given sql_handle. Always 0.</td>
        </tr>
        <tr>
            <td>request_id</td>
            <td>bigint</td>
            <td>Request ID, which identifies the request running in a specific session.</td>
        </tr>
    </tbody>
</table>
