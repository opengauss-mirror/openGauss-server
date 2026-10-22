# SYSCONFIGURES

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:01:51.227Z pushedAt=2026-09-29T02:34:56.658Z -->

Returns information about parameters.

**Table 1** SYSCONFIGURES

<table aria-label="table1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>value</td>
            <td>sql_variant</td>
            <td>Current value of the parameter.</td>
        </tr>
        <tr>
            <td>config</td>
            <td>int</td>
            <td>Parameter ID, always `NULL`.</td>
        </tr>
        <tr>
            <td>comment</td>
            <td>nvarchar(255)</td>
            <td>Brief description of the parameter.</td>
        </tr>
        <tr>
            <td>status</td>
            <td>smallint</td>
            <td>Status type of the parameter. `0` indicates static and `1` indicates dynamic.<br>
            The value is `0` for parameters at the postmaster and internal levels.<br>
            The value is `1` for parameters at the sighup, backend, suset, and userset levels.
            </td>
        </tr>
    </tbody>
</table>
