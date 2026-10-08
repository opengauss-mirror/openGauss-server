# CONFIGURATIONS

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:30:46.896Z pushedAt=2026-09-21T09:53:49.801Z -->

Returns parameter-related information.

**Table 1** CONFIGURATIONS

<table aria-label="Table 1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Column Name</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>configuration_id</td>
            <td>int</td>
            <td>ID of the parameter. The value is always NULL.</td>
        </tr>
        <tr>
            <td>name</td>
            <td>nvarchar(35)</td>
            <td>Parameter name</td>
        </tr>
        <tr>
            <td>value</td>
            <td>sql_variant</td>
            <td>Current value of the parameter</td>
        </tr>
        <tr>
            <td>minimum</td>
            <td>sql_variant</td>
            <td>Minimum value of the parameter.</td>
        </tr>
        <tr>
            <td>maximum</td>
            <td>sql_variant</td>
            <td>Maximum value of the parameter</td>
        </tr>
        <tr>
            <td>value_in_use</td>
            <td>sql_variant</td>
            <td>Current value of the parameter</td>
        </tr>
        <tr>
            <td>description</td>
            <td>nvarchar(255)</td>
            <td>Brief description of the parameter</td>
        </tr>
        <tr>
            <td>is_dynamic</td>
            <td>bit</td>
            <td>Indicates whether the parameter takes effect dynamically. 1 means dynamic, 0 means non-dynamic.<br>
            For parameters at the postmaster and internal levels, the value is 0.<br>
            For parameters at the sighup, backend, suset, and userset levels, the value is 1.
            </td>
        </tr>
        <tr>
            <td>is_advanced</td>
            <td>bit</td>
            <td>Indicates whether it is an advanced parameter. The value is always 0.</td>
        </tr>
    </tbody>
</table>