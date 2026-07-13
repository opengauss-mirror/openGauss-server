# Table Size Does not Change After VACUUM FULL Is Executed on the Table<a name="EN-US_TOPIC_0291615102"></a>

## Symptom<a name="section6396133915314"></a>

A user runs the  **VACUUM FULL**  command to clear a table, the table size does not change.

## Cause Analysis<a name="section19352134312532"></a>

Assume the table is named  **table\_name**. Possible causes are as follows:

- No data has been deleted from the  **table\_name**  table. Therefore, the execution of  **VACUUM FULL table\_name**  does not cause the table size to change.

- Concurrent transactions exist during the execution of  **VACUUM FULL table\_name**. As a result, recently deleted data may be skipped when clearing the table.

## Procedure<a name="section34071447115313"></a>

For the second possible cause, use either of the following methods:

- Wait until all concurrent transactions are complete, and run the  **VACUUM FULL table\_name**  command again.

- If the table size still does not change, ensure no service operations are performed on the table, and then execute the following SQL statements to query the active transaction list status:

    ```
    select txid_current();
    ```

    The current XID is obtained. Then, run the following command to check the active transaction list:

    ```
    select txid_current_snapshot();
    ```

    If any XID in the active transaction list is smaller than the current transaction XID, stop the database and then start it. Run  **VACUUM FULL**  to clear the table again.
