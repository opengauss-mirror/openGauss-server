# Different Data Is Displayed for the Same Table Queried By Multiple Users<a name="EN-US_TOPIC_0291615104"></a>

## Symptom<a name="section4581718173618"></a>

Two users log in to the same database human\_resource and run the following statement separately to query the areas table, but obtain different results.

```
select count(*) from areas;
```

## Cause Analysis<a name="section1611323143617"></a>

1. Check whether tables with same names are really the same table. In a relational database, a table is identified by three elements:  **database**,  **schema**, and  **table**. In this issue,  **database**  is  **human\_resource**  and  **table**  is  **areas**.
2. Check whether schemas of tables with the same name are consistent. Log in as users  **omm**  and  **user01**  separately. It is found that  **search\_path**  is  **public**  and  _$user_, respectively. As  **omm**  is the cluster administrator, a schema having the same name as user  **omm**  will not be created by default. That is, all tables will be created in  **public**  if no schema is specified. However, when a common user, such as  **user01**, is created, the same-name schema \(**user01**\) is created by default. That is, all tables are created in  **user01**  if the schema is not specified.
3. If different users access different data in the same table, check whether objects in the table have different access policies for different users.

## Procedure<a name="section8271112913368"></a>

- For the query of tables with the same name in different schemas, add the schema reference to the queried table. The format is as follows:

    ```
    schema.table
    ```

- If different access policies result in different query results of the same table, you can query the  **pg\_rlspolicy**system catalog to determine the specific access rules.
