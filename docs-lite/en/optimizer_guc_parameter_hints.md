# Optimizer GUC Parameter Hints<a name="EN-US_TOPIC_0000001096400532"></a>

## Function<a name="section290819468377"></a>

Sets GUC parameters related to query optimization that take effect during the query execution. For details about the application scenarios of hints, see the description of each GUC parameter.

## Syntax<a name="section530131664410"></a>

```
set(param value)
```

## Parameters<a name="section41303128143838"></a>

- **param**  indicates the parameter name.
- **value**  indicates the value of a parameter.
- Currently, the following parameters can be set and take effect by using Hint:
    - Boolean

        **enable\_bitmapscan, enable\_hashagg, enable\_hashjoin, enable\_indexscan, enable\_indexonlyscan, enable\_material, enable\_mergejoin, enable\_nestloop, enable\_index\_nestloop, enable\_seqscan, enable\_sort, enable\_tidscan, partition\_iterator\_elimination, partition\_page\_estimation, var\_eq\_const\_selectivity,** and **enable\_functional\_dependency**

    - Integer

        **query\_dop**

    - Floating point

        **cost\_weight\_index**,  **default\_limit\_rows**,  **seq\_page\_cost**,  **random\_page\_cost**,  **cpu\_tuple\_cost**,  **cpu\_index\_tuple\_cost**,  **cpu\_operator\_cost**, and  **effective\_cache\_size**

    - Enumeration

        try\_vector\_engine\_strategy

>[!NOTE]NOTE 
>
>- If you set a parameter that is not in the whitelist and the parameter value is invalid or the hint syntax is incorrect, the query execution is not affected. Run  **explain\(verbose on\)**. An error message is displayed, indicating that hint parsing fails.
>- The GUC parameter hint takes effect only in the outermost query. That is, the GUC parameter hint in the subquery does not take effect.
>- The GUC parameter hint in the view definition does not take effect.
>- In the  **CREATE TABLE ... AS ...**  statement, the outermost GUC parameter hint takes effect.
