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

        **enable\_bitmapscan, enable\_hashagg, enable\_hashjoin, enable\_indexscan, enable\_indexonlyscan, enable\_material, enable\_mergejoin, enable\_nestloop, enable\_index\_nestloop, enable\_seqscan, enable\_sort, enable\_tidscan, partition\_iterator\_elimination, partition\_page\_estimation, enable\_functional\_dependency, var\_eq\_const\_selectivity, enable\_inner\_unique\_opt, enable\_broadcast, enable\_fast\_query\_shipping, enable\_force\_smp, enable\_imcsscan, enable\_remotegroup, enable\_remotejoin, enable\_remotelimit, enable\_remotesort, enable\_smp\_dml, enable\_sortgroup\_agg, enable\_stream\_operator, enable\_stream\_recursive,** and **enable\_trigger\_shipping**

    - Integer

        **query\_dop**, **best\_agg\_plan**, and **effective\_cache\_size**

    - Floating point

        **cost\_weight\_index**,  **default\_limit\_rows**,  **seq\_page\_cost**,  **random\_page\_cost**,  **cpu\_tuple\_cost**,  **cpu\_index\_tuple\_cost**, and  **cpu\_operator\_cost**
        
    - Enumeration type
    
    ​       **try_vector_engine_strategy** and **rewrite\_rule**

    - String

        **node\_name**

>[!NOTE]NOTE 
>
>- Some parameters are available only for specific deployment modes or build options. If a parameter is unavailable in the current instance, its Hint generates an unrecognized-parameter warning and does not affect correct query execution.
>- If you set a parameter that is not in the whitelist and the parameter value is invalid or the hint syntax is incorrect, the query execution is not affected. Run  **explain\(verbose on\)**. An error message is displayed, indicating that hint parsing fails.
>- The GUC parameter hint takes effect only in the outermost query. That is, the GUC parameter hint in the subquery does not take effect.
>- The GUC parameter hint in the view definition does not take effect.
>- In the  **CREATE TABLE ... AS ...**  statement, the outermost GUC parameter hint takes effect.
