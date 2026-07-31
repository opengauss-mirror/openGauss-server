# Optimizing SQL Self-Diagnosis<a name="EN-US_TOPIC_0245374558"></a>

Performance issues may occur when you query data or run the  **INSERT**,  **DELETE**,  **UPDATE**, or  **CREATE TABLE AS**  statement. In this case, you can query the  **warning**  column in the [GS\_WLM\_SESSION\_STATISTICS](https://docs.opengauss.org/en/docs/latest/database_reference/gs_wlm_session_statistics.html)  and  [GS\_WLM\_SESSION\_HISTORY](https://docs.opengauss.org/en/docs/latest/database_reference/gs_wlm_session_history.html) views to obtain reference for performance optimization.

Alarms that can trigger SQL self diagnosis depend on the settings of  **[resource\_track\_level](resource_load_management_overview.md)**. If  **resource\_track\_level**  is set to  **query**, alarms about the failures in collecting column statistics and pushing down SQL statements will trigger the diagnosis. If  **resource\_track\_level**  is set to  **operator**, all alarms will trigger the diagnosis.

Whether a SQL plan will be diagnosed depends on the settings of  **[resource\_track\_cost](resource_load_management_overview.md)**. A SQL plan will be diagnosed only if its execution cost is greater than  **resource\_track\_cost**. You can use the  **EXPLAIN**  keyword to check the plan execution cost.

## Alarms<a name="en-us_topic_0237121523_section1451592315913"></a>

Currently, performance alarms will be reported when statistics about one or multiple columns are not collected.

An alarm will be reported when statistics about one or multiple columns are not collected. For details about the optimization, see  [Updating Statistics](update_statistics.md)  and  [Optimizing Statistics](optimizing_statistics.md).

Example alarms:

No statistics about a table are not collected.

```
Statistic Not Collect:
    schema_test.t1
```

The statistics about a single column are not collected.

```
Statistic Not Collect:
    schema_test.t2(c1,c2)
```

The statistics about multiple columns are not collected.

```
Statistic Not Collect:
    schema_test.t3((c1,c2))
```

The statistics about a single column and multiple columns are not collected.

```
Statistic Not Collect:
    schema_test.t4(c1,c2)    schema_test.t4((c1,c2))
```

## Restrictions<a name="en-us_topic_0237121523_section728715105125"></a>

1. An alarm contains a maximum of 2048 characters. If the length of an alarm exceeds this value \(for example, a large number of long table names and column names are displayed in the alarm when their statistics are not collected\), a warning instead of an alarm will be reported.

    ```
    WARNING, "Planner issue report is truncated, the rest of planner issues will be skipped"
    ```

2. If a query statement contains the  **Limit**  operator, alarms of operators lower than  **Limit**  will not be reported.
