# gms_profiler

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:25:47.347Z pushedAt=2026-09-12T06:25:14.295Z -->

## gms_profiler Overview

gms_profiler is an extension based on openGauss, used to collect the execution status of PL/pgSQL programs. By analyzing the collected data, it helps identify performance bottlenecks in PL/pgSQL programs and provides statistics on code coverage. The currently supported interfaces include: START_PROFILER, STOP_PROFILER, PAUSE_PROFILER, RESUME_PROFILER, FLUSH_DATA, and others.

## gms_profiler Limitations

- The extension can only be loaded using the CREATE EXTENSION command.
- It is recommended to encapsulate the extension's interfaces within stored procedures for invocation; direct calls may return failures.
- Scenarios involving exception handling within stored procedures are not supported, as they may lead to inaccurate data collection.
- If the flush_data interface is invoked during the test, calling ROLLBACK afterwards is not supported and will result in an error. If ROLLBACK is required, it is recommended to complete data collection and table writing uniformly through the stop_profiler interface.

## gms_profiler Installation

gms_profiler is included by default during the packaging and compilation of openGauss. After installing openGauss, you can directly load the extension by executing create extension gms_profiler;.

## Using gms_profiler

### Creating an Extension<a name="section21088306113"></a>

The gms_profiler extension can be created directly using the CREATE Extension command:

```
openGauss=# CREATE Extension gms_profiler;
```

### Using Extension<a name="section107391050141118"></a>

Create a stored procedure for testing.

```sql
openGauss=# create or replace procedure do_something (p_times in number) as
openGauss$# l_dummy number;
openGauss$# begin
openGauss$#     for i in 1 .. p_times loop
openGauss$#         select l_dummy +1 into l_dummy;
openGauss$#     end loop;
openGauss$# end;
openGauss$# /
CREATE PROCEDURE
openGauss=#
openGauss=# create or replace procedure do_wrapper (p_times in number) as
openGauss$# begin
openGauss$#     for i in 1 .. p_times loop
openGauss$#         do_something(p_times);
openGauss$#     end loop;
openGauss$# end;
openGauss$# /
CREATE PROCEDURE
openGauss=#
openGauss=# create or replace procedure test_profiler_start () as
openGauss$# declare
openGauss$# l_result binary_integer;
openGauss$# begin
openGauss$#     l_result := gms_profiler.start_profiler('test_profiler', 'simple');
openGauss$#     do_wrapper(p_times => 2);
openGauss$#     l_result := gms_profiler.stop_profiler();
openGauss$# end;
openGauss$# /
CREATE PROCEDURE
```

Call the stored procedure.

```
openGauss=# call test_profiler_start();
```

Query the results.

```
openGauss=# select * from gms_profiler.plsql_profiler_runs;
openGauss=# select * from gms_profiler.plsql_profiler_units;
openGauss=# select * from gms_profiler.plsql_profiler_data;
```

### Deleting an Extension<a name="section1587441381220"></a>

The method for deleting the gms_profiler extension in openGauss is as follows:

```
openGauss=# DROP Extension gms_profiler [CASCADE];
```

>[!NOTE] Note
>
>If the extension is depended on by other objects, you need to add the CASCADE keyword to delete all dependent objects.