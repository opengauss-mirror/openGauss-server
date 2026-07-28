# gms_profiler

## gms_profiler概述

gms_profiler是一个基于openGauss的插件，用于收集PL/pgSQL程序执行情况，通过分析收集的数据进而找到PL/pgSQL程序的性能瓶颈，统计程序的代码覆盖率。目前支持的接口有：START_PROFILER、STOP_PROFILER、PAUSE_PROFILER、RESUME_PROFILER、FLUSH_DATA等。

## gms_profiler限制

- 仅支持Create extension命令方式加载插件。
- 插件中接口建议封装在存储过程中调用，直接调用可能会返回失败。
- 不支持存储过程中存在异常处理的场景，会导致收集信息不准确。
- 如果测试过程调用了flush_data接口，不支持其后调用ROOLBACK操作，会报错。如需使用ROOLBACK, 建议统一通过stop_profiler接口完成收集信息写表。

## gms_profiler安装

openGauss打包编译时默认已经包含了gms_profiler, 可以在安装完openGauss后，直接通过create extension gms_profiler;加载插件。

## gms_profiler使用

### 创建Extension<a name="section21088306113"></a>

创建gms_profiler Extension可直接使用CREATE Extension命令进行创建：

```
openGauss=# CREATE Extension gms_profiler;
```

### 使用Extension<a name="section107391050141118"></a>

创建用于测试的存储过程。

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

调用存储过程

```
openGauss=# call test_profiler_start();
```

查询结果

```
openGauss=# select * from gms_profiler.plsql_profiler_runs;
openGauss=# select * from gms_profiler.plsql_profiler_units;
openGauss=# select * from gms_profiler.plsql_profiler_data;
```

### 删除Extension<a name="section1587441381220"></a>

在openGauss中删除gms_profiler Extension的方法如下所示：

```
openGauss=# DROP Extension gms_profiler [CASCADE];
```

>[!NOTE]说明
>
>如果Extension被其它对象依赖，需要加入CASCADE（级联）关键字，删除所有依赖对象。
