# gms_debug

## gms_debug概述

gms_debug是一个基于openGauss的插件，用于实现服务器端调试器，提供一种调试服务端PL/SQL程序单元的方法。目前支持的接口有：

- gms_debug.initialize
- gms_debug.attach_session
- gms_debug.set_breakpoint
- gms_debug.continue
- gms_debug.get_runtime_info
- gms_debug.detach_session
- gms_debug.debug_off
- gms_debug.probe_version

## gms_debug限制

- 仅支持create extension命令方式加载插件。
- 用于单机下调试存储过程，需要用户具有gs_role_pldebugger权限。
- 调试端必须设置参数兼容模式`set behavior_compat_options='proc_outparam_override'`

## gms_debug安装

openGauss打包编译时默认已经包含了gms_debug, 可以在安装完openGauss后，直接通过create extension gms_debug;加载插件。

## gms_debug使用

### 创建Extension<a name="section21088306113"></a>

创建gms_debug Extension可直接使用CREATE Extension命令进行创建：

```
openGauss=# CREATE Extension gms_debug;
```

### 使用Extension<a name="section107391050141118"></a>

gms\_debug下系统函数用于单机下调试存储过程，目前支持的接口及其描述如下所示。仅管理员有权限执行这些调试接口，且无权限修改和创建新函数。

对应权限角色为gs\_role\_pldebugger，可以由管理员用户通过如下命令将debugger权限赋权给该用户。

```
GRANT gs_role_pldebugger to user;
```

需要有两个客户端连接数据库，一个客户端运行代码作为目标端，另一个客户端执行调试函数作为调试端。

调试端必须设置参数兼容模式

```
set behavior_compat_options='proc_outparam_override';

```

#### gms_debug.initialize

- gms_debug.initialize(IN debug_session_id varchar2(30) DEFAULT '', IN diagnostics binary_integer DEFAULT 0) returns varchar2

  **描述**：目标端调用，初始化调试环境。

  **参数说明**：

  - `debug_session_id`: 调试会话 ID ,如果不指定，则生成一个唯一 ID。
  - `diagnostics`: 指示是否将诊断输出转储到跟踪文件(暂不支持)

  **返回值**：调试会话ID,如果调用者没有指定会话ID，则系统会自动生成一个。

  **示例**：

  ```
    openGauss=# create table test(a int, b varchar(40), c timestamp);
        /
    CREATE TABLE

    openGauss=# CREATE OR REPLACE FUNCTION test_debug(x int) RETURNS SETOF test AS
    $BODY$
    DECLARE
        sql_stmt VARCHAR2(500);
        r test%rowtype;
        rec record;
        b_tmp text;
        cnt int;
        a_tmp int;
        cur refcursor;
        n_tmp NUMERIC(24,6);
        t_tmp tsquery;
        CURSOR cur_arg(criterion INTEGER) IS
            SELECT * FROM test WHERE a < criterion;
    BEGIN
        cnt := 0;
        FOR r IN SELECT * FROM test
        WHERE a > x
        LOOP
            RETURN NEXT r;
        END LOOP;

        FOR rec in SELECT * FROM test
        WHERE a < x
        LOOP
            RETURN NEXT rec;
        END LOOP;

        FORALL index_1 IN 0..1
            INSERT INTO test VALUES (index_1, 'Happy Children''s Day!', '2021-6-1');

        SELECT b FROM test where a = 7 INTO b_tmp;
        sql_stmt := 'select a from test where b = :1;';
        OPEN cur FOR sql_stmt USING b_tmp;
        IF cur%isopen then LOOP
            FETCH cur INTO a_tmp;
            EXIT WHEN cur%notfound;
            END LOOP;
        END IF;
        CLOSE cur;
        WHILE cnt < 3 LOOP
            cnt := cnt + 1;
        END LOOP;

        RAISE INFO 'cnt is %', cnt;

        RETURN;

    END
    $BODY$
    LANGUAGE plpgsql;
    /
    CREATE FUNCTION
    openGauss=# select * from gms_debug.initialize();
      initialize  
    -------
     sgnode-0
    (1 row)
  ```

#### gms_debug.attach_session

- gms_debug.attach_session(IN debug_session_id varchar2(30), IN diagnostics binary_integer DEFAULT 0) returns void

  **描述**：调试端调用，此过程将目标程序的情况通知给调试端

  **参数说明**：

  - `debug_session_id`: 调试会话ID 
  - `diagnostics`: 如果非零则生成诊断输出(暂不支持)

  **返回值**：

  **示例**：

  ```
    openGauss=# set behavior_compat_options='proc_outparam_override';
    SET

    openGauss=# SELECT * FROM gms_debug.attach_session('sgnode-0');
        attach_session 
    -----------------
        
    (1 row)
  ```

#### gms_debug.set_breakpoint

- gms_debug.set_breakpoint(IN program program_info, IN line# binary_integer, OUT breakpoint# binary_integer, IN fuzzy binary_integer := 0, IN iterations binary_integer := 0 ) returns void

  **描述**：调试端调用，该函数在程序单元中设置一个断点，该断点在当前会话中持续存在

  **参数说明**：

  - `program`: 有关要设置断点的程序单元的信息 
  - `line#`: 要设置断点的行
  - `breakpoint#`: 调用成功后，包含用于引用断点的唯一断点编号
  - `fuzzy`: 仅适用于指定行没有可执行代码的情况(暂不支持)
  - `iterations`: 发出此断点信号之前等待的次数(暂不支持)

  **返回值**：调用状态

  **示例**：

    ```
    openGauss=# CREATE or REPLACE FUNCTION gms_breakpoint(funcname text, lineno int)
    returns void as $$
    declare 
        pro_info  gms_debug.program_info;
        bkline     binary_integer;
        ret     binary_integer;
    begin
        pro_info.name := funcname;
        ret := gms_debug.set_breakpoint(pro_info, lineno, bkline,1,1);
        RAISE NOTICE 'ret= %', ret;
        RAISE NOTICE 'ret= %', bkline;
    end;
    $$ LANGUAGE plpgsql;
    /
    CREATE FUNCTION
    ```

    ```
    openGauss=# select gms_breakpoint('test_debug', 15);
    NOTICE:  ret= 1
    CONTEXT:  referenced column: gms_breakpoint
    NOTICE:  ret= 0
    CONTEXT:  referenced column: gms_breakpoint
    gms_breakpoint 
    ----------------
    ```

#### gms_debug.get_runtime_info

- gms_debug.get_runtime_info(IN info_requested binary_integer, OUT run_info runtime_info) returns binary_integer

  **描述**：此函数返回有关当前程序的信息。

  **参数说明**：

  - `run_info`: 调试状态的信息
  - `info_requested`: 程序停止时应返回哪些信息

  **返回值**：调用状态

  **示例**：

    ```
    openGauss=# CREATE or REPLACE FUNCTION gms_info()
    returns void as $$
    declare
        run_info  gms_debug.runtime_info;
        ret     binary_integer;
    begin
        ret := gms_debug.get_runtime_info(1,run_info);
        RAISE NOTICE 'breakpoint= %', run_info.breakpoint;
        RAISE NOTICE 'stackdepth= %', run_info.stackdepth;
        RAISE NOTICE 'line= %', run_info.line#;
        RAISE NOTICE 'reason= %', run_info.reason;
    end;
    $$ LANGUAGE plpgsql;
     /
    CREATE FUNCTION

    openGauss=# select gms_info();
    returns void as $$
    NOTICE:  breakpoint= -1
    CONTEXT:  referenced column: gms_info
    NOTICE:  stackdepth= 0
    CONTEXT:  referenced column: gms_info
    NOTICE:  line= 46
    CONTEXT:  referenced column: gms_info
    NOTICE:  reason= 0
    CONTEXT:  referenced column: gms_info
    gms_info 
    --------------

    ```

#### gms_debug.continue

- gms_debug.continue(IN OUT run_info  runtime_info, IN breakflags binary_integer, IN info_requested  binary_integer := NULL ) returns binary_integer

  **描述**：此函数将给定的 breakflags（感兴趣的事件的掩码）传递给目标进程中的 Probe。它告诉 Probe 继续执行目标进程，并等待目标进程运行完成或发出事件信号。如果info_requested不是NULL，则调用GET_RUNTIME_INFO。

  **参数说明**：

  - `runtime_info`: 调试状态的信息
  - `breakflags`: 感兴趣的事件的掩码(不支持掩码叠加)
  - `info_requested`: 程序停止时应返回哪些信息

  **返回值**：调用状态

  **示例**：

    ```
    openGauss=# CREATE or REPLACE FUNCTION gms_continue()
    returns void as $$
    declare
        run_info  gms_debug.runtime_info;
        ret     binary_integer;
    begin
        ret := gms_debug.continue(run_info, 0, 2);
        RAISE NOTICE 'breakpoint= %', run_info.breakpoint;
        RAISE NOTICE 'stackdepth= %', run_info.stackdepth;
        RAISE NOTICE 'line= %', run_info.line#;
        RAISE NOTICE 'reason= %', run_info.reason;
        RAISE NOTICE 'ret= %',ret;
    end;
    $$ LANGUAGE plpgsql;
     /
    CREATE FUNCTION

    openGauss=# select gms_continue();
    returns void as $$
    NOTICE:  breakpoint= -1
    CONTEXT:  referenced column: gms_continue
    NOTICE:  stackdepth= 0
    CONTEXT:  referenced column: gms_continue
    NOTICE:  line= 46
    CONTEXT:  referenced column: gms_continue
    NOTICE:  reason= 0
    CONTEXT:  referenced column: gms_continue
    NOTICE:  ret= 0
    CONTEXT:  referenced column: gms_continue
    gms_continue 
    --------------

    ```

#### gms_debug.detach_session

- gms_debug.detach_session() returns void

  **描述**：调试端调用，此过程停止调试目标程序

  **参数说明**：

  **返回值**：

  **示例**：

    ```
    openGauss=# select gms_debug.detach_session();
    detach_session 
    ----------------
 
    ```

#### gms_debug.debug_off

- gms_debug.debug_off() returns void

  **描述**：目标端调用，关闭调试

  **参数说明**：

  **返回值**：

  **示例**：

    ```
    openGauss=# select * from gms_debug.debug_off();
    debug_off 
    ----------------
 
    ```

#### gms_debug.probe_version

- gms_debug.probe_version(OUT major binary_integer, OUT minor binary_integer) returns void

  **描述**：返回版本号

  **参数说明**：

  - `major`: 主版本号
  - `minor`: 次版本号

  **返回值**：

  **示例**：

    ```
    openGauss=# CREATE or REPLACE FUNCTION gms_version()
    returns void as $$
    declare
        major   binary_integer;
        minor   binary_integer;
    begin
        gms_debug.probe_version(major, minor);
        RAISE NOTICE 'major= %', major;
        RAISE NOTICE 'minor= %', minor;
    end;
    $$ LANGUAGE plpgsql;
     /
    CREATE FUNCTION

    openGauss=# select gms_version();
    returns void as $$
    NOTICE:  major= 1
    CONTEXT:  referenced column: gms_version
    NOTICE:  minor= 0
    CONTEXT:  referenced column: gms_version
    gms_version 
    --------------
 
    ```

### 删除Extension<a name="section1587441381220"></a>

在openGauss中删除gms_debug Extension的方法如下所示：

```
openGauss=# DROP Extension gms_debug [CASCADE];
```

>[!NOTE]说明
>
>如果Extension被其它对象依赖，需要加入CASCADE（级联）关键字，删除所有依赖对象。
