# gms_debug

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:25:19.832Z pushedAt=2026-09-24T10:52:41.186Z -->

## gms_debug Overview

gms_debug is an openGauss-based extension used to implement a server-side debugger, providing a method for debugging server-side PL/SQL program units. The currently supported interfaces are:

- gms_debug.initialize
- gms_debug.attach_session
- gms_debug.set_breakpoint
- gms_debug.continue
- gms_debug.get_runtime_info
- gms_debug.detach_session
- gms_debug.debug_off
- gms_debug.probe_version

## gms_debug Limitations

- Only the create extension command is supported for loading the plugin.
- It is used for debugging stored procedures in standalone mode, and the user must have the gs_role_pldebugger privilege.
- The debugger must set the compatibility mode parameter `set behavior_compat_options='proc_outparam_override'`.

## gms_debug Installation

gms_debug is included by default when openGauss is packaged and compiled. After openGauss is installed, you can load the extension directly by executing create extension gms_debug;.

## gms_debug Usage

### Creating an Extension<a name="section21088306113"></a>

To create the gms_debug extension, you can directly use the CREATE Extension command:

```
openGauss=# CREATE Extension gms_debug;
```

### Using Extension<a name="section107391050141118"></a>

The system functions under gms_debug are used for debugging stored procedures in standalone mode. The currently supported interfaces and their descriptions are shown below. Only the administrator has the permission to execute these debug interfaces, and has no permission to modify or create new functions.

The corresponding permission role is gs_role_pldebugger. The administrator can grant the debugger permission to a user by executing the following command.

```
GRANT gs_role_pldebugger to user;
```

Two clients must be connected to the database: one client runs the code as the target side, and the other client executes the debug functions as the debug side.

The debug side must set the parameter compatibility mode.

```
set behavior_compat_options='proc_outparam_override';

```

#### gms_debug.initialize

- gms_debug.initialize(IN debug_session_id varchar2(30) DEFAULT '', IN diagnostics binary_integer DEFAULT 0) returns varchar2

  **Description**: Invoked on the target side to initialize the debug environment.

  **Parameter description**:

  - `debug_session_id`: Debug session ID. If not specified, a unique ID is generated.
  - `diagnostics`: Whether to dump diagnostic output to a trace file (not yet supported)

  **Return value**: Debug session ID. If the caller does not specify a session ID, the system automatically generates one.

  **Example**:

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

  **Description**: Called by the debugger, this procedure notifies the debugger of the target program's status.

  **Parameter description**:

  - `debug_session_id`: Debug session ID
  - `diagnostics`: Generates diagnostic output if non-zero (not yet supported)

  **Return Value**:

  **Example**:

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

  **Description**: Called by the debugger, this function sets a breakpoint in a program unit, and the breakpoint persists within the current session.

  **Parameter description**:

  - `program`: Information about the program unit where the breakpoint is to be set.
  - `line#`: the line where the breakpoint is to be set
  - `breakpoint#`: after a successful call, contains a unique breakpoint number for referencing the breakpoint
  - `fuzzy`: only applicable when the specified line has no executable code (not yet supported)
  - `iterations`: the number of times to wait before signaling this breakpoint (not yet supported)

  **Return value**: call status

  **Example**:

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

  **Description**: This function returns information about the current program.

  **Parameter description**:

  - `run_info`: Information about the debug status
  - `info_requested`: information to be returned when the program stops

  **Return value**: call status

  **Example**:

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

  **Description**: This function passes the given breakflags (a mask of events of interest) to the Probe in the target process. It instructs the Probe to resume execution of the target process and waits until the target process completes or signals an event. If info_requested is not NULL, GET_RUNTIME_INFO is called.

  **Parameter Description**:

  - `runtime_info`: Information about the debug status
  - `breakflags`: Mask of events of interest (mask superposition is not supported)
  - `info_requested`: Information to be returned when the program stops

  **Return value**: Call status

  **Example**:

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

  **Description**: Debugger invocation. This procedure stops debugging the target program.

  **Parameter description**:

  **Return value**:

  **Example**:

    ```
    openGauss=# select gms_debug.detach_session();
    detach_session 
    ----------------
 
    ```

#### gms_debug.debug_off

- gms_debug.debug_off() returns void

  **Description**: Invoked on the target side to disable debugging.

  **Parameter description**:

  **Return value**:

  **Example**:

    ```
    openGauss=# select * from gms_debug.debug_off();
    debug_off 
    ----------------
 
    ```

#### gms_debug.probe_version

- gms_debug.probe_version(OUT major binary_integer, OUT minor binary_integer) returns void

  **Description**: Returns the version number.

  **Parameter description**:

  - `major`: Major version number.
  - `minor`: minor version number

  **Return value**:

  **Example**:

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

### Deleting the Extension<a name="section1587441381220"></a>

The method for deleting the gms_debug extension in openGauss is as follows:

```
openGauss=# DROP Extension gms_debug [CASCADE];
```

>[!NOTE] Note
>
>If the extension is depended on by other objects, you need to add the CASCADE keyword to delete all dependent objects.