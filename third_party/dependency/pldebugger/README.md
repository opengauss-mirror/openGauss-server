## Introduction

This module contains the  implementation patch and installation scripts of shared libraries "Pldebugger", which support for debugging the pl/pgsql functions in openGauss.



Installation
----------------------------------------------------------------------------
### Compile and Install

#### compiled together with openGauss:

1. Download source code pldebugger_3_0.tar.gz from the website:

   https://opengauss.obs.cn-south-1.myhuaweicloud.com/dependency/pldebugger_3_0.tar.gz

2. Copy it to **third_party/dependency/pldebugger** dictionary.

3. Compile openGauss, with '**--enable-pldebugger**' option when configure.

4. Pldebugger will be installed automatically with openGauss once compilation and installation finished.

   

#### compiled separately from openGauss:

1. Download source code pldebugger_3_0.tar.gz from the website:

   https://opengauss.obs.cn-south-1.myhuaweicloud.com/dependency/pldebugger_3_0.tar.gz 
2. Copy it to **third_party/dependency/pldebugger/** dictionary.

3. Compile openGauss.

4. cd **contrib/pldebugger**, run:

   ```
   make -sj && make install -sj 
   ```

5. Finished  

   

### Download and Install
1. Download software from the website:

   for x86 system:

   https://opengauss.obs.cn-south-1.myhuaweicloud.com/dependency/pldebugger_package_x86.tar.gz

   for aarch64 system:

   https://opengauss.obs.cn-south-1.myhuaweicloud.com/dependency/pldebugger_package_arm.tar.gz

2. Decompress the file.

3. Copy these files to the specified paths:

   ```
   cp plugin_debugger.so $GAUSSHOME/lib/postgresql/
   cp pldbgapi.control $GAUSSHOME/share/postgresql/extension/
   cp pldbgapi--1.0.sql $GAUSSHOME/share/postgresql/extension/
   cp pldbgapi--unpackaged--1.0.sql  $GAUSSHOME/share/postgresql/extension/
   ```

4. Finished.



## Quick Start

With installation of both pldebugger and openGauss, you are able to quickly use start the pldebugger through the following steps: 

1. Modify POSTMASTER-level GUC in postgresql.conf:
   
   ```
   shared_preload_libraries = '$libdir/plugin_debugger'
   ```
   
2. Restart the openGauss database for the new setting to take effect.

3. Create an extension in the databases you want to debug functions in: 

   ```
   postgres=# CREATE EXTENSION pldbgapi;
   ```

4. Check whether the version is correct: 

   ```
   postgres=# select * from pldbg_get_proxy_info();
   ```

5. Now you can debug a fucntion with pldebugger!



## Usage

The debugger contains two parts:

1.  The server. This is the backend that runs the code being debugged. It keeps waiting  for the debugging command  from client and then take the corresponding actions. 
2. The client. This is the backend that displays the source code, current stack frame, variables etc, and allows users to set the breakpoints or other debugging commands.



#### Start Up the Server & Client:

You have to  start up at least one pair of sever and client before debugging, however, a single client is able to handle multiple severs and debugging tasks, and  a server is only able to get connected with one debugging process at the same time. 

##### Sever startup：

1. In the first session window, get the function OID which you want to debug:

   ```
   postgres=# select oid from pg_proc where proname='compute';
     oid
   -------
    16416
   (1 row)
   ```

2. register the function in breakpoints and start the server:

   ```
   postgres=# select plpgsql_oid_debug(16416);
   NOTICE:  Pldebugger is started successfully, you are SERVER now.
   CONTEXT:  referenced column: plpgsql_oid_debug
    plpgsql_oid_debug
   -------------------
                    0
   (1 row)
   ```

3. call the debugging function just registered, and the **port** for client to connect will be returned:

   ```
   postgres=# select * from compute(1);
   NOTICE:  YOUR PROXY PORT ID IS:0
   CONTEXT:  PL/pgSQL function compute(integer) line 3 at assignment
   
   ```

4. keep this session waiting for a client to get connected with.

##### Client startup： 

1. Open a **new** session window.

2. Attach to the listening severs, using the port numbers that the target sever returned before.  After connected, the sessions will be created and their session ID will be returned and marked as their session handle for the future use.

   > **NOTICE**: there could be more than one severs are listening and available to attach, you could  connect with them in the same time through their port numbers.

   ```
   postgres=# select * from pldbg_attach_to_port(0);
   NOTICE:  Pldebugger is started successfully, you are CLIENT now.
    pldbg_attach_to_port
   ----------------------
                       1
   (1 row)
   
   postgres=# select * from pldbg_attach_to_port(1);
    pldbg_attach_to_port
   ----------------------
                       2
   (1 row)
   
   ```

3. Until now, you have build the debugging parts successfully, and you could begin debugging the target functions within their own sessions.



##### Debugging Examples:

The following commands should be run in the client:

> **NOTICE**: for the commands that require session ID, if you pass integer "0" as the session ID rather than the real one, it means do the commands to the client's **most recent session**.

- get stepped into the next  the command or the sub-function by session ID:

  ```
  postgres=# select pldbg_step_into(1);
          pldbg_step_into
  -------------------------------
   (16416,5,"compute(integer)")
  (1 row)
  
  ```

- step over the function through the session ID, it will not step into the sub-function:

  ```
  postgres=# select pldbg_step_over(1);
          pldbg_step_over
  -------------------------------
   (16416,6,"compute(integer)")
  (1 row)
  ```

- stop the target debugging process by session ID:

  ```
  postgres=# select * from pldbg_abort_target(1);
   pldbg_abort_target
  --------------------
   t
  (1 row)
  
  ```

- set the breakpoints with session ID, function ID and line number:

  ```
  postgres=# select pldbg_set_breakpoint(1, 16416, 6);
   pldbg_set_breakpoint
  ----------------------
   t
  (1 row)
  ```

- get the breakpoints by session ID:

  ```
  postgres=# select * from pldbg_get_breakpoints(2);
   func  | linenumber | targetname
  -------+------------+------------
   16416 |          6 |
   16416 |         -1 |
  (2 rows)
  ```

- delete th breakpoint by session ID, function OID, line number；

  ```
  postgres=# select * from pldbg_drop_breakpoint(1, 16416, 6);
   pldbg_drop_breakpoint
  -----------------------
   t
  (1 row)
  
  ```

- get the debugging variables by session ID:

  ```
  postgres=# select * from pldbg_get_variables(1);
                  name                | varclass | linenumber | isunique | isconst | isnotnull | dtype | value
  ------------------------------------+----------+------------+----------+---------+-----------+-------+-------
   i                                  | A        |          0 | t        | f       | f         |    23 | 1
   result_1                           | A        |          0 | t        | f       | f         |    20 | 2
   result_2                           | A        |          0 | t        | f       | f         |    20 | NULL
   __gsdb_sql_cursor_attri_found__    | L        |          0 | t        | f       | f         |    16 | NULL
   __gsdb_sql_cursor_attri_notfound__ | L        |          0 | t        | f       | f         |    16 | NULL
   __gsdb_sql_cursor_attri_isopen__   | L        |          0 | t        | f       | f         |    16 | f
   __gsdb_sql_cursor_attri_rowcount__ | L        |          0 | t        | f       | f         |    23 | NULL
  (7 rows)
  
  
  ```

- get the stack of the debugging process by session ID:

  ```
  postgres=# select * from pldbg_get_stack(1);
   level |    targetname     | func  | linenumber | args
  -------+-------------------+-------+------------+------
       0 | compute(integer) | 16416 |          4 | i=1
  (1 row)
  
  ```

- get the target function run to the ending by session ID:

  ```
  postgres=# select * from pldbg_continue(1);
  ```

- stop the debugging process by session ID, but still be kept in connection:

  ```
  postgres=# select * from pldbg_abort_target(1);
   pldbg_abort_target
  --------------------
   t
  (1 row)
  
  ```

  

Pldbugger shutdown:

The debugging process will be shut down automatically when server/client exits the current session,  or you can manually shutdown the pldebugger in the server/client:

```
postgres=# select * from pldbg_on();
NOTICE:  Pldebugger is started successfully, you are SERVER now.
 pldbg_on
----------
 t
(1 row)


postgres=# select * from pldbg_off();
 pldbg_off
-----------
 t
(1 row)

```



Other useful commands:

- get the source code of function by function OID:

  ```
  postgres=#  select * from pldbg_get_source(16416);
      pldbg_get_source
  ------------------------
                         +
   begin                 +
       result_1 = i + 1; +
       result_2 = i * 10;+
   return next;          +
   end;                  +
  
  (1 row)
  ```

- get the information of pldebugger:

  ```
  postgres=# select * from pldbg_get_proxy_info();
                                                                           serverversionstr                                                                         | serverversionnu
  m | proxyapiver | serverprocessid
  ------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------
  --+-------------+-----------------
   (openGauss 1.0.0 build eef8fd2b) compiled at 2020-09-18 17:08:10 commit 1231 last mr 1342 debug on x86_64-unknown-linux-gnu, compiled by g++ (GCC) 8.2.0, 64-bit |            9020
  4 |           3 |               7
  (1 row)
  ```

   