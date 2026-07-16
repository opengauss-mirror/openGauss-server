# gms_tcp

## gms_tcp概述

gms_tcp是一个基于openGauss的网络通信插件，提供TCP/IP级别的网络编程功能，允许数据库直接进行网络通信操作。

目前支持的主要接口有：

连接管理：

- OPEN_CONNECTION    --建立TCP连接，支持指定远程主机、端口、本地主机、端口等参数
- CLOSE_CONNECTION   --关闭指定的TCP连接
- CLOSE_ALL_CONNECTIONS --关闭所有TCP连接
- FLUSH             --立即将输出缓冲区中的数据发送到服务器

数据读取：

- READ_LINE         --读取一行数据，可选是否移除换行符
- READ_RAW         --读取指定长度的原始二进制数据
- READ_TEXT        --读取指定长度的文本数据
- GET_LINE         --获取一行数据的底层实现
- GET_RAW          --获取原始数据的底层实现
- GET_TEXT         --获取文本数据的底层实现

数据写入：

- WRITE_LINE       --写入一行数据，自动添加换行符
- WRITE_RAW        --写入原始二进制数据
- WRITE_TEXT       --写入文本数据

连接状态：

- AVAILABLE        --检查TCP连接中可读取的数据字节数

## gms_tcp限制

- 仅支持Create extension命令方式加载插件。
- 只支持在匿名块和PL/pgSQL中使用。
- 在Create extension gms_tcp；之前要保证数据库中没有gms_tcp这个schema。

## gms_tcp安装

openGauss打包编译时默认已经包含了gms_tcp, 可以在安装完openGauss后，直接通过create extension gms_tcp;加载插件。

## gms_tcp使用

### 创建Extension<a name="section21088306113"></a>

创建gms_tcp Extension可直接使用CREATE Extension命令进行创建：

```sql
openGauss=# CREATE Extension gms_tcp;
```

### 使用Extension<a name="section107391050141118"></a>

#### 对应接口

open_connection函数
接口：open_connection(remote_host in varchar2, remote_port in integer[, local_host in varchar2, local_port in integer, in_buffer_size in integer, out_buffer_size in integer, cset in varchar2, newline in varchar2, tx_timeout in integer]) RETURNS gms_tcp.connection
功能：建立TCP连接
参数：

- remote_host：远程主机地址
- remote_port：远程端口号
- local_host：本地主机地址（可选）
- local_port：本地端口号（可选）
- in_buffer_size：输入缓冲区大小（可选）
- out_buffer_size：输出缓冲区大小（可选）
- cset：字符集（可选）
- newline：换行符类型（可选，默认CRLF）
- tx_timeout：传输超时时间（可选）
返回值：TCP连接句柄

close_connection存储过程
接口：close_connection(c in gms_tcp.connection)
功能：关闭TCP连接
参数：c：要关闭的连接句柄

write_line函数
接口：write_line(c in gms_tcp.connection, data in varchar2) RETURNS integer
功能：写入一行数据（自动添加换行符）
参数：

- c：连接句柄
- data：要发送的数据
返回值：写入的字节数

write_text函数
接口：write_text(c in gms_tcp.connection, data in varchar2[, len in integer]) RETURNS integer
功能：写入文本数据
参数：

- c：连接句柄
- data：要发送的数据
- len：要发送的长度（可选）
返回值：写入的字节数

read_line存储过程
接口：read_line(c in gms_tcp.connection, data out varchar2, len out integer[, remove_crlf in boolean, peek in boolean])
功能：读取一行数据
参数：

- c：连接句柄
- data：接收数据的变量
- len：读取的数据长度
- remove_crlf：是否移除换行符（可选）
- peek：是否只查看不移除数据（可选）

read_text存储过程
接口：read_text(c in gms_tcp.connection, data out varchar2, data_len out integer[, len in integer, peek in boolean])
功能：读取文本数据
参数：

- c：连接句柄
- data：接收数据的变量
- data_len：实际读取的长度
- len：要读取的长度（可选）
- peek：是否只查看不移除数据（可选）

available函数
接口：available(c in gms_tcp.connection[, timeout in int]) RETURNS integer
功能：检查可读取的数据字节数
参数：

- c：连接句柄
- timeout：超时时间（毫秒，可选）
返回值：可读取的字节数

#### GMS_TCP 简单执行流程如下

先决条件是有一个TCP服务连接，然后将数据库当做客户端执行以下语句：

```sql
create extension gms_tcp;
create or replace function gms_tcp_test_in_buffer()
    returns void
    language plpgsql
as $function$
declare
    c gms_tcp.connection;
    num integer;
    data varchar2;
    len integer;
begin
    c = gms_tcp.open_connection(remote_host=>'127.0.0.1',
                                remote_port=>12358,
                                in_buffer_size=>20,
                                tx_timeout=>10);
    num = gms_tcp.write_line(c, 'in buffer');
    pg_sleep(1);
    
    num = gms_tcp.write_line(c, 'ok');
    pg_sleep(1);
    num = gms_tcp.available(c,1);
    if num > 0 then
        len = 5;
        data = gms_tcp.get_text(c, len);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;
    
    num = gms_tcp.write_line(c, 'ok');
    pg_sleep(1);
    num = gms_tcp.available(c,1);
    if num > 0 then
        len = 12;
        data = gms_tcp.get_text(c, len);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;
    
    num = gms_tcp.write_line(c, 'ok');
    pg_sleep(1);
    num = gms_tcp.available(c,1);
    if num > 0 then
        len = 13;
        data = gms_tcp.get_text(c, len);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;
    
    num = gms_tcp.write_line(c, 'ok');
    pg_sleep(1);
    num = gms_tcp.available(c,1);
    if num > 0 then
        len = 12;
        data = gms_tcp.get_text(c, len);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;
    
    num = gms_tcp.write_line(c, 'ok');
    pg_sleep(1);
    num = gms_tcp.available(c,1);
    if num > 0 then
        len = 5;
        data = gms_tcp.get_text(c, len);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;
    
    num = gms_tcp.write_line(c, 'ok');
    pg_sleep(1);
    num = gms_tcp.available(c,1);
    if num > 0 then
        len = 17;
        data = gms_tcp.get_text(c, len);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;
    
    num = gms_tcp.write_line(c, 'ok');
    pg_sleep(1);
    num = gms_tcp.available(c,1);
    if num > 0 then
        len = 9;
        data = gms_tcp.get_text(c, len);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;
    
    num = gms_tcp.write_line(c, 'ok');
    pg_sleep(1);
    num = gms_tcp.available(c,1);
    if num > 0 then
        len = 11;
        data = gms_tcp.get_text(c, len);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;
    
    num = gms_tcp.write_line(c, 'ok');
    pg_sleep(1);
    num = gms_tcp.available(c,1);
    if num > 0 then
        data = gms_tcp.get_line(c);
        raise info 'available: %, rcv: %.', num, data;
    end if;

    gms_tcp.close_all_connections();

exception
    when gms_tcp_network_error then
        raise info 'caught gms_tcp_network_error';
        gms_tcp.close_all_connections();
        
    when gms_tcp_bad_argument then
        raise info 'caught gms_tcp_bad_argument';
        gms_tcp.close_all_connections();
        
    when gms_tcp_buffer_too_small then
        raise info 'caught gms_tcp_buffer_too_small';
        gms_tcp.close_all_connections();
        
    when gms_tcp_end_of_input then
        raise info 'caught gms_tcp_end_of_input';
        gms_tcp.close_all_connections();

    when gms_tcp_transfer_timeout then
        raise info 'caught gms_tcp_transfer_timeout';
        gms_tcp.close_all_connections();

    when gms_tcp_partial_multibyte_char then
        raise info 'caught gms_tcp_partial_multibyte_char';
        gms_tcp.close_all_connections();
        
    when others then
        raise info 'caught others';
        gms_tcp.close_all_connections();
end;
$function$;

--
--test read data
--
--get line
create or replace function gms_tcp_test_get_line()
    returns void
    language plpgsql
as $function$
declare
    c gms_tcp.connection;
    num integer;
    data varchar2;
begin
    c = gms_tcp.open_connection(remote_host=>'127.0.0.1',
                                remote_port=>12358,
                                in_buffer_size=>20480,
                                out_buffer_size=>20480,
                                tx_timeout=>10);
    num = gms_tcp.write_line(c, 'get line');
    gms_tcp.flush(c);

    num = gms_tcp.available(c,1);
    if num > 0 then
        data = gms_tcp.get_line(c, true);
        raise info 'available: %, rcv: %.', num, data;
    end if;

    gms_tcp.close_all_connections();

exception
    when others then
        raise info 'caught others';
        gms_tcp.close_all_connections();
end;
$function$;

--get text
create or replace function gms_tcp_test_get_text()
    returns void
    language plpgsql
as $function$
declare
    c gms_tcp.connection;
    num integer;
    data varchar2;
begin
    c = gms_tcp.open_connection(remote_host=>'127.0.0.1',
                                remote_port=>12358,
                                in_buffer_size=>20480,
                                out_buffer_size=>20480,
                                tx_timeout=>10);
    num = gms_tcp.write_line(c, 'get text');
    gms_tcp.flush(c);

    num = gms_tcp.available(c,1);
    if num > 0 then
        data = gms_tcp.get_text(c, 17, true);
        raise info 'available: %, rcv: %.', num, data;
    end if;

    gms_tcp.close_all_connections();

exception
    when others then
        raise info 'caught others';
        gms_tcp.close_all_connections();
end;
$function$;

--get raw
create or replace function gms_tcp_test_get_raw()
    returns void
    language plpgsql
as $function$
declare
    c gms_tcp.connection;
    num integer;
    data raw;
begin
    c = gms_tcp.open_connection(remote_host=>'127.0.0.1',
                                remote_port=>12358,
                                in_buffer_size=>20480,
                                out_buffer_size=>20480,
                                tx_timeout=>10);
    num = gms_tcp.write_line(c, 'get raw');
    gms_tcp.flush(c);

    num = gms_tcp.available(c,1);
    if num > 0 then
        data = gms_tcp.get_raw(c, 4, true);
        raise info 'available: %, rcv: %.', num, data;
    end if;
    
    num = gms_tcp.available(c,1);
    if num > 0 then
        data = gms_tcp.get_raw(c, 8);
        raise info 'available: %, rcv: %.', num, data;
    end if;

    gms_tcp.close_all_connections();

exception
    when others then
        raise info 'caught others';
        gms_tcp.close_all_connections();
end;
$function$;

--read line
create or replace function gms_tcp_test_read_line()
    returns void
    language plpgsql
as $function$
declare
    c gms_tcp.connection;
    num integer;
    data varchar2;
    len integer;
begin
    c = gms_tcp.open_connection(remote_host=>'127.0.0.1',
                                remote_port=>12358,
                                in_buffer_size=>20480,
                                out_buffer_size=>20480,
                                tx_timeout=>10);
    num = gms_tcp.write_line(c, 'read line');
    gms_tcp.flush(c);

    num = gms_tcp.available(c,1);
    if num > 0 then
        gms_tcp.read_line(c, data, len, true);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;
    
    num = gms_tcp.available(c,1);
    if num > 0 then
        gms_tcp.read_line(c, data, len);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;

    gms_tcp.close_all_connections();

exception
    when others then
        raise info 'caught others';
        gms_tcp.close_all_connections();
end;
$function$;

--read text
create or replace function gms_tcp_test_read_text()
    returns void
    language plpgsql
as $function$
declare
    c gms_tcp.connection;
    num integer;
    data varchar2;
    len integer;
    out_len integer;
begin
    c = gms_tcp.open_connection(remote_host=>'127.0.0.1',
                                remote_port=>12358,
                                in_buffer_size=>20480,
                                out_buffer_size=>20480,
                                tx_timeout=>10);
    num = gms_tcp.write_line(c, 'read text');
    gms_tcp.flush(c);

    num = gms_tcp.available(c,1);
    if num > 0 then
        out_len = 18;
        gms_tcp.read_text(c, data, len, out_len, true);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;

    gms_tcp.close_all_connections();

exception
    when others then
        raise info 'caught others';
        gms_tcp.close_all_connections();
end;
$function$;

create or replace function gms_tcp_test_read_raw()
    returns void
    language plpgsql
as $function$
declare
    c gms_tcp.connection;
    num integer;
    data raw;
    len integer;
    out_len integer;
begin
    c = gms_tcp.open_connection(remote_host=>'127.0.0.1',
                                remote_port=>12358,
                                in_buffer_size=>20480,
                                tx_timeout=>10);
    num = gms_tcp.write_line(c, 'read raw');
    gms_tcp.flush(c);

    num = gms_tcp.available(c,1);
    if num > 0 then
        out_len = 3;
        gms_tcp.read_raw(c, data, len, out_len, true);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;
    
    num = gms_tcp.available(c,1);
    if num > 0 then
        out_len = 4;
        gms_tcp.read_raw(c, data, len, out_len, true);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;
    
    num = gms_tcp.available(c,1);
    if num > 0 then
        out_len = 8;
        gms_tcp.read_raw(c, data, len, out_len);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;

    gms_tcp.close_all_connections();

exception
    when others then
        raise info 'caught others';
        gms_tcp.close_all_connections();
end;
$function$;

--write line
create or replace function gms_tcp_test_write_line()
    returns void
    language plpgsql
as $function$
declare
    c gms_tcp.connection;
    num integer;
    data varchar2;
    len integer;
begin
    c = gms_tcp.open_connection(remote_host=>'127.0.0.1',
                                remote_port=>12358,
                                in_buffer_size=>20480,
                                newline=>'LF',
                                tx_timeout=>10);
    num = gms_tcp.write_line(c, 'write line');
    pg_sleep(1);
    num = gms_tcp.write_line(c, '0123456789');

    num = gms_tcp.available(c,1);
    if num > 0 then
        gms_tcp.read_line(c, data, len);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;

    gms_tcp.close_all_connections();

exception
    when others then
        raise info 'caught others';
        gms_tcp.close_all_connections();
end;
$function$;

--write text
create or replace function gms_tcp_test_write_text()
    returns void
    language plpgsql
as $function$
declare
    c gms_tcp.connection;
    num integer;
    data varchar2;
    len integer;
begin
    c = gms_tcp.open_connection(remote_host=>'127.0.0.1',
                                remote_port=>12358,
                                in_buffer_size=>20480,
                                --out_buffer_size=>20480,
                                tx_timeout=>10);
    num = gms_tcp.write_text(c, 'write text', 10);
    pg_sleep(1);
    num = gms_tcp.write_text(c, '0123456789', 6);

    num = gms_tcp.available(c,1);
    if num > 0 then
        gms_tcp.read_line(c, data, len);
        raise info 'available: %, rcv: %(%).', num, data, len;
    end if;

    gms_tcp.close_all_connections();

exception
    when others then
        raise info 'caught others';
        gms_tcp.close_all_connections();
end;
$function$;

create or replace function gms_tcp_test_error_in_buffer_size()
    returns void
    language plpgsql
as $function$
declare
    c gms_tcp.connection;
begin
    c = gms_tcp.open_connection(remote_host=>'127.0.0.1',
                                remote_port=>12358,
                                in_buffer_size=>40480,
                                out_buffer_size=>20480,
                                newline=>'lf',
                                tx_timeout=>10);
    gms_tcp.close_all_connections();

exception
    when gms_tcp_network_error then
        raise info 'caught gms_tcp_network_error';
        gms_tcp.close_all_connections();
        
    when gms_tcp_bad_argument then
        raise info 'caught gms_tcp_bad_argument';
        gms_tcp.close_all_connections();
        
    when gms_tcp_buffer_too_small then
        raise info 'caught gms_tcp_buffer_too_small';
        gms_tcp.close_all_connections();
        
    when gms_tcp_end_of_input then
        raise info 'caught gms_tcp_end_of_input';
        gms_tcp.close_all_connections();

    when gms_tcp_transfer_timeout then
        raise info 'caught gms_tcp_transfer_timeout';
        gms_tcp.close_all_connections();

    when gms_tcp_partial_multibyte_char then
        raise info 'caught gms_tcp_partial_multibyte_char';
        gms_tcp.close_all_connections();
        
    when others then
        raise info 'caught others';
        gms_tcp.close_all_connections();
end;
$function$;

create or replace function gms_tcp_test_error_out_buffer_size()
    returns void
    language plpgsql
as $function$
declare
    c gms_tcp.connection;
begin
    c = gms_tcp.open_connection(remote_host=>'127.0.0.1',
                                remote_port=>12358,
                                in_buffer_size=>20480,
                                out_buffer_size=>40480,
                                newline=>'lf',
                                tx_timeout=>10);
    gms_tcp.close_all_connections();

exception
    when gms_tcp_network_error then
        raise info 'caught gms_tcp_network_error';
        gms_tcp.close_all_connections();
        
    when gms_tcp_bad_argument then
        raise info 'caught gms_tcp_bad_argument';
        gms_tcp.close_all_connections();
        
    when gms_tcp_buffer_too_small then
        raise info 'caught gms_tcp_buffer_too_small';
        gms_tcp.close_all_connections();
        
    when gms_tcp_end_of_input then
        raise info 'caught gms_tcp_end_of_input';
        gms_tcp.close_all_connections();

    when gms_tcp_transfer_timeout then
        raise info 'caught gms_tcp_transfer_timeout';
        gms_tcp.close_all_connections();

    when gms_tcp_partial_multibyte_char then
        raise info 'caught gms_tcp_partial_multibyte_char';
        gms_tcp.close_all_connections();
        
    when others then
        raise info 'caught others';
        gms_tcp.close_all_connections();
end;
$function$;

--
--char_set
--
create or replace function gms_tcp_test_char_set()
    returns void
    language plpgsql
as $function$
declare
    c gms_tcp.connection;
    num integer;
    data varchar2;
begin
    c = gms_tcp.open_connection(remote_host=>'127.0.0.1',
                                remote_port=>12358,
                                in_buffer_size=>20480,
                                out_buffer_size=>20480,
                                cset=>'gbk',
                                tx_timeout=>10);
    num = gms_tcp.write_line(c, 'char set');
    gms_tcp.flush(c);

    num = gms_tcp.available(c,1);
    if num > 0 then
        data = gms_tcp.get_line(c);
        raise info 'available: %, rcv: %.', num, data;
    end if;

    gms_tcp.close_all_connections();

exception
    when others then
        raise info 'caught others';
        gms_tcp.close_all_connections();
end;
$function$;

create or replace function gms_tcp_test_quit()
    returns void
    language plpgsql
as $function$
declare
    c gms_tcp.connection;
    num integer;
begin
    c = gms_tcp.open_connection(remote_host=>'127.0.0.1',
                                remote_port=>12358,
                                tx_timeout=>10);
    num = gms_tcp.write_line(c, 'quit');

    gms_tcp.close_all_connections();

exception
    when others then
        raise info 'caught others';
        gms_tcp.close_all_connections();
end;
$function$;

select pg_sleep(5);

select gms_tcp_test_in_buffer();
select gms_tcp_test_get_line();
select gms_tcp_test_get_text();
select gms_tcp_test_get_raw();
select gms_tcp_test_read_line();
select gms_tcp_test_read_text();
select gms_tcp_test_read_raw();
select gms_tcp_test_write_line();
select gms_tcp_test_write_text();
select gms_tcp_test_error_in_buffer_size();
select gms_tcp_test_error_out_buffer_size();
select gms_tcp_test_char_set();
select gms_tcp_test_quit();

drop function gms_tcp_test_in_buffer();
drop function gms_tcp_test_get_line();
drop function gms_tcp_test_get_text();
drop function gms_tcp_test_get_raw();
drop function gms_tcp_test_read_line();
drop function gms_tcp_test_read_text();
drop function gms_tcp_test_read_raw();
drop function gms_tcp_test_write_line();
drop function gms_tcp_test_write_text();
drop function gms_tcp_test_error_in_buffer_size();
drop function gms_tcp_test_error_out_buffer_size();
drop function gms_tcp_test_char_set();
drop function gms_tcp_test_quit();
$$;
```

### 删除Extension<a name="section1587441381220"></a>

在openGauss中删除gms_tcp Extension的方法如下所示：

```sql
openGauss=# DROP Extension gms_tcp [CASCADE];
```

>[!NOTE]说明
>
>如果Extension被其它对象依赖，需要加入CASCADE（级联）关键字，删除所有依赖对象。
