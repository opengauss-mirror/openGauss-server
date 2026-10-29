# gms_tcp

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:26:39.051Z pushedAt=2026-09-20T09:49:10.012Z -->

## gms_tcp Overview

gms_tcp is a network communication plugin based on openGauss. It provides TCP/IP-level network programming capabilities, allowing the database to perform network communication operations directly.

The main interfaces currently supported are as follows:

Connection management:

- OPEN_CONNECTION    --Establish a TCP connection, supporting parameters such as remote host, port, local host, and port.
- CLOSE_CONNECTION   --Close the specified TCP connection
- CLOSE_ALL_CONNECTIONS --Close all TCP connections
- FLUSH             --Immediately send the data in the output buffer to the server

Data reading:

- READ_LINE         --Read a line of data, with an option of whether to remove the newline character
- READ_RAW         --Read raw binary data of a specified length
- READ_TEXT        --Read text data of a specified length
- GET_LINE         --Obtain the layer implementation for reading a line of data
- GET_RAW          --Obtain the layer implementation for reading raw data
- GET_TEXT         --Obtain the layer implementation for reading text data

Data writing:

- WRITE_LINE       --Writes a line of data, automatically appending a newline character
- WRITE_RAW        --Writes raw binary data
- WRITE_TEXT       --Writes text data

Connection status:

- AVAILABLE        --Check the number of data bytes available for reading in the TCP connection.

## gms_tcp Limitations

- The plugin can only be loaded using the CREATE EXTENSION command.
- It can only be used within anonymous blocks and PL/pgSQL.
- Before executing CREATE EXTENSION gms_tcp;, ensure that the gms_tcp schema does not already exist in the database.

## gms_tcp Installation

gms_tcp is included by default when openGauss is packaged and compiled. After openGauss is installed, the extension can be loaded directly by executing create extension gms_tcp;.

## Using gms_tcp

### Creating an Extension<a name="section21088306113"></a>

The gms_tcp extension can be created directly using the CREATE Extension command:

```sql
openGauss=# CREATE Extension gms_tcp;
```

### Using Extension<a name="section107391050141118"></a>

#### Corresponding Interfaces

open_connection Function
Interface: open_connection(remote_host in varchar2, remote_port in integer[, local_host in varchar2, local_port in integer, in_buffer_size in integer, out_buffer_size in integer, cset in varchar2, newline in varchar2, tx_timeout in integer]) RETURNS gms_tcp.connection
Function: Establishes a TCP connection.
Parameters

- remote_host: Remote host address.
- remote_port: Remote port number.
- local_host: Local host address (optional).
- local_port: local port number (optional)
- in_buffer_size: input buffer size (optional)
- out_buffer_size: output buffer size (optional)
- cset: character set (optional)
- newline: newline character type (optional, default: CRLF)
- tx_timeout: transmission timeout (optional)
Return value: TCP connection handle

close_connection stored procedure
Interface: close_connection(c in gms_tcp.connection)
Function: closes a TCP connection
Parameters: c: the connection handle to be closed

write_line function
Interface: write_line(c in gms_tcp.connection, data in varchar2) RETURNS integer
Function: writes a line of data (automatically appends a newline character)
Parameters:

- c: connection handle
- data: data to be sent
Return value: number of bytes written

write_text function
Interface: write_text(c in gms_tcp.connection, data in varchar2[, len in integer]) RETURNS integer
Function: writes text data
Parameters

- c: connection handle
- data: data to be sent
- len: length to be sent (optional)
Return value: number of bytes written

read_line stored procedure
Interface: read_line(c in gms_tcp.connection, data out varchar2, len out integer[, remove_crlf in boolean, peek in boolean])
Function: reads a line of data
Parameters

- c: connection handle
- data: variable for receiving data
- len: length of data read
- remove_crlf: Whether to remove newline characters (optional)
- peek: Whether to only peek without removing data (optional)

read_text stored procedure
Interface: read_text(c in gms_tcp.connection, data out varchar2, data_len out integer[, len in integer, peek in boolean])
Function: Reads text data
Parameters

- c: connection handle
- data: variable that receives data
- data_len: actual length read
- len: length to be read (optional)
- peek: whether to only peek without removing data (optional)

available function
Interface: available(c in gms_tcp.connection[, timeout in int]) RETURNS integer
Function: checks the number of bytes available for reading
Parameters

- c: connection handle
- timeout: timeout period (in milliseconds, optional)
Return value: number of readable bytes

#### Simple Execution Flow of GMS_TCP

The prerequisite is that a TCP service connection exists, and then the database acts as a client to execute the following statements:

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

### Deleting an Extension<a name="section1587441381220"></a>

The method for deleting the gms_tcp extension in openGauss is as follows:

```sql
openGauss=# DROP Extension gms_tcp [CASCADE];
```

>[!NOTE]Note
>
>If the extension is depended on by other objects, the CASCADE keyword must be added to delete all dependent objects.