\echo Use "CREATE EXTENSION gms_tcp" to load this file. \quit
--
-- create schema
--
CREATE SCHEMA gms_tcp;

--
-- create type connection
--
CREATE TYPE gms_tcp.connection;

CREATE FUNCTION gms_tcp.connection_in(cstring)
RETURNS gms_tcp.connection
AS 'MODULE_PATHNAME','gms_tcp_connection_in'
LANGUAGE C STRICT NOT FENCED;

CREATE FUNCTION gms_tcp.connection_out(gms_tcp.connection)
RETURNS cstring
AS 'MODULE_PATHNAME','gms_tcp_connection_out'
LANGUAGE C STRICT NOT FENCED;

CREATE TYPE gms_tcp.connection(
  internallength = 512,
  input = gms_tcp.connection_in,
  output = gms_tcp.connection_out
);

--
-- function: crlf
--
CREATE FUNCTION gms_tcp.crlf()
RETURNS varchar2
AS 'MODULE_PATHNAME','gms_tcp_crlf'
LANGUAGE C STRICT NOT FENCED;

--
-- function: available
-- determines the number of bytes available for reading from a tcp/ip connection.
--
CREATE FUNCTION gms_tcp.available_real(c       in gms_tcp.connection,
                                       timeout in int)
RETURNS integer
AS 'MODULE_PATHNAME','gms_tcp_available_real'
LANGUAGE C NOT FENCED;

create or replace function gms_tcp.available(c       in gms_tcp.connection,
                                             timeout in int default 0)
    returns integer
    language plpgsql
as $function$
begin
    return gms_tcp.available_real(c, timeout);
end;
$function$;

--
-- funciton: close_all_connections
--
CREATE FUNCTION gms_tcp.close_all_connections()
RETURNS void
AS 'MODULE_PATHNAME','gms_tcp_close_all_connections'
LANGUAGE C STRICT NOT FENCED;

--
-- funciton: close_connection
--
CREATE FUNCTION gms_tcp.close_connection(c in gms_tcp.connection)
RETURNS void
AS 'MODULE_PATHNAME','gms_tcp_close_connection'
LANGUAGE C STRICT NOT FENCED;

--
-- function: flush
-- transmits immediately to the server all data in the output buffer, if a buffer is used.
--
CREATE FUNCTION gms_tcp.flush(c in gms_tcp.connection)
RETURNS void
AS 'MODULE_PATHNAME','gms_tcp_flush'
LANGUAGE C STRICT NOT FENCED;

--
-- function: get_line
--
CREATE FUNCTION gms_tcp.get_line_real(c           in gms_tcp.connection,
                                      remove_crlf in boolean,
                                      peek        in boolean,
                                      ch_charset  in boolean default false)
RETURNS text
AS 'MODULE_PATHNAME','gms_tcp_get_line_real'
LANGUAGE C STRICT NOT FENCED;

create or replace function gms_tcp.get_line(c           in gms_tcp.connection,
                                            remove_crlf in boolean default false,
                                            peek        in boolean default false)
    returns text
    language plpgsql
as $function$
begin
    return gms_tcp.get_line_real(c, remove_crlf, peek, false);
end;
$function$;

--
-- function: get_raw
--
CREATE FUNCTION gms_tcp.get_raw_real(c    in gms_tcp.connection,
                                     len  in integer,
                                     peek in boolean)
RETURNS raw
AS 'MODULE_PATHNAME','gms_tcp_get_raw_real'
LANGUAGE C STRICT NOT FENCED;

create or replace function gms_tcp.get_raw(c    in gms_tcp.connection,
                                           len  in integer default 1,
                                           peek in boolean default false)
    returns raw
    language plpgsql
as $function$
begin
    return gms_tcp.get_raw_real(c, len, peek);
end;
$function$;

--
-- function: get_text
--
CREATE FUNCTION gms_tcp.get_text_real(c    in gms_tcp.connection,
                                      len  in integer,
                                      peek in boolean,
                                      ch_charset in boolean default false)
RETURNS text
AS 'MODULE_PATHNAME','gms_tcp_get_text_real'
LANGUAGE C STRICT NOT FENCED;

create or replace function gms_tcp.get_text(c    in gms_tcp.connection,
                                            len  in integer default 1,
                                            peek in boolean default false)
    returns text
    language plpgsql
as $function$
begin
    return gms_tcp.get_text_real(c, len, peek, false);
end;
$function$;

--
-- function: open_connection
--
CREATE FUNCTION gms_tcp.open_connection_real(remote_host     in varchar2,
                                             remote_port     in integer,
                                             local_host      in varchar2 default 0,
                                             local_port      in integer default 0,
                                             in_buffer_size  in integer default 0,
                                             out_buffer_size in integer default 0,
                                             cset            in varchar2 default 0,
                                             newline         in varchar2 default 'CRLF',
                                             tx_timeout      in integer default 2147483647)
RETURNS gms_tcp.connection
AS 'MODULE_PATHNAME','gms_tcp_open_connection'
LANGUAGE C STRICT NOT FENCED;

CREATE FUNCTION gms_tcp.open_connection(remote_host     in varchar2,
                                        remote_port     in integer,
                                        local_host      in varchar2 default null,
                                        local_port      in integer default null,
                                        in_buffer_size  in integer default null,
                                        out_buffer_size in integer default null,
                                        cset            in varchar2 default null,
                                        newline         in varchar2 default 'CRLF',
                                        tx_timeout      in integer default null)
RETURNS gms_tcp.connection
as $$
declare
    local_host_tmp      varchar2;
    local_port_tmp      integer;
    in_buffer_size_tmp  integer;
    out_buffer_size_tmp integer;
    cset_tmp            varchar2;
    newline_tmp         varchar2;
    tx_timeout_tmp      integer;
begin
    if remote_host is null or remote_port is null then
        raise exception 'error input, remote_host or remote_port is null';
    end if;

    if local_host is null then
        local_host_tmp = 0;
    else
        local_host_tmp = local_host;
    end if;

    if local_port is null then
        local_port_tmp = 0;
    else
        local_port_tmp = local_port;
    end if;

    if in_buffer_size is null then
        in_buffer_size_tmp = 0;
    else
        in_buffer_size_tmp = in_buffer_size;
    end if;

    if out_buffer_size is null then
        out_buffer_size_tmp = 0;
    else
        out_buffer_size_tmp = out_buffer_size;
    end if;

    if cset is null then
        cset_tmp = 0;
    else
        cset_tmp = cset;
    end if;

    if newline is null then
        newline_tmp = 'CRLF';
    else
        newline_tmp = newline;
    end if;

    if tx_timeout is null then
        tx_timeout_tmp = 2147483647;
    else
        tx_timeout_tmp = tx_timeout;
    end if;

    return gms_tcp.open_connection_real(remote_host,
                                        remote_port,
                                        local_host_tmp,
                                        local_port_tmp,
                                        in_buffer_size_tmp,
                                        out_buffer_size_tmp,
                                        cset_tmp,
                                        newline_tmp,
                                        tx_timeout_tmp);
end;
$$ LANGUAGE plpgsql;

--
-- function: read_line
--
CREATE OR REPLACE PROCEDURE gms_tcp.read_line(c    in gms_tcp.connection,
                                  data out varchar2,
                                  len  out integer,
                                  remove_crlf in boolean default false,
                                  peek in boolean default false)
as
begin
    data = gms_tcp.get_line_real(c, remove_crlf, peek, true);
    len = length(data);
end;

--
-- function: read_raw
--
CREATE OR REPLACE PROCEDURE gms_tcp.read_raw(c        in gms_tcp.connection,
                                 data     out raw,
                                 data_len out integer,
                                 len      in integer default 1,
                                 peek     in boolean default false)
as 
begin
    data = gms_tcp.get_raw_real(c, len, peek);
    data_len = length(data);
end;

--
-- function: read_text
--
CREATE OR REPLACE PROCEDURE gms_tcp.read_text(c        in gms_tcp.connection,
                                  data     out varchar2,
                                  data_len out integer,
                                  len      in integer default 1,
                                  peek     in boolean default false)
as
begin
    data = gms_tcp.get_text_real(c, len, peek, true);
    data_len = length(data);
end;

--
-- function: write_line
--
CREATE FUNCTION gms_tcp.write_line(c    in gms_tcp.connection,
                                   data in varchar2)
RETURNS integer
AS 'MODULE_PATHNAME','gms_tcp_write_line'
LANGUAGE C STRICT NOT FENCED;

--
-- function: write_raw
--
CREATE FUNCTION gms_tcp.write_raw_real(c    in gms_tcp.connection,
                                       data in raw,
                                       len  in integer)
RETURNS integer
AS 'MODULE_PATHNAME','gms_tcp_write_raw_real'
LANGUAGE C STRICT NOT FENCED;

create or replace function gms_tcp.write_raw(c    in gms_tcp.connection,
                                             data in raw,
                                             len  in integer default 0)
    returns integer
    language plpgsql
as $function$
begin
    return gms_tcp.write_raw_real(c, data, len);
end;
$function$;

--
-- function: write_text
--
CREATE FUNCTION gms_tcp.write_text_real(c    in gms_tcp.connection,
                                        data in varchar2,
                                        len  in integer default null)
RETURNS integer
AS 'MODULE_PATHNAME','gms_tcp_write_text_real'
LANGUAGE C STRICT NOT FENCED;

create or replace function gms_tcp.write_text(c    in gms_tcp.connection,
                                              data in varchar2,
                                              len  in integer default 0)
    returns integer
    language plpgsql
as $function$
begin
    return gms_tcp.write_text_real(c, data, len);
end;
$function$;
