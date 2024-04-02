/* contrib/gms_output/gms_output--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gms_output" to load this file. \quit

CREATE SCHEMA gms_output;
GRANT USAGE ON SCHEMA gms_output TO PUBLIC;

--SUPPORT TYPE CHARARR
CREATE TYPE gms_output.CHARARR IS TABLE OF VARCHAR2(32767);

CREATE OR REPLACE FUNCTION gms_output.enable(buff_size int default 20000)
RETURNS void
AS 'MODULE_PATHNAME','gms_output_enable'
LANGUAGE C;
--COMMENT ON FUNCTION gms_output.enable(int) IS 'This function enables calls to PUT, PUT_LINE, NEW_LINE, GET_LINE, and GET_LINES.';

CREATE OR REPLACE FUNCTION gms_output.disable()
RETURNS void
AS 'MODULE_PATHNAME','gms_output_disable'
LANGUAGE C;
--COMMENT ON FUNCTION gms_output.disable() IS 'This function disables calls to PUT, PUT_LINE, NEW_LINE, GET_LINE, and GET_LINES.';

CREATE OR REPLACE FUNCTION gms_output.put(text)
RETURNS void
AS 'MODULE_PATHNAME','gms_output_put'
LANGUAGE C
RETURNS NULL ON NULL INPUT;
--COMMENT ON FUNCTION gms_output.put(text) IS 'This function places a partial line in the buffer.';

CREATE OR REPLACE FUNCTION gms_output.put_line(text)
RETURNS void
AS 'MODULE_PATHNAME','gms_output_put_line'
LANGUAGE C
RETURNS NULL ON NULL INPUT;
--COMMENT ON FUNCTION gms_output.put_line(text) IS 'This function places a line in the buffer.';

CREATE OR REPLACE FUNCTION gms_output.new_line()
RETURNS void
AS 'MODULE_PATHNAME','gms_output_new_line'
LANGUAGE C;
--COMMENT ON FUNCTION gms_output.new_line() IS 'This function puts an end-of-line marker.';

CREATE OR REPLACE FUNCTION gms_output.get_line(line INOUT text, status INOUT INTEGER)
RETURNS RECORD
AS 'MODULE_PATHNAME','gms_output_get_line'
LANGUAGE C;
--COMMENT ON FUNCTION gms_output.get_line(line INOUT text, status INOUT INTEGER) IS 'This function retrieves a single line of buffered information..';

CREATE OR REPLACE FUNCTION gms_output.get_lines(lines INOUT text[], numlines INOUT INTEGER)
RETURNS RECORD
AS 'MODULE_PATHNAME','gms_output_get_lines'
LANGUAGE C;
--COMMENT ON FUNCTION gms_output.get_lines(lines INOUT text[], numlines INOUT INTEGER) IS 'This function retrieves an array of lines from the buffer.';
