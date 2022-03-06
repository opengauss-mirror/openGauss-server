\! gs_ktool -d all
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS MyCMK CASCADE;
CREATE CLIENT MASTER KEY MyCMK WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY MyCEK WITH VALUES (CLIENT_MASTER_KEY = MyCMK, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);


--行存
--类型
CREATE TABLE test_type_row (id int, 
tinyint_col tinyint  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
smallint_col smallint  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
bigint_col bigint  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
binary_col binary_integer  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
integer_col integer  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),

num_col numeric(10,4) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
ft_col1 float4 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
ft_col2 float8 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
ft_col3 float(3) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
ft_col4 BINARY_DOUBLE ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
ft_col5 integer(6,3) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),


char_col1 char(19)  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
char_col2 char  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
char_col3 character(16)  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
char_col4 character  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
char_col5 nchar(16)  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
char_col6 nchar  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),

char_col7 varchar(17)  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
char_col8 varchar ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),

char_col9 varchar2(10)  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
char_col10 varchar2  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
char_col11 nvarchar2(8),
char_col12 nvarchar2 ,
text_col text  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),

bt_col1 BLOB ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
bt_col2 BYTEA ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC)
) with (orientation=row);

insert into test_type_row values (1, 10, 256, 7894595564, 2147483647, 89, 123456.1223,
 123456.12354, 10.365456, 123456.1234, 10.3214, 321.321, 'ok', 't', 'ok', 'o', 'ok', 'k', 'good', 'ok', 'ok', 'ok', 'ok', 'g', 'text', 'name', 'desdfk');


--列存---待验证
CREATE TABLE test_type_column (id int, 
tinyint_col tinyint  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
smallint_col smallint  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
bigint_col bigint  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
binary_col binary_integer  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
integer_col integer  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),

num_col numeric(10,4) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
ft_col1 float4 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
ft_col2 float8 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
ft_col3 float(3) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
ft_col4 BINARY_DOUBLE ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),

char_col1 char(19)  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
char_col2 character(16)  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
char_col3 nchar(16)  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),

char_col4 varchar(17)  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
char_col5 character  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
char_col6 varchar2(10),
char_col7 nvarchar2(8),
char_col8 char  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),

clob_col clob  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
text_col text  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),

bt_col1 BLOB ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC),
bt_col3 BYTEA ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC)
)with (orientation=column);

insert into test_type_column values (1, 10, 256, 7894595564, 2147483647, 89, 123456.1223,
 123456.12354, 10.365456, 123456.1234, 10.3214, 'ok', 'thank', 'ok', 'ok', 'o', 'ok', 'ok', 'g', 'text', 'good', 'name', 'desdfk');

SELECT * FROM test_type_row order by id;
\d test_type_row
SELECT * FROM test_type_column order by id;
\d test_type_column
DROP TABLE test_type_row;
DROP TABLE test_type_column;


--fail 
CREATE TABLE unsupport_money(id int,
money_col money ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) );

CREATE TABLE unsupport_bool(id int,
bool_col boolean ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) );


CREATE TABLE unsupport_raw(id int,
raw_col RAW ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC));


CREATE TABLE unsupport_name(id int,
name_col name ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) );


CREATE TABLE unsupport_char(id int,
char_col "char"  ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC));

CREATE TABLE unsupport_clob(id int,
clob_col clob ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) );

CREATE TABLE unsupport_decimal(id int,
dec_col decimal(10,4) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC));

CREATE TABLE unsupport_date(id int,
date_col date ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) );

CREATE TABLE unsupport_date1(id int,
da_col time without time zone ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC));

CREATE TABLE unsupport_data2(id int,
data_col time with time zone ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC));

CREATE TABLE unsupport_data3(id int,
data_col timestamp without time zone ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC) );

CREATE TABLE unsupport_data4(id int,
data_col timestamp with time zone ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC));

CREATE TABLE unsupport_data5(id int,
data_col smalldatetime ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = MyCEK, ENCRYPTION_TYPE = DETERMINISTIC));

DROP table IF EXISTS unsupport_money;
DROP table IF EXISTS unsupport_bool;
DROP table IF EXISTS unsupport_raw;
DROP table IF EXISTS unsupport_name;
DROP table IF EXISTS unsupport_char;
DROP table IF EXISTS unsupport_clob;
DROP table IF EXISTS unsupport_decimal;
DROP table IF EXISTS unsupport_date;
DROP table IF EXISTS unsupport_date1;
DROP table IF EXISTS unsupport_data2;
DROP table IF EXISTS unsupport_data3;
DROP table IF EXISTS unsupport_date5;

DROP COLUMN ENCRYPTION KEY MyCEK;
DROP CLIENT MASTER KEY MyCMK;

\! gs_ktool -d all

\! gs_ktool -g
DROP CLIENT MASTER KEY IF EXISTS distributeby_cmk CASCADE;
CREATE CLIENT MASTER KEY distributeby_cmk WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = AES_256_CBC);
CREATE COLUMN ENCRYPTION KEY distributeby_cek WITH VALUES (CLIENT_MASTER_KEY = distributeby_cmk, ALGORITHM = AEAD_AES_256_CBC_HMAC_SHA256);
CREATE TABLE t_distributeby1(
    id_number int,
    name text encrypted with(column_encryption_key = distributeby_cek,encryption_type = DETERMINISTIC),
    data text) distribute by list(name)(slice s1 values (('China'),('Germary')),slice s2 values (('Japan')), slice s3 values (('USA')), slice s4 values (default));
CREATE TABLE t_distributeby2(
    id_number int,
    name text encrypted with(column_encryption_key = distributeby_cek,encryption_type = DETERMINISTIC),
    data text) distribute by hash(name);
CREATE TABLE t_distributeby3(
    id_number int,
    name text,
    data text) distribute by list(name)(slice s1 values (('China')),slice s2 values (('Japan')), slice s3 values (('USA')), slice s4 values ('Germary'),
        slice s5 values ('Israel'), slice s6 values ('India'), slice s7 values ('Peru'), slice s8 values ('Thailand'),
        slice s9 values ('South Africa'), slice s10 values ('New Zealand'), slice s11 values ('Nepal'), slice s12 values (default));
CREATE TABLE t_distributeby4(
    id_number int,
    name text,
    data text encrypted with(column_encryption_key = distributeby_cek,encryption_type = DETERMINISTIC)) 
distribute by list(name)(slice s1 values (('China')),slice s2 values (('Japan')), slice s3 values (('USA')), slice s4 values ('Germary'),
    slice s5 values ('Israel'), slice s6 values ('India'), slice s7 values ('Peru'), slice s8 values ('Thailand'),
    slice s9 values ('South Africa'), slice s10 values ('New Zealand'), slice s11 values ('Nepal'), slice s12 values (default));
create table ce_t1 (id BYTEAWITHOUTORDERWITHEQUALCOL);
create table ce_t2 (id BYTEAWITHOUTORDERCOL);

DROP table IF EXISTS ce_t1;
DROP table IF EXISTS ce_t2;
DROP table IF EXISTS t_distributeby1;
DROP table IF EXISTS t_distributeby2;
DROP table IF EXISTS t_distributeby3;
DROP table IF EXISTS t_distributeby4;
DROP COLUMN ENCRYPTION KEY IF EXISTS distributeby_cek;
DROP CLIENT MASTER KEY IF EXISTS distributeby_cmk;
\! gs_ktool -d all