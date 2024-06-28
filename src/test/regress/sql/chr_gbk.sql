--test chr function in GBK encoded database
CREATE DATABASE db1 ENCODING 'GBK' TEMPLATE template0 lc_collate = 'zh_CN.gbk' lc_ctype = 'zh_CN.gbk' ;
\c db1
set client_encoding = 'UTF8';
select chr(52144);
select chr(33088); --0x8140
select chr(65278); --0xfefe

--error
select chr(32146);
select chr(33087);
select chr(65279);
select chr(33151); --0x817f
select chr(65151); --0xfe7f
\c regression
clean connection to all force for database db1;
drop database db1;
