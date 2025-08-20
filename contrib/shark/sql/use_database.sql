create database testd with dbcompatibility = 'd';
\c testd
create extension shark;
use testd;
use test1;
use public;
create schema test_schema;
use test_schema;
create database test2;
use test2;

create database testa;
\c testa
create schema test_schema;
use test_schema;
use public;
use test1;
use testa;
create database test4;
use test4;

\c postgres
drop database testd;
drop database testa;
drop database test2;
drop database test4;
