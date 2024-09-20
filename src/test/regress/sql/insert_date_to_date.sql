create table tt(col1 date);
insert into tt values('10000-01-28');
insert into tt values(TO_DATE('10000-01-28','yyyy-mm-dd'));
drop table tt;
