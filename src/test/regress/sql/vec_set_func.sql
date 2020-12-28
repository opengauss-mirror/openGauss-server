create table hl_test002(a int,b varchar2(15), c varchar2(15)); 
insert into hl_test002 values(1,'gauss,ap', 'xue,dong,pu'); 
insert into hl_test002 values(1,'gauss,ap', NULL); 
insert into hl_test002 values(1,'xue,dong,pu', 'gauss,ap,db'); 
insert into hl_test002 values(1,'xue,dong,pu', NULL); 
insert into hl_test002 values(2,'xi,an', 'wang,,rui'); 
insert into hl_test002 values(2,'xi,an', NULL); 
insert into hl_test002 values(2,'wang,,rui', 'xi,an'); 
insert into hl_test002 values(2,'wang,,rui', NULL);

create table hl_test001(a int,b varchar2(15), c varchar2(15)) with (ORIENTATION = COLUMN); 
insert into hl_test001 select * from hl_test002;

select a,b,c,regexp_split_to_table(b,E',') from hl_test001 order by 1, 2, 3, 4;
select a,b,c,regexp_split_to_table(b,NULL) from hl_test001 order by 1, 2, 3, 4;
select a,b,c,regexp_split_to_table(b,E','), regexp_split_to_table(c,E',') from hl_test001 order by 1, 2, 3, 4, 5;
select regexp_split_to_table(b,E','), generate_series(1, 3) from hl_test001;

select a,b,c,regexp_split_to_table(regexp_split_to_table(b,E','), E'u') from hl_test001 order by 1, 2, 3, 4;
select a,b,c,substring(regexp_split_to_table(b,E','), 1, 100) from hl_test001 order by 1, 2, 3, 4;
select a,b,c,regexp_split_to_table(substring(b,1, 100), E',') from hl_test001 order by 1, 2, 3, 4;

drop table hl_test001;
drop table hl_test002;

