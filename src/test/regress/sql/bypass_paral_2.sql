--
-- bypass parallel test part2
--

start transaction;
select pg_sleep(1);
update bypass_paral set col3= 14 where col1=0 and col2=0;
select * from bypass_paral where col1=0 and col2=0;
commit;

start transaction;
update bypass_paral2 set col3='pp3',col4='pp4' where col2=0 and col1=0;
select * from bypass_paral2 where col2=0 and col1=0;
commit;
