--
-- bypass parallel test part6
--

start transaction;
select pg_sleep(1);
select * from bypass_paral where col1=1 and col2 is null;
update bypass_paral set col3='pp4' where col2 is null and col1=1;
select * from bypass_paral where col1=1 and col2 is null;
commit;
