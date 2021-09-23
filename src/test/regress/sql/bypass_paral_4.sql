--
-- bypass parallel test part4
--

start transaction;
select pg_sleep(1);
update bypass_paral set col3='pp3' where col1 is null and col2 is not null;
select * from bypass_paral where col1 is null and col2 is not null order by col2, col3;
commit;



