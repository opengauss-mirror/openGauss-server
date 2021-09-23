--
-- bypass parallel test part5
--

start transaction;
select pg_sleep(1);
update bypass_paral set col2=2 where col2 is null and col1 is null;
select * from bypass_paral where col1 is null and col2 is not null order by col2, col3;
commit;



