--
-- bypass parallel test part3
--

start transaction;
select pg_sleep(1);
update bypass_paral set col2= 14 where col1=1 and col2=1;
select * from bypass_paral where col1=1 and col2= 14;
commit;



