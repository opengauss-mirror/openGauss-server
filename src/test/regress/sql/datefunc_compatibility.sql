select add_months('2018-02-28',3) from dual;
select add_months('2018-02-27',3) from dual;
select add_months('2018-01-31',1) from dual;
select add_months('2018-01-31',2) from dual;
select add_months('2018-01-31',4) from dual;

set behavior_compat_options='end_month_calculate';
select add_months('2018-02-28',3) from dual;
select add_months('2018-02-27',3) from dual;
select add_months('2018-01-31',1) from dual;
select add_months('2018-01-31',2) from dual;
select add_months('2018-01-31',4) from dual;

set behavior_compat_options='';
select add_months('2018-02-28',3) from dual;
select add_months('2018-02-27',3) from dual;
select add_months('2018-01-31',1) from dual;
select add_months('2018-01-31',2) from dual;
select add_months('2018-01-31',4) from dual;
