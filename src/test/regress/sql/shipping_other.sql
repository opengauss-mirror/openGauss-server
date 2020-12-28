set current_schema='shipping_schema';
explain (num_costs off) 
select count(distinct(c1::numeric(10,5))) , c1::numeric(10,5), count(distinct(t6.d)) from t6 , (select random() c1 from t6)b group by c1::numeric(10,5);

------shipping in merge into----------
create table target(a int, b int);
insert into seq_t2 select generate_series(1,10), generate_series(1,10), generate_series(1,100);

explain (costs off, verbose on)
merge into seq_t1 using seq_t2
           on (seq_t1.a = seq_t2.a)
           when matched then update set b = 1
           when not matched then insert (a) values (1);

merge into seq_t1 using seq_t2
           on (seq_t1.a = seq_t2.a)
           when matched then update set b = 1
           when not matched then insert (a) values (1);

insert into target select 1, c from seq_t1;
select count(*) from seq_t1 where c not in (select b from target);

explain (costs off, verbose on)
merge into seq_t3 using seq_t2
           on (seq_t3.a = seq_t2.a)
           when matched then update set b = 1
           when not matched then insert (a) values (1);

merge into seq_t3 using seq_t2
           on (seq_t3.a = seq_t2.a)
           when matched then update set b = 1
           when not matched then insert (a) values (1);

truncate target;
insert into target select 1, c from seq_t3;
select count(*) from seq_t3 where c not in (select b from target);

truncate seq_t1;
truncate seq_t3;

explain (costs off, verbose on)
merge into seq_t1 using seq_t2
           on (seq_t1.a = seq_t2.a)
           when not matched then insert (a) values (1);

merge into seq_t1 using seq_t2
           on (seq_t1.a = seq_t2.a)
           when not matched then insert (a) values (1);

truncate target;
insert into target select 1, c from seq_t1;
select count(*) from seq_t1 where c not in (select b from target);

explain (costs off, verbose on)
merge into seq_t3 using seq_t2
           on (seq_t3.a = seq_t2.a)
           when not matched then insert (a) values (1);

merge into seq_t3 using seq_t2
           on (seq_t3.a = seq_t2.a)
           when not matched then insert (a) values (1);

truncate target;
insert into target select 1, c from seq_t3;
select count(*) from seq_t3 where c not in (select b from target);

truncate seq_t3;
explain (costs off, verbose on)
merge into seq_t3 using seq_t2
           on (seq_t3.a = seq_t2.a)
           when matched then update set b = 1
           when not matched then insert (a) values (nextval('seq1'));

merge into seq_t3 using seq_t2
           on (seq_t3.a = seq_t2.a)
           when matched then update set b = 1
           when not matched then insert (b) values (nextval('seq1'));

truncate target;
insert into target select 1, a from seq_t3;
select count(*) from seq_t3 where a not in (select b from target);

truncate target;
insert into target select 1, b from seq_t3;
select count(*) from seq_t3 where b not in (select b from target);

truncate target;
insert into target select 1, c from seq_t3;
select count(*) from seq_t3 where c not in (select b from target);

drop table target;

create table test_random_shippable(id int, id2 int) ;
explain verbose delete from test_random_shippable where id =(select (random() *10000)::int);


create table test_random_rep ( a int ,b int) distribute by replication;
insert into  test_random_rep values(1,2);
explain update test_random_rep set b = ceil(random()*1000);

drop table test_random_shippable;
drop table test_random_rep;
