CREATE TABLE regex_1 (
     id              int,
     name            text,
     person          text
);
create table regex_2 (
     id              int,
     name            text,
     value           text,
     primary key(id)
);
insert into regex_1 values(1, 'a', 'aaa');
insert into regex_2 values(1, 'b', 'bbb');
explain (costs on, verbose on, nodes on, analyse on, cpu on, detail on, buffers on) select * from regex_1;
explain (costs on, verbose on, nodes on, analyse on, cpu on, detail on, buffers on) select * from regex_2;
explain (costs on, verbose on, nodes on) select * from (select name from regex_1 intersect select name from regex_2 order by name) limit 10;
explain (costs on, verbose on, nodes on) select * from (select name from regex_1 union select name from regex_2 order by name) limit 10;
explain (costs on, verbose on, nodes on) insert into regex_1 (select * from regex_2);
set enable_hashjoin=on;
set enable_nestloop=off;
set enable_mergejoin=off;
explain (costs on, verbose on, nodes on) select * from regex_1, regex_2 where regex_1.name=regex_2.name;
set enable_hashjoin=off;
set enable_nestloop=off;
set enable_mergejoin=on;
explain (costs on, verbose on, nodes on) select * from regex_1, regex_2 where regex_1.name=regex_2.name;
set enable_hashjoin=on;
set enable_nestloop=on;
set enable_mergejoin=on;
explain (costs on, verbose on, nodes on) select * from regex_2, regex_1 where regex_1.id>regex_2.id;
explain (costs on, verbose on, nodes on) select avg(id) from regex_1;
explain (costs on, verbose on, nodes on) select sum(id)+2 from regex_2 where value is not NULL group by name having sum(id)>0;
explain (costs on, verbose on, nodes on) with recursive t(n) as (
values(1)
union all
select n+1 from t where n<100
)
select sum(n) from t;
