CREATE TABLE detail_1(
     id              int,
     name            text,
     person          text
);
create table detail_2(
     id              int,
     name            text,
     value           text,
     primary key(id)
);
insert into detail_1 values(1, 'a', 'aaa');
insert into detail_2 values(1, 'b', 'bbb');
explain (costs on, verbose on, analyse on, cpu on, detail off, buffers on) select * from detail_1;
explain (costs on, verbose on, analyse on, cpu on, detail on, buffers on) select * from detail_2;
explain (detail on) select * from detail_1;
explain (detail off) select * from detail_2;
drop table detail_1;
drop table detail_2;