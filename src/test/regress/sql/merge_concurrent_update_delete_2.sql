CREATE SCHEMA merge_concurrent_update_delete_2;
SET current_schema = merge_concurrent_update_delete_2;

-- test merge with concurrent update/delete on partition table(now row-movement)
CREATE TABLE merge_1(a int, b int, c int) partition by range(a) (partition p1 values less than(10), partition p2 values less than(maxvalue)) ENABLE ROW MOVEMENT;
CREATE TABLE merge_2(a int);
insert into merge_1 values(1,1,1);
insert into merge_2 values(1);

-- concurrent update on join condition row, lead row movement
\parallel on 2
begin
    update merge_1 set a=22 where a=1; -- row movement, will be delete and insert
    perform pg_sleep(3);
end;
/

-- this transaction will failed cause first transaction doing a row movement update, which lead the row be deleted and insert to another partition
begin
    perform pg_sleep(1);
    merge INTO merge_1 p1 using (select * from merge_2) p2 on (p1.a=p2.a) when matched then update set p1.b=66 when NOT MATCHED THEN INSERT (a,b,c) values(8,8,8);
end;
/
\parallel off

select * from merge_1 order by a; -- one rows(22,1,1)
delete from merge_1;

insert into merge_1 values(1,1,1);
-- concurrent update on join condition row, no row movement, after update, the row doesn't match, so the merge should go to not match condition
\parallel on 2
begin
    update merge_1 set a=9 where a=1;
    perform pg_sleep(3);
end;
/

begin
    perform pg_sleep(1);
    merge INTO merge_1 p1 using (select * from merge_2) p2 on (p1.a=p2.a) when matched then update set p1.b=66 when NOT MATCHED THEN INSERT (a,b,c) values(8,8,8);
end;
/
\parallel off
select * from merge_1 order by a; -- two rows(merge not matched)

delete from merge_1;

insert into merge_1 values(1,1,1);
-- concurrent update on non-join condition row, no row movement, after update, the row still match, so the merge should go to match condition
\parallel on 2
begin
    update merge_1 set b=88 where a=1;
    perform pg_sleep(3);
end;
/

begin
    perform pg_sleep(1);
    merge INTO merge_1 p1 using (select * from merge_2) p2 on (p1.a=p2.a) when matched then update set p1.b=66 when NOT MATCHED THEN INSERT (a,b,c) values(8,8,8);
end;
/
\parallel off
select * from merge_1 order by a; -- one rows(merge matched)

drop schema merge_concurrent_update_delete_2 cascade;