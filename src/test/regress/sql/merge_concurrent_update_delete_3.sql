CREATE SCHEMA merge_concurrent_update_delete_3;
SET current_schema = merge_concurrent_update_delete_3;

-- test merge with concurrent update/delete
CREATE TABLE merge_1(a int, b int, c int);
CREATE TABLE merge_2(a int);
insert into merge_1 values(1,1,1);
insert into merge_2 values(1);

-- concurrent delete on join condition row, after delete, the row doesn't match, so the merge should go to not match condition
\parallel on 2
begin
    delete from merge_1 where a=1;
    perform pg_sleep(3);
end;
/

begin
    perform pg_sleep(1);
    merge INTO merge_1 p1 using (select * from merge_2) p2 on (p1.a=p2.a) when matched then update set p1.b=66 when NOT MATCHED THEN INSERT (a,b,c) values(8,8,8);
end;
/
\parallel off

select * from merge_1 order by a; -- one new rows(merge not matched)
delete from merge_1;

insert into merge_1 values(1,1,1);
insert into merge_1 values(2,2,2);
-- concurrent delete on non-join condition row, after delete, the row still match, so the merge should go to match condition
\parallel on 2
begin
    delete from merge_1 where a=2;
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

drop schema merge_concurrent_update_delete_3 cascade;