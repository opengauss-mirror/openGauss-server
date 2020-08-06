--insert data
create or replace function insert_mm_02(dat text) returns void as $$
declare
	times integer :=1;
begin
	loop
		insert into partition_interval_parallel values(1, 1, dat);
		times = times + 1;
		if times > 500 then
			exit;
		end if;
  end loop;
end;
$$ language plpgsql;


select insert_mm_02('2020-05-1');
select insert_mm_02('2020-06-1');
select insert_mm_02('2020-07-1');
select insert_mm_02('2020-08-1');
select insert_mm_02('2020-09-1');
select insert_mm_02('2020-10-1');
select insert_mm_02('2020-11-1');
