set explain_perf_mode=pretty;
/*
 * 工行场景1测试
 */
explain (costs false) 
with recursive cte as
(
    select area_code
    from gcms.gcm_mag_area_h
    where area_code='100000'
    union all
    select h.area_code
    from cte
    join gcms.gcm_mag_area_h h
    on h.belong_area_code=cte.area_code
)
select area_code, count(*) from cte group by 1 order by 1 limit 20;

with recursive cte as
(
    select area_code
    from gcms.gcm_mag_area_h
    where area_code='100000'
    union all
    select h.area_code
    from cte
    join gcms.gcm_mag_area_h h
    on h.belong_area_code=cte.area_code
)
select area_code, count(*) from cte group by 1 order by 1 limit 20;

explain (costs false) 
with recursive cte as
(
    select area_code
    from gcms.gcm_mag_area_h
    where area_code='100000'
    union all
    select h.area_code
    from cte
    join gcms.gcm_mag_area_h h
    on h.belong_area_code=cte.area_code
)
select area_code, count(*) from cte group by 1 order by 1 limit 20;

with recursive cte as
(
    select area_code
    from gcms.gcm_mag_area_h
    where area_code='100000'
    union all
    select h.area_code
    from cte
    join gcms.gcm_mag_area_h h
    on h.belong_area_code=cte.area_code
)
select area_code, count(*) from cte group by 1 order by 1 limit 20;

/* 场景2 */
explain (costs false) 
with recursive cte as
(
    select area_code, area_code as chain, 1 as level /*chain为路径,例如>101>201>203;level为层级，从1开始*/
    from gcms.gcm_mag_area_h
    where area_code='100000'
    union all
    select h.area_code,cast(cte.chain||'>'||h.area_code as varchar2(30)),cte.level+1
    from cte
    join gcms.gcm_mag_area_h h
    on h.belong_area_code=cte.area_code and cte.level <3 and h.area_code not in (select regexp_split_to_table(cte.chain,'>') )/*如果是循环的结构，可以用chain来退出循环；此处cte.level<6表示到第6层停止*/
)
select * from cte order by 1 limit 20;

with recursive cte as
(
    select area_code, area_code as chain, 1 as level /*chain为路径,例如>101>201>203;level为层级，从1开始*/
    from gcms.gcm_mag_area_h
    where area_code='100000'
    union all
    select h.area_code,cast(cte.chain||'>'||h.area_code as varchar2(30)),cte.level+1
    from cte
    join gcms.gcm_mag_area_h h
    on h.belong_area_code=cte.area_code and cte.level <3 and h.area_code not in (select regexp_split_to_table(cte.chain,'>') )/*如果是循环的结构，可以用chain来退出循环；此处cte.level<6表示到第6层停止*/
)
select * from cte order by 1 limit 20;

explain (costs false) 
with recursive cte as
(
    select area_code, area_code as chain, 1 as level /*chain为路径,例如>101>201>203;level为层级，从1开始*/
    from gcms.gcm_mag_area_h
    where area_code='100000'
    union all
    select h.area_code,cast(cte.chain||'>'||h.area_code as varchar2(30)),cte.level+1
    from cte
    join gcms.gcm_mag_area_h h
    on h.belong_area_code=cte.area_code and cte.level <3 and h.area_code not in (select regexp_split_to_table(cte.chain,'>') )/*如果是循环的结构，可以用chain来退出循环；此处cte.level<6表示到第6层停止*/
)
select * from cte order by 1 limit 20;

with recursive cte as
(
    select area_code, area_code as chain, 1 as level /*chain为路径,例如>101>201>203;level为层级，从1开始*/
    from gcms.gcm_mag_area_h
    where area_code='100000'
    union all
    select h.area_code,cast(cte.chain||'>'||h.area_code as varchar2(30)),cte.level+1
    from cte
    join gcms.gcm_mag_area_h h
    on h.belong_area_code=cte.area_code and cte.level <3 and h.area_code not in (select regexp_split_to_table(cte.chain,'>') )/*如果是循环的结构，可以用chain来退出循环；此处cte.level<6表示到第6层停止*/
)
select * from cte order by 1 limit 20;


/* 场景3 */
explain (costs false) 
with recursive cte as
(
    select 1 as id,district_code,district_name,district_code as district_code_root,belong_district from gcms.gcc_mag_district_h
    union all
    select cte.id+1,h.district_code,h.district_name,cte.district_code_root,h.belong_district from cte join gcms.gcc_mag_district_h h on h.district_code=cte.belong_district
)
select id,district_code,district_name,district_code_root from cte order by 1,2,3 limit 20;

with recursive cte as
(
    select 1 as id,district_code,district_name,district_code as district_code_root,belong_district from gcms.gcc_mag_district_h
    union all
    select cte.id+1,h.district_code,h.district_name,cte.district_code_root,h.belong_district from cte join gcms.gcc_mag_district_h h on h.district_code=cte.belong_district
)
select id,district_code,district_name,district_code_root from cte order by 1,2,3 limit 20;

explain (costs false) 
with recursive cte as
(
    select 1 as id,district_code,district_name,district_code as district_code_root,belong_district from gcms.gcc_mag_district_h
    union all
    select cte.id+1,h.district_code,h.district_name,cte.district_code_root,h.belong_district from cte join gcms.gcc_mag_district_h h on h.district_code=cte.belong_district
)
select id,district_code,district_name,district_code_root from cte order by 1,2,3 limit 20;

with recursive cte as
(
    select 1 as id,district_code,district_name,district_code as district_code_root,belong_district from gcms.gcc_mag_district_h
    union all
    select cte.id+1,h.district_code,h.district_name,cte.district_code_root,h.belong_district from cte join gcms.gcc_mag_district_h h on h.district_code=cte.belong_district
)
select id,district_code,district_name,district_code_root from cte order by 1,2,3 limit 20;

/* 场景4 */
explain (costs false) 
select dm,(
	with recursive cte as
	(
		select distinct f.dm, f.sj_dm, f.name from test_rec_1 f where f.sj_dm=t1.sj_dm
		union all
		select distinct f.dm, f.sj_dm, f.name from test_rec_1 f join cte on
		f.dm=cte.sj_dm
	)
select distinct dm from cte where sj_dm=7) dm_2,
sj_dm, name from test_rec t1 order by 1,2,3 limit 20;

select dm,(
	with recursive cte as
	(
		select distinct f.dm, f.sj_dm, f.name from test_rec_1 f where f.sj_dm=t1.sj_dm
		union all
		select distinct f.dm, f.sj_dm, f.name from test_rec_1 f join cte on
		f.dm=cte.sj_dm
	)
select distinct dm from cte where sj_dm=7) dm_2,
sj_dm, name from test_rec t1 order by 1,2,3 limit 20;

explain (costs false) 
select dm,(
	with recursive cte as
	(
		select distinct f.dm, f.sj_dm, f.name from test_rec_1 f where f.sj_dm=t1.sj_dm
		union all
		select distinct f.dm, f.sj_dm, f.name from test_rec_1 f join cte on
		f.dm=cte.sj_dm
	)
select distinct dm from cte where sj_dm=7) dm_2,
sj_dm, name from test_rec t1 order by 1,2,3 limit 20;

select dm,(
	with recursive cte as
	(
		select distinct f.dm, f.sj_dm, f.name from test_rec_1 f where f.sj_dm=t1.sj_dm
		union all
		select distinct f.dm, f.sj_dm, f.name from test_rec_1 f join cte on
		f.dm=cte.sj_dm
	)
select distinct dm from cte where sj_dm=7) dm_2,
sj_dm, name from test_rec t1 order by 1,2,3 limit 20;

/* 场景5 */
/* 场景6 */

reset explain_perf_mode;
