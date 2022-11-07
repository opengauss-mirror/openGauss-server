SET CURRENT_SCHEMA TO segment_subpartition_select;

select * from range_list order by 1, 2, 3, 4;

select * from range_list where user_no is not null order by 1, 2, 3, 4;
select * from range_list where user_no is not null and dept_code = user_no order by 1, 2, 3, 4;
select * from range_list where user_no is not null and dept_code in ('2') order by 1, 2, 3, 4;
select * from range_list where user_no is not null and dept_code  <> '2' order by 1, 2, 3, 4;
select * from range_list partition (p_201901) order by 1, 2, 3, 4;
select * from range_list partition (p_201902) order by 1, 2, 3, 4;
select * from range_list where user_no is not null and dept_code  <> '2' UNION ALL select * from range_list partition (p_201902) order by 1, 2, 3, 4;
select * from range_list where user_no is not null and dept_code  <> '2' UNION ALL select * from range_list partition (p_201902) where dept_code in ('2') order by 1, 2, 3, 4;

select * from range_hash order by 1, 2, 3, 4;

select * from range_hash where user_no is not null order by 1, 2, 3, 4;
select * from range_hash where user_no is not null and dept_code = user_no order by 1, 2, 3, 4;
select * from range_hash where user_no is not null and dept_code in ('2') order by 1, 2, 3, 4;
select * from range_hash where user_no is not null and dept_code  <> '2' order by 1, 2, 3, 4;
select * from range_hash partition (p_201901) order by 1, 2, 3, 4;
select * from range_hash partition (p_201902) order by 1, 2, 3, 4;
select * from range_hash where user_no is not null and dept_code  <> '2' UNION ALL select * from range_hash partition (p_201902) order by 1, 2, 3, 4;
select * from range_hash where user_no is not null and dept_code  <> '2' UNION ALL select * from range_hash partition (p_201902) where dept_code in ('2') order by 1, 2, 3, 4;

select * from range_range order by 1, 2, 3, 4;

select * from range_range where user_no is not null order by 1, 2, 3, 4;
select * from range_range where user_no is not null and dept_code = user_no order by 1, 2, 3, 4;
select * from range_range where user_no is not null and dept_code in ('2') order by 1, 2, 3, 4;
select * from range_range where user_no is not null and dept_code  <> '2' order by 1, 2, 3, 4;
select * from range_range partition (p_201901) order by 1, 2, 3, 4;
select * from range_range partition (p_201902) order by 1, 2, 3, 4;
select * from range_range where user_no is not null and dept_code  <> '2' UNION ALL select * from range_range partition (p_201902) order by 1, 2, 3, 4;
select * from range_range where user_no is not null and dept_code  <> '2' UNION ALL select * from range_range partition (p_201902) where dept_code in ('2') order by 1, 2, 3, 4;

select * from view_temp;
--error
select * from view_temp partition (p_201901);
select * from view_temp partition (p_201902);
--join normal table 
select * from range_list left join t1 on range_list.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_list left join t1 on range_list.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_list right join t1 on range_list.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_list right join t1 on range_list.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_list full join t1 on range_list.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_list full join t1 on range_list.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_list inner join t1 on range_list.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_list inner join t1 on range_list.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;


select * from range_hash left join t1 on range_hash.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_hash left join t1 on range_hash.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_hash right join t1 on range_hash.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_hash right join t1 on range_hash.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_hash full join t1 on range_hash.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_hash full join t1 on range_hash.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_hash inner join t1 on range_hash.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_hash inner join t1 on range_hash.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;


select * from range_range left join t1 on range_range.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_range left join t1 on range_range.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_range right join t1 on range_range.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_range right join t1 on range_range.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_range full join t1 on range_range.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_range full join t1 on range_range.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

select * from range_range inner join t1 on range_range.month_code = t1.c1 order by 1, 2, 3, 4, 5, 6;
select * from range_range inner join t1 on range_range.month_code = t1.c1 where dept_code = 2 order by 1, 2, 3, 4, 5, 6;

--join range_list and range_hash

select * from range_list left join range_hash on range_list.month_code = range_hash.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_list left join range_hash on range_list.month_code = range_hash.month_code where range_list.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_list right join range_hash on range_list.month_code = range_hash.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_list right join range_hash on range_list.month_code = range_hash.month_code where range_list.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_list full join range_hash on range_list.month_code = range_hash.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_list full join range_hash on range_list.month_code = range_hash.month_code where range_list.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_list inner join range_hash on range_list.month_code = range_hash.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_list inner join range_hash on range_list.month_code = range_hash.month_code where range_list.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

--join range_hash and range_range

select * from range_hash left join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash left join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_hash right join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash right join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_hash full join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash full join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_hash inner join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash inner join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

--join range_hash and range_range

select * from range_hash left join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash left join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_hash right join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash right join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_hash full join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash full join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from range_hash inner join range_range on range_hash.month_code = range_range.month_code order by 1, 2, 3, 4, 5, 6, 7, 8;
select * from range_hash inner join range_range on range_hash.month_code = range_range.month_code where range_hash.dept_code = 2 order by 1, 2, 3, 4, 5, 6, 7, 8;

select * from pjade subpartition(hrp1_1) union select * from cjade order by 1,2,3;
select * from pjade subpartition(hrp1_1) p union select * from cjade order by 1,2,3;
select * from pjade subpartition(hrp1_1) union select * from cjade order by 1,2,3;
select * from pjade subpartition(hrp1_1) p union select * from cjade order by 1,2,3;