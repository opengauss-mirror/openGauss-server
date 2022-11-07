SET CURRENT_SCHEMA TO segment_subpartition_scan;

select * from range_list order by 1, 2, 3, 4;

select * from range_list where month_code = '201902' order by 1, 2, 3, 4;
select * from range_list where dept_code = '1' order by 1, 2, 3, 4;
select * from range_list where user_no = '1' order by 1, 2, 3, 4;
