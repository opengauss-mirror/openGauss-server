--
-- COST MODEL 
--
SET client_min_messages TO WARNING;
-- turn on the switch to stop calculating lossy pages
set sql_beta_feature=disable_bitmap_cost_with_lossy_pages;
-- initial
CREATE SCHEMA cost_model;
SET current_schema = cost_model;
drop table if exists test_bitmap_cost_with_lossy;
create table test_bitmap_cost_with_lossy(id int,c1 int,pad text);
create index test_bitmap_cost_with_lossy_idx_c1 on test_bitmap_cost_with_lossy(c1);
insert into test_bitmap_cost_with_lossy select id,id%250,'sssssssssssssssssssssssssssssssssssss' from (select generate_series(1,300000) id) test_bitmap_cost_with_lossy;
analyze test_bitmap_cost_with_lossy;

-- set work_mem to minimum, ensures that lossy pages are generated in execution plans
set work_mem='64kB';

-- before increasing the cost of lossy pages
explain(COSTS false) select * from test_bitmap_cost_with_lossy where c1=19;

reset sql_beta_feature;
-- after increasing the cost of lossy pages
explain(COSTS false) select * from test_bitmap_cost_with_lossy where c1=19;

-- clean up
reset work_mem;
reset client_min_messages;
DROP table test_bitmap_cost_with_lossy CASCADE;
DROP SCHEMA cost_model CASCADE;

