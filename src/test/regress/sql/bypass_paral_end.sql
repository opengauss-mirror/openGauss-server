--
-- bypass parallel test end
--


reset enable_seqscan;
reset enable_bitmapscan;
reset opfusion_debug_mode;
reset enable_opfusion;
reset enable_indexscan;
select * from bypass_paral order by col1,col2;
select * from bypass_paral2 order by col1,col2;
drop table bypass_paral;
drop table bypass_paral2;

