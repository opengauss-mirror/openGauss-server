UPDATE tbl_insert_003 SET R5_ID=repeat('a', 2570) WHERE R3_ID='203000000000030345';
UPDATE tbl_insert_004 SET R5_ID=repeat('a', 2570) WHERE R3_ID='203000000000030345';
delete from tbl_insert_003 where R3_ID = '203000000000030466';

insert into tbl_insert_004 select * from tbl_insert_003 WHERE RS_ID = 1;

