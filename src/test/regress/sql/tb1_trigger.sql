drop table if exists test_trigger_des_tbl cascade;
drop sequence if exists dump_seq_1;
CREATE TABLE test_trigger_des_tbl(id1 INT, id2 INT, id3 INT);
create sequence dump_seq_1;
CREATE OR REPLACE FUNCTION tri_insert_func() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
INSERT INTO test_trigger_des_tbl VALUES(nextval('dump_seq_1'),currval('dump_seq_1'),currval('dump_seq_1'));
RETURN NEW;
END
$$ LANGUAGE PLPGSQL;
drop trigger if exists insert_trigger on tb1 cascade;
create trigger insert_trigger after insert on tb1
        for each row 
		execute procedure tri_insert_func();
		
