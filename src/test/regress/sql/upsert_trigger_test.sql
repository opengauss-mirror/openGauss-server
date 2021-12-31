\c upsert
SET CURRENT_SCHEMA TO upsert_test;

CREATE FUNCTION upsert_before_func()
  RETURNS TRIGGER language plpgsql AS
$$
BEGIN
  IF (TG_OP = 'UPDATE') THEN
    RAISE warning 'before update (old): %', old.*::TEXT;
    RAISE warning 'before update (new): %', new.*::TEXT;
  elsIF (TG_OP = 'INSERT') THEN
    RAISE warning 'before insert (new): %', new.*::TEXT;
    IF NEW.key % 2 = 0 THEN
      NEW.color := NEW.color || ' trig modified';
      RAISE warning 'before insert (new, modified): %', new.*::TEXT;
    END IF;
  END IF;
  RETURN new;
END;
$$;
CREATE TRIGGER upsert_before_trig BEFORE INSERT OR UPDATE ON t_trigger
  FOR EACH ROW EXECUTE procedure upsert_before_func();

CREATE FUNCTION upsert_after_func()
  RETURNS TRIGGER language plpgsql AS
$$
BEGIN
  IF (TG_OP = 'UPDATE') THEN
    RAISE warning 'after update (old): %', old.*::TEXT;
    RAISE warning 'after update (new): %', new.*::TEXT;
  elsIF (TG_OP = 'INSERT') THEN
    RAISE warning 'after insert (new): %', new.*::TEXT;
  END IF;
  RETURN null;
END;
$$;
CREATE TRIGGER upsert_after_trig AFTER INSERT OR UPDATE ON t_trigger
  FOR EACH ROW EXECUTE procedure upsert_after_func();

INSERT INTO t_trigger values(1, 'black') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;
INSERT INTO t_trigger values(2, 'red') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;
INSERT INTO t_trigger values(3, 'orange') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;
INSERT INTO t_trigger values(4, 'green') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;
INSERT INTO t_trigger values(5, 'purple') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;
INSERT INTO t_trigger values(6, 'white') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;
INSERT INTO t_trigger values(7, 'pink') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;
INSERT INTO t_trigger values(8, 'yellow') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;

SELECT * FROM t_trigger ORDER BY key;

INSERT INTO t_trigger values(2, 'black') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;
INSERT INTO t_trigger values(3, 'red') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;
INSERT INTO t_trigger values(4, 'orange') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;
INSERT INTO t_trigger values(5, 'green') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;
INSERT INTO t_trigger values(6, 'purple') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;
INSERT INTO t_trigger values(7, 'white') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;
INSERT INTO t_trigger values(8, 'pink') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;
INSERT INTO t_trigger values(9, 'yellow') ON DUPLICATE KEY UPDATE color = 'updated ' || t_trigger.color;

SELECT * FROM t_trigger ORDER BY key;

create table test_range_pt (key int primary key, color text)
partition by range(key)
(
	partition p1 values less than (2000),
	partition p2 values less than (3000),
	partition p3 values less than (4000),
	partition p4 values less than (5000),
	partition p5 values less than (6000)
)ENABLE ROW MOVEMENT;

CREATE TRIGGER upsert_before_trig_for_pt BEFORE INSERT OR UPDATE ON test_range_pt
  FOR EACH ROW EXECUTE procedure upsert_before_func();

CREATE TRIGGER upsert_after_trig_for_pt AFTER INSERT OR UPDATE ON test_range_pt
  FOR EACH ROW EXECUTE procedure upsert_after_func();

INSERT INTO test_range_pt values(1, 'black') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;
INSERT INTO test_range_pt values(2, 'red') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;
INSERT INTO test_range_pt values(3, 'orange') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;
INSERT INTO test_range_pt values(4, 'green') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;
INSERT INTO test_range_pt values(5, 'purple') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;
INSERT INTO test_range_pt values(6, 'white') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;
INSERT INTO test_range_pt values(7, 'pink') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;
INSERT INTO test_range_pt values(8, 'yellow') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;

SELECT * FROM test_range_pt ORDER BY key;

INSERT INTO test_range_pt values(2, 'black') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;
INSERT INTO test_range_pt values(3, 'red') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;
INSERT INTO test_range_pt values(4, 'orange') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;
INSERT INTO test_range_pt values(5, 'green') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;
INSERT INTO test_range_pt values(6, 'purple') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;
INSERT INTO test_range_pt values(7, 'white') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;
INSERT INTO test_range_pt values(8, 'pink') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;
INSERT INTO test_range_pt values(9, 'yellow') ON DUPLICATE KEY UPDATE color = 'updated ' || test_range_pt.color;

DELETE FROM test_range_pt;
SELECT * FROM test_range_pt ORDER BY key;

INSERT INTO test_range_pt values(2, 'black')  ON DUPLICATE KEY UPDATE nothing;
INSERT INTO test_range_pt values(3, 'red')    ON DUPLICATE KEY UPDATE nothing;
INSERT INTO test_range_pt values(4, 'orange') ON DUPLICATE KEY UPDATE nothing;
INSERT INTO test_range_pt values(5, 'green')  ON DUPLICATE KEY UPDATE nothing;
INSERT INTO test_range_pt values(6, 'purple') ON DUPLICATE KEY UPDATE nothing;
INSERT INTO test_range_pt values(7, 'white')  ON DUPLICATE KEY UPDATE nothing;
INSERT INTO test_range_pt values(8, 'pink')   ON DUPLICATE KEY UPDATE nothing;
INSERT INTO test_range_pt values(9, 'yellow') ON DUPLICATE KEY UPDATE nothing;

SELECT * FROM test_range_pt ORDER BY key;

INSERT INTO test_range_pt values(2, 'black')  ON DUPLICATE KEY UPDATE nothing;
INSERT INTO test_range_pt values(3, 'red')    ON DUPLICATE KEY UPDATE nothing;
INSERT INTO test_range_pt values(4, 'orange') ON DUPLICATE KEY UPDATE nothing;
INSERT INTO test_range_pt values(5, 'green')  ON DUPLICATE KEY UPDATE nothing;
INSERT INTO test_range_pt values(6, 'purple') ON DUPLICATE KEY UPDATE nothing;
INSERT INTO test_range_pt values(7, 'white')  ON DUPLICATE KEY UPDATE nothing;
INSERT INTO test_range_pt values(8, 'pink')   ON DUPLICATE KEY UPDATE nothing;
INSERT INTO test_range_pt values(9, 'yellow') ON DUPLICATE KEY UPDATE nothing;

SELECT * FROM test_range_pt ORDER BY key;

drop table test_range_pt;
