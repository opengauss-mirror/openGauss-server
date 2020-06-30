CREATE FOREIGN TABLE emp_details (id serial, salary float);
CREATE FOREIGN TABLE emp_log (emp_id serial, logged_salary float);

CREATE OR REPLACE FUNCTION rec_insert()
  RETURNS trigger AS
$$
BEGIN
         INSERT INTO emp_log(logged_salary) VALUES(NEW.salary);
    RETURN NEW;
END;
$$
LANGUAGE 'plpgsql';

CREATE TRIGGER ins_same_rec
  AFTER INSERT
  ON emp_details
  FOR EACH ROW
  EXECUTE PROCEDURE rec_insert();

INSERT INTO emp_details VALUES (22000.0);

SELECT * FROM emp_log;

DROP FOREIGN TABLE emp_details;
DROP FOREIGN TABLE emp_log;
