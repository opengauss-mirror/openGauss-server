CREATE TABLE tb_class (id INT,class_name TEXT);
INSERT INTO tb_class (id,class_name) VALUES (1,'class_1');
INSERT INTO tb_class (id,class_name) VALUES (2,'class_2');

CREATE TABLE tb_student (id INT,student_name TEXT,class_id INT);
INSERT INTO tb_student (id,student_name,class_id) VALUES (1,'li lei',1);
INSERT INTO tb_student (id,student_name,class_id) VALUES (2,'han meimei',1);
INSERT INTO tb_student (id,student_name,class_id) VALUES (3,'zhang xiaoming',2);
INSERT INTO tb_student (id,student_name,class_id) VALUES (4,'wang peng',2);

CREATE VIEW vw_class AS SELECT * FROM tb_class;
CREATE VIEW vw_student AS SELECT * FROM tb_student;
CREATE VIEW vw_class_student AS SELECT c.class_name,s.student_name FROM tb_class c JOIN tb_student s ON c.id = s.class_id;
CREATE VIEW vw_class_1_student AS SELECT c.class_name,s.student_name FROM tb_class c JOIN tb_student s ON c.id = s.class_id WHERE c.id = 1;

CREATE TABLE tb_order (id INT,order_product TEXT,order_time timestamptz);
INSERT INTO tb_order (id,order_product) VALUES (1,'football');
INSERT INTO tb_order (id,order_product) VALUES (2,'baskball');

CREATE VIEW vw_order AS SELECT * FROM tb_order;
ALTER VIEW vw_order ALTER COLUMN order_time SET DEFAULT now();


CREATE TABLE tb_address (id INT,address TEXT);
INSERT INTO tb_address (id,address) VALUES (1,'a_address');
INSERT INTO tb_address (id,address) VALUES (2,'b_address');

CREATE VIEW vw_address AS SELECT * FROM tb_address;
ALTER VIEW vw_address RENAME TO vw_address_new;

CREATE TABLE tb_book (id INT,book_name TEXT);
INSERT INTO tb_book (id,book_name) VALUES (1,'englisen');
INSERT INTO tb_book (id,book_name) VALUES (2,'math');

CREATE VIEW vw_book AS SELECT * FROM tb_book;
DROP VIEW vw_book;