\! gs_ktool -d all
\! gs_ktool -g
\! gs_ktool -g

DROP CLIENT MASTER KEY IF EXISTS ImgCMK1_sm4 CASCADE;
DROP CLIENT MASTER KEY IF EXISTS ImgCMK_sm4 CASCADE;
CREATE CLIENT MASTER KEY ImgCMK1_sm4 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/1" , ALGORITHM = SM4);
CREATE CLIENT MASTER KEY ImgCMK_sm4 WITH ( KEY_STORE = gs_ktool , KEY_PATH = "gs_ktool/2" , ALGORITHM = SM4);
CREATE COLUMN ENCRYPTION KEY ImgCEK1_sm4 WITH VALUES (CLIENT_MASTER_KEY = ImgCMK1_sm4, ALGORITHM = SM4_sm3);
CREATE COLUMN ENCRYPTION KEY ImgCEK_sm4 WITH VALUES (CLIENT_MASTER_KEY = ImgCMK_sm4, ALGORITHM = SM4_sm3);

-- 创建目标表products和源表newproducts，并插入数据
drop table IF EXISTS products;
CREATE TABLE products
(
product_id INTEGER,
product_name VARCHAR2(60) encrypted with (column_encryption_key = ImgCEK_sm4, encryption_type = DETERMINISTIC),
category VARCHAR2(60)
);
INSERT INTO products VALUES (15011, 'vivitar 35mm', 'electrncs');
INSERT INTO products VALUES (15021, 'olympus is50', 'electrncs');
INSERT INTO products VALUES (16001, 'play gym', 'toys');
INSERT INTO products VALUES (16011, 'lamaze', 'toys');

ALTER TABLE products drop COLUMN product_name;
ALTER TABLE products drop COLUMN category;
ALTER TABLE products ADD COLUMN product_name VARCHAR2(60) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = ImgCEK_sm4, ENCRYPTION_TYPE = DETERMINISTIC) ;
ALTER TABLE products ADD COLUMN category VARCHAR2(60) ;
ALTER TABLE products drop COLUMN product_name;
ALTER TABLE products ADD COLUMN product_name VARCHAR2(60) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = ImgCEK_sm4, ENCRYPTION_TYPE = DETERMINISTIC) ;
ALTER TABLE products ADD COLUMN product_name_2 text ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = ImgCEK_sm4, ENCRYPTION_TYPE = DETERMINISTIC) ;

\d products
 
INSERT INTO products VALUES (175011, 'vivitar 35mm', 'electrncs', 'car');
INSERT INTO products VALUES (17021, 'olympus is50', 'electrncs', 'shoe');
INSERT INTO products VALUES (18001, 'play gym', 'toys', 'book');
INSERT INTO products VALUES (18011, 'lamaze', 'toys', 'computer');
INSERT INTO products VALUES (18661, 'harry potter', 'dvd', 'cup');

SELECT * FROM products ORDER BY product_id;
 
drop table IF EXISTS products;
DROP CLIENT MASTER KEY IF EXISTS ImgCMK1_sm4 CASCADE;
DROP CLIENT MASTER KEY IF EXISTS ImgCMK_sm4 CASCADE;
\! gs_ktool -d all
