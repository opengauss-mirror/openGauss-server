CREATE TABLE tab_auto_increment (
    col_id INT PRIMARY KEY AUTO_INCREMENT,
    col_name TEXT
) AUTO_INCREMENT = 100 COMMENT = 'this is auto increment table';

CREATE TABLE tab_constraint (
    col_id INT PRIMARY KEY,
    col_age INT,
    CONSTRAINT check_age CHECK ( col_age > 18)
);

CREATE TABLE tab_on_update1 (
    col_id INT,
    col_name TEXT,
    upd_time DATE DEFAULT CURRENT_DATE ON UPDATE CURRENT_DATE COMMENT 'update record date',
    PRIMARY KEY (col_id)
);

CREATE TABLE tab_on_update2 (
    col_id INT,
    col_name TEXT,
    upd_time TIME DEFAULT CURRENT_TIME ON UPDATE CURRENT_TIME COMMENT 'update record time',
    PRIMARY KEY (col_id)
);

CREATE TABLE tab_on_update3 (
    col_id INT,
    col_name TEXT,
    upd_time TIME DEFAULT CURRENT_TIME(2) ON UPDATE CURRENT_TIME(2) COMMENT 'update record time(2)',
    PRIMARY KEY (col_id)
);

CREATE TABLE tab_on_update4 (
    col_id INT,
    col_name TEXT,
    upd_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'update record timestamp',
    PRIMARY KEY (col_id)
);

CREATE TABLE tab_on_update5 (
    col_id INT,
    col_name TEXT,
    upd_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(2) ON UPDATE CURRENT_TIMESTAMP(2) COMMENT 'update record timestamp(2)',
    PRIMARY KEY (col_id)
);

CREATE TABLE tab_on_update6 (
    col_id INT,
    col_name TEXT,
    upd_time TIMESTAMP ON UPDATE CURRENT_TIMESTAMP + 1 COMMENT 'update record timestamp + 1',
    PRIMARY KEY (col_id)
);

CREATE TABLE tab_on_update7 (
    col_id INT,
    col_name TEXT,
    upd_time TIMESTAMP ON UPDATE CURRENT_TIMESTAMP - 1 COMMENT 'update record timestamp - 1',
    PRIMARY KEY (col_id)
);

CREATE TABLE tab_on_update8 (
    col_id INT,
    col_name TEXT,
    upd_time TIMESTAMP ON UPDATE CURRENT_TIMESTAMP + '1 hour' COMMENT 'update record timestamp - 1',
    PRIMARY KEY (col_id)
);

CREATE TABLE tab_on_update9 (
    col_id INT,
    col_name TEXT,
    upd_time TIME DEFAULT LOCALTIME ON UPDATE LOCALTIME  COMMENT 'update record local time',
    PRIMARY KEY (col_id)
);

CREATE TABLE tab_on_update10 (
    col_id INT,
    col_name TEXT,
    upd_time TIME DEFAULT LOCALTIME(2) ON UPDATE LOCALTIME(2)  COMMENT 'update record local time(2)',
    PRIMARY KEY (col_id)
);

CREATE TABLE tab_on_update11 (
    col_id INT,
    col_name TEXT,
    upd_time TIME DEFAULT LOCALTIMESTAMP ON UPDATE LOCALTIMESTAMP  COMMENT 'update record local timestamp',
    PRIMARY KEY (col_id)
);

CREATE TABLE tab_on_update12 (
    col_id INT,
    col_name TEXT,
    upd_time TIME DEFAULT LOCALTIMESTAMP(2) ON UPDATE LOCALTIMESTAMP(2)  COMMENT 'update record local timestamp(2)',
    PRIMARY KEY (col_id)
);