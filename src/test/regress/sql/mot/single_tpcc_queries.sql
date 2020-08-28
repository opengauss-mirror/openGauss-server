--
-- OltpNG TPCC Queries
--

SET default_tablespace = '';

SET default_with_oids = false;


CREATE FOREIGN TABLE bmsql_config (
  cfg_name    varchar(30),
  cfg_value   varchar(50),
  primary key (cfg_name)
) SERVER mot_server;

CREATE FOREIGN TABLE bmsql_warehouse (
  w_id        integer   not null,
  w_ytd       decimal(12,2),
  w_tax       decimal(4,4),
  w_name      varchar(10),
  w_street_1  varchar(20),
  w_street_2  varchar(20),
  w_city      varchar(20),
  w_state     char(2),
  w_zip       char(9),
  primary key (w_id)
) SERVER mot_server;

CREATE FOREIGN TABLE bmsql_district (
  d_w_id       integer       not null,
  d_id         integer       not null,
  d_ytd        decimal(12,2),
  d_tax        decimal(4,4),
  d_next_o_id  integer,
  d_name       varchar(10),
  d_street_1   varchar(20),
  d_street_2   varchar(20),
  d_city       varchar(20),
  d_state      char(2),
  d_zip        char(9),
  primary key (d_w_id, d_id)
) SERVER mot_server;

CREATE FOREIGN TABLE bmsql_customer (
  c_w_id         integer        not null,
  c_d_id         integer        not null,
  c_id           integer        not null,
  c_discount     decimal(4,4),
  c_credit       char(2),
  c_last         varchar(16)    not null,
  c_first        varchar(16)    not null,
  c_credit_lim   decimal(12,2),
  c_balance      decimal(12,2),
  c_ytd_payment  decimal(12,2),
  c_payment_cnt  integer,
  c_delivery_cnt integer,
  c_street_1     varchar(20),
  c_street_2     varchar(20),
  c_city         varchar(20),
  c_state        char(2),
  c_zip          char(9),
  c_phone        char(16),
  c_since        timestamp,
  c_middle       char(2),
  c_data         varchar(500),
  primary key (c_w_id, c_d_id, c_id)
) SERVER mot_server;

CREATE FOREIGN TABLE bmsql_history (
  hist_id  integer,
  h_c_id   integer,
  h_c_d_id integer,
  h_c_w_id integer,
  h_d_id   integer,
  h_w_id   integer,
  h_date   timestamp,
  h_amount decimal(6,2),
  h_data   varchar(24)
) SERVER mot_server;

CREATE FOREIGN TABLE bmsql_new_order (
  no_w_id  integer   not null,
  no_d_id  integer   not null,
  no_o_id  integer   not null,
  primary key (no_w_id, no_d_id, no_o_id)
) SERVER mot_server;

CREATE FOREIGN TABLE bmsql_oorder (
  o_w_id       integer      not null,
  o_d_id       integer      not null,
  o_id         integer      not null,
  o_c_id       integer      not null,
  o_carrier_id integer,
  o_ol_cnt     integer,
  o_all_local  integer,
  o_entry_d    timestamp,
  primary key (o_w_id, o_d_id, o_id)
) SERVER mot_server;

CREATE FOREIGN TABLE bmsql_order_line (
  ol_w_id         integer   not null,
  ol_d_id         integer   not null,
  ol_o_id         integer   not null,
  ol_number       integer   not null,
  ol_i_id         integer   not null,
  ol_delivery_d   timestamp,
  ol_amount       decimal(6,2),
  ol_supply_w_id  integer,
  ol_quantity     integer,
  ol_dist_info    char(24),
  primary key (ol_w_id, ol_d_id, ol_o_id, ol_number)
) SERVER mot_server;

CREATE FOREIGN TABLE bmsql_item (
  i_id     integer      not null,
  i_name   varchar(24),
  i_price  decimal(5,2),
  i_data   varchar(50),
  i_im_id  integer,
  primary key (i_id)
) SERVER mot_server;

CREATE FOREIGN TABLE bmsql_stock (
  s_w_id       integer       not null,
  s_i_id       integer       not null,
  s_quantity   integer,
  s_ytd        integer,
  s_order_cnt  integer,
  s_remote_cnt integer,
  s_data       varchar(50),
  s_dist_01    char(24),
  s_dist_02    char(24),
  s_dist_03    char(24),
  s_dist_04    char(24),
  s_dist_05    char(24),
  s_dist_06    char(24),
  s_dist_07    char(24),
  s_dist_08    char(24),
  s_dist_09    char(24),
  s_dist_10    char(24),
  primary key (s_w_id, s_i_id)
) SERVER mot_server;

CREATE index bmsql_customer_idx1  on  bmsql_customer (c_w_id, c_d_id, c_last, c_first);

--CREATE index bmsql_oorder_idx1 on  bmsql_oorder (o_w_id, o_d_id, o_carrier_id, o_id);

--CREATE index  d_idx1 on  bmsql_district(d_w_id);
--CREATE index  d_idx2 on  bmsql_customer(c_w_id, c_d_id);
--CREATE index  d_idx3 on bmsql_history (h_c_w_id, h_c_d_id, h_c_id) ;
--CREATE index  d_idx4 on bmsql_history(h_w_id, h_d_id);
--CREATE index  d_idx5 on  bmsql_new_order(no_w_id, no_d_id, no_o_id);
CREATE index  d_idx6 on bmsql_oorder(o_w_id, o_d_id, o_c_id) ;
--CREATE index  d_idx7 on  bmsql_order_line(ol_w_id, ol_d_id, ol_o_id);
--CREATE index  d_idx8 on  bmsql_order_line(ol_supply_w_id, ol_i_id);
--CREATE index  d_idx9 on  bmsql_stock(s_w_id);
--CREATE index  d_idx10 on  bmsql_stock(s_i_id);
--customer--
insert into bmsql_customer values (1,1,1, .2326, 'GC', 'BAR', 'BARBAR', 50000.00, -10.00, 10.00,1,1, 'US48CVC4nX', 'US48CVC4nY', 'US48CVC4nZ', 'IL', 'ZIPZIP', '12341234', '2018-10-28 21:35:30.726', 'MD', 'ABC123'); 
insert into bmsql_customer values (1,1,2, .2326, 'GC', 'DDD', 'DARDAR', 50000.00, -10.00, 10.00,1,1, 'US48CVC4nX', 'US48CVC4nY', 'US48CVC4nZ', 'IL', 'ZIPZIP', '12341234', '2018-10-28 21:35:30.729', 'MD', 'ABC123');
insert into bmsql_customer values (1,1,3, .2326, 'GC', 'FFF', 'GARGAR', 50000.00, -10.00, 10.00,1,1, 'US48CVC4nX', 'US48CVC4nY', 'US48CVC4nZ', 'IL', 'ZIPZIP', '12341234', '2018-10-28 21:35:30.728', 'MD', 'ABC123');
insert into bmsql_customer values (1,1,4, .2326, 'GC', 'VVV', 'BGFBGF', 50000.00, -10.00, 10.00,1,1, 'US48CVC4nX', 'US48CVC4nY', 'US48CVC4nZ', 'IL', 'ZIPZIP', '12341234', '2018-10-28 21:35:30.723', 'MD', 'ABC123');

--district--
insert into bmsql_district values (1,1, 123.22, .1234, 100, 'VVV', 'FFF', 'DDD', 'XXX', 'IL', '84965');
insert into bmsql_district values (1,2, 123.22, .1234, 100, 'VVV', 'FFF', 'DDD', 'XXX', 'IL', '84965');
insert into bmsql_district values (1,7, 123.22, .1234, 100, 'VVV', 'FFF', 'DDD', 'XXX', 'IL', '84965');
insert into bmsql_district values (1,9, 123.22, .1234, 100, 'VVV', 'FFF', 'DDD', 'XXX', 'IL', '84965');

--history--
insert into bmsql_history values (1, 1, 1, 1, 1, 1, '2018-10-28 21:35:30.723', 33.33, 'OP9090');
insert into bmsql_history values (2, 1, 1, 1, 1, 1, '2018-10-28 21:35:30.723', 33.33, 'OP9090');
insert into bmsql_history values (3, 1, 1, 1, 1, 1, '2018-10-28 21:35:30.723', 33.33, 'OP9090');
insert into bmsql_history values (4, 1, 1, 1, 1, 1, '2018-10-28 21:35:30.723', 33.33, 'OP9090');

--item--
insert into bmsql_item values (1, 'HAHCVA', 567.89, 'DADADA', 90);
insert into bmsql_item values (2, 'HAHERA', 667.89, 'DADADV', 90);
insert into bmsql_item values (3, 'HAHCXA', 587.89, 'CVCVCDA', 90);
insert into bmsql_item values (4, 'HAERHA', 436.89, 'UHVZSFLKJDADADA', 90);

--new order--
insert into bmsql_new_order values (1, 23, 456);
insert into bmsql_new_order values (1, 63, 656);
insert into bmsql_new_order values (1, 993, 666);
insert into bmsql_new_order values (1, 253, 1456);

--o order--
insert into bmsql_oorder values (1,1, 1,1,1,1,1, '2018-10-28 21:35:30.723');
insert into bmsql_oorder values (1,1, 2,1,2,1,1, '2018-10-28 21:45:30.723');
insert into bmsql_oorder values (1,1, 45,1,3,1,1, '2018-10-28 21:32:30.723');
insert into bmsql_oorder values (1,1, 67,1,4,1,1, '2018-10-28 21:31:30.723');

--orderline--
insert into bmsql_order_line values (1,1,1, 123, 341, '2018-10-28 21:31:30.723', 99.13,  89, 23, 'DADADA');
insert into bmsql_order_line values (1,1,1, 153, 41, '2018-10-28 21:39:30.723', 99.13,  55, 24, 'DADCDA');
insert into bmsql_order_line values (1,1,1, 423, 641, '2018-10-28 21:32:30.723', 99.13,  23, 26, 'DAZXADA');
insert into bmsql_order_line values (1,1,1, 723, 941, '2018-10-28 21:11:30.723', 99.13,  34, 92, 'DADFGADA');

--stock--
insert into bmsql_stock values (1,341, 874, 387, 5, 8, 'DATDATADATA', 'SS', 'FFF' , 'XCXV', 'XSXSX', 'WEQ', 'TTT', 'DASDAS', 'TFSDFSD' , 'AKDAFA', 'OKYA' );
insert into bmsql_stock values (1,41, 74, 4287, 5, 8, 'DATDATADATA', 'SS', 'FFF' , 'XCXV', 'XSXSX', 'WEQ', 'TTT', 'DASDAS', 'TFSDFSD' , 'AKDAFA', 'OKYA' );
insert into bmsql_stock values (1,641, 174, 1587, 5, 8, 'DATDATADATA', 'SS', 'FFF' , 'XCXV', 'XSXSX', 'WEQ', 'TTT', 'DASDAS', 'TFSDFSD' , 'AKDAFA', 'OKYA' );
insert into bmsql_stock values (1,941, 77, 127, 5, 8, 'DATDATADATA', 'SS', 'FFF' , 'XCXV', 'XSXSX', 'WEQ', 'TTT', 'DASDAS', 'TFSDFSD' , 'AKDAFA', 'OKYA' );

--warehouse--
insert into bmsql_warehouse values (1, 300000.00, .0940, 'Y8tGa6iFq', 'zoqVwMnEDh3ON', 'sooG1kQQ8Pz', 'NvUlyevdvfoZME2q', 'IL', '442111111');

--tpcc queries
SELECT c_discount, c_last, c_credit, w_tax FROM bmsql_customer JOIN bmsql_warehouse ON (w_id = c_w_id) WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
SELECT d_tax, d_next_o_id FROM bmsql_district WHERE d_w_id = 1 AND d_id = 1 FOR UPDATE;
UPDATE bmsql_district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = 1 AND d_id = 1;
SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM bmsql_stock WHERE s_w_id = 1 AND s_i_id = 41 FOR UPDATE;
SELECT i_price, i_name, i_data FROM bmsql_item WHERE i_id = 4;
SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip FROM bmsql_warehouse WHERE w_id = 1 ;
SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip FROM bmsql_district WHERE d_w_id = 1 AND d_id = 7;
SELECT c_id FROM bmsql_customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'BAR' ORDER BY c_first;
SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance FROM bmsql_customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id =  1 FOR UPDATE;
SELECT c_data FROM bmsql_customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
UPDATE bmsql_warehouse SET w_ytd = w_ytd + 2 WHERE w_id = 1;
UPDATE bmsql_district SET d_ytd = d_ytd + 7 WHERE d_w_id = 1 AND d_id = 7;
UPDATE bmsql_customer SET c_balance = c_balance - 3, c_ytd_payment = c_ytd_payment + 5, c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM bmsql_order_line WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 1 ORDER BY ol_w_id, ol_d_id, ol_o_id, ol_number;
SELECT count(*) AS low_stock FROM (SELECT s_w_id, s_i_id, s_quantity FROM bmsql_stock WHERE s_w_id = 1 AND s_quantity < 99 AND s_i_id IN (SELECT ol_i_id FROM bmsql_district JOIN bmsql_order_line ON ol_w_id = d_w_id AND ol_d_id = d_id AND ol_o_id >= d_next_o_id - 20 AND ol_o_id < d_next_o_id WHERE d_w_id = 1 AND d_id = 1)) AS L;
SELECT no_o_id FROM bmsql_new_order WHERE no_w_id = 1 AND no_d_id = 7 ORDER BY no_o_id ASC LIMIT 1;
DELETE FROM bmsql_new_order WHERE no_w_id = 1 AND no_d_id = 23 AND no_o_id = 456;
SELECT o_c_id FROM bmsql_oorder WHERE o_w_id = 1 AND o_d_id = 1 AND o_id = 67;
SELECT sum(ol_amount) AS sum_ol_amount FROM bmsql_order_line WHERE ol_w_id = 1 AND ol_d_id = 1 AND ol_o_id = 1;

--cleanup
DROP FOREIGN TABLE bmsql_warehouse;
DROP FOREIGN TABLE bmsql_district;
DROP FOREIGN TABLE bmsql_customer;
DROP FOREIGN TABLE bmsql_history;
DROP FOREIGN TABLE bmsql_item;
DROP FOREIGN TABLE bmsql_new_order;
DROP FOREIGN TABLE bmsql_oorder;
DROP FOREIGN TABLE bmsql_stock;
DROP FOREIGN TABLE bmsql_order_line;
DROP FOREIGN TABLE bmsql_config;
