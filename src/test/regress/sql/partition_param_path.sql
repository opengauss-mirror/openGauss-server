CREATE TABLE am_acct_net (
	acct_seq numeric(12,0) NOT NULL,
	post_date numeric(8,0) NOT NULL,
	post_clear_date numeric(8,0) NOT NULL,
	fund_net numeric(18,2),
	debit_amt numeric(18,2),
	credit_amt numeric(18,2),
	product_id numeric(16,0),
	xsjg_id numeric(16,0),
	seat_no character varying(6),
	trade_acc character varying(36),
	dm_class character varying(10),
	cust_type character varying(6),
	channel_actor_id numeric(16,0),
	bank_acc_no character varying(36),
	fee_dept_code character varying(10),
	bank_channel character varying(20),
	fund_acc character varying(16),
	product_code character varying(50),
	xsjg_code character varying(64),
	item_key character varying(64)
);
CREATE INDEX idx_am_acct_net_01 ON am_acct_net USING btree (acct_seq, post_date, item_key) TABLESPACE pg_default;

CREATE TABLE am_acct_net_his (
	acct_seq numeric(12,0) NOT NULL,
	post_date numeric(8,0) NOT NULL,
	post_clear_date numeric(8,0) NOT NULL,
	fund_net numeric(18,2),
	debit_amt numeric(18,2),
	credit_amt numeric(18,2),
	product_id numeric(16,0),
	xsjg_id numeric(16,0),
	seat_no character varying(6),
	trade_acc character varying(36),
	dm_class character varying(10),
	cust_type character varying(6),
	channel_actor_id numeric(16,0),
	bank_acc_no character varying(36),
	fee_dept_code character varying(10),
	bank_channel character varying(20),
	fund_acc character varying(16),
	product_code character varying(50),
	xsjg_code character varying(64),
	item_key character varying(64)
)
WITH (orientation=row, compression=no)
PARTITION BY RANGE (post_date)
( 
	 PARTITION m_202201 VALUES LESS THAN (20220201) TABLESPACE pg_default,
	 PARTITION m_202202 VALUES LESS THAN (20220301) TABLESPACE pg_default,
	 PARTITION m_max_date VALUES LESS THAN (MAXVALUE) TABLESPACE pg_default
)
ENABLE ROW MOVEMENT;
CREATE INDEX idx_am_acct_net_his_01 ON am_acct_net_his USING btree (acct_seq, post_date) LOCAL(PARTITION m_202201_acct_seq_post_date_idx, PARTITION m_202202_acct_seq_post_date_idx, PARTITION m_max_date_acct_seq_post_date_idx)  WITH (fillfactor=90) TABLESPACE pg_default;


CREATE OR REPLACE VIEW vw_am_acct_net
AS SELECT a.acct_seq, a.post_date, a.post_clear_date, a.fund_net, 
            a.debit_amt, a.credit_amt, a.product_id, a.xsjg_id, a.product_code, 
            a.xsjg_code, a.seat_no, a.trade_acc, a.dm_class, a.cust_type, 
            a.channel_actor_id, a.bank_acc_no, a.fee_dept_code, a.bank_channel, 
            a.fund_acc, a.item_key
           FROM am_acct_net a
UNION ALL 
         SELECT b.acct_seq, b.post_date, b.post_clear_date, b.fund_net, 
            b.debit_amt, b.credit_amt, b.product_id, b.xsjg_id, b.product_code, 
            b.xsjg_code, b.seat_no, b.trade_acc, b.dm_class, b.cust_type, 
            b.channel_actor_id, b.bank_acc_no, b.fee_dept_code, b.bank_channel, 
            b.fund_acc, b.item_key
           FROM am_acct_net_his b;
CREATE TABLE am_acct_book_code (
	acct_seq numeric(12,0),
	acct_book_id numeric(5,0) NOT NULL,
	acct_code character varying(30) NOT NULL,
	init_date numeric(8,0)
);
ALTER TABLE am_acct_book_code ADD CONSTRAINT pk_am_acct_book_code PRIMARY KEY (acct_book_id, acct_code);
INSERT INTO am_acct_book_code (acct_seq,acct_book_id,acct_code,init_date) VALUES
	 (721,1,'118',20180808),
	 (722,1,'221',20180808),
	 (741,2,'10110',20180809);
	 
	 
INSERT INTO am_acct_net_his (acct_seq,post_date,post_clear_date,fund_net,debit_amt,credit_amt,product_id,xsjg_id,product_code,xsjg_code,seat_no,trade_acc,dm_class,cust_type,channel_actor_id,bank_acc_no,fee_dept_code,bank_channel,fund_acc,item_key) VALUES
	 (119,20181029,20181029,20181029.29,20181029.29,0.00,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'EBF11FD87CA5FA5BC1D9DDE0EE12822E'),
	 (119,20181030,20181030,20181030.30,20181030.30,0.00,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'EBF11FD87CA5FA5BC1D9DDE0EE12822E'),
	 (119,20181031,20181031,20181031.31,20181031.31,0.00,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'EBF11FD87CA5FA5BC1D9DDE0EE12822E'),
	 (941,20181029,20181029,20181029.29,20181029.29,0.00,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'EBF11FD87CA5FA5BC1D9DDE0EE12822E'),
	 (941,20181030,20181030,20181030.30,20181030.30,0.00,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'EBF11FD87CA5FA5BC1D9DDE0EE12822E'),
	 (941,20181031,20181031,20181031.31,20181031.31,0.00,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'EBF11FD87CA5FA5BC1D9DDE0EE12822E');
	 
	 
INSERT INTO am_acct_net (acct_seq,post_date,post_clear_date,fund_net,debit_amt,credit_amt,product_id,xsjg_id,product_code,xsjg_code,seat_no,trade_acc,dm_class,cust_type,channel_actor_id,bank_acc_no,fee_dept_code,bank_channel,fund_acc,item_key) VALUES
	 (721,20181101,20181101,0.00,0.00,0.00,NULL,NULL,'510413',NULL,'JS431',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'57843458FB8266709AC77E28A85EEA5B'),
	 (721,20181101,20181101,3535.02,3535.02,0.00,NULL,NULL,'513500',NULL,'JS431',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'5AD63338368098DFD83AE4A71F59392A'),
	 (721,20181101,20181101,3000.00,3000.00,0.00,NULL,NULL,'513503',NULL,'JS431',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'8E178E960F3296F15A5AB78D84647DA1'),
	 (721,20181101,20181101,0.00,0.00,0.00,NULL,NULL,'510710',NULL,'JS431',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'9471869B191A471EF8C3B4204FD85268'),
	 (721,20181101,20181101,0.00,0.00,0.00,NULL,NULL,'510410',NULL,'JS431',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'B661F7730613723B6FAE5B4ADFDF1E87'),
	 (721,20181101,20181101,0.00,0.00,0.00,NULL,NULL,'511210',NULL,'JS431',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'BFEB97F13CBA09234D35FAAE840EB4DA'),
	 (721,20181101,20181101,1336.00,1336.00,0.00,NULL,NULL,'513500',NULL,'JS486',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'D7FFA61CBE43645549923F3E49B26698'),
	 (722,20181101,20181101,1336.00,0.00,1336.00,NULL,NULL,NULL,'001237','JS486',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'29D25000F31B26E42C82F0D0CEDE330F'),
	 (722,20181101,20181101,6535.02,0.00,6535.02,NULL,NULL,NULL,'626','JS431',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'33555F57007C0DC105B8CDB8F457B11B'),
	 (741,20181101,20181101,0.00,0.00,0.00,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'EBF11FD87CA5FA5BC1D9DDE0EE12822E');

explain SELECT A.* FROM VW_AM_ACCT_NET A JOIN AM_ACCT_BOOK_CODE B
    ON A.ACCT_SEQ = B.ACCT_SEQ
    WHERE  B.ACCT_BOOK_ID = 2 AND B.acct_code ='10110' AND  A.POST_DATE <= 20181101 order by 1,2,3,4;

SELECT A.* FROM VW_AM_ACCT_NET A JOIN AM_ACCT_BOOK_CODE B
    ON A.ACCT_SEQ = B.ACCT_SEQ
    WHERE  B.ACCT_BOOK_ID = 2 AND B.acct_code ='10110' AND  A.POST_DATE <= 20181101 order by 1,2,3,4;

explain SELECT A.* FROM VW_AM_ACCT_NET A JOIN AM_ACCT_BOOK_CODE B
    ON A.ACCT_SEQ = B.ACCT_SEQ
    WHERE  B.ACCT_BOOK_ID = 2 AND B.acct_code ='10110' order by 1,2,3,4;

SELECT A.* FROM VW_AM_ACCT_NET A JOIN AM_ACCT_BOOK_CODE B
    ON A.ACCT_SEQ = B.ACCT_SEQ
    WHERE  B.ACCT_BOOK_ID = 2 AND B.acct_code ='10110' order by 1,2,3,4;

DROP TABLE am_acct_net CASCADE;
DROP TABLE am_acct_net_his CASCADE;
DROP TABLE am_acct_book_code CASCADE;
