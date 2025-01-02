/*
 * This file is used to test the function of ExecVecAsofJoin()
 */
----
--- Create Table and Insert Data
----

CREATE TABLE prices(
	ticker char(10),
	pa integer,
	wh timestamp,
	price DECIMAL(10,2))with(orientation=column);
CREATE TABLE holdings(  
	ticker char(10),
	pa integer,
	wh timestamp,
	shares DECIMAL(10,2))with(orientation=column);

 

insert into  prices values('APPL', 1, '2001-01-01 00:00:00', 1);
insert into  prices values('APPL', 1,  '2001-01-01 00:01:00', 2);
insert into  prices values('APPL', 1, '2001-01-01 00:02:00', 3);
insert into  prices values('MSFT', 2, '2001-01-01 00:00:00', 1);
insert into  prices values('MSFT', 2, '2001-01-01 00:01:00', 2);
insert into  prices values('MSFT', 2, '2001-01-01 00:02:00', 3);
insert into  prices values('GOOG', 1, '2001-01-01 00:00:00', 1);
insert into  prices values('GOOG', 1,'2001-01-01 00:01:00', 2);
insert into  prices values('GOOG', 1,'2001-01-01 00:02:00', 3);

insert into  holdings values('APPL', 1, '2000-12-31 23:59:30', 5.16);
insert into  holdings values('APPL', 2, '2001-01-01 00:00:30', 2.94);
insert into  holdings values('APPL', 1, '2001-01-01 00:01:30', 24.13);
insert into  holdings values('MSFT', 1, '2000-12-31 23:59:30', 9.33);
insert into  holdings values('MSFT', 2, '2001-01-01 00:00:30', 23.45);
insert into  holdings values('MSFT', 1, '2001-01-01 00:01:30', 10.58);
insert into  holdings values('DATA', 1, '2000-12-31 23:59:30', 6.65);
insert into  holdings values('DATA', 1, '2001-01-01 00:00:30', 17.95);
insert into  holdings values('DATA', 1, '2001-01-01 00:01:30', 18.37);

----
--- Normal Case
----
set query_dop = 1;
SELECT h.ticker, h.wh, price * shares AS total 
FROM holdings h 
ASOF JOIN prices p ON h.ticker = p.ticker AND h.wh >= p.wh order by h.ticker;

explain (verbose on, costs off) 
SELECT h.ticker, h.wh, price * shares AS total 
FROM holdings h 
ASOF JOIN prices p ON h.ticker = p.ticker AND h.wh >= p.wh order by h.ticker;

SELECT h.ticker, h.wh
FROM (
  SELECT ticker, 
   wh,
   pa
  FROM holdings 
)  h ASOF JOIN (
  SELECT ticker, 
   wh,
   pa
  FROM prices
) p
  ON h.ticker = p.ticker AND h.pa = p.pa  AND h.wh >= p.wh order by h.ticker;

SELECT h.ticker, h.wh
FROM (
  SELECT ticker, 
   wh, 
   pa+1 as pas
  FROM holdings
)  h ASOF JOIN (
  SELECT ticker, 
   wh, 
   pa+1 as pas
  FROM prices
) p
  ON h.ticker = p.ticker  AND h.wh >= p.wh AND h.pas >= p.pas order by h.ticker;


-- SMP case
set query_dop = 2;
SELECT h.ticker, h.wh, price * shares AS total 
FROM holdings h 
ASOF JOIN prices p ON h.ticker = p.ticker AND h.wh >= p.wh order by h.ticker;
explain (verbose on, costs off)
SELECT h.ticker, h.wh, price * shares AS total 
FROM holdings h 
ASOF JOIN prices p ON h.ticker = p.ticker AND h.wh >= p.wh order by h.ticker;



----
--- Clean table and resource
----

drop table prices;
drop table holdings;

