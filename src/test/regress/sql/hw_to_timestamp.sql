SET DateStyle TO 'ISO, MDY';

SELECT TO_TIMESTAMP('32-1-1','DD-MM-YYYY');
SELECT TO_TIMESTAMP('31-1-1','DD-MM-YYYY');
SELECT TO_TIMESTAMP('1-1-1','DD-MM-YYYY');
SELECT TO_TIMESTAMP('0-1-1','DD-MM-YYYY');

SELECT TO_TIMESTAMP('1-0-1','DD-MM-YYYY');
SELECT TO_TIMESTAMP('1-1-1','DD-MM-YYYY');
SELECT TO_TIMESTAMP('1-12-1','DD-MM-YYYY');
SELECT TO_TIMESTAMP('1-13-1','DD-MM-YYYY');

SELECT TO_TIMESTAMP('28-2-1','DD-MM-YYYY');
SELECT TO_TIMESTAMP('29-2-1','DD-MM-YYYY');
SELECT TO_TIMESTAMP('29-2-4','DD-MM-YYYY');

SELECT TO_TIMESTAMP('0-1','DDD-YYYY');
SELECT TO_TIMESTAMP('367-1','DDD-YYYY');
SELECT TO_TIMESTAMP('366-1','DDD-YYYY');
SELECT TO_TIMESTAMP('366-4','DDD-YYYY');

SELECT TO_TIMESTAMP('0','HH12');
SELECT TO_TIMESTAMP('1','HH12');
SELECT TO_TIMESTAMP('12','HH12');
SELECT TO_TIMESTAMP('13','HH12');

SELECT TO_TIMESTAMP('0','HH24');
SELECT TO_TIMESTAMP('23','HH24');
SELECT TO_TIMESTAMP('24','HH24');

SELECT TO_TIMESTAMP('24','H24');

SELECT TO_TIMESTAMP('0','MI');
SELECT TO_TIMESTAMP('59','MI');
SELECT TO_TIMESTAMP('60','MI');

SELECT TO_TIMESTAMP('0','SS');
SELECT TO_TIMESTAMP('59','SS');
SELECT TO_TIMESTAMP('60','SS');


SELECT TO_TIMESTAMP('60','DDD-PS');
SELECT TO_TIMESTAMP('366-4','YYYY-P');
SELECT TO_TIMESTAMP('366-4','YYYY-DD-12');

select to_timestamp('-4713','YYYY') from dual;
select to_timestamp('-4712','YYYY') from dual;
select to_timestamp('-1','YYYY') from dual;
select to_timestamp('0','YYYY') from dual;
select to_timestamp('1','YYYY') from dual;
select to_timestamp('9999','YYYY') from dual;
select to_timestamp('10000','YYYY') from dual;

select to_timestamp('10','H24') from dual;
select to_timestamp('10','H24') from dual;

SELECT to_timestamp('-2012-2/3,21.15;36:18','YYYY-MM/DD,HH24.MI;SS:FF') FROM DUAL;
SELECT to_timestamp('-1-2/3,21.15;36:18','YYYY-MM/DD,HH24.MI;SS:FF') FROM DUAL;
SELECT to_timestamp('0-2/3,21.15;36:18','YYYY-MM/DD,HH24.MI;SS:FF') FROM DUAL;
SELECT to_timestamp('9999-2/3,21.15;36:18','YYYY-MM/DD,HH24.MI;SS:FF') FROM DUAL;
SELECT to_timestamp('10000-2/3,21.15;36:18','YYYY-MM/DD,HH24.MI;SS:FF') FROM DUAL;

select to_timestamp('-4713-3-4 13:2:3.234015', 'syyyy-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('-4712-3-4 13:2:3.234015', 'syyyy-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('0-3-4 13:2:3.234015', 'syyyy-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('9999-3-4 13:2:3.234015', 'syyyy-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('10000-3-4 13:2:3.234015', 'syyyy-mm-dd hh24:mi:ss.ff') from dual;

select to_timestamp('-4713-3-4 13:2:3.234015', 'SYYYY-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('-4712-3-4 13:2:3.234015', 'SYYYY-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('0-3-4 13:2:3.234015', 'SYYYY-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('9999-3-4 13:2:3.234015', 'SYYYY-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('10000-3-4 13:2:3.234015', 'SYYYY-mm-dd hh24:mi:ss.ff') from dual;

select to_timestamp('-4713-3-4 13:2:3.234015', 'RR-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('-4712-3-4 13:2:3.234015', 'RR-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('-1-3-4 13:2:3.234015', 'RR-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('0-3-4 13:2:3.234015', 'RR-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('1-3-4 13:2:3.234015', 'RR-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('9999-3-4 13:2:3.234015', 'RR-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('10000-3-4 13:2:3.234015', 'RR-mm-dd hh24:mi:ss.ff') from dual;

select to_timestamp('-4713-3-4 13:2:3.234015', 'RRRR-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('-4712-3-4 13:2:3.234015', 'RRRR-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('-1-3-4 13:2:3.234015', 'RRRR-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('0-3-4 13:2:3.234015', 'RRRR-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('1-3-4 13:2:3.234015', 'RRRR-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('9999-3-4 13:2:3.234015', 'RRRR-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('10000-3-4 13:2:3.234015', 'RRRR-mm-dd hh24:mi:ss.ff') from dual;

select to_timestamp('-4713-3-4 13:2:3.234015', 'rr-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('-4712-3-4 13:2:3.234015', 'rr-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('-1-3-4 13:2:3.234015', 'rr-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('0-3-4 13:2:3.234015', 'rr-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('1-3-4 13:2:3.234015', 'rr-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('9999-3-4 13:2:3.234015', 'rr-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('10000-3-4 13:2:3.234015', 'rr-mm-dd hh24:mi:ss.ff') from dual;

select to_timestamp('-4713-3-4 13:2:3.234015', 'rrrr-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('-4712-3-4 13:2:3.234015', 'rrrr-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('-1-3-4 13:2:3.234015', 'rrrr-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('0-3-4 13:2:3.234015', 'rrrr-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('1-3-4 13:2:3.234015', 'rrrr-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('9999-3-4 13:2:3.234015', 'rrrr-mm-dd hh24:mi:ss.ff') from dual;
select to_timestamp('10000-3-4 13:2:3.234015', 'rrrr-mm-dd hh24:mi:ss.ff') from dual;

SELECT TO_TIMESTAMP('0000-09-01','YYYYY-MM-DD') FROM DUAL;
SELECT TO_TIMESTAMP('0000-09-01','SYYYYY-MM-DD') FROM DUAL;
SELECT TO_TIMESTAMP('0000-09-01','SYYYY-Y-MM-DD') FROM DUAL;

SELECT TO_TIMESTAMP('25361','SSSSS') FROM DUAL;
SELECT TO_TIMESTAMP('25361-2','SSSSS-HH12') FROM DUAL;
SELECT TO_TIMESTAMP('25361-07','SSSSS-HH12') FROM DUAL;
SELECT TO_TIMESTAMP('25361-2','SSSSS-MI') FROM DUAL;
SELECT TO_TIMESTAMP('25361-10','SSSSS-MI') FROM DUAL;
SELECT TO_TIMESTAMP('25361-2','SSSSS-SS') FROM DUAL;
SELECT TO_TIMESTAMP('25361-41','SSSSS-SS') FROM DUAL;

SELECT TO_TIMESTAMP('2650000','J') FROM DUAL;
SELECT TO_TIMESTAMP('2650000-2542','J-YYYY') FROM DUAL;
SELECT TO_TIMESTAMP('2650000-2543','J-YYYY') FROM DUAL;
SELECT TO_TIMESTAMP('2650000-4','J-MM') FROM DUAL;
SELECT TO_TIMESTAMP('2650000-5','J-MM') FROM DUAL;
SELECT TO_TIMESTAMP('2650000-8','J-DD') FROM DUAL;
SELECT TO_TIMESTAMP('2650000-9','J-DD') FROM DUAL;

SELECT TO_TIMESTAMP('2012-245','YYYY-DDD') FROM DUAL;
SELECT TO_TIMESTAMP('2012-245-8','YYYY-DDD-MM') FROM DUAL;
SELECT TO_TIMESTAMP('2012-245-9','YYYY-DDD-MM') FROM DUAL;
SELECT TO_TIMESTAMP('2012-245-1','YYYY-DDD-DD') FROM DUAL;
SELECT TO_TIMESTAMP('2012-245-2','YYYY-DDD-DD') FROM DUAL;

--MS
SELECT TO_TIMESTAMP('-1','MS') FROM DUAL;
SELECT TO_TIMESTAMP('0','MS') FROM DUAL;
SELECT TO_TIMESTAMP('256','MS') FROM DUAL;
SELECT TO_TIMESTAMP('999','MS') FROM DUAL;
SELECT TO_TIMESTAMP('1000','MS') FROM DUAL;
--WW
SELECT TO_TIMESTAMP('0','WW') FROM DUAL;
SELECT TO_TIMESTAMP('1','WW') FROM DUAL;
SELECT TO_TIMESTAMP('10','WW') FROM DUAL;
SELECT TO_TIMESTAMP('53','WW') FROM DUAL;
SELECT TO_TIMESTAMP('54','WW') FROM DUAL;
--D
SELECT TO_TIMESTAMP('0','D') FROM DUAL;
SELECT TO_TIMESTAMP('1','D') FROM DUAL;
SELECT TO_TIMESTAMP('4','D') FROM DUAL;
SELECT TO_TIMESTAMP('7','D') FROM DUAL;
SELECT TO_TIMESTAMP('8','D') FROM DUAL;
--WW & D YYYY
SELECT TO_TIMESTAMP('2012-50-0','YYYY-WW-D') FROM DUAL;
SELECT TO_TIMESTAMP('2012-50-1','YYYY-WW-D') FROM DUAL;
SELECT TO_TIMESTAMP('2012-50-4','YYYY-WW-D') FROM DUAL;
SELECT TO_TIMESTAMP('2012-50-7','YYYY-WW-D') FROM DUAL;
SELECT TO_TIMESTAMP('2012-50-8','YYYY-WW-D') FROM DUAL;
SELECT TO_TIMESTAMP('2012-0-4','YYYY-WW-D') FROM DUAL;
SELECT TO_TIMESTAMP('2012-1-4','YYYY-WW-D') FROM DUAL;
SELECT TO_TIMESTAMP('2012-50-4','YYYY-WW-D') FROM DUAL;
SELECT TO_TIMESTAMP('2012-53-4','YYYY-WW-D') FROM DUAL;
SELECT TO_TIMESTAMP('2012-54-4','YYYY-WW-D') FROM DUAL;
--US
SELECT TO_TIMESTAMP('-1','US') FROM DUAL;
SELECT TO_TIMESTAMP('0','US') FROM DUAL;
SELECT TO_TIMESTAMP('99999','US') FROM DUAL;
SELECT TO_TIMESTAMP('999999','US') FROM DUAL;
SELECT TO_TIMESTAMP('1000000','US') FROM DUAL;
--W
SELECT TO_TIMESTAMP('-1','W') FROM DUAL;
SELECT TO_TIMESTAMP('0','W') FROM DUAL;
SELECT TO_TIMESTAMP('1','W') FROM DUAL;
SELECT TO_TIMESTAMP('5','W') FROM DUAL;
SELECT TO_TIMESTAMP('6','W') FROM DUAL;

SHOW nls_timestamp_format;
SELECT TO_TIMESTAMP('01') FROM DUAL;
SELECT TO_TIMESTAMP('01-Sep') FROM DUAL;
SELECT TO_TIMESTAMP('01-Sep-1998') FROM DUAL;
SELECT TO_TIMESTAMP('01-Sep-1998 11') FROM DUAL;
SELECT TO_TIMESTAMP('01-Sep-1998 11:12') FROM DUAL;
SELECT TO_TIMESTAMP('01-Sep-1998 11:12:13') FROM DUAL;
SELECT TO_TIMESTAMP('01-Sep-1998 11:12:13.12') FROM DUAL;
SELECT TO_TIMESTAMP('01-Sep-1998 11:12:13.12 PM') FROM DUAL;
SELECT TO_TIMESTAMP('01-Sep-1998 11:12:13 PM') FROM DUAL;
SET nls_timestamp_format = 'YYYY-MM-DD HH:MI:SS.FF AM';
SHOW nls_timestamp_format;
SELECT TO_TIMESTAMP('1998') FROM DUAL;
SELECT TO_TIMESTAMP('1998-12-28') FROM DUAL;
SELECT TO_TIMESTAMP('1998-12-28 01') FROM DUAL;
SELECT TO_TIMESTAMP('1998-12-28 01:02') FROM DUAL;
SELECT TO_TIMESTAMP('1998-12-28 01:02:03') FROM DUAL;
SELECT TO_TIMESTAMP('1998-12-28 01:02:03.12') FROM DUAL;
SELECT TO_TIMESTAMP('1998-12-28 01:02:03.12 AM') FROM DUAL;
SELECT TO_TIMESTAMP('1998-12-28 11:12:13 PM') FROM DUAL;
SELECT TO_TIMESTAMP('01') FROM DUAL;
select to_date('2018--12;30', 'yyyy//mm/,,dd') from dual;
select to_date('2018-12  30', 'yyyy//mm/dd') from dual;
select to_date('20181231124559','yyyy-MM-dd hh24:mi:ss'); 
SELECT (TO_DATE('1999(12*23 12  26','yyyy-MM-dd hh24:mi:ss')) FROM dual;   
SELECT (TO_DATE('1999(12*23 12    26','yyyy-MM-dd hh24:mi:ss')) FROM dual;      
SELECT (TO_DATE('1999(12*23 12
26','yyyy-MM-dd hh24:mi:ss')) FROM dual;      
SELECT to_date('10', 'SSSSS');
SELECT TO_TIMESTAMP('01', 'yyyy-MM-dd') FROM DUAL; 
SELECT TO_CHAR(TO_DATE('27OCT17', 'DDMONRR') ,'YYYY') "Year" FROM DUAL;
SELECT TO_CHAR(TO_DATE('27-OCT98', 'DD-MON-RR') ,'YYYY') "Year" FROM DUAL;
SELECT TO_CHAR(TO_DATE('27DEC98', 'DDMON-RR') ,'MON') "Month" FROM DUAL;
SELECT TO_CHAR(TO_DATE('27/OCT/17', 'DD-MON-RR') ,'DD') "Year" FROM DUAL;
SELECT to_date('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaa        aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                                                11111');
SELECT to_date('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaa        aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                              aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 11111');


/*----------------------------------------------
openGauss7777777: support timezone Asia/Beijing
----------------------------------------------*/
set timezone='Asia/Beijing';
select extract(timezone from now());

declare
    current_time_beijing  timestamptz;
    current_time_other      timestamptz;
begin
    set timezone='Asia/Beijing';
    current_time_beijing = transaction_timestamp();
    set timezone='UTC';
    current_time_other = transaction_timestamp();
    raise notice '%', age(current_time_beijing, current_time_other);
    set timezone='Asia/Shanghai';
    current_time_other = transaction_timestamp();
    raise notice '%', age(current_time_beijing, current_time_other);
    set timezone='Asia/Chongqing';
    current_time_other = transaction_timestamp();
    raise notice '%', age(current_time_beijing, current_time_other);
	
	set timezone to default;
end;
/
