create foreign table t_login(
	pid integer,
	PHY_ID integer,
	BSS_ID varchar(16),
	DD_APDATE timestamp,
	seq_No integer,
	primary key (pid, seq_No)
);

create foreign table t_name_list(
	obj_no varchar(16),
	obj_quality char,
	expire_date timestamp
);

CREATE OR REPLACE PROCEDURE login(v_cnt integer, ev_pid integer, ev_phyId integer, ev_ddApdate timestamp with time zone, out rv_loginWiFiwhiteList TEXT)
    AS
    DECLARE
		v_loop integer := 0;
		v_timestamp timestamp;
		v_seqNo integer;
		v_bssid varchar(16);
		v_flag integer;
    BEGIN
        rv_loginWiFiwhiteList := '';
		/*if connected times more than 0*/
		if v_cnt > 0 then	
			rv_loginWiFiwhiteList := 'Y';
			--RAISE EXCEPTION 'if v_cnt > 0, v_cnt = (%), rv_loginWiFiwhiteList := Y', v_cnt;
		else
			/*check if connected on last week*/
			select count(1) into v_cnt
			from t_login 
			where pid = ev_pid 
				and PHY_ID = ev_phyId 
				and BSS_ID like 'd4:68:ba%' 
				and extract(second from ev_ddApdate) - extract(second from DD_APDATE) <= 604800;
				--and since_epoch(second, ev_ddApdate) - since_epoch(second, DD_APDATE) <= 604800;
			/*if not connected on last week rv_loginWiFiwhiteList = N*/
			if (v_cnt = 0) then 
				rv_loginWiFiwhiteList := 'N'; 
				--RAISE EXCEPTION 'if v_cnt = 0, v_cnt = (%), rv_loginWiFiwhiteList := N', v_cnt;
			else 
				--RAISE EXCEPTION ' else, v_cnt = (%)', v_cnt;
				v_loop := 0;
				v_timestamp := ev_ddApdate;

				while (v_loop < v_cnt 
					and (rv_loginWiFiwhiteList != 'Y' or rv_loginWiFiwhiteList is null or rv_loginWiFiwhiteList = '')) loop
					 
					select seq_No into v_seqNo 
					from t_login 
					where pid = ev_pid 
						and PHY_ID = ev_phyId
						and BSS_ID like 'd4:68:ba%' 
						and extract(second from ev_ddApdate) - extract(second from DD_APDATE) <= 604800
						--and since_epoch(second, ev_ddApdate) - since_epoch(second, DD_APDATE) <= 604800 
						and DD_APDATE < v_timeStamp 
					order by DD_APDATE desc limit 1; 
					
					select DD_APDATE into v_timeStamp 
					from t_login 
					where pid = ev_pid 
						and seq_No = v_seqNo order by DD_APDATE limit 1; 
					
					select BSS_ID into v_bssid 
					from t_login 
					where pid = ev_pid 
						and seq_No = v_seqNo ; 
					
					select count(1) into v_flag 
					from t_name_list 
					where obj_no = v_bssid 
						and obj_quality = 'w' 
						and expire_date > ev_ddApdate; 
					
					if (v_flag > 0) then
						rv_loginWiFiwhiteList := 'Y'; 
						--RAISE EXCEPTION ' if(v_flag > 0), v_flag = (%)', v_flag;
					end if; 
					
					v_loop := v_loop + 1; 
				end loop; 
				
				if (v_loop = v_cnt 
					and (rv_loginWiFiwhiteList != 'Y' or rv_loginWiFiwhiteList is null or rv_loginWiFiwhiteList = '')) then
						rv_loginWiFiwhiteList := 'N';
				end if; 
			end if;
		end if;	
END;
/

insert into t_login values (1,1,'d4:68:ba%', current_timestamp, 1);
select login(1,1,1,current_timestamp);
insert into t_name_list values ('d4:68:ba%','y', current_timestamp + '1 day'::INTERVAL);
select login(0,1,1,current_timestamp);
insert into t_login values (1,1,'d4:68:ba:dd%', current_timestamp, 2);
select login(0,1,1,current_timestamp);
insert into t_name_list values ('d4:68:ba:da','w', current_timestamp + '1 day'::INTERVAL);
select login(0,1,1,current_timestamp);
insert into t_name_list values ('d4:68:ba%','w', current_timestamp + '1 day'::INTERVAL);
select login(0,1,1,current_timestamp);

drop foreign table t_login;
drop foreign table t_name_list;
