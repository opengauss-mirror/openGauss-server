create extension gms_output;
create extension gms_inaddr;

begin
gms_output.enable;
gms_output.put_line(gms_inaddr.get_host_address('localhost'));
gms_output.put_line(gms_inaddr.get_host_name('127.0.0.1'));
gms_output.put_line(gms_inaddr.get_host_address(null));
gms_output.put_line(gms_inaddr.get_host_name(null));
end
/

CREATE TABLE tb1_22 (col1 number primary key, check(col1<100),col2 nchar(15) not null,address1 varchar2 default gms_inaddr.GET_HOST_NAME,name1 varchar2 default gms_inaddr.get_host_address);

begin
gms_output.enable;
gms_output.put_line(gms_inaddr.get_host_address('localhostxx'));
gms_output.put_line(gms_inaddr.get_host_name('127.0.0.1'));
end;
/

begin
gms_output.enable;
gms_output.put_line(gms_inaddr.get_host_name('10.254.180.400'));
end;
/

begin
gms_output.enable;
gms_output.put_line(gms_inaddr.get_host_name('::1'));
end;
/

drop table tb1_22;
drop extension gms_output;
drop extension gms_inaddr;
