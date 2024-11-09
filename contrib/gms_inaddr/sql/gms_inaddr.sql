create extension gms_output;
create extension gms_inaddr;

begin
gms_output.enable;
gms_output.put_line(gms_inaddr.get_host_address('localhost'));
gms_output.put_line(gms_inaddr.get_host_name('127.0.0.1'));
end
/

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

drop extension gms_output;
drop extension gms_inaddr;
