--default value
show track_thread_wait_status_interval;
--boundary setting
set track_thread_wait_status_interval=0;
show track_thread_wait_status_interval;
set track_thread_wait_status_interval=1440;
show track_thread_wait_status_interval;
--out of range value setting
set track_thread_wait_status_interval=-1;
set track_thread_wait_status_interval=1441;
--regular setting
set track_thread_wait_status_interval=5;
show track_thread_wait_status_interval;