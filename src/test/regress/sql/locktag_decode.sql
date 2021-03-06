---atomic ops tests
select locktag_decode('');
select locktag_decode('271b:0:0:0:6');
select locktag_decode('271b:0:0:0:0:6');
