#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="rep_db"

function test_1() {
	echo "create database and tables."
	exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
	exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"
	# Create some preexisting content on publisher
	exec_sql $case_db $pub_node1_port "CREATE TABLE tab_notrep AS SELECT generate_series(1,10) AS a"
	exec_sql $case_db $pub_node1_port "CREATE TABLE tab_ins AS SELECT generate_series(1,1002) AS a"
	exec_sql $case_db $pub_node1_port "CREATE TABLE tab_full AS SELECT generate_series(1,10) AS a"
	exec_sql $case_db $pub_node1_port "CREATE TABLE tab_full2 (x text)"
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_full2 VALUES ('a'), ('b'), ('b')"
	exec_sql $case_db $pub_node1_port "CREATE TABLE tab_rep (a int primary key)"
	exec_sql $case_db $pub_node1_port "CREATE TABLE tab_mixed (a int primary key, b text, c numeric(10,2))"
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_mixed (a, b, c) VALUES (1, 'foo', 1.1)"
	exec_sql $case_db $pub_node1_port "CREATE TABLE tab_include (a int, b text) WITH (storage_type = ustore)"
	exec_sql $case_db $pub_node1_port "CREATE INDEX covering ON tab_include USING ubtree (a) INCLUDE(b)"
	exec_sql $case_db $pub_node1_port "ALTER TABLE tab_include REPLICA IDENTITY FULL"
	exec_sql $case_db $pub_node1_port "CREATE TABLE tab_full_pk (a int primary key, b text)"
	exec_sql $case_db $pub_node1_port "ALTER TABLE tab_full_pk REPLICA IDENTITY FULL"
	# Let this table with REPLICA IDENTITY NOTHING, allowing only INSERT changes.
	exec_sql $case_db $pub_node1_port "CREATE TABLE tab_nothing (a int)"
	exec_sql $case_db $pub_node1_port "ALTER TABLE tab_nothing REPLICA IDENTITY NOTHING"
	# Replicate the changes without replica identity index
	exec_sql $case_db $pub_node1_port "CREATE TABLE tab_no_replidentity_index(c1 int)"
	exec_sql $case_db $pub_node1_port "CREATE INDEX idx_no_replidentity_index ON tab_no_replidentity_index(c1)"

	# Setup structure on subscriber
	exec_sql $case_db $sub_node1_port "CREATE TABLE tab_notrep (a int)"
	exec_sql $case_db $sub_node1_port "CREATE TABLE tab_ins (a int)"
	exec_sql $case_db $sub_node1_port "CREATE TABLE tab_full (a int)"
	exec_sql $case_db $sub_node1_port "CREATE TABLE tab_full2 (x text)"
	exec_sql $case_db $sub_node1_port "CREATE TABLE tab_rep (a int primary key)"
	exec_sql $case_db $sub_node1_port "CREATE TABLE tab_full_pk (a int primary key, b text)"
	exec_sql $case_db $sub_node1_port "ALTER TABLE tab_full_pk REPLICA IDENTITY FULL"
	exec_sql $case_db $sub_node1_port "CREATE TABLE tab_nothing (a int)"

	# different column count and order than on publisher
	exec_sql $case_db $sub_node1_port "CREATE TABLE tab_mixed (d text default 'local', c numeric(10,2), b text, a int primary key)"

	# replication of the table with included index
	exec_sql $case_db $sub_node1_port "CREATE TABLE tab_include (a int, b text) WITH (storage_type = ustore)"
	exec_sql $case_db $sub_node1_port "CREATE INDEX covering ON tab_include USING ubtree (a) INCLUDE(b)"
	exec_sql $case_db $sub_node1_port "ALTER TABLE tab_include REPLICA IDENTITY FULL"
	# replication of the table without replica identity index
	exec_sql $case_db $sub_node1_port "CREATE TABLE tab_no_replidentity_index(c1 int)"
	exec_sql $case_db $sub_node1_port "CREATE INDEX idx_no_replidentity_index ON tab_no_replidentity_index(c1)"
	
	# Setup logical replication
	echo "create publication and subscription."
	publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
	exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub"
	exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub_ins_only WITH (publish = insert)"
	exec_sql $case_db $pub_node1_port "ALTER PUBLICATION tap_pub ADD TABLE tab_rep, tab_full, tab_full2, tab_mixed, tab_include, tab_nothing, tab_full_pk, tab_no_replidentity_index"
	exec_sql $case_db $pub_node1_port "ALTER PUBLICATION tap_pub_ins_only ADD TABLE tab_ins"
	exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub, tap_pub_ins_only"

	# Wait for initial table sync to finish
	wait_for_subscription_sync $case_db $sub_node1_port

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_notrep")" = "0" ]; then
		echo "check non-replicated table is empty on subscriber success"
	else
		echo "$failed_keyword when check non-replicated table is empty on subscriber"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_ins")" = "1002" ]; then
		echo "check initial data was copied to subscriber success"
	else
		echo "$failed_keyword when check initial data was copied to subscriber"
		exit 1
	fi

	exec_sql $case_db $pub_node1_port "INSERT INTO tab_ins SELECT generate_series(1,50)"
	exec_sql $case_db $pub_node1_port "DELETE FROM tab_ins WHERE a > 20"
	exec_sql $case_db $pub_node1_port "UPDATE tab_ins SET a = -a"
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_rep SELECT generate_series(1,50)"
	exec_sql $case_db $pub_node1_port "DELETE FROM tab_rep WHERE a > 20"
	exec_sql $case_db $pub_node1_port "UPDATE tab_rep SET a = -a"
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_mixed VALUES (2, 'bar', 2.2)"
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_full_pk VALUES (1, 'foo'), (2, 'baz')"
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_nothing VALUES (generate_series(1,20))"
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_include SELECT generate_series(1,50)"
	exec_sql $case_db $pub_node1_port "DELETE FROM tab_include WHERE a > 20"
	exec_sql $case_db $pub_node1_port "UPDATE tab_include SET a = -a"
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_no_replidentity_index VALUES(1)"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(a), max(a) FROM tab_ins")" = "1052|1|1002" ]; then
		echo "check replicated inserts on subscriber success"
	else
		echo "$failed_keyword when check replicated inserts on subscriber"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(a), max(a) FROM tab_rep")" = "20|-20|-1" ]; then
		echo "check replicated changes on subscriber success"
	else
		echo "$failed_keyword when check replicated changes on subscriber"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM tab_mixed")" = "local|1.10|foo|1
local|2.20|bar|2" ]; then
		echo "check replicated changes with different column order success"
	else
		echo "$failed_keyword when check replicated changes with different column order"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_nothing")" = "20" ]; then
		echo "check replicated changes with REPLICA IDENTITY NOTHING success"
	else
		echo "$failed_keyword when check replicated changes with REPLICA IDENTITY NOTHING"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(a), max(a) FROM tab_include")" = "20|-20|-1" ]; then
		echo "check replicated changes with primary key index with included columns success"
	else
		echo "$failed_keyword when check replicated changes with primary key index with included columns"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT c1 FROM tab_no_replidentity_index")" = "1" ]; then
		echo "check value replicated to subscriber without replica identity index success"
	else
		echo "$failed_keyword when check value replicated to subscriber without replica identity index"
		exit 1
	fi
	# insert some duplicate rows
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_full SELECT generate_series(1,10)"

	# Test behaviour of ALTER PUBLICATION ... DROP TABLE
	#
	# When a publisher drops a table from publication, it should also stop sending
	# its changes to subscribers. We look at the subscriber whether it receives
	# the row that is inserted to the table on the publisher after it is dropped
	# from the publication.
	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(a), max(a) FROM tab_ins")" = "1052|1|1002" ]; then
		echo "check rows on subscriber before table drop from publication success"
	else
		echo "$failed_keyword when check rows on subscriber before table drop from publication"
		exit 1
	fi

	# Drop the table from publication
	exec_sql $case_db $pub_node1_port "ALTER PUBLICATION tap_pub_ins_only DROP TABLE tab_ins"
	# Insert a row in publisher, but publisher will not send this row to subscriber
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_ins VALUES(8888)"

	wait_for_catchup  $case_db $pub_node1_port "tap_sub"

	# Subscriber will not receive the inserted row, after table is dropped from
	# publication, so row count should remain the same.
	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(a), max(a) FROM tab_ins")" = "1052|1|1002" ]; then
		echo "check rows on subscriber after table drop from publication success"
	else
		echo "$failed_keyword when check rows on subscriber after table drop from publication"
		exit 1
	fi

	# Delete the inserted row in publisher
	exec_sql $case_db $pub_node1_port "DELETE FROM tab_ins WHERE a = 8888"
	# Add the table to publication again
	exec_sql $case_db $pub_node1_port "ALTER PUBLICATION tap_pub_ins_only ADD TABLE tab_ins"
	# Refresh publication after table is added to publication
	exec_sql $case_db $sub_node1_port "ALTER SUBSCRIPTION tap_sub REFRESH PUBLICATION"

	# Test replication with multiple publications for a subscription such that the
	# operations are performed on the table from the first publication in the list.

	# Create tables on publisher
	exec_sql $case_db $pub_node1_port "CREATE TABLE temp1 (a int)"
	exec_sql $case_db $pub_node1_port "CREATE TABLE temp2 (a int)"

	# Create tables on subscriber
	exec_sql $case_db $sub_node1_port "CREATE TABLE temp1 (a int)"
	exec_sql $case_db $sub_node1_port "CREATE TABLE temp2 (a int)"

	# Setup logical replication that will only be used for this test
	exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub_temp1 FOR TABLE temp1 WITH (publish = insert)"
	exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub_temp2 FOR TABLE temp2"

	exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub_temp1 CONNECTION '$publisher_connstr' PUBLICATION tap_pub_temp1, tap_pub_temp2"

	# Wait for initial table sync to finish
	wait_for_subscription_sync $case_db $sub_node1_port

	# Subscriber table will have no rows initially
	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM temp1")" = "0" ]; then
		echo "check initial rows on subscriber with multiple publications success"
	else
		echo "$failed_keyword when check initial rows on subscriber with multiple publications"
		exit 1
	fi

	# Insert a row into the table that's part of first publication in subscriber
	# list of publications.
	exec_sql $case_db $pub_node1_port "INSERT INTO temp1 VALUES (1)"

	wait_for_catchup $case_db $pub_node1_port "tap_sub_temp1"

	# Subscriber should receive the inserted row
	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM temp1")" = "1" ]; then
		echo "check rows on subscriber with multiple publications success"
	else
		echo "$failed_keyword when check rows on subscriber with multiple publications"
		exit 1
	fi

	# Drop subscription as we don't need it anymore
	exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION tap_sub_temp1"

	# Drop publications as we don't need them anymore
	exec_sql $case_db $pub_node1_port "DROP PUBLICATION tap_pub_temp1"
	exec_sql $case_db $pub_node1_port "DROP PUBLICATION tap_pub_temp2"

	# Clean up the tables on both publisher and subscriber as we don't need them
	exec_sql $case_db $pub_node1_port "DROP TABLE temp1"
	exec_sql $case_db $pub_node1_port "DROP TABLE temp2"
	exec_sql $case_db $sub_node1_port "DROP TABLE temp1"
	exec_sql $case_db $sub_node1_port "DROP TABLE temp2"

	# add REPLICA IDENTITY FULL so we can update
	exec_sql $case_db $pub_node1_port "ALTER TABLE tab_full REPLICA IDENTITY FULL"
	exec_sql $case_db $sub_node1_port "ALTER TABLE tab_full REPLICA IDENTITY FULL"
	exec_sql $case_db $pub_node1_port "ALTER TABLE tab_full2 REPLICA IDENTITY FULL"
	exec_sql $case_db $sub_node1_port "ALTER TABLE tab_full2 REPLICA IDENTITY FULL"
	exec_sql $case_db $pub_node1_port "ALTER TABLE tab_ins REPLICA IDENTITY FULL"
	exec_sql $case_db $sub_node1_port "ALTER TABLE tab_ins REPLICA IDENTITY FULL"
	# tab_mixed can use DEFAULT, since it has a primary key

	# and do the updates
	exec_sql $case_db $pub_node1_port "UPDATE tab_full SET a = a * a"
	exec_sql $case_db $pub_node1_port "UPDATE tab_full2 SET x = 'bb' WHERE x = 'b'"
	exec_sql $case_db $pub_node1_port "UPDATE tab_mixed SET b = 'baz' WHERE a = 1"
	exec_sql $case_db $pub_node1_port "UPDATE tab_full_pk SET b = 'bar' WHERE a = 1"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(a), max(a) FROM tab_full")" = "20|1|100" ]; then
		echo "check update works with REPLICA IDENTITY FULL and duplicate tuples success"
	else
		echo "$failed_keyword when check update works with REPLICA IDENTITY FULL and duplicate tuples"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT x FROM tab_full2 ORDER BY 1")" = "a
bb
bb" ]; then
		echo "check update works with REPLICA IDENTITY FULL and text datums success"
	else
		echo "$failed_keyword when check update works with REPLICA IDENTITY FULL and text datums"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM tab_mixed ORDER BY a")" = "local|1.10|baz|1
local|2.20|bar|2" ]; then
		echo "check update works with different column order and subscriber local values success"
	else
		echo "$failed_keyword when check update works with different column order and subscriber local values"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM tab_full_pk ORDER BY a")" = "1|bar
2|baz" ]; then
		echo "check update works with REPLICA IDENTITY FULL and a primary key success"
	else
		echo "$failed_keyword when check update works with REPLICA IDENTITY FULL and a primary key"
		exit 1
	fi

	# Check that subscriber handles cases where update/delete target tuple
	# is missing. 
	exec_sql $case_db $sub_node1_port "DELETE FROM tab_full_pk"

	# Note that the current location of the log file is not grabbed immediately
	# after reloading the configuration, but after sending one SQL command to
	# the node so that we are sure that the reloading has taken effect.
	logfile=$(get_log_file "sub_datanode1")

	location=$(awk 'END{print NR}' $logfile)

	exec_sql $case_db $pub_node1_port "UPDATE tab_full_pk SET b = 'quux' WHERE a = 1"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	content=$(tail -n +$location $logfile)
	if [[ "$content" =~ "logical replication did not find row for update in replication target relation \"tab_full_pk\"" ]]; then
		echo "check print updated row is missing success"
	else
		echo "$failed_keyword when check print updated row is missing"
		exit 1
	fi

	# check behavior with toasted values
	exec_sql $case_db $pub_node1_port "UPDATE tab_mixed SET b = repeat('xyzzy', 100000) WHERE a = 2"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT a, length(b), c, d FROM tab_mixed ORDER BY a")" = "1|3|1.10|local
2|500000|2.20|local" ]; then
		echo "check update transmits large column value success"
	else
		echo "$failed_keyword when check update transmits large column value"
		exit 1
	fi

	exec_sql $case_db $pub_node1_port "UPDATE tab_mixed SET c = 3.3 WHERE a = 2"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT a, length(b), c, d FROM tab_mixed ORDER BY a")" = "1|3|1.10|local
2|500000|3.30|local" ]; then
		echo "check update with non-transmitted large column value success"
	else
		echo "$failed_keyword when check update with non-transmitted large column value"
		exit 1
	fi

	# check behavior with dropped columns

	# this update should get transmitted before the column goes away
	exec_sql $case_db $pub_node1_port "UPDATE tab_mixed SET b = 'bar', c = 2.2 WHERE a = 2"
	exec_sql $case_db $pub_node1_port "ALTER TABLE tab_mixed DROP COLUMN b"
	exec_sql $case_db $pub_node1_port "UPDATE tab_mixed SET c = 11.11 WHERE a = 1"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM tab_mixed ORDER BY a")" = "local|11.11|baz|1
local|2.20|bar|2" ]; then
		echo "check update works with dropped publisher column success"
	else
		echo "$failed_keyword when check update works with dropped publisher column"
		exit 1
	fi

	exec_sql $case_db $sub_node1_port "ALTER TABLE tab_mixed DROP COLUMN d"
	exec_sql $case_db $pub_node1_port "UPDATE tab_mixed SET c = 22.22 WHERE a = 2"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT * FROM tab_mixed ORDER BY a")" = "11.11|baz|1
22.22|bar|2" ]; then
		echo "check update works with dropped subscriber column success"
	else
		echo "$failed_keyword when check update works with dropped subscriber column"
		exit 1
	fi

	# check that change of connection string and/or publication list causes
	# restart of subscription workers. We check the state along with
	# application_name to ensure that the walsender is (re)started.
	#
	# Not all of these are registered as tests as we need to poll for a change
	# but the test suite will fail nonetheless when something goes wrong.
	exec_sql $case_db $sub_node1_port "ALTER SUBSCRIPTION tap_sub SET PUBLICATION tap_pub_ins_only"

	exec_sql $case_db $pub_node1_port "INSERT INTO tab_ins SELECT generate_series(1001,1100)"
	exec_sql $case_db $pub_node1_port "DELETE FROM tab_rep"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(a), max(a) FROM tab_ins")" = "1152|1|1100" ]; then
		echo "check replicated inserts after subscription publication change success"
	else
		echo "$failed_keyword when check replicated inserts after subscription publication change"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(a), max(a) FROM tab_rep")" = "20|-20|-1" ]; then
		echo "check changes skipped after subscription publication change success"
	else
		echo "$failed_keyword when check changes skipped after subscription publication change"
		exit 1
	fi

	# check alter publication (relcache invalidation etc)
	exec_sql $case_db $pub_node1_port "ALTER PUBLICATION tap_pub_ins_only SET (publish = 'insert, delete')"
	exec_sql $case_db $pub_node1_port "ALTER PUBLICATION tap_pub_ins_only ADD TABLE tab_full"
	exec_sql $case_db $pub_node1_port "DELETE FROM tab_ins WHERE a > 0"
	exec_sql $case_db $sub_node1_port "ALTER SUBSCRIPTION tap_sub REFRESH PUBLICATION WITH (copy_data = false)"
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_full VALUES(0)"
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_notrep VALUES (11)"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM tab_notrep")" = "0" ]; then
		echo "check non-replicated table is empty on subscriber success"
	else
		echo "$failed_keyword when check non-replicated table is empty on subscriber"
		exit 1
	fi

	# note that data are different on provider and subscriber
	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(a), max(a) FROM tab_ins")" = "1052|1|1002" ]; then
		echo "check replicated deletes after alter publication success"
	else
		echo "$failed_keyword when check replicated deletes after alter publication"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(a), max(a) FROM tab_full")" = "21|0|100" ]; then
		echo "check replicated insert after alter publication success"
	else
		echo "$failed_keyword when check replicated insert after alter publication"
		exit 1
	fi

	# check restart on rename
	exec_sql $case_db $sub_node1_port "ALTER SUBSCRIPTION tap_sub RENAME TO tap_sub_renamed"

	poll_query_until $case_db $sub_node1_port "SELECT subname FROM pg_stat_subscription" "tap_sub_renamed" "Timed out while waiting for apply to restart after renaming SUBSCRIPTION"

	# check all the cleanup
	exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION tap_sub_renamed"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM pg_subscription")" = "0" ]; then
		echo "check subscription was dropped on subscriber success"
	else
		echo "$failed_keyword when check subscription was dropped on subscriber"
		exit 1
	fi

	if [ "$(exec_sql $case_db $pub_node1_port "SELECT count(*) FROM pg_replication_slots")" = "2" ]; then
		echo "check replication slot was dropped on publisher success"
	else
		echo "$failed_keyword when check replication slot was dropped on publisher"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM pg_subscription_rel")" = "0" ]; then
		echo "check subscription relation status was dropped on subscriber success"
	else
		echo "$failed_keyword when check subscription relation status was dropped on subscriber"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*) FROM pg_replication_origin")" = "0" ]; then
		echo "check replication origin was dropped on subscriber success"
	else
		echo "$failed_keyword when check replication origin was dropped on subscriber"
		exit 1
	fi

	# CREATE PUBLICATION while wal_level=hot_standby should succeed, with a WARNING
	restart_guc "pub_datanode1" "wal_level = hot_standby"
	logfile=$(get_log_file "pub_datanode1")

	location=$(awk 'END{print NR}' $logfile)

	exec_sql $case_db $pub_node1_port "BEGIN;CREATE TABLE skip_wal(a int);CREATE PUBLICATION tap_pub2 FOR TABLE skip_wal;ROLLBACK;"

	content=$(tail -n +$location $logfile)
	if [[ "$content" =~ "wal_level is insufficient to publish logical changes" ]]; then
		echo "check print wal_level warning success"
	else
		echo "$failed_keyword when check print wal_level warning"
		exit 1
	fi

	restart_guc "pub_datanode1" "wal_level = logical"
}

function tear_down() {
	exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS tap_sub"
	exec_sql $case_db $pub_node1_port "DROP PUBLICATION IF EXISTS tap_pub, tap_pub_ins_only"

	exec_sql $db $sub_node1_port "DROP DATABASE $case_db"
	exec_sql $db $pub_node1_port "DROP DATABASE $case_db"

	echo "tear down"
}

test_1
tear_down