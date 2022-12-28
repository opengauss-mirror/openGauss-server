#!/bin/sh

source $1/env_utils.sh $1 $2

case_db="constraints"

function test_1() {
	echo "create database and tables."
	exec_sql $db $pub_node1_port "CREATE DATABASE $case_db"
	exec_sql $db $sub_node1_port "CREATE DATABASE $case_db"

	# Setup structure on publisher
	exec_sql $case_db $pub_node1_port "CREATE TABLE tab_fk (bid int PRIMARY KEY);"
	exec_sql $case_db $pub_node1_port "CREATE TABLE tab_fk_ref (id int PRIMARY KEY, junk text, bid int REFERENCES tab_fk (bid));"

	# Setup structure on subscriber; column order intentionally different
	exec_sql $case_db $sub_node1_port "CREATE TABLE tab_fk (bid int PRIMARY KEY);"
	exec_sql $case_db $sub_node1_port "CREATE TABLE tab_fk_ref (id int PRIMARY KEY, bid int REFERENCES tab_fk (bid), junk text);"

	# Setup logical replication
	echo "create publication and subscription."
	publisher_connstr="port=$pub_node1_port host=$g_local_ip dbname=$case_db user=$username password=$passwd"
	exec_sql $case_db $pub_node1_port "CREATE PUBLICATION tap_pub FOR ALL TABLES;"
	exec_sql $case_db $sub_node1_port "CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub WITH (copy_data = false)"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	exec_sql $case_db $pub_node1_port "INSERT INTO tab_fk (bid) VALUES (1);"
	# "junk" value is meant to be large enough to force out-of-line storage
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_fk_ref (id, bid, junk) VALUES (1, 1, repeat(pi()::text,20000));"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(bid), max(bid) FROM tab_fk;")" = "1|1|1" ]; then
		echo "check replicated tab_fk inserts on subscriber success"
	else
		echo "$failed_keyword when check replicated tab_fk inserts on subscriber change"
		exit 1
	fi

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(bid), max(bid) FROM tab_fk_ref;")" = "1|1|1" ]; then
		echo "check replicated tab_fk_ref inserts on subscriber success"
	else
		echo "$failed_keyword when check replicated tab_fk_ref inserts on subscriber"
		exit 1
	fi

	# Drop the fk on publisher
	exec_sql $case_db $pub_node1_port "DROP TABLE tab_fk CASCADE;"

	# Insert data
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_fk_ref (id, bid) VALUES (2, 2);"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	# FK is not enforced on subscriber
	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(bid), max(bid) FROM tab_fk_ref;")" = "2|1|2" ]; then
		echo "check FK ignored on subscriber success"
	else
		echo "$failed_keyword when check FK ignored on subscriber"
		exit 1
	fi

	# Add replica trigger
	exec_sql $case_db $sub_node1_port "CREATE FUNCTION filter_basic_dml_fn() RETURNS TRIGGER AS \$\$
		BEGIN
			IF (TG_OP = 'INSERT') THEN
				IF (NEW.id < 10) THEN
					RETURN NEW;
				ELSE
					RETURN NULL;
				END IF;
			ELSIF (TG_OP = 'UPDATE') THEN
				RETURN NULL;
			ELSE
				RAISE WARNING 'Unknown action';
				RETURN NULL;
			END IF;
		END;
		\$\$ LANGUAGE plpgsql;
		CREATE TRIGGER filter_basic_dml_trg
			BEFORE INSERT OR UPDATE OF bid ON tab_fk_ref
			FOR EACH ROW EXECUTE PROCEDURE filter_basic_dml_fn();
		ALTER TABLE tab_fk_ref ENABLE REPLICA TRIGGER filter_basic_dml_trg;"
	
	# Insert data
	exec_sql $case_db $pub_node1_port "INSERT INTO tab_fk_ref (id, bid) VALUES (10, 10);"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	# The trigger should cause the insert to be skipped on subscriber
	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(bid), max(bid) FROM tab_fk_ref;")" = "2|1|2" ]; then
		echo "check replica insert trigger applied on subscriber success"
	else
		echo "$failed_keyword when check replica insert trigger applied on subscriber"
		exit 1
	fi

	# Update data
	exec_sql $case_db $pub_node1_port "UPDATE tab_fk_ref SET bid = 2 WHERE bid = 1;"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	# The trigger should cause the update to be skipped on subscriber
	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(bid), max(bid) FROM tab_fk_ref;")" = "2|1|2" ]; then
		echo "check replica update column trigger applied on subscriber success"
	else
		echo "$failed_keyword when check replica update column trigger applied on subscriber"
		exit 1
	fi

	# Update on a column not specified in the trigger, but it will trigger
	# anyway because logical replication ships all columns in an update.
	exec_sql $case_db $pub_node1_port "UPDATE tab_fk_ref SET id = 6 WHERE id = 1;"

	wait_for_catchup $case_db $pub_node1_port "tap_sub"

	if [ "$(exec_sql $case_db $sub_node1_port "SELECT count(*), min(id), max(id) FROM tab_fk_ref;")" = "2|1|2" ]; then
		echo "check column trigger applied even on update for other column success"
	else
		echo "$failed_keyword when check column trigger applied even on update for other column"
		exit 1
	fi
}

function tear_down() {
	exec_sql $case_db $sub_node1_port "DROP SUBSCRIPTION IF EXISTS tap_sub"
	exec_sql $case_db $pub_node1_port "DROP PUBLICATION IF EXISTS tap_pub"

	exec_sql $db $sub_node1_port "DROP DATABASE $case_db"
	exec_sql $db $pub_node1_port "DROP DATABASE $case_db"

	echo "tear down"
}

test_1
tear_down