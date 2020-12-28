DECLARE
	group_count              int;
	i                        int;
	node_names               text;
BEGIN
	select count(*) from pgxc_group into group_count;
	IF group_count != 1
	THEN
		RAISE INFO 'current group_count is %', group_count;
		i := 1;
		FOR group_p in (select * from pgxc_group)
		LOOP
			select string_agg(node_name,',') from pgxc_node, pgxc_group where pgxc_node.oid = any(group_members) and pgxc_group.oid = group_p.oid into node_names;
			RAISE INFO 'group% name: %, members: %', i, group_p.group_name, node_names;
			i := i+1;
		END LOOP;
		RETURN;
	END IF;
	
	RAISE INFO 'check group_count ok.';
	RETURN;
END;
/

DECLARE
	group_info               record;
	node_names               text;
	query                    text;
	test_case                text;
BEGIN
	test_case := 'create group with buckets';
	select * from pgxc_group into group_info;
	select string_agg(node_name,',') from pgxc_node, pgxc_group where pgxc_node.oid = any(group_members) and pgxc_group.oid = group_info.oid into node_names;
	
	RAISE INFO 'test %...', test_case;
	query := 'create node group ' || group_info.group_name || '_dummy with(' || node_names || ') buckets(' || group_info.group_buckets || ')';
	execute query;
	
	query := 'drop node group ' || group_info.group_name || '_dummy';
	execute query;
	RAISE INFO 'test % ok', test_case;
	RETURN;
END;
/

DECLARE
	group_info               record;
	node_names               text;
	query                    text;
	test_case                text;
BEGIN
	test_case := 'create group with buckets number not correct';
	select * from pgxc_group into group_info;
	select string_agg(node_name,',') from pgxc_node, pgxc_group where pgxc_node.oid = any(group_members) and pgxc_group.oid = group_info.oid into node_names;
	
	RAISE INFO 'test %...', test_case;
	query := 'create node group ' || group_info.group_name || '_dummy with(' || node_names || ') buckets(' || group_info.group_buckets || ',0)';
	execute query;
	
	RAISE INFO 'test % ok', test_case;
	RETURN;
END;
/

DECLARE
	group_info               record;
	node_names               text;
	bucket_str               text;
	query                    text;
	test_case                text;
BEGIN
	test_case := 'create group with buckets not even: some node has more buckets';
	select * from pgxc_group into group_info;
	select string_agg(node_name,',') from pgxc_node, pgxc_group where pgxc_node.oid = any(group_members) and pgxc_group.oid = group_info.oid into node_names;
	
	RAISE INFO 'test %...', test_case;
	select regexp_replace(group_info.group_buckets,'(^|,)0($|,)','\11\2') into bucket_str;
	select regexp_replace(bucket_str,'(^|,)0($|,)','\11\2') into bucket_str;
	select regexp_replace(bucket_str,'(^|,)0($|,)','\11\2') into bucket_str;
	query := 'create node group ' || group_info.group_name || '_dummy with(' || node_names || ') buckets(' || bucket_str || ')';
	execute query;
	
	RAISE INFO 'test % ok', test_case;
	RETURN;
END;
/

DECLARE
	group_info               record;
	node_names               text;
	bucket_str               text;
	query                    text;
	test_case                text;
BEGIN
	test_case := 'create group with buckets not even: some node has no buckets';
	select * from pgxc_group into group_info;
	select string_agg(node_name,',') from pgxc_node, pgxc_group where pgxc_node.oid = any(group_members) and pgxc_group.oid = group_info.oid into node_names;
	
	RAISE INFO 'test %...', test_case;
	select regexp_replace(group_info.group_buckets,'(^|,)0($|,)','\11\2','g') into bucket_str;
	query := 'create node group ' || group_info.group_name || '_dummy with(' || node_names || ') buckets(' || bucket_str || ')';
	execute query;
	
	RAISE INFO 'test % ok', test_case;
	RETURN;
END;
/

DECLARE
	node_count               int;
	group_info               record;
	node_names               text;
	bucket_str               text;
	query                    text;
	test_case                text;
BEGIN
	test_case := 'create group with bucket id out of range';
	select * from pgxc_group into group_info;
	select string_agg(node_name,',') from pgxc_node, pgxc_group where pgxc_node.oid = any(group_members) and pgxc_group.oid = group_info.oid into node_names;
	
	RAISE INFO 'test %...', test_case;
	select count(*) from pgxc_node, pgxc_group where pgxc_node.oid = any(group_members) and pgxc_group.oid = group_info.oid into node_count;
	select regexp_replace(group_info.group_buckets,'(^|,)0($|,)','\1'||node_count||'\2') into bucket_str;
	query := 'create node group ' || group_info.group_name || '_dummy with(' || node_names || ') buckets(' || bucket_str || ')';
	execute query;
	
	RAISE INFO 'test % ok', test_case;
	RETURN;
END;
/
