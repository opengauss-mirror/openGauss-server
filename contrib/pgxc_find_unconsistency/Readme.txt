pgxc_find_unconsistency.sh is the script used for consistency checking. To find which transactions have wrong status with 'committed' and 'aborted' on different nodes in database cluster.
It shouldn't ocurr in normal situation. If some xids are found in xillist, we should analyse these xids using pgxc_xacts_iscommitted() and other views or functions for further investigation .

Format:
sh pgxc_find_unconsistency.sh dbname portnum

The function pgxc_parse_clog called in this script is a function extended from pg_parse_clog to show the status of all of the transactions fors nodes in the cluster. 
pg_parse_clog is a function shows all of the transaction's status in current node. The input parameters for pgxc_parse_clog or pg_parse_clog are null.
Normally users needn't call these two functions directly, just need to run the  pgxc_find_unconsistency.sh to find unconsistency if exists. 
And users can use pgxc_xacts_iscomitted(xid) to check the status of a transaction with id 'xid'. The type of input parameter 'xid' is int8.


