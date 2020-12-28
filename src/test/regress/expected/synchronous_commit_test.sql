set synchronous_commit=remote_apply;
show synchronous_commit;
 synchronous_commit
--------------------
 remote_apply
(1 row)

set synchronous_commit=2;
show synchronous_commit;
 synchronous_commit
--------------------
 remote_apply
(1 row)