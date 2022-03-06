\c postgres
select * from gs_get_shared_memctx_detail('CBBTopMemoryContext') limit 1;
select * from gs_get_shared_memctx_detail('AbnormalContext');
select * from gs_get_shared_memctx_detail(NULL);
select * from gs_get_session_memctx_detail('CBBTopMemoryContext') limit 1;
select * from gs_get_session_memctx_detail('AbnormalContext');
select * from gs_get_session_memctx_detail(NULL);
select * from gs_get_thread_memctx_detail(100, 'CBBTopMemoryContext');
select * from gs_get_thread_memctx_detail(100, NULL);
select gs_get_thread_memctx_detail(tid, 'CBBTopMemoryContext') from pv_thread_memory_context where contextname = 'CBBTopMemoryContext' limit 1;
select gs_get_thread_memctx_detail(tid, NULL) from pv_thread_memory_context where contextname = 'CBBTopMemoryContext';