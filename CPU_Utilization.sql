
-- Stress CPU Query
--DECLARE @T DATETIME, @F BIGINT;
--SET @T = GETDATE();
--WHILE DATEADD(SECOND,120,@T)>GETDATE()
--SET @F=POWER(2,30);

--CPU stress test - https://sqlworldwide.com/stress-test-using-sqlcmd-utility/

/* List of possible issues with high CPU */

--Wait statistics of the session
--Scheduler workload
--IO stalling queries
--Memory grant for session
--Blocking scenarios
--Optional Max degree of parallelism for query
--Execution plan operators consuming a lot of CPU
--Ad-hoc workload of the server
--Parameter sniffing configuration

/*
Operator Cost -
Cost of IO + Cost of CPU

IO Cost = Cost of 1 page + Cost of remaining pages
CPU Cost = Cost of 1 ROW + Cost of remaining ROWS
*/

--https://techcommunity.microsoft.com/t5/core-infrastructure-and-security/sql-high-cpu-scenario-troubleshooting-using-sys-dm-exec-query/ba-p/370314

--CHECKOUT SCRIPTS GARAGE FROM SQLMAESTROS


--Find whether SQL process using most of the CPU

DECLARE @ms_ticks_now BIGINT
SELECT @ms_ticks_now = ms_ticks
FROM sys.dm_os_sys_info;
SELECT TOP 60 record_id
	,dateadd(ms, - 1 * (@ms_ticks_now - [timestamp]), GetDate()) AS EventTime
	,[SQLProcess (%)]
	,SystemIdle
	,100 - SystemIdle - [SQLProcess (%)] AS [OtherProcess (%)]
FROM (
	SELECT record.value('(./Record/@id)[1]', 'int') AS record_id
		,record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS SystemIdle
		,record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS [SQLProcess (%)]
		,TIMESTAMP
	FROM (
		SELECT TIMESTAMP
			,convert(XML, record) AS record
		FROM sys.dm_os_ring_buffers
		WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
			AND record LIKE '%<SystemHealth>%'
		) AS x
	) AS y
ORDER BY record_id DESC

EXEC 


--Current Queries using CPU
SELECT 
	r.session_id
	,st.TEXT AS batch_text
	,SUBSTRING(st.TEXT, statement_start_offset / 2 + 1, (
			(
				CASE 
					WHEN r.statement_end_offset = - 1
						THEN (LEN(CONVERT(NVARCHAR(max), st.TEXT)) * 2)
					ELSE r.statement_end_offset
					END
				) - r.statement_start_offset
			) / 2 + 1) AS statement_text
	,qp.query_plan AS 'XML Plan'
	,r.cpu_time, r.total_elapsed_time
	,r.logical_reads, r.writes, r.dop
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS st
CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) AS qp
ORDER BY cpu_time DESC

--Historical Queries using CPU
SELECT TOP(10) qs.execution_count AS [Execution Count],
	(qs.total_logical_reads)/1000.0 AS [Total Logical Reads in ms],
	(qs.total_logical_reads/qs.execution_count)/1000.0 AS [Avg Logical Reads in ms],
	(qs.total_worker_time)/1000.0 AS [Total Worker Time in ms],
	(qs.total_worker_time/qs.execution_count)/1000.0 AS [Avg Worker Time in ms],
	(qs.total_elapsed_time)/1000.0 AS [Total Elapsed Time in ms],
	(qs.total_elapsed_time/qs.execution_count)/1000.0 AS [Avg Elapsed Time in ms],
	qs.creation_time AS [Creation Time]
	,t.text AS [Complete Query Text],
	SUBSTRING(t.[TEXT], statement_start_offset / 2 + 1, (
			(
				CASE 
					WHEN statement_end_offset = - 1
						THEN (LEN(CONVERT(NVARCHAR(max), t.[TEXT])) * 2)
					ELSE statement_end_offset
					END
				) - statement_start_offset
			) / 2 + 1) AS statement_text,
--eqp.query_plan AS SQLStatement
	qp.query_plan AS [Query Plan]
	,qs.query_hash
FROM sys.dm_exec_query_stats AS qs WITH (NOLOCK)
CROSS APPLY sys.dm_exec_sql_text(plan_handle) AS t
CROSS APPLY sys.dm_exec_query_plan(plan_handle) AS qp
WHERE t.dbid = DB_ID()
ORDER BY (qs.total_logical_reads/qs.execution_count) DESC;


/* Find Top 10 CPU Intensive Queries */
;WITH eqs
AS (
    SELECT 
         [execution_count]
        ,[total_worker_time]/1000  AS [TotalCPUTime_ms]
        ,[total_elapsed_time]/1000  AS [TotalDuration_ms]
        ,query_hash
        ,plan_handle
        ,[sql_handle]
    FROM sys.dm_exec_query_stats
    )
SELECT TOP 10 est.[text] as sqltext,
    eqs.*
FROM eqs
OUTER APPLY sys.dm_exec_query_plan(eqs.plan_handle) eqp
OUTER APPLY sys.dm_exec_sql_text(eqs.sql_handle) AS est
ORDER BY [TotalCPUTime_ms] DESC


/* Check for a busy CPU */
/*  Tuning expensive queries can also help reduce the number of runnable_tasks_count */
SELECT AVG(current_tasks_count) AS [Avg Current Task], 
AVG(runnable_tasks_count) AS [Avg Wait Task]  -- greater than zero implies , then CPU pressure may be present and blocking may occur
FROM sys.dm_os_schedulers
WHERE scheduler_id < 255
AND status = 'VISIBLE ONLINE'

/* display all statements being run and the associated scheduler that the statement is running on is */
SELECT 
a.scheduler_id ,
b.session_id,
 (SELECT TOP 1 SUBSTRING(s2.text,statement_start_offset / 2+1 , 
      ( (CASE WHEN statement_end_offset = -1 
         THEN (LEN(CONVERT(nvarchar(max),s2.text)) * 2) 
         ELSE statement_end_offset END)  - statement_start_offset) / 2+1))  AS sql_statement
FROM sys.dm_os_schedulers a 
INNER JOIN sys.dm_os_tasks b on a.active_worker_address = b.worker_address
INNER JOIN sys.dm_exec_requests c on b.task_address = c.task_address
CROSS APPLY sys.dm_exec_sql_text(c.sql_handle) AS s2 


