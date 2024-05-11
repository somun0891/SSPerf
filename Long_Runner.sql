
/*
-- RUN OSTRESS TEST

setx rml C:\"Program Files"\"Microsoft Corporation"\RMLUtils

rml

ostress -q -SSTORM -d"WideWorldImporters" -n10 -r5 -Q"EXEC [Integration].[GetOrderUpdates] @LASTCUTOFF = '2010-01-01' , @NEWCUTOFF = '2016-01-01' ;" -o"C:\temp\ostress"

--More example-
ostress.exe -E -dmaster -Q"select name from sys.databases" -n30 -r10
*/

/* Long running queries */

use WideWorldImporters;
SELECT  st.text,
        qp.query_plan,
        qs.*
FROM    (
    SELECT  TOP 50 *
    FROM    sys.dm_exec_query_stats
    ORDER BY total_worker_time DESC
) AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
WHERE qs.max_worker_time > 300
      OR qs.max_elapsed_time > 300

--OR USE THIS ONE

SELECT TOP 10
t.TEXT QueryName,
s.execution_count AS ExecutionCount,
s.max_elapsed_time AS MaxElapsedTime, --microseconds
ISNULL(s.total_elapsed_time / 1000 / NULLIF(s.execution_count, 0), 0) AS AvgElapsedTime,--microseconds
s.creation_time AS LogCreatedOn,
ISNULL(s.execution_count / 1000 / NULLIF(DATEDIFF(s, s.creation_time, GETDATE()), 0), 0) AS FrequencyPerSec
,query_plan
FROM sys.dm_exec_query_stats s
CROSS APPLY sys.dm_exec_query_plan( s.plan_handle ) u
CROSS APPLY sys.dm_exec_sql_text( s.plan_handle ) t
ORDER BY MaxElapsedTime DESC

-- OR BETTER

SELECT TOP 25
	databases.name,
	dm_exec_sql_text.text AS TSQL_Text,
	CAST(CAST(dm_exec_query_stats.total_worker_time AS DECIMAL)/CAST(dm_exec_query_stats.execution_count AS DECIMAL) AS INT) as cpu_per_execution,
	CAST(CAST(dm_exec_query_stats.total_logical_reads AS DECIMAL)/CAST(dm_exec_query_stats.execution_count AS DECIMAL) AS INT) as logical_reads_per_execution,
	CAST(CAST(dm_exec_query_stats.total_elapsed_time AS DECIMAL)/CAST(dm_exec_query_stats.execution_count AS DECIMAL) AS INT) as elapsed_time_per_execution,
	dm_exec_query_stats.creation_time,  --when plan was created
	dm_exec_query_stats.execution_count,
	dm_exec_query_stats.total_worker_time AS total_cpu_time, --microseconds
	dm_exec_query_stats.max_worker_time AS max_cpu_time,  --microseconds
	dm_exec_query_stats.total_elapsed_time,   --microseconds
	dm_exec_query_stats.max_elapsed_time,   --microseconds
	CAST(CAST(dm_exec_query_stats.execution_count AS DECIMAL) / CAST((CASE WHEN DATEDIFF(HOUR, dm_exec_query_stats.creation_time, CURRENT_TIMESTAMP) = 0 
	            THEN 1 ELSE DATEDIFF(HOUR, dm_exec_query_stats.creation_time, CURRENT_TIMESTAMP) END) AS DECIMAL) AS INT) AS executions_per_hour,
	dm_exec_query_stats.total_logical_reads, 
	dm_exec_query_stats.max_logical_reads,
	dm_exec_query_stats.total_physical_reads, 
	dm_exec_query_stats.max_physical_reads,
	dm_exec_query_plan.query_plan,
	dm_exec_cached_plans.cacheobjtype,
	dm_exec_cached_plans.objtype,
	dm_exec_cached_plans.size_in_bytes
FROM sys.dm_exec_query_stats 
CROSS APPLY sys.dm_exec_sql_text(dm_exec_query_stats.plan_handle)
CROSS APPLY sys.dm_exec_query_plan(dm_exec_query_stats.plan_handle)
INNER JOIN sys.databases
ON dm_exec_sql_text.dbid = databases.database_id
INNER JOIN sys.dm_exec_cached_plans 
ON dm_exec_cached_plans.plan_handle = dm_exec_query_stats.plan_handle
WHERE databases.name = 'WideWorldImporters'
ORDER BY dm_exec_query_stats.max_logical_reads DESC;

