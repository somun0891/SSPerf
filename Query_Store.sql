
--Query Store essentials

-- Query store can be enabled at the database level NOT at server level

--Find and fix queries that have a regression in performance due to plan changes
--Overview of how often and in which context a query has been executed, helping the DBA on performance tuning tasks
--Overview of the historic plan changes for a given query
--Identity top n queries (by time, CPU time, IO, etc.) in the past x hours
--Analyze the use of resources (IO, CPU and memory)

/*
Max_duration ( long running transaction , locking , blocking etc)
Avg_cpu_time ( consistently high CPU)
Max_cpu_time (spike in CPU)
Max_rowcount (query that is fetching large amount of data, reporting query , missing where etc)
Avg_query_max_used_memory ( high mem usage)
Avg_tempdb_space_used ( high tempdb usage – mostly data doesn’t fit in RAM , insufficient memory granted , large group by etc)
*/

ALTER DATABASE AdventureWorks2017
SET QUERY_STORE (
    OPERATION_MODE = READ_WRITE,
    CLEANUP_POLICY = 
    (STALE_QUERY_THRESHOLD_DAYS = 30),
    DATA_FLUSH_INTERVAL_SECONDS = 3000, --50 * 60
    MAX_STORAGE_SIZE_MB = 500,
    INTERVAL_LENGTH_MINUTES = 15,
    SIZE_BASED_CLEANUP_MODE = AUTO,
    QUERY_CAPTURE_MODE = AUTO,
    MAX_PLANS_PER_QUERY = 50
);

ALTER DATABASE WideWorldImporters
SET QUERY_STORE (
    OPERATION_MODE = READ_WRITE,
    CLEANUP_POLICY = 
    (STALE_QUERY_THRESHOLD_DAYS = 30),
    DATA_FLUSH_INTERVAL_SECONDS = 3000,
    MAX_STORAGE_SIZE_MB = 500,
    INTERVAL_LENGTH_MINUTES = 15,
    SIZE_BASED_CLEANUP_MODE = AUTO,
    QUERY_CAPTURE_MODE = AUTO,
    MAX_PLANS_PER_QUERY = 50
);

 -- MONITOR QUERY STORE
 SELECT * FROM SYS.database_query_store_options

 --MONITOR MEM ALLOCATION FOR QUERY STORE OBJECTS
 SELECT type,
 sum(pages_kb) as Memory_Used_KB,
  sum(pages_kb)/1000 as Memory_Used_MB
 FROM SYS.dm_os_memory_clerks where type like '%querydisk%' OR type like '%QDS%'
 GROUP BY type;

--Check on avg how much compile duration attributed to a query hash
USE WideWorldImporters;
 SELECT 
  QUERY_HASH,
MIN(avg_compile_duration) AS MIN_compile_duration,
MAX(avg_compile_duration) AS MAX_compile_duration,
AVG(avg_compile_duration) AS AVG_compile_duration,
COUNT(QUERY_HASH) AS COUNT_OF_QUERY_HASH
FROM SYS.query_store_query 
GROUP BY 
  QUERY_HASH

SELECT 
  Q.QUERY_HASH,
  P.QUERY_PLAN_HASH,
MIN(avg_CPU_TIME) AS MIN_cpu_time,
MAX(avg_CPU_TIME) AS MAX_cpu_time,
AVG(avg_CPU_TIME) AS Avg_cpu_time,
COUNT(QUERY_HASH) AS COUNT_OF_QUERY_HASH
FROM 
SYS.query_store_query q 
	join SYS.query_store_plan p
		on p.query_id = q.query_id
	join SYS.query_store_runtime_stats rs
		on p.plan_id = rs.plan_id
GROUP BY 
  Q.QUERY_HASH,
  P.QUERY_PLAN_HASH
ORDER BY MIN_cpu_time DESC


SELECT TOP 10 * FROM Sales.Invoicelines
SELECT * FROM SYS.STATS WHERE OBJECT_NAME(OBJECT_ID) = 'Invoicelines'

DBCC SHOW_STATISTICS('Sales.Invoicelines',PK_Sales_InvoiceLines)
         WITH STAT_HEADER
             ,HISTOGRAM;


  SELECT * FROM SYS.dm_os_memory_cache_hash_tables


 --SELECT * FROM SYS.query_store_wait_stats
 -- SELECT * FROM SYS.dm_exec_session_wait_stats

/* Find top 50 queries with the most expensive memory grants */
use WideWorldImporters;
SELECT top 50 *
from (
	SELECT
			DB_NAME() AS DBNAME,
			s.name AS SchemaName,
			o.name AS ObjectName,
			SUBSTRING(t.query_sql_text,1,1000) AS QueryText,
			p.query_plan_hash,
			p.query_plan,
			q.query_hash,
			(MAX(rs.max_query_max_used_memory)/128) AS MaxMemoryMB
		FROM sys.query_store_query q
		INNER JOIN sys.query_store_query_text t
			ON q.query_text_id = t.query_text_id
		INNER JOIN sys.query_store_plan p
			ON q.query_id = p.query_id
		INNER JOIN sys.query_store_runtime_stats rs
			ON p.plan_id = rs.plan_id
		INNER JOIN sys.query_store_runtime_stats_interval rsi
			ON rs.runtime_stats_interval_id = rsi.runtime_stats_interval_id
		LEFT JOIN sys.objects o
			ON q.OBJECT_ID = o.OBJECT_ID
		LEFT JOIN sys.schemas s
			ON o.schema_id = s.schema_id     
		WHERE rsi.start_time > DATEADD(day , -1 , GETUTCDATE())
		GROUP BY s.name, o.name, SUBSTRING(t.query_sql_text,1,1000),
			p.query_plan_hash,
			p.query_plan,
			q.query_hash
	)x 
ORDER BY MaxMemoryMB DESC
OPTION(RECOMPILE)


use WideWorldImporters;
--Find query plans and text in query store (historical)
SELECT 
    intv.start_time,
    intv.end_time,
    txt.query_sql_text,
	txt.statement_sql_handle,
	qry.query_id,
    pl.plan_id,
    qry.query_hash
	,qry.batch_sql_handle
	,rst.avg_cpu_time/1000 as cpu_time_ms
	,rst.avg_duration/1000 as duration_ms 
	,rst.avg_logical_io_reads as logical_io_reads_pages
	,rst.avg_logical_io_reads/128 as logical_io_reads_MB
	,rst.count_executions
	,CAST(rst.avg_tempdb_space_used/128.0 AS DECIMAL(10,2)) AS tempdb_space_used_MB
	,rst.avg_query_max_used_memory/128 as max_used_memory_MB
	,rst.avg_rowcount
	,TRY_CONVERT(xml , pl.query_plan) as plan_xml
	,qry.context_settings_id
	,txt.query_text_id
	,qry.is_internal_query
	,qry.last_execution_time
	,qry.last_optimize_cpu_time/1000 as last_optimize_cpu_time_ms
	,qry.last_optimize_duration/1000 as last_optimize_duration_ms
	,qry.avg_optimize_cpu_time/1000 as avg_optimize_cpu_time_ms
	,qry.avg_optimize_duration/1000 as avg_optimize_duration_ms
	,qry.query_parameterization_type
	,qry.query_parameterization_type_desc
	,pl.compatibility_level
	,pl.is_forced_plan
	,pl.is_parallel_plan
FROM sys.query_store_plan AS pl
INNER JOIN sys.query_store_query AS qry
    ON pl.query_id = qry.query_id
INNER JOIN sys.query_store_query_text AS txt
    ON qry.query_text_id = txt.query_text_id
INNER JOIN sys.query_store_runtime_stats rst
	on rst.plan_id = pl.plan_id
INNER JOIN sys.query_store_runtime_stats_interval intv
	on rst.runtime_stats_interval_id = intv.runtime_stats_interval_id
WHERE  txt.query_sql_text like '%InvoiceLines%' 



use WideWorldImporters;
select * from  sys.query_store_runtime_stats
where plan_id=824

select * from  sys.query_store_wait_stats
where plan_id=824

-- How to force a plan
EXEC sp_query_store_force_plan
    @query_id =43236,
    @plan_id = 706;


USE WIDEWORLDIMPORTERS;

--Find queries with MULTIPLE plans
SELECT 
q.query_id
,qt.query_sql_text
,convert(xml,p.query_plan) AS plan_xml
,p.last_execution_time
,q.query_hash
,p.query_plan_hash
FROM (
SELECT COUNT(*) AS count, q.query_id ,q.query_hash
FROM sys.query_store_query_text AS qt
JOIN sys.query_store_query AS q
    ON qt.query_text_id = q.query_text_id
JOIN sys.query_store_plan AS p
    ON p.query_id = q.query_id
GROUP BY q.query_id,q.query_hash
HAVING COUNT(distinct plan_id) > 1
) AS qm
JOIN sys.query_store_query AS q
    ON qm.query_id = q.query_id
JOIN sys.query_store_plan AS p
    ON q.query_id = p.query_id
JOIN sys.query_store_query_text qt 
    ON qt.query_text_id = q.query_text_id
ORDER BY query_id, plan_id;


-- Top 10 queries LAST executed on the server
SELECT TOP 10 qt.query_sql_text, q.query_id, 
    qt.query_text_id, p.plan_id, rs.last_execution_time
FROM sys.query_store_query_text AS qt 
JOIN sys.query_store_query AS q 
    ON qt.query_text_id = q.query_text_id 
JOIN sys.query_store_plan AS p 
    ON q.query_id = p.query_id 
JOIN sys.query_store_runtime_stats AS rs 
    ON p.plan_id = rs.plan_id
ORDER BY rs.last_execution_time DESC;


/* Force last known good plan */
ALTER DATABASE WIDEWORLDIMPORTERS
SET AUTOMATIC_TUNING (
	FORCE_LAST_GOOD_PLAN = ON
);

/* Free proc cache */
ALTER DATABASE SCOPED CONFIGURATION CLEAR PROCEDURE_CACHE;

/* Check query tuning recommendations */
USE WIDEWORLDIMPORTERS;
SELECT * FROM SYS.DM_DB_TUNING_RECOMMENDATIONS



--https://learn.microsoft.com/en-us/training/modules/use-sql-server-query-tuning-assistant/2-decide-qta-right





