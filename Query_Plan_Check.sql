
/*
-- RUN OSTRESS TEST

setx rml C:\"Program Files"\"Microsoft Corporation"\RMLUtils

rml

ostress -q -SSTORM -d"WideWorldImporters" -n10 -r5 -Q"EXEC [Integration].[GetOrderUpdates] @LASTCUTOFF = '2010-01-01' , @NEWCUTOFF = '2016-01-01' ;" -o"C:\temp\ostress"

--More example-
ostress.exe -E -dmaster -Q"select name from sys.databases" -n30 -r10
*/


/*
Why Is My Terrible Plan Still In Cache?
Creating the index doesn’t cause SQL Server to find related plans in the cache right away and flush them out. My execution plans will hang out in the cache until one of these things happens:

1.The query runs again. SQL Server sees that the schema on the table has changed and decides it needs to reconsider what to do, it recompiles the execution plan*.
2.The old plan gets “aged” out of cache if it isn’t used again (maybe pretty fast if there’s a bunch of memory pressure)
3.The plan is cleared by DBCC FREEPROCACHE, taking the database offline, a SQL Server restart, or a settings change that impacts the plan cache
*/

use adventureworks2017;
/* Recompile after index change/create - helps create fresh plan with fresh statistics but stil need to run the procedure again to see it*/
/* Identify procedure cache */
SELECT SCHEMA_NAME(SCHEMA_ID) SchemaName, name ProcedureName,
last_execution_time LastExecuted,
last_elapsed_time LastElapsedTime,
execution_count ExecutionCount,
cached_time CachedTime
FROM sys.dm_exec_procedure_stats ps JOIN
sys.objects o ON ps.object_id = o.object_id
WHERE ps.database_id = DB_ID();

/* Recompile SP */
--EXEC StoredProcedureName @parameters WITH RECOMPILE
--EXEC sp_recompile 'StoredProcedureName'


/* See all queries and their current plans  and their use/execution counts along with plan size*/
/* Plans may be evicted at any time by sql server ,USE QUERY STORE VIEWS FOR PLAN CACHE HISTORY */
SELECT 
query_plan,
text as SQL_TEXT,
qp.objectid,
plan_handle,
cp.cacheobjtype,
objtype,
size_in_bytes as plan_size,
usecounts AS execution_count,*
FROM sys.dm_exec_cached_plans cp
CROSS APPLY sys.dm_exec_query_plan(plan_handle) qp
CROSS APPLY sys.dm_exec_sql_text(plan_handle) t

/* total cache size */
SELECT
	SUM(CAST(dm_exec_cached_plans.size_in_bytes AS BIGINT)) / 1024 AS size_in_KB
FROM sys.dm_exec_cached_plans
WHERE dm_exec_cached_plans.cacheobjtype = N'Compiled Plan'
AND dm_exec_cached_plans.objtype IN(N'Adhoc', N'Prepared')
AND dm_exec_cached_plans.usecounts = 1;


--Search xml plan where the index is actually used, and will not include unrelated references.
USE AdventureWorks2017
DECLARE @index_name AS NVARCHAR(128) = '[PK_SalesOrderHeader_SalesOrderID]';
 
;WITH XMLNAMESPACES
    (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')   
SELECT
	stmt.value('(@StatementText)[1]', 'varchar(max)') AS sql_text,
	obj.value('(@Database)[1]', 'varchar(128)') AS database_name,
	obj.value('(@Schema)[1]', 'varchar(128)') AS schema_name,
	obj.value('(@Table)[1]', 'varchar(128)') AS table_name,
	obj.value('(@Index)[1]', 'varchar(128)') AS index_name,
	obj.value('(@IndexKind)[1]', 'varchar(128)') AS index_type,
	dm_exec_query_plan.query_plan,
	dm_exec_cached_plans.usecounts AS execution_count
FROM sys.dm_exec_cached_plans
CROSS APPLY sys.dm_exec_query_plan(plan_handle)
CROSS APPLY query_plan.nodes('/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple') AS nodes(stmt)
CROSS APPLY stmt.nodes('.//IndexScan/Object[@Index=sql:variable("@index_name")]') AS index_object(obj)


/* Find out how many plans use a particular index */
DECLARE @index_name AS NVARCHAR(128) = '[PK_SalesOrderHeader_SalesOrderID]';
 
;WITH XMLNAMESPACES
    (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')   
SELECT
	SUM(CAST(dm_exec_cached_plans.size_in_bytes AS BIGINT)) AS plan_cache_size_in_bytes,
	COUNT(*) AS number_of_plans
FROM sys.dm_exec_cached_plans
CROSS APPLY sys.dm_exec_query_plan(plan_handle)
CROSS APPLY query_plan.nodes('/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple') AS nodes(stmt)
CROSS APPLY stmt.nodes('.//IndexScan/Object[@Index=sql:variable("@index_name")]') AS index_object(obj)


--Search plan for finding queries using implicit conversions OR missing join predicate OR Warnings
;WITH XMLNAMESPACES
    (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')   
SELECT
	dm_exec_sql_text.text AS sql_text,
	CAST(CAST(dm_exec_query_stats.execution_count AS DECIMAL) / CAST((CASE WHEN DATEDIFF(HOUR, dm_exec_query_stats.creation_time, CURRENT_TIMESTAMP) = 0 THEN 1 ELSE DATEDIFF(HOUR, dm_exec_query_stats.creation_time, CURRENT_TIMESTAMP) END) AS DECIMAL) AS INT) AS executions_per_hour,
	dm_exec_query_stats.creation_time, 
	dm_exec_query_stats.execution_count,
	CAST(CAST(dm_exec_query_stats.total_worker_time AS DECIMAL)/CAST(dm_exec_query_stats.execution_count AS DECIMAL) AS INT) as cpu_per_execution,
	CAST(CAST(dm_exec_query_stats.total_logical_reads AS DECIMAL)/CAST(dm_exec_query_stats.execution_count AS DECIMAL) AS INT) as logical_reads_per_execution,
	CAST(CAST(dm_exec_query_stats.total_elapsed_time AS DECIMAL)/CAST(dm_exec_query_stats.execution_count AS DECIMAL) AS INT) as elapsed_time_per_execution,
	dm_exec_query_stats.total_worker_time AS total_cpu_time,
	dm_exec_query_stats.max_worker_time AS max_cpu_time, 
	dm_exec_query_stats.total_elapsed_time, 
	dm_exec_query_stats.max_elapsed_time, 
	dm_exec_query_stats.total_logical_reads, 
	dm_exec_query_stats.max_logical_reads,
	dm_exec_query_stats.total_physical_reads, 
	dm_exec_query_stats.max_physical_reads,
	dm_exec_query_plan.query_plan
FROM sys.dm_exec_query_stats
CROSS APPLY sys.dm_exec_sql_text(dm_exec_query_stats.sql_handle)
CROSS APPLY sys.dm_exec_query_plan(dm_exec_query_stats.plan_handle)
WHERE 
query_plan.exist('//PlanAffectingConvert') = 1
OR 
(
query_plan.exist('//RelOp[@PhysicalOp = "Index Scan"]') = 1
	   OR query_plan.exist('//RelOp[@PhysicalOp = "Clustered Index Scan"]') = 1
)
OR
(
query_plan.exist('//Warnings') = 1
)
AND query_plan.exist('//ColumnReference[@Database = "[AdventureWorks2017]"]') = 1
ORDER BY dm_exec_query_stats.total_worker_time DESC;



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
ISNULL(s.total_elapsed_time / 1000 / NULLIF(s.execution_count, 0), 0) AS AvgElapsedTime,--milliseconds
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


/* How to Identify Whether Optimizer is Reusing the Plans or Not
--start investigating the queries that have CountOfSQLHandles > 1.
*/

SELECT eqs.query_hash AS QueryHash
    ,COUNT(DISTINCT sql_handle) AS CountOfSQLHandles
    ,SUM(execution_count) AS NoOfExecutions  
    ,SUM(eqs.total_logical_reads) AS TotalLogicalReads 
    ,SUM(eqs.total_worker_time) AS TotalCPUTime_mcs
    ,SUM(eqs.total_elapsed_time) AS TotalDuration 
    ,MAX(est.[TEXT]) AS OneSuchQuery  -- ‘OneSuchQuery’ column gives us one of the queries that have different sql_handle but same query_hash.
FROM sys.dm_exec_query_stats eqs
CROSS APPLY sys.dm_exec_sql_text(eqs.sql_handle) est
--WHERE query_hash = 0x8D7EAB9799567E59
GROUP BY eqs.query_hash
-- HAVING COUNT(DISTINCT sql_handle) > 2


GO

/* Clear plan cache */

select text,sql_handle,plan_handle,query_hash,execution_count,*
FROM sys.dm_exec_query_stats eqs
CROSS APPLY sys.dm_exec_sql_text(eqs.sql_handle) est
where text like '%master.dbo.spt_values%'

/* For SP */
DECLARE @PlanHandle VARBINARY(64);

SELECT  @PlanHandle = deps.plan_handle
FROM    sys.dm_exec_procedure_stats AS deps
WHERE   deps.object_id = OBJECT_ID('Integration.GetOrderUpdates') AND deps.database_id = DB_ID();

SELECT  @PlanHandle; --show handle

IF @PlanHandle IS NOT NULL
    BEGIN
        DBCC FREEPROCCACHE(@PlanHandle);
    END
GO

--Check before removing plans from cache
SELECT 
t.TEXT QueryName,
s.execution_count AS ExecutionCount,
s.max_elapsed_time AS MaxElapsedTime, --microseconds
ISNULL(s.total_elapsed_time / 1000 / NULLIF(s.execution_count, 0), 0) AS AvgElapsedTime,--milli
s.creation_time AS LogCreatedOn,
ISNULL(s.execution_count / 1000 / NULLIF(DATEDIFF(s, s.creation_time, GETDATE()), 0), 0) AS FrequencyPerSec
,query_plan
,query_hash
,cp.cacheobjtype
,cp.objtype
,cp.refcounts
,cp.usecounts
,sql_handle
,s.plan_handle
FROM sys.dm_exec_query_stats s
join sys.dm_exec_cached_plans cp on cp.plan_handle = s.plan_handle
CROSS APPLY sys.dm_exec_query_plan( s.plan_handle ) u
CROSS APPLY sys.dm_exec_sql_text( s.plan_handle ) t
--where s.query_hash = 0xEEACE0315B6D5923
where t.TEXT like '%SalesOrderID%'
and cp.objtype = 'Prepared'

--Adhoc/Prepared/Dynamic SQL Query plan cache removal
DECLARE @PlanHandle TABLE(RN INT IDENTITY(1,1) , plan_handle VARBINARY(64));
DECLARE @MaxCtr INT;
DECLARE @Ctr INT = 1;
DECLARE @PLAN varbinary(64);
declare @SEARCHSTRING VARCHAR(512) = 'SalesOrderID'

INSERT INTO @PlanHandle( plan_handle)
SELECT  cp.plan_handle
FROM    sys.dm_exec_query_stats AS s
join sys.dm_exec_cached_plans cp on cp.plan_handle = s.plan_handle
CROSS APPLY sys.dm_exec_sql_text( s.plan_handle ) t
WHERE t.TEXT like '%' + @SEARCHSTRING + '%' and cp.objtype = 'Prepared'
and cp.plan_handle IS NOT NULL;

SELECT  plan_handle AS 'Plans to be removed' FROM @PlanHandle

SET @MaxCtr = (SELECT ISNULL(COUNT(plan_handle) ,0) FROM @PlanHandle); --show handle

WHILE @Ctr <= @MaxCtr
BEGIN
	SELECT @PLAN = plan_handle FROM @PlanHandle WHERE RN = @Ctr
   DBCC FREEPROCCACHE(@PLAN) WITH NO_INFOMSGS;
   set @Ctr = @Ctr + 1;
END



/* Check log space */
DBCC SQLPERF (LOGSPACE);


DECLARE @PLAN_HANDLE VARBINARY(64) = 0x06000B007F42723090D1C1C52A02000001000000000000000000000000000000000000000000000000000000

DBCC FREEPROCCACHE (@PLAN_HANDLE) WITH NO_INFOMSGS;

GO

/* Check procedure stats */

SELECT * FROM SYS.dm_exec_procedure_stats
select  text,* from sys.dm_exec_procedure_stats eps
CROSS APPLY sys.dm_exec_sql_text(eps.sql_handle) 


--Shows query plan xml
--SELECT * FROM sys.dm_exec_query_plan(0x05000700D21F5E3DD042C1C52A02000001000000000000000000000000000000000000000000000000000000)


use WideWorldImporters;
/* Query exec stats */
SELECT
t.[text],
[qs].[execution_count],
[qs].[total_logical_reads]/[qs].[execution_count] [AvgLogicalReads],
qs.total_worker_time AS [TotalExecutionTime_mcs],
qs.total_worker_time/qs.execution_count as [AvgExecutionTime_mcs],
qs.max_elapsed_time AS MaxElapsedTime,
ISNULL(qs.total_elapsed_time / 1000 / NULLIF(qs.execution_count, 0), 0) AS AvgElapsedTime,
qs.creation_time AS LogCreatedOn,
ISNULL(qs.execution_count / 1000 / NULLIF(DATEDIFF(s, qs.creation_time, GETDATE()), 0), 0) AS FrequencyPerSec,
[qs].[max_logical_reads],
[qs].[plan_handle],
[p].[query_plan],
qs.[sql_handle],
qs.query_hash,
qs.query_plan_hash,
qs.total_grant_kb
FROM sys.dm_exec_query_stats [qs]
join  sys.dm_exec_cached_plans cp on qs.plan_handle = cp.plan_handle
CROSS APPLY sys.dm_exec_sql_text([qs].sql_handle) [t]
CROSS APPLY sys.dm_exec_query_plan([qs].[plan_handle]) [p]
WHERE [t].text LIKE '%InvoiceLines%';
GO


--check plan attributes
select *
from  sys.dm_exec_cached_plans
CROSS APPLY sys.dm_exec_plan_attributes(plan_handle)



/* Force a parallel plan using ENABLE_PARALLEL_PLAN_PREFERENCE hint */
use AdventureWorksDW2017;
set showplan_xml on;
go
select
	fis.SalesAmount,
	dd.CalendarYear
from 
	dbo.FactInternetSales fis
	join dbo.DimDate dd on dd.DateKey = fis.OrderDateKey
option(use hint('ENABLE_PARALLEL_PLAN_PREFERENCE'));
go
set showplan_xml off;
go

