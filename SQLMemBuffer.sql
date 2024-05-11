
use wideworldimporters;
select * from sys.database_scoped_configurations

--Check users/sessions/CPU time/DiskIO/Blocked By
exec sp_who2

--check session health
SELECT es.session_id, ib.event_info,es.*
FROM sys.dm_exec_sessions AS es
CROSS APPLY sys.dm_exec_input_buffer(es.session_id, NULL) AS ib
WHERE es.session_id > 50
order by cpu_time desc
GO

--see memory occupied by sql servr
SELECT
	CAST(physical_memory_kb/1024.0/1024.0 AS DECIMAL(20,2)) as TotalMemoryGb,
	CAST(virtual_memory_kb/1024.0/1024.0  AS DECIMAL(20,2)) as VirtualMemory_page_files_on_disk_Gb,
	CAST(committed_kb/1024.0/1024.0 AS DECIMAL(20,2)) as BufferPoolMemInUse_Gb, --data/index pages/cache/plan
	CAST(committed_target_kb/1024.0/1024.0 AS DECIMAL(20,2)) as Target_Memory_Consumption_Gb
FROM sys.dm_os_sys_info;

--alternative query to above
SELECT total_physical_memory_kb / 1024.0 / 1024.0 AS TotalMemoryGb,
available_physical_memory_kb / 1024.0 /1024.0 AS Target_Memory_Consumption_Gb,
total_page_file_kb / 1024.0 / 1024.0 AS TotalPages_In_Memory_Gb,
available_page_file_kb / 1024.0 / 1024.0 AS AvailablePages_In_Memory_Gb,
system_memory_state_desc
FROM sys.dm_os_sys_memory


/************TEST QUERY START************/

--alter database master
--set compatibility_level = 120 --change from 140 to 130

--select compatibility_level,* from sys.databases 

----SELECT top(100000) v1.* FROM master.dbo.spt_values v1 , master.dbo.spt_values v2
----order by v1.number desc,v2.number desc 


----SLOW QUERY
--select  *  from WideWorldImporters.Sales.InvoiceLines INVL
--join WideWorldImporters.Sales.Invoices INV
--ON INVL.InvoiceID = INV.InvoiceID
--CROSS APPLY
--(
--SELECT SUM(INVL0.LineProfit) as TotalLineProfit
--FROM WideWorldImporters.Sales.InvoiceLines INVL0
--WHERE INVL0.InvoiceID = INV.InvoiceID
--)X

----BAD QUERY
--DECLARE @sometable table (id uniqueidentifier)

--insert into @sometable
--SELECT NEWID()
--FROM sys.columns A
--CROSS JOIN sys.columns B
--CROSS JOIN sys.columns C




--select top 10 * from WideWorldImporters.Sales.InvoiceLines INV2
--alter database master
--set compatibility_level = 140 --back to latest 

/************TEST QUERY END************/


/* shows memory grants for a particular session */

select * from sys.dm_exec_query_memory_grants

/* Find out on what resource your session is waiting on */

select * from sys.dm_os_waiting_tasks where 
SESSION_ID = 52 
--OR wait_type like '%RESOURCE_SEMAPHORE%'

/* Check mem usage of various sql server components */
--SQLQERESERVATIONS - this clerk is responsible for memory grants to queries and can reserve memory for sort operations */

SELECT TOP(50) [type] AS [ClerkType],name,
SUM(pages_kb) / 1024 AS [SizeMb]
FROM sys.dm_os_memory_clerks WITH (NOLOCK)
GROUP BY [type],name
ORDER BY SUM(pages_kb) DESC


/*
Operations requiring reserved mem - sorts, spools, joins and any other memory intensive op
Desired Memory – how much this plan wants in a perfect scenario, in kilobytes. Cached as part of the cached execution plan, and will remain consistent for all executions of this plan.
Granted Memory – how much this execution of the query actually got. This can vary between execution to execution depending on the server’s available workspace memory at the time. Yes, dear reader, this is one of the reasons why sometimes your query is slow, and sometimes it’s fast. This value is not cached as part of the plan.
Grant Wait Time – how long this execution of the query had to wait in order to get its memory. This relates to the Perfmon counter Memory Grants Pending. Not cached, since it varies from execution to execution.
Required memory: Minimum memory needed to run sort and hash join. It is called required because a query would not start without this memory available. SQL server uses this memory to create internal data structures to handle sort and hash join.
Max Used Memory – how much this execution used, and also not cached obviously. This is where things start to get interesting.
query workspace memory grant - When SQL Server starts running a query it needs to figure out a minimum amount of memory that’s appropriate for all the sorts, spools, joins, and other things it may need to do in memory in the query. 
Separate from buffer memory for data pages or memory for cached plans
Resource Semaphore waits -  new queries that come in may have to wait for a query workspace memory grant to run. This shows up as a RESOURCE_SEMAPHORE wait
*/

/*
Too low estimate -
1. SQL start spilling data to disk/tempdb

Too high estimate -
1. Query have to wait with higher grant wait time
2. Other session may suffer due to unnecessary memory hogging
3. SQL Server may clear out a lot of otherwise-useful memory just to run your query
4. “spools” are worktables being built in memory (and tempdb) in SQL Server, build “spool” temporary tables behind the scenes. 
loads the temp tables up in memory turn by turn and then had to go through a long, painful process of reading from them may be multiple times.

Possible issues - 
Parameter sniffing issues causes to overestimate/underestimate
Functions in join fields can cause to do more work to return few rows
Incorrect Cardinality estimate
*/
/* memory grants by session */
SELECT session_id, requested_memory_kb / 1024 as RequestedMemMb, 
granted_memory_kb / 1024 as GrantedMemMb 
, text
FROM sys.dm_exec_query_memory_grants qmg
CROSS APPLY sys.dm_exec_sql_text(sql_handle)

/* How much data pages make up a table */
use AdventureWorks2017;
SELECT OBJECT_SCHEMA_NAME(s.object_id) as [SchemaName],OBJECT_NAME(s.object_id) AS [TableName],
       s.row_count AS [TotalRowCount_Agg],
       s.used_page_count AS [UsedPages],
       s.reserved_page_count AS [ReservedPages]
	   ,s.[partition_id]
FROM sys.dm_db_partition_stats s
    JOIN sys.tables t
        ON s.object_id = t.object_id
WHERE OBJECT_NAME(s.object_id) = 'salesorderdetail';

 -- Find out number of data pages that make up the index
 use adventureworks2017;
 select index_id,in_row_data_page_count as NumPages,row_count,in_row_used_page_count
 from sys.dm_db_partition_Stats
 where object_id = object_id('person.person')
 and index_id =1 --check the iterator in the exec plan for index being used

/* page allocations breakdown by type - IAM/data/index pages */
SELECT OBJECT_NAME(pa.object_id) AS [TableName],
       pa.page_free_space_percent,
       pa.page_type_desc,
       pa.allocated_page_page_id,
       pa.extent_file_id
FROM sys.dm_db_database_page_allocations(DB_ID(), OBJECT_ID('sales.salesorderdetail'), NULL, NULL, 'DETAILED') AS pa;



/* mem allocation broken down by database */
SELECT TOP 5 DB_NAME(database_id) AS [Database Name],
COUNT(*) * 8/1024.0 AS [Cached Size (MB)]
FROM sys.dm_os_buffer_descriptors WITH (NOLOCK)
GROUP BY DB_NAME(database_id)
ORDER BY [Cached Size (MB)] DESC OPTION (RECOMPILE);

/* Aggregated info related to buffer cache */
SELECT
	COUNT(*) AS buffer_cache_pages,
	COUNT(*) * 8 / 1024 AS buffer_cache_used_MB --MEMORYCLERK_SQLBUFFERPOOL
FROM sys.dm_os_buffer_descriptors;
 

 /* buffer pages and size by TABLE */
  
SELECT
	objects.name AS object_name,
	objects.type_desc AS object_type_description,
	COUNT(*) AS buffer_cache_pages,
	COUNT(*) * 8 / 1024  AS buffer_cache_used_MB
FROM sys.dm_os_buffer_descriptors
INNER JOIN sys.allocation_units
ON allocation_units.allocation_unit_id = dm_os_buffer_descriptors.allocation_unit_id
INNER JOIN sys.partitions
ON ((allocation_units.container_id = partitions.hobt_id AND type IN (1,3))
OR (allocation_units.container_id = partitions.partition_id AND type IN (2)))
INNER JOIN sys.objects
ON partitions.object_id = objects.object_id
WHERE allocation_units.type IN (1,2,3)
AND objects.is_ms_shipped = 0
AND dm_os_buffer_descriptors.database_id = DB_ID()
GROUP BY objects.name,
		 objects.type_desc
ORDER BY COUNT(*) DESC;
 

 /*buffer pages and size by INDEX */
 SELECT
	indexes.name AS index_name,
	objects.name AS object_name,
	objects.type_desc AS object_type_description,
	COUNT(*) AS buffer_cache_pages,
	COUNT(*) * 8 / 1024  AS buffer_cache_used_MB
FROM sys.dm_os_buffer_descriptors
INNER JOIN sys.allocation_units
ON allocation_units.allocation_unit_id = dm_os_buffer_descriptors.allocation_unit_id
INNER JOIN sys.partitions
ON ((allocation_units.container_id = partitions.hobt_id AND type IN (1,3))
OR (allocation_units.container_id = partitions.partition_id AND type IN (2)))
INNER JOIN sys.objects
ON partitions.object_id = objects.object_id
INNER JOIN sys.indexes
ON objects.object_id = indexes.object_id
AND partitions.index_id = indexes.index_id
WHERE allocation_units.type IN (1,2,3)
AND objects.is_ms_shipped = 0
AND dm_os_buffer_descriptors.database_id = DB_ID()
GROUP BY indexes.name,
		 objects.name,
		 objects.type_desc
ORDER BY COUNT(*) DESC;

/* How much space occupied by a table and its index*/
  ;with cte as (  
  SELECT  
  t.name as TableName,  
  SUM (s.used_page_count) as used_pages_count,  
  SUM (CASE  
              WHEN (i.index_id < 2) THEN (in_row_data_page_count + lob_used_page_count + row_overflow_used_page_count)  
              ELSE lob_used_page_count + row_overflow_used_page_count  
          END) as pages  
  FROM sys.dm_db_partition_stats  AS s   
  JOIN sys.tables AS t ON s.object_id = t.object_id  
  JOIN sys.indexes AS i ON i.[object_id] = t.[object_id] AND s.index_id = i.index_id  
  GROUP BY t.name  
  )  
  ,cte2 as(select  
      cte.TableName,   
      (cte.pages * 8.) as TableSizeInKB,   
      ((CASE WHEN cte.used_pages_count > cte.pages   
                  THEN cte.used_pages_count - cte.pages  
                  ELSE 0   
            END) * 8.) as IndexSizeInKB  
  from cte  
 )  
 select TableName,TableSizeInKB,IndexSizeInKB,  
 case when (TableSizeInKB+IndexSizeInKB)>1024*1024   
 then cast((TableSizeInKB+IndexSizeInKB)/1024*1024 as varchar)+'GB'  
 when (TableSizeInKB+IndexSizeInKB)>1024   
 then cast((TableSizeInKB+IndexSizeInKB)/1024 as varchar)+'MB'  
 else cast((TableSizeInKB+IndexSizeInKB) as varchar)+'KB' end [TableSizeIn+IndexSizeIn]  
 from cte2  
 order by 2 desc 

/* Comparing  pages in memory vs total for each TABLE */

;WITH CTE_BUFFER_CACHE AS (
	SELECT
		objects.name AS object_name,
		objects.type_desc AS object_type_description,
		objects.object_id,
		COUNT(*) AS buffer_cache_pages,
		COUNT(*) * 8 / 1024  AS buffer_cache_used_MB
	FROM sys.dm_os_buffer_descriptors
	INNER JOIN sys.allocation_units
	ON allocation_units.allocation_unit_id = dm_os_buffer_descriptors.allocation_unit_id
	INNER JOIN sys.partitions
	ON ((allocation_units.container_id = partitions.hobt_id AND type IN (1,3))
	OR (allocation_units.container_id = partitions.partition_id AND type IN (2)))
	INNER JOIN sys.objects
	ON partitions.object_id = objects.object_id
	WHERE allocation_units.type IN (1,2,3)
	AND objects.is_ms_shipped = 0
	AND dm_os_buffer_descriptors.database_id = DB_ID()
	GROUP BY objects.name,
			 objects.type_desc,
			 objects.object_id
)
SELECT
	PARTITION_STATS.name,
	CTE_BUFFER_CACHE.object_type_description,
	CTE_BUFFER_CACHE.buffer_cache_pages,
	CTE_BUFFER_CACHE.buffer_cache_used_MB,
	PARTITION_STATS.total_number_of_used_pages,
	PARTITION_STATS.total_number_of_used_pages * 8 / 1024 AS total_mb_used_by_object,
	CAST((CAST(CTE_BUFFER_CACHE.buffer_cache_pages AS DECIMAL) / CAST(PARTITION_STATS.total_number_of_used_pages AS DECIMAL) * 100) AS DECIMAL(5,2)) AS percent_of_pages_in_memory
FROM CTE_BUFFER_CACHE
INNER JOIN (
	SELECT 
		objects.name,
		objects.object_id,
		SUM(used_page_count) AS total_number_of_used_pages
	FROM sys.dm_db_partition_stats
	INNER JOIN sys.objects
	ON objects.object_id = dm_db_partition_stats.object_id
	WHERE objects.is_ms_shipped = 0
	GROUP BY objects.name, objects.object_id) PARTITION_STATS
ON PARTITION_STATS.object_id = CTE_BUFFER_CACHE.object_id
ORDER BY CAST(CTE_BUFFER_CACHE.buffer_cache_pages AS DECIMAL) / CAST(PARTITION_STATS.total_number_of_used_pages AS DECIMAL) DESC;

/* Comparing  pages in memory vs total for each INDEX */
/* More in memory implies objects being used in queries which are not optimal */
SELECT
	indexes.name AS index_name,
	objects.name AS object_name,
	objects.type_desc AS object_type_description,
	COUNT(*) AS buffer_cache_pages,
	COUNT(*) * 8 / 1024  AS buffer_cache_used_MB,
	SUM(allocation_units.used_pages) AS pages_in_index,
	SUM(allocation_units.used_pages) * 8 /1024 AS total_index_size_MB,
	CAST((CAST(COUNT(*) AS DECIMAL) / CAST(SUM(allocation_units.used_pages) AS DECIMAL) * 100) AS DECIMAL(5,2)) AS percent_of_pages_in_memory
FROM sys.dm_os_buffer_descriptors
INNER JOIN sys.allocation_units
ON allocation_units.allocation_unit_id = dm_os_buffer_descriptors.allocation_unit_id
INNER JOIN sys.partitions
ON ((allocation_units.container_id = partitions.hobt_id AND type IN (1,3))
OR (allocation_units.container_id = partitions.partition_id AND type IN (2)))
INNER JOIN sys.objects
ON partitions.object_id = objects.object_id
INNER JOIN sys.indexes
ON objects.object_id = indexes.object_id
AND partitions.index_id = indexes.index_id
WHERE allocation_units.type IN (1,2,3)
AND objects.is_ms_shipped = 0
AND dm_os_buffer_descriptors.database_id = DB_ID()
GROUP BY indexes.name,
		 objects.name,
		 objects.type_desc
ORDER BY CAST((CAST(COUNT(*) AS DECIMAL) / CAST(SUM(allocation_units.used_pages) AS DECIMAL) * 100) AS DECIMAL(5,2)) DESC;


/* Find tables/indexed views with data pages in memory with HIGHER FREE SPACE PERCENT */
/*
Higher the no of buffer cache pages with more free space, more memory is occupied
 Check for wide tables/lob data
 A non-increasing key may result in page splits as data is written out-of-order. 
 A GUID as a key is the worst
*/
SELECT
	objects.name AS object_name,
	objects.type_desc AS object_type_description,
	COUNT(*) AS buffer_cache_pages,
	CAST(COUNT(*) * 8 AS DECIMAL) / 1024  AS buffer_cache_total_MB,
	CAST(SUM(CAST(dm_os_buffer_descriptors.free_space_in_bytes AS BIGINT)) AS DECIMAL) / 1024 / 1024 AS buffer_cache_free_space_in_MB,
	CAST((CAST(SUM(CAST(dm_os_buffer_descriptors.free_space_in_bytes AS BIGINT)) AS DECIMAL) / 1024 / 1024) / (CAST(COUNT(*) * 8 AS DECIMAL) / 1024) * 100 AS DECIMAL(5,2)) AS buffer_cache_percent_free_space
FROM sys.dm_os_buffer_descriptors
INNER JOIN sys.allocation_units
ON allocation_units.allocation_unit_id = dm_os_buffer_descriptors.allocation_unit_id
INNER JOIN sys.partitions
ON ((allocation_units.container_id = partitions.hobt_id AND type IN (1,3))
OR (allocation_units.container_id = partitions.partition_id AND type IN (2)))
INNER JOIN sys.objects
ON partitions.object_id = objects.object_id
WHERE allocation_units.type IN (1,2,3)
AND objects.is_ms_shipped = 0
AND dm_os_buffer_descriptors.database_id = DB_ID()
GROUP BY objects.name,
			objects.type_desc,
			objects.object_id
HAVING COUNT(*) > 0
ORDER BY COUNT(*) DESC;
