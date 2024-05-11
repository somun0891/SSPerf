
USE MASTER;
/*
sp_BlitzCache -  Find expensive queries

Common sp_BlitzCache Parameters 
The @SortOrder parameter lets you pick which top 10 queries you want to examine:

reads - logical reads
CPU - from total_worker_time in sys.dm_exec_query_stats
executions - how many times the query ran since the CreationDate
xpm - executions per minute, derived from the CreationDate and LastExecution
recent compilations - if you're looking for things that are recompiling a lot
memory grant - if you're troubleshooting a RESOURCE_SEMAPHORE issue and want to find queries getting a lot of memory
writes - if you wanna find those pesky ETL processes

You can also use average or avg for a lot of the sorts, like @SortOrder = 'avg reads'
all - sorts by all the different sort order options, and returns a single result set of hot messes. This is a little tricky because:
We find the @Top N queries by CPU, then by reads, then writes, duration, executions, memory grant, spills, etc. If you want to set @Top > 10, 
you also have to set @BringThePain = 1 to make sure you understand that it can be pretty slow.
As we work through each pattern, we exclude the results from the prior patterns. So for example, we get the top 10 by CPU, 
and then when we go to get the top 10 by reads, we exclude queries that were already found in the top 10 by CPU. 
As a result, the top 10 by reads may not really be the top 10 by reads - because some of those might have been in the top 10 by CPU.
To make things even a little more confusing, in the Pattern column of the output, we only specify the first pattern that matched, 
not all of the patterns that matched. It would be cool if at some point in the future, we turned this into a comma-delimited list of patterns that a query matched, 
and then we'd be able to get down to a tighter list of top queries. 
For now, though, this is kinda unscientific.

query hash - filters for only queries that have multiple cached plans (even though they may all still be the same plan, just different copies stored.) 
If you use @SortOrder = 'query hash', you can specify a second sort order with a comma, like 'query hash, reads' in order to find only queries with multiple plans, 
sorted by the ones doing the most reads. The default second sort is CPU.
Other common parameters include:

@Top = 10 - by default, you get 10 plans, but you can ask for more. Just know that the more you get, the slower it goes.
@ExportToExcel = 1 - turn this on, and it doesn't return XML fields that would hinder you from copy/pasting the data into Excel.
@ExpertMode = 1 - turn this on, and you get more columns with more data. Doesn't take longer to run though.
@IgnoreSystemDBs = 0 - if you want to show queries in master/model/msdb. By default we hide these. Additionally hides queries from databases named dbadmin, 
                       dbmaintenance, and dbatools.
@MinimumExecutionCount = 0 - in servers like data warehouses where lots of queries only run a few times, you can set a floor number for examination.
*/


--EXEC sp_BlitzCache @ExpertMode = 1 ,@SortOrder  = 'CPU' , @Top = 10     --executions,spills,avg reads,writes, duration, memory grant, spills,query hash,recent compilations
--EXEC sp_BlitzCache @ExpertMode = 1 ,@SortOrder  = 'query hash, reads' , @Top = 10  
--EXEC sp_BlitzCache @OnlySqlHandles = '0x03000700D21F5E3D45FC6F0153AF000001000000000000000000000000000000000000000000000000000000'; 
/********************************************************************************/


/*
sp_BlitzWho: What Queries are Running Now

This is like sp_who, except it goes into way, way, way more details.

It's designed for query tuners, so it includes things like memory grants, degrees of parallelism, and execution plans.
*/


-- EXEC sp_BlitzWho


/********************************************************************************/

/*
p_BlitzIndex: Tune Your Indexes

SQL Server tracks your indexes: how big they are, how often they change, whether they're used to make queries go faster, and which indexes you should consider adding. 
The results columns are fairly self-explanatory.

By default, sp_BlitzIndex analyzes the indexes of the database you're in (your current context.)

Common parameters include:

@DatabaseName - if you want to analyze a specific database
@SchemaName, @TableName - if you pass in these, sp_BlitzIndex does a deeper-dive analysis of just one table. You get several result sets back describing more information 
about the table's current indexes, foreign key relationships, missing indexes, and fields in the table.
@GetAllDatabases = 1 - slower, but lets you analyze all the databases at once, up to 50. If you want more than 50 databases, you also have to pass in @BringThePain = 1.
@ThresholdMB = 250 - by default, we only analyze objects over 250MB because you're busy.

@Mode = 0 (default) - returns high-priority (1-100) advice on the most urgent index issues.
@Mode = 4: Diagnose Details - like @Mode 0, but returns even more advice (priorities 1-255) with things you may not be able to fix right away, 
and things we just want to warn you about.
@Mode = 1: Summarize - total numbers of indexes, space used, etc per database.
@Mode = 2: Index Usage Details - an inventory of your existing indexes and their usage statistics. Great for copy/pasting into Excel to do slice & dice analysis. 
			This is the only mode that works with the @Output parameters: you can export this data to table on a monthly basis if you need to go back and look to see which indexes 
			were used over time.
@Mode = 3: Missing Indexes - an inventory of indexes SQL Server is suggesting. Also great for copy/pasting into Excel for later analysis.

sp_BlitzIndex focuses on mainstream index types. Other index types have varying amounts of support:

Fully supported: rowstore indexes, columnstore indexes, temporal tables.
Columnstore indexes: fully supported. Key columns are shown as includes rather than keys since they're not in a specific order.
In-Memory OLTP (Hekaton): unsupported. These objects show up in the results, but for more info, you'll want to use sp_BlitzInMemoryOLTP instead.
Graph tables: unsupported. These objects show up in the results, but we don't do anything special with 'em, like call out that they're graph tables.
Spatial indexes: unsupported. We call out that they're spatial, but we don't do any special handling for them.
XML indexes: unsupported. These objects show up in the results, but we don't include the index's columns or sizes.

Advanced sp_BlitzIndex Parameters
In addition to the parameters common to many of the stored procedures, here are the ones specific to sp_BlitzIndex:

@SkipPartitions = 1 - add this if you want to analyze large partitioned tables. We skip these by default for performance reasons.
@SkipStatistics = 0 - right now, by default, we skip statistics analysis because we've had some performance issues on this.
@Filter = 0 (default) - 1=No low-usage warnings for objects with 0 reads. 2=Only warn for objects >= 500MB
@OutputDatabaseName, @OutputSchemaName, @OutputTableName - these only work for @Mode = 2, index usage detail.

*/

-- sp_BlitzIndex @DatabaseName = 'AdventureWorksDW2017' , @Mode = 2 



/*

sp_BlitzLock: Deadlock Analysis
Checks either the System Health session or a specific Extended Event session that captures deadlocks and parses out all the XML for you.

Parameters you can use:

@Top: Use if you want to limit the number of deadlocks to return. This is ordered by event date ascending.
@DatabaseName: If you want to filter to a specific database
@StartDate: The date you want to start searching on.
@EndDate: The date you want to stop searching on.
@ObjectName: If you want to filter to a specific table. The object name has to be fully qualified 'Database.Schema.Table'
@StoredProcName: If you want to search for a single stored procedure. Don't specify a schema or database name - just a stored procedure name alone is all you need, 
                 and if it exists in any schema (or multiple schemas), we'll find it.
@AppName: If you want to filter to a specific application.
@HostName: If you want to filter to a specific host.
@LoginName: If you want to filter to a specific login.
@EventSessionPath: If you want to point this at an XE session rather than the system health session.

*/

-- sp_BlitzLock @Top = 10 , @DatabaseName = 'AdventureWorksDW2017' , @StartDate = '2024-05-01' , @EndDate = '2024-05-31' 

-- sp_BlitzLock @Top = 10 , @DatabaseName = 'AdventureWorksDW2017' , @StartDate = '2024-05-01' , @EndDate = '2024-05-31' @StoredProcName = 'my sproc'