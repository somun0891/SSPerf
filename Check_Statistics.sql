
/* Check stats */

select * from sys.tables

use WideWorldImpors;
GO
sp_helpstats N'Sales.Invoices','ALL'

ALTER DATABASE WideWorldImporters
SET AUTO_CREATE_STATIhhhhhhCS ON; --DEFAULT=ON

ALTER DATABASE WideWorldImportersuuuu
SET AUTO_UPDATE_STATISTICS ON; --DEFAULT=ON


/* Use this TRACE FLAG to assist with checking which stats_id(either OWN _WA_* stats or Index stats) are used */
/* determine what statistics are used by the optimizer during a plan compilation */
/* Index stats mayn't be used if index seek is not possible and default _WA_* stats may be used */
DBCC TRACEON (3604 , -1)
DBCC tracestatus(3604)
--E.G.
SELECT * 
FROM WideWorldImporters.Sales.Invoices
where InvoiceID > 70000
option(querytraceon 2363, querytraceon 3604); --SQL 2014 and later , use 2363

--AUTO STAT THRESHOLD - UPDATE ATEAST 20% of rows + 500 to trigger auto update stats 
--It can take some time for SQL server to reset modification counter once threshold(20% + 500) is reached, 
--give it around 30 mins or so,check last_updated field after that
--If modification_counter = 0 , the stats are already auto-updated with recent last_updated time field
--must run the select query with filter leveraging the statistic to make modification_counter= 0
--when above the 20x+500 threshold.
SELECT OBJECT_SCHEMA_NAME(s.object_id)+'.' + OBJECT_NAME(s.object_id) AS ObjName
,s.object_id,	name ,s.stats_id,last_updated,rows,rows_sampled,steps,unfiltered_rows,
modification_counter
,CASE WHEN s.name like '%_WA_%' THEN 'System_Stats' ELSE 'Auto Created - User defined' END AS Stat_Type
FROM SYS.STATS s
outer apply SYS.dm_db_stats_properties(object_id , stats_id)  sp
where OBJECT_NAME(s.object_id) = N'SalesOrderHeader' --TABLE
and OBJECT_SCHEMA_NAME(s.object_id) = N'Sales' --SCHEMA

--How to create statistics
CREATE STATISTICS STAT_TOTALDUE ON AdventureWorks2017.Sales.SalesOrderHeader(TOTALDUE);

--Show statistics histogram
DBCC SHOW_STATISTICS('Sales.SalesOrderHeader' , STAT_TOTALDUE); --table , statname


--Explain various fields 
--RANGE_HI_KEY,RANGE_ROWS,EQ_ROWS,DISTINCT_RANGE_ROWS,AVG_RANGE_ROWS
select count(1) from AdventureWorks2017.Sales.SalesOrderHeader
where TotalDue > 14.2987 and TotalDue < 15.4479 --RANGE_ROWS --21 rows xpected

select count(1) from AdventureWorks2017.Sales.SalesOrderHeader
where TotalDue = 15.4479 --EQ_ROWS --201 rows xpected

select count(distinct TotalDue) from AdventureWorks2017.Sales.SalesOrderHeader
where TotalDue > 14.2987 and TotalDue < 15.4479 --DISTINCT_RANGE_ROWS --1 row xpected

--AVG_RANGE_ROWS = RANGE_ROWS /DISTINCT_RANGE_ROWS  --21 row xpected
select count(1)/count(distinct TotalDue) from AdventureWorks2017.Sales.SalesOrderHeader
where TotalDue > 14.2987 and TotalDue < 15.4479  -- 21 row expected
















/*********************************************************************/

/***************************************DEBUG START *********************************/

--SELECT * INTO Sales.Invoices_tmp from Sales.Invoices

declare @rowcount BIGINT;
select @rowcount = count(1) from Sales.Invoices_tmp --70510

--UPDATE ATEAST 20% of rows + 500 to trigger auto update stats 
select ( @rowcount * 0.20 ) + 500 --14602 --THRESHOLD

select distinct Orderid from Sales.Invoices_tmp order by Orderid

/* Use this TRACE FLAG to assist with checking which stats_id(either OWN _WA_* stats or Index stats) are used */
/* determine what statistics are used by the optimizer during a plan compilation */
DBCC TRACEON (3604 , -1)
DBCC tracestatus(3604)

SELECT * 
FROM WideWorldImporters.Sales.Invoices
where InvoiceID > 70000
option(querytraceon 2363, querytraceon 3604); --SQL 2014 and later , use 2363


SELECT * 
FROM WideWorldImporters.Sales.Invoices
where ConfirmedDeliveryTime > CAST('2016-05-01' AS DATETIME2(0))
OPTION (querytraceon 9204, querytraceon 9292 , querytraceon 2388 ,  querytraceon 3604 , RECOMPILE) 
GO
SP_HELPINDEX 'WideWorldImporters.Sales.Invoices'

--It can take some time for SQL server to reset modification counter once threshold is reached, 
--give it around 30 mins or so,check last_updated field after that

SELECT OBJECT_SCHEMA_NAME(s.object_id)+'.' + OBJECT_NAME(s.object_id) AS ObjName
,s.object_id,	name ,s.stats_id,last_updated,rows,rows_sampled,steps,unfiltered_rows,modification_counter
FROM SYS.STATS s
outer apply SYS.dm_db_stats_properties(object_id , stats_id)  sp
where OBJECT_NAME(s.object_id) = N'Invoices_tmp' and OBJECT_SCHEMA_NAME(s.object_id) = N'Sales' 

--select id, name as [IndexName] ,rowmodctr [Rows Modification] ,rowcnt [Number of Rows] from sys.sysindexes where ID =OBJECT_ID('Invoices_tmp')


--SELECT * FROM Sales.Invoices_tmp
--WHERE BillToCustomerID = 401 

--update  Sales.Invoices_tmp
--SET DELIVERYMETHODID = -1
--WHERE BillToCustomerID = 401 --22165(Iter-1) + 22165(Iter-2)
----AND DELIVERYMETHODID = 3

--DBCC FREEPROCCACHE

/* Clear plan cache */

--sql handle from query store but seems to be evicted here- 0x0200000053BCDB1C7C997B38FC701336CB79C9DE0AF9C5400000000000000000000000000000000000000000

/*
select text,sql_handle,plan_handle,query_hash,execution_count,*
FROM sys.dm_exec_query_stats eqs
CROSS APPLY sys.dm_exec_sql_text(eqs.sql_handle) est
where text like '%DELIVERYMETHODID%'

SELECT * FROM sys.dm_exec_query_stats eqs



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

*/

/***************************************DEBUG END *********************************/


























