-- SQL Server Indexing Scripts
--
-- Scripts provided by MSSQLTips.com are from various contributors. Use links below to learn more about the scripts.
-- 
-- Be careful using any of these scripts. Test all scripts in Test/Dev prior to using in Production environments.
-- Please refer to the disclaimer policy: https://www.mssqltips.com/disclaimer/
-- Please refer to the copyright policy: https://www.mssqltips.com/copyright/
--
-- Note, these scripts are meant to be run individually.
--
-- Have a script to contribute or an update?  Send an email to: tips@mssqltips.com


---------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------
-- Purpose: This query will return information about indexes and index usage.  Run this in the user database.
-- More Information: https://www.mssqltips.com/sqlservertip/1545/deeper-insight-into-used-and-unused-indexes-for-sql-server/

SELECT PVT.SCHEMANAME, PVT.TABLENAME, PVT.INDEXNAME, PVT.INDEX_ID, [1] AS COL1, [2] AS COL2, [3] AS COL3, [4] AS COL4,  [5] AS COL5, [6] AS COL6, [7] AS COL7, B.USER_SEEKS, B.USER_SCANS, B.USER_LOOKUPS, B.USER_UPDATES 
FROM   (SELECT SCHEMA_NAME(A.SCHEMA_id) AS SCHEMANAME,
               A.NAME AS TABLENAME, 
               A.OBJECT_ID, 
               B.NAME AS INDEXNAME, 
               B.INDEX_ID, 
               D.NAME AS COLUMNNAME, 
               C.KEY_ORDINAL 
        FROM   SYS.OBJECTS A 
               INNER JOIN SYS.INDEXES B ON A.OBJECT_ID = B.OBJECT_ID 
               LEFT JOIN SYS.INDEX_COLUMNS C ON B.OBJECT_ID = C.OBJECT_ID AND B.INDEX_ID = C.INDEX_ID 
               LEFT JOIN SYS.COLUMNS D ON C.OBJECT_ID = D.OBJECT_ID AND C.COLUMN_ID = D.COLUMN_ID 
        WHERE  A.TYPE = 'U') P 
       PIVOT 
       (MIN(COLUMNNAME) 
        FOR KEY_ORDINAL IN ( [1],[2],[3],[4],[5],[6],[7] ) ) AS PVT 
INNER JOIN SYS.DM_DB_INDEX_USAGE_STATS B ON PVT.OBJECT_ID = B.OBJECT_ID AND PVT.INDEX_ID = B.INDEX_ID AND B.DATABASE_ID = DB_ID() 
UNION -- below returns indexes not used with usage information
SELECT SCHEMANAME, TABLENAME, INDEXNAME, INDEX_ID, [1] AS COL1, [2] AS COL2, [3] AS COL3, [4] AS COL4, [5] AS COL5, [6] AS COL6, [7] AS COL7, 0, 0, 0, 0 
FROM   (SELECT SCHEMA_NAME(A.SCHEMA_id) AS SCHEMANAME,
               A.NAME AS TABLENAME, 
               A.OBJECT_ID, 
               B.NAME AS INDEXNAME, 
               B.INDEX_ID, 
               D.NAME AS COLUMNNAME, 
               C.KEY_ORDINAL 
        FROM   SYS.OBJECTS A 
               INNER JOIN SYS.INDEXES B ON A.OBJECT_ID = B.OBJECT_ID 
			   LEFT JOIN SYS.INDEX_COLUMNS C ON B.OBJECT_ID = C.OBJECT_ID AND B.INDEX_ID = C.INDEX_ID 
               LEFT JOIN SYS.COLUMNS D ON C.OBJECT_ID = D.OBJECT_ID AND C.COLUMN_ID = D.COLUMN_ID 
        WHERE  A.TYPE = 'U') P 
       PIVOT 
       (MIN(COLUMNNAME) 
        FOR KEY_ORDINAL IN ( [1],[2],[3],[4],[5],[6],[7] ) ) AS PVT 
WHERE  NOT EXISTS (SELECT OBJECT_ID, 
                          INDEX_ID 
                   FROM   SYS.DM_DB_INDEX_USAGE_STATS B 
                   WHERE  DATABASE_ID = DB_ID(DB_NAME()) 
                          AND PVT.OBJECT_ID = B.OBJECT_ID 
                          AND PVT.INDEX_ID = B.INDEX_ID) 
ORDER BY SCHEMANAME, TABLENAME, INDEX_ID; 


---------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------
-- Purpose: This query will find indexes with duplicate columns. Run this in the user database.
-- More Information: https://www.mssqltips.com/sqlservertip/3604/identify-sql-server-indexes-with-duplicate-columns/

select t1.tablename,t1.indexname,t1.columnlist,t2.indexname,t2.columnlist from
   (select distinct object_name(i.object_id) tablename,i.name indexname,
             (select distinct stuff((select ', ' + c.name
                                       from sys.index_columns ic1 inner join 
                                            sys.columns c on ic1.object_id=c.object_id and 
                                                             ic1.column_id=c.column_id
                                      where ic1.index_id = ic.index_id and 
                                            ic1.object_id=i.object_id and 
                                            ic1.index_id=i.index_id
                                      order by index_column_id FOR XML PATH('')),1,2,'')
                from sys.index_columns ic 
               where object_id=i.object_id and index_id=i.index_id) as columnlist
       from sys.indexes i inner join 
    	    sys.index_columns ic on i.object_id=ic.object_id and 
                                    i.index_id=ic.index_id inner join
            sys.objects o on i.object_id=o.object_id 
      where o.is_ms_shipped=0) t1 inner join
   (select distinct object_name(i.object_id) tablename,i.name indexname,
             (select distinct stuff((select ', ' + c.name
                                       from sys.index_columns ic1 inner join 
                                            sys.columns c on ic1.object_id=c.object_id and 
                                                             ic1.column_id=c.column_id
                                      where ic1.index_id = ic.index_id and 
                                            ic1.object_id=i.object_id and 
                                            ic1.index_id=i.index_id
                                      order by index_column_id FOR XML PATH('')),1,2,'')
                from sys.index_columns ic 
               where object_id=i.object_id and index_id=i.index_id) as columnlist
       from sys.indexes i inner join 
    	    sys.index_columns ic on i.object_id=ic.object_id and 
                                    i.index_id=ic.index_id inner join
            sys.objects o on i.object_id=o.object_id 
 where o.is_ms_shipped=0) t2 on t1.tablename=t2.tablename and 
       substring(t2.columnlist,1,len(t1.columnlist))=t1.columnlist and 
       (t1.columnlist<>t2.columnlist or 
         (t1.columnlist=t2.columnlist and t1.indexname<>t2.indexname))

---------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------
-- Purpose: This query will list all indexes in the database and show index columns and included columns. Run this in the user database.
-- More Information: https://www.mssqltips.com/sqlservertip/2914/rolling-up-multiple-rows-into-a-single-row-and-column-for-sql-server-data/

SELECT 
   SCHEMA_NAME(ss.SCHEMA_id) AS SchemaName,
   ss.name as TableName, 
   ss2.name as IndexName, 
   ss2.index_id,
   ss2.type_desc,
   STUFF((SELECT ', ' + name 
    from sys.index_columns a inner join sys.all_columns b on a.object_id = b.object_id and a.column_id = b.column_id and a.object_id = ss.object_id and a.index_id = ss2.index_id and is_included_column = 0
	order by a.key_ordinal
    FOR XML PATH('')), 1, 2, '') IndexColumns,
   STUFF((SELECT ', ' + name 
    from sys.index_columns a inner join sys.all_columns b on a.object_id = b.object_id and a.column_id = b.column_id and a.object_id = ss.object_id and a.index_id = ss2.index_id and is_included_column = 1
    FOR XML PATH('')), 1, 2, '') IncludedColumns
FROM sys.objects SS INNER JOIN SYS.INDEXES ss2 ON ss.OBJECT_ID = ss2.OBJECT_ID 
WHERE ss.type = 'U'
ORDER BY 1, 2, 3         


---------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------
-- Purpose: This query will list all indexes in the database and show index columns and included columns 
-- using STRING_AGG (SQL 2017 and later). Run this in the user database.
-- More Information: https://www.mssqltips.com/sqlservertip/2914/rolling-up-multiple-rows-into-a-single-row-and-column-for-sql-server-data/

SELECT 
   SCHEMA_NAME(ss.SCHEMA_id) AS SCHEMANAME,
   ss.name as TableName, 
   ss2.name as IndexName, 
   ss2.index_id,
   (SELECT STRING_AGG(name,', ') 
    from sys.index_columns a inner join sys.all_columns b on a.object_id = b.object_id and a.column_id = b.column_id and a.object_id = ss.object_id and a.index_id = ss2.index_id and is_included_column = 0
	) as IndexColumns,
   (SELECT STRING_AGG(name,', ') 
    from sys.index_columns a inner join sys.all_columns b on a.object_id = b.object_id and a.column_id = b.column_id and a.object_id = ss.object_id and a.index_id = ss2.index_id and is_included_column = 1
    ) as IncludedColumns
FROM sys.objects SS INNER JOIN SYS.INDEXES ss2 ON ss.OBJECT_ID = ss2.OBJECT_ID 
WHERE ss.type = 'U'
ORDER BY 1, 2, 3   


---------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------
-- Purpose: This script will generate create scripts for all existing indexes in a database. Run this in the user database.
-- More Information: https://www.mssqltips.com/sqlservertip/3441/script-out-all-sql-server-indexes-in-a-database-using-tsql/
-- Here are some things to be aware of when generating the creation script:
--   If index was disabled, then code to disable it will also be generated
--   If index has included columns they will be included in the same order
--   Script considers all properties of the index (FILEGROUP, ALLOW_PAGE_LOCKS, STATISTICS_NORECOMPUTE, FILLFACTOR, SORT_IN_TEMPDB, etc.). The final script will create the original index.
--   Will not script indexes linked to the Primary Key
--   Will not script partitioned indexes

declare @SchemaName varchar(100)declare @TableName varchar(256)
declare @IndexName varchar(256)
declare @ColumnName varchar(100)
declare @is_unique varchar(100)
declare @IndexTypeDesc varchar(100)
declare @FileGroupName varchar(100)
declare @is_disabled varchar(100)
declare @IndexOptions varchar(max)
declare @IndexColumnId int
declare @IsDescendingKey int 
declare @IsIncludedColumn int
declare @TSQLScripCreationIndex varchar(max)
declare @TSQLScripDisableIndex varchar(max)

declare CursorIndex cursor for
 select schema_name(t.schema_id) [schema_name], t.name, ix.name,
 case when ix.is_unique = 1 then 'UNIQUE ' else '' END 
 , ix.type_desc,
 case when ix.is_padded=1 then 'PAD_INDEX = ON, ' else 'PAD_INDEX = OFF, ' end
 + case when ix.allow_page_locks=1 then 'ALLOW_PAGE_LOCKS = ON, ' else 'ALLOW_PAGE_LOCKS = OFF, ' end
 + case when ix.allow_row_locks=1 then  'ALLOW_ROW_LOCKS = ON, ' else 'ALLOW_ROW_LOCKS = OFF, ' end
 + case when INDEXPROPERTY(t.object_id, ix.name, 'IsStatistics') = 1 then 'STATISTICS_NORECOMPUTE = ON, ' else 'STATISTICS_NORECOMPUTE = OFF, ' end
 + case when ix.ignore_dup_key=1 then 'IGNORE_DUP_KEY = ON, ' else 'IGNORE_DUP_KEY = OFF, ' end
 + 'SORT_IN_TEMPDB = OFF, FILLFACTOR =' + CAST(ix.fill_factor AS VARCHAR(3)) AS IndexOptions
 , ix.is_disabled , FILEGROUP_NAME(ix.data_space_id) FileGroupName
 from sys.tables t 
 inner join sys.indexes ix on t.object_id=ix.object_id
 where ix.type>0 and ix.is_primary_key=0 and ix.is_unique_constraint=0 --and schema_name(tb.schema_id)= @SchemaName and tb.name=@TableName
 and t.is_ms_shipped=0 and t.name<>'sysdiagrams'
 order by schema_name(t.schema_id), t.name, ix.name

open CursorIndex
fetch next from CursorIndex into  @SchemaName, @TableName, @IndexName, @is_unique, @IndexTypeDesc, @IndexOptions,@is_disabled, @FileGroupName

while (@@fetch_status=0)
begin
 declare @IndexColumns varchar(max)
 declare @IncludedColumns varchar(max)
 
 set @IndexColumns=''
 set @IncludedColumns=''
 
 declare CursorIndexColumn cursor for 
  select col.name, ixc.is_descending_key, ixc.is_included_column
  from sys.tables tb 
  inner join sys.indexes ix on tb.object_id=ix.object_id
  inner join sys.index_columns ixc on ix.object_id=ixc.object_id and ix.index_id= ixc.index_id
  inner join sys.columns col on ixc.object_id =col.object_id  and ixc.column_id=col.column_id
  where ix.type>0 and (ix.is_primary_key=0 or ix.is_unique_constraint=0)
  and schema_name(tb.schema_id)=@SchemaName and tb.name=@TableName and ix.name=@IndexName
  order by ixc.index_column_id
 
 open CursorIndexColumn 
 fetch next from CursorIndexColumn into  @ColumnName, @IsDescendingKey, @IsIncludedColumn
 
 while (@@fetch_status=0)
 begin
  if @IsIncludedColumn=0 
   set @IndexColumns=@IndexColumns + @ColumnName  + case when @IsDescendingKey=1  then ' DESC, ' else  ' ASC, ' end
  else 
   set @IncludedColumns=@IncludedColumns  + @ColumnName  +', ' 

  fetch next from CursorIndexColumn into @ColumnName, @IsDescendingKey, @IsIncludedColumn
 end

 close CursorIndexColumn
 deallocate CursorIndexColumn

 set @IndexColumns = substring(@IndexColumns, 1, len(@IndexColumns)-1)
 set @IncludedColumns = case when len(@IncludedColumns) >0 then substring(@IncludedColumns, 1, len(@IncludedColumns)-1) else '' end
 --  print @IndexColumns
 --  print @IncludedColumns

 set @TSQLScripCreationIndex =''
 set @TSQLScripDisableIndex =''
 set @TSQLScripCreationIndex='CREATE '+ @is_unique  +@IndexTypeDesc + ' INDEX ' +QUOTENAME(@IndexName)+' ON ' + QUOTENAME(@SchemaName) +'.'+ QUOTENAME(@TableName)+ '('+@IndexColumns+') '+ 
  case when len(@IncludedColumns)>0 then CHAR(13) +'INCLUDE (' + @IncludedColumns+ ')' else '' end + CHAR(13)+'WITH (' + @IndexOptions+ ') ON ' + QUOTENAME(@FileGroupName) + ';'  

 if @is_disabled=1 
  set  @TSQLScripDisableIndex=  CHAR(13) +'ALTER INDEX ' +QUOTENAME(@IndexName) + ' ON ' + QUOTENAME(@SchemaName) +'.'+ QUOTENAME(@TableName) + ' DISABLE;' + CHAR(13) 

 print @TSQLScripCreationIndex
 print @TSQLScripDisableIndex

 fetch next from CursorIndex into  @SchemaName, @TableName, @IndexName, @is_unique, @IndexTypeDesc, @IndexOptions,@is_disabled, @FileGroupName

end
close CursorIndex
deallocate CursorIndex


---------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------
-- Purpose: Script to Disable Non Used SQL Server Indexes. Run this in the user database.
-- More Information: https://www.mssqltips.com/sqlservertip/4776/script-to-disable-non-used-sql-server-indexes/
-- This will PRINT the commands. You can uncomment the EXEC line to execute disabling the index
-- This could be run as stand alone code or created as a stored procedure

CREATE PROC dbo.usp_Disable_UnusedIndexes
AS
BEGIN
   SET NOCOUNT ON

   DECLARE @tblName SYSNAME
   DECLARE @indName SYSNAME
   DECLARE @schName SYSNAME
   DECLARE @disableTSQL VARCHAR(300)

   DECLARE c_unused_indexes CURSOR
   FOR
   SELECT DISTINCT obj.NAME AS ObjectName
      ,ind.NAME AS IndexName
      ,sch.NAME AS schemaName
   FROM sys.dm_db_index_usage_stats ius
        INNER JOIN sys.indexes ind on ius.index_id = ind.index_id
        INNER JOIN sys.objects obj on ind.OBJECT_ID = obj.OBJECT_ID
        INNER JOIN sys.schemas sch on obj.schema_id = sch.schema_id
   WHERE  OBJECTPROPERTY(ius.OBJECT_ID, 'IsSystemTable') = 0
      AND LEFT(obj.NAME, 3) NOT IN ('sys','sql','que','fil')
      AND LOWER(ind.type_desc) = 'NONCLUSTERED'
      AND ind.is_primary_key = 0
      AND ind.is_unique_constraint = 0
      AND ius.user_seeks = 0
      AND ius.user_scans = 0
      AND ius.user_lookups = 0
      AND ius.user_updates > 0
      AND ius.database_id = db_id()
      AND sch.name <> 'sys'

   OPEN c_unused_indexes

   FETCH NEXT
   FROM c_unused_indexes
   INTO @tblName
       ,@indName
       ,@schName

   WHILE @@FETCH_STATUS = 0
   BEGIN
      SET @disableTSQL = 'ALTER INDEX ' + @indName + ' ON ' + @schName + '.[' + @tblName + '] DISABLE'

      PRINT @disableTSQL

      --EXEC (@disableTSQL)

      FETCH NEXT
      FROM c_unused_indexes
      INTO @tblName
          ,@indName
          ,@schName
   END

   CLOSE c_unused_indexes
   DEALLOCATE c_unused_indexes

   SET NOCOUNT OFF
END
GO


---------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------
-- Purpose: Script to return index fill factor and fragmentation level to find the best value for fill factor
-- More Information: https://www.mssqltips.com/sqlservertip/5908/what-is-the-best-value-for-fill-factor-in-sql-server/
-- you can add a specific table to the WHERE clause or leave as is to get all tables in the database

SELECT
  tbl.name TableName
, idx.name IndexName, idx.fill_factor
, CAST(Fragmentation.avg_page_space_used_in_percent AS DECIMAL(4,1)) ActualFillFactor
, CAST(Fragmentation.avg_fragmentation_in_percent AS DECIMAL(4,1)) CurrentFragmentation
, Fragmentation.fragment_count
, CAST(Fragmentation.avg_fragment_size_in_pages AS DECIMAL(8,1)) AvgFragmentSize 
FROM
  sys.tables tbl
    INNER JOIN
  sys.indexes idx ON tbl.object_id = idx.object_id
    CROSS APPLY
  sys.dm_db_index_physical_stats(DB_ID(), tbl.object_id, idx.index_id, 0, 'SAMPLED') Fragmentation
--WHERE 
--  tbl.name LIKE 'RandomValue[0-9]%';	


---------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
-- Purpose: Rebuild All Indexes Script for SQL Server 2005 and Later
-- https://www.mssqltips.com/sqlservertip/1367/sql-server-script-to-rebuild-all-indexes-for-all-tables-and-all-databases/
-- this will PRINT the commands. You could uncomment the EXEC line if you want to actually rebuild the indexes.

DECLARE @Database NVARCHAR(255)   
DECLARE @Table NVARCHAR(255)  
DECLARE @cmd NVARCHAR(1000)  

DECLARE DatabaseCursor CURSOR READ_ONLY FOR  
SELECT name FROM master.sys.databases   
WHERE name NOT IN ('master','msdb','tempdb','model','distribution')  -- databases to exclude
--AND name IN ('DB1', 'DB2') -- use this to select specific databases and comment out line above
AND state = 0 -- database is online
AND is_in_standby = 0 -- database is not read only for log shipping
ORDER BY 1  

OPEN DatabaseCursor  

FETCH NEXT FROM DatabaseCursor INTO @Database  
WHILE @@FETCH_STATUS = 0  
BEGIN  

   SET @cmd = 'DECLARE TableCursor CURSOR READ_ONLY FOR SELECT ''['' + table_catalog + ''].['' + table_schema + ''].['' +  
   table_name + '']'' as tableName FROM [' + @Database + '].INFORMATION_SCHEMA.TABLES WHERE table_type = ''BASE TABLE'''   

   -- create table cursor  
   EXEC (@cmd)  
   OPEN TableCursor   

   FETCH NEXT FROM TableCursor INTO @Table   
   WHILE @@FETCH_STATUS = 0   
   BEGIN
      BEGIN TRY   
         SET @cmd = 'ALTER INDEX ALL ON ' + @Table + ' REBUILD' 
         PRINT @cmd -- uncomment if you want to see commands
         --EXEC (@cmd) 
      END TRY
      BEGIN CATCH
         PRINT '---'
         PRINT @cmd
         PRINT ERROR_MESSAGE() 
         PRINT '---'
      END CATCH

      FETCH NEXT FROM TableCursor INTO @Table   
   END   

   CLOSE TableCursor   
   DEALLOCATE TableCursor  

   FETCH NEXT FROM DatabaseCursor INTO @Database  
END  
CLOSE DatabaseCursor   
DEALLOCATE DatabaseCursor


---------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------
-- Purpose: SQL Server Index and Statistics Report
-- https://www.mssqltips.com/sqlservertip/1550/sql-server-index-and-statistics-report/

CREATE PROCEDURE [dbo].[usp_Detail_Index_Stats] @table_name sysname  
AS  

-- ******VARIABLE DECLARATIONS****** 
DECLARE @IndexTable TABLE 
        (  
        [Database] sysname, [Table] sysname, [Index Name] sysname NULL, index_id smallint,  
        [object_id] INT, [Index Type] VARCHAR(20), [Alloc Unit Type] VARCHAR(20),  
        [Avg Frag %] decimal(5,2), [Row Ct] bigint, [Stats Update Dt] datetime  
        )  

DECLARE @dbid smallint --Database id for current database 
DECLARE @objectid INT    --Object id for table being analyzed 
DECLARE @indexid INT     --Index id for the target index for the STATS_DATE() function 

-- ******VARIABLE ASSIGNMENTS****** 
SELECT @dbid = DB_ID(DB_NAME())  
SELECT @objectid = OBJECT_ID(@table_name)  

IF @objectid IS NULL
BEGIN
	PRINT 'Table not found'
	RETURN
END

-- ******Load @IndexTable with Index Metadata****** 
INSERT INTO @IndexTable  
   ( 
   [Database], [Table], [Index Name], index_id, [object_id],  
   [Index Type], [Alloc Unit Type], [Avg Frag %], [Row Ct] 
   ) 
SELECT  
   DB_NAME() AS "Database",  
   @table_name AS "Table",  
   SI.NAME AS "Index Name",  
   IPS.index_id, IPS.OBJECT_ID,     --These fields included for joins only 
   IPS.index_type_desc,                --Heap, Non-clustered, or Clustered 
   IPS.alloc_unit_type_desc,       --In-row data or BLOB data 
   CAST(IPS.avg_fragmentation_in_percent AS decimal(5,2)),  
   IPS.record_count  
FROM sys.dm_db_index_physical_stats (@dbid, @objectid, NULL, NULL, 'sampled') IPS  
   LEFT JOIN sys.sysindexes SI ON IPS.OBJECT_ID = SI.id AND IPS.index_id = SI.indid  
WHERE IPS.index_id <> 0  

-- ******ADD STATISTICS INFORMATION****** 
DECLARE curIndex_ID CURSOR FOR  
   SELECT I.index_id  
   FROM @IndexTable I  
   ORDER BY I.index_id  
    
OPEN curIndex_ID  
   FETCH NEXT FROM curIndex_ID INTO @indexid  
    
   WHILE @@FETCH_STATUS = 0  
       BEGIN  
           UPDATE @IndexTable  
           SET [Stats Update Dt] = STATS_DATE(@objectid, @indexid)  
           WHERE [object_id] = @objectid AND [index_id] = @indexid  
            
           FETCH NEXT FROM curIndex_ID INTO @indexid  
       END  
    
CLOSE curIndex_ID  
DEALLOCATE curIndex_ID  
    
-- ******RETURN RESULTS****** 
SELECT I.[Database], I.[Table], I.[Index Name], I.[Index Type],        
   I.[Avg Frag %], I.[Row Ct],  
   CONVERT(VARCHAR, I.[Stats Update Dt], 110) AS "Stats Dt"
FROM @IndexTable I  
ORDER BY I.[Index Type], I.[index_id] 


---------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
-- Purpose: Generate index build script for Missing Indexes that have High Impact SQL  

SELECT
    migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) AS ImprovementMeasure,
    DB_NAME(mid.database_id) AS DatabaseName,
    mid.[statement] AS TableName,
    ISNULL(mid.equality_columns, '') + ISNULL(', ' + mid.inequality_columns, '') AS IndexColumns,
    mid.included_columns AS IncludedColumns,
    IndexScript = CONCAT(
        N'CREATE NONCLUSTERED INDEX [IX_ChangeMyName_', mig.index_handle, '] ON ',
        mid.[statement],
        ' ( ',
        ISNULL(mid.equality_columns, mid.inequality_columns),
        ' )',
        CASE
            WHEN (mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL) THEN
                N' INCLUDE ( ' + mid.inequality_columns
            ELSE N''
        END,
        CASE
            WHEN mid.included_columns IS NOT NULL AND (mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL) THEN
                N', ' + mid.included_columns
            WHEN mid.included_columns IS NOT NULL THEN
                N' INCLUDE ( ' + mid.included_columns
            ELSE N''
        END,
        CASE
            WHEN mid.included_columns IS NOT NULL OR (mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL) THEN
                N' )'
            ELSE N''
        END
    )
FROM sys.dm_db_missing_index_groups AS mig
INNER JOIN sys.dm_db_missing_index_group_stats AS migs ON mig.index_group_handle = migs.group_handle
INNER JOIN sys.dm_db_missing_index_details AS mid ON mig.index_handle = mid.index_handle
WHERE database_id > 4
    AND DB_NAME(database_id) NOT IN ('SSISDB', 'ReportServer', 'ReportServerTempDB', 'distribution')
ORDER BY ImprovementMeasure DESC;


---------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
-- Purpose: List of paused resumable index sql server
-- SQL Server 2017 or later
-- question about paused_time and allocated_mb

SELECT
    object_id,
    index_id,
    name as IndexName,
    state_desc,
    last_pause_time as paused_time,
    DateDiff(mi,last_pause_time,getdate()) as PausedSinceMinutes,
    page_count as PageCounts,  
    round(percent_complete,0) As PercentCompleted
FROM sys.index_resumable_operations
WHERE state = 1

---------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------
--Purpose: Get index sizes

SELECT SCHEMA_NAME(tbl.[schema_id]) SchemaName,
		tbl.[name] AS TableName,
		idx.[name] AS IndexName,
		SUM(ps.[used_page_count]) * 8/1024 AS IndexSizeInMB
FROM sys.dm_db_partition_stats AS ps
		INNER JOIN sys.indexes AS idx ON ps.[object_id] = idx.[object_id]
									   AND ps.[index_id] = idx.[index_id]
		INNER JOIN sys.tables tbl ON tbl.[object_id] = idx.[object_id]
GROUP BY tbl.[name], idx.[name], tbl.[schema_id]
ORDER BY IndexSizeInMB DESC


---------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------
--Purpose: Missing Columns in a SQL Server Columnstore Index
--https://www.mssqltips.com/sqlservertip/5742/script-to-create-and-update-missing-sql-server-columnstore-indexes/

SELECT DISTINCT 'CREATE NONCLUSTERED COLUMNSTORE INDEX [' + i.NAME + '] ON dbo.' + tbl.NAME + ' (' + IndexColumns.IndexColumnList + ') WITH (DROP_EXISTING = ON) '
FROM sys.tables AS tbl
INNER JOIN sys.indexes AS i
   ON (
         i.index_id > 0
         AND i.is_hypothetical = 0
         )
      AND (i.object_id = tbl.object_id)
INNER JOIN sys.index_columns AS ic
   ON (
         ic.column_id > 0
         AND (
            ic.key_ordinal > 0
            OR ic.partition_ordinal = 0
            OR ic.is_included_column != 0
            )
         )
      AND (
         ic.index_id = CAST(i.index_id AS INT)
         AND ic.object_id = i.object_id
         )
INNER JOIN (
   SELECT object_id,
      (
         STUFF((
               SELECT ',' + NAME
               FROM sys.columns
               WHERE object_id = C.object_id
               FOR XML PATH(''),
                  TYPE
               ).value('.', 'NVARCHAR(MAX)'), 1, 1, '')
         ) AS IndexColumnList
   FROM sys.columns AS C
   GROUP BY C.object_id
   ) AS IndexColumns
   ON IndexColumns.object_id = ic.object_id
WHERE 
       tbl.NAME     LIKE 'fact%'
   AND tbl.NAME     NOT LIKE '%OLD%'
   AND tbl.NAME     NOT LIKE '%BACK%'
   AND i.type_desc  LIKE '%NONCLUSTERED COLUMNSTORE%'



---------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------
-- Purpose: Script to Manage SQL Server Rebuilds and Reorganize for Index Fragmentation
-- https://www.mssqltips.com/sqlservertip/4470/script-to-manage-sql-server-rebuilds-and-reorganize-for-index-fragmentation/

----
-- Script that reorganizes or rebuilds all indexes having an average fragmentation 
-- percentage above a given threshold. It also works in the case
-- where Availability Groups are enabled as it determines if the
-- relevant databases are the primary replicas.
--
-- This script supports only SQL Server 2005 or later.
-- Also, if you execute this script in a SQL Server 2005 instance 
-- or later, any databases with compatibility level 2000 (80) or earlier
-- will be automatically excluded from the index reorganization/rebuild process.
----

--Initial check - You must be SysAdmin
DECLARE @isSysAdmin INT
SET @isSysAdmin=(SELECT IS_SRVROLEMEMBER ('sysadmin'));

--Initial check - You must be using SQL Server 2005 or later
DECLARE @SQLServerVersion INT
SET @SQLServerVersion=(SELECT CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)),CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(50)))-1) AS INT));


IF @isSysAdmin=1 AND @SQLServerVersion >= 9
BEGIN 

   --
   -- Variable/parameters Declaration
   --
   DECLARE @dbname NVARCHAR(128);
   DECLARE @ReorganizeOrRebuildCommand NVARCHAR(MAX);
   DECLARE @dbid INT;
   DECLARE @indexFillFactor VARCHAR(5); 
   DECLARE @fragmentationThreshold VARCHAR(10);
   DECLARE @indexStatisticsScanningMode VARCHAR(20);
   DECLARE @verboseMode BIT;
   DECLARE @reportOnly BIT;
   DECLARE @sortInTempdb VARCHAR(3);
   DECLARE @isHadrEnabled BIT;
   DECLARE @databaseToCheck VARCHAR(250)
   DECLARE @dynamic_command NVARCHAR(1024);
   DECLARE @dynamic_command_get_tables NVARCHAR(MAX);

   --Initializations - Do not change
   SET @databaseToCheck=NULL;
   SET @dynamic_command = NULL;
   SET @dynamic_command_get_tables = NULL;
   SET @isHadrEnabled=0;

   SET NOCOUNT ON;

   ---------------------------------------------------------
   --Set Parameter Values: You can change these (optional) -
   --Note: The script has default parameters set   -
   ---------------------------------------------------------
   --if set to 1: it will just generate a report with the index reorganization/rebuild statements
   --if set to 0: it will reorganize or rebuild the fragmented indexes
   SET @reportOnly = 0;
   
   --optional: if not set (NULL), it will scann all databases
   --If name is set (i.e. 'testDB') it will just scan the given database
   SET @databaseToCheck = NULL;
   
   --maintains only the indexes that have average fragmentation percentage equal or higher from the given value
   SET @fragmentationThreshold = 15; 
   
   --fill factor - the percentage of the data page to be filled up with index data
   SET @indexFillFactor = 90; 
   
   --sets the scanning mode for index statistics 
   --available values: 'DEFAULT', NULL, 'LIMITED', 'SAMPLED', or 'DETAILED'
   SET @indexStatisticsScanningMode='SAMPLED';
   
   --if set to ON: sorts intermediate index results in TempDB 
   --if set to OFF: sorts intermediate index results in user database's log file
   SET @sortInTempdb='ON'; 
   
   --if set to 0: Does not output additional information about the index reorganization/rebuild process
   --if set to 1: Outputs additional information about the index reorganization/rebuild process
   SET @verboseMode = 0; 
   ------------------------------
   --End Parameter Values Setup -
   ------------------------------
   
   -- check if given database exists and if compatibility level >= SQL 2005 (90)
   IF @verboseMode=1
    PRINT 'Checking if database '+@databaseToCheck+' exists and if compatibility level equals or greater 2005 (90)';
   
    -- if given database does not exist, raise error with severity 20
    -- in order to terminate script's execution
   IF @databaseToCheck IS NOT NULL
   BEGIN
      DECLARE @checkResult INT
      SET @checkResult=(SELECT COUNT(*) FROM master.sys.databases WHERE [name]=RTRIM(@databaseToCheck));
      IF @checkResult<1
         RAISERROR('Error executing index reorganization/rebuild script: Database does not exist' , 20, 1) WITH LOG;
   
      DECLARE @checkResult2 INT
      SET @checkResult=(SELECT [compatibility_level] FROM master.sys.databases WHERE [name]=RTRIM(@databaseToCheck));
      IF @checkResult<90
         RAISERROR('Error executing index reorganization/rebuild script: Only databases with SQL Server 2005 or later compatibility level are supported' , 20, 1) WITH LOG;  
   END
   
   IF @verboseMode=1
      PRINT 'Initial checks completed with no errors.';
   
   -- Temporary table for storing index fragmentation details
   IF OBJECT_ID('tempdb..#tmpFragmentedIndexes') IS NULL
   BEGIN
      CREATE TABLE #tmpFragmentedIndexes
         (
         [dbName] sysname,
         [tableName] sysname,
         [schemaName] sysname,
         [indexName] sysname,
         [databaseID] SMALLINT ,
         [objectID] INT ,
         [indexID] INT ,
         [AvgFragmentationPercentage] FLOAT,
         [reorganizationOrRebuildCommand] NVARCHAR(MAX)
         );
   END 
   
   -- Initialize temporary table
   DELETE FROM #tmpFragmentedIndexes;
   
   -- Validate parameters/set defaults
   IF @sortInTempdb NOT IN ('ON','OFF')
      SET @sortInTempdb='ON';
   
   -- Check if instance has AlwaysOn AGs enabled
   SET @isHadrEnabled=CAST((SELECT ISNULL(SERVERPROPERTY('IsHadrEnabled'),0)) AS BIT);
   
   -- if database not specified scan all databases
   IF @databaseToCheck IS NULL
   BEGIN
      DECLARE dbNames_cursor CURSOR
      FOR
         SELECT  s.[name] AS dbName,
                 s.database_id
         FROM    master.sys.databases s            
         WHERE   s.state_desc = 'ONLINE'
             AND s.is_read_only != 1            
             AND s.[name] NOT IN ( 'master', 'model', 'tempdb' )
             AND s.[compatibility_level]>=90
         ORDER BY s.database_id;    
   END 
   ELSE
   -- if database specified, scan only that database
   BEGIN
      DECLARE dbNames_cursor CURSOR 
      FOR
         SELECT  s.[name] AS dbName ,
                 s.database_id
         FROM    master.sys.databases s            
         WHERE   s.state_desc = 'ONLINE'
             AND s.is_read_only != 1                        
             AND s.[name]=RTRIM(@databaseToCheck)    
   END 
   
   -- if Always On Availability Groups are enabled, check for primary databases
   -- (thus exclude secondary databases)
   IF @isHadrEnabled=1
   BEGIN
   
      DEALLOCATE dbNames_cursor;
   
      -- if database not specified scan all databases
      IF @databaseToCheck IS NULL
      BEGIN
         DECLARE dbNames_cursor CURSOR
         FOR
         SELECT  s.[name] AS dbName ,
                 s.database_id
         FROM    master.sys.databases s
         LEFT JOIN master.sys.dm_hadr_availability_replica_states r ON s.replica_id = r.replica_id
         WHERE   s.state_desc = 'ONLINE'
             AND s.is_read_only != 1
             AND UPPER(ISNULL(r.role_desc, 'NonHadrEnabled')) NOT LIKE 'SECONDARY'
             AND s.[name] NOT IN ( 'master', 'model', 'tempdb' )
             AND s.[compatibility_level]>=90 
         ORDER BY s.database_id;    
      END
      ELSE
      -- if database specified, scan only that database
      BEGIN
         DECLARE dbNames_cursor CURSOR
         FOR
         SELECT  s.[name] AS dbName ,
                 s.database_id
         FROM    master.sys.databases s
         LEFT JOIN master.sys.dm_hadr_availability_replica_states r ON s.replica_id = r.replica_id
         WHERE   s.state_desc = 'ONLINE'
             AND s.is_read_only != 1
             AND UPPER(ISNULL(r.role_desc, 'NonHadrEnabled')) NOT LIKE 'SECONDARY'    
             AND s.[name]=RTRIM(@databaseToCheck);  
      END 
   END 
   
   
   --
   -- For each database included in the cursor, 
   -- gather all tables that have indexes with 
   -- average fragmentation percentage equal or above @fragmentationThreshold
   --
   OPEN dbNames_cursor;
   FETCH NEXT FROM dbNames_cursor INTO @dbname, @dbid;
   WHILE @@fetch_status = 0
   BEGIN   
    
      --If verbose mode is enabled, print logs
      IF @verboseMode = 1
      BEGIN
         PRINT ''
         PRINT 'Gathering index fragmentation statistics for database: ['+ @dbname + '] with id: ' + CAST(@dbid AS VARCHAR(10));    
      END;
                      
      SET @dynamic_command_get_tables = N'
      USE [' + @dbname+ N'];
      INSERT INTO #tmpFragmentedIndexes (
      [dbName],
      [tableName],
      [schemaName],
      [indexName],
      [databaseID],
      [objectID],
      [indexID],
      [AvgFragmentationPercentage],
      [reorganizationOrRebuildCommand]  
      )
      SELECT
        DB_NAME() as [dbName], 
        tbl.name as [tableName],
        SCHEMA_NAME (tbl.schema_id) as schemaName, 
        idx.Name as [indexName], 
        pst.database_id as [databaseID], 
        pst.object_id as [objectID], 
        pst.index_id as [indexID], 
        pst.avg_fragmentation_in_percent as [AvgFragmentationPercentage],
        CASE WHEN pst.avg_fragmentation_in_percent > 30 THEN 
        ''ALTER INDEX [''+idx.Name+''] ON [''+DB_NAME()+''].[''+SCHEMA_NAME (tbl.schema_id)+''].[''+tbl.name+''] REBUILD WITH (FILLFACTOR = '+@indexFillFactor+', SORT_IN_TEMPDB = '+@sortInTempdb+', STATISTICS_NORECOMPUTE = OFF);''
        WHEN pst.avg_fragmentation_in_percent > 5 AND pst.avg_fragmentation_in_percent <= 30 THEN 
        ''ALTER INDEX [''+idx.Name+''] ON [''+DB_NAME()+''].[''+SCHEMA_NAME (tbl.schema_id)+''].[''+tbl.name+''] REORGANIZE;''     
        ELSE
        NULL
        END
      FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL , '''+@indexStatisticsScanningMode+''') as pst
      INNER JOIN sys.tables as tbl ON pst.object_id = tbl.object_id
      INNER JOIN sys.indexes idx ON pst.object_id = idx.object_id AND pst.index_id = idx.index_id
      WHERE pst.index_id != 0  
        AND pst.alloc_unit_type_desc IN ( N''IN_ROW_DATA'', N''ROW_OVERFLOW_DATA'')
        AND pst.avg_fragmentation_in_percent >= '+ @fragmentationThreshold + '';
           
      -- if verbose  mode is enabled, print logs    
      IF @verboseMode=1
      BEGIN
         PRINT 'Index fragmentation statistics script: ';    
         PRINT @dynamic_command_get_tables;
      END
   
      -- gather index fragmentation statistics
      EXEC (@dynamic_command_get_tables);
          
      -- bring next record from the cursor
      FETCH NEXT FROM dbNames_cursor INTO @dbname, @dbid;
   END;
   
   CLOSE dbNames_cursor;
   DEALLOCATE dbNames_cursor;
   
   -- if 'report only' mode is enabled
   IF @reportOnly=1
   BEGIN 
      SELECT   dbName ,
               tableName ,
               schemaName ,
               indexName ,            
               AvgFragmentationPercentage ,
               reorganizationOrRebuildCommand
      FROM    #tmpFragmentedIndexes
      ORDER BY AvgFragmentationPercentage DESC;
   END
   ELSE 
   -- if 'report only' mode is disabled, then execute 
   -- index reorganize/rebuild statements
   BEGIN 
      DECLARE reorganizeOrRebuildCommands_cursor CURSOR
      FOR
      SELECT  reorganizationOrRebuildCommand
      FROM #tmpFragmentedIndexes
      WHERE reorganizationOrRebuildCommand IS NOT NULL
      ORDER BY AvgFragmentationPercentage DESC;
   
      OPEN reorganizeOrRebuildCommands_cursor;
      FETCH NEXT FROM reorganizeOrRebuildCommands_cursor INTO @ReorganizeOrRebuildCommand;
      WHILE @@fetch_status = 0
      BEGIN   
         IF @verboseMode = 1
         BEGIN
            PRINT ''
            PRINT 'Executing script:'     
            PRINT @ReorganizeOrRebuildCommand
         END
             
         EXEC (@ReorganizeOrRebuildCommand);          
         FETCH NEXT FROM reorganizeOrRebuildCommands_cursor INTO @ReorganizeOrRebuildCommand;
      END;
   
      CLOSE reorganizeOrRebuildCommands_cursor;
      DEALLOCATE reorganizeOrRebuildCommands_cursor;
   
      PRINT ''
      PRINT 'All fragmented indexes have been reorganized/rebuilt.'
      PRINT ''
   END
END 
ELSE
BEGIN
   PRINT '';
   PRINT 'Error: You need to be SysAdmin and use SQL Server 2005 or later in order to use this script.';
   PRINT '';
END
--End of Script
-----------------------------------------------------------------------------------------------