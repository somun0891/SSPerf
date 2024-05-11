/*
 Valid values for @pRows:
 Any positive integer (current default is 10)

 Valid values for @pDebug
 0 - Debug is off (default) and the code will be executed
 1 - Debug if on and the code will be displayed instead of being executed.
-----------------------------------------------------------------------------------------------------------------------
 Example Usage:
--===== Example usage for things that have run in the last hour
     -- regardless of when they were compiled.
   EXEC SP_Worst_Performing_Queries 'Recent CPU'      , 10;
   EXEC SP_Worst_Performing_Queries 'Recent Reads'    , 10;
   EXEC SP_Worst_Performing_Queries 'Recent Duration' , 10;
   EXEC SP_Worst_Performing_Queries 'Recent Writes'   , 10;
   EXEC SP_Worst_Performing_Queries 'Recent Physical' , 10;
   EXEC SP_Worst_Performing_Queries 'Recent Count'    , 10;

--===== Example usage for things since they were last compiled.
   EXEC SP_Worst_Performing_Queries 'CPU'     , 10;
   EXEC SP_Worst_Performing_Queries 'Reads'   , 10;
   EXEC SP_Worst_Performing_Queries 'Duration', 10;
   EXEC SP_Worst_Performing_Queries 'Writes'  , 10;
   EXEC SP_Worst_Performing_Queries 'Physical', 10;
   EXEC SP_Worst_Performing_Queries 'Count'   , 10;
*/


 use master;
 GO
 CREATE OR ALTER PROCEDURE SP_Worst_Performing_Queries   
	 @pSortType  VARCHAR(30) = 'RECENT Reads',
        @pRows      INT         = '10',
        @pDebug     BIT         = 0     
     AS

--===== Environmental Presets  
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

--===== Declare some obviously named variable(s)
DECLARE @SQL VARCHAR(MAX)
; 
--===== Create the SQL to get the data in the order requested
 SELECT @SQL = REPLACE(REPLACE(REPLACE(REPLACE('
 SELECT  TOP (<<@pRows>>) --SP_Worst_Performing_Queries
         SampleDT       = GETDATE()                                         --Rev 01
        ,SampleType     = ''<<@pSortType>>''                                --Rev 01
        ,SampleValue    = [stats].<<SortColumn>>                            --Rev 01
        ,CurrentDBName  = db.DBName                                         --Rev 02.1
        ,CurrentDBID    = DB_ID(db.DBName)                                  --Rev 02.1
        ,ExecDBID       = [txt].DBID                                        --Rev 01 - Rev02.2
        ,ExecObjectID   = [txt].ObjectID                                    --Rev 01 - Rev02.2
        ,ExecDBName     = DB_NAME([txt].DBID)                               --Rev 01 - Rev02.2
        ,ExecSchemaName = OBJECT_SCHEMA_NAME([txt].ObjectID, [txt].DBID)    --Rev 01 - Rev02.2
        ,ExecObjectName = OBJECT_NAME([txt].ObjectID, [txt].DBID)           --Rev 01 - Rev02.2
        ,StatementText  = (SELECT REPLACE(REPLACE(CAST(''--'' + CHAR(10)  
                                + SUBSTRING(txt.Text
                                          ,[stats].Statement_Start_Offset/2+1
                                          ,(CASE [stats].Statement_End_Offset 
                                               WHEN -1 
                                               THEN DATALENGTH(txt.Text)
                                               ELSE [stats].Statement_End_Offset 
                                            END)/2 - [stats].Statement_Start_Offset/2+1
                                ) AS VARCHAR(MAX)),CHAR(0),''''),CHAR(31),'''')
                             AS [processing-instruction(StatementTextSQL)] FOR XML PATH(''''), TYPE)
        ,FullText = (SELECT REPLACE(REPLACE(CAST(''--''+CHAR(10)+[txt].Text AS VARCHAR(MAX)),CHAR(0),''''),CHAR(31),'''')
                         AS [processing-instruction(FullTextSQL)] FOR XML PATH(''''), TYPE)
        ,[plan].query_plan
        ,[stats].* 
   FROM       sys.dm_exec_query_stats                       [stats]
  OUTER APPLY sys.dm_exec_sql_text  ([stats].sql_handle)    [txt]
  OUTER APPLY sys.dm_exec_query_plan([stats].plan_handle)   [plan]
  OUTER APPLY (SELECT DBName = DB_NAME(CONVERT(INT,value)) -- Rev 02.1
                 FROM sys.dm_exec_plan_attributes([stats].plan_handle) 
                WHERE attribute = ''dbid'')                 [db]
  WHERE [Stats].Last_Execution_Time >= ''<<LookBackDate>>''
  ORDER BY [stats].<<SortColumn>> DESC
;'
------- This is the other end of the REPLACE's
,'<<@pSortType>>',@pSortType)
,'<<@pRows>>',CAST(@pRows AS VARCHAR(10)))
,'<<LookBackDate>>', CAST(CASE WHEN @pSortType LIKE '%Recent%' THEN DATEADD(hh,-1,GETDATE()) ELSE '1753' END AS VARCHAR(20)))
,'<<SortColumn>>', CASE 
                       WHEN @pSortType LIKE '%Count%'    THEN 'Execution_Count'
                       WHEN @pSortType LIKE '%CPU%'      THEN 'Total_Worker_Time'
                       WHEN @pSortType LIKE '%Duration%' THEN 'Total_Elapsed_Time'
                       WHEN @pSortType LIKE '%Reads%'    THEN 'Total_Logical_Reads'
                       WHEN @pSortType LIKE '%Physical%' THEN 'Total_Physical_Reads'
                       WHEN @pSortType LIKE '%Writes%'   THEN 'Total_Logical_Writes'
                    END)
;
--===== Produce the output according to what the @pDebug variable is set to
     IF @pDebug = 0 EXEC (@SQL);
   ELSE PRINT @SQL;


   go

 USE MASTER;  
EXEC sp_ms_marksystemobject 'SP_Worst_Performing_Queries';  

SELECT name, is_ms_shipped   
   FROM sys.objects  
  WHERE name = 'SP_Worst_Performing_Queries'  
;


/*
Test run -
 EXEC SP_Worst_Performing_Queries 'Recent Reads'    , 10;
*/

