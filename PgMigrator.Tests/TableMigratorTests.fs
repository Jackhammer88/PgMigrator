module TableMigratorTests

open PgMigrator
open PgMigrator.Config
open PgMigrator.SourceProviders
open PgMigrator.Types
open Xunit

let tryRunQuery' _ : Async<Result<unit, string>> = async { return Ok() }
let tryWriteRecords' _ _ _ _= async { return Ok() }
let pgSessionDestroy () = ()
let tryFinish' () : Async<Result<unit, string>> = async { return Ok() }
let tryGetTablesInfo' () = async { return Ok [] }
let tryReadTable' (_ : string) (_ : Map<string,TypeMapping>) =
    async {
        let r: SourceTableData =
            { ColumnNamesString = ""
              ColumnValues = List.empty }
        return Ok r
    }
    
let tryReadTablePart (_ : string) (_ : TableInfo) (_ : Map<string,TypeMapping>) (_ : int) (_ : int) : Async<Result<SourceTableData,string>> =
    async {
        let r: SourceTableData =
            { ColumnNamesString = ""
              ColumnValues = List.empty }
        return Ok r
    }
    
let pgProviderDestroy () =
    ()
    
let createflowData list
    : MigrationFlowData = {
        Tables = list
        TargetSchema = "public"
        TableMappings = []
        TypeMappings = Map.empty
        TablesInfo = []
        RemoveNullBytes = false
    }

[<Fact>]
let tryMigrateAllTablesAsync_StopsOnWriteError_ReturnsTableName () =
    // Arrange
    let errorTableName = "Table3"
    let tryWriteRecords'' _ _ tableName _ = async {
        return
            match tableName with
            | t when t = errorTableName -> Error t
            | _ -> Ok ()
    }
    
    let pgSession: PgSession =
        { tryRunQuery = tryRunQuery'
          tryWriteRecords = tryWriteRecords''
          destroy = pgSessionDestroy
          tryFinish = tryFinish' }
        
    let pgProvider: SourceProvider =
        { SourceType = Pgsql
          tryGetTablesInfo = tryGetTablesInfo'
          tryReadTable = tryReadTable'
          tryReadTablePart = tryReadTablePart
          destroy = pgProviderDestroy }
        
    let flowData : MigrationFlowData = {
        Tables = [ "Table1"; "Table2"; errorTableName; "Table4" ]
        TargetSchema = "public"
        TableMappings = []
        TypeMappings = Map.empty
        TablesInfo = []
        RemoveNullBytes = false
    }

    // Act
    let result =
        TableMigrator.tryMigrateEagerAsync
            pgProvider
            pgSession
            flowData
        |> Async.RunSynchronously

    // Assert
    let testResult =
        match result with
        | Ok _ -> ""
        | Error e -> e
    Assert.Equal(errorTableName, testResult)


[<Fact>]
let tryMigrateAllTablesAsync_StopsOnReadError_ReturnsTableName () =
    // Arrange
    let errorTableName = "Table3"
    let tryReadTable'' (table : string) (_ : Map<string,TypeMapping>) =
        async {
            let r: SourceTableData =
                { ColumnNamesString = ""
                  ColumnValues = List.empty }
                
            return
                match table with
                | t when t = errorTableName -> Error t
                | _ -> Ok r
        }
    
    let pgSession: PgSession =
        { tryRunQuery = tryRunQuery'
          tryWriteRecords = tryWriteRecords'
          destroy = pgSessionDestroy
          tryFinish = tryFinish' }
        
    let pgProvider: SourceProvider =
        { SourceType = Pgsql
          tryGetTablesInfo = tryGetTablesInfo'
          tryReadTable = tryReadTable''
          tryReadTablePart = tryReadTablePart
          destroy = pgProviderDestroy }
    let flowData = createflowData [ "Table1"; "Table2"; errorTableName; "Table4" ]

    // Act
    let result =
        TableMigrator.tryMigrateEagerAsync
            pgProvider
            pgSession
            flowData
        |> Async.RunSynchronously

    // Assert
    let testResult =
        match result with
        | Ok _ -> ""
        | Error e -> e
    Assert.Equal(errorTableName, testResult)
    
[<Fact>]
let tryMigrateAllTablesAsync_AllTablesPassedSuccessfully () =
    // Arrange    
    let pgSession: PgSession =
        { tryRunQuery = tryRunQuery'
          tryWriteRecords = tryWriteRecords'
          destroy = pgSessionDestroy
          tryFinish = tryFinish' }
        
    let pgProvider: SourceProvider =
        { SourceType = Pgsql
          tryGetTablesInfo = tryGetTablesInfo'
          tryReadTable = tryReadTable'
          tryReadTablePart = tryReadTablePart
          destroy = pgProviderDestroy }
        
    let tables = List.init 100 (fun i -> $"Table{i + 1}")
    let flowData = createflowData tables
    
    // Act
    let result =
        TableMigrator.tryMigrateEagerAsync
            pgProvider
            pgSession
            flowData
        |> Async.RunSynchronously

    // Assert
    Assert.True(result.IsOk)