module TableMigratorTests

open PgMigrator
open PgMigrator.DataProviders
open PgMigrator.Types
open Xunit

let tryRunQuery' _ : Async<Result<unit, string>> = async { return Ok() }
let tryWriteRecords' _ _ _ _ _= async { return Ok() }
let pgSessionDestroy () = ()
let tryFinish' () : Async<Result<unit, string>> = async { return Ok() }
let tryGetTablesInfo' () = async { return Ok [] }
let tryReadTable' (_ : string) (_ : Map<string,TypeMapping>) =
    async {
        let r: SourceTableData =
            { ColumnNamesString = ""
              ColumnValues = Seq.empty }
        return Ok r
    }
let pgProviderDestroy () =
    ()

[<Fact>]
let tryMigrateAllTablesAsync_StopsOnWriteError_ReturnsTableName () =
    // Arrange
    let errorTableName = "Table3"
    let tryWriteRecords'' _ _ tableName _ _ = async {
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
          destroy = pgProviderDestroy }

    // Act
    let result =
        TableMigrator.tryMigrateAllTablesAsync
            pgProvider
            pgSession
            "public"
            [ "Table1"; "Table2"; errorTableName; "Table4" ]
            []
            Map.empty
            true
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
                  ColumnValues = Seq.empty }
                
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
          destroy = pgProviderDestroy }

    // Act
    let result =
        TableMigrator.tryMigrateAllTablesAsync
            pgProvider
            pgSession
            "public"
            [ "Table1"; "Table2"; errorTableName; "Table4" ]
            []
            Map.empty
            true
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
          destroy = pgProviderDestroy }
        
    let tables = List.init 100 (fun i -> $"Table{i + 1}")

    // Act
    let result =
        TableMigrator.tryMigrateAllTablesAsync
            pgProvider
            pgSession
            "public"
            tables
            []
            Map.empty
            true
        |> Async.RunSynchronously

    // Assert
    Assert.True(result.IsOk)