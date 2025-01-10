namespace PgMigrator

open System
open System.Data
open Npgsql

type ColumnInfo() =
    member val TableName: string = "" with get, set
    member val ColumnName: string = "" with get, set
    member val DataType: string = "" with get, set
    member val IsNullable: bool = false with get, set
    member val IsPrimaryKey: bool = false with get, set

type ConnectionsInfo = {
    Source : IDbConnection
    Target : NpgsqlConnection
    Transaction : NpgsqlTransaction
} with interface IDisposable with
           member this.Dispose() =
               try
                   this.Transaction.Dispose()
                   this.Target.Dispose()
                   this.Source.Dispose()
               with
               | ex -> Console.Error.WriteLine(ex) 

type TableInfo =
    { TableName: string
      Columns: List<ColumnInfo>
      PkCount: int }
    
module SourceTypes =
    
    [<Literal>]
    let postgres = "postgresql"
    
    [<Literal>]
    let mssql = "mssql"