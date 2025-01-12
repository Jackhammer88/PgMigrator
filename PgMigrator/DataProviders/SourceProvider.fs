namespace PgMigrator.DataProviders

open System
open Npgsql
open NpgsqlTypes
open PgMigrator.Types

type SourceTableData = {
    ColumnNamesString : string
    ColumnValues : seq<List<obj * NpgsqlDbType>>
}

type SourceProvider = {
    SourceType : SourceType
    tryGetTablesInfo : unit -> Async<Result<TableInfo list,string>>
    tryReadTable : string -> Map<string,TypeMapping> -> Async<Result<SourceTableData,string>>
    destroy : unit -> unit
} with interface IDisposable with
           member this.Dispose() = this.destroy()

