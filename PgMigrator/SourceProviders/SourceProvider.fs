namespace PgMigrator.SourceProviders

open System
open NpgsqlTypes
open PgMigrator.Types

type SourceTableData = {
    ColumnNamesString : string
    ColumnValues : (obj * NpgsqlDbType) list list
}

type SourceProvider = {
    SourceType : SourceType
    tryGetTablesInfo : unit -> Async<Result<TableInfo list,string>>
    tryReadTable : string -> Map<string,TypeMapping> -> Async<Result<SourceTableData,string>>
    tryReadTablePart : string -> TableInfo -> Map<string,TypeMapping> -> int -> int -> Async<Result<SourceTableData,string>>    
    destroy : unit -> unit
} with interface IDisposable with
           member this.Dispose() = this.destroy()

