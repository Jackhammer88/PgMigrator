namespace PgMigrator.Types

open System
open Npgsql
open NpgsqlTypes
open PgMigrator
open PgMigrator.DataProviders

type PgSession = {
    tryRunQuery : string -> Async<Result<unit,string>>
    tryWriteRecords : SourceTableData -> string -> string -> TableMapping list -> bool -> Async<Result<unit,string>>
    destroy : unit -> unit
    tryFinish : unit -> Async<Result<unit,string>>
} with interface IDisposable with
           member this.Dispose() = this.destroy()
           
module PgSessionFactory =
    open PgMigrator.Helpers
    
    let tryWriteRecordsAsync
        (connection : NpgsqlConnection)
        (transaction : NpgsqlTransaction)
        (records : SourceTableData)
        schema
        tableName
        (tableMappings : TableMapping list)
        removeNullBytes
        = async {
            try
                let tableMappingsSet =
                    tableMappings
                    |> List.map (fun mapping -> mapping.Old, mapping) // Создаем пары (Old, TableMapping)
                    |> Map.ofList
                    
                let targetTableName =
                    match tableMappingsSet.TryGetValue tableName with
                    | s, m when s -> $"{schema}.{m.New}"
                    | _ -> $"{schema}.{tableName}"
                    
                use! writer =
                    connection.BeginBinaryImportAsync(
                        $"COPY {targetTableName} ({records.ColumnNamesString}) FROM STDIN (FORMAT BINARY)")
                    |> Async.AwaitTask
                
                let trySanitize (value : obj) dbType : obj =
                    match value with
                    | :? string as str
                        when removeNullBytes
                             && not (String.IsNullOrEmpty str)
                             && dbType = NpgsqlDbType.Varchar ->
                        str.Replace("\u0000", "")
                    | _ -> value
                
                let writeRow (row : List<obj * NpgsqlDbType>) = async {
                        do! writer.StartRowAsync() |> Async.AwaitTask
                        // Обрабатываем каждую колонку в строке
                        do! row
                            |> Seq.iterAsync (fun (value, dbType) ->
                                writer.WriteAsync((trySanitize value dbType), dbType)
                                |> Async.AwaitTask
                            )
                    }
                
                do! records.ColumnValues |> Seq.iterAsync writeRow
                do! writer.CompleteAsync().AsTask() |> Async.AwaitTask |> Async.Ignore

                return Ok ()
            with
            | ex ->
                GlobalLogger.instance.logError "Failed to write records" ex
                do! transaction.RollbackAsync() |> Async.AwaitTask
                return Error ex.Message
        }
        
    let private tryRunQuery (connection: NpgsqlConnection) query =
        async {
            try
                use command = connection.CreateCommand()
                command.CommandText <- query
                let! _ = command.ExecuteNonQueryAsync() |> Async.AwaitTask
                return Ok()
            with ex ->
                GlobalLogger.instance.logError "" ex
                return Error ex.Message
        }
        
    let tryFinish (transaction : NpgsqlTransaction) = async {
        try
            do! transaction.CommitAsync() |> Async.AwaitTask
            return Ok ()
        with
        | ex ->
            GlobalLogger.instance.logError "" ex
            return Error ex.Message
    }
    
    let tryCreateAsync cs = async {
        try
            let connection = new NpgsqlConnection(cs)
            do! connection.OpenAsync() |> Async.AwaitTask
            let! transaction = connection.BeginTransactionAsync().AsTask() |> Async.AwaitTask
            
            let tryRunQuery' = tryRunQuery connection
            let tryWriteRecords' = tryWriteRecordsAsync connection transaction
            let destroy' () =
                transaction.Dispose()
                connection.Dispose()
            let tryFinish' () = tryFinish transaction
            return Ok {
                tryRunQuery = tryRunQuery'
                tryWriteRecords = tryWriteRecords'
                destroy = destroy'
                tryFinish = tryFinish'
            }
        with
        | ex ->
            GlobalLogger.instance.logError "" ex
            return Error ex.Message
    }