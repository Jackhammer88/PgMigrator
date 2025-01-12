namespace PgMigrator.Types

open System
open Npgsql
open PgMigrator
open PgMigrator.DataProviders

type PgSession = {
    tryRunQuery : string -> Async<Result<unit,string>>
    tryWriteRecords : SourceTableData -> string -> string -> TableMapping list -> Async<Result<unit,string>>
    destroy : unit -> unit
    tryFinish : unit -> Async<Result<unit,string>>
} with interface IDisposable with
           member this.Dispose() = this.destroy()

module PgSessionFactory =
    let tryWriteRecordsAsync
        (connection : NpgsqlConnection)
        (transaction : NpgsqlTransaction)
        (records : SourceTableData)
        schema
        tableName
        (tableMappings : TableMapping list)= async {
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
                    
                
                for row in records.ColumnValues do
                    do! writer.StartRowAsync() |> Async.AwaitTask
                    
                    for value,dbType in row do
                        do! writer.WriteAsync(value,dbType) |> Async.AwaitTask |> Async.Ignore
                 
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