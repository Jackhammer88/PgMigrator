namespace PgMigrator.Types

open System
open Npgsql
open NpgsqlTypes
open PgMigrator
open PgMigrator.DataProviders

type PgSession = {
    tryRunQuery : string -> Async<Result<unit,string>>
    tryWriteRecords : SourceTableData -> string -> string -> bool -> Async<Result<unit,string>>
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
        removeNullBytes
        = async {
            try                    
                let targetTableName = $"{schema}.{tableName}"
                    
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
                GlobalLogger.instance.logError $"Failed to write records to table '{schema}.{tableName}'." ex
                do! transaction.RollbackAsync() |> Async.AwaitTask
                return Error $"Failed to write records to table '{schema}.{tableName}': {ex.Message}"
        }
        
    let private tryRunQuery (connection: NpgsqlConnection) query =
        async {
            try
                use command = connection.CreateCommand()
                command.CommandText <- query
                let! _ = command.ExecuteNonQueryAsync() |> Async.AwaitTask
                return Ok()
            with ex ->
                GlobalLogger.instance.logError "Query execution failed." ex
                return Error $"Query execution failed. {ex.Message}"
        }
        
    let tryFinish (transaction : NpgsqlTransaction) = async {
        try
            do! transaction.CommitAsync() |> Async.AwaitTask
            return Ok ()
        with
        | ex ->
            GlobalLogger.instance.logError "Failed to commit transaction." ex
            return Error $"Failed to commit transaction. {ex.Message}"
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
            GlobalLogger.instance.logError "Failed to establish a connection to the target database." ex
            return Error $"Failed to establish a connection to the target database. {ex.Message}"
    }