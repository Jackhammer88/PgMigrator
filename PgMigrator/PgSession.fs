namespace PgMigrator.Types

open System
open Npgsql
open PgMigrator.DataProviders

type PgSession = {
    tryRunQuery : string -> Async<Result<unit,string>>
    tryWriteRecords : SourceTableData -> string option -> string -> TableMapping list -> Async<Result<unit,string>>
    destroy : unit -> unit
    tryFinish : unit -> Async<Result<unit,string>>
} with interface IDisposable with
           member this.Dispose() = this.destroy()

module PgSessionFactory =
    let tryWriteRecordsAsync
        (connection : NpgsqlConnection)
        (transaction : NpgsqlTransaction)
        (records : SourceTableData)
        targetSchema
        tableName
        (tableMappings : TableMapping list)= async {
            try
                // Если требуется, преобразуем имя целевой схемы
                let schema = targetSchema |> Option.defaultValue "public"

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

                let complete =
                    records.ColumnValues
                    |> Seq.iter(fun l ->
                        writer.StartRow()
                        
                        l |> List.iter (fun (value,dbType) ->
                            writer.Write(value,dbType)
                            ())
                        ())
                    |> writer.Complete
                
                return Ok ()
            with
            | ex ->
                System.Console.Error.WriteLine(ex)
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
                System.Console.Error.WriteLine(ex)
                return Error ex.Message
        }
        
    let tryFinish (transaction : NpgsqlTransaction) = async {
        try
            do! transaction.CommitAsync() |> Async.AwaitTask            
            return Ok ()
        with
        | ex ->
            System.Console.Error.WriteLine(ex)
            return Error ex.Message
    }
    
    let tryCreateAsync cs = async {
        try
            let connection = new NpgsqlConnection(cs)
            do! connection.OpenAsync() |> Async.AwaitTask
            let! transaction = connection.BeginTransactionAsync().AsTask() |> Async.AwaitTask
            
            return Ok {
                tryRunQuery = tryRunQuery connection
                tryWriteRecords = tryWriteRecordsAsync connection transaction
                
                destroy = fun _ ->
                    transaction.Dispose()
                    connection.Dispose()
                    
                tryFinish = fun _ -> tryFinish transaction
            }
        with
        | ex ->
            System.Console.Error.WriteLine(ex)
            return Error ex.Message
    }