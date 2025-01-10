namespace PgMigrator

open System
open System.Diagnostics
open Npgsql
open PgMigrator.Config
open PgMigrator.Mapping
open FsToolkit.ErrorHandling
open PgMigrator.Types

module PgMigratorMain =
    let generateTypeMappings userTypeMappings sourceType =
        let userMappings = 
            userTypeMappings
            |> List.map (fun m -> 
                let key = 
                    m.Old.Param 
                    |> Option.map (fun p -> $"{m.Old.Type}({p})") 
                    |> Option.defaultValue m.Old.Type
                key, m)
            |> Map.ofList

        // Объединяем словари. Пользовательские маппинги перезаписывают дефолтные
        let combinedMappings =
            match sourceType with
            | SourceTypes.mssql -> Map.fold (fun acc key value -> Map.add key value acc) DbTypeDefaultMappings.mssqlToPgsql userMappings
            | SourceTypes.postgres -> userMappings
            | _ -> failwithf $"Unknown source type: '{sourceType}'"

        combinedMappings
    
    let filterTables (userTables : string list) dbTables =
        if userTables.Length <> 0 then
            userTables
            |> Seq.choose (fun ct ->
                dbTables
                |> Seq.tryFind (fun t ->
                    String.Equals(ct, t.TableName, StringComparison.InvariantCultureIgnoreCase)))
            |> Seq.toList
        else
            dbTables
            
    /// Подготовка подключений
    let prepareConnections config =
        try
            let sourceConnection =
                SourceDataProvider.getSourceConnection config.SourceCs config.SourceType
            sourceConnection.Open()
            
            let targetConnection = new NpgsqlConnection(config.TargetCs)
            targetConnection.Open()
            let transaction = targetConnection.BeginTransaction()
            
            {
                Source = sourceConnection
                Target = targetConnection
                Transaction = transaction
            }
        with
        | ex ->
            Console.Error.WriteLine(ex)
            failwith $"{ex}"
    
    [<EntryPoint>]
    let main args =
        CommandLineParser.processCliCommands args
        let configFile = CommandLineParser.getConfigPath args

        // Читаем конфигурацию
        let config = ConfigManager.readConfig configFile
        let typeMappings = generateTypeMappings config.TypeMappings config.SourceType
        
        // Получение информации о таблицах источника
        let dbTables = TableManager.getTablesInfo config.SourceCs config.SourceType config.SourceSchema

        // Фильтрация по выбранным таблицам
        let filteredTable = filterTables config.Tables dbTables

        if filteredTable.Length > 0 then
            let stopwatch = Stopwatch.StartNew()

            use connectionsInfo = prepareConnections config
            
            let flowResult =
                result {                    
                    // Создание схемы БД
                    let script = SchemaGenerator.makeSchemaScript filteredTable config typeMappings
                    do! TargetDbScriptRunner.tryRun connectionsInfo script
                    
                    // Мигация таблиц
                    do! SourceDataProvider.migrateAllTablesAsync filteredTable connectionsInfo config typeMappings
                        |> Async.RunSynchronously
                        |> Seq.filter _.IsError
                        |> Seq.tryPick (function
                            | Error e -> Some (Error e) // Возвращаем ошибку, если она есть
                            | Ok _ -> None) // Пропускаем успешные результаты
                        |> function
                            | Some err -> err // Возвращаем первую ошибку
                            | None -> Ok ()   // Если ошибок нет, возвращаем Ok
                }
            
            match flowResult with
            | Ok _ ->
                connectionsInfo.Transaction.Commit()
                printfn "Migration complete successfully"
                printfn $"{stopwatch.Elapsed.TotalSeconds} seconds"
                0
            | Error e ->
                connectionsInfo.Transaction.Rollback()
                printfn $"Error: {e}"
                1
        else
            printf "No selected tables."
            0
