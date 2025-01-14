namespace PgMigrator

open System
open System.Diagnostics
open Microsoft.FSharp.Core
open PgMigrator.Config
open PgMigrator.DataProviders
open PgMigrator.Mapping
open FsToolkit.ErrorHandling
open PgMigrator.Types

module PgMigratorMain =
    /// Создаёт карту маппинга типов, включая пользовательские, если они объявлены в конфигурации.
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
            | Mssql -> Map.fold (fun acc key value -> Map.add key value acc) DbTypeDefaultMappings.mssqlToPgsql userMappings
            | Pgsql -> userMappings

        combinedMappings
    
    let processFlowResult flowResult =
        match flowResult with
        | Ok _ ->
            printfn "Migration complete successfully"
            0
        | Error e ->
            printfn $"An error occurred: {e}"
            match GlobalLogger.instance.Mode with
            | FileMode f -> printfn $"{Environment.NewLine}For details, see the logfile at: '{f}'"
            | VerboseFileMode f -> printfn $"{Environment.NewLine}For details, see the logfile at: '{f}'"
            | _ -> ()
            1
    
    /// Фильтрует таблицы БД согласно списка БД из конфигурации.
    /// При пустом списке конфигурации выбирает все доступные таблицы.
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
    
    /// Пытается подготовить источник миграции к использованию.
    let tryGetSourceProvider (connectionsInfo : ConnectionsInfo) =
        let cs = connectionsInfo.SourceCs
        let schema = connectionsInfo.SourceSchema
        
        match connectionsInfo.SourceType with
            | Mssql -> MssqlProvider.createAsync cs schema |> Async.RunSynchronously
            | Pgsql -> PgsqlProvider.createAsync cs schema |> Async.RunSynchronously
    
    [<EntryPoint>]
    let main args =
        // Измерение времени выполнения
        let stopwatch = Stopwatch.StartNew()
        
        let flowResult =
            result {
                // Настройка уровня логгирования
                let isLoggerVerbose = CommandLineParser.getLoggerMode args
                GlobalLogger.setupLogger isLoggerVerbose
                
                // Обработка аргументов-действий
                CommandLineParser.processCliCommands args    
                
                // Чтение файла конфигурации
                let configFile = CommandLineParser.getConfigPath args
                
                // Читаем конфигурацию
                let! config = ConfigManager.tryReadConfig configFile
      
                let connectionsInfo = {
                    TargetCs = config.TargetCs
                    SourceCs = config.SourceCs
                    SourceType = config.getSourceType
                    SourceSchema = config.SourceSchema
                    TargetSchema = config.TargetSchema
                }
                
                // Получаем провайдера данных источника
                use! sourceProvider = tryGetSourceProvider connectionsInfo
                
                 // Получение информации о таблицах источника
                let! dbTables = sourceProvider.tryGetTablesInfo() |> Async.RunSynchronously
                
                // Фильтрация по выбранным таблицам
                let filterTables = filterTables config.Tables dbTables
                do! match filterTables.Length with
                    | c when c > 0 -> Ok ()
                    | _ -> Error "No tables found"
                    
                let dbReflectionData = {
                    TablesInfo = filterTables
                    TableMappings = config.TableMappings
                    TypeMappings = generateTypeMappings config.TypeMappings connectionsInfo.SourceType
                    TargetSchema = config.TargetSchema |> Option.defaultValue "public"
                }
                                
                // Создание схемы БД
                let script = SchemaGenerator.makeSchemaScript dbReflectionData                
                use! pgSession = PgSessionFactory.tryCreateAsync connectionsInfo.TargetCs |> Async.RunSynchronously
                do! pgSession.tryRunQuery script |> Async.RunSynchronously
                
                // Миграция таблиц
                let migrationData : MigrationFlowData = {
                    Tables = filterTables |> List.map _.TableName
                    TablesInfo = dbReflectionData.TablesInfo
                    TargetSchema = dbReflectionData.TargetSchema
                    TableMappings = dbReflectionData.TableMappings
                    TypeMappings = dbReflectionData.TypeMappings
                    RemoveNullBytes = config.RemoveNullBytes |> Option.defaultValue false
                }
                
                // Выбор стратегии миграции
                let asyncScenario =
                    match config.BatchSize with
                    | None -> TableMigrator.tryMigrateEagerAsync sourceProvider pgSession migrationData
                    | Some size -> TableMigrator.tryMigrateSequentialAsync sourceProvider pgSession migrationData size
                
                do! asyncScenario |> Async.RunSynchronously
                
                // Применяем транзакцию
                do! pgSession.tryFinish() |> Async.RunSynchronously
            }
        
        let errorCode = processFlowResult flowResult
        
        if errorCode = 0 then
            printfn $"{stopwatch.Elapsed.TotalSeconds} seconds"
            
        errorCode
