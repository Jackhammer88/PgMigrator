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
    let tryGetSourceProvider sourceType cs schema =
        match sourceType with
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
                
                let sourceType = config.getSourceType
                let sourceSchema = config.SourceSchema
                let targetSchema = config.TargetSchema |> Option.defaultValue "public"
                let sourceCs = config.SourceCs
                let targetCs = config.TargetCs
                let typeMappings = generateTypeMappings config.TypeMappings sourceType
                let tableMappings = config.TableMappings
                let removeNullBytes = config.RemoveNullBytes |> Option.defaultValue false
                
                // Получаем провайдера данных источника
                use! sourceProvider = tryGetSourceProvider sourceType sourceCs sourceSchema
                
                 // Получение информации о таблицах источника
                let! dbTables = sourceProvider.tryGetTablesInfo() |> Async.RunSynchronously
                
                // Фильтрация по выбранным таблицам
                let filteredTable = filterTables config.Tables dbTables
                do! match filteredTable.Length with
                    | c when c > 0 -> Ok ()
                    | _ -> Error "No tables found"
                                
                // Создание схемы БД
                let script = SchemaGenerator.makeSchemaScript filteredTable tableMappings typeMappings targetSchema                
                use! pgSession = PgSessionFactory.tryCreateAsync targetCs |> Async.RunSynchronously
                do! pgSession.tryRunQuery script |> Async.RunSynchronously
                
                // Миграция таблиц
                let tableNames = filteredTable |> List.map _.TableName
                do! TableMigrator.tryMigrateAllTablesAsync sourceProvider pgSession targetSchema tableNames tableMappings typeMappings removeNullBytes
                    |> Async.RunSynchronously
               
                // Применяем транзакцию
                do! pgSession.tryFinish() |> Async.RunSynchronously
            }
        
        let errorCode = processFlowResult flowResult
        
        if errorCode = 0 then
            printfn $"{stopwatch.Elapsed.TotalSeconds} seconds"
            
        errorCode
