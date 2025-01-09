namespace PgMigrator

open System
open System.Diagnostics
open Npgsql
open PgMigrator.Config
open PgMigrator.Mapping

module PgMigratorMain =
    let generateTypeMappings (userTypeMappings : TypeMapping list) sourceType =
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
    
    [<EntryPoint>]
    let main args =
        CommandLineParser.processCliCommands args
        let configFile = CommandLineParser.getConfigPath args

        // Читаем конфигурацию
        let config = ConfigManager.readConfig configFile
        let finalTypeMappings = generateTypeMappings config.TypeMappings config.SourceType
        
        // Получение информации о таблицах источника
        let tables = TableManager.getTablesInfo config.SourceCs config.SourceType config.SourceSchema

        // Фильтрация по выбранным таблицам
        let filteredTable =
            if config.Tables.Length = 0 then
                tables
            else
                config.Tables
                |> Seq.choose (fun ct ->
                    tables
                    |> Seq.tryFind (fun t ->
                        String.Equals(ct, t.TableName, StringComparison.InvariantCultureIgnoreCase)))
                |> Seq.toList

        if filteredTable.Length > 0 then
            let stopwatch = Stopwatch.StartNew()

            use sourceConnection =
                SourceDataProvider.getSourceConnection config.SourceCs config.SourceType
            sourceConnection.Open()
            
            use targetConnection = new NpgsqlConnection(config.TargetCs)
            targetConnection.Open()
            use transaction = targetConnection.BeginTransaction()

            try
                let schemaScript =
                    SchemaGenerator.generatePgSchemaScript filteredTable config finalTypeMappings
                    
                TargetDbScriptRunner.run targetConnection transaction schemaScript

                filteredTable
                |> Seq.iter (fun t ->
                    printfn $"Migrating table: %s{t.TableName}"
                    SourceDataProvider.migrateTable sourceConnection targetConnection config t.TableName finalTypeMappings)

                transaction.Commit()

                printfn $"{stopwatch.Elapsed.TotalSeconds} seconds"
                0
            with ex ->
                transaction.Rollback()
                printfn $"Error: %s{ex.ToString()}"
                1
        else
            printf "No selected tables."
            0
