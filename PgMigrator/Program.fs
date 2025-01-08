namespace PgMigrator

open System
open System.Diagnostics

module PgMigratorMain =
    [<EntryPoint>]
    let main args =
        try
            CommandLineParser.processCliCommands args
            let configFile = CommandLineParser.getConfigPath args

            // Читаем конфигурацию
            let config = MigrationConfigManager.readConfig configFile

            let tables = DbTableManager.getTablesInfo config.SourceCs config.SourceType

            let sortedFilteredTables =
                if config.Tables.Count = 0 then
                    tables
                else
                    config.Tables
                    |> Seq.choose (fun ct ->
                        tables
                        |> Seq.tryFind (fun t ->
                            String.Equals(ct, t.TableName, StringComparison.InvariantCultureIgnoreCase)))
                    |> Seq.toList

            if sortedFilteredTables.Length > 0 then                
                let stopwatch = Stopwatch.StartNew()
                
                let schema = DbSchemaGenerator.generatePgSchema sortedFilteredTables config
                //printfn $"%s{schema}"
                TargetDbScriptRunner.runScript config.TargetCs schema
                //
                // sortedFilteredTables
                // |> Seq.iter (fun t ->
                //     printfn $"Migrating table: %s{t.TableName}"
                //     SourceDataProvider.migrateTable config.SourceCs config.TargetCs config.SourceType t.TableName
                //     )
                
                //
                sortedFilteredTables
                |> Seq.iter (fun t ->
                    printfn $"Migrating table: %s{t.TableName}"
                    SourceDataProvider.migrateTable' config t.TableName
                    )
                
                printfn $"{stopwatch.Elapsed.TotalSeconds} seconds"
                0
            else
                printf "No selected tables."
                0
        with ex ->
            printfn $"Error: %s{ex.ToString()}"
            1
