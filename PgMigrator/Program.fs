namespace PgMigrator

open System
open DbInfoTypes

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
                printf "Selected tables:\n"

                sortedFilteredTables |> Seq.iter (fun t -> printfn $"%s{t.TableName}")
                
                let schema = DbSchemaGenerator.generatePgSchema sortedFilteredTables config
                printf $"%s{schema}"
                0
            else
                printf "No selected tables."
                0
        with ex ->
            printfn $"Error: %s{ex.ToString()}"
            1
