namespace PgMigrator

open System
open System.IO

module PgMigratorMain =
    [<EntryPoint>]
    let main args =
        try
            let configFile =
                if args.Length = 0 || not (File.Exists args.[0]) then "config.yaml"
                else args.[0]
                
            // Читаем конфигурацию
            let config = MigrationConfigManager.readConfig configFile

            let tables = PgsqlTableManager.getTablesInfo config.SourceCs
            
            let sortedFilteredTables =
                if config.Tables.Count = 0 then tables
                else
                    config.Tables
                        |> Seq.choose (fun ct ->
                            tables
                            |> Seq.tryFind (fun t -> 
                                String.Equals(ct, t.TableName, StringComparison.InvariantCultureIgnoreCase))
                        )
                        |> Seq.toList

            if sortedFilteredTables.Length > 0 then
                printf "Selected tables:\n"
                
                sortedFilteredTables |> List.iter (fun t -> printfn $"%s{t.TableName}")
                0
            else
                printf "No selected tables."
                0
        with ex ->
            printfn $"Error: %s{ex.ToString()}"
            1
