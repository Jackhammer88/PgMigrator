namespace PgMigrator

open PgMigrator.DataProviders
open PgMigrator.Types

module SourceDataProvider =
    let private tryMigrateTableAsync
        (sourceProvider : SourceProvider)
        (pgSession: PgSession)
        (config: MigrationConfig)
        tableName
        (typeMappings: Map<string, TypeMapping>)
        = async {
            try
                printfn $"Migrating table: %s{tableName}"

                let! recordsResult = sourceProvider.tryReadTable tableName typeMappings
                
                let records =
                    match recordsResult with
                    | Ok r -> r
                    | Error e -> failwith e

                let! result = pgSession.tryWriteRecords records config.TargetSchema tableName config.TableMappings

                return
                    match result with
                    | Ok _ -> Ok()
                    | Error e -> failwith e
            with ex ->
                return Error ex.Message
        }
    
    let migrateAllTablesAsync tables sourceProvider pgSession config typeMappings =
        tables
        |> Seq.map(fun t ->
            tryMigrateTableAsync sourceProvider pgSession config t typeMappings)
        |> Async.Sequential