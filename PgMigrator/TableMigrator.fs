namespace PgMigrator

open PgMigrator.DataProviders
open PgMigrator.Types

module TableMigrator =
    /// Миграция всех таблиц
    let tryMigrateAllTablesAsync
        sourceProvider
        pgSession
        targetSchema
        (tables: string list)
        (tableMappings : TableMapping list)
        typeMappings
        removeNullBytes =
            
        let tryMigrateTableAsync tableName =
             async {
                try
                    printfn $"Migrating table: %s{tableName}"

                    let! recordsResult = sourceProvider.tryReadTable tableName typeMappings
                    
                    let records =
                        match recordsResult with
                        | Ok r -> r
                        | Error e -> failwith e

                    let! result = pgSession.tryWriteRecords records targetSchema tableName tableMappings removeNullBytes

                    return
                        match result with
                        | Ok _ -> Ok()
                        | Error e -> failwith e
                with ex ->
                    return Error ex.Message
            }
        
        // Рекурсивный обход всех таблиц
        let rec go xs =
            async {
                match xs with
                | [] -> return Ok ()
                | tableName :: rest ->
                    let! result = tableName |> tryMigrateTableAsync 
                    match result with
                    | Ok () -> return! go rest
                    | Error e -> return Error e
            }
        go tables