namespace PgMigrator

open PgMigrator.Config
open PgMigrator.DataProviders
open PgMigrator.Types

module TableMigrator =
    /// Миграция всех таблиц
    let tryMigrateEagerAsync
        sourceProvider
        pgSession
        (flowData: MigrationFlowData)=
            
        let tables = flowData.Tables
        let targetSchema = flowData.TargetSchema
        let tableMappings = flowData.TableMappings
        let typeMappings = flowData.TypeMappings 
        let removeNullBytes = flowData.RemoveNullBytes
            
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
                        | Error e -> Error e
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
        
    let tryMigrateSequentialAsync
        sourceProvider
        pgSession
        flowData
        batchSize=
        let { Tables=tables
              TargetSchema=targetSchema
              TableMappings=tableMappings
              TablesInfo=tablesInfo
              TypeMappings=typeMappings 
              RemoveNullBytes=removeNullBytes
              } = flowData
            
        let tryMigrateTableAsync tableName =
             async {
                try
                    printfn $"Migrating table: %s{tableName}"
                    
                    let tableInfo =
                        tablesInfo
                        |> List.tryFind (fun t -> t.TableName = tableName)
                        |> Option.defaultWith (fun _ -> failwith $"Table: '{tableName}' - TableInfo not found")                                   
                    
                    let iterate =
                        // Рекурсивный обход всех записей таблицы батчами
                        let rec go offset = async {
                            let! recordsResult =
                                sourceProvider.tryReadTablePart tableName tableInfo typeMappings offset batchSize
                            
                            match recordsResult with
                            | Error e -> return Error e
                            | Ok records ->
                                if records.ColumnValues.Length = 0 then
                                    return Ok ()
                                else
                                    let! result =
                                        pgSession.tryWriteRecords records targetSchema tableName tableMappings removeNullBytes
                                    
                                    match result with
                                    | Error e -> return Error e
                                    | Ok _ ->
                                        if records.ColumnValues.Length < batchSize then
                                            return Ok ()
                                        else
                                            return! go (offset + batchSize)
                        }
                        go 0

                    return! iterate
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