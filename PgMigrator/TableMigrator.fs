namespace PgMigrator

open PgMigrator.Config
open PgMigrator.DataProviders
open PgMigrator.Types

module TableMigrator =
    
    /// Map имён старых-новых таблиц для ускорения их поиска. (Old, TableMapping) 
    let private createTablesMap (tablesMappings : TableMapping list) =
        tablesMappings
        |> List.map (fun mapping -> mapping.Old, mapping) 
        |> Map.ofList
        
    let printProcess oldTableName newTableName =
        printfn $"Migrating table: {oldTableName} -> {newTableName}"
        
    /// Миграция всех таблиц
    let tryMigrateEagerAsync
        sourceProvider
        pgSession
        (flowData: MigrationFlowData)=
            
        let tables = flowData.Tables
        let targetSchema = flowData.TargetSchema
        let tablesMappingsSet = createTablesMap flowData.TableMappings
        let typeMappings = flowData.TypeMappings 
        let removeNullBytes = flowData.RemoveNullBytes
            
        let tryMigrateTableAsync tableName =
             async {
                try
                    let newTableName =
                        match tablesMappingsSet.TryFind tableName with
                        | Some n -> n.New
                        | None -> tableName
                    
                    printProcess tableName newTableName

                    let! recordsResult = sourceProvider.tryReadTable newTableName typeMappings
                    
                    let records =
                        match recordsResult with
                        | Ok r -> r
                        | Error e -> failwith e

                    let! result = pgSession.tryWriteRecords records targetSchema tableName removeNullBytes

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
              TablesInfo=tablesInfo
              TypeMappings=typeMappings 
              RemoveNullBytes=removeNullBytes
              } = flowData
        
        let tablesMappingsSet = createTablesMap flowData.TableMappings
            
        let tryMigrateTableAsync tableName =
             async {
                try
                    let newTableName =
                        match tablesMappingsSet.TryFind tableName with
                        | Some n -> n.New
                        | None -> tableName
                    
                    printProcess tableName newTableName
                    
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
                                        pgSession.tryWriteRecords records targetSchema newTableName removeNullBytes
                                    
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