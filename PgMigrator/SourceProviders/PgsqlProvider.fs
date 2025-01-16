namespace PgMigrator.DataProviders

open System.Data
open Npgsql
open PgMigrator
open PgMigrator.Mapping
open PgMigrator.Types
open Dapper

module PgsqlProvider =
    let processDbTablesInfo (tables: ColumnInfo list) : TableInfo list =
        tables
        |> List.groupBy _.TableName
        |> List.map (fun (tableName, columns) ->
            { TableName = tableName
              Columns = columns |> Seq.toList
              PkCount = columns |> Seq.filter _.IsPrimaryKey |> Seq.length })

    let getTablesInfoAsync (connection: IDbConnection) schema () =
        let schemaCondition =
            match schema with
            | Some s -> $"AND t.table_schema = '{s}'"
            | None -> "AND t.table_schema NOT IN ('pg_catalog', 'information_schema')"

        let query =
            $"""SELECT
        t.table_name AS TableName,
        c.column_name AS ColumnName,
        CASE 
            WHEN c.data_type IN ('char', 'character', 'varchar', 'nvarchar', 'character varying') THEN 
                c.data_type || '(' || 
                CASE 
                    WHEN c.character_maximum_length = -1 THEN 'max'
                    ELSE c.character_maximum_length::VARCHAR
                END || ')'
            WHEN c.data_type IN ('numeric', 'decimal') THEN 
                c.data_type || '(' || c.numeric_precision::VARCHAR || ', ' || c.numeric_scale::VARCHAR || ')'
            WHEN c.data_type = 'USER-DEFINED' THEN 
                pt.typname
            ELSE 
                c.data_type
        END AS  DataType,
        c.is_nullable = 'YES' AS IsNullable,
        CASE
            WHEN kc.column_name IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS IsPrimaryKey
    FROM information_schema.tables t
    JOIN 
        information_schema.columns c
        ON t.table_schema = c.table_schema 
        AND t.table_name = c.table_name
    LEFT JOIN 
        information_schema.key_column_usage kc
        ON kc.table_schema = t.table_schema 
        AND kc.table_name = t.table_name 
        AND kc.column_name = c.column_name 
        AND EXISTS (
            SELECT 1 
            FROM information_schema.table_constraints tc
            WHERE 
                tc.table_schema = kc.table_schema 
                AND tc.table_name = kc.table_name 
                AND tc.constraint_name = kc.constraint_name 
                AND tc.constraint_type = 'PRIMARY KEY'
        )
    LEFT JOIN 
        pg_catalog.pg_attribute pa
        ON pa.attname = c.column_name 
        AND pa.attrelid = (
            SELECT c.oid FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = t.table_name AND n.nspname = t.table_schema
        )
    LEFT JOIN 
        pg_catalog.pg_type pt
    ON pt.oid = pa.atttypid
    WHERE 
        t.table_type = 'BASE TABLE'
        {schemaCondition}
    ORDER BY 
        t.table_schema, t.table_name, c.ordinal_position;
            """

        async {
            try
                let! columns = connection.QueryAsync<ColumnInfo>(query) |> Async.AwaitTask
                return columns.AsList() |> List.ofSeq |> processDbTablesInfo |> Result.Ok
            with ex ->
                GlobalLogger.instance.logError "" ex
                return Error ex.Message
        }

    let private mapNameToPgType columnNumber columnName typeName typeMappings =
        let targetTypeName = DbTypeMapper.getTargetTypeName typeName typeMappings

        let targetType = DbTypeMapper.getNpgsqlDbType targetTypeName
        (columnNumber, columnName, typeName, targetType)

    let private addSchemaName schema tableName =
        match schema with
        | Some s -> $"{s}.{tableName}"
        | None -> tableName

    let private escapeColumnName (columnName: string) =
        match columnName.ToLowerInvariant() with
        | s when s = "date" || s = "user" || s = "order" -> $"\"{s}\""
        | _ -> columnName

    let fetchDataAsync
        (connection: NpgsqlConnection)
        typeMappings
        query = async {
        try
            // Выполним SELECT * из исходной БД
            use selectCommand = connection.CreateCommand()
            selectCommand.CommandText <- query
            use! sourceReader = selectCommand.ExecuteReaderAsync() |> Async.AwaitTask

            // Получаем имена и типы столбцов исходной таблицы
            let columnsCount = sourceReader.FieldCount

            let columnNames =
                [ for i in 0 .. columnsCount - 1 do
                      let columnName = sourceReader.GetName(i)
                      let columnTypeName = sourceReader.GetDataTypeName(i)
                      yield mapNameToPgType i columnName columnTypeName typeMappings ]

            // Форматируем строку с именами столбцов для вставки
            let columnNamesString =
                columnNames
                |> List.map (fun (_, name, _, _) -> escapeColumnName name)
                |> String.concat ", "

            return Ok
                { ColumnNamesString = columnNamesString
                  ColumnValues =
                    [ while sourceReader.Read() do
                          yield columnNames
                                |> List.map (fun (n, _, _, t) -> sourceReader.GetValue(n), t) ] }
        with ex ->
            GlobalLogger.instance.logError "" ex
            return Error ex.Message
    }
    
    let tryReadTableAsync
        (connection: NpgsqlConnection)
        (sourceSchema : string option)
        (tableName : string)
        typeMappings=
        
        let escapedTableName = $"\"{tableName}\"" |> addSchemaName sourceSchema
        
        $"SELECT * FROM {escapedTableName}"
        |> fetchDataAsync connection typeMappings
        
    let tryReadTablePartAsync
        (connection: NpgsqlConnection)
        (sourceSchema: string option)
        (tableName: string)
        (tableInfo: TableInfo)
        typeMappings
        offset
        count= async {
            if tableInfo.Columns.Length = 0 then
                failwith $"tableName: '{tableName}'. tableInfo.Columns.Length = 0"
            
            let escapedTableName = $"\"{tableName}\"" |> addSchemaName sourceSchema
            
            let keysString =
                    tableInfo.Columns
                    |> List.filter _.IsPrimaryKey
                    |> List.map _.ColumnName
                    |> function
                        | [] -> tableInfo.Columns |> List.head |> _.ColumnName
                        | keys -> keys |> String.concat ", "
                    |> (fun c -> $"ORDER BY {c}")

            let query =
                $"SELECT * FROM {escapedTableName} {keysString} OFFSET {offset} LIMIT {count}"
            
            return! fetchDataAsync connection typeMappings query
        }
    

    let createAsync cs schema =
        async {
            try
                let connection = new NpgsqlConnection(cs)
                do! connection.OpenAsync() |> Async.AwaitTask
                
                return
                    Ok
                        { SourceType = Pgsql
                          tryReadTable = tryReadTableAsync connection schema
                          tryReadTablePart = tryReadTablePartAsync connection schema
                          destroy = connection.Dispose
                          tryGetTablesInfo = getTablesInfoAsync connection schema }
            with ex ->
                GlobalLogger.instance.logError "" ex
                return Error ex.Message
        }
