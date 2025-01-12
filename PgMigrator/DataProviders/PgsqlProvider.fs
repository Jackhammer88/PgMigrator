namespace PgMigrator.DataProviders

open System.Data
open FsToolkit.ErrorHandling
open Npgsql
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
            ELSE 
                c.data_type
        END AS  DataType,
        c.is_nullable = 'YES' AS IsNullable,
        CASE
            WHEN kc.column_name IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS IsPrimaryKey
    FROM 
        information_schema.tables t
    JOIN 
        information_schema.columns c
    ON 
        t.table_schema = c.table_schema AND t.table_name = c.table_name
    LEFT JOIN 
        information_schema.key_column_usage kc
    ON 
        kc.table_schema = t.table_schema AND 
        kc.table_name = t.table_name AND 
        kc.column_name = c.column_name AND 
        EXISTS (
            SELECT 1 
            FROM information_schema.table_constraints tc
            WHERE 
                tc.table_schema = kc.table_schema AND 
                tc.table_name = kc.table_name AND 
                tc.constraint_name = kc.constraint_name AND 
                tc.constraint_type = 'PRIMARY KEY'
        )
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
                System.Console.Error.WriteLine(ex)
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

    let tryReadTableAsync
        (connection: NpgsqlConnection)
        (sourceSchema : string option)
        (tableName : string)
        typeMappings=
        async {
            let escapedTableName = $"\"{tableName}\"" |> addSchemaName sourceSchema

            try
                // Выполним SELECT * из исходной БД
                use selectCommand = connection.CreateCommand()
                selectCommand.CommandText <- $"SELECT * FROM {escapedTableName}"
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
                System.Console.WriteLine(ex)
                return Error ex.Message
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
                          destroy = connection.Dispose
                          tryGetTablesInfo = getTablesInfoAsync connection schema }
            with ex ->
                System.Console.Error.WriteLine(ex)
                return Error ex.Message
        }