namespace PgMigrator.DataProviders

open System.Data
open Microsoft.Data.SqlClient
open Microsoft.FSharp.Control
open PgMigrator.Mapping
open PgMigrator.Types
open Dapper

module MssqlProvider =
    let processDbTablesInfo (tables: ColumnInfo list) : TableInfo list =
        tables
        |> List.groupBy _.TableName
        |> List.map (fun (tableName, columns) ->
            { TableName = tableName
              Columns = columns |> Seq.toList
              PkCount = columns |> Seq.filter _.IsPrimaryKey |> Seq.length })

    let tryGetTablesInfo (connection: IDbConnection) schema () =
        let schemaCondition =
            match schema with
            | Some s -> $"AND t.TABLE_SCHEMA = '{s}'"
            | None -> "AND t.TABLE_SCHEMA NOT IN ('sys', 'information_schema')"

        let query =
            $"""
        SELECT 
    t.TABLE_NAME AS TableName,
    c.COLUMN_NAME AS ColumnName,
    CASE 
        WHEN c.DATA_TYPE IN ('char', 'varchar', 'nvarchar', 'binary', 'varbinary') THEN 
            c.DATA_TYPE + '(' + 
            CASE 
                WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN 'max'
                ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR)
            END + ')'
        WHEN c.DATA_TYPE IN ('numeric', 'decimal') THEN 
            c.DATA_TYPE + '(' + 
            CAST(c.NUMERIC_PRECISION AS VARCHAR) + ', ' + 
            CAST(c.NUMERIC_SCALE AS VARCHAR) + ')'
        ELSE 
            c.DATA_TYPE
    END AS DataType,
    CASE 
        WHEN c.IS_NULLABLE = 'YES' THEN 1
        ELSE 0
    END AS IsNullable,
    CASE 
        WHEN pk.COLUMN_NAME IS NOT NULL THEN 1
        ELSE 0
    END AS IsPrimaryKey
FROM 
    INFORMATION_SCHEMA.TABLES t
JOIN 
    INFORMATION_SCHEMA.COLUMNS c
ON 
    t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
LEFT JOIN 
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk
ON 
    pk.TABLE_NAME = t.TABLE_NAME 
    AND pk.TABLE_SCHEMA = t.TABLE_SCHEMA 
    AND pk.COLUMN_NAME = c.COLUMN_NAME
    AND EXISTS (
        SELECT 1 
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        WHERE 
            tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
            AND tc.CONSTRAINT_NAME = pk.CONSTRAINT_NAME
            AND tc.TABLE_NAME = pk.TABLE_NAME
            AND tc.TABLE_SCHEMA = pk.TABLE_SCHEMA
    )
WHERE 
    t.TABLE_TYPE = 'BASE TABLE'
    {schemaCondition}
ORDER BY 
    t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION;
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
        (connection: SqlConnection)
        (sourceSchema: string option)
        (tableName: string)
        typeMappings
        =
        async {
            let escapedTableName = $"[{tableName}]" |> addSchemaName sourceSchema

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

                return
                    Ok
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
                let connection = new SqlConnection(cs)
                do! connection.OpenAsync() |> Async.AwaitTask

                return
                    Ok
                        {
                          SourceType = Mssql
                          tryGetTablesInfo = tryGetTablesInfo connection schema
                          tryReadTable = tryReadTableAsync connection schema
                          destroy = connection.Dispose
                          }
            with ex ->
                System.Console.Error.WriteLine(ex)
                return Error ex.Message
        }
