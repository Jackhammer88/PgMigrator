namespace PgMigrator

open Dapper
open Microsoft.Data.SqlClient
open Npgsql

module DbTableManager =
    let processDbTablesInfo (tables: List<ColumnInfo>) : List<TableInfo> =
            tables
            |> Seq.groupBy _.TableName
            |> Seq.map (fun (tableName, columns) ->
                {
                    TableName = tableName
                    Columns =
                        columns
                        |> Seq.toList
                    PkCount =
                        columns
                        |> Seq.filter _.IsPrimaryKey |> Seq.length
                })
            |> Seq.toList
        
    let getPostgresTablesInfo (cs: string) : List<TableInfo> =
        use connection = new NpgsqlConnection(cs)

        let query =
            """SELECT
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
        AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
    ORDER BY 
        t.table_schema, t.table_name, c.ordinal_position;
            """
        connection.Query<ColumnInfo>(query).AsList()
        |> Seq.toList
        |> processDbTablesInfo
        
    let getMssqlTablesInfo (cs: string) : List<TableInfo> =
        use connection = new SqlConnection(cs)
        let query = """
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
    AND t.TABLE_SCHEMA NOT IN ('sys', 'information_schema')
ORDER BY 
    t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION;
"""
        connection.Query<ColumnInfo>(query).AsList()
            |> Seq.toList
            |> processDbTablesInfo
        
    let getTablesInfo (cs: string) (sourceType: string) : List<TableInfo> =
        match sourceType.ToLowerInvariant() with
        | SourceTypes.postgres -> getPostgresTablesInfo cs
        | SourceTypes.mssql -> getMssqlTablesInfo cs
        | _ -> failwithf $"Wrong db type: %s{sourceType}"
