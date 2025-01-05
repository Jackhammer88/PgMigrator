namespace PgMigrator

open Dapper
open Npgsql
open DbInfoTypes

module PgsqlTableManager =
    let getTablesInfo (cs: string) : List<TableInfo> =
        use connection = new NpgsqlConnection(cs)

        let query =
            """SELECT
        t.table_name AS TableName,
        c.column_name AS ColumnName,
        c.data_type AS DataType,
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

        connection.Query<AllTablesInfo>(query)
        |> Seq.map (fun row ->
            { TableName = row.TableName
              ColumnName = row.ColumnName
              DataType = row.DataType
              IsNullable = row.IsNullable
              IsPrimaryKey = row.IsPrimaryKey })
        |> Seq.groupBy (fun info -> info.TableName)
        |> Seq.map (fun (tableName, columns) ->
            { TableName = tableName
              PkCount =
                  columns
                  |> Seq.filter (fun i -> i.IsPrimaryKey)
                  |> Seq.length
              Columns =
                columns
                |> Seq.map (fun c ->
                    { ColumnName = c.ColumnName
                      DataType = c.DataType
                      IsNullable = c.IsNullable
                      IsPrimaryKey = c.IsPrimaryKey })
                |> Seq.toList })
        |> Seq.toList
        
    let private formatColumnList (tableInfo : TableInfo) =
        let compositePk =
            if tableInfo.PkCount > 1 then
                    let pkColumns = 
                        tableInfo.Columns
                        |> List.filter (_.IsPrimaryKey)
                        |> List.map (_.ColumnName)
                        |> String.concat ", "
                    $",\n\tCONSTRAINT %s{tableInfo.TableName}_pk PRIMARY KEY (%s{pkColumns})"
                else ""
        let singlePk = if tableInfo.PkCount = 1 then " PRIMARY KEY" else ""
        let columns =
            tableInfo.Columns
            |> List.map (fun i -> $"""
        %s{i.ColumnName} %s{i.DataType} %s{if i.IsNullable then "NULL" else "NOT NULL"}%s{if i.IsPrimaryKey then singlePk else ""}""")
            |> String.concat ","
        $"""
    CREATE TABLE public.{tableInfo.TableName} ({columns}{compositePk}
    );
    """
