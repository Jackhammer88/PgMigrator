namespace PgMigrator

open PgMigrator.Config
open PgMigrator.Mapping
open PgMigrator.Types

module SchemaGenerator =
    let private escapePgTableName name =
        match name with
        | "user" -> "\"user\""
        | n -> n

    let private formatColumnList
        (tableInfo: TableInfo)
        (typeMap: Map<string, TypeMapping>)
        (tableMap: Map<string, TableMapping>)
        targetSchema
        =
        let newTableName =
            match tableMap.TryFind tableInfo.TableName with
            | Some newName -> newName.New
            | None -> tableInfo.TableName
            |> escapePgTableName
            |> fun tableName -> $"{targetSchema}.{tableName}"

        let compositePk =
            if tableInfo.PkCount > 1 then
                let pkColumns =
                    tableInfo.Columns
                    |> Seq.filter _.IsPrimaryKey
                    |> Seq.map _.ColumnName
                    |> String.concat ", "

                $",\n    CONSTRAINT %s{tableInfo.TableName}_pk PRIMARY KEY (%s{pkColumns})"
            else
                ""

        let singlePk = if tableInfo.PkCount = 1 then " PRIMARY KEY" else ""

        let columns =
            tableInfo.Columns
            |> Seq.map (fun i ->
                let newTypeName = DbTypeMapper.getTargetTypeName i.DataType typeMap

                $"""
    %s{i.ColumnName} %s{newTypeName} %s{if i.IsNullable then "NULL" else "NOT NULL"}%s{if i.IsPrimaryKey then singlePk else ""}""")
            |> String.concat ","

        $"""
CREATE TABLE {newTableName} ({columns}{compositePk}
);
    """

    let private addSchemaCreationScript schemaName script =
        $"CREATE SCHEMA IF NOT EXISTS {schemaName};\n{script}"

    let makeSchemaScript (dbReflectionData: DbReflectionData) =
        let { TablesInfo = tablesInfo
              TableMappings = tableMappings
              TypeMappings = typeMappings
              TargetSchema = targetSchema } =
            dbReflectionData

        let tableMap = tableMappings |> List.map (fun m -> m.Old, m) |> Map.ofList

        tablesInfo
        |> Seq.fold (fun acc table -> acc + (formatColumnList table typeMappings tableMap targetSchema)) ""
        |> addSchemaCreationScript targetSchema
