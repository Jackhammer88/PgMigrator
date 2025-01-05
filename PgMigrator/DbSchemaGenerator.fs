namespace PgMigrator

open DbInfoTypes

module DbSchemaGenerator =
    let private formatColumnList
        (tableInfo : TableInfo) (typeMap: Map<string, string>) (tableMap: Map<string, string>) =
        let newTableName =
            match tableMap.TryFind tableInfo.TableName with
            | Some newName -> newName
            | None -> tableInfo.TableName
            
        let compositePk =
            if tableInfo.PkCount > 1 then
                    let pkColumns = 
                        tableInfo.Columns
                        |> Seq.filter (_.IsPrimaryKey)
                        |> Seq.map (_.ColumnName)
                        |> String.concat ", "
                    $",\n    CONSTRAINT %s{tableInfo.TableName}_pk PRIMARY KEY (%s{pkColumns})"
                else ""
        let singlePk = if tableInfo.PkCount = 1 then " PRIMARY KEY" else ""
        let columns =
            tableInfo.Columns
            |> Seq.map (fun i ->
                let newTypeName =
                    match typeMap.TryFind i.DataType
                    with
                    | Some newType -> newType
                    | None -> i.DataType
                    
                $"""
    %s{i.ColumnName} %s{newTypeName} %s{if i.IsNullable then "NULL" else "NOT NULL"}%s{if i.IsPrimaryKey then singlePk else ""}"""
            )
            |> String.concat ","

        $"""
CREATE TABLE public.{newTableName} ({columns}{compositePk}
);
    """
    
    let generatePgSchema (tables: List<TableInfo>) (config: MigrationConfig) : string =
        let tableMap =
            config.TableMappings
            |> Seq.map (fun m -> m.Old, m.New) |> Map.ofSeq
            
        let typeMap =
            config.TypeMappings
            |> Seq.map (fun m -> m.Old, m.New) |> Map.ofSeq
        
        tables
        |> Seq.fold (fun acc table -> acc + (formatColumnList table typeMap tableMap)) ""
