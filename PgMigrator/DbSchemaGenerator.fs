namespace PgMigrator

module DbSchemaGenerator =
    let resolveType (typeMap : Map<string, string>) (originalType : string) =
        // 1) Проверяем, нет ли у исходного типа круглых скобок
        let idx = originalType.IndexOf("(")
        let maxIdx = originalType.ToLowerInvariant().IndexOf("max")
        if idx = -1 || maxIdx <> -1 then
            // Нет скобок -> тип без параметров, например, "nvarchar"
            // Ищем в словаре
            match typeMap.TryFind originalType with
            | Some newType -> newType
            | None -> originalType
        else
            // Есть скобки: например "nvarchar(50)"
            let baseType = originalType.Substring(0, idx) // "nvarchar"
            let parameters = originalType.Substring(idx)  // "(50)"
            // map "nvarchar" -> "varchar"
            match typeMap.TryFind baseType with
            | Some newType ->
                // Собираем новый тип + те же параметры -> "varchar(50)"
                newType + parameters
            | None ->
                // Не нашли замену, значит оставляем как есть
                originalType
                
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
                let newTypeName = resolveType typeMap i.DataType
                
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
