namespace PgMigrator

open System.Data
open Microsoft.Data.SqlClient
open Npgsql
open NpgsqlTypes
open PgMigrator.Config
open PgMigrator.Mapping

module SourceDataProvider =
    let getSourceConnection (cs: string) (sourceType: string) : IDbConnection =
        match sourceType with
        | SourceTypes.postgres -> new NpgsqlConnection(cs)
        | SourceTypes.mssql -> new SqlConnection(cs)
        | _ -> failwith "Unknown source type"

    let private mapNameToPgType columnNumber columnName typeName typeMappings =
        let targetTypeName =
            DbTypeMapper.getTargetTypeName typeName typeMappings

        let targetType = DbTypeMapper.getNpgsqlDbType targetTypeName
        (columnNumber, columnName, typeName, targetType)

    let private writeRowData (writer: NpgsqlBinaryImporter) columnNames sourceReader needSanitizeStrings =
        writer.StartRow()

        for c in columnNames do
            let (number, _, _, targetType: NpgsqlDbType) = c

            let columnValue =
                match SourceDataReader.readSourceRecordValue sourceReader number with
                | Some v -> v
                | None -> null

            let sanitizedValue =
                match columnValue with
                | :? string as str when
                    needSanitizeStrings
                    && not (System.String.IsNullOrEmpty str)
                    && targetType = NpgsqlDbType.Varchar
                    ->
                    str.Replace("\u0000", "") :> obj
                | _ -> columnValue

            writer.Write(sanitizedValue, targetType)


    let escapeTableName sourceType tableName =
        match sourceType with
        | SourceTypes.mssql -> $"[{tableName}]"
        | SourceTypes.postgres -> $"\"{tableName}\""
        | s -> failwithf $"Unknown source type {s}"


    let escapeColumnName (columnName: string) =
        match columnName.ToLowerInvariant() with
        | s when s = "date" || s = "user" || s = "order" -> $"\"{s}\""
        | _ -> columnName

    let addSchemaName schema tableName =
        match schema with
        | Some s -> $"{s}.{tableName}"
        | None -> tableName

    let migrateTable
        (srcCon: IDbConnection)
        (targetCon: NpgsqlConnection)
        (config: MigrationConfig)
        tableName
        (typeMappings: Map<string, TypeMapping>)
        =
        let tableMappingsSet =
            config.TableMappings
            |> List.map (fun mapping -> mapping.Old, mapping) // Создаем пары (Old, TableMapping)
            |> Map.ofList

        // Выполним SELECT * из исходной БД
        use selectCommand = srcCon.CreateCommand()

        let escapedTableName =
            tableName
            |> escapeTableName config.SourceType
            |> addSchemaName config.SourceSchema

        selectCommand.CommandText <- $"SELECT * FROM {escapedTableName}"
        use sourceReader = selectCommand.ExecuteReader()

        // Если записи существуют, то мигрируем
        if sourceReader.FieldCount > 0 then
            // Получаем имена и типы столбцов исходной таблицы
            let columnsCount = sourceReader.FieldCount

            let columnNames =
                [ for i in 0 .. columnsCount - 1 do
                      let columnName = sourceReader.GetName(i)
                      let columnTypeName = sourceReader.GetDataTypeName(i)
                      yield mapNameToPgType i columnName columnTypeName typeMappings ]

            // Если требуется, преобразуем имя целевой таблицы
            let schema = config.TargetSchema |> Option.defaultValue "public"

            let targetTableName =
                match tableMappingsSet.TryGetValue tableName with
                | s, m when s -> $"{schema}.{m.New}"
                | _ -> $"{schema}.{tableName}"

            // Форматируем строку с именами столбцов для вставки
            let columnNamesString =
                columnNames
                |> List.map (fun (_, name, _, _) -> escapeColumnName name)
                |> String.concat ", "

            use writer =
                targetCon.BeginBinaryImport($"COPY {targetTableName} ({columnNamesString}) FROM STDIN (FORMAT BINARY)")

            let needRemoveNullBytes = config.RemoveNullBytes |> Option.defaultValue false

            // Записываем оставшиеся строки
            while sourceReader.Read() do
                writeRowData writer columnNames sourceReader needRemoveNullBytes

            writer.Complete() |> ignore
            ()
        else
            ()
