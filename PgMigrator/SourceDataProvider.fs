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
        let targetTypeName = DbTypeMapper.getTargetTypeName typeName typeMappings

        let targetType = DbTypeMapper.getNpgsqlDbType targetTypeName
        (columnNumber, columnName, typeName, targetType)

    let private writeRowDataAsync (writer: NpgsqlBinaryImporter) columnNames sourceReader needSanitizeStrings =
        task {
            do! writer.StartRowAsync()

            for (n, _, _, t) in columnNames do
                let value =
                    match SourceDataReader.readSourceRecordValue sourceReader n with
                    | Some v ->
                        match v with
                        | :? string as str when
                            needSanitizeStrings
                            && not (System.String.IsNullOrEmpty str)
                            && t = NpgsqlDbType.Varchar
                            ->
                            str.Replace("\u0000", "") :> obj
                        | _ -> v
                    | None -> null

                do! writer.WriteAsync(value, t)
        }

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

    let migrateTableAsync
        (srcCon: IDbConnection)
        (targetCon: NpgsqlConnection)
        (config: MigrationConfig)
        tableName
        (typeMappings: Map<string, TypeMapping>)
        =
        async {
            try
                printfn $"Migrating table: %s{tableName}"

                let tableMappingsSet =
                    config.TableMappings
                    |> List.map (fun mapping -> mapping.Old, mapping) // Создаем пары (Old, TableMapping)
                    |> Map.ofList

                let needRemoveNullBytes = config.RemoveNullBytes |> Option.defaultValue false

                let escapedTableName =
                    tableName
                    |> escapeTableName config.SourceType
                    |> addSchemaName config.SourceSchema

                // Выполним SELECT * из исходной БД
                use selectCommand = srcCon.CreateCommand()

                selectCommand.CommandText <- $"SELECT * FROM {escapedTableName}"
                use sourceReader = selectCommand.ExecuteReader()

                // Получаем имена и типы столбцов исходной таблицы
                let columnsCount = sourceReader.FieldCount

                let columnNames =
                    [ for i in 0 .. columnsCount - 1 do
                          let columnName = sourceReader.GetName(i)
                          let columnTypeName = sourceReader.GetDataTypeName(i)
                          yield mapNameToPgType i columnName columnTypeName typeMappings ]

                // Если требуется, преобразуем имя целевой схемы
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
                    targetCon.BeginBinaryImport(
                        $"COPY {targetTableName} ({columnNamesString}) FROM STDIN (FORMAT BINARY)")

                // Записываем все строки
                while sourceReader.Read() && sourceReader.FieldCount > 0 do
                    do! writeRowDataAsync writer columnNames sourceReader needRemoveNullBytes
                        |> Async.AwaitTask

                do! writer.CompleteAsync().AsTask() |> Async.AwaitTask |> Async.Ignore

                return Ok()
            with ex ->
                return Error ex.Message
        }

    let migrateAllTablesAsync tables connectionsInfo config typeMappings =
        tables
        |> List.map (fun t ->
            migrateTableAsync connectionsInfo.Source connectionsInfo.Target config t.TableName typeMappings)
        |> Async.Sequential