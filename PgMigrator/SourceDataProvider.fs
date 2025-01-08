namespace PgMigrator

open System
open System.Data
open Microsoft.Data.SqlClient
open Npgsql
open NpgsqlTypes

module SourceDataProvider =
    let getSourceConnection (cs: string) (sourceType: string) : IDbConnection =
        match sourceType with
        | SourceTypes.postgres -> new NpgsqlConnection(cs)
        | SourceTypes.mssql -> new SqlConnection(cs)
        | _ -> failwith "Unknown source type"

    let migrateTable sourceCs targetCs sourceType tableName =
        use sourceConnection = getSourceConnection sourceCs sourceType
        use targetConnection = new NpgsqlConnection(targetCs) // целевая БД: Postgres
        sourceConnection.Open()
        targetConnection.Open()

        // Выполним SELECT * из исходной БД
        use selectCommand = sourceConnection.CreateCommand()
        selectCommand.CommandText <- $"SELECT * FROM {tableName}"

        // Считаем структуру — т.е. сколько колонок, какие имена
        use reader = selectCommand.ExecuteReader()

        // Для каждой строки будем формировать INSERT
        while reader.Read() do
            // 1) Собираем список (columnName, value)
            let columnCount = reader.FieldCount

            let columns =
                [ for i in 0 .. columnCount - 1 ->
                      let name = reader.GetName(i)
                      let value = reader.GetValue(i)
                      (name, value) ]

            // 2) Сформируем список имён колонок и имён параметров
            //    Пример: col1, col2, col3  и  @col1, @col2, @col3
            let columnNames = columns |> List.map fst |> String.concat ", "

            let paramNames =
                columns |> List.map (fun (colName, _) -> "@" + colName) |> String.concat ", "

            // 3) Сформируем текст INSERT
            let insertSql = $"INSERT INTO {tableName} ({columnNames}) VALUES ({paramNames})"

            // 4) Создадим команду для целевой БД
            use insertCommand = targetConnection.CreateCommand()
            insertCommand.CommandText <- insertSql

            // 5) Добавим параметры
            for colName, colValue in columns do
                let p = insertCommand.CreateParameter()
                p.ParameterName <- "@" + colName
                p.Value <- if isNull colValue then box DBNull.Value else colValue
                insertCommand.Parameters.Add(p) |> ignore

            // 6) Выполним вставку
            insertCommand.ExecuteNonQuery() |> ignore

        ()

    let private mapNameToPgType columnNumber columnName typeName typeMappingsSet =
        let targetType = TargetDataMapper.convertType typeName typeMappingsSet
        (columnNumber, columnName, typeName, targetType)

    let private writeRowData (writer : NpgsqlBinaryImporter) columnNames sourceReader sourceType =
        writer.StartRow()
        for c in columnNames do
            let (number, _, sourceTypeName, targetType : NpgsqlDbType) = c
            let columnValue =
                match SourceDataReader.readSourceRecordValue sourceType sourceReader number sourceTypeName
                with
                | Some v -> v
                | None -> null
                
            writer.Write(columnValue, targetType)

    
    let migrateTable' (config: MigrationConfig) tableName =
        let tableMappingsSet =
            config.TableMappings |> Seq.map (fun m -> m.Old, m.New) |> Map.ofSeq

        let typeMappingsSet =
            config.TypeMappings |> Seq.map (fun m -> m.Old, m.New) |> Map.ofSeq

        use sourceConnection = getSourceConnection config.SourceCs config.SourceType
        use targetConnection = new NpgsqlConnection(config.TargetCs) // целевая БД: Postgres
        sourceConnection.Open()
        targetConnection.Open()

        // Выполним SELECT * из исходной БД
        use selectCommand = sourceConnection.CreateCommand()
        selectCommand.CommandText <- $"SELECT * FROM {tableName}"
        use sourceReader = selectCommand.ExecuteReader()

        // Если записи существуют, то мигрируем
        if sourceReader.Read() && sourceReader.FieldCount > 0 then
            // Получаем имена и типы столбцов исходной таблицы
            let columnsCount = sourceReader.FieldCount
            let columnNames =
                [ for i in 0 .. columnsCount - 1 do
                      let columnName = sourceReader.GetName(i)
                      let columnTypeName = sourceReader.GetDataTypeName(i)
                      yield mapNameToPgType i columnName columnTypeName typeMappingsSet ]
            
            // Если требуется, преобразуем имя целевой таблицы
            let targetDbName =
                match tableMappingsSet.TryGetValue tableName with
                | s, nName when s -> nName
                | _ -> tableName

            // Форматируем строку с именами столбцов для вставки
            let columnNamesString =
                columnNames
                |> List.map (fun (_, name, _, _) -> name)
                |> String.concat ", "

            use writer =
                targetConnection.BeginBinaryImport(
                    $"COPY {targetDbName} ({columnNamesString}) FROM STDIN (FORMAT BINARY)")
            
            // Записываем первую строку
            writeRowData writer columnNames sourceReader config.SourceType

            // Записываем оставшиеся строки
            while sourceReader.Read() do
                writeRowData writer columnNames sourceReader config.SourceType

            writer.Complete() |> ignore
            ()
        else
            ()
