namespace PgMigrator

open System.Data

module SourceDataReader =
    let private readMssqlRecordValue (reader: IDataReader) number typeName =
        if reader.IsDBNull(number) then
            None
        else
            let value : obj =
                reader.GetValue(number)
                // match typeName with
                // | "bit" -> reader.GetBoolean(number)
                // | "datetime" -> reader.GetDateTime(number)
                // | "int" -> reader.GetInt32(number)
                // | "smallint" -> reader.GetInt16(number)
                // | "tinyint" -> reader.GetByte(number)
                // | "char" -> reader.GetChar(number)
                // | t when t = "varchar" || t = "nvarchar" -> reader.GetString(number)
                // | n -> failwithf $"Unknown type: '{n}'"
            Some(value)
    
    let private readPgSqlRecordValue (reader: IDataReader) number typeName =
        //let clearTypeName = Regex.Replace(typeName, @"\((\d+)\)", "")

        if reader.IsDBNull(number) then
            None
        else
            try
                let value : obj =
                    reader.GetValue(number)
                    // match clearTypeName with
                    // | "bigint" -> reader.GetInt64(number)
                    // | "bytea" -> reader.GetValue(number)
                    // | "money" -> reader.GetDecimal(number)
                    // | "bit" -> reader.GetBoolean(number)
                    // | t when t = "datetime" || t = "timestamp without time zone" -> reader.GetDateTime(number)
                    // | t when t = "int" || t = "integer" -> reader.GetInt32(number)
                    // | "smallint" -> reader.GetInt16(number)
                    // | "tinyint" -> reader.GetByte(number)
                    // | "character" -> reader.GetString(number)
                    // | "boolean" -> reader.GetBoolean(number)
                    // | t when t = "varchar" || t = "nvarchar" || t = "character varying"
                    //     || t = "text" -> reader.GetString(number)
                    // | n -> failwithf $"Unknown source type: '{n}'"
                
                Some(value)
            with
            | ex -> failwithf $"Error: number = {number}; typename = {typeName}. Error desc: {ex}"
            
    let readSourceRecordValue sourceType (reader: IDataReader) number typeName =
        match sourceType with
        | SourceTypes.mssql -> readMssqlRecordValue reader number typeName
        | SourceTypes.postgres -> readPgSqlRecordValue reader number typeName
        | s -> failwithf $"Unkwnown source type: '{s}'"

