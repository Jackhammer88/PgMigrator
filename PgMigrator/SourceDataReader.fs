namespace PgMigrator

open System.Data

module SourceDataReader =
    let readSourceRecordValue (reader: IDataReader) number =
        match reader.IsDBNull(number) with
        | true -> None
        | false ->
            let value: obj = reader.GetValue(number)
            Some(value)
