module DbInfoTypes

type ColumnInfo() =
    member val TableName: string = "" with get, set
    member val ColumnName: string = "" with get, set
    member val DataType: string = "" with get, set
    member val IsNullable: bool = false with get, set
    member val IsPrimaryKey: bool = false with get, set

type TableInfo =
    { TableName: string
      Columns: List<ColumnInfo>
      PkCount: int }
    
module SourceTypes =
    
    [<Literal>]
    let postgres = "postgresql"
    
    [<Literal>]
    let mssql = "mssql"