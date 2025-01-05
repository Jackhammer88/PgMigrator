module DbInfoTypes

type AllTablesInfo =
    { TableName: string
      ColumnName: string
      DataType: string
      IsNullable: bool
      IsPrimaryKey: bool }

type ColumnInfo =
    { ColumnName: string
      DataType: string
      IsNullable: bool
      IsPrimaryKey: bool }

type TableInfo =
    { TableName: string
      Columns: List<ColumnInfo>
      PkCount: int }