namespace PgMigrator.Types

[<CLIMutable>]
type ColumnInfo =
    { TableName: string
      ColumnName: string
      DataType: string
      IsNullable: bool
      IsPrimaryKey: bool }