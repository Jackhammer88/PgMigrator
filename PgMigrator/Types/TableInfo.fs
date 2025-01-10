namespace PgMigrator.Types

type TableInfo =
    { TableName: string
      Columns: List<ColumnInfo>
      PkCount: int }