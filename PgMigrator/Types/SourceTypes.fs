namespace PgMigrator.Types

type SourceType =
    | Mssql
    | Pgsql

module SourceTypes =

    [<Literal>]
    let postgres = "postgresql"

    [<Literal>]
    let mssql = "mssql"