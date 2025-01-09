namespace PgMigrator.Mapping

open PgMigrator.Config

module DbTypeDefaultMappings =
    let mssqlToPgsql =
      [
        "varbinary",
        { Old = { Type = "varbinary"; Param = None }
          New = { Type = "bytea"; Param = None }
          TransferParam = false }

        "bit",
        { Old = { Type = "bit"; Param = None }
          New = { Type = "boolean"; Param = None }
          TransferParam = false }
        
        "binary",
        { Old = { Type = "binary"; Param = None }
          New = { Type = "bytea"; Param = None }
          TransferParam = false }

        "nchar",
        { Old = { Type = "nchar"; Param = None }
          New = { Type = "char"; Param = None }
          TransferParam = true }

        "varchar",
        { Old = { Type = "varchar"; Param = None }
          New = { Type = "varchar"; Param = None }
          TransferParam = true }

        "varchar(max)",
        { Old = { Type = "varchar"; Param = Some "max" }
          New = { Type = "text"; Param = None }
          TransferParam = false }

        "nvarchar",
        { Old = { Type = "nvarchar"; Param = None }
          New = { Type = "varchar"; Param = None }
          TransferParam = true }

        "nvarchar(max)",
        { Old = { Type = "nvarchar"; Param = Some "max" }
          New = { Type = "text"; Param = None }
          TransferParam = false }

        "tinyint",
        { Old = { Type = "tinyint"; Param = None }
          New = { Type = "smallint"; Param = None }
          TransferParam = false }

        "image",
        { Old = { Type = "image"; Param = None }
          New = { Type = "bytea"; Param = None }
          TransferParam = false }

        "smalldatetime",
        { Old = { Type = "smalldatetime"; Param = None }
          New = { Type = "timestamp"; Param = None }
          TransferParam = false }

        "datetime",
        { Old = { Type = "datetime"; Param = None }
          New = { Type = "timestamp"; Param = None }
          TransferParam = false }

        "timestamp",
        { Old = { Type = "timestamp"; Param = None }
          New = { Type = "bytea"; Param = None }
          TransferParam = false }

        "smallmoney",
        { Old = { Type = "smallmoney"; Param = None }
          New =
            { Type = "numeric"
              Param = Some "10,4" }
          TransferParam = false }

        "decimal",
        { Old = { Type = "decimal"; Param = None }
          New = { Type = "numeric"; Param = None }
          TransferParam = true }

        "float",
        { Old = { Type = "float"; Param = None }
          New = { Type = "double precision"; Param = None }
          TransferParam = false }
        
        "money",
        { Old = { Type = "money"; Param = None }
          New =
            { Type = "numeric"
              Param = Some "19,4" }
          TransferParam = false }

        "uniqueidentifier",
        { Old =
            { Type = "uniqueidentifier"
              Param = None }
          New = { Type = "uuid"; Param = None }
          TransferParam = false }

        "datetimeoffset",
        { Old =
            { Type = "datetimeoffset"
              Param = None }
          New = { Type = "timestamptz"; Param = None }
          TransferParam = false }

        "datetime2",
        { Old = { Type = "datetime2"; Param = None }
          New = { Type = "timestamp"; Param = None }
          TransferParam = false }

        "rowversion",
        { Old = { Type = "rowversion"; Param = None }
          New = { Type = "bytea"; Param = None }
          TransferParam = false } ]
      |> Map.ofList