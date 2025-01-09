namespace PgMigrator

open System.Data

module TargetDbScriptRunner =
    let run (connection: IDbConnection) (transaction : IDbTransaction) script =
        use command = connection.CreateCommand()
        command.CommandText <- script
        command.Transaction <- transaction
        command.ExecuteNonQuery() |> ignore
        ()

