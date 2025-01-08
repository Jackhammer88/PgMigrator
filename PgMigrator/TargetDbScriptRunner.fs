namespace PgMigrator

open Npgsql

module TargetDbScriptRunner =
    let runScript cs script =
        use targetConnection = new NpgsqlConnection(cs)
        targetConnection.Open()
        use command = targetConnection.CreateCommand()
        command.CommandText <- script
        command.ExecuteNonQuery() |> ignore
        ()

