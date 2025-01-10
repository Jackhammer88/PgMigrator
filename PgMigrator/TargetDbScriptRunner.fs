namespace PgMigrator

module TargetDbScriptRunner =
    let tryRun connectionsInfo script =
        try
            use command = connectionsInfo.Target.CreateCommand()
            command.CommandText <- script
            command.Transaction <- connectionsInfo.Transaction
            command.ExecuteNonQuery() |> ignore
            Ok ()
        with
        | ex ->
            System.Console.Error.WriteLine(ex)
            Error ex.Message

