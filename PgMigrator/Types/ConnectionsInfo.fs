namespace PgMigrator.Types

open System
open System.Data
open Npgsql

type ConnectionsInfo =
    { Source: IDbConnection
      Target: NpgsqlConnection
      Transaction: NpgsqlTransaction }

    interface IDisposable with
        member this.Dispose() =
            try
                this.Transaction.Dispose()
                this.Target.Dispose()
                this.Source.Dispose()
            with ex ->
                Console.Error.WriteLine(ex)