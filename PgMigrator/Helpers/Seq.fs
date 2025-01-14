namespace PgMigrator.Helpers

module Seq =
    let internal iterAsync (f: 'T -> Async<unit>) (source: seq<'T>) =
        async { for item in source do do! f item }
