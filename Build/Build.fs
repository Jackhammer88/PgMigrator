namespace Build

open System.IO
open Fake.Core
open Fake.DotNet
open Fake.IO.FileSystemOperators
open Fake.Core.TargetOperators

module Program =
    [<Literal>]
    let appName = "PgMigrator"
    let rootDirectory = Path.GetFullPath(__SOURCE_DIRECTORY__ </> "..")
    let sln = rootDirectory </> $"{appName}.sln"
    let rootProject = rootDirectory </> appName </> $"{appName}.fsproj"
    let outputDirectory = rootDirectory </> "Artifacts"
    let configuration = DotNet.BuildConfiguration.Release

    let initTargets () =
        Target.create "Clean" (fun _ ->
            if Directory.Exists outputDirectory then
                Directory.Delete(outputDirectory, true)

            DotNet.exec id "clean" sln |> ignore)

        Target.create "Restore" (fun _ -> DotNet.restore id sln)

        Target.create "Build" (fun _ ->
            DotNet.build
                (fun args ->
                    { args with
                        Configuration = configuration
                        NoRestore = true
                        NoLogo = true })
                sln)

        Target.create "Test" (fun _ ->
            DotNet.test
                (fun args ->
                    { args with
                        Configuration = configuration
                        NoRestore = true
                        NoLogo = true })
                sln)

        let msbuildArgs =
            MSBuild.CliArguments.Create()
            |> fun args ->
                { args with
                    Properties = [ "PublishReadyToRun", "true" ] }


        Target.create "Publish-win-x64" (fun _ ->
            DotNet.publish
                (fun args ->
                    let targetOutputDir = outputDirectory </> $"{appName}-win-x64"

                    { args with
                        MSBuildParams = msbuildArgs
                        Configuration = configuration
                        SelfContained = true |> Option.Some
                        NoLogo = true
                        Runtime = "win-x64" |> Option.Some
                        OutputPath = Some targetOutputDir })
                rootProject)

        Target.create "Publish-linux-x64" (fun _ ->
            DotNet.publish
                (fun args ->
                    let targetOutputDir = outputDirectory </> $"{appName}-linux-x64"

                    { args with
                        MSBuildParams = msbuildArgs
                        Configuration = configuration
                        SelfContained = true |> Option.Some
                        NoLogo = true
                        Runtime = "linux-x64" |> Option.Some
                        OutputPath = Some targetOutputDir })
                rootProject)

        Target.create "All" (fun _ -> Trace.log "All")

        ("Clean" ==> "Restore" ==> "Build" ==> "Test" ==> "Publish-win-x64" ==> "All")
        |> ignore

        "Clean" ==> "Restore" ==> "Build" ==> "Test" ==> "Publish-linux-x64" ==> "All"

    [<EntryPoint>]
    let main argv =
        try
            argv
            |> Array.toList
            |> Context.FakeExecutionContext.Create false "build.fsx"
            |> Context.RuntimeContext.Fake
            |> Context.setExecutionContext

            initTargets () |> ignore
            Target.runOrDefaultWithArguments "All"
            0
        with ex ->
            printfn $"Error: {ex.Message}"
            1
