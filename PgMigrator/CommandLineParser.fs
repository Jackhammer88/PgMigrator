namespace PgMigrator

open System
open System.IO
open PgMigrator.Config

module CommandLineParser =
    let private printHelp () =
        printfn
            """
PgMigrator Usage:
  pgmigrator [config.yaml] [options]

Options:
  --help, -h              Show this help message.
  --generate-config, -g <file>   Generate a default configuration file at the specified folder path.
"""

    let getConfigPath (args: string[]) : string =
        match args |> Array.toList with
        | configFile :: _ when File.Exists configFile -> configFile
        | _ -> "config.yaml"

    let processCliCommands (args: string[]) =
        match args |> Array.toList with
        | [] -> ()
        | [ "--generate-config"; folder ] ->
            ConfigManager.createDefaultConfig folder
            Environment.Exit(0)
        | [ "-g"; folder ] ->
            ConfigManager.createDefaultConfig folder
            Environment.Exit(0)
        | [ "--help" ]
        | [ "-h" ] ->
            printHelp ()
            Environment.Exit(0)
        | [ "--generate-config" ]
        | [ "-g" ] ->
            ConfigManager.createDefaultConfig ""
            printHelp ()
            Environment.Exit(0)
        | _ -> ()
