namespace PgMigrator.Config

open System
open System.IO
open FsToolkit.ErrorHandling
open PgMigrator
open PgMigrator.Types
open YamlDotNet.Serialization

module ConfigManager =
    let private writeYaml (filename: string) (config: MigrationConfig) : Result<unit, string> =
        if File.Exists filename then
            failwithf $"File %s{filename} is already exists."

        try
            let serializer = SerializerBuilder().Build()

            let yaml = serializer.Serialize(config)
            use writer = new StreamWriter(filename)
            writer.Write(yaml)
            Ok()
        with ex ->
            Error $"Failed to write configuration to %s{filename}: %s{ex.Message}"

    let private tryReadYaml (filename: string) =
        try
            let deserializer =
                DeserializerBuilder().WithCaseInsensitivePropertyMatching().Build()

            use reader = new StreamReader(filename)
            deserializer.Deserialize<MigrationConfig>(reader) |> Result.Ok
        with ex ->
            GlobalLogger.instance.logError "Can't read config" ex
            ex.Message |> Result.Error

    let private validateTypeMapping (mappings: TypeMapping list) =
        let hasEmptyTypes =
            mappings
            |> List.exists (fun m -> String.IsNullOrWhiteSpace(m.Old.Type) || String.IsNullOrWhiteSpace(m.New.Type))

        if hasEmptyTypes then
            failwith "Type mappings must contain correct type names."

        ()

    let private validateTableMapping (mapping: TableMapping) =
        if String.IsNullOrEmpty(mapping.Old) || String.IsNullOrEmpty(mapping.New) then
            failwithf $"Mapping musn't contain empty fields: Old = '%O{mapping}'"

    let private validateConfig (config: MigrationConfig) =
        try
            // Проверим SourceType
            match config.SourceTypeName.ToLowerInvariant() with
            | SourceTypes.postgres
            | SourceTypes.mssql -> () // OK
            | _ -> failwithf $"Unsupported SourceType: '%s{config.SourceTypeName}'"

            // Проверим, что SourceCs и TargetCs не пустые
            if String.IsNullOrWhiteSpace config.SourceCs then
                failwith "SourceCs is empty"

            if String.IsNullOrWhiteSpace config.TargetCs then
                failwith "TargetCs is empty"

            // Проверим Mapping
            validateTypeMapping config.TypeMappings

            config.TableMappings |> List.iter validateTableMapping
            
            Ok ()
        with ex -> ex.Message |> Result.Error
        

    let tryReadConfig file =
        result {
            let! config = tryReadYaml file
            do! validateConfig config
            return config
        }

    let normalizeFolder folder =
        if folder = "~" || folder = "~/" then
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile)
        else
            folder

    let createDefaultConfig (folder: string) =
        let defaultConfig =
            { SourceCs = "Server=127.0.0.1;Database=yourBase;User Id=sa;Password=yourStrongPass;Encrypt=false;"
              TargetCs =
                "User ID=postgres;Password=yourStrongPass;Host=127.0.0.1;Port=5432;Database=yourBase;Pooling=true;Connection Lifetime=0;"
              SourceSchema = Some "dbo"
              TargetSchema = Some "public"
              RemoveNullBytes = Some true
              SourceTypeName = SourceTypes.mssql
              Tables = []
              TableMappings = [ { Old = "Table1"; New = "Table2" } ]
              TypeMappings =
                [ { Old = { Type = "varchar"; Param = Some "max" }
                    New = { Type = "text"; Param = None }
                    TransferParam = false } ] }

        let normalizedFolder = normalizeFolder folder

        if not (Directory.Exists(normalizedFolder)) then
            Directory.CreateDirectory(normalizedFolder) |> ignore

        let fullname = Path.Combine(normalizedFolder, "config.yaml")

        match writeYaml fullname defaultConfig with
        | Ok _ -> printfn $"Config file was written in %s{folder} successfully"
        | Error err -> printfn $"%s{err}"

        ()
