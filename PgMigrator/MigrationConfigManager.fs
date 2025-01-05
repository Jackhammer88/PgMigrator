namespace PgMigrator

open System
open System.Collections.Generic
open System.IO
open System.Text.RegularExpressions
open DbInfoTypes
open YamlDotNet.Serialization

type Mapping() =
    member val Old = "" with get, set
    member val New = "" with get, set

type MigrationConfig() =
    member val SourceCs = "" with get, set
    member val TargetCs = "" with get, set
    member val SourceType = "" with get, set

    member val Tables = List<string>() with get, set
    member val TypeMappings = List<Mapping>() with get, set
    member val TableMappings = List<Mapping>() with get, set

module MigrationConfigManager =
    let private writeYaml (filename: string) (config: MigrationConfig) : Result<unit, string> =
        if File.Exists filename then
            failwithf $"File %s{filename} is already exists."

        try
            let serializer = SerializerBuilder()
                                .Build()
            let yaml = serializer.Serialize(config)
            use writer = new StreamWriter(filename)
            writer.Write(yaml)
            Ok()
        with ex ->
            Error $"Failed to write configuration to %s{filename}: %s{ex.Message}"

    let private readYaml (filename: string) : MigrationConfig =
        let deserializer = DeserializerBuilder().Build()
        use reader = new StreamReader(filename)
        deserializer.Deserialize<MigrationConfig>(reader)

    let private extractBaseType (dataType: string) =
        let matchResult = Regex.Match(dataType, @"^[a-zA-Z]+")

        if matchResult.Success then
            matchResult.Value.ToLowerInvariant()
        else
            dataType.ToLowerInvariant()

    let private validateTypeMapping (m: Mapping) (sourceType: string) =
        let pgType = extractBaseType m.New

        if not (Set.contains pgType DbTypes.psqlTypes) then
            failwithf $"Type %s{m.New} is not correct pgsql type"

        let source = m.Old.ToLowerInvariant()

        match sourceType.ToLowerInvariant() with
        | SourceTypes.postgres ->
            if not (Set.contains source DbTypes.psqlTypes) then
                failwithf $"Type %s{source} is not correct pgsql type"
        | SourceTypes.mssql ->
            if not (Set.contains source DbTypes.mssqlTypes) then
                failwithf $"Type %s{source} is not correct mssql type"
        | _ -> failwith "Unknown source db type"

        ()

    let private validateTableMapping (m: Mapping) =
        if String.IsNullOrWhiteSpace m.Old || String.IsNullOrWhiteSpace m.New then
            failwithf $"Mapping contains empty fields: Old='%s{m.Old}' New='%s{m.New}'"

    let private validateConfig (config: MigrationConfig) =
        // Проверим SourceType
        match config.SourceType.ToLowerInvariant() with
        | SourceTypes.postgres
        | SourceTypes.mssql -> () // OK
        | _ -> failwithf $"Unsupported SourceType: '%s{config.SourceType}'"

        // Проверим, что SourceCs и TargetCs не пустые
        if String.IsNullOrWhiteSpace config.SourceCs then
            failwith "SourceCs is empty"

        if String.IsNullOrWhiteSpace config.TargetCs then
            failwith "TargetCs is empty"

        // Проверим Mapping
        config.TypeMappings
        |> Seq.iter (fun i -> validateTypeMapping i config.SourceType)

        config.TableMappings |> Seq.iter validateTableMapping

        // Всё ок
        config

    let readConfig (file: string) : MigrationConfig =
        let config = readYaml file
        validateConfig config |> ignore
        config
        
    let normalizeFolder folder =
        if folder = "~" || folder = "~/" then Environment.GetFolderPath(Environment.SpecialFolder.UserProfile)
        else folder

    let createDefaultConfig (folder: string) =
        let config = MigrationConfig()
        config.SourceCs <- "Server=127.0.0.1;Database=yourBase;User Id=sa;Password=yourStrongPass;Encrypt=false;"

        config.TargetCs <-
            "User ID=postgres;Password=yourStrongPass;Host=127.0.0.1;Port=5432;Database=yourBase;Pooling=true;Connection Lifetime=0;"

        config.SourceType <- SourceTypes.mssql
        
        let normalizedFolder = normalizeFolder folder
        if not (Directory.Exists(normalizedFolder)) then Directory.CreateDirectory(normalizedFolder) |> ignore

        let fullname = Path.Combine(normalizedFolder, "config.yaml")
        
        match writeYaml fullname config with
        | Ok _ -> printfn $"Config file was written in %s{folder} successfully"
        | Error err -> printfn $"%s{err}"
        ()
