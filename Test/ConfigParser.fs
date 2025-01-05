namespace PgMigrator

open System
open System.Collections.Generic
open System.IO
open YamlDotNet.Serialization

type Mapping() =
    member val Old = "" with get, set
    member val New = "" with get, set

type MigrationConfig() =
    member val SourceCs = "" with get, set
    member val TargetCs = "" with get, set
    member val SourceType = "Postgresql" with get, set

    member val Tables = List<string>() with get, set
    member val TypeMappings = List<Mapping>() with get, set
    member val TableMappings = List<Mapping>() with get, set

module MigrationConfigManager =
    let private writeYaml (filename: string) (config: MigrationConfig) =
        let serializer = SerializerBuilder().Build()
        let yaml = serializer.Serialize(config)
        use writer = new StreamWriter(filename)
        writer.Write(yaml)

    let private readYaml (filename: string) : MigrationConfig =
        let deserializer = DeserializerBuilder().Build()
        use reader = new StreamReader(filename)
        deserializer.Deserialize<MigrationConfig>(reader)
    
    let private validateTypeMapping (m: Mapping) (sourceType: string) =
        let pgType = m.New.ToLowerInvariant()
        if not (Set.contains pgType DbTypes.psqlTypes) then failwithf $"Type %s{m.New} is not correct pgsql type"
        
        let source = m.Old.ToLowerInvariant()
        match sourceType.ToLowerInvariant() with 
        | "postgresql" -> if not (Set.contains source DbTypes.psqlTypes) then failwithf $"Type %s{source} is not correct pgsql type"
        | "mssql" -> if not (Set.contains source DbTypes.mssqlTypes) then failwithf $"Type %s{source} is not correct mssql type"
        | _ -> failwith "Unknown source db type"
        ()
        
    let private validateTableMapping (m: Mapping) =
        if String.IsNullOrWhiteSpace m.Old || String.IsNullOrWhiteSpace m.New then
            failwithf $"Mapping contains empty fields: Old='%s{m.Old}' New='%s{m.New}'"
    
    let private validateConfig (config: MigrationConfig) =
        // Проверим SourceType
        match config.SourceType.ToLowerInvariant() with
        | "postgresql" | "mssql" -> ()  // OK
        | _ -> failwithf $"Unsupported SourceType: '%s{config.SourceType}'"

        // Проверим, что SourceCs и TargetCs не пустые
        if String.IsNullOrWhiteSpace config.SourceCs then
            failwith "SourceCs is empty"
        if String.IsNullOrWhiteSpace config.TargetCs then
            failwith "TargetCs is empty"

        // Проверим Mapping
        config.TypeMappings |> Seq.iter (fun i -> validateTypeMapping i config.SourceType)
        config.TableMappings |> Seq.iter validateTableMapping
        
        // Если дошли сюда - значит всё ок
        config
    
    let readConfig (filename: string) : MigrationConfig =
        let config = readYaml filename
        validateConfig config |> ignore
        config