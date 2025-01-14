namespace PgMigrator.Types

open YamlDotNet.Serialization

[<CLIMutable>]
type TableMapping = {
    [<YamlMember(Alias = "Old")>]
    Old: string
    
    [<YamlMember(Alias = "New")>]
    New: string
}

[<CLIMutable>]
type TypeInfo = {
    [<YamlMember(Alias = "Type")>]
    Type: string
    
    [<YamlMember(Alias = "Param")>]
    Param: Option<string>
}

[<CLIMutable>]
type TypeMapping = {
    [<YamlMember(Alias = "Old")>]
    Old: TypeInfo
    
    [<YamlMember(Alias = "New")>]
    New: TypeInfo
    
    [<YamlMember(Alias = "TransferParam")>]
    TransferParam: bool
}

[<CLIMutable>]
type MigrationConfig = {
      [<YamlMember(Alias = "SourceCs")>]
      SourceCs: string
      
      [<YamlMember(Alias = "TargetCs")>]
      TargetCs: string
      
      [<YamlMember(Alias = "SourceSchema")>]
      SourceSchema: string option
      
      [<YamlMember(Alias = "TargetSchema")>]
      TargetSchema: string option
      
      [<YamlMember(Alias = "BatchSize")>]
      BatchSize: int option
      
      [<YamlMember(Alias = "RemoveNullBytes")>]
      RemoveNullBytes: bool option
      
      [<YamlMember(Alias = "SourceType")>]
      SourceTypeName: string
      
      [<YamlMember(Alias = "Tables")>]
      Tables: string list
      
      [<YamlMember(Alias = "TypeMappings")>]
      TypeMappings: TypeMapping list
      
      [<YamlMember(Alias = "TableMappings")>]
      TableMappings: TableMapping list
    } with
        member internal this.getSourceType =
            match this.SourceTypeName with
            | SourceTypes.mssql -> Mssql
            | SourceTypes.postgres -> Pgsql
            | s -> failwith $"Unknown source type {s}"