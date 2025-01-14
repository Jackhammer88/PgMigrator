namespace PgMigrator.Config

open PgMigrator.Types

type MigrationFlowData = {
    Tables : string list
    TargetSchema : string
    TableMappings : TableMapping list
    TypeMappings : Map<string, TypeMapping>
    TablesInfo : TableInfo list
    RemoveNullBytes : bool
}

type DbReflectionData = {
    TablesInfo : TableInfo list
    TableMappings : TableMapping list
    TypeMappings : Map<string, TypeMapping>
    TargetSchema : string
}

type ConnectionsInfo = {
    TargetCs : string
    SourceCs : string
    SourceType : SourceType
    SourceSchema : string option
    TargetSchema : string option
}
    

