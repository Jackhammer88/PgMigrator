namespace PgMigrator.Mapping

open System.Text.RegularExpressions
open NpgsqlTypes

module TargetDataMapper =
    let private mapToNpgSqlDbType typeName =
        let cleanTypeName = 
            Regex.Replace(typeName, @"\s*\(.*\)", "") // Удаляет все символы от '(' до конца строки

        match cleanTypeName with
        | "bigint" -> NpgsqlDbType.Bigint
        | "boolean" -> NpgsqlDbType.Boolean
        | "money" -> NpgsqlDbType.Money
        | "float" -> NpgsqlDbType.Real
        | b when b = "bytea" || b = "image" -> NpgsqlDbType.Bytea
        | n when n = "decimal" || n = "smallmoney" -> NpgsqlDbType.Numeric
        | "bit" -> NpgsqlDbType.Boolean
        | t when t = "varchar" || t = "character varying" || t = "nvarchar" -> NpgsqlDbType.Varchar
        | t when t = "int" || t = "integer" -> NpgsqlDbType.Integer
        | "text" -> NpgsqlDbType.Text
        | d when d = "timestamp" || d = "timestamp without time zone" || d = "smalldatetime"
            || d = "datetime" ->
            NpgsqlDbType.Timestamp
        | "character" -> NpgsqlDbType.Char
        | "smallint" -> NpgsqlDbType.Smallint
        | "char" -> NpgsqlDbType.Char
        | t -> failwith $"Unknown type: '{t}'"
    
    // let convertType sourceType (typeMappingsSet : Map<string, TypeMapping>) =
    //     DbSchemaGenerator.resolveType typeMappingsSet sourceType
    //     |> mapToNpgSqlDbType