namespace PgMigrator

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
        | "bytea" -> NpgsqlDbType.Bytea
        | t when t = "varchar" || t = "character varying" -> NpgsqlDbType.Varchar
        | t when t = "int" || t = "integer" -> NpgsqlDbType.Integer
        | "timestamp without time zone" -> NpgsqlDbType.Timestamp
        | "text" -> NpgsqlDbType.Text
        | "timestamp" -> NpgsqlDbType.Timestamp
        | "character" -> NpgsqlDbType.Char
        | "smallint" -> NpgsqlDbType.Smallint
        | "char" -> NpgsqlDbType.Char
        | t -> failwith $"Unknown type: '{t}'"
    
    let convertType sourceType typeMappingsSet =
        DbSchemaGenerator.resolveType typeMappingsSet sourceType
        |> mapToNpgSqlDbType