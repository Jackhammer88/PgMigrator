module DbTypeMapperGetNpgsqlDbTypeTests

open System.Text.RegularExpressions
open NpgsqlTypes
open PgMigrator.Mapping
open Xunit

let mappings = DbTypeDefaultMappings.mssqlToPgsql
    
let extractBaseType originalType =
    let targetTypeName = DbTypeMapper.getTargetTypeName originalType mappings
    let baseType = Regex.Replace(targetTypeName, @"\s*\(.*?\)", "").Trim()
    DbTypeMapper.getNpgsqlDbType baseType

[<Fact>]
let BinarySingleTest () =
    let result = extractBaseType "binary"
    Assert.Equal(NpgsqlDbType.Bytea, result)

[<Fact>]
let BigIntTest () =
    let result = extractBaseType "bigint"
    Assert.Equal(NpgsqlDbType.Bigint, result)

[<Fact>]
let TimestampTest () =
    let result = extractBaseType "timestamp"
    Assert.Equal(NpgsqlDbType.Bytea, result)

[<Fact>]
let CharTest () =
    let result = extractBaseType "char"
    Assert.Equal(NpgsqlDbType.Char, result)

[<Fact>]
let DateTest () =
    let result = extractBaseType "date"
    Assert.Equal(NpgsqlDbType.Date, result)

[<Fact>]
let DoublePrecisionTest () =
    let result = extractBaseType "float"
    Assert.Equal(NpgsqlDbType.Double, result)

[<Fact>]
let IntTest () =
    let result = extractBaseType "int"
    Assert.Equal(NpgsqlDbType.Integer, result)

[<Fact>]
let IntegerTest () =
    let result = extractBaseType "integer"
    Assert.Equal(NpgsqlDbType.Integer, result)

[<Fact>]
let MoneyTest () =
    let result = extractBaseType "money"
    Assert.Equal(NpgsqlDbType.Numeric, result)

[<Fact>]
let SmallmoneyTest () =
    let result = extractBaseType "smallmoney"
    Assert.Equal(NpgsqlDbType.Numeric, result)

[<Fact>]
let NumericTest () =
    let result = extractBaseType "numeric"
    Assert.Equal(NpgsqlDbType.Numeric, result)

[<Fact>]
let RealTest () =
    let result = extractBaseType "real"
    Assert.Equal(NpgsqlDbType.Real, result)

[<Fact>]
let TinyintTest () =
    let result = extractBaseType "tinyint"
    Assert.Equal(NpgsqlDbType.Smallint, result)

[<Fact>]
let TextTest () =
    let result = extractBaseType "text"
    Assert.Equal(NpgsqlDbType.Text, result)

[<Fact>]
let VarcharTest () =
    let result = extractBaseType "varchar"
    Assert.Equal(NpgsqlDbType.Varchar, result)

[<Fact>]
let BitTest () =
    let result = extractBaseType "bit"
    Assert.Equal(NpgsqlDbType.Boolean, result)