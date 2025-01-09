module DbTypeMapperTests

open PgMigrator.Mapping
open Xunit

let mappings = DbTypeDefaultMappings.mssqlToPgsql

[<Fact>]
let BinarySingleTest () =
    let result = DbTypeMapper.getTargetTypeName "binary" mappings
    Assert.Equal("bytea", result)

[<Fact>]
let BinaryTest () =
    let result = DbTypeMapper.getTargetTypeName "binary(20)" mappings
    Assert.Equal("bytea", result)

[<Fact>]
let VarbinaryTest () =
    let result = DbTypeMapper.getTargetTypeName "varbinary(10)" mappings
    Assert.Equal("bytea", result)

[<Fact>]
let VarbinaryMaxTest () =
    let result = DbTypeMapper.getTargetTypeName "varbinary(max)" mappings
    Assert.Equal("bytea", result)

[<Fact>]
let NcharSingleTest () =
    let result = DbTypeMapper.getTargetTypeName "nchar" mappings
    Assert.Equal("char", result)

[<Fact>]
let NcharTest () =
    let result = DbTypeMapper.getTargetTypeName "nchar(10)" mappings
    Assert.Equal("char(10)", result)

[<Fact>]
let CharTest () =
    let result = DbTypeMapper.getTargetTypeName "char(10)" mappings
    Assert.Equal("char(10)", result)

[<Fact>]
let CharSingleTest () =
    let result = DbTypeMapper.getTargetTypeName "char" mappings
    Assert.Equal("char", result)

[<Fact>]
let TextTest () =
    let result = DbTypeMapper.getTargetTypeName "text" mappings
    Assert.Equal("text", result)

[<Fact>]
let VarcharTest () =
    let result = DbTypeMapper.getTargetTypeName "varchar(10)" mappings
    Assert.Equal("varchar(10)", result)

[<Fact>]
let VarcharMaxTest () =
    let result = DbTypeMapper.getTargetTypeName "varchar(max)" mappings
    Assert.Equal("text", result)

[<Fact>]
let TinyintTest () =
    let result = DbTypeMapper.getTargetTypeName "tinyint" mappings
    Assert.Equal("smallint", result)

[<Fact>]
let NvarcharTest () =
    let result = DbTypeMapper.getTargetTypeName "nvarchar(10)" mappings
    Assert.Equal("varchar(10)", result)

[<Fact>]
let NvarcharMaxTest () =
    let result = DbTypeMapper.getTargetTypeName "nvarchar(max)" mappings
    Assert.Equal("text", result)

[<Fact>]
let ImageTest () =
    let result = DbTypeMapper.getTargetTypeName "image" mappings
    Assert.Equal("bytea", result)

[<Fact>]
let SmalldatetimeTest () =
    let result = DbTypeMapper.getTargetTypeName "smalldatetime" mappings
    Assert.Equal("timestamp", result)

[<Fact>]
let DatetimeTest () =
    let result = DbTypeMapper.getTargetTypeName "datetime" mappings
    Assert.Equal("timestamp", result)

[<Fact>]
let SmallmoneyTest () =
    let result = DbTypeMapper.getTargetTypeName "smallmoney" mappings
    Assert.Equal("numeric(10,4)", result)

[<Fact>]
let MoneyTest () =
    let result = DbTypeMapper.getTargetTypeName "money" mappings
    Assert.Equal("numeric(19,4)", result)

[<Fact>]
let DecimalTest () =
    let result = DbTypeMapper.getTargetTypeName "decimal(19,4)" mappings
    Assert.Equal("numeric(19,4)", result)

[<Fact>]
let FloatTest () =
    let result = DbTypeMapper.getTargetTypeName "float" mappings
    Assert.Equal("double precision", result)

[<Fact>]
let NumericTest () =
    let result = DbTypeMapper.getTargetTypeName "numeric(19,4)" mappings
    Assert.Equal("numeric(19,4)", result)

[<Fact>]
let UuidTest () =
    let result = DbTypeMapper.getTargetTypeName "uniqueidentifier" mappings
    Assert.Equal("uuid", result)

[<Fact>]
let DatetimeoffsetTest () =
    let result = DbTypeMapper.getTargetTypeName "datetimeoffset" mappings
    Assert.Equal("timestamptz", result)

[<Fact>]
let datetime2Test () =
    let result = DbTypeMapper.getTargetTypeName "datetime2" mappings
    Assert.Equal("timestamp", result)

[<Fact>]
let RowversionTest () =
    let result = DbTypeMapper.getTargetTypeName "rowversion" mappings
    Assert.Equal("bytea", result)

[<Fact>]
let XmlTest () =
    let result = DbTypeMapper.getTargetTypeName "xml" mappings
    Assert.Equal("xml", result)

[<Fact>]
let RealTest () =
    let result = DbTypeMapper.getTargetTypeName "real" mappings
    Assert.Equal("real", result)

[<Fact>]
let GeographyTest () =
    let result = DbTypeMapper.getTargetTypeName "geography" mappings
    Assert.Equal("geography", result)

[<Fact>]
let GeometryTest () =
    let result = DbTypeMapper.getTargetTypeName "geometry" mappings
    Assert.Equal("geometry", result)
    
[<Fact>]
let NoMappingTest () =
    let result = DbTypeMapper.getTargetTypeName "unknownType(10)" mappings
    Assert.Equal("unknownType(10)", result)