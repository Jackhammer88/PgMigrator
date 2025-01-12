namespace PgMigrator.Mapping

open System.Text.RegularExpressions
open NpgsqlTypes
open PgMigrator.Types

module DbTypeMapper =
    let private tryGetTypeOrInner sourceType (typeMappings: Map<string, TypeMapping>) =
        let baseOfType =
            Regex.Replace(sourceType, @"\s*\(.*?\)", "").Trim()

        let paramOpt =
            match Regex.Match(sourceType, @"\((.*?)\)") with
            | m when m.Success -> Some(m.Groups[1].Value.Trim())
            | _ -> None
        
        match typeMappings.TryGetValue baseOfType with
        | s,m when s && m.New.Param.IsSome -> $"{m.New.Type}({m.New.Param.Value})"
        | s,m when s && m.New.Param.IsNone ->
            if paramOpt.IsNone then m.New.Type
            else
                if m.TransferParam then $"{m.New.Type}({paramOpt.Value})"
                else m.New.Type
        | _ -> sourceType
    
    let private tryGetMaxTypeOrInner sourceType (typeMappings: Map<string, TypeMapping>) =
        match typeMappings.TryGetValue sourceType with
        | s,m when s && m.New.Param.IsSome -> Some $"{m.New.Type}({m.New.Param.Value})"
        | s,m when s && m.New.Param.IsNone -> Some m.New.Type
        | s,_ when not s -> Some (tryGetTypeOrInner sourceType typeMappings)
        | _ -> None
    
    let getTargetTypeName (sourceType: string)
                      (typeMappings: Map<string, TypeMapping>) =
        if not (sourceType.EndsWith("(max)")) then
            tryGetTypeOrInner sourceType typeMappings
        else
            let maxMappingResult = tryGetMaxTypeOrInner sourceType typeMappings
            match maxMappingResult with
            | Some m -> m
            | None -> sourceType
    
    let getNpgsqlDbType sourceType=
        let baseOfType = Regex.Replace(sourceType, @"\s*\(.*?\)", "").Trim()
        match baseOfType with
        | "array" -> NpgsqlDbType.Array
        | "bigint" -> NpgsqlDbType.Bigint
        | "boolean" -> NpgsqlDbType.Boolean
        | "box" -> NpgsqlDbType.Box
        | "bytea" -> NpgsqlDbType.Bytea
        | "circle" -> NpgsqlDbType.Circle
        | "char" | "character" -> NpgsqlDbType.Char
        | "date" -> NpgsqlDbType.Date
        | "double precision" -> NpgsqlDbType.Double
        | i when i = "integer" || i = "int" -> NpgsqlDbType.Integer
        | "line" -> NpgsqlDbType.Line
        | "lseg" -> NpgsqlDbType.LSeg
        | "money" -> NpgsqlDbType.Money
        | "numeric" -> NpgsqlDbType.Numeric
        | "path" -> NpgsqlDbType.Path
        | "point" -> NpgsqlDbType.Point
        | "polygon" -> NpgsqlDbType.Polygon
        | "real" -> NpgsqlDbType.Real
        | "smallint" -> NpgsqlDbType.Smallint
        | "text" -> NpgsqlDbType.Text
        | "time" -> NpgsqlDbType.Time
        | "timestamp without time zone" | "timestamp" -> NpgsqlDbType.Timestamp
        | "character varying" | "varchar" -> NpgsqlDbType.Varchar
        | "refcursor" -> NpgsqlDbType.Refcursor
        | "inet" -> NpgsqlDbType.Inet
        | "bit" -> NpgsqlDbType.Bit
        | "timestamptz" -> NpgsqlDbType.TimestampTz
        | "uuid" -> NpgsqlDbType.Uuid
        | "xml" -> NpgsqlDbType.Xml
        | "oidvector" -> NpgsqlDbType.Oidvector
        | "interval" -> NpgsqlDbType.Interval
        | "timetz" -> NpgsqlDbType.TimeTz
        | "name" -> NpgsqlDbType.Name
        | "macaddr" -> NpgsqlDbType.MacAddr
        | "json" -> NpgsqlDbType.Json
        | "jsonb" -> NpgsqlDbType.Jsonb
        | "hstore" -> NpgsqlDbType.Hstore
        | "internalchar" -> NpgsqlDbType.InternalChar
        | "varbit" -> NpgsqlDbType.Varbit
        | "unknown" -> NpgsqlDbType.Unknown
        | "oid" -> NpgsqlDbType.Oid
        | "xid" -> NpgsqlDbType.Xid
        | "cid" -> NpgsqlDbType.Cid
        | "cidr" -> NpgsqlDbType.Cidr
        | "tsvector" -> NpgsqlDbType.TsVector
        | "tsquery" -> NpgsqlDbType.TsQuery
        | "regtype" -> NpgsqlDbType.Regtype
        | "geometry" -> NpgsqlDbType.Geometry
        | "citext" -> NpgsqlDbType.Citext
        | "int2vector" -> NpgsqlDbType.Int2Vector
        | "tid" -> NpgsqlDbType.Tid
        | "macaddr8" -> NpgsqlDbType.MacAddr8
        | "geography" -> NpgsqlDbType.Geography
        | "regconfig" -> NpgsqlDbType.Regconfig
        | "jsonpath" -> NpgsqlDbType.JsonPath
        | "pg_lsn" -> NpgsqlDbType.PgLsn
        | "ltree" -> NpgsqlDbType.LTree
        | "lquery" -> NpgsqlDbType.LQuery
        | "ltxtquery" -> NpgsqlDbType.LTxtQuery
        | "xid8" -> NpgsqlDbType.Xid8
        | "multirange" -> NpgsqlDbType.Multirange
        | "bigint_multirange" -> NpgsqlDbType.BigIntMultirange
        | "date_multirange" -> NpgsqlDbType.DateMultirange
        | "integer_multirange" -> NpgsqlDbType.IntegerMultirange
        | "numeric_multirange" -> NpgsqlDbType.NumericMultirange
        | "timestamp_multirange" -> NpgsqlDbType.TimestampMultirange
        | "timestamptz_multirange" -> NpgsqlDbType.TimestampTzMultirange
        | "range" -> NpgsqlDbType.Range
        | "bigint_range" -> NpgsqlDbType.BigIntRange
        | "date_range" -> NpgsqlDbType.DateRange
        | "integer_range" -> NpgsqlDbType.IntegerRange
        | "numeric_range" -> NpgsqlDbType.NumericRange
        | "timestamp_range" -> NpgsqlDbType.TimestampRange
        | "timestamptz_range" -> NpgsqlDbType.TimestampTzRange
        | _ -> failwith $"Unsupported PostgreSQL type: {sourceType}"