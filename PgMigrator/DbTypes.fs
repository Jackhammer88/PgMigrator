namespace PgMigrator

module DbTypes =
    let psqlTypes = 
        set [
            // Числовые
            "smallint"
            "integer"
            "int"
            "bigint"
            "real"
            "double precision"
            "numeric"
            "numeric"
            "decimal"

            // Символьные
            "char"
            "varchar"
            "text"

            // Логический
            "boolean"

            // Дата и время
            "date"
            "time"
            "time with time zone"
            "timestamp"
            "timestamp with time zone"
            "interval"

            // Монетарный
            "money"

            // Бинарные
            "bytea"

            // Сетевые
            "cidr"
            "inet"
            "macaddr"
            "macaddr8"

            // Геометрические
            "point"
            "line"
            "lseg"
            "box"
            "path"
            "polygon"
            "circle"

            // JSON и XML
            "json"
            "jsonb"
            "xml"

            // Массивы
            "array"

            // UUID
            "uuid"

            // Диапазоны
            "int4range"
            "int8range"
            "numrange"
            "tsrange"
            "tstzrange"
            "daterange"

            // Идентификаторы объектов
            "oid"

            // Полнотекстовый поиск
            "tsvector"
            "tsquery"

            // Репликация и специальные типы
            "pg_lsn"
        ]
        
    let mssqlTypes = 
        set [
            // Числовые (целые)
            "tinyint"
            "smallint"
            "int"
            "bigint"

            // Числовые (с плавающей точкой и точные)
            "decimal"
            "numeric"
            "money"
            "smallmoney"
            "float"
            "real"

            // Символьные
            "char"
            "varchar"
            "text"
            "nchar"
            "nvarchar"
            "ntext"

            // Дата и время
            "date"
            "time"
            "datetime"
            "datetime2"
            "smalldatetime"
            "datetimeoffset"

            // Логический
            "bit"

            // Двоичные
            "binary"
            "varbinary"
            "image"

            // Уникальные идентификаторы
            "uniqueidentifier"

            // Специальные
            "sql_variant"
            "xml"
            "cursor"
            "table"

            // Пространственные
            "geometry"
            "geography"
        ]
