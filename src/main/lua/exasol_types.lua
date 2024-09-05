---@meta exasol_types
local M = {}

---Context for Exasol Lua UDFs.
---@class ExasolUdfContext
local ExasolUdfContext = {}
---Get the connection details for the named connection.
---@param connection_name string The name of the connection.
---@return Connection? connection connection details.
function ExasolUdfContext.get_connection(connection_name)
end

---An Exasol connection object
---@class Connection
---@field address string? The address of the connection.
---@field user string? The user name for the connection.
---@field password string? The password for the connection.
local Connection = {}

---Response for a createVirtualSchema request
---@class CreateVirtualSchemaResponse
---@field type "createVirtualSchema"
---@field schemaMetadata ExasolSchemaMetadata 

---Response for a refresh request
---@class RefreshVirtualSchemaResponse
---@field type "refresh"
---@field schemaMetadata ExasolSchemaMetadata 

---Response for a set properties request
---@class SetPropertiesResponse
---@field type "setProperties"
---@field schemaMetadata ExasolSchemaMetadata 

---Pushdown request
---@class PushdownRequest
---@field type "pushdown"
---@field involvedTables PushdownInvolvedTable[]
---@field pushdownRequest table<string, any>
---@field schemaMetadataInfo SchemaMetadataInfo

---@class PushdownInvolvedTable
---@field name string
---@field adapterNotes string?
---@field columns PushdownInvolvedColumn[]

---@class PushdownInvolvedColumn
---@field name string
---@field dataType ExasolDatatypeMetadata

---Schema metadata info in requests
---@class SchemaMetadataInfo
---@field name string virtual schema name
---@field adapterNotes string?
---@field properties table<string, string>

---Response for a pushdown request
---@class PushdownResponse
---@field type "pushdown"
---@field sql string The SQL statement to be executed in the remote system.

---Response for a createVirtualSchema request
---Based on https://github.com/exasol/virtual-schema-common-java/blob/main/src/main/java/com/exasol/adapter/metadata/converter/SchemaMetadataJsonConverter.java
---@class ExasolSchemaMetadata
---@field tables ExasolTableMetadata[] The tables in the virtual schema.
---@field adapterNotes? string Notes for the virtual schema adapter.
local ExasolSchemaMetadata = {}

---@class ExasolTableMetadata
---@field type ExasolObjectType Object type, e.g. `table`
---@field name string Name of the table
---@field adapterNotes string? Notes for the table adapter
---@field comment string? Comment for the table
---@field columns ExasolColumnMetadata[] Columns in the table
local ExasolTableMetadata = {}

---@class ExasolColumnMetadata
---@field name string Name of the column
---@field adapterNotes string? Notes for the table adapter
---@field dataType ExasolDatatypeMetadata Data type of the column
---@field isNullable boolean?  Whether the column is nullable (default: true)
---@field isIdentity boolean?  Whether the column is an identity column (default: false)
---@field default string? Default value for the column
---@field comment string? Comment for the column
local ExasolColumnMetadata = {}

---@class ExasolDatatypeMetadata
---@field type ExasolDataType Data type name, e.g. `decimal` or `varchar`
---@field precision integer? The precision of the data type for types DECIMAL and INTERVAL
---@field scale integer? The scale of the data type for DECIMAL types
---@field size integer? The size of the data type for CHAR and VARCHAR types
---@field characterSet string? The character set of the data type for CHAR and VARCHAR types
---@field withLocalTimeZone boolean? Whether the data type is with local time zone for type TIMESTAMP
---@field fractionalSecondsPrecision integer? The fractional seconds precision of data type TIMESTAMP
---@field srid integer? The spatial reference identifier for data type GEOMETRY
---@field bytesize integer? The byte size of type HASHTYPE
---@field fromTo ExasolIntervalType? The range of type INTERVAL
---@field fraction integer? The fraction of type INTERVAL DAY TO SECOND
local ExasolDatatypeMetadata = {}

---@enum ExasolObjectType
M.OBJECT_TYPES = {TABLE = "table"}

---@enum ExasolDataType
M.DATA_TYPES = {
    DECIMAL = "decimal",
    DOUBLE = "double",
    VARCHAR = "varchar",
    CHAR = "char",
    DATE = "date",
    TIMESTAMP = "timestamp",
    BOOLEAN = "boolean",
    GEOMETRY = "geometry",
    INTERVAL = "interval",
    HASHTYPE = "hashtype"
    -- UNKNOWN = "unknown" -- Causes Exasol error "Unsupported data type (UNKNOWN)"
    -- UNSUPPORTED = "unsupported", -- Unsupported data type (UNSUPPORTED)
}

---@enum ExasolIntervalType
M.INTERVAL_TYPES = {DAY_TO_SECONDS = "DAY TO SECONDS", YEAR_TO_MONTH = "YEAR TO MONTH"}

return M
