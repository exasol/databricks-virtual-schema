local exasol = require("exasol_types")
local ConnectionReader = require("exasol.adapter.databricks.ConnectionReader")
local util = require("exasol.adapter.databricks.util")
local log = require("remotelog")
local ExaError = require("ExaError")

---This class reads schema, table and column metadata from the source.
---@class MetadataReader
---@field _exasol_context ExasolUdfContext 
---@field _databricks_client_factory DatabricksRestClientFactory 
local MetadataReader = {}
MetadataReader.__index = MetadataReader

---Create a new `MetadataReader`.
---@param  exasol_context ExasolUdfContext
---@param  databricks_client_factory DatabricksRestClientFactory
---@return MetadataReader metadata_reader
function MetadataReader:new(exasol_context, databricks_client_factory)
    assert(exasol_context ~= nil,
           "The metadata reader requires an Exasol context handle in order to read metadata from the database")
    local instance = setmetatable({}, self)
    instance:_init(exasol_context, databricks_client_factory)
    return instance
end

function MetadataReader:_init(exasol_context, databricks_client_factory)
    self._exasol_context = exasol_context
    self._databricks_client_factory = databricks_client_factory
end

---@param properties DatabricksAdapterProperties
---@return DatabricksRestClient
function MetadataReader:_create_databricks_client(properties)
    local connection_name = properties:get_connection_name()
    local connection_details = ConnectionReader:new(self._exasol_context):read(connection_name)
    return self._databricks_client_factory(connection_details)
end

local EXASOL_MAX_VARCHAR_SIZE = 2000000

---Extract precision and scale from Databricks type text for DECIMAL type
---@param type_text string Databricks type text, e.g. "decimal(10,2)"
---@return integer precision
---@return integer scale
local function extract_precision_and_scale(type_text)
    return 1, 2
end

---@param databricks_column DatabricksColumn
---@return string error_message
local function unsupported_interval_type_error(databricks_column)
    local exa_error = tostring(ExaError:new("E-VSDAB-9",
                                            "Unknown Databricks interval type {{interval_type}} for column {{column_name}} "
                                                    .. "at position {{column_position}} (comment: {{column_comment}})",
                                            {
        interval_type = databricks_column.type.text,
        column_name = databricks_column.name,
        column_position = databricks_column.position,
        column_comment = databricks_column.comment
    }):add_ticket_mitigation())
    log.error(exa_error)
    return exa_error
end

local function unsupported_type()
    return nil
end

-- Databricks types: https://docs.databricks.com/en/sql/language-manual/sql-ref-datatypes.html
---@type table<string, fun(databricks_column: DatabricksColumn): ExasolDatatypeMetadata?>
local DATA_TYPE_FACTORIES = {
    STRING = function()
        -- https://docs.databricks.com/en/sql/language-manual/data-types/string-type.html
        -- Databricks does not report columns size, so we use the maximum size for VARCHAR
        return {type = exasol.DATA_TYPES.VARCHAR, size = EXASOL_MAX_VARCHAR_SIZE}
    end,
    BYTE = function()
        -- https://docs.databricks.com/en/sql/language-manual/data-types/tinyint-type.html
        -- Range: -128 to 127
        return {type = exasol.DATA_TYPES.DECIMAL, precision = 3, scale = 0}
    end,
    SHORT = function()
        -- https://docs.databricks.com/en/sql/language-manual/data-types/smallint-type.html
        -- Range: -32,768 to 32,767
        return {type = exasol.DATA_TYPES.DECIMAL, precision = 5, scale = 0}
    end,
    INT = function()
        -- https://docs.databricks.com/en/sql/language-manual/data-types/int-type.html
        -- Range: -2,147,483,648 to 2,147,483,647
        return {type = exasol.DATA_TYPES.DECIMAL, precision = 10, scale = 0}
    end,
    LONG = function()
        -- https://docs.databricks.com/en/sql/language-manual/data-types/bigint-type.html
        -- Range: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
        return {type = exasol.DATA_TYPES.DECIMAL, precision = 19, scale = 0}
    end,
    FLOAT = function()
        -- https://docs.databricks.com/en/sql/language-manual/data-types/float-type.html
        -- Exasol has no FLOAT type, so we use DOUBLE
        return {type = exasol.DATA_TYPES.DOUBLE}
    end,
    DOUBLE = function()
        -- https://docs.databricks.com/en/sql/language-manual/data-types/double-type.html
        return {type = exasol.DATA_TYPES.DOUBLE}
    end,
    DECIMAL = function(databricks_column)
        -- https://docs.databricks.com/en/sql/language-manual/data-types/decimal-type.html
        -- TODO: extract precision and scale from databricks_column.type.text
        local precision, scale = extract_precision_and_scale(databricks_column.type.text)
        return {type = exasol.DATA_TYPES.DECIMAL, precision = precision, scale = scale}
    end,
    BOOLEAN = function()
        -- https://docs.databricks.com/en/sql/language-manual/data-types/boolean-type.html
        return {type = exasol.DATA_TYPES.BOOLEAN}
    end,
    TIMESTAMP = function(databricks_column)
        -- https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-type.html
        -- Range: -290308-12-21 BCE 19:59:06 GMT to +294247-01-10 CE 04:00:54 GMT
        return {type = exasol.DATA_TYPES.TIMESTAMP, withLocalTimeZone = true}
    end,
    TIMESTAMP_NTZ = function(databricks_column)
        -- https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-ntz-type.html
        -- Range: -290308-12-21 BCE 19:59:06 to +294247-01-10 CE 04:00:54
        return {type = exasol.DATA_TYPES.TIMESTAMP, withLocalTimeZone = false}
    end,
    INTERVAL = function(databricks_column)
        -- https://docs.databricks.com/en/sql/language-manual/data-types/interval-type.html
        if databricks_column.type.text == "interval year to month" then
            return {type = exasol.DATA_TYPES.INTERVAL, fromTo = exasol.INTERVAL_TYPES.YEAR_TO_MONTH}
        elseif databricks_column.type.text == "interval hour to second" then
            return {type = exasol.DATA_TYPES.INTERVAL, fromTo = exasol.INTERVAL_TYPES.DAY_TO_SECONDS}
        else
            error(unsupported_interval_type_error(databricks_column))
        end
    end,
    BINARY = unsupported_type,
    ARRAY = unsupported_type,
    MAP = unsupported_type,
    STRUCT = unsupported_type,
    VARIANT = unsupported_type
}

---@param databricks_column DatabricksColumn
---@return string error_message
local function unknown_databricks_type_error(databricks_column)
    local exa_error = tostring(ExaError:new("E-VSDAB-7",
                                            "Unknown Databricks data type {{data_type}} / {{data_type_text}} for column {{column_name}} "
                                                    .. "at position {{column_position}} (precision: {{data_type_precision}}, "
                                                    .. "scale: {{data_type_scale}}, comment: {{column_comment}})", {
        data_type = databricks_column.type.name,
        data_type_text = databricks_column.type.text,
        data_type_precision = databricks_column.type.precision,
        data_type_scale = databricks_column.type.scale,
        column_name = databricks_column.name,
        column_position = databricks_column.position,
        column_comment = databricks_column.comment
    }):add_ticket_mitigation())
    log.error(exa_error)
    return exa_error
end

---@param databricks_column DatabricksColumn
---@return string error_message
local function unsupported_databricks_type_error(databricks_column)
    local message = tostring(ExaError:new("E-VSDAB-8",
                                          "Exasol does not support Databricks data type {{data_type}} of column {{column_name}} "
                                                  .. "at position {{column_position}} with comment {{column_comment}}",
                                          {
        data_type = databricks_column.type.name,
        column_name = databricks_column.name,
        column_position = databricks_column.position,
        column_comment = databricks_column.comment
    }):add_mitigations("Please remove the column or change the data type."))
    log.warn(message)
    return message
end

---@param databricks_column DatabricksColumn
---@return ExasolDatatypeMetadata exasol_data_type
local function convert_data_type(databricks_column)
    local data_type = databricks_column.type
    local factory = DATA_TYPE_FACTORIES[data_type.name]
    if not factory then
        error(unknown_databricks_type_error(databricks_column))
    end
    local mapped_type = factory(databricks_column)
    if not mapped_type then
        error(unsupported_databricks_type_error(databricks_column))
    end
    return mapped_type
end

---@param databricks_colum DatabricksColumn
---@return ExasolColumnMetadata exasol_column_metadata
local function convert_column_metadata(databricks_colum)
    return {
        name = databricks_colum.name,
        dataType = convert_data_type(databricks_colum),
        isNullable = databricks_colum.nullable,
        isIdentity = nil, -- Databricks does not support identity columns
        default = nil, -- Databricks does not support default values
        comment = databricks_colum.comment
    }
end

---@param databricks_table DatabricksTable
---@return ExasolTableMetadata exasol_table_metadata
local function convert_table_metadata(databricks_table)
    return {
        type = exasol.OBJECT_TYPES.TABLE,
        name = databricks_table.name,
        comment = databricks_table.comment,
        columns = util.map(databricks_table.columns, convert_column_metadata)
    }
end

---Read the database metadata of the given schema (i.e. the internal structure of that schema)
---@param properties DatabricksAdapterProperties
---@return ExasolSchemaMetadata schema_metadata
function MetadataReader:read(properties)
    local databricks_client = self:_create_databricks_client(properties)
    local tables = databricks_client:list_tables(properties:get_catalog_name(), properties:get_schema_name())
    return {tables = util.map(tables, convert_table_metadata), adapterNotes = "notes"}
end

return MetadataReader
