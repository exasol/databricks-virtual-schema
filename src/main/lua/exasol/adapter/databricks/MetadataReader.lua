local ConnectionReader = require("exasol.adapter.databricks.ConnectionReader")
local log = require("remotelog")

--- This class reads schema, table and column metadata from the source.
-- @type MetadataReader
local MetadataReader = {}
MetadataReader.__index = MetadataReader

--- Create a new `MetadataReader`.
-- @param exasol_context handle to local database functions and status
-- @return metadata reader
function MetadataReader:new(exasol_context)
    assert(exasol_context ~= nil,
           "The metadata reader requires an Exasol context handle in order to read metadata from the database")
    local instance = setmetatable({}, self)
    instance:_init(exasol_context)
    return instance
end

function MetadataReader:_init(exasol_context)
    self._exasol_context = exasol_context
end

function MetadataReader:_get_connection(properties)
    local connection_name = properties:get_connection_name()
    return ConnectionReader:new(self._exasol_context):read(connection_name)
end

--- Read the database metadata of the given schema (i.e. the internal structure of that schema)
-- <p>
-- The scan can optionally be limited to a set of user-defined tables. If the list of tables to include in the scan
-- is omitted, then all tables in the source schema are scanned and reported.
-- </p>
-- @return schema metadata
function MetadataReader:read(properties)
    local connection = self:_get_connection(properties)
    local tables = {}
    local config = {}
    return {tables = tables, adapterNotes = "notes", config = config, connection = connection}
end

return MetadataReader
