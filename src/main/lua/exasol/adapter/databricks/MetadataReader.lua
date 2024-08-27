require("exasol_types")
local ConnectionReader = require("exasol.adapter.databricks.ConnectionReader")
local log = require("remotelog")

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

---Read the database metadata of the given schema (i.e. the internal structure of that schema)
---@param properties DatabricksAdapterProperties
---@return table schema_metadata
function MetadataReader:read(properties)
    local databricks_client = self:_create_databricks_client(properties)
    databricks_client:list_catalogs()
    local tables = {}
    local config = {}
    return {tables = tables, adapterNotes = "notes", config = config}
end

return MetadataReader
