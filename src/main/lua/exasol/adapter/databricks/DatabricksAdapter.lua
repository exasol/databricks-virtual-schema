local log = require("remotelog")
local PushdownMetadata = require("exasol.adapter.databricks.PushdownMetadata")

---@class DatabricksAdapter
---@field _metadata_reader MetadataReader
-- Derive from AbstractVirtualSchemaAdapter
local DatabricksAdapter = {}
DatabricksAdapter.__index = DatabricksAdapter
local AbstractVirtualSchemaAdapter = require("exasol.vscl.AbstractVirtualSchemaAdapter")
setmetatable(DatabricksAdapter, {__index = AbstractVirtualSchemaAdapter})
local VERSION<const> = "0.2.0"

local adapter_capabilities = require("exasol.adapter.databricks.adapter_capabilities")
local DatabricksQueryRewriter = require("exasol.adapter.databricks.DatabricksQueryRewriter")

---Create a `DatabricksAdapter`.
---@param metadata_reader MetadataReader metadata reader
---@return DatabricksAdapter
function DatabricksAdapter:new(metadata_reader)
    local instance = setmetatable({}, self)
    instance:_init(metadata_reader)
    return instance
end

function DatabricksAdapter:_init(metadata_reader)
    AbstractVirtualSchemaAdapter._init(self)
    self._metadata_reader = metadata_reader
end

--- Get the version number of the Virtual Schema adapter.
-- @return Virtual Schema adapter version
function DatabricksAdapter:get_version()
    return VERSION
end

--- Get the name of the Virtual Schema adapter.
-- @return Virtual Schema adapter name
function DatabricksAdapter:get_name()
    return "Databricks Virtual Schema (Lua)"
end

--- Create a virtual schema.
---@param request unknown virtual schema request
---@param properties DatabricksAdapterProperties user-defined properties
---@return CreateVirtualSchemaResponse response containing the metadata for the virtual schema like table and column structure
function DatabricksAdapter:create_virtual_schema(request, properties)
    properties:validate()
    local metadata = self:_handle_schema_scanning_request(request, properties)
    return {type = "createVirtualSchema", schemaMetadata = metadata}
end

---@param _request unknown
---@param properties DatabricksAdapterProperties
---@return ExasolSchemaMetadata schema_metadata
function DatabricksAdapter:_handle_schema_scanning_request(_request, properties)
    return self._metadata_reader:read(properties)
end

--- Refresh the metadata of the Virtual Schema.
--- <p>
--- Re-reads the structure and data types of the schema.
--- </p>
---@param request unknown virtual schema request
---@param properties DatabricksAdapterProperties user-defined properties
---@return RefreshVirtualSchemaResponse response containing the metadata for the virtual schema like table and column structure
function DatabricksAdapter:refresh(request, properties)
    properties:validate()
    local metadata = self:_handle_schema_scanning_request(request, properties)
    return {type = "refresh", schemaMetadata = metadata}
end

--- Alter the schema properties.
---This request provides two sets of user-defined properties. The old ones (i.e. the ones that where set before this
---request) and the properties that the user changed.
---@param request unknown virtual schema request
---@param old_properties DatabricksAdapterProperties old user-defined properties
---@param new_properties DatabricksAdapterProperties new user-defined properties
---@return SetPropertiesResponse response containing the metadata for the virtual schema like table and column structure
function DatabricksAdapter:set_properties(request, old_properties, new_properties)
    log.debug("Old properties " .. tostring(old_properties))
    log.debug("New properties " .. tostring(new_properties))
    ---@diagnostic disable-next-line: undefined-field # Type annotations for base library not available
    local merged_properties = old_properties:merge(new_properties)
    log.debug("Merged properties: %s", tostring(merged_properties))
    merged_properties:validate()
    local metadata = self:_handle_schema_scanning_request(request, merged_properties)
    return {type = "setProperties", schemaMetadata = metadata}
end

---Rewrite a pushed down query.
---@param request PushdownRequest virtual schema request
---@param properties DatabricksAdapterProperties user-defined properties
---@return PushdownResponse response
function DatabricksAdapter:push_down(request, properties)
    properties:validate()
    local pushdown_metadata = PushdownMetadata.create(request)
    local rewriter = DatabricksQueryRewriter:new(properties:get_connection_name(), pushdown_metadata)
    local rewritten_query = rewriter:rewrite(request.pushdownRequest)
    return {type = "pushdown", sql = rewritten_query}
end

---@return string[]
function DatabricksAdapter:_define_capabilities()
    return adapter_capabilities.get_capabilities()
end

return DatabricksAdapter
