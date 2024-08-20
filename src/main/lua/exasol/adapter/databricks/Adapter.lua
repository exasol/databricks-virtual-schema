local log = require("remotelog")

-- Derive from AbstractVirtualSchemaAdapter
local Adapter = {}
Adapter.__index = Adapter
local AbstractVirtualSchemaAdapter = require(
                                         "exasol.vscl.AbstractVirtualSchemaAdapter")
setmetatable(Adapter, {__index = AbstractVirtualSchemaAdapter})
local VERSION<const> = "1.5.6"

local adapter_capabilities = require(
                                 "exasol.adapter.databricks.adapter_capabilities")
local QueryRewriter = require(
                          "exasol.adapter.databricks.DatabricksQueryRewriter")

--- Create an `Adapter`.
-- @param metadata_reader metadata reader
-- @return Adapter
function Adapter:new(metadata_reader)
    local instance = setmetatable({}, self)
    instance:_init(metadata_reader)
    return instance
end

function Adapter:_init(metadata_reader)
    AbstractVirtualSchemaAdapter._init(self)
    self._metadata_reader = metadata_reader
end

--- Get the version number of the Virtual Schema adapter.
-- @return Virtual Schema adapter version
function Adapter:get_version() return VERSION end

--- Get the name of the Virtual Schema adapter.
-- @return Virtual Schema adapter name
function Adapter:get_name() return "Row-level Security adapter (Lua)" end

--- Create a virtual schema.
-- @param request virtual schema request
-- @param properties user-defined properties
-- @return response containing the metadata for the virtual schema like table and column structure
function Adapter:create_virtual_schema(request, properties)
    properties:validate()
    local metadata = self:_handle_schema_scanning_request(request, properties)
    return {type = "createVirtualSchema", schemaMetadata = metadata}
end

function Adapter:_handle_schema_scanning_request(request, properties)
    local config = ""
    return self._metadata_reader:read(config)
end

--- Refresh the metadata of the Virtual Schema.
-- <p>
-- Re-reads the structure and data types of the schema protected by RLS.
-- </p>
-- @param request virtual schema request
-- @param properties user-defined properties
-- @return response containing the metadata for the virtual schema like table and column structure
function Adapter:refresh(request, properties)
    properties:validate()
    return {
        type = "refresh",
        schemaMetadata = self:_handle_schema_scanning_request(request,
                                                              properties)
    }
end

--- Alter the schema properties.
-- This request provides two sets of user-defined properties. The old ones (i.e. the ones that where set before this
-- request) and the properties that the user changed.
-- @param request virtual schema request
-- @param old_properties old user-defined properties
-- @param new_properties new user-defined properties
-- @return response containing the metadata for the virtual schema like table and column structure
function Adapter:set_properties(request, old_properties, new_properties)
    log.debug("Old properties " .. tostring(old_properties))
    log.debug("New properties " .. tostring(new_properties))
    local merged_properties = old_properties:merge(new_properties)
    log.debug("Merged properties " .. tostring(merged_properties))
    merged_properties:validate()
    return {
        type = "setProperties",
        schemaMetadata = self:_handle_schema_scanning_request(request,
                                                              merged_properties)
    }
end

--- Rewrite a pushed down query.
-- @param request virtual schema request
-- @param properties user-defined properties
-- @return response containing the list of reported capabilities
function Adapter:push_down(request, properties)
    properties:validate()
    local adapter_cache = request.schemaMetadataInfo.adapterNotes
    local rewriter = QueryRewriter:new()
    local rewritten_query = rewriter:rewrite(request.pushdownRequest,
                                             properties:get_schema_name(),
                                             adapter_cache,
                                             request.involvedTables)
    return {type = "pushdown", sql = rewritten_query}
end

function Adapter:_define_capabilities() return adapter_capabilities end

return Adapter
