---This class rewrites the query.
---@class DatabricksQueryRewriter
---@field _connection_id string name of the Databricks connection
---@field _pushdown_metadata PushdownMetadata metadata of the pushdown request
local DatabricksQueryRewriter = {_NAME = "DatabricksQueryRewriter"}
DatabricksQueryRewriter.__index = DatabricksQueryRewriter

local QueryRenderer = require("exasol.vscl.QueryRenderer")
local ImportQueryBuilder = require("exasol.vscl.ImportQueryBuilder")
local log = require("remotelog")
local cjson = require("cjson")

--- Create a new instance of a `RemoteQueryRewriter`.
---@param connection_id string ID of the connection object that defines the details of the connection to the remote Exasol
---@param pushdown_metadata PushdownMetadata metadata of the pushdown request
---@return DatabricksQueryRewriter new_instance
function DatabricksQueryRewriter:new(connection_id, pushdown_metadata)
    local instance = setmetatable({}, self)
    instance._connection_id = connection_id
    instance._pushdown_metadata = pushdown_metadata
    return instance
end

local function replace_source_table_name(element, pushdown_metadata)
    local extended_element = {}
    if (type(element) == "table") then
        for key, value in pairs(element) do
            if (type(value) == "table") then
                log.debug("Replace for key: %s", key)
                extended_element[key] = replace_source_table_name(value, pushdown_metadata)
            else
                log.debug("Copy key: %s, Value: %s", key, value)
                extended_element[key] = value
            end
        end
        if (element.type ~= nil and element.type == "table" and element.schema == nil) then
            local table_name = element.name
            local schema_name = pushdown_metadata:get_table_notes(table_name):get_qualified_databricks_schema_name()
            log.debug("Extended table '%s' with source schema '%s' ", table_name, schema_name)
            extended_element.schema = schema_name
        end
    else
        log.debug("Return non-table element: %s (%s)", element, type(element))
        return element
    end
    return extended_element
end

function DatabricksQueryRewriter:_create_import(query)
    local import_query = ImportQueryBuilder:new():source_type("JDBC"):connection(self._connection_id):column_types(
            query.selectListDataTypes):statement(query):build()
    local renderer = QueryRenderer:new(import_query)
    return renderer:render()
end

-- Override
function DatabricksQueryRewriter:rewrite(original_query)
    log.debug("Original query: %s", cjson.encode(original_query))
    local remote_query = replace_source_table_name(original_query, self._pushdown_metadata)
    log.debug("Rewritten query: %s", cjson.encode(remote_query))
    return self:_create_import(remote_query)
end

return DatabricksQueryRewriter
