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

---@param element any
---@return any
function DatabricksQueryRewriter:_replace_source_table_name(element)
    local extended_element = {}
    if (type(element) == "table") then
        for key, value in pairs(element) do
            if (type(value) == "table") then
                extended_element[key] = self:_replace_source_table_name(value)
            else
                extended_element[key] = value
            end
        end
        if (element.type ~= nil and element.type == "table" and element.schema == nil) then
            local table_name = element.name
            local table_notes = self._pushdown_metadata:get_table_notes(table_name)
            local catalog_name = table_notes:get_databricks_catalog_name()
            local schema_name = table_notes:get_databricks_schema_name()
            log.debug("Extended table '%s' with source catalog %s and schema %s", table_name, catalog_name, schema_name)
            extended_element.schema = schema_name
            extended_element.catalog = catalog_name
        end
    else
        return element
    end
    return extended_element
end

---@param query SelectSqlStatement
---@return string rewritten_query
function DatabricksQueryRewriter:_create_import(query)
    local import_query = ImportQueryBuilder:new():source_type("JDBC"):connection(self._connection_id):column_types(
            query.selectListDataTypes):statement(query):build()
    local renderer = QueryRenderer:new(import_query, {identifier_quote = "`"})
    return renderer:render()
end

---@param original_query SelectSqlStatement original query as specified by VS user
---@return string rewritten_query rewritten query to be fed into the ExaLoader for import
function DatabricksQueryRewriter:rewrite(original_query)
    local remote_query = self:_replace_source_table_name(original_query)
    return self:_create_import(remote_query)
end

return DatabricksQueryRewriter
