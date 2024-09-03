---This class rewrites the query.
---@class DatabricksQueryRewriter
---@field _connection_id string name of the Databricks connection
local DatabricksQueryRewriter = {_NAME = "DatabricksQueryRewriter"}
DatabricksQueryRewriter.__index = DatabricksQueryRewriter

local QueryRenderer = require("exasol.vscl.QueryRenderer")
local ImportQueryBuilder = require("exasol.vscl.ImportQueryBuilder")

--- Create a new instance of a `RemoteQueryRewriter`.
-- @param connection_id ID of the connection object that defines the details of the connection to the remote Exasol
-- @return new instance
function DatabricksQueryRewriter:new(connection_id)
    local instance = setmetatable({}, self)
    instance:_init(connection_id)
    return instance
end

function DatabricksQueryRewriter:_init(connection_id)
    self._connection_id = connection_id
end

function DatabricksQueryRewriter:_create_import(original_query, source_schema_id)
    local remote_query = original_query
    local import_query = ImportQueryBuilder:new():connection(self._connection_id):column_types(
            original_query.selectListDataTypes):statement(remote_query):build()
    local renderer = QueryRenderer:new(import_query)
    return renderer:render()
end

-- Override
function DatabricksQueryRewriter:rewrite(original_query, source_schema_id, _, _)
    return self:_create_import(original_query, source_schema_id)
end

return DatabricksQueryRewriter
