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

---Adapt the given query structure element for pushdown to Databricks, e.g. by adding Databricks catalog and schema names.
---@param element any
---@return any updated_element
function DatabricksQueryRewriter:_recursive_patch_query_element(element)
    if (type(element) ~= "table") then
        return element
    end
    local extended_element = {}
    for key, value in pairs(element) do
        extended_element[key] = self:_recursive_patch_query_element(value)
    end
    self:_patch_query_element(extended_element)
    return extended_element
end

function DatabricksQueryRewriter:_patch_query_element(element)
    local element_patcher = {table = self._patch_table, column = self._patch_column}
    local patcher = element_patcher[element.type]
    if patcher then
        patcher(self, element)
    end
end

---Update a table expression:
--- * Insert Databricks catalog and schema
--- * Replace Exasol table name (upper case) with original Databricks table name.
---@param table_expression TableExpression
function DatabricksQueryRewriter:_patch_table(table_expression)
    local exasol_table_name = table_expression.name
    local table_notes = self._pushdown_metadata:get_table_notes(exasol_table_name)
    local databricks_catalog_name = table_notes:get_databricks_catalog_name()
    local databricks_schema_name = table_notes:get_databricks_schema_name()
    local databricks_table_name = table_notes:get_databricks_table_name()
    log.debug("Extended original table %s to databricks table %s.%s.%s", exasol_table_name, databricks_catalog_name,
              databricks_schema_name, databricks_table_name)
    table_expression.name = databricks_table_name
    table_expression.schema = databricks_schema_name
    table_expression.catalog = databricks_catalog_name
end

---Replace Exasol table and column name (upper case) with original Databricks names.
---@param column_expression ColumnReference
function DatabricksQueryRewriter:_patch_column(column_expression)
    local exasol_table_name = column_expression.tableName
    local exasol_column_name = column_expression.name
    local table_notes = self._pushdown_metadata:get_table_notes(exasol_table_name)
    local databricks_table_name = table_notes:get_databricks_table_name()
    local databricks_column_name = table_notes:get_databricks_column_name(exasol_column_name)
    log.debug("Extended original column %s.%s to databricks %s.%s", exasol_table_name, column_expression.name,
              databricks_table_name, databricks_column_name)
    column_expression.name = databricks_column_name
    column_expression.tableName = databricks_table_name
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
    local remote_query = self:_recursive_patch_query_element(original_query)
    log.trace("Rewritten query structure: %s", cjson.encode(remote_query))
    return self:_create_import(remote_query)
end

return DatabricksQueryRewriter
