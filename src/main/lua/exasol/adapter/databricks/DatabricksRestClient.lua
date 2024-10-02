require("exasol.adapter.databricks.common_types")
require("exasol.adapter.databricks.databricks_types")
local log = require("remotelog")
local ExaError = require("ExaError")
local http_client = require("exasol.adapter.databricks.http_client")
local cjson = require("cjson")
local util = require("exasol.adapter.databricks.util")

---@alias DatabricksRestClientFactory fun(connection_details: DatabricksConnectionDetails): DatabricksRestClient
---@alias TokenProvider fun(): string

---@class DatabricksRestClient
---@field _base_url string base URL of the Databricks REST API
---@field _token_provider TokenProvider returns authentication tokens
local DatabricksRestClient = {}
DatabricksRestClient.__index = DatabricksRestClient;

---Create a new `DatabricksRestClient`.
---@param base_url string Databricks REST API base URL
---@param token_provider TokenProvider Databricks REST API token provider
---@return DatabricksRestClient client
function DatabricksRestClient:new(base_url, token_provider)
    local instance = setmetatable({}, self)
    instance._base_url = base_url
    instance._token_provider = token_provider
    return instance
end

---Send a GET request to the given path.
---@param path string
---@return table response body
---@private
function DatabricksRestClient:_get_request(path)
    local url = self._base_url .. path
    log.debug("Sending GET request to " .. url)
    local body = http_client.request({
        url = url,
        method = "GET",
        headers = {Authorization = "Bearer " .. self._token_provider()},
        verify_tls_certificate = false
    })
    local data = cjson.decode(body)
    if data.next_page_token then
        local exa_error = ExaError:new("E-VSDAB-12",
                                       "Pagination not implemented yet for request {{url}}, next page token: {{next_page_token}}",
                                       {url = url, next_page_token = data.next_page_token})
        log.error(exa_error)
        error(tostring(exa_error))
    end
    return data
end

---@param raw table<string,any> raw data
---@return DatabricksColumn
local function convert_column(raw)
    return {
        name = raw.name,
        position = raw.position,
        comment = raw.comment,
        type = {name = raw.type_name, text = raw.type_text, precision = raw.type_precision, scale = raw.type_scale},
        nullable = raw.nullable,
        databricks_metadata = raw
    }
end

---@param raw table<string,any> raw data
---@return DatabricksTable
local function convert_table(raw)
    local columns = util.map(raw.columns, convert_column)
    return {
        name = raw.name,
        catalog_name = raw.catalog_name,
        schema_name = raw.schema_name,
        full_name = raw.full_name,
        table_type = raw.table_type,
        data_source_format = raw.data_source_format,
        comment = raw.comment,
        columns = columns,
        databricks_metadata = raw
    }
end

---Get a list of all tables.
---See Databricks documentation https://docs.databricks.com/api/workspace/tables/list
---@param catalog_name string name of the catalog
---@param schema_name string name of the schema
---@return DatabricksTable[] tables
function DatabricksRestClient:list_tables(catalog_name, schema_name)
    local response = self:_get_request(string.format(
            "/api/2.1/unity-catalog/tables?catalog_name=%s&schema_name=%s&max_results=50"
                    .. "&include_delta_metadata=true&omit_columns=false&omit_properties=true&include_browse=false",
            catalog_name, schema_name))
    local tables = util.map(response.tables, convert_table)
    log.debug("Found %d tables in %s.%s", #tables, catalog_name, schema_name)
    return tables
end

return DatabricksRestClient
