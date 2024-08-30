require("exasol.adapter.databricks.common_types")
require("exasol.adapter.databricks.databricks_types")
local log = require("remotelog")
local ExaError = require("ExaError")
local http_client = require("exasol.adapter.databricks.http_client")
local cjson = require("cjson")
local util = require("exasol.adapter.databricks.util")

---@alias DatabricksRestClientFactory fun(connection_details: DatabricksConnectionDetails): DatabricksRestClient

---@class DatabricksRestClient
---@field _connection_details DatabricksConnectionDetails connection details
local DatabricksRestClient = {}
DatabricksRestClient.__index = DatabricksRestClient;

--- Create a new `DatabricksRestClient`.
---@param  connection_details DatabricksConnectionDetails connection details
---@return DatabricksRestClient client
function DatabricksRestClient:new(connection_details)
    local instance = setmetatable({}, self)
    instance:_init(connection_details)
    return instance
end

function DatabricksRestClient:_init(connection_details)
    self._connection_details = connection_details
end

---Send a GET request to the given path.
---@param path string
---@return table response body
---@private
function DatabricksRestClient:_get_request(path)
    local url = self._connection_details.url .. path
    log.debug("Sending GET request to " .. url)
    local body = http_client.request({
        url = url,
        method = "GET",
        headers = {Authorization = "Bearer " .. self._connection_details.token},
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

---Get a list of all catalogs.
---@return DatabricksCatalog[] catalogs
function DatabricksRestClient:list_catalogs()
    local response = self:_get_request("/api/2.1/unity-catalog/catalogs?include_browse=true&max_results=1000")
    local catalogs = util.map(response.catalogs, function(raw)
        return {name = raw.name, browse_only = raw.browse_only, full_name = raw.full_name}
    end)
    log.debug("Received %d catalogs", #catalogs)
    return catalogs
end

---@param raw table<string,any> raw data
---@return DatabricksColumn
local function convert_column(raw)
    return {
        name = raw.name,
        position = raw.position,
        comment = raw.comment,
        type = {name = raw.type_name, text = raw.type_text, precision = raw.type_precision, scale = raw.type_scale},
        nullable = raw.nullable
    }
end

---@param raw table<string,any> raw data
---@return DatabricksTable
local function convert_table(raw)
    local columns = util.map(raw.columns, convert_column)
    return {name = raw.name, full_name = raw.full_name, comment = raw.comment, columns = columns}
end

---Get a list of all tables.
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
