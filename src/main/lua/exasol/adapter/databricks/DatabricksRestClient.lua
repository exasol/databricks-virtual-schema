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
        headers = {Authorization = "Bearer " .. self._connection_details.token}
    })
    log.trace("Received body %s", body)
    local data = cjson.decode(body)
    if data.next_page_token then
        local exa_error = ExaError:new("E-VSDAB-6",
                                       "Pagination not implemented yet for request {{url}}, next page token: {{next_page_token}}",
                                       {url = url, next_page_token = data.next_page_token})
        log.error(exa_error)
        error(tostring(exa_error))
    end
    return data
end

---Send a GET request to the given path.
---@param path string
---@return table response body
---@private
function DatabricksRestClient:_simple_get_request(path)
    local url = self._connection_details.url .. path
    url = "https://httpbin.org/get"
    log.debug("Sending GET request to " .. url)

    local sink, get_body = table_sink()
    local first_result, status_code, response_headers, status_line = http.request({
        url = url,
        -- sink = sink,
        method = "GET",
        headers = {Authorization = "Bearer token"},
        redirect = true,
        sink = sink
    })
    log.info("First result %s", first_result)
    log.info("Second result %s", status_code)
    log.info("Thrid result %s", response_headers)
    log.info("Fourth result %s", status_line)
    log.info("body %s", get_body())
    log.info("Result: first arg: %s, status: %s, headers %s, status line %s", first_result, status_code,
             tostring(response_headers), status_line)

    return {catalogs = {}}
end

---Get a list of all catalogs.
---@return table<DatabricksCatalog> catalogs
function DatabricksRestClient:list_catalogs()
    local response = self:_get_request("/api/2.1/unity-catalog/catalogs") -- ?include_browse=true&max_results=1000")
    local catalogs = util.map(response.catalogs, function(raw)
        return {name = raw.name, browse_only = raw.browse_only, full_name = raw.full_name}
    end)
    log.debug("Received %d catalogs", #catalogs)
    return catalogs
end

return DatabricksRestClient
