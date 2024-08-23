require("exasol.adapter.databricks.common_types")
require("exasol.adapter.databricks.databricks_types")
local log = require("remotelog")
local ExaError = require("ExaError")
local http = require("socket.http")
local cjson = require("cjson")
local util = require("exasol.adapter.databricks.util")

local function configure_http()
    http.PROXY = nil
    http.USERAGENT = "Exasol Databricks Virtual Schema"
end

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
    configure_http()
end

local function table_sink()
    local result = {}
    local function sink(chunk, err)
        if chunk then
            table.insert(result, chunk)
        end
        return 1
    end
    local function result_getter()
        return table.concat(result, "\n")
    end
    return sink, result_getter
end

---Send a GET request to the given path.
---@param path string
---@return table response body
---@private
function DatabricksRestClient:_get_request(path)
    local url = self._connection_details.url .. path
    log.debug("Sending GET request to " .. url)
    local sink, get_body = table_sink()
    local _, status_code, _response_headers, status_line = http.request({
        url = url,
        method = "GET",
        headers = {Authorization = "Bearer " .. self._connection_details.token},
        redirect = true,
        sink = sink
    })
    local body = get_body()
    if status_code ~= 200 then
        local exa_error = tostring(ExaError:new("E-VSDAB-5",
                                                "Databricks request for URL {{url}} failed with status {{status}} ({{status_message}}) and body {{body}}",
                                                {
            url = url,
            status = status_code,
            status_message = status_line,
            body = body
        }))
        log.error(exa_error)
        error(exa_error)
    end
    log.debug("Received response with status %d ('%s') and body size %d", status_code, status_line, #body)
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

---Get a list of all catalogs.
---@return table<DatabricksCatalog> catalogs
function DatabricksRestClient:list_catalog()
    local response = self:_get_request("/api/2.1/unity-catalog/catalogs?include_browse=true&max_results=1000")
    local catalogs = util.map(response.catalogs, function(raw)
        return {name = raw.name, browse_only = raw.browse_only, full_name = raw.full_name}
    end)
    log.debug("Received %d catalogs", #catalogs)
    return catalogs
end

return DatabricksRestClient
