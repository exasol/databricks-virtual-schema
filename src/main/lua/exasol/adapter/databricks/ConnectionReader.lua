require("exasol_types")
require("exasol.adapter.databricks.common_types")
local log = require("remotelog")
local ExaError = require("ExaError")

--- This class reads details of a Databricks JDBC connection.
---@class ConnectionReader
---@field _exasol_context ExasolUdfContext handle to local database functions and status
local ConnectionReader = {};
ConnectionReader.__index = ConnectionReader;

--- Create a new `ConnectionReader`.
---@param  exasol_context ExasolUdfContext handle to local database functions and status
-- @return connection definition reader
function ConnectionReader:new(exasol_context)
    local instance = setmetatable({}, self)
    instance:_init(exasol_context)
    return instance
end

function ConnectionReader:_init(exasol_context)
    self._exasol_context = exasol_context
end

---Parse the JDBC URL arguments into a table
---@param jdbc_url_args string JDBC URL arguments
---@return table<string, string> arguments
local function parse_args(jdbc_url_args)
    local args = {}
    for k, v in jdbc_url_args:gmatch("([^;=]+)=([^;=]+)") do
        args[k] = v
    end
    return args
end

---Parse the given JDBC URL into its components
---@param jdbc_url string JDBC URL to be parsed
---@return string? host
---@return number? port
local function parse_jdbc_url(jdbc_url)
    local host, port, jdbc_url_args = jdbc_url:match("jdbc:databricks://([^:]+):(%d+)(.*)")
    if jdbc_url_args then
        local args = parse_args(jdbc_url_args)
    end
    return host, tonumber(port)
end

---Read the details for the connection object with the given name
---@param connection_name string name of the connection to be read
---@return DatabricksConnectionDetails connection connection details
function ConnectionReader:read(connection_name)
    log.trace("Reading connection details for '%s'...", connection_name)
    local connection_details = self._exasol_context.get_connection(connection_name)
    if not connection_details then
        error(tostring(ExaError:new("E-VSDAB-2", "Connection {{connection_name}} not found.",
                                    {connection_name = connection_name})))
    end
    local jdbc_url = connection_details.address
    if not jdbc_url then
        error(tostring(ExaError:new("E-VSDAB-3", "Connection {{connection_name}} has no address.",
                                    {connection_name = connection_name})))
    end
    local host, port = parse_jdbc_url(jdbc_url)
    if not host then
        error(tostring(ExaError:new("E-VSDAB-4",
                                    "Connection {{connection_name}} contains invalid JDBC URL {{jdbc_url}}.",
                                    {connection_name = connection_name, jdbc_url = jdbc_url}):add_mitigations(
                "URL must be in the form 'jdbc:databricks://<host>:<port>;parameter=value;...'.")))
    end
    if connection_details.user ~= "token" then
        error(tostring(ExaError:new("E-VSDAB-13", "Connection {{connection_name}} contains invalid user {{user}}.",
                                    {connection_name = connection_name, user = connection_details.user})
                :add_mitigations(
                        "Only token authentication is supported, please specify USER='token' and PASSWORD='<token>` in the connection.")))
    end
    if not connection_details.password then
        error(tostring(ExaError:new("E-VSDAB-14", "Connection {{connection_name}} does not contain a valid token.",
                                    {connection_name = connection_name}):add_mitigations(
                "Please specify PASSWORD='<token>` in the connection.")))
    end
    port = port or 443
    local url = "https://" .. host .. ":" .. port
    log.trace("Extracted Databricks URL '%s' from JDBC URL", url)
    return {url = url, token = connection_details.password}
end

return ConnectionReader;
